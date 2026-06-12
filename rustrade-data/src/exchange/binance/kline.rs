//! Live Binance kline (candle) WebSocket payloads and their normalisation to
//! [`Candle`](crate::subscription::candle::Candle).
//!
//! Covers both the [`BinanceSpot`](crate::exchange::binance::spot::BinanceSpot) `@kline_<interval>`
//! stream and the
//! [`BinanceFuturesUsdMarket`](crate::exchange::binance::futures::BinanceFuturesUsdMarket)
//! `@continuousKline_<interval>` stream (perpetual-only). Endpoints are public/unauthenticated.
//!
//! # Closed candles only (no repaint)
//!
//! rustrade emits **closed candles only** — an in-progress kline (`k.x == false`) yields an empty
//! [`MarketIter`](crate::event::MarketIter), so consumers never see a repainting/lookahead value.
//! The exclusive `close_time` boundary is recomputed library-side as `open + interval` (see
//! [`close_time_from_open`](crate::subscription::candle::close_time_from_open)), **not** taken from
//! Binance's wire `T` (its `period-end − 1ms` convention) — consumers comparing against the raw `T`
//! will see a 1ms difference by design.
//!
//! # Reconnection: no replay, no dedup (consumer policy)
//!
//! Across a reconnect the underlying [`Connector`](crate::exchange::Connector) /
//! `ReconnectingStream` re-subscribes but does **not** replay or de-duplicate: a closed candle
//! straddling the disconnect may be **re-delivered or skipped**. De-duplication and gap-back-fill
//! are **consumer policy, not library policy** (consistent with rustrade's "no consumer-specific
//! policy in the library" rule). A consumer wanting a gapless series should reconcile the live
//! candle stream against a
//! [`fetch_candles`](crate::exchange::binance::historical::BinanceHistoricalClient::fetch_candles)
//! backfill keyed on `close_time` (the field both paths agree on, since `open ≡ close − interval`).

use super::BinanceChannel;
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::{Candle, CandleInterval, close_time_from_open},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::subscription::SubscriptionId;
use serde::Deserialize;
use smol_str::SmolStr;

/// The inner `k` payload shared by the [`BinanceSpot`](super::spot::BinanceSpot) `@kline_`
/// and [`BinanceFuturesUsdMarket`](super::futures::BinanceFuturesUsdMarket) `@continuousKline_` streams.
///
/// Both surfaces carry an identical `k` object for the fields rustrade consumes; they differ
/// only in where the market symbol lives (spot top-level `s` vs futures top-level `ps`) and
/// in the continuous payload omitting `k.s`. This struct deliberately omits the symbol so it
/// can be reused by both — the symbol is captured by the outer model and used to build the
/// [`SubscriptionId`].
///
/// ### Correctness notes
/// - OHLCV are JSON **strings** on the wire (e.g. `"0.01634790"`) → parsed via `de_str` to
///   [`Decimal`], never through an `f64` (which silently truncates precision).
/// - `open_time` (`t`) is the candle's open instant; the exclusive `close_time` boundary is
///   recomputed library-side via [`close_time_from_open`] (`open + interval`), **not** taken
///   from the wire `T` (Binance's `period-end − 1ms` convention).
// `Serialize` is intentionally not derived: the fields are decoded from Binance's wire shape via
// `deserialize_with` (epoch-ms timestamps, string-encoded decimals), so a derived `Serialize` would
// emit a different shape that does not round-trip — and nothing serializes this decode-only payload.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize)]
pub struct BinanceKlineData {
    #[serde(
        alias = "t",
        deserialize_with = "rustrade_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub open_time: DateTime<Utc>,
    #[serde(alias = "i")]
    pub interval: CandleInterval,
    #[serde(
        alias = "o",
        deserialize_with = "rustrade_integration::serde::de::de_str"
    )]
    pub open: Decimal,
    #[serde(
        alias = "h",
        deserialize_with = "rustrade_integration::serde::de::de_str"
    )]
    pub high: Decimal,
    #[serde(
        alias = "l",
        deserialize_with = "rustrade_integration::serde::de::de_str"
    )]
    pub low: Decimal,
    #[serde(
        alias = "c",
        deserialize_with = "rustrade_integration::serde::de::de_str"
    )]
    pub close: Decimal,
    #[serde(
        alias = "v",
        deserialize_with = "rustrade_integration::serde::de::de_str"
    )]
    pub volume: Decimal,
    #[serde(alias = "n")]
    pub trade_count: u64,
    /// `true` once the kline interval has closed. rustrade emits **closed candles only**
    /// (no repaint/lookahead) — an in-progress kline yields an empty [`MarketIter`].
    #[serde(alias = "x")]
    pub closed: bool,
}

impl BinanceKlineData {
    /// Map a kline payload to a normalised [`Candle`] [`MarketEvent`], honouring the
    /// closed-only policy and the `close_time = open + interval` boundary contract.
    ///
    /// - In-progress klines (`x == false`) yield an empty [`MarketIter`] (no event).
    /// - A closed kline whose computed `close_time` overflows the representable
    ///   [`DateTime<Utc>`] range yields an observable [`DataError`] rather than a silent
    ///   drop or a plausible-but-wrong timestamp (unreachable for real intervals, but the
    ///   boundary contract forbids a silent fallback).
    fn into_market_iter<InstrumentKey>(
        self,
        exchange_id: ExchangeId,
        instrument: InstrumentKey,
    ) -> MarketIter<InstrumentKey, Candle> {
        if !self.closed {
            return MarketIter(vec![]);
        }

        match close_time_from_open(self.open_time, self.interval.to_step()) {
            Some(close_time) => MarketIter(vec![Ok(MarketEvent {
                time_exchange: close_time,
                time_received: Utc::now(),
                exchange: exchange_id,
                instrument,
                kind: Candle {
                    close_time,
                    open: self.open,
                    high: self.high,
                    low: self.low,
                    close: self.close,
                    volume: self.volume,
                    trade_count: self.trade_count,
                },
            })]),
            None => MarketIter(vec![Err(DataError::Socket(format!(
                "Binance candle close_time overflow: open_time {} + interval {} exceeds the representable DateTime<Utc> range",
                self.open_time, self.interval
            )))]),
        }
    }
}

/// [`BinanceSpot`](super::spot::BinanceSpot) real-time kline (candle) message.
///
/// ### Raw Payload Example
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams>
/// ```json
/// {
///     "e": "kline",
///     "E": 1638747660000,
///     "s": "BTCUSDT",
///     "k": {
///         "t": 1638747660000, "T": 1638747719999, "s": "BTCUSDT", "i": "1m",
///         "f": 100, "L": 200, "o": "0.0010", "c": "0.0020", "h": "0.0025",
///         "l": "0.0015", "v": "1000", "n": 100, "x": false, "q": "1.0000",
///         "V": "500", "Q": "0.500", "B": "123456"
///     }
/// }
/// ```
// `Serialize` is intentionally not derived: the hand-written `Deserialize` reads Binance's wire
// frame (top-level `s`, nested `k`), a different shape than this struct's own fields, so a derived
// `Serialize` would not round-trip — and nothing serializes these wire types.
#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub struct BinanceKline {
    /// Instrument-map routing key `{channel}|{MARKET}` (e.g. `@kline_1m|BTCUSDT`), baked at
    /// deserialize from the top-level symbol (`s`) and the inner interval (`k.i`) by reusing
    /// [`ExchangeSub::id`](crate::exchange::ExchangeSub) — the single source of truth shared with
    /// subscribe-time key construction. Baking here (rather than re-deriving per frame in `id()`)
    /// means the subscribe-time and frame-time keys cannot drift and silently misroute.
    pub subscription_id: SubscriptionId,
    pub kline: BinanceKlineData,
}

impl<'de> Deserialize<'de> for BinanceKline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        /// Private wire mirror of the spot `@kline_` frame: the symbol lives top-level (`s`) and
        /// the interval is nested in `k.i`, so the routing key needs both fields — a single-field
        /// `deserialize_with` cannot see across them.
        #[derive(Deserialize)]
        struct Wire {
            #[serde(rename = "s")]
            symbol: SmolStr,
            #[serde(rename = "k")]
            kline: BinanceKlineData,
        }

        let Wire { symbol, kline } = Wire::deserialize(deserializer)?;
        let subscription_id =
            ExchangeSub::from((BinanceChannel::spot_candle(kline.interval), symbol.as_str())).id();
        Ok(Self {
            subscription_id,
            kline,
        })
    }
}

impl Identifier<Option<SubscriptionId>> for BinanceKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from((exchange_id, instrument, kline): (ExchangeId, InstrumentKey, BinanceKline)) -> Self {
        kline.kline.into_market_iter(exchange_id, instrument)
    }
}

/// [`BinanceFuturesUsdMarket`](super::futures::BinanceFuturesUsdMarket) real-time continuous-contract
/// (perpetual) kline message.
///
/// Differs from the spot [`BinanceKline`] shape: the symbol is the top-level `ps` (pair) and
/// `ct` (contract type), and the inner `k` object has **no** `s` field.
///
/// ### Raw Payload Example
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-streams>
/// ```json
/// {
///     "e": "continuous_kline",
///     "E": 1607443058651,
///     "ps": "BTCUSDT",
///     "ct": "PERPETUAL",
///     "k": {
///         "t": 1607443020000, "T": 1607443079999, "i": "1m", "f": 116467658886,
///         "L": 116468012423, "o": "18787.00", "c": "18804.04", "h": "18804.04",
///         "l": "18786.54", "v": "197.664", "n": 543, "x": false, "q": "3715253.19494",
///         "V": "184.769", "Q": "3472925.84746", "B": "0"
///     }
/// }
/// ```
// `Serialize` is intentionally not derived: the hand-written `Deserialize` reads Binance's wire
// frame (top-level `ps`, nested `k`), a different shape than this struct's own fields, so a derived
// `Serialize` would not round-trip — and nothing serializes these wire types.
#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub struct BinanceContinuousKline {
    /// Instrument-map routing key `{channel}|{MARKET}` (e.g.
    /// `_perpetual@continuousKline_1m|BTCUSDT`), baked at deserialize from the top-level pair
    /// (`ps`) and the inner interval (`k.i`) by reusing
    /// [`ExchangeSub::id`](crate::exchange::ExchangeSub) — the single source of truth shared with
    /// subscribe-time key construction. The continuous payload has no `k.s`, so the pair is the
    /// only symbol source. The channel prefix is hardcoded perpetual (`_perpetual@`): the wire
    /// `ct` (contract type) is intentionally not deserialized, since the
    /// `_perpetual@continuousKline_` subscription this model decodes only ever receives PERPETUAL
    /// frames. A non-perpetual frame (impossible given the subscription) would build a
    /// non-matching key and be dropped at the instrument-map lookup rather than mis-attributed.
    pub subscription_id: SubscriptionId,
    pub kline: BinanceKlineData,
}

impl<'de> Deserialize<'de> for BinanceContinuousKline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        /// Private wire mirror of the futures `@continuousKline_` frame: the pair lives top-level
        /// (`ps`) and the interval is nested in `k.i`, so the routing key needs both fields — a
        /// single-field `deserialize_with` cannot see across them.
        #[derive(Deserialize)]
        struct Wire {
            #[serde(rename = "ps")]
            pair: SmolStr,
            #[serde(rename = "k")]
            kline: BinanceKlineData,
        }

        let Wire { pair, kline } = Wire::deserialize(deserializer)?;
        let subscription_id = ExchangeSub::from((
            BinanceChannel::futures_candle(kline.interval),
            pair.as_str(),
        ))
        .id();
        Ok(Self {
            subscription_id,
            kline,
        })
    }
}

impl Identifier<Option<SubscriptionId>> for BinanceContinuousKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceContinuousKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, BinanceContinuousKline),
    ) -> Self {
        kline.kline.into_market_iter(exchange_id, instrument)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    /// Spot `@kline_1m` frame for a closed candle.
    const SPOT_CLOSED: &str = r#"
    {
        "e": "kline", "E": 1638747660000, "s": "BTCUSDT",
        "k": {
            "t": 1638747660000, "T": 1638747719999, "s": "BTCUSDT", "i": "1m",
            "f": 100, "L": 200, "o": "0.0010", "c": "0.0020", "h": "0.0025",
            "l": "0.0015", "v": "1000", "n": 100, "x": true, "q": "1.0000",
            "V": "500", "Q": "0.500", "B": "123456"
        }
    }"#;

    /// Futures `continuousKline_1s` frame for an in-progress candle (note: no `k.s`).
    const FUTURES_OPEN: &str = r#"
    {
        "e": "continuous_kline", "E": 1607443058651, "ps": "BTCUSDT", "ct": "PERPETUAL",
        "k": {
            "t": 1607443020000, "T": 1607443079999, "i": "1s",
            "f": 116467658886, "L": 116468012423, "o": "18787.00", "c": "18804.04",
            "h": "18804.04", "l": "18786.54", "v": "197.664", "n": 543, "x": false,
            "q": "3715253.19494", "V": "184.769", "Q": "3472925.84746", "B": "0"
        }
    }"#;

    /// Futures `continuousKline_1m` frame for a **closed** candle (note: no `k.s`; the symbol
    /// source is the top-level `ps` pair).
    const FUTURES_CLOSED: &str = r#"
    {
        "e": "continuous_kline", "E": 1607443079999, "ps": "BTCUSDT", "ct": "PERPETUAL",
        "k": {
            "t": 1607443020000, "T": 1607443079999, "i": "1m",
            "f": 116467658886, "L": 116468012423, "o": "18787.00", "c": "18804.04",
            "h": "18810.00", "l": "18786.54", "v": "197.664", "n": 543, "x": true,
            "q": "3715253.19494", "V": "184.769", "Q": "3472925.84746", "B": "0"
        }
    }"#;

    #[test]
    fn spot_kline_deserialises_and_builds_map_key() {
        let kline = serde_json::from_str::<BinanceKline>(SPOT_CLOSED).unwrap();
        assert_eq!(
            kline.subscription_id,
            SubscriptionId::from("@kline_1m|BTCUSDT")
        );
        assert_eq!(kline.kline.interval, CandleInterval::Min1);
        assert!(kline.kline.closed);
        assert_eq!(kline.kline.open, dec!(0.0010));
        assert_eq!(
            kline.id(),
            Some(SubscriptionId::from("@kline_1m|BTCUSDT")),
            "map key must be {{channel}}|{{MARKET}}, not the lowercase stream name"
        );
    }

    #[test]
    fn futures_continuous_kline_deserialises_without_k_s() {
        let kline = serde_json::from_str::<BinanceContinuousKline>(FUTURES_OPEN).unwrap();
        assert_eq!(
            kline.subscription_id,
            SubscriptionId::from("_perpetual@continuousKline_1s|BTCUSDT")
        );
        assert_eq!(kline.kline.interval, CandleInterval::Sec1);
        assert!(!kline.kline.closed);
        assert_eq!(
            kline.id(),
            Some(SubscriptionId::from(
                "_perpetual@continuousKline_1s|BTCUSDT"
            ))
        );
    }

    #[test]
    fn high_precision_ohlcv_round_trips_via_decimal_not_f64() {
        // A value an f64 intermediate would truncate; str→Decimal must preserve it exactly.
        let input = r#"
        {
            "e": "kline", "E": 1, "s": "ETHUSDT",
            "k": { "t": 0, "T": 1, "s": "ETHUSDT", "i": "1m", "o": "0.000000010000000",
                   "c": "0", "h": "0", "l": "0", "v": "0", "n": 0, "x": true }
        }"#;
        let kline = serde_json::from_str::<BinanceKline>(input).unwrap();
        assert_eq!(kline.kline.open, dec!(0.000000010000000));
    }

    #[test]
    fn closed_candle_maps_to_candle_with_boundary_close_time() {
        let kline = serde_json::from_str::<BinanceKline>(SPOT_CLOSED).unwrap();
        let MarketIter(events) =
            MarketIter::<u64, Candle>::from((ExchangeId::BinanceSpot, 1u64, kline));
        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().unwrap().unwrap();
        // close_time = open (1638747660000ms) + 1m = 1638747720000ms — NOT the wire `T`
        // (1638747719999ms, the `period-end − 1ms` convention).
        let expected = Utc.timestamp_millis_opt(1638747720000).unwrap();
        assert_eq!(event.kind.close_time, expected);
        assert_eq!(event.time_exchange, expected);
        assert_eq!(event.kind.trade_count, 100);
    }

    #[test]
    fn in_progress_candle_emits_nothing() {
        let kline = serde_json::from_str::<BinanceContinuousKline>(FUTURES_OPEN).unwrap();
        let MarketIter(events) =
            MarketIter::<u64, Candle>::from((ExchangeId::BinanceFuturesUsd, 1u64, kline));
        assert!(events.is_empty(), "in-progress klines must not emit events");
    }

    #[test]
    fn closed_continuous_kline_maps_with_boundary_close_time_via_ps_pair() {
        // Symmetric to `closed_candle_maps_to_candle_with_boundary_close_time`, but for the
        // futures continuous frame: the symbol comes from `ps` (no `k.s`) and the OHLCV must
        // map through unchanged.
        let kline = serde_json::from_str::<BinanceContinuousKline>(FUTURES_CLOSED).unwrap();
        let MarketIter(events) =
            MarketIter::<u64, Candle>::from((ExchangeId::BinanceFuturesUsd, 7u64, kline));
        assert_eq!(
            events.len(),
            1,
            "a closed continuous kline must emit exactly one candle"
        );
        let event = events.into_iter().next().unwrap().unwrap();
        // close_time = open (1607443020000ms) + 1m = 1607443080000ms — the exclusive boundary,
        // NOT the wire `T` (1607443079999ms).
        let expected = Utc.timestamp_millis_opt(1607443080000).unwrap();
        assert_eq!(event.kind.close_time, expected);
        assert_eq!(event.time_exchange, expected);
        assert_eq!(event.kind.open, dec!(18787.00));
        assert_eq!(event.kind.high, dec!(18810.00));
        assert_eq!(event.kind.low, dec!(18786.54));
        assert_eq!(event.kind.close, dec!(18804.04));
        assert_eq!(event.kind.volume, dec!(197.664));
        assert_eq!(event.kind.trade_count, 543);
    }

    /// Drift guard: for **every** [`CandleInterval`] the routing key baked at deserialize must
    /// equal the canonical instrument-map key `{channel}|{MARKET}` — the same key built at
    /// subscribe time by [`ExchangeSub::id`] and stored in the instrument
    /// [`Map`](crate::subscription::Map). Asserting against the literal wire format here (rather
    /// than a second [`ExchangeSub::id`] call, which would tautologically re-derive the baked key)
    /// also pins the channel prefix and the `{}|{}` separator, so a regression in either is caught,
    /// not just a wrong field being read. If the baked and subscribe-time keys ever drift, frames
    /// silently misroute/drop at `Map::find`; iterating here catches that at construction (in CI),
    /// not in production. Covers both spot (`@kline_`) and futures (`_perpetual@continuousKline_`).
    #[test]
    fn baked_subscription_id_matches_exchange_sub_for_every_interval() {
        const MARKET: &str = "BTCUSDT";

        for interval in CandleInterval::ALL {
            let wire = interval.as_str();

            // Spot `@kline_<interval>`.
            let spot_frame = format!(
                r#"{{ "e": "kline", "E": 1, "s": "{MARKET}",
                      "k": {{ "t": 0, "T": 1, "s": "{MARKET}", "i": "{wire}",
                              "o": "0", "c": "0", "h": "0", "l": "0", "v": "0", "n": 0, "x": true }} }}"#
            );
            let spot = serde_json::from_str::<BinanceKline>(&spot_frame).unwrap();
            assert_eq!(
                spot.id(),
                Some(SubscriptionId::from(format!("@kline_{wire}|{MARKET}"))),
                "spot kline routing key drifted from the canonical `@kline_<i>|<MARKET>` form for {interval:?}"
            );

            // Futures `_perpetual@continuousKline_<interval>` (no `k.s`; symbol from `ps`).
            let futures_frame = format!(
                r#"{{ "e": "continuous_kline", "E": 1, "ps": "{MARKET}", "ct": "PERPETUAL",
                      "k": {{ "t": 0, "T": 1, "i": "{wire}",
                              "o": "0", "c": "0", "h": "0", "l": "0", "v": "0", "n": 0, "x": true }} }}"#
            );
            let futures = serde_json::from_str::<BinanceContinuousKline>(&futures_frame).unwrap();
            assert_eq!(
                futures.id(),
                Some(SubscriptionId::from(format!(
                    "_perpetual@continuousKline_{wire}|{MARKET}"
                ))),
                "futures kline routing key drifted from the canonical `_perpetual@continuousKline_<i>|<MARKET>` form for {interval:?}"
            );
        }
    }

    #[test]
    fn zero_volume_candle_is_delivered_not_filtered() {
        // Binance REST gap-fills zero-trade periods (V=0, OHLC=prev_close); the library must
        // not drop them. (WS omits them, but if one arrives it is still delivered.)
        let input = r#"
        {
            "e": "kline", "E": 1, "s": "BTCUSDT",
            "k": { "t": 0, "T": 59999, "s": "BTCUSDT", "i": "1m", "o": "100", "c": "100",
                   "h": "100", "l": "100", "v": "0", "n": 0, "x": true }
        }"#;
        let kline = serde_json::from_str::<BinanceKline>(input).unwrap();
        let MarketIter(events) =
            MarketIter::<u64, Candle>::from((ExchangeId::BinanceSpot, 1u64, kline));
        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.volume, dec!(0));
        assert_eq!(event.kind.trade_count, 0);
    }
}
