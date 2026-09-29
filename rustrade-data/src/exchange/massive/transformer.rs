//! JSON to rustrade type transformations for Massive API responses.

use super::error::MassiveError;
use crate::{
    books::Level,
    subscription::{
        book::OrderBookL1,
        candle::{Candle, IntervalStep, close_time_from_open},
        trade::PublicTrade,
    },
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;

/// Deserialize only the first element of a conditions array without allocating a Vec.
pub(super) fn deserialize_first_condition<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{SeqAccess, Visitor};

    struct FirstElementVisitor;

    impl<'de> Visitor<'de> for FirstElementVisitor {
        type Value = Option<i32>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an array of integers or null")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_seq(self)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let first = seq.next_element()?;
            // Drain remaining elements without storing them
            while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
            Ok(first)
        }
    }

    deserializer.deserialize_option(FirstElementVisitor)
}

/// Raw aggregates response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct AggregatesResponse {
    /// Ticker symbol requested
    #[allow(dead_code)] // Retained for API schema; internal pagination uses next_url
    pub ticker: Option<String>,

    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// Request ID for debugging
    #[allow(dead_code)] // Retained for API schema completeness
    pub request_id: Option<String>,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Aggregate bars
    pub results: Option<Vec<AggregateBar>>,
}

/// Single OHLCV bar from Massive aggregates endpoint.
#[derive(Debug, Deserialize)]
pub struct AggregateBar {
    /// Open price
    #[serde(rename = "o", with = "rust_decimal::serde::float")]
    pub open: Decimal,

    /// High price
    #[serde(rename = "h", with = "rust_decimal::serde::float")]
    pub high: Decimal,

    /// Low price
    #[serde(rename = "l", with = "rust_decimal::serde::float")]
    pub low: Decimal,

    /// Close price
    #[serde(rename = "c", with = "rust_decimal::serde::float")]
    pub close: Decimal,

    /// Volume
    #[serde(rename = "v", with = "rust_decimal::serde::float")]
    pub volume: Decimal,

    /// Volume-weighted average price
    #[serde(rename = "vw", with = "rust_decimal::serde::float_option", default)]
    #[allow(dead_code)] // Retained for API schema completeness; not yet consumed
    pub vwap: Option<Decimal>,

    /// Unix timestamp in milliseconds (start of the bar)
    #[serde(rename = "t")]
    pub timestamp: i64,

    /// Number of trades in this bar
    #[serde(rename = "n")]
    pub trade_count: Option<u64>,
}

/// What a Massive aggregate bar was **built from**, which is market-dependent and decides the
/// meaning of every activity count on the bar.
///
/// Massive documents `v` identically across asset classes ("the trading volume of the symbol in
/// the given time period"), but forex aggregates are not built from a trade tape: spot FX has no
/// consolidated one, so Massive derives its forex bars from quoted bid/ask updates. `v` there
/// counts *quote activity*, and a quote is not a trade — nobody transacted that "volume".
///
/// Carrying it into [`Candle::volume`] anyway would be exactly the silent lie
/// [that field's contract](Candle::volume) exists to prevent: a volume-derived feature, a VWAP, or
/// a liquidity filter would read a real number where the venue reports nothing about size at all.
/// Spot FX is the motivating example named in `Candle::volume`'s own docs.
///
/// # Why this classifies the bar rather than one field
/// `n` ("the number of transactions in the aggregate window") is documented just as generically as
/// `v`, and the same fact overrides both: a bar generated from quotes contains **no transactions**,
/// so `n` cannot be a transaction count on it either. It is a quote-update count — the same
/// quantity as `v`, differing only in units. Gating `v` while passing `n` through would leave
/// `candle.trade_count` reporting quote ticks in the hundreds per minute regardless of liquidity,
/// so a `trade_count >= 50` liquidity filter would pass on every forex bar and look like it was
/// working. That is bit-for-bit the defect gating `v` removes.
///
/// Refs: <https://massive.com/docs/rest/forex/aggregates/custom-bars> (forex aggregates are
/// "generated from quoted bid/ask prices rather than executed trades").
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum AggregateProvenance {
    /// Built from a trade tape — stocks, options and crypto. `v` and `n` mean what they say.
    TradeTape,
    /// Built from quoted bid/ask updates, so neither `v` nor `n` describes any transaction and the
    /// candle reports `volume: None` and `trade_count: None`.
    QuoteTape,
}

impl AggregateProvenance {
    /// Classify a Massive REST ticker by its asset-class prefix.
    ///
    /// `C:` is forex (`C:EURUSD`); everything else — `X:` crypto, `O:` options, bare stock
    /// symbols — has a trade tape. A ticker whose class Massive adds later defaults to
    /// [`TradeTape`](Self::TradeTape), matching the pre-existing behaviour for anything
    /// unrecognised.
    ///
    /// The prefix match is **ASCII case-insensitive**. Massive's own tickers are uppercase, but
    /// nothing upstream of here normalises one: `validate_ticker` rejects empty strings and
    /// URL-breaking characters only. A case-sensitive match would classify `c:eurusd` as
    /// [`TradeTape`](Self::TradeTape) and report quote-update counts as real trading activity —
    /// precisely the silent lie this type exists to prevent.
    #[must_use]
    pub fn for_ticker(ticker: &str) -> Self {
        // `get` rather than slicing: a leading character 3 or 4 bytes wide puts byte 2 *inside* it,
        // and `&ticker[..2]` panics on a non-char boundary. `get` returns `None` there, falling
        // through to `TradeTape` like any other ticker that is not `C:`-prefixed.
        if ticker
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("C:"))
        {
            Self::QuoteTape
        } else {
            Self::TradeTape
        }
    }

    /// Apply these semantics to a wire `v`.
    pub(super) fn volume(self, volume: Decimal) -> Option<Decimal> {
        match self {
            Self::TradeTape => Some(volume),
            Self::QuoteTape => None,
        }
    }

    /// Apply these semantics to a wire `n`.
    ///
    /// `None` in, `None` out: the provider already models an absent count, and that absence is
    /// carried through as "unknown" rather than flattened to a fabricated `0`.
    fn trade_count(self, trade_count: Option<u64>) -> Option<u64> {
        match self {
            Self::TradeTape => trade_count,
            Self::QuoteTape => None,
        }
    }
}

impl AggregateBar {
    /// Convert to rustrade [`Candle`], mapping `(multiplier, timespan)` to the
    /// shared [`IntervalStep`].
    ///
    /// `close_time` is the exclusive end-of-period boundary `open + interval`,
    /// computed per-bar via [`close_time_from_open`] — calendar intervals
    /// (`month`/`quarter`/`year`) therefore use leap-year-correct month
    /// arithmetic, not an approximate fixed `Duration`.
    ///
    /// `provenance` says what this market's `v` and `n` mean — see [`AggregateProvenance`], and
    /// [`AggregateProvenance::for_ticker`] to derive it from the ticker you requested.
    ///
    /// Test-only. `mod transformer` is `pub(crate)` and this type is not re-exported, so a `pub fn`
    /// here would not actually be reachable by a downstream user however it were annotated —
    /// leaving it looking like public API while carrying `#[allow(dead_code)]` claimed a contract
    /// that does not exist. Production code takes `into_candle_with_step`, which pre-computes the
    /// step once per stream instead of per bar.
    #[cfg(test)]
    fn into_candle(
        self,
        multiplier: u32,
        timespan: &str,
        provenance: AggregateProvenance,
    ) -> Result<Candle, MassiveError> {
        self.into_candle_with_step(timespan_to_step(multiplier, timespan), provenance)
    }

    /// Convert to rustrade [`Candle`] with a pre-computed [`IntervalStep`].
    ///
    /// Use this variant when processing multiple bars with the same timespan to
    /// avoid recomputing the step for each bar. The step is still applied
    /// **per-bar** (`close_time_from_open(open, step)`) because calendar months
    /// are variable-length — a single pre-computed `Duration` for the whole
    /// stream would be wrong for `month`/`quarter`/`year`.
    ///
    /// `provenance` says what this market's `v` and `n` mean — see [`AggregateProvenance`].
    ///
    /// # Errors
    ///
    /// Returns [`MassiveError::InvalidInput`] if the computed `close_time`
    /// overflows the representable [`DateTime<Utc>`] range. An un-parseable
    /// *input* `timestamp` is, by contrast, tolerated with a warning and
    /// `UNIX_EPOCH` (preserving prior behaviour) — only a computed *boundary*
    /// overflow is a hard error.
    pub fn into_candle_with_step(
        self,
        step: IntervalStep,
        provenance: AggregateProvenance,
    ) -> Result<Candle, MassiveError> {
        let start_time = Utc
            .timestamp_millis_opt(self.timestamp)
            .single()
            .unwrap_or_else(|| {
                tracing::warn!(
                    timestamp_ms = self.timestamp,
                    "AggregateBar has out-of-range timestamp; using UNIX_EPOCH"
                );
                DateTime::<Utc>::UNIX_EPOCH
            });
        let close_time =
            close_time_from_open(start_time, step).ok_or_else(|| MassiveError::InvalidInput {
                message: format!("candle close_time overflow: open={start_time}, step={step:?}"),
            })?;

        Ok(Candle {
            close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            // Neither field is unconditionally `Some`: a quote-derived bar contains no transactions,
            // so `v` counts quote updates and `n` counts the same events in different units.
            // Reporting either as trading activity would be indistinguishable downstream from the
            // real thing. See `AggregateProvenance`.
            //
            // `n` is also absent on some trade-tape bars, and that `None` is carried through as
            // "unknown" rather than the old lossy `unwrap_or(0)`. (The WebSocket aggregate has no
            // count at all: its `z` is an average trade *size* — see `message::MassiveAggregate`.)
            volume: provenance.volume(self.volume),
            trade_count: provenance.trade_count(self.trade_count),
        })
    }
}

/// Parse aggregates JSON response.
pub fn parse_aggregates_response(body: &str) -> Result<AggregatesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

/// Map a Massive `(multiplier, timespan)` to the shared [`IntervalStep`].
///
/// Fixed-length units (`second`…`week`) become [`IntervalStep::Fixed`], exact in
/// UTC. Calendar units (`month`/`quarter`/`year`) become [`IntervalStep::Months`]
/// (1 / 3 / 12 months per multiplier) so the boundary uses leap-year-correct
/// calendar arithmetic instead of the previous approximate `+30/91/365 days`.
pub fn timespan_to_step(multiplier: u32, timespan: &str) -> IntervalStep {
    let mult = i64::from(multiplier);
    match timespan {
        "second" => IntervalStep::Fixed(Duration::seconds(mult)),
        "minute" => IntervalStep::Fixed(Duration::minutes(mult)),
        "hour" => IntervalStep::Fixed(Duration::hours(mult)),
        "day" => IntervalStep::Fixed(Duration::days(mult)),
        "week" => IntervalStep::Fixed(Duration::weeks(mult)),
        "month" => IntervalStep::Months(multiplier),
        "quarter" => IntervalStep::Months(multiplier.saturating_mul(3)),
        "year" => IntervalStep::Months(multiplier.saturating_mul(12)),
        _ => {
            tracing::warn!(timespan = %timespan, "unknown timespan, defaulting to minutes");
            IntervalStep::Fixed(Duration::minutes(mult))
        }
    }
}

// ============================================================================
// Trades
// ============================================================================

/// Raw trades response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct TradesResponse {
    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Trade results
    pub results: Option<Vec<TradeRecord>>,
}

/// Single trade from Massive trades endpoint.
#[derive(Debug, Deserialize)]
pub struct TradeRecord {
    /// First trade condition code (crypto: 1=sell, 2=buy; equities: SIP codes differ).
    ///
    /// Only the first condition is extracted; remaining conditions are ignored.
    #[serde(
        rename = "conditions",
        default,
        deserialize_with = "deserialize_first_condition"
    )]
    pub first_condition: Option<i32>,

    /// Exchange ID
    #[serde(rename = "exchange")]
    #[allow(dead_code)] // Retained for API schema completeness
    pub exchange: Option<i32>,

    /// Trade ID
    #[serde(rename = "id", default)]
    pub id: String,

    /// Participant timestamp (nanoseconds)
    #[serde(rename = "participant_timestamp", default)]
    #[allow(dead_code)] // Accessed via timestamp() method
    pub participant_timestamp: i64,

    /// Trade price
    #[serde(rename = "price", with = "rust_decimal::serde::float")]
    pub price: Decimal,

    /// Trade size
    #[serde(rename = "size", with = "rust_decimal::serde::float")]
    pub size: Decimal,

    /// SIP timestamp (nanoseconds) - when SIP received the trade
    #[serde(rename = "sip_timestamp", default)]
    #[allow(dead_code)] // Retained for API schema completeness
    pub sip_timestamp: i64,
}

impl TradeRecord {
    /// Convert to rustrade PublicTrade type.
    ///
    /// # Side Detection (Crypto Only)
    ///
    /// The `side` field is only meaningful for **crypto tickers** (`X:` prefix).
    /// Crypto condition codes: 1 = sell-initiated, 2 = buy-initiated.
    ///
    /// For equities and other asset classes, condition codes represent SIP/CTA
    /// tape conditions (e.g., 1 = Regular Trade) which are unrelated to trade
    /// direction. The `side` will be `None` or incorrect for non-crypto tickers.
    ///
    /// # Timestamps
    ///
    /// Trade timestamps are not preserved in [`PublicTrade`]. If timestamp
    /// information is required, access the raw [`TradeRecord`] via
    /// [`parse_trades_response`] directly.
    pub fn into_public_trade(self) -> PublicTrade {
        // Crypto condition codes: 1 = sell-initiated, 2 = buy-initiated
        // Note: This mapping is ONLY valid for crypto (X: prefix) tickers
        let side = self.first_condition.and_then(|c| match c {
            1 => Some(Side::Sell),
            2 => Some(Side::Buy),
            _ => None,
        });

        PublicTrade {
            id: SmolStr::from(self.id),
            price: self.price,
            amount: self.size,
            side,
        }
    }

    /// Get the exchange timestamp as `DateTime<Utc>`.
    #[allow(dead_code)] // Public API for consumers accessing raw TradeRecord
    pub fn timestamp(&self) -> DateTime<Utc> {
        nanos_to_datetime(self.participant_timestamp)
    }
}

/// Convert nanosecond timestamp to `DateTime<Utc>`.
///
/// Returns [`DateTime::<Utc>::UNIX_EPOCH`] for out-of-range timestamps.
/// Negative values (pre-epoch) are handled correctly using Euclidean division.
fn nanos_to_datetime(nanos: i64) -> DateTime<Utc> {
    let secs = nanos.div_euclid(1_000_000_000);
    // rem_euclid always returns non-negative value in [0, 999_999_999], fits u32
    #[allow(clippy::cast_possible_truncation)]
    let nsecs = nanos.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single().unwrap_or_else(|| {
        tracing::warn!(nanos, "out-of-range nanosecond timestamp; using UNIX_EPOCH");
        DateTime::<Utc>::UNIX_EPOCH
    })
}

/// Parse trades JSON response.
pub fn parse_trades_response(body: &str) -> Result<TradesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

// ============================================================================
// Quotes (BBO/NBBO)
// ============================================================================

/// Raw quotes response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct QuotesResponse {
    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Quote results
    pub results: Option<Vec<QuoteRecord>>,
}

/// Single quote from Massive quotes endpoint.
#[derive(Debug, Deserialize)]
pub struct QuoteRecord {
    /// Ask price
    #[serde(rename = "ask_price", with = "rust_decimal::serde::float")]
    pub ask_price: Decimal,

    /// Ask size (optional - forex quotes don't include size)
    #[serde(
        rename = "ask_size",
        default,
        with = "rust_decimal::serde::float_option"
    )]
    pub ask_size: Option<Decimal>,

    /// Bid price
    #[serde(rename = "bid_price", with = "rust_decimal::serde::float")]
    pub bid_price: Decimal,

    /// Bid size (optional - forex quotes don't include size)
    #[serde(
        rename = "bid_size",
        default,
        with = "rust_decimal::serde::float_option"
    )]
    pub bid_size: Option<Decimal>,

    /// Participant timestamp (nanoseconds)
    #[serde(rename = "participant_timestamp", default)]
    pub participant_timestamp: i64,

    /// SIP timestamp (nanoseconds)
    #[serde(rename = "sip_timestamp", default)]
    #[allow(dead_code)] // Retained for API schema completeness
    pub sip_timestamp: i64,
}

impl QuoteRecord {
    /// Convert to rustrade OrderBookL1 type.
    ///
    /// Forex quotes from Massive omit `bid_size`/`ask_size`; absent sizes are
    /// represented as `Decimal::ZERO` to satisfy the shared `Level` type.
    /// Callers handling venues that may report zero-size quotes should
    /// disambiguate via the source feed if required.
    pub fn into_order_book_l1(self) -> OrderBookL1 {
        let timestamp = self.timestamp();

        OrderBookL1 {
            last_update_time: timestamp,
            best_bid: Some(Level {
                price: self.bid_price,
                amount: self.bid_size.unwrap_or(Decimal::ZERO),
            }),
            best_ask: Some(Level {
                price: self.ask_price,
                amount: self.ask_size.unwrap_or(Decimal::ZERO),
            }),
        }
    }

    /// Get the exchange timestamp as `DateTime<Utc>`.
    pub fn timestamp(&self) -> DateTime<Utc> {
        nanos_to_datetime(self.participant_timestamp)
    }
}

/// Parse quotes JSON response.
pub fn parse_quotes_response(body: &str) -> Result<QuotesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

// ============================================================================
// Fair Market Value
// ============================================================================

/// Fair Market Value - a calculated mid-price from Massive.
///
/// This is a dedicated type (not mapped to PublicTrade) because FMV represents
/// a calculated value, not an actual trade execution.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FairMarketValue {
    /// Timestamp of the FMV calculation
    pub time: DateTime<Utc>,
    /// Calculated fair market value price
    pub price: Decimal,
}

impl FairMarketValue {
    /// Create a new FairMarketValue.
    pub fn new(time: DateTime<Utc>, price: Decimal) -> Self {
        Self { time, price }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Tests should panic on unexpected values
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const SAMPLE_AGGREGATES: &str = r#"{
        "ticker": "X:BTCUSD",
        "queryCount": 2,
        "resultsCount": 2,
        "adjusted": true,
        "results": [
            {
                "v": 234.5678,
                "vw": 65432.1,
                "o": 65000.0,
                "c": 65100.0,
                "h": 65200.0,
                "l": 64900.0,
                "t": 1704067200000,
                "n": 150
            },
            {
                "v": 345.6789,
                "vw": 65150.0,
                "o": 65100.0,
                "c": 65250.0,
                "h": 65300.0,
                "l": 65050.0,
                "t": 1704067260000,
                "n": 175
            }
        ],
        "status": "OK",
        "request_id": "abc123"
    }"#;

    #[test]
    fn test_parse_aggregates() {
        let response = parse_aggregates_response(SAMPLE_AGGREGATES).unwrap();
        assert_eq!(response.ticker, Some("X:BTCUSD".to_string()));
        assert_eq!(response.results_count, 2);
        assert_eq!(response.status, "OK");

        let results = response.results.unwrap();
        assert_eq!(results.len(), 2);

        let bar = &results[0];
        assert_eq!(bar.open, dec!(65000.0));
        assert_eq!(bar.close, dec!(65100.0));
        assert_eq!(bar.high, dec!(65200.0));
        assert_eq!(bar.low, dec!(64900.0));
        assert_eq!(bar.trade_count, Some(150));
    }

    #[test]
    fn test_aggregate_bar_to_candle() {
        let bar = AggregateBar {
            open: dec!(65000.0),
            high: dec!(65200.0),
            low: dec!(64900.0),
            close: dec!(65100.0),
            volume: dec!(234.5678),
            vwap: Some(dec!(65050.0)),
            timestamp: 1704067200000,
            trade_count: Some(150),
        };

        let candle = bar
            .into_candle(1, "minute", AggregateProvenance::TradeTape)
            .unwrap();

        assert_eq!(candle.open, dec!(65000.0));
        assert_eq!(candle.close, dec!(65100.0));
        assert_eq!(candle.trade_count, Some(150));

        // close_time should be 1 minute after start
        let expected_close =
            Utc.timestamp_millis_opt(1704067200000).single().unwrap() + Duration::minutes(1);
        assert_eq!(candle.close_time, expected_close);
    }

    #[test]
    fn test_aggregate_bar_monthly_calendar_boundary() {
        // A January monthly bar must close at Feb 1 00:00 UTC via calendar
        // arithmetic, NOT open + 30 days (= Jan 31, the previous approximate bug).
        // `AggregateBar` is not `Clone`, so build a fresh bar per assertion.
        let bar_at = || AggregateBar {
            open: dec!(65000.0),
            high: dec!(65200.0),
            low: dec!(64900.0),
            close: dec!(65100.0),
            volume: dec!(234.5678),
            vwap: None,
            timestamp: 1_704_067_200_000, // 2024-01-01 00:00:00 UTC
            trade_count: Some(150),
        };

        // Month: Jan 1 -> Feb 1 (not 1704067200000 + 30d = 2024-01-31).
        assert_eq!(
            bar_at()
                .into_candle(1, "month", AggregateProvenance::TradeTape)
                .unwrap()
                .close_time
                .timestamp_millis(),
            1_706_745_600_000 // 2024-02-01 00:00:00 UTC
        );
        // Quarter: Jan 1 -> Apr 1.
        assert_eq!(
            bar_at()
                .into_candle(1, "quarter", AggregateProvenance::TradeTape)
                .unwrap()
                .close_time
                .timestamp_millis(),
            1_711_929_600_000 // 2024-04-01 00:00:00 UTC
        );
        // Year: Jan 1 -> next Jan 1.
        assert_eq!(
            bar_at()
                .into_candle(1, "year", AggregateProvenance::TradeTape)
                .unwrap()
                .close_time
                .timestamp_millis(),
            1_735_689_600_000 // 2025-01-01 00:00:00 UTC
        );
    }

    #[test]
    fn test_parse_with_next_url() {
        let json = r#"{
            "ticker": "X:BTCUSD",
            "resultsCount": 50000,
            "status": "OK",
            "next_url": "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/minute/1704067200000/1704153600000?cursor=abc123",
            "results": []
        }"#;

        let response = parse_aggregates_response(json).unwrap();
        assert!(response.next_url.is_some());
        assert!(response.next_url.unwrap().contains("cursor="));
    }

    #[test]
    fn test_parse_empty_results() {
        let json = r#"{
            "ticker": "X:BTCUSD",
            "resultsCount": 0,
            "status": "OK",
            "results": []
        }"#;

        let response = parse_aggregates_response(json).unwrap();
        assert_eq!(response.results_count, 0);
        assert!(response.results.unwrap().is_empty());
    }

    #[test]
    fn test_timespan_to_step() {
        assert_eq!(
            timespan_to_step(1, "second"),
            IntervalStep::Fixed(Duration::seconds(1))
        );
        assert_eq!(
            timespan_to_step(5, "minute"),
            IntervalStep::Fixed(Duration::minutes(5))
        );
        assert_eq!(
            timespan_to_step(1, "hour"),
            IntervalStep::Fixed(Duration::hours(1))
        );
        assert_eq!(
            timespan_to_step(1, "day"),
            IntervalStep::Fixed(Duration::days(1))
        );
        assert_eq!(
            timespan_to_step(1, "week"),
            IntervalStep::Fixed(Duration::weeks(1))
        );
        // Calendar units map to month counts (multiplier-scaled), not Durations.
        assert_eq!(timespan_to_step(1, "month"), IntervalStep::Months(1));
        assert_eq!(timespan_to_step(2, "quarter"), IntervalStep::Months(6));
        assert_eq!(timespan_to_step(1, "year"), IntervalStep::Months(12));
    }

    #[test]
    fn test_parse_trades() {
        let json = r#"{
            "results": [
                {
                    "conditions": [2],
                    "exchange": 1,
                    "id": "12345",
                    "participant_timestamp": 1704067200000000000,
                    "price": 65100.50,
                    "size": 0.5,
                    "sip_timestamp": 1704067200001000000
                }
            ],
            "status": "OK",
            "resultsCount": 1
        }"#;

        let response = parse_trades_response(json).unwrap();
        assert_eq!(response.results_count, 1);

        let results = response.results.unwrap();
        let trade = &results[0];
        assert_eq!(trade.price, dec!(65100.50));
        assert_eq!(trade.size, dec!(0.5));
        assert_eq!(trade.first_condition, Some(2)); // buy-initiated (crypto)

        let public_trade = results.into_iter().next().unwrap().into_public_trade();
        assert_eq!(public_trade.side, Some(Side::Buy));
    }

    #[test]
    fn test_parse_quotes() {
        let json = r#"{
            "results": [
                {
                    "ask_price": 65200.0,
                    "ask_size": 1.5,
                    "bid_price": 65100.0,
                    "bid_size": 2.0,
                    "participant_timestamp": 1704067200000000000,
                    "sip_timestamp": 1704067200001000000
                }
            ],
            "status": "OK",
            "resultsCount": 1
        }"#;

        let response = parse_quotes_response(json).unwrap();
        assert_eq!(response.results_count, 1);

        let results = response.results.unwrap();
        let quote = results.into_iter().next().unwrap();
        let l1 = quote.into_order_book_l1();

        assert_eq!(l1.best_bid.unwrap().price, dec!(65100.0));
        assert_eq!(l1.best_ask.unwrap().price, dec!(65200.0));
    }

    #[test]
    fn test_fair_market_value() {
        let fmv = FairMarketValue::new(
            Utc.timestamp_millis_opt(1704067200000).single().unwrap(),
            dec!(65150.0),
        );
        assert_eq!(fmv.price, dec!(65150.0));
    }

    /// The REST path derives the same distinction from the ticker's asset-class prefix, and applies
    /// it to **both** activity counts.
    ///
    /// `n` is documented as "the number of transactions in the aggregate window" — exactly as
    /// generically as `v` is documented as trading volume, and overridden by exactly the same fact:
    /// a bar Massive generated from quoted bid/ask updates contains no transactions to count. A
    /// quote-tick count arriving in `candle.trade_count` reads as liquidity that is not there, so a
    /// `trade_count >= 50` filter would pass on every forex bar and look like it was working.
    #[test]
    fn a_forex_rest_bar_reports_neither_volume_nor_trade_count() {
        let bar = || AggregateBar {
            open: dec!(1.1),
            high: dec!(1.2),
            low: dec!(1.0),
            close: dec!(1.15),
            volume: dec!(4321),
            vwap: None,
            timestamp: 1_704_067_200_000,
            trade_count: Some(150),
        };

        let forex = bar()
            .into_candle(1, "minute", AggregateProvenance::for_ticker("C:EURUSD"))
            .unwrap();
        assert_eq!(forex.volume, None);
        assert_eq!(forex.trade_count, None);

        // The identical wire shape on a market that does have a tape keeps both, so this is a
        // per-market decision rather than a blanket drop.
        let crypto = bar()
            .into_candle(1, "minute", AggregateProvenance::for_ticker("X:BTCUSD"))
            .unwrap();
        assert_eq!(crypto.volume, Some(dec!(4321)));
        assert_eq!(crypto.trade_count, Some(150));
    }

    #[test]
    fn aggregate_provenance_classifies_only_the_forex_prefix_as_a_quote_tape() {
        assert_eq!(
            AggregateProvenance::for_ticker("C:EURUSD"),
            AggregateProvenance::QuoteTape
        );
        for traded in ["X:BTCUSD", "AAPL", "O:SPY251219C00650000", "I:SPX"] {
            assert_eq!(
                AggregateProvenance::for_ticker(traded),
                AggregateProvenance::TradeTape,
                "{traded}"
            );
        }
    }

    /// Nothing upstream normalises a caller-supplied ticker, so a case-sensitive prefix match
    /// would report a forex quote-update count as real traded volume.
    #[test]
    fn aggregate_provenance_classifies_the_forex_prefix_regardless_of_case() {
        for forex in ["c:eurusd", "c:EURUSD", "C:eurusd"] {
            assert_eq!(
                AggregateProvenance::for_ticker(forex),
                AggregateProvenance::QuoteTape,
                "{forex}"
            );
        }
    }

    /// `..2` is a byte range, and `€:EURUSD` is the case that matters: a 3-byte leading character
    /// puts byte 2 mid-character, which slicing would panic on. The rest do not straddle a
    /// boundary — `é` is exactly 2 bytes, and the short strings stop before index 2 — and are here
    /// to pin that every non-`C:` shape reaches `TradeTape`, not only the ones `get` rejects.
    #[test]
    fn aggregate_provenance_does_not_panic_on_a_short_or_non_ascii_ticker() {
        for ticker in ["", "C", "€:EURUSD", "é"] {
            assert_eq!(
                AggregateProvenance::for_ticker(ticker),
                AggregateProvenance::TradeTape,
                "{ticker}"
            );
        }
    }
}
