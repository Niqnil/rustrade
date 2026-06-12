//! DBN record to rustrade event transformers.
//!
//! Converts Databento's DBN format records into normalized rustrade events.
//!
//! # Price Format
//!
//! DBN uses fixed-point `i64` with 1e-9 scaling (9 decimal places).
//! All conversions use `Decimal::new(px, 9)` for lossless representation.
//!
//! # Timestamp Format
//!
//! DBN timestamps are `u64` nanoseconds since UNIX epoch:
//! - `ts_event`: Exchange timestamp (used as `time_exchange`)
//! - `ts_recv`: Databento receive timestamp

use super::error::DatabentoErrorKind;
use crate::books::Level;
use crate::error::DataError;
use crate::subscription::book::OrderBookL1;
use crate::subscription::candle::{Candle, CandleInterval, close_time_from_open};
use crate::subscription::{quote::Quote, trade::PublicTrade};
use chrono::{DateTime, TimeZone, Utc};
use databento::dbn::{Mbp1Msg, OhlcvMsg, Schema, TradeMsg};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use rustrade_instrument::exchange::ExchangeId;
use smol_str::format_smolstr;

const UNDEF_PRICE: i64 = i64::MAX;
const UNDEF_SIZE: u32 = u32::MAX;

/// Convert a DBN TradeMsg to a PublicTrade.
///
/// Returns the exchange timestamp and the converted trade, or an error description.
///
/// # Side Field
///
/// DBN side values:
/// - `'A'` (65): Sell aggressor → `Some(Side::Sell)`
/// - `'B'` (66): Buy aggressor → `Some(Side::Buy)`
/// - `'N'` (78): No side specified by source → `None`
/// - Other values: Returns error (malformed data)
pub fn dbn_trade_to_public_trade(
    trade: &TradeMsg,
) -> Result<(DateTime<Utc>, PublicTrade), &'static str> {
    if trade.price == UNDEF_PRICE {
        return Err("undefined price");
    }

    let price = Decimal::new(trade.price, 9);
    let amount = Decimal::from(trade.size);

    let time_exchange = nanos_to_datetime(trade.hd.ts_event)?;

    // DBN guarantees Side is ASCII (range [0, 127]); i8 -> u8 cast is lossless.
    #[allow(clippy::cast_sign_loss)]
    let side = match trade.side as u8 {
        b'A' => Some(Side::Sell),
        b'B' => Some(Side::Buy),
        b'N' => None,
        _ => return Err("unknown trade side value"),
    };

    Ok((
        time_exchange,
        PublicTrade {
            id: format_smolstr!("{}", trade.sequence),
            price,
            amount,
            side,
        },
    ))
}

/// Convert a DBN Mbp1Msg (top-of-book) to a Quote.
///
/// Returns the exchange timestamp and the converted quote, or an error description.
///
/// Note: When DBN provides `UNDEF_SIZE` (`u32::MAX`) for bid/ask size, the
/// corresponding `bid_amount`/`ask_amount` is set to zero. Callers cannot
/// distinguish "empty book level" from "size unavailable in feed."
pub fn dbn_mbp1_to_quote(msg: &Mbp1Msg) -> Result<(DateTime<Utc>, Quote), &'static str> {
    let [level] = &msg.levels;

    if level.bid_px == UNDEF_PRICE || level.ask_px == UNDEF_PRICE {
        return Err("undefined bid or ask price");
    }

    let bid_price = Decimal::new(level.bid_px, 9);
    let ask_price = Decimal::new(level.ask_px, 9);
    // UNDEF_SIZE (u32::MAX) means size unavailable; map to zero (see rustdoc note)
    let bid_amount = if level.bid_sz == UNDEF_SIZE {
        Decimal::ZERO
    } else {
        Decimal::from(level.bid_sz)
    };
    let ask_amount = if level.ask_sz == UNDEF_SIZE {
        Decimal::ZERO
    } else {
        Decimal::from(level.ask_sz)
    };

    let time_exchange = nanos_to_datetime(msg.hd.ts_event)?;

    Ok((
        time_exchange,
        Quote {
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        },
    ))
}

/// Convert a DBN Mbp1Msg (top-of-book) to an OrderBookL1.
///
/// Returns the exchange timestamp and the converted order book snapshot, or an error description.
///
/// Unlike [`dbn_mbp1_to_quote`] which returns a flat [`Quote`], this returns [`OrderBookL1`]
/// with `Option<Level>` fields, allowing callers to distinguish "no data" from "level exists."
///
/// Prices are converted losslessly from DBN's fixed-point `i64` (9 decimal places) to `Decimal`.
pub fn dbn_mbp1_to_orderbook_l1(
    msg: &Mbp1Msg,
) -> Result<(DateTime<Utc>, OrderBookL1), &'static str> {
    let [level] = &msg.levels;

    let time_exchange = nanos_to_datetime(msg.hd.ts_event)?;

    let best_bid = if level.bid_px != UNDEF_PRICE {
        let price = Decimal::new(level.bid_px, 9);
        let amount = if level.bid_sz == UNDEF_SIZE {
            Decimal::ZERO
        } else {
            Decimal::from(level.bid_sz)
        };
        Some(Level { price, amount })
    } else {
        None
    };

    let best_ask = if level.ask_px != UNDEF_PRICE {
        let price = Decimal::new(level.ask_px, 9);
        let amount = if level.ask_sz == UNDEF_SIZE {
            Decimal::ZERO
        } else {
            Decimal::from(level.ask_sz)
        };
        Some(Level { price, amount })
    } else {
        None
    };

    Ok((
        time_exchange,
        OrderBookL1 {
            last_update_time: time_exchange,
            best_bid,
            best_ask,
        },
    ))
}

fn nanos_to_datetime(nanos: u64) -> Result<DateTime<Utc>, &'static str> {
    let secs = i64::try_from(nanos / 1_000_000_000).map_err(|_| "timestamp out of i64 range")?;
    let nsecs = (nanos % 1_000_000_000) as u32;

    Utc.timestamp_opt(secs, nsecs)
        .single()
        .ok_or("invalid timestamp")
}

/// Build a Databento conversion [`DataError`] for an OHLCV record.
fn ohlcv_conversion_error(message: String) -> DataError {
    DataError::Databento {
        kind: DatabentoErrorKind::Decode,
        context: "converting OHLCV record".to_string(),
        message,
    }
}

/// Convert a DBN [`OhlcvMsg`] to a [`Candle`], returning the exchange timestamp
/// (`time_exchange`) alongside it.
///
/// # Timing contract
///
/// Databento stamps OHLCV `ts_event` at the bar's **open** instant. This function
/// normalises it to the exclusive `close_time` boundary
/// (`close_time = open + interval`) via the shared
/// [`close_time_from_open`](crate::subscription::candle::close_time_from_open)
/// helper, exactly as the Binance and Hyperliquid candle paths do, so the
/// [`Candle::close_time`](crate::subscription::candle::Candle::close_time)
/// contract holds. `time_exchange` is set equal to `close_time`.
///
/// # Native OHLCV vs aggregate-from-trades
///
/// These are Databento's **native** OHLCV bars. For some venues Databento
/// recommends aggregating from the trades schema instead; that is a consumer
/// policy choice and is not performed here.
///
/// # `trade_count`
///
/// [`OhlcvMsg`] carries no trade-count field, so [`Candle::trade_count`] is set to
/// `0` rather than fabricated.
///
/// # Errors
///
/// Returns [`DataError::Databento`] if any price field is the DBN `UNDEF_PRICE`
/// sentinel (`i64::MAX`), if `ts_event` is unrepresentable, or if the computed
/// `close_time` overflows the [`DateTime<Utc>`] range. The latter two are
/// defensively unreachable in practice: DBN `ts_event` is `u64` nanoseconds and
/// caps near year 2554, far inside the representable range even after adding a
/// daily step — but the contract surfaces the failure rather than silently
/// fabricating a timestamp, and stays correct if DBN ever widens the field.
/// Unlike the
/// trade/quote transformers in this module — which return `&'static str` and are
/// `debug!`-skipped by their callers for malformed/undefined-price records — this
/// returns a typed [`DataError`] that callers **propagate**: native OHLCV bars
/// have no skippable-malformed mode (Databento only emits a bar when data
/// exists), so the only realistic failures are an undefined-price sentinel or a
/// timestamp/boundary overflow, which the `close_time` contract requires be
/// surfaced, never silently skipped.
pub fn dbn_ohlcv_to_candle(
    msg: &OhlcvMsg,
    interval: CandleInterval,
) -> Result<(DateTime<Utc>, Candle), DataError> {
    // Guard the DBN undefined-price sentinel (`i64::MAX`), consistent with the
    // trade/quote paths in this module. Native OHLCV bars should never carry it
    // (Databento only emits a bar when data exists), but if one ever does, decoding
    // it as `Decimal::new(i64::MAX, 9)` would silently yield a garbage ~9.2e9 price
    // that flows downstream unflagged — exactly the silent failure the close_time
    // contract requires be surfaced, never fabricated.
    if msg.open == UNDEF_PRICE
        || msg.high == UNDEF_PRICE
        || msg.low == UNDEF_PRICE
        || msg.close == UNDEF_PRICE
    {
        return Err(ohlcv_conversion_error(
            "undefined price (UNDEF_PRICE sentinel) in OHLCV record".to_string(),
        ));
    }

    let open_time = nanos_to_datetime(msg.hd.ts_event)
        .map_err(|e| ohlcv_conversion_error(format!("OHLCV ts_event: {e}")))?;

    let close_time = close_time_from_open(open_time, interval.to_step()).ok_or_else(|| {
        ohlcv_conversion_error(format!(
            "OHLCV close_time overflow: open={open_time}, interval={}",
            interval.as_str()
        ))
    })?;

    Ok((
        close_time,
        Candle {
            close_time,
            open: Decimal::new(msg.open, 9),
            high: Decimal::new(msg.high, 9),
            low: Decimal::new(msg.low, 9),
            close: Decimal::new(msg.close, 9),
            volume: Decimal::from(msg.volume),
            // OhlcvMsg has no trade-count field; report 0 rather than fabricate (G21.2).
            trade_count: 0,
        },
    ))
}

/// Map a [`CandleInterval`] to the Databento OHLCV [`Schema`] that produces it,
/// rejecting intervals Databento does not natively offer.
///
/// Databento's native OHLCV schemas cover only `1s`/`1m`/`1h`/`1d` (plus
/// session-based `ohlcv-eod`, which has no [`CandleInterval`] equivalent and is
/// out of scope). The shared [`CandleInterval`] is the venue-agnostic union, so
/// the unsupported variants must be rejected before they reach the API.
///
/// The match is intentionally exhaustive (no `_` arm): adding a [`CandleInterval`]
/// variant is a compile error here, forcing a conscious decision on whether
/// Databento can serve it rather than silently passing it through.
///
/// Returning the [`Schema`] (not `()`) lets the historical caller thread the
/// derived schema straight into its request, so interval and schema cannot
/// diverge.
///
/// # Errors
///
/// Returns [`DataError::UnsupportedInterval`] for the 12 intervals Databento does
/// not serve.
pub fn ensure_databento_ohlcv_supports(
    exchange: ExchangeId,
    interval: CandleInterval,
) -> Result<Schema, DataError> {
    match interval {
        CandleInterval::Sec1 => Ok(Schema::Ohlcv1S),
        CandleInterval::Min1 => Ok(Schema::Ohlcv1M),
        CandleInterval::Hour1 => Ok(Schema::Ohlcv1H),
        CandleInterval::Day1 => Ok(Schema::Ohlcv1D),
        CandleInterval::Min3
        | CandleInterval::Min5
        | CandleInterval::Min15
        | CandleInterval::Min30
        | CandleInterval::Hour2
        | CandleInterval::Hour4
        | CandleInterval::Hour6
        | CandleInterval::Hour8
        | CandleInterval::Hour12
        | CandleInterval::Day3
        | CandleInterval::Week1
        | CandleInterval::Month1 => Err(DataError::UnsupportedInterval { exchange, interval }),
    }
}

/// Derive the [`CandleInterval`] for an OHLCV record from its `rtype` discriminant.
///
/// A single Databento live connection can interleave multiple OHLCV schemas (e.g.
/// `1m` and `1h`) on one stream, so the live transform must read each record's
/// interval from its own `rtype`, not from a connection-wide subscription value.
///
/// Returns `None` for rtypes with no [`CandleInterval`] equivalent —
/// `ohlcv-eod` (session-close daily) and the deprecated `ohlcv-deprecated` — and
/// for any non-OHLCV rtype. Callers skip `None` observably rather than panicking.
#[must_use]
pub fn rtype_to_candle_interval(rtype: u8) -> Option<CandleInterval> {
    use databento::dbn::enums::rtype;
    match rtype {
        rtype::OHLCV_1S => Some(CandleInterval::Sec1),
        rtype::OHLCV_1M => Some(CandleInterval::Min1),
        rtype::OHLCV_1H => Some(CandleInterval::Hour1),
        rtype::OHLCV_1D => Some(CandleInterval::Day1),
        _ => None,
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_trade_conversion() {
        use rust_decimal_macros::dec;

        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 150_250_000_000;
        trade.size = 100;
        trade.side = b'B' as i8;
        trade.sequence = 12345;

        let (time, public_trade) = dbn_trade_to_public_trade(&trade).unwrap();

        assert_eq!(public_trade.price, dec!(150.25));
        assert_eq!(public_trade.amount, dec!(100));
        assert_eq!(public_trade.side, Some(Side::Buy));
        assert_eq!(public_trade.id.as_str(), "12345");
        assert_eq!(time.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_trade_sell_side() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 100_000_000_000;
        trade.size = 50;
        trade.side = b'A' as i8;
        trade.sequence = 1;

        let (_, public_trade) = dbn_trade_to_public_trade(&trade).unwrap();
        assert_eq!(public_trade.side, Some(Side::Sell));
    }

    #[test]
    fn test_trade_no_side() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 100_000_000_000;
        trade.size = 10;
        trade.side = b'N' as i8;
        trade.sequence = 1;

        let (_, public_trade) = dbn_trade_to_public_trade(&trade).unwrap();
        assert!(public_trade.side.is_none());
    }

    #[test]
    fn test_trade_invalid_side_rejected() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 100_000_000_000;
        trade.size = 10;
        trade.side = b'X' as i8;

        assert!(dbn_trade_to_public_trade(&trade).is_err());
    }

    #[test]
    fn test_trade_undefined_price() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = i64::MAX;

        assert!(dbn_trade_to_public_trade(&trade).is_err());
    }

    #[test]
    fn test_quote_conversion() {
        use rust_decimal_macros::dec;

        let mut msg = Mbp1Msg::default();
        msg.hd.ts_event = 1_700_000_000_000_000_000;
        msg.levels[0].bid_px = 100_000_000_000;
        msg.levels[0].ask_px = 100_500_000_000;
        msg.levels[0].bid_sz = 1000;
        msg.levels[0].ask_sz = 500;

        let (time, quote) = dbn_mbp1_to_quote(&msg).unwrap();

        assert_eq!(quote.bid_price, dec!(100));
        assert_eq!(quote.ask_price, dec!(100.5));
        assert_eq!(quote.bid_amount, dec!(1000));
        assert_eq!(quote.ask_amount, dec!(500));
        assert_eq!(time.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_orderbook_l1_conversion() {
        let mut msg = Mbp1Msg::default();
        msg.hd.ts_event = 1_700_000_000_000_000_000;
        msg.levels[0].bid_px = 100_000_000_000;
        msg.levels[0].ask_px = 100_500_000_000;
        msg.levels[0].bid_sz = 1000;
        msg.levels[0].ask_sz = 500;

        let (time, l1) = dbn_mbp1_to_orderbook_l1(&msg).unwrap();

        let best_bid = l1.best_bid.unwrap();
        let best_ask = l1.best_ask.unwrap();

        assert_eq!(best_bid.price, Decimal::from(100));
        assert_eq!(best_ask.price, Decimal::from_str("100.5").unwrap());
        assert_eq!(best_bid.amount, Decimal::from(1000));
        assert_eq!(best_ask.amount, Decimal::from(500));
        assert_eq!(time.timestamp(), 1_700_000_000);
        assert_eq!(l1.last_update_time.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_orderbook_l1_undefined_prices() {
        let mut msg = Mbp1Msg::default();
        msg.hd.ts_event = 1_700_000_000_000_000_000;
        msg.levels[0].bid_px = i64::MAX; // UNDEF_PRICE
        msg.levels[0].ask_px = i64::MAX; // UNDEF_PRICE
        msg.levels[0].bid_sz = 1000;
        msg.levels[0].ask_sz = 500;

        let (_, l1) = dbn_mbp1_to_orderbook_l1(&msg).unwrap();

        assert!(l1.best_bid.is_none());
        assert!(l1.best_ask.is_none());
    }

    // --- OHLCV / candle transforms (TG21) ---

    /// Build an `OhlcvMsg` with the given open-instant (`ts_event`) and OHLCV
    /// values in DBN fixed-point (1e-9) / integer-volume units.
    fn ohlcv(ts_event: u64, open: i64, high: i64, low: i64, close: i64, volume: u64) -> OhlcvMsg {
        // OhlcvMsg has no `Default` (it maps to multiple rtypes); seed from a
        // schema. `dbn_ohlcv_to_candle` ignores the record's rtype (the interval
        // is passed explicitly), so the seed schema does not constrain the test.
        let mut msg = OhlcvMsg::default_for_schema(Schema::Ohlcv1M);
        msg.hd.ts_event = ts_event;
        msg.open = open;
        msg.high = high;
        msg.low = low;
        msg.close = close;
        msg.volume = volume;
        msg
    }

    #[test]
    fn ohlcv_to_candle_normalises_close_time_from_open() {
        use rust_decimal_macros::dec;

        // open = 2024-01-01 00:00:00 UTC. A 1-minute bar must close at open + 60s.
        let msg = ohlcv(
            1_704_067_200_000_000_000,
            45_000_500_000_000, // 45000.5
            45_500_000_000_000, // 45500.0
            44_800_000_000_000, // 44800.0
            45_250_000_000_000, // 45250.0
            1234,
        );

        let (time_exchange, candle) = dbn_ohlcv_to_candle(&msg, CandleInterval::Min1).unwrap();

        // close_time = open + 1m, and time_exchange == close_time (period END).
        assert_eq!(candle.close_time.timestamp(), 1_704_067_260);
        assert_eq!(time_exchange, candle.close_time);
        // Fixed-point 1e-9 decode is lossless.
        assert_eq!(candle.open, dec!(45000.5));
        assert_eq!(candle.high, dec!(45500.0));
        assert_eq!(candle.low, dec!(44800.0));
        assert_eq!(candle.close, dec!(45250.0));
        // Volume is an integer count, not fixed-point.
        assert_eq!(candle.volume, dec!(1234));
        // OhlcvMsg carries no trade-count (G21.2).
        assert_eq!(candle.trade_count, 0);
    }

    #[test]
    fn ohlcv_to_candle_close_time_per_interval() {
        let open_ns = 1_704_067_200_000_000_000; // 2024-01-01 00:00:00 UTC
        let cases = [
            (CandleInterval::Sec1, 1_704_067_201),  // +1s
            (CandleInterval::Min1, 1_704_067_260),  // +60s
            (CandleInterval::Hour1, 1_704_070_800), // +3600s
            (CandleInterval::Day1, 1_704_153_600),  // +86400s
        ];
        for (interval, expected_close_secs) in cases {
            let msg = ohlcv(
                open_ns,
                1_000_000_000,
                1_000_000_000,
                1_000_000_000,
                1_000_000_000,
                1,
            );
            let (_, candle) = dbn_ohlcv_to_candle(&msg, interval).unwrap();
            assert_eq!(
                candle.close_time.timestamp(),
                expected_close_secs,
                "interval {interval}"
            );
        }
    }

    #[test]
    fn ohlcv_to_candle_never_overflows_for_max_u64_ts_event() {
        // The conversion's overflow arms (ts_event unrepresentable / close_time
        // overflow) are defensively unreachable given the `u64`-nanosecond input
        // domain: u64::MAX ns is only ~year 2554, far inside chrono's range even
        // after adding the largest native step (1 day). This documents that no
        // OhlcvMsg fixture can exercise the error path — the path exists to
        // *surface* (never skip) a failure if DBN ever widens the field, per the
        // close_time contract (21.2). The structural "return DataError, propagate"
        // guarantee is enforced by the signature, not reachable via fixture.
        let msg = ohlcv(u64::MAX, 1, 1, 1, 1, 1);
        let (time_exchange, candle) = dbn_ohlcv_to_candle(&msg, CandleInterval::Day1).unwrap();
        assert_eq!(time_exchange, candle.close_time);
    }

    #[test]
    fn ohlcv_to_candle_rejects_undef_price_in_any_field() {
        // A bar should never carry UNDEF_PRICE, but if one does, the conversion
        // surfaces a DataError rather than decoding i64::MAX into a garbage ~9.2e9
        // price — mirroring the trade/quote UNDEF_PRICE guards in this module.
        let open_ns = 1_704_067_200_000_000_000; // 2024-01-01 00:00:00 UTC
        for field in 0..4 {
            let mut vals = [1_000_000_000_i64; 4];
            vals[field] = UNDEF_PRICE;
            let msg = ohlcv(open_ns, vals[0], vals[1], vals[2], vals[3], 1);
            assert!(
                dbn_ohlcv_to_candle(&msg, CandleInterval::Min1).is_err(),
                "expected UNDEF_PRICE in field {field} to be rejected"
            );
        }
        // A fully-defined bar still converts.
        let msg = ohlcv(open_ns, 1, 1, 1, 1, 1);
        assert!(dbn_ohlcv_to_candle(&msg, CandleInterval::Min1).is_ok());
    }

    #[test]
    fn ensure_databento_ohlcv_supports_maps_native_intervals() {
        let ex = ExchangeId::DatabentoGlbx;
        assert_eq!(
            ensure_databento_ohlcv_supports(ex, CandleInterval::Sec1).unwrap(),
            Schema::Ohlcv1S
        );
        assert_eq!(
            ensure_databento_ohlcv_supports(ex, CandleInterval::Min1).unwrap(),
            Schema::Ohlcv1M
        );
        assert_eq!(
            ensure_databento_ohlcv_supports(ex, CandleInterval::Hour1).unwrap(),
            Schema::Ohlcv1H
        );
        assert_eq!(
            ensure_databento_ohlcv_supports(ex, CandleInterval::Day1).unwrap(),
            Schema::Ohlcv1D
        );
    }

    #[test]
    fn ensure_databento_ohlcv_supports_rejects_non_native_intervals() {
        let ex = ExchangeId::DatabentoGlbx;
        for interval in CandleInterval::ALL {
            let native = matches!(
                interval,
                CandleInterval::Sec1
                    | CandleInterval::Min1
                    | CandleInterval::Hour1
                    | CandleInterval::Day1
            );
            let result = ensure_databento_ohlcv_supports(ex, interval);
            assert_eq!(result.is_ok(), native, "interval {interval}");
            if !native {
                assert!(
                    matches!(
                        result,
                        Err(DataError::UnsupportedInterval { interval: i, .. }) if i == interval
                    ),
                    "expected UnsupportedInterval for {interval}"
                );
            }
        }
    }

    #[test]
    fn rtype_to_candle_interval_maps_native_and_skips_others() {
        use databento::dbn::enums::rtype;
        assert_eq!(
            rtype_to_candle_interval(rtype::OHLCV_1S),
            Some(CandleInterval::Sec1)
        );
        assert_eq!(
            rtype_to_candle_interval(rtype::OHLCV_1M),
            Some(CandleInterval::Min1)
        );
        assert_eq!(
            rtype_to_candle_interval(rtype::OHLCV_1H),
            Some(CandleInterval::Hour1)
        );
        assert_eq!(
            rtype_to_candle_interval(rtype::OHLCV_1D),
            Some(CandleInterval::Day1)
        );
        // No CandleInterval equivalent => graceful skip (None), not panic.
        assert_eq!(rtype_to_candle_interval(rtype::OHLCV_EOD), None);
        // ohlcv-deprecated rtype (0x11) — referenced by literal to avoid the
        // deprecated `RType::OhlcvDeprecated`/`OHLCV_DEPRECATED` symbols.
        assert_eq!(rtype_to_candle_interval(0x11), None);
        // A non-OHLCV rtype is also None.
        assert_eq!(rtype_to_candle_interval(rtype::MBP_1), None);
    }
}
