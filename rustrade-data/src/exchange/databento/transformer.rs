//! DBN record to rustrade event transformers.
//!
//! Converts Databento's DBN format records into normalized rustrade events.
//!
//! # Price Format
//!
//! DBN uses fixed-point `i64` with 1e-9 scaling (9 decimal places).
//! Convert via `price as f64 / FIXED_PRICE_SCALE`.
//!
//! # Timestamp Format
//!
//! DBN timestamps are `u64` nanoseconds since UNIX epoch:
//! - `ts_event`: Exchange timestamp (used as `time_exchange`)
//! - `ts_recv`: Databento receive timestamp

use crate::subscription::{quote::Quote, trade::PublicTrade};
use chrono::{DateTime, TimeZone, Utc};
use databento::dbn::{Mbp1Msg, TradeMsg};
use rustrade_instrument::Side;
use smol_str::format_smolstr;

const FIXED_PRICE_SCALE: f64 = 1_000_000_000.0;

const UNDEF_PRICE: i64 = i64::MAX;
const UNDEF_SIZE: u32 = u32::MAX;

/// Convert a DBN TradeMsg to a PublicTrade.
///
/// Returns the exchange timestamp and the converted trade, or an error description.
pub fn dbn_trade_to_public_trade(
    trade: &TradeMsg,
) -> Result<(DateTime<Utc>, PublicTrade), &'static str> {
    if trade.price == UNDEF_PRICE {
        return Err("undefined price");
    }

    let price = trade.price as f64 / FIXED_PRICE_SCALE;
    let amount = trade.size as f64;

    let time_exchange = nanos_to_datetime(trade.hd.ts_event)?;

    // DBN guarantees Side is ASCII (range [0, 127]); i8 -> u8 cast is lossless.
    #[allow(clippy::cast_sign_loss)]
    let side = match trade.side as u8 {
        b'A' => Side::Sell,
        b'B' => Side::Buy,
        _ => return Err("unknown or undefined trade side"),
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
/// corresponding `bid_amount`/`ask_amount` is set to `0.0`. Callers cannot
/// distinguish "empty book level" from "size unavailable in feed."
pub fn dbn_mbp1_to_quote(msg: &Mbp1Msg) -> Result<(DateTime<Utc>, Quote), &'static str> {
    let [level] = &msg.levels;

    if level.bid_px == UNDEF_PRICE || level.ask_px == UNDEF_PRICE {
        return Err("undefined bid or ask price");
    }

    let bid_price = level.bid_px as f64 / FIXED_PRICE_SCALE;
    let ask_price = level.ask_px as f64 / FIXED_PRICE_SCALE;
    // UNDEF_SIZE (u32::MAX) means size unavailable; map to 0.0 (see rustdoc note)
    let bid_amount = if level.bid_sz == UNDEF_SIZE {
        0.0
    } else {
        level.bid_sz as f64
    };
    let ask_amount = if level.ask_sz == UNDEF_SIZE {
        0.0
    } else {
        level.ask_sz as f64
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

fn nanos_to_datetime(nanos: u64) -> Result<DateTime<Utc>, &'static str> {
    let secs = i64::try_from(nanos / 1_000_000_000).map_err(|_| "timestamp out of i64 range")?;
    let nsecs = (nanos % 1_000_000_000) as u32;

    Utc.timestamp_opt(secs, nsecs)
        .single()
        .ok_or("invalid timestamp")
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_conversion() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 150_250_000_000;
        trade.size = 100;
        trade.side = b'B' as i8;
        trade.sequence = 12345;

        let (time, public_trade) = dbn_trade_to_public_trade(&trade).unwrap();

        assert_eq!(public_trade.price, 150.25);
        assert_eq!(public_trade.amount, 100.0);
        assert_eq!(public_trade.side, Side::Buy);
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
        assert_eq!(public_trade.side, Side::Sell);
    }

    #[test]
    fn test_trade_unknown_side_rejected() {
        let mut trade = TradeMsg::default();
        trade.hd.ts_event = 1_700_000_000_000_000_000;
        trade.price = 100_000_000_000;
        trade.size = 10;
        trade.side = b'N' as i8;

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
        let mut msg = Mbp1Msg::default();
        msg.hd.ts_event = 1_700_000_000_000_000_000;
        msg.levels[0].bid_px = 100_000_000_000;
        msg.levels[0].ask_px = 100_500_000_000;
        msg.levels[0].bid_sz = 1000;
        msg.levels[0].ask_sz = 500;

        let (time, quote) = dbn_mbp1_to_quote(&msg).unwrap();

        assert_eq!(quote.bid_price, 100.0);
        assert_eq!(quote.ask_price, 100.5);
        assert_eq!(quote.bid_amount, 1000.0);
        assert_eq!(quote.ask_amount, 500.0);
        assert_eq!(time.timestamp(), 1_700_000_000);
    }
}
