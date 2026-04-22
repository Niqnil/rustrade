//! Trade tick transformation for IB tick-by-tick data.
//!
//! Transforms IB's `realtime::Trade` into barter's [`PublicTrade`].
//!
//! # Side Inference
//!
//! IB tick-by-tick trades do not include trade side (buyer/seller initiated).
//! This implementation defaults to `Side::Buy`. For more accurate side
//! inference, consumers can compare trade price to concurrent bid/ask quotes.

use crate::subscription::trade::PublicTrade;
use barter_instrument::Side;
use chrono::{DateTime, Utc};
use ibapi::market_data::realtime::Trade;
use smol_str::{SmolStr, format_smolstr};
use std::hash::{Hash, Hasher};
use tracing::warn;

/// Convert an IB trade to a PublicTrade.
///
/// # Side Inference
///
/// Trade side is not provided by IB. This function defaults to `Side::Buy`.
/// For more accurate inference, compare the trade price to the current
/// bid/ask spread.
///
/// # Returns
///
/// Returns `None` if price or size is NaN/Inf (invalid trade data from IB).
pub fn from_ib_trade(trade: &Trade) -> Option<PublicTrade> {
    if !trade.price.is_finite() {
        warn!(
            price = trade.price,
            "IB trade has non-finite price, skipping"
        );
        return None;
    }
    if !trade.size.is_finite() {
        warn!(size = trade.size, "IB trade has non-finite size, skipping");
        return None;
    }

    Some(PublicTrade {
        id: generate_trade_id(trade),
        price: trade.price,
        amount: trade.size,
        side: Side::Buy,
    })
}

/// Parse the trade timestamp.
///
/// Falls back to current time if the timestamp is invalid, with a warning.
pub fn parse_trade_time(trade: &Trade) -> DateTime<Utc> {
    let unix_ts = trade.time.unix_timestamp();
    DateTime::from_timestamp(unix_ts, 0).unwrap_or_else(|| {
        warn!(
            unix_timestamp = unix_ts,
            "Invalid trade timestamp from IB, using current time"
        );
        Utc::now()
    })
}

/// Generate a unique trade ID from trade data.
///
/// IB doesn't provide trade IDs for tick-by-tick data, so we generate one
/// from a hash of time + price + size. Returns [`SmolStr`] which stores the
/// 16-char hex ID inline (no heap allocation).
///
/// # Collision Risk
///
/// IB's tick-by-tick API provides only second-resolution timestamps (Unix
/// seconds). We hash using nanoseconds for forward-compatibility, but since
/// IB populates only seconds, the nanosecond component is effectively zero.
/// Trades in the same second with identical price and size will produce the
/// same ID.
fn generate_trade_id(trade: &Trade) -> SmolStr {
    let mut hasher = fnv::FnvHasher::default();
    trade.time.unix_timestamp_nanos().hash(&mut hasher);
    trade.price.to_bits().hash(&mut hasher);
    trade.size.to_bits().hash(&mut hasher);
    format_smolstr!("{:016x}", hasher.finish())
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use ibapi::market_data::realtime::TradeAttribute;

    fn make_offset_datetime(unix_time: i64) -> ::time::OffsetDateTime {
        ::time::OffsetDateTime::from_unix_timestamp(unix_time)
            .unwrap_or(::time::OffsetDateTime::UNIX_EPOCH)
    }

    fn make_trade(unix_time: i64, price: f64, size: f64) -> Trade {
        Trade {
            tick_type: "Last".to_string(),
            time: make_offset_datetime(unix_time),
            price,
            size,
            trade_attribute: TradeAttribute {
                past_limit: false,
                unreported: false,
            },
            exchange: String::new(),
            special_conditions: String::new(),
        }
    }

    #[test]
    fn converts_trade_fields() {
        let ib_trade = make_trade(1700000000, 150.25, 100.0);
        let trade = from_ib_trade(&ib_trade).unwrap();

        assert_eq!(trade.price, 150.25);
        assert_eq!(trade.amount, 100.0);
        assert_eq!(trade.side, Side::Buy);
        assert!(!trade.id.is_empty());
    }

    #[test]
    fn rejects_non_finite_price() {
        let trade = make_trade(1700000000, f64::NAN, 100.0);
        assert!(from_ib_trade(&trade).is_none());

        let trade = make_trade(1700000000, f64::INFINITY, 100.0);
        assert!(from_ib_trade(&trade).is_none());
    }

    #[test]
    fn rejects_non_finite_size() {
        let trade = make_trade(1700000000, 100.0, f64::NAN);
        assert!(from_ib_trade(&trade).is_none());

        let trade = make_trade(1700000000, 100.0, f64::INFINITY);
        assert!(from_ib_trade(&trade).is_none());
    }

    #[test]
    fn generates_unique_ids() {
        let trade1 = make_trade(1700000000, 150.25, 100.0);
        let trade2 = make_trade(1700000001, 150.25, 100.0);
        let trade3 = make_trade(1700000000, 150.26, 100.0);

        let id1 = generate_trade_id(&trade1);
        let id2 = generate_trade_id(&trade2);
        let id3 = generate_trade_id(&trade3);

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id2, id3);
    }

    #[test]
    fn same_trade_same_id() {
        let trade1 = make_trade(1700000000, 150.25, 100.0);
        let trade2 = make_trade(1700000000, 150.25, 100.0);

        assert_eq!(generate_trade_id(&trade1), generate_trade_id(&trade2));
    }

    #[test]
    fn parses_valid_timestamp() {
        let trade = make_trade(1700000000, 100.0, 10.0);
        let time = parse_trade_time(&trade);

        assert_eq!(time.timestamp(), 1700000000);
    }
}
