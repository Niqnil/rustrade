//! Trade tick transformation for IB tick-by-tick data.
//!
//! Transforms IB's `realtime::Trade` into rustrade's [`PublicTrade`].
//!
//! # Side Inference
//!
//! IB tick-by-tick trades do not include trade side (buyer/seller initiated).
//! This implementation defaults to `Side::Buy`. For more accurate side
//! inference, consumers can compare trade price to concurrent bid/ask quotes.

use crate::subscription::trade::PublicTrade;
use chrono::{DateTime, Utc};
use ibapi::market_data::realtime::Trade;
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use smol_str::{SmolStr, format_smolstr};
use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::warn;

// Process-global counters aggregated across all instruments. Never reset, so
// rate-limiting persists even after instrument reconnects. The `total_bad_*`
// log field reflects cumulative errors, not per-instrument counts.
static BAD_PRICE_COUNT: AtomicU64 = AtomicU64::new(0);
static BAD_SIZE_COUNT: AtomicU64 = AtomicU64::new(0);

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
        let count = BAD_PRICE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count.is_multiple_of(1000) {
            warn!(
                price = trade.price,
                total_bad_prices = count,
                "IB trade has non-finite price, skipping"
            );
        }
        return None;
    }
    if !trade.size.is_finite() {
        let count = BAD_SIZE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count.is_multiple_of(1000) {
            warn!(
                size = trade.size,
                total_bad_sizes = count,
                "IB trade has non-finite size, skipping"
            );
        }
        return None;
    }

    // is_finite guards above handle NaN/Inf with rate-limited logging.
    // try_from is a safety net — can only fail for |x| > 7.9e28 (impossible for prices).
    let price = Decimal::try_from(trade.price).ok()?;
    let amount = Decimal::try_from(trade.size).ok()?;

    Some(PublicTrade {
        id: generate_trade_id(trade),
        price,
        amount,
        side: Side::Buy,
    })
}

/// Parse the trade timestamp.
///
/// # Arguments
///
/// * `trade` - The IB trade tick
/// * `now` - Fallback timestamp (caller's current time, avoids redundant syscalls)
///
/// # Fallback behavior
///
/// Falls back to `now` if the timestamp is out of range. In practice this is
/// unreachable: `time::OffsetDateTime` (IB's type) has range year ±9999, while
/// `chrono::DateTime<Utc>` has range year ±262143 — any valid IB timestamp
/// converts successfully. The fallback exists for defensive safety.
pub fn parse_trade_time(trade: &Trade, now: DateTime<Utc>) -> DateTime<Utc> {
    let unix_ts = trade.time.unix_timestamp();
    DateTime::from_timestamp(unix_ts, 0).unwrap_or_else(|| {
        warn!(
            unix_timestamp = unix_ts,
            "Invalid trade timestamp from IB, using current time"
        );
        now
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
        use rust_decimal_macros::dec;

        let ib_trade = make_trade(1700000000, 150.25, 100.0);
        let trade = from_ib_trade(&ib_trade).unwrap();

        assert_eq!(trade.price, dec!(150.25));
        assert_eq!(trade.amount, dec!(100));
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
        let fallback = Utc::now();
        let time = parse_trade_time(&trade, fallback);

        // Should use trade timestamp, not fallback
        assert_eq!(time.timestamp(), 1700000000);
    }

    // Note: No test for fallback path — it's unreachable. See parse_trade_time docs.
    // time::OffsetDateTime range (±9999 years) is a subset of chrono::DateTime<Utc>
    // range (±262143 years), so any valid IB timestamp always converts successfully.
}
