use crate::{
    order::id::{ClientOrderId, StrategyId},
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use fnv::FnvHashMap;
use ibapi::orders::{CommissionReport, ExecutionData};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::{cell::RefCell, sync::Arc};
use tracing::warn;

// Thread-local cache for parsed IANA timezones. IB uses per-exchange timezones,
// but most portfolios only see a handful (US/Eastern, Europe/London, etc.).
//
// Note: This cache is per-thread. In the dedicated `ibkr-order-stream` thread
// spawned by `account_stream`, the cache is fully effective. In `spawn_blocking`
// contexts (e.g., `fetch_trades`), Tokio's thread pool means each pool thread
// maintains its own cache — still beneficial but less effective than a single
// dedicated thread.
thread_local! {
    static TZ_CACHE: RefCell<FnvHashMap<SmolStr, Tz>> = RefCell::new(FnvHashMap::default());
}

/// Buffers IB executions until their commission reports arrive.
///
/// IB sends `ExecutionData` and `CommissionReport` as separate events.
/// This buffer holds executions until we can match them with commissions
/// to produce complete `Trade` events.
#[derive(Debug, Clone)]
pub struct ExecutionBuffer {
    inner: Arc<Mutex<ExecutionBufferInner>>,
}

#[derive(Debug, Default)]
struct ExecutionBufferInner {
    pending: FnvHashMap<String, PendingExecution>,
}

#[derive(Debug, Clone)]
struct PendingExecution {
    execution: ExecutionData,
    instrument: InstrumentNameExchange,
    client_order_id: ClientOrderId,
}

impl ExecutionBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExecutionBufferInner::default())),
        }
    }

    /// Buffer an execution, waiting for its commission report.
    pub fn add_execution(
        &self,
        execution: ExecutionData,
        instrument: InstrumentNameExchange,
        client_order_id: ClientOrderId,
    ) {
        let exec_id = execution.execution.execution_id.clone();
        let mut inner = self.inner.lock();
        inner.pending.insert(
            exec_id,
            PendingExecution {
                execution,
                instrument,
                client_order_id,
            },
        );

        // Warn if buffer is growing unexpectedly large (possible commission report leak)
        let pending_count = inner.pending.len();
        if pending_count > 1000 && pending_count.is_multiple_of(100) {
            warn!(
                pending_count,
                "ExecutionBuffer has >1000 pending entries; commission reports may be delayed or lost"
            );
        }
    }

    /// Try to complete a trade with a commission report.
    /// Returns the completed Trade if the matching execution was buffered.
    pub fn complete_with_commission(
        &self,
        report: &CommissionReport,
    ) -> Option<Trade<AssetNameExchange, InstrumentNameExchange>> {
        let pending = {
            let mut inner = self.inner.lock();
            inner.pending.remove(&report.execution_id)?
        };

        build_trade(pending, report)
    }

    /// Get number of pending executions (for diagnostics).
    pub fn pending_count(&self) -> usize {
        self.inner.lock().pending.len()
    }

    /// Clear stale executions older than the given duration.
    ///
    /// Returns number of cleared entries.
    ///
    /// # Caller Responsibility
    ///
    /// This method is not called automatically. Callers should invoke it
    /// periodically to prevent unbounded growth if commission reports are
    /// delayed or lost.
    pub fn clear_stale(&self, max_age: std::time::Duration) -> usize {
        let now = Utc::now();
        let mut inner = self.inner.lock();
        let before = inner.pending.len();

        let max_age_secs = i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX);
        inner.pending.retain(|_, pending| {
            if let Some(exec_time) = parse_ib_timestamp(&pending.execution.execution.time) {
                let age = now.signed_duration_since(exec_time);
                age.num_seconds() < max_age_secs
            } else {
                // Evict entries with unparseable timestamps to prevent memory leak
                false
            }
        });

        before - inner.pending.len()
    }
}

impl Default for ExecutionBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a barter Trade from IB execution + commission data.
///
/// Returns `None` if the side string is unrecognized (logged as warning).
fn build_trade(
    pending: PendingExecution,
    commission: &CommissionReport,
) -> Option<Trade<AssetNameExchange, InstrumentNameExchange>> {
    let exec = &pending.execution.execution;

    let side = match parse_ib_side(&exec.side) {
        Some(s) => s,
        None => {
            warn!(
                side = %exec.side,
                exec_id = %exec.execution_id,
                "Unknown IB side string, dropping trade"
            );
            return None;
        }
    };

    let price = parse_decimal_or_warn(exec.price, "exec.price");
    let quantity = parse_decimal_or_warn(exec.shares, "exec.shares");
    let commission_amount = parse_decimal_or_warn(commission.commission, "commission");

    let time_exchange = parse_ib_timestamp(&exec.time).unwrap_or_else(Utc::now);

    Some(Trade {
        id: TradeId::new(&exec.execution_id),
        order_id: crate::order::id::OrderId::new(&pending.client_order_id.0),
        instrument: pending.instrument,
        strategy: StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        fees: AssetFees {
            asset: AssetNameExchange::from(commission.currency.as_str()),
            fees: commission_amount,
            fees_quote: None, // Indexer computes based on fee asset vs instrument quote
        },
    })
}

/// Parse IB side string to barter Side.
///
/// IB sends uppercase: "BOT" (bought), "SLD" (sold), "BUY", "SELL",
/// "SSHORT" (short sell), "SLONG" (sell long).
///
/// Returns `None` for unrecognized values to avoid silent data corruption.
pub fn parse_ib_side(s: &str) -> Option<Side> {
    match s {
        "BOT" | "BUY" => Some(Side::Buy),
        "SLD" | "SELL" | "SSHORT" | "SLONG" => Some(Side::Sell),
        _ => None,
    }
}

/// Convert f64 to Decimal, logging a warning if conversion fails (NaN/Inf).
///
/// Returns `Decimal::ZERO` for invalid values. This is acceptable because:
/// - IB's API should never return NaN/Inf for prices, quantities, or commissions
/// - If it does, something is fundamentally broken and the warning log surfaces it
/// - Callers processing trades in bulk shouldn't abort on one corrupted record
///
/// For stricter handling, callers can check for zero in critical fields.
pub fn parse_decimal_or_warn(value: f64, field_name: &str) -> Decimal {
    Decimal::try_from(value).unwrap_or_else(|e| {
        warn!(field = %field_name, value = %value, error = %e, "Invalid f64 for Decimal, using zero");
        Decimal::ZERO
    })
}

/// Parse IB timestamp format (YYYYMMDD HH:MM:SS timezone).
///
/// IB sends timestamps like "20250418 10:30:00 US/Eastern". This function
/// parses the timezone and converts to UTC.
///
/// # Fallback Behavior
///
/// - Unknown timezone string: treats as UTC (logs warning)
/// - DST-ambiguous time (during "fall back" transition): returns `None`, caller
///   typically falls back to `Utc::now()`. This affects ~1 second per timezone
///   per year and is unlikely to occur in practice.
pub fn parse_ib_timestamp(s: &str) -> Option<DateTime<Utc>> {
    // Use iterator to avoid Vec allocation
    let mut parts = s.split_whitespace();
    let date_part = parts.next()?;
    let time_part = parts.next()?;
    let tz_part = parts.next();

    // Find the space between date and time by byte offset to avoid format! allocation
    let datetime_end = date_part.len() + 1 + time_part.len();
    let datetime_str = &s[..datetime_end.min(s.len())];

    let naive = NaiveDateTime::parse_from_str(datetime_str, "%Y%m%d %H:%M:%S").ok()?;

    // Try to parse timezone, fall back to UTC
    if let Some(tz_str) = tz_part {
        // Use cached timezone to avoid re-parsing IANA database on every call
        let tz_opt = TZ_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(tz) = cache.get(tz_str) {
                return Some(*tz);
            }
            if let Ok(tz) = tz_str.parse::<Tz>() {
                cache.insert(SmolStr::new(tz_str), tz);
                return Some(tz);
            }
            None
        });

        if let Some(tz) = tz_opt {
            return tz
                .from_local_datetime(&naive)
                .single()
                .map(|dt| dt.with_timezone(&Utc));
        }
        warn!(timezone = %tz_str, "Unknown timezone in IB timestamp, treating as UTC");
    }

    Some(naive.and_utc())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_parse_ib_timestamp_with_timezone() {
        // US/Eastern is UTC-4 during daylight saving time (April)
        let ts = parse_ib_timestamp("20250418 10:30:00 US/Eastern");
        assert!(ts.is_some());
        let dt = ts.unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 4);
        assert_eq!(dt.day(), 18);
        // 10:30 Eastern = 14:30 UTC (EDT is UTC-4)
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn test_parse_ib_timestamp_no_timezone() {
        // Without timezone, treat as UTC
        let ts = parse_ib_timestamp("20250418 10:30:00");
        assert!(ts.is_some());
        let dt = ts.unwrap();
        assert_eq!(dt.hour(), 10);
    }

    #[test]
    fn test_parse_ib_timestamp_invalid() {
        assert!(parse_ib_timestamp("invalid").is_none());
        assert!(parse_ib_timestamp("").is_none());
    }

    #[test]
    fn test_parse_ib_side() {
        assert_eq!(parse_ib_side("BOT"), Some(Side::Buy));
        assert_eq!(parse_ib_side("BUY"), Some(Side::Buy));
        assert_eq!(parse_ib_side("SLD"), Some(Side::Sell));
        assert_eq!(parse_ib_side("SELL"), Some(Side::Sell));
        assert_eq!(parse_ib_side("SSHORT"), Some(Side::Sell));
        assert_eq!(parse_ib_side("SLONG"), Some(Side::Sell));
        // Unknown returns None
        assert_eq!(parse_ib_side("UNKNOWN"), None);
    }
}
