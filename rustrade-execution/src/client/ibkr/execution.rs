use crate::{
    order::id::{OrderId, StrategyId},
    trade::{AssetFees, Trade, TradeId},
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use fnv::FnvHashMap;
use ibapi::orders::{CommissionReport, ExecutionData, ExecutionSide};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use smol_str::{SmolStr, format_smolstr};
use std::{cell::RefCell, collections::hash_map::Entry, sync::Arc};
use tracing::{debug, warn};

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
}

impl ExecutionBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExecutionBufferInner::default())),
        }
    }

    /// Buffer an execution, waiting for its commission report.
    pub fn add_execution(&self, execution: ExecutionData, instrument: InstrumentNameExchange) {
        let exec_id = execution.execution.execution_id.clone();
        let mut inner = self.inner.lock();
        inner.pending.insert(
            exec_id,
            PendingExecution {
                execution,
                instrument,
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

        Some(build_trade(pending, report))
    }

    /// Take every pending execution out of the buffer as a trade whose fee is unknown: zero, in
    /// [`UNKNOWN_FEE_ASSET`].
    ///
    /// For a read that has ended, such as one executions request, whose commission reports will
    /// not arrive in it any more.
    pub(super) fn take_without_commission(
        &self,
    ) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
        std::mem::take(&mut self.inner.lock().pending)
            .into_values()
            .map(|pending| {
                let fees = AssetFees::new(
                    AssetNameExchange::from(UNKNOWN_FEE_ASSET),
                    Decimal::ZERO,
                    None,
                );
                trade_with_fees(pending, fees)
            })
            .collect()
    }

    /// Move every pending execution into `target`, returning how many moved.
    ///
    /// Fill recovery buffers what it reads in a buffer of its own, then hands over whatever is
    /// still waiting for a commission report, so a report that reaches the account stream later
    /// can complete it there.
    ///
    /// Both buffers stay locked for the move, so a commission report the stream reads meanwhile
    /// finds the execution in one of them. `target` is locked first. The stream locks only its own
    /// buffer, so the order cannot deadlock against it.
    pub(super) fn drain_into(&self, target: &ExecutionBuffer) -> usize {
        let mut target = target.inner.lock();
        let drained = std::mem::take(&mut self.inner.lock().pending);
        let count = drained.len();
        target.pending.extend(drained);
        count
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

/// An IB execution id, read as the execution it reports and that execution's revision.
///
/// IB reports a correction as a further execution whose id differs from the one it corrects only
/// in the digits after the final period: `0000e0d5.5f8b1c2a.01.02` corrects
/// `0000e0d5.5f8b1c2a.01.01`. IB's documentation gives that example rather than a rule, but every
/// execution IB first reports ends in `01`, so a higher revision is read as a correction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExecutionRevision<'a> {
    /// The id up to its final period, which every revision of the execution shares.
    pub(super) execution: &'a str,
    /// The digits after the final period, kept to write the previous revision's id at the same
    /// width.
    digits: &'a str,
    pub(super) revision: u32,
}

impl<'a> ExecutionRevision<'a> {
    /// `None` when `execution_id` does not end in a period followed by digits. Such an id is read
    /// as an execution that has not been corrected.
    pub(super) fn parse(execution_id: &'a str) -> Option<Self> {
        let (execution, digits) = execution_id.rsplit_once('.')?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(Self {
            execution,
            digits,
            revision: digits.parse().ok()?,
        })
    }

    /// Whether this revision corrects an earlier one.
    pub(super) fn is_correction(&self) -> bool {
        self.revision > 1
    }

    /// The id of the revision this one corrects, or `None` if it is not a correction.
    pub(super) fn previous_id(&self) -> Option<TradeId> {
        self.is_correction().then(|| {
            TradeId(format_smolstr!(
                "{}.{:0width$}",
                self.execution,
                self.revision - 1,
                width = self.digits.len()
            ))
        })
    }
}

/// The revision of the execution `id` names, `0` when it names none. Sorting by it puts each
/// execution ahead of its corrections.
pub(super) fn revision_of(id: &TradeId) -> u32 {
    ExecutionRevision::parse(&id.0).map_or(0, |revision| revision.revision)
}

/// `trades` with each execution only at its latest revision, in the order the executions first
/// appear, so that a corrected execution is read as corrected rather than twice. An execution
/// read twice at one revision is kept once.
pub(super) fn keep_latest_revisions(
    trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
    // The kept position and revision of each execution with a revision.
    let mut latest = FnvHashMap::<SmolStr, (usize, u32)>::with_capacity_and_hasher(
        trades.len(),
        Default::default(),
    );
    let mut kept = Vec::with_capacity(trades.len());
    for trade in trades {
        let Some(revision) = ExecutionRevision::parse(&trade.id.0) else {
            kept.push(trade);
            continue;
        };
        let (execution, revision) = (SmolStr::new(revision.execution), revision.revision);
        match latest.entry(execution) {
            Entry::Vacant(entry) => {
                entry.insert((kept.len(), revision));
                kept.push(trade);
            }
            Entry::Occupied(mut entry) => {
                let (position, kept_revision) = *entry.get();
                if revision == kept_revision {
                    debug!(exec_id = %trade.id, "IBKR execution read twice; keeping one");
                    continue;
                }
                let (older, newer) = if revision > kept_revision {
                    entry.insert((position, revision));
                    (
                        std::mem::replace(&mut kept[position], trade),
                        &kept[position],
                    )
                } else {
                    (trade, &kept[position])
                };
                debug!(
                    corrected = %older.id,
                    correction = %newer.id,
                    "IBKR execution corrected; keeping the correction"
                );
            }
        }
    }
    kept
}

/// The fee asset of a trade whose commission report IB did not send.
pub const UNKNOWN_FEE_ASSET: &str = "UNKNOWN";

/// The venue order id of IB order `ib_order_id`, as [`Open`](crate::order::state::Open) and every
/// [`Trade`] of this client carry it.
pub(super) fn ib_order_id(ib_order_id: i32) -> OrderId {
    OrderId::new(format_smolstr!("{ib_order_id}"))
}

/// Build a rustrade Trade from IB execution + commission data.
fn build_trade(
    pending: PendingExecution,
    commission: &CommissionReport,
) -> Trade<AssetNameExchange, InstrumentNameExchange> {
    let commission_amount = parse_decimal_or_warn(commission.commission, "commission");
    let fees = AssetFees {
        asset: AssetNameExchange::from(commission.currency.as_str()),
        fees: commission_amount,
        fees_quote: None, // Indexer computes based on fee asset vs instrument quote
    };
    trade_with_fees(pending, fees)
}

/// Build a rustrade Trade from an IB execution and its fees.
fn trade_with_fees(
    pending: PendingExecution,
    fees: AssetFees<AssetNameExchange>,
) -> Trade<AssetNameExchange, InstrumentNameExchange> {
    let exec = &pending.execution.execution;

    // `ExecutionSide` is a closed two-variant enum in ibapi 3.x (the decoder
    // rejects unknown wire values upstream), so the mapping is total.
    let side = match exec.side {
        ExecutionSide::Bought => Side::Buy,
        ExecutionSide::Sold => Side::Sell,
    };

    let price = parse_decimal_or_warn(exec.price, "exec.price");
    let quantity = parse_decimal_or_warn(exec.shares, "exec.shares");

    let time_exchange = parse_ib_timestamp(&exec.time).unwrap_or_else(Utc::now);

    Trade {
        id: TradeId::new(&exec.execution_id),
        // The id the order's `Open` state carries, which is what a fill is matched against.
        order_id: ib_order_id(exec.order_id),
        instrument: pending.instrument,
        strategy: StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        // IB reports the order's running total on every execution, so the order advances from the
        // fill itself rather than waiting on the next `OrderStatus`.
        order_filled_quantity: Some(parse_decimal_or_warn(
            exec.cumulative_quantity,
            "exec.cumulative_quantity",
        )),
        fees,
    }
}

/// Convert f64 to Decimal, logging a warning if conversion fails (NaN/Inf).
///
/// Returns `Decimal::ZERO` for invalid values. This is acceptable because:
/// - IB's API should never return NaN/Inf for prices, quantities, or commissions
/// - If it does, something is fundamentally broken and the warning log surfaces it
/// - Callers processing trades in bulk shouldn't abort on one corrupted record
///
/// Where zero would be read as a real value, such as an ended order's fill, use
/// [`try_decimal_or_warn`] and keep it unknown instead.
pub fn parse_decimal_or_warn(value: f64, field_name: impl std::fmt::Display) -> Decimal {
    try_decimal_or_warn(value, field_name).unwrap_or(Decimal::ZERO)
}

/// Convert an IB `f64` to a `Decimal`: `None`, with a warning, when it is not a finite number
/// that fits, so that a caller can keep it unknown rather than read it as zero.
///
/// `field_name` names the value in the warning; a `format_args!` can add context, such as the
/// order, without formatting it unless the warning fires.
pub fn try_decimal_or_warn(value: f64, field_name: impl std::fmt::Display) -> Option<Decimal> {
    Decimal::try_from(value)
        .map_err(
            |e| warn!(field = %field_name, value = %value, error = %e, "Invalid f64 for Decimal"),
        )
        .ok()
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
    fn drain_into_moves_everything_and_keeps_the_target() {
        let execution = |exec_id: &str| ExecutionData {
            execution: ibapi::orders::Execution {
                execution_id: exec_id.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let add = |buffer: &ExecutionBuffer, exec_id: &str| {
            buffer.add_execution(execution(exec_id), InstrumentNameExchange::new("AAPL"));
        };
        let source = ExecutionBuffer::new();
        let target = ExecutionBuffer::new();
        add(&source, "a");
        add(&source, "b");
        add(&target, "c");

        assert_eq!(source.drain_into(&target), 2);
        assert_eq!(source.pending_count(), 0);
        assert_eq!(target.pending_count(), 3);
    }

    #[test]
    fn execution_revision_reads_the_digits_after_the_final_period() {
        let original = ExecutionRevision::parse("0000e0d5.5f8b1c2a.01.01").unwrap();
        assert_eq!(original.execution, "0000e0d5.5f8b1c2a.01");
        assert_eq!(original.revision, 1);
        assert!(!original.is_correction());
        assert_eq!(original.previous_id(), None);

        let correction = ExecutionRevision::parse("0000e0d5.5f8b1c2a.01.02").unwrap();
        assert_eq!(correction.execution, "0000e0d5.5f8b1c2a.01");
        assert!(correction.is_correction());
        assert_eq!(
            correction.previous_id(),
            Some(TradeId::new("0000e0d5.5f8b1c2a.01.01"))
        );

        // The previous revision keeps the width of the digits.
        assert_eq!(
            ExecutionRevision::parse("x.10").unwrap().previous_id(),
            Some(TradeId::new("x.09"))
        );
        assert_eq!(
            ExecutionRevision::parse("x.3").unwrap().previous_id(),
            Some(TradeId::new("x.2"))
        );
    }

    #[test]
    fn execution_id_without_a_revision_is_never_a_correction() {
        for id in ["e1", "abc.", "abc.0x", "abc.1a", "", ".", "abc.99999999999"] {
            assert_eq!(ExecutionRevision::parse(id), None, "{id:?}");
        }
        assert_eq!(revision_of(&TradeId::new("e1")), 0);
        assert_eq!(revision_of(&TradeId::new("a.b.02")), 2);
    }

    fn trade(id: &str, price: i64) -> Trade<AssetNameExchange, InstrumentNameExchange> {
        Trade {
            id: TradeId::new(id),
            order_id: crate::order::id::OrderId::new("cid"),
            instrument: InstrumentNameExchange::new("AAPL"),
            strategy: StrategyId::unknown(),
            time_exchange: DateTime::<Utc>::MIN_UTC,
            side: Side::Buy,
            price: Decimal::from(price),
            quantity: Decimal::ONE,
            order_filled_quantity: Some(Decimal::ONE),
            fees: AssetFees::new(AssetNameExchange::from("USD"), Decimal::ZERO, None),
        }
    }

    #[test]
    fn keep_latest_revisions_reads_a_corrected_execution_once() {
        let kept = keep_latest_revisions(vec![
            trade("a.01.01", 100),
            trade("b.01.01", 200),
            trade("a.01.02", 101),
            trade("a.01.02", 101),
            trade("plain", 300),
            // An earlier revision after a later one is still the earlier one.
            trade("b.01.03", 202),
            trade("b.01.02", 201),
        ]);
        let read: Vec<_> = kept
            .iter()
            .map(|trade| (trade.id.0.as_str(), trade.price))
            .collect();
        assert_eq!(
            read,
            [
                ("a.01.02", Decimal::from(101)),
                ("b.01.03", Decimal::from(202)),
                ("plain", Decimal::from(300)),
            ]
        );
    }

    #[test]
    fn test_parse_ib_timestamp_invalid() {
        assert!(parse_ib_timestamp("invalid").is_none());
        assert!(parse_ib_timestamp("").is_none());
    }
}
