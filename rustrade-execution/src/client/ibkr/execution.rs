use crate::{
    order::id::{ClientOrderId, OrderId, StrategyId},
    trade::{AssetFees, TimeSource, Trade, TradeId, TradeOrigin},
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use fnv::FnvHashMap;
use ibapi::orders::{CommissionReport, Execution, ExecutionData, ExecutionFilter, ExecutionSide};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use smol_str::{SmolStr, format_smolstr};
use std::{cell::RefCell, collections::hash_map::Entry, sync::Arc, time::Duration};
use tracing::{debug, warn};

// Thread-local cache of the zones IB's times name, as resolved, or `None` for a name that is not a
// tz database name, so that one is warned of once per thread rather than on every time. IB names
// the gateway host's zone, so a gateway's times usually name one.
//
// Note: This cache is per-thread. In the dedicated `ibkr-order-stream` thread
// spawned by `account_stream`, the cache is fully effective. In `spawn_blocking`
// contexts (e.g., `fetch_trades`), Tokio's thread pool means each pool thread
// maintains its own cache — still beneficial but less effective than a single
// dedicated thread.
thread_local! {
    static TZ_CACHE: RefCell<FnvHashMap<SmolStr, Option<Tz>>> =
        RefCell::new(FnvHashMap::default());
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
    resolved: ResolvedExecution,
    /// This host's time when the execution was buffered, which ages it in
    /// [`ExecutionBuffer::clear_stale`] when its own time does not read.
    buffered_at: DateTime<Utc>,
}

/// Which API clients' executions this client reports, and how it tells its own from theirs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExecutionScope {
    /// This API client's id, which an execution of one of its orders carries.
    pub(super) api_client_id: i32,
    /// Whether other clients' executions are reported too:
    /// [`IbkrConfig::other_clients_fills`](super::IbkrConfig::other_clients_fills).
    pub(super) other_clients: bool,
}

impl ExecutionScope {
    /// The order id and origin `execution`'s trade is reported with, or `None` when it is another
    /// client's and other clients' are not reported.
    ///
    /// An execution IB flags as an IB-initiated liquidation is reported whoever's order it was,
    /// as [`TradeOrigin::Liquidation`]: it moves the account's position, which no client chose.
    /// That is why an executions request asks IB for every client's, and this decides which are
    /// reported.
    pub(super) fn classify(self, execution: &Execution) -> Option<(OrderId, TradeOrigin)> {
        let own = execution.client_id == self.api_client_id;
        let liquidation = execution.liquidation != 0;
        if !own && !liquidation && !self.other_clients {
            return None;
        }
        let origin = if liquidation {
            TradeOrigin::Liquidation
        } else if own {
            TradeOrigin::Order
        } else {
            TradeOrigin::External
        };
        let order_id = if own {
            ib_order_id(execution.order_id)
        } else {
            foreign_order_id(execution)
        };
        Some((order_id, origin))
    }
}

/// What an execution's trade is reported with, besides what the execution itself carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedExecution {
    pub(super) instrument: InstrumentNameExchange,
    pub(super) order_id: OrderId,
    pub(super) origin: TradeOrigin,
    /// The execution's time, read by [`parse_ib_timestamp`] in the gateway's session zone, or
    /// `None` where it does not read.
    pub(super) time: Option<DateTime<Utc>>,
}

impl ExecutionBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExecutionBufferInner::default())),
        }
    }

    /// Buffer an execution, waiting for its commission report.
    pub(super) fn add_execution(&self, execution: ExecutionData, resolved: ResolvedExecution) {
        let exec_id = execution.execution.execution_id.clone();
        let mut inner = self.inner.lock();
        inner.pending.insert(
            exec_id,
            PendingExecution {
                execution,
                resolved,
                buffered_at: Utc::now(),
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
    /// An execution is aged by its own time, or, when that does not read, by when it was
    /// buffered, so one waiting on its commission report is not cleared for its time alone.
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
            let time = pending.resolved.time.unwrap_or(pending.buffered_at);
            now.signed_duration_since(time).num_seconds() < max_age_secs
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
///
/// No instrument implies it, so the indexer keeps such a trade with its fee zeroed and warns
/// (see [`AccountEventIndexer::trade`](crate::indexer::AccountEventIndexer::trade)).
pub const UNKNOWN_FEE_ASSET: &str = "UNKNOWN";

/// The venue order id of IB order `ib_order_id`, as [`Open`](crate::order::state::Open) and every
/// [`Trade`] of this client carry it.
pub(super) fn ib_order_id(ib_order_id: i32) -> OrderId {
    OrderId::new(format_smolstr!("{ib_order_id}"))
}

/// The venue order id of `execution`, an execution of another client's order.
///
/// IB numbers orders per API client, so that client's order id could name one of this client's.
/// The permanent id IB gives an order does not, and is used where there is one: `perm:` and the
/// id. An order from outside IB can have none (`0`), and is named by its client and order id
/// instead: `client:`, the client id, `:` and the order id.
pub(super) fn foreign_order_id(execution: &Execution) -> OrderId {
    if execution.perm_id != 0 {
        OrderId::new(format_smolstr!("perm:{}", execution.perm_id))
    } else {
        OrderId::new(format_smolstr!(
            "client:{}:{}",
            execution.client_id,
            execution.order_id
        ))
    }
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
        fees_quote: None, // `AccountEventIndexer` fills this in for a fee in base or quote
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

    let ResolvedExecution {
        instrument,
        order_id,
        origin,
        time,
    } = pending.resolved;
    // Kept without a time it can read, rather than lost: stamped with this host's clock, and
    // marked so.
    let (time_exchange, time_source) = match time {
        Some(time) => (time, TimeSource::Venue),
        None => {
            warn!(exec_id = %exec.execution_id, time = %exec.time, "IBKR execution time unreadable, using now");
            (Utc::now(), TimeSource::Host)
        }
    };
    Trade {
        id: TradeId::new(&exec.execution_id),
        // For this client's order, the id its `Open` state carries, which is what a fill is
        // matched against.
        order_id,
        instrument,
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
        origin,
        position_side: None,
        // The order reference: this client places each order with its client order id there.
        // Another client's order carries whatever reference that client gave it, if any.
        client_order_id: (!exec.order_reference.is_empty())
            .then(|| ClientOrderId::new(exec.order_reference.as_str())),
        time_source,
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

/// Reads IB's text time, as on an execution or a completed order, as an instant.
///
/// IB writes such a time as a wall-clock reading and the zone it is in, such as
/// `20261027 12:48:41 Asia/Singapore`, where the zone is the gateway host's, or as
/// `20261027-04:48:41`, which is UTC.
///
/// - **The zone is a tz database name**, such as `Asia/Singapore`, or one of its aliases, such as
///   `US/Eastern` or `MET`.
/// - **A time that names no zone is read in `session_zone`**, the gateway's zone from the
///   connection handshake (see [`IbkrClient::session_zone`](super::IbkrClient::session_zone)).
///   Without one, it is `None`. A time that names a zone is read in it, whatever
///   `session_zone` is.
/// - **A zone that is not a tz database name is `None`**, with a `warn!` the first time on each
///   thread. Read in a guessed zone, such as UTC, the instant would be off by the zone's offset,
///   possibly by hours.
/// - **A reading in a repeated hour**, as clocks go back, is the earlier of its two instants. One
///   in a skipped hour, as clocks go forward, never showed on a clock, and is `None`.
/// - Anything else that does not read as one of these forms is `None`, including the `-` form
///   followed by a zone.
pub fn parse_ib_timestamp(s: &str, session_zone: Option<Tz>) -> Option<DateTime<Utc>> {
    let s = s.trim();
    let (date, rest) = s
        .split_once(char::is_whitespace)
        .map_or((s, ""), |(date, rest)| (date, rest.trim_start()));
    if let Some((date, time)) = date.split_once('-') {
        return rest
            .is_empty()
            .then(|| naive_ib_time(date, time))
            .flatten()
            .map(|naive| naive.and_utc());
    }
    let (time, zone) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, None), |(time, zone)| (time, Some(zone.trim())));
    let naive = naive_ib_time(date, time)?;
    let zone = match zone {
        Some(name) => resolve_zone(name)?,
        None => session_zone?,
    };
    zone.from_local_datetime(&naive)
        .earliest()
        .map(|time| time.with_timezone(&Utc))
}

/// `date` (`YYYYMMDD`) and `time` (`HH:MM:SS`) as a wall-clock reading.
fn naive_ib_time(date: &str, time: &str) -> Option<NaiveDateTime> {
    Some(NaiveDateTime::new(
        NaiveDate::parse_from_str(date, "%Y%m%d").ok()?,
        NaiveTime::parse_from_str(time, "%H:%M:%S").ok()?,
    ))
}

/// The tz database zone `name` names, or `None`, with a `warn!` the first time on this thread.
fn resolve_zone(name: &str) -> Option<Tz> {
    if let Some(zone) = TZ_CACHE.with(|cache| cache.borrow().get(name).copied()) {
        return zone;
    }
    let zone = name.parse::<Tz>().ok();
    if zone.is_none() {
        warn!(
            timezone = name,
            "Unknown time zone in IB times; not reading them"
        );
    }
    TZ_CACHE.with(|cache| cache.borrow_mut().insert(SmolStr::new(name), zone));
    zone
}

/// The gateway's zone, as `ibapi` read it from the connection handshake, as a `chrono_tz` zone:
/// `None` when `ibapi` did not recognise it (it warns at connect), or with a `warn!` when
/// `chrono_tz` does not know its name.
pub(super) fn session_zone(client: &ibapi::client::blocking::Client) -> Option<Tz> {
    client.time_zone().and_then(chrono_zone)
}

/// `zone`, a zone `ibapi` resolved, as the `chrono_tz` zone of its name, or `None` with a
/// `warn!` when `chrono_tz` does not know the name.
fn chrono_zone(zone: &time_tz::Tz) -> Option<Tz> {
    let name = time_tz::TimeZone::name(zone);
    name.parse()
        .inspect_err(|_| {
            warn!(
                timezone = name,
                "IB Gateway's time zone unknown to chrono-tz"
            )
        })
        .ok()
}

/// How many days of executions a read asks IB for. With no day count, IB lists only the current
/// day's, so a read across midnight would miss the fills before it.
pub(super) const EXECUTION_DAYS: i32 = 7;

/// How far back a read of [`EXECUTION_DAYS`] days of executions surely reaches. Two days short of
/// the read: IB counts its days in a time zone of its own, whose day can start up to a day after
/// this host's and shifts by an hour across daylight saving, and whether its count takes in today
/// is not documented. A paper account listed the previous day's fills for a count of 2 and not
/// for 1, so in practice the count takes in today.
pub(super) const EXECUTIONS_COVER: Duration = Duration::from_secs(5 * 24 * 60 * 60);

/// The filter for a read of [`EXECUTION_DAYS`] days of every client's executions.
pub(super) fn executions_filter() -> ExecutionFilter {
    ExecutionFilter {
        last_n_days: EXECUTION_DAYS,
        ..ExecutionFilter::default()
    }
}

/// Where IB's retention cuts into a span from `start` read at `now`: the start of
/// [`EXECUTIONS_COVER`], when the span starts before it. Fills before it may still be listed,
/// but are not sure to be.
pub(super) fn executions_held_since(
    start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let covered_from = now - EXECUTIONS_COVER;
    (start < covered_from).then_some(covered_from)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;

    fn utc(time: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(time).unwrap().to_utc()
    }

    /// A time is read in the zone it names, a tz database name or one of its aliases, whatever
    /// the session's zone.
    #[test]
    fn an_ib_time_is_read_in_the_zone_it_names() {
        let session = Some(chrono_tz::Asia::Tokyo);
        for (time, expected) in [
            ("20261027 12:48:41 Asia/Singapore", "2026-10-27T04:48:41Z"),
            // Daylight saving time: UTC-4 in April, UTC-5 in December.
            ("20250418 10:30:00 US/Eastern", "2025-04-18T14:30:00Z"),
            ("20251218 10:30:00 America/New_York", "2025-12-18T15:30:00Z"),
            ("20260929 14:00:00 UTC", "2026-09-29T14:00:00Z"),
            ("20260115 09:00:00 MET", "2026-01-15T08:00:00Z"),
        ] {
            assert_eq!(
                parse_ib_timestamp(time, session),
                Some(utc(expected)),
                "{time}"
            );
        }
    }

    /// A time that names no zone is read in the session's, and without one is not read: UTC
    /// would be a guess.
    #[test]
    fn an_ib_time_without_a_zone_is_read_in_the_sessions() {
        let time = "20261027 12:48:41";
        assert_eq!(
            parse_ib_timestamp(time, Some(chrono_tz::Asia::Singapore)),
            Some(utc("2026-10-27T04:48:41Z"))
        );
        assert_eq!(
            parse_ib_timestamp("20261027  12:48:41", Some(chrono_tz::Asia::Singapore)),
            Some(utc("2026-10-27T04:48:41Z")),
            "older gateways put two spaces between the date and the time"
        );
        assert_eq!(parse_ib_timestamp(time, None), None);
    }

    /// A zone that is not a tz database name is not read, in UTC or the session's zone.
    #[test]
    fn an_ib_time_in_an_unknown_zone_is_not_read() {
        for time in [
            "20261027 12:48:41 Some Standard Time",
            "20261027 12:48:41 Nowhere/Atlantis",
        ] {
            assert_eq!(
                parse_ib_timestamp(time, Some(chrono_tz::UTC)),
                None,
                "{time}"
            );
        }
    }

    /// `YYYYMMDD-HH:MM:SS` is UTC, whatever the session's zone.
    #[test]
    fn an_ib_time_with_a_hyphen_is_utc() {
        assert_eq!(
            parse_ib_timestamp("20261027-04:48:41", Some(chrono_tz::Asia::Singapore)),
            Some(utc("2026-10-27T04:48:41Z"))
        );
        assert_eq!(parse_ib_timestamp("20261027-04:48:41 UTC", None), None);
    }

    /// A reading in a repeated hour is its earlier instant, and one in a skipped hour is not read.
    #[test]
    fn an_ib_time_in_a_daylight_saving_change() {
        // US clocks went back at 02:00 EDT on 2025-11-02, so 01:30 came twice.
        assert_eq!(
            parse_ib_timestamp("20251102 01:30:00 US/Eastern", None),
            Some(utc("2025-11-02T05:30:00Z"))
        );
        // And forward at 02:00 EST on 2025-03-09, so 02:30 never showed.
        assert_eq!(
            parse_ib_timestamp("20250309 02:30:00 US/Eastern", None),
            None
        );
    }

    #[test]
    fn an_ib_time_that_is_no_time_is_not_read() {
        for time in [
            "",
            "invalid",
            "20261027",
            "20261027 25:00:00 UTC",
            "20261327 12:00:00 UTC",
        ] {
            assert_eq!(
                parse_ib_timestamp(time, Some(chrono_tz::UTC)),
                None,
                "{time:?}"
            );
        }
    }

    /// A fill whose time reads is the venue's; one whose time does not is kept, at this host's
    /// time, and marked so.
    #[test]
    fn a_fill_without_a_time_it_can_read_is_marked_as_this_hosts() {
        let buffer = ExecutionBuffer::new();
        let at = utc("2026-10-27T04:48:41Z");
        for (exec_id, time) in [("read", Some(at)), ("unread", None)] {
            buffer.add_execution(
                ExecutionData {
                    execution: ibapi::orders::Execution {
                        execution_id: exec_id.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ResolvedExecution {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    order_id: ib_order_id(1),
                    origin: TradeOrigin::Order,
                    time,
                },
            );
        }

        let before = Utc::now();
        let mut trades = buffer.take_without_commission();
        trades.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(trades[0].id, TradeId::new("read"));
        assert_eq!(
            (trades[0].time_exchange, trades[0].time_source),
            (at, TimeSource::Venue)
        );
        assert_eq!(trades[1].time_source, TimeSource::Host);
        assert!((before..=Utc::now()).contains(&trades[1].time_exchange));
    }

    /// The zone `ibapi` resolves a gateway's zone name to is the `chrono_tz` zone of that name.
    #[test]
    fn a_zone_ibapi_resolved_is_the_chrono_zone_of_its_name() {
        assert_eq!(
            chrono_zone(time_tz::timezones::db::asia::SINGAPORE),
            Some(chrono_tz::Asia::Singapore)
        );
        assert_eq!(
            chrono_zone(time_tz::timezones::db::america::NEW_YORK),
            Some(chrono_tz::America::New_York)
        );
    }

    /// An execution waiting for its commission report longer than `max_age` is cleared. One whose
    /// time does not read is aged by when it was buffered, so a fresh one is kept.
    #[test]
    fn clear_stale_clears_executions_older_than_the_age() {
        let buffer = ExecutionBuffer::new();
        let now = Utc::now();
        let resolved = |time| ResolvedExecution {
            instrument: InstrumentNameExchange::new("AAPL"),
            order_id: ib_order_id(1),
            origin: TradeOrigin::Order,
            time,
        };
        let execution = |exec_id: &str| ExecutionData {
            execution: ibapi::orders::Execution {
                execution_id: exec_id.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        for (exec_id, time) in [
            ("old", Some(now - chrono::Duration::hours(2))),
            ("recent", Some(now - chrono::Duration::minutes(1))),
            ("unread", None),
        ] {
            buffer.add_execution(execution(exec_id), resolved(time));
        }
        buffer.inner.lock().pending.insert(
            "unread-old".to_string(),
            PendingExecution {
                execution: execution("unread-old"),
                resolved: resolved(None),
                buffered_at: now - chrono::Duration::hours(2),
            },
        );

        assert_eq!(buffer.clear_stale(Duration::from_secs(3_600)), 2);
        let mut kept: Vec<_> = buffer
            .take_without_commission()
            .into_iter()
            .map(|trade| trade.id)
            .collect();
        kept.sort();
        assert_eq!(kept, [TradeId::new("recent"), TradeId::new("unread")]);
    }

    /// A fill reports the order reference as its client order id: this client places each order
    /// with its client order id there. An order placed without one reports none.
    #[test]
    fn a_fill_carries_the_order_reference_as_its_client_order_id() {
        let buffer = ExecutionBuffer::new();
        for (exec_id, reference) in [("a", "outside-1"), ("b", "")] {
            buffer.add_execution(
                ExecutionData {
                    execution: ibapi::orders::Execution {
                        execution_id: exec_id.to_string(),
                        order_reference: reference.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ResolvedExecution {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    order_id: ib_order_id(1),
                    origin: TradeOrigin::Order,
                    time: None,
                },
            );
        }

        let mut trades = buffer.take_without_commission();
        trades.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(
            trades[0].client_order_id,
            Some(ClientOrderId::new("outside-1"))
        );
        assert_eq!(trades[1].client_order_id, None);
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
            buffer.add_execution(
                execution(exec_id),
                ResolvedExecution {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    order_id: ib_order_id(1),
                    origin: TradeOrigin::Order,
                    time: None,
                },
            );
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
            origin: TradeOrigin::Order,
            position_side: None,
            client_order_id: None,
            time_source: TimeSource::Venue,
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
    fn executions_held_since_is_the_cover_start_only_when_the_span_starts_before_it() {
        let now = Utc.with_ymd_and_hms(2026, 10, 23, 9, 0, 0).unwrap();
        let covered_from = Utc.with_ymd_and_hms(2026, 10, 18, 9, 0, 0).unwrap();
        let before = covered_from - chrono::TimeDelta::seconds(1);
        assert_eq!(executions_held_since(before, now), Some(covered_from));
        assert_eq!(executions_held_since(covered_from, now), None);
        assert_eq!(executions_held_since(now, now), None);
    }

    #[test]
    fn executions_filter_reads_every_client_over_the_execution_days() {
        let filter = executions_filter();
        assert_eq!(filter.last_n_days, 7);
        assert_eq!(filter.client_id, None);
        assert!(filter.specific_dates.is_empty());
    }
}
