//! Fill recovery across `ibapi`'s internal transport reconnect.
//!
//! When the socket to TWS/Gateway drops, `ibapi` reconnects it by itself. The order update stream
//! that [`account_stream`](crate::client::ExecutionClient::account_stream) reads stays registered
//! across that, so it neither errors nor ends. Whatever TWS sent while the socket was down is
//! simply never delivered. The same holds, from this client's point of view, while TWS itself has
//! lost its link to IB's servers (notice 1100).
//!
//! A watcher thread next to the stream worker closes that gap for fills:
//!
//! 1. It samples [`Client::is_connected`] and drains the globally routed notice stream every
//!    [`POLL_INTERVAL`], estimating when a gap started ([`GapTracker`]).
//! 2. When the gap closes, meaning `ibapi`'s [`TRANSPORT_RECONNECT_CODE`] notice or TWS's
//!    1101/1102, it asks TWS for the day's executions and keeps those from the start of the gap
//!    on ([`RecoveredFills`]).
//! 3. It emits them through the stream's [`EventSink`], which drops every trade already delivered
//!    and reports a correction as one.
//!
//! 4. It then checks how the orders it holds as live ([`KnownLiveOrders`]) ended during the gap:
//!    it lists the open orders once and looks up each held order the listing no longer shows in
//!    IB's completed orders ([`recover_ended_orders`]), reporting each that ended as an
//!    [`AccountEventKind::OrderSnapshot`]. A check that fails is retried on a backoff.
//!
//! When fill recovery keeps failing, the watcher reports the gap as
//! [`AccountEventKind::FillRecoveryGaveUp`] and goes on, with the order check, and with the next
//! gap.
//!
//! The watcher also ends the stream when the client shuts down for good. `ibapi` never closes the
//! order update stream, so the reader cannot see that happen, but it does close every notice
//! stream.

use super::{
    ListingLock,
    ended_orders::{EndedOrderReader, release_ended},
    execution::{ExecutionBuffer, ExecutionRevision, revision_of},
    listed_cids,
    order::{OrderIdMap, PendingCancels},
    resolve_execution,
};
use crate::{
    AccountEventKind, UnindexedAccountEvent,
    client::{
        dedup::{DEDUP_CACHE_SIZE, SharedDedupCache, dedup_key_from_event, is_duplicate},
        order_recovery::{
            EventSender, KnownLiveOrders, NoPendingFills, OpenListing, OrderLookup,
            SharedKnownLiveOrders, UncheckedOrders, recover_ended_orders,
        },
    },
    emit_stream_terminated,
    error::{StreamTerminationReason, UnindexedClientError},
    fill_recovery::{FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope},
    order::id::ClientOrderId,
    trade::{Trade, TradeAmendment, TradeAmendmentKind, TradeId},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashSet;
use ibapi::{
    TRANSPORT_RECONNECT_CODE,
    client::blocking::{Client, NoticeStream},
    orders::{ExecutionData, ExecutionFilter, Executions},
    subscriptions::SubscriptionItem,
};
use lru::LruCache;
use parking_lot::Mutex;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, ibkr::ContractRegistry,
    instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// How many executions an [`EventSink`] remembers the latest delivered revision of: as many as
/// the dedup cache remembers fills.
const REVISIONS_REMEMBERED: NonZeroUsize = match NonZeroUsize::new(DEDUP_CACHE_SIZE) {
    Some(capacity) => capacity,
    None => panic!("the dedup cache size is non-zero"),
};

/// How often the watcher samples the transport state and drains notices.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Why the account stream ends when `ibapi` shuts the client down for good.
pub(super) const CLIENT_SHUT_DOWN: &str = "IBKR client shut down: ibapi gave up reconnecting \
     to TWS/Gateway, the client was disconnected, or TWS/Gateway ended the API session";

/// How far before the estimated start of a gap recovery reaches back.
///
/// The start is taken from the last sample that saw the transport connected, or from the notice
/// that announced the gap, so it can be late by a poll interval plus scheduling delay. IB's
/// execution timestamps have whole-second resolution and come from IB's clock, not this host's.
/// The margin covers all of that. Executions it reaches that the stream already delivered are
/// dropped by the [`EventSink`].
const RECOVERY_LOOKBACK: chrono::Duration = chrono::Duration::seconds(30);

/// Upper bound on reading one recovery's executions from TWS. Read in slices of [`POLL_INTERVAL`],
/// so a consumer that goes meanwhile is noticed within one.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Recovery attempts that may fail, for a reason other than the transport dropping again, before
/// the gap is given up and reported as [`AccountEventKind::FillRecoveryGaveUp`].
const MAX_RECOVERY_FAILURES: u32 = 3;

/// TWS has lost its connection to IB's servers. The API socket stays up.
const CODE_LINK_LOST: i32 = 1100;
/// TWS is reconnected to IB's servers; market data subscriptions were lost.
const CODE_LINK_RESTORED_DATA_LOST: i32 = 1101;
/// TWS is reconnected to IB's servers; market data subscriptions were kept.
const CODE_LINK_RESTORED_DATA_KEPT: i32 = 1102;

/// What a globally routed notice says about event delivery to this client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectivityNotice {
    /// Events may stop reaching this client from now on.
    Lost,
    /// Events reach this client again. Anything missed since the gap began needs recovering.
    Restored,
}

/// Classify a notice code, or `None` for one that says nothing about event delivery.
pub(super) fn classify_notice(code: i32) -> Option<ConnectivityNotice> {
    match code {
        CODE_LINK_LOST => Some(ConnectivityNotice::Lost),
        CODE_LINK_RESTORED_DATA_LOST | CODE_LINK_RESTORED_DATA_KEPT | TRANSPORT_RECONNECT_CODE => {
            Some(ConnectivityNotice::Restored)
        }
        _ => None,
    }
}

/// Whether `execution` answers an executions request, rather than reporting a fill as it happens.
///
/// TWS tags a live execution with request id `-1`. `ibapi` numbers its own requests upward from
/// 1,500,000,000. `ibapi` also copies every execution it receives to the order update stream, including
/// those answering [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades)
/// and fill recovery, so the stream worker uses this to skip them. Otherwise each such call would
/// replay the whole day's fills onto the stream.
pub(super) fn is_replayed_execution(execution: &ExecutionData) -> bool {
    execution.request_id > 0
}

/// Tracks gaps in event delivery and when recovering one is due.
///
/// Pure bookkeeping, fed by the watcher loop, so every transition can be tested without TWS.
#[derive(Debug)]
pub(super) struct GapTracker {
    /// The last sample that saw the transport connected.
    last_connected: DateTime<Utc>,
    /// The estimated start of the earliest gap not yet recovered.
    gap_start: Option<DateTime<Utc>>,
    /// A sample saw the transport disconnected, and no connected sample has followed yet.
    transport_down: bool,
    /// Delivery was restored after `gap_start`, so recovery can run.
    recovery_due: bool,
    /// Consecutive recovery attempts that failed for a reason other than transport loss.
    failures: u32,
}

impl GapTracker {
    pub(super) fn new(now: DateTime<Utc>) -> Self {
        Self {
            last_connected: now,
            gap_start: None,
            transport_down: false,
            recovery_due: false,
            failures: 0,
        }
    }

    /// Record one sample of the transport state.
    ///
    /// A disconnected sample opens a gap at the last connected sample, since the drop happened
    /// somewhere between the two. A gap already open keeps its earlier start. The first connected
    /// sample after a disconnected one makes recovery due, so recovery does not hinge on the
    /// reconnect notice alone.
    pub(super) fn observe_connection(&mut self, connected: bool, now: DateTime<Utc>) {
        if connected {
            if std::mem::take(&mut self.transport_down) {
                self.recovery_due = true;
            }
            self.last_connected = now;
        } else {
            self.transport_down = true;
            self.gap_start.get_or_insert(self.last_connected);
        }
    }

    /// Record a connectivity notice read at `now`.
    ///
    /// A restoration with no gap on record still makes recovery due, starting at `now`: a drop
    /// and reconnect that both fall between two samples leave no disconnected sample behind.
    pub(super) fn observe_notice(&mut self, notice: ConnectivityNotice, now: DateTime<Utc>) {
        self.gap_start.get_or_insert(now);
        if notice == ConnectivityNotice::Restored {
            self.recovery_due = true;
        }
    }

    /// The instant to recover executions from, if recovery is due and the transport is up.
    pub(super) fn recovery_floor(&self, connected: bool) -> Option<DateTime<Utc>> {
        if !(self.recovery_due && connected) {
            return None;
        }
        self.gap_start.map(|start| start - RECOVERY_LOOKBACK)
    }

    /// The gap was recovered.
    pub(super) fn recovered(&mut self) {
        self.gap_start = None;
        self.recovery_due = false;
        self.failures = 0;
    }

    /// A recovery attempt failed. Returns `true` when recovery should be abandoned.
    ///
    /// A transport loss is not counted. The transport dropped again, the gap stays open from its
    /// original start, and recovery runs again once the transport is back.
    pub(super) fn recovery_failed(&mut self, transport_lost: bool) -> bool {
        if transport_lost {
            return false;
        }
        self.failures += 1;
        self.failures >= MAX_RECOVERY_FAILURES
    }
}

/// The account stream's sending half, shared by the order-stream worker and the recovery watcher.
///
/// Sending and ending the stream go through one lock. So once [`terminate`](Self::terminate) has
/// emitted `StreamTerminated`, nothing can follow it, whichever thread sends next. Trades pass
/// through a dedup cache, so a fill that recovery reads again is delivered once, and a correction
/// is reported as one (see [`send_execution`](Self::send_execution)). Each event sent teaches the
/// client's [`KnownLiveOrders`] what it says about an order.
#[derive(Debug, Clone)]
pub(super) struct EventSink {
    tx: Arc<Mutex<Option<mpsc::UnboundedSender<UnindexedAccountEvent>>>>,
    dedup: SharedDedupCache,
    /// The latest revision delivered of each execution, by [`ExecutionRevision::execution`].
    /// Bounded like the dedup cache, so it forgets the oldest execution rather than grow.
    ///
    /// Lock order: `revisions`, then `known`, then `tx`; `dedup` is taken and released on its
    /// own. Nothing takes `revisions` while holding another, nor `known` while holding `tx`.
    revisions: Arc<Mutex<LruCache<SmolStr, DeliveredRevision>>>,
    /// The orders the client holds as live. Held across each send of an event it learns from,
    /// as the order check holds it across its own, so an order this stream reports ending and
    /// the check reach the consumer in the order they were decided, and the order is reported
    /// once.
    known: SharedKnownLiveOrders,
}

/// The latest revision of an execution an [`EventSink`] delivered.
#[derive(Debug)]
struct DeliveredRevision {
    revision: u32,
    id: TradeId,
}

impl EventSink {
    pub(super) fn new(
        tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
        dedup: SharedDedupCache,
        known: SharedKnownLiveOrders,
    ) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
            dedup,
            revisions: Arc::new(Mutex::new(LruCache::new(REVISIONS_REMEMBERED))),
            known,
        }
    }

    /// Send `event`, having learnt from it what [`KnownLiveOrders`] learns. Returns `false` once
    /// the stream has ended or the consumer has gone.
    pub(super) fn send(&self, event: UnindexedAccountEvent) -> bool {
        let known = KnownLiveOrders::observes(&event.kind).then(|| {
            let mut known = self.known.lock();
            known.observe(&event);
            known
        });
        let sent = self.send_event(event);
        drop(known);
        sent
    }

    /// Send `trade`, an execution completed by its commission report, unless it or a later
    /// revision of it was already delivered. Returns `false` once the stream has ended or the
    /// consumer has gone.
    ///
    /// A correction (see [`ExecutionRevision`]) is sent as [`AccountEventKind::TradeAmended`],
    /// [`Corrected`](TradeAmendmentKind::Corrected) with `trade` as the replacement. It names as
    /// the original the latest revision this stream delivered. When it delivered none, because
    /// the execution predates the stream or has dropped out of what this sink remembers, it names
    /// the revision the correction's id says it corrects. Recovery passes an execution ahead of
    /// its corrections, so an original that fell in a gap is still delivered first.
    pub(super) fn send_execution(
        &self,
        trade: Trade<AssetNameExchange, InstrumentNameExchange>,
    ) -> bool {
        let Some(revision) = ExecutionRevision::parse(&trade.id.0) else {
            return self.send_trade(trade);
        };
        let execution = SmolStr::new(revision.execution);
        let number = revision.revision;

        // Held through the send, so the stream worker and the recovery watcher cannot interleave
        // two revisions of one execution.
        let mut revisions = self.revisions.lock();
        let original = match revisions.peek(&execution) {
            Some(delivered) if delivered.revision >= number => {
                trace!(
                    exec_id = %trade.id,
                    delivered = %delivered.id,
                    "IBKR execution already delivered at this revision or a later one, skipping"
                );
                return true;
            }
            Some(delivered) => Some(delivered.id.clone()),
            None => revision.previous_id(),
        };
        let id = trade.id.clone();
        let sent = match original {
            None => self.send_trade(trade),
            Some(original) => {
                debug!(%original, correction = %id, "IBKR execution corrected, reporting it");
                self.send(UnindexedAccountEvent {
                    exchange: ExchangeId::Ibkr,
                    kind: AccountEventKind::TradeAmended(TradeAmendment::new(
                        trade.instrument.clone(),
                        trade.order_id.clone(),
                        // IB does not say when it corrected the execution.
                        Utc::now(),
                        Some(original),
                        TradeAmendmentKind::Corrected { replacement: trade },
                    )),
                })
            }
        };
        if sent {
            revisions.put(
                execution,
                DeliveredRevision {
                    revision: number,
                    id,
                },
            );
        }
        sent
    }

    /// Send `trade` unless it was already delivered. Returns `false` once the stream has ended or
    /// the consumer has gone.
    fn send_trade(&self, trade: Trade<AssetNameExchange, InstrumentNameExchange>) -> bool {
        let event = UnindexedAccountEvent {
            exchange: ExchangeId::Ibkr,
            kind: AccountEventKind::Trade(trade),
        };
        if let Some(key) = dedup_key_from_event(&event)
            && is_duplicate(&self.dedup, key)
        {
            trace!("IBKR dedup: skipping trade already delivered");
            return true;
        }
        self.send(event)
    }

    /// Emit `StreamTerminated` with `reason` and end the stream, unless it has already ended.
    pub(super) fn terminate(&self, reason: StreamTerminationReason) {
        if let Some(tx) = self.tx.lock().take() {
            emit_stream_terminated(&tx, ExchangeId::Ibkr, reason);
        }
    }

    /// Whether the stream is still open and its consumer still reading.
    pub(super) fn is_open(&self) -> bool {
        self.tx.lock().as_ref().is_some_and(|tx| !tx.is_closed())
    }
}

/// The order check sends what it learns through the sink as it is, under the [`KnownLiveOrders`]
/// lock it already holds.
impl EventSender for EventSink {
    fn send_event(&self, event: UnindexedAccountEvent) -> bool {
        self.tx
            .lock()
            .as_ref()
            .is_some_and(|tx| tx.send(event).is_ok())
    }
}

/// Why a recovery attempt failed.
#[derive(Debug, thiserror::Error)]
pub(super) enum RecoveryError {
    #[error("executions request failed: {0}")]
    Ibapi(#[source] ibapi::Error),
    #[error("executions request did not complete within {}s", RECOVERY_TIMEOUT.as_secs())]
    TimedOut,
    /// The stream ended, or its consumer went, while recovery was reading.
    #[error("account stream ended during recovery")]
    StreamClosed,
}

impl RecoveryError {
    fn is_transport_loss(&self) -> bool {
        matches!(self, Self::Ibapi(e) if super::is_transport_loss(e))
    }

    /// How [`AccountEventKind::FillRecoveryGaveUp`] states this failure.
    fn failure(&self) -> FillRecoveryFailure {
        match self {
            Self::TimedOut => FillRecoveryFailure::TimedOut {
                timeout_secs: RECOVERY_TIMEOUT.as_secs(),
            },
            Self::Ibapi(_) | Self::StreamClosed => FillRecoveryFailure::Request(self.to_string()),
        }
    }
}

/// Turns what one executions request returns into the trades of a gap.
///
/// Keeps executions from `floor` on, for orders and contracts this client tracks, and pairs each
/// with its commission report as the stream worker does.
pub(super) struct RecoveredFills<'a> {
    floor: DateTime<Utc>,
    /// This API client's id, which an execution of one of its orders carries.
    api_client_id: i32,
    contracts: &'a ContractRegistry,
    order_ids: &'a OrderIdMap,
    awaiting_commission: ExecutionBuffer,
    trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
    /// Executions kept although their timestamp did not parse.
    unparseable: usize,
}

impl<'a> RecoveredFills<'a> {
    pub(super) fn new(
        floor: DateTime<Utc>,
        api_client_id: i32,
        contracts: &'a ContractRegistry,
        order_ids: &'a OrderIdMap,
    ) -> Self {
        Self {
            floor,
            api_client_id,
            contracts,
            order_ids,
            awaiting_commission: ExecutionBuffer::new(),
            trades: Vec::new(),
            unparseable: 0,
        }
    }

    pub(super) fn push(&mut self, item: Executions) {
        match item {
            Executions::ExecutionData(execution) => {
                // An execution whose time does not parse (a DST-ambiguous local time, or a
                // format change) is kept: dropping it could lose a fill from the gap, while
                // keeping one the stream already delivered costs nothing past the dedup cache.
                match super::execution::parse_ib_timestamp(&execution.execution.time) {
                    Some(time) if time < self.floor => return,
                    Some(_) => {}
                    None => {
                        debug!(
                            exec_id = %execution.execution.execution_id,
                            time = %execution.execution.time,
                            "Unparseable timestamp in recovered execution, keeping it"
                        );
                        self.unparseable += 1;
                    }
                }
                if let Some(instrument) = resolve_execution(
                    &execution,
                    self.api_client_id,
                    self.contracts,
                    self.order_ids,
                ) {
                    self.awaiting_commission
                        .add_execution(execution, instrument);
                }
            }
            Executions::CommissionReport(report) => {
                if let Some(trade) = self.awaiting_commission.complete_with_commission(&report) {
                    self.trades.push(trade);
                }
            }
        }
    }

    /// The recovered trades, each execution ahead of its corrections. Executions still without a
    /// commission report move to `pending`, the stream's own buffer, where a report arriving on
    /// the stream later completes them.
    ///
    /// The trades are ordered by revision, then by arrival. An execution IB has not corrected is
    /// revision `01`, so in practice only the corrections move: to the end, after every original
    /// in the gap, rather than among them in time order.
    pub(super) fn finish(
        self,
        pending: &ExecutionBuffer,
    ) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
        if self.unparseable > 0 {
            warn!(
                count = self.unparseable,
                "Recovered IBKR executions with unparseable timestamps were kept regardless of \
                 the recovery window"
            );
        }
        let moved = self.awaiting_commission.drain_into(pending);
        if moved > 0 {
            warn!(
                count = moved,
                "Recovered IBKR executions arrived without a commission report; they are \
                 delivered only if the report reaches the account stream later"
            );
        }
        let mut trades = self.trades;
        // Stable, so the executions of each revision keep the order they arrived in.
        trades.sort_by_cached_key(|trade| revision_of(&trade.id));
        trades
    }
}

/// Ask TWS for the day's executions and return the fills from `floor` on.
fn recover_fills(
    client: &Client,
    floor: DateTime<Utc>,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
    pending: &ExecutionBuffer,
    sink: &EventSink,
) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, RecoveryError> {
    // No server-side time filter: TWS reads `ExecutionFilter::time` in a zone of its own choosing.
    // A day's executions are few, and `RecoveredFills` applies the window. Only this API client's:
    // IB numbers orders per client, so another's could name an order id this client also uses.
    let api_client_id = client.client_id();
    let subscription = client
        .executions(ExecutionFilter {
            client_id: Some(api_client_id),
            ..ExecutionFilter::default()
        })
        .map_err(RecoveryError::Ibapi)?;
    let deadline = Instant::now() + RECOVERY_TIMEOUT;
    let mut fills = RecoveredFills::new(floor, api_client_id, contracts, order_ids);

    loop {
        if !sink.is_open() {
            return Err(RecoveryError::StreamClosed);
        }
        let slice = deadline
            .saturating_duration_since(Instant::now())
            .min(POLL_INTERVAL);
        let waited = Instant::now();
        match subscription.next_timeout(slice) {
            Some(Ok(SubscriptionItem::Data(item))) => fills.push(item),
            Some(Ok(SubscriptionItem::Notice(notice))) => {
                debug!(%notice, "Notice during IBKR fill recovery");
            }
            Some(Err(e)) => return Err(RecoveryError::Ibapi(e)),
            // `next_timeout` answers `None` both at the end marker and when the slice runs out.
            None if Instant::now() >= deadline => return Err(RecoveryError::TimedOut),
            None if returned_early(waited.elapsed(), slice) => break,
            None => {}
        }
    }

    Ok(fills.finish(pending))
}

/// Whether a blocking wait of `timeout` that produced nothing ended before its time.
///
/// `ibapi`'s timed receives answer `None` both when the time runs out and when there is nothing
/// left to wait for: a subscription past its end marker, or a notice stream the client closed on
/// shutdown. Only the second returns before the timeout.
pub(super) fn returned_early(elapsed: Duration, timeout: Duration) -> bool {
    elapsed < timeout / 2
}

/// Everything the recovery watcher thread needs.
pub(super) struct RecoveryWatcher {
    pub(super) client: Arc<Client>,
    pub(super) notices: NoticeStream,
    pub(super) contracts: ContractRegistry,
    pub(super) order_ids: OrderIdMap,
    pub(super) pending_cancels: PendingCancels,
    pub(super) pending: ExecutionBuffer,
    pub(super) known: SharedKnownLiveOrders,
    pub(super) listings: ListingLock,
    pub(super) sink: EventSink,
    /// The runtime the order check, which is shared with the other venues, runs its timers on.
    pub(super) runtime: tokio::runtime::Handle,
}

/// What became of one attempt at recovering a gap's fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recovery {
    /// The gap is done with: its fills were recovered, or reported given up.
    Done,
    /// It failed and will be tried again.
    Retry,
    /// The stream has ended, or its consumer has gone.
    StreamClosed,
}

impl RecoveryWatcher {
    /// Watch for gaps, recover their fills, and check how the orders held as live ended, until
    /// the stream ends or its consumer goes.
    ///
    /// If fill recovery keeps failing, the gap is reported as
    /// [`AccountEventKind::FillRecoveryGaveUp`] and the stream stays open. If the client shuts
    /// down for good, the watcher stops: the order-update reader ends the stream then, on the
    /// `Error::Shutdown` `ibapi` sends it.
    pub(super) fn run(self) {
        let mut tracker = GapTracker::new(Utc::now());
        let mut unchecked = UncheckedOrders::default();

        while self.sink.is_open() {
            let connected = self.client.is_connected();
            tracker.observe_connection(connected, Utc::now());

            while let Some(notice) = self.notices.try_next() {
                observe_notice(&mut tracker, &notice);
            }

            if let Some(floor) = tracker.recovery_floor(connected) {
                match self.recover(&mut tracker, floor) {
                    // Every order held as live may have ended during the gap. Checked after the
                    // fills, so an order's fills reach the stream before how it ended.
                    Recovery::Done => unchecked.open(self.known.lock().instruments()),
                    Recovery::Retry => {}
                    Recovery::StreamClosed => return,
                }
            }

            if connected && order_check_due(&unchecked) {
                self.check_orders(&mut unchecked);
            }

            // The poll interval's sleep. It ends early on a notice, or at once if `ibapi` closed
            // the notice stream, which it does only when the client shuts down for good. Nothing
            // is left to recover then, so the watcher stops. Ending the stream is left to the
            // order-update reader, which `ibapi` hands `Error::Shutdown` explicitly.
            let waited = Instant::now();
            match self.notices.next_timeout(POLL_INTERVAL) {
                Some(notice) => observe_notice(&mut tracker, &notice),
                None if returned_early(waited.elapsed(), POLL_INTERVAL) => {
                    debug!("IBKR notice stream closed; recovery stops");
                    return;
                }
                None => {}
            }
        }
    }

    /// Recover the gap from `floor` on.
    fn recover(&self, tracker: &mut GapTracker, floor: DateTime<Utc>) -> Recovery {
        match recover_fills(
            &self.client,
            floor,
            &self.contracts,
            &self.order_ids,
            &self.pending,
            &self.sink,
        ) {
            Ok(trades) => {
                info!(
                    count = trades.len(),
                    since = %floor,
                    "Recovered IBKR fills after a gap in event delivery"
                );
                for trade in trades {
                    if !self.sink.send_execution(trade) {
                        return Recovery::StreamClosed;
                    }
                }
                tracker.recovered();
                Recovery::Done
            }
            Err(RecoveryError::StreamClosed) => Recovery::StreamClosed,
            Err(e) => {
                warn!(error = %e, since = %floor, "IBKR fill recovery failed");
                if !tracker.recovery_failed(e.is_transport_loss()) {
                    return Recovery::Retry;
                }
                error!(
                    error = %e,
                    since = %floor,
                    "Giving up IBKR fill recovery: reconcile the gap with fetch_trades"
                );
                let gave_up = UnindexedAccountEvent::new(
                    ExchangeId::Ibkr,
                    AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                        FillRecoveryScope::AllInstruments,
                        floor,
                        Utc::now().max(floor),
                        MAX_RECOVERY_FAILURES,
                        e.failure(),
                    )),
                );
                if !self.sink.send(gave_up) {
                    return Recovery::StreamClosed;
                }
                tracker.recovered();
                Recovery::Done
            }
        }
    }

    /// Check how the orders held as live on each instrument due ended, as
    /// [`recover_ended_orders`] does, with one listing of the open orders, and a lookup in IB's
    /// completed orders of each held order it no longer shows.
    ///
    /// Runs on this thread: each listing and lookup is a blocking read, so the futures the check
    /// awaits are ready at once, and the check's time budget
    /// ([`ORDER_CHECK_TIMEOUT_SECS`](crate::client::order_recovery::ORDER_CHECK_TIMEOUT_SECS)),
    /// checked between them, cannot cut one short. Each read is bounded instead by the listing's
    /// stall timeout, and a check makes at most three: the open orders, the completed orders and
    /// the executions, each once. Notices that arrive meanwhile are read once it returns.
    fn check_orders(&self, unchecked: &mut UncheckedOrders) {
        let reader = EndedOrderReader::new(
            &self.client,
            &self.listings,
            &self.contracts,
            &self.order_ids,
            &self.pending_cancels,
        );
        let list_open = |_instruments: Vec<_>| std::future::ready(self.listed_cids());
        let lookup = |key| {
            let found = reader.lookup(&key);
            if let Ok(OrderLookup::Ended(order)) = &found {
                release_ended(order, &self.order_ids, &self.pending_cancels);
            }
            std::future::ready(found)
        };
        self.runtime.block_on(recover_ended_orders(
            ExchangeId::Ibkr,
            &self.known,
            unchecked,
            &NoPendingFills,
            &self.sink,
            OpenListing::Batched,
            list_open,
            lookup,
        ));
    }

    /// The client order ids of this API client's open orders, as IB lists them.
    fn listed_cids(&self) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
        let listing = self.listings.open_orders(&self.client)?;
        Ok(listed_cids(
            listing,
            self.client.client_id(),
            &self.order_ids,
        ))
    }
}

/// Whether an instrument's check of how its orders held as live ended is due.
fn order_check_due(unchecked: &UncheckedOrders) -> bool {
    unchecked
        .next_due(&NoPendingFills)
        .is_some_and(|due| due <= tokio::time::Instant::now())
}

fn observe_notice(tracker: &mut GapTracker, notice: &ibapi::Notice) {
    if let Some(kind) = classify_notice(notice.code) {
        info!(code = notice.code, message = %notice.message, "IBKR connectivity notice");
        tracker.observe_notice(kind, Utc::now());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use crate::{
        client::dedup::new_dedup_cache,
        order::{OrderKind, TimeInForce, id::ClientOrderId},
    };
    use ibapi::{
        contracts::Contract,
        orders::{CommissionReport, Execution},
    };
    use rust_decimal::Decimal;
    use rustrade_instrument::Side;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn classify_notice_maps_connectivity_codes() {
        assert_eq!(classify_notice(1100), Some(ConnectivityNotice::Lost));
        assert_eq!(classify_notice(1101), Some(ConnectivityNotice::Restored));
        assert_eq!(classify_notice(1102), Some(ConnectivityNotice::Restored));
        assert_eq!(
            classify_notice(TRANSPORT_RECONNECT_CODE),
            Some(ConnectivityNotice::Restored)
        );
        // Farm-status and other unrouted notices say nothing about order-event delivery.
        for code in [2104, 2106, 2158, 1300, 0] {
            assert_eq!(classify_notice(code), None, "code {code}");
        }
    }

    #[test]
    fn early_return_means_nothing_left_to_wait_for() {
        let timeout = Duration::from_secs(1);
        assert!(returned_early(Duration::ZERO, timeout));
        assert!(returned_early(Duration::from_millis(100), timeout));
        assert!(!returned_early(timeout, timeout));
        // A timeout slightly late or slightly early by scheduling is still a timeout.
        assert!(!returned_early(Duration::from_millis(990), timeout));
        assert!(!returned_early(Duration::from_millis(1100), timeout));
    }

    #[test]
    fn replayed_execution_is_one_answering_a_request() {
        let with_request_id = |request_id| ExecutionData {
            request_id,
            ..ExecutionData::default()
        };
        assert!(!is_replayed_execution(&with_request_id(-1)));
        // An untagged frame decodes to 0; it is not an answer to any request ibapi made.
        assert!(!is_replayed_execution(&with_request_id(0)));
        assert!(is_replayed_execution(&with_request_id(1_500_000_000)));
    }

    #[test]
    fn tracker_is_quiet_without_a_gap() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_connection(true, at("2026-09-29T14:00:01Z"));
        assert_eq!(tracker.recovery_floor(true), None);
    }

    #[test]
    fn observed_drop_recovers_from_last_connected_sample() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_connection(true, at("2026-09-29T14:00:10Z"));
        tracker.observe_connection(false, at("2026-09-29T14:00:11Z"));
        tracker.observe_connection(false, at("2026-09-29T14:05:00Z"));
        // Still down: nothing is due until delivery is restored.
        assert_eq!(tracker.recovery_floor(false), None);

        tracker.observe_connection(true, at("2026-09-29T14:05:01Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:05:01Z"));

        assert_eq!(
            tracker.recovery_floor(true),
            Some(at("2026-09-29T14:00:10Z") - RECOVERY_LOOKBACK)
        );
    }

    #[test]
    fn observed_reconnect_makes_recovery_due_without_a_notice() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_connection(true, at("2026-09-29T14:00:10Z"));
        tracker.observe_connection(false, at("2026-09-29T14:00:11Z"));
        tracker.observe_connection(true, at("2026-09-29T14:00:40Z"));

        assert_eq!(
            tracker.recovery_floor(true),
            Some(at("2026-09-29T14:00:10Z") - RECOVERY_LOOKBACK)
        );
    }

    #[test]
    fn restoration_without_an_observed_drop_recovers_from_the_notice() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_connection(true, at("2026-09-29T14:00:10Z"));
        // Drop and reconnect both fell between two samples.
        tracker.observe_connection(true, at("2026-09-29T14:00:11Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:00:11Z"));

        assert_eq!(
            tracker.recovery_floor(true),
            Some(at("2026-09-29T14:00:11Z") - RECOVERY_LOOKBACK)
        );
    }

    #[test]
    fn link_loss_notice_opens_gap_that_restoration_closes() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_notice(ConnectivityNotice::Lost, at("2026-09-29T14:01:00Z"));
        // TWS's link to IB is down but the socket is up: nothing to recover yet.
        tracker.observe_connection(true, at("2026-09-29T14:02:00Z"));
        assert_eq!(tracker.recovery_floor(true), None);

        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:03:00Z"));
        assert_eq!(
            tracker.recovery_floor(true),
            Some(at("2026-09-29T14:01:00Z") - RECOVERY_LOOKBACK)
        );
    }

    #[test]
    fn recovery_waits_for_the_transport() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:00:05Z"));
        assert_eq!(tracker.recovery_floor(false), None);
        assert!(tracker.recovery_floor(true).is_some());
    }

    #[test]
    fn recovered_gap_is_cleared() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:00:05Z"));
        tracker.recovered();
        assert_eq!(tracker.recovery_floor(true), None);

        // A later gap starts afresh rather than from the recovered one.
        tracker.observe_connection(true, at("2026-09-29T15:00:00Z"));
        tracker.observe_connection(false, at("2026-09-29T15:00:01Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T15:00:30Z"));
        assert_eq!(
            tracker.recovery_floor(true),
            Some(at("2026-09-29T15:00:00Z") - RECOVERY_LOOKBACK)
        );
    }

    #[test]
    fn transport_loss_during_recovery_keeps_the_gap_and_is_not_counted() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_connection(false, at("2026-09-29T14:00:01Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:01:00Z"));
        let floor = tracker.recovery_floor(true);

        for _ in 0..MAX_RECOVERY_FAILURES * 2 {
            assert!(!tracker.recovery_failed(true));
        }
        // The second drop is later than the first; the gap still starts at the first.
        tracker.observe_connection(false, at("2026-09-29T14:01:01Z"));
        assert_eq!(tracker.recovery_floor(true), floor);
    }

    #[test]
    fn repeated_other_failures_abandon_recovery() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:00:05Z"));
        for _ in 1..MAX_RECOVERY_FAILURES {
            assert!(!tracker.recovery_failed(false));
        }
        assert!(tracker.recovery_failed(false));
    }

    #[test]
    fn success_resets_the_failure_count() {
        let mut tracker = GapTracker::new(at("2026-09-29T14:00:00Z"));
        tracker.observe_notice(ConnectivityNotice::Restored, at("2026-09-29T14:00:05Z"));
        for _ in 1..MAX_RECOVERY_FAILURES {
            assert!(!tracker.recovery_failed(false));
        }
        tracker.recovered();
        assert!(!tracker.recovery_failed(false));
    }

    fn trade(id: &str) -> Trade<AssetNameExchange, InstrumentNameExchange> {
        use crate::{
            order::id::{OrderId, StrategyId},
            trade::{AssetFees, TradeId},
        };
        Trade {
            id: TradeId::new(id),
            order_id: OrderId::new("cid"),
            instrument: InstrumentNameExchange::new("AAPL"),
            strategy: StrategyId::unknown(),
            time_exchange: at("2026-09-29T14:00:00Z"),
            side: Side::Buy,
            price: Decimal::from(100),
            quantity: Decimal::ONE,
            order_filled_quantity: Some(Decimal::ONE),
            fees: AssetFees::new(AssetNameExchange::from("USD"), Decimal::ONE, None),
        }
    }

    fn sink() -> (EventSink, mpsc::UnboundedReceiver<UnindexedAccountEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            EventSink::new(
                tx,
                new_dedup_cache(),
                KnownLiveOrders::shared(ExchangeId::Ibkr),
            ),
            rx,
        )
    }

    /// The sink teaches the client's live orders what each event it sends says about an order,
    /// so a later gap's order check asks only about those still live; what the order check sends
    /// through it, it has already learnt.
    #[test]
    fn the_sink_teaches_the_live_orders_what_it_sends() {
        use crate::order::{
            Order, OrderKey, OrderKind, TimeInForce,
            id::{OrderId, StrategyId},
            state::{Cancelled, Open, OrderState},
        };
        use rust_decimal_macros::dec;

        let known = KnownLiveOrders::shared(ExchangeId::Ibkr);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::new(tx, new_dedup_cache(), known.clone());
        let snapshot = |cid: &str, state| {
            UnindexedAccountEvent::new(
                ExchangeId::Ibkr,
                AccountEventKind::OrderSnapshot(crate::Snapshot::new(Order {
                    key: OrderKey {
                        exchange: ExchangeId::Ibkr,
                        instrument: InstrumentNameExchange::new("AAPL"),
                        strategy: StrategyId::unknown(),
                        cid: ClientOrderId::new(cid),
                    },
                    side: Side::Buy,
                    price: Some(dec!(150)),
                    quantity: dec!(10),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state,
                })),
            )
        };
        let open = || {
            OrderState::active(Open::new(
                crate::order::id::VenueOrderId::Assigned(OrderId::new("7")),
                Utc::now(),
                Decimal::ZERO,
            ))
        };

        assert!(sink.send(snapshot("a", open())));
        assert!(known.lock().contains(&ClientOrderId::new("a")));
        assert!(sink.send(snapshot(
            "a",
            OrderState::inactive(Cancelled::new(OrderId::new("7"), Utc::now(), None))
        )));
        assert!(!known.lock().contains(&ClientOrderId::new("a")));

        assert!(sink.send_event(snapshot("b", open())));
        assert!(!known.lock().contains(&ClientOrderId::new("b")));
        assert_eq!(drain(&mut rx).len(), 3);
    }

    /// A gap given up is reported with why its last read failed.
    #[test]
    fn a_failed_recovery_states_why() {
        assert_eq!(
            RecoveryError::TimedOut.failure(),
            FillRecoveryFailure::TimedOut {
                timeout_secs: RECOVERY_TIMEOUT.as_secs()
            }
        );
        assert!(matches!(
            RecoveryError::Ibapi(ibapi::Error::Simple("refused".into())).failure(),
            FillRecoveryFailure::Request(reason) if reason.contains("refused")
        ));
    }

    fn drain(
        rx: &mut mpsc::UnboundedReceiver<UnindexedAccountEvent>,
    ) -> Vec<UnindexedAccountEvent> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    #[test]
    fn sink_delivers_a_trade_once() {
        let (sink, mut rx) = sink();
        assert!(sink.send_trade(trade("e1")));
        assert!(sink.send_trade(trade("e1")));
        assert!(sink.send_trade(trade("e2")));

        let ids: Vec<_> = drain(&mut rx)
            .into_iter()
            .map(|event| match event.kind {
                AccountEventKind::Trade(trade) => trade.id,
                other => panic!("expected a trade, got {other:?}"),
            })
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].0.as_str(), "e1");
        assert_eq!(ids[1].0.as_str(), "e2");
    }

    /// What `event` delivered: a trade's id, or an amendment's original and replacement ids.
    fn delivered(event: UnindexedAccountEvent) -> String {
        match event.kind {
            AccountEventKind::Trade(trade) => trade.id.0.to_string(),
            AccountEventKind::TradeAmended(amendment) => match amendment.kind {
                TradeAmendmentKind::Corrected { replacement } => format!(
                    "{} corrected by {}",
                    amendment
                        .original
                        .expect("an IBKR correction names its original"),
                    replacement.id
                ),
                other => panic!("expected a correction, got {other:?}"),
            },
            other => panic!("expected a trade or an amendment, got {other:?}"),
        }
    }

    fn delivered_all(rx: &mut mpsc::UnboundedReceiver<UnindexedAccountEvent>) -> Vec<String> {
        drain(rx).into_iter().map(delivered).collect()
    }

    #[test]
    fn sink_reports_a_correction_of_a_delivered_execution() {
        let (sink, mut rx) = sink();
        let mut correction = trade("x.01.02");
        correction.price = Decimal::from(101);
        assert!(sink.send_execution(trade("x.01.01")));
        assert!(sink.send_execution(correction.clone()));

        let events = drain(&mut rx);
        assert_eq!(events.len(), 2, "{events:?}");
        let AccountEventKind::TradeAmended(amendment) = &events[1].kind else {
            panic!("expected an amendment, got {:?}", events[1]);
        };
        assert_eq!(amendment.instrument, correction.instrument);
        assert_eq!(amendment.order_id, correction.order_id);
        assert_eq!(amendment.original, Some(TradeId::new("x.01.01")));
        assert_eq!(
            amendment.kind,
            TradeAmendmentKind::Corrected {
                replacement: correction
            }
        );
    }

    /// The original predates the stream, so the snapshot the consumer started from counts it.
    /// Sent as a trade, the correction would count it twice.
    #[test]
    fn sink_reports_a_correction_of_an_execution_it_did_not_deliver() {
        let (sink, mut rx) = sink();
        assert!(sink.send_execution(trade("x.01.02")));
        assert_eq!(delivered_all(&mut rx), ["x.01.01 corrected by x.01.02"]);
    }

    #[test]
    fn sink_names_the_latest_revision_it_delivered() {
        let (sink, mut rx) = sink();
        for id in ["x.01.01", "x.01.02", "x.01.04"] {
            assert!(sink.send_execution(trade(id)));
        }
        assert_eq!(
            delivered_all(&mut rx),
            [
                "x.01.01",
                "x.01.01 corrected by x.01.02",
                "x.01.02 corrected by x.01.04"
            ]
        );
    }

    #[test]
    fn sink_drops_a_revision_older_than_one_delivered() {
        let (sink, mut rx) = sink();
        for id in ["x.01.02", "x.01.01", "x.01.02", "y"] {
            assert!(sink.send_execution(trade(id)));
        }
        assert_eq!(
            delivered_all(&mut rx),
            ["x.01.01 corrected by x.01.02", "y"]
        );
    }

    #[test]
    fn sink_delivers_an_execution_without_a_revision_once() {
        let (sink, mut rx) = sink();
        for id in ["e1", "e1", "x.01.01", "x.01.01"] {
            assert!(sink.send_execution(trade(id)));
        }
        assert_eq!(delivered_all(&mut rx), ["e1", "x.01.01"]);
    }

    #[test]
    fn nothing_follows_stream_terminated() {
        let (sink, mut rx) = sink();
        let other = sink.clone();
        sink.terminate(StreamTerminationReason::Error("first".into()));
        other.terminate(StreamTerminationReason::Error("second".into()));
        assert!(!other.send_trade(trade("e1")));
        assert!(!sink.is_open());

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1, "exactly one terminal event: {events:?}");
        assert!(matches!(
            &events[0].kind,
            AccountEventKind::StreamTerminated(StreamTerminationReason::Error(reason)) if reason == "first"
        ));
        // The sender went with the terminal event, so the consumer sees the end of the channel.
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn sink_closes_when_consumer_goes() {
        let (sink, rx) = sink();
        assert!(sink.is_open());
        drop(rx);
        assert!(!sink.is_open());
        assert!(!sink.send_trade(trade("e1")));
    }

    const CON_ID: i32 = 265598;
    const IB_ORDER_ID: i32 = 7;

    fn tracked() -> (ContractRegistry, OrderIdMap) {
        let contracts = ContractRegistry::new();
        contracts
            .register(
                InstrumentNameExchange::new("AAPL"),
                Contract {
                    contract_id: CON_ID,
                    ..Contract::default()
                },
            )
            .unwrap();
        let order_ids = OrderIdMap::new();
        order_ids
            .register(
                ClientOrderId::new("cid-7"),
                IB_ORDER_ID,
                super::super::order::OrderContext {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    side: Side::Buy,
                    price: Some(Decimal::from(100)),
                    quantity: Decimal::from(2),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                },
            )
            .unwrap();
        (contracts, order_ids)
    }

    fn execution(exec_id: &str, order_id: i32, time: &str) -> Executions {
        Executions::ExecutionData(ExecutionData {
            request_id: 9001,
            contract: Contract {
                contract_id: CON_ID,
                ..Contract::default()
            },
            execution: Execution {
                order_id,
                execution_id: exec_id.to_string(),
                time: time.to_string(),
                shares: 1.0,
                price: 100.0,
                cumulative_quantity: 1.0,
                ..Execution::default()
            },
        })
    }

    /// `item`, an execution, as one of API client `api_client_id`'s orders.
    fn from_api_client(api_client_id: i32, item: Executions) -> Executions {
        let Executions::ExecutionData(mut data) = item else {
            unreachable!("an execution")
        };
        data.execution.client_id = api_client_id;
        Executions::ExecutionData(data)
    }

    fn commission(exec_id: &str) -> Executions {
        Executions::CommissionReport(CommissionReport {
            execution_id: exec_id.to_string(),
            commission: 1.0,
            currency: "USD".to_string(),
            ..CommissionReport::default()
        })
    }

    #[test]
    fn recovered_fills_keep_the_window_and_tracked_orders() {
        let (contracts, order_ids) = tracked();
        let pending = ExecutionBuffer::new();
        let mut fills = RecoveredFills::new(at("2026-09-29T14:00:00Z"), 0, &contracts, &order_ids);

        for item in [
            // Before the window: the stream delivered it, or it predates the stream.
            execution("before", IB_ORDER_ID, "20260929 13:59:59 UTC"),
            commission("before"),
            // In the window.
            execution("gap", IB_ORDER_ID, "20260929 14:00:00 UTC"),
            commission("gap"),
            // In the window, but for an order this client does not track.
            execution("foreign", 99, "20260929 14:00:01 UTC"),
            commission("foreign"),
            // In the window, under the tracked order's id, but another API client's order: IB
            // numbers orders per client.
            from_api_client(
                905,
                execution("other-client", IB_ORDER_ID, "20260929 14:00:02 UTC"),
            ),
            commission("other-client"),
        ] {
            fills.push(item);
        }

        let trades = fills.finish(&pending);
        assert_eq!(trades.len(), 1, "{trades:?}");
        assert_eq!(trades[0].id.0.as_str(), "gap");
        // The IB order id, which the order's `Open` state carries.
        assert_eq!(trades[0].order_id.0, IB_ORDER_ID.to_string());
        assert_eq!(trades[0].fees.fees, Decimal::ONE);
        assert_eq!(pending.pending_count(), 0);
    }

    /// A time that does not parse cannot be placed against the window, and dropping the execution
    /// could lose a fill from the gap, so it is kept.
    #[test]
    fn recovered_execution_with_unparseable_time_is_kept() {
        let (contracts, order_ids) = tracked();
        let pending = ExecutionBuffer::new();
        let mut fills = RecoveredFills::new(at("2026-09-29T14:00:00Z"), 0, &contracts, &order_ids);
        fills.push(execution("garbled", IB_ORDER_ID, "not a time"));
        fills.push(commission("garbled"));

        let trades = fills.finish(&pending);
        assert_eq!(trades.len(), 1, "{trades:?}");
        assert_eq!(trades[0].id.0.as_str(), "garbled");
    }

    /// TWS can answer with a correction ahead of the execution it corrects. Both fell in the gap,
    /// so the consumer has neither: the original must go first, or the sink would report the
    /// correction of an execution the consumer never saw and then drop that execution as older.
    #[test]
    fn recovered_fills_put_an_execution_ahead_of_its_corrections() {
        let (contracts, order_ids) = tracked();
        let pending = ExecutionBuffer::new();
        let mut fills = RecoveredFills::new(at("2026-09-29T14:00:00Z"), 0, &contracts, &order_ids);
        for item in [
            execution("x.01.02", IB_ORDER_ID, "20260929 14:00:00 UTC"),
            commission("x.01.02"),
            execution("x.01.01", IB_ORDER_ID, "20260929 14:00:00 UTC"),
            commission("x.01.01"),
            execution("y.01.01", IB_ORDER_ID, "20260929 14:00:01 UTC"),
            commission("y.01.01"),
        ] {
            fills.push(item);
        }

        let (sink, mut rx) = sink();
        for trade in fills.finish(&pending) {
            assert!(sink.send_execution(trade));
        }
        assert_eq!(
            delivered_all(&mut rx),
            ["x.01.01", "y.01.01", "x.01.01 corrected by x.01.02"]
        );
    }

    #[test]
    fn recovered_execution_without_commission_moves_to_the_stream_buffer() {
        let (contracts, order_ids) = tracked();
        let pending = ExecutionBuffer::new();
        let mut fills = RecoveredFills::new(at("2026-09-29T14:00:00Z"), 0, &contracts, &order_ids);
        fills.push(execution("late", IB_ORDER_ID, "20260929 14:00:30 UTC"));

        assert!(fills.finish(&pending).is_empty());
        assert_eq!(pending.pending_count(), 1);

        // The report arriving on the stream afterwards completes it there.
        let Executions::CommissionReport(report) = commission("late") else {
            unreachable!()
        };
        let trade = pending.complete_with_commission(&report).unwrap();
        assert_eq!(trade.id.0.as_str(), "late");
    }
}
