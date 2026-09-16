use crate::{
    EngineEvent, Timed,
    engine::clock::{EngineClock, HistoricalClock},
    error::BarterError,
    execution::{AccountStreamEvent, request::ExecutionRequest},
    shutdown::Shutdown,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, stream::FusedStream};
use rustrade_execution::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    exchange::mock::{SimulatedVenue, VenueOutcome},
    indexer::AccountEventIndexer,
    order::{Order, request::UnindexedOrderResponseCancel, state::UnindexedOrderState},
};
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    instrument::name::InstrumentNameExchange,
};
use rustrade_integration::{
    channel::UnboundedRx,
    collection::{FnvIndexMap, snapshot::Snapshot},
};
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, VecDeque},
    pin::Pin,
    task::{Context, Poll},
};
use tracing::info;

/// Builds the execution half of a deterministic simulation.
pub mod builder;

pub use builder::{SimExecutionBuild, SimExecutionBuilder};

/// One simulated venue and everything needed to drive it deterministically.
///
/// Built by [`SimExecutionBuilder::add_venue`]; consumed by [`SimRunner::new`].
#[derive(Debug)]
pub struct SimVenue {
    /// The venue's state machine: ledger, pricing, and the order its output must be delivered in.
    pub venue: SimulatedVenue,
    /// Translates between the `Engine`'s indices and the venue's exchange-native names.
    ///
    /// Owned per venue because [`SimRunner`] replaces [`ExecutionManager`], which is where this
    /// translation and the initial account snapshot otherwise live.
    ///
    /// [`ExecutionManager`]: crate::execution::manager::ExecutionManager
    pub indexer: AccountEventIndexer,
    /// Receives the `Engine`'s [`ExecutionRequest`]s for this venue.
    ///
    /// Drained inline on the engine's own thread — never handed to a task, which is the whole
    /// difference between this path and [`ExecutionManager`]'s.
    ///
    /// [`ExecutionManager`]: crate::execution::manager::ExecutionManager
    pub request_rx: UnboundedRx<ExecutionRequest>,
    /// Simulated delay from the `Engine` issuing a request to the venue acting on it.
    pub to_venue: TimeDelta,
    /// Simulated delay from the venue acting to the `Engine` observing the result.
    pub from_venue: TimeDelta,
}

/// What a venue owes, erased to one type so differently-typed responses can share one queue.
///
/// The erasure happens here rather than on [`VenueOutcome`] so that `rustrade-execution` keeps its
/// precise per-request response types and never learns that a scheduling queue exists.
#[derive(Debug)]
enum Deliverable {
    /// A balance or trade the venue produced while booking a request.
    Account(UnindexedAccountEvent),
    /// The response to an [`ExecutionRequest::Open`].
    OpenResponse(Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>),
    /// The response to an [`ExecutionRequest::Cancel`].
    CancelResponse(UnindexedOrderResponseCancel),
}

/// One deliverable and the simulated instant it becomes visible to the `Engine`.
#[derive(Debug)]
struct ScheduledEvent {
    time: DateTime<Utc>,
    /// Monotone tie-break within one instant. Ordering on `time` alone would be unspecified: a
    /// [`BinaryHeap`] is not a stable sort, so equal keys pop in an arbitrary order.
    seq: u64,
    exchange: ExchangeIndex,
    payload: Deliverable,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        (self.time, self.seq) == (other.time, other.seq)
    }
}

impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.time, self.seq).cmp(&(other.time, other.seq))
    }
}

/// Default [`SimRunner::with_feedback_limit`]: account deliverables emitted with no intervening
/// source event before a run is abandoned as a zero-delay feedback cycle.
///
/// Generous enough that no realistic strategy reaches it — it allows roughly three thousand orders
/// opened simultaneously against one market event — and small enough that a runaway is reported in
/// milliseconds rather than filling memory first.
pub const DEFAULT_FEEDBACK_LIMIT: usize = 10_000;

/// Priority of an account deliverable within one instant. See [`SimRunner`]'s ordering contract.
const CLASS_ACCOUNT: u8 = 1;

/// Priority of a source event within one instant. See [`SimRunner`]'s ordering contract.
fn source_class<MarketKind>(event: &EngineEvent<MarketKind>) -> u8 {
    match event {
        // Market data is marked to last, so a fill at the same instant is already in the position
        // it prices. This is the ordering #289 is about.
        EngineEvent::Market(_) => 2,
        // An account event injected into the source ranks with the ones this runner schedules.
        EngineEvent::Account(_) => CLASS_ACCOUNT,
        // Auxiliary and control events lead, matching the merge this runner reads from: a stock
        // split adjusts a position before any fill stamped at that instant is applied to it.
        _ => 0,
    }
}

/// Deterministic, synchronous driver for one or more [`SimulatedVenue`]s.
///
/// A `Stream` of [`EngineEvent`]s that merges a time-ordered source (market data plus auxiliary
/// events) with the account events its own venues produce, and is intended to be polled **inline**
/// by [`async_run`](crate::engine::run::async_run) — that is, by the `Engine`'s own task, with no
/// channel or forwarding task in between.
///
/// # Why inline polling is the whole point
/// The `Engine` sends its execution requests synchronously *inside* `Engine::process`, so by the
/// time `process` returns, every request that event provoked is already queued on this runner's
/// receivers. The next poll therefore observes them, books them against the venue, and schedules
/// what they produce — all before the next source event is drawn.
///
/// Forwarding the source through a channel instead, as a live system does, hands that decision to
/// the tokio scheduler: an always-ready market source races arbitrarily far ahead of the engine and
/// account events land at a scheduler-determined index within it. Balance and position *quantities*
/// survive that, because they do not depend on where in the sequence a fill lands; every
/// time-derived statistic does not.
///
/// # Ordering contract
/// Deliverables and source events are merged on `(time, class)`, where `class` breaks a tie within
/// one instant:
///
/// | class | events |
/// |---|---|
/// | 0 | auxiliary and control — corporate actions, contract expiries |
/// | 1 | account — balances, trades, order responses |
/// | 2 | market data |
///
/// Auxiliary events keeping the tie matches the source merge this runner reads from: a stock split
/// must adjust a position before any fill stamped at that instant is applied to it. Account events
/// preceding market data at one instant is the rule this type exists for — a fill stamped at `T`
/// must reach the `Engine` before the market event at `T` marks the resulting position to that
/// price, or the terminal `pnl_unrealised` describes a position the run did not hold.
///
/// Within one instant and class, delivery follows booking order: every account event a request
/// produced precedes that request's response, so "the `Engine` has the response" implies "the
/// `Engine` has already seen every account event for that order".
///
/// # Latency is simulated, never slept
/// A request issued at `T` reaches its venue at `T + to_venue` and its result becomes visible at
/// `T + to_venue + from_venue`. Both are simulated offsets applied to the queue key, so a run's
/// wall-clock duration does not scale with the latency being modelled, and delivery order is a
/// function of the dataset alone.
///
/// # Termination
/// Exhausting the source yields [`Shutdown::AfterDrain`] once, and only then is the queue drained
/// and the stream ended. The `Engine` answers that event by suppressing further algo orders, so the
/// drain cannot be extended indefinitely by a strategy that trades on the very fills being drained
/// — and it is the `Engine` that stops generating rather than this runner silently discarding, so
/// no request is ever accepted and then dropped.
///
/// A fill booked against the final source event is still always delivered, so ending a run cannot
/// cut the ledgers short. The [`ExecutionRequest::Drain`] the `Engine` sends in reply is therefore
/// a no-op here: this runner, not the execution side, owns the moment a simulated run ends.
/// [`ExecutionRequest::Shutdown`] abandons whatever is still scheduled, matching its documented
/// meaning.
///
/// # Zero-delay feedback cycles are reported, not spun on
/// A strategy that opens an order in response to its own fill, against a venue whose simulated
/// round trip is zero, schedules that fill at the instant of the request that provoked it. The
/// account event then outranks every later source event forever: simulated time never advances and
/// the source is never drawn again. At most [`with_feedback_limit`](Self::with_feedback_limit)
/// account deliverables are therefore emitted with no intervening source event, after which the
/// run is abandoned with [`BarterError::SimFeedbackLoop`], readable via [`error`](Self::error).
///
/// # Panics
/// Panics if a venue reports an account event or response whose exchange, asset or instrument is
/// absent from that venue's own index, and if a simulated timestamp overflows [`DateTime<Utc>`].
/// Both mean the simulation is misconfigured in a way that would otherwise silently drop a fill,
/// which is the failure mode this type exists to remove.
#[pin_project::pin_project]
pub struct SimRunner<Source, MarketKind>
where
    Source: Stream,
{
    #[pin]
    source: futures::stream::Peekable<Source>,
    venues: FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: BinaryHeap<Reverse<ScheduledEvent>>,
    /// Monotone across every venue, so two venues booking at one instant still pop in booking
    /// order rather than an arbitrary one.
    seq: u64,
    /// The `Engine`'s clock, shared. Read to stamp each request with the simulated instant the
    /// `Engine` issued it at — the same instant the async client path reads.
    clock: HistoricalClock,
    /// Initial per-venue account snapshots, emitted before anything else.
    seeding: VecDeque<EngineEvent<MarketKind>>,
    /// Ceiling on `since_source`. See [`SimRunner::with_feedback_limit`].
    feedback_limit: usize,
    /// Account deliverables emitted since the last source event, or since the source was exhausted.
    ///
    /// The seeding snapshots are not charged against it: they are bounded by the venue count and
    /// precede any feedback there could be.
    since_source: usize,
    /// Why the run was abandoned, if it was. Read through [`SimRunner::error`].
    error: Option<BarterError>,
    source_done: bool,
    /// Latch: [`Shutdown::AfterDrain`] is emitted exactly once, when the source is exhausted.
    drain_signalled: bool,
    terminated: bool,
}

/// Manual because [`futures::stream::Peekable`] is [`Debug`] only when its `Source` is, and
/// demanding that of every caller would buy nothing: the field carries no state worth printing.
impl<Source, MarketKind> std::fmt::Debug for SimRunner<Source, MarketKind>
where
    Source: Stream,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimRunner")
            .field("source", &"Peekable<Source>")
            .field("venues", &self.venues.keys().collect::<Vec<_>>())
            .field("pending", &self.pending.len())
            .field("seq", &self.seq)
            .field("clock", &self.clock)
            .field("seeding", &self.seeding.len())
            .field("feedback_limit", &self.feedback_limit)
            .field("since_source", &self.since_source)
            .field("error", &self.error)
            .field("source_done", &self.source_done)
            .field("drain_signalled", &self.drain_signalled)
            .field("terminated", &self.terminated)
            .finish()
    }
}

impl<Source, MarketKind> SimRunner<Source, MarketKind>
where
    Source: Stream,
{
    /// Construct a `SimRunner` over the provided venues and time-ordered source.
    ///
    /// `clock` must be the same [`HistoricalClock`] the `Engine` was built with: it is read to
    /// learn the simulated instant each request was issued at.
    ///
    /// `source` must be ascending by [`Timed::time`]. The backtest harness's market/auxiliary merge
    /// produces exactly that, and enforces it against a streamed market source.
    ///
    /// Each venue's account snapshot is queued immediately, ahead of any source event. Besides
    /// seeding balances and instruments it is what marks that venue's account connection healthy,
    /// which the `Engine` would otherwise wait on forever.
    ///
    /// # Panics
    /// Panics if a venue's initial snapshot references an asset or instrument absent from that
    /// venue's own index — see the type-level `# Panics`.
    pub fn new(
        venues: FnvIndexMap<ExchangeIndex, SimVenue>,
        source: Source,
        clock: HistoricalClock,
    ) -> Self {
        let seeding = venues
            .values()
            .map(|slot| {
                let snapshot = UnindexedAccountEvent {
                    exchange: slot.venue.exchange,
                    kind: AccountEventKind::Snapshot(slot.venue.account_snapshot()),
                };

                index_account_event(&slot.indexer, snapshot)
            })
            .collect();

        Self {
            source: futures::StreamExt::peekable(source),
            venues,
            pending: BinaryHeap::new(),
            seq: 0,
            clock,
            seeding,
            feedback_limit: DEFAULT_FEEDBACK_LIMIT,
            since_source: 0,
            error: None,
            source_done: false,
            drain_signalled: false,
            terminated: false,
        }
    }

    /// Set how many account deliverables may be emitted with no intervening source event before the
    /// run is abandoned as a zero-delay feedback cycle. Defaults to [`DEFAULT_FEEDBACK_LIMIT`].
    ///
    /// Raise it for a strategy that legitimately opens more than a few thousand orders against a
    /// single market event. Raising it will not rescue a true zero-delay cycle — that has no limit
    /// at which it terminates — so a run hitting even a large limit is almost always reporting the
    /// configuration described in [`BarterError::SimFeedbackLoop`].
    pub fn with_feedback_limit(mut self, limit: usize) -> Self {
        self.feedback_limit = limit;
        self
    }

    /// Why the run was abandoned, if it was.
    ///
    /// # Caller obligation
    /// A `Stream` cannot yield an error and keep its item type, so an abandoned run is reported by
    /// ending the stream — indistinguishable, to the `Engine`, from one that finished. **A caller
    /// must check this once the stream has ended**, before treating the terminal state as a
    /// result: the statistics of an abandoned run describe only the portion of the dataset that was
    /// read. [`backtest`](crate::backtest::backtest) does so.
    pub fn error(&self) -> Option<&BarterError> {
        self.error.as_ref()
    }

    /// The venues this runner drove, for inspection once a run has ended.
    pub fn venues(&self) -> &FnvIndexMap<ExchangeIndex, SimVenue> {
        &self.venues
    }
}

impl<Source, MarketKind> Stream for SimRunner<Source, MarketKind>
where
    Source: Stream<Item = Timed<EngineEvent<MarketKind>>>,
{
    type Item = EngineEvent<MarketKind>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.terminated {
            return Poll::Ready(None);
        }

        // Venues are seeded before any source event, so the Engine starts from a known account.
        if let Some(event) = this.seeding.pop_front() {
            return Poll::Ready(Some(event));
        }

        loop {
            // Everything the Engine emitted while processing the previous event is already queued
            // (its requests are sent synchronously inside `process`), so this observes it in full.
            if drain_requests(this.venues, this.pending, this.seq, this.clock)
                == RequestDrain::Abandon
            {
                *this.pending = BinaryHeap::new();
                *this.terminated = true;
                return Poll::Ready(None);
            }

            let account_leads = if *this.source_done {
                // Ask the Engine to stop generating *before* draining what it is already owed.
                // Doing so is what bounds the drain: the Engine suppresses further algo orders on
                // this event, so nothing new is booked while the queue empties. Suppressing at the
                // Engine rather than discarding requests here keeps the guarantee that a request
                // this runner accepted is always answered.
                if !*this.drain_signalled {
                    *this.drain_signalled = true;
                    // The drain is a fresh stretch with no source events left to reset the budget,
                    // so it gets its own. Exceeding it means the Engine is still generating, which
                    // `AfterDrain` was just sent to stop.
                    *this.since_source = 0;
                    return Poll::Ready(Some(EngineEvent::Shutdown(Shutdown::AfterDrain)));
                }

                true
            } else {
                // Copy the ordering key out so the peek's borrow of `source` ends here, freeing it
                // to be re-polled below.
                let ordering = match this.source.as_mut().poll_peek(cx) {
                    // Ordering cannot be decided until the source's next event — or its end — is
                    // known.
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        *this.source_done = true;
                        continue;
                    }
                    Poll::Ready(Some(timed)) => (timed.time, source_class(&timed.value)),
                };

                this.pending
                    .peek()
                    .is_some_and(|Reverse(next)| (next.time, CLASS_ACCOUNT) <= ordering)
            };

            if account_leads {
                // Checked before popping, so the diagnostic can name the event that would have been
                // emitted rather than one already gone.
                if let Some(error) = check_feedback(
                    this.venues,
                    this.pending,
                    *this.since_source,
                    *this.feedback_limit,
                ) {
                    *this.error = Some(error);
                    *this.pending = BinaryHeap::new();
                    *this.terminated = true;
                    return Poll::Ready(None);
                }

                if let Some(event) = pop_due(this.venues, this.pending) {
                    *this.since_source += 1;
                    return Poll::Ready(Some(event));
                }

                // Nothing left owed and no source to draw from: the run is over.
                *this.terminated = true;
                return Poll::Ready(None);
            }

            // The peeked item is buffered, so this returns it synchronously.
            return match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(timed)) => {
                    // A source event is the only thing that proves simulated time is advancing.
                    *this.since_source = 0;
                    Poll::Ready(Some(timed.value))
                }
                Poll::Ready(None) => {
                    *this.source_done = true;
                    continue;
                }
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

/// Decide whether emitting another account deliverable would exceed the feedback budget.
///
/// Returns the error to abandon the run with, or `None` to proceed. `None` on an empty queue or an
/// unregistered venue is deliberate: neither can emit anything, and [`pop_due`] already owns the
/// diagnostic for the latter.
fn check_feedback(
    venues: &FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: &BinaryHeap<Reverse<ScheduledEvent>>,
    since_source: usize,
    limit: usize,
) -> Option<BarterError> {
    if since_source < limit {
        return None;
    }

    let Reverse(next) = pending.peek()?;

    Some(BarterError::SimFeedbackLoop {
        exchange: venues.get(&next.exchange)?.venue.exchange,
        time: next.time,
        limit,
    })
}

impl<Source, MarketKind> FusedStream for SimRunner<Source, MarketKind>
where
    Source: Stream<Item = Timed<EngineEvent<MarketKind>>>,
{
    /// Terminated exactly once a poll has returned `Ready(None)`, which is the latch `poll_next`
    /// sets — so this cannot drift from the stream's actual behaviour.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

/// Whether a drain observed a request that abandons everything still scheduled.
#[derive(Debug, PartialEq, Eq)]
enum RequestDrain {
    Continue,
    Abandon,
}

/// Book every queued request against its venue, scheduling what each one produces.
///
/// # Why `rx.rx.try_recv()` rather than the receiver's `Iterator`
/// [`UnboundedRx`]'s `Iterator` impl `continue`s on `TryRecvError::Empty`, so `next()` spins
/// forever on a channel that is empty but still connected — which is the normal state here, on
/// every poll, for every venue that was not just sent a request.
fn drain_requests(
    venues: &mut FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
    clock: &HistoricalClock,
) -> RequestDrain {
    let now = clock.time();
    let mut drain = RequestDrain::Continue;

    for (exchange, slot) in venues.iter_mut() {
        while let Ok(request) = slot.request_rx.rx.try_recv() {
            // One arrival instant per request, so the venue's ledger and the events describing it
            // agree on when it happened.
            let arrives = checked_offset(now, slot.to_venue, "a request arriving at the venue");
            let delivers = checked_offset(
                arrives,
                slot.from_venue,
                "a venue response arriving at the Engine",
            );

            match request {
                // This runner already owes the Engine every scheduled deliverable and ends only
                // once it has emitted them, so a graceful drain has nothing left to ask for. The
                // Engine sends one in reply to the `Shutdown::AfterDrain` this runner itself emits
                // on source exhaustion; see the type's `# Termination`.
                ExecutionRequest::Drain => {}
                ExecutionRequest::Shutdown => drain = RequestDrain::Abandon,
                ExecutionRequest::Open(request) => {
                    let request = slot
                        .indexer
                        .order_request(&request)
                        .unwrap_or_else(|error| {
                            panic!(
                                "SimRunner received open request for non-configured key: {error}"
                            )
                        })
                        .into_owned_instrument();

                    slot.venue.advance_time(arrives);

                    schedule(
                        pending,
                        seq,
                        *exchange,
                        delivers,
                        slot.venue.open_order(request),
                        Deliverable::OpenResponse,
                    );
                }
                ExecutionRequest::Cancel(request) => {
                    let request = slot
                        .indexer
                        .order_request(&request)
                        .unwrap_or_else(|error| {
                            panic!(
                                "SimRunner received cancel request for non-configured key: {error}"
                            )
                        })
                        .into_owned_instrument();

                    slot.venue.advance_time(arrives);

                    schedule(
                        pending,
                        seq,
                        *exchange,
                        delivers,
                        slot.venue.cancel_order(request),
                        Deliverable::CancelResponse,
                    );
                }
            }
        }
    }

    drain
}

/// Apply a simulated latency offset, refusing to schedule an event before its own cause.
///
/// The async driver falls back to the unoffset instant on overflow, which schedules a response at
/// or before the request that provoked it — an ordering the rest of this type guarantees cannot
/// happen. A timestamp that cannot absorb a millisecond offset is a corrupt dataset, so it is
/// reported rather than absorbed.
fn checked_offset(time: DateTime<Utc>, offset: TimeDelta, what: &str) -> DateTime<Utc> {
    time.checked_add_signed(offset).unwrap_or_else(|| {
        panic!("SimRunner cannot represent the simulated time of {what}: {time} + {offset}")
    })
}

/// Push everything one request owes onto the queue: its account events, then its response.
///
/// The response goes last and each item takes its own `seq`, so "the `Engine` has the response"
/// implies "the `Engine` has already seen every account event for that order".
///
/// `wrap` erases the response type at the queue rather than at the venue, which is what lets opens
/// and cancels — whose responses have nothing in common — share one queue.
fn schedule<Response>(
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
    exchange: ExchangeIndex,
    time: DateTime<Utc>,
    outcome: VenueOutcome<Response>,
    wrap: fn(Response) -> Deliverable,
) {
    let VenueOutcome { events, response } = outcome;

    for event in events {
        pending.push(Reverse(ScheduledEvent {
            time,
            seq: *seq,
            exchange,
            payload: Deliverable::Account(event),
        }));
        *seq += 1;
    }

    pending.push(Reverse(ScheduledEvent {
        time,
        seq: *seq,
        exchange,
        payload: wrap(response),
    }));
    *seq += 1;
}

/// Pop the earliest scheduled deliverable and index it for the `Engine`.
///
/// # Panics
/// Panics if the deliverable's venue is absent, or if its keys are absent from that venue's index —
/// see [`SimRunner`]'s `# Panics`.
fn pop_due<MarketKind>(
    venues: &FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
) -> Option<EngineEvent<MarketKind>> {
    let Reverse(ScheduledEvent {
        time: _,
        seq: _,
        exchange,
        payload,
    }) = pending.pop()?;

    let slot = venues.get(&exchange).unwrap_or_else(|| {
        panic!("SimRunner scheduled an event for an unregistered venue: {exchange}")
    });

    Some(match payload {
        Deliverable::Account(event) => index_account_event(&slot.indexer, event),
        Deliverable::OpenResponse(order) => {
            let Order {
                key,
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state,
            } = order;

            let key = slot.indexer.order_key(key).unwrap_or_else(|error| {
                panic!(
                    "SimRunner venue returned an open response for a non-configured key: {error}"
                )
            });
            let state = slot.indexer.order_state(state).unwrap_or_else(|error| {
                panic!("SimRunner venue returned an unindexable open response state: {error}")
            });

            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                exchange: key.exchange,
                kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
                    key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state,
                })),
            }))
        }
        Deliverable::CancelResponse(response) => {
            let response = slot
                .indexer
                .order_response_cancel(response)
                .unwrap_or_else(|error| {
                    panic!("SimRunner venue returned an unindexable cancel response: {error}")
                });

            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                exchange: response.key.exchange,
                kind: AccountEventKind::OrderCancelled(response),
            }))
        }
    })
}

/// Index one venue account event for the `Engine`.
///
/// # Panics
/// Panics if the event references an exchange, asset or instrument absent from this venue's own
/// index. Every one of them came from the [`IndexedInstruments`] this index was built from, so a
/// failure is a misconfiguration rather than a venue reporting something unexpected — and dropping
/// the event instead would silently lose a fill.
///
/// [`IndexedInstruments`]: rustrade_instrument::index::IndexedInstruments
fn index_account_event<MarketKind>(
    indexer: &AccountEventIndexer,
    event: UnindexedAccountEvent,
) -> EngineEvent<MarketKind> {
    let event = indexer.account_event(event).unwrap_or_else(|error| {
        panic!("SimRunner venue produced an unindexable AccountEvent: {error}")
    });

    EngineEvent::Account(AccountStreamEvent::Item(event))
}

/// Log a one-line summary of what each venue booked, for a run's post-mortem.
pub(crate) fn log_venue_summary(venues: &FnvIndexMap<ExchangeIndex, SimVenue>) {
    for (exchange, slot) in venues {
        info!(
            %exchange,
            exchange_id = %slot.venue.exchange,
            orders_booked = slot.venue.order_sequence(),
            "SimRunner venue finished"
        );
    }
}

#[cfg(test)]
// Test code: panicking on a bad fixture is acceptable, and an `expect` message names which
// invariant the fixture violated.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::engine::execution_tx::{ExecutionTxMap, MultiExchangeTxMap};
    use futures::{StreamExt, stream};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rustrade_data::{
        event::{DataKind, MarketEvent},
        streams::consumer::MarketStreamEvent,
        subscription::trade::PublicTrade,
    };
    use rustrade_execution::{
        AccountSnapshot,
        balance::{AssetBalance, Balance},
        client::mock::MockExecutionConfig,
        market::MarketSnapshot,
        order::{
            OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::{OrderRequestOpen, RequestOpen},
        },
    };
    use rustrade_instrument::{
        Side, asset::name::AssetNameExchange, index::IndexedInstruments,
        instrument::InstrumentIndex, test_utils::instrument,
    };
    use rustrade_integration::channel::Tx;

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    /// The single fixture instrument's index.
    ///
    /// Asserted against the built index in [`Harness::new`], so a test can name the instrument
    /// while writing its source script — before the harness that indexes it exists.
    fn instrument_key() -> InstrumentIndex {
        InstrumentIndex::new(0)
    }

    fn funded(asset: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
        AssetBalance {
            asset: AssetNameExchange::new(asset),
            balance: Balance::new(amount, amount),
            time_exchange: at(0),
        }
    }

    fn config(latency_ms: u64) -> MockExecutionConfig {
        MockExecutionConfig {
            mocked_exchange: EXCHANGE,
            initial_state: AccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![funded("btc", dec!(100)), funded("usdt", dec!(10_000_000))],
                instruments: vec![],
            },
            latency_ms,
            fee_model: Default::default(),
            fill_model: Default::default(),
        }
    }

    /// A market `Item` at `millis`, priced at `price`.
    fn market(millis: i64, price: Decimal) -> Timed<EngineEvent<DataKind>> {
        Timed::new(
            EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
                time_exchange: at(millis),
                time_received: at(millis),
                exchange: EXCHANGE,
                instrument: instrument_key(),
                kind: DataKind::Trade(PublicTrade {
                    id: "t".into(),
                    price,
                    amount: dec!(0.01),
                    side: None,
                }),
            })),
            at(millis),
        )
    }

    /// An auxiliary (class 0) source event at `millis`.
    fn expiry(millis: i64) -> Timed<EngineEvent<DataKind>> {
        Timed::new(EngineEvent::ContractExpiry(instrument_key()), at(millis))
    }

    /// A compact label for an emitted event, so a test asserts the *sequence* a run produces rather
    /// than restating each event's contents — which is what every ordering guarantee here is about.
    fn label(event: &EngineEvent<DataKind>) -> &'static str {
        match event {
            EngineEvent::Market(_) => "market",
            EngineEvent::ContractExpiry(_) => "aux",
            EngineEvent::Shutdown(Shutdown::AfterDrain) => "after_drain",
            EngineEvent::Shutdown(Shutdown::Immediate) => "shutdown",
            EngineEvent::Account(AccountStreamEvent::Item(event)) => match &event.kind {
                AccountEventKind::Snapshot(_) => "snapshot",
                AccountEventKind::BalanceSnapshot(_) => "balance",
                AccountEventKind::Trade(_) => "trade",
                AccountEventKind::OrderSnapshot(_) => "order",
                AccountEventKind::OrderCancelled(_) => "cancelled",
                _ => "account_other",
            },
            _ => "other",
        }
    }

    type TestSource = stream::Iter<std::vec::IntoIter<Timed<EngineEvent<DataKind>>>>;

    /// A `SimRunner` over one simulated spot venue, plus the pieces the `Engine` would own.
    ///
    /// Requests are queued through the real [`MultiExchangeTxMap`] and the clock is advanced by
    /// hand, so a test drives the runner exactly as `Engine::process` does: read an event, advance
    /// simulated time to it, send whatever it provoked, read the next.
    struct Harness {
        runner: SimRunner<TestSource, DataKind>,
        txs: MultiExchangeTxMap,
        clock: HistoricalClock,
        exchange: ExchangeIndex,
    }

    impl Harness {
        fn new(latency_ms: u64, source: Vec<Timed<EngineEvent<DataKind>>>) -> Self {
            let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
            assert_eq!(
                instruments.instruments()[0].key,
                instrument_key(),
                "the fixture's source scripts name this index before the index exists"
            );

            let SimExecutionBuild {
                execution_tx_map,
                venues,
            } = SimExecutionBuilder::new(&instruments)
                .add_venue(config(latency_ms))
                .unwrap()
                .build();

            let exchange = *venues
                .keys()
                .next()
                .expect("one venue was added, so one is built");
            let clock = HistoricalClock::new(at(0));

            Self {
                runner: SimRunner::new(venues, stream::iter(source), clock.clone()),
                txs: execution_tx_map,
                clock,
                exchange,
            }
        }

        /// Stand in for the `Engine`: queue a market buy on the venue's channel, stamped with the
        /// price the strategy saw. `Engine::send_requests` does this synchronously inside `process`,
        /// which is why the runner observes it on the very next poll.
        fn send_open(&self, price: Decimal) {
            self.send(ExecutionRequest::Open(OrderRequestOpen {
                key: OrderKey {
                    exchange: self.exchange,
                    instrument: instrument_key(),
                    strategy: StrategyId::new("test"),
                    cid: ClientOrderId::random(),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    // `None` by construction for a Market order; the venue prices it from `market`.
                    price: None,
                    quantity: dec!(0.01),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: false,
                    market: Some(MarketSnapshot::new(None, None, Some(price))),
                },
            }));
        }

        fn send(&self, request: ExecutionRequest) {
            self.txs
                .find(&self.exchange)
                .expect("the fixture venue is routable")
                .send(request)
                .expect("the runner holds the receiver for the whole test");
        }

        /// Read the next event's label, as the engine's feed would read the event itself.
        async fn next(&mut self) -> Option<&'static str> {
            self.runner.next().await.as_ref().map(label)
        }

        /// Drain the rest of the run into labels.
        async fn rest(&mut self) -> Vec<&'static str> {
            let mut labels = Vec::new();
            while let Some(event) = self.runner.next().await {
                labels.push(label(&event));
            }
            labels
        }
    }

    /// Equal times must not decide the order between themselves: a `BinaryHeap` is not a stable
    /// sort, so without `seq` two deliverables booked at one instant pop in an unspecified order —
    /// and the balance of a fill could follow the response that reports it.
    #[test]
    fn scheduled_events_pop_in_time_then_seq_order() {
        fn filler() -> Deliverable {
            Deliverable::Account(UnindexedAccountEvent {
                exchange: EXCHANGE,
                kind: AccountEventKind::Snapshot(AccountSnapshot {
                    exchange: EXCHANGE,
                    balances: vec![],
                    instruments: vec![],
                }),
            })
        }

        let mut heap = BinaryHeap::new();
        for (millis, seq) in [(20, 5), (10, 1), (10, 3), (10, 2), (5, 9)] {
            heap.push(Reverse(ScheduledEvent {
                time: at(millis),
                seq,
                exchange: ExchangeIndex::new(0),
                payload: filler(),
            }));
        }

        let popped = std::iter::from_fn(|| heap.pop())
            .map(|Reverse(event)| (event.time.timestamp_millis(), event.seq))
            .collect::<Vec<_>>();

        assert_eq!(popped, vec![(5, 9), (10, 1), (10, 2), (10, 3), (20, 5)]);
    }

    /// The venue's opening account state must reach the `Engine` before any market data.
    ///
    /// Besides seeding balances and instruments it is what marks the venue's account connection
    /// healthy — `ConnectivityStates::update_from_account_event` flips on *any* account event — and
    /// an `Engine` that never sees one waits on it forever.
    #[tokio::test]
    async fn venue_snapshots_are_emitted_before_any_source_event() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100))]);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));
    }

    /// The ordering this type exists for: a fill stamped at `T` reaches the `Engine` before the
    /// market event at `T`.
    ///
    /// Otherwise the market event marks a position the `Engine` does not yet hold, and the terminal
    /// `pnl_unrealised` describes a position the run never had. This is #289 in one assertion.
    #[tokio::test]
    async fn an_account_event_precedes_the_market_event_at_the_same_instant() {
        // Two events share instant 10: the one that provokes the order, and the one the fill must
        // overtake.
        let mut harness = Harness::new(0, vec![market(10, dec!(100)), market(10, dec!(100))]);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        // What the Engine does while processing that market event.
        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec!["balance", "trade", "order", "market", "after_drain"],
            "the whole fill precedes the second event at instant 10"
        );
    }

    /// Auxiliary events keep the tie, matching the merge this runner reads from: a corporate action
    /// or expiry adjusts a position before any fill stamped at that instant is applied to it.
    #[tokio::test]
    async fn an_aux_event_precedes_an_account_event_at_the_same_instant() {
        let mut harness = Harness::new(
            0,
            vec![market(10, dec!(100)), expiry(10), market(30, dec!(100))],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec!["aux", "balance", "trade", "order", "market", "after_drain"],
            "the aux event at instant 10 leads the fill scheduled at instant 10"
        );
    }

    /// Simulated latency moves a fill to a later instant, and the source is read up to it.
    ///
    /// The same script run at zero latency delivers the fill before the intervening market event;
    /// at 200ms it delivers after. Nothing sleeps either way — the offsets are applied to the queue
    /// key, so the two runs take the same wall-clock time.
    #[tokio::test]
    async fn simulated_latency_shifts_delivery_past_an_intervening_market_event() {
        async fn run(latency_ms: u64) -> Vec<&'static str> {
            let mut harness = Harness::new(
                latency_ms,
                vec![
                    market(0, dec!(100)),
                    market(100, dec!(100)),
                    market(300, dec!(100)),
                ],
            );

            assert_eq!(harness.next().await, Some("snapshot"));
            assert_eq!(harness.next().await, Some("market"));

            harness.clock.advance_to(at(0));
            harness.send_open(dec!(100));

            harness.rest().await
        }

        assert_eq!(
            run(0).await,
            vec![
                "balance",
                "trade",
                "order",
                "market",
                "market",
                "after_drain"
            ],
            "at zero latency the fill is visible at the instant it was requested"
        );

        // 200ms is split into two 100ms legs, so the round trip lands at instant 200 — past the
        // event at 100, before the one at 300.
        assert_eq!(
            run(200).await,
            vec![
                "market",
                "balance",
                "trade",
                "order",
                "market",
                "after_drain"
            ],
            "a 200ms round trip delivers the fill between the events at 100 and 300"
        );
    }

    /// Exhausting the source asks the `Engine` to stop generating, and only then drains.
    ///
    /// Emitting `Shutdown::AfterDrain` first is what bounds the drain: the `Engine` suppresses
    /// further algo orders on that event, so nothing new is booked while the queue empties. A fill
    /// booked against the final source event is still delivered in full, which is the guarantee
    /// that stops a run ending with its ledgers disagreeing.
    #[tokio::test]
    async fn source_exhaustion_asks_the_engine_to_stop_before_the_queue_is_drained() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100))]);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        // Booked against the very last source event.
        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec!["after_drain", "balance", "trade", "order"],
            "the Engine is told to stop first, then everything owed is delivered"
        );
    }

    /// The termination latch reports only what `poll_next` has actually done, so a `select!` over
    /// this stream cannot drop the last event by believing it ended early.
    #[tokio::test]
    async fn is_terminated_latches_only_once_a_poll_has_returned_none() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100))]);

        assert!(!harness.runner.is_terminated());
        assert_eq!(harness.next().await, Some("snapshot"));
        assert!(!harness.runner.is_terminated());

        assert_eq!(harness.rest().await, vec!["market", "after_drain"]);
        assert!(harness.runner.is_terminated());
    }

    /// A graceful drain has nothing to ask of this runner: it already owes the `Engine` everything
    /// scheduled and ends only once it has delivered it.
    #[tokio::test]
    async fn drain_is_a_no_op() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100))]);

        assert_eq!(harness.next().await, Some("snapshot"));
        harness.send(ExecutionRequest::Drain);

        assert_eq!(
            harness.rest().await,
            vec!["market", "after_drain"],
            "the run continues unchanged"
        );
    }

    /// `Shutdown` abandons whatever is still scheduled, matching its documented meaning — the
    /// abrupt counterpart to the drain above.
    #[tokio::test]
    async fn shutdown_abandons_whatever_is_still_scheduled() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100))]);

        assert_eq!(harness.next().await, Some("snapshot"));

        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));
        harness.send(ExecutionRequest::Shutdown);

        assert_eq!(
            harness.rest().await,
            Vec::<&str>::new(),
            "the fill was booked but is abandoned unread, and no source event follows"
        );
        assert!(harness.runner.is_terminated());
    }

    /// A strategy trading on its own fills at zero latency is reported rather than spun on.
    ///
    /// Each delivered account event provokes another order whose outcome is stamped at the same
    /// instant, so it outranks every later source event: simulated time stops and the queue grows
    /// without bound. The limit is lowered here to make the runaway three events long instead of
    /// ten thousand; `a_strategy_trading_on_its_own_fills_is_reported_rather_than_hung` covers the
    /// same condition end to end at the real default.
    #[tokio::test]
    async fn the_feedback_limit_reports_a_zero_delay_cycle_rather_than_emitting_forever() {
        let mut harness = Harness::new(0, vec![market(10, dec!(100)), market(20, dec!(100))]);
        harness.runner = harness.runner.with_feedback_limit(3);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        // Two fills' worth of deliverables, all stamped at instant 10, all ahead of the event at 20.
        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));
        harness.send_open(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec!["balance", "trade", "order"],
            "exactly `limit` account events are emitted with no source event between them"
        );

        assert!(harness.runner.is_terminated());
        assert_eq!(
            harness.runner.error(),
            Some(&BarterError::SimFeedbackLoop {
                exchange: EXCHANGE,
                time: at(10),
                limit: 3,
            }),
            "the report names the venue and the instant simulated time stopped at"
        );
    }

    /// A source event resets the budget, so a strategy that trades steadily is not mistaken for a
    /// runaway however long the run is.
    #[tokio::test]
    async fn a_source_event_resets_the_feedback_budget() {
        let mut harness = Harness::new(
            0,
            vec![
                market(10, dec!(100)),
                market(20, dec!(100)),
                market(30, dec!(100)),
            ],
        );
        harness.runner = harness.runner.with_feedback_limit(3);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        // Three deliverables at instant 10 — exactly the budget, and no more.
        harness.clock.advance_to(at(10));
        harness.send_open(dec!(100));
        assert_eq!(harness.next().await, Some("balance"));
        assert_eq!(harness.next().await, Some("trade"));
        assert_eq!(harness.next().await, Some("order"));

        // The next source event proves time is advancing, so the next fill starts from zero again.
        assert_eq!(harness.next().await, Some("market"));
        harness.clock.advance_to(at(20));
        harness.send_open(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec!["balance", "trade", "order", "market", "after_drain"],
            "a second full fill is delivered, so the budget was reset rather than accumulated"
        );
        assert_eq!(harness.runner.error(), None);
    }
}
