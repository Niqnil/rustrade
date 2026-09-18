use crate::{
    EngineEvent, Timed,
    engine::clock::{EngineClock, HistoricalClock},
    error::BarterError,
    execution::{AccountStreamEvent, request::ExecutionRequest},
    shutdown::Shutdown,
};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::{Stream, stream::FusedStream};
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_execution::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    exchange::mock::{SimulatedVenue, VenueOutcome},
    indexer::AccountEventIndexer,
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::UnindexedOrderState,
    },
};
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
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
pub mod market;

pub use builder::{SimExecutionBuild, SimExecutionBuilder};
pub use market::VenueMarketUpdate;

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
    ///
    /// The venue really does act at that later instant, against the market it holds then — see
    /// [`SimRunner`]'s `# A request is booked when it arrives`.
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

/// A request the `Engine` has issued and its venue has not yet acted on.
///
/// Indexed and owned at the moment it was drained rather than when it is acted on, so a
/// misconfigured key is reported against the request that carried it — see [`SimRunner`]'s
/// `# Panics`.
#[derive(Debug)]
enum Action {
    Open(OrderRequestOpen<ExchangeId, InstrumentNameExchange>),
    Cancel(OrderRequestCancel<ExchangeId, InstrumentNameExchange>),
}

/// What one queue entry holds: something owed to the `Engine`, or something owed to a venue.
///
/// Both directions share one queue because both are events on one simulated timeline and their
/// relative order is the thing that has to be decided. Splitting them into two queues would make
/// "is this order booked against the market as of its arrival instant" a comparison between two
/// data structures rather than a pop from one.
#[derive(Debug)]
enum Payload {
    /// Engine-bound: emitted by [`SimRunner`] as an [`EngineEvent`].
    Deliver(Deliverable),
    /// Venue-bound: executed against [`SimVenue::venue`], and never emitted.
    Act(Action),
}

/// One payload and the simulated instant it comes due.
#[derive(Debug)]
struct ScheduledEvent {
    time: DateTime<Utc>,
    /// Precedence within one instant. See [`SimRunner`]'s ordering contract.
    ///
    /// Part of [`Ord`] rather than of the peek alone. Comparing it only where the queue is
    /// weighed against the source would let [`BinaryHeap::peek`] answer with a low-priority entry
    /// while a high-priority one sat behind it at the same instant — #289 reintroduced silently,
    /// and invisible to the result-stability fixture, whose strategy sends one order kind.
    class: u8,
    /// Monotone tie-break within one instant and class. Ordering on `(time, class)` alone would be
    /// unspecified: a [`BinaryHeap`] is not a stable sort, so equal keys pop in an arbitrary order.
    seq: u64,
    exchange: ExchangeIndex,
    payload: Payload,
}

impl ScheduledEvent {
    /// The key this entry is ordered by, and the one a peek is compared against.
    fn key(&self) -> (DateTime<Utc>, u8, u64) {
        (self.time, self.class, self.seq)
    }
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
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
        self.key().cmp(&other.key())
    }
}

/// Default [`SimRunner::with_feedback_limit`]: account deliverables emitted with no intervening
/// source event before a run is abandoned as a zero-delay feedback cycle.
///
/// Generous enough that no realistic strategy reaches it — it allows roughly three thousand orders
/// opened simultaneously against one market event — and small enough that a runaway is reported in
/// milliseconds rather than filling memory first.
///
/// # Resting fills spend from the same budget
/// A tick that crosses many resting orders at once emits three deliverables per fill, so around
/// three thousand simultaneous fills reach this limit too. The budget is per source event —
/// `since_source` is reset when a source event is returned, *before* that event is routed to the
/// venues — so the accounting is right, but a run abandoned this way is reporting a wide book
/// rather than the feedback cycle [`BarterError::SimFeedbackLoop`] names. Raise the limit with
/// [`SimRunner::with_feedback_limit`] if that is the case.
///
/// # Booking a request does not spend from it
/// Only what the `Engine` is shown is counted. A venue acting on a request emits nothing, and
/// every action produces at least a response, so a cycle is still bounded by the deliverables it
/// generates — and the limit means the same number of emitted events it always did.
pub const DEFAULT_FEEDBACK_LIMIT: usize = 10_000;

/// Precedence of a venue-bound action within one instant. See [`SimRunner`]'s ordering contract.
///
/// Lowest, so a venue has acted on everything it was handed for instant `T` before the `Engine` is
/// shown anything stamped `T` — which is what the drain-then-peek order of an earlier design did
/// structurally, and what keeps this change from moving any existing result.
const CLASS_ACTION: u8 = 0;

/// Precedence of an auxiliary or control event within one instant.
const CLASS_AUX: u8 = 1;

/// Precedence of an account deliverable within one instant.
const CLASS_ACCOUNT: u8 = 2;

/// Precedence of a market data event within one instant.
const CLASS_MARKET: u8 = 3;

/// Precedence of a source event within one instant. See [`SimRunner`]'s ordering contract.
fn source_class<MarketKind>(event: &EngineEvent<MarketKind>) -> u8 {
    match event {
        // Market data is marked to last, so a fill at the same instant is already in the position
        // it prices. This is the ordering #289 is about.
        EngineEvent::Market(_) => CLASS_MARKET,
        // An account event injected into the source ranks with the ones this runner schedules.
        EngineEvent::Account(_) => CLASS_ACCOUNT,
        // Auxiliary and control events lead, matching the merge this runner reads from: a stock
        // split adjusts a position before any fill stamped at that instant is applied to it.
        _ => CLASS_AUX,
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
/// receivers. The next poll therefore observes them and queues each one at the instant its venue
/// will act on it — all before the next source event is drawn.
///
/// Forwarding the source through a channel instead, as a live system does, hands that decision to
/// the tokio scheduler: an always-ready market source races arbitrarily far ahead of the engine and
/// account events land at a scheduler-determined index within it. Balance and position *quantities*
/// survive that, because they do not depend on where in the sequence a fill lands; every
/// time-derived statistic does not.
///
/// # A request is booked when it arrives
///
/// Observing a request and acting on it are separate instants. A request the `Engine` issued at
/// `T` is queued the moment it is observed, but stamped `T + to_venue`, and the venue does not see
/// it until the queue reaches that instant — by which time every source event up to it has been
/// routed. So an order is matched against the market its venue actually holds when it arrives.
///
/// Three things follow that are otherwise unrepresentable, and each is pinned by a test:
///
/// - **A tick that prints while a request is in flight cannot fill it.** The order is not on the
///   book yet. Booking on the poll that observed the request instead would let liquidity that was
///   gone before the order existed trade against it.
/// - **An order that is marketable when it arrives crosses as the aggressor**, paying the book,
///   rather than resting at a price the market had already left and being filled there later.
/// - **A cancel can lose to a fill.** A fill struck between a cancel being sent and arriving
///   retires the order first, and the cancel is answered
///   [`ApiError::OrderAlreadyFullyFilled`](rustrade_execution::error::ApiError::OrderAlreadyFullyFilled).
///   This is the race `to_venue` exists to model; booking on observation made the cancel win every
///   time, at any latency.
///
/// At `latency_ms: 0` the two instants coincide on every request, so none of this moves a result
/// that a zero-latency run already produced.
///
/// # Ordering contract
/// Queue entries and source events are merged on `(time, class)`, where `class` breaks a tie within
/// one instant:
///
/// | class | entries |
/// |---|---|
/// | 0 | venue-bound actions — the requests this runner has yet to book |
/// | 1 | auxiliary and control — corporate actions, contract expiries |
/// | 2 | account — balances, trades, order responses |
/// | 3 | market data |
///
/// Actions leading is the tie-break described above: of the things stamped `T`, a venue acts on
/// what it was handed before the `Engine` is shown anything. Auxiliary events keeping the next tie
/// matches the source merge this runner reads from: a stock split must adjust a position before any
/// fill stamped at that instant is applied to it. Account events preceding market data at one
/// instant is the rule this type exists for — a fill stamped at `T` must reach the `Engine` before
/// the market event at `T` marks the resulting position to that price, or the terminal
/// `pnl_unrealised` describes a position the run did not hold.
///
/// `class` is compared inside the queue's own ordering, not only where the queue is weighed against
/// the source. Comparing it at the peek alone would let the queue hand back a low-priority entry
/// while a high-priority one sat behind it at the same instant.
///
/// Within one instant and class, delivery follows booking order: every account event a request
/// produced precedes that request's response, so "the `Engine` has the response" implies "the
/// `Engine` has already seen every account event for that order".
///
/// ## Only three of the four classes are ever emitted
///
/// An action is a queue entry, not an event: it is run against its venue and produces the next
/// entries, and the `Engine` never sees it. It shares the queue because both directions are events
/// on one simulated timeline and their relative order is precisely the thing that has to be
/// decided.
///
/// ## A fill caused by market data does not add a class of its own
///
/// A resting order filled by a tick looks at first like an exception to "account leads market",
/// since it must follow the very event that caused it. It is not, and the four claims below are
/// the whole of why.
///
/// 1. **`class` is within-instant precedence between a deliverable and the *source*, not a record
///    of what caused what.** It answers "of the things stamped `T`, which does the `Engine` see
///    first", and nothing else. Causality is not expressible in it and does not need to be.
/// 2. **Every scheduled deliverable ranks as an account event, whatever produced it.** A fill
///    provoked by a request and a fill provoked by a tick are both account events to the `Engine`
///    and are marked identically.
/// 3. **Causality is structural, not a matter of priority.** Market state is applied and matched
///    when an event is **emitted**, never when it is peeked. So anything a tick enqueues is
///    enqueued during the poll that returns that tick, and is therefore drawn from the queue only
///    on a later poll — after the tick, at any latency, without any rule saying so.
/// 4. **At `from_venue = 0` it is delivered immediately after its tick and before the remaining
///    source events at that instant**, because the fill and those events share an instant and the
///    account class beats the market one. So the position the fill opens is still marked to `T`,
///    which is the property the contract is really about.
///
/// Claims 3 and 4 together are the contract for a tick-caused fill: booked at `T`, marked at `T`,
/// and never delivered before its own cause.
///
/// # Latency is simulated, never slept
/// A request issued at `T` reaches its venue at `T + to_venue` and its result becomes visible at
/// `T + to_venue + from_venue`. Both are simulated offsets applied to the queue key, so a run's
/// wall-clock duration does not scale with the latency being modelled, and delivery order is a
/// function of the dataset alone.
///
/// `to_venue` is price-relevant, not merely a delivery delay: it is the interval during which the
/// market may move away from an order that has not arrived — see `# A request is booked when it
/// arrives`.
///
/// A fill nothing requested — a resting order crossed by a tick — pays `from_venue` alone. There
/// was no request to carry to the venue, so there is no such leg to charge, and the fill becomes
/// visible at `time_exchange + from_venue`.
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
/// meaning — including any request that has not yet reached its venue, which is therefore never
/// booked at all rather than booked and then left unreported.
///
/// ## A position opened during the drain is never marked
///
/// `Shutdown::AfterDrain` is emitted *before* the queue is emptied, so everything drained after it
/// is delivered with no market event left to follow. A position opened by any of it is therefore
/// booked but never marked, and `pnl_unrealised` — which is event-driven — keeps its post-trade
/// value.
///
/// This is a property of **termination**, not of resting orders: it applies identically to a market
/// order sent in reply to the last source event, and
/// `source_exhaustion_asks_the_engine_to_stop_before_the_queue_is_drained` has asserted it for one
/// since before limit orders existed. Fixing it would mean marking to the last known price when the
/// summary is generated, which belongs to the statistics layer. It cannot be fixed here: the only
/// thing this runner could do is emit a market event of its own, and that would be a fabricated
/// observation in a stream whose whole contract is that every market event came from the source.
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
    MarketKind: VenueMarketUpdate,
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
    /// Per-instrument market state, folded from the source and handed to the venues trading it.
    ///
    /// Keyed by instrument rather than by venue because the market is a property of the instrument:
    /// two venues trading it see one market, and folding the stream once is both cheaper and the
    /// only way they cannot disagree.
    market: FnvHashMap<InstrumentIndex, MarketKind::State>,
}

/// Manual because [`futures::stream::Peekable`] is [`Debug`] only when its `Source` is, and
/// demanding that of every caller would buy nothing: the field carries no state worth printing.
impl<Source, MarketKind> std::fmt::Debug for SimRunner<Source, MarketKind>
where
    Source: Stream,
    MarketKind: VenueMarketUpdate,
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
    MarketKind: VenueMarketUpdate,
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
    /// # Opening balances are stamped at the session start
    ///
    /// The seeded balances are re-stamped to `clock`'s instant, discarding the `time_exchange` each
    /// carried in the venue's configured initial state.
    ///
    /// A seeding snapshot is not an observation on the simulated timeline — it is the account's
    /// opening condition, and the session opens at `clock`'s instant by definition. The configured
    /// stamp describes when those balances were captured in the real world, which a simulation has
    /// no use for: the clock is seeded from the market and auxiliary sources, so a balance stamped
    /// before the first market event arrives behind a clock already wound forward to it, and one
    /// stamped after it would wind the clock past the session start before any market event had
    /// been seen.
    ///
    /// Leaving it alone was not a workable alternative. `time_engine_start` is derived from the
    /// dataset at run time, so a static configuration cannot be written to agree with it, and the
    /// mismatch surfaced as `HistoricalClock received out-of-order events` logged at ERROR — once
    /// per venue, per run — for a configuration that was never wrong.
    ///
    /// Orders carried in a configured initial state keep their own stamps; they contribute to
    /// [`AccountSnapshot::time_most_recent`](rustrade_execution::AccountSnapshot::time_most_recent)
    /// alongside the balances, so a seeded order stamped ahead of the session start still advances
    /// the clock.
    ///
    /// The stamp is load-bearing for more than the clock: the seeded balance is the first point in
    /// each asset's equity series, and a drawdown window opens at the instant of its running peak.
    /// So a seeding stamp is where every reported drawdown is measured *from*, and stamping it
    /// anywhere but the session start inflates
    /// [`Drawdown::duration`](crate::statistic::metric::drawdown::Drawdown::duration) by the gap.
    ///
    /// # Panics
    /// Panics if a venue's initial snapshot references an asset or instrument absent from that
    /// venue's own index — see the type-level `# Panics`.
    pub fn new(
        venues: FnvIndexMap<ExchangeIndex, SimVenue>,
        source: Source,
        clock: HistoricalClock,
    ) -> Self {
        let time_engine_start = clock.time();

        let seeding = venues
            .values()
            .map(|slot| {
                let mut snapshot = slot.venue.account_snapshot();
                for balance in &mut snapshot.balances {
                    balance.time_exchange = time_engine_start;
                }

                let snapshot = UnindexedAccountEvent {
                    exchange: slot.venue.exchange,
                    kind: AccountEventKind::Snapshot(snapshot),
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
            market: FnvHashMap::default(),
        }
    }

    /// Set how many account deliverables may be emitted with no intervening source event before the
    /// run is abandoned as a zero-delay feedback cycle. Defaults to [`DEFAULT_FEEDBACK_LIMIT`].
    ///
    /// Raise it for a strategy that legitimately opens more than a few thousand orders against a
    /// single market event, or for a book deep enough that one tick fills that many resting orders
    /// — see [`DEFAULT_FEEDBACK_LIMIT`]. Raising it will not rescue a true zero-delay cycle — that
    /// has no limit at which it terminates — so a run hitting even a large limit is otherwise
    /// reporting the configuration described in [`BarterError::SimFeedbackLoop`].
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
    MarketKind: VenueMarketUpdate,
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

            // Whether the queue's earliest entry comes due at or before the source's next event.
            // No longer only a question about account events: an entry may be a request its venue
            // has yet to act on, which has to be weighed against the source for the same reason —
            // it is due at an instant, and the market as of that instant is what it acts against.
            let queue_leads = if *this.source_done {
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
                    .is_some_and(|Reverse(next)| (next.time, next.class) <= ordering)
            };

            if queue_leads {
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

                match step(this.venues, this.pending, this.seq) {
                    Step::Emitted(event) => {
                        *this.since_source += 1;
                        return Poll::Ready(Some(event));
                    }
                    // A venue acted. Nothing to emit, and what it produced is now queued, so the
                    // next iteration weighs that against the source exactly as it did this one.
                    Step::Acted => continue,
                    // Nothing left owed and no source to draw from: the run is over.
                    Step::Empty => {
                        *this.terminated = true;
                        return Poll::Ready(None);
                    }
                }
            }

            // The peeked item is buffered, so this returns it synchronously.
            return match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(timed)) => {
                    // A source event is the only thing that proves simulated time is advancing.
                    *this.since_source = 0;

                    // Routed BEFORE the event is returned, which is the whole ordering rule: an
                    // order the `Engine` sends in reaction to the tick at `T` must be matched
                    // against the market as of `T`, not `T-1`. Returning first and routing on the
                    // next poll would reverse that at zero latency.
                    route_market(
                        this.venues,
                        this.market,
                        this.pending,
                        this.seq,
                        &timed.value,
                    );

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

/// Folds one source event into per-instrument market state, hands it to every venue trading it,
/// and schedules whatever resting orders it filled.
///
/// Anything that is not a market data item is ignored: an account event, a command or a reconnect
/// carries no price. A reconnect in particular is deliberately *not* treated as clearing the
/// market — a gap in a feed does not mean the instrument stopped having a price, and a venue that
/// forgot its market on every reconnect would refuse to match orders it should have matched.
///
/// A venue that does not trade the instrument is skipped: its own index is what decides, so a
/// runner driving several venues never leaks one venue's instruments into another's view.
///
/// # A tick-caused fill pays one latency leg, not two
/// `to_venue` is the delay from the `Engine` issuing a request to the venue acting on it. Nothing
/// was issued here — the market moved and the venue acted on its own book — so there is no such
/// leg to pay, and the fill becomes visible at `time_exchange + from_venue`. Charging both would
/// model a round trip that never happened, and at `from_venue = 0` the fill is delivered
/// immediately after the tick that caused it, which is what marks the position it opens to that
/// tick's price.
fn route_market<MarketKind>(
    venues: &mut FnvIndexMap<ExchangeIndex, SimVenue>,
    market: &mut FnvHashMap<InstrumentIndex, MarketKind::State>,
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
    event: &EngineEvent<MarketKind>,
) where
    MarketKind: VenueMarketUpdate,
{
    let EngineEvent::Market(MarketStreamEvent::Item(event)) = event else {
        return;
    };

    let state = market.entry(event.instrument).or_default();
    MarketKind::apply(state, event);
    let snapshot = MarketKind::snapshot(state);
    let depth = MarketKind::depth(state);

    for (exchange, slot) in venues.iter_mut() {
        // `Err` means this venue does not trade the instrument, which is ordinary on a multi-venue
        // run and not a misconfiguration.
        let Ok(name) = slot
            .indexer
            .map
            .find_instrument_name_exchange(event.instrument)
        else {
            continue;
        };

        // Cloned because `apply_market` keys on the owned name; the map borrow ends here.
        let name = name.clone();
        let fills = slot
            .venue
            .apply_market(&name, snapshot, depth, event.time_exchange);

        if fills.is_empty() {
            continue;
        }

        let delivers = checked_offset(
            event.time_exchange,
            slot.from_venue,
            "a resting fill arriving at the Engine",
        );

        schedule_events(pending, seq, *exchange, delivers, fills);
    }
}

/// Decide whether emitting another account deliverable would exceed the feedback budget.
///
/// Returns the error to abandon the run with, or `None` to proceed. `None` on an empty queue or an
/// unregistered venue is deliberate: neither can emit anything, and [`step`] already owns the
/// diagnostic for the latter.
///
/// # An action at the head is not what the budget counts
/// The budget counts what the `Engine` is *shown*, so a venue-bound action is let through: it
/// emits nothing, and reporting against it would name a request rather than the deliverable the
/// run stopped on. It cannot defer the guard indefinitely either — every action this runner
/// queues produces at least a response, so the head becomes a deliverable within one step and the
/// limit is reached on the same count it would have been.
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

    if matches!(next.payload, Payload::Act(_)) {
        return None;
    }

    Some(BarterError::SimFeedbackLoop {
        exchange: venues.get(&next.exchange)?.venue.exchange,
        time: next.time,
        limit,
    })
}

impl<Source, MarketKind> FusedStream for SimRunner<Source, MarketKind>
where
    Source: Stream<Item = Timed<EngineEvent<MarketKind>>>,
    MarketKind: VenueMarketUpdate,
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

/// Queue every request the `Engine` has issued at the instant its venue will act on it.
///
/// Nothing is booked here. A request is indexed, stamped `now + to_venue`, and pushed onto the
/// same queue the venues' output waits in, so that [`act`] runs it against the market as of its
/// own arrival instant rather than as of the instant it was sent — see [`SimRunner`]'s
/// `# A request is booked when it arrives`.
///
/// # Indexed here, acted on later
/// Translating the keys at this point keeps the panic on the poll that observed the request, where
/// the `Engine` call that produced it is still the nearest thing in the stack trace. It also keeps
/// [`Action`] free of the `Engine`'s index types, so nothing in the queue needs a venue to be
/// interpreted.
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

            let action = match request {
                // This runner already owes the Engine every scheduled deliverable and ends only
                // once it has emitted them, so a graceful drain has nothing left to ask for. The
                // Engine sends one in reply to the `Shutdown::AfterDrain` this runner itself emits
                // on source exhaustion; see the type's `# Termination`.
                ExecutionRequest::Drain => continue,
                ExecutionRequest::Shutdown => {
                    drain = RequestDrain::Abandon;
                    continue;
                }
                ExecutionRequest::Open(request) => Action::Open(
                    slot.indexer
                        .order_request(&request)
                        .unwrap_or_else(|error| {
                            panic!(
                                "SimRunner received open request for non-configured key: {error}"
                            )
                        })
                        .into_owned_instrument(),
                ),
                ExecutionRequest::Cancel(request) => Action::Cancel(
                    slot.indexer
                        .order_request(&request)
                        .unwrap_or_else(|error| {
                            panic!(
                                "SimRunner received cancel request for non-configured key: {error}"
                            )
                        })
                        .into_owned_instrument(),
                ),
            };

            pending.push(Reverse(ScheduledEvent {
                time: arrives,
                class: CLASS_ACTION,
                seq: *seq,
                exchange: *exchange,
                payload: Payload::Act(action),
            }));
            *seq += 1;
        }
    }

    drain
}

/// Run one request against its venue, scheduling everything that comes back.
///
/// Called only from [`step`], and only once the queue has established that `time` is the earliest
/// instant anything is due at — so every market event up to `time` has already been routed and the
/// venue's book is the one this request actually arrives to.
///
/// The venue's clock is advanced first, which is what makes a deadline an unconditional cutoff: an
/// order whose [`TimeInForce::GoodTillDate`] deadline fell between this request being sent and it
/// arriving is retired before the request is served, so a cancel for it is answered
/// `OrderAlreadyExpired` rather than succeeding. Nothing requested those expiries, so they pay
/// `from_venue` alone — the same one leg a tick-caused fill pays — and are scheduled ahead of this
/// request's own response, which they precede in fact as well as in the queue.
///
/// # Panics
/// Panics if `exchange` is not registered, or if a simulated timestamp overflows — see
/// [`SimRunner`]'s `# Panics`.
///
/// [`TimeInForce::GoodTillDate`]: rustrade_execution::order::TimeInForce::GoodTillDate
fn act(
    venues: &mut FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
    exchange: ExchangeIndex,
    time: DateTime<Utc>,
    action: Action,
) {
    let slot = venues.get_mut(&exchange).unwrap_or_else(|| {
        panic!("SimRunner scheduled an action for an unregistered venue: {exchange}")
    });

    let delivers = checked_offset(
        time,
        slot.from_venue,
        "a venue response arriving at the Engine",
    );

    schedule_events(
        pending,
        seq,
        exchange,
        delivers,
        slot.venue.advance_time(time),
    );

    match action {
        Action::Open(request) => schedule(
            pending,
            seq,
            exchange,
            delivers,
            slot.venue.open_order(request),
            Deliverable::OpenResponse,
        ),
        Action::Cancel(request) => schedule(
            pending,
            seq,
            exchange,
            delivers,
            slot.venue.cancel_order(request),
            Deliverable::CancelResponse,
        ),
    }
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

    schedule_events(pending, seq, exchange, time, events);

    pending.push(Reverse(ScheduledEvent {
        time,
        class: CLASS_ACCOUNT,
        seq: *seq,
        exchange,
        payload: Payload::Deliver(wrap(response)),
    }));
    *seq += 1;
}

/// Push account events onto the queue, in the order the venue gave them.
///
/// Split out of [`schedule`] because a fill caused by a market event has no response to accompany
/// it: nothing requested it, so there is nobody to answer. Giving [`schedule`] an
/// `Option<Response>` instead would make every request-driven caller state that it does have one.
fn schedule_events(
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
    exchange: ExchangeIndex,
    time: DateTime<Utc>,
    events: Vec<UnindexedAccountEvent>,
) {
    for event in events {
        pending.push(Reverse(ScheduledEvent {
            time,
            class: CLASS_ACCOUNT,
            seq: *seq,
            exchange,
            payload: Payload::Deliver(Deliverable::Account(event)),
        }));
        *seq += 1;
    }
}

/// What one pop from the queue did.
///
/// Popping used to mean emitting: the queue held only deliverables, so an entry and an
/// [`EngineEvent`] were the same thing. Now that a venue-bound action waits in the same queue, a
/// pop can advance the simulation without producing anything for the `Engine`, and the caller has
/// to be able to tell that from an empty queue — otherwise a run ends with its last orders
/// unbooked.
#[derive(Debug)]
// The large variant is the point: this is a return value, destructured at its single call site and
// either handed straight to `Poll::Ready` or dropped. Boxing it would buy stack bytes that
// `Poll::Ready(Some(event))` immediately re-materialises, and charge a heap allocation for every
// event a run emits.
#[allow(clippy::large_enum_variant)]
enum Step<MarketKind> {
    /// A deliverable was popped and indexed.
    Emitted(EngineEvent<MarketKind>),
    /// An action was popped and run against its venue. Nothing to emit; poll again.
    Acted,
    /// The queue is empty.
    Empty,
}

/// Pop the earliest queue entry: emit it if the `Engine` is owed it, run it if a venue is.
///
/// # Panics
/// Panics if the entry's venue is absent, or if a deliverable's keys are absent from that venue's
/// index — see [`SimRunner`]'s `# Panics`.
fn step<MarketKind>(
    venues: &mut FnvIndexMap<ExchangeIndex, SimVenue>,
    pending: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    seq: &mut u64,
) -> Step<MarketKind> {
    let Some(Reverse(ScheduledEvent {
        time,
        class: _,
        seq: _,
        exchange,
        payload,
    })) = pending.pop()
    else {
        return Step::Empty;
    };

    let payload = match payload {
        Payload::Act(action) => {
            act(venues, pending, seq, exchange, time, action);
            return Step::Acted;
        }
        Payload::Deliver(payload) => payload,
    };

    let slot = venues.get(&exchange).unwrap_or_else(|| {
        panic!("SimRunner scheduled an event for an unregistered venue: {exchange}")
    });

    Step::Emitted(match payload {
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
        exchange::mock::VenueInstrumentMarket,
        market::MarketSnapshot,
        order::{
            OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::{OrderRequestCancel, OrderRequestOpen, RequestCancel, RequestOpen},
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
        ///
        /// The stamp is decision-time provenance only. This runner drives a market-driven venue,
        /// which prices the fill from its own book — so `price` is what the strategy was looking
        /// at, not what it will pay.
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

        /// Queue a resting buy limit at `price`.
        ///
        /// It carries no `MarketSnapshot`: a limit order is judged and priced against the venue's
        /// own market, which this runner routes to it from the source.
        fn send_limit(&self, price: Decimal) -> ClientOrderId {
            let cid = ClientOrderId::random();
            self.send(ExecutionRequest::Open(OrderRequestOpen {
                key: OrderKey {
                    exchange: self.exchange,
                    instrument: instrument_key(),
                    strategy: StrategyId::new("test"),
                    cid: cid.clone(),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    price: Some(price),
                    quantity: dec!(0.01),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    position_id: None,
                    reduce_only: false,
                    market: None,
                },
            }));
            cid
        }

        /// A resting buy that retires itself at `expiry`, returning the id so it can be cancelled.
        fn send_limit_gtd(&self, price: Decimal, expiry: DateTime<Utc>) -> ClientOrderId {
            let cid = ClientOrderId::random();
            self.send(ExecutionRequest::Open(OrderRequestOpen {
                key: OrderKey {
                    exchange: self.exchange,
                    instrument: instrument_key(),
                    strategy: StrategyId::new("test"),
                    cid: cid.clone(),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    price: Some(price),
                    quantity: dec!(0.01),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodTillDate { expiry },
                    position_id: None,
                    reduce_only: false,
                    market: None,
                },
            }));
            cid
        }

        fn send_cancel(&self, cid: ClientOrderId) {
            self.send(ExecutionRequest::Cancel(OrderRequestCancel {
                key: OrderKey {
                    exchange: self.exchange,
                    instrument: instrument_key(),
                    strategy: StrategyId::new("test"),
                    cid,
                },
                state: RequestCancel { id: None },
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

        /// The fixture venue's own view of the fixture instrument, if anything has routed it one.
        fn venue_market(&self) -> Option<VenueInstrumentMarket> {
            self.runner
                .venues()
                .get(&self.exchange)
                .expect("the fixture venue is registered")
                .venue
                .market(&InstrumentNameExchange::new("btc_usdt"))
                .copied()
        }

        /// Drain the rest of the run into labels.
        async fn rest(&mut self) -> Vec<&'static str> {
            self.rest_events()
                .await
                .iter()
                .map(label)
                .collect::<Vec<_>>()
        }

        /// Drain the rest of the run into whole events, for the assertions a label cannot make —
        /// the price a fill was struck at, or which error a cancel was answered with.
        async fn rest_events(&mut self) -> Vec<EngineEvent<DataKind>> {
            let mut events = Vec::new();
            while let Some(event) = self.runner.next().await {
                events.push(event);
            }
            events
        }
    }

    /// The price of every trade in a run, in delivery order.
    fn trade_prices(events: &[EngineEvent<DataKind>]) -> Vec<Decimal> {
        events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::Trade(trade),
                    ..
                })) => Some(trade.price),
                _ => None,
            })
            .collect()
    }

    /// Every cancel response in a run, as `Ok(())` or the error it was refused with.
    fn cancel_outcomes(events: &[EngineEvent<DataKind>]) -> Vec<Result<(), String>> {
        events
            .iter()
            .filter_map(|event| match event {
                EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::OrderCancelled(response),
                    ..
                })) => Some(
                    response
                        .state
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.to_string()),
                ),
                _ => None,
            })
            .collect()
    }

    /// A filler deliverable, for tests that care about a queue entry's key rather than its
    /// contents.
    fn filler() -> Payload {
        Payload::Deliver(Deliverable::Account(UnindexedAccountEvent {
            exchange: EXCHANGE,
            kind: AccountEventKind::Snapshot(AccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![],
                instruments: vec![],
            }),
        }))
    }

    /// Equal times must not decide the order between themselves: a `BinaryHeap` is not a stable
    /// sort, so without `seq` two deliverables booked at one instant pop in an unspecified order —
    /// and the balance of a fill could follow the response that reports it.
    #[test]
    fn scheduled_events_pop_in_time_then_seq_order() {
        let mut heap = BinaryHeap::new();
        for (millis, seq) in [(20, 5), (10, 1), (10, 3), (10, 2), (5, 9)] {
            heap.push(Reverse(ScheduledEvent {
                time: at(millis),
                class: CLASS_ACCOUNT,
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

    /// `class` orders the queue itself, not only the comparison against the source.
    ///
    /// Comparing it at the peek alone would let the queue hand back a low-priority entry while a
    /// high-priority one sat behind it at the same instant — an action booked after the fill it
    /// was supposed to precede. The pins that would catch it elsewhere cannot: the result-stability
    /// fixture sends one order kind, so a class inversion leaves its artifact byte-identical.
    #[test]
    fn scheduled_events_pop_in_class_order_before_seq_order() {
        let mut heap = BinaryHeap::new();
        // Pushed with `seq` ascending *against* class, so an `Ord` that ignored class would pop
        // them in exactly the reverse of the expected order rather than coincidentally agreeing.
        for (class, seq) in [
            (CLASS_MARKET, 0),
            (CLASS_ACCOUNT, 1),
            (CLASS_AUX, 2),
            (CLASS_ACTION, 3),
        ] {
            heap.push(Reverse(ScheduledEvent {
                time: at(10),
                class,
                seq,
                exchange: ExchangeIndex::new(0),
                payload: filler(),
            }));
        }

        let popped = std::iter::from_fn(|| heap.pop())
            .map(|Reverse(event)| event.class)
            .collect::<Vec<_>>();

        assert_eq!(
            popped,
            vec![CLASS_ACTION, CLASS_AUX, CLASS_ACCOUNT, CLASS_MARKET],
            "at one instant the venue acts first and market data is marked last"
        );
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
            "the order is abandoned before its venue ever sees it, and no source event follows"
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

    /// A configured opening balance stamped before the session start must not arrive behind the
    /// clock.
    ///
    /// `time_engine_start` is derived from the dataset at run time, so a static configuration
    /// cannot be written to agree with it. Before the seeding snapshot was stamped at the session
    /// start, every such run logged `HistoricalClock received out-of-order events` at ERROR — once
    /// per venue, per run — for a configuration that was never wrong.
    #[tokio::test]
    async fn seeded_balances_are_stamped_at_the_session_start() {
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let SimExecutionBuild { venues, .. } = SimExecutionBuilder::new(&instruments)
            .add_venue(config(0))
            .unwrap()
            .build();

        // `config`'s balances are stamped `at(0)`; open the session long after that, which is the
        // ordinary case — opening balances are captured before the data the run replays.
        let session_start = at(60_000);
        let clock = HistoricalClock::new(session_start);
        let source: Vec<Timed<EngineEvent<DataKind>>> = Vec::new();
        let mut runner = SimRunner::new(venues, stream::iter(source), clock.clone());

        let event = runner
            .next()
            .await
            .expect("the seeding snapshot is queued ahead of every source event");

        let EngineEvent::Account(AccountStreamEvent::Item(event)) = event else {
            panic!("the first event a venue produces is its seeding account snapshot");
        };
        let AccountEventKind::Snapshot(snapshot) = event.kind else {
            panic!("the seeding account event is a snapshot");
        };

        assert_eq!(
            snapshot.balances.len(),
            2,
            "the fixture funds two assets, so two balances must be seeded"
        );
        for balance in &snapshot.balances {
            assert_eq!(
                balance.time_exchange, session_start,
                "a seeded balance is the account's opening condition, so it is stamped at the \
                 instant the session opens - not at whatever the configuration recorded"
            );
        }

        assert_eq!(
            snapshot.time_most_recent(),
            Some(session_start),
            "the snapshot the clock reads must be the session start, not an earlier instant"
        );
        assert_eq!(
            clock.time(),
            session_start,
            "the clock must be left where it started: a seeding snapshot is an opening condition, \
             not an observation that moves the simulated instant"
        );
    }

    /// The venue is fed each market event, and is fed it **before** the `Engine` is.
    ///
    /// The routing is behaviour-neutral today — nothing in the venue reads its market to price a
    /// fill — which is exactly why it needs pinning. A route that silently stopped working would
    /// change no result and fail no other test, right up until resting orders started matching
    /// against a market that was never delivered.
    #[tokio::test]
    async fn a_market_event_reaches_the_venues_before_the_engine_sees_it() {
        let mut harness = Harness::new(0, vec![market(1_000, dec!(50_000))]);

        assert!(
            harness.venue_market().is_none(),
            "a venue has no market until one is routed to it"
        );

        assert_eq!(
            harness.next().await,
            Some("snapshot"),
            "the seeding snapshot leads"
        );
        assert!(
            harness.venue_market().is_none(),
            "an account event carries no price, so it must not touch the venue's market"
        );

        assert_eq!(harness.next().await, Some("market"));

        let market = harness
            .venue_market()
            .expect("the venue must hold the market event just emitted");
        assert_eq!(
            market.time_exchange,
            at(1_000),
            "the venue's market is stamped with the event's own instant"
        );
        assert_eq!(
            market.snapshot.last_price,
            Some(dec!(50_000)),
            "a trade at 50,000 is the instrument's price"
        );
    }

    /// Later events replace earlier ones, so the venue tracks the market rather than its first
    /// sight of it.
    #[tokio::test]
    async fn the_venues_market_advances_with_the_source() {
        let mut harness = Harness::new(
            0,
            vec![market(1_000, dec!(50_000)), market(2_000, dec!(50_500))],
        );
        let _ = harness.rest().await;

        let market = harness.venue_market().expect("both events were routed");
        assert_eq!(market.time_exchange, at(2_000));
        assert_eq!(market.snapshot.last_price, Some(dec!(50_500)));
    }

    /// A fill caused by a market event is delivered *after* that event, because it cannot precede
    /// its own cause — and immediately after it, so the position it opens is marked to that tick.
    ///
    /// This is the one place the ordering contract's "account events lead market data" reads as an
    /// exception. It is not: the contract is about *marking*, and a fill delivered immediately
    /// after the tick that caused it is booked at that instant and marked at that instant.
    #[tokio::test]
    async fn a_resting_fill_follows_the_tick_that_caused_it() {
        let mut harness = Harness::new(
            0,
            vec![
                market(10, dec!(200)),
                market(20, dec!(90)),
                market(30, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        // 200 traded, so a bid at 100 is not marketable and rests.
        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec![
                // The reservation, then the response saying the order is working.
                "balance",
                "order",
                // The tick that crosses it...
                "market",
                // ...and only then the fill it caused, trade before terminal order.
                "balance",
                "trade",
                "order",
                "market",
                "after_drain"
            ],
            "the fill follows its cause, and its trade precedes the order it terminated"
        );
    }

    /// Within the instant that caused it, a resting fill still leads the remaining market events —
    /// so a position opened at `T` is marked to `T` rather than left unmarked until the next
    /// instant, which is the ordering #289 exists to prevent.
    ///
    /// This fails if the class of a scheduled deliverable is ever changed from account (1) to
    /// something ranking below market data.
    #[tokio::test]
    async fn a_resting_fill_precedes_the_remaining_market_events_at_its_instant() {
        let mut harness = Harness::new(
            0,
            vec![
                market(10, dec!(200)),
                market(20, dec!(90)),
                market(20, dec!(91)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec![
                "balance",
                "order", //
                "market",
                "balance",
                "trade",
                "order", //
                "market",
                "after_drain"
            ],
            "the fill leads the second market event at instant 20, which marks it to that instant"
        );
    }

    /// An auxiliary event at the fill's instant still leads it, exactly as it leads a fill provoked
    /// by a request: a corporate action or expiry adjusts a position before any fill stamped at
    /// that instant is applied to it.
    #[tokio::test]
    async fn an_aux_event_at_a_resting_fills_instant_still_leads() {
        let mut harness = Harness::new(
            0,
            vec![
                market(10, dec!(200)),
                expiry(20),
                market(20, dec!(90)),
                market(30, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec![
                "balance",
                "order", //
                "aux",
                "market",
                "balance",
                "trade",
                "order", //
                "market",
                "after_drain"
            ],
            "the aux event at instant 20 leads the tick that caused the fill, and so the fill"
        );
    }

    /// A tick-caused fill pays `from_venue` alone. Nothing was requested, so there is no
    /// `to_venue` leg to pay — charging both would model a round trip that never happened.
    #[tokio::test]
    async fn a_resting_fill_pays_one_latency_leg_not_two() {
        // 200ms round trip, so each leg is 100ms.
        let mut harness = Harness::new(
            200,
            vec![
                market(10, dec!(200)),
                market(500, dec!(90)),
                market(550, dec!(200)),
                market(700, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        assert_eq!(
            harness.rest().await,
            vec![
                // The open pays both legs: requested at 10, visible at 210.
                "balance",
                "order", //
                // The tick at 500 crosses it; the fill is visible at 600, so it lands after the
                // event at 550 and before the one at 700. Two legs would have put it at 700.
                "market",
                "market",
                "balance",
                "trade",
                "order", //
                "market",
                "after_drain"
            ],
            "the fill is visible one `from_venue` leg after the tick that caused it"
        );
    }

    /// A deadline reached by a market tick reaches the `Engine`, rather than being eaten.
    ///
    /// The order rests below the market and is never crossed, so nothing but its deadline can
    /// retire it.
    #[tokio::test]
    async fn a_deadline_reached_by_a_tick_is_delivered() {
        let mut harness = Harness::new(
            0,
            vec![
                market(10, dec!(200)),
                market(500, dec!(200)),
                market(700, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        // Far below the market, so only the deadline at 500 can retire it.
        harness.send_limit_gtd(dec!(1), at(500));

        assert_eq!(
            harness.rest().await,
            vec![
                "balance",
                "order",  // the open rests
                "market", // the tick at 500, which reaches the deadline as it is applied
                "balance",
                "order", // and so retires it: release, then the terminal snapshot
                "market",
                "after_drain"
            ],
            "an expiry follows the tick that reached it, exactly as a resting fill does — a \
             market is applied on emit, so anything it retires is drawn on a later poll"
        );
    }

    /// A deadline reached while advancing the clock for an unrelated request is delivered too.
    ///
    /// Nothing ticks between the open and the cancel, so only `advance_time`'s sweep can find it —
    /// this is the entry point a tick-driven sweep alone would miss.
    #[tokio::test]
    async fn a_deadline_reached_by_a_request_is_delivered_and_the_cancel_is_told_why() {
        let mut harness = Harness::new(0, vec![market(10, dec!(200)), market(900, dec!(200))]);

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        let cid = harness.send_limit_gtd(dec!(1), at(500));

        assert_eq!(harness.next().await, Some("balance"));
        assert_eq!(harness.next().await, Some("order"));

        // Past the deadline, with no tick in between.
        harness.clock.advance_to(at(600));
        harness.send_cancel(cid);

        assert_eq!(
            harness.rest().await,
            vec![
                "balance",
                "order",     // swept by the advance the cancel provoked
                "cancelled", // the cancel itself, answered `OrderAlreadyExpired`
                "market",
                "after_drain"
            ],
            "the sweep precedes the response to the request that provoked it"
        );
    }

    /// A tick between a request being sent and reaching its venue cannot fill it: the order was
    /// not on the book yet.
    ///
    /// The market dips through the limit at instant 50 and recovers by 100, while the order does
    /// not arrive until 110. Booking on the poll that observed the request instead would put the
    /// order on a book as of instant 10 and let the dip trade against it — a fill against
    /// liquidity that was gone before the order existed, and the strongest form of the look-ahead
    /// this runner exists to remove.
    #[tokio::test]
    async fn a_tick_before_an_order_arrives_cannot_fill_it() {
        // 200ms round trip, so each leg is 100ms.
        let mut harness = Harness::new(
            200,
            vec![
                market(10, dec!(200)),
                market(50, dec!(90)),
                market(100, dec!(200)),
                market(900, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        let events = harness.rest_events().await;

        assert_eq!(
            events.iter().map(label).collect::<Vec<_>>(),
            vec![
                // Both ticks precede the order's arrival at 110, so neither can see it.
                "market",
                "market",
                // It arrives to the recovered market and rests, visible at 210.
                "balance",
                "order",
                "market",
                "after_drain"
            ],
            "the order rests on the book it arrives to, having missed the dip entirely"
        );
        assert!(
            trade_prices(&events).is_empty(),
            "an order that was still in flight when the market dipped has traded nothing"
        );
    }

    /// An order that *is* marketable on arrival takes the book it arrives to, at that book's price.
    ///
    /// The mirror of the test above: the dip at instant 50 does not recover, so the order reaches
    /// the venue marketable and crosses as the aggressor. The price is what distinguishes the two
    /// bookings — an order resting first and being matched later fills at its own limit, because a
    /// maker is paid the price it posted, while an aggressor pays the book.
    #[tokio::test]
    async fn an_order_marketable_on_arrival_takes_the_book_it_arrives_to() {
        let mut harness = Harness::new(
            200,
            vec![
                market(10, dec!(200)),
                market(50, dec!(90)),
                market(900, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        let events = harness.rest_events().await;

        assert_eq!(
            events.iter().map(label).collect::<Vec<_>>(),
            vec![
                "market", //
                "balance",
                "trade",
                "order", //
                "market",
                "after_drain"
            ],
            "the order fills on arrival rather than resting"
        );
        assert_eq!(
            trade_prices(&events),
            vec![dec!(90)],
            "an aggressor pays the market it arrived to, not the limit it would have rested at"
        );
    }

    /// A tick stamped at the very instant an order arrives does not get to price it.
    ///
    /// The tie has to be broken somewhere and this runner breaks it towards the venue: of the
    /// things stamped `T`, a venue acts on what it was handed before it is shown anything else.
    /// That is what an earlier design did structurally by booking every request before the source
    /// was peeked, and keeping it is what makes booking at the arrival instant leave every
    /// existing result untouched at `latency_ms: 0`, where the two instants coincide on every
    /// request.
    #[tokio::test]
    async fn a_tick_at_the_arrival_instant_does_not_price_the_order() {
        let mut harness = Harness::new(
            200,
            vec![
                market(10, dec!(200)),
                // Stamped at exactly `10 + to_venue`.
                market(110, dec!(90)),
                market(900, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        harness.send_limit(dec!(100));

        let events = harness.rest_events().await;

        assert_eq!(
            events.iter().map(label).collect::<Vec<_>>(),
            vec![
                // The tick at 110, emitted after the venue has already booked the order against
                // the market as of instant 10 — the booking is not an emission, so it leaves no
                // label of its own.
                "market",
                // The order rested, visible one `from_venue` leg later at 210.
                "balance",
                "order", //
                // And the tick that followed it onto the book crossed it, also visible at 210.
                "balance",
                "trade",
                "order", //
                "market",
                "after_drain"
            ],
            "the order rests first and is crossed second, rather than arriving marketable"
        );
        assert_eq!(
            trade_prices(&events),
            vec![dec!(100)],
            "it rested first, so it was filled at its own limit rather than at the tick's price"
        );
    }

    /// A cancel loses to a fill that happened while it was in flight, and is told so.
    ///
    /// The race `to_venue` exists to model. The order is crossed at instant 340 by a tick the
    /// `Engine` has not seen the consequences of, while the cancel it sent at 300 does not reach
    /// the venue until 400. Booking the cancel on the poll that observed it would let it win every
    /// such race no matter how large the latency, which is the opposite of what modelling latency
    /// is for.
    #[tokio::test]
    async fn a_cancel_loses_to_a_fill_struck_while_it_was_in_flight() {
        let mut harness = Harness::new(
            200,
            vec![
                market(10, dec!(200)),
                market(300, dec!(200)),
                market(340, dec!(90)),
                market(900, dec!(200)),
            ],
        );

        assert_eq!(harness.next().await, Some("snapshot"));
        assert_eq!(harness.next().await, Some("market"));

        harness.clock.advance_to(at(10));
        let cid = harness.send_limit(dec!(100));

        assert_eq!(harness.next().await, Some("balance"));
        assert_eq!(harness.next().await, Some("order"));
        assert_eq!(harness.next().await, Some("market"));

        // Sent at 300, so it reaches the venue at 400 — after the tick at 340.
        harness.clock.advance_to(at(300));
        harness.send_cancel(cid);

        let events = harness.rest_events().await;

        assert_eq!(
            events.iter().map(label).collect::<Vec<_>>(),
            vec![
                // The tick at 340 crosses the resting order; the fill is visible at 440.
                "market",
                "balance",
                "trade",
                "order",
                // The cancel reaches the venue at 400 and is answered at 500.
                "cancelled",
                "market",
                "after_drain"
            ],
            "the fill is delivered whole, and the cancel is answered after it"
        );
        assert_eq!(
            trade_prices(&events),
            vec![dec!(100)],
            "the resting order filled at its own limit before the cancel could reach the venue"
        );
        assert_eq!(
            cancel_outcomes(&events),
            vec![Err("order rejected: order already fully filled".to_string())],
            "the cancel is told to reconcile against the fill rather than being granted"
        );
    }

    /// Latency is price-relevant: the same script at two latencies fills a market order at two
    /// prices, differing by exactly the ticks that printed while the order was in flight.
    ///
    /// This is the property modelling `to_venue` is *for*. While a market order was priced from
    /// the snapshot its own request carried, `latency_ms` changed only when a result was delivered
    /// and never what it was — so a backtest could raise it to any value and report identical
    /// fills, which is a silent way of saying latency does not matter.
    #[tokio::test]
    async fn market_order_fill_price_moves_by_the_ticks_a_request_flies_over() {
        async fn fill_price_at(latency_ms: u64) -> Decimal {
            let mut harness = Harness::new(
                latency_ms,
                vec![
                    market(0, dec!(100)),
                    // Inside the flight interval at 200ms (a 100ms leg), outside it at zero.
                    market(50, dec!(110)),
                    // Outside it at both: a venue pricing from here would be reading a tick the
                    // order cannot have reached, which is the look-ahead this design forbids.
                    market(150, dec!(999)),
                    market(400, dec!(100)),
                ],
            );

            assert_eq!(harness.next().await, Some("snapshot"));
            assert_eq!(harness.next().await, Some("market"));

            harness.clock.advance_to(at(0));
            // Deliberately not a price the venue holds at any instant: a fill here would mean the
            // request's own snapshot had priced it.
            harness.send_open(dec!(1));

            let events = harness.rest_events().await;
            let prices = trade_prices(&events);
            assert_eq!(prices.len(), 1, "the script sends exactly one market order");
            prices[0]
        }

        assert_eq!(
            fill_price_at(0).await,
            dec!(100),
            "with no flight time the order is priced at the market it was decided against"
        );
        assert_eq!(
            fill_price_at(200).await,
            dec!(110),
            "a 100ms outbound leg carries the order over the tick at 50, and it pays that market"
        );
    }
}
