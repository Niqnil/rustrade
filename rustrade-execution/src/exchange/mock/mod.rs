use crate::{
    UnindexedAccountEvent,
    client::mock::MockExecutionConfig,
    error::StreamTerminationReason,
    exchange::mock::{
        request::{MockExchangeRequest, MockExchangeRequestKind},
        venue::VenueRegime,
    },
    market::MarketObservation,
};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::stream::BoxStream;
use rustrade_instrument::{
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use std::{collections::VecDeque, fmt::Debug, sync::Arc, time::Duration};

/// The longest the market-driven loop sleeps for a venue deadline in one go, well inside the
/// longest a tokio timer can wait. A deadline beyond it is waited for in steps.
const MAX_DEADLINE_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

/// How far before a deadline, by the venue's clock, a wake for it still counts as due: the timer
/// and the clock are two sources and can differ by a hair.
const DEADLINE_TOLERANCE: TimeDelta = TimeDelta::seconds(1);
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info, warn};

pub mod account;
pub mod orders;
pub mod request;
pub mod venue;

pub use venue::{CancelOutcome, OpenOutcome, SimulatedVenue, VenueInstrumentMarket, VenueOutcome};

/// The clock a [`market-driven`](MockExchange::new_market_driven) [`MockExchange`] stamps
/// everything with.
pub type MockExchangeClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// One input from the market feed of a [`market-driven`](MockExchange::new_market_driven)
/// [`MockExchange`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MarketFeedEvent {
    /// The market of `instrument` as it now stands. See [`SimulatedVenue::apply_market`].
    Observation {
        instrument: InstrumentNameExchange,
        observation: MarketObservation,
    },
    /// The feed fell behind and dropped `missed` observations before this point.
    ///
    /// The venue never sees what they showed, so a price that crossed a resting order only in one
    /// of them filled nothing. See [`MockExchange::new_market_driven`] for what the driver does.
    Lagged { missed: u64 },
}

/// Asynchronous driver for a [`SimulatedVenue`]: channels, simulated latency, emission ordering.
///
/// It owns no ledger and prices nothing. What the venue models, what it rejects and what it leaves
/// unaccounted for are documented on [`SimulatedVenue`]; this type decides only *when* the venue's
/// output reaches a client.
///
/// It drives a venue in one of two [`VenueRegime`]s:
/// - [`new`](Self::new): request-priced, for a backtest that sends one request at a time. A
///   market order is priced from the snapshot its request carries, and nothing can rest.
/// - [`new_market_driven`](Self::new_market_driven): fed a live market, for paper trading. Limit
///   orders rest and fill as the market crosses them, as in a backtest of the same venue.
///
/// # Ordering
/// Everything the venue produces — account events and every response, fetches included — is queued
/// in the order the venue produced it and drained by a single task, so emission order equals
/// booking order. A response is emitted after the account events produced before it, so a client
/// holding a response has already been sent every event the venue booked ahead of it. Closing
/// `request_rx` closes that queue in turn, and [`run`](Self::run) returns only once it has drained.
pub struct MockExchange {
    /// The venue's state machine. Ledger, pricing and ordering obligations live here.
    pub venue: SimulatedVenue,
    /// Simulated round-trip delay. Transport policy, so it lives on the driver, not the venue.
    pub latency_ms: u64,
    pub request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
    pub event_tx: broadcast::Sender<UnindexedAccountEvent>,
    /// The market feed and clock of a market-driven driver, `None` for a request-priced one.
    market: Option<MarketDrive>,
}

/// Manual because the market feed is a boxed stream, which has no `Debug`.
impl Debug for MockExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockExchange")
            .field("venue", &self.venue)
            .field("latency_ms", &self.latency_ms)
            .field("request_rx", &self.request_rx)
            .field("event_tx", &self.event_tx)
            .field("market_driven", &self.market.is_some())
            .finish()
    }
}

/// What a market-driven driver reads besides its requests.
struct MarketDrive {
    feed: BoxStream<'static, MarketFeedEvent>,
    clock: MockExchangeClock,
}

impl MockExchange {
    /// A request-priced driver, building its venue from `config` with [`SimulatedVenue::new`].
    ///
    /// Each request is booked when it is received, at its own `time_request` plus half of
    /// `latency_ms`, and its output is emitted `latency_ms` of real time later.
    pub fn new(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self {
            venue: SimulatedVenue::new(&config, instruments),
            latency_ms: config.latency_ms,
            request_rx,
            event_tx,
            market: None,
        }
    }

    /// A driver that runs `venue` on a live market: `feed` supplies the market, and `clock` the
    /// time everything is stamped with.
    ///
    /// # Time
    /// The venue is stamped from `clock`, held so that it never moves backwards: an instant before
    /// the latest one already stamped is stamped with that latest one instead. A request's own
    /// `time_request` is not used. The clock is moved to `clock`'s instant when [`run`](Self::run)
    /// starts, so interest and holding costs accrue from then, and on each venue deadline (an
    /// order's expiry, a holding-cost or interest boundary) when it falls, whether or not anything
    /// else arrives.
    ///
    /// # Latency, as in a backtest of the same venue
    /// A request waits half of `latency_ms` of real time before it is booked, against the market
    /// held at that moment, and its output is emitted half of `latency_ms` after that. A fill the
    /// market causes on a resting order pays the second half alone. Integer division drops an odd
    /// millisecond from each half.
    ///
    /// # Market input
    /// Each [`MarketFeedEvent::Observation`] is applied with [`SimulatedVenue::apply_market`] as it
    /// arrives, ahead of a request that is due at the same moment. An instrument the feed has not
    /// yet observed refuses a market order, while a limit order on it rests.
    ///
    /// When the feed reports [`MarketFeedEvent::Lagged`], the driver logs an error and emits a
    /// [`StreamTerminated`](crate::AccountEventKind::StreamTerminated) after everything queued
    /// before it, which ends the account stream of a [`MockExecution`](crate::client::mock::MockExecution)
    /// client. The venue keeps running, and nothing it booked is lost: a client that reopens the
    /// stream can read its fills back. What the dropped observations showed is lost, so a resting
    /// order may have missed a fill. Fill recovery cannot repair that.
    ///
    /// When the feed ends, the driver logs a warning and serves requests on the last market it
    /// saw. Deadlines keep falling.
    ///
    /// The feed is served ahead of requests, so while it has a backlog a request is read, and its
    /// wait for the venue begins, only after that backlog is applied. The latency above is a lower
    /// bound. The backlog is bounded by the feed's own buffer.
    ///
    /// # Panics
    /// If `venue` is not [`VenueRegime::MarketDriven`]: a request-priced venue does not price from
    /// the feed, so feeding it would change nothing.
    pub fn new_market_driven(
        venue: SimulatedVenue,
        latency_ms: u64,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        feed: BoxStream<'static, MarketFeedEvent>,
        clock: MockExchangeClock,
    ) -> Self {
        assert_eq!(
            venue.regime(),
            VenueRegime::MarketDriven,
            "MockExchange::new_market_driven given a request-priced venue: build it with \
             SimulatedVenue::new_market_driven"
        );
        Self {
            venue,
            latency_ms,
            request_rx,
            event_tx,
            market: Some(MarketDrive { feed, clock }),
        }
    }

    /// Serves requests until the request channel closes, then lets queued output finish.
    ///
    /// Venue output is not emitted inline: it is queued and drained by a single task, so that
    /// emission order equals booking order. Closing the request channel closes that queue in turn,
    /// and this method returns only once the queue has drained, so shutting the venue down cannot
    /// strand a fill the client is still waiting on. A market-driven driver first books the
    /// requests still waiting out their latency.
    pub async fn run(mut self) {
        let (emit_tx, emit_rx) = mpsc::unbounded_channel();
        let emitter = tokio::spawn(Self::emit_queued_output(
            emit_rx,
            self.event_tx.clone(),
            self.venue.exchange,
        ));

        match self.market.take() {
            None => {
                let emit = Emit::new(emit_tx, self.venue.exchange, self.latency_ms);
                self.serve_request_priced(&emit).await;
            }
            Some(drive) => {
                let emit = Emit::new(emit_tx, self.venue.exchange, self.latency_ms / 2);
                self.serve_market_driven(drive, &emit).await;
            }
        }

        // `emit` is dropped above, which closes the queue: let the emitter finish draining it. A
        // fill booked on the final request would otherwise be dropped with the task, which is
        // precisely the truncation the drain exists to prevent.
        if let Err(error) = emitter.await {
            error!(
                exchange = %self.venue.exchange,
                %error,
                "MockExchange emitter task did not shut down cleanly; queued fills may be lost"
            );
        }

        info!(exchange = %self.venue.exchange, "MockExchange shutting down");
    }

    /// The request-priced loop: each request is booked as soon as it is received.
    async fn serve_request_priced(&mut self, emit: &Emit) {
        while let Some(request) = self.request_rx.recv().await {
            // Advancing the clock can retire an order whose deadline has passed. Those events
            // answer no request, and are queued before this request's own output so that the
            // client sees the expiry before anything the request goes on to produce.
            let expiries = self.advance_venue_time(request.time_request);
            emit.queue(expiries, None);
            self.book(request.kind, emit);
        }
    }

    /// The market-driven loop: market observations, requests once their latency has passed, and
    /// the venue's own deadlines, in that order of priority at any one moment.
    ///
    /// Every branch is cancel-safe: receiving from a channel or a stream loses nothing when another
    /// branch wins, and both timers are rebuilt on each pass from state held outside them.
    async fn serve_market_driven(&mut self, drive: MarketDrive, emit: &Emit) {
        let MarketDrive { mut feed, clock } = drive;
        let mut clock = MonotonicClock::new(clock);
        let to_venue = Duration::from_millis(self.latency_ms / 2);
        let exchange = self.venue.exchange;

        // Requests waiting out their way to the venue. The latency is fixed, so they fall due in
        // the order they arrived.
        let mut arriving = VecDeque::<(tokio::time::Instant, MockExchangeRequestKind)>::new();
        let mut feed_open = true;
        let mut requests_open = true;
        // A deadline the venue still reports after being woken for it, which would otherwise wake
        // this loop without end. See where it is set.
        let mut stuck_deadline = None;

        let started = self.venue.advance_time(clock.now());
        emit.queue(started, None);

        while requests_open || !arriving.is_empty() {
            let arrives_at = arriving.front().map(|(at, _)| *at);
            let deadline = self
                .venue
                .next_deadline()
                .filter(|deadline| Some(*deadline) != stuck_deadline);
            let deadline_at = deadline.map(|deadline| {
                // Rebuilt on every pass, so it needs no state of its own and cannot drift.
                let wait = (deadline - clock.peek()).to_std().unwrap_or(Duration::ZERO);
                tokio::time::Instant::now() + wait.min(MAX_DEADLINE_WAIT)
            });

            tokio::select! {
                biased;

                event = futures::StreamExt::next(&mut feed), if feed_open => match event {
                    Some(MarketFeedEvent::Observation { instrument, observation }) => {
                        let events = self.venue.apply_market(&instrument, observation, clock.now());
                        emit.queue(events, None);
                    }
                    Some(MarketFeedEvent::Lagged { missed }) => {
                        error!(
                            %exchange,
                            missed,
                            "MockExchange market feed lagged: the venue missed market \
                             observations, so resting orders may have missed fills; ending the \
                             account stream"
                        );
                        emit.queue(
                            vec![UnindexedAccountEvent::stream_terminated(
                                exchange,
                                StreamTerminationReason::Error(format!(
                                    "mock exchange market feed lagged: {missed} observations missed"
                                )),
                            )],
                            None,
                        );
                    }
                    None => {
                        feed_open = false;
                        warn!(
                            %exchange,
                            "MockExchange market feed ended: serving requests on the last market seen"
                        );
                    }
                },

                () = sleep_until(arrives_at), if arrives_at.is_some() => {
                    if let Some((_, kind)) = arriving.pop_front() {
                        let expiries = self.venue.advance_time(clock.now());
                        emit.queue(expiries, None);
                        self.book(kind, emit);
                    }
                },

                request = self.request_rx.recv(), if requests_open => match request {
                    Some(request) => {
                        arriving.push_back((tokio::time::Instant::now() + to_venue, request.kind));
                    }
                    None => requests_open = false,
                },

                () = sleep_until(deadline_at), if deadline_at.is_some() => {
                    // The timer and `clock` are two sources, so the wake can land a hair before the
                    // deadline by `clock`. It is due by the timer, so the venue moves to it.
                    if let Some(deadline) = deadline {
                        // A deadline further out than the cap woke this loop early: wait again.
                        if deadline - clock.peek() > DEADLINE_TOLERANCE {
                            continue;
                        }
                        let now = clock.raise_to(deadline);
                        let events = self.venue.advance_time(now);
                        emit.queue(events, None);
                        if self.venue.next_deadline() == Some(deadline) {
                            error!(
                                %exchange,
                                %deadline,
                                "MockExchange venue still reports a deadline it was moved to; \
                                 no longer waking for it"
                            );
                            stuck_deadline = Some(deadline);
                        }
                    }
                },
            }
        }
    }

    /// Books one request against the venue, queuing what it produced and the response it owes.
    ///
    /// The venue books the request against its own ledger before returning, so a later request
    /// already sees it.
    fn book(&mut self, kind: MockExchangeRequestKind, emit: &Emit) {
        match kind {
            MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                emit.respond(Vec::new(), response_tx, self.venue.account_snapshot());
            }
            MockExchangeRequestKind::FetchBalances {
                response_tx,
                assets,
            } => {
                emit.respond(Vec::new(), response_tx, self.venue.balances(&assets));
            }
            MockExchangeRequestKind::FetchOrdersOpen {
                response_tx,
                instruments,
            } => {
                emit.respond(
                    Vec::new(),
                    response_tx,
                    self.venue.orders_open(&instruments),
                );
            }
            MockExchangeRequestKind::FetchOrdersEnded {
                response_tx,
                orders,
            } => {
                emit.respond(Vec::new(), response_tx, self.venue.orders_ended(&orders));
            }
            MockExchangeRequestKind::FetchTrades {
                response_tx,
                start,
                end,
            } => {
                let mut trades = self.venue.trades(start);
                trades.retain(|trade| trade.time_exchange <= end);
                emit.respond(Vec::new(), response_tx, trades);
            }
            MockExchangeRequestKind::CancelOrder {
                response_tx,
                request,
            } => {
                // The venue releases what the cancelled order held before returning, so the
                // restatement it owes precedes the response, exactly as an open's does.
                let outcome = self.venue.cancel_order(request);
                emit.respond(outcome.events, response_tx, outcome.response);
            }
            MockExchangeRequestKind::OpenOrder {
                response_tx,
                request,
            } => {
                let outcome = self.venue.open_order(request);
                emit.respond(outcome.events, response_tx, outcome.response);
            }
        }
    }

    /// Applies this driver's latency model, then advances the venue to the resulting instant.
    ///
    /// Returns whatever the advance retired — see [`SimulatedVenue::advance_time`]. The caller must
    /// deliver them; dropping them would leave the client holding an order the venue no longer has.
    #[must_use]
    fn advance_venue_time(&mut self, time_request: DateTime<Utc>) -> Vec<UnindexedAccountEvent> {
        let client_to_exchange_latency = self.latency_ms / 2;

        let time_exchange = time_request
            .checked_add_signed(TimeDelta::milliseconds(client_to_exchange_latency as i64))
            .unwrap_or(time_request);

        self.venue.advance_time(time_exchange)
    }

    /// Drains queued venue output in order, emitting each batch's account events before its
    /// response.
    ///
    /// Runs until `emit_rx` closes -- which happens when [`MockExchange::run`] returns -- and then
    /// finishes whatever is still queued, so a shutdown cannot strand a booked fill or a swept
    /// expiry.
    async fn emit_queued_output(
        mut emit_rx: mpsc::UnboundedReceiver<PendingEmission>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        exchange: ExchangeId,
    ) {
        while let Some(emission) = emit_rx.recv().await {
            tokio::time::sleep_until(emission.ready_at).await;

            // Order is the venue's contract; this preserves it verbatim.
            for event in emission.events {
                if event_tx.send(event).is_err() {
                    error!(
                        %exchange,
                        "MockExchange failed to send AccountEvent notification to client"
                    );
                }
            }

            // Absent for events nothing requested, such as an order retired by its own deadline.
            if let Some(respond) = emission.respond {
                respond(exchange);
            }
        }
    }

    pub fn account_stream(&self) -> BoxStream<'static, UnindexedAccountEvent> {
        futures::StreamExt::boxed(BroadcastStream::new(self.event_tx.subscribe()).map_while(
            |result| match result {
                Ok(event) => Some(event),
                Err(error) => {
                    error!(
                        ?error,
                        "MockExchange Broadcast AccountStream lagged - terminating"
                    );
                    None
                }
            },
        ))
    }
}

/// Sleeps until `at`, or forever when there is nothing to wait for; the `select!` branch using it
/// is disabled in that case, so the forever is never awaited.
async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// A driver's clock, held so that it never moves backwards.
struct MonotonicClock {
    clock: MockExchangeClock,
    latest: Option<DateTime<Utc>>,
}

impl MonotonicClock {
    fn new(clock: MockExchangeClock) -> Self {
        Self {
            clock,
            latest: None,
        }
    }

    /// The instant to stamp now: the clock's, unless it is before the latest already stamped.
    fn now(&mut self) -> DateTime<Utc> {
        let now = (self.clock)();
        self.raise_to(now)
    }

    /// The instant [`now`](Self::now) would return, without recording it.
    fn peek(&self) -> DateTime<Utc> {
        let now = (self.clock)();
        self.latest.map_or(now, |latest| latest.max(now))
    }

    /// Moves the clock to at least `time`, returning the instant to stamp.
    fn raise_to(&mut self, time: DateTime<Utc>) -> DateTime<Utc> {
        let latest = self.latest.map_or(time, |latest| latest.max(time));
        self.latest = Some(latest);
        latest
    }
}

/// The emission queue as the serving loop sees it: what it owes the client, and how long each
/// batch waits before it is sent.
struct Emit {
    tx: mpsc::UnboundedSender<PendingEmission>,
    exchange: ExchangeId,
    /// How long each batch waits after the venue produced it.
    delay: Duration,
}

impl Emit {
    fn new(
        tx: mpsc::UnboundedSender<PendingEmission>,
        exchange: ExchangeId,
        delay_ms: u64,
    ) -> Self {
        Self {
            tx,
            exchange,
            delay: Duration::from_millis(delay_ms),
        }
    }

    /// Queues `events`, followed by `response` on `response_tx`.
    fn respond<Response>(
        &self,
        events: Vec<UnindexedAccountEvent>,
        response_tx: oneshot::Sender<Response>,
        response: Response,
    ) where
        Response: Send + 'static,
    {
        let respond: Respond = Box::new(move |exchange| {
            if response_tx.send(response).is_err() {
                error!(
                    %exchange,
                    kind = std::any::type_name::<Response>(),
                    "MockExchange failed to send oneshot response to client"
                );
            }
        });
        self.queue(events, Some(respond));
    }

    /// Queues account events, and any response they precede, for the emitter to drain in order.
    ///
    /// Everything this venue produces goes through here rather than to `event_tx` directly, so
    /// that emission order equals production order across *all* of it -- fills, expiries, cancel
    /// restatements and responses alike. Sending any of them inline would let it overtake a batch
    /// still waiting out its latency, and an absolute balance delivered out of order is the wrong
    /// balance, not merely a late one.
    ///
    /// # Why a queue rather than a task per fill
    /// A balance is an **absolute snapshot**, not a delta: successive fills report `9_999_500`,
    /// then `9_999_000`, then `9_998_500`. Applying them out of order therefore does not merely
    /// reorder history, it yields the wrong balance. Each fill previously got its own
    /// [`tokio::spawn`]. Those tasks raced -- at `latency_ms: 0` they all become runnable at once
    /// -- so snapshots reached the client in arbitrary order and the last to *arrive* won. One
    /// queue, drained by one task, makes emission order equal booking order by construction.
    fn queue(&self, events: Vec<UnindexedAccountEvent>, respond: Option<Respond>) {
        if events.is_empty() && respond.is_none() {
            return;
        }

        // Stamped at production time, not at emission time, so the delay each batch waits is
        // measured from when the venue produced *it*. The delay is fixed, so these are
        // non-decreasing in queue order and the drain never sleeps away a latency twice.
        let ready_at = tokio::time::Instant::now() + self.delay;

        // Failure means the emitter is gone, which happens only once `run` has returned. There is
        // no client left to notify, so there is nothing to do but say so.
        if self
            .tx
            .send(PendingEmission {
                ready_at,
                events,
                respond,
            })
            .is_err()
        {
            error!(
                exchange = %self.exchange,
                "MockExchange could not queue venue output: the emitter has stopped"
            );
        }
    }
}

/// Sends one response to the channel owed it, erased so that every response type shares one queue.
type Respond = Box<dyn FnOnce(ExchangeId) + Send>;

/// One batch of venue output awaiting emission, held in booking order by [`MockExchange`]'s
/// emitter queue.
///
/// # Why a response is optional
/// Not everything this venue produces answers a request. An order retired by its own deadline is
/// swept whenever the clock advances, so the events belong to nobody's response. They must still be
/// delivered in the same ordered stream as everything else: a balance is an absolute restatement,
/// so an expiry overtaking a fill still queued behind its latency would leave the client holding
/// the wrong balance, which is the exact failure this queue exists to prevent.
struct PendingEmission {
    /// When this batch's latency expires, measured from the instant the venue produced it.
    ready_at: tokio::time::Instant,
    /// The account events the venue produced, in the order it requires.
    events: Vec<UnindexedAccountEvent>,
    /// Sends the response, once every event above has been emitted, if this batch answers a
    /// request at all.
    respond: Option<Respond>,
}

/// Fixtures shared by this module's driver tests and [`venue`]'s state-machine tests.
///
/// They live in the parent rather than beside either set of tests because a private item is visible
/// to descendant modules: `venue::tests` can reach `mock::fixtures`, whereas `mock::tests` could not
/// reach anything declared inside `venue`.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
pub(crate) mod fixtures {
    use crate::{
        InstrumentAccountSnapshot, UnindexedAccountSnapshot,
        balance::{AssetBalance, Balance},
        client::mock::MockExecutionConfig,
        fee::{FeeModelConfig, SpotFeeAsset},
        fill::SimFillConfig,
        market::MarketSnapshot,
        order::{
            Order, OrderEvent, OrderKey, OrderKind, TimeInForce, UnindexedOrder,
            id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
            request::{OrderRequestOpen, RequestOpen},
            state::{Open, OrderState},
        },
        position::PositionReport,
    };
    use chrono::{DateTime, Utc};
    use fnv::FnvHashMap;
    use rust_decimal::Decimal;
    use rustrade_instrument::{
        Side, Underlying,
        asset::name::AssetNameExchange,
        exchange::ExchangeId,
        instrument::{
            Instrument,
            kind::InstrumentKind,
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
        },
    };

    pub(super) const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    pub(super) fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    pub(super) fn base() -> AssetNameExchange {
        AssetNameExchange::new("BTC")
    }

    pub(super) fn quote() -> AssetNameExchange {
        AssetNameExchange::new("USDT")
    }

    pub(super) fn instrument_name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    /// A balance with nothing reserved, which is the only shape this venue can model.
    pub(super) fn funded(
        asset: AssetNameExchange,
        amount: Decimal,
    ) -> AssetBalance<AssetNameExchange> {
        AssetBalance {
            asset,
            balance: Balance::new(amount, amount),
            time_exchange: Utc::now(),
        }
    }

    /// Zero latency, so a driver's queue is exercised without any wall-clock sleeping.
    pub(super) fn config_from_balances(
        balances: Vec<AssetBalance<AssetNameExchange>>,
        fee_model: FeeModelConfig,
    ) -> MockExecutionConfig {
        config_from_balances_with_fill(balances, fee_model, SimFillConfig::default())
    }

    pub(super) fn config_from_balances_with_fill(
        balances: Vec<AssetBalance<AssetNameExchange>>,
        fee_model: FeeModelConfig,
        fill_model: SimFillConfig,
    ) -> MockExecutionConfig {
        MockExecutionConfig::new(
            EXCHANGE,
            UnindexedAccountSnapshot {
                exchange: EXCHANGE,
                balances,
                instruments: vec![],
            },
            0, // latency_ms
            fee_model,
            fill_model,
            SpotFeeAsset::default(),
            crate::slippage::SlippageConfig::default(),
            crate::fill::MarketOrderTiming::default(),
            crate::fill::StaleMarketPolicy::default(),
            None,
            None,
        )
    }

    /// A spot config whose account already holds one open order, as `initial_state` seeds it.
    ///
    /// The only way an order can rest on a venue [`MockExchange`] drives: that venue is
    /// [`VenueRegime::RequestPriced`], which refuses `OrderKind::Limit` outright.
    ///
    /// [`VenueRegime::RequestPriced`]: crate::exchange::mock::venue::VenueRegime::RequestPriced
    pub(super) fn spot_config_holding(
        btc: &str,
        usdt: &str,
        order: UnindexedOrder,
    ) -> MockExecutionConfig {
        MockExecutionConfig::new(
            EXCHANGE,
            UnindexedAccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![funded(base(), d(btc)), funded(quote(), d(usdt))],
                instruments: vec![InstrumentAccountSnapshot {
                    instrument: instrument_name(),
                    orders: vec![order],
                    orders_complete: true,
                    position: PositionReport::Unreported,
                    isolated: None,
                }],
            },
            0, // latency_ms
            FeeModelConfig::default(),
            SimFillConfig::default(),
            SpotFeeAsset::default(),
            crate::slippage::SlippageConfig::default(),
            crate::fill::MarketOrderTiming::default(),
            crate::fill::StaleMarketPolicy::default(),
            None,
            None,
        )
    }

    /// An open buy resting at `price`, retiring itself at `expiry`.
    pub(super) fn seeded_gtd(
        cid: &str,
        price: &str,
        opened: DateTime<Utc>,
        expiry: DateTime<Utc>,
    ) -> UnindexedOrder {
        Order {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(cid),
            },
            side: Side::Buy,
            price: Some(d(price)),
            quantity: d("0.01"),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodTillDate { expiry },
            state: OrderState::active(Open::new(
                VenueOrderId::Assigned(OrderId::new(cid)),
                opened,
                rust_decimal::Decimal::ZERO,
            )),
        }
    }

    /// The spot fixture: `btc` of the base asset and `usdt` of the quote, both fully free.
    pub(super) fn spot_config(
        btc: &str,
        usdt: &str,
        fee_model: FeeModelConfig,
    ) -> MockExecutionConfig {
        config_from_balances(
            vec![funded(base(), d(btc)), funded(quote(), d(usdt))],
            fee_model,
        )
    }

    pub(super) fn spot_config_with_fill(
        btc: &str,
        usdt: &str,
        fee_model: FeeModelConfig,
        fill_model: SimFillConfig,
    ) -> MockExecutionConfig {
        config_from_balances_with_fill(
            vec![funded(base(), d(btc)), funded(quote(), d(usdt))],
            fee_model,
            fill_model,
        )
    }

    pub(super) fn instruments_of(
        instrument: Instrument<ExchangeId, AssetNameExchange>,
    ) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        let mut instruments = FnvHashMap::default();
        instruments.insert(instrument.name_exchange.clone(), instrument);
        instruments
    }

    pub(super) fn spot_instruments()
    -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        instruments_of(Instrument {
            exchange: EXCHANGE,
            name_internal: InstrumentNameInternal::new("btcusdt"),
            name_exchange: instrument_name(),
            underlying: Underlying {
                base: base(),
                quote: quote(),
            },
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind: InstrumentKind::Spot,
            spec: None,
            data_venue: None,
        })
    }

    /// A Market order carrying the snapshot the venue will price it against.
    ///
    /// The snapshot is set on the request rather than passed alongside it, because
    /// [`RequestOpen::market`] is the venue's only price source and a second copy on the same path
    /// could disagree with it.
    pub(super) fn request(
        instrument: InstrumentNameExchange,
        side: Side,
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument,
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("test-cid"),
            },
            state: RequestOpen {
                market,
                ..RequestOpen::new(
                    side,
                    None, // Market orders don't have a limit price
                    d(quantity),
                    OrderKind::Market,
                    TimeInForce::ImmediateOrCancel,
                )
            },
        }
    }

    /// `request`, carrying `effect` as its [`RequestOpen::margin_effect`].
    pub(super) fn with_effect(
        mut request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        effect: crate::margin::MarginEffect,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request.state = request.state.with_margin_effect(effect);
        request
    }

    pub(super) fn buy_request(
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request(instrument_name(), Side::Buy, quantity, market)
    }

    pub(super) fn sell_request(
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request(instrument_name(), Side::Sell, quantity, market)
    }

    /// The terms a plainly resting order carries: on the book until somebody cancels it.
    pub(super) fn gtc() -> TimeInForce {
        TimeInForce::GoodUntilCancelled { post_only: false }
    }

    /// A Limit order at `price`, carrying its own `cid` so several can rest at once.
    ///
    /// `market` is deliberately `None`: a limit order is judged and priced against the venue's own
    /// market, never against a snapshot its sender stamped.
    pub(super) fn limit_request(
        cid: &str,
        side: Side,
        quantity: &str,
        price: &str,
        time_in_force: TimeInForce,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(cid),
            },
            state: RequestOpen::new(
                side,
                Some(d(price)),
                d(quantity),
                OrderKind::Limit,
                time_in_force,
            ),
        }
    }

    /// Both sides of a book and no trade price, which is what an L1-only feed supplies.
    pub(super) fn book(best_bid: &str, best_ask: &str) -> MarketSnapshot {
        MarketSnapshot {
            best_bid: Some(d(best_bid)),
            best_ask: Some(d(best_ask)),
            last_price: None,
        }
    }

    /// A trade price and no book, which is what a trades-only feed supplies.
    pub(super) fn traded(last_price: &str) -> MarketSnapshot {
        MarketSnapshot {
            best_bid: None,
            best_ask: None,
            last_price: Some(d(last_price)),
        }
    }

    /// A snapshot whose three prices are all `price`, wrapped as the venue receives it.
    pub(super) fn market_prices(price: &str) -> Option<MarketSnapshot> {
        let p = Some(d(price));
        Some(MarketSnapshot {
            best_bid: p,
            best_ask: p,
            last_price: p,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::{fixtures::*, *};
    use crate::market::MarketSnapshot;
    use crate::{
        AccountEventKind,
        fee::FeeModelConfig,
        order::{
            OrderEvent, OrderKey,
            id::ClientOrderId,
            request::RequestCancel,
            state::{InactiveOrderState, OrderState},
        },
    };
    use rust_decimal::Decimal;
    use rustrade_instrument::Side;

    /// A [`SimulatedVenue`] balance is an absolute restatement, so emission order *is* the answer.
    ///
    /// Each fill used to be emitted by its own spawned task. Those tasks raced, so the snapshots
    /// `9_999_500`, `9_999_000`, `9_998_500`, ... reached the client in arbitrary order and the
    /// last to arrive won — which is not the same thing as the last to be booked. That went
    /// unnoticed only because the timestamp each snapshot carries is derived from a clock
    /// contaminated with wall-clock time, and so happened to be unique and increasing, letting the
    /// engine discard the out-of-order ones as stale. Correcting that clock removes the accident.
    ///
    /// Asserted on the broadcast stream rather than on any downstream balance, because the ordering
    /// is the venue's contract to keep: the engine cannot repair snapshots it receives out of order.
    ///
    /// # Why this must stay multi-threaded
    /// The race is between spawned tasks. On a `current_thread` runtime they are serialised by the
    /// scheduler and this test passes against the unfixed code, detecting nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_order_emissions_are_ordered_across_fills() {
        const FILLS: usize = 8;
        const DEBIT_PER_FILL: &str = "500";

        let usdt_start = "10000000";

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(64);

        let exchange = MockExchange::new(
            spot_config("100", usdt_start, FeeModelConfig::default()),
            request_rx,
            event_tx,
            spot_instruments(),
        );

        let driver = tokio::spawn(exchange.run());

        let mut responses = Vec::with_capacity(FILLS);
        for nth in 0..FILLS {
            let (response_tx, response_rx) = oneshot::channel();
            let mut request = buy_request("0.01", market_prices("50000"));
            request.key.cid = ClientOrderId::new(format!("cid-{nth}"));

            request_tx
                .send(MockExchangeRequest::open_order(
                    Utc::now(),
                    response_tx,
                    request,
                ))
                .unwrap();
            responses.push(response_rx);
        }

        // Closing the request channel ends `run`, which drains the emitter before returning.
        drop(request_tx);
        driver.await.unwrap();

        for response in responses {
            let response = response.await.expect("every open is answered");
            assert!(
                matches!(
                    response.state,
                    OrderState::Active(_) | OrderState::Inactive(_)
                ),
                "the account is funded and the instrument priced, so every open fills: {response:?}"
            );
        }

        // Each fill also restates the base it received; this test pins the quote it paid with.
        let balances = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event.kind {
                AccountEventKind::BalanceSnapshot(snapshot) if snapshot.0.asset == quote() => {
                    Some(snapshot.0.balance.total)
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        // One quote snapshot per fill, each debiting the quote asset by the same notional.
        let expected = (1..=FILLS)
            .map(|nth| d(usdt_start) - d(DEBIT_PER_FILL) * Decimal::from(nth))
            .collect::<Vec<_>>();

        assert_eq!(
            balances, expected,
            "balance snapshots must reach the client in the order the venue booked them"
        );
    }

    /// The async driver builds its venue from the configuration, slippage included.
    #[tokio::test]
    async fn the_driver_fills_at_the_configured_slippage() {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(16);
        let config = MockExecutionConfig {
            slippage: crate::slippage::SlippageConfig::fixed(d("0.01")).unwrap(),
            ..spot_config("100", "10000000", FeeModelConfig::default())
        };
        let exchange = MockExchange::new(config, request_rx, event_tx, spot_instruments());
        let driver = tokio::spawn(exchange.run());

        let (response_tx, response_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::open_order(
                Utc::now(),
                response_tx,
                buy_request("0.01", market_prices("50000")),
            ))
            .unwrap();
        response_rx.await.expect("the open is answered");
        drop(request_tx);
        driver.await.unwrap();

        let prices = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event.kind {
                AccountEventKind::Trade(trade) => Some(trade.price),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(prices, vec![d("50500")], "50,000 bought 1% above");
    }

    /// A cancel is rejected by the venue, and the driver answers the channel it was asked on.
    ///
    /// The rejection itself is the venue's own; what this pins is that the driver
    /// forwards it rather than leaving the caller's `oneshot` to be dropped — which is what the
    /// previous, mistyped implementation did, since it could not produce the channel's response
    /// type at all.
    /// An order retired by its own deadline reaches the client, in the emitter's queue rather than
    /// around it.
    ///
    /// The sweep runs synchronously at the top of `run`'s loop, while the emitter may still be
    /// asleep waiting out an earlier batch's latency. Sending it straight to `event_tx` would let
    /// it overtake that batch — and a balance is an absolute restatement, so a snapshot applied out
    /// of order leaves the client holding the wrong number, not merely a late one. This asserts the
    /// expiry lands *after* the fill booked before it.
    #[tokio::test(start_paused = true)]
    async fn a_swept_expiry_is_emitted_behind_the_fill_booked_before_it() {
        let opened = Utc::now();
        let expiry = opened + chrono::TimeDelta::seconds(30);

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(64);

        let mut config =
            spot_config_holding("100", "10000000", seeded_gtd("gtd", "1", opened, expiry));
        // Non-zero, so the emitter is genuinely asleep when the next request sweeps.
        config.latency_ms = 50;

        let exchange = MockExchange::new(config, request_rx, event_tx, spot_instruments());
        let driver = tokio::spawn(exchange.run());

        // Booked before the deadline, so its fill is queued and still waiting out its latency.
        let (fill_tx, fill_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::open_order(
                opened,
                fill_tx,
                buy_request("0.01", market_prices("50000")),
            ))
            .unwrap();

        // A second request stamped past the deadline: advancing the clock for it sweeps the
        // seeded order while the fill above is still in the emitter's queue.
        let (snapshot_tx, snapshot_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::fetch_account_snapshot(
                expiry + chrono::TimeDelta::seconds(1),
                snapshot_tx,
            ))
            .unwrap();

        drop(request_tx);
        driver.await.unwrap();
        fill_rx.await.expect("the open is answered");
        let _ = snapshot_rx.await;

        let kinds = std::iter::from_fn(|| events.try_recv().ok())
            .map(|event| match event.kind {
                AccountEventKind::BalanceSnapshot(_) => "balance",
                AccountEventKind::Trade(_) => "trade",
                AccountEventKind::OrderSnapshot(_) => "order",
                _ => "other",
            })
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            ["balance", "balance", "trade", "order"],
            "the fill's events must all precede the sweep, which for a seeded order — held with \
             no reservation — owes only its terminal snapshot"
        );
    }

    #[tokio::test]
    async fn a_cancel_request_is_answered_rather_than_dropped() {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _events) = broadcast::channel(16);

        let exchange = MockExchange::new(
            spot_config("100", "10000", FeeModelConfig::default()),
            request_rx,
            event_tx,
            spot_instruments(),
        );
        let driver = tokio::spawn(exchange.run());

        let (response_tx, response_rx) = oneshot::channel();
        let open = buy_request("1", market_prices("50000"));
        request_tx
            .send(MockExchangeRequest::cancel_order(
                Utc::now(),
                response_tx,
                OrderEvent {
                    key: open.key,
                    state: RequestCancel { id: None },
                },
            ))
            .unwrap();

        let response = response_rx.await.expect("a cancel must be answered");
        assert!(
            response.state.is_err(),
            "this venue rests no orders, so a cancel must be rejected: {response:?}"
        );

        drop(request_tx);
        driver.await.unwrap();
    }

    /// The mock client's order-state lookup reaches the venue's ledger and back: an order that
    /// filled is reported, and one the venue never held is omitted.
    #[tokio::test]
    async fn an_order_state_lookup_is_answered_from_the_ledger() {
        use crate::client::{OrderStatusClient, mock::MockExecution};

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = broadcast::channel(16);
        let exchange = MockExchange::new(
            spot_config("100", "10000000", FeeModelConfig::default()),
            request_rx,
            event_tx,
            spot_instruments(),
        );
        let driver = tokio::spawn(exchange.run());

        let open = buy_request("1", market_prices("50000"));
        let (response_tx, response_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::open_order(
                Utc::now(),
                response_tx,
                open.clone(),
            ))
            .unwrap();
        let opened = response_rx.await.expect("an open must be answered");
        assert!(matches!(
            opened.state,
            OrderState::Inactive(InactiveOrderState::FullyFilled(_))
        ));

        let client = MockExecution {
            mocked_exchange: EXCHANGE,
            clock: Utc::now,
            request_tx: request_tx.clone(),
            event_rx,
        };
        let unknown = OrderKey {
            cid: ClientOrderId::new("never-opened"),
            ..open.key.clone()
        };
        let ended = client
            .fetch_ended_orders(&[open.key.clone(), unknown])
            .await
            .expect("the venue is running");

        let [order] = ended.as_slice() else {
            panic!("only the filled order is reported: {ended:?}");
        };
        assert_eq!(order.key, open.key);
        assert!(matches!(order.state, InactiveOrderState::FullyFilled(_)));

        drop(client);
        drop(request_tx);
        driver.await.unwrap();
    }

    /// A request-priced driver answers a fetch after the output booked before it, so a balance
    /// read while a fill is in flight is never older than the fill's own restatement.
    #[tokio::test(start_paused = true)]
    async fn a_fetch_is_answered_after_the_fill_booked_before_it() {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(64);
        let mut config = spot_config("0", "10000000", FeeModelConfig::default());
        config.latency_ms = 50;
        let exchange = MockExchange::new(config, request_rx, event_tx, spot_instruments());
        let driver = tokio::spawn(exchange.run());

        let (open_tx, open_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::open_order(
                Utc::now(),
                open_tx,
                buy_request("0.01", market_prices("50000")),
            ))
            .unwrap();
        let (balances_tx, balances_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::fetch_balances(
                Utc::now(),
                vec![quote()],
                balances_tx,
            ))
            .unwrap();

        balances_rx.await.expect("the fetch is answered");
        let kinds = std::iter::from_fn(|| events.try_recv().ok())
            .map(|event| event_name(&event))
            .collect::<Vec<_>>();
        assert!(
            kinds.contains(&"trade"),
            "the fill booked before the fetch is emitted before the fetch is answered: {kinds:?}"
        );

        open_rx.await.expect("the open is answered");
        drop(request_tx);
        driver.await.unwrap();
    }

    /// `new_market_driven` refuses a venue that would ignore its feed.
    #[test]
    #[should_panic(expected = "request-priced venue")]
    fn a_market_driven_driver_refuses_a_request_priced_venue() {
        let config = spot_config("0", "10000", FeeModelConfig::default());
        let (_request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _events) = broadcast::channel(16);
        let _ = MockExchange::new_market_driven(
            SimulatedVenue::new(&config, spot_instruments()),
            0,
            request_rx,
            event_tx,
            futures::StreamExt::boxed(futures::stream::empty()),
            Arc::new(Utc::now),
        );
    }

    fn event_name(event: &UnindexedAccountEvent) -> &'static str {
        match &event.kind {
            AccountEventKind::BalanceSnapshot(_) => "balance",
            AccountEventKind::Trade(_) => "trade",
            AccountEventKind::OrderSnapshot(_) => "order",
            AccountEventKind::StreamTerminated(_) => "terminated",
            _ => "other",
        }
    }

    /// A market-driven driver under test: its channels, a clock moving with tokio's, and the
    /// running task.
    struct Paper {
        request_tx: mpsc::UnboundedSender<MockExchangeRequest>,
        market_tx: mpsc::UnboundedSender<MarketFeedEvent>,
        events: broadcast::Receiver<UnindexedAccountEvent>,
        driver: tokio::task::JoinHandle<()>,
        /// The instant the clock read when the driver started.
        start: DateTime<Utc>,
    }

    impl Paper {
        /// Runs `config` market-driven, on a clock that reads `start` now and moves with tokio's.
        fn start(config: MockExecutionConfig) -> Self {
            let start = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
            let anchor = tokio::time::Instant::now();
            let clock: MockExchangeClock = Arc::new(move || {
                start + TimeDelta::from_std(anchor.elapsed()).expect("a test runs for less")
            });
            Self::start_on(config, clock, start)
        }

        fn start_on(
            config: MockExecutionConfig,
            clock: MockExchangeClock,
            start: DateTime<Utc>,
        ) -> Self {
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            let (event_tx, events) = broadcast::channel(256);
            let (market_tx, market_rx) = mpsc::unbounded_channel();
            let exchange = MockExchange::new_market_driven(
                SimulatedVenue::new_market_driven(&config, spot_instruments()),
                config.latency_ms,
                request_rx,
                event_tx,
                futures::StreamExt::boxed(tokio_stream::wrappers::UnboundedReceiverStream::new(
                    market_rx,
                )),
                clock,
            );
            Self {
                request_tx,
                market_tx,
                events,
                driver: tokio::spawn(exchange.run()),
                start,
            }
        }

        fn observe(&self, snapshot: MarketSnapshot) {
            self.market_tx
                .send(MarketFeedEvent::Observation {
                    instrument: instrument_name(),
                    observation: MarketObservation::new(snapshot),
                })
                .unwrap();
        }

        async fn open(
            &self,
            request: crate::order::request::OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        ) -> crate::order::Order<
            ExchangeId,
            InstrumentNameExchange,
            crate::order::state::UnindexedOrderState,
        > {
            let (response_tx, response_rx) = oneshot::channel();
            self.request_tx
                .send(MockExchangeRequest::open_order(
                    Utc::now(),
                    response_tx,
                    request,
                ))
                .unwrap();
            response_rx.await.expect("an open is answered")
        }

        /// The next account event, waiting for it in tokio's (paused) time.
        async fn next_event(&mut self) -> UnindexedAccountEvent {
            tokio::time::timeout(std::time::Duration::from_secs(3600), self.events.recv())
                .await
                .expect("an account event arrives")
                .expect("the account channel is open")
        }

        /// Account events up to and including the first trade.
        async fn until_trade(
            &mut self,
        ) -> crate::trade::Trade<AssetNameExchange, InstrumentNameExchange> {
            loop {
                if let AccountEventKind::Trade(trade) = self.next_event().await.kind {
                    return trade;
                }
            }
        }

        async fn stop(self) {
            drop(self.request_tx);
            self.driver.await.unwrap();
        }
    }

    fn priced(price: &str) -> MarketSnapshot {
        market_prices(price).unwrap()
    }

    /// A limit order rests on a market-driven driver, and fills when a later observation crosses
    /// it, with nothing requesting the fill. Its response takes the whole of `latency_ms`, not one
    /// and a half times it.
    #[tokio::test(start_paused = true)]
    async fn a_paper_limit_rests_then_fills_when_the_market_crosses_it() {
        let mut config = spot_config("0", "100000", FeeModelConfig::default());
        config.latency_ms = 100;
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));

        let sent = tokio::time::Instant::now();
        let response = paper
            .open(limit_request("rest", Side::Buy, "1", "49000", gtc()))
            .await;
        assert!(
            matches!(response.state, OrderState::Active(_)),
            "a limit below the market rests: {response:?}"
        );
        assert_eq!(
            sent.elapsed(),
            std::time::Duration::from_millis(100),
            "half the latency to the venue, half back"
        );

        paper.observe(priced("48000"));
        let trade = paper.until_trade().await;
        assert_eq!(trade.quantity, d("1"));
        assert!(
            trade.price <= d("49000"),
            "a buy limit never pays more: {trade:?}"
        );
        paper.stop().await;
    }

    /// A request is booked once its outbound half of the latency has passed, against the market
    /// held then, as a backtest of the same venue books it.
    #[tokio::test(start_paused = true)]
    async fn a_paper_request_is_booked_against_the_market_when_it_arrives() {
        let mut config = spot_config("0", "10000000", FeeModelConfig::default());
        config.latency_ms = 100;
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));

        let (response_tx, response_rx) = oneshot::channel();
        paper
            .request_tx
            .send(MockExchangeRequest::open_order(
                Utc::now(),
                response_tx,
                // The snapshot a request carries prices nothing on a market-driven venue.
                buy_request("0.01", market_prices("1")),
            ))
            .unwrap();
        // Still on its way: the market moves before the order reaches the venue at 50 ms.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        paper.observe(priced("51000"));

        let trade = paper.until_trade().await;
        assert_eq!(
            trade.price,
            d("51000"),
            "booked against the market as it arrived"
        );
        assert_eq!(
            trade.time_exchange,
            paper.start + TimeDelta::milliseconds(50),
            "stamped when it reached the venue"
        );
        response_rx.await.expect("the open is answered");
        paper.stop().await;
    }

    /// An order's deadline falls when it is due, with no market event or request to move the
    /// clock.
    #[tokio::test(start_paused = true)]
    async fn a_paper_order_expires_at_its_deadline_in_a_quiet_market() {
        let config = spot_config("0", "100000", FeeModelConfig::default());
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));
        let expiry = paper.start + TimeDelta::seconds(10);
        let started = tokio::time::Instant::now();

        let response = paper
            .open(limit_request(
                "gtd",
                Side::Buy,
                "1",
                "49000",
                crate::order::TimeInForce::GoodTillDate { expiry },
            ))
            .await;
        assert!(matches!(response.state, OrderState::Active(_)));

        let expired = loop {
            let event = paper.next_event().await;
            if let AccountEventKind::OrderSnapshot(order) = event.kind
                && matches!(
                    order.0.state,
                    OrderState::Inactive(InactiveOrderState::Expired(_))
                )
            {
                break order.0;
            }
        };
        let OrderState::Inactive(InactiveOrderState::Expired(expired)) = expired.state else {
            unreachable!("matched above")
        };
        assert_eq!(expired.time_exchange, expiry, "retired at its own deadline");
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(10),
            "and not before that time has passed: {:?}",
            started.elapsed()
        );
        paper.stop().await;
    }

    /// A lagged feed ends the account stream after everything booked before it, and the venue
    /// keeps serving.
    #[tokio::test(start_paused = true)]
    async fn a_lagged_paper_feed_ends_the_account_stream_after_the_fills_before_it() {
        let config = spot_config("0", "10000000", FeeModelConfig::default());
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));
        paper.open(buy_request("0.01", None)).await;
        paper
            .market_tx
            .send(MarketFeedEvent::Lagged { missed: 3 })
            .unwrap();

        let mut kinds = Vec::new();
        while kinds.last() != Some(&"terminated") {
            kinds.push(event_name(&paper.next_event().await));
        }
        assert!(
            kinds.contains(&"trade"),
            "the fill booked before the lag precedes the termination: {kinds:?}"
        );

        let response = paper.open(buy_request("0.01", None)).await;
        assert!(
            matches!(
                response.state,
                OrderState::Inactive(InactiveOrderState::FullyFilled(_))
            ),
            "the venue keeps trading after the lag: {response:?}"
        );
        paper.stop().await;
    }

    /// Once the feed ends, the driver trades on the last market it saw.
    #[tokio::test(start_paused = true)]
    async fn a_paper_driver_keeps_trading_once_its_feed_ends() {
        let config = spot_config("0", "10000000", FeeModelConfig::default());
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));
        // The replacement's receiver is dropped at once; dropping the original ends the feed.
        let (replacement, _) = mpsc::unbounded_channel();
        drop(std::mem::replace(&mut paper.market_tx, replacement));

        paper.open(buy_request("0.01", None)).await;
        assert_eq!(paper.until_trade().await.price, d("50000"));
        paper.stop().await;
    }

    /// A clock that steps back does not move the venue back: the stamp holds at the latest one.
    #[tokio::test(start_paused = true)]
    async fn a_paper_clock_that_steps_back_does_not_move_the_venue_back() {
        let later = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let reading = Arc::new(std::sync::Mutex::new(later));
        let clock: MockExchangeClock = {
            let reading = Arc::clone(&reading);
            Arc::new(move || *reading.lock().unwrap())
        };
        let config = spot_config("0", "10000000", FeeModelConfig::default());
        let mut paper = Paper::start_on(config, clock, later);
        paper.observe(priced("50000"));
        // Settles the observation before the clock steps back.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        *reading.lock().unwrap() = later - TimeDelta::seconds(100);

        paper.open(buy_request("0.01", None)).await;
        assert_eq!(paper.until_trade().await.time_exchange, later);
        paper.stop().await;
    }

    /// A request-priced driver books a request half its latency after the request was made, and a
    /// trades fetch bounded by that very instant includes the trade.
    #[tokio::test]
    async fn a_request_priced_request_is_booked_half_the_latency_after_it_was_made() {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _events) = broadcast::channel(16);
        let mut config = spot_config("0", "10000000", FeeModelConfig::default());
        config.latency_ms = 100;
        let exchange = MockExchange::new(config, request_rx, event_tx, spot_instruments());
        let driver = tokio::spawn(exchange.run());

        let made = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let (open_tx, open_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::open_order(
                made,
                open_tx,
                buy_request("0.01", market_prices("50000")),
            ))
            .unwrap();
        open_rx.await.expect("the open is answered");

        let booked = made + TimeDelta::milliseconds(50);
        let (trades_tx, trades_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::fetch_trades(
                made, trades_tx, made, booked,
            ))
            .unwrap();
        let trades = trades_rx.await.expect("the fetch is answered");
        assert_eq!(
            trades
                .iter()
                .map(|trade| trade.time_exchange)
                .collect::<Vec<_>>(),
            vec![booked],
            "booked at half the latency, and a bound at that instant includes it"
        );
        drop(request_tx);
        driver.await.unwrap();
    }

    /// Closing the request channel does not drop a request still on its way to the venue.
    #[tokio::test(start_paused = true)]
    async fn a_paper_request_in_flight_is_answered_after_the_request_channel_closes() {
        let mut config = spot_config("0", "10000000", FeeModelConfig::default());
        config.latency_ms = 100;
        let paper = Paper::start(config);
        paper.observe(priced("50000"));

        let (response_tx, response_rx) = oneshot::channel();
        paper
            .request_tx
            .send(MockExchangeRequest::open_order(
                Utc::now(),
                response_tx,
                buy_request("0.01", None),
            ))
            .unwrap();
        drop(paper.request_tx);

        let response = response_rx.await.expect("the open is still answered");
        assert!(matches!(response.state, OrderState::Inactive(_)));
        paper.driver.await.unwrap();
    }

    /// The driver's `Debug` names it, whichever mode it runs in.
    #[tokio::test]
    async fn a_driver_debug_names_the_driver() {
        let (_request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _events) = broadcast::channel(16);
        let config = spot_config("0", "10000000", FeeModelConfig::default());
        let exchange = MockExchange::new(config, request_rx, event_tx, spot_instruments());
        assert!(format!("{exchange:?}").contains("MockExchange"));
    }

    /// Before the venue has observed an instrument a market order is refused, while a resting
    /// limit is accepted and can then be cancelled.
    #[tokio::test(start_paused = true)]
    async fn a_paper_venue_refuses_a_market_order_before_the_first_observation_and_cancels_a_rest()
    {
        let mut config = spot_config("0", "10000000", FeeModelConfig::default());
        config.latency_ms = 100;
        let paper = Paper::start(config);

        let refused = paper.open(buy_request("0.01", None)).await;
        assert!(
            matches!(refused.state, OrderState::Inactive(_)),
            "nothing to price a market order from: {refused:?}"
        );

        let rest = limit_request("rest", Side::Buy, "1", "49000", gtc());
        let key = rest.key.clone();
        let resting = paper.open(rest).await;
        assert!(matches!(resting.state, OrderState::Active(_)));

        let (response_tx, response_rx) = oneshot::channel();
        paper
            .request_tx
            .send(MockExchangeRequest::cancel_order(
                Utc::now(),
                response_tx,
                OrderEvent {
                    key,
                    state: RequestCancel { id: None },
                },
            ))
            .unwrap();
        let cancelled = response_rx.await.expect("the cancel is answered");
        assert!(cancelled.state.is_ok(), "{cancelled:?}");
        paper.stop().await;
    }

    /// A deadline further out than one sleep can wait is retired at its own instant, not early.
    #[tokio::test(start_paused = true)]
    async fn a_paper_deadline_beyond_the_longest_sleep_is_retired_on_time() {
        let config = spot_config("0", "100000", FeeModelConfig::default());
        let mut paper = Paper::start(config);
        paper.observe(priced("50000"));
        let expiry = paper.start + TimeDelta::days(3);
        let started = tokio::time::Instant::now();

        paper
            .open(limit_request(
                "far",
                Side::Buy,
                "1",
                "49000",
                crate::order::TimeInForce::GoodTillDate { expiry },
            ))
            .await;

        let expired = loop {
            let event = paper
                .events
                .recv()
                .await
                .expect("the account channel is open");
            if let AccountEventKind::OrderSnapshot(order) = event.kind
                && let OrderState::Inactive(InactiveOrderState::Expired(expired)) = order.0.state
            {
                break expired;
            }
        };
        assert_eq!(expired.time_exchange, expiry);
        assert!(started.elapsed() >= std::time::Duration::from_secs(3 * 24 * 60 * 60));
        paper.stop().await;
    }
}
