//! Drives a simulation from outside, one instant at a time.

use super::{
    DEFAULT_FEEDBACK_LIMIT, RequestDrain, SimCore, SimFault, SimVenue, Step, VenueMarketUpdate,
    source_class,
};
use crate::{
    EngineEvent,
    engine::{
        Engine, Processor,
        clock::{EngineClock, HistoricalClock},
        execution_tx::MultiExchangeTxMap,
        state::{EngineState, position::PositionSeed},
    },
    error::BarterError,
    shutdown::Shutdown,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_instrument::{
    Side, exchange::ExchangeIndex, instrument::name::InstrumentNameInternal,
};
use rustrade_integration::{Terminal, collection::FnvIndexMap};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, fmt::Debug};
use thiserror::Error;

/// The [`Engine`] a [`SimStepper`] drives: a simulated clock, and execution requests routed to the
/// simulated venues by the [`MultiExchangeTxMap`] a [`SimExecutionBuild`] returns.
///
/// [`SimExecutionBuild`]: super::SimExecutionBuild
pub type SimEngine<GlobalData, InstrumentData, Strategy, Risk> = Engine<
    HistoricalClock,
    EngineState<GlobalData, InstrumentData>,
    MultiExchangeTxMap,
    Strategy,
    Risk,
>;

/// What processing one event produced, as [`SimStepper`]'s calls return it: the `Engine`'s own
/// audit of that event.
pub type StepAudit<GlobalData, InstrumentData, Strategy, Risk, MarketKind> = <SimEngine<
    GlobalData,
    InstrumentData,
    Strategy,
    Risk,
> as Processor<
    EngineEvent<MarketKind>,
>>::Audit;

/// Deterministic, synchronous driver for simulated venues, advanced by its caller one instant at
/// a time: the gym-style loop of feeding a bar, observing, acting, and feeding the next.
///
/// It drives the same venues, in the same order, as [`SimRunner`](super::SimRunner) does for a
/// backtest, and books every fill through the same `Engine` path, so a policy trained against it
/// trades through the code it will run on. What differs is who decides when an instant is
/// complete. A `SimRunner` reads a `Stream` and cannot tell "nothing more yet" from "nothing more
/// at this instant", so it waits. A `SimStepper` is handed every event of one instant at once, so
/// it knows, and settles everything due at that instant before it returns.
///
/// Every call is synchronous and spawns nothing: there is no runtime to await and no task to
/// join, so it can run inside `tokio::task::spawn_blocking`, or on any thread. A `SimStepper` holds
/// no global or thread-local state, and is `Send`, so many can live side by side in one process.
///
/// # Building one
/// 1. Build the venues with a [`SimExecutionBuilder`](super::SimExecutionBuilder), from a
///    [`SimStart`](super::SimStart) when the account opens holding positions.
/// 2. Build the `Engine` with a [`HistoricalClock`], the build's
///    [`execution_tx_map`](super::SimExecutionBuild::execution_tx_map), and the build's
///    [`position_seeds`](super::SimExecutionBuild::position_seeds).
/// 3. Build the stepper with [`SimStepper::new`], from the build's
///    [`venues`](super::SimExecutionBuild::venues) and a clone of the same clock.
///
/// The repository's `sim_stepper` example does exactly this.
///
/// # One step
/// [`step`](Self::step) takes an instant and every event at it, then:
/// 1. Books every request the `Engine` sent since the previous call, at the instant it was sent.
///    An order sent after the previous step is therefore filled against the previous instant's
///    market, not this one's.
/// 2. Delivers everything that came due between the two instants, in time order: delayed fills,
///    and the venues' own deadlines (order expiries, holding costs).
/// 3. Hands the instant's market data to the venues, which match resting orders against it, and
///    shows each event to the `Engine`. Auxiliary events at the instant go first, then market data.
///    A bar that fills a market order held for it is shown after that fill, as on
///    [`SimRunner`](super::SimRunner): see `A bar that fills a held market order` in its
///    `# Ordering contract`.
/// 4. Delivers every fill, rejection and balance update due at the instant, including those for
///    orders the `Engine`'s strategy sent in reply to this instant's events.
///
/// When it returns, the `Engine`'s state holds everything due at the instant: positions,
/// balances, and order states. It returns the `Engine`'s audit of each event it showed it, in
/// order: a fill arrives as an [`AccountEventKind::Trade`], and a refused order as an
/// [`AccountEventKind::OrderSnapshot`] in a rejected state, keyed by its instrument and
/// `ClientOrderId`, so every request is answered in the same call.
///
/// An instant may carry events for only some instruments.
///
/// # Acting between steps
/// Orders the caller decides after observing a step go through the `Engine`, for example with
/// [`Engine::action`] and a [`Command::SendOpenRequests`](crate::engine::command::Command), which
/// sends them synchronously and needs no strategy. Then either:
/// - call [`settle`](Self::settle), which books them at the current instant and returns once they
///   and everything they caused are delivered. It can be called any number of times at one
///   instant, each call seeing what the previous ones booked, so a later order can be sized from
///   the balances an earlier one left; or
/// - call the next [`step`](Self::step), which books them first, at the instant they were sent.
///
/// # Fill timing is an optimistic idealisation
/// At zero latency an order sent after the instant `T` fills against the market as of `T`: a
/// market order at the price of the bar just delivered, typically its close. That assumes trading
/// at exactly the observed price, with no time to react, no spread crossed and no market impact.
/// A policy can learn to exploit it. Slippage
/// ([`MockExecutionConfig::slippage`](rustrade_execution::client::mock::MockExecutionConfig))
/// raises the cost of trading, but does not remove the information advantage.
///
/// [`MockExecutionConfig::market_order_timing`] removes it. Under
/// [`MarketOrderTiming::NextBar`] a market order sent after the instant `T` is held, and a
/// [`settle`](Self::settle) returns with it open. The next step whose bar is for that instrument
/// fills it at that bar's open, or at its worst price as a stress bound, and shows the `Engine` the
/// fill before the bar. An order still held at [`finish`](Self::finish) is answered as expired.
///
/// [`MockExecutionConfig::market_order_timing`]: rustrade_execution::client::mock::MockExecutionConfig::market_order_timing
/// [`MarketOrderTiming::NextBar`]: rustrade_execution::fill::MarketOrderTiming::NextBar
///
/// # Latency
/// With a non-zero `latency_ms`, a request reaches its venue `to_venue` after it was sent, and its
/// result reaches the `Engine` `from_venue` later, as on [`SimRunner`](super::SimRunner). A step or
/// settle only runs what is due by its instant, so a delayed fill lands in the first step whose
/// instant reaches it, before that step's market data.
///
/// # A market order on an instrument with no event at this instant
/// A venue keeps each instrument's last market. A market order on an instrument with no event at
/// the current instant is therefore filled at that instrument's last price, however old, and one
/// on an instrument that has never had a market is rejected. A real order would instead trade
/// against the book as it is now, or be refused if the market is closed or halted. This gap
/// between simulation and trading is known, and is the caller's to avoid, for example by sending
/// orders only for instruments that had an event at this instant.
///
/// [`AccountEventKind::Trade`]: rustrade_execution::AccountEventKind::Trade
/// [`AccountEventKind::OrderSnapshot`]: rustrade_execution::AccountEventKind::OrderSnapshot
///
/// # Bar timestamps
/// A bar's price is known only once it closes, so a candle must be stamped at its close:
/// `MarketEvent::time_exchange` is the candle's `close_time`, and the step's instant.
///
/// # Errors end the session
/// Every call returns an `Err` rather than panicking, including for a misconfiguration that
/// [`SimRunner`](super::SimRunner) panics on. After any `Err`, every later call returns the same
/// error: a partly applied step leaves no state worth continuing from.
///
/// # Ending
/// [`finish`](Self::finish) ends the session as a backtest ends: the `Engine` stops generating
/// orders, and everything still scheduled is delivered. A deadline after the last instant is
/// never reached. Dropping a stepper without finishing it is also fine: nothing was spawned, so
/// nothing is left running.
///
/// Shutting the `Engine` down instead, with [`SyncShutdown::shutdown`], abandons everything still
/// scheduled, as it does for a backtest, and the call that observes it fails with
/// [`SimStepError::Finished`].
///
/// [`SyncShutdown::shutdown`]: crate::shutdown::SyncShutdown::shutdown
pub struct SimStepper<MarketKind>
where
    MarketKind: VenueMarketUpdate,
{
    core: SimCore<MarketKind>,
    /// The clock's instant when this stepper was built, which the first step may not precede.
    session_start: DateTime<Utc>,
    /// The instant of the most recent step.
    instant: Option<DateTime<Utc>>,
    /// Whether the `Engine` passed to the first call has been checked against this stepper.
    checked: bool,
    ended: Option<Ended>,
}

/// Manual because a derive would demand `MarketKind: Debug`, as on `SimCore`.
impl<MarketKind> std::fmt::Debug for SimStepper<MarketKind>
where
    MarketKind: VenueMarketUpdate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimStepper")
            .field("core", &self.core)
            .field("session_start", &self.session_start)
            .field("instant", &self.instant)
            .field("checked", &self.checked)
            .field("ended", &self.ended)
            .finish()
    }
}

/// Why a [`SimStepper`] takes no further steps.
#[derive(Debug)]
enum Ended {
    /// [`SimStepper::finish`] ran.
    Finished,
    /// A call failed with this error, which every later call returns.
    Failed(BarterError),
}

/// Why a [`SimStepper`] call failed. After any of them the stepper takes no further steps.
///
/// `#[non_exhaustive]`: a later check adds a variant without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Error)]
#[non_exhaustive]
pub enum SimStepError {
    /// A step's instant is not after the previous step's, or is before the session start.
    #[error("a step at {time} is not after the previous instant {previous}")]
    NotLater {
        time: DateTime<Utc>,
        previous: DateTime<Utc>,
    },
    /// An event passed to a step is stamped at another instant.
    #[error("an event stamped {found} was passed to the step at {time}")]
    OffInstant {
        time: DateTime<Utc>,
        found: DateTime<Utc>,
    },
    /// The simulation is misconfigured in a way that would otherwise drop or misplace an event,
    /// such as a key absent from a venue's own index.
    #[error("{0}")]
    Misconfigured(String),
    /// The `Engine` was not built for this stepper: its clock is not the stepper's.
    #[error("the Engine was not built with this SimStepper's clock")]
    WrongEngine,
    /// The `Engine`'s positions on the venues' instruments differ from the
    /// [`SimStart`](super::SimStart) the venues were built from.
    #[error("the Engine's starting positions differ from the venues' SimStart: {0}")]
    StartMismatch(String),
    /// The `Engine` reported a terminal audit, such as an unrecoverable error, and stopped.
    #[error("the Engine stopped: {0}")]
    EngineStopped(String),
    /// The session already ended with [`SimStepper::finish`], or the `Engine` was shut down,
    /// abandoning everything still scheduled.
    #[error("the simulation has finished")]
    Finished,
}

/// How far a call runs the queue once its own events are delivered.
#[derive(Debug, Clone, Copy)]
enum Until {
    /// Up to and including this instant.
    Instant(DateTime<Utc>),
    /// Until nothing is left.
    Drained,
}

impl<MarketKind> SimStepper<MarketKind>
where
    MarketKind: VenueMarketUpdate,
{
    /// Construct a `SimStepper` over the provided venues.
    ///
    /// `clock` must be a clone of the [`HistoricalClock`] the `Engine` was built with; the first
    /// call checks it. Its instant now is the session start: the venues' opening balances are
    /// stamped at it, and no step may precede it.
    ///
    /// # Errors
    /// [`SimStepError::Misconfigured`] if a venue's opening account references an asset or
    /// instrument absent from that venue's own index.
    pub fn new(
        venues: FnvIndexMap<ExchangeIndex, SimVenue>,
        clock: HistoricalClock,
    ) -> Result<Self, BarterError> {
        let session_start = clock.time();
        let core = SimCore::new(venues, clock, DEFAULT_FEEDBACK_LIMIT).map_err(misconfigured)?;

        Ok(Self {
            core,
            session_start,
            instant: None,
            checked: false,
            ended: None,
        })
    }

    /// Set how many account events one call may deliver with no market event or venue deadline in
    /// between before it fails with [`BarterError::SimFeedbackLoop`]. Each call starts afresh. Defaults to
    /// [`DEFAULT_FEEDBACK_LIMIT`]; see [`SimRunner::with_feedback_limit`].
    ///
    /// [`SimRunner::with_feedback_limit`]: super::SimRunner::with_feedback_limit
    #[must_use]
    pub fn with_feedback_limit(mut self, limit: usize) -> Self {
        self.core.feedback_limit = limit;
        self
    }

    /// The venues this stepper drives.
    pub fn venues(&self) -> &FnvIndexMap<ExchangeIndex, SimVenue> {
        &self.core.venues
    }

    /// The instant of the most recent step, or `None` before the first.
    pub fn instant(&self) -> Option<DateTime<Utc>> {
        self.instant
    }

    /// Whether this stepper takes no further steps: it finished, or a call failed.
    pub fn has_ended(&self) -> bool {
        self.ended.is_some()
    }

    /// Advance to `time`, showing the `Engine` every event in `events`, all at that instant, and
    /// return once everything due by it is delivered. See the type's `# One step`.
    ///
    /// `events` may be empty, which moves simulated time to `time` and delivers what came due.
    ///
    /// # Errors
    /// - [`SimStepError::NotLater`] if `time` is not after the previous step's instant, or is
    ///   before the session start.
    /// - [`SimStepError::OffInstant`] if a market event's `time_exchange`, or a corporate
    ///   action's `effective_time`, is not `time`.
    /// - Those of every call: see [`settle`](Self::settle).
    pub fn step<GlobalData, InstrumentData, Strategy, Risk>(
        &mut self,
        engine: &mut SimEngine<GlobalData, InstrumentData, Strategy, Risk>,
        time: DateTime<Utc>,
        events: impl IntoIterator<Item = EngineEvent<MarketKind>>,
    ) -> Result<Vec<StepAudit<GlobalData, InstrumentData, Strategy, Risk, MarketKind>>, BarterError>
    where
        SimEngine<GlobalData, InstrumentData, Strategy, Risk>:
            Processor<EngineEvent<MarketKind>, Audit: Terminal + Debug>,
    {
        self.guarded(|this| {
            this.check_engine(engine)?;
            this.core.since_source = 0;

            let previous = this.instant.unwrap_or(this.session_start);
            if time < previous || this.instant.is_some_and(|previous| time == previous) {
                return Err(SimStepError::NotLater { time, previous }.into());
            }

            let mut batch = events.into_iter().collect::<Vec<_>>();
            if let Some(found) = batch.iter().filter_map(stamp).find(|found| *found != time) {
                return Err(SimStepError::OffInstant { time, found }.into());
            }
            // Stable, so events of one class keep the caller's order.
            batch.sort_by_key(source_class);

            let mut audits = Vec::with_capacity(batch.len());
            this.run(
                engine,
                &mut VecDeque::from(batch),
                Until::Instant(time),
                &mut audits,
            )?;

            // An instant with no market data has nothing that would move the clock, and the next
            // settle books at the clock's instant.
            engine.clock.advance_to(time);
            this.instant = Some(time);

            Ok(audits)
        })
    }

    /// Book every request the `Engine` sent since the previous call, at the current instant, and
    /// return once everything due by that instant is delivered. See the type's
    /// `# Acting between steps`.
    ///
    /// A request a non-zero latency delays past the current instant is booked by the step that
    /// reaches its arrival instead.
    ///
    /// # Errors
    /// Any of these ends the session:
    /// - [`SimStepError::WrongEngine`] if `engine` was not built with this stepper's clock, and
    ///   [`SimStepError::StartMismatch`] if its positions on the venues' instruments differ from
    ///   the venues' [`SimStart`](super::SimStart). Both are checked on the first call only.
    /// - [`BarterError::SimFeedbackLoop`] if the `Engine` keeps trading on its own fills with no
    ///   simulated time passing — see [`with_feedback_limit`](Self::with_feedback_limit).
    /// - [`SimStepError::EngineStopped`] if the `Engine` reports a terminal audit.
    /// - [`SimStepError::Misconfigured`] if a venue produces an event its index cannot translate,
    ///   or a simulated instant overflows.
    /// - [`SimStepError::Finished`] after [`finish`](Self::finish), or once the `Engine` has been
    ///   shut down, and the original error after any failed call.
    pub fn settle<GlobalData, InstrumentData, Strategy, Risk>(
        &mut self,
        engine: &mut SimEngine<GlobalData, InstrumentData, Strategy, Risk>,
    ) -> Result<Vec<StepAudit<GlobalData, InstrumentData, Strategy, Risk, MarketKind>>, BarterError>
    where
        SimEngine<GlobalData, InstrumentData, Strategy, Risk>:
            Processor<EngineEvent<MarketKind>, Audit: Terminal + Debug>,
    {
        self.guarded(|this| {
            this.check_engine(engine)?;
            // The caller acting is not the Engine trading on its own fills, so each call starts a
            // fresh feedback budget.
            this.core.since_source = 0;

            let mut audits = Vec::new();
            this.run(
                engine,
                &mut VecDeque::new(),
                Until::Instant(engine.clock.time()),
                &mut audits,
            )?;

            Ok(audits)
        })
    }

    /// End the session as a backtest ends, and return what that delivered.
    ///
    /// The `Engine` is shown [`Shutdown::AfterDrain`], so it stops generating orders, and then
    /// everything still scheduled is delivered: delayed fills, and responses to requests already
    /// sent. A venue deadline after the last instant is never reached.
    ///
    /// # Errors
    /// Those of [`settle`](Self::settle).
    pub fn finish<GlobalData, InstrumentData, Strategy, Risk>(
        &mut self,
        engine: &mut SimEngine<GlobalData, InstrumentData, Strategy, Risk>,
    ) -> Result<Vec<StepAudit<GlobalData, InstrumentData, Strategy, Risk, MarketKind>>, BarterError>
    where
        SimEngine<GlobalData, InstrumentData, Strategy, Risk>:
            Processor<EngineEvent<MarketKind>, Audit: Terminal + Debug>,
    {
        self.guarded(|this| {
            this.check_engine(engine)?;

            // As on source exhaustion in `SimRunner`: no deadline past the last instant, and a
            // fresh feedback budget for the drain.
            this.core.wakeups.stop();
            this.core.since_source = 0;

            let mut audits = Vec::new();
            process(
                engine,
                EngineEvent::Shutdown(Shutdown::AfterDrain),
                &mut audits,
            )?;
            this.run(engine, &mut VecDeque::new(), Until::Drained, &mut audits)?;

            this.ended = Some(Ended::Finished);
            Ok(audits)
        })
    }

    /// Run `call` unless the session has ended, and end it if `call` fails.
    fn guarded<T>(
        &mut self,
        call: impl FnOnce(&mut Self) -> Result<T, BarterError>,
    ) -> Result<T, BarterError> {
        match &self.ended {
            Some(Ended::Finished) => return Err(SimStepError::Finished.into()),
            Some(Ended::Failed(error)) => return Err(error.clone()),
            None => {}
        }

        let result = call(self);
        if let Err(error) = &result {
            self.ended = Some(Ended::Failed(error.clone()));
        }
        result
    }

    /// On the first call only: check that `engine` was built for this stepper.
    fn check_engine<GlobalData, InstrumentData, Strategy, Risk>(
        &mut self,
        engine: &SimEngine<GlobalData, InstrumentData, Strategy, Risk>,
    ) -> Result<(), SimStepError> {
        if self.checked {
            return Ok(());
        }

        if !engine.clock.is_shared_with(&self.core.clock) {
            return Err(SimStepError::WrongEngine);
        }
        check_start(&self.core.venues, &engine.state)?;

        self.checked = true;
        Ok(())
    }

    /// Deliver `batch`, merged with the queue on `(time, class)` as [`SimRunner`](super::SimRunner)
    /// merges its source, then whatever else is due by `until`.
    fn run<Processed>(
        &mut self,
        engine: &mut Processed,
        batch: &mut VecDeque<EngineEvent<MarketKind>>,
        until: Until,
        audits: &mut Vec<Processed::Audit>,
    ) -> Result<(), BarterError>
    where
        Processed: Processor<EngineEvent<MarketKind>, Audit: Terminal + Debug>,
    {
        loop {
            // Venues are seeded before anything else, so the Engine starts from a known account.
            if let Some(event) = self.core.seeding.pop_front() {
                process(engine, event, audits)?;
                continue;
            }

            // Everything the Engine sent while processing the previous event is already queued
            // (its requests are sent synchronously inside `process`), so this observes it in full.
            // Sent by `SyncShutdown::shutdown`, between calls or from a strategy hook within one.
            // Either way the session is over, and an `Ok` would claim this call's events delivered.
            if self.core.drain_requests().map_err(misconfigured)? == RequestDrain::Abandon {
                self.core.abandon();
                return Err(SimStepError::Finished.into());
            }

            // A bar held back behind the fill it released, once that fill is out: it came before
            // the rest of the batch.
            if let Some(event) = self.core.take_deferred() {
                process(engine, event, audits)?;
                continue;
            }

            let queue_leads = match (batch.front(), until) {
                // Not yet due, so what is due before it is in the queue.
                _ if self.core.has_deferred() => true,
                (Some(event), Until::Instant(time)) => self.core.leads((time, source_class(event))),
                (None, Until::Instant(time)) => self.core.due_by(time),
                // Until the queue pops empty, which ends the run below.
                (_, Until::Drained) => true,
            };

            if queue_leads {
                // Checked before popping, so the diagnostic can name the event that would have been
                // emitted rather than one already gone.
                if let Some(error) = self.core.check_feedback() {
                    return Err(error);
                }

                match self.core.pop().map_err(misconfigured)? {
                    Step::Emitted(event) => process(engine, event, audits)?,
                    Step::Acted | Step::Woken => {}
                    // Drained, once every market order still held for a bar that will never
                    // come is answered.
                    Step::Empty => {
                        if matches!(until, Until::Drained)
                            && self.core.expire_held().map_err(misconfigured)?
                        {
                            continue;
                        }
                        return Ok(());
                    }
                }
                continue;
            }

            // A drain carries no batch: the queue leads it until it pops empty, above.
            let (Some(event), Until::Instant(time)) = (batch.pop_front(), until) else {
                return Ok(());
            };

            // Routed before the Engine sees the event, so an order it sends in reply is matched
            // against the market as of this instant. A bar that released a held market order is
            // held back instead, and shown once its fill is out.
            if let Some(event) = self.core.route_source(time, event).map_err(misconfigured)? {
                process(engine, event, audits)?;
            }
        }
    }
}

/// Show `event` to the `Engine` and keep its audit, failing if the audit is terminal.
fn process<Processed, MarketKind>(
    engine: &mut Processed,
    event: EngineEvent<MarketKind>,
    audits: &mut Vec<Processed::Audit>,
) -> Result<(), BarterError>
where
    Processed: Processor<EngineEvent<MarketKind>, Audit: Terminal + Debug>,
{
    let audit = engine.process(event);
    if audit.is_terminal() {
        return Err(SimStepError::EngineStopped(format!("{audit:?}")).into());
    }

    audits.push(audit);
    Ok(())
}

/// The instant an event states it belongs to, for the events that state one.
fn stamp<MarketKind>(event: &EngineEvent<MarketKind>) -> Option<DateTime<Utc>> {
    match event {
        EngineEvent::Market(MarketStreamEvent::Item(event)) => Some(event.time_exchange),
        EngineEvent::CorporateAction { effective_time, .. } => Some(*effective_time),
        _ => None,
    }
}

/// Check the `Engine`'s positions on each venue built from a [`SimStart`](super::SimStart) against
/// the positions that start seeded.
///
/// Compared on instrument, side, quantity and entry price: what the venue's ledger and the
/// `Engine`'s position both depend on.
fn check_start<GlobalData, InstrumentData>(
    venues: &FnvIndexMap<ExchangeIndex, SimVenue>,
    state: &EngineState<GlobalData, InstrumentData>,
) -> Result<(), SimStepError> {
    type Held = (InstrumentNameInternal, Side, Decimal, Decimal);

    let started = venues
        .iter()
        .filter_map(|(exchange, slot)| Some((*exchange, slot.position_seeds.as_ref()?)))
        .collect::<Vec<_>>();
    if started.is_empty() {
        return Ok(());
    }

    let mut seeded = started
        .iter()
        .flat_map(|(_, seeds)| seeds.iter())
        .map(|seed: &PositionSeed| {
            (
                seed.instrument.clone(),
                seed.side,
                seed.quantity_abs,
                seed.price_entry_average,
            )
        })
        .collect::<Vec<Held>>();

    let mut held = state
        .instruments
        .0
        .values()
        .filter(|instrument| {
            started
                .iter()
                .any(|(exchange, _)| *exchange == instrument.instrument.exchange)
        })
        .flat_map(|instrument| {
            instrument.position.positions.values().map(|position| {
                (
                    instrument.instrument.name_internal.clone(),
                    position.side,
                    position.quantity_abs,
                    position.price_entry_average,
                )
            })
        })
        .collect::<Vec<Held>>();

    seeded.sort();
    held.sort();

    if seeded == held {
        Ok(())
    } else {
        Err(SimStepError::StartMismatch(format!(
            "the Engine holds {held:?}, but the venues were started with {seeded:?}"
        )))
    }
}

/// How a [`SimStepper`] reports a [`SimFault`], which [`SimRunner`](super::SimRunner) panics on.
fn misconfigured(reason: SimFault) -> BarterError {
    SimStepError::Misconfigured(format!("SimStepper {reason}")).into()
}

/// A `SimStepper` and the `Engine` it drives can be held in a map and moved between threads.
const _: () = {
    const fn assert_send<T: Send + 'static>() {}

    assert_send::<SimStepper<rustrade_data::event::DataKind>>();
    assert_send::<
        SimEngine<
            crate::engine::state::global::DefaultGlobalData,
            crate::engine::state::instrument::data::DefaultInstrumentMarketData,
            crate::strategy::DefaultStrategy<
                EngineState<
                    crate::engine::state::global::DefaultGlobalData,
                    crate::engine::state::instrument::data::DefaultInstrumentMarketData,
                >,
            >,
            crate::risk::DefaultRiskManager<
                EngineState<
                    crate::engine::state::global::DefaultGlobalData,
                    crate::engine::state::instrument::data::DefaultInstrumentMarketData,
                >,
            >,
        >,
    >();
};
