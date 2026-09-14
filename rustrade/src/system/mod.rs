/// Top-level trading system architecture for composing trading engines with execution components.
///
/// This module provides an architecture for building and running trading systems composed of the
/// `Engine` processor core and various execution components. The system framework abstracts away the
/// low-level concurrency and communication mechanisms, allowing users to focus on implementing
/// trading strategies.
use crate::{
    engine::{
        Processor,
        audit::{AuditTick, Auditor, context::EngineContext},
        command::Command,
        state::{instrument::filter::InstrumentFilter, trading::TradingState},
    },
    execution::builder::ExecutionHandles,
    shutdown::{AsyncShutdown, Shutdown},
};
use rustrade_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use rustrade_integration::{
    channel::{Tx, UnboundedRx, UnboundedTx},
    collection::{one_or_many::OneOrMany, snapshot::SnapUpdates},
};
use std::fmt::Debug;
use tokio::task::{AbortHandle, JoinError, JoinHandle};

/// Provides a `SystemBuilder` for constructing a Barter trading system, and associated types.
pub mod builder;

/// Provides a convenient `SystemConfig` used for defining a Barter trading system.
pub mod config;

/// Initialised and running Barter trading system.
///
/// Contains handles for the `Engine` and all auxillary system tasks.
///
/// It provides methods for interacting with the system, such as sending `Engine` [`Command`]s,
/// managing [`TradingState`], and shutting down gracefully.
// `#[derive(Debug)]` bounds only the type parameters, never the associated types `Engine::Audit`
// and `Engine::Snapshot` that `engine` and `audit` are built from, so it does not compile here. A
// hand-written impl would have to demand those bounds of every caller that merely holds a `System`.
#[allow(missing_debug_implementations)]
pub struct System<Engine, Event>
where
    Engine: Processor<Event> + Auditor<Engine::Audit, Context = EngineContext>,
{
    /// Task handle for the running `Engine`.
    pub engine: JoinHandle<(Engine, Engine::Audit)>,

    /// Handles to auxiliary system components (execution components, event forwarding, etc.).
    pub handles: SystemAuxillaryHandles,

    /// Transmitter for sending events directly to the `Engine`, bypassing any market/aux merge.
    ///
    /// # Ordering obligation (historical / backtest engines)
    /// Events injected here do **not** pass through the backtest harness's time-ordering asserts
    /// (`merge_market_with_aux` / `assert_aux_corporate_action_effective_times`). A
    /// [`HistoricalClock`](crate::engine::clock::HistoricalClock) advances monotonically off each
    /// event's `time_exchange` and never rewinds, so the caller must feed events in ascending
    /// simulated-time order — including any injected
    /// [`EngineEvent::CorporateAction`](crate::EngineEvent::CorporateAction), whose `effective_time`
    /// the clock advances to *before* the action is validated. Injecting an out-of-order event moves
    /// the clock forward (observably, via out-of-order logs) and subsequent earlier events will then
    /// not advance it. A [`LiveClock`](crate::engine::clock::LiveClock) is unaffected (it reads
    /// `Utc::now()`).
    pub feed_tx: UnboundedTx<Event>,

    /// Optional audit snapshot with updates (present when audit sending is enabled).
    pub audit:
        Option<SnapUpdates<AuditTick<Engine::Snapshot>, UnboundedRx<AuditTick<Engine::Audit>>>>,
}

impl<Engine, Event> System<Engine, Event>
where
    Engine: Processor<Event> + Auditor<Engine::Audit, Context = EngineContext>,
    Event: Debug + Clone + Send,
{
    /// Shutdown the `System` gracefully.
    ///
    /// Sends [`Shutdown::Immediate`], so the `Engine` stops at once and any execution request still
    /// in flight is abandoned — whatever those requests would have reported never arrives. That is
    /// normally what live trading wants: stopping should not wait on a venue that may be slow or
    /// unreachable.
    ///
    /// A caller that would rather wait for those responses sends [`Shutdown::AfterDrain`] into the
    /// `Engine` feed itself instead of calling this — as
    /// [`shutdown_after_backtest`](Self::shutdown_after_backtest) does.
    pub async fn shutdown(mut self) -> Result<(Engine, Engine::Audit), JoinError>
    where
        Event: From<Shutdown>,
    {
        self.send(Shutdown::Immediate);

        let (engine, shutdown_audit) = self.engine.await?;

        self.handles.shutdown().await?;

        Ok((engine, shutdown_audit))
    }

    /// [`AbortHandle`]s for every task this `System` spawned, engine included.
    ///
    /// [`JoinHandle::abort_handle`] only borrows, so this observes the task tree without consuming
    /// or disturbing it, and the returned handles stay valid after `self` is moved into a shutdown
    /// method.
    ///
    /// # Why this exists
    /// A `System` that is *dropped* rather than shut down leaves its tasks **detached**, not
    /// cancelled — that is what dropping a [`JoinHandle`] means. Two of them then cannot terminate
    /// on their own: the engine ends only on the explicit `Shutdown` that
    /// [`shutdown_after_backtest`](Self::shutdown_after_backtest) sends, and `account_to_engine`
    /// only on the explicit abort it performs — each skipped precisely when the future holding the
    /// `System` is dropped. Holding these across a cancellable await forces the teardown the
    /// graceful path would have done.
    pub(crate) fn abort_handles(&self) -> Vec<AbortHandle> {
        std::iter::once(self.engine.abort_handle())
            .chain(self.handles.abort_handles())
            .collect()
    }

    /// Shutdown the `System` ungracefully.
    pub async fn abort(self) -> Result<(Engine, Engine::Audit), JoinError>
    where
        Event: From<Shutdown>,
    {
        self.send(Shutdown::Immediate);

        let (engine, shutdown_audit) = self.engine.await?;

        self.handles.abort();

        Ok((engine, shutdown_audit))
    }

    /// Shutdown a backtesting `System` gracefully after the `Stream` of `MarketStreamEvent`s has
    /// ended.
    ///
    /// **Note that for live & paper-trading this market stream will never end, so use
    /// System::shutdown() for that use case**.
    ///
    /// # Why this sends [`Shutdown::AfterDrain`]
    /// The `Engine` reads market events and account events from a **single FIFO feed**, and the
    /// task forwarding the market stream into it completes as soon as the stream has been
    /// *forwarded* — not when the `Engine` has *processed* it. A stop enqueued at that moment
    /// therefore sits ahead of every account event the run is about to produce, and the `Engine`
    /// terminates on it without ever reading them.
    ///
    /// Ordering alone cannot fix that: the responses are provoked *by* processing the final market
    /// events, so they are necessarily behind any marker placed after those events.
    /// [`Shutdown::AfterDrain`] instead has the `Engine` stop generating new orders and wait until
    /// nothing is left in flight, which is the only point at which the run has genuinely finished.
    ///
    /// # Panics
    /// Panics if the Engine task has already dropped its receiver (i.e., panicked).
    pub async fn shutdown_after_backtest(self) -> Result<(Engine, Engine::Audit), JoinError>
    where
        Event: From<Shutdown>,
    {
        let Self {
            engine,
            handles:
                SystemAuxillaryHandles {
                    mut execution,
                    market_to_engine,
                    account_to_engine,
                },
            feed_tx,
            audit: _,
        } = self;

        // Wait for the MarketStream to finish forwarding before initiating shutdown. Note that
        // this returns once the events are IN the feed, not once the Engine has processed them --
        // which is precisely why the stop below has to be `AfterDrain` rather than immediate.
        market_to_engine.await?;

        #[allow(clippy::expect_used)] // Critical invariant: Engine must be alive during shutdown
        feed_tx
            .send(Shutdown::AfterDrain)
            .expect("Engine cannot drop Feed receiver");
        drop(feed_tx);

        let (engine, shutdown_audit) = engine.await?;

        account_to_engine.abort();
        execution.shutdown().await?;

        Ok((engine, shutdown_audit))
    }

    /// Send [`OrderRequestCancel`]s to the `Engine` for execution.
    pub fn send_cancel_requests(&self, requests: OneOrMany<OrderRequestCancel>)
    where
        Event: From<Command>,
    {
        self.send(Command::SendCancelRequests(requests))
    }

    /// Send [`OrderRequestOpen`]s to the `Engine` for execution.
    pub fn send_open_requests(&self, requests: OneOrMany<OrderRequestOpen>)
    where
        Event: From<Command>,
    {
        self.send(Command::SendOpenRequests(requests))
    }

    /// Instruct the `Engine` to close open positions.
    ///
    /// Use the `InstrumentFilter` to configure which positions are closed.
    pub fn close_positions(&self, filter: InstrumentFilter)
    where
        Event: From<Command>,
    {
        self.send(Command::ClosePositions(filter))
    }

    /// Instruct the `Engine` to cancel open orders.
    ///
    /// Use the `InstrumentFilter` to configure which orders are cancelled.
    pub fn cancel_orders(&self, filter: InstrumentFilter)
    where
        Event: From<Command>,
    {
        self.send(Command::CancelOrders(filter))
    }

    /// Update the algorithmic `TradingState` of the `Engine`.
    pub fn trading_state(&self, trading_state: TradingState)
    where
        Event: From<TradingState>,
    {
        self.send(trading_state)
    }

    /// Take ownership of the audit snapshot with updates if present.
    ///
    /// Note that by this will not be present if the `System` was built in
    /// [`AuditMode::Disabled`](builder::AuditMode) (default).
    pub fn take_audit(
        &mut self,
    ) -> Option<SnapUpdates<AuditTick<Engine::Snapshot>, UnboundedRx<AuditTick<Engine::Audit>>>>
    {
        self.audit.take()
    }

    /// Send an `Event` to the `Engine`.
    #[allow(clippy::expect_used)] // Critical invariant: Engine must be alive to receive events
    fn send<T>(&self, event: T)
    where
        T: Into<Event>,
    {
        self.feed_tx
            .send(event)
            .expect("Engine cannot drop Feed receiver")
    }
}

/// Collection of task handles for auxiliary system components that support the `Engine`.
///
/// Used by the [`System`] to shut down auxillary components.
#[derive(Debug)]
pub struct SystemAuxillaryHandles {
    /// Handles for running execution components.
    pub execution: ExecutionHandles,

    /// Task that forwards market events to the engine.
    pub market_to_engine: JoinHandle<()>,

    /// Task that forwards account events to the engine.
    pub account_to_engine: JoinHandle<()>,
}

impl AsyncShutdown for SystemAuxillaryHandles {
    type Result = Result<(), JoinError>;

    async fn shutdown(&mut self) -> Self::Result {
        // Event -> Engine tasks do not need graceful shutdown, so abort
        self.market_to_engine.abort();
        self.account_to_engine.abort();

        // Await execution components shutdowns concurrently
        self.execution.shutdown().await
    }
}

impl SystemAuxillaryHandles {
    pub fn abort(self) {
        self.execution
            .into_iter()
            .chain(std::iter::once(self.market_to_engine))
            .chain(std::iter::once(self.account_to_engine))
            .for_each(|handle| handle.abort());
    }

    /// [`AbortHandle`]s for every auxiliary task, without consuming the handles.
    ///
    /// Enumerates the same task set as [`abort`](Self::abort) — a task added to this struct must be
    /// added to both.
    pub(crate) fn abort_handles(&self) -> impl Iterator<Item = AbortHandle> + '_ {
        self.execution.abort_handles().chain([
            self.market_to_engine.abort_handle(),
            self.account_to_engine.abort_handle(),
        ])
    }
}
