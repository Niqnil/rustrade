use crate::{EngineEvent, Timed};
/// Backtesting utilities for algorithmic trading strategies.
///
/// This module provides tools for running historical simulations of trading strategies
/// using market data, and analyzing the performance of these simulations.
use crate::{
    backtest::{
        aux_events::{
            AuxEventSource, NoAuxEvents, assert_aux_contract_expiry_times,
            assert_aux_corporate_action_effective_times, assert_aux_events_sorted,
        },
        market_data::BacktestMarketData,
        summary::{BacktestResult, BacktestSummary, MultiBacktestSummary},
    },
    engine::{
        Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        run::async_run,
        state::{
            EngineState, connectivity::reconcile_venue_roles, instrument::data::InstrumentDataState,
        },
    },
    error::BarterError,
    risk::RiskManager,
    statistic::time::TimeInterval,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::config::ExecutionConfig,
};
use crate::{
    engine::Engine,
    execution::sim::{
        SimExecutionBuild, SimExecutionBuilder, SimRunner, VenueMarketUpdate, log_venue_summary,
    },
};
use chrono::{DateTime, Utc};
use fnv::FnvHashSet;
use futures::{Stream, StreamExt, future::try_join_all, stream::FusedStream};
use rust_decimal::Decimal;
use rustrade_data::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use rustrade_execution::AccountEvent;
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use smol_str::SmolStr;
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use tracing::error;

/// Defines the [`AuxEventSource`] interface for interleaving non-market `EngineEvent`s (e.g.
/// corporate actions, contract expiries) into a backtest in simulated-time order.
pub mod aux_events;

/// Defines the interface and implementations for different types of market data sources
/// that can be used in backtests.
pub mod market_data;

/// Contains data structures for representing backtest results and metrics.
pub mod summary;

/// Configuration for constants used across all backtests in a batch.
///
/// Contains shared inputs like instruments, execution configurations,
/// market data, and summary time intervals.
#[derive(Debug, Clone)]
pub struct BacktestArgsConstant<MarketData, SummaryInterval, State, AuxEvents = NoAuxEvents> {
    /// Set of trading instruments indexed by unique identifiers.
    pub instruments: IndexedInstruments,
    /// Exchange execution configurations.
    pub executions: Vec<ExecutionConfig>,
    /// Historical market data to use for simulation.
    pub market_data: MarketData,
    /// Time interval for aggregating and reporting summary statistics.
    pub summary_interval: SummaryInterval,
    /// The [`EngineState`] every backtest in the batch starts from.
    ///
    /// Built by the caller, and never modified here: each run reconciles venue roles on its own
    /// clone — see the `# Connectivity` section on [`backtest`].
    pub engine_state: State,
    /// Source of auxiliary (non-market) `EngineEvent`s to interleave with the market data in
    /// simulated-time order (e.g. corporate actions, contract expiries).
    ///
    /// Defaults to [`NoAuxEvents`] (yields nothing), so existing backtests opt out at negligible
    /// per-event overhead.
    /// Corporate actions are market facts shared across an entire strategy sweep, so they live on
    /// this shared constant and thread through [`run_backtests`] for free.
    pub aux_events: AuxEvents,
}

/// Configuration for variables that can change between individual backtests.
///
/// Contains parameters that define a specific strategy variant to test.
#[derive(Debug, Clone)]
pub struct BacktestArgsDynamic<Strategy, Risk> {
    /// Unique identifier for this backtest.
    pub id: SmolStr,
    /// Risk-free return rate used for performance metrics.
    pub risk_free_return: Decimal,
    /// Trading strategy to backtest.
    pub strategy: Strategy,
    /// Risk management rules.
    pub risk: Risk,
}
/// Run multiple backtests concurrently, each with different strategy parameters.
///
/// Takes the shared constants and an iterator of different strategy configurations,
/// then executes all backtests in parallel, collecting the results.
///
/// # A failing run cancels its siblings
/// The first run to fail short-circuits the batch: the others are cancelled rather than allowed to
/// finish, and their task trees are torn down with them (see `AbortOnDrop`). Nothing partial is
/// returned for a cancelled run, and no summary is produced for it — a sweep either yields one
/// [`BacktestSummary`] per configuration or fails as a whole. A cancelled run's market source stops
/// being read at its next await point, which for a metered provider bounds what a doomed sweep
/// spends.
pub async fn run_backtests<
    MarketData,
    SummaryInterval,
    Strategy,
    Risk,
    GlobalData,
    InstrumentData,
    Aux,
>(
    args_constant: Arc<
        BacktestArgsConstant<
            MarketData,
            SummaryInterval,
            EngineState<GlobalData, InstrumentData>,
            Aux,
        >,
    >,
    args_dynamic_iter: impl IntoIterator<Item = BacktestArgsDynamic<Strategy, Risk>>,
) -> Result<MultiBacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    // The simulated venues are fed each market event before the `Engine` sees it, which needs
    // a way to read a market out of this kind. Satisfied by `DataKind`.
    InstrumentData::MarketEventKind: VenueMarketUpdate,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Default + Send + 'static,
    Aux: AuxEventSource<InstrumentData::MarketEventKind, ExchangeIndex, AssetIndex, InstrumentIndex>
        + Send
        + Sync,
{
    let time_start = std::time::Instant::now();

    // Project each run down to its `summary` inside its own future, so the terminal `EngineState`
    // is dropped as that run completes rather than pinned in the joined output until the whole
    // batch finishes. The batch path intentionally does not retain per-run terminal `EngineState`
    // (callers needing it drive `backtest` directly).
    let backtest_futures = args_dynamic_iter.into_iter().map(|args_dynamic| {
        let args_constant = Arc::clone(&args_constant);
        async move {
            backtest(args_constant, args_dynamic)
                .await
                .map(|result| result.summary)
        }
    });

    // Run all backtests concurrently
    let summaries = try_join_all(backtest_futures).await?;

    Ok(MultiBacktestSummary::new(
        std::time::Instant::now().duration_since(time_start),
        summaries,
    ))
}

/// Run a single backtest with the given parameters.
///
/// Simulates a trading strategy using historical market data and generates performance metrics.
///
/// # Auxiliary (non-market) event injection
/// Events from [`BacktestArgsConstant::aux_events`] (corporate actions, contract expiries, commands)
/// are **pre-merged** with the market stream into one time-ordered stream before the engine, so each
/// is processed at the correct point in simulated time. An aux event sharing a timestamp with a
/// market event is ordered **first** (so e.g. a split applies before same-instant fills), and the
/// engine clock is seeded from `min(first_market_event, first_aux_event)` — an aux event scheduled
/// before the first market tick still orders and stamps correctly. The merge/tie-break/seed logic is
/// unit-tested in `backtest::tests`.
///
/// # What the returned result contains
/// Returns a [`BacktestResult`] with two parts:
/// - `summary`: a [`BacktestSummary`] whose `trading_summary` aggregates statistics derived from
///   **closed** positions (PnL, returns, drawdown per `TearSheet`). A position left **open** at the
///   end of the run — e.g. one that took a stock split but no subsequent closing fill — contributes
///   nothing to it, and a notional-preserving split moves no aggregate metric on its own.
/// - `engine_state`: the terminal [`EngineState`], so callers can inspect post-run positions directly
///   (e.g. assert a corporate action rescaled an open position's `quantity_abs` /
///   `price_entry_average`) without driving their own engine harness.
///
/// # Errors
/// Returns [`BarterError::BacktestAllOrdersRejected`] when the strategy sent open requests and
/// the exchange rejected every one of them. Such a run filled nothing, so its statistics describe
/// no trading at all. Partial rejections are not an error — an insufficient balance is a
/// legitimate simulated outcome — and a strategy that sends no requests is unaffected. Per-instrument
/// counts and the first reason are on each `TearSheet`.
///
/// Returns [`BarterError::SimFeedbackLoop`] when the strategy trades on its own fills against a
/// venue whose simulated round trip is zero. That is a zero-delay cycle rather than a slow run — no
/// scheduling can order a response both after its cause and before the next input — so it is
/// reported instead of being run. See [`SimRunner`].
///
/// This path drives the engine through [`async_run`], which generates no audit stream, so the
/// per-event `EngineOutput` audit is not observable here; the split *economics per event* are
/// asserted at the `Engine::process_with_audit` seam (see the `test_corporate_action_*` tests). The
/// terminal `engine_state` exposes the net effect.
///
/// # Market source failure
/// A [`BacktestMarketData`] source that fails part-way through — a truncated file, a decode error, a
/// failed page fetch — **aborts the run**: the engine is shut down and that error is returned in
/// place of a result. No partial [`BacktestSummary`] is ever produced, because statistics computed
/// over a prefix of the dataset are indistinguishable from statistics over all of it.
///
/// # Connectivity: derived, not declared
/// Each venue's [`VenueRole`] is re-derived from the execution clients this function builds, on the
/// clone it runs against — see [`reconcile_venue_roles`]. A venue registered purely so its prices
/// are available, with nothing executing there, is therefore marked [`VenueRole::DataOnly`] and is
/// never waited on for an account connection nothing would establish, whether or not the caller
/// declared anything.
///
/// The supplied `engine_state` is not modified, and declaring the venues upfront with
/// [`EngineStateBuilder::execution_venues`] remains supported: both derive the same roles from the
/// same inputs, so a state built that way reconciles to itself.
///
/// One configuration still cannot converge, and is reported rather than inferred: a venue that
/// neither prices an instrument nor has an execution client provides no connection that could
/// report healthy, so `ConnectivityStates::global` stays [`Health::Reconnecting`] for the whole
/// run. `reconcile_venue_roles` logs a warning naming it.
///
/// [`SystemBuilder`]: crate::system::builder::SystemBuilder
/// [`EngineStateBuilder::execution_venues`]: crate::engine::state::builder::EngineStateBuilder::execution_venues
/// [`VenueRole`]: crate::engine::state::connectivity::VenueRole
/// [`VenueRole::DataOnly`]: crate::engine::state::connectivity::VenueRole::DataOnly
/// [`Health::Reconnecting`]: crate::engine::state::connectivity::Health::Reconnecting
pub async fn backtest<
    MarketData,
    SummaryInterval,
    Strategy,
    Risk,
    GlobalData,
    InstrumentData,
    Aux,
>(
    args_constant: Arc<
        BacktestArgsConstant<
            MarketData,
            SummaryInterval,
            EngineState<GlobalData, InstrumentData>,
            Aux,
        >,
    >,
    args_dynamic: BacktestArgsDynamic<Strategy, Risk>,
) -> Result<BacktestResult<SummaryInterval, EngineState<GlobalData, InstrumentData>>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    // The simulated venues are fed each market event before the `Engine` sees it, which needs
    // a way to read a market out of this kind. Satisfied by `DataKind`.
    InstrumentData::MarketEventKind: VenueMarketUpdate,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Send + 'static,
    Aux: AuxEventSource<InstrumentData::MarketEventKind, ExchangeIndex, AssetIndex, InstrumentIndex>
        + Send
        + Sync,
{
    // Lazily merge the market stream with the auxiliary (non-market) events into a single
    // time-ordered stream BEFORE the engine channel, so an injected event (e.g. a corporate action)
    // is processed at the correct point in simulated time. The market side stays lazy — it is never
    // collected — so peak memory is O(1) in the dataset size, and the common no-aux case adds only
    // negligible per-event overhead vs a pre-corporate-action backtest. Merging into one stream — rather than
    // forwarding market and aux as two producers into the engine feed — is what preserves the time
    // order (two `forward_to` tasks would interleave non-deterministically). See [`AuxEventSource`].
    let market_first = args_constant.market_data.time_first_event().await?;
    // Boxed so the merged feed is `Unpin`, which is what lets the engine poll it inline via
    // `async_run` rather than through a forwarding task. A `BacktestMarketData` source is an
    // `impl Stream` with no `Unpin` guarantee, and this is the one place that can pin it.
    let raw_market = Box::pin(args_constant.market_data.stream().await?);
    // The aux side is tiny (corporate actions / expiries number in the handful), so collecting it is
    // cheap and lets the merge peek it synchronously.
    let aux = args_constant.aux_events.aux_events().collect::<Vec<_>>();
    // Enforce the `AuxEventSource` contract before the merge below, which relies on `aux` being sorted
    // ascending by `Timed::time`. A custom source yielding unsorted events would otherwise silently feed
    // the engine a non-monotonic timeline in release builds — wrong results with no failure point. Hard
    // panic shared with `AuxEventsInMemory::new`; the aux set is handful-sized, so the O(n) scan is
    // immeasurable. See [`assert_aux_events_sorted`].
    assert_aux_events_sorted(&aux);
    // Enforce the second `AuxEventSource` obligation: every injected `CorporateAction`'s
    // `effective_time` must equal its wrapping `Timed::time`. A mismatch would order the action at
    // one instant but apply it at another (a silent look-ahead / stale-stamp bug); the handler
    // cannot see the wrapping `Timed`, so this pre-merge site is the only place to catch it. Hard
    // panic, handful-sized scan. See [`assert_aux_corporate_action_effective_times`].
    assert_aux_corporate_action_effective_times(&aux);
    // Enforce the third `AuxEventSource` obligation: every injected `ContractExpiry`'s wrapping
    // `Timed::time` must equal its target instrument's own `expiry` (engine-side ground truth on the
    // `InstrumentKind`). The instant is not on the payload, so — like the corporate-action check —
    // the handler cannot see the wrapping `Timed`; a mismatch would order the expiry at one instant
    // but settle it at another (silent look-ahead). Hard panic, handful-sized scan. See
    // [`assert_aux_contract_expiry_times`].
    assert_aux_contract_expiry_times(&aux, &args_constant.instruments);

    // Seed the clock from the earliest of the first market event and the first aux event, so an aux
    // event scheduled before the first market tick still orders and stamps correctly.
    let clock_start = aux
        .first()
        .map_or(market_first, |first| market_first.min(first.time));
    let clock = HistoricalClock::new(clock_start);

    // Build the simulated venues. Nothing is spawned, connected or awaited: `SimRunner` drives
    // them inline on the engine's own thread, which is what makes a run reproducible.
    let SimExecutionBuild {
        execution_tx_map,
        venues,
    } = args_constant
        .executions
        .clone()
        .into_iter()
        .try_fold(
            SimExecutionBuilder::new(&args_constant.instruments),
            |builder, config| match config {
                ExecutionConfig::Mock(mock_config) => builder.add_venue(mock_config),
            },
        )?
        .build();

    // The same derivation `SystemBuilder` performs when it builds the state itself. Read before
    // `execution_tx_map` is moved into the Engine below.
    let execution_venues = execution_tx_map
        .execution_venues()
        .collect::<FnvHashSet<_>>();

    // Applied to the clone, so the caller's own `engine_state` is left untouched and every backtest
    // in a sweep derives its roles from the clients IT was built with.
    let mut engine_state = args_constant.engine_state.clone();
    reconcile_venue_roles(
        &mut engine_state.connectivity,
        &args_constant.instruments,
        &execution_venues,
    );

    let mut engine = Engine::new(
        clock.clone(),
        engine_state,
        execution_tx_map,
        args_dynamic.strategy,
        args_dynamic.risk,
    );

    // Lazily merge the market stream with the aux events into one time-ordered stream.
    // Per-run, so concurrent `run_backtests` runs never observe each other's source failures.
    let source_error = Arc::new(OnceLock::new());
    let source = merge_market_with_aux(raw_market, market_first, aux, Arc::clone(&source_error));

    // Interleave the venues' account events into that stream in simulated time, and let the engine
    // poll the result **inline**. There is deliberately no channel and no forwarding task between
    // the two: the engine sends its execution requests synchronously inside `process`, so polling
    // the feed inline is what lets a fill be booked and scheduled before the next market event is
    // drawn. Forwarding through a channel instead is what made fill placement — and therefore every
    // time-derived statistic — a function of the tokio scheduler rather than of the dataset.
    let mut feed = SimRunner::new(venues, source, clock);

    // Ends when the source is exhausted *and* every scheduled deliverable has been drained, so the
    // run cannot stop while a fill it provoked is still owed. That is the whole of this path's
    // shutdown: no `Shutdown` to enqueue behind the account events it would truncate, no task tree
    // to abort, and nothing to await beyond the engine loop itself.
    let _shutdown_audit = async_run(&mut feed, &mut engine).await;

    log_venue_summary(feed.venues());

    // A feed ending says only that there is no more input; it does not say the run completed. Both
    // ways it can end early produce a complete-looking summary over an incomplete dataset, so both
    // are checked before any statistic is generated. The feed has ended by this point, so each is
    // settled.
    //
    // The source failing is the dataset's fault; a feedback loop is the configuration's. Neither
    // can reach the caller as a tear sheet.
    if let Some(error) = source_error.get() {
        return Err(error.clone());
    }

    if let Some(error) = feed.error() {
        return Err(error.clone());
    }

    let trading_summary = engine
        .trading_summary_generator(args_dynamic.risk_free_return)
        .generate(args_constant.summary_interval);

    // A run in which every open request was rejected measured nothing: the summary it would
    // return is a tear sheet of zeros, which reads exactly like a strategy that chose to stay
    // flat. Report it as the failure it is rather than handing back a plausible-looking result.
    // Partial rejections are left alone -- an insufficient balance is a legitimate simulated
    // outcome that a strategy should experience -- and a strategy that sends no requests at all
    // never trips this.
    if trading_summary.rejected_every_order() {
        return Err(BarterError::BacktestAllOrdersRejected {
            rejected: trading_summary.orders_rejected,
            reason: trading_summary
                .instruments
                .values()
                .find_map(|sheet| sheet.first_rejection_reason.clone())
                .unwrap_or_else(|| "reason not recorded".to_string()),
        });
    }

    // `trading_summary_generator` only borrows the engine, so the terminal state can be moved out
    // here and returned for direct post-run inspection (open positions, balances, instrument state).
    Ok(BacktestResult {
        summary: BacktestSummary {
            id: args_dynamic.id,
            risk_free_return: args_dynamic.risk_free_return,
            trading_summary,
        },
        engine_state: engine.state,
    })
}

/// A lazy, time-ordered two-way merge of a backtest market stream and pre-collected auxiliary
/// (non-market) events.
///
/// The market side stays **lazy** — it is polled on demand and never collected, so peak memory is
/// O(1) in the dataset size and the common no-aux case adds only negligible per-event overhead
/// versus a pre-corporate-action backtest. The aux side is tiny (corporate actions / expiries number in the handful), so it is
/// held as a `Peekable` iterator and peeked synchronously to decide ordering.
///
/// # Ordering contract
/// - `aux` MUST be sorted ascending by [`Timed::time`] (the [`AuxEventSource`] obligation); the
///   market stream is assumed time-sorted by `time_exchange`.
/// - Aux events win ties (`aux.time <= market.time`), so an injected event at the same instant as a
///   market event is processed first — e.g. a stock split adjusts positions before any fill stamped
///   at that instant.
/// - Between two **aux** events at the same instant, ordering follows their original
///   [`AuxEventSource`] order: the aux side is consumed as a stable, in-order iterator and the merge
///   never reorders equal-time aux events. Inject aux events already in the order you want ties
///   broken.
/// - A [`MarketStreamEvent::Reconnecting`] carries no timestamp; its ordering inherits the prior
///   market event's `time_exchange` (`last_market_time`), falling back to the seed only if it leads.
///   For an in-memory backtest no `Reconnecting` events occur, so the carry-forward is purely
///   defensive.
///
/// # Market source failure
/// An `Err` from the market stream **terminates the merge**: the error is recorded in the shared
/// `source_error` slot and the stream ends, dropping any aux events still pending. The engine then
/// shuts down normally and [`backtest`] reads the slot and returns the error rather than a summary.
/// Continuing past the error would silently produce statistics over a truncated dataset. The slot
/// is created per run, so concurrent [`run_backtests`] runs never observe each other's failures.
///
/// A market event that moves time backwards is treated identically — see [`take_market`].
///
/// # Termination is sticky
/// The first `Ready(None)` latches, so a later poll yields `None` rather than resuming. That is
/// what makes the abort above actually an abort: the terminating paths end the *market* side, and
/// without the latch the next poll would fall through to draining `aux` — emitting events at
/// instants the data never reached, after the run was declared over. Latent while the only consumer
/// is `forward_to` (which stops at the first `None`), which is precisely why it is pinned here
/// rather than left to that consumer. [`FusedStream`] reports the same latch, so a `select!` over
/// this stream needs no redundant `.fuse()`.
///
/// # The ordering time is published, not discarded
/// Each item is a [`Timed`] [`EngineEvent`] carrying the instant this merge ordered it by. The
/// engine ignores the wrapper and reads simulated time from the event itself, but a consumer that
/// must interleave a *third* source against this one needs that instant before it can decide which
/// side leads — and for a [`MarketStreamEvent::Reconnecting`], which carries no timestamp of its
/// own, this merge's carried-forward `last_market_time` is the only place it exists. See
/// [`SimRunner`], which schedules simulated account events against it.
///
/// [`SimRunner`]: crate::execution::sim::SimRunner
#[pin_project::pin_project]
struct TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, MarketKind>, BarterError>>,
{
    #[pin]
    market: futures::stream::Peekable<St>,
    aux: std::iter::Peekable<
        std::vec::IntoIter<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
    >,
    last_market_time: DateTime<Utc>,
    source_error: Arc<OnceLock<BarterError>>,
    terminated: bool,
}

impl<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey> Stream
    for TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, MarketKind>, BarterError>>,
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    type Item = Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.terminated {
            return Poll::Ready(None);
        }

        let polled = match this.aux.peek().map(|timed| timed.time) {
            // No aux remaining: forward the market side verbatim (the O(1)-memory fast path).
            None => {
                let polled = this.market.as_mut().poll_next(cx);
                take_market(polled, this.last_market_time, this.source_error)
            }
            // Aux has an event: peek the market's next event to order them.
            Some(aux_time) => match this.market.as_mut().poll_peek(cx) {
                // Can't decide ordering until the market's next event (or its end) is known.
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(Ok(market_next))) => {
                    // Copy the Copy `DateTime` out so the peek borrow of `this.market` ends here,
                    // freeing `this.market` to be re-polled in the `else` arm below.
                    let market_time = match market_next {
                        MarketStreamEvent::Item(market_event) => market_event.time_exchange,
                        MarketStreamEvent::Reconnecting(_) => *this.last_market_time,
                    };
                    if aux_time <= market_time {
                        // Aux leads or ties — emit it. `aux.peek()` was `Some`, so `next()` is
                        // `Some`.
                        Poll::Ready(this.aux.next())
                    } else {
                        let polled = this.market.as_mut().poll_next(cx);
                        take_market(polled, this.last_market_time, this.source_error)
                    }
                }
                // A failed source outranks any pending aux event: the run is over either way, and
                // emitting further aux events would imply a timeline the data never reached. The
                // peeked item is buffered, so re-polling returns it synchronously for `take_market`
                // to record.
                Poll::Ready(Some(Err(_))) => {
                    let polled = this.market.as_mut().poll_next(cx);
                    take_market(polled, this.last_market_time, this.source_error)
                }
                // Market exhausted — drain the remaining aux events in order.
                Poll::Ready(None) => Poll::Ready(this.aux.next()),
            },
        };

        if matches!(polled, Poll::Ready(None)) {
            *this.terminated = true;
        }

        polled
    }
}

impl<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey> FusedStream
    for TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, MarketKind>, BarterError>>,
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    /// Terminated exactly once a poll has returned `Ready(None)`, which is the latch `poll_next`
    /// sets — so this cannot drift from the stream's actual behaviour.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

/// Convert one polled market item into a merged output, recording a source failure.
///
/// An `Err` ends the merged stream (`Ready(None)`) after storing the error, so [`backtest`] can
/// distinguish a failed run from a complete one. Only the first error is kept — it is the one that
/// stopped the run, and any later error is a consequence of it.
///
/// # The ordering obligation is enforced here, in release
/// A market event stamped earlier than the merge has already advanced to is treated exactly like a
/// source failure: recorded and terminal. This is the only place a *streamed* source's ordering can
/// be checked — [`MarketDataInMemory`](market_data::MarketDataInMemory) asserts sortedness in its
/// constructor, but a lazy source cannot be inspected without reading it, so the check has to ride
/// the read. The comparison is one `DateTime` compare per event against a value this function
/// already maintains for `Reconnecting` ordering, so the streamed path is not paying for a
/// guarantee the in-memory path gets free.
///
/// Aborting rather than warning matches the in-memory path's hard `assert!`: an event that moves
/// simulated time backwards produces a non-monotonic clock, and every statistic computed after it
/// is wrong in a way no downstream consumer can detect. A `debug_assert!` — the only prior check,
/// inside
/// [`merge_time_sorted`](rustrade_data::streams::merge::merge_time_sorted) and only if the factory
/// happened to use it — is compiled out of exactly the builds a real backtest runs in.
///
/// For the first event, `last_market_time` still holds the seed, which is the
/// [`time_first_event`](market_data::BacktestMarketData::time_first_event) the source itself
/// reported. A stream whose first event precedes that contradicts its own source, and the aux merge
/// was already seeded against the wrong instant, so it is the same violation.
fn take_market<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    polled: Poll<Option<Result<MarketStreamEvent<InstrumentKey, MarketKind>, BarterError>>>,
    last_market_time: &mut DateTime<Utc>,
    source_error: &Arc<OnceLock<BarterError>>,
) -> Poll<Option<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>>
where
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    match polled {
        Poll::Pending => Poll::Pending,
        Poll::Ready(None) => Poll::Ready(None),
        Poll::Ready(Some(Ok(event))) => {
            // Only `Item` carries a timestamp; `Reconnecting` inherits `last_market_time` and so
            // can never violate the ordering it is ordered by.
            if let MarketStreamEvent::Item(market_event) = &event
                && market_event.time_exchange < *last_market_time
            {
                let error = BarterError::BacktestMarketData(format!(
                    "market data must be sorted ascending by MarketEvent::time_exchange, but an \
                     event stamped {} arrived after the merge had advanced to {}",
                    market_event.time_exchange, last_market_time
                ));
                error!(
                    %error,
                    "backtest market data source is not time-sorted - aborting run rather than \
                     reporting a summary over a non-monotonic clock"
                );
                let _ = source_error.set(error);
                return Poll::Ready(None);
            }
            Poll::Ready(Some(convert_market(event, last_market_time)))
        }
        Poll::Ready(Some(Err(error))) => {
            error!(
                %error,
                "backtest market data source failed - aborting run rather than reporting a \
                 summary over partial data"
            );
            let _ = source_error.set(error);
            Poll::Ready(None)
        }
    }
}

/// Convert a market event to a [`Timed`] [`EngineEvent`], advancing `last_market_time` on an `Item`
/// so a later [`MarketStreamEvent::Reconnecting`] (which has no timestamp) can inherit the prior
/// time for ordering.
///
/// The [`Timed::time`] returned is the instant the merge ordered this event by — the event's own
/// `time_exchange`, or the carried-forward `last_market_time` for a `Reconnecting`. Publishing it
/// rather than discarding it is what lets a downstream consumer interleave against this stream
/// without re-deriving an ordering key the merge has already computed. See [`SimRunner`].
///
/// [`SimRunner`]: crate::execution::sim::SimRunner
fn convert_market<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    event: MarketStreamEvent<InstrumentKey, MarketKind>,
    last_market_time: &mut DateTime<Utc>,
) -> Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>
where
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    if let MarketStreamEvent::Item(market_event) = &event {
        *last_market_time = market_event.time_exchange;
    }
    Timed {
        value: EngineEvent::from(event),
        time: *last_market_time,
    }
}

/// Build a [`TimedMergeStream`]. `seed` is the fallback ordering time for a leading
/// [`MarketStreamEvent::Reconnecting`]; `aux` MUST be sorted ascending by [`Timed::time`].
/// `source_error` receives a market source failure, for the caller to check once the run ends.
fn merge_market_with_aux<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    market: St,
    seed: DateTime<Utc>,
    aux: Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
    source_error: Arc<OnceLock<BarterError>>,
) -> TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, MarketKind>, BarterError>>,
{
    TimedMergeStream {
        market: market.peekable(),
        aux: aux.into_iter().peekable(),
        last_market_time: seed,
        source_error,
        terminated: false,
    }
}

#[cfg(test)]
// Test code: panicking on a bad fixture is acceptable, and an `expect` message names which
// invariant the fixture violated.
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use futures::stream;
    use rustrade_data::{event::DataKind, subscription::trade::PublicTrade};
    use rustrade_instrument::exchange::ExchangeId;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    /// A `Timed` aux marker whose `ContractExpiry` instrument id identifies it in assertions.
    fn marker(id: usize, secs: i64) -> Timed<EngineEvent<DataKind>> {
        Timed::new(expiry(id), at(secs))
    }

    fn expiry(id: usize) -> EngineEvent<DataKind> {
        EngineEvent::ContractExpiry(InstrumentIndex::new(id))
    }

    /// A market `Item` at `secs`; its `instrument` id identifies it in assertions (it becomes a
    /// `MarketEvent` engine event after the merge, distinct from the `ContractExpiry` aux markers).
    fn trade_event(id: usize, secs: i64) -> MarketStreamEvent<InstrumentIndex, DataKind> {
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: at(secs),
            time_received: at(secs),
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(id),
            kind: DataKind::Trade(PublicTrade {
                id: "t".into(),
                price: Decimal::ONE,
                amount: Decimal::ONE,
                side: None,
            }),
        })
    }

    /// The `instrument` id of a `MarketEvent` engine event, or `None` for any other variant.
    fn market_id(event: &EngineEvent<DataKind>) -> Option<usize> {
        match event {
            EngineEvent::Market(MarketStreamEvent::Item(market_event)) => {
                Some(market_event.instrument.index())
            }
            _ => None,
        }
    }

    /// The instrument id of a `ContractExpiry` aux marker, or `None` for any other variant.
    fn expiry_id(event: &EngineEvent<DataKind>) -> Option<usize> {
        match event {
            EngineEvent::ContractExpiry(instrument) => Some(instrument.index()),
            _ => None,
        }
    }

    async fn merge(
        market: Vec<MarketStreamEvent<InstrumentIndex, DataKind>>,
        seed: DateTime<Utc>,
        aux: Vec<Timed<EngineEvent<DataKind>>>,
    ) -> Vec<EngineEvent<DataKind>> {
        let (merged, error) = merge_fallible(market.into_iter().map(Ok).collect(), seed, aux).await;
        assert!(
            error.is_none(),
            "infallible fixture must not record an error"
        );
        merged
    }

    /// Merge a market script that may contain failures, returning the recorded source error.
    async fn merge_fallible(
        market: Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>>,
        seed: DateTime<Utc>,
        aux: Vec<Timed<EngineEvent<DataKind>>>,
    ) -> (Vec<EngineEvent<DataKind>>, Option<BarterError>) {
        let source_error = Arc::new(OnceLock::new());
        // The merge's `Timed` wrapper is unwrapped here: these tests assert the *ordering* the
        // merge produces, which the sequence of values already expresses. `SimRunner`'s own tests
        // cover the published time.
        let merged =
            merge_market_with_aux(stream::iter(market), seed, aux, Arc::clone(&source_error))
                .map(|timed| timed.value)
                .collect::<Vec<_>>()
                .await;

        (merged, source_error.get().cloned())
    }

    #[tokio::test]
    async fn merge_interleaves_by_time_with_aux_first_on_ties() {
        // Market items at t=10 (id 0) and t=30 (id 1); aux markers at t=20 (id 100) and t=30 (id 101).
        let market = vec![trade_event(0, 10), trade_event(1, 30)];
        let aux = vec![marker(100, 20), marker(101, 30)];
        let merged = merge(market, at(0), aux).await;
        // aux (101) at t=30 must precede market (1) at the same t=30 (aux wins ties).
        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0), None, None, Some(1)]
        );
        assert_eq!(
            merged.iter().map(expiry_id).collect::<Vec<_>>(),
            vec![None, Some(100), Some(101), None]
        );
    }

    #[tokio::test]
    async fn merge_empty_aux_is_market_identity() {
        // The O(1)-memory fast path: market is forwarded verbatim.
        let market = vec![trade_event(0, 10), trade_event(1, 20)];
        let merged = merge(market, at(0), Vec::new()).await;
        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0), Some(1)]
        );
    }

    #[tokio::test]
    async fn merge_empty_market_yields_aux_in_order() {
        let aux = vec![marker(100, 5), marker(101, 6)];
        let merged = merge(Vec::new(), at(0), aux).await;
        assert_eq!(
            merged.iter().map(expiry_id).collect::<Vec<_>>(),
            vec![Some(100), Some(101)]
        );
    }

    #[tokio::test]
    async fn merge_both_empty_is_empty() {
        let merged = merge(Vec::new(), at(0), Vec::new()).await;
        assert!(merged.is_empty());
    }

    #[tokio::test]
    async fn merge_reconnecting_carries_prior_time_forward() {
        // Market: Item @10, Reconnecting (no timestamp, inherits 10), Item @30.
        let market = vec![
            trade_event(0, 10),
            MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot),
            trade_event(1, 30),
        ];
        // An aux marker at t=20 must order AFTER the Reconnecting (carried time 10 <= 20) and BEFORE
        // the t=30 item; this reveals the carried-forward time.
        let aux = vec![marker(100, 20)];
        let merged = merge(market, at(0), aux).await;
        let order: Vec<_> = merged
            .iter()
            .map(|event| match event {
                EngineEvent::Market(MarketStreamEvent::Item(m)) => {
                    format!("item{}", m.instrument.index())
                }
                EngineEvent::Market(MarketStreamEvent::Reconnecting(_)) => {
                    "reconnecting".to_string()
                }
                EngineEvent::ContractExpiry(instrument) => format!("aux{}", instrument.index()),
                _ => "other".to_string(),
            })
            .collect();
        assert_eq!(order, vec!["item0", "reconnecting", "aux100", "item1"]);
    }

    #[tokio::test]
    async fn merge_leading_reconnecting_uses_seed() {
        // A leading Reconnecting has no prior market time, so it inherits the seed. With the seed at
        // t=7 and an aux marker at t=5, the aux (5 <= 7) must lead the Reconnecting.
        let market = vec![MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)];
        let aux = vec![marker(100, 5)];
        let merged = merge(market, at(7), aux).await;
        let order: Vec<_> = merged
            .iter()
            .map(|event| match event {
                EngineEvent::Market(MarketStreamEvent::Reconnecting(_)) => "reconnecting",
                EngineEvent::ContractExpiry(_) => "aux",
                _ => "other",
            })
            .collect();
        assert_eq!(order, vec!["aux", "reconnecting"]);
    }

    fn source_failure() -> BarterError {
        BarterError::BacktestMarketData("truncated source".to_string())
    }

    #[tokio::test]
    async fn merge_records_source_error_and_stops_on_the_no_aux_fast_path() {
        let market = vec![
            Ok(trade_event(0, 10)),
            Err(source_failure()),
            Ok(trade_event(1, 30)),
        ];
        let (merged, error) = merge_fallible(market, at(0), vec![]).await;

        // Events before the failure are emitted; nothing after it is.
        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0)]
        );
        assert_eq!(error, Some(source_failure()));
    }

    /// A failed source ends the run, so pending aux events must not be drained afterwards — doing
    /// so would advance the timeline past data that was never read.
    #[tokio::test]
    async fn merge_source_error_drops_pending_aux_events() {
        let market = vec![Ok(trade_event(0, 10)), Err(source_failure())];
        let aux = vec![marker(100, 20), marker(101, 40)];
        let (merged, error) = merge_fallible(market, at(0), aux).await;

        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0)]
        );
        assert_eq!(merged.iter().filter_map(expiry_id).count(), 0);
        assert_eq!(error, Some(source_failure()));
    }

    /// Only the first error is retained — it is the one that stopped the run.
    #[tokio::test]
    async fn merge_keeps_the_first_source_error() {
        let market = vec![
            Err(source_failure()),
            Err(BarterError::BacktestMarketData("second".to_string())),
        ];
        let (merged, error) = merge_fallible(market, at(0), vec![]).await;

        assert!(merged.is_empty());
        assert_eq!(error, Some(source_failure()));
    }

    #[tokio::test]
    async fn merge_without_failure_records_no_error() {
        let market = vec![Ok(trade_event(0, 10)), Ok(trade_event(1, 30))];
        let (merged, error) = merge_fallible(market, at(0), vec![marker(100, 20)]).await;

        assert_eq!(merged.len(), 3);
        assert_eq!(error, None);
    }

    /// The release-build half of the sort obligation. `MarketDataInMemory` asserts sortedness in
    /// its constructor; a streamed source cannot be inspected without reading it, so the check has
    /// to ride the read — and a `debug_assert!` inside a merge helper the factory may not even use
    /// is compiled out of every build a real backtest runs in.
    #[tokio::test]
    async fn merge_aborts_on_a_market_event_that_moves_time_backwards() {
        let market = vec![
            Ok(trade_event(0, 30)),
            // Backwards. Emitting it would run the simulated clock in reverse, and every statistic
            // computed afterwards would be wrong with nothing downstream able to tell.
            Ok(trade_event(1, 10)),
            Ok(trade_event(2, 40)),
        ];
        let (merged, error) = merge_fallible(market, at(0), vec![]).await;

        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0)],
            "nothing at or after the violation may be emitted"
        );
        // Reported, not merely stopped: a truncated run that returns `Ok` is the silent-partial
        // -result failure this path exists to prevent.
        let error = error.expect("an out-of-order event must be recorded like a source failure");
        assert!(matches!(error, BarterError::BacktestMarketData(_)));
        // Both instants are named, so the offending event is identifiable without a re-run.
        let message = error.to_string();
        assert!(message.contains("1970-01-01 00:00:10"), "{message}");
        assert!(message.contains("1970-01-01 00:00:30"), "{message}");
    }

    /// Equal timestamps are the common case on a tick tape and on a k-way merge of several
    /// instruments; only a step backwards is a violation.
    #[tokio::test]
    async fn merge_accepts_repeated_timestamps() {
        let market = vec![
            Ok(trade_event(0, 10)),
            Ok(trade_event(1, 10)),
            Ok(trade_event(2, 10)),
        ];
        let (merged, error) = merge_fallible(market, at(0), vec![]).await;

        assert_eq!(
            merged.iter().map(market_id).collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2)]
        );
        assert_eq!(error, None);
    }

    /// The ordering check must not fire on the path that has no timestamp of its own.
    #[tokio::test]
    async fn merge_does_not_treat_reconnecting_as_out_of_order() {
        let market = vec![
            Ok(trade_event(0, 10)),
            Ok(MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)),
            Ok(trade_event(1, 30)),
        ];
        let (merged, error) = merge_fallible(market, at(0), vec![]).await;

        assert_eq!(merged.len(), 3);
        assert_eq!(error, None);
    }

    /// Termination is a latch, not a property of what the market side happened to return on one
    /// poll. Without it, the poll after an abort falls through to draining `aux` — emitting events
    /// at instants the data never reached, *after* the run was declared over.
    #[tokio::test]
    async fn merge_stays_ended_after_an_abort_rather_than_resuming_from_aux() {
        let source_error = Arc::new(OnceLock::new());
        let market = vec![Ok(trade_event(0, 10)), Err(source_failure())];
        let aux = vec![marker(100, 20), marker(101, 40)];
        let mut merged =
            merge_market_with_aux(stream::iter(market), at(0), aux, Arc::clone(&source_error));

        assert_eq!(
            merged
                .next()
                .await
                .map(|timed| timed.value)
                .as_ref()
                .and_then(market_id),
            Some(0)
        );
        assert!(merged.next().await.is_none(), "the error ends the stream");
        assert!(merged.is_terminated());

        // The aux markers at t=20 and t=40 are still buffered. Polling past the end must not reach
        // them.
        for _ in 0..3 {
            assert!(
                merged.next().await.is_none(),
                "polling past the end must stay `None`"
            );
            assert!(merged.is_terminated());
        }
    }

    /// The same latch on the ordinary exhaustion path, so `is_terminated` cannot report a state the
    /// stream does not actually hold.
    #[tokio::test]
    async fn merge_reports_termination_only_once_it_has_ended() {
        let source_error = Arc::new(OnceLock::new());
        let mut merged = merge_market_with_aux(
            stream::iter(vec![Ok(trade_event(0, 10))]),
            at(0),
            vec![marker(100, 20)],
            Arc::clone(&source_error),
        );

        assert!(!merged.is_terminated());
        assert_eq!(
            merged
                .next()
                .await
                .map(|timed| timed.value)
                .as_ref()
                .and_then(market_id),
            Some(0)
        );
        assert!(!merged.is_terminated());
        assert_eq!(
            merged
                .next()
                .await
                .map(|timed| timed.value)
                .as_ref()
                .and_then(expiry_id),
            Some(100)
        );
        // Still not terminated: nothing has returned `None` yet, and claiming otherwise would make
        // a `select!` drop the last event.
        assert!(!merged.is_terminated());
        assert!(merged.next().await.is_none());
        assert!(merged.is_terminated());
    }
}
