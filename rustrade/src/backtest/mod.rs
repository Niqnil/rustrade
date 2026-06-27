use crate::{EngineEvent, Timed};
/// Backtesting utilities for algorithmic trading strategies.
///
/// This module provides tools for running historical simulations of trading strategies
/// using market data, and analyzing the performance of these simulations.
use crate::{
    backtest::{
        aux_events::{AuxEventSource, NoAuxEvents},
        market_data::BacktestMarketData,
        summary::{BacktestSummary, MultiBacktestSummary},
    },
    engine::{
        Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        state::{EngineState, instrument::data::InstrumentDataState},
    },
    error::BarterError,
    risk::RiskManager,
    statistic::time::TimeInterval,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::{builder::EngineFeedMode, config::ExecutionConfig},
};
use crate::{
    engine::Engine,
    execution::builder::{ExecutionBuild, ExecutionBuilder},
    system::builder::{AuditMode, SystemBuild},
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, future::try_join_all};
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
    sync::Arc,
    task::{Context, Poll},
};

/// Defines the [`AuxEventSource`](aux_events::AuxEventSource) interface for interleaving non-market
/// `EngineEvent`s (e.g. corporate actions, contract expiries) into a backtest in simulated-time
/// order.
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
    /// EngineState.
    pub engine_state: State,
    /// Source of auxiliary (non-market) `EngineEvent`s to interleave with the market data in
    /// simulated-time order (e.g. corporate actions, contract expiries).
    ///
    /// Defaults to [`NoAuxEvents`] (yields nothing), so existing backtests opt out at zero cost.
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

    let backtest_futures = args_dynamic_iter
        .into_iter()
        .map(|args_dynamic| backtest(Arc::clone(&args_constant), args_dynamic));

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
/// # What the returned summary does and does not contain
/// This function returns only a [`BacktestSummary`] whose `trading_summary` aggregates statistics
/// derived from **closed** positions (PnL, returns, drawdown per `TearSheet`). It does **not** expose
/// final engine/position state: a position left **open** at the end of the run — e.g. one that took a
/// stock split but no subsequent closing fill — contributes nothing to `trading_summary`, and a
/// notional-preserving split moves no aggregate metric on its own. Callers needing to inspect
/// post-run positions (or assert that a corporate action mutated a position) must drive their own
/// engine harness; the split *economics* are asserted at the `Engine::process_with_audit` seam (see
/// the `test_corporate_action_*` tests), which this path cannot reach because it hardcodes
/// [`AuditMode::Disabled`] — the per-event `EngineOutput` stream is therefore not observable here.
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
) -> Result<BacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
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
    // collected — so peak memory is O(1) in the dataset size, and the common no-aux case streams
    // exactly as a pre-corporate-action backtest did. Merging into one stream — rather than
    // forwarding market and aux as two producers into the engine feed — is what preserves the time
    // order (two `forward_to` tasks would interleave non-deterministically). See [`AuxEventSource`].
    let market_first = args_constant.market_data.time_first_event().await?;
    let raw_market = args_constant.market_data.stream().await?;
    // The aux side is tiny (corporate actions / expiries number in the handful), so collecting it is
    // cheap and lets the merge peek it synchronously.
    let aux = args_constant.aux_events.aux_events().collect::<Vec<_>>();

    // Seed the clock from the earliest of the first market event and the first aux event, so an aux
    // event scheduled before the first market tick still orders and stamps correctly.
    let clock_start = aux
        .first()
        .map_or(market_first, |first| market_first.min(first.time));
    let clock = HistoricalClock::new(clock_start);

    // Build Execution infrastructure
    let ExecutionBuild {
        execution_tx_map,
        account_channel,
        futures,
    } = args_constant
        .executions
        .clone()
        .into_iter()
        .try_fold(
            ExecutionBuilder::new(&args_constant.instruments),
            |builder, config| match config {
                ExecutionConfig::Mock(mock_config) => builder.add_mock(mock_config, clock.clone()),
            },
        )?
        .build();

    let engine = Engine::new(
        clock,
        args_constant.engine_state.clone(),
        execution_tx_map,
        args_dynamic.strategy,
        args_dynamic.risk,
    );

    // Drive the engine from the single lazily-merged time-ordered stream. Its item is `EngineEvent`,
    // which the engine's feed accepts directly (`Event: From<MarketStream::Item>` is satisfied
    // reflexively), so it flows through `SystemBuild`'s existing single market-forwarding task and
    // `shutdown_after_backtest` needs no change.
    let market_stream = merge_market_with_aux(raw_market, market_first, aux);

    let system = SystemBuild::new(
        engine,
        EngineFeedMode::Stream,
        AuditMode::Disabled,
        market_stream,
        account_channel,
        futures,
    )
    .init()
    .await?;

    let (engine, _shutdown_audit) = system.shutdown_after_backtest().await?;

    let trading_summary = engine
        .trading_summary_generator(args_dynamic.risk_free_return)
        .generate(args_constant.summary_interval);

    Ok(BacktestSummary {
        id: args_dynamic.id,
        risk_free_return: args_dynamic.risk_free_return,
        trading_summary,
    })
}

/// A lazy, time-ordered two-way merge of a backtest market stream and pre-collected auxiliary
/// (non-market) events.
///
/// The market side stays **lazy** — it is polled on demand and never collected, so peak memory is
/// O(1) in the dataset size and the common no-aux case streams exactly as a pre-corporate-action
/// backtest did. The aux side is tiny (corporate actions / expiries number in the handful), so it is
/// held as a `Peekable` iterator and peeked synchronously to decide ordering.
///
/// # Ordering contract
/// - `aux` MUST be sorted ascending by [`Timed::time`] (the [`AuxEventSource`] obligation); the
///   market stream is assumed time-sorted by `time_exchange`.
/// - Aux events win ties (`aux.time <= market.time`), so an injected event at the same instant as a
///   market event is processed first — e.g. a stock split adjusts positions before any fill stamped
///   at that instant.
/// - A [`MarketStreamEvent::Reconnecting`] carries no timestamp; its ordering inherits the prior
///   market event's `time_exchange` (`last_market_time`), falling back to the seed only if it leads.
///   For an in-memory backtest no `Reconnecting` events occur, so the carry-forward is purely
///   defensive.
///
/// The yielded item is a bare [`EngineEvent`]; the engine reads simulated time itself (via the
/// market event's exchange timestamp), so the ordering time is internal to this merge.
#[pin_project::pin_project]
struct TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    #[pin]
    market: futures::stream::Peekable<St>,
    aux: std::iter::Peekable<
        std::vec::IntoIter<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
    >,
    last_market_time: DateTime<Utc>,
}

impl<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey> Stream
    for TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = MarketStreamEvent<InstrumentKey, MarketKind>>,
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    type Item = EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // No aux remaining: forward the market side verbatim (the O(1)-memory fast path).
        let Some(aux_time) = this.aux.peek().map(|timed| timed.time) else {
            return this
                .market
                .as_mut()
                .poll_next(cx)
                .map(|opt| opt.map(|event| convert_market(event, this.last_market_time)));
        };

        // Aux has an event: peek the market's next event to order them.
        match this.market.as_mut().poll_peek(cx) {
            // Can't decide ordering until the market's next event (or its end) is known.
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(market_next)) => {
                // Copy the Copy `DateTime` out so the peek borrow of `this.market` ends here,
                // freeing `this.market` to be re-polled in the `else` arm below.
                let market_time = match market_next {
                    MarketStreamEvent::Item(market_event) => market_event.time_exchange,
                    MarketStreamEvent::Reconnecting(_) => *this.last_market_time,
                };
                if aux_time <= market_time {
                    // Aux leads or ties — emit it. `aux.peek()` was `Some`, so `next()` is `Some`.
                    Poll::Ready(this.aux.next().map(|timed| timed.value))
                } else {
                    this.market
                        .as_mut()
                        .poll_next(cx)
                        .map(|opt| opt.map(|event| convert_market(event, this.last_market_time)))
                }
            }
            // Market exhausted — drain the remaining aux events in order.
            Poll::Ready(None) => Poll::Ready(this.aux.next().map(|timed| timed.value)),
        }
    }
}

/// Convert a market event to an [`EngineEvent`], advancing `last_market_time` on an `Item` so a later
/// [`MarketStreamEvent::Reconnecting`] (which has no timestamp) can inherit the prior time for
/// ordering.
fn convert_market<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    event: MarketStreamEvent<InstrumentKey, MarketKind>,
    last_market_time: &mut DateTime<Utc>,
) -> EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    if let MarketStreamEvent::Item(market_event) = &event {
        *last_market_time = market_event.time_exchange;
    }
    EngineEvent::from(event)
}

/// Build a [`TimedMergeStream`]. `seed` is the fallback ordering time for a leading
/// [`MarketStreamEvent::Reconnecting`]; `aux` MUST be sorted ascending by [`Timed::time`].
fn merge_market_with_aux<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    market: St,
    seed: DateTime<Utc>,
    aux: Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
) -> TimedMergeStream<St, MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    St: Stream<Item = MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    TimedMergeStream {
        market: market.peekable(),
        aux: aux.into_iter().peekable(),
        last_market_time: seed,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
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
        merge_market_with_aux(stream::iter(market), seed, aux)
            .collect::<Vec<_>>()
            .await
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
}
