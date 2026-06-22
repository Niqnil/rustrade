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
use futures::{StreamExt, future::try_join_all, stream};
use rust_decimal::Decimal;
use rustrade_data::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use rustrade_execution::AccountEvent;
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use smol_str::SmolStr;
use std::{fmt::Debug, sync::Arc};

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
    // Collect the market stream and pre-merge it with the auxiliary (non-market) events into a
    // single time-ordered stream BEFORE the engine channel, so an injected event (e.g. a corporate
    // action) is processed at the correct point in simulated time. Merging into one stream — rather
    // than forwarding market and aux as two producers into the engine feed — is what preserves the
    // time order (two `forward_to` tasks would interleave non-deterministically). See
    // [`AuxEventSource`].
    let market_first = args_constant.market_data.time_first_event().await?;
    let raw_market = args_constant
        .market_data
        .stream()
        .await?
        .collect::<Vec<_>>()
        .await;
    let market = market_events_to_timed(raw_market, market_first);
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

    // Drive the engine from the single pre-merged time-ordered stream. Its item is `EngineEvent`,
    // which the engine's feed accepts directly (`Event: From<MarketStream::Item>` is satisfied
    // reflexively), so it flows through `SystemBuild`'s existing single market-forwarding task and
    // `shutdown_after_backtest` needs no change.
    let market_stream = stream::iter(merge_timed(market, aux));

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

/// Map time-sorted market events to `Timed<EngineEvent>`, preserving order.
///
/// A [`MarketStreamEvent::Reconnecting`] carries no timestamp; its position is preserved by
/// carrying the previous event's `time_exchange` forward (`seed` is used only if the very first
/// event is a `Reconnecting`). For an in-memory backtest no `Reconnecting` events occur, so the
/// carry-forward is purely defensive.
fn market_events_to_timed<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    events: Vec<MarketStreamEvent<InstrumentKey, MarketKind>>,
    seed: DateTime<Utc>,
) -> Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>
where
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>:
        From<MarketStreamEvent<InstrumentKey, MarketKind>>,
{
    let mut last_time = seed;
    events
        .into_iter()
        .map(|event| {
            let time = match &event {
                MarketStreamEvent::Item(market_event) => {
                    last_time = market_event.time_exchange;
                    market_event.time_exchange
                }
                MarketStreamEvent::Reconnecting(_) => last_time,
            };
            Timed::new(EngineEvent::from(event), time)
        })
        .collect()
}

/// Two-way merge of two `time`-sorted `Vec<Timed<EngineEvent>>` into one time-ordered
/// `Vec<EngineEvent>`.
///
/// Both inputs are assumed sorted ascending by [`Timed::time`] (an O(N+M) merge). Aux events win
/// ties (`aux.time <= market.time`), so an injected event at the same instant as a market event is
/// processed first — e.g. a stock split adjusts positions before any fill stamped at that instant.
fn merge_timed<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    market: Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
    aux: Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>,
) -> Vec<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>> {
    let mut merged = Vec::with_capacity(market.len() + aux.len());
    let mut market = market.into_iter().peekable();
    let mut aux = aux.into_iter().peekable();
    loop {
        // Take from aux when it leads or ties (aux wins ties); otherwise from market. Each branch
        // is only reached after the corresponding `peek()` returned `Some`, so `next()` is
        // guaranteed `Some` — the `if let` is a total match, never a silent drop.
        let next = match (aux.peek(), market.peek()) {
            (Some(a), Some(m)) if a.time <= m.time => aux.next(),
            (Some(_), Some(_)) => market.next(),
            (Some(_), None) => aux.next(),
            (None, Some(_)) => market.next(),
            (None, None) => break,
        };
        if let Some(timed) = next {
            merged.push(timed.value);
        }
    }
    merged
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use rustrade_data::{event::DataKind, subscription::trade::PublicTrade};
    use rustrade_instrument::exchange::ExchangeId;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    /// A `Timed` marker whose `ContractExpiry` instrument id identifies it in assertions.
    fn marker(id: usize, secs: i64) -> Timed<EngineEvent<DataKind>> {
        Timed::new(expiry(id), at(secs))
    }

    fn expiry(id: usize) -> EngineEvent<DataKind> {
        EngineEvent::ContractExpiry(InstrumentIndex::new(id))
    }

    fn trade_event(secs: i64) -> MarketStreamEvent<InstrumentIndex, DataKind> {
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: at(secs),
            time_received: at(secs),
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(0),
            kind: DataKind::Trade(PublicTrade {
                id: "t".into(),
                price: Decimal::ONE,
                amount: Decimal::ONE,
                side: None,
            }),
        })
    }

    #[test]
    fn merge_interleaves_by_time_with_aux_first_on_ties() {
        let market = vec![marker(0, 10), marker(1, 30)];
        let aux = vec![marker(100, 20), marker(101, 30)];
        // aux (101) at t=30 must precede market (1) at the same t=30.
        assert_eq!(
            merge_timed(market, aux),
            vec![expiry(0), expiry(100), expiry(101), expiry(1)]
        );
    }

    #[test]
    fn merge_empty_aux_is_market_identity() {
        let market = vec![marker(0, 10), marker(1, 20)];
        assert_eq!(merge_timed(market, Vec::new()), vec![expiry(0), expiry(1)]);
    }

    #[test]
    fn merge_empty_market_yields_aux_in_order() {
        let aux = vec![marker(100, 5), marker(101, 6)];
        assert_eq!(merge_timed(Vec::new(), aux), vec![expiry(100), expiry(101)]);
    }

    #[test]
    fn merge_both_empty_is_empty() {
        let merged: Vec<EngineEvent<DataKind>> = merge_timed(Vec::new(), Vec::new());
        assert!(merged.is_empty());
    }

    #[test]
    fn market_events_to_timed_stamps_time_exchange_and_carries_forward_reconnecting() {
        let events = vec![
            trade_event(10),
            MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot),
            trade_event(30),
        ];
        let timed: Vec<Timed<EngineEvent<DataKind>>> = market_events_to_timed(events, at(1));
        let times: Vec<i64> = timed.iter().map(|timed| timed.time.timestamp()).collect();
        // Item @10, Reconnecting carries the prior 10 forward, Item @30.
        assert_eq!(times, vec![10, 10, 30]);
    }

    #[test]
    fn market_events_to_timed_uses_seed_for_leading_reconnecting() {
        let timed = market_events_to_timed::<DataKind, ExchangeIndex, AssetIndex, InstrumentIndex>(
            vec![MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)],
            at(7),
        );
        assert_eq!(timed.len(), 1);
        assert_eq!(timed[0].time, at(7));
    }
}
