#![allow(clippy::unwrap_used, clippy::expect_used)] // Benchmark code: panics acceptable

use chrono::{DateTime, Utc};
use criterion::{Criterion, Throughput};
use futures::StreamExt;
use rust_decimal::Decimal;
use rustrade::{
    backtest,
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        market_data::{BacktestMarketData, MarketDataInMemory},
        summary::BacktestSummary,
    },
    engine::{
        Engine, Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            order::in_flight_recorder::InFlightRequestRecorder,
            trading::TradingState,
        },
    },
    error::BarterError,
    execution::builder::{ExecutionBuild, ExecutionBuilder},
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::{
        algo::AlgoStrategy,
        close_positions::{ClosePositionsStrategy, close_open_positions_with_market_orders},
        on_disconnect::OnDisconnectStrategy,
        on_trading_disabled::OnTradingDisabled,
    },
    system::{
        builder::{AuditMode, EngineFeedMode, SystemBuild},
        config::{ExecutionConfig, InstrumentConfig, SystemConfig},
    },
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountEvent,
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
    },
};
use rustrade_instrument::{
    Side,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
    sync::Arc,
};

criterion::criterion_main!(benchmark_backtest);

// Config containing max balances to enable spamming open order requests
const CONFIG: &str = r#"
{
  "risk_free_return": 0.05,
  "system": {
    "executions": [
      {
        "mocked_exchange": "binance_spot",
        "latency_ms": 100,
        "fee_model": { "Percentage": { "rate": "0.05" } },
        "initial_state": {
          "exchange": "binance_spot",
          "balances": [
            {
              "asset": "usdt",
              "balance": {
                "total": 99999999999999,
                "free": 99999999999999
              },
              "time_exchange": "2025-03-24T21:30:00Z"
            },
            {
              "asset": "btc",
              "balance": {
                "total": 99999999999999,
                "free": 99999999999999
              },
              "time_exchange": "2025-03-24T21:30:00Z"
            },
            {
              "asset": "eth",
              "balance": {
                "total": 99999999999999,
                "free": 99999999999999
              },
              "time_exchange": "2025-03-24T21:30:00Z"
            },
            {
              "asset": "sol",
              "balance": {
                "total": 99999999999999,
                "free": 99999999999999
              },
              "time_exchange": "2025-03-24T21:30:00Z"
            }
          ],
          "instruments": [
            {
              "instrument": "BTCUSDT",
              "orders": []
            },
            {
              "instrument": "ETHUSDT",
              "orders": []
            },
            {
              "instrument": "SOLUSDT",
              "orders": []
            }
          ]
        }
      }
    ],
    "instruments": [
      {
        "exchange": "binance_spot",
        "name_exchange": "BTCUSDT",
        "underlying": {
          "base": "btc",
          "quote": "usdt"
        },
        "quote": "underlying_quote",
        "kind": "spot"
      },
      {
        "exchange": "binance_spot",
        "name_exchange": "ETHUSDT",
        "underlying": {
          "base": "eth",
          "quote": "usdt"
        },
        "quote": "underlying_quote",
        "kind": "spot"
      },
      {
        "exchange": "binance_spot",
        "name_exchange": "SOLUSDT",
        "underlying": {
          "base": "sol",
          "quote": "usdt"
        },
        "quote": "underlying_quote",
        "kind": "spot"
      }
    ]
  }
}
"#;

const FILE_PATH_MARKET_DATA_INDEXED: &str =
    "examples/data/binance_spot_trades_l1_btcusdt_ethusdt_solusdt.json";

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

fn benchmark_backtest() {
    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = serde_json::from_str(CONFIG).unwrap();

    let args_constant = args_constant(instruments, executions);
    let args_dynamic = args_dynamic(risk_free_return);

    // `configure_from_args` lets callers target a single group/case (e.g. `-- "AuxSeam"`) and use
    // criterion's `--save-baseline` / `--baseline` regression workflow; with no args it runs all.
    let mut c = Criterion::default().without_plots().configure_from_args();

    bench_backtest(&mut c, Arc::clone(&args_constant), &args_dynamic);
    bench_backtests_concurrent(&mut c, args_constant, args_dynamic);
    bench_aux_seam(&mut c);
    bench_audit_seam(&mut c);
}

fn bench_backtest(
    c: &mut Criterion,
    args_constant: Arc<
        BacktestArgsConstant<
            MarketDataInMemory<DataKind>,
            Daily,
            EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
        >,
    >,
    args_dynamic: &BacktestArgsDynamic<
        LoseMoneyStrategy,
        DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
    >,
) {
    let mut group = c.benchmark_group("Backtest");
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(50);
    group.throughput(Throughput::Elements(1));

    group.bench_function("Single", |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || (Arc::clone(&args_constant), args_dynamic.clone()),
            |(constant, dynamic)| {
                rt.block_on(
                    async move { backtest::backtest(constant, dynamic).await.unwrap().summary },
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_backtests_concurrent(
    c: &mut Criterion,
    args_constant: Arc<
        BacktestArgsConstant<
            MarketDataInMemory<DataKind>,
            Daily,
            EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
        >,
    >,
    args_dynamic: BacktestArgsDynamic<
        LoseMoneyStrategy,
        DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
    >,
) {
    let bench_func = |b: &mut criterion::Bencher, num_concurrent| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || {
                let dynamics = (0..num_concurrent)
                    .map(|_| args_dynamic.clone())
                    .collect::<Vec<_>>();

                (Arc::clone(&args_constant), dynamics)
            },
            |(constant, dynamics)| {
                rt.block_on(async move {
                    backtest::run_backtests(constant, dynamics).await.unwrap();
                });
            },
            criterion::BatchSize::SmallInput,
        );
    };

    // 10 concurrent backtests
    let mut group = c.benchmark_group("Backtest Concurrent");
    group.throughput(Throughput::Elements(10));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(15));
    group.sample_size(50);
    group.bench_function("10", |b| bench_func(b, 10));
    group.finish();

    // 500 concurrent backtests
    let mut group = c.benchmark_group("Backtest Concurrent");
    group.throughput(Throughput::Elements(500));
    group.warm_up_time(std::time::Duration::from_secs(10));
    group.measurement_time(std::time::Duration::from_secs(120));
    group.sample_size(10);
    group.bench_function("500", |b| bench_func(b, 500));
    group.finish();
}

// ---------------------------------------------------------------------------------------------
// Aux-event seam A/B benchmark (issue #179)
//
// `backtest()` always wraps the market stream in `TimedMergeStream` to interleave auxiliary
// (non-market) events in simulated-time order, even when there are none (`NoAuxEvents`). This group
// measures the per-event overhead of that seam by comparing two throughput cases over the same
// fixture:
//   - `NoAuxEvents` — the shipped `backtest()` path (market stream flows through the merge).
//   - `MarketOnly`  — the pre-corporate-action baseline: the raw market stream fed straight to the
//                     engine, no merge (see `backtest_market_only`, a faithful copy of the pre-seam
//                     `backtest()` body).
// The delta between the two events/sec figures IS the seam cost, guarding the "negligible overhead"
// claim against regression. Both cases use a genuinely no-op strategy so that per-tick order
// generation / `MockExchange` task spawns (which dwarf the seam by orders of magnitude) don't drown
// the signal. Compare across revisions with `--save-baseline` / `--baseline`.
// ---------------------------------------------------------------------------------------------

/// [`EngineState`] used by the aux-seam A/B — the library's `DefaultInstrumentMarketData` (already
/// implements every required trait) rather than a bespoke instrument-data type.
type SeamState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
/// Shared constants for the aux-seam A/B. `AuxEvents` defaults to [`NoAuxEvents`].
type SeamConstant = BacktestArgsConstant<MarketDataInMemory<DataKind>, Daily, SeamState>;
/// Per-run variables for the aux-seam A/B (no-op strategy + default risk).
type SeamDynamic = BacktestArgsDynamic<NoOpStrategy, DefaultRiskManager<SeamState>>;

fn bench_aux_seam(c: &mut Criterion) {
    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = serde_json::from_str(CONFIG).unwrap();

    let (args_constant, market_events) = args_constant_seam(instruments, executions);
    let num_events = market_events.len();
    let args_dynamic = args_dynamic_seam(risk_free_return);

    let mut group = c.benchmark_group("Backtest AuxSeam");
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(50);
    // Each iteration drives the whole fixture once, so reporting events/sec makes the per-event seam
    // overhead directly visible as the throughput delta between the two cases below.
    group.throughput(Throughput::Elements(num_events as u64));

    // A — shipped path: `NoAuxEvents` still routes the market stream through `TimedMergeStream`.
    group.bench_function("NoAuxEvents", |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || (Arc::clone(&args_constant), args_dynamic.clone()),
            |(constant, dynamic)| {
                rt.block_on(
                    async move { backtest::backtest(constant, dynamic).await.unwrap().summary },
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // B — pre-seam baseline: raw market stream fed straight to the engine, no merge.
    group.bench_function("MarketOnly", |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || {
                (
                    Arc::clone(&args_constant),
                    Arc::clone(&market_events),
                    args_dynamic.clone(),
                )
            },
            |(constant, events, dynamic)| {
                rt.block_on(async move {
                    backtest_market_only(constant, events, dynamic, AuditMode::Disabled)
                        .await
                        .unwrap()
                })
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------------------------
// Audit-stream A/B benchmark
//
// The engine builds a full `AuditTick` on **every** processed event regardless of audit mode — both
// `async_run` and `async_run_with_audit` call `process_with_audit`. The only per-event work the audit
// stream adds is the `audit_tx.send(audit)` that moves that tick into the audit channel. This group
// isolates that send cost by running the same fixture + no-op strategy twice, differing ONLY in
// `AuditMode`:
//   - `AuditDisabled` — `async_run`: builds each tick, drops it (no channel).
//   - `AuditEnabled`  — `async_run_with_audit`: builds each tick AND sends it to a drained consumer.
// The delta between the two events/sec figures IS the per-event audit-send cost. That cost scales with
// the `AuditTick` size, so it is the wall-clock counterpart to the `size_of` guards in the engine
// tests (root-boxing the order payloads shrank the tick this path moves). Compare across revisions
// with `--save-baseline` / `--baseline`.
//
// Caveat: both cases run on a `new_current_thread()` runtime, so in `AuditEnabled` the per-event
// `audit_tx.send(audit)` producer and the spawned drain contend for the *same* thread. That folds
// single-thread producer/drain scheduling overhead into the measured delta — overhead not present in
// a multi-threaded production deployment — so this A/B likely *over*-states the send cost. That makes
// it a conservative regression guard (a real regression can only be larger than measured), not an
// absolute production figure; profile on a multi-thread runtime if the absolute cost matters.
// ---------------------------------------------------------------------------------------------
fn bench_audit_seam(c: &mut Criterion) {
    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = serde_json::from_str(CONFIG).unwrap();

    let (args_constant, market_events) = args_constant_seam(instruments, executions);
    let num_events = market_events.len();
    let args_dynamic = args_dynamic_seam(risk_free_return);

    let mut group = c.benchmark_group("Backtest AuditSeam");
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(50);
    // Each iteration drives the whole fixture once, so events/sec makes the per-event audit-send cost
    // directly visible as the throughput delta between the two cases.
    group.throughput(Throughput::Elements(num_events as u64));

    // A — audit stream off: the engine builds each tick and drops it.
    group.bench_function("AuditDisabled", |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || {
                (
                    Arc::clone(&args_constant),
                    Arc::clone(&market_events),
                    args_dynamic.clone(),
                )
            },
            |(constant, events, dynamic)| {
                rt.block_on(async move {
                    backtest_market_only(constant, events, dynamic, AuditMode::Disabled)
                        .await
                        .unwrap()
                })
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // B — audit stream on: each tick is additionally sent to (and drained by) a live receiver.
    group.bench_function("AuditEnabled", |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        b.iter_batched(
            || {
                (
                    Arc::clone(&args_constant),
                    Arc::clone(&market_events),
                    args_dynamic.clone(),
                )
            },
            |(constant, events, dynamic)| {
                rt.block_on(async move {
                    backtest_market_only(constant, events, dynamic, AuditMode::Enabled)
                        .await
                        .unwrap()
                })
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Pre-corporate-action ("pre-seam") backtest baseline: mirrors [`backtest::backtest`] exactly except
/// it feeds the raw market stream straight to the engine, bypassing the `TimedMergeStream` aux merge.
///
/// This is a copy of the pre-seam `backtest()` body (git `6824b47^`), rebuilt here purely from the
/// public API so the A/B needs no library changes. `EngineEvent: From<MarketStreamEvent>` is
/// derived, so `SystemBuild` accepts the raw `MarketStreamEvent` stream via its
/// `Event: From<MarketStream::Item>` bound.
///
/// # Why the baseline builds its own stream instead of calling `market_data.stream()`
/// [`BacktestMarketData::stream`] is fallible, and production unwraps each `Result` in an inline
/// `match` inside `TimedMergeStream::poll_next` — the very thing under measurement. Reaching for
/// `.filter_map(|event| ready(event.ok()))` here would put a combinator layer plus a `Ready` future
/// per event into the *baseline*, so arm B would carry work arm A does not and the seam cost this
/// group exists to guard would be understated, possibly to zero. `market_events` is therefore the
/// same `Arc<Vec<_>>` the fixture handed [`MarketDataInMemory`], replayed through the same
/// lazy-clone iterator that type uses — minus the `Ok` wrapper it only adds to satisfy the trait.
/// The merge is then the single difference between the arms.
///
/// **Absolute** figures still are not comparable against `--save-baseline` snapshots taken before
/// the market stream became fallible. Re-baseline rather than comparing across it.
///
/// `audit_mode` parameterises this baseline for two A/Bs: the aux-seam group always passes
/// [`AuditMode::Disabled`], while [`bench_audit_seam`] runs it at both modes so the throughput delta
/// isolates the per-event `audit_tx.send(audit)` cost. When enabled, the audit stream is drained
/// concurrently (see below), so the send actually enqueues to and is consumed by a live receiver.
async fn backtest_market_only(
    args_constant: Arc<SeamConstant>,
    market_events: Arc<Vec<MarketStreamEvent<InstrumentIndex, DataKind>>>,
    args_dynamic: SeamDynamic,
    audit_mode: AuditMode,
) -> Result<BacktestSummary<Daily>, BarterError> {
    let market_first = args_constant.market_data.time_first_event().await?;
    let raw_market = futures::stream::iter(
        (0..market_events.len()).map(move |index| market_events[index].clone()),
    );
    let clock = HistoricalClock::new(market_first);

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

    // No merge — the raw market stream is fed directly (the pre-seam path this baseline measures).
    let mut system = SystemBuild::new(
        engine,
        EngineFeedMode::Stream,
        audit_mode,
        raw_market,
        account_channel,
        futures,
    )
    .init()
    .await?;

    // With the audit stream enabled, drain it concurrently on the runtime so each per-event
    // `audit_tx.send(audit)` actually enqueues to (and is consumed by) a live receiver. Without a
    // drain, `shutdown_after_backtest` drops the audit receiver up front and every send early-returns
    // a no-op — hiding the very send cost the audit A/B measures. `take_audit()` returns `None` under
    // `AuditMode::Disabled`, so the disabled path is byte-for-byte the pre-seam baseline.
    let audit_drain = system.take_audit().map(|audit| {
        tokio::spawn(async move {
            let mut updates = audit.updates.into_stream();
            let mut count: u64 = 0;
            while updates.next().await.is_some() {
                count += 1;
            }
            count
        })
    });

    let (engine, _shutdown_audit) = system.shutdown_after_backtest().await?;

    if let Some(handle) = audit_drain {
        // Join the drain so its consume-side cost is folded into the measured wall-clock. Surface a
        // drain-task panic instead of swallowing it — a dead drain would silently undercount the
        // per-event audit-send cost this A/B exists to measure.
        handle.await.expect("audit drain task panicked");
    }

    // Mirror the seam case's downstream work so the A/B isolates the merge, not summary generation.
    let trading_summary = engine
        .trading_summary_generator(args_dynamic.risk_free_return)
        .generate(args_constant.summary_interval);

    Ok(BacktestSummary {
        id: args_dynamic.id,
        risk_free_return: args_dynamic.risk_free_return,
        trading_summary,
    })
}

/// Zero-work strategy for the aux-seam A/B: emits no orders on any event, so the benchmark measures
/// the per-event stream/engine path (where the seam sits) instead of order-generation +
/// `MockExchange` latency-task overhead.
#[derive(Debug, Clone, Default)]
struct NoOpStrategy;

impl AlgoStrategy for NoOpStrategy {
    type State = SeamState;

    fn generate_algo_orders(
        &self,
        _state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        (std::iter::empty(), std::iter::empty())
    }
}

impl ClosePositionsStrategy for NoOpStrategy {
    type State = SeamState;

    fn close_positions_requests<'a>(
        &'a self,
        _state: &'a Self::State,
        _filter: &'a InstrumentFilter,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        (std::iter::empty(), std::iter::empty())
    }
}

impl
    OnDisconnectStrategy<
        HistoricalClock,
        SeamState,
        MultiExchangeTxMap,
        DefaultRiskManager<SeamState>,
    > for NoOpStrategy
{
    type OnDisconnect = ();

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            SeamState,
            MultiExchangeTxMap,
            Self,
            DefaultRiskManager<SeamState>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
    }
}

impl
    OnTradingDisabled<HistoricalClock, SeamState, MultiExchangeTxMap, DefaultRiskManager<SeamState>>
    for NoOpStrategy
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            SeamState,
            MultiExchangeTxMap,
            Self,
            DefaultRiskManager<SeamState>,
        >,
    ) -> Self::OnTradingDisabled {
    }
}

/// Build the shared constants for the aux-seam A/B, returning the market events themselves so each
/// group can report events/sec and so [`backtest_market_only`] can replay the *same* fixture without
/// going through the fallible [`BacktestMarketData`] stream.
fn args_constant_seam(
    instruments: Vec<InstrumentConfig>,
    executions: Vec<ExecutionConfig>,
) -> (
    Arc<SeamConstant>,
    Arc<Vec<MarketStreamEvent<InstrumentIndex, DataKind>>>,
) {
    let instruments = IndexedInstruments::new(instruments);

    let market_events = Arc::new(market_data_from_file(FILE_PATH_MARKET_DATA_INDEXED));
    let market_data = MarketDataInMemory::new(Arc::clone(&market_events));
    let time_engine_start = DateTime::<Utc>::from_str("2025-03-25T23:07:00.773674205Z").unwrap();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    (
        Arc::new(BacktestArgsConstant {
            instruments,
            executions,
            market_data,
            summary_interval: Daily,
            engine_state,
            aux_events: NoAuxEvents,
        }),
        market_events,
    )
}

fn args_dynamic_seam(risk_free_return: Decimal) -> SeamDynamic {
    BacktestArgsDynamic {
        id: SmolStr::new("benches/backtest/aux_seam"),
        risk_free_return,
        strategy: NoOpStrategy,
        risk: DefaultRiskManager::default(),
    }
}

#[derive(Debug, Clone)]
struct LoseMoneyStrategy {
    pub id: StrategyId,
}

impl Default for LoseMoneyStrategy {
    fn default() -> Self {
        Self {
            id: StrategyId::new("LoseMoneyStrategy"),
        }
    }
}

impl AlgoStrategy for LoseMoneyStrategy {
    type State = EngineState<DefaultGlobalData, LoseMoneyInstrumentData>;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let opens = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .filter_map(|state| {
                let trade_not_sent_as_order_open = state.data.last_trade.as_ref()?;

                Some(OrderRequestOpen {
                    key: OrderKey {
                        exchange: state.instrument.exchange,
                        instrument: state.key,
                        strategy: self.id.clone(),
                        cid: ClientOrderId::random(),
                    },
                    state: RequestOpen {
                        side: Side::Buy,
                        price: None, // Market orders don't have a limit price
                        quantity: trade_not_sent_as_order_open.amount,
                        kind: OrderKind::Market,
                        time_in_force: TimeInForce::ImmediateOrCancel,
                        position_id: None,
                        reduce_only: false,
                        market: None,
                    },
                })
            });

        (std::iter::empty(), opens)
    }
}

impl ClosePositionsStrategy for LoseMoneyStrategy {
    type State = EngineState<DefaultGlobalData, LoseMoneyInstrumentData>;

    fn close_positions_requests<'a>(
        &'a self,
        state: &'a Self::State,
        filter: &'a InstrumentFilter,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        close_open_positions_with_market_orders(&self.id, state, filter, |_, _| {
            ClientOrderId::random()
        })
    }
}

impl
    OnDisconnectStrategy<
        HistoricalClock,
        EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
        MultiExchangeTxMap,
        DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
    > for LoseMoneyStrategy
{
    type OnDisconnect = ();

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
            MultiExchangeTxMap,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
    }
}

impl
    OnTradingDisabled<
        HistoricalClock,
        EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
        MultiExchangeTxMap,
        DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
    > for LoseMoneyStrategy
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
            MultiExchangeTxMap,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
        >,
    ) -> Self::OnTradingDisabled {
    }
}

#[derive(Debug, Clone, Default)]
struct LoseMoneyInstrumentData {
    last_trade: Option<PublicTrade>,
    market_data: DefaultInstrumentMarketData,
}

impl InstrumentDataState for LoseMoneyInstrumentData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.market_data.price()
    }
}

impl Processor<&MarketEvent<InstrumentIndex>> for LoseMoneyInstrumentData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentIndex>) -> Self::Audit {
        if let DataKind::Trade(trade) = &event.kind {
            self.last_trade = Some(trade.clone())
        } else {
            self.last_trade = None;
        }
    }
}

impl Processor<&AccountEvent> for LoseMoneyInstrumentData {
    type Audit = ();

    fn process(&mut self, _: &AccountEvent) -> Self::Audit {}
}

impl InFlightRequestRecorder for LoseMoneyInstrumentData {
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeIndex, InstrumentIndex>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeIndex, InstrumentIndex>) {}
}

fn args_constant(
    instruments: Vec<InstrumentConfig>,
    executions: Vec<ExecutionConfig>,
) -> Arc<
    BacktestArgsConstant<
        MarketDataInMemory<DataKind>,
        Daily,
        EngineState<DefaultGlobalData, LoseMoneyInstrumentData>,
    >,
> {
    // Construct IndexedInstruments
    let instruments = IndexedInstruments::new(instruments);

    // Initialise MarketData
    let market_events = market_data_from_file(FILE_PATH_MARKET_DATA_INDEXED);
    let market_data = MarketDataInMemory::new(Arc::new(market_events));
    let time_engine_start = DateTime::<Utc>::from_str("2025-03-25T23:07:00.773674205Z").unwrap();

    // Construct EngineState
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        LoseMoneyInstrumentData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    })
}

pub fn market_data_from_file<InstrumentKey, Kind>(
    file_path: &str,
) -> Vec<MarketStreamEvent<InstrumentKey, Kind>>
where
    InstrumentKey: for<'de> Deserialize<'de>,
    Kind: for<'de> Deserialize<'de>,
{
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);

    let mut events = reader
        .lines()
        .map(|line_result| {
            let line = line_result.unwrap();
            serde_json::from_str::<MarketStreamEvent<InstrumentKey, Kind>>(&line).unwrap()
        })
        .collect::<Vec<_>>();

    // `MarketDataInMemory::new` hard-asserts events are globally sorted ascending by `time_exchange`
    // (the backtest time-merge relies on it). The recorded fixture interleaves three instruments and
    // is NOT globally sorted (10k+ inversions), so before this helper sorted, `new` panicked at bench
    // *setup* — `bench_backtest`/`bench_backtests_concurrent` could not run at all, and any prior
    // `--baseline` numbers for those two groups are not comparable (they never completed). Sorting
    // here (stable) fixes that. Applied identically to every bench case, so it does not bias the
    // aux-seam / audit A/Bs.
    //
    // These synthetic corpora are Item-only. The sort key below maps `Reconnecting` to `None`, which
    // `Option`'s `Ord` sorts before every `Some` — so a `Reconnecting` event would be hoisted to the
    // front of the corpus regardless of when it occurred (this matches `new`'s own sortedness check,
    // which uses the same key, but is not a meaningful replay order). Assert the Item-only invariant
    // so a future fixture that contains reconnects fails loudly here instead of being quietly
    // reordered.
    assert!(
        events
            .iter()
            .all(|event| matches!(event, MarketStreamEvent::Item(_))),
        "market_data_from_file expects an Item-only corpus: the time sort cannot order Reconnecting \
         events (they collapse to `None`, sorting to the front). Filter or stable-partition them \
         first."
    );
    events.sort_by_key(|event| match event {
        MarketStreamEvent::Item(event) => Some(event.time_exchange),
        MarketStreamEvent::Reconnecting(_) => None,
    });

    events
}

fn args_dynamic(
    risk_free_return: Decimal,
) -> BacktestArgsDynamic<
    LoseMoneyStrategy,
    DefaultRiskManager<EngineState<DefaultGlobalData, LoseMoneyInstrumentData>>,
> {
    BacktestArgsDynamic {
        id: SmolStr::new("benches/backtest"),
        risk_free_return,
        strategy: LoseMoneyStrategy::default(),
        risk: DefaultRiskManager::default(),
    }
}
