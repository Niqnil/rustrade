#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the [`Shutdown::AfterDrain`] barrier a backtest shuts down through.
//!
//! A market stream ending says only that there is no more *input*. The order responses that input
//! provoked are still in flight, and they are forwarded into the **same** FIFO feed the market
//! events came down. Terminating the moment the market stream has been *forwarded* therefore
//! enqueues the stop ahead of every one of those responses, and the `Engine` never reads them — so
//! the run reports a tear sheet of zeros that is indistinguishable from a strategy which chose to
//! stay flat.
//!
//! `latency_ms` is **zero** throughout: none of this depends on the mock exchange's simulated
//! network delay, which is a separate defect.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::NoAuxEvents, backtest,
        market_data::MarketDataInMemory,
    },
    engine::{
        Engine,
        clock::HistoricalClock,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    error::BarterError,
    execution::request::ExecutionRequest,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::{
        DefaultStrategy, algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::config::ExecutionConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountSnapshot,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
    },
};
use rustrade_instrument::{
    Side,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
    test_utils::instrument,
};

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

type ProbeState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type ProbeTxMap = rustrade::engine::execution_tx::MultiExchangeTxMap<
    rustrade_integration::channel::UnboundedTx<ExecutionRequest>,
>;

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// Sends one market buy per `generate_algo_orders` call, for the first `limit` calls.
///
/// `limit = 1` sends a single order; `usize::MAX` never stops on its own, which is what pins the
/// drain's suppression of new orders — see
/// [`draining_suppresses_new_orders_so_the_run_terminates`].
#[derive(Debug)]
struct OneShotStrategy {
    sent: AtomicUsize,
    limit: usize,
}

impl OneShotStrategy {
    fn once() -> Self {
        Self {
            sent: AtomicUsize::new(0),
            limit: 1,
        }
    }

    /// Never stops generating. Without the drain suppressing new orders, every response would
    /// provoke another order and the run would never reach quiescence.
    fn always() -> Self {
        Self {
            sent: AtomicUsize::new(0),
            limit: usize::MAX,
        }
    }
}

impl AlgoStrategy for OneShotStrategy {
    type State = ProbeState;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let opens = if self.sent.fetch_add(1, Ordering::Relaxed) < self.limit {
            state
                .instruments
                .instruments(&InstrumentFilter::None)
                .map(|instrument| OrderRequestOpen {
                    key: OrderKey {
                        exchange: instrument.instrument.exchange,
                        instrument: instrument.key,
                        strategy: StrategyId::new("probe"),
                        cid: ClientOrderId::random(),
                    },
                    state: RequestOpen {
                        side: Side::Buy,
                        price: None,
                        quantity: dec!(0.01),
                        kind: OrderKind::Market,
                        time_in_force: TimeInForce::ImmediateOrCancel,
                        position_id: None,
                        reduce_only: false,
                    },
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        (std::iter::empty(), opens)
    }
}

impl ClosePositionsStrategy for OneShotStrategy {
    type State = ProbeState;

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

impl OnDisconnectStrategy<HistoricalClock, ProbeState, ProbeTxMap, DefaultRiskManager<ProbeState>>
    for OneShotStrategy
{
    type OnDisconnect = ();

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            ProbeState,
            ProbeTxMap,
            Self,
            DefaultRiskManager<ProbeState>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
    }
}

impl OnTradingDisabled<HistoricalClock, ProbeState, ProbeTxMap, DefaultRiskManager<ProbeState>>
    for OneShotStrategy
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            ProbeState,
            ProbeTxMap,
            Self,
            DefaultRiskManager<ProbeState>,
        >,
    ) -> Self::OnTradingDisabled {
    }
}

fn funded(asset: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: AssetNameExchange::new(asset),
        balance: Balance::new(amount, amount),
        time_exchange: ts("2025-03-24T22:00:00Z"),
    }
}

/// Shared fixture: one funded spot instrument, three trades, zero simulated latency.
fn args_constant()
-> Arc<BacktestArgsConstant<MarketDataInMemory<DataKind>, Daily, ProbeState, NoAuxEvents>> {
    let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
    let key = instruments.instruments()[0].key;

    let market_events = [
        "2025-03-24T22:00:00Z",
        "2025-03-24T22:30:00Z",
        "2025-03-24T23:00:00Z",
    ]
    .into_iter()
    .map(|time| {
        let time = ts(time);
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: EXCHANGE,
            instrument: key,
            kind: DataKind::Trade(PublicTrade {
                id: "t".into(),
                price: dec!(50_000),
                amount: dec!(0.01),
                side: None,
            }),
        })
    })
    .collect::<Vec<_>>();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .trading_state(TradingState::Enabled)
    .build();

    Arc::new(BacktestArgsConstant {
        instruments,
        executions: vec![ExecutionConfig::Mock(MockExecutionConfig {
            mocked_exchange: EXCHANGE,
            initial_state: AccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![funded("btc", dec!(100)), funded("usdt", dec!(10_000_000))],
                instruments: vec![],
            },
            // Zero: nothing here depends on the mock exchange's wall-clock latency model.
            latency_ms: 0,
            fee_model: Default::default(),
            fill_model: Default::default(),
        })],
        market_data: MarketDataInMemory::new(Arc::new(market_events)),
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    })
}

fn args_dynamic<S>(
    id: &str,
    strategy: S,
) -> BacktestArgsDynamic<S, DefaultRiskManager<ProbeState>> {
    BacktestArgsDynamic {
        id: id.into(),
        risk_free_return: Decimal::ZERO,
        strategy,
        risk: DefaultRiskManager::default(),
    }
}

/// The response to an order sent on the last stretch of the market stream must still reach the
/// `Engine`.
///
/// `MockExecution` supplies no market prices, so the only response a market order can currently
/// draw is a rejection — which is exactly what makes this a delivery test: the rejection is
/// produced by the exchange either way, and the only question is whether the `Engine` ever sees
/// it. Before the drain barrier this run returned `Ok` with an all-zero summary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_open_reaches_the_engine_instead_of_being_discarded_at_shutdown() {
    let error = backtest(
        args_constant(),
        args_dynamic("one", OneShotStrategy::once()),
    )
    .await
    .expect_err("every open was rejected, so the run must report that rather than a summary");

    match error {
        BarterError::BacktestAllOrdersRejected { rejected, reason } => {
            assert_eq!(
                rejected, 1,
                "one open was sent, so one rejection is expected"
            );
            assert!(
                reason.contains("no market price available"),
                "the exchange's own reason must be carried through verbatim, got: {reason}"
            );
        }
        other => panic!("expected BacktestAllOrdersRejected, got {other:?}"),
    }
}

/// A drain must not let the strategy keep feeding itself.
///
/// The strategy here generates an order on every call. Each response it receives would provoke
/// another order, so without the drain suppressing new orders the run never reaches quiescence and
/// never terminates. The timeout turns that into a legible failure rather than a hung test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_suppresses_new_orders_so_the_run_terminates() {
    let run = backtest(
        args_constant(),
        args_dynamic("always", OneShotStrategy::always()),
    );

    let result = tokio::time::timeout(std::time::Duration::from_secs(20), run)
        .await
        .expect("a drain that suppresses new orders terminates; this timing out means it did not");

    let error = result.expect_err("every open was rejected");
    let BarterError::BacktestAllOrdersRejected { rejected, .. } = error else {
        panic!("expected BacktestAllOrdersRejected, got {error:?}");
    };

    // Terminating at all is the assertion this test exists for; without the drain suppressing new
    // orders the run above would never have returned. This is the secondary check: reaching
    // `BacktestAllOrdersRejected` requires `rejected == opened`, so every order that was sent was
    // also answered — nothing was stranded in flight.
    //
    // Not asserted exactly: orders are generated per processed event, and the initial account
    // snapshot is one of them alongside the three market events, so the count is at least three
    // rather than exactly three.
    assert!(
        rejected >= 3,
        "one order per market event should have been sent and answered, got {rejected}"
    );
}

/// The fast path: nothing in flight when the stop arrives, so there is nothing to drain.
///
/// Guards against the barrier introducing a stall in the ordinary case of a strategy that never
/// trades.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_sends_no_orders_completes_immediately() {
    let run = backtest(
        args_constant(),
        args_dynamic("none", DefaultStrategy::<ProbeState>::default()),
    );

    let result = tokio::time::timeout(std::time::Duration::from_secs(20), run)
        .await
        .expect("a run with nothing in flight must not wait on the drain")
        .expect("a strategy that sends no orders is a legitimate run, not a failure");

    let summary = &result.summary.trading_summary;
    assert_eq!(summary.orders_opened, 0);
    assert_eq!(summary.orders_rejected, 0);
}
