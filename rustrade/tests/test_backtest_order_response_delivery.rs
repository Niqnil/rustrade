#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the drained shutdown a backtest ends through.
//!
//! A market stream ending says only that there is no more *input*. The order responses that input
//! provoked are still in flight, and they are forwarded into the **same** FIFO feed the market
//! events came down. Terminating the moment the market stream has been *forwarded* therefore
//! enqueues the stop ahead of every one of those responses, and the `Engine` never reads them — so
//! the run reports a tear sheet of zeros that is indistinguishable from a strategy which chose to
//! stay flat.
//!
//! Waiting for the `Engine`'s own requests to fall quiet is not enough either. A filled order
//! produces a *response* and, separately, the `Trade` and balance that the fill actually consists
//! of. Stopping when the response resolves the last in-flight request cuts the run off while those
//! are still in the account channel, so the balance ledger and the position ledger each lose a
//! different, run-dependent number of fills — and can end up contradicting each other within a
//! single run. The end of a run is therefore owned by the execution side: each `ExecutionManager`
//! finishes its in-flight requests, forwards the account events they produced, and only then closes
//! its channel, which is what ends the `Engine`'s feed.
//!
//! [`assert_ledgers_reconcile`] is the regression assertion for that: it holds on every run here,
//! and failed on roughly a third of runs before the drain moved.
//!
//! `latency_ms` is **zero** throughout: none of this depends on the mock exchange's simulated
//! network delay, which is a separate defect.
//!
//! # What these tests deliberately do not assert
//! A backtest's market feed is unpaced and its account events land at a scheduler-determined point
//! within it, so anything derived from *when* a fill was priced against the feed — `pnl_unrealised`,
//! `Position::time_exchange_update`, tear-sheet series — still varies run to run, as does the number
//! of orders a per-event strategy emits. Only quantities that a complete set of fills determines are
//! asserted here.

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
        market_data::MarketDataInMemory, summary::BacktestResult,
    },
    engine::{
        Engine,
        clock::HistoricalClock,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
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
    asset::{
        AssetIndex, ExchangeAsset,
        name::{AssetNameExchange, AssetNameInternal},
    },
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

/// Sends one buy per `generate_algo_orders` call that can be priced, for the first `limit` such
/// calls.
///
/// `limit = 1` sends a single order; `usize::MAX` never stops on its own, which is what pins the
/// drain's suppression of new orders — see
/// [`draining_suppresses_new_orders_so_the_run_terminates`].
///
/// # Why emission is gated on the instrument having a price
/// The `Engine` calls this for the initial account snapshot as well as for market events, and
/// whichever of those it reaches first is decided by task scheduling. Emitting unconditionally
/// therefore produced an order priced against an *empty* market snapshot on some runs and not
/// others, which the venue rejects — so the same fixture flip-flopped between "one fill" and "one
/// fill plus one cold-start rejection". Waiting for a price is also what a strategy needing one
/// would really do. The cold-start rejection itself is covered by `MockExchange`'s own unit tests.
#[derive(Debug)]
struct OneShotStrategy {
    sent: AtomicUsize,
    limit: usize,
    kind: OrderKind,
}

impl OneShotStrategy {
    fn once() -> Self {
        Self {
            sent: AtomicUsize::new(0),
            limit: 1,
            kind: OrderKind::Market,
        }
    }

    /// Sends a single order the mock exchange refuses outright, whatever the market looks like.
    ///
    /// `MockExchange` supports only [`OrderKind::Market`] and checks that before it tries to price
    /// anything, so this rejects on every run rather than only on those where the order happened to
    /// be generated before the first market event.
    fn once_rejected() -> Self {
        Self {
            sent: AtomicUsize::new(0),
            limit: 1,
            kind: OrderKind::Limit,
        }
    }

    /// Never stops generating. Without the drain suppressing new orders, every response would
    /// provoke another order and the run would never reach quiescence.
    fn always() -> Self {
        Self {
            sent: AtomicUsize::new(0),
            limit: usize::MAX,
            kind: OrderKind::Market,
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
        // Only instruments the venue could actually price. See the type docs.
        let priced = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .filter(|instrument| instrument.data.price().is_some())
            .collect::<Vec<_>>();

        // `sent` is incremented only on calls that could emit, so `limit` counts orders rather
        // than counting calls that produced nothing.
        if priced.is_empty() || self.sent.fetch_add(1, Ordering::Relaxed) >= self.limit {
            return (std::iter::empty(), Vec::new());
        }

        let opens = priced
            .into_iter()
            .map(|instrument| OrderRequestOpen {
                key: OrderKey {
                    exchange: instrument.instrument.exchange,
                    instrument: instrument.key,
                    strategy: StrategyId::new("probe"),
                    cid: ClientOrderId::random(),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    // A Limit order carries one so the request is well-formed; the mock rejects it
                    // on `kind` before reading it.
                    price: (self.kind != OrderKind::Market).then(|| dec!(50_000)),
                    quantity: dec!(0.01),
                    kind: self.kind,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: false,
                    market: None,
                },
            })
            .collect::<Vec<_>>();

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
                balances: vec![funded("btc", dec!(100)), funded("usdt", initial_usdt())],
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

/// Quote balance the mock account starts with.
fn initial_usdt() -> Decimal {
    dec!(10_000_000)
}

/// Every fill debits the quote asset and enlarges the position, so the two ledgers must describe
/// the same set of fills.
///
/// This is the assertion the drain exists for. Both ledgers are delivered as separate account
/// events, and a shutdown that cut the run short truncated them by *different* amounts: runs ended
/// having debited three fills' worth of quote while holding two fills' worth of position, or having
/// debited without opening a position at all.
///
/// Fees are `Zero` in this fixture, so the identity is exact:
/// `initial_usdt - final_usdt == price_entry_average * quantity_abs`.
fn assert_ledgers_reconcile(result: &BacktestResult<Daily, ProbeState>) {
    let usdt_end = result
        .summary
        .trading_summary
        .assets
        .get(&ExchangeAsset::<AssetNameInternal>::new(EXCHANGE, "usdt"))
        .expect("the run is funded in usdt, so it must be summarised")
        .balance_end
        .expect("a funded asset has a closing balance")
        .total;

    let debited = initial_usdt() - usdt_end;

    let positions = result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| instrument.position.positions.values())
        .collect::<Vec<_>>();

    let held = positions
        .iter()
        .map(|position| position.price_entry_average * position.quantity_abs)
        .sum::<Decimal>();

    assert_eq!(
        debited, held,
        "quote debited ({debited}) must equal the notional the position ledger holds ({held}) - \
         a mismatch means the run was cut off between the two halves of a fill"
    );

    // Each fill is one trade of 0.01, so the position must also account for every trade it names.
    let quantity: Decimal = positions.iter().map(|position| position.quantity_abs).sum();
    let trades: usize = positions.iter().map(|position| position.trades.len()).sum();

    assert_eq!(
        quantity,
        dec!(0.01) * Decimal::from(trades),
        "the position holds {quantity} across {trades} trades of 0.01 each"
    );
}

/// The response to an order sent on the last stretch of the market stream must still reach the
/// `Engine`.
///
/// The rejection is produced by the exchange either way, so the only question this asks is whether
/// the `Engine` ever sees it. Before the drain this run returned `Ok` with an all-zero summary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_open_reaches_the_engine_instead_of_being_discarded_at_shutdown() {
    let error = backtest(
        args_constant(),
        args_dynamic("one", OneShotStrategy::once_rejected()),
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
                reason.contains("does not support OrderKind::Limit"),
                "the exchange's own reason must be carried through verbatim, got: {reason}"
            );
        }
        other => panic!("expected BacktestAllOrdersRejected, got {other:?}"),
    }
}

/// A *fill* sent on the last stretch of the market stream must reach the `Engine` whole.
///
/// The rejection case above only proves the order's response arrived. A fill is delivered as three
/// separate things — the balance it debits, the `Trade` it consists of, and the response that
/// reports it filled — and the response is the one that clears the request from flight. A shutdown
/// keyed on that clearing dropped the other two, so this asserts the whole fill landed, not just
/// the part that ends the wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fill_reaches_the_engine_whole_instead_of_being_truncated_at_shutdown() {
    let run = backtest(
        args_constant(),
        args_dynamic("fill", OneShotStrategy::once()),
    );

    let result = tokio::time::timeout(std::time::Duration::from_secs(20), run)
        .await
        .expect("a drained shutdown terminates; this timing out means it did not")
        .expect("the open fills, so the run produces a summary rather than an error");

    let summary = &result.summary.trading_summary;
    assert_eq!(
        summary.orders_opened, 1,
        "the strategy sends exactly one open"
    );
    assert_eq!(
        summary.orders_rejected, 0,
        "the instrument is priced and the account funded, so the open fills"
    );

    let positions = result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| instrument.position.positions.values())
        .collect::<Vec<_>>();

    let [position] = positions.as_slice() else {
        panic!("one fill opens exactly one position, got {positions:?}");
    };

    assert_eq!(position.quantity_abs, dec!(0.01));
    assert_eq!(
        position.price_entry_average,
        dec!(50_000),
        "the fill is priced at the market snapshot the Engine stamped on the request"
    );
    assert_eq!(position.trades.len(), 1, "one fill is one trade");

    assert_ledgers_reconcile(&result);
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

    let result = result.expect("every open fills, so the run produces a summary");

    let summary = &result.summary.trading_summary;
    assert_eq!(
        summary.orders_rejected, 0,
        "the instrument is priced and the account funded, so nothing is rejected"
    );
    assert!(
        summary.orders_opened >= 3,
        "one order per market event at least should have been sent, got {}",
        summary.orders_opened
    );

    // Every order sent was answered *and* its fill delivered in full. Not asserted exactly: this
    // strategy emits per processed event, and how many account events the Engine gets through
    // before the stop is scheduler-dependent, so `orders_opened` is 3 or 4 run to run.
    let trades: usize = result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| instrument.position.positions.values())
        .map(|position| position.trades.len())
        .sum();

    assert_eq!(
        trades, summary.orders_opened,
        "every one of the {} orders sent must have produced a delivered fill, got {trades}",
        summary.orders_opened
    );

    assert_ledgers_reconcile(&result);
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
