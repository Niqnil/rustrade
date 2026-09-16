#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for how a backtest ends, and for the order responses it still owes when it
//! does.
//!
//! A market stream ending says only that there is no more *input*. The order responses that input
//! provoked are still owed, so a run that stops the moment the input does never reads them — and
//! reports a tear sheet of zeros indistinguishable from a strategy which chose to stay flat.
//!
//! Waiting for the `Engine`'s own requests to fall quiet is not enough either. A filled order
//! produces a *response* and, separately, the `Trade` and balance that the fill actually consists
//! of. Stopping when the response resolves the last in-flight request cuts the run off while those
//! are still owed, so the balance ledger and the position ledger each lose a different number of
//! fills — and can end up contradicting each other within a single run.
//!
//! `SimRunner` removes both by construction rather than by timing: it owns the queue of what each
//! venue owes, and ends only once the market source is exhausted **and** that queue is empty.
//! [`assert_ledgers_reconcile`] is the regression assertion for it, and failed on roughly a third of
//! runs before the drain moved.
//!
//! # These runs are deterministic, and that is asserted
//! Account events are interleaved into the market stream in *simulated* time on the `Engine`'s own
//! thread, so a fill stamped at instant `T` reaches the `Engine` before the market event at `T`
//! marks the resulting position to that price. Everything derived from where a fill lands in the
//! sequence is therefore a function of the dataset alone: `pnl_unrealised`,
//! `Position::time_exchange_update` and the order counts are asserted exactly, at fixed values.
//!
//! Under the forwarded feed these were scheduler-dependent — a fill could land after the market
//! events that should have marked it, leaving a position that was never priced — so each of them is
//! also a guard on that not returning.
//!
//! The three market events carry **rising prices**, so a position marked to the last of them has a
//! `pnl_unrealised` that only the correct interleaving produces. A flat fixture would report zero
//! whether the fill landed in the right place or not.
//!
//! `latency_ms` is **zero** throughout. That is what makes
//! [`a_strategy_trading_on_its_own_fills_is_reported_rather_than_hung`] possible: at zero simulated
//! latency, a strategy that trades on its own fills is a zero-delay cycle.

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
    execution::{request::ExecutionRequest, sim::DEFAULT_FEEDBACK_LIMIT},
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
/// feedback guard — see [`a_strategy_trading_on_its_own_fills_is_reported_rather_than_hung`].
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

    /// Never stops generating: one order per call, and the `Engine` calls the strategy for
    /// account events as well as for market events. Against a venue with zero simulated latency
    /// that is a zero-delay feedback cycle rather than a strategy — see
    /// [`a_strategy_trading_on_its_own_fills_is_reported_rather_than_hung`].
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

    // Rising, so a position marked to the last event has a non-zero `pnl_unrealised` that only the
    // correct fill placement produces. See the module docs.
    let market_events = [
        ("2025-03-24T22:00:00Z", dec!(50_000)),
        ("2025-03-24T22:30:00Z", dec!(51_000)),
        ("2025-03-24T23:00:00Z", dec!(52_000)),
    ]
    .into_iter()
    .map(|(time, price)| {
        let time = ts(time);
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: EXCHANGE,
            instrument: key,
            kind: DataKind::Trade(PublicTrade {
                id: "t".into(),
                price,
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

    // The fill is provoked by the first market event and delivered at that same instant, ahead of
    // the two that follow -- so both of them mark it, and the last one to do so sets the timestamp.
    // A fill landing after them instead leaves a position that was never priced: `pnl_unrealised`
    // of zero and a `time_exchange_update` of the entry. That is what the forwarded feed produced
    // on some runs, and these two assertions are the guard on it.
    assert_eq!(
        position.pnl_unrealised,
        dec!(20),
        "0.01 entered at 50_000 and marked to the closing 52_000 is 20, with zero fees"
    );
    assert_eq!(
        position.time_exchange_update,
        ts("2025-03-24T23:00:00Z"),
        "the last market event marks the position, so it owns the update timestamp"
    );

    assert_ledgers_reconcile(&result);
}

/// A strategy that trades on its own fills at zero simulated latency is reported, not spun on.
///
/// The strategy here opens an order on every call, and the `Engine` calls it for account events as
/// well as for market events. At `latency_ms: 0` the response to each of those orders is stamped at
/// the very instant of the request that provoked it, so it outranks every later market event:
/// simulated time never advances, the market source is never drawn again, and the queue of owed
/// deliverables grows without bound.
///
/// No discrete-event simulator can resolve that by scheduling alone — there is no instant to place
/// the response at that is both after its cause and before the next input — so it is a property of
/// the *configuration*, and the library's job is to say so. Before the feedback guard it presented
/// as a silent spin at 100% CPU for as long as it was left running.
///
/// # Why there is no `tokio::time::timeout` here
/// The other runs in this file wrap themselves in one. It would buy nothing here: the spin this
/// guards never awaits anything pending, so the run never yields to the runtime and no timer can
/// fire against it. A regression would hang until the CI job's own timeout, which is precisely why
/// the condition has to be detected in the runner rather than waited out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strategy_trading_on_its_own_fills_is_reported_rather_than_hung() {
    let error = backtest(
        args_constant(),
        args_dynamic("always", OneShotStrategy::always()),
    )
    .await
    .expect_err("a zero-delay feedback cycle is a failed configuration, not a run");

    match error {
        BarterError::SimFeedbackLoop {
            exchange,
            time,
            limit,
        } => {
            assert_eq!(exchange, EXCHANGE);
            assert_eq!(
                time,
                ts("2025-03-24T22:00:00Z"),
                "the cycle closes on the first market event, and the clock never leaves it"
            );
            assert_eq!(limit, DEFAULT_FEEDBACK_LIMIT);
        }
        other => panic!("expected SimFeedbackLoop, got {other:?}"),
    }
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
