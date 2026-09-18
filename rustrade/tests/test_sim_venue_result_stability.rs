#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Result stability for the simulated venue.
//!
//! This runs a fixed strategy over the committed market-data fixture and compares the resulting
//! tear sheet — realised PnL, fee totals, per-asset closing balances, drawdown windows, trade
//! counts — byte-for-byte against a committed golden artifact, so a result that moves fails CI
//! rather than waiting to be noticed.
//!
//! # What it is defending
//!
//! Most of the work this guards is not supposed to move a number at all: market state on the venue,
//! balance reservations, a restructured open-order store, limit orders, time in force, and booking
//! each request at the instant it reaches its venue all left this artifact byte-identical. An order
//! that filled before must still fill, at the same price, leaving the same balances behind, and
//! "must not change" is only a claim until something checks it.
//!
//! It is not a claim that the number may *never* move. Pricing a market order from the venue's own
//! book rather than from the snapshot its request carried moved it deliberately: the fixture runs at
//! `latency_ms: 100`, so each order now pays the market 50ms after it was decided rather than the
//! market it was decided against. The job of this test is to make that a diff somebody signed off
//! on, not to forbid it.
//!
//! # Why the summary rather than the engine state
//!
//! A `Debug` or `serde` dump of `EngineState` changes shape the moment a field is added, which the
//! work this guards is going to do repeatedly. `TradingSummary` is the reported *result*, so it
//! moves only when a number moves — which is the property actually being defended.
//!
//! # Fixture
//!
//! [`FILE_PATH_MARKET_DATA`] is the Binance spot L1-and-trades capture that ships with the
//! examples: 50,000 events across three instruments spanning roughly three minutes. It is the only
//! market-data fixture in this repo that may be redistributed, which is why it is the one used
//! here.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataInMemory},
        summary::BacktestResult,
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
    execution::request::ExecutionRequest,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::config::SystemConfig,
};
use rustrade_data::{event::DataKind, streams::consumer::MarketStreamEvent};
use rustrade_execution::order::{
    OrderKey, OrderKind, TimeInForce,
    id::{ClientOrderId, StrategyId},
    request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
};
use rustrade_instrument::{
    Side,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use serde::Deserialize;

const CONFIG_PATH: &str = "examples/config/backtest_config.json";
const FILE_PATH_MARKET_DATA: &str =
    "examples/data/binance_spot_trades_l1_btcusdt_ethusdt_solusdt.json";
/// The expected tear sheet. Derived from [`FILE_PATH_MARKET_DATA`], which is redistributable.
const GOLDEN_PATH: &str = "tests/data/sim_venue_stability_summary.txt";

/// One order every `ORDER_EVERY_N_CALLS` calls, at most [`MAX_ORDERS`] in total.
///
/// # Why not one per call
///
/// The `Engine` calls the strategy for account events as well as market events, so a strategy that
/// emits unconditionally trades on its own fills — a zero-delay feedback cycle that
/// `SimRunner`'s guard is designed to abandon rather than run. Spacing emissions well apart leaves
/// every fill's account event landing on a call that emits nothing, so the run exercises fills
/// without ever arming that guard.
const ORDER_EVERY_N_CALLS: usize = 400;

/// Total orders the fixture sends. Enough fills to move every number on the tear sheet; small
/// enough that the funded quote balance covers them all.
const MAX_ORDERS: usize = 40;

type ProbeState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type ProbeTxMap = rustrade::engine::execution_tx::MultiExchangeTxMap<
    rustrade_integration::channel::UnboundedTx<ExecutionRequest>,
>;

#[derive(Deserialize)]
struct Config {
    risk_free_return: Decimal,
    system: SystemConfig,
}

/// Buys a fixed quantity on a fixed schedule, ignoring the market entirely.
///
/// Deliberately not a trading idea. A fixture defending result stability wants the *venue* to be
/// the only thing that decides anything; a strategy reacting to price would fold the venue's
/// behaviour and its own into one number and make a diff impossible to attribute.
#[derive(Debug)]
struct MetronomeStrategy {
    calls: AtomicUsize,
    sent: AtomicUsize,
}

impl MetronomeStrategy {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            sent: AtomicUsize::new(0),
        }
    }
}

impl AlgoStrategy for MetronomeStrategy {
    type State = ProbeState;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if !call.is_multiple_of(ORDER_EVERY_N_CALLS) {
            return (std::iter::empty(), Vec::new());
        }

        // Only instruments the venue can price. A cold-start rejection is legitimate venue
        // behaviour, but it is not what this fixture is measuring.
        let priced = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .filter(|instrument| instrument.data.price().is_some())
            .collect::<Vec<_>>();
        if priced.is_empty() {
            return (std::iter::empty(), Vec::new());
        }

        let seq = self.sent.fetch_add(1, Ordering::Relaxed);
        if seq >= MAX_ORDERS {
            return (std::iter::empty(), Vec::new());
        }

        // Rotate, so every instrument's ledger is exercised rather than only whichever instrument
        // happens to be priced first.
        let instrument = priced[seq % priced.len()];

        let open = OrderRequestOpen {
            key: OrderKey {
                exchange: instrument.instrument.exchange,
                instrument: instrument.key,
                strategy: StrategyId::new("metronome"),
                // Deterministic: a random CID would differ run to run, and the restructured
                // open-order store is going to key on this.
                cid: ClientOrderId::new(format!("metronome-{seq}")),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: None,
                quantity: dec!(0.001),
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market: None,
            },
        };

        (std::iter::empty(), vec![open])
    }
}

impl ClosePositionsStrategy for MetronomeStrategy {
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
    for MetronomeStrategy
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
    for MetronomeStrategy
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

fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("backtest config fixture must be readable");
    serde_json::from_reader(BufReader::new(file)).expect("backtest config fixture must parse")
}

/// Loads the fixture and sorts it, which it needs before `MarketDataInMemory` will accept it.
///
/// The recorded capture interleaves three instruments and is **not** globally sorted by
/// `time_exchange` — it holds over ten thousand inversions — while `MarketDataInMemory::new`
/// hard-asserts global sortedness because the backtest time-merge depends on it. The benches that
/// read this same file carry the identical sort for the identical reason
/// (`rustrade/benches/backtest/mod.rs`).
///
/// The corpus is `Item`-only, which the assert below pins: the sort key maps `Reconnecting` to
/// `None`, and `Option`'s `Ord` puts `None` first, so a reconnect would be hoisted to the front of
/// the run regardless of when it happened.
fn market_data_from_file() -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    let file = File::open(FILE_PATH_MARKET_DATA).expect("market data fixture must be readable");
    let mut events = BufReader::new(file)
        .lines()
        .map(|line| {
            serde_json::from_str::<MarketStreamEvent<InstrumentIndex, DataKind>>(
                &line.expect("market data fixture must be readable"),
            )
            .expect("every market data fixture line must parse")
        })
        .collect::<Vec<_>>();

    assert!(
        events
            .iter()
            .all(|event| matches!(event, MarketStreamEvent::Item(_))),
        "the fixture must be Item-only: the time sort cannot order Reconnecting events"
    );

    // Stable, so events sharing a timestamp keep their recorded order and the run stays reproducible.
    events.sort_by_key(|event| match event {
        MarketStreamEvent::Item(event) => Some(event.time_exchange),
        MarketStreamEvent::Reconnecting(_) => None,
    });

    events
}

async fn run() -> BacktestResult<Daily, ProbeState> {
    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    let instruments = IndexedInstruments::new(instruments);
    let market_data = MarketDataInMemory::new(Arc::new(market_data_from_file()));
    let time_engine_start = market_data.time_first_event().await.unwrap();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    backtest(
        Arc::new(BacktestArgsConstant {
            instruments,
            executions,
            market_data,
            summary_interval: Daily,
            engine_state,
            aux_events: NoAuxEvents,
        }),
        BacktestArgsDynamic {
            id: "stability".into(),
            risk_free_return,
            strategy: MetronomeStrategy::new(),
            risk: DefaultRiskManager::<ProbeState>::default(),
        },
    )
    .await
    .expect("the stability fixture must complete")
}

/// The first differing line of two tear sheets, with a little context either side.
///
/// A whole-file diff of a 185-line `Debug` dump buries the one line that moved. A result change
/// here is usually a single number, and the line it is on is the entire diagnosis.
fn first_difference(expected: &str, actual: &str) -> String {
    let mut report = String::new();

    let expected: Vec<&str> = expected.lines().collect();
    let actual: Vec<&str> = actual.lines().collect();

    let Some(line) = (0..expected.len().max(actual.len()))
        .find(|index| expected.get(*index) != actual.get(*index))
    else {
        // Unreachable while the caller only calls this on a mismatch, but a panic in the failure
        // reporter would replace a useful message with a useless one.
        return "the two differ, but no differing line was found (trailing newline?)".to_string();
    };

    let context = line.saturating_sub(2)..(line + 3).min(expected.len().max(actual.len()));
    report.push_str(&format!("first difference at line {}:\n", line + 1));
    for index in context {
        let expected_line = expected.get(index).copied().unwrap_or("<end of file>");
        let actual_line = actual.get(index).copied().unwrap_or("<end of file>");
        if expected_line == actual_line {
            report.push_str(&format!("   {expected_line}\n"));
        } else {
            report.push_str(&format!("  -{expected_line}\n  +{actual_line}\n"));
        }
    }

    report.push_str(&format!(
        "\n({} expected lines, {} actual)",
        expected.len(),
        actual.len()
    ));
    report
}

/// The run's tear sheet, compared against the committed golden artifact.
///
/// Stage 3's acceptance criterion is that an existing backtest reports what it reported before. That
/// is only a claim until something checks it, and a claim a human re-reads off a log is checked once
/// and then forgotten. Committing the expected tear sheet makes the criterion CI's problem.
///
/// The artifact is `Debug`, not JSON: `TradingSummary::assets` is keyed by `ExchangeAsset`, a
/// struct, and `serde_json` rejects non-string map keys, so `to_string` on a funded summary fails
/// outright (tracked as issue #301). `Debug` is total, and stable for as long as `TradingSummary`'s
/// own shape is.
///
/// # Regenerating
///
/// A change to `TradingSummary`'s *shape* — a new field, a renamed one — legitimately rewrites this
/// file without any result having moved. Regenerate with:
///
/// ```text
/// UPDATE_GOLDEN=1 cargo test -p rustrade --test test_sim_venue_result_stability
/// ```
///
/// and read the diff before committing it. A diff confined to added fields is a shape change; a diff
/// touching a number is a result change, and needs explaining rather than accepting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_venue_backtest_summary_is_stable() {
    let result = run().await;
    let summary = &result.summary.trading_summary;
    let actual = format!("{summary:#?}\n");

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN_PATH, &actual).expect("golden artifact must be writable");
        println!("UPDATE_GOLDEN set: rewrote {GOLDEN_PATH}");
    } else {
        let expected = std::fs::read_to_string(GOLDEN_PATH).unwrap_or_else(|error| {
            panic!(
                "golden artifact {GOLDEN_PATH} must be readable ({error}); regenerate it with \
                 UPDATE_GOLDEN=1"
            )
        });

        if actual != expected {
            panic!(
                "the simulated venue's reported tear sheet changed.\n\n{}\n\nIf the change is \
                 intended, regenerate with `UPDATE_GOLDEN=1 cargo test -p rustrade --test \
                 test_sim_venue_result_stability` and justify every moved number in the PR.",
                first_difference(&expected, &actual)
            );
        }
    }

    // The fixture is only meaningful if it actually traded. A silent regression to zero fills would
    // otherwise make every later comparison vacuously "stable".
    let trades: usize = result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| instrument.position.positions.values())
        .map(|position| position.trades.len())
        .sum();
    println!("open-position trades: {trades}");
    assert!(
        trades > 0 || !summary.instruments.is_empty(),
        "the stability fixture must trade, otherwise it defends nothing"
    );
}

/// Two runs of the identical fixture must agree.
///
/// Distinct from the stability check above: that one compares across a *code* change, this one
/// catches nondeterminism within a single build — iteration order, task scheduling, or anything
/// else that lets the same input produce two answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_venue_backtest_is_deterministic_across_runs() {
    let first = run().await;
    let second = run().await;

    assert_eq!(
        format!("{:#?}", first.summary.trading_summary),
        format!("{:#?}", second.summary.trading_summary),
        "the same fixture run twice must produce the same tear sheet"
    );
}
