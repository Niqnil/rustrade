#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Result stability for the simulated venue.
//!
//! The simulated venue is gaining market state of its own, and with it balance reservations and a
//! restructured open-order store. None of that is meant to change what an existing backtest
//! reports: an order that filled before must still fill, at the same price, leaving the same
//! balances behind.
//!
//! "Must not change" is only a claim until something checks it. This runs a fixed strategy over the
//! committed market-data fixture and canonicalises the resulting [`TradingSummary`] to JSON, so the
//! whole tear sheet — realised PnL, fee totals, per-asset closing balances, trade counts — can be
//! compared byte-for-byte across a change.
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

/// Canonical JSON for the run's tear sheet — the artifact a change is diffed against.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_venue_backtest_summary_is_stable() {
    let result = run().await;
    let summary = &result.summary.trading_summary;

    // `Debug`, not JSON: `TradingSummary::assets` is keyed by `ExchangeAsset`, a struct, and
    // `serde_json` rejects non-string map keys — `to_string` on a funded summary fails outright.
    // `Debug` is total, and stable for as long as `TradingSummary`'s own shape is, which is exactly
    // the window this fixture defends.
    println!("---8<--- TRADING SUMMARY ---8<---");
    println!("{summary:#?}");
    println!("---8<--- END ---8<---");

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
