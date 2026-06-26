#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the backtest auxiliary-event injection seam.
//!
//! These tests exercise the *plumbing* end-to-end through the public [`backtest`] /
//! [`run_backtests`] API: an [`EngineEvent::CorporateAction`] (and an
//! [`EngineEvent::ContractExpiry`]) supplied via [`AuxEventsInMemory`] is merged with the market
//! stream, flows through the engine, and the backtest runs to completion. The merge ordering /
//! tie-break / clock-seed logic itself is unit-tested in `backtest::tests`.
//!
//! Deep audit-replica + post-split position parity is covered separately via the direct
//! `Engine::process_with_audit` path, which `backtest` cannot reach (it hardcodes
//! `AuditMode::Disabled`, so per-event outputs and final engine state are not observable here). The
//! value-bearing assertions on post-split `quantity_abs` / `price_entry_average` / `SplitRemainder`
//! live in `test_engine_process_engine_event_with_audit.rs` —
//! `test_corporate_action_replica_parity_floor_split` and
//! `test_corporate_action_injected_mid_stream_clock_and_outputs`. The two files form **one**
//! contract: this file proves an aux event traverses the `backtest` async/merge/clock-seed path;
//! that file proves the engine applies the split correctly.

use std::{fs::File, io::BufReader, sync::Arc};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent, SplitRoundingPolicy, Timed,
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::AuxEventsInMemory, backtest,
        market_data::MarketDataInMemory, run_backtests,
    },
    engine::{
        command::Command,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_instrument::{
    corporate_action::CorporateActionKind, exchange::ExchangeId, index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use rustrade_integration::collection::one_or_many::OneOrMany;
use serde::Deserialize;

const CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/config/backtest_config.json"
);

type AuxBacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type DefaultAuxEvents = AuxEventsInMemory;

#[derive(Deserialize)]
struct Config {
    // `risk_free_return` is also present in the JSON but unused here; serde ignores it.
    system: SystemConfig,
}

fn load_config() -> Config {
    let reader = BufReader::new(File::open(CONFIG_PATH).expect("backtest_config.json must exist"));
    serde_json::from_reader(reader).expect("backtest_config.json must deserialize")
}

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// A single trade `MarketEvent` for the instrument at `instrument` index.
fn trade_for(
    instrument: usize,
    time: &str,
    price: Decimal,
) -> MarketStreamEvent<InstrumentIndex, DataKind> {
    let time = ts(time);
    MarketStreamEvent::Item(MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex::new(instrument),
        kind: DataKind::Trade(PublicTrade {
            id: "trade".into(),
            price,
            amount: dec!(0.01),
            side: None,
        }),
    })
}

/// A single trade `MarketEvent` for BTCUSDT (instrument index 0).
fn trade(time: &str, price: Decimal) -> MarketStreamEvent<InstrumentIndex, DataKind> {
    trade_for(0, time, price)
}

/// Four BTCUSDT trades spanning 22:00–23:00; any aux event in that window interleaves mid-stream.
fn market_events() -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    vec![
        trade("2025-03-24T22:00:00Z", dec!(60_000)),
        trade("2025-03-24T22:15:00Z", dec!(60_100)),
        trade("2025-03-24T22:45:00Z", dec!(60_200)),
        trade("2025-03-24T23:00:00Z", dec!(60_300)),
    ]
}

fn args_constant(
    aux_events: DefaultAuxEvents,
) -> Arc<
    BacktestArgsConstant<MarketDataInMemory<DataKind>, Daily, AuxBacktestState, DefaultAuxEvents>,
> {
    args_constant_with_market_data(aux_events, market_events())
}

/// As [`args_constant`], but with a caller-supplied market-data series (e.g. to feed a second
/// pre-declared instrument its own post-split prints).
fn args_constant_with_market_data(
    aux_events: DefaultAuxEvents,
    events: Vec<MarketStreamEvent<InstrumentIndex, DataKind>>,
) -> Arc<
    BacktestArgsConstant<MarketDataInMemory<DataKind>, Daily, AuxBacktestState, DefaultAuxEvents>,
> {
    let Config {
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    let instruments = IndexedInstruments::new(instruments);
    let market_data = MarketDataInMemory::new(Arc::new(events));

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .trading_state(TradingState::Enabled)
    .build();

    Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events,
    })
}

fn args_dynamic(
    id: &str,
) -> BacktestArgsDynamic<DefaultStrategy<AuxBacktestState>, DefaultRiskManager<AuxBacktestState>> {
    BacktestArgsDynamic {
        id: id.into(),
        risk_free_return: dec!(0.05),
        strategy: DefaultStrategy::default(),
        risk: DefaultRiskManager::default(),
    }
}

fn split_event(effective: &str, ratio: Decimal) -> Timed<EngineEvent> {
    Timed::new(
        EngineEvent::CorporateAction {
            id: "btcusdt-2025-03-24-split".into(),
            instrument: InstrumentIndex::new(0),
            kind: CorporateActionKind::StockSplit { ratio },
            policy: SplitRoundingPolicy::Fractional,
            effective_time: ts(effective),
        },
        ts(effective),
    )
}

/// A `CorporateAction` injected mid-stream flows through the merge + engine and the backtest
/// completes (full plumbing: threading, merge, clock seed, `stream::iter`, shutdown).
///
/// Plumbing-only by necessity: a mid-stream split is notional-preserving and the position stays
/// open, so it leaves no trace in the summary-level `trading_summary` (unlike the
/// before-first-tick case, whose clock seed IS summary-observable — see
/// `backtest_runs_with_aux_event_before_first_market_event`). The post-split quantity/basis
/// economics for this exact mid-stream scenario are asserted at the engine seam in
/// `test_corporate_action_injected_mid_stream_clock_and_outputs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_runs_with_corporate_action_injected_mid_stream() {
    let aux = AuxEventsInMemory::new(Arc::new(vec![split_event("2025-03-24T22:30:00Z", dec!(2))]));

    let summary = backtest(args_constant(aux), args_dynamic("with-split"))
        .await
        .expect("backtest with an injected split must complete");

    assert_eq!(summary.id, "with-split");
}

/// A `ContractExpiry` injected mid-stream is now backtest-testable for the first time — assert the
/// run completes (the aux seam delivers a non-split, non-market `EngineEvent`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_runs_with_contract_expiry_injected_mid_stream() {
    let expiry = Timed::new(
        EngineEvent::ContractExpiry(InstrumentIndex::new(0)),
        ts("2025-03-24T22:30:00Z"),
    );
    let aux = AuxEventsInMemory::new(Arc::new(vec![expiry]));

    let summary = backtest(args_constant(aux), args_dynamic("with-expiry"))
        .await
        .expect("backtest with an injected contract expiry must complete");

    assert_eq!(summary.id, "with-expiry");
}

/// An aux event scheduled *before* the first market tick still drives a complete run (the clock is
/// seeded from `min(first_market_event, first_aux_event)`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_runs_with_aux_event_before_first_market_event() {
    let aux = AuxEventsInMemory::new(Arc::new(vec![split_event("2025-03-24T21:45:00Z", dec!(2))]));

    let summary = backtest(args_constant(aux), args_dynamic("early-split"))
        .await
        .expect("backtest with an aux event before the first market tick must complete");

    assert_eq!(summary.id, "early-split");

    // Value-bearing seam check (not just "it ran"): the 21:45 aux event is BEFORE the first market
    // tick (22:00), so it can only influence the engine's start time if the aux source was actually
    // drained and merged. The clock seeds from `min(first_market, first_aux)`, freezing
    // `time_engine_start` into [21:45, 22:00); a swallowed aux event would leave it at ~22:00. Exact
    // equality is avoided because `HistoricalClock::time()` adds a sub-ms wall-clock delta when
    // `meta.time_start` is captured at `Engine::new` — the 15-minute bracket absorbs that jitter.
    let start = summary.trading_summary.time_engine_start;
    assert!(
        start >= ts("2025-03-24T21:45:00Z") && start < ts("2025-03-24T22:00:00Z"),
        "clock must seed from the 21:45 aux event, before the 22:00 first market tick; got {start}"
    );
}

/// The aux source threads through the concurrent `run_backtests` sweep unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backtests_sweep_threads_aux_events() {
    let aux = AuxEventsInMemory::new(Arc::new(vec![split_event("2025-03-24T22:30:00Z", dec!(2))]));

    let summaries = run_backtests(
        args_constant(aux),
        [args_dynamic("sweep-a"), args_dynamic("sweep-b")],
    )
    .await
    .expect("run_backtests sweep with injected splits must complete");

    assert_eq!(summaries.summaries.len(), 2);
}

/// Market-data series for the two-identity non-standard-split smoke: the "old" identity (idx0,
/// BTCUSDT) prints across the whole window; the pre-declared "new" identity (idx1, ETHUSDT) prints
/// ONLY after the 22:30 split — the natural shape, since the new contract did not exist before.
fn market_events_two_identity() -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    vec![
        trade_for(0, "2025-03-24T22:00:00Z", dec!(60_000)),
        trade_for(0, "2025-03-24T22:15:00Z", dec!(60_100)),
        trade_for(0, "2025-03-24T22:45:00Z", dec!(60_200)),
        trade_for(1, "2025-03-24T22:50:00Z", dec!(2_000)),
        trade_for(1, "2025-03-24T22:55:00Z", dec!(2_010)),
        trade_for(0, "2025-03-24T23:00:00Z", dec!(60_300)),
    ]
}

/// A `Command::ClosePositions` for `instrument`, wrapped for injection through the aux seam.
fn close_command_event(time: &str, instrument: usize) -> Timed<EngineEvent> {
    Timed::new(
        EngineEvent::Command(Command::ClosePositions(InstrumentFilter::Instruments(
            OneOrMany::One(InstrumentIndex::new(instrument)),
        ))),
        ts(time),
    )
}

/// **Structural feasibility smoke for the non-standard-split wrapper flow (plumbing only).**
///
/// Proves a downstream wrapper can drive the whole non-standard-split protocol through the public
/// `backtest()` API with no library re-registration: BOTH identities are pre-declared at
/// construction (the config registers idx0/idx1/idx2), the aux seam carries BOTH a `CorporateAction`
/// AND a flatten `Command::ClosePositions`, the new identity receives its own post-split data, and
/// the run completes to a `BacktestSummary`.
///
/// SCOPE — this asserts PLUMBING only, not economics: `backtest()` hardcodes `AuditMode::Disabled`,
/// so the `OptionPositionsRequireIdentityChange` observable is invisible here (its content is
/// asserted on the `process_with_audit` path in `test_engine_process_engine_event_with_audit.rs`).
/// The close trigger is *pre-planned* (injected at the known split time), not *reactive-from-output*
/// — the identical caveat the `OpenOrdersAtSplit` backtest pattern already carries. Kind-agnostic:
/// the config's spot instruments stand in for the option identities (a faithful option *trading*
/// backtest needs a custom strategy + price-bearing mock fills, deferred to a future milestone). The
/// injected `Command` generates no order — `DefaultStrategy` opens no position — so the MockExchange
/// price-less-market-order panic cannot fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_non_standard_split_seam_carries_corporate_action_and_close_command() {
    let aux = AuxEventsInMemory::new(Arc::new(vec![
        // 3:2 fractional forward ⇒ non-standard (the wrapper must migrate the option identity).
        split_event("2025-03-24T22:30:00Z", dec!(1.5)),
        // The wrapper flattens the old identity right after the split.
        close_command_event("2025-03-24T22:31:00Z", 0),
    ]));

    let summary = backtest(
        args_constant_with_market_data(aux, market_events_two_identity()),
        args_dynamic("non-standard-identity-change"),
    )
    .await
    .expect("backtest carrying a CorporateAction + a flatten Command must complete");

    assert_eq!(summary.id, "non-standard-identity-change");
}
