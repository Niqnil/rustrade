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
//! `process_with_audit` path (it cannot run through `backtest`, which hardcodes
//! `AuditMode::Disabled`).

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
    engine::state::{
        EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData, trading::TradingState,
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

/// A single trade `MarketEvent` for BTCUSDT (instrument index 0).
fn trade(time: &str, price: Decimal) -> MarketStreamEvent<InstrumentIndex, DataKind> {
    let time = ts(time);
    MarketStreamEvent::Item(MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex::new(0),
        kind: DataKind::Trade(PublicTrade {
            id: "trade".into(),
            price,
            amount: dec!(0.01),
            side: None,
        }),
    })
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
    let Config {
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    let instruments = IndexedInstruments::new(instruments);
    let market_data = MarketDataInMemory::new(Arc::new(market_events()));

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
