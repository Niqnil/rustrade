#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the venue roles a [`backtest`] run derives for itself.
//!
//! `backtest` receives a **pre-built** `EngineState` but builds its own execution clients, one set
//! per run — so it is the only component that knows both which venues hold instruments and which
//! of them anything actually executes on. It reconciles the two on its own clone of the state.
//!
//! The property under test is the one a split configuration depends on: a venue registered purely
//! so its prices are available must not be given an account connection to wait on. Getting
//! it wrong errors nowhere — the run completes with global connectivity pinned at `Reconnecting`
//! and every strategy that gates on it gated for the whole run — which is exactly why it needs a
//! test rather than a reviewer.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::NoAuxEvents, backtest,
        market_data::MarketDataInMemory,
    },
    engine::state::{
        EngineState,
        builder::EngineStateBuilder,
        connectivity::{Health, VenueRole},
        global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData,
        trading::TradingState,
    },
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::ExecutionConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{AccountSnapshot, client::mock::MockExecutionConfig};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
    test_utils::instrument,
};

/// Prices the instrument. Nothing is executed here, and no execution client is registered for it.
const DATA: ExchangeId = ExchangeId::LseEquities;

/// Executes the instrument. No market data subscription is ever made to it.
const EXECUTION: ExchangeId = ExchangeId::BinanceSpot;

type BacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// The two-instrument pattern: one instrument priced on [`DATA`] and never traded, driving orders
/// on a *different* instrument traded on [`EXECUTION`].
///
/// This — rather than a single instrument carrying a [`DataVenue`] — is the configuration the
/// instrument model alone gets wrong. `DATA` is the `Instrument::exchange` of the priced
/// instrument, so the model claims it holds an account; only the execution clients can say
/// otherwise. (A dual-venue instrument names `EXECUTION` as its `exchange`, so `DATA` never looks
/// like an account-holding venue in the first place.)
fn instruments() -> IndexedInstruments {
    IndexedInstruments::new([
        instrument(DATA, "xau", "usd"),
        instrument(EXECUTION, "btc", "usdt"),
    ])
}

/// Trades for the instrument priced on [`DATA`], tagged with that venue.
fn market_events(
    instruments: &IndexedInstruments,
) -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    // Resolved rather than hardcoded: `InstrumentIndex` is assigned by sort order, so an
    // `ExchangeId` declared later would silently re-point a literal index at the other instrument.
    let priced = instruments
        .instruments()
        .iter()
        .find(|keyed| keyed.value.data_exchange().value == DATA)
        .expect("the fixture prices one instrument on DATA")
        .key;

    ["2025-03-24T22:00:00Z", "2025-03-24T22:30:00Z"]
        .into_iter()
        .map(|time| {
            let time = ts(time);
            MarketStreamEvent::Item(MarketEvent {
                time_exchange: time,
                time_received: time,
                exchange: DATA,
                instrument: priced,
                kind: DataKind::Trade(PublicTrade {
                    id: "trade".into(),
                    price: dec!(3_000),
                    amount: dec!(0.01),
                    side: None,
                }),
            })
        })
        .collect()
}

fn mock_config() -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: EXECUTION,
        initial_state: AccountSnapshot {
            exchange: EXECUTION,
            balances: vec![],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: Default::default(),
        fill_model: Default::default(),
    }
}

fn engine_state(instruments: &IndexedInstruments) -> BacktestState {
    // Deliberately built WITHOUT `execution_venues`: the caller is not required to declare them,
    // and this is the state shape every pre-existing backtest supplies.
    EngineStateBuilder::new(instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .trading_state(TradingState::Enabled)
    .build()
}

fn args_dynamic(
    id: &str,
) -> BacktestArgsDynamic<DefaultStrategy<BacktestState>, DefaultRiskManager<BacktestState>> {
    BacktestArgsDynamic {
        id: id.into(),
        risk_free_return: Decimal::ZERO,
        strategy: DefaultStrategy::default(),
        risk: DefaultRiskManager::default(),
    }
}

/// The pricing venue is marked [`VenueRole::DataOnly`] from the execution clients the run built,
/// even though the supplied state could not know that — and the supplied state is left untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_derives_venue_roles_from_the_execution_clients_it_builds() {
    let instruments = instruments();
    let engine_state = engine_state(&instruments);

    // The state alone cannot tell that nothing executes on `DATA`: the instrument model says only
    // that some instrument is priced there. This is what the run has to correct.
    assert_eq!(
        engine_state.connectivity.connectivity(&DATA).role,
        VenueRole::Both
    );

    let market_data = MarketDataInMemory::new(Arc::new(market_events(&instruments)));

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        executions: vec![ExecutionConfig::Mock(mock_config())],
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    let result = backtest(Arc::clone(&args_constant), args_dynamic("venue-roles"))
        .await
        .expect("a backtest with a pricing-only venue must complete");

    let connectivity = &result.engine_state.connectivity;

    assert_eq!(
        connectivity.connectivity(&DATA).role,
        VenueRole::DataOnly,
        "no execution client was registered for the pricing venue, so it holds no account"
    );
    assert_eq!(
        connectivity.connectivity(&EXECUTION).role,
        VenueRole::Both,
        "the traded instrument is priced on its own venue, so that venue provides both dimensions"
    );

    // The consequence, and the whole point: the pricing venue is fully healthy on its market data
    // connection alone. Approximated as `Both` it would sit at `Reconnecting` forever, since no
    // account event can arrive from a venue with no execution client.
    assert_eq!(
        connectivity.connectivity(&DATA).market_data,
        Health::Healthy,
        "the run fed market events tagged with the pricing venue"
    );
    assert!(connectivity.connectivity(&DATA).all_healthy());

    // `BacktestArgsConstant` is shared across a whole sweep, so the reconciliation must apply to
    // the run's own clone.
    assert_eq!(
        args_constant
            .engine_state
            .connectivity
            .connectivity(&DATA)
            .role,
        VenueRole::Both,
        "the caller's state must not be modified"
    );
}
