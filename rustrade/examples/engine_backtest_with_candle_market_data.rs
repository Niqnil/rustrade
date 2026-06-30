#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Backtest driven by **candle** market data.
//!
//! This example exists to demonstrate two things that are easy to get wrong when
//! feeding candles (rather than trades / L1) into the engine:
//!
//! 1. **`time_exchange` must be the candle's `close_time` (the period END).**
//!    The backtest clock derives "current time" — and replays events in that
//!    order — from each event's `time_exchange`. A candle aggregates a whole time
//!    window; stamping its *open* would make a completed bar enter the timeline at
//!    the instant its period *began*, i.e. silent lookahead. We compute the
//!    boundary with the library's shared [`close_time_from_open`] helper and use it
//!    for both `Candle::close_time` and the wrapping `MarketEvent::time_exchange`.
//!
//! 2. **Candles need a custom [`InstrumentDataState`].** The built-in
//!    [`DefaultInstrumentMarketData`](rustrade::engine::state::instrument::data::DefaultInstrumentMarketData)
//!    only tracks trades + L1 and ignores `DataKind::Candle`, so a candle-driven
//!    engine must supply its own state. [`CandleInstrumentData`] below is a minimal
//!    one that exposes the latest candle close as the instrument price.

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataInMemory},
    },
    engine::{
        Processor,
        state::{
            EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
            instrument::data::InstrumentDataState,
            order::in_flight_recorder::InFlightRequestRecorder, trading::TradingState,
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
    subscription::candle::{Candle, IntervalStep, close_time_from_open},
};
use rustrade_execution::{
    AccountEvent,
    order::request::{OrderRequestCancel, OrderRequestOpen},
};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use serde::Deserialize;
use std::{fs::File, io::BufReader, sync::Arc};

const CONFIG_PATH: &str = "rustrade/examples/config/backtest_config.json";

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

#[tokio::main]
async fn main() {
    rustrade::logging::init_logging();

    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    // The backtest_config.json defines three instruments; IndexedInstruments
    // assigns them indices in order, so BTCUSDT is InstrumentIndex(0). This
    // example only emits candle events for BTCUSDT — ETHUSDT/SOLUSDT receive no
    // market data, which is valid (no positions simply open on them).
    let instruments = IndexedInstruments::new(instruments);

    // Build an in-memory stream of 1h candle MarketEvents for BTCUSDT.
    let market_events = candle_market_data();
    let market_data = MarketDataInMemory::new(Arc::new(market_events));
    let time_engine_start = market_data.time_first_event().await.unwrap();

    // EngineState parameterised with the custom candle state (the default state
    // would silently ignore every DataKind::Candle event).
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        CandleInstrumentData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    // DefaultStrategy/DefaultRiskManager are no-ops — this example demonstrates the
    // candle → MarketEvent → engine wiring and the custom state, not a strategy.
    let args_dynamic = BacktestArgsDynamic {
        id: "candle-backtest-demo".into(),
        risk_free_return,
        strategy: DefaultStrategy::<EngineState<DefaultGlobalData, CandleInstrumentData>>::default(
        ),
        risk: DefaultRiskManager::<EngineState<DefaultGlobalData, CandleInstrumentData>>::default(),
    };

    let summary = backtest(args_constant, args_dynamic).await.unwrap();

    println!("\nBacktest complete (BacktestId = {})", summary.id);
    summary.trading_summary.print_summary();
}

/// Build a deterministic in-memory stream of 1-hour BTCUSDT candle events.
///
/// The key line is `time_exchange: candle.close_time` — the period-END instant the
/// engine clock orders on. Open time is never used for `time_exchange`.
fn candle_market_data() -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    // First candle opens just after the mock account's initial balance timestamp.
    let first_open: DateTime<Utc> = "2025-03-24T22:00:00Z".parse().unwrap();
    let step = IntervalStep::Fixed(Duration::hours(1));

    (0..48)
        .map(|i| {
            let open_time = first_open + Duration::hours(i);
            // Derive the exclusive period-end boundary via the shared helper, the
            // same way the library's candle producers do.
            let close_time = close_time_from_open(open_time, step)
                .expect("1h candle boundary is well within DateTime<Utc> range");

            // A gentle deterministic drift so the demo has price movement.
            let close = Decimal::from(60_000) + Decimal::from(i * 25);
            let candle = Candle {
                close_time,
                open: close - Decimal::from(10),
                high: close + Decimal::from(20),
                low: close - Decimal::from(20),
                close,
                volume: Decimal::from(5),
                trade_count: 100,
            };

            MarketStreamEvent::Item(MarketEvent {
                // Period END — see the module docs. Stamping `open_time` here would
                // be lookahead.
                time_exchange: candle.close_time,
                // Synthetic: an in-memory backtest has no real receipt instant.
                // A live producer would stamp local wall-clock here (e.g. `Utc::now()`);
                // only `time_exchange` drives clock/replay ordering.
                time_received: candle.close_time,
                exchange: ExchangeId::BinanceSpot,
                instrument: InstrumentIndex::new(0),
                kind: DataKind::Candle(candle),
            })
        })
        .collect()
}

pub fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("Failed to open config file");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect("Failed to parse config file")
}

/// Minimal candle-consuming [`InstrumentDataState`].
///
/// Tracks the most recent [`Candle`] and exposes its `close` as the instrument
/// price. A real strategy would maintain rolling windows, indicators, etc. — the
/// point here is that consuming `DataKind::Candle` requires a custom state because
/// the default one ignores it.
#[derive(Debug, Clone, Default)]
pub struct CandleInstrumentData {
    pub last_candle: Option<Candle>,
}

impl InstrumentDataState for CandleInstrumentData {
    type MarketEventKind = DataKind;

    fn price(&self) -> Option<Decimal> {
        self.last_candle.as_ref().map(|candle| candle.close)
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>> for CandleInstrumentData {
    type Audit = ();

    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        if let DataKind::Candle(candle) = &event.kind {
            self.last_candle = Some(*candle);
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey>
    Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>> for CandleInstrumentData
{
    type Audit = ();

    fn process(&mut self, _: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self::Audit {}
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey>
    for CandleInstrumentData
{
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {}
}
