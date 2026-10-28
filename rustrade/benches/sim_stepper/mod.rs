#![allow(clippy::unwrap_used, clippy::expect_used)] // Benchmark code: panics acceptable

//! The per-step cost of driving one simulated venue bar by bar with a `SimStepper`, as a
//! backtest or RL loop does: one spot instrument on a cross margin account with hourly interest,
//! zero latency, fixed slippage.
//!
//! - `hold`: each step shows the engine a bar and nothing else.
//! - `round_trip`: each step shows a bar, then sends one market order (buying and selling in
//!   turn) and settles it, so every step fills.
//! - `build`: a fresh session, which every episode needs.
//!
//! Each `hold` and `round_trip` iteration runs `STEPS` one-second bars on a fresh session,
//! building it excluded. `build` times the construction alone.

use chrono::{DateTime, TimeDelta, Utc};
use criterion::{BatchSize, Criterion, Throughput};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent,
    engine::{
        Engine,
        clock::HistoricalClock,
        command::Command,
        in_flight::InFlightDeadlines,
        state::{
            EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
            instrument::data::DefaultInstrumentMarketData, trading::TradingState,
        },
    },
    execution::sim::{
        SimEngine, SimExecutionBuild, SimExecutionBuilder, SimStart, SimStepper, SimVenueOptions,
        StartBalance,
    },
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::candle::Candle,
};
use rustrade_execution::{
    AccountSnapshot,
    client::mock::MockExecutionConfig,
    holding_cost::{DayCount, RateSeries},
    margin::{MarginEffect, PeriodicMarginInterest, SpotMarginConfig},
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestOpen, RequestOpen},
    },
    slippage::SlippageConfig,
};
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
    test_utils::instrument,
};
use rustrade_integration::collection::one_or_many::OneOrMany;
use std::{hint::black_box, sync::Arc};

criterion::criterion_main!(benchmark_sim_stepper);

criterion::criterion_group!(
    name = benchmark_sim_stepper;
    config = Criterion::default();
    targets = bench_steps, bench_build
);

const EXCHANGE: ExchangeId = ExchangeId::Mock;
const STEPS: usize = 100;
/// A UTC midnight, so `STEPS` one-second bars after it cross no interest boundary.
const START_SECS: i64 = 86_400 * 20_000;

type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type BenchEngine = SimEngine<
    DefaultGlobalData,
    DefaultInstrumentMarketData,
    DefaultStrategy<State>,
    DefaultRiskManager<State>,
>;

struct Session {
    engine: BenchEngine,
    stepper: SimStepper<DataKind>,
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(START_SECS + secs, 0).unwrap()
}

fn session() -> Session {
    let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usd")]);
    let config = MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: Default::default(),
        fill_model: Default::default(),
        spot_fee_asset: Default::default(),
        slippage: SlippageConfig::fixed(dec!(0.01)).unwrap(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: Some(
            SpotMarginConfig::new(
                AssetNameExchange::new("usd"),
                dec!(3),
                MarginEffect::NoSideEffect,
            )
            .unwrap(),
        ),
        fee_asset: None,
    };
    let interest = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365).with_rates(
        AssetNameExchange::new("btc"),
        RateSeries::constant(dec!(0.08)),
    );
    let options = SimVenueOptions::default().with_margin_interest(Arc::new(interest));
    let start = SimStart::new(
        vec![StartBalance::new(
            AssetNameExchange::new("usd"),
            dec!(10000),
        )],
        vec![],
    );

    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(config, options, start)
        .unwrap()
        .build();
    let seeds = build.position_seeds();
    let SimExecutionBuild {
        execution_tx_map,
        venues,
    } = build;

    let clock = HistoricalClock::new(at(0));
    let state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(at(0))
    .trading_state(TradingState::Enabled)
    .positions(seeds)
    .build();

    Session {
        engine: Engine::new(
            clock.clone(),
            state,
            execution_tx_map,
            InFlightDeadlines::default(),
            DefaultStrategy::default(),
            DefaultRiskManager::default(),
        ),
        stepper: SimStepper::new(venues, clock).unwrap(),
    }
}

fn bar(secs: i64) -> EngineEvent<DataKind> {
    let close = dec!(30000) + Decimal::from(secs % 7) * dec!(10);
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: at(secs),
        time_received: at(secs),
        exchange: EXCHANGE,
        instrument: InstrumentIndex::new(0),
        kind: DataKind::Candle(Candle {
            close_time: at(secs),
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(100)),
            trade_count: Some(0),
        }),
    }))
}

fn order(side: Side, step: usize) -> Command {
    Command::SendOpenRequests(OneOrMany::One(OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex::new(0),
            instrument: InstrumentIndex::new(0),
            strategy: StrategyId::new("bench"),
            cid: ClientOrderId::new(step.to_string()),
        },
        state: RequestOpen::new(
            side,
            None,
            dec!(0.1),
            OrderKind::Market,
            TimeInForce::ImmediateOrCancel,
        ),
    }))
}

/// Run `STEPS` bars, trading on each when `round_trip`, and return the audits delivered.
fn run(session: &mut Session, round_trip: bool) -> usize {
    let mut audits = 0;
    for step in 1..=STEPS {
        audits += session
            .stepper
            .step(&mut session.engine, at(step as i64), [bar(step as i64)])
            .unwrap()
            .len();
        if round_trip {
            let side = if step % 2 == 1 { Side::Buy } else { Side::Sell };
            session.engine.action(&order(side, step));
            audits += session.stepper.settle(&mut session.engine).unwrap().len();
        }
    }
    audits
}

fn bench_steps(c: &mut Criterion) {
    // A round trip that never fills would time nothing but refusals.
    let mut probe = session();
    run(&mut probe, true);
    let flat = probe
        .engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex::new(0))
        .position
        .positions
        .is_empty();
    assert!(flat, "the round trips must close flat");
    let closed = probe
        .engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex::new(0))
        .tear_sheet
        .pnl_returns
        .total
        .count;
    assert_eq!(
        closed,
        Decimal::from(STEPS / 2),
        "every round trip must close a position into the tear sheet"
    );
    let usd = probe
        .engine
        .state
        .assets
        .0
        .values()
        .find(|asset| asset.asset.name_exchange.name() == "usd")
        .and_then(|asset| asset.balance.as_ref())
        .unwrap()
        .value
        .total;
    assert!(
        usd < dec!(10000),
        "the round trips must pay slippage: {usd}"
    );
    assert_eq!(
        probe.engine.state.trading,
        TradingState::Enabled,
        "the session must still be trading"
    );

    for (name, round_trip) in [("hold", false), ("round_trip", true)] {
        let mut group = c.benchmark_group(format!("sim_stepper/{name}"));
        group.throughput(Throughput::Elements(STEPS as u64));
        group.bench_function("steps", |b| {
            b.iter_batched_ref(
                session,
                |session| black_box(run(session, round_trip)),
                BatchSize::SmallInput,
            )
        });
        group.finish();
    }
}

fn bench_build(c: &mut Criterion) {
    c.bench_function("sim_stepper/build", |b| b.iter(|| black_box(session())));
}
