#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! A runner driving several simulated venues hands each venue only its own exchange's market.
//!
//! Two exchanges list an instrument under the same exchange-native name, so a venue that matched
//! events by name alone would trade on the other exchange's prices.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent,
    engine::{
        Engine,
        audit::EngineAudit,
        clock::HistoricalClock,
        command::Command,
        in_flight::InFlightDeadlines,
        state::{
            EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
            instrument::data::DefaultInstrumentMarketData, trading::TradingState,
        },
    },
    execution::{
        AccountStreamEvent,
        sim::{
            SimEngine, SimExecutionBuild, SimExecutionBuilder, SimStart, SimStepper,
            SimVenueOptions, StartBalance,
        },
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
    AccountEvent, AccountEventKind, AccountSnapshot,
    client::mock::MockExecutionConfig,
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestOpen, RequestOpen},
    },
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

const FIRST: ExchangeId = ExchangeId::BinanceSpot;
const SECOND: ExchangeId = ExchangeId::Kraken;

type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type TestEngine = SimEngine<
    DefaultGlobalData,
    DefaultInstrumentMarketData,
    DefaultStrategy<State>,
    DefaultRiskManager<State>,
>;

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn config(exchange: ExchangeId) -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: exchange,
        initial_state: AccountSnapshot {
            exchange,
            balances: vec![],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: Default::default(),
        fill_model: Default::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: None,
        fee_asset: None,
    }
}

fn usdt() -> SimStart {
    SimStart::new(
        vec![StartBalance::new(
            AssetNameExchange::new("usdt"),
            dec!(10_000),
        )],
        vec![],
    )
}

/// A one-second candle on `instrument` of `exchange`, closing at `secs` at `close`.
fn bar(
    exchange: ExchangeId,
    instrument: usize,
    secs: i64,
    close: Decimal,
) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: at(secs),
        time_received: at(secs),
        exchange,
        instrument: InstrumentIndex::new(instrument),
        kind: DataKind::Candle(Candle {
            close_time: at(secs),
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(10)),
            trade_count: Some(5),
        }),
    }))
}

/// A resting limit buy of one unit at `limit` on the venue at `exchange`.
fn resting_buy(exchange: usize, instrument: usize, limit: Decimal, cid: &str) -> Command {
    Command::SendOpenRequests(OneOrMany::One(OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex::new(exchange),
            instrument: InstrumentIndex::new(instrument),
            strategy: StrategyId::new("agent"),
            cid: ClientOrderId::new(cid),
        },
        state: RequestOpen::new(
            Side::Buy,
            Some(limit),
            dec!(1),
            OrderKind::Limit,
            TimeInForce::GoodUntilCancelled { post_only: false },
        ),
    }))
}

/// The instrument of each trade among the audits' events.
fn traded(audits: &[EngineAudit<EngineEvent<DataKind>, impl Sized>]) -> Vec<InstrumentIndex> {
    audits
        .iter()
        .filter_map(|audit| match audit {
            EngineAudit::Process(process) => match &process.event {
                EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::Trade(trade),
                    ..
                })) => Some(trade.instrument),
                _ => None,
            },
            EngineAudit::FeedEnded => None,
        })
        .collect()
}

#[test]
fn a_tick_fills_only_the_venue_on_its_own_exchange() {
    // Instrument 0 is on the first exchange and 1 on the second, both named `btcusdt` there.
    let instruments = IndexedInstruments::new([
        instrument(FIRST, "btc", "usdt"),
        instrument(SECOND, "btc", "usdt"),
    ]);
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(config(FIRST), SimVenueOptions::default(), usdt())
        .unwrap()
        .add_venue_with_start(config(SECOND), SimVenueOptions::default(), usdt())
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
    let mut engine: TestEngine = Engine::new(
        clock.clone(),
        state,
        execution_tx_map,
        InFlightDeadlines::default(),
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );
    let mut stepper = SimStepper::new(venues, clock).unwrap();

    stepper
        .step(
            &mut engine,
            at(3),
            [bar(FIRST, 0, 3, dec!(100)), bar(SECOND, 1, 3, dec!(100))],
        )
        .unwrap();
    engine.action(&resting_buy(0, 0, dec!(95), "first"));
    engine.action(&resting_buy(1, 1, dec!(95), "second"));
    let resting = stepper.settle(&mut engine).unwrap();
    assert!(
        traded(&resting).is_empty(),
        "neither limit is marketable yet"
    );

    // Only the second exchange's price falls through the limit.
    let crossed = stepper
        .step(&mut engine, at(6), [bar(SECOND, 1, 6, dec!(94))])
        .unwrap();
    assert_eq!(traded(&crossed), vec![InstrumentIndex::new(1)]);

    // The first exchange's own tick then fills its own order, which was left untouched.
    let crossed = stepper
        .step(&mut engine, at(9), [bar(FIRST, 0, 9, dec!(94))])
        .unwrap();
    assert_eq!(traded(&crossed), vec![InstrumentIndex::new(0)]);
}
