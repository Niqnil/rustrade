#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Drive a simulation from outside, one bar at a time, as a gym-style reinforcement-learning loop
//! does: feed a bar, observe the `Engine`'s state, act, and feed the next.
//!
//! The account opens holding 0.5 BTC bought at 60,000, plus 10,000 USDT. On each bar the "policy"
//! below decides from the observed state, and its orders are filled at that bar's close before the
//! next bar is fed. That same-bar fill is an optimistic idealisation: see `SimStepper`'s rustdoc.
//! Setting `MockExecutionConfig::market_order_timing` to `MarketOrderTiming::NextBar` holds each
//! order for the next bar instead, the pessimistic comparison.

use chrono::{DateTime, Utc};
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
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            position::PositionSeed,
            trading::TradingState,
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
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestOpen, RequestOpen},
    },
};
use rustrade_instrument::{
    Side, Underlying,
    asset::name::AssetNameExchange,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex},
};
use rustrade_integration::collection::one_or_many::OneOrMany;

type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type AgentEngine = SimEngine<
    DefaultGlobalData,
    DefaultInstrumentMarketData,
    DefaultStrategy<State>,
    DefaultRiskManager<State>,
>;

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
const BTC: InstrumentIndex = InstrumentIndex(0);

fn main() {
    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::spot(
            EXCHANGE,
            "binance_spot-btc_usdt",
            "btc_usdt",
            Underlying::new("btc", "usdt"),
            None,
        ))
        .build();

    // One starting state for both sides: the venue holds the BTC as a balance, the Engine as a
    // position with its entry price.
    let start = SimStart::new(
        vec![StartBalance::new(
            AssetNameExchange::new("usdt"),
            dec!(10_000),
        )],
        vec![PositionSeed::new(
            "binance_spot-btc_usdt",
            Side::Buy,
            dec!(0.5),
            dec!(60_000),
            time(0),
        )],
    );

    let (mut engine, mut stepper) = episode(&instruments, start);

    let closes = [dec!(61_000), dec!(60_500), dec!(62_000), dec!(61_200)];
    for (n, close) in closes.into_iter().enumerate() {
        let instant = time(3 * (n as i64 + 1));

        // Feed every bar of this instant. When it returns, everything due by the bar is booked.
        stepper
            .step(&mut engine, instant, [bar(instant, close)])
            .unwrap();

        // Observe, and act.
        let holding = btc_position(&engine);
        let side = match holding {
            Some(_) if close > dec!(61_500) => Some(Side::Sell),
            None if close < dec!(61_500) => Some(Side::Buy),
            _ => None,
        };
        if let Some(side) = side {
            engine.action(&market_order(side, dec!(0.5), n));
            // Book the action at this bar's close, before the next bar exists.
            let audits = stepper.settle(&mut engine).unwrap();
            println!(
                "{instant}: {side:?} 0.5 BTC at {close}, {} events",
                audits.len()
            );
        }

        println!(
            "{instant}: close {close}, position {:?}, usdt {}",
            btc_position(&engine),
            balance(&engine, "usdt"),
        );
    }

    stepper.finish(&mut engine).unwrap();
}

/// Build the venue, the Engine and the stepper for one episode. A reset is another call.
fn episode(
    instruments: &IndexedInstruments,
    start: SimStart,
) -> (AgentEngine, SimStepper<DataKind>) {
    let build = SimExecutionBuilder::new(instruments)
        .add_venue_with_start(
            MockExecutionConfig {
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
                slippage: Default::default(),
                market_order_timing: Default::default(),
            },
            SimVenueOptions::default(),
            start,
        )
        .unwrap()
        .build();
    let seeds = build.position_seeds();
    let SimExecutionBuild {
        execution_tx_map,
        venues,
    } = build;

    let clock = HistoricalClock::new(time(0));
    let state = EngineStateBuilder::new(instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time(0))
    .trading_state(TradingState::Enabled)
    .positions(seeds)
    .build();

    let engine = Engine::new(
        clock.clone(),
        state,
        execution_tx_map,
        InFlightDeadlines::default(),
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );
    let stepper = SimStepper::new(venues, clock).unwrap();

    (engine, stepper)
}

fn time(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_760_000_000 + secs, 0).unwrap()
}

/// A three-second candle closing at `close_time`, stamped at its close.
fn bar(close_time: DateTime<Utc>, close: Decimal) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: close_time,
        time_received: close_time,
        exchange: EXCHANGE,
        instrument: BTC,
        kind: DataKind::Candle(Candle {
            close_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }),
    }))
}

fn market_order(side: Side, quantity: Decimal, n: usize) -> Command {
    Command::SendOpenRequests(OneOrMany::One(OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: BTC,
            strategy: StrategyId::new("agent"),
            cid: ClientOrderId::new(format!("agent-{n}")),
        },
        state: RequestOpen {
            side,
            price: None,
            quantity,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            position_id: None,
            reduce_only: false,
            market: None,
        },
    }))
}

fn btc_position(engine: &AgentEngine) -> Option<(Side, Decimal, Decimal)> {
    let state = engine
        .state
        .instruments
        .instruments(&InstrumentFilter::None)
        .next()?;
    state.position.positions.values().next().map(|position| {
        (
            position.side,
            position.quantity_abs,
            position.price_entry_average,
        )
    })
}

fn balance(engine: &AgentEngine, asset: &str) -> Decimal {
    engine
        .state
        .assets
        .assets()
        .find(|state| state.asset.name_internal.as_ref() == asset)
        .and_then(|state| state.balance.as_ref())
        .map(|balance| balance.value.total)
        .unwrap_or_default()
}
