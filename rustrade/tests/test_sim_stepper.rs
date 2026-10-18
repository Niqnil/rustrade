#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! `SimStepper` drives the simulated venues one instant at a time, from the caller's loop, and
//! settles everything due at an instant before it returns.
//!
//! Each test pins one guarantee a gym-style caller relies on: an order sent after observing a bar
//! fills at that bar, before the next exists; several settles at one instant each see the
//! previous fill; a refused order is answered in the same call; an account that opens holding
//! positions holds them on both sides; and stepping books exactly what a backtest over the same
//! data books.

use std::sync::Mutex;

use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use futures::{StreamExt, stream};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent, SplitRoundingPolicy, Timed,
    engine::{
        Engine,
        audit::EngineAudit,
        clock::HistoricalClock,
        command::Command,
        execution_tx::MultiExchangeTxMap,
        in_flight::InFlightDeadlines,
        run::async_run,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            position::PositionSeed,
            trading::TradingState,
        },
    },
    error::BarterError,
    execution::{
        AccountStreamEvent,
        sim::{
            SimEngine, SimExecutionBuild, SimExecutionBuilder, SimRunner, SimStart, SimStartError,
            SimStepError, SimStepper, SimVenueOptions, StartBalance,
        },
    },
    risk::DefaultRiskManager,
    shutdown::{Shutdown, SyncShutdown},
    strategy::{
        DefaultStrategy, algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::candle::Candle,
};
use rustrade_execution::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    fill::{MarketOrderTiming, NextBarPrice},
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
        state::{InactiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side,
    asset::{AssetIndex, name::AssetNameExchange},
    corporate_action::{CorporateActionKind, SplitRatio},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        InstrumentIndex,
        kind::{InstrumentKind, cfd::CfdContract},
        name::InstrumentNameExchange,
    },
    test_utils::instrument,
};
use rustrade_integration::collection::one_or_many::OneOrMany;

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
const BTC: usize = 0;
const ETH: usize = 1;
const SPX: usize = 2;

type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type TestEngine<Strategy> =
    SimEngine<DefaultGlobalData, DefaultInstrumentMarketData, Strategy, DefaultRiskManager<State>>;

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn instruments() -> IndexedInstruments {
    let mut spx = instrument(EXCHANGE, "spx500", "usdt");
    spx.kind = InstrumentKind::Cfd(CfdContract {
        contract_size: dec!(1),
        settlement_asset: spx.underlying.quote.clone(),
    });
    IndexedInstruments::new([
        instrument(EXCHANGE, "btc", "usdt"),
        instrument(EXCHANGE, "eth", "usdt"),
        spx,
    ])
}

fn config(latency_ms: u64) -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![],
            instruments: vec![],
        },
        latency_ms,
        fee_model: Default::default(),
        fill_model: Default::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
    }
}

fn usdt(total: Decimal) -> SimStart {
    SimStart::new(
        vec![StartBalance::new(AssetNameExchange::new("usdt"), total)],
        vec![],
    )
}

/// A one-second candle on `instrument` closing at `secs`, at `close`, stamped at its close.
fn bar(instrument: usize, secs: i64, close: Decimal) -> EngineEvent<DataKind> {
    bar_from(instrument, secs, close, close)
}

/// A one-second candle on `instrument` closing at `secs` that opened at `open`.
fn bar_from(instrument: usize, secs: i64, open: Decimal, close: Decimal) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: at(secs),
        time_received: at(secs),
        exchange: EXCHANGE,
        instrument: InstrumentIndex::new(instrument),
        kind: DataKind::Candle(Candle {
            close_time: at(secs),
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            volume: Some(dec!(10)),
            trade_count: Some(5),
        }),
    }))
}

fn order(instrument: usize, side: Side, quantity: Decimal, cid: &str) -> Command {
    Command::SendOpenRequests(OneOrMany::One(OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex::new(0),
            instrument: InstrumentIndex::new(instrument),
            strategy: StrategyId::new("agent"),
            cid: ClientOrderId::new(cid),
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

/// One simulated spot venue trading btc and eth against usdt, the `Engine` on it, and the stepper.
struct Session<Strategy> {
    engine: TestEngine<Strategy>,
    stepper: SimStepper<DataKind>,
}

/// Build a session whose venue starts from `start`, and whose `Engine` is seeded with
/// `engine_positions`, or with the venue's own seeds when `None`.
fn session_with<Strategy>(
    latency_ms: u64,
    start: SimStart,
    engine_positions: Option<Vec<PositionSeed>>,
    strategy: Strategy,
) -> Session<Strategy> {
    session_configured(config(latency_ms), start, engine_positions, strategy)
}

/// [`session_with`], for a venue configured by `config`.
fn session_configured<Strategy>(
    config: MockExecutionConfig,
    start: SimStart,
    engine_positions: Option<Vec<PositionSeed>>,
    strategy: Strategy,
) -> Session<Strategy> {
    let instruments = instruments();
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(config, SimVenueOptions::default(), start)
        .unwrap()
        .build();
    let seeds = engine_positions.unwrap_or_else(|| build.position_seeds());
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
            strategy,
            DefaultRiskManager::default(),
        ),
        stepper: SimStepper::new(venues, clock).unwrap(),
    }
}

fn session(latency_ms: u64, start: SimStart) -> Session<DefaultStrategy<State>> {
    session_with(latency_ms, start, None, DefaultStrategy::default())
}

/// The event behind each audit, in delivery order.
fn events<Output>(
    audits: &[EngineAudit<EngineEvent<DataKind>, Output>],
) -> Vec<&EngineEvent<DataKind>> {
    audits
        .iter()
        .filter_map(|audit| match audit {
            EngineAudit::Process(process) => Some(&process.event),
            EngineAudit::FeedEnded => None,
        })
        .collect()
}

/// `(instrument, side, price, quantity, time)` of each trade among `events`.
type Fill = (InstrumentIndex, Side, Decimal, Decimal, DateTime<Utc>);

fn fills<'a>(events: impl IntoIterator<Item = &'a EngineEvent<DataKind>>) -> Vec<Fill> {
    events
        .into_iter()
        .filter_map(|event| match event {
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::Trade(trade),
                ..
            })) => Some((
                trade.instrument,
                trade.side,
                trade.price,
                trade.quantity,
                trade.time_exchange,
            )),
            _ => None,
        })
        .collect()
}

/// The `ClientOrderId` of each order the venue refused, among `events`.
fn refused<'a>(events: impl IntoIterator<Item = &'a EngineEvent<DataKind>>) -> Vec<ClientOrderId> {
    events
        .into_iter()
        .filter_map(|event| match event {
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::OrderSnapshot(snapshot),
                ..
            })) if matches!(
                snapshot.0.state,
                OrderState::Inactive(InactiveOrderState::OpenFailed(_))
            ) =>
            {
                Some(snapshot.0.key.cid.clone())
            }
            _ => None,
        })
        .collect()
}

fn balance<Strategy>(engine: &TestEngine<Strategy>, asset: &str) -> Decimal {
    engine
        .state
        .assets
        .assets()
        .find(|state| state.asset.name_internal.as_ref() == asset)
        .and_then(|state| state.balance.as_ref())
        .map(|balance| balance.value.total)
        .unwrap_or_default()
}

/// `(side, quantity, entry price)` of the `Engine`'s position in `instrument`, if it holds one.
fn position<Strategy>(
    engine: &TestEngine<Strategy>,
    instrument: usize,
) -> Option<(Side, Decimal, Decimal)> {
    engine
        .state
        .instruments
        .instruments(&InstrumentFilter::None)
        .nth(instrument)
        .and_then(|state| state.position.positions.values().next())
        .map(|position| {
            (
                position.side,
                position.quantity_abs,
                position.price_entry_average,
            )
        })
}

fn price<Strategy>(engine: &TestEngine<Strategy>, instrument: usize) -> Option<Decimal> {
    engine
        .state
        .instruments
        .instruments(&InstrumentFilter::None)
        .nth(instrument)
        .and_then(|state| state.data.price())
}

#[test]
fn an_order_sent_after_a_step_fills_at_that_bars_close_before_the_next_bar_exists() {
    let mut s = session(0, usdt(dec!(10_000)));
    assert_eq!(s.stepper.instant(), None);
    assert_eq!(s.stepper.venues().len(), 1);

    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    assert_eq!(s.stepper.instant(), Some(at(3)));
    assert!(!s.stepper.has_ended());
    // A candle moves the Engine's mark, which a caller values its portfolio at.
    assert_eq!(price(&s.engine, BTC), Some(dec!(100)));

    s.engine.action(&order(BTC, Side::Buy, dec!(2), "buy"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(
        fills(events(&settled)),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(100),
            dec!(2),
            at(3)
        )]
    );
    assert_eq!(
        position(&s.engine, BTC),
        Some((Side::Buy, dec!(2), dec!(100)))
    );
    assert_eq!(balance(&s.engine, "usdt"), dec!(9800));
    assert_eq!(balance(&s.engine, "btc"), dec!(2));
}

#[test]
fn settles_at_one_instant_each_fill_at_its_close_and_see_the_fills_before_them() {
    let mut s = session(0, usdt(dec!(1000)));

    s.stepper
        .step(
            &mut s.engine,
            at(3),
            [bar(BTC, 3, dec!(100)), bar(ETH, 3, dec!(50))],
        )
        .unwrap();

    s.engine.action(&order(BTC, Side::Buy, dec!(6), "a"));
    let first = s.stepper.settle(&mut s.engine).unwrap();
    let after_first = balance(&s.engine, "usdt");

    // Sized from what the first fill left: all of it.
    let quantity = after_first / dec!(50);
    s.engine.action(&order(ETH, Side::Buy, quantity, "b"));
    let second = s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(after_first, dec!(400));
    assert_eq!(
        fills(events(&first)),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(100),
            dec!(6),
            at(3)
        )]
    );
    assert_eq!(
        fills(events(&second)),
        vec![(
            InstrumentIndex::new(ETH),
            Side::Buy,
            dec!(50),
            dec!(8),
            at(3)
        )]
    );
    assert_eq!(balance(&s.engine, "usdt"), Decimal::ZERO);
}

#[test]
fn an_order_left_for_the_next_step_fills_at_the_close_it_was_sent_after() {
    let mut s = session(0, usdt(dec!(10_000)));

    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "buy"));
    let next = s
        .stepper
        .step(&mut s.engine, at(6), [bar(BTC, 6, dec!(110))])
        .unwrap();

    let next = events(&next);
    assert_eq!(
        fills(next.iter().copied()),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(100),
            dec!(1),
            at(3)
        )]
    );
    // Booked before the bar that followed it, which then marks it.
    let trade_at = next
        .iter()
        .position(|event| !fills([*event]).is_empty())
        .unwrap();
    let bar_at = next
        .iter()
        .position(|event| matches!(event, EngineEvent::Market(_)))
        .unwrap();
    assert!(trade_at < bar_at);
    assert_eq!(price(&s.engine, BTC), Some(dec!(110)));
}

#[test]
fn a_step_matches_resting_orders_on_every_instrument_before_returning() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(
            &mut s.engine,
            at(3),
            [bar(BTC, 3, dec!(100)), bar(ETH, 3, dec!(50))],
        )
        .unwrap();

    for (instrument, limit, cid) in [(BTC, dec!(95), "btc"), (ETH, dec!(45), "eth")] {
        let Command::SendOpenRequests(OneOrMany::One(mut request)) =
            order(instrument, Side::Buy, dec!(1), cid)
        else {
            unreachable!()
        };
        request.state.kind = OrderKind::Limit;
        request.state.price = Some(limit);
        request.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        s.engine
            .action(&Command::SendOpenRequests(OneOrMany::One(request)));
    }
    let resting = s.stepper.settle(&mut s.engine).unwrap();
    assert!(
        fills(events(&resting)).is_empty(),
        "neither limit is marketable yet"
    );

    let crossed = s
        .stepper
        .step(
            &mut s.engine,
            at(6),
            [bar(ETH, 6, dec!(44)), bar(BTC, 6, dec!(94))],
        )
        .unwrap();

    let mut crossed = fills(events(&crossed));
    crossed.sort();
    assert_eq!(
        crossed,
        vec![
            // A resting limit fills at its limit, not at the bar that crossed it.
            (
                InstrumentIndex::new(BTC),
                Side::Buy,
                dec!(95),
                dec!(1),
                at(6)
            ),
            (
                InstrumentIndex::new(ETH),
                Side::Buy,
                dec!(45),
                dec!(1),
                at(6)
            ),
        ]
    );
    assert!(position(&s.engine, BTC).is_some() && position(&s.engine, ETH).is_some());
}

#[test]
fn a_refused_order_is_answered_in_the_same_settle() {
    let mut s = session(0, usdt(dec!(100)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    s.engine.action(&order(BTC, Side::Buy, dec!(5), "too-big"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(
        refused(events(&settled)),
        vec![ClientOrderId::new("too-big")]
    );
    assert_eq!(position(&s.engine, BTC), None);
}

#[test]
fn an_instant_may_carry_bars_for_only_some_instruments() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(
            &mut s.engine,
            at(3),
            [bar(BTC, 3, dec!(100)), bar(ETH, 3, dec!(50))],
        )
        .unwrap();
    s.stepper
        .step(&mut s.engine, at(6), [bar(BTC, 6, dec!(110))])
        .unwrap();

    assert_eq!(price(&s.engine, BTC), Some(dec!(110)));
    assert_eq!(price(&s.engine, ETH), Some(dec!(50)));

    // The documented gap: an instrument with no bar at this instant fills at its last price.
    s.engine.action(&order(ETH, Side::Buy, dec!(1), "stale"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();
    assert_eq!(
        fills(events(&settled)),
        vec![(
            InstrumentIndex::new(ETH),
            Side::Buy,
            dec!(50),
            dec!(1),
            at(6)
        )]
    );
}

#[test]
fn a_market_order_on_an_instrument_never_priced_is_refused() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    s.engine.action(&order(ETH, Side::Buy, dec!(1), "unpriced"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(
        refused(events(&settled)),
        vec![ClientOrderId::new("unpriced")]
    );
}

#[test]
fn latency_delays_a_fill_to_the_first_step_that_reaches_it() {
    // One second each way.
    let mut s = session(2000, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    s.engine.action(&order(BTC, Side::Buy, dec!(1), "late"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();
    assert!(
        fills(events(&settled)).is_empty(),
        "it reaches the venue at 4s"
    );

    let next = s
        .stepper
        .step(&mut s.engine, at(6), [bar(BTC, 6, dec!(110))])
        .unwrap();

    // Booked at 4s against the market as of then, the 3s bar; seen at 5s, before the 6s bar.
    let next = events(&next);
    assert_eq!(
        fills(next.iter().copied()),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(100),
            dec!(1),
            at(4)
        )]
    );
    assert!(matches!(next.last(), Some(EngineEvent::Market(_))));
}

/// A session whose venue holds each market order for its instrument's next bar.
fn next_bar_session(start: SimStart) -> Session<DefaultStrategy<State>> {
    let config = MockExecutionConfig {
        market_order_timing: MarketOrderTiming::NextBar(NextBarPrice::Open),
        ..config(0)
    };
    session_configured(config, start, None, DefaultStrategy::default())
}

#[test]
fn under_next_bar_an_order_fills_at_the_next_bars_open_before_that_bar_is_shown() {
    let mut s = next_bar_session(usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    s.engine.action(&order(BTC, Side::Buy, dec!(2), "next"));
    let settled = s.stepper.settle(&mut s.engine).unwrap();
    assert!(
        fills(events(&settled)).is_empty(),
        "held for the next bar, so nothing fills at the close it was sent after"
    );
    assert!(refused(events(&settled)).is_empty());
    assert_eq!(position(&s.engine, BTC), None);

    let stepped = s
        .stepper
        .step(
            &mut s.engine,
            at(4),
            [bar_from(BTC, 4, dec!(102), dec!(105))],
        )
        .unwrap();
    let stepped = events(&stepped);

    assert_eq!(
        fills(stepped.iter().copied()),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(102),
            dec!(2),
            at(4)
        )],
        "filled at the next bar's open, stamped at that bar"
    );
    let trade_at = stepped
        .iter()
        .position(|event| {
            matches!(
                event,
                EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::Trade(_),
                    ..
                }))
            )
        })
        .unwrap();
    let bar_at = stepped
        .iter()
        .position(|event| matches!(event, EngineEvent::Market(_)))
        .unwrap();
    assert!(
        trade_at < bar_at,
        "the Engine sees the bar with the fill it released already booked"
    );
    assert_eq!(
        position(&s.engine, BTC),
        Some((Side::Buy, dec!(2), dec!(102)))
    );
    assert_eq!(balance(&s.engine, "usdt"), dec!(9796));
}

#[test]
fn under_next_bar_finish_expires_an_order_still_held() {
    let mut s = next_bar_session(usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "held"));
    s.stepper.settle(&mut s.engine).unwrap();

    let finished = s.stepper.finish(&mut s.engine).unwrap();

    assert!(fills(events(&finished)).is_empty());
    assert!(
        events(&finished).iter().any(|event| matches!(
            event,
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::OrderSnapshot(snapshot),
                ..
            })) if snapshot.0.key.cid == ClientOrderId::new("held")
                && matches!(snapshot.0.state, OrderState::Inactive(InactiveOrderState::Expired(_)))
        )),
        "the order is answered as expired: {:?}",
        events(&finished)
    );
    assert_eq!(
        balance(&s.engine, "usdt"),
        dec!(10_000),
        "and its hold released"
    );
    assert!(s.stepper.has_ended());
}

#[test]
fn finish_delivers_what_is_still_owed_and_ends_the_session() {
    let mut s = session(2000, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "owed"));

    let finished = s.stepper.finish(&mut s.engine).unwrap();

    assert_eq!(
        fills(events(&finished)),
        vec![(
            InstrumentIndex::new(BTC),
            Side::Buy,
            dec!(100),
            dec!(1),
            at(4)
        )]
    );
    assert!(s.stepper.has_ended());
    assert!(matches!(
        s.stepper.step(&mut s.engine, at(6), []),
        Err(BarterError::SimStep(SimStepError::Finished))
    ));
}

#[test]
fn an_error_ends_the_session_and_every_later_call_returns_it() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    let not_later = s
        .stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(101))])
        .unwrap_err();
    assert_eq!(
        not_later,
        BarterError::SimStep(SimStepError::NotLater {
            time: at(3),
            previous: at(3),
        })
    );
    assert_eq!(s.stepper.settle(&mut s.engine).unwrap_err(), not_later);
    assert!(s.stepper.has_ended());
}

#[test]
fn the_first_step_may_open_the_session_but_no_step_may_go_back() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(0), [bar(BTC, 0, dec!(100))])
        .unwrap();

    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(6), [bar(BTC, 6, dec!(100))])
        .unwrap();
    assert_eq!(
        s.stepper
            .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
            .unwrap_err(),
        BarterError::SimStep(SimStepError::NotLater {
            time: at(3),
            previous: at(6),
        })
    );
}

#[test]
fn a_corporate_action_effective_at_another_instant_is_refused() {
    let mut s = session(0, usdt(dec!(10_000)));
    let split = EngineEvent::CorporateAction {
        id: "split".into(),
        instrument: InstrumentIndex::new(BTC),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Fractional,
        effective_time: at(4),
    };

    assert_eq!(
        s.stepper.step(&mut s.engine, at(3), [split]).unwrap_err(),
        BarterError::SimStep(SimStepError::OffInstant {
            time: at(3),
            found: at(4),
        })
    );
}

#[test]
fn a_start_is_refused_for_a_configuration_that_already_has_an_opening_account() {
    let instruments = instruments();
    let with_balance = MockExecutionConfig {
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![AssetBalance {
                asset: AssetNameExchange::new("usdt"),
                balance: Balance::new(dec!(1), dec!(1)),
                time_exchange: at(0),
            }],
            instruments: vec![],
        },
        ..config(0)
    };
    let with_instrument = MockExecutionConfig {
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![],
            instruments: vec![InstrumentAccountSnapshot {
                instrument: InstrumentNameExchange::new("btc_usdt"),
                orders: vec![],
                orders_complete: false,
                position: Default::default(),
                isolated: None,
            }],
        },
        ..config(0)
    };

    for config in [with_balance, with_instrument] {
        assert_eq!(
            SimExecutionBuilder::new(&instruments)
                .add_venue_with_start(config, SimVenueOptions::default(), usdt(dec!(1)))
                .unwrap_err(),
            BarterError::SimStart(SimStartError::InitialStateNotEmpty { exchange: EXCHANGE })
        );
    }
}

#[test]
fn an_event_stamped_at_another_instant_is_refused() {
    let mut s = session(0, usdt(dec!(10_000)));

    assert_eq!(
        s.stepper
            .step(
                &mut s.engine,
                at(3),
                [bar(BTC, 3, dec!(100)), bar(ETH, 4, dec!(50))]
            )
            .unwrap_err(),
        BarterError::SimStep(SimStepError::OffInstant {
            time: at(3),
            found: at(4),
        })
    );
}

#[test]
fn an_engine_built_with_another_clock_is_refused() {
    let mut s = session(0, usdt(dec!(10_000)));
    let mut other = session(0, usdt(dec!(10_000)));

    assert_eq!(
        s.stepper
            .step(&mut other.engine, at(3), [bar(BTC, 3, dec!(100))])
            .unwrap_err(),
        BarterError::SimStep(SimStepError::WrongEngine)
    );
}

fn holding_btc() -> SimStart {
    SimStart::new(
        vec![StartBalance::new(
            AssetNameExchange::new("usdt"),
            dec!(1000),
        )],
        vec![PositionSeed::new(
            "binance_spot-btc_usdt",
            Side::Buy,
            dec!(2),
            dec!(90),
            at(0),
        )],
    )
}

#[test]
fn a_start_holding_a_spot_position_holds_it_on_both_sides_and_can_close_it() {
    let mut s = session(0, holding_btc());

    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    assert_eq!(
        position(&s.engine, BTC),
        Some((Side::Buy, dec!(2), dec!(90)))
    );
    assert_eq!(balance(&s.engine, "btc"), dec!(2));

    s.engine.action(&order(BTC, Side::Sell, dec!(2), "close"));
    s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(position(&s.engine, BTC), None);
    assert_eq!(balance(&s.engine, "btc"), Decimal::ZERO);
    assert_eq!(balance(&s.engine, "usdt"), dec!(1200));
}

#[test]
fn a_start_holding_a_cfd_closes_against_its_entry_and_pays_back_its_margin() {
    // The margin, the full notional at entry, is held by the position, not listed as a balance.
    let start = SimStart::new(
        vec![],
        vec![PositionSeed::new(
            "binance_spot-spx500_usdt",
            Side::Sell,
            dec!(1),
            dec!(5000),
            at(0),
        )],
    );
    let mut s = session(0, start);

    s.stepper
        .step(&mut s.engine, at(3), [bar(SPX, 3, dec!(4900))])
        .unwrap();
    assert_eq!(
        position(&s.engine, SPX),
        Some((Side::Sell, dec!(1), dec!(5000)))
    );

    s.engine.action(&order(SPX, Side::Buy, dec!(1), "close"));
    s.stepper.settle(&mut s.engine).unwrap();

    assert_eq!(position(&s.engine, SPX), None);
    assert_eq!(balance(&s.engine, "usdt"), dec!(5100));
}

#[test]
fn an_engine_not_seeded_with_the_venues_start_is_refused() {
    let mut s = session_with(0, holding_btc(), Some(vec![]), DefaultStrategy::default());

    let error = s
        .stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap_err();

    assert!(
        matches!(error, BarterError::SimStep(SimStepError::StartMismatch(_))),
        "{error}"
    );
}

#[test]
fn an_engine_seeded_at_another_quantity_or_entry_is_refused() {
    let held = |quantity, entry| {
        vec![PositionSeed::new(
            "binance_spot-btc_usdt",
            Side::Buy,
            quantity,
            entry,
            at(0),
        )]
    };

    for engine_positions in [held(dec!(1), dec!(90)), held(dec!(2), dec!(95))] {
        let mut s = session_with(
            0,
            holding_btc(),
            Some(engine_positions),
            DefaultStrategy::default(),
        );

        let error = s.stepper.settle(&mut s.engine).unwrap_err();
        assert!(
            matches!(error, BarterError::SimStep(SimStepError::StartMismatch(_))),
            "{error}"
        );
    }
}

#[test]
fn a_venue_whose_opening_account_it_cannot_index_is_refused() {
    let instruments = instruments();
    let unknown_asset = MockExecutionConfig {
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![AssetBalance {
                asset: AssetNameExchange::new("doge"),
                balance: Balance::new(dec!(1), dec!(1)),
                time_exchange: at(0),
            }],
            instruments: vec![],
        },
        ..config(0)
    };
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue(unknown_asset)
        .unwrap()
        .build();

    let error = SimStepper::<DataKind>::new(build.venues, HistoricalClock::new(at(0))).unwrap_err();

    assert!(
        matches!(
            &error,
            BarterError::SimStep(SimStepError::Misconfigured(reason))
                if reason.starts_with("SimStepper venue produced an unindexable AccountEvent")
        ),
        "{error}"
    );
}

#[test]
fn a_terminal_audit_ends_the_session() {
    let mut s = session(0, usdt(dec!(10_000)));

    let error = s
        .stepper
        .step(
            &mut s.engine,
            at(3),
            [EngineEvent::Shutdown(Shutdown::Immediate)],
        )
        .unwrap_err();

    assert!(
        matches!(error, BarterError::SimStep(SimStepError::EngineStopped(_))),
        "{error}"
    );
    assert!(s.stepper.has_ended());
}

#[test]
fn shutting_the_engine_down_fails_the_next_call_without_delivering_it() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    s.engine.shutdown();

    assert_eq!(
        s.stepper
            .step(&mut s.engine, at(6), [bar(BTC, 6, dec!(110))])
            .unwrap_err(),
        BarterError::SimStep(SimStepError::Finished)
    );
    assert_eq!(
        price(&s.engine, BTC),
        Some(dec!(100)),
        "the 6s bar is never shown"
    );
    assert!(s.stepper.has_ended());
    assert_eq!(
        s.stepper.settle(&mut s.engine).unwrap_err(),
        BarterError::SimStep(SimStepError::Finished)
    );

    // `settle` and `finish` observe it the same way.
    for finish in [false, true] {
        let mut s = session(0, usdt(dec!(10_000)));
        s.engine.shutdown();
        let result = match finish {
            false => s.stepper.settle(&mut s.engine),
            true => s.stepper.finish(&mut s.engine),
        };
        assert_eq!(
            result.unwrap_err(),
            BarterError::SimStep(SimStepError::Finished)
        );
    }
}

#[test]
fn a_shutdown_from_a_strategy_hook_fails_the_call_it_happens_in() {
    let mut s = session_with(0, usdt(dec!(10_000)), None, ShutsDownWhenDisabled);

    let error = s
        .stepper
        .step(
            &mut s.engine,
            at(3),
            [
                bar(BTC, 3, dec!(100)),
                EngineEvent::TradingStateUpdate(TradingState::Disabled),
            ],
        )
        .unwrap_err();

    // The trading state update is auxiliary, so it goes first, and the bar after it is never shown.
    assert_eq!(error, BarterError::SimStep(SimStepError::Finished));
    assert_eq!(price(&s.engine, BTC), None);
    assert!(s.stepper.has_ended());
}

/// Trades nothing, and shuts the `Engine` down when trading is disabled.
#[derive(Debug, Default)]
struct ShutsDownWhenDisabled;

impl AlgoStrategy for ShutsDownWhenDisabled {
    type State = State;

    fn generate_algo_orders(
        &self,
        _state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        (std::iter::empty(), std::iter::empty())
    }
}

impl ClosePositionsStrategy for ShutsDownWhenDisabled {
    type State = State;

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

impl OnDisconnectStrategy<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for ShutsDownWhenDisabled
{
    type OnDisconnect = ();

    fn on_disconnect(_: &mut TestEngine<Self>, _: ExchangeId) -> Self::OnDisconnect {}
}

impl OnTradingDisabled<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for ShutsDownWhenDisabled
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(engine: &mut TestEngine<Self>) -> Self::OnTradingDisabled {
        engine.shutdown();
    }
}

#[test]
fn a_stepper_debug_prints_its_progress() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();

    let debug = format!("{:?}", s.stepper);

    assert!(debug.starts_with("SimStepper"), "{debug}");
    assert!(
        debug.contains("instant: Some(1970-01-01T00:00:03Z)"),
        "{debug}"
    );
}

#[test]
fn a_settle_before_any_step_books_at_the_session_start() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "early"));

    let settled = s.stepper.settle(&mut s.engine).unwrap();

    // The opening account is delivered first, and the order is refused: nothing is priced yet.
    assert_eq!(balance(&s.engine, "usdt"), dec!(10_000));
    assert_eq!(refused(events(&settled)), vec![ClientOrderId::new("early")]);
    assert_eq!(s.stepper.instant(), None);
}

#[test]
fn a_step_with_no_events_delivers_a_deadline_it_reaches() {
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&Command::SendOpenRequests(OneOrMany::One(
        OrderRequestOpen {
            key: OrderKey {
                exchange: ExchangeIndex::new(0),
                instrument: InstrumentIndex::new(BTC),
                strategy: StrategyId::new("agent"),
                cid: ClientOrderId::new("gtd"),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: Some(dec!(90)),
                quantity: dec!(1),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodTillDate { expiry: at(5) },
                position_id: None,
                reduce_only: false,
                market: None,
            },
        },
    )));
    s.stepper.settle(&mut s.engine).unwrap();

    let stepped = s.stepper.step(&mut s.engine, at(6), []).unwrap();

    let expired = events(&stepped)
        .into_iter()
        .filter_map(|event| match event {
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::OrderSnapshot(snapshot),
                ..
            })) if matches!(
                snapshot.0.state,
                OrderState::Inactive(InactiveOrderState::Expired(_))
            ) =>
            {
                Some(snapshot.0.key.cid.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(expired, vec![ClientOrderId::new("gtd")]);
    assert_eq!(s.stepper.instant(), Some(at(6)));
}

#[test]
fn each_call_starts_a_fresh_feedback_budget() {
    // How many account events booking one market order delivers.
    let per_order = {
        let mut s = session(0, usdt(dec!(10_000)));
        s.stepper
            .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
            .unwrap();
        s.engine.action(&order(BTC, Side::Buy, dec!(1), "probe"));
        s.stepper.settle(&mut s.engine).unwrap().len()
    };

    // One order fits the budget, but two in one stretch would not, were it not reset per call.
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper = s.stepper.with_feedback_limit(per_order);
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "a"));
    s.stepper.settle(&mut s.engine).unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "b"));
    s.stepper.settle(&mut s.engine).unwrap();
    // A step with no market data resets it too.
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "c"));
    let stepped = s.stepper.step(&mut s.engine, at(6), []).unwrap();
    assert_eq!(fills(events(&stepped)).len(), 1);

    // And the limit is the one set.
    let mut s = session(0, usdt(dec!(10_000)));
    s.stepper = s.stepper.with_feedback_limit(per_order - 1);
    s.stepper
        .step(&mut s.engine, at(3), [bar(BTC, 3, dec!(100))])
        .unwrap();
    s.engine.action(&order(BTC, Side::Buy, dec!(1), "over"));
    let error = s.stepper.settle(&mut s.engine).unwrap_err();
    assert!(
        matches!(error, BarterError::SimFeedbackLoop { limit, .. } if limit == per_order - 1),
        "{error}"
    );
}

/// A whole session of caller actions, as one string, for comparing two runs.
fn scripted_run() -> String {
    let mut s = session(0, usdt(dec!(10_000)));
    let mut log = Vec::new();

    for (n, (btc, eth)) in [
        (dec!(100), dec!(50)),
        (dec!(104), dec!(48)),
        (dec!(99), dec!(51)),
    ]
    .into_iter()
    .enumerate()
    {
        let secs = 3 * (n as i64 + 1);
        log.push(format!(
            "{:?}",
            s.stepper
                .step(
                    &mut s.engine,
                    at(secs),
                    [bar(BTC, secs, btc), bar(ETH, secs, eth)]
                )
                .unwrap()
        ));
        let side = if n % 2 == 0 { Side::Buy } else { Side::Sell };
        s.engine
            .action(&order(BTC, side, dec!(1), &format!("btc-{n}")));
        log.push(format!("{:?}", s.stepper.settle(&mut s.engine).unwrap()));
        s.engine
            .action(&order(ETH, side, dec!(2), &format!("eth-{n}")));
        log.push(format!("{:?}", s.stepper.settle(&mut s.engine).unwrap()));
    }
    log.push(format!("{:?}", s.stepper.finish(&mut s.engine).unwrap()));

    log.join("\n")
}

#[test]
fn the_same_inputs_give_the_same_events() {
    assert_eq!(scripted_run(), scripted_run());
}

/// Buys one unit of each instrument on every bar while flat, and sells it on the next bar.
///
/// It acts once per bar per instrument, so it is driven identically by a backtest, which shows it
/// every event, and by a stepper.
#[derive(Debug, Default)]
struct EveryBar {
    acted: Mutex<FnvHashMap<InstrumentIndex, DateTime<Utc>>>,
}

impl AlgoStrategy for EveryBar {
    type State = State;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let mut acted = self.acted.lock().unwrap();
        let opens = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .filter_map(|instrument| {
                let bar = instrument.data.candle?.close_time;
                if acted.get(&instrument.key) == Some(&bar) || !instrument.orders.0.is_empty() {
                    return None;
                }
                acted.insert(instrument.key, bar);

                let side = match instrument.position.positions.is_empty() {
                    true => Side::Buy,
                    false => Side::Sell,
                };
                Some(OrderRequestOpen {
                    key: OrderKey {
                        exchange: instrument.instrument.exchange,
                        instrument: instrument.key,
                        strategy: StrategyId::new("every-bar"),
                        cid: ClientOrderId::new(format!("{}-{}", instrument.key, bar.timestamp())),
                    },
                    state: RequestOpen {
                        side,
                        price: None,
                        quantity: dec!(1),
                        kind: OrderKind::Market,
                        time_in_force: TimeInForce::ImmediateOrCancel,
                        position_id: None,
                        reduce_only: false,
                        market: None,
                    },
                })
            })
            .collect::<Vec<_>>();

        (std::iter::empty(), opens)
    }
}

impl ClosePositionsStrategy for EveryBar {
    type State = State;

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

impl OnDisconnectStrategy<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for EveryBar
{
    type OnDisconnect = ();

    fn on_disconnect(_: &mut TestEngine<Self>, _: ExchangeId) -> Self::OnDisconnect {}
}

impl OnTradingDisabled<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for EveryBar
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(_: &mut TestEngine<Self>) -> Self::OnTradingDisabled {}
}

/// Bars for both instruments at three instants, the second and third with prices moved.
fn bars() -> Vec<(DateTime<Utc>, Vec<EngineEvent<DataKind>>)> {
    [
        (dec!(100), dec!(50)),
        (dec!(104), dec!(48)),
        (dec!(99), dec!(51)),
    ]
    .into_iter()
    .enumerate()
    .map(|(n, (btc, eth))| {
        let secs = 3 * (n as i64 + 1);
        (at(secs), vec![bar(BTC, secs, btc), bar(ETH, secs, eth)])
    })
    .collect()
}

#[tokio::test]
async fn stepping_books_what_a_backtest_over_the_same_bars_books() {
    // Stepped.
    let mut stepped = session_with(0, usdt(dec!(10_000)), None, EveryBar::default());
    let mut stepped_fills = Vec::new();
    for (time, instant) in bars() {
        let audits = stepped
            .stepper
            .step(&mut stepped.engine, time, instant)
            .unwrap();
        stepped_fills.extend(fills(events(&audits)));
    }
    let audits = stepped.stepper.finish(&mut stepped.engine).unwrap();
    stepped_fills.extend(fills(events(&audits)));

    // Streamed, through the runner a backtest uses, over the same venues and Engine.
    let instruments = instruments();
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(config(0), SimVenueOptions::default(), usdt(dec!(10_000)))
        .unwrap()
        .build();
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
    .build();
    let mut streamed: TestEngine<EveryBar> = Engine::new(
        clock.clone(),
        state,
        execution_tx_map,
        InFlightDeadlines::default(),
        EveryBar::default(),
        DefaultRiskManager::default(),
    );
    let source = bars()
        .into_iter()
        .flat_map(|(time, events)| events.into_iter().map(move |event| Timed::new(event, time)))
        .collect::<Vec<_>>();
    let mut streamed_fills = Vec::new();
    let mut feed = SimRunner::new(venues, stream::iter(source), clock).inspect(|event| {
        streamed_fills.extend(fills([event]));
    });
    async_run(&mut feed, &mut streamed).await;
    drop(feed);

    assert_eq!(stepped_fills.len(), 6, "a buy, a sell and a buy on each");
    assert_eq!(stepped_fills, streamed_fills);
    for asset in ["usdt", "btc", "eth"] {
        assert_eq!(
            balance(&stepped.engine, asset),
            balance(&streamed, asset),
            "{asset}"
        );
    }
}
