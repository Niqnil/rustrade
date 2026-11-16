#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! An order the simulated venue replaces under a new venue order id stays one order in the engine.
//!
//! The venue's reports go through the account-event indexer and `EngineState`, as in a backtest.
//! Whether the order goes on under its new id or starts afresh there, its old id reported
//! cancelled, the engine follows it to the new id and keeps tracking it: after a change made
//! outside any client, reported as a replace, and after the engine's own amend, answered with the
//! id it replaced.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::engine::state::{
    EngineState,
    global::DefaultGlobalData,
    instrument::data::DefaultInstrumentMarketData,
    order::{applied::OrderReplaced, in_flight_recorder::InFlightRequestRecorder},
};
use rustrade_execution::{
    AccountSnapshot, UnindexedAccountEvent,
    balance::{AssetBalance, Balance},
    client::mock::{AmendConfig, MockExecutionConfig},
    exchange::mock::SimulatedVenue,
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
    market::{MarketObservation, MarketSnapshot},
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        amend::{AmendPatch, AmendTarget, RequestAmend},
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        report::{OrderReport, Replaces},
        request::{OrderRequestOpen, ReduceOnly, RequestOpen},
        state::ActiveOrderState,
    },
};
use rustrade_instrument::{
    Side, Underlying,
    asset::name::AssetNameExchange,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex, name::InstrumentNameExchange},
    test_utils::instrument,
};
use rustrade_integration::collection::snapshot::Snapshot;

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
const TIME: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

type Engine = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

fn instrument_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("btc_usdt")
}

fn funded(asset: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: AssetNameExchange::new(asset),
        balance: Balance::new(amount, amount),
        time_exchange: TIME,
    }
}

fn venue(amend: AmendConfig) -> SimulatedVenue {
    let config = MockExecutionConfig::new(
        EXCHANGE,
        AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![funded("btc", dec!(10)), funded("usdt", dec!(1_000_000))],
            instruments: vec![],
        },
        0,
    )
    .with_amend(amend);
    let spot = Instrument {
        exchange: EXCHANGE,
        name_internal: instrument(EXCHANGE, "btc", "usdt").name_internal,
        name_exchange: instrument_name(),
        underlying: Underlying {
            base: AssetNameExchange::new("btc"),
            quote: AssetNameExchange::new("usdt"),
        },
        quote: rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
        kind: rustrade_instrument::instrument::kind::InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    };
    let mut venue = SimulatedVenue::new_market_driven(
        &config,
        [(instrument_name(), spot)].into_iter().collect(),
    );
    let _ = venue.advance_time(TIME);
    let _ = venue.apply_market(&instrument_name(), MarketObservation::new(market()), TIME);
    venue
}

fn cid() -> ClientOrderId {
    ClientOrderId::new("resting")
}

fn key() -> OrderKey<ExchangeId, InstrumentNameExchange> {
    OrderKey {
        exchange: EXCHANGE,
        instrument: instrument_name(),
        strategy: StrategyId::new("strategy"),
        cid: cid(),
    }
}

fn market() -> MarketSnapshot {
    MarketSnapshot {
        best_bid: Some(dec!(100)),
        best_ask: Some(dec!(101)),
        last_price: Some(dec!(100)),
    }
}

/// The venue and the engine, both holding a buy of 1 at 90 the engine sent, resting on the book.
fn resting(amend: AmendConfig) -> (SimulatedVenue, AccountEventIndexer, Engine) {
    let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
    let indexer = AccountEventIndexer::new(Arc::new(
        generate_execution_instrument_map(&instruments, EXCHANGE).unwrap(),
    ));
    let mut engine: Engine = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(TIME)
    .build();
    let mut venue = venue(amend);

    let request = RequestOpen::new(
        Side::Buy,
        Some(dec!(90)),
        dec!(1),
        OrderKind::Limit,
        TimeInForce::GoodUntilCancelled { post_only: false },
    );
    let indexed: OrderRequestOpen<ExchangeIndex, InstrumentIndex> = OrderEvent {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: key().strategy,
            cid: cid(),
        },
        state: request.clone(),
    };
    engine
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .orders
        .record_in_flight_open(&indexed, TIME);
    let outcome = venue.open_order(OrderEvent {
        key: key(),
        state: request.with_market(market()),
    });
    assert!(outcome.response.state.is_accepted(), "{outcome:?}");
    apply(&indexer, &mut engine, outcome.events);
    // The answer to the open, as the engine receives it.
    let answer = indexer.order_snapshot(outcome.response).unwrap();
    engine
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .update_from_order_snapshot(Snapshot(&OrderReport::state_only(answer)));
    assert!(
        matches!(tracked(&engine), Some(ActiveOrderState::Open(_))),
        "precondition: the engine holds the order resting"
    );
    (venue, indexer, engine)
}

/// Apply `events` to `engine`, returning the replaces it followed.
fn apply(
    indexer: &AccountEventIndexer,
    engine: &mut Engine,
    events: Vec<UnindexedAccountEvent>,
) -> Vec<OrderReplaced<ExchangeIndex, InstrumentIndex>> {
    events
        .into_iter()
        .flat_map(|event| {
            let event = indexer.account_event(event).unwrap();
            engine.update_from_account(&event).orders.replaced
        })
        .collect()
}

fn tracked(engine: &Engine) -> Option<ActiveOrderState> {
    engine
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .orders
        .get(&cid())
        .map(|order| order.state.clone())
}

fn engine_id(engine: &Engine) -> Option<VenueOrderId> {
    tracked(engine)?.open_meta().map(|open| open.id.clone())
}

/// The venue order id the engine holds the order by.
fn held_id(engine: &Engine) -> OrderId {
    engine_id(engine)
        .and_then(|id| id.assigned().cloned())
        .expect("the venue named the order")
}

/// The engine's amend of the order, known by `id`, to `patch`.
fn amend_request<ExchangeKey, InstrumentKey>(
    exchange: ExchangeKey,
    instrument: InstrumentKey,
    id: &OrderId,
    patch: AmendPatch,
) -> OrderEvent<RequestAmend, ExchangeKey, InstrumentKey> {
    OrderEvent {
        key: OrderKey {
            exchange,
            instrument,
            strategy: key().strategy,
            cid: cid(),
        },
        state: RequestAmend::new(
            VenueOrderId::Assigned(id.clone()),
            patch,
            AmendTarget {
                side: Side::Buy,
                price: Some(dec!(90)),
                quantity: dec!(1),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                reduce_only: Some(ReduceOnly::No),
            },
            Decimal::ZERO,
        ),
    }
}

/// For the order going on under its new id, and for it starting afresh there.
fn configs() -> [AmendConfig; 2] {
    let reassigning = AmendConfig::FULL.with_reassign_id(true);
    [reassigning, reassigning.with_filled_restarts(true)]
}

/// A change made at the venue outside any client is followed to the order's new id, once, and
/// the old id's cancel, when the venue reports one, leaves the order tracked.
#[test]
fn an_order_changed_outside_the_engine_is_followed_to_its_new_id() {
    for amend in configs() {
        let (mut venue, indexer, mut engine) = resting(amend);
        let old = held_id(&engine);

        let events = venue
            .modify_externally(&cid(), AmendPatch::default().with_price(dec!(95)))
            .unwrap();
        let replaced = apply(&indexer, &mut engine, events);

        let [replaced] = replaced.as_slice() else {
            panic!("{amend:?}: expected one replace followed, got {replaced:?}");
        };
        assert_eq!(replaced.replaced, Replaces::new(old.clone()), "{amend:?}");
        assert_ne!(
            replaced.id,
            VenueOrderId::Assigned(old.clone()),
            "{amend:?}"
        );
        assert_eq!(engine_id(&engine), Some(replaced.id.clone()), "{amend:?}");
        let held = engine
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .orders
            .tracked(&cid())
            .unwrap();
        assert_eq!(held.order.price, Some(dec!(95)), "{amend:?}");
        assert_eq!(held.superseded_ids(), [old], "{amend:?}");
    }
}

/// The engine's own amend, answered under a new id, moves the order to it, whichever of the
/// stream's report and the answer comes first, and the old id's cancel leaves it tracked.
#[test]
fn the_engines_own_amend_under_a_new_id_keeps_the_order_tracked() {
    for amend in configs() {
        let (mut venue, indexer, mut engine) = resting(amend);
        let old = held_id(&engine);
        let patch = AmendPatch::default().with_price(dec!(95));
        engine
            .instruments
            .instrument_index_mut(&InstrumentIndex(0))
            .orders
            .record_in_flight_amend(
                &amend_request(ExchangeIndex(0), InstrumentIndex(0), &old, patch),
                TIME,
            );

        let outcome = venue.amend_order(amend_request(EXCHANGE, instrument_name(), &old, patch));
        let new = match &outcome.response.state {
            Ok(amended) => amended.open.id.clone(),
            Err(error) => panic!("{amend:?}: the amend was refused: {error:?}"),
        };
        assert_ne!(new, VenueOrderId::Assigned(old.clone()), "{amend:?}");
        // The stream's report arrives before the answer, as the venue sends them.
        let replaced = apply(&indexer, &mut engine, outcome.events);
        let answer = indexer.order_response_amend(outcome.response).unwrap();
        engine
            .instruments
            .instrument_index_mut(&InstrumentIndex(0))
            .update_from_amend_response(&answer);

        assert_eq!(replaced.len(), 1, "{amend:?}: the stream links the replace");
        assert!(
            matches!(tracked(&engine), Some(ActiveOrderState::Open(_))),
            "{amend:?}: the answer settles the amend: {:?}",
            tracked(&engine)
        );
        assert_eq!(engine_id(&engine), Some(new), "{amend:?}");
        let held = engine
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .orders
            .tracked(&cid())
            .unwrap();
        assert_eq!(held.order.price, Some(dec!(95)), "{amend:?}");
        assert_eq!(held.superseded_ids(), [old], "{amend:?}");
    }
}
