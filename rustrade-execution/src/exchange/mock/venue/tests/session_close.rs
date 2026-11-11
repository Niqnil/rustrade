//! Day orders on a venue that knows when its sessions close: accepted, resting until just after
//! the next close, then expired, wherever the order went in between.

use super::*;
use crate::{
    client::mock::SessionCalendarConfig,
    order::amend::{AmendPatch, AmendTarget, RequestAmend},
};
use rustrade_instrument::calendar::{CalendarError, DailyClose, SessionCalendar};
use std::time::Duration;

/// 16:00 New York, every weekday.
fn new_york_close() -> SessionCalendarConfig {
    SessionCalendarConfig::Daily(DailyClose::new(
        NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        chrono_tz::America::New_York,
    ))
}

fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
    chrono::TimeZone::with_ymd_and_hms(&Utc, y, m, d, h, min, s).unwrap()
}

/// Tuesday 2026-11-10, 10:00 in New York (EST).
fn tuesday_morning() -> DateTime<Utc> {
    utc(2026, 11, 10, 15, 0, 0)
}

/// Tuesday 2026-11-10's close, 16:00 EST.
fn tuesday_close() -> DateTime<Utc> {
    utc(2026, 11, 10, 21, 0, 0)
}

/// The first instant after `close`: when a day order of that session expires.
fn just_after(close: DateTime<Utc>) -> DateTime<Utc> {
    close + TimeDelta::nanoseconds(1)
}

fn day() -> TimeInForce {
    TimeInForce::GoodUntilEndOfDay
}

fn day_config() -> MockExecutionConfig {
    spot_config("10", "1000000", FeeModelConfig::default()).with_session_close(new_york_close())
}

fn quote_book(venue: &mut SimulatedVenue, bid: &str, ask: &str, at: DateTime<Utc>) {
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book(bid, ask)),
                at
            )
            .is_empty(),
        "a book the resting orders do not cross fills nothing"
    );
}

/// A venue with a 16:00 New York close, at `now`, quoting 49000/49100.
fn day_venue_at(now: DateTime<Utc>) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(&day_config(), spot_instruments());
    advance(&mut venue, now);
    quote_book(&mut venue, "49000", "49100", now);
    venue
}

fn deadline_of(venue: &SimulatedVenue, name: &str) -> Option<DateTime<Utc>> {
    venue
        .account
        .orders()
        .resting_order(&ClientOrderId::new(name))
        .unwrap_or_else(|| panic!("{name} is open"))
        .deadline
}

fn expired_in(
    events: &[UnindexedAccountEvent],
) -> Vec<&Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order)
                if matches!(
                    order.0.state,
                    OrderState::Inactive(InactiveOrderState::Expired(_))
                ) =>
            {
                Some(&order.0)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn without_a_session_close_a_day_limit_is_refused_saying_how_to_enable_it() {
    let mut venue = make_market_venue("10", "1000000");
    advance(&mut venue, tuesday_morning());

    let outcome = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));

    assert!(outcome.events.is_empty());
    match outcome.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(
            OrderError::UnsupportedTimeInForce { ref reason, .. },
        )) => assert!(
            reason.contains("MockExecutionConfig::session_close"),
            "{reason}"
        ),
        ref other => panic!("a day limit must be refused, got: {other:?}"),
    }
}

#[test]
fn a_day_limit_rests_through_the_close_and_expires_the_instant_after_it() {
    let mut venue = day_venue_at(tuesday_morning());

    let outcome = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));

    assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);
    assert_eq!(
        deadline_of(&venue, "day"),
        Some(just_after(tuesday_close()))
    );
    assert_eq!(venue.next_deadline(), Some(just_after(tuesday_close())));

    // Still working at the close itself.
    advance(&mut venue, tuesday_close());
    assert_eq!(venue.orders_open(&[]).len(), 1);

    let events = venue.advance_time(just_after(tuesday_close()));

    assert_eq!(events.len(), 2, "the hold released, then the order expired");
    assert_eq!(
        balance_of(&events[0]).balance.free,
        d("1000000"),
        "the release restates what resting took"
    );
    let expired = order_of(&events[1]);
    assert_eq!(
        expired.time_in_force,
        day(),
        "it still reports its own time in force"
    );
    match &expired.state {
        OrderState::Inactive(InactiveOrderState::Expired(expired)) => {
            assert_eq!(expired.time_exchange, just_after(tuesday_close()));
            assert_eq!(expired.filled_quantity, Some(Decimal::ZERO));
        }
        other => panic!("the order must be reported expired, got: {other:?}"),
    }
    assert!(venue.orders_open(&[]).is_empty());
}

/// What trades at the close itself can still fill a day order, unlike a `GoodTillDate` order,
/// whose deadline is an inclusive cutoff at its stated instant.
#[test]
fn a_tick_at_the_close_still_fills_a_day_order() {
    let mut venue = day_venue_at(tuesday_morning());
    let _ = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));
    let _ = venue.open_order(limit_request(
        "gtd",
        Side::Buy,
        "1",
        "48000",
        TimeInForce::GoodTillDate {
            expiry: tuesday_close(),
        },
    ));

    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("47900", "48000")),
        tuesday_close(),
    );

    let traded: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::Trade(trade) => trade.client_order_id.as_ref(),
            _ => None,
        })
        .collect();
    assert_eq!(traded, vec![&ClientOrderId::new("day")], "{events:?}");
    let expired = expired_in(&events);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].key.cid, ClientOrderId::new("gtd"));
}

#[test]
fn a_day_order_sent_outside_the_session_expires_at_the_next_close() {
    let monday_close = just_after(utc(2026, 11, 16, 21, 0, 0));
    for (sent, deadline, when) in [
        // Friday after the close, Saturday, Sunday: Monday's close.
        (
            utc(2026, 11, 13, 21, 0, 0),
            monday_close,
            "at Friday's close",
        ),
        (utc(2026, 11, 13, 23, 0, 0), monday_close, "Friday evening"),
        (utc(2026, 11, 14, 15, 0, 0), monday_close, "Saturday"),
        (utc(2026, 11, 15, 15, 0, 0), monday_close, "Sunday"),
        // Before Tuesday's open: Tuesday's close.
        (
            utc(2026, 11, 10, 12, 0, 0),
            just_after(tuesday_close()),
            "before the open",
        ),
    ] {
        let mut venue = day_venue_at(sent);

        let outcome = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));

        assert_eq!(
            rested_open(&outcome).filled_quantity,
            Decimal::ZERO,
            "{when}"
        );
        assert_eq!(deadline_of(&venue, "day"), Some(deadline), "{when}");
    }
}

/// A marketable day order the book can fill only in part rests the rest, as a good-until-cancelled
/// one does, and the remainder expires at the close keeping what traded.
#[test]
fn a_partly_filled_day_order_rests_its_remainder_then_expires_keeping_its_fills() {
    let mut venue = day_venue_at(tuesday_morning());
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("47800", "47900"))
                    .with_depth(MarketDepth::new(Some(d("0.4")), Some(d("0.4")))),
                tuesday_morning()
            )
            .is_empty()
    );

    let outcome = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));

    assert_eq!(filled_trade(&outcome).quantity, d("0.4"));
    assert_eq!(rested_open(&outcome).filled_quantity, d("0.4"));
    assert_eq!(
        deadline_of(&venue, "day"),
        Some(just_after(tuesday_close()))
    );

    let events = venue.advance_time(just_after(tuesday_close()));

    let expired = expired_in(&events);
    assert_eq!(expired.len(), 1, "{events:?}");
    match &expired[0].state {
        OrderState::Inactive(InactiveOrderState::Expired(expired)) => {
            assert_eq!(expired.filled_quantity, Some(d("0.4")));
        }
        other => panic!("expected expired, got: {other:?}"),
    }
    assert_eq!(venue.trades(tuesday_morning()).len(), 1, "the fill stands");
}

#[test]
fn a_cancel_after_a_day_order_expired_is_answered_already_expired() {
    let mut venue = day_venue_at(tuesday_morning());
    let _ = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));
    let _ = venue.advance_time(just_after(tuesday_close()));

    let cancelled = venue.cancel_order(OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: instrument_name(),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::new("day"),
        },
        state: RequestCancel { id: None },
    });

    assert!(
        matches!(
            cancelled.response.state,
            Err(OrderError::Rejected(ApiError::OrderAlreadyExpired))
        ),
        "{:?}",
        cancelled.response.state
    );
}

/// An amend sends the order to the back of its queue, restamping its arrival, but the session it
/// was accepted in still ends it.
#[test]
fn an_amend_does_not_move_a_day_orders_deadline() {
    let mut venue = day_venue_at(tuesday_morning());
    let _ = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));
    // Wednesday: an amend after the close would be too late, so amend just before it.
    advance(&mut venue, tuesday_close() - TimeDelta::minutes(1));

    let order = venue
        .account
        .orders()
        .get(&ClientOrderId::new("day"))
        .cloned()
        .unwrap();
    let amended = venue.amend_order(OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: instrument_name(),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::new("day"),
        },
        state: RequestAmend::new(
            order.state.id.clone(),
            AmendPatch::default().with_price(d("47000")),
            AmendTarget {
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                reduce_only: None,
            },
            order.state.filled_quantity,
        ),
    });

    assert!(
        amended.response.state.is_ok(),
        "{:?}",
        amended.response.state
    );
    let resting = venue
        .account
        .orders()
        .get(&ClientOrderId::new("day"))
        .unwrap();
    assert_eq!(resting.price, Some(d("47000")));
    assert_eq!(
        resting.state.time_exchange,
        tuesday_close() - TimeDelta::minutes(1)
    );
    assert_eq!(
        deadline_of(&venue, "day"),
        Some(just_after(tuesday_close()))
    );

    let events = venue.advance_time(just_after(tuesday_close()));
    assert_eq!(expired_in(&events).len(), 1, "{events:?}");
}

/// A day order held for a fresh market keeps the deadline it was given on arrival when it is
/// released to the book, and one still held at the close expires there.
#[test]
fn a_held_day_order_keeps_its_deadline_when_released_and_expires_held() {
    let config = day_config().with_stale_market(StaleMarketPolicy::Wait {
        max_age: Duration::from_secs(10),
    });
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, tuesday_morning());
    quote_book(&mut venue, "49000", "49100", tuesday_morning());
    let stale = tuesday_morning() + TimeDelta::minutes(1);
    advance(&mut venue, stale);

    let _ = venue.open_order(limit_request("released", Side::Buy, "1", "48000", day()));
    let _ = venue.open_order(limit_request("held", Side::Buy, "1", "48000", day()));
    assert_eq!(
        venue.account.orders().held().len(),
        2,
        "both wait for a fresh market"
    );
    assert_eq!(
        deadline_of(&venue, "held"),
        Some(just_after(tuesday_close()))
    );

    // Cancel one so that only the other is released, then release it with a fresh book.
    let _ = venue.cancel_order(OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: instrument_name(),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::new("held"),
        },
        state: RequestCancel { id: None },
    });
    // Releasing it restates its hold, so this tick is not silent.
    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("49000", "49100")),
        stale + TimeDelta::minutes(1),
    );

    assert!(
        venue.account.orders().held().is_empty(),
        "released to the book"
    );
    assert_eq!(
        deadline_of(&venue, "released"),
        Some(just_after(tuesday_close())),
        "the release keeps the deadline given on arrival"
    );
    let events = venue.advance_time(just_after(tuesday_close()));
    assert_eq!(expired_in(&events).len(), 1, "{events:?}");

    // Held through the close: expires held.
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, tuesday_morning());
    quote_book(&mut venue, "49000", "49100", tuesday_morning());
    advance(&mut venue, stale);
    let _ = venue.open_order(limit_request("held", Side::Buy, "1", "48000", day()));

    let events = venue.advance_time(just_after(tuesday_close()));

    assert_eq!(expired_in(&events).len(), 1, "{events:?}");
    assert_eq!(venue.balances(&[quote()])[0].balance.free, d("1000000"));
}

/// A snapshot keeps a day order's deadline, so a venue restored with no session close at all
/// still expires it when it was due.
#[test]
fn a_restored_day_order_keeps_its_deadline_whatever_the_calendar() {
    let mut venue = day_venue_at(tuesday_morning());
    let _ = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));
    let snapshot: SimulatedVenueSnapshot =
        serde_json::from_str(&serde_json::to_string(&venue.snapshot()).unwrap()).unwrap();

    let without_calendar = spot_config("10", "1000000", FeeModelConfig::default());
    let mut restored =
        SimulatedVenue::from_snapshot(&without_calendar, spot_instruments(), snapshot).unwrap();

    assert_eq!(
        deadline_of(&restored, "day"),
        Some(just_after(tuesday_close()))
    );
    assert_eq!(restored.next_deadline(), venue.next_deadline());
    let events = restored.advance_time(just_after(tuesday_close()));
    assert_eq!(expired_in(&events).len(), 1, "{events:?}");
}

/// A day order seeded in `initial_state`: a config with that seeded order.
fn seeded_day_config(
    session_close: Option<SessionCalendarConfig>,
    accepted: DateTime<Utc>,
) -> MockExecutionConfig {
    let mut config = spot_config("10", "1000000", FeeModelConfig::default());
    config.session_close = session_close;
    config.initial_state.instruments = vec![InstrumentAccountSnapshot {
        instrument: instrument_name(),
        orders: vec![Order {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("seeded"),
            },
            side: Side::Buy,
            price: Some(d("48000")),
            quantity: d("1"),
            kind: OrderKind::Limit,
            time_in_force: day(),
            state: OrderState::active(Open::new(
                VenueOrderId::Assigned(OrderId::new("seeded")),
                accepted,
                Decimal::ZERO,
            )),
        }],
        orders_complete: true,
        position: PositionReport::Unreported,
        isolated: None,
    }];
    config
}

/// A seeded day order expires at the close after the live venue accepted it, which may already
/// have passed; without a session close it has no deadline, as before.
#[test]
fn a_seeded_day_order_expires_at_the_close_after_its_acceptance() {
    // Accepted Monday, so its session closed Monday 16:00 EST.
    let monday = utc(2026, 11, 9, 15, 0, 0);
    let monday_close = just_after(utc(2026, 11, 9, 21, 0, 0));

    let venue = SimulatedVenue::new_market_driven(
        &seeded_day_config(Some(new_york_close()), monday),
        spot_instruments(),
    );
    assert_eq!(deadline_of(&venue, "seeded"), Some(monday_close));
    let mut venue = venue;
    let events = venue.advance_time(tuesday_morning());
    assert_eq!(
        expired_in(&events).len(),
        1,
        "already past its close: {events:?}"
    );

    // Attached as a calendar of the caller's own instead of configured: the same.
    let venue =
        SimulatedVenue::new_market_driven(&seeded_day_config(None, monday), spot_instruments());
    assert_eq!(
        deadline_of(&venue, "seeded"),
        None,
        "no calendar, no deadline"
    );
    let venue = venue
        .with_session_calendar(new_york_close().calendar())
        .unwrap();
    assert_eq!(deadline_of(&venue, "seeded"), Some(monday_close));
}

/// A calendar that cannot answer.
#[derive(Debug)]
struct Unavailable;

impl SessionCalendar for Unavailable {
    fn next_close_after(&self, _: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError> {
        Err(CalendarError::unavailable("down for maintenance"))
    }
}

#[test]
fn a_calendar_that_cannot_answer_refuses_the_day_order_and_moves_nothing() {
    let mut venue = make_market_venue("10", "1000000")
        .with_session_calendar(Arc::new(Unavailable))
        .unwrap();
    advance(&mut venue, tuesday_morning());
    quote_book(&mut venue, "49000", "49100", tuesday_morning());

    let outcome = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));

    assert!(outcome.events.is_empty());
    assert!(venue.orders_open(&[]).is_empty());
    match outcome.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(
            OrderError::UnsupportedTimeInForce { ref reason, .. },
        )) => assert!(reason.contains("down for maintenance"), "{reason}"),
        ref other => panic!("expected a refusal, got: {other:?}"),
    }

    // Nor does a good-until-cancelled order need it.
    let outcome = venue.open_order(limit_request("gtc", Side::Buy, "1", "48000", gtc()));
    assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);
}

#[test]
fn a_seeded_day_order_the_calendar_cannot_answer_for_is_an_error() {
    let venue = SimulatedVenue::new_market_driven(
        &seeded_day_config(None, tuesday_morning()),
        spot_instruments(),
    );

    assert_eq!(
        venue.with_session_calendar(Arc::new(Unavailable)).err(),
        Some(CalendarError::unavailable("down for maintenance"))
    );
}

/// A calendar answering the instant it was asked from, not one after it.
#[derive(Debug)]
struct Stuck;

impl SessionCalendar for Stuck {
    fn next_close_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError> {
        Ok(after)
    }
}

#[test]
#[should_panic(expected = "not after")]
fn a_calendar_breaking_its_contract_panics() {
    let mut venue = make_market_venue("10", "1000000")
        .with_session_calendar(Arc::new(Stuck))
        .unwrap();
    advance(&mut venue, tuesday_morning());
    quote_book(&mut venue, "49000", "49100", tuesday_morning());

    let _ = venue.open_order(limit_request("day", Side::Buy, "1", "48000", day()));
}

/// A day market order needs no calendar and is given no deadline: it works only on arrival.
#[test]
fn a_day_market_order_needs_no_session_close() {
    let mut venue = make_venue("10", "1000000");
    let mut request = buy_request("1", market_prices("48000"));
    request.state.time_in_force = day();

    let outcome = venue.open_order(request);

    assert!(matches!(
        outcome.response.state,
        OrderState::Inactive(InactiveOrderState::FullyFilled(_))
    ));
}
