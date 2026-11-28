//! The expiry-day cutoff: from a set instant before an option expires, an order that would open
//! or increase a position in it is refused, and one that only closes is taken.

use super::{
    options::{
        buy, config as option_config, driven_instruments, driven_venue, expiry, feed, funds,
        held_option, limit, option_name, option_position, option_request, sell,
    },
    *,
};
use crate::{
    client::mock::{SessionCalendarConfig, SessionCloseOverrides},
    error::OpenRefusal,
    exchange::mock::venue::{ExpiryRule, ExpiryRuleUnplaceable, VenueRestoreError},
    expiry::{ExpiryAnchor, ExpiryAnchorError, ExpiryDayCutoff, ExpirySellOut},
    order::{
        amend::{AmendPatch, AmendTarget, RequestAmend},
        request::OrderRequestAmend,
    },
};
use rustrade_instrument::{
    calendar::DailyClose,
    instrument::kind::{
        InstrumentKindDiscriminant,
        option::{OptionKind, OptionSettlement},
    },
};
use std::time::Duration;

/// Ten seconds before [`expiry`]: when [`cutoff`] starts refusing.
fn cutoff_at() -> DateTime<Utc> {
    time(90)
}

/// Opening orders refused from ten seconds before expiry.
fn cutoff() -> ExpiryDayCutoff {
    ExpiryDayCutoff::new(ExpiryAnchor::BeforeExpiry(Duration::from_secs(10)))
}

/// A market-driven venue listing an option of `kind` and `settlement`, cutting off under
/// [`cutoff`], funded with `usd_balance` and `spy_balance`, with SPY at 100 and the option at 1.5.
fn cutting_off(
    kind: OptionKind,
    settlement: OptionSettlement,
    usd_balance: &str,
    spy_balance: &str,
) -> SimulatedVenue {
    let fund = funds(usd_balance, spy_balance);
    let mut venue = driven_venue(kind, settlement, |config| {
        fund(config);
        config.expiry_day_cutoff = Some(cutoff());
    });
    feed(&mut venue, "100", "1.5", time(2));
    venue
}

/// A venue as [`cutting_off`] builds it, for a physically settled call.
fn cutting_off_calls(usd_balance: &str, spy_balance: &str) -> SimulatedVenue {
    cutting_off(
        OptionKind::Call,
        OptionSettlement::Physical,
        usd_balance,
        spy_balance,
    )
}

/// Moves `venue`'s clock to `at` with the market unchanged.
fn reach(venue: &mut SimulatedVenue, at: DateTime<Utc>) {
    feed(venue, "100", "1.5", at);
}

/// Asserts `outcome` is the refusal of an order opening a position after the cutoff at
/// `cutoff`, having moved nothing.
fn assert_cut_off(outcome: &OpenOutcome, cutoff: DateTime<Utc>) {
    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OpenRefused(OpenRefusal::ExpiryDayCutoff { cutoff: refused })
            ))) if *refused == cutoff
        ),
        "refused at the cutoff {cutoff}: {:?}",
        outcome.response.state
    );
    assert!(outcome.events.is_empty(), "{:?}", outcome.events);
}

fn assert_accepted(outcome: &OpenOutcome) {
    assert!(
        outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
}

#[test]
fn an_opening_buy_is_taken_before_the_cutoff_and_refused_from_it() {
    let mut venue = cutting_off_calls("10000", "0");
    reach(&mut venue, time(89));
    buy(&mut venue, "1");
    reach(&mut venue, cutoff_at());

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert_cut_off(&outcome, cutoff_at());
    assert_eq!(option_position(&venue), Some(d("1")));
}

#[test]
fn a_sale_closing_a_long_is_taken_after_the_cutoff() {
    let mut venue = cutting_off_calls("10000", "0");
    buy(&mut venue, "2");
    reach(&mut venue, cutoff_at());

    sell(&mut venue, "2");

    assert_eq!(option_position(&venue), None);
}

/// A long of 5 with a sale of 4 already working leaves 1 to close: a sale of 3 would open 2, and
/// is refused, while a sale of 1 closes the last contract.
#[test]
fn a_sale_counts_the_working_sales_against_the_long_and_is_refused_if_any_of_it_opens() {
    let mut venue = cutting_off_calls("10000", "1000");
    buy(&mut venue, "5");
    assert_accepted(&venue.open_order(limit(option_name(), Side::Sell, "5", "4", "working")));
    reach(&mut venue, cutoff_at());

    let opening = venue.open_order(limit(option_name(), Side::Sell, "5", "3", "opening"));
    let closing = venue.open_order(limit(option_name(), Side::Sell, "5", "1", "closing"));

    assert_cut_off(&opening, cutoff_at());
    assert_accepted(&closing);
    assert_eq!(option_position(&venue), Some(d("5")));
}

/// Shares enough to secure the short are free, so only the cutoff refuses it.
#[test]
fn a_sale_that_would_turn_a_long_into_a_short_is_refused_whole() {
    let mut venue = cutting_off_calls("10000", "1000");
    buy(&mut venue, "5");
    reach(&mut venue, cutoff_at());

    let outcome = venue.open_order(option_request(Side::Sell, "7", None));

    assert_cut_off(&outcome, cutoff_at());
    assert_eq!(option_position(&venue), Some(d("5")));
}

#[test]
fn a_buy_covering_a_short_is_taken_after_the_cutoff_and_one_beyond_it_is_refused() {
    let mut venue = cutting_off_calls("10000", "200");
    sell(&mut venue, "2");
    reach(&mut venue, cutoff_at());

    let beyond = venue.open_order(option_request(Side::Buy, "3", None));
    assert_cut_off(&beyond, cutoff_at());

    buy(&mut venue, "2");
    assert_eq!(option_position(&venue), None);
}

#[test]
fn a_cash_settled_option_is_cut_off_too() {
    let mut venue = cutting_off(OptionKind::Put, OptionSettlement::Cash, "10000", "0");
    reach(&mut venue, cutoff_at());

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert_cut_off(&outcome, cutoff_at());
}

/// A known limitation: the cutoff refuses new orders only.
#[test]
fn an_opening_order_working_from_before_the_cutoff_keeps_working_and_fills_after_it() {
    let mut venue = cutting_off_calls("10000", "0");
    assert_accepted(&venue.open_order(limit(option_name(), Side::Buy, "1", "1", "bid")));
    reach(&mut venue, cutoff_at());
    assert_eq!(venue.orders_open(&[]).unwrap().len(), 1);

    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("0.9")),
        time(95),
    );

    assert_eq!(option_position(&venue), Some(d("1")));
}

/// A closing sale cannot be raised past the long after the cutoff, since the venue amends no
/// option order: the cutoff cannot be passed in two steps.
#[test]
fn after_the_cutoff_an_amend_raising_a_closing_sale_is_refused() {
    let mut venue = cutting_off_calls("10000", "1000");
    buy(&mut venue, "1");
    assert_accepted(&venue.open_order(limit(option_name(), Side::Sell, "5", "1", "ask")));
    reach(&mut venue, cutoff_at());
    let order = venue
        .account
        .orders()
        .get(&ClientOrderId::new("ask"))
        .cloned()
        .unwrap();

    let outcome = venue.amend_order(OrderRequestAmend {
        key: order.key.clone(),
        state: RequestAmend::new(
            order.state.id.clone(),
            AmendPatch::default().with_quantity(d("2")),
            AmendTarget {
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                reduce_only: None,
            },
            Decimal::ZERO,
        ),
    });

    assert!(
        matches!(
            outcome.response.state,
            Err(OrderError::AmendRefused(AmendRefusal::Venue(_)))
        ),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(venue.orders_open(&[]).unwrap()[0].quantity, d("1"));
}

/// A close every day at [`time`]`(seconds)`'s time of day, in UTC. [`expiry`] falls on that date.
fn closing_at(seconds: i64) -> SessionCalendarConfig {
    SessionCalendarConfig::Daily(DailyClose::new(time(seconds).time(), chrono_tz::UTC))
}

/// Opening orders refused from ten seconds before the close of the option's expiry day.
fn before_close() -> ExpiryDayCutoff {
    ExpiryDayCutoff::new(ExpiryAnchor::BeforeClose(Duration::from_secs(10)))
}

/// The config of a venue cutting off [`before_close`], with `session_close`.
fn before_close_config(session_close: Option<SessionCalendarConfig>) -> MockExecutionConfig {
    let mut config = option_config("10000", FeeModelConfig::default());
    config.expiry_day_cutoff = Some(before_close());
    config.session_close = session_close;
    config
}

/// The option's cutoff cannot be placed, for `source`.
fn unplaceable(source: ExpiryAnchorError) -> ExpiryRuleUnplaceable {
    ExpiryRuleUnplaceable {
        rule: ExpiryRule::DayCutoff,
        instrument: option_name(),
        source,
    }
}

/// The option closes under its own class's close, not the venue's, and is cut off ten seconds
/// before it.
#[test]
fn a_cutoff_before_the_close_is_placed_by_the_options_own_close() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        config.expiry_day_cutoff = Some(before_close());
        config.session_close = Some(closing_at(40));
        config.session_close_overrides = SessionCloseOverrides::new()
            .with_kind(InstrumentKindDiscriminant::Option, closing_at(80));
    });
    feed(&mut venue, "100", "1.5", time(69));
    buy(&mut venue, "1");
    reach(&mut venue, time(70));

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert_cut_off(&outcome, time(70));
}

#[test]
fn a_cutoff_the_calendar_cannot_place_is_refused_at_build() {
    assert_eq!(
        SimulatedVenue::try_new_market_driven(
            &before_close_config(Some(closing_at(110))),
            driven_instruments(OptionKind::Call, OptionSettlement::Physical),
        )
        .err(),
        Some(VenueConfigError::ExpiryRule(unplaceable(
            ExpiryAnchorError::CloseAfterExpiry {
                close: time(110),
                expiry: expiry(),
            }
        )))
    );
}

/// A venue cutting off before the close with no calendar for its option: built, for a calendar
/// given later. Its clock has not moved.
fn uncalendared_venue(config: &MockExecutionConfig) -> SimulatedVenue {
    SimulatedVenue::try_new_market_driven(
        config,
        driven_instruments(OptionKind::Call, OptionSettlement::Physical),
    )
    .expect("built without a calendar, for one given later")
}

#[test]
#[should_panic(expected = "expiry-day cutoff SPY_C100")]
fn a_cutoff_before_the_close_with_no_calendar_panics_when_the_clock_first_moves() {
    let _ = uncalendared_venue(&before_close_config(None)).advance_time(time(1));
}

#[test]
#[should_panic(
    expected = "expiry-day cutoff is anchored to the session calendar, but the option SPY_C100 \
                has no session calendar"
)]
fn an_option_order_whose_cutoff_has_no_calendar_panics() {
    let _ = uncalendared_venue(&before_close_config(None)).open_order(option_request(
        Side::Buy,
        "1",
        None,
    ));
}

/// A sale closing a long held since `initial_state` could never be refused by the cutoff, but the
/// venue still stops on it rather than run on without the option's cutoff.
#[test]
#[should_panic(expected = "option SPY_C100 has no session calendar")]
fn a_closing_order_whose_cutoff_has_no_calendar_panics_too() {
    let mut config = before_close_config(None);
    config.initial_state.instruments.push(held_option());

    let _ = uncalendared_venue(&config).open_order(option_request(Side::Sell, "1", None));
}

/// A sell-out with no calendar stops the venue only when its clock moves: an order before that
/// is no part of it.
#[test]
fn an_option_order_on_a_venue_with_only_its_sell_out_unplaced_does_not_panic() {
    let mut config = option_config("10000", FeeModelConfig::default());
    config.expiry_sell_out = Some(ExpirySellOut::new(ExpiryAnchor::BeforeClose(
        Duration::from_secs(10),
    )));

    let _ = uncalendared_venue(&config).open_order(option_request(Side::Buy, "1", None));
}

#[test]
fn a_calendar_given_after_build_places_the_cutoff() {
    let mut venue = uncalendared_venue(&before_close_config(None))
        .with_session_calendar(closing_at(80).calendar())
        .unwrap();
    reach(&mut venue, time(70));

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert_cut_off(&outcome, time(70));
}

#[test]
fn a_restored_venue_refuses_from_the_cutoff_its_config_places() {
    let original = cutting_off_calls("10000", "0");
    let mut config = option_config("10000", FeeModelConfig::default());
    config.expiry_day_cutoff = Some(cutoff());
    let mut restored = SimulatedVenue::from_snapshot(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Physical),
        original.snapshot(),
    )
    .unwrap();
    reach(&mut restored, cutoff_at());

    let outcome = restored.open_order(option_request(Side::Buy, "1", None));

    assert_cut_off(&outcome, cutoff_at());
}

#[test]
fn a_restore_whose_cutoff_the_calendar_cannot_place_is_refused() {
    let venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});

    assert_eq!(
        SimulatedVenue::from_snapshot(
            &before_close_config(Some(closing_at(110))),
            driven_instruments(OptionKind::Call, OptionSettlement::Physical),
            venue.snapshot(),
        )
        .err(),
        Some(VenueRestoreError::ExpiryRule(unplaceable(
            ExpiryAnchorError::CloseAfterExpiry {
                close: time(110),
                expiry: expiry(),
            }
        )))
    );
}

#[test]
fn a_cutoff_serialises_as_its_anchor() {
    let json = serde_json::json!({"anchor": {"before_close": {"secs": 2700, "nanos": 0}}});
    let cutoff = ExpiryDayCutoff::new(ExpiryAnchor::BeforeClose(Duration::from_secs(2700)));

    assert_eq!(serde_json::to_value(cutoff).unwrap(), json);
    assert_eq!(
        serde_json::from_value::<ExpiryDayCutoff>(json).unwrap(),
        cutoff
    );
}
