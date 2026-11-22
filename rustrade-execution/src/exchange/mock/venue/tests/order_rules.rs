//! An instrument's order rules: what the venue refuses on arrival, at a held order's release and
//! on an amend, with the typed error a live client loading the same rules returns, and the rules
//! it refuses to be built with.

use super::{
    amend::{amend, amended, price},
    *,
};
use crate::{
    error::{Bound, BoundsViolation, OrderField, PrecisionLimit, PrecisionViolation},
    order::amend::AmendPatch,
    rules::{OrderRules, OrderRulesField, ValueRule},
    slippage::SlippageConfig,
};

/// Tick 0.01; lot 0.001 in 0.001..=100; market lot up to 10; notional at least 5, a market order's
/// too.
fn rules() -> OrderRules {
    OrderRules::new(
        ValueRule::new(d("0"), d("0"), d("0.01")),
        ValueRule::new(d("0.001"), d("100"), d("0.001")),
        ValueRule::new(d("0"), d("10"), d("0")),
        ValueRule::new(d("5"), d("0"), d("0")),
    )
    .with_market_notional(ValueRule::new(d("5"), d("0"), d("0")))
}

/// A spot config holding 10 BTC and 100,000 USDT, with no fees and `rules` on BTCUSDT.
fn ruled_config(rules: OrderRules) -> MockExecutionConfig {
    spot_config("10", "100000", FeeModelConfig::default())
        .with_order_rules(instrument_name(), rules)
}

/// A market-driven venue under `config` whose BTCUSDT last traded at `last` at `time(1)`.
fn venue_at(config: &MockExecutionConfig, last: &str) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(traded(last)),
                time(1)
            )
            .is_empty()
    );
    venue
}

/// The error an open was refused with on arrival, having moved nothing.
fn refusal(venue: &SimulatedVenue, outcome: &OpenOutcome) -> UnindexedOrderError {
    assert!(
        outcome.events.is_empty(),
        "a refused open reports nothing: {:?}",
        outcome.events
    );
    assert!(venue.orders_open(&[]).is_empty(), "nothing rests");
    assert!(venue.trades(time(0)).is_empty(), "nothing trades");
    match &outcome.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(error)) => error.clone(),
        other => panic!("expected the open to be refused, got: {other:?}"),
    }
}

fn off_grid(field: OrderField, value: &str, step: &str) -> UnindexedOrderError {
    OrderError::InvalidPrecision(PrecisionViolation::new(
        field,
        d(value),
        PrecisionLimit::NotOnGrid { step: d(step) },
    ))
}

fn out_of_bounds(field: OrderField, bound: Bound, limit: &str, value: &str) -> UnindexedOrderError {
    OrderError::OutOfBounds(BoundsViolation::new(field, bound, d(limit), d(value)))
}

fn market_buy(cid: &str, quantity: &str) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
    let mut request = request(instrument_name(), Side::Buy, quantity, None);
    request.key.cid = ClientOrderId::new(cid);
    request
}

fn is_filled(outcome: &OpenOutcome) -> bool {
    matches!(
        outcome.response.state,
        OrderState::Inactive(InactiveOrderState::FullyFilled(_))
    )
}

// --- On arrival ------------------------------------------------------------------------------

#[test]
fn a_limit_price_off_the_tick_is_refused() {
    let mut venue = venue_at(&ruled_config(rules()), "100");

    let outcome = venue.open_order(limit_request("off", Side::Buy, "1", "90.005", gtc()));

    assert_eq!(
        refusal(&venue, &outcome),
        off_grid(OrderField::Price, "90.005", "0.01")
    );
    assert_eq!(free_quote(&venue), d("100000"), "nothing held");
}

#[test]
fn a_quantity_off_the_step_or_out_of_range_is_refused() {
    for (quantity, error) in [
        ("1.0005", off_grid(OrderField::Quantity, "1.0005", "0.001")),
        (
            "101",
            out_of_bounds(OrderField::Quantity, Bound::Max, "100", "101"),
        ),
    ] {
        let mut venue = venue_at(&ruled_config(rules()), "100");

        let outcome = venue.open_order(limit_request("off", Side::Buy, quantity, "90", gtc()));

        assert_eq!(refusal(&venue, &outcome), error, "{quantity}");
    }
}

#[test]
fn a_limit_notional_below_the_minimum_is_refused() {
    let mut venue = venue_at(&ruled_config(rules()), "100");

    let outcome = venue.open_order(limit_request("small", Side::Buy, "0.05", "90", gtc()));

    assert_eq!(
        refusal(&venue, &outcome),
        out_of_bounds(OrderField::Notional, Bound::Min, "5", "4.5")
    );
}

#[test]
fn a_market_quantity_above_the_market_maximum_is_refused() {
    let mut venue = venue_at(&ruled_config(rules()), "100");

    let outcome = venue.open_order(market_buy("big", "11"));

    assert_eq!(
        refusal(&venue, &outcome),
        out_of_bounds(OrderField::Quantity, Bound::Max, "10", "11")
    );
}

#[test]
fn a_market_order_below_the_market_notional_is_refused_at_the_price_before_slippage() {
    // 0.05 at 99.5 is 4.975, under the minimum of 5. Slipped 1% to 100.495 it would be 5.02475.
    let config = ruled_config(rules()).with_slippage(SlippageConfig::fixed(d("0.01")).unwrap());
    let mut venue = venue_at(&config, "99.5");

    let outcome = venue.open_order(market_buy("small", "0.05"));

    assert_eq!(
        refusal(&venue, &outcome),
        out_of_bounds(OrderField::Notional, Bound::Min, "5", "4.975"),
        "measured at the price before slippage, on the quantity asked for"
    );
}

#[test]
fn a_market_order_at_the_market_notional_minimum_fills() {
    let mut venue = venue_at(&ruled_config(rules()), "100");

    let outcome = venue.open_order(market_buy("enough", "0.05"));

    assert!(is_filled(&outcome), "{:?}", outcome.response.state);
}

#[test]
fn the_market_notional_rule_does_not_apply_to_a_limit_order() {
    // A marketable limit order is under the notional rule alone, which a limit of 100 for 0.06
    // meets; the market notional's minimum of 10 does not apply to it.
    let config =
        ruled_config(rules().with_market_notional(ValueRule::new(d("10"), d("0"), d("0"))));
    let mut venue = venue_at(&config, "100");

    let outcome = venue.open_order(limit_request("limit", Side::Buy, "0.06", "100", gtc()));

    assert!(is_filled(&outcome), "{:?}", outcome.response.state);

    let mut fresh = venue_at(&config, "100");
    let outcome = fresh.open_order(market_buy("market", "0.06"));
    assert_eq!(
        refusal(&fresh, &outcome),
        out_of_bounds(OrderField::Notional, Bound::Min, "10", "6"),
        "a market order of the same size is refused"
    );
}

#[test]
fn a_market_order_with_no_price_is_still_refused_as_unpriceable() {
    let mut venue = SimulatedVenue::new_market_driven(&ruled_config(rules()), spot_instruments());
    advance(&mut venue, time(1));

    let outcome = venue.open_order(market_buy("blind", "1"));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(reason)
            ))) if reason.contains("cannot price")
        ),
        "{:?}",
        outcome.response.state
    );
}

#[test]
fn an_instrument_without_rules_is_not_checked() {
    let config = spot_config("10", "100000", FeeModelConfig::default());
    let mut venue = venue_at(&config, "100");

    let outcome = venue.open_order(limit_request("off", Side::Buy, "1.0005", "90.005", gtc()));

    assert!(rested_open(&outcome).filled_quantity.is_zero());
    assert_eq!(venue.order_rules(&instrument_name()), None);
}

#[test]
fn the_venue_reports_the_rules_it_checks() {
    let venue = venue_at(&ruled_config(rules()), "100");

    assert_eq!(venue.order_rules(&instrument_name()), Some(&rules()));
    assert_eq!(
        venue.order_rules(&InstrumentNameExchange::new("ETHUSDT")),
        None
    );
}

#[test]
fn rules_built_from_a_map_are_the_same_as_rules_given_one_by_one() {
    let by_map = spot_config("10", "100000", FeeModelConfig::default()).with_order_rules_from(
        std::collections::HashMap::from([(instrument_name(), rules())]),
    );

    assert_eq!(by_map.order_rules, ruled_config(rules()).order_rules);
}

// --- A held market order, at its release -------------------------------------------------------

/// A venue holding market orders for the next bar, under `rules`, fed one bar that closed at 100.
fn holding_under(rules: OrderRules) -> SimulatedVenue {
    let config = ruled_config(rules)
        .with_market_order_timing(MarketOrderTiming::NextBar(NextBarPrice::Open));
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(feed_bar(&mut venue, 1, ohlc("95", "105", "90", "100")).is_empty());
    venue
}

/// A market notional of at least 150.
fn notional_at_least_150() -> OrderRules {
    OrderRules::default().with_market_notional(ValueRule::new(d("150"), d("0"), d("0")))
}

#[test]
fn a_held_market_order_is_checked_at_release_not_on_the_price_it_arrived_at() {
    let mut venue = holding_under(notional_at_least_150());

    // 100 at the close it arrives on, under the minimum: held all the same.
    let outcome = venue.open_order(ioc_buy("held", "1"));
    assert!(rested_open(&outcome).filled_quantity.is_zero());

    let events = feed_bar(&mut venue, 2, ohlc("160", "170", "150", "165"));

    assert_eq!(events.len(), 4, "filled at the open of 160: {events:?}");
    assert_eq!(trade_of(&events[2]).price, d("160"));
}

#[test]
fn a_held_market_order_breaking_the_market_notional_at_release_ends_refused() {
    let mut venue = holding_under(notional_at_least_150());
    let held = venue.open_order(ioc_buy("held", "1"));
    assert!(rested_open(&held).filled_quantity.is_zero());
    assert_eq!(free_quote(&venue), d("99900"));

    let events = feed_bar(&mut venue, 2, ohlc("140", "145", "135", "142"));

    let [released, ended] = events.as_slice() else {
        panic!("the released hold, then the order: {events:?}");
    };
    assert_eq!(balance_of(released).balance.free, d("100000"));
    let refused = out_of_bounds(OrderField::Notional, Bound::Min, "150", "140");
    let state = OrderState::Inactive(InactiveOrderState::OpenFailed(refused.clone()));
    assert_eq!(order_of(ended).state, state, "the arrival's typed refusal");
    assert_eq!(order_of(ended).key.cid, ClientOrderId::new("held"));
    assert!(venue.trades(time(0)).is_empty());
    assert!(venue.orders_open(&[]).is_empty());
    assert_eq!(
        venue
            .orders_ended(std::slice::from_ref(&held.response.key))
            .into_iter()
            .map(|order| order.state)
            .collect::<Vec<_>>(),
        [InactiveOrderState::OpenFailed(refused)],
        "a lookup reports it as the stream did"
    );
}

#[test]
fn a_refused_held_order_survives_a_snapshot() {
    let config = ruled_config(notional_at_least_150())
        .with_market_order_timing(MarketOrderTiming::NextBar(NextBarPrice::Open));
    let mut venue = holding_under(notional_at_least_150());
    let held = venue.open_order(ioc_buy("held", "1"));
    let _ = feed_bar(&mut venue, 2, ohlc("140", "145", "135", "142"));
    let snapshot = venue.snapshot();

    let restored = SimulatedVenue::from_snapshot(
        &config,
        spot_instruments(),
        serde_json::from_str(&serde_json::to_string(&snapshot).unwrap()).unwrap(),
    )
    .unwrap();

    assert_eq!(restored.snapshot(), snapshot);
    let cid = &held.response.key.cid;
    assert!(venue.account.order_ended(cid).is_some());
    assert_eq!(
        restored.account.order_ended(cid),
        venue.account.order_ended(cid)
    );
    assert_eq!(
        restored.order_rules(&instrument_name()),
        Some(&notional_at_least_150())
    );
}

// --- Amends ------------------------------------------------------------------------------------

/// A venue under `rules()` resting a buy of 1 at 90 under `resting`, the market last at 100.
fn resting_under_rules() -> SimulatedVenue {
    let mut venue = venue_at(&ruled_config(rules()), "100");
    let outcome = venue.open_order(limit_request("resting", Side::Buy, "1", "90", gtc()));
    assert!(rested_open(&outcome).filled_quantity.is_zero());
    advance(&mut venue, time(2));
    venue
}

fn amend_refusal(outcome: &AmendOutcome) -> UnindexedOrderError {
    assert!(outcome.events.is_empty(), "{:?}", outcome.events);
    match &outcome.response.state {
        Err(error) => error.clone(),
        Ok(amended) => panic!("expected the amend to be refused, got: {amended:?}"),
    }
}

#[test]
fn an_amend_to_a_price_off_the_tick_is_refused_with_nothing_moved() {
    let mut venue = resting_under_rules();
    let before = venue
        .account
        .orders()
        .get(&ClientOrderId::new("resting"))
        .cloned();

    let outcome = amend(&mut venue, "resting", price("89.995"));

    assert_eq!(
        amend_refusal(&outcome),
        off_grid(OrderField::Price, "89.995", "0.01")
    );
    assert_eq!(
        venue
            .account
            .orders()
            .get(&ClientOrderId::new("resting"))
            .cloned(),
        before
    );
}

#[test]
fn an_amend_is_checked_on_the_whole_order_as_amended() {
    // A quantity of 0.05 at the unchanged price of 90 is a notional of 4.5.
    let mut venue = resting_under_rules();

    let outcome = amend(
        &mut venue,
        "resting",
        AmendPatch::default().with_quantity(d("0.05")),
    );

    assert_eq!(
        amend_refusal(&outcome),
        out_of_bounds(OrderField::Notional, Bound::Min, "5", "4.5")
    );
}

#[test]
fn an_amend_the_rules_allow_is_made() {
    let mut venue = resting_under_rules();

    let outcome = amend(&mut venue, "resting", price("89.99"));

    assert_eq!(amended(&outcome).price, Some(d("89.99")));
}

#[test]
fn an_external_amend_is_checked_too() {
    let mut venue = resting_under_rules();

    let refused = venue.modify_externally(
        &ClientOrderId::new("resting"),
        AmendPatch::default().with_quantity(d("1.0005")),
    );

    assert_eq!(
        refused,
        Err(off_grid(OrderField::Quantity, "1.0005", "0.001"))
    );
}

// --- Construction ------------------------------------------------------------------------------

#[test]
fn rules_for_an_instrument_the_venue_does_not_trade_are_refused() {
    let config =
        ruled_config(rules()).with_order_rules(InstrumentNameExchange::new("BTCUSD"), rules());

    for refused in [
        SimulatedVenue::try_new(&config, spot_instruments()).err(),
        SimulatedVenue::try_new_market_driven(&config, spot_instruments()).err(),
    ] {
        assert_eq!(
            refused,
            Some(VenueConfigError::OrderRules(
                InvalidOrderRules::UnknownInstrument(InstrumentNameExchange::new("BTCUSD"))
            ))
        );
    }
}

#[test]
fn a_rule_with_its_minimum_above_its_maximum_is_refused_naming_it() {
    let inverted = rules().with_trailing_offsets(
        ValueRule::default(),
        ValueRule::new(d("50"), d("10"), d("0")),
    );

    let refused = SimulatedVenue::try_new(&ruled_config(inverted), spot_instruments()).err();

    let error = InvalidOrderRules::MinAboveMax {
        instrument: instrument_name(),
        rule: OrderRulesField::TrailingOffsetBelow,
        min: d("50"),
        max: d("10"),
    };
    assert_eq!(
        error.to_string(),
        "the trailing_offset_below rule of BTCUSDT has a minimum 50 above its maximum 10"
    );
    assert_eq!(refused, Some(VenueConfigError::OrderRules(error)));
}

#[test]
fn a_rule_with_equal_minimum_and_maximum_is_accepted() {
    let pinned = rules().with_market_notional(ValueRule::new(d("10"), d("10"), d("0")));

    assert!(SimulatedVenue::try_new(&ruled_config(pinned), spot_instruments()).is_ok());
}

#[test]
fn a_restore_refuses_rules_it_cannot_enforce() {
    let venue = venue_at(&ruled_config(rules()), "100");
    let config =
        ruled_config(rules()).with_order_rules(InstrumentNameExchange::new("BTCUSD"), rules());

    let refused = SimulatedVenue::from_snapshot(&config, spot_instruments(), venue.snapshot());

    assert_eq!(
        refused.err(),
        Some(VenueRestoreError::OrderRules(
            InvalidOrderRules::UnknownInstrument(InstrumentNameExchange::new("BTCUSD"))
        ))
    );
}

#[test]
fn a_config_keeps_its_rules_through_serde_and_reads_none_when_they_are_left_out() {
    let config = ruled_config(rules());

    let json = serde_json::to_string(&config).unwrap();
    assert_eq!(
        serde_json::from_str::<MockExecutionConfig>(&json).unwrap(),
        config
    );

    let plain =
        serde_json::to_value(spot_config("10", "100000", FeeModelConfig::default())).unwrap();
    assert!(plain.get("order_rules").is_none(), "left out when empty");
    let read = serde_json::from_value::<MockExecutionConfig>(plain).unwrap();
    assert!(read.order_rules.is_empty());
}
