//! A corporate action applied to the venue: a stock split cancels every order on its instrument,
//! and the venue trades nothing on it until it is observed again.

use super::amend::{amend, amended, price};
use super::*;
use rustrade_instrument::corporate_action::{CorporateActionKind, SplitRatio};

fn split(ratio: &str) -> CorporateActionKind {
    CorporateActionKind::StockSplit {
        ratio: SplitRatio::new(d(ratio)).unwrap(),
    }
}

fn apply_split(venue: &mut SimulatedVenue, id: &str, seconds: i64) -> CorporateActionOutcome {
    venue.apply_corporate_action(id, &instrument_name(), &split("2"), time(seconds))
}

/// A market-driven venue holding 10 BTC and 1,000,000 USDT at `time(1)`, quoting 49000/49100.
fn quoted_venue() -> SimulatedVenue {
    let mut venue = make_market_venue("10", "1000000");
    advance(&mut venue, time(1));
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("49000", "49100")),
                time(1)
            )
            .is_empty()
    );
    venue
}

fn rest(venue: &mut SimulatedVenue, cid: &str, side: Side, quantity: &str, limit: &str) {
    let outcome = venue.open_order(limit_request(cid, side, quantity, limit, gtc()));
    assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);
}

fn cancelled_cids(events: &[UnindexedAccountEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(report) => Some(&report.0.order),
            _ => None,
        })
        .map(|order| {
            assert!(
                matches!(
                    order.state,
                    OrderState::Inactive(InactiveOrderState::Cancelled(_))
                ),
                "a split reports each order cancelled: {order:?}"
            );
            order.key.cid.to_string()
        })
        .collect()
}

#[test]
fn a_split_cancels_every_order_on_its_instrument_releasing_each_hold() {
    let mut venue = quoted_venue();
    rest(&mut venue, "bid", Side::Buy, "1", "48000");
    rest(&mut venue, "ask", Side::Sell, "2", "50000");

    let outcome = apply_split(&mut venue, "split-1", 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    assert_eq!(
        cancelled_cids(&outcome.events),
        ["ask", "bid"],
        "by client order id"
    );
    // `[balance, order]` per order, the balance first.
    assert_eq!(outcome.events.len(), 4, "{:#?}", outcome.events);
    assert_eq!(balance_of(&outcome.events[0]).balance.free, d("10"));
    assert_eq!(balance_of(&outcome.events[2]).balance.free, d("1000000"));
    let cancelled = order_of(&outcome.events[1]);
    let OrderState::Inactive(InactiveOrderState::Cancelled(state)) = &cancelled.state else {
        unreachable!("checked above");
    };
    assert_eq!(state.time_exchange, time(5), "stamped at the split");
    assert_eq!(state.filled_quantity, Some(Decimal::ZERO));
    assert!(venue.orders_open(&[]).is_empty());
}

#[test]
fn a_split_leaves_the_orders_of_other_instruments_alone() {
    let eth = InstrumentNameExchange::new("ETHUSDT");
    let mut instruments = spot_instruments();
    let mut other = instruments[&instrument_name()].clone();
    other.name_exchange = eth.clone();
    other.name_internal = InstrumentNameInternal::new("ethusdt");
    other.underlying.base = AssetNameExchange::new("ETH");
    instruments.insert(eth.clone(), other);
    let config = config_from_balances(
        vec![
            funded(base(), d("10")),
            funded(AssetNameExchange::new("ETH"), d("10")),
            funded(quote(), d("1000000")),
        ],
        maker_taker_fee(),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, instruments);
    advance(&mut venue, time(1));
    rest(&mut venue, "btc-bid", Side::Buy, "1", "48000");
    let mut eth_bid = limit_request("eth-bid", Side::Buy, "1", "3000", gtc());
    eth_bid.key.instrument = eth.clone();
    assert!(matches!(
        venue.open_order(eth_bid).response.state,
        OrderState::Active(ActiveOrderState::Open(_))
    ));

    let outcome = apply_split(&mut venue, "split-1", 5);

    assert_eq!(cancelled_cids(&outcome.events), ["btc-bid"]);
    let open = venue.orders_open(&[]);
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].key.instrument, eth);
}

#[test]
fn a_split_cancels_a_market_order_held_for_the_next_bar() {
    let mut venue = venue_holding(NextBarPrice::Open, "1000000");
    let held = venue.open_order(ioc_buy("held", "1"));
    assert!(
        matches!(
            held.response.state,
            OrderState::Active(ActiveOrderState::Open(_))
        ),
        "held for the next bar: {:?}",
        held.response.state
    );

    let outcome = apply_split(&mut venue, "split-1", 5);

    assert_eq!(cancelled_cids(&outcome.events), ["held"]);
    assert!(venue.account.orders().is_empty());
}

/// Under `NextBar` the venue still holds the pre-split bar, but a market order is not held for the
/// next one: the hold is costed at the market, which a split leaves unpriced.
#[test]
fn after_a_split_a_market_order_is_refused_rather_than_held_for_the_next_bar() {
    let mut venue = venue_holding(NextBarPrice::Open, "1000000");
    assert!(apply_split(&mut venue, "split-1", 5).events.is_empty());
    assert!(
        venue.market(&instrument_name()).unwrap().bar.is_some(),
        "the pre-split bar is kept"
    );

    let refused = venue.open_order(ioc_buy("after", "1"));

    match &refused.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(reason),
        ))) => assert!(reason.contains("since a split"), "{reason}"),
        other => panic!("expected the market order refused, got: {other:?}"),
    }
    assert!(venue.account.orders().is_empty());
}

#[test]
fn after_a_split_the_market_values_holdings_but_trades_nothing_until_observed_again() {
    let mut venue = quoted_venue();
    assert!(apply_split(&mut venue, "split-1", 5).events.is_empty());

    let market = venue
        .market(&instrument_name())
        .expect("kept for valuation");
    assert_eq!(market.snapshot, book("49000", "49100"));
    assert_eq!(market.split_at, Some(time(5)));

    // A market order is refused as unpriced, naming the split.
    let refused = venue.open_order(buy_request("1", None));
    match &refused.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(reason),
        ))) => assert!(reason.contains("since a split"), "{reason}"),
        other => panic!("expected the market order refused, got: {other:?}"),
    }

    // A buy the pre-split ask would cross rests instead.
    rest(&mut venue, "bid", Side::Buy, "1", "50000");

    // The first observation after the split clears the mark, and trades as any other.
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("24500", "24550")),
        time(6),
    );
    assert_eq!(venue.market(&instrument_name()).unwrap().split_at, None);
    assert!(
        events
            .iter()
            .any(|event| matches!(&event.kind, AccountEventKind::Trade(_))),
        "the post-split ask crosses the resting bid: {events:#?}"
    );
}

#[test]
fn an_observation_that_is_not_fresh_also_ends_the_wait_after_a_split() {
    let mut venue = quoted_venue();
    let _ = apply_split(&mut venue, "split-1", 5);

    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("24500", "24550")).with_fresh(false),
        time(6),
    );

    assert_eq!(venue.market(&instrument_name()).unwrap().split_at, None);
}

#[test]
fn after_a_split_an_amend_is_not_crossed_by_the_pre_split_market() {
    let mut venue = quoted_venue();
    let _ = apply_split(&mut venue, "split-1", 5);
    rest(&mut venue, "resting", Side::Buy, "1", "24000");

    // The pre-split ask is 49100: an amend to 50000 would cross it.
    let outcome = amend(&mut venue, "resting", price("50000"));

    let amended = amended(&outcome);
    assert_eq!(amended.price, Some(d("50000")));
    assert_eq!(amended.open.filled_quantity, Decimal::ZERO, "{amended:?}");
    assert!(
        !outcome
            .events
            .iter()
            .any(|event| matches!(&event.kind, AccountEventKind::Trade(_))),
        "nothing traded: {:#?}",
        outcome.events
    );
}

#[test]
fn the_same_action_applied_twice_changes_nothing_the_second_time() {
    let mut venue = quoted_venue();
    let _ = apply_split(&mut venue, "split-1", 5);
    // Observed again, then an order placed after the split.
    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("24500", "24550")),
        time(6),
    );
    rest(&mut venue, "bid", Side::Buy, "1", "24000");

    let again = apply_split(&mut venue, "split-1", 7);

    assert_eq!(again.response, CorporateActionResponse::AlreadyApplied);
    assert!(again.events.is_empty());
    assert_eq!(venue.orders_open(&[]).len(), 1);
    assert_eq!(venue.market(&instrument_name()).unwrap().split_at, None);

    // Another id is another action.
    let another = apply_split(&mut venue, "split-2", 8);
    assert_eq!(another.response, CorporateActionResponse::Applied);
    assert_eq!(cancelled_cids(&another.events), ["bid"]);
}

#[test]
fn a_refused_action_changes_nothing_and_does_not_record_its_id() {
    let mut venue = quoted_venue();
    rest(&mut venue, "bid", Side::Buy, "1", "48000");

    let unknown = venue.apply_corporate_action(
        "split-1",
        &InstrumentNameExchange::new("NOPE"),
        &split("2"),
        time(5),
    );

    assert_eq!(
        unknown.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::UnknownInstrument)
    );
    assert!(unknown.events.is_empty());
    assert_eq!(venue.orders_open(&[]).len(), 1);
    assert_eq!(venue.market(&instrument_name()).unwrap().split_at, None);

    // The id was not recorded, so the same action applies once it names the right instrument.
    let applied = apply_split(&mut venue, "split-1", 6);
    assert_eq!(applied.response, CorporateActionResponse::Applied);
    assert_eq!(cancelled_cids(&applied.events), ["bid"]);
}

#[test]
fn a_split_of_an_instrument_that_is_not_the_deliverable_equity_is_refused() {
    let mut venue = make_cfd_venue("100000", FeeModelConfig::default());

    let outcome =
        venue.apply_corporate_action("split-1", &cfd_instrument_name(), &split("2"), time(5));

    assert_eq!(
        outcome.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::InstrumentKindNotSupported)
    );
    assert!(outcome.events.is_empty());
}

#[test]
fn an_order_whose_deadline_falls_at_the_split_expires_rather_than_being_cancelled() {
    let mut venue = quoted_venue();
    let gtd = TimeInForce::GoodTillDate { expiry: time(5) };
    let outcome = venue.open_order(limit_request("gtd", Side::Buy, "1", "48000", gtd));
    assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);

    let outcome = apply_split(&mut venue, "split-1", 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    let states = outcome
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(report) => Some(&report.0.order.state),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(
            states.as_slice(),
            [OrderState::Inactive(InactiveOrderState::Expired(_))]
        ),
        "{states:?}"
    );
}

#[test]
fn a_restored_venue_remembers_the_actions_it_applied() {
    let mut venue = quoted_venue();
    let _ = apply_split(&mut venue, "split-1", 5);
    let snapshot = venue.snapshot();
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(
        json["corporate_actions_applied"],
        serde_json::json!(["split-1"])
    );

    let config =
        spot_config_with_fill("10", "1000000", maker_taker_fee(), SimFillConfig::default());
    let mut restored = SimulatedVenue::from_snapshot(
        &config,
        spot_instruments(),
        serde_json::from_value(json).unwrap(),
    )
    .unwrap();

    assert_eq!(
        apply_split(&mut restored, "split-1", 6).response,
        CorporateActionResponse::AlreadyApplied
    );
}

#[test]
fn a_snapshot_of_a_venue_that_applied_no_action_is_written_as_before() {
    let json = serde_json::to_value(quoted_venue().snapshot()).unwrap();

    assert!(
        json.get("corporate_actions_applied").is_none(),
        "left out when empty, so an older reader sees no new field: {json}"
    );
}
