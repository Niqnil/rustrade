//! A corporate action applied to the venue: a stock split cancels or adjusts the orders on its
//! instrument, restates the account's holding of it and the market that values it, and the venue
//! trades nothing on it until it is observed again.

use super::amend::{amend, amended, price};
use super::*;
use crate::client::mock::SplitAction;
use rustrade_instrument::{
    corporate_action::{CorporateActionKind, SplitRatio, SplitRoundingPolicy},
    instrument::spec::{
        InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
        OrderQuantityUnits,
    },
};

fn split(ratio: &str) -> CorporateActionKind {
    CorporateActionKind::StockSplit {
        ratio: SplitRatio::new(d(ratio)).unwrap(),
    }
}

fn apply_split(venue: &mut SimulatedVenue, id: &str, seconds: i64) -> CorporateActionOutcome {
    venue.apply_corporate_action(
        id,
        &instrument_name(),
        &split("2"),
        SplitRoundingPolicy::Fractional,
        time(seconds),
    )
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
    // `[balance, order]` per order, the balance first, then the holding restated.
    assert_eq!(outcome.events.len(), 5, "{:#?}", outcome.events);
    assert_eq!(balance_of(&outcome.events[0]).balance.free, d("10"));
    assert_eq!(balance_of(&outcome.events[2]).balance.free, d("1000000"));
    let restated = balance_of(&outcome.events[4]);
    assert_eq!(restated.asset, base());
    assert_eq!(
        (restated.balance.total, restated.balance.free),
        (d("20"), d("20"))
    );
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
    let bar = venue.market(&instrument_name()).unwrap().bar.unwrap();
    assert!(cancelled_cids(&apply_split(&mut venue, "split-1", 5).events).is_empty());
    let restated = venue.market(&instrument_name()).unwrap().bar.unwrap();
    let half = |price: Decimal| price / d("2");
    assert_eq!(
        restated,
        MarketBar::new(
            half(bar.open),
            half(bar.high),
            half(bar.low),
            half(bar.close)
        ),
        "the bar is kept, restated"
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
    assert!(cancelled_cids(&apply_split(&mut venue, "split-1", 5).events).is_empty());

    let market = venue
        .market(&instrument_name())
        .expect("kept for valuation");
    assert_eq!(market.snapshot, book("24500", "24550"), "restated");
    assert_eq!(market.split_at, Some(time(5)));

    // A market order is refused as unpriced, naming the split.
    let refused = venue.open_order(buy_request("1", None));
    match &refused.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(reason),
        ))) => assert!(reason.contains("since a split"), "{reason}"),
        other => panic!("expected the market order refused, got: {other:?}"),
    }

    // A buy the restated ask would cross rests instead.
    rest(&mut venue, "bid", Side::Buy, "1", "30000");

    // The first observation after the split clears the mark, and trades as any other.
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("24400", "24450")),
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

    // The restated ask is 24550: an amend to 50000 would cross it.
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
        SplitRoundingPolicy::Fractional,
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

    let outcome = venue.apply_corporate_action(
        "split-1",
        &cfd_instrument_name(),
        &split("2"),
        SplitRoundingPolicy::Fractional,
        time(5),
    );

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

// -------------------------------------------------------------------------------------------
// The split rule and the restated holding.
// -------------------------------------------------------------------------------------------

fn apply(
    venue: &mut SimulatedVenue,
    ratio: &str,
    policy: SplitRoundingPolicy,
    seconds: i64,
) -> CorporateActionOutcome {
    venue.apply_corporate_action(
        "split-1",
        &instrument_name(),
        &split(ratio),
        policy,
        time(seconds),
    )
}

/// A market-driven venue built from `config` and `instruments`, at `time(1)`, quoting
/// 49000/49100.
fn venue_with(
    config: &MockExecutionConfig,
    instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(config, instruments);
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

/// `btc` BTC and `usdt` USDT, maker 2bp, adjusting buy limits on a forward split.
fn adjusting(btc: &str, usdt: &str) -> MockExecutionConfig {
    spot_config(btc, usdt, maker_taker_fee()).with_split(SplitConfig::ADJUST_FORWARD)
}

/// The spot instrument with a tick size of `tick` and a quantity step of `step`.
fn instruments_with_spec(
    tick: &str,
    step: &str,
) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    let mut instruments = spot_instruments();
    for instrument in instruments.values_mut() {
        instrument.spec = Some(InstrumentSpec::new(
            InstrumentSpecPrice::new(Decimal::ZERO, d(tick)),
            InstrumentSpecQuantity::new(OrderQuantityUnits::Base, Decimal::ZERO, d(step)),
            InstrumentSpecNotional::new(Decimal::ZERO),
        ));
    }
    instruments
}

/// Each buy resting on the book, best first: its client order id, price, quantity and filled
/// quantity.
fn bids(venue: &SimulatedVenue) -> Vec<(String, Decimal, Decimal, Decimal)> {
    venue
        .account
        .orders()
        .resting(&instrument_name(), Side::Buy)
        .map(|order| {
            (
                order.key.cid.to_string(),
                order.price.unwrap(),
                order.quantity,
                order.state.filled_quantity,
            )
        })
        .collect()
}

fn bid(
    cid: &str,
    price: &str,
    quantity: &str,
    filled: &str,
) -> (String, Decimal, Decimal, Decimal) {
    (cid.to_string(), d(price), d(quantity), d(filled))
}

#[test]
fn adjust_restates_a_buy_limit_and_cancels_every_other_order() {
    let mut venue = venue_with(&adjusting("10", "1000000"), spot_instruments());
    rest(&mut venue, "bid", Side::Buy, "1", "48000");
    rest(&mut venue, "ask", Side::Sell, "2", "50000");
    let id = venue
        .account
        .orders()
        .get(&ClientOrderId::new("bid"))
        .unwrap()
        .state
        .id
        .clone();
    let free_quote = venue.account.free(&quote());

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    // [the ask's release, the ask cancelled, the holding restated, the bid's hold recommitted, the
    // bid restated]
    assert_eq!(outcome.events.len(), 5, "{:#?}", outcome.events);
    assert_eq!(
        order_of(&outcome.events[1]).key.cid,
        ClientOrderId::new("ask")
    );
    assert_eq!(cancelled_cids(&outcome.events[..2]), ["ask"]);
    let holding = balance_of(&outcome.events[2]);
    assert_eq!(holding.asset, base());
    assert_eq!(
        (holding.balance.total, holding.balance.free),
        (d("20"), d("20"))
    );
    let hold = balance_of(&outcome.events[3]);
    assert_eq!(hold.asset, quote());
    assert_eq!(
        hold.balance.free, free_quote,
        "2 at 24000 holds what 1 at 48000 held"
    );

    let adjusted = order_of(&outcome.events[4]);
    assert_eq!(adjusted.key.cid, ClientOrderId::new("bid"));
    assert_eq!(
        (adjusted.price, adjusted.quantity),
        (Some(d("24000")), d("2"))
    );
    let OrderState::Active(ActiveOrderState::Open(open)) = &adjusted.state else {
        panic!("the adjusted order is open: {adjusted:?}");
    };
    assert_eq!(open.id, id, "it keeps its venue order id");
    assert_eq!(open.time_exchange, time(5), "stamped at the split");
    assert_eq!(bids(&venue), [bid("bid", "24000", "2", "0")]);
    assert_eq!(venue.orders_open(&[]).len(), 1);
}

#[test]
fn adjust_on_a_forward_split_still_cancels_every_order_on_a_reverse_split() {
    let mut venue = venue_with(&adjusting("10", "1000000"), spot_instruments());
    rest(&mut venue, "bid", Side::Buy, "1", "48000");

    let outcome = apply(&mut venue, "0.5", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(cancelled_cids(&outcome.events), ["bid"]);
    assert!(venue.orders_open(&[]).is_empty());
}

#[test]
fn a_reverse_split_adjusts_where_the_venue_is_configured_to() {
    let config = spot_config("10", "1000000", maker_taker_fee())
        .with_split(SplitConfig::ADJUST_FORWARD.with_reverse(SplitAction::Adjust));
    let mut venue = venue_with(&config, spot_instruments());
    rest(&mut venue, "bid", Side::Buy, "1", "48000");

    let _ = apply(&mut venue, "0.5", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(bids(&venue), [bid("bid", "96000", "0.5", "0")]);
}

#[test]
fn adjust_rounds_the_price_down_to_the_tick_and_the_quantity_down_to_a_whole_share_under_floor() {
    let mut venue = venue_with(
        &adjusting("10", "1000000"),
        instruments_with_spec("1", "0.01"),
    );
    rest(&mut venue, "bid", Side::Buy, "0.5", "48001");

    let _ = apply(&mut venue, "3", SplitRoundingPolicy::Floor, 5);

    // 48001 / 3 = 16000.33 rounds down to 16000; 0.5 x 3 = 1.5 rounds down to 1.
    assert_eq!(bids(&venue), [bid("bid", "16000", "1", "0")]);
}

#[test]
fn adjust_rounds_the_quantity_down_to_the_step_under_fractional() {
    let mut venue = venue_with(
        &adjusting("10", "1000000"),
        instruments_with_spec("1", "0.4"),
    );
    rest(&mut venue, "bid", Side::Buy, "0.5", "48001");

    let _ = apply(&mut venue, "3", SplitRoundingPolicy::Fractional, 5);

    // 0.5 x 3 = 1.5 rounds down to 1.2, a whole number of steps of 0.4.
    assert_eq!(bids(&venue), [bid("bid", "16000", "1.2", "0")]);
}

#[test]
fn adjust_cancels_an_order_whose_price_or_quantity_rounds_to_zero() {
    let mut venue = venue_with(&adjusting("10", "1000000"), instruments_with_spec("1", "0"));
    rest(&mut venue, "tiny", Side::Buy, "0.25", "48000");
    rest(&mut venue, "cheap", Side::Buy, "1", "1");
    rest(&mut venue, "kept", Side::Buy, "1", "48000");

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Floor, 5);

    // 0.25 x 2 = 0.5 floors to no share; 1 / 2 = 0.5 rounds to no tick.
    assert_eq!(
        cancelled_cids(
            &outcome
                .events
                .iter()
                .filter(|event| matches!(
                    &event.kind,
                    AccountEventKind::OrderSnapshot(report)
                        if !matches!(report.0.order.state, OrderState::Active(_))
                ))
                .cloned()
                .collect::<Vec<_>>()
        ),
        ["cheap", "tiny"]
    );
    assert_eq!(bids(&venue), [bid("kept", "24000", "2", "0")]);
}

#[test]
fn adjusted_orders_keep_their_order_among_themselves() {
    let mut venue = venue_with(&adjusting("10", "1000000"), spot_instruments());
    rest(&mut venue, "a", Side::Buy, "1", "48000");
    rest(&mut venue, "b", Side::Buy, "1", "48000");
    rest(&mut venue, "c", Side::Buy, "1", "48500");

    let _ = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(
        bids(&venue),
        [
            bid("c", "24250", "2", "0"),
            bid("a", "24000", "2", "0"),
            bid("b", "24000", "2", "0"),
        ]
    );
}

#[test]
fn adjust_restates_what_an_order_has_filled_exactly_and_rounds_only_the_rest() {
    let mut venue = venue_with(&adjusting("10", "1000000"), spot_instruments());
    // Only 0.5 is offered at the ask, so a bid of 1.75 crossing it fills 0.5 and rests 1.25.
    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("49000", "49100"))
            .with_depth(MarketDepth::new(Some(d("1")), Some(d("0.5")))),
        time(2),
    );
    let _ = venue.open_order(limit_request("bid", Side::Buy, "1.75", "49100", gtc()));
    assert_eq!(bids(&venue), [bid("bid", "49100", "1.75", "0.5")]);

    let _ = apply(&mut venue, "2", SplitRoundingPolicy::Floor, 5);

    // Filled 0.5 x 2 = 1, kept exact; the rest, 1.25 x 2 = 2.5, floors to 2.
    assert_eq!(bids(&venue), [bid("bid", "24550", "3", "1")]);
}

#[test]
fn adjust_leaves_an_order_seeded_through_initial_state_holding_nothing() {
    let config = spot_config_holding(
        "10",
        "1000000",
        seeded_gtd("seeded", "48000", time(0), time(1000)),
    )
    .with_split(SplitConfig::ADJUST_FORWARD);
    let mut venue = venue_with(&config, spot_instruments());

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    // [the holding restated, the order restated]: nothing held, so no hold is recommitted.
    assert_eq!(outcome.events.len(), 2, "{:#?}", outcome.events);
    assert_eq!(balance_of(&outcome.events[0]).asset, base());
    assert_eq!(order_of(&outcome.events[1]).quantity, d("0.02"));
    assert_eq!(bids(&venue), [bid("seeded", "24000", "0.02", "0")]);
    let seeded = venue
        .account
        .orders()
        .resting_order(&ClientOrderId::new("seeded"))
        .unwrap();
    assert_eq!(seeded.reservation, None);
}

#[test]
fn adjust_cancels_an_order_whose_hold_the_account_can_no_longer_afford() {
    // The maker fee rises from 2bp to 1% at time(3), after the bid rests.
    let fee = FeeModelConfig::Dated(
        DatedFeeModel::new(
            maker_taker_fee(),
            vec![FeeChange::new(
                time(3),
                FeeModelConfig::Percentage(PercentageFeeModel::maker_taker(d("0.01"), d("0.01"))),
            )],
        )
        .unwrap(),
    );
    let config = spot_config("10", "48010", fee).with_split(SplitConfig::ADJUST_FORWARD);
    let mut venue = venue_with(&config, spot_instruments());
    rest(&mut venue, "bid", Side::Buy, "1", "48000");

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    // 2 at 24000 now holds 48480, more than the 48010 held.
    assert_eq!(cancelled_cids(&outcome.events), ["bid"]);
    assert!(venue.orders_open(&[]).is_empty());
    assert_eq!(
        venue.account.free(&quote()),
        d("48010"),
        "the hold is released"
    );
}

#[test]
fn floor_pays_cash_in_lieu_for_the_fraction_of_a_share_at_the_restated_mid() {
    let mut venue = venue_with(
        &spot_config("3", "1000000", maker_taker_fee()),
        spot_instruments(),
    );

    let outcome = apply(&mut venue, "0.5", SplitRoundingPolicy::Floor, 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    assert_eq!(outcome.events.len(), 2, "{:#?}", outcome.events);
    let holding = balance_of(&outcome.events[0]);
    assert_eq!(holding.asset, base());
    assert_eq!(
        (holding.balance.total, holding.balance.free),
        (d("1"), d("1"))
    );
    // 3 x 0.5 = 1.5 shares: 0.5 of one at the mid, 49050 / 0.5 = 98100.
    let paid = balance_of(&outcome.events[1]);
    assert_eq!(paid.asset, quote());
    assert_eq!(
        (paid.balance.total, paid.balance.free),
        (d("1049050"), d("1049050"))
    );
}

#[test]
fn fractional_keeps_the_fraction_of_a_share_and_pays_nothing() {
    let mut venue = venue_with(
        &spot_config("3", "1000000", maker_taker_fee()),
        spot_instruments(),
    );

    let outcome = apply(&mut venue, "0.5", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(outcome.events.len(), 1, "{:#?}", outcome.events);
    assert_eq!(balance_of(&outcome.events[0]).balance.total, d("1.5"));
    assert_eq!(venue.account.free(&quote()), d("1000000"));
}

#[test]
fn floor_with_cash_in_lieu_to_pay_and_no_market_to_price_it_is_refused_until_one_is_fed() {
    let mut venue = make_market_venue("3", "1000000");
    advance(&mut venue, time(1));

    let refused = apply(&mut venue, "0.5", SplitRoundingPolicy::Floor, 5);

    assert_eq!(
        refused.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::NoPriceForCashInLieu)
    );
    assert!(refused.events.is_empty());
    assert_eq!(venue.account.free(&base()), d("3"));

    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("49000", "49100")),
        time(6),
    );
    let applied = apply(&mut venue, "0.5", SplitRoundingPolicy::Floor, 7);
    assert_eq!(applied.response, CorporateActionResponse::Applied);
    assert_eq!(venue.account.free(&base()), d("1"));
}

#[test]
fn floor_needs_no_market_when_the_split_leaves_whole_shares() {
    let mut venue = make_market_venue("3", "1000000");
    advance(&mut venue, time(1));

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Floor, 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    assert_eq!(venue.account.free(&base()), d("6"));
}

#[test]
fn the_market_is_restated_prices_divided_and_sizes_multiplied() {
    let mut venue = make_market_venue("10", "1000000");
    advance(&mut venue, time(1));
    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(MarketSnapshot::new(
            Some(d("49000")),
            Some(d("49100")),
            Some(d("49050")),
        ))
        .with_depth(MarketDepth::new(Some(d("3")), Some(d("4")))),
        time(1),
    );

    let _ = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    let market = venue.market(&instrument_name()).unwrap();
    assert_eq!(
        market.snapshot,
        MarketSnapshot::new(Some(d("24500")), Some(d("24550")), Some(d("24525")))
    );
    assert_eq!(market.depth, MarketDepth::new(Some(d("6")), Some(d("8"))));
}

/// A margin account owing `owed` BTC as principal and `interest` as interest, holding `held`.
fn owing_btc(held: &str, owed: &str, interest: &str) -> MockExecutionConfig {
    config_from_balances(
        vec![
            AssetBalance::new(
                base(),
                Balance::new_margin(d(held), d(held), d(owed), d(interest)),
                Utc::now(),
            ),
            funded(quote(), d("100000")),
        ],
        FeeModelConfig::default(),
    )
    .with_spot_margin(SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow).unwrap())
}

#[test]
fn a_loan_of_the_equity_is_restated_and_reported() {
    let mut venue =
        SimulatedVenue::new_market_driven(&owing_btc("1", "1", "0.01"), spot_instruments());
    advance(&mut venue, time(1));

    let outcome = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(outcome.response, CorporateActionResponse::Applied);
    let holding = balance_of(&outcome.events[0]);
    assert_eq!(
        holding.balance,
        Balance::new_margin(d("2"), d("2"), d("2"), d("0.02"))
    );
    assert!(
        outcome
            .events
            .iter()
            .any(|event| matches!(event.kind, AccountEventKind::MarginDebt(_))),
        "the changed debt is reported: {:#?}",
        outcome.events
    );
}

#[test]
fn floor_refuses_a_split_that_leaves_a_loan_of_the_equity_fractional() {
    let mut venue =
        SimulatedVenue::new_market_driven(&owing_btc("0", "1", "0"), spot_instruments());
    advance(&mut venue, time(1));

    let refused = apply(&mut venue, "0.5", SplitRoundingPolicy::Floor, 5);

    assert_eq!(
        refused.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::FractionalShortUnderFloor)
    );
    assert!(refused.events.is_empty());
    // A whole loan after the split is restated.
    let applied = apply(&mut venue, "2", SplitRoundingPolicy::Floor, 6);
    assert_eq!(applied.response, CorporateActionResponse::Applied);
    assert_eq!(venue.account.owed(&base()), d("2"));
}

#[test]
fn floor_refuses_a_split_that_leaves_a_holding_below_zero_fractional() {
    let mut venue = make_market_venue("-1", "1000000");
    advance(&mut venue, time(1));

    let refused = apply(&mut venue, "1.5", SplitRoundingPolicy::Floor, 5);

    assert_eq!(
        refused.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::FractionalShortUnderFloor)
    );
    let applied = apply(&mut venue, "2", SplitRoundingPolicy::Floor, 6);
    assert_eq!(applied.response, CorporateActionResponse::Applied);
    assert_eq!(venue.account.free(&base()), d("-2"));
}

#[test]
fn a_split_is_refused_while_an_order_on_another_instrument_holds_the_equity() {
    let btceur = InstrumentNameExchange::new("BTCEUR");
    let eur = AssetNameExchange::new("EUR");
    let mut instruments = spot_instruments();
    let mut other = instruments[&instrument_name()].clone();
    other.name_exchange = btceur.clone();
    other.name_internal = InstrumentNameInternal::new("btceur");
    other.underlying.quote = eur.clone();
    instruments.insert(btceur.clone(), other);
    let config = config_from_balances(
        vec![
            funded(base(), d("10")),
            funded(quote(), d("1000000")),
            funded(eur, d("1000000")),
        ],
        maker_taker_fee(),
    );
    let mut venue = venue_with(&config, instruments);
    let mut sell = limit_request("eur-ask", Side::Sell, "1", "60000", gtc());
    sell.key.instrument = btceur;
    assert!(matches!(
        venue.open_order(sell).response.state,
        OrderState::Active(ActiveOrderState::Open(_))
    ));

    let refused = apply(&mut venue, "2", SplitRoundingPolicy::Fractional, 5);

    assert_eq!(
        refused.response,
        CorporateActionResponse::Refused(CorporateActionRefusal::HeldByAnotherInstrument)
    );
    assert!(refused.events.is_empty());
    assert_eq!(venue.account.free(&base()), d("9"));
}
