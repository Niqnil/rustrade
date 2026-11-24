//! Amending a resting order in place: what moves on the ledger and the book, what the answer says,
//! and what is refused with nothing changed.

use super::*;
use crate::{
    client::mock::AmendConfig,
    error::AmendField,
    order::{
        amend::{AmendAllowed, AmendPatch, AmendTarget, Amended, RequestAmend},
        request::{OrderRequestAmend, ReduceOnly},
    },
};

fn cid(cid: &str) -> ClientOrderId {
    ClientOrderId::new(cid)
}

/// An amend of the order resting under `name` on `instrument`, addressed and described as its
/// sender tracks it: by the id the venue gave it, from the order as the venue holds it.
fn amend_of(
    venue: &SimulatedVenue,
    instrument: InstrumentNameExchange,
    name: &str,
    patch: AmendPatch,
) -> OrderRequestAmend<ExchangeId, InstrumentNameExchange> {
    let (id, current, filled) = match venue.account.orders().get(&cid(name)) {
        Some(order) => (
            order.state.id.clone(),
            AmendTarget {
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                reduce_only: Some(ReduceOnly::No),
            },
            order.state.filled_quantity,
        ),
        // An order the venue does not hold: what its sender last tracked.
        None => (
            VenueOrderId::Assigned(OrderId::new(name)),
            AmendTarget {
                side: Side::Buy,
                price: Some(d("48000")),
                quantity: d("1"),
                kind: OrderKind::Limit,
                time_in_force: gtc(),
                reduce_only: Some(ReduceOnly::No),
            },
            Decimal::ZERO,
        ),
    };
    OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument,
            strategy: StrategyId::new("test"),
            cid: cid(name),
        },
        state: RequestAmend::new(id, patch, current, filled),
    }
}

/// Amends the spot order resting under `name`.
pub(super) fn amend(venue: &mut SimulatedVenue, name: &str, patch: AmendPatch) -> AmendOutcome {
    let request = amend_of(venue, instrument_name(), name, patch);
    venue.amend_order(request)
}

pub(super) fn price(price: &str) -> AmendPatch {
    AmendPatch::default().with_price(d(price))
}

fn quantity(quantity: &str) -> AmendPatch {
    AmendPatch::default().with_quantity(d(quantity))
}

/// The order an amend answered with, or a panic naming what it answered instead.
pub(super) fn amended(outcome: &AmendOutcome) -> &Amended {
    match &outcome.response.state {
        Ok(amended) => amended,
        Err(error) => panic!("expected the amend to be made, got: {error:?}"),
    }
}

/// Why an amend was refused, having moved nothing.
fn refusal(outcome: &AmendOutcome) -> &UnindexedOrderError {
    assert!(
        outcome.events.is_empty(),
        "a refused amend reports nothing: {:?}",
        outcome.events
    );
    match &outcome.response.state {
        Err(error) => error,
        Ok(amended) => panic!("expected the amend to be refused, got: {amended:?}"),
    }
}

fn amend_refusal(outcome: &AmendOutcome) -> &AmendRefusal {
    match refusal(outcome) {
        OrderError::AmendRefused(refusal) => refusal,
        other => panic!("expected AmendRefused, got: {other:?}"),
    }
}

/// Asserts the amend was refused by the venue, for a reason it could not declare in advance.
fn assert_venue_refusal(outcome: &AmendOutcome) {
    assert!(
        matches!(amend_refusal(outcome), AmendRefusal::Venue(_)),
        "{:?}",
        outcome.response.state
    );
}

/// The orders queued on `side` of the spot instrument, best first.
fn queue(venue: &SimulatedVenue, side: Side) -> Vec<String> {
    venue
        .account
        .orders()
        .resting(&instrument_name(), side)
        .map(|order| order.key.cid.0.to_string())
        .collect()
}

fn resting(venue: &SimulatedVenue, name: &str) -> OpenOrder {
    venue
        .account
        .orders()
        .get(&cid(name))
        .unwrap_or_else(|| panic!("{name} is resting"))
        .clone()
}

fn free_quote(venue: &SimulatedVenue) -> Decimal {
    venue.balances(&[quote()])[0].balance.free
}

fn cash_flows(events: &[UnindexedAccountEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event.kind, AccountEventKind::CashFlow(_)))
        .count()
}

fn debt_updates(events: &[UnindexedAccountEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event.kind, AccountEventKind::MarginDebt(_)))
        .count()
}

/// [`venue_resting_one_buy`] at 48,000, with the clock moved to `time(2)`: it holds 48,009.6, the
/// order and its 2bp maker fee.
fn resting_buy() -> SimulatedVenue {
    let (mut venue, _) = venue_resting_one_buy("48000");
    advance(&mut venue, time(2));
    venue
}

/// [`resting_buy`], configured to amend as `amend` says.
fn resting_buy_amending(amend: AmendConfig) -> SimulatedVenue {
    let mut config =
        spot_config_with_fill("10", "1000000", maker_taker_fee(), SimFillConfig::default());
    config.amend = amend;
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
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
    let outcome = venue.open_order(limit_request("resting", Side::Buy, "1", "48000", gtc()));
    assert_eq!(outcome.events.len(), 1, "{:?}", outcome.response.state);
    advance(&mut venue, time(2));
    venue
}

/// Feeds the spot book 49,000 / 49,100 at `time(2)` with only `ask_size` offered.
fn offer_only(venue: &mut SimulatedVenue, ask_size: &str) {
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("49000", "49100"))
                    .with_depth(MarketDepth::new(None, Some(d(ask_size)))),
                time(2),
            )
            .is_empty()
    );
}

// --- What an amend moves -------------------------------------------------------------------

#[test]
fn a_price_amend_moves_the_hold_and_answers_with_the_amended_order() {
    let mut venue = resting_buy();
    let before = resting(&venue, "resting");

    let outcome = amend(&mut venue, "resting", price("47000"));

    let [restated, reported] = outcome.events.as_slice() else {
        panic!(
            "an amend that trades nothing restates its hold, then reports the order: {outcome:?}"
        );
    };
    assert_eq!(
        balance_of(restated).balance,
        Balance::new(d("1000000"), d("1000000") - d("47009.4")),
        "47,000 and its maker fee held in place of 48,009.6"
    );
    let answer = amended(&outcome);
    assert_eq!(answer.price, Some(d("47000")));
    assert_eq!(answer.quantity, d("1"));
    assert_eq!(answer.kind, OrderKind::Limit);
    assert_eq!(answer.previous_id, None);
    assert_eq!(
        answer.open,
        Open::new(before.state.id.clone(), time(2), Decimal::ZERO),
        "the same id, stamped when the amend sent it to the back"
    );
    let after = resting(&venue, "resting");
    assert_eq!(after.price, Some(d("47000")));
    assert_eq!(after.state, answer.open);
    let report = report_of(reported);
    assert_eq!(
        report.order,
        after.clone().map_state(OrderState::active),
        "the order as it now rests"
    );
    assert_eq!(
        report.reported,
        ReportedTerms::ALL,
        "from the venue's own book"
    );
}

/// An order changed from outside any client is amended as `amend_order` amends it, and only the
/// account stream hears of it: the restated hold, then the order as it now rests.
#[test]
fn an_external_modification_amends_the_order_and_reports_it_on_the_stream() {
    let mut venue = resting_buy();
    let before = resting(&venue, "resting");

    let events = venue
        .modify_externally(&cid("resting"), price("47000"))
        .unwrap();

    let [restated, reported] = events.as_slice() else {
        panic!("the restated hold, then the order: {events:?}");
    };
    assert_eq!(
        balance_of(restated).balance,
        Balance::new(d("1000000"), d("1000000") - d("47009.4"))
    );
    let after = resting(&venue, "resting");
    assert_eq!(after.price, Some(d("47000")));
    assert_eq!(
        after.state,
        Open::new(before.state.id, time(2), Decimal::ZERO)
    );
    let report = report_of(reported);
    assert_eq!(report.order, after.map_state(OrderState::active));
    assert_eq!(report.reported, ReportedTerms::ALL);
}

/// An external modification the venue refuses changes nothing, and says why as an amend's answer
/// would.
#[test]
fn a_refused_external_modification_changes_nothing() {
    let mut venue = resting_buy();
    let before = resting(&venue, "resting");

    assert!(matches!(
        venue.modify_externally(&cid("resting"), quantity("0")),
        Err(OrderError::AmendRefused(_))
    ));
    assert_eq!(resting(&venue, "resting"), before);

    assert!(matches!(
        venue.modify_externally(&cid("absent"), price("47000")),
        Err(OrderError::Rejected(_))
    ));
}

/// The venue's own interface is not its client's API: a change from outside is made on a venue
/// whose client may amend nothing, and in more than one field at once.
#[test]
fn an_external_modification_is_made_where_the_client_may_amend_nothing() {
    let mut venue = resting_buy_amending(AmendConfig::NONE);
    let both = AmendPatch::default()
        .with_price(d("47000"))
        .with_quantity(d("2"));
    assert!(matches!(
        amend_refusal(&amend(&mut venue, "resting", both)),
        AmendRefusal::Unsupported { .. }
    ));

    let events = venue.modify_externally(&cid("resting"), both).unwrap();

    let after = resting(&venue, "resting");
    assert_eq!((after.price, after.quantity), (Some(d("47000")), d("2")));
    let reported = events.last().expect("the order is reported");
    assert_eq!(order_of(reported), &after.map_state(OrderState::active));
    assert_eq!(
        free_quote(&venue),
        d("1000000") - d("94018.8"),
        "2 at 47,000 and its 2bp maker fee held"
    );
}

/// What the book refuses a client's amend it refuses a change from outside too, changing and
/// reporting nothing.
#[test]
fn an_external_modification_the_book_refuses_changes_nothing() {
    // A post-only order the new price would cross.
    let (mut venue, _) = venue_resting_one_buy("47000");
    let _ = venue.open_order(limit_request("maker", Side::Buy, "1", "48000", post_only()));
    let (before, free) = (resting(&venue, "maker"), free_quote(&venue));
    assert!(matches!(
        venue.modify_externally(&cid("maker"), price("49200")),
        Err(OrderError::AmendRefused(AmendRefusal::WouldTakeLiquidity))
    ));
    assert_eq!(
        (resting(&venue, "maker"), free_quote(&venue)),
        (before, free)
    );

    // More than the account can afford.
    let mut venue = resting_buy();
    let (before, free) = (resting(&venue, "resting"), free_quote(&venue));
    assert!(matches!(
        venue.modify_externally(&cid("resting"), quantity("30")),
        Err(OrderError::Rejected(ApiError::BalanceInsufficient(..)))
    ));
    assert_eq!(
        (resting(&venue, "resting"), free_quote(&venue)),
        (before, free)
    );

    // A price change on a market the venue no longer trusts.
    let mut venue = venue_waiting();
    let _ = venue.open_order(limit_request("resting", Side::Buy, "1", "90", gtc()));
    advance(&mut venue, time(12));
    let (before, free) = (resting(&venue, "resting"), free_quote(&venue));
    assert!(matches!(
        venue.modify_externally(&cid("resting"), price("95")),
        Err(OrderError::AmendRefused(AmendRefusal::Venue(_)))
    ));
    assert_eq!(
        (resting(&venue, "resting"), free_quote(&venue)),
        (before, free)
    );
}

/// A change from outside that the market crosses trades as a client's amend does, and reports the
/// order filled.
#[test]
fn an_external_modification_through_the_market_fills_and_reports_the_order_filled() {
    let mut venue = resting_buy();

    let events = venue
        .modify_externally(&cid("resting"), price("49200"))
        .unwrap();

    let [_paid, _received, traded, ended] = events.as_slice() else {
        panic!("the balances, the trade, then the order: {events:?}");
    };
    assert_eq!(trade_of(traded).quantity, d("1"));
    assert_eq!(filled_price(order_of(ended)), d("49100"));
    assert!(venue.account.is_filled(&cid("resting")));
}

/// A change from outside keeps or loses the order's place in the queue by the book's rule, as a
/// client's amend does.
#[test]
fn an_external_modification_keeps_or_loses_the_orders_place_as_an_amend_does() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));

    venue
        .modify_externally(&cid("resting"), quantity("0.5"))
        .unwrap();
    assert_eq!(queue(&venue, Side::Buy), ["resting", "behind"]);

    venue
        .modify_externally(&cid("resting"), quantity("2"))
        .unwrap();
    assert_eq!(queue(&venue, Side::Buy), ["behind", "resting"]);
}

/// On a venue that reassigns ids, the order changed from outside is reported under its new id,
/// which the book then holds it by.
#[test]
fn an_external_modification_on_a_venue_that_reassigns_ids_reports_the_new_id() {
    let mut venue = resting_buy_amending(AmendConfig::FULL.with_reassign_id(true));
    let old = resting(&venue, "resting").state.id;

    let events = venue
        .modify_externally(&cid("resting"), price("47000"))
        .unwrap();

    let held = resting(&venue, "resting");
    assert_ne!(held.state.id, old);
    let reported = events.last().expect("the order is reported");
    assert_eq!(order_of(reported), &held.map_state(OrderState::active));
    let old_id = old.assigned().cloned().expect("the venue named the order");
    assert_eq!(report_of(reported).replaces, Some(Replaces::new(old_id)));
}

#[test]
fn a_price_amend_goes_behind_the_orders_already_at_its_new_price() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("waiting", Side::Buy, "1", "47000", gtc()));
    assert_eq!(queue(&venue, Side::Buy), ["resting", "waiting"]);

    let outcome = amend(&mut venue, "resting", price("47000"));

    assert!(outcome.response.state.is_ok());
    assert_eq!(queue(&venue, Side::Buy), ["waiting", "resting"]);
}

#[test]
fn a_quantity_decrease_keeps_the_orders_place_and_is_stamped_at_the_amend() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));
    let before = resting(&venue, "resting");
    assert_eq!(before.state.time_exchange, time(1));

    let outcome = amend(&mut venue, "resting", quantity("0.5"));

    let stamped = Open::new(before.state.id.clone(), time(2), Decimal::ZERO);
    assert_eq!(amended(&outcome).open, stamped);
    assert_eq!(resting(&venue, "resting").state, stamped);
    let [.., reported] = outcome.events.as_slice() else {
        panic!("the order is reported: {outcome:?}");
    };
    assert_eq!(order_of(reported).state, OrderState::active(stamped));
    assert_eq!(queue(&venue, Side::Buy), ["resting", "behind"]);
    assert_eq!(
        free_quote(&venue),
        d("1000000") - d("24004.8") - d("48009.6"),
        "half the hold given back"
    );
}

#[test]
fn a_quantity_increase_goes_to_the_back() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));

    let outcome = amend(&mut venue, "resting", quantity("2"));

    assert_eq!(amended(&outcome).open.time_exchange, time(2));
    assert_eq!(amended(&outcome).quantity, d("2"));
    assert_eq!(queue(&venue, Side::Buy), ["behind", "resting"]);
    assert_eq!(
        free_quote(&venue),
        d("1000000") - d("96019.2") - d("48009.6"),
        "twice the hold"
    );
}

#[test]
fn a_price_amend_through_the_market_fills_as_a_taker() {
    let mut venue = resting_buy();
    let id = resting(&venue, "resting").state.id;

    let outcome = amend(&mut venue, "resting", price("49200"));

    let [paid, received, traded, ended] = outcome.events.as_slice() else {
        panic!(
            "the balances, the trade, then the order: {:?}",
            outcome.events
        );
    };
    assert_eq!(
        balance_of(paid).balance,
        Balance::new(d("950850.9"), d("950850.9")),
        "1 at the 49,100 ask and its 10bp taker fee"
    );
    assert_eq!(balance_of(received).balance, Balance::new(d("11"), d("11")));
    let trade = trade_of(traded);
    assert_eq!(trade.price, d("49100"));
    assert_eq!(trade.quantity, d("1"));
    assert_eq!(trade.order_id, id.or_client_id(&cid("resting")));
    assert_eq!(filled_price(order_of(ended)), d("49100"));
    assert_eq!(amended(&outcome).open.filled_quantity, d("1"));
    assert!(venue.orders_open(&[]).is_empty());
    assert!(venue.account.is_filled(&cid("resting")));
}

#[test]
fn a_price_amend_through_a_thin_market_rests_what_is_left_under_its_id() {
    let mut venue = resting_buy();
    offer_only(&mut venue, "0.4");
    let id = resting(&venue, "resting").state.id;

    let outcome = amend(&mut venue, "resting", price("49200"));

    let [paid, _received, traded, reported] = outcome.events.as_slice() else {
        panic!(
            "the balances, the trade, then the order: {:?}",
            outcome.events
        );
    };
    assert_eq!(
        balance_of(paid).balance,
        Balance::new(
            d("1000000") - d("19659.64"),
            d("1000000") - d("19659.64") - d("29525.904")
        ),
        "0.4 bought as a taker, 0.6 held at 49,200 as a maker"
    );
    assert_eq!(trade_of(traded).quantity, d("0.4"));
    assert_eq!(trade_of(traded).order_filled_quantity, Some(d("0.4")));
    let answer = amended(&outcome);
    assert_eq!(answer.open, Open::new(id, time(2), d("0.4")));
    assert_eq!(answer.price, Some(d("49200")));
    assert_eq!(resting(&venue, "resting").state, answer.open);
    assert_eq!(
        order_of(reported).state,
        OrderState::active(answer.open.clone()),
        "reported as it rests, carrying the part the amend filled"
    );
}

/// An order that has paid its ticket pays none again on an amend that trades part of it, nor
/// holds one for what the amend leaves resting.
#[test]
fn a_price_amend_of_an_order_filled_in_part_pays_and_holds_no_second_ticket() {
    let ticket = FeeModelConfig::Composite(
        CompositeFeeModel::new(vec![FeeComponent::new(FeeBasis::PerOrder(d("5")))]).unwrap(),
    );
    let config = spot_config_with_fill("10", "1000000", ticket, SimFillConfig::default());
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("47800", "47900"))
                    .with_depth(MarketDepth::new(None, Some(d("0.25")))),
                time(1)
            )
            .is_empty()
    );

    // 0.25 offered through the limit fills that much of the order, which pays its ticket.
    let arrival = venue.open_order(limit_request("resting", Side::Buy, "1", "48000", gtc()));
    let first = trades_in(&arrival.events);
    assert_eq!((first[0].quantity, first[0].fees.fees), (d("0.25"), d("5")));
    advance(&mut venue, time(2));

    // Another 0.25 is offered: the amend through it buys that, and rests the other 0.5.
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("49000", "49100"))
                    .with_depth(MarketDepth::new(None, Some(d("0.25")))),
                time(2),
            )
            .is_empty()
    );
    let outcome = amend(&mut venue, "resting", price("49200"));
    let second = trades_in(&outcome.events);
    assert_eq!(
        (second[0].quantity, second[0].fees.fees),
        (d("0.25"), Decimal::ZERO)
    );
    assert_eq!(amended(&outcome).open.filled_quantity, d("0.5"));
    let quote_now = venue.balances(&[quote()])[0].balance;
    assert_eq!(
        quote_now.total - quote_now.free,
        d("24600"),
        "the 0.5 resting at 49,200, and no ticket"
    );
}

// --- Spot margin: the loan follows the order -------------------------------------------------

#[test]
fn a_margin_amend_borrows_only_what_it_adds_and_repays_what_it_no_longer_needs() {
    let mut venue = margin_market_venue("0", "100000");
    let _ = venue.open_order(limit_request("rest", Side::Sell, "1", "50000", gtc()));

    let raised = amend(&mut venue, "rest", quantity("2"));
    assert_eq!(
        balance_of(&raised.events[0]).balance,
        Balance::new_margin(d("2"), Decimal::ZERO, d("2"), Decimal::ZERO),
        "one more borrowed for the one more held"
    );
    assert_eq!(debt_updates(&raised.events), 1, "{:?}", raised.events);

    let lowered = amend(&mut venue, "rest", quantity("0.5"));
    assert_eq!(
        balance_of(&lowered.events[0]).balance,
        Balance::new_margin(d("0.5"), Decimal::ZERO, d("0.5"), Decimal::ZERO),
        "what is no longer held is repaid, as a cancel repays it"
    );
}

/// A change from outside that borrows reports the debt as a client's amend does.
#[test]
fn an_external_modification_that_borrows_reports_the_debt() {
    let mut venue = margin_market_venue("0", "100000");
    let _ = venue.open_order(limit_request("rest", Side::Sell, "1", "50000", gtc()));

    let events = venue
        .modify_externally(&cid("rest"), quantity("2"))
        .unwrap();

    assert_eq!(
        balance_of(&events[0]).balance,
        Balance::new_margin(d("2"), Decimal::ZERO, d("2"), Decimal::ZERO),
        "one more borrowed for the one more held"
    );
    assert_eq!(debt_updates(&events), 1, "{events:?}");
}

#[test]
fn the_interest_an_untraded_orders_amend_is_charged_waits_for_the_order_to_trade_or_end() {
    let mut venue =
        margin_market_venue("0", "100000").with_margin_interest(Arc::new(hourly_interest(quote())));
    let _ = venue.open_order(limit_request("rest", Side::Buy, "1", "48000", gtc()));

    // 120,000 held against 100,000: 20,000 lent and charged.
    let outcome = amend(&mut venue, "rest", quantity("2.5"));
    assert_eq!(debt_updates(&outcome.events), 1, "{:?}", outcome.events);
    assert_eq!(cash_flows(&outcome.events), 0, "{:?}", outcome.events);

    let cancelled = venue.cancel_order(OrderRequestCancel {
        key: limit_request("rest", Side::Buy, "1", "48000", gtc()).key,
        state: RequestCancel { id: None },
    });
    assert_eq!(cash_flows(&cancelled.events), 1, "{:?}", cancelled.events);
}

#[test]
fn the_interest_a_traded_orders_amend_is_charged_is_reported_with_the_amend() {
    let mut venue =
        margin_market_venue("0", "100000").with_margin_interest(Arc::new(hourly_interest(quote())));
    offer_only(&mut venue, "0.5");
    let opened = venue.open_order(limit_request("part", Side::Buy, "1", "49100", gtc()));
    assert_eq!(rested_open(&opened).filled_quantity, d("0.5"));

    // 2.5 left at 48,000 is 120,000 held against the 75,450 left: the shortfall is lent.
    let outcome = amend(
        &mut venue,
        "part",
        AmendPatch::default()
            .with_price(d("48000"))
            .with_quantity(d("3")),
    );

    assert_eq!(amended(&outcome).open.filled_quantity, d("0.5"));
    assert_eq!(cash_flows(&outcome.events), 1, "{:?}", outcome.events);
}

// --- CFD: the shortability a raised or marketable sell asks for -----------------------------

/// Amends the CFD order resting under `name`.
fn amend_cfd(venue: &mut SimulatedVenue, name: &str, patch: AmendPatch) -> AmendOutcome {
    let request = amend_of(venue, cfd_instrument_name(), name, patch);
    venue.amend_order(request)
}

fn borrow_refusal(outcome: &AmendOutcome) -> BorrowRejectReason {
    match refusal(outcome) {
        OrderError::Rejected(ApiError::BorrowRejected(reject)) => reject.reason,
        other => panic!("expected BorrowRejected, got: {other:?}"),
    }
}

#[test]
fn a_raised_cfd_sell_is_measured_without_its_own_old_quantity() {
    let mut venue = make_cfd_venue_lending(lending(Shortability::new().with_available(d("3"))));
    let rested = venue.open_order(cfd_limit("short", Side::Sell, "2", "5100"));
    assert!(rested.response.state.is_accepted());

    // A short of 3, not 2 + 3.
    assert!(
        amend_cfd(&mut venue, "short", quantity("3"))
            .response
            .state
            .is_ok()
    );

    let past = amend_cfd(&mut venue, "short", quantity("4"));
    assert_eq!(
        borrow_refusal(&past),
        BorrowRejectReason::InventoryUnavailable
    );
    assert_eq!(resting_cfd_quantity(&venue, "short"), d("3"));
}

#[test]
fn a_partly_filled_cfd_sells_raise_is_measured_by_what_it_has_left_to_sell() {
    let mut venue = make_cfd_venue_lending(lending(Shortability::new().with_available(d("3"))));
    // One bid at 4,990: a sell of 2 at 4,980 trades 1 and rests the other.
    assert!(
        venue
            .apply_market(
                &cfd_instrument_name(),
                MarketObservation::new(book("4990", "5000"))
                    .with_depth(MarketDepth::new(Some(d("1")), None)),
                time(1),
            )
            .is_empty()
    );
    let rested = venue.open_order(cfd_limit("short", Side::Sell, "2", "4980"));
    assert_eq!(rested_open(&rested).filled_quantity, d("1"));
    advance(&mut venue, time(2));
    assert!(cfd_market(&mut venue, "4970", "5000", "5000").is_empty());

    // Short 1 already, and a total of 3 leaves 2 to sell: a short of 3, within what there is.
    assert!(
        amend_cfd(&mut venue, "short", quantity("3"))
            .response
            .state
            .is_ok()
    );

    let past = amend_cfd(&mut venue, "short", quantity("4"));
    assert_eq!(
        borrow_refusal(&past),
        BorrowRejectReason::InventoryUnavailable
    );
    assert_eq!(resting_cfd_quantity(&venue, "short"), d("3"));
}

fn resting_cfd_quantity(venue: &SimulatedVenue, name: &str) -> Decimal {
    venue.account.orders().get(&cid(name)).unwrap().quantity
}

#[test]
fn only_a_raised_or_marketable_cfd_sell_asks_whether_it_may_short() {
    let table = ShortabilityTable::new()
        .with_shortability(
            cfd_instrument_name(),
            time(0),
            Shortability::new().with_available(d("2")),
        )
        .with_shortability(
            cfd_instrument_name(),
            time(5),
            Shortability::new().with_shortable(false),
        );
    let mut venue = make_cfd_venue_lending(table);
    let rested = venue.open_order(cfd_limit("short", Side::Sell, "2", "5100"));
    assert!(rested.response.state.is_accepted());
    advance(&mut venue, time(5));

    assert!(
        amend_cfd(&mut venue, "short", quantity("1"))
            .response
            .state
            .is_ok()
    );
    assert!(
        amend_cfd(&mut venue, "short", price("5200"))
            .response
            .state
            .is_ok()
    );

    let raised = amend_cfd(&mut venue, "short", quantity("3"));
    assert_eq!(borrow_refusal(&raised), BorrowRejectReason::NotShortable);
    // Through the 4,990 bid.
    let marketable = amend_cfd(&mut venue, "short", price("4980"));
    assert_eq!(
        borrow_refusal(&marketable),
        BorrowRejectReason::NotShortable
    );
    assert_eq!(resting_cfd_quantity(&venue, "short"), d("1"));
}

// --- Refused, changing nothing ---------------------------------------------------------------

#[test]
fn a_post_only_amend_through_the_market_is_refused() {
    let (mut venue, _) = venue_resting_one_buy("47000");
    let _ = venue.open_order(limit_request("maker", Side::Buy, "1", "48000", post_only()));
    let free = free_quote(&venue);

    let outcome = amend(&mut venue, "maker", price("49200"));

    assert_eq!(*amend_refusal(&outcome), AmendRefusal::WouldTakeLiquidity);
    assert_eq!(resting(&venue, "maker").price, Some(d("48000")));
    assert_eq!(free_quote(&venue), free);
}

#[test]
fn an_amend_the_account_cannot_afford_is_refused_and_the_order_keeps_its_place() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));
    let before = resting(&venue, "resting");
    let free = free_quote(&venue);

    let outcome = amend(&mut venue, "resting", quantity("30"));

    assert!(
        matches!(
            refusal(&outcome),
            OrderError::Rejected(ApiError::BalanceInsufficient(Some(asset), _)) if *asset == quote()
        ),
        "{:?}",
        outcome.response.state
    );
    assert!(outcome.events.is_empty(), "{:?}", outcome.events);
    assert_eq!(resting(&venue, "resting"), before);
    assert_eq!(queue(&venue, Side::Buy), ["resting", "behind"]);
    assert_eq!(free_quote(&venue), free);
    assert_eq!(venue.balances(&[quote()])[0].balance.total, d("1000000"));
}

#[test]
fn a_crossing_amend_the_account_cannot_afford_trades_nothing_and_draws_no_slippage() {
    let mut config =
        spot_config_with_fill("10", "1000000", maker_taker_fee(), SimFillConfig::default());
    config.slippage = SlippageConfig::uniform(d("0.01"), d("0.025"), 3).unwrap();
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
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
    let rested = venue.open_order(limit_request("resting", Side::Buy, "1", "48000", gtc()));
    assert_eq!(rested.events.len(), 1, "{:?}", rested.response.state);
    advance(&mut venue, time(2));
    let before = resting(&venue, "resting");
    let free = free_quote(&venue);
    let slippage = venue.slippage.clone();

    // 30 through the 49,100 ask is about 1.5 million, past the million the account holds.
    let outcome = amend(&mut venue, "resting", price("49200").with_quantity(d("30")));

    assert!(
        matches!(
            refusal(&outcome),
            OrderError::Rejected(ApiError::BalanceInsufficient(Some(asset), _)) if *asset == quote()
        ),
        "{:?}",
        outcome.response.state
    );
    assert!(outcome.events.is_empty(), "{:?}", outcome.events);
    assert_eq!(resting(&venue, "resting"), before);
    assert_eq!(free_quote(&venue), free);
    assert_eq!(venue.slippage, slippage, "a refused fill takes no draw");

    // The same crossing amend, affordable, does draw.
    assert!(
        amend(&mut venue, "resting", price("49200"))
            .response
            .state
            .is_ok()
    );
    assert_ne!(venue.slippage, slippage);
}

#[test]
fn an_amend_of_an_order_the_venue_no_longer_holds_says_why_as_a_cancel_does() {
    let mut venue = resting_buy();
    let ghost = amend(&mut venue, "ghost", price("47000"));
    assert!(
        matches!(
            refusal(&ghost),
            OrderError::Rejected(ApiError::OrderRejected(_))
        ),
        "{:?}",
        ghost.response.state
    );

    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("47800", "47900")),
        time(2),
    );
    let filled = amend(&mut venue, "resting", price("47000"));
    assert!(matches!(
        refusal(&filled),
        OrderError::Rejected(ApiError::OrderAlreadyFullyFilled)
    ));

    let _ = venue.open_order(limit_request("cancelled", Side::Buy, "1", "46000", gtc()));
    let _ = venue.cancel_order(OrderRequestCancel {
        key: limit_request("cancelled", Side::Buy, "1", "46000", gtc()).key,
        state: RequestCancel { id: None },
    });
    let cancelled = amend(&mut venue, "cancelled", price("45000"));
    assert!(matches!(
        refusal(&cancelled),
        OrderError::Rejected(ApiError::OrderAlreadyCancelled)
    ));
}

#[test]
fn an_amend_after_the_orders_deadline_finds_it_expired() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request(
        "dated",
        Side::Buy,
        "1",
        "46000",
        TimeInForce::GoodTillDate { expiry: time(5) },
    ));
    assert!(
        !venue.advance_time(time(5)).is_empty(),
        "the deadline retires it"
    );

    let outcome = amend(&mut venue, "dated", price("45000"));

    assert!(matches!(
        refusal(&outcome),
        OrderError::Rejected(ApiError::OrderAlreadyExpired)
    ));
}

#[test]
fn an_amend_addressed_by_another_venue_id_is_rejected() {
    let mut venue = resting_buy();
    let before = resting(&venue, "resting");
    let mut request = amend_of(&venue, instrument_name(), "resting", price("47000"));
    request.state.id = VenueOrderId::Assigned(OrderId::new("someone-else"));

    let outcome = venue.amend_order(request);

    assert!(matches!(
        refusal(&outcome),
        OrderError::Rejected(ApiError::OrderRejected(_))
    ));
    assert_eq!(resting(&venue, "resting"), before);
}

#[test]
fn an_amend_is_checked_against_the_order_the_venue_holds() {
    let mut venue = resting_buy();

    // Tracked by its sender at 47,000, held at 48,000: moving it to 48,000 changes nothing.
    let mut stale = amend_of(&venue, instrument_name(), "resting", price("48000"));
    stale.state.current.price = Some(d("47000"));
    let outcome = venue.amend_order(stale);
    assert_eq!(*amend_refusal(&outcome), AmendRefusal::NoChange);

    let none_left = amend(&mut venue, "resting", quantity("0"));
    assert_eq!(
        *amend_refusal(&none_left),
        AmendRefusal::QuantityNotAboveFilled {
            filled: Decimal::ZERO
        }
    );

    let trigger = amend(
        &mut venue,
        "resting",
        AmendPatch::default().with_trigger_price(d("47000")),
    );
    assert_eq!(
        *amend_refusal(&trigger),
        AmendRefusal::NotOnKind {
            field: AmendField::TriggerPrice
        }
    );
}

#[test]
fn an_amend_its_config_does_not_allow_is_refused() {
    let mut none = resting_buy_amending(AmendConfig::NONE);
    assert!(none.amend_support().is_none());
    assert_eq!(
        *amend_refusal(&amend(&mut none, "resting", price("47000"))),
        AmendRefusal::Unsupported {
            field: AmendField::Price
        }
    );

    let mut lower_only =
        resting_buy_amending(AmendConfig::FULL.with_price(AmendAllowed::DecreaseOnly));
    assert_eq!(
        *amend_refusal(&amend(&mut lower_only, "resting", price("48500"))),
        AmendRefusal::Increase {
            field: AmendField::Price
        }
    );
    assert!(
        amend(&mut lower_only, "resting", price("47000"))
            .response
            .state
            .is_ok()
    );

    let mut one_field = resting_buy_amending(AmendConfig::FULL.with_multi_field(false));
    let both = AmendPatch::default()
        .with_price(d("47000"))
        .with_quantity(d("2"));
    assert_eq!(
        *amend_refusal(&amend(&mut one_field, "resting", both)),
        AmendRefusal::MultipleFields
    );
}

#[test]
fn a_venue_that_reassigns_ids_names_the_old_one_and_reports_its_own_fill_under_it() {
    let mut venue = resting_buy_amending(AmendConfig::FULL.with_reassign_id(true));
    offer_only(&mut venue, "0.4");
    let old = resting(&venue, "resting").state.id;
    let sequence = venue.order_sequence();

    // Refused: no id is used up.
    assert!(
        amend(&mut venue, "resting", quantity("30"))
            .response
            .state
            .is_err()
    );
    assert_eq!(venue.order_sequence(), sequence);

    let outcome = amend(&mut venue, "resting", price("49200"));

    let traded = outcome
        .events
        .iter()
        .find(|event| matches!(event.kind, AccountEventKind::Trade(_)))
        .map(trade_of)
        .expect("the amend traded");
    assert_eq!(traded.order_id, old.or_client_id(&cid("resting")));
    let answer = amended(&outcome);
    assert_eq!(answer.previous_id, Some(old.clone()));
    assert_ne!(answer.open.id, old);
    assert_eq!(resting(&venue, "resting").state.id, answer.open.id);
    assert_eq!(venue.order_sequence(), sequence + 1);
    assert!(!venue.amend_support().keeps_id);
    // The stream's report of the order, the last event the answer follows, names the new id and
    // links the old one. The order goes on under its new id, so nothing reports the old one ended.
    let reported = outcome.events.last().expect("the order is reported");
    assert_eq!(
        order_of(reported).state,
        OrderState::active(answer.open.clone())
    );
    let old_id = old.assigned().cloned().expect("the venue named the order");
    assert_eq!(report_of(reported).replaces, Some(Replaces::new(old_id)));
}

/// Restarted under its new id, the order under the old one is reported cancelled with what filled
/// under it, after the report of the amended order, which counts that fill in its totals.
#[test]
fn a_venue_that_restarts_a_reassigned_order_reports_the_old_one_cancelled_after_it() {
    let mut venue = resting_buy_amending(
        AmendConfig::FULL
            .with_reassign_id(true)
            .with_filled_restarts(true),
    );
    offer_only(&mut venue, "0.4");
    let old = resting(&venue, "resting");
    let old_id = old
        .state
        .id
        .assigned()
        .cloned()
        .expect("the venue named it");

    let outcome = amend(&mut venue, "resting", price("49200"));

    let answer = amended(&outcome);
    assert_eq!(
        answer.previous_id,
        Some(VenueOrderId::Assigned(old_id.clone()))
    );
    assert_eq!(answer.open.filled_quantity, d("0.4"));
    let [.., replacement, ended] = outcome.events.as_slice() else {
        panic!("the amended order, then the old one: {outcome:?}");
    };
    let replacement = report_of(replacement);
    assert_eq!(
        replacement.order.state,
        OrderState::active(answer.open.clone())
    );
    assert_eq!(replacement.replaces, Some(Replaces::new(old_id.clone())));
    let ended = report_of(ended);
    assert_eq!(ended.order.key, replacement.order.key);
    assert_eq!(ended.replaces, None);
    assert_eq!(ended.order.price, old.price);
    assert_eq!(
        ended.order.state,
        OrderState::inactive(Cancelled {
            id: old_id,
            time_exchange: answer.open.time_exchange,
            filled_quantity: Some(d("0.4")),
        })
    );
    // The book holds the order under its new id, in totals.
    assert_eq!(resting(&venue, "resting").state, answer.open);
}

/// Restarting is a reassigned id's: an amend keeping the order's id reports no order ended.
#[test]
fn restarting_does_nothing_on_a_venue_that_keeps_ids() {
    let mut venue = resting_buy_amending(AmendConfig::FULL.with_filled_restarts(true));

    let outcome = amend(&mut venue, "resting", price("47000"));

    assert!(outcome.response.state.is_ok());
    let reported = outcome.events.last().expect("the order is reported");
    assert!(
        matches!(order_of(reported).state, OrderState::Active(_)),
        "{reported:?}"
    );
    assert_eq!(report_of(reported).replaces, None);
}

#[test]
fn a_venue_that_reassigns_ids_reassigns_none_to_an_order_its_amend_fills() {
    let mut venue = resting_buy_amending(AmendConfig::FULL.with_reassign_id(true));
    let old = resting(&venue, "resting").state.id;
    let sequence = venue.order_sequence();

    let outcome = amend(&mut venue, "resting", price("49200"));

    let answer = amended(&outcome);
    assert_eq!(answer.open.filled_quantity, d("1"));
    assert_eq!(answer.open.id, old);
    assert_eq!(answer.previous_id, None);
    assert_eq!(venue.order_sequence(), sequence);
}

#[test]
fn an_order_seeded_through_the_initial_state_is_not_amended() {
    let config = spot_config_holding(
        "1",
        "1000",
        seeded_gtd("seeded", "100", time(0), time(1000)),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());

    let outcome = amend(&mut venue, "seeded", price("99"));

    assert_venue_refusal(&outcome);
    assert_eq!(resting(&venue, "seeded").price, Some(d("100")));
}

/// A seeded order holds nothing, so changed from outside it takes what it now needs, as an
/// arriving order does, and pays for its fill once, from that hold.
#[test]
fn an_order_seeded_through_the_initial_state_can_be_changed_from_outside() {
    let config = spot_config_holding(
        "1",
        "1000",
        seeded_gtd("seeded", "100", time(0), time(1000)),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    assert_eq!(
        free_quote(&venue),
        d("1000"),
        "a seeded order holds nothing"
    );

    let events = venue
        .modify_externally(&cid("seeded"), price("99"))
        .unwrap();

    let [restated, reported] = events.as_slice() else {
        panic!("the hold taken, then the order: {events:?}");
    };
    assert_eq!(
        balance_of(restated).balance,
        Balance::new(d("1000"), d("1000") - d("0.99"))
    );
    let after = resting(&venue, "seeded");
    assert_eq!(after.price, Some(d("99")));
    assert_eq!(order_of(reported), &after.map_state(OrderState::active));

    // Filled at its limit, it pays the 0.99 it holds, and nothing more.
    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("97", "98")),
        time(1),
    );
    assert!(venue.account.is_filled(&cid("seeded")));
    let balances = venue.balances(&[base(), quote()]);
    let [btc, usdt] = balances.as_slice() else {
        panic!("both balances");
    };
    assert_eq!(btc.balance.total, d("1.01"));
    assert_eq!(
        usdt.balance,
        Balance::new(d("1000") - d("0.99"), d("1000") - d("0.99"))
    );
}

/// The venue seeded through `initial_state` with [`seeded_gtd`] at 100 for 0.01, of which
/// `filled` has already filled, holding 1 BTC and 1,000 USDT, and configured to amend as `amend`
/// says.
fn seeded_venue(filled: &str, amend: AmendConfig) -> SimulatedVenue {
    let mut order = seeded_gtd("seeded", "100", time(0), time(1000));
    order.state = OrderState::active(Open::new(
        VenueOrderId::Assigned(OrderId::new("seeded")),
        time(0),
        d(filled),
    ));
    let config = spot_config_holding("1", "1000", order).with_amend(amend);
    SimulatedVenue::new_market_driven(&config, spot_instruments())
}

/// A client's amend is checked as the client asked before the book is consulted: a patch that
/// does not apply, or that the client may not ask, is refused as such, even for a seeded order.
#[test]
fn a_seeded_orders_amend_is_refused_first_for_what_the_client_asked() {
    let mut venue = seeded_venue("0", AmendConfig::NONE);
    assert_eq!(
        *amend_refusal(&amend(&mut venue, "seeded", quantity("0"))),
        AmendRefusal::QuantityNotAboveFilled {
            filled: Decimal::ZERO
        }
    );
    assert_eq!(
        *amend_refusal(&amend(&mut venue, "seeded", price("99"))),
        AmendRefusal::Unsupported {
            field: AmendField::Price
        }
    );
}

/// A seeded order changed from outside to more than the account can afford is refused, takes no
/// hold, and reports nothing.
#[test]
fn an_external_modification_of_a_seeded_order_the_account_cannot_afford_changes_nothing() {
    let mut venue = seeded_venue("0", AmendConfig::FULL);
    let before = resting(&venue, "seeded");

    assert!(matches!(
        venue.modify_externally(&cid("seeded"), quantity("20")),
        Err(OrderError::Rejected(ApiError::BalanceInsufficient(..)))
    ));
    assert_eq!(resting(&venue, "seeded"), before);
    assert_eq!(free_quote(&venue), d("1000"));
}

/// The hold a seeded order takes when changed from outside is released when it is cancelled.
#[test]
fn a_seeded_order_changed_from_outside_releases_its_hold_when_cancelled() {
    let mut venue = seeded_venue("0", AmendConfig::FULL);
    venue
        .modify_externally(&cid("seeded"), price("99"))
        .unwrap();
    assert_eq!(free_quote(&venue), d("1000") - d("0.99"));

    let cancelled = venue.cancel_order(OrderRequestCancel {
        key: limit_request("seeded", Side::Buy, "0.01", "99", gtc()).key,
        state: RequestCancel { id: None },
    });

    assert!(cancelled.response.state.is_ok(), "{cancelled:?}");
    assert_eq!(free_quote(&venue), d("1000"));
}

/// A seeded order that arrived part-filled holds only what it has left when changed from outside,
/// and its fill pays only for that.
#[test]
fn a_part_filled_seeded_order_changed_from_outside_holds_and_pays_only_its_remainder() {
    let mut venue = seeded_venue("0.004", AmendConfig::FULL);

    venue
        .modify_externally(&cid("seeded"), price("99"))
        .unwrap();
    assert_eq!(free_quote(&venue), d("1000") - d("0.594"), "0.006 at 99");

    let _ = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(book("97", "98")),
        time(1),
    );
    assert!(venue.account.is_filled(&cid("seeded")));
    let balances = venue.balances(&[base(), quote()]);
    let [btc, usdt] = balances.as_slice() else {
        panic!("both balances");
    };
    assert_eq!(btc.balance.total, d("1.006"));
    assert_eq!(usdt.balance.total, d("1000") - d("0.594"));
}

#[test]
fn on_a_stale_market_a_price_amend_is_refused_and_a_quantity_amend_is_made() {
    let mut venue = venue_waiting();
    let _ = venue.open_order(limit_request("resting", Side::Buy, "1", "90", gtc()));
    advance(&mut venue, time(12));

    let repriced = amend(&mut venue, "resting", price("95"));
    assert_venue_refusal(&repriced);
    assert_eq!(resting(&venue, "resting").price, Some(d("90")));

    let resized = amend(&mut venue, "resting", quantity("2"));
    assert_eq!(amended(&resized).quantity, d("2"));
    assert_eq!(free_quote(&venue), d("10000") - d("180"));

    // Held until a fresh market, so not on the book to amend.
    let _ = venue.open_order(limit_request("held", Side::Buy, "1", "95", gtc()));
    assert!(venue.account.orders().holds(&instrument_name()));
    assert_venue_refusal(&amend(&mut venue, "held", quantity("2")));
}

#[test]
fn on_a_stale_market_an_amend_of_an_order_the_market_crosses_is_refused() {
    let mut venue = SimulatedVenue::new_market_driven(&stale_config(MAX_AGE), spot_instruments());
    advance(&mut venue, time(1));
    // Only 0.4 offered at 100: a buy of 1 at 101 takes it, and the rest stays crossing the book.
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(book("90", "100"))
                    .with_depth(MarketDepth::new(None, Some(d("0.4")))),
                time(1),
            )
            .is_empty()
    );
    let rested = venue.open_order(limit_request("crossing", Side::Buy, "1", "101", gtc()));
    assert_eq!(rested_open(&rested).filled_quantity, d("0.4"));
    advance(&mut venue, time(12));
    let before = resting(&venue, "crossing");
    let free = free_quote(&venue);

    // Either would trade against a market the venue no longer trusts.
    for patch in [quantity("2"), quantity("0.8")] {
        let outcome = amend(&mut venue, "crossing", patch);
        assert_venue_refusal(&outcome);
        assert!(outcome.events.is_empty(), "{:?}", outcome.events);
    }
    assert_eq!(resting(&venue, "crossing"), before);
    assert_eq!(free_quote(&venue), free);
}

#[test]
fn a_market_order_held_for_its_next_bar_is_not_amended() {
    let config = spot_config("10", "1000000", FeeModelConfig::default())
        .with_market_order_timing(MarketOrderTiming::NextBar(NextBarPrice::Open));
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(feed_bar(&mut venue, 1, ohlc("95", "105", "90", "100")).is_empty());
    let held = venue.open_order(market_order("held", Side::Buy, "1", gtc()));
    assert!(
        held.response.state.is_accepted(),
        "{:?}",
        held.response.state
    );
    assert!(venue.account.orders().holds(&instrument_name()));
    let free = free_quote(&venue);

    assert_venue_refusal(&amend(&mut venue, "held", quantity("2")));
    assert_eq!(free_quote(&venue), free);
    assert!(venue.account.orders().holds(&instrument_name()));
}

// --- An external change under a new client order id, and one that ends the order -----------

/// A cancel of the order resting under `name`, by its client order id alone.
fn cancel(venue: &mut SimulatedVenue, name: &str) -> CancelOutcome {
    venue.cancel_order(OrderRequestCancel {
        key: limit_request(name, Side::Buy, "1", "48000", gtc()).key,
        state: RequestCancel { id: None },
    })
}

/// An external change under a new client order id moves the order there: it keeps its venue
/// order id and, lowered in quantity alone, its place in the queue; its snapshot is linked to the
/// order it renames; and the old id no longer finds it.
#[test]
fn an_external_change_under_a_new_client_order_id_renames_the_order() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));
    let before = resting(&venue, "resting");
    let id = before
        .state
        .id
        .assigned()
        .cloned()
        .expect("the venue named it");

    let events = venue
        .modify_externally(
            &cid("resting"),
            ExternalChange::new(quantity("0.5")).with_cid(cid("renamed")),
        )
        .unwrap();

    let reported = report_of(events.last().expect("the order is reported"));
    let after = resting(&venue, "renamed");
    assert_eq!(after.key.cid, cid("renamed"));
    assert_eq!(after.quantity, d("0.5"));
    assert_eq!(
        after.state.id, before.state.id,
        "it keeps its venue order id"
    );
    assert_eq!(reported.order, after.clone().map_state(OrderState::active));
    assert_eq!(
        reported.replaces,
        Some(Replaces::new(id).with_cid(cid("resting")))
    );
    assert_eq!(queue(&venue, Side::Buy), ["renamed", "behind"]);
    assert!(venue.account.orders().get(&cid("resting")).is_none());
    assert!(matches!(
        refusal(&amend(&mut venue, "resting", price("47000"))),
        OrderError::Rejected(_)
    ));
    assert!(cancel(&mut venue, "resting").response.state.is_err());
    assert!(cancel(&mut venue, "renamed").response.state.is_ok());
}

/// A renamed order that loses its place goes behind the orders at its new price, under the new
/// id, and keeps its deadline under it.
#[test]
fn a_renamed_order_keeps_its_deadline_and_is_queued_under_its_new_id() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request(
        "dated",
        Side::Buy,
        "1",
        "47000",
        TimeInForce::GoodTillDate { expiry: time(5) },
    ));
    let _ = venue.open_order(limit_request("there", Side::Buy, "1", "46000", gtc()));

    venue
        .modify_externally(
            &cid("dated"),
            ExternalChange::new(price("46000")).with_cid(cid("renamed")),
        )
        .unwrap();
    assert_eq!(queue(&venue, Side::Buy), ["resting", "there", "renamed"]);

    let retired = venue.advance_time(time(5));
    let ended = retired
        .iter()
        .find_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(report) => Some(&report.0),
            _ => None,
        })
        .expect("the deadline retires it");
    assert_eq!(ended.order.key.cid, cid("renamed"));
    assert!(matches!(ended.order.state, OrderState::Inactive(_)));
    assert_eq!(queue(&venue, Side::Buy), ["resting", "there"]);
}

/// A new client order id that names an order already resting is refused, changing nothing; one
/// equal to the order's own keeps it, linking nothing.
#[test]
fn a_rename_onto_a_resting_order_is_refused_and_onto_its_own_id_keeps_it() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("other", Side::Buy, "1", "46000", gtc()));
    let before = resting(&venue, "resting");

    assert!(matches!(
        venue.modify_externally(
            &cid("resting"),
            ExternalChange::new(quantity("0.5")).with_cid(cid("other")),
        ),
        Err(OrderError::AmendRefused(AmendRefusal::Venue(_)))
    ));
    assert_eq!(resting(&venue, "resting"), before);

    let events = venue
        .modify_externally(
            &cid("resting"),
            ExternalChange::new(quantity("0.5")).with_cid(cid("resting")),
        )
        .unwrap();
    let reported = report_of(events.last().expect("the order is reported"));
    assert_eq!(reported.order.key.cid, cid("resting"));
    assert_eq!(reported.replaces, None);
}

/// A new client order id that names an order held off the book, not queued, is refused too,
/// changing nothing: the id names an order the venue still holds.
#[test]
fn a_rename_onto_a_held_orders_id_is_refused() {
    let mut venue = venue_waiting();
    let _ = venue.open_order(limit_request("resting", Side::Buy, "1", "90", gtc()));
    advance(&mut venue, time(12));
    // Held until a fresh market, where a quantity amend alone is still made.
    let _ = venue.open_order(limit_request("held", Side::Buy, "1", "95", gtc()));
    assert!(venue.account.orders().holds(&instrument_name()));
    let before = resting(&venue, "resting");

    assert!(matches!(
        venue.modify_externally(
            &cid("resting"),
            ExternalChange::new(quantity("0.5")).with_cid(cid("held")),
        ),
        Err(OrderError::AmendRefused(AmendRefusal::Venue(_)))
    ));
    assert_eq!(resting(&venue, "resting"), before);
    assert_eq!(queue(&venue, Side::Buy), ["resting"]);
    assert!(venue.account.orders().holds(&instrument_name()));

    let events = venue
        .modify_externally(&cid("resting"), quantity("0.5"))
        .unwrap();
    assert_eq!(report_of(events.last().unwrap()).order.quantity, d("0.5"));
}

/// A rename onto the client order id of an order that has ended forgets how that one ended, as
/// placing a new order under it does: the id names the renamed order alone, listed once, as open.
#[test]
fn a_rename_onto_an_ended_orders_id_forgets_how_that_one_ended() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("renamed", Side::Buy, "1", "46000", gtc()));
    assert!(cancel(&mut venue, "renamed").response.state.is_ok());
    let key = limit_request("renamed", Side::Buy, "1", "48000", gtc()).key;
    assert_eq!(venue.orders_ended(std::slice::from_ref(&key)).len(), 1);

    venue
        .modify_externally(
            &cid("resting"),
            ExternalChange::new(quantity("0.5")).with_cid(cid("renamed")),
        )
        .unwrap();

    assert!(venue.orders_ended(&[key]).is_empty());
    let listed: Vec<_> = venue
        .account_snapshot(&[])
        .instruments
        .iter()
        .flat_map(|instrument| &instrument.orders)
        .filter(|report| report.order.key.cid == cid("renamed"))
        .map(|report| report.order.state.clone())
        .collect();
    assert!(
        matches!(listed.as_slice(), [OrderState::Active(_)]),
        "{listed:?}"
    );
}

/// A rename on a venue that also reassigns the venue order id links both old ids; restarted, the
/// order under its old venue order id is reported ended under its old client order id.
#[test]
fn a_rename_with_a_reassigned_id_links_both_old_ids() {
    for restarts in [false, true] {
        let mut venue = resting_buy_amending(
            AmendConfig::FULL
                .with_reassign_id(true)
                .with_filled_restarts(restarts),
        );
        let old = resting(&venue, "resting").state.id;
        let old_id = old.assigned().cloned().expect("the venue named it");

        let events = venue
            .modify_externally(
                &cid("resting"),
                ExternalChange::new(price("47000")).with_cid(cid("renamed")),
            )
            .unwrap();

        let after = resting(&venue, "renamed");
        assert_ne!(after.state.id, old, "restarts {restarts}");
        let reports: Vec<_> = events
            .iter()
            .filter(|event| matches!(event.kind, AccountEventKind::OrderSnapshot(_)))
            .map(report_of)
            .collect();
        let (replacement, ended) = match reports.as_slice() {
            [replacement] => (replacement, None),
            [replacement, ended] => (replacement, Some(ended)),
            other => panic!("restarts {restarts}: {other:?}"),
        };
        assert_eq!(replacement.order.key.cid, cid("renamed"));
        assert_eq!(
            replacement.replaces,
            Some(Replaces::new(old_id.clone()).with_cid(cid("resting")))
        );
        assert_eq!(ended.is_some(), restarts);
        if let Some(ended) = ended {
            assert_eq!(ended.order.key.cid, cid("resting"));
            assert!(matches!(ended.order.state, OrderState::Inactive(_)));
        }
    }
}

/// [`resting_buy`] after an amend fills 0.4 of it at 49,100 and rests the rest at 49,200.
fn partly_filled_buy() -> SimulatedVenue {
    let mut venue = resting_buy_amending(AmendConfig::FULL);
    offer_only(&mut venue, "0.4");
    let outcome = amend(&mut venue, "resting", price("49200"));
    assert_eq!(amended(&outcome).open.filled_quantity, d("0.4"));
    venue
}

/// An external amend down to the filled quantity or below ends the order, as Binance does: its
/// hold is released and it is reported expired with what filled, its quantity cut to that, as the
/// venue then remembers it; under a new client order id, linked to the order it renames.
#[test]
fn an_external_amend_down_to_the_fill_ends_the_order() {
    for (amended_to, rename) in [("0.4", None), ("0.1", None), ("0.4", Some("renamed"))] {
        let mut venue = partly_filled_buy();
        let before = resting(&venue, "resting");
        let id = before
            .state
            .id
            .assigned()
            .cloned()
            .expect("the venue named it");
        let free = free_quote(&venue);
        let mut change = ExternalChange::new(quantity(amended_to));
        if let Some(rename) = rename {
            change = change.with_cid(cid(rename));
        }

        let events = venue.modify_externally(&cid("resting"), change).unwrap();

        let case = format!("{amended_to} {rename:?}");
        let [restated, reported] = events.as_slice() else {
            panic!("{case}: the released hold, then the order: {events:?}");
        };
        assert!(
            balance_of(restated).balance.free > free,
            "{case}: the hold is released"
        );
        let reported = report_of(reported);
        let name = rename.unwrap_or("resting");
        assert_eq!(reported.order.key.cid, cid(name), "{case}");
        assert_eq!(reported.order.quantity, d("0.4"), "{case}");
        assert_eq!(reported.order.price, before.price, "{case}");
        assert_eq!(reported.reported, ReportedTerms::ALL, "{case}");
        assert_eq!(
            reported.order.state,
            OrderState::expired(Expired::new(id.clone(), time(2), Some(d("0.4")))),
            "{case}"
        );
        assert_eq!(
            reported.replaces,
            rename.map(|_| Replaces::new(id.clone()).with_cid(cid("resting"))),
            "{case}"
        );
        assert!(venue.account.orders().is_empty(), "{case}");
        let key = OrderKey {
            cid: cid(name),
            ..limit_request("resting", Side::Buy, "1", "48000", gtc()).key
        };
        let [ended] = &venue.orders_ended(&[key])[..] else {
            panic!("{case}: the venue remembers how it ended");
        };
        assert!(
            matches!(ended.state, InactiveOrderState::Expired(_)),
            "{case}"
        );
    }
}

/// A client's own amend down to the fill is still refused: the refusal is the library's, before
/// any request leaves. And an external one the book cannot make, or that changes a field the
/// order cannot take, is refused, changing nothing.
#[test]
fn only_an_external_amend_down_to_the_fill_ends_the_order() {
    let mut venue = partly_filled_buy();
    let before = resting(&venue, "resting");
    assert_eq!(
        *amend_refusal(&amend(&mut venue, "resting", quantity("0.4"))),
        AmendRefusal::QuantityNotAboveFilled { filled: d("0.4") }
    );
    assert!(matches!(
        venue.modify_externally(
            &cid("resting"),
            AmendPatch::default()
                .with_quantity(d("0.4"))
                .with_trigger_price(d("1")),
        ),
        Err(OrderError::AmendRefused(AmendRefusal::NotOnKind { .. }))
    ));
    assert_eq!(resting(&venue, "resting"), before);
}
