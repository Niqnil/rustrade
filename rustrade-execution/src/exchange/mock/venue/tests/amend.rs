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
fn amend(venue: &mut SimulatedVenue, name: &str, patch: AmendPatch) -> AmendOutcome {
    let request = amend_of(venue, instrument_name(), name, patch);
    venue.amend_order(request)
}

fn price(price: &str) -> AmendPatch {
    AmendPatch::default().with_price(d(price))
}

fn quantity(quantity: &str) -> AmendPatch {
    AmendPatch::default().with_quantity(d(quantity))
}

/// The order an amend answered with, or a panic naming what it answered instead.
fn amended(outcome: &AmendOutcome) -> &Amended {
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

    let [restated] = outcome.events.as_slice() else {
        panic!("an amend that trades nothing restates its hold alone: {outcome:?}");
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
fn a_quantity_decrease_keeps_the_orders_place_and_arrival() {
    let mut venue = resting_buy();
    let _ = venue.open_order(limit_request("behind", Side::Buy, "1", "48000", gtc()));
    let before = resting(&venue, "resting");

    let outcome = amend(&mut venue, "resting", quantity("0.5"));

    assert_eq!(
        amended(&outcome).open.time_exchange,
        before.state.time_exchange
    );
    assert_eq!(resting(&venue, "resting").state, before.state);
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

    let [paid, _received, traded] = outcome.events.as_slice() else {
        panic!("the balances then the trade: {:?}", outcome.events);
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
    assert_eq!(resting(&venue, "resting"), before);
    assert_eq!(queue(&venue, Side::Buy), ["resting", "behind"]);
    assert_eq!(free_quote(&venue), free);
    assert_eq!(venue.balances(&[quote()])[0].balance.total, d("1000000"));
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
