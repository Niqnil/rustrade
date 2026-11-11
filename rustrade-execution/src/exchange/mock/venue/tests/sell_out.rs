//! The expiry sell-out: a long option in the money that the account cannot afford to exercise,
//! sold before it expires.

use super::super::liquidation::LiquidationState;
use super::{
    options::{
        STRIKE, balance, buy, driven_instruments, driven_venue, expiry, feed, funds, limit,
        option_instrument, option_name, option_position, spy, spy_name, trades, usd,
    },
    *,
};
use crate::{
    expiry::{ExpirySellOut, LongOptionExpiry, SellOutAffordability},
    fill::SimFillConfig,
    margin::SpotMarginLiquidation,
    slippage::SlippageConfig,
};
use rustrade_instrument::instrument::{
    kind::option::{OptionKind, OptionSettlement},
    name::InstrumentNameInternal,
};
use std::time::Duration;

/// Ten seconds before [`expiry`]: when [`sell_out`] checks.
fn check() -> DateTime<Utc> {
    time(90)
}

/// Checked ten seconds before expiry, the whole position sold unless all of it is affordable.
fn sell_out() -> ExpirySellOut {
    ExpirySellOut::new(Duration::from_secs(10))
}

/// Configures [`sell_out`] on a venue funded with `usd_balance` and `spy_balance`.
fn selling_out(
    usd_balance: &str,
    spy_balance: &str,
    sell_out: ExpirySellOut,
) -> impl FnOnce(&mut MockExecutionConfig) {
    let fund = funds(usd_balance, spy_balance);
    move |config| {
        fund(config);
        config.expiry_sell_out = Some(sell_out);
    }
}

/// A venue that sells out under [`sell_out`], holding `contracts` of an option of `kind` bought
/// at 1.5 with SPY at 100, with SPY then at `spy_price` and the option at `option_price` before
/// the check. Funded with `usd_balance` and `spy_balance`.
fn holding(
    kind: OptionKind,
    contracts: &str,
    usd_balance: &str,
    spy_balance: &str,
    spy_price: &str,
    option_price: &str,
) -> SimulatedVenue {
    let mut venue = driven_venue(
        kind,
        OptionSettlement::Physical,
        selling_out(usd_balance, spy_balance, sell_out()),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, contracts);
    feed(&mut venue, spy_price, option_price, time(50));
    venue
}

fn liquidations(
    events: &[UnindexedAccountEvent],
) -> Vec<&Trade<AssetNameExchange, InstrumentNameExchange>> {
    trades(events)
        .into_iter()
        .filter(|trade| trade.origin == TradeOrigin::Liquidation)
        .collect()
}

/// A call costing 10,000 to exercise, held by an account left with 9,850 after its premium, is
/// sold at the check at the price a market sell gets, and nothing is left to settle at expiry.
#[test]
fn a_call_in_the_money_the_account_cannot_exercise_is_sold_at_the_check() {
    let mut venue = holding(OptionKind::Call, "1", "10000", "0", "120", "20");

    let events = venue.advance_time(check());

    let sold = trades(&events);
    assert_eq!(sold.len(), 1, "{events:?}");
    let sale = sold[0];
    assert_eq!(
        (
            &sale.instrument,
            sale.origin,
            sale.side,
            sale.price,
            sale.quantity,
            sale.time_exchange,
            &sale.client_order_id,
        ),
        (
            &option_name(),
            TradeOrigin::Liquidation,
            Side::Sell,
            d("20"),
            d("1"),
            check(),
            &None,
        )
    );
    assert_eq!(option_position(&venue), None);
    // 10,000 less 150 of premium, plus 2,000 for the sale.
    assert_eq!(balance(&venue, usd()).total, d("11850"));
    assert!(trades(&venue.advance_time(expiry())).is_empty());
}

#[test]
fn a_call_the_account_can_exercise_is_held_to_expiry_and_exercised() {
    let mut venue = holding(OptionKind::Call, "1", "20000", "0", "120", "20");

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(option_position(&venue), Some(d("1")));

    let settled = venue.advance_time(expiry());
    assert_eq!(
        trades(&settled)
            .iter()
            .map(|trade| trade.origin)
            .collect::<Vec<_>>(),
        vec![TradeOrigin::Exercise, TradeOrigin::Exercise]
    );
}

/// Exactly the strike's cash is enough: an exercise that takes the free balance to zero is
/// affordable.
#[test]
fn a_call_whose_exercise_takes_exactly_the_free_cash_is_kept() {
    // 10,150 less 150 of premium leaves exactly the 10,000 the exercise pays.
    let mut venue = holding(OptionKind::Call, "1", "10150", "0", "120", "20");

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(option_position(&venue), Some(d("1")));
}

#[test]
fn a_call_out_of_the_money_is_not_sold() {
    let mut venue = holding(OptionKind::Call, "1", "10000", "0", "90", "0.5");

    assert!(trades(&venue.advance_time(check())).is_empty());
    let settled = venue.advance_time(expiry());
    assert_eq!(trades(&settled)[0].origin, TradeOrigin::Expiry);
}

/// At the money is not in the money, as at expiry.
#[test]
fn a_call_at_the_money_is_not_sold() {
    let mut venue = holding(OptionKind::Call, "1", "10000", "0", STRIKE, "0.5");

    assert!(trades(&venue.advance_time(check())).is_empty());
}

#[test]
fn a_put_without_the_shares_is_sold_and_one_with_them_is_kept() {
    let mut unshared = holding(OptionKind::Put, "1", "10000", "0", "80", "20");
    let events = unshared.advance_time(check());
    assert_eq!(liquidations(&events).len(), 1, "{events:?}");
    assert_eq!(option_position(&unshared), None);

    let mut shared = holding(OptionKind::Put, "1", "10000", "100", "80", "20");
    assert!(trades(&shared.advance_time(check())).is_empty());
    let settled = shared.advance_time(expiry());
    assert_eq!(trades(&settled)[1].side, Side::Sell, "{settled:?}");
    assert_eq!(balance(&shared, spy()).total, Decimal::ZERO);
}

/// Shares a covered call holds are not free, so they do not let a put be exercised.
#[test]
fn shares_securing_a_short_call_do_not_count_towards_a_put() {
    let mut call = option_instrument(OptionKind::Call, OptionSettlement::Physical);
    call.name_exchange = InstrumentNameExchange::new("SPY_C100_CALL");
    call.name_internal = InstrumentNameInternal::new("spy_c100_call");
    let mut instruments = driven_instruments(OptionKind::Put, OptionSettlement::Physical);
    instruments.insert(call.name_exchange.clone(), call.clone());
    let config = {
        let mut config = config_from_balances(Vec::new(), FeeModelConfig::default());
        selling_out("10000", "100", sell_out())(&mut config);
        config
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, instruments);
    advance(&mut venue, time(1));
    feed(&mut venue, "100", "1.5", time(2));
    let _ = venue.apply_market(
        &call.name_exchange,
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");
    // The covered call holds all 100 shares.
    let sold = venue.open_order(request(call.name_exchange.clone(), Side::Sell, "1", None));
    assert!(
        sold.response.state.is_accepted(),
        "{:?}",
        sold.response.state
    );
    assert_eq!(balance(&venue, spy()).free, Decimal::ZERO);
    feed(&mut venue, "80", "20", time(50));

    let events = venue.advance_time(check());

    let sales = liquidations(&events);
    assert_eq!(sales.len(), 1, "{events:?}");
    assert_eq!(sales[0].instrument, option_name());
}

#[test]
fn per_contract_keeps_the_contracts_the_account_can_exercise_and_sells_the_rest() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        selling_out(
            "25000",
            "0",
            sell_out().with_affordability(SellOutAffordability::PerContract),
        ),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "3");
    feed(&mut venue, "120", "20", time(50));

    // 25,000 less 450 of premium pays for two exercises at 10,000.
    let events = venue.advance_time(check());

    let sales = liquidations(&events);
    assert_eq!(sales.len(), 1, "{events:?}");
    assert_eq!(sales[0].quantity, d("1"));
    assert_eq!(option_position(&venue), Some(d("2")));
}

#[test]
fn per_position_sells_the_whole_position_when_not_all_of_it_is_affordable() {
    let mut venue = holding(OptionKind::Call, "3", "25000", "0", "120", "20");

    let events = venue.advance_time(check());

    assert_eq!(liquidations(&events)[0].quantity, d("3"));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn the_haircut_cuts_the_price_a_market_sell_gets_and_bid_ask_sells_at_the_bid() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out().with_haircut(d("0.1")).unwrap())(config);
        config.fill_model = SimFillConfig::BidAsk(Default::default());
    });
    feed(&mut venue, "100", "1.5", time(2));
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(book("1.4", "1.5")),
        time(2),
    );
    buy(&mut venue, "1");
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded("120")), time(50));
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(book("19", "21")),
        time(50),
    );

    let events = venue.advance_time(check());

    // 90% of the bid.
    assert_eq!(liquidations(&events)[0].price, d("17.1"));
}

#[test]
fn a_sale_pays_the_sell_side_fee() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out())(config);
        config.fee_model = FeeModelConfig::PerContract(crate::fee::PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(check());

    let sale = liquidations(&events)[0];
    assert_eq!(sale.fees.fees, d("0.65"));
    // 10,000 less 150.65 for the buy, plus 2,000 less 0.65 for the sale.
    assert_eq!(balance(&venue, usd()).total, d("11848.70"));
}

/// Under a bid-ask fill model, a book with no bid gives a sale no price, so nothing is sold and
/// the position goes on to expiry, where it is exercised whatever it costs.
#[test]
fn a_long_with_no_price_to_sell_at_goes_on_to_expiry() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out())(config);
        config.fill_model = SimFillConfig::BidAsk(Default::default());
    });
    let ask_only = MarketSnapshot {
        best_bid: None,
        best_ask: Some(d("1.5")),
        last_price: None,
    };
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded("100")), time(2));
    let _ = venue.apply_market(&option_name(), MarketObservation::new(ask_only), time(2));
    buy(&mut venue, "1");
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded("120")), time(50));

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(option_position(&venue), Some(d("1")));
    let settled = venue.advance_time(expiry());
    assert_eq!(trades(&settled)[0].origin, TradeOrigin::Exercise);
    assert!(balance(&venue, usd()).free.is_sign_negative());
}

/// Without a price for the underlying the venue cannot tell whether the option is in the money,
/// so it sells nothing.
#[test]
fn a_long_whose_underlying_has_no_price_is_not_sold() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        selling_out("10000", "0", sell_out()),
    );
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(option_position(&venue), Some(d("1")));
}

#[test]
fn a_cash_settled_option_is_never_sold() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Cash,
        selling_out("200", "0", sell_out()),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(venue.next_deadline(), Some(expiry()));
}

/// The sell-out happens only with the venue configured for it.
#[test]
fn without_a_sell_out_an_unaffordable_call_is_held_to_expiry() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    assert_eq!(venue.next_deadline(), Some(expiry()));
    assert!(trades(&venue.advance_time(check())).is_empty());
}

#[test]
fn the_orders_working_on_the_option_are_cancelled_before_it_is_sold() {
    let mut venue = holding(OptionKind::Call, "1", "10000", "0", "120", "20");
    let resting = venue.open_order(limit(option_name(), Side::Sell, "50", "1", "rest"));
    assert!(resting.response.state.is_accepted());

    let events = venue.advance_time(check());

    assert_eq!(venue.account.orders_open().count(), 0);
    let cancelled = events
        .iter()
        .position(|event| matches!(&event.kind, AccountEventKind::OrderSnapshot(_)))
        .expect("the resting sale is cancelled");
    let sold = events
        .iter()
        .position(|event| matches!(&event.kind, AccountEventKind::Trade(_)))
        .expect("the option is sold");
    assert!(cancelled < sold, "{events:?}");
    assert_eq!(option_position(&venue), None);
}

/// On a venue that lends, a call's exercise may borrow what the free cash lacks, so the account
/// can afford it.
#[test]
fn on_a_margin_venue_what_it_would_lend_counts_towards_a_call() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out())(config);
        config.spot_margin =
            Some(SpotMarginConfig::new(usd(), d("3"), MarginEffect::Borrow).unwrap());
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    assert!(trades(&venue.advance_time(check())).is_empty());
    assert_eq!(option_position(&venue), Some(d("1")));
}

/// A venue that sells out under [`sell_out`], funded with `usd_balance` and holding one contract
/// each of two calls bought at 1.5 with SPY at 100, [`option_name`] and the returned second one,
/// with SPY then at 120 and both calls at 20 before the check.
fn holding_two_calls(usd_balance: &str) -> (SimulatedVenue, InstrumentNameExchange) {
    let mut second = option_instrument(OptionKind::Call, OptionSettlement::Physical);
    second.name_exchange = InstrumentNameExchange::new("SPY_C100_B");
    second.name_internal = InstrumentNameInternal::new("spy_c100_b");
    let mut instruments = driven_instruments(OptionKind::Call, OptionSettlement::Physical);
    instruments.insert(second.name_exchange.clone(), second.clone());
    let mut config = config_from_balances(Vec::new(), FeeModelConfig::default());
    selling_out(usd_balance, "0", sell_out())(&mut config);
    let mut venue = SimulatedVenue::new_market_driven(&config, instruments);
    advance(&mut venue, time(1));
    for (instrument, price, at) in [
        (spy_name(), "100", 2),
        (option_name(), "1.5", 2),
        (second.name_exchange.clone(), "1.5", 2),
        (spy_name(), "120", 50),
        (option_name(), "20", 50),
        (second.name_exchange.clone(), "20", 50),
    ] {
        let _ = venue.apply_market(&instrument, MarketObservation::new(traded(price)), time(at));
        if at == 2 && instrument != spy_name() {
            let bought = venue.open_order(request(instrument, Side::Buy, "1", None));
            assert!(bought.response.state.is_accepted());
        }
    }
    (venue, second.name_exchange)
}

/// A long the check keeps is not priced, so it takes no slippage draw and a seeded run's later
/// fills are the same as without the check.
#[test]
fn a_long_the_check_keeps_takes_no_slippage_draw() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("20000", "0", sell_out())(config);
        config.slippage = SlippageConfig::uniform(d("0.01"), d("0.025"), 3).unwrap();
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));
    let slippage = venue.slippage.clone();

    assert!(trades(&venue.advance_time(check())).is_empty());

    assert_eq!(option_position(&venue), Some(d("1")));
    assert_eq!(venue.slippage, slippage);
}

/// While a liquidation is under way the venue lends nothing, so what it would otherwise lend
/// does not count towards a call, and a call the free cash cannot exercise is sold.
#[test]
fn during_a_liquidation_what_the_venue_would_lend_does_not_count_towards_a_call() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out())(config);
        config.spot_margin = Some(
            SpotMarginConfig::new(usd(), d("3"), MarginEffect::Borrow)
                .unwrap()
                .with_liquidation(SpotMarginLiquidation::new(d("1.1"), d("0")).unwrap())
                .unwrap(),
        );
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));
    // As a venue restored from a snapshot taken while a thin book left a liquidation unfinished.
    venue.liquidation = LiquidationState::restored(true, false);

    let events = venue.advance_time(check());

    assert_eq!(liquidations(&events).len(), 1, "{events:?}");
    assert_eq!(option_position(&venue), None);
}

/// Two longs checked at one instant share the free cash in name order: the first takes what its
/// exercise needs, and the second, left short, is sold.
#[test]
fn longs_checked_at_one_instant_share_the_free_cash_in_name_order() {
    let (mut venue, second) = holding_two_calls("15000");

    // 15,000 less 300 of premium pays for one exercise.
    let events = venue.advance_time(check());

    let sales = liquidations(&events);
    assert_eq!(sales.len(), 1, "{events:?}");
    assert_eq!(sales[0].instrument, second);
    assert_eq!(option_position(&venue), Some(d("1")));
}

/// What the first long's exercise takes is exactly its cost, so free cash that pays for both
/// exercises keeps both.
#[test]
fn longs_checked_at_one_instant_are_both_kept_when_the_free_cash_pays_for_both() {
    // 20,300 less 300 of premium leaves exactly the 20,000 the two exercises pay.
    let (mut venue, second) = holding_two_calls("20300");

    let events = venue.advance_time(check());

    assert!(trades(&events).is_empty(), "{events:?}");
    assert_eq!(option_position(&venue), Some(d("1")));
    assert!(
        venue
            .positions
            .get(&second)
            .is_some_and(NetPosition::is_long)
    );
}

#[test]
fn the_next_deadline_includes_the_sell_out_check() {
    let venue = holding(OptionKind::Call, "1", "10000", "0", "120", "20");

    assert_eq!(venue.next_deadline(), Some(check()));
}

/// A check at the expiry's own instant sells before the option settles.
#[test]
fn a_check_at_expiry_sells_before_settling() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        selling_out("10000", "0", ExpirySellOut::new(Duration::ZERO)),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    let all = trades(&events);
    assert_eq!(all.len(), 1, "{events:?}");
    assert_eq!(all[0].origin, TradeOrigin::Liquidation);
}

/// A check at the expiry's own instant that keeps the long leaves the expiry to settle it.
#[test]
fn a_check_at_expiry_that_keeps_the_long_leaves_it_to_be_exercised() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        selling_out("20000", "0", ExpirySellOut::new(Duration::ZERO)),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    assert_eq!(
        trades(&events)
            .iter()
            .map(|trade| trade.origin)
            .collect::<Vec<_>>(),
        vec![TradeOrigin::Exercise, TradeOrigin::Exercise],
        "{events:?}"
    );
    assert_eq!(option_position(&venue), None);
}

/// One jump past both the check and the expiry still checks first, against the market held.
#[test]
fn a_jump_past_the_check_and_the_expiry_sells_then_has_nothing_to_settle() {
    let mut venue = holding(OptionKind::Call, "1", "10000", "0", "120", "20");

    let events = venue.advance_time(time(200));

    let all = trades(&events);
    assert_eq!(all.len(), 1, "{events:?}");
    assert_eq!(all[0].origin, TradeOrigin::Liquidation);
}

/// Under [`LongOptionExpiry::Lapse`] what is sold is sold, and what is kept lapses.
#[test]
fn under_lapse_an_unaffordable_long_is_still_sold() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        selling_out("10000", "0", sell_out())(config);
        config.long_option_expiry = LongOptionExpiry::Lapse;
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(check());

    assert_eq!(liquidations(&events).len(), 1, "{events:?}");
}

/// A restored venue still checks a long it holds when the check falls after the snapshot.
#[test]
fn a_restored_venue_still_checks_a_long_it_holds() {
    let venue = holding(OptionKind::Call, "1", "10000", "0", "120", "20");
    let snapshot =
        serde_json::from_str(&serde_json::to_string(&venue.snapshot()).unwrap()).unwrap();
    let mut config = config_from_balances(Vec::new(), FeeModelConfig::default());
    selling_out("10000", "0", sell_out())(&mut config);
    let mut restored = SimulatedVenue::from_snapshot(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Physical),
        snapshot,
    )
    .expect("a venue's own snapshot restores");

    assert_eq!(restored.next_deadline(), Some(check()));
    // The restored venue holds no market until it is fed one.
    feed(&mut restored, "120", "20", time(60));
    let events = restored.advance_time(check());
    assert_eq!(liquidations(&events).len(), 1, "{events:?}");
}

#[test]
fn a_haircut_outside_zero_to_one_is_refused() {
    for haircut in ["-0.01", "1", "1.5"] {
        assert_eq!(
            sell_out().with_haircut(d(haircut)),
            Err(crate::expiry::ExpirySellOutError::Haircut {
                haircut: d(haircut)
            })
        );
    }
    assert!(sell_out().with_haircut(Decimal::ZERO).is_ok());
    assert!(sell_out().with_haircut(d("0.99")).is_ok());
}

#[test]
fn a_sell_out_round_trips_through_serde_and_a_bad_haircut_does_not_deserialise() {
    let config = sell_out()
        .with_affordability(SellOutAffordability::PerContract)
        .with_haircut(d("0.02"))
        .unwrap();
    let json = serde_json::to_string(&config).unwrap();
    assert_eq!(
        serde_json::from_str::<ExpirySellOut>(&json).unwrap(),
        config
    );
    let bad = json.replace("0.02", "1.5");
    assert!(serde_json::from_str::<ExpirySellOut>(&bad).is_err());
    // Only the time is required.
    assert_eq!(
        serde_json::from_str::<ExpirySellOut>(r#"{"before_expiry":{"secs":10,"nanos":0}}"#)
            .unwrap(),
        sell_out()
    );
}
