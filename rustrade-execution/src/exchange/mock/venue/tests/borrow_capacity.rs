//! What the borrow capacity query reports: how much more the account can borrow now, without an
//! order to price it.

use super::*;

/// A fed venue for `config`, trading BTC at `price` from `time(1)`.
fn fed_venue(config: &MockExecutionConfig, price: &str) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(
        venue
            .apply_market(
                &instrument_name(),
                MarketObservation::new(traded(price)),
                time(1),
            )
            .is_empty()
    );
    venue
}

/// The capacity `venue` reports for an order on BTCUSDT on `side`.
fn capacity(venue: &SimulatedVenue, side: Side) -> BorrowCapacity {
    venue
        .borrow_capacity(&instrument_name(), side)
        .expect("a margin venue lends against BTCUSDT")
}

/// The reason `venue` refuses to report a capacity for `instrument`.
fn invalid_reason(venue: &SimulatedVenue, instrument: &InstrumentNameExchange) -> String {
    match venue.borrow_capacity(instrument, Side::Sell) {
        Err(ApiError::InstrumentInvalid(named, reason)) => {
            assert_eq!(named, *instrument);
            reason
        }
        other => panic!("expected InstrumentInvalid, got: {other:?}"),
    }
}

/// 100,000 USDT held, owing `btc_owed` BTC as principal and `btc_interest` as interest, lending
/// up to 3x.
fn owing_btc_config(btc_owed: &str, btc_interest: &str) -> MockExecutionConfig {
    MockExecutionConfig {
        spot_margin: Some(SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow).unwrap()),
        ..config_from_balances(
            vec![
                AssetBalance::new(
                    base(),
                    Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d(btc_owed), d(btc_interest)),
                    Utc::now(),
                ),
                funded(quote(), d("100000")),
            ],
            FeeModelConfig::default(),
        )
    }
}

#[test]
fn a_sell_borrows_base_and_a_buy_quote_up_to_the_leverage_limit() {
    // 10,000 USDT at 3x lends 20,000 USDT of value: 0.4 BTC at 50,000.
    let venue = fed_venue(&margin_config("0", "10000", "3"), "50000");

    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base()).with_borrowable_now(d("0.4"))
    );
    assert_eq!(
        capacity(&venue, Side::Buy),
        BorrowCapacity::new(quote()).with_borrowable_now(d("20000"))
    );
}

#[test]
fn the_capacity_is_what_an_order_arriving_now_is_lent() {
    let mut venue = fed_venue(&margin_config("0", "10000", "3"), "50000");
    let borrowable_now = capacity(&venue, Side::Sell).borrowable_now.unwrap();

    let outcome = venue.open_order(sell_request("1", None));

    assert!(
        insufficient_reason(&outcome).ends_with(&format!("Borrowable: {borrowable_now}")),
        "{}",
        insufficient_reason(&outcome)
    );
}

#[test]
fn the_free_balance_is_not_added_to_the_capacity() {
    // The 0.25 BTC held lets a sell deliver 0.25 before it borrows, but is not added here. The
    // leverage limit counts it as 12,500 of equity: 22,500 lends twice that at 3x, 0.9 BTC.
    let venue = fed_venue(&margin_config("0.25", "10000", "3"), "50000");

    assert_eq!(capacity(&venue, Side::Sell).borrowable_now, Some(d("0.9")));
}

#[test]
fn a_cap_tighter_than_the_leverage_limit_bounds_the_capacity_and_is_its_limit() {
    let config = with_max_borrow(margin_config("0", "10000", "3"), base(), "0.3");
    let venue = fed_venue(&config, "50000");

    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base())
            .with_borrowable_now(d("0.3"))
            .with_account_limit(d("0.3"))
    );
    assert_eq!(
        capacity(&venue, Side::Buy),
        BorrowCapacity::new(quote()).with_borrowable_now(d("20000")),
        "the cap is on BTC alone"
    );
}

#[test]
fn a_cap_looser_than_the_leverage_limit_is_still_reported_as_the_limit() {
    let config = with_max_borrow(margin_config("0", "10000", "3"), base(), "1");
    let venue = fed_venue(&config, "50000");

    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base())
            .with_borrowable_now(d("0.4"))
            .with_account_limit(d("1"))
    );
}

#[test]
fn what_the_account_owes_principal_and_interest_counts_against_the_cap() {
    // Owes 0.1 BTC plus 0.01 interest, so a cap of 0.3 leaves 0.19. The leverage limit leaves
    // far more: 94,500 equity at 3x, less 5,500 owed, is 3.67 BTC.
    let config = with_max_borrow(owing_btc_config("0.1", "0.01"), base(), "0.3");
    let venue = fed_venue(&config, "50000");

    assert_eq!(capacity(&venue, Side::Sell).borrowable_now, Some(d("0.19")));
}

#[test]
fn a_debt_past_the_cap_leaves_nothing_to_borrow() {
    let config = with_max_borrow(owing_btc_config("0.1", "0.01"), base(), "0.05");
    let venue = fed_venue(&config, "50000");

    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base())
            .with_borrowable_now(Decimal::ZERO)
            .with_account_limit(d("0.05"))
    );
}

#[test]
fn room_below_the_minimum_borrow_is_reported_as_none_to_borrow() {
    // The leverage limit leaves 0.4 BTC, less than the least that may be lent.
    let config = with_min_borrow(margin_config("0", "10000", "3"), base(), "0.5");
    assert_eq!(
        capacity(&fed_venue(&config, "50000"), Side::Sell).borrowable_now,
        Some(Decimal::ZERO)
    );

    // Room of exactly the minimum may be lent.
    let config = with_min_borrow(margin_config("0", "10000", "3"), base(), "0.4");
    assert_eq!(
        capacity(&fed_venue(&config, "50000"), Side::Sell).borrowable_now,
        Some(d("0.4"))
    );

    // The cap leaves 0.3, less than the minimum, though the leverage limit leaves more.
    let config = with_min_borrow(
        with_max_borrow(margin_config("0", "100000", "3"), base(), "0.3"),
        base(),
        "0.35",
    );
    assert_eq!(
        capacity(&fed_venue(&config, "50000"), Side::Sell).borrowable_now,
        Some(Decimal::ZERO)
    );

    // The cap leaves exactly the minimum, which may be lent.
    let config = with_min_borrow(
        with_max_borrow(margin_config("0", "100000", "3"), base(), "0.3"),
        base(),
        "0.3",
    );
    assert_eq!(
        capacity(&fed_venue(&config, "50000"), Side::Sell).borrowable_now,
        Some(d("0.3"))
    );
}

#[test]
fn a_venue_without_markets_answers_only_what_it_can_value() {
    // No market prices BTC, so a sell's capacity is unknown, while a buy borrows the valuation
    // asset against an account holding only that.
    let venue = margin_venue("0", "10000", "3");
    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base()),
        "BTC cannot be valued"
    );
    assert_eq!(capacity(&venue, Side::Buy).borrowable_now, Some(d("20000")));

    // A BTC holding the venue cannot value makes even the valuation asset's capacity unknown,
    // where the leverage limit would leave it out of the equity and understate it.
    let venue = margin_venue("1", "10000", "3");
    assert_eq!(capacity(&venue, Side::Buy), BorrowCapacity::new(quote()));
}

#[test]
fn a_cap_that_rules_a_loan_out_is_known_without_a_market() {
    let config = with_max_borrow(margin_config("0", "10000", "3"), base(), "0");
    let venue = SimulatedVenue::new(&config, spot_instruments());
    assert_eq!(
        capacity(&venue, Side::Sell),
        BorrowCapacity::new(base())
            .with_borrowable_now(Decimal::ZERO)
            .with_account_limit(Decimal::ZERO)
    );

    let config = with_min_borrow(
        with_max_borrow(margin_config("0", "10000", "3"), base(), "0.3"),
        base(),
        "0.35",
    );
    let venue = SimulatedVenue::new(&config, spot_instruments());
    assert_eq!(
        capacity(&venue, Side::Sell).borrowable_now,
        Some(Decimal::ZERO),
        "what the cap leaves is below the minimum borrow"
    );

    // A cap with room left says nothing of the leverage limit, so the capacity stays unknown.
    let config = with_max_borrow(margin_config("0", "10000", "3"), base(), "0.3");
    let venue = SimulatedVenue::new(&config, spot_instruments());
    assert_eq!(capacity(&venue, Side::Sell).borrowable_now, None);
}

#[test]
fn a_cash_venue_lends_against_no_instrument() {
    let venue = make_venue("1", "10000");

    let reason = invalid_reason(&venue, &instrument_name());
    assert!(reason.contains("no spot margin configuration"), "{reason}");
}

#[test]
fn an_instrument_the_venue_does_not_trade_is_invalid() {
    let venue = margin_venue("0", "10000", "3");

    invalid_reason(&venue, &InstrumentNameExchange::new("ETHUSDT"));
}

#[test]
fn a_cash_settled_instrument_is_invalid_on_a_margin_venue() {
    let config = MockExecutionConfig {
        spot_margin: Some(SpotMarginConfig::new(usd(), d("3"), MarginEffect::Borrow).unwrap()),
        ..config_from_balances(vec![funded(usd(), d("10000"))], FeeModelConfig::default())
    };
    let venue = SimulatedVenue::new(&config, instruments_of(cfd_instrument()));

    let reason = invalid_reason(&venue, &cfd_instrument_name());
    assert!(reason.contains("cash-settled"), "{reason}");
}
