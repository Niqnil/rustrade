//! The borrow price window: how much the venue lends when it values each asset over a trailing
//! window of its price, at the highest for an asset the account owes and the lowest for the rest.

use super::*;
use crate::{margin::SpotMarginLiquidation, notice::NoticeKind};

/// A margin account valued in USDT, holding `balances`, lending up to `max_leverage`, with the
/// borrow price `window` if any, and the rest of the spot margin configuration `configure` sets.
fn window_config(
    balances: Vec<AssetBalance<AssetNameExchange>>,
    max_leverage: &str,
    window: Option<TimeDelta>,
    configure: impl FnOnce(SpotMarginConfig) -> SpotMarginConfig,
) -> MockExecutionConfig {
    let mut spot_margin =
        SpotMarginConfig::new(quote(), d(max_leverage), MarginEffect::Borrow).unwrap();
    if let Some(window) = window {
        spot_margin = spot_margin.with_borrow_price_window(window).unwrap();
    }
    MockExecutionConfig {
        spot_margin: Some(configure(spot_margin)),
        ..config_from_balances(balances, FeeModelConfig::default())
    }
}

/// An hour, which every test's prices fall within unless it says otherwise.
fn hour() -> Option<TimeDelta> {
    Some(TimeDelta::hours(1))
}

/// `amount` of `asset` held, `borrowed` of it owed, no interest.
fn holding(
    asset: AssetNameExchange,
    amount: &str,
    borrowed: &str,
) -> AssetBalance<AssetNameExchange> {
    AssetBalance::new(
        asset,
        Balance::new_margin(d(amount), d(amount), d(borrowed), Decimal::ZERO),
        Utc::now(),
    )
}

/// Feeds `instrument` each of `prices`, the second of each pair, at `time` of the first.
fn feed(venue: &mut SimulatedVenue, instrument: &InstrumentNameExchange, prices: &[(i64, &str)]) {
    for (at, price) in prices {
        let events =
            venue.apply_market(instrument, MarketObservation::new(traded(price)), time(*at));
        assert!(events.is_empty(), "{events:?}");
    }
}

/// How much more of BTC (`Side::Sell`) or USDT (`Side::Buy`) the venue would lend now.
fn borrowable_now(venue: &SimulatedVenue, side: Side) -> Decimal {
    venue
        .borrow_capacity(&instrument_name(), side)
        .unwrap()
        .borrowable_now
        .unwrap()
}

/// 0.1 BTC and 1 ETH at 5x, BTC counted at 99.9 % and ETH at 95 %: Binance's worked example.
fn binances_example(window: Option<TimeDelta>) -> SimulatedVenue {
    let config = window_config(
        vec![
            funded(base(), d("0.1")),
            funded(eth(), d("1")),
            funded(quote(), Decimal::ZERO),
        ],
        "5",
        window,
        |config| {
            config
                .with_borrow_collateral_ratio(base(), d("0.999"))
                .unwrap()
                .with_borrow_collateral_ratio(eth(), d("0.95"))
                .unwrap()
        },
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    // BTC between 80,200 and 80,500, now 80,300; ETH between 2,180 and 2,200, now 2,190.
    feed(
        &mut venue,
        &instrument_name(),
        &[(1, "80200"), (2, "80500"), (3, "80300")],
    );
    feed(
        &mut venue,
        &eth_instrument_name(),
        &[(1, "2200"), (2, "2180"), (3, "2190")],
    );
    venue
}

#[test]
fn binances_example_values_the_borrowed_token_at_its_highest_and_the_others_at_their_lowest() {
    // Borrowing BTC makes it a token the account owes: all 0.1 BTC count at 80,500, and so does
    // the loan. ETH, held and not owed, counts at 2,180. Binance's answer is 0.5025.
    let lent = borrowable_now(&binances_example(hour()), Side::Sell);

    assert_eq!(lent, d("4") * (d("8041.95") + d("2071")) / d("80500"));
    assert_eq!(lent.round_dp(4), d("0.5025"));
}

#[test]
fn without_a_window_the_current_prices_value_the_account() {
    // 0.1 BTC at 80,300 and 1 ETH at 2,190: more than the window lends.
    let lent = borrowable_now(&binances_example(None), Side::Sell);

    assert_eq!(lent, d("4") * (d("8021.97") + d("2080.5")) / d("80300"));
}

#[test]
fn an_asset_held_and_not_owed_counts_at_its_lowest() {
    // 1 BTC, low 8,000 in the window, now 9,000: 8,000 of equity at 3x lends 16,000 USDT, where
    // the current price would lend 18,000.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    feed(
        &mut venue,
        &instrument_name(),
        &[(1, "10000"), (2, "8000"), (3, "9000")],
    );

    assert_eq!(borrowable_now(&venue, Side::Buy), d("16000"));
}

#[test]
fn a_price_older_than_the_window_no_longer_counts() {
    // A minute's window: 8,000 at 1 s counts at 30 s, and not once the clock reaches 62 s, though
    // nothing was observed since 9,000 at 30 s.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        Some(TimeDelta::minutes(1)),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    feed(&mut venue, &instrument_name(), &[(1, "8000"), (30, "9000")]);
    assert_eq!(borrowable_now(&venue, Side::Buy), d("16000"));

    advance(&mut venue, time(61));
    assert_eq!(
        borrowable_now(&venue, Side::Buy),
        d("16000"),
        "61 s: 1 s is in"
    );

    advance(&mut venue, time(62));
    assert_eq!(
        borrowable_now(&venue, Side::Buy),
        d("18000"),
        "62 s: 1 s is out"
    );

    // Nothing observed in the last minute: the window is empty, and the current price counts.
    advance(&mut venue, time(100));
    assert_eq!(
        borrowable_now(&venue, Side::Buy),
        d("18000"),
        "100 s: only the current"
    );
}

#[test]
fn the_window_never_lends_more_than_the_current_prices() {
    // 10 BTC held and 1 owed, at 5x and a ratio of 0.95. BTC is owed, so the window values it at
    // its high of 20,000: room 4 × 171,000 − 20,000 = 664,000. At its current 10,000, the room is
    // 4 × 85,500 − 10,000 = 332,000, and that is what it lends.
    let config = window_config(
        vec![holding(base(), "10", "1"), funded(quote(), Decimal::ZERO)],
        "5",
        hour(),
        |config| {
            config
                .with_borrow_collateral_ratio(base(), d("0.95"))
                .unwrap()
        },
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    feed(
        &mut venue,
        &instrument_name(),
        &[(1, "20000"), (2, "10000")],
    );

    assert_eq!(borrowable_now(&venue, Side::Buy), d("332000"));
}

#[test]
fn a_debt_is_valued_at_its_highest() {
    // 30,000 USDT held, 1 BTC owed: BTC's high of 12,000 makes the debt 12,000 against 18,000 of
    // equity, so 3x lends 24,000 more USDT. At its current 10,000, it would lend 30,000.
    let config = window_config(
        vec![holding(base(), "0", "1"), funded(quote(), d("30000"))],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    feed(
        &mut venue,
        &instrument_name(),
        &[(1, "12000"), (2, "10000")],
    );

    assert_eq!(borrowable_now(&venue, Side::Buy), d("24000"));
}

#[test]
fn a_bars_high_and_low_count() {
    // One observation at 9,000 carrying a bar from 7,000 to 11,000, with 1 BTC and 10,000 USDT
    // held at 3x.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), d("10000"))],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    let bar = MarketBar::new(d("9000"), d("11000"), d("7000"), d("9000"));
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded("9000")).with_bar(Some(bar)),
        time(1),
    );
    assert!(events.is_empty(), "{events:?}");

    // 1 BTC at 7,000 and 10,000 USDT: 17,000 of equity lends 34,000 USDT.
    assert_eq!(borrowable_now(&venue, Side::Buy), d("34000"));
    // 1 BTC, the asset borrowed, at 11,000 with 10,000 USDT: 21,000 lends 42,000, 42 / 11 BTC.
    assert_eq!(borrowable_now(&venue, Side::Sell), d("42000") / d("11000"));
}

/// USDTBTC: USDT priced in BTC, the inverse of BTCUSDT.
fn usdt_btc_instruments()
-> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    let name = InstrumentNameExchange::new("USDTBTC");
    let mut instruments = FnvHashMap::default();
    instruments.insert(
        name.clone(),
        Instrument {
            exchange: EXCHANGE,
            name_internal: InstrumentNameInternal::new("usdtbtc"),
            name_exchange: name,
            underlying: Underlying {
                base: quote(),
                quote: base(),
            },
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind: InstrumentKind::Spot,
            spec: None,
            data_venue: None,
        },
    );
    instruments
}

#[test]
fn on_an_inverse_pair_an_assets_lowest_is_the_inverse_of_the_instruments_highest() {
    // USDTBTC between 0.0001 and 0.000125 BTC, now 0.0001: BTC between 8,000 and 10,000 USDT, now
    // 10,000. 1 BTC held and not owed counts at 8,000, so 3x lends 16,000 USDT, where the current
    // price would lend 20,000. A sell of USDTBTC borrows its base, USDT.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, usdt_btc_instruments());
    let name = InstrumentNameExchange::new("USDTBTC");
    feed(
        &mut venue,
        &name,
        &[(1, "0.0001"), (2, "0.000125"), (3, "0.0001")],
    );

    let capacity = venue.borrow_capacity(&name, Side::Sell).unwrap();
    assert_eq!(capacity.asset, quote());
    assert_eq!(capacity.borrowable_now, Some(d("16000")));
}

#[test]
fn an_order_is_lent_no_more_than_the_window_allows() {
    // 1 BTC, low 8,000 in the window, now 9,000: 3x lends 16,000 USDT, so a buy of 1.9 BTC
    // (17,100) is refused and one of 1.7 (15,300) is not.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    feed(&mut venue, &instrument_name(), &[(1, "8000"), (2, "9000")]);

    let refused = venue.open_order(buy_request("1.9", None));
    assert!(
        insufficient_reason(&refused).contains("Borrowable: 16000"),
        "{}",
        insufficient_reason(&refused)
    );

    let bought = venue.open_order(buy_request("1.7", None));
    assert_eq!(filled_trade(&bought).quantity, d("1.7"));
}

#[test]
fn the_liquidation_check_values_the_account_at_current_prices() {
    // 1 BTC held against 8,000 USDT owed, liquidated at 1.1. One observation at 20,000 with a bar
    // low of 8,000: at the bar's low the margin level would be 1.0, but at the current 20,000 it
    // is 2.5, and the account is not liquidated.
    let config = window_config(
        vec![funded(base(), d("1")), holding(quote(), "0", "8000")],
        "3",
        hour(),
        |config| {
            config
                .with_liquidation(SpotMarginLiquidation::new(d("1.1"), Decimal::ZERO).unwrap())
                .unwrap()
        },
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    let bar = MarketBar::new(d("20000"), d("20000"), d("8000"), d("20000"));
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded("20000")).with_bar(Some(bar)),
        time(1),
    );

    assert!(
        !events.iter().any(|event| matches!(
            &event.kind,
            AccountEventKind::Notice(notice) if notice.kind == NoticeKind::Liquidation
        )),
        "{events:?}"
    );
    // The window still values what the venue lends: at 8,000 the BTC is no equity beyond the
    // debt, where at 20,000 it would lend 2 × 12,000 − 8,000 = 16,000.
    assert_eq!(borrowable_now(&venue, Side::Buy), Decimal::ZERO);
}

#[test]
fn a_request_priced_venue_lends_at_the_requests_own_price() {
    // No driver feeds this venue, so there is no window to look back on: the order's own
    // snapshot at 9,000 values 1 BTC at 9,000, and 3x lends 18,000 USDT.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new(&config, spot_instruments());
    advance(&mut venue, time(1));

    let refused = venue.open_order(buy_request("2.1", Some(traded("9000"))));
    assert!(
        insufficient_reason(&refused).contains("Borrowable: 18000"),
        "{}",
        insufficient_reason(&refused)
    );
}

#[test]
fn a_price_of_zero_is_not_recorded() {
    // A bar whose low is zero values nothing, as a market at zero does: the window's low is the
    // observation's own 9,000, and 3x lends 18,000 USDT against 1 BTC.
    let config = window_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    let bar = MarketBar::new(d("9000"), d("9000"), Decimal::ZERO, d("9000"));
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded("9000")).with_bar(Some(bar)),
        time(1),
    );
    assert!(events.is_empty(), "{events:?}");

    assert_eq!(borrowable_now(&venue, Side::Buy), d("18000"));
}

#[test]
fn on_an_inverse_pair_an_owed_assets_highest_is_the_inverse_of_the_instruments_lowest() {
    // USDTBTC between 0.0001 and 0.000125 BTC, now 0.000125: BTC between 8,000 and 10,000 USDT,
    // now 8,000. 1 BTC owed against 30,000 USDT counts at 10,000: equity 20,000 against a debt of
    // 10,000 lends 30,000 more USDT at 3x, where the current price would lend 36,000.
    let config = window_config(
        vec![holding(base(), "0", "1"), funded(quote(), d("30000"))],
        "3",
        hour(),
        |config| config,
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, usdt_btc_instruments());
    let name = InstrumentNameExchange::new("USDTBTC");
    feed(&mut venue, &name, &[(1, "0.0001"), (2, "0.000125")]);

    let capacity = venue.borrow_capacity(&name, Side::Sell).unwrap();
    assert_eq!(capacity.borrowable_now, Some(d("30000")));
}
