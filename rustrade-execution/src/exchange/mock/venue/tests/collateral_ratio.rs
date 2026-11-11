//! Collateral ratios: what part of an asset's value counts as equity when the venue lends, and
//! when it checks the account for liquidation.

use super::*;
use crate::{
    margin::{SpotMarginConfigError, SpotMarginLiquidation},
    notice::NoticeKind,
};

/// A margin account valued in USDT, holding `balances`, lending up to `max_leverage`, with the
/// ratios and liquidation `configure` adds.
fn ratio_config(
    balances: Vec<AssetBalance<AssetNameExchange>>,
    max_leverage: &str,
    configure: impl FnOnce(SpotMarginConfig) -> Result<SpotMarginConfig, SpotMarginConfigError>,
) -> MockExecutionConfig {
    let spot_margin =
        SpotMarginConfig::new(quote(), d(max_leverage), MarginEffect::Borrow).unwrap();
    config_from_balances(balances, FeeModelConfig::default())
        .with_spot_margin(configure(spot_margin).unwrap())
}

/// A fed venue trading BTCUSDT at `btc` and ETHUSDT at `eth` from `time(1)`.
fn fed_venue(config: &MockExecutionConfig, btc: &str, eth: &str) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(config, btc_and_eth_instruments());
    advance(&mut venue, time(1));
    for (instrument, price) in [(instrument_name(), btc), (eth_instrument_name(), eth)] {
        assert!(
            venue
                .apply_market(&instrument, MarketObservation::new(traded(price)), time(1))
                .is_empty()
        );
    }
    venue
}

/// How much more of BTC (`Side::Sell`) or USDT (`Side::Buy`) the venue would lend now.
fn borrowable_now(venue: &SimulatedVenue, side: Side) -> Decimal {
    venue
        .borrow_capacity(&instrument_name(), side)
        .unwrap()
        .borrowable_now
        .unwrap()
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

#[test]
fn binances_worked_max_borrow_example_is_reproduced() {
    // Binance's example: 0.1 BTC and 1 ETH at 5x, BTC counted at 99.9 % and ETH at 95 %, BTC
    // (the debt token) at 80,500 and ETH at 2,180, lends at most 0.5025 BTC.
    let config = ratio_config(
        vec![
            funded(base(), d("0.1")),
            funded(eth(), d("1")),
            funded(quote(), Decimal::ZERO),
        ],
        "5",
        |config| {
            config
                .with_borrow_collateral_ratio(base(), d("0.999"))?
                .with_borrow_collateral_ratio(eth(), d("0.95"))
        },
    );
    let venue = fed_venue(&config, "80500", "2180");

    let lent = borrowable_now(&venue, Side::Sell);

    assert_eq!(lent, d("4") * (d("8041.95") + d("2071")) / d("80500"));
    assert_eq!(lent.round_dp(4), d("0.5025"));
}

#[test]
fn only_what_an_asset_holds_beyond_its_debt_is_weighted() {
    // 2 BTC held, 1 owed, at 10,000: the owed BTC counts in full, the other at 0.8. Equity 8,000
    // at 3x lends 16,000 against the 10,000 owed: 6,000 more. Weighting all 2 BTC would leave
    // 2,000, and weighting none 10,000.
    let config = ratio_config(
        vec![holding(base(), "2", "1"), funded(quote(), Decimal::ZERO)],
        "3",
        |config| config.with_borrow_collateral_ratio(base(), d("0.8")),
    );
    let venue = fed_venue(&config, "10000", "1000");

    assert_eq!(borrowable_now(&venue, Side::Buy), d("6000"));
}

#[test]
fn an_asset_owed_beyond_its_holdings_is_not_weighted() {
    // 0.5 BTC held against 1 owed, and 20,000 USDT: equity 15,000 at 3x lends 30,000 against the
    // 10,000 owed, whatever BTC's ratio. Weighting its negative net would count it for less debt.
    let config = ratio_config(
        vec![holding(base(), "0.5", "1"), funded(quote(), d("20000"))],
        "3",
        |config| config.with_borrow_collateral_ratio(base(), d("0.1")),
    );
    let venue = fed_venue(&config, "10000", "1000");

    assert_eq!(borrowable_now(&venue, Side::Buy), d("20000"));
}

#[test]
fn an_order_needing_more_than_the_weighted_equity_allows_is_refused() {
    // 1 BTC at 10,000 counted at 0.5: 5,000 of equity at 2x lends 5,000 USDT.
    let config = ratio_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "2",
        |config| config.with_borrow_collateral_ratio(base(), d("0.5")),
    );
    let mut venue = fed_venue(&config, "10000", "1000");

    let refused = venue.open_order(buy_request("0.6", None));
    assert!(
        insufficient_reason(&refused).contains("Borrowable: 5000"),
        "{}",
        insufficient_reason(&refused)
    );

    let bought = venue.open_order(buy_request("0.5", None));
    assert_eq!(filled_trade(&bought).quantity, d("0.5"));
}

#[test]
fn a_ratio_of_zero_lends_nothing_against_the_asset() {
    let config = ratio_config(
        vec![funded(base(), d("1")), funded(quote(), Decimal::ZERO)],
        "3",
        |config| config.with_borrow_collateral_ratio(base(), Decimal::ZERO),
    );
    let venue = fed_venue(&config, "10000", "1000");

    assert_eq!(borrowable_now(&venue, Side::Buy), Decimal::ZERO);
}

/// 1 BTC held against 8,000 USDT owed, at 3x, liquidated at 1.1, with BTC's borrow ratio at 0.5
/// and its liquidation ratio at `liquidation_ratio`.
fn owing_8000_usdt(liquidation_ratio: &str) -> MockExecutionConfig {
    ratio_config(
        vec![funded(base(), d("1")), holding(quote(), "0", "8000")],
        "3",
        |config| {
            config
                .with_borrow_collateral_ratio(base(), d("0.5"))?
                .with_liquidation_collateral_ratio(base(), d(liquidation_ratio))?
                .with_liquidation(SpotMarginLiquidation::new(d("1.1"), Decimal::ZERO).unwrap())
        },
    )
}

/// Whether the events include the notice that starts a liquidation.
fn liquidated(events: &[UnindexedAccountEvent]) -> bool {
    events.iter().any(|event| {
        matches!(&event.kind, AccountEventKind::Notice(notice) if notice.kind == NoticeKind::Liquidation)
    })
}

#[test]
fn liquidation_weighs_collateral_by_the_liquidation_ratio_not_the_borrow_ratio() {
    // 10,000 of BTC against 8,000 owed: a margin level of 1.25 at full value, above 1.1. The
    // borrow ratio of 0.5 would put it at 0.625, so it does not count.
    let config = owing_8000_usdt("1");
    // Priced before the clock first moves, which would otherwise find BTC unvalued.
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded("10000")),
        time(1),
    );
    assert!(!liquidated(&events), "{events:?}");

    // At 0.85, the BTC counts for 8,500: a margin level of 1.0625, at or below 1.1.
    let config = owing_8000_usdt("0.85");
    // Priced before the clock first moves, which would otherwise find BTC unvalued.
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    let events = venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded("10000")),
        time(1),
    );
    assert!(liquidated(&events), "{events:?}");
}
