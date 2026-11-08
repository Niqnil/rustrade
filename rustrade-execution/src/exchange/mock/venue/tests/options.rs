//! Options: premium paid in cash, a long position, the orders refused, and the venue's own
//! settlement at expiry.

use super::*;
use crate::{expiry::LongOptionExpiry, fee::PerContractFeeModel, order::request::RequestOpen};
use rustrade_instrument::instrument::{
    kind::option::{OptionContract, OptionExercise, OptionKind, OptionSettlement},
    name::InstrumentNameInternal,
    quote::InstrumentQuoteAsset,
};

const STRIKE: &str = "100";

fn usd() -> AssetNameExchange {
    AssetNameExchange::new("usd")
}

fn spy() -> AssetNameExchange {
    AssetNameExchange::new("spy")
}

fn spy_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("SPY")
}

fn option_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("SPY_C100")
}

/// The instant every option here expires.
fn expiry() -> DateTime<Utc> {
    time(100)
}

fn spy_instrument() -> Instrument<ExchangeId, AssetNameExchange> {
    Instrument {
        exchange: EXCHANGE,
        name_internal: InstrumentNameInternal::new("spy"),
        name_exchange: spy_name(),
        underlying: Underlying {
            base: spy(),
            quote: usd(),
        },
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    }
}

/// A 100-share option on SPY struck at [`STRIKE`], expiring at [`expiry`].
fn option_instrument(
    kind: OptionKind,
    settlement: OptionSettlement,
) -> Instrument<ExchangeId, AssetNameExchange> {
    Instrument {
        exchange: EXCHANGE,
        name_internal: InstrumentNameInternal::new("spy_c100"),
        name_exchange: option_name(),
        underlying: Underlying {
            base: spy(),
            quote: usd(),
        },
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Option(OptionContract {
            contract_size: d("100"),
            settlement_asset: usd(),
            kind,
            exercise: OptionExercise::American,
            settlement,
            expiry: expiry(),
            strike: d(STRIKE),
        }),
        spec: None,
        data_venue: None,
    }
}

fn config(usd_balance: &str, fee_model: FeeModelConfig) -> MockExecutionConfig {
    config_from_balances(
        vec![funded(usd(), d(usd_balance)), funded(spy(), Decimal::ZERO)],
        fee_model,
    )
}

/// A request-priced venue listing the call alone.
fn priced_venue(usd_balance: &str, fee_model: FeeModelConfig) -> SimulatedVenue {
    let mut venue = SimulatedVenue::new(
        &config(usd_balance, fee_model),
        instruments_of(option_instrument(
            OptionKind::Call,
            OptionSettlement::Physical,
        )),
    );
    advance(&mut venue, time(1));
    venue
}

/// A market-driven venue listing SPY and the option, with `configure` applied to its config.
fn driven_venue(
    kind: OptionKind,
    settlement: OptionSettlement,
    configure: impl FnOnce(&mut MockExecutionConfig),
) -> SimulatedVenue {
    let mut config = config("10000", FeeModelConfig::default());
    configure(&mut config);
    let mut venue =
        SimulatedVenue::new_market_driven(&config, driven_instruments(kind, settlement));
    advance(&mut venue, time(1));
    venue
}

/// SPY and the option, as [`driven_venue`] lists them.
fn driven_instruments(
    kind: OptionKind,
    settlement: OptionSettlement,
) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    let mut instruments = instruments_of(spy_instrument());
    let option = option_instrument(kind, settlement);
    instruments.insert(option.name_exchange.clone(), option);
    instruments
}

/// `venue` restored from its snapshot, written to JSON and read back, with [`driven_venue`]'s
/// default config and instruments.
fn restored(
    venue: &SimulatedVenue,
    kind: OptionKind,
    settlement: OptionSettlement,
) -> SimulatedVenue {
    let snapshot =
        serde_json::from_str(&serde_json::to_string(&venue.snapshot()).unwrap()).unwrap();
    SimulatedVenue::from_snapshot(
        &config("10000", FeeModelConfig::default()),
        driven_instruments(kind, settlement),
        snapshot,
    )
    .expect("a venue's own snapshot restores")
}

fn option_request(
    side: Side,
    quantity: &str,
    market: Option<MarketSnapshot>,
) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
    request(option_name(), side, quantity, market)
}

fn balance(venue: &SimulatedVenue, asset: AssetNameExchange) -> Balance {
    venue.balances(&[asset]).remove(0).balance
}

/// The option position the venue reports, `None` while flat.
fn option_position(venue: &SimulatedVenue) -> Option<Decimal> {
    let snapshot = venue.account_snapshot();
    let instrument = snapshot
        .instruments
        .iter()
        .find(|instrument| instrument.instrument == option_name())
        .expect("an option is always in the snapshot, flat or not");
    match &instrument.position {
        PositionReport::Open(position) => Some(position.quantity),
        PositionReport::Flat => None,
        PositionReport::Unreported => panic!("an option position is reported, not Unreported"),
    }
}

/// Feeds SPY at `price` at `at`, and the option at `option` (for its fills).
fn feed(venue: &mut SimulatedVenue, spy_price: &str, option: &str, at: DateTime<Utc>) {
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded(spy_price)), at);
    let _ = venue.apply_market(&option_name(), MarketObservation::new(traded(option)), at);
}

/// Buys `quantity` of the option on a market-driven venue at `price`.
fn buy(venue: &mut SimulatedVenue, quantity: &str) {
    let outcome = venue.open_order(option_request(Side::Buy, quantity, None));
    assert!(
        outcome.response.state.is_accepted(),
        "the buy should fill: {:?}",
        outcome.response.state
    );
}

fn trades(
    events: &[UnindexedAccountEvent],
) -> Vec<&Trade<AssetNameExchange, InstrumentNameExchange>> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::Trade(trade) => Some(trade),
            _ => None,
        })
        .collect()
}

#[test]
fn a_buy_pays_the_premium_times_the_contract_size_and_a_per_contract_fee() {
    let fee = FeeModelConfig::PerContract(PerContractFeeModel {
        commission_per_contract: d("0.65"),
    });
    let mut venue = priced_venue("1000", fee);

    let outcome = venue.open_order(option_request(Side::Buy, "2", market_prices("1.5")));

    assert!(
        outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    // 2 contracts × 1.50 × 100 = 300 of premium, and 2 × 0.65 of fee.
    let paid = filled_balance(&outcome);
    assert_eq!(paid.asset, usd());
    assert_eq!(paid.balance.total, d("1000") - d("300") - d("1.30"));
    assert_eq!(paid.balance.free, paid.balance.total);
    assert_eq!(filled_trade(&outcome).fees.fees, d("1.30"));
    assert_eq!(option_position(&venue), Some(d("2")));
}

#[test]
fn a_buy_the_free_cash_cannot_pay_for_is_refused() {
    let mut venue = priced_venue("299", FeeModelConfig::default());

    let outcome = venue.open_order(option_request(Side::Buy, "2", market_prices("1.5")));

    assert!(
        !outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(balance(&venue, usd()).free, d("299"));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn a_sale_closes_the_long_and_receives_the_premium() {
    let mut venue = priced_venue("1000", FeeModelConfig::default());
    let _ = venue.open_order(option_request(Side::Buy, "2", market_prices("1.5")));

    let outcome = venue.open_order(option_request(Side::Sell, "2", market_prices("2.5")));

    assert!(
        outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    // Paid 300, received 2 × 2.50 × 100 = 500.
    assert_eq!(balance(&venue, usd()).total, d("1200"));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn a_sale_beyond_the_long_held_is_refused() {
    let mut venue = priced_venue("1000", FeeModelConfig::default());

    let flat = venue.open_order(option_request(Side::Sell, "1", market_prices("1.5")));
    assert!(
        !flat.response.state.is_accepted(),
        "selling short must be refused"
    );

    let _ = venue.open_order(option_request(Side::Buy, "2", market_prices("1.5")));
    let beyond = venue.open_order(option_request(Side::Sell, "3", market_prices("1.5")));
    assert!(
        !beyond.response.state.is_accepted(),
        "selling 3 of 2 must be refused"
    );
    assert_eq!(option_position(&venue), Some(d("2")));
}

#[test]
fn resting_sales_count_against_what_may_still_be_sold() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "2");

    let first = venue.open_order(OrderRequestOpen {
        key: OrderKey {
            cid: ClientOrderId::new("first"),
            ..option_request(Side::Sell, "2", None).key
        },
        state: RequestOpen::new(Side::Sell, Some(d("5")), d("2"), OrderKind::Limit, gtc()),
    });
    assert!(
        first.response.state.is_accepted(),
        "{:?}",
        first.response.state
    );

    let second = venue.open_order(OrderRequestOpen {
        key: OrderKey {
            cid: ClientOrderId::new("second"),
            ..option_request(Side::Sell, "1", None).key
        },
        state: RequestOpen::new(Side::Sell, Some(d("5")), d("1"), OrderKind::Limit, gtc()),
    });
    assert!(
        !second.response.state.is_accepted(),
        "both sales filling would sell 3 of 2: {:?}",
        second.response.state
    );
}

#[test]
fn an_order_at_or_after_expiry_is_refused() {
    let mut venue = priced_venue("1000", FeeModelConfig::default());
    let _ = venue.advance_time(expiry());

    let outcome = venue.open_order(option_request(Side::Buy, "1", market_prices("1.5")));

    assert!(
        !outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
}

#[test]
fn a_zero_price_never_fills_an_option() {
    let mut venue = priced_venue("1000", FeeModelConfig::default());

    let outcome = venue.open_order(option_request(Side::Buy, "1", market_prices("0")));

    assert!(
        !outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(balance(&venue, usd()).total, d("1000"));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn a_zero_option_quote_is_no_price() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    let _ = venue.apply_market(&option_name(), MarketObservation::new(traded("0")), time(3));

    // A zero says nothing traded: the option has no price now, rather than the stale 1.50.
    assert_eq!(
        venue.market(&option_name()).unwrap().snapshot.last_price,
        None
    );
}

#[test]
fn the_next_deadline_includes_a_listed_option_expiry() {
    let venue = priced_venue("1000", FeeModelConfig::default());

    assert_eq!(venue.next_deadline(), Some(expiry()));
}

#[test]
fn an_out_of_the_money_option_expires_at_zero() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "2");
    feed(&mut venue, "99", "0.5", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    let trade = trades[0];
    assert_eq!(trade.instrument, option_name());
    assert_eq!(trade.origin, TradeOrigin::Expiry);
    assert_eq!(
        (trade.side, trade.price, trade.quantity),
        (Side::Sell, d("0"), d("2"))
    );
    assert_eq!(trade.time_exchange, expiry());
    assert_eq!(trade.fees.fees, Decimal::ZERO);
    assert_eq!(option_position(&venue), None);
    assert_eq!(balance(&venue, usd()).total, d("10000") - d("300"));
}

#[test]
fn a_lapsing_long_expires_at_zero_in_the_money() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        config.long_option_expiry = LongOptionExpiry::Lapse;
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price),
        (TradeOrigin::Expiry, d("0"))
    );
    assert_eq!(
        balance(&venue, spy()).total,
        Decimal::ZERO,
        "nothing is delivered"
    );
}

#[test]
fn an_exercised_call_buys_the_underlying_at_the_strike() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 2, "{events:?}");
    let (option, spot) = (trades[0], trades[1]);
    assert_eq!(option.instrument, option_name());
    assert_eq!(
        (option.origin, option.price, option.quantity),
        (TradeOrigin::Exercise, d("0"), d("1"))
    );
    assert_eq!(spot.instrument, spy_name());
    assert_eq!(
        (spot.origin, spot.side, spot.price, spot.quantity),
        (TradeOrigin::Exercise, Side::Buy, d(STRIKE), d("100"))
    );
    // 10,000 less 150 of premium, less 100 shares at 100.
    assert_eq!(
        balance(&venue, usd()).total,
        d("10000") - d("150") - d("10000")
    );
    assert_eq!(balance(&venue, spy()).total, d("100"));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn an_exercised_put_without_the_shares_takes_the_underlying_below_zero() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    let events = venue.advance_time(expiry());

    let spot = trades(&events)[1];
    assert_eq!(
        (spot.side, spot.price, spot.quantity),
        (Side::Sell, d(STRIKE), d("100"))
    );
    assert_eq!(
        balance(&venue, usd()).total,
        d("10000") - d("150") + d("10000")
    );
    let shares = balance(&venue, spy());
    assert_eq!((shares.total, shares.free), (d("-100"), d("-100")));
}

#[test]
fn a_cash_settled_option_pays_its_intrinsic_value() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price, trades[0].quantity),
        (TradeOrigin::Exercise, d("20"), d("1"))
    );
    // 10,000 less 150 of premium, plus 20 × 100 of intrinsic value.
    assert_eq!(
        balance(&venue, usd()).total,
        d("10000") - d("150") + d("2000")
    );
    assert_eq!(balance(&venue, spy()).total, Decimal::ZERO);
}

#[test]
fn settlement_values_the_underlying_as_it_stood_before_the_jump() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    // The tick that carries the clock past expiry is after it, so it does not price it.
    let events = venue.apply_market(
        &spy_name(),
        MarketObservation::new(traded("150")),
        time(200),
    );

    let trades = trades(&events);
    assert_eq!(
        trades[0].price,
        d("20"),
        "settled at 120, not 150: {events:?}"
    );
    assert_eq!(trades[0].time_exchange, expiry());
}

#[test]
fn an_expiry_without_an_underlying_price_waits_for_one() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");

    let at_expiry = venue.advance_time(expiry());
    assert!(
        trades(&at_expiry).is_empty(),
        "no price, so nothing settles: {at_expiry:?}"
    );
    assert_eq!(option_position(&venue), Some(d("1")));

    let priced = venue.apply_market(
        &spy_name(),
        MarketObservation::new(traded("110")),
        time(150),
    );

    let trades = trades(&priced);
    assert_eq!(trades.len(), 1, "{priced:?}");
    assert_eq!(
        (trades[0].price, trades[0].time_exchange),
        (d("10"), time(150))
    );
    assert_eq!(option_position(&venue), None);
}

#[test]
fn expiry_retires_the_orders_still_working_on_the_option() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    let rested = venue.open_order(OrderRequestOpen {
        key: OrderKey {
            cid: ClientOrderId::new("resting"),
            ..option_request(Side::Buy, "1", None).key
        },
        state: RequestOpen::new(Side::Buy, Some(d("1")), d("1"), OrderKind::Limit, gtc()),
    });
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(balance(&venue, usd()).free, d("10000") - d("100"));

    let events = venue.advance_time(expiry());

    let retired = events
        .iter()
        .find_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order) => Some(&order.0),
            _ => None,
        })
        .expect("the resting order is retired");
    assert!(
        matches!(
            retired.state,
            UnindexedOrderState::Inactive(InactiveOrderState::Expired(_))
        ),
        "{:?}",
        retired.state
    );
    assert_eq!(
        balance(&venue, usd()).free,
        d("10000"),
        "its hold is released"
    );
}

#[test]
fn an_option_position_is_never_charged_a_holding_cost() {
    #[derive(Debug)]
    struct EverySecond;
    impl HoldingCostModel for EverySecond {
        fn next_boundary(
            &self,
            _: &InstrumentNameExchange,
            after: DateTime<Utc>,
        ) -> Option<DateTime<Utc>> {
            Some(after + chrono::TimeDelta::seconds(1))
        }
        fn charge(
            &self,
            _: &HeldPosition<'_>,
            _: DateTime<Utc>,
        ) -> Option<crate::holding_cost::HoldingCharge> {
            Some(crate::holding_cost::HoldingCharge::new(
                CashFlowKind::Funding {
                    rate: None,
                    position_quantity: None,
                },
                d("-1"),
            ))
        }
    }

    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {})
        .with_holding_costs(vec![Arc::new(EverySecond)]);
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let events = venue.advance_time(time(20));

    assert!(
        !events
            .iter()
            .any(|event| matches!(event.kind, AccountEventKind::CashFlow(_))),
        "{events:?}"
    );
}

#[test]
fn a_restored_venue_settles_a_held_option_at_its_expiry() {
    let (kind, settlement) = (OptionKind::Call, OptionSettlement::Cash);
    let mut original = driven_venue(kind, settlement, |_| {});
    feed(&mut original, "100", "1.5", time(2));
    buy(&mut original, "1");

    let mut restored = restored(&original, kind, settlement);
    assert_eq!(restored.next_deadline(), original.next_deadline());
    assert_eq!(restored.next_deadline(), Some(expiry()));

    // The market is not restored, so both are fed the same underlying price before expiry.
    for venue in [&mut original, &mut restored] {
        let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded("110")), time(50));
    }
    let settled = original.advance_time(expiry());
    assert_eq!(trades(&settled).len(), 1, "{settled:?}");
    assert_eq!(restored.advance_time(expiry()), settled);
    assert_eq!(option_position(&restored), None);
}

#[test]
fn a_restored_venue_still_waits_to_settle_an_expiry_it_had_no_price_for() {
    let (kind, settlement) = (OptionKind::Call, OptionSettlement::Cash);
    let mut original = driven_venue(kind, settlement, |_| {});
    let _ = original.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut original, "1");
    assert!(trades(&original.advance_time(expiry())).is_empty());

    let mut restored = restored(&original, kind, settlement);
    assert_eq!(option_position(&restored), Some(d("1")));

    let price = |venue: &mut SimulatedVenue| {
        venue.apply_market(
            &spy_name(),
            MarketObservation::new(traded("110")),
            time(150),
        )
    };
    let settled = price(&mut original);
    assert_eq!(trades(&settled).len(), 1, "{settled:?}");
    assert_eq!(price(&mut restored), settled);
    assert_eq!(option_position(&restored), None);
}

/// One option contract held long since `initial_state`, bought at 1.50.
fn held_option() -> InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>
{
    InstrumentAccountSnapshot {
        instrument: option_name(),
        orders: Vec::new(),
        orders_complete: false,
        position: PositionReport::Open(Position::new(
            d("1"),
            Some(d("1.5")),
            None,
            None,
            None,
            None,
            None,
            time(0),
        )),
        isolated: None,
    }
}

/// A good-till-cancelled limit order on `instrument`, identified by `cid`.
fn limit(
    instrument: InstrumentNameExchange,
    side: Side,
    price: &str,
    quantity: &str,
    cid: &str,
) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
    OrderRequestOpen {
        key: OrderKey {
            cid: ClientOrderId::new(cid),
            ..request(instrument, side, quantity, None).key
        },
        state: RequestOpen::new(side, Some(d(price)), d(quantity), OrderKind::Limit, gtc()),
    }
}

#[test]
fn a_resting_buy_does_not_count_against_what_may_be_sold() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    let rested = venue.open_order(limit(option_name(), Side::Buy, "0.5", "1", "bid"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );

    let sale = venue.open_order(limit(option_name(), Side::Sell, "5", "1", "ask"));

    assert!(
        sale.response.state.is_accepted(),
        "a resting buy closes nothing: {:?}",
        sale.response.state
    );
}

#[test]
fn a_cash_settled_put_pays_its_intrinsic_value_on_every_contract() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Cash, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "2");
    feed(&mut venue, "90", "10", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price, trades[0].quantity),
        (TradeOrigin::Exercise, d("10"), d("2"))
    );
    // 10,000 less 2 × 1.50 × 100 of premium, plus 10 × 2 × 100 of intrinsic value.
    assert_eq!(
        balance(&venue, usd()).total,
        d("10000") - d("300") + d("2000")
    );
}

#[test]
fn an_option_exactly_at_the_money_expires_at_zero() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price),
        (TradeOrigin::Expiry, Decimal::ZERO)
    );
    assert_eq!(
        balance(&venue, spy()).total,
        Decimal::ZERO,
        "nothing delivered"
    );
}

#[test]
fn a_zero_underlying_price_is_no_price_to_settle_at() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    feed(&mut venue, "0", "1.5", time(2));
    buy(&mut venue, "1");

    let events = venue.advance_time(expiry());

    assert!(trades(&events).is_empty(), "{events:?}");
    assert_eq!(option_position(&venue), Some(d("1")));
}

/// An option held from `initial_state` that has already expired when the clock first moves
/// settles at the first price for its underlying, and only its own orders are retired: an order
/// resting on the underlying is left working.
#[test]
fn an_option_expired_before_the_clock_first_moves_settles_at_the_first_price() {
    let mut config = config("10000", FeeModelConfig::default());
    config.initial_state.instruments.push(held_option());
    let mut venue = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Cash),
    );

    assert!(trades(&venue.advance_time(time(150))).is_empty());
    let rested = venue.open_order(limit(spy_name(), Side::Buy, "1", "1", "spy-bid"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );

    let priced = venue.apply_market(
        &spy_name(),
        MarketObservation::new(traded("110")),
        time(160),
    );

    let trades = trades(&priced);
    assert_eq!(trades.len(), 1, "{priced:?}");
    assert_eq!(
        (trades[0].price, trades[0].time_exchange),
        (d("10"), time(160))
    );
    assert_eq!(option_position(&venue), None);
    assert_eq!(
        venue
            .account
            .orders_open()
            .map(|order| order.key.cid.clone())
            .collect::<Vec<_>>(),
        [ClientOrderId::new("spy-bid")],
        "the order on the underlying is still working"
    );
}

#[test]
fn an_option_observation_keeps_only_the_prices_it_can_trade_at() {
    let bar = |low: &str| Some(MarketBar::new(d("2"), d("3"), d(low), d("2.5")));
    let observed = |snapshot: MarketSnapshot, bar: Option<MarketBar>, fresh: bool| {
        let mut observation = MarketObservation::new(snapshot).with_bar(bar);
        observation.fresh = fresh;
        priced_only(observation)
    };
    let none = MarketSnapshot::default();

    assert_eq!(
        observed(none, bar("1"), true).bar,
        bar("1"),
        "a positive bar is kept"
    );
    assert_eq!(
        observed(none, bar("0"), true).bar,
        None,
        "a bar touching zero is dropped"
    );
    assert_eq!(observed(none, bar("-1"), true).bar, None);

    // Each price source alone keeps an observation fresh; none at all does not.
    let quoted = |bid: Option<&str>, ask: Option<&str>, last: Option<&str>| MarketSnapshot {
        best_bid: bid.map(d),
        best_ask: ask.map(d),
        last_price: last.map(d),
    };
    assert!(observed(none, bar("1"), true).fresh, "a bar alone");
    assert!(
        observed(quoted(Some("1"), None, None), None, true).fresh,
        "a bid alone"
    );
    assert!(
        observed(quoted(None, Some("1"), None), None, true).fresh,
        "an ask alone"
    );
    assert!(
        observed(quoted(None, None, Some("1")), None, true).fresh,
        "a last price alone"
    );
    assert!(!observed(quoted(Some("0"), Some("0"), Some("0")), bar("0"), true).fresh);
    assert!(
        !observed(quoted(Some("1"), Some("1"), Some("1")), bar("1"), false).fresh,
        "a stale observation stays stale"
    );
}

#[test]
fn an_option_without_a_spot_instrument_for_its_underlying_settles_only_by_lapsing() {
    // `priced_venue` lists the call alone, so nothing prices its underlying.
    let mut exercising = priced_venue("1000", FeeModelConfig::default());
    let _ = exercising.open_order(option_request(Side::Buy, "1", market_prices("1.5")));
    assert!(trades(&exercising.advance_time(time(200))).is_empty());
    assert_eq!(option_position(&exercising), Some(d("1")));

    let mut config = config("1000", FeeModelConfig::default());
    config.long_option_expiry = LongOptionExpiry::Lapse;
    let mut lapsing = SimulatedVenue::new(
        &config,
        instruments_of(option_instrument(
            OptionKind::Call,
            OptionSettlement::Physical,
        )),
    );
    advance(&mut lapsing, time(1));
    let _ = lapsing.open_order(option_request(Side::Buy, "1", market_prices("1.5")));
    let events = lapsing.advance_time(time(200));
    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price, trades[0].time_exchange),
        (TradeOrigin::Expiry, Decimal::ZERO, expiry())
    );
}

/// A long held since `initial_state` on an account listing no quote balance is paid its
/// settlement into a quote balance opened at zero.
#[test]
fn a_seeded_long_without_a_quote_balance_is_paid_into_a_new_one() {
    let mut config = config_from_balances(vec![], FeeModelConfig::default());
    config.initial_state.instruments.push(held_option());
    let mut venue = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Cash),
    );
    advance(&mut venue, time(1));
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded("110")), time(50));

    let events = venue.advance_time(expiry());

    assert_eq!(trades(&events).len(), 1, "{events:?}");
    assert_eq!(balance(&venue, usd()).total, d("1000"), "10 × 1 × 100");
}

#[test]
fn a_snapshot_holding_an_option_short_is_refused() {
    let (kind, settlement) = (OptionKind::Call, OptionSettlement::Cash);
    let mut venue = driven_venue(kind, settlement, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let mut json = serde_json::to_value(venue.snapshot()).unwrap();
    json["positions"][0][1]["quantity"] = serde_json::json!("-1");
    let refused = SimulatedVenue::from_snapshot(
        &config("10000", FeeModelConfig::default()),
        driven_instruments(kind, settlement),
        serde_json::from_value(json).unwrap(),
    );

    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::OptionShort(option_name())
    );
}

/// A market input can carry the clock past an expiry before the driver's next `advance_time`, and
/// an order placed then is refused all the same.
#[test]
fn an_order_after_a_market_input_crossed_the_expiry_is_refused() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(150),
    );
    assert!(
        venue.time_exchange() < expiry(),
        "only the market moved past it"
    );

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert!(
        matches!(
            outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::InstrumentInvalid(..)
            )))
        ),
        "{:?}",
        outcome.response.state
    );
}
