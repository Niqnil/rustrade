//! Options: premium paid in cash, a long position, the orders refused, and the venue's own
//! settlement at expiry.

use super::*;
use crate::{
    client::mock::TimeInForceRules,
    expiry::LongOptionExpiry,
    fee::PerContractFeeModel,
    order::{
        TimeInForceKind,
        request::{RequestCancel, RequestOpen},
    },
    short_option::{Collateral, ShortPutCollateral},
};
use rustrade_instrument::instrument::{
    kind::{
        InstrumentKindDiscriminant,
        option::{OptionContract, OptionExercise, OptionKind, OptionSettlement},
    },
    name::InstrumentNameInternal,
    quote::InstrumentQuoteAsset,
};

pub(super) const STRIKE: &str = "100";

pub(super) fn usd() -> AssetNameExchange {
    AssetNameExchange::new("usd")
}

pub(super) fn spy() -> AssetNameExchange {
    AssetNameExchange::new("spy")
}

pub(super) fn spy_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("SPY")
}

pub(super) fn option_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("SPY_C100")
}

/// The instant every option here expires.
pub(super) fn expiry() -> DateTime<Utc> {
    time(100)
}

pub(super) fn spy_instrument() -> Instrument<ExchangeId, AssetNameExchange> {
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
pub(super) fn option_instrument(
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

/// A request-priced venue listing the call alone. It lapses its longs: one that exercised could
/// never price the underlying, so it takes no option buy.
fn priced_venue(usd_balance: &str, fee_model: FeeModelConfig) -> SimulatedVenue {
    let mut config = config(usd_balance, fee_model);
    config.long_option_expiry = LongOptionExpiry::Lapse;
    let mut venue = SimulatedVenue::new(
        &config,
        instruments_of(option_instrument(
            OptionKind::Call,
            OptionSettlement::Physical,
        )),
    );
    advance(&mut venue, time(1));
    venue
}

/// A market-driven venue listing SPY and the option, with `configure` applied to its config.
pub(super) fn driven_venue(
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
pub(super) fn driven_instruments(
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

pub(super) fn option_request(
    side: Side,
    quantity: &str,
    market: Option<MarketSnapshot>,
) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
    request(option_name(), side, quantity, market)
}

pub(super) fn balance(venue: &SimulatedVenue, asset: AssetNameExchange) -> Balance {
    venue.balances(&[asset]).remove(0).balance
}

/// The option position the venue reports, `None` while flat.
pub(super) fn option_position(venue: &SimulatedVenue) -> Option<Decimal> {
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
pub(super) fn feed(venue: &mut SimulatedVenue, spy_price: &str, option: &str, at: DateTime<Utc>) {
    let _ = venue.apply_market(&spy_name(), MarketObservation::new(traded(spy_price)), at);
    let _ = venue.apply_market(&option_name(), MarketObservation::new(traded(option)), at);
}

/// Buys `quantity` of the option on a market-driven venue at `price`.
pub(super) fn buy(venue: &mut SimulatedVenue, quantity: &str) {
    let outcome = venue.open_order(option_request(Side::Buy, quantity, None));
    assert!(
        outcome.response.state.is_accepted(),
        "the buy should fill: {:?}",
        outcome.response.state
    );
}

pub(super) fn trades(
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
fn a_venue_ruled_to_take_only_day_and_gtc_options_refuses_an_option_ioc_and_takes_a_spot_one() {
    let mut config = config("100000", FeeModelConfig::default());
    config.long_option_expiry = LongOptionExpiry::Lapse;
    config.time_in_force = TimeInForceRules::new().with_accepted(
        InstrumentKindDiscriminant::Option,
        &[
            TimeInForceKind::GoodUntilEndOfDay,
            TimeInForceKind::GoodUntilCancelled,
        ],
    );
    let mut instruments = instruments_of(spy_instrument());
    let option = option_instrument(OptionKind::Call, OptionSettlement::Physical);
    instruments.insert(option.name_exchange.clone(), option);
    let mut venue = SimulatedVenue::new(&config, instruments);
    advance(&mut venue, time(1));
    let with = |mut request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
                time_in_force| {
        request.state.time_in_force = time_in_force;
        request
    };

    let refused = venue.open_order(with(
        option_request(Side::Buy, "1", market_prices("1.5")),
        TimeInForce::ImmediateOrCancel,
    ));
    assert!(refused.events.is_empty(), "{:?}", refused.events);
    match &refused.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(
            OrderError::UnsupportedTimeInForce {
                time_in_force: TimeInForce::ImmediateOrCancel,
                reason,
                ..
            },
        )) => assert!(
            reason.contains("GoodUntilCancelled, GoodUntilEndOfDay for option orders"),
            "{reason}"
        ),
        other => panic!("expected UnsupportedTimeInForce, got: {other:?}"),
    }
    assert_eq!(option_position(&venue), None);

    let day = venue.open_order(with(
        option_request(Side::Buy, "1", market_prices("1.5")),
        TimeInForce::GoodUntilEndOfDay,
    ));
    assert!(day.response.state.is_accepted(), "{:?}", day.response.state);
    let spot = venue.open_order(with(
        request(spy_name(), Side::Buy, "1", market_prices("400")),
        TimeInForce::ImmediateOrCancel,
    ));
    assert!(
        spot.response.state.is_accepted(),
        "{:?}",
        spot.response.state
    );
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

/// On a venue that lends, the shortfall of a put exercised without the shares is lent, as a
/// broker's margin loan: the base is borrowed, so its free balance stays at zero, the loan is owed
/// and counted as debt, and it accrues interest from then on like any other.
#[test]
fn an_exercised_put_without_the_shares_on_a_margin_venue_borrows_them() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        config.spot_margin =
            Some(SpotMarginConfig::new(usd(), d("3"), MarginEffect::Borrow).unwrap());
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    let events = venue.advance_time(expiry());

    assert_eq!(trades(&events).len(), 2, "{events:?}");
    let shares = balance(&venue, spy());
    assert_eq!((shares.total, shares.free), (Decimal::ZERO, Decimal::ZERO));
    assert_eq!(venue.account.owed(&spy()), d("100"));
    assert_eq!(
        venue.account.loans().collect::<Vec<_>>(),
        vec![(&spy(), d("100"))]
    );
    assert_eq!(
        balance(&venue, usd()).total,
        d("10000") - d("150") + d("10000")
    );
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
pub(super) fn limit(
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
    // A market-driven venue listing the call alone, so nothing prices its underlying.
    let mut exercising = SimulatedVenue::new_market_driven(
        &config("1000", FeeModelConfig::default()),
        instruments_of(option_instrument(
            OptionKind::Call,
            OptionSettlement::Physical,
        )),
    );
    advance(&mut exercising, time(1));
    let _ = exercising.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut exercising, "1");
    assert!(trades(&exercising.advance_time(time(200))).is_empty());
    assert_eq!(option_position(&exercising), Some(d("1")));

    // `priced_venue` lists the call alone too, and lapses.
    let mut lapsing = priced_venue("1000", FeeModelConfig::default());
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
fn a_snapshot_holding_a_cash_settled_call_short_is_refused() {
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
        VenueRestoreError::OptionShortUnsecurable(option_name())
    );
}

/// The short check refuses only a negative quantity: a flat option position is not a short.
#[test]
fn a_snapshot_holding_a_flat_option_position_is_not_refused_as_a_short() {
    let (kind, settlement) = (OptionKind::Call, OptionSettlement::Cash);
    let mut venue = driven_venue(kind, settlement, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let mut json = serde_json::to_value(venue.snapshot()).unwrap();
    json["positions"][0][1]["quantity"] = serde_json::json!("0");
    let restored = SimulatedVenue::from_snapshot(
        &config("10000", FeeModelConfig::default()),
        driven_instruments(kind, settlement),
        serde_json::from_value(json).unwrap(),
    );

    assert!(restored.is_ok(), "{:?}", restored.err());
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

/// A request-priced venue has no market feed, so under `Exercise` it refuses an option buy it could
/// never settle, but still sells a long it was seeded with.
#[test]
fn a_request_priced_venue_that_exercises_refuses_an_option_buy() {
    let mut config = config("1000", FeeModelConfig::default());
    config.initial_state.instruments.push(held_option());
    let mut venue = SimulatedVenue::new(
        &config,
        instruments_of(option_instrument(
            OptionKind::Call,
            OptionSettlement::Physical,
        )),
    );
    advance(&mut venue, time(1));

    let bought = venue.open_order(option_request(Side::Buy, "1", market_prices("1.5")));
    match &bought.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(reason),
        ))) => {
            assert!(reason.contains("LongOptionExpiry::Lapse"), "{reason}");
        }
        other => panic!("the buy must be refused: {other:?}"),
    }
    assert_eq!(option_position(&venue), Some(d("1")));
    assert_eq!(balance(&venue, usd()).total, d("1000"));

    let sold = venue.open_order(option_request(Side::Sell, "1", market_prices("2")));
    assert!(
        sold.response.state.is_accepted(),
        "{:?}",
        sold.response.state
    );
    assert_eq!(option_position(&venue), None);
}

/// A lapsing long already expired when the clock first moves needs no price: it lapses at zero
/// in that very move, though nothing ever prices its underlying.
#[test]
fn a_lapsing_option_expired_before_the_clock_first_moves_lapses_without_a_price() {
    let mut config = config("10000", FeeModelConfig::default());
    config.long_option_expiry = LongOptionExpiry::Lapse;
    config.initial_state.instruments.push(held_option());
    let mut venue = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Cash),
    );

    let events = venue.advance_time(time(150));

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price, trades[0].time_exchange),
        (TradeOrigin::Expiry, Decimal::ZERO, time(150))
    );
    assert_eq!(option_position(&venue), None);
}

// -------------------------------------------------------------------------------------------
// Secured shorts: collateral held while a short is open, released on buyback, spent or released
// at expiry.
// -------------------------------------------------------------------------------------------

/// Sets the account's opening balances to `usd` and `spy`.
pub(super) fn funds(usd_balance: &str, spy_balance: &str) -> impl FnOnce(&mut MockExecutionConfig) {
    let balances = vec![funded(usd(), d(usd_balance)), funded(spy(), d(spy_balance))];
    move |config| config.initial_state.balances = balances
}

/// Sells `quantity` of the option on a market-driven venue, asserting it fills.
fn sell(venue: &mut SimulatedVenue, quantity: &str) -> Vec<UnindexedAccountEvent> {
    let outcome = venue.open_order(option_request(Side::Sell, quantity, None));
    assert!(
        outcome.response.state.is_accepted(),
        "the sale should fill: {:?}",
        outcome.response.state
    );
    outcome.events
}

/// What [`SimulatedVenue::collateral_held`] reports for a short holding `amount` of `asset`.
fn collateral(asset: AssetNameExchange, amount: &str) -> Option<Collateral> {
    Some(Collateral {
        asset,
        amount: d(amount),
    })
}

fn totals(venue: &SimulatedVenue, asset: AssetNameExchange) -> (Decimal, Decimal) {
    let balance = balance(venue, asset);
    (balance.total, balance.free)
}

fn snapshots_of(events: &[UnindexedAccountEvent], asset: &AssetNameExchange) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                &event.kind,
                AccountEventKind::BalanceSnapshot(balance) if balance.0.asset == *asset
            )
        })
        .count()
}

#[test]
fn a_short_put_receives_its_premium_and_holds_its_strike() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));

    sell(&mut venue, "1");

    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(
        totals(&venue, usd()),
        (d("10150"), d("150")),
        "1.5 × 100 received, 100 × 100 held"
    );
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(usd(), "10000")
    );
}

#[test]
fn a_short_put_pays_its_fee_out_of_the_premium() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        funds("20000", "0")(config);
        config.fee_model = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
    });
    feed(&mut venue, "100", "1.5", time(2));

    sell(&mut venue, "2");

    assert_eq!(
        totals(&venue, usd()),
        (d("20000") + d("300") - d("1.3"), d("300") - d("1.3")),
        "2 × 1.5 × 100 less 2 × 0.65 received, 20000 held"
    );
}

#[test]
fn a_short_put_the_free_cash_does_not_cover_is_refused_under_the_strike_rule() {
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("9999", "0"),
    );
    feed(&mut venue, "100", "1.5", time(2));

    let outcome = venue.open_order(option_request(Side::Sell, "1", None));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(Some(asset), _)
            ))) if *asset == usd()
        ),
        "the premium does not count towards the strike: {:?}",
        outcome.response.state
    );
    assert_eq!(totals(&venue, usd()), (d("9999"), d("9999")));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn under_strike_less_premium_the_premium_counts_towards_the_strike() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        funds("9850", "0")(config);
        config.short_put_collateral = ShortPutCollateral::StrikeLessPremium;
    });
    feed(&mut venue, "100", "1.5", time(2));

    sell(&mut venue, "1");

    assert_eq!(totals(&venue, usd()), (d("10000"), Decimal::ZERO));
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(usd(), "10000")
    );

    let short = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        funds("9849", "0")(config);
        config.short_put_collateral = ShortPutCollateral::StrikeLessPremium;
    });
    let mut short = short;
    feed(&mut short, "100", "1.5", time(2));
    let refused = short.open_order(option_request(Side::Sell, "1", None));
    assert!(
        !refused.response.state.is_accepted(),
        "9849 and the 150 premium are short of 10000"
    );
}

#[test]
fn a_covered_call_holds_the_shares_it_would_deliver() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("0", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));

    let events = sell(&mut venue, "1");

    assert_eq!(totals(&venue, spy()), (d("100"), Decimal::ZERO));
    assert_eq!(totals(&venue, usd()), (d("150"), d("150")));
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(spy(), "100")
    );
    assert_eq!(snapshots_of(&events, &spy()), 1, "{events:?}");
    assert_eq!(snapshots_of(&events, &usd()), 1, "{events:?}");

    let spot_sale = venue.open_order(request(spy_name(), Side::Sell, "1", market_prices("100")));
    assert!(
        !spot_sale.response.state.is_accepted(),
        "the shares covering the call are held"
    );
}

#[test]
fn a_short_call_without_the_shares_is_refused() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("100000", "99"),
    );
    feed(&mut venue, "100", "1.5", time(2));

    let outcome = venue.open_order(option_request(Side::Sell, "1", None));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(Some(asset), _)
            ))) if *asset == spy()
        ),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(option_position(&venue), None);
}

#[test]
fn a_short_cash_settled_call_is_refused() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, funds("0", "1000"));
    feed(&mut venue, "100", "1.5", time(2));

    let outcome = venue.open_order(option_request(Side::Sell, "1", None));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(reason)
            ))) if reason.contains("cash-settled call")
        ),
        "{:?}",
        outcome.response.state
    );
}

#[test]
fn a_short_on_a_request_priced_venue_is_refused() {
    let mut venue = priced_venue("1000000", FeeModelConfig::default());

    let outcome = venue.open_order(option_request(Side::Sell, "1", market_prices("1.5")));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(reason)
            ))) if reason.contains("RequestPriced")
        ),
        "{:?}",
        outcome.response.state
    );
}

#[test]
fn buying_a_short_put_back_releases_its_strike() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "105", "1", time(3));

    buy(&mut venue, "1");

    assert_eq!(option_position(&venue), None);
    assert_eq!(
        totals(&venue, usd()),
        (d("10050"), d("10050")),
        "150 received, 100 paid back, the strike released"
    );
    assert_eq!(venue.collateral_held(&option_name()), None);
}

#[test]
fn buying_part_of_a_short_back_releases_its_share() {
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("20000", "0"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "2");
    feed(&mut venue, "105", "1", time(3));

    buy(&mut venue, "1");

    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(usd(), "10000")
    );
    assert_eq!(totals(&venue, usd()), (d("20200"), d("10200")));
}

#[test]
fn buying_a_covered_call_back_releases_the_shares_and_restates_them() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("1000", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "105", "6", time(3));

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert!(outcome.response.state.is_accepted());
    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
    assert_eq!(totals(&venue, usd()), (d("550"), d("550")));
    assert_eq!(
        snapshots_of(&outcome.events, &spy()),
        1,
        "{:?}",
        outcome.events
    );
}

#[test]
fn a_buyback_the_account_cannot_pay_for_keeps_the_collateral_held() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("0", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "130", "31", time(3));

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert!(
        !outcome.response.state.is_accepted(),
        "3100 to pay, 150 free"
    );
    assert_eq!(totals(&venue, spy()), (d("100"), Decimal::ZERO));
    assert_eq!(option_position(&venue), Some(d("-1")));
}

/// As above, but with `free` charged below zero while the short is open, as a holding cost the
/// account cannot cover leaves it. Holding the collateral again after the refused buyback must not
/// be refused for the shortfall: the buyback is refused and the collateral stays held.
#[test]
fn a_refused_buyback_with_free_below_zero_keeps_the_collateral_held() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    assert_eq!(totals(&venue, usd()), (d("10150"), d("150")));

    let _ = venue
        .account
        .charge(&usd(), d("-10100"), DateTime::<Utc>::MIN_UTC);
    assert_eq!(
        totals(&venue, usd()),
        (d("50"), d("-9950")),
        "the charge overdraws free"
    );
    feed(&mut venue, "100", "1", time(3));

    let outcome = venue.open_order(option_request(Side::Buy, "1", None));

    assert!(
        matches!(
            &outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(Some(asset), _)
            ))) if *asset == usd()
        ),
        "100 to pay, 50 in the account: {:?}",
        outcome.response.state
    );
    assert!(outcome.events.is_empty(), "{:?}", outcome.events);
    assert_eq!(
        totals(&venue, usd()),
        (d("50"), d("-9950")),
        "the collateral is held again on top of the overdrawn free"
    );
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(usd(), "10000")
    );
    assert_eq!(option_position(&venue), Some(d("-1")));
}

#[test]
fn a_resting_sale_to_open_reserves_its_collateral_until_it_fills() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));

    let rested = venue.open_order(limit(option_name(), Side::Sell, "2", "1", "ask"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(totals(&venue, usd()), (d("10000"), Decimal::ZERO));
    assert_eq!(option_position(&venue), None);

    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("2.5")),
        time(3),
    );

    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(
        totals(&venue, usd()),
        (d("10200"), d("200")),
        "filled at its limit of 2"
    );
}

#[test]
fn cancelling_a_resting_sale_to_open_releases_its_collateral() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("0", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    let rested = venue.open_order(limit(option_name(), Side::Sell, "2", "1", "ask"));
    assert!(rested.response.state.is_accepted());
    assert_eq!(totals(&venue, spy()), (d("100"), Decimal::ZERO));

    let cancelled = venue.cancel_order(OrderRequestCancel {
        key: limit(option_name(), Side::Sell, "2", "1", "ask").key,
        state: RequestCancel { id: None },
    });
    assert!(cancelled.response.state.is_ok());

    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
}

#[test]
fn under_strike_less_premium_a_resting_sale_holds_the_strike_less_its_premium_at_the_limit() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        funds("9850", "0")(config);
        config.short_put_collateral = ShortPutCollateral::StrikeLessPremium;
    });
    feed(&mut venue, "100", "1", time(2));

    let rested = venue.open_order(limit(option_name(), Side::Sell, "1.5", "1", "ask"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(totals(&venue, usd()), (d("9850"), Decimal::ZERO));

    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.6")),
        time(3),
    );

    assert_eq!(totals(&venue, usd()), (d("10000"), Decimal::ZERO));
}

#[test]
fn a_resting_buyback_of_a_short_put_holds_nothing_its_release_pays_for() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");

    let rested = venue.open_order(limit(option_name(), Side::Buy, "1", "1", "bid"));

    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(
        totals(&venue, usd()),
        (d("10150"), d("150")),
        "the strike it will release pays its 100"
    );
}

#[test]
fn a_sale_beyond_the_long_closes_it_and_opens_a_short() {
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("30000", "0"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    sell(&mut venue, "3");

    assert_eq!(option_position(&venue), Some(d("-2")));
    assert_eq!(
        totals(&venue, usd()),
        (d("30000") - d("150") + d("450"), d("30300") - d("20000"))
    );
}

#[test]
fn a_call_sale_filled_in_part_that_closes_a_long_and_rests_a_short_holds_the_shares() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        funds("1000", "100")(config);
        config.fee_model = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(book("1.5", "1.6"))
            .with_depth(MarketDepth::new(Some(d("1")), Some(d("1")))),
        time(3),
    );

    let outcome = venue.open_order(limit(option_name(), Side::Sell, "1.5", "2", "ask"));

    assert!(
        outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(option_position(&venue), None, "the fill closed the long");
    assert_eq!(
        totals(&venue, spy()),
        (d("100"), Decimal::ZERO),
        "the remainder would open a short, so it holds the shares, and the fill's fee is not \
         charged in them"
    );
    assert_eq!(
        totals(&venue, usd()),
        (d("998.70"), d("998.70")),
        "150 + 0.65 paid for the long, 150 − 0.65 received for closing it"
    );
}

#[test]
fn a_covered_call_receives_its_premium_less_the_fee() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        funds("0", "100")(config);
        config.fee_model = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
    });
    feed(&mut venue, "100", "1.5", time(2));

    sell(&mut venue, "1");

    assert_eq!(totals(&venue, usd()), (d("149.35"), d("149.35")));
    assert_eq!(totals(&venue, spy()), (d("100"), Decimal::ZERO));
}

#[test]
fn a_sale_closing_a_long_call_moves_no_shares() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("1000", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let events = sell(&mut venue, "1");

    assert_eq!(snapshots_of(&events, &spy()), 0, "{events:?}");
    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
    assert_eq!(venue.collateral_held(&option_name()), None);
}

#[test]
fn a_put_sale_filled_in_part_holds_the_strike_for_both_the_fill_and_the_remainder() {
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("30000", "0"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(book("1.5", "1.6"))
            .with_depth(MarketDepth::new(Some(d("1")), Some(d("1")))),
        time(3),
    );

    let outcome = venue.open_order(limit(option_name(), Side::Sell, "1.5", "2", "ask"));

    assert!(
        outcome.response.state.is_accepted(),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(
        totals(&venue, usd()),
        (d("30150"), d("10150")),
        "150 received; 10000 held by the short, 10000 by the resting sale"
    );
}

#[test]
fn a_resting_sale_behind_one_that_closes_the_long_holds_the_shares_of_the_short_it_opens() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("1000", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    let closing = venue.open_order(limit(option_name(), Side::Sell, "2", "1", "ask"));
    assert!(
        closing.response.state.is_accepted(),
        "{:?}",
        closing.response.state
    );
    assert_eq!(
        totals(&venue, spy()),
        (d("100"), d("100")),
        "it only closes the long"
    );

    let opening = venue.open_order(limit(option_name(), Side::Sell, "2", "1", "ask2"));
    assert!(
        opening.response.state.is_accepted(),
        "{:?}",
        opening.response.state
    );
    assert_eq!(
        totals(&venue, spy()),
        (d("100"), Decimal::ZERO),
        "once the first fills, this one opens a short, so it holds the shares from now"
    );

    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("2.1")),
        time(3),
    );

    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(totals(&venue, spy()), (d("100"), Decimal::ZERO));
    assert_eq!(
        venue.collateral_held(&option_name()),
        collateral(spy(), "100")
    );
}

#[test]
fn a_resting_buy_behind_one_that_buys_the_short_back_holds_the_premium_of_the_long_it_opens() {
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("10200", "0"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    assert_eq!(totals(&venue, usd()), (d("10350"), d("350")));

    let buyback = venue.open_order(limit(option_name(), Side::Buy, "1", "1", "bid"));
    assert!(
        buyback.response.state.is_accepted(),
        "{:?}",
        buyback.response.state
    );
    assert_eq!(
        totals(&venue, usd()),
        (d("10350"), d("350")),
        "the strike it releases pays for it"
    );

    let opening = venue.open_order(limit(option_name(), Side::Buy, "1", "1", "bid2"));
    assert!(
        opening.response.state.is_accepted(),
        "{:?}",
        opening.response.state
    );
    assert_eq!(
        totals(&venue, usd()),
        (d("10350"), d("250")),
        "once the first fills, this one opens a long, so it holds its 100 premium"
    );
}

#[test]
fn a_resting_covered_call_whose_fee_exceeds_its_premium_is_cancelled_at_its_fill_without_the_quote()
{
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        funds("0", "100")(config);
        config.fee_model = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("2"),
        });
    });
    feed(&mut venue, "100", "0.005", time(2));

    let rested = venue.open_order(limit(option_name(), Side::Sell, "0.01", "1", "ask"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(
        totals(&venue, spy()),
        (d("100"), Decimal::ZERO),
        "it holds the shares, and not the 1 of quote its fee exceeds its premium by"
    );

    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("0.02")),
        time(3),
    );

    assert!(venue.orders_open(&[]).is_empty());
    assert_eq!(option_position(&venue), None);
    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
    assert_eq!(totals(&venue, usd()), (Decimal::ZERO, Decimal::ZERO));
}

#[test]
fn an_out_of_the_money_short_put_expires_and_releases_its_strike() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "120", "0.01", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].side, trades[0].price, trades[0].origin),
        (Side::Buy, Decimal::ZERO, TradeOrigin::Expiry)
    );
    assert_eq!(totals(&venue, usd()), (d("10150"), d("10150")));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn an_assigned_short_put_buys_the_shares_with_the_strike_it_held() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 2, "{events:?}");
    assert_eq!(
        (trades[0].side, trades[0].price, trades[0].origin),
        (Side::Buy, Decimal::ZERO, TradeOrigin::Assignment)
    );
    assert_eq!(
        (
            trades[1].instrument.clone(),
            trades[1].side,
            trades[1].price,
            trades[1].quantity,
            trades[1].origin
        ),
        (
            spy_name(),
            Side::Buy,
            d(STRIKE),
            d("100"),
            TradeOrigin::Assignment
        )
    );
    assert_eq!(totals(&venue, usd()), (d("150"), d("150")));
    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
    assert_eq!(option_position(&venue), None);
}

#[test]
fn an_assigned_covered_call_delivers_the_shares_it_held() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("0", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(
        (trades[1].side, trades[1].price, trades[1].quantity),
        (Side::Sell, d(STRIKE), d("100"))
    );
    assert_eq!(totals(&venue, spy()), (Decimal::ZERO, Decimal::ZERO));
    assert_eq!(totals(&venue, usd()), (d("10150"), d("10150")));
}

#[test]
fn an_out_of_the_money_covered_call_expires_and_releases_the_shares() {
    let mut venue = driven_venue(
        OptionKind::Call,
        OptionSettlement::Physical,
        funds("0", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "90", "0.01", time(50));

    let _ = venue.advance_time(expiry());

    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
}

#[test]
fn an_assigned_cash_settled_put_pays_its_intrinsic_value_out_of_the_strike_it_held() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Cash, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    let events = venue.advance_time(expiry());

    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "{events:?}");
    assert_eq!(
        (trades[0].side, trades[0].price, trades[0].origin),
        (Side::Buy, d("20"), TradeOrigin::Assignment)
    );
    assert_eq!(totals(&venue, usd()), (d("8150"), d("8150")));
}

#[test]
fn a_short_is_assigned_even_when_longs_lapse() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        config.long_option_expiry = LongOptionExpiry::Lapse;
    });
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    let _ = venue.advance_time(expiry());

    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
}

#[test]
fn a_short_without_an_underlying_price_waits_for_one_holding_its_collateral() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |config| {
        config.long_option_expiry = LongOptionExpiry::Lapse;
    });
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    sell(&mut venue, "1");

    let events = venue.advance_time(expiry());

    assert!(trades(&events).is_empty(), "{events:?}");
    assert_eq!(option_position(&venue), Some(d("-1")));
    assert_eq!(totals(&venue, usd()), (d("10150"), d("150")));

    let events = venue.apply_market(&spy_name(), MarketObservation::new(traded("80")), time(101));

    assert_eq!(trades(&events).len(), 2, "{events:?}");
    assert_eq!(totals(&venue, spy()), (d("100"), d("100")));
}

/// One option contract held short since `initial_state`, sold at 1.50.
fn held_short() -> InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>
{
    let mut held = held_option();
    held.position = PositionReport::Open(Position::new(
        d("-1"),
        Some(d("1.5")),
        None,
        None,
        None,
        None,
        None,
        time(0),
    ));
    held
}

#[test]
fn a_seeded_short_holds_its_collateral_out_of_the_seeded_balance() {
    let mut config = config("10500", FeeModelConfig::default());
    config.initial_state.instruments.push(held_short());

    let venue = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Put, OptionSettlement::Physical),
    );

    assert_eq!(totals(&venue, usd()), (d("10500"), d("500")));
    assert_eq!(option_position(&venue), Some(d("-1")));
}

#[test]
#[should_panic(expected = "does not cover")]
fn a_seeded_short_the_seeded_balance_does_not_cover_panics() {
    let mut config = config("9999", FeeModelConfig::default());
    config.initial_state.instruments.push(held_short());

    let _ = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Put, OptionSettlement::Physical),
    );
}

#[test]
#[should_panic(expected = "cash-settled call")]
fn a_seeded_short_cash_settled_call_panics() {
    let mut config = config("1000000", FeeModelConfig::default());
    config.initial_state.instruments.push(held_short());

    let _ = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Cash),
    );
}

#[test]
#[should_panic(expected = "holds no balance of")]
fn a_seeded_short_without_a_balance_of_its_collateral_asset_panics() {
    let mut config = config_from_balances(vec![funded(spy(), d("100"))], FeeModelConfig::default());
    config.initial_state.instruments.push(held_short());

    let _ = SimulatedVenue::new_market_driven(
        &config,
        driven_instruments(OptionKind::Put, OptionSettlement::Physical),
    );
}

/// A buy is measured as if the buys already resting filled first, so one that closes only what
/// they would not is refused on a request-priced venue that exercises, as opening a long. Such a
/// venue rests no order of its own; this one is seeded.
#[test]
fn a_request_priced_venue_that_exercises_counts_resting_buys_against_the_short() {
    let mut config = config("10000", FeeModelConfig::default());
    config.initial_state.instruments.push(held_short());
    let mut venue = SimulatedVenue::new(
        &config,
        instruments_of(option_instrument(
            OptionKind::Put,
            OptionSettlement::Physical,
        )),
    );
    venue.account = AccountState::new(
        venue
            .account
            .balances()
            .cloned()
            .map(|balance| (balance.asset.clone(), balance))
            .collect(),
        {
            let mut book = OpenOrders::default();
            book.insert(RestingOrder::new(
                Order {
                    key: OrderKey {
                        exchange: EXCHANGE,
                        instrument: option_name(),
                        strategy: StrategyId::new("test"),
                        cid: ClientOrderId::new("resting"),
                    },
                    side: Side::Buy,
                    price: Some(d("0.5")),
                    quantity: d("1"),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open {
                        id: VenueOrderId::Assigned(OrderId::new("resting")),
                        time_exchange: time(0),
                        filled_quantity: Decimal::ZERO,
                    },
                },
                None,
            ))
            .unwrap();
            book
        },
        Default::default(),
        Vec::new(),
    );
    advance(&mut venue, time(1));

    let bought = venue.open_order(option_request(Side::Buy, "1", market_prices("1.5")));

    match &bought.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(reason),
        ))) => {
            assert!(reason.contains("LongOptionExpiry::Lapse"), "{reason}");
        }
        other => panic!("the resting buy already buys the short back: {other:?}"),
    }
    assert_eq!(option_position(&venue), Some(d("-1")));
}

#[test]
fn a_restored_short_keeps_its_collateral_and_is_assigned_at_expiry() {
    let (kind, settlement) = (OptionKind::Put, OptionSettlement::Physical);
    let mut venue = driven_venue(kind, settlement, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");

    let mut restored = restored(&venue, kind, settlement);

    assert_eq!(totals(&restored, usd()), (d("10150"), d("150")));
    feed(&mut restored, "80", "20", time(50));
    let _ = restored.advance_time(expiry());
    assert_eq!(totals(&restored, spy()), (d("100"), d("100")));
    assert_eq!(totals(&restored, usd()), (d("150"), d("150")));
}

#[test]
fn a_snapshot_whose_balance_does_not_hold_the_collateral_is_refused() {
    let (kind, settlement) = (OptionKind::Put, OptionSettlement::Physical);
    let mut venue = driven_venue(kind, settlement, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");

    let mut json = serde_json::to_value(venue.snapshot()).unwrap();
    for balance in json["account"]["balances"].as_array_mut().unwrap() {
        if balance["asset"] == "usd" {
            balance["balance"]["free"] = balance["balance"]["total"].clone();
        }
    }
    let refused = SimulatedVenue::from_snapshot(
        &config("10000", FeeModelConfig::default()),
        driven_instruments(kind, settlement),
        serde_json::from_value(json).unwrap(),
    );

    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::OptionCollateralNotHeld {
            asset: usd(),
            required: d("10000"),
            held: Decimal::ZERO,
        }
    );
}

#[test]
fn a_snapshot_whose_balance_holds_the_collateral_only_with_an_orders_reservation_is_refused() {
    let (kind, settlement) = (OptionKind::Put, OptionSettlement::Physical);
    let mut venue = driven_venue(kind, settlement, funds("20000", "0"));
    feed(&mut venue, "100", "1.5", time(2));
    sell(&mut venue, "1");
    let rested = venue.open_order(limit(spy_name(), Side::Buy, "90", "1", "bid"));
    assert!(
        rested.response.state.is_accepted(),
        "{:?}",
        rested.response.state
    );
    assert_eq!(totals(&venue, usd()), (d("20150"), d("10060")));

    // One unit more free: total − free covers the collateral only by counting the 90 the
    // resting buy reserves.
    let mut json = serde_json::to_value(venue.snapshot()).unwrap();
    for balance in json["account"]["balances"].as_array_mut().unwrap() {
        if balance["asset"] == "usd" {
            balance["balance"]["free"] = serde_json::to_value(d("10061")).unwrap();
        }
    }
    let refused = SimulatedVenue::from_snapshot(
        &config("20000", FeeModelConfig::default()),
        driven_instruments(kind, settlement),
        serde_json::from_value(json).unwrap(),
    );

    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::OptionCollateralNotHeld {
            asset: usd(),
            required: d("10000"),
            held: d("9999"),
        }
    );
}

#[test]
fn an_exercised_call_without_the_cash_on_a_margin_venue_borrows_it_and_charges_interest() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        funds("150", "0")(config);
        config.spot_margin = Some(
            SpotMarginConfig::new(usd(), d("3"), MarginEffect::Borrow)
                .unwrap()
                .with_max_borrow(usd(), d("1"))
                .unwrap(),
        );
    })
    .with_margin_interest(Arc::new(super::FlatAtBorrow));
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    feed(&mut venue, "120", "20", time(50));

    let events = venue.advance_time(expiry());

    assert_eq!(
        venue.account.loans().collect::<Vec<_>>(),
        vec![(&usd(), d("10000"))],
        "the whole shortfall, past the cap of 1: a delivery cannot be refused"
    );
    let flows: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::CashFlow(flow) => Some(flow),
            _ => None,
        })
        .collect();
    assert_eq!(flows.len(), 1, "{events:?}");
    assert_eq!(flows[0].instrument, Some(spy_name()));
    assert!(
        matches!(
            events.last().map(|event| &event.kind),
            Some(AccountEventKind::CashFlow(_))
        ),
        "the interest follows the delivery's trade: {events:?}"
    );
}

// -------------------------------------------------------------------------------------------
// Amends: refused for an option order.
// -------------------------------------------------------------------------------------------

#[test]
fn an_option_orders_amend_is_refused_and_changes_nothing() {
    use crate::order::{
        amend::{AmendPatch, AmendTarget, RequestAmend},
        request::OrderRequestAmend,
    };

    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    let rested = venue.open_order(limit(option_name(), Side::Buy, "1", "1", "bid"));
    assert!(rested.response.state.is_accepted());
    let usd_before = totals(&venue, usd());
    let order = venue
        .account
        .orders()
        .get(&ClientOrderId::new("bid"))
        .cloned()
        .unwrap();

    let outcome = venue.amend_order(OrderRequestAmend {
        key: order.key.clone(),
        state: RequestAmend::new(
            order.state.id.clone(),
            AmendPatch::default().with_price(d("1.2")),
            AmendTarget {
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                reduce_only: None,
            },
            Decimal::ZERO,
        ),
    });

    assert!(outcome.events.is_empty());
    assert!(
        matches!(
            outcome.response.state,
            Err(OrderError::AmendRefused(AmendRefusal::Venue(_)))
        ),
        "{:?}",
        outcome.response.state
    );
    assert_eq!(totals(&venue, usd()), usd_before);
    assert_eq!(
        venue.account.orders().get(&ClientOrderId::new("bid")),
        Some(&order)
    );
}

/// What a resting sell closes is measured behind the other resting sells, leaving out the one
/// named: an order costed while it is still on the book does not count against itself.
#[test]
fn the_resting_sells_an_order_is_measured_behind_leave_out_the_one_named() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "2");
    let rested = venue.open_order(limit(option_name(), Side::Sell, "5", "1", "ask"));
    assert!(rested.response.state.is_accepted());
    let terms = venue.instrument_terms(&option_name()).unwrap();
    let position = venue.net_position(&terms, &option_name());
    assert_eq!(position.quantity, d("2"));

    let behind_all = venue.behind_resting(&terms, &option_name(), Side::Sell, position, None);
    let behind_others = venue.behind_resting(
        &terms,
        &option_name(),
        Side::Sell,
        position,
        Some(&ClientOrderId::new("ask")),
    );

    assert_eq!(behind_all.quantity, d("1"), "the resting sell of 1 counted");
    assert_eq!(behind_others.quantity, d("2"), "and left out");
}
