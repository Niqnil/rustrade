//! Liquidation of a spot margin account, and the margin-call notices before it.

use super::*;
use crate::{
    margin::{SpotMarginConfigError, SpotMarginLiquidation},
    notice::{AccountNotice, NoticeKind},
};

/// Liquidation at a margin level of `level` or below, charging `fee_rate` of the debt repaid.
fn liquidation(level: &str, fee_rate: &str) -> SpotMarginLiquidation {
    SpotMarginLiquidation::new(d(level), d(fee_rate)).unwrap()
}

/// A margin account valued in USDT, lending up to 3x, liquidated as `liquidation` says.
fn liquidating_config(
    balances: Vec<AssetBalance<AssetNameExchange>>,
    liquidation: SpotMarginLiquidation,
) -> MockExecutionConfig {
    MockExecutionConfig {
        spot_margin: Some(
            SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow)
                .unwrap()
                .with_liquidation(liquidation)
                .unwrap(),
        ),
        ..config_from_balances(balances, FeeModelConfig::default())
    }
}

/// A fed venue trading BTC at `price` from `time(1)`, holding `balances`.
fn liquidating_venue(
    balances: Vec<AssetBalance<AssetNameExchange>>,
    liquidation: SpotMarginLiquidation,
    price: &str,
) -> SimulatedVenue {
    let config = liquidating_config(balances, liquidation);
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, price, 1).is_empty());
    venue
}

/// A venue holding 10,000 USDT that has sold 1 BTC it borrowed at 10,000: 20,000 USDT held
/// against 1 BTC owed, a margin level of 2 at that price.
fn short_one_btc(liquidation: SpotMarginLiquidation) -> SimulatedVenue {
    let mut venue = liquidating_venue(
        vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
        liquidation,
        "10000",
    );
    let outcome = venue.open_order(sell_request("1", None));
    assert_eq!(filled_trade(&outcome).quantity, d("1"));
    venue
}

/// A venue holding 10,000 USDT that has bought 2 BTC at 10,000, borrowing 10,000 USDT: 2 BTC held
/// against 10,000 USDT owed, a margin level of 2 at that price.
fn long_two_btc(liquidation: SpotMarginLiquidation) -> SimulatedVenue {
    let mut venue = liquidating_venue(
        vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
        liquidation,
        "10000",
    );
    let outcome = venue.open_order(buy_request("2", None));
    assert_eq!(filled_trade(&outcome).quantity, d("2"));
    venue
}

/// A trade at `price`, and nothing else, observed at `time(seconds)`.
fn tick(venue: &mut SimulatedVenue, price: &str, seconds: i64) -> Vec<UnindexedAccountEvent> {
    venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded(price)),
        time(seconds),
    )
}

/// What each event is, in a word and the asset or kind it names, so a test can pin the order.
fn shape(events: &[UnindexedAccountEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| match &event.kind {
            AccountEventKind::BalanceSnapshot(balance) => format!("balance {}", balance.0.asset),
            AccountEventKind::MarginDebt(update) => format!("debt {}", update.asset),
            AccountEventKind::Trade(trade) => format!("trade {}", trade.side),
            AccountEventKind::OrderSnapshot(order) => format!("order {}", order.0.key.cid),
            AccountEventKind::Notice(notice) => format!("notice {}", notice.kind),
            AccountEventKind::CashFlow(_) => "cash flow".to_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

fn notice_of(event: &UnindexedAccountEvent) -> &AccountNotice<InstrumentNameExchange> {
    match &event.kind {
        AccountEventKind::Notice(notice) => notice,
        other => panic!("expected a notice, got: {other:?}"),
    }
}

fn notices(events: &[UnindexedAccountEvent]) -> Vec<NoticeKind> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::Notice(notice) => Some(notice.kind),
            _ => None,
        })
        .collect()
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

/// The snapshot `events` report of the order `cid`.
fn order_named<'a>(
    events: &'a [UnindexedAccountEvent],
    cid: &str,
) -> &'a Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
    events
        .iter()
        .find_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order)
                if order.0.key.cid == ClientOrderId::new(cid) =>
            {
                Some(&order.0)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no snapshot of order {cid} in {:?}", shape(events)))
}

fn assert_cancelled(order: &Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>) {
    assert!(
        matches!(
            order.state,
            OrderState::Inactive(InactiveOrderState::Cancelled(_))
        ),
        "{}: {:?}",
        order.key.cid,
        order.state
    );
}

/// Asserts `outcome` was refused for want of `asset`, with nothing borrowable.
fn assert_refused_a_loan(outcome: &OpenOutcome, asset: AssetNameExchange) {
    match &outcome.response.state {
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::BalanceInsufficient(Some(refused), reason),
        ))) => {
            assert_eq!(*refused, asset);
            assert!(reason.contains("Borrowable: 0"), "{reason}");
        }
        other => panic!("expected BalanceInsufficient, got: {other:?}"),
    }
}

fn liquidation_fee(
    event: &UnindexedAccountEvent,
) -> &CashFlow<AssetNameExchange, InstrumentNameExchange> {
    match &event.kind {
        AccountEventKind::CashFlow(flow) => flow,
        other => panic!("expected a cash flow, got: {other:?}"),
    }
}

#[test]
fn an_account_above_its_liquidation_level_is_left_alone() {
    let mut venue = short_one_btc(liquidation("1.1", "0.02"));

    // 20,000 held against 18,000 owed: a margin level of 1.11.
    assert!(tick(&mut venue, "18000", 2).is_empty());
    assert_eq!(balance_now(&venue, base()).margin.unwrap().borrowed, d("1"));
}

#[test]
fn a_short_at_its_liquidation_level_is_bought_back_and_charged_the_fee_on_what_it_owed() {
    let mut venue = short_one_btc(liquidation("1.1", "0.02"));

    // 20,000 held against 18,200 owed: a margin level of 1.0989.
    let events = tick(&mut venue, "18200", 2);

    assert_eq!(
        shape(&events),
        [
            "notice liquidation",
            "balance USDT",
            "balance BTC",
            "debt BTC",
            "trade buy",
            "balance USDT",
            "cash flow",
        ]
    );
    let notice = notice_of(&events[0]);
    assert_eq!(notice.margin_level, Some(d("20000") / d("18200")));
    assert_eq!(notice.instrument, None);
    assert_eq!(notice.status, "liquidation");
    assert_eq!(notice.time_exchange, time(2));

    let trade = trade_of(&events[4]);
    assert_eq!(
        (trade.side, trade.price, trade.quantity),
        (Side::Buy, d("18200"), d("1"))
    );
    assert_eq!(trade.origin, TradeOrigin::Liquidation);
    assert_eq!(trade.client_order_id, None);
    assert_eq!(trade.strategy, StrategyId::unknown());
    assert_eq!(trade.time_exchange, time(2));

    let fee = liquidation_fee(&events[6]);
    assert_eq!(
        fee.kind,
        CashFlowKind::LiquidationFee {
            rate: Some(d("0.02")),
            repaid: Some(d("18200")),
        }
    );
    assert_eq!((fee.asset.clone(), fee.amount), (quote(), d("-364")));
    assert_eq!(fee.instrument, None);

    assert_eq!(
        balance_now(&venue, base()),
        Balance::new_margin(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    );
    // 20,000 less the 18,200 buy and the 364 fee.
    assert_eq!(
        balance_now(&venue, quote()),
        Balance::new_margin(d("1436"), d("1436"), Decimal::ZERO, Decimal::ZERO)
    );
    assert!(
        tick(&mut venue, "30000", 3).is_empty(),
        "an account that owes nothing is never liquidated again"
    );
}

#[test]
fn a_long_borrowing_the_valuation_asset_sells_only_what_repays_the_loan_and_the_fee() {
    let mut venue = long_two_btc(liquidation("1.1", "0.02"));

    // 2 BTC at 5,500 against 10,000 USDT owed: a margin level of 1.1.
    let events = tick(&mut venue, "5500", 2);

    let sold = trades(&events);
    assert_eq!(
        sold.len(),
        2,
        "one sale repays the loan, another raises the fee"
    );
    assert!(
        sold.iter()
            .all(|trade| trade.side == Side::Sell && trade.origin == TradeOrigin::Liquidation)
    );
    assert_eq!(sold[0].quantity, d("10000") / d("5500"));
    assert_eq!(sold[1].quantity, d("200") / d("5500"));

    let usdt = balance_now(&venue, quote());
    assert_eq!(usdt.margin.unwrap(), MarginDetails::default());
    assert!(
        usdt.free >= Decimal::ZERO && usdt.free < d("0.000000000000000000001"),
        "the sales raise the loan and the fee and no more: {usdt:?}"
    );
    let btc = balance_now(&venue, base()).total;
    assert!(
        (btc - (d("2") - d("10200") / d("5500"))).abs() < d("0.000000000000000000001"),
        "{btc}"
    );
}

#[test]
fn every_open_order_is_cancelled_before_the_forced_trade() {
    let mut venue = short_one_btc(liquidation("1.1", "0"));
    let rested = venue.open_order(gtc_limit("resting", Side::Buy, "0.1", "9000"));
    rested_open(&rested);

    let events = tick(&mut venue, "18200", 2);

    assert_eq!(
        shape(&events),
        [
            "notice liquidation",
            "balance USDT",
            "order resting",
            "balance USDT",
            "balance BTC",
            "debt BTC",
            "trade buy",
        ],
        "the released hold, then the cancel, then the buy; a zero fee charges nothing"
    );
    assert!(venue.account.orders().is_empty());
}

#[test]
fn a_margin_call_is_sent_once_per_crossing_and_ends_without_a_notice_at_a_liquidation() {
    let mut venue = short_one_btc(
        liquidation("1.1", "0.02")
            .with_margin_call(d("1.3"))
            .unwrap(),
    );

    // 20,000 against 15,500: 1.29, under the margin-call level.
    let called = tick(&mut venue, "15500", 2);
    assert_eq!(notices(&called), [NoticeKind::MarginCall]);
    assert_eq!(
        notice_of(&called[0]).margin_level,
        Some(d("20000") / d("15500"))
    );
    assert_eq!(notice_of(&called[0]).status, "margin call");
    assert!(
        tick(&mut venue, "15600", 3).is_empty(),
        "still called: no second notice"
    );

    // 20,000 against 15,000: 1.33, back above it.
    let restored = tick(&mut venue, "15000", 4);
    assert_eq!(notices(&restored), [NoticeKind::MarginRestored]);
    assert!(tick(&mut venue, "14000", 5).is_empty());

    assert_eq!(
        notices(&tick(&mut venue, "15500", 6)),
        [NoticeKind::MarginCall]
    );
    assert_eq!(
        notices(&tick(&mut venue, "18200", 7)),
        [NoticeKind::Liquidation],
        "a liquidation ends the margin call without restoring it"
    );
    assert!(
        notices(&tick(&mut venue, "1000", 8)).is_empty(),
        "an account that owes nothing after its liquidation is not restored either"
    );
}

#[test]
fn a_margin_call_is_restored_when_the_account_repays_everything_it_owes() {
    let mut venue = short_one_btc(
        liquidation("1.1", "0.02")
            .with_margin_call(d("1.3"))
            .unwrap(),
    );
    assert_eq!(
        notices(&tick(&mut venue, "15500", 2)),
        [NoticeKind::MarginCall]
    );

    let repaid = venue.open_order(with_effect(buy_request("1", None), MarginEffect::Repay));
    assert_eq!(filled_trade(&repaid).quantity, d("1"));

    let events = venue.advance_time(time(3));
    assert_eq!(notices(&events), [NoticeKind::MarginRestored]);
    assert_eq!(notice_of(&events[0]).margin_level, None);
}

#[test]
fn a_debt_the_account_cannot_cover_is_left_owed_and_the_liquidation_continues_without_a_notice() {
    let mut venue = short_one_btc(liquidation("1.1", "0.02"));

    // 20,000 buys back only 0.8 of the BTC owed at 25,000.
    let (events, warned) = warnings(|| tick(&mut venue, "25000", 2));

    assert_eq!(trades(&events)[0].quantity, d("0.8"));
    assert_eq!(
        balance_now(&venue, base()).margin.unwrap().borrowed,
        d("0.2")
    );
    // The fee on the 20,000 repaid finds nothing left to pay it with.
    assert_eq!(
        balance_now(&venue, quote()),
        Balance::new_margin(d("-400"), d("-400"), Decimal::ZERO, Decimal::ZERO)
    );

    let shortfall = |warned: &[String]| {
        warned
            .iter()
            .filter(|message| message.contains("could not repay every debt"))
            .count()
    };
    assert_eq!(shortfall(&warned), 1, "{warned:?}");

    let (events, warned) = warnings(|| tick(&mut venue, "25000", 3));
    assert!(
        events.is_empty(),
        "nothing more can be repaid, and the liquidation under way sends no second notice"
    );
    assert_eq!(
        shortfall(&warned),
        0,
        "warned once per liquidation: {warned:?}"
    );
}

#[test]
fn a_debt_the_book_is_too_thin_to_close_is_closed_by_later_inputs() {
    let mut venue = short_one_btc(liquidation("1.1", "0.02"));
    let thin = |venue: &mut SimulatedVenue, seconds| {
        venue.apply_market(
            &instrument_name(),
            MarketObservation::new(traded("18200"))
                .with_depth(MarketDepth::new(Some(d("0.6")), Some(d("0.6")))),
            time(seconds),
        )
    };

    let first = thin(&mut venue, 2);
    assert_eq!(notices(&first), [NoticeKind::Liquidation]);
    assert_eq!(trades(&first)[0].quantity, d("0.6"));
    assert_eq!(
        balance_now(&venue, base()).margin.unwrap().borrowed,
        d("0.4")
    );

    let placed = venue.open_order(gtc_limit("later", Side::Buy, "0.1", "9000"));
    rested_open(&placed);

    // Above the liquidation level now, but a liquidation under way closes every debt, and
    // cancels what the account placed since.
    let second = thin(&mut venue, 3);
    assert!(notices(&second).is_empty());
    assert_eq!(shape(&second)[..2], ["balance USDT", "order later"]);
    assert_eq!(trades(&second)[0].quantity, d("0.4"));
    assert_eq!(
        balance_now(&venue, base()).margin.unwrap(),
        MarginDetails::default()
    );

    let [first_fee, second_fee] = [&first, &second].map(|events| {
        let flows = events
            .iter()
            .filter_map(|event| match &event.kind {
                AccountEventKind::CashFlow(flow) => Some(flow.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(flows.len(), 1, "{:?}", shape(events));
        flows[0].clone()
    });
    assert_eq!(
        (first_fee.amount, second_fee.amount),
        (d("-218.4"), d("-145.6")),
        "each pass is charged on what it repaid"
    );
    assert!(first_fee.id.is_some());
    assert_ne!(first_fee.id, second_fee.id, "each fee has an id of its own");
    assert!(thin(&mut venue, 4).is_empty());
}

#[test]
fn moving_the_clock_liquidates_an_account_that_owes_the_valuation_asset() {
    // Owes 1,000 USDT and holds 1,000: a margin level of 1, with no market needed to value it.
    let config = liquidating_config(
        vec![AssetBalance::new(
            quote(),
            Balance::new_margin(d("1000"), d("1000"), d("1000"), Decimal::ZERO),
            Utc::now(),
        )],
        liquidation("1.1", "0.02"),
    );
    let mut venue = SimulatedVenue::new(&config, spot_instruments());

    let events = venue.advance_time(time(1));

    assert_eq!(
        shape(&events),
        [
            "notice liquidation",
            "balance USDT",
            "debt USDT",
            "balance USDT",
            "cash flow",
        ],
        "repaid from the account's own balance, then the fee"
    );
    assert_eq!(
        balance_now(&venue, quote()),
        Balance::new_margin(d("-20"), d("-20"), Decimal::ZERO, Decimal::ZERO)
    );
}

#[test]
fn a_debt_the_venue_cannot_value_is_not_checked() {
    // Owes 1 BTC with no market to value it, and holds nothing else.
    let config = liquidating_config(
        vec![
            AssetBalance::new(
                base(),
                Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d("1"), Decimal::ZERO),
                Utc::now(),
            ),
            funded(quote(), d("1")),
        ],
        liquidation("1.1", "0.02"),
    );
    let mut venue = SimulatedVenue::new(&config, spot_instruments());

    let (events, warned) = warnings(|| venue.advance_time(time(1)));
    assert!(events.is_empty());
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert!(warned[0].contains("cannot value a debt"), "{warned:?}");

    let (events, warned) = warnings(|| venue.advance_time(time(2)));
    assert!(events.is_empty());
    assert!(warned.is_empty(), "warned once per venue: {warned:?}");
    assert_eq!(balance_now(&venue, base()).margin.unwrap().borrowed, d("1"));
}

#[test]
fn a_liquidation_the_leverage_limit_could_reach_is_refused() {
    let config = SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow).unwrap();

    // Borrowed to 3x, an account holds 1.5 times what it owes.
    assert_eq!(
        config.clone().with_liquidation(liquidation("1.5", "0")),
        Err(SpotMarginConfigError::LiquidationWithinLeverage {
            level: d("1.5"),
            max_leverage: d("3"),
        })
    );
    assert!(
        config
            .clone()
            .with_liquidation(liquidation("1.49", "0"))
            .is_ok()
    );
    let lends_nothing = SpotMarginConfig::new(quote(), d("1"), MarginEffect::Borrow).unwrap();
    assert!(
        lends_nothing
            .with_liquidation(liquidation("5", "0"))
            .is_ok()
    );
}

#[test]
fn a_fee_taken_in_base_is_grossed_up_so_the_buy_back_clears_the_loan() {
    let config = MockExecutionConfig {
        fee_model: FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001"))),
        spot_fee_asset: SpotFeeAsset::Received,
        ..liquidating_config(
            vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
            liquidation("1.1", "0.02"),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, "10000", 1).is_empty());
    venue.open_order(sell_request("1", None));

    // 19,990 left of the sale against 18,200 owed: a margin level of 1.098.
    let events = tick(&mut venue, "18200", 2);

    let bought = trades(&events)[0];
    assert_eq!(bought.fees.asset, base());
    assert!(
        bought.quantity > d("1"),
        "grossed up by the fee: {}",
        bought.quantity
    );
    let btc = balance_now(&venue, base());
    assert_eq!(btc.margin.unwrap(), MarginDetails::default());
    assert!(
        btc.free >= Decimal::ZERO && btc.free < d("0.000000000000000000001"),
        "{btc:?}"
    );
}

#[test]
fn a_debt_in_a_pairs_quote_is_bought_back_by_selling_the_valuation_asset_as_its_base() {
    // Valued in BTC, the base of BTCUSDT: 1 BTC held against 9,500 USDT owed, worth 0.95 BTC at
    // 10,000, a margin level of 1.05.
    let config = MockExecutionConfig {
        spot_margin: Some(
            SpotMarginConfig::new(base(), d("3"), MarginEffect::Borrow)
                .unwrap()
                .with_liquidation(liquidation("1.1", "0.02"))
                .unwrap(),
        ),
        ..config_from_balances(
            vec![
                funded(base(), d("1")),
                AssetBalance::new(
                    quote(),
                    Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d("9500"), Decimal::ZERO),
                    Utc::now(),
                ),
            ],
            FeeModelConfig::default(),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());

    let events = tick(&mut venue, "10000", 1);

    let sold = trades(&events);
    assert_eq!(sold.len(), 1);
    assert_eq!((sold[0].side, sold[0].quantity), (Side::Sell, d("0.95")));
    assert_eq!(
        balance_now(&venue, quote()),
        Balance::new_margin(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    );
    // 1 BTC less the 0.95 sold and the 0.019 fee, on the 0.95 BTC the debt was worth.
    assert_eq!(balance_now(&venue, base()).total, d("0.031"));
}

fn eth() -> AssetNameExchange {
    AssetNameExchange::new("ETH")
}

fn eth_instrument_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("ETHUSDT")
}

/// BTCUSDT and ETHUSDT.
fn btc_and_eth_instruments()
-> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    let mut instruments = spot_instruments();
    instruments.insert(
        eth_instrument_name(),
        Instrument {
            exchange: EXCHANGE,
            name_internal: InstrumentNameInternal::new("ethusdt"),
            name_exchange: eth_instrument_name(),
            underlying: Underlying {
                base: eth(),
                quote: quote(),
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
fn collateral_is_sold_in_name_order_and_only_as_much_as_the_debt_and_fee_need() {
    let config = liquidating_config(
        vec![
            funded(base(), d("0.1")),
            funded(eth(), Decimal::ZERO),
            funded(quote(), Decimal::ZERO),
        ],
        liquidation("1.1", "0.02"),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, "10000", 1).is_empty());
    let eth_tick = |venue: &mut SimulatedVenue, price: &str, seconds| {
        venue.apply_market(
            &eth_instrument_name(),
            MarketObservation::new(traded(price)),
            time(seconds),
        )
    };
    assert!(eth_tick(&mut venue, "1000", 1).is_empty());
    // 2 ETH bought with 2,000 USDT borrowed against the 1,000 of BTC: a margin level of 1.5.
    let bought = venue.open_order(request(eth_instrument_name(), Side::Buy, "2", None));
    assert_eq!(filled_trade(&bought).quantity, d("2"));

    // 1,000 of BTC and 1,200 of ETH against 2,000 owed: a margin level of 1.1.
    let events = eth_tick(&mut venue, "600", 2);

    let sold = trades(&events)
        .into_iter()
        .map(|trade| (trade.instrument.clone(), trade.quantity))
        .collect::<Vec<_>>();
    assert_eq!(
        sold,
        [
            (instrument_name(), d("0.1")),
            (eth_instrument_name(), d("1000") / d("600")),
            (eth_instrument_name(), d("40") / d("600")),
        ],
        "all the BTC first, then the ETH the rest of the loan needs, then the ETH the fee needs"
    );
    assert_eq!(
        balance_now(&venue, quote()).margin.unwrap(),
        MarginDetails::default()
    );
    let eth_left = balance_now(&venue, eth()).total;
    assert!(
        (eth_left - (d("2") - d("1040") / d("600"))).abs() < d("0.000000000000000000001"),
        "{eth_left}"
    );
}

/// A short of 1 ETH sold at 1,000 against `btc` held and `usdt` of the account's own, on a venue
/// trading BTCUSDT at 10,000 and ETHUSDT at 1,000, charging `fee_model`'s fees in
/// `spot_fee_asset`, and liquidating at 1.1 for a fee of 2 %.
fn short_one_eth(
    fee_model: FeeModelConfig,
    spot_fee_asset: SpotFeeAsset,
    btc: &str,
    usdt: &str,
) -> SimulatedVenue {
    let config = MockExecutionConfig {
        fee_model,
        spot_fee_asset,
        ..liquidating_config(
            vec![
                funded(base(), d(btc)),
                funded(eth(), Decimal::ZERO),
                funded(quote(), d(usdt)),
            ],
            liquidation("1.1", "0.02"),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments());
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, "10000", 1).is_empty());
    assert!(eth_tick(&mut venue, "1000", 1).is_empty());
    let sold = venue.open_order(request(eth_instrument_name(), Side::Sell, "1", None));
    assert_eq!(filled_trade(&sold).quantity, d("1"));
    venue
}

/// A trade on ETHUSDT at `price`, and nothing else, observed at `time(seconds)`.
fn eth_tick(venue: &mut SimulatedVenue, price: &str, seconds: i64) -> Vec<UnindexedAccountEvent> {
    venue.apply_market(
        &eth_instrument_name(),
        MarketObservation::new(traded(price)),
        time(seconds),
    )
}

/// Each trade's instrument, side and quantity.
fn traded_legs(events: &[UnindexedAccountEvent]) -> Vec<(InstrumentNameExchange, Side, Decimal)> {
    trades(events)
        .into_iter()
        .map(|trade| (trade.instrument.clone(), trade.side, trade.quantity))
        .collect()
}

#[test]
fn a_fee_the_free_balance_covers_in_part_sells_collateral_for_only_the_rest() {
    let mut venue = short_one_eth(FeeModelConfig::default(), SpotFeeAsset::Quote, "0.1", "10");

    // 80 of BTC and 1,010 USDT against 1,000 of ETH owed: a margin level of 1.09.
    let events = tick(&mut venue, "800", 2);

    assert_eq!(
        traded_legs(&events),
        [
            (eth_instrument_name(), Side::Buy, d("1")),
            (instrument_name(), Side::Sell, d("0.0125")),
        ],
        "the ETH bought back on its own pair, not on BTCUSDT, which is also quoted in USDT; then \
         10 of the 20 fee raised from BTC, since 10 USDT is left free"
    );
    assert_eq!(
        balance_now(&venue, eth()),
        Balance::new_margin(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    );
    assert_eq!(balance_now(&venue, quote()).total, Decimal::ZERO);
    assert_eq!(balance_now(&venue, base()).total, d("0.0875"));
}

#[test]
fn a_buy_back_costing_more_than_the_free_balance_sells_collateral_first() {
    let mut venue = short_one_eth(
        FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001"))),
        SpotFeeAsset::Received,
        "0.1",
        "0",
    );
    assert_eq!(balance_now(&venue, quote()).total, d("999"));

    // 1,000 of BTC and 999 USDT against 1,900 of ETH owed: a margin level of 1.05.
    let events = eth_tick(&mut venue, "1900", 2);

    let legs = traded_legs(&events);
    assert_eq!(
        legs.iter()
            .map(|(instrument, side, _)| (instrument.clone(), *side))
            .collect::<Vec<_>>(),
        [
            (instrument_name(), Side::Sell),
            (eth_instrument_name(), Side::Buy),
            (instrument_name(), Side::Sell),
        ],
        "BTC sold for what the buy-back lacks, the ETH bought back, then BTC sold for the fee"
    );
    // The buy-back costs 1 / 0.999 ETH at 1,900, of which 999 USDT is free; a BTC sale delivers
    // 9,990 USDT per BTC, net of its fee.
    let lacked = d("1") / d("0.999") * d("1900") - d("999");
    assert!(
        (legs[0].2 - lacked / d("9990")).abs() < d("0.000000000000000000001"),
        "{legs:?}"
    );
    assert_eq!(
        balance_now(&venue, eth()).margin.unwrap(),
        MarginDetails::default(),
        "the buy-back, grossed up for its fee in ETH, repays all of the ETH owed"
    );
}

#[test]
fn a_buy_back_the_balance_cannot_quite_afford_trades_a_digit_less() {
    let mut venue = short_one_btc(liquidation("1.1", "0"));

    // 20,000 buys 2/3 of a BTC at 30,000, which rounds up to more than the account holds.
    let events = tick(&mut venue, "30000", 2);

    let bought = trades(&events);
    assert_eq!(bought.len(), 1, "{:?}", shape(&events));
    assert_eq!(bought[0].quantity, d("0.6666666666666666666666666666"));
    let usdt = balance_now(&venue, quote());
    assert!(
        usdt.free >= Decimal::ZERO && usdt.free < d("0.00000000000000000001"),
        "{usdt:?}"
    );
}

#[test]
fn a_sale_whose_quantity_rounds_down_is_nudged_up_to_raise_the_whole_fee() {
    let mut venue = liquidating_venue(
        vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
        liquidation("1.1", "0.02"),
        "10000",
    );
    let outcome = venue.open_order(buy_request("2.5", None));
    assert_eq!(filled_trade(&outcome).quantity, d("2.5"));

    // 2.5 BTC at 6,300 against 15,000 owed: a margin level of 1.05. The fee is 300, and
    // 300 / 6,300 rounds down, so that quantity sells for a little less than the fee.
    let events = tick(&mut venue, "6300", 2);

    let sold = trades(&events);
    assert_eq!(sold.len(), 2, "{:?}", shape(&events));
    assert!(
        d("300") / d("6300") * d("6300") < d("300"),
        "the case this test is for"
    );
    assert!(sold[1].quantity > d("300") / d("6300"), "{:?}", sold[1]);
    let usdt = balance_now(&venue, quote());
    assert_eq!(usdt.margin.unwrap(), MarginDetails::default());
    assert!(
        usdt.free >= Decimal::ZERO && usdt.free < d("0.00000000000000000001"),
        "the fee is raised in full, so it leaves no negative balance: {usdt:?}"
    );
    assert!(
        tick(&mut venue, "6300", 3).is_empty(),
        "the loan and the fee are repaid in one pass, so no liquidation continues"
    );
}

/// Prices every fill at the last trade, recording what it was asked to price.
#[derive(Debug, Default)]
struct Recording(std::sync::Mutex<Vec<(Side, Decimal, Option<Decimal>)>>);

impl Recording {
    /// The side, quantity and last price of each fill priced since the last call.
    fn take(&self) -> Vec<(Side, Decimal, Option<Decimal>)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl FillModel for Recording {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal> {
        self.0
            .lock()
            .unwrap()
            .push((fill.side, fill.quantity, fill.market.last_price));
        fill.market.last_price
    }
}

#[test]
fn a_leg_is_priced_for_the_base_it_trades_and_only_for_an_asset_the_account_holds() {
    let config = liquidating_config(
        vec![
            funded(base(), Decimal::ZERO),
            funded(eth(), Decimal::ZERO),
            funded(quote(), d("1000")),
        ],
        liquidation("1.1", "0.02"),
    );
    let recording = Arc::new(Recording::default());
    let mut venue = SimulatedVenue::new_market_driven(&config, btc_and_eth_instruments())
        .with_fill_model(recording.clone());
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, "10000", 1).is_empty());
    assert!(eth_tick(&mut venue, "1000", 1).is_empty());
    let bought = venue.open_order(request(eth_instrument_name(), Side::Buy, "2.5", None));
    assert_eq!(filled_trade(&bought).quantity, d("2.5"));
    recording.take();

    // 2.5 ETH at 625 against 1,500 USDT owed: a margin level of 1.04.
    let events = eth_tick(&mut venue, "625", 2);

    assert_eq!(
        recording.take(),
        [
            (Side::Sell, d("2.4"), Some(d("625"))),
            (Side::Sell, d("0.048"), Some(d("625"))),
        ],
        "the ETH the loan and then the fee need, in ETH; the BTC the account does not hold is \
         never priced"
    );
    assert_eq!(
        traded_legs(&events),
        [
            (eth_instrument_name(), Side::Sell, d("2.4")),
            (eth_instrument_name(), Side::Sell, d("0.048")),
        ]
    );
}

#[test]
fn a_pair_without_a_market_is_passed_over_for_one_with_a_market() {
    // A second BTC/USDT pair, first by name, that the venue is never fed.
    let unfed = InstrumentNameExchange::new("BTC-USDT");
    let mut instruments = spot_instruments();
    let mut alias = instruments[&instrument_name()].clone();
    alias.name_exchange = unfed.clone();
    alias.name_internal = InstrumentNameInternal::new("btc-usdt");
    instruments.insert(unfed, alias);
    let config = liquidating_config(
        vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
        liquidation("1.1", "0"),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, instruments);
    advance(&mut venue, time(1));
    assert!(tick(&mut venue, "10000", 1).is_empty());
    assert_eq!(
        filled_trade(&venue.open_order(sell_request("1", None))).quantity,
        d("1")
    );

    let events = tick(&mut venue, "18200", 2);

    assert_eq!(
        traded_legs(&events),
        [(instrument_name(), Side::Buy, d("1"))]
    );
}

#[test]
fn a_leg_whose_fee_takes_all_it_delivers_is_not_traded() {
    // Owes 11,000 USDT against 2 BTC, and a sale's fee is 6,000 per BTC.
    let config = MockExecutionConfig {
        fee_model: FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("6000"),
        }),
        ..liquidating_config(
            vec![
                funded(base(), d("2")),
                AssetBalance::new(
                    quote(),
                    Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d("11000"), Decimal::ZERO),
                    Utc::now(),
                ),
            ],
            liquidation("1.1", "0.02"),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());

    // At 6,000 a margin level of 1.09, and a sale delivers nothing net of its fee.
    let (events, warned) = warnings(|| tick(&mut venue, "6000", 1));

    assert_eq!(shape(&events), ["notice liquidation"]);
    assert_eq!(
        balance_now(&venue, quote()).margin.unwrap().borrowed,
        d("11000")
    );
    assert_eq!(balance_now(&venue, base()).total, d("2"));
    assert!(
        warned
            .iter()
            .any(|message| message.contains("could not repay every debt")),
        "{warned:?}"
    );
}

#[test]
fn held_and_resting_orders_are_all_cancelled_in_client_order_id_order() {
    // Owes 1 BTC and holds 20,000 USDT, on a venue holding market orders for the next bar.
    let config = MockExecutionConfig {
        market_order_timing: MarketOrderTiming::NextBar(NextBarPrice::Open),
        ..liquidating_config(
            vec![
                AssetBalance::new(
                    base(),
                    Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d("1"), Decimal::ZERO),
                    Utc::now(),
                ),
                funded(quote(), d("20000")),
            ],
            liquidation("1.1", "0"),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(feed_bar(&mut venue, 1, ohlc("10000", "10000", "10000", "10000")).is_empty());
    rested_open(&venue.open_order(gtc_limit("c-resting", Side::Buy, "0.1", "9000")));
    let held = venue.open_order(ioc_buy("b-held", "0.1"));
    assert!(
        matches!(held.response.state, OrderState::Active(_)),
        "held for the next bar: {:?}",
        held.response.state
    );
    rested_open(&venue.open_order(gtc_limit("a-resting", Side::Buy, "0.1", "9000")));

    // 20,000 against 18,200 owed, and a trade price is no bar, so the held order is still held.
    let events = tick(&mut venue, "18200", 2);

    assert_eq!(
        shape(&events)[..7],
        [
            "notice liquidation",
            "balance USDT",
            "order a-resting",
            "balance USDT",
            "order b-held",
            "balance USDT",
            "order c-resting",
        ],
        "each releases what it held, by client order id"
    );
    assert!(venue.account.orders().is_empty());
    assert_eq!(
        traded_legs(&events),
        [(instrument_name(), Side::Buy, d("1"))]
    );
}

#[test]
fn a_dust_debt_that_values_at_zero_is_not_liquidated() {
    // Owes 1e-20 BTC, which at 1e-10 USDT is worth less than `Decimal` can hold.
    let config = liquidating_config(
        vec![
            AssetBalance::new(
                base(),
                Balance::new_margin(
                    Decimal::ZERO,
                    Decimal::ZERO,
                    d("0.00000000000000000001"),
                    Decimal::ZERO,
                ),
                Utc::now(),
            ),
            funded(quote(), d("1")),
        ],
        liquidation("1.1", "0"),
    );
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));

    assert!(tick(&mut venue, "0.0000000001", 1).is_empty());
    assert_eq!(
        balance_now(&venue, base()).margin.unwrap().borrowed,
        d("0.00000000000000000001")
    );
}

#[test]
fn while_a_liquidation_is_under_way_the_venue_lends_nothing() {
    let mut venue = short_one_btc(liquidation("1.1", "0.02"));
    let at = |venue: &mut SimulatedVenue, price: &str, ask: &str, seconds| {
        venue.apply_market(
            &instrument_name(),
            MarketObservation::new(traded(price))
                .with_depth(MarketDepth::new(Some(d("1")), Some(d(ask)))),
            time(seconds),
        )
    };
    // 0.6 of the 1 BTC owed bought back at 18,200, leaving 8,861.6 USDT free.
    assert_eq!(
        notices(&at(&mut venue, "18200", "0.6", 2)),
        [NoticeKind::Liquidation]
    );
    // BTC falls to 10,000 with nothing offered, so the liquidation is still under way, though
    // 8,861.6 held against 4,000 owed is a margin level of 2.2, within the leverage limit.
    assert!(trades(&at(&mut venue, "10000", "0", 3)).is_empty());
    assert_eq!(balance_now(&venue, quote()).free, d("8861.6"));

    // 9,000 needs a loan of 138.4, which the leverage limit allows, but nothing is lent.
    let refused = venue.open_order(gtc_limit("borrows", Side::Buy, "1", "9000"));
    assert_refused_a_loan(&refused, quote());
    // Nor is BTC lent to a market sale, which the bid on offer would fill at once.
    let refused = venue.open_order(sell_request("0.1", None));
    assert_refused_a_loan(&refused, base());
    rested_open(&venue.open_order(gtc_limit("pays", Side::Buy, "0.1", "9000")));

    // The rest bought back at 10,000, and the order it could pay for cancelled.
    let closed = at(&mut venue, "10000", "1", 4);
    assert_cancelled(order_named(&closed, "pays"));
    assert_eq!(balance_now(&venue, quote()).free, d("4781.6"));

    // 5,400 needs a loan of 618.4, which the venue lends again now that nothing is owed.
    rested_open(&venue.open_order(gtc_limit("borrows-after", Side::Buy, "0.6", "9000")));
    assert_eq!(
        balance_now(&venue, quote()).margin.unwrap().borrowed,
        d("618.4")
    );
}

#[test]
fn while_a_liquidation_is_under_way_a_held_order_whose_fill_needs_a_loan_is_cancelled() {
    // Owes 1 BTC and holds 20,000 USDT, on a venue holding market orders for the next bar.
    let config = MockExecutionConfig {
        market_order_timing: MarketOrderTiming::NextBar(NextBarPrice::Open),
        ..liquidating_config(
            vec![
                AssetBalance::new(
                    base(),
                    Balance::new_margin(Decimal::ZERO, Decimal::ZERO, d("1"), Decimal::ZERO),
                    Utc::now(),
                ),
                funded(quote(), d("20000")),
            ],
            liquidation("1.1", "0.02"),
        )
    };
    let mut venue = SimulatedVenue::new_market_driven(&config, spot_instruments());
    advance(&mut venue, time(1));
    assert!(feed_bar(&mut venue, 1, ohlc("10000", "10000", "10000", "10000")).is_empty());
    let at = |venue: &mut SimulatedVenue, price: &str, ask: &str, seconds| {
        venue.apply_market(
            &instrument_name(),
            MarketObservation::new(traded(price))
                .with_depth(MarketDepth::new(Some(d("1")), Some(d(ask)))),
            time(seconds),
        )
    };
    // 0.6 of the 1 BTC owed bought back at 18,200, leaving 8,861.6 USDT free; then BTC falls to
    // 10,000 with nothing offered, so the liquidation is still under way.
    assert_eq!(
        notices(&at(&mut venue, "18200", "0.6", 2)),
        [NoticeKind::Liquidation]
    );
    assert!(trades(&at(&mut venue, "10000", "0", 3)).is_empty());
    assert_eq!(balance_now(&venue, quote()).free, d("8861.6"));

    // 0.85 at 10,000 is 8,500, which the account pays for itself, so it is held.
    let held = venue.open_order(ioc_buy("held", "0.85"));
    assert!(
        matches!(held.response.state, OrderState::Active(_)),
        "held for the next bar: {:?}",
        held.response.state
    );

    // The bar opens at 11,000, where the fill costs 9,350: a loan of 488.4 that the leverage
    // limit allows, but nothing is lent, so the order is cancelled. The liquidation then buys
    // back the 0.4 still owed at 11,000, charging 2% of 4,400.
    let events = feed_bar(&mut venue, 4, ohlc("11000", "11000", "11000", "11000"));

    assert_cancelled(order_named(&events, "held"));
    assert_eq!(
        traded_legs(&events),
        [(instrument_name(), Side::Buy, d("0.4"))]
    );
    assert!(venue.account.orders().is_empty());
    assert_eq!(balance_now(&venue, quote()).free, d("4373.6"));
    assert_eq!(
        balance_now(&venue, quote()).margin.unwrap(),
        MarginDetails::default()
    );
}

#[test]
fn an_order_a_liquidation_cancels_reports_the_interest_its_loan_was_charged_when_it_rested() {
    let mut venue = liquidating_venue(
        vec![funded(base(), Decimal::ZERO), funded(quote(), d("10000"))],
        liquidation("1.1", "0"),
        "10000",
    )
    .with_margin_interest(Arc::new(hourly_interest(base())));
    assert_eq!(
        filled_trade(&venue.open_order(sell_request("1", None))).quantity,
        d("1")
    );
    // A sell resting above the market borrows the 0.1 BTC it would deliver, charged 0.00001.
    rested_open(&venue.open_order(gtc_limit("resting", Side::Sell, "0.1", "20000")));

    let events = tick(&mut venue, "18200", 2);

    assert_eq!(
        shape(&events)[..5],
        [
            "notice liquidation",
            "balance BTC",
            "debt BTC",
            "cash flow",
            "order resting"
        ],
        "the released hold and the loan it repaid, the charge it never reported, then the cancel: \
         {:?}",
        shape(&events)
    );
    assert_eq!(
        *liquidation_fee(&events[3]),
        interest_flow(
            base(),
            "0.1",
            "0.00001",
            time(2),
            InterestTiming::OnBorrow,
            "1"
        ),
        "stamped at the cancel; the short's own charge was the first"
    );
    assert_cancelled(order_named(&events, "resting"));
}
