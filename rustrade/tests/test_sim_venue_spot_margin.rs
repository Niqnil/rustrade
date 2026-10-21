#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! A simulated venue with spot margin and the engine describe the same holding while the account
//! goes short and long of an asset it starts without.
//!
//! The venue lends a sell the base the account lacks and repays the loan from what a buy
//! receives, so its base balance alone no longer says what the account holds: the holding is the
//! balance less the loan. Every fill of a seeded random sequence goes through the venue, the
//! account-event indexer and `EngineState`, as in a backtest, and after each one:
//! - the venue's net base, its total less what it owes, equals the engine's signed position,
//! - the account's PnL marked at the last price, its quote change plus its net base at that
//!   price, equals the engine's realised plus unrealised PnL before exit fees, and
//! - the base balance the engine holds reports the loan the venue does.
//!
//! With margin interest, the clock moves between fills, so loans are charged at borrow and at the
//! hourly boundaries the clock crosses. The engine leaves account-level interest out of its
//! position and PnL, so the venue's net base falls short of the engine's position, and the
//! account's PnL short of the engine's, by exactly the base interest charged, valued at the mark.

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::engine::state::{
    EngineState, global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
    position::PositionId,
};
use rustrade_execution::{
    AccountEventKind, AccountSnapshot, PeriodicMarginInterest, SpotFeeAsset, SpotMarginConfig,
    balance::{AssetBalance, Balance, MarginDetails},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PercentageFeeModel},
    fill::SimFillConfig,
    holding_cost::{DayCount, RateSeries},
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
    market::MarketSnapshot,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
    },
};
use rustrade_instrument::{
    Side, Underlying,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex, name::InstrumentNameExchange},
    test_utils::instrument,
};

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
/// The run's start, on a whole hour, so interest boundaries fall on the hour from it.
const TIME: DateTime<Utc> = DateTime::UNIX_EPOCH;

/// How far the clock moves before each fill: not a divisor of an hour, so fills land at
/// different points of the hour and some fall between boundaries the clock crosses.
const STEP: TimeDelta = TimeDelta::minutes(25);

/// No base, so the first sell borrows, and enough quote that leverage 3 lends every random sell.
const INITIAL_QUOTE: Decimal = dec!(100_000_000);

/// Prices whose reciprocals terminate in decimal, so a fee converted to base at one is exact.
const PRICES: [Decimal; 5] = [dec!(50), dec!(80), dec!(100), dec!(125), dec!(200)];
const QUANTITIES: [Decimal; 5] = [dec!(0.01), dec!(0.25), dec!(0.5), dec!(1), dec!(2)];

const FILLS_PER_RUN: usize = 300;

/// A weighted entry average can round in the last of `Decimal`'s 28 digits, and the unrealised
/// PnL multiplies that by the quantity held.
const PNL_TOLERANCE: Decimal = dec!(0.000000000000000001);

/// xorshift64: a fixed sequence per seed, with no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

fn base() -> AssetNameExchange {
    AssetNameExchange::new("btc")
}

fn quote() -> AssetNameExchange {
    AssetNameExchange::new("usdt")
}

fn instrument_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("btc_usdt")
}

fn funded(asset: AssetNameExchange, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset,
        balance: Balance::new(amount, amount),
        time_exchange: TIME,
    }
}

/// Hourly interest on base loans at 87.6% a year, exactly 0.01% an hour, so every charge on the
/// quantities traded is exact.
fn hourly_interest() -> PeriodicMarginInterest {
    PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
        .with_rates(base(), RateSeries::constant(dec!(0.876)))
}

fn venue(spot_fee_asset: SpotFeeAsset) -> SimulatedVenue {
    let config = MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![
                funded(base(), Decimal::ZERO),
                funded(quote(), INITIAL_QUOTE),
            ],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.001))),
        fill_model: SimFillConfig::default(),
        spot_fee_asset,
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: Some(SpotMarginConfig::new(quote(), dec!(3)).unwrap()),
        fee_asset: None,
    };
    let spot = Instrument {
        exchange: EXCHANGE,
        name_internal: instrument(EXCHANGE, "btc", "usdt").name_internal,
        name_exchange: instrument_name(),
        underlying: Underlying {
            base: base(),
            quote: quote(),
        },
        quote: rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
        kind: rustrade_instrument::instrument::kind::InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    };
    SimulatedVenue::new(&config, [(instrument_name(), spot)].into_iter().collect())
}

fn market_order(
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> OrderEvent<RequestOpen, ExchangeId, InstrumentNameExchange> {
    OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: instrument_name(),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::random(),
        },
        state: RequestOpen::new(
            side,
            None,
            quantity,
            OrderKind::Market,
            TimeInForce::ImmediateOrCancel,
        )
        .with_market(MarketSnapshot {
            best_bid: Some(price),
            best_ask: Some(price),
            last_price: Some(price),
        }),
    }
}

fn balance_now(venue: &SimulatedVenue, asset: AssetNameExchange) -> Balance {
    venue.balances(&[asset]).remove(0).balance
}

/// What one run did.
struct Run {
    borrows: usize,
    repays: usize,
    /// The base interest charged at borrow and at boundaries.
    interest: Decimal,
    /// How many boundaries charged interest.
    boundary_charges: usize,
}

/// The base interest the margin-interest cash flows among `events` charged, and how many there
/// were.
fn base_interest<'a>(
    events: impl IntoIterator<Item = &'a rustrade_execution::UnindexedAccountEvent>,
) -> (Decimal, usize) {
    events
        .into_iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::CashFlow(flow) if flow.asset == base() => Some(-flow.amount),
            _ => None,
        })
        .fold((Decimal::ZERO, 0), |(sum, count), charged| {
            (sum + charged, count + 1)
        })
}

/// Runs one seeded sequence of market fills, checking the venue against the engine after each,
/// with `interest` charged on the venue's loans if given.
fn run(spot_fee_asset: SpotFeeAsset, seed: u64, interest: Option<PeriodicMarginInterest>) -> Run {
    let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
    let indexer = AccountEventIndexer::new(Arc::new(
        generate_execution_instrument_map(&instruments, EXCHANGE).unwrap(),
    ));
    let mut engine: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
        EngineState::builder(&instruments, DefaultGlobalData, |_| {
            DefaultInstrumentMarketData::default()
        })
        .time_engine_start(TIME)
        .build();
    let base_index = engine
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .instrument
        .underlying
        .base;

    let mut venue = venue(spot_fee_asset);
    if let Some(model) = interest {
        venue = venue.with_margin_interest(Arc::new(model));
    }
    let mut rng = Rng(seed);
    let mut realised_exited = Decimal::ZERO;
    let mut run = Run {
        borrows: 0,
        repays: 0,
        interest: Decimal::ZERO,
        boundary_charges: 0,
    };
    let mut time = TIME;
    // The clock's first move, from which boundaries are charged.
    assert!(venue.advance_time(time).is_empty());

    for step in 0..FILLS_PER_RUN {
        let side = rng.pick(&[Side::Buy, Side::Sell]);
        let quantity = rng.pick(&QUANTITIES);
        let price = rng.pick(&PRICES);

        time += STEP;
        let charged = venue.advance_time(time);
        let (at_boundaries, count) = base_interest(&charged);
        run.interest += at_boundaries;
        run.boundary_charges += count;

        let owed_before = balance_now(&venue, base()).margin.unwrap().borrowed;
        let outcome = venue.open_order(market_order(side, quantity, price));
        assert!(
            outcome.response.state.is_accepted(),
            "step {step}: {side:?} {quantity} @ {price} was refused: {:?}",
            outcome.response.state
        );
        let owed_after = balance_now(&venue, base()).margin.unwrap().borrowed;
        run.borrows += usize::from(owed_after > owed_before);
        run.repays += usize::from(owed_after < owed_before);
        run.interest += base_interest(&outcome.events).0;

        for event in charged.into_iter().chain(outcome.events) {
            let event = indexer.account_event(event).unwrap();
            if let Some(exited) = engine.update_from_account(&event).exited {
                realised_exited += exited.pnl_realised;
            }
        }

        let position = engine
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .get(&PositionId::NETTING);
        let (held, pnl_open) = position.map_or((Decimal::ZERO, Decimal::ZERO), |position| {
            let held = match position.side {
                Side::Buy => position.quantity_abs,
                Side::Sell => -position.quantity_abs,
            };
            let unrealised_before_exit_fees =
                held * (price - position.price_entry_average) * position.contract_size;
            (held, position.pnl_realised + unrealised_before_exit_fees)
        });

        let venue_base = balance_now(&venue, base());
        let MarginDetails { borrowed, interest } = venue_base.margin.unwrap();
        let net_base = venue_base.total - borrowed - interest;
        assert_eq!(
            net_base + run.interest,
            held,
            "step {step} ({spot_fee_asset:?}): after {side:?} {quantity} @ {price}, the account \
             holds {net_base} net of its loan, and was charged {} interest, but the engine holds \
             {held}",
            run.interest
        );
        assert!(
            venue_base.total.is_zero() || borrowed.is_zero(),
            "step {step}: the account both holds {} and owes {borrowed}: a buy repays first",
            venue_base.total
        );

        let account_pnl = balance_now(&venue, quote()).total - INITIAL_QUOTE + net_base * price;
        let engine_pnl = realised_exited + pnl_open - run.interest * price;
        assert!(
            (account_pnl - engine_pnl).abs() <= PNL_TOLERANCE,
            "step {step} ({spot_fee_asset:?}): the account made {account_pnl} marked at \
             {price}, the engine {engine_pnl}"
        );

        let engine_base = engine
            .assets
            .asset_index(&base_index)
            .balance
            .unwrap()
            .value;
        assert_eq!(
            engine_base, venue_base,
            "step {step}: the engine's base balance"
        );
    }

    run
}

/// The default charges every fee in quote, so the base a fill moves is its quantity.
#[test]
fn under_quote_fees_a_margin_account_and_the_engine_agree_fill_by_fill() {
    for seed in [1, 7, 42] {
        let Run {
            borrows,
            repays,
            interest,
            ..
        } = run(SpotFeeAsset::Quote, seed, None);
        assert!(
            borrows > 0 && repays > 0,
            "seed {seed}: {borrows} borrows, {repays} repays"
        );
        assert_eq!(
            interest,
            Decimal::ZERO,
            "seed {seed}: no model, no interest"
        );
    }
}

/// `Received` charges a buy's fee in base, so a buy repays the loan from its quantity less the
/// fee, which the engine nets from its position.
#[test]
fn under_received_fees_a_margin_account_and_the_engine_agree_fill_by_fill() {
    for seed in [1, 7, 42] {
        let Run {
            borrows, repays, ..
        } = run(SpotFeeAsset::Received, seed, None);
        assert!(
            borrows > 0 && repays > 0,
            "seed {seed}: {borrows} borrows, {repays} repays"
        );
    }
}

/// Interest at borrow and at each hour the clock crosses moves the account away from the
/// engine by exactly what it charged, under either fee asset.
#[test]
fn with_margin_interest_a_margin_account_and_the_engine_differ_by_exactly_the_interest() {
    for spot_fee_asset in [SpotFeeAsset::Quote, SpotFeeAsset::Received] {
        for seed in [1, 7, 42] {
            let Run {
                borrows,
                repays,
                interest,
                boundary_charges,
            } = run(spot_fee_asset, seed, Some(hourly_interest()));
            assert!(
                borrows > 0 && repays > 0 && boundary_charges > 0,
                "seed {seed} ({spot_fee_asset:?}): {borrows} borrows, {repays} repays, \
                 {boundary_charges} boundary charges"
            );
            assert!(interest > Decimal::ZERO);
        }
    }
}
