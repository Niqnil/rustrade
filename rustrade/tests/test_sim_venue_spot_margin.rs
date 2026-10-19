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

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::engine::state::{
    EngineState, global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
    position::PositionId,
};
use rustrade_execution::{
    AccountSnapshot, SpotFeeAsset, SpotMarginConfig,
    balance::{AssetBalance, Balance, MarginDetails},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PercentageFeeModel},
    fill::SimFillConfig,
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
const TIME: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

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
        state: RequestOpen {
            side,
            price: None,
            quantity,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            position_id: None,
            reduce_only: false,
            market: Some(MarketSnapshot {
                best_bid: Some(price),
                best_ask: Some(price),
                last_price: Some(price),
            }),
        },
    }
}

fn balance_now(venue: &SimulatedVenue, asset: AssetNameExchange) -> Balance {
    venue.balances(&[asset]).remove(0).balance
}

/// Runs one seeded sequence of market fills, checking the venue against the engine after each,
/// and returns how many fills borrowed and how many repaid.
fn run(spot_fee_asset: SpotFeeAsset, seed: u64) -> (usize, usize) {
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
    let mut rng = Rng(seed);
    let mut realised_exited = Decimal::ZERO;
    let (mut borrows, mut repays) = (0, 0);

    for step in 0..FILLS_PER_RUN {
        let side = rng.pick(&[Side::Buy, Side::Sell]);
        let quantity = rng.pick(&QUANTITIES);
        let price = rng.pick(&PRICES);

        let owed_before = balance_now(&venue, base()).margin.unwrap().borrowed;
        let outcome = venue.open_order(market_order(side, quantity, price));
        assert!(
            outcome.response.state.is_accepted(),
            "step {step}: {side:?} {quantity} @ {price} was refused: {:?}",
            outcome.response.state
        );
        let owed_after = balance_now(&venue, base()).margin.unwrap().borrowed;
        borrows += usize::from(owed_after > owed_before);
        repays += usize::from(owed_after < owed_before);

        for event in outcome.events {
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
            net_base, held,
            "step {step} ({spot_fee_asset:?}): after {side:?} {quantity} @ {price}, the account \
             holds {net_base} net of its loan but the engine holds {held}"
        );
        assert!(
            venue_base.total.is_zero() || borrowed.is_zero(),
            "step {step}: the account both holds {} and owes {borrowed}: a buy repays first",
            venue_base.total
        );

        let account_pnl = balance_now(&venue, quote()).total - INITIAL_QUOTE + net_base * price;
        let engine_pnl = realised_exited + pnl_open;
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

    (borrows, repays)
}

/// The default charges every fee in quote, so the base a fill moves is its quantity.
#[test]
fn under_quote_fees_a_margin_account_and_the_engine_agree_fill_by_fill() {
    for seed in [1, 7, 42] {
        let (borrows, repays) = run(SpotFeeAsset::Quote, seed);
        assert!(
            borrows > 0 && repays > 0,
            "seed {seed}: {borrows} borrows, {repays} repays"
        );
    }
}

/// `Received` charges a buy's fee in base, so a buy repays the loan from its quantity less the
/// fee, which the engine nets from its position.
#[test]
fn under_received_fees_a_margin_account_and_the_engine_agree_fill_by_fill() {
    for seed in [1, 7, 42] {
        let (borrows, repays) = run(SpotFeeAsset::Received, seed);
        assert!(
            borrows > 0 && repays > 0,
            "seed {seed}: {borrows} borrows, {repays} repays"
        );
    }
}
