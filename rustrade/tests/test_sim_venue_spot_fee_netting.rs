#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! The simulated venue's spot balances and the engine's spot position describe the same holding,
//! whichever asset the venue charges a fill's fee in.
//!
//! Under `SpotFeeAsset::Received` a buy's fee comes out of the base it receives, and the engine
//! nets a base fee from a spot position. If either side got the fee's sign or its asset wrong, the
//! engine's position would drift from the account's base balance by the fees, and a full-size
//! close would ask to sell more than the account holds. So every fill of a seeded random sequence
//! goes through the venue, the account-event indexer and `EngineState`, as in a backtest, and
//! after each one:
//! - the change in the venue's base balance equals the engine's signed position, and
//! - the account's PnL marked at the last price, its quote change plus its base change at that
//!   price, equals the engine's realised plus unrealised PnL before exit fees, less the fee of any
//!   fill that moved no holding from flat, which no position books.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::engine::state::{
    EngineState, global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
    position::PositionId,
};
use rustrade_execution::{
    AccountEventKind, AccountSnapshot, SpotFeeAsset,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PerContractFeeModel, PercentageFeeModel},
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

/// Enough of both assets that a random fill is never refused for want of balance, so every fill
/// in a sequence trades.
const INITIAL_BASE: Decimal = dec!(10_000);
const INITIAL_QUOTE: Decimal = dec!(100_000_000);

/// Prices whose reciprocals terminate in decimal, so a fee converted to base at one is exact and
/// the PnL comparison can be exact too, up to the rounding of a weighted entry average.
const PRICES: [Decimal; 7] = [
    dec!(50),
    dec!(80),
    dec!(100),
    dec!(125),
    dec!(160),
    dec!(200),
    dec!(250),
];
/// From a sliver to several units.
const QUANTITIES: [Decimal; 6] = [
    dec!(0.001),
    dec!(0.01),
    dec!(0.25),
    dec!(0.5),
    dec!(1),
    dec!(2),
];

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

fn venue(fee_model: FeeModelConfig, spot_fee_asset: SpotFeeAsset) -> SimulatedVenue {
    let config = MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![funded(base(), INITIAL_BASE), funded(quote(), INITIAL_QUOTE)],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model,
        fill_model: SimFillConfig::default(),
        spot_fee_asset,
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: None,
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

fn balance_total(venue: &SimulatedVenue, asset: AssetNameExchange) -> Decimal {
    venue.balances(&[asset]).remove(0).balance.total
}

/// Runs one seeded sequence of market fills, checking the venue against the engine after each.
fn run(fee_model: FeeModelConfig, spot_fee_asset: SpotFeeAsset, seed: u64) -> RunStats {
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

    let mut venue = venue(fee_model, spot_fee_asset);
    let mut rng = Rng(seed);
    let mut realised_exited = Decimal::ZERO;
    // Fees of fills that moved no holding on a flat instrument. Such a fill opens no position, so
    // its fee reaches no position's PnL, as `InstrumentState::update_from_trade` documents.
    let mut fees_unbooked = Decimal::ZERO;
    let mut stats = RunStats::default();

    for step in 0..FILLS_PER_RUN {
        let side = rng.pick(&[Side::Buy, Side::Sell]);
        let quantity = rng.pick(&QUANTITIES);
        let price = rng.pick(&PRICES);

        let outcome = venue.open_order(market_order(side, quantity, price));
        assert!(
            outcome.response.state.is_accepted(),
            "step {step}: {side:?} {quantity} @ {price} was refused: {:?}",
            outcome.response.state
        );

        for event in outcome.events {
            let event = indexer.account_event(event).unwrap();
            if let AccountEventKind::Trade(trade) = &event.kind {
                if trade.fees.asset == base_index {
                    stats.base_fees += 1;
                }
                if trade.fees.asset == base_index && trade.fees.fees > quantity {
                    stats.fees_above_quantity += 1;
                }
                if trade.fees.asset == base_index && trade.fees.fees == quantity {
                    stats.fees_equal_to_quantity += 1;
                    let flat = engine
                        .instruments
                        .instrument_index(&InstrumentIndex(0))
                        .position
                        .positions
                        .is_empty();
                    if flat {
                        fees_unbooked += trade.fees.fees_quote.unwrap();
                    }
                }
            }
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

        let base_moved = balance_total(&venue, base()) - INITIAL_BASE;
        assert_eq!(
            base_moved, held,
            "step {step} ({spot_fee_asset:?}, {fee_model:?}): after {side:?} {quantity} @ \
             {price}, the account's base moved by {base_moved} but the engine holds {held}"
        );

        let account_pnl = balance_total(&venue, quote()) - INITIAL_QUOTE + base_moved * price;
        let engine_pnl = realised_exited + pnl_open - fees_unbooked;
        assert!(
            (account_pnl - engine_pnl).abs() <= PNL_TOLERANCE,
            "step {step} ({spot_fee_asset:?}, {fee_model:?}): the account made {account_pnl} \
             marked at {price}, the engine {engine_pnl}"
        );
    }

    stats
}

#[derive(Debug, Default)]
struct RunStats {
    /// Fills whose fee was charged in base.
    base_fees: usize,
    /// Fills whose base fee exceeded their quantity, which move the holding against their side.
    fees_above_quantity: usize,
    /// Fills whose base fee equalled their quantity, which move no holding.
    fees_equal_to_quantity: usize,
}

fn fee_models() -> [FeeModelConfig; 5] {
    [
        FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.001))),
        // A rebate on every fill: a negative base fee adds to what a buy receives. Inverting its
        // sign is a known bug class in other libraries.
        FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(-0.0005))),
        FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: dec!(0.5),
        }),
        // Above every price, so in base it exceeds the quantity a buy receives: the buy takes base
        // from the account, and the engine moves the holding down.
        FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: dec!(300),
        }),
        // Equal to one of the prices, where in base it is exactly the quantity a buy receives,
        // so the buy moves no holding.
        FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: dec!(100),
        }),
    ]
}

/// The default charges every fee in quote, so the base a fill moves is its quantity.
#[test]
fn under_quote_fees_the_account_and_the_engine_agree_fill_by_fill() {
    for fee_model in fee_models() {
        for seed in [1, 7, 42] {
            let stats = run(fee_model, SpotFeeAsset::Quote, seed);
            assert_eq!(stats.base_fees, 0, "no fee is charged in base");
        }
    }
}

/// `Received` charges a buy's fee in base, which the engine nets from its position.
#[test]
fn under_received_fees_the_account_and_the_engine_agree_fill_by_fill() {
    let (mut fees_above_quantity, mut fees_equal_to_quantity) = (0, 0);
    for fee_model in fee_models() {
        for seed in [1, 7, 42] {
            let stats = run(fee_model, SpotFeeAsset::Received, seed);
            assert!(stats.base_fees > 0, "{fee_model:?}: some buys pay in base");
            fees_above_quantity += stats.fees_above_quantity;
            fees_equal_to_quantity += stats.fees_equal_to_quantity;
        }
    }
    assert!(
        fees_above_quantity > 0,
        "the flat fee must exceed some buy's quantity, so that edge is exercised"
    );
    assert!(
        fees_equal_to_quantity > 0,
        "the flat fee must equal some buy's quantity, so that edge is exercised"
    );
}
