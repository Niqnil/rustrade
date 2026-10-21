#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! A simulated venue charging fees in a third asset, and the engine valuing them.
//!
//! With `MockExecutionConfig::fee_asset`, the venue charges a `BTCUSDT` fill's fee in BNB, at a
//! discount, as a live account paying fees with BNB is charged. The fill reports its fee in BNB,
//! so the account-event indexer gives it no quote equivalent, as it does a live fill's. The engine
//! then values it with `BNBUSDT`'s price when it has one, and leaves it out of PnL when it has
//! none, the same as live.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    Timed,
    engine::state::{
        EngineState, fee::FeeConversionOutcome, global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData,
    },
};
use rustrade_execution::{
    AccountSnapshot, FeeAssetConfig,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PercentageFeeModel},
    fill::SimFillConfig,
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
    market::{MarketObservation, MarketSnapshot},
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
    instrument::{
        Instrument, InstrumentIndex, kind::InstrumentKind, name::InstrumentNameExchange,
        quote::InstrumentQuoteAsset,
    },
    test_utils::instrument,
};

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

fn time(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap()
}

fn asset(name: &str) -> AssetNameExchange {
    AssetNameExchange::new(name)
}

fn name(base: &str, quote: &str) -> InstrumentNameExchange {
    InstrumentNameExchange::new(format!("{base}_{quote}"))
}

fn spot(base: &str, quote: &str) -> Instrument<ExchangeId, AssetNameExchange> {
    Instrument {
        exchange: EXCHANGE,
        name_internal: instrument(EXCHANGE, base, quote).name_internal,
        name_exchange: name(base, quote),
        underlying: Underlying {
            base: asset(base),
            quote: asset(quote),
        },
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    }
}

fn funded(name: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: asset(name),
        balance: Balance::new(amount, amount),
        time_exchange: time(0),
    }
}

/// A fed venue trading `BTCUSDT`, pricing BNB at a 599 / 601 `BNBUSDT` book, and charging a
/// 10bp fee in BNB at a quarter off.
fn venue() -> SimulatedVenue {
    let config = MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![
                funded("btc", dec!(0)),
                funded("usdt", dec!(100_000)),
                funded("bnb", dec!(1)),
            ],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: FeeModelConfig::Percentage(PercentageFeeModel::maker_taker(
            dec!(0.001),
            dec!(0.001),
        )),
        fill_model: SimFillConfig::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: None,
        fee_asset: Some(FeeAssetConfig::new(asset("bnb"), dec!(0.25)).unwrap()),
    };
    let mut venue = SimulatedVenue::new_market_driven(
        &config,
        [spot("btc", "usdt"), spot("bnb", "usdt")]
            .into_iter()
            .map(|instrument| (instrument.name_exchange.clone(), instrument))
            .collect(),
    );
    let _ = venue.advance_time(time(1));
    let bnb_book = MarketSnapshot {
        best_bid: Some(dec!(599)),
        best_ask: Some(dec!(601)),
        last_price: None,
    };
    assert!(
        venue
            .apply_market(
                &name("bnb", "usdt"),
                MarketObservation::new(bnb_book),
                time(1)
            )
            .is_empty()
    );
    venue
}

/// Moves the venue to `time(seconds)` with `BTCUSDT` last traded at `price`.
fn price_btc(venue: &mut SimulatedVenue, price: Decimal, seconds: i64) {
    let _ = venue.advance_time(time(seconds));
    let _ = venue.apply_market(
        &name("btc", "usdt"),
        MarketObservation::new(MarketSnapshot::from_last_price(Some(price))),
        time(seconds),
    );
}

fn market_order(side: Side) -> OrderEvent<RequestOpen, ExchangeId, InstrumentNameExchange> {
    OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: name("btc", "usdt"),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::random(),
        },
        state: RequestOpen::new(
            side,
            None,
            dec!(1),
            OrderKind::Market,
            TimeInForce::ImmediateOrCancel,
        ),
    }
}

struct RoundTrip {
    /// The engine's realised PnL on the round trip.
    pnl_realised: Decimal,
    /// The fee conversions the engine reported, in order.
    outcomes: Vec<FeeConversionOutcome>,
    /// The fills the engine left out of PnL for want of a price.
    unconverted: usize,
    /// How much the venue's BNB and USDT balances moved.
    bnb_moved: Decimal,
    usdt_moved: Decimal,
}

/// Buys 1 BTC at 50,000 and sells it at 51,000 through the venue, the indexer and the engine,
/// which holds `BNBUSDT` at `engine_bnb_price` when given one.
fn round_trip(engine_bnb_price: Option<Decimal>) -> RoundTrip {
    let instruments = IndexedInstruments::new([
        instrument(EXCHANGE, "btc", "usdt"),
        instrument(EXCHANGE, "bnb", "usdt"),
    ]);
    let indexer = AccountEventIndexer::new(Arc::new(
        generate_execution_instrument_map(&instruments, EXCHANGE).unwrap(),
    ));
    let mut engine: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
        EngineState::builder(&instruments, DefaultGlobalData, |_| {
            DefaultInstrumentMarketData::default()
        })
        .time_engine_start(time(0))
        .build();
    let btc_usdt = InstrumentIndex(
        instruments
            .instruments()
            .iter()
            .position(|keyed| keyed.value.name_exchange == name("btc", "usdt"))
            .unwrap(),
    );
    if let Some(price) = engine_bnb_price {
        let bnb_usdt = instruments
            .instruments()
            .iter()
            .find(|keyed| keyed.value.name_exchange == name("bnb", "usdt"))
            .unwrap()
            .key;
        engine
            .instruments
            .instrument_index_mut(&bnb_usdt)
            .data
            .last_traded_price = Some(Timed::new(price, time(1)));
    }

    let mut venue = venue();
    let mut outcomes = Vec::new();
    let mut pnl_realised = Decimal::ZERO;
    for (side, price, seconds) in [(Side::Buy, dec!(50_000), 2), (Side::Sell, dec!(51_000), 3)] {
        price_btc(&mut venue, price, seconds);
        let outcome = venue.open_order(market_order(side));
        assert!(outcome.response.state.is_accepted(), "{outcome:?}");
        for event in outcome.events {
            let applied = engine.update_from_account(&indexer.account_event(event).unwrap());
            outcomes.extend(applied.fee_conversion.map(|conversion| conversion.outcome));
            if let Some(exited) = applied.exited {
                pnl_realised = exited.pnl_realised;
            }
        }
    }

    let total = |name: &str| venue.balances(&[asset(name)]).remove(0).balance.total;
    RoundTrip {
        pnl_realised,
        outcomes,
        unconverted: engine
            .instruments
            .instrument_index(&btc_usdt)
            .tear_sheet
            .fills_fee_unconverted,
        bnb_moved: total("bnb") - dec!(1),
        usdt_moved: total("usdt") - dec!(100_000),
    }
}

/// The venue charges 50 and 51 USDT of fees, a quarter off, as 0.0625 and 0.06375 BNB at 600, and
/// exchanges the assets free of fee. The engine, holding BNBUSDT at 600 too, values those fees at
/// what the venue charged, so its realised PnL is the account's USDT gain less its BNB spent,
/// valued at that price.
#[test]
fn the_engine_values_the_venues_fee_asset_charges_when_it_prices_the_asset() {
    let run = round_trip(Some(dec!(600)));

    assert_eq!(run.usdt_moved, dec!(1000));
    assert_eq!(run.bnb_moved, dec!(-0.12625));
    assert_eq!(run.pnl_realised, dec!(924.25));
    assert_eq!(run.pnl_realised, run.usdt_moved + run.bnb_moved * dec!(600));
    assert_eq!(run.unconverted, 0);
    assert!(matches!(
        run.outcomes.as_slice(),
        [
            FeeConversionOutcome::Converted { .. },
            FeeConversionOutcome::Converted { .. }
        ]
    ));
}

/// An engine that does not price BNB leaves the fees out of PnL, as a live run does, and counts
/// both fills.
#[test]
fn an_engine_without_the_fee_assets_price_leaves_the_fees_out() {
    let run = round_trip(None);

    assert_eq!(run.bnb_moved, dec!(-0.12625));
    assert_eq!(run.pnl_realised, dec!(1000));
    assert_eq!(run.unconverted, 2);
    assert!(matches!(
        run.outcomes.as_slice(),
        [
            FeeConversionOutcome::NoPrice { .. },
            FeeConversionOutcome::NoPrice { .. }
        ]
    ));
}
