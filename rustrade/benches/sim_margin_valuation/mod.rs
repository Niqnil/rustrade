#![allow(clippy::unwrap_used, clippy::expect_used)] // Benchmark code: panics acceptable

//! The per-tick cost of a simulated venue's spot margin check, which values the whole account at
//! the end of every market tick while the account owes something and liquidation is configured.
//!
//! Each iteration feeds one `SimulatedVenue::apply_market` tick to a venue trading `instruments`
//! spot pairs `x{i}/usdt`, all priced, on an account holding usdt and `held` of the base assets.
//! It owes a small loan of `x0`, far from the liquidation level, so nothing is liquidated and no
//! order is open: the tick's work is storing the price and checking the margin.
//!
//! - `liquidation_off`: the margin check returns at once. The baseline.
//! - `liquidation_on`: the check values every balance in usdt.
//!
//! The difference between the two is the valuation's cost per tick. The id's suffix is
//! `<instruments>i_<held>h`.

use chrono::{DateTime, Utc};
use criterion::{BenchmarkId, Criterion};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountSnapshot, MarginEffect, SpotMarginConfig, SpotMarginLiquidation,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    market::{MarketObservation, MarketSnapshot},
};
use rustrade_instrument::{
    Underlying,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{
        Instrument,
        kind::InstrumentKind,
        name::{InstrumentNameExchange, InstrumentNameInternal},
        quote::InstrumentQuoteAsset,
    },
};
use std::hint::black_box;

criterion::criterion_main!(benchmark_sim_margin_valuation);

criterion::criterion_group!(
    name = benchmark_sim_margin_valuation;
    config = Criterion::default();
    targets = bench_tick
);

const EXCHANGE: ExchangeId = ExchangeId::Mock;

/// `(instruments, held)`: the venue's spot pairs, and how many of their base assets the account
/// holds besides usdt.
const SIZES: [(usize, usize); 9] = [
    (1, 1),
    (10, 1),
    (10, 10),
    (100, 1),
    (100, 10),
    (100, 100),
    (1000, 1),
    (1000, 10),
    (1000, 100),
];

const PRICE: Decimal = dec!(100);

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn usdt() -> AssetNameExchange {
    AssetNameExchange::new("usdt")
}

fn base(i: usize) -> AssetNameExchange {
    AssetNameExchange::new(format!("x{i}"))
}

fn name(i: usize) -> InstrumentNameExchange {
    InstrumentNameExchange::new(format!("x{i}_usdt"))
}

fn spot(i: usize) -> Instrument<ExchangeId, AssetNameExchange> {
    Instrument {
        exchange: EXCHANGE,
        name_internal: InstrumentNameInternal::new_from_exchange(EXCHANGE, name(i)),
        name_exchange: name(i),
        underlying: Underlying {
            base: base(i),
            quote: usdt(),
        },
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    }
}

fn observation(price: Decimal) -> MarketObservation {
    MarketObservation::new(MarketSnapshot::new(
        Some(price - dec!(0.01)),
        Some(price + dec!(0.01)),
        Some(price),
    ))
}

/// A venue of `instruments` priced pairs, on an account holding `held` base assets and owing one
/// unit of `x0`, liquidating at a margin level of 1.1 when `liquidation` is set.
fn venue(instruments: usize, held: usize, liquidation: bool) -> SimulatedVenue {
    let mut spot_margin = SpotMarginConfig::new(usdt(), dec!(3), MarginEffect::Borrow).unwrap();
    if liquidation {
        spot_margin = spot_margin
            .with_liquidation(SpotMarginLiquidation::new(dec!(1.1), dec!(0.02)).unwrap())
            .unwrap();
    }

    let mut balances = vec![AssetBalance {
        asset: usdt(),
        balance: Balance::new(dec!(1_000_000), dec!(1_000_000)),
        time_exchange: at(0),
    }];
    balances.extend((0..held).map(|i| AssetBalance {
        asset: base(i),
        balance: match i {
            0 => Balance::new_margin(dec!(10), dec!(10), dec!(1), Decimal::ZERO),
            _ => Balance::new(dec!(10), dec!(10)),
        },
        time_exchange: at(0),
    }));

    let config = MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances,
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: Default::default(),
        fill_model: Default::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: Some(spot_margin),
        fee_asset: None,
        long_option_expiry: Default::default(),
        short_put_collateral: Default::default(),
        amend: Default::default(),
        time_in_force: Default::default(),
        session_close: None,
        fee_model_by_kind: Default::default(),
    };
    let map: FnvHashMap<_, _> = (0..instruments).map(|i| (name(i), spot(i))).collect();
    let mut venue = SimulatedVenue::new_market_driven(&config, map);
    for i in 0..instruments {
        let events = venue.apply_market(&name(i), observation(PRICE), at(0));
        assert!(events.is_empty(), "seeding x{i}_usdt produced {events:?}");
    }
    venue
}

fn bench_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("sim_margin_valuation");
    for (instruments, held) in SIZES {
        for liquidation in [false, true] {
            let mode = match liquidation {
                true => "liquidation_on",
                false => "liquidation_off",
            };
            let mut venue = venue(instruments, held, liquidation);
            let ticked = name(0);
            let mut secs = 0;
            group.bench_function(
                BenchmarkId::new(mode, format!("{instruments}i_{held}h")),
                |b| {
                    b.iter(|| {
                        secs += 1;
                        let price = PRICE + Decimal::from(secs % 2) / dec!(100);
                        let events = venue.apply_market(&ticked, observation(price), at(secs));
                        debug_assert!(events.is_empty());
                        black_box(events)
                    })
                },
            );
        }
    }
    group.finish();
}
