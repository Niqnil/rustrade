#![allow(clippy::unwrap_used, clippy::expect_used)] // Benchmark code: panics acceptable

//! The cost of pricing, Greeks and implied volatility with the finite-difference model, for
//! American options across the expiries and moneyness a listed equity options chain spans.
//!
//! Every iteration runs the operation once on each of 48 cases, so the throughput line reads as
//! cases per second, and the time per iteration divided by 48 is the mean time per case. The cases
//! are American calls and puts on a stock at 100, with a 4% rate and 30% volatility:
//! - expiries of 7, 30, 90 and 365 days;
//! - strikes at standardized moneyness `ln(K/S) / (σ√t)` of −1, 0 and +1;
//! - with no dividend, and with one 0.50 cash dividend halfway to expiry.
//!
//! Groups, each on grids of 400×200 (the default), 200×100 and 100×50 (space × time steps):
//! - `american_price`: `price` at 30% volatility.
//! - `american_greeks`: `greeks` at 30% volatility, one valuation plus the bumped revaluations for
//!   vega and rho.
//! - `american_implied_volatility`: `implied_volatility` with the default configuration, solving
//!   for the price the default grid gives at 30%. On a coarser grid that price is slightly off the
//!   grid's own, as a market price is off any model's, so the solve does not start at its answer.
//!   Setup checks that every case solves, so a failing solve is never what is timed.

use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, measurement::WallTime,
};
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};
use rustrade_pricing::{
    CashDividend, FiniteDifferenceModel, ImpliedVolatilityConfig, OptionInputs, OptionModel,
    implied_volatility,
};
use std::{hint::black_box, time::Duration};

criterion::criterion_main!(benchmark_american);

criterion::criterion_group!(
    name = benchmark_american;
    config = Criterion::default();
    targets = bench_price, bench_greeks, bench_implied_volatility
);

const SPOT: f64 = 100.0;
const RATE: f64 = 0.04;
const VOLATILITY: f64 = 0.30;
const DIVIDEND: f64 = 0.50;
const DAYS: [f64; 4] = [7.0, 30.0, 90.0, 365.0];
const MONEYNESS: [f64; 3] = [-1.0, 0.0, 1.0];

/// Space × time steps, the default grid first.
const GRIDS: [(usize, usize); 3] = [(400, 200), (200, 100), (100, 50)];

/// One case and the price the default grid gives it at [`VOLATILITY`].
struct Case {
    inputs: OptionInputs,
    target_price: f64,
}

fn cases() -> Vec<Case> {
    let reference = FiniteDifferenceModel::default();
    let mut cases = Vec::new();
    for days in DAYS {
        let expiry = days / 365.0;
        for dividend in [false, true] {
            for kind in [OptionKind::Call, OptionKind::Put] {
                for moneyness in MONEYNESS {
                    let strike = SPOT * (moneyness * VOLATILITY * expiry.sqrt()).exp();
                    let mut inputs = OptionInputs::new(
                        kind,
                        OptionExercise::American,
                        SPOT,
                        strike,
                        expiry,
                        RATE,
                    );
                    if dividend {
                        inputs =
                            inputs.with_dividends(vec![CashDividend::new(expiry / 2.0, DIVIDEND)]);
                    }
                    let target_price = reference.price(&inputs, VOLATILITY).unwrap();
                    cases.push(Case {
                        inputs,
                        target_price,
                    });
                }
            }
        }
    }
    cases
}

fn models() -> impl Iterator<Item = (String, FiniteDifferenceModel)> {
    GRIDS.into_iter().map(|(space, time)| {
        (
            format!("{space}x{time}"),
            FiniteDifferenceModel::new(space, time).unwrap(),
        )
    })
}

/// A group whose iteration covers every case. One iteration on the default grid takes tens to
/// hundreds of milliseconds, too long for criterion's default of 100 linearly growing samples in
/// 5 seconds, so each sample runs the same number of iterations instead.
fn group<'a>(c: &'a mut Criterion, name: &str, cases: &[Case]) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(cases.len() as u64));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group
}

fn bench_price(c: &mut Criterion) {
    let cases = cases();
    let mut group = group(c, "american_price", &cases);
    for (grid, model) in models() {
        group.bench_function(BenchmarkId::from_parameter(grid), |b| {
            b.iter(|| {
                for case in &cases {
                    black_box(model.price(black_box(&case.inputs), VOLATILITY).unwrap());
                }
            })
        });
    }
    group.finish();
}

fn bench_greeks(c: &mut Criterion) {
    let cases = cases();
    let mut group = group(c, "american_greeks", &cases);
    for (grid, model) in models() {
        group.bench_function(BenchmarkId::from_parameter(grid), |b| {
            b.iter(|| {
                for case in &cases {
                    black_box(model.greeks(black_box(&case.inputs), VOLATILITY).unwrap());
                }
            })
        });
    }
    group.finish();
}

fn bench_implied_volatility(c: &mut Criterion) {
    let cases = cases();
    let config = ImpliedVolatilityConfig::default();
    let mut group = group(c, "american_implied_volatility", &cases);
    for (grid, model) in models() {
        for case in &cases {
            implied_volatility(&model, &case.inputs, case.target_price, &config).unwrap_or_else(
                |error| panic!("grid {grid}: {:?} does not solve: {error}", case.inputs),
            );
        }
        group.bench_function(BenchmarkId::from_parameter(grid), |b| {
            b.iter(|| {
                for case in &cases {
                    black_box(
                        implied_volatility(
                            &model,
                            black_box(&case.inputs),
                            black_box(case.target_price),
                            &config,
                        )
                        .unwrap(),
                    );
                }
            })
        });
    }
    group.finish();
}
