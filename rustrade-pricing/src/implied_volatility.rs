//! Implied volatility: the volatility at which a model reproduces an option's price.

use crate::black_scholes::{bisect_volatility, discounted_forward, european_price};
use crate::inputs::{OptionInputs, PricingError};
use crate::model::OptionModel;
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};

/// Settings for [`implied_volatility`].
///
/// The defaults search volatilities from 1e-7 to 4.0 (400%) and stop once the price is reproduced
/// within 1e-8 of `max(1, price)`, or the volatility is pinned within 1e-8.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct ImpliedVolatilityConfig {
    /// Lowest volatility searched. Must be finite and positive.
    pub min_volatility: f64,
    /// Highest volatility searched. Must be finite and above `min_volatility`.
    pub max_volatility: f64,
    /// The solve stops once the volatility is known within this. Must be finite and positive.
    pub volatility_tolerance: f64,
    /// The solve stops once the model price is within this of the target, relative to
    /// `max(1, price)`. Also the slack the bound checks allow. Must be finite and positive.
    pub price_tolerance: f64,
    /// Most root-finding iterations before giving up with [`ImpliedVolatilityError::NoConvergence`].
    /// Each iteration prices the option once. Must be at least 1.
    pub max_iterations: usize,
}

impl Default for ImpliedVolatilityConfig {
    fn default() -> Self {
        Self {
            min_volatility: 1e-7,
            max_volatility: 4.0,
            volatility_tolerance: 1e-8,
            price_tolerance: 1e-8,
            max_iterations: 100,
        }
    }
}

impl ImpliedVolatilityConfig {
    /// Sets the lowest volatility searched.
    pub fn with_min_volatility(mut self, min_volatility: f64) -> Self {
        self.min_volatility = min_volatility;
        self
    }

    /// Sets the highest volatility searched.
    pub fn with_max_volatility(mut self, max_volatility: f64) -> Self {
        self.max_volatility = max_volatility;
        self
    }

    /// Sets the volatility tolerance.
    pub fn with_volatility_tolerance(mut self, volatility_tolerance: f64) -> Self {
        self.volatility_tolerance = volatility_tolerance;
        self
    }

    /// Sets the price tolerance, relative to `max(1, price)`.
    pub fn with_price_tolerance(mut self, price_tolerance: f64) -> Self {
        self.price_tolerance = price_tolerance;
        self
    }

    /// Sets the most root-finding iterations.
    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    fn validate(&self) -> Result<(), ImpliedVolatilityError> {
        let invalid = |reason| Err(ImpliedVolatilityError::InvalidConfig { reason });
        if !(self.min_volatility.is_finite() && self.min_volatility > 0.0) {
            return invalid("min_volatility must be finite and positive");
        }
        if !(self.max_volatility.is_finite() && self.max_volatility > self.min_volatility) {
            return invalid("max_volatility must be finite and above min_volatility");
        }
        if !(self.volatility_tolerance.is_finite() && self.volatility_tolerance > 0.0) {
            return invalid("volatility_tolerance must be finite and positive");
        }
        if !(self.price_tolerance.is_finite() && self.price_tolerance > 0.0) {
            return invalid("price_tolerance must be finite and positive");
        }
        if self.max_iterations == 0 {
            return invalid("max_iterations must be at least 1");
        }
        Ok(())
    }
}

/// Why no implied volatility was found for a price.
///
/// Each case says what is wrong with the price relative to the model, so a caller can tell a bad
/// quote from a model limit. Prices are in the option's currency.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ImpliedVolatilityError {
    /// The model could not price the option (invalid inputs, expired, unsupported exercise).
    #[error(transparent)]
    Pricing(#[from] PricingError),
    /// The price is negative or not finite.
    #[error("option price must be finite and not negative, got {price}")]
    InvalidPrice {
        /// The price given.
        price: f64,
    },
    /// The settings are invalid.
    #[error("invalid implied volatility settings: {reason}")]
    InvalidConfig {
        /// What is wrong.
        reason: &'static str,
    },
    /// The price is below the option's no-arbitrage floor, which usually means a stale or bad
    /// quote. For an American option the floor is its exercise value (`spot - strike` for a call,
    /// `strike - spot` for a put). For a European option, which cannot be exercised now, it is the
    /// discounted forward intrinsic value: the same difference with the spot replaced by the
    /// present value of the stock delivered at expiry (net of the yield and of the dividends paid
    /// before expiry), and the strike discounted to today.
    #[error("price {price} is below the no-arbitrage floor {intrinsic}")]
    BelowIntrinsic {
        /// The price given.
        price: f64,
        /// The option's no-arbitrage floor.
        intrinsic: f64,
    },
    /// The price is at or below the option's floor: the model's price at the lowest volatility
    /// searched, which is at least the no-arbitrage floor of
    /// [`BelowIntrinsic`](Self::BelowIntrinsic) (and above it when holding beats exercising, for
    /// example an American call when rates are positive). No volatility is determined: either
    /// none reproduces the price, or every volatility up to some level does, as for a deep
    /// in-the-money American put that should be exercised now, or any option deep enough in the
    /// money that its price does not move with volatility. Where vega is that small, a model's
    /// discretisation error can put even a price it produced itself at or below the floor (see
    /// [`FiniteDifferenceModel`](crate::FiniteDifferenceModel)'s accuracy): such a price carries no
    /// usable volatility either.
    #[error("price {price} is at the option's floor {floor}: volatility is undetermined")]
    AtIntrinsic {
        /// The price given.
        price: f64,
        /// The model's price at the lowest volatility searched.
        floor: f64,
    },
    /// The price is above what any volatility searched can give: above the spot for a call, the
    /// strike for an American put (its discounted strike for a European put), or the model's price
    /// at the highest volatility searched.
    #[error("price {price} is above the upper bound {bound}")]
    AboveUpperBound {
        /// The price given.
        price: f64,
        /// The bound it exceeds.
        bound: f64,
    },
    /// The solve did not settle within the iteration limit, or settled on a volatility where the
    /// model's price jumps rather than crosses the target.
    #[error(
        "no convergence after {iterations} iterations: volatility {volatility}, price residual {residual}"
    )]
    NoConvergence {
        /// Iterations run.
        iterations: usize,
        /// The best volatility found.
        volatility: f64,
        /// The model price at that volatility minus the target.
        residual: f64,
    },
}

/// The volatility at which `model` prices the option at `price`.
///
/// `price` is one number, for example a quote's mid; [`mid_price`](crate::mid_price) turns a bid
/// and ask into one, refusing a quote that has none.
///
/// # Method
/// 1. The inputs, the settings and the price are checked.
/// 2. The price is checked against the option's bounds before any search: below its no-arbitrage
///    floor gives [`BelowIntrinsic`](ImpliedVolatilityError::BelowIntrinsic); at or below the
///    model's price at the lowest volatility gives
///    [`AtIntrinsic`](ImpliedVolatilityError::AtIntrinsic); above the spot (call), the strike
///    (put) or the model's price at the highest volatility gives
///    [`AboveUpperBound`](ImpliedVolatilityError::AboveUpperBound).
/// 3. Brent's method searches the bracket. Its top is the European Black-Scholes implied volatility
///    of the price, with margin, when the model's price there is above the target (an American
///    option is worth at least its European counterpart, so that volatility is at or above the
///    answer); otherwise the highest volatility searched.
/// 4. The solve stops when the price is reproduced within `price_tolerance` (relative to
///    `max(1, price)`) or the bracket is narrower than `volatility_tolerance`. A result pinned by
///    the volatility tolerance is accepted if its price is within 1e-4 of `max(1, price)` of the
///    target, and otherwise refused with
///    [`NoConvergence`](ImpliedVolatilityError::NoConvergence): that happens only if the model's
///    price jumps.
///
/// The result is deterministic: the same model, inputs, price and settings give the same
/// volatility.
pub fn implied_volatility<M: OptionModel + ?Sized>(
    model: &M,
    inputs: &OptionInputs,
    price: f64,
    config: &ImpliedVolatilityConfig,
) -> Result<f64, ImpliedVolatilityError> {
    inputs.validate()?;
    config.validate()?;
    if !price.is_finite() || price < 0.0 {
        return Err(ImpliedVolatilityError::InvalidPrice { price });
    }
    let tolerance = config.price_tolerance * price.max(1.0);

    let intrinsic = lower_bound(inputs);
    if price < intrinsic - tolerance {
        return Err(ImpliedVolatilityError::BelowIntrinsic { price, intrinsic });
    }
    let bound = upper_bound(inputs);
    if price >= bound {
        return Err(ImpliedVolatilityError::AboveUpperBound { price, bound });
    }

    let low = config.min_volatility;
    let floor = model.price(inputs, low)?;
    if price <= floor + tolerance {
        return Err(ImpliedVolatilityError::AtIntrinsic { price, floor });
    }

    let (high, high_price) = bracket_top(model, inputs, price, config)?;
    if high_price < price - tolerance {
        return Err(ImpliedVolatilityError::AboveUpperBound {
            price,
            bound: high_price,
        });
    }

    brent(
        |volatility| {
            model
                .price(inputs, volatility)
                .map(|model_price| model_price - price)
        },
        (low, floor - price),
        (high, high_price - price),
        Tolerances {
            price: tolerance,
            jump: JUMP_TOLERANCE * price.max(1.0),
            volatility: config.volatility_tolerance,
        },
        config.max_iterations,
    )
}

/// No-arbitrage floor on the price: the exercise value for an American option, the discounted
/// forward intrinsic value for a European one.
fn lower_bound(inputs: &OptionInputs) -> f64 {
    if inputs.exercise != OptionExercise::European {
        return inputs.intrinsic_value();
    }
    let forward = stock_forward(inputs);
    let strike = inputs.strike * libm::exp(-inputs.rate * inputs.time_to_expiry);
    match inputs.kind {
        OptionKind::Call => (forward - strike).max(0.0),
        OptionKind::Put => (strike - forward).max(0.0),
    }
}

/// The present value of the stock delivered at expiry, net of the yield and of the dividends paid
/// before expiry.
fn stock_forward(inputs: &OptionInputs) -> f64 {
    discounted_forward(
        inputs.spot,
        inputs.time_to_expiry,
        inputs.rate,
        inputs.dividend_yield,
        inputs
            .dividends_before_expiry()
            .iter()
            .map(|dividend| (dividend.time, dividend.amount)),
    )
}

/// No-arbitrage ceiling on the price: the spot for a call, the strike for an American put, the
/// discounted strike for a European put.
fn upper_bound(inputs: &OptionInputs) -> f64 {
    match (inputs.kind, inputs.exercise) {
        (OptionKind::Call, _) => inputs.spot,
        (OptionKind::Put, OptionExercise::European) => {
            inputs.strike * libm::exp(-inputs.rate * inputs.time_to_expiry)
        }
        (OptionKind::Put, _) => inputs.strike,
    }
}

/// Margin over the European implied volatility for the bracket's top: the American model differs
/// from the European formula by its early-exercise premium (which only lowers the answer), by its
/// discretisation, and by the dividends being taken out of the spot for the European estimate.
const EUROPEAN_HINT_SCALE: f64 = 1.25;
const EUROPEAN_HINT_MARGIN: f64 = 0.05;

/// The top of the search bracket and the model's price there.
fn bracket_top<M: OptionModel + ?Sized>(
    model: &M,
    inputs: &OptionInputs,
    price: f64,
    config: &ImpliedVolatilityConfig,
) -> Result<(f64, f64), ImpliedVolatilityError> {
    if let Some(hint) = european_hint(inputs, price, config) {
        let top = (hint * EUROPEAN_HINT_SCALE + EUROPEAN_HINT_MARGIN).min(config.max_volatility);
        if top < config.max_volatility {
            let top_price = model.price(inputs, top)?;
            if top_price >= price {
                return Ok((top, top_price));
            }
        }
    }
    let top = config.max_volatility;
    Ok((top, model.price(inputs, top)?))
}

/// The European Black-Scholes implied volatility of `price`, with the dividends taken out of the
/// spot, if one exists in the searched range.
///
/// The spot given to the formula is the stock's discounted forward grown back by the yield, so the
/// formula's own yield discount recovers that forward exactly.
fn european_hint(
    inputs: &OptionInputs,
    price: f64,
    config: &ImpliedVolatilityConfig,
) -> Option<f64> {
    let spot = stock_forward(inputs) * libm::exp(inputs.dividend_yield * inputs.time_to_expiry);
    if spot <= 0.0 {
        return None;
    }
    let price_at = |volatility| {
        european_price(
            inputs.kind,
            spot,
            inputs.strike,
            inputs.time_to_expiry,
            inputs.rate,
            inputs.dividend_yield,
            volatility,
        )
    };
    bisect_volatility(
        price_at,
        price,
        config.min_volatility,
        config.max_volatility,
    )
}

/// How far from the target, relative to `max(1, price)`, a result pinned by the volatility
/// tolerance may still price before it counts as a jump rather than a root.
const JUMP_TOLERANCE: f64 = 1e-4;

/// Absolute tolerances for [`brent`].
#[derive(Debug, Clone, Copy)]
struct Tolerances {
    /// Stop once `|f|` is within this.
    price: f64,
    /// A result pinned by the volatility tolerance is refused when `|f|` is beyond this.
    jump: f64,
    /// Stop once the bracket is narrower than this.
    volatility: f64,
}

/// Brent's method for the root of `f` between `a` and `b`, given `f` at both with opposite signs
/// (or zero).
fn brent<F>(
    mut f: F,
    (mut a, mut fa): (f64, f64),
    (mut b, mut fb): (f64, f64),
    tolerances: Tolerances,
    max_iterations: usize,
) -> Result<f64, ImpliedVolatilityError>
where
    F: FnMut(f64) -> Result<f64, PricingError>,
{
    let tolerance = tolerances.price;
    let step = 0.5 * tolerances.volatility;
    if fa.abs() <= tolerance {
        return Ok(a);
    }
    if fb.abs() <= tolerance {
        return Ok(b);
    }
    // `c` is the previous iterate and the other end of the bracket [b, c].
    let (mut c, mut fc) = (a, fa);
    let mut d = b - a;
    let mut e = d;
    for iteration in 1..=max_iterations {
        if (fb > 0.0) == (fc > 0.0) {
            c = a;
            fc = fa;
            d = b - a;
            e = d;
        }
        if fc.abs() < fb.abs() {
            a = b;
            b = c;
            c = a;
            fa = fb;
            fb = fc;
            fc = fa;
        }
        let half_width = 0.5 * (c - b);
        if half_width.abs() <= step {
            return if fb.abs() <= tolerances.jump {
                Ok(b)
            } else {
                Err(ImpliedVolatilityError::NoConvergence {
                    iterations: iteration,
                    volatility: b,
                    residual: fb,
                })
            };
        }
        if e.abs() >= step && fa.abs() > fb.abs() {
            // Try inverse quadratic interpolation (secant when only two points are distinct).
            let s = fb / fa;
            let (mut p, mut q) = if a == c {
                (2.0 * half_width * s, 1.0 - s)
            } else {
                let q = fa / fc;
                let r = fb / fc;
                (
                    s * (2.0 * half_width * q * (q - r) - (b - a) * (r - 1.0)),
                    (q - 1.0) * (r - 1.0) * (s - 1.0),
                )
            };
            if p > 0.0 {
                q = -q;
            } else {
                p = -p;
            }
            let bounded = 3.0 * half_width * q - (step * q).abs();
            if 2.0 * p < bounded.min((e * q).abs()) {
                e = d;
                d = p / q;
            } else {
                d = half_width;
                e = d;
            }
        } else {
            d = half_width;
            e = d;
        }
        a = b;
        fa = fb;
        b += if d.abs() > step {
            d
        } else {
            step.copysign(half_width)
        };
        fb = f(b)?;
        if fb.abs() <= tolerance {
            return Ok(b);
        }
    }
    Err(ImpliedVolatilityError::NoConvergence {
        iterations: max_iterations,
        volatility: b,
        residual: fb,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    const TIGHT: Tolerances = Tolerances {
        price: 1e-14,
        jump: 1e-10,
        volatility: 1e-14,
    };

    fn quadratic_root(target: f64, max_iterations: usize) -> Result<f64, ImpliedVolatilityError> {
        let f = |x: f64| Ok(x * x - target);
        brent(
            f,
            (0.0, -target),
            (4.0, 16.0 - target),
            TIGHT,
            max_iterations,
        )
    }

    #[test]
    fn brent_finds_a_smooth_root() {
        let root = quadratic_root(2.0, 100).unwrap();
        assert!((root - 2f64.sqrt()).abs() < 1e-12, "{root}");
    }

    #[test]
    fn brent_accepts_an_end_that_already_fits() {
        assert_eq!(quadratic_root(0.0, 100), Ok(0.0));
        assert_eq!(quadratic_root(16.0, 100), Ok(4.0));
    }

    #[test]
    fn brent_gives_up_after_the_iteration_limit() {
        assert!(matches!(
            quadratic_root(2.0, 2),
            Err(ImpliedVolatilityError::NoConvergence { iterations: 2, .. })
        ));
    }

    #[test]
    fn brent_converges_superlinearly() {
        // Bisection needs about 48 halvings to pin a root in [0, 4] within 1e-14. Interpolation
        // needs a handful of evaluations on a smooth function, and its safeguards keep a flat or
        // steep one from doing worse than bisection. These counts pin that behaviour.
        // A function, its bracket, and the evaluations Brent takes.
        type Case = (fn(f64) -> f64, (f64, f64), usize);
        let cases: [Case; 8] = [
            (|x| x * x - 2.0, (0.0, 4.0), 9),
            (|x| x.exp() - 3.0, (0.0, 4.0), 9),
            (
                |x| (x - 1.0).powi(3) + 0.5 * (x - 1.0) - 0.3,
                (0.0, 4.0),
                11,
            ),
            (|x| (5.0 * (x - 1.3)).atan(), (0.0, 4.0), 8),
            (|x| x * x * x - 2.0 * x - 5.0, (2.0, 3.0), 5),
            // Flat around its root.
            (|x| (x - 1.0).powi(5), (0.0, 4.0), 25),
            // Steep: interpolation overshoots without the safeguards.
            (|x| (10.0 * x).exp() - 2.0, (0.0, 4.0), 8),
            (|x| (100.0 * (x - 1.7)).tanh(), (0.0, 4.0), 12),
        ];
        for (index, (g, (low, high), expected)) in cases.into_iter().enumerate() {
            let mut evaluations = 0;
            let f = |x: f64| {
                evaluations += 1;
                Ok(g(x))
            };
            let root = brent(f, (low, g(low)), (high, g(high)), TIGHT, 100).unwrap();
            assert!(g(root).abs() <= 1e-13, "case {index}: {root}");
            assert_eq!(evaluations, expected, "case {index}");
        }
    }

    #[test]
    fn brent_refuses_a_jump() {
        // A step function crosses zero without a root: the bracket shrinks onto the step.
        let f = |x: f64| Ok(if x < 1.0 { -1.0 } else { 1.0 });
        let tolerances = Tolerances {
            price: 1e-8,
            jump: 1e-4,
            volatility: 1e-8,
        };
        let result = brent(f, (0.0, -1.0), (4.0, 1.0), tolerances, 100);
        match result {
            Err(ImpliedVolatilityError::NoConvergence { volatility, .. }) => {
                assert!((volatility - 1.0).abs() < 1e-7, "{volatility}")
            }
            other => panic!("expected NoConvergence, got {other:?}"),
        }
    }

    #[test]
    fn config_setters_set_only_their_field() {
        let base = ImpliedVolatilityConfig::default();
        let cases = [
            (
                base.with_min_volatility(0.01),
                ImpliedVolatilityConfig {
                    min_volatility: 0.01,
                    ..base
                },
            ),
            (
                base.with_max_volatility(2.5),
                ImpliedVolatilityConfig {
                    max_volatility: 2.5,
                    ..base
                },
            ),
            (
                base.with_volatility_tolerance(1e-6),
                ImpliedVolatilityConfig {
                    volatility_tolerance: 1e-6,
                    ..base
                },
            ),
            (
                base.with_price_tolerance(1e-5),
                ImpliedVolatilityConfig {
                    price_tolerance: 1e-5,
                    ..base
                },
            ),
            (
                base.with_max_iterations(7),
                ImpliedVolatilityConfig {
                    max_iterations: 7,
                    ..base
                },
            ),
        ];
        for (set, expected) in cases {
            assert_eq!(set, expected);
            assert_ne!(set, base);
        }
    }

    #[test]
    fn config_validation_names_each_problem() {
        let base = ImpliedVolatilityConfig::default();
        assert_eq!(base.validate(), Ok(()));
        let cases = [
            ImpliedVolatilityConfig {
                min_volatility: 0.0,
                ..base
            },
            ImpliedVolatilityConfig {
                max_volatility: base.min_volatility,
                ..base
            },
            ImpliedVolatilityConfig {
                max_volatility: f64::INFINITY,
                ..base
            },
            ImpliedVolatilityConfig {
                volatility_tolerance: 0.0,
                ..base
            },
            ImpliedVolatilityConfig {
                price_tolerance: f64::NAN,
                ..base
            },
            ImpliedVolatilityConfig {
                price_tolerance: f64::INFINITY,
                ..base
            },
            ImpliedVolatilityConfig {
                price_tolerance: 0.0,
                ..base
            },
            ImpliedVolatilityConfig {
                max_iterations: 0,
                ..base
            },
        ];
        for case in cases {
            assert!(
                matches!(
                    case.validate(),
                    Err(ImpliedVolatilityError::InvalidConfig { .. })
                ),
                "{case:?}"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod model_tests {
    use super::*;
    use crate::finite_difference::FiniteDifferenceModel;
    use crate::inputs::CashDividend;
    use crate::model::Valuation;
    use OptionExercise::{American, European};
    use OptionKind::{Call, Put};
    use std::cell::RefCell;

    fn solve(inputs: &OptionInputs, price: f64) -> Result<f64, ImpliedVolatilityError> {
        implied_volatility(
            &FiniteDifferenceModel::default(),
            inputs,
            price,
            &ImpliedVolatilityConfig::default(),
        )
    }

    #[test]
    fn round_trips_across_options_and_volatilities() {
        let model = FiniteDifferenceModel::default();
        let dividends = vec![CashDividend::new(0.1, 0.8), CashDividend::new(0.35, 0.8)];
        for kind in [Call, Put] {
            for exercise in [American, European] {
                for strike in [80.0, 100.0, 115.0] {
                    for volatility in [0.05, 0.25, 0.8, 2.0] {
                        let inputs = OptionInputs::new(kind, exercise, 100.0, strike, 0.5, 0.04)
                            .with_dividends(dividends.clone());
                        let price = model.price(&inputs, volatility).unwrap();
                        let label = format!("{kind} {exercise} K{strike} vol {volatility}");
                        match solve(&inputs, price) {
                            Ok(solved) => {
                                // The solve stops once the price is within its tolerance or the
                                // volatility within 1e-8, so the volatility is found within the
                                // larger of 1e-8 and the price tolerance over vega. Deep in the
                                // money at low volatility vega vanishes: any volatility that
                                // reproduces the price fits.
                                let tolerance = 1e-8 * price.max(1.0);
                                let vega = model.vega(&inputs, volatility).unwrap();
                                if vega > 1e-3 {
                                    let allowed = 2.0 * tolerance / vega + 1e-8;
                                    assert!(
                                        (solved - volatility).abs() <= allowed,
                                        "{label}: {solved}"
                                    );
                                } else {
                                    let repriced = model.price(&inputs, solved).unwrap();
                                    assert!((repriced - price).abs() <= tolerance, "{label}");
                                }
                            }
                            // Deep in the money at low volatility the price sits on its floor,
                            // where no single volatility fits.
                            Err(ImpliedVolatilityError::AtIntrinsic { .. }) => {
                                assert!(model.vega(&inputs, volatility).unwrap() < 1e-3, "{label}")
                            }
                            Err(other) => panic!("{label}: {other:?}"),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn refuses_a_price_below_the_exercise_value() {
        let inputs = OptionInputs::new(Put, American, 90.0, 100.0, 0.5, 0.04);
        assert_eq!(
            solve(&inputs, 9.5),
            Err(ImpliedVolatilityError::BelowIntrinsic {
                price: 9.5,
                intrinsic: 10.0
            })
        );
    }

    #[test]
    fn a_european_floor_is_its_discounted_forward_intrinsic_value() {
        // Below spot - strike is no arbitrage for a European call on a dividend payer; below the
        // discounted forward intrinsic value is.
        let inputs = OptionInputs::new(Call, European, 100.0, 80.0, 0.5, 0.04)
            .with_dividends(vec![CashDividend::new(0.25, 2.0)]);
        let floor = 100.0 - 2.0 * (-0.04f64 * 0.25).exp() - 80.0 * (-0.04f64 * 0.5).exp();
        assert!((lower_bound(&inputs) - floor).abs() < 1e-12);
        assert!(floor < 20.0);
        match solve(&inputs, floor - 0.01) {
            Err(ImpliedVolatilityError::BelowIntrinsic { intrinsic, .. }) => {
                assert!((intrinsic - floor).abs() < 1e-12)
            }
            other => panic!("expected BelowIntrinsic, got {other:?}"),
        }
        let put = OptionInputs {
            kind: Put,
            ..inputs
        };
        assert_eq!(lower_bound(&put), 0.0);
    }

    #[test]
    fn a_european_floor_counts_the_yield_lost_on_its_dividends() {
        // A dividend paid at t leaves the stock with that amount less, which would have earned the
        // stock's growth r - q until expiry: its present value is D·exp(-r·t - q·(T - t)).
        let (s, k, t, r, q) = (100.0, 110.0, 0.75, 0.05, 0.03);
        let (paid_at, amount) = (0.25, 2.0);
        let put = OptionInputs::new(Put, European, s, k, t, r)
            .with_dividend_yield(q)
            .with_dividends(vec![CashDividend::new(paid_at, amount)]);
        let forward = s * (-q * t).exp() - amount * (-r * paid_at - q * (t - paid_at)).exp();
        let floor = k * (-r * t).exp() - forward;
        assert!((lower_bound(&put) - floor).abs() < 1e-12);
        // Just above the floor is a valid price, and solves.
        let price = floor + 0.01;
        assert!(solve(&put, price).is_ok());
        let call = OptionInputs {
            kind: Call,
            strike: 80.0,
            ..put
        };
        let call_floor = forward - 80.0 * (-r * t).exp();
        assert!((lower_bound(&call) - call_floor).abs() < 1e-12);
    }

    #[test]
    fn the_price_tolerance_is_relative_to_the_price() {
        // 1e-3 of a price near 50 is 0.05: a deep in-the-money put 0.01 above its exercise value
        // is at its floor.
        let inputs = OptionInputs::new(Put, American, 50.0, 100.0, 0.25, 0.05);
        let config = ImpliedVolatilityConfig {
            price_tolerance: 1e-3,
            ..ImpliedVolatilityConfig::default()
        };
        assert!(matches!(
            implied_volatility(&FiniteDifferenceModel::default(), &inputs, 50.01, &config),
            Err(ImpliedVolatilityError::AtIntrinsic { .. })
        ));
    }

    #[test]
    fn the_slack_below_the_exercise_value_is_inclusive() {
        // A put 0.75 in the money, priced exactly the tolerance of 0.25 below that: within the
        // slack, so the price is at the floor rather than below it.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.75, 0.5, 0.04);
        let config = ImpliedVolatilityConfig {
            price_tolerance: 0.25,
            ..ImpliedVolatilityConfig::default()
        };
        assert!(matches!(
            implied_volatility(&FiniteDifferenceModel::default(), &inputs, 0.5, &config),
            Err(ImpliedVolatilityError::AtIntrinsic { .. })
        ));
    }

    #[test]
    fn a_price_at_the_floor_has_no_volatility() {
        // A deep in-the-money American put priced at its exercise value.
        let deep = OptionInputs::new(Put, American, 50.0, 100.0, 0.25, 0.05);
        assert!(matches!(
            solve(&deep, 50.0),
            Err(ImpliedVolatilityError::AtIntrinsic { price: 50.0, .. })
        ));
        // A far out-of-the-money option priced at zero.
        let far = OptionInputs::new(Call, American, 100.0, 300.0, 0.1, 0.05);
        assert!(matches!(
            solve(&far, 0.0),
            Err(ImpliedVolatilityError::AtIntrinsic { price: 0.0, .. })
        ));
        // A call between its exercise value and its zero-volatility value, S - K·exp(-rT).
        let call = OptionInputs::new(Call, American, 100.0, 90.0, 1.0, 0.05);
        let floor = 100.0 - 90.0 * (-0.05f64).exp();
        match solve(&call, 11.0) {
            Err(ImpliedVolatilityError::AtIntrinsic { floor: found, .. }) => {
                assert!((found - floor).abs() < 1e-6, "{found} vs {floor}")
            }
            other => panic!("expected AtIntrinsic, got {other:?}"),
        }
    }

    #[test]
    fn refuses_a_price_above_the_bounds() {
        let call = OptionInputs::new(Call, American, 100.0, 100.0, 0.5, 0.04);
        assert_eq!(
            solve(&call, 100.0),
            Err(ImpliedVolatilityError::AboveUpperBound {
                price: 100.0,
                bound: 100.0
            })
        );
        let american_put = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        assert_eq!(
            solve(&american_put, 100.0),
            Err(ImpliedVolatilityError::AboveUpperBound {
                price: 100.0,
                bound: 100.0
            })
        );
        let european_put = OptionInputs {
            exercise: European,
            ..american_put
        };
        let discounted = 100.0 * (-0.04f64 * 0.5).exp();
        assert_eq!(
            solve(&european_put, 99.0),
            Err(ImpliedVolatilityError::AboveUpperBound {
                price: 99.0,
                bound: discounted
            })
        );
        // Below the spot but above what 400% volatility gives.
        let short = OptionInputs::new(Call, American, 100.0, 100.0, 0.01, 0.04);
        match solve(&short, 60.0) {
            Err(ImpliedVolatilityError::AboveUpperBound { bound, .. }) => {
                assert!(bound < 60.0, "{bound}")
            }
            other => panic!("expected AboveUpperBound, got {other:?}"),
        }
    }

    #[test]
    fn refuses_invalid_prices_inputs_and_settings() {
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        for price in [-0.1, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                solve(&inputs, price),
                Err(ImpliedVolatilityError::InvalidPrice { .. })
            ));
        }
        let expired = OptionInputs {
            time_to_expiry: 0.0,
            ..inputs.clone()
        };
        assert!(matches!(
            solve(&expired, 1.0),
            Err(ImpliedVolatilityError::Pricing(
                PricingError::Expired { .. }
            ))
        ));
        let config = ImpliedVolatilityConfig {
            max_iterations: 0,
            ..ImpliedVolatilityConfig::default()
        };
        assert!(matches!(
            implied_volatility(&FiniteDifferenceModel::default(), &inputs, 5.0, &config),
            Err(ImpliedVolatilityError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn iteration_limit_is_reported() {
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let config = ImpliedVolatilityConfig {
            max_iterations: 1,
            ..ImpliedVolatilityConfig::default()
        };
        assert!(matches!(
            implied_volatility(&FiniteDifferenceModel::default(), &inputs, 6.0, &config),
            Err(ImpliedVolatilityError::NoConvergence { iterations: 1, .. })
        ));
    }

    /// A model whose price is `slope · volatility`, to drive the solver without a grid.
    struct Linear {
        slope: f64,
    }

    impl OptionModel for Linear {
        fn value(
            &self,
            _inputs: &OptionInputs,
            volatility: f64,
        ) -> Result<Valuation, PricingError> {
            Ok(Valuation {
                price: self.slope * volatility,
                delta: 0.0,
                gamma: 0.0,
                theta: 0.0,
            })
        }
    }

    #[test]
    fn works_with_any_model() {
        // An at-the-money put has no exercise value, so only the model's prices bound it.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let model = Linear { slope: 10.0 };
        let solved =
            implied_volatility(&model, &inputs, 3.0, &ImpliedVolatilityConfig::default()).unwrap();
        assert!((solved - 0.3).abs() < 1e-9, "{solved}");
    }

    #[test]
    fn falls_back_to_the_widest_bracket_when_the_european_hint_is_too_low() {
        // The European estimate of 2.0 at the money is about 0.29; a model this flat needs 0.6,
        // above the narrowed bracket's top, so the solver must widen to the configured maximum.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let model = Linear { slope: 10.0 / 3.0 };
        let solved =
            implied_volatility(&model, &inputs, 2.0, &ImpliedVolatilityConfig::default()).unwrap();
        assert!((solved - 0.6).abs() < 1e-9, "{solved}");
    }

    #[test]
    fn a_price_within_tolerance_of_the_range_top_solves_to_it() {
        // The model gives 3.0 at the top of the range, exactly the tolerance of 1.0 below the
        // price: the top fits.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let model = Linear { slope: 0.75 };
        let config = ImpliedVolatilityConfig {
            price_tolerance: 0.25,
            ..ImpliedVolatilityConfig::default()
        };
        assert_eq!(implied_volatility(&model, &inputs, 4.0, &config), Ok(4.0));
    }

    /// A model whose price rises with volatility and jumps by `2·jump` at 0.5.
    struct Stepped {
        jump: f64,
    }

    impl OptionModel for Stepped {
        fn value(
            &self,
            _inputs: &OptionInputs,
            volatility: f64,
        ) -> Result<Valuation, PricingError> {
            let side = if volatility < 0.5 { -1.0 } else { 1.0 };
            Ok(Valuation {
                price: 10.0 + 2.0 * (volatility - 0.5) + side * self.jump,
                delta: 0.0,
                gamma: 0.0,
                theta: 0.0,
            })
        }
    }

    #[test]
    fn the_jump_tolerance_is_relative_to_the_price() {
        // The solve ends pinned at the jump, missing a price of 10 by `jump`; it accepts a miss
        // within 1e-4 of the price (1e-3 here) and refuses a larger one.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let config = ImpliedVolatilityConfig::default();
        let small = implied_volatility(&Stepped { jump: 5e-4 }, &inputs, 10.0, &config).unwrap();
        assert!((small - 0.5).abs() < 1e-7, "{small}");
        assert!(matches!(
            implied_volatility(&Stepped { jump: 2e-3 }, &inputs, 10.0, &config),
            Err(ImpliedVolatilityError::NoConvergence { .. })
        ));
    }

    /// Wraps a model and records each volatility it prices.
    struct Recording<M> {
        inner: M,
        priced: RefCell<Vec<f64>>,
    }

    impl<M> Recording<M> {
        fn new(inner: M) -> Self {
            Self {
                inner,
                priced: RefCell::new(Vec::new()),
            }
        }

        fn priced(&self) -> Vec<f64> {
            self.priced.borrow().clone()
        }
    }

    impl<M: OptionModel> OptionModel for Recording<M> {
        fn value(&self, inputs: &OptionInputs, volatility: f64) -> Result<Valuation, PricingError> {
            self.priced.borrow_mut().push(volatility);
            self.inner.value(inputs, volatility)
        }
    }

    #[test]
    fn the_bracket_top_is_the_widened_european_estimate() {
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let config = ImpliedVolatilityConfig::default();
        let price = FiniteDifferenceModel::default()
            .price(&inputs, 0.3)
            .unwrap();
        let hint = european_hint(&inputs, price, &config).unwrap();
        let model = Recording::new(FiniteDifferenceModel::default());
        let (top, top_price) = bracket_top(&model, &inputs, price, &config).unwrap();
        assert_eq!(top, hint * EUROPEAN_HINT_SCALE + EUROPEAN_HINT_MARGIN);
        assert!(top_price >= price);
        assert_eq!(model.priced(), vec![top]);
    }

    #[test]
    fn a_bracket_top_clamped_to_the_range_is_priced_once() {
        // The widened estimate is above the range's top, which then prices below the target:
        // the top is priced once, not once as the estimate and again as the fallback.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04);
        let config = ImpliedVolatilityConfig {
            max_volatility: 0.12,
            ..ImpliedVolatilityConfig::default()
        };
        let price = 2.0;
        let hint = european_hint(&inputs, price, &config).unwrap();
        assert!(
            hint * EUROPEAN_HINT_SCALE + EUROPEAN_HINT_MARGIN >= 0.12,
            "{hint}"
        );
        let model = Recording::new(Linear { slope: 10.0 });
        let (top, top_price) = bracket_top(&model, &inputs, price, &config).unwrap();
        assert_eq!((top, top_price), (0.12, 1.2));
        assert_eq!(model.priced(), vec![0.12]);
    }

    #[test]
    fn solves_in_few_model_evaluations() {
        // Each evaluation is a full grid solve; these counts pin the bracket and Brent's rate.
        let dividends = vec![CashDividend::new(0.1, 0.8), CashDividend::new(0.35, 0.8)];
        let cases = [
            (
                OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.04),
                0.3,
                5,
            ),
            (
                OptionInputs::new(Call, American, 100.0, 115.0, 0.5, 0.04)
                    .with_dividends(dividends),
                0.25,
                8,
            ),
            (
                OptionInputs::new(Put, European, 100.0, 115.0, 0.5, 0.04),
                0.8,
                6,
            ),
        ];
        for (inputs, volatility, most) in cases {
            let price = FiniteDifferenceModel::default()
                .price(&inputs, volatility)
                .unwrap();
            let model = Recording::new(FiniteDifferenceModel::default());
            let solved =
                implied_volatility(&model, &inputs, price, &ImpliedVolatilityConfig::default())
                    .unwrap();
            assert!(
                (solved - volatility).abs() < 1e-6,
                "{solved} vs {volatility}"
            );
            let evaluations = model.priced().len();
            assert!(
                evaluations <= most,
                "{evaluations} evaluations, expected at most {most}"
            );
        }
    }

    #[test]
    fn upper_bound_by_kind_and_exercise() {
        let call = OptionInputs::new(Call, European, 100.0, 90.0, 1.0, 0.05);
        assert_eq!(upper_bound(&call), 100.0);
        let american_put = OptionInputs::new(Put, American, 100.0, 90.0, 1.0, 0.05);
        assert_eq!(upper_bound(&american_put), 90.0);
        let european_put = OptionInputs {
            exercise: European,
            ..american_put
        };
        assert_eq!(upper_bound(&european_put), 90.0 * (-0.05f64).exp());
    }

    #[test]
    fn european_hint_takes_the_dividends_out_of_the_spot() {
        let config = ImpliedVolatilityConfig::default();
        let plain = OptionInputs::new(Call, European, 100.0, 100.0, 1.0, 0.0);
        let with_dividend = plain
            .clone()
            .with_dividends(vec![CashDividend::new(0.5, 5.0)]);
        let price = 8.0;
        let plain_hint = european_hint(&plain, price, &config).unwrap();
        let dividend_hint = european_hint(&with_dividend, price, &config).unwrap();
        // A lower spot needs a higher volatility for the same call price.
        assert!(
            dividend_hint > plain_hint,
            "{dividend_hint} vs {plain_hint}"
        );
        // With a yield and no dividends, the estimate is the European volatility itself.
        let yielding =
            OptionInputs::new(Call, European, 100.0, 95.0, 0.75, 0.04).with_dividend_yield(0.03);
        let european = european_price(Call, 100.0, 95.0, 0.75, 0.04, 0.03, 0.35);
        let hint = european_hint(&yielding, european, &config).unwrap();
        assert!((hint - 0.35).abs() < 1e-10, "{hint}");
        // Dividends worth more than the spot leave no estimate.
        let swamped = plain.with_dividends(vec![CashDividend::new(0.5, 150.0)]);
        assert_eq!(european_hint(&swamped, price, &config), None);
    }
}
