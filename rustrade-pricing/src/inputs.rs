//! Contract and market inputs to an option model, and the errors their validation raises.

use chrono::{DateTime, Utc};
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};

/// Seconds in the 365-day year that [`year_fraction`] divides by.
const SECONDS_PER_YEAR: f64 = 365.0 * 86_400.0;

/// The time from `start` to `end` in years of 365 days, counting every calendar second.
///
/// This is the convention every input in this crate uses: [`OptionInputs::time_to_expiry`] and
/// each [`CashDividend::time`]. Pass the exact instants, the expiry's time of day included (for
/// example 16:00 New York for a US equity option on its last trading day), so an option's last
/// hours are not rounded away. The result is negative when `end` is before `start`.
pub fn year_fraction(start: DateTime<Utc>, end: DateTime<Utc>) -> f64 {
    let delta = end - start;
    let seconds = delta.num_seconds() as f64;
    let nanos = f64::from(delta.subsec_nanos());
    (seconds + nanos * 1e-9) / SECONDS_PER_YEAR
}

/// A known cash dividend on the underlying: the stock's price drops by `amount` at `time`.
///
/// `time` is the ex-dividend instant in years from the valuation instant (see [`year_fraction`]).
/// The drop happens there, so pass the instant the stock first trades without the dividend, for
/// example the ex-date's market open.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CashDividend {
    /// Ex-dividend instant, in years from the valuation instant.
    pub time: f64,
    /// Cash amount per share, in the same currency as the spot and strike.
    pub amount: f64,
}

impl CashDividend {
    /// A dividend of `amount` going ex at `time` years from the valuation instant.
    pub fn new(time: f64, amount: f64) -> Self {
        Self { time, amount }
    }
}

/// Contract and market inputs to an option model, everything but the volatility.
///
/// Rates and yields are continuously compounded, per year. Times are in years of 365 days (see
/// [`year_fraction`]).
///
/// # Dividends
/// Discrete cash dividends go in [`dividends`](Self::dividends); a continuous yield (for an index
/// or a fund, or a borrow cost) goes in [`dividend_yield`](Self::dividend_yield). Both may be
/// used at once. Dividends need not be sorted. A dividend whose `time` is not strictly between the
/// valuation instant and expiry has no effect: one at or before 0 is already in the spot price,
/// and one at or after expiry falls after the option ends.
///
/// When the stock is worth less than a dividend at its ex-date, it pays what it is worth and
/// falls to zero.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionInputs {
    /// Call or put.
    pub kind: OptionKind,
    /// Exercise style. Models in this crate price [`OptionExercise::American`] and
    /// [`OptionExercise::European`]; [`OptionExercise::Bermudan`] is refused.
    pub exercise: OptionExercise,
    /// Underlying price at the valuation instant. Must be finite and positive.
    pub spot: f64,
    /// Strike. Must be finite and positive.
    pub strike: f64,
    /// Years from the valuation instant to expiry. Must be finite and positive: an option at or
    /// past expiry is refused with [`PricingError::Expired`].
    pub time_to_expiry: f64,
    /// Risk-free rate, continuously compounded. Must be finite; zero and negative are allowed.
    pub rate: f64,
    /// Continuous dividend yield (or borrow cost), continuously compounded. Must be finite; zero
    /// and negative are allowed.
    pub dividend_yield: f64,
    /// Discrete cash dividends. Each `amount` must be finite and not negative, each `time` finite.
    pub dividends: Vec<CashDividend>,
}

impl OptionInputs {
    /// Inputs with no dividends and no yield.
    pub fn new(
        kind: OptionKind,
        exercise: OptionExercise,
        spot: f64,
        strike: f64,
        time_to_expiry: f64,
        rate: f64,
    ) -> Self {
        Self {
            kind,
            exercise,
            spot,
            strike,
            time_to_expiry,
            rate,
            dividend_yield: 0.0,
            dividends: Vec::new(),
        }
    }

    /// Sets the continuous dividend yield.
    pub fn with_dividend_yield(mut self, dividend_yield: f64) -> Self {
        self.dividend_yield = dividend_yield;
        self
    }

    /// Sets the discrete cash dividends.
    pub fn with_dividends(mut self, dividends: Vec<CashDividend>) -> Self {
        self.dividends = dividends;
        self
    }

    /// The value of exercising now: `spot - strike` for a call, `strike - spot` for a put, or zero
    /// if that is negative.
    pub fn intrinsic_value(&self) -> f64 {
        payoff(self.kind, self.spot, self.strike)
    }

    /// Checks every input, returning the first that is invalid.
    ///
    /// Models call this before pricing; it is public so a caller can validate inputs once, up
    /// front.
    pub fn validate(&self) -> Result<(), PricingError> {
        if self.exercise == OptionExercise::Bermudan {
            return Err(PricingError::UnsupportedExercise(self.exercise));
        }
        require_positive("spot", self.spot)?;
        require_positive("strike", self.strike)?;
        if self.time_to_expiry.is_nan() || self.time_to_expiry.is_infinite() {
            return Err(PricingError::InvalidInput {
                name: "time_to_expiry",
                value: self.time_to_expiry,
                requirement: "finite",
            });
        }
        if self.time_to_expiry <= 0.0 {
            return Err(PricingError::Expired {
                time_to_expiry: self.time_to_expiry,
            });
        }
        require_finite("rate", self.rate)?;
        require_finite("dividend_yield", self.dividend_yield)?;
        for dividend in &self.dividends {
            require_finite("dividend time", dividend.time)?;
            if !dividend.amount.is_finite() || dividend.amount < 0.0 {
                return Err(PricingError::InvalidInput {
                    name: "dividend amount",
                    value: dividend.amount,
                    requirement: "finite and not negative",
                });
            }
        }
        Ok(())
    }

    /// The dividends that fall strictly between now and expiry, sorted by time, with zero
    /// amounts dropped.
    pub(crate) fn dividends_before_expiry(&self) -> Vec<CashDividend> {
        let mut dividends: Vec<CashDividend> = self
            .dividends
            .iter()
            .copied()
            .filter(|dividend| {
                dividend.time > 0.0 && dividend.time < self.time_to_expiry && dividend.amount > 0.0
            })
            .collect();
        dividends.sort_by(|a, b| {
            a.time
                .total_cmp(&b.time)
                .then(a.amount.total_cmp(&b.amount))
        });
        dividends
    }
}

/// The value of exercising an option of `kind` struck at `strike` when the underlying is at `spot`.
pub(crate) fn payoff(kind: OptionKind, spot: f64, strike: f64) -> f64 {
    match kind {
        OptionKind::Call => (spot - strike).max(0.0),
        OptionKind::Put => (strike - spot).max(0.0),
    }
}

/// Checks a volatility passed to a model.
pub(crate) fn validate_volatility(volatility: f64) -> Result<(), PricingError> {
    require_positive("volatility", volatility)
}

fn require_positive(name: &'static str, value: f64) -> Result<(), PricingError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(PricingError::InvalidInput {
            name,
            value,
            requirement: "finite and positive",
        })
    }
}

fn require_finite(name: &'static str, value: f64) -> Result<(), PricingError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(PricingError::InvalidInput {
            name,
            value,
            requirement: "finite",
        })
    }
}

/// Why a model could not price an option.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PricingError {
    /// An input is out of its domain (for example a non-positive spot, or a NaN rate).
    #[error("{name} must be {requirement}, got {value}")]
    InvalidInput {
        /// The input's name.
        name: &'static str,
        /// The value given.
        value: f64,
        /// What the input must be.
        requirement: &'static str,
    },
    /// The option is at or past expiry (`time_to_expiry <= 0`), where it is worth its exercise
    /// value and has no volatility to price.
    #[error("option is at or past expiry (time_to_expiry = {time_to_expiry})")]
    Expired {
        /// The time to expiry given, in years.
        time_to_expiry: f64,
    },
    /// The model does not price this exercise style.
    #[error("exercise style {0} is not supported")]
    UnsupportedExercise(OptionExercise),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn inputs() -> OptionInputs {
        OptionInputs::new(
            OptionKind::Put,
            OptionExercise::American,
            100.0,
            95.0,
            0.5,
            0.03,
        )
    }

    #[test]
    fn year_fraction_counts_seconds_over_365_days() {
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(year_fraction(start, end), 1.0);

        let half_day = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(year_fraction(start, half_day), 0.5 / 365.0);
        assert_eq!(year_fraction(half_day, start), -0.5 / 365.0);

        let plus_half_second = start + chrono::Duration::milliseconds(500);
        assert_eq!(
            year_fraction(start, plus_half_second),
            0.5 / SECONDS_PER_YEAR
        );
    }

    #[test]
    fn intrinsic_value_by_kind() {
        let put = inputs();
        assert_eq!(put.intrinsic_value(), 0.0);
        let call = OptionInputs {
            kind: OptionKind::Call,
            ..put
        };
        assert_eq!(call.intrinsic_value(), 5.0);
    }

    #[test]
    fn validate_accepts_valid_inputs() {
        let valid = inputs()
            .with_dividend_yield(-0.01)
            .with_dividends(vec![CashDividend::new(-1.0, 0.0)]);
        assert_eq!(valid.validate(), Ok(()));
    }

    #[test]
    fn validate_refuses_each_invalid_input() {
        let cases: Vec<(OptionInputs, &str)> = vec![
            (
                OptionInputs {
                    spot: 0.0,
                    ..inputs()
                },
                "spot",
            ),
            (
                OptionInputs {
                    spot: f64::INFINITY,
                    ..inputs()
                },
                "spot",
            ),
            (
                OptionInputs {
                    strike: -1.0,
                    ..inputs()
                },
                "strike",
            ),
            (
                OptionInputs {
                    time_to_expiry: f64::NAN,
                    ..inputs()
                },
                "time_to_expiry",
            ),
            (
                OptionInputs {
                    time_to_expiry: f64::INFINITY,
                    ..inputs()
                },
                "time_to_expiry",
            ),
            (
                OptionInputs {
                    rate: f64::NAN,
                    ..inputs()
                },
                "rate",
            ),
            (
                inputs().with_dividend_yield(f64::INFINITY),
                "dividend_yield",
            ),
            (
                inputs().with_dividends(vec![CashDividend::new(f64::NAN, 1.0)]),
                "dividend time",
            ),
            (
                inputs().with_dividends(vec![CashDividend::new(0.1, -0.5)]),
                "dividend amount",
            ),
            (
                inputs().with_dividends(vec![CashDividend::new(0.1, f64::NAN)]),
                "dividend amount",
            ),
        ];
        for (case, expected) in cases {
            match case.validate() {
                Err(PricingError::InvalidInput { name, .. }) => assert_eq!(name, expected),
                other => panic!("{expected}: expected InvalidInput, got {other:?}"),
            }
        }
    }

    #[test]
    fn validate_refuses_expired_and_bermudan() {
        let expired = OptionInputs {
            time_to_expiry: 0.0,
            ..inputs()
        };
        assert_eq!(
            expired.validate(),
            Err(PricingError::Expired {
                time_to_expiry: 0.0
            })
        );
        let bermudan = OptionInputs {
            exercise: OptionExercise::Bermudan,
            ..inputs()
        };
        assert_eq!(
            bermudan.validate(),
            Err(PricingError::UnsupportedExercise(OptionExercise::Bermudan))
        );
    }

    #[test]
    fn validate_volatility_requires_finite_positive() {
        assert_eq!(validate_volatility(0.2), Ok(()));
        for bad in [0.0, -0.1, f64::NAN, f64::INFINITY] {
            assert!(validate_volatility(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn dividends_before_expiry_filters_and_sorts() {
        let inputs = inputs().with_dividends(vec![
            CashDividend::new(0.4, 1.0),
            CashDividend::new(0.0, 1.0),
            CashDividend::new(0.5, 1.0),
            CashDividend::new(0.1, 0.0),
            CashDividend::new(0.2, 2.0),
            CashDividend::new(0.2, 0.5),
        ]);
        assert_eq!(
            inputs.dividends_before_expiry(),
            vec![
                CashDividend::new(0.2, 0.5),
                CashDividend::new(0.2, 2.0),
                CashDividend::new(0.4, 1.0),
            ]
        );
    }
}
