//! The [`OptionModel`] trait every pricing model implements, and the values it returns.

use crate::inputs::{OptionInputs, PricingError};

/// A model's price for an option and the sensitivities it gets from the same solve.
///
/// Units are per unit of each input, not scaled for display:
/// - `delta`: change in price per 1.0 change in spot.
/// - `gamma`: change in delta per 1.0 change in spot.
/// - `theta`: change in price per year of calendar time passing (negative when the option loses
///   value as time passes). Divide by 365 for a per-day figure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Valuation {
    /// Option price.
    pub price: f64,
    /// ∂price/∂spot.
    pub delta: f64,
    /// ∂²price/∂spot².
    pub gamma: f64,
    /// ∂price/∂t, per year.
    pub theta: f64,
}

/// A [`Valuation`] plus the sensitivities to volatility and rate.
///
/// Units are per unit of each input: `vega` is the change in price per 1.0 (100 percentage points)
/// change in volatility, and `rho` per 1.0 change in the rate. Multiply by 0.01 for "per vol point"
/// or "per 1%" figures.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Greeks {
    /// Option price.
    pub price: f64,
    /// ∂price/∂spot.
    pub delta: f64,
    /// ∂²price/∂spot².
    pub gamma: f64,
    /// ∂price/∂t, per year.
    pub theta: f64,
    /// ∂price/∂volatility.
    pub vega: f64,
    /// ∂price/∂rate.
    pub rho: f64,
}

/// The volatility bump [`OptionModel`]'s default `vega` uses: 1e-4, or half the volatility if
/// that is smaller, so the lower bump stays positive.
pub(crate) fn volatility_bump(volatility: f64) -> f64 {
    VOLATILITY_BUMP.min(0.5 * volatility)
}

const VOLATILITY_BUMP: f64 = 1e-4;

/// The rate bump [`OptionModel`]'s default `rho` uses.
pub(crate) const RATE_BUMP: f64 = 1e-4;

/// An option pricing model: a price and its sensitivities from contract and market inputs and a
/// volatility.
///
/// The [implied-volatility solver](crate::implied_volatility()) works with any implementation, so
/// a user can plug in their own model and keep the rest of this crate.
///
/// # Contract for implementations
/// - Validate the inputs ([`OptionInputs::validate`]) and the volatility, and return an error
///   rather than a NaN.
/// - Be deterministic: the same inputs and volatility give bit-identical results.
/// - Never price an American option below its exercise value, nor any option below zero.
/// - The price should increase with volatility. The solver brackets a root and needs the price to
///   cross the target once.
pub trait OptionModel {
    /// The price, delta, gamma and theta at `volatility`.
    fn value(&self, inputs: &OptionInputs, volatility: f64) -> Result<Valuation, PricingError>;

    /// The price alone. Defaults to [`value`](Self::value)'s price.
    fn price(&self, inputs: &OptionInputs, volatility: f64) -> Result<f64, PricingError> {
        self.value(inputs, volatility)
            .map(|valuation| valuation.price)
    }

    /// ∂price/∂volatility, per 1.0 of volatility.
    ///
    /// The default is a central difference of [`price`](Self::price), bumping volatility by 1e-4
    /// (or by half of it, when the volatility is below 2e-4).
    fn vega(&self, inputs: &OptionInputs, volatility: f64) -> Result<f64, PricingError> {
        crate::inputs::validate_volatility(volatility)?;
        let bump = volatility_bump(volatility);
        let up = self.price(inputs, volatility + bump)?;
        let down = self.price(inputs, volatility - bump)?;
        Ok((up - down) / (2.0 * bump))
    }

    /// ∂price/∂rate, per 1.0 of rate.
    ///
    /// The default is a central difference of [`price`](Self::price), bumping the rate by 1e-4
    /// and holding the dividend yield fixed.
    fn rho(&self, inputs: &OptionInputs, volatility: f64) -> Result<f64, PricingError> {
        let up = self.price(
            &OptionInputs {
                rate: inputs.rate + RATE_BUMP,
                ..inputs.clone()
            },
            volatility,
        )?;
        let down = self.price(
            &OptionInputs {
                rate: inputs.rate - RATE_BUMP,
                ..inputs.clone()
            },
            volatility,
        )?;
        Ok((up - down) / (2.0 * RATE_BUMP))
    }

    /// Every sensitivity at once: [`value`](Self::value) plus [`vega`](Self::vega) and
    /// [`rho`](Self::rho).
    fn greeks(&self, inputs: &OptionInputs, volatility: f64) -> Result<Greeks, PricingError> {
        let valuation = self.value(inputs, volatility)?;
        Ok(Greeks {
            price: valuation.price,
            delta: valuation.delta,
            gamma: valuation.gamma,
            theta: valuation.theta,
            vega: self.vega(inputs, volatility)?,
            rho: self.rho(inputs, volatility)?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::black_scholes::{european_price, norm_cdf, norm_pdf};
    use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};

    /// A European closed-form model, to check the trait's default bumps against analytic values.
    struct ClosedForm;

    impl OptionModel for ClosedForm {
        fn value(&self, inputs: &OptionInputs, volatility: f64) -> Result<Valuation, PricingError> {
            inputs.validate()?;
            crate::inputs::validate_volatility(volatility)?;
            Ok(Valuation {
                price: european_price(
                    inputs.kind,
                    inputs.spot,
                    inputs.strike,
                    inputs.time_to_expiry,
                    inputs.rate,
                    inputs.dividend_yield,
                    volatility,
                ),
                delta: 0.0,
                gamma: 0.0,
                theta: 0.0,
            })
        }
    }

    fn inputs() -> OptionInputs {
        OptionInputs::new(
            OptionKind::Call,
            OptionExercise::European,
            100.0,
            105.0,
            0.5,
            0.04,
        )
        .with_dividend_yield(0.01)
    }

    fn d1_d2(inputs: &OptionInputs, volatility: f64) -> (f64, f64) {
        let std_dev = volatility * inputs.time_to_expiry.sqrt();
        let d1 = ((inputs.spot / inputs.strike).ln()
            + (inputs.rate - inputs.dividend_yield) * inputs.time_to_expiry)
            / std_dev
            + 0.5 * std_dev;
        (d1, d1 - std_dev)
    }

    #[test]
    fn default_vega_and_rho_match_closed_form() {
        let inputs = inputs();
        let volatility = 0.25;
        let (d1, d2) = d1_d2(&inputs, volatility);
        let t = inputs.time_to_expiry;
        let vega = inputs.spot * (-inputs.dividend_yield * t).exp() * norm_pdf(d1) * t.sqrt();
        let rho = inputs.strike * t * (-inputs.rate * t).exp() * norm_cdf(d2);

        let greeks = ClosedForm.greeks(&inputs, volatility).unwrap();
        assert!(
            (greeks.vega - vega).abs() < 1e-6,
            "{} vs {vega}",
            greeks.vega
        );
        assert!((greeks.rho - rho).abs() < 1e-6, "{} vs {rho}", greeks.rho);
        assert_eq!(greeks.price, ClosedForm.price(&inputs, volatility).unwrap());
    }

    #[test]
    fn default_vega_keeps_the_lower_bump_positive() {
        assert_eq!(volatility_bump(0.2), 1e-4);
        assert_eq!(volatility_bump(1e-4), 5e-5);
        let vega = ClosedForm.vega(&inputs(), 1e-4).unwrap();
        assert!(vega.is_finite());
    }

    #[test]
    fn default_vega_refuses_an_invalid_volatility() {
        assert!(matches!(
            ClosedForm.vega(&inputs(), 0.0),
            Err(PricingError::InvalidInput {
                name: "volatility",
                ..
            })
        ));
    }
}
