//! Option Greeks subscription type.
//!
//! This module defines the [`OptionGreeks`] data type for option analytics
//! (delta, gamma, theta, vega, implied volatility).
//!
//! Unlike other subscription types in this module, Greeks are typically
//! computed by the exchange/broker from live market data rather than being
//! raw market data themselves.

use serde::{Deserialize, Serialize};

/// Option Greeks values.
///
/// All fields are optional because the data source may not return all Greeks
/// depending on the contract type, market state, or subscription level.
///
/// # Fields
///
/// - `delta`: Rate of change of option price with respect to underlying price
/// - `gamma`: Rate of change of delta with respect to underlying price
/// - `theta`: Rate of change of option price with respect to time (per day)
/// - `vega`: Rate of change of option price with respect to volatility
/// - `implied_volatility`: Market-implied volatility
/// - `theoretical_price`: Model price computed by the exchange/broker
/// - `underlying_price`: Current underlying price used in computation
///
/// # Note on Precision
///
/// Values are stored as `f64` rather than `Decimal` because Greeks are
/// analytics (not monetary values) and the precision loss is acceptable.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct OptionGreeks {
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
    pub implied_volatility: Option<f64>,
    pub theoretical_price: Option<f64>,
    pub underlying_price: Option<f64>,
}

impl OptionGreeks {
    /// Returns true if at least one first-order Greek (delta, gamma, theta,
    /// vega, or implied volatility) is present.
    ///
    /// Does NOT consider `theoretical_price` or `underlying_price` — a tick
    /// containing only those fields is treated as having no Greek data.
    pub fn has_any_greek(&self) -> bool {
        self.delta.is_some()
            || self.gamma.is_some()
            || self.theta.is_some()
            || self.vega.is_some()
            || self.implied_volatility.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_greeks_default_has_no_values() {
        let greeks = OptionGreeks::default();
        assert!(!greeks.has_any_greek());
    }

    #[test]
    fn option_greeks_partial_has_any_greek() {
        let greeks = OptionGreeks {
            delta: Some(0.55),
            ..Default::default()
        };
        assert!(greeks.has_any_greek());
    }

    #[test]
    fn option_greeks_full_has_any_greek() {
        let greeks = OptionGreeks {
            delta: Some(0.55),
            gamma: Some(0.02),
            theta: Some(-0.05),
            vega: Some(0.15),
            implied_volatility: Some(0.25),
            theoretical_price: Some(5.50),
            underlying_price: Some(150.0),
        };
        assert!(greeks.has_any_greek());
    }

    #[test]
    fn option_greeks_price_only_has_no_greek() {
        let greeks = OptionGreeks {
            theoretical_price: Some(5.50),
            underlying_price: Some(150.0),
            ..Default::default()
        };
        assert!(!greeks.has_any_greek());
    }
}
