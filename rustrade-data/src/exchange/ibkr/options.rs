//! Option Greeks and chain data types for Interactive Brokers.
//!
//! This module provides IBKR-specific functionality for option analytics:
//!
//! - Conversion from ibapi's `OptionComputation` to [`OptionGreeks`]
//! - [`OptionChainEntry`] for option chain metadata
//!
//! The core [`OptionGreeks`] type is defined in [`subscription::greeks`] and
//! re-exported here for convenience.
//!
//! # Calculator vs Real-Time
//!
//! IB provides two ways to get Greeks:
//!
//! 1. **Calculators** (Phase 5A): You provide volatility/price inputs, IB computes Greeks.
//!    See [`IbkrHistoricalData::calculate_theoretical_greeks`] and
//!    [`IbkrHistoricalData::calculate_implied_volatility`].
//!
//! 2. **Real-time ticks** (Phase 5B): Subscribe to option market data, receive
//!    Greeks computed from live prices via `TickTypes::OptionComputation`.
//!
//! # Subscription Requirements
//!
//! Option Greeks require OPRA subscription ($1.50/mo) for US options.
//!
//! [`subscription::greeks`]: crate::subscription::greeks
//! [`IbkrHistoricalData::calculate_theoretical_greeks`]: super::historical::IbkrHistoricalData::calculate_theoretical_greeks
//! [`IbkrHistoricalData::calculate_implied_volatility`]: super::historical::IbkrHistoricalData::calculate_implied_volatility

use chrono::NaiveDate;
use ibapi::contracts::{OptionChain, OptionComputation};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::market_data::OptionChainDescriptor};
use serde::{Deserialize, Serialize};

pub use crate::subscription::greeks::OptionGreeks;

impl OptionGreeks {
    /// Convert from ibapi's `OptionComputation` to [`OptionGreeks`].
    pub(crate) fn from_ib(computation: &OptionComputation) -> Self {
        Self {
            delta: computation.delta,
            gamma: computation.gamma,
            theta: computation.theta,
            vega: computation.vega,
            implied_volatility: computation.implied_volatility,
            theoretical_price: computation.option_price,
            underlying_price: computation.underlying_price,
        }
    }
}

/// Option chain entry for a specific exchange.
///
/// Represents available option series for an underlying on a particular exchange.
/// Use [`IbkrHistoricalData::fetch_option_chain`] to retrieve this data.
///
/// [`IbkrHistoricalData::fetch_option_chain`]: super::historical::IbkrHistoricalData::fetch_option_chain
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionChainEntry {
    /// Contract ID of the underlying security.
    pub underlying_contract_id: i32,
    /// Option trading class (e.g., "AAPL" for standard, "AAPL7" for weekly).
    pub trading_class: String,
    /// Option multiplier (e.g., "100" for standard equity options).
    pub multiplier: String,
    /// Exchange where this option series trades.
    pub exchange: String,
    /// Available expiration dates.
    pub expirations: Vec<NaiveDate>,
    /// Available strike prices.
    pub strikes: Vec<Decimal>,
}

impl OptionChainEntry {
    /// Create OptionChainEntry from ibapi's OptionChain.
    ///
    /// Expirations that fail YYYYMMDD parsing and strike prices that fail
    /// f64→Decimal conversion (NaN/Infinity/IB sentinel values) are skipped.
    pub fn from_ib(chain: &OptionChain) -> Self {
        Self {
            underlying_contract_id: chain.underlying_contract_id,
            trading_class: chain.trading_class.clone(),
            multiplier: chain.multiplier.clone(),
            exchange: chain.exchange.clone(),
            expirations: chain
                .expirations
                .iter()
                .filter_map(|s| NaiveDate::parse_from_str(s, "%Y%m%d").ok())
                .collect(),
            strikes: chain
                .strikes
                .iter()
                .filter_map(|&s| super::decimal_from_f64(s))
                .collect(),
        }
    }

    /// Convert to unified [`OptionChainDescriptor`].
    ///
    /// # Errors
    ///
    /// Returns [`rust_decimal::Error`] if the multiplier string cannot be parsed
    /// as a decimal. Surfacing the parse error lets callers log the offending
    /// value instead of silently dropping the entry.
    ///
    /// # Notes
    ///
    /// - `exercise` is `None` because IBKR does not return exercise style in chain data
    /// - `exchange` is always `ExchangeId::Ibkr` (the IBKR-specific exchange string is not mapped)
    pub fn to_descriptor(&self) -> Result<OptionChainDescriptor, rust_decimal::Error> {
        let multiplier = self.multiplier.parse::<Decimal>()?;

        Ok(OptionChainDescriptor::new(
            ExchangeId::Ibkr,
            multiplier,
            self.expirations.clone(),
            self.strikes.clone(),
            None,
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn option_greeks_from_ib_computation() {
        use ibapi::contracts::{OptionComputation, tick_types::TickType};

        let computation = OptionComputation {
            field: TickType::ModelOption,
            tick_attribute: Some(1),
            delta: Some(0.55),
            gamma: Some(0.02),
            theta: Some(-0.05),
            vega: Some(0.15),
            implied_volatility: Some(0.25),
            option_price: Some(5.50),
            underlying_price: Some(150.0),
            present_value_dividend: Some(0.0),
        };

        let greeks = OptionGreeks::from_ib(&computation);

        assert_eq!(greeks.delta, Some(0.55));
        assert_eq!(greeks.gamma, Some(0.02));
        assert_eq!(greeks.theta, Some(-0.05));
        assert_eq!(greeks.vega, Some(0.15));
        assert_eq!(greeks.implied_volatility, Some(0.25));
        assert_eq!(greeks.theoretical_price, Some(5.50));
        assert_eq!(greeks.underlying_price, Some(150.0));
        assert!(greeks.has_any_greek());
    }

    #[test]
    fn option_greeks_empty_has_any_greek_false() {
        let greeks = OptionGreeks::default();
        assert!(!greeks.has_any_greek());
    }

    #[test]
    fn option_chain_entry_from_ib() {
        let chain = OptionChain {
            underlying_contract_id: 265598,
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec!["20240119".to_string(), "20240216".to_string()],
            strikes: vec![140.0, 145.0, 150.0, 155.0, 160.0],
        };

        let entry = OptionChainEntry::from_ib(&chain);

        assert_eq!(entry.underlying_contract_id, 265598);
        assert_eq!(entry.trading_class, "AAPL");
        assert_eq!(entry.multiplier, "100");
        assert_eq!(entry.exchange, "SMART");
        assert_eq!(entry.expirations.len(), 2);
        assert_eq!(
            entry.expirations[0],
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap()
        );
        assert_eq!(
            entry.expirations[1],
            NaiveDate::from_ymd_opt(2024, 2, 16).unwrap()
        );
        assert_eq!(entry.strikes.len(), 5);
    }

    #[test]
    fn option_chain_entry_skips_invalid_expirations() {
        let chain = OptionChain {
            underlying_contract_id: 265598,
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec![
                "20240119".to_string(),
                "invalid".to_string(),
                "20240216".to_string(),
            ],
            strikes: vec![150.0],
        };

        let entry = OptionChainEntry::from_ib(&chain);

        assert_eq!(entry.expirations.len(), 2);
    }

    #[test]
    fn option_chain_entry_to_descriptor() {
        use rust_decimal_macros::dec;

        let entry = OptionChainEntry {
            underlying_contract_id: 265598,
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec![
                NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
                NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(),
            ],
            strikes: vec![dec!(145), dec!(150), dec!(155)],
        };

        let descriptor = entry.to_descriptor().unwrap();

        assert_eq!(descriptor.exchange, ExchangeId::Ibkr);
        assert_eq!(descriptor.multiplier, dec!(100));
        assert_eq!(descriptor.expirations.len(), 2);
        assert_eq!(descriptor.strikes.len(), 3);
        assert!(descriptor.exercise.is_none());
    }

    #[test]
    fn option_chain_entry_to_descriptor_invalid_multiplier() {
        let entry = OptionChainEntry {
            underlying_contract_id: 265598,
            trading_class: "AAPL".to_string(),
            multiplier: "not_a_number".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec![],
            strikes: vec![],
        };

        assert!(entry.to_descriptor().is_err());
    }
}
