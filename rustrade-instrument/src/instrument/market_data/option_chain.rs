//! Option chain discovery types for cross-exchange option chain enumeration.
//!
//! [`OptionChainDescriptor`] represents the available option contracts for an underlying,
//! providing a unified view across exchanges with different API capabilities.
//!
//! # Exchange API Divergence
//!
//! | Field | IBKR | Alpaca |
//! |-------|------|--------|
//! | `exchange` | always `ExchangeId::Ibkr` (per-chain string ignored) | ✓ |
//! | `multiplier` | ✓ | ✓ (`size` field) |
//! | `expirations` | ✓ | ✓ |
//! | `strikes` | ✓ | ✓ |
//! | `exercise` | ✗ (caller must know) | ✓ (`style` field) |
//!
//! # Example
//!
//! ```
//! use chrono::NaiveDate;
//! use rust_decimal_macros::dec;
//! use rustrade_instrument::{
//!     exchange::ExchangeId,
//!     instrument::{
//!         kind::option::OptionExercise,
//!         market_data::option_chain::OptionChainDescriptor,
//!     },
//! };
//!
//! let descriptor = OptionChainDescriptor::new(
//!     ExchangeId::Ibkr,
//!     dec!(100),
//!     vec![
//!         NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
//!         NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(),
//!     ],
//!     vec![dec!(145), dec!(150), dec!(155)],
//!     None, // IBKR doesn't provide exercise style
//! );
//!
//! // Expand to contracts (US equity = American)
//! let contracts = descriptor
//!     .to_contracts(Some(OptionExercise::American))
//!     .unwrap();
//!
//! // 2 expirations × 3 strikes × 2 (Call + Put) = 12 contracts
//! assert_eq!(contracts.len(), 12);
//! ```

use crate::{
    exchange::ExchangeId,
    instrument::{
        kind::option::{OptionExercise, OptionKind},
        market_data::kind::MarketDataOptionContract,
    },
};
use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Error when expanding an [`OptionChainDescriptor`] to contracts.
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum OptionChainError {
    /// Exercise style required but not provided.
    ///
    /// Occurs when `OptionChainDescriptor::exercise` is `None` and no
    /// `exercise` parameter is passed to `to_contracts()`.
    #[error("exercise style required: descriptor has no exercise field and none provided")]
    ExerciseRequired,
}

/// Option chain descriptor for cross-exchange option discovery.
///
/// Represents the matrix of available option contracts (expirations × strikes)
/// for an underlying on a specific exchange. Use [`to_contracts`](Self::to_contracts)
/// to expand into individual [`MarketDataOptionContract`] instances.
///
/// # API Divergence
///
/// The `exercise` field documents exchange API differences (per [#70] pattern):
/// - **Alpaca**: Returns `style` ("american"/"european") per contract — stored in `exercise`
/// - **IBKR**: Does not return exercise style — `exercise` is `None`, caller provides it
///
/// # Construction
///
/// Use [`OptionChainDescriptor::new`] to create instances. Outside this crate,
/// direct struct literal syntax is not available due to `#[non_exhaustive]`.
///
/// [#70]: https://github.com/Niqnil/rustrade/issues/70
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct OptionChainDescriptor {
    /// Exchange where this option chain trades.
    pub exchange: ExchangeId,

    /// Contract multiplier (e.g., 100 for standard equity options).
    pub multiplier: Decimal,

    /// Available expiration dates, sorted ascending.
    pub expirations: Vec<NaiveDate>,

    /// Available strike prices, sorted ascending.
    pub strikes: Vec<Decimal>,

    /// Exercise style when provided by the exchange API.
    ///
    /// - `Some(style)`: API returned exercise style (Alpaca)
    /// - `None`: API does not provide it (IBKR) — caller must supply to [`to_contracts`](Self::to_contracts)
    pub exercise: Option<OptionExercise>,
}

impl OptionChainDescriptor {
    /// Create a new option chain descriptor.
    ///
    /// # Arguments
    ///
    /// * `exchange` - Exchange where this option chain trades
    /// * `multiplier` - Contract multiplier (e.g., 100 for standard equity options)
    /// * `expirations` - Available expiration dates
    /// * `strikes` - Available strike prices
    /// * `exercise` - Exercise style if known, `None` if API doesn't provide it
    pub fn new(
        exchange: ExchangeId,
        multiplier: Decimal,
        expirations: Vec<NaiveDate>,
        strikes: Vec<Decimal>,
        exercise: Option<OptionExercise>,
    ) -> Self {
        Self {
            exchange,
            multiplier,
            expirations,
            strikes,
            exercise,
        }
    }

    /// Expand to all option contracts (expirations × strikes × {Call, Put}).
    ///
    /// # Arguments
    ///
    /// * `exercise` - Exercise style override. If `Some`, uses this value.
    ///   If `None`, uses `self.exercise`. Returns error if both are `None`.
    ///
    /// # Returns
    ///
    /// Vector of [`MarketDataOptionContract`] for each combination of
    /// expiration, strike, and option kind (Call/Put). Results are ordered:
    /// for each expiration, for each strike, Call then Put.
    ///
    /// # Errors
    ///
    /// Returns [`OptionChainError::ExerciseRequired`] if no exercise style
    /// is available (both `self.exercise` and `exercise` param are `None`).
    pub fn to_contracts(
        &self,
        exercise: Option<OptionExercise>,
    ) -> Result<Vec<MarketDataOptionContract>, OptionChainError> {
        let exercise = exercise
            .or(self.exercise)
            .ok_or(OptionChainError::ExerciseRequired)?;

        let mut contracts = Vec::with_capacity(self.contract_count());

        for expiration in &self.expirations {
            let expiry = expiration.and_time(NaiveTime::MIN).and_utc();

            for &strike in &self.strikes {
                contracts.push(MarketDataOptionContract {
                    kind: OptionKind::Call,
                    exercise,
                    expiry,
                    strike,
                });
                contracts.push(MarketDataOptionContract {
                    kind: OptionKind::Put,
                    exercise,
                    expiry,
                    strike,
                });
            }
        }

        Ok(contracts)
    }

    /// Number of contracts this descriptor would expand to.
    ///
    /// Returns `expirations.len() × strikes.len() × 2` (Call + Put for each),
    /// saturating at `usize::MAX` to defend against adversarial deserialized input.
    pub fn contract_count(&self) -> usize {
        self.expirations
            .len()
            .saturating_mul(self.strikes.len())
            .saturating_mul(2)
    }
}

impl fmt::Display for OptionChainDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OptionChain({}: {} expirations × {} strikes, multiplier={})",
            self.exchange,
            self.expirations.len(),
            self.strikes.len(),
            self.multiplier
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::Timelike;
    use rust_decimal_macros::dec;

    fn sample_descriptor() -> OptionChainDescriptor {
        OptionChainDescriptor::new(
            ExchangeId::Ibkr,
            dec!(100),
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 19).unwrap(),
                NaiveDate::from_ymd_opt(2024, 2, 16).unwrap(),
            ],
            vec![dec!(145), dec!(150), dec!(155)],
            None,
        )
    }

    #[test]
    fn to_contracts_with_exercise_param() {
        let descriptor = sample_descriptor();
        let contracts = descriptor
            .to_contracts(Some(OptionExercise::American))
            .unwrap();

        // 2 expirations × 3 strikes × 2 kinds = 12
        assert_eq!(contracts.len(), 12);
        assert_eq!(descriptor.contract_count(), 12);

        // First expiration, first strike: Call then Put
        assert_eq!(contracts[0].kind, OptionKind::Call);
        assert_eq!(contracts[0].strike, dec!(145));
        assert_eq!(contracts[0].exercise, OptionExercise::American);

        assert_eq!(contracts[1].kind, OptionKind::Put);
        assert_eq!(contracts[1].strike, dec!(145));
    }

    #[test]
    fn to_contracts_uses_stored_exercise() {
        let mut descriptor = sample_descriptor();
        descriptor.exercise = Some(OptionExercise::European);

        // Pass None — should use stored exercise
        let contracts = descriptor.to_contracts(None).unwrap();

        assert_eq!(contracts[0].exercise, OptionExercise::European);
    }

    #[test]
    fn to_contracts_param_overrides_stored() {
        let mut descriptor = sample_descriptor();
        descriptor.exercise = Some(OptionExercise::European);

        // Pass American — should override stored European
        let contracts = descriptor
            .to_contracts(Some(OptionExercise::American))
            .unwrap();

        assert_eq!(contracts[0].exercise, OptionExercise::American);
    }

    #[test]
    fn to_contracts_error_when_no_exercise() {
        let descriptor = sample_descriptor(); // exercise: None

        let result = descriptor.to_contracts(None);

        assert!(matches!(result, Err(OptionChainError::ExerciseRequired)));
    }

    #[test]
    fn to_contracts_expiry_is_midnight_utc() {
        let descriptor = sample_descriptor();
        let contracts = descriptor
            .to_contracts(Some(OptionExercise::American))
            .unwrap();

        let expiry = contracts[0].expiry;
        assert_eq!(expiry.hour(), 0);
        assert_eq!(expiry.minute(), 0);
        assert_eq!(expiry.second(), 0);
        assert_eq!(
            expiry.date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 19).unwrap()
        );
    }

    #[test]
    fn empty_chain_produces_no_contracts() {
        let descriptor = OptionChainDescriptor::new(
            ExchangeId::AlpacaBroker,
            dec!(100),
            vec![],
            vec![dec!(150)],
            Some(OptionExercise::American),
        );

        let contracts = descriptor.to_contracts(None).unwrap();
        assert!(contracts.is_empty());
        assert_eq!(descriptor.contract_count(), 0);
    }

    #[test]
    fn display_format() {
        let descriptor = sample_descriptor();
        let display = format!("{}", descriptor);
        assert!(display.contains("Ibkr"));
        assert!(display.contains("2 expirations"));
        assert!(display.contains("3 strikes"));
        assert!(display.contains("100"));
    }

    #[test]
    fn serialization_roundtrip() {
        let mut descriptor = sample_descriptor();
        descriptor.exercise = Some(OptionExercise::American);

        let json = serde_json::to_string(&descriptor).unwrap();
        let parsed: OptionChainDescriptor = serde_json::from_str(&json).unwrap();

        assert_eq!(descriptor, parsed);
    }
}
