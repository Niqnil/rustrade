use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Computes the trading fee for a single fill.
///
/// # Arguments
/// * `price` - Execution price per unit of the underlying.
/// * `quantity` - Number of contracts (or shares/units) filled.
/// * `contract_size` - Multiplier converting contracts to underlying units
///   (e.g. 100 for standard equity options). Use `Decimal::ONE` for spot.
pub trait FeeModel {
    fn compute_fee(&self, price: Decimal, quantity: Decimal, contract_size: Decimal) -> Decimal;
}

/// Zero-fee model. Useful for backtests where fees are excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize)]
pub struct ZeroFeeModel;

impl FeeModel for ZeroFeeModel {
    fn compute_fee(&self, _price: Decimal, _quantity: Decimal, _contract_size: Decimal) -> Decimal {
        Decimal::ZERO
    }
}

/// Flat commission charged per contract filled.
///
/// `total_fee = commission_per_contract * quantity.abs()`
///
/// This matches the typical Alpaca/IBKR per-contract options pricing.
/// `contract_size` is accepted but not used; the fee is per contract unit,
/// not per underlying share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PerContractFeeModel {
    pub commission_per_contract: Decimal,
}

impl FeeModel for PerContractFeeModel {
    fn compute_fee(&self, _price: Decimal, quantity: Decimal, _contract_size: Decimal) -> Decimal {
        self.commission_per_contract * quantity.abs()
    }
}

/// Enum-dispatched fee model for use in types that require `Clone`, `PartialEq`,
/// `Serialize`, and `Deserialize` (e.g. `InstrumentState`).
///
/// Prefer this over `Box<dyn FeeModel>` when the field must be part of a derived
/// `serde` struct. Defaults to [`ZeroFeeModel`].
///
/// # Double-counting warning
///
/// Only enable [`FeeModelConfig::PerContract`] when the `ExecutionClient` reports
/// `Trade.fees.fees = 0` for fills (i.e., commission is not already embedded in
/// fill reports). If the client already includes broker commission in `fees.fees`
/// and `PerContract` is also active, fees will be counted twice.
///
/// # Current variants
///
/// Only options-style flat per-contract commission is currently supported. A
/// percentage-of-notional variant (`rate * price * quantity`) is the most common
/// fee structure for spot and futures exchanges and is the primary use case of
/// `MockExchange::fees_percent`.
// TODO(Q-3): add PercentageFeeModel { rate: Decimal } variant for spot/futures.
// MockExchange's fees_percent field duplicates this responsibility; the variant
// should eventually replace it so backtests can use a unified fee config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum FeeModelConfig {
    Zero(ZeroFeeModel),
    PerContract(PerContractFeeModel),
}

impl Default for FeeModelConfig {
    fn default() -> Self {
        Self::Zero(ZeroFeeModel)
    }
}

impl FeeModel for FeeModelConfig {
    fn compute_fee(&self, price: Decimal, quantity: Decimal, contract_size: Decimal) -> Decimal {
        match self {
            FeeModelConfig::Zero(m) => m.compute_fee(price, quantity, contract_size),
            FeeModelConfig::PerContract(m) => m.compute_fee(price, quantity, contract_size),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn zero_fee_model_always_returns_zero() {
        assert_eq!(ZeroFeeModel.compute_fee(d("100"), d("5"), d("100")), Decimal::ZERO);
        assert_eq!(ZeroFeeModel.compute_fee(Decimal::ZERO, Decimal::ZERO, Decimal::ONE), Decimal::ZERO);
    }

    #[test]
    fn per_contract_fee_charges_by_quantity() {
        let model = PerContractFeeModel { commission_per_contract: d("0.65") };
        assert_eq!(model.compute_fee(d("100"), d("10"), d("100")), d("6.5"));
    }

    #[test]
    fn per_contract_fee_uses_abs_quantity() {
        let model = PerContractFeeModel { commission_per_contract: d("0.65") };
        // Negative quantity (sell side) should produce the same fee as positive.
        assert_eq!(
            model.compute_fee(d("100"), d("-10"), d("100")),
            model.compute_fee(d("100"), d("10"), d("100")),
        );
    }

    // --- FeeModelConfig enum dispatch ---

    #[test]
    fn fee_model_config_zero_dispatches() {
        let cfg = FeeModelConfig::Zero(ZeroFeeModel);
        assert_eq!(cfg.compute_fee(d("100"), d("5"), d("100")), Decimal::ZERO);
    }

    #[test]
    fn fee_model_config_per_contract_dispatches() {
        let model = PerContractFeeModel { commission_per_contract: d("0.65") };
        let cfg = FeeModelConfig::PerContract(model);
        assert_eq!(
            cfg.compute_fee(d("100"), d("10"), d("100")),
            model.compute_fee(d("100"), d("10"), d("100")),
        );
    }

    #[test]
    fn fee_model_config_default_is_zero() {
        assert_eq!(FeeModelConfig::default(), FeeModelConfig::Zero(ZeroFeeModel));
    }
}
