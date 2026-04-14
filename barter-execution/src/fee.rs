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

/// Percentage-of-notional fee model for spot and futures exchanges.
///
/// `total_fee = rate * price * quantity.abs()`
///
/// Common for crypto spot/futures exchanges (e.g. Binance 0.1% taker fee).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PercentageFeeModel {
    /// Fee rate as a decimal fraction. Typical range is `[0, 1]`:
    /// - `0.001` = 0.1% (common taker fee)
    /// - `0.0005` = 0.05% (common maker fee)
    ///
    /// No validation is performed; values outside `[0, 1]` are accepted
    /// but produce unusual fee amounts.
    pub rate: Decimal,
}

impl FeeModel for PercentageFeeModel {
    fn compute_fee(&self, price: Decimal, quantity: Decimal, _contract_size: Decimal) -> Decimal {
        self.rate * price * quantity.abs()
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
/// Only enable a non-[`Zero`](FeeModelConfig::Zero) fee model when the `ExecutionClient`
/// reports `Trade.fees.fees = 0` for fills (i.e., commission is not already embedded in
/// fill reports). If the client already includes fees in `fees.fees` and a fee model
/// is also active, fees will be counted twice.
///
/// # Variants
///
/// - [`Zero`](FeeModelConfig::Zero): No fees (backtests where fees are excluded).
/// - [`PerContract`](FeeModelConfig::PerContract): Flat per-contract commission (options).
/// - [`Percentage`](FeeModelConfig::Percentage): Percentage of notional (spot/futures).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum FeeModelConfig {
    Zero(ZeroFeeModel),
    PerContract(PerContractFeeModel),
    Percentage(PercentageFeeModel),
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
            FeeModelConfig::Percentage(m) => m.compute_fee(price, quantity, contract_size),
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

    // --- PercentageFeeModel ---

    #[test]
    fn percentage_fee_computes_rate_times_notional() {
        // 0.1% fee rate
        let model = PercentageFeeModel { rate: d("0.001") };
        // 10 units at price 100 = notional 1000, fee = 1000 * 0.001 = 1
        assert_eq!(model.compute_fee(d("100"), d("10"), d("1")), d("1"));
    }

    #[test]
    fn percentage_fee_uses_abs_quantity() {
        let model = PercentageFeeModel { rate: d("0.001") };
        assert_eq!(
            model.compute_fee(d("100"), d("-10"), d("1")),
            model.compute_fee(d("100"), d("10"), d("1")),
        );
    }

    #[test]
    fn fee_model_config_percentage_dispatches() {
        let model = PercentageFeeModel { rate: d("0.001") };
        let cfg = FeeModelConfig::Percentage(model);
        assert_eq!(
            cfg.compute_fee(d("100"), d("10"), d("1")),
            model.compute_fee(d("100"), d("10"), d("1")),
        );
    }

    // --- Serde round-trip tests ---

    #[test]
    fn zero_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::Zero(ZeroFeeModel);
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"Zero":null}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn fee_model_config_default_when_field_omitted() {
        // Simulates deserializing a struct where fee_model field is absent
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            fee_model: FeeModelConfig,
        }
        let parsed: Wrapper = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(parsed.fee_model, FeeModelConfig::Zero(ZeroFeeModel));
    }

    #[test]
    fn percentage_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::Percentage(PercentageFeeModel { rate: d("0.001") });
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"Percentage":{"rate":"0.001"}}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn per_contract_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::PerContract(PerContractFeeModel { commission_per_contract: d("0.65") });
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"PerContract":{"commission_per_contract":"0.65"}}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }
}
