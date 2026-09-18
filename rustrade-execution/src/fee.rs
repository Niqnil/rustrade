use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Which side of the trade a fill was on: the one that supplied liquidity, or the one that took it.
///
/// Venues price these differently, and usually by a wide margin — a maker rebate against a taker
/// fee is the whole economics of quoting. A fee model that cannot tell them apart charges the taker
/// rate for everything, which systematically overstates the cost of exactly the strategies that
/// rest orders in order to earn the maker side.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub enum Liquidity {
    /// The fill took liquidity already resting on the book: a market order, or a limit order
    /// marketable on arrival. The default, because it is the conservative assumption — a model that
    /// guesses wrong this way overstates costs rather than inventing profit.
    #[default]
    Taker,
    /// The fill supplied liquidity: an order that rested on the book and was matched by someone
    /// else's incoming order.
    Maker,
}

/// Computes the trading fee for a single fill.
///
/// # Arguments
/// * `price` - Execution price per unit of the underlying.
/// * `quantity` - Number of contracts (or shares/units) filled.
/// * `contract_size` - Multiplier converting contracts to underlying units
///   (e.g. 100 for standard equity options). Use `Decimal::ONE` for spot.
/// * `liquidity` - Whether the fill made or took liquidity; see [`Liquidity`].
pub trait FeeModel {
    fn compute_fee(
        &self,
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        liquidity: Liquidity,
    ) -> Decimal;
}

/// Zero-fee model. Useful for backtests where fees are excluded.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct ZeroFeeModel;

impl FeeModel for ZeroFeeModel {
    fn compute_fee(
        &self,
        _price: Decimal,
        _quantity: Decimal,
        _contract_size: Decimal,
        _liquidity: Liquidity,
    ) -> Decimal {
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
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_per_contract: Decimal,
}

impl FeeModel for PerContractFeeModel {
    /// `liquidity` is ignored: a per-contract commission is a flat brokerage charge, and the
    /// brokers this models (Alpaca, IBKR options) do not vary it by who supplied liquidity.
    fn compute_fee(
        &self,
        _price: Decimal,
        quantity: Decimal,
        _contract_size: Decimal,
        _liquidity: Liquidity,
    ) -> Decimal {
        self.commission_per_contract * quantity.abs()
    }
}

/// Percentage-of-notional fee model for spot, futures and CFD exchanges.
///
/// `total_fee = rate * price * quantity.abs() * contract_size`
///
/// Common for crypto spot/futures exchanges (e.g. Binance 0.1% taker fee).
///
/// # Why `contract_size` is applied
///
/// The notional a percentage is charged on is the *underlying* exposure, not the contract count.
/// For `Spot` the multiplier is `Decimal::ONE` and the term is inert, but a €25-per-point index
/// CFD at 5000 has a notional of €125,000, not €5,000 — dropping the multiplier would understate
/// the fee by exactly that factor, silently, on every fill. The same applies to any `Future` or
/// `Perpetual` whose `contract_size` is not one.
///
/// Contrast [`PerContractFeeModel`], which ignores the multiplier because its fee genuinely is
/// per contract rather than per underlying unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PercentageFeeModel {
    /// Fee rate charged when the fill **took** liquidity, and when no [`maker_rate`](Self::maker_rate)
    /// is configured. Typical range is `[0, 1]`:
    /// - `0.001` = 0.1% (common taker fee)
    /// - `0.0005` = 0.05% (common maker fee)
    ///
    /// No validation is performed; values outside `[0, 1]` are accepted
    /// but produce unusual fee amounts.
    #[serde(with = "rust_decimal::serde::str")]
    pub rate: Decimal,

    /// Fee rate charged when the fill **made** liquidity. `None` charges [`rate`](Self::rate) on
    /// both sides.
    ///
    /// Absent by default so a configuration written before this field existed deserialises and
    /// prices exactly as it did. Set it to model a venue's maker schedule — commonly a fraction of
    /// the taker rate, and on some venues a rebate, which this represents as a negative rate.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::str_option"
    )]
    pub maker_rate: Option<Decimal>,
}

impl PercentageFeeModel {
    /// One rate, charged on both sides of the book.
    pub fn new(rate: Decimal) -> Self {
        Self {
            rate,
            maker_rate: None,
        }
    }

    /// Distinct maker and taker rates, as a venue's fee schedule quotes them.
    ///
    /// A maker *rebate* is a negative `maker_rate`.
    pub fn maker_taker(maker_rate: Decimal, taker_rate: Decimal) -> Self {
        Self {
            rate: taker_rate,
            maker_rate: Some(maker_rate),
        }
    }

    /// The rate charged for `liquidity`: [`maker_rate`](Self::maker_rate) when it is configured and
    /// the fill made liquidity, otherwise [`rate`](Self::rate).
    pub fn rate_for(&self, liquidity: Liquidity) -> Decimal {
        match liquidity {
            Liquidity::Maker => self.maker_rate.unwrap_or(self.rate),
            Liquidity::Taker => self.rate,
        }
    }
}

impl FeeModel for PercentageFeeModel {
    fn compute_fee(
        &self,
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        liquidity: Liquidity,
    ) -> Decimal {
        self.rate_for(liquidity) * price * quantity.abs() * contract_size
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
    fn compute_fee(
        &self,
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        liquidity: Liquidity,
    ) -> Decimal {
        match self {
            FeeModelConfig::Zero(model) => {
                model.compute_fee(price, quantity, contract_size, liquidity)
            }
            FeeModelConfig::PerContract(model) => {
                model.compute_fee(price, quantity, contract_size, liquidity)
            }
            FeeModelConfig::Percentage(model) => {
                model.compute_fee(price, quantity, contract_size, liquidity)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn zero_fee_model_always_returns_zero() {
        assert_eq!(
            ZeroFeeModel.compute_fee(d("100"), d("5"), d("100"), Liquidity::Taker),
            Decimal::ZERO
        );
        assert_eq!(
            ZeroFeeModel.compute_fee(Decimal::ZERO, Decimal::ZERO, Decimal::ONE, Liquidity::Taker),
            Decimal::ZERO
        );
    }

    #[test]
    fn per_contract_fee_charges_by_quantity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        assert_eq!(
            model.compute_fee(d("100"), d("10"), d("100"), Liquidity::Taker),
            d("6.5")
        );
    }

    #[test]
    fn per_contract_fee_uses_abs_quantity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        // Negative quantity (sell side) should produce the same fee as positive.
        assert_eq!(
            model.compute_fee(d("100"), d("-10"), d("100"), Liquidity::Taker),
            model.compute_fee(d("100"), d("10"), d("100"), Liquidity::Taker),
        );
    }

    // --- FeeModelConfig enum dispatch ---

    #[test]
    fn fee_model_config_zero_dispatches() {
        let cfg = FeeModelConfig::Zero(ZeroFeeModel);
        assert_eq!(
            cfg.compute_fee(d("100"), d("5"), d("100"), Liquidity::Taker),
            Decimal::ZERO
        );
    }

    #[test]
    fn fee_model_config_per_contract_dispatches() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        let cfg = FeeModelConfig::PerContract(model);
        assert_eq!(
            cfg.compute_fee(d("100"), d("10"), d("100"), Liquidity::Taker),
            model.compute_fee(d("100"), d("10"), d("100"), Liquidity::Taker),
        );
    }

    #[test]
    fn fee_model_config_default_is_zero() {
        assert_eq!(
            FeeModelConfig::default(),
            FeeModelConfig::Zero(ZeroFeeModel)
        );
    }

    // --- PercentageFeeModel ---

    #[test]
    fn percentage_fee_computes_rate_times_notional() {
        // 0.1% fee rate
        let model = PercentageFeeModel::new(d("0.001"));
        // 10 units at price 100 = notional 1000, fee = 1000 * 0.001 = 1
        assert_eq!(
            model.compute_fee(d("100"), d("10"), d("1"), Liquidity::Taker),
            d("1")
        );
    }

    #[test]
    fn percentage_fee_uses_abs_quantity() {
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(d("100"), d("-10"), d("1"), Liquidity::Taker),
            model.compute_fee(d("100"), d("10"), d("1"), Liquidity::Taker),
        );
    }

    #[test]
    fn percentage_fee_scales_by_contract_size() {
        // A EUR25-per-point index CFD at 5000: the notional is 125_000, not 5_000, so 0.1% is 125.
        // Charging 5 here would understate every CFD fill by the multiplier.
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(d("5000"), d("1"), d("25"), Liquidity::Taker),
            d("125")
        );
    }

    #[test]
    fn percentage_fee_is_unchanged_for_a_unit_contract_size() {
        // Spot is the overwhelmingly common case and must be untouched by the multiplier.
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Taker),
            d("1")
        );
    }

    #[test]
    fn per_contract_fee_ignores_contract_size() {
        // The counterpart to `percentage_fee_scales_by_contract_size`: this fee genuinely is per
        // contract, so the multiplier must NOT apply. Pinned so the two models cannot drift into
        // sharing one rule.
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        assert_eq!(
            model.compute_fee(d("5000"), d("10"), d("25"), Liquidity::Taker),
            model.compute_fee(d("5000"), d("10"), Decimal::ONE, Liquidity::Taker),
        );
    }

    #[test]
    fn fee_model_config_percentage_dispatches() {
        let model = PercentageFeeModel::new(d("0.001"));
        let cfg = FeeModelConfig::Percentage(model);
        assert_eq!(
            cfg.compute_fee(d("100"), d("10"), d("1"), Liquidity::Taker),
            model.compute_fee(d("100"), d("10"), d("1"), Liquidity::Taker),
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
        let cfg = FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001")));
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"Percentage":{"rate":"0.001"}}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn per_contract_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(
            json,
            r#"{"PerContract":{"commission_per_contract":"0.65"}}"#
        );
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    // --- Maker / taker ---

    #[test]
    fn a_percentage_model_without_a_maker_rate_charges_one_rate_on_both_sides() {
        let model = PercentageFeeModel::new(d("0.001"));

        assert_eq!(
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Maker),
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Taker),
            "an unconfigured maker rate must price exactly as the model did before the field \
             existed"
        );
    }

    #[test]
    fn a_configured_maker_rate_is_charged_only_to_the_maker() {
        let model = PercentageFeeModel::maker_taker(d("0.0002"), d("0.001"));

        assert_eq!(
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Taker),
            d("1"),
            "0.1% of 1000 notional"
        );
        assert_eq!(
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Maker),
            d("0.2"),
            "0.02% of 1000 notional"
        );
    }

    /// Some venues pay for liquidity. A rebate is a negative fee, not a missing one.
    #[test]
    fn a_maker_rebate_is_a_negative_fee() {
        let model = PercentageFeeModel::maker_taker(d("-0.0001"), d("0.001"));

        assert_eq!(
            model.compute_fee(d("100"), d("10"), Decimal::ONE, Liquidity::Maker),
            d("-0.1")
        );
    }

    #[test]
    fn a_per_contract_commission_does_not_vary_with_liquidity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };

        assert_eq!(
            model.compute_fee(d("100"), d("10"), d("100"), Liquidity::Maker),
            model.compute_fee(d("100"), d("10"), d("100"), Liquidity::Taker)
        );
    }

    /// A config written before `maker_rate` existed must still deserialise, and price as it did.
    #[test]
    fn a_percentage_model_deserialises_without_a_maker_rate() {
        let model: PercentageFeeModel =
            serde_json::from_str(r#"{"rate":"0.001"}"#).expect("legacy shape must parse");

        assert_eq!(model, PercentageFeeModel::new(d("0.001")));
        assert_eq!(model.maker_rate, None);
    }

    #[test]
    fn a_percentage_model_round_trips_a_maker_rate() {
        let model = PercentageFeeModel::maker_taker(d("0.0002"), d("0.001"));
        let json = serde_json::to_string(&model).expect("must serialise");

        assert_eq!(
            serde_json::from_str::<PercentageFeeModel>(&json).expect("must round-trip"),
            model
        );
    }
}
