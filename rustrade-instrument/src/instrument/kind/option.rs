use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// `OptionContract` specification containing all the information needed to fully identify an
/// option instrument.
///
/// # Type Parameters
/// * `AssetKey` - Type used to identify the settlement asset for the option contract.
///
/// # Fields
/// * `contract_size` - Multiplier that determines how many of the underlying asset the contract represents.
/// * `settlement_asset` - Asset used for settlement when the option is exercised.
/// * `kind` - Call (right to buy) or Put (right to sell).
/// * `exercise` - Exercise style (American, European, or Bermudan) defining when the option
///   can be exercised.
/// * `settlement` - How an exercised option settles: by delivering the underlying at the strike,
///   or in cash. Absent from a serialised contract, it defaults to
///   [`Physical`](OptionSettlement::Physical).
/// * `expiry` - The date and time when the option expires.
/// * `strike` - The price at which the option holder can buy (for calls) or sell (for puts)
///   the underlying asset upon exercise.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OptionContract<AssetKey> {
    pub contract_size: Decimal,
    pub settlement_asset: AssetKey,
    pub kind: OptionKind,
    pub exercise: OptionExercise,
    #[serde(default)]
    pub settlement: OptionSettlement,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
    pub strike: Decimal,
}
/// [`OptionContract`] kind - Put or Call.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OptionKind {
    #[serde(alias = "CALL", alias = "Call")]
    Call,
    #[serde(alias = "PUT", alias = "Put")]
    Put,
}

impl Display for OptionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                OptionKind::Call => "call",
                OptionKind::Put => "put",
            }
        )
    }
}

/// How an exercised [`OptionContract`] settles.
///
/// A contract term, like [`OptionExercise`]: it is fixed when the contract is listed, and two
/// contracts that differ only in it are different instruments.
#[derive(
    Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum OptionSettlement {
    /// Exercise delivers the underlying at the strike: a call buys it, a put sells it. US equity
    /// and ETF options settle this way.
    #[default]
    #[serde(alias = "PHYSICAL", alias = "Physical")]
    Physical,
    /// Exercise pays the intrinsic value in the settlement asset, and nothing is delivered. Index
    /// options settle this way.
    #[serde(alias = "CASH", alias = "Cash")]
    Cash,
}

impl Display for OptionSettlement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                OptionSettlement::Physical => "physical",
                OptionSettlement::Cash => "cash",
            }
        )
    }
}

/// [`OptionContract`] exercise style.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OptionExercise {
    #[serde(alias = "AMERICAN", alias = "American")]
    American,
    #[serde(alias = "BERMUDAN", alias = "Bermudan")]
    Bermudan,
    #[serde(alias = "EUROPEAN", alias = "European")]
    European,
}

impl Display for OptionExercise {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                OptionExercise::American => "american",
                OptionExercise::Bermudan => "bermudan",
                OptionExercise::European => "european",
            }
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn contract(settlement: OptionSettlement) -> OptionContract<String> {
        OptionContract {
            contract_size: dec!(100),
            settlement_asset: "usd".to_string(),
            kind: OptionKind::Call,
            exercise: OptionExercise::American,
            settlement,
            expiry: "2026-11-20T21:00:00Z".parse().unwrap(),
            strike: dec!(700),
        }
    }

    #[test]
    fn a_contract_serialised_without_settlement_loads_as_physical() {
        let json = r#"{
            "contract_size": "100",
            "settlement_asset": "usd",
            "kind": "call",
            "exercise": "american",
            "expiry": 1795208400000,
            "strike": "700"
        }"#;

        let contract: OptionContract<String> = serde_json::from_str(json).unwrap();

        assert_eq!(contract.settlement, OptionSettlement::Physical);
    }

    #[test]
    fn settlement_round_trips_in_lowercase() {
        for settlement in [OptionSettlement::Physical, OptionSettlement::Cash] {
            let json = serde_json::to_string(&contract(settlement)).unwrap();
            assert!(
                json.contains(&format!(r#""settlement":"{settlement}""#)),
                "{json}"
            );

            let back: OptionContract<String> = serde_json::from_str(&json).unwrap();
            assert_eq!(back.settlement, settlement);
        }
    }

    #[test]
    fn settlement_accepts_capitalised_aliases() {
        for (raw, expected) in [
            (r#""PHYSICAL""#, OptionSettlement::Physical),
            (r#""Physical""#, OptionSettlement::Physical),
            (r#""CASH""#, OptionSettlement::Cash),
            (r#""Cash""#, OptionSettlement::Cash),
        ] {
            assert_eq!(
                serde_json::from_str::<OptionSettlement>(raw).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn contracts_differing_only_in_settlement_are_different_instruments() {
        assert_ne!(
            contract(OptionSettlement::Physical),
            contract(OptionSettlement::Cash)
        );
    }
}
