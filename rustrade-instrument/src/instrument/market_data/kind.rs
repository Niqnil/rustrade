use crate::instrument::kind::option::{OptionExercise, OptionKind};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Defines the type of [`MarketDataInstrument`](super::MarketDataInstrument) which is being
/// traded on a given `base_quote` market.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataInstrumentKind {
    #[default]
    Spot,
    Perpetual,
    Future(MarketDataFutureContract),
    Option(MarketDataOptionContract),
    /// Contract-for-difference.
    ///
    /// A unit variant, unlike its [`InstrumentKind::Cfd`](crate::instrument::kind::InstrumentKind)
    /// counterpart: a CFD has no expiry or strike to bind a subscription on, and the market-data
    /// twin needs neither `contract_size` nor `settlement_asset` (both are execution-side
    /// concerns). Shaped like `Perpetual` for exactly that reason.
    ///
    /// Kept distinct from `Spot` rather than folded into it, because a single connector can serve
    /// both a spot instrument and a CFD on the same `(exchange, base, quote)` — IBKR's `secType`
    /// `STK` vs `CFD` being the concrete case. Folded, subscription binding would resolve to
    /// whichever of the two iterated first.
    Cfd,
}

impl Display for MarketDataInstrumentKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MarketDataInstrumentKind::Spot => "spot".to_string(),
                MarketDataInstrumentKind::Perpetual => "perpetual".to_string(),
                MarketDataInstrumentKind::Cfd => "cfd".to_string(),
                MarketDataInstrumentKind::Future(contract) =>
                    format!("future_{}-UTC", contract.expiry.date_naive()),
                MarketDataInstrumentKind::Option(contract) => format!(
                    "option_{}_{}_{}-UTC_{}",
                    contract.kind,
                    contract.exercise,
                    contract.expiry.date_naive(),
                    contract.strike,
                ),
            }
        )
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MarketDataFutureContract {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MarketDataOptionContract {
    pub kind: OptionKind,
    pub exercise: OptionExercise,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub expiry: DateTime<Utc>,
    pub strike: Decimal,
}
