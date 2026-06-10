//! Contract builders for IB integration.

use ibapi::contracts::{Contract, Currency, Exchange, OptionRight, SecurityType, Symbol};
use thiserror::Error;

/// Reasons a [`ContractConfig`](super::ContractConfig) cannot be mapped to a
/// valid [`Contract`].
///
/// Every variant represents an input that would otherwise force the builder to
/// silently fabricate a *wrong* contract (e.g. defaulting a missing option
/// `right` to Call, or an unknown `security_type` to a stock). Surfacing these
/// at construction — rather than deferring to an opaque IBKR submission
/// rejection — keeps failures observable, per the library's contract.
///
/// `#[non_exhaustive]`: new validation reasons may be added without a breaking
/// change as the contract builders grow.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ContractConfigError {
    /// An `OPT` contract was configured without a `right`.
    #[error("OPT contract requires a `right` field (expected one of C/CALL/P/PUT)")]
    MissingOptionRight,

    /// A `right` was supplied but is not one of `C`/`CALL`/`P`/`PUT`.
    #[error("unrecognized option right {right:?} (expected one of C/CALL/P/PUT)")]
    UnrecognizedOptionRight { right: String },

    /// An `OPT` contract was configured without a `strike`.
    #[error("OPT contract requires a `strike` field")]
    MissingStrike,

    /// A `FUT` or `OPT` contract was configured without a `last_trade_date`.
    #[error("FUT/OPT contract requires a `last_trade_date` field")]
    MissingLastTradeDate,

    /// The `security_type` is not one of the supported `STK`/`FUT`/`OPT`/`CASH`.
    #[error("unrecognized security_type {security_type:?} (expected one of STK/FUT/OPT/CASH)")]
    UnrecognizedSecurityType { security_type: String },
}

/// Map a human/wire option-right string to `OptionRight`.
///
/// Accepts IBKR wire values (`"C"`/`"P"`) and common long forms
/// (`"CALL"`/`"PUT"`), case-insensitively. Returns `None` for anything else.
fn parse_option_right(right: &str) -> Option<OptionRight> {
    let right = right.trim();
    if right.eq_ignore_ascii_case("C") || right.eq_ignore_ascii_case("CALL") {
        Some(OptionRight::Call)
    } else if right.eq_ignore_ascii_case("P") || right.eq_ignore_ascii_case("PUT") {
        Some(OptionRight::Put)
    } else {
        None
    }
}

/// Build a stock contract for the given symbol.
pub fn stock_contract(symbol: &str, exchange: &str, currency: &str) -> Contract {
    Contract::stock(symbol)
        .on_exchange(exchange)
        .in_currency(currency)
        .build()
}

/// Build a futures contract.
pub fn futures_contract(
    symbol: &str,
    last_trade_date: &str,
    exchange: &str,
    currency: &str,
) -> Contract {
    Contract {
        symbol: Symbol::new(symbol),
        security_type: SecurityType::Future,
        last_trade_date_or_contract_month: last_trade_date.to_string(),
        exchange: Exchange::new(exchange),
        currency: Currency::new(currency),
        ..Default::default()
    }
}

/// Build an options contract.
///
/// # Errors
///
/// Returns [`ContractConfigError::UnrecognizedOptionRight`] if `right` is not
/// one of `C`/`CALL`/`P`/`PUT` (case-insensitive; leading and trailing
/// whitespace is ignored). An option contract is invalid without a valid right,
/// so this is surfaced at construction rather than as a later IBKR submission
/// rejection.
///
/// Only `right` is validated here. `last_trade_date` and `strike` are forwarded
/// to the IBKR contract as-is; an empty date or a `0.0` strike will build a
/// (quietly wrong) contract. Presence of those fields is checked one level up in
/// [`ContractConfig::to_contract`](super::ContractConfig::to_contract), which is
/// the intended entry point for config-driven construction.
pub fn option_contract(
    symbol: &str,
    last_trade_date: &str,
    strike: f64,
    right: &str,
    exchange: &str,
    currency: &str,
) -> Result<Contract, ContractConfigError> {
    let right =
        parse_option_right(right).ok_or_else(|| ContractConfigError::UnrecognizedOptionRight {
            right: right.to_string(),
        })?;
    Ok(Contract {
        symbol: Symbol::new(symbol),
        security_type: SecurityType::Option,
        last_trade_date_or_contract_month: last_trade_date.to_string(),
        strike,
        right: Some(right),
        exchange: Exchange::new(exchange),
        currency: Currency::new(currency),
        ..Default::default()
    })
}

/// Build a forex contract.
pub fn forex_contract(symbol: &str, currency: &str) -> Contract {
    Contract {
        symbol: Symbol::new(symbol),
        security_type: SecurityType::ForexPair,
        exchange: Exchange::new("IDEALPRO"),
        currency: Currency::new(currency),
        ..Default::default()
    }
}
