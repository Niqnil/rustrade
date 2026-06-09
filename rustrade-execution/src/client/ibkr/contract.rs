//! Contract builders for IB integration.

use ibapi::contracts::{Contract, Currency, Exchange, OptionRight, SecurityType, Symbol};

/// Error returned by [`option_contract`] when the `right` string cannot be
/// mapped to an [`OptionRight`].
///
/// Carries the offending input so callers can surface it (via [`Display`] or
/// [`InvalidOptionRight::right`]). Building an option contract without a valid
/// right is rejected at construction rather than deferred to an opaque IBKR
/// submission failure.
///
/// [`Display`]: std::fmt::Display
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidOptionRight(String);

impl InvalidOptionRight {
    /// The unrecognized `right` input that triggered this error.
    #[must_use]
    pub fn right(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InvalidOptionRight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unrecognized option right {:?} (expected one of C/CALL/P/PUT)",
            self.0
        )
    }
}

impl std::error::Error for InvalidOptionRight {}

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
/// Returns [`InvalidOptionRight`] if `right` is not one of `C`/`CALL`/`P`/`PUT`
/// (case-insensitive; leading and trailing whitespace is ignored). An option
/// contract is invalid without a right, so this is surfaced at construction
/// rather than as a later IBKR submission rejection.
pub fn option_contract(
    symbol: &str,
    last_trade_date: &str,
    strike: f64,
    right: &str,
    exchange: &str,
    currency: &str,
) -> Result<Contract, InvalidOptionRight> {
    let right = parse_option_right(right).ok_or_else(|| InvalidOptionRight(right.to_string()))?;
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
