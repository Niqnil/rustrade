//! Contract builders for IB integration.

use ibapi::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};

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
pub fn option_contract(
    symbol: &str,
    last_trade_date: &str,
    strike: f64,
    right: &str,
    exchange: &str,
    currency: &str,
) -> Contract {
    Contract {
        symbol: Symbol::new(symbol),
        security_type: SecurityType::Option,
        last_trade_date_or_contract_month: last_trade_date.to_string(),
        strike,
        right: right.to_string(),
        exchange: Exchange::new(exchange),
        currency: Currency::new(currency),
        ..Default::default()
    }
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
