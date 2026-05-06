//! Massive (formerly Polygon.io) market data connectors.
//!
//! Provides access to institutional-grade market data across all major asset classes:
//! stocks, options, indices, forex, crypto, and futures.
//!
//! # Testing Status
//!
//! **Partially integration-tested.** We have a currencies subscription (crypto + forex)
//! but not stocks, options, indices, or futures subscriptions. Verified endpoints:
//!
//! - ✅ REST aggregates (crypto, forex, stocks, options, futures via free tier)
//! - ✅ REST tick trades (crypto)
//! - ✅ REST quotes (forex)
//! - ✅ WebSocket streaming (crypto)
//! - ✅ Reference data (tickers, exchanges, market status, holidays)
//! - ✅ Corporate actions (dividends, splits)
//! - ❌ REST aggregates (indices) — requires indices subscription
//! - ❌ REST tick trades (stocks) — requires stocks subscription
//! - ❌ REST quotes (stocks) — requires stocks subscription
//! - ❌ WebSocket (stocks) — requires stocks subscription
//!
//! Unit tests for JSON transformation live inline in `reference.rs` and `transformer.rs`.
//! Live API integration tests are in `tests/massive_integration.rs`.
//!
//! # Architecture
//!
//! - [`MassiveRestClient`]: Historical and intraday data via REST API
//! - [`MassiveLive`]: Real-time streaming via WebSocket
//!
//! # Authentication
//!
//! Requires `MASSIVE_API_KEY` environment variable from an active Massive subscription.
//! Get your API key from: <https://massive.com/dashboard/api-keys>
//!
//! # Symbol Conventions
//!
//! **Important**: REST API and WebSocket use different symbol formats!
//!
//! ## REST API Symbols
//!
//! - `X:BTCUSD` — Crypto
//! - `C:EURUSD` — Forex
//! - `O:AAPL251219C00150000` — Options
//! - `I:SPX` — Indices
//! - `AAPL` — Stocks
//!
//! ## WebSocket Symbols
//!
//! - `BTC-USD` — Crypto (hyphenated)
//! - `EUR-USD` — Forex (hyphenated)
//! - `O:AAPL251219C00150000` — Options
//! - `AAPL` — Stocks
//!
//! # Examples
//!
//! ## REST Client
//!
//! ```ignore
//! use rustrade_data::exchange::massive::MassiveRestClient;
//!
//! let client = MassiveRestClient::from_env()?;
//! let candles = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to).await?;
//! ```
//!
//! ## WebSocket Client
//!
//! ```ignore
//! use rustrade_data::exchange::massive::{MassiveLive, Market, ChannelType};
//! use std::collections::HashMap;
//!
//! let instruments = HashMap::from([("BTC-USD".into(), "btc".into())]);
//! let mut client = MassiveLive::from_env(Market::Crypto, ExchangeId::Massive, instruments)?;
//! client.subscribe(&["BTC-USD"], ChannelType::Trade);
//! let stream = client.start().await?;
//! ```

mod error;
pub(crate) mod live;
pub(crate) mod reference;
pub(crate) mod rest;
pub(crate) mod transformer;

pub use error::MassiveError;
pub use live::{ChannelType, Market, MassiveLive};
pub use reference::{
    Address, CurrencyStatus, Dividend, DividendFrequency, DividendQuery, Exchange, MarketHoliday,
    MarketStatus, SortOrder, SplitQuery, StockSplit, Ticker, TickerDetails, TickerQuery,
};
pub use rest::MassiveRestClient;
pub use transformer::FairMarketValue;
