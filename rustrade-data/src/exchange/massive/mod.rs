//! Massive (formerly Polygon.io) market data connectors.
//!
//! Provides access to institutional-grade market data across all major asset classes:
//! stocks, options, indices, forex, crypto, and futures.
//!
//! # Architecture
//!
//! - [`MassiveRestClient`]: Historical and intraday data via REST API
//!
//! WebSocket streaming (`MassiveLive`) is planned for a future release.
//!
//! # Authentication
//!
//! Requires `MASSIVE_API_KEY` environment variable from an active Massive subscription.
//! Get your API key from: <https://massive.com/dashboard/api-keys>
//!
//! # Symbol Conventions
//!
//! Massive uses prefix conventions to identify asset classes:
//! - `X:BTCUSD` — Crypto
//! - `C:EURUSD` — Forex
//! - `O:AAPL251219C00150000` — Options
//! - `I:SPX` — Indices
//! - (no prefix) — Stocks
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::massive::MassiveRestClient;
//!
//! let client = MassiveRestClient::from_env()?;
//! let candles = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to).await?;
//! ```

mod error;
pub mod rest;
pub(crate) mod transformer;

pub use error::MassiveError;
pub use rest::MassiveRestClient;
pub use transformer::FairMarketValue;
