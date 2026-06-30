//! Alpaca options market data client.
//!
//! Provides REST-based option contract discovery and chain snapshots with Greeks.
//!
//! # Endpoints
//!
//! - [`AlpacaOptionsClient::fetch_contracts`]: `GET /v2/options/contracts` — discover available contracts
//! - [`AlpacaOptionsClient::fetch_snapshots`]: `GET /v1beta1/options/snapshots` — chain snapshots with Greeks
//!
//! # Authentication
//!
//! Requires `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` environment variables.
//!
//! # Data Feeds
//!
//! - [`AlpacaOptionFeed::Opra`]: Real-time OPRA feed (requires paid subscription)
//! - [`AlpacaOptionFeed::Indicative`]: 15-minute delayed feed (free)
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::alpaca::options::{
//!     AlpacaOptionsClient, AlpacaOptionContractQuery, AlpacaOptionFeed,
//! };
//! use chrono::NaiveDate;
//!
//! let client = AlpacaOptionsClient::from_env()?;
//!
//! // Discover AAPL options expiring in next 30 days
//! let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()])
//!     .expiration_gte(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
//!     .expiration_lte(NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
//!
//! let contracts = client.fetch_contracts(&query).await?;
//!
//! // Fetch snapshots with Greeks (using free delayed feed)
//! let symbols: Vec<_> = contracts.iter().map(|c| c.symbol.clone()).collect();
//! let snapshots = client.fetch_snapshots(&symbols, AlpacaOptionFeed::Indicative).await?;
//! ```
//!
//! # Testing Status
//!
//! **Tested locally, CI planned (free tier — paper trading allowed):**
//! - Contract discovery via `fetch_contracts()`
//! - Indicative feed (15-min delayed) via `fetch_snapshots()`
//!
//! **NOT tested (requires OPRA subscription via Algo Trader Plus):**
//! - OPRA real-time feed — implemented but unverified against real endpoints
//!
//! # Limitations
//!
//! - **REST only**: Greeks streaming is NOT available via WebSocket. Use snapshots for
//!   point-in-time Greeks data.
//! - **Rate limits**: Alpaca applies rate limits; the client handles 429 responses with retry.

mod contracts;
mod snapshots;

pub use contracts::{AlpacaOptionContract, AlpacaOptionContractQuery};
pub use snapshots::{AlpacaOptionFeed, AlpacaOptionQuote, AlpacaOptionSnapshot, AlpacaOptionTrade};

use super::rest::AlpacaRestClient;

/// Errors from Alpaca options API operations.
///
/// Alias of the shared [`AlpacaRestError`](super::super::rest::AlpacaRestError): options requests
/// have no failure modes beyond the common transport/protocol ones, so the single REST error type
/// is reused rather than duplicated per endpoint family.
pub use super::rest::AlpacaRestError as AlpacaOptionsError;

/// Alpaca options market data client.
///
/// Provides access to option contract discovery and chain snapshots via REST API.
/// The client handles authentication, rate limiting, and pagination automatically.
///
/// # Construction
///
/// Use [`AlpacaOptionsClient::from_env`] to create a client using environment variables,
/// or [`AlpacaOptionsClient::new`] for explicit credentials.
///
/// Internally this is a thin wrapper over the shared [`AlpacaRestClient`] transport.
#[derive(Clone, Debug)]
pub struct AlpacaOptionsClient {
    rest: AlpacaRestClient,
}

impl AlpacaOptionsClient {
    /// Create a new client with explicit credentials.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Alpaca API key
    /// * `api_secret` - Alpaca API secret
    /// * `paper` - Use paper trading endpoint if true
    ///
    /// # Errors
    ///
    /// Returns error if the HTTP client cannot be built (invalid headers).
    pub fn new(api_key: &str, api_secret: &str, paper: bool) -> Result<Self, AlpacaOptionsError> {
        Ok(Self {
            rest: AlpacaRestClient::new(api_key, api_secret, paper)?,
        })
    }

    /// Create a client from environment variables.
    ///
    /// Reads:
    /// - `ALPACA_API_KEY` - API key (required)
    /// - `ALPACA_SECRET_KEY` - API secret (required)
    /// - `ALPACA_PAPER` - Set to "true" for paper trading (optional, defaults to false)
    ///
    /// # Errors
    ///
    /// Returns error if required environment variables are missing.
    pub fn from_env() -> Result<Self, AlpacaOptionsError> {
        Ok(Self {
            rest: AlpacaRestClient::from_env()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::AlpacaOptionsClient;

    #[test]
    fn client_debug_hides_credentials() {
        let client = AlpacaOptionsClient::new("test-key-id", "test-secret-value", true)
            .expect("client construction with ASCII credentials should succeed");
        let debug_str = format!("{client:?}");

        assert!(!debug_str.contains("test-key-id"));
        assert!(!debug_str.contains("test-secret-value"));
    }
}
