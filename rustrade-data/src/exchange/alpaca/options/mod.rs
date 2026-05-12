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
//! # Limitations
//!
//! - **REST only**: Greeks streaming is NOT available via WebSocket. Use snapshots for
//!   point-in-time Greeks data.
//! - **Rate limits**: Alpaca applies rate limits; the client handles 429 responses with retry.

mod contracts;
mod snapshots;

pub use contracts::{AlpacaOptionContract, AlpacaOptionContractQuery};
pub use snapshots::{AlpacaOptionFeed, AlpacaOptionQuote, AlpacaOptionSnapshot, AlpacaOptionTrade};

use reqwest::header::{HeaderMap, HeaderValue};
use std::{env, time::Duration};
use thiserror::Error;
use tracing::{debug, warn};

/// Base URL for Alpaca Broker API (trading + options contracts).
const BROKER_API_BASE: &str = "https://api.alpaca.markets";

/// Base URL for Alpaca Data API (market data including options snapshots).
const DATA_API_BASE: &str = "https://data.alpaca.markets";

/// Paper trading base URL.
const PAPER_API_BASE: &str = "https://paper-api.alpaca.markets";

/// Default timeout for REST requests.
const REQUEST_TIMEOUT_SECS: u64 = 30;

/// Maximum retry attempts for rate-limited requests.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Default delay when rate-limited (if no Retry-After header).
const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 60;

/// Errors from Alpaca options API operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AlpacaOptionsError {
    /// Environment variable not set or invalid.
    #[error("environment variable error: {0}")]
    EnvVar(String),

    /// HTTP client error.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// API returned an error response.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// Rate limit exceeded after retries.
    #[error("rate limit exceeded after {attempts} attempts")]
    RateLimitExceeded { attempts: u32 },

    /// Invalid response format.
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

/// Alpaca options market data client.
///
/// Provides access to option contract discovery and chain snapshots via REST API.
/// The client handles authentication, rate limiting, and pagination automatically.
///
/// # Construction
///
/// Use [`AlpacaOptionsClient::from_env`] to create a client using environment variables,
/// or [`AlpacaOptionsClient::new`] for explicit credentials.
#[derive(Clone)]
pub struct AlpacaOptionsClient {
    http: reqwest::Client,
    broker_base: String,
    data_base: String,
}

impl std::fmt::Debug for AlpacaOptionsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaOptionsClient")
            .field("broker_base", &self.broker_base)
            .field("data_base", &self.data_base)
            .finish_non_exhaustive()
    }
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
        let mut headers = HeaderMap::new();
        headers.insert(
            "APCA-API-KEY-ID",
            HeaderValue::from_str(api_key)
                .map_err(|e| AlpacaOptionsError::EnvVar(format!("invalid API key: {e}")))?,
        );
        headers.insert(
            "APCA-API-SECRET-KEY",
            HeaderValue::from_str(api_secret)
                .map_err(|e| AlpacaOptionsError::EnvVar(format!("invalid API secret: {e}")))?,
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()?;

        let broker_base = if paper {
            PAPER_API_BASE.to_string()
        } else {
            BROKER_API_BASE.to_string()
        };

        Ok(Self {
            http,
            broker_base,
            data_base: DATA_API_BASE.to_string(),
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
        let api_key = env::var("ALPACA_API_KEY")
            .map_err(|e| AlpacaOptionsError::EnvVar(format!("ALPACA_API_KEY: {e}")))?;
        let api_secret = env::var("ALPACA_SECRET_KEY")
            .map_err(|e| AlpacaOptionsError::EnvVar(format!("ALPACA_SECRET_KEY: {e}")))?;
        let paper = env::var("ALPACA_PAPER")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Self::new(&api_key, &api_secret, paper)
    }

    /// Execute a request with rate limit retry.
    async fn request_with_retry<T>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, AlpacaOptionsError>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut attempts = 0u32;

        loop {
            attempts += 1;

            // `try_clone` returns `None` only for streaming bodies; safe for these GET requests.
            let response = request
                .try_clone()
                .ok_or_else(|| {
                    AlpacaOptionsError::InvalidResponse(
                        "request body is not cloneable; cannot retry".into(),
                    )
                })?
                .send()
                .await?;

            let status = response.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempts >= MAX_RETRY_ATTEMPTS {
                    return Err(AlpacaOptionsError::RateLimitExceeded { attempts });
                }

                let delay = parse_retry_after(response.headers())
                    .unwrap_or(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));

                warn!(
                    attempt = attempts,
                    delay_secs = delay.as_secs(),
                    "Alpaca rate limited, retrying"
                );
                tokio::time::sleep(delay).await;
                continue;
            }

            if !status.is_success() {
                let message = response.text().await.unwrap_or_default();
                return Err(AlpacaOptionsError::Api {
                    status: status.as_u16(),
                    message,
                });
            }

            let body = response.text().await?;
            debug!(len = body.len(), "Alpaca response received");

            return serde_json::from_str(&body).map_err(|e| {
                AlpacaOptionsError::InvalidResponse(format!("JSON parse error: {e}"))
            });
        }
    }
}

/// Parse the `Retry-After` header (delay in seconds).
///
/// `x-ratelimit-reset` is intentionally not used as a fallback because it carries
/// a Unix epoch timestamp, not a duration; mis-interpreting it would produce
/// sleeps measured in years.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
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
