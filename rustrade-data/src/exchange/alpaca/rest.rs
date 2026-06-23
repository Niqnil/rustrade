//! Shared Alpaca REST transport.
//!
//! A single authenticated [`reqwest::Client`] with rate-limit retry and JSON deserialisation,
//! shared by every Alpaca REST surface (options contracts/snapshots, corporate actions, …) so the
//! authentication, retry policy, and error type live in one place instead of being copied per
//! endpoint family.
//!
//! # Authentication
//!
//! Requires `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` (and the optional `ALPACA_PAPER`) environment
//! variables when constructed via [`AlpacaRestClient::from_env`], or explicit credentials via
//! [`AlpacaRestClient::new`]. Credentials are sent as the `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY`
//! headers on every request.

use reqwest::header::{HeaderMap, HeaderValue};
use std::{env, time::Duration};
use thiserror::Error;
use tracing::{debug, warn};

/// Base URL for Alpaca Broker API (trading + options contracts).
const BROKER_API_BASE: &str = "https://api.alpaca.markets";

/// Base URL for Alpaca Data API (market data, corporate actions, options snapshots).
const DATA_API_BASE: &str = "https://data.alpaca.markets";

/// Paper trading base URL.
const PAPER_API_BASE: &str = "https://paper-api.alpaca.markets";

/// Default timeout for REST requests.
const REQUEST_TIMEOUT_SECS: u64 = 30;

/// Maximum retry attempts for rate-limited requests.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Default delay when rate-limited (if no `Retry-After` header).
const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 60;

/// Errors from Alpaca REST API operations.
///
/// Shared across every Alpaca REST surface (options, corporate actions, …); the variants are
/// transport/protocol concerns (auth, HTTP, API status, rate limiting, deserialisation) common to
/// all of them, so there is no endpoint-specific error type to keep in sync.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AlpacaRestError {
    /// A required environment variable is not set (see [`AlpacaRestClient::from_env`]).
    #[error("environment variable error: {0}")]
    EnvVar(String),

    /// A supplied credential cannot be encoded as an HTTP header value (e.g. non-ASCII bytes).
    #[error("invalid credential: {0}")]
    InvalidCredential(String),

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

/// Authenticated Alpaca REST client.
///
/// Holds the configured [`reqwest::Client`] (with auth headers + timeout) plus the broker and data
/// API base URLs. Endpoint-family wrappers (`AlpacaOptionsClient`, the corporate-action source, …)
/// build requests against these bases and drive them through [`request_with_retry`].
///
/// [`request_with_retry`]: AlpacaRestClient::request_with_retry
#[derive(Clone)]
pub struct AlpacaRestClient {
    http: reqwest::Client,
    broker_base: String,
    data_base: String,
}

impl std::fmt::Debug for AlpacaRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client` (and therefore its auth headers) so credentials
        // never leak through `Debug`.
        f.debug_struct("AlpacaRestClient")
            .field("broker_base", &self.broker_base)
            .field("data_base", &self.data_base)
            .finish_non_exhaustive()
    }
}

impl AlpacaRestClient {
    /// Create a new client with explicit credentials.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Alpaca API key
    /// * `api_secret` - Alpaca API secret
    /// * `paper` - Use the paper trading broker endpoint if true
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built (e.g. non-ASCII credentials that cannot
    /// be encoded as header values).
    pub fn new(api_key: &str, api_secret: &str, paper: bool) -> Result<Self, AlpacaRestError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "APCA-API-KEY-ID",
            HeaderValue::from_str(api_key)
                .map_err(|e| AlpacaRestError::InvalidCredential(format!("invalid API key: {e}")))?,
        );
        headers.insert(
            "APCA-API-SECRET-KEY",
            HeaderValue::from_str(api_secret).map_err(|e| {
                AlpacaRestError::InvalidCredential(format!("invalid API secret: {e}"))
            })?,
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
    /// Returns an error if either required environment variable is missing or the client cannot be
    /// built.
    pub fn from_env() -> Result<Self, AlpacaRestError> {
        let api_key = env::var("ALPACA_API_KEY")
            .map_err(|e| AlpacaRestError::EnvVar(format!("ALPACA_API_KEY: {e}")))?;
        let api_secret = env::var("ALPACA_SECRET_KEY")
            .map_err(|e| AlpacaRestError::EnvVar(format!("ALPACA_SECRET_KEY: {e}")))?;
        let paper = env::var("ALPACA_PAPER")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Self::new(&api_key, &api_secret, paper)
    }

    /// The broker API base URL (`https://api.alpaca.markets`, or the paper endpoint).
    pub(crate) fn broker_base(&self) -> &str {
        &self.broker_base
    }

    /// The data API base URL (`https://data.alpaca.markets`).
    pub(crate) fn data_base(&self) -> &str {
        &self.data_base
    }

    /// Begin a `GET` request against `url`, pre-authenticated with the client's default headers.
    pub(crate) fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.http.get(url)
    }

    /// Execute a request, retrying on HTTP 429 (rate limited), then deserialise the JSON body.
    ///
    /// The request must be cloneable (true for the query-string GET requests used here) so it can
    /// be re-sent across retries.
    pub(crate) async fn request_with_retry<T>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, AlpacaRestError>
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
                    AlpacaRestError::InvalidResponse(
                        "request body is not cloneable; cannot retry".into(),
                    )
                })?
                .send()
                .await?;

            let status = response.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempts >= MAX_RETRY_ATTEMPTS {
                    return Err(AlpacaRestError::RateLimitExceeded { attempts });
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
                return Err(AlpacaRestError::Api {
                    status: status.as_u16(),
                    message,
                });
            }

            let body = response.text().await?;
            debug!(len = body.len(), "Alpaca response received");

            return serde_json::from_str(&body)
                .map_err(|e| AlpacaRestError::InvalidResponse(format!("JSON parse error: {e}")));
        }
    }
}

/// Parse the `Retry-After` header (delay in seconds).
///
/// `x-ratelimit-reset` is intentionally not used as a fallback because it carries a Unix epoch
/// timestamp, not a duration; mis-interpreting it would produce sleeps measured in years.
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
    use super::AlpacaRestClient;

    #[test]
    fn client_debug_hides_credentials() {
        let client = AlpacaRestClient::new("test-key-id", "test-secret-value", true)
            .expect("client construction with ASCII credentials should succeed");
        let debug_str = format!("{client:?}");

        assert!(!debug_str.contains("test-key-id"));
        assert!(!debug_str.contains("test-secret-value"));
    }
}
