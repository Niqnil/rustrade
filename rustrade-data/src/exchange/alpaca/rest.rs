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

/// Delay when rate-limited without a readable wait, and the longest wait read from Alpaca's
/// headers: the length of its rate-limit window (see [`parse_rate_limit_delay`]).
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

    /// The request could not be cloned for a retry — `reqwest` returns `None` from `try_clone`
    /// only for streaming request bodies. Never occurs for the GET/query-string requests this
    /// client issues today, but returned (not panicked) so a future streaming-body caller of the
    /// shared retry helper recovers instead of aborting the task.
    #[error("request body is not cloneable; cannot retry (streaming bodies are unsupported)")]
    NotCloneable,

    /// A paginated fetch exceeded the maximum number of pages before the API reported the end of
    /// the result set.
    ///
    /// Returned as a terminal error on the affected fetch. For a market-data client a silent
    /// truncation is indistinguishable from a genuinely small result set, so pagination fails
    /// loudly rather than returning a partial result. Hitting this limit indicates either an
    /// unexpectedly large query or a server that never signals the final page — inspect the query
    /// before retrying.
    #[error("pagination exceeded the {limit}-page safety limit (attempted page {pages})")]
    PaginationLimitExceeded { pages: usize, limit: usize },

    /// A paginated fetch received a `next_page_token` it had already used as a cursor.
    ///
    /// Returned as a terminal error on the affected fetch. A repeated token would otherwise
    /// re-fetch the same pages until the page cap ran out; detecting the cycle turns silent
    /// non-termination into an observable failure after a single wasted round-trip. The token is
    /// server-supplied, so the retained value is truncated to a bounded diagnostic prefix (real
    /// Alpaca tokens are far shorter than the bound and are retained whole).
    #[error("pagination cycle detected: page_token {page_token:?} was already used")]
    CyclicPagination { page_token: String },
}

/// Authenticated Alpaca REST client.
///
/// Holds the configured [`reqwest::Client`] (with auth headers + timeout) plus the broker and data
/// API base URLs. Endpoint-family wrappers (`AlpacaOptionsClient`, the corporate-action source, …)
/// build requests against these bases and drive them through a shared retrying request helper.
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

    /// Override both API base URLs (broker and data).
    ///
    /// Points the client at API-compatible endpoints other than the production Alpaca hosts —
    /// a local mock server in tests, or a proxy/gateway. Credentials, timeouts and retry
    /// behaviour are unchanged.
    ///
    /// Infallible by design: unlike a `next_url`-following client, this client only ever
    /// requests URLs it builds itself from these bases (pagination advances via an opaque
    /// `page_token` query parameter, never a server-supplied URL), so there is no trusted
    /// origin to derive — and nothing to fail on — at construction.
    #[must_use]
    pub fn with_base_urls(
        mut self,
        broker_base: impl Into<String>,
        data_base: impl Into<String>,
    ) -> Self {
        self.broker_base = broker_base.into();
        self.data_base = data_base.into();
        self
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
    ///
    /// # Errors
    /// Returns [`AlpacaRestError::NotCloneable`] if the request carries a streaming body (`reqwest`
    /// cannot clone it for a retry). This never happens for the GET requests this client issues, but
    /// is surfaced as a recoverable `Err` — consistent with every other failure mode of this shared
    /// helper — rather than a panic, so a future streaming-body caller can handle it.
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

            // `try_clone` returns `None` only for streaming request bodies. Every current caller
            // passes a GET/query-string request (no body), so this always clones — but return a
            // typed `Err` rather than panic: this shared retry helper already surfaces every other
            // failure (rate limit, HTTP status, deserialisation) as an `AlpacaRestError`, and the
            // function returns `Result`, so an added error branch is free and lets a future
            // streaming-body caller recover instead of aborting the task.
            let response = match request.try_clone() {
                Some(request) => request.send().await?,
                None => return Err(AlpacaRestError::NotCloneable),
            };

            let status = response.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempts >= MAX_RETRY_ATTEMPTS {
                    return Err(AlpacaRestError::RateLimitExceeded { attempts });
                }

                let delay = parse_rate_limit_delay(response.headers())
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

/// How long Alpaca asks a rate-limited client to wait, from a 429's headers.
///
/// `Retry-After`, a delay in seconds, when present: taken as given, zero included, and not read in
/// its HTTP-date form. Otherwise `X-RateLimit-Reset`, which Alpaca documents: a Unix timestamp in
/// seconds, read as the time from now until then, at least 1 s so a reset already past never
/// busy-loops. `None` when neither header is readable.
///
/// Either is capped at [`DEFAULT_RATE_LIMIT_DELAY_SECS`], the length of Alpaca's window: a longer
/// wait is a bad header or clock skew, and must not stall the client for longer.
///
/// The execution crate's Alpaca client reads a 429 by the same rule.
fn parse_rate_limit_delay(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
    };

    let seconds = match header("retry-after") {
        Some(seconds) => seconds,
        None => {
            let reset = header("x-ratelimit-reset")?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            reset.saturating_sub(now).max(1)
        }
    };
    Some(Duration::from_secs(
        seconds.min(DEFAULT_RATE_LIMIT_DELAY_SECS),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::{AlpacaRestClient, DEFAULT_RATE_LIMIT_DELAY_SECS, parse_rate_limit_delay};
    use reqwest::header::HeaderMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn headers(pairs: &[(&'static str, String)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, value.parse().unwrap());
        }
        headers
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn a_rate_limit_delay_prefers_retry_after_then_reads_the_reset_time() {
        let both = headers(&[
            ("retry-after", "7".to_string()),
            ("x-ratelimit-reset", (now() + 30).to_string()),
        ]);
        assert_eq!(parse_rate_limit_delay(&both), Some(Duration::from_secs(7)));

        let reset =
            parse_rate_limit_delay(&headers(&[("x-ratelimit-reset", (now() + 30).to_string())]))
                .unwrap();
        assert!(
            (Duration::from_secs(29)..=Duration::from_secs(30)).contains(&reset),
            "{reset:?}"
        );

        let past = headers(&[("x-ratelimit-reset", (now() - 5).to_string())]);
        assert_eq!(parse_rate_limit_delay(&past), Some(Duration::from_secs(1)));

        assert_eq!(parse_rate_limit_delay(&HeaderMap::new()), None);
        let malformed = headers(&[("retry-after", "soon".to_string())]);
        assert_eq!(parse_rate_limit_delay(&malformed), None);

        let window = Some(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));
        for name in ["retry-after", "x-ratelimit-reset"] {
            let huge = headers(&[(name, u64::MAX.to_string())]);
            assert_eq!(parse_rate_limit_delay(&huge), window, "{name}");
        }
    }

    #[test]
    fn client_debug_hides_credentials() {
        let client = AlpacaRestClient::new("test-key-id", "test-secret-value", true)
            .expect("client construction with ASCII credentials should succeed");
        let debug_str = format!("{client:?}");

        assert!(!debug_str.contains("test-key-id"));
        assert!(!debug_str.contains("test-secret-value"));
    }
}
