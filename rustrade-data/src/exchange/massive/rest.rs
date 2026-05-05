//! REST client for Massive historical and intraday data.
//!
//! Provides access to aggregates (OHLCV), trades, and quotes across all asset classes.

use super::error::MassiveError;
use super::transformer::{
    AggregatesResponse, QuotesResponse, TradesResponse, parse_aggregates_response,
    parse_quotes_response, parse_trades_response, timespan_to_duration,
};
use crate::subscription::{book::OrderBookL1, candle::Candle, trade::PublicTrade};
use async_stream::try_stream;
use futures::Stream;
use reqwest::{Client, StatusCode, header};
use std::env;
use std::time::Duration;
use tracing::debug;

const BASE_URL: &str = "https://api.massive.com";
const ENV_API_KEY: &str = "MASSIVE_API_KEY";

/// Truncate response body for error messages (max 512 chars, UTF-8 safe).
fn truncate_body(body: &str) -> String {
    let boundary = body.floor_char_boundary(512);
    body[..boundary].to_owned()
}

/// Validate ticker doesn't contain URL-breaking characters.
fn validate_ticker(ticker: &str) -> Result<(), MassiveError> {
    if ticker.is_empty() {
        return Err(MassiveError::InvalidInput {
            message: "ticker must not be empty".into(),
        });
    }
    if ticker.contains(['/', '?', '#', ' ', '%']) {
        return Err(MassiveError::InvalidInput {
            message: "ticker contains invalid URL characters".into(),
        });
    }
    Ok(())
}

/// Validate next_url is from the expected origin to prevent token leakage.
fn validate_next_url(next_url: &str, base_url: &str) -> Result<(), MassiveError> {
    if !next_url.starts_with(base_url) {
        return Err(MassiveError::InvalidInput {
            message: format!(
                "next_url origin mismatch: expected {}, got {}",
                base_url, next_url
            ),
        });
    }
    Ok(())
}

/// REST client for Massive historical and intraday market data.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::massive::MassiveRestClient;
/// use chrono::{Utc, Duration};
///
/// let client = MassiveRestClient::from_env()?;
/// let to = Utc::now();
/// let from = to - Duration::days(1);
///
/// let mut stream = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to);
/// while let Some(candle) = stream.next().await {
///     println!("{:?}", candle?);
/// }
/// ```
#[derive(Clone)]
pub struct MassiveRestClient {
    client: Client,
    #[allow(dead_code)] // Retained for WebSocket auth; HTTP auth is in client headers
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for MassiveRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MassiveRestClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl MassiveRestClient {
    /// Create a new client with explicit API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Massive API key from <https://massive.com/dashboard/api-keys>
    pub fn new(api_key: impl Into<String>) -> Result<Self, MassiveError> {
        let api_key = api_key.into();
        let mut headers = header::HeaderMap::new();
        let auth_value =
            header::HeaderValue::from_str(&format!("Bearer {}", api_key)).map_err(|e| {
                MassiveError::Auth {
                    message: format!("Invalid API key format: {}", e),
                }
            })?;
        headers.insert(header::AUTHORIZATION, auth_value);

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            api_key,
            base_url: BASE_URL.to_string(),
        })
    }

    /// Create a new client from `MASSIVE_API_KEY` environment variable.
    pub fn from_env() -> Result<Self, MassiveError> {
        let api_key =
            env::var(ENV_API_KEY).map_err(|_| MassiveError::EnvVar { var: ENV_API_KEY })?;
        Self::new(api_key)
    }

    /// Override the base URL (useful for testing or legacy polygon.io endpoint).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Fetch a page body from the given URL with standard error handling.
    async fn fetch_page_body(&self, url: &str) -> Result<String, MassiveError> {
        let response = self.client.get(url).send().await?;
        let status = response.status();

        // Extract retry-after before consuming response
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        // Check rate limit before consuming body (avoids wasted I/O on 429)
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(MassiveError::RateLimited { retry_after });
        }

        let body = response.text().await?;

        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(MassiveError::Auth {
                message: truncate_body(&body),
            });
        }

        if !status.is_success() {
            return Err(MassiveError::Api {
                status: status.as_u16(),
                message: truncate_body(&body),
            });
        }

        Ok(body)
    }

    /// Fetch a single page of aggregates from the given URL.
    async fn fetch_aggregates_page(&self, url: &str) -> Result<AggregatesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_aggregates_response(&body)
    }

    /// Fetch aggregated OHLCV bars for a symbol.
    ///
    /// Returns a stream that handles pagination automatically. Does not collect
    /// results into memory — processes each page as it arrives.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `X:BTCUSD`, `C:EURUSD`, `AAPL`)
    /// * `multiplier` - Size of the timespan multiplier (e.g., 1, 5, 15)
    /// * `timespan` - Size unit: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stream = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to);
    /// ```
    pub fn fetch_aggregates<'a>(
        &'a self,
        ticker: &'a str,
        multiplier: u32,
        timespan: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<Candle, MassiveError>> + 'a {
        try_stream! {
            validate_ticker(ticker)?;

            let from_ms = from.timestamp_millis();
            let to_ms = to.timestamp_millis();

            let initial_url = format!(
                "{}/v2/aggs/ticker/{}/range/{}/{}/{}/{}?adjusted=true&sort=asc&limit=50000",
                self.base_url, ticker, multiplier, timespan, from_ms, to_ms
            );

            let mut next_url: Option<String> = Some(initial_url);

            // Pre-compute duration once for all bars in this stream
            let bar_duration = timespan_to_duration(multiplier, timespan);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching aggregates page");

                let parsed = self.fetch_aggregates_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed aggregates response"
                );

                if let Some(results) = parsed.results {
                    for bar in results {
                        yield bar.into_candle_with_duration(bar_duration);
                    }
                }

                // Validate next_url origin before following
                if let Some(ref url) = parsed.next_url {
                    validate_next_url(url, &self.base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch tick-level trades for a symbol.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `X:BTCUSD`, `AAPL`)
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    pub fn fetch_trades<'a>(
        &'a self,
        ticker: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<PublicTrade, MassiveError>> + 'a {
        try_stream! {
            validate_ticker(ticker)?;

            let from_ns = from.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "from timestamp out of nanosecond range (~1678-2262)".into(),
            })?;
            let to_ns = to.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "to timestamp out of nanosecond range (~1678-2262)".into(),
            })?;

            let initial_url = format!(
                "{}/v3/trades/{}?timestamp.gte={}&timestamp.lte={}&limit=50000&sort=timestamp&order=asc",
                self.base_url, ticker, from_ns, to_ns
            );

            let mut next_url: Option<String> = Some(initial_url);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching trades page");

                let parsed = self.fetch_trades_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed trades response"
                );

                if let Some(results) = parsed.results {
                    for trade in results {
                        yield trade.into_public_trade();
                    }
                }

                // Validate next_url origin before following
                if let Some(ref url) = parsed.next_url {
                    validate_next_url(url, &self.base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch a single page of trades from the given URL.
    async fn fetch_trades_page(&self, url: &str) -> Result<TradesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_trades_response(&body)
    }

    /// Fetch quotes (BBO/NBBO) for a symbol.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `C:EURUSD`, `AAPL`)
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    pub fn fetch_quotes<'a>(
        &'a self,
        ticker: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<OrderBookL1, MassiveError>> + 'a {
        try_stream! {
            validate_ticker(ticker)?;

            let from_ns = from.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "from timestamp out of nanosecond range (~1678-2262)".into(),
            })?;
            let to_ns = to.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "to timestamp out of nanosecond range (~1678-2262)".into(),
            })?;

            let initial_url = format!(
                "{}/v3/quotes/{}?timestamp.gte={}&timestamp.lte={}&limit=50000&sort=timestamp&order=asc",
                self.base_url, ticker, from_ns, to_ns
            );

            let mut next_url: Option<String> = Some(initial_url);

            while let Some(url) = next_url.take() {
                debug!(url = %url, "Fetching quotes page");

                let parsed = self.fetch_quotes_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed quotes response"
                );

                if let Some(results) = parsed.results {
                    for quote in results {
                        yield quote.into_order_book_l1();
                    }
                }

                // Validate next_url origin before following
                if let Some(ref url) = parsed.next_url {
                    validate_next_url(url, &self.base_url)?;
                }
                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch a single page of quotes from the given URL.
    async fn fetch_quotes_page(&self, url: &str) -> Result<QuotesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_quotes_response(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = MassiveRestClient::new("test_api_key");
        assert!(client.is_ok());
    }

    #[test]
    fn test_from_env_missing() {
        temp_env::with_var_unset(ENV_API_KEY, || {
            let result = MassiveRestClient::from_env();
            assert!(matches!(result, Err(MassiveError::EnvVar { .. })));
        });
    }
}
