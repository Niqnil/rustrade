//! REST client for Massive historical and intraday data.
//!
//! Provides access to aggregates (OHLCV), trades, and quotes across all asset classes.

use super::error::MassiveError;
use super::pagination::PaginationGuard;
use super::transformer::{
    AggregateProvenance, AggregatesResponse, QuotesResponse, TradesResponse,
    parse_aggregates_response, parse_quotes_response, parse_trades_response, timespan_to_step,
};
use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped, truncate_str};
use crate::subscription::{
    book::OrderBookL1,
    candle::{Candle, open_time_from_close},
    trade::PublicTrade,
};
use async_stream::try_stream;
use futures::Stream;
use reqwest::{Client, StatusCode, header};
use std::env;
use std::time::Duration;
use tracing::debug;
use url::Url;

const BASE_URL: &str = "https://api.massive.com";
const ENV_API_KEY: &str = "MASSIVE_API_KEY";

/// Byte cap applied to a response body before it is stored in a [`MassiveError`] message.
const ERROR_MESSAGE_BODY_BYTES: usize = 512;

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
    // Attached per-request as an `Authorization: Bearer` header from the
    // origin-validated `fetch_page_body` chokepoint, never as a client-wide
    // default header — so the token can only ever ride a request whose
    // destination origin has already been validated. See #198.
    api_key: String,
    base_url: String,
    // Trusted origin (scheme + host + port) derived from `base_url`, parsed once at
    // construction so each `next_url` check compares against cached state and a
    // malformed base URL fails fast rather than on the first request. See #198.
    base_origin: url::Origin,
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
    ///
    /// # Redirects
    ///
    /// The client does **not** follow HTTP redirects. The API key is attached as
    /// an `Authorization: Bearer` header to each request, from a single
    /// origin-validated chokepoint, so auto-following a
    /// server-issued 3xx could carry it off the trusted origin. Massive uses
    /// explicit `next_url` cursor pagination and is not expected to redirect, so
    /// an unexpected 3xx surfaces as [`MassiveError::Api`] rather than being
    /// followed. A base URL set via [`Self::with_base_url`] must therefore serve
    /// responses directly, without redirect indirection.
    ///
    /// # Known limitation
    ///
    /// Redirect-following is disabled *wholesale* (`reqwest::redirect::Policy::none()`),
    /// so this applies to **same-origin** redirects too: even a same-origin 301/308
    /// — e.g. trailing-slash normalization by a load balancer or CDN — is surfaced
    /// as a terminal [`MassiveError::Api`] with the 3xx status rather than being
    /// transparently followed. This is a deliberate trade-off: a wholesale block
    /// keeps the "token never leaves the trusted origin" guarantee structural
    /// rather than dependent on reqwest's cross-origin header-stripping. It relies
    /// on Massive serving `next_url` pages directly; a deployment fronted by a
    /// redirecting proxy is not supported.
    pub fn new(api_key: impl Into<String>) -> Result<Self, MassiveError> {
        let api_key = api_key.into();
        // Validate the key forms a well-formed `Authorization` header value now, so a
        // malformed key fails fast at construction (as `Auth`) instead of as a confusing
        // per-request error. The token is deliberately NOT installed as a client-wide
        // default header — that would ride every request regardless of host. It is attached
        // per-request in `fetch_page_body`, only after the destination origin is validated,
        // so the credential is coupled to the origin check by construction. See #198.
        header::HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|e| {
            MassiveError::Auth {
                message: format!("Invalid API key format: {e}"),
            }
        })?;

        let base_origin = Self::parse_base_origin(BASE_URL)?;

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            // Transport-layer companion to the `validate_next_url` origin guard. The API key is
            // attached per-request (see `fetch_page_body`), so a server-issued 3xx must not be allowed
            // to bounce an origin-validated request to another host: `Policy::none()` stops reqwest
            // auto-following any redirect, so an unexpected 3xx is returned unfollowed and surfaces as
            // a `MassiveError::Api` instead of the client re-issuing the request against the redirect
            // target. This makes the "token never leaves the trusted origin" guarantee structural
            // rather than relying on reqwest's internal cross-origin header stripping. (`https_only` is
            // deliberately NOT set: it would reject the `http://` base URL that `with_base_url` accepts
            // for local testing, while adding nothing against the leak — the origin check already
            // rejects any `http://` `next_url` under an `https` base, since scheme is part of the
            // compared origin.)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            client,
            api_key,
            base_url: BASE_URL.to_string(),
            base_origin,
        })
    }

    /// Create a new client from `MASSIVE_API_KEY` environment variable.
    pub fn from_env() -> Result<Self, MassiveError> {
        let api_key =
            env::var(ENV_API_KEY).map_err(|_| MassiveError::EnvVar { var: ENV_API_KEY })?;
        Self::new(api_key)
    }

    /// Override the base URL (useful for testing or legacy polygon.io endpoint).
    ///
    /// The base URL defines the trusted origin: a paginated `next_url` is followed
    /// only when it shares this scheme, host, and port, and redirects are never
    /// followed (see the "Redirects" note on [`Self::new`]). The origin is parsed
    /// and cached here, so a `base_url` that is not a valid URL is rejected up
    /// front with [`MassiveError::InvalidInput`] rather than deferred to the first
    /// request.
    ///
    /// # Errors
    ///
    /// Returns [`MassiveError::InvalidInput`] if `base_url` does not parse as a
    /// URL, or if it parses but uses a scheme other than `http`/`https` — a
    /// non-special scheme (e.g. `file:`) yields an opaque origin that would
    /// reject every subsequent request, so it is refused up front.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Result<Self, MassiveError> {
        let base_url = base_url.into();
        self.base_origin = Self::parse_base_origin(&base_url)?;
        self.base_url = base_url;
        Ok(self)
    }

    /// Parse `base_url` into the trusted [origin](url::Origin) used to vet every
    /// `next_url`. A base URL that fails to parse — or that uses a non-`http(s)`
    /// scheme — is a client-side misconfiguration (surfaced as
    /// [`MassiveError::InvalidInput`]), not a security event.
    ///
    /// The scheme is restricted to `http`/`https` deliberately: a non-special
    /// scheme (e.g. `file:`) parses fine but its [origin](url::Url::origin) is a
    /// fresh [`url::Origin::Opaque`] that never compares equal to any other —
    /// even one parsed from the identical string — so every subsequent
    /// [`validate_next_url`](Self::validate_next_url) check (including the very
    /// first request, built from `base_url` itself) would fail with a misleading
    /// [`MassiveError::UntrustedNextUrl`]. Rejecting it here fails the build fast
    /// with a clear message instead of self-bricking every request.
    fn parse_base_origin(base_url: &str) -> Result<url::Origin, MassiveError> {
        let url = Url::parse(base_url).map_err(|e| MassiveError::InvalidInput {
            message: format!("base_url is not a valid URL ({e}): {base_url}"),
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(MassiveError::InvalidInput {
                message: format!(
                    "base_url must use the http or https scheme, got `{}`: {base_url}",
                    url.scheme()
                ),
            });
        }
        Ok(url.origin())
    }

    /// Get the base URL.
    pub(super) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Validate ticker doesn't contain URL-breaking characters.
    pub(super) fn validate_ticker(ticker: &str) -> Result<(), MassiveError> {
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

    /// Validate that `next_url` shares the client's trusted origin before a request
    /// is issued, so the API token is never sent to an untrusted host.
    ///
    /// The client attaches `Authorization: Bearer <key>` to each request from the
    /// [`fetch_page_body`](Self::fetch_page_body) chokepoint, *after* this check
    /// passes — so a URL naming a different origin is rejected before the token is
    /// ever attached. A `starts_with(base_url)` prefix check is insufficient — a
    /// look-alike host such as `https://api.massive.com.evil.example` (or the
    /// separator-less `https://api.massive.comevil.example`) shares the prefix yet
    /// is a different origin. `next_url` is parsed and its
    /// [origin](url::Url::origin) (scheme + host + port) compared against the
    /// client's cached `base_origin`; a `next_url` that fails to parse or whose
    /// origin differs is rejected (fail-closed) with
    /// [`MassiveError::UntrustedNextUrl`].
    ///
    /// The trusted `base_origin` is parsed once at construction (see
    /// [`Self::parse_base_origin`]), so a malformed base URL is rejected there —
    /// it can never reach this check.
    ///
    /// On success returns the parsed [`Url`] so the caller can issue the request
    /// without re-parsing the string.
    fn validate_next_url(next_url: &str, base_origin: &url::Origin) -> Result<Url, MassiveError> {
        let untrusted = || MassiveError::UntrustedNextUrl {
            next_url: next_url.to_owned(),
            expected_origin: base_origin.ascii_serialization(),
        };

        let parsed = Url::parse(next_url).map_err(|_| untrusted())?;
        if &parsed.origin() != base_origin {
            return Err(untrusted());
        }
        Ok(parsed)
    }

    /// Fetch a page body from the given URL with standard error handling.
    ///
    /// This is the single chokepoint that issues an authenticated request: every
    /// URL is [origin-validated](Self::validate_next_url) against the client's
    /// cached origin *before* the request is sent, and the `Authorization: Bearer`
    /// token is attached here per-request only after that check passes. Doing both
    /// in one place (not at each pagination call site) makes the token-leak guard
    /// structural — a new paginated fetch cannot leak the credential by forgetting
    /// to call the validator, because the token lives on this path alone. See #198.
    pub(super) async fn fetch_page_body(&self, url: &str) -> Result<String, MassiveError> {
        let validated_url = Self::validate_next_url(url, &self.base_origin)?;
        // Attach the bearer token only now, after the destination origin has been validated —
        // the token is not a client-wide default header, so it can never accompany an
        // unvalidated URL. `bearer_auth` also marks the header sensitive (redacted in reqwest
        // logs). See #198.
        let response = self
            .client
            .get(validated_url)
            .bearer_auth(&self.api_key)
            .send()
            .await?;
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

        // A non-success body is only a diagnostic that `truncate_str` caps for the error message, so
        // read it under a bound: a pathological proxy/CDN error page must not be buffered in full.
        // The success body is the real JSON page and is read whole below.
        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                return Err(MassiveError::Auth {
                    message: truncate_str(&body, ERROR_MESSAGE_BODY_BYTES),
                });
            }
            return Err(MassiveError::Api {
                status: status.as_u16(),
                message: truncate_str(&body, ERROR_MESSAGE_BODY_BYTES),
            });
        }

        let body = response.text().await?;
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
    /// Each [`Candle`]'s `close_time` is the exclusive end-of-period boundary
    /// `bar_open + interval` (see [`Candle::close_time`](crate::subscription::candle::Candle)).
    /// Fixed units (`second`…`week`) are exact in UTC; **calendar units
    /// (`month`/`quarter`/`year`) use leap-year-correct calendar arithmetic** —
    /// e.g. a January monthly bar closes at `Feb 1 00:00 UTC`, aligning with
    /// Binance `1M` / IBKR monthly boundaries (previously an approximate
    /// `+30/91/365 days`).
    ///
    /// # Range contract
    ///
    /// Yields exactly the candles whose `close_time ∈ [from, to]` (both inclusive),
    /// matched on `close_time` — the field consumers receive. Massive's endpoint
    /// natively filters by the bar's open-time, so this method widens the request
    /// by one interval and trims by `close_time`, consistent with the library's
    /// other historical fetches.
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
            Self::validate_ticker(ticker)?;

            // Map the interval to a step once; the boundary is still computed
            // per-bar (calendar months are variable-length, so a single Duration
            // for the whole stream would be wrong for month/quarter/year).
            let bar_step = timespan_to_step(multiplier, timespan);

            // What `v` means on this ticker's market. Forex bars are aggregated from quoted
            // bid/ask rather than a trade tape, so their `v` is quote activity, not volume.
            let bar_volume = AggregateProvenance::for_ticker(ticker);

            // Range contract: yield candles whose `close_time ∈ [from, to]`. The
            // Massive (Polygon) endpoint filters by the bar's open-time, so widen
            // the lower bound by one interval to capture the candle whose
            // `close_time == from` (open == from − interval), then trim by
            // `close_time` below — consistent with the library's other fetches.
            // `None` (underflow near DateTime::MIN_UTC) is not an error: the boundary
            // candle would have an unrepresentable open and so cannot exist, making
            // the un-widened bound already correct. See `open_time_from_close`.
            let request_from = open_time_from_close(from, bar_step).unwrap_or(from);
            let from_ms = request_from.timestamp_millis();
            let to_ms = to.timestamp_millis();

            let initial_url = format!(
                "{}/v2/aggs/ticker/{}/range/{}/{}/{}/{}?adjusted=true&sort=asc&limit=50000",
                self.base_url, ticker, multiplier, timespan, from_ms, to_ms
            );

            let mut next_url: Option<String> = Some(initial_url);
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
                debug!(url = %url, "Fetching aggregates page");

                let parsed = self.fetch_aggregates_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed aggregates response"
                );

                if let Some(results) = parsed.results {
                    for bar in results {
                        let candle = bar.into_candle_with_step(bar_step, bar_volume)?;
                        if candle.close_time >= from && candle.close_time <= to {
                            yield candle;
                        }
                    }
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
            Self::validate_ticker(ticker)?;

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
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
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
            Self::validate_ticker(ticker)?;

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
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
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
#[allow(clippy::expect_used)] // Test code: panics on unexpected values are acceptable
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

    // --- validate_next_url (#182: token-leak guard) ---------------------------

    const BASE: &str = "https://api.massive.com";

    fn base_origin() -> url::Origin {
        Url::parse(BASE).expect("BASE is a valid URL").origin()
    }

    fn assert_untrusted(next_url: &str) {
        match MassiveRestClient::validate_next_url(next_url, &base_origin()) {
            Err(MassiveError::UntrustedNextUrl {
                next_url: got,
                expected_origin,
            }) => {
                assert_eq!(got, next_url);
                assert_eq!(expected_origin, "https://api.massive.com");
            }
            other => panic!("expected UntrustedNextUrl for {next_url:?}, got {other:?}"),
        }
    }

    #[test]
    fn validate_next_url_accepts_same_origin_different_path() {
        // The documented shape: same origin, path + `cursor` query differ.
        assert!(
            MassiveRestClient::validate_next_url(
                "https://api.massive.com/v3/reference/tickers?cursor=YWN0aXZlPXRydWU",
                &base_origin(),
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_next_url_accepts_explicit_default_port() {
        // `:443` is the default for https, so origins are equal.
        assert!(
            MassiveRestClient::validate_next_url(
                "https://api.massive.com:443/v2/aggs",
                &base_origin()
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_next_url_rejects_lookalike_subdomain() {
        // The reported #182 vector: shares the base URL as a string prefix
        // (so `starts_with` passed) but is a different host.
        assert_untrusted("https://api.massive.com.attacker.example/v2/aggs?cursor=abc");
    }

    #[test]
    fn validate_next_url_rejects_no_dot_suffix_host() {
        // Proves the fix closes the separator-less bypass, not just subdomains:
        // `api.massive.comevil.example` also `starts_with("https://api.massive.com")`.
        assert_untrusted("https://api.massive.comevil.example/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_scheme_downgrade() {
        // https -> http would send the bearer token in cleartext.
        assert_untrusted("http://api.massive.com/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_port_mismatch() {
        assert_untrusted("https://api.massive.com:8443/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_userinfo_confusion() {
        // Host is `attacker.example`; `api.massive.com` is only userinfo.
        assert_untrusted("https://api.massive.com@attacker.example/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_unparseable_next_url() {
        // Fail-closed: an unverifiable URL must never receive the token.
        assert_untrusted("not a url");
    }

    #[test]
    fn validate_next_url_rejects_non_http_scheme() {
        // Non-http(s) schemes yield opaque origins that never compare equal.
        assert_untrusted("file:///etc/passwd");
    }

    #[test]
    fn with_base_url_rejects_unparseable_base_as_invalid_input() {
        // A broken client-configured base URL is a misconfiguration, not a
        // token-leak attempt — and it now fails fast at construction (#198),
        // surfacing as InvalidInput rather than deferring to the first request.
        let result = MassiveRestClient::new("test_api_key")
            .expect("client builds")
            .with_base_url("not a url");
        assert!(matches!(result, Err(MassiveError::InvalidInput { .. })));
    }

    #[test]
    fn with_base_url_rejects_non_http_scheme() {
        // A base URL that *parses* but uses a non-http(s) scheme (e.g. `file:`)
        // yields an opaque origin that never compares equal — which would brick
        // every subsequent request with a misleading UntrustedNextUrl. It must
        // instead fail fast at construction as InvalidInput (#198). Distinct from
        // the parse-failure path above: `file:///data` parses successfully, so
        // this exercises the scheme check specifically, not `Url::parse` erroring.
        let result = MassiveRestClient::new("test_api_key")
            .expect("client builds")
            .with_base_url("file:///data");
        assert!(matches!(result, Err(MassiveError::InvalidInput { .. })));
    }
}
