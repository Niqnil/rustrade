// Alpaca ExecutionClient implementation
//
// Uses raw reqwest for REST and barter-integration tungstenite for WebSocket.
// No official Alpaca Rust SDK — built directly on reqwest + barter-integration
// to avoid supply chain risk in a trading system that handles real money.
//
// Architecture:
// - REST (reqwest): account_snapshot, fetch_balances, fetch_open_orders,
//   fetch_trades, open_order, cancel_order
// - WebSocket (tungstenite): account_stream via Alpaca's trade_updates stream
//   at wss://[paper-]api.alpaca.markets/stream
//
// Auth: header-based (APCA-API-KEY-ID + APCA-API-SECRET-KEY), no HMAC signing.
// A reqwest::Client is built with these as default headers so every request
// carries them automatically.
//
// Resilience features:
// - Rate limit handling: reads X-Ratelimit-Remaining / X-Ratelimit-Reset headers;
//   backs off on 429 with up to MAX_RATE_LIMIT_ATTEMPTS total attempts
// - Reconnection: account_stream reconnects on WS close/error with exponential
//   backoff (1 s → 30 s, max 10 attempts)
// - Heartbeat monitoring: reconnects if no WS message for HEARTBEAT_TIMEOUT_SECS
// - Fill recovery: after reconnect, fetches missed fills via GET /v2/account/activities
//   since disconnect_time; sent through the dedup cache to filter duplicates
// - Dedup cache: LRU keyed on "{order_id}:{cumulative_filled_qty}" prevents
//   duplicate fills arising from the overlap between WS events before disconnect
//   and the fill-recovery REST window
//
// Known limitations:
// - Only FILL activities are recovered after reconnect; order lifecycle events
//   (new, cancelled, expired) are not — callers must call fetch_open_orders after
//   each reconnect to reconcile open-order state.

use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::ExecutionClient,
    error::{ApiError, ConnectivityError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::protocol::websocket::{WebSocket, WsMessage};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use futures::{SinkExt as _, StreamExt as _, stream::BoxStream};
use indexmap::IndexMap;
use itertools::Itertools as _;
use lru::LruCache;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use std::{num::NonZeroUsize, pin::Pin, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INITIAL_BACKOFF_MS: u64 = 1_000;
const MAX_BACKOFF_MS: u64 = 30_000;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
/// If no WS activity for this long, force reconnect.
const HEARTBEAT_TIMEOUT_SECS: u64 = 35;
/// Timeout for fill recovery REST queries after reconnect.
const FILL_RECOVERY_TIMEOUT_SECS: u64 = 30;
/// Extra lookback from disconnect timestamp to cover Tokio scheduling jitter and
/// client/server clock drift on cloud VMs. The dedup cache absorbs resulting duplicates.
const SIGNAL_RECOVERY_LOOKBACK_MS: i64 = 1_500;
/// Alpaca's activity page size limit.
const ALPACA_MAX_ACTIVITIES: usize = 100;
/// Default cooldown when rate-limited (if X-Ratelimit-Reset header is absent).
const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 60;
/// Total REST attempts (1 initial + retries) before giving up on rate-limit errors.
/// The loop runs `0..MAX_RATE_LIMIT_ATTEMPTS`, retrying while `attempt + 1 < MAX`.
const MAX_RATE_LIMIT_ATTEMPTS: u32 = 4;
/// Dedup LRU cache size. Each entry is a ~50–70 byte String (UUID + decimal).
/// 2_000 entries ≈ 120–140 KB — ample for options trading fill rates.
const DEDUP_CACHE_SIZE: usize = 2_000;
/// Timeout for the initial WS auth+subscribe handshake.
const WS_HANDSHAKE_TIMEOUT_SECS: u64 = 15;
/// Timeout for a graceful WS close. Prevents indefinite blocking when the
/// server does not respond to the close frame before reconnect/shutdown.
const WS_CLOSE_TIMEOUT_SECS: u64 = 5;

// ---------------------------------------------------------------------------
// GracefulShutdownStream
// ---------------------------------------------------------------------------

/// Wrapper stream that signals the `connection_manager` task to shut down gracefully
/// when dropped, allowing it to send an orderly WebSocket close frame.
///
/// # Shutdown sequence
/// Dropping this stream drops `inner` (the channel receiver), which makes `tx.closed()`
/// resolve on the `connection_manager`'s next `select!` poll. The `tx.closed()` arm
/// sends a WebSocket close frame (with `WS_CLOSE_TIMEOUT_SECS` timeout) and then
/// returns, dropping the task cleanly.
///
/// `JoinHandle::drop` detaches the task — it is NOT aborted. The task exits within
/// the current `select!` iteration (if the receiver is already dropped when polled)
/// or after the current heartbeat window / backoff sleep at most.
struct GracefulShutdownStream<S> {
    inner: S,
    /// Keeps the `JoinHandle` alive until this stream is dropped. Dropping
    /// the `JoinHandle` detaches (not cancels) the task, allowing it to keep
    /// running until `tx.closed()` resolves. Without this field the handle
    /// would be detached immediately at `connection_manager` spawn time,
    /// preventing any future `.await` or abort if the design changes.
    _handle: tokio::task::JoinHandle<()>,
}

impl<S> GracefulShutdownStream<S> {
    fn new(inner: S, handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            inner,
            _handle: handle,
        }
    }
}

impl<S: futures::Stream + Unpin> futures::Stream for GracefulShutdownStream<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S> Drop for GracefulShutdownStream<S> {
    fn drop(&mut self) {
        // Do not abort. Dropping `self.inner` (the channel receiver) makes `tx.closed()`
        // resolve, which causes `connection_manager` to send a graceful WS close frame
        // and return. Dropping the JoinHandle here detaches (not cancels) the task.
    }
}

// ---------------------------------------------------------------------------
// Rate limit tracker
// ---------------------------------------------------------------------------

/// Thread-safe rate-limit state shared across all clones of AlpacaClient.
struct RateLimitTracker {
    blocked_until: parking_lot::Mutex<Option<tokio::time::Instant>>,
}

impl RateLimitTracker {
    fn new() -> Self {
        Self {
            blocked_until: parking_lot::Mutex::new(None),
        }
    }

    /// Sleep until the current cooldown expires. Returns immediately if not blocked.
    async fn wait_if_blocked(&self) {
        loop {
            // Capture the current time once per iteration — reused for both the
            // expired-deadline check inside the lock and the sleep calculation below.
            // Eliminates one vDSO call per REST request on the common non-blocked path.
            let now = tokio::time::Instant::now();
            // Read and conditionally clear the deadline in a single lock acquisition.
            // The guard is dropped before the `.await` below — holding a sync Mutex
            // across an await would deadlock. A TOCTOU window still exists between the
            // guard drop and `sleep_until`: a concurrent `on_rate_limited` call could
            // extend the deadline after we read it. The loop re-reads on wake and
            // corrects any extended deadline, so the race is recovered from on the next
            // iteration rather than being fully prevented.
            let deadline = {
                let mut guard = self.blocked_until.lock();
                let d = *guard;
                if matches!(d, Some(t) if t <= now) {
                    // Clear expired deadline so on_rate_limited correctly
                    // distinguishes "new rate-limit event" from "extended cooldown".
                    *guard = None;
                }
                d
            };
            match deadline {
                None => return,
                Some(until) => {
                    if until <= now {
                        // Deadline was expired and cleared above; no sleep needed.
                        return;
                    }
                    debug!(
                        delay_ms = (until - now).as_millis() as u64,
                        "Alpaca REST rate-limited, waiting before request"
                    );
                    tokio::time::sleep_until(until).await;
                }
            }
        }
    }

    /// Record a rate-limit event, extending any existing cooldown if longer.
    fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let delay = retry_after.unwrap_or(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));
        let new_deadline = tokio::time::Instant::now() + delay;
        let mut guard = self.blocked_until.lock();
        let was_blocked = guard.is_some();
        *guard = Some(guard.map_or(new_deadline, |existing| existing.max(new_deadline)));
        if was_blocked {
            debug!(
                delay_secs = delay.as_secs(),
                "Alpaca rate-limit cooldown extended"
            );
        } else {
            warn!(
                delay_secs = delay.as_secs(),
                "Alpaca entering rate-limit degradation mode"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Exponential backoff
// ---------------------------------------------------------------------------

struct ExponentialBackoff {
    attempt: u32,
    max_attempts: u32,
    initial_ms: u64,
    max_ms: u64,
}

impl ExponentialBackoff {
    fn new() -> Self {
        Self {
            attempt: 0,
            max_attempts: MAX_RECONNECT_ATTEMPTS,
            initial_ms: INITIAL_BACKOFF_MS,
            max_ms: MAX_BACKOFF_MS,
        }
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Waits for the current backoff duration. Returns `false` if max attempts exhausted.
    async fn wait(&mut self) -> bool {
        if self.attempt >= self.max_attempts {
            return false;
        }
        let delay_ms = self
            .initial_ms
            .saturating_mul(2u64.saturating_pow(self.attempt))
            .min(self.max_ms);
        self.attempt += 1;
        debug!(
            attempt = self.attempt,
            max = self.max_attempts,
            delay_ms,
            "Alpaca reconnect backoff"
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        true
    }
}

// ---------------------------------------------------------------------------
// Dedup cache
// ---------------------------------------------------------------------------

/// LRU cache keyed on `trade.id`, which both paths synthesise as
/// `"{order_id}:{cumulative_filled_qty}"`.
///
/// WS fills: `convert_trade_update` sets `trade_id = order.id + ":" + order.filled_qty`
/// (cumulative from the order update payload).
///
/// REST fills: `recover_fills` accumulates per-execution qty per order and overrides
/// `trade.id` to the same format before inserting into the cache.
///
/// Using cumulative qty (not per-execution qty) means two equal-size partial fills
/// on the same order produce distinct keys (`order:1` and `order:2`), preventing
/// silent fill drops.
///
/// [`SmolStr`] keys avoid heap allocation for IDs ≤22 bytes. UUID-length keys
/// (36 chars) always heap-allocate in `SmolStr`; `format_smolstr!` uses an
/// internal `String` buffer for long keys, identical in allocation cost to
/// `format!(…).into::<SmolStr>()`. The type is kept for API consistency with
/// other key types in this codebase.
type SharedDedupCache = Arc<parking_lot::Mutex<LruCache<SmolStr, ()>>>;

fn new_dedup_cache() -> SharedDedupCache {
    // allow(clippy::unwrap_used) — NonZeroUsize::new on a non-zero constant
    // cannot fail at runtime.
    #[allow(clippy::unwrap_used)]
    Arc::new(parking_lot::Mutex::new(LruCache::new(
        NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
    )))
}

/// Returns `true` if this key was already seen (duplicate). Inserts if new.
fn is_duplicate(cache: &SharedDedupCache, key: &SmolStr) -> bool {
    let mut guard = cache.lock();
    // peek avoids promoting to MRU on the duplicate (discard) path
    if guard.peek(key).is_some() {
        return true;
    }
    // Clone on the insert (non-duplicate) path only. UUID-length SmolStr keys
    // heap-allocate, but the WS path is single-threaded — there is no mutex
    // contention to justify cloning before the lock on the duplicate fast-path.
    guard.put(key.clone(), ());
    false
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Alpaca execution client.
// Serialize intentionally omitted — would expose secret_key in plaintext.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaConfig {
    // Private fields prevent accidental credential exposure via struct access.
    api_key: String,
    secret_key: String,
    /// Use paper trading endpoints instead of production.
    pub paper: bool,
    /// Test-only: override the REST base URL (e.g., to point at a wiremock server).
    #[cfg(test)]
    pub base_url_override: Option<String>,
}

// Custom Debug to avoid leaking credentials in logs.
impl std::fmt::Debug for AlpacaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaConfig")
            .field("api_key", &"***")
            .field("secret_key", &"***")
            .field("paper", &self.paper)
            .finish()
    }
}

impl AlpacaConfig {
    pub fn new(api_key: String, secret_key: String, paper: bool) -> Self {
        Self {
            api_key,
            secret_key,
            paper,
            #[cfg(test)]
            base_url_override: None,
        }
    }

    /// Test-only: create config with a custom base URL for wiremock testing.
    #[cfg(test)]
    pub fn with_base_url(api_key: String, secret_key: String, base_url: String) -> Self {
        Self {
            api_key,
            secret_key,
            paper: true,
            base_url_override: Some(base_url),
        }
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Base URL for REST API calls.
    ///
    /// In test builds, checks `base_url_override` first to allow wiremock testing.
    pub fn rest_base_url(&self) -> &str {
        #[cfg(test)]
        if let Some(ref url) = self.base_url_override {
            return url.as_str();
        }
        if self.paper {
            "https://paper-api.alpaca.markets"
        } else {
            "https://api.alpaca.markets"
        }
    }

    /// WebSocket URL for trade_updates stream.
    pub fn ws_url(&self) -> &'static str {
        if self.paper {
            "wss://paper-api.alpaca.markets/stream"
        } else {
            "wss://api.alpaca.markets/stream"
        }
    }
}

// ---------------------------------------------------------------------------
// REST response serde types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AlpacaAccount {
    equity: String,
    buying_power: String,
    options_buying_power: Option<String>,
    // crypto_buying_power is present in Alpaca's account response and represents
    // available buying power specifically for crypto orders. Not currently used in
    // balance logic (buying_power serves as the general free USD balance), but
    // retained so serde doesn't error on accounts where the field is present.
    #[allow(dead_code)]
    // retained for serde completeness; may be used for per-asset-class reporting
    crypto_buying_power: Option<String>,
}

/// A single position returned by GET /v2/positions.
#[derive(Debug, Deserialize)]
struct AlpacaPosition {
    /// Exchange symbol (e.g., "BTC/USD" for crypto, "AAPL" for equity).
    symbol: String,
    /// Asset class: "us_equity", "crypto", "us_option".
    asset_class: String,
    /// Total quantity held (base currency for crypto).
    qty: String,
    /// Quantity available to trade (not locked in open orders).
    qty_available: String,
}

#[derive(Debug, Deserialize)]
struct AlpacaOrderResponse {
    id: String,
    client_order_id: Option<String>,
    symbol: String,
    qty: Option<String>,
    filled_qty: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    time_in_force: String,
    limit_price: Option<String>,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct AlpacaActivity {
    id: String,
    order_id: String,
    symbol: String,
    side: String,
    price: String,
    qty: String,
    transaction_time: String,
}

#[derive(Debug, Deserialize)]
struct AlpacaApiError {
    message: String,
}

// ---------------------------------------------------------------------------
// AlpacaPositionIntent
// ---------------------------------------------------------------------------

/// Explicit position intent for Alpaca order placement.
///
/// Required for options orders; valid (but optional) for equities.
/// Omit entirely for crypto orders (causes 422 Unprocessable Entity).
///
/// Use `AlpacaClient::open_order_with_intent` to supply a specific intent
/// instead of the heuristic mapping used by the `ExecutionClient` trait impl.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaPositionIntent {
    BuyToOpen,
    BuyToClose,
    SellToOpen,
    SellToClose,
}

// ---------------------------------------------------------------------------
// REST order request body
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct AlpacaOrderRequest<'a> {
    symbol: &'a str,
    qty: String,
    side: &'static str,
    #[serde(rename = "type")]
    order_type: &'static str,
    time_in_force: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_order_id: Option<&'a str>,
    // position_intent: heuristic mapping (buy→buy_to_open, sell→sell_to_close).
    // Correct for directional long-only strategies. Omit for exchanges/asset
    // classes that don't require it (stocks/crypto ignore this field).
    #[serde(skip_serializing_if = "Option::is_none")]
    position_intent: Option<AlpacaPositionIntent>,
}

// ---------------------------------------------------------------------------
// WebSocket message types
// ---------------------------------------------------------------------------

/// Outer container for all Alpaca stream messages.
///
/// `data` is kept as a [`serde_json::value::RawValue`] to avoid allocating a full DOM tree
/// for heartbeats and auth/listening acks that never reach `AlpacaTradeUpdate` parsing.
#[derive(Debug, Deserialize)]
struct AlpacaStreamMessage<'a> {
    // Short well-known values ("trade_updates", "listening", "authorization")
    // all fit inline in SmolStr — avoids one heap alloc per WS message.
    stream: SmolStr,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

/// Parsed payload of a `trade_updates` event.
///
/// Numeric and timestamp fields borrow directly from the `RawValue` input buffer
/// (`#[serde(borrow)]`), propagating the zero-copy design of `AlpacaStreamMessage`.
/// This eliminates 4–6 heap allocations per fill event. The borrow is valid because
/// Alpaca's numeric and timestamp strings contain no JSON escape sequences.
#[derive(Debug, Deserialize)]
struct AlpacaTradeUpdate<'a> {
    // Short event tag ("fill", "partial_fill", "new", ...) — fits inline.
    event: SmolStr,
    #[serde(borrow)]
    order: AlpacaOrderWs<'a>,
    /// Fill price for this specific execution (None for non-fill events).
    #[serde(borrow)]
    price: Option<&'a str>,
    /// Quantity for this specific execution (None for non-fill events).
    #[serde(borrow)]
    qty: Option<&'a str>,
    #[serde(borrow)]
    timestamp: Option<&'a str>,
}

/// Order state embedded in a `trade_updates` WebSocket event.
#[derive(Debug, Deserialize)]
struct AlpacaOrderWs<'a> {
    // UUIDs (36 chars) heap-allocate in SmolStr, but using SmolStr directly avoids
    // an intermediate String allocation when serde deserialises the field.
    id: SmolStr,
    client_order_id: Option<SmolStr>,
    // Ticker symbols ("AAPL", "BTC/USD") fit inline in SmolStr (≤23 bytes),
    // eliminating the heap allocation entirely for most symbols.
    symbol: SmolStr,
    #[serde(borrow)]
    qty: Option<&'a str>,
    // Alpaca guarantees `filled_qty` for fill/partial_fill and most lifecycle
    // events, but some event types (e.g. `rejected`) may omit the field.
    // Using Option avoids a deserialization failure that would silently drop
    // the event. Call sites use `.unwrap_or("0")`.
    #[serde(borrow)]
    filled_qty: Option<&'a str>,
    // Short enums ("buy"/"sell", "market"/"limit"/..., "day"/"gtc"/..., status)
    // — all fit inline in SmolStr.
    side: SmolStr,
    #[serde(rename = "type")]
    order_type: SmolStr,
    time_in_force: SmolStr,
    #[serde(borrow)]
    limit_price: Option<&'a str>,
    status: SmolStr,
}

// ---------------------------------------------------------------------------
// AlpacaClient
// ---------------------------------------------------------------------------

/// Alpaca execution client supporting options, equities, and crypto via the
/// single unified Alpaca trading API.
///
/// All three asset classes share the same REST and WebSocket endpoints. The
/// key behavioral differences handled transparently:
/// - Options: `position_intent` field is required (detected by OCC symbol format)
/// - Crypto: `position_intent` is omitted (not a valid field for crypto orders);
///   fractional quantities are supported natively via `Decimal::to_string()`
/// - Equities: `position_intent` is valid but optional for long-only strategies
///
/// Cloning is cheap: all inner state is behind `Arc`.
#[derive(Clone)]
pub struct AlpacaClient {
    config: Arc<AlpacaConfig>,
    /// reqwest client with APCA-API-KEY-ID and APCA-API-SECRET-KEY pre-set as
    /// default headers — every request carries auth automatically.
    http: reqwest::Client,
    rate_limiter: Arc<RateLimitTracker>,
}

impl std::fmt::Debug for AlpacaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaClient")
            .field("paper", &self.config.paper)
            .finish_non_exhaustive()
    }
}

impl AlpacaClient {
    /// Build a `reqwest::Client` with Alpaca auth headers pre-set.
    ///
    /// # Panics
    ///
    /// Panics if the API key or secret contains characters that are invalid
    /// in an HTTP header value (non-ASCII or control characters).
    fn build_http(config: &AlpacaConfig) -> reqwest::Client {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        // HTTP header names are case-insensitive; use lowercase per HTTP/2 convention.
        headers.insert(
            HeaderName::from_static("apca-api-key-id"),
            HeaderValue::from_str(&config.api_key)
                .expect("Alpaca API key contains invalid header characters"),
        );
        headers.insert(
            HeaderName::from_static("apca-api-secret-key"),
            HeaderValue::from_str(&config.secret_key)
                .expect("Alpaca secret key contains invalid header characters"),
        );
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build reqwest client for Alpaca")
    }

    fn base_url(&self) -> &str {
        self.config.rest_base_url()
    }
}

// ---------------------------------------------------------------------------
// REST helper: rate-limited request with retry
// ---------------------------------------------------------------------------

/// Parse the `X-Ratelimit-Reset` response header into a cooldown [`Duration`].
///
/// The header value is a Unix epoch timestamp (seconds). Returns the duration
/// from now until that timestamp, clamped to a minimum of 1 second to avoid
/// a zero-delay busy loop. Returns `None` if the header is absent or malformed;
/// callers should fall back to [`DEFAULT_RATE_LIMIT_DELAY_SECS`].
fn parse_rate_limit_delay(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|reset_ts| {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Duration::from_secs(reset_ts.saturating_sub(now_secs).max(1))
        })
}

/// Execute a REST request with rate-limit awareness and retry.
///
/// The `build_request` closure is called on every attempt so the caller doesn't
/// need a clone of `RequestBuilder` (which may not be cloneable with streaming bodies).
/// For GET/POST/DELETE with fixed bodies, the closure is a cheap re-construction.
///
/// On HTTP 429, reads `X-Ratelimit-Reset` (Unix epoch) to determine the cooldown
/// duration and retries up to `MAX_RATE_LIMIT_ATTEMPTS - 1` times.
async fn rest_with_retry<T>(
    rate_limiter: &RateLimitTracker,
    mut build_request: impl FnMut() -> reqwest::RequestBuilder,
) -> Result<T, UnindexedClientError>
where
    T: for<'de> Deserialize<'de>,
{
    for attempt in 0..MAX_RATE_LIMIT_ATTEMPTS {
        rate_limiter.wait_if_blocked().await;
        let response = build_request()
            .send()
            .await
            .map_err(|e| connectivity_err(format!("Alpaca REST request failed: {e}")))?;

        // Check X-Ratelimit-Remaining as an early warning; if exactly 0, the next
        // request will 429. We don't proactively pause here — let the 429 handle it —
        // but log at debug level so it's visible in traces.
        if response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            == Some(0)
        {
            debug!("Alpaca REST rate-limit bucket exhausted (X-Ratelimit-Remaining: 0)");
        }

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let reset_delay = parse_rate_limit_delay(response.headers());

            if attempt + 1 < MAX_RATE_LIMIT_ATTEMPTS {
                warn!(
                    attempt = attempt + 1,
                    max_attempts = MAX_RATE_LIMIT_ATTEMPTS,
                    "Alpaca REST rate-limited (429), retrying"
                );
                rate_limiter.on_rate_limited(reset_delay);
                continue;
            }

            // Final attempt still rate-limited — return typed error.
            warn!(
                max_attempts = MAX_RATE_LIMIT_ATTEMPTS,
                "Alpaca REST rate-limit retries exhausted"
            );
            return Err(UnindexedClientError::Api(ApiError::RateLimit));
        }

        // 204 No Content is only valid for DELETE endpoints; use rest_delete_with_retry
        // for those. Reaching here for a 204 indicates API misuse — return a clear error
        // rather than a misleading "EOF while parsing" JSON failure.
        if status == reqwest::StatusCode::NO_CONTENT {
            return Err(connectivity_err(
                "Alpaca REST returned 204 No Content — use rest_delete_with_retry for DELETE endpoints"
                    .to_string(),
            ));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| connectivity_err(format!("Alpaca REST read body failed: {e}")))?;

        if status.is_success() {
            return serde_json::from_slice::<T>(&bytes).map_err(|e| {
                connectivity_err(format!(
                    "Alpaca REST JSON parse error ({status}): {e} | body: {}",
                    String::from_utf8_lossy(&bytes)
                        .chars()
                        .take(200)
                        .collect::<String>()
                ))
            });
        }

        // Parse API error body for a better error message.
        let api_err = serde_json::from_slice::<AlpacaApiError>(&bytes)
            .map(|e| e.message)
            .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());

        // 4xx = API-level rejection (wrong parameters, auth failure, insufficient funds).
        // 5xx / other = server-side failure treated as connectivity error.
        // Callers that pattern-match on UnindexedClientError (e.g. open_order_inner) rely
        // on Api(ApiError) to classify business rejections vs connectivity failures.
        //
        // Uses parse_api_error for consistent classification: 422 "insufficient funds"
        // maps to BalanceInsufficient (not generic OrderRejected), enabling callers to
        // trigger balance refresh on insufficient-funds rejections.
        if status.is_client_error() {
            return Err(UnindexedClientError::Api(parse_api_error(status, &api_err)));
        }
        return Err(connectivity_err(format!(
            "Alpaca REST error {status}: {api_err}"
        )));
    }
    unreachable!("Alpaca REST retry loop exited without returning")
}

/// Execute a DELETE request, returning an order error on rejection.
///
/// Handles 204 No Content (success), 422 / 403 (API rejection), and 429 (rate limit).
async fn rest_delete_with_retry(
    rate_limiter: &RateLimitTracker,
    mut build_request: impl FnMut() -> reqwest::RequestBuilder,
) -> Result<(), UnindexedOrderError> {
    for attempt in 0..MAX_RATE_LIMIT_ATTEMPTS {
        rate_limiter.wait_if_blocked().await;
        let response = build_request().send().await.map_err(|e| {
            UnindexedOrderError::Connectivity(ConnectivityError::Socket(format!(
                "Alpaca cancel request failed: {e}"
            )))
        })?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let reset_delay = parse_rate_limit_delay(response.headers());

            if attempt + 1 < MAX_RATE_LIMIT_ATTEMPTS {
                warn!(
                    attempt = attempt + 1,
                    max_attempts = MAX_RATE_LIMIT_ATTEMPTS,
                    "Alpaca cancel rate-limited (429), retrying"
                );
                rate_limiter.on_rate_limited(reset_delay);
                continue;
            }

            // Final attempt still rate-limited — return typed error.
            warn!(
                max_attempts = MAX_RATE_LIMIT_ATTEMPTS,
                "Alpaca cancel rate-limit retries exhausted"
            );
            return Err(UnindexedOrderError::Rejected(ApiError::RateLimit));
        }

        // 204 No Content: cancel succeeded.
        if status == reqwest::StatusCode::NO_CONTENT || status.is_success() {
            return Ok(());
        }

        let bytes = response
            .bytes()
            .await
            .inspect_err(
                |e| warn!(%e, %status, "Alpaca cancel_order: failed to read error response body"),
            )
            .unwrap_or_default();
        let msg = serde_json::from_slice::<AlpacaApiError>(&bytes)
            .map(|e| e.message)
            .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());

        return Err(parse_order_error(status, &msg));
    }
    unreachable!("Alpaca cancel retry loop exited without returning")
}

// ---------------------------------------------------------------------------
// ExecutionClient implementation
// ---------------------------------------------------------------------------

impl ExecutionClient for AlpacaClient {
    const EXCHANGE: ExchangeId = ExchangeId::Alpaca;
    type Config = AlpacaConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// # Panics
    ///
    /// Panics if the API key or secret key contains characters invalid in an
    /// HTTP header value. See [`AlpacaClient::build_http`] for details.
    fn new(config: Self::Config) -> Self {
        let http = Self::build_http(&config);
        Self {
            config: Arc::new(config),
            http,
            rate_limiter: Arc::new(RateLimitTracker::new()),
        }
    }

    /// # Rate limit note
    ///
    /// When both USD and non-USD assets are requested (the common startup case), this
    /// method fetches `/v2/account` and `/v2/positions` in parallel for ~100-300ms
    /// latency savings. Under rate pressure, both requests may hit 429 simultaneously
    /// and retry independently. Operators approaching Alpaca rate limits should call
    /// with explicit asset filters to serialize requests if needed.
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let base = self.base_url();
        let http = self.http.clone();
        let rl = &self.rate_limiter;

        let wants_usd = assets.is_empty()
            || assets
                .iter()
                .any(|a| a.name().as_str().eq_ignore_ascii_case("usd"));
        let wants_non_usd = assets.is_empty()
            || assets
                .iter()
                .any(|a| !a.name().as_str().eq_ignore_ascii_case("usd"));

        // Fetch account + positions in parallel when both are needed (common startup case).
        // URLs are extracted before the closures to avoid re-allocating on each retry attempt.
        let account_url = format!("{base}/v2/account");
        let positions_url = format!("{base}/v2/positions");
        let balances = match (wants_usd, wants_non_usd) {
            (true, true) => {
                let (account, positions): (AlpacaAccount, Vec<AlpacaPosition>) = tokio::try_join!(
                    rest_with_retry(rl, || http.get(&account_url)),
                    rest_with_retry(rl, || http.get(&positions_url)),
                )?;
                let mut balances = convert_account_to_balances(&account, assets);
                balances.extend(convert_positions_to_balances(&positions, assets));
                balances
            }
            (true, false) => {
                let account: AlpacaAccount = rest_with_retry(rl, || http.get(&account_url)).await?;
                convert_account_to_balances(&account, assets)
            }
            (false, true) => {
                let positions: Vec<AlpacaPosition> =
                    rest_with_retry(rl, || http.get(&positions_url)).await?;
                convert_positions_to_balances(&positions, assets)
            }
            (false, false) => Vec::new(),
        };

        let open_orders = fetch_raw_open_orders(&http, rl, base, instruments).await?;

        // Group open orders by instrument symbol, building InstrumentAccountSnapshots.
        let instrument_snapshots = build_instrument_snapshots(open_orders, instruments);

        Ok(AccountSnapshot::new(
            ExchangeId::Alpaca,
            balances,
            instrument_snapshots,
        ))
    }

    /// Returns a live stream of account events (fills, order updates).
    ///
    /// # Startup race window
    ///
    /// Fills arriving between `account_snapshot` and this method being called by the
    /// caller are not recovered automatically — the WebSocket connection does not exist
    /// yet during that window. Fills that arrive after the connection is established but
    /// before the first poll are buffered in the tungstenite internal buffer and delivered
    /// normally. Callers requiring fill completeness at startup **must** call
    /// [`ExecutionClient::fetch_trades`] with a ~1 s lookback after calling this method.
    ///
    /// This gap also applies on every **reconnect**: during the auth+subscribe handshake
    /// (`connect_and_subscribe`), any `trade_updates` messages that arrive are consumed
    /// by the handshake loop and not forwarded. Fill events in this window are recovered
    /// via the REST activities endpoint (anchored to `disconnect_time`). Lifecycle events
    /// (`new`, `canceled`, `rejected`) consumed during the handshake are **not** recovered
    /// — callers must call `fetch_open_orders` after each reconnect to reconcile order state.
    ///
    /// # Fill recovery ordering
    ///
    /// After a reconnect, missed fills are recovered from the REST activities endpoint
    /// using `direction=asc` to match the chronological order in which the WS stream
    /// advanced `filled_qty`. The dedup key `"{order_id}:{cum_qty}"` is synthesised
    /// by accumulating per-execution qty in that order. If Alpaca returns activities
    /// out of chronological order (e.g. at pagination boundaries), dedup keys will
    /// diverge and fills may be dropped or duplicated for that order.
    ///
    /// # Lifecycle event deduplication
    ///
    /// Order lifecycle events (`new`, `canceled`, `expired`) are **not** deduplicated
    /// across reconnects — only fill events carry a dedup key. After a reconnect, Alpaca
    /// re-delivers lifecycle events for orders that were active at disconnect time.
    /// Specifically, Alpaca re-delivers a `new` event for **every order open at disconnect
    /// time**, not only orders that changed during the gap.
    /// Callers must make [`AccountEventKind::OrderSnapshot`] and
    /// [`AccountEventKind::OrderCancelled`] processing idempotent, or call
    /// [`ExecutionClient::fetch_open_orders`] after each reconnect to reconcile state.
    ///
    /// # Rejected orders
    ///
    /// Alpaca `rejected` events are delivered as `AccountEventKind::OrderCancelled` with
    /// `state: Err(OrderRejected(...))`. Match on `response.state.is_err()` to distinguish
    /// rejections from true cancels — do not call `.unwrap()` on `OrderCancelled.state`.
    ///
    /// # Stream drop behaviour
    ///
    /// Dropping the returned `BoxStream` initiates a graceful shutdown of the
    /// background `connection_manager` task: the channel close causes the task
    /// to send a WebSocket close frame and exit within the current heartbeat
    /// window (≤60 s). Any [`AccountEvent`] items already queued but not yet
    /// polled are discarded. Callers who drop and re-subscribe must call
    /// [`ExecutionClient::fetch_trades`] with a short lookback to recover the gap.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange], // ignored — Alpaca's trade_updates stream delivers all asset classes on one channel
        // instruments is used to filter fill recovery (REST) after a reconnect only.
        // Live WS events are NOT filtered by instrument: Alpaca's trade_updates stream
        // delivers all account events on one channel with no per-symbol subscription.
        instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // Verify the initial connection before returning the stream; distinguishes
        // "can't connect at all" from "connected but later disconnected".
        let initial_ws = connect_and_subscribe(&self.config).await?;

        // Unbounded channel — memory grows if the consumer is slow, but fills
        // are never silently dropped. Silent fill loss corrupts position state;
        // OOM is loudly observable. The WS delivery path uses non-blocking send()
        // which only fails if the receiver is dropped.
        let (tx, rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();
        let dedup = new_dedup_cache();
        let config = self.config.clone();
        let http = self.http.clone();
        let rate_limiter = self.rate_limiter.clone();
        let instruments = instruments.to_vec();

        let cm_handle = tokio::spawn(connection_manager(
            tx,
            dedup,
            config,
            http,
            rate_limiter,
            instruments,
            Some(initial_ws),
        ));

        let rx_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let guarded = GracefulShutdownStream::new(rx_stream, cm_handle);
        Ok(futures::StreamExt::boxed(guarded))
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let key = crate::order::OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        // Require the exchange order ID — Alpaca's DELETE endpoint uses the UUID.
        // If only clientOrderId is available, the caller should first resolve it
        // via fetch_open_orders.
        let order_id: SmolStr = match &request.state.id {
            Some(id) => id.0.clone(),
            None => {
                warn!(
                    instrument = %key.instrument,
                    "Alpaca cancel_order: no exchange order ID available (clientOrderId-only cancel not supported)"
                );
                return Some(crate::order::request::OrderResponseCancel {
                    key,
                    state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                        "exchange order ID required for cancel (fetch_open_orders to resolve)"
                            .into(),
                    ))),
                });
            }
        };

        let base = self.base_url();
        let http = self.http.clone();
        let url = format!("{base}/v2/orders/{order_id}");

        match rest_delete_with_retry(&self.rate_limiter, || http.delete(&url)).await {
            Ok(()) => {
                let exchange_order_id = OrderId(order_id);
                Some(crate::order::request::OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(exchange_order_id, Utc::now())),
                })
            }
            Err(e) => Some(crate::order::request::OrderResponseCancel { key, state: Err(e) }),
        }
    }

    /// # Position intent derivation
    ///
    /// Alpaca options/equities require explicit `position_intent`. This impl derives
    /// intent from `RequestOpen::reduce_only` and `side`:
    ///
    /// | reduce_only | side | intent       | use case                          |
    /// |-------------|------|--------------|-----------------------------------|
    /// | false       | Buy  | BuyToOpen    | open long / add to long position  |
    /// | false       | Sell | SellToOpen   | open short / write option         |
    /// | true        | Buy  | BuyToClose   | close short position              |
    /// | true        | Sell | SellToClose  | close long position               |
    ///
    /// For explicit control, use [`AlpacaClient::open_order_with_intent`].
    ///
    /// # Market order price
    ///
    /// The returned `Order.price` echoes the request price. For market orders this is
    /// typically `Decimal::ZERO` (a placeholder). The **actual fill price** arrives via
    /// the WebSocket `trade_updates` stream as a `Trade` event. Do not rely on
    /// `Order.price` from this REST ack for market order fill prices.
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let side = request.state.side;
        let reduce_only = request.state.reduce_only;
        self.open_order_inner(request, map_position_intent(side, reduce_only))
            .await
    }

    /// Fetches balances sequentially (unlike `account_snapshot` which parallelizes).
    ///
    /// Sequential fetch is intentional for live operation: under rate pressure, parallel
    /// requests may both hit 429 simultaneously and retry independently, doubling the
    /// backoff delay. The startup latency savings from `account_snapshot`'s parallel
    /// fetch are worth the tradeoff there; for periodic balance refreshes during live
    /// trading, sequential is safer.
    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let base = self.base_url();
        let http = self.http.clone();
        let mut result = Vec::new();

        // Only fetch the account (USD balance) when USD is among the requested assets.
        // Crypto-only requests skip this call to conserve rate-limit budget.
        let wants_usd = assets.is_empty()
            || assets
                .iter()
                .any(|a| a.name().as_str().eq_ignore_ascii_case("usd"));
        if wants_usd {
            // Pre-allocate URL to avoid re-allocation on each retry attempt.
            let account_url = format!("{base}/v2/account");
            let account: AlpacaAccount =
                rest_with_retry(&self.rate_limiter, || http.get(&account_url)).await?;
            result.extend(convert_account_to_balances(&account, assets));
        }

        // Fetch positions for non-USD asset balances (e.g., BTC, ETH from crypto holdings).
        let wants_non_usd = assets.is_empty()
            || assets
                .iter()
                .any(|a| !a.name().as_str().eq_ignore_ascii_case("usd"));
        if wants_non_usd {
            // Pre-allocate URL to avoid re-allocation on each retry attempt.
            let positions_url = format!("{base}/v2/positions");
            let positions: Vec<AlpacaPosition> =
                rest_with_retry(&self.rate_limiter, || http.get(&positions_url)).await?;
            result.extend(convert_positions_to_balances(&positions, assets));
        }

        Ok(result)
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let base = self.base_url();
        let http = self.http.clone();

        let open_orders =
            fetch_raw_open_orders(&http, &self.rate_limiter, base, instruments).await?;

        let result = open_orders
            .into_iter()
            .filter_map(|o| convert_open_order(&o))
            .collect();
        Ok(result)
    }

    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        let after_str = time_since.to_rfc3339();
        let base = self.base_url();
        let http = self.http.clone();

        let page = paginate_activities(&http, &self.rate_limiter, base, &after_str).await?;

        // Propagate truncation as an error so callers can detect incomplete results.
        // The crypto repo can match on `Truncated` and alert operators.
        if page.truncated {
            return Err(UnindexedClientError::Truncated {
                limit: MAX_ACTIVITY_PAGES,
            });
        }

        // Empty instruments slice means "all instruments" — same convention as
        // fetch_open_orders. Build a set only when filtering is needed.
        let trades = if instruments.is_empty() {
            page.activities
                .into_iter()
                .filter_map(|a| convert_activity_to_trade(&a))
                .collect()
        } else {
            let instrument_set: fnv::FnvHashSet<&str> =
                instruments.iter().map(|i| i.name().as_str()).collect();
            page.activities
                .into_iter()
                .filter(|a| instrument_set.contains(a.symbol.as_str()))
                .filter_map(|a| convert_activity_to_trade(&a))
                .collect()
        };

        Ok(trades)
    }
}

// ---------------------------------------------------------------------------
// AlpacaClient public extension methods (not on ExecutionClient trait)
// ---------------------------------------------------------------------------

impl AlpacaClient {
    /// Place an order with an explicit `position_intent` override.
    ///
    /// Use this instead of `ExecutionClient::open_order` when you need to specify
    /// exact position intent (e.g. `SellToOpen` for writing a short option, or
    /// `BuyToClose` for closing a short position by buying).
    ///
    /// `intent` is only sent for non-crypto symbols. For crypto symbols (those
    /// containing `/`) the field is always omitted regardless of `intent`.
    ///
    /// # Caller obligations
    /// The caller is responsible for passing the semantically correct intent.
    /// No validation is performed against the order side or existing position.
    pub async fn open_order_with_intent(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        intent: AlpacaPositionIntent,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        self.open_order_inner(request, intent).await
    }

    async fn open_order_inner(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        intent: AlpacaPositionIntent,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let instrument = request.key.instrument.clone();
        let side = request.state.side;
        let price = request.state.price;
        let quantity = request.state.quantity;
        let kind = request.state.kind;
        let time_in_force = request.state.time_in_force;
        let cid = request.key.cid.clone();

        let order_key = crate::order::OrderKey::new(
            ExchangeId::Alpaca,
            instrument.clone(),
            request.key.strategy.clone(),
            cid.clone(),
        );

        // Validate time_in_force before building the request — reject post_only early.
        let tif_str = match map_time_in_force(time_in_force) {
            Ok(s) => s,
            Err(msg) => {
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                        msg.to_string(),
                    ))),
                });
            }
        };

        let body = AlpacaOrderRequest {
            symbol: instrument.name().as_str(),
            qty: quantity.to_string(),
            side: map_side(side),
            order_type: map_order_kind(kind),
            time_in_force: tif_str,
            limit_price: if matches!(kind, OrderKind::Limit) {
                Some(price.to_string())
            } else {
                None
            },
            client_order_id: Some(cid.0.as_str()),
            // position_intent is required for options orders and valid (but optional)
            // for equities. It is NOT a valid field for crypto orders and must be
            // omitted, or Alpaca will return 422 Unprocessable Entity.
            // We detect options by OCC symbol format; equities also get the field
            // as it aids intent tracking on margin accounts.
            position_intent: if is_options_or_equity_symbol(instrument.name().as_str()) {
                Some(intent)
            } else {
                None
            },
        };

        let base = self.base_url();
        let http = self.http.clone();
        let rl = &self.rate_limiter;
        // Pre-allocate URL to avoid re-allocation on each retry attempt.
        let orders_url = format!("{base}/v2/orders");

        let result: Result<AlpacaOrderResponse, UnindexedClientError> =
            rest_with_retry(rl, || http.post(&orders_url).json(&body)).await;

        match result {
            Ok(resp) => {
                let exchange_order_id = OrderId(SmolStr::new(&resp.id));
                let time_exchange = parse_timestamp(&resp.created_at).unwrap_or_else(Utc::now);
                let filled_qty = Decimal::from_str(&resp.filled_qty).unwrap_or(Decimal::ZERO);

                Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: Ok(Open::new(exchange_order_id, time_exchange, filled_qty)),
                })
            }
            Err(e) => {
                let order_err = match e {
                    UnindexedClientError::Connectivity(ce) => UnindexedOrderError::Connectivity(ce),
                    UnindexedClientError::Api(ae) => UnindexedOrderError::Rejected(ae),
                    // AccountSnapshot, AccountStream, Truncated, and TruncatedSnapshot are not
                    // returned by rest_with_retry (REST-only path for orders), but matching
                    // explicitly ensures any new ClientError variant causes a compile error
                    // here rather than being silently misclassified. If a future refactor
                    // makes them reachable, the panic surfaces the bug loudly rather than
                    // producing a wrong error type.
                    UnindexedClientError::AccountSnapshot(_)
                    | UnindexedClientError::AccountStream(_)
                    | UnindexedClientError::Truncated { .. }
                    | UnindexedClientError::TruncatedSnapshot { .. } => {
                        unreachable!(
                            "rest_with_retry (order path) does not produce Account*/Truncated* variants"
                        )
                    }
                };
                Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: Err(order_err),
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// REST helpers
// ---------------------------------------------------------------------------

/// Maximum number of open orders returned by a single `/v2/orders` request.
///
/// Alpaca's API caps at 500; accounts exceeding this have an incomplete snapshot.
const MAX_OPEN_ORDERS: usize = 500;

/// Fetch all open orders from Alpaca, optionally filtered by symbol.
///
/// # Errors
///
/// Returns [`UnindexedClientError::TruncatedSnapshot`] when exactly 500 results
/// are returned, indicating the API limit was likely hit and data may be incomplete.
/// This is an Alpaca API limitation with no pagination support for open orders.
async fn fetch_raw_open_orders(
    http: &reqwest::Client,
    rate_limiter: &RateLimitTracker,
    base: &str,
    instruments: &[InstrumentNameExchange],
) -> Result<Vec<AlpacaOrderResponse>, UnindexedClientError> {
    let orders: Vec<AlpacaOrderResponse> = if instruments.is_empty() {
        rest_with_retry(rate_limiter, || {
            http.get(format!("{base}/v2/orders"))
                .query(&[("status", "open"), ("limit", "500")])
        })
        .await?
    } else {
        let symbols = instruments.iter().map(|i| i.name().as_str()).join(",");
        rest_with_retry(rate_limiter, || {
            http.get(format!("{base}/v2/orders")).query(&[
                ("status", "open"),
                ("limit", "500"),
                ("symbols", &symbols),
            ])
        })
        .await?
    };
    if orders.len() == MAX_OPEN_ORDERS {
        warn!(
            limit = MAX_OPEN_ORDERS,
            "Alpaca fetch_raw_open_orders: received exactly {MAX_OPEN_ORDERS} results — \
             response is likely truncated"
        );
        return Err(UnindexedClientError::TruncatedSnapshot {
            limit: MAX_OPEN_ORDERS,
        });
    }
    Ok(orders)
}

// ---------------------------------------------------------------------------
// Activity pagination
// ---------------------------------------------------------------------------

/// Maximum number of pages fetched by [`paginate_activities`].
///
/// 50 pages × 100 items = 5 000 fills. Exceeding this during recovery indicates
/// an unusually long outage; we warn and truncate rather than looping forever.
const MAX_ACTIVITY_PAGES: usize = 50;

/// Result of [`paginate_activities`] including truncation status.
///
/// When `truncated` is true, the `activities` vector contains a partial result
/// capped at [`MAX_ACTIVITY_PAGES`] pages. Callers should handle this case
/// appropriately — typically by alerting operators about potential data loss.
struct ActivityPage {
    activities: Vec<AlpacaActivity>,
    truncated: bool,
}

/// Fetch all FILL activities since `after` using token-based pagination.
///
/// Alpaca returns up to `ALPACA_MAX_ACTIVITIES` per page. If a full page is
/// returned, the next request uses the last item's `id` as the `page_token`.
/// Pagination terminates when a page has fewer items than `page_size`, or after
/// [`MAX_ACTIVITY_PAGES`] pages (whichever comes first).
///
/// Returns [`ActivityPage`] with `truncated = true` if the page limit was reached,
/// allowing callers to detect and handle partial results.
// Compile-time string form of ALPACA_MAX_ACTIVITIES (avoids runtime to_string() allocation).
const PAGE_SIZE_STR: &str = "100"; // must match ALPACA_MAX_ACTIVITIES
const _: () = assert!(
    ALPACA_MAX_ACTIVITIES == 100,
    "PAGE_SIZE_STR must be updated to match ALPACA_MAX_ACTIVITIES",
);

async fn paginate_activities(
    http: &reqwest::Client,
    rate_limiter: &RateLimitTracker,
    base: &str,
    after: &str,
) -> Result<ActivityPage, UnindexedClientError> {
    let mut all = Vec::with_capacity(ALPACA_MAX_ACTIVITIES);
    let mut page_token: Option<String> = None;
    let mut pages = 0usize;
    let mut truncated = false;

    loop {
        if pages >= MAX_ACTIVITY_PAGES {
            truncated = true;
            break;
        }
        pages += 1;
        // Borrow page_token as &str so the closure can capture by reference without cloning.
        let page_token_ref = page_token.as_deref();
        let activities: Vec<AlpacaActivity> = rest_with_retry(rate_limiter, || {
            let mut req = http.get(format!("{base}/v2/account/activities")).query(&[
                ("activity_type", "FILL"),
                ("after", after),
                ("page_size", PAGE_SIZE_STR),
                ("direction", "asc"),
            ]);
            if let Some(token) = page_token_ref {
                req = req.query(&[("page_token", token)]);
            }
            req
        })
        .await?;

        let page_len = activities.len();
        // Capture the page token from the current page BEFORE extending `all`, so that
        // any future filtering between here and all.extend cannot shift the last element
        // and cause the same page to be re-fetched indefinitely.
        //
        // Alpaca activity IDs are ULID-format (monotonically increasing) and are accepted
        // as exclusive page_token cursors by GET /v2/account/activities: the next page
        // begins with the item AFTER the token, so the boundary item is not re-delivered.
        // The pagination contract relies on this property — verify against Alpaca API docs
        // if behaviour changes.
        let page_token_candidate = activities.last().map(|a| a.id.clone());
        all.extend(activities);

        if page_len < ALPACA_MAX_ACTIVITIES {
            break;
        }
        match page_token_candidate {
            Some(token) if !token.is_empty() => {
                debug!("Alpaca paginate_activities: fetching next page ({page_len} results)");
                page_token = Some(token);
            }
            // Empty token or no last item — pagination complete.
            // Guard against empty string to prevent infinite loop if Alpaca ever
            // returns a full page with last.id = "" (would restart from beginning).
            _ => break,
        }
    }

    Ok(ActivityPage {
        activities: all,
        truncated,
    })
}

// ---------------------------------------------------------------------------
// WebSocket connection manager
// ---------------------------------------------------------------------------

/// Long-running task managing the WebSocket lifecycle for account_stream.
///
/// Loop: connect → auth → subscribe → stream events → on disconnect → backoff
/// → fill recovery → reconnect. The `tx` channel persists across reconnections
/// so the consumer sees a seamless event stream.
///
/// Terminates when the consumer drops the stream or max reconnect attempts are
/// exhausted.
#[allow(clippy::cognitive_complexity)] // the inner select! loop owns `ws` and mutates `backoff` —
// extracting to a function requires threading 4 non-Clone values (ws, tx, dedup, backoff)
// through the call, which adds more complexity than it removes
async fn connection_manager(
    tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: SharedDedupCache,
    config: Arc<AlpacaConfig>,
    http: reqwest::Client,
    rate_limiter: Arc<RateLimitTracker>,
    instruments: Vec<InstrumentNameExchange>,
    initial_ws: Option<WebSocket>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut disconnect_time: Option<DateTime<Utc>> = None;
    let mut current_ws = initial_ws;

    'outer: loop {
        // --- Connect (skip on first iteration if initial_ws was provided) ---
        let mut ws = match current_ws.take() {
            Some(ws) => ws,
            None => match connect_and_subscribe(&config).await {
                Ok(ws) => ws,
                Err(e) => {
                    error!(%e, "Alpaca WS connect/subscribe failed");
                    if !backoff.wait().await {
                        error!("Alpaca max reconnect attempts exhausted");
                        break;
                    }
                    continue;
                }
            },
        };
        info!("Alpaca account_stream connected and subscribed");
        // Session established — reset backoff regardless of whether any text events
        // arrive. Without this, a heartbeat-only session (Pings only, no trade_updates)
        // would never call process_ws_text and therefore never reset the counter,
        // causing the next disconnect to exhaust the reconnect budget faster than expected.
        //
        // Edge case: if the server accepts the WS handshake and immediately closes the
        // connection (fast-accept-close loop), backoff resets on every iteration, causing
        // all MAX_RECONNECT_ATTEMPTS attempts to run at INITIAL_BACKOFF_MS rather than
        // escalating. This is accepted behaviour for this pathological server scenario —
        // the 10-attempt budget still provides ~10 s of protection before giving up.
        backoff.reset();

        // --- Fill recovery after reconnect ---
        // Runs before the event loop so live events arriving during recovery are
        // captured by the already-connected WS session. The dedup cache prevents
        // duplicates between recovered REST fills and live WS events.
        if let Some(dt) = disconnect_time.take() {
            let base = config.rest_base_url();
            let after_str = dt.to_rfc3339();
            match tokio::time::timeout(
                Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS),
                recover_fills(
                    &http,
                    &rate_limiter,
                    &instruments,
                    base,
                    &after_str,
                    &tx,
                    &dedup,
                ),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => warn!(
                    timeout_secs = FILL_RECOVERY_TIMEOUT_SECS,
                    "Alpaca fill recovery timed out — some fills may be missing"
                ),
            }
        }

        // --- Stream events ---
        // Each iteration of the inner loop polls ws.next(), a heartbeat timer,
        // and tx.closed() simultaneously via select!. The heartbeat deadline is
        // reset on every received message (rolling window).
        //
        // The timer is pinned once and reset in-place via Sleep::reset(), avoiding
        // a new Sleep allocation and Tokio timer-wheel registration per loop iteration.
        // Track the wall-clock time of the last received message. Used to anchor
        // fill recovery after a heartbeat timeout: Utc::now() at reconnect time
        // would be ~HEARTBEAT_TIMEOUT_SECS after the last real message, causing
        // the recovery window to miss fills in that silent period.

        let mut last_message_time = Utc::now();
        let heartbeat = tokio::time::sleep(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS));
        tokio::pin!(heartbeat);

        // Resets the rolling heartbeat deadline and records the wall-clock receive time.
        // Pin<&mut Sleep> cannot be passed to a regular function, so a macro avoids
        // repeating the same two-line block across every message-bearing select! arm.
        // NOTE: must be defined AFTER the variables it captures for macro hygiene.
        macro_rules! reset_heartbeat {
            () => {
                heartbeat.as_mut().reset(
                    tokio::time::Instant::now() + Duration::from_secs(HEARTBEAT_TIMEOUT_SECS),
                );
                last_message_time = Utc::now();
            };
        }
        loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(WsMessage::Ping(_))) => {
                            // tokio-tungstenite automatically queues a Pong when poll_next
                            // returns a Ping; sending a second Pong would be a duplicate.
                            reset_heartbeat!();
                        }
                        Some(Ok(WsMessage::Text(text))) => {
                            process_ws_text(text.as_str(), &tx, &dedup, &mut backoff);
                            reset_heartbeat!();
                        }
                        Some(Ok(WsMessage::Binary(bytes))) => {
                            // Alpaca paper trading sends binary-framed JSON.
                            match std::str::from_utf8(&bytes) {
                                Ok(text) => {
                                    process_ws_text(text, &tx, &dedup, &mut backoff);
                                    // Only reset heartbeat for valid UTF-8 frames that may carry
                                    // real events. A corrupt binary frame (e.g. from a proxy)
                                    // must not keep the watchdog from firing.
                                    reset_heartbeat!();
                                }
                                Err(e) => warn!(%e, "Alpaca WS binary frame: not valid UTF-8"),
                            }
                        }
                        Some(Ok(WsMessage::Close(frame))) => {
                            warn!(frame = ?frame, "Alpaca WS closed by server");
                            break;
                        }
                        Some(Ok(_)) => {} // Pong, Frame — ignore
                        Some(Err(e)) => {
                            warn!(%e, "Alpaca WS error, reconnecting");
                            break;
                        }
                        None => {
                            warn!("Alpaca WS stream ended, reconnecting");
                            break;
                        }
                    }
                }
                _ = &mut heartbeat => {
                    warn!(
                        timeout_secs = HEARTBEAT_TIMEOUT_SECS,
                        "Alpaca heartbeat timeout, reconnecting"
                    );
                    // Heartbeat timeout is a failure — do NOT reset backoff here.
                    // Backoff resets on successful event receipt in process_ws_text.
                    break;
                }
                _ = tx.closed() => {
                    debug!("Alpaca account_stream consumer dropped, terminating");
                    let _ = tokio::time::timeout(
                        Duration::from_secs(WS_CLOSE_TIMEOUT_SECS),
                        ws.close(None),
                    ).await;
                    break 'outer;
                }
            }
        }

        // --- Record disconnect time for fill recovery ---
        // Anchor to last_message_time, not Utc::now(). For heartbeat-triggered
        // disconnects, Utc::now() would be ~HEARTBEAT_TIMEOUT_SECS after the last
        // real message, causing the recovery window to miss fills in that gap.
        disconnect_time =
            Some(last_message_time - chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS));

        // --- Close stale WS ---
        let _ =
            tokio::time::timeout(Duration::from_secs(WS_CLOSE_TIMEOUT_SECS), ws.close(None)).await;

        if tx.is_closed() {
            break;
        }
        if !backoff.wait().await {
            error!("Alpaca max reconnect attempts exhausted, stream terminating");
            break;
        }
    }
}

/// Connect to the Alpaca WebSocket, authenticate, and subscribe to trade_updates.
///
/// On auth or subscribe failure the connection is closed cleanly.
async fn connect_and_subscribe(config: &AlpacaConfig) -> Result<WebSocket, UnindexedClientError> {
    let url = config.ws_url();
    debug!(%url, "Alpaca: connecting to WebSocket");

    let mut ws = barter_integration::protocol::websocket::connect(url)
        .await
        .map_err(|e| UnindexedClientError::AccountStream(format!("WS connect: {e}")))?;

    // auth + subscribe with overall timeout
    let result = tokio::time::timeout(
        Duration::from_secs(WS_HANDSHAKE_TIMEOUT_SECS),
        ws_handshake(&mut ws, config),
    )
    .await;

    match result {
        Ok(Ok(())) => Ok(ws),
        Ok(Err(e)) => {
            let _ = ws.close(None).await;
            Err(UnindexedClientError::AccountStream(e))
        }
        Err(_) => {
            let _ = ws.close(None).await;
            Err(UnindexedClientError::AccountStream(format!(
                "WS handshake timed out after {WS_HANDSHAKE_TIMEOUT_SECS}s"
            )))
        }
    }
}

/// Perform the Alpaca WS auth + subscribe sequence on a connected WebSocket.
///
/// Protocol:
/// 1. Send `{"action":"auth","key":...,"secret":...}`
/// 2. Await `{"stream":"authorization","data":{"status":"authorized",...}}`
/// 3. Send `{"action":"listen","data":{"streams":["trade_updates"]}}`
/// 4. Await `{"stream":"listening","data":{"streams":["trade_updates"]}}`
async fn ws_handshake(ws: &mut WebSocket, config: &AlpacaConfig) -> Result<(), String> {
    // Step 1: send auth
    let auth = serde_json::json!({
        "action": "auth",
        "key": config.api_key(),
        // direct field access within module — secret_key has no pub getter to
        // prevent external credential exposure
        "secret": config.secret_key,
    })
    .to_string();
    ws.send(WsMessage::Text(auth.into()))
        .await
        .map_err(|e| format!("WS auth send: {e}"))?;

    // Step 2: wait for authorization message
    loop {
        match ws.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                if let Some(result) = check_auth_response(text.as_str()) {
                    result?;
                    break;
                }
            }
            Some(Ok(WsMessage::Binary(bytes))) => {
                if let Ok(text) = std::str::from_utf8(&bytes)
                    && let Some(result) = check_auth_response(text)
                {
                    result?;
                    break;
                }
            }
            Some(Err(e)) => return Err(format!("WS error during auth: {e}")),
            None => return Err("WS closed before auth response".into()),
            _ => {} // ping/pong during auth — ignore
        }
    }

    // Step 3: subscribe to trade_updates
    let sub = serde_json::json!({
        "action": "listen",
        "data": { "streams": ["trade_updates"] }
    })
    .to_string();
    ws.send(WsMessage::Text(sub.into()))
        .await
        .map_err(|e| format!("WS subscribe send: {e}"))?;

    // Step 4: wait for listening acknowledgment (optional but confirms subscription)
    loop {
        match ws.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                if check_listen_ack(text.as_str()) {
                    break;
                }
                // Other messages in this window are NOT buffered and are permanently
                // dropped — callers must reconcile order state via fetch_open_orders
                // after each (re)connect, as documented in account_stream's doc comment.
                // Log at warn! for trade_updates (fill/lifecycle events) to ensure
                // production operators see dropped events; trace! for other streams.
                if let Ok(msg) = serde_json::from_str::<AlpacaStreamMessage<'_>>(text.as_str()) {
                    if msg.stream == "trade_updates" {
                        warn!(stream = %msg.stream, "WS trade_updates event dropped during listen-ack handshake — will be recovered via REST for fills, but lifecycle events (new/canceled) are lost");
                    } else {
                        trace!(stream = %msg.stream, "WS message dropped during listen-ack handshake");
                    }
                } else {
                    trace!(
                        bytes = text.len(),
                        "WS non-stream message dropped during listen-ack handshake"
                    );
                }
            }
            Some(Ok(WsMessage::Binary(bytes))) => {
                if let Ok(text) = std::str::from_utf8(&bytes)
                    && check_listen_ack(text)
                {
                    break;
                }
                trace!(
                    bytes = bytes.len(),
                    "WS binary message dropped during listen-ack handshake"
                );
            }
            Some(Err(e)) => return Err(format!("WS error during subscribe: {e}")),
            None => return Err("WS closed before subscribe ack".into()),
            _ => {}
        }
    }

    info!("Alpaca WS authenticated and subscribed to trade_updates");
    Ok(())
}

/// Parse a WS message to check for authorization response.
/// Returns `None` if the message is not an authorization response.
/// Returns `Some(Ok(()))` on success, `Some(Err(...))` on auth failure.
///
/// Uses `AlpacaStreamMessage` for consistency with the rest of the WS parsing pipeline.
fn check_auth_response(text: &str) -> Option<Result<(), String>> {
    let msg = serde_json::from_str::<AlpacaStreamMessage<'_>>(text).ok()?;
    if msg.stream != "authorization" {
        return None;
    }
    #[derive(Deserialize)]
    struct AuthData<'a> {
        status: &'a str,
    }
    let data = serde_json::from_str::<AuthData<'_>>(msg.data.get()).ok()?;
    if data.status == "authorized" {
        Some(Ok(()))
    } else {
        Some(Err(format!(
            "Alpaca WS auth failed: status={}",
            data.status
        )))
    }
}

/// Returns `true` if the WS message is a listening acknowledgment for trade_updates.
///
/// Uses `AlpacaStreamMessage` for consistency with the rest of the WS parsing pipeline.
fn check_listen_ack(text: &str) -> bool {
    let Ok(msg) = serde_json::from_str::<AlpacaStreamMessage<'_>>(text) else {
        return false;
    };
    if msg.stream != "listening" {
        return false;
    }
    #[derive(Deserialize)]
    struct ListenData<'a> {
        #[serde(borrow)]
        streams: Vec<&'a str>,
    }
    let Ok(data) = serde_json::from_str::<ListenData<'_>>(msg.data.get()) else {
        return false;
    };
    data.streams.contains(&"trade_updates")
}

// ---------------------------------------------------------------------------
// Process incoming WS messages
// ---------------------------------------------------------------------------

/// Parse a raw WS text message and forward relevant account events to `tx`.
fn process_ws_text(
    text: &str,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    backoff: &mut ExponentialBackoff,
) {
    let msg: AlpacaStreamMessage<'_> = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            trace!(
                %e,
                raw = ?&text[..text.len().min(200)],
                "Alpaca WS: skipped non-JSON message"
            );
            return;
        }
    };

    // Any successfully-parsed frame proves the connection is alive — reset backoff here
    // so that unrecognised event types (pending_cancel, replaced, etc.) also reset it.
    backoff.reset();

    match msg.stream.as_str() {
        "trade_updates" => {
            let update: AlpacaTradeUpdate<'_> = match serde_json::from_str(msg.data.get()) {
                Ok(u) => u,
                Err(e) => {
                    // Use warn (not trace): the outer frame was valid trade_updates JSON,
                    // so failure here signals an unexpected payload format. Could indicate
                    // an Alpaca API change or a missing required field dropping a real
                    // event (e.g. a rejected order). Visible at production log levels.
                    warn!(
                        %e,
                        raw = ?&msg.data.get()[..msg.data.get().len().min(200)],
                        "Alpaca WS trade_updates: failed to deserialize event — event dropped"
                    );
                    return;
                }
            };

            // PERF: Check dedup BEFORE constructing the full event for fill events.
            // This avoids heap allocations (Trade, InstrumentNameExchange, TradeId) on
            // duplicate fills, which occur on every reconnect during recovery overlap.
            if is_fill_event(&update) {
                let key = early_dedup_key(&update);
                if is_duplicate(dedup, &key) {
                    trace!("Alpaca WS: skipping duplicate fill event (early check)");
                    return;
                }
            }

            if let Some(event) = convert_trade_update(update) {
                // Consumer dropped errors are benign; connection_manager will detect
                // tx.closed() on the next select! poll and exit cleanly.
                let _ = tx.send(event);
            }
        }
        "authorization" | "listening" => {
            // Ack messages can appear during initial stream if the handshake was
            // not fully awaited. These are benign.
            trace!(stream = %msg.stream, "Alpaca WS: auth/listen ack received during stream");
        }
        other => {
            trace!(%other, "Alpaca WS: ignoring unknown stream type");
        }
    }
}

/// Extract a dedup key from an account event, if applicable.
///
/// Uses `trade.id` as the key. Both the WS path (`convert_trade_update`) and the
/// REST recovery path (`recover_fills`) synthesise `TradeId` as
/// `"{order_id}:{cumulative_filled_qty}"`, ensuring the same fill produces the
/// same key regardless of which path delivered it.
fn fill_dedup_key_from_event(event: &UnindexedAccountEvent) -> Option<&SmolStr> {
    match &event.kind {
        // Return a reference to avoid cloning the SmolStr; is_duplicate takes &SmolStr.
        AccountEventKind::Trade(trade) => Some(&trade.id.0),
        _ => None,
    }
}

/// Returns `true` if this event type produces a fill (Trade) event.
///
/// Used for early dedup check before allocating the full event.
#[inline]
fn is_fill_event(update: &AlpacaTradeUpdate<'_>) -> bool {
    matches!(update.event.as_str(), "fill" | "partial_fill")
}

/// Construct the dedup key from raw WS update fields without allocating the full event.
///
/// Key format: `"{order_id}:{cumulative_filled_qty}"` — same as `convert_trade_update`
/// and `recover_fills` produce, ensuring cross-source dedup works correctly.
///
/// Note: The `format_smolstr!` call heap-allocates for UUID-length order IDs (36 chars
/// exceeds SmolStr's 22-byte inline limit). This is unavoidable given the key length.
/// Passing the Decimal directly to format_smolstr! (rather than via intermediate String)
/// lets Decimal's Display impl write directly into the buffer, eliminating one allocation.
fn early_dedup_key(update: &AlpacaTradeUpdate<'_>) -> SmolStr {
    let filled_qty = update.order.filled_qty.unwrap_or("0");
    // Parse and normalize to match convert_trade_update's key format exactly.
    // Decimal::from_str + normalize() are stack-only; no heap allocation until format_smolstr!.
    let qty = Decimal::from_str(filled_qty).unwrap_or(Decimal::ZERO);
    format_smolstr!("{}:{}", update.order.id, qty.normalize())
}

// ---------------------------------------------------------------------------
// Fill recovery
// ---------------------------------------------------------------------------

/// Fetch fills missed during a WS disconnect and forward through the dedup cache.
async fn recover_fills(
    http: &reqwest::Client,
    rate_limiter: &RateLimitTracker,
    instruments: &[InstrumentNameExchange],
    base: &str,
    after: &str,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) {
    // Empty `instruments` means "recover for all subscribed symbols" (same convention as
    // `fetch_trades` / `fetch_open_orders`). Skip the set allocation when not filtering.
    info!(%after, instruments = instruments.len(), "Alpaca recovering fills after reconnect");
    let instrument_set: fnv::FnvHashSet<&str> = if instruments.is_empty() {
        fnv::FnvHashSet::default()
    } else {
        instruments.iter().map(|i| i.name().as_str()).collect()
    };

    let page = match paginate_activities(http, rate_limiter, base, after).await {
        Ok(p) => p,
        Err(e) => {
            error!(%e, "Alpaca fill recovery: REST request failed");
            return;
        }
    };

    // Log truncation as error — the returned data is partial and some fills are
    // permanently lost. Callers cannot propagate this error (recover_fills returns ()),
    // but the log ensures operators are alerted.
    if page.truncated {
        error!(
            max_pages = MAX_ACTIVITY_PAGES,
            "Alpaca fill recovery: max page limit reached, truncating — \
             fills from this outage are permanently lost. Manual reconciliation required."
        );
    }

    let activities = page.activities;

    let mut recovered = 0u32;
    let mut duplicates = 0u32;

    // Track cumulative filled qty per order so that the synthesised TradeId matches
    // the WS path: both produce "{order_id}:{cumulative_filled_qty}". This prevents
    // same-size partial fill collisions (e.g. two 1-lot fills on a 2-lot order)
    // and ensures cross-source dedup works correctly after reconnect.
    //
    // Activities are fetched with direction=asc so they arrive in chronological order.
    // Cumulative accumulation here must match the WS path's order.filled_qty progression.
    // If Alpaca violates this ordering guarantee, dedup keys will diverge and fills
    // may be dropped or duplicated.
    // Borrow `activities` so &str keys into order_id strings remain valid for the
    // entire loop. Consuming iteration would drop each activity at end of its iteration,
    // invalidating keys that still need to be looked up by later fills on the same order.
    let mut cumulative_qty: FnvHashMap<&str, Decimal> = FnvHashMap::default();

    for activity in &activities {
        if !instrument_set.is_empty() && !instrument_set.contains(activity.symbol.as_str()) {
            // Safe to skip without advancing cumulative_qty: each Alpaca order is bound
            // to exactly one symbol, so no fill for a different symbol can ever share
            // the same order_id as a fill we're tracking.
            continue;
        }

        // Advance cumulative_qty for every activity, regardless of whether parsing
        // succeeds. The WS path uses Alpaca's cumulative filled_qty, which counts ALL
        // fills — including those with malformed fields. Skipping the counter for a bad
        // fill causes subsequent fills to produce dedup keys that diverge from the WS
        // path (off by the bad fill's qty), so the next good fill appears as a duplicate
        // on one path and a new fill on the other, resulting in either a missed or a
        // double-delivered fill for that execution.
        //
        // activity.qty is always populated for FILL activities (Alpaca guarantees it).
        // unwrap_or(ZERO) guards against unexpected API changes; a zero qty skips
        // incrementing the cumulative counter for that activity. Note: a non-empty
        // but non-parseable qty string (e.g. an API regression sending "abc") also
        // produces exec_qty=ZERO — the counter stalls, causing subsequent fills on
        // the same order to produce dedup keys that diverge from the WS path.
        let exec_qty = Decimal::from_str(&activity.qty).unwrap_or(Decimal::ZERO);
        let cum = cumulative_qty
            .entry(activity.order_id.as_str())
            .or_default();
        *cum += exec_qty;
        let cumulative = *cum;

        let mut trade = match convert_activity_to_trade(activity) {
            Some(t) => t,
            None => {
                warn!(id = %activity.id, symbol = %activity.symbol, "Alpaca: skipping activity with unparseable fields");
                continue; // Counter already advanced; dedup key sequence stays aligned with WS.
            }
        };

        // Override trade.id to match the WS synthesised format so that
        // fill_dedup_key_from_event produces the same key for both sources.
        // Normalise the cumulative Decimal to strip trailing zeros so the string
        // representation matches the WS path (e.g. "1" == "1.00" after normalize).
        trade.id = TradeId(format_smolstr!(
            "{}:{}",
            activity.order_id,
            cumulative.normalize()
        ));

        let event = UnindexedAccountEvent::new(ExchangeId::Alpaca, AccountEventKind::Trade(trade));

        // Re-use fill_dedup_key_from_event so the key logic is in one place.
        if fill_dedup_key_from_event(&event).is_some_and(|k| is_duplicate(dedup, k)) {
            duplicates += 1;
            continue;
        }
        if tx.send(event).is_err() {
            debug!("Alpaca fill recovery: consumer dropped during recovery");
            return;
        }
        recovered += 1;
    }

    info!(recovered, duplicates, "Alpaca fill recovery complete");
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

/// Convert an Alpaca account response to barter balance entries.
///
/// Returns a single USD balance with:
/// - `total` = equity (total account value including open positions)
/// - `free` = options_buying_power (if available) or buying_power
///
/// If `assets` is non-empty, only returns the balance if "usd" (case-insensitive)
/// is in the requested set. An empty `assets` slice returns the USD balance unconditionally.
fn convert_account_to_balances(
    account: &AlpacaAccount,
    assets: &[AssetNameExchange],
) -> Vec<AssetBalance<AssetNameExchange>> {
    // Preserve the caller's casing for the USD asset name (e.g. "USD" vs "usd").
    // When no filter is given, fall back to lowercase "usd" as the canonical form.
    let usd_entry = assets
        .iter()
        .find(|a| a.name().as_str().eq_ignore_ascii_case("usd"));

    // Filter check: if assets is specified, only return USD balance if requested.
    if !assets.is_empty() && usd_entry.is_none() {
        return Vec::new();
    }

    let usd_name = usd_entry
        .cloned()
        .unwrap_or_else(|| AssetNameExchange::new("usd"));

    let total = Decimal::from_str(&account.equity).unwrap_or(Decimal::ZERO);
    let free = account
        .options_buying_power
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        // Filter out zero: Alpaca returns options_buying_power="0.00" for equity-only
        // accounts (options not enabled) rather than omitting the field. Without this
        // filter, unwrap_or_else never fires and the engine reports free=0, blocking
        // all orders on an account that may have substantial buying_power.
        .filter(|d| !d.is_zero())
        .unwrap_or_else(|| Decimal::from_str(&account.buying_power).unwrap_or(Decimal::ZERO));

    vec![AssetBalance::new(
        usd_name,
        Balance::new(total, free),
        Utc::now(),
    )]
}

/// Convert Alpaca positions to crypto asset balance entries.
///
/// Only positions with `asset_class == "crypto"` are included. The base asset
/// is extracted from the symbol (e.g., `"BTC/USD"` → `"btc"`).
///
/// - `total` = quantity of the holding in base currency units (e.g. 0.5 BTC)
/// - `free`  = qty_available (base currency units not locked in open orders)
///
/// If `assets` is non-empty, only positions whose base asset name matches an
/// entry in the slice (case-insensitive) are returned.
fn convert_positions_to_balances(
    positions: &[AlpacaPosition],
    assets: &[AssetNameExchange],
) -> Vec<AssetBalance<AssetNameExchange>> {
    let now = Utc::now();
    positions
        .iter()
        .filter(|p| p.asset_class.eq_ignore_ascii_case("crypto"))
        .filter_map(|p| {
            // Alpaca crypto symbols are "BASE/QUOTE" (e.g., "BTC/USD").
            // Extract the base currency as the asset name.
            let base = p
                .symbol
                .split('/')
                .next()
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_else(|| p.symbol.to_ascii_lowercase());

            // Apply assets filter if specified.
            if !assets.is_empty()
                && !assets
                    .iter()
                    .any(|a| a.name().as_str().eq_ignore_ascii_case(&base))
            {
                return None;
            }

            // total and free are in base currency units (e.g., BTC), not USD,
            // consistent with AssetBalance semantics for currency/crypto assets.
            let total = Decimal::from_str(&p.qty).unwrap_or(Decimal::ZERO);
            let free = Decimal::from_str(&p.qty_available).unwrap_or(Decimal::ZERO);
            let asset_name = AssetNameExchange::new(base);
            Some(AssetBalance::new(
                asset_name,
                Balance::new(total, free),
                now,
            ))
        })
        .collect()
}

/// Group a list of open orders into per-instrument snapshots for account_snapshot.
///
/// When `instruments` is non-empty, a snapshot is returned for every requested instrument
/// (possibly with an empty orders list). When empty, only instruments with open orders
/// are returned.
fn build_instrument_snapshots(
    orders: Vec<AlpacaOrderResponse>,
    instruments: &[InstrumentNameExchange],
) -> Vec<InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>> {
    // Build ordered map from symbol → snapshot to preserve deterministic ordering.
    let mut by_symbol: IndexMap<SmolStr, Vec<_>> = IndexMap::new();

    for order in orders {
        let sym = SmolStr::new(&order.symbol);
        if let Some(converted) = convert_open_order(&order) {
            let wrapped = crate::order::Order {
                key: converted.key,
                side: converted.side,
                price: converted.price,
                quantity: converted.quantity,
                kind: converted.kind,
                time_in_force: converted.time_in_force,
                state: OrderState::active(converted.state),
            };
            by_symbol.entry(sym).or_default().push(wrapped);
        }
    }

    // If instruments is empty, return all; otherwise filter to requested set.
    if instruments.is_empty() {
        by_symbol
            .into_iter()
            .map(|(sym, orders)| {
                InstrumentAccountSnapshot::new(InstrumentNameExchange::new(sym), orders)
            })
            .collect()
    } else {
        instruments
            .iter()
            .map(|inst| {
                // swap_remove is O(1); output order is determined by the `instruments`
                // slice, not by the internal IndexMap order of `by_symbol`.
                let orders = by_symbol
                    .swap_remove(inst.name().as_str())
                    .unwrap_or_default();
                InstrumentAccountSnapshot::new(inst.clone(), orders)
            })
            .collect()
    }
}

/// Convert an Alpaca REST order into barter's Open state order.
fn convert_open_order(
    o: &AlpacaOrderResponse,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let order_id = OrderId(SmolStr::new(&o.id));
    let cid = o
        .client_order_id
        .as_deref()
        .map(ClientOrderId::new)
        .unwrap_or_else(|| ClientOrderId::new(o.id.as_str()));

    let instrument = InstrumentNameExchange::new(&o.symbol);
    let side = parse_side(&o.side)?;
    let quantity = Decimal::from_str(o.qty.as_deref().unwrap_or("0")).ok()?;
    // Notional orders (placed by dollar value, qty=null) have no representable quantity.
    // Skip them rather than recording a zero-quantity order that would corrupt reconciliation.
    if quantity.is_zero() {
        return None;
    }
    let price = o
        .limit_price
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let filled_qty = Decimal::from_str(&o.filled_qty).unwrap_or(Decimal::ZERO);
    let kind = parse_order_kind(&o.order_type)?;
    let time_in_force = parse_time_in_force(&o.time_in_force);
    let time_exchange = parse_timestamp(&o.created_at).unwrap_or_else(Utc::now);

    Some(Order {
        key: OrderKey::new(
            ExchangeId::Alpaca,
            instrument,
            // Alpaca doesn't carry strategy IDs in any response field.
            StrategyId::unknown(),
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: Open::new(order_id, time_exchange, filled_qty),
    })
}

/// Convert an Alpaca FILL activity into a barter Trade.
fn convert_activity_to_trade(
    a: &AlpacaActivity,
) -> Option<Trade<QuoteAsset, InstrumentNameExchange>> {
    let trade_id = TradeId::new(&a.id);
    let order_id = OrderId(SmolStr::new(&a.order_id));
    let instrument = InstrumentNameExchange::new(&a.symbol);
    let side = parse_side(&a.side)?;
    let price = Decimal::from_str(&a.price).ok()?;
    let quantity = Decimal::from_str(&a.qty).ok()?;
    let time_exchange = parse_timestamp(&a.transaction_time).unwrap_or_else(|| {
        warn!(id = %a.id, "Alpaca activity: unparseable transaction_time, using now");
        Utc::now()
    });

    // Alpaca equities and options are commission-free. Crypto trades incur
    // maker/taker fees (currently 0.15–0.25%); callers should account for this
    // separately if accurate PnL tracking for crypto is required.
    Some(Trade::new(
        trade_id,
        order_id,
        instrument,
        StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        AssetFees::quote_fees(Decimal::ZERO),
    ))
}

/// Convert a WebSocket trade_update event into a barter AccountEvent.
fn convert_trade_update(update: AlpacaTradeUpdate<'_>) -> Option<UnindexedAccountEvent> {
    // Early exit for unrecognised event types before incurring allocations for
    // instrument/order_id/cid — those are wasted for unknown events.
    let event_str = update.event.as_str();
    if !matches!(
        event_str,
        "fill"
            | "partial_fill"
            | "new"
            | "accepted"
            | "pending_new"
            | "canceled"
            | "expired"
            | "replaced"
            | "done_for_day"
            | "rejected"
    ) {
        trace!(event = %event_str, "Alpaca WS: ignoring trade_updates event type");
        return None;
    }

    let order = &update.order;
    let instrument = InstrumentNameExchange::new(&*order.symbol);
    let order_id = OrderId(order.id.clone());
    let cid = order
        .client_order_id
        .as_deref()
        .map(ClientOrderId::new)
        .unwrap_or_else(|| ClientOrderId::new(order.id.as_str()));

    match event_str {
        "fill" | "partial_fill" => {
            // Use event-level price/qty for the per-execution trade.
            let price = update.price.and_then(|s| Decimal::from_str(s).ok())?;
            let quantity = update.qty.and_then(|s| Decimal::from_str(s).ok())?;
            let side = parse_side(&order.side)?;
            let time_exchange = update
                .timestamp
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);

            // Trade ID: synthesise from order_id + cumulative filled qty since
            // Alpaca WS trade_updates don't include an activity ID.
            // Normalise via Decimal to strip trailing zeros so the key matches the
            // REST recovery path (e.g. "1.00" and "1" both normalise to "1").
            //
            // If filled_qty is unparseable (API regression), cum_qty falls back to
            // zero. Two consecutive bad fills on the same order would produce the same
            // dedup key ("order_id:0"), causing the second fill to be silently dropped.
            // Warn loudly so API regressions are surfaced immediately.
            let cum_qty = Decimal::from_str(order.filled_qty.unwrap_or("0"))
                .inspect_err(|e| {
                    warn!(
                        order_id = %order.id,
                        filled_qty = ?order.filled_qty,
                        %e,
                        "Alpaca WS: failed to parse filled_qty — dedup key will use 0, \
                         a second malformed fill on the same order would be deduplicated away"
                    );
                })
                .unwrap_or(Decimal::ZERO);
            let trade_id = TradeId(format_smolstr!("{}:{}", order.id, cum_qty.normalize()));

            // Alpaca equities and options are commission-free. Crypto trades incur
            // maker/taker fees (currently 0.15–0.25%); callers should account for this
            // separately if accurate PnL tracking for crypto is required.
            let trade = Trade::new(
                trade_id,
                order_id,
                instrument,
                StrategyId::unknown(),
                time_exchange,
                side,
                price,
                quantity,
                AssetFees::quote_fees(Decimal::ZERO),
            );
            Some(UnindexedAccountEvent::new(
                ExchangeId::Alpaca,
                AccountEventKind::Trade(trade),
            ))
        }

        "new" | "accepted" | "pending_new" => {
            // Order acknowledged by Alpaca — emit an OrderSnapshot.
            let side = parse_side(&order.side)?;
            let quantity = Decimal::from_str(order.qty.unwrap_or("0")).unwrap_or(Decimal::ZERO);
            // Notional orders (placed by dollar value) have qty=null, yielding quantity=0.
            // Emitting an OrderSnapshot with quantity=0 would corrupt OMS state — skip,
            // consistent with convert_open_order which also returns None for zero-qty orders.
            if quantity.is_zero() {
                trace!(order_id = %order.id, "Alpaca WS: skipping notional order snapshot (qty=None)");
                return None;
            }
            let price = order
                .limit_price
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            let filled_qty =
                Decimal::from_str(order.filled_qty.unwrap_or("0")).unwrap_or(Decimal::ZERO);
            let kind = parse_order_kind(&order.order_type)?;
            let time_in_force = parse_time_in_force(&order.time_in_force);
            let time_exchange = update
                .timestamp
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);

            let open_state = Open::new(order_id, time_exchange, filled_qty);
            let order_snapshot = crate::order::Order {
                key: OrderKey::new(ExchangeId::Alpaca, instrument, StrategyId::unknown(), cid),
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state: OrderState::active(open_state),
            };
            Some(UnindexedAccountEvent::new(
                ExchangeId::Alpaca,
                AccountEventKind::OrderSnapshot(
                    barter_integration::collection::snapshot::Snapshot(order_snapshot),
                ),
            ))
        }

        "canceled" | "expired" | "replaced" | "done_for_day" => {
            // Order no longer active.
            //
            // NOTE on "replaced": Alpaca's replace operation cancels the original order
            // and creates a NEW order with a different order ID. This arm correctly marks
            // the original as cancelled, but callers must call `fetch_open_orders` to
            // discover the replacement order (which has a new ID and won't appear in OMS
            // state automatically).
            let time_exchange = update
                .timestamp
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);
            let cancelled = Cancelled::new(order_id, time_exchange);
            let response = crate::order::request::OrderResponseCancel {
                key: OrderKey::new(ExchangeId::Alpaca, instrument, StrategyId::unknown(), cid),
                state: Ok(cancelled),
            };
            Some(UnindexedAccountEvent::new(
                ExchangeId::Alpaca,
                AccountEventKind::OrderCancelled(response),
            ))
        }

        "rejected" => {
            let response = crate::order::request::OrderResponseCancel {
                key: OrderKey::new(ExchangeId::Alpaca, instrument, StrategyId::unknown(), cid),
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    format!("order rejected: status={}", order.status),
                ))),
            };
            Some(UnindexedAccountEvent::new(
                ExchangeId::Alpaca,
                AccountEventKind::OrderCancelled(response),
            ))
        }

        // All recognised event types are handled above; the early-return guard at the
        // top of this function ensures we never reach here for unknown event types.
        _ => unreachable!("convert_trade_update: unrecognised event passed early-return guard"),
    }
}

// ---------------------------------------------------------------------------
// Field parsers
// ---------------------------------------------------------------------------

fn parse_side(s: &str) -> Option<Side> {
    match s {
        "buy" | "Buy" | "BUY" => Some(Side::Buy),
        "sell" | "Sell" | "SELL" => Some(Side::Sell),
        other => {
            trace!(%other, "Alpaca: unknown order side");
            None
        }
    }
}

fn parse_order_kind(s: &str) -> Option<OrderKind> {
    match s {
        "market" | "Market" => Some(OrderKind::Market),
        "limit" | "Limit" => Some(OrderKind::Limit),
        other => {
            // stop, stop_limit, trailing_stop — not in barter's OrderKind enum.
            // Return None to skip the order; the consumer never placed these via barter.
            trace!(%other, "Alpaca: unsupported order type, skipping");
            None
        }
    }
}

fn parse_time_in_force(s: &str) -> TimeInForce {
    match s {
        "gtc" | "GTC" => TimeInForce::GoodUntilCancelled { post_only: false },
        "day" | "DAY" => TimeInForce::GoodUntilEndOfDay,
        "fok" | "FOK" => TimeInForce::FillOrKill,
        "ioc" | "IOC" => TimeInForce::ImmediateOrCancel,
        other => {
            warn!(%other, "Alpaca: unknown time_in_force, defaulting to GoodUntilEndOfDay");
            TimeInForce::GoodUntilEndOfDay
        }
    }
}

fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn map_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn map_order_kind(kind: OrderKind) -> &'static str {
    match kind {
        OrderKind::Market => "market",
        OrderKind::Limit => "limit",
    }
}

/// Map barter's `TimeInForce` to Alpaca's string representation.
///
/// # Errors
///
/// Returns `Err` if `post_only: true` is requested. Alpaca does not support
/// post-only orders — silently placing a taker-eligible GTC order would be
/// the opposite of caller intent, risking unexpected taker fees. Callers
/// requiring maker-only execution must use a different venue or strategy.
fn map_time_in_force(tif: TimeInForce) -> Result<&'static str, &'static str> {
    match tif {
        TimeInForce::GoodUntilCancelled { post_only } => {
            if post_only {
                return Err("Alpaca does not support post_only orders");
            }
            Ok("gtc")
        }
        TimeInForce::GoodUntilEndOfDay => Ok("day"),
        TimeInForce::FillOrKill => Ok("fok"),
        TimeInForce::ImmediateOrCancel => Ok("ioc"),
    }
}

/// Returns `true` if the symbol is an equity or options symbol (i.e., NOT crypto).
///
/// Crypto symbols on Alpaca always contain a forward slash (e.g., `"BTC/USD"`).
/// Equities and OCC option symbols never contain a slash. We use this to decide
/// whether to include `position_intent` in the order request: the field is valid
/// for equities and required for options, but causes a 422 on crypto orders.
///
/// NOTE: relies on Alpaca's documented symbol format (as of 2025 API). If Alpaca
/// introduces a new asset class whose symbols contain `/`, `position_intent` would
/// be silently omitted for those orders, causing 422 errors.
fn is_options_or_equity_symbol(symbol: &str) -> bool {
    !symbol.contains('/')
}

/// Derives Alpaca's `position_intent` from the generic `reduce_only` flag and `side`.
///
/// | reduce_only | side | intent       | use case                          |
/// |-------------|------|--------------|-----------------------------------|
/// | false       | Buy  | BuyToOpen    | open long / add to long position  |
/// | false       | Sell | SellToOpen   | open short / write option         |
/// | true        | Buy  | BuyToClose   | close short position              |
/// | true        | Sell | SellToClose  | close long position               |
fn map_position_intent(side: Side, reduce_only: bool) -> AlpacaPositionIntent {
    match (reduce_only, side) {
        (false, Side::Buy) => AlpacaPositionIntent::BuyToOpen,
        (false, Side::Sell) => AlpacaPositionIntent::SellToOpen,
        (true, Side::Buy) => AlpacaPositionIntent::BuyToClose,
        (true, Side::Sell) => AlpacaPositionIntent::SellToClose,
    }
}

/// Classifies an HTTP error status + body into a typed [`ApiError`].
///
/// Used by both `rest_with_retry` (for general REST calls) and `rest_delete_with_retry`
/// (for cancel operations) to ensure consistent error classification across all REST paths.
fn parse_api_error(status: reqwest::StatusCode, message: &str) -> crate::error::UnindexedApiError {
    // Fast path: 429 doesn't need message parsing.
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return ApiError::RateLimit;
    }

    // Compute lowercase once for all match guards that inspect the message body.
    let lower = message.to_ascii_lowercase();
    match status.as_u16() {
        // Match "already" before "insufficient": if Alpaca ever sends a 422 body
        // containing both substrings, this arm wins and maps to OrderAlreadyCancelled,
        // which is more specific than BalanceInsufficient.
        422 if lower.contains("already") => ApiError::OrderAlreadyCancelled,
        // Alpaca returns 422 for business-rule rejections including insufficient
        // funds. 403 is *Forbidden* — auth/permission failure — and must NOT be
        // mapped to BalanceInsufficient even if the body happens to contain the
        // substring "insufficient".
        422 if lower.contains("insufficient") => {
            ApiError::BalanceInsufficient(AssetNameExchange::new("usd"), message.to_owned())
        }
        403 => ApiError::OrderRejected(format!("forbidden (auth/permission): {message}")),
        404 => ApiError::OrderRejected(format!("order not found: {message}")),
        _ => ApiError::OrderRejected(message.to_owned()),
    }
}

/// Wraps [`parse_api_error`] for order-specific error handling (e.g., cancel_order).
fn parse_order_error(status: reqwest::StatusCode, message: &str) -> UnindexedOrderError {
    UnindexedOrderError::Rejected(parse_api_error(status, message))
}

fn connectivity_err(msg: impl Into<String>) -> UnindexedClientError {
    UnindexedClientError::Connectivity(ConnectivityError::Socket(msg.into()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpaca_config_debug_redacts_credentials() {
        let cfg = AlpacaConfig::new("my_key".into(), "my_secret".into(), true);
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("my_key"), "api_key should be redacted");
        assert!(
            !debug.contains("my_secret"),
            "secret_key should be redacted"
        );
        assert!(debug.contains("paper: true"));
    }

    #[test]
    fn test_alpaca_config_urls() {
        let paper = AlpacaConfig::new("k".into(), "s".into(), true);
        assert!(paper.rest_base_url().contains("paper-api"));
        assert!(paper.ws_url().contains("paper-api"));

        let live = AlpacaConfig::new("k".into(), "s".into(), false);
        assert!(!live.rest_base_url().contains("paper-api"));
        assert!(!live.ws_url().contains("paper-api"));
    }

    #[test]
    fn test_parse_side() {
        assert_eq!(parse_side("buy"), Some(Side::Buy));
        assert_eq!(parse_side("sell"), Some(Side::Sell));
        assert_eq!(parse_side("Buy"), Some(Side::Buy));
        assert_eq!(parse_side("BUY"), Some(Side::Buy));
        assert_eq!(parse_side("unknown"), None);
    }

    #[test]
    fn test_parse_order_kind() {
        assert_eq!(parse_order_kind("market"), Some(OrderKind::Market));
        assert_eq!(parse_order_kind("limit"), Some(OrderKind::Limit));
        assert_eq!(parse_order_kind("stop"), None);
    }

    #[test]
    fn test_map_time_in_force_roundtrip() {
        assert_eq!(
            map_time_in_force(TimeInForce::GoodUntilCancelled { post_only: false }),
            Ok("gtc")
        );
        assert_eq!(map_time_in_force(TimeInForce::GoodUntilEndOfDay), Ok("day"));
        assert_eq!(map_time_in_force(TimeInForce::FillOrKill), Ok("fok"));
        assert_eq!(map_time_in_force(TimeInForce::ImmediateOrCancel), Ok("ioc"));
    }

    #[test]
    fn test_map_time_in_force_rejects_post_only() {
        let result = map_time_in_force(TimeInForce::GoodUntilCancelled { post_only: true });
        assert!(result.is_err(), "post_only must be rejected");
        assert!(result.unwrap_err().contains("post_only"));
    }

    #[test]
    fn test_parse_timestamp_valid() {
        let ts = parse_timestamp("2025-04-18T14:30:00Z");
        assert!(ts.is_some());
        let ts2 = parse_timestamp("2025-04-18T14:30:00.123456Z");
        assert!(ts2.is_some());
        assert_eq!(parse_timestamp("not-a-timestamp"), None);
    }

    #[test]
    fn test_check_auth_response_authorized() {
        let msg =
            r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#;
        assert!(matches!(check_auth_response(msg), Some(Ok(()))));
    }

    #[test]
    fn test_check_auth_response_unauthorized() {
        let msg = r#"{"stream":"authorization","data":{"status":"unauthorized"}}"#;
        assert!(matches!(check_auth_response(msg), Some(Err(_))));
    }

    #[test]
    fn test_check_auth_response_non_auth_message() {
        let msg = r#"{"stream":"trade_updates","data":{}}"#;
        assert!(check_auth_response(msg).is_none());
    }

    #[test]
    fn test_check_listen_ack() {
        let ack = r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#;
        assert!(check_listen_ack(ack));

        let other = r#"{"stream":"authorization","data":{}}"#;
        assert!(!check_listen_ack(other));
    }

    #[test]
    fn test_dedup_cache() {
        let cache = new_dedup_cache();
        let key = SmolStr::new("order-1:1");
        assert!(
            !is_duplicate(&cache, &key),
            "first time should not be duplicate"
        );
        assert!(
            is_duplicate(&cache, &key),
            "second time should be duplicate"
        );
    }

    #[tokio::test]
    async fn test_exponential_backoff_progression_and_exhaustion() {
        tokio::time::pause();

        let mut b = ExponentialBackoff::new();

        // First wait should succeed and increment attempt.
        assert!(b.wait().await, "first wait should return true");
        assert_eq!(b.attempt, 1);

        // Drain remaining attempts.
        while b.wait().await {}

        // Attempt counter saturates at max_attempts.
        assert_eq!(b.attempt, MAX_RECONNECT_ATTEMPTS);

        // Once exhausted, wait returns false immediately without sleeping.
        assert!(!b.wait().await, "exhausted backoff should return false");

        // reset() restores attempt to 0.
        b.reset();
        assert_eq!(b.attempt, 0);

        // After reset, wait works again.
        assert!(b.wait().await, "wait should succeed after reset");
        assert_eq!(b.attempt, 1);
    }

    #[test]
    fn test_convert_account_to_balances_empty_assets() {
        let account = AlpacaAccount {
            equity: "12000.00".into(),
            buying_power: "10000.00".into(),
            options_buying_power: Some("8000.00".into()),
            crypto_buying_power: None,
        };
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances.len(), 1);
        assert_eq!(
            balances[0].balance.total,
            Decimal::from_str("12000.00").unwrap()
        );
        // options_buying_power is preferred for free
        assert_eq!(
            balances[0].balance.free,
            Decimal::from_str("8000.00").unwrap()
        );
    }

    #[test]
    fn test_convert_account_to_balances_usd_filter() {
        let account = AlpacaAccount {
            equity: "12000.00".into(),
            buying_power: "10000.00".into(),
            options_buying_power: None,
            crypto_buying_power: None,
        };
        let usd = vec![AssetNameExchange::new("USD")];
        let balances = convert_account_to_balances(&account, &usd);
        assert_eq!(balances.len(), 1);

        let non_usd = vec![AssetNameExchange::new("BTC")];
        let balances = convert_account_to_balances(&account, &non_usd);
        assert!(balances.is_empty());
    }

    #[test]
    fn test_is_options_or_equity_symbol() {
        // Crypto symbols contain '/'
        assert!(!is_options_or_equity_symbol("BTC/USD"));
        assert!(!is_options_or_equity_symbol("ETH/USD"));
        assert!(!is_options_or_equity_symbol("SOL/USD"));

        // Equity symbols — no slash
        assert!(is_options_or_equity_symbol("AAPL"));
        assert!(is_options_or_equity_symbol("SPY"));
        assert!(is_options_or_equity_symbol("MSFT"));

        // OCC option symbols — no slash
        assert!(is_options_or_equity_symbol("SPY250418C00450000"));
        assert!(is_options_or_equity_symbol("AAPL250418P00145000"));
    }

    #[test]
    fn test_parse_order_error_already_cancelled() {
        // Locks in match arm ordering: a 422 with "already" but NOT "insufficient"
        // must map to OrderAlreadyCancelled, not BalanceInsufficient.
        assert!(matches!(
            parse_order_error(
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                "order is already cancelled"
            ),
            UnindexedOrderError::Rejected(ApiError::OrderAlreadyCancelled)
        ));
    }

    #[test]
    fn test_parse_order_error_already_wins_over_insufficient_on_422() {
        // If Alpaca sends a body containing both "already" and "insufficient",
        // the "already" arm must win (it appears first in the match).
        assert!(matches!(
            parse_order_error(
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                "order already cancelled due to insufficient margin"
            ),
            UnindexedOrderError::Rejected(ApiError::OrderAlreadyCancelled)
        ));
    }

    fn make_order_ws<'a>(
        id: &str,
        symbol: &str,
        side: &str,
        filled_qty: &'a str,
    ) -> AlpacaOrderWs<'a> {
        AlpacaOrderWs {
            id: SmolStr::new(id),
            client_order_id: None,
            symbol: SmolStr::new(symbol),
            qty: Some("2"),
            filled_qty: Some(filled_qty),
            side: SmolStr::new(side),
            order_type: SmolStr::new("limit"),
            time_in_force: SmolStr::new("day"),
            limit_price: Some("100.00"),
            status: SmolStr::new("partially_filled"),
        }
    }

    #[test]
    fn test_convert_trade_update_fill_produces_trade_with_dedup_key() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            order: make_order_ws("ord-1", "SPY", "buy", "1"),
            price: Some("150.00"),
            qty: Some("1"),
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let event = convert_trade_update(update).expect("fill should produce an event");
        let AccountEventKind::Trade(trade) = event.kind else {
            panic!("expected Trade, got {:?}", event.kind);
        };
        // Trade ID must be "{order_id}:{cumulative_filled_qty}" for dedup to match REST path.
        assert_eq!(trade.id.0.as_str(), "ord-1:1");
        assert_eq!(trade.price, Decimal::from_str("150.00").unwrap());
        assert_eq!(trade.quantity, Decimal::from_str("1").unwrap());
    }

    #[test]
    fn test_convert_trade_update_partial_fill() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("partial_fill"),
            order: make_order_ws("ord-2", "AAPL", "sell", "0.5"),
            price: Some("200.00"),
            qty: Some("0.5"),
            timestamp: None,
        };
        let event = convert_trade_update(update).expect("partial_fill should produce an event");
        assert!(matches!(event.kind, AccountEventKind::Trade(_)));
    }

    #[test]
    fn test_convert_trade_update_new_order_produces_snapshot() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("new"),
            order: AlpacaOrderWs {
                id: SmolStr::new("ord-new"),
                client_order_id: Some(SmolStr::new("cid-1")),
                symbol: SmolStr::new("AAPL"),
                qty: Some("10"),
                filled_qty: Some("0"),
                side: SmolStr::new("buy"),
                order_type: SmolStr::new("limit"),
                time_in_force: SmolStr::new("day"),
                limit_price: Some("150.00"),
                status: SmolStr::new("new"),
            },
            price: None,
            qty: None,
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let event = convert_trade_update(update).expect("new event should produce an event");
        assert!(matches!(event.kind, AccountEventKind::OrderSnapshot(_)));
    }

    #[test]
    fn test_convert_trade_update_canceled_produces_cancel() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("canceled"),
            order: make_order_ws("ord-3", "AAPL", "sell", "0"),
            price: None,
            qty: None,
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let event = convert_trade_update(update).expect("canceled should produce an event");
        let AccountEventKind::OrderCancelled(response) = event.kind else {
            panic!("expected OrderCancelled, got {:?}", event.kind);
        };
        assert!(response.state.is_ok());
    }

    #[test]
    fn test_convert_trade_update_rejected_produces_error() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("rejected"),
            order: make_order_ws("ord-4", "SPY", "buy", "0"),
            price: None,
            qty: None,
            timestamp: None,
        };
        let event = convert_trade_update(update).expect("rejected should produce an event");
        let AccountEventKind::OrderCancelled(response) = event.kind else {
            panic!("expected OrderCancelled, got {:?}", event.kind);
        };
        assert!(response.state.is_err());
    }

    #[test]
    fn test_convert_open_order_notional_qty_none_is_skipped() {
        // qty=None means this is a notional order (placed by dollar value).
        // Recording it with quantity=0 would corrupt reconciliation, so it must be skipped.
        let order = AlpacaOrderResponse {
            id: "ord-notional".to_string(),
            client_order_id: None,
            symbol: "SPY".to_string(),
            qty: None,
            filled_qty: "0".to_string(),
            side: "buy".to_string(),
            order_type: "market".to_string(),
            time_in_force: "day".to_string(),
            limit_price: None,
            created_at: "2025-04-18T14:30:00Z".to_string(),
        };
        assert!(convert_open_order(&order).is_none());
    }

    #[test]
    fn test_convert_activity_to_trade_bad_price_returns_none() {
        // When price is unparseable, convert_activity_to_trade must return None.
        // recover_fills advances cumulative_qty BEFORE this check so that the dedup
        // key sequence stays aligned with the WS path even when a fill is skipped.
        let activity = AlpacaActivity {
            id: "act-1".to_string(),
            order_id: "ord-1".to_string(),
            symbol: "SPY250418C00450000".to_string(),
            side: "buy".to_string(),
            price: "not-a-number".to_string(),
            qty: "1".to_string(),
            transaction_time: "2025-04-18T14:30:00Z".to_string(),
        };
        assert!(convert_activity_to_trade(&activity).is_none());
    }

    #[test]
    fn test_convert_positions_to_balances_crypto() {
        let positions = vec![
            AlpacaPosition {
                symbol: "BTC/USD".into(),
                asset_class: "crypto".into(),
                qty: "0.5".into(),
                qty_available: "0.4".into(),
            },
            AlpacaPosition {
                symbol: "ETH/USD".into(),
                asset_class: "crypto".into(),
                qty: "2.0".into(),
                qty_available: "2.0".into(),
            },
            // Equity positions should be filtered out
            AlpacaPosition {
                symbol: "AAPL".into(),
                asset_class: "us_equity".into(),
                qty: "10".into(),
                qty_available: "10".into(),
            },
        ];

        // All crypto assets
        let balances = convert_positions_to_balances(&positions, &[]);
        assert_eq!(balances.len(), 2, "only crypto positions returned");
        assert_eq!(balances[0].asset.name().as_str(), "btc");
        // total = qty (0.5 BTC), free = qty_available (0.4 BTC)
        assert_eq!(balances[0].balance.total, Decimal::from_str("0.5").unwrap());
        assert_eq!(balances[0].balance.free, Decimal::from_str("0.4").unwrap());

        // Filter to BTC only
        let btc_only = vec![AssetNameExchange::new("BTC")];
        let balances = convert_positions_to_balances(&positions, &btc_only);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].asset.name().as_str(), "btc");
    }

    fn make_order_response(id: &str, symbol: &str) -> AlpacaOrderResponse {
        AlpacaOrderResponse {
            id: id.to_string(),
            client_order_id: None,
            symbol: symbol.to_string(),
            qty: Some("1".to_string()),
            filled_qty: "0".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            time_in_force: "day".to_string(),
            limit_price: Some("100.00".to_string()),
            created_at: "2025-04-18T14:30:00Z".to_string(),
        }
    }

    #[test]
    fn test_build_instrument_snapshots_empty_instruments_returns_only_with_orders() {
        let orders = vec![
            make_order_response("o1", "AAPL"),
            make_order_response("o2", "SPY"),
        ];
        let snapshots = build_instrument_snapshots(orders, &[]);
        assert_eq!(snapshots.len(), 2);
        let symbols: Vec<&str> = snapshots
            .iter()
            .map(|s| s.instrument.name().as_str())
            .collect();
        assert!(symbols.contains(&"AAPL"));
        assert!(symbols.contains(&"SPY"));
    }

    #[test]
    fn test_build_instrument_snapshots_requested_instrument_no_orders_gets_empty_snapshot() {
        // When instruments list is provided, every requested instrument must appear
        // even if it has no open orders — callers depend on this for reconciliation.
        let orders = vec![make_order_response("o1", "AAPL")];
        let instruments = vec![
            InstrumentNameExchange::new("AAPL"),
            InstrumentNameExchange::new("SPY"),
        ];
        let snapshots = build_instrument_snapshots(orders, &instruments);
        assert_eq!(snapshots.len(), 2);
        let spy = snapshots
            .iter()
            .find(|s| s.instrument.name().as_str() == "SPY")
            .expect("SPY snapshot must be present even with no orders");
        assert!(spy.orders.is_empty());
    }

    #[test]
    fn test_build_instrument_snapshots_non_requested_instrument_excluded() {
        // An instrument with open orders that is NOT in the requested list must
        // not appear in the output when the instruments list is non-empty.
        let orders = vec![
            make_order_response("o1", "AAPL"),
            make_order_response("o2", "MSFT"), // not requested
        ];
        let instruments = vec![InstrumentNameExchange::new("AAPL")];
        let snapshots = build_instrument_snapshots(orders, &instruments);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].instrument.name().as_str(), "AAPL");
    }

    /// Verifies that the dedup key synthesised by `recover_fills` (REST path) matches
    /// the key produced by `convert_trade_update` (WS path) for the same partial fills.
    ///
    /// This is the critical invariant for cross-source dedup after a reconnect:
    /// both paths must produce `"{order_id}:{cumulative_filled_qty}"` for the same fill.
    #[test]
    fn test_recover_fills_dedup_key_matches_ws_path() {
        let order_id = "ord-1";

        // WS path: Alpaca sends cumulative filled_qty with each event.
        // Two partial fills of 1 lot each → filled_qty "1" then "2".
        let ws_keys: Vec<SmolStr> = ["1", "2"]
            .iter()
            .filter_map(|filled_qty| {
                let update = AlpacaTradeUpdate {
                    event: SmolStr::new("partial_fill"),
                    order: make_order_ws(order_id, "SPY", "buy", filled_qty),
                    price: Some("150.00"),
                    qty: Some("1"),
                    timestamp: None,
                };
                let event = convert_trade_update(update)?;
                fill_dedup_key_from_event(&event).cloned()
            })
            .collect();

        // REST path: recover_fills accumulates cumulative qty from per-execution activities.
        // Two activities with exec qty "1" each → cumulative 1 then 2.
        let mut cumulative = Decimal::ZERO;
        let rest_keys: Vec<SmolStr> = ["1", "1"]
            .iter()
            .map(|exec_qty| {
                cumulative += Decimal::from_str(exec_qty).unwrap();
                format_smolstr!("{}:{}", order_id, cumulative.normalize())
            })
            .collect();

        assert_eq!(
            ws_keys, rest_keys,
            "REST recovery dedup keys must match WS path keys for cross-source dedup to work"
        );
        assert_eq!(ws_keys[0].as_str(), "ord-1:1");
        assert_eq!(ws_keys[1].as_str(), "ord-1:2");
    }

    /// Verifies that `early_dedup_key` produces the same key as the full event path.
    ///
    /// The early dedup check (M-1 optimization) extracts the key from raw WS fields
    /// before constructing the full event. This test ensures both paths produce
    /// identical keys, otherwise duplicate detection would fail.
    #[test]
    fn early_dedup_key_matches_full_event_path() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            order: make_order_ws("ord-abc", "SPY", "buy", "5"),
            price: Some("150.00"),
            qty: Some("5"),
            timestamp: None,
        };

        // Early path: extract key before full event construction
        let early_key = early_dedup_key(&update);

        // Full path: construct event then extract key
        let event = convert_trade_update(update).expect("fill should produce an event");
        let full_key =
            fill_dedup_key_from_event(&event).expect("fill event should have a dedup key");

        assert_eq!(
            early_key.as_str(),
            full_key.as_str(),
            "early_dedup_key must produce the same key as the full event path"
        );
        assert_eq!(early_key.as_str(), "ord-abc:5");
    }

    /// Verifies that `early_dedup_key` correctly normalizes decimal strings.
    ///
    /// Alpaca may send "1.00" or "1" for the same fill. Both must produce the same
    /// dedup key to avoid false negatives in duplicate detection.
    #[test]
    fn early_dedup_key_normalizes_decimal_strings() {
        // Test with trailing zeros: "1.00" should normalize to "1"
        let update1 = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            order: AlpacaOrderWs {
                id: SmolStr::new("ord-x"),
                client_order_id: Some(SmolStr::new("cid")),
                symbol: SmolStr::new("AAPL"),
                qty: Some("10"),
                filled_qty: Some("1.00"),
                side: SmolStr::new("buy"),
                order_type: SmolStr::new("market"),
                time_in_force: SmolStr::new("day"),
                limit_price: None,
                status: SmolStr::new("filled"),
            },
            price: Some("100.00"),
            qty: Some("10"),
            timestamp: None,
        };
        assert_eq!(early_dedup_key(&update1).as_str(), "ord-x:1");

        // Test already normalized: "1" stays "1"
        let update2 = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            order: AlpacaOrderWs {
                id: SmolStr::new("ord-x"),
                client_order_id: Some(SmolStr::new("cid")),
                symbol: SmolStr::new("AAPL"),
                qty: Some("10"),
                filled_qty: Some("1"),
                side: SmolStr::new("buy"),
                order_type: SmolStr::new("market"),
                time_in_force: SmolStr::new("day"),
                limit_price: None,
                status: SmolStr::new("filled"),
            },
            price: Some("100.00"),
            qty: Some("10"),
            timestamp: None,
        };
        assert_eq!(early_dedup_key(&update2).as_str(), "ord-x:1");

        // Test single trailing zero: "1.0" normalizes to "1"
        let update3 = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            order: AlpacaOrderWs {
                id: SmolStr::new("ord-x"),
                client_order_id: Some(SmolStr::new("cid")),
                symbol: SmolStr::new("AAPL"),
                qty: Some("10"),
                filled_qty: Some("1.0"),
                side: SmolStr::new("buy"),
                order_type: SmolStr::new("market"),
                time_in_force: SmolStr::new("day"),
                limit_price: None,
                status: SmolStr::new("filled"),
            },
            price: Some("100.00"),
            qty: Some("10"),
            timestamp: None,
        };
        assert_eq!(early_dedup_key(&update3).as_str(), "ord-x:1");
    }

    /// Regression guard for HIGH-2: a `rejected` event without `filled_qty` in the JSON
    /// previously caused the entire AlpacaTradeUpdate to fail deserialization, silently
    /// dropping the event. After the fix, `filled_qty` is Option and defaults to None
    /// (unwrapped to "0" at use-sites), so the event reaches the `rejected` branch.
    #[test]
    fn process_ws_text_rejected_event_without_filled_qty_is_not_dropped() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let dedup = new_dedup_cache();
        let mut backoff = ExponentialBackoff::new();

        // Minimal rejected-event JSON — no `filled_qty` field in the order object.
        let json = r#"{"stream":"trade_updates","data":{"event":"rejected","order":{"id":"test-rej-id","client_order_id":"test-cid","symbol":"AAPL","qty":"10","side":"buy","type":"limit","time_in_force":"day","limit_price":"100.00","status":"rejected"}}}"#;

        process_ws_text(json, &tx, &dedup, &mut backoff);

        // The event must NOT be silently dropped — an OrderCancelled must be emitted.
        let event = rx.try_recv()
            .expect("rejected event without filled_qty must produce an AccountEvent, not be silently dropped");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(_)),
            "rejected event must map to OrderCancelled, got: {:?}",
            event.kind
        );
    }

    /// Pins the string representation of `Decimal::ZERO.normalize()`, which is used
    /// as the dedup key fallback when `filled_qty` is unparseable: `"{order_id}:0"`.
    #[test]
    fn decimal_zero_normalize_is_zero_str() {
        assert_eq!(Decimal::ZERO.normalize().to_string(), "0");
    }

    /// Verifies that trailing zeros are stripped by `normalize()`, ensuring dedup keys
    /// match regardless of whether Alpaca returns `"1.00"` or `"1"` for the same qty.
    /// This is the critical invariant for REST/WS dedup key equivalence.
    #[test]
    fn decimal_normalize_strips_trailing_zeros() {
        // Alpaca may return "1.00" in REST but WS cumulative may be "1" — must match.
        let from_rest = Decimal::from_str("1.00").unwrap().normalize();
        let from_ws = Decimal::from_str("1").unwrap().normalize();
        assert_eq!(from_rest.to_string(), from_ws.to_string());
        assert_eq!(from_rest.to_string(), "1");

        // More edge cases: various trailing zero representations.
        assert_eq!(
            Decimal::from_str("100.000")
                .unwrap()
                .normalize()
                .to_string(),
            "100"
        );
        assert_eq!(
            Decimal::from_str("0.10").unwrap().normalize().to_string(),
            "0.1"
        );
        assert_eq!(
            Decimal::from_str("0.100").unwrap().normalize().to_string(),
            "0.1"
        );
    }

    // H-2: zero options_buying_power falls back to buying_power (not free=0).
    // Alpaca returns options_buying_power="0.00" on equity-only accounts (options
    // not enabled) rather than omitting the field. Without the .filter(!is_zero())
    // guard the engine would see free=0 and block all orders.
    #[test]
    fn convert_account_to_balances_zero_options_buying_power_falls_back_to_buying_power() {
        let account = AlpacaAccount {
            equity: "12000.00".into(),
            buying_power: "10000.00".into(),
            options_buying_power: Some("0.00".into()), // equity-only: options not enabled
            crypto_buying_power: None,
        };
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances.len(), 1);
        assert_eq!(
            balances[0].balance.free,
            Decimal::from_str("10000.00").unwrap(),
            "zero options_buying_power must fall back to buying_power, not report free=0"
        );
    }

    // M-2: map_position_intent derives intent from (reduce_only, side).
    #[test]
    fn map_position_intent_open_buy_maps_to_buy_to_open() {
        assert_eq!(
            map_position_intent(Side::Buy, false),
            AlpacaPositionIntent::BuyToOpen
        );
    }

    #[test]
    fn map_position_intent_open_sell_maps_to_sell_to_open() {
        assert_eq!(
            map_position_intent(Side::Sell, false),
            AlpacaPositionIntent::SellToOpen
        );
    }

    #[test]
    fn map_position_intent_reduce_buy_maps_to_buy_to_close() {
        assert_eq!(
            map_position_intent(Side::Buy, true),
            AlpacaPositionIntent::BuyToClose
        );
    }

    #[test]
    fn map_position_intent_reduce_sell_maps_to_sell_to_close() {
        assert_eq!(
            map_position_intent(Side::Sell, true),
            AlpacaPositionIntent::SellToClose
        );
    }

    // M-1: parse_order_error — pin all status-code branches not covered by existing tests.
    #[test]
    fn parse_order_error_403_with_insufficient_body_maps_to_order_rejected_not_balance() {
        // A suspended/forbidden account should NOT route to BalanceInsufficient
        // (which could trigger a balance-retry loop). Must be OrderRejected.
        assert!(matches!(
            parse_order_error(reqwest::StatusCode::FORBIDDEN, "insufficient permissions"),
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
    }

    #[test]
    fn parse_order_error_404_maps_to_order_rejected_with_not_found_prefix() {
        let err = parse_order_error(reqwest::StatusCode::NOT_FOUND, "order not found");
        let UnindexedOrderError::Rejected(ApiError::OrderRejected(msg)) = err else {
            panic!("expected OrderRejected, got {err:?}");
        };
        assert!(
            msg.contains("order not found"),
            "message should contain 'order not found': {msg}"
        );
    }

    #[test]
    fn parse_order_error_422_insufficient_only_maps_to_balance_insufficient() {
        // No "already" in body → must not match OrderAlreadyCancelled; must be BalanceInsufficient.
        assert!(matches!(
            parse_order_error(
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                "insufficient funds for this order"
            ),
            UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(_, _))
        ));
    }

    #[test]
    fn parse_order_error_429_maps_to_rate_limit() {
        assert!(matches!(
            parse_order_error(reqwest::StatusCode::TOO_MANY_REQUESTS, "rate limited"),
            UnindexedOrderError::Rejected(ApiError::RateLimit)
        ));
    }

    // M-4: parse_time_in_force — pin the unknown-value fallback to GoodUntilEndOfDay.
    // If Alpaca adds a new TIF (e.g. "opg" for at-the-open), orders continue to be
    // tracked with EOD expiry until this function is updated; the warn! makes it visible.
    #[test]
    fn parse_time_in_force_unknown_value_falls_back_to_good_until_end_of_day() {
        assert_eq!(
            parse_time_in_force("opg"),
            TimeInForce::GoodUntilEndOfDay,
            "unknown TIF must fall back to GoodUntilEndOfDay (with a warn! in production)"
        );
    }

    // L-4: build_instrument_snapshots — output order must match the instruments slice,
    // not the internal IndexMap insertion order of the orders vec.
    #[test]
    fn build_instrument_snapshots_output_order_matches_instruments_slice() {
        // Orders arrive in symbol order: SPY, AAPL, MSFT.
        let orders = vec![
            make_order_response("o1", "SPY"),
            make_order_response("o2", "AAPL"),
            make_order_response("o3", "MSFT"),
        ];
        // Request a different order: MSFT first, then AAPL.
        let instruments = vec![
            InstrumentNameExchange::new("MSFT"),
            InstrumentNameExchange::new("AAPL"),
        ];
        let snapshots = build_instrument_snapshots(orders, &instruments);
        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots[0].instrument.name().as_str(),
            "MSFT",
            "first snapshot must be MSFT (first in instruments slice)"
        );
        assert_eq!(
            snapshots[1].instrument.name().as_str(),
            "AAPL",
            "second snapshot must be AAPL (second in instruments slice)"
        );
    }

    // ---------------------------------------------------------------------------
    // HTTP-mocked tests — paginate_activities (H-1) and fetch_raw_open_orders (H-3)
    // ---------------------------------------------------------------------------
    //
    // These tests use wiremock to stand up a local HTTP server, verifying the full
    // pagination loop and truncation logic without touching the real Alpaca API.
    mod http_tests {
        use super::super::*;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

        /// Serves pre-configured JSON pages in registration order.
        ///
        /// Each call to `respond` advances an atomic counter and returns the next
        /// page body. Panics if called more times than pages were configured —
        /// surfaces unexpected extra requests as an explicit test failure rather
        /// than silently returning a stale response.
        struct Sequential {
            call: std::sync::atomic::AtomicU32,
            pages: Vec<serde_json::Value>,
        }

        impl Sequential {
            fn new(pages: Vec<serde_json::Value>) -> Self {
                Self {
                    call: std::sync::atomic::AtomicU32::new(0),
                    pages,
                }
            }
        }

        impl Respond for Sequential {
            fn respond(&self, _: &Request) -> ResponseTemplate {
                let i = self.call.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;
                let body = self.pages.get(i).unwrap_or_else(|| {
                    panic!(
                        "Sequential: request #{i} has no configured response \
                         (only {} page(s) supplied)",
                        self.pages.len()
                    )
                });
                ResponseTemplate::new(200).set_body_json(body)
            }
        }

        /// Build a JSON array of N minimal AlpacaActivity objects with unique IDs.
        fn make_activities_json(count: usize, id_prefix: &str) -> serde_json::Value {
            serde_json::Value::Array(
                (0..count)
                    .map(|i| {
                        serde_json::json!({
                            "id": format!("{id_prefix}-{i:05}"),
                            "order_id": "ord-1",
                            "symbol": "SPY",
                            "side": "buy",
                            "price": "100.00",
                            "qty": "1",
                            "transaction_time": "2025-04-18T14:30:00Z"
                        })
                    })
                    .collect(),
            )
        }

        /// Build a JSON array of N minimal AlpacaOrderResponse objects with unique IDs.
        fn make_orders_json(count: usize) -> serde_json::Value {
            serde_json::Value::Array(
                (0..count)
                    .map(|i| {
                        serde_json::json!({
                            "id": format!("order-{i:05}"),
                            "client_order_id": null,
                            "symbol": "SPY",
                            "qty": "1",
                            "filled_qty": "0",
                            "side": "buy",
                            "type": "limit",
                            "time_in_force": "day",
                            "limit_price": "100.00",
                            "created_at": "2025-04-18T14:30:00Z"
                        })
                    })
                    .collect(),
            )
        }

        // --- H-1: paginate_activities ---

        /// Single page with fewer items than the page limit — loop terminates immediately,
        /// no further request issued.
        #[tokio::test]
        async fn paginate_activities_single_page_below_max_returns_all_not_truncated() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(make_activities_json(5, "act")),
                )
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = paginate_activities(&http, &rl, &server.uri(), "2025-01-01T00:00:00Z")
                .await
                .unwrap();

            assert_eq!(result.activities.len(), 5);
            assert!(!result.truncated);
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
        }

        /// First page has exactly ALPACA_MAX_ACTIVITIES items, which triggers a second
        /// request. The second page is empty, so the loop terminates without truncation.
        #[tokio::test]
        async fn paginate_activities_exactly_page_size_items_fetches_second_page() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(Sequential::new(vec![
                    make_activities_json(ALPACA_MAX_ACTIVITIES, "act"),
                    serde_json::json!([]), // empty second page → loop stops
                ]))
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = paginate_activities(&http, &rl, &server.uri(), "2025-01-01T00:00:00Z")
                .await
                .unwrap();

            assert_eq!(result.activities.len(), ALPACA_MAX_ACTIVITIES);
            assert!(!result.truncated);
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                2,
                "exactly 2 requests: first full page + second empty page"
            );
        }

        /// Two-page case: 100 items on page 1, 37 on page 2 — all accumulated,
        /// loop terminates on the partial second page without truncation.
        #[tokio::test]
        async fn paginate_activities_two_pages_returns_combined_activities_not_truncated() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(Sequential::new(vec![
                    make_activities_json(ALPACA_MAX_ACTIVITIES, "p1"),
                    make_activities_json(37, "p2"),
                ]))
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = paginate_activities(&http, &rl, &server.uri(), "2025-01-01T00:00:00Z")
                .await
                .unwrap();

            assert_eq!(result.activities.len(), ALPACA_MAX_ACTIVITIES + 37);
            assert!(!result.truncated);
            assert_eq!(server.received_requests().await.unwrap().len(), 2);
        }

        /// When every page is full the loop runs until MAX_ACTIVITY_PAGES pages have been
        /// fetched, then sets truncated=true and stops. Exactly MAX_ACTIVITY_PAGES HTTP
        /// requests are issued (the truncation guard fires before the (N+1)th call).
        #[tokio::test]
        async fn paginate_activities_at_max_pages_sets_truncated_true() {
            let server = MockServer::start().await;

            // Always return a full page — the loop must enforce the cap itself.
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(make_activities_json(ALPACA_MAX_ACTIVITIES, "act")),
                )
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = paginate_activities(&http, &rl, &server.uri(), "2025-01-01T00:00:00Z")
                .await
                .unwrap();

            assert!(
                result.truncated,
                "must be truncated after MAX_ACTIVITY_PAGES pages"
            );
            assert_eq!(
                result.activities.len(),
                MAX_ACTIVITY_PAGES * ALPACA_MAX_ACTIVITIES,
                "must accumulate exactly MAX_ACTIVITY_PAGES * page_size activities"
            );
            // The truncation check fires at the top of the loop when pages == MAX_ACTIVITY_PAGES,
            // before the (MAX_ACTIVITY_PAGES+1)th request would be issued.
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                MAX_ACTIVITY_PAGES,
                "loop must issue exactly MAX_ACTIVITY_PAGES requests then stop"
            );
        }

        // --- H-3: fetch_raw_open_orders truncation boundary ---

        /// 499 orders (one below MAX_OPEN_ORDERS) is not truncated — returns Ok.
        #[tokio::test]
        async fn fetch_raw_open_orders_499_results_returns_ok() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(make_orders_json(MAX_OPEN_ORDERS - 1)),
                )
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = fetch_raw_open_orders(&http, &rl, &server.uri(), &[]).await;

            assert!(
                result.is_ok(),
                "499 orders must not trigger truncation: {result:?}"
            );
            assert_eq!(result.unwrap().len(), MAX_OPEN_ORDERS - 1);
        }

        /// Exactly MAX_OPEN_ORDERS results triggers TruncatedSnapshot because Alpaca's
        /// API cap means the response is likely incomplete. An off-by-one here would
        /// either silently corrupt OMS state or incorrectly reject a valid account.
        #[tokio::test]
        async fn fetch_raw_open_orders_500_results_returns_truncated_snapshot_error() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(make_orders_json(MAX_OPEN_ORDERS)),
                )
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let result = fetch_raw_open_orders(&http, &rl, &server.uri(), &[]).await;

            assert!(
                matches!(
                    result,
                    Err(UnindexedClientError::TruncatedSnapshot { limit }) if limit == MAX_OPEN_ORDERS
                ),
                "500 orders must return TruncatedSnapshot, got: {result:?}"
            );
        }

        // --- L-2: open_order passes reduce_only through to position_intent ---

        /// Verifies that `open_order` correctly derives `position_intent` from
        /// `reduce_only` and `side`. This test exercises the full path:
        /// `open_order` → `map_position_intent` → `open_order_inner` → HTTP request.
        ///
        /// Uses wiremock to capture the request body and verify position_intent.
        #[tokio::test]
        async fn open_order_reduce_only_sell_sends_sell_to_close_intent() {
            use crate::client::ExecutionClient;
            use crate::order::request::{OrderRequestOpen, RequestOpen};
            use crate::order::{
                OrderKey, OrderKind, TimeInForce,
                id::{ClientOrderId, StrategyId},
            };
            use barter_instrument::Side;
            use barter_instrument::exchange::ExchangeId;
            use barter_instrument::instrument::name::InstrumentNameExchange;
            use rust_decimal::Decimal;
            use wiremock::matchers::{method, path};

            let server = MockServer::start().await;

            // Mock POST /v2/orders to return a valid order response.
            // Use a custom responder to capture and verify the request body.
            let captured_body = std::sync::Arc::new(parking_lot::Mutex::new(None));
            let captured_clone = captured_body.clone();

            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(move |req: &Request| {
                    // Capture the request body for later assertion
                    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                    *captured_clone.lock() = Some(body);

                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "id": "test-order-id",
                        "client_order_id": "test-cid",
                        "symbol": "AAPL",
                        "qty": "10",
                        "filled_qty": "0",
                        "side": "sell",
                        "type": "market",
                        "time_in_force": "ioc",
                        "limit_price": null,
                        "created_at": "2025-04-18T14:30:00Z"
                    }))
                })
                .mount(&server)
                .await;

            // Create client with base_url_override pointing to mock server
            let config =
                AlpacaConfig::with_base_url("test-key".into(), "test-secret".into(), server.uri());
            let client = AlpacaClient::new(config);

            // Create a Sell order with reduce_only=true (should map to SellToClose)
            let request = OrderRequestOpen {
                key: OrderKey {
                    exchange: ExchangeId::Alpaca,
                    instrument: InstrumentNameExchange::new("AAPL"),
                    strategy: StrategyId::new("test-strategy"),
                    cid: ClientOrderId::new("test-cid"),
                },
                state: RequestOpen {
                    side: Side::Sell,
                    price: Decimal::ZERO,
                    quantity: Decimal::new(10, 0),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: true, // This should map to SellToClose
                },
            };

            // Call open_order (borrows instrument)
            let result = client
                .open_order(OrderRequestOpen {
                    key: OrderKey {
                        exchange: request.key.exchange,
                        instrument: &request.key.instrument,
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: request.state.clone(),
                })
                .await;

            // Verify the order was accepted
            assert!(result.is_some(), "open_order should return a result");
            let order = result.unwrap();
            assert!(
                order.state.is_ok(),
                "order should be accepted: {:?}",
                order.state
            );

            // Verify the request body contained position_intent=sell_to_close
            let body = captured_body
                .lock()
                .take()
                .expect("request body should be captured");
            assert_eq!(
                body.get("position_intent").and_then(|v| v.as_str()),
                Some("sell_to_close"),
                "reduce_only=true + Side::Sell should produce position_intent=sell_to_close, got: {body}"
            );
        }

        /// Verifies that reduce_only=false + Buy produces BuyToOpen intent.
        #[tokio::test]
        async fn open_order_not_reduce_only_buy_sends_buy_to_open_intent() {
            use crate::client::ExecutionClient;
            use crate::order::request::{OrderRequestOpen, RequestOpen};
            use crate::order::{
                OrderKey, OrderKind, TimeInForce,
                id::{ClientOrderId, StrategyId},
            };
            use barter_instrument::Side;
            use barter_instrument::exchange::ExchangeId;
            use barter_instrument::instrument::name::InstrumentNameExchange;
            use rust_decimal::Decimal;

            let server = MockServer::start().await;

            let captured_body = std::sync::Arc::new(parking_lot::Mutex::new(None));
            let captured_clone = captured_body.clone();

            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(move |req: &Request| {
                    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                    *captured_clone.lock() = Some(body);

                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "id": "test-order-id",
                        "client_order_id": "test-cid",
                        "symbol": "AAPL",
                        "qty": "10",
                        "filled_qty": "0",
                        "side": "buy",
                        "type": "market",
                        "time_in_force": "ioc",
                        "limit_price": null,
                        "created_at": "2025-04-18T14:30:00Z"
                    }))
                })
                .mount(&server)
                .await;

            let config =
                AlpacaConfig::with_base_url("test-key".into(), "test-secret".into(), server.uri());
            let client = AlpacaClient::new(config);

            let instrument = InstrumentNameExchange::new("AAPL");
            let request = OrderRequestOpen {
                key: OrderKey {
                    exchange: ExchangeId::Alpaca,
                    instrument: &instrument,
                    strategy: StrategyId::new("test-strategy"),
                    cid: ClientOrderId::new("test-cid"),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    price: Decimal::ZERO,
                    quantity: Decimal::new(10, 0),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: false, // This should map to BuyToOpen
                },
            };

            let result = client.open_order(request).await;

            assert!(result.is_some(), "open_order should return a result");
            let order = result.unwrap();
            assert!(
                order.state.is_ok(),
                "order should be accepted: {:?}",
                order.state
            );

            let body = captured_body
                .lock()
                .take()
                .expect("request body should be captured");
            assert_eq!(
                body.get("position_intent").and_then(|v| v.as_str()),
                Some("buy_to_open"),
                "reduce_only=false + Side::Buy should produce position_intent=buy_to_open, got: {body}"
            );
        }
    }
}
