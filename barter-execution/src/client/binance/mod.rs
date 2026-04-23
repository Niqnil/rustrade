// BinanceSpot ExecutionClient implementation
//
// Uses the official binance-sdk crate for Binance Spot REST + WebSocket API.
// Gated behind the "binance" feature flag.
//
// Architecture:
// - REST API (SpotRestApi) for: account_snapshot, fetch_balances, fetch_open_orders,
//   fetch_trades, open_order, cancel_order
// - WebSocket API (SpotWsApi) for: account_stream (user data stream via
//   userDataStream.subscribe.signature)
//
// Resilience features:
// - Event deduplication: LRU cache keyed on (trade_id/order_id, exec_type) prevents
//   duplicate processing after reconnect + fill recovery
// - Rate limit handling: detects HTTP 429 / Binance -1015, retries with exponential
//   backoff (Retry-After header is not accessible through the SDK's anyhow::Error
//   chain, so computed delays are used), blocks further REST calls until cooldown expires
// - Reconnection: account_stream auto-reconnects on WS disconnect/error with
//   exponential backoff (1s → 30s, max 10 attempts)
// - Heartbeat monitoring: tracks WS activity via AtomicBool flag; forces reconnect
//   if no activity (messages, ping, pong) for 30 seconds
// - Fill recovery: on reconnect, fetches missed trades via REST since disconnect
//   timestamp, sends through dedup cache to avoid duplicates
//
// Known limitations (Phase 1 scope decisions):
// - balanceUpdate events (deposits/withdrawals) are silently ignored. The crypto
//   repo wrapper should call fetch_balances or account_snapshot periodically to
//   reconcile balances after external transfers.

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
use binance_sdk::{
    common::{
        config::{ConfigurationRestApi, ConfigurationWebsocketApi},
        errors::WebsocketError,
        models::WebsocketEvent,
    },
    spot::{
        SpotRestApi, SpotWsApi,
        rest_api::{GetAccountParams, GetOpenOrdersParams, MyTradesParams, RestApi},
        websocket_api::{
            OrderCancelParams, OrderPlaceParams, OrderPlaceSideEnum, OrderPlaceTimeInForceEnum,
            OrderPlaceTypeEnum, UserDataStreamEventsResponse,
            UserDataStreamSubscribeSignatureParams, WebsocketApi, WebsocketApiHandle,
        },
    },
};
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::BoxStream;
use lru::LruCache;
use rust_decimal::Decimal;
use serde::Deserialize;
use smol_str::{SmolStr, format_smolstr};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};

// ---------------------------------------------------------------------------
// AbortOnDropStream — ensures connection_manager task is cleaned up
// ---------------------------------------------------------------------------

// AbortOnDropStream — new type not present upstream. Ensures connection_manager
// task is cancelled (at the next .await point) when the consumer drops the stream,
// preventing a background task leak and abandoned TCP connection.

/// Wrapper stream that aborts the connection_manager JoinHandle when dropped.
///
/// This ensures the channel and its associated task are cleaned up when the
/// consumer drops the account_stream. Note: `abort()` cancels at the next `.await`
/// point — if the task is mid-disconnect, the TCP connection may not close
/// gracefully. The OS will reclaim the socket via keepalive/FIN_WAIT timeout.
struct AbortOnDropStream<S> {
    inner: S,
    handle: tokio::task::JoinHandle<()>,
}

impl<S> AbortOnDropStream<S> {
    fn new(inner: S, handle: tokio::task::JoinHandle<()>) -> Self {
        Self { inner, handle }
    }
}

impl<S: futures::Stream + Unpin> futures::Stream for AbortOnDropStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S> Drop for AbortOnDropStream<S> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Initial backoff delay for reconnection attempts.
const INITIAL_BACKOFF_MS: u64 = 1_000;
/// Maximum backoff delay (cap for exponential growth).
const MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum number of consecutive reconnect attempts before giving up.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
/// If no WS activity (messages, ping, pong) for this duration, force reconnect.
const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
/// Timeout for fill recovery REST queries after reconnect.
const FILL_RECOVERY_TIMEOUT_SECS: u64 = 30;
/// Timeout for the initial WebSocket API TCP+TLS handshake.
/// Without this, a network partition holds the write lock for up to 75–127 s
/// (OS TCP timeout), stalling all concurrent open_order/cancel_order callers.
const CONNECT_TIMEOUT_SECS: u64 = 15;
/// Extra lookback subtracted from Signal disconnect timestamps to cover Tokio scheduling
/// jitter between the actual WS close and when the monitor task records Utc::now().
/// The dedup cache absorbs any resulting duplicate fills.
const SIGNAL_RECOVERY_LOOKBACK_MS: i64 = 500;
/// Size of the LRU dedup cache. 10k entries covers ~hours of high-frequency
/// trading at typical fill rates; each entry is ~84-88 bytes (DedupKey =
/// SmolStr[24] + SmolStr[24] + DedupEventKind[1] + padding[7] = 56 bytes, plus
/// LruCache node overhead: 2 linked-list pointers[16] + hashbrown slot[~12-16]
/// ≈ 28-32 bytes). At 10k: ~840-880 KB.
/// At very high fill rates (>333 distinct fills/sec sustained during the 30s
/// recovery window), LRU eviction could allow a fill to pass dedup twice.
/// Increase this constant if such volumes are expected.
const DEDUP_CACHE_SIZE: usize = 10_000;
/// Maximum trades per Binance REST query.
/// Stored as `usize` for direct use in `Vec::len()` comparisons; cast to `i32` at SDK call sites
/// (`MyTradesParams::limit(i32)`).
const BINANCE_MAX_TRADES: usize = 1000;
// Compile-time guard: SDK call sites cast this to i32; ensure it never overflows.
const _: () = assert!(
    BINANCE_MAX_TRADES <= i32::MAX as usize,
    "BINANCE_MAX_TRADES overflows i32"
);
/// Default delay when rate-limited (exponential backoff; Binance's `Retry-After`
/// header is not accessible through the SDK's `anyhow::Error` chain).
const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 10;
/// Maximum number of REST retry attempts on rate-limit errors.
const MAX_RATE_LIMIT_RETRIES: u32 = 3;

// ---------------------------------------------------------------------------
// Dedup cache
// ---------------------------------------------------------------------------

/// Event kind discriminant for dedup keys. Using an enum instead of a SmolStr
/// constant avoids constructing a string value on every event in the hot path.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
enum DedupEventKind {
    Trade,
    New,
    Cancelled,
}

/// Dedup cache key: (instrument, event ID, event kind).
///
/// Using separate `SmolStr` fields avoids the `format!("{}:{}", ...)` construction
/// and the heap allocation when a combined string would exceed SmolStr's 23-byte
/// inline limit. Both fields are already `SmolStr` values — no allocation needed.
///
/// - For TRADE events: instrument + trade_id + `DedupEventKind::Trade`
/// - For NEW events: instrument + order_id + `DedupEventKind::New`
/// - For CANCELED/EXPIRED: instrument + order_id + `DedupEventKind::Cancelled`
#[derive(Debug, Hash, Eq, PartialEq)]
struct DedupKey {
    instrument: SmolStr,
    id: SmolStr,
    kind: DedupEventKind,
}
type SharedDedupCache = Arc<parking_lot::Mutex<LruCache<DedupKey, ()>>>;

fn new_dedup_cache() -> SharedDedupCache {
    // allow(clippy::unwrap_used) — NonZeroUsize::new on a literal constant
    // cannot fail at runtime.
    #[allow(clippy::unwrap_used)]
    Arc::new(parking_lot::Mutex::new(LruCache::new(
        NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
    )))
}

/// Extract a dedup key from an account event, if applicable.
/// Returns None for events that don't need deduplication (e.g. balance snapshots).
fn dedup_key_from_event(event: &UnindexedAccountEvent) -> Option<DedupKey> {
    // Binance trade IDs and order IDs are per-symbol, NOT globally unique.
    // The instrument field prevents cross-symbol collisions during multi-symbol
    // recover_fills (buffer_unordered), where BTCUSDT trade 9001 and ETHUSDT trade
    // 9001 are distinct fills with otherwise identical IDs.
    match &event.kind {
        AccountEventKind::Trade(trade) => Some(DedupKey {
            instrument: trade.instrument.name().clone(),
            id: trade.id.0.clone(),
            kind: DedupEventKind::Trade,
        }),
        AccountEventKind::OrderSnapshot(snap) => {
            // OrderSnapshot wraps Order<..., OrderState<...>>
            // For NEW events the state is Active(Open { id, .. })
            match &snap.0.state {
                OrderState::Active(active) => {
                    // ActiveOrderState variants: OpenInFlight, Open, CancelInFlight
                    // We only get OrderSnapshot for NEW events (Open state)
                    use crate::order::state::ActiveOrderState;
                    match active {
                        ActiveOrderState::Open(open) => Some(DedupKey {
                            instrument: snap.0.key.instrument.name().clone(),
                            id: open.id.0.clone(),
                            kind: DedupEventKind::New,
                        }),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        AccountEventKind::OrderCancelled(resp) => match &resp.state {
            Ok(cancelled) => Some(DedupKey {
                instrument: resp.key.instrument.name().clone(),
                id: cancelled.id.0.clone(),
                kind: DedupEventKind::Cancelled,
            }),
            Err(_) => None, // error responses don't need dedup
        },
        _ => None, // BalanceSnapshot, Snapshot — no dedup needed
    }
}

/// Check and insert a dedup key. Returns true if the event is a duplicate.
/// Takes `key` by value to avoid cloning on the non-duplicate (common) path.
fn is_duplicate(cache: &SharedDedupCache, key: DedupKey) -> bool {
    // parking_lot::Mutex — never poisons (if a prior callback panicked,
    // the mutex auto-unlocks cleanly). Blocking in async context is acceptable here:
    // the lock is held for two hash ops on a bounded LRU cache (~microseconds), and
    // contention is minimal: the WS callback task and recover_fills may run concurrently
    // on separate Tokio worker threads; lock hold time is bounded to two hash operations
    // (~microseconds), so thread stalls are negligible. Worst-case contention:
    // recover_fills runs buffer_unordered(8) concurrently — under peak recovery load,
    // the WS callback may block for the duration of a concurrent put, but recover_fills
    // is bounded by FILL_RECOVERY_TIMEOUT_SECS and occurs only after reconnect.
    // Note: with `worker_threads = 1`, the 8 concurrent `recover_fills` tasks all block
    // on this mutex sequentially; upgrade to `tokio::sync::Mutex` if a single-worker
    // runtime is required.
    let mut guard = cache.lock();
    // peek avoids promoting the duplicate to MRU position (we're about to
    // discard it anyway), saving a linked-list move on the early-exit path.
    if guard.peek(&key).is_some() {
        return true;
    }
    guard.put(key, ());
    false
}

// ---------------------------------------------------------------------------
// Rate limit tracker
// ---------------------------------------------------------------------------

/// Tracks rate-limit state across REST API calls.
///
/// Thread-safe: inner state is behind a Mutex so clones of BinanceSpot (which
/// share the same Arc<RateLimitTracker>) all respect the same cooldown.
struct RateLimitTracker {
    /// If set, REST calls should wait until this instant before proceeding.
    // parking_lot::Mutex — never poisons, consistent with SharedDedupCache
    blocked_until: parking_lot::Mutex<Option<tokio::time::Instant>>,
}

impl RateLimitTracker {
    fn new() -> Self {
        Self {
            blocked_until: parking_lot::Mutex::new(None),
        }
    }

    /// Sleep if currently in a rate-limit cooldown. Returns immediately if not blocked.
    ///
    /// Loops after waking to re-check the deadline: another task may have called
    /// `on_rate_limited` with a longer cooldown while this task was sleeping.
    async fn wait_if_blocked(&self) {
        loop {
            let deadline = *self.blocked_until.lock();
            match deadline {
                None => return,
                Some(until) => {
                    let now = tokio::time::Instant::now();
                    if until <= now {
                        return;
                    }
                    // debug! not warn! — on_rate_limited already logs the event;
                    // multiple concurrent callers all hitting wait_if_blocked during
                    // recover_fills would otherwise flood the log with identical lines.
                    // as_millis() returns u128; truncation impossible (u64::MAX ms ≈ 584M years)
                    #[allow(clippy::cast_possible_truncation)]
                    let delay_ms = (until - now).as_millis() as u64;
                    debug!(
                        delay_ms,
                        "BinanceSpot REST rate-limited, waiting before request"
                    );
                    tokio::time::sleep_until(until).await;
                }
            }
        }
    }

    /// Record a rate-limit event. Extends the cooldown if a longer one is already active.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context (`tokio::time::Instant::now()`
    /// requires an active runtime).
    fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let delay = retry_after.unwrap_or(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));
        let new_deadline = tokio::time::Instant::now() + delay;
        let mut guard = self.blocked_until.lock();
        let was_blocked = guard.is_some();
        *guard = Some(guard.map_or(new_deadline, |existing| existing.max(new_deadline)));
        // only warn on mode entry; subsequent calls from the retry loop extend
        // the cooldown silently to avoid duplicate "entering degradation mode" lines.
        if was_blocked {
            debug!(
                delay_secs = delay.as_secs(),
                "BinanceSpot rate-limit cooldown extended"
            );
        } else {
            warn!(
                delay_secs = delay.as_secs(),
                "BinanceSpot entering rate-limit degradation mode"
            );
        }
    }

    // no clear() method — cooldowns expire naturally via wait_if_blocked().
    // A previous unconditional clear() on success raced with concurrent calls:
    // call A succeeds → clears cooldown → call B's 429 cooldown is erased.
}

/// Check if an anyhow::Error from binance-sdk is a rate-limit error.
/// Covers HTTP 429 / -1003 (WAF/queue overflow: requests rejected before execution)
/// and -1015 (IP rate-limit ban). Both warrant the same backoff response.
fn is_rate_limit_error(e: &anyhow::Error) -> bool {
    // iterate the error chain and match against the actual Display strings
    // from binance-sdk's TooManyRequestsError and RateLimitBanError variants.
    // Note: cause.to_string() allocates per chain entry — acceptable since this
    // only runs on error paths.
    for cause in e.chain() {
        let msg = cause.to_string();
        if msg.contains("Too many requests")
            || msg.contains("been banned for exceeding rate limits")
            || contains_error_code(&msg, "-1015")
            || contains_error_code(&msg, "-1003")
        {
            return true;
        }
    }
    false
}

/// Check if an anyhow::Error from binance-sdk is an API-level rejection (HTTP 4xx).
///
/// binance-sdk =44.0.1 wraps both transport failures and API rejections as
/// `WebsocketError::ResponseError`. This function distinguishes them so API rejections
/// (-2010, -1121, etc.) don't tear down a healthy WS session.
/// Re-verify on SDK upgrade — if the SDK changes error wrapping, `downcast_ref` returns
/// `None` and all rejections would be misclassified as transport errors.
fn is_api_rejection_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<WebsocketError>()
        .is_some_and(|we| matches!(we, WebsocketError::ResponseError { .. }))
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

    /// Wait for the current backoff duration. Returns `false` if max attempts exhausted.
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
            "BinanceSpot reconnect backoff"
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        true
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the BinanceSpot execution client.
// Serialize intentionally omitted — would expose secret_key in plaintext
#[derive(Clone, Deserialize)]
pub struct BinanceSpotConfig {
    // not pub — prevents accidental credential exposure via struct access.
    // Use BinanceSpotConfig::new() to construct, or deserialize from config file.
    api_key: String,
    secret_key: String,
    /// Use testnet endpoints instead of production.
    pub testnet: bool,
}

// custom Debug to avoid leaking credentials in logs
impl std::fmt::Debug for BinanceSpotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceSpotConfig")
            .field("api_key", &"***")
            .field("secret_key", &"***")
            .field("testnet", &self.testnet)
            .finish()
    }
}

impl BinanceSpotConfig {
    pub fn new(api_key: String, secret_key: String, testnet: bool) -> Self {
        Self {
            api_key,
            secret_key,
            testnet,
        }
    }

    /// Read-only access to the API key (e.g. for logging or header construction).
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

// ---------------------------------------------------------------------------
// BinanceSpot client
// ---------------------------------------------------------------------------

/// BinanceSpot execution client using the official binance-sdk.
///
/// - REST API: account snapshot, balance/order/trade queries (startup/cold paths)
/// - WebSocket API: order placement, order cancellation, user data stream (hot paths)
#[derive(Clone)]
pub struct BinanceSpot {
    config: Arc<BinanceSpotConfig>,
    rest: Arc<RestApi>,
    // Factory handle (cheap Clone) used to create WS connections.
    ws_handle: WebsocketApiHandle,
    // shared WS session for order operations (order.place, order.cancel).
    // Distinct from the account_stream WS session created in connection_manager.
    // Lazily connected on the first open_order / cancel_order call; cleared on
    // connectivity errors so the next call reconnects. All clones share the same
    // session via Arc<RwLock<...>>.
    ws_api: Arc<RwLock<Option<WebsocketApi>>>,
    // shared rate-limit tracker across all REST calls
    rate_limiter: Arc<RateLimitTracker>,
}

impl std::fmt::Debug for BinanceSpot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceSpot")
            .field("testnet", &self.config.testnet)
            .finish_non_exhaustive()
    }
}

impl BinanceSpot {
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_rest(config: &BinanceSpotConfig) -> Arc<RestApi> {
        let rest_config = ConfigurationRestApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance REST configuration");

        Arc::new(if config.testnet {
            SpotRestApi::testnet(rest_config)
        } else {
            SpotRestApi::production(rest_config)
        })
    }

    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_ws_handle(config: &BinanceSpotConfig) -> WebsocketApiHandle {
        let ws_config = ConfigurationWebsocketApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance WebSocket configuration");

        if config.testnet {
            SpotWsApi::testnet(ws_config)
        } else {
            SpotWsApi::production(ws_config)
        }
    }

    /// Returns the shared WebSocket session, connecting on the first call.
    /// If the previous session was cleared (due to a connectivity error),
    /// establishes a new connection.
    async fn get_ws_api(&self) -> anyhow::Result<WebsocketApi> {
        // Fast path: read lock to check if already connected
        {
            let guard = self.ws_api.read().await;
            if let Some(ref ws) = *guard {
                return Ok(ws.clone());
            }
        }
        // Slow path: write lock to connect.
        // The write lock is held across connect().await (TCP+TLS handshake). Concurrent
        // open_order / cancel_order callers that also reach get_ws_api will block for the
        // connection duration. The timeout below bounds the worst case to CONNECT_TIMEOUT_SECS
        // instead of the OS TCP timeout (75–127 s); on timeout, the write lock is released
        // immediately and callers receive an error.
        let mut guard = self.ws_api.write().await;
        // Double-check after acquiring write lock (another task may have connected)
        if let Some(ref ws) = *guard {
            return Ok(ws.clone());
        }
        let ws = tokio::time::timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            self.ws_handle.connect(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!("BinanceSpot WS connect timed out after {CONNECT_TIMEOUT_SECS}s")
        })??;
        *guard = Some(ws.clone());
        Ok(ws)
    }

    /// Clear the cached WS session after a connectivity error, so the next
    /// `get_ws_api()` call establishes a fresh connection.
    async fn clear_ws_api(&self) {
        // release the write lock before awaiting disconnect. Taking `ws` under
        // the lock then dropping the lock means `get_ws_api()` can establish a new
        // session concurrently while the old TCP connection is being torn down (two-
        // connection window). This is safe: each `WebsocketApi` holds fully independent
        // connection state (separate auth sessions, separate TCP sockets). No duplicate
        // events are received on both connections because the user data stream
        // subscription lives only on the connection_manager WS session, not on the
        // ws_api order session managed here.
        // Awaiting inline (rather than spawning) prevents unbounded task accumulation
        // under rapid retries during a network partition.
        let ws = {
            let mut guard = self.ws_api.write().await;
            guard.take()
        };
        if let Some(ws) = ws {
            match tokio::time::timeout(Duration::from_secs(5), ws.disconnect()).await {
                Ok(Err(e)) => warn!(%e, "BinanceSpot failed to disconnect stale WS session"),
                Err(_) => warn!("BinanceSpot WS disconnect timed out (5s)"),
                Ok(Ok(())) => {}
            }
        }
    }
}

/// Execute a REST call with rate-limit awareness and retry.
///
/// Free function so it can be used both from `BinanceSpot` methods and from
/// concurrent per-instrument futures that only have `Arc<RestApi>` + `Arc<RateLimitTracker>`.
async fn rest_call_with_retry<T>(
    rest: &Arc<RestApi>,
    rate_limiter: &RateLimitTracker,
    mut make_call: impl FnMut(
        Arc<RestApi>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<T>> + Send>,
    >,
) -> anyhow::Result<T> {
    // on the last iteration (attempt == MAX_RATE_LIMIT_RETRIES) the rate-limit
    // guard `attempt < MAX_RATE_LIMIT_RETRIES` is false, so a rate-limit error falls
    // through to the catch-all `Err(e) => return Err(e)` arm. All three match arms
    // return on the last iteration, so the post-loop unreachable!() is a runtime
    // safety net — the loop body always returns before exhaustion.
    for attempt in 0..=MAX_RATE_LIMIT_RETRIES {
        rate_limiter.wait_if_blocked().await;
        match make_call(Arc::clone(rest)).await {
            Ok(v) => return Ok(v),
            Err(e) if is_rate_limit_error(&e) && attempt < MAX_RATE_LIMIT_RETRIES => {
                // exponential delay starting at 1s (not DEFAULT_RATE_LIMIT_DELAY_SECS=10s).
                // The retry loop uses an aggressive initial delay to recover quickly from
                // transient bursts. DEFAULT_RATE_LIMIT_DELAY_SECS is for the externally-set
                // "blocked" state (e.g. Retry-After header), not for per-call retries.
                let delay = Duration::from_secs(2u64.saturating_pow(attempt).min(30));
                warn!(
                    attempt = attempt + 1,
                    max = MAX_RATE_LIMIT_RETRIES,
                    delay_secs = delay.as_secs(),
                    "BinanceSpot REST rate-limited, retrying"
                );
                rate_limiter.on_rate_limited(Some(delay));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("BinanceSpot REST retries exhausted: loop invariant violated")
}

/// Fetch open orders for a single instrument with rate-limit retry.
///
/// Returns `(instrument, orders)` so callers can associate results with their symbol.
/// Used by both `account_snapshot` (wraps in `OrderState::active()`) and
/// `fetch_open_orders` (returns `Open` directly), eliminating the duplicated
/// ~40-line REST + concurrency pattern.
async fn fetch_open_orders_for_instrument(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    instrument: InstrumentNameExchange,
) -> Result<
    (
        InstrumentNameExchange,
        Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
    ),
    UnindexedClientError,
> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    let response = rest_call_with_retry(&rest, &rate_limiter, |rest| {
        let sym = symbol_str.clone();
        Box::pin(async move {
            let params = GetOpenOrdersParams::builder().symbol(sym).build()?;
            rest.get_open_orders(params).await
        })
    })
    .await
    .map_err(connectivity_error)?;

    let orders_data = response
        .data()
        .await
        .map_err(|e| connectivity_error(e.into()))?;

    let orders = orders_data
        .into_iter()
        .filter_map(|o| convert_open_order(&o, &instrument))
        .collect();

    Ok((instrument, orders))
}

/// Paginate `GET /api/v3/myTrades` for a single instrument since `start_time_ms`.
///
/// Uses cursor-based pagination: first page queries by `start_time`; subsequent pages
/// use `from_id = last_id + 1` (Binance ignores `start_time` when `from_id` is set).
/// Trade IDs are monotonically increasing per symbol, so this produces a gapless result.
///
/// Returns raw response items. Callers decide how to handle `Err` (propagate vs. log-skip).
async fn paginate_my_trades(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    instrument: &InstrumentNameExchange,
    start_time_ms: i64,
) -> Result<Vec<binance_sdk::spot::rest_api::MyTradesResponseInner>, UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    // cursor-based pagination — first page uses start_time; subsequent pages use
    // from_id (Binance ignores start_time when from_id is set). Trade IDs are
    // monotonically increasing per symbol, so from_id = last_id + 1 continues exactly
    // where the previous page left off with no overlap or gap.
    let mut all_pages = Vec::new();
    let mut cursor: Option<i64> = None;
    loop {
        let fid = cursor; // Option<i64> is Copy
        let response = rest_call_with_retry(rest, rate_limiter, |rest| {
            let sym = symbol_str.clone();
            let stm = start_time_ms;
            Box::pin(async move {
                // const_assert! above guarantees BINANCE_MAX_TRADES fits in i32
                #[allow(clippy::cast_possible_truncation)]
                let builder = MyTradesParams::builder(sym).limit(BINANCE_MAX_TRADES as i32);
                let params = if let Some(id) = fid {
                    builder.from_id(id).build()?
                } else {
                    builder.start_time(stm).build()?
                };
                rest.my_trades(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let page = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        let page_len = page.len();
        let last_id = page.last().and_then(|t| t.id);
        all_pages.extend(page);

        if page_len < BINANCE_MAX_TRADES {
            break;
        }
        match last_id {
            Some(id) => {
                debug!(%instrument, "BinanceSpot paginate_my_trades: fetching next page ({page_len} results)");
                match id.checked_add(1) {
                    Some(next) => cursor = Some(next),
                    None => break, // saturated at i64::MAX; no further pages possible
                }
            }
            None => {
                warn!(%instrument, "BinanceSpot paginate_my_trades: trade missing ID, stopping pagination");
                break;
            }
        }
    }
    Ok(all_pages)
}

// ---------------------------------------------------------------------------
// ExecutionClient implementation
// ---------------------------------------------------------------------------

impl ExecutionClient for BinanceSpot {
    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
    type Config = BinanceSpotConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// # Panics
    ///
    /// Panics if the binance-sdk REST or WebSocket configuration builder fails
    /// (e.g. empty or malformed API key/secret). See [`BinanceSpot::build_rest`]
    /// and [`BinanceSpot::build_ws_handle`] for details.
    fn new(config: Self::Config) -> Self {
        let rest = Self::build_rest(&config);
        let ws_handle = Self::build_ws_handle(&config);
        Self {
            config: Arc::new(config),
            rest,
            ws_handle,
            ws_api: Arc::new(RwLock::new(None)),
            rate_limiter: Arc::new(RateLimitTracker::new()),
        }
    }

    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // Fetch account info via REST (with rate-limit retry)
        let response = rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            Box::pin(async move {
                let params = GetAccountParams::builder().build()?;
                rest.get_account(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let account = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        // Convert balances, filtering to requested assets
        let balances = filter_and_convert_balances(account.balances.unwrap_or_default(), assets);

        // Fetch open orders for all instruments concurrently (with retry)
        // limit concurrency to avoid bursting Binance's request weight limits
        // (each GET /api/v3/openOrders costs 3 weight; 8 concurrent = 24 weight).
        // account_snapshot wraps Open orders in OrderState::active(); fetch_open_orders
        // returns them without the wrapper — both use fetch_open_orders_for_instrument.
        use futures::{StreamExt as _, TryStreamExt};
        let instrument_snapshots: Vec<_> =
            futures::stream::iter(instruments.iter().cloned().map(|instrument| {
                fetch_open_orders_for_instrument(
                    self.rest.clone(),
                    self.rate_limiter.clone(),
                    instrument,
                )
            }))
            .buffer_unordered(8)
            .map(|result| {
                let (inst, orders) = result?;
                let wrapped = orders
                    .into_iter()
                    .map(|o| Order {
                        key: o.key,
                        side: o.side,
                        price: o.price,
                        quantity: o.quantity,
                        kind: o.kind,
                        time_in_force: o.time_in_force,
                        state: OrderState::active(o.state),
                    })
                    .collect();
                Ok::<_, UnindexedClientError>(InstrumentAccountSnapshot::new(inst, wrapped))
            })
            .try_collect()
            .await?;

        Ok(AccountSnapshot::new(
            ExchangeId::BinanceSpot,
            balances,
            instrument_snapshots,
        ))
    }

    /// Returns a live stream of account events (fills, order updates, balance changes).
    ///
    /// # Startup Race Window
    ///
    /// There is a brief window between the initial WS subscribe response and the
    /// internal event listener being registered inside the spawned connection manager.
    /// TRADE fills arriving in this gap are silently dropped. Callers that require fill
    /// completeness at startup MUST call [`ExecutionClient::fetch_trades`] with a
    /// 1–2 second lookback **unconditionally after `account_stream` returns**, before
    /// processing any events. Do not use the first event's arrival as a readiness
    /// signal — the first event may be a recovered fill sent before live WS events
    /// start. The dedup cache absorbs any duplicates from the overlapping time window.
    /// Callers MUST also call [`ExecutionClient::fetch_open_orders`] after each
    /// reconnect to reconcile open-order state — order lifecycle events (NEW, CANCELED)
    /// are not recovered after a WS disconnect, only TRADE fills are.
    async fn account_stream(
        &self,
        // _assets is intentionally ignored — Binance pushes outboundAccountPosition
        // for all account assets regardless of any filter. Client-side filtering would hide
        // balance updates for assets not in the initial list. See account_snapshot for filtering.
        _assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // Resilient account stream with auto-reconnection, heartbeat monitoring,
        // fill recovery, and event deduplication.
        //
        // Architecture: a persistent unbounded mpsc channel bridges events to the
        // consumer. A "connection manager" task owns the reconnection loop — on WS
        // disconnect or heartbeat timeout it tears down the old connection, backs off,
        // reconnects, recovers missed fills via REST, and re-subscribes. The consumer
        // sees a seamless BoxStream that only terminates when the consumer drops it or
        // max reconnect attempts are exhausted.

        // unbounded channel — memory grows if the consumer is slow, but events
        // are never silently dropped. Silent data loss (corrupted position state) is a
        // worse failure mode than observable memory pressure. The WS callback uses
        // the synchronous send() which only fails if the receiver is dropped.
        let (tx, rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();
        let dedup = new_dedup_cache();
        let ws_handle = self.ws_handle.clone();
        let rest = self.rest.clone();
        let rate_limiter = self.rate_limiter.clone();
        let instruments = instruments.to_vec();
        // all current Binance Spot symbols are ≤22 bytes (within SmolStr's 23-byte
        // inline limit), making clone() a stack memcpy with no heap allocation. Guard
        // this implicit invariant so future symbols that exceed the limit are caught early.
        debug_assert!(
            instruments.iter().all(|i| i.name().len() <= 23),
            "instrument name exceeds SmolStr inline capacity: {:?}",
            instruments.iter().find(|i| i.name().len() > 23)
        );

        // Verify initial connection succeeds before returning the stream.
        // This lets the caller distinguish "can't connect at all" from "connected
        // but later disconnected" (the latter is handled by auto-reconnect).
        let initial_ws = ws_handle.connect().await.map_err(|e| {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
        })?;

        #[allow(clippy::expect_used)] // Builder has no required fields; infallible
        let params = UserDataStreamSubscribeSignatureParams::builder()
            .build()
            .expect("UserDataStreamSubscribeSignatureParams has no required fields");

        match initial_ws
            .user_data_stream_subscribe_signature(params)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                // binance-sdk has no Drop impl for TCP close — must disconnect
                // explicitly to avoid leaking the connection on subscribe failure.
                // Awaiting inline (rather than spawning) ensures the socket is cleaned
                // up before returning Err, matching the pattern used in clear_ws_api.
                match tokio::time::timeout(Duration::from_secs(5), initial_ws.disconnect()).await {
                    Ok(Err(de)) => {
                        warn!(%de, "BinanceSpot failed to disconnect WS after subscribe failure")
                    }
                    Err(_) => {
                        warn!("BinanceSpot WS disconnect timed out (5s) after subscribe failure")
                    }
                    Ok(Ok(())) => {}
                }
                return Err(UnindexedClientError::Internal(e.to_string()));
            }
        }

        // race window — events arriving between the subscribe response above
        // and subscribe_on_ws_events() being called inside connection_manager are
        // silently dropped. The gap is bounded by Tokio scheduler latency; typically
        // milliseconds under load (no sub-millisecond guarantee).
        // account_snapshot reconciles open-order state, but TRADE fills in this window
        // are not recoverable without an explicit fetch_trades lookback. Callers that
        // require fill completeness at startup should call fetch_trades with a ~1s
        // lookback after account_stream returns.

        // Spawn the connection manager task.
        // `initial_ws` is passed in so the first iteration skips the connect step.
        let cm_handle = tokio::spawn(connection_manager(
            tx,
            dedup,
            ws_handle,
            rest,
            rate_limiter,
            instruments,
            Some(initial_ws),
        ));

        // wrap the stream to abort connection_manager on drop, ensuring the
        // WS subscription and TCP connection are cleaned up even if the consumer
        // drops the stream without waiting for graceful shutdown.
        let rx_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let guarded_stream = AbortOnDropStream::new(rx_stream, cm_handle);
        Ok(futures::StreamExt::boxed(guarded_stream))
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let instrument = request.key.instrument.clone();
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let ws = match self.get_ws_api().await {
            Ok(ws) => ws,
            Err(e) => {
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket(format!("{e:#}")),
                    )),
                });
            }
        };

        // SDK constraint — OrderCancelParams::builder takes String, not &str.
        // Allocates one String for the symbol; unavoidable without SDK changes.
        let mut params_builder =
            OrderCancelParams::builder(request.key.instrument.name().to_string());

        // Use exchange order ID if available and parseable, otherwise use client order ID
        if let Some(ref order_id) = request.state.id {
            if let Ok(id) = order_id.0.parse::<i64>() {
                params_builder = params_builder.order_id(id);
            } else {
                // exchange order ID exists but isn't a valid i64 — fall back to cid.
                // This is unexpected; Binance orderId should always be numeric.
                // error! not warn!: corrupted order state may result in cancelling the wrong
                // order (if the clientOrderId doesn't match) or a silent no-op cancel.
                error!(
                    order_id = %order_id.0,
                    "BinanceSpot cancel: exchange orderId not parseable as i64, falling back to clientOrderId"
                );
                params_builder = params_builder.orig_client_order_id(request.key.cid.0.to_string());
            }
        } else {
            params_builder = params_builder.orig_client_order_id(request.key.cid.0.to_string());
        }

        let params = match params_builder.build() {
            Ok(p) => p,
            Err(e) => {
                error!(%e, "BinanceSpot failed to build cancel order params");
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                });
            }
        };

        match ws.order_cancel(params).await {
            Ok(response) => match response.data() {
                Ok(data) => {
                    let time_exchange = data
                        .transact_time
                        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                        .unwrap_or_else(Utc::now);

                    let exchange_order_id = match data.order_id {
                        Some(id) => OrderId(format_smolstr!("{id}")),
                        None => {
                            error!("BinanceSpot cancel response missing orderId");
                            return Some(UnindexedOrderResponseCancel {
                                key,
                                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                                    "cancel response missing orderId".into(),
                                ))),
                            });
                        }
                    };

                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Ok(Cancelled::new(exchange_order_id, time_exchange)),
                    })
                }
                Err(e) => {
                    // serde_json deserialization failure on a successful response — not an API error
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                            e.to_string(),
                        ))),
                    })
                }
            },
            Err(e) => {
                // binance-sdk =44.0.1 routes both transport failures and API-level
                // rejections (status >= 400) through this outer Err path as ResponseError.
                // Distinguish them so API rejections (-2010, -1121, etc.) don't tear down
                // a healthy WS session and don't surface as ConnectivityError to the engine.
                // Check api_rejection first (zero-alloc downcast) — order rejections are the
                // common case; rate limits during placement are rare.
                if is_api_rejection_error(&e) {
                    // API-level rejection — WS session is healthy, don't tear it down
                    let api_err = parse_binance_api_error(e.to_string(), &instrument);
                    // if api_err is BalanceInsufficient, its AssetNameExchange field
                    // holds the instrument name ("BTCUSDT"), not an asset name — see
                    // parse_binance_api_error for details. Do not match on that field
                    // to identify the low-balance asset.
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Err(UnindexedOrderError::from(api_err)),
                    })
                } else if is_rate_limit_error(&e) {
                    // WS-level 429 — update the shared rate limiter so REST calls also back off.
                    self.rate_limiter.on_rate_limited(None);
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Err(UnindexedOrderError::from(ApiError::RateLimit)),
                    })
                } else {
                    // Transport-level error — clear cached session so next call reconnects.
                    // Order status is unknown (may or may not have reached the matching engine).
                    self.clear_ws_api().await;
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Err(UnindexedOrderError::Connectivity(
                            ConnectivityError::Socket(format!("{e:#}")),
                        )),
                    })
                }
            }
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let instrument = request.key.instrument.clone();
        let side = request.state.side;
        let price = request.state.price;
        let quantity = request.state.quantity;
        let kind = request.state.kind;
        let time_in_force = request.state.time_in_force;
        let cid = request.key.cid.clone();

        let order_key = OrderKey::new(
            ExchangeId::BinanceSpot,
            instrument.clone(),
            request.key.strategy.clone(),
            cid.clone(),
        );

        let ws = match self.get_ws_api().await {
            Ok(ws) => ws,
            Err(e) => {
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: Err(UnindexedOrderError::Connectivity(
                        ConnectivityError::Socket(format!("{e:#}")),
                    )),
                });
            }
        };

        let binance_side = match side {
            Side::Buy => OrderPlaceSideEnum::Buy,
            Side::Sell => OrderPlaceSideEnum::Sell,
        };

        let (binance_type, binance_tif) = convert_order_kind_tif(kind, time_in_force);

        // market BUY sends base quantity, not quoteOrderQty. Callers must
        // specify how much of the base asset they want, not how much quote to spend.
        // The trait's OrderRequestOpen has a single `quantity` field so we can't
        // distinguish — this is a known semantic difference from Binance convention.
        // SDK constraint — OrderPlaceParams::builder takes String, not &str.
        // Allocates two Strings (symbol + client_order_id); unavoidable without SDK changes.
        let mut params_builder =
            OrderPlaceParams::builder(instrument.name().to_string(), binance_side, binance_type)
                .quantity(quantity)
                .new_client_order_id(cid.0.to_string());

        // Set price for limit orders
        if matches!(kind, OrderKind::Limit) {
            params_builder = params_builder.price(price);
        }

        if let Some(tif) = binance_tif {
            params_builder = params_builder.time_in_force(tif);
        }

        let params = match params_builder.build() {
            Ok(p) => p,
            Err(e) => {
                error!(%e, "BinanceSpot failed to build new order params");
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                });
            }
        };

        match ws.order_place(params).await {
            Ok(response) => match response.data() {
                Ok(data) => {
                    let time_exchange = data
                        .transact_time
                        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                        .unwrap_or_else(Utc::now);

                    let exchange_order_id = match data.order_id {
                        Some(id) => OrderId(format_smolstr!("{id}")),
                        None => {
                            error!("BinanceSpot open_order response missing orderId");
                            return Some(Order {
                                key: order_key,
                                side,
                                price,
                                quantity,
                                kind,
                                time_in_force,
                                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                                    "open_order response missing orderId".into(),
                                ))),
                            });
                        }
                    };

                    let filled_qty = data
                        .executed_qty
                        .as_deref()
                        .and_then(|q| Decimal::from_str(q).ok())
                        .unwrap_or(Decimal::ZERO);

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
                    // serde_json deserialization failure on a successful response — not an API error
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                            e.to_string(),
                        ))),
                    })
                }
            },
            Err(e) => {
                // binance-sdk =44.0.1 routes both transport failures and API-level
                // rejections (status >= 400) through this outer Err path as ResponseError.
                // Distinguish them so API rejections (-2010, -1121, etc.) don't tear down
                // a healthy WS session and don't surface as ConnectivityError to the engine.
                // Check api_rejection first (zero-alloc downcast) — order rejections are the
                // common case; rate limits during placement are rare.
                if is_api_rejection_error(&e) {
                    // API-level rejection — WS session is healthy, don't tear it down
                    let api_err = parse_binance_api_error(e.to_string(), &instrument);
                    // if api_err is BalanceInsufficient, its AssetNameExchange field
                    // holds the instrument name ("BTCUSDT"), not an asset name — see
                    // parse_binance_api_error for details. Do not match on that field
                    // to identify the low-balance asset.
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: Err(UnindexedOrderError::from(api_err)),
                    })
                } else if is_rate_limit_error(&e) {
                    // WS-level 429 — update the shared rate limiter so REST calls also back off.
                    self.rate_limiter.on_rate_limited(None);
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: Err(UnindexedOrderError::from(ApiError::RateLimit)),
                    })
                } else {
                    // Transport-level error — clear cached session so next call reconnects.
                    // Order status is unknown (may or may not have reached the matching engine).
                    self.clear_ws_api().await;
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: Err(UnindexedOrderError::Connectivity(
                            ConnectivityError::Socket(format!("{e:#}")),
                        )),
                    })
                }
            }
        }
    }

    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let response = rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            Box::pin(async move {
                let params = GetAccountParams::builder().build()?;
                rest.get_account(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let account = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        Ok(filter_and_convert_balances(
            account.balances.unwrap_or_default(),
            assets,
        ))
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // limit concurrency to avoid bursting Binance's request weight limits
        // (each GET /api/v3/openOrders costs 3 weight; 8 concurrent = 24 weight).
        // try_fold into a flat Vec avoids the intermediate Vec<Vec<_>> that
        // try_collect().flatten() would allocate.
        use futures::{StreamExt as _, TryStreamExt as _};
        futures::stream::iter(instruments.iter().cloned().map(|instrument| {
            fetch_open_orders_for_instrument(
                self.rest.clone(),
                self.rate_limiter.clone(),
                instrument,
            )
        }))
            .buffer_unordered(8)
            .try_fold(Vec::with_capacity(instruments.len()), |mut acc: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, (_, orders)| async move {
                acc.extend(orders);
                Ok(acc)
            })
            .await
    }

    // `.iter().cloned()` is required: Rust async closures cannot satisfy the HRTB
    // `for<'a> FnMut(&'a InstrumentNameExchange) -> impl Future + 'static` needed by
    // the iterator machinery, even when the clone is moved inside the closure body.
    #[allow(clippy::redundant_iter_cloned)]
    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        use futures::StreamExt;

        if instruments.is_empty() {
            debug!(
                "BinanceSpot fetch_trades called with empty instruments slice — returning empty result"
            );
            return Ok(Vec::new());
        }
        let start_time_ms = time_since.timestamp_millis();
        // Vec::new() — capacity(instruments.len()) would be misleading since this accumulates
        // up to BINANCE_MAX_TRADES * instruments.len() trades total.
        let mut all_trades = Vec::new();

        // Binance requires per-symbol queries for trade history.
        // Limit concurrency to avoid bursting Binance's request weight limits
        // (each GET /api/v3/myTrades costs 20 weight; 8 concurrent = 160 weight).
        let mut stream = futures::stream::iter(instruments.iter().cloned().map(|inst| {
            let rest = self.rest.clone();
            let rate_limiter = self.rate_limiter.clone();
            async move {
                let pages = paginate_my_trades(&rest, &rate_limiter, &inst, start_time_ms).await?;
                Ok::<_, UnindexedClientError>((inst, pages))
            }
        }))
        .buffer_unordered(8);
        while let Some(result) = stream.next().await {
            let (instrument, trades_data) = result?;
            for t in trades_data {
                if let Some(trade) = convert_my_trade(&t, &instrument) {
                    all_trades.push(trade);
                }
            }
        }

        Ok(all_trades)
    }
}

// ---------------------------------------------------------------------------
// Connection manager (reconnection, heartbeat, fill recovery)
// ---------------------------------------------------------------------------

/// Connect to Binance WS and subscribe to the user data stream.
/// On failure, disconnects the WS to avoid leaking the TCP connection.
async fn connect_and_subscribe(ws_handle: &WebsocketApiHandle) -> anyhow::Result<WebsocketApi> {
    let ws = ws_handle.connect().await?;
    #[allow(clippy::expect_used)] // Builder has no required fields; infallible
    let params = UserDataStreamSubscribeSignatureParams::builder()
        .build()
        .expect("UserDataStreamSubscribeSignatureParams has no required fields");
    match ws.user_data_stream_subscribe_signature(params).await {
        Ok(_) => Ok(ws),
        Err(e) => {
            warn!(%e, "BinanceSpot WS subscribe failed, cleaning up connection");
            let ws_cleanup = ws;
            // fire-and-forget disconnect — the JoinHandle is intentionally
            // dropped. Unlike `clear_ws_api` (which awaits inline to prevent unbounded
            // task accumulation under rapid retries), here we're on the connect failure
            // path: at most 3 cleanup tasks may overlap during early attempts (1s, 2s,
            // 4s backoff < 5s cleanup timeout); starting at attempt 3 the backoff
            // (8s) exceeds the cleanup timeout so no further accumulation occurs.
            // Each cleanup task is bounded to 5s — acceptable accumulation.
            // If the Tokio runtime shuts down before the task completes, the task is
            // cancelled and the TCP socket is reclaimed by the OS.
            tokio::spawn(async move {
                match tokio::time::timeout(Duration::from_secs(5), ws_cleanup.disconnect()).await {
                    Ok(Err(dc_err)) => warn!(%dc_err, "BinanceSpot WS cleanup disconnect failed"),
                    Err(_) => warn!("BinanceSpot WS cleanup disconnect timed out (5s)"),
                    Ok(Ok(())) => {}
                }
            });
            Err(e)
        }
    }
}

/// Long-running task that manages the WebSocket lifecycle for account_stream.
///
/// Drives the reconnection loop: connect → subscribe → stream events → on disconnect
/// → backoff → fill recovery → reconnect. The `tx` channel persists across reconnections
/// so the consumer sees a seamless event stream.
///
/// Terminates when:
/// - The consumer drops the stream (receiver side of `tx` is closed)
/// - Max reconnect attempts are exhausted
///
/// # Panics
///
/// This function is spawned via `tokio::spawn`. If it panics, Tokio surfaces the panic
/// via the `JoinHandle`. Because the handle is transferred to `AbortOnDropStream` and
/// dropped, the panic is discarded at drop — the consumer will observe the
/// `UnboundedReceiverStream` ending (yielding `None`), indistinguishable from normal
/// max-reconnect exhaustion. The WS subscription cleanup (`subscription.unsubscribe()`)
/// at the end of the loop body will be skipped on panic. If you change this to `.await`
/// the handle, check `JoinError::is_panic()`.
// inherent complexity from reconnection loop (connect → subscribe → callback →
// fill recovery → heartbeat monitor → cleanup → backoff). Not worth splitting further.
#[allow(clippy::cognitive_complexity)]
async fn connection_manager(
    tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: SharedDedupCache,
    ws_handle: WebsocketApiHandle,
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    instruments: Vec<InstrumentNameExchange>,
    initial_ws: Option<WebsocketApi>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut disconnect_time: Option<DateTime<Utc>> = None;
    let mut current_ws = initial_ws;

    loop {
        // --- Connect (skip on first iteration if initial_ws was provided) ---
        let ws = match current_ws.take() {
            Some(ws) => ws,
            None => match connect_and_subscribe(&ws_handle).await {
                // backoff is NOT reset here — resetting on TCP-connect success
                // would lock retry intervals at INITIAL_BACKOFF_MS forever when the
                // server closes within the first heartbeat window (auth rejection,
                // server-side close). Reset is deferred to the monitor loop after the
                // first heartbeat interval survives (proven-stable connection).
                Ok(ws) => ws,
                Err(e) => {
                    error!(%e, "BinanceSpot WS connect/subscribe failed");
                    if !backoff.wait().await {
                        error!("BinanceSpot max reconnect attempts exhausted");
                        break;
                    }
                    continue;
                }
            },
        };
        info!("BinanceSpot account_stream connected and subscribed");

        // --- Set up WS event callback BEFORE fill recovery ---
        // binance-sdk silently drops WS messages with no registered subscriber.
        // Register the callback first so events arriving during fill recovery (REST)
        // are captured. The dedup cache prevents duplicates between live events and
        // recovered fills.
        let (signal_tx, signal_rx) = oneshot::channel::<()>();
        let mut signal_tx_opt = Some(signal_tx);
        // start as true — grants the connection one full heartbeat window before
        // requiring activity. A slow first ping from Binance would otherwise trigger a
        // false-positive timeout and unnecessary reconnect on the very first check.
        let heartbeat_flag = Arc::new(AtomicBool::new(true));
        let hb_callback = heartbeat_flag.clone();
        let dedup_callback = dedup.clone();
        // tx: used directly by recover_fills and the heartbeat/consumer-drop monitor.
        // event_tx: cloned into the WS callback closure (taken on first send failure or
        // stream termination so the callback becomes a no-op after disconnect).
        let mut event_tx = Some(tx.clone());
        // 32: covers typical multi-asset accounts without excessive over-allocation;
        // outboundAccountPosition emits one entry per asset, so 8 would reallocate
        // for accounts with more than 8 assets.
        let mut event_buf = Vec::with_capacity(32);

        // Safety — `signal_tx_opt.take()` and `event_tx.take()` are non-atomic,
        // which is safe because binance-sdk spawns one tokio::spawn'd task per
        // subscription; that task processes events sequentially from an internal channel,
        // so FnMut callbacks are never invoked concurrently per subscription. If the SDK
        // changes to concurrent callbacks, these `Option::take()` calls would need a Mutex.
        // verified against binance-sdk =44.0.1. Re-verify on SDK upgrade.
        let subscription = ws.subscribe_on_ws_events(move |event| {
            let Some(ref sender) = event_tx else { return };
            match event {
                WebsocketEvent::Message(json_str) => {
                    // Release: pairs with Acquire swap in monitor task so the stored
                    // `true` is visible before the monitor swaps in `false`.
                    hb_callback.store(true, Ordering::Release);
                    match serde_json::from_str::<UserDataStreamEventsResponse>(&json_str) {
                        Ok(user_event) => {
                            let stream_terminated = convert_user_data_events(user_event, &mut event_buf);
                            for ev in event_buf.drain(..) {
                                // Dedup check
                                if let Some(key) = dedup_key_from_event(&ev)
                                    && is_duplicate(&dedup_callback, key)
                                {
                                    trace!("BinanceSpot dedup: skipping duplicate event");
                                    continue;
                                }
                                if sender.send(ev).is_err() {
                                    warn!("BinanceSpot account_stream receiver dropped, suppressing further sends");
                                    event_tx.take();
                                    if let Some(s) = signal_tx_opt.take() {
                                        let _ = s.send(());
                                    }
                                    return;
                                }
                            }
                            // EventStreamTerminated arrives as a JSON message,
                            // not a WS close frame — signal reconnect explicitly.
                            if stream_terminated {
                                event_tx.take();
                                if let Some(s) = signal_tx_opt.take() {
                                    let _ = s.send(());
                                }
                            }
                        }
                        Err(e) => {
                            // Could be a subscription response, WS API metadata, or a real
                            // user-data event that failed to deserialize (SDK version skew)
                            trace!(
                                error = %e,
                                raw = %json_str.get(..200).unwrap_or(json_str.as_str()),
                                "BinanceSpot WS: skipped non-UserDataStream message"
                            );
                        }
                    }
                }
                WebsocketEvent::Ping | WebsocketEvent::Pong => {
                    // SDK handles ping/pong at protocol level; we just track activity.
                    // Release: pairs with Acquire swap in monitor task (same as Message handler).
                    hb_callback.store(true, Ordering::Release);
                }
                WebsocketEvent::Error(e) => {
                    // warn! not error! — a transient WS error that triggers
                    // auto-reconnect is recoverable. error! is reserved for failures
                    // that exhaust reconnect attempts (logged in connection_manager).
                    warn!(%e, "BinanceSpot WebSocket error, will attempt reconnect");
                    event_tx.take();
                    if let Some(s) = signal_tx_opt.take() {
                        let _ = s.send(());
                    }
                }
                WebsocketEvent::Close(code, reason) => {
                    warn!(code, %reason, "BinanceSpot WebSocket closed");
                    event_tx.take();
                    if let Some(s) = signal_tx_opt.take() {
                        let _ = s.send(());
                    }
                }
                _ => {}
            }
        });

        // --- Fill recovery after reconnect (with timeout to avoid blocking forever) ---
        // Runs after subscribe_on_ws_events so live events during recovery are captured.
        //
        // WARNING — only fills (GET /api/v3/myTrades) are recovered here.
        // Order lifecycle events (NEW, CANCELED, EXPIRED) that occurred during the
        // disconnect window are NOT recovered. Open-order state may be stale until
        // the next account_snapshot or fetch_open_orders call. The crypto repo wrapper
        // MUST call fetch_open_orders after each reconnect to reconcile open-order state.
        if let Some(dt) = disconnect_time.take() {
            match tokio::time::timeout(
                Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS),
                recover_fills(&rest, &rate_limiter, &instruments, dt, &tx, &dedup),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    // Timeout fires when REST calls are slow (rate-limited, network latency).
                    // Fills recovered so far are already in the channel; remaining instruments
                    // were not queried. The count of recovered fills (if any) is logged inside
                    // recover_fills before the timeout fires.
                    warn!(
                        timeout_secs = FILL_RECOVERY_TIMEOUT_SECS,
                        "BinanceSpot fill recovery timed out — remaining instruments not queried, some fills may be missing"
                    );
                }
            }
        }

        // --- Monitor: wait for disconnect, heartbeat timeout, or consumer drop ---
        enum DisconnectReason {
            Signal,
            HeartbeatTimeout,
            ConsumerDropped,
        }
        let reason = {
            let mut signal_rx = signal_rx;
            loop {
                tokio::select! {
                    _ = tx.closed() => {
                        debug!("BinanceSpot account_stream consumer dropped, terminating");
                        break DisconnectReason::ConsumerDropped;
                    }
                    _ = &mut signal_rx => {
                        warn!("BinanceSpot WS disconnected, will attempt reconnect");
                        break DisconnectReason::Signal;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS)) => {
                        // AcqRel on the swap: the Acquire half synchronizes with the
                        // callback's Release store of `true`, so if we read `true` we
                        // know the callback's side effects are visible. The Release half
                        // is a no-op here since no other thread reads the `false` we
                        // write back — but AcqRel is the semantically correct ordering
                        // for a read-modify-write and protects against future readers.
                        if heartbeat_flag.swap(false, Ordering::AcqRel) {
                            // Activity detected: connection is proven stable for one full
                            // heartbeat window — safe to reset reconnect backoff so a
                            // stable connection doesn't carry over prior failure counts.
                            backoff.reset();
                            continue;
                        }
                        warn!("BinanceSpot heartbeat timeout ({}s), will attempt reconnect", HEARTBEAT_TIMEOUT_SECS);
                        break DisconnectReason::HeartbeatTimeout;
                    }
                }
            }
        };
        let should_reconnect = !matches!(reason, DisconnectReason::ConsumerDropped);

        // record disconnect time BEFORE cleanup so fill recovery covers
        // the full gap. For heartbeat timeouts the connection may have died up to
        // HEARTBEAT_TIMEOUT_SECS ago, so subtract that as a safety margin.
        // For signal-based disconnects (WS close/error/stream terminated), subtract a
        // small margin to cover Tokio scheduling jitter between the WS close event and
        // the monitor task recording Utc::now(). Dedup cache handles any resulting duplicates.
        if should_reconnect {
            disconnect_time = Some(match reason {
                DisconnectReason::HeartbeatTimeout => {
                    Utc::now()
                        - chrono::Duration::seconds(HEARTBEAT_TIMEOUT_SECS as i64)
                        - chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS)
                }
                _ => Utc::now() - chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS),
            });
        }

        // --- Cleanup current connection ---
        // must explicitly unsubscribe — Subscription::drop only detaches the
        // internal JoinHandle, it doesn't abort it.
        // assumption (verified against binance-sdk =44.0.1): unsubscribe() stops
        // the callback task before returning, so no further invocations of the FnMut
        // callback occur after this point. The old `heartbeat_flag` Arc is therefore
        // safe to drop here. Re-verify on SDK upgrade if the callback invocation model changes.
        subscription.unsubscribe();
        // binance-sdk WebsocketApi has no Drop impl that closes the TCP
        // connection — must call disconnect() explicitly
        if let Err(e) = ws.disconnect().await {
            warn!(%e, "BinanceSpot failed to disconnect WebSocket");
        }

        if !should_reconnect || tx.is_closed() {
            debug!("BinanceSpot connection manager exiting");
            break;
        }
        if !backoff.wait().await {
            error!("BinanceSpot max reconnect attempts exhausted, stream terminating");
            break;
        }
    }
}

/// Recover fills missed during a WebSocket disconnection.
///
/// Fetches trades since `disconnect_time` via REST and sends them through the dedup
/// cache. Trades already seen (from before the disconnect) are filtered out; only
/// genuinely missed fills reach the consumer.
// `.iter().cloned()` is required: Rust async closures cannot satisfy the HRTB
// `for<'a> FnMut(&'a InstrumentNameExchange) -> impl Future + 'static` needed by
// the iterator machinery, even when the clone is moved inside the closure body.
#[allow(clippy::redundant_iter_cloned)]
async fn recover_fills(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    instruments: &[InstrumentNameExchange],
    disconnect_time: DateTime<Utc>,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) {
    use futures::StreamExt;

    if instruments.is_empty() {
        debug!(
            "BinanceSpot recover_fills called with empty instruments slice — no fills will be recovered"
        );
        return;
    }
    info!(
        since = %disconnect_time,
        instruments = instruments.len(),
        "BinanceSpot recovering fills after reconnect"
    );

    let start_time_ms = disconnect_time.timestamp_millis();
    let mut recovered = 0u32;
    let mut duplicates = 0u32;
    let mut failed_instruments = 0u32;

    // limit concurrency to avoid bursting Binance's request weight limits
    // (each GET /api/v3/myTrades costs 20 weight; 8 concurrent = 160 weight).
    // Returns None on per-instrument REST failure so the outer loop can count failures.
    // pagination is critical for fill recovery — missing a page means permanently
    // lost fills (no second chance after the recovery window). paginate_my_trades handles
    // the full cursor-based pagination loop shared with fetch_trades.
    let mut stream = futures::stream::iter(instruments.iter().cloned().map(|inst| {
        let rest = rest.clone();
        let rl = rate_limiter.clone();
        async move {
            let raw = match paginate_my_trades(&rest, &rl, &inst, start_time_ms).await {
                Ok(pages) => pages,
                Err(e) => {
                    warn!(%e, %inst, "BinanceSpot fill recovery: REST request failed");
                    return None;
                }
            };
            let trades: Vec<_> = raw
                .into_iter()
                .filter_map(|t| convert_my_trade(&t, &inst))
                .collect();
            Some(trades)
        }
    }))
    .buffer_unordered(8);
    while let Some(result) = stream.next().await {
        let trades = match result {
            Some(t) => t,
            None => {
                failed_instruments += 1;
                continue;
            }
        };
        for trade in trades {
            // Construct the event first so dedup_key_from_event can be reused,
            // keeping key construction in one place.
            let event =
                UnindexedAccountEvent::new(ExchangeId::BinanceSpot, AccountEventKind::Trade(trade));
            // Only Trade events are deduped during recovery — we don't recover NEW/CANCELLED
            // lifecycle events here (those require fetch_open_orders reconciliation).
            if let Some(key) = dedup_key_from_event(&event)
                && is_duplicate(dedup, key)
            {
                duplicates += 1;
                continue;
            }
            if tx.send(event).is_err() {
                // early return on consumer drop — no point recovering remaining
                // instruments if the receiver is gone. The timeout wrapper in
                // connection_manager treats this identically to normal completion.
                debug!("BinanceSpot fill recovery: consumer dropped during recovery");
                return;
            }
            recovered += 1;
        }
    }

    if failed_instruments > 0 {
        error!(
            recovered,
            duplicates,
            failed_instruments,
            "BinanceSpot fill recovery complete with failures — some fills may be permanently missed"
        );
    } else {
        info!(recovered, duplicates, "BinanceSpot fill recovery complete");
    }
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

/// Filter Binance account balances to the requested assets and convert to barter types.
///
/// If `assets` is empty, **all** balances are returned (no filter applied). This matches
/// the `ExecutionClient::account_snapshot` contract where an empty slice means "all assets."
///
/// Zero-balance entries (`free == 0` and `locked == 0`) are **intentionally included**.
/// Binance's `GET /api/v3/account` returns every asset the account has ever touched,
/// including those with zero balance. The engine or caller is responsible for filtering
/// if zero-balance assets are not desired.
fn convert_balance_entry(
    b: binance_sdk::spot::rest_api::GetAccountResponseBalancesInner,
    now: chrono::DateTime<Utc>,
) -> Option<AssetBalance<AssetNameExchange>> {
    let asset_name = AssetNameExchange::new(b.asset.as_deref()?);
    let free = match b.free.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceSpot balance missing/unparseable 'free' field");
            return None;
        }
    };
    let locked = match b.locked.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceSpot balance missing/unparseable 'locked' field");
            return None;
        }
    };
    Some(AssetBalance::new(
        asset_name,
        Balance::new(free + locked, free),
        now,
    ))
}

fn filter_and_convert_balances(
    balances: Vec<binance_sdk::spot::rest_api::GetAccountResponseBalancesInner>,
    assets: &[AssetNameExchange],
) -> Vec<AssetBalance<AssetNameExchange>> {
    let now = Utc::now();
    // Empty assets slice means "return all" — skip building the set entirely.
    if assets.is_empty() {
        return balances
            .into_iter()
            .filter_map(|b| convert_balance_entry(b, now))
            .collect();
    }
    // For small slices (≤16 assets), linear scan avoids allocation and hashing overhead.
    // For larger slices, HashSet O(1) lookup amortizes the construction cost.
    if assets.len() <= 16 {
        return balances
            .into_iter()
            .filter_map(|b| {
                let asset_name_str = b.asset.as_deref()?;
                if !assets.iter().any(|a| a.name().as_str() == asset_name_str) {
                    return None;
                }
                convert_balance_entry(b, now)
            })
            .collect();
    }
    use std::collections::HashSet;
    let asset_set: HashSet<&str> = assets.iter().map(|a| a.name().as_str()).collect();
    balances
        .into_iter()
        .filter_map(|b| {
            let asset_name_str = b.asset.as_deref()?;
            if !asset_set.contains(asset_name_str) {
                return None;
            }
            convert_balance_entry(b, now)
        })
        .collect()
}

/// Convert a Binance open order into barter's Open state order.
// `AllOrdersResponseInner` is the response type for both `GET /api/v3/allOrders`
// and `GET /api/v3/openOrders` in binance-sdk =44.0.1 — they share the same struct.
// Verified against the SDK source. Re-verify on SDK upgrade if open-orders parsing breaks.
fn convert_open_order(
    o: &binance_sdk::spot::rest_api::AllOrdersResponseInner,
    instrument: &InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let order_id_raw = match o.order_id {
        Some(id) => id,
        None => {
            warn!(%instrument, "BinanceSpot open order missing orderId");
            return None;
        }
    };
    let order_id = OrderId(format_smolstr!("{}", order_id_raw));
    if o.client_order_id.is_none() {
        warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing clientOrderId, using orderId as fallback — order may not reconcile with engine state");
    }
    let cid = ClientOrderId::new(
        o.client_order_id
            .as_deref()
            .unwrap_or(&format_smolstr!("{}", order_id_raw)),
    );
    let side = match o.side.as_deref() {
        // parse_side already logs a warning on unknown values
        Some(s) => parse_side(s)?,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing side");
            return None;
        }
    };
    let price = match o.price.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing/unparseable price");
            return None;
        }
    };
    let quantity = match o
        .orig_qty
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
    {
        Some(v) => v,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing/unparseable origQty");
            return None;
        }
    };
    let filled_qty = match o.executed_qty.as_deref() {
        Some(s) => match Decimal::from_str(s) {
            Ok(v) => v,
            Err(_) => {
                warn!(%instrument, order_id = %order_id_raw, executed_qty = s, "BinanceSpot open order unparseable executedQty, defaulting to 0");
                Decimal::ZERO
            }
        },
        None => Decimal::ZERO,
    };
    let kind = match o.r#type.as_deref() {
        // parse_order_kind already logs a warning on unknown values
        Some(t) => parse_order_kind(t)?,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing type");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(o.time_in_force.as_deref().unwrap_or("GTC"));
    let time_exchange = match o.time.and_then(|ms| Utc.timestamp_millis_opt(ms).single()) {
        Some(ts) => ts,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceSpot open order missing/unparseable time, using now");
            Utc::now()
        }
    };

    Some(Order {
        key: OrderKey::new(
            ExchangeId::BinanceSpot,
            instrument.clone(),
            // Binance doesn't carry strategy IDs in any response field.
            // Callers must reconcile orders by ClientOrderId or OrderId — never StrategyId.
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

/// Convert a Binance myTrades REST response into a barter Trade.
fn convert_my_trade(
    t: &binance_sdk::spot::rest_api::MyTradesResponseInner,
    instrument: &InstrumentNameExchange,
) -> Option<Trade<QuoteAsset, InstrumentNameExchange>> {
    let trade_id_raw = match t.id {
        Some(id) => id,
        None => {
            warn!(%instrument, "BinanceSpot trade missing id");
            return None;
        }
    };
    let trade_id = TradeId(format_smolstr!("{}", trade_id_raw));
    let order_id = match t.order_id {
        Some(id) => OrderId(format_smolstr!("{}", id)),
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceSpot trade missing orderId");
            return None;
        }
    };
    let side = match t.is_buyer {
        Some(is_buyer) => {
            if is_buyer {
                Side::Buy
            } else {
                Side::Sell
            }
        }
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceSpot trade missing isBuyer");
            return None;
        }
    };
    let price = match t.price.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceSpot trade missing/unparseable price");
            return None;
        }
    };
    let quantity = match t.qty.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceSpot trade missing/unparseable qty");
            return None;
        }
    };
    let commission = t
        .commission
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let time_exchange = match t.time.and_then(|ms| Utc.timestamp_millis_opt(ms).single()) {
        Some(ts) => ts,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceSpot trade missing/unparseable time, using now");
            Utc::now()
        }
    };

    // known limitation — the trait's Trade<QuoteAsset, _> hardcodes QuoteAsset,
    // so BNB fee-discount commission cannot be represented. BNB fees are the default
    // for active traders; debug! avoids log flooding on every REST trade.
    if let Some(ref comm_asset) = t.commission_asset
        && comm_asset == "BNB"
    {
        debug!(
            %instrument, commission_asset = %comm_asset,
            "BinanceSpot REST trade fee paid in BNB (not quote asset) — P&L commission will be misattributed"
        );
    }
    Some(Trade::new(
        trade_id,
        order_id,
        instrument.clone(),
        StrategyId::unknown(), // Binance doesn't carry strategy IDs
        time_exchange,
        side,
        price,
        quantity,
        AssetFees::quote_fees(commission),
    ))
}

/// Convert binance-sdk UserDataStreamEventsResponse to barter AccountEvents.
///
/// Pushes into the provided buffer to avoid per-message heap allocation.
/// A single Binance event (e.g., outboundAccountPosition) may map to multiple
/// barter events (one per asset balance).
///
/// Returns `true` if the stream should be considered terminated (requires reconnect).
fn convert_user_data_events(
    event: UserDataStreamEventsResponse,
    buf: &mut Vec<UnindexedAccountEvent>,
) -> bool {
    match event {
        UserDataStreamEventsResponse::ExecutionReport(report) => {
            if let Some(ev) = convert_execution_report(*report) {
                buf.push(ev);
            }
            false
        }
        UserDataStreamEventsResponse::OutboundAccountPosition(position) => {
            convert_account_position(*position, buf);
            false
        }
        UserDataStreamEventsResponse::BalanceUpdate(_update) => {
            // balanceUpdate events are for deposits/withdrawals;
            // outboundAccountPosition covers balance changes from trades.
            // deposit/withdrawal balance changes are not forwarded to the consumer.
            // The crypto repo wrapper should call fetch_balances or account_snapshot
            // periodically to reconcile balances after external transfers.
            debug!("BinanceSpot ignoring BalanceUpdate event");
            false
        }
        UserDataStreamEventsResponse::EventStreamTerminated(_) => {
            // Binance sends EventStreamTerminated as a JSON message, not a WS
            // close frame. Without signalling reconnect here, the stream silently dies
            // while heartbeat ping/pong keeps the connection alive.
            warn!("BinanceSpot user data stream terminated by exchange, signalling reconnect");
            true
        }
        _ => {
            trace!("BinanceSpot ignoring unhandled user data event");
            false
        }
    }
}

/// Convert a Binance executionReport to a barter AccountEvent.
///
/// ExecutionReport field mapping (from Binance API docs):
/// - s: symbol, c: clientOrderId, S: side, o: order type, f: time in force
/// - q: quantity, p: price, x: execution type, X: order status
/// - i: orderId, l: last filled qty, L: last filled price
/// - n: commission, N: commission asset, T: transaction time, t: tradeId
/// - z: cumulative filled qty
// inherent complexity from matching all Binance execution types (TRADE, NEW,
// CANCELED, EXPIRED, REJECTED, REPLACE) with field validation per variant.
#[allow(clippy::cognitive_complexity)]
fn convert_execution_report(
    report: binance_sdk::spot::websocket_api::ExecutionReport,
) -> Option<UnindexedAccountEvent> {
    let exec_type = match report.x.as_deref() {
        Some(t) => t,
        None => {
            warn!("BinanceSpot executionReport missing execution type (x), dropping");
            return None;
        }
    };
    let symbol = match report.s.as_deref() {
        Some(s) => InstrumentNameExchange::new(s),
        None => {
            warn!("BinanceSpot executionReport missing symbol (s), dropping");
            return None;
        }
    };
    // check order_id first — if missing we drop the event, so avoid
    // constructing cid unnecessarily.
    let order_id = match report.i {
        Some(id) => OrderId(format_smolstr!("{id}")),
        None => {
            warn!(%symbol, "BinanceSpot executionReport missing orderId (i), dropping");
            return None;
        }
    };
    let cid = match report.c.as_deref() {
        Some(c) => ClientOrderId::new(c),
        None => ClientOrderId::new(order_id.0.as_str()),
    };

    // binance-sdk renames single-letter fields: `t_uppercase` = Binance JSON field `T`
    let time_exchange = match report
        .t_uppercase
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(t) => t,
        None => {
            warn!(%symbol, "BinanceSpot executionReport missing/unparseable transaction time (T), using now");
            Utc::now()
        }
    };

    match exec_type {
        "NEW" => convert_new_order(&report, symbol, cid, order_id, time_exchange),
        "TRADE" => {
            // Partial or full fill
            let trade_id = match report.t {
                Some(id) => TradeId(format_smolstr!("{id}")),
                None => {
                    warn!(%symbol, "BinanceSpot TRADE event missing trade ID (t), dropping");
                    return None;
                }
            };
            let side = match report.s_uppercase.as_deref().and_then(parse_side) {
                Some(s) => s,
                None => {
                    warn!(%symbol, "BinanceSpot TRADE event missing/unknown side (S), dropping");
                    return None;
                }
            };
            let last_price = match report.l_uppercase.as_deref() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%symbol, error = %e, raw = s, "BinanceSpot TRADE event unparseable last price (L), dropping fill");
                        return None;
                    }
                },
                None => {
                    warn!(%symbol, "BinanceSpot TRADE event missing last price (L), dropping fill");
                    return None;
                }
            };
            let last_qty = match report.l.as_deref() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%symbol, error = %e, raw = s, "BinanceSpot TRADE event unparseable last qty (l), dropping fill");
                        return None;
                    }
                },
                None => {
                    warn!(%symbol, "BinanceSpot TRADE event missing last qty (l), dropping fill");
                    return None;
                }
            };
            // Commission parse failure: log and default to 0 rather than dropping the fill.
            let commission = match Decimal::from_str(report.n.as_deref().unwrap_or("0")) {
                Ok(v) => v,
                Err(e) => {
                    warn!(%symbol, error = %e, "BinanceSpot TRADE event unparseable commission (n), defaulting to 0");
                    Decimal::ZERO
                }
            };

            // known limitation — the trait's Trade<QuoteAsset, _> hardcodes
            // QuoteAsset, so BNB fee-discount commission cannot be represented.
            // BNB fees are the default for active traders; see module-level FORK comment.
            if let Some(ref comm_asset) = report.n_uppercase
                && comm_asset == "BNB"
            {
                debug!(
                    %symbol, commission_asset = %comm_asset,
                    "BinanceSpot WS TRADE fee paid in BNB (not quote asset) — P&L commission will be misattributed"
                );
            }
            let trade = Trade::new(
                trade_id,
                order_id,
                symbol,
                StrategyId::unknown(), // Binance doesn't carry strategy IDs
                time_exchange,
                side,
                last_price,
                last_qty,
                AssetFees::quote_fees(commission),
            );

            Some(UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::Trade(trade),
            ))
        }
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
            let cancelled = Cancelled::new(order_id, time_exchange);
            let response = UnindexedOrderResponseCancel {
                key: OrderKey::new(
                    ExchangeId::BinanceSpot,
                    symbol,
                    StrategyId::unknown(), // Binance doesn't carry strategy IDs
                    cid,
                ),
                state: Ok(cancelled),
            };

            Some(UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::OrderCancelled(response),
            ))
        }
        "REJECTED" => {
            // order rejected by the matching engine after initial acceptance
            // (e.g., insufficient funds discovered post-validation). Map to
            // OrderCancelled with an error state so the engine removes this order.
            let reject_reason = report.r.unwrap_or_else(|| "unknown".to_string());
            warn!(
                %symbol, %order_id, reason = %reject_reason,
                "BinanceSpot order REJECTED by matching engine"
            );
            let response = UnindexedOrderResponseCancel {
                key: OrderKey::new(ExchangeId::BinanceSpot, symbol, StrategyId::unknown(), cid),
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    reject_reason,
                ))),
            };

            Some(UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::OrderCancelled(response),
            ))
        }
        "REPLACE" => {
            // REPLACE is emitted when an order is replaced via the cancel-replace
            // endpoint. The execution report describes the CANCELLED original order:
            // field `i` = original order ID (already extracted as `order_id` above).
            // The replacement order arrives as a subsequent NEW execution report with
            // its own order ID. We emit OrderCancelled for the original order so the
            // engine removes it from its open-order book.
            let cancelled = Cancelled::new(order_id, time_exchange);
            let response = UnindexedOrderResponseCancel {
                key: OrderKey::new(ExchangeId::BinanceSpot, symbol, StrategyId::unknown(), cid),
                state: Ok(cancelled),
            };
            Some(UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::OrderCancelled(response),
            ))
        }
        _ => {
            // PENDING_NEW and PENDING_CANCEL are transient states; the final
            // state (NEW/CANCELED/etc.) follows shortly after.
            trace!(exec_type, "BinanceSpot ignoring execution type");
            None
        }
    }
}

/// Convert a Binance NEW execution report into an OrderSnapshot event.
fn convert_new_order(
    report: &binance_sdk::spot::websocket_api::ExecutionReport,
    symbol: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> Option<UnindexedAccountEvent> {
    let side = match report.s_uppercase.as_deref().and_then(parse_side) {
        Some(s) => s,
        None => {
            warn!(%symbol, "BinanceSpot NEW event missing/unknown side (S), dropping");
            return None;
        }
    };
    let kind = parse_order_kind(report.o.as_deref().unwrap_or("LIMIT"))?;
    let price = match (report.p.as_deref(), kind) {
        (Some(p), _) => match Decimal::from_str(p) {
            Ok(v) => v,
            Err(e) => {
                warn!(%symbol, price = p, error = %e, "BinanceSpot NEW event unparseable price (p), dropping");
                return None;
            }
        },
        (None, OrderKind::Market) => {
            warn!(%symbol, "BinanceSpot NEW market order missing price field (p), defaulting to 0");
            Decimal::ZERO
        }
        (None, OrderKind::Limit) => {
            warn!(%symbol, "BinanceSpot NEW limit order missing price (p), dropping");
            return None;
        }
    };
    let quantity = match report.q.as_deref() {
        Some(q) => match Decimal::from_str(q) {
            Ok(v) => v,
            Err(e) => {
                warn!(%symbol, qty = q, error = %e, "BinanceSpot NEW event unparseable quantity (q), dropping");
                return None;
            }
        },
        None => {
            warn!(%symbol, "BinanceSpot NEW order missing quantity (q), dropping");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(report.f.as_deref().unwrap_or("GTC"));
    // Binance field z: cumulative filled quantity; usually 0 for NEW events
    // but read it in case of immediate partial fill on aggressive orders.
    let filled_qty = report
        .z
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    let order = Order {
        key: OrderKey::new(
            ExchangeId::BinanceSpot,
            symbol,
            StrategyId::unknown(), // Binance doesn't carry strategy IDs
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: OrderState::active(Open::new(order_id, time_exchange, filled_qty)),
    };

    Some(UnindexedAccountEvent::new(
        ExchangeId::BinanceSpot,
        AccountEventKind::OrderSnapshot(barter_integration::collection::snapshot::Snapshot::new(
            order,
        )),
    ))
}

/// Convert a Binance outboundAccountPosition to balance snapshot events.
///
/// Emits one BalanceSnapshot event per asset since `Snapshot<AssetBalance>` wraps
/// a single balance. Pushes into the provided buffer to avoid per-message allocation.
fn convert_account_position(
    position: binance_sdk::spot::websocket_api::OutboundAccountPosition,
    buf: &mut Vec<UnindexedAccountEvent>,
) {
    // Use field `u` (last account update time) rather than `E` (event time)
    // for more accurate balance timestamps
    let time_exchange = position
        .u
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .unwrap_or_else(Utc::now);

    for b in position.b_uppercase.unwrap_or_default() {
        let asset = match b.a {
            Some(a) => AssetNameExchange::new(a),
            None => {
                warn!("BinanceSpot account position entry missing asset name");
                continue;
            }
        };
        let free = match b.f.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
            Some(v) => v,
            None => {
                warn!(%asset, "BinanceSpot account position missing/unparseable 'free' field");
                continue;
            }
        };
        let locked = match b.l.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
            Some(v) => v,
            None => {
                warn!(%asset, "BinanceSpot account position missing/unparseable 'locked' field");
                continue;
            }
        };
        let balance = AssetBalance::new(asset, Balance::new(free + locked, free), time_exchange);
        buf.push(UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::BalanceSnapshot(
                barter_integration::collection::snapshot::Snapshot::new(balance),
            ),
        ));
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

fn parse_side(s: &str) -> Option<Side> {
    match s {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => {
            warn!(side = s, "unknown Binance order side");
            None
        }
    }
}

fn parse_order_kind(t: &str) -> Option<OrderKind> {
    match t {
        "MARKET" => Some(OrderKind::Market),
        // STOP_LOSS and TAKE_PROFIT are conditional orders that Binance
        // triggers at a stop price. Barter's OrderKind has no conditional variant.
        // Drop them: mapping to Market would misrepresent conditional orders as
        // immediately executable in snapshots, which is more dangerous than omitting.
        "STOP_LOSS" | "TAKE_PROFIT" => {
            warn!(
                order_type = t,
                "dropping conditional Binance order type (no OrderKind equivalent)"
            );
            None
        }
        // STOP_LOSS_LIMIT and TAKE_PROFIT_LIMIT are conditional orders
        // with a limit price that enter the book only after a stop trigger.
        // Mapping to Limit is imprecise (treats them as resting limit orders)
        // but preserves visibility into open orders. Dropping them (like the
        // pure stop variants above) would lose order tracking entirely.
        // Acceptable for Phase 1: the crypto repo wrapper doesn't place stop orders.
        "LIMIT" | "LIMIT_MAKER" | "STOP_LOSS_LIMIT" | "TAKE_PROFIT_LIMIT" => Some(OrderKind::Limit),
        _ => {
            warn!(order_type = t, "unsupported Binance order type");
            None
        }
    }
}

fn parse_time_in_force(tif: &str) -> TimeInForce {
    match tif {
        "GTC" => TimeInForce::GoodUntilCancelled { post_only: false },
        "GTX" => TimeInForce::GoodUntilCancelled { post_only: true },
        "IOC" => TimeInForce::ImmediateOrCancel,
        "FOK" => TimeInForce::FillOrKill,
        "GTD" => TimeInForce::GoodUntilEndOfDay,
        _ => {
            warn!(
                time_in_force = tif,
                "unknown Binance TimeInForce, defaulting to GTC"
            );
            TimeInForce::GoodUntilCancelled { post_only: false }
        }
    }
}

fn convert_order_kind_tif(
    kind: OrderKind,
    tif: TimeInForce,
) -> (OrderPlaceTypeEnum, Option<OrderPlaceTimeInForceEnum>) {
    match kind {
        OrderKind::Market => (OrderPlaceTypeEnum::Market, None),
        OrderKind::Limit => match tif {
            TimeInForce::GoodUntilCancelled { post_only: false } => (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Gtc),
            ),
            TimeInForce::GoodUntilCancelled { post_only: true } => {
                // LIMIT_MAKER is Binance's post-only order type (rejects if
                // it would immediately match as taker)
                (OrderPlaceTypeEnum::LimitMaker, None)
            }
            TimeInForce::FillOrKill => (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Fok),
            ),
            TimeInForce::ImmediateOrCancel => (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Ioc),
            ),
            TimeInForce::GoodUntilEndOfDay => {
                warn!("Binance Spot does not support GTD; coercing to GTC");
                (
                    OrderPlaceTypeEnum::Limit,
                    Some(OrderPlaceTimeInForceEnum::Gtc),
                )
            }
        },
    }
}

/// Returns true if `msg` contains `code` as a standalone numeric token:
/// not immediately preceded or followed by another ASCII digit.
/// Prevents "-2013" from matching "-20130" (suffix guard) or "1-2013" (prefix guard).
///
/// Iterates all occurrences so that if the first match fails a digit-guard check
/// (e.g. `-2013` found inside `-20130`), a later valid occurrence is not missed.
fn contains_error_code(msg: &str, code: &str) -> bool {
    let code_len = code.len();
    let mut start = 0;
    while let Some(rel) = msg[start..].find(code) {
        let pos = start + rel;
        let prefix_ok = pos == 0 || !msg[..pos].ends_with(|c: char| c.is_ascii_digit());
        let suffix_ok = !msg[pos + code_len..].starts_with(|c: char| c.is_ascii_digit());
        if prefix_ok && suffix_ok {
            return true;
        }
        start = pos + 1;
    }
    false
}

/// Parse Binance error strings to barter ApiError.
///
/// depends on binance-sdk's internal error formatting (not a public API contract).
/// If the SDK changes its error message format, these string matches may silently stop working.
/// Matches numeric error codes first (stable), then falls back to message text heuristics.
fn parse_binance_api_error(
    error_msg: String,
    instrument: &InstrumentNameExchange,
) -> ApiError<AssetNameExchange, InstrumentNameExchange> {
    // Match on Binance error codes first — these are stable numeric identifiers
    if contains_error_code(&error_msg, "-1003") || contains_error_code(&error_msg, "-1015") {
        // -1003: too many requests; -1015: IP rate-limit ban. Both are transient throttles.
        return ApiError::RateLimit;
    }
    if contains_error_code(&error_msg, "-2011") {
        // -2011: "Unknown order sent" — typically means already cancelled/filled
        return ApiError::OrderAlreadyCancelled;
    }
    if contains_error_code(&error_msg, "-2013") {
        // -2013: "Order does not exist" — on cancel attempts this almost always means
        // the order was already filled or cancelled (a normal race condition).
        return ApiError::OrderAlreadyCancelled;
    }
    if contains_error_code(&error_msg, "-1121") {
        return ApiError::InstrumentInvalid(instrument.clone(), error_msg);
    }
    if contains_error_code(&error_msg, "-2010") {
        // -2010: "Account has insufficient balance for requested action"
        // same limitation as text heuristic below — the AssetNameExchange field
        // holds the instrument name, not an asset name. See parse_binance_api_error.
        return ApiError::BalanceInsufficient(
            AssetNameExchange::new(instrument.name().as_str()),
            error_msg,
        );
    }

    // Fall back to case-insensitive message text heuristics (avoid to_lowercase allocation)
    fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
        haystack
            .as_bytes()
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
    }
    if contains_ignore_case(&error_msg, "insufficient")
        || contains_ignore_case(&error_msg, "not enough")
    {
        // the AssetNameExchange field here holds the *instrument* name (e.g.
        // "BTCUSDT"), NOT an actual asset name ("BTC" or "USDT"). Splitting the pair
        // into base/quote is unreliable without exchange symbol-info metadata.
        // WARNING: do NOT pattern-match on the AssetNameExchange value to identify
        // a specific asset — use the error_msg string for diagnostics only.
        ApiError::BalanceInsufficient(
            AssetNameExchange::new(instrument.name().as_str()),
            error_msg,
        )
    } else if contains_ignore_case(&error_msg, "rate limit") {
        ApiError::RateLimit
    } else if contains_ignore_case(&error_msg, "unknown order") {
        // -2011/-2013 map to OrderAlreadyCancelled via numeric code above.
        // The text-only fallback here (no code present) maps "unknown order" to
        // OrderRejected — intentionally asymmetric. If the SDK strips error codes,
        // the same semantic maps to a different variant. Acceptable: numeric codes
        // are always present in practice; the text path is a defensive last resort.
        ApiError::OrderRejected(error_msg)
    } else if contains_ignore_case(&error_msg, "invalid symbol") {
        ApiError::InstrumentInvalid(instrument.clone(), error_msg)
    } else {
        ApiError::OrderRejected(error_msg)
    }
}

fn connectivity_error(e: anyhow::Error) -> UnindexedClientError {
    // use alternate display {:#} to preserve the full anyhow error chain.
    // Known limitation: all SDK errors (auth failures, HTTP 5xx, malformed responses,
    // network errors) are uniformly classified as ConnectivityError::Socket. The SDK
    // uses anyhow::Error, making it impractical to distinguish error categories without
    // parsing the string — callers cannot distinguish "network unavailable" from
    // "auth rejected" without inspecting the error message.
    UnindexedClientError::Connectivity(ConnectivityError::Socket(format!("{e:#}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_parse_side() {
        assert_eq!(parse_side("BUY"), Some(Side::Buy));
        assert_eq!(parse_side("SELL"), Some(Side::Sell));
        assert_eq!(parse_side("buy"), None);
        assert_eq!(parse_side("UNKNOWN"), None);
    }

    #[test]
    fn test_parse_order_kind() {
        assert_eq!(parse_order_kind("MARKET"), Some(OrderKind::Market));
        assert_eq!(parse_order_kind("LIMIT"), Some(OrderKind::Limit));
        assert_eq!(parse_order_kind("LIMIT_MAKER"), Some(OrderKind::Limit));
        assert_eq!(parse_order_kind("STOP_LOSS"), None);
        assert_eq!(parse_order_kind("TAKE_PROFIT"), None);
        assert_eq!(parse_order_kind("STOP_LOSS_LIMIT"), Some(OrderKind::Limit));
        assert_eq!(
            parse_order_kind("TAKE_PROFIT_LIMIT"),
            Some(OrderKind::Limit)
        );
        assert_eq!(parse_order_kind("UNKNOWN_TYPE"), None);
    }

    #[test]
    fn test_parse_time_in_force() {
        assert_eq!(
            parse_time_in_force("GTC"),
            TimeInForce::GoodUntilCancelled { post_only: false }
        );
        assert_eq!(
            parse_time_in_force("GTX"),
            TimeInForce::GoodUntilCancelled { post_only: true }
        );
        assert_eq!(parse_time_in_force("IOC"), TimeInForce::ImmediateOrCancel);
        assert_eq!(parse_time_in_force("FOK"), TimeInForce::FillOrKill);
        assert_eq!(parse_time_in_force("GTD"), TimeInForce::GoodUntilEndOfDay);
        // Unknown defaults to GTC
        assert_eq!(
            parse_time_in_force("UNKNOWN"),
            TimeInForce::GoodUntilCancelled { post_only: false }
        );
    }

    #[test]
    fn test_convert_order_kind_tif() {
        // binance-sdk enums don't derive PartialEq, so use matches!
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Market, TimeInForce::ImmediateOrCancel),
            (OrderPlaceTypeEnum::Market, None)
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Gtc)
            )
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: true }
            ),
            (OrderPlaceTypeEnum::LimitMaker, None)
        ));
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Limit, TimeInForce::FillOrKill),
            (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Fok)
            )
        ));
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Limit, TimeInForce::ImmediateOrCancel),
            (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Ioc)
            )
        ));
        // GoodUntilEndOfDay coerces to GTC on Binance Spot
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Limit, TimeInForce::GoodUntilEndOfDay),
            (
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Gtc)
            )
        ));
    }

    #[test]
    fn test_parse_binance_api_error() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        assert!(matches!(
            parse_binance_api_error("Insufficient balance".into(), &instrument),
            ApiError::BalanceInsufficient(_, _)
        ));
        assert!(matches!(
            parse_binance_api_error("Not enough funds".into(), &instrument),
            ApiError::BalanceInsufficient(_, _)
        ));
        assert_eq!(
            parse_binance_api_error("Rate limit exceeded".into(), &instrument),
            ApiError::RateLimit
        );
        assert_eq!(
            parse_binance_api_error("Error code -1015".into(), &instrument),
            ApiError::RateLimit
        );
        // -1003: too many requests — must also map to RateLimit (not OrderRejected)
        assert_eq!(
            parse_binance_api_error("Error -1003: too many requests".into(), &instrument),
            ApiError::RateLimit
        );
        // -2011 maps to OrderAlreadyCancelled
        assert_eq!(
            parse_binance_api_error("Error code -2011".into(), &instrument),
            ApiError::OrderAlreadyCancelled
        );
        // -2013 maps to OrderAlreadyCancelled (benign race condition on cancel)
        assert!(matches!(
            parse_binance_api_error("Unknown order sent -2013".into(), &instrument),
            ApiError::OrderAlreadyCancelled
        ));
        // -2013 without "unknown order" text still matches via code
        assert!(matches!(
            parse_binance_api_error("Order does not exist -2013".into(), &instrument),
            ApiError::OrderAlreadyCancelled
        ));
        // "Unknown order" text without a code falls through to text heuristic
        assert!(matches!(
            parse_binance_api_error("Unknown order encountered".into(), &instrument),
            ApiError::OrderRejected(_)
        ));
        assert!(matches!(
            parse_binance_api_error("Invalid symbol -1121".into(), &instrument),
            ApiError::InstrumentInvalid(_, _)
        ));
        // -2010 matched by numeric code (not text heuristic)
        assert!(matches!(
            parse_binance_api_error(
                "Server-side response error (code -2010): Account has insufficient balance".into(),
                &instrument
            ),
            ApiError::BalanceInsufficient(_, _)
        ));
        assert!(matches!(
            parse_binance_api_error("Some other error".into(), &instrument),
            ApiError::OrderRejected(_)
        ));
    }

    #[test]
    fn test_contains_error_code_suffix_guard() {
        // Suffix digit guard: "-2013" must not match "-20130" or "-20131"
        assert!(
            !contains_error_code("-20130", "-2013"),
            "-20130 should not match -2013"
        );
        assert!(
            !contains_error_code("-20131", "-2013"),
            "-20131 should not match -2013"
        );
        // Exact match and match with trailing text should succeed
        assert!(contains_error_code("-2013", "-2013"), "exact match");
        assert!(
            contains_error_code("Error -2013: text", "-2013"),
            "match with trailing text"
        );
    }

    #[test]
    fn test_contains_error_code_prefix_guard() {
        // Prefix digit guard: "-2013" must not match a string where the code
        // is immediately preceded by a digit (e.g. "1-2013" in some error context).
        assert!(
            !contains_error_code("1-2013", "-2013"),
            "1-2013 should not match -2013"
        );
        assert!(
            !contains_error_code("error 1-2013 text", "-2013"),
            "embedded 1-2013 should not match"
        );
        // Non-digit prefix should still match
        assert!(
            contains_error_code("code=-2013,", "-2013"),
            "=-2013 prefix should match"
        );
        assert!(
            contains_error_code(" -2013 ", "-2013"),
            "space prefix should match"
        );
    }

    #[test]
    fn test_contains_error_code_second_occurrence_valid() {
        // When the first occurrence fails the suffix digit guard (e.g. "-2013" inside
        // "-20130"), the function must continue scanning and find the later valid occurrence.
        assert!(
            contains_error_code("response code -20130 or -2013: rate limit", "-2013"),
            "second valid occurrence must match when first fails digit guard"
        );
        // Symmetric: first valid, second is a longer code — first must still match
        assert!(
            contains_error_code("-2013 or -20130", "-2013"),
            "first valid occurrence must match when second is a longer code"
        );
    }

    #[test]
    fn test_dedup_cache() {
        let cache = new_dedup_cache();
        let key = DedupKey {
            instrument: SmolStr::from("BTCUSDT"),
            id: SmolStr::from("12345"),
            kind: DedupEventKind::Trade,
        };

        // First time: not a duplicate
        assert!(!is_duplicate(&cache, key));
        // Second time: is a duplicate
        let key = DedupKey {
            instrument: SmolStr::from("BTCUSDT"),
            id: SmolStr::from("12345"),
            kind: DedupEventKind::Trade,
        };
        assert!(is_duplicate(&cache, key));

        // Different key (same id, different kind): not a duplicate
        let key2 = DedupKey {
            instrument: SmolStr::from("BTCUSDT"),
            id: SmolStr::from("12345"),
            kind: DedupEventKind::New,
        };
        assert!(!is_duplicate(&cache, key2));
    }

    #[test]
    fn test_is_rate_limit_error() {
        // Matches actual binance-sdk TooManyRequestsError Display output
        assert!(is_rate_limit_error(&anyhow::anyhow!(
            "Too many requests. You are being rate-limited. Please slow down."
        )));
        // Matches actual binance-sdk RateLimitBanError Display output
        assert!(is_rate_limit_error(&anyhow::anyhow!(
            "The IP address has been banned for exceeding rate limits. Contact support."
        )));
        // Binance error codes in the msg body
        assert!(is_rate_limit_error(&anyhow::anyhow!(
            "Error -1015: too many new orders"
        )));
        assert!(is_rate_limit_error(&anyhow::anyhow!(
            "Error -1003: too many requests"
        )));
        // Non-rate-limit errors
        assert!(!is_rate_limit_error(&anyhow::anyhow!("order 4290 failed")));
        assert!(!is_rate_limit_error(&anyhow::anyhow!("connection timeout")));
        assert!(!is_rate_limit_error(&anyhow::anyhow!("unknown error")));
        // Digit-boundary false-positive guard: longer codes must NOT match
        assert!(
            !is_rate_limit_error(&anyhow::anyhow!("Error -10150: some other error")),
            "-10150 should not match -1015"
        );
        assert!(
            !is_rate_limit_error(&anyhow::anyhow!("Error -10030: some other error")),
            "-10030 should not match -1003"
        );
    }

    #[test]
    fn test_is_api_rejection_error() {
        // A ResponseError from binance-sdk (HTTP 4xx API rejection) is detected
        let rejection = anyhow::anyhow!(WebsocketError::ResponseError {
            code: -2010,
            message: "Account has insufficient balance for requested action.".into(),
        });
        assert!(
            is_api_rejection_error(&rejection),
            "ResponseError should be detected as API rejection"
        );

        // A transport error (e.g. connection reset) is NOT an API rejection
        let transport = anyhow::anyhow!("connection reset by peer");
        assert!(
            !is_api_rejection_error(&transport),
            "plain transport error should not be detected as API rejection"
        );

        // A rate-limit error string (not a WebsocketError) is NOT an API rejection
        let rate_limit = anyhow::anyhow!("Too many requests. You are being rate-limited.");
        assert!(
            !is_api_rejection_error(&rate_limit),
            "rate-limit string error should not be detected as API rejection"
        );
    }

    #[test]
    fn test_dedup_key_from_event_trade_includes_instrument() {
        use crate::order::id::{OrderId, StrategyId};
        use crate::trade::{AssetFees, Trade, TradeId};
        use barter_instrument::Side;
        use barter_instrument::asset::QuoteAsset;
        use chrono::Utc;
        use rust_decimal::Decimal;

        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let trade = Trade::<QuoteAsset, InstrumentNameExchange>::new(
            TradeId::new("9001"),
            OrderId::new("4242"),
            instrument.clone(),
            StrategyId::unknown(),
            Utc::now(),
            Side::Buy,
            Decimal::ZERO,
            Decimal::ZERO,
            AssetFees::quote_fees(Decimal::ZERO),
        );
        let event =
            UnindexedAccountEvent::new(ExchangeId::BinanceSpot, AccountEventKind::Trade(trade));

        let key = dedup_key_from_event(&event).expect("Trade should produce a DedupKey");
        assert_eq!(key.kind, DedupEventKind::Trade);
        assert_eq!(key.id.as_str(), "9001", "key.id should be the trade ID");
        assert_eq!(
            key.instrument.as_str(),
            "BTCUSDT",
            "key.instrument should be the symbol"
        );

        // Same trade ID on a different instrument must produce a different key (cross-symbol collision prevention)
        let instrument2 = InstrumentNameExchange::new("ETHUSDT");
        let trade2 = Trade::<QuoteAsset, InstrumentNameExchange>::new(
            TradeId::new("9001"),
            OrderId::new("7777"),
            instrument2,
            StrategyId::unknown(),
            Utc::now(),
            Side::Buy,
            Decimal::ZERO,
            Decimal::ZERO,
            AssetFees::quote_fees(Decimal::ZERO),
        );
        let event2 =
            UnindexedAccountEvent::new(ExchangeId::BinanceSpot, AccountEventKind::Trade(trade2));
        let key2 = dedup_key_from_event(&event2).expect("Trade should produce a DedupKey");
        assert_ne!(
            key, key2,
            "same trade ID on different symbols must produce distinct keys"
        );
    }

    // ---------------------------------------------------------------------------
    // convert_account_position tests
    // ---------------------------------------------------------------------------

    fn make_balance_inner(
        asset: &str,
        free: &str,
        locked: &str,
    ) -> binance_sdk::spot::websocket_api::OutboundAccountPositionBInner {
        binance_sdk::spot::websocket_api::OutboundAccountPositionBInner {
            a: Some(asset.to_string()),
            f: Some(free.to_string()),
            l: Some(locked.to_string()),
        }
    }

    #[test]
    fn test_convert_account_position_happy_path() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: Some(vec![make_balance_inner("BTC", "1.5", "0.5")]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);

        assert_eq!(buf.len(), 1);
        match &buf[0].kind {
            AccountEventKind::BalanceSnapshot(snap) => {
                let balance = &snap.0;
                assert_eq!(balance.asset.as_ref(), "BTC");
                // total = free + locked = 1.5 + 0.5 = 2.0; free = 1.5
                let expected_total = Decimal::from_str("2.0").unwrap();
                let expected_free = Decimal::from_str("1.5").unwrap();
                assert_eq!(balance.balance.total, expected_total);
                assert_eq!(balance.balance.free, expected_free);
            }
            other => panic!("expected BalanceSnapshot, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_account_position_u_field_none_uses_now() {
        // When `u` is None the function falls back to Utc::now() — just verify it doesn't panic
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: None,
            b_uppercase: Some(vec![make_balance_inner("ETH", "2.0", "0.0")]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn test_convert_account_position_missing_asset_name_skipped() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: Some(vec![
                binance_sdk::spot::websocket_api::OutboundAccountPositionBInner {
                    a: None, // missing asset name
                    f: Some("1.0".to_string()),
                    l: Some("0.0".to_string()),
                },
                make_balance_inner("USDT", "100.0", "0.0"), // valid
            ]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);
        // The entry with missing asset name is skipped; USDT entry is kept
        assert_eq!(buf.len(), 1);
        match &buf[0].kind {
            AccountEventKind::BalanceSnapshot(snap) => {
                assert_eq!(snap.0.asset.as_ref(), "USDT");
            }
            other => panic!("expected BalanceSnapshot, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_account_position_unparseable_free_skipped() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: Some(vec![
                binance_sdk::spot::websocket_api::OutboundAccountPositionBInner {
                    a: Some("BTC".to_string()),
                    f: Some("not-a-number".to_string()),
                    l: Some("0.0".to_string()),
                },
                make_balance_inner("ETH", "1.0", "0.0"), // valid
            ]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);
        // BTC skipped due to unparseable free; ETH kept
        assert_eq!(buf.len(), 1);
        match &buf[0].kind {
            AccountEventKind::BalanceSnapshot(snap) => {
                assert_eq!(snap.0.asset.as_ref(), "ETH");
            }
            other => panic!("expected BalanceSnapshot, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_account_position_empty_balances() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: Some(vec![]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);
        assert!(
            buf.is_empty(),
            "empty balance list should produce no events"
        );
    }

    #[test]
    fn test_convert_account_position_b_field_none() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: None, // no B field at all
            ..Default::default()
        };
        let mut buf = Vec::new();
        convert_account_position(position, &mut buf);
        assert!(buf.is_empty(), "None B field should produce no events");
    }

    // Behavioral test — verify wait() exhaustion and reset
    #[tokio::test]
    async fn test_exponential_backoff_exhaustion() {
        tokio::time::pause(); // auto-advances to next timer when all tasks are waiting
        let mut backoff = ExponentialBackoff::new();

        // All MAX_RECONNECT_ATTEMPTS calls should return true
        for i in 0..MAX_RECONNECT_ATTEMPTS {
            assert!(
                backoff.wait().await,
                "expected true on attempt {i} (before exhaustion)"
            );
        }

        // The next call must return false (exhausted)
        assert!(!backoff.wait().await, "expected false after max attempts");
    }

    #[tokio::test]
    async fn test_exponential_backoff_reset() {
        tokio::time::pause();
        let mut backoff = ExponentialBackoff::new();

        for _ in 0..MAX_RECONNECT_ATTEMPTS {
            backoff.wait().await;
        }
        assert!(!backoff.wait().await, "should be exhausted");

        backoff.reset();
        assert!(backoff.wait().await, "should succeed again after reset");
    }

    // 7b: convert_execution_report round-trip tests
    // ExecutionReport derives Default with all-Option fields, making it easy to
    // construct targeted test cases.
    fn make_base_report() -> binance_sdk::spot::websocket_api::ExecutionReport {
        binance_sdk::spot::websocket_api::ExecutionReport {
            s: Some("BTCUSDT".to_string()),
            i: Some(12345),
            c: Some("client-1".to_string()),
            t_uppercase: Some(1_700_000_000_000),
            ..Default::default()
        }
    }

    #[test]
    fn test_convert_execution_report_new() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("NEW".to_string()),
            s_uppercase: Some("BUY".to_string()),
            o: Some("LIMIT".to_string()),
            p: Some("50000.00".to_string()),
            q: Some("0.01".to_string()),
            f: Some("GTC".to_string()),
            z: Some("0".to_string()),
            ..make_base_report()
        };

        let event = convert_execution_report(report).expect("NEW event should produce Some");
        assert_eq!(event.exchange, ExchangeId::BinanceSpot);
        match &event.kind {
            AccountEventKind::OrderSnapshot(snap) => {
                let order = &snap.0;
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.kind, OrderKind::Limit);
                assert_eq!(order.price, Decimal::from_str("50000.00").unwrap());
                assert_eq!(order.quantity, Decimal::from_str("0.01").unwrap());
                assert_eq!(order.key.cid.0.as_str(), "client-1");
                assert_eq!(order.key.instrument.name().as_str(), "BTCUSDT");
            }
            other => panic!("NEW should yield OrderSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_convert_execution_report_trade() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            s_uppercase: Some("BUY".to_string()),
            t: Some(9999),
            l_uppercase: Some("50000.00".to_string()),
            l: Some("0.01".to_string()),
            n: Some("0.000001".to_string()),
            ..make_base_report()
        };

        let event = convert_execution_report(report).expect("TRADE event should produce Some");
        assert_eq!(event.exchange, ExchangeId::BinanceSpot);
        match &event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.side, Side::Buy);
                assert_eq!(trade.price, Decimal::from_str("50000.00").unwrap());
                assert_eq!(trade.quantity, Decimal::from_str("0.01").unwrap());
                assert_eq!(trade.id.0.as_str(), "9999");
                assert_eq!(trade.order_id.0.as_str(), "12345");
                assert_eq!(trade.instrument.name().as_str(), "BTCUSDT");
            }
            other => panic!("TRADE should yield Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_convert_execution_report_trade_missing_last_price() {
        // l_uppercase (L = last filled price) missing: must drop the fill
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            s_uppercase: Some("BUY".to_string()),
            t: Some(9999),
            l_uppercase: None, // missing L field
            l: Some("0.01".to_string()),
            ..make_base_report()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "missing last price (L) should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_trade_missing_last_qty() {
        // l (last filled qty) missing: must drop the fill
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            s_uppercase: Some("BUY".to_string()),
            t: Some(9999),
            l_uppercase: Some("50000.00".to_string()),
            l: None, // missing l field
            ..make_base_report()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "missing last qty (l) should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_canceled() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("CANCELED".to_string()),
            ..make_base_report()
        };

        let event = convert_execution_report(report).expect("CANCELED should produce Some");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(ref r) if r.state.is_ok()),
            "CANCELED should yield OrderCancelled with Ok state"
        );
    }

    #[test]
    fn test_convert_execution_report_expired() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("EXPIRED".to_string()),
            ..make_base_report()
        };
        let event = convert_execution_report(report).expect("EXPIRED should produce Some");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(ref r) if r.state.is_ok()),
            "EXPIRED should yield OrderCancelled with Ok state"
        );
    }

    #[test]
    fn test_convert_execution_report_expired_in_match() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("EXPIRED_IN_MATCH".to_string()),
            ..make_base_report()
        };
        let event = convert_execution_report(report).expect("EXPIRED_IN_MATCH should produce Some");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(ref r) if r.state.is_ok()),
            "EXPIRED_IN_MATCH should yield OrderCancelled with Ok state"
        );
    }

    #[test]
    fn test_convert_execution_report_rejected() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("REJECTED".to_string()),
            r: Some("INSUFFICIENT_FUNDS".to_string()),
            ..make_base_report()
        };

        let event = convert_execution_report(report).expect("REJECTED should produce Some");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(ref r) if r.state.is_err()),
            "REJECTED should yield OrderCancelled with Err state"
        );
    }

    #[test]
    fn test_convert_execution_report_missing_exec_type() {
        // Missing x field: must drop the event
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            s: Some("BTCUSDT".to_string()),
            ..Default::default()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "missing execution type should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_missing_symbol() {
        // Missing s field with valid x: must drop the event
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("NEW".to_string()),
            ..Default::default()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "missing symbol should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_missing_order_id() {
        // Missing i field (orderId): shared early-exit path for all exec types
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("NEW".to_string()),
            s: Some("BTCUSDT".to_string()),
            i: None,
            ..Default::default()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "missing orderId should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_trade_missing_trade_id() {
        // Missing t field (tradeId) on a TRADE event: must drop the fill
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            t: None,
            ..make_base_report()
        };
        assert!(
            convert_execution_report(report).is_none(),
            "TRADE event missing tradeId should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_replace_yields_cancelled() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("REPLACE".to_string()),
            ..make_base_report()
        };
        // REPLACE describes the cancelled original order (field `i` = original order ID).
        // The replacement order arrives as a subsequent NEW execution report.
        let event = convert_execution_report(report).expect("REPLACE should produce Some");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(ref r) if r.state.is_ok()),
            "REPLACE should yield OrderCancelled with Ok state"
        );
    }

    // ---------------------------------------------------------------------------
    // filter_and_convert_balances tests
    // ---------------------------------------------------------------------------

    fn make_balance(
        asset: &str,
        free: &str,
        locked: &str,
    ) -> binance_sdk::spot::rest_api::GetAccountResponseBalancesInner {
        binance_sdk::spot::rest_api::GetAccountResponseBalancesInner {
            asset: Some(asset.to_string()),
            free: Some(free.to_string()),
            locked: Some(locked.to_string()),
        }
    }

    #[test]
    fn test_filter_balances_empty_assets_returns_all() {
        let balances = vec![
            make_balance("BTC", "1.0", "0.0"),
            make_balance("USDT", "500.0", "50.0"),
        ];
        let result = filter_and_convert_balances(balances, &[]);
        assert_eq!(result.len(), 2, "empty filter should return all balances");
    }

    #[test]
    fn test_filter_balances_matching_asset_returned() {
        let balances = vec![
            make_balance("BTC", "1.5", "0.5"),
            make_balance("ETH", "10.0", "0.0"),
        ];
        let assets = vec![AssetNameExchange::new("BTC")];
        let result = filter_and_convert_balances(balances, &assets);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].asset, AssetNameExchange::new("BTC"));
        // total = free + locked = 1.5 + 0.5
        assert_eq!(result[0].balance.total, Decimal::from_str("2.0").unwrap());
        assert_eq!(result[0].balance.free, Decimal::from_str("1.5").unwrap());
    }

    #[test]
    fn test_filter_balances_non_matching_asset_filtered_out() {
        let balances = vec![
            make_balance("BTC", "1.0", "0.0"),
            make_balance("ETH", "2.0", "0.0"),
        ];
        let assets = vec![AssetNameExchange::new("USDT")];
        let result = filter_and_convert_balances(balances, &assets);
        assert!(
            result.is_empty(),
            "non-matching asset should be filtered out"
        );
    }

    #[test]
    fn test_filter_balances_missing_asset_field_skipped() {
        let balances = vec![
            binance_sdk::spot::rest_api::GetAccountResponseBalancesInner {
                asset: None,
                free: Some("1.0".to_string()),
                locked: Some("0.0".to_string()),
            },
            make_balance("USDT", "100.0", "0.0"),
        ];
        let result = filter_and_convert_balances(balances, &[]);
        assert_eq!(
            result.len(),
            1,
            "entry with missing asset should be skipped"
        );
        assert_eq!(result[0].asset, AssetNameExchange::new("USDT"));
    }

    #[test]
    fn test_filter_balances_unparseable_free_skipped() {
        let balances = vec![
            binance_sdk::spot::rest_api::GetAccountResponseBalancesInner {
                asset: Some("BTC".to_string()),
                free: Some("not-a-number".to_string()),
                locked: Some("0.0".to_string()),
            },
        ];
        let result = filter_and_convert_balances(balances, &[]);
        assert!(
            result.is_empty(),
            "unparseable free field should be skipped"
        );
    }

    #[test]
    fn test_filter_balances_zero_balance_included() {
        // Zero-balance entries are intentionally passed through — the caller decides
        // whether to filter them. Binance returns all ever-touched assets including zeroes.
        let balances = vec![
            make_balance("BTC", "0.00000000", "0.00000000"),
            make_balance("USDT", "100.0", "0.0"),
        ];
        let result = filter_and_convert_balances(balances, &[]);
        assert_eq!(result.len(), 2, "zero-balance entries must be included");
        let btc = result
            .iter()
            .find(|b| b.asset == AssetNameExchange::new("BTC"))
            .unwrap();
        assert_eq!(btc.balance.total, Decimal::ZERO);
        assert_eq!(btc.balance.free, Decimal::ZERO);
    }

    #[test]
    fn test_filter_balances_duplicate_assets_in_response() {
        // if the API response contains two entries for the same asset,
        // filter_and_convert_balances emits two AssetBalance entries. Callers
        // are responsible for deduplication if this matters for their use case.
        let balances = vec![
            make_balance("BTC", "1.0", "0.0"),
            make_balance("BTC", "2.0", "0.0"),
        ];
        let result = filter_and_convert_balances(balances, &[]);
        assert_eq!(
            result.len(),
            2,
            "duplicate asset entries produce two AssetBalance entries"
        );
    }

    // ---------------------------------------------------------------------------
    // convert_my_trade tests
    // ---------------------------------------------------------------------------

    fn make_base_trade() -> binance_sdk::spot::rest_api::MyTradesResponseInner {
        binance_sdk::spot::rest_api::MyTradesResponseInner {
            id: Some(9001),
            order_id: Some(4242),
            price: Some("50000.00".to_string()),
            qty: Some("0.01".to_string()),
            commission: Some("0.05".to_string()),
            time: Some(1_700_000_000_000),
            is_buyer: Some(true),
            ..Default::default()
        }
    }

    #[test]
    fn test_convert_my_trade_happy_path() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let trade =
            convert_my_trade(&make_base_trade(), &instrument).expect("valid trade should convert");
        assert_eq!(trade.instrument, instrument);
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.price, Decimal::from_str("50000.00").unwrap());
        assert_eq!(trade.quantity, Decimal::from_str("0.01").unwrap());
    }

    #[test]
    fn test_convert_my_trade_sell_side() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let t = binance_sdk::spot::rest_api::MyTradesResponseInner {
            is_buyer: Some(false),
            ..make_base_trade()
        };
        let trade = convert_my_trade(&t, &instrument).expect("sell-side trade should convert");
        assert_eq!(trade.side, Side::Sell);
    }

    #[test]
    fn test_convert_my_trade_missing_id_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let t = binance_sdk::spot::rest_api::MyTradesResponseInner {
            id: None,
            ..make_base_trade()
        };
        assert!(
            convert_my_trade(&t, &instrument).is_none(),
            "missing id should return None"
        );
    }

    #[test]
    fn test_convert_my_trade_missing_order_id_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let t = binance_sdk::spot::rest_api::MyTradesResponseInner {
            order_id: None,
            ..make_base_trade()
        };
        assert!(
            convert_my_trade(&t, &instrument).is_none(),
            "missing orderId should return None"
        );
    }

    #[test]
    fn test_convert_my_trade_missing_is_buyer_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let t = binance_sdk::spot::rest_api::MyTradesResponseInner {
            is_buyer: None,
            ..make_base_trade()
        };
        assert!(
            convert_my_trade(&t, &instrument).is_none(),
            "missing isBuyer should return None"
        );
    }

    #[test]
    fn test_convert_my_trade_commission_none_defaults_to_zero() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let t = binance_sdk::spot::rest_api::MyTradesResponseInner {
            commission: None,
            ..make_base_trade()
        };
        let trade =
            convert_my_trade(&t, &instrument).expect("None commission should still convert");
        assert_eq!(trade.fees.fees, Decimal::ZERO);
    }

    // ---------------------------------------------------------------------------
    // convert_open_order tests
    // ---------------------------------------------------------------------------

    fn make_base_open_order() -> binance_sdk::spot::rest_api::AllOrdersResponseInner {
        binance_sdk::spot::rest_api::AllOrdersResponseInner {
            order_id: Some(12345),
            client_order_id: Some("cid-abc".to_string()),
            side: Some("BUY".to_string()),
            r#type: Some("LIMIT".to_string()),
            price: Some("50000.00".to_string()),
            orig_qty: Some("0.01".to_string()),
            executed_qty: Some("0.0".to_string()),
            time_in_force: Some("GTC".to_string()),
            time: Some(1_700_000_000_000),
            ..Default::default()
        }
    }

    #[test]
    fn test_convert_open_order_happy_path() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let order = convert_open_order(&make_base_open_order(), &instrument)
            .expect("valid order should convert");
        assert_eq!(order.key.instrument, instrument);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.kind, OrderKind::Limit);
        assert_eq!(order.price, Decimal::from_str("50000.00").unwrap());
        assert_eq!(order.quantity, Decimal::from_str("0.01").unwrap());
        assert_eq!(order.state.filled_quantity, Decimal::ZERO);
    }

    #[test]
    fn test_convert_open_order_missing_order_id_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            order_id: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, &instrument).is_none(),
            "missing orderId should return None"
        );
    }

    #[test]
    fn test_convert_open_order_missing_side_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            side: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, &instrument).is_none(),
            "missing side should return None"
        );
    }

    #[test]
    fn test_convert_open_order_missing_type_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            r#type: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, &instrument).is_none(),
            "missing type should return None"
        );
    }

    #[test]
    fn test_convert_open_order_executed_qty_none_defaults_to_zero() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            executed_qty: None,
            ..make_base_open_order()
        };
        let order =
            convert_open_order(&o, &instrument).expect("None executedQty should still convert");
        assert_eq!(
            order.state.filled_quantity,
            Decimal::ZERO,
            "None executedQty should default to zero"
        );
    }

    #[test]
    fn test_convert_open_order_executed_qty_unparseable_defaults_to_zero() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            executed_qty: Some("bad-value".to_string()),
            ..make_base_open_order()
        };
        let order = convert_open_order(&o, &instrument)
            .expect("unparseable executedQty should still convert");
        assert_eq!(
            order.state.filled_quantity,
            Decimal::ZERO,
            "unparseable executedQty should default to zero"
        );
    }

    // ---------------------------------------------------------------------------
    // dedup_key_from_event — non-Open active state paths
    // ---------------------------------------------------------------------------

    #[test]
    fn test_dedup_key_from_event_non_open_states_return_none() {
        use crate::order::state::{
            ActiveOrderState, CancelInFlight, InactiveOrderState, OpenInFlight,
        };
        use barter_integration::collection::snapshot::Snapshot;

        let key = OrderKey::new(
            ExchangeId::BinanceSpot,
            InstrumentNameExchange::new("BTCUSDT"),
            StrategyId::unknown(),
            ClientOrderId::new("cid1"),
        );

        // OpenInFlight → None (order is not yet acknowledged by exchange)
        let event = UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::OrderSnapshot(Snapshot(Order::new(
                key.clone(),
                Side::Buy,
                Decimal::ZERO,
                Decimal::ZERO,
                OrderKind::Market,
                TimeInForce::ImmediateOrCancel,
                OrderState::<AssetNameExchange, InstrumentNameExchange>::Active(
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                ),
            ))),
        );
        assert!(
            dedup_key_from_event(&event).is_none(),
            "OpenInFlight should return None — dedup not meaningful before exchange ack"
        );

        // CancelInFlight → None
        let event = UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::OrderSnapshot(Snapshot(Order::new(
                key.clone(),
                Side::Buy,
                Decimal::ZERO,
                Decimal::ZERO,
                OrderKind::Market,
                TimeInForce::ImmediateOrCancel,
                OrderState::<AssetNameExchange, InstrumentNameExchange>::Active(
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                ),
            ))),
        );
        assert!(
            dedup_key_from_event(&event).is_none(),
            "CancelInFlight should return None"
        );

        // Inactive(FullyFilled) → None
        let event = UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::OrderSnapshot(Snapshot(Order::new(
                key,
                Side::Buy,
                Decimal::ZERO,
                Decimal::ZERO,
                OrderKind::Market,
                TimeInForce::ImmediateOrCancel,
                OrderState::<AssetNameExchange, InstrumentNameExchange>::Inactive(
                    InactiveOrderState::FullyFilled,
                ),
            ))),
        );
        assert!(
            dedup_key_from_event(&event).is_none(),
            "Inactive state should return None"
        );
    }

    #[test]
    fn test_dedup_key_from_event_cancelled_error_returns_none() {
        // A REJECTED execution report produces OrderCancelled with Err state.
        // Verify that dedup_key_from_event returns None for such events (no dedup needed).
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("REJECTED".to_string()),
            r: Some("INSUFFICIENT_FUNDS".to_string()),
            ..make_base_report()
        };
        let event = convert_execution_report(report).expect("REJECTED report should produce Some");
        assert!(
            matches!(&event.kind, AccountEventKind::OrderCancelled(r) if r.state.is_err()),
            "prerequisite: event is OrderCancelled with Err"
        );
        assert!(
            dedup_key_from_event(&event).is_none(),
            "OrderCancelled with Err state should return None"
        );
    }

    // ---------------------------------------------------------------------------
    // convert_user_data_events tests
    // ---------------------------------------------------------------------------

    #[test]
    fn test_convert_user_data_events_execution_report_pushes_to_buf() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("NEW".to_string()),
            s_uppercase: Some("BUY".to_string()),
            o: Some("LIMIT".to_string()),
            p: Some("50000.00".to_string()),
            q: Some("0.01".to_string()),
            f: Some("GTC".to_string()),
            z: Some("0".to_string()),
            ..make_base_report()
        };
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(
            UserDataStreamEventsResponse::ExecutionReport(Box::new(report)),
            &mut buf,
        );
        assert!(
            !terminated,
            "ExecutionReport should not signal stream termination"
        );
        assert_eq!(buf.len(), 1, "ExecutionReport should push one event");
        assert!(matches!(buf[0].kind, AccountEventKind::OrderSnapshot(_)));
    }

    #[test]
    fn test_convert_user_data_events_account_position_pushes_to_buf() {
        let position = binance_sdk::spot::websocket_api::OutboundAccountPosition {
            u: Some(1_700_000_000_000),
            b_uppercase: Some(vec![make_balance_inner("BTC", "1.0", "0.0")]),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(
            UserDataStreamEventsResponse::OutboundAccountPosition(Box::new(position)),
            &mut buf,
        );
        assert!(
            !terminated,
            "OutboundAccountPosition should not signal stream termination"
        );
        assert_eq!(
            buf.len(),
            1,
            "OutboundAccountPosition should push one balance event"
        );
    }

    #[test]
    fn test_convert_user_data_events_balance_update_ignored() {
        let update = binance_sdk::spot::websocket_api::BalanceUpdate {
            ..Default::default()
        };
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(
            UserDataStreamEventsResponse::BalanceUpdate(Box::new(update)),
            &mut buf,
        );
        assert!(
            !terminated,
            "BalanceUpdate should not signal stream termination"
        );
        assert!(buf.is_empty(), "BalanceUpdate should push no events");
    }

    #[test]
    fn test_convert_user_data_events_stream_terminated_signals_reconnect() {
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(
            UserDataStreamEventsResponse::EventStreamTerminated(Default::default()),
            &mut buf,
        );
        assert!(
            terminated,
            "EventStreamTerminated must signal stream termination"
        );
        assert!(
            buf.is_empty(),
            "EventStreamTerminated should push no events"
        );
    }

    // ---------------------------------------------------------------------------
    // RateLimitTracker tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_rate_limit_tracker_not_blocked_initially() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new();
        // wait_if_blocked should return immediately (no cooldown set)
        tokio::time::timeout(
            std::time::Duration::from_millis(1),
            tracker.wait_if_blocked(),
        )
        .await
        .expect("wait_if_blocked should return immediately when not blocked");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_blocks_until_deadline() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new();
        let delay = Duration::from_secs(5);
        tracker.on_rate_limited(Some(delay));

        // Should not complete immediately
        assert!(
            tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked())
                .await
                .is_err(),
            "wait_if_blocked should block while cooldown is active"
        );

        // Advance past cooldown
        tokio::time::advance(delay + Duration::from_millis(1)).await;
        tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked())
            .await
            .expect("wait_if_blocked should return after cooldown expires");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_cooldown_extends_to_max() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new();

        // Set initial 5s cooldown
        tracker.on_rate_limited(Some(Duration::from_secs(5)));
        // Extend with a longer 10s cooldown — deadline should be pushed out
        tracker.on_rate_limited(Some(Duration::from_secs(10)));

        // Advance past the initial 5s — should still be blocked
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked())
                .await
                .is_err(),
            "cooldown should have been extended to 10s"
        );

        // Advance past the extended 10s deadline
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked())
            .await
            .expect("wait_if_blocked should return after extended cooldown expires");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_shorter_cooldown_does_not_shorten() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new();

        // Set 10s cooldown then try to shorten with 2s — deadline should stay at 10s
        tracker.on_rate_limited(Some(Duration::from_secs(10)));
        tracker.on_rate_limited(Some(Duration::from_secs(2)));

        // Advance past 2s — should still be blocked
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked())
                .await
                .is_err(),
            "shorter on_rate_limited must not shorten existing cooldown"
        );
    }

    // ---------------------------------------------------------------------------
    // M1: BalanceInsufficient type confusion — explicit field value test
    // ---------------------------------------------------------------------------

    #[test]
    fn test_parse_binance_api_error_balance_insufficient_holds_instrument_name() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        match parse_binance_api_error("Insufficient balance".into(), &instrument) {
            ApiError::BalanceInsufficient(asset_field, _) => {
                // documented known-wrong value — the AssetNameExchange field holds
                // the *instrument* name ("BTCUSDT"), not an asset name ("BTC" or "USDT").
                // Splitting the instrument into base/quote requires symbol-info metadata.
                // Do NOT pattern-match on this field to identify the low-balance asset.
                assert_eq!(
                    asset_field.name().as_str(),
                    "BTCUSDT",
                    "BalanceInsufficient.0 holds the instrument name, not an asset name"
                );
            }
            other => panic!("expected BalanceInsufficient, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------------------
    // L1: is_api_rejection_error with a context-wrapped error chain
    // ---------------------------------------------------------------------------

    #[test]
    fn test_is_api_rejection_error_with_wrapped_error_chain() {
        // `is_api_rejection_error` uses `anyhow::Error::downcast_ref`, which searches
        // the *entire* error chain (not just the root). This test verifies that a
        // ResponseError wrapped in anyhow context layers is still detected correctly,
        // so SDK-internal context wrapping does not break the rejection check.
        let raw = anyhow::anyhow!(WebsocketError::ResponseError {
            code: -2010,
            message: "insufficient balance".into(),
        });
        // Unwrapped root: must be detected
        assert!(
            is_api_rejection_error(&raw),
            "unwrapped ResponseError at root must be detected"
        );
        // Context-wrapped: anyhow::downcast_ref searches the full chain, so this is also detected
        let wrapped = raw.context("outer context (e.g. SDK adds context layer)");
        assert!(
            is_api_rejection_error(&wrapped),
            "context-wrapped ResponseError must still be detected — anyhow::downcast_ref searches the full chain"
        );
    }
}
