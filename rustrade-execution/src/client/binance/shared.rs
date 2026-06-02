//! Shared Binance execution infrastructure.
//!
//! Exchange-agnostic building blocks used by both the spot and margin clients:
//! reconnect/backoff, rate-limit tracking, event deduplication, the
//! connection-manager stream guard, Binance error parsing/classification, and the
//! Binance-string → rustrade-enum parsers (side/order-kind/TIF).
//!
//! Nothing here is spot- or margin-specific: the dedup cache and parsers operate on
//! rustrade's own types (`UnindexedAccountEvent`, `OrderKind`, `TimeInForce`, `Side`)
//! or on Binance's stable wire strings, so both clients reuse them unchanged. The
//! SDK-typed converters (which differ between spot's WS-API enums and margin's REST
//! params) deliberately stay in their respective modules.

use crate::{
    AccountEventKind, UnindexedAccountEvent,
    error::{ApiError, ConnectivityError, OrderError, UnindexedClientError},
    order::{OrderKind, TimeInForce, TrailingOffsetType, state::OrderState},
};
use binance_sdk::common::errors::{ConnectorError, WebsocketError};
use lru::LruCache;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, warn};

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
pub(crate) struct AbortOnDropStream<S> {
    inner: S,
    handle: tokio::task::JoinHandle<()>,
}

impl<S> AbortOnDropStream<S> {
    pub(crate) fn new(inner: S, handle: tokio::task::JoinHandle<()>) -> Self {
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
pub(crate) const INITIAL_BACKOFF_MS: u64 = 1_000;
/// Maximum backoff delay (cap for exponential growth).
pub(crate) const MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum number of consecutive reconnect attempts before giving up.
pub(crate) const MAX_RECONNECT_ATTEMPTS: u32 = 10;
/// If no WS activity (messages, ping, pong) for this duration, force reconnect.
pub(crate) const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
/// Timeout for fill recovery REST queries after reconnect.
pub(crate) const FILL_RECOVERY_TIMEOUT_SECS: u64 = 30;
/// Timeout for the initial WebSocket API TCP+TLS handshake.
/// Without this, a network partition holds the write lock for up to 75–127 s
/// (OS TCP timeout), stalling all concurrent open_order/cancel_order callers.
pub(crate) const CONNECT_TIMEOUT_SECS: u64 = 15;
/// Extra lookback subtracted from Signal disconnect timestamps to cover Tokio scheduling
/// jitter between the actual WS close and when the monitor task records Utc::now().
/// The dedup cache absorbs any resulting duplicate fills.
pub(crate) const SIGNAL_RECOVERY_LOOKBACK_MS: i64 = 500;
/// Size of the LRU dedup cache. 10k entries covers ~hours of high-frequency
/// trading at typical fill rates; each entry is ~84-88 bytes (DedupKey =
/// SmolStr[24] + SmolStr[24] + DedupEventKind[1] + padding[7] = 56 bytes, plus
/// LruCache node overhead: 2 linked-list pointers[16] + hashbrown slot[~12-16]
/// ≈ 28-32 bytes). At 10k: ~840-880 KB.
/// At very high fill rates (>333 distinct fills/sec sustained during the 30s
/// recovery window), LRU eviction could allow a fill to pass dedup twice.
/// Increase this constant if such volumes are expected.
pub(crate) const DEDUP_CACHE_SIZE: usize = 10_000;
/// Maximum trades per Binance REST query.
/// Stored as `usize` for direct use in `Vec::len()` comparisons; cast to `i32` at SDK call sites
/// (`MyTradesParams::limit(i32)`).
pub(crate) const BINANCE_MAX_TRADES: usize = 1000;
// Compile-time guard: SDK call sites cast this to i32; ensure it never overflows.
const _: () = assert!(
    BINANCE_MAX_TRADES <= i32::MAX as usize,
    "BINANCE_MAX_TRADES overflows i32"
);
/// Default delay when rate-limited (exponential backoff; Binance's `Retry-After`
/// header is not accessible through the SDK's `anyhow::Error` chain).
pub(crate) const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 10;
/// Maximum number of REST retry attempts on rate-limit errors.
pub(crate) const MAX_RATE_LIMIT_RETRIES: u32 = 3;

// ---------------------------------------------------------------------------
// Dedup cache
// ---------------------------------------------------------------------------

/// Event kind discriminant for dedup keys. Using an enum instead of a SmolStr
/// constant avoids constructing a string value on every event in the hot path.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub(crate) enum DedupEventKind {
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
pub(crate) struct DedupKey {
    pub(crate) instrument: SmolStr,
    pub(crate) id: SmolStr,
    pub(crate) kind: DedupEventKind,
}
pub(crate) type SharedDedupCache = Arc<parking_lot::Mutex<LruCache<DedupKey, ()>>>;

pub(crate) fn new_dedup_cache() -> SharedDedupCache {
    // allow(clippy::unwrap_used) — NonZeroUsize::new on a literal constant
    // cannot fail at runtime.
    #[allow(clippy::unwrap_used)]
    Arc::new(parking_lot::Mutex::new(LruCache::new(
        NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
    )))
}

/// Extract a dedup key from an account event, if applicable.
/// Returns None for events that don't need deduplication (e.g. balance snapshots).
pub(crate) fn dedup_key_from_event(event: &UnindexedAccountEvent) -> Option<DedupKey> {
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
        _ => None, // BalanceSnapshot, BalanceStreamUpdate, Snapshot, StreamError — no dedup needed
    }
}

/// Check and insert a dedup key. Returns true if the event is a duplicate.
/// Takes `key` by value to avoid cloning on the non-duplicate (common) path.
pub(crate) fn is_duplicate(cache: &SharedDedupCache, key: DedupKey) -> bool {
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
/// Thread-safe: inner state is behind a Mutex so clones of the client (which
/// share the same Arc<RateLimitTracker>) all respect the same cooldown.
pub(crate) struct RateLimitTracker {
    /// If set, REST calls should wait until this instant before proceeding.
    // parking_lot::Mutex — never poisons, consistent with SharedDedupCache
    blocked_until: parking_lot::Mutex<Option<tokio::time::Instant>>,
}

impl RateLimitTracker {
    pub(crate) fn new() -> Self {
        Self {
            blocked_until: parking_lot::Mutex::new(None),
        }
    }

    /// Sleep if currently in a rate-limit cooldown. Returns immediately if not blocked.
    ///
    /// Loops after waking to re-check the deadline: another task may have called
    /// `on_rate_limited` with a longer cooldown while this task was sleeping.
    pub(crate) async fn wait_if_blocked(&self) {
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
                        "Binance REST rate-limited, waiting before request"
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
    pub(crate) fn on_rate_limited(&self, retry_after: Option<Duration>) {
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
                "Binance rate-limit cooldown extended"
            );
        } else {
            warn!(
                delay_secs = delay.as_secs(),
                "Binance entering rate-limit degradation mode"
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
pub(crate) fn is_rate_limit_error(e: &anyhow::Error) -> bool {
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
/// binance-sdk wraps both transport failures and API rejections as
/// `WebsocketError::ResponseError`. This function distinguishes them so API rejections
/// (-2010, -1121, etc.) don't tear down a healthy WS session.
/// Re-verify on SDK upgrade — if the SDK changes error wrapping, `downcast_ref` returns
/// `None` and all rejections would be misclassified as transport errors.
pub(crate) fn is_api_rejection_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<WebsocketError>()
        .is_some_and(|we| matches!(we, WebsocketError::ResponseError { .. }))
}

// ---------------------------------------------------------------------------
// Exponential backoff
// ---------------------------------------------------------------------------

pub(crate) struct ExponentialBackoff {
    attempt: u32,
    max_attempts: u32,
    initial_ms: u64,
    max_ms: u64,
}

impl ExponentialBackoff {
    pub(crate) fn new() -> Self {
        Self {
            attempt: 0,
            max_attempts: MAX_RECONNECT_ATTEMPTS,
            initial_ms: INITIAL_BACKOFF_MS,
            max_ms: MAX_BACKOFF_MS,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Wait for the current backoff duration. Returns `false` if max attempts exhausted.
    pub(crate) async fn wait(&mut self) -> bool {
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
            "Binance reconnect backoff"
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        true
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers (Binance wire strings → rustrade enums)
// ---------------------------------------------------------------------------

pub(crate) fn parse_side(s: &str) -> Option<Side> {
    match s {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => {
            warn!(side = s, "unknown Binance order side");
            None
        }
    }
}

pub(crate) fn parse_order_kind(t: &str) -> Option<OrderKind> {
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

pub(crate) fn parse_time_in_force(tif: &str) -> TimeInForce {
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

// ---------------------------------------------------------------------------
// Error parsing / classification
// ---------------------------------------------------------------------------

/// Case-insensitive substring search that avoids the `to_lowercase` allocation.
fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
}

/// Returns true if `msg` contains `code` as a standalone numeric token:
/// not immediately preceded or followed by another ASCII digit.
/// Prevents "-2013" from matching "-20130" (suffix guard) or "1-2013" (prefix guard).
///
/// Iterates all occurrences so that if the first match fails a digit-guard check
/// (e.g. `-2013` found inside `-20130`), a later valid occurrence is not missed.
pub(crate) fn contains_error_code(msg: &str, code: &str) -> bool {
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

/// Parse Binance error strings to rustrade ApiError.
///
/// depends on binance-sdk's internal error formatting (not a public API contract).
/// If the SDK changes its error message format, these string matches may silently stop working.
/// Matches numeric error codes first (stable), then falls back to message text heuristics.
pub(crate) fn parse_binance_api_error(
    error_msg: String,
    instrument: &InstrumentNameExchange,
) -> ApiError<AssetNameExchange, InstrumentNameExchange> {
    // Match on Binance error codes first — these are stable numeric identifiers
    if contains_error_code(&error_msg, "-1002") || contains_error_code(&error_msg, "-2015") {
        // -1002: "You are not authorized to execute this request"
        // -2015: "Invalid API-key, IP, or permissions for action"
        // Auth failures must not be retried as order rejections.
        return ApiError::Unauthenticated(error_msg);
    }
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

pub(crate) fn connectivity_error(e: anyhow::Error) -> UnindexedClientError {
    let msg = format!("{e:#}");

    // Check for auth failures before falling back to generic connectivity error.
    // -1002: "You are not authorized to execute this request"
    // -2015: "Invalid API-key, IP, or permissions for action"
    if contains_error_code(&msg, "-1002")
        || contains_error_code(&msg, "-2015")
        || contains_ignore_case(&msg, "invalid api-key")
        || contains_ignore_case(&msg, "invalid signature")
        || contains_ignore_case(&msg, "signature for this request is not valid")
    {
        return UnindexedClientError::Api(ApiError::Unauthenticated(msg));
    }

    UnindexedClientError::Connectivity(ConnectivityError::Socket(msg))
}

// ---------------------------------------------------------------------------
// REST call retry wrapper
// ---------------------------------------------------------------------------

/// Execute a REST call with rate-limit awareness and retry.
///
/// Generic over the SDK `RestApi` type (`R`) so it serves both the spot
/// (`binance_sdk::spot::rest_api::RestApi`) and margin
/// (`binance_sdk::margin_trading::rest_api::RestApi`) clients — the helper never touches
/// `R` itself, it only hands an `Arc<R>` clone to the per-attempt closure. Also usable
/// from concurrent per-instrument futures that hold only `Arc<R>` + `Arc<RateLimitTracker>`.
pub(crate) async fn rest_call_with_retry<R, T>(
    rest: &Arc<R>,
    rate_limiter: &RateLimitTracker,
    mut make_call: impl FnMut(
        Arc<R>,
    )
        -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<T>> + Send>>,
) -> anyhow::Result<T>
where
    // `Arc<R>` is moved into the `+ Send` future, which requires `R: Send + Sync`. Both SDK
    // RestApi types satisfy this; stating it here surfaces the constraint at the definition
    // rather than as a confusing error at the call sites' `Box::pin(async move ...)`.
    R: Send + Sync,
{
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
                    attempt = attempt.saturating_add(1),
                    max = MAX_RATE_LIMIT_RETRIES,
                    delay_secs = delay.as_secs(),
                    "Binance REST rate-limited, retrying"
                );
                rate_limiter.on_rate_limited(Some(delay));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("Binance REST retries exhausted: loop invariant violated")
}

// ---------------------------------------------------------------------------
// Order-kind / TIF classification (rustrade enums → Binance semantics)
// ---------------------------------------------------------------------------

/// Venue-neutral Binance order type — the single source of truth for mapping a rustrade
/// [`OrderKind`] to Binance order semantics, shared by the spot and margin clients.
///
/// Spot maps this to the WS-API `OrderPlaceTypeEnum`; margin maps it to the REST `r#type`
/// **string** (the margin SDK types the field as a plain `String`, not an enum) via
/// [`as_binance_str`](Self::as_binance_str). Keeping the decision logic here (in
/// [`classify_order_kind_tif`]) avoids duplicating the match arms across the two clients,
/// which differ only in their SDK output types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinanceOrderType {
    Market,
    Limit,
    LimitMaker,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

impl BinanceOrderType {
    /// The Binance API order-type wire string (e.g. `"STOP_LOSS_LIMIT"`).
    pub(crate) fn as_binance_str(self) -> &'static str {
        match self {
            BinanceOrderType::Market => "MARKET",
            BinanceOrderType::Limit => "LIMIT",
            BinanceOrderType::LimitMaker => "LIMIT_MAKER",
            BinanceOrderType::StopLoss => "STOP_LOSS",
            BinanceOrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            BinanceOrderType::TakeProfit => "TAKE_PROFIT",
            BinanceOrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
        }
    }
}

/// Venue-neutral Binance time-in-force. Both spot and margin expose exactly `GTC`/`IOC`/`FOK`
/// on their order endpoints; post-only is modelled as [`BinanceOrderType::LimitMaker`] (no TIF),
/// matching Binance's own API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinanceTimeInForce {
    Gtc,
    Ioc,
    Fok,
}

/// Map a rustrade [`OrderKind`] + [`TimeInForce`] to Binance order semantics.
///
/// Returns `None` for combinations Binance does not support (so callers surface
/// `UnsupportedOrderType`). `TrailingStop`/`TrailingStopLimit` classify to
/// [`BinanceOrderType::StopLoss`]/`None` here (valid for spot, which sets `trailingDelta`);
/// the **margin** adapter rejects trailing kinds *before* calling this, since the margin SDK
/// has no `trailingDelta` binding.
pub(crate) fn classify_order_kind_tif(
    kind: OrderKind,
    tif: TimeInForce,
) -> Option<(BinanceOrderType, Option<BinanceTimeInForce>)> {
    match kind {
        OrderKind::Market => Some((BinanceOrderType::Market, None)),
        OrderKind::Limit => match tif {
            TimeInForce::GoodUntilCancelled { post_only: false } => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Gtc)))
            }
            TimeInForce::GoodUntilCancelled { post_only: true } => {
                // LIMIT_MAKER is Binance's post-only order type (rejects if
                // it would immediately match as taker)
                Some((BinanceOrderType::LimitMaker, None))
            }
            TimeInForce::FillOrKill => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Fok)))
            }
            TimeInForce::ImmediateOrCancel => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Ioc)))
            }
            // Binance does not support GTD (good-til-end-of-day), GTC-until-date, MOO, or MOC.
            // Surface as unsupported rather than silently coercing — these have venue-specific
            // semantics (e.g. an end-of-day auto-cancel) that a GTC coercion would silently drop,
            // risking an unintended resting/overnight order.
            TimeInForce::GoodUntilEndOfDay
            | TimeInForce::GoodTillDate { .. }
            | TimeInForce::AtOpen
            | TimeInForce::AtClose => {
                warn!(time_in_force = ?tif, "Binance does not support this TimeInForce");
                None
            }
        },
        // Conditional orders: stop_price/trailing_delta set separately by the caller.
        OrderKind::Stop { .. } => Some((BinanceOrderType::StopLoss, None)),
        OrderKind::StopLimit { .. } => match tif {
            // StopLimit requires TIF like regular Limit orders.
            TimeInForce::GoodUntilCancelled { post_only: false } => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Gtc),
            )),
            TimeInForce::FillOrKill => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Fok),
            )),
            TimeInForce::ImmediateOrCancel => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Ioc),
            )),
            _ => {
                warn!(time_in_force = ?tif, "Binance StopLimit does not support this TimeInForce");
                None
            }
        },
        OrderKind::TakeProfit { .. } => Some((BinanceOrderType::TakeProfit, None)),
        OrderKind::TakeProfitLimit { .. } => match tif {
            // TakeProfitLimit requires TIF like regular Limit orders.
            TimeInForce::GoodUntilCancelled { post_only: false } => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Gtc),
            )),
            TimeInForce::FillOrKill => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Fok),
            )),
            TimeInForce::ImmediateOrCancel => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Ioc),
            )),
            _ => {
                warn!(time_in_force = ?tif, "Binance TakeProfitLimit does not support this TimeInForce");
                None
            }
        },
        // TrailingStop: Binance uses STOP_LOSS with a trailingDelta parameter. Only
        // BasisPoints and Percentage are supported; Absolute requires manual conversion by
        // the caller: basis_points = (absolute / price) * 10000.
        OrderKind::TrailingStop { offset_type, .. } => match offset_type {
            TrailingOffsetType::BasisPoints | TrailingOffsetType::Percentage => {
                Some((BinanceOrderType::StopLoss, None))
            }
            TrailingOffsetType::Absolute => {
                warn!(
                    "Binance TrailingStop does not support Absolute offset; \
                     convert to basis points: (absolute / price) * 10000"
                );
                None
            }
        },
        // Binance does not support TrailingStopLimit.
        OrderKind::TrailingStopLimit { .. } => {
            warn!("Binance does not support TrailingStopLimit orders");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// REST order-error classification
// ---------------------------------------------------------------------------

/// Classify an `anyhow::Error` from a REST order/cancel call into an [`OrderError`].
///
/// REST errors differ from the WS-API path: binance-sdk surfaces them as
/// [`ConnectorError`], whose `Display` **omits** the Binance numeric code (it lives in a
/// separate `code` field). So the WS classifier [`is_api_rejection_error`] (which downcasts
/// to `WebsocketError`) does not apply here — we downcast to `ConnectorError` instead, splice
/// the code back into the message, and reuse [`parse_binance_api_error`] for precise mapping.
///
/// - 401/403 → [`ApiError::Unauthenticated`]; 429/418 → [`ApiError::RateLimit`].
/// - 400/404 / other client errors that carry a Binance code → mapped by code/text.
/// - Network/server failures (and the SDK's codeless transport/decode wrappers — failed HTTP
///   request, response-byte read, gzip, or UTF-8 decode) → [`OrderError::Connectivity`]: the
///   order may or may not have reached the matching engine.
pub(crate) fn classify_rest_order_error(
    e: &anyhow::Error,
    instrument: &InstrumentNameExchange,
) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
    let Some(ce) = e.downcast_ref::<ConnectorError>() else {
        // Not an SDK ConnectorError — treat as opaque transport failure.
        return OrderError::Connectivity(ConnectivityError::Socket(format!("{e:#}")));
    };

    match ce {
        ConnectorError::TooManyRequestsError { .. } | ConnectorError::RateLimitBanError { .. } => {
            OrderError::Rejected(ApiError::RateLimit)
        }
        ConnectorError::UnauthorizedError { msg, .. }
        | ConnectorError::ForbiddenError { msg, .. } => {
            OrderError::Rejected(ApiError::Unauthenticated(msg.clone()))
        }
        ConnectorError::ServerError { msg, .. } | ConnectorError::NetworkError(msg) => {
            OrderError::Connectivity(ConnectivityError::Socket(msg.clone()))
        }
        ConnectorError::BadRequestError { msg, code }
        | ConnectorError::NotFoundError { msg, code }
        | ConnectorError::ConnectorClientError { msg, code } => {
            // A codeless `ConnectorClientError` from the SDK's `http_request` is a transport or
            // response-decode failure, not a matching-engine decision (genuine Binance rejections
            // carry a numeric code). These prefixes are the SDK's codeless transport/decode sites:
            // the request never completed, or a 2xx body could not be read/decompressed/decoded.
            // Route them to Connectivity — the order's venue status is unknown — rather than
            // misreporting a definitive rejection. Match by prefix (not a blanket `code.is_none()`)
            // so an unusual HTTP error *status* with no Binance code still maps to a rejection.
            // (Body-deserialization failures surface via `.data()`, a separate path handled at
            // that call site.)
            //
            // These prefixes mirror the codeless error sites in the binance-sdk `http_request`
            // helper (binance-sdk `src/common`). They are not a stable public contract — re-verify
            // this list (and the test below that pins it) whenever the binance-sdk pin is bumped.
            const TRANSPORT_PREFIXES: [&str; 4] = [
                "HTTP request failed",
                "Failed to get response bytes",
                "Failed to decompress gzip response",
                "Failed to convert response to UTF-8",
            ];
            if code.is_none() && TRANSPORT_PREFIXES.iter().any(|p| msg.starts_with(p)) {
                return OrderError::Connectivity(ConnectivityError::Socket(msg.clone()));
            }
            // Splice the code (Display omits it) back in so the shared code-first parser maps
            // -2010/-1121/-2011/… precisely; falls back to text heuristics when code is absent.
            let msg_with_code = code.map_or_else(|| msg.clone(), |c| format!("{c} {msg}"));
            OrderError::Rejected(parse_binance_api_error(msg_with_code, instrument))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(
        msg: &str,
        code: Option<i64>,
    ) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
        let err = anyhow::Error::new(ConnectorError::ConnectorClientError {
            msg: msg.to_string(),
            code,
        });
        classify_rest_order_error(&err, &InstrumentNameExchange::new("BTCUSDT"))
    }

    #[test]
    fn codeless_transport_failures_map_to_connectivity() {
        // The SDK's codeless transport/decode wrappers (http_request error arm) must classify as
        // Connectivity — venue status unknown — never as a definitive rejection. Misreporting one
        // of these as Rejected risks a phantom position when the order actually reached the engine.
        // This test also pins the brittle prefix list against silent SDK message-format drift.
        for msg in [
            "HTTP request failed: connection reset",
            "Failed to get response bytes: error reading body",
            "Failed to decompress gzip response",
            "Failed to convert response to UTF-8: invalid utf-8 sequence",
        ] {
            assert!(
                matches!(classify(msg, None), OrderError::Connectivity(_)),
                "expected Connectivity for {msg:?}"
            );
        }
    }

    #[test]
    fn coded_error_maps_to_rejection() {
        // A genuine matching-engine rejection carries a Binance numeric code.
        assert!(matches!(
            classify("Account has insufficient balance.", Some(-2010)),
            OrderError::Rejected(_)
        ));
    }

    #[test]
    fn codeless_non_transport_status_error_maps_to_rejection() {
        // An unusual HTTP error *status* with no Binance code (SDK's catch-all `_` arm) did reach
        // the venue, so it must remain a rejection — not be swallowed as connectivity by an
        // over-broad `code.is_none()` guard. This is the distinction the prefix match preserves.
        assert!(matches!(
            classify("Conflict", None),
            OrderError::Rejected(_)
        ));
    }

    fn classify_err(ce: ConnectorError) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
        classify_rest_order_error(
            &anyhow::Error::new(ce),
            &InstrumentNameExchange::new("BTCUSDT"),
        )
    }

    #[test]
    fn rate_limit_variants_map_to_rejection_ratelimit() {
        // 429/418 surface as RateLimit so callers can back off; venue did respond.
        for ce in [
            ConnectorError::TooManyRequestsError {
                msg: "Too many requests.".to_string(),
                code: Some(-1003),
            },
            ConnectorError::RateLimitBanError {
                msg: "IP banned.".to_string(),
                code: Some(-1003),
            },
        ] {
            assert!(matches!(
                classify_err(ce),
                OrderError::Rejected(ApiError::RateLimit)
            ));
        }
    }

    #[test]
    fn auth_variants_map_to_unauthenticated() {
        // 401/403 are definitive auth rejections, not connectivity.
        for ce in [
            ConnectorError::UnauthorizedError {
                msg: "bad key".to_string(),
                code: Some(-2014),
            },
            ConnectorError::ForbiddenError {
                msg: "forbidden".to_string(),
                code: None,
            },
        ] {
            assert!(matches!(
                classify_err(ce),
                OrderError::Rejected(ApiError::Unauthenticated(_))
            ));
        }
    }

    #[test]
    fn server_and_network_variants_map_to_connectivity() {
        // 5xx / transport failures leave the order's venue status unknown → Connectivity.
        for ce in [
            ConnectorError::ServerError {
                msg: "internal error".to_string(),
                status_code: Some(503),
            },
            ConnectorError::NetworkError("connection reset".to_string()),
        ] {
            assert!(matches!(classify_err(ce), OrderError::Connectivity(_)));
        }
    }
}
