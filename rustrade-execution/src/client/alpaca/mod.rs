// Alpaca ExecutionClient implementation
//
// Uses raw reqwest for REST and rustrade-integration tungstenite for WebSocket.
// No official Alpaca Rust SDK — built directly on reqwest + rustrade-integration
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
//   pauses every request until the reset once a response reports none remaining, and
//   backs off on 429 with up to MAX_RATE_LIMIT_ATTEMPTS total attempts
// - Reconnection: account_stream reconnects on WS close/error with exponential
//   backoff (1 s → 30 s, max 10 attempts)
// - Heartbeat monitoring: reconnects if no WS message for HEARTBEAT_TIMEOUT_SECS
// - Fill recovery: after reconnect, fetches missed fills via GET /v2/account/activities
//   since disconnect_time; sent through the dedup cache to filter duplicates. A read that fails,
//   times out or truncates is reported as AccountEventKind::FillRecoveryGaveUp
// - Dedup cache: LRU keyed on "{order_id}:{cumulative_filled_qty}" prevents
//   duplicate fills arising from the overlap between WS events before disconnect
//   and the fill-recovery REST window
// - Ended-order recovery: after the fills, each order held as live that one listing of the open
//   orders no longer shows is looked up by client order id (GET /v2/orders:by_client_order_id),
//   and how it ended is reported; a failed check is retried on a backoff while connected
//
// Known limitations:
// - Order lifecycle events missed while disconnected are recovered only for orders the client
//   holds as live (placed through it, listed by it, or seen on its stream); an order placed
//   elsewhere and ended during the outage is not reported.
// - A fill frame whose order status is neither partially_filled nor filled emits the
//   execution but no order snapshot, so a fill arriving after its order's terminal frame
//   cannot resurrect a retired order as a resting one. That order's filled quantity is
//   settled instead by the terminal frame's own filled_qty, or by fetch_open_orders.

use crate::{
    AccountEventKind, AccountSnapshot, FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope,
    InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::{
        BracketOrderClient, ExecutionClient, OrderStatusClient,
        order_recovery::{
            KnownLiveOrders, NoPendingFills, OpenListing, OrderLookup, SharedKnownLiveOrders,
            UncheckedOrders, fetch_ended_by_key, recover_ended_orders,
        },
    },
    emit_stream_terminated,
    error::{
        ApiError, ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce, TrailingOffsetType, UnindexedInactiveOrder,
        UnindexedOrderKey, UnindexedOrderSnapshot,
        bracket::{
            BracketOrderRequest as UnifiedBracketOrderRequest,
            BracketOrderResult as UnifiedBracketOrderResult,
        },
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{
            ActiveOrderState, Cancelled, Expired, Filled, InactiveOrderState, Open, OrderState,
            UnindexedOrderState,
        },
    },
    parse_env_bool,
    position::{Position, PositionReport},
    trade::{AssetFees, Trade, TradeId, TradesRead},
};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{SinkExt as _, StreamExt as _, stream::BoxStream};
use indexmap::IndexMap;
use itertools::Itertools as _;
use lru::LruCache;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use rustrade_integration::protocol::websocket::{WebSocket, WsMessage};
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
                    // as_millis() returns u128; truncation impossible (u64::MAX ms ≈ 584M years)
                    #[allow(clippy::cast_possible_truncation)]
                    let delay_ms = (until - now).as_millis() as u64;
                    debug!(delay_ms, "Alpaca REST rate-limited, waiting before request");
                    tokio::time::sleep_until(until).await;
                }
            }
        }
    }

    /// Record a rate-limit event, extending any existing cooldown if longer.
    fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let delay = retry_after.unwrap_or(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));
        let was_blocked = self.extend(delay);
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

    /// Record a response reporting no requests left in the current window: pause every request
    /// until the window resets, `reset` from now, so the next one is not refused with a 429.
    fn on_bucket_exhausted(&self, reset: Duration) {
        let was_blocked = self.extend(reset);
        // info! not warn!: nothing was refused. A 429 logs at warn.
        if !was_blocked {
            info!(
                delay_ms = u64::try_from(reset.as_millis()).unwrap_or(u64::MAX),
                "Alpaca rate-limit window exhausted (X-Ratelimit-Remaining: 0), pausing requests until it resets"
            );
        }
    }

    /// Push the cooldown out to `delay` from now unless it already ends later, and return whether
    /// one was active.
    fn extend(&self, delay: Duration) -> bool {
        let now = tokio::time::Instant::now();
        let new_deadline = now + delay;
        let mut guard = self.blocked_until.lock();
        let was_blocked = guard.is_some_and(|until| until > now);
        *guard = Some(guard.map_or(new_deadline, |existing| existing.max(new_deadline)));
        was_blocked
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

    /// Number of reconnect attempts consumed so far.
    ///
    /// After [`wait`](Self::wait) has returned `false` this equals `max_attempts` — the number of
    /// attempts made before the budget was exhausted. Used to populate
    /// [`StreamTerminationReason::ReconnectBudgetExhausted`].
    fn attempts(&self) -> u32 {
        self.attempt
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

/// LRU cache of fill dedup keys, `"{order_id}:{cumulative_filled_qty}"` ([`fill_dedup_key`]).
/// The key is not the fill's `TradeId`, which is the venue's execution id.
///
/// WS fills: `early_dedup_key` builds it from `order.id` and `order.filled_qty` (the cumulative
/// from the order update payload).
///
/// REST fills: `recover_fills` builds it from the activity's own `cum_qty`, the same figure the WS
/// path reads. Where Alpaca omits `cum_qty` it falls back to accumulating per-execution qty within
/// the batch, which is correct only for an order whose fills lie wholly inside the recovery
/// window.
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
    #[serde(default = "default_paper")]
    pub paper: bool,
    /// Test-only: override the REST base URL (e.g., to point at a wiremock server).
    #[cfg(test)]
    pub base_url_override: Option<String>,
}

/// Serde default for [`AlpacaConfig::paper`]: an absent `paper` field deserializes to the **safe**
/// paper environment (`true`).
///
/// `#[serde(default = "…")]` requires a named function (it cannot take a literal), so this exists
/// purely to supply that default to the derive.
fn default_paper() -> bool {
    true
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
    /// Create a new Alpaca config using the **safe** paper-trading endpoints (alias for
    /// [`paper`](Self::paper)).
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self::paper(api_key, secret_key)
    }

    /// Create a config targeting Alpaca's **paper-trading** endpoints (simulated funds).
    pub fn paper(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
            paper: true,
            #[cfg(test)]
            base_url_override: None,
        }
    }

    /// Create a config targeting Alpaca's **production** endpoints.
    ///
    /// ⚠️ Production trades execute against the live account with **real funds**. Prefer
    /// [`paper`](Self::paper) unless you explicitly intend live execution.
    pub fn production(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
            paper: false,
            #[cfg(test)]
            base_url_override: None,
        }
    }

    /// Build a config from environment variables.
    ///
    /// Reads:
    /// - `ALPACA_API_KEY` (required) — API key id.
    /// - `ALPACA_SECRET_KEY` (required) — API secret.
    /// - `ALPACA_PAPER` (optional) — `"true"`/`"false"` (case-insensitive). **Absent ⇒ the safe
    ///   paper environment.** Set `ALPACA_PAPER=false` to target production (real funds).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaConfigError`] (never panics):
    /// - a required credential var is unset ([`MissingApiKey`](AlpacaConfigError::MissingApiKey) /
    ///   [`MissingSecretKey`](AlpacaConfigError::MissingSecretKey)) or holds non-UTF-8
    ///   ([`InvalidApiKey`](AlpacaConfigError::InvalidApiKey) /
    ///   [`InvalidSecretKey`](AlpacaConfigError::InvalidSecretKey));
    /// - `ALPACA_PAPER` is neither `true` nor `false`, or holds non-UTF-8
    ///   ([`InvalidPaper`](AlpacaConfigError::InvalidPaper)).
    pub fn from_env() -> Result<Self, AlpacaConfigError> {
        let api_key = match std::env::var("ALPACA_API_KEY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Err(AlpacaConfigError::MissingApiKey),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AlpacaConfigError::InvalidApiKey);
            }
        };
        let secret_key = match std::env::var("ALPACA_SECRET_KEY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Err(AlpacaConfigError::MissingSecretKey),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AlpacaConfigError::InvalidSecretKey);
            }
        };

        let paper = match std::env::var("ALPACA_PAPER") {
            Ok(value) => parse_env_bool(&value).ok_or(AlpacaConfigError::InvalidPaper(value))?,
            Err(std::env::VarError::NotPresent) => true,
            // The toggle value is not secret, so echo it (lossily) like the parse-failure arm above —
            // an actionable "got X" beats a hardcoded sentinel.
            Err(std::env::VarError::NotUnicode(value)) => {
                return Err(AlpacaConfigError::InvalidPaper(
                    value.to_string_lossy().into_owned(),
                ));
            }
        };

        if paper {
            Ok(Self::paper(api_key, secret_key))
        } else {
            Ok(Self::production(api_key, secret_key))
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

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum AlpacaConfigError {
    #[error("ALPACA_API_KEY environment variable not set")]
    MissingApiKey,

    // No payload: the raw value is secret-key material, so it must never be echoed into an error
    // message or log. The variant name already identifies which credential var is non-UTF-8.
    #[error("ALPACA_API_KEY environment variable is not valid UTF-8")]
    InvalidApiKey,

    #[error("ALPACA_SECRET_KEY environment variable not set")]
    MissingSecretKey,

    #[error("ALPACA_SECRET_KEY environment variable is not valid UTF-8")]
    InvalidSecretKey,

    #[error("ALPACA_PAPER must be true or false, got {0}")]
    InvalidPaper(String),
}

// ---------------------------------------------------------------------------
// REST response serde types
// ---------------------------------------------------------------------------

/// The fields of GET /v2/account the USD balance is built from.
///
/// Amounts are decoded as decimals, so a malformed one fails the request instead of reading as
/// zero.
#[derive(Debug, Deserialize)]
struct AlpacaAccount {
    /// Settled and unsettled cash. Negative when the account has borrowed on margin; a short
    /// sale's proceeds are credited to it.
    #[serde(with = "rust_decimal::serde::str")]
    cash: Decimal,
    /// Buying power for securities that cannot be bought on margin. On a margin account it is
    /// equity less the initial margin requirement, so it counts the loan value of held
    /// marginable stock and can exceed cash; a short sale's proceeds do not raise it.
    #[serde(with = "rust_decimal::serde::str")]
    non_marginable_buying_power: Decimal,
}

/// A single position returned by GET /v2/positions.
///
/// Amounts are decoded as decimals, so a malformed one fails the request instead of reading as
/// zero or as a flat position.
#[derive(Debug, Deserialize)]
struct AlpacaPosition {
    /// Exchange symbol (e.g., "BTC/USD" for crypto, "AAPL" for equity, an OCC symbol for an
    /// option).
    symbol: String,
    /// Asset class: "us_equity", "crypto", "us_option".
    asset_class: String,
    /// Direction of the position. The size is taken from `qty`'s magnitude and the sign from
    /// this, so a short reads as short whichever sign `qty` carries. Missing reads as
    /// [`AlpacaPositionSide::Unknown`].
    #[serde(default)]
    side: AlpacaPositionSide,
    /// Quantity held: shares, contracts, or base currency for crypto.
    #[serde(with = "rust_decimal::serde::str")]
    qty: Decimal,
    /// Quantity available to trade (not locked in open orders).
    #[serde(with = "rust_decimal::serde::str")]
    qty_available: Decimal,
    /// Average entry price per unit.
    #[serde(default, with = "rust_decimal::serde::str_option")]
    avg_entry_price: Option<Decimal>,
    /// Unrealised profit or loss of the whole position, in USD.
    #[serde(default, with = "rust_decimal::serde::str_option")]
    unrealized_pl: Option<Decimal>,
}

/// Direction of an [`AlpacaPosition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AlpacaPositionSide {
    Long,
    Short,
    /// A missing side or a value Alpaca does not document. Decoded rather than rejected so that
    /// one position cannot fail the whole response, which also carries the crypto balances; a
    /// position that needs a direction fails in [`convert_positions`] instead.
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct AlpacaOrderResponse {
    id: String,
    client_order_id: Option<String>,
    symbol: String,
    qty: Option<String>,
    filled_qty: String,
    /// The average price of the order's fills, when any filled.
    #[serde(default)]
    filled_avg_price: Option<String>,
    /// The order's status: `new`, `filled`, `canceled`, and so on. Read by the lookup of how an
    /// order ended and from the response to placing one. Optional so that a response without it
    /// still decodes.
    #[serde(default)]
    status: Option<String>,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    time_in_force: String,
    limit_price: Option<String>,
    stop_price: Option<String>,
    trail_percent: Option<String>,
    trail_price: Option<String>,
    created_at: String,
    /// When the order last changed state. Nullable in Alpaca's schema, so callers fall back to
    /// `created_at` via [`order_state_time`].
    updated_at: Option<String>,
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
    /// The order's cumulative filled quantity as of this execution, as Alpaca reported it --
    /// the same figure the WebSocket path reads from `order.filled_qty`.
    ///
    /// Alpaca's schema lists this on trade activities without qualifying by fill type -- unlike
    /// `leaves_qty`, which it documents as partial-fill-specific. It is parsed as optional
    /// regardless, so an omission degrades to the fallback below rather than discarding the
    /// whole activity.
    cum_qty: Option<String>,
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
// Bracket order types
// ---------------------------------------------------------------------------

/// Take-profit parameters for Alpaca bracket orders.
///
/// The take-profit leg is always a limit order at the specified price.
#[derive(Debug, Serialize)]
struct TakeProfitParams {
    limit_price: String,
}

/// Stop-loss parameters for Alpaca bracket orders.
///
/// When `limit_price` is `None`, the stop-loss is a stop (market) order.
/// When `limit_price` is `Some`, the stop-loss becomes a stop-limit order.
#[derive(Debug, Serialize)]
struct StopLossParams {
    stop_price: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_price: Option<String>,
}

/// Request to place a bracket order (entry + take-profit + stop-loss).
///
/// A bracket order consists of three linked orders submitted in a single request:
/// 1. **Entry**: Limit order to enter the position
/// 2. **Take Profit**: Limit order to exit at profit target
/// 3. **Stop Loss**: Stop or stop-limit order to exit at loss limit
///
/// When either the take-profit or stop-loss fills, Alpaca automatically cancels
/// the other leg.
///
/// # Constraints
///
/// - `time_in_force` must be `Day` or `GoodUntilCancelled` (no extended hours)
/// - Entry order type is always `Limit`
/// - Take-profit is always a `Limit` order
/// - Stop-loss is a `Stop` order (or `StopLimit` if `stop_loss_limit_price` is set)
///
/// # Example
///
/// ```ignore
/// let request = AlpacaBracketOrderRequest::new(
///     "AAPL".into(),
///     StrategyId::new("momentum"),
///     ClientOrderId::new("bracket-001"),
///     Side::Buy,
///     dec!(10),
///     dec!(150.00),  // entry
///     dec!(160.00),  // take profit
///     dec!(145.00),  // stop loss
///     TimeInForce::GoodUntilCancelled { post_only: false },
/// );
/// // For stop-limit SL: .with_stop_loss_limit_price(dec!(144.00))
/// let result = client.open_bracket_order(request).await;
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlpacaBracketOrderRequest {
    /// Instrument to trade.
    pub instrument: InstrumentNameExchange,
    /// Strategy identifier for order correlation.
    pub strategy: StrategyId,
    /// Client order ID for the parent (entry) order.
    pub cid: ClientOrderId,
    /// Buy or Sell for the entry order (exits use opposite side).
    pub side: Side,
    /// Number of shares/contracts.
    pub quantity: Decimal,
    /// Entry limit price.
    pub entry_price: Decimal,
    /// Take-profit limit price.
    pub take_profit_price: Decimal,
    /// Stop-loss trigger price.
    pub stop_loss_price: Decimal,
    /// Optional stop-loss limit price. When set, makes the stop-loss a stop-limit order.
    pub stop_loss_limit_price: Option<Decimal>,
    /// Time-in-force for all legs. Must be `Day` or `GoodUntilCancelled`.
    pub time_in_force: TimeInForce,
}

impl AlpacaBracketOrderRequest {
    /// Create a new bracket order request.
    ///
    /// For a stop-limit stop-loss leg, chain `.with_stop_loss_limit_price()`.
    #[allow(clippy::too_many_arguments)] // Bracket orders inherently need many params
    pub fn new(
        instrument: InstrumentNameExchange,
        strategy: StrategyId,
        cid: ClientOrderId,
        side: Side,
        quantity: Decimal,
        entry_price: Decimal,
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            instrument,
            strategy,
            cid,
            side,
            quantity,
            entry_price,
            take_profit_price,
            stop_loss_price,
            stop_loss_limit_price: None,
            time_in_force,
        }
    }

    /// Set the stop-loss limit price, converting the SL leg to a stop-limit order.
    #[must_use]
    pub fn with_stop_loss_limit_price(mut self, price: Decimal) -> Self {
        self.stop_loss_limit_price = Some(price);
        self
    }
}

/// Result of placing an Alpaca bracket order.
///
/// Contains the parent order with its state. The take-profit and stop-loss legs
/// are managed by Alpaca and their status can be queried via `fetch_open_orders`.
///
/// # Note
///
/// Unlike IBKR which returns three separate orders, Alpaca's bracket API returns
/// a single parent order. The child legs (TP/SL) are implicitly created and linked
/// by Alpaca. Use `fetch_open_orders` to retrieve all legs after placement.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlpacaBracketOrderResult {
    /// Parent (entry) order with its current state.
    pub parent: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
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
    stop_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trail_percent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trail_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_order_id: Option<&'a str>,
    // position_intent: heuristic mapping (buy→buy_to_open, sell→sell_to_close).
    // Correct for directional long-only strategies. Omit for exchanges/asset
    // classes that don't require it (stocks/crypto ignore this field).
    #[serde(skip_serializing_if = "Option::is_none")]
    position_intent: Option<AlpacaPositionIntent>,
    // Bracket order fields (order_class, take_profit, stop_loss).
    // Set order_class to "bracket" and populate TP/SL for bracket orders.
    #[serde(skip_serializing_if = "Option::is_none")]
    order_class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    take_profit: Option<TakeProfitParams>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_loss: Option<StopLossParams>,
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
/// Alpaca's numeric and timestamp strings, and its execution ids (UUIDs), contain no JSON escape
/// sequences.
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
    /// Alpaca's id for this event. On a fill it is the execution's id, the same as the UUID after
    /// `::` in that fill's FILL activity id (see [`activity_execution_id`]), so it is the fill's
    /// [`TradeId`] on every path.
    #[serde(borrow)]
    execution_id: Option<&'a str>,
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
    // the event. A snapshot of a live order reads its absence as nothing filled
    // yet; a cancel reads it as unknown; the dedup key, and a fill's fallback trade id, read it
    // as "0".
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
    #[serde(borrow)]
    stop_price: Option<&'a str>,
    #[serde(borrow)]
    trail_percent: Option<&'a str>,
    #[serde(borrow)]
    trail_price: Option<&'a str>,
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
/// # Rate limits
/// Once a response reports no requests left in the current window (`X-Ratelimit-Remaining: 0`),
/// every request, orders included, waits until the window resets (`X-Ratelimit-Reset`, at most a
/// minute away), so it is not refused. A 429 waits the same way and is retried.
///
/// Cloning is cheap: all inner state is behind `Arc`.
#[derive(Clone)]
pub struct AlpacaClient {
    config: Arc<AlpacaConfig>,
    /// reqwest client with APCA-API-KEY-ID and APCA-API-SECRET-KEY pre-set as
    /// default headers — every request carries auth automatically.
    http: reqwest::Client,
    rate_limiter: Arc<RateLimitTracker>,
    /// Pre-allocated `/v2/orders` endpoint URL to avoid allocation per request.
    orders_url: String,
    /// The orders seen live and not yet seen end, which a reconnect asks about. Shared by every
    /// clone and every account stream, since an order placed through one ends on any of them.
    known_live: SharedKnownLiveOrders,
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
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
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

    /// Hold each of `orders` as live, for a reconnect to ask about if the stream misses its end.
    fn remember_live<'a>(
        &self,
        orders: impl IntoIterator<Item = &'a Order<ExchangeId, InstrumentNameExchange, Open>>,
    ) {
        let mut known = self.known_live.lock();
        for order in orders {
            known.live(&order.key, order.quantity, &order.state);
        }
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

/// Pause every request until the window resets when a response reports none remaining in it.
///
/// The pause is at most [`DEFAULT_RATE_LIMIT_DELAY_SECS`], the length of Alpaca's window. Call on
/// any response except a 429, which sets its own cooldown. Without an
/// `X-Ratelimit-Reset` there is nothing to pause until, so the next request goes ahead and a 429,
/// if it comes, backs off.
fn observe_rate_limit_remaining(
    rate_limiter: &RateLimitTracker,
    headers: &reqwest::header::HeaderMap,
) {
    let exhausted = headers
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        == Some(0);
    if !exhausted {
        return;
    }
    match parse_rate_limit_delay(headers) {
        // Alpaca's window is a minute, so a reset further out is a bad header or clock skew, and
        // must not hold orders back for longer.
        Some(reset) => rate_limiter
            .on_bucket_exhausted(reset.min(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS))),
        None => debug!(
            "Alpaca REST rate-limit window exhausted (X-Ratelimit-Remaining: 0) without X-Ratelimit-Reset"
        ),
    }
}

/// Execute a REST request with rate-limit awareness and retry.
///
/// The `build_request` closure is called on every attempt so the caller doesn't
/// need a clone of `RequestBuilder` (which may not be cloneable with streaming bodies).
/// For GET/POST/DELETE with fixed bodies, the closure is a cheap re-construction.
///
/// On HTTP 429, reads `X-Ratelimit-Reset` (Unix epoch) to determine the cooldown
/// duration and retries up to `MAX_RATE_LIMIT_ATTEMPTS - 1` times. Any other response reporting
/// `X-Ratelimit-Remaining: 0` pauses later requests until that reset; see
/// [`observe_rate_limit_remaining`].
///
/// # Errors
///
/// - [`UnindexedClientError::Connectivity`]: the request or its body read failed, or a 5xx.
/// - [`UnindexedClientError::Api`]: a 4xx, classified by [`parse_api_error`], or 429 retries
///   exhausted ([`ApiError::RateLimit`]).
/// - [`UnindexedClientError::Internal`]: a 2xx body that does not decode into `T`, or a 204 on a
///   call that expects a body. Retrying returns the same answer.
async fn rest_with_retry<T>(
    rate_limiter: &RateLimitTracker,
    build_request: impl FnMut() -> reqwest::RequestBuilder,
) -> Result<T, UnindexedClientError>
where
    T: for<'de> Deserialize<'de>,
{
    match rest_request(rate_limiter, build_request).await? {
        Fetched::Found(value) => Ok(value),
        Fetched::NotFound(message) => Err(UnindexedClientError::Api(parse_api_error(
            reqwest::StatusCode::NOT_FOUND,
            &message,
        ))),
    }
}

/// [`rest_with_retry`] for a lookup by id: a 404 is `Ok(None)`, Alpaca knowing nothing under the
/// id asked for, rather than an error.
async fn rest_lookup_with_retry<T>(
    rate_limiter: &RateLimitTracker,
    build_request: impl FnMut() -> reqwest::RequestBuilder,
) -> Result<Option<T>, UnindexedClientError>
where
    T: for<'de> Deserialize<'de>,
{
    match rest_request(rate_limiter, build_request).await? {
        Fetched::Found(value) => Ok(Some(value)),
        Fetched::NotFound(_) => Ok(None),
    }
}

/// What [`rest_request`] got back from a request that did not fail.
#[derive(Debug)]
enum Fetched<T> {
    /// A 2xx body, decoded.
    Found(T),
    /// A 404, with Alpaca's message.
    NotFound(String),
}

/// [`rest_with_retry`] and [`rest_lookup_with_retry`], which differ only in how they read a 404.
async fn rest_request<T>(
    rate_limiter: &RateLimitTracker,
    mut build_request: impl FnMut() -> reqwest::RequestBuilder,
) -> Result<Fetched<T>, UnindexedClientError>
where
    T: for<'de> Deserialize<'de>,
{
    for attempt in 0..MAX_RATE_LIMIT_ATTEMPTS {
        rate_limiter.wait_if_blocked().await;
        let response = build_request()
            .send()
            .await
            .map_err(|e| connectivity_err(format!("Alpaca REST request failed: {e}")))?;

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

        observe_rate_limit_remaining(rate_limiter, response.headers());

        // 204 No Content is only valid for DELETE endpoints; use rest_delete_with_retry
        // for those. Reaching here for a 204 indicates API misuse — return a clear error
        // rather than a misleading "EOF while parsing" JSON failure. A retry cannot fix it.
        if status == reqwest::StatusCode::NO_CONTENT {
            return Err(UnindexedClientError::Internal(
                "Alpaca REST returned 204 No Content — use rest_delete_with_retry for DELETE endpoints"
                    .to_string(),
            ));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| connectivity_err(format!("Alpaca REST read body failed: {e}")))?;

        // A 2xx body that does not fit the model is not transient: retrying returns the same
        // body. An order path must still treat it as status unknown; see `order_post_error`.
        if status.is_success() {
            return serde_json::from_slice::<T>(&bytes)
                .map(Fetched::Found)
                .map_err(|e| {
                    UnindexedClientError::Internal(format!(
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

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(Fetched::NotFound(api_err));
        }

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
/// Handles 204 No Content (success), 422 / 403 (API rejection), and 429 (rate limit). Any other
/// response reporting `X-Ratelimit-Remaining: 0` pauses later requests, as in [`rest_with_retry`].
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

        observe_rate_limit_remaining(rate_limiter, response.headers());

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
    const EXCHANGE: ExchangeId = ExchangeId::AlpacaBroker;

    // Equities and crypto are both `Spot` here — Alpaca settles each as an outright holding, and
    // the client separates them only by symbol shape. `Option` is genuinely routed: the
    // `position_intent` Alpaca requires on options orders is derived from that same shape test,
    // which is `is_options_or_equity_symbol` — a not-a-crypto-pair check (`!symbol.contains('/')`),
    // not OCC format validation. Equities take the branch too, harmlessly: `position_intent` is
    // accepted on an equity order.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[
        InstrumentKindDiscriminant::Spot,
        InstrumentKindDiscriminant::Option,
    ];
    type Config = AlpacaConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// # Panics
    ///
    /// Panics if the API key or secret key contains characters invalid in an
    /// HTTP header value.
    fn new(config: Self::Config) -> Self {
        let http = Self::build_http(&config);
        let orders_url = format!("{}/v2/orders", config.rest_base_url());
        Self {
            config: Arc::new(config),
            http,
            rate_limiter: Arc::new(RateLimitTracker::new()),
            orders_url,
            known_live: KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
        }
    }

    /// # Balances and positions
    ///
    /// - **USD**: `total` is the account's `cash`, and `free` the lesser of cash and its
    ///   `non_marginable_buying_power`: cash that can be spent without borrowing. Alpaca's
    ///   buying power figures include the loan value of held stock, so none of them alone is
    ///   free cash. A short sale's proceeds are credited to cash but do not raise buying power,
    ///   so `free` stays below `total` while a short is open. Cash is negative while the account
    ///   borrows on margin, and then `free` is negative too. Account equity is not reported: it
    ///   is cash plus the value of the positions below, so it can be derived.
    /// - **Equities and options**: each holding becomes the
    ///   [`InstrumentAccountSnapshot::position`] of its instrument, [`PositionReport::Open`] with a
    ///   signed [`Position`] (negative for a short), the average entry price and the unrealised
    ///   PnL in USD. An equity or option without a holding is [`PositionReport::Flat`], since
    ///   Alpaca lists every open position. An option's
    ///   entry price is the premium per share of the underlying, as quoted and as orders are
    ///   priced, not per contract. Margin, liquidation price and leverage are `None`, and
    ///   `time_exchange` is the time of the call, since Alpaca does not timestamp positions.
    /// - **Crypto**: each holding is an asset balance of its base asset (e.g. `btc` for
    ///   `BTC/USD`), in base units. Alpaca crypto is spot-only and cannot be sold short, so there
    ///   is no position to report: its instruments are [`PositionReport::Unreported`].
    ///
    /// # Limitations
    ///
    /// A position is reported only under the instrument name Alpaca uses for it, ignoring case:
    /// the ticker for an equity and the OCC symbol for an option. A requested equity or option
    /// named otherwise is reported [`PositionReport::Flat`] even while Alpaca holds it. A crypto
    /// pair is recognised by the `/` in its name (e.g. `BTC/USD`, the form its orders use); one
    /// named without it is taken for an equity and reported flat too. With `instruments` empty,
    /// every instrument that has an open order or a position gets a snapshot; otherwise only the
    /// requested ones do.
    ///
    /// # Errors
    ///
    /// [`ClientError::Internal`](crate::error::ClientError::Internal) when a response does not
    /// decode, or when any non-zero equity or option position, requested or not, has a missing or
    /// unrecognised `side`: its direction cannot be known, and leaving it out would read as flat.
    /// Other request failures are `Connectivity` or `Api`, as for every Alpaca REST call.
    ///
    /// # Rate limit note
    ///
    /// `/v2/positions` is fetched on every call, since it holds both the crypto balances and the
    /// equity and option positions. When USD is requested too (the common startup case),
    /// `/v2/account` is fetched in parallel with it for ~100-300ms latency savings. Under rate
    /// pressure, both requests may hit 429 simultaneously and retry independently. Operators
    /// approaching Alpaca rate limits can request only non-USD assets to skip `/v2/account`.
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

        // URLs are extracted before the closures to avoid re-allocating on each retry attempt.
        let account_url = format!("{base}/v2/account");
        let positions_url = format!("{base}/v2/positions");
        let (account, positions): (Option<AlpacaAccount>, Vec<AlpacaPosition>) = tokio::try_join!(
            async {
                if wants_usd {
                    rest_with_retry(rl, || http.get(&account_url))
                        .await
                        .map(Some)
                } else {
                    Ok(None)
                }
            },
            rest_with_retry(rl, || http.get(&positions_url)),
        )?;

        let mut balances = account
            .map(|account| convert_account_to_balances(&account, assets))
            .unwrap_or_default();
        if wants_non_usd {
            balances.extend(convert_positions_to_balances(&positions, assets));
        }

        // Converted before the open orders are fetched, so a position that cannot be converted
        // fails the call without a wasted request.
        let converted_positions = convert_positions(&positions, Utc::now())?;
        let open_orders = fetch_raw_open_orders(&http, rl, base, instruments).await?;

        // Group open orders and positions by instrument symbol.
        let instrument_snapshots =
            build_instrument_snapshots(open_orders, converted_positions, instruments);
        {
            let mut known = self.known_live.lock();
            for order in instrument_snapshots
                .iter()
                .flat_map(|snapshot| &snapshot.orders)
            {
                if let OrderState::Active(ActiveOrderState::Open(open)) = &order.state {
                    known.live(&order.key, order.quantity, open);
                }
            }
        }

        Ok(AccountSnapshot::new(
            ExchangeId::AlpacaBroker,
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
    /// via the REST activities endpoint (anchored to `disconnect_time`). How an order held as live
    /// ended in this window is recovered too (see below); a lifecycle event of any other order,
    /// such as `new` for one placed elsewhere, is not.
    ///
    /// # Fill recovery ordering
    ///
    /// After a reconnect, missed fills are recovered from the REST activities endpoint
    /// using `direction=asc` to match the chronological order in which the WS stream
    /// advanced `filled_qty`. The dedup key `"{order_id}:{cum_qty}"` is taken from the
    /// activity's own `cum_qty`, so it matches the WS path by construction and does not
    /// depend on the order in which activities arrive.
    ///
    /// Only where Alpaca omits `cum_qty` does the key fall back to accumulating per-execution
    /// qty within the batch. That fallback counts from zero per order, so it is correct only for
    /// an order whose fills lie wholly inside the recovery window, and it is sensitive to
    /// activities arriving out of chronological order (e.g. at pagination boundaries).
    ///
    /// A recovered fill also carries the order's cumulative filled quantity, so it advances the
    /// order's `filled_quantity` without waiting for a `fetch_open_orders` reconciliation.
    ///
    /// Every fill's [`TradeId`] is Alpaca's execution id, whether the stream, recovery, or
    /// [`ExecutionClient::fetch_trades`] delivers it, so a fill read again by `fetch_trades`
    /// matches the one the stream delivered. A stream fill whose `execution_id` is missing, null
    /// or empty is logged and identified by `"{order_id}:{filled_qty}"` instead.
    ///
    /// The read is made once, with no retry. When it fails, times out, or stops at 5,000 fills,
    /// the stream sends one [`AccountEventKind::FillRecoveryGaveUp`] covering the stream's
    /// `instruments`, or every instrument when that list is empty. A truncated read delivers the
    /// fills it read first, and the event's span starts just before the last of them.
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
    /// # Orders that ended while disconnected
    ///
    /// A reconnect also reports how each order the client holds as live ended, where it did, as an
    /// [`AccountEventKind::OrderSnapshot`] of its inactive state: filled (with the average fill
    /// price), cancelled or replaced (as cancelled, with what filled before), expired, or rejected.
    /// Like fill recovery, it covers only the instruments the stream was opened with, or every
    /// instrument when that list is empty. Alpaca's closed-order list filters on when an order was
    /// submitted, so such an order is found by asking about it, not by time.
    ///
    /// - **Which orders.** The client holds an order as live from the response to placing it,
    ///   from a listing of open orders ([`account_snapshot`](ExecutionClient::account_snapshot),
    ///   [`fetch_open_orders`](ExecutionClient::fetch_open_orders)), and from its live reports on
    ///   any of its account streams, until it sees the order end; a cancel Alpaca has only
    ///   accepted does not end it. A bracket's take-profit and stop-loss legs carry client order
    ///   ids Alpaca assigns, so they are held only once a listing or the stream reports them. It
    ///   holds up to 4,096 orders and forgets the oldest past that, logged at `warn`. An order
    ///   placed outside this client and never listed or reported to it is not covered.
    /// - **Cost.** One `GET /v2/orders?status=open` request listing every instrument with an
    ///   order held, then one `GET /v2/orders:by_client_order_id` for each held order the listing
    ///   no longer shows, 8 at a time. These share the account's rate limit with orders, so after
    ///   an outage in which many held orders ended, the check can hold orders back until the
    ///   window resets (see [Rate limits](AlpacaClient#rate-limits)).
    /// - **Fills first.** The check starts once fill recovery has finished or given up, so an
    ///   order's recovered fills arrive before how it ended. It runs alongside the stream, which
    ///   is read from the start. A fill that brings an order to its full quantity ends it, and
    ///   that order is not reported again.
    /// - **Keys.** Each snapshot carries [`StrategyId::unknown`], since Alpaca records no
    ///   strategy. The engine matches it to the order it tracks by client order id.
    /// - **Failures.** Each order's lookup is settled as it ends. An instrument whose listing or
    ///   lookup fails, or whose check is still running after 30 s, is retried while connected 1,
    ///   2, 4, 8 and 16 minutes later, asking only about the orders still held, then given up,
    ///   logged at `error`; its orders are asked about again at the next reconnect. A listing that
    ///   fails charges every instrument in it. Alpaca lists at most 500 open orders and has no
    ///   pagination for them, so a listing of 500 may be truncated and fails: an account with 500
    ///   or more open orders on the held instruments is not covered. An order Alpaca does not know
    ///   (404) stops being held, logged at `warn`; one still live, `done_for_day`, or in a state
    ///   this version cannot read stays held.
    ///
    /// The same lookup is public as [`OrderStatusClient::fetch_ended_orders`].
    ///
    /// # Rejected orders
    ///
    /// Alpaca `rejected` events are delivered as `AccountEventKind::OrderCancelled` with
    /// `state: Err(OrderRejected(...))`. Match on `response.state.is_err()` to distinguish
    /// rejections from true cancels — do not call `.unwrap()` on `OrderCancelled.state`.
    ///
    /// # Orders done for the day
    ///
    /// A `done_for_day` event is delivered as an [`AccountEventKind::OrderSnapshot`] of the order
    /// as open, with what it has filled: Alpaca stops working the order until the next trading
    /// day but has not ended it.
    ///
    /// # Stream drop behaviour
    ///
    /// Dropping the returned `BoxStream` initiates a graceful shutdown of the
    /// background `connection_manager` task: the channel close causes the task
    /// to send a WebSocket close frame and exit within the current heartbeat
    /// window (≤60 s). Any `AccountEvent` items already queued but not yet
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
        let known = self.known_live.clone();
        let instruments = instruments.to_vec();

        let cm_handle = tokio::spawn(connection_manager(
            tx,
            dedup,
            config,
            http,
            rate_limiter,
            known,
            instruments,
            Some(initial_ws),
        ));

        let rx_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let guarded = GracefulShutdownStream::new(rx_stream, cm_handle);
        Ok(futures::StreamExt::boxed(guarded))
    }

    /// Cancel an order.
    ///
    /// # Async cancel semantics
    ///
    /// Returns `Ok(Cancelled)` once Alpaca has **accepted** the cancel, not once the order is
    /// cancelled: `DELETE /v2/orders/{id}` answers 204 when the cancel request is accepted and the
    /// order will be cancelled. Until it is, the order can be `pending_cancel`, still live, and can
    /// still fill. The account stream reports how it ended, and any fill.
    ///
    /// The 204 has no body, so `filled_quantity` is zero whatever the order filled, and
    /// `time_exchange` is the local time the answer arrived.
    ///
    /// The client keeps holding the order as live until the stream reports how it ended, so if the
    /// stream is down when it does, a reconnect still finds out (see
    /// [`account_stream`](ExecutionClient::account_stream)).
    ///
    /// Needs the venue order id. A request without one is refused, since Alpaca cancels by that
    /// id only; resolve it through [`ExecutionClient::fetch_open_orders`].
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
        let order_id: SmolStr = match request.state.id.as_ref().and_then(VenueOrderId::assigned) {
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
                // REST DELETE returns no response body, so the filled quantity is unknown
                // here; the account stream's `canceled` update reports it.
                Some(crate::order::request::OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(exchange_order_id, Utc::now(), None)),
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
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
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

        let result: Vec<_> = open_orders
            .into_iter()
            .filter_map(|o| convert_open_order(&o))
            .collect();
        self.remember_live(&result);
        Ok(result)
    }

    /// Reads at most 50 pages of FILL activities per call, 5,000 fills counted across the whole
    /// account before the instrument filter, and returns `resume` when it stops there. So a call for one instrument can return no trades with `resume: Some`.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        if start > end {
            return Ok(TradesRead::complete(Vec::new()));
        }
        let page = paginate_activities(
            &self.http,
            &self.rate_limiter,
            self.base_url(),
            start,
            Some(end),
        )
        .await?;

        // A resume past `end` means the read went beyond the span: nothing in it is left.
        let resume = page.resume.filter(|resume| *resume <= end);
        // A read from `resume` would stop where this one did, so it cannot advance.
        if resume.is_some_and(|resume| resume <= floor_millis(start)) {
            error!(
                %start,
                fills_read = page.activities.len(),
                "Alpaca fetch_trades: a full read did not advance past the span's first millisecond"
            );
            return Err(UnindexedClientError::Truncated {
                fills_read: page.activities.len(),
            });
        }

        // Empty instruments slice means "all instruments" — same convention as
        // fetch_open_orders.
        let instrument_set: fnv::FnvHashSet<&str> =
            instruments.iter().map(|i| i.name().as_str()).collect();
        let trades = page
            .activities
            .iter()
            .filter(|a| instrument_set.is_empty() || instrument_set.contains(a.symbol.as_str()))
            // Alpaca's bounds are not exact (see `paginate_activities`), so the span is applied
            // here. An activity whose time does not parse is kept: Alpaca placed it in the span.
            .filter(|a| {
                parse_timestamp(&a.transaction_time)
                    .is_none_or(|time| (start..=end).contains(&time))
            })
            .filter_map(convert_activity_to_trade)
            .collect();

        Ok(TradesRead::new(trades, resume))
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
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
        self.open_order_inner(request, intent).await
    }

    /// Place a bracket order (entry + take-profit + stop-loss) in a single request.
    ///
    /// A bracket order consists of three linked orders:
    /// 1. **Entry**: Limit order to enter the position
    /// 2. **Take Profit**: Limit order to exit at profit target
    /// 3. **Stop Loss**: Stop (or stop-limit) order to exit at loss limit
    ///
    /// When the entry order fills, Alpaca activates both exit legs. When either
    /// exit leg fills, Alpaca automatically cancels the other.
    ///
    /// # Constraints
    ///
    /// - `time_in_force` must be `Day` or `GoodUntilCancelled` (Alpaca rejects others)
    /// - No extended hours trading with bracket orders
    /// - Entry is always a limit order
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = AlpacaBracketOrderRequest::new(
    ///     "AAPL".into(),
    ///     StrategyId::new("momentum"),
    ///     ClientOrderId::new("bracket-001"),
    ///     Side::Buy,
    ///     dec!(10),
    ///     dec!(150.00),  // entry
    ///     dec!(160.00),  // take profit
    ///     dec!(145.00),  // stop loss
    ///     TimeInForce::GoodUntilCancelled { post_only: false },
    /// );
    /// let result = client.open_bracket_order(request).await;
    /// ```
    pub async fn open_bracket_order(
        &self,
        request: AlpacaBracketOrderRequest,
    ) -> AlpacaBracketOrderResult {
        let order_key = crate::order::OrderKey::new(
            ExchangeId::AlpacaBroker,
            request.instrument.clone(),
            request.strategy.clone(),
            request.cid.clone(),
        );

        // Validate time_in_force: bracket orders only support day or gtc
        let tif_str = match request.time_in_force {
            TimeInForce::GoodUntilEndOfDay => "day",
            TimeInForce::GoodUntilCancelled { post_only } => {
                if post_only {
                    return AlpacaBracketOrderResult {
                        parent: Order {
                            key: order_key,
                            side: request.side,
                            price: Some(request.entry_price),
                            quantity: request.quantity,
                            kind: OrderKind::Limit,
                            time_in_force: request.time_in_force,
                            state: OrderState::inactive(OrderError::Rejected(
                                ApiError::OrderRejected(
                                    "Alpaca does not support post_only for bracket orders"
                                        .to_string(),
                                ),
                            )),
                        },
                    };
                }
                "gtc"
            }
            other => {
                return AlpacaBracketOrderResult {
                    parent: Order {
                        key: order_key,
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                            format!(
                                "Alpaca bracket orders only support Day or GTC time_in_force, got {:?}",
                                other
                            ),
                        ))),
                    },
                };
            }
        };

        // Validate price ordering. Alpaca rejects mis-ordered brackets with a
        // generic 422; surface a structured local error before round-trip.
        let price_ordering_ok = match request.side {
            Side::Buy => {
                request.stop_loss_price < request.entry_price
                    && request.entry_price < request.take_profit_price
            }
            Side::Sell => {
                request.take_profit_price < request.entry_price
                    && request.entry_price < request.stop_loss_price
            }
        };
        if !price_ordering_ok {
            return AlpacaBracketOrderResult {
                parent: Order {
                    key: order_key,
                    side: request.side,
                    price: Some(request.entry_price),
                    quantity: request.quantity,
                    kind: OrderKind::Limit,
                    time_in_force: request.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        format!(
                            "Invalid bracket price ordering for {:?} side: entry={}, take_profit={}, stop_loss={}",
                            request.side,
                            request.entry_price,
                            request.take_profit_price,
                            request.stop_loss_price,
                        ),
                    ))),
                },
            };
        }

        // Validate stop-loss limit price if provided. For a Buy bracket, the SL
        // is a sell stop(-limit); the limit must be at or below the trigger so
        // the order can fill if price gaps down. Reverse for Sell brackets.
        if let Some(sl_limit) = request.stop_loss_limit_price {
            let sl_limit_ok = match request.side {
                Side::Buy => sl_limit <= request.stop_loss_price,
                Side::Sell => sl_limit >= request.stop_loss_price,
            };
            if !sl_limit_ok {
                return AlpacaBracketOrderResult {
                    parent: Order {
                        key: order_key,
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                            format!(
                                "Invalid stop-loss limit price for {:?} bracket: \
                                 stop_loss_price={}, stop_loss_limit_price={}",
                                request.side, request.stop_loss_price, sl_limit,
                            ),
                        ))),
                    },
                };
            }
        }

        // Build the bracket order request
        let body = AlpacaOrderRequest {
            symbol: request.instrument.name().as_str(),
            qty: request.quantity.to_string(),
            side: map_side(request.side),
            order_type: "limit",
            time_in_force: tif_str,
            limit_price: Some(request.entry_price.to_string()),
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            client_order_id: Some(request.cid.0.as_str()),
            position_intent: if is_options_or_equity_symbol(request.instrument.name().as_str()) {
                Some(map_position_intent(request.side, false))
            } else {
                None
            },
            order_class: Some("bracket"),
            take_profit: Some(TakeProfitParams {
                limit_price: request.take_profit_price.to_string(),
            }),
            stop_loss: Some(StopLossParams {
                stop_price: request.stop_loss_price.to_string(),
                limit_price: request.stop_loss_limit_price.map(|p| p.to_string()),
            }),
        };

        let http = self.http.clone();
        let rl = &self.rate_limiter;

        let result: Result<AlpacaOrderResponse, UnindexedClientError> =
            rest_with_retry(rl, || http.post(&self.orders_url).json(&body)).await;

        match result {
            Ok(resp) => {
                let state = placed_order_state(&resp, &order_key.instrument, request.quantity);
                self.known_live
                    .lock()
                    .placed(&order_key, request.quantity, &state);

                AlpacaBracketOrderResult {
                    parent: Order {
                        key: order_key,
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state,
                    },
                }
            }
            Err(e) => {
                let order_err = order_post_error(e);
                AlpacaBracketOrderResult {
                    parent: Order {
                        key: order_key,
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::inactive(order_err),
                    },
                }
            }
        }
    }

    async fn open_order_inner(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        intent: AlpacaPositionIntent,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
        let instrument = request.key.instrument.clone();
        let side = request.state.side;
        let price = request.state.price;
        let quantity = request.state.quantity;
        let kind = request.state.kind;
        let time_in_force = request.state.time_in_force;
        let cid = request.key.cid.clone();

        let order_key = crate::order::OrderKey::new(
            ExchangeId::AlpacaBroker,
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        msg.to_string(),
                    ))),
                });
            }
        };

        // Validate order kind — reject unsupported types early.
        let order_type_str = match map_order_kind(kind) {
            Some(s) => s,
            None => {
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: OrderState::inactive(OrderError::UnsupportedOrderType(format!(
                        "Alpaca connector does not yet support OrderKind::{kind:?}"
                    ))),
                });
            }
        };

        // Validate trailing stop offset type — Alpaca only supports percent and absolute.
        if let OrderKind::TrailingStop {
            offset_type: TrailingOffsetType::BasisPoints,
            ..
        } = kind
        {
            return Some(Order {
                key: order_key,
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state: OrderState::inactive(OrderError::UnsupportedOrderType(
                    "Alpaca does not support TrailingOffsetType::BasisPoints; \
                     use Percentage or Absolute"
                        .to_string(),
                )),
            });
        }

        // StopLimit requires Order.price (the limit price applied once trigger fires).
        // Guard locally to avoid a wire round-trip producing a generic 422.
        if matches!(kind, OrderKind::StopLimit { .. }) && price.is_none() {
            return Some(Order {
                key: order_key,
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                    "StopLimit order requires Order.price (the limit price) to be set".to_string(),
                ))),
            });
        }

        // Extract stop/trailing parameters based on order kind.
        let (stop_price, trail_percent, trail_price) = match kind {
            OrderKind::Stop { trigger_price } | OrderKind::StopLimit { trigger_price } => {
                (Some(trigger_price.to_string()), None, None)
            }
            OrderKind::TrailingStop {
                offset,
                offset_type,
            } => match offset_type {
                TrailingOffsetType::Percentage => (None, Some(offset.to_string()), None),
                TrailingOffsetType::Absolute => (None, None, Some(offset.to_string())),
                TrailingOffsetType::BasisPoints => unreachable!("validated above"),
            },
            // TakeProfit/TakeProfitLimit rejected by map_order_kind → unreachable here
            OrderKind::Market
            | OrderKind::Limit
            | OrderKind::TakeProfit { .. }
            | OrderKind::TakeProfitLimit { .. }
            | OrderKind::TrailingStopLimit { .. } => (None, None, None),
        };

        let body = AlpacaOrderRequest {
            symbol: instrument.name().as_str(),
            qty: quantity.to_string(),
            side: map_side(side),
            order_type: order_type_str,
            time_in_force: tif_str,
            limit_price: match kind {
                OrderKind::Limit | OrderKind::StopLimit { .. } => price.map(|p| p.to_string()),
                _ => None,
            },
            stop_price,
            trail_percent,
            trail_price,
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
            // Non-bracket order: no bracket fields
            order_class: None,
            take_profit: None,
            stop_loss: None,
        };

        let http = self.http.clone();
        let rl = &self.rate_limiter;

        let result: Result<AlpacaOrderResponse, UnindexedClientError> =
            rest_with_retry(rl, || http.post(&self.orders_url).json(&body)).await;

        match result {
            Ok(resp) => {
                let state = placed_order_state(&resp, &order_key.instrument, quantity);
                self.known_live.lock().placed(&order_key, quantity, &state);

                Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state,
                })
            }
            Err(e) => {
                let order_err = order_post_error(e);
                Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: OrderState::inactive(order_err),
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

/// Maximum number of pages fetched by one [`paginate_activities`] call.
///
/// 50 pages × 100 items = 5 000 fills. It bounds how long one read, such as a reconnect's
/// recovery, can take; a read that reaches it says where to read on from.
const MAX_ACTIVITY_PAGES: usize = 50;

/// What one [`paginate_activities`] call read.
struct ActivityPage {
    /// Every FILL activity read, in Alpaca's ascending order.
    activities: Vec<AlpacaActivity>,
    /// Set when the read stopped at [`MAX_ACTIVITY_PAGES`]: every activity before this time was
    /// read, and some from it on may not have been.
    resume: Option<DateTime<Utc>>,
}

/// `time` rounded down to the millisecond.
fn floor_millis(time: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(time.timestamp_millis()).unwrap_or(time)
}

/// Fetch the FILL activities from `start`, and up to `end` if given, using token-based
/// pagination.
///
/// Alpaca returns up to `ALPACA_MAX_ACTIVITIES` per page. If a full page is
/// returned, the next request uses the last item's `id` as the `page_token`.
/// Pagination terminates when a page has fewer items than `page_size`, or after
/// [`MAX_ACTIVITY_PAGES`] pages (whichever comes first).
///
/// # Bounds
///
/// Alpaca's `after` and `until` are not exact. Observed on paper (2026-10-05):
/// - `after` matches an activity whose time, rounded down to the millisecond, is at or after it.
///   So a microsecond `after` drops the rest of its own millisecond. It is sent rounded down to
///   the millisecond, which reads the whole of `start`'s.
/// - `until` matched no single rule: exact to the microsecond for one fill, yet including a fill
///   15 µs past it for two that shared a millisecond. It is sent as the millisecond after
///   `end`'s, and callers apply `end` to what is read.
///
/// So the activities read can begin before `start` and end after `end`, within their
/// milliseconds.
///
/// # Resuming
///
/// Within one millisecond Alpaca orders activities by id, not by time, so a read cut inside a
/// millisecond may not have reached an earlier fill in it. [`ActivityPage::resume`] is therefore
/// the millisecond of the last activity read, and a read from it reads that millisecond again.
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
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
) -> Result<ActivityPage, UnindexedClientError> {
    let after = floor_millis(start).to_rfc3339_opts(SecondsFormat::Millis, true);
    let until = end.map(|end| {
        (floor_millis(end) + TimeDelta::milliseconds(1))
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    });
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
                ("after", after.as_str()),
                ("page_size", PAGE_SIZE_STR),
                ("direction", "asc"),
            ]);
            if let Some(until) = until.as_deref() {
                req = req.query(&[("until", until)]);
            }
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

    // Activities are ascending by millisecond, so the last one whose time parses bounds what is
    // left. None parsing leaves everything from `start`.
    let resume = truncated.then(|| {
        floor_millis(
            all.iter()
                .rev()
                .find_map(|activity| parse_timestamp(&activity.transaction_time))
                .unwrap_or(start),
        )
    });
    Ok(ActivityPage {
        activities: all,
        resume,
    })
}

// ---------------------------------------------------------------------------
// WebSocket connection manager
// ---------------------------------------------------------------------------

/// Long-running task managing the WebSocket lifecycle for account_stream.
///
/// Loop: connect → auth → subscribe → fill recovery → ended-order check → stream events → on
/// disconnect → backoff → reconnect. The `tx` channel persists across reconnections so the
/// consumer sees a seamless event stream.
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
    known: SharedKnownLiveOrders,
    instruments: Vec<InstrumentNameExchange>,
    initial_ws: Option<WebSocket>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut disconnect_time: Option<DateTime<Utc>> = None;
    // Instruments whose known-live orders a reconnect has yet to check: checked after the fills,
    // and retried on a timer while connected, once due.
    let mut unchecked = UncheckedOrders::default();
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
                        // Signal terminal stream death in-band before tx drops.
                        emit_stream_terminated(
                            &tx,
                            ExchangeId::AlpacaBroker,
                            StreamTerminationReason::ReconnectBudgetExhausted {
                                attempts: backoff.attempts(),
                                last_error: e.to_string(),
                            },
                        );
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
            recover_fills_or_report(
                &http,
                &rate_limiter,
                &instruments,
                config.rest_base_url(),
                dt,
                Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS),
                &tx,
                &dedup,
                &known,
            )
            .await;
            // Only the instruments this stream recovers fills for, every one when none is named.
            let held = {
                let known = known.lock();
                if instruments.is_empty() {
                    known.instruments()
                } else {
                    known.instruments_among(&instruments)
                }
            };
            unchecked.open(held);
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

        // How the orders held as live ended: checked alongside the stream, so it is read, and a
        // disconnect seen, from the start. Fill recovery has finished or given up above, so the
        // fills come first. Dropping this when the stream loop ends loses nothing, since each
        // lookup and each check is settled in one step as soon as it ends.
        let order_checks =
            run_order_checks(&http, &rate_limiter, &config, &known, &mut unchecked, &tx);
        tokio::pin!(order_checks);

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
        // Distinguishes why the inner stream loop exited so the terminal emit below can report a
        // meaningful `last_error`. The consumer-drop arm uses `break 'outer` and never yields one.
        enum DisconnectReason {
            ServerClose,
            Error(String),
            StreamEnded,
            HeartbeatTimeout,
        }
        let reason = loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(WsMessage::Ping(_))) => {
                            // tokio-tungstenite automatically queues a Pong when poll_next
                            // returns a Ping; sending a second Pong would be a duplicate.
                            reset_heartbeat!();
                        }
                        Some(Ok(WsMessage::Text(text))) => {
                            process_ws_text(text.as_str(), &tx, &dedup, &known, &mut backoff);
                            reset_heartbeat!();
                        }
                        Some(Ok(WsMessage::Binary(bytes))) => {
                            // Alpaca paper trading sends binary-framed JSON.
                            match std::str::from_utf8(&bytes) {
                                Ok(text) => {
                                    process_ws_text(text, &tx, &dedup, &known, &mut backoff);
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
                            break DisconnectReason::ServerClose;
                        }
                        Some(Ok(_)) => {} // Pong, Frame — ignore
                        Some(Err(e)) => {
                            warn!(%e, "Alpaca WS error, reconnecting");
                            break DisconnectReason::Error(e.to_string());
                        }
                        None => {
                            warn!("Alpaca WS stream ended, reconnecting");
                            break DisconnectReason::StreamEnded;
                        }
                    }
                }
                () = &mut order_checks => {}
                _ = &mut heartbeat => {
                    warn!(
                        timeout_secs = HEARTBEAT_TIMEOUT_SECS,
                        "Alpaca heartbeat timeout, reconnecting"
                    );
                    // Heartbeat timeout is a failure — do NOT reset backoff here.
                    // Backoff resets on successful event receipt in process_ws_text.
                    break DisconnectReason::HeartbeatTimeout;
                }
                _ = tx.closed() => {
                    debug!("Alpaca account_stream consumer dropped, terminating");
                    let _ = tokio::time::timeout(
                        Duration::from_secs(WS_CLOSE_TIMEOUT_SECS),
                        ws.close(None),
                    ).await;
                    // Consumer dropped the receiver — no StreamTerminated emit (channel already
                    // closed, so it would be a no-op; see emit_stream_terminated docs).
                    break 'outer;
                }
            }
        };

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
            // Consumer dropped the receiver — no StreamTerminated emit (channel already closed,
            // so it would be a no-op; see emit_stream_terminated docs).
            break;
        }
        if !backoff.wait().await {
            error!("Alpaca max reconnect attempts exhausted, stream terminating");
            // Surrender after repeated reconnects: report the failure that triggered this round
            // (consumer-drop already broke out via `break 'outer`).
            let last_error = match reason {
                DisconnectReason::HeartbeatTimeout => {
                    format!("heartbeat timeout ({HEARTBEAT_TIMEOUT_SECS}s)")
                }
                DisconnectReason::ServerClose => "WebSocket closed by server".to_string(),
                DisconnectReason::Error(e) => e,
                DisconnectReason::StreamEnded => "WebSocket stream ended".to_string(),
            };
            emit_stream_terminated(
                &tx,
                ExchangeId::AlpacaBroker,
                StreamTerminationReason::ReconnectBudgetExhausted {
                    attempts: backoff.attempts(),
                    last_error,
                },
            );
            break;
        }
    }
}

/// Error during WebSocket handshake (auth + subscribe).
///
/// Separates transport errors (network issues, connection drops) from auth errors
/// (invalid credentials) so callers can apply appropriate retry logic.
#[derive(Debug)]
enum HandshakeError {
    /// Network-level failure (WS send failed, connection dropped).
    /// Transient — retry with backoff.
    Transport(String),
    /// Authentication rejected by Alpaca (invalid/expired credentials).
    /// Not transient — do not retry without credential changes.
    Auth(String),
}

/// Connect to the Alpaca WebSocket, authenticate, and subscribe to trade_updates.
///
/// On auth or subscribe failure the connection is closed cleanly.
async fn connect_and_subscribe(config: &AlpacaConfig) -> Result<WebSocket, UnindexedClientError> {
    let url = config.ws_url();
    debug!(%url, "Alpaca: connecting to WebSocket");

    let mut ws = rustrade_integration::protocol::websocket::connect(url)
        .await
        .map_err(|e| {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(format!(
                "WS connect: {e}"
            )))
        })?;

    // auth + subscribe with overall timeout
    let result = tokio::time::timeout(
        Duration::from_secs(WS_HANDSHAKE_TIMEOUT_SECS),
        ws_handshake(&mut ws, config),
    )
    .await;

    match result {
        Ok(Ok(())) => Ok(ws),
        Ok(Err(HandshakeError::Transport(e))) => {
            let _ = ws.close(None).await;
            Err(UnindexedClientError::Connectivity(
                ConnectivityError::Socket(e),
            ))
        }
        Ok(Err(HandshakeError::Auth(e))) => {
            let _ = ws.close(None).await;
            Err(UnindexedClientError::Api(ApiError::Unauthenticated(e)))
        }
        Err(_) => {
            let _ = ws.close(None).await;
            Err(UnindexedClientError::Connectivity(
                ConnectivityError::Timeout,
            ))
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
async fn ws_handshake(ws: &mut WebSocket, config: &AlpacaConfig) -> Result<(), HandshakeError> {
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
        .map_err(|e| HandshakeError::Transport(format!("WS auth send: {e}")))?;

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
            Some(Err(e)) => {
                return Err(HandshakeError::Transport(format!(
                    "WS error during auth: {e}"
                )));
            }
            None => {
                return Err(HandshakeError::Transport(
                    "WS closed before auth response".into(),
                ));
            }
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
        .map_err(|e| HandshakeError::Transport(format!("WS subscribe send: {e}")))?;

    // Step 4: wait for listening acknowledgment (optional but confirms subscription)
    loop {
        match ws.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                if check_listen_ack(text.as_str()) {
                    break;
                }
                // Other messages in this window are NOT buffered and are permanently
                // dropped. On a reconnect, REST recovers the fills and how each order held
                // as live ended; any other lifecycle event is lost, as documented in
                // account_stream's doc comment.
                // Log at warn! for trade_updates (fill/lifecycle events) to ensure
                // production operators see dropped events; trace! for other streams.
                if let Ok(msg) = serde_json::from_str::<AlpacaStreamMessage<'_>>(text.as_str()) {
                    if msg.stream == "trade_updates" {
                        warn!(stream = %msg.stream, "WS trade_updates event dropped during listen-ack handshake — on a reconnect, fills and how orders held as live ended are recovered via REST; other lifecycle events are lost");
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
            Some(Err(e)) => {
                return Err(HandshakeError::Transport(format!(
                    "WS error during subscribe: {e}"
                )));
            }
            None => {
                return Err(HandshakeError::Transport(
                    "WS closed before subscribe ack".into(),
                ));
            }
            _ => {}
        }
    }

    info!("Alpaca WS authenticated and subscribed to trade_updates");
    Ok(())
}

/// Parse a WS message to check for authorization response.
/// Returns `None` if the message is not an authorization response.
/// Returns `Some(Ok(()))` on success, `Some(Err(HandshakeError::Auth(...)))` on auth failure.
///
/// Uses `AlpacaStreamMessage` for consistency with the rest of the WS parsing pipeline.
fn check_auth_response(text: &str) -> Option<Result<(), HandshakeError>> {
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
        Some(Err(HandshakeError::Auth(format!(
            "Alpaca WS auth failed: status={}",
            data.status
        ))))
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
///
/// Each event is applied to `known` before it is sent, under the lock, so an order this reports
/// ending and a reconnect's check of it reach the consumer in the order they were decided, and the
/// check reports only an order not already reported.
fn process_ws_text(
    text: &str,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    known: &SharedKnownLiveOrders,
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

            for event in convert_trade_update(update).into_iter().flatten() {
                let known = KnownLiveOrders::observes(&event.kind).then(|| {
                    let mut known = known.lock();
                    known.observe(&event);
                    known
                });
                // Consumer dropped errors are benign; connection_manager will detect
                // tx.closed() on the next select! poll and exit cleanly.
                let _ = tx.send(event);
                drop(known);
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

/// The dedup key for a fill that took order `order_id` to `cumulative` filled.
///
/// The account stream and fill recovery both build it, from the cumulative each reports for the
/// order (the WS frame's `order.filled_qty`, the FILL activity's `cum_qty`), so one fill
/// delivered by both is recognised. It is kept apart from the fill's [`TradeId`], the venue's
/// execution id: if the two paths ever disagreed on that id, fills would still not be delivered
/// twice. `normalize` strips trailing zeros, so `"1.00"` and `"1"` give one key. A WS fill whose
/// `filled_qty` does not parse is keyed at zero, so a second such fill on the same order is taken
/// for a duplicate even though its execution id differs; `convert_trade_update` warns of it.
///
/// The `format_smolstr!` call heap-allocates for UUID-length order ids (36 chars exceeds
/// SmolStr's inline limit), which is unavoidable given the key length.
fn fill_dedup_key(order_id: &str, cumulative: Decimal) -> SmolStr {
    format_smolstr!("{}:{}", order_id, cumulative.normalize())
}

/// Returns `true` if this event type produces a fill (Trade) event.
///
/// Used for early dedup check before allocating the full event.
#[inline]
fn is_fill_event(update: &AlpacaTradeUpdate<'_>) -> bool {
    matches!(update.event.as_str(), "fill" | "partial_fill")
}

/// The [`fill_dedup_key`] of a WS fill, from its raw fields, before the full event is built.
///
/// An unparseable `filled_qty` reads as zero, as `convert_trade_update` reports it.
fn early_dedup_key(update: &AlpacaTradeUpdate<'_>) -> SmolStr {
    let filled_qty = update.order.filled_qty.unwrap_or("0");
    let qty = Decimal::from_str(filled_qty).unwrap_or(Decimal::ZERO);
    fill_dedup_key(&update.order.id, qty)
}

/// The execution id in a FILL activity's `id`, `"{time}::{execution id}"`: the same id the WS
/// fill carries as `execution_id`. The time prefix is US Eastern local time, so it is never read.
/// An id without `::`, or with nothing after it, is taken whole, so it stays unique.
fn activity_execution_id(activity_id: &str) -> &str {
    activity_id
        .split_once("::")
        .map(|(_, execution_id)| execution_id)
        .filter(|execution_id| !execution_id.is_empty())
        .unwrap_or(activity_id)
}

// ---------------------------------------------------------------------------
// Fill recovery
// ---------------------------------------------------------------------------

/// Fills a recovery read did not deliver: those from `start` until the recovery began.
#[derive(Debug, PartialEq)]
struct UnreadFills {
    start: DateTime<Utc>,
    reason: FillRecoveryFailure,
}

/// Recover the fills missed since `disconnect` ([`recover_fills`]) within `timeout`, and send an
/// [`AccountEventKind::FillRecoveryGaveUp`] for those the read did not deliver, after the fills
/// it did.
async fn recover_fills_or_report(
    http: &reqwest::Client,
    rate_limiter: &RateLimitTracker,
    instruments: &[InstrumentNameExchange],
    base: &str,
    disconnect: DateTime<Utc>,
    timeout: Duration,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    known: &SharedKnownLiveOrders,
) {
    // The live stream is already subscribed, so every fill after this arrives live.
    let recovery_start = Utc::now();
    let outcome = match tokio::time::timeout(
        timeout,
        recover_fills(
            http,
            rate_limiter,
            instruments,
            base,
            disconnect,
            tx,
            dedup,
            known,
        ),
    )
    .await
    {
        Ok(outcome) => outcome,
        // One account-wide activities query serves every instrument, so a timeout can have missed
        // fills in any of them: name the requested set. Nothing was forwarded, since recovery
        // sends its fills only after the read has finished.
        Err(_) => {
            warn!(
                timeout_secs = timeout.as_secs(),
                instruments = ?instruments
                    .iter()
                    .map(|instrument| instrument.name().as_str())
                    .collect::<Vec<_>>(),
                "Alpaca fill recovery timed out — fills since the disconnect may be missing for \
                 any of the instruments (every instrument when the list is empty)"
            );
            Err(UnreadFills {
                start: disconnect,
                reason: FillRecoveryFailure::TimedOut {
                    timeout_secs: timeout.as_secs(),
                },
            })
        }
    };
    if let Err(unread) = outcome
        && tx
            .send(fill_recovery_gave_up(instruments, unread, recovery_start))
            .is_err()
    {
        debug!("Alpaca fill recovery: consumer dropped before the give-up was sent");
    }
}

/// The [`AccountEventKind::FillRecoveryGaveUp`] for `unread`, whose recovery began at `end`,
/// on a stream opened with `instruments`, every instrument when empty. Alpaca does not retry the
/// read, so it was read once.
///
/// A truncated read can return fills stamped after `end`, by Alpaca's clock, which may run ahead
/// of this host's. The span then ends at its start rather than before it: the event is still
/// sent, since a report with nothing left to read costs the consumer one read, while a report
/// withheld on a clock comparison would be a silent loss.
fn fill_recovery_gave_up(
    instruments: &[InstrumentNameExchange],
    unread: UnreadFills,
    end: DateTime<Utc>,
) -> UnindexedAccountEvent {
    let UnreadFills { start, reason } = unread;
    // One account-wide read serves every instrument, so one event covers them all.
    let scope = if instruments.is_empty() {
        FillRecoveryScope::AllInstruments
    } else {
        FillRecoveryScope::Instruments(instruments.to_vec())
    };
    UnindexedAccountEvent::new(
        ExchangeId::AlpacaBroker,
        AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
            scope,
            start,
            end.max(start),
            1,
            reason,
        )),
    )
}

/// Fetch fills missed during a WS disconnect, since `after`, and forward through the dedup cache.
///
/// A fill that brings an order to its full quantity ends it in `known`, so a reconnect's check
/// does not report it again.
///
/// Returns the fills it did not deliver: all of them when the read fails, and those from the
/// millisecond of the last one read when the read stops at [`MAX_ACTIVITY_PAGES`]. The fills it read are sent
/// first. A consumer that drops the stream part-way gets `Ok`, as there is no one to report to.
async fn recover_fills(
    http: &reqwest::Client,
    rate_limiter: &RateLimitTracker,
    instruments: &[InstrumentNameExchange],
    base: &str,
    after: DateTime<Utc>,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    known: &SharedKnownLiveOrders,
) -> Result<(), UnreadFills> {
    // Empty `instruments` means "recover for all subscribed symbols" (same convention as
    // `fetch_trades` / `fetch_open_orders`). Skip the set allocation when not filtering.
    info!(%after, instruments = instruments.len(), "Alpaca recovering fills after reconnect");
    let instrument_set: fnv::FnvHashSet<&str> = if instruments.is_empty() {
        fnv::FnvHashSet::default()
    } else {
        instruments.iter().map(|i| i.name().as_str()).collect()
    };

    let page = match paginate_activities(http, rate_limiter, base, after, None).await {
        Ok(p) => p,
        Err(e) => {
            error!(%e, "Alpaca fill recovery: REST request failed");
            return Err(UnreadFills {
                start: after,
                reason: FillRecoveryFailure::Request(e.to_string()),
            });
        }
    };

    let activities = page.activities;

    // The read is account-wide, so one that stopped at the page cap has every fill before its
    // resume point, whatever the instrument: the millisecond of its last fill, which a read from
    // there reads again. The dedup cache absorbs the fills read twice.
    let unread = page.resume.map(|start| {
        error!(
            max_pages = MAX_ACTIVITY_PAGES,
            fills_read = activities.len(),
            %start,
            "Alpaca fill recovery: max page limit reached, truncating — fills from this time on \
             are not delivered; read them with fetch_trades"
        );
        UnreadFills {
            start,
            reason: FillRecoveryFailure::Truncated {
                fills_read: activities.len(),
            },
        }
    });

    let mut recovered = 0u32;
    let mut duplicates = 0u32;

    // Fallback only. The dedup key is "{order_id}:{cumulative_filled_qty}" on both paths, and the
    // cumulative is now taken from the activity's own `cum_qty` -- the same figure the WS path
    // reads from `order.filled_qty` -- so the two agree by construction rather than by
    // reconstruction.
    //
    // This counter is what the key falls back to when `cum_qty` is absent, which reproduces the
    // previous behaviour exactly. That behaviour is correct only for an order whose fills lie
    // entirely inside the recovery window: it counts from zero per order within the batch, so an
    // order that had already partly filled before the window yields keys offset by the amount it
    // filled earlier, and those keys match nothing the WS path ever emitted. Preferring `cum_qty`
    // is what removes that failure mode.
    //
    // Activities are fetched with direction=asc, so accumulation here follows execution order.
    // Borrow `activities` so &str keys into order_id strings remain valid for the entire loop:
    // consuming iteration would drop each activity at the end of its iteration, invalidating keys
    // that later fills on the same order still need to look up.
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

        let trade = match convert_activity_to_trade(activity) {
            Some(t) => t,
            None => {
                warn!(id = %activity.id, symbol = %activity.symbol, "Alpaca: skipping activity with unparseable fields");
                continue; // Counter already advanced; dedup key sequence stays aligned with WS.
            }
        };

        // The dedup key, which matches the WS path's for the same fill. `order_filled_quantity`
        // holds the venue's own cumulative, parsed by `convert_activity_to_trade`; `cumulative`
        // is the intra-batch fallback described above. The trade keeps its execution id.
        //
        // The counter is advanced for every activity, including those that carry `cum_qty` and so
        // never consult it. That is deliberate: it keeps the fallback usable for a later activity
        // on the same order that omits the field. A batch mixing both therefore interleaves two
        // key schemes, and the fallback keys in it remain offset by whatever the order filled
        // before the window -- the field's presence rescues an activity, not an order.
        let key = fill_dedup_key(
            &activity.order_id,
            trade.order_filled_quantity.unwrap_or(cumulative),
        );
        if is_duplicate(dedup, &key) {
            duplicates += 1;
            continue;
        }

        let event =
            UnindexedAccountEvent::new(ExchangeId::AlpacaBroker, AccountEventKind::Trade(trade));
        let mut held = known.lock();
        held.observe(&event);
        let sent = tx.send(event);
        drop(held);
        if sent.is_err() {
            debug!("Alpaca fill recovery: consumer dropped during recovery");
            return Ok(());
        }
        recovered += 1;
    }

    info!(recovered, duplicates, "Alpaca fill recovery complete");
    unread.map_or(Ok(()), Err)
}

// ---------------------------------------------------------------------------
// Ended-order recovery
// ---------------------------------------------------------------------------

/// Look the order under `key` up with `GET /v2/orders:by_client_order_id`.
///
/// A 404 is [`OrderLookup::Unknown`], and so is an order on another symbol than the key's (ignoring
/// case, as Alpaca's symbols are upper case).
async fn fetch_order_lookup(
    http: reqwest::Client,
    rate_limiter: Arc<RateLimitTracker>,
    config: Arc<AlpacaConfig>,
    key: UnindexedOrderKey,
) -> Result<OrderLookup, UnindexedClientError> {
    let url = format!("{}/v2/orders:by_client_order_id", config.rest_base_url());
    let cid = key.cid.0.as_str();
    let found: Option<AlpacaOrderResponse> = rest_lookup_with_retry(&rate_limiter, || {
        http.get(&url).query(&[("client_order_id", cid)])
    })
    .await?;
    let Some(order) = found else {
        debug!(instrument = %key.instrument, cid = %key.cid, "Alpaca does not know this order");
        return Ok(OrderLookup::Unknown);
    };
    if !order
        .symbol
        .eq_ignore_ascii_case(key.instrument.name().as_str())
    {
        debug!(
            instrument = %key.instrument,
            cid = %key.cid,
            symbol = %order.symbol,
            "Alpaca knows this client order id on another symbol"
        );
        return Ok(OrderLookup::Unknown);
    }
    Ok(
        convert_ended_order(&order, &key).map_or(OrderLookup::NotEnded, |order| {
            OrderLookup::Ended(Box::new(order))
        }),
    )
}

/// The client order ids one `GET /v2/orders?status=open` lists on `instruments`, for a
/// reconnect's check of the orders held as live. A listing of 500 may be truncated, so it fails.
async fn listed_open_cids(
    http: reqwest::Client,
    rate_limiter: Arc<RateLimitTracker>,
    config: Arc<AlpacaConfig>,
    instruments: Vec<InstrumentNameExchange>,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let orders =
        fetch_raw_open_orders(&http, &rate_limiter, config.rest_base_url(), &instruments).await?;
    Ok(orders
        .iter()
        .map(|order| alpaca_cid(order.client_order_id.as_deref(), &order.id))
        .collect())
}

/// [`recover_ended_orders`] on Alpaca: one listing of every instrument due by
/// [`listed_open_cids`], and lookups by [`fetch_order_lookup`]. Fill recovery has finished or been
/// given up before it runs, so no fills are pending.
async fn recover_alpaca_ended_orders(
    http: &reqwest::Client,
    rate_limiter: &Arc<RateLimitTracker>,
    config: &Arc<AlpacaConfig>,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) {
    recover_ended_orders(
        ExchangeId::AlpacaBroker,
        known,
        unchecked,
        &NoPendingFills,
        tx,
        OpenListing::Batched,
        |instruments| {
            listed_open_cids(
                http.clone(),
                rate_limiter.clone(),
                config.clone(),
                instruments,
            )
        },
        |key| fetch_order_lookup(http.clone(), rate_limiter.clone(), config.clone(), key),
    )
    .await;
}

/// Run every order check as it falls due, the first at once and a failed one after its backoff,
/// for as long as it is polled. It never completes, and once the consumer has gone it only waits.
///
/// Fill recovery has finished or been given up before this starts, so no fills are pending.
async fn run_order_checks(
    http: &reqwest::Client,
    rate_limiter: &Arc<RateLimitTracker>,
    config: &Arc<AlpacaConfig>,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) {
    loop {
        // A check stops at a gone consumer before settling, so its instruments stay due.
        if tx.is_closed() {
            return std::future::pending().await;
        }
        match unchecked.next_due(&NoPendingFills) {
            Some(due) => tokio::time::sleep_until(due).await,
            None => return std::future::pending().await,
        }
        recover_alpaca_ended_orders(http, rate_limiter, config, known, unchecked, tx).await;
    }
}

/// How an order that has ended did end, from its REST order under `key`, the key it was asked for
/// (see [`ended_order_state`]).
///
/// Returns `None` for an order still live (see [`order_status_is_live`]), including
/// `done_for_day`, and, with a warning, for an order whose status is missing or unknown or that
/// cannot be converted. A caller reads `None` as "not ended", so such an order is asked about again
/// later rather than retired on a guess.
fn convert_ended_order(
    o: &AlpacaOrderResponse,
    key: &UnindexedOrderKey,
) -> Option<UnindexedInactiveOrder> {
    let instrument = &key.instrument;
    let Some(status) = o.status.as_deref() else {
        warn!(%instrument, cid = %key.cid, order_id = %o.id, "Alpaca order missing status");
        return None;
    };
    if order_status_is_live(status) {
        return None;
    }
    let Some(order) = convert_representable_open_order(o) else {
        warn!(%instrument, cid = %key.cid, order_id = %o.id, status, "Alpaca order cannot be represented, treating it as not ended");
        return None;
    };
    // Read again rather than from the open order, which reads an unknown fill as zero; that
    // conversion has already warned of it.
    let filled_qty = Decimal::from_str(&o.filled_qty).ok();
    let Some(state) = ended_order_state(
        status,
        OrderId(SmolStr::new(&o.id)),
        order.state.time_exchange,
        order.quantity,
        filled_qty,
        o.filled_avg_price.as_deref(),
    ) else {
        warn!(%instrument, cid = %key.cid, order_id = %o.id, status, "Alpaca order has a status this version does not know, treating it as not ended");
        return None;
    };
    let mut order = order.map_state(|_| state);
    order.key = key.clone();
    Some(order)
}

/// Whether an Alpaca order in `status` is still live, so may yet fill or be cancelled.
///
/// `done_for_day` is live: Alpaca works the order again the next trading day. The rest are
/// Alpaca's working and pending statuses. Every other status has ended the order (see
/// [`ended_order_state`]) or is unknown to this version.
fn order_status_is_live(status: &str) -> bool {
    matches!(
        status,
        "new"
            | "partially_filled"
            | "done_for_day"
            | "pending_cancel"
            | "pending_replace"
            | "accepted"
            | "pending_new"
            | "accepted_for_bidding"
            | "stopped"
            | "suspended"
            | "calculated"
            | "held"
    )
}

/// How an order ended, from its Alpaca `status`, or `None` for a status that does not end it.
///
/// `filled` becomes [`InactiveOrderState::FullyFilled`] with the reported fill, or the whole
/// `quantity` when that is unknown, and `filled_avg_price`; `canceled` becomes
/// [`InactiveOrderState::Cancelled`], and so does `replaced`, since the order replacing it has an
/// id and client order id of its own; `expired` becomes [`InactiveOrderState::Expired`], each with
/// what filled before, `None` when that is unknown; and `rejected` becomes
/// [`InactiveOrderState::OpenFailed`]. Shared by the lookup of an order that ended
/// ([`convert_ended_order`]) and the response to placing one ([`placed_order_state`]).
fn ended_order_state<AssetKey, InstrumentKey>(
    status: &str,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
    quantity: Decimal,
    filled_qty: Option<Decimal>,
    filled_avg_price: Option<&str>,
) -> Option<InactiveOrderState<AssetKey, InstrumentKey>> {
    Some(match status {
        // A filled order filled its whole quantity, whether or not `filled_qty` said so.
        "filled" => InactiveOrderState::FullyFilled(Filled::new(
            order_id,
            time_exchange,
            filled_qty.unwrap_or(quantity),
            alpaca_avg_price(filled_avg_price),
        )),
        "canceled" | "replaced" => {
            InactiveOrderState::Cancelled(Cancelled::new(order_id, time_exchange, filled_qty))
        }
        "expired" => InactiveOrderState::Expired(Expired::new(order_id, time_exchange, filled_qty)),
        "rejected" => {
            InactiveOrderState::OpenFailed(OrderError::Rejected(ApiError::OrderRejected(format!(
                "Alpaca rejected order {order_id} after accepting it"
            ))))
        }
        _ => return None,
    })
}

/// The state that `resp`, Alpaca's response to placing an order of `quantity`, reports.
///
/// An order that ended in the response itself is reported as it ended (see
/// [`ended_order_state`]): an IOC or FOK order that found no liquidity, or partly filled and had
/// the rest cancelled, is `Cancelled` or `Expired` with what filled, not `Open`, and one Alpaca
/// rejected after accepting it is `OpenFailed`. A live status (see [`order_status_is_live`]) reads
/// from what filled: `FullyFilled`, with `filled_avg_price`, once it covers `quantity`, otherwise
/// `Open`. So does a status that is missing or that this version does not know, with a warning,
/// since the account stream reports the order's next state either way.
fn placed_order_state(
    resp: &AlpacaOrderResponse,
    instrument: &InstrumentNameExchange,
    quantity: Decimal,
) -> UnindexedOrderState {
    let order_id = OrderId(SmolStr::new(&resp.id));
    let time_exchange = order_state_time(resp);
    let filled_qty = alpaca_filled_qty(&resp.id, Some(&resp.filled_qty));
    let filled_avg_price = resp.filled_avg_price.as_deref();
    match resp.status.as_deref() {
        Some(status) => {
            if let Some(ended) = ended_order_state(
                status,
                order_id.clone(),
                time_exchange,
                quantity,
                filled_qty,
                filled_avg_price,
            ) {
                return OrderState::Inactive(ended);
            }
            if !order_status_is_live(status) {
                warn!(%instrument, %order_id, status, "Alpaca placed an order with a status this version does not know, reading it from what filled");
            }
        }
        None => {
            warn!(%instrument, %order_id, "Alpaca placed an order without a status, reading it from what filled");
        }
    }
    match filled_qty {
        Some(filled_qty) if filled_qty >= quantity => OrderState::fully_filled(Filled::new(
            order_id,
            time_exchange,
            filled_qty,
            alpaca_avg_price(filled_avg_price),
        )),
        // A live order's fill only grows, so an unknown one reads as nothing filled until the
        // account stream reports more.
        _ => OrderState::active(Open::new(
            VenueOrderId::Assigned(order_id),
            time_exchange,
            filled_qty.unwrap_or(Decimal::ZERO),
        )),
    }
}

/// The average price of an Alpaca order's fills, from its `filled_avg_price`: `None` when no
/// price is reported or it does not parse.
fn alpaca_avg_price(filled_avg_price: Option<&str>) -> Option<Decimal> {
    filled_avg_price.and_then(|price| Decimal::from_str(price).ok())
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

/// Convert an Alpaca account response to rustrade balance entries.
///
/// Returns a single USD balance with:
/// - `total` = `cash`, which is negative while the account borrows on margin
/// - `free` = the lesser of `cash` and `non_marginable_buying_power`: cash that can be spent
///   without borrowing, within what Alpaca allows
///
/// `free` is capped at cash because `non_marginable_buying_power` counts the loan value of held
/// marginable stock, so it exceeds cash whenever such stock is held. The cap keeps
/// `free <= total`, so with negative cash `free` is negative too.
///
/// Account equity is not the total: equity and option holdings are reported as positions, and
/// counting them again here would double them.
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

    vec![AssetBalance::new(
        usd_name,
        Balance::new(
            account.cash,
            account.cash.min(account.non_marginable_buying_power),
        ),
        Utc::now(),
    )]
}

/// Convert Alpaca positions to crypto asset balance entries.
///
/// Only positions with `asset_class == "crypto"` are included; [`convert_positions`] reports the
/// rest. The base asset is extracted from the symbol (e.g., `"BTC/USD"` → `"btc"`).
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
        .filter(|p| is_crypto_position(p))
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
            let asset_name = AssetNameExchange::new(base);
            Some(AssetBalance::new(
                asset_name,
                Balance::new(p.qty, p.qty_available),
                now,
            ))
        })
        .collect()
}

/// Whether an Alpaca position is a crypto holding, which is reported as an asset balance rather
/// than a [`Position`].
fn is_crypto_position(position: &AlpacaPosition) -> bool {
    position.asset_class.eq_ignore_ascii_case("crypto")
}

/// Convert Alpaca's non-crypto positions (equities and options) to [`Position`]s, each with its
/// symbol, in Alpaca's order.
///
/// Crypto is left to [`convert_positions_to_balances`]: Alpaca crypto is spot-only and cannot be
/// sold short, so a holding is simply a balance of the base asset, as on every spot venue. Every
/// other asset class is a position, so a class Alpaca adds later is reported rather than dropped.
///
/// Each [`Position`] carries:
/// - `quantity`: the magnitude of `qty`, negative when `side` is `short`;
/// - `entry_price`: `avg_entry_price`, per share for an equity and per share of the underlying
///   for an option (the premium as quoted, not multiplied by the contract size);
/// - `unrealized_pnl`: `unrealized_pl`, in USD for the whole position;
/// - `time_exchange`: `now`, since Alpaca does not timestamp positions.
///
/// A position with zero quantity is left out.
///
/// # Errors
///
/// [`ClientError::Internal`](crate::error::ClientError::Internal) when a position's `side` is
/// missing or neither `long` nor `short`: reporting it either way could invert it, and leaving it
/// out would read as flat.
fn convert_positions(
    positions: &[AlpacaPosition],
    now: DateTime<Utc>,
) -> Result<Vec<(&str, Position)>, UnindexedClientError> {
    positions
        .iter()
        .filter(|p| !is_crypto_position(p) && !p.qty.is_zero())
        .map(|p| {
            let size = p.qty.abs();
            let quantity = match p.side {
                AlpacaPositionSide::Long => size,
                AlpacaPositionSide::Short => -size,
                AlpacaPositionSide::Unknown => {
                    return Err(UnindexedClientError::Internal(format!(
                        "Alpaca position {} has a missing or unrecognised side",
                        p.symbol
                    )));
                }
            };
            let position = Position::new(
                quantity,
                p.avg_entry_price,
                p.unrealized_pl,
                None,
                None,
                None,
                now,
            );
            Ok((p.symbol.as_str(), position))
        })
        .collect()
}

/// Group open orders and positions into per-instrument snapshots for account_snapshot.
///
/// When `instruments` is non-empty, a snapshot is returned for every requested instrument
/// (possibly with no orders and no position). When empty, only instruments with an open order or
/// a position are returned.
///
/// `positions` come from [`convert_positions`], which holds every non-zero equity and option
/// position, so an equity or option without one is [`PositionReport::Flat`]. A crypto pair (a
/// symbol with a `/`) is [`PositionReport::Unreported`]: its holding is a balance.
///
/// Requested names match Alpaca's symbols ignoring case. When two requested names differ only in
/// case, the first gets the symbol's orders and position; the second is
/// [`PositionReport::Unreported`] with its orders not complete, since nothing is known about it.
///
/// Each snapshot declares its orders complete unless [`convert_open_order`] left one of its
/// orders out. That holds because `orders` is every open order, unpaged: a response at Alpaca's
/// cap fails the whole snapshot in [`fetch_raw_open_orders`] rather than arriving here short. And
/// every order is listed under the client order id the order stream reports it under
/// ([`alpaca_cid`]): the one it was placed with, or the one Alpaca assigns to a bracket's
/// take-profit and stop-loss legs.
fn build_instrument_snapshots(
    orders: Vec<AlpacaOrderResponse>,
    positions: Vec<(&str, Position)>,
    instruments: &[InstrumentNameExchange],
) -> Vec<InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>> {
    /// One instrument's converted orders, whether none was left out, and its position.
    struct Listing {
        orders: Vec<UnindexedOrderSnapshot>,
        complete: bool,
        position: Option<Position>,
    }

    impl Listing {
        fn empty() -> Self {
            Self {
                orders: Vec::new(),
                complete: true,
                position: None,
            }
        }
    }

    // Build ordered map from symbol → snapshot to preserve deterministic ordering.
    let mut by_symbol: IndexMap<SmolStr, Listing> = IndexMap::new();

    // Keyed by the upper-case symbol, so a requested name in another case still finds its
    // listing. Alpaca's own symbols are upper case, so an unfiltered read lists them unchanged.
    for order in orders {
        let listing = by_symbol
            .entry(order.symbol.to_ascii_uppercase().into())
            .or_insert_with(Listing::empty);
        match convert_open_order(&order) {
            Some(converted) => listing.orders.push(converted.into()),
            None => listing.complete = false,
        }
    }

    for (symbol, position) in positions {
        by_symbol
            .entry(symbol.to_ascii_uppercase().into())
            .or_insert_with(Listing::empty)
            .position = Some(position);
    }

    let snapshot = |instrument: InstrumentNameExchange, listing: Listing| {
        // `positions` holds every non-zero equity and option position, so one missing here is
        // flat. Crypto is reported as balances, never as a position.
        let position = if is_options_or_equity_symbol(instrument.name().as_str()) {
            listing
                .position
                .map_or(PositionReport::Flat, PositionReport::from_position)
        } else {
            PositionReport::Unreported
        };
        InstrumentAccountSnapshot::new(instrument, listing.orders, listing.complete, position, None)
    };

    // If instruments is empty, return all; otherwise filter to requested set.
    if instruments.is_empty() {
        by_symbol
            .into_iter()
            .map(|(sym, listing)| snapshot(InstrumentNameExchange::new(sym), listing))
            .collect()
    } else {
        // Upper-case symbols already given to an earlier requested name.
        let mut claimed = std::collections::HashSet::new();
        instruments
            .iter()
            .map(|inst| {
                let symbol = inst.name().as_str().to_ascii_uppercase();
                // A second name for the same symbol, differing only in case: its listing went to
                // the first, so nothing is known about it rather than it being flat and orderless.
                if !claimed.insert(symbol.clone()) {
                    return InstrumentAccountSnapshot::new(
                        inst.clone(),
                        Vec::new(),
                        false,
                        PositionReport::Unreported,
                        None,
                    );
                }
                // swap_remove is O(1); output order is determined by the `instruments`
                // slice, not by the internal IndexMap order of `by_symbol`.
                let listing = by_symbol
                    .swap_remove(symbol.as_str())
                    .unwrap_or_else(Listing::empty);
                snapshot(inst.clone(), listing)
            })
            .collect()
    }
}

/// Convert an Alpaca REST open order into rustrade's Open state order.
///
/// `None`, with a `warn!`, for an order it cannot represent: a notional order (placed by dollar
/// value, so its `qty` is null), which this client never places but the Alpaca dashboard can, or
/// one whose side, quantity or kind does not parse. A list missing that order is not every open
/// order, so [`build_instrument_snapshots`] must not declare it complete.
///
/// It warns on every call, so an order like that which stays open is logged again each time the
/// open orders are fetched.
fn convert_open_order(
    o: &AlpacaOrderResponse,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let converted = convert_representable_open_order(o);
    if converted.is_none() {
        warn!(
            order_id = %o.id,
            symbol = %o.symbol,
            qty = ?o.qty,
            side = %o.side,
            order_type = %o.order_type,
            "Alpaca open order cannot be represented - leaving it out"
        );
    }
    converted
}

/// The client order id an Alpaca order is reported under, on the REST and stream paths alike: its
/// `client_order_id`, or its venue id when it has none.
///
/// Both paths must agree, or a complete snapshot would miss an order the stream is tracking under
/// another id.
fn alpaca_cid(client_order_id: Option<&str>, order_id: &str) -> ClientOrderId {
    ClientOrderId::new(client_order_id.unwrap_or(order_id))
}

/// [`convert_open_order`] without the `warn!`.
fn convert_representable_open_order(
    o: &AlpacaOrderResponse,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let order_id = OrderId(SmolStr::new(&o.id));
    let cid = alpaca_cid(o.client_order_id.as_deref(), &o.id);

    let instrument = InstrumentNameExchange::new(&o.symbol);
    let side = parse_side(&o.side)?;
    let quantity = Decimal::from_str(o.qty.as_deref().unwrap_or("0")).ok()?;
    // Notional orders (placed by dollar value, qty=null) have no representable quantity.
    // Leave them out rather than recording a zero-quantity order that would corrupt
    // reconciliation.
    if quantity.is_zero() {
        return None;
    }
    let price = o
        .limit_price
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok());
    let filled_qty = alpaca_filled_qty(&o.id, Some(&o.filled_qty)).unwrap_or(Decimal::ZERO);
    let kind = parse_order_kind(
        &o.order_type,
        o.stop_price.as_deref(),
        o.trail_percent.as_deref(),
        o.trail_price.as_deref(),
    )?;
    let time_in_force = parse_time_in_force(&o.time_in_force);
    let time_exchange = order_state_time(o);

    Some(Order {
        key: OrderKey::new(
            ExchangeId::AlpacaBroker,
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
        state: Open::new(VenueOrderId::Assigned(order_id), time_exchange, filled_qty),
    })
}

/// Convert an Alpaca FILL activity into a rustrade Trade.
fn convert_activity_to_trade(
    a: &AlpacaActivity,
) -> Option<Trade<AssetNameExchange, InstrumentNameExchange>> {
    let trade_id = TradeId::new(activity_execution_id(&a.id));
    let order_id = OrderId(SmolStr::new(&a.order_id));
    let instrument = InstrumentNameExchange::new(&a.symbol);
    let side = parse_side(&a.side)?;
    let price = Decimal::from_str(&a.price).ok()?;
    let quantity = Decimal::from_str(&a.qty).ok()?;
    // `None` when Alpaca omitted the field or sent something unparseable, which is the honest
    // report: the venue did not tell us where the order stands, rather than "nothing is filled".
    let order_filled_quantity = a.cum_qty.as_deref().and_then(|s| Decimal::from_str(s).ok());
    let time_exchange = parse_timestamp(&a.transaction_time).unwrap_or_else(|| {
        warn!(id = %a.id, "Alpaca activity: unparseable transaction_time, using now");
        Utc::now()
    });

    // Alpaca equities and options are commission-free. Crypto trades incur
    // maker/taker fees (currently 0.15–0.25%) charged in the credited asset,
    // but fee info is not available in trade responses — use Activities API
    // for end-of-day fee reconciliation.
    Some(Trade::new(
        trade_id,
        order_id,
        instrument,
        StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        order_filled_quantity,
        AssetFees::new(
            AssetNameExchange::from("USD"),
            Decimal::ZERO,
            Some(Decimal::ZERO),
        ),
    ))
}

/// Build an `OrderSnapshot` event from the order payload embedded in a `trade_updates` frame.
///
/// Shared by the acknowledgement arm and the fill arms of [`convert_trade_update`], so that a
/// fill reports the order's new cumulative `filled_quantity` the same way an acknowledgement
/// reports its initial one.
///
/// Returns `None` for a notional order (placed by dollar value, `qty` is null). Emitting a
/// snapshot with `quantity == 0` would read as an order with nothing left to fill and retire it,
/// so such orders are left untracked -- consistent with `convert_open_order` on the REST path.
fn ws_order_snapshot(
    order: &AlpacaOrderWs<'_>,
    instrument: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> Option<UnindexedAccountEvent> {
    let side = parse_side(&order.side)?;
    let quantity = Decimal::from_str(order.qty.unwrap_or("0")).unwrap_or(Decimal::ZERO);
    if quantity.is_zero() {
        trace!(order_id = %order.id, "Alpaca WS: skipping notional order snapshot (qty=None)");
        return None;
    }
    let price = order.limit_price.and_then(|s| Decimal::from_str(s).ok());
    // A live order's fill only grows, so a frame that omits it (some lifecycle events may) reads
    // as nothing filled yet, without a warning; one that garbles it is still reported.
    let filled_qty = order
        .filled_qty
        .and_then(|_| alpaca_filled_qty(&order.id, order.filled_qty))
        .unwrap_or(Decimal::ZERO);
    let kind = parse_order_kind(
        &order.order_type,
        order.stop_price,
        order.trail_percent,
        order.trail_price,
    )?;
    let time_in_force = parse_time_in_force(&order.time_in_force);

    let order_snapshot = crate::order::Order {
        key: OrderKey::new(
            ExchangeId::AlpacaBroker,
            instrument,
            StrategyId::unknown(),
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: OrderState::active(Open::new(
            VenueOrderId::Assigned(order_id),
            time_exchange,
            filled_qty,
        )),
    };
    Some(UnindexedAccountEvent::new(
        ExchangeId::AlpacaBroker,
        AccountEventKind::OrderSnapshot(rustrade_integration::collection::snapshot::Snapshot(
            order_snapshot,
        )),
    ))
}

/// Whether a fill frame's order payload describes an order that is still live at the exchange,
/// and may therefore be written into engine state as an `Open` snapshot.
///
/// A fill frame carries the order's status alongside the execution. Only `partially_filled` and
/// `filled` say the order reached this execution while working; every other status means some
/// other frame owns the order's current state. Writing an `Open` snapshot from one of those would
/// resurrect an order the engine has already retired, leaving a resting order that does not exist
/// at the exchange -- a fill that arrives after its order's terminal frame is exactly the ordering
/// this guards against.
fn ws_fill_order_is_live(status: &str) -> bool {
    matches!(status, "partially_filled" | "filled")
}

/// Convert a WebSocket trade_update event into rustrade AccountEvents.
///
/// Returns up to two events, in the order they must be applied. A fill frame genuinely carries
/// two facts -- the execution print and the order's new cumulative filled quantity -- so it maps
/// to both a `Trade` and an `OrderSnapshot`.
///
/// The `Trade` is always first. A fully-filled snapshot retires its order, and routing a fill
/// against an order that has already been retired is a strictly harder problem than routing it
/// against a live one; emitting the execution first keeps the easy ordering.
fn convert_trade_update(update: AlpacaTradeUpdate<'_>) -> [Option<UnindexedAccountEvent>; 2] {
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
        return [None, None];
    }

    let order = &update.order;
    let instrument = InstrumentNameExchange::new(&*order.symbol);
    let order_id = OrderId(order.id.clone());
    let cid = alpaca_cid(order.client_order_id.as_deref(), &order.id);

    match event_str {
        "fill" | "partial_fill" => {
            // Use event-level price/qty for the per-execution trade.
            let (Some(price), Some(quantity), Some(side)) = (
                update.price.and_then(|s| Decimal::from_str(s).ok()),
                update.qty.and_then(|s| Decimal::from_str(s).ok()),
                parse_side(&order.side),
            ) else {
                return [None, None];
            };
            let time_exchange = update
                .timestamp
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);

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
            // The execution id, which the FILL activity for this fill carries too, so the fill has
            // one TradeId however it is delivered. Without it, fall back to the dedup key, which
            // is unique per fill but matches nothing a REST read returns.
            let trade_id = match update.execution_id.filter(|id| !id.is_empty()) {
                Some(execution_id) => TradeId::new(execution_id),
                None => {
                    warn!(
                        order_id = %order.id,
                        "Alpaca WS: fill without a usable execution_id — its TradeId is \
                         \"{{order_id}}:{{filled_qty}}\", which fetch_trades will not match"
                    );
                    TradeId(fill_dedup_key(&order.id, cum_qty))
                }
            };

            // Alpaca equities and options are commission-free. Crypto trades incur
            // maker/taker fees (currently 0.15–0.25%) in the credited asset, but
            // fee info is not available in WebSocket updates.
            let trade = Trade::new(
                trade_id,
                order_id.clone(),
                instrument.clone(),
                StrategyId::unknown(),
                time_exchange,
                side,
                price,
                quantity,
                // The venue's own cumulative for this order, as of this execution.
                Some(cum_qty),
                AssetFees::new(
                    AssetNameExchange::from("USD"),
                    Decimal::ZERO,
                    Some(Decimal::ZERO),
                ),
            );
            let trade_event = UnindexedAccountEvent::new(
                ExchangeId::AlpacaBroker,
                AccountEventKind::Trade(trade),
            );

            // The execution alone does not move the order: `filled_quantity` is only ever carried
            // into engine state by an order snapshot, so without this second event a partially
            // filled order reads as having nothing filled until REST reconciliation refreshes it.
            let snapshot_event = if ws_fill_order_is_live(&order.status) {
                ws_order_snapshot(order, instrument, cid, order_id, time_exchange)
            } else {
                trace!(
                    order_id = %order.id,
                    status = %order.status,
                    "Alpaca WS: fill for an order the exchange no longer reports as working — \
                     emitting the execution without an order snapshot"
                );
                None
            };

            [Some(trade_event), snapshot_event]
        }

        "new" | "accepted" | "pending_new" | "done_for_day" => {
            // Order acknowledged by Alpaca, or done for the day: Alpaca stops working a
            // `done_for_day` order until the next trading day but has not ended it, so it is
            // reported open with what it has filled, never retired.
            let time_exchange = update
                .timestamp
                .and_then(parse_timestamp)
                .unwrap_or_else(Utc::now);

            [
                ws_order_snapshot(order, instrument, cid, order_id, time_exchange),
                None,
            ]
        }

        "canceled" | "expired" | "replaced" => {
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
            let filled_qty = alpaca_filled_qty(&order.id, order.filled_qty);
            let cancelled = Cancelled::new(order_id, time_exchange, filled_qty);
            let response = crate::order::request::OrderResponseCancel {
                key: OrderKey::new(
                    ExchangeId::AlpacaBroker,
                    instrument,
                    StrategyId::unknown(),
                    cid,
                ),
                state: Ok(cancelled),
            };
            [
                Some(UnindexedAccountEvent::new(
                    ExchangeId::AlpacaBroker,
                    AccountEventKind::OrderCancelled(response),
                )),
                None,
            ]
        }

        "rejected" => {
            let response = crate::order::request::OrderResponseCancel {
                key: OrderKey::new(
                    ExchangeId::AlpacaBroker,
                    instrument,
                    StrategyId::unknown(),
                    cid,
                ),
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    format!("order rejected: status={}", order.status),
                ))),
            };
            [
                Some(UnindexedAccountEvent::new(
                    ExchangeId::AlpacaBroker,
                    AccountEventKind::OrderCancelled(response),
                )),
                None,
            ]
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

fn parse_order_kind(
    order_type: &str,
    stop_price: Option<&str>,
    trail_percent: Option<&str>,
    trail_price: Option<&str>,
) -> Option<OrderKind> {
    match order_type {
        "market" | "Market" => Some(OrderKind::Market),
        "limit" | "Limit" => Some(OrderKind::Limit),
        "stop" | "Stop" => {
            let trigger_price = stop_price.and_then(|s| Decimal::from_str(s).ok())?;
            Some(OrderKind::Stop { trigger_price })
        }
        "stop_limit" | "Stop_limit" => {
            let trigger_price = stop_price.and_then(|s| Decimal::from_str(s).ok())?;
            Some(OrderKind::StopLimit { trigger_price })
        }
        "trailing_stop" | "Trailing_stop" => {
            // Alpaca returns either trail_percent or trail_price, not both.
            if let Some(pct) = trail_percent.and_then(|s| Decimal::from_str(s).ok()) {
                Some(OrderKind::TrailingStop {
                    offset: pct,
                    offset_type: TrailingOffsetType::Percentage,
                })
            } else if let Some(price) = trail_price.and_then(|s| Decimal::from_str(s).ok()) {
                Some(OrderKind::TrailingStop {
                    offset: price,
                    offset_type: TrailingOffsetType::Absolute,
                })
            } else {
                trace!("Alpaca: trailing_stop missing trail_percent and trail_price");
                None
            }
        }
        other => {
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

/// When the exchange last reported this order's state.
///
/// [`Open::time_exchange`] orders an order's states, and the engine discards a snapshot older
/// than the state it already tracks. `created_at` does not order them: it is the same value for
/// every snapshot of one order, so a snapshot stamped with it is indistinguishable from the
/// acknowledgement and is discarded the moment anything has advanced the tracked order past it.
/// `updated_at` moves with each state change, which is what that comparison needs.
///
/// Falls back to `created_at` when the venue omits `updated_at`, and to now when neither parses.
fn order_state_time(order: &AlpacaOrderResponse) -> DateTime<Utc> {
    order
        .updated_at
        .as_deref()
        .and_then(parse_timestamp)
        .or_else(|| parse_timestamp(&order.created_at))
        .unwrap_or_else(Utc::now)
}

/// The quantity Alpaca order `order_id` has filled, from its `filled_qty`: `None`, with a
/// warning, when it is missing or does not parse, so that an unknown fill is not read as zero.
fn alpaca_filled_qty(order_id: &str, filled_qty: Option<&str>) -> Option<Decimal> {
    let Some(raw) = filled_qty else {
        warn!(%order_id, "Alpaca did not report how much the order filled");
        return None;
    };
    match Decimal::from_str(raw) {
        Ok(filled_qty) => Some(filled_qty),
        Err(_) => {
            warn!(%order_id, filled_qty = raw, "Alpaca reported an unparseable filled_qty, treating it as unknown");
            None
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

fn map_order_kind(kind: OrderKind) -> Option<&'static str> {
    match kind {
        OrderKind::Market => Some("market"),
        OrderKind::Limit => Some("limit"),
        OrderKind::Stop { .. } => Some("stop"),
        OrderKind::StopLimit { .. } => Some("stop_limit"),
        OrderKind::TrailingStop { .. } => Some("trailing_stop"),
        // Alpaca does not support take-profit or trailing stop-limit orders.
        OrderKind::TakeProfit { .. }
        | OrderKind::TakeProfitLimit { .. }
        | OrderKind::TrailingStopLimit { .. } => None,
    }
}

/// Map rustrade's `TimeInForce` to Alpaca's string representation.
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
        TimeInForce::AtOpen => Ok("opg"),
        TimeInForce::AtClose => Ok("cls"),
        // Alpaca's `gtd` requires an `expired_at` timestamp on the order request,
        // which this client does not currently surface. Reject to avoid silently
        // dropping the expiry semantics.
        TimeInForce::GoodTillDate { .. } => {
            Err("Alpaca GoodTillDate is not yet wired through this client")
        }
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
        // The body says what ran short ("insufficient buying power", or "insufficient qty
        // available" on a sell) but names no asset, so none is guessed.
        422 if lower.contains("insufficient") => {
            ApiError::BalanceInsufficient(None, message.to_owned())
        }
        401 => ApiError::Unauthenticated(format!("unauthorized: {message}")),
        403 => ApiError::Unauthenticated(format!("forbidden: {message}")),
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

/// Map a failed order `POST` through [`rest_with_retry`] to the order's error.
///
/// [`UnindexedClientError::Internal`] there means Alpaca answered 2xx, so it accepted the order,
/// but the response could not be read. The order may be live, so it is reported as
/// [`OrderError::Connectivity`], the status-unknown error, never as a rejection. A caller must
/// reconcile it (open orders, fills) before resubmitting, or it may place the order twice.
fn order_post_error(error: UnindexedClientError) -> UnindexedOrderError {
    match error {
        UnindexedClientError::Connectivity(ce) => OrderError::Connectivity(ce),
        UnindexedClientError::Api(ae) => OrderError::Rejected(ae),
        UnindexedClientError::Internal(msg) => OrderError::Connectivity(ConnectivityError::Socket(
            format!("Alpaca order status unknown, its 2xx response could not be read: {msg}"),
        )),
        // `rest_with_retry` returns none of these today. Matched explicitly so a new
        // `ClientError` variant is a compile error here, and reported as status unknown rather
        // than panicking an order path if that ever changes.
        other @ (UnindexedClientError::TaskFailed(_)
        | UnindexedClientError::Truncated { .. }
        | UnindexedClientError::TruncatedSnapshot { .. }) => OrderError::Connectivity(
            ConnectivityError::Socket(format!("Alpaca order status unknown: {other}")),
        ),
    }
}

// ---------------------------------------------------------------------------
// OrderStatusClient implementation
// ---------------------------------------------------------------------------

impl OrderStatusClient for AlpacaClient {
    /// Looks each order up with `GET /v2/orders:by_client_order_id`, up to 8 at a time.
    ///
    /// `filled` is reported as fully filled with `filled_avg_price`, `canceled` and `replaced` as
    /// cancelled (the order replacing one has its own client order id, which the caller must
    /// learn from [`ExecutionClient::fetch_open_orders`]), `expired` as expired and `rejected` as
    /// open failed. `done_for_day` is not ended: Alpaca works the order again the next trading day.
    /// A 404, or an order on another symbol than the key's (ignoring case), is unknown.
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        fetch_ended_by_key(orders, |key| {
            fetch_order_lookup(
                self.http.clone(),
                self.rate_limiter.clone(),
                self.config.clone(),
                key,
            )
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// BracketOrderClient implementation
// ---------------------------------------------------------------------------

impl BracketOrderClient for AlpacaClient {
    async fn open_bracket_order(
        &self,
        request: UnifiedBracketOrderRequest<ExchangeId, &InstrumentNameExchange>,
    ) -> UnifiedBracketOrderResult {
        let alpaca_request = AlpacaBracketOrderRequest {
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
            side: request.state.side,
            quantity: request.state.quantity,
            entry_price: request.state.entry_price,
            take_profit_price: request.state.take_profit_price,
            stop_loss_price: request.state.stop_loss_price,
            stop_loss_limit_price: request.state.stop_loss_limit_price,
            time_in_force: request.state.time_in_force,
        };

        let result = self.open_bracket_order(alpaca_request).await;

        UnifiedBracketOrderResult::parent_only(result.parent)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_alpaca_config_new_uses_paper_trading_by_default() {
        let cfg = AlpacaConfig::new("my_key".into(), "my_secret".into());
        assert!(cfg.paper);
    }

    #[test]
    fn test_alpaca_config_debug_redacts_credentials() {
        let cfg = AlpacaConfig::new("my_key".into(), "my_secret".into());
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("my_key"), "api_key should be redacted");
        assert!(
            !debug.contains("my_secret"),
            "secret_key should be redacted"
        );
        assert!(debug.contains("paper: true"));
    }

    #[test]
    fn test_alpaca_config_deserialize_omitted_paper_defaults_to_paper() {
        let cfg: AlpacaConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret"
    }"#,
        )
        .unwrap();

        assert!(cfg.paper);
        assert_eq!(cfg.rest_base_url(), "https://paper-api.alpaca.markets");
    }

    #[test]
    fn test_alpaca_config_deserialize_paper_true() {
        let cfg: AlpacaConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret",
        "paper": true
    }"#,
        )
        .unwrap();

        assert!(cfg.paper);
        assert_eq!(cfg.rest_base_url(), "https://paper-api.alpaca.markets");
    }

    #[test]
    fn test_alpaca_config_deserialize_paper_false() {
        let cfg: AlpacaConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret",
        "paper": false
    }"#,
        )
        .unwrap();

        assert!(!cfg.paper);
        assert_eq!(cfg.rest_base_url(), "https://api.alpaca.markets");
    }

    #[test]
    fn test_alpaca_config_urls() {
        let paper = AlpacaConfig::paper("k".into(), "s".into());
        assert!(paper.rest_base_url().contains("paper-api"));
        assert!(paper.ws_url().contains("paper-api"));

        let live = AlpacaConfig::production("k".into(), "s".into());
        assert!(!live.rest_base_url().contains("paper-api"));
        assert!(!live.ws_url().contains("paper-api"));
    }

    #[test]
    #[serial_test::serial]
    fn test_alpaca_config_from_env_defaults_to_paper_trading() {
        temp_env::with_vars(
            [
                ("ALPACA_API_KEY", Some("my_key")),
                ("ALPACA_SECRET_KEY", Some("my_secret")),
                ("ALPACA_PAPER", None),
            ],
            || {
                let cfg = AlpacaConfig::from_env().unwrap();
                assert!(cfg.paper);
                assert_eq!(cfg.rest_base_url(), "https://paper-api.alpaca.markets");
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_alpaca_config_from_env_accepts_explicit_production() {
        temp_env::with_vars(
            [
                ("ALPACA_API_KEY", Some("my_key")),
                ("ALPACA_SECRET_KEY", Some("my_secret")),
                ("ALPACA_PAPER", Some("false")),
            ],
            || {
                let cfg = AlpacaConfig::from_env().unwrap();
                assert!(!cfg.paper);
                assert_eq!(cfg.rest_base_url(), "https://api.alpaca.markets");
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_alpaca_config_from_env_rejects_invalid_paper() {
        temp_env::with_vars(
            [
                ("ALPACA_API_KEY", Some("my_key")),
                ("ALPACA_SECRET_KEY", Some("my_secret")),
                ("ALPACA_PAPER", Some("maybe")),
            ],
            || {
                let err = AlpacaConfig::from_env().unwrap_err();
                assert!(matches!(err, AlpacaConfigError::InvalidPaper(value) if value == "maybe"));
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_alpaca_config_from_env_requires_credentials() {
        temp_env::with_vars(
            [
                ("ALPACA_API_KEY", None),
                ("ALPACA_SECRET_KEY", Some("my_secret")),
                ("ALPACA_PAPER", None),
            ],
            || {
                let err = AlpacaConfig::from_env().unwrap_err();
                assert!(matches!(err, AlpacaConfigError::MissingApiKey));
            },
        );
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
        // Market and Limit don't need extra params
        assert_eq!(
            parse_order_kind("market", None, None, None),
            Some(OrderKind::Market)
        );
        assert_eq!(
            parse_order_kind("limit", None, None, None),
            Some(OrderKind::Limit)
        );

        // Stop requires stop_price
        assert_eq!(
            parse_order_kind("stop", Some("150.00"), None, None),
            Some(OrderKind::Stop {
                trigger_price: Decimal::from_str("150.00").unwrap()
            })
        );
        assert_eq!(parse_order_kind("stop", None, None, None), None);

        // StopLimit requires stop_price
        assert_eq!(
            parse_order_kind("stop_limit", Some("145.00"), None, None),
            Some(OrderKind::StopLimit {
                trigger_price: Decimal::from_str("145.00").unwrap()
            })
        );

        // TrailingStop with percentage
        assert_eq!(
            parse_order_kind("trailing_stop", None, Some("5.0"), None),
            Some(OrderKind::TrailingStop {
                offset: Decimal::from_str("5.0").unwrap(),
                offset_type: TrailingOffsetType::Percentage,
            })
        );

        // TrailingStop with absolute price
        assert_eq!(
            parse_order_kind("trailing_stop", None, None, Some("2.50")),
            Some(OrderKind::TrailingStop {
                offset: Decimal::from_str("2.50").unwrap(),
                offset_type: TrailingOffsetType::Absolute,
            })
        );

        // TrailingStop without either offset returns None
        assert_eq!(parse_order_kind("trailing_stop", None, None, None), None);

        // Unknown type returns None
        assert_eq!(parse_order_kind("unknown", None, None, None), None);
    }

    #[test]
    fn test_map_order_kind() {
        assert_eq!(map_order_kind(OrderKind::Market), Some("market"));
        assert_eq!(map_order_kind(OrderKind::Limit), Some("limit"));
        assert_eq!(
            map_order_kind(OrderKind::Stop {
                trigger_price: Decimal::from_str("150.00").unwrap()
            }),
            Some("stop")
        );
        assert_eq!(
            map_order_kind(OrderKind::StopLimit {
                trigger_price: Decimal::from_str("145.00").unwrap()
            }),
            Some("stop_limit")
        );
        assert_eq!(
            map_order_kind(OrderKind::TrailingStop {
                offset: Decimal::from_str("5.0").unwrap(),
                offset_type: TrailingOffsetType::Percentage,
            }),
            Some("trailing_stop")
        );
        assert_eq!(
            map_order_kind(OrderKind::TrailingStop {
                offset: Decimal::from_str("2.50").unwrap(),
                offset_type: TrailingOffsetType::Absolute,
            }),
            Some("trailing_stop")
        );
        // TrailingStopLimit is not supported by Alpaca
        assert_eq!(
            map_order_kind(OrderKind::TrailingStopLimit {
                offset: Decimal::from_str("5.0").unwrap(),
                offset_type: TrailingOffsetType::Percentage,
                limit_offset: Decimal::from_str("1.0").unwrap(),
            }),
            None
        );
        // TakeProfit/TakeProfitLimit are not supported by Alpaca
        assert_eq!(
            map_order_kind(OrderKind::TakeProfit {
                trigger_price: Decimal::from_str("160.00").unwrap()
            }),
            None
        );
        assert_eq!(
            map_order_kind(OrderKind::TakeProfitLimit {
                trigger_price: Decimal::from_str("160.00").unwrap()
            }),
            None
        );
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

    // =========================================================================
    // Bracket Order Serialization Tests
    // =========================================================================

    #[test]
    fn test_bracket_order_serializes_with_stop_loss_stop_order() {
        // Bracket order with stop-loss as a simple stop order (no limit_price)
        let body = AlpacaOrderRequest {
            symbol: "AAPL",
            qty: "10".to_string(),
            side: "buy",
            order_type: "limit",
            time_in_force: "gtc",
            limit_price: Some("150.00".to_string()),
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            client_order_id: Some("bracket-001"),
            position_intent: Some(AlpacaPositionIntent::BuyToOpen),
            order_class: Some("bracket"),
            take_profit: Some(TakeProfitParams {
                limit_price: "160.00".to_string(),
            }),
            stop_loss: Some(StopLossParams {
                stop_price: "145.00".to_string(),
                limit_price: None,
            }),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["symbol"], "AAPL");
        assert_eq!(json["qty"], "10");
        assert_eq!(json["side"], "buy");
        assert_eq!(json["type"], "limit");
        assert_eq!(json["time_in_force"], "gtc");
        assert_eq!(json["limit_price"], "150.00");
        assert_eq!(json["order_class"], "bracket");

        // Take profit should have limit_price
        assert_eq!(json["take_profit"]["limit_price"], "160.00");

        // Stop loss should have stop_price but NO limit_price
        assert_eq!(json["stop_loss"]["stop_price"], "145.00");
        assert!(
            json["stop_loss"].get("limit_price").is_none(),
            "stop_loss.limit_price should be omitted when None"
        );
    }

    #[test]
    fn test_bracket_order_serializes_with_stop_loss_stop_limit_order() {
        // Bracket order with stop-loss as a stop-limit order (has limit_price)
        let body = AlpacaOrderRequest {
            symbol: "SPY",
            qty: "5".to_string(),
            side: "sell",
            order_type: "limit",
            time_in_force: "day",
            limit_price: Some("450.00".to_string()),
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            client_order_id: Some("bracket-002"),
            position_intent: Some(AlpacaPositionIntent::SellToClose),
            order_class: Some("bracket"),
            take_profit: Some(TakeProfitParams {
                limit_price: "440.00".to_string(),
            }),
            stop_loss: Some(StopLossParams {
                stop_price: "455.00".to_string(),
                limit_price: Some("456.00".to_string()),
            }),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["symbol"], "SPY");
        assert_eq!(json["side"], "sell");
        assert_eq!(json["time_in_force"], "day");
        assert_eq!(json["order_class"], "bracket");

        // Take profit
        assert_eq!(json["take_profit"]["limit_price"], "440.00");

        // Stop loss with limit_price (stop-limit order)
        assert_eq!(json["stop_loss"]["stop_price"], "455.00");
        assert_eq!(
            json["stop_loss"]["limit_price"], "456.00",
            "stop_loss.limit_price should be present for stop-limit orders"
        );
    }

    // =========================================================================
    // Bracket Order Validation Tests
    // =========================================================================

    #[tokio::test]
    async fn test_open_bracket_order_rejects_invalid_tif() {
        use rust_decimal_macros::dec;
        use rustrade_instrument::instrument::name::InstrumentNameExchange;

        // Create a minimal client with dummy credentials (no network call will be made)
        let config = AlpacaConfig::new("dummy_key".into(), "dummy_secret".into());
        let client = AlpacaClient::new(config);

        let request = AlpacaBracketOrderRequest::new(
            InstrumentNameExchange::new("SPY"),
            crate::order::id::StrategyId::new("test"),
            crate::order::id::ClientOrderId::new("test-tif"),
            Side::Buy,
            dec!(1),
            dec!(100.00),
            dec!(120.00),
            dec!(90.00),
            TimeInForce::ImmediateOrCancel, // Invalid for brackets
        );

        let result = client.open_bracket_order(request).await;

        assert!(
            result.parent.state.is_failed(),
            "Bracket order with IOC TIF should be rejected locally"
        );
    }

    #[tokio::test]
    async fn test_open_bracket_order_rejects_invalid_price_ordering() {
        use rust_decimal_macros::dec;
        use rustrade_instrument::instrument::name::InstrumentNameExchange;

        let config = AlpacaConfig::new("dummy_key".into(), "dummy_secret".into());
        let client = AlpacaClient::new(config);

        // Buy bracket with SL > entry (invalid)
        let request = AlpacaBracketOrderRequest::new(
            InstrumentNameExchange::new("SPY"),
            crate::order::id::StrategyId::new("test"),
            crate::order::id::ClientOrderId::new("test-price"),
            Side::Buy,
            dec!(1),
            dec!(100.00),
            dec!(120.00),
            dec!(105.00), // Invalid: SL > entry for buy
            TimeInForce::GoodUntilCancelled { post_only: false },
        );

        let result = client.open_bracket_order(request).await;

        assert!(
            result.parent.state.is_failed(),
            "Bracket order with invalid price ordering should be rejected locally"
        );
    }

    #[tokio::test]
    async fn test_open_bracket_order_rejects_invalid_sl_limit_price() {
        use rust_decimal_macros::dec;
        use rustrade_instrument::instrument::name::InstrumentNameExchange;

        let config = AlpacaConfig::new("dummy_key".into(), "dummy_secret".into());
        let client = AlpacaClient::new(config);

        // Buy bracket with SL limit > SL trigger (invalid for sell stop-limit)
        let request = AlpacaBracketOrderRequest::new(
            InstrumentNameExchange::new("SPY"),
            crate::order::id::StrategyId::new("test"),
            crate::order::id::ClientOrderId::new("test-sl-limit"),
            Side::Buy,
            dec!(1),
            dec!(100.00),
            dec!(120.00),
            dec!(90.00),
            TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .with_stop_loss_limit_price(dec!(95.00)); // Invalid: limit > trigger for sell SL

        let result = client.open_bracket_order(request).await;

        assert!(
            result.parent.state.is_failed(),
            "Bracket order with invalid SL limit price should be rejected locally"
        );
    }

    #[test]
    fn test_non_bracket_order_omits_bracket_fields() {
        // Regular limit order should not have bracket fields
        let body = AlpacaOrderRequest {
            symbol: "AAPL",
            qty: "1".to_string(),
            side: "buy",
            order_type: "limit",
            time_in_force: "gtc",
            limit_price: Some("150.00".to_string()),
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            client_order_id: Some("regular-001"),
            position_intent: Some(AlpacaPositionIntent::BuyToOpen),
            order_class: None,
            take_profit: None,
            stop_loss: None,
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["symbol"], "AAPL");
        assert!(
            json.get("order_class").is_none(),
            "order_class should be omitted for non-bracket orders"
        );
        assert!(
            json.get("take_profit").is_none(),
            "take_profit should be omitted for non-bracket orders"
        );
        assert!(
            json.get("stop_loss").is_none(),
            "stop_loss should be omitted for non-bracket orders"
        );
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
        assert!(matches!(
            check_auth_response(msg),
            Some(Err(HandshakeError::Auth(_)))
        ));
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
        // equity and buying_power are larger than cash on a margin account holding positions;
        // neither may leak into the balance.
        let account: AlpacaAccount = serde_json::from_value(serde_json::json!({
            "cash": "9000.50",
            "equity": "12000.00",
            "buying_power": "36000.00",
            "regt_buying_power": "18000.00",
            "options_buying_power": "8000.00",
            "non_marginable_buying_power": "8500.25",
        }))
        .unwrap();
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].asset, AssetNameExchange::new("usd"));
        assert_eq!(balances[0].balance.total, dec!(9000.50), "total is cash");
        assert_eq!(
            balances[0].balance.free,
            dec!(8500.25),
            "free is non-marginable buying power when below cash"
        );
        assert_eq!(balances[0].balance.margin, None);
    }

    /// Long marginable stock: non-marginable buying power is equity less initial margin, which
    /// exceeds cash, so `free` is capped at cash.
    #[test]
    fn test_convert_account_to_balances_free_capped_at_cash_with_long_stock() {
        let account: AlpacaAccount = serde_json::from_value(serde_json::json!({
            "cash": "50000",
            "equity": "100000",
            "initial_margin": "25000",
            "non_marginable_buying_power": "75000",
        }))
        .unwrap();
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances[0].balance.total, dec!(50000));
        assert_eq!(balances[0].balance.free, dec!(50000));
    }

    /// A short sale credits its proceeds to cash but lowers non-marginable buying power, so
    /// `free` is the buying power, below `total`.
    #[test]
    fn test_convert_account_to_balances_open_short_keeps_free_below_cash() {
        let account: AlpacaAccount = serde_json::from_value(serde_json::json!({
            "cash": "10200",
            "equity": "10000",
            "initial_margin": "100",
            "non_marginable_buying_power": "9900",
        }))
        .unwrap();
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances[0].balance.total, dec!(10200));
        assert_eq!(balances[0].balance.free, dec!(9900));
    }

    #[test]
    fn test_convert_account_to_balances_negative_cash_on_margin() {
        let account: AlpacaAccount = serde_json::from_value(serde_json::json!({
            "cash": "-2500.75",
            "equity": "7500.00",
            "buying_power": "5000.00",
            "non_marginable_buying_power": "1250.00",
        }))
        .unwrap();
        let balances = convert_account_to_balances(&account, &[]);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].balance.total, dec!(-2500.75));
        assert_eq!(
            balances[0].balance.free,
            dec!(-2500.75),
            "no free cash while borrowing; free <= total still holds"
        );
    }

    #[test]
    fn test_alpaca_account_malformed_cash_fails_to_decode() {
        let result = serde_json::from_value::<AlpacaAccount>(serde_json::json!({
            "cash": "not-a-number",
            "non_marginable_buying_power": "0",
        }));
        assert!(result.is_err(), "a malformed cash must not read as zero");
    }

    #[test]
    fn test_convert_account_to_balances_usd_filter() {
        let account = AlpacaAccount {
            cash: dec!(12000.00),
            non_marginable_buying_power: dec!(10000.00),
        };
        let usd = vec![AssetNameExchange::new("USD")];
        let balances = convert_account_to_balances(&account, &usd);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].asset, AssetNameExchange::new("USD"));

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
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            status: SmolStr::new("partially_filled"),
        }
    }

    /// The one event produced by a frame that maps to a single `AccountEvent`.
    ///
    /// Asserts the second slot is empty, so a test written for a single-event frame fails
    /// loudly if that frame ever starts producing two.
    fn sole_event(events: [Option<UnindexedAccountEvent>; 2]) -> Option<UnindexedAccountEvent> {
        let [first, second] = events;
        assert!(
            second.is_none(),
            "expected a single event, got a second: {second:?}"
        );
        first
    }

    /// The `(Trade, OrderSnapshot)` pair a fill frame must produce, in that order.
    fn fill_events(
        events: [Option<UnindexedAccountEvent>; 2],
    ) -> (
        Trade<AssetNameExchange, InstrumentNameExchange>,
        crate::order::Order<
            ExchangeId,
            InstrumentNameExchange,
            OrderState<AssetNameExchange, InstrumentNameExchange>,
        >,
    ) {
        let [first, second] = events;
        let first = first.expect("a fill frame must produce an execution");
        let second = second.expect("a fill frame must produce an order snapshot");
        let AccountEventKind::Trade(trade) = first.kind else {
            panic!("first event must be the Trade, got {:?}", first.kind);
        };
        let AccountEventKind::OrderSnapshot(rustrade_integration::collection::snapshot::Snapshot(
            order,
        )) = second.kind
        else {
            panic!(
                "second event must be the OrderSnapshot, got {:?}",
                second.kind
            );
        };
        (trade, order)
    }

    #[test]
    fn test_convert_trade_update_fill_produces_trade_with_execution_id() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            execution_id: Some("exec-1"),
            order: make_order_ws("ord-1", "SPY", "buy", "1"),
            price: Some("150.00"),
            qty: Some("1"),
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let (trade, order) = fill_events(convert_trade_update(update));
        // The venue's execution id, which the fill's FILL activity carries too.
        assert_eq!(trade.id.0.as_str(), "exec-1");
        assert_eq!(trade.price, Decimal::from_str("150.00").unwrap());
        assert_eq!(trade.quantity, Decimal::from_str("1").unwrap());

        // The execution print alone leaves the order reading as untouched. The snapshot carries
        // the cumulative filled quantity, which is the only way it reaches engine state.
        let OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) = order.state
        else {
            panic!("expected an Open snapshot, got {:?}", order.state);
        };
        assert_eq!(open.filled_quantity, Decimal::from_str("1").unwrap());
        assert_eq!(open.id.assigned().map(|id| id.0.as_str()), Some("ord-1"));
    }

    #[test]
    fn test_convert_trade_update_partial_fill() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("partial_fill"),
            execution_id: None,
            order: make_order_ws("ord-2", "AAPL", "sell", "0.5"),
            price: Some("200.00"),
            qty: Some("0.5"),
            timestamp: None,
        };
        let (trade, order) = fill_events(convert_trade_update(update));
        assert_eq!(trade.quantity, Decimal::from_str("0.5").unwrap());

        // make_order_ws reports qty=2, so 0.5 cumulative leaves 1.5 working: the order stays
        // Open, and a consumer reading quantity_remaining now sees 1.5 rather than 2.
        let OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) = order.state
        else {
            panic!("expected an Open snapshot, got {:?}", order.state);
        };
        assert_eq!(open.filled_quantity, Decimal::from_str("0.5").unwrap());
        assert_eq!(
            open.quantity_remaining(order.quantity),
            Decimal::from_str("1.5").unwrap()
        );
    }

    /// A `fill` that completes the order reports nothing left to fill, which is how the engine
    /// learns the order is done. Without the snapshot the order would sit in engine state as a
    /// resting order with `filled_quantity` 0 until REST reconciliation refreshed it.
    #[test]
    fn a_full_fill_snapshot_reports_nothing_left_to_fill() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            execution_id: None,
            // make_order_ws reports qty=2; a cumulative filled of 2 completes it.
            order: AlpacaOrderWs {
                status: SmolStr::new("filled"),
                ..make_order_ws("ord-full", "SPY", "buy", "2")
            },
            price: Some("150.00"),
            qty: Some("1"),
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let (_trade, order) = fill_events(convert_trade_update(update));
        let OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) = order.state
        else {
            panic!("expected an Open snapshot, got {:?}", order.state);
        };
        assert_eq!(open.filled_quantity, Decimal::from_str("2").unwrap());
        assert!(
            open.quantity_remaining(order.quantity).is_zero(),
            "a completed order must report nothing left to fill so the engine retires it"
        );
    }

    /// A fill whose order the exchange no longer reports as working must not be written back into
    /// engine state as an `Open` order. The execution still counts -- it moved the position --
    /// but resurrecting the order would leave a resting order that does not exist at the venue.
    #[test]
    fn a_fill_for_an_order_no_longer_working_emits_the_execution_without_a_snapshot() {
        for status in [
            "canceled",
            "expired",
            "rejected",
            "done_for_day",
            "pending_cancel",
        ] {
            let update = AlpacaTradeUpdate {
                event: SmolStr::new("partial_fill"),
                execution_id: None,
                order: AlpacaOrderWs {
                    status: SmolStr::new(status),
                    ..make_order_ws("ord-late", "SPY", "buy", "1")
                },
                price: Some("150.00"),
                qty: Some("1"),
                timestamp: None,
            };
            let [trade, snapshot] = convert_trade_update(update);
            assert!(
                matches!(trade.map(|e| e.kind), Some(AccountEventKind::Trade(_))),
                "the execution must still be reported for status {status}"
            );
            assert!(
                snapshot.is_none(),
                "status {status} must not produce an order snapshot"
            );
        }
    }

    /// A notional order (placed by dollar value) carries no `qty`, so there is no quantity to
    /// report a remaining amount against. The execution is still reported; the order is not.
    #[test]
    fn a_notional_order_fill_emits_the_execution_without_a_snapshot() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("partial_fill"),
            execution_id: None,
            order: AlpacaOrderWs {
                qty: None,
                ..make_order_ws("ord-notional-ws", "SPY", "buy", "1")
            },
            price: Some("150.00"),
            qty: Some("1"),
            timestamp: None,
        };
        let [trade, snapshot] = convert_trade_update(update);
        assert!(matches!(
            trade.map(|e| e.kind),
            Some(AccountEventKind::Trade(_))
        ));
        assert!(
            snapshot.is_none(),
            "a notional order has no quantity to snapshot against"
        );
    }

    #[test]
    fn test_convert_trade_update_new_order_produces_snapshot() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("new"),
            execution_id: None,
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
                stop_price: None,
                trail_percent: None,
                trail_price: None,
                status: SmolStr::new("new"),
            },
            price: None,
            qty: None,
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let event =
            sole_event(convert_trade_update(update)).expect("new event should produce an event");
        assert!(matches!(event.kind, AccountEventKind::OrderSnapshot(_)));
    }

    #[test]
    fn test_convert_trade_update_canceled_produces_cancel() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("canceled"),
            execution_id: None,
            order: make_order_ws("ord-3", "AAPL", "sell", "0"),
            price: None,
            qty: None,
            timestamp: Some("2025-04-18T14:30:00Z"),
        };
        let event =
            sole_event(convert_trade_update(update)).expect("canceled should produce an event");
        let AccountEventKind::OrderCancelled(response) = event.kind else {
            panic!("expected OrderCancelled, got {:?}", event.kind);
        };
        assert!(response.state.is_ok());
    }

    /// A `canceled` update carries what filled, and an unknown fill stays unknown, not zero.
    #[test]
    fn a_canceled_update_carries_its_fill_or_none() {
        let cancelled = |filled_qty: Option<&'static str>| {
            let mut order = make_order_ws("ord-3", "AAPL", "sell", "0");
            order.filled_qty = filled_qty;
            let update = AlpacaTradeUpdate {
                event: SmolStr::new("canceled"),
                execution_id: None,
                order,
                price: None,
                qty: None,
                timestamp: Some("2025-04-18T14:30:00Z"),
            };
            let event =
                sole_event(convert_trade_update(update)).expect("canceled should produce an event");
            let AccountEventKind::OrderCancelled(response) = event.kind else {
                panic!("expected OrderCancelled, got {:?}", event.kind);
            };
            let Ok(cancelled) = response.state else {
                panic!("expected Ok, got {:?}", response.state);
            };
            cancelled.filled_quantity
        };

        assert_eq!(cancelled(Some("1")), Some(Decimal::ONE));
        assert_eq!(cancelled(Some("0")), Some(Decimal::ZERO));
        assert_eq!(cancelled(None), None);
        assert_eq!(cancelled(Some("not a number")), None);
    }

    /// An ended order whose `filled_qty` does not parse reports its fill unknown, not zero; a
    /// filled one filled its whole quantity regardless.
    #[test]
    fn an_ended_order_with_an_unparseable_fill_reports_it_unknown() {
        let key = OrderKey::new(
            ExchangeId::AlpacaBroker,
            InstrumentNameExchange::new("AAPL"),
            StrategyId::new("strategy"),
            ClientOrderId::new("c"),
        );
        let ended = |status: &str| {
            let mut order = make_order_response("o1", "AAPL");
            order.status = Some(status.to_string());
            order.filled_qty = "not a number".to_string();
            convert_ended_order(&order, &key).map(|order| order.state)
        };

        assert!(matches!(
            ended("canceled"),
            Some(InactiveOrderState::Cancelled(Cancelled {
                filled_quantity: None,
                ..
            }))
        ));
        assert!(matches!(
            ended("expired"),
            Some(InactiveOrderState::Expired(Expired {
                filled_quantity: None,
                ..
            }))
        ));
        assert!(matches!(
            ended("filled"),
            Some(InactiveOrderState::FullyFilled(Filled { filled_quantity, .. }))
                if filled_quantity == Decimal::ONE
        ));
    }

    #[test]
    fn test_convert_trade_update_rejected_produces_error() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("rejected"),
            execution_id: None,
            order: make_order_ws("ord-4", "SPY", "buy", "0"),
            price: None,
            qty: None,
            timestamp: None,
        };
        let event =
            sole_event(convert_trade_update(update)).expect("rejected should produce an event");
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
            filled_avg_price: None,
            status: None,
            side: "buy".to_string(),
            order_type: "market".to_string(),
            time_in_force: "day".to_string(),
            limit_price: None,
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            created_at: "2025-04-18T14:30:00Z".to_string(),
            updated_at: None,
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
            cum_qty: Some("1".to_string()),
        };
        assert!(convert_activity_to_trade(&activity).is_none());
    }

    #[test]
    fn test_convert_positions_to_balances_crypto() {
        let positions = vec![
            AlpacaPosition {
                qty_available: dec!(0.4),
                ..alpaca_position("BTC/USD", "crypto", AlpacaPositionSide::Long, dec!(0.5))
            },
            alpaca_position("ETH/USD", "crypto", AlpacaPositionSide::Long, dec!(2.0)),
            // Equity positions should be filtered out
            alpaca_position("AAPL", "us_equity", AlpacaPositionSide::Long, dec!(10)),
        ];

        // All crypto assets
        let balances = convert_positions_to_balances(&positions, &[]);
        assert_eq!(balances.len(), 2, "only crypto positions returned");
        assert_eq!(balances[0].asset.name().as_str(), "btc");
        // total = qty (0.5 BTC), free = qty_available (0.4 BTC)
        assert_eq!(balances[0].balance.total, dec!(0.5));
        assert_eq!(balances[0].balance.free, dec!(0.4));

        // Filter to BTC only
        let btc_only = vec![AssetNameExchange::new("BTC")];
        let balances = convert_positions_to_balances(&positions, &btc_only);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].asset.name().as_str(), "btc");
    }

    /// A position as `/v2/positions` reports it, with `qty_available` equal to `qty`.
    fn alpaca_position(
        symbol: &str,
        asset_class: &str,
        side: AlpacaPositionSide,
        qty: Decimal,
    ) -> AlpacaPosition {
        AlpacaPosition {
            symbol: symbol.into(),
            asset_class: asset_class.into(),
            side,
            qty,
            qty_available: qty,
            avg_entry_price: Some(dec!(100)),
            unrealized_pl: Some(dec!(0)),
        }
    }

    #[test]
    fn test_convert_positions_sign_comes_from_side_not_qty() {
        let now = Utc::now();
        let positions = vec![
            alpaca_position("AAPL", "us_equity", AlpacaPositionSide::Long, dec!(10)),
            alpaca_position("TSLA", "us_equity", AlpacaPositionSide::Short, dec!(-3)),
            // Short with an unsigned qty still reads as short.
            alpaca_position("MSFT", "us_equity", AlpacaPositionSide::Short, dec!(4)),
        ];

        let converted = convert_positions(&positions, now).unwrap();
        let quantities: Vec<_> = converted.iter().map(|(s, p)| (*s, p.quantity)).collect();
        assert_eq!(
            quantities,
            vec![("AAPL", dec!(10)), ("TSLA", dec!(-3)), ("MSFT", dec!(-4))]
        );
        assert!(converted.iter().all(|(_, p)| p.time_exchange == now));
    }

    #[test]
    fn test_convert_positions_leaves_out_crypto_and_flat() {
        let positions = vec![
            alpaca_position("BTC/USD", "crypto", AlpacaPositionSide::Long, dec!(0.5)),
            alpaca_position("AAPL", "us_equity", AlpacaPositionSide::Long, dec!(0)),
            alpaca_position("SPY", "us_equity", AlpacaPositionSide::Long, dec!(1)),
        ];

        let converted = convert_positions(&positions, Utc::now()).unwrap();
        let symbols: Vec<_> = converted.iter().map(|(s, _)| *s).collect();
        assert_eq!(symbols, vec!["SPY"]);
    }

    #[test]
    fn test_alpaca_position_decodes_equity_option_and_fractional() {
        let positions: Vec<AlpacaPosition> = serde_json::from_value(serde_json::json!([
            {
                "asset_id": "00000000-0000-0000-0000-000000000001",
                "symbol": "XYZ",
                "exchange": "NASDAQ",
                "asset_class": "us_equity",
                "qty": "-7",
                "qty_available": "-7",
                "side": "short",
                "avg_entry_price": "50.25",
                "market_value": "-351.75",
                "unrealized_pl": "0",
            },
            {
                "symbol": "XYZ271217C00055000",
                "asset_class": "us_option",
                "qty": "2",
                "qty_available": "2",
                "side": "long",
                "avg_entry_price": "1.35",
                "unrealized_pl": "-20",
            },
            {
                "symbol": "ABC",
                "asset_class": "us_equity",
                "qty": "0.125",
                "qty_available": "0.125",
                "side": "long",
                "avg_entry_price": "80",
                "unrealized_pl": null,
            },
        ]))
        .unwrap();

        let converted = convert_positions(&positions, Utc::now()).unwrap();
        let [
            (short_sym, short),
            (option_sym, option),
            (fraction_sym, fraction),
        ] = converted.as_slice()
        else {
            panic!("expected three positions, got {converted:?}");
        };

        assert_eq!(*short_sym, "XYZ");
        assert_eq!(short.quantity, dec!(-7));
        assert_eq!(short.entry_price, Some(dec!(50.25)));
        assert!(short.is_short());

        assert_eq!(*option_sym, "XYZ271217C00055000");
        assert_eq!(option.quantity, dec!(2));
        assert_eq!(option.entry_price, Some(dec!(1.35)), "per-share premium");
        assert_eq!(option.unrealized_pnl, Some(dec!(-20)));

        assert_eq!(*fraction_sym, "ABC");
        assert_eq!(fraction.quantity, dec!(0.125));
        assert_eq!(fraction.unrealized_pnl, None);

        for (_, p) in &converted {
            assert_eq!(p.margin_used, None);
            assert_eq!(p.liquidation_price, None);
            assert_eq!(p.leverage, None);
        }
    }

    /// An unknown or missing side decodes, so the crypto balances in the same response survive,
    /// but a position that needs a direction fails rather than guessing one or reading as flat.
    #[test]
    fn test_alpaca_position_unknown_side_fails_only_where_a_direction_is_needed() {
        let positions: Vec<AlpacaPosition> = serde_json::from_value(serde_json::json!([
            {
                "symbol": "BTC/USD",
                "asset_class": "crypto",
                "qty": "0.5",
                "qty_available": "0.5",
            },
            {
                "symbol": "XYZ",
                "asset_class": "us_equity",
                "qty": "1",
                "qty_available": "1",
                "side": "sideways",
            },
        ]))
        .unwrap();
        assert_eq!(positions[0].side, AlpacaPositionSide::Unknown);
        assert_eq!(positions[1].side, AlpacaPositionSide::Unknown);

        let balances = convert_positions_to_balances(&positions, &[]);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].balance.total, dec!(0.5));

        let result = convert_positions(&positions, Utc::now());
        assert!(
            matches!(&result, Err(UnindexedClientError::Internal(msg)) if msg.contains("XYZ")),
            "an unknown side must not guess a direction: {result:?}"
        );
    }

    /// `updated_at` orders an order's states; `created_at` is constant across all of them.
    #[test]
    fn order_state_time_prefers_updated_at() {
        let mut resp = make_order_response("ord-1", "SPY");
        resp.created_at = "2025-04-18T14:30:00Z".to_string();
        resp.updated_at = Some("2025-04-18T15:45:00Z".to_string());
        assert_eq!(
            order_state_time(&resp),
            parse_timestamp("2025-04-18T15:45:00Z").unwrap(),
            "updated_at must win over created_at"
        );

        // Alpaca's schema makes updated_at nullable, so creation time is the fallback.
        resp.updated_at = None;
        assert_eq!(
            order_state_time(&resp),
            parse_timestamp("2025-04-18T14:30:00Z").unwrap(),
            "absent updated_at falls back to created_at"
        );

        // An unparseable updated_at must not silently become `now`, which would order this state
        // ahead of every later one.
        resp.updated_at = Some("not-a-timestamp".to_string());
        assert_eq!(
            order_state_time(&resp),
            parse_timestamp("2025-04-18T14:30:00Z").unwrap(),
            "unparseable updated_at falls back to created_at"
        );
    }

    /// A placement response without a status reads from what filled, as one with a live status
    /// does.
    #[test]
    fn a_placement_response_without_a_status_reads_from_what_filled() {
        let instrument = InstrumentNameExchange::new("SPY");
        let mut resp = make_order_response("ord-1", "SPY");
        resp.filled_qty = "0.4".to_string();
        let OrderState::Active(ActiveOrderState::Open(open)) =
            placed_order_state(&resp, &instrument, Decimal::ONE)
        else {
            panic!("expected Open");
        };
        assert_eq!(open.filled_quantity, Decimal::new(4, 1));

        resp.filled_qty = "1".to_string();
        resp.filled_avg_price = Some("100.5".to_string());
        let state = placed_order_state(&resp, &instrument, Decimal::ONE);
        let OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) = state else {
            panic!("expected FullyFilled, got {state:?}");
        };
        assert_eq!(filled.filled_quantity, Decimal::ONE);
        assert_eq!(filled.avg_price, Some(Decimal::new(1005, 1)));
    }

    fn make_order_response(id: &str, symbol: &str) -> AlpacaOrderResponse {
        AlpacaOrderResponse {
            id: id.to_string(),
            client_order_id: None,
            symbol: symbol.to_string(),
            qty: Some("1".to_string()),
            filled_qty: "0".to_string(),
            filled_avg_price: None,
            status: None,
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            time_in_force: "day".to_string(),
            limit_price: Some("100.00".to_string()),
            stop_price: None,
            trail_percent: None,
            trail_price: None,
            created_at: "2025-04-18T14:30:00Z".to_string(),
            updated_at: None,
        }
    }

    #[test]
    fn test_build_instrument_snapshots_empty_instruments_returns_only_with_orders() {
        let orders = vec![
            make_order_response("o1", "AAPL"),
            make_order_response("o2", "SPY"),
        ];
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &[]);
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
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &instruments);
        assert_eq!(snapshots.len(), 2);
        let spy = snapshots
            .iter()
            .find(|s| s.instrument.name().as_str() == "SPY")
            .expect("SPY snapshot must be present even with no orders");
        assert!(spy.orders.is_empty());
    }

    /// A notional order: placed by dollar value, so Alpaca reports no `qty`.
    fn make_notional_order_response(id: &str, symbol: &str) -> AlpacaOrderResponse {
        AlpacaOrderResponse {
            qty: None,
            ..make_order_response(id, symbol)
        }
    }

    fn snapshot_for<'a>(
        snapshots: &'a [InstrumentAccountSnapshot<
            ExchangeId,
            AssetNameExchange,
            InstrumentNameExchange,
        >],
        symbol: &str,
    ) -> &'a InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange> {
        snapshots
            .iter()
            .find(|snapshot| snapshot.instrument.name().as_str() == symbol)
            .unwrap_or_else(|| panic!("no snapshot for {symbol}"))
    }

    /// Every open order converted, so each list is every order open on the venue, including a
    /// requested instrument with none: that entry is what lets the engine see an order that ended
    /// while the stream was down is gone.
    #[test]
    fn test_build_instrument_snapshots_declares_complete_lists_complete() {
        let instruments = [
            InstrumentNameExchange::new("AAPL"),
            InstrumentNameExchange::new("SPY"),
        ];
        for requested in [&instruments[..], &[]] {
            let orders = vec![make_order_response("o1", "AAPL")];
            let snapshots = build_instrument_snapshots(orders, Vec::new(), requested);
            assert!(!snapshots.is_empty());
            assert!(snapshots.iter().all(|snapshot| snapshot.orders_complete));
        }
    }

    /// A notional order cannot be represented, so its instrument's list is not every open order
    /// and must not say so: the engine would retire that order as absent. Other instruments are
    /// unaffected.
    #[test]
    fn test_build_instrument_snapshots_notional_order_leaves_only_its_instrument_incomplete() {
        let orders = vec![
            make_order_response("o1", "AAPL"),
            make_order_response("o2", "SPY"),
            make_notional_order_response("o3", "SPY"),
        ];
        let instruments = [
            InstrumentNameExchange::new("AAPL"),
            InstrumentNameExchange::new("SPY"),
        ];
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &instruments);

        assert!(snapshot_for(&snapshots, "AAPL").orders_complete);
        let spy = snapshot_for(&snapshots, "SPY");
        assert!(!spy.orders_complete);
        assert_eq!(spy.orders.len(), 1);
    }

    /// Unfiltered, an instrument whose only open order cannot be represented still gets an entry,
    /// one that says its list is not complete.
    #[test]
    fn test_build_instrument_snapshots_unfiltered_lists_an_instrument_with_only_a_notional_order() {
        let orders = vec![make_notional_order_response("o1", "SPY")];
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &[]);

        let spy = snapshot_for(&snapshots, "SPY");
        assert!(!spy.orders_complete);
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
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &instruments);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].instrument.name().as_str(), "AAPL");
    }

    fn position(quantity: Decimal) -> Position {
        Position::new(
            quantity,
            Some(dec!(100)),
            None,
            None,
            None,
            None,
            Utc::now(),
        )
    }

    #[test]
    fn test_build_instrument_snapshots_unfiltered_lists_positions_without_orders() {
        let orders = vec![
            make_order_response("o1", "AAPL"),
            make_order_response("o2", "MSFT"),
            make_order_response("o3", "BTC/USD"),
        ];
        let positions = vec![("AAPL", position(dec!(5))), ("TSLA", position(dec!(-2)))];

        let snapshots = build_instrument_snapshots(orders, positions, &[]);
        let listed: Vec<_> = snapshots
            .iter()
            .map(|s| {
                (
                    s.instrument.name().as_str(),
                    s.orders.len(),
                    s.position.quantity(),
                )
            })
            .collect();
        // An equity with an order but no position is flat; crypto is never a position.
        assert_eq!(
            listed,
            vec![
                ("AAPL", 1, Some(dec!(5))),
                ("MSFT", 1, Some(Decimal::ZERO)),
                ("BTC/USD", 1, None),
                ("TSLA", 0, Some(dec!(-2)))
            ]
        );
        assert!(snapshots.iter().all(|s| s.orders_complete));
    }

    #[test]
    fn test_build_instrument_snapshots_filtered_attaches_only_requested_positions() {
        let positions = vec![("AAPL", position(dec!(5))), ("TSLA", position(dec!(-2)))];
        let instruments = vec![
            InstrumentNameExchange::new("tsla"),
            InstrumentNameExchange::new("MSFT"),
            InstrumentNameExchange::new("BTC/USD"),
            InstrumentNameExchange::new("TSLA"),
        ];

        let snapshots = build_instrument_snapshots(Vec::new(), positions, &instruments);
        let listed: Vec<_> = snapshots
            .iter()
            .map(|s| (s.instrument.name().as_str(), s.position.quantity()))
            .collect();
        // A requested equity with no position is reported flat, and one requested in another case
        // still finds its position; crypto is never a position. A second name for a symbol
        // already claimed is unknown, not flat.
        assert_eq!(
            listed,
            vec![
                ("tsla", Some(dec!(-2))),
                ("MSFT", Some(Decimal::ZERO)),
                ("BTC/USD", None),
                ("TSLA", None)
            ]
        );
        assert!(!snapshots[3].orders_complete);
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
            .map(|filled_qty| {
                early_dedup_key(&AlpacaTradeUpdate {
                    event: SmolStr::new("partial_fill"),
                    execution_id: None,
                    order: make_order_ws(order_id, "SPY", "buy", filled_qty),
                    price: Some("150.00"),
                    qty: Some("1"),
                    timestamp: None,
                })
            })
            .collect();

        // REST path: recover_fills accumulates cumulative qty from per-execution activities.
        // Two activities with exec qty "1" each → cumulative 1 then 2.
        let mut cumulative = Decimal::ZERO;
        let rest_keys: Vec<SmolStr> = ["1", "1"]
            .iter()
            .map(|exec_qty| {
                cumulative += Decimal::from_str(exec_qty).unwrap();
                fill_dedup_key(order_id, cumulative)
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
    /// A fill without an `execution_id` falls back to its dedup key for its `TradeId`, the key the
    /// early dedup check reads from the raw WS fields before the event is built.
    #[test]
    fn a_ws_fill_without_an_execution_id_is_identified_by_its_dedup_key() {
        let update = AlpacaTradeUpdate {
            event: SmolStr::new("fill"),
            execution_id: None,
            order: make_order_ws("ord-abc", "SPY", "buy", "5"),
            price: Some("150.00"),
            qty: Some("5"),
            timestamp: None,
        };

        // Early path: extract key before full event construction
        let early_key = early_dedup_key(&update);

        // A fill frame also carries the order snapshot; the execution is always the first slot.
        let [event, _snapshot] = convert_trade_update(update);
        let Some(UnindexedAccountEvent {
            kind: AccountEventKind::Trade(trade),
            ..
        }) = event
        else {
            panic!("fill should produce an execution");
        };

        assert_eq!(
            trade.id.0.as_str(),
            early_key.as_str(),
            "without an execution_id the TradeId is the dedup key"
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
            execution_id: None,
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
                stop_price: None,
                trail_percent: None,
                trail_price: None,
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
            execution_id: None,
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
                stop_price: None,
                trail_percent: None,
                trail_price: None,
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
            execution_id: None,
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
                stop_price: None,
                trail_percent: None,
                trail_price: None,
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

        let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
        process_ws_text(json, &tx, &dedup, &known, &mut backoff);

        // The event must NOT be silently dropped — an OrderCancelled must be emitted.
        let event = rx.try_recv()
            .expect("rejected event without filled_qty must produce an AccountEvent, not be silently dropped");
        assert!(
            matches!(event.kind, AccountEventKind::OrderCancelled(_)),
            "rejected event must map to OrderCancelled, got: {:?}",
            event.kind
        );
    }

    /// A `trade_updates` fill frame as Alpaca sends it: its `execution_id` is the trade's id. One
    /// that is null, empty or absent falls back to the dedup key.
    #[test]
    fn process_ws_text_identifies_a_fill_by_its_execution_id() {
        let frame = |execution_id: &str| {
            format!(
                r#"{{"stream":"trade_updates","data":{{"event":"fill",{execution_id}"order":{{"id":"ord-1","client_order_id":"cid-1","symbol":"SPY","qty":"2","filled_qty":"2","side":"buy","type":"market","time_in_force":"day","status":"filled"}},"price":"100.00","qty":"2","timestamp":"2025-04-18T14:30:00Z"}}}}"#
            )
        };
        let cases = [
            (
                r#""execution_id":"524b1902-817e-446c-825b-a9fcfebbc17e","#,
                "524b1902-817e-446c-825b-a9fcfebbc17e",
            ),
            (r#""execution_id":null,"#, "ord-1:2"),
            (r#""execution_id":"","#, "ord-1:2"),
            ("", "ord-1:2"),
        ];
        for (field, expected) in cases {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            process_ws_text(
                &frame(field),
                &tx,
                &new_dedup_cache(),
                &known,
                &mut ExponentialBackoff::new(),
            );
            let Ok(UnindexedAccountEvent {
                kind: AccountEventKind::Trade(trade),
                ..
            }) = rx.try_recv()
            else {
                panic!("the fill frame {field:?} produces a Trade");
            };
            assert_eq!(trade.id.0.as_str(), expected, "frame {field:?}");
        }
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
    fn parse_order_error_401_maps_to_unauthenticated() {
        // 401 Unauthorized: invalid/expired API credentials.
        assert!(matches!(
            parse_order_error(reqwest::StatusCode::UNAUTHORIZED, "bad credentials"),
            UnindexedOrderError::Rejected(ApiError::Unauthenticated(_))
        ));
    }

    #[test]
    fn parse_order_error_403_maps_to_unauthenticated() {
        // 403 Forbidden indicates auth/permission failure — use Unauthenticated, not
        // OrderRejected or BalanceInsufficient (which could trigger incorrect retry logic).
        assert!(matches!(
            parse_order_error(reqwest::StatusCode::FORBIDDEN, "account suspended"),
            UnindexedOrderError::Rejected(ApiError::Unauthenticated(_))
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
            UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(None, _))
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
        let snapshots = build_instrument_snapshots(orders, Vec::new(), &instruments);
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
            let result = paginate_activities(&http, &rl, &server.uri(), recovery_after(), None)
                .await
                .unwrap();

            assert_eq!(result.activities.len(), 5);
            assert_eq!(result.resume, None);
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
            let result = paginate_activities(&http, &rl, &server.uri(), recovery_after(), None)
                .await
                .unwrap();

            assert_eq!(result.activities.len(), ALPACA_MAX_ACTIVITIES);
            assert_eq!(result.resume, None);
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
            let result = paginate_activities(&http, &rl, &server.uri(), recovery_after(), None)
                .await
                .unwrap();

            assert_eq!(result.activities.len(), ALPACA_MAX_ACTIVITIES + 37);
            assert_eq!(result.resume, None);
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
            let result = paginate_activities(&http, &rl, &server.uri(), recovery_after(), None)
                .await
                .unwrap();

            assert!(
                result.resume.is_some(),
                "must say where to read on after MAX_ACTIVITY_PAGES pages"
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

        // --- account_snapshot: cash balance, equity/option positions, crypto balances ---

        /// Mounts `/v2/account`, `/v2/positions` and an empty `/v2/orders` on `server`.
        /// Synthetic values only; no provider data.
        async fn mount_account_and_positions(server: &MockServer) {
            Mock::given(method("GET"))
                .and(path("/v2/account"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "ACTIVE",
                    "currency": "USD",
                    "cash": "-1500.50",
                    "equity": "4000.00",
                    "buying_power": "5000.00",
                    "options_buying_power": "2000.00",
                    "non_marginable_buying_power": "1000.00",
                })))
                .mount(server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v2/positions"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {
                        "symbol": "XYZ",
                        "asset_class": "us_equity",
                        "side": "short",
                        "qty": "-10",
                        "qty_available": "-10",
                        "avg_entry_price": "20.00",
                        "unrealized_pl": "15.50",
                    },
                    {
                        "symbol": "XYZ271217C00025000",
                        "asset_class": "us_option",
                        "side": "long",
                        "qty": "3",
                        "qty_available": "3",
                        "avg_entry_price": "0.85",
                        "unrealized_pl": "-45",
                    },
                    {
                        "symbol": "ABC",
                        "asset_class": "us_equity",
                        "side": "long",
                        "qty": "2.5",
                        "qty_available": "2.5",
                        "avg_entry_price": "40.00",
                        "unrealized_pl": "1.25",
                    },
                    {
                        "symbol": "BTC/USD",
                        "asset_class": "crypto",
                        "side": "long",
                        "qty": "0.75",
                        "qty_available": "0.5",
                        "avg_entry_price": "50000",
                        "unrealized_pl": "0",
                    },
                ])))
                .mount(server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
                .mount(server)
                .await;
        }

        fn client_for(server: &MockServer) -> AlpacaClient {
            AlpacaClient::new(AlpacaConfig::with_base_url(
                "test-key".into(),
                "test-secret".into(),
                server.uri(),
            ))
        }

        #[tokio::test]
        async fn account_snapshot_reports_cash_positions_and_crypto_balances() {
            use crate::client::ExecutionClient;
            use rust_decimal_macros::dec;

            let server = MockServer::start().await;
            mount_account_and_positions(&server).await;

            let snapshot = client_for(&server)
                .account_snapshot(&[], &[])
                .await
                .unwrap();

            // USD: cash (negative on margin), and free capped at it, not equity or buying power.
            let balances: Vec<_> = snapshot
                .balances
                .iter()
                .map(|b| (b.asset.name().as_str(), b.balance.total, b.balance.free))
                .collect();
            assert_eq!(
                balances,
                vec![
                    ("usd", dec!(-1500.50), dec!(-1500.50)),
                    ("btc", dec!(0.75), dec!(0.5)),
                ],
                "crypto stays a balance; equities and options do not appear as balances"
            );

            let positions: Vec<_> = snapshot
                .instruments
                .iter()
                .map(|i| {
                    let p = i.position.open().unwrap();
                    (
                        i.instrument.name().as_str(),
                        p.quantity,
                        p.entry_price,
                        p.unrealized_pnl,
                    )
                })
                .collect();
            assert_eq!(
                positions,
                vec![
                    ("XYZ", dec!(-10), Some(dec!(20.00)), Some(dec!(15.50))),
                    (
                        "XYZ271217C00025000",
                        dec!(3),
                        Some(dec!(0.85)),
                        Some(dec!(-45))
                    ),
                    ("ABC", dec!(2.5), Some(dec!(40.00)), Some(dec!(1.25))),
                ],
                "short equity, option and fractional positions; no crypto position"
            );
        }

        /// A USD-only request still fetches positions, since they back the instrument
        /// snapshots, and reports no crypto balance.
        #[tokio::test]
        async fn account_snapshot_usd_only_still_reports_positions() {
            use crate::client::ExecutionClient;
            use rust_decimal_macros::dec;

            let server = MockServer::start().await;
            mount_account_and_positions(&server).await;

            let usd = [AssetNameExchange::new("USD")];
            let instruments = [InstrumentNameExchange::new("XYZ")];
            let snapshot = client_for(&server)
                .account_snapshot(&usd, &instruments)
                .await
                .unwrap();

            assert_eq!(snapshot.balances.len(), 1);
            assert_eq!(snapshot.balances[0].asset, AssetNameExchange::new("USD"));
            assert_eq!(snapshot.balances[0].balance.total, dec!(-1500.50));

            assert_eq!(snapshot.instruments.len(), 1);
            assert_eq!(snapshot.instruments[0].position.quantity(), Some(dec!(-10)));
        }

        /// A non-USD request skips `/v2/account` entirely.
        #[tokio::test]
        async fn account_snapshot_non_usd_only_skips_account_request() {
            use crate::client::ExecutionClient;

            let server = MockServer::start().await;
            mount_account_and_positions(&server).await;

            let btc = [AssetNameExchange::new("btc")];
            let snapshot = client_for(&server)
                .account_snapshot(&btc, &[])
                .await
                .unwrap();

            assert_eq!(snapshot.balances.len(), 1);
            assert_eq!(snapshot.balances[0].asset, AssetNameExchange::new("btc"));
            let requests = server.received_requests().await.unwrap();
            assert!(
                requests.iter().all(|r| r.url.path() != "/v2/account"),
                "no USD requested, so /v2/account must not be fetched"
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
            use rust_decimal::Decimal;
            use rustrade_instrument::Side;
            use rustrade_instrument::exchange::ExchangeId;
            use rustrade_instrument::instrument::name::InstrumentNameExchange;
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
                    exchange: ExchangeId::AlpacaBroker,
                    instrument: InstrumentNameExchange::new("AAPL"),
                    strategy: StrategyId::new("test-strategy"),
                    cid: ClientOrderId::new("test-cid"),
                },
                state: RequestOpen {
                    side: Side::Sell,
                    price: None,
                    quantity: Decimal::new(10, 0),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: true, // This should map to SellToClose
                    market: None,
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
                order.state.is_accepted(),
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
            use rust_decimal::Decimal;
            use rustrade_instrument::Side;
            use rustrade_instrument::exchange::ExchangeId;
            use rustrade_instrument::instrument::name::InstrumentNameExchange;

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
                    exchange: ExchangeId::AlpacaBroker,
                    instrument: &instrument,
                    strategy: StrategyId::new("test-strategy"),
                    cid: ClientOrderId::new("test-cid"),
                },
                state: RequestOpen {
                    side: Side::Buy,
                    price: None,
                    quantity: Decimal::new(10, 0),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: false, // This should map to BuyToOpen
                    market: None,
                },
            };

            let result = client.open_order(request).await;

            assert!(result.is_some(), "open_order should return a result");
            let order = result.unwrap();
            assert!(
                order.state.is_accepted(),
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

        /// A 2xx body that does not decode is not transient, so a query reports it as `Internal`
        /// rather than as connectivity a caller would retry.
        #[tokio::test]
        async fn rest_with_retry_reports_an_undecodable_2xx_body_as_internal() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account"))
                .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let url = format!("{}/v2/account", server.uri());
            let result: Result<serde_json::Value, _> =
                rest_with_retry(&rl, || http.get(&url)).await;
            assert!(
                matches!(result, Err(UnindexedClientError::Internal(_))),
                "{result:?}"
            );
        }

        /// Whether `rl` still holds requests back after 50 ms.
        async fn holds_back(rl: &RateLimitTracker) -> bool {
            tokio::time::timeout(Duration::from_millis(50), rl.wait_if_blocked())
                .await
                .is_err()
        }

        /// The rate-limit headers of a response: `remaining`, and a reset 30 s ahead if `reset`.
        fn with_rate_limit(
            template: ResponseTemplate,
            remaining: u32,
            reset: bool,
        ) -> ResponseTemplate {
            let template = template.insert_header("x-ratelimit-remaining", remaining.to_string());
            if !reset {
                return template;
            }
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            template.insert_header("x-ratelimit-reset", (now_secs + 30).to_string())
        }

        /// A response reporting no requests left holds every later request back until the reset;
        /// one with requests left, or with no reset to wait for, does not.
        #[tokio::test]
        async fn rest_with_retry_pauses_once_no_requests_remain() {
            for (remaining, reset, pauses) in [(0, true, true), (1, true, false), (0, false, false)]
            {
                let server = MockServer::start().await;
                Mock::given(method("GET"))
                    .and(path("/v2/account"))
                    .respond_with(with_rate_limit(
                        ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
                        remaining,
                        reset,
                    ))
                    .mount(&server)
                    .await;

                let http = reqwest::Client::new();
                let rl = RateLimitTracker::new();
                let url = format!("{}/v2/account", server.uri());
                let _: serde_json::Value = rest_with_retry(&rl, || http.get(&url)).await.unwrap();
                assert_eq!(
                    holds_back(&rl).await,
                    pauses,
                    "remaining {remaining}, reset {reset}"
                );
            }
        }

        /// A reset further out than Alpaca's one-minute window holds requests back for a minute
        /// at most.
        #[tokio::test]
        async fn a_pause_lasts_a_minute_at_most() {
            tokio::time::pause();
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
            headers.insert(
                "x-ratelimit-reset",
                (now_secs + 3_600).to_string().parse().unwrap(),
            );
            let rl = RateLimitTracker::new();

            observe_rate_limit_remaining(&rl, &headers);
            assert!(holds_back(&rl).await);
            tokio::time::advance(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS)).await;
            assert!(!holds_back(&rl).await);
        }

        /// A cancel's response reporting no requests left holds later requests back too.
        #[tokio::test]
        async fn rest_delete_with_retry_pauses_once_no_requests_remain() {
            let server = MockServer::start().await;
            Mock::given(method("DELETE"))
                .and(path("/v2/orders/abc"))
                .respond_with(with_rate_limit(ResponseTemplate::new(204), 0, true))
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let url = format!("{}/v2/orders/abc", server.uri());
            rest_delete_with_retry(&rl, || http.delete(&url))
                .await
                .unwrap();
            assert!(holds_back(&rl).await);
        }

        /// An order Alpaca accepted (2xx) but whose response does not decode may be live, so it
        /// fails as status unknown (`Connectivity`), never as a rejection, and does not panic.
        #[tokio::test]
        async fn open_order_with_an_undecodable_2xx_response_is_status_unknown() {
            use crate::client::ExecutionClient;
            use crate::error::OrderError;
            use crate::order::request::{OrderRequestOpen, RequestOpen};
            use crate::order::state::{InactiveOrderState, OrderState};
            use crate::order::{
                OrderKey, OrderKind, TimeInForce,
                id::{ClientOrderId, StrategyId},
            };
            use rustrade_instrument::Side;
            use rustrade_instrument::exchange::ExchangeId;
            use rustrade_instrument::instrument::name::InstrumentNameExchange;

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": 1 })),
                )
                .mount(&server)
                .await;

            let instrument = InstrumentNameExchange::new("AAPL");
            let order = client_for(&server)
                .open_order(OrderRequestOpen {
                    key: OrderKey {
                        exchange: ExchangeId::AlpacaBroker,
                        instrument: &instrument,
                        strategy: StrategyId::new("test-strategy"),
                        cid: ClientOrderId::new("test-cid"),
                    },
                    state: RequestOpen {
                        side: Side::Buy,
                        price: None,
                        quantity: Decimal::new(10, 0),
                        kind: OrderKind::Market,
                        time_in_force: TimeInForce::ImmediateOrCancel,
                        position_id: None,
                        reduce_only: false,
                        market: None,
                    },
                })
                .await
                .expect("open_order should return a result");

            assert!(
                matches!(
                    order.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Connectivity(
                        _
                    )))
                ),
                "{:?}",
                order.state
            );
        }

        /// The bracket path maps an accepted (2xx) but unreadable response the same way: the
        /// parent fails as status unknown (`Connectivity`), never as a rejection.
        #[tokio::test]
        async fn open_bracket_order_with_an_undecodable_2xx_response_is_status_unknown() {
            use crate::error::OrderError;
            use crate::order::state::{InactiveOrderState, OrderState};
            use rustrade_instrument::instrument::name::InstrumentNameExchange;

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": 1 })),
                )
                .mount(&server)
                .await;

            let result = client_for(&server)
                .open_bracket_order(AlpacaBracketOrderRequest::new(
                    InstrumentNameExchange::new("SPY"),
                    crate::order::id::StrategyId::new("test"),
                    crate::order::id::ClientOrderId::new("test-bracket"),
                    Side::Buy,
                    Decimal::ONE,
                    Decimal::new(100, 0),
                    Decimal::new(120, 0),
                    Decimal::new(90, 0),
                    TimeInForce::GoodUntilCancelled { post_only: false },
                ))
                .await;

            assert!(
                matches!(
                    result.parent.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Connectivity(
                        _
                    )))
                ),
                "{:?}",
                result.parent.state
            );
        }

        // -----------------------------------------------------------------------
        // recover_fills — what a recovered fill reports, and how it is keyed
        // -----------------------------------------------------------------------
        //
        // These drive `recover_fills` itself rather than re-deriving its arithmetic in the test.
        // A key that is correct in isolation is worth nothing if the function does not produce it.

        /// The disconnect anchor the recovery tests read from.
        fn recovery_after() -> DateTime<Utc> {
            "2025-01-01T00:00:00Z".parse().expect("valid time")
        }

        /// One FILL activity as Alpaca serves it. `cum_qty` is omitted entirely when `None`, which
        /// is how the pre-existing fallback path is reached.
        fn activity_json(
            id: &str,
            order_id: &str,
            qty: &str,
            cum_qty: Option<&str>,
        ) -> serde_json::Value {
            let mut v = serde_json::json!({
                "id": id,
                "order_id": order_id,
                "symbol": "SPY",
                "side": "buy",
                "price": "100.00",
                "qty": qty,
                "transaction_time": "2025-04-18T14:30:00Z"
            });
            if let Some(c) = cum_qty {
                v["cum_qty"] = serde_json::Value::String(c.to_string());
            }
            v
        }

        /// Serve `activities` from a mock and run `recover_fills` against it, returning every
        /// event it forwarded.
        async fn drive_recover_fills_with(
            activities: Vec<serde_json::Value>,
            dedup: SharedDedupCache,
            known: &SharedKnownLiveOrders,
        ) -> Vec<UnindexedAccountEvent> {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::Value::Array(activities)),
                )
                .mount(&server)
                .await;

            let http = reqwest::Client::new();
            let rl = RateLimitTracker::new();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let outcome = recover_fills(
                &http,
                &rl,
                &[],
                &server.uri(),
                recovery_after(),
                &tx,
                &dedup,
                known,
            )
            .await;
            assert_eq!(outcome, Ok(()), "a full read leaves nothing unrecovered");
            drop(tx);

            let mut out = Vec::new();
            while let Ok(event) = rx.try_recv() {
                out.push(event);
            }
            out
        }

        async fn drive_recover_fills(
            activities: Vec<serde_json::Value>,
        ) -> Vec<UnindexedAccountEvent> {
            drive_recover_fills_with(
                activities,
                new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await
        }

        /// Run `recover_fills` for every instrument against `server`, returning what it left
        /// unread and every event it forwarded.
        async fn recover_fills_from(
            server: &MockServer,
        ) -> (Result<(), UnreadFills>, Vec<UnindexedAccountEvent>) {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let outcome = recover_fills(
                &reqwest::Client::new(),
                &RateLimitTracker::new(),
                &[],
                &server.uri(),
                recovery_after(),
                &tx,
                &new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;
            drop(tx);
            (outcome, std::iter::from_fn(|| rx.try_recv().ok()).collect())
        }

        /// A recovery read that fails delivers nothing and leaves every fill since the disconnect
        /// unread, with the request's error.
        #[tokio::test]
        async fn a_failed_recovery_read_leaves_every_fill_since_the_disconnect_unread() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(ResponseTemplate::new(500).set_body_string("unavailable"))
                .mount(&server)
                .await;

            let (outcome, events) = recover_fills_from(&server).await;

            let Err(UnreadFills {
                start,
                reason: FillRecoveryFailure::Request(_),
            }) = outcome
            else {
                panic!("expected the request's failure, got {outcome:?}");
            };
            assert_eq!(start, recovery_after(), "from the disconnect");
            assert!(events.is_empty(), "nothing forwarded: {events:?}");
        }

        /// A recovery read that stops at the page cap delivers the fills it read, then leaves
        /// those from the millisecond of the last one on unread: Alpaca orders a millisecond's
        /// fills by id, so the cut may have passed over an earlier one in it. A last time that
        /// does not parse falls back to the latest that does, and none parsing to the disconnect.
        #[tokio::test]
        async fn a_truncated_recovery_read_delivers_what_it_read_and_leaves_the_rest_unread() {
            let time = |s: &str| s.parse::<DateTime<Utc>>().expect("valid time");
            // The page's other activities are at 14:30:00.
            let cases = [
                (
                    Some("2025-04-18T14:30:01.234567Z"),
                    time("2025-04-18T14:30:01.234Z"),
                ),
                (Some("not a time"), time("2025-04-18T14:30:00Z")),
                (None, recovery_after()),
            ];
            for (last_time, expected_start) in cases {
                let mut page = make_activities_json(ALPACA_MAX_ACTIVITIES, "act");
                let Some(activities) = page.as_array_mut() else {
                    panic!("an array of activities");
                };
                match last_time {
                    Some(last_time) => {
                        if let Some(last) = activities.last_mut() {
                            last["transaction_time"] =
                                serde_json::Value::String(last_time.to_string());
                        }
                    }
                    None => {
                        for activity in activities.iter_mut() {
                            activity["transaction_time"] =
                                serde_json::Value::String("not a time".to_string());
                        }
                    }
                }
                let server = MockServer::start().await;
                Mock::given(method("GET"))
                    .and(path("/v2/account/activities"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(page))
                    .mount(&server)
                    .await;

                let (outcome, events) = recover_fills_from(&server).await;

                let fills_read = MAX_ACTIVITY_PAGES * ALPACA_MAX_ACTIVITIES;
                assert_eq!(
                    outcome,
                    Err(UnreadFills {
                        start: expected_start,
                        reason: FillRecoveryFailure::Truncated { fills_read },
                    }),
                    "last time {last_time:?}"
                );
                assert_eq!(events.len(), fills_read, "every fill read is forwarded");
            }
        }

        /// One account-wide read serves every instrument the stream covers, so one give-up names
        /// them all: the stream's list, or every instrument when it has none. It was read once.
        #[test]
        fn a_fill_recovery_give_up_covers_every_instrument_the_stream_does() {
            let start = recovery_after();
            let end = start + chrono::Duration::minutes(5);
            let spy = InstrumentNameExchange::new("SPY");
            let qqq = InstrumentNameExchange::new("QQQ");
            let cases = [
                (vec![], FillRecoveryScope::AllInstruments),
                (
                    vec![spy.clone(), qqq.clone()],
                    FillRecoveryScope::Instruments(vec![spy, qqq]),
                ),
            ];
            for (instruments, scope) in cases {
                let reason = FillRecoveryFailure::TimedOut { timeout_secs: 30 };
                let unread = UnreadFills {
                    start,
                    reason: reason.clone(),
                };
                assert_eq!(
                    fill_recovery_gave_up(&instruments, unread, end),
                    UnindexedAccountEvent::new(
                        ExchangeId::AlpacaBroker,
                        AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                            scope, start, end, 1, reason,
                        )),
                    )
                );
            }
        }

        /// A truncated read can return fills stamped after the moment recovery began, by a venue
        /// clock ahead of this host's. The give-up is still sent, its span ending at its start
        /// rather than before it.
        #[test]
        fn a_read_that_reached_past_the_recovery_start_still_reports() {
            let end = recovery_after();
            let start = end + chrono::Duration::seconds(1);
            let unread = UnreadFills {
                start,
                reason: FillRecoveryFailure::Truncated { fills_read: 5_000 },
            };
            let event = fill_recovery_gave_up(&[], unread, end);
            let AccountEventKind::FillRecoveryGaveUp(gave_up) = event.kind else {
                panic!("expected FillRecoveryGaveUp, got {event:?}");
            };
            assert_eq!((gave_up.start, gave_up.end), (start, start));
        }

        /// Run `recover_fills_or_report` for `instruments` against `server` within `timeout`,
        /// returning every event it sent.
        async fn recover_fills_or_report_from(
            server: &MockServer,
            instruments: &[InstrumentNameExchange],
            timeout: Duration,
        ) -> Vec<UnindexedAccountEvent> {
            let (tx, mut rx) = mpsc::unbounded_channel();
            recover_fills_or_report(
                &reqwest::Client::new(),
                &RateLimitTracker::new(),
                instruments,
                &server.uri(),
                recovery_after(),
                timeout,
                &tx,
                &new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;
            drop(tx);
            std::iter::from_fn(|| rx.try_recv().ok()).collect()
        }

        /// A recovery that times out forwards nothing and reports every fill since the disconnect,
        /// for the stream's instruments.
        #[tokio::test]
        async fn a_timed_out_recovery_reports_every_fill_since_the_disconnect() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!([]))
                        .set_delay(Duration::from_secs(3_600)),
                )
                .mount(&server)
                .await;
            let spy = InstrumentNameExchange::new("SPY");

            let events = recover_fills_or_report_from(
                &server,
                std::slice::from_ref(&spy),
                Duration::from_millis(50),
            )
            .await;

            let [event] = events.as_slice() else {
                panic!("only the give-up is sent: {events:?}");
            };
            let AccountEventKind::FillRecoveryGaveUp(gave_up) = &event.kind else {
                panic!("expected FillRecoveryGaveUp, got {event:?}");
            };
            assert_eq!(gave_up.scope, FillRecoveryScope::Instruments(vec![spy]));
            assert_eq!(gave_up.start, recovery_after(), "from the disconnect");
            assert!(gave_up.end >= gave_up.start);
            assert_eq!(gave_up.attempts, 1);
            assert!(
                matches!(gave_up.reason, FillRecoveryFailure::TimedOut { .. }),
                "{:?}",
                gave_up.reason
            );
        }

        /// A truncated recovery sends every fill it read before the give-up, so a consumer sees the
        /// report after what was delivered.
        #[tokio::test]
        async fn a_truncated_recovery_reports_after_the_fills_it_read() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(make_activities_json(ALPACA_MAX_ACTIVITIES, "act")),
                )
                .mount(&server)
                .await;

            let events = recover_fills_or_report_from(&server, &[], Duration::from_secs(30)).await;

            let Some((last, fills)) = events.split_last() else {
                panic!("events are sent");
            };
            assert_eq!(fills.len(), MAX_ACTIVITY_PAGES * ALPACA_MAX_ACTIVITIES);
            assert!(
                fills
                    .iter()
                    .all(|event| matches!(event.kind, AccountEventKind::Trade(_))),
                "every fill comes first"
            );
            let AccountEventKind::FillRecoveryGaveUp(gave_up) = &last.kind else {
                panic!("the give-up comes last, got {last:?}");
            };
            assert_eq!(gave_up.scope, FillRecoveryScope::AllInstruments);
            assert!(matches!(
                gave_up.reason,
                FillRecoveryFailure::Truncated { .. }
            ));
        }

        /// A FILL activity's execution id is the part of its `id` after `::`; an id without one is
        /// taken whole.
        #[test]
        fn an_activity_id_names_its_execution_after_the_separator() {
            assert_eq!(
                activity_execution_id("20261005041849348::524b1902-817e-446c-825b-a9fcfebbc17e"),
                "524b1902-817e-446c-825b-a9fcfebbc17e"
            );
            assert_eq!(activity_execution_id("act-1"), "act-1");
            assert_eq!(
                activity_execution_id("20261005041849348::"),
                "20261005041849348::"
            );
        }

        /// One fill carries one `TradeId` however it is delivered: over the account stream, by
        /// reconnect recovery, and by `fetch_trades`. So a consumer reconciling with `fetch_trades`
        /// matches what the stream already delivered.
        #[tokio::test]
        async fn a_fill_has_one_trade_id_on_the_stream_in_recovery_and_from_fetch_trades() {
            use crate::client::ExecutionClient;

            let execution_id = "524b1902-817e-446c-825b-a9fcfebbc17e";
            let activity = activity_json(
                &format!("20250418103000000::{execution_id}"),
                "ord-1",
                "2",
                Some("2"),
            );

            let [streamed, _snapshot] = convert_trade_update(AlpacaTradeUpdate {
                event: SmolStr::new("fill"),
                execution_id: Some(execution_id),
                order: super::make_order_ws("ord-1", "SPY", "buy", "2"),
                price: Some("100.00"),
                qty: Some("2"),
                timestamp: Some("2025-04-18T14:30:00Z"),
            });
            let Some(streamed) = streamed else {
                panic!("a fill converts to a Trade");
            };

            let recovered = drive_recover_fills(vec![activity.clone()]).await;
            let [recovered] = recovered.as_slice() else {
                panic!("one fill recovered: {recovered:?}");
            };

            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::Value::Array(vec![activity])),
                )
                .mount(&server)
                .await;
            let Ok(fetched) = client_for(&server)
                .fetch_trades(recovery_after(), Utc::now(), &[])
                .await
            else {
                panic!("fetch_trades reads the activity");
            };
            let [fetched] = fetched.trades.as_slice() else {
                panic!("one fill fetched: {fetched:?}");
            };

            assert_eq!(trade_of(&streamed).id.0.as_str(), execution_id);
            assert_eq!(trade_of(recovered).id.0.as_str(), execution_id);
            assert_eq!(fetched.id.0.as_str(), execution_id);
        }

        // -----------------------------------------------------------------------
        // fetch_trades — bounded reads of a span, resumed by the caller
        // -----------------------------------------------------------------------

        fn time(s: &str) -> DateTime<Utc> {
            s.parse().expect("valid time")
        }

        /// Alpaca's activities endpoint over `activities`, which are sorted by id, as observed on
        /// paper: `after` matches an activity whose time, rounded down to the millisecond, is at
        /// or after it, `until` one whose rounded time is before it, and `page_token` resumes
        /// after the activity it names.
        struct ActivitiesVenue {
            activities: Vec<serde_json::Value>,
        }

        impl Respond for ActivitiesVenue {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let query: std::collections::HashMap<String, String> =
                    request.url.query_pairs().into_owned().collect();
                let bound = |name: &str| query.get(name).map(|s| time(s));
                let (after, until) = (bound("after"), bound("until"));
                let size: usize = query
                    .get("page_size")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(ALPACA_MAX_ACTIVITIES);
                let in_bounds = self.activities.iter().filter(|activity| {
                    let at =
                        floor_millis(time(activity["transaction_time"].as_str().expect("a time")));
                    after.is_none_or(|after| at >= after) && until.is_none_or(|until| at < until)
                });
                let page: Vec<_> = match query.get("page_token") {
                    Some(token) => in_bounds
                        .skip_while(|activity| activity["id"] != token.as_str())
                        .skip(1)
                        .take(size)
                        .cloned()
                        .collect(),
                    None => in_bounds.take(size).cloned().collect(),
                };
                ResponseTemplate::new(200).set_body_json(page)
            }
        }

        /// A FILL activity on `symbol` with id `id` at `transaction_time`.
        fn fill_at(id: &str, symbol: &str, transaction_time: DateTime<Utc>) -> serde_json::Value {
            serde_json::json!({
                "id": id,
                "order_id": format!("ord-{id}"),
                "symbol": symbol,
                "side": "buy",
                "price": "100.00",
                "qty": "1",
                "cum_qty": "1",
                "transaction_time": transaction_time.to_rfc3339_opts(SecondsFormat::Micros, true),
            })
        }

        async fn serve_activities(activities: Vec<serde_json::Value>) -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(ActivitiesVenue { activities })
                .mount(&server)
                .await;
            server
        }

        /// Read `start..=end` to its end, from each call's `resume`, returning the trade ids
        /// read and how many calls it took.
        async fn read_span(
            client: &AlpacaClient,
            start: DateTime<Utc>,
            end: DateTime<Utc>,
            instruments: &[InstrumentNameExchange],
        ) -> (std::collections::HashSet<String>, usize) {
            use crate::client::ExecutionClient;

            let mut ids = std::collections::HashSet::new();
            let mut from = start;
            for calls in 1..=10 {
                let read = match client.fetch_trades(from, end, instruments).await {
                    Ok(read) => read,
                    Err(error) => panic!("call {calls} failed: {error:?}"),
                };
                ids.extend(read.trades.iter().map(|trade| trade.id.0.to_string()));
                match read.resume {
                    Some(resume) => from = resume,
                    None => return (ids, calls),
                }
            }
            panic!("the reads did not reach the span's end");
        }

        /// A span holding more fills than one call reads is read completely across calls, each
        /// resumed from the last. Three fills share each millisecond, ordered by id against
        /// their times, so a call's cut falls inside one: its last fill read is later than an
        /// unread one in the same millisecond, which a resume from the last fill's exact time
        /// would lose.
        #[tokio::test]
        async fn fetch_trades_reads_a_busy_span_completely_across_calls() {
            let base = time("2025-04-18T14:30:00Z");
            let mut activities = Vec::new();
            for ms in 0..4_000_i64 {
                let symbol = if ms % 2 == 0 { "SPY" } else { "QQQ" };
                for (k, micros) in [("a", 900), ("b", 500), ("c", 100)] {
                    let at = base + TimeDelta::milliseconds(ms) + TimeDelta::microseconds(micros);
                    activities.push(fill_at(&format!("{ms:017}::{ms}-{k}"), symbol, at));
                }
            }
            let server = serve_activities(activities).await;
            let client = client_for(&server);
            let end = time("2025-04-18T15:00:00Z");

            let (ids, calls) = read_span(&client, recovery_after(), end, &[]).await;
            assert_eq!(ids.len(), 12_000, "every fill is read once or more");
            assert_eq!(calls, 3, "5,000 activities per call");

            let spy = [InstrumentNameExchange::new("SPY")];
            let (ids, _) = read_span(&client, recovery_after(), end, &spy).await;
            assert_eq!(ids.len(), 6_000, "every SPY fill, and only those");

            let Some(requests) = server.received_requests().await else {
                panic!("requests are recorded");
            };
            assert!(
                requests
                    .iter()
                    .all(|request| request.url.query_pairs().any(
                        |(name, value)| name == "until" && value == "2025-04-18T15:00:00.001Z"
                    )),
                "every request is bounded by the span's end"
            );
        }

        /// A call can spend its whole bound on other instruments' fills: it returns none, and
        /// says where to read on from.
        #[tokio::test]
        async fn fetch_trades_says_where_to_read_on_after_a_call_without_matches() {
            use crate::client::ExecutionClient;

            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(make_activities_json(ALPACA_MAX_ACTIVITIES, "act")),
                )
                .mount(&server)
                .await;

            let qqq = [InstrumentNameExchange::new("QQQ")];
            let Ok(read) = client_for(&server)
                .fetch_trades(recovery_after(), Utc::now(), &qqq)
                .await
            else {
                panic!("the read succeeds");
            };
            assert!(read.trades.is_empty(), "every fill read is SPY's");
            assert_eq!(read.resume, Some(time("2025-04-18T14:30:00Z")));
        }

        /// A full read that does not get past the span's first millisecond cannot advance, so it
        /// is an error rather than a resume that would loop.
        #[tokio::test]
        async fn fetch_trades_that_cannot_advance_is_truncated() {
            use crate::client::ExecutionClient;

            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(make_activities_json(ALPACA_MAX_ACTIVITIES, "act")),
                )
                .mount(&server)
                .await;

            let result = client_for(&server)
                .fetch_trades(time("2025-04-18T14:30:00.000500Z"), Utc::now(), &[])
                .await;
            assert!(
                matches!(
                    result,
                    Err(UnindexedClientError::Truncated { fills_read: 5_000 })
                ),
                "got {result:?}"
            );
        }

        /// The span is applied exactly, although Alpaca's bounds reach the whole of the
        /// milliseconds holding `start` and `end`.
        #[tokio::test]
        async fn fetch_trades_returns_only_the_span() {
            let (start, end) = (
                time("2025-04-18T14:30:00.123456Z"),
                time("2025-04-18T14:30:05.678901Z"),
            );
            let micro = TimeDelta::microseconds(1);
            let server = serve_activities(vec![
                fill_at("1::before", "SPY", start - micro),
                fill_at("2::start", "SPY", start),
                fill_at("3::end", "SPY", end),
                fill_at("4::after", "SPY", end + micro),
            ])
            .await;

            let (ids, calls) = read_span(&client_for(&server), start, end, &[]).await;
            assert_eq!(calls, 1);
            let mut ids: Vec<_> = ids.into_iter().collect();
            ids.sort();
            assert_eq!(ids, ["end", "start"]);
        }

        /// A span whose start is after its end is empty: nothing is requested.
        #[tokio::test]
        async fn fetch_trades_reads_nothing_for_an_empty_span() {
            use crate::client::ExecutionClient;

            let server = MockServer::start().await;
            let start = recovery_after();
            let Ok(read) = client_for(&server)
                .fetch_trades(start, start - TimeDelta::seconds(1), &[])
                .await
            else {
                panic!("an empty span reads");
            };
            assert_eq!(read, TradesRead::complete(Vec::new()));
            assert_eq!(server.received_requests().await.map(|r| r.len()), Some(0));
        }

        /// Alpaca's `after` compares at millisecond precision, so a recovery from mid-millisecond
        /// asks from that millisecond's start, or it would miss a fill later in it.
        #[tokio::test]
        async fn recovery_reads_from_the_start_of_the_disconnects_millisecond() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/account/activities"))
                .and(wiremock::matchers::query_param(
                    "after",
                    "2025-01-01T00:00:00.123Z",
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
                .expect(1)
                .mount(&server)
                .await;

            let (tx, _rx) = mpsc::unbounded_channel();
            let outcome = recover_fills(
                &reqwest::Client::new(),
                &RateLimitTracker::new(),
                &[],
                &server.uri(),
                time("2025-01-01T00:00:00.123456Z"),
                &tx,
                &new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;
            assert_eq!(outcome, Ok(()));
            server.verify().await;
        }

        /// The dedup keys `dedup` holds, oldest first.
        fn dedup_keys(dedup: &SharedDedupCache) -> Vec<String> {
            let mut keys: Vec<_> = dedup
                .lock()
                .iter()
                .map(|(key, _)| key.to_string())
                .collect();
            // The LRU iterates most recent first.
            keys.reverse();
            keys
        }

        fn trade_of(
            event: &UnindexedAccountEvent,
        ) -> &Trade<AssetNameExchange, InstrumentNameExchange> {
            match &event.kind {
                AccountEventKind::Trade(t) => t,
                other => panic!("expected a Trade event, got {other:?}"),
            }
        }

        /// A recovered fill reports where it left the order, so it advances `filled_quantity`
        /// without waiting for a `fetch_open_orders` reconciliation that the caller may never make.
        #[tokio::test]
        async fn a_recovered_fill_reports_the_cumulative_alpaca_sent() {
            let events = drive_recover_fills(vec![
                activity_json("act-1", "ord-1", "2", Some("2")),
                activity_json("act-2", "ord-1", "3", Some("5")),
            ])
            .await;

            let reported: Vec<_> = events
                .iter()
                .map(|e| trade_of(e).order_filled_quantity)
                .collect();
            assert_eq!(
                reported,
                vec![
                    Some(Decimal::from_str("2").unwrap()),
                    Some(Decimal::from_str("5").unwrap())
                ]
            );
        }

        /// The dedup key comes from the venue's own cumulative, not from counting executions
        /// within the recovery batch.
        ///
        /// An order that had already partly filled *before* the recovery window is the case that
        /// separates the two, and it is the case the previous implementation got wrong: counting
        /// from zero inside the batch yields a key the WebSocket path never emitted for that fill.
        #[tokio::test]
        async fn the_dedup_key_uses_the_venue_cumulative_not_an_intra_batch_count() {
            // Three lots filled before the window; the recovered 2-lot fill takes the order to 5.
            let dedup = new_dedup_cache();
            drive_recover_fills_with(
                vec![activity_json("act-1", "ord-1", "2", Some("5"))],
                dedup.clone(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;

            assert_eq!(
                dedup_keys(&dedup),
                vec!["ord-1:5"],
                "counting executions within the batch would key this as ord-1:2"
            );
        }

        /// The property the key exists for: one fill delivered twice, by two different paths, is
        /// one fill. Before the cumulative was carried this held only for an order whose fills lay
        /// wholly inside the recovery window -- precisely the orders least in need of recovery.
        #[tokio::test]
        async fn a_fill_already_delivered_over_websocket_is_not_recovered_twice() {
            let dedup = new_dedup_cache();

            // The same execution as it arrived over WebSocket: 2 lots, leaving the order at 5. Its
            // execution id differs from the activity's, so this also pins that dedup does not
            // rest on the TradeId: were the two ids ever to disagree, the fill is still not
            // delivered twice.
            let update = AlpacaTradeUpdate {
                event: SmolStr::new("partial_fill"),
                execution_id: Some("exec-a"),
                order: super::make_order_ws("ord-1", "SPY", "buy", "5"),
                price: Some("100.00"),
                qty: Some("2"),
                timestamp: None,
            };
            let ws_key = early_dedup_key(&update);
            assert!(
                !is_duplicate(&dedup, &ws_key),
                "precondition: first sighting"
            );

            let events = drive_recover_fills_with(
                vec![activity_json(
                    "20250418103000000::exec-b",
                    "ord-1",
                    "2",
                    Some("5"),
                )],
                dedup,
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;

            assert!(
                events.is_empty(),
                "a fill already delivered over WebSocket must not be re-delivered by recovery, \
                 got {events:?}"
            );
        }

        /// With `cum_qty` absent the key falls back to counting within the batch -- exactly what
        /// shipped before -- and the fill reports no cumulative rather than a fabricated one.
        #[tokio::test]
        async fn without_a_reported_cumulative_the_previous_behaviour_is_preserved() {
            let dedup = new_dedup_cache();
            let events = drive_recover_fills_with(
                vec![
                    activity_json("act-1", "ord-1", "2", None),
                    activity_json("act-2", "ord-1", "3", None),
                ],
                dedup.clone(),
                &KnownLiveOrders::shared(ExchangeId::AlpacaBroker),
            )
            .await;

            assert_eq!(
                dedup_keys(&dedup),
                vec!["ord-1:2", "ord-1:5"],
                "intra-batch accumulation"
            );
            let ids: Vec<_> = events.iter().map(|e| trade_of(e).id.0.as_str()).collect();
            assert_eq!(ids, vec!["act-1", "act-2"], "each keeps its activity's id");

            assert!(
                events
                    .iter()
                    .all(|e| trade_of(e).order_filled_quantity.is_none()),
                "a venue that reported no cumulative must not have one invented for it"
            );
        }

        // --- How orders ended: fetch_ended_orders, the reconnect check, and what is held ---

        /// An order as `GET /v2/orders` and `GET /v2/orders:by_client_order_id` serve it, of
        /// quantity 2.
        fn order_json(cid: &str, symbol: &str, status: &str, filled: &str) -> serde_json::Value {
            serde_json::json!({
                "id": format!("id-{cid}"),
                "client_order_id": cid,
                "symbol": symbol,
                "qty": "2",
                "filled_qty": filled,
                "filled_avg_price": if filled == "0" { None } else { Some("101.5") },
                "status": status,
                "side": "buy",
                "type": "limit",
                "time_in_force": "gtc",
                "limit_price": "100",
                "created_at": "2026-10-01T14:30:00Z",
                "updated_at": "2026-10-01T15:00:00Z"
            })
        }

        /// Serve `order` as the lookup of client order id `cid`, expecting `calls` lookups.
        async fn mount_lookup(server: &MockServer, cid: &str, order: ResponseTemplate, calls: u64) {
            use wiremock::matchers::query_param;
            Mock::given(method("GET"))
                .and(path("/v2/orders:by_client_order_id"))
                .and(query_param("client_order_id", cid))
                .respond_with(order)
                .expect(calls)
                .mount(server)
                .await;
        }

        fn ok(body: serde_json::Value) -> ResponseTemplate {
            ResponseTemplate::new(200).set_body_json(body)
        }

        fn alpaca_key(instrument: &str, cid: &str) -> UnindexedOrderKey {
            OrderKey::new(
                ExchangeId::AlpacaBroker,
                InstrumentNameExchange::new(instrument),
                StrategyId::new("strategy"),
                ClientOrderId::new(cid),
            )
        }

        fn hold(known: &SharedKnownLiveOrders, key: &UnindexedOrderKey) {
            let open = Open::new(
                VenueOrderId::Assigned(OrderId::new(format!("id-{}", key.cid))),
                Utc::now(),
                Decimal::ZERO,
            );
            known.lock().live(key, Decimal::TWO, &open);
        }

        #[tokio::test]
        async fn fetch_ended_orders_reads_each_end_alpaca_reports() {
            use rust_decimal_macros::dec;

            let server = MockServer::start().await;
            for (cid, status, filled) in [
                ("filled", "filled", "2"),
                ("canceled", "canceled", "1"),
                ("replaced", "replaced", "0"),
                ("expired", "expired", "1"),
                ("rejected", "rejected", "0"),
                ("done", "done_for_day", "1"),
                ("live", "new", "0"),
                ("unknown-status", "frozen", "0"),
            ] {
                mount_lookup(&server, cid, ok(order_json(cid, "SPY", status, filled)), 1).await;
            }
            mount_lookup(
                &server,
                "elsewhere",
                ok(order_json("elsewhere", "QQQ", "canceled", "0")),
                1,
            )
            .await;
            mount_lookup(
                &server,
                "gone",
                ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "code": 40410000,
                    "message": "order not found for gone"
                })),
                1,
            )
            .await;

            let keys: Vec<_> = [
                "filled",
                "canceled",
                "replaced",
                "expired",
                "rejected",
                "done",
                "live",
                "unknown-status",
                "elsewhere",
                "gone",
            ]
            .into_iter()
            .map(|cid| alpaca_key("spy", cid))
            .collect();
            let ended = client_for(&server).fetch_ended_orders(&keys).await.unwrap();

            let mut found: Vec<_> = ended
                .iter()
                .map(|order| (order.key.cid.0.as_str(), &order.state))
                .collect();
            found.sort_by_key(|(cid, _)| *cid);
            let [
                (canceled, InactiveOrderState::Cancelled(cancelled)),
                (expired, InactiveOrderState::Expired(expiry)),
                (filled, InactiveOrderState::FullyFilled(fill)),
                (rejected, InactiveOrderState::OpenFailed(failed)),
                (replaced, InactiveOrderState::Cancelled(replacement)),
            ] = found.as_slice()
            else {
                panic!("filled, canceled, replaced, expired and rejected: {found:?}");
            };
            assert_eq!(
                [*canceled, *expired, *filled, *rejected, *replaced],
                ["canceled", "expired", "filled", "rejected", "replaced"]
            );
            assert_eq!(
                (fill.filled_quantity, fill.avg_price),
                (dec!(2), Some(dec!(101.5)))
            );
            assert_eq!(
                cancelled.filled_quantity,
                Some(dec!(1)),
                "what filled before"
            );
            assert_eq!(expiry.filled_quantity, Some(dec!(1)));
            assert_eq!(replacement.filled_quantity, Some(Decimal::ZERO));
            assert_eq!(
                cancelled.time_exchange,
                parse_timestamp("2026-10-01T15:00:00Z").unwrap(),
                "stamped when the order last changed"
            );
            assert!(matches!(
                failed,
                OrderError::Rejected(ApiError::OrderRejected(message))
                    if message.contains("id-rejected")
            ));
            assert!(
                ended
                    .iter()
                    .all(|order| order.key.strategy == StrategyId::new("strategy")
                        && order.key.instrument == InstrumentNameExchange::new("spy")),
                "each carries the key it was asked for"
            );
        }

        #[tokio::test]
        async fn fetch_ended_orders_fails_when_a_lookup_fails() {
            let server = MockServer::start().await;
            mount_lookup(&server, "a", ok(order_json("a", "SPY", "canceled", "0")), 1).await;
            mount_lookup(&server, "b", ResponseTemplate::new(500), 1).await;

            let result = client_for(&server)
                .fetch_ended_orders(&[alpaca_key("SPY", "a"), alpaca_key("SPY", "b")])
                .await;

            assert!(
                matches!(result, Err(UnindexedClientError::Connectivity(_))),
                "{result:?}"
            );
        }

        /// One listing covers every instrument due; only the held orders it no longer shows are
        /// looked up, and only those that ended are reported and stop being held.
        #[tokio::test]
        async fn a_reconnect_check_lists_once_and_reports_the_orders_that_ended() {
            use wiremock::matchers::query_param;

            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .and(query_param("status", "open"))
                .and(query_param("symbols", "QQQ,SPY"))
                .respond_with(ok(serde_json::json!([order_json(
                    "spy-listed",
                    "SPY",
                    "new",
                    "0"
                )])))
                .expect(1)
                .mount(&server)
                .await;
            mount_lookup(&server, "spy-listed", ResponseTemplate::new(500), 0).await;
            mount_lookup(
                &server,
                "spy-cancelled",
                ok(order_json("spy-cancelled", "SPY", "canceled", "1")),
                1,
            )
            .await;
            mount_lookup(
                &server,
                "spy-done",
                ok(order_json("spy-done", "SPY", "done_for_day", "1")),
                1,
            )
            .await;
            mount_lookup(
                &server,
                "qqq-filled",
                ok(order_json("qqq-filled", "QQQ", "filled", "2")),
                1,
            )
            .await;

            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            let [listed, cancelled, done, filled] = [
                ("SPY", "spy-listed"),
                ("SPY", "spy-cancelled"),
                ("SPY", "spy-done"),
                ("QQQ", "qqq-filled"),
            ]
            .map(|(instrument, cid)| alpaca_key(instrument, cid));
            for key in [&listed, &cancelled, &done, &filled] {
                hold(&known, key);
            }
            let mut unchecked = UncheckedOrders::default();
            unchecked.open(known.lock().instruments());
            let config = Arc::new(AlpacaConfig::with_base_url(
                "test-key".into(),
                "test-secret".into(),
                server.uri(),
            ));
            let (tx, mut rx) = mpsc::unbounded_channel();

            recover_alpaca_ended_orders(
                &reqwest::Client::new(),
                &Arc::new(RateLimitTracker::new()),
                &config,
                &known,
                &mut unchecked,
                &tx,
            )
            .await;

            let mut reported: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
                .map(|event| {
                    let AccountEventKind::OrderSnapshot(
                        rustrade_integration::collection::snapshot::Snapshot(order),
                    ) = event.kind
                    else {
                        panic!("an order snapshot: {event:?}");
                    };
                    assert!(matches!(order.state, OrderState::Inactive(_)), "{order:?}");
                    order.key.cid
                })
                .collect();
            reported.sort();
            assert_eq!(reported, [filled.cid.clone(), cancelled.cid.clone()]);
            let known = known.lock();
            assert!(known.contains(&listed.cid) && known.contains(&done.cid));
            assert!(!known.contains(&cancelled.cid) && !known.contains(&filled.cid));
            assert!(unchecked.is_empty(), "both instruments checked");
        }

        /// A listing that fails charges every instrument in it, which then waits for a retry.
        #[tokio::test]
        async fn a_failed_listing_is_retried_for_every_instrument_in_it() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(ResponseTemplate::new(503))
                .expect(1)
                .mount(&server)
                .await;

            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            hold(&known, &alpaca_key("SPY", "a"));
            hold(&known, &alpaca_key("QQQ", "b"));
            let mut unchecked = UncheckedOrders::default();
            unchecked.open(known.lock().instruments());
            let config = Arc::new(AlpacaConfig::with_base_url(
                "test-key".into(),
                "test-secret".into(),
                server.uri(),
            ));
            let (tx, mut rx) = mpsc::unbounded_channel();

            recover_alpaca_ended_orders(
                &reqwest::Client::new(),
                &Arc::new(RateLimitTracker::new()),
                &config,
                &known,
                &mut unchecked,
                &tx,
            )
            .await;

            assert!(rx.try_recv().is_err(), "nothing reported");
            let now = tokio::time::Instant::now();
            assert!(
                unchecked.ready(&NoPendingFills, now).is_empty(),
                "both wait"
            );
            for instrument in ["QQQ", "SPY"] {
                assert_eq!(
                    unchecked.failed(&InstrumentNameExchange::new(instrument), now),
                    Some(crate::client::order_recovery::GapFailure::Retry(
                        Duration::from_secs(crate::client::order_recovery::GAP_RETRY_BASE_SECS * 2)
                    )),
                    "{instrument} has failed once already"
                );
            }
            assert_eq!(known.lock().instruments().len(), 2, "both still held");
        }

        #[tokio::test]
        async fn placing_listing_and_cancelling_keep_the_held_orders_in_step() {
            use crate::order::request::{OrderRequestCancel, RequestCancel, RequestOpen};

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(ok(order_json("resting", "SPY", "new", "0")))
                .up_to_n_times(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/v2/orders"))
                .respond_with(ok(order_json("filled", "SPY", "filled", "2")))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(ok(serde_json::json!([order_json(
                    "listed", "QQQ", "new", "0"
                )])))
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .and(path("/v2/orders/id-resting"))
                .respond_with(ResponseTemplate::new(204))
                .mount(&server)
                .await;
            let client = client_for(&server);
            let spy = InstrumentNameExchange::new("SPY");

            for cid in ["resting", "filled"] {
                let request = OrderRequestOpen {
                    key: OrderKey::new(
                        ExchangeId::AlpacaBroker,
                        &spy,
                        StrategyId::new("strategy"),
                        ClientOrderId::new(cid),
                    ),
                    state: RequestOpen {
                        side: Side::Buy,
                        price: Some(Decimal::ONE_HUNDRED),
                        quantity: Decimal::TWO,
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                        position_id: None,
                        reduce_only: false,
                        market: None,
                    },
                };
                client.open_order(request).await.unwrap();
            }
            client.fetch_open_orders(&[]).await.unwrap();
            {
                let known = client.known_live.lock();
                assert!(known.contains(&ClientOrderId::new("resting")));
                assert!(
                    !known.contains(&ClientOrderId::new("filled")),
                    "filled in its placement response"
                );
                assert!(known.contains(&ClientOrderId::new("listed")));
            }

            let cancel = client
                .cancel_order(OrderRequestCancel {
                    key: OrderKey::new(
                        ExchangeId::AlpacaBroker,
                        &spy,
                        StrategyId::new("strategy"),
                        ClientOrderId::new("resting"),
                    ),
                    state: RequestCancel {
                        id: Some(VenueOrderId::Assigned(OrderId::new("id-resting"))),
                    },
                })
                .await
                .unwrap();
            let Ok(cancelled) = &cancel.state else {
                panic!("expected Ok, got {cancel:?}");
            };
            assert_eq!(
                cancelled.filled_quantity, None,
                "the DELETE response has no body, so what filled is unknown"
            );
            assert!(
                client
                    .known_live
                    .lock()
                    .contains(&ClientOrderId::new("resting")),
                "a cancel Alpaca has only accepted has not ended the order"
            );
        }

        /// An order that ended in its placement response is reported as it ended, on both
        /// placement paths, and is not held as live: an IOC that found no liquidity or partly
        /// filled and expired the rest, one cancelled or replaced, one rejected after Alpaca
        /// accepted it, and one filled. A live status is reported open, with what filled (zero
        /// when that is unknown), and held, as is one this version does not know; one whose fill
        /// covers the quantity is reported filled.
        #[tokio::test]
        async fn an_order_that_ended_in_its_placement_response_is_reported_as_it_ended() {
            use crate::order::request::RequestOpen;

            let Some(time) = parse_timestamp("2026-10-01T15:00:00Z") else {
                panic!("order_json's updated_at parses");
            };
            let id = |cid: &str| OrderId::new(format!("id-{cid}"));
            let open = |cid: &str, filled| {
                OrderState::active(Open::new(VenueOrderId::Assigned(id(cid)), time, filled))
            };
            let cases: [(&str, &str, &str, UnindexedOrderState, bool); 13] = [
                (
                    "ioc-unfilled",
                    "expired",
                    "0",
                    OrderState::expired(Expired::new(
                        id("ioc-unfilled"),
                        time,
                        Some(Decimal::ZERO),
                    )),
                    false,
                ),
                (
                    "ioc-partial",
                    "expired",
                    "1",
                    OrderState::expired(Expired::new(id("ioc-partial"), time, Some(Decimal::ONE))),
                    false,
                ),
                (
                    "fill-unknown",
                    "expired",
                    "not-a-number",
                    OrderState::expired(Expired::new(id("fill-unknown"), time, None)),
                    false,
                ),
                (
                    "cancelled",
                    "canceled",
                    "1",
                    OrderState::inactive(Cancelled::new(id("cancelled"), time, Some(Decimal::ONE))),
                    false,
                ),
                (
                    "rejected",
                    "rejected",
                    "0",
                    OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        "Alpaca rejected order id-rejected after accepting it".to_string(),
                    ))),
                    false,
                ),
                (
                    "filled",
                    "filled",
                    "2",
                    OrderState::fully_filled(Filled::new(
                        id("filled"),
                        time,
                        Decimal::TWO,
                        Some(Decimal::new(1015, 1)),
                    )),
                    false,
                ),
                (
                    "replaced",
                    "replaced",
                    "0",
                    OrderState::inactive(Cancelled::new(id("replaced"), time, Some(Decimal::ZERO))),
                    false,
                ),
                (
                    "live-covered",
                    "partially_filled",
                    "2",
                    OrderState::fully_filled(Filled::new(
                        id("live-covered"),
                        time,
                        Decimal::TWO,
                        Some(Decimal::new(1015, 1)),
                    )),
                    false,
                ),
                ("new", "new", "0", open("new", Decimal::ZERO), true),
                (
                    "live-fill-unknown",
                    "new",
                    "not-a-number",
                    open("live-fill-unknown", Decimal::ZERO),
                    true,
                ),
                (
                    "partial",
                    "partially_filled",
                    "1",
                    open("partial", Decimal::ONE),
                    true,
                ),
                (
                    "for-the-day",
                    "done_for_day",
                    "1",
                    open("for-the-day", Decimal::ONE),
                    true,
                ),
                (
                    "unknown",
                    "a_status_not_yet_documented",
                    "0",
                    open("unknown", Decimal::ZERO),
                    true,
                ),
            ];

            for (cid, status, filled, expected, held) in cases {
                for bracket in [false, true] {
                    let server = MockServer::start().await;
                    Mock::given(method("POST"))
                        .and(path("/v2/orders"))
                        .respond_with(ok(order_json(cid, "SPY", status, filled)))
                        .expect(1)
                        .mount(&server)
                        .await;
                    let client = client_for(&server);
                    let spy = InstrumentNameExchange::new("SPY");
                    let time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };

                    let state = if bracket {
                        client
                            .open_bracket_order(AlpacaBracketOrderRequest::new(
                                spy,
                                StrategyId::new("strategy"),
                                ClientOrderId::new(cid),
                                Side::Buy,
                                Decimal::TWO,
                                Decimal::ONE_HUNDRED,
                                Decimal::new(120, 0),
                                Decimal::new(90, 0),
                                time_in_force,
                            ))
                            .await
                            .parent
                            .state
                    } else {
                        let request = OrderRequestOpen {
                            key: OrderKey::new(
                                ExchangeId::AlpacaBroker,
                                &spy,
                                StrategyId::new("strategy"),
                                ClientOrderId::new(cid),
                            ),
                            state: RequestOpen {
                                side: Side::Buy,
                                price: Some(Decimal::ONE_HUNDRED),
                                quantity: Decimal::TWO,
                                kind: OrderKind::Limit,
                                time_in_force,
                                position_id: None,
                                reduce_only: false,
                                market: None,
                            },
                        };
                        let Some(order) = client.open_order(request).await else {
                            panic!("open_order returns the order");
                        };
                        order.state
                    };

                    assert_eq!(state, expected, "{status} {filled}, bracket: {bracket}");
                    assert_eq!(
                        client.known_live.lock().contains(&ClientOrderId::new(cid)),
                        held,
                        "{status} {filled}, bracket: {bracket}"
                    );
                }
            }
        }

        /// The stream holds an acknowledged order, keeps holding one done for the day (reported
        /// open, not retired), and drops one cancelled.
        #[test]
        fn the_stream_keeps_the_held_orders_in_step() {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let dedup = new_dedup_cache();
            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            let mut backoff = ExponentialBackoff::new();
            let frame = |event: &str, filled: &str| {
                format!(
                    r#"{{"stream":"trade_updates","data":{{"event":"{event}","order":{{"id":"id-a","client_order_id":"a","symbol":"SPY","qty":"2","filled_qty":"{filled}","side":"buy","type":"limit","time_in_force":"gtc","limit_price":"100","status":"{event}"}}}}}}"#
                )
            };
            let cid = ClientOrderId::new("a");

            process_ws_text(&frame("new", "0"), &tx, &dedup, &known, &mut backoff);
            assert!(known.lock().contains(&cid), "acknowledged");

            process_ws_text(
                &frame("done_for_day", "1"),
                &tx,
                &dedup,
                &known,
                &mut backoff,
            );
            assert!(known.lock().contains(&cid), "done for the day is not ended");

            process_ws_text(&frame("canceled", "1"), &tx, &dedup, &known, &mut backoff);
            assert!(!known.lock().contains(&cid), "cancelled");

            let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            let [_, done, cancelled] = events.as_slice() else {
                panic!("three events: {events:?}");
            };
            let AccountEventKind::OrderSnapshot(
                rustrade_integration::collection::snapshot::Snapshot(order),
            ) = &done.kind
            else {
                panic!("done_for_day reports an order snapshot: {done:?}");
            };
            let OrderState::Active(ActiveOrderState::Open(open)) = &order.state else {
                panic!("open: {order:?}");
            };
            assert_eq!(open.filled_quantity, Decimal::ONE, "with what it filled");
            assert!(matches!(
                cancelled.kind,
                AccountEventKind::OrderCancelled(_)
            ));
        }

        /// A recovered fill that brings an order to its full quantity ends it, so a reconnect's
        /// check does not ask about it.
        #[tokio::test]
        async fn a_recovered_fill_that_completes_an_order_stops_it_being_held() {
            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            let key = alpaca_key("SPY", "a");
            known.lock().live(
                &key,
                Decimal::TWO,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new("ord-1")),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );

            drive_recover_fills_with(
                vec![activity_json("act-1", "ord-1", "1", Some("1"))],
                new_dedup_cache(),
                &known,
            )
            .await;
            assert!(known.lock().contains(&key.cid), "half filled");

            drive_recover_fills_with(
                vec![activity_json("act-2", "ord-1", "1", Some("2"))],
                new_dedup_cache(),
                &known,
            )
            .await;
            assert!(!known.lock().contains(&key.cid), "filled");
        }

        /// The first check runs as soon as the loop is polled, and a failed one waits for its
        /// backoff rather than running again at once.
        #[tokio::test]
        async fn the_check_loop_runs_at_once_then_waits_out_a_failure() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/orders"))
                .respond_with(ResponseTemplate::new(503))
                .expect(1)
                .mount(&server)
                .await;
            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            hold(&known, &alpaca_key("SPY", "a"));
            let mut unchecked = UncheckedOrders::default();
            unchecked.open(known.lock().instruments());
            let config = Arc::new(AlpacaConfig::with_base_url(
                "test-key".into(),
                "test-secret".into(),
                server.uri(),
            ));
            let (tx, _rx) = mpsc::unbounded_channel();

            let ran = tokio::time::timeout(
                Duration::from_secs(1),
                run_order_checks(
                    &reqwest::Client::new(),
                    &Arc::new(RateLimitTracker::new()),
                    &config,
                    &known,
                    &mut unchecked,
                    &tx,
                ),
            )
            .await;

            assert!(ran.is_err(), "the loop never completes");
            assert!(
                unchecked.contains(&InstrumentNameExchange::new("SPY")),
                "SPY waits for its retry"
            );
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                1,
                "the failed check is not run again at once"
            );
        }

        /// Once the consumer has gone, the loop asks nothing.
        #[tokio::test]
        async fn the_check_loop_waits_once_the_consumer_has_gone() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(503))
                .expect(0)
                .mount(&server)
                .await;
            let known = KnownLiveOrders::shared(ExchangeId::AlpacaBroker);
            hold(&known, &alpaca_key("SPY", "a"));
            let mut unchecked = UncheckedOrders::default();
            unchecked.open(known.lock().instruments());
            let config = Arc::new(AlpacaConfig::with_base_url(
                "test-key".into(),
                "test-secret".into(),
                server.uri(),
            ));
            let (tx, rx) = mpsc::unbounded_channel();
            drop(rx);

            let ran = tokio::time::timeout(
                Duration::from_millis(200),
                run_order_checks(
                    &reqwest::Client::new(),
                    &Arc::new(RateLimitTracker::new()),
                    &config,
                    &known,
                    &mut unchecked,
                    &tx,
                ),
            )
            .await;

            assert!(ran.is_err(), "the loop never completes");
            assert!(
                server.received_requests().await.unwrap().is_empty(),
                "nothing asked once the consumer has gone"
            );
        }
    }
}
