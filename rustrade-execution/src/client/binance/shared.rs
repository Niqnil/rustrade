//! Shared Binance execution infrastructure.
//!
//! Exchange-agnostic building blocks used by both the spot and margin clients:
//! reconnect/backoff, rate-limit tracking, the connection-manager stream guard, Binance error
//! parsing/classification, and the Binance-string → rustrade-enum parsers (side/order-kind/TIF).
//!
//! Nothing here is spot- or margin-specific: the parsers operate on rustrade's own types
//! (`OrderKind`, `TimeInForce`, `Side`) or on Binance's stable wire strings, so both clients
//! reuse them unchanged. The two event converters join them the same way, each naming the field
//! subset it reads as a trait -- [`BinanceOrderFields`] for the REST order-response endpoints,
//! [`BinanceExecutionReportFields`] for the WebSocket user-data `executionReport`. Those subsets
//! are provably identical across every SDK type implementing them, even though the structs around
//! them are not, which is what makes one converter safe to share and keeps a venue name out of
//! both. The remaining SDK-typed converters, which differ between spot's WS-API enums and
//! margin's REST params, deliberately stay in their respective modules.
//!
//! Event deduplication lives in [`crate::client::dedup`], shared with the other clients that
//! need it, and is re-exported here so Binance call sites keep a single import site.

use crate::{
    AccountEventKind, UnindexedAccountEvent,
    error::{ApiError, ConnectivityError, OrderError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce, TrailingOffsetType,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::UnindexedOrderResponseCancel,
        state::{Cancelled, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};

// Deduplication moved to `client::dedup` when Hyperliquid needed the same machinery; re-exported
// here so the spot and margin call sites keep naming one module.
pub(crate) use crate::client::dedup::{
    SharedDedupCache, dedup_key_from_event, is_duplicate, new_dedup_cache,
};
use binance_sdk::common::errors::{ConnectorError, WebsocketError};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use smol_str::format_smolstr;
use std::{
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, trace, warn};

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
// Compile-time guard: reconnect sites cast this to i64 (`chrono::Duration::seconds`); ensure it
// never overflows.
const _: () = assert!(
    HEARTBEAT_TIMEOUT_SECS <= i64::MAX as u64,
    "HEARTBEAT_TIMEOUT_SECS overflows i64"
);
/// Timeout for fill recovery REST queries after reconnect.
pub(crate) const FILL_RECOVERY_TIMEOUT_SECS: u64 = 30;
/// Timeout for the initial WebSocket API TCP+TLS handshake.
/// Without this, a network partition holds the write lock for up to 75–127 s
/// (OS TCP timeout), stalling all concurrent open_order/cancel_order callers.
pub(crate) const CONNECT_TIMEOUT_SECS: u64 = 15;
// Compile-time guard: fill-recovery sites cast this to i64 (`chrono::Duration::seconds`); ensure it
// never overflows.
const _: () = assert!(
    CONNECT_TIMEOUT_SECS <= i64::MAX as u64,
    "CONNECT_TIMEOUT_SECS overflows i64"
);
/// Extra lookback subtracted from Signal disconnect timestamps to cover Tokio scheduling
/// jitter between the actual WS close and when the monitor task records Utc::now().
/// The dedup cache absorbs any resulting duplicate fills.
pub(crate) const SIGNAL_RECOVERY_LOOKBACK_MS: i64 = 500;
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

    /// Number of reconnect attempts consumed so far.
    ///
    /// After [`wait`](Self::wait) has returned `false` this equals `max_attempts` — i.e. the
    /// number of attempts made before the budget was exhausted. Used to populate
    /// [`crate::error::StreamTerminationReason::ReconnectBudgetExhausted`].
    pub(crate) fn attempts(&self) -> u32 {
        self.attempt
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
// Order-response conversion (REST open-order / all-order endpoints)
// ---------------------------------------------------------------------------

/// The subset of a Binance order-response struct that [`convert_open_order`] reads.
///
/// binance-sdk generates a *distinct* response type per endpoint, with no shared trait:
/// `AllOrdersResponseInner` and `GetOpenOrdersResponseInner` on spot,
/// `QueryMarginAccountsOpenOrdersResponseInner` on margin. Those structs are emphatically not
/// interchangeable -- the spot family carries substantially more fields than the margin one, and
/// several fields share a name while differing in type -- but the eleven named here are identical
/// in name and type across all of them.
///
/// Naming that read subset is what makes a single converter safe to share. The dependency surface
/// is explicit, so an SDK change to any *other* field cannot silently alter order parsing, and a
/// correction to the conversion reaches every Binance client at once instead of one of them.
pub(crate) trait BinanceOrderFields {
    fn order_id(&self) -> Option<i64>;
    fn client_order_id(&self) -> Option<&str>;
    fn side(&self) -> Option<&str>;
    fn price(&self) -> Option<&str>;
    fn orig_qty(&self) -> Option<&str>;
    fn executed_qty(&self) -> Option<&str>;
    fn order_type(&self) -> Option<&str>;
    fn time_in_force(&self) -> Option<&str>;
    fn time(&self) -> Option<i64>;
    /// When the order last changed state, as opposed to [`BinanceOrderFields::time`], which is when
    /// it was created. Every endpoint implemented below carries it.
    fn update_time(&self) -> Option<i64>;
    fn symbol(&self) -> Option<&str>;
}

/// Implement [`BinanceOrderFields`] for SDK response types that share these field names.
///
/// Every struct listed below declares these eleven fields with the same types, so the accessors
/// are identical; a macro keeps them from drifting apart under hand-editing.
macro_rules! impl_binance_order_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceOrderFields for $t {
                fn order_id(&self) -> Option<i64> { self.order_id }
                fn client_order_id(&self) -> Option<&str> { self.client_order_id.as_deref() }
                fn side(&self) -> Option<&str> { self.side.as_deref() }
                fn price(&self) -> Option<&str> { self.price.as_deref() }
                fn orig_qty(&self) -> Option<&str> { self.orig_qty.as_deref() }
                fn executed_qty(&self) -> Option<&str> { self.executed_qty.as_deref() }
                fn order_type(&self) -> Option<&str> { self.r#type.as_deref() }
                fn time_in_force(&self) -> Option<&str> { self.time_in_force.as_deref() }
                fn time(&self) -> Option<i64> { self.time }
                fn update_time(&self) -> Option<i64> { self.update_time }
                fn symbol(&self) -> Option<&str> { self.symbol.as_deref() }
            }
        )*
    };
}

impl_binance_order_fields!(
    binance_sdk::spot::rest_api::AllOrdersResponseInner,
    binance_sdk::spot::rest_api::GetOpenOrdersResponseInner,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsOpenOrdersResponseInner,
);

/// Convert a Binance open order into rustrade's `Open` state order.
///
/// `exchange` stamps the resulting [`OrderKey`] and every diagnostic below, so one
/// implementation serves each Binance client without a venue name baked into its warnings.
pub(crate) fn convert_open_order<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let order_id_raw = match o.order_id() {
        Some(id) => id,
        None => {
            warn!(%exchange, %instrument, "Binance open order missing orderId");
            return None;
        }
    };
    let order_id = OrderId(format_smolstr!("{}", order_id_raw));
    if o.client_order_id().is_none() {
        warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing clientOrderId, using orderId as fallback — order may not reconcile with engine state");
    }
    let cid = ClientOrderId::new(
        o.client_order_id()
            .unwrap_or(&format_smolstr!("{}", order_id_raw)),
    );
    let side = match o.side() {
        // parse_side already logs a warning on unknown values
        Some(s) => parse_side(s)?,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing side");
            return None;
        }
    };
    let price = o.price().and_then(|s| Decimal::from_str(s).ok());
    let quantity = match o.orig_qty().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing/unparseable origQty");
            return None;
        }
    };
    let filled_qty = match o.executed_qty() {
        Some(s) => match Decimal::from_str(s) {
            Ok(v) => v,
            Err(_) => {
                warn!(%exchange, %instrument, order_id = %order_id_raw, executed_qty = s, "Binance open order unparseable executedQty, defaulting to 0");
                Decimal::ZERO
            }
        },
        None => Decimal::ZERO,
    };
    let kind = match o.order_type() {
        // parse_order_kind already logs a warning on unknown values
        Some(t) => parse_order_kind(t)?,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing type");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(o.time_in_force().unwrap_or("GTC"));
    // `update_time` over `time`: `Open::time_exchange` orders an order's states, and the engine
    // discards a snapshot older than the state it already tracks. `time` is the creation stamp and
    // is identical across every snapshot of one order, so a snapshot carrying it is discarded the
    // moment a WebSocket fill has advanced the tracked order past creation -- which is exactly the
    // partially-filled order this fetch exists to reconcile.
    let time_exchange = match o
        .update_time()
        .or_else(|| o.time())
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(ts) => ts,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing/unparseable time, using now");
            Utc::now()
        }
    };

    Some(Order {
        key: OrderKey::new(
            exchange,
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
        state: Open::new(VenueOrderId::Assigned(order_id), time_exchange, filled_qty),
    })
}

/// Convert an open-order response whose instrument is recovered from its own `symbol` field,
/// rather than supplied by the caller. Used by the no-symbol "return all" path
/// (each client's no-symbol "return all" fetch), where each order may belong to a different instrument. Drops
/// (with a warning) any order missing `symbol`; otherwise delegates to [`convert_open_order`].
pub(crate) fn convert_open_order_owned_symbol<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let instrument = match o.symbol() {
        Some(s) => InstrumentNameExchange::new(s),
        None => {
            warn!(%exchange, "Binance open order missing symbol in return-all query, dropping order");
            return None;
        }
    };
    convert_open_order(o, exchange, &instrument)
}

// ---------------------------------------------------------------------------
// executionReport conversion (WebSocket user-data stream)
// ---------------------------------------------------------------------------

/// The subset of a Binance `executionReport` that [`convert_execution_report`] reads.
///
/// As with [`BinanceOrderFields`], binance-sdk generates a distinct nominal type per stream
/// family and gives them no shared trait: `spot::websocket_api::ExecutionReport` and
/// `margin_trading::websocket_streams::ExecutionReport`. The two are not interchangeable -- spot
/// declares 55 fields to margin's 50, and eight fields sharing a name differ in type
/// (`Option<i64>` on spot against `Option<String>` on margin). None of those eight is named here;
/// the eighteen below are identical in name and type across both.
///
/// Naming that read subset is what makes a single converter safe to share, and keeps the
/// dependency on the SDK explicit: a change to any *other* field cannot silently alter execution
/// handling, and a correction to the conversion reaches every Binance client at once.
///
/// Accessors borrow rather than consume, so one report can serve both events a `TRADE` produces.
pub(crate) trait BinanceExecutionReportFields {
    /// Binance field `x`: what happened (`NEW`, `TRADE`, `CANCELED`, ...).
    fn execution_type(&self) -> Option<&str>;
    /// Binance field `X`: the order's status *after* this execution, which is not the same
    /// question as `execution_type` -- a `TRADE` may leave the order `PARTIALLY_FILLED`,
    /// `FILLED`, or already retired by a report that overtook it.
    fn order_status(&self) -> Option<&str>;
    /// Binance field `s`.
    fn symbol(&self) -> Option<&str>;
    /// Binance field `S`.
    fn side(&self) -> Option<&str>;
    /// Binance field `i`: the venue's order id.
    fn order_id(&self) -> Option<i64>;
    /// Binance field `c`.
    fn client_order_id(&self) -> Option<&str>;
    /// Binance field `T`: transaction time, in milliseconds.
    fn transaction_time(&self) -> Option<i64>;
    /// Binance field `t`: the execution's own id, present only on a `TRADE`.
    fn trade_id(&self) -> Option<i64>;
    /// Binance field `L`: the price of this execution alone.
    fn last_executed_price(&self) -> Option<&str>;
    /// Binance field `l`: the quantity of this execution alone.
    fn last_executed_quantity(&self) -> Option<&str>;
    /// Binance field `n`.
    fn commission_amount(&self) -> Option<&str>;
    /// Binance field `N`: the asset the commission was charged in, which need not be either leg
    /// of the traded pair.
    fn commission_asset(&self) -> Option<&str>;
    /// Binance field `z`: the *order's* cumulative filled quantity as of this execution, as
    /// opposed to `last_executed_quantity`, which is this execution alone.
    fn cumulative_filled_quantity(&self) -> Option<&str>;
    /// Binance field `r`.
    fn reject_reason(&self) -> Option<&str>;
    /// Binance field `o`.
    fn order_type(&self) -> Option<&str>;
    /// Binance field `p`: the order's limit price, `"0"` on order kinds that carry none.
    fn price(&self) -> Option<&str>;
    /// Binance field `q`: the order's total quantity, as opposed to any executed portion of it.
    fn order_quantity(&self) -> Option<&str>;
    /// Binance field `f`.
    fn time_in_force(&self) -> Option<&str>;
}

/// Implement [`BinanceExecutionReportFields`] for SDK types that share these field names.
///
/// Every struct listed below declares these eighteen fields with the same types, so the accessors
/// are identical; a macro keeps them from drifting apart under hand-editing.
macro_rules! impl_binance_execution_report_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceExecutionReportFields for $t {
                fn execution_type(&self) -> Option<&str> { self.x.as_deref() }
                fn order_status(&self) -> Option<&str> { self.x_uppercase.as_deref() }
                fn symbol(&self) -> Option<&str> { self.s.as_deref() }
                fn side(&self) -> Option<&str> { self.s_uppercase.as_deref() }
                fn order_id(&self) -> Option<i64> { self.i }
                fn client_order_id(&self) -> Option<&str> { self.c.as_deref() }
                fn transaction_time(&self) -> Option<i64> { self.t_uppercase }
                fn trade_id(&self) -> Option<i64> { self.t }
                fn last_executed_price(&self) -> Option<&str> { self.l_uppercase.as_deref() }
                fn last_executed_quantity(&self) -> Option<&str> { self.l.as_deref() }
                fn commission_amount(&self) -> Option<&str> { self.n.as_deref() }
                fn commission_asset(&self) -> Option<&str> { self.n_uppercase.as_deref() }
                fn cumulative_filled_quantity(&self) -> Option<&str> { self.z.as_deref() }
                fn reject_reason(&self) -> Option<&str> { self.r.as_deref() }
                fn order_type(&self) -> Option<&str> { self.o.as_deref() }
                fn price(&self) -> Option<&str> { self.p.as_deref() }
                fn order_quantity(&self) -> Option<&str> { self.q.as_deref() }
                fn time_in_force(&self) -> Option<&str> { self.f.as_deref() }
            }
        )*
    };
}

impl_binance_execution_report_fields!(
    binance_sdk::spot::websocket_api::ExecutionReport,
    binance_sdk::margin_trading::websocket_streams::ExecutionReport,
);

/// Whether a `TRADE` report's order status says the order is still live at the exchange, and may
/// therefore be written into engine state as an `Open` snapshot.
///
/// A `TRADE` report carries the order's status (`X`) alongside the execution. Only
/// `PARTIALLY_FILLED` and `FILLED` say the order reached this execution while working; every other
/// status means some other report owns the order's current state. Writing an `Open` snapshot from
/// one of those would resurrect an order the engine has already retired, leaving a resting order
/// that does not exist at the exchange -- a fill that arrives after its order's terminal report is
/// exactly the ordering this guards against.
fn trade_order_is_live(status: &str) -> bool {
    matches!(status, "PARTIALLY_FILLED" | "FILLED")
}

/// Convert a Binance `executionReport` into rustrade account events, appended to `buf`.
///
/// A `TRADE` report genuinely carries two facts -- the execution print (`l`/`L`) and the order's
/// new cumulative filled quantity (`z`) -- so it appends both a `Trade` and an `OrderSnapshot`.
/// Every other execution type appends at most one event, and an unusable report appends none.
///
/// The `Trade` is always first. A fully-filled snapshot retires its order, and routing a fill
/// against an order that has already been retired is a strictly harder problem than routing it
/// against a live one; appending the execution first keeps the easy ordering.
///
/// `exchange` stamps every event and every diagnostic below, so one implementation serves each
/// Binance client without a venue name baked into its warnings.
// Inherent complexity: one arm per Binance execution type (TRADE, NEW, CANCELED, EXPIRED,
// REJECTED, REPLACE), each validating the fields its own variant needs.
#[allow(clippy::cognitive_complexity)]
pub(crate) fn convert_execution_report<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    buf: &mut Vec<UnindexedAccountEvent>,
) {
    let Some(exec_type) = report.execution_type() else {
        warn!(%exchange, "Binance executionReport missing execution type (x), dropping");
        return;
    };
    let Some(symbol) = report.symbol().map(InstrumentNameExchange::new) else {
        warn!(%exchange, "Binance executionReport missing symbol (s), dropping");
        return;
    };
    // Check order_id first -- if it is missing the event is dropped, so avoid constructing cid.
    let Some(order_id_raw) = report.order_id() else {
        warn!(%exchange, %symbol, "Binance executionReport missing orderId (i), dropping");
        return;
    };
    let order_id = OrderId(format_smolstr!("{order_id_raw}"));
    let cid = match report.client_order_id() {
        Some(c) => ClientOrderId::new(c),
        None => ClientOrderId::new(order_id.0.as_str()),
    };

    let time_exchange = match report
        .transaction_time()
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(t) => t,
        None => {
            warn!(%exchange, %symbol, "Binance executionReport missing/unparseable transaction time (T), using now");
            Utc::now()
        }
    };

    match exec_type {
        "NEW" => {
            buf.extend(convert_order_snapshot(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        "TRADE" => {
            // Partial or full fill.
            let Some(trade_id) = report.trade_id() else {
                warn!(%exchange, %symbol, "Binance TRADE event missing trade ID (t), dropping");
                return;
            };
            let trade_id = TradeId(format_smolstr!("{trade_id}"));
            // parse_side already logs a warning on unknown values.
            let Some(side) = report.side().and_then(parse_side) else {
                warn!(%exchange, %symbol, "Binance TRADE event missing/unknown side (S), dropping");
                return;
            };
            let last_price = match report.last_executed_price() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%exchange, %symbol, error = %e, raw = s, "Binance TRADE event unparseable last price (L), dropping fill");
                        return;
                    }
                },
                None => {
                    warn!(%exchange, %symbol, "Binance TRADE event missing last price (L), dropping fill");
                    return;
                }
            };
            let last_qty = match report.last_executed_quantity() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%exchange, %symbol, error = %e, raw = s, "Binance TRADE event unparseable last qty (l), dropping fill");
                        return;
                    }
                },
                None => {
                    warn!(%exchange, %symbol, "Binance TRADE event missing last qty (l), dropping fill");
                    return;
                }
            };
            // Commission parse failure: log and default to 0 rather than dropping the fill.
            let commission = match Decimal::from_str(report.commission_amount().unwrap_or("0")) {
                Ok(v) => v,
                Err(e) => {
                    warn!(%exchange, %symbol, error = %e, "Binance TRADE event unparseable commission (n), defaulting to 0");
                    Decimal::ZERO
                }
            };
            // Use the venue's own commission asset (N: e.g. BNB, USDT, BTC). fees_quote is None
            // here; the indexer computes it if the fee is in the quote or base asset. The
            // "UNKNOWN" fallback (rare: the API omits N) will fail indexing.
            let fee_asset = report
                .commission_asset()
                .map(AssetNameExchange::from)
                .unwrap_or_else(|| AssetNameExchange::from("UNKNOWN"));
            // `z` is the order's cumulative filled quantity as of this execution, which is what
            // advances the order; `last_qty` above is this execution alone.
            let order_filled_quantity = report
                .cumulative_filled_quantity()
                .and_then(|s| Decimal::from_str(s).ok());

            let trade = Trade::new(
                trade_id,
                order_id.clone(),
                symbol.clone(),
                StrategyId::unknown(), // Binance doesn't carry strategy IDs
                time_exchange,
                side,
                last_price,
                last_qty,
                order_filled_quantity,
                AssetFees::new(fee_asset, commission, None),
            );
            buf.push(UnindexedAccountEvent::new(
                exchange,
                AccountEventKind::Trade(trade),
            ));

            // The execution alone does not move the order: `filled_quantity` is only ever carried
            // into engine state by an order snapshot, so without this second event a partially
            // filled order reads as having nothing filled until REST reconciliation refreshes it.
            // `convert_order_snapshot` reads field `z` (cumulative filled quantity), which is
            // exactly what a TRADE report carries, so the same builder serves both arms.
            let order_status = report.order_status().unwrap_or_default();
            if trade_order_is_live(order_status) {
                buf.extend(convert_order_snapshot(
                    report,
                    exchange,
                    symbol,
                    cid,
                    order_id,
                    time_exchange,
                ));
            } else {
                trace!(
                    %exchange,
                    %symbol,
                    status = order_status,
                    "Binance TRADE for an order the exchange no longer reports as working — \
                     emitting the execution without an order snapshot"
                );
            }
        }
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
            buf.push(cancelled_event(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        "REJECTED" => {
            // Rejected by the matching engine after initial acceptance (e.g. insufficient funds
            // discovered post-validation). Mapped to OrderCancelled with an error state so the
            // engine removes this order.
            let reject_reason = report.reject_reason().unwrap_or("unknown");
            warn!(
                %exchange, %symbol, %order_id, reason = reject_reason,
                "Binance order REJECTED by matching engine"
            );
            let response = UnindexedOrderResponseCancel {
                key: OrderKey::new(exchange, symbol, StrategyId::unknown(), cid),
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    reject_reason.to_string(),
                ))),
            };
            buf.push(UnindexedAccountEvent::new(
                exchange,
                AccountEventKind::OrderCancelled(response),
            ));
        }
        "REPLACE" => {
            // REPLACE is emitted when an order is replaced via the cancel-replace endpoint. The
            // report describes the CANCELLED original order: field `i` is the original order id
            // (already extracted as `order_id` above). The replacement arrives as a subsequent
            // NEW report with its own order id. Emitting OrderCancelled for the original is what
            // removes it from the engine's open-order book.
            buf.push(cancelled_event(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        _ => {
            // PENDING_NEW and PENDING_CANCEL are transient; the terminal state
            // (NEW/CANCELED/...) follows shortly after.
            trace!(%exchange, exec_type, "Binance ignoring execution type");
        }
    }
}

/// Build the `OrderCancelled` event shared by the `CANCELED`/`EXPIRED`/`REPLACE` arms.
///
/// All of them report an order leaving the book with whatever it had filled (`z`) at that point,
/// and differ only in why -- which the caller has already established by matching on `x`.
fn cancelled_event<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    symbol: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> UnindexedAccountEvent {
    let filled_qty = report
        .cumulative_filled_quantity()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let response = UnindexedOrderResponseCancel {
        key: OrderKey::new(exchange, symbol, StrategyId::unknown(), cid),
        state: Ok(Cancelled::new(order_id, time_exchange, filled_qty)),
    };
    UnindexedAccountEvent::new(exchange, AccountEventKind::OrderCancelled(response))
}

/// Build the `OrderSnapshot` event for a report that leaves the order resting at the exchange.
///
/// Serves both the `NEW` arm and the live half of the `TRADE` arm: the fields describing the
/// order -- side, kind, price, quantity, TIF and cumulative filled quantity -- are carried
/// identically by both, so one builder covers the acknowledgement and every fill that follows it.
///
/// Returns `None` when a field the snapshot cannot be built without is missing or unparseable;
/// each such case is warned about individually.
fn convert_order_snapshot<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    symbol: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> Option<UnindexedAccountEvent> {
    // parse_side already logs a warning on unknown values.
    let Some(side) = report.side().and_then(parse_side) else {
        warn!(%exchange, %symbol, "Binance order report missing/unknown side (S), dropping snapshot");
        return None;
    };
    // parse_order_kind already logs a warning on unknown values.
    let kind = parse_order_kind(report.order_type().unwrap_or("LIMIT"))?;
    let price: Option<Decimal> = match (report.price(), &kind) {
        (Some(p), _) => match Decimal::from_str(p) {
            Ok(v) if !v.is_zero() => Some(v),
            Ok(_) => {
                // Binance sends "0" / "0.00" as the price field for Market/Stop/TrailingStop
                // orders. Trace only when a zero arrives on an order kind that should carry a
                // limit price, so the surprising case is observable.
                if matches!(
                    kind,
                    OrderKind::Limit
                        | OrderKind::StopLimit { .. }
                        | OrderKind::TakeProfitLimit { .. }
                        | OrderKind::TrailingStopLimit { .. }
                ) {
                    trace!(%exchange, %symbol, %kind, "Binance order report has zero price (p) on limit-type order, treating as no limit price");
                }
                None
            }
            Err(e) => {
                warn!(%exchange, %symbol, price = p, error = %e, "Binance order report unparseable price (p), dropping snapshot");
                return None;
            }
        },
        (
            None,
            OrderKind::Market
            | OrderKind::Stop { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TrailingStop { .. },
        ) => {
            // Market, Stop and TakeProfit orders carry no limit price.
            None
        }
        (
            None,
            OrderKind::Limit
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfitLimit { .. }
            | OrderKind::TrailingStopLimit { .. },
        ) => {
            warn!(%exchange, %symbol, "Binance limit-type order report missing price (p), dropping snapshot");
            return None;
        }
    };
    let quantity = match report.order_quantity() {
        Some(q) => match Decimal::from_str(q) {
            Ok(v) => v,
            Err(e) => {
                warn!(%exchange, %symbol, qty = q, error = %e, "Binance order report unparseable quantity (q), dropping snapshot");
                return None;
            }
        },
        None => {
            warn!(%exchange, %symbol, "Binance order report missing quantity (q), dropping snapshot");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(report.time_in_force().unwrap_or("GTC"));
    // Field `z`: the order's cumulative filled quantity. Usually 0 on a NEW, but read it there
    // too in case of an immediate partial fill on an aggressive order; on a TRADE it is the whole
    // point of the snapshot.
    let filled_qty = report
        .cumulative_filled_quantity()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    let order = Order {
        key: OrderKey::new(
            exchange,
            symbol,
            StrategyId::unknown(), // Binance doesn't carry strategy IDs
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
        exchange,
        AccountEventKind::OrderSnapshot(rustrade_integration::collection::snapshot::Snapshot::new(
            order,
        )),
    ))
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
/// Each client maps this onto its own SDK's per-endpoint order-type enum (spot's WS-API
/// `OrderPlaceTypeEnum`, margin's REST `MarginAccountNewOrderTypeEnum`), so neither ever builds
/// the wire string by hand. Keeping the decision logic here (in [`classify_order_kind_tif`])
/// avoids duplicating the match arms across the two clients, which differ only in their SDK
/// output types.
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
        ConnectorError::UnauthorizedError { msg, code }
        | ConnectorError::ForbiddenError { msg, code } => {
            // Splice the code (Display omits it) into the message so downstream callers can tell
            // auth-failure subtypes apart (e.g. -2014 invalid key vs -2015 IP/permission), matching
            // the BadRequest arm below and this function's documented contract.
            let msg_with_code = code.map_or_else(|| msg.clone(), |c| format!("{c} {msg}"));
            OrderError::Rejected(ApiError::Unauthenticated(msg_with_code))
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
