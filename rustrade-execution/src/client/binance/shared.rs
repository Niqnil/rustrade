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
use binance_sdk::common::{
    errors::{ConnectorError, WebsocketError},
    models::ParamBuildError,
};
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

/// Check if an anyhow::Error from binance-sdk REST is a rate-limit error.
/// Covers HTTP 429 / -1003 (WAF/queue overflow: requests rejected before execution),
/// -1015 (IP rate-limit ban), and a WAF 403 (`ConnectorError::ForbiddenError` without an auth
/// code or wording). All warrant the same backoff response.
pub(crate) fn is_rate_limit_error(e: &anyhow::Error) -> bool {
    // A WAF 403 carries none of the texts below; recognise it by type so it is backed off too.
    if let Some(ConnectorError::ForbiddenError { msg, code }) = e.downcast_ref::<ConnectorError>()
        && is_waf_block(&with_code(msg, *code))
    {
        return true;
    }
    // iterate the error chain and match against the actual Display strings
    // from binance-sdk's TooManyRequestsError and RateLimitBanError variants.
    // Note: cause.to_string() allocates per chain entry — acceptable since this
    // only runs on error paths.
    for cause in e.chain() {
        let msg = cause.to_string();
        if msg.contains("Too many requests")
            || msg.contains("been banned for exceeding rate limits")
            || has_rate_limit_error_code(&msg)
        {
            return true;
        }
    }
    false
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
/// several fields share a name while differing in type -- but the twelve named here are identical
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
    /// The order's status. `openOrders` serves only live orders, but `allOrders` also serves
    /// cancelled, expired and filled ones through the same accessors, so [`convert_open_order`]
    /// reads it rather than trusting the endpoint.
    fn status(&self) -> Option<&str>;
}

/// Implement [`BinanceOrderFields`] for SDK response types that share these field names.
///
/// Every struct listed below declares these twelve fields with the same types, so the accessors
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
                fn status(&self) -> Option<&str> { self.status.as_deref() }
            }
        )*
    };
}

impl_binance_order_fields!(
    binance_sdk::spot::rest_api::AllOrdersResponseInner,
    binance_sdk::spot::rest_api::GetOpenOrdersResponseInner,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsOpenOrdersResponseInner,
);

/// Whether a REST order response's status says the order is resting at the exchange, and may
/// therefore become an `Open` order.
///
/// `NEW` and `PARTIALLY_FILLED` are working orders. `PENDING_NEW` is an order-list leg that waits
/// for its working order to fill; it is at the exchange and `openOrders` returns it, so it is live
/// too. Every other status is terminal (`FILLED`, `CANCELED`, `REJECTED`, `EXPIRED`,
/// `EXPIRED_IN_MATCH`), unused (`PENDING_CANCEL`), or unknown to this version.
///
/// This is an allow-list for the same reason [`trade_order_is_live`] is one: an `Open` snapshot of
/// an order that is no longer live resurrects it. A cancelled order that had partly filled would
/// rest in engine state with quantity remaining, and nothing at the exchange would ever fill or
/// cancel it.
fn rest_order_is_open(status: &str) -> bool {
    matches!(status, "NEW" | "PARTIALLY_FILLED" | "PENDING_NEW")
}

/// Convert a Binance open order into rustrade's `Open` state order.
///
/// Returns `None`, with a warning, for an order whose status is not live (see
/// [`rest_order_is_open`]) or is missing, whichever endpoint served it. `openOrders` serves only
/// live orders, so there this never fires in practice. An `allOrders` row for a finished order
/// cannot be expressed as `Open` at all; reading that endpoint needs a conversion to
/// [`OrderState`], which this is not.
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
    match o.status() {
        Some(status) if rest_order_is_open(status) => {}
        Some(status) => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, status, "Binance order is not live, not converting it to an open order");
            return None;
        }
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance open order missing status");
            return None;
        }
    }
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

/// One symbol's `openOrders` rows after conversion.
#[derive(Debug)]
pub(crate) struct OpenOrderListing {
    pub(crate) orders: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
    /// Whether `orders` shows every row the venue listed under the `clientOrderId` it was placed
    /// with. This is the `orders_complete` an account snapshot built from the listing may claim.
    pub(crate) complete: bool,
}

/// Convert one symbol's `openOrders` rows with [`convert_open_order`], recording whether the
/// result can stand for the whole listing.
///
/// A row the converter drops may be a live order, and a row without a `clientOrderId` is kept under
/// its `orderId`, where the order it was placed as cannot be found. Either leaves the listing
/// unable to say that an order missing from it is gone, so either makes it incomplete. A dropped
/// row that really was finished makes it incomplete too; `openOrders` does not serve those, and
/// erring towards "incomplete" costs only a reconciliation.
pub(crate) fn convert_open_order_listing<T: BinanceOrderFields>(
    rows: &[T],
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
) -> OpenOrderListing {
    let mut complete = true;
    let orders = rows
        .iter()
        .filter_map(|row| {
            let order = convert_open_order(row, exchange, instrument);
            complete &= order.is_some() && row.client_order_id().is_some();
            order
        })
        .collect();
    OpenOrderListing { orders, complete }
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
// Recovered fills: the order's cumulative filled quantity
// ---------------------------------------------------------------------------

/// Where a `myTrades` walk starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MyTradesFrom {
    /// Every execution on the instrument at or after this time, in epoch milliseconds.
    Time(i64),
    /// Every execution of this one order, from its first.
    ///
    /// Queried by `orderId` alone, Binance returns an order's oldest executions first, in
    /// ascending trade id, even when the order has more executions than `limit`; later pages
    /// continue with `fromId`. Observed on the Spot testnet (2026-09-24, a five-execution order
    /// read with `limit=2`); Margin serves the same parameters and is assumed to match.
    Order(i64),
}

/// The fields of a `myTrades` execution that fill recovery reads to rebuild an order's
/// cumulative filled quantity. Spot and margin serve different types that share these three.
pub(crate) trait BinanceExecutionFields {
    /// The trade id, which Binance assigns in increasing order per symbol.
    fn id(&self) -> Option<i64>;
    fn order_id(&self) -> Option<i64>;
    /// The size of this execution.
    fn qty(&self) -> Option<&str>;
}

macro_rules! impl_binance_execution_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceExecutionFields for $t {
                fn id(&self) -> Option<i64> { self.id }
                fn order_id(&self) -> Option<i64> { self.order_id }
                fn qty(&self) -> Option<&str> { self.qty.as_deref() }
            }
        )*
    };
}

impl_binance_execution_fields!(
    binance_sdk::spot::rest_api::MyTradesResponseInner,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsTradeListResponseInner,
);

/// How long fill recovery may spend reading recovered orders' executions, from the moment it
/// starts.
///
/// Half of [`FILL_RECOVERY_TIMEOUT_SECS`], which bounds the whole recovery and drops every fill
/// not yet sent when it expires. The lookups only enrich fills; they must never cost one. Past
/// this budget the remaining fills go out without a cumulative, as they did before it existed.
pub(crate) const ORDER_EXECUTIONS_BUDGET: Duration =
    Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS / 2);

/// How many orders' executions one instrument's recovery reads at once.
///
/// Recovery already runs up to eight instruments at once, so this allows 32 lookups in flight.
/// Each is weight 5 (Spot, `myTrades` with `orderId`) or 10 (Margin) against a limit of several
/// thousand a minute, and [`rest_call_with_retry`] backs off when Binance signals the limit.
pub(crate) const ORDER_EXECUTIONS_IN_FLIGHT: usize = 4;

/// The cumulative filled quantity of each recovered execution's order, as of that execution,
/// keyed by trade id.
///
/// `myTrades` reports executions only, with no cumulative, so a recovered fill cannot advance its
/// order on its own. For each order among `recovered`, this walks that order's executions from its
/// first with `fetch` and sums them in trade-id order. That is the figure the WebSocket reports as
/// `z` on the same execution, so a recovered fill and a live one carry the same
/// [`Trade::order_filled_quantity`]. Up to [`ORDER_EXECUTIONS_IN_FLIGHT`] orders are read at once.
///
/// An order is left out, and its recovered fills keep `None`, when its walk fails, when the walk
/// is unusable (see [`order_running_totals`]), or once `deadline` passes. Each case is logged.
/// `None` is what every recovered fill carried before this existed, and the order's state must
/// then be learned some other way.
pub(crate) async fn recovered_order_totals<T, F, Fut>(
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
    recovered: &[T],
    deadline: tokio::time::Instant,
    fetch: F,
) -> fnv::FnvHashMap<i64, Decimal>
where
    T: BinanceExecutionFields,
    F: Fn(i64) -> Fut,
    Fut: Future<Output = Result<Vec<T>, UnindexedClientError>>,
{
    use futures::StreamExt as _;

    let mut order_ids: Vec<i64> = recovered.iter().filter_map(|t| t.order_id()).collect();
    order_ids.sort_unstable();
    order_ids.dedup();

    let fetch = &fetch;
    let mut lookups = futures::stream::iter(order_ids.iter().copied())
        .map(|order_id| async move { (order_id, fetch(order_id).await) })
        .buffer_unordered(ORDER_EXECUTIONS_IN_FLIGHT);

    let mut totals = fnv::FnvHashMap::default();
    let mut settled = 0;
    loop {
        let (order_id, walk) = match tokio::time::timeout_at(deadline, lookups.next()).await {
            Ok(Some(lookup)) => lookup,
            Ok(None) => break,
            Err(_elapsed) => {
                warn!(
                    %exchange, %instrument,
                    orders_left = order_ids.len() - settled,
                    budget = ?ORDER_EXECUTIONS_BUDGET,
                    "fill recovery ran out of time reading orders' executions; the remaining \
                     orders' recovered fills carry no cumulative filled quantity"
                );
                break;
            }
        };
        settled += 1;
        let executions = match walk {
            Ok(executions) => executions,
            Err(error) => {
                warn!(
                    %exchange, %instrument, order_id, %error,
                    "fill recovery could not read the order's executions; its recovered fills \
                     carry no cumulative filled quantity"
                );
                continue;
            }
        };
        match order_running_totals(order_id, &executions) {
            Some(running) => totals.extend(running),
            None => warn!(
                %exchange, %instrument, order_id, executions = executions.len(),
                "fill recovery read an order's executions it cannot sum (a missing id or size, \
                 or another order's execution); its recovered fills carry no cumulative filled \
                 quantity"
            ),
        }
    }
    totals
}

/// `(trade id, cumulative filled quantity as of that execution)` for every execution of one
/// order, or `None` if the walk cannot be trusted to sum.
///
/// A running total is only right if every execution is counted exactly once, in order. So the
/// walk is refused whole, rather than summed around a gap, when an execution has no id or no
/// parseable size, or belongs to a different order: the last would mean the venue ignored the
/// `orderId` filter, and summing another order's fills would report a quantity this order never
/// filled. An execution repeated across pages is counted once.
pub(crate) fn order_running_totals<T: BinanceExecutionFields>(
    order_id: i64,
    executions: &[T],
) -> Option<Vec<(i64, Decimal)>> {
    let mut sized = executions
        .iter()
        .map(|execution| {
            if execution.order_id() != Some(order_id) {
                return None;
            }
            let qty = Decimal::from_str(execution.qty()?).ok()?;
            Some((execution.id()?, qty))
        })
        .collect::<Option<Vec<_>>>()?;

    sized.sort_unstable_by_key(|&(id, _)| id);
    sized.dedup_by_key(|&mut (id, _)| id);

    let mut cumulative = Decimal::ZERO;
    Some(
        sized
            .into_iter()
            .map(|(id, qty)| {
                cumulative += qty;
                (id, cumulative)
            })
            .collect(),
    )
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

/// Whether a Binance error message means the credentials were refused, on any endpoint.
///
/// By code: `-1002` ("You are not authorized to execute this request"), `-2015` ("Invalid
/// API-key, IP, or permissions for action"), `-1022` ("Signature for this request is not valid")
/// or `-2014` ("API-key format invalid"). By wording, for messages that arrive without a code.
fn is_auth_failure(msg: &str) -> bool {
    contains_error_code(msg, "-1002")
        || contains_error_code(msg, "-2015")
        || contains_error_code(msg, "-1022")
        || contains_error_code(msg, "-2014")
        || contains_ignore_case(msg, "invalid api-key")
        || contains_ignore_case(msg, "invalid signature")
        || contains_ignore_case(msg, "signature for this request is not valid")
}

/// Whether a Binance error message carries a code meaning the venue itself failed the request:
/// `-1000` (unknown error), `-1001` (internal error), `-1006` (unexpected response from the
/// message bus), `-1007` (backend timeout) or `-1008` (server overloaded). Binance says to retry
/// these, and for `-1006`/`-1007` states that the execution status is unknown, so an order that
/// fails this way may still have executed.
fn has_venue_failure_code(msg: &str) -> bool {
    ["-1000", "-1001", "-1006", "-1007", "-1008"]
        .iter()
        .any(|code| contains_error_code(msg, code))
}

/// Whether a Binance error message carries a throttling code, on any endpoint: `-1003` (too many
/// requests) or `-1015` (too many orders). Both are transient.
fn has_rate_limit_error_code(msg: &str) -> bool {
    contains_error_code(msg, "-1003") || contains_error_code(msg, "-1015")
}

/// Parse Binance error strings to rustrade ApiError, in an order's context.
///
/// Order and cancel paths call it through [`parse_binance_order_rejection`], which first catches
/// the venue-failure codes that leave an order's status unknown.
///
/// depends on binance-sdk's internal error formatting (not a public API contract).
/// If the SDK changes its error message format, these string matches may silently stop working.
/// Matches numeric error codes first (stable), then falls back to message text heuristics.
pub(crate) fn parse_binance_api_error(
    error_msg: String,
    instrument: &InstrumentNameExchange,
) -> ApiError<AssetNameExchange, InstrumentNameExchange> {
    // Match on Binance error codes first — these are stable numeric identifiers
    if is_auth_failure(&error_msg) {
        // Auth failures must not be retried as order rejections.
        return ApiError::Unauthenticated(error_msg);
    }
    if has_rate_limit_error_code(&error_msg) {
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

/// Parse the message of a Binance rejection of an *order* request (place or cancel, REST or WS
/// API) into an [`UnindexedOrderError`].
///
/// A venue failure ([`has_venue_failure_code`]) → [`OrderError::Connectivity`]: the order may
/// or may not have reached the matching engine, and reporting a definitive rejection for one that
/// executed would leave the caller blind to a fill. Everything else is a rejection, mapped by
/// [`parse_binance_api_error`]. The venue-failure check comes first on purpose: should a message
/// ever carry both a venue-failure code and an auth or throttle marker, "status unknown" is the
/// label that cannot hide a fill.
pub(crate) fn parse_binance_order_rejection(
    msg: String,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    if has_venue_failure_code(&msg) {
        OrderError::Connectivity(ConnectivityError::Socket(msg))
    } else {
        OrderError::Rejected(parse_binance_api_error(msg, instrument))
    }
}

/// Parse the message of a Binance rejection of a *query* (a non-order request) into a
/// [`UnindexedClientError`], by Binance code.
///
/// Deliberately separate from [`parse_binance_api_error`], which reads codes in an order's
/// context: there `-2011`/`-2013` mean the order was already cancelled, and anything unknown is an
/// [`ApiError::OrderRejected`]. A query has no order to reject, so an unrecognised code is
/// [`ApiError::RequestRejected`].
///
/// - an auth failure ([`is_auth_failure`]) → [`ApiError::Unauthenticated`];
///   `-1003`/`-1015` → [`ApiError::RateLimit`].
/// - a venue failure ([`has_venue_failure_code`]) → [`ConnectivityError::Socket`]: the venue
///   failed the request, not the other way round, and Binance says to retry.
/// - `-1021` (timestamp outside `recvWindow`) → [`ConnectivityError::Socket`]: the request took
///   too long to arrive, or the local clock drifted. The SDK stamps each attempt afresh, so a
///   retry can succeed; a clock that stays wrong keeps failing, so callers should bound retries.
/// - `-1121` → [`ApiError::InstrumentInvalid`] when the request named one instrument; otherwise
///   there is no instrument to attach, so it falls through.
/// - anything else → [`ApiError::RequestRejected`].
fn parse_binance_query_rejection(
    msg: String,
    instrument: Option<&InstrumentNameExchange>,
) -> UnindexedClientError {
    let api = if is_auth_failure(&msg) {
        ApiError::Unauthenticated(msg)
    } else if has_rate_limit_error_code(&msg) {
        ApiError::RateLimit
    } else if has_venue_failure_code(&msg) || contains_error_code(&msg, "-1021") {
        return UnindexedClientError::Connectivity(ConnectivityError::Socket(msg));
    } else if let Some(instrument) = instrument.filter(|_| contains_error_code(&msg, "-1121")) {
        ApiError::InstrumentInvalid(instrument.clone(), msg)
    } else {
        ApiError::RequestRejected(msg)
    };
    UnindexedClientError::Api(api)
}

/// Classify an `anyhow::Error` from a REST *query* (a non-order request, sent through
/// [`rest_call_with_retry`]) into an [`UnindexedClientError`], so that
/// [`is_transient`](crate::error::ClientError::is_transient) is true only for failures a retry
/// can cure.
///
/// Pass `instrument` when the request named a single instrument, so a `-1121` invalid-symbol
/// rejection can say which.
///
/// - The SDK's [`ParamBuildError`] (a request builder missing a required field) is a bug in this
///   client → [`UnindexedClientError::Internal`].
/// - A [`ConnectorError`] is classified by [`classify_connector_error`], and a venue rejection by
///   [`parse_binance_query_rejection`].
/// - Anything else failed before the request was sent: the SDK returns every network and venue
///   failure as a `ConnectorError`, so untyped text comes from its request plumbing (joining or
///   parsing the URL, signing, header values), which fails identically on retry. An auth failure
///   ([`is_auth_failure`]) → [`ApiError::Unauthenticated`], otherwise
///   [`UnindexedClientError::Internal`].
///
/// A response body that arrives but does not decode is a separate path: see
/// [`response_decode_error`].
pub(crate) fn classify_rest_query_error(
    e: &anyhow::Error,
    instrument: Option<&InstrumentNameExchange>,
) -> UnindexedClientError {
    if let Some(build) = e.downcast_ref::<ParamBuildError>() {
        return UnindexedClientError::Internal(format!("building Binance request: {build}"));
    }
    if let Some(ce) = e.downcast_ref::<ConnectorError>() {
        return match classify_connector_error(ce) {
            RequestFailure::RateLimited => UnindexedClientError::Api(ApiError::RateLimit),
            RequestFailure::Unauthenticated(msg) => {
                UnindexedClientError::Api(ApiError::Unauthenticated(msg))
            }
            RequestFailure::Transport(msg) => {
                UnindexedClientError::Connectivity(ConnectivityError::Socket(msg))
            }
            RequestFailure::Rejected(msg) => parse_binance_query_rejection(msg, instrument),
        };
    }

    let msg = format!("{e:#}");
    if is_auth_failure(&msg) {
        return UnindexedClientError::Api(ApiError::Unauthenticated(msg));
    }

    UnindexedClientError::Internal(msg)
}

/// Map a failed `RestApiResponse::data()` into [`UnindexedClientError::Internal`].
///
/// In binance-sdk `data()` only runs `serde_json::from_str` over a body already read and
/// decoded, so a failure is a mismatch between a 2xx body and the SDK's model: in practice
/// deterministic, since retrying returns the same body. A body that could not be read or decoded in the
/// first place fails earlier, inside the request, and [`classify_rest_query_error`] reports it as
/// connectivity.
pub(crate) fn response_decode_error(e: ConnectorError) -> UnindexedClientError {
    UnindexedClientError::Internal(format!("decoding Binance response: {e}"))
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
// Request-error classification (REST and WS API)
// ---------------------------------------------------------------------------

/// How a binance-sdk request failed — a REST [`ConnectorError`] or a WS-API
/// [`WebsocketError::ResponseError`] — before the caller's context (an order or a query) decides
/// which rustrade error that is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestFailure {
    /// HTTP 429 or 418 (throttled or IP-banned), or a 403 that is not an auth failure.
    RateLimited,
    /// HTTP 401, or a 403 that is an auth failure. Carries the message with the Binance code
    /// spliced in.
    Unauthenticated(String),
    /// The request never completed, the venue failed it (5xx), or a response body could not be
    /// read. Whether the venue acted on it is unknown.
    Transport(String),
    /// The venue answered and refused the request. Carries the message with the Binance code
    /// spliced in, for a code-first parser.
    Rejected(String),
}

/// Splice a Binance code back into a message whose `Display` omits it.
fn with_code(msg: &str, code: Option<i64>) -> String {
    code.map_or_else(|| msg.to_owned(), |c| format!("{c} {msg}"))
}

/// Whether an HTTP 403 is Binance's web application firewall (WAF) limit rather than an auth
/// failure. Binance documents 403 as the WAF limit being violated, and a CDN block answers 403
/// too; it is an auth failure only when its code or wording says so. `msg` carries the code.
fn is_waf_block(msg: &str) -> bool {
    !is_auth_failure(msg)
}

/// Classify an HTTP 403 (REST or WS API). A WAF block was refused at the edge, so an order never
/// reached the matching engine; it is a limit to back off from, which [`is_rate_limit_error`]
/// also recognises so [`rest_call_with_retry`] backs off in place.
fn classify_forbidden(msg: String) -> RequestFailure {
    if is_waf_block(&msg) {
        // `RateLimited` carries no message, so log the body it replaces. A CDN's 403 body is a
        // full HTML page: log only its start.
        const LOG_EXCERPT_CHARS: usize = 256;
        let excerpt = msg
            .char_indices()
            .nth(LOG_EXCERPT_CHARS)
            .map_or(msg.as_str(), |(end, _)| &msg[..end]);
        warn!(body = %excerpt, "Binance answered 403 (WAF limit); reporting RateLimit");
        RequestFailure::RateLimited
    } else {
        RequestFailure::Unauthenticated(msg)
    }
}

/// Classify a binance-sdk WS-API [`WebsocketError::ResponseError`]: the venue answered a request
/// with a status of 400 or more. `msg` is the error's `Display`, which already carries `code`.
///
/// A negative `code` is Binance's own error code, for a code-first parser. When the response had
/// no error object, the SDK falls back to the HTTP status as `code` (and `"Unknown error"` as
/// the message), so a non-negative `code` is read as a status, as the REST path reads one:
/// 429/418 → rate-limited, 401 → unauthenticated, 403 → [`classify_forbidden`], 5xx →
/// transport (the venue failed it; the status of any order is unknown).
fn classify_ws_response_error(code: i64, msg: String) -> RequestFailure {
    match code {
        429 | 418 => RequestFailure::RateLimited,
        401 => RequestFailure::Unauthenticated(msg),
        403 => classify_forbidden(msg),
        500..=599 => RequestFailure::Transport(msg),
        _ => RequestFailure::Rejected(msg),
    }
}

/// Classify a binance-sdk REST [`ConnectorError`], shared by [`classify_rest_order_error`] and
/// [`classify_rest_query_error`].
///
/// `ConnectorError`'s `Display` **omits** the Binance numeric code (it lives in a separate `code`
/// field), so the code is spliced back into the carried message wherever the caller parses it.
fn classify_connector_error(ce: &ConnectorError) -> RequestFailure {
    match ce {
        ConnectorError::TooManyRequestsError { .. } | ConnectorError::RateLimitBanError { .. } => {
            RequestFailure::RateLimited
        }
        // Splice the code in so callers can tell auth-failure subtypes apart (e.g. -2014
        // invalid key vs -2015 IP/permission).
        ConnectorError::UnauthorizedError { msg, code } => {
            RequestFailure::Unauthenticated(with_code(msg, *code))
        }
        ConnectorError::ForbiddenError { msg, code } => classify_forbidden(with_code(msg, *code)),
        ConnectorError::ServerError { msg, .. } | ConnectorError::NetworkError(msg) => {
            RequestFailure::Transport(msg.clone())
        }
        ConnectorError::BadRequestError { msg, code }
        | ConnectorError::NotFoundError { msg, code }
        | ConnectorError::ConnectorClientError { msg, code } => {
            // A codeless `ConnectorClientError` from the SDK's `http_request` is a transport or
            // response-decode failure, not a venue decision (genuine Binance rejections carry a
            // numeric code). These prefixes are the SDK's codeless transport/decode sites: the
            // request never completed, or a 2xx body could not be read/decompressed/decoded.
            // Route them to Transport — the request's venue status is unknown — rather than
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
                RequestFailure::Transport(msg.clone())
            } else {
                RequestFailure::Rejected(with_code(msg, *code))
            }
        }
    }
}

/// Classify an `anyhow::Error` from a REST order/cancel call into an [`OrderError`].
///
/// REST errors differ from the WS-API path: binance-sdk surfaces them as
/// [`ConnectorError`], whose `Display` **omits** the Binance numeric code (it lives in a
/// separate `code` field). So the WS classifier [`classify_ws_order_error`] (which downcasts
/// to `WebsocketError`) does not apply here — we downcast to `ConnectorError` instead, classify
/// it with [`classify_connector_error`], and map a venue rejection with
/// [`parse_binance_order_rejection`].
///
/// - 401 → [`ApiError::Unauthenticated`]; 429/418 → [`ApiError::RateLimit`]; 403 →
///   `Unauthenticated` when it is an auth failure, else `RateLimit` (Binance's WAF limit).
/// - 400/404 / other client errors that carry a Binance code → mapped by code/text; a venue
///   failure code → [`OrderError::Connectivity`].
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

    order_error_from(classify_connector_error(ce), instrument)
}

/// Classify an `anyhow::Error` from a WS-API order/cancel request.
///
/// binance-sdk returns both transport failures and venue responses with status >= 400 through
/// the same `Err`. Returns `None` unless it is a [`WebsocketError::ResponseError`]: the venue
/// answered, so the session is healthy and the caller keeps it. Any other error is a transport
/// failure for the caller to handle. `downcast_ref` searches the whole error chain, so context
/// layers do not hide the `ResponseError`. Re-verify on an SDK upgrade: if the SDK changes its
/// error wrapping, `downcast_ref` returns `None` and every rejection would be treated as a
/// transport failure.
pub(crate) fn classify_ws_order_error(
    e: &anyhow::Error,
    instrument: &InstrumentNameExchange,
) -> Option<UnindexedOrderError> {
    match e.downcast_ref::<WebsocketError>()? {
        // The typed error's own `Display`, not `e`'s: a context layer would hide the code.
        response @ WebsocketError::ResponseError { code, .. } => Some(order_error_from(
            classify_ws_response_error(*code, response.to_string()),
            instrument,
        )),
        _ => None,
    }
}

/// Map a [`RequestFailure`] of an order or cancel request to an [`OrderError`].
fn order_error_from(
    failure: RequestFailure,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    match failure {
        RequestFailure::RateLimited => OrderError::Rejected(ApiError::RateLimit),
        RequestFailure::Unauthenticated(msg) => {
            OrderError::Rejected(ApiError::Unauthenticated(msg))
        }
        RequestFailure::Transport(msg) => OrderError::Connectivity(ConnectivityError::Socket(msg)),
        RequestFailure::Rejected(msg) => parse_binance_order_rejection(msg, instrument),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `myTrades` execution reduced to the three fields recovery reads.
    #[derive(Debug, Clone, Copy)]
    struct Execution {
        id: Option<i64>,
        order_id: Option<i64>,
        qty: Option<&'static str>,
    }

    impl BinanceExecutionFields for Execution {
        fn id(&self) -> Option<i64> {
            self.id
        }
        fn order_id(&self) -> Option<i64> {
            self.order_id
        }
        fn qty(&self) -> Option<&str> {
            self.qty
        }
    }

    fn execution(id: i64, order_id: i64, qty: &'static str) -> Execution {
        Execution {
            id: Some(id),
            order_id: Some(order_id),
            qty: Some(qty),
        }
    }

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    #[test]
    fn running_totals_accumulate_in_trade_id_order_counting_a_repeat_once() {
        // Out of order, and trade 12 twice, as an overlapping page would serve it.
        let executions = [
            execution(12, 7, "0.5"),
            execution(10, 7, "1"),
            execution(12, 7, "0.5"),
            execution(15, 7, "2"),
        ];

        assert_eq!(
            order_running_totals(7, &executions),
            Some(vec![
                (10, Decimal::ONE),
                (12, Decimal::new(15, 1)),
                (15, Decimal::new(35, 1)),
            ])
        );
    }

    #[test]
    fn running_totals_refuse_a_walk_holding_another_orders_execution() {
        // The venue ignoring `orderId` would serve the symbol's other orders; summing them would
        // report a quantity this order never filled.
        let executions = [execution(10, 7, "1"), execution(11, 8, "5")];

        assert_eq!(order_running_totals(7, &executions), None);
    }

    #[test]
    fn running_totals_refuse_a_walk_with_an_unreadable_execution() {
        let unsized_execution = Execution {
            qty: None,
            ..execution(11, 7, "0")
        };
        let unparseable = execution(11, 7, "abc");
        let unidentified = Execution {
            id: None,
            ..execution(11, 7, "1")
        };

        for broken in [unsized_execution, unparseable, unidentified] {
            assert_eq!(
                order_running_totals(7, &[execution(10, 7, "1"), broken]),
                None,
                "{broken:?}",
            );
        }
    }

    #[tokio::test]
    async fn a_recovered_fill_counts_what_its_order_filled_before_the_window() {
        // Only trade 12 fell inside the recovery window; trade 10 filled before the disconnect.
        let recovered = [execution(12, 7, "2")];
        let order_walk = vec![execution(10, 7, "1"), execution(12, 7, "2")];

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| {
                assert_eq!(order_id, 7);
                let walk = order_walk.clone();
                async move { Ok(walk) }
            },
        )
        .await;

        assert_eq!(totals.get(&12), Some(&Decimal::new(3, 0)));
    }

    #[tokio::test]
    async fn an_order_whose_walk_fails_is_left_out_and_the_others_are_not() {
        let recovered = [execution(20, 1, "1"), execution(21, 2, "1")];

        let totals = recovered_order_totals(
            ExchangeId::BinanceMargin,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| async move {
                match order_id {
                    1 => Err(UnindexedClientError::Connectivity(
                        ConnectivityError::Socket("connection reset".into()),
                    )),
                    _ => Ok(vec![execution(21, 2, "1")]),
                }
            },
        )
        .await;

        assert_eq!(totals.get(&20), None);
        assert_eq!(totals.get(&21), Some(&Decimal::ONE));
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_stop_at_the_deadline_keeping_the_walks_that_answered() {
        // Order 1's walk never answers; order 2's does. Recovery must return what it has at the
        // deadline, so the fills still go out inside the overall recovery timeout.
        let recovered = [execution(20, 1, "1"), execution(21, 2, "1")];
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(5);

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            deadline,
            |order_id| async move {
                if order_id == 1 {
                    std::future::pending::<()>().await;
                }
                Ok(vec![execution(21, 2, "1")])
            },
        )
        .await;

        assert_eq!(totals.get(&20), None);
        assert_eq!(totals.get(&21), Some(&Decimal::ONE));
        assert_eq!(tokio::time::Instant::now(), deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_run_side_by_side_up_to_the_in_flight_bound() {
        // Each walk takes 4 s against a 5 s budget: one at a time, only the first would finish.
        // Side by side, the first ORDER_EXECUTIONS_IN_FLIGHT finish together at 4 s, and the one
        // over the bound starts only then, so the deadline cuts it.
        let recovered: Vec<Execution> = (1_i64..)
            .take(ORDER_EXECUTIONS_IN_FLIGHT + 1)
            .map(|order_id| execution(100 + order_id, order_id, "1"))
            .collect();
        let over_bound = recovered.last().and_then(|t| t.id);

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
                Ok(vec![execution(100 + order_id, order_id, "1")])
            },
        )
        .await;

        assert_eq!(totals.len(), ORDER_EXECUTIONS_IN_FLIGHT, "{totals:?}");
        assert!(
            over_bound.is_some_and(|id| !totals.contains_key(&id)),
            "{totals:?}"
        );
    }

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
        // A 401, or a 403 that is an auth failure, is a definitive auth rejection.
        for ce in [
            ConnectorError::UnauthorizedError {
                msg: "bad key".to_string(),
                code: Some(-2014),
            },
            ConnectorError::ForbiddenError {
                msg: "Invalid API-key, IP, or permissions for action.".to_string(),
                code: Some(-2015),
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

    // --- REST query-error classification ---

    fn classify_query(ce: ConnectorError) -> UnindexedClientError {
        classify_rest_query_error(
            &anyhow::Error::new(ce),
            Some(&InstrumentNameExchange::new("BTCUSDT")),
        )
    }

    fn bad_request(code: i64, msg: &str) -> ConnectorError {
        ConnectorError::BadRequestError {
            msg: msg.to_string(),
            code: Some(code),
        }
    }

    #[test]
    fn a_query_the_venue_refuses_is_a_request_rejection_carrying_its_code() {
        let err = classify_query(bad_request(
            -1127,
            "More than 24 hours between startTime and endTime.",
        ));
        let UnindexedClientError::Api(ApiError::RequestRejected(msg)) = &err else {
            panic!("expected RequestRejected, got {err:?}");
        };
        assert!(msg.contains("-1127"), "code missing from {msg:?}");
        assert!(!err.is_transient());
    }

    #[test]
    fn a_query_naming_an_invalid_symbol_is_an_invalid_instrument() {
        let err = classify_query(bad_request(-1121, "Invalid symbol."));
        assert!(
            matches!(
                &err,
                UnindexedClientError::Api(ApiError::InstrumentInvalid(instrument, _))
                    if instrument.name().as_str() == "BTCUSDT"
            ),
            "got {err:?}"
        );

        // With no single instrument in the request there is none to name.
        let err = classify_rest_query_error(
            &anyhow::Error::new(bad_request(-1121, "Invalid symbol.")),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn order_codes_do_not_leak_into_query_classification() {
        // On an order path -2011/-2013 mean "already cancelled"; a query has no order to cancel.
        for code in [-2011, -2013] {
            let err = classify_query(bad_request(code, "Unknown order sent."));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn query_rejections_with_auth_or_throttle_codes_keep_their_meaning() {
        for (code, msg) in [
            (-1002, "You are not authorized to execute this request."),
            (-2015, "Invalid API-key, IP, or permissions for action."),
            (-1022, "Signature for this request is not valid."),
            (-2014, "API-key format invalid."),
        ] {
            let err = classify_query(bad_request(code, msg));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
                "{code}: got {err:?}"
            );
        }
        for code in [-1003, -1015] {
            let err = classify_query(bad_request(code, "Too much request weight used."));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::RateLimit)),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn query_rejections_a_retry_can_cure_stay_transient() {
        for code in [-1000, -1001, -1006, -1007, -1008, -1021] {
            let err = classify_query(bad_request(code, "Internal error; please try again."));
            assert!(
                matches!(err, UnindexedClientError::Connectivity(_)),
                "{code}: got {err:?}"
            );
            assert!(err.is_transient());
        }
    }

    #[test]
    fn query_transport_server_and_throttle_failures_classify_like_orders() {
        for ce in [
            ConnectorError::ConnectorClientError {
                msg: "HTTP request failed: connection reset".to_string(),
                code: None,
            },
            ConnectorError::ServerError {
                msg: "Server error: 503".to_string(),
                status_code: Some(503),
            },
            ConnectorError::NetworkError("connection reset".to_string()),
        ] {
            let err = classify_query(ce);
            assert!(
                matches!(err, UnindexedClientError::Connectivity(_)),
                "got {err:?}"
            );
            assert!(err.is_transient());
        }

        let err = classify_query(ConnectorError::TooManyRequestsError {
            msg: "Too many requests.".to_string(),
            code: Some(-1003),
        });
        assert!(matches!(
            err,
            UnindexedClientError::Api(ApiError::RateLimit)
        ));

        let err = classify_query(ConnectorError::UnauthorizedError {
            msg: "API-key format invalid.".to_string(),
            code: Some(-2014),
        });
        let UnindexedClientError::Api(ApiError::Unauthenticated(msg)) = &err else {
            panic!("expected Unauthenticated, got {err:?}");
        };
        assert!(msg.contains("-2014"), "code missing from {msg:?}");
    }

    #[test]
    fn a_request_the_client_failed_to_build_is_internal() {
        let err = classify_rest_query_error(
            &anyhow::Error::new(ParamBuildError::UninitializedField("symbol")),
            None,
        );
        assert!(
            matches!(&err, UnindexedClientError::Internal(msg) if msg.contains("symbol")),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn a_response_body_that_does_not_decode_is_internal() {
        // What binance-sdk's `data()` returns when the body does not fit its model: the
        // `serde_json` error as a codeless `ConnectorClientError`.
        let err = response_decode_error(ConnectorError::ConnectorClientError {
            msg: "invalid type: map, expected a sequence at line 1 column 0".to_string(),
            code: None,
        });
        assert!(
            matches!(&err, UnindexedClientError::Internal(msg) if msg.contains("invalid type")),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn untyped_query_errors_are_classified_by_their_text() {
        // Errors from the SDK's request plumbing arrive as untyped text, not a ConnectorError.
        for msg in [
            "Error -1002: unauthorized",
            // Code only, no auth wording: isolates the numeric-code branch.
            "Error -2015: permission denied for action",
            "invalid signature provided",
            "The signature for this request is not valid.",
            "Invalid API-key format",
        ] {
            let err = classify_rest_query_error(&anyhow::anyhow!("{msg}"), None);
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
                "{msg:?}: got {err:?}"
            );
        }

        // Anything else failed before the request left, and fails the same way on retry.
        let err = classify_rest_query_error(
            &anyhow::anyhow!("relative URL without a base").context("Failed to join base URL"),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Internal(_)),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn a_forbidden_query_is_an_auth_failure_only_when_it_says_so() {
        // Binance documents 403 as its WAF limit: back off and retry.
        let err = classify_query(ConnectorError::ForbiddenError {
            msg: "<html>403 Forbidden</html>".to_string(),
            code: None,
        });
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RateLimit)),
            "got {err:?}"
        );

        let err = classify_query(ConnectorError::ForbiddenError {
            msg: "Invalid API-key, IP, or permissions for action.".to_string(),
            code: Some(-2015),
        });
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn query_classification_reads_the_sdk_error_through_added_context() {
        let err = classify_rest_query_error(
            &anyhow::Error::new(bad_request(-1127, "More than 24 hours.")).context("outer"),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
            "got {err:?}"
        );
    }

    // --- Order rejection codes ---

    #[test]
    fn an_order_forbidden_by_the_waf_is_a_rate_limit() {
        // Refused at the edge: the order never reached the matching engine.
        assert!(matches!(
            classify_err(ConnectorError::ForbiddenError {
                msg: "<html>403 Forbidden</html>".to_string(),
                code: None,
            }),
            OrderError::Rejected(ApiError::RateLimit)
        ));
    }

    #[test]
    fn an_order_the_venue_failed_has_an_unknown_status() {
        // Binance: the order may have executed. A definitive rejection would hide a fill.
        for code in [-1000, -1001, -1006, -1007, -1008] {
            let err = classify_err(ConnectorError::BadRequestError {
                msg: "Timeout waiting for response from backend server.".to_string(),
                code: Some(code),
            });
            assert!(
                matches!(err, OrderError::Connectivity(_)),
                "{code}: got {err:?}"
            );
            assert!(err.is_transient());
        }

        // The WS-API reports the same codes in its `ResponseError` text.
        let err = parse_binance_order_rejection(
            "Server\u{2010}side response error (code -1007): Timeout waiting for response from \
             backend server. Send status unknown; execution status unknown."
                .to_string(),
            &InstrumentNameExchange::new("BTCUSDT"),
        );
        assert!(matches!(err, OrderError::Connectivity(_)), "got {err:?}");
    }

    #[test]
    fn an_order_with_a_bad_signature_or_key_format_is_unauthenticated() {
        for (code, msg) in [
            (-1022, "Signature for this request is not valid."),
            (-2014, "API-key format invalid."),
        ] {
            let err = classify_err(ConnectorError::BadRequestError {
                msg: msg.to_string(),
                code: Some(code),
            });
            assert!(
                matches!(err, OrderError::Rejected(ApiError::Unauthenticated(_))),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn a_definitive_order_rejection_is_still_a_rejection() {
        let err = classify_err(ConnectorError::BadRequestError {
            msg: "Account has insufficient balance for requested action.".to_string(),
            code: Some(-2010),
        });
        assert!(
            matches!(err, OrderError::Rejected(ApiError::BalanceInsufficient(..))),
            "got {err:?}"
        );
    }

    #[test]
    fn order_rejections_keep_their_order_meaning() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        for (msg, expected) in [
            // Cancel races stay "already cancelled".
            (
                "-2013 Order does not exist.",
                ApiError::OrderAlreadyCancelled,
            ),
            ("-2011 Unknown order sent.", ApiError::OrderAlreadyCancelled),
            // A stale timestamp on an order is a definitive refusal: it did not execute.
            (
                "-1021 Timestamp for this request is outside of the recvWindow.",
                ApiError::OrderRejected(
                    "-1021 Timestamp for this request is outside of the recvWindow.".to_string(),
                ),
            ),
            // Auth by wording alone, with no code.
            (
                "Invalid API-key, IP, or permissions for action.",
                ApiError::Unauthenticated(
                    "Invalid API-key, IP, or permissions for action.".to_string(),
                ),
            ),
        ] {
            assert_eq!(
                parse_binance_order_rejection(msg.to_string(), &instrument),
                OrderError::Rejected(expected),
                "{msg:?}"
            );
        }
    }

    fn classify_ws(code: i64, message: &str) -> Option<UnindexedOrderError> {
        classify_ws_order_error(
            &anyhow::Error::new(WebsocketError::ResponseError {
                code,
                message: message.to_string(),
            }),
            &InstrumentNameExchange::new("BTCUSDT"),
        )
    }

    #[test]
    fn a_ws_response_without_an_error_body_is_read_by_its_status() {
        // With no error object the SDK reports the HTTP status as `code`, "Unknown error" as text.
        let err = classify_ws(503, "Unknown error");
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "a 5xx leaves the order's status unknown: {err:?}"
        );
        for code in [429, 418, 403] {
            let err = classify_ws(code, "Unknown error");
            assert!(
                matches!(err, Some(OrderError::Rejected(ApiError::RateLimit))),
                "{code}: got {err:?}"
            );
        }
        let err = classify_ws(401, "Unknown error");
        assert!(
            matches!(
                err,
                Some(OrderError::Rejected(ApiError::Unauthenticated(_)))
            ),
            "got {err:?}"
        );
        let err = classify_ws(409, "Unknown error");
        assert!(
            matches!(err, Some(OrderError::Rejected(ApiError::OrderRejected(_)))),
            "got {err:?}"
        );
    }

    #[test]
    fn a_ws_response_with_a_binance_code_is_read_by_its_code() {
        let err = classify_ws(-1007, "Timeout waiting for response from backend server.");
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "got {err:?}"
        );

        // A context layer must not hide the code.
        let wrapped = anyhow::Error::new(WebsocketError::ResponseError {
            code: -1007,
            message: "Timeout waiting for response from backend server.".to_string(),
        })
        .context("outer");
        let err = classify_ws_order_error(&wrapped, &InstrumentNameExchange::new("BTCUSDT"));
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "got {err:?}"
        );
        let err = classify_ws(-1003, "Too much request weight used.");
        assert!(
            matches!(err, Some(OrderError::Rejected(ApiError::RateLimit))),
            "got {err:?}"
        );
    }

    #[test]
    fn a_waf_403_is_backed_off_as_a_rate_limit_but_an_auth_403_is_not() {
        let waf = anyhow::Error::new(ConnectorError::ForbiddenError {
            msg: "<html>403 Forbidden</html>".to_string(),
            code: None,
        });
        assert!(is_rate_limit_error(&waf));

        let auth = anyhow::Error::new(ConnectorError::ForbiddenError {
            msg: "Invalid API-key, IP, or permissions for action.".to_string(),
            code: Some(-2015),
        });
        assert!(!is_rate_limit_error(&auth));
    }
}
