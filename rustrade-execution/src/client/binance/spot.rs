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
//   chain, so computed delays are used), blocks further REST calls until cooldown expires,
//   and does not open a WS-API session during it. Separately, pauses REST queries until the
//   next minute once responses report >= 90% of the request-weight limit used
// - Reconnection: account_stream auto-reconnects on WS disconnect/error with
//   exponential backoff (1s → 30s, max 10 attempts)
// - Heartbeat monitoring: tracks WS activity via AtomicBool flag; forces reconnect
//   if no activity (messages, ping, pong) for 30 seconds
// - Fill recovery: on reconnect, fetches missed trades via REST since disconnect
//   timestamp, sends through dedup cache to avoid duplicates
//
// Known limitations:
// - balanceUpdate events (deposits/withdrawals) are silently ignored. The crypto
//   repo wrapper should call fetch_balances or account_snapshot periodically to
//   reconcile balances after external transfers.
// - A TRADE report whose order status (X) is neither PARTIALLY_FILLED nor FILLED emits
//   the execution but no order snapshot, so a fill arriving after its order's terminal
//   report cannot resurrect a retired order as a resting one. That order's filled
//   quantity is settled instead by the terminal report's own z, or by fetch_open_orders.

use super::shared::{
    AbortOnDropStream, BINANCE_MAX_TRADES, BinanceOrderType, BinanceTimeInForce,
    CONNECT_TIMEOUT_SECS, ExponentialBackoff, FILL_RECOVERY_TIMEOUT_SECS, HEARTBEAT_TIMEOUT_SECS,
    MyTradesFrom, ORDER_EXECUTIONS_BUDGET, OpenOrderListing, RateLimitTracker, RequestKind,
    SIGNAL_RECOVERY_LOOKBACK_MS, SharedDedupCache, UnrecoveredFills, UserDataFrame, WeightPool,
    classify_order_kind_tif, classify_rest_query_error, classify_ws_order_error,
    convert_ended_order, convert_execution_report, convert_open_order_listing,
    convert_open_order_owned_symbol, dedup_key_from_event, drop_after, gap_failed, gap_time,
    is_duplicate, is_handshake_rate_limit, is_unknown_order, log_unrecognised_frame,
    new_dedup_cache, parse_user_data_frame, placed_order_state, recovered_order_totals,
    response_decode_error, rest_call_with_retry, unix_ms,
};
use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, AssetBalanceUpdate, Balance, BalanceUpdate},
    client::{
        ExecutionClient, OrderStatusClient,
        order_recovery::{
            KnownLiveOrders, OpenListing, OrderLookup, SharedKnownLiveOrders, UncheckedOrders,
            fetch_ended_by_key, recover_ended_orders,
        },
    },
    emit_stream_terminated,
    error::{
        ApiError, ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce, TrailingOffsetType, UnindexedInactiveOrder,
        UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Open, OrderState, UnindexedOrderState},
    },
    parse_env_bool,
    position::PositionReport,
    trade::{AssetFees, Trade, TradeId},
};
use binance_sdk::{
    common::{
        config::{ConfigurationRestApi, ConfigurationWebsocketApi},
        models::WebsocketEvent,
    },
    spot::{
        SpotRestApi, SpotWsApi,
        rest_api::{
            GetAccountParams, GetOpenOrdersParams, GetOrderParams, MyTradesParams, RestApi,
        },
        websocket_api::{
            OrderCancelParams, OrderPlaceParams, OrderPlaceSideEnum, OrderPlaceTimeInForceEnum,
            OrderPlaceTypeEnum, UserDataStreamSubscribeSignatureParams, WebsocketApi,
            WebsocketApiHandle,
        },
    },
};
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashSet;
use futures::stream::BoxStream;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use serde::Deserialize;
use smol_str::format_smolstr;
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};

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
    #[serde(default = "default_testnet")]
    pub testnet: bool,
}

/// Serde default for [`BinanceSpotConfig::testnet`]: an absent `testnet` field deserializes to the
/// **safe** testnet environment (`true`).
///
/// `#[serde(default = "…")]` requires a named function (it cannot take a literal), so this exists
/// purely to supply that default to the derive.
fn default_testnet() -> bool {
    true
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
    /// Create a new config using the **safe** testnet endpoints (alias for
    /// [`testnet`](Self::testnet)).
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self::testnet(api_key, secret_key)
    }

    /// Create a config targeting Binance Spot's **testnet** endpoints (simulated funds).
    pub fn testnet(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
            testnet: true,
        }
    }

    /// Create a config targeting Binance Spot's **production** endpoints.
    ///
    /// ⚠️ Production trades execute against the live account with **real funds**. Prefer
    /// [`testnet`](Self::testnet) unless you explicitly intend live execution.
    pub fn production(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
            testnet: false,
        }
    }

    /// Build a config from environment variables.
    ///
    /// Reads:
    /// - `BINANCE_API_KEY` (required) — API key.
    /// - `BINANCE_SECRET_KEY` (required) — API secret.
    /// - `BINANCE_TESTNET` (optional) — `"true"`/`"false"` (case-insensitive). **Absent ⇒ the safe
    ///   testnet environment.** Set `BINANCE_TESTNET=false` to target production (real funds).
    ///
    /// # Errors
    ///
    /// Returns [`BinanceSpotConfigError`] (never panics):
    /// - a required credential var is unset
    ///   ([`MissingApiKey`](BinanceSpotConfigError::MissingApiKey) /
    ///   [`MissingSecretKey`](BinanceSpotConfigError::MissingSecretKey)) or holds non-UTF-8
    ///   ([`InvalidApiKey`](BinanceSpotConfigError::InvalidApiKey) /
    ///   [`InvalidSecretKey`](BinanceSpotConfigError::InvalidSecretKey));
    /// - `BINANCE_TESTNET` is neither `true` nor `false`, or holds non-UTF-8
    ///   ([`InvalidTestnet`](BinanceSpotConfigError::InvalidTestnet)).
    pub fn from_env() -> Result<Self, BinanceSpotConfigError> {
        let api_key = match std::env::var("BINANCE_API_KEY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => {
                return Err(BinanceSpotConfigError::MissingApiKey);
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(BinanceSpotConfigError::InvalidApiKey);
            }
        };
        let secret_key = match std::env::var("BINANCE_SECRET_KEY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => {
                return Err(BinanceSpotConfigError::MissingSecretKey);
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(BinanceSpotConfigError::InvalidSecretKey);
            }
        };
        let testnet = match std::env::var("BINANCE_TESTNET") {
            Ok(value) => {
                parse_env_bool(&value).ok_or(BinanceSpotConfigError::InvalidTestnet(value))?
            }
            Err(std::env::VarError::NotPresent) => true,
            // The toggle value is not secret, so echo it (lossily) like the parse-failure arm above —
            // an actionable "got X" beats a hardcoded sentinel.
            Err(std::env::VarError::NotUnicode(value)) => {
                return Err(BinanceSpotConfigError::InvalidTestnet(
                    value.to_string_lossy().into_owned(),
                ));
            }
        };

        if testnet {
            Ok(Self::testnet(api_key, secret_key))
        } else {
            Ok(Self::production(api_key, secret_key))
        }
    }

    /// Read-only access to the API key (e.g. for logging or header construction).
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum BinanceSpotConfigError {
    #[error("BINANCE_API_KEY environment variable not set")]
    MissingApiKey,

    // No payload: the raw value is secret-key material, so it must never be echoed into an error
    // message or log. The variant name already identifies which credential var is non-UTF-8.
    #[error("BINANCE_API_KEY environment variable is not valid UTF-8")]
    InvalidApiKey,

    #[error("BINANCE_SECRET_KEY environment variable not set")]
    MissingSecretKey,

    #[error("BINANCE_SECRET_KEY environment variable is not valid UTF-8")]
    InvalidSecretKey,

    #[error("BINANCE_TESTNET must be true or false, got {0}")]
    InvalidTestnet(String),
}

// ---------------------------------------------------------------------------
// BinanceSpot client
// ---------------------------------------------------------------------------

/// Why [`BinanceSpot::get_ws_api`] has no WS-API session to hand out. Either way, nothing was
/// sent.
#[derive(Debug)]
enum WsApiUnavailable {
    /// A rate-limit cooldown is active, or Binance refused the handshake with 429 or 418.
    RateLimited(String),
    /// Any other connect failure.
    Connect(String),
}

impl WsApiUnavailable {
    /// The error an order or cancel that could not be sent fails with. A rate limit is a
    /// rejection, which tells the caller the request was not sent and to back off.
    fn into_order_error(self) -> UnindexedOrderError {
        match self {
            Self::RateLimited(msg) => {
                warn!(%msg, "BinanceSpot WS-API rate-limited, request not sent");
                UnindexedOrderError::Rejected(ApiError::RateLimit)
            }
            Self::Connect(msg) => UnindexedOrderError::Connectivity(ConnectivityError::Socket(msg)),
        }
    }
}

/// BinanceSpot execution client using the official binance-sdk.
///
/// - REST API: account snapshot, balance/order/trade queries (startup/cold paths)
/// - WebSocket API: order placement, order cancellation, user data stream (hot paths)
///
/// # Rate limits
/// REST and the WebSocket API share one per-IP request-weight limit per minute. Its value comes
/// from each WebSocket API response, and is Binance's documented 6000 until the first one. Once
/// a response reports at least 90% of it used, REST queries wait for the next minute, so the
/// rest is left for orders and cancels, which never wait for it. A query, including a
/// [`fetch_open_orders`](Self::fetch_open_orders) after a reconnect, can therefore take up to
/// about a minute longer. A reconnect's fill recovery does not wait for it either, since a pause
/// could outlast the recovery's 30 s and delay its fills to a retry. After Binance answers with a
/// rate-limit error, REST calls wait out a cooldown, and an order or cancel that would have to
/// open a new WebSocket API session fails with [`ApiError::RateLimit`] instead, unsent.
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
    // The orders seen live and not yet seen end, which a reconnect asks about. Shared by every
    // clone and every account stream, since an order placed through one ends on any of them.
    known_live: SharedKnownLiveOrders,
}

impl std::fmt::Debug for BinanceSpot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceSpot")
            .field("testnet", &self.config.testnet)
            .finish_non_exhaustive()
    }
}

impl BinanceSpot {
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
    ///
    /// Does not connect during a rate-limit cooldown: a handshake then counts against the limit
    /// that set it, and repeated refusals can escalate to an IP ban (418). A handshake Binance
    /// refuses with 429 or 418 starts a cooldown, so REST calls back off too. An existing session
    /// is handed out regardless, since orders do not wait on the cooldown.
    async fn get_ws_api(&self) -> Result<WebsocketApi, WsApiUnavailable> {
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
        if self.rate_limiter.is_blocked() {
            return Err(WsApiUnavailable::RateLimited(
                "rate-limit cooldown active, not connecting the WS-API session".into(),
            ));
        }
        let ws = match tokio::time::timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            self.ws_handle.connect(),
        )
        .await
        {
            Ok(Ok(ws)) => ws,
            Ok(Err(e)) if is_handshake_rate_limit(&e) => {
                self.rate_limiter.on_rate_limited(None);
                return Err(WsApiUnavailable::RateLimited(format!("{e:#}")));
            }
            Ok(Err(e)) => return Err(WsApiUnavailable::Connect(format!("{e:#}"))),
            Err(_) => {
                return Err(WsApiUnavailable::Connect(format!(
                    "BinanceSpot WS connect timed out after {CONNECT_TIMEOUT_SECS}s"
                )));
            }
        };
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
    kind: RequestKind,
) -> Result<(InstrumentNameExchange, OpenOrderListing), UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    let response = rest_call_with_retry(&rest, &rate_limiter, kind, |rest| {
        let sym = symbol_str.clone();
        Box::pin(async move {
            let params = GetOpenOrdersParams::builder().symbol(sym).build()?;
            rest.get_open_orders(params).await
        })
    })
    .await
    .map_err(|e| classify_rest_query_error(&e, Some(&instrument)))?;

    let orders_data = response.data().await.map_err(response_decode_error)?;

    let listing = convert_open_order_listing(&orders_data, ExchangeId::BinanceSpot, &instrument);

    Ok((instrument, listing))
}

/// Fetch *all* open orders in a single no-symbol `GET /api/v3/openOrders` call. Backs the
/// [`fetch_open_orders`](BinanceSpot::fetch_open_orders) "return all" sentinel: with no symbol the
/// venue returns orders across every instrument, so each order's instrument is recovered from its
/// own `symbol` field (orders missing it are dropped).
async fn fetch_all_open_orders(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
    let response = rest_call_with_retry(&rest, &rate_limiter, RequestKind::Query, |rest| {
        Box::pin(async move {
            let params = GetOpenOrdersParams::builder().build()?;
            rest.get_open_orders(params).await
        })
    })
    .await
    .map_err(|e| classify_rest_query_error(&e, None))?;

    let orders_data = response.data().await.map_err(response_decode_error)?;

    let orders = orders_data
        .into_iter()
        .filter_map(|o| convert_open_order_owned_symbol(&o, ExchangeId::BinanceSpot))
        .collect();

    Ok(orders)
}

/// Look the order under `key` up with `GET /api/v3/order` by its client order id (weight 4).
async fn fetch_order_lookup(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    key: UnindexedOrderKey,
    kind: RequestKind,
) -> Result<OrderLookup, UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol = key.instrument.name().to_string();
    let cid = key.cid.0.to_string();
    let response = match rest_call_with_retry(&rest, &rate_limiter, kind, |rest| {
        let (symbol, cid) = (symbol.clone(), cid.clone());
        Box::pin(async move {
            let params = GetOrderParams::builder(symbol)
                .orig_client_order_id(cid)
                .build()?;
            rest.get_order(params).await
        })
    })
    .await
    {
        Ok(response) => response,
        Err(e) if is_unknown_order(&e) => {
            debug!(instrument = %key.instrument, cid = %key.cid, error = %e, "BinanceSpot does not know this order");
            return Ok(OrderLookup::Unknown);
        }
        Err(e) => return Err(classify_rest_query_error(&e, Some(&key.instrument))),
    };

    let row = response.data().await.map_err(response_decode_error)?;

    Ok(convert_ended_order(&row, ExchangeId::BinanceSpot, &key)
        .map_or(OrderLookup::NotEnded, |order| {
            OrderLookup::Ended(Box::new(order))
        }))
}

/// The client order ids `GET /api/v3/openOrders` lists on `instruments` (weight 6 each, one at a
/// time), for a reconnect's check of the orders held as live.
async fn listed_open_cids(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    instruments: Vec<InstrumentNameExchange>,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let mut listed = FnvHashSet::default();
    for instrument in instruments {
        let (_, listing) = fetch_open_orders_for_instrument(
            rest.clone(),
            rate_limiter.clone(),
            instrument,
            RequestKind::Essential,
        )
        .await?;
        listed.extend(listing.orders.into_iter().map(|order| order.key.cid));
    }
    Ok(listed)
}

/// [`recover_ended_orders`] on Binance Spot: listings by [`listed_open_cids`] and lookups by
/// [`fetch_order_lookup`], both [`RequestKind::Essential`], as fill recovery's reads are.
async fn recover_spot_ended_orders(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    unrecovered: &UnrecoveredFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) {
    recover_ended_orders(
        ExchangeId::BinanceSpot,
        known,
        unchecked,
        unrecovered,
        tx,
        OpenListing::PerInstrument,
        |instruments| listed_open_cids(rest.clone(), rate_limiter.clone(), instruments),
        |key| {
            fetch_order_lookup(
                rest.clone(),
                rate_limiter.clone(),
                key,
                RequestKind::Essential,
            )
        },
    )
    .await;
}

/// Paginate `GET /api/v3/myTrades` for a single instrument, from `from`.
///
/// Uses cursor-based pagination: the first page queries by `start_time`, or by `order_id` for a
/// single order's executions; subsequent pages use `from_id = last_id + 1` (Binance ignores
/// `start_time` when `from_id` is set), keeping `order_id` alongside it, a combination Binance
/// documents as supported. A [`MyTradesFrom::Span`] stops at the first page reaching past its end
/// and drops the executions after it. Trade IDs are monotonically increasing per symbol, so this produces a
/// gapless result.
///
/// Returns raw response items. Callers decide how to handle `Err` (propagate vs. log-skip).
async fn paginate_my_trades(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    instrument: &InstrumentNameExchange,
    from: MyTradesFrom,
    kind: RequestKind,
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
        let response = rest_call_with_retry(rest, rate_limiter, kind, |rest| {
            let sym = symbol_str.clone();
            Box::pin(async move {
                // const_assert! above guarantees BINANCE_MAX_TRADES fits in i32
                #[allow(clippy::cast_possible_truncation)]
                let builder = MyTradesParams::builder(sym).limit(BINANCE_MAX_TRADES as i32);
                let params = match (from, fid) {
                    (
                        MyTradesFrom::Time(start_time_ms)
                        | MyTradesFrom::Span {
                            start: start_time_ms,
                            ..
                        },
                        None,
                    ) => builder.start_time(start_time_ms),
                    (MyTradesFrom::Time(_) | MyTradesFrom::Span { .. }, Some(id)) => {
                        builder.from_id(id)
                    }
                    (MyTradesFrom::Order(order_id), None) => builder.order_id(order_id),
                    (MyTradesFrom::Order(order_id), Some(id)) => {
                        builder.order_id(order_id).from_id(id)
                    }
                }
                .build()?;
                rest.my_trades(params).await
            })
        })
        .await
        .map_err(|e| classify_rest_query_error(&e, Some(instrument)))?;

        let mut page = response.data().await.map_err(response_decode_error)?;

        let page_len = page.len();
        let last_id = page.last().and_then(|t| t.id);
        // A span ends once a page reaches past it.
        let past_end = match from {
            MyTradesFrom::Span { end, .. } => drop_after(&mut page, end),
            MyTradesFrom::Time(_) | MyTradesFrom::Order(_) => false,
        };
        all_pages.extend(page);

        if past_end || page_len < BINANCE_MAX_TRADES {
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

    // Binance Spot trades spot pairs only; its derivatives live on separate USD-M/COIN-M venues
    // with their own APIs, which this client does not speak.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] =
        &[InstrumentKindDiscriminant::Spot];
    type Config = BinanceSpotConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// # Panics
    ///
    /// Panics if the binance-sdk REST or WebSocket configuration builder fails
    /// (e.g. empty or malformed API key/secret).
    fn new(config: Self::Config) -> Self {
        let rest = Self::build_rest(&config);
        let ws_handle = Self::build_ws_handle(&config);
        Self {
            config: Arc::new(config),
            rest,
            ws_handle,
            ws_api: Arc::new(RwLock::new(None)),
            rate_limiter: Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            known_live: KnownLiveOrders::shared(ExchangeId::BinanceSpot),
        }
    }

    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // Fetch account info via REST (with rate-limit retry)
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move {
                    let params = GetAccountParams::builder().build()?;
                    rest.get_account(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;

        let account = response.data().await.map_err(response_decode_error)?;

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
                    RequestKind::Query,
                )
            }))
            .buffer_unordered(8)
            .map(|result| {
                let (inst, listing) = result?;
                self.remember_live(&listing.orders);
                let wrapped = listing
                    .orders
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
                Ok::<_, UnindexedClientError>(InstrumentAccountSnapshot::new(
                    inst,
                    wrapped,
                    listing.complete,
                    PositionReport::Unreported,
                    None,
                ))
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
    ///
    /// # A recovery that does not finish
    ///
    /// Recovery after a reconnect is bounded by a 30 s timeout, and an instrument's query can
    /// fail. Each instrument's gap, from the disconnect to just after the recovery began, is kept
    /// until its fills are forwarded. A gap not read is retried after 1, 2, 4, 8 and 16 minutes,
    /// whether the stream stays connected or reconnects in between. A retry reads only the gap,
    /// not fills the stream delivered live after it; the few seconds at its end that overlap live
    /// delivery are deduplicated. Each failed read is logged at `warn` with the instrument and the
    /// gap. After five failed retries the gap is given up, logged at `error`, and its fills are not
    /// delivered: read them with [`ExecutionClient::fetch_trades`].
    ///
    /// A recovered gap counts as read even when a fill in it could not be converted, which is
    /// logged, or its order's cumulative could not be looked up (see below).
    ///
    /// # A recovered fill advances the order too
    ///
    /// A fill that arrives live over the WebSocket carries the order's cumulative filled
    /// quantity in [`Trade::order_filled_quantity`] (`executionReport`'s `z`), so it advances
    /// the order by itself. A fill recovered after a disconnect comes from REST `myTrades`,
    /// which reports executions only, so recovery rebuilds the same figure by reading each
    /// recovered order's executions from its first: one extra request per order (another per
    /// further 1,000 executions), up to four orders at a time per instrument.
    ///
    /// Those lookups have their own time budget inside the recovery timeout, so they can never
    /// cost a fill. A fill whose order was not looked up in time, or whose lookup failed, goes
    /// out with `order_filled_quantity: None`, logged at `warn`. It then advances the position
    /// but not the order, which keeps whatever `filled_quantity` it held before the gap until
    /// the order's state is learned some other way.
    ///
    /// # Orders that ended while disconnected
    ///
    /// A reconnect also reports how each order the client holds as live on one of `instruments`
    /// ended, where it did, as an [`AccountEventKind::OrderSnapshot`] of its inactive state:
    /// filled, cancelled with what filled before, expired, or rejected. Like fill recovery, it
    /// covers only the instruments the stream was opened with. Binance's order history filters on when an
    /// order was created, so such an order is found by asking about it, not by time.
    ///
    /// - **Which orders.** The client holds an order as live from the response to placing it,
    ///   from a listing of open orders ([`account_snapshot`](ExecutionClient::account_snapshot),
    ///   [`fetch_open_orders`](ExecutionClient::fetch_open_orders)), and from its live reports on
    ///   any of its account streams, until it sees the order end. It holds up to 4,096 orders and
    ///   forgets the oldest past that, logged at `warn`. An order placed outside this client and
    ///   never listed or reported to it is not covered.
    /// - **Cost.** One `openOrders` request per instrument with an order held (weight 6), then one
    ///   `GET /api/v3/order` (weight 4) for each held order the listing no longer shows.
    /// - **Fills first.** An instrument is checked only once its fill gap is recovered or given up,
    ///   so an order's recovered fills arrive before how it ended. A fill that brings an order to
    ///   its full quantity ends it, and that order is not reported again.
    /// - **Keys.** Each snapshot carries [`StrategyId::unknown`], since Binance records no
    ///   strategy. The engine matches it to the order it tracks by client order id.
    /// - **Failures.** Each order's lookup is settled as it ends, and each instrument as soon as its
    ///   check ends. One whose listing or any lookup fails, or whose check is still running when a
    ///   pass reaches 30 s, is retried on the fill gaps' schedule (1, 2, 4, 8 and 16 minutes),
    ///   asking only about the orders still held, then given up, logged at `error`. Its orders are
    ///   asked about again at the next reconnect. An order Binance does not know (`-2013`) stops being held, logged
    ///   at `warn`; one it still reports live, or in a state this version cannot read, stays held.
    ///
    /// The same lookup is public as [`OrderStatusClient::fetch_ended_orders`].
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
        let known_live = self.known_live.clone();
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
            known_live,
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
            Err(unavailable) => {
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(unavailable.into_order_error()),
                });
            }
        };

        // SDK constraint — OrderCancelParams::builder takes String, not &str.
        // Allocates one String for the symbol; unavoidable without SDK changes.
        let mut params_builder =
            OrderCancelParams::builder(request.key.instrument.name().to_string());

        // Use exchange order ID if available and parseable, otherwise use client order ID
        if let Some(order_id) = request.state.id.as_ref().and_then(VenueOrderId::assigned) {
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

        let sent_ms = unix_ms();
        let result = ws.order_cancel(params).await;
        // A WS-API response reports the shared spot pool's weight limit and usage.
        if let Ok(response) = &result {
            self.rate_limiter
                .observe_ws_api(response.rate_limits.as_deref(), sent_ms);
        }
        match result {
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

                    let filled_qty = data
                        .executed_qty
                        .as_deref()
                        .and_then(|q| Decimal::from_str(q).ok())
                        .unwrap_or(Decimal::ZERO);

                    self.known_live.lock().ended(&key.cid);
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Ok(Cancelled::new(exchange_order_id, time_exchange, filled_qty)),
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
                // binance-sdk routes both transport failures and venue responses with status
                // >= 400 (ResponseError) through this outer Err path. Distinguish them so a
                // venue response (-2010, -1121, etc.) doesn't tear down a healthy WS session.
                // A venue response is mostly a rejection, but one that leaves the order's
                // status unknown (a venue-failure code, a 5xx) surfaces as Connectivity.
                // Check the venue response first (zero-alloc downcast) — order rejections are
                // the common case; rate limits during placement are rare.
                if let Some(order_err) = classify_ws_order_error(&e, &instrument) {
                    // Venue response — WS session is healthy, don't tear it down. A throttle
                    // (429, -1003, a WAF 403) backs off the shared limiter so REST calls
                    // back off too.
                    if matches!(order_err, OrderError::Rejected(ApiError::RateLimit)) {
                        self.rate_limiter.on_rate_limited(None);
                    }
                    Some(UnindexedOrderResponseCancel {
                        key,
                        state: Err(order_err),
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
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
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
            Err(unavailable) => {
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: OrderState::inactive(unavailable.into_order_error()),
                });
            }
        };

        let binance_side = match side {
            Side::Buy => OrderPlaceSideEnum::Buy,
            Side::Sell => OrderPlaceSideEnum::Sell,
        };

        let (binance_type, binance_tif) = match convert_order_kind_tif(kind, time_in_force) {
            Some(converted) => converted,
            None => {
                return Some(Order {
                    key: order_key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: OrderState::inactive(OrderError::UnsupportedOrderType(format!(
                        "Binance Spot does not yet support OrderKind::{kind:?}"
                    ))),
                });
            }
        };

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

        // Set price, stop_price, and trailing_delta in a single exhaustive match
        // so that adding a new OrderKind variant fails to compile until handled here.
        match kind {
            OrderKind::Limit => {
                params_builder = params_builder.price(price);
            }
            OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
                params_builder = params_builder.stop_price(trigger_price);
            }
            OrderKind::StopLimit { trigger_price }
            | OrderKind::TakeProfitLimit { trigger_price } => {
                params_builder = params_builder.price(price).stop_price(trigger_price);
            }
            OrderKind::TrailingStop {
                offset,
                offset_type,
            } => {
                // Convert to basis points (i32). Binance trailingDelta is in basis points.
                let basis_points: i32 = match offset_type {
                    TrailingOffsetType::BasisPoints => {
                        let Ok(bp) = i32::try_from(offset) else {
                            return Some(Order {
                                key: order_key,
                                side,
                                price,
                                quantity,
                                kind,
                                time_in_force,
                                state: OrderState::inactive(OrderError::UnsupportedOrderType(
                                    format!(
                                        "TrailingStop basis-point offset {offset} overflows i32 \
                                         (Binance trailingDelta filter typically caps at 2000)"
                                    ),
                                )),
                            });
                        };
                        bp
                    }
                    TrailingOffsetType::Percentage => {
                        let Ok(bp) = i32::try_from(offset * Decimal::from(100)) else {
                            return Some(Order {
                                key: order_key,
                                side,
                                price,
                                quantity,
                                kind,
                                time_in_force,
                                state: OrderState::inactive(OrderError::UnsupportedOrderType(
                                    format!(
                                        "TrailingStop percentage offset {offset} overflows i32 \
                                         after scaling to basis points"
                                    ),
                                )),
                            });
                        };
                        bp
                    }
                    TrailingOffsetType::Absolute => {
                        // convert_order_kind_tif already rejects Absolute; surface a clean
                        // error here too rather than panic, so a future refactor that
                        // changes that contract still fails observably.
                        return Some(Order {
                            key: order_key,
                            side,
                            price,
                            quantity,
                            kind,
                            time_in_force,
                            state: OrderState::inactive(OrderError::UnsupportedOrderType(
                                "TrailingStop with Absolute offset is not supported by Binance; \
                                 convert to basis points: (absolute / price) * 10000"
                                    .into(),
                            )),
                        });
                    }
                };
                params_builder = params_builder.trailing_delta(basis_points);
            }
            // Market and TrailingStopLimit need no price/stop_price/trailing_delta here.
            // (TrailingStopLimit is already rejected by convert_order_kind_tif above.)
            // Wildcard catches them and any future variants added before this match is updated.
            _ => {}
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                });
            }
        };

        let sent_ms = unix_ms();
        let result = ws.order_place(params).await;
        // A WS-API response reports the shared spot pool's weight limit and usage.
        if let Ok(response) = &result {
            self.rate_limiter
                .observe_ws_api(response.rate_limits.as_deref(), sent_ms);
        }
        match result {
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
                                state: OrderState::inactive(OrderError::Rejected(
                                    ApiError::OrderRejected(
                                        "open_order response missing orderId".into(),
                                    ),
                                )),
                            });
                        }
                    };

                    let filled_qty = data
                        .executed_qty
                        .as_deref()
                        .and_then(|q| Decimal::from_str(q).ok())
                        .unwrap_or(Decimal::ZERO);

                    // Read from the response's status, so an order that ended in it (an IOC or
                    // FOK order that expired) is not reported, or held, as live. An `ACK`
                    // response, the default for order types other than MARKET and LIMIT, carries
                    // no status and reads as open.
                    let state = placed_order_state(
                        ExchangeId::BinanceSpot,
                        &instrument,
                        data.status.as_deref(),
                        exchange_order_id,
                        time_exchange,
                        filled_qty,
                        quantity,
                        data.cummulative_quote_qty.as_deref(),
                    );
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
                    // serde_json deserialization failure on a successful response — not an API error
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                            e.to_string(),
                        ))),
                    })
                }
            },
            Err(e) => {
                // binance-sdk routes both transport failures and venue responses with status
                // >= 400 (ResponseError) through this outer Err path. Distinguish them so a
                // venue response (-2010, -1121, etc.) doesn't tear down a healthy WS session.
                // A venue response is mostly a rejection, but one that leaves the order's
                // status unknown (a venue-failure code, a 5xx) surfaces as Connectivity.
                // Check the venue response first (zero-alloc downcast) — order rejections are
                // the common case; rate limits during placement are rare.
                if let Some(order_err) = classify_ws_order_error(&e, &instrument) {
                    // Venue response — WS session is healthy, don't tear it down. A throttle
                    // (429, -1003, a WAF 403) backs off the shared limiter so REST calls
                    // back off too.
                    if matches!(order_err, OrderError::Rejected(ApiError::RateLimit)) {
                        self.rate_limiter.on_rate_limited(None);
                    }
                    Some(Order {
                        key: order_key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: OrderState::inactive(order_err),
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
                        state: OrderState::inactive(OrderError::Connectivity(
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
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move {
                    let params = GetAccountParams::builder().build()?;
                    rest.get_account(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;

        let account = response.data().await.map_err(response_decode_error)?;

        Ok(filter_and_convert_balances(
            account.balances.unwrap_or_default(),
            assets,
        ))
    }

    /// An empty `instruments` slice is the [`ExecutionClient`] "return all" sentinel: a single
    /// no-symbol `GET /api/v3/openOrders` call returns open orders across every instrument (each
    /// order's instrument is recovered from its own `symbol` field). A non-empty slice fetches the
    /// listed instruments concurrently, per-symbol.
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // Empty slice = "return all" sentinel: a single no-symbol query is both correct (the
        // contract requires all instruments) and far cheaper than enumerating every symbol.
        if instruments.is_empty() {
            let orders =
                fetch_all_open_orders(self.rest.clone(), self.rate_limiter.clone()).await?;
            self.remember_live(&orders);
            return Ok(orders);
        }
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
                RequestKind::Query,
            )
        }))
            .buffer_unordered(8)
            .try_fold(Vec::with_capacity(instruments.len()), |mut acc: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, (_, listing)| async move {
                acc.extend(listing.orders);
                Ok(acc)
            })
            .await
            .inspect(|orders| self.remember_live(orders))
    }

    /// **Documented deviation from the `ExecutionClient::fetch_trades` "return all" contract:**
    /// Binance's `myTrades` endpoint requires a symbol — there is no no-symbol "all trades" query
    /// (unlike open orders). An empty `instruments` slice therefore has nothing to query and
    /// returns an empty `Vec`; callers wanting all trades must enumerate instruments explicitly.
    // `.iter().cloned()` is required: Rust async closures cannot satisfy the HRTB
    // `for<'a> FnMut(&'a InstrumentNameExchange) -> impl Future + 'static` needed by
    // the iterator machinery, even when the clone is moved inside the closure body.
    #[allow(clippy::redundant_iter_cloned)]
    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
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
                let pages = paginate_my_trades(
                    &rest,
                    &rate_limiter,
                    &inst,
                    MyTradesFrom::Time(start_time_ms),
                    RequestKind::Query,
                )
                .await?;
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

/// Looks each order up with `GET /api/v3/order` by symbol and client order id, weight 4 each, up
/// to eight at a time. Binance's `FILLED` is reported with its average price, `CANCELED` with
/// what filled before it, `EXPIRED` and `EXPIRED_IN_MATCH` (self-trade prevention) as expired, and
/// `REJECTED` as [`OpenFailed`](crate::order::state::InactiveOrderState::OpenFailed). An order
/// Binance does not know under the key's symbol (`-2013`, or `-1121` for a symbol that does not
/// exist) is omitted. A row whose status this version does not know is omitted too, with a
/// warning, as is a `PENDING_CANCEL` one, which Binance does not use.
///
/// The account stream runs the same lookup itself after a reconnect; see
/// [`account_stream`](ExecutionClient::account_stream).
impl OrderStatusClient for BinanceSpot {
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        fetch_ended_by_key(orders, |key| {
            fetch_order_lookup(
                self.rest.clone(),
                self.rate_limiter.clone(),
                key,
                RequestKind::Query,
            )
        })
        .await
    }
}

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
    known: SharedKnownLiveOrders,
    instruments: Vec<InstrumentNameExchange>,
    initial_ws: Option<WebsocketApi>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut disconnect_time: Option<DateTime<Utc>> = None;
    // Gaps not yet recovered: read at reconnect, and retried on a timer while connected, once due.
    let mut unrecovered = UnrecoveredFills::default();
    // Instruments whose known-live orders a reconnect has yet to check, likewise.
    let mut unchecked = UncheckedOrders::default();
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
                        // Signal terminal stream death in-band before tx drops.
                        emit_stream_terminated(
                            &tx,
                            ExchangeId::BinanceSpot,
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
        let known_callback = known.clone();
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
        // verified against binance-sdk =50.0.0. Re-verify on SDK upgrade.
        let subscription = ws.subscribe_on_ws_events(move |event| {
            let Some(ref sender) = event_tx else { return };
            match event {
                WebsocketEvent::Message(json_str) => {
                    // Release: pairs with Acquire swap in monitor task so the stored
                    // `true` is visible before the monitor swaps in `false`.
                    hb_callback.store(true, Ordering::Release);
                    // Borrowed discriminator + matched-variant parse (no full Value DOM).
                    // RPC responses (the subscribe ack) are ignored inside the converter, and
                    // unrecognised frames are logged there (throttled warn).
                    let stream_terminated = convert_user_data_events(&json_str, &mut event_buf);
                    for ev in event_buf.drain(..) {
                        // Dedup check
                        if let Some(key) = dedup_key_from_event(&ev)
                            && is_duplicate(&dedup_callback, key)
                        {
                            trace!("BinanceSpot dedup: skipping duplicate event");
                            continue;
                        }
                        // Held across the send, so an order this reports ending and a reconnect's
                        // check of it reach the stream in the order they were decided, and the
                        // check reports only an order not already reported.
                        let known = KnownLiveOrders::observes(&ev.kind).then(|| {
                            let mut known = known_callback.lock();
                            known.observe(&ev);
                            known
                        });
                        if sender.send(ev).is_err() {
                            drop(known);
                            warn!("BinanceSpot account_stream receiver dropped, suppressing further sends");
                            event_tx.take();
                            if let Some(s) = signal_tx_opt.take() {
                                let _ = s.send(());
                            }
                            return;
                        }
                    }
                    // eventStreamTerminated arrives as a JSON message,
                    // not a WS close frame — signal reconnect explicitly.
                    if stream_terminated {
                        event_tx.take();
                        if let Some(s) = signal_tx_opt.take() {
                            let _ = s.send(());
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

        // --- Recovery after reconnect (each with a timeout to avoid blocking forever) ---
        // Runs after subscribe_on_ws_events so live events during recovery are captured.
        //
        // Fills first, then how the orders held as live ended: an order whose fills are not all
        // recovered is not checked yet. Each gap and check is opened before it is read, so one
        // that fails or times out is kept for a retry.
        if let Some(dt) = disconnect_time.take() {
            unrecovered.open(&instruments, dt, Utc::now());
            // Only the instruments this stream recovers fills for, so fills-first holds for each.
            let held = known.lock().instruments_among(&instruments);
            unchecked.open(held);
        }
        recover_fills(&rest, &rate_limiter, &mut unrecovered, &tx, &dedup, &known).await;
        recover_spot_ended_orders(
            &rest,
            &rate_limiter,
            &known,
            &mut unchecked,
            &unrecovered,
            &tx,
        )
        .await;

        // --- Monitor: wait for disconnect, heartbeat timeout, or consumer drop ---
        enum DisconnectReason {
            Signal,
            HeartbeatTimeout,
            ConsumerDropped,
        }
        let reason = {
            let mut signal_rx = signal_rx;
            // Gaps and order checks a recovery did not finish are retried as they fall due,
            // alongside the monitor, so a disconnect is still seen at once. An order check waiting
            // on a gap runs right after the gap's retry. It never completes; dropping it when the
            // monitor ends loses nothing, since each gap, and each instrument's order check, is
            // settled in one step as soon as its read ends.
            let retry_gaps = async {
                loop {
                    let next_check = unchecked.next_due(&unrecovered);
                    match unrecovered.next_due().into_iter().chain(next_check).min() {
                        Some(due) => tokio::time::sleep_until(due).await,
                        None => std::future::pending::<()>().await,
                    }
                    recover_fills(&rest, &rate_limiter, &mut unrecovered, &tx, &dedup, &known)
                        .await;
                    recover_spot_ended_orders(
                        &rest,
                        &rate_limiter,
                        &known,
                        &mut unchecked,
                        &unrecovered,
                        &tx,
                    )
                    .await;
                }
            };
            tokio::pin!(retry_gaps);
            loop {
                tokio::select! {
                    // Biased: a consumer drop is terminal and wins; the gap retry is polled last.
                    biased;
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
                    // Last: the arms above end the monitor and win when ready together.
                    () = &mut retry_gaps => {}
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
        // assumption (verified against binance-sdk =50.0.0): unsubscribe() stops
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
            // Consumer dropped the receiver — no StreamTerminated emit (the channel is already
            // closed, so it would be a no-op; see emit_stream_terminated docs).
            debug!("BinanceSpot connection manager exiting");
            break;
        }
        if !backoff.wait().await {
            error!("BinanceSpot max reconnect attempts exhausted, stream terminating");
            // Surrender after repeated reconnects: the most recent failure is the disconnect
            // that triggered this round (ConsumerDropped already broke out above).
            let last_error = match reason {
                DisconnectReason::HeartbeatTimeout => {
                    format!("heartbeat timeout ({HEARTBEAT_TIMEOUT_SECS}s)")
                }
                _ => "WebSocket disconnected (server close/error)".to_string(),
            };
            emit_stream_terminated(
                &tx,
                ExchangeId::BinanceSpot,
                StreamTerminationReason::ReconnectBudgetExhausted {
                    attempts: backoff.attempts(),
                    last_error,
                },
            );
            break;
        }
    }
}

/// Recover the fills of every gap in `unrecovered` that is due: the fills missed while the stream
/// was disconnected.
///
/// Reads each gap's span by REST and sends its trades through the dedup cache, so a trade already
/// delivered is not sent again. A gap whose fills are all forwarded leaves `unrecovered`; one whose
/// read fails or does not finish is retried later, until it is given up (see
/// [`UnrecoveredFills`]). Each gap is settled as soon as its read ends, so dropping this future
/// part-way, as a disconnect during a retry does, loses nothing.
///
/// `myTrades` reports executions only, with no cumulative, so each recovered trade's
/// `order_filled_quantity` is rebuilt from its order's executions by
/// [`recovered_order_totals`]. A trade whose order could not be looked up keeps `None`.
///
/// The whole recovery is bounded by [`FILL_RECOVERY_TIMEOUT_SECS`]. Fills forwarded before the
/// deadline stay delivered. Its reads are [`RequestKind::Essential`]: they wait out a rate-limit
/// cooldown but not the pause near the weight limit, which could outlast the budget.
async fn recover_fills(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    unrecovered: &mut UnrecoveredFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    known: &SharedKnownLiveOrders,
) {
    use futures::StreamExt;

    let due = unrecovered.due(tokio::time::Instant::now());
    let Some(oldest) = due.iter().map(|(_, gap)| gap.start_ms).min() else {
        return;
    };
    info!(
        gaps = due.len(),
        oldest = %gap_time(oldest),
        "BinanceSpot recovering fills missed while disconnected"
    );

    // Which gaps of `due` have been settled, recovered or failed, and how many have started: the
    // stream starts them in order, eight at a time, so those from `started` on were never read.
    let mut settled = vec![false; due.len()];
    let started = AtomicUsize::new(0);
    let recovery = async {
        let order_executions_deadline = tokio::time::Instant::now() + ORDER_EXECUTIONS_BUDGET;
        let mut recovered = 0u32;
        let mut duplicates = 0u32;

        // limit concurrency to avoid bursting Binance's request weight limits
        // (each GET /api/v3/myTrades costs 20 weight; 8 concurrent = 160 weight).
        // pagination is critical for fill recovery — a missed page means a gap read short.
        // paginate_my_trades handles the full cursor-based pagination loop shared with
        // fetch_trades.
        let mut stream =
            futures::stream::iter(due.iter().cloned().enumerate().map(|(index, (inst, gap))| {
                started.store(index + 1, Ordering::Relaxed);
                let rest = rest.clone();
                let rl = rate_limiter.clone();
                async move {
                    let span = MyTradesFrom::Span {
                        start: gap.start_ms,
                        end: gap.end_ms,
                    };
                    let raw =
                        match paginate_my_trades(&rest, &rl, &inst, span, RequestKind::Essential)
                            .await
                        {
                            Ok(pages) => pages,
                            Err(e) => return (index, Err(e)),
                        };
                    // `myTrades` carries no cumulative, so each recovered fill's is rebuilt from
                    // its order's executions; without it the fill advances the position but not
                    // the order.
                    let totals = recovered_order_totals(
                        ExchangeId::BinanceSpot,
                        &inst,
                        &raw,
                        order_executions_deadline,
                        |order_id| {
                            paginate_my_trades(
                                &rest,
                                &rl,
                                &inst,
                                MyTradesFrom::Order(order_id),
                                RequestKind::Essential,
                            )
                        },
                    )
                    .await;
                    let trades: Vec<_> = raw
                        .iter()
                        .filter_map(|t| {
                            let mut trade = convert_my_trade(t, &inst)?;
                            trade.order_filled_quantity =
                                t.id.and_then(|id| totals.get(&id).copied());
                            Some(trade)
                        })
                        .collect();
                    (index, Ok(trades))
                }
            }))
            .buffer_unordered(8);
        while let Some((index, result)) = stream.next().await {
            let (inst, gap) = &due[index];
            settled[index] = true;
            let trades = match result {
                Ok(trades) => trades,
                Err(e) => {
                    gap_failed(
                        unrecovered,
                        ExchangeId::BinanceSpot,
                        inst,
                        gap,
                        &e.to_string(),
                    );
                    continue;
                }
            };
            for trade in trades {
                // Construct the event first so dedup_key_from_event can be reused,
                // keeping key construction in one place.
                let event = UnindexedAccountEvent::new(
                    ExchangeId::BinanceSpot,
                    AccountEventKind::Trade(trade),
                );
                // Only Trade events are deduped during recovery — we don't recover NEW/CANCELLED
                // lifecycle events here (those require fetch_open_orders reconciliation).
                if let Some(key) = dedup_key_from_event(&event)
                    && is_duplicate(dedup, key)
                {
                    duplicates += 1;
                    continue;
                }
                // A recovered fill that completes its order ends it, so a later check of how the
                // orders held as live ended does not report it again.
                let sent = {
                    let mut known = known.lock();
                    known.observe(&event);
                    tx.send(event)
                };
                if sent.is_err() {
                    // early return on consumer drop — no point recovering remaining
                    // gaps if the receiver is gone.
                    debug!("BinanceSpot fill recovery: consumer dropped during recovery");
                    return;
                }
                recovered += 1;
            }
            unrecovered.recovered(inst, gap);
        }
        info!(recovered, duplicates, "BinanceSpot fill recovery complete");
    };
    // A timeout drops `recovery` at an await, between gaps: one gap's fills are sent without
    // awaiting, so each is either fully forwarded and settled, or not forwarded at all. A gap
    // whose read started and did not finish has failed; one never started stays due as it was.
    if tokio::time::timeout(Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS), recovery)
        .await
        .is_err()
    {
        let started = started.load(Ordering::Relaxed);
        for ((inst, gap), _) in due[..started]
            .iter()
            .zip(&settled)
            .filter(|(_, settled)| !**settled)
        {
            gap_failed(
                unrecovered,
                ExchangeId::BinanceSpot,
                inst,
                gap,
                "fill recovery timed out",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

/// Filter Binance account balances to the requested assets and convert to rustrade types.
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

/// Convert a Binance myTrades REST response into a rustrade Trade.
fn convert_my_trade(
    t: &binance_sdk::spot::rest_api::MyTradesResponseInner,
    instrument: &InstrumentNameExchange,
) -> Option<Trade<AssetNameExchange, InstrumentNameExchange>> {
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

    // Use actual commission asset from Binance (e.g., BNB, USDT, BTC).
    // fees_quote is set to None here; the indexer will compute it if fee is in
    // quote or base asset. For third-party assets (BNB), downstream must convert.
    // "UNKNOWN" fallback (rare: API omits commission_asset) will fail indexing.
    let fee_asset = t
        .commission_asset
        .as_deref()
        .map(AssetNameExchange::from)
        .unwrap_or_else(|| AssetNameExchange::from("UNKNOWN"));

    Some(Trade::new(
        trade_id,
        order_id,
        instrument.clone(),
        StrategyId::unknown(), // Binance doesn't carry strategy IDs
        time_exchange,
        side,
        price,
        quantity,
        // `myTrades` reports executions only -- no cumulative, no order status.
        None,
        AssetFees::new(fee_asset, commission, None),
    ))
}

/// Convert a raw BinanceSpot user-data WS frame to rustrade AccountEvents.
///
/// Pushes into the provided buffer to avoid per-message heap allocation.
/// A single Binance event (e.g., outboundAccountPosition) may map to multiple
/// rustrade events (one per asset balance).
///
/// Returns `true` if the stream should be considered terminated (requires reconnect).
///
/// # Frame shape
///
/// The subscription is WS-API `userDataStream.subscribe.signature`, so each pushed event arrives
/// wrapped as `{ "subscriptionId", "event": { "e", .. } }`, and binance-sdk passes the frame on
/// unchanged; [`parse_user_data_frame`] unwraps it, as for margin. RPC responses (the subscribe
/// acknowledgement) are ignored. A frame of any other shape, including an event without a
/// readable `e` tag, is ignored and logged by [`log_unrecognised_frame`] (throttled `warn`), so a
/// change in delivery cannot drop events silently. Unknown event types inside the envelope are
/// ignored at `trace`.
///
/// # Hot path
///
/// Reads the `e` discriminator from a borrowed view of the frame, then deserializes **only**
/// the matched variant straight from the inner event slice — avoiding the full
/// `serde_json::Value` DOM that `UserDataStreamEventsResponse`'s `#[serde(try_from = "Value")]`
/// would build for every inbound frame.
fn convert_user_data_events(frame: &str, buf: &mut Vec<UnindexedAccountEvent>) -> bool {
    let (event_type, event) = match parse_user_data_frame(frame) {
        UserDataFrame::Response => return false,
        UserDataFrame::Event {
            event_type, event, ..
        } => (event_type, event),
        UserDataFrame::Unrecognised => {
            static SEEN: AtomicU64 = AtomicU64::new(0);
            log_unrecognised_frame("BinanceSpot", &SEEN, frame);
            return false;
        }
    };
    // Tag values match `UserDataStreamEventsResponse`'s `try_from = "Value"` arms (binance-sdk).
    match event_type {
        "executionReport" => {
            // Single typed pass straight from the inner event — no intermediate DOM, and only the
            // matched branch deserializes its payload. The SDK struct ignores the unknown `e` tag.
            match serde_json::from_str::<binance_sdk::spot::websocket_api::ExecutionReport>(event) {
                Ok(report) => {
                    convert_execution_report(&report, ExchangeId::BinanceSpot, buf);
                }
                Err(e) => {
                    warn!(error = %e, "BinanceSpot: undeserializable executionReport, dropping")
                }
            }
            false
        }
        "outboundAccountPosition" => {
            match serde_json::from_str::<binance_sdk::spot::websocket_api::OutboundAccountPosition>(
                event,
            ) {
                Ok(position) => convert_account_position(position, buf),
                Err(e) => {
                    warn!(error = %e, "BinanceSpot: undeserializable outboundAccountPosition, dropping")
                }
            }
            false
        }
        "balanceUpdate" => {
            // balanceUpdate events are for deposits/withdrawals;
            // outboundAccountPosition covers balance changes from trades.
            // deposit/withdrawal balance changes are not forwarded to the consumer.
            // A caller should call fetch_balances or account_snapshot periodically to
            // reconcile balances after external transfers.
            // No log here: this is the per-frame receive hot path.
            false
        }
        "eventStreamTerminated" => {
            // Binance sends eventStreamTerminated as a JSON message, not a WS
            // close frame. Without signalling reconnect here, the stream silently dies
            // while heartbeat ping/pong keeps the connection alive.
            warn!("BinanceSpot user data stream terminated by exchange, signalling reconnect");
            true
        }
        // listStatus, externalLockUpdate, and any future/unknown event types: harmless
        // fall-through (observable at trace, never tears down the stream).
        _ => {
            trace!(event_type, "BinanceSpot ignoring unhandled user data event");
            false
        }
    }
}

/// Convert a Binance outboundAccountPosition to balance stream-update events.
///
/// Emits one `BalanceStreamUpdate` event per asset (the WS message is a `free`/`locked` partial,
/// not a full snapshot). Pushes into the provided buffer to avoid per-message allocation.
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
        // WS `outboundAccountPosition` is a free/locked partial (no debt), so emit a
        // `BalanceStreamUpdate` rather than a full `BalanceSnapshot`. Spot carries no margin debt,
        // so nothing is preserved here — but routing it through the update path keeps one
        // consistent model (REST → snapshot, WS → update) and protects margin debt downstream.
        let update =
            AssetBalanceUpdate::new(asset, BalanceUpdate::new(free, locked), time_exchange);
        buf.push(UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::BalanceStreamUpdate(
                rustrade_integration::collection::snapshot::Snapshot::new(update),
            ),
        ));
    }
}

/// Convert rustrade OrderKind + TimeInForce to Binance SDK order type and TIF.
///
/// Returns `None` for unsupported combinations, which `open_order` converts to
/// `UnsupportedOrderType` rejection.
///
/// # Conditional Orders
///
/// - `Stop` → `STOP_LOSS` (market order triggered at stop price)
/// - `StopLimit` → `STOP_LOSS_LIMIT` (limit order triggered at stop price)
/// - `TakeProfit` → `TAKE_PROFIT` (market order triggered at take-profit price)
/// - `TakeProfitLimit` → `TAKE_PROFIT_LIMIT` (limit order triggered at take-profit price)
///
/// # Trailing Stop Orders
///
/// Binance implements trailing stops via `STOP_LOSS` with `trailingDelta` parameter.
///
/// **Supported offset types:**
/// - `BasisPoints`: used directly (1 basis point = 0.01%)
/// - `Percentage`: converted to basis points (multiplied by 100)
///
/// **Unsupported:**
/// - `Absolute`: returns `None` → `UnsupportedOrderType`. The caller (`open_order`)
///   must surface this so end users can convert absolute offsets to basis points
///   themselves: `basis_points = (absolute / price) * 10000`
/// - `TrailingStopLimit`: Binance does not support limit orders with trailing stops
///
fn convert_order_kind_tif(
    kind: OrderKind,
    tif: TimeInForce,
) -> Option<(OrderPlaceTypeEnum, Option<OrderPlaceTimeInForceEnum>)> {
    // The decision logic (which kind/TIF combinations Binance supports, GTD coercion,
    // trailing-offset handling) lives in the shared classifier so spot and margin share it;
    // this adapter only maps the venue-neutral result onto spot's WS-API enum types.
    let (binance_type, binance_tif) = classify_order_kind_tif(kind, tif)?;
    let type_enum = match binance_type {
        BinanceOrderType::Market => OrderPlaceTypeEnum::Market,
        BinanceOrderType::Limit => OrderPlaceTypeEnum::Limit,
        BinanceOrderType::LimitMaker => OrderPlaceTypeEnum::LimitMaker,
        BinanceOrderType::StopLoss => OrderPlaceTypeEnum::StopLoss,
        BinanceOrderType::StopLossLimit => OrderPlaceTypeEnum::StopLossLimit,
        BinanceOrderType::TakeProfit => OrderPlaceTypeEnum::TakeProfit,
        BinanceOrderType::TakeProfitLimit => OrderPlaceTypeEnum::TakeProfitLimit,
    };
    let tif_enum = binance_tif.map(|t| match t {
        BinanceTimeInForce::Gtc => OrderPlaceTimeInForceEnum::Gtc,
        BinanceTimeInForce::Ioc => OrderPlaceTimeInForceEnum::Ioc,
        BinanceTimeInForce::Fok => OrderPlaceTimeInForceEnum::Fok,
    });
    Some((type_enum, tif_enum))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::client::binance::shared::*;
    use crate::client::dedup::{DedupEventKind, DedupKey};
    use crate::order::{TrailingOffsetType, id::ClientOrderId};
    use binance_sdk::common::errors::WebsocketError;
    use smol_str::SmolStr;

    #[test]
    fn test_binance_spot_config_new_uses_testnet_by_default() {
        let cfg = BinanceSpotConfig::new("my_key".into(), "my_secret".into());
        assert!(cfg.testnet);
        assert_eq!(cfg.api_key(), "my_key");
    }

    #[test]
    fn test_binance_spot_config_production_is_explicit() {
        let cfg = BinanceSpotConfig::production("my_key".into(), "my_secret".into());
        assert!(!cfg.testnet);
    }

    #[test]
    fn test_binance_spot_config_debug_redacts_credentials() {
        let cfg = BinanceSpotConfig::new("my_key".into(), "my_secret".into());
        let debug = format!("{cfg:?}");

        assert!(!debug.contains("my_key"), "api_key should be redacted");
        assert!(
            !debug.contains("my_secret"),
            "secret_key should be redacted"
        );
        assert!(debug.contains("testnet: true"));
    }

    #[test]
    fn test_binance_spot_config_deserialize_omitted_testnet_defaults_to_testnet() {
        let cfg: BinanceSpotConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret"
    }"#,
        )
        .unwrap();

        assert!(cfg.testnet);
        assert_eq!(cfg.api_key(), "my_key");
    }

    #[test]
    fn test_binance_spot_config_deserialize_testnet_true() {
        let cfg: BinanceSpotConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret",
        "testnet": true
    }"#,
        )
        .unwrap();

        assert!(cfg.testnet);
    }

    #[test]
    fn test_binance_spot_config_deserialize_testnet_false() {
        let cfg: BinanceSpotConfig = serde_json::from_str(
            r#"{
        "api_key": "my_key",
        "secret_key": "my_secret",
        "testnet": false
    }"#,
        )
        .unwrap();

        assert!(!cfg.testnet);
    }

    #[test]
    #[serial_test::serial]
    fn test_binance_spot_config_from_env_defaults_to_testnet() {
        temp_env::with_vars(
            [
                ("BINANCE_API_KEY", Some("my_key")),
                ("BINANCE_SECRET_KEY", Some("my_secret")),
                ("BINANCE_TESTNET", None),
            ],
            || {
                let cfg = BinanceSpotConfig::from_env().unwrap();
                assert!(cfg.testnet);
                assert_eq!(cfg.api_key(), "my_key");
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_binance_spot_config_from_env_accepts_explicit_production() {
        temp_env::with_vars(
            [
                ("BINANCE_API_KEY", Some("my_key")),
                ("BINANCE_SECRET_KEY", Some("my_secret")),
                ("BINANCE_TESTNET", Some("false")),
            ],
            || {
                let cfg = BinanceSpotConfig::from_env().unwrap();
                assert!(!cfg.testnet);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_binance_spot_config_from_env_rejects_invalid_testnet() {
        temp_env::with_vars(
            [
                ("BINANCE_API_KEY", Some("my_key")),
                ("BINANCE_SECRET_KEY", Some("my_secret")),
                ("BINANCE_TESTNET", Some("maybe")),
            ],
            || {
                let err = BinanceSpotConfig::from_env().unwrap_err();
                assert!(
                    matches!(err, BinanceSpotConfigError::InvalidTestnet(value) if value == "maybe")
                );
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_binance_spot_config_from_env_requires_credentials() {
        temp_env::with_vars(
            [
                ("BINANCE_API_KEY", None),
                ("BINANCE_SECRET_KEY", Some("my_secret")),
                ("BINANCE_TESTNET", None),
            ],
            || {
                let err = BinanceSpotConfig::from_env().unwrap_err();
                assert!(matches!(err, BinanceSpotConfigError::MissingApiKey));
            },
        );
    }

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
        use rust_decimal::Decimal;

        // binance-sdk enums don't derive PartialEq, so use matches!
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Market, TimeInForce::ImmediateOrCancel),
            Some((OrderPlaceTypeEnum::Market, None))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            Some((
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Gtc)
            ))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: true }
            ),
            Some((OrderPlaceTypeEnum::LimitMaker, None))
        ));
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Limit, TimeInForce::FillOrKill),
            Some((
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Fok)
            ))
        ));
        assert!(matches!(
            convert_order_kind_tif(OrderKind::Limit, TimeInForce::ImmediateOrCancel),
            Some((
                OrderPlaceTypeEnum::Limit,
                Some(OrderPlaceTimeInForceEnum::Ioc)
            ))
        ));
        // GoodUntilEndOfDay is unsupported on Binance (no native EOD order) — surfaced as
        // unsupported rather than silently coerced to GTC, which would drop the EOD semantics.
        assert!(convert_order_kind_tif(OrderKind::Limit, TimeInForce::GoodUntilEndOfDay).is_none());

        // Conditional orders
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::Stop {
                    trigger_price: Decimal::from(100)
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            Some((OrderPlaceTypeEnum::StopLoss, None))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::StopLimit {
                    trigger_price: Decimal::from(100)
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            Some((
                OrderPlaceTypeEnum::StopLossLimit,
                Some(OrderPlaceTimeInForceEnum::Gtc)
            ))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::TakeProfit {
                    trigger_price: Decimal::from(150)
                },
                TimeInForce::ImmediateOrCancel
            ),
            Some((OrderPlaceTypeEnum::TakeProfit, None))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::TakeProfitLimit {
                    trigger_price: Decimal::from(150)
                },
                TimeInForce::FillOrKill
            ),
            Some((
                OrderPlaceTypeEnum::TakeProfitLimit,
                Some(OrderPlaceTimeInForceEnum::Fok)
            ))
        ));

        // TrailingStop with BasisPoints/Percentage → StopLoss (trailingDelta set separately)
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::TrailingStop {
                    offset: Decimal::from(100),
                    offset_type: TrailingOffsetType::BasisPoints,
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            Some((OrderPlaceTypeEnum::StopLoss, None))
        ));
        assert!(matches!(
            convert_order_kind_tif(
                OrderKind::TrailingStop {
                    offset: Decimal::from(5),
                    offset_type: TrailingOffsetType::Percentage,
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            ),
            Some((OrderPlaceTypeEnum::StopLoss, None))
        ));

        // TrailingStop with Absolute → unsupported (returns None)
        assert!(
            convert_order_kind_tif(
                OrderKind::TrailingStop {
                    offset: Decimal::from(10),
                    offset_type: TrailingOffsetType::Absolute,
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            )
            .is_none()
        );

        // TrailingStopLimit → unsupported (Binance doesn't support)
        assert!(
            convert_order_kind_tif(
                OrderKind::TrailingStopLimit {
                    offset: Decimal::from(100),
                    offset_type: TrailingOffsetType::BasisPoints,
                    limit_offset: Decimal::from(10),
                },
                TimeInForce::GoodUntilCancelled { post_only: false }
            )
            .is_none()
        );
    }

    #[test]
    fn test_parse_binance_api_error() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        assert!(matches!(
            parse_binance_api_error("Insufficient balance".into(), &instrument),
            ApiError::BalanceInsufficient(None, _)
        ));
        assert!(matches!(
            parse_binance_api_error("Not enough funds".into(), &instrument),
            ApiError::BalanceInsufficient(None, _)
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
        // -2010 is read by its text: Binance gives it for many reasons besides balance
        assert!(matches!(
            parse_binance_api_error(
                "Server-side response error (code -2010): Account has insufficient balance".into(),
                &instrument
            ),
            ApiError::BalanceInsufficient(None, _)
        ));
        assert!(matches!(
            parse_binance_api_error(
                "Server-side response error (code -2010): Order would trigger immediately.".into(),
                &instrument
            ),
            ApiError::OrderRejected(_)
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
            kind: DedupEventKind::OrderState {
                filled_quantity: Decimal::ZERO,
            },
        };
        assert!(!is_duplicate(&cache, key2));
    }

    /// Two order-state snapshots for one order differ only by cumulative filled quantity, and
    /// must not be deduplicated against each other -- otherwise the acknowledgement (`z = 0`)
    /// swallows every fill snapshot that follows it.
    #[test]
    fn dedup_distinguishes_order_states_and_still_collapses_a_replay() {
        let cache = new_dedup_cache();
        let state = |filled: &str| DedupKey {
            instrument: SmolStr::from("BTCUSDT"),
            id: SmolStr::from("12345"),
            kind: DedupEventKind::OrderState {
                filled_quantity: Decimal::from_str(filled).unwrap(),
            },
        };

        // Acknowledgement, then two fills on the same order: three distinct states.
        assert!(!is_duplicate(&cache, state("0")), "ack is new");
        assert!(
            !is_duplicate(&cache, state("1")),
            "first fill is not the ack"
        );
        assert!(
            !is_duplicate(&cache, state("2")),
            "second fill is not the first"
        );

        // A re-delivered snapshot still collides, which is what keeps a replayed
        // acknowledgement from resurrecting an order that has since retired.
        assert!(
            is_duplicate(&cache, state("0")),
            "replayed ack is a duplicate"
        );
        assert!(
            is_duplicate(&cache, state("2")),
            "replayed fill is a duplicate"
        );

        // Decimal hashes normalised, so the venue's chosen representation does not split a key.
        assert!(
            is_duplicate(&cache, state("2.00")),
            "2.00 must be the same key as 2"
        );
    }

    /// Drive frames through the same two stages the live WebSocket callback does: convert, then
    /// apply the dedup gate. Returns what a consumer would actually receive.
    ///
    /// Every other test in this file calls `convert_execution_report` directly. That is the wrong
    /// altitude to prove a fill reaches anyone: the converter can be perfectly correct while the
    /// gate downstream of it discards what the converter produced.
    fn drive_ws_pipeline(frames: &[&str]) -> Vec<UnindexedAccountEvent> {
        use crate::client::dedup::dedup_key_from_event;

        let cache = new_dedup_cache();
        let mut delivered = Vec::new();
        let mut buf = Vec::new();
        for frame in frames {
            let _ = convert_user_data_events(frame, &mut buf);
            for ev in buf.drain(..) {
                if let Some(key) = dedup_key_from_event(&ev)
                    && is_duplicate(&cache, key)
                {
                    continue;
                }
                delivered.push(ev);
            }
        }
        delivered
    }

    // Wrapped as the WS-API subscription delivers them: `{ subscriptionId, event }`.
    const WS_NEW: &str = r#"{"subscriptionId":0,"event":{"e":"executionReport","s":"BTCUSDT",
        "i":12345,"c":"client-1","x":"NEW","X":"NEW","S":"BUY","o":"LIMIT","f":"GTC","q":"2",
        "p":"100","z":"0","T":1700000000000}}"#;

    const WS_PARTIAL_FILL: &str = r#"{"subscriptionId":0,"event":{"e":"executionReport",
        "s":"BTCUSDT","i":12345,"c":"client-1","x":"TRADE","X":"PARTIALLY_FILLED","S":"BUY",
        "o":"LIMIT","f":"GTC","q":"2","p":"100","z":"1","l":"1","L":"100","t":555,"n":"0.1",
        "N":"USDT","T":1700000001000}}"#;

    /// A fill's order snapshot must survive the dedup gate that sits between the converter and the
    /// consumer.
    ///
    /// The acknowledgement and the fill describe one order id, so a key built from the id alone
    /// makes the second look like a replay of the first. `Open::filled_quantity` then never leaves
    /// the `0` the acknowledgement carried, which is the defect the converter change was meant to
    /// fix -- fixed in the converter, undone one stage later.
    #[test]
    fn ws_fill_snapshot_survives_the_dedup_gate() {
        use crate::order::state::ActiveOrderState;

        let delivered = drive_ws_pipeline(&[WS_NEW, WS_PARTIAL_FILL]);

        assert_eq!(
            delivered.len(),
            3,
            "expected the ack snapshot, then the fill's execution and its snapshot, got: {delivered:?}"
        );

        let snapshots: Vec<Decimal> = delivered
            .iter()
            .filter_map(|ev| match &ev.kind {
                AccountEventKind::OrderSnapshot(snap) => match &snap.0.state {
                    OrderState::Active(ActiveOrderState::Open(open)) => Some(open.filled_quantity),
                    _ => None,
                },
                _ => None,
            })
            .collect();

        assert_eq!(
            snapshots,
            vec![Decimal::ZERO, Decimal::ONE],
            "the fill's snapshot must reach the consumer carrying the advanced filled quantity"
        );
    }

    /// The gate must still collapse a genuinely re-delivered frame, because a replayed
    /// acknowledgement for a retired order is re-inserted as a live resting order by the engine.
    #[test]
    fn ws_replayed_frame_is_still_deduplicated() {
        let delivered = drive_ws_pipeline(&[WS_NEW, WS_PARTIAL_FILL, WS_NEW, WS_PARTIAL_FILL]);

        assert_eq!(
            delivered.len(),
            3,
            "re-delivering both frames must add nothing, got: {delivered:?}"
        );
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
    fn test_classify_ws_order_error_recognises_venue_responses_only() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        // A ResponseError from binance-sdk (the venue answered with status >= 400) is classified
        let rejection = anyhow::anyhow!(WebsocketError::ResponseError {
            code: -2010,
            message: "Account has insufficient balance for requested action.".into(),
        });
        assert!(
            matches!(
                classify_ws_order_error(&rejection, &instrument),
                Some(OrderError::Rejected(ApiError::BalanceInsufficient(None, _)))
            ),
            "ResponseError should be classified as a venue response"
        );

        // A transport error (e.g. connection reset) is NOT a venue response
        let transport = anyhow::anyhow!("connection reset by peer");
        assert!(
            classify_ws_order_error(&transport, &instrument).is_none(),
            "plain transport error should not be classified as a venue response"
        );

        // A rate-limit error string (not a WebsocketError) is NOT a venue response
        let rate_limit = anyhow::anyhow!("Too many requests. You are being rate-limited.");
        assert!(
            classify_ws_order_error(&rate_limit, &instrument).is_none(),
            "rate-limit string error should not be classified as a venue response"
        );
    }

    #[test]
    fn test_dedup_key_from_event_trade_includes_instrument() {
        use crate::order::id::{OrderId, StrategyId};
        use crate::trade::{AssetFees, Trade, TradeId};
        use chrono::Utc;
        use rust_decimal::Decimal;
        use rustrade_instrument::Side;

        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let trade = Trade::<AssetNameExchange, InstrumentNameExchange>::new(
            TradeId::new("9001"),
            OrderId::new("4242"),
            instrument.clone(),
            StrategyId::unknown(),
            Utc::now(),
            Side::Buy,
            Decimal::ZERO,
            Decimal::ZERO,
            None,
            AssetFees::new(
                AssetNameExchange::from("USDT"),
                Decimal::ZERO,
                Some(Decimal::ZERO),
            ),
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
        let trade2 = Trade::<AssetNameExchange, InstrumentNameExchange>::new(
            TradeId::new("9001"),
            OrderId::new("7777"),
            instrument2,
            StrategyId::unknown(),
            Utc::now(),
            Side::Buy,
            Decimal::ZERO,
            Decimal::ZERO,
            None,
            AssetFees::new(
                AssetNameExchange::from("USDT"),
                Decimal::ZERO,
                Some(Decimal::ZERO),
            ),
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
            AccountEventKind::BalanceStreamUpdate(snap) => {
                let update = &snap.0;
                assert_eq!(update.asset.as_ref(), "BTC");
                // free = 1.5, locked = 0.5; total = free + locked = 2.0
                let expected_free = Decimal::from_str("1.5").unwrap();
                let expected_locked = Decimal::from_str("0.5").unwrap();
                assert_eq!(update.update.free, expected_free);
                assert_eq!(update.update.locked, expected_locked);
                assert_eq!(update.update.total(), Decimal::from_str("2.0").unwrap());
            }
            other => panic!("expected BalanceStreamUpdate, got {:?}", other),
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
            AccountEventKind::BalanceStreamUpdate(snap) => {
                assert_eq!(snap.0.asset.as_ref(), "USDT");
            }
            other => panic!("expected BalanceStreamUpdate, got {:?}", other),
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
            AccountEventKind::BalanceStreamUpdate(snap) => {
                assert_eq!(snap.0.asset.as_ref(), "ETH");
            }
            other => panic!("expected BalanceStreamUpdate, got {:?}", other),
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

    /// Run the shared converter over one report and collect what it appends.
    fn convert(
        report: binance_sdk::spot::websocket_api::ExecutionReport,
    ) -> Vec<UnindexedAccountEvent> {
        let mut buf = Vec::new();
        convert_execution_report(&report, ExchangeId::BinanceSpot, &mut buf);
        buf
    }

    /// The one event produced by a report that maps to a single `AccountEvent`.
    ///
    /// Asserts nothing followed it, so a test written for a single-event report fails loudly if
    /// that report ever starts producing two.
    fn sole_event(mut events: Vec<UnindexedAccountEvent>) -> Option<UnindexedAccountEvent> {
        assert!(
            events.len() <= 1,
            "expected at most a single event, got: {events:?}"
        );
        events.pop()
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

        let event = sole_event(convert(report)).expect("NEW event should produce Some");
        assert_eq!(event.exchange, ExchangeId::BinanceSpot);
        match &event.kind {
            AccountEventKind::OrderSnapshot(snap) => {
                let order = &snap.0;
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.kind, OrderKind::Limit);
                assert_eq!(order.price, Some(Decimal::from_str("50000.00").unwrap()));
                assert_eq!(order.quantity, Decimal::from_str("0.01").unwrap());
                assert_eq!(order.key.cid.0.as_str(), "client-1");
                assert_eq!(order.key.instrument.name().as_str(), "BTCUSDT");
            }
            other => panic!("NEW should yield OrderSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_convert_execution_report_trade() {
        // No `X` (order status): the report says nothing about whether the order is still
        // working, so only the execution is emitted.
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            s_uppercase: Some("BUY".to_string()),
            t: Some(9999),
            l_uppercase: Some("50000.00".to_string()),
            l: Some("0.01".to_string()),
            n: Some("0.000001".to_string()),
            ..make_base_report()
        };

        let event = sole_event(convert(report)).expect("TRADE event should produce Some");
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

    /// The `(Trade, OrderSnapshot)` pair a fill report must produce, in that order.
    fn trade_events(
        events: Vec<UnindexedAccountEvent>,
    ) -> (
        Trade<AssetNameExchange, InstrumentNameExchange>,
        crate::order::Order<
            ExchangeId,
            InstrumentNameExchange,
            OrderState<AssetNameExchange, InstrumentNameExchange>,
        >,
    ) {
        let [first, second]: [UnindexedAccountEvent; 2] = events
            .try_into()
            .expect("a fill report must produce an execution and an order snapshot");
        let AccountEventKind::Trade(trade) = first.kind else {
            panic!("first event must be the Trade, got {:?}", first.kind);
        };
        let AccountEventKind::OrderSnapshot(snap) = second.kind else {
            panic!(
                "second event must be the OrderSnapshot, got {:?}",
                second.kind
            );
        };
        (trade, snap.0)
    }

    /// A `PARTIALLY_FILLED` report carries the order's cumulative filled quantity in `z`. The
    /// execution alone never reaches `Open::filled_quantity`, so without the snapshot a half-done
    /// order reads as having nothing filled until REST reconciliation refreshes it.
    #[test]
    fn a_partially_filled_report_carries_the_cumulative_filled_quantity() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            x_uppercase: Some("PARTIALLY_FILLED".to_string()),
            s_uppercase: Some("BUY".to_string()),
            o: Some("LIMIT".to_string()),
            p: Some("50000.00".to_string()),
            q: Some("0.01".to_string()),
            f: Some("GTC".to_string()),
            t: Some(9999),
            l_uppercase: Some("50000.00".to_string()),
            l: Some("0.004".to_string()),
            z: Some("0.004".to_string()),
            ..make_base_report()
        };

        let (trade, order) = trade_events(convert(report));
        assert_eq!(trade.quantity, Decimal::from_str("0.004").unwrap());

        let OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) = order.state
        else {
            panic!("expected an Open snapshot, got {:?}", order.state);
        };
        assert_eq!(open.filled_quantity, Decimal::from_str("0.004").unwrap());
        assert_eq!(
            open.quantity_remaining(order.quantity),
            Decimal::from_str("0.006").unwrap()
        );
    }

    /// A `FILLED` report reports nothing left to fill, which is how the engine learns the order
    /// is done rather than leaving it as a resting order that no longer exists at the venue.
    #[test]
    fn a_filled_report_reports_nothing_left_to_fill() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("TRADE".to_string()),
            x_uppercase: Some("FILLED".to_string()),
            s_uppercase: Some("BUY".to_string()),
            o: Some("LIMIT".to_string()),
            p: Some("50000.00".to_string()),
            q: Some("0.01".to_string()),
            f: Some("GTC".to_string()),
            t: Some(10_000),
            l_uppercase: Some("50000.00".to_string()),
            l: Some("0.006".to_string()),
            z: Some("0.01".to_string()),
            ..make_base_report()
        };

        let (_trade, order) = trade_events(convert(report));
        let OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) = order.state
        else {
            panic!("expected an Open snapshot, got {:?}", order.state);
        };
        assert!(
            open.quantity_remaining(order.quantity).is_zero(),
            "a completed order must report nothing left to fill so the engine retires it"
        );
    }

    /// A fill whose order the exchange no longer reports as working must not be written back into
    /// engine state as an `Open` order. The execution still counts -- it moved the position --
    /// but resurrecting the order would leave a resting order that does not exist at the venue.
    #[test]
    fn a_trade_for_an_order_no_longer_working_emits_the_execution_without_a_snapshot() {
        for status in ["CANCELED", "EXPIRED", "REJECTED", "PENDING_CANCEL", "NEW"] {
            let report = binance_sdk::spot::websocket_api::ExecutionReport {
                x: Some("TRADE".to_string()),
                x_uppercase: Some(status.to_string()),
                s_uppercase: Some("BUY".to_string()),
                o: Some("LIMIT".to_string()),
                p: Some("50000.00".to_string()),
                q: Some("0.01".to_string()),
                f: Some("GTC".to_string()),
                t: Some(9999),
                l_uppercase: Some("50000.00".to_string()),
                l: Some("0.004".to_string()),
                z: Some("0.004".to_string()),
                ..make_base_report()
            };
            let events = convert(report);
            assert_eq!(
                events.len(),
                1,
                "status {status} must produce the execution alone, got: {events:?}"
            );
            assert!(
                matches!(events[0].kind, AccountEventKind::Trade(_)),
                "the execution must still be reported for status {status}"
            );
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
            sole_event(convert(report)).is_none(),
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
            sole_event(convert(report)).is_none(),
            "missing last qty (l) should return None"
        );
    }

    #[test]
    fn test_convert_execution_report_canceled() {
        let report = binance_sdk::spot::websocket_api::ExecutionReport {
            x: Some("CANCELED".to_string()),
            ..make_base_report()
        };

        let event = sole_event(convert(report)).expect("CANCELED should produce Some");
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
        let event = sole_event(convert(report)).expect("EXPIRED should produce Some");
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
        let event = sole_event(convert(report)).expect("EXPIRED_IN_MATCH should produce Some");
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

        let event = sole_event(convert(report)).expect("REJECTED should produce Some");
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
            sole_event(convert(report)).is_none(),
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
            sole_event(convert(report)).is_none(),
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
            sole_event(convert(report)).is_none(),
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
            sole_event(convert(report)).is_none(),
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
        let event = sole_event(convert(report)).expect("REPLACE should produce Some");
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

    fn make_base_open_order() -> binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
        binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            order_id: Some(12345),
            client_order_id: Some("cid-abc".to_string()),
            side: Some("BUY".to_string()),
            r#type: Some("LIMIT".to_string()),
            price: Some("50000.00".to_string()),
            orig_qty: Some("0.01".to_string()),
            executed_qty: Some("0.0".to_string()),
            time_in_force: Some("GTC".to_string()),
            time: Some(1_700_000_000_000),
            status: Some("NEW".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_convert_open_order_all_orders_response_converts_identically() {
        // `convert_open_order` is generic over `BinanceOrderFields`; the live open-orders path feeds
        // it `GetOpenOrdersResponseInner` (what every other test here uses) while `allOrders`
        // would feed it `AllOrdersResponseInner`. Pin that the second impl reads the same fields,
        // so the two endpoint structs cannot drift apart unnoticed. This guards field drift only:
        // an `allOrders` row for a finished order must not convert, which
        // `test_convert_open_order_refuses_an_all_orders_row_that_is_not_live` pins.
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let all_orders = binance_sdk::spot::rest_api::AllOrdersResponseInner {
            order_id: Some(12345),
            client_order_id: Some("cid-abc".to_string()),
            side: Some("BUY".to_string()),
            r#type: Some("LIMIT".to_string()),
            price: Some("50000.00".to_string()),
            orig_qty: Some("0.01".to_string()),
            executed_qty: Some("0.0".to_string()),
            time_in_force: Some("GTC".to_string()),
            time: Some(1_700_000_000_000),
            status: Some("NEW".to_string()),
            ..Default::default()
        };
        let from_all_orders = convert_open_order(&all_orders, ExchangeId::BinanceSpot, &instrument)
            .expect("valid order should convert");
        let from_open_orders = convert_open_order(
            &make_base_open_order(),
            ExchangeId::BinanceSpot,
            &instrument,
        )
        .expect("valid order should convert");
        assert_eq!(from_all_orders, from_open_orders);
    }

    /// An `allOrders` row for an order that is no longer live must not become an `Open` order.
    ///
    /// The case that matters is a cancelled order that had partly filled: as `Open` it would rest
    /// in engine state with quantity remaining, and nothing at the exchange would ever fill or
    /// cancel it.
    #[test]
    fn test_convert_open_order_refuses_an_all_orders_row_that_is_not_live() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        for status in [
            "CANCELED",
            "FILLED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "REJECTED",
            "PENDING_CANCEL",
            "SOME_FUTURE_STATUS",
        ] {
            let row = binance_sdk::spot::rest_api::AllOrdersResponseInner {
                order_id: Some(12345),
                client_order_id: Some("cid-abc".to_string()),
                side: Some("BUY".to_string()),
                r#type: Some("LIMIT".to_string()),
                price: Some("50000.00".to_string()),
                orig_qty: Some("0.01".to_string()),
                executed_qty: Some("0.004".to_string()),
                time_in_force: Some("GTC".to_string()),
                time: Some(1_700_000_000_000),
                status: Some(status.to_string()),
                ..Default::default()
            };
            assert_eq!(
                convert_open_order(&row, ExchangeId::BinanceSpot, &instrument),
                None,
                "{status}"
            );
        }
    }

    #[test]
    fn test_convert_open_order_admits_every_live_status() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        for status in ["NEW", "PARTIALLY_FILLED", "PENDING_NEW"] {
            let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
                status: Some(status.to_string()),
                ..make_base_open_order()
            };
            assert!(
                convert_open_order(&o, ExchangeId::BinanceSpot, &instrument).is_some(),
                "{status}"
            );
        }
    }

    /// A listing is complete only while it shows every row under the id its order was placed with.
    ///
    /// The engine retires an order a complete snapshot leaves out, so a row that is dropped, or kept
    /// under its `orderId` alone, must make the listing incomplete: either can hide a live order.
    #[test]
    fn test_convert_open_order_listing_is_complete_only_when_every_row_survives_under_its_cid() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let second = || binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            order_id: Some(67890),
            client_order_id: Some("cid-def".to_string()),
            ..make_base_open_order()
        };

        let listing = convert_open_order_listing(
            &[make_base_open_order(), second()],
            ExchangeId::BinanceSpot,
            &instrument,
        );
        assert_eq!(listing.orders.len(), 2);
        assert!(listing.complete);

        let empty = convert_open_order_listing::<
            binance_sdk::spot::rest_api::GetOpenOrdersResponseInner,
        >(&[], ExchangeId::BinanceSpot, &instrument);
        assert!(empty.orders.is_empty());
        assert!(empty.complete, "no open orders is a complete answer");

        let dropped = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            side: None,
            ..second()
        };
        let listing = convert_open_order_listing(
            &[make_base_open_order(), dropped],
            ExchangeId::BinanceSpot,
            &instrument,
        );
        assert_eq!(listing.orders.len(), 1);
        assert!(!listing.complete, "a dropped row may be a live order");

        let without_cid = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            client_order_id: None,
            ..second()
        };
        let listing = convert_open_order_listing(
            &[make_base_open_order(), without_cid],
            ExchangeId::BinanceSpot,
            &instrument,
        );
        assert_eq!(listing.orders.len(), 2, "the row is still converted");
        assert!(
            !listing.complete,
            "an order kept under its orderId cannot be found under the cid it was placed with"
        );
    }

    #[test]
    fn test_convert_open_order_missing_status_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            status: None,
            ..make_base_open_order()
        };
        assert!(convert_open_order(&o, ExchangeId::BinanceSpot, &instrument).is_none());
    }

    /// A REST order snapshot is stamped with when the order last changed, not when it was created.
    ///
    /// The engine discards an `Open` snapshot older than the state it already tracks, so a
    /// snapshot carrying `time` is thrown away as soon as a WebSocket fill has advanced the
    /// tracked order past creation -- the exact order a reconciliation fetch is meant to repair.
    #[test]
    fn test_convert_open_order_prefers_update_time_over_creation_time() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        let updated = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            update_time: Some(1_700_000_005_000),
            ..make_base_open_order()
        };
        let order = convert_open_order(&updated, ExchangeId::BinanceSpot, &instrument)
            .expect("valid order should convert");
        assert_eq!(
            order.state.time_exchange,
            Utc.timestamp_millis_opt(1_700_000_005_000)
                .single()
                .unwrap(),
            "update_time must win over time"
        );

        // Falls back to `time` when the venue omits `update_time`, rather than to `now` -- which
        // would be worse than creation time, since it is not a venue-reported instant at all.
        let order = convert_open_order(
            &make_base_open_order(),
            ExchangeId::BinanceSpot,
            &instrument,
        )
        .expect("valid order should convert");
        assert_eq!(
            order.state.time_exchange,
            Utc.timestamp_millis_opt(1_700_000_000_000)
                .single()
                .unwrap(),
            "absent update_time falls back to time"
        );
    }

    #[test]
    fn test_convert_open_order_happy_path() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let order = convert_open_order(
            &make_base_open_order(),
            ExchangeId::BinanceSpot,
            &instrument,
        )
        .expect("valid order should convert");
        assert_eq!(order.key.instrument, instrument);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.kind, OrderKind::Limit);
        assert_eq!(order.price, Some(Decimal::from_str("50000.00").unwrap()));
        assert_eq!(order.quantity, Decimal::from_str("0.01").unwrap());
        assert_eq!(order.state.filled_quantity, Decimal::ZERO);
    }

    #[test]
    fn test_convert_open_order_owned_symbol_recovers_instrument() {
        // The no-symbol "return all" path derives the instrument from each order's own `symbol`.
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            symbol: Some("ETHUSDT".to_string()),
            ..make_base_open_order()
        };
        let order = convert_open_order_owned_symbol(&o, ExchangeId::BinanceSpot)
            .expect("valid order should convert");
        assert_eq!(order.key.instrument.name().as_str(), "ETHUSDT");
        assert_eq!(order.side, Side::Buy);
    }

    #[test]
    fn test_convert_open_order_owned_symbol_missing_symbol_returns_none() {
        // make_base_open_order() leaves `symbol` unset — drop rather than guess the instrument.
        let o = make_base_open_order();
        assert!(o.symbol.is_none());
        assert!(convert_open_order_owned_symbol(&o, ExchangeId::BinanceSpot).is_none());
    }

    #[test]
    fn test_convert_open_order_missing_order_id_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            order_id: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, ExchangeId::BinanceSpot, &instrument).is_none(),
            "missing orderId should return None"
        );
    }

    #[test]
    fn test_convert_open_order_missing_side_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            side: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, ExchangeId::BinanceSpot, &instrument).is_none(),
            "missing side should return None"
        );
    }

    #[test]
    fn test_convert_open_order_missing_type_returns_none() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            r#type: None,
            ..make_base_open_order()
        };
        assert!(
            convert_open_order(&o, ExchangeId::BinanceSpot, &instrument).is_none(),
            "missing type should return None"
        );
    }

    #[test]
    fn test_convert_open_order_executed_qty_none_defaults_to_zero() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            executed_qty: None,
            ..make_base_open_order()
        };
        let order = convert_open_order(&o, ExchangeId::BinanceSpot, &instrument)
            .expect("None executedQty should still convert");
        assert_eq!(
            order.state.filled_quantity,
            Decimal::ZERO,
            "None executedQty should default to zero"
        );
    }

    #[test]
    fn test_convert_open_order_executed_qty_unparseable_defaults_to_zero() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let o = binance_sdk::spot::rest_api::GetOpenOrdersResponseInner {
            executed_qty: Some("bad-value".to_string()),
            ..make_base_open_order()
        };
        let order = convert_open_order(&o, ExchangeId::BinanceSpot, &instrument)
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
        use rustrade_integration::collection::snapshot::Snapshot;

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
                None, // Market orders have no limit price
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
                None, // Market orders have no limit price
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
                None, // Market orders have no limit price
                Decimal::ONE,
                OrderKind::Market,
                TimeInForce::ImmediateOrCancel,
                OrderState::<AssetNameExchange, InstrumentNameExchange>::Inactive(
                    InactiveOrderState::FullyFilled(crate::order::state::Filled::new(
                        OrderId::new("123"),
                        Utc::now(),
                        Decimal::ONE, // FullyFilled must have non-zero filled_quantity
                        None,
                    )),
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
        let event = sole_event(convert(report)).expect("REJECTED report should produce Some");
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

    /// Build a raw user-data wire frame from an SDK event struct.
    ///
    /// The SDK event structs carry only the `E` (event time) field, not the lowercase `e`
    /// discriminator, so the tag must be injected. The event is then wrapped as the WS-API
    /// subscription delivers it, `{ "subscriptionId": 0, "event": { "e": "<type>", .. } }`.
    fn user_data_frame<T: serde::Serialize>(event_type: &str, event: &T) -> String {
        let mut value = serde_json::to_value(event).expect("event serializes to Value");
        value
            .as_object_mut()
            .expect("event serializes to a JSON object")
            .insert(
                "e".to_string(),
                serde_json::Value::String(event_type.to_string()),
            );
        serde_json::json!({ "subscriptionId": 0, "event": value }).to_string()
    }

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
        let frame = user_data_frame("executionReport", &report);
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(&frame, &mut buf);
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
        let frame = user_data_frame("outboundAccountPosition", &position);
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(&frame, &mut buf);
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
        let frame = user_data_frame("balanceUpdate", &update);
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(&frame, &mut buf);
        assert!(
            !terminated,
            "BalanceUpdate should not signal stream termination"
        );
        assert!(buf.is_empty(), "BalanceUpdate should push no events");
    }

    #[test]
    fn test_convert_user_data_events_stream_terminated_signals_reconnect() {
        // No payload struct — the terminal event is just the discriminator and its time.
        let frame =
            r#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1700000000000}}"#;
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(frame, &mut buf);
        assert!(
            terminated,
            "eventStreamTerminated must signal stream termination"
        );
        assert!(
            buf.is_empty(),
            "eventStreamTerminated should push no events"
        );
    }

    #[test]
    fn test_convert_user_data_events_unknown_event_ignored() {
        // listStatus / externalLockUpdate / future event types: harmless fall-through —
        // ignored, no events pushed, stream not terminated.
        let frame =
            r#"{"subscriptionId":0,"event":{"e":"listStatus","E":1700000000000,"s":"BTCUSDT"}}"#;
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(frame, &mut buf);
        assert!(!terminated, "unknown event must not signal termination");
        assert!(buf.is_empty(), "unknown event should push no events");
    }

    /// Regression: the WS-API subscription wraps every event, and binance-sdk passes the frame on
    /// unchanged. The converter used to read `e` from the top level, found none in the envelope,
    /// and dropped every event without a log. An enveloped fill must reach the buffer, and a frame
    /// that is not an envelope must not be read as one.
    #[test]
    fn test_convert_user_data_events_reads_the_ws_api_envelope() {
        let fill = r#"{"subscriptionId":3,"event":{"e":"executionReport","E":1700000001000,
            "s":"BTCUSDT","c":"client-1","S":"SELL","o":"MARKET","f":"GTC","q":"0.5","p":"0",
            "x":"TRADE","X":"FILLED","i":777,"l":"0.5","z":"0.5","L":"40000","n":"0.02",
            "N":"USDT","T":1700000001000,"t":888}}"#;
        let mut buf = Vec::new();
        assert!(!convert_user_data_events(fill, &mut buf));
        assert!(
            buf.iter()
                .any(|ev| matches!(ev.kind, AccountEventKind::Trade(_))),
            "the enveloped fill must produce its trade, got: {buf:?}"
        );

        // The pre-fix fixture shape: a bare event with no envelope is not an event frame.
        let bare = r#"{"e":"executionReport","s":"BTCUSDT","i":777,"x":"NEW","X":"NEW"}"#;
        buf.clear();
        assert!(!convert_user_data_events(bare, &mut buf));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_convert_user_data_events_non_user_data_frame_ignored() {
        // RPC responses such as the subscribe ack carry a top-level `id` — ignored, not mis-parsed.
        let frame = r#"{"id":"abc","status":200,"result":[]}"#;
        let mut buf = Vec::new();
        let terminated = convert_user_data_events(frame, &mut buf);
        assert!(
            !terminated,
            "non-user-data frame must not signal termination"
        );
        assert!(buf.is_empty(), "non-user-data frame should push no events");
    }

    // ---------------------------------------------------------------------------
    // RateLimitTracker tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_rate_limit_tracker_not_blocked_initially() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        // wait_if_blocked should return immediately (no cooldown set)
        tokio::time::timeout(
            std::time::Duration::from_millis(1),
            tracker.wait_if_blocked(RequestKind::Order),
        )
        .await
        .expect("wait_if_blocked should return immediately when not blocked");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_blocks_until_deadline() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        let delay = Duration::from_secs(5);
        tracker.on_rate_limited(Some(delay));

        // Should not complete immediately
        assert!(
            tokio::time::timeout(
                Duration::from_millis(1),
                tracker.wait_if_blocked(RequestKind::Order)
            )
            .await
            .is_err(),
            "wait_if_blocked should block while cooldown is active"
        );

        // Advance past cooldown
        tokio::time::advance(delay + Duration::from_millis(1)).await;
        tokio::time::timeout(
            Duration::from_millis(1),
            tracker.wait_if_blocked(RequestKind::Order),
        )
        .await
        .expect("wait_if_blocked should return after cooldown expires");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_cooldown_extends_to_max() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);

        // Set initial 5s cooldown
        tracker.on_rate_limited(Some(Duration::from_secs(5)));
        // Extend with a longer 10s cooldown — deadline should be pushed out
        tracker.on_rate_limited(Some(Duration::from_secs(10)));

        // Advance past the initial 5s — should still be blocked
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(1),
                tracker.wait_if_blocked(RequestKind::Order)
            )
            .await
            .is_err(),
            "cooldown should have been extended to 10s"
        );

        // Advance past the extended 10s deadline
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(
            Duration::from_millis(1),
            tracker.wait_if_blocked(RequestKind::Order),
        )
        .await
        .expect("wait_if_blocked should return after extended cooldown expires");
    }

    #[tokio::test]
    async fn test_rate_limit_tracker_shorter_cooldown_does_not_shorten() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);

        // Set 10s cooldown then try to shorten with 2s — deadline should stay at 10s
        tracker.on_rate_limited(Some(Duration::from_secs(10)));
        tracker.on_rate_limited(Some(Duration::from_secs(2)));

        // Advance past 2s — should still be blocked
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(1),
                tracker.wait_if_blocked(RequestKind::Order)
            )
            .await
            .is_err(),
            "shorter on_rate_limited must not shorten existing cooldown"
        );
    }

    // ---------------------------------------------------------------------------
    // L1: classify_ws_order_error with a context-wrapped error chain
    // ---------------------------------------------------------------------------

    #[test]
    fn test_classify_ws_order_error_with_wrapped_error_chain() {
        // `classify_ws_order_error` uses `anyhow::Error::downcast_ref`, which searches
        // the *entire* error chain (not just the root). This test verifies that a
        // ResponseError wrapped in anyhow context layers is still detected correctly,
        // so SDK-internal context wrapping does not break the rejection check.
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let raw = anyhow::anyhow!(WebsocketError::ResponseError {
            code: -2010,
            message: "insufficient balance".into(),
        });
        // Unwrapped root: must be detected
        assert!(
            classify_ws_order_error(&raw, &instrument).is_some(),
            "unwrapped ResponseError at root must be detected"
        );
        // Context-wrapped: anyhow::downcast_ref searches the full chain, so this is also detected
        let wrapped = raw.context("outer context (e.g. SDK adds context layer)");
        assert!(
            classify_ws_order_error(&wrapped, &instrument).is_some(),
            "context-wrapped ResponseError must still be detected — anyhow::downcast_ref searches the full chain"
        );
    }

    /// A spot REST response reporting `x-mbx-used-weight-1m` at 90% of the default limit pauses
    /// later queries but not orders; under it, nothing pauses.
    #[tokio::test]
    async fn rest_used_weight_near_the_limit_pauses_queries() {
        for (used, pauses) in [(5_399, false), (5_400, true)] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/api/v3/openOrders"))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .insert_header("x-mbx-used-weight-1m", used.to_string())
                        .set_body_json(serde_json::json!([])),
                )
                .mount(&server)
                .await;
            let rest = Arc::new(SpotRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key("key")
                    .api_secret("secret")
                    .base_path(server.uri())
                    .build()
                    .unwrap(),
            ));
            let tracker = Arc::new(RateLimitTracker::new(WeightPool::Spot));

            fetch_all_open_orders(rest, Arc::clone(&tracker))
                .await
                .unwrap();

            let query_waits = tokio::time::timeout(
                Duration::from_millis(50),
                tracker.wait_if_blocked(RequestKind::Query),
            )
            .await
            .is_err();
            assert_eq!(query_waits, pauses, "used weight {used}");
            tokio::time::timeout(
                Duration::from_millis(50),
                tracker.wait_if_blocked(RequestKind::Order),
            )
            .await
            .expect("orders never wait on the pause");
        }
    }

    /// A spot client whose WS-API order session connects to `ws_url`.
    fn client_with_ws_api(ws_url: String) -> BinanceSpot {
        let mut client = <BinanceSpot as ExecutionClient>::new(BinanceSpotConfig::new(
            "key".into(),
            "secret".into(),
        ));
        client.ws_handle = SpotWsApi::from_config(
            ConfigurationWebsocketApi::builder()
                .api_key("key")
                .api_secret("secret")
                .ws_url(ws_url)
                .build()
                .unwrap(),
        );
        client
    }

    /// A cancel by client order ID for `instrument`.
    fn cancel_request(
        instrument: &InstrumentNameExchange,
    ) -> OrderRequestCancel<ExchangeId, &InstrumentNameExchange> {
        crate::order::OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::BinanceSpot,
                instrument,
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::new("cid"),
            },
            state: crate::order::request::RequestCancel { id: None },
        }
    }

    /// A local WebSocket server: every connection is handed to `serve`. Returns its `ws://` URL.
    async fn ws_server<F, Fut>(serve: F) -> String
    where
        F: Fn(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream));
            }
        });
        url
    }

    /// During a rate-limit cooldown a cancel with no WS-API session is not sent: it fails as a
    /// rate-limit rejection, without a handshake.
    #[tokio::test]
    async fn cancel_without_a_session_during_a_cooldown_is_rejected_unsent() {
        let client = <BinanceSpot as ExecutionClient>::new(BinanceSpotConfig::new(
            "key".into(),
            "secret".into(),
        ));
        client
            .rate_limiter
            .on_rate_limited(Some(Duration::from_secs(60)));
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        let response = client
            .cancel_order(cancel_request(&instrument))
            .await
            .expect("a cancel always answers");

        assert!(
            matches!(
                response.state,
                Err(UnindexedOrderError::Rejected(ApiError::RateLimit))
            ),
            "{:?}",
            response.state
        );
        assert!(
            client.ws_api.read().await.is_none(),
            "no session was opened"
        );
    }

    /// A WS-API handshake Binance refuses with 429 starts a cooldown, and the cancel that needed
    /// the session fails as a rate-limit rejection.
    #[tokio::test]
    async fn a_handshake_refused_with_429_starts_a_cooldown() {
        use tokio_tungstenite::tungstenite::http;

        let url = ws_server(|stream| async move {
            // tungstenite's handshake callback fixes the error type to a whole HTTP response.
            #[allow(clippy::result_large_err)]
            let refuse = |_: &http::Request<()>, _| {
                Err(http::Response::builder()
                    .status(http::StatusCode::TOO_MANY_REQUESTS)
                    .body(None)
                    .unwrap())
            };
            let _ = tokio_tungstenite::accept_hdr_async(stream, refuse).await;
        })
        .await;
        let client = client_with_ws_api(url);
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        let response = client
            .cancel_order(cancel_request(&instrument))
            .await
            .expect("a cancel always answers");

        assert!(
            matches!(
                response.state,
                Err(UnindexedOrderError::Rejected(ApiError::RateLimit))
            ),
            "{:?}",
            response.state
        );
        assert!(client.rate_limiter.is_blocked());
    }

    /// A WS-API cancel response carries the spot pool's weight limit and usage: the limit
    /// replaces the default, and usage at 90% of it pauses queries but not orders.
    #[tokio::test]
    async fn a_ws_api_response_near_the_weight_limit_pauses_queries() {
        use futures::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;

        let url = ws_server(|stream| async move {
            let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            while let Some(Ok(message)) = ws.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                let response = serde_json::json!({
                    "id": request["id"],
                    "status": 200,
                    "result": {
                        "symbol": "BTCUSDT",
                        "origClientOrderId": "cid",
                        "orderId": 7,
                        "transactTime": 1_700_000_000_000_i64,
                        "executedQty": "0",
                    },
                    "rateLimits": [{
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "MINUTE",
                        "intervalNum": 1,
                        "limit": 1_000,
                        "count": 950,
                    }],
                });
                if ws.send(Message::text(response.to_string())).await.is_err() {
                    return;
                }
            }
        })
        .await;
        let client = client_with_ws_api(url);
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        let response = client
            .cancel_order(cancel_request(&instrument))
            .await
            .expect("a cancel always answers");

        assert!(response.state.is_ok(), "{:?}", response.state);
        assert_eq!(client.rate_limiter.weight_limit(), 1_000);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                client.rate_limiter.wait_if_blocked(RequestKind::Query),
            )
            .await
            .is_err(),
            "queries pause"
        );
        tokio::time::timeout(
            Duration::from_millis(50),
            client.rate_limiter.wait_if_blocked(RequestKind::Order),
        )
        .await
        .expect("orders never wait on the pause");
    }

    /// Fill recovery does not wait for the pause near the weight limit, which could outlast its
    /// budget and lose the fills: with queries paused, it still reads the trades at once.
    #[tokio::test]
    async fn fill_recovery_does_not_wait_for_the_weight_pause() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            // One trade, so recovery also looks its order up: both reads run under the pause.
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "symbol": "BTCUSDT",
                    "id": 1,
                    "orderId": 7,
                    "price": "100",
                    "qty": "1",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": Utc::now().timestamp_millis(),
                    "isBuyer": true,
                    "isMaker": false,
                }])),
            )
            .expect(2)
            .mount(&server)
            .await;
        let rest = Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));
        let tracker = Arc::new(RateLimitTracker::new(WeightPool::Spot));
        tracker.throttle(Duration::from_secs(60));
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            Utc::now() - chrono::Duration::minutes(1),
            Utc::now(),
        );

        // Far under the 60 s pause set above, and ample for two local round trips.
        tokio::time::timeout(
            Duration::from_millis(800),
            recover_fills(
                &rest,
                &tracker,
                &mut unrecovered,
                &tx,
                &new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::BinanceSpot),
            ),
        )
        .await
        .expect("recovery does not wait for the pause");
    }

    /// A recovery that fails keeps its gap, closed at its own start; the retry reads exactly that
    /// span, forwards only the fill inside it (a later one arrived live), and clears it.
    #[tokio::test]
    async fn a_failed_recovery_keeps_its_closed_gap_for_the_retry() {
        let disconnect = Utc::now() - chrono::Duration::minutes(10);
        let reconnect = Utc::now() - chrono::Duration::minutes(5);
        let trade = |id: i64, time: DateTime<Utc>| {
            serde_json::json!({
                "symbol": "BTCUSDT", "id": id, "orderId": id, "price": "100", "qty": "1",
                "commission": "0", "commissionAsset": "USDT", "time": time.timestamp_millis(),
                "isBuyer": true, "isMaker": false, "isBestMatch": true,
            })
        };
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1100, "msg": "rejected"})),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    trade(1, disconnect + chrono::Duration::minutes(1)),
                    trade(2, Utc::now() - chrono::Duration::minutes(1)),
                ])),
            )
            .mount(&server)
            .await;
        let rest = Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));
        let tracker = Arc::new(RateLimitTracker::new(WeightPool::Spot));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let dedup = new_dedup_cache();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            disconnect,
            reconnect,
        );

        recover_fills(
            &rest,
            &tracker,
            &mut unrecovered,
            &tx,
            &dedup,
            &KnownLiveOrders::shared(ExchangeId::BinanceSpot),
        )
        .await;
        assert!(!unrecovered.is_empty(), "the failed gap is kept");
        assert!(rx.try_recv().is_err(), "nothing forwarded");
        assert!(
            unrecovered.due(tokio::time::Instant::now()).is_empty(),
            "and waits for its retry"
        );

        unrecovered.make_due();
        recover_fills(
            &rest,
            &tracker,
            &mut unrecovered,
            &tx,
            &dedup,
            &KnownLiveOrders::shared(ExchangeId::BinanceSpot),
        )
        .await;
        assert!(unrecovered.is_empty(), "the retry recovered it");
        let forwarded: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(
            forwarded.len(),
            1,
            "only the fill inside the gap: {forwarded:?}"
        );

        let start_times: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter_map(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "startTime")
                    .map(|(_, value)| value.into_owned())
            })
            .collect();
        let start = disconnect.timestamp_millis().to_string();
        assert_eq!(start_times, [start.clone(), start]);
    }

    /// A span walk reads on by id through a full page inside the span, and stops at the page that
    /// reaches past its end, without the executions after it.
    #[tokio::test]
    async fn a_span_walk_pages_by_id_and_stops_past_the_end() {
        let start = Utc::now().timestamp_millis() - 600_000;
        let end = start + 10_000;
        let trade = |id: i64, time: i64| {
            serde_json::json!({
                "symbol": "BTCUSDT", "id": id, "orderId": id, "price": "100", "qty": "1",
                "commission": "0", "commissionAsset": "USDT", "time": time,
                "isBuyer": true, "isMaker": false, "isBestMatch": true,
            })
        };
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            .and(wiremock::matchers::query_param("fromId", "1001"))
            // A full page that reaches past the end: only the end, not a short page, stops the walk.
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(
                    (1_001..=2_000)
                        .map(|id| trade(id, if id == 1_001 { end } else { end + 1 }))
                        .collect::<Vec<_>>(),
                ),
            )
            .with_priority(1)
            .mount(&server)
            .await;
        let full_page: Vec<_> = (1..=1_000).map(|id| trade(id, start + id)).collect();
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(full_page))
            .mount(&server)
            .await;
        let rest = Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));

        let read = paginate_my_trades(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &InstrumentNameExchange::new("BTCUSDT"),
            MyTradesFrom::Span { start, end },
            RequestKind::Query,
        )
        .await
        .unwrap();

        let ids: Vec<_> = read.iter().filter_map(|t| t.id).collect();
        assert_eq!(ids, (1..=1_001).collect::<Vec<_>>());
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    /// When recovery times out, a gap whose read started has failed and waits for its retry, but
    /// one never started (eight are read at a time) is not charged and stays due.
    #[tokio::test]
    async fn a_timed_out_recovery_charges_only_the_gaps_it_started() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([]))
                    .set_delay(Duration::from_secs(3_600)),
            )
            .mount(&server)
            .await;
        let rest = Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .timeout(3_600_000_u64)
                .retries(0_u32)
                .build()
                .unwrap(),
        ));
        let instruments: Vec<_> = (0..9)
            .map(|i| InstrumentNameExchange::new(format!("SYM{i}USDT")))
            .collect();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &instruments,
            Utc::now() - chrono::Duration::minutes(10),
            Utc::now(),
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        // Paused: once every read is waiting, the clock jumps to the recovery timeout.
        tokio::time::pause();

        recover_fills(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &mut unrecovered,
            &tx,
            &new_dedup_cache(),
            &KnownLiveOrders::shared(ExchangeId::BinanceSpot),
        )
        .await;

        let now = tokio::time::Instant::now();
        assert_eq!(
            unrecovered.due(now).len(),
            1,
            "the gap never started stays due"
        );
        assert_eq!(
            unrecovered
                .due(now + Duration::from_secs(crate::client::order_recovery::GAP_RETRY_BASE_SECS))
                .len(),
            9,
            "the eight started wait for their first retry"
        );
    }

    // -----------------------------------------------------------------------
    // How orders ended: convert_ended_order, fetch_ended_orders, recover_ended_orders
    // -----------------------------------------------------------------------

    /// An order row for client order id `cid` in `status`: 1 of 2 filled, for 105 quote.
    fn order_row(cid: &str, status: &str) -> serde_json::Value {
        serde_json::json!({
            "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": cid, "price": "100",
            "origQty": "2", "executedQty": "1", "cummulativeQuoteQty": "105", "status": status,
            "timeInForce": "GTC", "type": "LIMIT", "side": "BUY", "time": 1_700_000_000_000_i64,
            "updateTime": 1_700_000_060_000_i64,
        })
    }

    fn get_order_row(row: serde_json::Value) -> binance_sdk::spot::rest_api::GetOrderResponse {
        serde_json::from_value(row).unwrap()
    }

    fn spot_key(instrument: &str, cid: &str) -> UnindexedOrderKey {
        OrderKey::new(
            ExchangeId::BinanceSpot,
            InstrumentNameExchange::new(instrument),
            StrategyId::new("strategy"),
            ClientOrderId::new(cid),
        )
    }

    fn ended_state(
        key: &UnindexedOrderKey,
        row: serde_json::Value,
    ) -> Option<crate::order::state::UnindexedInactiveOrderState> {
        let order = convert_ended_order(&get_order_row(row), ExchangeId::BinanceSpot, key)?;
        assert_eq!(&order.key, key, "reported under the key asked");
        assert_eq!(order.quantity, Decimal::TWO);
        Some(order.state)
    }

    #[test]
    fn an_ended_order_row_says_how_the_order_ended() {
        use crate::order::state::{Expired, InactiveOrderState};
        let key = spot_key("BTCUSDT", "a");
        let ended = Utc.timestamp_millis_opt(1_700_000_060_000).unwrap();
        let id = OrderId::new("7");

        let mut filled = order_row("a", "FILLED");
        filled["executedQty"] = "2".into();
        filled["cummulativeQuoteQty"] = "210".into();
        assert_eq!(
            ended_state(&key, filled),
            Some(InactiveOrderState::FullyFilled(
                crate::order::state::Filled::new(
                    id.clone(),
                    ended,
                    Decimal::TWO,
                    Some(Decimal::from(105)),
                )
            ))
        );
        assert_eq!(
            ended_state(&key, order_row("a", "CANCELED")),
            Some(InactiveOrderState::Cancelled(Cancelled::new(
                id.clone(),
                ended,
                Decimal::ONE
            )))
        );
        for status in ["EXPIRED", "EXPIRED_IN_MATCH"] {
            assert_eq!(
                ended_state(&key, order_row("a", status)),
                Some(InactiveOrderState::Expired(Expired::new(
                    id.clone(),
                    ended,
                    Decimal::ONE
                ))),
                "{status}"
            );
        }
        assert!(matches!(
            ended_state(&key, order_row("a", "REJECTED")),
            Some(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(_)
            )))
        ));
    }

    #[test]
    fn a_live_or_unreadable_order_row_is_not_ended() {
        let key = spot_key("BTCUSDT", "a");
        for status in [
            "NEW",
            "PARTIALLY_FILLED",
            "PENDING_NEW",
            "PENDING_CANCEL",
            "SOMETHING_NEW",
        ] {
            assert_eq!(ended_state(&key, order_row("a", status)), None, "{status}");
        }
        let mut no_status = order_row("a", "FILLED");
        no_status["status"] = serde_json::Value::Null;
        assert_eq!(ended_state(&key, no_status), None);
        let mut no_side = order_row("a", "FILLED");
        no_side["side"] = serde_json::Value::Null;
        assert_eq!(ended_state(&key, no_side), None);
    }

    #[test]
    fn an_unknown_order_or_symbol_is_recognised_by_its_code() {
        use binance_sdk::common::errors::ConnectorError;
        let bad = |code| {
            anyhow::Error::new(ConnectorError::BadRequestError {
                msg: "no".into(),
                code: Some(code),
            })
        };
        assert!(is_unknown_order(&bad(-2013)));
        assert!(is_unknown_order(&bad(-1121)));
        assert!(!is_unknown_order(&bad(-1100)));
        assert!(is_unknown_order(&anyhow::Error::new(
            ConnectorError::NotFoundError {
                msg: "no".into(),
                code: Some(-2013),
            }
        )));
        assert!(!is_unknown_order(&anyhow::anyhow!("-2013 in text only")));
    }

    fn rest_at(server: &wiremock::MockServer) -> Arc<RestApi> {
        Arc::new(SpotRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ))
    }

    /// Answer `GET /api/v3/order` for client order id `cid` with `response`.
    async fn mount_order(
        server: &wiremock::MockServer,
        cid: &str,
        response: wiremock::ResponseTemplate,
    ) {
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/order"))
            .and(wiremock::matchers::query_param("origClientOrderId", cid))
            .respond_with(response)
            .mount(server)
            .await;
    }

    fn venue_error(code: i64) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(400)
            .set_body_json(serde_json::json!({"code": code, "msg": "refused"}))
    }

    fn row_response(cid: &str, status: &str) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200).set_body_json(order_row(cid, status))
    }

    /// The client order ids asked about with `GET /api/v3/order`.
    async fn looked_up(server: &wiremock::MockServer) -> Vec<String> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v3/order")
            .filter_map(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "origClientOrderId")
                    .map(|(_, value)| value.into_owned())
            })
            .collect()
    }

    #[tokio::test]
    async fn fetch_ended_orders_reports_each_ended_order_under_its_key_and_omits_the_rest() {
        let server = wiremock::MockServer::start().await;
        mount_order(&server, "filled", row_response("filled", "FILLED")).await;
        mount_order(&server, "cancelled", row_response("cancelled", "CANCELED")).await;
        mount_order(&server, "live", row_response("live", "PARTIALLY_FILLED")).await;
        mount_order(&server, "unknown", venue_error(-2013)).await;
        mount_order(&server, "no-symbol", venue_error(-1121)).await;
        let mut client = <BinanceSpot as ExecutionClient>::new(BinanceSpotConfig::new(
            "key".into(),
            "secret".into(),
        ));
        client.rest = rest_at(&server);
        let keys = ["filled", "cancelled", "live", "unknown"].map(|cid| spot_key("BTCUSDT", cid));

        let mut ended = client
            .fetch_ended_orders(&[keys.to_vec(), vec![spot_key("NOSUCH", "no-symbol")]].concat())
            .await
            .unwrap();

        ended.sort_by(|a, b| a.key.cid.cmp(&b.key.cid));
        let reported: Vec<_> = ended.iter().map(|order| &order.key).collect();
        assert_eq!(
            reported,
            [&keys[1], &keys[0]],
            "the strategy asked survives"
        );
        assert!(matches!(
            ended[0].state,
            crate::order::state::InactiveOrderState::Cancelled(_)
        ));
        assert!(matches!(
            ended[1].state,
            crate::order::state::InactiveOrderState::FullyFilled(_)
        ));
        assert!(client.fetch_ended_orders(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fetch_ended_orders_fails_when_any_lookup_fails() {
        let server = wiremock::MockServer::start().await;
        mount_order(&server, "cancelled", row_response("cancelled", "CANCELED")).await;
        mount_order(&server, "refused", venue_error(-1100)).await;
        let mut client = <BinanceSpot as ExecutionClient>::new(BinanceSpotConfig::new(
            "key".into(),
            "secret".into(),
        ));
        client.rest = rest_at(&server);

        let result = client
            .fetch_ended_orders(&[
                spot_key("BTCUSDT", "cancelled"),
                spot_key("BTCUSDT", "refused"),
            ])
            .await;

        assert!(
            matches!(
                result,
                Err(UnindexedClientError::Api(ApiError::RequestRejected(_)))
            ),
            "{result:?}"
        );
    }

    /// A venue whose BTCUSDT listing shows only `live`, and whose order lookups answer `gone` as
    /// cancelled after `delay` and `unknown` as unknown.
    async fn venue_after_a_disconnect(delay: Duration) -> wiremock::MockServer {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/openOrders"))
            .and(wiremock::matchers::query_param("symbol", "BTCUSDT"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([order_row("live", "NEW")])),
            )
            .mount(&server)
            .await;
        mount_order(
            &server,
            "gone",
            row_response("gone", "CANCELED").set_delay(delay),
        )
        .await;
        mount_order(&server, "unknown", venue_error(-2013)).await;
        server
    }

    /// `live`, `gone` and `unknown`, all held as live on BTCUSDT.
    fn held_orders() -> SharedKnownLiveOrders {
        let known = KnownLiveOrders::shared(ExchangeId::BinanceSpot);
        for (n, cid) in ["live", "gone", "unknown"].into_iter().enumerate() {
            known.lock().live(
                &spot_key("BTCUSDT", cid),
                Decimal::TWO,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new(n.to_string())),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );
        }
        known
    }

    fn btcusdt_unchecked() -> UncheckedOrders {
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
        unchecked
    }

    #[tokio::test]
    async fn a_reconnect_reports_how_an_unlisted_order_ended_and_forgets_an_unknown_one() {
        let server = venue_after_a_disconnect(Duration::ZERO).await;
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_spot_ended_orders(
            &rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &known,
            &mut unchecked,
            &UnrecoveredFills::default(),
            &tx,
        )
        .await;

        let sent: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let [event] = sent.as_slice() else {
            panic!("one order ended: {sent:?}");
        };
        let AccountEventKind::OrderSnapshot(snapshot) = &event.kind else {
            panic!("an order snapshot: {event:?}");
        };
        assert_eq!(snapshot.0.key.cid, ClientOrderId::new("gone"));
        assert_eq!(snapshot.0.key.strategy, StrategyId::unknown());
        assert!(matches!(
            snapshot.0.state,
            OrderState::Inactive(crate::order::state::InactiveOrderState::Cancelled(_))
        ));
        let mut asked = looked_up(&server).await;
        asked.sort();
        assert_eq!(
            asked,
            ["gone", "unknown"],
            "a listed order is not looked up"
        );
        let known = known.lock();
        assert!(known.contains(&ClientOrderId::new("live")));
        assert!(!known.contains(&ClientOrderId::new("gone")));
        assert!(
            !known.contains(&ClientOrderId::new("unknown")),
            "asking again cannot help"
        );
        assert!(unchecked.is_empty());
    }

    #[tokio::test]
    async fn an_order_the_stream_reports_ending_during_the_check_is_not_reported_again() {
        let server = venue_after_a_disconnect(Duration::from_millis(300)).await;
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (rest, tracker) = (
            rest_at(&server),
            Arc::new(RateLimitTracker::new(WeightPool::Spot)),
        );
        let unrecovered = UnrecoveredFills::default();

        tokio::join!(
            recover_spot_ended_orders(&rest, &tracker, &known, &mut unchecked, &unrecovered, &tx,),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                known.lock().ended(&ClientOrderId::new("gone"));
            }
        );

        assert!(rx.try_recv().is_err(), "the stream already reported it");
        assert!(unchecked.is_empty());
    }

    #[tokio::test]
    async fn an_instrument_with_unrecovered_fills_is_not_checked_yet() {
        let server = venue_after_a_disconnect(Duration::ZERO).await;
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            Utc::now() - chrono::Duration::minutes(1),
            Utc::now(),
        );
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_spot_ended_orders(
            &rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &known,
            &mut unchecked,
            &unrecovered,
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err());
        assert!(server.received_requests().await.unwrap().is_empty());
        assert!(!unchecked.is_empty(), "it waits for its fills");
        assert_eq!(unchecked.next_due(&unrecovered), None, "woken by the gap");
    }

    #[tokio::test]
    async fn a_failed_check_sends_nothing_and_waits_for_its_retry() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/openOrders"))
            .respond_with(venue_error(-1100))
            .mount(&server)
            .await;
        let server_rest = rest_at(&server);
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_spot_ended_orders(
            &server_rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &known,
            &mut unchecked,
            &UnrecoveredFills::default(),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err());
        assert!(!unchecked.is_empty());
        assert!(
            unchecked
                .next_due(&UnrecoveredFills::default())
                .is_some_and(|due| due > tokio::time::Instant::now()),
            "retried after a backoff"
        );
        assert!(
            known.lock().contains(&ClientOrderId::new("gone")),
            "still held"
        );
    }

    #[tokio::test]
    async fn an_unlisted_order_the_lookup_finds_live_stays_held() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/openOrders"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        for (cid, status) in [
            ("live", "NEW"),
            ("gone", "PENDING_CANCEL"),
            ("unknown", "WHAT"),
        ] {
            mount_order(&server, cid, row_response(cid, status)).await;
        }
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_spot_ended_orders(
            &rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &known,
            &mut unchecked,
            &UnrecoveredFills::default(),
            &tx,
        )
        .await;

        assert!(rx.try_recv().is_err(), "none has ended");
        let known = known.lock();
        for cid in ["live", "gone", "unknown"] {
            assert!(
                known.contains(&ClientOrderId::new(cid)),
                "{cid}: only an order Binance does not know is dropped"
            );
        }
        assert!(unchecked.is_empty());
    }

    #[tokio::test]
    async fn one_instrument_failing_does_not_hold_up_another() {
        let server = venue_after_a_disconnect(Duration::ZERO).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/openOrders"))
            .and(wiremock::matchers::query_param("symbol", "ETHUSDT"))
            .respond_with(venue_error(-1100))
            .mount(&server)
            .await;
        let known = held_orders();
        let eth = spot_key("ETHUSDT", "eth");
        known.lock().live(
            &eth,
            Decimal::TWO,
            &Open::new(
                VenueOrderId::Assigned(OrderId::new("9")),
                Utc::now(),
                Decimal::ZERO,
            ),
        );
        let mut unchecked = btcusdt_unchecked();
        unchecked.open([eth.instrument.clone()]);
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_spot_ended_orders(
            &rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
            &known,
            &mut unchecked,
            &UnrecoveredFills::default(),
            &tx,
        )
        .await;

        let sent: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(sent.len(), 1, "BTCUSDT's ended order: {sent:?}");
        assert!(!unchecked.contains(&InstrumentNameExchange::new("BTCUSDT")));
        assert!(
            unchecked.contains(&eth.instrument),
            "ETHUSDT waits for its retry"
        );
        assert!(known.lock().contains(&eth.cid));
    }

    #[tokio::test]
    async fn a_check_dropped_part_way_changes_nothing() {
        let server = venue_after_a_disconnect(Duration::from_millis(500)).await;
        let known = held_orders();
        let mut unchecked = btcusdt_unchecked();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let dropped = tokio::time::timeout(
            Duration::from_millis(100),
            recover_spot_ended_orders(
                &rest_at(&server),
                &Arc::new(RateLimitTracker::new(WeightPool::Spot)),
                &known,
                &mut unchecked,
                &UnrecoveredFills::default(),
                &tx,
            ),
        )
        .await;

        assert!(dropped.is_err(), "the lookup of `gone` was still waiting");
        assert!(rx.try_recv().is_err());
        assert!(known.lock().contains(&ClientOrderId::new("gone")));
        assert!(unchecked.contains(&InstrumentNameExchange::new("BTCUSDT")));
        assert!(
            unchecked
                .next_due(&UnrecoveredFills::default())
                .is_some_and(|due| due <= tokio::time::Instant::now()),
            "still due, not charged a failure"
        );
    }
}
