//! Hyperliquid spot trading ExecutionClient implementation.
//!
//! Uses the same `hyperliquid_rust_sdk` clients as the perpetuals client, but with
//! spot-specific endpoints and data handling.
//!
//! # Differences from Perpetuals
//!
//! - **Coin naming**: Spot uses pair format `"PURR/USDC"` vs symbol-only `"BTC"` for perps
//! - **Asset indices**: Spot uses 10000+ (vs 0-9999 for perps)
//! - **Balances**: Uses `user_token_balances()` instead of `user_state()` margin summary
//! - **No positions**: Spot has no margin/leverage concepts
//! - **$10 minimum**: Spot orders require minimum $10 notional value
//!
//! # Unified Account Model
//!
//! Hyperliquid uses a unified account model:
//! - Spot balances count as perpetual collateral
//! - Separate wallets within the unified account (explicit transfer required)
//! - Perpetual liquidation cannot sweep spot holdings
//!
//! # WebSocket Events
//!
//! `UserFills` and `OrderUpdates` subscriptions deliver **both** spot and perp events,
//! intermingled in the same stream. Events are filtered by coin name format:
//! - Spot coins contain `/` (e.g., `"PURR/USDC"`)
//! - Perp coins are single symbols (e.g., `"BTC"`)
//!
//! If a user has both perp and spot clients for the same wallet, events will be
//! duplicated. Consumers should use one client type per account, or deduplicate externally.
//!
//! # Conditional Orders (Stop, TakeProfit)
//!
//! See [`super`] module documentation for conditional order support details.
//! The same constraints apply to spot:
//! - Supported: `Stop`, `StopLimit`, `TakeProfit`, `TakeProfitLimit`
//! - Unsupported: `TrailingStop`, `TrailingStopLimit`, `Market`
//! - UUID requirement: Trigger orders MUST use [`crate::order::id::ClientOrderId::uuid()`]

use super::common::{
    CancelOnDropStream, HYPERLIQUID_OPEN_ORDERS_COMPLETE, cid_to_cloid, instrument_to_spot_coin,
    is_spot_coin, map_tif, millis_to_datetime, parse_decimal, parse_side, round_to_5_sig_figs,
    spot_coin_to_instrument, user_fills,
};
use super::config::HyperliquidConfig;
use super::error::{map_order_error, map_sdk_error};
use crate::client::dedup::{dedup_key_from_event, is_duplicate, new_dedup_cache};
use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::ExecutionClient,
    emit_stream_terminated,
    error::{
        ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use chrono::{DateTime, Utc};
use ethers::signers::Signer;
use futures::{StreamExt, stream::BoxStream};
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, InfoClient, Message, Subscription};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use rustrade_integration::collection::snapshot::Snapshot;
use smol_str::{SmolStr, format_smolstr};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

/// Hyperliquid spot trading execution client.
///
/// Wraps the official `hyperliquid_rust_sdk` to implement the `ExecutionClient` trait
/// for spot trading on Hyperliquid DEX.
#[derive(Debug, Clone)]
pub struct HyperliquidSpotClient {
    config: HyperliquidConfig,
    info_client: Arc<InfoClient>,
    exchange_client: Arc<ExchangeClient>,
}

impl HyperliquidSpotClient {
    /// Create a new spot client asynchronously.
    ///
    /// Use this when calling from an async context (e.g., tokio tests).
    /// For sync contexts, use `ExecutionClient::new()`.
    pub async fn connect(config: HyperliquidConfig) -> Result<Self, ConnectivityError> {
        let base_url = if config.testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };

        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(format!("InfoClient: {e}")))?;

        let wallet = config.wallet.clone();
        let exchange_client = ExchangeClient::new(None, wallet, Some(base_url), None, None)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("ExchangeClient: {e}")))?;

        info!(
            testnet = config.testnet,
            wallet = %config.wallet_address_hex(),
            "Created HyperliquidSpotClient"
        );

        Ok(Self {
            config,
            info_client: Arc::new(info_client),
            exchange_client: Arc::new(exchange_client),
        })
    }

    /// Returns the base URL for the configured network (mainnet or testnet).
    fn base_url(&self) -> BaseUrl {
        if self.config.testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        }
    }

    /// Returns the wallet address as a hex string (for logging/debugging).
    pub fn wallet_address(&self) -> String {
        self.config.wallet_address_hex()
    }

    /// Returns the wallet address as ethers H160.
    fn wallet_h160(&self) -> ethers::types::H160 {
        self.config.wallet.address()
    }
}

impl ExecutionClient for HyperliquidSpotClient {
    const EXCHANGE: ExchangeId = ExchangeId::HyperliquidSpot;

    // Spot only — the perpetual counterpart is `HyperliquidClient`.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] =
        &[InstrumentKindDiscriminant::Spot];

    type Config = HyperliquidConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Creates a new Hyperliquid spot client synchronously.
    ///
    /// # Panics
    ///
    /// - If no Tokio runtime is available on the current thread
    /// - If called from within an async context (e.g., inside `async fn`, `spawn`, or `block_on`)
    /// - If SDK initialization fails (network error, invalid credentials)
    ///
    /// # Recommended Usage
    ///
    /// Use [`HyperliquidSpotClient::connect`] instead — it's async-safe and returns `Result`.
    /// This method exists only for trait compliance; prefer `connect()` in all new code.
    fn new(config: Self::Config) -> Self {
        let base_url = if config.testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };

        let handle = tokio::runtime::Handle::current();

        let info_client = handle.block_on(async {
            InfoClient::new(None, Some(base_url))
                .await
                .unwrap_or_else(|e| panic!("Failed to create Hyperliquid InfoClient: {e}"))
        });

        let wallet = config.wallet.clone();
        let exchange_client = handle.block_on(async {
            ExchangeClient::new(None, wallet, Some(base_url), None, None)
                .await
                .unwrap_or_else(|e| panic!("Failed to create Hyperliquid ExchangeClient: {e}"))
        });

        info!(
            testnet = config.testnet,
            wallet = %config.wallet_address_hex(),
            "Created HyperliquidSpotClient"
        );

        Self {
            config,
            info_client: Arc::new(info_client),
            exchange_client: Arc::new(exchange_client),
        }
    }

    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let address = self.wallet_h160();

        // Fetch spot token balances and open orders concurrently
        let (token_balances, open_orders) = tokio::try_join!(
            async {
                self.info_client
                    .user_token_balances(address)
                    .await
                    .map_err(map_sdk_error)
            },
            async {
                self.info_client
                    .open_orders(address)
                    .await
                    .map_err(map_sdk_error)
            }
        )?;

        let now = Utc::now();
        let balances = parse_token_balances(&token_balances.balances, now);

        // Build instrument filter if provided
        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        // Group open orders by instrument (filter to spot orders only)
        let mut orders_by_instrument: std::collections::HashMap<InstrumentNameExchange, Vec<_>> =
            std::collections::HashMap::new();

        for order in &open_orders {
            // Filter: only spot coins (contain '/')
            if !is_spot_coin(&order.coin) {
                continue;
            }

            let instrument = spot_coin_to_instrument(&order.coin);
            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let Some(side) = parse_side(&order.side) else {
                continue;
            };
            let Some(price) = parse_decimal(&order.limit_px, "limit_px") else {
                continue;
            };
            let Some(quantity) = parse_decimal(&order.sz, "sz") else {
                continue;
            };
            let Some(time_exchange) = millis_to_datetime(order.timestamp) else {
                warn!(
                    oid = order.oid,
                    timestamp = order.timestamp,
                    "Invalid order timestamp, skipping"
                );
                continue;
            };

            let order_id = format_smolstr!("{}", order.oid);
            let order_snapshot = Order {
                key: OrderKey {
                    exchange: ExchangeId::HyperliquidSpot,
                    instrument: instrument.clone(),
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(order_id.clone()),
                },
                side,
                price: Some(price),
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: crate::order::state::OrderState::active(Open {
                    id: VenueOrderId::Assigned(OrderId(order_id)),
                    time_exchange,
                    filled_quantity: Decimal::ZERO,
                }),
            };

            orders_by_instrument
                .entry(instrument)
                .or_default()
                .push(order_snapshot);
        }

        // Build instrument snapshots from orders (spot has no positions)
        let instrument_snapshots: Vec<_> = orders_by_instrument
            .into_iter()
            .map(|(instrument, orders)| InstrumentAccountSnapshot {
                instrument,
                orders,
                orders_complete: HYPERLIQUID_OPEN_ORDERS_COMPLETE,
                position: None, // Spot has no positions
                isolated: None,
            })
            .collect();

        Ok(AccountSnapshot {
            exchange: ExchangeId::HyperliquidSpot,
            balances,
            instruments: instrument_snapshots,
        })
    }

    /// Returns a live stream of account events (fills, order updates) for spot orders.
    ///
    /// # Instrument filtering
    ///
    /// The `instruments` parameter is **ignored** — Hyperliquid's WebSocket API does not
    /// support per-instrument subscriptions for user events. All fills and order updates
    /// are delivered, but we filter client-side to only emit spot events (coins with '/').
    ///
    /// # Task lifecycle
    ///
    /// Spawns two background tasks (fills, orders) that are automatically cancelled
    /// when the returned stream is dropped.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let user = self.wallet_h160();
        let base_url = self.base_url();

        let mut ws_client = InfoClient::with_reconnect(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(e.to_string()))?;

        let (fills_tx, mut fills_rx) = mpsc::unbounded_channel::<Message>();
        let (orders_tx, mut orders_rx) = mpsc::unbounded_channel::<Message>();

        ws_client
            .subscribe(Subscription::UserFills { user }, fills_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("UserFills subscribe: {e}")))?;

        ws_client
            .subscribe(Subscription::OrderUpdates { user }, orders_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("OrderUpdates subscribe: {e}")))?;

        info!(%user, "Subscribed to Hyperliquid spot account stream");

        let (event_tx, event_rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();
        let cancel_token = CancellationToken::new();

        // Both tasks share `event_tx` and both observe `recv() -> None` when the SDK gives up on the
        // socket, so guard the terminal emit with a shared flag — only the first non-cancellation
        // terminal sends `StreamTerminated`, avoiding a double-emit.
        let terminated = Arc::new(AtomicBool::new(false));

        // Scoped to this stream rather than to the client: a fresh `account_stream` opens a fresh
        // subscription, whose snapshot the new consumer has not seen. It must outlive a reconnect,
        // which it does -- the SDK reconnects underneath this task, not around it.
        let fills_dedup = new_dedup_cache();

        // Spawn task to process fills (filtered to spot only)
        let fills_event_tx = event_tx.clone();
        let fills_cancel = cancel_token.clone();
        let fills_terminated = terminated.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = fills_cancel.cancelled() => {
                        debug!("Spot fills task cancelled");
                        return;
                    }
                    msg = fills_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Spot fills receiver closed");
                            // SDK gave up on the stream (channel closed). Emit a single terminal
                            // StreamTerminated across both tasks (guarded by the shared flag).
                            if fills_terminated
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                            {
                                emit_stream_terminated(
                                    &fills_event_tx,
                                    ExchangeId::HyperliquidSpot,
                                    StreamTerminationReason::Error(
                                        "hyperliquid spot account stream closed".to_string(),
                                    ),
                                );
                            }
                            return;
                        };
                        match msg {
                            Message::UserFills(fills) => {
                                // Hyperliquid opens a `userFills` subscription with a snapshot of
                                // recent fills, and the SDK resubscribes on every reconnect. Each
                                // reconnect therefore redelivers fills already sent. A trade is a
                                // delta the consumer accumulates, so redelivering one double-counts
                                // filled quantity and fees -- hence the dedup cache.
                                //
                                // Order updates on the sibling task are deliberately NOT deduped:
                                // they are absolute state, so a replayed one is idempotent, while
                                // dropping one could strand the consumer on stale state.
                                for fill in fills.data.fills {
                                    // Filter: only spot coins
                                    if !is_spot_coin(&fill.coin) {
                                        continue;
                                    }
                                    let Some(event) = fill_to_account_event(&fill) else {
                                        continue;
                                    };
                                    if let Some(key) = dedup_key_from_event(&event)
                                        && is_duplicate(&fills_dedup, key)
                                    {
                                        trace!(
                                            tid = fill.tid,
                                            "Hyperliquid spot dedup: skipping fill already delivered"
                                        );
                                        continue;
                                    }
                                    if fills_event_tx.send(event).is_err() {
                                        debug!("Spot fills event channel closed");
                                        return;
                                    }
                                }
                            }
                            Message::NoData => {
                                warn!("Spot UserFills WebSocket disconnected");
                            }
                            Message::HyperliquidError(e) => {
                                // Transient, non-terminal: the loop continues. Log only — no
                                // in-band event (consumers took no action on the old StreamError).
                                error!(%e, "Spot UserFills WebSocket error");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Spawn task to process order updates (filtered to spot only)
        // NOTE: ws_client is moved here to keep the WebSocket alive. When this task
        // exits (via cancellation or channel close), the WebSocket connection closes,
        // which causes fills_rx to also close.
        let orders_event_tx = event_tx;
        let orders_cancel = cancel_token.clone();
        let orders_terminated = terminated;
        tokio::spawn(async move {
            let _ws_client = ws_client;

            loop {
                tokio::select! {
                    biased;
                    () = orders_cancel.cancelled() => {
                        debug!("Spot orders task cancelled");
                        return;
                    }
                    msg = orders_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Spot orders receiver closed");
                            // SDK gave up on the stream (channel closed). Emit a single terminal
                            // StreamTerminated across both tasks (guarded by the shared flag).
                            if orders_terminated
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                            {
                                emit_stream_terminated(
                                    &orders_event_tx,
                                    ExchangeId::HyperliquidSpot,
                                    StreamTerminationReason::Error(
                                        "hyperliquid spot account stream closed".to_string(),
                                    ),
                                );
                            }
                            return;
                        };
                        match msg {
                            Message::OrderUpdates(updates) => {
                                for update in updates.data {
                                    // Filter: only spot coins
                                    if !is_spot_coin(&update.order.coin) {
                                        continue;
                                    }
                                    if let Some(event) = order_update_to_account_event(&update)
                                        && orders_event_tx.send(event).is_err()
                                    {
                                        debug!("Spot orders event channel closed");
                                        return;
                                    }
                                }
                            }
                            Message::NoData => {
                                warn!("Spot OrderUpdates WebSocket disconnected");
                            }
                            Message::HyperliquidError(e) => {
                                // Transient, non-terminal: the loop continues. Log only — no
                                // in-band event (consumers took no action on the old StreamError).
                                error!(%e, "Spot OrderUpdates WebSocket error");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(event_rx);
        let guarded_stream = CancelOnDropStream::new(stream, cancel_token);
        Ok(guarded_stream.boxed())
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        use crate::order::{request::OrderResponseCancel, state::Cancelled};
        use hyperliquid_rust_sdk::{
            ClientCancelRequest, ClientCancelRequestCloid, ExchangeResponseStatus,
        };
        use uuid::Uuid;

        let coin = match instrument_to_spot_coin(request.key.instrument) {
            Some(c) => c,
            None => {
                warn!(
                    instrument = %request.key.instrument,
                    "Invalid spot instrument format (expected BASE-QUOTE-SPOT)"
                );
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(format!(
                            "Invalid instrument format: {}",
                            request.key.instrument
                        )),
                    )),
                });
            }
        };

        enum CancelMethod {
            ByOid(u64),
            ByCloid(Uuid),
        }

        // How the venue addresses the order decides which endpoint cancels it. This reads the
        // identifier's kind rather than inferring it from the string's shape: an oid and a cloid
        // are different identifiers that happen to both be text, and telling them apart by parsing
        // means a cloid which parses as a number gets cancelled as somebody else's oid.
        let cancel_method = match &request.state.id {
            Some(VenueOrderId::Assigned(id)) => {
                id.0.parse::<u64>()
                    .map(CancelMethod::ByOid)
                    .map_err(|_| "venue order id is not a numeric Hyperliquid oid")
            }
            // The venue accepted the order without assigning an oid, so the only handle on it is
            // the client id sent with it -- and `cancel_by_cloid` requires that to be a UUID. The
            // UUID guard at placement is what keeps this arm reachable.
            Some(VenueOrderId::ClientAssigned) => Uuid::parse_str(&request.key.cid.0)
                .map(CancelMethod::ByCloid)
                .map_err(|_| "order is addressable only by its client id, which is not a UUID"),
            None => Err("order is still in flight; the venue has acknowledged nothing to cancel"),
        };

        let cancel_method = match cancel_method {
            Ok(method) => method,
            Err(reason) => {
                warn!(%reason, cid = %request.key.cid, "Cannot determine how to cancel order");
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(reason.to_string()),
                    )),
                });
            }
        };

        // Terminal states keep a plain `OrderId`: nothing compares them for identity, so an order
        // the venue never named records the client id it was addressed by instead.
        let cancelled_id = match &request.state.id {
            Some(venue_id) => venue_id.or_client_id(&request.key.cid),
            None => OrderId(request.key.cid.0.clone()),
        };

        let response = match cancel_method {
            CancelMethod::ByOid(oid) => {
                debug!(oid, "Cancelling spot order by OID");
                let cancel_request = ClientCancelRequest { asset: coin, oid };
                self.exchange_client.cancel(cancel_request, None).await
            }
            CancelMethod::ByCloid(cloid) => {
                debug!(%cloid, "Cancelling spot order by cloid (trigger order)");
                let cancel_request = ClientCancelRequestCloid { asset: coin, cloid };
                self.exchange_client
                    .cancel_by_cloid(cancel_request, None)
                    .await
            }
        };

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Spot cancel order failed (transport)");
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(map_order_error(e, request.key.instrument)),
                });
            }
        };

        match response {
            ExchangeResponseStatus::Ok(_) => {
                debug!("Spot cancel order accepted");
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Ok(Cancelled::new(
                        cancelled_id.clone(),
                        // SDK cancel response omits server timestamp; use local clock.
                        Utc::now(),
                        Decimal::ZERO,
                    )),
                })
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Spot cancel rejected by exchange");
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(msg),
                    )),
                })
            }
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
        use hyperliquid_rust_sdk::{
            ClientLimit, ClientOrder, ClientOrderRequest, ClientTrigger, ExchangeDataStatus,
            ExchangeResponseStatus,
        };

        let make_rejected =
            |msg: String| -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
                Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(
                        crate::error::ApiError::OrderRejected(msg),
                    )),
                }
            };

        let make_unsupported =
            |msg: String| -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
                Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::UnsupportedOrderType(msg)),
                }
            };

        let coin = match instrument_to_spot_coin(request.key.instrument) {
            Some(c) => c,
            None => {
                warn!(
                    instrument = %request.key.instrument,
                    "Invalid spot instrument format (expected BASE-QUOTE-SPOT)"
                );
                return Some(make_rejected(format!(
                    "Invalid instrument format: {}",
                    request.key.instrument
                )));
            }
        };
        let is_buy = request.state.side == Side::Buy;

        match request.state.kind {
            OrderKind::Market => {
                return Some(make_unsupported(
                    "Hyperliquid does not support market orders; use Limit with IOC time-in-force"
                        .to_string(),
                ));
            }
            OrderKind::TrailingStop { .. } | OrderKind::TrailingStopLimit { .. } => {
                return Some(make_unsupported(
                    "Hyperliquid does not support trailing stop orders".to_string(),
                ));
            }
            OrderKind::Limit
            | OrderKind::Stop { .. }
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TakeProfitLimit { .. } => {}
        }

        let cloid = cid_to_cloid(&request.key.cid);
        let cloid_is_some = cloid.is_some();
        let is_trigger_order = matches!(
            request.state.kind,
            OrderKind::Stop { .. }
                | OrderKind::StopLimit { .. }
                | OrderKind::TakeProfit { .. }
                | OrderKind::TakeProfitLimit { .. }
        );
        if is_trigger_order && cloid.is_none() {
            return Some(make_rejected(
                "Trigger orders require UUID-format client order ID (use ClientOrderId::uuid()). \
                 Non-UUID IDs cannot be cancelled via cancel_by_cloid()."
                    .to_string(),
            ));
        }

        let limit_px = match request.state.kind {
            // Market triggers: SDK uses trigger_px as limit_px
            OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
                round_to_5_sig_figs(trigger_price)
            }
            _ => match request.state.price {
                Some(p) => round_to_5_sig_figs(p),
                None => {
                    return Some(make_rejected(
                        "Hyperliquid requires limit price for Limit/StopLimit/TakeProfitLimit orders"
                            .to_string(),
                    ));
                }
            },
        };

        let sz = round_to_5_sig_figs(request.state.quantity);

        // Hyperliquid spot requires minimum $10 notional value
        // For market triggers, use trigger_price for notional calculation
        let notional_price = match request.state.kind {
            OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
                trigger_price
            }
            _ => request.state.price.unwrap_or(Decimal::ZERO),
        };
        let notional = notional_price * request.state.quantity;
        if notional < Decimal::TEN {
            warn!(
                instrument = %request.key.instrument,
                %notional,
                "Spot order below $10 minimum notional value"
            );
            return Some(make_rejected(format!(
                "Spot order notional ${notional} below $10 minimum"
            )));
        }

        if matches!(request.state.time_in_force, TimeInForce::FillOrKill) {
            warn!(
                instrument = %request.key.instrument,
                "FillOrKill not supported by Hyperliquid, using ImmediateOrCancel (may result in partial fills)"
            );
        }
        let tif = map_tif(&request.state.time_in_force).to_string();

        // Build order_type based on OrderKind
        let order_type = match request.state.kind {
            OrderKind::Limit => ClientOrder::Limit(ClientLimit { tif }),
            OrderKind::Stop { trigger_price } => ClientOrder::Trigger(ClientTrigger {
                is_market: true,
                trigger_px: round_to_5_sig_figs(trigger_price),
                tpsl: "sl".to_string(),
            }),
            OrderKind::StopLimit { trigger_price } => ClientOrder::Trigger(ClientTrigger {
                is_market: false,
                trigger_px: round_to_5_sig_figs(trigger_price),
                tpsl: "sl".to_string(),
            }),
            OrderKind::TakeProfit { trigger_price } => ClientOrder::Trigger(ClientTrigger {
                is_market: true,
                trigger_px: round_to_5_sig_figs(trigger_price),
                tpsl: "tp".to_string(),
            }),
            OrderKind::TakeProfitLimit { trigger_price } => ClientOrder::Trigger(ClientTrigger {
                is_market: false,
                trigger_px: round_to_5_sig_figs(trigger_price),
                tpsl: "tp".to_string(),
            }),
            // Already rejected above
            OrderKind::Market
            | OrderKind::TrailingStop { .. }
            | OrderKind::TrailingStopLimit { .. } => {
                unreachable!("unsupported order kinds rejected earlier")
            }
        };

        let order_request = ClientOrderRequest {
            asset: coin,
            is_buy,
            reduce_only: request.state.reduce_only,
            limit_px,
            sz,
            cloid,
            order_type,
        };

        let response = match self.exchange_client.order(order_request, None).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Spot open order failed");
                return Some(Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(map_order_error(e, request.key.instrument)),
                });
            }
        };

        let state = match response {
            ExchangeResponseStatus::Ok(exchange_resp) => {
                let status = exchange_resp
                    .data
                    .and_then(|d| d.statuses.into_iter().next());

                match status {
                    Some(ExchangeDataStatus::Resting(resting)) => {
                        debug!(oid = resting.oid, "Spot order resting");
                        OrderState::active(Open {
                            id: VenueOrderId::Assigned(OrderId(format_smolstr!("{}", resting.oid))),
                            // SDK does not return exchange timestamp on order placement; use local clock.
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Filled(filled)) => {
                        debug!(oid = filled.oid, avg_px = %filled.avg_px, "Spot order filled");
                        let avg_price = parse_decimal(&filled.avg_px, "avg_px");
                        OrderState::fully_filled(Filled::new(
                            OrderId(format_smolstr!("{}", filled.oid)),
                            // SDK does not return exchange timestamp on order placement; use local clock.
                            Utc::now(),
                            parse_decimal(&filled.total_sz, "total_sz")
                                .unwrap_or(request.state.quantity),
                            avg_price,
                        ))
                    }
                    Some(ExchangeDataStatus::Error(msg)) => {
                        warn!(%msg, "Spot order rejected by exchange");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(msg),
                        ))
                    }
                    Some(
                        ExchangeDataStatus::WaitingForFill | ExchangeDataStatus::WaitingForTrigger,
                    ) => {
                        // Use cid as OrderId only if it's a valid UUID (required for
                        // cancel_by_cloid). Trigger orders pass the UUID guard above;
                        // limit IOC/FOK orders may have non-UUID cids — those become
                        // untrackable, so reject with an observable failure rather than
                        // returning an OrderId that cancel cannot parse.
                        if cloid_is_some {
                            debug!(cloid = %request.key.cid.0, "Spot order waiting (cloid trackable)");
                            OrderState::active(Open {
                                id: VenueOrderId::ClientAssigned,
                                time_exchange: Utc::now(),
                                filled_quantity: Decimal::ZERO,
                            })
                        } else {
                            warn!("Spot order accepted but cid is not UUID — untrackable");
                            OrderState::inactive(OrderError::Rejected(
                                crate::error::ApiError::OrderRejected(
                                    "order accepted but cid is not UUID; cannot track for cancel"
                                        .to_string(),
                                ),
                            ))
                        }
                    }
                    Some(ExchangeDataStatus::Success) | None => {
                        warn!("Spot order accepted but no order ID returned");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(
                                "no order ID in response".to_string(),
                            ),
                        ))
                    }
                }
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Spot order rejected");
                OrderState::inactive(OrderError::Rejected(crate::error::ApiError::OrderRejected(
                    msg,
                )))
            }
        };

        Some(Order {
            key: OrderKey {
                exchange: ExchangeId::HyperliquidSpot,
                instrument: request.key.instrument.clone(),
                strategy: request.key.strategy.clone(),
                cid: request.key.cid.clone(),
            },
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state,
        })
    }

    async fn fetch_balances(
        &self,
        _assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let address = self.wallet_h160();

        let token_balances = self
            .info_client
            .user_token_balances(address)
            .await
            .map_err(map_sdk_error)?;

        let now = Utc::now();
        Ok(parse_token_balances(&token_balances.balances, now))
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let address = self.wallet_h160();

        let open_orders = self
            .info_client
            .open_orders(address)
            .await
            .map_err(map_sdk_error)?;

        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        let mut result = Vec::new();
        for order in open_orders {
            // Filter: only spot coins
            if !is_spot_coin(&order.coin) {
                continue;
            }

            let instrument = spot_coin_to_instrument(&order.coin);

            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let Some(side) = parse_side(&order.side) else {
                continue;
            };
            let Some(price) = parse_decimal(&order.limit_px, "limit_px") else {
                continue;
            };
            let Some(quantity) = parse_decimal(&order.sz, "sz") else {
                continue;
            };
            let Some(time_exchange) = millis_to_datetime(order.timestamp) else {
                warn!(
                    oid = order.oid,
                    timestamp = order.timestamp,
                    "Invalid order timestamp, skipping"
                );
                continue;
            };

            let order_id = format_smolstr!("{}", order.oid);
            result.push(Order {
                key: OrderKey {
                    exchange: ExchangeId::HyperliquidSpot,
                    instrument,
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(order_id.clone()),
                },
                side,
                price: Some(price),
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Open {
                    id: VenueOrderId::Assigned(OrderId(order_id)),
                    time_exchange,
                    filled_quantity: Decimal::ZERO,
                },
            });
        }

        Ok(result)
    }

    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
        let address = self.wallet_h160();

        let fills = user_fills(&self.info_client, address).await?;

        // Safety: .max(0) ensures the value is non-negative before casting to u64.
        #[allow(clippy::cast_sign_loss)]
        let time_since_ms = time_since.timestamp_millis().max(0) as u64;

        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        let mut result = Vec::new();
        for fill in fills {
            // Filter by time
            if fill.time < time_since_ms {
                continue;
            }

            // Filter: only spot coins (must contain '/') and parse base/quote in one pass
            let Some((base_asset, quote_asset)) = fill.coin.split_once('/') else {
                continue;
            };

            let instrument = spot_coin_to_instrument(&fill.coin);

            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let Some(side) = parse_side(&fill.side) else {
                continue;
            };
            let Some(price) = parse_decimal(&fill.px, "px") else {
                continue;
            };
            let Some(quantity) = parse_decimal(&fill.sz, "sz") else {
                continue;
            };
            let fee = parse_decimal(&fill.fee, "fee").unwrap_or(Decimal::ZERO);

            let Some(time_exchange) = millis_to_datetime(fill.time) else {
                warn!(time = fill.time, "Invalid fill timestamp, skipping");
                continue;
            };

            // Prefer what the venue says the fee was charged in. The side-based rule below is
            // only a guess — spot buys usually pay in base and sells in quote, but a fee can be
            // charged in a third asset — and it is reachable only if `feeToken` is absent, which
            // has not been observed.
            let fee_asset = fill
                .fee_token
                .as_deref()
                .unwrap_or(if matches!(side, Side::Buy) {
                    base_asset
                } else {
                    quote_asset
                });

            result.push(Trade {
                id: TradeId(format_smolstr!("{}", fill.tid)),
                order_id: OrderId(format_smolstr!("{}", fill.oid)),
                instrument,
                strategy: StrategyId::unknown(),
                time_exchange,
                side,
                price,
                quantity,
                // `TradeInfo` carries no cumulative filled quantity; Hyperliquid reports order
                // state as its own `OrderUpdate` message.
                order_filled_quantity: None,
                fees: AssetFees {
                    asset: AssetNameExchange::from(fee_asset),
                    fees: fee,
                    // Only set quote-equivalent when the fee is actually denominated in
                    // the quote asset. The downstream indexer recomputes for base-asset fees.
                    //
                    // Case-insensitive, matching the stream path: `fee_asset` is now the venue's
                    // own `feeToken` rather than a substring of `coin`, so it is no longer equal
                    // to `quote_asset` byte-for-byte by construction.
                    fees_quote: if fee_asset.eq_ignore_ascii_case(quote_asset) {
                        Some(fee)
                    } else {
                        None
                    },
                },
            });
        }

        Ok(result)
    }
}

/// Parse spot token balances from SDK response.
fn parse_token_balances(
    balances: &[hyperliquid_rust_sdk::UserTokenBalance],
    now: DateTime<Utc>,
) -> Vec<AssetBalance<AssetNameExchange>> {
    balances
        .iter()
        .map(|bal| {
            let total = parse_decimal(&bal.total, "total").unwrap_or(Decimal::ZERO);
            let hold = parse_decimal(&bal.hold, "hold").unwrap_or(Decimal::ZERO);
            let free = (total - hold).max(Decimal::ZERO);

            AssetBalance::new(
                AssetNameExchange::from(bal.coin.as_str()),
                Balance::new(total, free),
                now,
            )
        })
        .collect()
}

/// Convert SDK TradeInfo (fill) to AccountEvent::Trade for spot.
fn fill_to_account_event(fill: &hyperliquid_rust_sdk::TradeInfo) -> Option<UnindexedAccountEvent> {
    let side = parse_side(&fill.side)?;
    let price = parse_decimal(&fill.px, "fill.px")?;
    let quantity = parse_decimal(&fill.sz, "fill.sz")?;
    let fee = parse_decimal(&fill.fee, "fill.fee").unwrap_or(Decimal::ZERO);
    let time_exchange = millis_to_datetime(fill.time)?;

    // Parse base/quote in one pass to avoid redundant string scan
    let (_base, quote_asset) = fill.coin.split_once('/').unwrap_or(("", "USDC"));
    let instrument = spot_coin_to_instrument(&fill.coin);
    let order_id = OrderId(format_smolstr!("{}", fill.oid));

    // SDK's `TradeInfo` exposes `fee_token` directly, which is the asset the fee is
    // denominated in (typically base asset for buys, quote asset for sells).
    let fee_asset = fill.fee_token.as_str();

    let trade = Trade {
        id: TradeId(format_smolstr!("{}", fill.tid)),
        order_id,
        instrument,
        strategy: StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        // `TradeInfo` carries no cumulative filled quantity, so the order's state must be learned
        // from an `OrderUpdate` -- which Hyperliquid sends as its own message.
        order_filled_quantity: None,
        fees: AssetFees {
            asset: AssetNameExchange::from(fee_asset),
            fees: fee,
            // Only set quote-equivalent when the fee is actually denominated in
            // the quote asset. The downstream indexer recomputes for base-asset fees.
            fees_quote: if fee_asset.eq_ignore_ascii_case(quote_asset) {
                Some(fee)
            } else {
                None
            },
        },
    };

    Some(AccountEvent::new(
        ExchangeId::HyperliquidSpot,
        AccountEventKind::Trade(trade),
    ))
}

/// Convert SDK OrderUpdate to AccountEvent::OrderSnapshot for spot.
fn order_update_to_account_event(
    update: &hyperliquid_rust_sdk::OrderUpdate,
) -> Option<UnindexedAccountEvent> {
    let order = &update.order;
    let side = parse_side(&order.side)?;
    let price = parse_decimal(&order.limit_px, "order.limit_px")?;
    let orig_sz = parse_decimal(&order.orig_sz, "order.orig_sz")?;
    let time_exchange = millis_to_datetime(update.status_timestamp)?;
    let instrument = spot_coin_to_instrument(&order.coin);

    let order_id_smol = format_smolstr!("{}", order.oid);
    let cid = order
        .cloid
        .as_deref()
        .map(|c| ClientOrderId::new(SmolStr::new(c)))
        .unwrap_or_else(|| ClientOrderId::new(order_id_smol.clone()));

    let state = match update.status.as_str() {
        "open" | "resting" => {
            let current_sz = parse_decimal(&order.sz, "order.sz")?;
            let filled_quantity = (orig_sz - current_sz).max(Decimal::ZERO);
            crate::order::state::OrderState::active(Open {
                id: VenueOrderId::Assigned(OrderId(order_id_smol)),
                time_exchange,
                filled_quantity,
            })
        }
        "filled" => crate::order::state::OrderState::fully_filled(Filled::new(
            OrderId(order_id_smol),
            time_exchange,
            orig_sz,
            None,
        )),
        "canceled" | "cancelled" => {
            let current_sz = parse_decimal(&order.sz, "order.sz")?;
            let filled_quantity = (orig_sz - current_sz).max(Decimal::ZERO);
            crate::order::state::OrderState::inactive(Cancelled::new(
                OrderId(order_id_smol),
                time_exchange,
                filled_quantity,
            ))
        }
        status => {
            warn!(%status, "Unknown order status");
            return None;
        }
    };

    let order_snapshot = Order {
        key: OrderKey {
            exchange: ExchangeId::HyperliquidSpot,
            instrument,
            strategy: StrategyId::unknown(),
            cid,
        },
        side,
        price: Some(price),
        quantity: orig_sz,
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        state,
    };

    Some(AccountEvent::new(
        ExchangeId::HyperliquidSpot,
        AccountEventKind::OrderSnapshot(Snapshot(order_snapshot)),
    ))
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_fill_to_account_event_spot() {
        let fill_json = r#"{
            "coin": "PURR/USDC",
            "side": "B",
            "px": "0.05",
            "sz": "1000",
            "time": 1714100000000,
            "hash": "0xspot123",
            "startPosition": "0",
            "dir": "Open Long",
            "closedPnl": "0",
            "oid": 12345,
            "cloid": null,
            "crossed": false,
            "fee": "0.025",
            "feeToken": "USDC",
            "tid": 99999
        }"#;

        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();
        let event = fill_to_account_event(&fill).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidSpot);
        match event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.instrument.as_ref(), "PURR-USDC-SPOT");
                assert_eq!(
                    trade.id.0, "99999",
                    "the id is the fill's tid, not its hash"
                );
                assert_eq!(trade.side, Side::Buy);
                assert_eq!(trade.price, dec!(0.05));
                assert_eq!(trade.quantity, dec!(1000));
                assert_eq!(trade.fees.fees, dec!(0.025));
                assert_eq!(trade.fees.asset.as_ref(), "USDC");
            }
            _ => panic!("Expected Trade event"),
        }
    }

    /// Build a spot `TradeInfo` sharing one transaction hash with its siblings.
    fn spot_sweep_fill(tid: u64, px: &str) -> hyperliquid_rust_sdk::TradeInfo {
        let json = format!(
            r#"{{
                "coin": "PURR/USDC", "side": "B", "px": "{px}", "sz": "1000",
                "time": 1714100000000, "hash": "0xonesweep", "startPosition": "0",
                "dir": "Open Long", "closedPnl": "0", "oid": 4242, "cloid": null,
                "crossed": true, "fee": "0.025", "feeToken": "USDC", "tid": {tid}
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn a_spot_trade_id_identifies_the_fill_not_the_transaction() {
        let first = fill_to_account_event(&spot_sweep_fill(111, "0.05")).unwrap();
        let second = fill_to_account_event(&spot_sweep_fill(222, "0.051")).unwrap();

        let (AccountEventKind::Trade(first), AccountEventKind::Trade(second)) =
            (first.kind, second.kind)
        else {
            panic!("Expected Trade events");
        };

        assert_eq!(first.order_id, second.order_id, "one order produced both");
        assert_ne!(first.id, second.id, "but they are distinct fills");
    }

    #[test]
    fn a_replayed_spot_fill_is_delivered_once() {
        let cache = new_dedup_cache();
        let event = fill_to_account_event(&spot_sweep_fill(111, "0.05")).unwrap();
        let replay = fill_to_account_event(&spot_sweep_fill(111, "0.05")).unwrap();

        assert!(!is_duplicate(&cache, dedup_key_from_event(&event).unwrap()));
        assert!(is_duplicate(&cache, dedup_key_from_event(&replay).unwrap()));
        // And the sibling fill of the same sweep still gets through.
        let sibling = fill_to_account_event(&spot_sweep_fill(222, "0.051")).unwrap();
        assert!(!is_duplicate(
            &cache,
            dedup_key_from_event(&sibling).unwrap()
        ));
    }

    #[test]
    fn test_order_update_to_account_event_spot() {
        let update_json = r#"{
            "order": {
                "coin": "HYPE/USDC",
                "side": "A",
                "limitPx": "25.5",
                "sz": "10",
                "oid": 12346,
                "timestamp": 1714100000000,
                "origSz": "10",
                "cloid": null
            },
            "status": "open",
            "statusTimestamp": 1714100000000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidSpot);
        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.key.instrument.as_ref(), "HYPE-USDC-SPOT");
                assert_eq!(order.side, Side::Sell);
                assert_eq!(order.price, Some(dec!(25.5)));
                assert_eq!(order.quantity, dec!(10));
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Active(_)
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }
}
