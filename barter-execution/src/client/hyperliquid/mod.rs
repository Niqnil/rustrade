//! Hyperliquid perpetual futures ExecutionClient implementation.
//!
//! Uses the official `hyperliquid_rust_sdk` crate for REST and WebSocket API access.
//! Gated behind the "hyperliquid" feature flag.
//!
//! # Authentication
//!
//! Hyperliquid uses EVM-based authentication (Ethereum private key + EIP-712 signatures)
//! instead of traditional API key/secret. The SDK handles all signing internally.
//!
//! # Architecture
//!
//! - REST (`InfoClient`): account_snapshot, fetch_balances, fetch_open_orders, fetch_trades
//! - REST (`ExchangeClient`): open_order, cancel_order
//! - WebSocket (`InfoClient` with `with_reconnect`): account_stream via UserFills + OrderUpdates subscriptions
//!
//! # Limitations
//!
//! - **SDK-managed reconnect**: WebSocket streams use `InfoClient::with_reconnect()` for automatic
//!   reconnection. REST clients (`InfoClient::new()`, `ExchangeClient::new()`) do not auto-reconnect.
//! - **Perpetuals only**: Spot trading is future work
//! - **Price precision**: Hyperliquid requires 5 significant figures for prices

pub mod config;
pub mod error;

use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::ExecutionClient,
    error::{ConnectivityError, OrderError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, Open, OrderState, UnindexedOrderState},
    },
    position::Position,
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::collection::snapshot::Snapshot;
use chrono::{DateTime, TimeZone, Utc};
use config::HyperliquidConfig;
use error::map_sdk_error;
use ethers::signers::Signer;
use futures::{Stream, StreamExt, stream::BoxStream};
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, InfoClient, Message, Subscription};
use rust_decimal::Decimal;
use smol_str::{SmolStr, format_smolstr};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{collections::HashSet, str::FromStr, sync::Arc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// USDC asset name on Hyperliquid (the only collateral asset for perps).
const USDC_ASSET: &str = "USDC";

/// Stream wrapper that cancels background tasks when dropped.
///
/// Ensures spawned WebSocket processing tasks are cleaned up when the consumer
/// drops the account stream, preventing task leaks.
struct CancelOnDropStream<S> {
    inner: S,
    cancel_token: CancellationToken,
}

impl<S: Stream + Unpin> Stream for CancelOnDropStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<S> Drop for CancelOnDropStream<S> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Hyperliquid perpetual futures execution client.
///
/// Wraps the official `hyperliquid_rust_sdk` to implement the `ExecutionClient` trait.
/// Supports perpetual futures trading on Hyperliquid DEX.
#[derive(Debug, Clone)]
pub struct HyperliquidClient {
    config: HyperliquidConfig,
    info_client: Arc<InfoClient>,
    exchange_client: Arc<ExchangeClient>,
}

impl HyperliquidClient {
    /// Create a new client asynchronously.
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
            "Created HyperliquidClient"
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

/// Parse a decimal string from SDK response, logging warnings on failure.
fn parse_decimal(value: &str, field: &str) -> Option<Decimal> {
    Decimal::from_str(value)
        .map_err(|e| warn!(%field, %value, %e, "Failed to parse decimal"))
        .ok()
}

/// Parse SDK side string to barter Side.
///
/// Hyperliquid API returns "B" for buy/long and "A" for sell/short (ask-side).
/// Extra variants included for defensive parsing of potential future API changes.
fn parse_side(side: &str) -> Option<Side> {
    match side {
        "B" | "b" | "BUY" | "Buy" | "buy" => Some(Side::Buy),
        "A" | "a" | "S" | "s" | "SELL" | "Sell" | "sell" => Some(Side::Sell),
        _ => {
            warn!(%side, "Unknown side string");
            None
        }
    }
}

/// Convert milliseconds timestamp to DateTime<Utc>.
///
/// Returns `None` for timestamps outside the representable range (year 292M+).
fn millis_to_datetime(millis: u64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(millis as i64).single()
}

/// Build instrument name from Hyperliquid coin name (e.g., "BTC" -> "BTC-USD-PERP").
fn coin_to_instrument(coin: &str) -> InstrumentNameExchange {
    InstrumentNameExchange::from(format_smolstr!("{}-USD-PERP", coin))
}

/// Extract coin name from instrument (e.g., "BTC-USD-PERP" -> "BTC").
///
/// Returns `String` because Hyperliquid SDK requires `String` for asset fields.
fn instrument_to_coin(instrument: &InstrumentNameExchange) -> String {
    let s = instrument.as_ref();
    // Expected format: "COIN-USD-PERP" or just "COIN"
    match s.split_once('-') {
        Some((coin, _)) => coin.to_string(),
        None => s.to_string(),
    }
}

/// Round a price to 5 significant figures (Hyperliquid requirement).
fn round_to_5_sig_figs(value: Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;

    if value.is_zero() {
        return 0.0;
    }

    let f = value.to_f64().unwrap_or(0.0);
    if f == 0.0 {
        return 0.0;
    }

    // Clamp magnitude to prevent overflow when casting to i32
    #[allow(clippy::cast_possible_truncation)]
    let magnitude = f.abs().log10().floor().clamp(-30.0, 30.0) as i32;
    let scale = 10_f64.powi(4 - magnitude);
    (f * scale).round() / scale
}

/// Map barter TimeInForce to Hyperliquid TIF string.
fn map_tif(tif: &TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GoodUntilCancelled { post_only: true } => "Alo",
        TimeInForce::GoodUntilCancelled { post_only: false } => "Gtc",
        TimeInForce::ImmediateOrCancel => "Ioc",
        TimeInForce::FillOrKill => "Ioc", // Hyperliquid doesn't have FOK, use IOC
        TimeInForce::GoodUntilEndOfDay => "Gtc", // No EOD on Hyperliquid
    }
}

/// Convert a ClientOrderId to SDK cloid format (UUID) if valid.
///
/// Returns `Some(Uuid)` if the cid is a valid UUID, `None` otherwise.
/// Non-UUID CIDs are logged at debug level since they're common in tests/examples.
fn cid_to_cloid(cid: &ClientOrderId) -> Option<Uuid> {
    match Uuid::parse_str(cid.0.as_str()) {
        Ok(uuid) => Some(uuid),
        Err(_) => {
            debug!(cid = %cid.0, "CID is not a valid UUID, cloid will be None");
            None
        }
    }
}

impl ExecutionClient for HyperliquidClient {
    const EXCHANGE: ExchangeId = ExchangeId::HyperliquidPerp;

    type Config = HyperliquidConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Creates a new Hyperliquid client synchronously.
    ///
    /// # Panics
    ///
    /// - If no Tokio runtime is available on the current thread
    /// - If called from within an async context (e.g., inside `async fn`, `spawn`, or `block_on`)
    /// - If SDK initialization fails (network error, invalid credentials)
    ///
    /// # Recommended Usage
    ///
    /// Use [`HyperliquidClient::connect`] instead — it's async-safe and returns `Result`.
    /// This method exists only for trait compliance; prefer `connect()` in all new code.
    fn new(config: Self::Config) -> Self {
        let base_url = if config.testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };

        // SDK initialization is async; block on it since ExecutionClient::new is sync.
        // WARNING: This will panic if called from within an async context.
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
            "Created HyperliquidClient"
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

        // Fetch user state (balances + positions) and open orders concurrently
        let (user_state, open_orders) = tokio::try_join!(
            async {
                self.info_client
                    .user_state(address)
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

        // Build balance from margin summary (USDC is the only collateral)
        let account_value =
            parse_decimal(&user_state.margin_summary.account_value, "account_value")
                .unwrap_or(Decimal::ZERO);
        let margin_used = parse_decimal(
            &user_state.margin_summary.total_margin_used,
            "total_margin_used",
        )
        .unwrap_or(Decimal::ZERO);

        // Free balance can go negative during liquidation (margin_used > account_value).
        // Clamp to zero since negative free balance has no meaningful interpretation.
        let free_balance = (account_value - margin_used).max(Decimal::ZERO);
        let balances = vec![AssetBalance::new(
            AssetNameExchange::from(USDC_ASSET),
            Balance::new(account_value, free_balance),
            now,
        )];

        // Build instrument filter if provided
        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        // Group open orders by instrument
        let mut orders_by_instrument: std::collections::HashMap<InstrumentNameExchange, Vec<_>> =
            std::collections::HashMap::with_capacity(open_orders.len());

        for order in &open_orders {
            let instrument = coin_to_instrument(&order.coin);
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
                    exchange: ExchangeId::HyperliquidPerp,
                    instrument: instrument.clone(),
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(order_id.clone()),
                },
                side,
                price,
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: crate::order::state::OrderState::active(Open {
                    id: OrderId(order_id),
                    time_exchange,
                    filled_quantity: Decimal::ZERO,
                }),
            };

            orders_by_instrument
                .entry(instrument)
                .or_default()
                .push(order_snapshot);
        }

        // Build positions from asset_positions
        let mut instrument_snapshots = Vec::new();
        for asset_pos in user_state.asset_positions {
            let pos = &asset_pos.position;
            let instrument = coin_to_instrument(&pos.coin);

            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let quantity = parse_decimal(&pos.szi, "szi").unwrap_or(Decimal::ZERO);
            let entry_price = pos
                .entry_px
                .as_ref()
                .and_then(|p| parse_decimal(p, "entry_px"));
            let unrealized_pnl = parse_decimal(&pos.unrealized_pnl, "unrealized_pnl");
            let margin_used = parse_decimal(&pos.margin_used, "margin_used");
            let liquidation_price = pos
                .liquidation_px
                .as_ref()
                .and_then(|p| parse_decimal(p, "liquidation_px"));
            let leverage = Some(Decimal::from(pos.leverage.value));

            let position = if quantity.is_zero() {
                None
            } else {
                Some(Position::new(
                    quantity,
                    entry_price,
                    unrealized_pnl,
                    margin_used,
                    liquidation_price,
                    leverage,
                    now,
                ))
            };

            let orders = orders_by_instrument.remove(&instrument).unwrap_or_default();

            instrument_snapshots.push(InstrumentAccountSnapshot {
                instrument,
                orders,
                position,
            });
        }

        // Add any instruments that have orders but no position
        for (instrument, orders) in orders_by_instrument {
            instrument_snapshots.push(InstrumentAccountSnapshot {
                instrument,
                orders,
                position: None,
            });
        }

        Ok(AccountSnapshot {
            exchange: ExchangeId::HyperliquidPerp,
            balances,
            instruments: instrument_snapshots,
        })
    }

    /// Returns a live stream of account events (fills, order updates).
    ///
    /// # Instrument filtering
    ///
    /// The `instruments` parameter is **ignored** — Hyperliquid's WebSocket API does not
    /// support per-instrument subscriptions for user events. All fills and order updates
    /// across all instruments are delivered. Consumers requiring instrument filtering
    /// must filter client-side.
    ///
    /// # Task lifecycle
    ///
    /// Spawns two background tasks (fills, orders) that are automatically cancelled
    /// when the returned stream is dropped. The `ws_client` is held by the orders task;
    /// when cancelled, both tasks exit and the WebSocket connection closes.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let user = self.wallet_h160();
        let base_url = self.base_url();

        // Create a dedicated InfoClient for WebSocket streaming.
        // Using with_reconnect() enables SDK-managed reconnection.
        let mut ws_client = InfoClient::with_reconnect(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(e.to_string()))?;

        // Create channels for subscriptions
        let (fills_tx, mut fills_rx) = mpsc::unbounded_channel::<Message>();
        let (orders_tx, mut orders_rx) = mpsc::unbounded_channel::<Message>();

        // Subscribe to user fills
        ws_client
            .subscribe(Subscription::UserFills { user }, fills_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("UserFills subscribe: {e}")))?;

        // Subscribe to order updates
        ws_client
            .subscribe(Subscription::OrderUpdates { user }, orders_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("OrderUpdates subscribe: {e}")))?;

        info!(%user, "Subscribed to Hyperliquid account stream");

        // Create output channel for merged events
        let (event_tx, event_rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();

        // CancellationToken ensures tasks exit when stream is dropped
        let cancel_token = CancellationToken::new();

        // Spawn task to process fills
        let fills_event_tx = event_tx.clone();
        let fills_cancel = cancel_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = fills_cancel.cancelled() => {
                        debug!("Fills task cancelled");
                        return;
                    }
                    msg = fills_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Fills receiver closed");
                            return;
                        };
                        match msg {
                            Message::UserFills(fills) => {
                                for fill in fills.data.fills {
                                    if let Some(event) = fill_to_account_event(&fill)
                                        && fills_event_tx.send(event).is_err()
                                    {
                                        debug!("Fills event channel closed");
                                        return;
                                    }
                                }
                            }
                            Message::NoData => {
                                warn!("UserFills WebSocket disconnected");
                            }
                            Message::HyperliquidError(e) => {
                                error!(%e, "UserFills WebSocket error");
                                let _ = fills_event_tx.send(AccountEvent::new(
                                    ExchangeId::HyperliquidPerp,
                                    AccountEventKind::StreamError(e),
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Spawn task to process order updates
        // NOTE: ws_client is moved here to keep the WebSocket alive. When this task
        // exits (via cancellation or channel close), the WebSocket connection closes,
        // which causes fills_rx to also close.
        let orders_event_tx = event_tx;
        let orders_cancel = cancel_token.clone();
        tokio::spawn(async move {
            let _ws_client = ws_client;

            loop {
                tokio::select! {
                    biased;
                    () = orders_cancel.cancelled() => {
                        debug!("Orders task cancelled");
                        return;
                    }
                    msg = orders_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Orders receiver closed");
                            return;
                        };
                        match msg {
                            Message::OrderUpdates(updates) => {
                                for update in updates.data {
                                    if let Some(event) = order_update_to_account_event(&update)
                                        && orders_event_tx.send(event).is_err()
                                    {
                                        debug!("Orders event channel closed");
                                        return;
                                    }
                                }
                            }
                            Message::NoData => {
                                warn!("OrderUpdates WebSocket disconnected");
                            }
                            Message::HyperliquidError(e) => {
                                error!(%e, "OrderUpdates WebSocket error");
                                let _ = orders_event_tx.send(AccountEvent::new(
                                    ExchangeId::HyperliquidPerp,
                                    AccountEventKind::StreamError(e),
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Wrap stream with drop guard that cancels tasks
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(event_rx);
        let guarded_stream = CancelOnDropStream {
            inner: stream,
            cancel_token,
        };
        Ok(guarded_stream.boxed())
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        use crate::order::{request::OrderResponseCancel, state::Cancelled};
        use hyperliquid_rust_sdk::ClientCancelRequest;

        let coin = instrument_to_coin(request.key.instrument);

        // Get order ID from request
        let order_id = match &request.state.id {
            Some(id) => id,
            None => {
                warn!("Cancel request missing order ID");
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected("Missing order ID".to_string()),
                    )),
                });
            }
        };

        // Parse order ID to u64
        let oid: u64 = match order_id.0.parse() {
            Ok(id) => id,
            Err(e) => {
                warn!(?order_id, %e, "Failed to parse order ID as u64");
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(format!("Invalid order ID: {e}")),
                    )),
                });
            }
        };

        let cancel_request = ClientCancelRequest { asset: coin, oid };

        use hyperliquid_rust_sdk::ExchangeResponseStatus;

        let response = match self.exchange_client.cancel(cancel_request, None).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Cancel order failed (transport)");
                return Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(error::map_order_error(e, request.key.instrument)),
                });
            }
        };

        match response {
            ExchangeResponseStatus::Ok(_) => {
                debug!("Cancel order accepted");
                // Hyperliquid cancel response doesn't include an exchange timestamp
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Ok(Cancelled::new(
                        order_id.clone(),
                        Utc::now(),
                        Decimal::ZERO, // Cancel response doesn't include filled quantity
                    )),
                })
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Cancel rejected by exchange");
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
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
            ClientLimit, ClientOrder, ClientOrderRequest, ExchangeDataStatus,
            ExchangeResponseStatus,
        };

        let coin = instrument_to_coin(request.key.instrument);
        let is_buy = request.state.side == Side::Buy;

        // Round price and quantity to 5 significant figures
        let limit_px = round_to_5_sig_figs(request.state.price);
        let sz = round_to_5_sig_figs(request.state.quantity);

        // Map time-in-force (warn if FOK is substituted with IOC)
        if matches!(request.state.time_in_force, TimeInForce::FillOrKill) {
            warn!(
                instrument = %request.key.instrument,
                "FillOrKill not supported by Hyperliquid, using ImmediateOrCancel (may result in partial fills)"
            );
        }
        let tif = map_tif(&request.state.time_in_force).to_string();

        // Build order request
        // Pass cloid if cid is a valid UUID (enables order correlation via client ID)
        let cloid = cid_to_cloid(&request.key.cid);
        let order_request = ClientOrderRequest {
            asset: coin,
            is_buy,
            reduce_only: request.state.reduce_only,
            limit_px,
            sz,
            cloid,
            order_type: ClientOrder::Limit(ClientLimit { tif }),
        };

        let response = match self.exchange_client.order(order_request, None).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Open order failed");
                return Some(Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(error::map_order_error(e, request.key.instrument)),
                });
            }
        };

        // Parse response
        let state = match response {
            ExchangeResponseStatus::Ok(exchange_resp) => {
                // Check status from response data
                let status = exchange_resp
                    .data
                    .and_then(|d| d.statuses.into_iter().next());

                match status {
                    Some(ExchangeDataStatus::Resting(resting)) => {
                        debug!(oid = resting.oid, "Order resting");
                        OrderState::active(Open {
                            id: OrderId(format_smolstr!("{}", resting.oid)),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Filled(filled)) => {
                        debug!(oid = filled.oid, avg_px = %filled.avg_px, "Order filled");
                        // Hyperliquid provides avg_px for filled orders
                        let avg_price = parse_decimal(&filled.avg_px, "avg_px");
                        OrderState::fully_filled(Filled::new(
                            OrderId(format_smolstr!("{}", filled.oid)),
                            Utc::now(),
                            parse_decimal(&filled.total_sz, "total_sz")
                                .unwrap_or(request.state.quantity),
                            avg_price,
                        ))
                    }
                    Some(ExchangeDataStatus::Error(msg)) => {
                        warn!(%msg, "Order rejected by exchange");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(msg),
                        ))
                    }
                    Some(ExchangeDataStatus::WaitingForFill)
                    | Some(ExchangeDataStatus::WaitingForTrigger) => {
                        // Trigger/conditional orders return no usable order ID.
                        // Reject since we can't track or cancel these orders.
                        warn!("Trigger/conditional orders not supported");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(
                                "trigger/conditional orders not supported".to_string(),
                            ),
                        ))
                    }
                    Some(ExchangeDataStatus::Success) | None => {
                        // Generic success without order ID — SDK didn't return structured data.
                        // This shouldn't happen for limit orders; reject to avoid silent failures.
                        warn!("Order accepted but no order ID returned");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(
                                "no order ID in response".to_string(),
                            ),
                        ))
                    }
                }
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Order rejected");
                OrderState::inactive(OrderError::Rejected(crate::error::ApiError::OrderRejected(
                    msg,
                )))
            }
        };

        Some(Order {
            key: OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
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

        let user_state = self
            .info_client
            .user_state(address)
            .await
            .map_err(map_sdk_error)?;

        let now = Utc::now();

        // Hyperliquid perps use USDC as the only collateral
        let account_value =
            parse_decimal(&user_state.margin_summary.account_value, "account_value")
                .unwrap_or(Decimal::ZERO);
        let margin_used = parse_decimal(
            &user_state.margin_summary.total_margin_used,
            "total_margin_used",
        )
        .unwrap_or(Decimal::ZERO);

        // Free balance can go negative during liquidation; clamp to zero
        let free_balance = (account_value - margin_used).max(Decimal::ZERO);
        Ok(vec![AssetBalance::new(
            AssetNameExchange::from(USDC_ASSET),
            Balance::new(account_value, free_balance),
            now,
        )])
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
            let instrument = coin_to_instrument(&order.coin);

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
                    exchange: ExchangeId::HyperliquidPerp,
                    instrument,
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(order_id.clone()),
                },
                side,
                price,
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Open {
                    id: OrderId(order_id),
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

        let fills = self
            .info_client
            .user_fills(address)
            .await
            .map_err(map_sdk_error)?;

        // Clamp to 0 for dates before epoch (shouldn't happen in practice)
        #[allow(clippy::cast_sign_loss)] // timestamp_millis >= 0 after max(0)
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

            let instrument = coin_to_instrument(&fill.coin);

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

            result.push(Trade {
                id: TradeId(SmolStr::new(&fill.hash)),
                order_id: OrderId(format_smolstr!("{}", fill.oid)),
                instrument,
                strategy: StrategyId::unknown(),
                time_exchange,
                side,
                price,
                quantity,
                fees: AssetFees {
                    asset: AssetNameExchange::from("USDC"),
                    fees: fee,
                    fees_quote: Some(fee),
                },
            });
        }

        Ok(result)
    }
}

/// Convert SDK TradeInfo (fill) to AccountEvent::Trade.
fn fill_to_account_event(fill: &hyperliquid_rust_sdk::TradeInfo) -> Option<UnindexedAccountEvent> {
    let side = parse_side(&fill.side)?;
    let price = parse_decimal(&fill.px, "fill.px")?;
    let quantity = parse_decimal(&fill.sz, "fill.sz")?;
    let fee = parse_decimal(&fill.fee, "fill.fee").unwrap_or(Decimal::ZERO);
    let time_exchange = millis_to_datetime(fill.time)?;
    let instrument = coin_to_instrument(&fill.coin);
    let order_id = OrderId(format_smolstr!("{}", fill.oid));

    let trade = Trade {
        id: TradeId(SmolStr::new(&fill.hash)),
        order_id,
        instrument,
        strategy: StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        fees: AssetFees {
            asset: AssetNameExchange::from("USDC"),
            fees: fee,
            fees_quote: Some(fee),
        },
    };

    Some(AccountEvent::new(
        ExchangeId::HyperliquidPerp,
        AccountEventKind::Trade(trade),
    ))
}

/// Convert SDK OrderUpdate to AccountEvent::OrderSnapshot.
fn order_update_to_account_event(
    update: &hyperliquid_rust_sdk::OrderUpdate,
) -> Option<UnindexedAccountEvent> {
    let order = &update.order;
    let side = parse_side(&order.side)?;
    let price = parse_decimal(&order.limit_px, "order.limit_px")?;
    let orig_sz = parse_decimal(&order.orig_sz, "order.orig_sz")?;
    let time_exchange = millis_to_datetime(update.status_timestamp)?;
    let instrument = coin_to_instrument(&order.coin);

    // Use cloid (client order ID) if available, fall back to OID
    let order_id_smol = format_smolstr!("{}", order.oid);
    let cid = order
        .cloid
        .as_deref()
        .map(|c| ClientOrderId::new(SmolStr::new(c)))
        .unwrap_or_else(|| ClientOrderId::new(order_id_smol.clone()));

    // Determine order state from status
    let state = match update.status.as_str() {
        "open" | "resting" => {
            let current_sz = parse_decimal(&order.sz, "order.sz")?;
            let filled_quantity = (orig_sz - current_sz).max(Decimal::ZERO);
            crate::order::state::OrderState::active(Open {
                id: OrderId(order_id_smol),
                time_exchange,
                filled_quantity,
            })
        }
        "filled" => crate::order::state::OrderState::fully_filled(Filled::new(
            OrderId(order_id_smol),
            time_exchange,
            orig_sz, // Fully filled means filled_quantity == orig_sz
            None,    // OrderUpdate doesn't include avg_price
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

    // SDK's OrderUpdate doesn't include original order type or TIF, so we default
    // to Limit/GTC. This is a known limitation — IOC/FOK orders will be misrepresented.
    let order_snapshot = Order {
        key: OrderKey {
            exchange: ExchangeId::HyperliquidPerp,
            instrument,
            strategy: StrategyId::unknown(),
            cid,
        },
        side,
        price,
        quantity: orig_sz,
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        state,
    };

    Some(AccountEvent::new(
        ExchangeId::HyperliquidPerp,
        AccountEventKind::OrderSnapshot(Snapshot(order_snapshot)),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_parse_decimal_valid() {
        assert_eq!(parse_decimal("123.456", "test"), Some(dec!(123.456)));
        assert_eq!(parse_decimal("0", "test"), Some(dec!(0)));
        assert_eq!(parse_decimal("-50.5", "test"), Some(dec!(-50.5)));
    }

    #[test]
    fn test_parse_decimal_invalid() {
        assert_eq!(parse_decimal("", "test"), None);
        assert_eq!(parse_decimal("abc", "test"), None);
        assert_eq!(parse_decimal("12.34.56", "test"), None);
    }

    #[test]
    fn test_parse_side() {
        assert_eq!(parse_side("B"), Some(Side::Buy));
        assert_eq!(parse_side("BUY"), Some(Side::Buy));
        assert_eq!(parse_side("buy"), Some(Side::Buy));
        assert_eq!(parse_side("A"), Some(Side::Sell));
        assert_eq!(parse_side("S"), Some(Side::Sell));
        assert_eq!(parse_side("SELL"), Some(Side::Sell));
        assert_eq!(parse_side("sell"), Some(Side::Sell));
        assert_eq!(parse_side("X"), None);
        assert_eq!(parse_side(""), None);
    }

    #[test]
    fn test_coin_to_instrument() {
        let inst = coin_to_instrument("BTC");
        assert_eq!(inst.as_ref(), "BTC-USD-PERP");

        let inst = coin_to_instrument("ETH");
        assert_eq!(inst.as_ref(), "ETH-USD-PERP");
    }

    #[test]
    fn test_instrument_to_coin() {
        let coin = instrument_to_coin(&InstrumentNameExchange::from("BTC-USD-PERP"));
        assert_eq!(coin, "BTC");

        let coin = instrument_to_coin(&InstrumentNameExchange::from("ETH-USD-PERP"));
        assert_eq!(coin, "ETH");

        // Just coin name without suffix
        let coin = instrument_to_coin(&InstrumentNameExchange::from("SOL"));
        assert_eq!(coin, "SOL");
    }

    #[test]
    fn test_round_to_5_sig_figs() {
        assert_eq!(round_to_5_sig_figs(dec!(0)), 0.0);
        assert_eq!(round_to_5_sig_figs(dec!(12345)), 12345.0);
        assert_eq!(round_to_5_sig_figs(dec!(123456)), 123460.0);
        assert_eq!(round_to_5_sig_figs(dec!(0.00012345)), 0.00012345);
        assert_eq!(round_to_5_sig_figs(dec!(0.000123456)), 0.00012346);
        assert_eq!(round_to_5_sig_figs(dec!(1.23456789)), 1.2346);
    }

    #[test]
    fn test_map_tif() {
        assert_eq!(
            map_tif(&TimeInForce::GoodUntilCancelled { post_only: false }),
            "Gtc"
        );
        assert_eq!(
            map_tif(&TimeInForce::GoodUntilCancelled { post_only: true }),
            "Alo"
        );
        assert_eq!(map_tif(&TimeInForce::ImmediateOrCancel), "Ioc");
        assert_eq!(map_tif(&TimeInForce::FillOrKill), "Ioc");
        assert_eq!(map_tif(&TimeInForce::GoodUntilEndOfDay), "Gtc");
    }

    #[test]
    fn test_millis_to_datetime() {
        let dt = millis_to_datetime(1714100000000).unwrap();
        assert_eq!(dt.timestamp_millis(), 1714100000000);

        // Zero timestamp (Unix epoch) is valid
        assert!(millis_to_datetime(0).is_some());
    }

    #[test]
    fn test_fill_to_account_event() {
        let fill_json = r#"{
            "coin": "BTC",
            "side": "B",
            "px": "65000.5",
            "sz": "0.1",
            "time": 1714100000000,
            "hash": "0xabc123",
            "startPosition": "0",
            "dir": "Open Long",
            "closedPnl": "0",
            "oid": 12345,
            "cloid": null,
            "crossed": false,
            "fee": "0.65",
            "feeToken": "USDC",
            "tid": 99999
        }"#;

        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();
        let event = fill_to_account_event(&fill).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidPerp);
        match event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.instrument.as_ref(), "BTC-USD-PERP");
                assert_eq!(trade.side, Side::Buy);
                assert_eq!(trade.price, dec!(65000.5));
                assert_eq!(trade.quantity, dec!(0.1));
                assert_eq!(trade.fees.fees, dec!(0.65));
            }
            _ => panic!("Expected Trade event"),
        }
    }

    #[test]
    fn test_fill_to_account_event_sell() {
        let fill_json = r#"{
            "coin": "ETH",
            "side": "A",
            "px": "3200",
            "sz": "1.5",
            "time": 1714100000000,
            "hash": "0xdef456",
            "startPosition": "1.5",
            "dir": "Close Long",
            "closedPnl": "150.0",
            "oid": 12346,
            "cloid": null,
            "crossed": true,
            "fee": "4.8",
            "feeToken": "USDC",
            "tid": 100000
        }"#;

        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();
        let event = fill_to_account_event(&fill).unwrap();

        match event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.instrument.as_ref(), "ETH-USD-PERP");
                assert_eq!(trade.side, Side::Sell);
                assert_eq!(trade.price, dec!(3200));
                assert_eq!(trade.quantity, dec!(1.5));
            }
            _ => panic!("Expected Trade event"),
        }
    }

    #[test]
    fn test_order_update_to_account_event_open() {
        let update_json = r#"{
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "64000",
                "sz": "0.5",
                "oid": 12345,
                "timestamp": 1714100000000,
                "origSz": "0.5",
                "cloid": null
            },
            "status": "open",
            "statusTimestamp": 1714100000000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidPerp);
        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.key.instrument.as_ref(), "BTC-USD-PERP");
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.price, dec!(64000));
                assert_eq!(order.quantity, dec!(0.5));
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Active(_)
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_to_account_event_filled() {
        let update_json = r#"{
            "order": {
                "coin": "ETH",
                "side": "A",
                "limitPx": "3250",
                "sz": "0",
                "oid": 12346,
                "timestamp": 1714100000000,
                "origSz": "2.0",
                "cloid": null
            },
            "status": "filled",
            "statusTimestamp": 1714100001000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update).unwrap();

        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.side, Side::Sell);
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Inactive(
                        crate::order::state::InactiveOrderState::FullyFilled(_)
                    )
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_to_account_event_cancelled() {
        let update_json = r#"{
            "order": {
                "coin": "SOL",
                "side": "B",
                "limitPx": "150",
                "sz": "10",
                "oid": 12347,
                "timestamp": 1714100000000,
                "origSz": "10",
                "cloid": null
            },
            "status": "canceled",
            "statusTimestamp": 1714100002000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update).unwrap();

        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.key.instrument.as_ref(), "SOL-USD-PERP");
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Inactive(
                        crate::order::state::InactiveOrderState::Cancelled(_)
                    )
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_unknown_status_returns_none() {
        let update_json = r#"{
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "64000",
                "sz": "0.5",
                "oid": 12345,
                "timestamp": 1714100000000,
                "origSz": "0.5",
                "cloid": null
            },
            "status": "unknown_status",
            "statusTimestamp": 1714100000000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update);
        assert!(event.is_none());
    }
}
