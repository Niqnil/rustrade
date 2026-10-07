//! Hyperliquid spot trading ExecutionClient implementation.
//!
//! Uses the same `hyperliquid_rust_sdk` clients as the perpetuals client, but with
//! spot-specific endpoints and data handling.
//!
//! # Differences from Perpetuals
//!
//! - **Coin naming**: Spot pairs are named `@{index}` (`"@107"` is HYPE/USDC), except
//!   `"PURR/USDC"`; perps by their asset (`"BTC"`). See [Spot coins](#spot-coins).
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
//! intermingled in the same stream. Events are filtered by the shape of the coin name, with
//! [`CoinKind::of`]: this client keeps only spot coins (`"@107"`, `"PURR/USDC"`).
//!
//! If a user has both perp and spot clients for the same wallet, events will be
//! duplicated. Consumers should use one client type per account, or deduplicate externally.
//!
//! # Spot coins
//!
//! Hyperliquid names every spot pair but PURR/USDC by its index (`"@107"`), which does not name
//! its assets. The client reads Hyperliquid's `spotMeta` when it is created, and fails to be
//! created if it cannot, and resolves each coin through it to the instrument `BASE-QUOTE-SPOT`,
//! named from the pair's tokens (`"@107"` is `HYPE-USDC-SPOT`, `"@207"` is `HYPE-USDT0-SPOT`).
//!
//! A spot coin missing from `spotMeta`, such as a pair listed after the client was created, makes
//! it read `spotMeta` again, at most once every ten seconds, each read abandoned after five. A coin still missing after that is:
//! - an error from [`ExecutionClient::account_snapshot`], [`ExecutionClient::fetch_open_orders`]
//!   and [`ExecutionClient::fetch_trades`], whose lists would otherwise be short with nothing to
//!   say so. This holds whichever instruments were asked for: the coin's instrument is unknown,
//!   so it cannot be told apart from them;
//! - left out of the account stream, logged once per stream and coin, with `error!` for fills
//!   and `warn!` for order updates. [`ExecutionClient::fetch_trades`] recovers a fill left out
//!   once the pair resolves.
//!
//! Orders are placed through the SDK's `ExchangeClient`, which reads `spotMeta` once, when it is
//! created, and is never refreshed. So a pair listed after the client was created can be reported
//! but not traded until the client is created again.
//!
//! # Conditional Orders (Stop, TakeProfit)
//!
//! See [`super`] module documentation for conditional order support details.
//! The same constraints apply to spot:
//! - Supported: `Stop`, `StopLimit`, `TakeProfit`, `TakeProfitLimit`
//! - Unsupported: `TrailingStop`, `TrailingStopLimit`, `Market`
//!
//! # Client order ids
//!
//! As for perpetuals, every order must be placed under a client id in
//! [`ClientOrderId::uuid()`](crate::order::id::ClientOrderId::uuid) form. See the
//! [`super`] module documentation for why.

use super::common::{
    CLOID_REQUIRED, CancelOnDropStream, OpenOrder, OpenOrderListing, UserFill, cid_to_cloid,
    instrument_to_spot_coin, map_tif, millis_to_datetime, open_order_to_order, open_orders,
    parse_decimal, parse_side, round_to_5_sig_figs, span_millis, spot_balances,
    spot_pair_to_instrument, user_fills_by_time,
};
use super::config::HyperliquidConfig;
use super::error::{map_order_error, map_sdk_error};
use super::order_recovery::{
    ReconnectWatch, fetch_order_record, listed_cids, lookup_from_record, remember_open,
    remember_snapshot, send_fills, send_observed, spawn_order_checks,
};
use super::spot_coins::{SpotCoins, spot_pair};
use crate::client::dedup::new_dedup_cache;
use crate::client::order_recovery::{
    KnownLiveOrders, OrderLookup, SharedKnownLiveOrders, fetch_ended_by_key,
};
use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::{ExecutionClient, OrderStatusClient},
    emit_stream_terminated,
    error::{
        ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce, UnindexedInactiveOrder, UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId, TradesRead},
};
use chrono::{DateTime, Utc};
use ethers::signers::Signer;
use fnv::FnvHashSet;
use futures::{StreamExt, stream::BoxStream};
use hyperliquid_rust_sdk::{ExchangeClient, InfoClient, Message, Subscription};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    hyperliquid::{CoinKind, SpotPair, SpotPairs},
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Hyperliquid spot trading execution client.
///
/// Wraps the official `hyperliquid_rust_sdk` to implement the `ExecutionClient` trait
/// for spot trading on Hyperliquid DEX.
#[derive(Debug, Clone)]
pub struct HyperliquidSpotClient {
    config: HyperliquidConfig,
    info_client: Arc<InfoClient>,
    exchange_client: Arc<ExchangeClient>,
    spot_coins: SpotCoins,
    /// The orders seen live and not yet seen end, which a reconnect asks about. Shared by every
    /// clone and every account stream, since an order placed through one ends on any of them.
    known_live: SharedKnownLiveOrders,
}

impl HyperliquidSpotClient {
    /// Create a new spot client asynchronously.
    ///
    /// Use this when calling from an async context (e.g., tokio tests).
    /// For sync contexts, use `ExecutionClient::new()`.
    ///
    /// # Errors
    ///
    /// [`ConnectivityError::Socket`] when the SDK clients cannot be created, or Hyperliquid's
    /// `spotMeta` cannot be read (see [Spot coins](self#spot-coins)).
    pub async fn connect(config: HyperliquidConfig) -> Result<Self, ConnectivityError> {
        let base_url = config.base_url();

        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(format!("InfoClient: {e}")))?;

        let wallet = config.wallet.clone();
        let exchange_client = ExchangeClient::new(None, wallet, Some(base_url), None, None)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("ExchangeClient: {e}")))?;

        let info_client = Arc::new(info_client);
        let spot_coins = SpotCoins::fetch(Arc::clone(&info_client))
            .await
            .map_err(|e| ConnectivityError::Socket(format!("spotMeta: {e}")))?;

        info!(
            network = ?config.network,
            wallet = %config.wallet_address_hex(),
            "Created HyperliquidSpotClient"
        );

        Ok(Self {
            config,
            info_client,
            exchange_client: Arc::new(exchange_client),
            spot_coins,
            known_live: KnownLiveOrders::shared(ExchangeId::HyperliquidSpot),
        })
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
    /// - If SDK initialization fails (network error, invalid credentials), or Hyperliquid's
    ///   `spotMeta` cannot be read
    ///
    /// # Recommended Usage
    ///
    /// Use [`HyperliquidSpotClient::connect`] instead — it's async-safe and returns `Result`.
    /// This method exists only for trait compliance; prefer `connect()` in all new code.
    fn new(config: Self::Config) -> Self {
        let base_url = config.base_url();

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

        let info_client = Arc::new(info_client);
        let spot_coins = handle.block_on(async {
            SpotCoins::fetch(Arc::clone(&info_client))
                .await
                .unwrap_or_else(|e| panic!("Failed to read Hyperliquid spotMeta: {e}"))
        });

        info!(
            network = ?config.network,
            wallet = %config.wallet_address_hex(),
            "Created HyperliquidSpotClient"
        );

        Self {
            config,
            info_client,
            exchange_client: Arc::new(exchange_client),
            spot_coins,
            known_live: KnownLiveOrders::shared(ExchangeId::HyperliquidSpot),
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
            open_orders(&self.info_client, address)
        )?;

        let now = Utc::now();
        let balances = spot_balances(&token_balances.balances, now).collect();

        let coins = open_orders.iter().map(|order| order.coin.as_str());
        let pairs = self.spot_coins.covering(coins.clone()).await;
        for coin in coins {
            spot_pair(&pairs, coin)?;
        }

        // Spot has no positions, so the open orders are the whole of each instrument's entry.
        let instrument_snapshots = OpenOrderListing::new(
            &open_orders,
            ExchangeId::HyperliquidSpot,
            instruments,
            |coin| pairs.get(coin).map(spot_pair_to_instrument),
        )
        .into_snapshots()
        .collect();

        let snapshot = AccountSnapshot {
            exchange: ExchangeId::HyperliquidSpot,
            balances,
            instruments: instrument_snapshots,
        };
        remember_snapshot(&self.known_live, &snapshot);
        Ok(snapshot)
    }

    /// Returns a live stream of account events (fills, order updates) for spot orders.
    ///
    /// # Instrument filtering
    ///
    /// The `instruments` parameter is **ignored** — Hyperliquid's WebSocket API does not
    /// support per-instrument subscriptions for user events. All fills and order updates
    /// are delivered, but we filter client-side to only emit spot events (see
    /// [Spot coins](self#spot-coins)).
    ///
    /// # Task lifecycle
    ///
    /// Spawns three background tasks (fills, orders, and the reconnect's order check) that are
    /// automatically cancelled when the returned stream is dropped.
    ///
    /// # Orders that ended while disconnected
    ///
    /// After each reconnect the stream reports how each order the client holds as live ended,
    /// where it did, exactly as [`HyperliquidClient`](super::HyperliquidClient)'s does; see its
    /// `account_stream`. The listing leaves out an order on a spot coin missing from `spotMeta`,
    /// which cannot be on an instrument the client holds an order on, but a lookup that finds an
    /// order on one, even after `spotMeta` is read again (see [Spot coins](self#spot-coins)),
    /// fails, and is retried.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let user = self.wallet_h160();
        let base_url = self.config.base_url();

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

        // How the orders held as live ended while the socket was down: checked after each
        // reconnect, once the fills snapshot that opens the resubscription has been sent on. Its
        // own token also stops it promptly when the stream terminates.
        let reconnected = Arc::new(Notify::new());
        let checks_cancel = cancel_token.child_token();
        let (list_client, list_coins) = (self.info_client.clone(), self.spot_coins.clone());
        let (lookup_client, lookup_coins) = (self.info_client.clone(), self.spot_coins.clone());
        spawn_order_checks(
            ExchangeId::HyperliquidSpot,
            self.known_live.clone(),
            reconnected.clone(),
            checks_cancel.clone(),
            event_tx.clone(),
            move |instruments| {
                spot_listed_cids(list_client.clone(), list_coins.clone(), user, instruments)
            },
            move |key| spot_order_lookup(lookup_client.clone(), lookup_coins.clone(), user, key),
        );

        // Spawn task to process fills (filtered to spot only)
        let fills_event_tx = event_tx.clone();
        let fills_cancel = cancel_token.clone();
        let fills_terminated = terminated.clone();
        let fills_coins = self.spot_coins.clone();
        let fills_checks_cancel = checks_cancel.clone();
        tokio::spawn(async move {
            // Spot coins already logged as missing from spotMeta, so each is logged once.
            let mut missing_coins = HashSet::new();
            let mut reconnects = ReconnectWatch::new(reconnected);
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
                            fills_checks_cancel.cancel();
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
                                let pairs = fills_coins
                                    .covering(fills.data.fills.iter().map(|fill| fill.coin.as_str()))
                                    .await;
                                let convert = |fill: &hyperliquid_rust_sdk::TradeInfo| {
                                    // Filter: only spot coins
                                    if CoinKind::of(&fill.coin) != CoinKind::Spot {
                                        return None;
                                    }
                                    let Some(pair) = pairs.get(&fill.coin) else {
                                        if missing_coins.insert(fill.coin.clone()) {
                                            error!(
                                                coin = %fill.coin,
                                                tid = fill.tid,
                                                "Hyperliquid spot fills on a coin not in \
                                                 spotMeta are left out (fetch_trades recovers \
                                                 them); logged once per stream"
                                            );
                                        }
                                        return None;
                                    };
                                    fill_to_account_event(fill, pair)
                                };
                                if !send_fills(
                                    &fills.data,
                                    convert,
                                    &fills_dedup,
                                    &fills_event_tx,
                                    &mut reconnects,
                                ) {
                                    debug!("Spot fills event channel closed");
                                    return;
                                }
                            }
                            Message::NoData => {
                                warn!("Spot UserFills WebSocket disconnected");
                                reconnects.dropped();
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
        let orders_coins = self.spot_coins.clone();
        let orders_known = self.known_live.clone();
        tokio::spawn(async move {
            let _ws_client = ws_client;
            // Spot coins already logged as missing from spotMeta, so each is logged once.
            let mut missing_coins = HashSet::new();

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
                            checks_cancel.cancel();
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
                                let pairs = orders_coins
                                    .covering(updates.data.iter().map(|update| update.order.coin.as_str()))
                                    .await;
                                for update in updates.data {
                                    // Filter: only spot coins
                                    let coin = update.order.coin.as_str();
                                    if CoinKind::of(coin) != CoinKind::Spot {
                                        continue;
                                    }
                                    let Some(pair) = pairs.get(coin) else {
                                        if missing_coins.insert(coin.to_owned()) {
                                            warn!(
                                                %coin,
                                                oid = update.order.oid,
                                                "Hyperliquid spot order updates on a coin not in \
                                                 spotMeta are left out; logged once per stream"
                                            );
                                        }
                                        continue;
                                    };
                                    if let Some(event) = order_update_to_account_event(&update, pair)
                                        && !send_observed(&orders_known, &orders_event_tx, event)
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
    ) -> UnindexedOrderResponseCancel {
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
                return OrderResponseCancel {
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
                };
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
            // the cloid it was placed with -- which placement derives from the client id the same
            // way, and refuses the order if it cannot.
            Some(VenueOrderId::ClientAssigned) => cid_to_cloid(&request.key.cid)
                .map(CancelMethod::ByCloid)
                .ok_or("order is addressable only by its client id, which is not a canonical UUID"),
            None => Err("order is still in flight; the venue has acknowledged nothing to cancel"),
        };

        let cancel_method = match cancel_method {
            Ok(method) => method,
            Err(reason) => {
                warn!(%reason, cid = %request.key.cid, "Cannot determine how to cancel order");
                return OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(reason.to_string()),
                    )),
                };
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
                debug!(%cloid, "Cancelling spot order by cloid (no oid assigned)");
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
                return OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(map_order_error(e, request.key.instrument)),
                };
            }
        };

        match response {
            ExchangeResponseStatus::Ok(_) => {
                debug!("Spot cancel order accepted");
                // Hyperliquid answers a cancel once the order has left the book.
                self.known_live.lock().ended(&request.key.cid);
                OrderResponseCancel {
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
                        // Nor does it carry the filled quantity.
                        None,
                    )),
                }
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Spot cancel rejected by exchange");
                OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidSpot,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(msg),
                    )),
                }
            }
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
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
                return make_rejected(format!(
                    "Invalid instrument format: {}",
                    request.key.instrument
                ));
            }
        };
        let is_buy = request.state.side == Side::Buy;

        match request.state.kind {
            OrderKind::Market => {
                return make_unsupported(
                    "Hyperliquid does not support market orders; use Limit with IOC time-in-force"
                        .to_string(),
                );
            }
            OrderKind::TrailingStop { .. } | OrderKind::TrailingStopLimit { .. } => {
                return make_unsupported(
                    "Hyperliquid does not support trailing stop orders".to_string(),
                );
            }
            OrderKind::Limit
            | OrderKind::Stop { .. }
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TakeProfitLimit { .. } => {}
        }

        // The venue reports every order under its cloid, so an order placed without one could
        // not be matched to the id the caller tracks it by. See the module docs.
        let Some(cloid) = cid_to_cloid(&request.key.cid) else {
            return make_rejected(CLOID_REQUIRED.to_string());
        };

        let limit_px = match request.state.kind {
            // Market triggers: SDK uses trigger_px as limit_px
            OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
                round_to_5_sig_figs(trigger_price)
            }
            _ => match request.state.price {
                Some(p) => round_to_5_sig_figs(p),
                None => {
                    return make_rejected(
                        "Hyperliquid requires limit price for Limit/StopLimit/TakeProfitLimit orders"
                            .to_string(),
                    );
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
            return make_rejected(format!("Spot order notional ${notional} below $10 minimum"));
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
            cloid: Some(cloid),
            order_type,
        };

        let response = match self.exchange_client.order(order_request, None).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Spot open order failed");
                return Order {
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
                };
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
                        // The venue answered without an oid, so the order's only handle is the
                        // cloid it was placed with. A standalone trigger order is answered
                        // `resting` with an oid; these statuses belong to grouped orders.
                        debug!(cid = %request.key.cid, "Spot order waiting under its cloid");
                        OrderState::active(Open {
                            id: VenueOrderId::ClientAssigned,
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
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

        let order = Order {
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
        };
        self.known_live
            .lock()
            .placed(&order.key, order.quantity, &order.state);
        order
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
        Ok(spot_balances(&token_balances.balances, now).collect())
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let address = self.wallet_h160();

        let open_orders = open_orders(&self.info_client, address).await?;
        let pairs = self
            .spot_coins
            .covering(open_orders.iter().map(|order| order.coin.as_str()))
            .await;

        let orders = spot_open_orders(&open_orders, &pairs, instruments)?;
        remember_open(&self.known_live, &orders);
        Ok(orders)
    }

    /// Reads the span with `userFillsByTime`, to its end within the call, so the read is always
    /// complete (`resume: None`). Hyperliquid keeps only each wallet's 10,000 most recent fills,
    /// so a span reaching further back is read only as far as those go.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        let Some((start_ms, end_ms)) = span_millis(start, end) else {
            return Ok(TradesRead::complete(Vec::new()));
        };
        let address = self.wallet_h160();

        let fills = user_fills_by_time(&self.info_client, address, start_ms, end_ms).await?;
        let pairs = self
            .spot_coins
            .covering(fills.iter().map(|fill| fill.coin.as_str()))
            .await;

        spot_trades(&fills, &pairs, start, end, instruments).map(TradesRead::complete)
    }
}

/// Looks each order up with `orderStatus` (weight 2), as
/// [`HyperliquidClient`](super::HyperliquidClient) does; see its implementation for how each
/// status is reported. An order on a spot coin missing from `spotMeta` even after it is read again
/// (see [Spot coins](self#spot-coins)) fails the call, since it cannot be told apart from an order
/// on the key's instrument.
///
/// The account stream runs the same lookup itself after a reconnect; see
/// [`account_stream`](ExecutionClient::account_stream).
impl OrderStatusClient for HyperliquidSpotClient {
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        let address = self.wallet_h160();
        fetch_ended_by_key(orders, |key| {
            spot_order_lookup(
                self.info_client.clone(),
                self.spot_coins.clone(),
                address,
                key,
            )
        })
        .await
    }
}

/// Look the order under `key` up with `orderStatus`, on the spot pair its coin names.
///
/// # Errors
///
/// When the lookup fails, or the order is on a spot coin missing from `spotMeta`.
async fn spot_order_lookup(
    info_client: Arc<InfoClient>,
    spot_coins: SpotCoins,
    address: ethers::types::H160,
    key: UnindexedOrderKey,
) -> Result<OrderLookup, UnindexedClientError> {
    let Some(record) = fetch_order_record(&info_client, address, &key.cid).await? else {
        return Ok(OrderLookup::Unknown);
    };
    let coin = record.order.coin.as_str();
    let pairs = spot_coins.covering([coin]).await;
    let instrument = spot_pair(&pairs, coin)?.map(spot_pair_to_instrument);
    Ok(lookup_from_record(key, &record, instrument))
}

/// The client order ids `openOrders` (weight 20) lists on the spot `instruments`, for a
/// reconnect's check of the orders held as live. An order on a spot coin missing from `spotMeta` is
/// left out (see `listed_cids`).
///
/// # Errors
///
/// When the listing fails.
async fn spot_listed_cids(
    info_client: Arc<InfoClient>,
    spot_coins: SpotCoins,
    address: ethers::types::H160,
    instruments: Vec<InstrumentNameExchange>,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let rows = open_orders(&info_client, address).await?;
    let pairs = spot_coins
        .covering(rows.iter().map(|row| row.coin.as_str()))
        .await;
    Ok(listed_cids(&rows, &instruments, |coin| {
        pairs.get(coin).map(spot_pair_to_instrument)
    }))
}

/// Convert the open-order rows on spot pairs, keeping those on `instruments` (every one when
/// empty). Rows on other markets are left out.
///
/// # Errors
///
/// When a row is on a spot coin missing from `pairs` (see [`spot_pair`]).
fn spot_open_orders(
    rows: &[OpenOrder],
    pairs: &SpotPairs,
    instruments: &[InstrumentNameExchange],
) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
    let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
        None
    } else {
        let mut set = HashSet::with_capacity(instruments.len());
        set.extend(instruments.iter().cloned());
        Some(set)
    };

    let mut orders = Vec::new();
    for order in rows {
        let Some(pair) = spot_pair(pairs, &order.coin)? else {
            continue;
        };
        let instrument = spot_pair_to_instrument(pair);
        if instrument_filter
            .as_ref()
            .is_some_and(|f| !f.contains(&instrument))
        {
            continue;
        }
        orders.extend(open_order_to_order(
            order,
            ExchangeId::HyperliquidSpot,
            instrument,
        ));
    }
    Ok(orders)
}

/// Convert the fills on spot pairs within `start..=end`, keeping those on `instruments` (every one
/// when empty). Fills on other markets, and fills that do not parse, are left out.
///
/// # Errors
///
/// When a fill is on a spot coin missing from `pairs` (see [`spot_pair`]).
fn spot_trades(
    fills: &[UserFill],
    pairs: &SpotPairs,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    instruments: &[InstrumentNameExchange],
) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
    let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
        None
    } else {
        let mut set = HashSet::with_capacity(instruments.len());
        set.extend(instruments.iter().cloned());
        Some(set)
    };

    let mut result = Vec::new();
    for fill in fills {
        let Some(pair) = spot_pair(pairs, &fill.coin)? else {
            continue;
        };
        let (base_asset, quote_asset) = (pair.base(), pair.quote());
        let instrument = spot_pair_to_instrument(pair);

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
        if !(start..=end).contains(&time_exchange) {
            continue;
        }

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

/// Convert SDK TradeInfo (fill) on `pair` to AccountEvent::Trade for spot.
fn fill_to_account_event(
    fill: &hyperliquid_rust_sdk::TradeInfo,
    pair: &SpotPair,
) -> Option<UnindexedAccountEvent> {
    let side = parse_side(&fill.side)?;
    let price = parse_decimal(&fill.px, "fill.px")?;
    let quantity = parse_decimal(&fill.sz, "fill.sz")?;
    let fee = parse_decimal(&fill.fee, "fill.fee").unwrap_or(Decimal::ZERO);
    let time_exchange = millis_to_datetime(fill.time)?;

    let quote_asset = pair.quote();
    let instrument = spot_pair_to_instrument(pair);
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

/// Convert SDK OrderUpdate on `pair` to AccountEvent::OrderSnapshot for spot.
fn order_update_to_account_event(
    update: &hyperliquid_rust_sdk::OrderUpdate,
    pair: &SpotPair,
) -> Option<UnindexedAccountEvent> {
    super::common::order_update_to_account_event(
        update,
        ExchangeId::HyperliquidSpot,
        spot_pair_to_instrument(pair),
    )
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::spot_coins::test_spot_pairs;
    use crate::client::dedup::{dedup_key_from_event, is_duplicate};

    mod order_recovery {
        use super::super::super::common::info_tests::info_client_against;
        use super::super::super::order_recovery::tests::{CID, CLOID, record_json, row};
        use super::super::super::spot_coins::TEST_SPOT_META;
        use super::*;
        use crate::order::state::InactiveOrderState;
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// A server answering `spotMeta` with [`TEST_SPOT_META`] and every other info request
        /// with `body`, and the spot coins read from it.
        async fn serve(body: serde_json::Value) -> (MockServer, Arc<InfoClient>, SpotCoins) {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/info"))
                .and(body_string_contains("spotMeta"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(TEST_SPOT_META, "application/json"),
                )
                .with_priority(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/info"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .with_priority(2)
                .mount(&server)
                .await;
            let client = Arc::new(info_client_against(server.uri()).await);
            let coins = SpotCoins::fetch(client.clone()).await.unwrap();
            (server, client, coins)
        }

        fn key() -> UnindexedOrderKey {
            OrderKey {
                exchange: ExchangeId::HyperliquidSpot,
                instrument: InstrumentNameExchange::new("HYPE-USDC-SPOT"),
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::new(CID),
            }
        }

        fn order_status(coin: &str) -> serde_json::Value {
            serde_json::json!({
                "status": "order",
                "order": record_json(coin, "canceled", "0.01", Some(CLOID)),
            })
        }

        #[tokio::test]
        async fn a_spot_lookup_names_the_pair_from_spot_meta() {
            let (_server, client, coins) = serve(order_status("@107")).await;

            let lookup = spot_order_lookup(client, coins, ethers::types::H160::zero(), key())
                .await
                .unwrap();

            let OrderLookup::Ended(order) = lookup else {
                panic!("expected an ended order, got {lookup:?}");
            };
            assert_eq!(order.key, key());
            assert!(matches!(order.state, InactiveOrderState::Cancelled(_)));
        }

        #[tokio::test]
        async fn a_spot_lookup_of_a_perp_order_is_unknown() {
            let (_server, client, coins) = serve(order_status("BTC")).await;

            let lookup = spot_order_lookup(client, coins, ethers::types::H160::zero(), key())
                .await
                .unwrap();

            assert!(matches!(lookup, OrderLookup::Unknown));
        }

        #[tokio::test]
        async fn a_spot_lookup_fails_on_a_coin_missing_from_spot_meta() {
            let (_server, client, coins) = serve(order_status("@999")).await;

            let lookup = spot_order_lookup(client, coins, ethers::types::H160::zero(), key()).await;

            assert!(lookup.is_err());
        }

        #[tokio::test]
        async fn a_spot_listing_names_only_the_orders_on_the_pairs_asked_about() {
            let rows = serde_json::json!([
                row("@107", 1, Some(CLOID)),
                row("@207", 2, None),
                row("BTC", 3, None),
            ]);
            let (_server, client, coins) = serve(rows).await;

            let listed = spot_listed_cids(
                client,
                coins,
                ethers::types::H160::zero(),
                vec![InstrumentNameExchange::new("HYPE-USDC-SPOT")],
            )
            .await
            .unwrap();

            assert_eq!(
                listed.into_iter().collect::<Vec<_>>(),
                [ClientOrderId::new(CID)]
            );
        }

        #[tokio::test]
        async fn a_spot_listing_leaves_out_a_coin_missing_from_spot_meta() {
            let rows = serde_json::json!([row("@999", 1, None), row("@107", 2, Some(CLOID))]);
            let (_server, client, coins) = serve(rows).await;

            let listed = spot_listed_cids(
                client,
                coins,
                ethers::types::H160::zero(),
                vec![InstrumentNameExchange::new("HYPE-USDC-SPOT")],
            )
            .await
            .unwrap();

            assert_eq!(
                listed.into_iter().collect::<Vec<_>>(),
                [ClientOrderId::new(CID)]
            );
        }
    }
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_integration::collection::snapshot::Snapshot;

    /// The pair `coin` names in [`test_spot_pairs`].
    fn pair(coin: &str) -> SpotPair {
        test_spot_pairs().get(coin).unwrap().clone()
    }

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
        let event = fill_to_account_event(&fill, &pair("PURR/USDC")).unwrap();

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
        let first =
            fill_to_account_event(&spot_sweep_fill(111, "0.05"), &pair("PURR/USDC")).unwrap();
        let second =
            fill_to_account_event(&spot_sweep_fill(222, "0.051"), &pair("PURR/USDC")).unwrap();

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
        let event =
            fill_to_account_event(&spot_sweep_fill(111, "0.05"), &pair("PURR/USDC")).unwrap();
        let replay =
            fill_to_account_event(&spot_sweep_fill(111, "0.05"), &pair("PURR/USDC")).unwrap();

        assert!(!is_duplicate(&cache, dedup_key_from_event(&event).unwrap()));
        assert!(is_duplicate(&cache, dedup_key_from_event(&replay).unwrap()));
        // And the sibling fill of the same sweep still gets through.
        let sibling =
            fill_to_account_event(&spot_sweep_fill(222, "0.051"), &pair("PURR/USDC")).unwrap();
        assert!(!is_duplicate(
            &cache,
            dedup_key_from_event(&sibling).unwrap()
        ));
    }

    #[test]
    fn test_order_update_to_account_event_spot() {
        let update_json = r#"{
            "order": {
                "coin": "@107",
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
        let event = order_update_to_account_event(&update, &pair("@107")).unwrap();

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
    /// A stream fill on `coin`, bought, with its fee in `fee_token`.
    fn stream_fill(coin: &str, fee_token: &str) -> hyperliquid_rust_sdk::TradeInfo {
        let json = format!(
            r#"{{
                "coin": "{coin}", "side": "B", "px": "25.5", "sz": "2",
                "time": 1714100000000, "hash": "0xindexed", "startPosition": "0",
                "dir": "Buy", "closedPnl": "0", "oid": 4343, "cloid": null,
                "crossed": true, "fee": "0.01", "feeToken": "{fee_token}", "tid": 333
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn a_fill_on_an_indexed_coin_is_named_from_its_tokens() {
        let event = fill_to_account_event(&stream_fill("@107", "HYPE"), &pair("@107")).unwrap();
        let AccountEventKind::Trade(trade) = event.kind else {
            panic!("Expected Trade event");
        };
        assert_eq!(trade.instrument.as_ref(), "HYPE-USDC-SPOT");
        assert_eq!(trade.fees.asset.as_ref(), "HYPE");
        assert_eq!(
            trade.fees.fees_quote, None,
            "a fee in the base asset is not in quote"
        );
    }

    #[test]
    fn a_fill_quoted_in_another_token_takes_its_quote_from_the_pair() {
        let event = fill_to_account_event(&stream_fill("@207", "USDT0"), &pair("@207")).unwrap();
        let AccountEventKind::Trade(trade) = event.kind else {
            panic!("Expected Trade event");
        };
        assert_eq!(trade.instrument.as_ref(), "HYPE-USDT0-SPOT");
        assert_eq!(
            trade.fees.fees_quote,
            Some(dec!(0.01)),
            "USDT0 is this pair's quote, not USDC"
        );
    }

    /// Open-order rows on a perpetual, an indexed spot coin, PURR/USDC and an outcome coin, in
    /// the `openOrders` response's shape. Values are synthetic.
    fn open_order_rows(spot_coin: &str) -> Vec<OpenOrder> {
        serde_json::from_value(serde_json::json!([
            {"coin": "BTC", "side": "B", "limitPx": "50000", "sz": "0.1", "oid": 1,
             "timestamp": 1714100000000u64},
            {"coin": spot_coin, "side": "B", "limitPx": "25.5", "sz": "2", "oid": 2,
             "timestamp": 1714100000000u64},
            {"coin": "PURR/USDC", "side": "A", "limitPx": "0.05", "sz": "100", "oid": 3,
             "timestamp": 1714100000000u64},
            {"coin": "#12", "side": "B", "limitPx": "0.5", "sz": "10", "oid": 4,
             "timestamp": 1714100000000u64}
        ]))
        .unwrap()
    }

    #[test]
    fn open_orders_on_indexed_coins_are_listed_and_other_markets_left_out() {
        let orders = spot_open_orders(&open_order_rows("@107"), &test_spot_pairs(), &[]).unwrap();

        let instruments: Vec<_> = orders.iter().map(|o| o.key.instrument.as_ref()).collect();
        assert_eq!(instruments, ["HYPE-USDC-SPOT", "PURR-USDC-SPOT"]);

        let hype_only = [InstrumentNameExchange::from("HYPE-USDC-SPOT")];
        let orders =
            spot_open_orders(&open_order_rows("@107"), &test_spot_pairs(), &hype_only).unwrap();
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn an_open_order_on_a_spot_coin_missing_from_spot_meta_fails_the_list() {
        let result = spot_open_orders(&open_order_rows("@999"), &test_spot_pairs(), &[]);
        assert!(
            matches!(&result, Err(UnindexedClientError::Internal(msg)) if msg.contains("@999")),
            "a list missing an order must not read as complete: {result:?}"
        );
    }

    /// `userFillsByTime` rows on a perpetual, an indexed spot coin and PURR/USDC, at
    /// [`fill_time`].
    fn user_fills(spot_coin: &str) -> Vec<UserFill> {
        serde_json::from_value(serde_json::json!([
            {"coin": "BTC", "side": "B", "px": "50000", "sz": "0.1", "time": 1714100000000u64,
             "oid": 1, "fee": "0.5", "feeToken": "USDC", "tid": 11},
            {"coin": spot_coin, "side": "B", "px": "25.5", "sz": "2", "time": 1714100000000u64,
             "oid": 2, "fee": "0.01", "feeToken": "HYPE", "tid": 12},
            {"coin": "PURR/USDC", "side": "A", "px": "0.05", "sz": "100",
             "time": 1714100000000u64, "oid": 3, "fee": "0.002", "tid": 13}
        ]))
        .unwrap()
    }

    fn fill_time() -> DateTime<Utc> {
        millis_to_datetime(1714100000000).unwrap()
    }

    #[test]
    fn trades_on_indexed_coins_are_read_with_assets_from_spot_meta() {
        let trades = spot_trades(
            &user_fills("@107"),
            &test_spot_pairs(),
            fill_time(),
            fill_time(),
            &[],
        )
        .unwrap();

        let read: Vec<_> = trades
            .iter()
            .map(|t| (t.instrument.as_ref(), t.fees.asset.as_ref()))
            .collect();
        assert_eq!(
            read,
            [("HYPE-USDC-SPOT", "HYPE"), ("PURR-USDC-SPOT", "USDC")],
            "the perpetual is left out; PURR/USDC's missing feeToken falls back to the quote, \
             since it sold"
        );
    }

    #[test]
    fn a_trade_on_a_spot_coin_missing_from_spot_meta_fails_the_read() {
        let result = spot_trades(
            &user_fills("@999"),
            &test_spot_pairs(),
            fill_time(),
            fill_time(),
            &[],
        );
        assert!(
            matches!(&result, Err(UnindexedClientError::Internal(msg)) if msg.contains("@999")),
            "a read missing a fill must not read as complete: {result:?}"
        );
    }
}
