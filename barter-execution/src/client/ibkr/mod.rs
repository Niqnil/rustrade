//! Interactive Brokers ExecutionClient implementation.
//!
//! Uses the `ibapi` crate for IB TWS/Gateway connectivity. Supports equities,
//! futures, options, and forex.
//!
//! # Connection
//!
//! Requires TWS or IB Gateway running locally with API enabled:
//!
//! | Application | Live Port | Paper Port |
//! |-------------|-----------|------------|
//! | TWS         | 7496      | 7497       |
//! | IB Gateway  | 4001      | 4002       |
//!
//! Enable API in TWS/Gateway: Configure → API → Settings → Enable ActiveX and Socket Clients.
//! For order placement, uncheck "Read-Only API".
//!
//! # Architecture
//!
//! - Connection: TCP socket to TWS/Gateway
//! - Orders: Subscription-based events (OrderStatus, ExecutionData, CommissionReport)
//! - Account: Subscription-based position and balance updates
//!
//! # Limitations
//!
//! - **Order types**: Only Market and Limit supported (no Stop, Trailing, Algo)
//! - **TimeInForce**: No `post_only` (IB has no maker-only orders)
//! - **No auto-reconnect**: Caller responsibility per library philosophy
//!
//! # See Also
//!
//! - [IB API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/trader-workstation-api/)
//! - [`barter_data::exchange::ibkr`] for market data

pub mod account;
pub mod contract;
pub mod execution;
pub mod order;

use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, Snapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::ExecutionClient,
    error::{ApiError, ConnectivityError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{
            OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, UnindexedOrderResponseCancel,
        },
        state::{Cancelled, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use account::BalanceAggregator;
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    ibkr::ContractRegistry,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use execution::{ExecutionBuffer, parse_decimal_or_warn, parse_ib_side};
use futures::stream::BoxStream;
use ibapi::{accounts::types::AccountGroup, client::blocking::Client};
use order::{OrderContext, OrderIdMap, build_ib_order};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for the IBKR execution client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrConfig {
    /// TWS/Gateway host (e.g., "127.0.0.1")
    pub host: String,
    /// TWS/Gateway port (7496=TWS live, 7497=TWS paper, 4001=GW live, 4002=GW paper)
    pub port: u16,
    /// Client ID (must be unique per connection)
    pub client_id: i32,
    /// Account ID (e.g., "DU123456" for paper)
    pub account: String,
    /// Pre-configured contracts to register on startup
    #[serde(default)]
    pub contracts: Vec<ContractConfig>,
}

/// Pre-configured contract for startup registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractConfig {
    pub name: String,
    pub symbol: String,
    pub security_type: String,
    pub exchange: String,
    pub currency: String,
    #[serde(default)]
    pub last_trade_date: Option<String>,
    /// Strike price for options. Uses f64 because `ibapi::Contract.strike` is f64.
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default)]
    pub right: Option<String>,
}

impl ContractConfig {
    fn to_contract(&self) -> ibapi::contracts::Contract {
        match self.security_type.as_str() {
            "STK" => contract::stock_contract(&self.symbol, &self.exchange, &self.currency),
            "FUT" => contract::futures_contract(
                &self.symbol,
                self.last_trade_date.as_deref().unwrap_or(""),
                &self.exchange,
                &self.currency,
            ),
            "OPT" => contract::option_contract(
                &self.symbol,
                self.last_trade_date.as_deref().unwrap_or(""),
                self.strike.unwrap_or(0.0),
                self.right.as_deref().unwrap_or("C"),
                &self.exchange,
                &self.currency,
            ),
            "CASH" => contract::forex_contract(&self.symbol, &self.currency),
            other => {
                warn!(security_type = %other, symbol = %self.symbol, "Unknown security_type, defaulting to STK");
                contract::stock_contract(&self.symbol, &self.exchange, &self.currency)
            }
        }
    }
}

/// Interactive Brokers execution client.
///
/// # Clone Behavior
///
/// Cloning creates a shallow copy with shared `Arc` references to the underlying
/// IB connection, contract registry, order ID map, and execution buffer. All clones
/// share the same TWS/Gateway connection and state.
#[derive(Clone)]
pub struct IbkrClient {
    config: Arc<IbkrConfig>,
    client: Arc<Client>,
    contracts: ContractRegistry,
    order_ids: OrderIdMap,
    execution_buffer: ExecutionBuffer,
    next_order_id: Arc<Mutex<i32>>,
}

impl std::fmt::Debug for IbkrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbkrClient")
            .field("config", &self.config)
            .field("contracts_count", &self.contracts.len())
            .field("pending_orders", &self.order_ids.len())
            .finish()
    }
}

impl IbkrClient {
    /// Connect to TWS/Gateway and initialize the client (sync, blocking).
    ///
    /// # Errors
    ///
    /// Returns error if connection fails or contract resolution fails.
    pub fn connect_sync(config: IbkrConfig) -> Result<Self, UnindexedClientError> {
        let url = format!("{}:{}", config.host, config.port);
        info!(url = %url, client_id = config.client_id, "Connecting to IB");

        let client = Client::connect(&url, config.client_id).map_err(|e| {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
        })?;

        let next_id = client.next_order_id();

        let contracts = ContractRegistry::new();

        for contract_config in &config.contracts {
            let contract = contract_config.to_contract();
            let name = InstrumentNameExchange::from(contract_config.name.as_str());

            match client.contract_details(&contract) {
                Ok(details) => {
                    if let Some(detail) = details.into_iter().next() {
                        contracts.register(name.clone(), detail.contract.clone());
                        debug!(name = %name, con_id = detail.contract.contract_id, "Registered contract");
                    }
                }
                Err(e) => {
                    warn!(name = %name, error = %e, "Failed to resolve contract");
                }
            }
        }

        info!(
            contracts = contracts.len(),
            next_order_id = next_id,
            "Connected to IB"
        );

        Ok(Self {
            config: Arc::new(config),
            client: Arc::new(client),
            contracts,
            order_ids: OrderIdMap::new(),
            execution_buffer: ExecutionBuffer::new(),
            next_order_id: Arc::new(Mutex::new(next_id)),
        })
    }

    /// Get the next order ID and increment the counter.
    #[allow(clippy::expect_used)] // Panic is correct: i32::MAX orders means system is broken
    fn allocate_order_id(&self) -> i32 {
        let mut id = self.next_order_id.lock();
        let current = *id;
        *id = id
            .checked_add(1)
            .expect("order ID overflow: i32::MAX exceeded");
        current
    }

    /// Register a contract for an instrument.
    pub fn register_contract(
        &self,
        name: InstrumentNameExchange,
        contract: ibapi::contracts::Contract,
    ) {
        self.contracts.register(name, contract);
    }

    /// Get the contract registry.
    pub fn contract_registry(&self) -> &ContractRegistry {
        &self.contracts
    }

    /// Get the number of pending executions awaiting commission reports.
    ///
    /// Useful for monitoring whether commission reports are being received.
    /// A growing count may indicate IB connection issues or delayed reports.
    pub fn pending_execution_count(&self) -> usize {
        self.execution_buffer.pending_count()
    }

    /// Clear stale executions older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// Call this periodically to prevent unbounded memory growth if commission
    /// reports are delayed or lost. A reasonable interval is 5-10 minutes with
    /// a max_age of 1 hour.
    pub fn clear_stale_executions(&self, max_age: std::time::Duration) -> usize {
        self.execution_buffer.clear_stale(max_age)
    }

    /// Clear order ID mappings older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// # Why This Is Needed
    ///
    /// IB does not guarantee event ordering between `OrderStatus("Filled")` and
    /// `ExecutionData`/`CommissionReport`. For fast-filling orders (especially
    /// market orders), execution data may arrive AFTER the filled status — or
    /// the filled status may not arrive at all. Removing mappings on terminal
    /// status would cause data loss.
    ///
    /// Call this periodically alongside `clear_stale_executions()`. A reasonable
    /// interval is 5-10 minutes with a max_age of 1 hour.
    pub fn clear_stale_order_ids(&self, max_age: std::time::Duration) -> usize {
        self.order_ids.clear_stale(max_age)
    }
}

impl ExecutionClient for IbkrClient {
    const EXCHANGE: ExchangeId = ExchangeId::Ibkr;

    type Config = IbkrConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Create a new IBKR client by connecting to TWS/Gateway.
    ///
    /// # Blocking
    ///
    /// This method performs blocking TCP I/O and IB API handshake. If called
    /// from an async context, wrap in `tokio::task::spawn_blocking` or call
    /// from a dedicated thread.
    ///
    /// # Panics
    ///
    /// Panics if connection fails. The `ExecutionClient` trait doesn't allow
    /// `new()` to return `Result`. Use `IbkrClient::connect_sync()` directly
    /// for fallible construction.
    #[track_caller]
    fn new(config: Self::Config) -> Self {
        #[allow(clippy::expect_used)] // Trait signature doesn't allow Result
        Self::connect_sync(config).expect("failed to connect to IB")
    }

    /// Fetch account snapshot (balances and positions).
    ///
    /// # Limitations
    ///
    /// - The `orders` field in each `InstrumentAccountSnapshot` is always empty.
    ///   IB's positions endpoint returns position data only, not open orders.
    ///   Use `fetch_open_orders()` or `account_stream()` for order state.
    ///
    /// - Position quantity and average cost from IB are not carried in the
    ///   returned `InstrumentAccountSnapshot`. The struct only indicates which
    ///   instruments have positions, not the position sizes. This is a
    ///   limitation of the `InstrumentAccountSnapshot` type, not the IB API.
    ///
    /// # Timeout
    ///
    /// This method blocks on IB's position subscription until IB sends an
    /// end-of-data marker. If IB is stalled, this will block indefinitely.
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let balances = self.fetch_balances(assets).await?;

        let client = self.client.clone();
        let contracts = self.contracts.clone();
        // M-10 fix: Use HashSet for O(1) lookup
        let instruments_filter: HashSet<_> = instruments.iter().cloned().collect();

        let instrument_snapshots = tokio::task::spawn_blocking(move || {
            use ibapi::accounts::PositionUpdate;

            // ibapi::Error is unstructured — we cannot distinguish connection failures
            // (transient, should retry) from API errors (e.g., invalid request).
            // Mapped to Internal (non-transient) conservatively; the crypto repo wrapper
            // should implement reconnect logic based on connection state, not error type.
            let positions_sub = client
                .positions()
                .map_err(|e| UnindexedClientError::Internal(format!("positions: {e}")))?;

            let mut snapshots = Vec::new();
            let mut seen = HashSet::new();
            for pos_update in positions_sub {
                if let PositionUpdate::Position(pos) = pos_update
                    && let Some(instrument) = contracts.get_name_by_con_id(pos.contract.contract_id)
                    && (instruments_filter.is_empty() || instruments_filter.contains(&instrument))
                    && seen.insert(instrument.clone())
                {
                    snapshots.push(InstrumentAccountSnapshot {
                        instrument,
                        orders: Vec::new(),
                    });
                }
            }
            Ok::<_, UnindexedClientError>(snapshots)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))??;

        Ok(AccountSnapshot {
            exchange: ExchangeId::Ibkr,
            balances,
            instruments: instrument_snapshots,
        })
    }

    /// Stream account events (order updates, fills, commissions).
    ///
    /// # Thread Lifecycle
    ///
    /// Spawns a background thread to read from the blocking IB subscription.
    /// The thread terminates when:
    /// - The returned `BoxStream` is dropped (channel closes)
    /// - The IB subscription ends (disconnect)
    ///
    /// **Important:** If IB is stalled (no events flowing), the thread blocks on
    /// the iterator. Dropping the stream signals termination, but the thread won't
    /// observe it until the next IB event arrives. For graceful shutdown during
    /// stalls, the caller should disconnect the IB connection.
    ///
    /// # Duplicate Events
    ///
    /// Orders submitted via `open_order()` will emit `OrderSnapshot` events on
    /// this stream in addition to the response from `open_order()` itself. This
    /// is inherent to IB's API — both the per-order subscription and the global
    /// order update stream receive the same `OrderStatus` events. Callers should
    /// deduplicate if needed.
    ///
    /// # Filter Parameters
    ///
    /// The `assets` and `instruments` parameters are currently ignored. IB's
    /// `order_update_stream()` returns events for all orders on the account.
    /// Callers subscribing for a specific instrument will receive events for
    /// all instruments.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let client = self.client.clone();

        // M-8 fix: Wrap blocking subscription call in spawn_blocking
        // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
        let order_sub = tokio::task::spawn_blocking(move || client.order_update_stream())
            .await
            .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
            .map_err(|e| UnindexedClientError::Internal(format!("order updates: {e}")))?;

        let contracts_clone = self.contracts.clone();
        let order_ids_clone = self.order_ids.clone();
        let exec_buffer_clone = self.execution_buffer.clone();

        let (tx, rx) = mpsc::unbounded_channel();

        std::thread::Builder::new()
            .name("ibkr-order-stream".to_string())
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, OrderIdMap, etc.) remains usable. On panic the
                // thread exits, tx is dropped, and the stream closes — caller observes EOF.
                let result = catch_unwind(AssertUnwindSafe(|| {
                    use ibapi::orders::OrderUpdate;

                    for update in order_sub {
                        let event = match update {
                            OrderUpdate::OrderStatus(status) => {
                                let ib_id = status.order_id;
                                // Use single-lock method for terminal status to avoid read+write
                                let is_terminal =
                                    matches!(status.status.as_str(), "Cancelled" | "Inactive");

                                let lookup_result = if is_terminal {
                                    order_ids_clone.remove_and_get_context(ib_id)
                                } else {
                                    order_ids_clone.get_client_id_and_context(ib_id)
                                };

                                if let Some((client_id, ctx)) = lookup_result {
                                    let order = make_order_from_status(
                                        &status,
                                        client_id,
                                        &ctx,
                                        is_terminal,
                                    );
                                    Some(UnindexedAccountEvent {
                                        exchange: ExchangeId::Ibkr,
                                        kind: AccountEventKind::OrderSnapshot(Snapshot::new(order)),
                                    })
                                } else {
                                    debug!(ib_order_id = ib_id, "OrderStatus for unknown order ID");
                                    None
                                }
                            }
                            OrderUpdate::ExecutionData(exec) => {
                                let order_id = exec.execution.order_id;
                                let con_id = exec.contract.contract_id;

                                // Fail-fast: skip second lookup if first fails
                                let Some(client_id) = order_ids_clone.get_client_id(order_id)
                                else {
                                    debug!(
                                        ib_order_id = order_id,
                                        con_id, "ExecutionData for unknown order ID, dropping"
                                    );
                                    continue;
                                };
                                let Some(instrument) = contracts_clone.get_name_by_con_id(con_id)
                                else {
                                    debug!(
                                        ib_order_id = order_id,
                                        con_id, "ExecutionData for unknown contract ID, dropping"
                                    );
                                    continue;
                                };

                                exec_buffer_clone.add_execution(exec, instrument, client_id);
                                None
                            }
                            OrderUpdate::CommissionReport(report) => exec_buffer_clone
                                .complete_with_commission(&report)
                                .map(|trade| UnindexedAccountEvent {
                                    exchange: ExchangeId::Ibkr,
                                    kind: AccountEventKind::Trade(trade),
                                }),
                            _ => None,
                        };

                        if let Some(e) = event
                            && tx.send(e).is_err()
                        {
                            break;
                        }
                    }
                }));

                if let Err(panic_info) = result {
                    let msg = panic_info
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_info.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!("Order stream worker panicked: {msg}");
                    // Channel type is UnindexedAccountEvent, not Result — cannot send error.
                    // Caller observes stream close; they can check logs for panic message.
                }
            })
            .map_err(|e| UnindexedClientError::TaskFailed(format!("thread spawn: {e}")))?;

        Ok(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
    }

    /// Cancel an order.
    ///
    /// # Async Cancel Semantics
    ///
    /// Returns `Ok(Cancelled)` when the cancel request is **submitted**, not when
    /// the order is confirmed cancelled. The actual cancellation confirmation comes
    /// via `account_stream` as an `OrderStatus::Cancelled` event.
    ///
    /// The order ID mapping is retained until terminal status is received via
    /// `account_stream`, ensuring fill events arriving between cancel request and
    /// confirmation are not lost.
    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let ib_order_id = match self.order_ids.get_ib_id(&request.key.cid) {
            Some(id) => id,
            None => {
                return Some(OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        "order ID not found in map".to_string(),
                    ))),
                });
            }
        };

        let client = self.client.clone();

        let result =
            tokio::task::spawn_blocking(move || client.cancel_order(ib_order_id, "")).await;

        match result {
            Ok(Ok(_sub)) => {
                // H-3 fix: Do NOT remove order_ids here. The mapping is needed to
                // correlate any fill events that arrive between now and when IB
                // confirms the cancel. Removal happens in account_stream when
                // OrderStatus::Cancelled is received.
                Some(OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(
                        OrderId::new(format_smolstr!("{}", ib_order_id)),
                        Utc::now(),
                    )),
                })
            }
            Ok(Err(e)) => {
                error!(order_id = ib_order_id, error = %e, "Failed to cancel order");
                Some(OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                })
            }
            Err(e) => {
                error!(order_id = ib_order_id, error = %e, "Task join error");
                Some(OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                })
            }
        }
    }

    /// Submit an order to IB.
    ///
    /// # Cancellation Safety
    ///
    /// This future registers the order ID mapping before submitting to IB.
    /// If the future is cancelled (e.g., via `tokio::select!` timeout) after
    /// registration but before completion:
    /// - The order may still be submitted to IB
    /// - The order ID mapping will leak (not cleaned up)
    /// - Subsequent fills will be processed via `account_stream`
    ///
    /// Callers should avoid cancelling this future mid-flight. If timeout
    /// behavior is needed, prefer setting IB's native order timeout via
    /// `TimeInForce` instead.
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let key = OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let contract = match self.contracts.get_contract(request.key.instrument) {
            Some(c) => c,
            None => {
                return Some(Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Err(crate::error::OrderError::Rejected(
                        ApiError::InstrumentInvalid(
                            request.key.instrument.clone(),
                            "contract not registered".to_string(),
                        ),
                    )),
                });
            }
        };

        let quantity: f64 = match request.state.quantity.try_into() {
            Ok(q) => q,
            Err(_) => {
                return Some(Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        format!("quantity {} exceeds f64 range", request.state.quantity),
                    ))),
                });
            }
        };

        let ib_order = match build_ib_order(
            request.state.side,
            quantity,
            &request.state.kind,
            request.state.price,
            &request.state.time_in_force,
        ) {
            Ok(o) => o,
            Err(e) => {
                return Some(Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                });
            }
        };

        let ib_order_id = self.allocate_order_id();

        // Store order context for reconstructing Order from OrderStatus callbacks
        let context = OrderContext {
            instrument: request.key.instrument.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
        };
        self.order_ids
            .register(request.key.cid.clone(), ib_order_id, context);

        let client = self.client.clone();
        let side = request.state.side;
        let price = request.state.price;
        let req_quantity = request.state.quantity;
        let kind = request.state.kind;
        let tif = request.state.time_in_force;

        // Move both place_order AND subscription iteration into spawn_blocking
        // to avoid blocking Tokio worker threads on IB's synchronous iterator.
        let result = tokio::task::spawn_blocking(move || {
            use ibapi::orders::PlaceOrder;

            let sub = match client.place_order(ib_order_id, &contract, &ib_order) {
                Ok(s) => s,
                Err(e) => return Err(e.to_string()),
            };

            for event in sub {
                if let PlaceOrder::OrderStatus(status) = event {
                    match status.status.as_str() {
                        "Submitted" | "PreSubmitted" | "PendingSubmit" => {
                            let filled = parse_decimal_or_warn(status.filled, "status.filled");
                            return Ok(Some((ib_order_id, filled)));
                        }
                        "Cancelled" | "Inactive" => {
                            // Don't remove here - let outer match at line 726 handle cleanup
                            // to centralize all error-path removals in one place
                            return Err(status.status);
                        }
                        _ => continue,
                    }
                }
            }

            // Subscription exhausted without terminal status.
            // Do NOT remove mapping here - ExecutionData/CommissionReport may still
            // arrive via account_stream. Time-based cleanup via clear_stale_order_ids()
            // handles stale mappings.
            Ok(None)
        })
        .await;

        match result {
            Ok(Ok(Some((order_id, filled)))) => Some(Order {
                key,
                side,
                price,
                quantity: req_quantity,
                kind,
                time_in_force: tif,
                state: Ok(Open::new(
                    OrderId::new(format_smolstr!("{}", order_id)),
                    Utc::now(),
                    filled,
                )),
            }),
            Ok(Ok(None)) => {
                // Subscription exhausted without terminal status. The order WAS submitted
                // (we have an IB order ID), but we lost tracking. Return Open with zero
                // filled — caller can query via fetch_open_orders or wait for account_stream.
                warn!(
                    ib_order_id,
                    "Order subscription ended without terminal status, returning Open"
                );
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: Ok(Open::new(
                        OrderId::new(format_smolstr!("{}", ib_order_id)),
                        Utc::now(),
                        Decimal::ZERO,
                    )),
                })
            }
            Ok(Err(status)) => {
                // Cleanup order_ids for rejection (place_order error or Cancelled/Inactive)
                self.order_ids.remove_by_ib_id(ib_order_id);
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        status,
                    ))),
                })
            }
            Err(e) => {
                self.order_ids.remove_by_ib_id(ib_order_id);
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                })
            }
        }
    }

    /// Fetch account balances.
    ///
    /// # Limitations
    ///
    /// The `time_exchange` field in returned balances uses `Utc::now()`, not
    /// the actual IB server timestamp. IB's account summary endpoint does not
    /// provide timestamps per balance update.
    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let client = self.client.clone();
        let account = self.config.account.clone();
        // M-10 fix: Use HashSet for O(1) lookup instead of O(n) linear scan
        let assets_filter: HashSet<AssetNameExchange> = assets.iter().cloned().collect();

        tokio::task::spawn_blocking(move || {
            let group = AccountGroup(account);
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .account_summary(&group, &["TotalCashValue", "AvailableFunds"])
                .map_err(|e| UnindexedClientError::Internal(format!("account_summary: {e}")))?;

            let mut aggregator = BalanceAggregator::new();
            for summary in sub {
                aggregator.process(&summary);
            }

            let mut balances = aggregator.to_balances();

            if !assets_filter.is_empty() {
                balances.retain(|b| assets_filter.contains(&b.asset));
            }

            Ok(balances)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }

    /// Fetch open orders.
    ///
    /// # Limitations
    ///
    /// - The `filled_qty` in returned orders is set to zero. IB's open orders endpoint
    ///   returns order definitions, not fill status. For accurate filled quantities,
    ///   use `account_stream` which provides `OrderStatus` events with fill progress.
    /// - The `time_in_force` defaults to `GoodUntilCancelled`. IB's open orders endpoint
    ///   does not return the original TIF setting.
    /// - This method blocks on IB's subscription until IB sends an end-of-data marker.
    ///   If IB is stalled, this will block indefinitely.
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        // M-10 fix: Use HashSet for O(1) lookup
        let instruments_filter: HashSet<_> = instruments.iter().cloned().collect();

        tokio::task::spawn_blocking(move || {
            use ibapi::orders::Orders;

            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .all_open_orders()
                .map_err(|e| UnindexedClientError::Internal(format!("open_orders: {e}")))?;

            let mut orders = Vec::new();
            for order_item in sub {
                let order_data = match order_item {
                    Orders::OrderData(data) => data,
                    _ => continue,
                };

                let instrument = match contracts.get_name_by_con_id(order_data.contract.contract_id)
                {
                    Some(i) => i,
                    None => continue,
                };

                if !instruments_filter.is_empty() && !instruments_filter.contains(&instrument) {
                    continue;
                }

                let client_id = order_ids
                    .get_client_id(order_data.order_id)
                    .unwrap_or_else(|| {
                        ClientOrderId::new(format_smolstr!("{}", order_data.order_id))
                    });

                let side = match order_data.order.action {
                    ibapi::orders::Action::Buy => Side::Buy,
                    ibapi::orders::Action::Sell
                    | ibapi::orders::Action::SellShort
                    | ibapi::orders::Action::SellLong => Side::Sell,
                };

                let price = order_data
                    .order
                    .limit_price
                    .map(|p| parse_decimal_or_warn(p, "limit_price"))
                    .unwrap_or_default();
                let kind = if order_data.order.order_type == "LMT" {
                    OrderKind::Limit
                } else {
                    OrderKind::Market
                };

                orders.push(Order {
                    key: OrderKey {
                        exchange: ExchangeId::Ibkr,
                        instrument,
                        strategy: StrategyId::unknown(),
                        cid: client_id,
                    },
                    side,
                    price,
                    quantity: parse_decimal_or_warn(
                        order_data.order.total_quantity,
                        "total_quantity",
                    ),
                    kind,
                    // IB's open orders endpoint doesn't return TIF; default to GTC
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open::new(
                        OrderId::new(format_smolstr!("{}", order_data.order_id)),
                        Utc::now(),
                        Decimal::ZERO, // M-5: filled_qty unavailable from open orders endpoint
                    ),
                });
            }

            Ok(orders)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }

    /// Fetch historical trades (executions).
    ///
    /// # Limitations
    ///
    /// - IB only returns executions from the current trading day. The `time_since`
    ///   parameter is applied client-side to filter within that day. For historical
    ///   executions beyond today, use IB's Flex Query or Activity Statements.
    /// - **Fees are always zero.** IB's executions endpoint doesn't include commission
    ///   data. For trades with accurate fees, use `account_stream()` which pairs
    ///   `ExecutionData` with `CommissionReport` events.
    /// - This method blocks on IB's executions subscription until IB sends an
    ///   end-of-data marker. If IB is stalled, this will block indefinitely.
    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        // M-10 fix: Use HashSet for O(1) lookup
        let instruments_filter: HashSet<_> = instruments.iter().cloned().collect();

        tokio::task::spawn_blocking(move || {
            use ibapi::orders::Executions;

            let filter = ibapi::orders::ExecutionFilter::default();
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .executions(filter)
                .map_err(|e| UnindexedClientError::Internal(format!("executions: {e}")))?;

            let mut trades = Vec::new();
            for exec_item in sub {
                let exec_data = match exec_item {
                    Executions::ExecutionData(data) => data,
                    _ => continue,
                };

                let instrument = match contracts.get_name_by_con_id(exec_data.contract.contract_id)
                {
                    Some(i) => i,
                    None => continue,
                };

                if !instruments_filter.is_empty() && !instruments_filter.contains(&instrument) {
                    continue;
                }

                let exec = &exec_data.execution;
                let exec_time = match execution::parse_ib_timestamp(&exec.time) {
                    Some(t) => t,
                    None => {
                        warn!(
                            exec_id = %exec.execution_id,
                            time = %exec.time,
                            "Unparseable timestamp in execution, skipping"
                        );
                        continue;
                    }
                };

                if exec_time < time_since {
                    continue;
                }

                let side = match parse_ib_side(&exec.side) {
                    Some(s) => s,
                    None => {
                        warn!(
                            side = %exec.side,
                            exec_id = %exec.execution_id,
                            "Unknown IB side string, skipping trade"
                        );
                        continue;
                    }
                };

                let client_id = order_ids
                    .get_client_id(exec.order_id)
                    .unwrap_or_else(|| ClientOrderId::new(format_smolstr!("{}", exec.order_id)));

                trades.push(Trade {
                    id: TradeId::new(&exec.execution_id),
                    order_id: OrderId::new(&client_id.0),
                    instrument,
                    strategy: StrategyId::unknown(),
                    time_exchange: exec_time,
                    side,
                    price: parse_decimal_or_warn(exec.price, "exec.price"),
                    quantity: parse_decimal_or_warn(exec.shares, "exec.shares"),
                    fees: AssetFees::default(),
                });
            }

            Ok(trades)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }
}

/// Build an Order from IB OrderStatus using stored OrderContext.
///
/// `is_terminal` indicates Cancelled/Inactive status (caller determines this before
/// choosing the lookup method, so we take it as a parameter to avoid re-matching).
fn make_order_from_status(
    status: &ibapi::orders::OrderStatus,
    client_id: ClientOrderId,
    ctx: &OrderContext,
    is_terminal: bool,
) -> Order<ExchangeId, InstrumentNameExchange, OrderState<AssetNameExchange, InstrumentNameExchange>>
{
    let ib_id = status.order_id;
    let order_id = OrderId::new(format_smolstr!("{}", ib_id));

    let state = if is_terminal {
        OrderState::inactive(Cancelled::new(order_id, Utc::now()))
    } else if status.status.as_str() == "Filled" {
        OrderState::fully_filled()
    } else {
        OrderState::active(Open::new(
            order_id,
            Utc::now(),
            parse_decimal_or_warn(status.filled, "status.filled"),
        ))
    };

    Order {
        key: OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: ctx.instrument.clone(),
            strategy: StrategyId::unknown(),
            cid: client_id,
        },
        side: ctx.side,
        price: ctx.price,
        quantity: ctx.quantity,
        kind: ctx.kind,
        time_in_force: ctx.time_in_force,
        state,
    }
}

pub use execution::parse_ib_timestamp;
