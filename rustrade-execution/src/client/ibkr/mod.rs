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
//! - **Order types**: Market, Limit, Stop, StopLimit, TrailingStop, TrailingStopLimit,
//!   and Bracket (entry + take-profit + stop-loss) supported. No Algo orders.
//! - **TimeInForce**: No `post_only` (IB has no maker-only orders)
//! - **No auto-reconnect**: Caller responsibility per library philosophy
//!
//! # See Also
//!
//! - [IB API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/trader-workstation-api/)
//! - `rustrade_data::exchange::ibkr` for market data

pub mod account;
pub mod contract;
pub mod execution;
pub mod order;

use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, Snapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::ExecutionClient,
    error::{ApiError, ConnectivityError, OrderError, UnindexedClientError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{
            OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, UnindexedOrderResponseCancel,
        },
        state::{Cancelled, Expired, Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use account::BalanceAggregator;
use chrono::{DateTime, Utc};
use execution::{ExecutionBuffer, parse_decimal_or_warn, parse_ib_side};
use futures::stream::BoxStream;
use ibapi::{
    accounts::{AccountSummaryResult, types::AccountGroup},
    client::blocking::Client,
};
pub use order::{BracketOrderRequest, BracketOrderResult};
use order::{
    OrderContext, OrderIdMap, PendingCancels, build_ib_bracket_with_oca, build_ib_order,
    side_to_action, time_in_force_to_ib,
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId, ibkr::ContractRegistry,
    instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// Configuration for the IBKR execution client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrConfig {
    /// TWS/Gateway host (e.g., "127.0.0.1")
    pub host: String,
    /// TWS/Gateway port (7496=TWS live, 7497=TWS paper, 4001=GW live, 4002=GW paper)
    pub port: u16,
    /// Client ID (must be unique per connection)
    pub client_id: i32,
    /// Account ID (e.g., "DU123456" for paper).
    ///
    /// Currently unused — balance/position queries use "All" group.
    /// Reserved for future multi-account routing (advisor accounts).
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

/// Account group for "All" accounts (used by reqAccountSummary).
static ACCOUNT_GROUP_ALL: std::sync::LazyLock<AccountGroup> =
    std::sync::LazyLock::new(|| AccountGroup("All".to_string()));

/// Timeout for position stream iteration.
/// Workaround for ibapi bug where `PositionEnd` isn't routed to subscription.
const POSITION_STREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
    pending_cancels: PendingCancels,
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
            pending_cancels: PendingCancels::new(),
            execution_buffer: ExecutionBuffer::new(),
            next_order_id: Arc::new(Mutex::new(next_id)),
        })
    }

    /// Get the next order ID and increment the counter.
    fn allocate_order_id(&self) -> i32 {
        self.allocate_order_id_range(1)
    }

    /// Allocate a contiguous range of order IDs atomically.
    ///
    /// Returns the first ID in the range. Caller uses `base`, `base+1`, ..., `base+(count-1)`.
    ///
    /// This is essential for bracket orders which require consecutive IDs (parent=N,
    /// take_profit=N+1, stop_loss=N+2). A single lock acquisition ensures no other
    /// concurrent `open_order` call can grab an ID in the middle of the range.
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds `i32::MAX`, or if the resulting range would overflow `i32::MAX`.
    #[allow(clippy::expect_used)] // Panic is correct: i32::MAX orders means system is broken
    fn allocate_order_id_range(&self, count: u32) -> i32 {
        let count_i32: i32 = count.try_into().expect("count exceeds i32::MAX");
        let mut id = self.next_order_id.lock();
        let base = *id;
        *id = id
            .checked_add(count_i32)
            .expect("order ID overflow: i32::MAX exceeded");
        base
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

    /// Clear pending cancel entries older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// Pending cancels are tracked to differentiate user-initiated cancellation
    /// from time-based expiration. If a cancel request is submitted but the order
    /// never receives terminal status (e.g., network disconnect), the entry would
    /// remain indefinitely. Call this alongside other stale cleanup methods.
    pub fn clear_stale_pending_cancels(&self, max_age: std::time::Duration) -> usize {
        self.pending_cancels.clear_stale(max_age)
    }

    /// Disconnect from IB Gateway.
    ///
    /// Signals the ibapi client to shut down and releases the client ID for reuse.
    ///
    /// [`IbkrClient`] implements [`Clone`] and has no `Drop` impl: the underlying
    /// connection is released automatically when the last `Arc<Client>` reference
    /// is dropped. Calling `disconnect()` explicitly terminates the connection
    /// **immediately for all clones** sharing this client.
    ///
    /// Any active `account_stream()` iterators will receive errors on their
    /// next iteration attempt.
    ///
    /// This is idempotent — calling it multiple times is safe.
    pub fn disconnect(&self) {
        debug!("Disconnecting IbkrClient");
        self.client.disconnect();
    }

    /// Place a bracket order (entry + take-profit + stop-loss) with OCA linking.
    ///
    /// A bracket order consists of three linked orders:
    /// 1. **Entry**: Limit order to enter the position
    /// 2. **Take Profit**: Limit order to exit at profit target (OCA-linked to SL)
    /// 3. **Stop Loss**: Stop order to exit at loss limit (OCA-linked to TP)
    ///
    /// # OCA (One-Cancels-All) Behavior
    ///
    /// When either exit order fills, IB automatically cancels the other. This
    /// prevents the dangerous scenario where take-profit fills but stop-loss
    /// remains open, potentially opening an unintended opposing position.
    ///
    /// # All-or-Nothing Semantics
    ///
    /// If any leg is rejected by IB (e.g., insufficient margin, invalid price),
    /// this method cancels all other legs and returns all three as `Inactive`.
    /// You will never receive a mix of active and inactive legs.
    ///
    /// # Cancellation Safety
    ///
    /// This future registers order ID mappings before submitting to IB. If
    /// cancelled mid-flight, orders may still be submitted and mappings will
    /// leak. Avoid cancelling this future; use IB's native order timeout via
    /// `TimeInForce` if timeout behavior is needed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = BracketOrderRequest {
    ///     instrument: "AAPL".into(),
    ///     strategy: StrategyId::new("my-strategy"),
    ///     parent_cid: ClientOrderId::new("bracket-001"),
    ///     side: Side::Buy,
    ///     quantity: dec!(100),
    ///     entry_price: dec!(150.00),
    ///     take_profit_price: dec!(160.00),  // +$10 profit target
    ///     stop_loss_price: dec!(145.00),    // -$5 stop loss
    ///     time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
    /// };
    /// let result = client.open_bracket_order(request).await;
    /// ```
    pub async fn open_bracket_order(&self, request: BracketOrderRequest) -> BracketOrderResult {
        let instrument = request.instrument.clone();

        // Look up contract
        let contract = match self.contracts.get_contract(&instrument) {
            Some(c) => c,
            None => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::InstrumentInvalid(
                        instrument,
                        "contract not registered".to_string(),
                    )),
                );
            }
        };

        // Convert quantity to f64
        let quantity: f64 = match request.quantity.try_into() {
            Ok(q) => q,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "quantity {} exceeds f64 range",
                        request.quantity
                    ))),
                );
            }
        };

        // Convert prices to f64
        let entry_price: f64 = match request.entry_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "entry_price {} exceeds f64 range",
                        request.entry_price
                    ))),
                );
            }
        };
        let tp_price: f64 = match request.take_profit_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "take_profit_price {} exceeds f64 range",
                        request.take_profit_price
                    ))),
                );
            }
        };
        let sl_price: f64 = match request.stop_loss_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "stop_loss_price {} exceeds f64 range",
                        request.stop_loss_price
                    ))),
                );
            }
        };

        // Validate time_in_force and convert to IB wire format
        let ib_tif = match time_in_force_to_ib(&request.time_in_force) {
            Ok(tif) => tif,
            Err(e) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "TIF not supported by IB: {e}"
                    ))),
                );
            }
        };

        // Allocate 3 consecutive order IDs atomically
        let parent_ib_id = self.allocate_order_id_range(3);
        let tp_ib_id = parent_ib_id + 1;
        let sl_ib_id = parent_ib_id + 2;

        // Build bracket orders with OCA linking
        let action = side_to_action(request.side);
        let ib_orders = build_ib_bracket_with_oca(
            parent_ib_id,
            action,
            quantity,
            entry_price,
            tp_price,
            sl_price,
            ib_tif,
        );

        // Generate client order IDs for children
        let parent_cid = request.parent_cid.clone();
        let (tp_cid, sl_cid) = derive_child_cids(&parent_cid);

        // Determine opposite side for exit orders
        let exit_side = match request.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        // Register order contexts for all three legs
        let parent_ctx = OrderContext {
            instrument: instrument.clone(),
            side: request.side,
            price: Some(request.entry_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
        };
        let tp_ctx = OrderContext {
            instrument: instrument.clone(),
            side: exit_side,
            price: Some(request.take_profit_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
        };
        let sl_ctx = OrderContext {
            instrument: instrument.clone(),
            side: exit_side,
            price: None, // Stop orders don't have a limit price
            quantity: request.quantity,
            kind: OrderKind::Stop {
                trigger_price: request.stop_loss_price,
            },
            time_in_force: request.time_in_force,
        };

        self.order_ids
            .register(parent_cid.clone(), parent_ib_id, parent_ctx);
        self.order_ids.register(tp_cid.clone(), tp_ib_id, tp_ctx);
        self.order_ids.register(sl_cid.clone(), sl_ib_id, sl_ctx);

        // Place all three orders in spawn_blocking
        let client = self.client.clone();
        let result = tokio::task::spawn_blocking(move || {
            use ibapi::orders::PlaceOrder;

            // Place parent (transmit=false, held until SL is sent)
            let parent_sub = match client.place_order(parent_ib_id, &contract, &ib_orders[0]) {
                Ok(s) => s,
                Err(e) => return Err(format!("parent order failed: {e}")),
            };

            // Place take-profit (transmit=false)
            let tp_sub = match client.place_order(tp_ib_id, &contract, &ib_orders[1]) {
                Ok(s) => s,
                Err(e) => {
                    // Cancel parent before returning
                    let _ = client.cancel_order(parent_ib_id, "");
                    return Err(format!("take_profit order failed: {e}"));
                }
            };

            // Place stop-loss (transmit=true, triggers all)
            let sl_sub = match client.place_order(sl_ib_id, &contract, &ib_orders[2]) {
                Ok(s) => s,
                Err(e) => {
                    // Cancel parent and TP before returning
                    let _ = client.cancel_order(parent_ib_id, "");
                    let _ = client.cancel_order(tp_ib_id, "");
                    return Err(format!("stop_loss order failed: {e}"));
                }
            };

            // Wait for first status from each subscription
            let mut parent_status = None;
            let mut tp_status = None;
            let mut sl_status = None;

            // Poll parent subscription
            for event in parent_sub {
                if let PlaceOrder::OrderStatus(status) = event {
                    match status.status.as_str() {
                        "Submitted" | "PreSubmitted" | "PendingSubmit" => {
                            parent_status = Some(Ok(status.filled));
                            break;
                        }
                        "Cancelled" | "Inactive" => {
                            parent_status = Some(Err(status.status));
                            break;
                        }
                        // Any other terminal status (e.g. "Filled", "ApiCancelled",
                        // "PendingCancel") is treated as a rejection rather than
                        // silently ignored — preserves observable failure semantics.
                        other => {
                            parent_status = Some(Err(format!("unexpected parent status: {other}")));
                            break;
                        }
                    }
                }
            }

            // Poll TP subscription
            for event in tp_sub {
                if let PlaceOrder::OrderStatus(status) = event {
                    match status.status.as_str() {
                        "Submitted" | "PreSubmitted" | "PendingSubmit" => {
                            tp_status = Some(Ok(status.filled));
                            break;
                        }
                        "Cancelled" | "Inactive" => {
                            tp_status = Some(Err(status.status));
                            break;
                        }
                        other => {
                            tp_status =
                                Some(Err(format!("unexpected take_profit status: {other}")));
                            break;
                        }
                    }
                }
            }

            // Poll SL subscription
            for event in sl_sub {
                if let PlaceOrder::OrderStatus(status) = event {
                    match status.status.as_str() {
                        "Submitted" | "PreSubmitted" | "PendingSubmit" => {
                            sl_status = Some(Ok(status.filled));
                            break;
                        }
                        "Cancelled" | "Inactive" => {
                            sl_status = Some(Err(status.status));
                            break;
                        }
                        other => {
                            sl_status = Some(Err(format!("unexpected stop_loss status: {other}")));
                            break;
                        }
                    }
                }
            }

            // Verify all three legs reported a successful status. A `None` here
            // means the subscription closed without producing a recognised event
            // — surface that as an error rather than silently treating it as
            // success. Any error or missing status cancels all legs.
            match (parent_status, tp_status, sl_status) {
                (Some(Ok(parent)), Some(Ok(tp)), Some(Ok(sl))) => Ok((parent, tp, sl)),
                (parent, tp, sl) => {
                    let _ = client.cancel_order(parent_ib_id, "");
                    let _ = client.cancel_order(tp_ib_id, "");
                    let _ = client.cancel_order(sl_ib_id, "");
                    Err(format!(
                        "bracket order rejected: parent={parent:?}, tp={tp:?}, sl={sl:?}"
                    ))
                }
            }
        })
        .await;

        match result {
            Ok(Ok((parent_filled, tp_filled, sl_filled))) => {
                let now = Utc::now();
                let parent_filled_dec = parse_decimal_or_warn(parent_filled, "parent.filled");
                let tp_filled_dec = parse_decimal_or_warn(tp_filled, "tp.filled");
                let sl_filled_dec = parse_decimal_or_warn(sl_filled, "sl.filled");

                BracketOrderResult {
                    parent: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: parent_cid,
                        },
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            OrderId::new(format_smolstr!("{}", parent_ib_id)),
                            now,
                            parent_filled_dec,
                        )),
                    },
                    take_profit: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: tp_cid,
                        },
                        side: exit_side,
                        price: Some(request.take_profit_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            OrderId::new(format_smolstr!("{}", tp_ib_id)),
                            now,
                            tp_filled_dec,
                        )),
                    },
                    stop_loss: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: sl_cid,
                        },
                        side: exit_side,
                        price: None, // Stop orders don't have a limit price
                        quantity: request.quantity,
                        kind: OrderKind::Stop {
                            trigger_price: request.stop_loss_price,
                        },
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            OrderId::new(format_smolstr!("{}", sl_ib_id)),
                            now,
                            sl_filled_dec,
                        )),
                    },
                }
            }
            Ok(Err(err_msg)) => {
                // Clean up order ID mappings
                self.order_ids.remove_by_ib_id(parent_ib_id);
                self.order_ids.remove_by_ib_id(tp_ib_id);
                self.order_ids.remove_by_ib_id(sl_ib_id);

                make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(err_msg)),
                )
            }
            Err(join_err) => {
                // Clean up order ID mappings
                self.order_ids.remove_by_ib_id(parent_ib_id);
                self.order_ids.remove_by_ib_id(tp_ib_id);
                self.order_ids.remove_by_ib_id(sl_ib_id);

                make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "task join error: {join_err}"
                    ))),
                )
            }
        }
    }
}

/// Derive bracket child CIDs from the parent CID.
///
/// Convention: `{parent}_tp` for take-profit, `{parent}_sl` for stop-loss.
/// All bracket call sites must use this helper to keep the naming consistent.
fn derive_child_cids(parent_cid: &ClientOrderId) -> (ClientOrderId, ClientOrderId) {
    (
        ClientOrderId::new(format_smolstr!("{}_tp", parent_cid.0)),
        ClientOrderId::new(format_smolstr!("{}_sl", parent_cid.0)),
    )
}

/// Helper to create a BracketOrderResult with all legs inactive (same error).
fn make_all_inactive_bracket(
    request: &BracketOrderRequest,
    error: OrderError<AssetNameExchange, InstrumentNameExchange>,
) -> BracketOrderResult {
    let exit_side = match request.side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    };

    let parent_cid = request.parent_cid.clone();
    let (tp_cid, sl_cid) = derive_child_cids(&parent_cid);

    BracketOrderResult {
        parent: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: parent_cid,
            },
            side: request.side,
            price: Some(request.entry_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error.clone()),
        },
        take_profit: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: tp_cid,
            },
            side: exit_side,
            price: Some(request.take_profit_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error.clone()),
        },
        stop_loss: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: sl_cid,
            },
            side: exit_side,
            price: None, // Stop orders don't have a limit price
            quantity: request.quantity,
            kind: OrderKind::Stop {
                trigger_price: request.stop_loss_price,
            },
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error),
        },
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
    /// # Known Issue: ibapi Decode Errors
    ///
    /// You may see log errors like `"error decoding message: error occurred:
    /// unexpected message: Error"`. This is an upstream ibapi limitation where
    /// IB Error messages (type 4) on subscription channels aren't properly
    /// routed — they're logged but don't affect functionality.
    ///
    /// # Timeout
    ///
    /// Uses 5-second timeout between position updates. If IB stalls mid-stream
    /// (e.g., due to the ibapi `PositionEnd` routing bug), returns partial results
    /// after 5s of inactivity rather than blocking indefinitely.
    ///
    /// **For accounts with no positions, this method waits the full 5-second
    /// timeout before returning an empty position list.**
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // H-2 fix: Run balances and positions concurrently (independent IB requests)
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        let balances_future = self.fetch_balances(assets);
        let positions_future = tokio::task::spawn_blocking(move || {
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

            // Use next_timeout() instead of blocking iterator to avoid hang.
            // ibapi bug: PositionEnd has no request_id so it's not routed to the
            // subscription, causing the iterator to block forever.
            while let Some(pos_update) = positions_sub.next_timeout(POSITION_STREAM_TIMEOUT) {
                let PositionUpdate::Position(pos) = pos_update else {
                    trace!(?pos_update, "Ignoring non-Position variant");
                    continue;
                };
                let Some(instrument) = contracts.get_name_by_con_id(pos.contract.contract_id)
                else {
                    continue;
                };
                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
                    continue;
                }
                if seen.contains(&instrument) {
                    debug!(
                        instrument = %instrument,
                        "Duplicate position for instrument (multi-account?), skipping"
                    );
                    continue;
                }
                seen.insert(instrument.clone());
                snapshots.push(InstrumentAccountSnapshot {
                    instrument,
                    orders: Vec::new(),
                    position: None,
                });
            }
            Ok::<_, UnindexedClientError>(snapshots)
        });

        // ibapi routes responses by request_id via thread-safe channels (RwLock<HashMap>),
        // so concurrent requests on the same client are safe.
        let (balances_result, positions_result) = tokio::join!(balances_future, positions_future);
        let balances = balances_result?;
        let instrument_snapshots = positions_result
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
        let pending_cancels_clone = self.pending_cancels.clone();
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
                                        &pending_cancels_clone,
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
                // Track user-initiated cancel for Cancelled vs Expired differentiation.
                // Insert only after cancel request succeeded — avoids stale entries if
                // the future is dropped before reaching this branch.
                self.pending_cancels.insert(ib_order_id);

                // H-3 fix: Do NOT remove order_ids here. The mapping is needed to
                // correlate any fill events that arrive between now and when IB
                // confirms the cancel. Removal happens in account_stream when
                // OrderStatus::Cancelled is received.
                // IBKR cancel_order returns no filled qty; use ZERO and let
                // subsequent OrderStatus events provide accurate fill info.
                Some(OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(
                        OrderId::new(format_smolstr!("{}", ib_order_id)),
                        Utc::now(),
                        Decimal::ZERO,
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
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::InstrumentInvalid(
                        request.key.instrument.clone(),
                        "contract not registered".to_string(),
                    ))),
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
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
            Ok(Ok(Some((order_id, filled)))) => {
                // IB always returns order status via subscription - never immediate fills.
                // The filled quantity here is from the OrderStatus event, not a complete fill.
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::active(Open::new(
                        OrderId::new(format_smolstr!("{}", order_id)),
                        Utc::now(),
                        filled,
                    )),
                })
            }
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
                    state: OrderState::active(Open::new(
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
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
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
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
        let assets_filter: Option<HashSet<AssetNameExchange>> = if assets.is_empty() {
            None
        } else {
            Some(assets.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            // IB's reqAccountSummary expects a group name ("All" for all linked accounts),
            // not an account ID. Using account ID causes error 321 "Unified group name is invalid".
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .account_summary(&ACCOUNT_GROUP_ALL, &["TotalCashValue", "AvailableFunds"])
                .map_err(|e| UnindexedClientError::Internal(format!("account_summary: {e}")))?;

            let mut aggregator = BalanceAggregator::new();
            for summary in sub {
                match summary {
                    AccountSummaryResult::Summary(s) => aggregator.process(&s),
                    AccountSummaryResult::End => break,
                }
            }

            let mut balances = aggregator.to_balances();

            if let Some(ref filter) = assets_filter {
                balances.retain(|b| filter.contains(&b.asset));
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
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

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

                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
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
                    .map(|p| parse_decimal_or_warn(p, "limit_price"));
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
    ) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            use ibapi::orders::Executions;

            let exec_filter = ibapi::orders::ExecutionFilter::default();
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .executions(exec_filter)
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

                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
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
                    // IBKR executions API lacks commission data (available via CommissionReport callback).
                    // "UNKNOWN" placeholder will fail indexing - use unindexed or correlate with WS.
                    fees: AssetFees::new(AssetNameExchange::from("UNKNOWN"), Decimal::ZERO, None),
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
/// # IBKR Status Mapping
///
/// | IB Status    | → OrderState          | Rationale                                    |
/// |--------------|-----------------------|----------------------------------------------|
/// | "Inactive"   | OpenFailed(Rejected)  | Order blocked by validation/margin/exchange  |
/// | "Cancelled"  | Cancelled or Expired  | See differentiation logic below              |
/// | "Filled"     | FullyFilled           | Order fully executed                         |
/// | Other        | Active(Open)          | Order working on exchange                    |
///
/// # Cancelled vs Expired Differentiation
///
/// IBKR sends `"Cancelled"` status for both user-initiated cancellation and
/// time-based expiration. We differentiate using:
///
/// 1. If order ID is in `pending_cancels` (set by `cancel_order`) → `Cancelled`
/// 2. Else if `time_in_force == GoodUntilEndOfDay` → `Expired` (DAY order expired)
/// 3. Else → `Cancelled` (broker-initiated or external cancellation)
///
/// # Known Limitation
///
/// If the broker cancels a DAY order before market close (e.g., insufficient margin),
/// it will be misclassified as `Expired`. This is rare and acceptable given the
/// alternative (forking ibapi to preserve order_id in error callbacks).
fn make_order_from_status(
    status: &ibapi::orders::OrderStatus,
    client_id: ClientOrderId,
    ctx: &OrderContext,
    pending_cancels: &PendingCancels,
) -> Order<ExchangeId, InstrumentNameExchange, OrderState<AssetNameExchange, InstrumentNameExchange>>
{
    let ib_id = status.order_id;
    let order_id = OrderId::new(format_smolstr!("{}", ib_id));

    let filled_qty = parse_decimal_or_warn(status.filled, "status.filled");
    let state = match status.status.as_str() {
        "Inactive" => {
            // "Inactive" means the order was accepted by IB but is not working:
            // validation failure, margin issue, exchange closed, share location hold.
            // This is a placement failure, not a cancellation.
            OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                "IB status: Inactive (order blocked by validation/margin/exchange)".into(),
            )))
        }
        "Cancelled" => {
            // Differentiate user-cancel from time-expiration
            let was_user_cancel = pending_cancels.remove(ib_id);

            if was_user_cancel {
                // User called cancel_order() — definitely a cancellation
                OrderState::inactive(Cancelled::new(order_id, Utc::now(), filled_qty))
            } else if matches!(ctx.time_in_force, TimeInForce::GoodUntilEndOfDay) {
                // DAY order without pending cancel — expired at market close
                OrderState::inactive(Expired::new(order_id, Utc::now(), filled_qty))
            } else {
                // GTC/IOC/FOK without pending cancel — broker or exchange cancelled
                OrderState::inactive(Cancelled::new(order_id, Utc::now(), filled_qty))
            }
        }
        "Filled" => OrderState::fully_filled(Filled::new(order_id, Utc::now(), filled_qty, None)),
        _ => {
            // "Submitted", "PreSubmitted", "PendingSubmit", "PendingCancel", etc.
            OrderState::active(Open::new(order_id, Utc::now(), filled_qty))
        }
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
