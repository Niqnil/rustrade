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
//! - WebSocket (`WsManager`): account_stream via UserFills + OrderUpdates subscriptions
//!
//! # Limitations
//!
//! - **No auto-reconnect**: Per library philosophy, reconnection is caller responsibility
//! - **Perpetuals only**: Spot trading is future work
//! - **Price precision**: Hyperliquid requires 5 significant figures for prices

pub mod config;
pub mod error;

use crate::{
    AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::ExecutionClient,
    error::{UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    position::Position,
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, TimeZone, Utc};
use config::HyperliquidConfig;
use error::map_sdk_error;
use ethers::signers::Signer;
use futures::stream::BoxStream;
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, InfoClient};
use rust_decimal::Decimal;
use smol_str::{SmolStr, format_smolstr};
use std::{collections::HashSet, str::FromStr, sync::Arc};
use tracing::{debug, info, warn};

/// USDC asset name on Hyperliquid (the only collateral asset for perps).
const USDC_ASSET: &str = "USDC";

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
    /// Returns the base URL for the configured network (mainnet or testnet).
    #[allow(dead_code)] // Will be used for WS reconnection
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

/// Parse SDK side string ("B" = buy/long, "A" or "S" = sell/short) to barter Side.
fn parse_side(side: &str) -> Option<Side> {
    match side.to_uppercase().as_str() {
        "B" | "BUY" => Some(Side::Buy),
        "A" | "S" | "SELL" => Some(Side::Sell),
        _ => {
            warn!(%side, "Unknown side string");
            None
        }
    }
}

/// Convert milliseconds timestamp to DateTime<Utc>.
fn millis_to_datetime(millis: u64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis as i64)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Build instrument name from Hyperliquid coin name (e.g., "BTC" -> "BTC-USD-PERP").
fn coin_to_instrument(coin: &str) -> InstrumentNameExchange {
    InstrumentNameExchange::from(format_smolstr!("{}-USD-PERP", coin))
}

/// Extract coin name from instrument (e.g., "BTC-USD-PERP" -> "BTC").
fn instrument_to_coin(instrument: &InstrumentNameExchange) -> String {
    let s = instrument.as_ref();
    // Expected format: "COIN-USD-PERP" or just "COIN"
    s.split('-').next().unwrap_or(s).to_string()
}

/// Round a price to 5 significant figures (Hyperliquid requirement).
///
/// Uses string formatting to get exact significant figure count.
fn round_to_5_sig_figs(value: Decimal) -> f64 {
    if value.is_zero() {
        return 0.0;
    }

    let f = value.to_string().parse::<f64>().unwrap_or(0.0);
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

impl ExecutionClient for HyperliquidClient {
    const EXCHANGE: ExchangeId = ExchangeId::HyperliquidPerp;

    type Config = HyperliquidConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        let base_url = if config.testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };

        // SDK initialization is async; block on it since ExecutionClient::new is sync.
        // Safe because callers are already in a tokio runtime context.
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

        let balances = vec![AssetBalance::new(
            AssetNameExchange::from(USDC_ASSET),
            Balance::new(account_value, account_value - margin_used),
            now,
        )];

        // Build instrument filter if provided
        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        // Group open orders by instrument
        let mut orders_by_instrument: std::collections::HashMap<InstrumentNameExchange, Vec<_>> =
            std::collections::HashMap::new();

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

            let order_snapshot = Order {
                key: OrderKey {
                    exchange: ExchangeId::HyperliquidPerp,
                    instrument: instrument.clone(),
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(format_smolstr!("{}", order.oid)),
                },
                side,
                price,
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: crate::order::state::OrderState::active(Open {
                    id: OrderId::new(format_smolstr!("{}", order.oid)),
                    time_exchange: millis_to_datetime(order.timestamp),
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

    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // TODO: Implement using WsManager subscriptions
        debug!("account_stream not yet implemented");
        Err(UnindexedClientError::Internal(
            "Hyperliquid account_stream not yet implemented".to_string(),
        ))
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

        match self.exchange_client.cancel(cancel_request, None).await {
            Ok(response) => {
                debug!(?response, "Cancel order response");
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Ok(Cancelled {
                        id: order_id.clone(),
                        time_exchange: Utc::now(),
                    }),
                })
            }
            Err(e) => {
                warn!(%e, "Cancel order failed");
                Some(OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(error::map_order_error(e, request.key.instrument)),
                })
            }
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        use hyperliquid_rust_sdk::{
            ClientLimit, ClientOrder, ClientOrderRequest, ExchangeDataStatus,
            ExchangeResponseStatus,
        };

        let coin = instrument_to_coin(request.key.instrument);
        let is_buy = request.state.side == Side::Buy;

        // Round price and quantity to 5 significant figures
        let limit_px = round_to_5_sig_figs(request.state.price);
        let sz = round_to_5_sig_figs(request.state.quantity);

        // Map time-in-force
        let tif = map_tif(&request.state.time_in_force).to_string();

        // Build order request
        let order_request = ClientOrderRequest {
            asset: coin,
            is_buy,
            reduce_only: request.state.reduce_only,
            limit_px,
            sz,
            cloid: None, // Could use request.key.cid if it's a valid UUID
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
                    state: Err(error::map_order_error(e, request.key.instrument)),
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
                        Ok(Open {
                            id: OrderId::new(format_smolstr!("{}", resting.oid)),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Filled(filled)) => {
                        debug!(oid = filled.oid, avg_px = %filled.avg_px, "Order filled");
                        Ok(Open {
                            id: OrderId::new(format_smolstr!("{}", filled.oid)),
                            time_exchange: Utc::now(),
                            filled_quantity: parse_decimal(&filled.total_sz, "total_sz")
                                .unwrap_or(request.state.quantity),
                        })
                    }
                    Some(ExchangeDataStatus::Error(msg)) => {
                        warn!(%msg, "Order rejected by exchange");
                        Err(UnindexedOrderError::Rejected(
                            crate::error::ApiError::OrderRejected(msg),
                        ))
                    }
                    Some(ExchangeDataStatus::WaitingForFill)
                    | Some(ExchangeDataStatus::WaitingForTrigger) => {
                        // Trigger/conditional orders - not fully supported yet
                        debug!("Order waiting for trigger/fill");
                        Ok(Open {
                            id: OrderId::new(SmolStr::new("pending")),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Success) | None => {
                        // Generic success without specific order ID
                        debug!("Order submitted successfully");
                        Ok(Open {
                            id: OrderId::new(SmolStr::new("unknown")),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                }
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Order rejected");
                Err(UnindexedOrderError::Rejected(
                    crate::error::ApiError::OrderRejected(msg),
                ))
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

        Ok(vec![AssetBalance::new(
            AssetNameExchange::from(USDC_ASSET),
            Balance::new(account_value, account_value - margin_used),
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
            Some(instruments.iter().cloned().collect())
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

            result.push(Order {
                key: OrderKey {
                    exchange: ExchangeId::HyperliquidPerp,
                    instrument,
                    strategy: StrategyId::unknown(),
                    cid: ClientOrderId::new(format_smolstr!("{}", order.oid)),
                },
                side,
                price,
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Open {
                    id: OrderId::new(format_smolstr!("{}", order.oid)),
                    time_exchange: millis_to_datetime(order.timestamp),
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
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
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
            Some(instruments.iter().cloned().collect())
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

            result.push(Trade {
                id: TradeId::new(SmolStr::new(&fill.hash)),
                order_id: OrderId::new(format_smolstr!("{}", fill.oid)),
                instrument,
                strategy: StrategyId::unknown(),
                time_exchange: millis_to_datetime(fill.time),
                side,
                price,
                quantity,
                fees: AssetFees {
                    asset: QuoteAsset,
                    fees: fee,
                },
            });
        }

        Ok(result)
    }
}
