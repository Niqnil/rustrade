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
        _request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        // TODO: Implement using ExchangeClient::cancel
        debug!("cancel_order not yet implemented");
        None
    }

    async fn open_order(
        &self,
        _request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        // TODO: Implement using ExchangeClient::order
        debug!("open_order not yet implemented");
        None
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
