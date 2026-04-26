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
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::ExecutionClient,
    error::{UnindexedClientError, UnindexedOrderError},
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    trade::Trade,
};
use barter_instrument::{
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use config::HyperliquidConfig;
use futures::stream::BoxStream;
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, InfoClient};
use std::sync::Arc;
use tracing::{debug, info};

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
        _instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // TODO: Implement using InfoClient::user_state()
        debug!("account_snapshot not yet implemented");
        Err(UnindexedClientError::Internal(
            "Hyperliquid account_snapshot not yet implemented".to_string(),
        ))
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
        // TODO: Implement using InfoClient::user_state() -> clearinghouse_state.margin_summary
        debug!("fetch_balances not yet implemented");
        Err(UnindexedClientError::Internal(
            "Hyperliquid fetch_balances not yet implemented".to_string(),
        ))
    }

    async fn fetch_open_orders(
        &self,
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // TODO: Implement using InfoClient::open_orders()
        debug!("fetch_open_orders not yet implemented");
        Err(UnindexedClientError::Internal(
            "Hyperliquid fetch_open_orders not yet implemented".to_string(),
        ))
    }

    async fn fetch_trades(
        &self,
        _time_since: DateTime<Utc>,
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        // TODO: Implement using InfoClient::user_fills()
        debug!("fetch_trades not yet implemented");
        Err(UnindexedClientError::Internal(
            "Hyperliquid fetch_trades not yet implemented".to_string(),
        ))
    }
}
