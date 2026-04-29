//! Hyperliquid Execution Example
//!
//! Demonstrates order execution with Hyperliquid perpetual futures:
//! - Connecting to Hyperliquid (testnet or mainnet)
//! - Fetching account balances and positions
//! - Placing a limit order
//! - Canceling an order
//! - Streaming account events
//!
//! # Prerequisites
//!
//! 1. Hyperliquid account with funds (testnet recommended)
//! 2. Wallet private key (EVM-compatible)
//! 3. Environment variables:
//!    - `HYPERLIQUID_PRIVATE_KEY`: Hex-encoded private key (with or without 0x prefix)
//!    - `HYPERLIQUID_TESTNET`: Set to "true" for testnet (recommended)
//!
//! # Usage
//!
//! ```bash
//! # Using testnet (recommended)
//! HYPERLIQUID_PRIVATE_KEY=0x... HYPERLIQUID_TESTNET=true \
//!     cargo run --example hyperliquid_execution --features hyperliquid
//!
//! # Or source from .env file
//! source .env && cargo run --example hyperliquid_execution --features hyperliquid
//! ```
//!
//! # Safety Note
//!
//! This example places a LIMIT order far from market price that should NOT fill.
//! Always use TESTNET for testing to avoid risking real funds.

// Examples use unwrap/expect for brevity — not production code
#![allow(clippy::unwrap_used, clippy::expect_used)]

use barter_execution::{
    client::{
        ExecutionClient,
        hyperliquid::{HyperliquidClient, config::HyperliquidConfig},
    },
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{RequestCancel, RequestOpen},
        state::{ActiveOrderState, OrderState},
    },
};
use barter_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use rust_decimal_macros::dec;
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    // Load configuration from environment
    let config = match HyperliquidConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            warn!("Configuration error: {e}");
            warn!("Set HYPERLIQUID_PRIVATE_KEY and optionally HYPERLIQUID_TESTNET=true");
            return;
        }
    };

    let network = if config.testnet { "TESTNET" } else { "MAINNET" };
    info!("Connecting to Hyperliquid {network}...");
    info!("Wallet: {}", config.wallet_address_hex());

    if !config.testnet {
        warn!("WARNING: Running on MAINNET - real funds at risk!");
        warn!("Set HYPERLIQUID_TESTNET=true for safe testing");
    }

    // Connect to Hyperliquid
    let client = match HyperliquidClient::connect(config).await {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect: {e}");
            return;
        }
    };

    info!("Connected!");

    // Fetch account snapshot
    info!("");
    info!("=== Account Snapshot ===");

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    match client.account_snapshot(&assets, &instruments).await {
        Ok(snapshot) => {
            info!("Balances:");
            for balance in &snapshot.balances {
                info!(
                    "  {}: total={}, free={}",
                    balance.asset, balance.balance.total, balance.balance.free
                );
            }

            info!("Positions:");
            let positions: Vec<_> = snapshot
                .instruments
                .iter()
                .filter(|i| i.position.is_some())
                .collect();

            if positions.is_empty() {
                info!("  (no open positions)");
            }
            for inst in positions {
                if let Some(pos) = &inst.position {
                    info!(
                        "  {}: qty={}, entry={:?}, pnl={:?}",
                        inst.instrument, pos.quantity, pos.entry_price, pos.unrealized_pnl
                    );
                }
            }

            info!("Open Orders:");
            let total_orders: usize = snapshot.instruments.iter().map(|i| i.orders.len()).sum();
            if total_orders == 0 {
                info!("  (no open orders)");
            }
            for inst in &snapshot.instruments {
                for order in &inst.orders {
                    info!(
                        "  {} {:?} {} @ {}",
                        inst.instrument, order.side, order.quantity, order.price
                    );
                }
            }
        }
        Err(e) => {
            warn!("Failed to get account snapshot: {e}");
        }
    }

    // Place a limit order (far from market to avoid filling)
    info!("");
    info!("=== Placing Limit Order ===");

    let btc_perp: InstrumentNameExchange = "BTC-USD-PERP".into();
    let order_cid =
        ClientOrderId::new(format!("example-{}", chrono::Utc::now().timestamp_millis()));
    let strategy = StrategyId::new("demo-strategy");

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &btc_perp,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Price must be within 80% of market price on Hyperliquid
    // BTC ~$95k, so $50k is ~47% below - safe limit
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(50000.0),
        quantity: dec!(0.001), // Minimum BTC size
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    info!("Placing BUY 0.001 BTC-USD-PERP @ $50,000 (won't fill - below market)");

    match client.open_order(open_request).await {
        Some(response) => {
            match &response.state {
                OrderState::Active(ActiveOrderState::Open(open_state)) => {
                    info!("Order placed successfully!");
                    info!("  Client Order ID: {}", response.key.cid);
                    info!("  Exchange Order ID: {}", open_state.id);

                    // Wait a moment then cancel
                    info!("");
                    info!("=== Canceling Order ===");
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let cancel_key = OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: &btc_perp,
                        strategy: response.key.strategy.clone(),
                        cid: response.key.cid.clone(),
                    };

                    let cancel_request = OrderEvent {
                        key: cancel_key,
                        state: RequestCancel {
                            id: Some(open_state.id.clone()),
                        },
                    };

                    match client.cancel_order(cancel_request).await {
                        Some(cancel_response) => match &cancel_response.state {
                            Ok(_cancelled) => {
                                info!("Order canceled successfully!");
                            }
                            Err(e) => {
                                warn!("Cancel rejected: {e:?}");
                            }
                        },
                        None => {
                            info!("Cancel request sent (no immediate response)");
                        }
                    }
                }
                OrderState::Inactive(e) => {
                    warn!("Order rejected: {e:?}");
                    warn!("This may be due to:");
                    warn!("  - Insufficient margin/balance");
                    warn!("  - Price more than 80% from market");
                    warn!("  - Invalid order parameters");
                }
                other => {
                    info!("Unexpected order state: {other:?}");
                }
            }
        }
        None => {
            info!("Order request sent (no immediate response)");
        }
    }

    // Demonstrate account stream (optional - brief listen)
    info!("");
    info!("=== Account Stream (5 seconds) ===");

    match client.account_stream(&[], &[]).await {
        Ok(mut stream) => {
            info!("Listening for account events...");

            let timeout = tokio::time::timeout(Duration::from_secs(5), async {
                let mut count = 0;
                while let Some(event) = stream.next().await {
                    info!("  Event: {:?}", event.kind);
                    count += 1;
                    if count >= 5 {
                        break;
                    }
                }
                count
            })
            .await;

            match timeout {
                Ok(count) => info!("Received {count} events"),
                Err(_) => info!("Timeout (no events - this is normal if no activity)"),
            }
        }
        Err(e) => {
            warn!("Failed to start account stream: {e}");
        }
    }

    info!("");
    info!("Example complete");
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .init()
}
