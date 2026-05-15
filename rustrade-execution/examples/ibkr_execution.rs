//! IBKR Execution Example
//!
//! **UNTESTED** — Requires TWS or IB Gateway connection.
//!
//! Demonstrates order execution with Interactive Brokers:
//! - Connecting to TWS/Gateway
//! - Fetching account balances and positions
//! - Placing a limit order
//! - Canceling an order
//!
//! # Prerequisites
//!
//! 1. TWS or IB Gateway running on localhost
//! 2. API connections enabled (Configure → API → Settings)
//! 3. Socket port: 7497 (TWS paper) or 4002 (Gateway paper)
//! 4. "Read-Only API" UNCHECKED to allow order placement
//!
//! # Usage
//!
//! ```bash
//! cargo run --example ibkr_execution --features ibkr
//! ```
//!
//! # Safety Note
//!
//! This example places a LIMIT order far from market price that should NOT fill.
//! Always verify TWS/Gateway is connected to a PAPER account before running.

// Examples use unwrap/expect for brevity — not production code
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        ibkr::{IbkrClient, IbkrConfig, contract::stock_contract},
    },
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{RequestCancel, RequestOpen},
        state::{ActiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    // Configuration for IB Gateway paper trading
    let config = IbkrConfig {
        host: "127.0.0.1".to_string(),
        port: 4002,     // Gateway paper; use 7497 for TWS paper
        client_id: 102, // Use different ID from market data connections
        account: std::env::var("IBKR_PAPER_ACCOUNT")
            .unwrap_or_else(|_| "YOUR_PAPER_ACCOUNT_ID".to_string()),
        contracts: vec![], // We'll register contracts manually
    };

    info!("Connecting to IB Gateway...");

    // Connect (blocking call)
    let client = match IbkrClient::connect_sync(config) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect: {e}");
            warn!("Make sure TWS/Gateway is running with API enabled on port 4002");
            return;
        }
    };

    info!("Connected!");

    // Register AAPL contract
    let aapl_name: InstrumentNameExchange = "AAPL".into();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    info!("Registered AAPL contract");

    // Fetch account snapshot (balances and positions)
    info!("");
    info!("=== Account Snapshot ===");

    let assets: Vec<AssetNameExchange> = vec!["USD".into()];
    let instruments: Vec<InstrumentNameExchange> = vec![aapl_name.clone()];

    match client.account_snapshot(&assets, &instruments).await {
        Ok(snapshot) => {
            info!("Balances:");
            for balance in &snapshot.balances {
                info!(
                    "  {}: total={}, free={}",
                    balance.asset, balance.balance.total, balance.balance.free
                );
            }

            info!("Open Orders:");
            if snapshot.instruments.is_empty() {
                info!("  (no open orders)");
            }
            for instrument_snapshot in &snapshot.instruments {
                info!(
                    "  {}: {} open orders",
                    instrument_snapshot.instrument,
                    instrument_snapshot.orders.len()
                );
            }
        }
        Err(e) => {
            warn!("Failed to get account snapshot: {e}");
        }
    }

    // Place a limit order (far from market to avoid filling)
    info!("");
    info!("=== Placing Limit Order ===");

    let order_cid = ClientOrderId::new("example-order-001");
    let strategy = StrategyId::new("demo-strategy");

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(1.00)), // Very low price - won't fill
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    info!("Placing BUY 1 AAPL @ $1.00 (won't fill - too far from market)");

    match client.open_order(open_request).await {
        Some(response) => {
            match &response.state {
                OrderState::Active(ActiveOrderState::Open(open_state)) => {
                    info!("Order placed successfully!");
                    info!("  Client Order ID: {}", response.key.cid);
                    info!("  Exchange Order ID: {:?}", open_state.id);

                    // Wait a moment then cancel
                    info!("");
                    info!("=== Canceling Order ===");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                    let cancel_key = OrderKey {
                        exchange: ExchangeId::Ibkr,
                        instrument: &aapl_name,
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
                OrderState::Inactive(inactive) => {
                    warn!("Order rejected: {inactive:?}");
                    warn!("This may be due to:");
                    warn!("  - 'Read-Only API' is checked in TWS settings");
                    warn!("  - Account not authorized for AAPL trading");
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
