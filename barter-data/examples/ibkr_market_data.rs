//! IBKR Market Data Example
//!
//! **UNTESTED** — Requires TWS or IB Gateway connection.
//!
//! Demonstrates streaming market data from Interactive Brokers:
//! - Quotes (OrderBookL1) for best bid/ask
//!
//! # Trades Subscription
//!
//! Tick-by-tick trades (`IbkrSubscriptionKind::Trades`) require a separate IB
//! market data subscription (NASDAQ TotalView-OpenView or similar). Without it,
//! the subscription will fail. This example only subscribes to quotes.
//!
//! # Prerequisites
//!
//! 1. TWS or IB Gateway running on localhost
//! 2. API connections enabled (Configure → API → Settings)
//! 3. Socket port: 7497 (TWS paper) or 4002 (Gateway paper)
//!
//! # Usage
//!
//! ```bash
//! cargo run --example ibkr_market_data --features ibkr
//! ```

// Examples use unwrap/expect for brevity — not production code
#![allow(clippy::unwrap_used, clippy::expect_used)]

use barter_data::exchange::ibkr::{
    IbkrMarketStream, IbkrStreamConfig,
    subscription::{IbkrSubscription, IbkrSubscriptionKind},
};
use barter_instrument::ibkr::ContractRegistry;
use futures_util::StreamExt;
use ibapi::contracts::Contract;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    // Configuration for IB Gateway paper trading
    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: 4002, // Gateway paper; use 7497 for TWS paper
        client_id: 100,
    };

    // Build contract registry with instruments we want to subscribe to
    let registry = ContractRegistry::new();

    // Register AAPL stock contract
    let aapl_contract = Contract::stock("AAPL").build();
    registry.register("AAPL".into(), aapl_contract.clone());

    let registry = Arc::new(registry);

    // Define subscriptions (see module docs for Trades subscription requirements)
    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    info!("Connecting to IB Gateway...");

    // Initialize the market data stream
    let mut stream = match IbkrMarketStream::init(config, registry, subscriptions) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to connect: {e}");
            warn!("Make sure TWS/Gateway is running with API enabled on port 4002");
            return;
        }
    };

    info!("Connected! Streaming market data for AAPL...");
    info!("Press Ctrl+C to stop");
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                info!("{event:?}");
            }
            Err(e) => {
                warn!("Stream error: {e}");
            }
        }
    }

    info!("Stream ended");
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
