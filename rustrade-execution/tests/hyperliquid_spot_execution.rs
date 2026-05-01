//! Hyperliquid Spot Execution Client Integration Tests
//!
//! These tests require a funded Hyperliquid testnet account.
//!
//! # Prerequisites
//!
//! 1. Hyperliquid testnet account with mock USDC (claim at app.hyperliquid-testnet.xyz/drip)
//! 2. Environment variables set (see .env.template):
//!    - HYPERLIQUID_PRIVATE_KEY: Wallet private key (hex)
//!    - HYPERLIQUID_TESTNET=true
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env (optional, or export manually)
//! source .env
//!
//! # Run all Hyperliquid spot integration tests
//! cargo test --test hyperliquid_spot_execution --features hyperliquid -- --ignored
//!
//! # Run specific test
//! cargo test --test hyperliquid_spot_execution --features hyperliquid test_spot_connection -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures without testnet connectivity.
//!
//! # Spot vs Perpetuals
//!
//! - Uses `HyperliquidSpotClient` instead of `HyperliquidClient`
//! - Instrument format: "BTC-USDC-SPOT" (converts to "BTC/USDC" for API)
//! - Balances from `user_token_balances()` instead of margin summary
//! - No positions (spot has no margin/leverage)
//! - $10 minimum order notional value

#![cfg(feature = "hyperliquid")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        hyperliquid::{config::HyperliquidConfig, spot::HyperliquidSpotClient},
    },
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
        state::{ActiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing_subscriber::{EnvFilter, fmt};

fn init_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init();
}

fn test_config() -> HyperliquidConfig {
    HyperliquidConfig::from_env().expect(
        "HYPERLIQUID_PRIVATE_KEY env var required. Set HYPERLIQUID_TESTNET=true for testnet.",
    )
}

fn hype_spot_instrument() -> InstrumentNameExchange {
    // HYPE/USDC is a real spot pair on Hyperliquid (index @107 on mainnet)
    // Note: Hyperliquid spot markets are memecoins, not BTC/ETH
    "HYPE-USDC-SPOT".into()
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_connection() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "Integration tests must run on testnet");

    println!("Wallet address: {}", config.wallet_address_hex());

    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    assert_eq!(HyperliquidSpotClient::EXCHANGE, ExchangeId::HyperliquidSpot);
    println!("Client wallet: {}", client.wallet_address());
    println!("Spot client created successfully");
}

// ============================================================================
// Account Snapshot Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_fetch_balances() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let assets: Vec<AssetNameExchange> = vec![];
    let result = client.fetch_balances(&assets).await;

    assert!(result.is_ok(), "fetch_balances failed: {:?}", result.err());

    let balances = result.unwrap();
    println!("Fetched {} spot balance(s)", balances.len());
    for balance in &balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_spot_account_snapshot() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let result = client.account_snapshot(&assets, &instruments).await;

    assert!(
        result.is_ok(),
        "account_snapshot failed: {:?}",
        result.err()
    );

    let snapshot = result.unwrap();
    println!("Exchange: {:?}", snapshot.exchange);
    println!("Spot balances: {}", snapshot.balances.len());
    for balance in &snapshot.balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }
    println!("Instruments with orders: {}", snapshot.instruments.len());
    for inst in &snapshot.instruments {
        println!(
            "  {}: orders={}, position={:?}",
            inst.instrument,
            inst.orders.len(),
            inst.position.as_ref().map(|p| p.quantity)
        );
    }
}

// ============================================================================
// Open Orders Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_fetch_open_orders() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let instruments: Vec<InstrumentNameExchange> = vec![];
    let result = client.fetch_open_orders(&instruments).await;

    assert!(
        result.is_ok(),
        "fetch_open_orders failed: {:?}",
        result.err()
    );

    let orders = result.unwrap();
    println!("Open spot orders: {}", orders.len());
    for order in &orders {
        println!(
            "  {:?} {} {} @ {}",
            order.side, order.quantity, order.key.instrument, order.price
        );
    }
}

// ============================================================================
// Historical Trades Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_fetch_trades() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let since = chrono::Utc::now() - chrono::Duration::days(7);
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let result = client.fetch_trades(since, &instruments).await;

    assert!(result.is_ok(), "fetch_trades failed: {:?}", result.err());

    let trades = result.unwrap();
    println!("Spot trades in last 7 days: {}", trades.len());
    for trade in trades.iter().take(10) {
        println!(
            "  {} {:?} {} {} @ {} (fee: {})",
            trade.time_exchange.format("%Y-%m-%d %H:%M:%S"),
            trade.side,
            trade.quantity,
            trade.instrument,
            trade.price,
            trade.fees.fees
        );
    }
}

// ============================================================================
// Order Lifecycle Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_place_and_cancel_limit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = hype_spot_instrument();
    let strategy = StrategyId::new("test-spot-strategy");
    let order_cid = ClientOrderId::new(format!(
        "spot-test-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Place a limit buy below market price (won't fill)
    // HYPE on testnet ~$90, place at $60 (within 80% of market, won't fill)
    // 1 HYPE @ $60 = $60 notional (above $10 minimum)
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(60.0),
        quantity: dec!(1.0),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing spot limit order: BUY 1 HYPE-USDC-SPOT @ $60 (won't fill)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Spot order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {}", open_state.id);

            // Wait a moment for order to be fully processed
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel the order
            let cancel_key = OrderKey {
                exchange: ExchangeId::HyperliquidSpot,
                instrument: &instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling spot order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Spot order canceled successfully!");
                    println!("  Cancelled at: {}", cancelled.time_exchange);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Spot order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_spot_minimum_notional_validation() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = hype_spot_instrument();
    let strategy = StrategyId::new("test-min-notional");
    let order_cid = ClientOrderId::new(format!("min-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Order below $10 minimum: 0.1 HYPE @ $60 = $6 notional
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(60.0),
        quantity: dec!(0.1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    println!("Placing order below $10 minimum (should be rejected locally)");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Inactive(e) => {
            println!("Order correctly rejected: {:?}", e);
            // Verify it mentions the minimum
            let error_msg = format!("{:?}", e);
            assert!(
                error_msg.contains("10") || error_msg.contains("minimum"),
                "Error should mention $10 minimum: {error_msg}"
            );
        }
        other => {
            panic!("Expected rejection, got: {:?}", other);
        }
    }
}

// ============================================================================
// Account Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_account_stream() {
    init_logging();

    let config = test_config();
    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let stream_result = client.account_stream(&assets, &instruments).await;

    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );

    let mut stream = stream_result.unwrap();

    println!("Spot account stream started. Waiting for events (10 second timeout)...");
    println!("(Note: stream receives both spot and perp events, filtered to spot only)");

    let timeout = tokio::time::timeout(Duration::from_secs(10), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            println!("Event: {:?}", event.kind);
            count += 1;
            if count >= 3 {
                break;
            }
        }
        count
    })
    .await;

    match timeout {
        Ok(count) => println!("Received {} events", count),
        Err(_) => println!("Timeout reached (this is normal if no spot orders are active)"),
    }
}

#[tokio::test]
#[ignore]
async fn test_spot_account_stream_with_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidSpotClient::connect(config)
        .await
        .expect("Failed to connect");

    // Start account stream
    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let stream_result = client.account_stream(&assets, &instruments).await;
    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );
    let mut stream = stream_result.unwrap();

    println!("Spot account stream started, placing order to trigger events...");

    // Give WebSocket time to fully connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Place an order to trigger stream events
    let instrument = hype_spot_instrument();
    let strategy = StrategyId::new("stream-test");
    let order_cid = ClientOrderId::new(format!("stream-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // 1 HYPE @ $60 = $60 notional (above minimum, within 80% of testnet market ~$90)
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(60.0),
        quantity: dec!(1.0),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    println!("Placing spot order (1 HYPE @ $60) to trigger stream events...");
    let response = client.open_order(open_request).await;

    if let Some(response) = response
        && let OrderState::Active(ActiveOrderState::Open(open_state)) = &response.state
    {
        println!("Order placed, waiting for stream events...");

        // Wait for order update in stream
        let timeout = tokio::time::timeout(Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(event) = stream.next().await {
                println!("Stream event: {:?}", event.kind);
                count += 1;
                if count >= 2 {
                    break;
                }
            }
            count
        })
        .await;

        match timeout {
            Ok(count) => println!("Received {} events from stream", count),
            Err(_) => println!("Timeout (events may have arrived before stream ready)"),
        }

        // Cleanup: cancel the order
        let cancel_key = OrderKey {
            exchange: ExchangeId::HyperliquidSpot,
            instrument: &instrument,
            strategy: response.key.strategy.clone(),
            cid: response.key.cid.clone(),
        };

        let cancel_request = rustrade_execution::order::OrderEvent {
            key: cancel_key,
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(open_state.id.clone()),
            },
        };

        let _ = client.cancel_order(cancel_request).await;
        println!("Cleanup: order canceled");
    }
}
