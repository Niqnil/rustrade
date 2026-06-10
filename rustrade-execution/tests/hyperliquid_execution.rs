//! Hyperliquid Execution Client Integration Tests
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
//! # Run all Hyperliquid integration tests
//! cargo test --test hyperliquid_execution --features hyperliquid -- --ignored
//!
//! # Run specific test
//! cargo test --test hyperliquid_execution --features hyperliquid test_connection -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures without testnet connectivity.
//!
//! # Mainnet Read-Only Test
//!
//! `test_mainnet_authentication` verifies mainnet connectivity using read-only operations.
//! Safe to run even with zero mainnet balance.

#![cfg(feature = "hyperliquid")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        hyperliquid::{HyperliquidClient, config::HyperliquidConfig},
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

fn btc_instrument() -> InstrumentNameExchange {
    "BTC-USD-PERP".into()
}

fn eth_instrument() -> InstrumentNameExchange {
    "ETH-USD-PERP".into()
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_connection() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "Integration tests must run on testnet");

    println!("Wallet address: {}", config.wallet_address_hex());

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    assert_eq!(HyperliquidClient::EXCHANGE, ExchangeId::HyperliquidPerp);
    println!("Client wallet: {}", client.wallet_address());
    println!("Client created successfully");
}

/// Test mainnet authentication with read-only operations.
/// Uses the same wallet as testnet (which has zero mainnet balance).
/// Verifies EIP-712 signing works against mainnet API.
#[tokio::test]
#[ignore]
async fn test_mainnet_authentication() {
    init_logging();

    // Create mainnet config (override testnet setting)
    let private_key =
        std::env::var("HYPERLIQUID_PRIVATE_KEY").expect("HYPERLIQUID_PRIVATE_KEY env var required");
    let config =
        HyperliquidConfig::from_private_key_mainnet(&private_key).expect("Invalid private key");

    assert!(!config.testnet, "This test must run on mainnet");
    println!("Mainnet wallet address: {}", config.wallet_address_hex());

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect to mainnet");

    // Read-only: fetch balances (will be 0 for test wallet)
    let assets: Vec<AssetNameExchange> = vec![];
    let result = client.fetch_balances(&assets).await;

    assert!(
        result.is_ok(),
        "Mainnet fetch_balances failed: {:?}",
        result.err()
    );

    let balances = result.unwrap();
    println!("Mainnet balances: {} asset(s)", balances.len());
    for balance in &balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }

    println!("Mainnet authentication successful (read-only)");
}

// ============================================================================
// Account Snapshot Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_balances() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let assets: Vec<AssetNameExchange> = vec![];
    let result = client.fetch_balances(&assets).await;

    assert!(result.is_ok(), "fetch_balances failed: {:?}", result.err());

    let balances = result.unwrap();
    println!("Fetched {} balance(s)", balances.len());
    for balance in &balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }

    assert!(!balances.is_empty(), "Expected at least USDC balance");
}

#[tokio::test]
#[ignore]
async fn test_account_snapshot() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
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
    println!("Balances: {}", snapshot.balances.len());
    for balance in &snapshot.balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }
    println!(
        "Instruments with positions/orders: {}",
        snapshot.instruments.len()
    );
    for inst in &snapshot.instruments {
        println!(
            "  {}: orders={}, position={:?}",
            inst.instrument,
            inst.orders.len(),
            inst.position.as_ref().map(|p| p.quantity)
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_account_snapshot_filtered() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments = vec![btc_instrument(), eth_instrument()];

    let result = client.account_snapshot(&assets, &instruments).await;

    assert!(
        result.is_ok(),
        "account_snapshot failed: {:?}",
        result.err()
    );

    let snapshot = result.unwrap();
    println!("Filtered snapshot for BTC and ETH perps");
    println!("Instruments returned: {}", snapshot.instruments.len());
}

// ============================================================================
// Open Orders Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_open_orders() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
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
    println!("Open orders: {}", orders.len());
    for order in &orders {
        println!(
            "  {:?} {} {} @ {:?}",
            order.side, order.quantity, order.key.instrument, order.price
        );
    }
}

// ============================================================================
// Historical Trades Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_trades() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let since = chrono::Utc::now() - chrono::Duration::days(7);
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let result = client.fetch_trades(since, &instruments).await;

    assert!(result.is_ok(), "fetch_trades failed: {:?}", result.err());

    let trades = result.unwrap();
    println!("Trades in last 7 days: {}", trades.len());
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
async fn test_place_and_cancel_limit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!("test-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Place a limit buy below market price (won't fill, but within 80% of market)
    // BTC ~$95k as of 2026, so $50k is ~47% below - within Hyperliquid's 80% limit
    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(50000.0)),
        quantity: dec!(0.001), // Minimum size
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing limit order: BUY 0.001 BTC-USD-PERP @ $50,000 (won't fill)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {}", open_state.id);

            // Wait a moment for order to be fully processed
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel the order
            let cancel_key = OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
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

            println!("Canceling order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Order canceled successfully!");
                    println!("  Cancelled at: {}", cancelled.time_exchange);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

// ============================================================================
// Conditional Order Tests
// ============================================================================

/// Test that Stop orders require UUID-format client order ID.
#[tokio::test]
#[ignore]
async fn test_stop_order_requires_uuid_cid() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");

    // Use a non-UUID cid - should be rejected
    let non_uuid_cid =
        ClientOrderId::new(format!("test-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: non_uuid_cid.clone(),
    };

    // Place a Stop order with non-UUID cid
    let request_open = RequestOpen {
        side: Side::Sell,
        price: None, // Market trigger - no limit price
        quantity: dec!(0.001),
        kind: OrderKind::Stop {
            trigger_price: dec!(80000.0), // Stop below current price
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    println!("Placing Stop order with non-UUID cid (should be rejected)...");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Inactive(rustrade_execution::order::state::InactiveOrderState::OpenFailed(
            rustrade_execution::error::OrderError::Rejected(err),
        )) => {
            println!("Stop order correctly rejected: {:?}", err);
            assert!(
                err.to_string().contains("UUID"),
                "Error message should mention UUID requirement"
            );
        }
        other => {
            panic!("Expected rejection for non-UUID cid, got: {:?}", other);
        }
    }
}

/// Test placing and canceling a Stop order with proper UUID cid.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_stop_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");

    // Use UUID cid as required for trigger orders
    let uuid_cid = ClientOrderId::uuid();
    println!("Using UUID cid: {}", uuid_cid);

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: uuid_cid.clone(),
    };

    // Place a Stop order (sell stop below market to avoid triggering)
    // BTC ~$95k, so $50k stop is well below market
    let request_open = RequestOpen {
        side: Side::Sell,
        price: None, // Market trigger
        quantity: dec!(0.001),
        kind: OrderKind::Stop {
            trigger_price: dec!(50000.0),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing Stop order: SELL 0.001 BTC-USD-PERP @ stop $50,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop order placed successfully!");
            println!("  Client Order ID (UUID): {}", response.key.cid);
            println!("  Order ID: {}", open_state.id);

            // Note: Exchange may return numeric OID (Resting) or UUID (WaitingForTrigger)
            // depending on the trigger order type. Both should work for cancellation.
            let is_uuid_format = open_state.id.0.contains('-');
            println!(
                "  Order ID format: {}",
                if is_uuid_format {
                    "UUID (cloid)"
                } else {
                    "numeric (OID)"
                }
            );

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel using the order ID (works for both OID and cloid)
            let cancel_key = OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
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

            println!("Canceling Stop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Stop order canceled successfully!");
                    println!("  Cancelled at: {}", cancelled.time_exchange);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Stop order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test placing and canceling a TakeProfit order.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_take_profit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");
    let uuid_cid = ClientOrderId::uuid();

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: uuid_cid.clone(),
    };

    // Place a TakeProfit order (sell TP above market to avoid triggering)
    // BTC ~$95k, so $150k TP is well above market
    let request_open = RequestOpen {
        side: Side::Sell,
        price: None, // Market trigger
        quantity: dec!(0.001),
        kind: OrderKind::TakeProfit {
            trigger_price: dec!(150000.0),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    println!("Placing TakeProfit order: SELL 0.001 BTC-USD-PERP @ TP $150,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TakeProfit order placed successfully!");
            println!("  Order ID (cloid): {}", open_state.id);

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel
            let cancel_key = OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
                instrument: &response.key.instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling TakeProfit order...");
            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(cancel_response.is_some());

            match &cancel_response.unwrap().state {
                Ok(_) => println!("TakeProfit order canceled successfully!"),
                Err(e) => panic!("Cancel rejected: {:?}", e),
            }
        }
        OrderState::Inactive(e) => {
            panic!("TakeProfit order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test that TrailingStop orders are rejected (unsupported).
#[tokio::test]
#[ignore]
async fn test_trailing_stop_unsupported() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: StrategyId::new("test"),
        cid: ClientOrderId::uuid(),
    };

    let request_open = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(0.001),
        kind: OrderKind::TrailingStop {
            offset: dec!(100.0),
            offset_type: rustrade_execution::order::TrailingOffsetType::Absolute,
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    println!("Placing TrailingStop order (should be rejected as unsupported)...");

    let response = client.open_order(open_request).await;
    assert!(response.is_some());

    match &response.unwrap().state {
        OrderState::Inactive(rustrade_execution::order::state::InactiveOrderState::OpenFailed(
            rustrade_execution::error::OrderError::UnsupportedOrderType(msg),
        )) => {
            println!("TrailingStop correctly rejected: {}", msg);
            assert!(msg.to_lowercase().contains("trailing"));
        }
        other => {
            panic!("Expected UnsupportedOrderType, got: {:?}", other);
        }
    }
}

// ============================================================================
// Account Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_account_stream() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
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

    println!("Account stream started. Waiting for events (10 second timeout)...");
    println!(
        "(Place/cancel an order manually to see events, or run test_account_stream_with_order)"
    );

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
        Err(_) => println!("Timeout reached (this is normal if no orders are active)"),
    }
}

#[tokio::test]
#[ignore]
async fn test_account_stream_with_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = HyperliquidClient::connect(config)
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

    println!("Account stream started, placing order to trigger events...");

    // Give WebSocket time to fully connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Place an order to trigger stream events
    let instrument = eth_instrument();
    let strategy = StrategyId::new("stream-test");
    let order_cid = ClientOrderId::new(format!("stream-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // ETH ~$3500 as of 2026, so $2000 is ~43% below - within 80% limit
    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(2000.0)),
        quantity: dec!(0.01),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    let response = client.open_order(open_request).await;
    assert!(response.is_some());
    let response = response.unwrap();

    let order_id = match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed: {}", open_state.id);
            Some(open_state.id.clone())
        }
        other => {
            println!("Order failed (may still get stream events): {:?}", other);
            None
        }
    };

    // Collect stream events
    println!("Collecting stream events...");
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            println!("  Stream event: {:?}", event.kind);
            events.push(event);
            if events.len() >= 5 {
                break;
            }
        }
        events
    })
    .await;

    match events {
        Ok(events) => println!("Received {} stream events", events.len()),
        Err(_) => println!("Stream timeout (events may have been received)"),
    }

    // Cleanup: cancel the order if it was placed
    if let Some(oid) = order_id {
        let cancel_key = OrderKey {
            exchange: ExchangeId::HyperliquidPerp,
            instrument: &instrument,
            strategy,
            cid: order_cid,
        };
        let cancel_request = rustrade_execution::order::OrderEvent {
            key: cancel_key,
            state: rustrade_execution::order::request::RequestCancel { id: Some(oid) },
        };
        let _ = client.cancel_order(cancel_request).await;
        println!("Cleanup: order cancelled");
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_cancel_nonexistent_order() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new("nonexistent-order");

    let cancel_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy,
        cid: order_cid,
    };

    let cancel_request = rustrade_execution::order::OrderEvent {
        key: cancel_key,
        state: rustrade_execution::order::request::RequestCancel {
            id: Some(rustrade_execution::order::id::OrderId::new("999999999")),
        },
    };

    let response = client.cancel_order(cancel_request).await;

    assert!(response.is_some());
    let response = response.unwrap();

    // Hyperliquid may return success or error for nonexistent orders
    println!("Cancel nonexistent order result: {:?}", response.state);
}

#[tokio::test]
#[ignore]
async fn test_cancel_without_order_id() {
    init_logging();

    let config = test_config();
    let client = HyperliquidClient::connect(config)
        .await
        .expect("Failed to connect");

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new("no-id-order");

    let cancel_key = OrderKey {
        exchange: ExchangeId::HyperliquidPerp,
        instrument: &instrument,
        strategy,
        cid: order_cid,
    };

    let cancel_request = rustrade_execution::order::OrderEvent {
        key: cancel_key,
        state: rustrade_execution::order::request::RequestCancel { id: None },
    };

    let response = client.cancel_order(cancel_request).await;

    assert!(response.is_some());
    let response = response.unwrap();

    assert!(
        response.state.is_err(),
        "Expected rejection when order ID is missing"
    );
    println!("Cancel correctly rejected: {:?}", response.state.err());
}
