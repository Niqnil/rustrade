//! Binance Conditional Orders Integration Tests
//!
//! Tests for Stop, StopLimit, TakeProfit, TakeProfitLimit, and TrailingStop orders.
//!
//! # Prerequisites
//!
//! 1. Binance testnet account (https://testnet.binance.vision/, login with GitHub)
//! 2. Environment variables set (see .env.template):
//!    - BINANCE_API_KEY
//!    - BINANCE_SECRET_KEY
//!    - BINANCE_TESTNET=true
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env
//! source .env
//!
//! # Run all Binance conditional order tests
//! cargo test --test binance_conditional_orders --features binance -- --ignored --nocapture
//!
//! # Run specific test
//! cargo test --test binance_conditional_orders --features binance test_stop_order -- --ignored --nocapture
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures without testnet connectivity.

#![cfg(feature = "binance")]
// Integration tests: unwrap/expect produce clear panic messages identifying the
// failed assertion, which is the desired behavior in test code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        binance::{BinanceSpot, BinanceSpotConfig},
    },
    order::{
        OrderKey, OrderKind, TimeInForce, TrailingOffsetType,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
        state::{ActiveOrderState, OrderState},
    },
};
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use std::time::Duration;
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

fn test_config() -> BinanceSpotConfig {
    let api_key = std::env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY env var required");
    let secret_key =
        std::env::var("BINANCE_SECRET_KEY").expect("BINANCE_SECRET_KEY env var required");

    // These tests place live conditional orders, so they are hardcoded to testnet — there is no
    // safe path on which `production()` is the intended target for this suite, regardless of how
    // BINANCE_TESTNET happens to be set in the environment.
    BinanceSpotConfig::testnet(api_key, secret_key)
}

fn test_instrument() -> InstrumentNameExchange {
    // BTCUSDT is available on testnet
    "BTCUSDT".into()
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

    let client = BinanceSpot::new(config);

    // Verify connection by fetching balances
    let balances = client.fetch_balances(&[]).await;
    assert!(
        balances.is_ok(),
        "fetch_balances failed: {:?}",
        balances.err()
    );

    let balances = balances.unwrap();
    println!("Connected to Binance testnet. Balances: {}", balances.len());
    for b in balances.iter().filter(|b| b.balance.total > Decimal::ZERO) {
        println!("  {}: {}", b.asset, b.balance.total);
    }
}

// ============================================================================
// Stop Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_stop_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-stop");
    let cid = ClientOrderId::new(format!("stop-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place STOP_LOSS order: sell BTC when price drops to $50,000
    // This won't trigger since testnet BTC is around $100k
    let request = RequestOpen {
        side: Side::Sell,
        price: None, // Market order when triggered
        quantity: dec!(0.001),
        kind: OrderKind::Stop {
            trigger_price: dec!(50000),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing Stop order: SELL 0.001 BTC @ market when price <= $50,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {}", open_state.id);

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel the order
            let cancel_key = OrderKey {
                exchange: ExchangeId::BinanceSpot,
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

            match &cancel_response.unwrap().state {
                Ok(cancelled) => println!("Order canceled at: {}", cancelled.time_exchange),
                Err(e) => panic!("Cancel rejected: {:?}", e),
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

// ============================================================================
// StopLimit Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_stop_limit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-stop-limit");
    let cid = ClientOrderId::new(format!(
        "stoplimit-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place STOP_LOSS_LIMIT order: sell BTC at limit $49,900 when price drops to $50,000
    let request = RequestOpen {
        side: Side::Sell,
        price: Some(dec!(49900)), // Limit price after trigger
        quantity: dec!(0.001),
        kind: OrderKind::StopLimit {
            trigger_price: dec!(50000),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing StopLimit order: SELL 0.001 BTC @ $49,900 when price <= $50,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("StopLimit order placed successfully!");
            println!("  Exchange Order ID: {}", open_state.id);

            // Cancel
            let cancel_request = rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: response.key.strategy.clone(),
                    cid: response.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(matches!(
                cancel_response.as_ref().map(|r| &r.state),
                Some(Ok(_))
            ));
            println!("Order canceled");
        }
        OrderState::Inactive(e) => panic!("StopLimit order rejected: {:?}", e),
        other => panic!("Unexpected state: {:?}", other),
    }
}

// ============================================================================
// TakeProfit Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_take_profit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-tp");
    let cid = ClientOrderId::new(format!("tp-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place TAKE_PROFIT order: sell BTC when price rises to $110,000
    // Must be within PERCENT_PRICE_BY_SIDE filter (typically ~10% from current price)
    let request = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(0.001),
        kind: OrderKind::TakeProfit {
            trigger_price: dec!(110000),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing TakeProfit order: SELL 0.001 BTC @ market when price >= $110,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TakeProfit order placed successfully!");
            println!("  Exchange Order ID: {}", open_state.id);

            // Cancel
            let cancel_request = rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: response.key.strategy.clone(),
                    cid: response.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(matches!(
                cancel_response.as_ref().map(|r| &r.state),
                Some(Ok(_))
            ));
            println!("Order canceled");
        }
        OrderState::Inactive(e) => panic!("TakeProfit order rejected: {:?}", e),
        other => panic!("Unexpected state: {:?}", other),
    }
}

// ============================================================================
// TakeProfitLimit Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_take_profit_limit_order() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-tpl");
    let cid = ClientOrderId::new(format!("tpl-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place TAKE_PROFIT_LIMIT order
    // Must be within PERCENT_PRICE_BY_SIDE filter (typically ~10% from current price)
    let request = RequestOpen {
        side: Side::Sell,
        price: Some(dec!(109000)), // Limit price
        quantity: dec!(0.001),
        kind: OrderKind::TakeProfitLimit {
            trigger_price: dec!(110000),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing TakeProfitLimit order: SELL 0.001 BTC @ $109,000 when price >= $110,000");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TakeProfitLimit order placed successfully!");
            println!("  Exchange Order ID: {}", open_state.id);

            // Cancel
            let cancel_request = rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: response.key.strategy.clone(),
                    cid: response.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(matches!(
                cancel_response.as_ref().map(|r| &r.state),
                Some(Ok(_))
            ));
            println!("Order canceled");
        }
        OrderState::Inactive(e) => panic!("TakeProfitLimit order rejected: {:?}", e),
        other => panic!("Unexpected state: {:?}", other),
    }
}

// ============================================================================
// TrailingStop Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_trailing_stop_basis_points() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-trailing");
    let cid = ClientOrderId::new(format!("trail-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place trailing stop with 100 basis points (1%) callback
    let request = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(0.001),
        kind: OrderKind::TrailingStop {
            offset: dec!(100), // 100 basis points = 1%
            offset_type: TrailingOffsetType::BasisPoints,
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing TrailingStop order: SELL 0.001 BTC with 1% (100 bps) trailing delta");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TrailingStop order placed successfully!");
            println!("  Exchange Order ID: {}", open_state.id);

            // Cancel
            let cancel_request = rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: response.key.strategy.clone(),
                    cid: response.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(matches!(
                cancel_response.as_ref().map(|r| &r.state),
                Some(Ok(_))
            ));
            println!("Order canceled");
        }
        OrderState::Inactive(e) => panic!("TrailingStop order rejected: {:?}", e),
        other => panic!("Unexpected state: {:?}", other),
    }
}

#[tokio::test]
#[ignore]
async fn test_trailing_stop_percentage() {
    init_logging();

    let config = test_config();
    assert!(config.testnet, "This test MUST run on testnet only!");

    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-trailing-pct");
    let cid = ClientOrderId::new(format!(
        "trail-pct-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // Place trailing stop with 2% callback (specified as percentage)
    let request = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(0.001),
        kind: OrderKind::TrailingStop {
            offset: dec!(2), // 2% → converts to 200 basis points
            offset_type: TrailingOffsetType::Percentage,
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request,
    };

    println!("Placing TrailingStop order: SELL 0.001 BTC with 2% trailing delta");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TrailingStop (percentage) order placed successfully!");
            println!("  Exchange Order ID: {}", open_state.id);

            // Cancel
            let cancel_request = rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: response.key.strategy.clone(),
                    cid: response.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            let cancel_response = client.cancel_order(cancel_request).await;
            assert!(matches!(
                cancel_response.as_ref().map(|r| &r.state),
                Some(Ok(_))
            ));
            println!("Order canceled");
        }
        OrderState::Inactive(e) => panic!("TrailingStop order rejected: {:?}", e),
        other => panic!("Unexpected state: {:?}", other),
    }
}

// ============================================================================
// Unsupported Order Type Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_trailing_stop_absolute_rejected() {
    init_logging();

    let config = test_config();
    assert!(
        config.testnet,
        "Integration tests must run on testnet only!"
    );
    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-trail-abs-rejected");
    let cid = ClientOrderId::new(format!("abs-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // TrailingStop with Absolute offset should be rejected
    let request = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(0.001),
        kind: OrderKind::TrailingStop {
            offset: dec!(1000), // $1000 absolute offset - NOT SUPPORTED
            offset_type: TrailingOffsetType::Absolute,
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request,
    };

    println!("Placing TrailingStop with Absolute offset (should be rejected)");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Inactive(e) => {
            println!("Correctly rejected: {:?}", e);
            let msg = format!("{:?}", e);
            assert!(
                msg.contains("Unsupported") || msg.contains("Absolute"),
                "Error should mention unsupported type: {msg}"
            );
        }
        other => panic!("Expected rejection, got: {:?}", other),
    }
}

#[tokio::test]
#[ignore]
async fn test_trailing_stop_limit_rejected() {
    init_logging();

    let config = test_config();
    assert!(
        config.testnet,
        "Integration tests must run on testnet only!"
    );
    let client = BinanceSpot::new(config);
    let instrument = test_instrument();
    let strategy = StrategyId::new("test-tsl-rejected");
    let cid = ClientOrderId::new(format!("tsl-{}", chrono::Utc::now().timestamp_millis()));

    let order_key = OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: cid.clone(),
    };

    // TrailingStopLimit should be rejected (Binance doesn't support it)
    let request = RequestOpen {
        side: Side::Sell,
        price: Some(dec!(99000)),
        quantity: dec!(0.001),
        kind: OrderKind::TrailingStopLimit {
            offset: dec!(100),
            offset_type: TrailingOffsetType::BasisPoints,
            limit_offset: dec!(50),
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key,
        state: request,
    };

    println!("Placing TrailingStopLimit order (should be rejected)");

    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Inactive(e) => {
            println!("Correctly rejected: {:?}", e);
            let msg = format!("{:?}", e);
            assert!(
                msg.contains("Unsupported") || msg.contains("TrailingStopLimit"),
                "Error should mention unsupported type: {msg}"
            );
        }
        other => panic!("Expected rejection, got: {:?}", other),
    }
}
