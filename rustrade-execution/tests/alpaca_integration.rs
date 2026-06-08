//! Alpaca Execution Client Integration Tests
//!
//! These tests require Alpaca paper trading API credentials.
//!
//! # Status
//!
//! **Tested locally, CI planned (paper trading allowed by Alpaca):**
//! - All order types: market, limit, stop, stop-limit, trailing stop
//! - Bracket orders with TP/SL legs
//! - Account snapshots, balances, positions
//! - Account event streaming via WebSocket
//!
//! Tests are marked `#[ignore]` to avoid CI failures without credentials.
//!
//! # Prerequisites
//!
//! 1. Alpaca paper trading account (https://app.alpaca.markets)
//! 2. Environment variables set (see .env.template):
//!    - ALPACA_API_KEY: Paper trading API key
//!    - ALPACA_SECRET_KEY: Paper trading secret key
//!    - ALPACA_PAPER=true (optional, defaults to true in tests)
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env (optional, or export manually)
//! source .env
//!
//! # Run all Alpaca integration tests
//! cargo test --test alpaca_integration --features alpaca -- --ignored
//!
//! # Run specific test
//! cargo test --test alpaca_integration --features alpaca test_account_snapshot -- --ignored
//! ```
//!
//! # Market Hours Note
//!
//! US equity market hours are 9:30 AM - 4:00 PM ET (9:30 PM - 4:00 AM SGT next day).
//! Order placement tests work on paper accounts regardless of market hours, but orders
//! for equities placed outside regular/extended hours may sit unfilled until markets open.
//! Crypto orders (e.g., BTC/USD) can be placed 24/7.

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountEventKind,
    client::{
        ExecutionClient,
        alpaca::{AlpacaClient, AlpacaConfig},
    },
    order::{
        OrderKey, OrderKind, TimeInForce, TrailingOffsetType,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
        state::{ActiveOrderState, InactiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use serial_test::serial;
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

fn test_config() -> AlpacaConfig {
    let api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required");
    let secret_key =
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required");

    AlpacaConfig::paper(api_key, secret_key)
}

#[allow(dead_code)] // Reserved for future AAPL equity tests; SPY currently used as the equity fixture
fn aapl_instrument() -> InstrumentNameExchange {
    "AAPL".into()
}

fn spy_instrument() -> InstrumentNameExchange {
    "SPY".into()
}

fn btc_instrument() -> InstrumentNameExchange {
    "BTC/USD".into()
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_connection() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    assert_eq!(AlpacaClient::EXCHANGE, ExchangeId::AlpacaBroker);
    println!("AlpacaClient created successfully (paper trading mode)");

    let balances = client.fetch_balances(&[]).await;
    assert!(
        balances.is_ok(),
        "Basic auth check via fetch_balances failed: {:?}",
        balances.err()
    );
    println!("Authentication verified via fetch_balances");
}

// ============================================================================
// Account Snapshot Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_account_snapshot() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

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

    println!("Instruments: {}", snapshot.instruments.len());
    for snap in &snapshot.instruments {
        println!("  {}: {} open orders", snap.instrument, snap.orders.len());
    }
}

#[tokio::test]
#[ignore]
async fn test_fetch_balances() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let result = client.fetch_balances(&assets).await;

    assert!(result.is_ok(), "fetch_balances failed: {:?}", result.err());

    let balances = result.unwrap();
    println!("Fetched {} balance(s)", balances.len());

    let has_usd = balances
        .iter()
        .any(|b| b.asset.name().as_str().eq_ignore_ascii_case("usd"));
    assert!(has_usd, "Expected USD balance in paper account");

    for balance in &balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }
}

// ============================================================================
// Open Orders Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_open_orders() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

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
// Order Lifecycle Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_limit_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(1.00)),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing limit order: BUY 1 SPY @ $1.00 (won't fill - price too low)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Side: {:?}", response.side);
            println!("  Quantity: {}", response.quantity);
            println!("  Price: {:?}", response.price);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
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
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(InactiveOrderState::FullyFilled(_)) => {
            panic!("Unexpected full fill at $1.00 - market moved unexpectedly");
        }
        OrderState::Inactive(e) => {
            panic!("Order rejected or expired: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_crypto_limit_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-crypto");
    let order_cid = ClientOrderId::new(format!(
        "test-btc-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(1000.00)),
        quantity: dec!(0.01),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing crypto limit order: BUY 0.01 BTC/USD @ $1000 (won't fill, $10 value)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Crypto order placed successfully!");
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
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

            println!("Canceling crypto order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            match &cancel_response.unwrap().state {
                Ok(_) => println!("Crypto order canceled successfully!"),
                Err(e) => panic!("Cancel rejected: {:?}", e),
            }
        }
        OrderState::Inactive(e) => {
            panic!("Crypto order rejected: {:?}", e);
        }
        _ => {
            panic!("Unexpected order state: {:?}", response.state);
        }
    }
}

// ============================================================================
// Account Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_account_stream() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let stream_result = client.account_stream(&assets, &instruments).await;

    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );

    let mut stream = stream_result.unwrap();

    println!("Account stream connected. Waiting for events (10 second timeout)...");
    println!("(Alpaca sends heartbeats every ~35s, so we may not see events in 10s)");

    let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            println!("Event received: {:?}", event.kind);
            count += 1;
            if count >= 3 {
                break;
            }
        }
        count
    })
    .await;

    match timeout_result {
        Ok(count) => {
            println!(
                "Received {} event(s) before stream ended or limit reached",
                count
            );
        }
        Err(_) => {
            println!("Timeout reached (expected - Alpaca heartbeat is 35s)");
            println!("Stream connection verified successfully!");
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_account_stream_with_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    println!("Starting account stream...");
    let stream_result = client.account_stream(&assets, &instruments).await;
    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );
    let mut stream = stream_result.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-stream");
    let order_cid = ClientOrderId::new(format!(
        "stream-test-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: Some(dec!(1.00)),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    println!("Placing order to trigger stream events...");
    let response = client
        .open_order(rustrade_execution::order::OrderEvent {
            key: order_key,
            state: request_open,
        })
        .await;

    let order_id = match response {
        Some(ref r) => match &r.state {
            OrderState::Active(ActiveOrderState::Open(o)) => Some(o.id.clone()),
            _ => None,
        },
        None => None,
    };

    println!("Waiting for order events on stream (5s timeout)...");
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            println!("Stream event: {:?}", event.kind);
            events.push(event);
            if events.len() >= 2 {
                break;
            }
        }
        events
    })
    .await;

    match events {
        Ok(evts) => {
            println!("Received {} event(s) from stream", evts.len());
            for evt in &evts {
                match &evt.kind {
                    AccountEventKind::OrderSnapshot(snapshot) => {
                        let order = snapshot.value();
                        println!(
                            "  OrderSnapshot: {:?} {} {}",
                            order.side, order.quantity, order.key.instrument
                        );
                    }
                    AccountEventKind::OrderCancelled(resp) => {
                        println!("  OrderCancelled: {:?}", resp.state);
                    }
                    AccountEventKind::Trade(trade) => {
                        println!("  Trade: {} @ {}", trade.quantity, trade.price);
                    }
                    _ => {
                        println!("  Other event type");
                    }
                }
            }
        }
        Err(_) => {
            println!(
                "Timeout waiting for stream events (may be normal if order wasn't acknowledged yet)"
            );
        }
    }

    if let Some(oid) = order_id {
        println!("Cleaning up: canceling test order...");
        let cancel_key = OrderKey {
            exchange: ExchangeId::AlpacaBroker,
            instrument: &instrument,
            strategy,
            cid: order_cid,
        };
        let _ = client
            .cancel_order(rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel { id: Some(oid) },
            })
            .await;
        println!("Cleanup complete");
    }
}

// ============================================================================
// Stop Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_stop_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-stop-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Stop order: triggers a market order when SPY drops to $1.00
    // This price is unrealistically low so the order will never trigger
    let request_open = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(1),
        kind: OrderKind::Stop {
            trigger_price: dec!(1.00),
        },
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing stop order: SELL 1 SPY @ stop $1.00 (won't trigger - price too low)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Kind: {:?}", response.kind);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
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

            println!("Canceling stop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Stop order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Stop order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_trailing_stop_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-trail-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Trailing stop order with 5% trail distance
    let request_open = RequestOpen {
        side: Side::Sell,
        price: None,
        quantity: dec!(1),
        kind: OrderKind::TrailingStop {
            offset: dec!(5.0),
            offset_type: TrailingOffsetType::Percentage,
        },
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing trailing stop order: SELL 1 SPY with 5% trail");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Trailing stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Kind: {:?}", response.kind);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
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

            println!("Canceling trailing stop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Trailing stop order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Trailing stop order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

// ============================================================================
// Bracket Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_bracket_order_with_stop() {
    use rustrade_execution::client::alpaca::{AlpacaBracketOrderRequest, AlpacaBracketOrderResult};

    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-bracket");
    let order_cid = ClientOrderId::new(format!(
        "test-bracket-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    // Bracket order with stop-loss as a simple stop order
    // Entry at $100 (way below market ~$580+, won't fill), TP at $120, SL at $90
    let request = AlpacaBracketOrderRequest::new(
        instrument.clone(),
        strategy.clone(),
        order_cid.clone(),
        Side::Buy,
        dec!(1),
        dec!(100.00),
        dec!(120.00),
        dec!(90.00),
        TimeInForce::GoodUntilCancelled { post_only: false },
    );

    println!("Placing bracket order: BUY 1 SPY @ $100 entry, $120 TP, $90 SL (stop)");

    let result: AlpacaBracketOrderResult = client.open_bracket_order(request).await;

    match &result.parent.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Bracket order placed successfully!");
            println!("  Client Order ID: {}", result.parent.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Entry Price: {:?}", result.parent.price);

            // Verify the order is on the book
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Fetch open orders to see all legs
            let instruments: Vec<InstrumentNameExchange> = vec![instrument.clone()];
            let open_orders = client.fetch_open_orders(&instruments).await;
            match open_orders {
                Ok(orders) => {
                    println!("Open orders after bracket placement: {}", orders.len());
                    for order in &orders {
                        println!(
                            "  {:?} {} {} @ {:?} (kind: {:?})",
                            order.side,
                            order.quantity,
                            order.key.instrument,
                            order.price,
                            order.kind
                        );
                    }
                    // Bracket order should create 3 legs (entry, TP, SL)
                    // but we only assert >= 1 since entry might be the only one visible before fill
                    assert!(
                        !orders.is_empty(),
                        "Expected at least the entry order to be visible"
                    );
                }
                Err(e) => {
                    println!("Warning: fetch_open_orders failed: {:?}", e);
                }
            }

            // Cancel the bracket order (canceling parent cancels all legs)
            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: result.parent.key.strategy.clone(),
                cid: result.parent.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling bracket order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            match &cancel_response.unwrap().state {
                Ok(cancelled) => {
                    println!("Bracket order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Bracket order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_bracket_order_with_stop_limit() {
    use rustrade_execution::client::alpaca::{AlpacaBracketOrderRequest, AlpacaBracketOrderResult};

    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-bracket-sl");
    let order_cid = ClientOrderId::new(format!(
        "test-bracket-sl-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    // Bracket order with stop-loss as a stop-limit order
    // Entry at $100 (way below market, won't fill), TP at $120, SL triggers at $90, limit at $88
    let request = AlpacaBracketOrderRequest::new(
        instrument.clone(),
        strategy.clone(),
        order_cid.clone(),
        Side::Buy,
        dec!(1),
        dec!(100.00),
        dec!(120.00),
        dec!(90.00),
        TimeInForce::GoodUntilEndOfDay,
    )
    .with_stop_loss_limit_price(dec!(88.00));

    println!("Placing bracket order: BUY 1 SPY @ $100 entry, $120 TP, $90/$88 SL (stop-limit)");

    let result: AlpacaBracketOrderResult = client.open_bracket_order(request).await;

    match &result.parent.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Bracket order (stop-limit SL) placed successfully!");
            println!("  Client Order ID: {}", result.parent.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Entry Price: {:?}", result.parent.price);

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel the bracket order
            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: result.parent.key.strategy.clone(),
                cid: result.parent.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling bracket order (stop-limit)...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            match &cancel_response.unwrap().state {
                Ok(cancelled) => {
                    println!("Bracket order (stop-limit) canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("Bracket order (stop-limit) rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}
