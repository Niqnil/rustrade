//! IBKR Execution Client Integration Tests
//!
//! These tests require IB Gateway or TWS running on localhost:4002 (paper account).
//!
//! # Prerequisites
//!
//! 1. IB Gateway or TWS running with API enabled
//! 2. Paper trading account (set IBKR_PAPER_ACCOUNT env var)
//! 3. Port 4002 (Gateway paper) or 7497 (TWS paper)
//!
//! # Running
//!
//! ```bash
//! # Run all IBKR integration tests
//! IBKR_PAPER_ACCOUNT=<account_id> cargo test --test ibkr_execution --features ibkr -- --ignored
//!
//! # Run specific test
//! IBKR_PAPER_ACCOUNT=<account_id> cargo test --test ibkr_execution --features ibkr test_connection -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures without IB connectivity.

#![cfg(feature = "ibkr")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Integration tests: panics are the correct failure mode

use barter_execution::{
    client::{
        ExecutionClient,
        ibkr::{IbkrClient, IbkrConfig, contract::stock_contract},
    },
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
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

fn test_client_id_base() -> i32 {
    std::env::var("IBKR_CLIENT_ID")
        .ok()
        .and_then(|id| id.parse().ok())
        .unwrap_or(200)
}

fn test_config(client_id_offset: i32) -> IbkrConfig {
    IbkrConfig {
        host: "127.0.0.1".to_string(),
        port: std::env::var("IBKR_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(4002),
        client_id: test_client_id_base() + client_id_offset,
        account: std::env::var("IBKR_PAPER_ACCOUNT").expect("IBKR_PAPER_ACCOUNT env var required"),
        contracts: vec![],
    }
}

fn aapl_instrument() -> InstrumentNameExchange {
    "AAPL".into()
}

/// Connect to IB, wrapping the blocking call in spawn_blocking.
async fn connect_client(config: IbkrConfig) -> Result<IbkrClient, String> {
    tokio::task::spawn_blocking(move || IbkrClient::connect_sync(config).map_err(|e| e.to_string()))
        .await
        .map_err(|e| format!("task join: {e}"))?
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_connection() {
    init_logging();

    let config = test_config(0);
    let client = connect_client(config).await;

    assert!(client.is_ok(), "Failed to connect: {:?}", client.err());
    let client = client.unwrap();

    assert_eq!(IbkrClient::EXCHANGE, ExchangeId::Ibkr);
    assert_eq!(client.contract_registry().len(), 0);
}

#[tokio::test]
#[ignore]
async fn test_contract_registration() {
    init_logging();

    let config = test_config(1);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");

    client.register_contract(aapl_name.clone(), aapl_contract);

    assert_eq!(client.contract_registry().len(), 1);
    assert!(
        client
            .contract_registry()
            .get_contract(&aapl_name)
            .is_some()
    );
}

// ============================================================================
// Account Snapshot Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_balances() {
    init_logging();

    let config = test_config(2);
    let client = connect_client(config).await.expect("connection failed");

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
}

#[tokio::test]
#[ignore]
async fn test_account_snapshot() {
    init_logging();

    let config = test_config(3);
    let client = connect_client(config).await.expect("connection failed");

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
    println!("Instruments: {}", snapshot.instruments.len());
}

// ============================================================================
// Open Orders Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_open_orders() {
    init_logging();

    let config = test_config(4);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

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
async fn test_place_and_cancel_limit_order() {
    init_logging();

    let config = test_config(5);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(1.00),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = barter_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing limit order: BUY 1 AAPL @ $1.00 (won't fill)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            // Brief delay before cancel - order is already Submitted so cancel should work
            // immediately, but small buffer avoids racing with IB's internal state machine
            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = barter_execution::order::OrderEvent {
                key: cancel_key,
                state: barter_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(_cancelled) => {
                    println!("Order canceled successfully!");
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
// Account Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_account_stream() {
    init_logging();

    let config = test_config(6);
    let client = connect_client(config).await.expect("connection failed");

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let stream_result = client.account_stream(&assets, &instruments).await;

    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );

    let mut stream = stream_result.unwrap();

    println!("Account stream started. Waiting for events (5 second timeout)...");

    let timeout = tokio::time::timeout(Duration::from_secs(5), async {
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

// ============================================================================
// Historical Trades Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_trades() {
    init_logging();

    let config = test_config(7);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let since = chrono::Utc::now() - chrono::Duration::hours(24);
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let result = client.fetch_trades(since, &instruments).await;

    assert!(result.is_ok(), "fetch_trades failed: {:?}", result.err());

    let trades = result.unwrap();
    println!("Trades in last 24h: {}", trades.len());
    for trade in trades.iter().take(5) {
        println!(
            "  {} {} {} @ {} (fees: {:?})",
            trade.time_exchange.format("%Y-%m-%d %H:%M:%S"),
            trade.side,
            trade.quantity,
            trade.price,
            trade.fees
        );
    }
}

// ============================================================================
// Order ID Mapping Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_order_id_mapping_cleanup() {
    init_logging();

    let config = test_config(8);
    let client = connect_client(config).await.expect("connection failed");

    assert_eq!(client.pending_execution_count(), 0);

    let cleared_execs = client.clear_stale_executions(Duration::from_secs(3600));
    let cleared_orders = client.clear_stale_order_ids(Duration::from_secs(3600));

    println!("Cleared {} stale executions", cleared_execs);
    println!("Cleared {} stale order IDs", cleared_orders);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_order_without_registered_contract() {
    init_logging();

    let config = test_config(9);
    let client = connect_client(config).await.expect("connection failed");

    let unknown_instrument: InstrumentNameExchange = "UNKNOWN_SYMBOL".into();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new("test-order-unknown");

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &unknown_instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(100.00),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = barter_execution::order::OrderEvent {
        key: order_key,
        state: request_open,
    };

    let response = client.open_order(open_request).await;

    assert!(response.is_some());
    let response = response.unwrap();

    assert!(
        response.state.is_failed(),
        "Expected rejection for unregistered contract"
    );
    println!("Order correctly rejected: {:?}", response.state);
}

#[tokio::test]
#[ignore]
async fn test_cancel_nonexistent_order() {
    init_logging();

    let config = test_config(10);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new("nonexistent-order");

    let cancel_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy,
        cid: order_cid,
    };

    let cancel_request = barter_execution::order::OrderEvent {
        key: cancel_key,
        state: barter_execution::order::request::RequestCancel { id: None },
    };

    let response = client.cancel_order(cancel_request).await;

    assert!(response.is_some());
    let response = response.unwrap();

    assert!(
        response.state.is_err(),
        "Expected rejection for nonexistent order"
    );
    println!("Cancel correctly rejected: {:?}", response.state.err());
}
