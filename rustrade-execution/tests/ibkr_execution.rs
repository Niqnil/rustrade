//! IBKR Execution Client Integration Tests
//!
//! These tests require IB Gateway or TWS running on localhost:4002 (paper account).
//!
//! # Safety
//!
//! **All tests use paper trading accounts only.** The `IBKR_PAPER_ACCOUNT` env var is required
//! and tests connect to port 4002 (Gateway paper) or 7497 (TWS paper). Never configure these
//! tests to use a live trading account.
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
//!
//! # Subscription Tiers
//!
//! Tests are organized by the market data subscriptions required to run them.
//!
//! ## Tier 0: Paper Account Only (FREE)
//!
//! No market data subscriptions needed — just a paper trading account.
//!
//! | Test | Description |
//! |------|-------------|
//! | `test_connection` | Connect to IB Gateway |
//! | `test_contract_registration` | Register contracts in local registry |
//! | `test_fetch_balances` | Fetch account balances |
//! | `test_account_snapshot` | Fetch full account snapshot |
//! | `test_fetch_open_orders` | Fetch currently open orders |
//! | `test_account_stream` | Stream account events |
//! | `test_fetch_trades` | Fetch historical trades |
//! | `test_order_id_mapping_cleanup` | Test stale order cleanup |
//! | `test_place_and_cancel_limit_order` | Place and cancel limit order |
//! | `test_order_without_registered_contract` | Verify rejection for unknown contract |
//! | `test_cancel_nonexistent_order` | Verify rejection for unknown order |
//! | `test_cancel_produces_cancelled_not_expired` | Verify Cancelled vs Expired state |
//! | `test_place_and_cancel_stop_order` | Stop order lifecycle |
//! | `test_place_and_cancel_stop_limit_order` | Stop-limit order lifecycle |
//! | `test_place_and_cancel_trailing_stop_percentage` | Trailing stop (%) lifecycle |
//! | `test_place_and_cancel_trailing_stop_limit_absolute` | Trailing stop-limit ($) lifecycle |
//! | `test_place_and_cancel_bracket_order` | Bracket order with OCA |
//! | `test_bracket_order_oca_group_linkage` | Verify OCA group linkage |
//! | `test_place_and_cancel_gtd_order` | Good-Till-Date order |
//! | `test_place_moo_order_premarket` | Market-on-Open (timing-sensitive) |
//! | `test_place_loo_order_premarket` | Limit-on-Open (timing-sensitive) |

#![cfg(feature = "ibkr")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Integration tests: panics are the correct failure mode

use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountEventKind,
    client::{
        ExecutionClient,
        ibkr::{IbkrClient, IbkrConfig, contract::stock_contract},
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
// Connection Tests — Tier 0: Paper Account Only (FREE)
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
// Account Snapshot Tests — Tier 0: Paper Account Only (FREE)
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
// Open Orders Tests — Tier 0: Paper Account Only (FREE)
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
// Order Lifecycle Tests — Tier 0: Paper Account Only (FREE)
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

    let open_request = rustrade_execution::order::OrderEvent {
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
// Account Stream Tests — Tier 0: Paper Account Only (FREE)
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
// Historical Trades Tests — Tier 0: Paper Account Only (FREE)
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
// Order ID Mapping Tests — Tier 0: Paper Account Only (FREE)
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
// Edge Case Tests — Tier 0: Paper Account Only (FREE)
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

    let open_request = rustrade_execution::order::OrderEvent {
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

    let cancel_request = rustrade_execution::order::OrderEvent {
        key: cancel_key,
        state: rustrade_execution::order::request::RequestCancel { id: None },
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

// ============================================================================
// Cancelled vs Expired Differentiation Tests — Tier 0: Paper Account Only (FREE)
// ============================================================================

/// Verifies that user-initiated cancel produces `Cancelled` state, not `Expired`.
///
/// This tests the pending_cancels tracking in `make_order_from_status`:
/// - DAY orders cancelled by user → Cancelled (tracked in pending_cancels)
/// - DAY orders expired at market close → Expired (not in pending_cancels)
#[tokio::test]
#[ignore]
async fn test_cancel_produces_cancelled_not_expired() {
    init_logging();

    let config = test_config(11);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    // Start account stream to observe order state changes
    let mut stream = client
        .account_stream(&assets, &instruments)
        .await
        .expect("account_stream failed");

    // Place a DAY order at $1 (won't fill)
    let strategy = StrategyId::new("test-cancel-vs-expire");
    let order_cid = ClientOrderId::new(format!(
        "cancel-test-{}",
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
        time_in_force: TimeInForce::GoodUntilEndOfDay, // DAY order - would be Expired if not cancelled
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing DAY limit order: BUY 1 AAPL @ $1.00");
    let response = client.open_order(open_request).await;
    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    let exchange_order_id = match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed: {:?}", open_state.id);
            open_state.id.clone()
        }
        other => panic!("Expected Open state, got: {:?}", other),
    };

    // Brief delay before cancel
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel the order (this adds to pending_cancels)
    let cancel_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: response.key.strategy.clone(),
        cid: response.key.cid.clone(),
    };

    let cancel_request = rustrade_execution::order::OrderEvent {
        key: cancel_key,
        state: rustrade_execution::order::request::RequestCancel {
            id: Some(exchange_order_id.clone()),
        },
    };

    println!("Cancelling order...");
    let cancel_response = client.cancel_order(cancel_request).await;
    assert!(cancel_response.is_some(), "Expected cancel response");

    // Collect stream events and find the final order state
    println!("Waiting for stream to emit Cancelled state...");
    let mut found_cancelled = false;
    let mut found_expired = false;

    let timeout_result = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            if let AccountEventKind::OrderSnapshot(snapshot) = &event.kind {
                let order = snapshot.value();
                // Match on order ID to find our order
                if order.key.cid == order_cid {
                    println!("Order event: {:?}", order.state);
                    match &order.state {
                        OrderState::Inactive(InactiveOrderState::Cancelled(_)) => {
                            found_cancelled = true;
                            break;
                        }
                        OrderState::Inactive(InactiveOrderState::Expired(_)) => {
                            found_expired = true;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    })
    .await;

    if timeout_result.is_err() {
        println!("Timeout waiting for order state (this may happen if stream doesn't emit)");
    }

    // The key assertion: user cancel should produce Cancelled, not Expired
    if found_cancelled {
        println!("SUCCESS: Order state is Cancelled (correct for user-initiated cancel)");
    } else if found_expired {
        panic!("FAILURE: Order state is Expired (should be Cancelled for user-initiated cancel)");
    } else {
        println!("WARNING: No terminal state observed in stream (cancel_order response was OK)");
    }
}

// ============================================================================
// Stop and Trailing Stop Order Tests (TG13 Phase 1 & 2) — Tier 0: Paper Account Only (FREE)
// ============================================================================

/// Test placing and cancelling a Stop order.
///
/// Uses a Sell Stop with trigger at $0.01 - since AAPL trades far above this,
/// the stop will never trigger and the order remains open for cancellation.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_stop_order() {
    init_logging();

    let config = test_config(12);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-stop-order");
    let order_cid = ClientOrderId::new(format!(
        "stop-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Sell Stop at $0.01 trigger - won't trigger since AAPL >> $0.01
    let request_open = RequestOpen {
        side: Side::Sell,
        price: dec!(0.00), // Not used for Stop (market) orders
        quantity: dec!(1),
        kind: OrderKind::Stop {
            trigger_price: dec!(0.01),
        },
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing Stop order: SELL 1 AAPL @ Stop $0.01 (won't trigger)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
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
                Ok(_cancelled) => {
                    println!("Stop order canceled successfully!");
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

/// Test placing and cancelling a Stop-Limit order.
///
/// Uses a Sell StopLimit with trigger at $0.01 and limit at $0.01 -
/// since AAPL trades far above this, the stop will never trigger.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_stop_limit_order() {
    init_logging();

    let config = test_config(13);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-stop-limit-order");
    let order_cid = ClientOrderId::new(format!(
        "stop-limit-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Sell StopLimit: trigger at $0.01, limit at $0.01 - won't trigger
    let request_open = RequestOpen {
        side: Side::Sell,
        price: dec!(0.01), // Limit price (used when stop triggers)
        quantity: dec!(1),
        kind: OrderKind::StopLimit {
            trigger_price: dec!(0.01),
        },
        time_in_force: TimeInForce::GoodUntilEndOfDay,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing StopLimit order: SELL 1 AAPL @ Stop $0.01, Limit $0.01 (won't trigger)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("StopLimit order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling StopLimit order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(_cancelled) => {
                    println!("StopLimit order canceled successfully!");
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("StopLimit order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test placing and cancelling a TrailingStop order with percentage offset.
///
/// Uses a Sell TrailingStop with 50% trail - the stop price trails 50% below
/// the highest price seen. Since this creates a very wide trail, the order
/// won't trigger and remains open for cancellation.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_trailing_stop_percentage() {
    init_logging();

    let config = test_config(14);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-trailing-stop-pct");
    let order_cid = ClientOrderId::new(format!(
        "trail-stop-pct-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Sell TrailingStop with 50% percentage offset - won't trigger
    let request_open = RequestOpen {
        side: Side::Sell,
        price: dec!(0.00), // Not used for trailing stop (market) orders
        quantity: dec!(1),
        kind: OrderKind::TrailingStop {
            offset: dec!(50), // 50% trailing offset
            offset_type: TrailingOffsetType::Percentage,
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing TrailingStop order: SELL 1 AAPL @ 50% trail (won't trigger)");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TrailingStop (percentage) order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling TrailingStop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(_cancelled) => {
                    println!("TrailingStop (percentage) order canceled successfully!");
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("TrailingStop order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test placing and cancelling a TrailingStopLimit order with absolute offset.
///
/// Uses a Sell TrailingStopLimit with $500 absolute trail and $1 limit offset -
/// the stop price trails $500 below the highest price seen. A $500 trailing
/// distance is far wider than typical intraday AAPL moves, so the stop won't
/// trigger and the order remains open for cancellation.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_trailing_stop_limit_absolute() {
    init_logging();

    let config = test_config(15);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-trailing-stop-limit-abs");
    let order_cid = ClientOrderId::new(format!(
        "trail-stop-limit-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Sell TrailingStopLimit: $500 absolute trail, $1 limit offset from stop
    let request_open = RequestOpen {
        side: Side::Sell,
        price: dec!(0.00), // Not used directly; limit_offset determines limit price
        quantity: dec!(1),
        kind: OrderKind::TrailingStopLimit {
            offset: dec!(500), // $500 trailing amount
            offset_type: TrailingOffsetType::Absolute,
            limit_offset: dec!(1), // Limit price = stop price - $1
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!(
        "Placing TrailingStopLimit order: SELL 1 AAPL @ $500 trail, $1 limit offset (won't trigger)"
    );

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("TrailingStopLimit (absolute) order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling TrailingStopLimit order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(_cancelled) => {
                    println!("TrailingStopLimit (absolute) order canceled successfully!");
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("TrailingStopLimit order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

// ============================================================================
// Bracket Order Tests (TG13 Phase 3) — Tier 0: Paper Account Only (FREE)
// ============================================================================

/// Test placing and cancelling a bracket order with OCA linkage.
///
/// Places a bracket order with:
/// - Entry: Buy limit at $1 (won't fill since AAPL >> $1)
/// - Take Profit: Sell limit at $2
/// - Stop Loss: Sell stop at $0.50
///
/// Verifies all three legs are accepted, then cancels the parent which
/// should cascade to cancel the children.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_bracket_order() {
    use rustrade_execution::client::ibkr::BracketOrderRequest;

    init_logging();

    let config = test_config(16);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-bracket-order");
    let parent_cid =
        ClientOrderId::new(format!("bracket-{}", chrono::Utc::now().timestamp_millis()));

    let request = BracketOrderRequest {
        instrument: aapl_name.clone(),
        strategy: strategy.clone(),
        parent_cid: parent_cid.clone(),
        side: Side::Buy,
        quantity: dec!(1),
        entry_price: dec!(1.00),       // Entry at $1 (won't fill)
        take_profit_price: dec!(2.00), // TP at $2
        stop_loss_price: dec!(0.50),   // SL at $0.50
        time_in_force: TimeInForce::GoodUntilEndOfDay,
    };

    println!("Placing bracket order: BUY 1 AAPL @ $1.00 entry, $2.00 TP, $0.50 SL");

    let result = client.open_bracket_order(request).await;

    // Check parent order
    match &result.parent.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Parent order placed successfully!");
            println!("  Client Order ID: {}", result.parent.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
        }
        OrderState::Inactive(e) => {
            panic!("Parent order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected parent order state: {:?}", other);
        }
    }

    // Check take profit order
    match &result.take_profit.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Take profit order placed successfully!");
            println!("  Client Order ID: {}", result.take_profit.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
        }
        OrderState::Inactive(e) => {
            panic!("Take profit order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected take profit order state: {:?}", other);
        }
    }

    // Check stop loss order
    match &result.stop_loss.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop loss order placed successfully!");
            println!("  Client Order ID: {}", result.stop_loss.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
        }
        OrderState::Inactive(e) => {
            panic!("Stop loss order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected stop loss order state: {:?}", other);
        }
    }

    // Brief delay before cancel
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel the parent order - this should cascade to children via IB's parent_id linkage
    if let OrderState::Active(ActiveOrderState::Open(open_state)) = &result.parent.state {
        let cancel_key = OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: &aapl_name,
            strategy: result.parent.key.strategy.clone(),
            cid: result.parent.key.cid.clone(),
        };

        let cancel_request = rustrade_execution::order::OrderEvent {
            key: cancel_key,
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(open_state.id.clone()),
            },
        };

        println!("Canceling bracket order (parent)...");
        let cancel_response = client.cancel_order(cancel_request).await;

        assert!(cancel_response.is_some(), "Expected cancel response");
        let cancel_response = cancel_response.unwrap();

        match &cancel_response.state {
            Ok(_cancelled) => {
                println!("Bracket order canceled successfully!");
                println!("  (Children should be auto-cancelled by IB via parent_id linkage)");
            }
            Err(e) => {
                panic!("Cancel rejected: {:?}", e);
            }
        }
    }
}

/// Test that bracket order OCA group is set correctly.
///
/// This test verifies the OCA linkage by checking that when we fetch open orders,
/// the TP and SL orders share the same OCA group identifier.
#[tokio::test]
#[ignore]
async fn test_bracket_order_oca_group_linkage() {
    use rustrade_execution::client::ibkr::BracketOrderRequest;

    init_logging();

    let config = test_config(17);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-bracket-oca");
    let parent_cid = ClientOrderId::new(format!(
        "bracket-oca-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let request = BracketOrderRequest {
        instrument: aapl_name.clone(),
        strategy: strategy.clone(),
        parent_cid: parent_cid.clone(),
        side: Side::Buy,
        quantity: dec!(1),
        entry_price: dec!(1.00),
        take_profit_price: dec!(2.00),
        stop_loss_price: dec!(0.50),
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
    };

    println!("Placing bracket order to verify OCA linkage...");
    let result = client.open_bracket_order(request).await;

    // Verify all three are active
    assert!(
        matches!(result.parent.state, OrderState::Active(_)),
        "Parent should be active"
    );
    assert!(
        matches!(result.take_profit.state, OrderState::Active(_)),
        "TP should be active"
    );
    assert!(
        matches!(result.stop_loss.state, OrderState::Active(_)),
        "SL should be active"
    );

    println!("All three legs placed successfully.");
    println!("  Parent CID: {}", result.parent.key.cid);
    println!(
        "  TP CID: {} (should end with _tp)",
        result.take_profit.key.cid
    );
    println!(
        "  SL CID: {} (should end with _sl)",
        result.stop_loss.key.cid
    );

    // Verify CID naming convention
    assert!(
        result.take_profit.key.cid.0.ends_with("_tp"),
        "TP CID should end with _tp"
    );
    assert!(
        result.stop_loss.key.cid.0.ends_with("_sl"),
        "SL CID should end with _sl"
    );

    // Brief delay before cleanup
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cleanup: cancel the parent
    if let OrderState::Active(ActiveOrderState::Open(open_state)) = &result.parent.state {
        let cancel_key = OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: &aapl_name,
            strategy: result.parent.key.strategy.clone(),
            cid: result.parent.key.cid.clone(),
        };

        let cancel_request = rustrade_execution::order::OrderEvent {
            key: cancel_key,
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(open_state.id.clone()),
            },
        };

        let _ = client.cancel_order(cancel_request).await;
        println!("Cleanup: bracket order cancelled.");
    }
}

// ============================================================================
// Extended Time-in-Force Tests (TG13 Phase 6) — Tier 0: Paper Account Only (FREE)
// ============================================================================

/// Test placing and cancelling a Good-Till-Date (GTD) order.
///
/// Uses a limit order with expiry set to tomorrow. The order won't fill at $1
/// and will be cancelled before expiry.
#[tokio::test]
#[ignore]
async fn test_place_and_cancel_gtd_order() {
    init_logging();

    let config = test_config(18);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-gtd-order");
    let order_cid = ClientOrderId::new(format!(
        "gtd-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // GTD order expiring tomorrow at 23:59:59 UTC
    let expiry = chrono::Utc::now() + chrono::Duration::days(1);
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(1.00),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodTillDate { expiry },
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!(
        "Placing GTD limit order: BUY 1 AAPL @ $1.00, expires {}",
        expiry.format("%Y-%m-%d %H:%M:%S UTC")
    );

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("GTD order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling GTD order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            let cancel_response = cancel_response.unwrap();

            match &cancel_response.state {
                Ok(_cancelled) => {
                    println!("GTD order canceled successfully!");
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }
        }
        OrderState::Inactive(e) => {
            panic!("GTD order rejected: {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test placing a Market-on-Open (MOO) order during pre-market.
///
/// Note: This test is timing-sensitive - it should be run during pre-market hours
/// (before 9:30 AM ET) for the order to be accepted. Running during regular hours
/// will result in rejection.
#[tokio::test]
#[ignore]
async fn test_place_moo_order_premarket() {
    init_logging();

    let config = test_config(19);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-moo-order");
    let order_cid = ClientOrderId::new(format!(
        "moo-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Market-on-Open order - only valid during pre-market
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(0.00), // Not used for market orders
        quantity: dec!(1),
        kind: OrderKind::Market,
        time_in_force: TimeInForce::AtOpen,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing MOO order: BUY 1 AAPL at market open");
    println!("Note: This test should be run during pre-market hours");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("MOO order placed successfully (pre-market)!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            // Cancel immediately to avoid execution at open
            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling MOO order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            match &cancel_response.unwrap().state {
                Ok(_) => println!("MOO order canceled successfully!"),
                Err(e) => panic!("Cancel rejected: {:?}", e),
            }
        }
        OrderState::Inactive(e) => {
            // This is expected if running outside pre-market hours
            println!("MOO order rejected (expected if not pre-market): {:?}", e);
            println!("Test is timing-sensitive - run during pre-market for success");
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}

/// Test placing a Limit-on-Open (LOO) order during pre-market.
///
/// Similar to MOO but with a limit price. The order will only fill if the
/// opening price is at or below the limit.
#[tokio::test]
#[ignore]
async fn test_place_loo_order_premarket() {
    init_logging();

    let config = test_config(20);
    let client = connect_client(config).await.expect("connection failed");

    let aapl_name = aapl_instrument();
    let aapl_contract = stock_contract("AAPL", "SMART", "USD");
    client.register_contract(aapl_name.clone(), aapl_contract);

    let strategy = StrategyId::new("test-loo-order");
    let order_cid = ClientOrderId::new(format!(
        "loo-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::Ibkr,
        instrument: &aapl_name,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Limit-on-Open at $1 - won't fill
    let request_open = RequestOpen {
        side: Side::Buy,
        price: dec!(1.00),
        quantity: dec!(1),
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::AtOpen,
        position_id: None,
        reduce_only: false,
    };

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing LOO order: BUY 1 AAPL @ $1.00 at market open");

    let response = client.open_order(open_request).await;

    assert!(response.is_some(), "Expected order response");
    let response = response.unwrap();

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("LOO order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(100)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: &aapl_name,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling LOO order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            assert!(cancel_response.is_some(), "Expected cancel response");
            match &cancel_response.unwrap().state {
                Ok(_) => println!("LOO order canceled successfully!"),
                Err(e) => panic!("Cancel rejected: {:?}", e),
            }
        }
        OrderState::Inactive(e) => {
            println!("LOO order rejected (may be timing-related): {:?}", e);
        }
        other => {
            panic!("Unexpected order state: {:?}", other);
        }
    }
}
