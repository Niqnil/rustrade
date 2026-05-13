//! IBKR Market Data & Historical Data Integration Tests
//!
//! These tests require IB Gateway or TWS running on localhost:4002 (paper account).
//!
//! # Status
//!
//! **NOT TESTED in CI.** IBKR has not confirmed permission to use credentials
//! for CI, and requires IB Gateway/TWS running locally.
//!
//! **Tested locally:** Tier 0 (connection) and Tier 1 (free IBKR Pro subscriptions).
//! **NOT tested locally:** Tier 2 (L2 depth) and Tier 3 (OPRA) — paid subscriptions.
//!
//! # Safety
//!
//! **All tests use paper trading accounts only.** Tests connect to port 4002 (Gateway paper)
//! or 7497 (TWS paper). Never configure these tests to use a live trading account.
//!
//! # Prerequisites
//!
//! 1. IB Gateway or TWS running with API enabled
//! 2. Market data subscriptions for test instruments (see tiers below)
//! 3. Port 4002 (Gateway paper) or 7497 (TWS paper)
//!
//! # Running
//!
//! ```bash
//! # Run all IBKR integration tests
//! cargo test --test ibkr_integration --features ibkr -- --ignored
//!
//! # Run specific test
//! cargo test --test ibkr_integration --features ibkr test_historical_daily_bars -- --ignored
//! ```
//!
//! # Subscription Tiers
//!
//! Tests are organized by the market data subscriptions required to run them.
//!
//! ## Tier 0: Connection Only (FREE)
//!
//! No market data subscriptions needed.
//!
//! | Test | Description |
//! |------|-------------|
//! | `test_historical_connection` | Connect to IB for historical data |
//! | `test_historical_from_shared_client` | Use shared ibapi client |
//! | `test_market_stream_connection` | Initialize market stream |
//! | `test_market_stream_unregistered_contract` | Verify rejection for unknown contract |
//! | `test_contract_resolution` | Resolve contract details |
//!
//! ## Tier 1: US Real-Time Non-Consolidated (FREE with IBKR Pro)
//!
//! Included with IBKR Pro accounts. Provides IEX/Cboe data (not consolidated NBBO).
//!
//! | Test | Description |
//! |------|-------------|
//! | `test_historical_daily_bars` | Fetch daily OHLCV bars |
//! | `test_historical_hourly_bars` | Fetch hourly bars |
//! | `test_historical_minute_bars` | Fetch 1-minute bars |
//! | `test_historical_midpoint_data` | Fetch midpoint bars |
//! | `test_historical_ticks_trade` | Fetch historical trade ticks |
//! | `test_historical_ticks_bid_ask` | Fetch historical bid/ask ticks |
//! | `test_historical_ticks_with_time_range` | Fetch ticks with specific time range |
//! | `test_market_stream_quotes` | Stream L1 quotes |
//! | `test_market_stream_multiple_subscriptions` | Stream multiple symbols |
//! | `test_calculate_theoretical_greeks` | Calculate option Greeks (calculator, no data) |
//! | `test_calculate_implied_volatility` | Calculate IV (calculator, no data) |
//! | `test_fetch_option_chain` | Fetch option chain structure |
//!
//! ## Tier 2: L2 Market Depth (PAID — varies by exchange)
//!
//! Requires exchange-specific L2 subscription.
//!
//! | Test | Description |
//! |------|-------------|
//! | `test_market_stream_depth` | Stream L2 order book depth |
//!
//! ## Tier 3: OPRA US Options (PAID)
//!
//! Required for real-time options quotes and Greeks.
//!
//! | Test | Description |
//! |------|-------------|
//! | `test_option_greeks_stream` | Stream real-time option Greeks |

#![cfg(feature = "ibkr")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Integration tests: panics are the correct failure mode

use ibapi::{
    contracts::{Contract, SecurityType},
    market_data::historical::{BarSize, WhatToShow},
};
use rustrade_data::{
    event::DataKind,
    exchange::ibkr::{
        IbkrMarketStream, IbkrStreamConfig,
        historical::{HistoricalRequest, HistoricalTickRequest, IbkrHistoricalData, ToDuration},
        subscription::{IbkrSubscription, IbkrSubscriptionKind},
    },
};
use rustrade_instrument::ibkr::ContractRegistry;
use serial_test::serial;
use std::{sync::Arc, time::Duration};
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

fn test_port() -> u16 {
    std::env::var("IBKR_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4002)
}

fn test_client_id_base() -> i32 {
    std::env::var("IBKR_CLIENT_ID")
        .ok()
        .and_then(|id| id.parse().ok())
        .unwrap_or(300)
}

fn aapl_contract() -> Contract {
    Contract::stock("AAPL").build()
}

/// Connect to IB for historical data, wrapping the blocking call in spawn_blocking.
async fn connect_historical(url: &str, client_id: i32) -> Result<IbkrHistoricalData, String> {
    let url = url.to_string();
    tokio::task::spawn_blocking(move || {
        IbkrHistoricalData::connect(&url, client_id).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("task join: {e}"))?
}

/// Connect raw ibapi client, wrapping the blocking call in spawn_blocking.
async fn connect_raw_client(
    url: &str,
    client_id: i32,
) -> Result<ibapi::client::blocking::Client, String> {
    let url = url.to_string();
    tokio::task::spawn_blocking(move || {
        ibapi::client::blocking::Client::connect(&url, client_id).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("task join: {e}"))?
}

// ============================================================================
// Historical Data Tests (Task 3.3.5) — Tier 0/1: Connection + US Real-Time (FREE)
// ============================================================================

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_connection() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base();

    let client = connect_historical(&url, client_id).await;

    assert!(client.is_ok(), "Failed to connect: {:?}", client.err());
    println!("Connected to IB for historical data");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_daily_bars() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 1;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalRequest::daily_trades(contract, 30);

    println!("Fetching 30 days of AAPL daily bars...");

    let result = client.fetch_candles(request).await;

    assert!(result.is_ok(), "fetch_candles failed: {:?}", result.err());

    let candles = result.unwrap();

    println!("Received {} candles", candles.len());
    assert!(!candles.is_empty(), "Expected at least one candle");

    for candle in candles.iter().take(5) {
        println!(
            "  {} O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.0} T:{}",
            candle.close_time.format("%Y-%m-%d"),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.trade_count
        );
    }

    let first = &candles[0];
    // M-6 fix: Include actual values in assertion messages for debugging
    assert!(
        first.high >= first.low,
        "High {:.4} should be >= Low {:.4}",
        first.high,
        first.low
    );
    assert!(
        first.high >= first.open,
        "High {:.4} should be >= Open {:.4}",
        first.high,
        first.open
    );
    assert!(
        first.high >= first.close,
        "High {:.4} should be >= Close {:.4}",
        first.high,
        first.close
    );
    assert!(
        first.low <= first.open,
        "Low {:.4} should be <= Open {:.4}",
        first.low,
        first.open
    );
    assert!(
        first.low <= first.close,
        "Low {:.4} should be <= Close {:.4}",
        first.low,
        first.close
    );
    assert!(
        !first.volume.is_sign_negative(),
        "Volume {} should be non-negative",
        first.volume
    );
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_hourly_bars() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 2;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalRequest {
        contract,
        end_date: None,
        duration: 5.days(),
        bar_size: BarSize::Hour,
        what_to_show: WhatToShow::Trades,
        regular_trading_hours_only: true,
    };

    println!("Fetching 5 days of AAPL hourly bars...");

    let result = client.fetch_candles(request).await;

    assert!(result.is_ok(), "fetch_candles failed: {:?}", result.err());

    let candles = result.unwrap();
    println!("Received {} hourly candles", candles.len());

    assert!(!candles.is_empty(), "Expected at least one hourly candle");

    if candles.len() > 1 {
        let first_time = candles[0].close_time;
        let second_time = candles[1].close_time;
        let diff = second_time - first_time;

        println!(
            "Time between first two bars: {} seconds",
            diff.num_seconds()
        );
        assert!(
            diff.num_seconds() > 0,
            "Candles should be chronologically ordered"
        );
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_minute_bars() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 3;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalRequest {
        contract,
        end_date: None,
        duration: 1.days(),
        bar_size: BarSize::Min,
        what_to_show: WhatToShow::Trades,
        regular_trading_hours_only: true,
    };

    println!("Fetching 1 day of AAPL 1-minute bars...");

    let result = client.fetch_candles(request).await;

    assert!(result.is_ok(), "fetch_candles failed: {:?}", result.err());

    let candles = result.unwrap();
    println!("Received {} minute candles", candles.len());

    assert!(!candles.is_empty(), "Expected at least one 1-minute candle");
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_midpoint_data() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 4;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalRequest {
        contract,
        end_date: None,
        duration: 5.days(),
        bar_size: BarSize::Day,
        what_to_show: WhatToShow::MidPoint,
        regular_trading_hours_only: true,
    };

    println!("Fetching 5 days of AAPL midpoint data...");

    let result = client.fetch_candles(request).await;

    assert!(result.is_ok(), "fetch_candles failed: {:?}", result.err());

    let candles = result.unwrap();
    println!("Received {} midpoint candles", candles.len());

    // Midpoint data may be empty outside market hours or if no recent quotes
    if !candles.is_empty() {
        let first = &candles[0];
        println!(
            "  First midpoint: {} C:{:.2}",
            first.close_time.format("%Y-%m-%d"),
            first.close
        );
        assert_eq!(
            first.trade_count, 0,
            "Midpoint data should have no trade count"
        );
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_from_shared_client() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 5;

    let ib_client = connect_raw_client(&url, client_id)
        .await
        .expect("connection failed");
    let ib_client = Arc::new(ib_client);

    let historical = IbkrHistoricalData::from_client(ib_client);

    let contract = aapl_contract();
    let request = HistoricalRequest::daily_trades(contract, 10);

    let result = historical.fetch_candles(request).await;

    assert!(
        result.is_ok(),
        "fetch_candles from shared client failed: {:?}",
        result.err()
    );

    let candles = result.unwrap();
    assert!(
        !candles.is_empty(),
        "Expected at least one candle from shared client"
    );
    println!("Received {} candles from shared client", candles.len());
}

// ============================================================================
// Historical Tick Data Tests (Task 13.4) — Tier 1: US Real-Time (FREE)
// ============================================================================

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_ticks_trade() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 6;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalTickRequest::recent(contract, 100);

    println!("Fetching recent 100 AAPL trade ticks...");

    let result = client.fetch_historical_ticks(request).await;

    assert!(
        result.is_ok(),
        "fetch_historical_ticks failed: {:?}",
        result.err()
    );

    let trades = result.unwrap();
    println!("Received {} trade ticks", trades.len());

    // May be empty outside market hours
    if !trades.is_empty() {
        for trade in trades.iter().take(5) {
            println!(
                "  Trade: id={} price={:.2} amount={} side={:?}",
                trade.id, trade.price, trade.amount, trade.side
            );
        }

        let first = &trades[0];
        assert!(
            !first.price.is_zero(),
            "Trade price should be non-zero: {}",
            first.price
        );
        assert!(
            first.side.is_none(),
            "IB historical ticks have no side info"
        );
    } else {
        println!("No ticks available (normal outside market hours)");
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_ticks_bid_ask() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 7;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();
    let request = HistoricalTickRequest::recent(contract, 100);

    println!("Fetching recent 100 AAPL bid/ask ticks...");

    let result = client.fetch_historical_bid_ask(request, false).await;

    assert!(
        result.is_ok(),
        "fetch_historical_bid_ask failed: {:?}",
        result.err()
    );

    let quotes = result.unwrap();
    println!("Received {} bid/ask ticks", quotes.len());

    // May be empty outside market hours
    if !quotes.is_empty() {
        for l1 in quotes.iter().take(5) {
            let bid = l1.best_bid.as_ref().map(|b| format!("{:.2}", b.price));
            let ask = l1.best_ask.as_ref().map(|a| format!("{:.2}", a.price));
            println!(
                "  L1: time={} bid={:?} ask={:?}",
                l1.last_update_time.format("%H:%M:%S"),
                bid,
                ask
            );
        }

        let first = &quotes[0];
        assert!(
            first.best_bid.is_some() && first.best_ask.is_some(),
            "Expected both bid and ask to be present"
        );

        let bid = first.best_bid.as_ref().unwrap();
        let ask = first.best_ask.as_ref().unwrap();
        assert!(
            bid.price < ask.price,
            "Bid {:.4} should be less than ask {:.4}",
            bid.price,
            ask.price
        );
    } else {
        println!("No ticks available (normal outside market hours)");
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_ticks_with_time_range() {
    use time::macros::datetime;

    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 8;

    let client = connect_historical(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();

    // Request ticks from a specific time (adjust date as needed for testing)
    let request = HistoricalTickRequest {
        contract,
        start: Some(datetime!(2024-01-15 14:30 UTC)),
        end: None,
        number_of_ticks: 50,
        regular_trading_hours_only: true,
    };

    println!("Fetching 50 AAPL trade ticks starting from 2024-01-15 14:30 UTC...");

    let result = client.fetch_historical_ticks(request).await;

    // This may fail if the date is too old or no data available
    match result {
        Ok(trades) => {
            println!("Received {} trade ticks", trades.len());
            for trade in trades.iter().take(3) {
                println!("  Trade: price={:.2} amount={}", trade.price, trade.amount);
            }
        }
        Err(e) => {
            println!("Request failed (expected if date too old): {}", e);
        }
    }
}

// ============================================================================
// Market Data Stream Tests (Task 3.2.7) — Tier 0/1/2: Varies by test
// ============================================================================

/// Tier 0: Connection Only (FREE)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_connection() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 10,
    };

    let registry = ContractRegistry::new();
    registry.register("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let result = IbkrMarketStream::init(config, registry, subscriptions);

    assert!(
        result.is_ok(),
        "Failed to initialize market stream: {:?}",
        result.err()
    );

    println!("Market stream initialized successfully");
}

/// Tier 1: US Real-Time Non-Consolidated (FREE with IBKR Pro)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_quotes() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 11,
    };

    let registry = ContractRegistry::new();
    registry.register("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let mut stream =
        IbkrMarketStream::init(config, registry, subscriptions).expect("stream init failed");

    println!("Waiting for quote events (10 second timeout)...");
    println!("Note: No quotes will arrive outside US market hours (9:30 AM - 4:00 PM ET)");

    let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut quote_count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(event) => {
                    if let DataKind::OrderBookL1(l1) = &event.kind {
                        let bid_price = l1.best_bid.as_ref().map(|b| b.price);
                        let bid_amount = l1.best_bid.as_ref().map(|b| b.amount);
                        let ask_price = l1.best_ask.as_ref().map(|a| a.price);
                        let ask_amount = l1.best_ask.as_ref().map(|a| a.amount);
                        println!(
                            "Quote: bid={:?} @ {:?}, ask={:?} @ {:?}",
                            bid_price, bid_amount, ask_price, ask_amount
                        );
                        quote_count += 1;
                        if quote_count >= 5 {
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("Stream error: {:?}", e);
                    break;
                }
            }
        }
        quote_count
    })
    .await;

    match timeout_result {
        Ok(count) => println!("Received {} quotes", count),
        // Timeout is acceptable: quotes only flow during US market hours (9:30-16:00 ET)
        Err(_) => println!("Timeout (normal outside market hours)"),
    }
}

/// Tier 2: L2 Market Depth (PAID — varies by exchange)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_depth() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 12,
    };

    let registry = ContractRegistry::new();
    registry.register("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Depth { rows: 5 },
    }];

    let mut stream =
        IbkrMarketStream::init(config, registry, subscriptions).expect("stream init failed");

    println!("Waiting for depth events (10 second timeout)...");
    println!("Note: Depth may not be available for all instruments or times");

    let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut depth_count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(event) => {
                    if let DataKind::OrderBook(book_event) = &event.kind {
                        println!("Depth event: {:?}", book_event);
                        depth_count += 1;
                        if depth_count >= 3 {
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("Stream error: {:?}", e);
                    break;
                }
            }
        }
        depth_count
    })
    .await;

    match timeout_result {
        Ok(count) => println!("Received {} depth updates", count),
        // Timeout is acceptable: depth only flows during market hours and requires L2 subscription
        Err(_) => println!("Timeout (depth may not be available)"),
    }
}

/// Tier 0: Connection Only (FREE)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_unregistered_contract() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 13,
    };

    let registry = Arc::new(ContractRegistry::new());

    let subscriptions = vec![IbkrSubscription {
        instrument: "UNKNOWN_SYMBOL".into(),
        key: "UNKNOWN".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let result = IbkrMarketStream::init(config, registry, subscriptions);

    // M-3: This test validates contract rejection, but connection errors also
    // cause is_err(). The test only confirms contract rejection when connected.
    assert!(result.is_err(), "Expected error for unregistered contract");

    println!(
        "Correctly rejected unregistered contract: {:?}",
        result.err()
    );
}

/// Tier 1: US Real-Time Non-Consolidated (FREE with IBKR Pro)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_multiple_subscriptions() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 14,
    };

    let registry = ContractRegistry::new();
    registry.register("AAPL".into(), aapl_contract());
    registry.register("MSFT".into(), Contract::stock("MSFT").build());
    let registry = Arc::new(registry);

    let subscriptions = vec![
        IbkrSubscription {
            instrument: "AAPL".into(),
            key: "AAPL".to_string(),
            kind: IbkrSubscriptionKind::Quotes,
        },
        IbkrSubscription {
            instrument: "MSFT".into(),
            key: "MSFT".to_string(),
            kind: IbkrSubscriptionKind::Quotes,
        },
    ];

    let result = IbkrMarketStream::init(config, registry, subscriptions);

    assert!(
        result.is_ok(),
        "Failed to initialize multi-subscription stream: {:?}",
        result.err()
    );

    println!("Multi-subscription stream initialized successfully");

    let mut stream = result.unwrap();

    let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut aapl_count = 0;
        let mut msft_count = 0;
        while let Some(result) = stream.next().await {
            if let Ok(event) = result {
                match event.instrument.as_str() {
                    "AAPL" => aapl_count += 1,
                    "MSFT" => msft_count += 1,
                    _ => {}
                }
                if aapl_count >= 2 && msft_count >= 2 {
                    break;
                }
            }
        }
        (aapl_count, msft_count)
    })
    .await;

    match timeout_result {
        Ok((aapl, msft)) => println!("Received {} AAPL, {} MSFT events", aapl, msft),
        Err(_) => println!("Timeout (normal outside market hours)"),
    }
}

// ============================================================================
// Contract Registry Integration — Tier 0: Connection Only (FREE)
// ============================================================================

#[serial]
#[tokio::test]
#[ignore]
async fn test_contract_resolution() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 20;

    let client = connect_raw_client(&url, client_id)
        .await
        .expect("connection failed");

    let contract = aapl_contract();

    println!("Resolving AAPL contract details...");

    // M-5 fix: Wrap blocking call in spawn_blocking for consistency
    let details = tokio::task::spawn_blocking(move || client.contract_details(&contract))
        .await
        .expect("task join failed");

    assert!(
        details.is_ok(),
        "contract_details failed: {:?}",
        details.err()
    );

    let details = details.unwrap();

    assert!(!details.is_empty(), "Expected at least one contract detail");

    let first = &details[0];
    println!("Contract ID: {}", first.contract.contract_id);
    println!("Symbol: {}", first.contract.symbol);
    println!("Exchange: {}", first.contract.exchange);
    println!("Currency: {}", first.contract.currency);

    let registry = ContractRegistry::new();
    registry.register("AAPL".into(), first.contract.clone());

    assert_eq!(registry.len(), 1);
    assert!(registry.get_contract(&"AAPL".into()).is_some());
    assert!(
        registry
            .get_name_by_con_id(first.contract.contract_id)
            .is_some()
    );
}

// ============================================================================
// Option Greeks Calculator Tests (Phase 5A) — Tier 1: US Real-Time (FREE)
// ============================================================================
// Note: These are calculator functions, not data fetches. They don't require
// OPRA subscription — they compute Greeks from user-provided inputs.

/// Create an AAPL call option contract for testing.
///
/// Uses a near-the-money strike with an expiration ~30 days out.
/// Adjust expiration date to a valid future date when running tests.
fn aapl_call_option() -> Contract {
    // Note: Update expiration date to a valid future date before running
    Contract::call("AAPL")
        .strike(200.0)
        .expires_on(2027, 6, 18) // Third Friday of June 2027
        .build()
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_calculate_theoretical_greeks() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 30;

    let client = IbkrHistoricalData::connect(&url, client_id).expect("connection failed");

    let option = aapl_call_option();

    println!("Calculating theoretical Greeks for AAPL call option...");
    println!("  Strike: {}", option.strike);
    println!("  Using volatility: 25% (0.25)");
    println!("  Using underlying price: $200.00");

    let greeks = client
        .calculate_theoretical_greeks(&option, 0.25, 200.0)
        .await;

    match greeks {
        Ok(g) => {
            println!("Theoretical Greeks:");
            if let Some(delta) = g.delta {
                println!("  Delta: {:.4}", delta);
            }
            if let Some(gamma) = g.gamma {
                println!("  Gamma: {:.4}", gamma);
            }
            if let Some(theta) = g.theta {
                println!("  Theta: {:.4}", theta);
            }
            if let Some(vega) = g.vega {
                println!("  Vega: {:.4}", vega);
            }
            if let Some(price) = g.theoretical_price {
                println!("  Theoretical Price: ${:.2}", price);
            }

            // For an ATM call with reasonable inputs, delta should be around 0.5
            assert!(
                g.has_any_greek(),
                "Expected at least some Greeks to be computed"
            );
        }
        Err(e) => panic!("calculate_theoretical_greeks failed: {e}"),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_calculate_implied_volatility() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 31;

    let client = IbkrHistoricalData::connect(&url, client_id).expect("connection failed");

    let option = aapl_call_option();

    println!("Calculating implied volatility for AAPL call option...");
    println!("  Strike: {}", option.strike);
    println!("  Using option price: $10.00");
    println!("  Using underlying price: $200.00");

    let greeks = client
        .calculate_implied_volatility(&option, 10.0, 200.0)
        .await;

    match greeks {
        Ok(g) => {
            println!("Implied Volatility Result:");
            if let Some(iv) = g.implied_volatility {
                println!("  IV: {:.2}%", iv * 100.0);
            }
            if let Some(delta) = g.delta {
                println!("  Delta: {:.4}", delta);
            }

            assert!(g.implied_volatility.is_some(), "Expected IV to be computed");
        }
        Err(e) => panic!("calculate_implied_volatility failed: {e}"),
    }
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_fetch_option_chain() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 32;

    let client = IbkrHistoricalData::connect(&url, client_id).expect("connection failed");

    println!("Fetching option chain for AAPL...");

    let chains = client
        .fetch_option_chain("AAPL", "SMART", SecurityType::Stock, 0)
        .await;

    match chains {
        Ok(entries) => {
            println!("Received {} option chain entries:", entries.len());
            for (i, entry) in entries.iter().take(3).enumerate() {
                println!("Entry {}:", i + 1);
                println!("  Exchange: {}", entry.exchange);
                println!("  Trading Class: {}", entry.trading_class);
                println!("  Multiplier: {}", entry.multiplier);
                println!("  Expirations: {} available", entry.expirations.len());
                if !entry.expirations.is_empty() {
                    println!(
                        "    First few: {:?}",
                        &entry.expirations[..3.min(entry.expirations.len())]
                    );
                }
                println!("  Strikes: {} available", entry.strikes.len());
                if !entry.strikes.is_empty() {
                    println!(
                        "    First few: {:?}",
                        &entry.strikes[..3.min(entry.strikes.len())]
                    );
                }
            }

            assert!(
                !entries.is_empty(),
                "Expected at least one option chain entry"
            );
        }
        Err(e) => {
            println!("Fetch failed: {}", e);
            // Option chain data should be available for AAPL without special subscriptions
            panic!("Option chain fetch failed: {}", e);
        }
    }
}

// ============================================================================
// Real-Time Option Greeks Streaming Tests (Phase 5B) — Tier 3: OPRA (PAID)
// ============================================================================

#[serial]
#[tokio::test]
#[ignore]
async fn test_option_greeks_stream() {
    init_logging();

    let config = IbkrStreamConfig {
        host: "127.0.0.1".to_string(),
        port: test_port(),
        client_id: test_client_id_base() + 33,
    };

    let option = aapl_call_option();

    let registry = ContractRegistry::new();
    registry.register("AAPL_CALL".into(), option);
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL_CALL".into(),
        key: "AAPL_CALL".to_string(),
        kind: IbkrSubscriptionKind::OptionGreeks,
    }];

    let result = IbkrMarketStream::init(config, registry, subscriptions);

    match result {
        Ok(mut stream) => {
            println!("Option Greeks stream initialized");
            println!("Waiting for Greeks updates (10 second timeout)...");
            println!("Note: Requires OPRA subscription for live Greeks");

            let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
                let mut greeks_count = 0;
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(event) => {
                            if let DataKind::OptionGreeks(g) = &event.kind {
                                println!("Greeks update:");
                                if let Some(delta) = g.delta {
                                    println!("  Delta: {:.4}", delta);
                                }
                                if let Some(iv) = g.implied_volatility {
                                    println!("  IV: {:.2}%", iv * 100.0);
                                }
                                greeks_count += 1;
                                if greeks_count >= 3 {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            println!("Stream error: {:?}", e);
                            break;
                        }
                    }
                }
                greeks_count
            })
            .await;

            match timeout_result {
                Ok(count) => println!("Received {} Greeks updates", count),
                Err(_) => println!("Timeout (requires OPRA subscription for live Greeks)"),
            }
        }
        Err(e) => panic!("Stream init failed: {e}"),
    }
}
