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
//! | `test_market_stream_option_quotes_delayed` | Stream delayed L1 quotes for many options at once (no OPRA needed) |
//! | `test_market_stream_reconnects_after_the_socket_is_cut` | Cut the socket through a local proxy: the stream reports the loss, reconnects and quotes again (delayed data) |
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
    error::DataError,
    event::{DataKind, MarketEvent},
    exchange::ibkr::{
        IbkrMarketDataType, IbkrMarketStream, IbkrStreamConfig,
        historical::{
            HistoricalRequest, HistoricalTickRequest, IbkrHistoricalData, IbkrHistoricalDataConfig,
            ToDuration,
        },
        subscription::{IbkrSubscription, IbkrSubscriptionKind},
    },
    streams::{consumer::MarketStreamOptions, reconnect::Event},
};
use rustrade_instrument::{exchange::ExchangeId, ibkr::ContractRegistry};
use serial_test::serial;
use std::{sync::Arc, time::Duration};
use tokio_stream::{Stream, StreamExt};
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

/// A market stream with the default options, its items unwrapped from their reconnection events.
/// A reconnect fails the test: none of the tests reading it cuts the connection.
async fn init_stream(
    config: IbkrStreamConfig,
    contracts: Arc<ContractRegistry>,
    subscriptions: Vec<IbkrSubscription<String>>,
) -> Result<impl Stream<Item = Result<MarketEvent<String, DataKind>, DataError>> + Unpin, DataError>
{
    let stream = IbkrMarketStream::init(
        config,
        contracts,
        subscriptions,
        MarketStreamOptions::default(),
    )
    .await?;
    Ok(stream.map(|event| match event {
        Event::Item(item) => item,
        Event::Reconnecting(exchange) => panic!("unexpected reconnect of {exchange}"),
    }))
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
            "  {} O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:?} T:{:?}",
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
        first.volume.is_some_and(|v| !v.is_sign_negative()),
        "Volume {:?} is absent or negative; a Trades bar always reports one",
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
            first.trade_count, None,
            "Midpoint data should have no trade count (unknown, not a fabricated 0)"
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

/// A request IB cannot answer within the connection's request timeout fails with
/// `RequestTimeout` naming it, instead of waiting: a candle request, bounded by `ibapi`, and a
/// tick request, bounded by this library. A millisecond is too short for any answer.
#[serial]
#[tokio::test]
#[ignore]
async fn test_historical_request_timeout() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 40;
    let config =
        IbkrHistoricalDataConfig::default().with_request_timeout(Some(Duration::from_millis(1)));

    let client = tokio::task::spawn_blocking(move || {
        IbkrHistoricalData::connect_with(&url, client_id, config)
    })
    .await
    .expect("task join")
    .expect("connection failed");

    let request = HistoricalRequest::daily_trades(aapl_contract(), 365);
    let result = client.fetch_candles(request).await;
    assert_eq!(
        result.unwrap_err(),
        DataError::RequestTimeout {
            operation: "fetch_candles".to_owned()
        }
    );

    // A tick request is read from a subscription, which this library bounds itself.
    let request = HistoricalTickRequest::recent(aapl_contract(), 1000);
    let result = client.fetch_historical_ticks(request).await;
    assert_eq!(
        result.unwrap_err(),
        DataError::RequestTimeout {
            operation: "fetch_historical_ticks".to_owned()
        }
    );
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

    let trades = result.unwrap().ticks;
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

    let quotes = result.unwrap().ticks;
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
        Ok(fetched) => {
            let trades = fetched.ticks;
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

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 10);

    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let result = init_stream(config, registry, subscriptions).await;

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

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 11);

    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let mut stream = init_stream(config, registry, subscriptions)
        .await
        .expect("stream init failed");

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

/// Tier 1: delayed option L1 quotes on many lines at once (FREE: delayed data needs no OPRA
/// subscription).
///
/// Streams `IBKR_OPTION_LINES` (default 40) AAPL calls around the money on one expiration with
/// delayed-frozen data, so it runs outside US market hours too (the last delayed quotes from the
/// close). Every line must stay unrefused (a paper account gets no market data while the same
/// user is logged in to a live session elsewhere, which fails it as `MarketDataNotAvailable`
/// 10197), most lines must quote, and no quoted price may be negative: IB's "no quote" price of
/// -1 must withdraw the side instead.
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_option_quotes_delayed() {
    use std::collections::HashMap;

    init_logging();

    let lines: usize = std::env::var("IBKR_OPTION_LINES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(40);

    let url = format!("127.0.0.1:{}", test_port());
    let resolver =
        IbkrHistoricalData::connect(&url, test_client_id_base() + 34).expect("connection failed");
    let calls = resolve_aapl_calls_near_the_money(&resolver, lines).await;
    resolver.disconnect();

    let registry = ContractRegistry::new();
    let subscriptions: Vec<_> = calls
        .into_iter()
        .map(|call| {
            let name = format!(
                "AAPL {} {}C",
                call.last_trade_date_or_contract_month, call.strike
            );
            registry.register_by_name_only(name.as_str().into(), call);
            IbkrSubscription::quotes(name.clone(), name.as_str().into())
        })
        .collect();

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 35)
        .with_market_data_type(IbkrMarketDataType::DelayedFrozen);
    let mut stream = init_stream(config, Arc::new(registry), subscriptions)
        .await
        .expect("stream init failed");

    let mut quotes: HashMap<String, usize> = HashMap::new();
    let mut errors = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    let DataKind::OrderBookL1(l1) = &event.kind else {
                        continue;
                    };
                    for level in l1.best_bid.iter().chain(l1.best_ask.iter()) {
                        assert!(
                            level.price >= rust_decimal::Decimal::ZERO,
                            "{}: quoted at {}",
                            event.instrument,
                            level.price
                        );
                    }
                    *quotes.entry(event.instrument).or_default() += 1;
                }
                Err(error) => errors.push(error),
            }
        }
    })
    .await;
    drop(stream);

    println!(
        "{} of {lines} lines quoted; errors: {errors:?}",
        quotes.len()
    );
    let refusals: Vec<_> = errors
        .iter()
        .filter(|error| {
            matches!(
                error,
                DataError::SubscriptionLimitReached { .. }
                    | DataError::MarketDataNotAvailable { .. }
            )
        })
        .collect();
    assert!(refusals.is_empty(), "lines refused: {refusals:?}");
    assert!(
        quotes.len() * 2 >= lines,
        "only {} of {lines} lines quoted within 30 s",
        quotes.len()
    );
}

/// A TCP proxy on a free local port that forwards every connection to TWS/Gateway on
/// `gateway_port`, and can cut every connection open through it at once.
struct CuttableProxy {
    port: u16,
    cut: tokio::sync::watch::Sender<u64>,
}

impl CuttableProxy {
    async fn start(gateway_port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy bind");
        let port = listener.local_addr().expect("proxy address").port();
        let (cut, _) = tokio::sync::watch::channel(0_u64);
        let cuts = cut.clone();
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                // A receiver made now has seen every cut so far, so only a later one ends this.
                let mut cut = cuts.subscribe();
                tokio::spawn(async move {
                    let Ok(mut outbound) =
                        tokio::net::TcpStream::connect(("127.0.0.1", gateway_port)).await
                    else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = cut.changed() => println!("proxy: connection cut"),
                    }
                });
            }
        });
        Self { port, cut }
    }

    /// Close every connection open through the proxy. New ones are still accepted.
    fn cut(&self) {
        self.cut.send_modify(|cuts| *cuts += 1);
    }
}

/// Tier 0 (FREE: delayed-frozen data needs no market data subscription)
///
/// Streams AAPL quotes through a local proxy, cuts the socket once a quote has arrived, and
/// requires the stream to report [`DataError::ConnectionLost`], then `Reconnecting`, then quote
/// again on its own: a quote after the reconnect proves the subscription was made anew. Failed
/// attempts in between (such as IB still holding the client ID) are allowed.
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_reconnects_after_the_socket_is_cut() {
    init_logging();

    let proxy = CuttableProxy::start(test_port()).await;
    let config = IbkrStreamConfig::new("127.0.0.1", proxy.port, test_client_id_base() + 36)
        .with_market_data_type(IbkrMarketDataType::DelayedFrozen);
    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL".into(), aapl_contract());
    let subscriptions = vec![IbkrSubscription::quotes("AAPL".to_string(), "AAPL".into())];

    let mut stream = IbkrMarketStream::init(
        config,
        Arc::new(registry),
        subscriptions,
        MarketStreamOptions::default(),
    )
    .await
    .expect("stream init failed");
    let is_quote = |event: &Event<ExchangeId, Result<MarketEvent<String, DataKind>, DataError>>| {
        matches!(
            event,
            Event::Item(Ok(MarketEvent {
                kind: DataKind::OrderBookL1(_),
                ..
            }))
        )
    };

    let first = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = stream.next().await {
            if is_quote(&event) {
                return;
            }
            println!("before the cut: {event:?}");
        }
        panic!("the stream ended");
    })
    .await;
    assert!(first.is_ok(), "no quote within 30 s before the cut");

    proxy.cut();

    let mut seen = Vec::new();
    let after = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(event) = stream.next().await {
            let quote = is_quote(&event);
            if !quote {
                println!("after the cut: {event:?}");
            }
            let reconnected = seen
                .iter()
                .any(|seen| matches!(seen, Event::Reconnecting(ExchangeId::Ibkr)));
            seen.push(event);
            if quote && reconnected {
                return;
            }
        }
        panic!("the stream ended");
    })
    .await;
    drop(stream);

    let lost = seen
        .iter()
        .position(|event| matches!(event, Event::Item(Err(DataError::ConnectionLost { .. }))));
    let reconnecting = seen
        .iter()
        .position(|event| matches!(event, Event::Reconnecting(ExchangeId::Ibkr)));
    assert!(
        after.is_ok(),
        "no quote within 90 s after the cut: {seen:?}"
    );
    assert!(
        matches!((lost, reconnecting), (Some(lost), Some(reconnecting)) if lost < reconnecting),
        "expected ConnectionLost, then Reconnecting: {seen:?}"
    );
    let reconnects = seen
        .iter()
        .filter(|event| matches!(event, Event::Reconnecting(_)))
        .count();
    assert_eq!(reconnects, 1, "one reconnect per cut: {seen:?}");
}

/// Tier 2: L2 Market Depth (PAID — varies by exchange)
#[serial]
#[tokio::test]
#[ignore]
async fn test_market_stream_depth() {
    init_logging();

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 12);

    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL".into(), aapl_contract());
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL".into(),
        key: "AAPL".to_string(),
        kind: IbkrSubscriptionKind::Depth { rows: 5 },
    }];

    let mut stream = init_stream(config, registry, subscriptions)
        .await
        .expect("stream init failed");

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

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 13);

    let registry = Arc::new(ContractRegistry::new());

    let subscriptions = vec![IbkrSubscription {
        instrument: "UNKNOWN_SYMBOL".into(),
        key: "UNKNOWN".to_string(),
        kind: IbkrSubscriptionKind::Quotes,
    }];

    let result = init_stream(config, registry, subscriptions).await;

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

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 14);

    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL".into(), aapl_contract());
    registry.register_by_name_only("MSFT".into(), Contract::stock("MSFT").build());
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

    let result = init_stream(config, registry, subscriptions).await;

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
    registry
        .register("AAPL".into(), first.contract.clone())
        .unwrap();

    assert_eq!(registry.len(), 1);
    assert!(registry.get_contract(&"AAPL".into()).is_some());
    assert!(
        registry
            .get_name_by_con_id(first.contract.contract_id)
            .is_some()
    );
}

// ============================================================================
// Option Greeks Calculator Tests — Tier 1: US Real-Time (FREE)
// ============================================================================
// Note: These are calculator functions, not data fetches. They don't require
// OPRA subscription — they compute Greeks from user-provided inputs.

/// AAPL underlying contract ID on IBKR.
///
/// This is a stable identifier — the same value is used as the canonical AAPL
/// conId throughout `exchange::ibkr::options`. `reqSecDefOptParams` (option
/// chain) and contract qualification require a real underlying conId; passing
/// `0` is rejected by IBKR with `[321] Invalid contract id`.
const AAPL_UNDERLYING_CONID: i32 = 265598;

/// Resolve a currently-listed, near-the-money AAPL call option from the live
/// option chain.
///
/// The previous helper hardcoded an expiration date that had to be hand-edited
/// before every run; once it lapsed, IBKR rejected the contract with
/// `[200] No security definition has been found`. Deriving the expiration and
/// strike from the live chain keeps the option-calculator tests valid over time
/// with no manual upkeep, and only uses values IBKR itself reports as listed.
async fn resolve_aapl_call_option(client: &IbkrHistoricalData) -> Contract {
    resolve_aapl_calls_near_the_money(client, 1)
        .await
        .pop()
        .expect("one AAPL call")
}

/// Resolve `count` listed AAPL calls on one expiration, at the strikes nearest the median strike
/// (about at the money). See [`resolve_aapl_call_option`] for how the series is chosen.
async fn resolve_aapl_calls_near_the_money(
    client: &IbkrHistoricalData,
    count: usize,
) -> Vec<Contract> {
    use chrono::{Datelike, Duration as ChronoDuration, Utc};
    use rust_decimal::prelude::ToPrimitive;

    // `fut_fop_exchange` must be empty to get every exchange's parameters; a
    // routing exchange like "SMART" filters the result to zero rows.
    let chains = client
        .fetch_option_chain("AAPL", None, SecurityType::Stock, AAPL_UNDERLYING_CONID)
        .await
        .expect("fetch AAPL option chain to resolve a valid option contract");

    // Prefer the standard SMART "AAPL" trading class (densest, most reliable
    // strike/expiration coverage); fall back to any returned chain. Partial
    // results are acceptable here: even a truncated enumeration is usable as
    // long as it contains one workable entry (each entry is complete in
    // isolation), which is exactly what `OptionChainResult` preserves.
    let chain = chains
        .entries
        .iter()
        .find(|c| c.exchange == "SMART" && c.trading_class == "AAPL")
        .or_else(|| chains.entries.first())
        .expect("at least one AAPL option chain entry");

    // Earliest expiration at least ~2 weeks out: avoids about-to-expire
    // contracts while staying on a standard, well-populated series.
    let cutoff = Utc::now().date_naive() + ChronoDuration::days(14);
    let expiration = chain
        .expirations
        .iter()
        .filter(|d| **d > cutoff)
        .min()
        .copied()
        .expect("a future AAPL option expiration in the chain");

    // The median strike ≈ at-the-money; take the `count` strikes around it. A strike listed in
    // the chain's parameters may still be missing from this expiration, so a test using many
    // tolerates a few that resolve to nothing.
    let mut strikes = chain.strikes.clone();
    strikes.sort();
    assert!(
        count <= strikes.len(),
        "{count} calls asked, {} strikes listed",
        strikes.len()
    );
    let first = (strikes.len() / 2)
        .saturating_sub(count / 2)
        .min(strikes.len() - count);

    strikes[first..first + count]
        .iter()
        .map(|strike| {
            Contract::call("AAPL")
                .strike(strike.to_f64().expect("strike Decimal converts to f64"))
                .expires_on(
                    expiration.year() as u16,
                    expiration.month() as u8,
                    expiration.day() as u8,
                )
                .build()
        })
        .collect()
}

/// Shared AAPL call option, resolved from the live chain exactly once per run.
static AAPL_CALL_OPTION: std::sync::OnceLock<Contract> = std::sync::OnceLock::new();

/// Return the shared near-the-money AAPL call, resolving it from the live option
/// chain on first use and caching it for the rest of the run.
///
/// Without this, every option test re-issued `reqSecDefOptParams` against the
/// live gateway just to pick a valid contract — a pacing/rate-limit risk. The
/// resolution uses a dedicated `client_id` (base+99) so the one-time chain fetch
/// never collides with a test's own connection. Tests are `#[serial]`, so a
/// plain `OnceLock` is race-free here — no async init primitive is needed.
///
/// If resolution panics, the short-lived client is dropped without an explicit
/// `disconnect()`; the connection is reclaimed when the test process exits,
/// which is acceptable for this `#[ignore]` live-gateway helper.
async fn aapl_call_option() -> Contract {
    if let Some(option) = AAPL_CALL_OPTION.get() {
        return option.clone();
    }
    let url = format!("127.0.0.1:{}", test_port());
    let client =
        IbkrHistoricalData::connect(&url, test_client_id_base() + 99).expect("connection failed");
    let option = resolve_aapl_call_option(&client).await;
    client.disconnect();
    AAPL_CALL_OPTION.get_or_init(|| option).clone()
}

#[serial]
#[tokio::test]
#[ignore]
async fn test_calculate_theoretical_greeks() {
    init_logging();

    let url = format!("127.0.0.1:{}", test_port());
    let client_id = test_client_id_base() + 30;

    let client = IbkrHistoricalData::connect(&url, client_id).expect("connection failed");

    let option = aapl_call_option().await;
    // Price the option at-the-money (underlying == strike) so the inputs are
    // internally consistent regardless of where AAPL is trading at run time.
    let underlying_price = option.strike;

    println!("Calculating theoretical Greeks for AAPL call option...");
    println!("  Strike: {}", option.strike);
    println!("  Using volatility: 25% (0.25)");
    println!("  Using underlying price: ${underlying_price:.2}");

    let greeks = client
        .calculate_theoretical_greeks(&option, 0.25, underlying_price)
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

    let option = aapl_call_option().await;
    // At-the-money (underlying == strike): intrinsic value is zero, so any
    // positive option price yields a well-defined implied volatility. ~5% of
    // the strike is a realistic ATM premium for a near-term option.
    let underlying_price = option.strike;
    let option_price = option.strike * 0.05;

    println!("Calculating implied volatility for AAPL call option...");
    println!("  Strike: {}", option.strike);
    println!("  Using option price: ${option_price:.2}");
    println!("  Using underlying price: ${underlying_price:.2}");

    let greeks = client
        .calculate_implied_volatility(&option, option_price, underlying_price)
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

    // This test intentionally opts out of the `aapl_call_option()` cache: it is
    // the test for `fetch_option_chain` itself, so it must exercise a real call.
    //
    // IBKR's `reqSecDefOptParams` requires a real underlying conId (`0` is
    // rejected with `[321] Invalid contract id`) and an empty `fut_fop_exchange`
    // to return every exchange's parameters (a routing exchange like "SMART"
    // filters the result to zero rows).
    let chains = client
        .fetch_option_chain("AAPL", None, SecurityType::Stock, AAPL_UNDERLYING_CONID)
        .await;

    match chains {
        Ok(result) => {
            let entries = &result.entries;
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

            // The pre-`OptionChainResult` API surfaced a mid-stream IB error as
            // a bare `Err`; assert the flag so a truncated (but non-empty)
            // enumeration still fails this test with the recorded reason.
            assert!(
                result.truncation_error.is_none(),
                "option chain enumeration was truncated: {:?}",
                result.truncation_error
            );

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
// Real-Time Option Greeks Streaming Tests — Tier 3: OPRA (PAID)
// ============================================================================

#[serial]
#[tokio::test]
#[ignore]
async fn test_option_greeks_stream() {
    init_logging();

    let config = IbkrStreamConfig::new("127.0.0.1", test_port(), test_client_id_base() + 33);

    // Resolve a currently-listed option from the shared once-per-run cache
    // before opening the streaming connection.
    let option = aapl_call_option().await;

    // The stream looks contracts up by name only, and the chain's contract carries no contract ID,
    // which `register` requires.
    let registry = ContractRegistry::new();
    registry.register_by_name_only("AAPL_CALL".into(), option);
    let registry = Arc::new(registry);

    let subscriptions = vec![IbkrSubscription {
        instrument: "AAPL_CALL".into(),
        key: "AAPL_CALL".to_string(),
        kind: IbkrSubscriptionKind::OptionGreeks,
    }];

    let result = init_stream(config, registry, subscriptions).await;

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
