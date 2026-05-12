//! Alpaca Market Data Integration Tests
//!
//! These tests verify connectivity and data reception from Alpaca market data streams.
//!
//! # Prerequisites
//!
//! 1. Alpaca account (https://app.alpaca.markets)
//! 2. Environment variables set (see .env.template):
//!    - ALPACA_API_KEY: API key
//!    - ALPACA_SECRET_KEY: Secret key
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env
//! source .env
//!
//! # Run all Alpaca data integration tests
//! cargo test --test alpaca_data --features alpaca -- --ignored
//!
//! # Run specific test
//! cargo test --test alpaca_data --features alpaca test_crypto_trade_stream -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures without credentials.
//!
//! # Market Hours Note
//!
//! US equity market hours are 9:30 AM - 4:00 PM ET (9:30 PM - 4:00 AM SGT next day).
//! The IEX feed only provides trade data during market hours. Tests that wait for
//! trade events (`test_iex_*_receives_data`) may timeout outside market hours.
//! Connection and subscription tests work anytime.
//!
//! Crypto streams (BTC/USD, etc.) are available 24/7 and are the primary validation.

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rust_decimal::Decimal;
use rustrade_data::{
    exchange::alpaca::{AlpacaCrypto, AlpacaIex, AlpacaSip},
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscription::{quote::Quotes, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
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

// ============================================================================
// Crypto Stream Tests (24/7 availability - primary validation)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_crypto_trade_stream_connection() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaCrypto::default(),
            "btc",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to crypto trade stream: {:?}",
        streams.err()
    );
    tracing::info!("Crypto trade stream connected and subscribed");
}

#[tokio::test]
#[ignore]
async fn test_crypto_trade_stream_receives_data() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaCrypto::default(),
            "btc",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init crypto stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;
        assert!(timeout.is_ok(), "Timeout waiting for crypto trade data");

        let event = timeout.unwrap();
        assert!(event.is_some(), "Stream ended without data");

        if let Event::Item(trade) = event.unwrap() {
            tracing::info!(?trade, "Received crypto trade");
            assert!(trade.kind.price > Decimal::ZERO, "Invalid trade price");
            assert!(trade.kind.amount > Decimal::ZERO, "Invalid trade amount");
            return;
        }
    }

    panic!("No crypto trade events received within timeout");
}

#[tokio::test]
#[ignore]
async fn test_crypto_quote_stream_connection() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaCrypto::default(),
            "btc",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to crypto quote stream: {:?}",
        streams.err()
    );
    tracing::info!("Crypto quote stream connected and subscribed");
}

#[tokio::test]
#[ignore]
async fn test_crypto_quote_stream_receives_data() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaCrypto::default(),
            "btc",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await
        .expect("Failed to init crypto quote stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;
        assert!(timeout.is_ok(), "Timeout waiting for crypto quote data");

        let event = timeout.unwrap();
        assert!(event.is_some(), "Stream ended without data");

        if let Event::Item(quote) = event.unwrap() {
            tracing::info!(?quote, "Received crypto quote");
            assert!(quote.kind.bid_price > Decimal::ZERO, "Invalid bid price");
            assert!(quote.kind.ask_price > Decimal::ZERO, "Invalid ask price");
            assert!(
                quote.kind.ask_price >= quote.kind.bid_price,
                "Ask < Bid (crossed market)"
            );
            return;
        }
    }

    panic!("No crypto quote events received within timeout");
}

#[tokio::test]
#[ignore]
async fn test_crypto_multiple_symbols() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([
            (
                AlpacaCrypto::default(),
                "btc",
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ),
            (
                AlpacaCrypto::default(),
                "eth",
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ),
        ])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to subscribe to multiple crypto symbols: {:?}",
        streams.err()
    );

    let mut stream = streams
        .unwrap()
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let mut btc_seen = false;
    let mut eth_seen = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

    while !(btc_seen && eth_seen) && tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;
        if let Ok(Some(Event::Item(event))) = timeout {
            match event.instrument.base.as_ref() {
                "btc" => {
                    btc_seen = true;
                    tracing::info!("Received BTC trade");
                }
                "eth" => {
                    eth_seen = true;
                    tracing::info!("Received ETH trade");
                }
                _ => {}
            }
        }
    }

    assert!(btc_seen, "No BTC trades received within timeout");
    assert!(eth_seen, "No ETH trades received within timeout");
}

// ============================================================================
// IEX Equity Stream Tests (market hours only for data reception)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_iex_trade_stream_connection() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaIex::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to IEX trade stream: {:?}",
        streams.err()
    );
    tracing::info!("IEX trade stream connected and subscribed");
}

#[tokio::test]
#[ignore]
async fn test_iex_quote_stream_connection() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaIex::default(),
            "aapl",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to IEX quote stream: {:?}",
        streams.err()
    );
    tracing::info!("IEX quote stream connected and subscribed");
}

#[tokio::test]
#[ignore]
async fn test_iex_multiple_symbols_subscription() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([
            (
                AlpacaIex::default(),
                "spy",
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ),
            (
                AlpacaIex::default(),
                "aapl",
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ),
        ])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to subscribe to multiple IEX symbols: {:?}",
        streams.err()
    );
    tracing::info!("IEX multi-symbol subscription confirmed");
}

/// Test receiving trade data from IEX.
///
/// NOTE: This test may timeout outside US market hours (9:30 AM - 4:00 PM ET).
/// The IEX feed only provides trade data during market hours.
#[tokio::test]
#[ignore]
async fn test_iex_trade_stream_receives_data() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaIex::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init IEX stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    tracing::info!("Waiting for IEX trade data (may timeout outside market hours)...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;

        match timeout {
            Ok(Some(Event::Item(trade))) => {
                tracing::info!(?trade, "Received IEX trade");
                assert!(trade.kind.price > Decimal::ZERO, "Invalid trade price");
                assert!(trade.kind.amount > Decimal::ZERO, "Invalid trade amount");
                return;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("Stream ended unexpectedly"),
            Err(_) => {
                tracing::warn!("Timeout waiting for IEX data - may be outside market hours");
                continue;
            }
        }
    }

    panic!(
        "No IEX trade events received within timeout. \
         If outside US market hours (9:30 AM - 4:00 PM ET), this is expected."
    );
}

/// Test receiving quote data from IEX.
///
/// NOTE: This test may timeout outside US market hours.
#[tokio::test]
#[ignore]
async fn test_iex_quote_stream_receives_data() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaIex::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await
        .expect("Failed to init IEX quote stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    tracing::info!("Waiting for IEX quote data (may timeout outside market hours)...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;

        match timeout {
            Ok(Some(Event::Item(quote))) => {
                tracing::info!(?quote, "Received IEX quote");
                assert!(quote.kind.bid_price > Decimal::ZERO, "Invalid bid price");
                assert!(quote.kind.ask_price > Decimal::ZERO, "Invalid ask price");
                return;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("Stream ended unexpectedly"),
            Err(_) => {
                tracing::warn!("Timeout waiting for IEX quote - may be outside market hours");
                continue;
            }
        }
    }

    panic!(
        "No IEX quote events received within timeout. \
         If outside US market hours (9:30 AM - 4:00 PM ET), this is expected."
    );
}

// ============================================================================
// SIP Equity Stream Tests (requires SIP subscription)
// ============================================================================

/// Test SIP trade stream connection.
///
/// NOTE: Requires SIP subscription. Will fail with auth error without subscription.
#[tokio::test]
#[ignore]
async fn test_sip_trade_stream_connection() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaSip::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to SIP trade stream: {:?}",
        streams.err()
    );
    tracing::info!("SIP trade stream connected and subscribed");
}

/// Test SIP quote stream connection.
///
/// NOTE: Requires SIP subscription.
#[tokio::test]
#[ignore]
async fn test_sip_quote_stream_connection() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaSip::default(),
            "aapl",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to connect to SIP quote stream: {:?}",
        streams.err()
    );
    tracing::info!("SIP quote stream connected and subscribed");
}

/// Test receiving trade data from SIP (consolidated tape).
///
/// NOTE: Requires SIP subscription. May timeout outside US market hours.
#[tokio::test]
#[ignore]
async fn test_sip_trade_stream_receives_data() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            AlpacaSip::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init SIP stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    tracing::info!("Waiting for SIP trade data (consolidated tape)...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;

        match timeout {
            Ok(Some(Event::Item(trade))) => {
                tracing::info!(?trade, "Received SIP trade");
                assert!(trade.kind.price > Decimal::ZERO, "Invalid trade price");
                assert!(trade.kind.amount > Decimal::ZERO, "Invalid trade amount");
                return;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("Stream ended unexpectedly"),
            Err(_) => {
                tracing::warn!("Timeout waiting for SIP data - may be outside market hours");
                continue;
            }
        }
    }

    panic!(
        "No SIP trade events received within timeout. \
         If outside US market hours (9:30 AM - 4:00 PM ET), this is expected."
    );
}

/// Test receiving quote data from SIP (consolidated NBBO).
///
/// NOTE: Requires SIP subscription. May timeout outside US market hours.
#[tokio::test]
#[ignore]
async fn test_sip_quote_stream_receives_data() {
    init_logging();

    let streams = Streams::<Quotes>::builder()
        .subscribe([(
            AlpacaSip::default(),
            "spy",
            "usd",
            MarketDataInstrumentKind::Spot,
            Quotes,
        )])
        .init()
        .await
        .expect("Failed to init SIP quote stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    tracing::info!("Waiting for SIP quote data (consolidated NBBO)...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(Duration::from_secs(30), stream.next()).await;

        match timeout {
            Ok(Some(Event::Item(quote))) => {
                tracing::info!(?quote, "Received SIP quote (NBBO)");
                assert!(quote.kind.bid_price > Decimal::ZERO, "Invalid bid price");
                assert!(quote.kind.ask_price > Decimal::ZERO, "Invalid ask price");
                return;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("Stream ended unexpectedly"),
            Err(_) => {
                tracing::warn!("Timeout waiting for SIP quote - may be outside market hours");
                continue;
            }
        }
    }

    panic!(
        "No SIP quote events received within timeout. \
         If outside US market hours (9:30 AM - 4:00 PM ET), this is expected."
    );
}

// ============================================================================
// Options Market Data Tests (REST API - available 24/7)
// ============================================================================

use chrono::Utc;
use rustrade_data::exchange::alpaca::options::{
    AlpacaOptionContractQuery, AlpacaOptionFeed, AlpacaOptionsClient,
};

/// Test fetching AAPL option contracts.
///
/// Verifies that we can discover option contracts with various filters.
#[tokio::test]
#[ignore]
async fn test_fetch_aapl_option_contracts() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    // Query AAPL options expiring in the next 60 days
    let today = Utc::now().date_naive();
    let sixty_days = today + chrono::Duration::days(60);

    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()])
        .expiration_gte(today)
        .expiration_lte(sixty_days)
        .limit(100);

    let contracts = client
        .fetch_contracts(&query)
        .await
        .expect("Failed to fetch contracts");

    tracing::info!(count = contracts.len(), "Fetched AAPL option contracts");

    assert!(!contracts.is_empty(), "No AAPL options found");

    // Verify contract data quality
    for contract in contracts.iter().take(5) {
        tracing::info!(
            symbol = %contract.symbol,
            expiration = %contract.expiration_date,
            strike = %contract.strike_price,
            option_type = %contract.option_type,
            style = %contract.style,
            "Contract"
        );

        assert!(
            contract.symbol.starts_with("AAPL"),
            "Symbol should start with AAPL"
        );
        assert!(
            contract.expiration_date >= today,
            "Expiration should be >= today"
        );
        assert!(
            contract.expiration_date <= sixty_days,
            "Expiration should be <= 60 days out"
        );
        assert!(
            contract.strike_price > Decimal::ZERO,
            "Strike should be positive"
        );
        assert!(
            contract.option_type == "call" || contract.option_type == "put",
            "Option type should be call or put"
        );
        assert!(
            contract.style == "american" || contract.style == "european",
            "Style should be american or european"
        );
    }
}

/// Test fetching option contracts with call-only filter.
#[tokio::test]
#[ignore]
async fn test_fetch_call_options_only() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    let today = Utc::now().date_naive();
    let thirty_days = today + chrono::Duration::days(30);

    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()])
        .expiration_gte(today)
        .expiration_lte(thirty_days)
        .call_only()
        .limit(50);

    let contracts = client
        .fetch_contracts(&query)
        .await
        .expect("Failed to fetch call options");

    tracing::info!(count = contracts.len(), "Fetched AAPL call options");

    assert!(!contracts.is_empty(), "No AAPL call options found");

    // Verify all are calls
    for contract in &contracts {
        assert_eq!(
            contract.option_type, "call",
            "Expected call option, got {}",
            contract.option_type
        );
    }
}

/// Test fetching option chain snapshots with Greeks.
///
/// Uses the indicative (delayed) feed which is free.
#[tokio::test]
#[ignore]
async fn test_fetch_aapl_chain_snapshot_with_greeks() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    // First get some contracts
    let today = Utc::now().date_naive();
    let thirty_days = today + chrono::Duration::days(30);

    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()])
        .expiration_gte(today)
        .expiration_lte(thirty_days)
        .limit(10);

    let contracts = client
        .fetch_contracts(&query)
        .await
        .expect("Failed to fetch contracts");

    assert!(!contracts.is_empty(), "No contracts to fetch snapshots for");

    // Fetch snapshots for these contracts
    let symbols: Vec<String> = contracts.iter().map(|c| c.symbol.clone()).collect();

    let snapshots = client
        .fetch_snapshots(&symbols, AlpacaOptionFeed::Indicative)
        .await
        .expect("Failed to fetch snapshots");

    tracing::info!(
        requested = symbols.len(),
        received = snapshots.len(),
        "Fetched option snapshots"
    );

    // We might not get snapshots for all symbols (some may not have recent activity)
    // but we should get at least some
    assert!(!snapshots.is_empty(), "No snapshots received");

    // Log snapshot details
    let mut greeks_count = 0;
    for snapshot in &snapshots {
        if snapshot.has_greeks() {
            greeks_count += 1;
            let greeks = snapshot.greeks();
            tracing::info!(
                symbol = %snapshot.symbol,
                delta = ?greeks.delta,
                gamma = ?greeks.gamma,
                theta = ?greeks.theta,
                vega = ?greeks.vega,
                iv = ?greeks.implied_volatility,
                "Snapshot with Greeks"
            );
        }

        if let Some(ref quote) = snapshot.latest_quote {
            tracing::info!(
                symbol = %snapshot.symbol,
                bid = %quote.bid_price,
                ask = %quote.ask_price,
                "Snapshot quote"
            );
            assert!(
                quote.bid_price >= Decimal::ZERO,
                "Bid should be non-negative"
            );
            assert!(
                quote.ask_price >= Decimal::ZERO,
                "Ask should be non-negative"
            );
        }
    }

    tracing::info!(
        total = snapshots.len(),
        with_greeks = greeks_count,
        "Snapshot summary"
    );
}

/// Test the convenience method for fetching entire chain snapshots.
#[tokio::test]
#[ignore]
async fn test_fetch_chain_snapshots_convenience() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    // SPY has a large active option chain (50k+ contracts) — a good stress test
    // for pagination and the 100-symbol batch limit.
    let snapshots = client
        .fetch_chain_snapshots("SPY", AlpacaOptionFeed::Indicative)
        .await
        .expect("Failed to fetch chain snapshots");

    tracing::info!(count = snapshots.len(), "Fetched SPY chain snapshots");

    // SPY has many options, we should get a substantial number
    assert!(!snapshots.is_empty(), "No SPY chain snapshots received");
}

// ============================================================================
// OPRA Options Tests (requires OPRA subscription)
// ============================================================================

/// Test fetching option snapshots with real-time OPRA feed.
///
/// NOTE: Requires OPRA subscription. Will return empty or error without subscription.
#[tokio::test]
#[ignore]
async fn test_fetch_opra_snapshots() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    // Get some AAPL contracts
    let today = Utc::now().date_naive();
    let thirty_days = today + chrono::Duration::days(30);

    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()])
        .expiration_gte(today)
        .expiration_lte(thirty_days)
        .limit(10);

    let contracts = client
        .fetch_contracts(&query)
        .await
        .expect("Failed to fetch contracts");

    assert!(!contracts.is_empty(), "No contracts to fetch snapshots for");

    let symbols: Vec<String> = contracts.iter().map(|c| c.symbol.clone()).collect();

    // Fetch with OPRA (real-time) feed
    let snapshots = client
        .fetch_snapshots(&symbols, AlpacaOptionFeed::Opra)
        .await
        .expect("Failed to fetch OPRA snapshots");

    tracing::info!(
        requested = symbols.len(),
        received = snapshots.len(),
        "Fetched OPRA option snapshots"
    );

    assert!(!snapshots.is_empty(), "No OPRA snapshots received");

    // Verify we got real-time data with Greeks
    let mut greeks_count = 0;
    for snapshot in &snapshots {
        if snapshot.has_greeks() {
            greeks_count += 1;
            let greeks = snapshot.greeks();
            tracing::info!(
                symbol = %snapshot.symbol,
                delta = ?greeks.delta,
                gamma = ?greeks.gamma,
                theta = ?greeks.theta,
                vega = ?greeks.vega,
                iv = ?greeks.implied_volatility,
                "OPRA snapshot with Greeks"
            );
        }
    }

    tracing::info!(
        total = snapshots.len(),
        with_greeks = greeks_count,
        "OPRA snapshot summary"
    );
}

/// Test fetching entire chain with OPRA feed.
///
/// NOTE: Requires OPRA subscription.
#[tokio::test]
#[ignore]
async fn test_fetch_opra_chain_snapshots() {
    init_logging();

    let client = AlpacaOptionsClient::from_env().expect("Failed to create options client");

    let snapshots = client
        .fetch_chain_snapshots("AAPL", AlpacaOptionFeed::Opra)
        .await
        .expect("Failed to fetch OPRA chain snapshots");

    tracing::info!(count = snapshots.len(), "Fetched AAPL OPRA chain snapshots");

    assert!(
        !snapshots.is_empty(),
        "No AAPL OPRA chain snapshots received"
    );
}
