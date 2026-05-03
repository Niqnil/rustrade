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
use rustrade_data::{
    exchange::alpaca::{AlpacaCrypto, AlpacaIex},
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
            assert!(trade.kind.price > 0.0, "Invalid trade price");
            assert!(trade.kind.amount > 0.0, "Invalid trade amount");
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
            assert!(quote.kind.bid_price > 0.0, "Invalid bid price");
            assert!(quote.kind.ask_price > 0.0, "Invalid ask price");
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
                assert!(trade.kind.price > 0.0, "Invalid trade price");
                assert!(trade.kind.amount > 0.0, "Invalid trade amount");
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
                assert!(quote.kind.bid_price > 0.0, "Invalid bid price");
                assert!(quote.kind.ask_price > 0.0, "Invalid ask price");
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
