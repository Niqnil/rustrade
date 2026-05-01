//! Hyperliquid Market Data Integration Tests
//!
//! These tests verify connectivity and data reception from Hyperliquid mainnet.
//!
//! # Running
//!
//! ```bash
//! # Run all Hyperliquid integration tests
//! cargo test --test hyperliquid_data --features hyperliquid -- --ignored
//!
//! # Run specific test
//! cargo test --test hyperliquid_data --features hyperliquid test_historical_candles -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures and rate limiting.

#![cfg(feature = "hyperliquid")]
// Integration test: panics on bad input are acceptable.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::Duration;
use futures_util::StreamExt;
use rustrade_data::{
    exchange::hyperliquid::{
        Hyperliquid,
        historical::{CandleInterval, HistoricalRequest, HyperliquidHistoricalData},
    },
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscription::{book::OrderBooksL2, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use std::time::Duration as StdDuration;
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
// Historical Data Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_historical_client_creation() {
    init_logging();

    let client = HyperliquidHistoricalData::new(false).await;
    assert!(
        client.is_ok(),
        "Failed to create historical client: {:?}",
        client.err()
    );
}

#[tokio::test]
#[ignore]
async fn test_historical_candles_hourly() {
    init_logging();

    let client = HyperliquidHistoricalData::new(false)
        .await
        .expect("Failed to create client");

    let request = HistoricalRequest::hourly("BTC", 1);
    let candles = client.fetch_candles(request).await;

    assert!(
        candles.is_ok(),
        "Failed to fetch candles: {:?}",
        candles.err()
    );

    let candles = candles.unwrap();
    assert!(!candles.is_empty(), "No candles returned");

    let first = &candles[0];
    assert!(first.open > 0.0, "Invalid open price");
    assert!(first.high >= first.low, "High < Low");
    assert!(first.volume >= 0.0, "Negative volume");

    tracing::info!(count = candles.len(), "Received hourly candles");
}

#[tokio::test]
#[ignore]
async fn test_historical_candles_daily() {
    init_logging();

    let client = HyperliquidHistoricalData::new(false)
        .await
        .expect("Failed to create client");

    let request = HistoricalRequest::daily("ETH", 7);
    let candles = client.fetch_candles(request).await;

    assert!(
        candles.is_ok(),
        "Failed to fetch daily candles: {:?}",
        candles.err()
    );

    let candles = candles.unwrap();
    assert!(!candles.is_empty(), "No daily candles returned");

    tracing::info!(count = candles.len(), "Received daily candles");
}

#[tokio::test]
#[ignore]
async fn test_historical_candles_all_intervals() {
    init_logging();

    let client = HyperliquidHistoricalData::new(false)
        .await
        .expect("Failed to create client");

    let intervals = [
        CandleInterval::Min15,
        CandleInterval::Hour1,
        CandleInterval::Hour4,
        CandleInterval::Day1,
    ];

    for interval in intervals {
        let end_time = chrono::Utc::now();
        let start_time = end_time - Duration::days(1);

        let request = HistoricalRequest {
            coin: "BTC".to_string(),
            interval,
            start_time,
            end_time,
        };

        let result = client.fetch_candles(request).await;
        assert!(
            result.is_ok(),
            "Failed to fetch {:?} candles: {:?}",
            interval,
            result.err()
        );

        tracing::info!(interval = ?interval, count = result.unwrap().len(), "Interval OK");
    }
}

// ============================================================================
// WebSocket Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_trade_stream_connection() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            Hyperliquid,
            "btc",
            "usdc",
            MarketDataInstrumentKind::Perpetual,
            PublicTrades,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to init trade stream: {:?}",
        streams.err()
    );
    tracing::info!("Trade stream connected");
}

#[tokio::test]
#[ignore]
async fn test_trade_stream_receives_data() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            Hyperliquid,
            "btc",
            "usdc",
            MarketDataInstrumentKind::Perpetual,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(StdDuration::from_secs(10), stream.next()).await;
        assert!(timeout.is_ok(), "Timeout waiting for trade data");

        let event = timeout.unwrap();
        assert!(event.is_some(), "Stream ended without data");

        if let Event::Item(trade) = event.unwrap() {
            tracing::info!(?trade, "Received trade");
            assert!(trade.kind.price > 0.0, "Invalid trade price");
            assert!(trade.kind.amount > 0.0, "Invalid trade amount");
            return;
        }
    }

    panic!("No trade events received within timeout");
}

#[tokio::test]
#[ignore]
async fn test_l2_book_stream_connection() {
    init_logging();

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            Hyperliquid,
            "btc",
            "usdc",
            MarketDataInstrumentKind::Perpetual,
            OrderBooksL2,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to init L2 book stream: {:?}",
        streams.err()
    );
    tracing::info!("L2 book stream connected");
}

#[tokio::test]
#[ignore]
async fn test_l2_book_stream_receives_data() {
    init_logging();

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            Hyperliquid,
            "btc",
            "usdc",
            MarketDataInstrumentKind::Perpetual,
            OrderBooksL2,
        )])
        .init()
        .await
        .expect("Failed to init stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let timeout = tokio::time::timeout(StdDuration::from_secs(30), stream.next()).await;

    assert!(timeout.is_ok(), "Timeout waiting for L2 book data");
    let event = timeout.unwrap();
    assert!(event.is_some(), "Stream ended without data");

    let book_event = event.unwrap();
    tracing::info!(?book_event, "Received L2 book");
}

#[tokio::test]
#[ignore]
async fn test_multiple_symbols_stream() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([
            (
                Hyperliquid,
                "btc",
                "usdc",
                MarketDataInstrumentKind::Perpetual,
                PublicTrades,
            ),
            (
                Hyperliquid,
                "eth",
                "usdc",
                MarketDataInstrumentKind::Perpetual,
                PublicTrades,
            ),
        ])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to init multi-symbol stream: {:?}",
        streams.err()
    );

    let mut stream = streams
        .unwrap()
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let mut btc_seen = false;
    let mut eth_seen = false;
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);

    while !(btc_seen && eth_seen) && tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(StdDuration::from_secs(10), stream.next()).await;
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
// Edge Cases
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_historical_invalid_coin() {
    init_logging();

    let client = HyperliquidHistoricalData::new(false)
        .await
        .expect("Failed to create client");

    let request = HistoricalRequest::hourly("INVALID_COIN_XYZ", 1);
    let result = client.fetch_candles(request).await;

    // Hyperliquid returns empty for unknown coins rather than error
    assert!(
        result.is_ok() && result.unwrap().is_empty(),
        "Expected empty result for invalid coin"
    );
}

#[tokio::test]
#[ignore]
async fn test_historical_testnet() {
    init_logging();

    let client = HyperliquidHistoricalData::new(true).await;
    assert!(
        client.is_ok(),
        "Failed to create testnet client: {:?}",
        client.err()
    );

    let request = HistoricalRequest::hourly("BTC", 1);
    let result = client.unwrap().fetch_candles(request).await;

    // Testnet may have less data but should connect
    assert!(
        result.is_ok(),
        "Testnet candle fetch failed: {:?}",
        result.err()
    );
    tracing::info!(count = result.unwrap().len(), "Testnet candles received");
}
