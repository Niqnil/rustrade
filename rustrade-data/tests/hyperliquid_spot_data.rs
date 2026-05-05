//! Hyperliquid Spot Market Data Integration Tests
//!
//! These tests verify connectivity and data reception from Hyperliquid spot markets.
//!
//! # Running
//!
//! ```bash
//! # Run all Hyperliquid spot integration tests
//! cargo test --test hyperliquid_spot_data --features hyperliquid -- --ignored
//!
//! # Run specific test
//! cargo test --test hyperliquid_spot_data --features hyperliquid test_spot_trade_stream_connection -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` to avoid CI failures and rate limiting.
//!
//! # Spot vs Perpetuals
//!
//! - Uses `HyperliquidSpot` connector instead of `Hyperliquid`
//! - Market format: `@{index}` where index is from `spotMeta` API (e.g., "@107" for HYPE)
//!   - Exception: PURR uses literal "PURR/USDC"
//!   - Get indices via: `curl -X POST https://api.hyperliquid.xyz/info -d '{"type":"spotMeta"}'`
//! - Same WebSocket protocol as perpetuals

#![cfg(feature = "hyperliquid")]
// Integration test: panics on bad input are acceptable.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rust_decimal::Decimal;
use rustrade_data::{
    exchange::hyperliquid::HyperliquidSpot,
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
// WebSocket Stream Tests - Trades
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_trade_stream_connection() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to init spot trade stream: {:?}",
        streams.err()
    );
    tracing::info!("Spot trade stream connected");
}

#[tokio::test]
#[ignore]
async fn test_spot_trade_stream_receives_data() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);

    tracing::info!("Waiting for spot trade data (60 second timeout)...");
    tracing::info!("Note: Spot markets may have lower volume than perps");

    while tokio::time::Instant::now() < deadline {
        let timeout = tokio::time::timeout(StdDuration::from_secs(30), stream.next()).await;

        match timeout {
            Ok(Some(Event::Item(trade))) => {
                tracing::info!(?trade, "Received spot trade");
                assert!(trade.kind.price > Decimal::ZERO, "Invalid trade price");
                assert!(trade.kind.amount > Decimal::ZERO, "Invalid trade amount");
                return;
            }
            Ok(Some(Event::Reconnecting(info))) => {
                tracing::warn!(?info, "Stream reconnecting");
            }
            Ok(None) => {
                panic!("Stream ended without data");
            }
            Err(_) => {
                tracing::warn!("Timeout waiting for trade, retrying...");
            }
        }
    }

    panic!("No spot trade events received within timeout (spot markets may have low volume)");
}

// ============================================================================
// WebSocket Stream Tests - L2 Order Book
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_l2_book_stream_connection() {
    init_logging();

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            OrderBooksL2,
        )])
        .init()
        .await;

    assert!(
        streams.is_ok(),
        "Failed to init spot L2 book stream: {:?}",
        streams.err()
    );
    tracing::info!("Spot L2 book stream connected");
}

#[tokio::test]
#[ignore]
async fn test_spot_l2_book_stream_receives_data() {
    init_logging();

    let streams = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            OrderBooksL2,
        )])
        .init()
        .await
        .expect("Failed to init stream");

    let mut stream = streams
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Stream error"));

    let timeout = tokio::time::timeout(StdDuration::from_secs(30), stream.next()).await;

    let event = timeout.expect("Timeout waiting for spot L2 book data");
    let book_event = event.expect("Stream ended without data");
    tracing::info!(?book_event, "Received spot L2 book");
}

// ============================================================================
// Multiple Streams
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_combined_streams() {
    init_logging();

    // Subscribe to both trades and books for BTC/USDC spot
    let trades = Streams::<PublicTrades>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("Failed to init trade stream");

    let books = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            HyperliquidSpot,
            "@107",
            "usdc",
            MarketDataInstrumentKind::Spot,
            OrderBooksL2,
        )])
        .init()
        .await
        .expect("Failed to init book stream");

    let mut trades_stream = trades
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Trade stream error"));

    let mut books_stream = books
        .select_all()
        .with_error_handler(|e| tracing::warn!(?e, "Book stream error"));

    let mut trade_seen = false;
    let mut book_seen = false;
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(60);

    tracing::info!("Waiting for both @107 (HYPE/USDC) spot trade and book data...");

    while !(trade_seen && book_seen) && tokio::time::Instant::now() < deadline {
        tokio::select! {
            Some(Event::Item(_)) = trades_stream.next() => {
                trade_seen = true;
                tracing::info!("Received spot trade");
            }
            Some(Event::Item(_)) = books_stream.next() => {
                book_seen = true;
                tracing::info!("Received spot book update");
            }
            else => break,
        }
    }

    // Book updates are more frequent, trades may be sparse
    assert!(book_seen, "No spot book updates received within timeout");
    if !trade_seen {
        tracing::warn!("No spot trades received (low volume is normal for spot)");
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_spot_invalid_pair_handling() {
    init_logging();

    // Try to subscribe to a non-existent spot pair
    let result = Streams::<PublicTrades>::builder()
        .subscribe([(
            HyperliquidSpot,
            "invalidcoin",
            "usdc",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await;

    // Should either fail to connect or receive no data
    // Hyperliquid may accept the subscription but send no data
    match result {
        Ok(streams) => {
            let mut stream = streams
                .select_all()
                .with_error_handler(|e| tracing::info!(?e, "Expected stream error"));

            let timeout = tokio::time::timeout(StdDuration::from_secs(5), stream.next()).await;

            // Should timeout with no data for invalid pair
            assert!(
                timeout.is_err() || matches!(timeout, Ok(None)),
                "Expected no data for invalid spot pair"
            );
            tracing::info!("Invalid pair correctly produced no data");
        }
        Err(e) => {
            tracing::info!(?e, "Invalid pair correctly rejected at subscribe time");
        }
    }
}
