//! Hyperliquid spot market data streaming example.
//!
//! Demonstrates how to subscribe to public trades and L2 order book snapshots
//! from Hyperliquid spot markets. Exits after receiving a few events.
//!
//! Run with: `cargo run --example hyperliquid_spot_market_data --features hyperliquid`
//!
//! # Spot Market Subscriptions
//!
//! Hyperliquid spot uses `@{index}` format for WebSocket subscriptions.
//! Use [`resolve_spot_pair`] to convert pair names to the required format:
//!
//! ```ignore
//! let coin = resolve_spot_pair("hype", "usdc").await?; // "@107"
//! ```

// Example binary: panics are acceptable for demonstration code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rustrade_data::{
    exchange::hyperliquid::{HyperliquidSpot, resolve_spot_pair},
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::{book::OrderBooksL2, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use tracing::{info, warn};

const MAX_EVENTS: usize = 10;

#[tokio::main]
async fn main() {
    init_logging();

    info!("Subscribing to Hyperliquid SPOT market data...");

    // Resolve pair name to @index format (fetches spotMeta on first call)
    let hype_usdc = resolve_spot_pair("hype", "usdc")
        .await
        .expect("Failed to resolve HYPE/USDC spot pair");
    info!("Resolved HYPE/USDC -> {}", hype_usdc);

    // Subscribe to HYPE/USDC spot trades
    let trades = Streams::<PublicTrades>::builder()
        .subscribe([(
            HyperliquidSpot,
            hype_usdc.as_str(),
            "usdc",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .unwrap();

    // Subscribe to HYPE/USDC spot L2 order book
    let books = Streams::<OrderBooksL2>::builder()
        .subscribe([(
            HyperliquidSpot,
            hype_usdc.as_str(),
            "usdc",
            MarketDataInstrumentKind::Spot,
            OrderBooksL2,
        )])
        .init()
        .await
        .unwrap();

    // Merge all streams
    let mut trades_stream = trades
        .select_all()
        .with_error_handler(|error| warn!(?error, "Trade stream error"));

    let mut books_stream = books
        .select_all()
        .with_error_handler(|error| warn!(?error, "Book stream error"));

    info!(
        "Subscribed to Hyperliquid {} (HYPE/USDC) spot trades + L2 books",
        hype_usdc
    );
    info!("Receiving {} events then exiting...", MAX_EVENTS);

    let mut event_count = 0;

    // Process both streams concurrently, exit after MAX_EVENTS
    while event_count < MAX_EVENTS {
        tokio::select! {
            Some(trade) = trades_stream.next() => {
                info!("Spot Trade: {:?}", trade);
                event_count += 1;
            }
            Some(book) = books_stream.next() => {
                info!("Spot Book: {:?}", book);
                event_count += 1;
            }
            else => break,
        }
    }

    info!("Received {} events, exiting", event_count);
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init()
}
