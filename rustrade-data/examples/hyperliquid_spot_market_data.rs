//! Hyperliquid spot market data streaming example.
//!
//! Demonstrates how to subscribe to public trades and L2 order book snapshots
//! from Hyperliquid spot markets. Exits after receiving a few events.
//!
//! Run with: `cargo run --example hyperliquid_spot_market_data --features hyperliquid`
//!
//! # Spot Market Subscriptions
//!
//! Hyperliquid names every spot pair but PURR/USDC by its index (`@107` is HYPE/USDC).
//! [`HyperliquidMeta::spot_pair`] finds a pair's coin from its tokens.

// Example binary: panics are acceptable for demonstration code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rustrade_data::{
    exchange::hyperliquid::{HyperliquidMeta, HyperliquidSpot, Network},
    instrument::MarketInstrumentData,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscriber::WebSocketSubscriber,
    subscription::{Subscription, book::OrderBooksL2, trade::PublicTrades},
};
use tracing::{info, warn};

const MAX_EVENTS: usize = 10;

#[tokio::main]
async fn main() {
    init_logging();

    info!("Subscribing to Hyperliquid SPOT market data...");

    // Read the spot pairs and find HYPE/USDC by its tokens
    let meta = HyperliquidMeta::fetch(Network::Mainnet, &[])
        .await
        .expect("Failed to read Hyperliquid market metadata");
    let pair = meta
        .spot_pair("HYPE", "USDC")
        .expect("Hyperliquid lists no HYPE/USDC pair");
    info!("HYPE/USDC is subscribed as {}", pair.coin());
    let hype_usdc = MarketInstrumentData::hyperliquid_spot("hype_usdc", pair);

    // Subscribe to HYPE/USDC spot trades
    let trades = Streams::<PublicTrades>::builder()
        .subscribe(
            WebSocketSubscriber,
            [Subscription {
                exchange: HyperliquidSpot,
                instrument: hype_usdc.clone(),
                kind: PublicTrades,
            }],
        )
        .init()
        .await
        .unwrap();

    // Subscribe to HYPE/USDC spot L2 order book
    let books = Streams::<OrderBooksL2>::builder()
        .subscribe(
            WebSocketSubscriber,
            [Subscription {
                exchange: HyperliquidSpot,
                instrument: hype_usdc.clone(),
                kind: OrderBooksL2,
            }],
        )
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
        pair.coin()
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
