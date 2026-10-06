//! Hyperliquid market data streaming example.
//!
//! Demonstrates how to subscribe to public trades and L2 order book snapshots
//! from Hyperliquid perpetual futures. Exits after receiving a few events.
//!
//! Trades are subscribed by asset name, which works for an all-capitals coin such as `BTC`.
//! Books are subscribed by the coins [`HyperliquidMeta`] reads, which also covers a mixed-case
//! coin (`kPEPE`) and a builder-deployed (HIP-3) one (`xyz:TSLA`).
//!
//! Run with: `cargo run --example hyperliquid_market_data --features hyperliquid`

// Example binary: panics are acceptable for demonstration code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rustrade_data::{
    exchange::hyperliquid::{Hyperliquid, HyperliquidMeta, Network},
    instrument::MarketInstrumentData,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscriber::WebSocketSubscriber,
    subscription::{Subscription, book::OrderBooksL2, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use tracing::{info, warn};

const MAX_EVENTS: usize = 10;

#[tokio::main]
async fn main() {
    init_logging();

    // Subscribe to BTC and ETH trades on separate connections (high volume)
    let trades = Streams::<PublicTrades>::builder()
        .subscribe(
            WebSocketSubscriber,
            [(
                Hyperliquid,
                "btc",
                "usdc",
                MarketDataInstrumentKind::Perpetual,
                PublicTrades,
            )],
        )
        .subscribe(
            WebSocketSubscriber,
            [(
                Hyperliquid,
                "eth",
                "usdc",
                MarketDataInstrumentKind::Perpetual,
                PublicTrades,
            )],
        )
        .init()
        .await
        .unwrap();

    // Read the coins, including builder deployer `xyz`'s perpetuals, and find two by name.
    // `perp_coin` ignores case and returns Hyperliquid's spelling.
    let meta = HyperliquidMeta::fetch(Network::Mainnet, &["xyz"])
        .await
        .expect("Failed to read Hyperliquid market metadata");
    let book_instruments = [("kpepe", "KPEPE"), ("xyz_tsla", "xyz:TSLA")].map(|(key, name)| {
        let perp = meta
            .perp_coin(name)
            .unwrap_or_else(|| panic!("Hyperliquid lists no perpetual {name}"));
        info!("{name} is subscribed as {}", perp.coin());
        MarketInstrumentData::hyperliquid_perp(key, perp)
    });

    // Subscribe to L2 order book snapshots
    let books = Streams::<OrderBooksL2>::builder()
        .subscribe(
            WebSocketSubscriber,
            book_instruments.map(|instrument| Subscription {
                exchange: Hyperliquid,
                instrument,
                kind: OrderBooksL2,
            }),
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

    info!("Subscribed to Hyperliquid BTC and ETH trades, kPEPE and xyz:TSLA L2 books");
    info!("Receiving {} events then exiting...", MAX_EVENTS);

    let mut event_count = 0;

    // Process both streams concurrently, exit after MAX_EVENTS
    while event_count < MAX_EVENTS {
        tokio::select! {
            Some(trade) = trades_stream.next() => {
                info!("Trade: {:?}", trade);
                event_count += 1;
            }
            Some(book) = books_stream.next() => {
                info!("Book: {:?}", book);
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
