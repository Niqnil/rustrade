//! Kraken Market Data Integration Tests
//!
//! These tests subscribe to Kraken's public spot WebSocket and check that subscriptions built from
//! base and quote assets are accepted, and that their data is routed back to them.
//!
//! # Running
//!
//! ```bash
//! cargo test -p rustrade-data --test kraken_data -- --ignored
//! ```
//!
//! Tests are marked `#[ignore]` so CI does not depend on a live venue.

// Integration test: panics on bad input are acceptable.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use rust_decimal::Decimal;
use rustrade_data::{
    exchange::kraken::Kraken,
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscriber::WebSocketSubscriber,
    subscription::{book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use std::{collections::HashSet, time::Duration};
use tracing_subscriber::{EnvFilter, fmt};

fn init_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
        )
        .try_init();
}

#[tokio::test]
#[ignore]
async fn test_order_books_l1_route_to_every_subscription() {
    init_logging();

    let streams = Streams::<OrderBooksL1>::builder()
        .subscribe(
            WebSocketSubscriber,
            [
                (
                    Kraken,
                    "btc",
                    "usd",
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                ),
                (
                    Kraken,
                    "eth",
                    "usd",
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                ),
            ],
        )
        .init()
        .await
        .expect("Kraken refused the subscriptions");

    let mut stream = streams
        .select_all()
        .with_error_handler(|error| panic!("stream error: {error:?}"));

    // The ticker channel opens with a snapshot, so each subscription answers at once
    let mut routed = HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while routed.len() < 2 {
        let event = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("timed out waiting for a top of book per subscription")
            .expect("stream ended");

        if let Event::Item(book) = event {
            let bid = book.kind.best_bid.expect("no best bid");
            let ask = book.kind.best_ask.expect("no best ask");
            assert!(
                bid.price > Decimal::ZERO && bid.price < ask.price,
                "{book:?}"
            );
            routed.insert(book.instrument.base.to_string());
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_public_trades_route_to_their_subscription() {
    init_logging();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            WebSocketSubscriber,
            [(
                Kraken,
                "btc",
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            )],
        )
        .init()
        .await
        .expect("Kraken refused the subscription");

    let mut stream = streams
        .select_all()
        .with_error_handler(|error| panic!("stream error: {error:?}"));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let event = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("timed out waiting for a trade")
            .expect("stream ended");

        if let Event::Item(trade) = event {
            assert_eq!(trade.instrument.base.to_string(), "btc");
            assert!(trade.kind.price > Decimal::ZERO, "{trade:?}");
            assert!(trade.kind.amount > Decimal::ZERO, "{trade:?}");
            assert!(trade.kind.id.parse::<u64>().is_ok(), "{trade:?}");
            return;
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_a_v1_spelling_is_refused_and_named() {
    init_logging();

    let Err(error) = Streams::<OrderBooksL1>::builder()
        .subscribe(
            WebSocketSubscriber,
            [
                (
                    Kraken,
                    "btc",
                    "usd",
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                ),
                (
                    Kraken,
                    "xbt",
                    "usd",
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                ),
            ],
        )
        .init()
        .await
    else {
        panic!("Kraken accepted the v1 spelling XBT/USD");
    };

    let error = format!("{error:?}");
    assert!(error.contains("ticker|XBT/USD"), "{error}");
    assert!(!error.contains("ticker|BTC/USD"), "{error}");
}
