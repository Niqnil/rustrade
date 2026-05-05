//! Databento Market Data Integration Tests
//!
//! These tests verify connectivity and data reception from Databento APIs.
//!
//! # Status
//!
//! **Live data integration is NOT TESTED.** Databento does not offer sandbox or
//! development API keys, and we do not have a subscription. The transformation
//! logic (DBN → rustrade events) is tested via fixtures in
//! `databento_transformer.rs`, but the network integration (authentication,
//! API calls, WebSocket streaming) has not been verified against real endpoints.
//!
//! # Prerequisites
//!
//! 1. Databento account with active subscription
//! 2. Environment variable set (see .env.template):
//!    - DATABENTO_API_KEY: API key from Databento dashboard
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env
//! source .env
//!
//! # Run all Databento integration tests
//! cargo test --test databento_integration --features databento -- --ignored
//!
//! # Run specific test
//! cargo test --test databento_integration --features databento test_historical_fetch_trades -- --ignored
//! ```
//!
//! # Market Hours Note
//!
//! CME Globex (ES futures) trades nearly 24/6:
//! - Sunday 5:00 PM CT to Friday 4:00 PM CT
//! - Daily maintenance break 4:00 PM - 5:00 PM CT
//!
//! Live streaming tests should work during these hours.

#![cfg(feature = "databento")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use databento::dbn::Schema;
use databento::historical::timeseries::GetRangeParams;
use futures_util::StreamExt;
use rustrade_data::exchange::databento::{DatabentoHistorical, DatabentoLive};
use rustrade_instrument::exchange::ExchangeId;
use std::collections::HashMap;
use std::pin::pin;
use time::{Duration, OffsetDateTime};
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
// Historical API Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_historical_client_creation() {
    init_logging();

    let client = DatabentoHistorical::from_env();
    assert!(
        client.is_ok(),
        "Failed to create historical client: {:?}",
        client.err()
    );
    tracing::info!("Historical client created successfully");
}

#[tokio::test]
#[ignore]
async fn test_historical_fetch_trades() {
    init_logging();

    let mut client = DatabentoHistorical::from_env().expect("Failed to create historical client");

    // Query a short time range from yesterday to ensure data exists
    let end = OffsetDateTime::now_utc() - Duration::hours(25);
    let start = end - Duration::minutes(5);

    let params = GetRangeParams::builder()
        .dataset("GLBX.MDP3")
        .symbols(vec!["ES.FUT".to_string()])
        .schema(Schema::Trades)
        .date_time_range(start..end)
        .build();

    tracing::info!(%start, %end, "Fetching ES futures trades");

    let trades = client
        .fetch_trades(&params, ExchangeId::DatabentoGlbx, "ES")
        .await;

    assert!(trades.is_ok(), "Failed to fetch trades: {:?}", trades.err());

    let trades = trades.unwrap();
    tracing::info!(count = trades.len(), "Fetched trades");

    // ES futures are very liquid, should have trades in any 5-minute window during market hours
    // But we don't assert on count since we might hit a maintenance window
    if !trades.is_empty() {
        let first = &trades[0];
        assert_eq!(first.exchange, ExchangeId::DatabentoGlbx);
        assert!(first.kind.price > 0.0, "Trade price should be positive");
        assert!(first.kind.amount > 0.0, "Trade amount should be positive");
        tracing::info!(
            price = first.kind.price,
            amount = first.kind.amount,
            "First trade"
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_historical_fetch_quotes() {
    init_logging();

    let mut client = DatabentoHistorical::from_env().expect("Failed to create historical client");

    // Query a short time range from yesterday
    let end = OffsetDateTime::now_utc() - Duration::hours(25);
    let start = end - Duration::minutes(5);

    let params = GetRangeParams::builder()
        .dataset("GLBX.MDP3")
        .symbols(vec!["ES.FUT".to_string()])
        .schema(Schema::Mbp1)
        .date_time_range(start..end)
        .build();

    tracing::info!(%start, %end, "Fetching ES futures quotes");

    let quotes = client
        .fetch_quotes(&params, ExchangeId::DatabentoGlbx, "ES")
        .await;

    assert!(quotes.is_ok(), "Failed to fetch quotes: {:?}", quotes.err());

    let quotes = quotes.unwrap();
    tracing::info!(count = quotes.len(), "Fetched quotes");

    if !quotes.is_empty() {
        let first = &quotes[0];
        assert_eq!(first.exchange, ExchangeId::DatabentoGlbx);
        assert!(first.kind.bid_price > 0.0, "Bid price should be positive");
        assert!(first.kind.ask_price > 0.0, "Ask price should be positive");
        assert!(
            first.kind.ask_price >= first.kind.bid_price,
            "Ask should be >= bid"
        );
        tracing::info!(
            bid = first.kind.bid_price,
            ask = first.kind.ask_price,
            "First quote"
        );
    }
}

// ============================================================================
// Live Streaming API Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_live_client_connection() {
    init_logging();

    // Use current front-month ES contract
    let instruments: HashMap<String, String> = [("ESM5".to_string(), "ES-front".to_string())]
        .into_iter()
        .collect();

    let client = DatabentoLive::from_env("GLBX.MDP3", ExchangeId::DatabentoGlbx, instruments).await;

    assert!(
        client.is_ok(),
        "Failed to create live client: {:?}",
        client.err()
    );
    tracing::info!("Live client connected successfully");
}

#[tokio::test]
#[ignore]
async fn test_live_subscribe() {
    init_logging();

    let instruments: HashMap<String, String> = [("ESM5".to_string(), "ES-front".to_string())]
        .into_iter()
        .collect();

    let mut client = DatabentoLive::from_env("GLBX.MDP3", ExchangeId::DatabentoGlbx, instruments)
        .await
        .expect("Failed to create live client");

    let result = client.subscribe(&["ESM5"], Schema::Trades).await;
    assert!(result.is_ok(), "Failed to subscribe: {:?}", result.err());
    tracing::info!("Subscription successful");
}

#[tokio::test]
#[ignore]
async fn test_live_stream_receives_data() {
    init_logging();

    let instruments: HashMap<String, String> = [("ESM5".to_string(), "ES-front".to_string())]
        .into_iter()
        .collect();

    let mut client = DatabentoLive::from_env("GLBX.MDP3", ExchangeId::DatabentoGlbx, instruments)
        .await
        .expect("Failed to create live client");

    client
        .subscribe(&["ESM5"], Schema::Trades)
        .await
        .expect("Failed to subscribe");

    tracing::info!("Starting live stream, waiting for data...");

    let stream = client.start().await.expect("Failed to start stream");
    let mut stream = pin!(stream);

    // Wait up to 60 seconds for at least one trade
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(market_event) => {
                    tracing::info!(?market_event, "Received market event");
                    return Ok(market_event);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Stream error");
                }
            }
        }
        Err("Stream ended without data")
    })
    .await;

    assert!(
        timeout.is_ok(),
        "Timeout waiting for live data. If outside CME Globex hours, this is expected."
    );

    let event = timeout.unwrap();
    assert!(event.is_ok(), "No valid market event received");

    tracing::info!("Live stream test passed");
}
