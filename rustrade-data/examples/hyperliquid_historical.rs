//! Hyperliquid historical candle data example.
//!
//! Demonstrates how to fetch historical OHLCV candles from Hyperliquid.
//!
//! Run with: `cargo run --example hyperliquid_historical --features hyperliquid`

// Example binary: panics are acceptable for demonstration code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use rustrade_data::exchange::hyperliquid::historical::{
    HistoricalRequest, HyperliquidHistoricalData,
};
use rustrade_data::subscription::candle::CandleInterval;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    // Create historical data client (mainnet)
    let client = HyperliquidHistoricalData::new(false)
        .await
        .expect("Failed to create client");

    // Fetch last 7 days of hourly BTC candles
    let request = HistoricalRequest {
        coin: "BTC".to_string(),
        interval: CandleInterval::Hour1,
        start_time: Utc::now() - Duration::days(7),
        end_time: Utc::now(),
    };

    info!("Fetching BTC hourly candles for last 7 days...");

    let candles = client
        .fetch_candles(request)
        .await
        .expect("Failed to fetch candles");

    info!("Received {} candles", candles.len());

    // Print first and last few candles
    for candle in candles.iter().take(3) {
        info!(
            "  {} | O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}",
            candle.close_time, candle.open, candle.high, candle.low, candle.close, candle.volume
        );
    }

    if candles.len() > 6 {
        info!("  ...");
        for candle in candles.iter().rev().take(3).rev() {
            info!(
                "  {} | O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2}",
                candle.close_time,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume
            );
        }
    }

    // Also fetch daily candles using the convenience builder
    info!("\nFetching ETH daily candles for last 30 days...");

    let daily_request = HistoricalRequest::daily("ETH", 30);
    let daily_candles = client
        .fetch_candles(daily_request)
        .await
        .expect("Failed to fetch daily candles");

    info!("Received {} daily candles", daily_candles.len());
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .init()
}
