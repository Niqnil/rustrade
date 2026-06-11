//! Binance historical klines example (public REST — no API key required).
//!
//! Demonstrates fetching historical OHLCV candles from both Binance surfaces:
//! spot (`/api/v3/klines`) and USD-M futures continuous
//! (`/fapi/v1/continuousKlines`, which serves `1s` candles).
//!
//! Run with: `cargo run --example binance_historical`

// Example binary: panics are acceptable for demonstration code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use futures::{StreamExt, pin_mut};
use rustrade_data::exchange::binance::BinanceHistoricalClient;
use rustrade_data::subscription::candle::CandleInterval;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    let end = Utc::now();

    // --- Spot: stream the last hour of 1m BTCUSDT candles -------------------
    let spot = BinanceHistoricalClient::spot();
    info!("Streaming spot BTCUSDT 1m candles for the last hour...");

    let stream = spot.fetch_candles(
        "BTCUSDT",
        CandleInterval::Min1,
        end - Duration::hours(1),
        end,
    );
    pin_mut!(stream);
    let mut count = 0usize;
    while let Some(candle) = stream.next().await {
        let candle = candle.expect("spot fetch failed");
        if count < 3 {
            info!(
                "  {} | O:{} H:{} L:{} C:{} V:{} trades:{}",
                candle.close_time,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.trade_count,
            );
        }
        count += 1;
    }
    info!("Received {count} spot candles");

    // --- Futures: collect 5 minutes of 1s candles (continuous surface) ------
    // 1s is available on futures ONLY via the continuous-contract surface.
    let futures = BinanceHistoricalClient::futures();
    info!("Collecting futures BTCUSDT 1s candles for the last 5 minutes...");

    let candles = futures
        .collect_candles(
            "BTCUSDT",
            CandleInterval::Sec1,
            end - Duration::minutes(5),
            end,
        )
        .await
        .expect("futures fetch failed");

    info!("Received {} futures 1s candles", candles.len());
    for candle in candles.iter().take(3) {
        info!(
            "  {} | C:{} V:{} trades:{}",
            candle.close_time, candle.close, candle.volume, candle.trade_count,
        );
    }
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
