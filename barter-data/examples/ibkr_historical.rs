//! IBKR Historical Data Example
//!
//! **UNTESTED** — Requires TWS or IB Gateway connection.
//!
//! Demonstrates fetching historical OHLCV bars from Interactive Brokers.
//!
//! # Prerequisites
//!
//! 1. TWS or IB Gateway running on localhost
//! 2. API connections enabled (Configure → API → Settings)
//! 3. Socket port: 7497 (TWS paper) or 4002 (Gateway paper)
//! 4. Market data subscription for the requested instrument
//!
//! # Usage
//!
//! ```bash
//! cargo run --example ibkr_historical --features ibkr
//! ```

// Examples use unwrap/expect for brevity — not production code
#![allow(clippy::unwrap_used, clippy::expect_used)]

use barter_data::exchange::ibkr::historical::{HistoricalRequest, IbkrHistoricalData, ToDuration};
use ibapi::{
    contracts::Contract,
    market_data::historical::{BarSize, WhatToShow},
};
use tracing::{info, warn};

#[tokio::main]
async fn main() {
    init_logging();

    // Connect to IB Gateway paper trading
    let url = "127.0.0.1:4002"; // Gateway paper; use 127.0.0.1:7497 for TWS paper
    let client_id = 101; // Use different ID from market data connection

    info!("Connecting to IB Gateway at {url}...");

    let client = match IbkrHistoricalData::connect(url, client_id) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect: {e}");
            warn!("Make sure TWS/Gateway is running with API enabled");
            return;
        }
    };

    info!("Connected! Fetching historical data for AAPL...");

    // Build request for AAPL daily bars
    let contract = Contract::stock("AAPL").build();
    let request = HistoricalRequest {
        contract,
        end_date: None, // Current time
        duration: 30.days(),
        bar_size: BarSize::Day,
        what_to_show: WhatToShow::Trades,
        regular_trading_hours_only: true,
    };

    // Fetch historical candles
    match client.fetch_candles(request).await {
        Ok(candles) => {
            info!("Received {} daily bars for AAPL:", candles.len());
            info!("");
            info!(
                "{:^12} {:>10} {:>10} {:>10} {:>10} {:>12} {:>8}",
                "Date", "Open", "High", "Low", "Close", "Volume", "Trades"
            );
            info!("{}", "-".repeat(80));

            for candle in candles.iter().take(10) {
                info!(
                    "{:^12} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>12.0} {:>8}",
                    candle.close_time.format("%Y-%m-%d"),
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.trade_count
                );
            }

            if candles.len() > 10 {
                info!("... ({} more bars)", candles.len() - 10);
            }
        }
        Err(e) => {
            warn!("Failed to fetch historical data: {e}");
            warn!("This may be due to:");
            warn!("  - No market data subscription for AAPL");
            warn!("  - IB pacing violation (too many requests)");
            warn!("  - Market is closed and no historical data available");
        }
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
