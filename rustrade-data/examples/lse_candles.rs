#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Historical candles from London Strategic Edge (LSE Data 3.0).
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** The provider's licence allows an account's data for its holder's own
//! personal use: their own research, trading and models. Supplying it to anyone else, in any form,
//! or running a product or service on it needs the provider's licence, and so does any use by or
//! for an organisation. Licence: <https://londonstrategicedge.com/licence>; terms:
//! <https://londonstrategicedge.com/terms>.
//!
//! In practice: do not commit what this prints to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires an API key (a free plan exists) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_candles --features lse
//! ```
//!
//! Demonstrates:
//! - **Reading the plan** first: rows per day and per download, and how far back each market goes.
//! - **Streaming** a chunked fetch, which processes each chunk as it arrives rather than buffering.
//! - **Symbols spelt per market** on REST: `EUR_USD` in `forex`, `AAPL` in `us_stocks`.
//!
//! # Properties worth knowing before you build on this
//!
//! - `close_time` is the **exclusive period end** (`open + interval`), computed library-side. A
//!   daily bar opens at its session's first trade, which for forex is the evening before.
//! - **Intraday periods with no trade are absent**, so consecutive candles are not guaranteed to
//!   be one interval apart.
//! - **Every download spends the plan's rows**, and the provider allows one download per key at a
//!   time. A client and its clones queue at one gate; separately built clients do not.
//! - The module documentation lists the data's known traps, some measured only on the provider's
//!   previous API: `rustrade_data::exchange::lse`.

use chrono::{Duration, Utc};
use futures::StreamExt;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::historical::LseCandleRequest;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::subscription::candle::CandleInterval;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    let client = LseClient::from_env()
        .expect("set LSE_API_KEY - get a key at https://londonstrategicedge.com/data");

    // The library reports the allowance; pacing policy is the caller's.
    let usage = client.usage().await.expect("usage");
    info!(
        plan = %usage.plan,
        rows_remaining_today = ?usage.rows.remaining_today(),
        rows_per_download = ?usage.rows.per_export,
        forex_history = ?usage.history.get("forex"),
        "allowance"
    );

    let end = Utc::now();
    let start = end - Duration::days(30);

    // Streaming: chunks are processed as they arrive. Prefer this over `collect_candles` for long
    // backfills at fine resolutions, where the whole range will not fit in memory.
    let stream = client.fetch_candles(LseCandleRequest::new(
        LseMarketCode::Forex,
        "EUR_USD",
        CandleInterval::Day1,
        start,
        end,
    ));
    futures::pin_mut!(stream);

    let mut fx_candles = 0usize;
    while let Some(candle) = stream.next().await {
        let candle = candle.expect("EUR_USD candle");
        fx_candles += 1;
        if fx_candles <= 3 {
            info!(
                close_time = %candle.close_time,
                close = %candle.close,
                volume = ?candle.volume,
                trades = ?candle.trade_count,
                "EUR_USD daily"
            );
        }
    }
    info!(fx_candles, "EUR_USD complete");

    // Prices and volumes are split-adjusted unless the request asks `as_published()`.
    let equities = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            CandleInterval::Day1,
            start,
            end,
        ))
        .await
        .expect("AAPL candles");

    if let Some(candle) = equities.last() {
        info!(
            close_time = %candle.close_time,
            close = %candle.close,
            volume = ?candle.volume,
            trades = ?candle.trade_count,
            "AAPL latest daily"
        );
    }
    info!(count = equities.len(), "AAPL complete");
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
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
