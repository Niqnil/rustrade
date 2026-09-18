#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Historical candles from the London Strategic Edge vault.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Terms: <https://londonstrategicedge.com/terms>
//!
//! In practice: do not commit what this prints to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires a free API key (no account, no card) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_candles --features lse
//! ```
//!
//! Demonstrates:
//! - **Streaming** a paged fetch, which processes each page as it arrives rather than buffering.
//! - **`volume: None` for FX** — the vault omits the field entirely rather than reporting zero.
//! - **Reading the shared allowance**, which meters streaming and bulk export against one pool.
//!
//! # Properties worth knowing before you build on this
//!
//! - `close_time` is the **exclusive period-end** boundary (`open + interval`), computed
//!   library-side; the vault reports only the bar's open instant.
//! - Candles are keyed on the **display symbol** (`EUR/USD`, `AAPL`, `BP.L`, `ES.F`), not a slug.
//! - **Sparseness differs by resolution.** *Intraday*, periods with no activity are absent rather
//!   than gap-filled, so consecutive candles are not guaranteed to be one interval apart. *Daily*
//!   is not sparse: non-trading days arrive as **flat** bars (`open == high == low == close`) for
//!   sampled Saturdays and the US Independence Day observance, with only Sundays absent — so a
//!   backtest sees a tradeable price on a closed market, and the flat OHLC is the only signal.
//!   Do not infer "no bar means the market was closed".
//! - **London (`.L`) listings are quoted in pence**, not pounds — `BP.L` prints ~548 where BP
//!   trades around £5.48. This integration quotes them in GBX, an asset distinct from GBP, and
//!   passes prices through unscaled.

use chrono::{Duration, Utc};
use futures::StreamExt;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_data::subscription::candle::CandleInterval;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    let client = LseVaultClient::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    // The allowance is shared between streaming and bulk export, so a consumer doing both budgets
    // against one pool. The library reports it; pacing policy is the caller's.
    let usage = client.usage().await.expect("usage");
    info!(
        bytes_remaining_month = usage.bytes_remaining_month(),
        exports_remaining_hour = usage.exports_remaining_hour(),
        calls_per_minute = usage.calls_per_minute,
        "allowance"
    );

    let end = Utc::now();
    let start = end - Duration::days(30);

    // Streaming: pages are processed as they arrive. Prefer this over `collect_candles` for long
    // backfills at fine resolutions, where the whole range will not fit in memory.
    let stream = client.fetch_candles("EUR/USD", CandleInterval::Day1, start, end);
    futures::pin_mut!(stream);

    let mut fx_candles = 0usize;
    while let Some(candle) = stream.next().await {
        let candle = candle.expect("EUR/USD candle");
        fx_candles += 1;

        // `None`, not zero: the vault publishes no consolidated volume for FX. A synthetic zero
        // would aggregate into a legitimate-looking total at every derived resolution.
        if fx_candles <= 3 {
            info!(
                close_time = %candle.close_time,
                close = %candle.close,
                volume = ?candle.volume,
                "EUR/USD daily"
            );
        }
    }
    info!(fx_candles, "EUR/USD complete");

    // Equities do carry a volume, so the same call shape yields `Some` here. The contrast is the
    // reason `volume` is an `Option` rather than a `Decimal` that quietly defaults.
    let equities = client
        .collect_candles("AAPL", CandleInterval::Day1, start, end)
        .await
        .expect("AAPL candles");

    if let Some(candle) = equities.last() {
        info!(
            close_time = %candle.close_time,
            close = %candle.close,
            volume = ?candle.volume,
            trade_count = ?candle.trade_count, // always `None`: the vault reports no trade count
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
