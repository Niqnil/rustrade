#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Option prints and one-minute option candles from the London Strategic Edge vault.
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
//! Requires a free API key from <https://londonstrategicedge.com/data>, in `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --example lse_options --features lse
//! ```
//!
//! Demonstrates:
//! - **Streaming the print tape** for one underlying, oldest first, and selecting contracts from
//!   it — the endpoint cannot filter by contract.
//! - **Greeks at print time**, and why their age matters.
//! - **Option candles** for the busiest contract seen.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Greeks are print-triggered.** They arrive only with a trade, so a contract's greeks are as
//!   old as its last print, and a contract that has not traded has none. There is no snapshot
//!   fallback on this feed.
//! - **No quotes.** Every row is a trade; there is no bid or ask on these paths.
//! - **Recent data is refused.** A range ending within a minute of now is rejected, because the
//!   provider can serve a recent window incomplete with nothing to say so. This example reads the
//!   previous weekday's session for that reason.
//! - **Timestamps are whole-second batch stamps.** Key and de-duplicate on the print `id`.

use chrono::{Datelike, Duration, NaiveTime, Utc, Weekday};
use futures::StreamExt;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use std::collections::HashMap;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    let client = LseVaultClient::from_env()
        .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data");

    // Five minutes of the previous weekday's session. 15:00 UTC is inside US options hours in both
    // summer and winter; a US holiday simply returns nothing.
    let mut day = Utc::now().date_naive() - Duration::days(1);
    while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
        day -= Duration::days(1);
    }
    let start = day
        .and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap())
        .and_utc();
    let end = start + Duration::minutes(5);

    let prints = client.fetch_option_flow(Some("SPY"), start, end);
    futures::pin_mut!(prints);

    let mut per_contract: HashMap<String, usize> = HashMap::new();
    let mut total = 0usize;
    while let Some(print) = prints.next().await {
        let print = print.expect("option print");
        total += 1;
        *per_contract
            .entry(print.contract.ticker.to_string())
            .or_default() += 1;

        if total <= 3 {
            info!(
                id = print.id,
                time = %print.time,
                ticker = %print.contract.ticker,
                kind = ?print.contract.kind,
                strike = %print.contract.strike,
                expiry = %print.contract.expiry,
                price = %print.price,
                contracts = print.volume,
                delta = ?print.greeks.delta,
                iv = ?print.greeks.implied_volatility,
                underlying = ?print.greeks.underlying_price,
                "SPY option print"
            );
        }
    }
    info!(
        total,
        contracts = per_contract.len(),
        "SPY print tape complete"
    );

    let Some((ticker, count)) = per_contract.into_iter().max_by_key(|(_, count)| *count) else {
        info!("no prints in the window - a US market holiday?");
        return;
    };
    info!(%ticker, count, "busiest contract");

    let candles = client
        .collect_option_candles(&ticker, start, end)
        .await
        .expect("option candles");
    for candle in &candles {
        info!(
            close_time = %candle.candle.close_time,
            close = %candle.candle.close,
            contracts = ?candle.candle.volume,
            prints = ?candle.candle.trade_count,
            // An average over the minute's prints, not a value at its close.
            delta_avg = ?candle.greeks.delta,
            "option candle"
        );
    }
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
