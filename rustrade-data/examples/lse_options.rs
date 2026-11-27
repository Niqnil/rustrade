#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! US option prints, candles, Greeks and a chain from London Strategic Edge (LSE Data 3.0).
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
//! cargo run --example lse_options --features lse
//! ```
//!
//! Demonstrates:
//! - **Streaming the print tape** for one underlying, oldest first, and picking the busiest
//!   contract from it.
//! - **One-minute candles and Greeks** for that contract.
//! - **One page of the chain**, with each contract's latest Greeks and when they were computed.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Greeks follow trades.** The Greeks selection has a row only for a minute with a trade, so a
//!   contract that has not traded has none, and the chain's Greeks are as old as their
//!   `greeks_at`.
//! - **No quotes.** Every print is a trade; there is no bid or ask on these paths.
//! - **Recent data is refused.** A tape range ending within a minute of now is rejected, because
//!   the provider can serve a recent window incomplete with nothing to say so. This example reads
//!   the previous weekday's session for that reason.
//! - **Prints carry no identifier.** Two prints can be identical in every field and are both real;
//!   the fetch keeps both.

use chrono::{Datelike, Duration, NaiveTime, Utc, Weekday};
use futures::StreamExt;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::subscription::candle::CandleInterval;
use std::collections::HashMap;
use tracing::info;

#[tokio::main]
async fn main() {
    init_logging();

    let client = LseClient::from_env()
        .expect("set LSE_API_KEY - get a key at https://londonstrategicedge.com/data");

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
                time = %print.time,
                ticker = %print.contract.ticker,
                kind = ?print.contract.kind,
                strike = %print.contract.strike,
                expiry = %print.contract.expiry,
                price = %print.price,
                contracts = print.size,
                venue = %print.venue,
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

    let candles = client.fetch_option_candles(&ticker, CandleInterval::Min1, start, end);
    futures::pin_mut!(candles);
    while let Some(candle) = candles.next().await {
        let candle = candle.expect("option candle");
        info!(
            close_time = %candle.close_time,
            close = %candle.close,
            contracts = ?candle.volume,
            prints = ?candle.trade_count,
            "option candle"
        );
    }

    let greeks = client.fetch_option_greeks(&ticker, start, end);
    futures::pin_mut!(greeks);
    while let Some(minute) = greeks.next().await {
        let minute = minute.expect("option greeks");
        info!(
            close_time = %minute.close_time,
            option_price = ?minute.option_price,
            delta = ?minute.greeks.delta,
            iv = ?minute.greeks.implied_volatility,
            "option greeks"
        );
    }

    let chain = client
        .fetch_option_chain("SPY", 5, 0)
        .await
        .expect("option chain");
    info!(contracts = chain.contracts, underlying_price = ?chain.underlying_price, "SPY chain");
    for entry in &chain.entries {
        info!(
            ticker = %entry.contract.ticker,
            last_price = ?entry.last_price,
            delta = ?entry.greeks.delta,
            greeks_at = ?entry.greeks_at,
            "chain contract"
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
