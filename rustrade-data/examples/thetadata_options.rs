#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! An option chain, its open interest and the risk-free rate from ThetaData, through a running
//! Theta Terminal.
//!
//! # ⚠️ Licensing: the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** ThetaData's individual plans license the data for personal, non-commercial
//! use only, and its terms forbid reproducing, distributing or publishing it, or making it
//! available to anyone else. Terms: <https://www.thetadata.net/terms-and-conditions>
//!
//! In practice: do not commit what this prints to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Start the Theta Terminal, logged in to your ThetaData account
//! (<https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html>), then:
//!
//! ```bash
//! cargo run --example thetadata_options --features thetadata
//! ```
//!
//! Set `THETADATA_BASE_URL` if the Terminal is not at `http://127.0.0.1:25503/v3`.
//!
//! Demonstrates:
//! - **Free-plan reads**: an underlying's expirations and strikes, the SOFR rate, today's market
//!   schedule.
//! - **Value-plan reads**: a whole chain's quotes in one snapshot request, and its open interest.
//!   On a free account these fail with `PermissionDenied`, which the example reports and skips.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Open interest describes the previous trading day.** OPRA reports it around 06:30 New York
//!   time.
//! - **A snapshot can be days old.** It holds each contract's latest quote from the past seven
//!   days; pass `current_day_only` to keep only today's.
//! - **An empty side is a zero price**, kept in the row and mapped to an absent level by
//!   `top_of_book`.

use chrono::{Duration, Utc};
use futures::StreamExt;
use rustrade_data::{
    exchange::thetadata::{
        DEFAULT_BASE_URL, Expiration, InterestRateSeries, OptionSelection, ThetaDataClient,
        ThetaDataError,
    },
    subscription::book::OrderBookL1,
};

const UNDERLYING: &str = "SPY";

#[tokio::main]
async fn main() {
    let base_url =
        std::env::var("THETADATA_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());
    let client = ThetaDataClient::new()
        .unwrap()
        .with_base_url(base_url)
        .unwrap();

    let today = client
        .market_day_today()
        .await
        .expect("Theta Terminal reachable");
    match today {
        Some(today) => println!("market today: {today:?}"),
        None => println!("market today: no schedule"),
    }

    let expirations: Vec<_> = client
        .option_expirations(UNDERLYING)
        .filter_map(|expiration| async move { expiration.ok() })
        .collect()
        .await;
    let next = *expirations
        .iter()
        .find(|expiration| **expiration >= Utc::now().date_naive())
        .expect("a future expiration");
    let strikes = client
        .option_strikes(UNDERLYING, next)
        .filter_map(|strike| async move { strike.ok() })
        .count()
        .await;
    println!(
        "{UNDERLYING}: {} expirations, next {next} with {strikes} strikes",
        expirations.len()
    );

    let end = Utc::now().date_naive();
    let sofr = client
        .interest_rate_history(InterestRateSeries::Sofr, end - Duration::days(10), end)
        .filter_map(|rate| async move { rate.ok() })
        .collect::<Vec<_>>()
        .await;
    if let Some(latest) = sofr.last() {
        println!(
            "SOFR on {}: {} (fraction {})",
            latest.date,
            latest.rate_percent,
            latest.rate()
        );
    }

    // The next expiration's chain, ten strikes either side of the underlying, in one request.
    let chain = OptionSelection::new(UNDERLYING, Expiration::On(next)).with_strike_range(10);
    let mut quotes = Box::pin(client.option_quote_snapshot(&chain, false));
    let mut count = 0;
    while let Some(quote) = quotes.next().await {
        match quote {
            Ok(quote) => {
                count += 1;
                if count <= 3 {
                    let book = OrderBookL1::from(&quote);
                    println!(
                        "{:?} {} mid {:?}",
                        quote.contract.kind,
                        quote.contract.strike,
                        book.mid_price()
                    );
                }
            }
            Err(ThetaDataError::PermissionDenied { .. }) => {
                println!("chain snapshots need the options value plan: skipped");
                return;
            }
            Err(error) => panic!("{error}"),
        }
    }
    println!("{count} quotes in the chain snapshot");

    let open_interest = client
        .option_open_interest_snapshot(&chain, false)
        .filter_map(|row| async move { row.ok() })
        .fold(0u64, |total, row| async move { total + row.open_interest })
        .await;
    println!("open interest across the chain: {open_interest}");
}
