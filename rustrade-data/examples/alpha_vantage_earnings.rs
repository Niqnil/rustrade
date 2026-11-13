#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! A company's coming and past earnings releases from Alpha Vantage: the fiscal period each
//! reports, its date, and whether it comes before the open or after the close.
//!
//! # Data use
//!
//! **Alpha Vantage's data is for personal, non-commercial use only.** Use by or for a firm, and
//! giving others access to the data, need a commercial licence from Alpha Vantage. Do not commit,
//! publish or redistribute what this prints. See <https://www.alphavantage.co/terms_of_service/>.
//!
//! # Running
//!
//! Get a free key at <https://www.alphavantage.co/support/#api-key>, then:
//!
//! ```bash
//! ALPHA_VANTAGE_API_KEY=<your key> \
//!   cargo run -p rustrade-data --example alpha_vantage_earnings --features alpha-vantage -- IBM
//! ```
//!
//! Two requests, of a free key's 25 a day.
//!
//! # Properties worth knowing before you build on this
//!
//! - **The calendar holds only dates Alpha Vantage has**: a company whose next date it lacks is
//!   missing, and it does not say whether a date is announced or estimated.
//! - **Fiscal period ends are month ends**, even for companies whose quarters end on a weekday.
//! - **A past release's time is only before the open or after the close**, so it is known by the
//!   open, or by the end of its day.

use rustrade_data::exchange::alpha_vantage::{AlphaVantageClient, CalendarHorizon};

#[tokio::main]
async fn main() {
    let symbol = std::env::args().nth(1).unwrap_or_else(|| "IBM".to_owned());
    let key = std::env::var("ALPHA_VANTAGE_API_KEY").expect("set ALPHA_VANTAGE_API_KEY");
    let client = AlphaVantageClient::new(&key).expect("API key");

    let coming = client
        .earnings_calendar(Some(&symbol), CalendarHorizon::TwelveMonths)
        .await
        .expect("earnings calendar");
    println!(
        "{symbol}: {} coming release(s) Alpha Vantage has a date for",
        coming.len()
    );
    for release in &coming {
        println!(
            "  period ending {}  expected {} {:?}",
            release.fiscal_period_end, release.date, release.timing
        );
    }

    let past = client
        .earnings_history(&symbol)
        .await
        .expect("earnings history");
    println!("{symbol}: {} past releases; the last eight:", past.len());
    for release in past.iter().rev().take(8).rev() {
        println!(
            "  period ended {}  released {} {:?}  (known by {})",
            release.fiscal_period_end, release.date, release.timing, release.known_at
        );
    }
}
