#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! A company's past earnings releases from SEC EDGAR: the fiscal period each reports, its date,
//! and whether it came before the open or after the close.
//!
//! # Running
//!
//! SEC requires automated requests to declare who sends them. Put a name and a contact email in
//! `SEC_EDGAR_USER_AGENT`, then:
//!
//! ```bash
//! SEC_EDGAR_USER_AGENT="Sample Company Name admin@example.com" \
//!   cargo run -p rustrade-data --example sec_edgar_earnings --features sec-edgar -- AAPL
//! ```
//!
//! # Properties worth knowing before you build on this
//!
//! - **The newest quarter appears once its 10-Q or 10-K is filed**, which can be weeks after its
//!   release.
//! - **Each release is dated by its 8-K filing's acceptance**, usually minutes after the release.
//! - **Foreign private issuers** (20-F, 6-K filers) are refused: their reports carry no 8-K items.
//! - SEC states that information on sec.gov may be copied and redistributed, citing SEC as the
//!   source. See <https://www.sec.gov/about/privacy-information>.

use chrono::{Days, Utc};
use rustrade_data::exchange::sec_edgar::SecEdgarClient;

#[tokio::main]
async fn main() {
    let symbol = std::env::args().nth(1).unwrap_or_else(|| "AAPL".to_owned());
    let user_agent = std::env::var("SEC_EDGAR_USER_AGENT")
        .expect("set SEC_EDGAR_USER_AGENT to a name and a contact email");
    let client = SecEdgarClient::new(&user_agent).expect("User-Agent");

    let tickers = client.company_tickers().await.expect("ticker list");
    let Some(company) = tickers.iter().find(|t| t.ticker == symbol.as_str()) else {
        eprintln!("{symbol} is not in SEC's ticker list");
        return;
    };
    println!(
        "{} ({}), CIK {}",
        company.title, company.ticker, company.cik
    );

    let to = Utc::now().date_naive();
    let from = to.checked_sub_days(Days::new(3 * 365)).unwrap();
    let releases = client
        .earnings_releases(company.cik, symbol.as_str(), from, to)
        .await
        .expect("earnings releases");
    println!("{} releases from {from} to {to}:", releases.len());
    for release in &releases {
        println!(
            "  period ended {}  released {} {:?}  (filing {})",
            release.fiscal_period_end,
            release.date,
            release.timing,
            release.source.record_id.as_deref().unwrap_or("?"),
        );
    }
}
