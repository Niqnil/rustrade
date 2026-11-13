//! Live checks against Alpha Vantage.
//!
//! `#[ignore]`d: they reach alphavantage.co with the key in `ALPHA_VANTAGE_API_KEY`. Run with
//! `cargo test -p rustrade-data --features alpha-vantage --test alpha_vantage_live -- --ignored --test-threads=1`.
//!
//! Three requests in all, of a free key's 25 a day. The checks assert shapes only: Alpha Vantage's
//! data is for personal, non-commercial use (<https://www.alphavantage.co/terms_of_service/>), so
//! nothing here prints or keeps it.

#![cfg(feature = "alpha-vantage")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Datelike, NaiveDate, Utc};
use rustrade_data::{
    earnings::{EarningsProvider, ReleaseStatus, ReleaseTiming, nearest_month_end},
    exchange::alpha_vantage::{AlphaVantageClient, CalendarHorizon},
};

fn client() -> AlphaVantageClient {
    let key = std::env::var("ALPHA_VANTAGE_API_KEY")
        .expect("ALPHA_VANTAGE_API_KEY must hold an Alpha Vantage API key");
    AlphaVantageClient::new(&key).unwrap()
}

fn is_month_end(date: NaiveDate) -> bool {
    nearest_month_end(date) == date
}

/// One test, so one client and one pace cover every request: separate clients would send
/// back to back, and Alpha Vantage refuses a free key's second request within a second.
#[tokio::test]
#[ignore = "reaches alphavantage.co; needs ALPHA_VANTAGE_API_KEY"]
async fn history_and_calendar_read_live() {
    let client = client();

    // Apple's history: reported after the close for years, with month-end periods.
    let releases = client.earnings_history("AAPL").await.unwrap();
    assert!(releases.len() > 100, "{} releases", releases.len());
    assert!(releases.windows(2).all(|w| w[0].date <= w[1].date));
    assert!(releases.iter().all(|r| r.symbol == "AAPL"
        && r.status == ReleaseStatus::Reported
        && r.source.provider == EarningsProvider::AlphaVantage
        && is_month_end(r.fiscal_period_end)
        && r.fiscal_period_end < r.date
        && r.known_at <= r.source.observed_at));
    let recent: Vec<_> = releases.iter().filter(|r| r.date.year() >= 2020).collect();
    assert!(recent.len() >= 20, "{} since 2020", recent.len());
    assert!(recent.iter().all(|r| r.timing == ReleaseTiming::AfterClose));

    // The full calendar: unconfirmed dates from yesterday on, with month-end periods.
    let all = client
        .earnings_calendar(None, CalendarHorizon::ThreeMonths)
        .await
        .unwrap();
    assert!(all.len() > 100, "{} releases", all.len());
    let yesterday = Utc::now().date_naive().pred_opt().unwrap();
    assert!(all.iter().all(|r| r.status == ReleaseStatus::Unknown
        && r.known_at == r.source.observed_at
        && is_month_end(r.fiscal_period_end)
        && r.date >= yesterday));
    assert!(all.windows(2).all(|w| w[0].date <= w[1].date));
    assert!(all.iter().any(|r| r.timing == ReleaseTiming::BeforeOpen));
    assert!(all.iter().any(|r| r.timing == ReleaseTiming::AfterClose));

    // The last company in the list, asked for alone, has the same release.
    let last = all.last().unwrap();
    let one = client
        .earnings_calendar(Some(last.symbol.as_str()), CalendarHorizon::ThreeMonths)
        .await
        .unwrap();
    assert!(
        one.iter()
            .any(|r| r.key() == last.key() && r.date == last.date),
        "{} releases for the symbol",
        one.len()
    );
}
