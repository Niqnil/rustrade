//! Live checks against SEC EDGAR.
//!
//! `#[ignore]`d: they reach sec.gov, and SEC requires a declared User-Agent, read here from
//! `SEC_EDGAR_USER_AGENT` (a name and a contact email). Run with
//! `cargo test -p rustrade-data --features sec-edgar --test sec_edgar_live -- --ignored`.
//!
//! About 15 requests in all, paced under SEC's limit.

#![cfg(feature = "sec-edgar")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Datelike, NaiveDate, TimeDelta, Weekday};
use rustrade_data::{
    earnings::{EarningsProvider, ReleaseStatus, ReleaseTiming},
    exchange::sec_edgar::{Cik, SecEdgarClient, SecEdgarError},
};

fn client() -> SecEdgarClient {
    let user_agent = std::env::var("SEC_EDGAR_USER_AGENT")
        .expect("SEC_EDGAR_USER_AGENT must hold a name and a contact email");
    SecEdgarClient::new(&user_agent).unwrap()
}

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[tokio::test]
#[ignore = "reaches sec.gov; needs SEC_EDGAR_USER_AGENT"]
async fn the_ticker_list_maps_large_issuers_to_their_ciks() {
    let tickers = client().company_tickers().await.unwrap();
    assert!(tickers.len() > 5_000, "{} tickers", tickers.len());
    let cik = |symbol: &str| tickers.iter().find(|t| t.ticker == symbol).map(|t| t.cik);
    assert_eq!(cik("AAPL"), Some(Cik::new(320_193)));
    assert_eq!(cik("MSFT"), Some(Cik::new(789_019)));
}

#[tokio::test]
#[ignore = "reaches sec.gov; needs SEC_EDGAR_USER_AGENT"]
async fn two_years_of_a_large_issuer_are_eight_releases_one_per_quarter() {
    // Apple: fiscal year ends late September, 52/53-week quarters, releases after the close.
    let releases = client()
        .earnings_releases(
            Cik::new(320_193),
            "AAPL",
            date(2024, 1, 1),
            date(2025, 12, 31),
        )
        .await
        .unwrap();
    assert_eq!(releases.len(), 8, "{releases:#?}");
    for pair in releases.windows(2) {
        let gap = pair[1].fiscal_period_end - pair[0].fiscal_period_end;
        assert!(
            (TimeDelta::days(80)..=TimeDelta::days(100)).contains(&gap),
            "{pair:#?}"
        );
    }
    for release in &releases {
        assert_eq!(release.symbol, "AAPL");
        assert_eq!(release.status, ReleaseStatus::Reported);
        assert_eq!(release.timing, ReleaseTiming::AfterClose, "{release:#?}");
        assert_eq!(release.source.provider, EarningsProvider::SecEdgar);
        assert!(release.source.record_id.is_some());
        // Apple's periods end on a Saturday; the release follows within about five weeks.
        assert_eq!(
            release.fiscal_period_end.weekday(),
            Weekday::Sat,
            "{release:#?}"
        );
        let lead = release.date - release.fiscal_period_end;
        assert!(
            (TimeDelta::days(20)..=TimeDelta::days(45)).contains(&lead),
            "{release:#?}"
        );
        assert_eq!(release.known_at.date_naive(), release.date);
        assert!(release.known_at <= release.source.observed_at);
    }
}

#[tokio::test]
#[ignore = "reaches sec.gov; needs SEC_EDGAR_USER_AGENT"]
async fn an_issuer_furnishing_quarterly_figures_under_item_2_02_gets_its_results_release() {
    // Tesla furnishes its quarterly production and deliveries figures under Item 2.02 early in
    // each quarter, before its results: only the results, after the close, may come back.
    let releases = client()
        .earnings_releases(
            Cik::new(1_318_605),
            "TSLA",
            date(2025, 1, 1),
            date(2025, 12, 31),
        )
        .await
        .unwrap();
    assert_eq!(releases.len(), 4, "{releases:#?}");
    for release in &releases {
        assert_eq!(release.timing, ReleaseTiming::AfterClose, "{release:#?}");
        assert!(release.date.day() > 10, "{release:#?}");
    }
}

#[tokio::test]
#[ignore = "reaches sec.gov; needs SEC_EDGAR_USER_AGENT"]
async fn a_foreign_private_issuer_is_refused() {
    // Taiwan Semiconductor files 20-F and 6-K reports.
    let tickers = client().company_tickers().await.unwrap();
    let tsm = tickers.iter().find(|t| t.ticker == "TSM").unwrap().cik;
    let error = client()
        .earnings_releases(tsm, "TSM", date(2025, 1, 1), date(2025, 12, 31))
        .await
        .unwrap_err();
    assert_eq!(error, SecEdgarError::ForeignPrivateIssuer { cik: tsm });
}
