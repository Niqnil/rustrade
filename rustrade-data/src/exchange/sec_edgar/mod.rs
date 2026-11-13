//! SEC EDGAR (behind the `sec-edgar` feature): past earnings releases of US-listed companies, read
//! from the 8-K filings that furnish them, as [`EarningsRelease`]s.
//!
//! EDGAR is the U.S. Securities and Exchange Commission's filing system. No account or key is
//! needed, but SEC requires every automated request to declare its sender (see
//! [`SecEdgarClient`]'s "User-Agent") and allows at most 10 requests a second.
//!
//! # Data use
//! SEC states that information presented on sec.gov is public information that may be copied or
//! further distributed without its permission, and asks that SEC be cited as the source. "SEC" and
//! "EDGAR" are SEC's registered trademarks. See "Website Dissemination" in
//! <https://www.sec.gov/about/privacy-information> and the fair-access rules in
//! <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data>.
//!
//! # Earnings releases
//! A company that publicly announces a completed quarter's or year's results furnishes the
//! announcement to SEC on Form 8-K under Item 2.02, "Results of Operations and Financial
//! Condition". [`SecEdgarClient::earnings_releases`] finds those filings in the company's filing
//! index and pairs each with the fiscal period it reports:
//!
//! - **One release per fiscal period.** For each 10-Q or 10-K, whose report date is the period's
//!   end, the release is the **latest** Item 2.02 8-K filed after that end and no later than the
//!   report (nor than the next period's end). An issuer can furnish other figures under Item 2.02
//!   between a period's end and its results, such as quarterly production and delivery numbers or
//!   preliminary results; the full results come last.
//! - **The newest period appears once its report is filed.** Until then nothing in the index says
//!   which Item 2.02 filing is its release, so it is left out rather than guessed. Large companies
//!   often file the 10-Q with the release or days later; a 10-Q is due 40 to 45 days after the
//!   quarter ends and a 10-K 60 to 90 days after the year ends.
//! - **Dated by the 8-K's acceptance.** The release's [`date`](crate::earnings::EarningsRelease::date),
//!   [`timing`](crate::earnings::EarningsRelease::timing) (against the regular session, 09:30 to 16:00 New York
//!   time) and [`known_at`](crate::earnings::EarningsRelease::known_at) come from the acceptance time in the
//!   filing's header. Companies usually furnish the 8-K with the release, minutes after it, but
//!   Item 2.02 allows up to four business days: a late filing dates its release late. Early closes
//!   are not known here, so a release after an early close but before 16:00 reads as
//!   [`DuringSession`](crate::earnings::ReleaseTiming::DuringSession).
//!
//! What it misses:
//! - **Foreign private issuers** file 20-F, 40-F and 6-K reports, which carry no item numbers:
//!   [`SecEdgarError::ForeignPrivateIssuer`].
//! - **Results first disclosed in the 10-Q or 10-K**, with no 8-K: no release.
//! - **Older filings**: EDGAR's item lists are complete from 2012; some earlier 8-Ks lack them, so
//!   their releases can be missing.
//! - **Coming releases**: EDGAR records filings only after the fact.
//!
//! # Example
//! ```no_run
//! # #[cfg(feature = "sec-edgar")]
//! # async fn example() -> Result<(), rustrade_data::exchange::sec_edgar::SecEdgarError> {
//! use chrono::NaiveDate;
//! use rustrade_data::exchange::sec_edgar::SecEdgarClient;
//!
//! let client = SecEdgarClient::new("Sample Company Name admin@example.com")?;
//! let tickers = client.company_tickers().await?;
//! let Some(apple) = tickers.iter().find(|t| t.ticker == "AAPL") else {
//!     return Ok(());
//! };
//! let releases = client
//!     .earnings_releases(
//!         apple.cik,
//!         "AAPL",
//!         NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
//!         NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
//!     )
//!     .await?;
//! for release in &releases {
//!     println!(
//!         "{} reported {} on {} ({:?})",
//!         release.symbol, release.fiscal_period_end, release.date, release.timing
//!     );
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`EarningsRelease`]: crate::earnings::EarningsRelease

mod client;
mod error;
mod filings;

pub use client::{
    CompanyTicker, DEFAULT_DATA_URL, DEFAULT_WWW_URL, SEC_MAX_REQUESTS_PER_SECOND, SecEdgarClient,
};
pub use error::SecEdgarError;

use serde::{Deserialize, Serialize};

/// A company's Central Index Key, its identifier in EDGAR. Displayed with the ten digits, zero
/// padded, that EDGAR's API paths use (`0000320193`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cik(u64);

impl Cik {
    /// The CIK numbered `cik`.
    pub const fn new(cik: u64) -> Self {
        Self(cik)
    }

    /// The CIK's number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Cik {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:010}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cik_displays_as_ten_digits() {
        assert_eq!(Cik::new(320193).to_string(), "0000320193");
        assert_eq!(Cik::new(1).get(), 1);
    }
}
