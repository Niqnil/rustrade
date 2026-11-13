//! Alpha Vantage (behind the `alpha-vantage` feature): earnings releases, coming and past, as
//! [`EarningsRelease`]s. **Its data is for personal, non-commercial use only**; see
//! ["Data use"](#data-use).
//!
//! Alpha Vantage is a market data provider with a free API key
//! (<https://www.alphavantage.co/support/#api-key>). [`AlphaVantageClient`] reads two of its
//! endpoints:
//!
//! - **The earnings calendar** ([`AlphaVantageClient::earnings_calendar`]): the releases it
//!   expects in the next three, six or twelve months, for one company or for all it covers.
//! - **The earnings history** ([`AlphaVantageClient::earnings_history`]): one company's past
//!   quarterly releases, back to the 1990s for long-listed companies.
//!
//! # Data use
//! Alpha Vantage's terms of service (<https://www.alphavantage.co/terms_of_service/>) grant the
//! data for personal, non-commercial use. Use by or for a firm, and giving others access to the
//! data, are commercial uses that need a commercial licence from Alpha Vantage. This library's
//! MIT licence covers its code, not the data it retrieves: do not commit, publish or redistribute
//! the data.
//!
//! # What the records say
//! - **Fiscal period ends are month ends.** Alpha Vantage rounds a period's last day to the end of
//!   its month: a quarter that ended on Saturday 26 September reads 30 September. The records'
//!   [`key`](crate::earnings::EarningsRelease::key) names a period by its nearest month end, so
//!   they match records of the same release from providers that give the exact day.
//! - **Time of day is before the open, after the close, or not given.** Alpha Vantage gives no
//!   time; the calendar leaves the time of day empty for most rows.
//! - **The calendar holds only dates Alpha Vantage has.** A company whose next date it does not
//!   have yet is missing, even within the horizon, and it does not say whether a date is
//!   announced or estimated ([`ReleaseStatus::Unknown`](crate::earnings::ReleaseStatus::Unknown)).
//!   It can include a release dated the day before the request.
//! - **Point in time.** A calendar record is known when fetched. A past release is known by its
//!   session bound (see [`AlphaVantageClient::earnings_history`]), never earlier than it was
//!   public; its exact time is not known. One whose bound has not passed is known when fetched.
//! - **Symbols** are Alpha Vantage's: it writes share classes with either `.` or `-`.
//!
//! # Refusals
//! Alpha Vantage answers a refused request (over the request rate or the daily allowance, or a
//! premium-only call) with HTTP 200 and a notice in place of the data. On the calendar, which is
//! CSV, the notice arrives cut into the CSV's columns with its text lost. The client reads both
//! as [`AlphaVantageError::Refused`].
//!
//! # Example
//! ```no_run
//! # #[cfg(feature = "alpha-vantage")]
//! # async fn example() -> Result<(), rustrade_data::exchange::alpha_vantage::AlphaVantageError> {
//! use rustrade_data::exchange::alpha_vantage::{AlphaVantageClient, CalendarHorizon};
//!
//! let client = AlphaVantageClient::new(&std::env::var("ALPHA_VANTAGE_API_KEY").unwrap_or_default())?;
//! for release in client.earnings_calendar(Some("IBM"), CalendarHorizon::TwelveMonths).await? {
//!     println!("{} expected on {} ({:?})", release.symbol, release.date, release.timing);
//! }
//! for release in client.earnings_history("IBM").await? {
//!     println!("{} reported {} on {}", release.symbol, release.fiscal_period_end, release.date);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`EarningsRelease`]: crate::earnings::EarningsRelease

mod client;
mod error;
mod records;

pub use client::{AlphaVantageClient, CalendarHorizon, DEFAULT_BASE_URL};
pub use error::AlphaVantageError;
