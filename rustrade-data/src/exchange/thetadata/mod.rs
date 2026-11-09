//! ThetaData market data (behind the `thetadata` feature): US options, stocks, indices, interest
//! rates and the market calendar, through the REST API of a Theta Terminal the caller runs. ⚠️ The
//! retrieved data is **not redistributable**.
//!
//! # ⚠️ Licensing: the data is NOT redistributable
//! This integration's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** ThetaData's individual plans license the data for personal, non-commercial
//! use only, and its terms forbid archiving, reproducing, distributing or publishing it, or making
//! it available to anyone else; commercial use needs a separate business agreement with ThetaData.
//!
//! In practice: do not commit retrieved data to a public repository, do not publish it as fixtures
//! or example datasets, and do not re-serve it. Terms:
//! <https://www.thetadata.net/terms-and-conditions>
//!
//! # The Theta Terminal
//! ThetaData serves its API through the Theta Terminal, a Java application that logs in to the
//! account (API key or credentials file) and serves REST on this host, by default at
//! [`DEFAULT_BASE_URL`]. [`ThetaDataClient`] talks to that API; running the Terminal is the
//! caller's: <https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html>.
//!
//! - **One session per account.** Another Terminal, or another client logging in to the same
//!   account, invalidates this Terminal's session: requests then fail with
//!   [`ThetaDataError::SessionInvalid`] until the Terminal is restarted alone on the account.
//! - **Concurrency is per account**, by the highest subscription tier: free 1, value 2, standard
//!   4, pro 8 requests in flight. See [`ThetaDataClient`] for how the client keeps within it. A
//!   stream holds its request's slot until it ends or is dropped, so at a limit of 1, drain or drop
//!   one stream before polling the next.
//! - **The REST API has no authentication of its own**: anything that reaches the Terminal's port
//!   uses the account, and the same port can serve endpoints beyond the REST API. The Terminal may
//!   listen on every network interface whatever the `host` in its `config.toml` says, so restrict
//!   the port outside it: a host firewall, or a container whose port is published only to
//!   `127.0.0.1` or kept on a private network.
//!
//! # Plans
//! Each method's documentation names the cheapest plan that serves it. A request the account's
//! plan does not include fails with [`ThetaDataError::PermissionDenied`]. Plans also limit how far
//! back history reaches and which intervals are served; ThetaData's pricing page has the current
//! limits. A request for dates before the plan's history starts fails with
//! [`ThetaDataError::InvalidRequest`] (HTTP 400), not with a permission error.
//!
//! # Data characteristics
//! - **Times are New York wall-clock times** in ThetaData's responses. They are converted to UTC
//!   here; see [`time::new_york_to_utc`] for the daylight-saving transitions.
//! - **Prices are exact.** Every price, strike and rate is read from its decimal text.
//! - **Rows are passed through as reported.** A quote side with nothing on it arrives as a zero
//!   price and size, an index price can arrive as zero before the first report of a session, and
//!   an interval with no trades as a bar of zero prices. They are kept as such, not dropped;
//!   [`Nbbo::top_of_book`] maps a zero price to an absent level.
//! - **Option roots are ThetaData's**, which can differ from the underlying's symbol (`SPXW`).
//! - **Open interest describes the previous trading day**: OPRA reports it around 06:30 New York
//!   time.
//! - **Exchange and condition codes are ThetaData's numeric codes**, kept as reported.
//! - **No data is an empty result**, not an error.

mod client;
mod error;
mod model;
mod option;
mod reference;
mod stock;
pub mod time;

pub use client::{DEFAULT_BASE_URL, ThetaDataClient, TimeWindow};
pub use error::ThetaDataError;
pub use model::{
    Bar, EndOfDay, Expiration, Interval, Nbbo, OhlcSnapshot, OptionContract, OptionSelection,
    Quote, Strike, SymbolQuote,
};
pub use option::{
    Listing, OptionBar, OptionEndOfDay, OptionOhlcSnapshot, OptionOpenInterest, OptionQuote,
};
pub use reference::{InterestRate, InterestRateSeries, MarketDay, MarketDayKind, MarketHoliday};
pub use stock::{IndexPrice, IndexPriceSnapshot};
