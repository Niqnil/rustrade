//! Option lists, history and snapshots.

use super::{
    client::{MAX_DAYS_INTRADAY, ThetaDataClient, TimeWindow, interval_params, max_days},
    error::ThetaDataError,
    model::{Bar, EndOfDay, Expiration, Interval, Nbbo, OptionContract, OptionSelection, Row},
    time,
};
use crate::subscription::book::OrderBookL1;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use futures::Stream;
use rust_decimal::Decimal;
use smol_str::SmolStr;

/// Which events a date or contract listing counts: days or contracts with trades, or with quotes.
///
/// Quotes are the wider listing: a contract can be quoted on a day it never trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Listing {
    Trade,
    Quote,
}

impl Listing {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::Quote => "quote",
        }
    }
}

/// An option's NBBO quote at a time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OptionQuote {
    pub contract: OptionContract,
    pub time: DateTime<Utc>,
    pub nbbo: Nbbo,
}

impl From<&OptionQuote> for OrderBookL1 {
    fn from(quote: &OptionQuote) -> Self {
        quote.nbbo.top_of_book(quote.time)
    }
}

/// One OHLC bar of an option.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct OptionBar {
    pub contract: OptionContract,
    pub bar: Bar,
}

/// An option's open interest as OPRA reported it.
///
/// OPRA reports open interest once a day, around 06:30 New York time; `time` is that report's, and
/// the figure is the open interest at the end of the **previous** trading day.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OptionOpenInterest {
    pub contract: OptionContract,
    pub time: DateTime<Utc>,
    pub open_interest: u64,
}

/// An option's end-of-day report.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OptionEndOfDay {
    pub contract: OptionContract,
    pub end_of_day: EndOfDay,
}

/// The current day's OHLC of an option, as of `time`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OptionOhlcSnapshot {
    pub contract: OptionContract,
    pub time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
    pub trade_count: u64,
}

fn decode_quote(row: &Row<'_>) -> Result<OptionQuote, ThetaDataError> {
    Ok(OptionQuote {
        contract: row.contract()?,
        time: row.timestamp("timestamp")?,
        nbbo: row.nbbo()?,
    })
}

fn decode_open_interest(row: &Row<'_>) -> Result<OptionOpenInterest, ThetaDataError> {
    Ok(OptionOpenInterest {
        contract: row.contract()?,
        time: row.timestamp("timestamp")?,
        open_interest: row.u64("open_interest")?,
    })
}

fn decode_end_of_day(row: &Row<'_>) -> Result<OptionEndOfDay, ThetaDataError> {
    Ok(OptionEndOfDay {
        contract: row.contract()?,
        end_of_day: row.end_of_day()?,
    })
}

fn decode_ohlc_snapshot(row: &Row<'_>) -> Result<OptionOhlcSnapshot, ThetaDataError> {
    Ok(OptionOhlcSnapshot {
        contract: row.contract()?,
        time: row.timestamp("timestamp")?,
        open: row.decimal("open")?,
        high: row.decimal("high")?,
        low: row.decimal("low")?,
        close: row.decimal("close")?,
        volume: row.u64("volume")?,
        trade_count: row.u64("count")?,
    })
}

impl ThetaDataClient {
    /// Every option root ThetaData lists (free plan).
    pub fn option_roots(
        &self,
    ) -> impl Stream<Item = Result<SmolStr, ThetaDataError>> + Send + use<> {
        self.rows("option/list/symbols".into(), Vec::new(), |row| {
            row.symbol("symbol")
        })
    }

    /// Every expiration `root` has had, expired ones included (free plan).
    pub fn option_expirations(
        &self,
        root: &str,
    ) -> impl Stream<Item = Result<NaiveDate, ThetaDataError>> + Send + use<> {
        self.rows(
            "option/list/expirations".into(),
            vec![("symbol", root.to_owned())],
            |row| row.date("expiration"),
        )
    }

    /// Every strike `root` has had on `expiration` (free plan).
    pub fn option_strikes(
        &self,
        root: &str,
        expiration: NaiveDate,
    ) -> impl Stream<Item = Result<Decimal, ThetaDataError>> + Send + use<> {
        self.rows(
            "option/list/strikes".into(),
            vec![
                ("symbol", root.to_owned()),
                ("expiration", time::format_date(expiration)),
            ],
            |row| row.decimal("strike"),
        )
    }

    /// The days ThetaData has trades or quotes for the selected contracts (free plan). The
    /// selection must name one expiration; `max_dte` and `strike_range` do not apply.
    pub fn option_dates(
        &self,
        listing: Listing,
        selection: &OptionSelection,
    ) -> impl Stream<Item = Result<NaiveDate, ThetaDataError>> + Send + use<> {
        let checked = match selection.expiration {
            Expiration::On(_) => Ok(selection
                .params()
                .into_iter()
                .filter(|(name, _)| !matches!(*name, "max_dte" | "strike_range"))
                .collect()),
            Expiration::All => Err(ThetaDataError::invalid_input(
                "an option date listing needs one expiration",
            )),
        };
        let client = self.clone();
        let path = format!("option/list/dates/{}", listing.as_str());
        async_stream::try_stream! {
            let params = checked?;
            for await date in client.rows(path, params, |row| row.date("date")) {
                yield date?;
            }
        }
    }

    /// The option contracts with trades or quotes on `date`, expired ones included (value plan).
    /// `roots` narrows the listing to those roots; empty lists every root.
    pub fn option_contracts(
        &self,
        listing: Listing,
        date: NaiveDate,
        roots: &[&str],
    ) -> impl Stream<Item = Result<OptionContract, ThetaDataError>> + Send + use<> {
        let mut params = vec![("date", time::format_date(date))];
        if !roots.is_empty() {
            params.push(("symbol", roots.join(",")));
        }
        self.rows(
            format!("option/list/contracts/{}", listing.as_str()),
            params,
            |row| row.contract(),
        )
    }

    /// NBBO quotes for the selected contracts over `start..=end`, every quote (`Interval::Tick`)
    /// or the last one at each interval (value plan; ticks need standard).
    ///
    /// Each day's rows cover `window`, by default the regular session. A selection of every
    /// expiration is fetched one day per request: ThetaData serves several days of quotes in one
    /// request only for a single expiration.
    pub fn option_quote_history(
        &self,
        selection: &OptionSelection,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<OptionQuote, ThetaDataError>> + Send + use<> {
        let mut params = selection.params();
        params.extend(interval_params(interval, window));
        let days = match selection.expiration {
            Expiration::All => 1,
            Expiration::On(_) => max_days(interval),
        };
        self.ranged_rows(
            "option/history/quote".into(),
            params,
            (start, end),
            days,
            decode_quote,
        )
    }

    /// OHLC bars for the selected contracts over `start..=end` (value plan).
    ///
    /// Refused for `Interval::Tick`, which is not a bar size, and for a selection with `max_dte`,
    /// which this endpoint does not take.
    pub fn option_ohlc_history(
        &self,
        selection: &OptionSelection,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<OptionBar, ThetaDataError>> + Send + use<> {
        let checked = if interval == Interval::Tick {
            Err(ThetaDataError::invalid_input(
                "OHLC bars need an interval, not ticks",
            ))
        } else if selection.max_dte.is_some() {
            Err(ThetaDataError::invalid_input(
                "option OHLC history does not take max_dte",
            ))
        } else {
            Ok(())
        };
        let mut params = selection.params();
        params.extend(interval_params(interval, window));
        let client = self.clone();
        async_stream::try_stream! {
            checked?;
            let length = interval.duration().unwrap_or(TimeDelta::zero());
            let decode = move |row: &Row<'_>| {
                Ok(OptionBar { contract: row.contract()?, bar: row.bar(length)? })
            };
            let rows = client.ranged_rows(
                "option/history/ohlc".into(),
                params,
                (start, end),
                max_days(interval),
                decode,
            );
            for await bar in rows {
                yield bar?;
            }
        }
    }

    /// Open interest for the selected contracts over `start..=end`, one report per contract and
    /// day (value plan). See [`OptionOpenInterest`] for which day a figure describes.
    pub fn option_open_interest_history(
        &self,
        selection: &OptionSelection,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<OptionOpenInterest, ThetaDataError>> + Send + use<> {
        self.ranged_rows(
            "option/history/open_interest".into(),
            selection.params(),
            (start, end),
            MAX_DAYS_INTRADAY,
            decode_open_interest,
        )
    }

    /// End-of-day reports for the selected contracts over `start..=end` (free plan).
    pub fn option_end_of_day_history(
        &self,
        selection: &OptionSelection,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<OptionEndOfDay, ThetaDataError>> + Send + use<> {
        self.ranged_rows(
            "option/history/eod".into(),
            selection.params(),
            (start, end),
            MAX_DAYS_INTRADAY,
            decode_end_of_day,
        )
    }

    /// The latest NBBO quote of each selected contract (value plan). With
    /// [`Expiration::All`], one request returns a whole chain.
    ///
    /// ThetaData returns each contract's latest quote from the past seven days, the most recent
    /// session's outside market hours, and drops contracts after their expiration's session.
    /// `current_day_only` keeps only today's (New York date) quotes.
    pub fn option_quote_snapshot(
        &self,
        selection: &OptionSelection,
        current_day_only: bool,
    ) -> impl Stream<Item = Result<OptionQuote, ThetaDataError>> + Send + use<> {
        self.rows(
            "option/snapshot/quote".into(),
            snapshot_params(selection, current_day_only),
            decode_quote,
        )
    }

    /// The current day's OHLC of each selected contract (value plan).
    pub fn option_ohlc_snapshot(
        &self,
        selection: &OptionSelection,
        current_day_only: bool,
    ) -> impl Stream<Item = Result<OptionOhlcSnapshot, ThetaDataError>> + Send + use<> {
        self.rows(
            "option/snapshot/ohlc".into(),
            snapshot_params(selection, current_day_only),
            decode_ohlc_snapshot,
        )
    }

    /// The latest open interest report of each selected contract (value plan).
    pub fn option_open_interest_snapshot(
        &self,
        selection: &OptionSelection,
        current_day_only: bool,
    ) -> impl Stream<Item = Result<OptionOpenInterest, ThetaDataError>> + Send + use<> {
        self.rows(
            "option/snapshot/open_interest".into(),
            snapshot_params(selection, current_day_only),
            decode_open_interest,
        )
    }
}

fn snapshot_params(
    selection: &OptionSelection,
    current_day_only: bool,
) -> Vec<(&'static str, String)> {
    let mut params = selection.params();
    if current_day_only {
        params.push(("current_day_only", "true".to_owned()));
    }
    params
}
