//! Stock and index lists, history and snapshots.

use super::{
    client::{MAX_DAYS_DAILY, ThetaDataClient, TimeWindow, interval_params, max_days},
    error::ThetaDataError,
    model::{Bar, EndOfDay, Interval, OhlcSnapshot, Quote, Row, SymbolQuote},
    option::Listing,
};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use futures::Stream;
use rust_decimal::Decimal;
use smol_str::SmolStr;

/// An index's price at a time.
///
/// Exchanges publish a price report for a popular index such as SPX about once a second, and
/// ThetaData omits a report whose price did not change. At an interval, `price` is the price at
/// that exact time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct IndexPrice {
    pub time: DateTime<Utc>,
    pub price: Decimal,
}

/// An index's latest price.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct IndexPriceSnapshot {
    pub symbol: SmolStr,
    pub time: DateTime<Utc>,
    pub price: Decimal,
}

impl ThetaDataClient {
    /// Every stock symbol ThetaData lists (free plan).
    pub fn stock_symbols(
        &self,
    ) -> impl Stream<Item = Result<SmolStr, ThetaDataError>> + Send + use<> {
        self.rows("stock/list/symbols".into(), Vec::new(), |row| {
            row.symbol("symbol")
        })
    }

    /// The days ThetaData has trades or quotes for `symbol` (free plan).
    pub fn stock_dates(
        &self,
        listing: Listing,
        symbol: &str,
    ) -> impl Stream<Item = Result<NaiveDate, ThetaDataError>> + Send + use<> {
        self.rows(
            format!("stock/list/dates/{}", listing.as_str()),
            vec![("symbol", symbol.to_owned())],
            |row| row.date("date"),
        )
    }

    /// OHLC bars for `symbol` over `start..=end` (value plan). Refused for `Interval::Tick`.
    pub fn stock_ohlc_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<Bar, ThetaDataError>> + Send + use<> {
        self.bar_history("stock/history/ohlc", symbol, start, end, interval, window)
    }

    /// NBBO quotes for `symbol` over `start..=end`, every quote (`Interval::Tick`) or the last
    /// one at each interval (value plan).
    pub fn stock_quote_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<Quote, ThetaDataError>> + Send + use<> {
        let mut params = vec![("symbol", symbol.to_owned())];
        params.extend(interval_params(interval, window));
        self.ranged_rows(
            "stock/history/quote".into(),
            params,
            (start, end),
            max_days(interval),
            |row: &Row<'_>| row.quote(),
        )
    }

    /// End-of-day reports for `symbol` over `start..=end` (free plan).
    pub fn stock_end_of_day_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<EndOfDay, ThetaDataError>> + Send + use<> {
        self.end_of_day_history("stock/history/eod", symbol, start, end)
    }

    /// The latest NBBO quote of each symbol (value plan).
    ///
    /// Like the option snapshots, a symbol's latest quote can be from an earlier session;
    /// `current_day_only` keeps only today's (New York date).
    pub fn stock_quote_snapshot(
        &self,
        symbols: &[&str],
        current_day_only: bool,
    ) -> impl Stream<Item = Result<SymbolQuote, ThetaDataError>> + Send + use<> {
        self.rows(
            "stock/snapshot/quote".into(),
            symbols_params(symbols, current_day_only),
            |row: &Row<'_>| row.symbol_quote(),
        )
    }

    /// The current day's OHLC of each symbol (value plan).
    pub fn stock_ohlc_snapshot(
        &self,
        symbols: &[&str],
        current_day_only: bool,
    ) -> impl Stream<Item = Result<OhlcSnapshot, ThetaDataError>> + Send + use<> {
        self.rows(
            "stock/snapshot/ohlc".into(),
            symbols_params(symbols, current_day_only),
            |row: &Row<'_>| row.ohlc_snapshot(),
        )
    }

    /// Every index symbol ThetaData lists (free plan).
    pub fn index_symbols(
        &self,
    ) -> impl Stream<Item = Result<SmolStr, ThetaDataError>> + Send + use<> {
        self.rows("index/list/symbols".into(), Vec::new(), |row| {
            row.symbol("symbol")
        })
    }

    /// The days ThetaData has prices for `symbol` (free plan).
    pub fn index_dates(
        &self,
        symbol: &str,
    ) -> impl Stream<Item = Result<NaiveDate, ThetaDataError>> + Send + use<> {
        self.rows(
            "index/list/dates".into(),
            vec![("symbol", symbol.to_owned())],
            |row| row.date("date"),
        )
    }

    /// Price reports for index `symbol` over `start..=end` (value plan).
    pub fn index_price_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<IndexPrice, ThetaDataError>> + Send + use<> {
        let mut params = vec![("symbol", symbol.to_owned())];
        params.extend(interval_params(interval, window));
        self.ranged_rows(
            "index/history/price".into(),
            params,
            (start, end),
            max_days(interval),
            |row: &Row<'_>| {
                Ok(IndexPrice {
                    time: row.timestamp("timestamp")?,
                    price: row.decimal("price")?,
                })
            },
        )
    }

    /// OHLC bars for index `symbol` over `start..=end` (standard plan). Refused for
    /// `Interval::Tick`.
    pub fn index_ohlc_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<Bar, ThetaDataError>> + Send + use<> {
        self.bar_history("index/history/ohlc", symbol, start, end, interval, window)
    }

    /// End-of-day reports for index `symbol` over `start..=end` (free plan).
    pub fn index_end_of_day_history(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<EndOfDay, ThetaDataError>> + Send + use<> {
        self.end_of_day_history("index/history/eod", symbol, start, end)
    }

    /// The latest price of each index (standard plan).
    pub fn index_price_snapshot(
        &self,
        symbols: &[&str],
        current_day_only: bool,
    ) -> impl Stream<Item = Result<IndexPriceSnapshot, ThetaDataError>> + Send + use<> {
        self.rows(
            "index/snapshot/price".into(),
            symbols_params(symbols, current_day_only),
            |row| {
                Ok(IndexPriceSnapshot {
                    symbol: row.symbol("symbol")?,
                    time: row.timestamp("timestamp")?,
                    price: row.decimal("price")?,
                })
            },
        )
    }

    fn bar_history(
        &self,
        path: &'static str,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: Interval,
        window: Option<TimeWindow>,
    ) -> impl Stream<Item = Result<Bar, ThetaDataError>> + Send + use<> {
        let mut params = vec![("symbol", symbol.to_owned())];
        params.extend(interval_params(interval, window));
        let client = self.clone();
        async_stream::try_stream! {
            if interval == Interval::Tick {
                Err(ThetaDataError::invalid_input("OHLC bars need an interval, not ticks"))?;
            }
            let length = interval.duration().unwrap_or(TimeDelta::zero());
            let rows = client.ranged_rows(
                path.into(),
                params,
                (start, end),
                max_days(interval),
                move |row: &Row<'_>| row.bar(length),
            );
            for await bar in rows {
                yield bar?;
            }
        }
    }

    fn end_of_day_history(
        &self,
        path: &'static str,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<EndOfDay, ThetaDataError>> + Send + use<> {
        self.ranged_rows(
            path.into(),
            vec![("symbol", symbol.to_owned())],
            (start, end),
            MAX_DAYS_DAILY,
            |row: &Row<'_>| row.end_of_day(),
        )
    }
}

pub(crate) fn symbols_params(
    symbols: &[&str],
    current_day_only: bool,
) -> Vec<(&'static str, String)> {
    let mut params = vec![("symbol", symbols.join(","))];
    if current_day_only {
        params.push(("current_day_only", "true".to_owned()));
    }
    params
}
