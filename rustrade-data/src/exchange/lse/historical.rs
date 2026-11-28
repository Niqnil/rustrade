//! Historical candles from LSE Data 3.0, read as verified CSV chunks.
//!
//! ```no_run
//! # use chrono::{Duration, Utc};
//! # use futures::StreamExt;
//! # use rustrade_data::exchange::lse::client::LseClient;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # use rustrade_data::exchange::lse::historical::LseCandleRequest;
//! # use rustrade_data::exchange::lse::market::LseMarketCode;
//! # use rustrade_data::subscription::candle::CandleInterval;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseClient::from_env()?;
//! let end = Utc::now();
//! let request = LseCandleRequest::new(
//!     LseMarketCode::Forex,
//!     "EUR_USD",
//!     CandleInterval::Hour1,
//!     end - Duration::days(7),
//!     end,
//! );
//!
//! let stream = client.fetch_candles(request);
//! // The returned stream holds borrows across await points, so it is `!Unpin` and has to be
//! // pinned before `StreamExt::next` will accept it.
//! futures::pin_mut!(stream);
//!
//! while let Some(candle) = stream.next().await {
//!     println!("{:?}", candle?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # ⚠️ Licensing
//! Candles retrieved here are **not redistributable**. See the
//! [module documentation](super#licensing).

use crate::exchange::lse::PROVIDER_TIMESTAMP_FORMAT;
use crate::exchange::lse::client::LseClient;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::market::{LseMarketCode, candle_interval_str};
use crate::subscription::candle::{
    Candle, CandleInterval, IntervalStep, close_time_from_open, open_time_from_close,
};
use async_stream::try_stream;
use chrono::{DateTime, Datelike, Days, Months, NaiveDate, NaiveDateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::str::FromStr;
use tracing::debug;

/// Format of a timestamp bound sent as `from` / `to`: UTC, no zone suffix, with a fraction only
/// when the instant has one (`%.f` prints 0, 3, 6 or 9 digits as needed).
const BOUND_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Format of a date bound sent as `from` / `to`.
const BOUND_DATE_FORMAT: &str = "%Y-%m-%d";

/// What one candle fetch asks for.
///
/// Built with [`new`](Self::new); `#[non_exhaustive]` so a selection the provider adds can join it
/// without breaking callers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseCandleRequest {
    /// The market the symbol is listed in.
    pub market: LseMarketCode,
    /// The symbol, spelt as the market's catalogue spells it: `AAPL`, `EUR_USD`, `BTCUSDT`,
    /// `SP500`, `VOD` (no `.L`), or an option contract with its root padded to six characters
    /// (`AAPL  261120C00100000`).
    pub symbol: SmolStr,
    /// The resolution. The provider serves 14 of [`CandleInterval`]'s variants.
    pub interval: CandleInterval,
    /// Inclusive lower bound on [`close_time`](Candle::close_time).
    pub start: DateTime<Utc>,
    /// Inclusive upper bound on [`close_time`](Candle::close_time).
    pub end: DateTime<Utc>,
    /// Whether prices and volumes are adjusted for splits (`true`, the provider's default) or
    /// served as published.
    pub adjusted: bool,
}

impl LseCandleRequest {
    /// Candles of `symbol` in `market` at `interval` whose close falls in `[start, end]`, adjusted
    /// for splits.
    pub fn new(
        market: LseMarketCode,
        symbol: impl Into<SmolStr>,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Self {
        Self {
            market,
            symbol: symbol.into(),
            interval,
            start,
            end,
            adjusted: true,
        }
    }

    /// Ask for prices and volumes as published, not adjusted for splits.
    #[must_use]
    pub fn as_published(mut self) -> Self {
        self.adjusted = false;
        self
    }
}

/// The span of one `/v3/export` selection, in either of the two forms the provider accepts.
///
/// The provider takes `from` and `to` as two timestamps or as two dates, never one of each, and the
/// two forms bound differently, so each is its own variant.
///
/// # ⚠️ The provider builds bars from the span it is asked for
/// A candle selection whose span starts or ends mid-period returns a partial bar at that edge that
/// looks whole: hourly bars asked for from 14:30 start with a 14:00 bar built from 14:30 on, and a
/// timestamp span over daily bars returns fragments of sessions. For candles, prefer
/// [`LseClient::fetch_candles`], which aligns every span to whole periods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseWindow {
    /// Instants in `[from, to)`: `from` inclusive, `to` exclusive, to the nanosecond.
    Times {
        /// Inclusive lower bound.
        from: DateTime<Utc>,
        /// Exclusive upper bound.
        to: DateTime<Utc>,
    },
    /// Trading days in `[from, to]`, both inclusive. A daily or longer bar is selected by its
    /// session date.
    Dates {
        /// First trading day.
        from: NaiveDate,
        /// Last trading day, included.
        to: NaiveDate,
    },
}

impl LseWindow {
    /// The `from` and `to` query values, as the provider spells them.
    pub(super) fn bounds(&self) -> (String, String) {
        match self {
            Self::Times { from, to } => (
                from.format(BOUND_TIME_FORMAT).to_string(),
                to.format(BOUND_TIME_FORMAT).to_string(),
            ),
            Self::Dates { from, to } => (
                from.format(BOUND_DATE_FORMAT).to_string(),
                to.format(BOUND_DATE_FORMAT).to_string(),
            ),
        }
    }

    /// Refuse a window whose end comes before its start, which the provider would refuse too.
    pub(super) fn check_order(&self) -> Result<(), LseError> {
        let ordered = match self {
            Self::Times { from, to } => from <= to,
            Self::Dates { from, to } => from <= to,
        };
        if ordered {
            Ok(())
        } else {
            Err(LseError::InvalidInput {
                message: format!("the window's end comes before its start: {self:?}"),
            })
        }
    }

    /// The window as instants: `[from, to)`.
    pub(super) fn instants(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            Self::Times { from, to } => (*from, *to),
            Self::Dates { from, to } => (
                from.and_time(chrono::NaiveTime::MIN).and_utc(),
                to.succ_opt()
                    .unwrap_or(*to)
                    .and_time(chrono::NaiveTime::MIN)
                    .and_utc(),
            ),
        }
    }
}

/// How an interval's periods are labelled, which decides how its windows are aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Period {
    /// A fixed width below a day, on a grid every width of which divides a UTC day.
    Intraday(TimeDelta),
    /// One session, labelled by its date.
    Day,
    /// Monday to Sunday, labelled by its Monday.
    Week,
    /// A calendar month, labelled by its first day.
    Month,
}

impl Period {
    fn of(interval: CandleInterval) -> Self {
        match interval.to_step() {
            IntervalStep::Fixed(width) if width < TimeDelta::days(1) => Self::Intraday(width),
            IntervalStep::Fixed(width) if width < TimeDelta::days(7) => Self::Day,
            IntervalStep::Fixed(_) => Self::Week,
            IntervalStep::Months(_) => Self::Month,
        }
    }
}

/// Floor `time` to the grid of `width`, measured from the Unix epoch.
///
/// Every intraday width the provider serves divides a day, so the epoch grid is the UTC-day grid
/// the provider buckets on (measured: hours at `:00`, four hours at `00/04/08/...` UTC).
fn floor_to_grid(time: DateTime<Utc>, width: TimeDelta) -> Option<DateTime<Utc>> {
    let width = width.num_seconds();
    if width <= 0 {
        return None;
    }
    let seconds = time.timestamp();
    DateTime::from_timestamp(seconds - seconds.rem_euclid(width), 0)
}

/// The aligned windows covering every candle that can close in `[start, end]`, in order.
///
/// # Why aligned
/// The provider **aggregates the window it is asked for** from its ticks: a window starting at
/// 14:30 returns a 14:00 hourly bar built from 14:30 onwards, and one starting on a Wednesday
/// returns that week's bar built from Wednesday on (measured 2026-11-27). Both are fragments that
/// look whole. So every window boundary sits on a period boundary: the interval's grid for an
/// intraday interval, a whole date for a daily one, a Monday for a weekly one and a first of the
/// month for a monthly one. Each window holds at most `chunk_bars` periods.
///
/// # Bounds
/// A candle's close is its open plus the interval, so the opens that can close in `[start, end]`
/// lie in `[start - interval, end - interval]`. A daily or longer bar opens at its session's first
/// trade, which can fall on the evening before its session date (forex: `22:04` UTC), so the date
/// range is widened by a day at the top. The caller trims on close.
pub(super) fn windows(
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    chunk_bars: u32,
) -> Vec<LseWindow> {
    let step = interval.to_step();
    // `None` (underflow near `DateTime::MIN_UTC`) leaves the bound un-widened: a bar whose open is
    // unrepresentable cannot exist, so the un-widened bound is already correct.
    let lowest_open = open_time_from_close(start, step).unwrap_or(start);
    let highest_open = open_time_from_close(end, step).unwrap_or(end);
    let chunk_bars = chunk_bars.max(1);
    let mut windows = Vec::new();

    match Period::of(interval) {
        Period::Intraday(width) => {
            let (Some(from), Some(last)) = (
                floor_to_grid(lowest_open, width),
                floor_to_grid(highest_open, width),
            ) else {
                return windows;
            };
            let to = last.checked_add_signed(width).unwrap_or(last);
            // `None` when `chunk_bars` periods overflow a `TimeDelta`: one window then spans the
            // whole range, which is what so many bars per chunk asks for.
            let span = i32::try_from(chunk_bars)
                .ok()
                .and_then(|bars| width.checked_mul(bars));
            let mut cursor = from;
            while cursor < to {
                let next = span
                    .and_then(|span| cursor.checked_add_signed(span))
                    .map_or(to, |next| next.min(to));
                windows.push(LseWindow::Times {
                    from: cursor,
                    to: next,
                });
                cursor = next;
            }
        }
        period => {
            let mut from = lowest_open.date_naive();
            let mut to = highest_open
                .date_naive()
                .checked_add_days(Days::new(1))
                .unwrap_or(highest_open.date_naive());
            match period {
                Period::Week => {
                    from = from
                        .checked_sub_days(Days::new(u64::from(
                            from.weekday().num_days_from_monday(),
                        )))
                        .unwrap_or(from);
                    to = to
                        .checked_add_days(Days::new(u64::from(
                            6 - to.weekday().num_days_from_monday(),
                        )))
                        .unwrap_or(to);
                }
                Period::Month => {
                    from = from.with_day(1).unwrap_or(from);
                    to = to
                        .with_day(1)
                        .and_then(|first| first.checked_add_months(Months::new(1)))
                        .and_then(|next| next.pred_opt())
                        .unwrap_or(to);
                }
                Period::Day | Period::Intraday(_) => {}
            }
            let mut cursor = from;
            while cursor <= to {
                // The last date of a chunk of `chunk_bars` periods starting at `cursor`.
                let next = match period {
                    Period::Week => cursor.checked_add_days(Days::new(7 * u64::from(chunk_bars))),
                    Period::Month => cursor.checked_add_months(Months::new(chunk_bars)),
                    Period::Day | Period::Intraday(_) => {
                        cursor.checked_add_days(Days::new(u64::from(chunk_bars)))
                    }
                };
                let last = next
                    .and_then(|next| next.pred_opt())
                    .map_or(to, |last| last.min(to));
                windows.push(LseWindow::Dates {
                    from: cursor,
                    to: last,
                });
                match last.succ_opt() {
                    Some(following) => cursor = following,
                    None => break,
                }
            }
        }
    }

    windows
}

/// One candle row as the provider writes it, in CSV or in Parquet.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CandleRow {
    pub(super) open_time: DateTime<Utc>,
    /// The session date of a daily or longer bar (the `date` column), else `None`.
    pub(super) session: Option<NaiveDate>,
    pub(super) open: Decimal,
    pub(super) high: Decimal,
    pub(super) low: Decimal,
    pub(super) close: Decimal,
    pub(super) volume: Option<Decimal>,
    pub(super) trades: Option<u64>,
}

/// Parse a provider timestamp (`YYYY-MM-DD hh:mm:ss[.fraction]`, UTC, no zone suffix).
pub(super) fn parse_provider_time(label: &str) -> Result<DateTime<Utc>, LseError> {
    NaiveDateTime::parse_from_str(label, PROVIDER_TIMESTAMP_FORMAT)
        .map(|naive| naive.and_utc())
        .map_err(|error| LseError::Deserialize {
            message: format!("invalid timestamp {label:?}: {error}"),
        })
}

/// Parse a decimal cell. The provider writes plain decimals; scientific notation is accepted too.
pub(super) fn parse_decimal(column: &str, cell: &str) -> Result<Decimal, LseError> {
    Decimal::from_str(cell)
        .or_else(|_| Decimal::from_scientific(cell))
        .map_err(|error| LseError::Deserialize {
            message: format!("column {column}: invalid number {cell:?}: {error}"),
        })
}

/// A CSV answer's header, resolved to column positions once.
pub(super) struct CsvColumns {
    names: csv::StringRecord,
}

impl CsvColumns {
    pub(super) fn new(names: csv::StringRecord) -> Self {
        Self { names }
    }

    /// The position of `name`, which the answer must carry.
    pub(super) fn required(&self, name: &str) -> Result<usize, LseError> {
        self.optional(name)
            .ok_or_else(|| LseError::UnsupportedSchema {
                columns: self.names.iter().collect::<Vec<_>>().join(", "),
            })
    }

    /// The position of `name`, if the answer carries it.
    pub(super) fn optional(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|column| column == name)
    }
}

/// A cell the row must carry, or [`LseError::NullValue`] when it is empty or null.
fn required<'r>(
    record: &'r csv::StringRecord,
    index: usize,
    name: &'static str,
) -> Result<&'r str, LseError> {
    cell(record, index).ok_or(LseError::NullValue { column: name })
}

/// A cell, or `None` when it is empty or the provider's null (`\N`).
pub(super) fn cell(record: &csv::StringRecord, index: usize) -> Option<&str> {
    record
        .get(index)
        .map(str::trim)
        .filter(|cell| !cell.is_empty() && *cell != "\\N")
}

/// Parse a verified CSV answer into its header and records.
pub(super) fn csv_records(body: &[u8]) -> Result<(CsvColumns, Vec<csv::StringRecord>), LseError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(body);
    let columns = CsvColumns::new(
        reader
            .headers()
            .map_err(|error| LseError::Deserialize {
                message: format!("unreadable CSV header: {error}"),
            })?
            .clone(),
    );
    let records = reader
        .records()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| LseError::Deserialize {
            message: format!("unreadable CSV row: {error}"),
        })?;

    Ok((columns, records))
}

fn candle_rows(body: &[u8]) -> Result<Vec<CandleRow>, LseError> {
    let (columns, records) = csv_records(body)?;
    let time = columns.required("time")?;
    let open = columns.required("open")?;
    let high = columns.required("high")?;
    let low = columns.required("low")?;
    let close = columns.required("close")?;
    let volume = columns.optional("volume");
    let trades = columns.optional("trades");
    let date = columns.optional("date");

    records
        .iter()
        .map(|record| {
            Ok(CandleRow {
                open_time: parse_provider_time(required(record, time, "time")?)?,
                session: date
                    .and_then(|index| cell(record, index))
                    .map(|label| {
                        NaiveDate::parse_from_str(label, BOUND_DATE_FORMAT).map_err(|error| {
                            LseError::Deserialize {
                                message: format!("invalid session date {label:?}: {error}"),
                            }
                        })
                    })
                    .transpose()?,
                open: parse_decimal("open", required(record, open, "open")?)?,
                high: parse_decimal("high", required(record, high, "high")?)?,
                low: parse_decimal("low", required(record, low, "low")?)?,
                close: parse_decimal("close", required(record, close, "close")?)?,
                volume: volume
                    .and_then(|index| cell(record, index))
                    .map(|cell| parse_decimal("volume", cell))
                    .transpose()?,
                trades: trades
                    .and_then(|index| cell(record, index))
                    .map(|cell| {
                        cell.parse::<u64>().map_err(|error| LseError::Deserialize {
                            message: format!("column trades: invalid count {cell:?}: {error}"),
                        })
                    })
                    .transpose()?,
            })
        })
        .collect()
}

/// The bar's position in its series: its open for an intraday interval, its session date (at
/// midnight UTC) for a daily or longer one, whose opens float with its first trade.
fn ordinal(row: &CandleRow, period: Period) -> Result<DateTime<Utc>, LseError> {
    match period {
        Period::Intraday(_) => Ok(row.open_time),
        Period::Day | Period::Week | Period::Month => row
            .session
            .map(|date| date.and_time(chrono::NaiveTime::MIN).and_utc())
            .ok_or(LseError::NullValue { column: "date" }),
    }
}

/// The checks every candle row of one series passes, in arrival order, and the conversion of a row
/// into a [`Candle`].
///
/// Shared by the CSV reader ([`LseClient::fetch_candles`]) and the Parquet decoder, so a bar is
/// held to the same rules and given the same `close_time` whichever format it arrived in. State
/// carries across chunks: chunks served out of order, or one repeating another's bars, would pass a
/// per-chunk check and still corrupt the series.
#[derive(Debug, Clone)]
pub(super) struct CandleSeries {
    /// Names the series in errors: the symbol, or the file being decoded.
    label: String,
    interval: CandleInterval,
    step: IntervalStep,
    period: Period,
    /// The previous bar's position and open.
    previous: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl CandleSeries {
    pub(super) fn new(label: impl Into<String>, interval: CandleInterval) -> Self {
        Self {
            label: label.into(),
            interval,
            step: interval.to_step(),
            period: Period::of(interval),
            previous: None,
        }
    }

    /// Whether a bar of this series carries a session date (`1d` and longer).
    #[cfg(feature = "lse-parquet")]
    pub(super) fn has_sessions(&self) -> bool {
        !matches!(self.period, Period::Intraday(_))
    }

    /// Check the next row and convert it into a [`Candle`] whose `close_time` is its open plus the
    /// interval.
    ///
    /// `window`, when given, is the span the row was asked for: a row opening outside it is
    /// [`LseError::UnexpectedCandleRange`].
    ///
    /// # Errors
    /// A bar not after the previous one is [`LseError::NonMonotonicCandles`]; intraday bars spaced
    /// closer than the interval are [`LseError::UnexpectedCandleResolution`]; a daily or longer bar
    /// without its session date is [`LseError::NullValue`]; a close past the representable range is
    /// [`LseError::TimestampOverflow`].
    pub(super) fn accept(
        &mut self,
        row: &CandleRow,
        window: Option<&LseWindow>,
    ) -> Result<Candle, LseError> {
        let position = ordinal(row, self.period)?;
        if let Some(window) = window {
            let (window_start, window_end) = window.instants();
            let inside = match window {
                LseWindow::Times { from, to } => row.open_time >= *from && row.open_time < *to,
                LseWindow::Dates { .. } => position >= window_start && position < window_end,
            };
            if !inside {
                return Err(LseError::UnexpectedCandleRange {
                    symbol: self.label.clone(),
                    open: row.open_time,
                    window_start,
                    window_end,
                });
            }
        }

        if let Some((previous_position, previous_open)) = self.previous {
            if position <= previous_position {
                return Err(LseError::NonMonotonicCandles {
                    symbol: self.label.clone(),
                    previous_open,
                    open: row.open_time,
                });
            }
            if let Period::Intraday(width) = self.period {
                let actual = row.open_time - previous_open;
                if actual < width {
                    return Err(LseError::UnexpectedCandleResolution {
                        symbol: self.label.clone(),
                        interval: self.interval,
                        previous_open,
                        open: row.open_time,
                        actual,
                    });
                }
            }
        }
        self.previous = Some((position, row.open_time));

        let close_time =
            close_time_from_open(row.open_time, self.step).ok_or(LseError::TimestampOverflow {
                open: row.open_time,
                interval: self.interval,
            })?;

        Ok(Candle {
            close_time,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
            trade_count: row.trades,
        })
    }
}

impl LseClient {
    /// Fetch historical candles, one verified chunk at a time.
    ///
    /// Returns a [`Stream`] that downloads, verifies and yields one chunk of at most
    /// [`chunk_bars`](LseClient::with_chunk_bars) bars before asking for the next. For a `Vec`, see
    /// [`collect_candles`](Self::collect_candles).
    ///
    /// # Range contract
    /// Yields exactly the candles whose [`close_time`](Candle::close_time) falls in
    /// `[request.start, request.end]` (**both inclusive**), in ascending order.
    ///
    /// `close_time` is computed here as the bar's open plus the interval, via the shared boundary
    /// helper. For a daily, weekly or monthly bar the provider's open is the **first trade** of the
    /// session, week or month (US stocks: `09:00` UTC, the start of pre-market; forex: `22:04` UTC
    /// on the evening before the session date; crypto: midnight), so `close_time` lands a period
    /// after that first trade. Sessions are shorter than a day, so this is never before the session
    /// ends: a backtest sees a daily bar no earlier than its session's close, and sometimes later.
    ///
    /// # Windows are aligned to whole periods
    /// The provider aggregates whatever window it is asked for, so a window cut mid-period would
    /// return a fragment that looks like a whole bar. Each chunk's window is aligned to whole
    /// periods instead, which is why the first and last chunks may reach past the range: the extra
    /// bars are trimmed on close.
    ///
    /// # Intraday bars include extended hours
    /// US stock bars run from pre-market to after-hours (the first hourly bar of a day opens at
    /// `09:00` UTC). Periods with no trade are omitted, so consecutive bars are not guaranteed to
    /// be one interval apart; fill a dense grid yourself if you need one.
    ///
    /// # A chunk with nothing stored yields nothing
    /// The provider answers a window holding no stored bars `404 data-unavailable`, and gives that
    /// answer both for dates with no trading and for dates it has not loaded. Such a chunk yields
    /// no bars rather than an error, so the result does not depend on how the range is chunked; an
    /// empty stretch therefore does not prove there was no trading.
    ///
    /// # Arguments
    /// See [`LseCandleRequest`]. An interval the provider does not serve is rejected up front with
    /// [`LseError::UnsupportedInterval`].
    ///
    /// # Errors
    /// Each yielded item is a `Result`, and the first error ends the stream. Resume after one by
    /// re-calling with `start` set past the last `close_time` received.
    /// - An inverted range is [`LseError::InvalidInput`].
    /// - A refusal is [`LseError::Api`] with the provider's code (`symbol-unknown`,
    ///   `plan-required`, `history-limit`, `export-too-wide`, ...), and a limit reached is
    ///   [`LseError::RateLimited`].
    /// - A chunk whose body disagrees with its checksum or size is [`LseError::Api`] with the
    ///   success status and no code; nothing from it is yielded.
    /// - A chunk carrying a bar outside its window is [`LseError::UnexpectedCandleRange`], bars out
    ///   of order are [`LseError::NonMonotonicCandles`], and intraday bars spaced closer than the
    ///   interval are [`LseError::UnexpectedCandleResolution`]. All are checked before any bar of
    ///   that chunk is yielded.
    ///
    /// # Request count
    /// One download per chunk, every chunk of the range requested even where it holds no bars (a
    /// weekend, a quiet symbol): a missing chunk cannot be told from a skipped one. Each download
    /// spends the plan's rows; see [`usage`](Self::usage).
    #[must_use = "fetch_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_candles(
        &self,
        request: LseCandleRequest,
    ) -> impl Stream<Item = Result<Candle, LseError>> + '_ {
        try_stream! {
            let timeframe = candle_interval_str(request.interval)
                .ok_or(LseError::UnsupportedInterval { interval: request.interval })?;

            // An inverted range is a caller error, not an empty result.
            if request.start > request.end {
                Err(LseError::InvalidInput {
                    message: format!(
                        "start ({}) must not be after end ({})",
                        request.start, request.end
                    ),
                })?;
            }

            let symbol = request.symbol.to_string();
            let mut series = CandleSeries::new(symbol.clone(), request.interval);

            for window in windows(
                request.interval,
                request.start,
                request.end,
                self.chunk_bars().get(),
            ) {
                let (from, to) = window.bounds();
                let query = [
                    ("market", request.market.as_str().to_owned()),
                    ("symbol", symbol.clone()),
                    ("data", "candles".to_owned()),
                    ("timeframe", timeframe.to_owned()),
                    ("from", from),
                    ("to", to),
                    // `/v3/candles` answers Parquet by default; CSV is asked for explicitly.
                    ("format", "csv".to_owned()),
                    ("adjusted", if request.adjusted { "1" } else { "0" }.to_owned()),
                ];
                let Some((body, _)) = self.get_export(&query).await? else {
                    debug!(%symbol, ?window, "lse candle chunk: no stored data");
                    continue;
                };
                let rows = candle_rows(&body)?;
                debug!(%symbol, ?window, rows = rows.len(), "lse candle chunk received");

                // Every structural check runs over the whole chunk before any bar is yielded.
                let mut candles = Vec::with_capacity(rows.len());
                for row in &rows {
                    let candle = series.accept(row, Some(&window))?;
                    if candle.close_time >= request.start && candle.close_time <= request.end {
                        candles.push(candle);
                    }
                }

                for candle in candles {
                    yield candle;
                }
            }
        }
    }

    /// Fetch historical candles into a `Vec`.
    ///
    /// Convenience over [`fetch_candles`](Self::fetch_candles) with the same range contract, for
    /// slices small enough to hold in memory.
    ///
    /// # Errors
    /// Fails on the first error, discarding any candles already received. See
    /// [`fetch_candles`](Self::fetch_candles).
    pub async fn collect_candles(
        &self,
        request: LseCandleRequest,
    ) -> Result<Vec<Candle>, LseError> {
        let stream = self.fetch_candles(request);
        futures::pin_mut!(stream);

        let mut candles = Vec::new();
        while let Some(candle) = stream.next().await {
            candles.push(candle?);
        }

        Ok(candles)
    }
}

/// Mock answers in the provider's file shape, for this crate's unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use sha2::{Digest, Sha256};

    /// A verified CSV answer: the body with its SHA-256, and its data row count in the
    /// `x-export-rows` header the provider sends (which the client does not check).
    pub(crate) fn csv_answer(body: &str) -> wiremock::ResponseTemplate {
        let rows = body.lines().count().saturating_sub(1);
        wiremock::ResponseTemplate::new(200)
            .insert_header(
                "x-checksum-sha256",
                hex::encode(Sha256::digest(body.as_bytes())),
            )
            .insert_header("x-export-rows", rows.to_string())
            .set_body_raw(body.to_owned(), "text/csv; charset=utf-8")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(text: &str) -> DateTime<Utc> {
        parse_provider_time(text).unwrap()
    }

    fn d(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, BOUND_DATE_FORMAT).unwrap()
    }

    #[test]
    fn intraday_windows_sit_on_the_grid_even_when_the_range_does_not() {
        // Closes in [14:30, 16:30] → opens in [13:30, 15:30] → grid [13:00, 16:00).
        let windows = windows(
            CandleInterval::Hour1,
            t("2026-11-20 14:30:00"),
            t("2026-11-20 16:30:00"),
            50_000,
        );

        assert_eq!(
            windows,
            [LseWindow::Times {
                from: t("2026-11-20 13:00:00"),
                to: t("2026-11-20 16:00:00"),
            }]
        );
    }

    #[test]
    fn intraday_windows_split_into_chunks_of_whole_bars() {
        let windows = windows(
            CandleInterval::Min1,
            t("2026-11-20 00:01:00"),
            t("2026-11-20 00:05:00"),
            2,
        );

        assert_eq!(
            windows,
            [
                LseWindow::Times {
                    from: t("2026-11-20 00:00:00"),
                    to: t("2026-11-20 00:02:00")
                },
                LseWindow::Times {
                    from: t("2026-11-20 00:02:00"),
                    to: t("2026-11-20 00:04:00")
                },
                LseWindow::Times {
                    from: t("2026-11-20 00:04:00"),
                    to: t("2026-11-20 00:05:00")
                },
            ]
        );
    }

    #[test]
    fn four_hour_windows_sit_on_the_utc_day_grid() {
        let [LseWindow::Times { from, to }] = windows(
            CandleInterval::Hour4,
            t("2026-11-20 05:00:00"),
            t("2026-11-20 13:00:00"),
            50_000,
        )[..] else {
            panic!("one window expected");
        };

        assert_eq!(
            (from, to),
            (t("2026-11-20 00:00:00"), t("2026-11-20 12:00:00"))
        );
    }

    #[test]
    fn chunk_bars_too_many_for_a_time_delta_give_one_window() {
        // Four hours times `i32::MAX` overflows a `TimeDelta`.
        let [LseWindow::Times { from, to }] = windows(
            CandleInterval::Hour4,
            t("2026-11-20 05:00:00"),
            t("2026-11-22 13:00:00"),
            u32::MAX,
        )[..] else {
            panic!("one window expected");
        };

        assert_eq!(
            (from, to),
            (t("2026-11-20 00:00:00"), t("2026-11-22 12:00:00"))
        );
    }

    #[test]
    fn daily_windows_are_whole_dates_widened_a_day_at_the_top() {
        // Closes in [11-19, 11-21] → opens in [11-18, 11-20] → dates [11-18, 11-21].
        let windows = windows(
            CandleInterval::Day1,
            t("2026-11-19 00:00:00"),
            t("2026-11-21 00:00:00"),
            50_000,
        );

        assert_eq!(
            windows,
            [LseWindow::Dates {
                from: d("2026-11-18"),
                to: d("2026-11-21")
            }]
        );
    }

    #[test]
    fn daily_windows_split_into_chunks_of_whole_dates() {
        let windows = windows(
            CandleInterval::Day1,
            t("2026-11-02 00:00:00"),
            t("2026-11-06 00:00:00"),
            2,
        );

        assert_eq!(
            windows,
            [
                LseWindow::Dates {
                    from: d("2026-11-01"),
                    to: d("2026-11-02")
                },
                LseWindow::Dates {
                    from: d("2026-11-03"),
                    to: d("2026-11-04")
                },
                LseWindow::Dates {
                    from: d("2026-11-05"),
                    to: d("2026-11-06")
                },
            ]
        );
    }

    #[test]
    fn weekly_windows_run_monday_to_sunday() {
        // 2026-11-18 is a Wednesday.
        let windows = windows(
            CandleInterval::Week1,
            t("2026-11-18 00:00:00"),
            t("2026-11-25 00:00:00"),
            50_000,
        );

        assert_eq!(
            windows,
            [LseWindow::Dates {
                from: d("2026-11-09"),
                to: d("2026-11-22")
            }]
        );
    }

    #[test]
    fn weekly_chunks_stay_on_mondays() {
        let windows = windows(
            CandleInterval::Week1,
            t("2026-11-16 00:00:00"),
            t("2026-12-07 00:00:00"),
            1,
        );

        for window in &windows {
            let LseWindow::Dates { from, to } = window else {
                panic!("dates expected");
            };
            assert_eq!(from.weekday(), chrono::Weekday::Mon, "{window:?}");
            assert_eq!(to.weekday(), chrono::Weekday::Sun, "{window:?}");
        }
        assert_eq!(windows.len(), 4);
    }

    #[test]
    fn monthly_windows_run_from_the_first_to_the_last_day() {
        let windows = windows(
            CandleInterval::Month1,
            t("2026-10-15 00:00:00"),
            t("2026-11-15 00:00:00"),
            50_000,
        );

        assert_eq!(
            windows,
            [LseWindow::Dates {
                from: d("2026-09-01"),
                to: d("2026-10-31")
            }]
        );
    }

    #[test]
    fn a_degenerate_range_still_covers_the_bar_closing_on_it() {
        let instant = t("2026-11-20 15:00:00");
        let windows = windows(CandleInterval::Min1, instant, instant, 50_000);

        assert_eq!(
            windows,
            [LseWindow::Times {
                from: t("2026-11-20 14:59:00"),
                to: t("2026-11-20 15:00:00")
            }]
        );
    }

    #[test]
    fn floor_to_grid_handles_instants_before_the_epoch() {
        let before = Utc.with_ymd_and_hms(1969, 12, 31, 23, 30, 0).unwrap();

        assert_eq!(
            floor_to_grid(before, TimeDelta::hours(1)).unwrap(),
            Utc.with_ymd_and_hms(1969, 12, 31, 23, 0, 0).unwrap()
        );
    }

    #[test]
    fn a_candle_csv_decodes_with_volume_and_trades() {
        let body = b"time,open,high,low,close,volume,trades\n\
            2026-11-20 15:00:00,348.34,349.1,347.9,348.5,873,42\n";

        let rows = candle_rows(body).unwrap();

        assert_eq!(
            rows,
            [CandleRow {
                open_time: t("2026-11-20 15:00:00"),
                session: None,
                open: Decimal::from_str("348.34").unwrap(),
                high: Decimal::from_str("349.1").unwrap(),
                low: Decimal::from_str("347.9").unwrap(),
                close: Decimal::from_str("348.5").unwrap(),
                volume: Some(Decimal::from(873)),
                trades: Some(42),
            }]
        );
    }

    #[test]
    fn a_daily_candle_carries_its_session_date_and_a_fractional_adjusted_volume() {
        let body = b"time,open,high,low,close,volume,trades,date\n\
            2026-11-17 22:04:00,1.1,1.2,1.0,1.15,12.5,3,2026-11-18\n";

        let rows = candle_rows(body).unwrap();

        assert_eq!(rows[0].session, Some(d("2026-11-18")));
        assert_eq!(rows[0].volume, Some(Decimal::from_str("12.5").unwrap()));
    }

    #[test]
    fn an_empty_or_null_volume_is_none() {
        let body = b"time,open,high,low,close,volume,trades\n\
            2026-11-20 15:00:00,1,1,1,1,,\\N\n";

        let rows = candle_rows(body).unwrap();

        assert_eq!((rows[0].volume, rows[0].trades), (None, None));
    }

    #[test]
    fn a_missing_price_column_is_an_unsupported_schema() {
        let body = b"time,open,high,low\n2026-11-20 15:00:00,1,1,1\n";

        assert!(matches!(
            candle_rows(body),
            Err(LseError::UnsupportedSchema { .. })
        ));
    }

    #[test]
    fn a_null_price_is_a_typed_error() {
        let body = b"time,open,high,low,close\n2026-11-20 15:00:00,1,1,1,\\N\n";

        assert!(matches!(
            candle_rows(body),
            Err(LseError::NullValue { column: "close" })
        ));
    }

    fn bar(open_time: DateTime<Utc>, session: Option<NaiveDate>) -> CandleRow {
        CandleRow {
            open_time,
            session,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: None,
            trades: None,
        }
    }

    /// Whether a fresh series accepts `row` in `window`, so each check sees no earlier bar.
    fn inside(interval: CandleInterval, row: &CandleRow, window: &LseWindow) -> bool {
        match CandleSeries::new("TEST", interval).accept(row, Some(window)) {
            Ok(_) => true,
            Err(LseError::UnexpectedCandleRange { .. }) => false,
            Err(other) => panic!("a range verdict expected, got {other}"),
        }
    }

    #[test]
    fn a_time_window_holds_its_start_and_refuses_its_end() {
        let window = LseWindow::Times {
            from: t("2026-11-20 15:00:00"),
            to: t("2026-11-20 15:02:00"),
        };
        let minute = |text| bar(t(text), None);

        assert!(!inside(
            CandleInterval::Min1,
            &minute("2026-11-20 14:59:00"),
            &window
        ));
        assert!(inside(
            CandleInterval::Min1,
            &minute("2026-11-20 15:00:00"),
            &window
        ));
        assert!(inside(
            CandleInterval::Min1,
            &minute("2026-11-20 15:01:00"),
            &window
        ));
        assert!(!inside(
            CandleInterval::Min1,
            &minute("2026-11-20 15:02:00"),
            &window
        ));
    }

    #[test]
    fn a_date_window_holds_both_its_days_by_session() {
        let window = LseWindow::Dates {
            from: d("2026-11-17"),
            to: d("2026-11-18"),
        };
        // The open is the previous evening's, as the provider stamps a session; the date decides.
        let session = |day| bar(t("2026-11-16 22:00:00"), Some(d(day)));

        assert!(!inside(
            CandleInterval::Day1,
            &session("2026-11-16"),
            &window
        ));
        assert!(inside(
            CandleInterval::Day1,
            &session("2026-11-17"),
            &window
        ));
        assert!(inside(
            CandleInterval::Day1,
            &session("2026-11-18"),
            &window
        ));
        assert!(!inside(
            CandleInterval::Day1,
            &session("2026-11-19"),
            &window
        ));
    }

    #[test]
    fn scientific_notation_is_read() {
        assert_eq!(
            parse_decimal("price", "1e-05").unwrap(),
            Decimal::from_str("0.00001").unwrap()
        );
    }
}
