//! Paged historical candles from the London Strategic Edge vault.
//!
//! ```no_run
//! # use chrono::{Duration, Utc};
//! # use futures::StreamExt;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # use rustrade_data::exchange::lse::vault::LseVaultClient;
//! # use rustrade_data::subscription::candle::CandleInterval;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseVaultClient::from_env()?;
//! let end = Utc::now();
//! let start = end - Duration::days(7);
//!
//! let stream = client.fetch_candles("EUR/USD", CandleInterval::Day1, start, end);
//! // The returned stream is a generator holding borrows across await points, so it is `!Unpin`
//! // and has to be pinned before `StreamExt::next` will accept it.
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
//! Candles retrieved here are **not redistributable**. See the [module documentation](super) and
//! <https://londonstrategicedge.com/terms>.

use crate::exchange::lse::error::LseError;
use crate::exchange::lse::market::candle_interval_str;
use crate::exchange::lse::vault::LseVaultClient;
use crate::subscription::candle::{
    Candle, CandleInterval, IntervalStep, close_time_from_open, open_time_from_close,
};
use async_stream::try_stream;
use chrono::{DateTime, Duration as TimeDelta, NaiveDateTime, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::debug;

/// Format of the `ts` field in a candle row (`2024-01-02 09:09:00.000000`).
///
/// `%.f` makes the fractional part optional, so a response that drops the microseconds still
/// parses. The value carries no timezone and is UTC.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Format accepted by the `start` / `end` query parameters.
///
/// **Second precision, and no finer.** The provider rejects both a sub-second value and the ISO
/// `2024-01-02T09:09:00Z` form with a `400` whose message (`use YYYY-MM-DD`) understates what is
/// actually accepted — the time-of-day *is* honoured.
const CURSOR_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Amount the page cursor advances past the last bar received.
///
/// The cursor is inclusive, so resuming exactly at the last open time would re-yield that bar.
/// One second is the smallest step the parameter accepts.
///
/// # ⚠️ This is lossless only while the finest resolution is one second
/// Bars are at least one second apart, so stepping a full second past the last open can never skip
/// one. Were the provider to publish a sub-second resolution, this step would silently drop bars —
/// revisit it alongside any new [`CandleInterval`] variant below [`Sec1`](CandleInterval::Sec1).
const CURSOR_STEP_SECS: i64 = 1;

/// One candle as the vault serves it.
///
/// The `symbol` field is echoed by the provider and deliberately not captured: the caller already
/// knows what it asked for, and unknown fields are ignored rather than rejected so an added field
/// does not break decoding.
#[derive(Debug, Deserialize)]
struct LseCandleRow {
    /// Bar **open** time, UTC. See [`TIMESTAMP_FORMAT`].
    ts: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// Absent entirely for FX; an integer for equities.
    #[serde(default)]
    volume: Option<Decimal>,
}

impl LseCandleRow {
    /// Parse the row's `ts` as the bar's open instant.
    fn open_time(&self) -> Result<DateTime<Utc>, LseError> {
        NaiveDateTime::parse_from_str(&self.ts, TIMESTAMP_FORMAT)
            .map(|naive| naive.and_utc())
            .map_err(|error| LseError::Deserialize {
                message: format!("invalid candle timestamp {:?}: {error}", self.ts),
            })
    }

    /// Convert into the library [`Candle`] model, given the period-end boundary.
    ///
    /// `trade_count` is unconditionally `None`: the vault reports no trade count for any dataset.
    /// `volume` carries the provider's absence through as `None` rather than as a zero — for FX
    /// the field is omitted entirely, and a synthetic zero would aggregate into a
    /// legitimate-looking total at every derived resolution.
    fn into_candle(self, close_time: DateTime<Utc>) -> Candle {
        Candle {
            close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: None,
        }
    }
}

impl LseVaultClient {
    /// Fetch historical candles for `symbol` at `interval`, paginating automatically.
    ///
    /// Returns a [`Stream`] that processes each page as it arrives rather than buffering the whole
    /// range. For a convenience `Vec`, see [`collect_candles`](Self::collect_candles).
    ///
    /// # Symbol
    /// The vault keys candles on the **display symbol** — `EUR/USD`, `AAPL`, `BP.L`, `ES.F` — not
    /// on a dataset slug. Nothing here needs [`slug`](super::market::slug).
    ///
    /// # Range contract
    /// Yields exactly the candles whose [`close_time`](Candle::close_time) falls in `[start, end]`
    /// (**both inclusive**), matched on `close_time` — the field consumers receive — consistent
    /// with the library's other historical fetches.
    ///
    /// The vault's own range is expressed on the bar's **open** time with an **exclusive** upper
    /// bound, so this method maps both bounds (lower widens to capture the bar whose
    /// `close_time == start`; upper is extended past the final bar's open) and then trims exactly
    /// on `close_time`. The trim also absorbs the cursor's second-precision truncation of
    /// sub-second `start`/`end` values.
    ///
    /// `close_time` is computed library-side as the exclusive period-end boundary
    /// (`open_time + interval`) via the shared boundary helper; the provider reports only the open
    /// instant.
    ///
    /// # Sparseness differs by resolution — and a closed market is not the same as no bar
    /// **Intraday**: periods with no activity are omitted entirely, so consecutive candles are
    /// **not** guaranteed to be one interval apart. This is the opposite of Binance's REST klines,
    /// which server-side gap-fill. Consumers needing a dense grid must fill it themselves.
    ///
    /// **Daily**: the series is **not** sparse. Non-trading days are emitted as *flat* bars —
    /// `open == high == low == close` — for every sampled Saturday and for the US Independence Day
    /// observance; only Sundays are absent. So a backtest sees a tradeable price on a closed market,
    /// and the flat OHLC is the only signal. Do not infer "no bar means the market was closed": see
    /// the [module's data characteristics](super#data-characteristics).
    ///
    /// # Volume
    /// FX candles carry **no volume**: the vault omits the field, which surfaces as
    /// [`volume: None`](Candle::volume) rather than a zero. `trade_count` is `None` for every
    /// dataset — the vault reports none.
    ///
    /// # Arguments
    /// * `symbol` - Display symbol, e.g. `"EUR/USD"` or `"AAPL"`.
    /// * `interval` - Resolution. The provider serves 14 of [`CandleInterval`]'s variants;
    ///   an unserved one is rejected up front with [`LseError::UnsupportedInterval`] rather than
    ///   relayed as a `400`.
    /// * `start` / `end` - Inclusive `close_time` bounds.
    ///
    /// # Errors
    /// Each yielded item is a `Result`. On `429` the stream yields [`LseError::RateLimited`] and
    /// **ends** — resume by re-calling with `start` set past the last `close_time` received. An
    /// inverted range is [`LseError::InvalidInput`]. Other failures surface as
    /// [`LseError::Api`] / [`Http`](LseError::Http) / [`Deserialize`](LseError::Deserialize).
    ///
    /// A page carrying a bar that closes past `end` ends the stream with
    /// [`LseError::UnexpectedCandleRange`] rather than trimming it away: the upper bound is exact
    /// by construction, so such a bar means the range parameters were not honoured. Every in-range
    /// bar on that page is yielded first. Note the lower bound is **not** symmetric — it is widened
    /// deliberately, so bars closing before `start` are trimmed without comment.
    ///
    /// A page whose opens step backwards ends the stream with
    /// [`LseError::NonMonotonicCandlePage`], and one whose bars are spaced closer than `interval`
    /// with [`LseError::UnexpectedCandleResolution`]. Both are checked before *any* bar of that
    /// page is yielded — unlike the range violation above, nothing from such a page is emitted.
    ///
    /// # Request count
    /// Because the row cap is applied silently, a short page cannot be distinguished from the end
    /// of the data. Pagination therefore continues until a page comes back **empty** or the cursor
    /// passes the requested range, which costs at most one extra request per fetch. Treating a
    /// short page as terminal would be faster and would silently truncate the result if the
    /// provider ever lowered its cap below the rows requested per page — the figure the provider
    /// itself reports as [`max_rows_per_request`](super::quota::QuotaStatus::max_rows_per_request),
    /// measured at 5,000 and overridable with
    /// [`with_page_limit`](LseVaultClient::with_page_limit).
    #[must_use = "fetch_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_candles<'a>(
        &'a self,
        symbol: &'a str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<Candle, LseError>> + 'a {
        try_stream! {
            // An inverted range is a caller error, not an empty result: the vault would answer 200
            // with a confusing selection rather than complaining.
            if start > end {
                Err(LseError::InvalidInput {
                    message: format!("start ({start}) must not be after end ({end})"),
                })?;
            }

            // Rejected up front so the caller gets a typed answer instead of a relayed 400.
            let timeframe = candle_interval_str(interval)
                .ok_or(LseError::UnsupportedInterval { interval })?;

            let step = interval.to_step();

            // Map the close-time contract onto the vault's open-time filter. `None` (underflow near
            // `DateTime::MIN_UTC`) is not an error: the boundary bar would have an unrepresentable
            // open and so cannot exist, making the un-widened bound already correct.
            let request_start = open_time_from_close(start, step).unwrap_or(start);
            let request_end_open = open_time_from_close(end, step).unwrap_or(end);

            // The vault's `end` is EXCLUSIVE on open time, so extend past the final bar's open to
            // readmit it. Saturating near `DateTime::MAX_UTC` is safe -- the close-time trim below
            // is exact regardless, so an un-extended bound only risks omitting a bar that cannot
            // exist at that boundary.
            let range_end = request_end_open
                .checked_add_signed(TimeDelta::seconds(CURSOR_STEP_SECS))
                .unwrap_or(request_end_open);

            let mut cursor = request_start;
            let mut page = 0usize;
            // Carried ACROSS pages, not reset per page: a vault that served each page ascending but
            // the pages themselves out of order, or that repeated a page's worth of bars after the
            // cursor advanced, would satisfy a per-page check and still corrupt the series.
            let mut previous_open: Option<DateTime<Utc>> = None;

            loop {
                // No sleep here: pacing is applied inside `get_json`, against a gate shared by
                // every clone of this client. Spacing pages from inside this loop would only ever
                // pace *this* fetch, so N concurrent fetches would issue N requests per interval —
                // see `LseVaultClient::with_pace`.
                let query = [
                    ("symbol", symbol.to_owned()),
                    // ⚠️ `timeframe`, NOT `resolution`. An unknown parameter is ignored silently
                    // and the vault defaults to 1-minute bars, returning a byte-identical shape.
                    ("timeframe", timeframe.to_owned()),
                    ("start", cursor.format(CURSOR_FORMAT).to_string()),
                    ("end", range_end.format(CURSOR_FORMAT).to_string()),
                    ("limit", self.page_limit().to_string()),
                ];

                let rows: Vec<LseCandleRow> = self.get_json("candles", &query).await?;
                page += 1;
                debug!(symbol, %cursor, rows = rows.len(), page, "vault candle page received");

                // The only reliable end-of-data signal; see the request-count note above.
                if rows.is_empty() {
                    break;
                }

                let mut max_open: Option<DateTime<Utc>> = None;
                let mut reached_end = false;

                // Two STRUCTURAL properties of the page, checked in full before a single bar of it
                // is yielded. Both are properties of the response as a whole rather than of any one
                // row, and a page violating either is not partially usable: the first invalidates
                // the cursor arithmetic, the second means every `close_time` this module computes
                // for the page is wrong. That is the difference from the range trim below, which is
                // a per-row property of a well-formed page and is therefore applied per row.
                //
                // The cost is one extra pass over a `Vec` already in memory. The instants this pass
                // parses are kept and reused by the conversion loop below rather than reparsed:
                // `open_time` is a format-string parse, not a comparison, so reparsing them would
                // double the timestamp work on every page of every backfill.
                let mut opens = Vec::with_capacity(rows.len());
                for row in &rows {
                    let open = row.open_time()?;

                    if let Some(previous) = previous_open {
                        // ORDER. Ascending is how the vault answers today, not something it
                        // guarantees, and the cursor advances past the newest open a page carried
                        // -- so a page served newest-first AND truncated at the row cap would jump
                        // the cursor to the end of the range, make the next page empty (the
                        // documented end-of-data signal), and return `Ok` holding the tail of the
                        // series with nothing to say the rest was skipped.
                        if open < previous {
                            Err(LseError::NonMonotonicCandlePage {
                                symbol: symbol.to_owned(),
                                page,
                                previous_open: previous,
                                open,
                            })?;
                        }

                        // RESOLUTION. Consecutive bars of a compliant fixed-interval series are
                        // exactly one interval apart, and a gap (a weekend, a holiday, a quiet
                        // symbol) only ever makes that spacing WIDER -- so spacing narrower than
                        // the interval means the response is finer-grained than the request. This
                        // is the runtime detector for the vault's documented habit of answering
                        // `200` to a misspelled resolution parameter with 1-minute bars in a
                        // byte-identical shape (see the `vault` module docs). Nothing else here can
                        // see that: the bars ascend, and they fall inside the requested range.
                        //
                        // Skipped for a calendar step -- `month` has no single width, and February
                        // would false-positive against a 31-day one.
                        if let IntervalStep::Fixed(width) = step {
                            let actual = open - previous;
                            if actual < width {
                                Err(LseError::UnexpectedCandleResolution {
                                    symbol: symbol.to_owned(),
                                    interval,
                                    previous_open: previous,
                                    open,
                                    actual,
                                })?;
                            }
                        }
                    }
                    previous_open = Some(open);
                    opens.push(open);
                }

                for (row, open) in rows.into_iter().zip(opens) {
                    let close_time = close_time_from_open(open, step)
                        .ok_or(LseError::TimestampOverflow { open, interval })?;

                    // Track every row, including trimmed ones, so the cursor advances past bars
                    // that fell outside the contract rather than re-requesting them forever.
                    max_open = Some(max_open.map_or(open, |seen| seen.max(open)));

                    // A bar past the upper bound ends the fetch — but only after the rest of this
                    // page has been examined, so the error never arrives having thrown away data
                    // the response actually carried. On a page that reaches here that is now belt
                    // and braces: the order check above rejects any page on which an out-of-range
                    // bar could precede an in-range one, since on an ascending page everything
                    // after the first out-of-range bar is also out of range. Kept because it costs
                    // one pass over a page already in memory, `max_open` below scans every row
                    // regardless, and "yield everything valid before failing" is the property worth
                    // holding structurally rather than as a consequence of another check.
                    if close_time > end {
                        reached_end = true;
                        continue;
                    }

                    // Below the lower bound only for the widened first page.
                    if close_time < start {
                        continue;
                    }

                    yield row.into_candle(close_time);
                }

                if reached_end {
                    // Terminal, not trimmed-and-logged. `range_end` is derived as `end - step + 1s`
                    // against an exclusive upper bound, so the newest bar a compliant page can carry
                    // is the one closing exactly on `end`. A later one means the vault answered
                    // outside the range it was asked for -- the same silently-ignored-parameter
                    // failure as the cursor non-advance below, and so the same kind of typed error.
                    // Trimming and continuing would hand back a series that looks complete; a caller
                    // cannot assert on a log line, so a warning here would be observable to an
                    // operator reading stderr and to nobody else. Every in-range bar on this page
                    // was already yielded above, so nothing this response did contain is lost.
                    Err(LseError::UnexpectedCandleRange {
                        symbol: symbol.to_owned(),
                        cursor,
                        page,
                        end,
                    })?;
                }

                let Some(last_open) = max_open else {
                    break;
                };

                let next_cursor = last_open
                    .checked_add_signed(TimeDelta::seconds(CURSOR_STEP_SECS))
                    .ok_or(LseError::CursorOverflow { last_open })?;

                // A page consisting solely of bars at or before the cursor would leave the cursor
                // unmoved and loop forever. That requires the provider to ignore `start`, which is
                // exactly the silent-parameter failure this integration guards against elsewhere,
                // so it is surfaced rather than trusted.
                if next_cursor <= cursor {
                    Err(LseError::Api {
                        status: 200,
                        message: format!(
                            "page {page} did not advance past {cursor}: the range parameters \
                             appear to have been ignored"
                        ),
                    })?;
                }

                cursor = next_cursor;

                // `>=` rather than `>`: the vault's upper bound is exclusive, so a cursor sitting
                // on it selects an empty window. Stopping here saves a guaranteed-empty request
                // whenever the last bar received was the final one in range.
                if cursor >= range_end {
                    break;
                }
            }
        }
    }

    /// Fetch historical candles into a `Vec`.
    ///
    /// Convenience over [`fetch_candles`](Self::fetch_candles) with the same range contract, for
    /// slices small enough to hold in memory. **Buffers the whole range** — prefer the stream for
    /// long backfills at fine resolutions.
    ///
    /// # Errors
    /// Fails on the first error, discarding any candles already received. See
    /// [`fetch_candles`](Self::fetch_candles).
    pub async fn collect_candles(
        &self,
        symbol: &str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>, LseError> {
        let stream = self.fetch_candles(symbol, interval, start, end);
        futures::pin_mut!(stream);

        let mut candles = Vec::new();
        while let Some(candle) = stream.next().await {
            candles.push(candle?);
        }

        Ok(candles)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn row(ts: &str, volume: Option<Decimal>) -> LseCandleRow {
        LseCandleRow {
            ts: ts.to_owned(),
            open: dec!(1),
            high: dec!(2),
            low: dec!(0.5),
            close: dec!(1.5),
            volume,
        }
    }

    #[test]
    fn an_equity_row_deserializes_float_prices_and_an_integer_volume() {
        // The measured equity *shape*: prices are JSON floats, volume a JSON integer. Both must
        // land in `Decimal` without a custom helper.
        //
        // Every number here is invented — deliberately round, or obviously not a price — so no row
        // in this repo can be mistaken for provider data, which we are not licensed to redistribute
        // (<https://londonstrategicedge.com/terms>). Decoding is value-independent, so a synthetic
        // row proves exactly the property a captured one would.
        //
        // `high` carries **15 significant digits** deliberately: that is the part under test, since
        // a price this long must survive the `f64` the JSON number is parsed as. 15 is also the
        // ceiling — the deserializer goes JSON float -> `f64` -> `Decimal`, so a 17-digit literal
        // like `10.123456789012345` lands as `10.123456789012344` and would make this test assert
        // the loss rather than the fidelity.
        let json = r#"{"ts":"2024-01-02 00:00:00.000000","symbol":"TEST","open":10.0,
                       "high":10.1234567890123,"low":10.0,"close":10.0,"volume":1000000}"#;

        let row: LseCandleRow = serde_json::from_str(json).unwrap();

        assert_eq!(row.open, dec!(10.0));
        assert_eq!(row.high, dec!(10.1234567890123));
        assert_eq!(row.volume, Some(dec!(1000000)));
    }

    #[test]
    fn an_fx_row_omitting_volume_deserializes_as_none() {
        // The measured FX shape: no `volume` key at all. This must be `None`, never `Some(0)`.
        // Values are invented, for the reason given above.
        let json = r#"{"ts":"2024-01-02 00:00:00.000000","symbol":"AAA/BBB","open":2.0,
                       "high":2.0,"low":2.0,"close":2.0}"#;

        let row: LseCandleRow = serde_json::from_str(json).unwrap();

        assert_eq!(row.volume, None);
    }

    #[test]
    fn timestamps_parse_as_utc() {
        // The value carries no zone. Treating it as UTC is what makes the pre-market open land at
        // 09:00 in winter and 08:00 in summer, i.e. 04:00 ET across the DST boundary.
        let open = row("2024-01-02 09:09:00.000000", None).open_time().unwrap();

        assert_eq!(
            open,
            "2024-01-02T09:09:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_timestamp_without_a_fractional_part_still_parses() {
        assert!(row("2024-01-02 09:09:00", None).open_time().is_ok());
    }

    #[test]
    fn a_malformed_timestamp_is_a_typed_error() {
        let error = row("not-a-timestamp", None).open_time().unwrap_err();

        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("not-a-timestamp"));
    }

    #[test]
    fn trade_count_is_always_none() {
        // The vault reports no trade count for any dataset, so it must be the explicit unknown.
        let candle = row("2024-01-02 09:09:00.000000", Some(dec!(5))).into_candle(Utc::now());

        assert_eq!(candle.trade_count, None);
    }

    #[test]
    fn a_daily_bars_close_is_the_next_midnight_not_its_own_label() {
        // `ts` is the bar's OPEN. Passing it through as `close_time` would shift every candle in
        // the integration by one bar.
        let open = row("2024-01-02 00:00:00.000000", None).open_time().unwrap();

        let close = close_time_from_open(open, CandleInterval::Day1.to_step()).unwrap();

        assert_eq!(
            close,
            "2024-01-03T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_monthly_bars_close_uses_calendar_arithmetic() {
        // Measured: monthly bars are labelled on the 1st. February's length rules out a fixed step.
        let open = row("2024-02-01 00:00:00.000000", None).open_time().unwrap();

        let close = close_time_from_open(open, CandleInterval::Month1.to_step()).unwrap();

        assert_eq!(
            close,
            "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }
}
