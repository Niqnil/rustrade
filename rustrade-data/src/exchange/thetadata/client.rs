//! The REST client over a running Theta Terminal.

use super::{
    error::ThetaDataError,
    model::{Interval, Row},
    time,
};
use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use async_stream::try_stream;
use chrono::{NaiveDate, NaiveTime, TimeDelta};
use futures::{Stream, StreamExt};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// Where a Theta Terminal on this host serves its v3 REST API by default.
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:25503/v3";

/// The longest span of days one request covers at one-minute intervals and above: ThetaData caps
/// such requests at a month, and 28 days fits inside every month.
pub(crate) const MAX_DAYS_INTRADAY: u32 = 28;

/// The longest span of days one request covers for daily data (end-of-day reports, open interest,
/// interest rates).
pub(crate) const MAX_DAYS_DAILY: u32 = 365;

/// The longest partial line the client buffers. A row is a few hundred bytes; a body that runs
/// past this without a newline is not NDJSON, and is refused rather than buffered without bound.
const MAX_LINE_BYTES: usize = 1 << 20;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(120);

/// The time-of-day window of an intraday history request, in New York time, both ends inclusive.
///
/// Without one, ThetaData serves the regular session, 09:30 to 16:00.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub start: NaiveTime,
    pub end: NaiveTime,
}

/// A client for the REST API of a [Theta Terminal] the caller runs.
///
/// The Terminal is ThetaData's local application: it logs in to the account and serves the API on
/// this host, at [`DEFAULT_BASE_URL`] unless configured otherwise. Starting, updating and
/// supervising it are the caller's.
///
/// # Concurrency
/// ThetaData limits each account to a number of concurrent requests, set by its highest
/// subscription tier (free 1, value 2, standard 4, pro 8) and shared by every client of the
/// account. The Terminal queues requests beyond the limit, up to its queue size, then answers 429
/// ([`ThetaDataError::Throttled`]). This client keeps at most
/// [`with_max_concurrent_requests`](Self::with_max_concurrent_requests) requests in flight
/// (default 1). Clones share the limit.
///
/// A stream holds its request's slot from its first poll until it ends or is dropped, including
/// while the caller is between rows. So at a limit of 1, polling a second stream of the same client
/// before the first has ended waits until the first ends or is dropped, and never returns if the
/// caller awaits the second inside the first's loop (or zips or selects the two). Drain or drop one
/// stream before polling the next, or set the limit to at least the number of streams polled
/// together.
///
/// # Ranges
/// History requests are capped by ThetaData: a month of data at one-minute intervals and above, a
/// single day below that. A method taking a date range splits it into requests within the caps,
/// sends them one after another, and streams the rows back in order.
///
/// # Errors and retries
/// The client does not retry; see [`ThetaDataError::is_transient`]. A request for which ThetaData
/// has no data yields no rows rather than an error.
///
/// [Theta Terminal]: https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html
#[derive(Debug, Clone)]
pub struct ThetaDataClient {
    http: reqwest::Client,
    base_url: String,
    permits: Arc<Semaphore>,
    max_concurrent_requests: NonZeroUsize,
    read_timeout: Duration,
}

impl ThetaDataClient {
    /// A client for a Terminal at [`DEFAULT_BASE_URL`], one request in flight at a time.
    pub fn new() -> Result<Self, ThetaDataError> {
        let limit = NonZeroUsize::MIN;
        Self::build(
            DEFAULT_BASE_URL.to_owned(),
            Arc::new(Semaphore::new(limit.get())),
            limit,
            DEFAULT_READ_TIMEOUT,
        )
    }

    /// A client over `permits`: the setters that rebuild the HTTP client pass the existing
    /// semaphore, so clones keep sharing one limit.
    fn build(
        base_url: String,
        permits: Arc<Semaphore>,
        max_concurrent_requests: NonZeroUsize,
        read_timeout: Duration,
    ) -> Result<Self, ThetaDataError> {
        let http = reqwest::Client::builder()
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .read_timeout(read_timeout)
            // The Terminal serves every response directly; a redirect would mean something else is
            // answering at the base URL.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ThetaDataError::Http {
                message: format!("building the HTTP client: {e}"),
            })?;
        Ok(Self {
            http,
            base_url,
            permits,
            max_concurrent_requests,
            read_timeout,
        })
    }

    /// The same client for a Terminal at `base_url`, e.g. `http://theta-terminal:25503/v3`.
    ///
    /// The URL must be `http` or `https`; a trailing `/` is ignored. ThetaData treats requests from
    /// different addresses as different clients (HTTP 476), so `localhost` and `127.0.0.1` are not
    /// interchangeable: use one spelling for the Terminal's whole run. Clones keep sharing the
    /// request limit.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Result<Self, ThetaDataError> {
        let base_url = base_url.into();
        let parsed = url::Url::parse(&base_url)
            .map_err(|e| ThetaDataError::invalid_input(format!("base URL {base_url:?}: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(ThetaDataError::invalid_input(format!(
                "base URL {base_url:?} is not http or https"
            )));
        }
        let base_url = base_url.trim_end_matches('/').to_owned();
        Self::build(
            base_url,
            self.permits,
            self.max_concurrent_requests,
            self.read_timeout,
        )
    }

    /// The same client keeping at most `limit` requests in flight, normally the account's
    /// concurrency limit. A fresh limit: clones made before this call keep sharing the old one.
    pub fn with_max_concurrent_requests(self, limit: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit.get())),
            max_concurrent_requests: limit,
            ..self
        }
    }

    /// The same client waiting at most `timeout` for each read of a response (default 120 s).
    ///
    /// A large history request can take the Terminal several seconds before its first row; the
    /// timeout bounds a stall, not a whole response. Clones keep sharing the request limit.
    pub fn with_read_timeout(self, timeout: Duration) -> Result<Self, ThetaDataError> {
        Self::build(
            self.base_url,
            self.permits,
            self.max_concurrent_requests,
            timeout,
        )
    }

    /// The Terminal's base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The most requests this client keeps in flight.
    pub fn max_concurrent_requests(&self) -> NonZeroUsize {
        self.max_concurrent_requests
    }

    /// The rows of one request, decoded by `decode`.
    pub(crate) fn rows<T, F>(
        &self,
        path: String,
        params: Vec<(&'static str, String)>,
        decode: F,
    ) -> impl Stream<Item = Result<T, ThetaDataError>> + Send + use<T, F>
    where
        T: Send + 'static,
        F: Fn(&Row<'_>) -> Result<T, ThetaDataError> + Send + 'static,
    {
        let client = self.clone();
        try_stream! {
            // Held until the response is read to the end: the Terminal counts the request as
            // in flight until then. The semaphore is never closed, so acquiring cannot fail.
            let _permit = client
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ThetaDataError::Http { message: "request limiter closed".into() })?;

            let url = format!("{}/{}", client.base_url, path);
            let response = client
                .http
                .get(&url)
                .query(&params)
                .query(&[("format", "ndjson")])
                .send()
                .await
                .map_err(|e| client.send_error(e))?;

            let status = response.status().as_u16();
            if status == 472 {
                return;
            }
            if !response.status().is_success() {
                let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES)
                    .await
                    .unwrap_or_default();
                Err(ThetaDataError::from_status(status, &body))?;
                return;
            }

            let mut body = response.bytes_stream();
            let mut buffer: Vec<u8> = Vec::new();
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|e| ThetaDataError::Http {
                    message: format!("reading the response: {e}"),
                })?;
                // The bytes already in the buffer hold no newline: only the new chunk is scanned.
                let scanned = buffer.len();
                buffer.extend_from_slice(&chunk);
                let (lines, consumed) = complete_lines(&buffer, scanned);
                for line in lines {
                    if let Some(row) = decode_line(&buffer[line], &decode)? {
                        yield row;
                    }
                }
                buffer.drain(..consumed);
                if buffer.len() > MAX_LINE_BYTES {
                    let error = ThetaDataError::decode(
                        format!("a line longer than {MAX_LINE_BYTES} bytes"),
                        &String::from_utf8_lossy(&buffer),
                    );
                    Err(error)?;
                }
            }
            if let Some(row) = decode_line(&buffer, &decode)? {
                yield row;
            }
        }
    }

    /// The rows of a request over a date range, split into spans of at most `max_days`, sent one
    /// after another and streamed back in order.
    pub(crate) fn ranged_rows<T, F>(
        &self,
        path: String,
        params: Vec<(&'static str, String)>,
        range: (NaiveDate, NaiveDate),
        max_days: u32,
        decode: F,
    ) -> impl Stream<Item = Result<T, ThetaDataError>> + Send + use<T, F>
    where
        T: Send + 'static,
        F: Fn(&Row<'_>) -> Result<T, ThetaDataError> + Clone + Send + 'static,
    {
        let client = self.clone();
        try_stream! {
            for (start, end) in day_spans(range.0, range.1, max_days)? {
                let mut params = params.clone();
                params.push(("start_date", time::format_date(start)));
                params.push(("end_date", time::format_date(end)));
                let rows = client.rows(path.clone(), params, decode.clone());
                futures::pin_mut!(rows);
                while let Some(row) = rows.next().await {
                    yield row?;
                }
            }
        }
    }

    fn send_error(&self, error: reqwest::Error) -> ThetaDataError {
        if error.is_connect() {
            ThetaDataError::TerminalUnreachable {
                base_url: self.base_url.clone(),
                message: error.to_string(),
            }
        } else {
            ThetaDataError::Http {
                message: error.to_string(),
            }
        }
    }
}

/// The complete lines in `buffer` (without their `\n`), and how many bytes they take: what follows
/// is a partial line, kept until the next chunk completes it. The first `scanned` bytes are known
/// to hold no newline and are not searched again.
fn complete_lines(buffer: &[u8], scanned: usize) -> (Vec<std::ops::Range<usize>>, usize) {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, _) in buffer
        .iter()
        .enumerate()
        .skip(scanned)
        .filter(|(_, byte)| **byte == b'\n')
    {
        lines.push(start..index);
        start = index + 1;
    }
    (lines, start)
}

/// One NDJSON line decoded, or `None` for a blank line.
fn decode_line<T>(
    line: &[u8],
    decode: &impl Fn(&Row<'_>) -> Result<T, ThetaDataError>,
) -> Result<Option<T>, ThetaDataError> {
    let line = std::str::from_utf8(line)
        .map_err(|e| ThetaDataError::decode(e, &String::from_utf8_lossy(line)))?
        .trim();
    if line.is_empty() {
        return Ok(None);
    }
    decode(&Row::parse(line)?).map(Some)
}

/// The most days one request may cover for `interval`.
pub(crate) fn max_days(interval: Interval) -> u32 {
    if interval.is_single_day() {
        1
    } else {
        MAX_DAYS_INTRADAY
    }
}

/// `start..=end` cut into consecutive spans of at most `max_days` days each.
pub(crate) fn day_spans(
    start: NaiveDate,
    end: NaiveDate,
    max_days: u32,
) -> Result<Vec<(NaiveDate, NaiveDate)>, ThetaDataError> {
    if start > end {
        return Err(ThetaDataError::invalid_input(format!(
            "start date {start} is after end date {end}"
        )));
    }
    let step = TimeDelta::days(i64::from(max_days.max(1)) - 1);
    let mut spans = Vec::new();
    let mut span_start = start;
    loop {
        let span_end = span_start
            .checked_add_signed(step)
            .map_or(end, |span_end| span_end.min(end));
        spans.push((span_start, span_end));
        match span_end.succ_opt() {
            Some(next) if span_end < end => span_start = next,
            _ => return Ok(spans),
        }
    }
}

/// The parameters of an intraday request's interval and time window.
pub(crate) fn interval_params(
    interval: Interval,
    window: Option<TimeWindow>,
) -> Vec<(&'static str, String)> {
    let mut params = vec![("interval", interval.as_str().to_owned())];
    if let Some(window) = window {
        params.push(("start_time", time::format_time(window.start)));
        params.push(("end_time", time::format_time(window.end)));
    }
    params
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    /// Feed `body` in chunks of `size` bytes and collect the lines, as the response loop does.
    fn lines_in_chunks(body: &[u8], size: usize) -> Vec<String> {
        let mut buffer = Vec::new();
        let mut lines = Vec::new();
        for chunk in body.chunks(size) {
            let scanned = buffer.len();
            buffer.extend_from_slice(chunk);
            let (complete, consumed) = complete_lines(&buffer, scanned);
            lines.extend(
                complete
                    .into_iter()
                    .map(|line| String::from_utf8(buffer[line].to_vec()).unwrap()),
            );
            buffer.drain(..consumed);
        }
        lines.push(String::from_utf8(buffer).unwrap());
        lines
    }

    #[test]
    fn lines_split_across_chunks_are_reassembled() {
        let body = "{\"a\":1}\n{\"b\":22}\n\n{\"c\":333}".as_bytes();
        let expected = vec!["{\"a\":1}", "{\"b\":22}", "", "{\"c\":333}"];
        for size in 1..=body.len() {
            assert_eq!(lines_in_chunks(body, size), expected, "chunk size {size}");
        }
        assert_eq!(complete_lines(b"no newline", 0), (vec![], 0));
        let (lines, consumed) = complete_lines(b"x\n", 0);
        assert_eq!(
            (lines.len(), lines.first().cloned(), consumed),
            (1, Some(0..1), 2)
        );
        // Bytes before `scanned` are not searched again, but a line still starts at 0.
        let (lines, consumed) = complete_lines(b"ab\ncd\n", 3);
        assert_eq!(
            (lines.len(), lines.first().cloned(), consumed),
            (1, Some(0..5), 6)
        );
    }

    #[test]
    fn a_character_split_across_chunks_is_reassembled() {
        // "é" and "€" are two and three bytes: every chunk size splits one of them somewhere.
        let body = "{\"s\":\"é\"}\n{\"s\":\"€\"}".as_bytes();
        for size in 1..=body.len() {
            assert_eq!(
                lines_in_chunks(body, size),
                vec!["{\"s\":\"é\"}", "{\"s\":\"€\"}"],
                "chunk size {size}"
            );
        }
    }

    #[test]
    fn a_line_decodes_or_is_skipped_when_blank() {
        let decode = |row: &Row<'_>| row.u64("n");
        assert_eq!(decode_line(b"  \r", &decode).unwrap(), None);
        assert_eq!(decode_line(b"{\"n\":5}\r", &decode).unwrap(), Some(5));
        assert!(matches!(
            decode_line(b"{\"n\":-5}", &decode),
            Err(ThetaDataError::Decode { .. })
        ));
        assert!(matches!(
            decode_line(&[0xff, 0xfe], &decode),
            Err(ThetaDataError::Decode { .. })
        ));
    }

    #[test]
    fn a_range_is_cut_into_consecutive_spans() {
        assert_eq!(
            day_spans(date(2024, 1, 1), date(2024, 3, 1), 28).unwrap(),
            vec![
                (date(2024, 1, 1), date(2024, 1, 28)),
                (date(2024, 1, 29), date(2024, 2, 25)),
                (date(2024, 2, 26), date(2024, 3, 1)),
            ]
        );
    }

    #[test]
    fn spans_of_one_day_cover_each_day() {
        assert_eq!(
            day_spans(date(2024, 1, 30), date(2024, 2, 1), 1).unwrap(),
            vec![
                (date(2024, 1, 30), date(2024, 1, 30)),
                (date(2024, 1, 31), date(2024, 1, 31)),
                (date(2024, 2, 1), date(2024, 2, 1)),
            ]
        );
    }

    #[test]
    fn a_range_within_one_span_is_one_request() {
        let one_day = date(2024, 5, 3);
        assert_eq!(
            day_spans(one_day, one_day, 28).unwrap(),
            vec![(one_day, one_day)]
        );
        assert_eq!(
            day_spans(date(2024, 1, 1), date(2024, 1, 28), 28).unwrap(),
            vec![(date(2024, 1, 1), date(2024, 1, 28))]
        );
        assert_eq!(
            day_spans(date(2024, 1, 1), date(2024, 1, 29), 28).unwrap(),
            vec![
                (date(2024, 1, 1), date(2024, 1, 28)),
                (date(2024, 1, 29), date(2024, 1, 29)),
            ]
        );
    }

    #[test]
    fn a_reversed_range_is_refused() {
        assert!(matches!(
            day_spans(date(2024, 1, 2), date(2024, 1, 1), 28),
            Err(ThetaDataError::InvalidInput { .. })
        ));
    }

    #[test]
    fn a_range_ending_on_the_last_date_terminates() {
        let spans = day_spans(NaiveDate::MAX.pred_opt().unwrap(), NaiveDate::MAX, 28).unwrap();
        assert_eq!(
            spans,
            vec![(NaiveDate::MAX.pred_opt().unwrap(), NaiveDate::MAX)]
        );
    }

    #[test]
    fn sub_minute_intervals_take_one_day_per_request() {
        assert_eq!(max_days(Interval::Tick), 1);
        assert_eq!(max_days(Interval::S1), 1);
        assert_eq!(max_days(Interval::M1), MAX_DAYS_INTRADAY);
        assert_eq!(max_days(Interval::H1), MAX_DAYS_INTRADAY);
    }

    #[test]
    fn interval_params_spell_the_window() {
        assert_eq!(
            interval_params(Interval::M5, None),
            vec![("interval", "5m".to_owned())]
        );
        let window = TimeWindow {
            start: NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
            end: NaiveTime::from_hms_milli_opt(20, 0, 0, 0).unwrap(),
        };
        assert_eq!(
            interval_params(Interval::M1, Some(window)),
            vec![
                ("interval", "1m".to_owned()),
                ("start_time", "04:00:00.000".to_owned()),
                ("end_time", "20:00:00.000".to_owned()),
            ]
        );
    }

    #[test]
    fn the_base_url_must_be_http() {
        let client = ThetaDataClient::new().unwrap();
        assert_eq!(client.base_url(), DEFAULT_BASE_URL);
        assert_eq!(client.max_concurrent_requests(), NonZeroUsize::MIN);
        let client = client
            .with_base_url("http://theta-terminal:25503/v3/")
            .unwrap();
        assert_eq!(client.base_url(), "http://theta-terminal:25503/v3");
        assert!(client.clone().with_base_url("ftp://x/v3").is_err());
        assert!(client.with_base_url("not a url").is_err());
    }

    #[test]
    fn settings_survive_each_other() {
        let limit = NonZeroUsize::new(4).unwrap();
        let client = ThetaDataClient::new()
            .unwrap()
            .with_max_concurrent_requests(limit)
            .with_base_url("http://127.0.0.1:1/v3")
            .unwrap()
            .with_read_timeout(Duration::from_secs(9))
            .unwrap();
        assert_eq!(client.max_concurrent_requests(), limit);
        assert_eq!(client.permits.available_permits(), 4);
        assert_eq!(client.read_timeout, Duration::from_secs(9));
        assert_eq!(client.base_url(), "http://127.0.0.1:1/v3");
    }
}
