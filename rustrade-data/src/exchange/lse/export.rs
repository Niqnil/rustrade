//! Exports from LSE Data 3.0: one selection as one verified file, held in memory or placed on disk.
//!
//! An export is one `GET` of `/v3/export`. The provider builds the whole file before it sends the
//! first byte, then sends its exact length and its SHA-256 with it. There is no job to wait for and
//! nothing to resume: the provider keeps nothing once a file is sent and ignores range requests, so
//! a transfer that is cut short is simply requested again.
//!
//! - [`LseClient::fetch_export`] returns the verified bytes.
//! - [`LseClient::download_export`] streams them to a file, verifying as they arrive, and places
//!   the file only once it is whole and verified.
//!
//! Both take an [`LseExportRequest`]. A Parquet file decodes into market events with
//! `parquet::read_export` behind the `lse-parquet` feature (not linked: the link would be dead in a
//! build without it); a CSV file is plain text.
//!
//! # Limits the provider enforces
//! - **One download at a time per key**, on every plan. A second is refused
//!   `429 connections-exhausted`, which is why [`LseClient`] admits one request at a time by
//!   default.
//! - **At most 2,500,000 rows per export**, refused `413 export-too-wide` before anything is built
//!   or charged. Split the window.
//! - **Rows per day** (12,500,000 on the free plan, then `429 rows-exhausted` until 00:00 UTC) and
//!   **bytes per week**, both reported by [`usage`](LseClient::usage). A file kept on disk can be
//!   decoded any number of times without spending them again.
//! - **A selection with nothing stored** is answered `404 data-unavailable`, which both calls
//!   return as `Ok(None)`: a weekend, a holiday, a quiet contract, or dates the provider has not
//!   loaded.
//!
//! # ⚠️ Licensing
//! Exported data is **not redistributable**, and use by or for an organisation needs the provider's
//! licence. A download produces a file on disk that is easy to commit or share by accident; it must
//! not be. See the [module documentation](super#licensing) and
//! <https://londonstrategicedge.com/licence>.

use crate::exchange::lse::client::LseClient;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::historical::LseWindow;
use crate::exchange::lse::market::{LseMarketCode, candle_interval_str};
use crate::exchange::lse::transport::FileHead;
use crate::subscription::candle::CandleInterval;
use smol_str::SmolStr;
use std::path::{Path, PathBuf};
use tokio::io::BufWriter;
use tracing::{debug, warn};

/// Size of the buffer coalescing download chunks before they reach the `.part` file.
const DOWNLOAD_BUFFER_BYTES: usize = 256 * 1024;

/// What an export selects from a symbol's stored history.
///
/// `#[non_exhaustive]`: the provider's paid feeds also serve trades, quotes, Level 3 orders and the
/// Level 2 book. Their layouts cannot be measured on a free key, so they join here once they can
/// be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LseExportData {
    /// Candles at the given resolution: `time, open, high, low, close, volume, trades`, plus the
    /// session `date` for `1d` and longer. The provider serves 14 of [`CandleInterval`]'s variants.
    Candles(CandleInterval),
    /// The trade slice: every stored trade, as `time, price, size, kind` (`time` to the
    /// nanosecond). Not served for forex and commodities, which the provider prices from quotes.
    TradeSlice,
    /// One row of Greeks per traded minute of a `us_options` contract.
    Greeks,
}

impl LseExportData {
    /// The `data` and `timeframe` query values.
    ///
    /// # Errors
    /// [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    fn query_values(&self) -> Result<(&'static str, &'static str), LseError> {
        match self {
            Self::Candles(interval) => candle_interval_str(*interval)
                .map(|timeframe| ("candles", timeframe))
                .ok_or(LseError::UnsupportedInterval {
                    interval: *interval,
                }),
            Self::TradeSlice => Ok(("candles", "tick")),
            Self::Greeks => Ok(("greeks", "1m")),
        }
    }

    /// A short name for file names and messages: `1h`, `tick`, `greeks`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Candles(interval) => candle_interval_str(*interval).unwrap_or("unserved"),
            Self::TradeSlice => "tick",
            Self::Greeks => "greeks",
        }
    }
}

/// The file format of an export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum LseExportFormat {
    /// Apache Parquet: typed, compressed, decodable with `parquet::read_export`. The default.
    #[default]
    Parquet,
    /// CSV with a header row, UTC timestamps as text and `\N` for a null.
    Csv,
}

impl LseExportFormat {
    /// The provider's spelling, also the file extension.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
        }
    }
}

/// One export selection.
///
/// Built with [`new`](Self::new); `#[non_exhaustive]` so a selection the provider adds can join it
/// without breaking callers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct LseExportRequest {
    /// The market the symbol is listed in.
    pub market: LseMarketCode,
    /// The symbol, spelt as the market's catalogue spells it on REST: `AAPL`, `EUR_USD`,
    /// `BTCUSDT`, `SP500`, `VOD` (no `.L`), or an option contract with its root padded to six
    /// characters (`SPY   261127C00719000`). [`rest_symbol_for`](super::market::rest_symbol_for)
    /// translates a registered (websocket) spelling.
    pub symbol: SmolStr,
    /// What to select.
    pub data: LseExportData,
    /// The span, as timestamps or as trading days.
    pub window: LseWindow,
    /// The file format; Parquet unless changed with [`with_format`](Self::with_format).
    pub format: LseExportFormat,
    /// Whether prices and volumes are adjusted for splits (`true`, the provider's default) or
    /// served as published.
    pub adjusted: bool,
}

impl LseExportRequest {
    /// Select `data` of `symbol` in `market` over `window`, as Parquet, adjusted for splits.
    pub fn new(
        market: LseMarketCode,
        symbol: impl Into<SmolStr>,
        data: LseExportData,
        window: LseWindow,
    ) -> Self {
        Self {
            market,
            symbol: symbol.into(),
            data,
            window,
            format: LseExportFormat::default(),
            adjusted: true,
        }
    }

    /// Ask for another file format.
    #[must_use]
    pub fn with_format(mut self, format: LseExportFormat) -> Self {
        self.format = format;
        self
    }

    /// Ask for prices and volumes as published, not adjusted for splits.
    #[must_use]
    pub fn as_published(mut self) -> Self {
        self.adjusted = false;
        self
    }

    /// A file name describing this selection, safe on every common filesystem:
    /// `<symbol>_<market>_<data>_<from>_<to>[_published].<format>`.
    ///
    /// Characters other than ASCII letters, digits, `-` and `.` become `_`, so a websocket-spelt
    /// `EUR/USD` and a padded option symbol both yield a single path component. A file carries no
    /// record of what it holds, Parquet included, so a name like this is the only one; keep it.
    pub fn file_name(&self) -> String {
        let (from, to) = self.window.bounds();
        let published = if self.adjusted { "" } else { "_published" };
        let stem = format!(
            "{}_{}_{}_{from}_{to}{published}",
            self.symbol,
            self.market.as_str(),
            self.data.label()
        );
        let stem: String = stem
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        format!("{stem}.{}", self.format.as_str())
    }

    /// The `/v3/export` query, after the checks that need no request.
    ///
    /// # Errors
    /// [`LseError::InvalidInput`] for a window whose end comes before its start, and
    /// [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    fn query(&self) -> Result<Vec<(&'static str, String)>, LseError> {
        self.window.check_order()?;
        let (data, timeframe) = self.data.query_values()?;
        let (from, to) = self.window.bounds();

        Ok(vec![
            ("market", self.market.as_str().to_owned()),
            ("symbol", self.symbol.to_string()),
            ("data", data.to_owned()),
            ("timeframe", timeframe.to_owned()),
            ("from", from),
            ("to", to),
            ("format", self.format.as_str().to_owned()),
            ("adjusted", if self.adjusted { "1" } else { "0" }.to_owned()),
        ])
    }

    /// Refuse a file whose headers say it holds something other than this selection.
    fn check_answer(&self, head: &FileHead) -> Result<(), LseError> {
        match head.adjusted {
            Some(adjusted) if adjusted != self.adjusted => Err(LseError::ExportMismatch {
                field: "adjusted",
                requested: self.adjusted.to_string(),
                reported: adjusted.to_string(),
            }),
            _ => Ok(()),
        }
    }
}

/// What the provider reported about a verified export.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseExportInfo {
    /// The file's length in bytes as received. The HTTP stack refuses a body shorter than its
    /// `Content-Length`, and the SHA-256 covers every byte.
    pub bytes: u64,
    /// The file's SHA-256 as lowercase hex, verified against the provider's checksum header.
    pub sha256: String,
    /// The rows the provider charged against the plan (`x-export-rows`).
    ///
    /// **Not the file's row count** for aggregated candles: a three-day hourly read was charged
    /// 4,178 rows and carried 73, so it counts the stored rows the bars were built from.
    pub rows_charged: Option<u64>,
    /// The provider's identifier for this export (`x-export-id`), worth quoting in a support
    /// request.
    pub export_id: Option<String>,
    /// The file name the provider suggested (`Content-Disposition`).
    pub file_name: Option<String>,
    /// Whether the provider reports the prices as split-adjusted (`x-lse3-adjusted`). Already
    /// checked against the request.
    pub adjusted: Option<bool>,
}

impl From<FileHead> for LseExportInfo {
    fn from(head: FileHead) -> Self {
        Self {
            bytes: head.bytes,
            sha256: head.sha256,
            rows_charged: head.rows_charged,
            export_id: head.export_id,
            file_name: head.file_name,
            adjusted: head.adjusted,
        }
    }
}

/// A verified export held in memory, from [`LseClient::fetch_export`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseExportBytes {
    /// The selection this answers.
    pub request: LseExportRequest,
    /// The file's bytes, every one verified.
    pub body: Vec<u8>,
    /// What the provider reported about it.
    pub info: LseExportInfo,
}

/// A verified export placed on disk, from [`LseClient::download_export`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LseExportFile {
    /// The selection this answers.
    pub request: LseExportRequest,
    /// Where the file was placed: the destination the download was given.
    pub path: PathBuf,
    /// What the provider reported about it.
    pub info: LseExportInfo,
}

/// What a download does when a file is already at its destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum LseExistingFile {
    /// Refuse with [`LseError::DestinationExists`] before sending the request, so nothing is
    /// spent. The default.
    ///
    /// The check runs before the download and the file is placed after it, so a file another
    /// process creates in between is replaced: this guards against a mistake, not a race.
    #[default]
    Refuse,
    /// Replace it, atomically: until the new file is whole and verified the old one stays as it
    /// was, and a failed download leaves it untouched. For refreshing a copy, since the provider
    /// corrects and rebuilds history.
    Replace,
}

/// Removes a `.part` file unless disarmed, so a failed or cancelled download leaves none behind.
///
/// A download is an `async` call a caller may drop at any await point, inside a `select!` or a
/// timeout, and nothing after that point runs. Only a destructor does, which is why the cleanup
/// lives here rather than in the download's error paths.
struct PartFile {
    path: PathBuf,
    armed: bool,
}

impl PartFile {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    /// The `.part` file has been renamed into place; there is nothing left to remove.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PartFile {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Blocking, but one metadata operation on a path the download just wrote; a destructor
        // cannot await.
        match std::fs::remove_file(&self.path) {
            Ok(()) => debug!(path = %self.path.display(), "removed an unfinished download"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                path = %self.path.display(),
                %error,
                "could not remove an unfinished download"
            ),
        }
    }
}

/// `<dest>.part`, beside the destination so the final rename stays on one filesystem.
fn part_path(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

/// The directory holding `destination`: its parent, or `.` for a bare file name.
fn directory_of(destination: &Path) -> &Path {
    match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Wrap a filesystem failure with what was being done and to which path.
fn io_error(action: &str, path: &Path) -> impl FnOnce(std::io::Error) -> LseError {
    let message = format!("{action} {}", path.display());
    move |source| LseError::Io { message, source }
}

/// Make a rename inside `directory` durable.
///
/// A rename is recorded in the directory, so it survives a crash only once the directory itself is
/// synced. Unix only: elsewhere a directory cannot be opened to sync.
#[cfg(unix)]
async fn sync_directory(directory: &Path) -> Result<(), LseError> {
    tokio::fs::File::open(directory)
        .await
        .map_err(io_error("opening directory", directory))?
        .sync_all()
        .await
        .map_err(io_error("syncing directory", directory))
}

#[cfg(not(unix))]
async fn sync_directory(_directory: &Path) -> Result<(), LseError> {
    Ok(())
}

impl LseClient {
    /// Fetch one export into memory, verified, or `None` when the provider stores nothing for it.
    ///
    /// The provider caps an export at 2,500,000 rows, a few tens of megabytes as Parquet and a few
    /// hundred as CSV, and the whole file is held here. To keep a file for decoding more than once
    /// without spending the plan's rows again, use [`download_export`](Self::download_export).
    ///
    /// # Errors
    /// - Before anything is sent: [`LseError::InvalidInput`] for an inverted window,
    ///   [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    /// - A refusal is [`LseError::Api`] with the provider's code (`export-too-wide`,
    ///   `symbol-unknown`, `plan-required`, `history-limit`, ...); a limit reached is
    ///   [`LseError::RateLimited`].
    /// - A file that fails its checks is [`LseError::MissingChecksum`] or
    ///   [`LseError::IntegrityMismatch`]; calling again downloads it again whole. One whose headers
    ///   describe another selection is [`LseError::ExportMismatch`].
    ///
    /// `Ok(None)` is the provider's `404 data-unavailable`, which it sends both for a window with
    /// nothing stored (a weekend, a holiday) and for a kind of data the market does not serve, so
    /// `None` alone does not prove the window empty.
    pub async fn fetch_export(
        &self,
        request: &LseExportRequest,
    ) -> Result<Option<LseExportBytes>, LseError> {
        let query = request.query()?;
        let Some((body, head)) = self.get_export(&query).await? else {
            return Ok(None);
        };
        request.check_answer(&head)?;

        Ok(Some(LseExportBytes {
            request: request.clone(),
            body,
            info: head.into(),
        }))
    }

    /// Download one export to `destination`, verified, or `None` when the provider stores nothing
    /// for it.
    ///
    /// The body streams to `<destination>.part` and is hashed as it arrives, so memory stays flat
    /// whatever the file's size. Only once every byte has matched the declared length and SHA-256
    /// is the file synced, renamed to `destination` and the rename synced: `destination` either
    /// holds a whole, verified file or is as it was before the call.
    ///
    /// # Cancellation
    /// Dropping the returned future (a timeout, a `select!`) removes the `.part` file. One left by
    /// a process that died mid-download is overwritten by the next download to that destination.
    ///
    /// # Arguments
    /// - `destination`: the file to place. Its directory must exist. See
    ///   [`LseExportRequest::file_name`] for a name that records the selection.
    /// - `existing`: what to do if `destination` already exists; see [`LseExistingFile`].
    ///
    /// # Errors
    /// As [`fetch_export`](Self::fetch_export), plus [`LseError::DestinationExists`] (checked
    /// before anything is sent) and [`LseError::Io`] for a filesystem failure. On every error
    /// before the rename the `.part` file is removed and `destination` is untouched.
    ///
    /// The one step after the rename is syncing the directory. If that fails, the `Io` error
    /// names the directory, and `destination` already holds the whole, verified file; only its
    /// placement surviving a crash is in doubt. A future dropped at that step likewise leaves the
    /// file placed.
    pub async fn download_export(
        &self,
        request: &LseExportRequest,
        destination: impl AsRef<Path>,
        existing: LseExistingFile,
    ) -> Result<Option<LseExportFile>, LseError> {
        let destination = destination.as_ref();
        let query = request.query()?;

        if existing == LseExistingFile::Refuse
            && tokio::fs::try_exists(destination)
                .await
                .map_err(io_error("checking", destination))?
        {
            return Err(LseError::DestinationExists {
                path: destination.to_owned(),
            });
        }

        let part = part_path(destination);
        // Armed before the open: a future dropped while the open runs on the blocking pool would
        // otherwise leave the file it creates behind. Removing one never created is a no-op.
        let mut guard = PartFile::new(part.clone());
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&part)
            .await
            .map_err(io_error("creating", &part))?;
        let mut writer = BufWriter::with_capacity(DOWNLOAD_BUFFER_BYTES, file);

        let Some(head) = self.get_export_into(&query, &mut writer).await? else {
            return Ok(None);
        };
        request.check_answer(&head)?;

        writer
            .into_inner()
            .sync_all()
            .await
            .map_err(io_error("syncing", &part))?;
        tokio::fs::rename(&part, destination)
            .await
            .map_err(io_error("placing", destination))?;
        guard.disarm();

        sync_directory(directory_of(destination)).await?;
        debug!(path = %destination.display(), bytes = head.bytes, "lse export placed");

        Ok(Some(LseExportFile {
            request: request.clone(),
            path: destination.to_owned(),
            info: head.into(),
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};

    fn request() -> LseExportRequest {
        LseExportRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            LseExportData::Candles(CandleInterval::Min1),
            LseWindow::Times {
                from: Utc.with_ymd_and_hms(2026, 11, 25, 15, 0, 0).unwrap(),
                to: Utc.with_ymd_and_hms(2026, 11, 25, 16, 0, 0).unwrap(),
            },
        )
    }

    fn query_value(query: &[(&str, String)], name: &str) -> String {
        query
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.clone())
            .unwrap()
    }

    #[test]
    fn a_candle_request_asks_for_candles_at_its_timeframe_as_parquet_adjusted() {
        let query = request().query().unwrap();

        assert_eq!(query_value(&query, "market"), "us_stocks");
        assert_eq!(query_value(&query, "symbol"), "AAPL");
        assert_eq!(query_value(&query, "data"), "candles");
        assert_eq!(query_value(&query, "timeframe"), "1m");
        assert_eq!(query_value(&query, "from"), "2026-11-25 15:00:00");
        assert_eq!(query_value(&query, "to"), "2026-11-25 16:00:00");
        assert_eq!(query_value(&query, "format"), "parquet");
        assert_eq!(query_value(&query, "adjusted"), "1");
    }

    #[test]
    fn a_trade_slice_is_candles_at_tick_and_greeks_are_their_own_data() {
        let mut slice = request();
        slice.data = LseExportData::TradeSlice;
        let query = slice.query().unwrap();
        assert_eq!(
            (
                query_value(&query, "data"),
                query_value(&query, "timeframe")
            ),
            ("candles".to_owned(), "tick".to_owned())
        );

        let mut greeks = request();
        greeks.data = LseExportData::Greeks;
        let query = greeks.query().unwrap();
        assert_eq!(
            (
                query_value(&query, "data"),
                query_value(&query, "timeframe")
            ),
            ("greeks".to_owned(), "1m".to_owned())
        );
    }

    #[test]
    fn a_csv_unadjusted_request_says_so() {
        let query = request()
            .with_format(LseExportFormat::Csv)
            .as_published()
            .query()
            .unwrap();

        assert_eq!(query_value(&query, "format"), "csv");
        assert_eq!(query_value(&query, "adjusted"), "0");
    }

    #[test]
    fn a_timestamp_bound_keeps_its_nanoseconds_and_a_date_window_sends_dates() {
        let mut slice = request();
        slice.window = LseWindow::Times {
            from: Utc
                .with_ymd_and_hms(2026, 11, 25, 15, 0, 0)
                .unwrap()
                .checked_add_signed(chrono::TimeDelta::nanoseconds(123_456_789))
                .unwrap(),
            to: Utc.with_ymd_and_hms(2026, 11, 25, 15, 1, 0).unwrap(),
        };
        let query = slice.query().unwrap();
        assert_eq!(query_value(&query, "from"), "2026-11-25 15:00:00.123456789");
        assert_eq!(query_value(&query, "to"), "2026-11-25 15:01:00");

        let mut daily = request();
        daily.window = LseWindow::Dates {
            from: NaiveDate::from_ymd_opt(2026, 11, 17).unwrap(),
            to: NaiveDate::from_ymd_opt(2026, 11, 25).unwrap(),
        };
        let query = daily.query().unwrap();
        assert_eq!(query_value(&query, "from"), "2026-11-17");
        assert_eq!(query_value(&query, "to"), "2026-11-25");
    }

    #[test]
    fn an_inverted_window_is_refused_before_anything_is_sent() {
        let mut inverted = request();
        inverted.window = LseWindow::Dates {
            from: NaiveDate::from_ymd_opt(2026, 11, 25).unwrap(),
            to: NaiveDate::from_ymd_opt(2026, 11, 24).unwrap(),
        };

        assert!(matches!(
            inverted.query(),
            Err(LseError::InvalidInput { .. })
        ));
    }

    #[test]
    fn a_one_day_window_is_not_inverted() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 25).unwrap();
        let mut single = request();
        single.window = LseWindow::Dates { from: day, to: day };

        assert!(single.query().is_ok());
    }

    #[test]
    fn an_unserved_interval_is_refused_before_anything_is_sent() {
        let mut unserved = request();
        unserved.data = LseExportData::Candles(CandleInterval::Hour2);

        assert!(matches!(
            unserved.query(),
            Err(LseError::UnsupportedInterval { .. })
        ));
    }

    #[test]
    fn the_file_name_records_the_selection_in_one_path_component() {
        let mut option = request();
        option.market = LseMarketCode::UsOptions;
        option.symbol = SmolStr::new("SPY   261127C00719000");
        option.data = LseExportData::TradeSlice;

        assert_eq!(
            option.file_name(),
            "SPY___261127C00719000_us_options_tick_2026-11-25_15_00_00_2026-11-25_16_00_00.parquet"
        );

        let mut forex = request().with_format(LseExportFormat::Csv).as_published();
        forex.symbol = SmolStr::new("EUR/USD");
        assert_eq!(
            forex.file_name(),
            "EUR_USD_us_stocks_1m_2026-11-25_15_00_00_2026-11-25_16_00_00_published.csv"
        );
    }

    #[test]
    fn an_answer_reporting_other_adjustment_is_refused() {
        let head = |adjusted| FileHead {
            bytes: 0,
            sha256: String::new(),
            rows_charged: None,
            export_id: None,
            file_name: None,
            adjusted,
        };

        assert!(request().check_answer(&head(Some(true))).is_ok());
        assert!(request().check_answer(&head(None)).is_ok());
        assert!(matches!(
            request().check_answer(&head(Some(false))),
            Err(LseError::ExportMismatch {
                field: "adjusted",
                ..
            })
        ));
    }

    #[test]
    fn a_bare_file_name_is_synced_in_the_working_directory() {
        assert_eq!(directory_of(Path::new("a.parquet")), Path::new("."));
        assert_eq!(directory_of(Path::new("data/a.parquet")), Path::new("data"));
        assert_eq!(directory_of(Path::new("/srv/a.parquet")), Path::new("/srv"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_directory_that_cannot_be_synced_is_an_io_error() {
        let missing = tempfile::tempdir().unwrap().path().join("gone");

        assert!(matches!(
            sync_directory(&missing).await,
            Err(LseError::Io { .. })
        ));
    }

    #[test]
    fn the_part_file_sits_beside_its_destination() {
        assert_eq!(
            part_path(Path::new("/data/AAPL.parquet")),
            PathBuf::from("/data/AAPL.parquet.part")
        );
    }

    #[test]
    fn an_armed_part_file_is_removed_on_drop_and_a_disarmed_one_is_kept() {
        let directory = tempfile::tempdir().unwrap();
        let removed = directory.path().join("removed.part");
        let kept = directory.path().join("kept.part");
        std::fs::write(&removed, b"partial").unwrap();
        std::fs::write(&kept, b"placed").unwrap();

        drop(PartFile::new(removed.clone()));
        let mut disarmed = PartFile::new(kept.clone());
        disarmed.disarm();
        drop(disarmed);

        assert!(!removed.exists());
        assert!(kept.exists());
    }
}
