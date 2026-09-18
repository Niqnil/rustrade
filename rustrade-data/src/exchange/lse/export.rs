//! Bulk export jobs against the vault: submit, poll, download.
//!
//! The vault serves candles over REST ([`historical`](super::historical)), but the **raw tick tape
//! is reachable only here** — there is no streaming or REST path to it. Exports are asynchronous:
//! submit a job, poll it to `ready`, then download the artifact.
//!
//! # ⚠️ This is a different API surface from the candles endpoint
//! Measured differences that a client written against the candles path gets wrong:
//!
//! - Submit answers **`202 Accepted`**, not `200`. A `== 200` check never polls.
//! - The job-id field is **renamed between responses**: the submit body says `job_id`, the status
//!   body says `id`. Hence [`LseExportJob`] and [`LseExportJobStatus`] are separate types.
//! - Allowance exhaustion is a **`429` with no `Retry-After`**, mapped here to
//!   [`LseError::QuotaExceeded`] rather than [`LseError::RateLimited`] — an exhausted export
//!   allowance is a different condition from a per-minute rate limit, and only the former carries a
//!   [`QuotaStatus`](super::quota::QuotaStatus).
//!
//! # ⚠️ A rejected submit still costs you an export
//! The allowance is **5 exports per hour**, and the counter increments on *attempt*: a `400` for a
//! misspelled dataset consumes 20% of the hour's budget just as a successful job does. (Requests
//! rejected at the CDN edge do not count.) This is why [`LseExportRequest::new`] validates
//! everything it can before a request is ever sent — pre-flight validation here is an **economic**
//! requirement, not defensive style.
//!
//! # ⚠️ `symbol: "all"` does not mean "every symbol", and every artifact is single-symbol
//! It is a literal that matches nothing. Measured as a controlled pair on one dataset, timeframe
//! and range: `symbol: "all"` returned `202` → `ready` → a **valid Parquet file with the full
//! schema and zero rows**, while the same request naming a real symbol returned rows. There is no
//! error and no warning, and it costs an export. The tick path behaves identically, and omitting
//! `symbol` is a hard `400` — so **no spelling produces a multi-symbol export**, and an export
//! naming `"all"` is rejected before it is sent; see [`LseExportRequest::new`].
//!
//! The consequence reaches the consumer: combining N instruments means N artifacts merged with
//! [`merge_time_sorted`](crate::streams::merge::merge_time_sorted), as on the candle replay path.
//! A decoded artifact is a synchronous iterator of already-tagged events, so it needs adapting to
//! that merge's stream item first; `read_export` documents the adapter. (Not linked: the decoder is
//! behind its own feature, so the link would be dead in a build without it.)
//!
//! The rejection is **case-sensitive on purpose**: `ALL` is Allstate's real ticker, so refusing it
//! would forbid a correct export with no way around it. "Matches nothing" is a property of the
//! (dataset, symbol) pair, not of the string.
//!
//! # Range semantics
//! `start` is **inclusive** and `end` is **exclusive**, matching the candles endpoint. Measured
//! twice: a tick export over `[2026-07-01, 2026-07-02)` yielded only 2026-07-01, and a daily candle
//! export ending `2026-07-20` produced no bar for the 20th. The range is **date-granular**, which
//! [`LseExportRange`] makes explicit by taking [`NaiveDate`] rather than silently truncating a
//! timestamp.
//!
//! # ⚠️ Licensing
//! Exported data is **not redistributable**. An export produces a file on your disk that is
//! trivially easy to commit or share by accident; it must not be. See the
//! [module documentation](super) and <https://londonstrategicedge.com/terms>.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_detail};
use crate::exchange::lse::market::{LseDataset, candle_interval_str};
use crate::exchange::lse::vault::LseVaultClient;
use crate::subscription::candle::CandleInterval;
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::StreamExt;
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

/// Size of the buffer used to read an existing `.part` file back into the hasher.
const HASH_READ_BUFFER_BYTES: usize = 64 * 1024;

/// Size of the write buffer coalescing download chunks before they reach the `.part` file.
const DOWNLOAD_BUFFER_BYTES: usize = 256 * 1024;

/// Total deadline for one artifact transfer, overriding the client's JSON-shaped one.
///
/// The shared client sets a 30 s *total* deadline, which `reqwest` applies through to the end of the
/// response body — correct for a page of JSON, fatal for a multi-gigabyte artifact, which would abort
/// at 30 s however healthy the connection. Six hours covers a 7 GB artifact on a link as slow as
/// ~3 Mbit/s. It is a backstop, not the failure detector: a stall is caught in seconds by the
/// client's per-read timeout, and an interrupted transfer resumes from the `.part` file.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

/// The literal the provider treats as a symbol rather than as "every symbol".
const ALL_SYMBOLS_LITERAL: &str = "all";

/// The only export format this integration decodes.
const EXPORT_FORMAT: &str = "parquet";

/// What an export job covers, in the provider's own timeframe vocabulary.
///
/// [`Tick`](Self::Tick) is the whole point of the export path: the raw tape is not reachable over
/// REST or WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseExportTimeframe {
    /// The raw tick tape.
    ///
    /// # ⚠️ The tick schema varies by dataset
    /// Measured: `fx` exports `{ts, symbol, bid, ask}` (a quote tape), `stocks` exports
    /// `{ts, symbol, price, volume}` (a trade tape). A decoder must dispatch on the columns
    /// actually present rather than assume one shape.
    Tick,

    /// Aggregated candles at the given resolution.
    Candle(CandleInterval),
}

impl LseExportTimeframe {
    /// Returns the provider's spelling of this timeframe.
    ///
    /// # Errors
    /// Returns [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    pub fn as_lse_str(&self) -> Result<&'static str, LseError> {
        match self {
            Self::Tick => Ok("tick"),
            Self::Candle(interval) => {
                candle_interval_str(*interval).ok_or(LseError::UnsupportedInterval {
                    interval: *interval,
                })
            }
        }
    }
}

/// The date range an export covers.
///
/// `start` is inclusive, `end` is **exclusive**, and both are dates rather than timestamps —
/// the export endpoint is date-granular, and taking [`NaiveDate`] states that in the type instead
/// of silently discarding a caller's time-of-day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LseExportRange {
    start: NaiveDate,
    end: NaiveDate,
}

impl LseExportRange {
    /// Construct a range.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidInput`] unless `start < end`. An empty or inverted range would
    /// produce a valid, empty artifact at the cost of one export.
    pub fn new(start: NaiveDate, end: NaiveDate) -> Result<Self, LseError> {
        if start >= end {
            return Err(LseError::InvalidInput {
                message: format!(
                    "export range start {start} must be before end {end} (end is exclusive)"
                ),
            });
        }

        Ok(Self { start, end })
    }

    /// The inclusive first date.
    pub fn start(&self) -> NaiveDate {
        self.start
    }

    /// The exclusive last date.
    pub fn end(&self) -> NaiveDate {
        self.end
    }

    /// Whether this range starts more than one day after `today`, and so is guaranteed to hold no
    /// data.
    ///
    /// Separate from [`new`](Self::new), and pure, deliberately: "is this range in the future"
    /// depends on when the question is asked, so a constructor answering it would bake one reading
    /// of the clock into a value that outlives it — and would make the type untestable without one.
    /// [`submit_export`](super::vault::LseVaultClient::submit_export) asks it at the point the
    /// export is actually spent, which is the instant the answer matters.
    ///
    /// # Why a whole day of slack
    /// `today` is read in UTC while the caller may be thinking in local time, and the widest real
    /// offset is UTC+14 — so a caller's "today" can legitimately be UTC's tomorrow, and never more.
    /// Comparing against `today + 1` therefore cannot reject a range someone meant, while still
    /// catching the failure this exists for: a fat-fingered year, which misses by hundreds of days.
    #[must_use]
    pub fn is_wholly_after(&self, today: NaiveDate) -> bool {
        self.start
            .checked_sub_days(chrono::Days::new(1))
            .is_some_and(|slack| slack > today)
    }
}

/// A validated export request.
///
/// Construction is the validation step; see [`new`](Self::new). Holding one of these means the
/// request is worth spending an export on, as far as anything checkable client-side goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseExportRequest {
    dataset: LseDataset,
    symbol: String,
    timeframe: LseExportTimeframe,
    range: LseExportRange,
}

impl LseExportRequest {
    /// Validate and build an export request.
    ///
    /// # Why this validates so much
    /// A rejected submit consumes one of five hourly exports. Every check here is a rejection the
    /// provider would otherwise bill for.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if `symbol` is blank. Surrounding whitespace is trimmed before
    ///   any check, so a padded symbol is neither smuggled past the `"all"` rejection below nor
    ///   sent verbatim into a `400` that would cost an export.
    /// - [`LseError::InvalidInput`] if `symbol` is `"all"`, on **either** timeframe — measured on
    ///   both to produce a valid, **empty** artifact with no error (see the
    ///   [module documentation](self)). The exact spelling `"ALL"` is admitted on
    ///   [`Stocks`](LseDataset::Stocks) and [`Etf`](LseDataset::Etf) only, where it is Allstate's
    ///   real ticker; on every other dataset a symbol is a pair or a contract slug, so no casing of
    ///   it names anything.
    /// - [`LseError::InvalidInput`] if a candle resolution is requested for one of the provider's
    ///   synthetic classes. Those serve candles over REST but are **tick-only on the export path**
    ///   — measured: `{dataset: volatility, timeframe: 1d}` → `400 … it is tick-only`.
    /// - [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    pub fn new(
        dataset: LseDataset,
        symbol: impl Into<String>,
        timeframe: LseExportTimeframe,
        range: LseExportRange,
    ) -> Result<Self, LseError> {
        // Trimmed before anything reads it, including the `"all"` comparison below and the request
        // body. Incidental whitespace is a copy-paste artefact, and leaving it in would both slip
        // `" all"` past an exact-match guard and buy a billed 400 for a symbol that is otherwise
        // correct.
        let symbol: String = symbol.into();
        let symbol = symbol.trim().to_owned();

        if symbol.is_empty() {
            return Err(LseError::InvalidInput {
                message: "export symbol must not be blank; the provider rejects a missing symbol \
                          with a 400, which still consumes an export"
                    .to_owned(),
            });
        }

        // Rejecting the resolution before the request is sent, not after being billed for a 400.
        let _ = timeframe.as_lse_str()?;

        // Case-insensitive with one narrow escape hatch: `ALL` is Allstate's real ticker (verified
        // live against the provider's own price endpoint), so rejecting *that* spelling on an
        // equity dataset would forbid a correct export with no way around it. Every other casing --
        // `all`, `All`, `aLL` -- is nobody's real symbol, because the provider publishes symbols
        // uppercase, and title case is exactly what a spreadsheet produces. Each one costs a billed
        // export to discover it matched nothing, so the guard covers them rather than only the
        // lowercase form.
        //
        // The escape hatch is scoped to the datasets where a bare equity ticker is even
        // representable. "Matches nothing" is a property of the (dataset, symbol) PAIR, and
        // `dataset` is right here: on `Fx` a symbol is pair-shaped (`EUR/USD`), on the index,
        // commodity, rates, volatility and futures classes it is a contract slug, and on `Crypto`
        // it is a pair -- so `ALL` is not Allstate on any of them, it is the same zero-row artifact
        // as `all`, and letting it through spends a billed export to learn that. `Stocks` and `Etf`
        // share one venue and one symbology (see `LseDataset::exchange_id`), so the hatch covers
        // both rather than assuming Allstate is the only uppercase-`ALL` listing.
        let equity_symbology = matches!(dataset, LseDataset::Stocks | LseDataset::Etf);
        if !(equity_symbology && symbol == "ALL")
            && symbol.eq_ignore_ascii_case(ALL_SYMBOLS_LITERAL)
        {
            // Only the equity classes get told about Allstate; naming it on `fx` would send the
            // reader after a listing that dataset does not carry.
            let hint = if equity_symbology {
                format!(
                    " (If you meant Allstate, its symbol is exactly {:?}, uppercase.)",
                    ALL_SYMBOLS_LITERAL.to_uppercase()
                )
            } else {
                format!(
                    " No casing of it is a symbol on dataset {:?}, whose symbols are not bare \
                     equity tickers.",
                    dataset.as_catalog_str()
                )
            };

            return Err(LseError::InvalidInput {
                message: format!(
                    "symbol {symbol:?} is a literal the provider matches against the symbol \
                     column, not a request for every symbol: an export naming it returns a valid \
                     but EMPTY artifact, with no error, and still consumes one of five hourly \
                     exports - name a real symbol instead. Measured on both the candle and the \
                     tick path; every artifact this provider will produce is single-symbol.{hint}"
                ),
            });
        }

        if let LseExportTimeframe::Candle(interval) = timeframe
            && !dataset.is_candle_class()
        {
            return Err(LseError::InvalidInput {
                message: format!(
                    "dataset {:?} is tick-only on the export path, so resolution {interval} \
                     cannot be exported for it - it serves candles over REST, but export \
                     timeframes follow the provider's candle-class split; use \
                     LseExportTimeframe::Tick",
                    dataset.as_catalog_str()
                ),
            });
        }

        Ok(Self {
            dataset,
            symbol,
            timeframe,
            range,
        })
    }

    /// The dataset this request targets.
    pub fn dataset(&self) -> LseDataset {
        self.dataset
    }

    /// The display symbol this request targets.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// The requested timeframe.
    pub fn timeframe(&self) -> LseExportTimeframe {
        self.timeframe
    }

    /// The requested date range.
    pub fn range(&self) -> LseExportRange {
        self.range
    }

    /// The JSON body the vault expects.
    ///
    /// # Errors
    /// Returns [`LseError::UnsupportedInterval`] if the timeframe has no provider spelling — a
    /// state [`new`](Self::new) already rejects, re-checked here rather than unwrapped.
    fn to_body(&self) -> Result<ExportRequestBody<'_>, LseError> {
        Ok(ExportRequestBody {
            dataset: self.dataset.as_catalog_str(),
            symbol: &self.symbol,
            timeframe: self.timeframe.as_lse_str()?,
            start: self.range.start().to_string(),
            end: self.range.end().to_string(),
            format: EXPORT_FORMAT,
        })
    }
}

/// The wire shape of an export submission.
#[derive(Debug, Serialize)]
struct ExportRequestBody<'a> {
    dataset: &'a str,
    symbol: &'a str,
    timeframe: &'a str,
    start: String,
    end: String,
    format: &'a str,
}

/// Lifecycle state of an export job.
///
/// `queued` and `ready` are measured; `failed` and `expired` are documented by the provider's own
/// client. [`Other`](Self::Other) preserves anything else rather than collapsing it to a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LseExportStatus {
    /// Accepted, not yet building.
    Queued,
    /// Building.
    Running,
    /// The artifact is available to download.
    Ready,
    /// The job will not produce an artifact.
    Failed,
    /// The artifact existed and has since been reaped (roughly 48 hours after it was built).
    Expired,
    /// A status this integration does not know. Preserved verbatim.
    Other(String),
}

impl LseExportStatus {
    /// Whether polling this job further is pointless.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed | Self::Expired)
    }

    /// The provider's spelling of this status.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Expired => "expired",
            Self::Other(other) => other,
        }
    }
}

impl From<&str> for LseExportStatus {
    fn from(value: &str) -> Self {
        match value {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "ready" => Self::Ready,
            "failed" => Self::Failed,
            "expired" => Self::Expired,
            other => Self::Other(other.to_owned()),
        }
    }
}

impl std::fmt::Display for LseExportStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LseExportStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::from(String::deserialize(deserializer)?.as_str()))
    }
}

/// The response to an export submission.
///
/// Distinct from [`LseExportJobStatus`] because the provider renames the identifier between the
/// two responses: `job_id` here, `id` there.
///
/// `#[non_exhaustive]` because this mirrors a provider response: the provider may add a field, and
/// mirroring it here should stay a non-breaking change.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LseExportJob {
    /// The job identifier, to be passed to [`LseVaultClient::export_status`].
    pub job_id: String,

    /// Status at submission time — measured as `queued`.
    pub status: LseExportStatus,

    /// The provider's size estimate.
    ///
    /// # ⚠️ Do not act on this
    /// It is `null` for candle exports, and for tick exports it appears to ignore the symbol and
    /// date filters entirely: measured at `7,111,419,512` for an artifact that turned out to be
    /// `596,985` bytes (**11,900×**) and `7,677,490,312` for one of `1,618,641` bytes (**4,700×**).
    /// Surfaced for completeness only; never use it for preflight, allocation or budgeting.
    #[serde(default)]
    pub est_bytes: Option<u64>,
}

/// The state of an export job, as reported by the status endpoint.
///
/// `#[non_exhaustive]` for the same reason as [`LseExportJob`] — it mirrors a provider response.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LseExportJobStatus {
    /// The job identifier. Note the provider spells this `job_id` on the submit response.
    pub id: String,

    /// Current lifecycle state.
    pub status: LseExportStatus,

    /// Row count of the artifact, once known.
    ///
    /// # ⚠️ Check this
    /// A zero-row export is still a well-formed Parquet file carrying the complete schema, so
    /// "status is ready and the file parses" does **not** imply the request matched anything.
    #[serde(default)]
    pub rows: Option<u64>,

    /// Artifact size in bytes, once known. Unlike [`LseExportJob::est_bytes`], this is exact.
    #[serde(default)]
    pub bytes: Option<u64>,

    /// Lowercase hex SHA-256 of the artifact, once known.
    #[serde(default)]
    pub sha256: Option<String>,

    /// The provider's diagnostic when [`status`](Self::status) is `failed`.
    #[serde(default)]
    pub error: Option<String>,

    /// When the artifact is reaped — roughly 48 hours after it is built.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,

    /// The provider's internal source table, e.g. `candles_etf_1d` or `ticks_fx`.
    #[serde(default)]
    pub table_name: Option<String>,

    /// What the provider says this job covers, echoed back in its own spelling.
    ///
    /// Retained rather than discarded because this provider **silently substitutes defaults for
    /// parameters it does not recognise**: a misspelled field name earns a `200` and an artifact
    /// covering something other than what was asked for. These echoes are the only client-side
    /// evidence of what a job actually covers, so
    /// [`download_export`](LseVaultClient::download_export) checks each present one against the
    /// request it was handed. Kept as strings — the provider's own vocabulary — rather than parsed,
    /// so an unexpected spelling is reported as a mismatch instead of failing to decode a job that
    /// is otherwise fine.
    #[serde(default)]
    pub dataset: Option<String>,

    /// See [`dataset`](Self::dataset).
    #[serde(default)]
    pub symbol: Option<String>,

    /// See [`dataset`](Self::dataset).
    #[serde(default)]
    pub timeframe: Option<String>,

    /// See [`dataset`](Self::dataset).
    #[serde(default)]
    pub start: Option<String>,

    /// See [`dataset`](Self::dataset).
    #[serde(default)]
    pub end: Option<String>,
}

/// A downloaded export artifact, with the provenance needed to decode it safely.
///
/// # Why this is not a bare `PathBuf`
/// An export job targets exactly one dataset, and [`LseDataset::exchange_id`] is total, so **one
/// file has exactly one [`ExchangeId`]**. Carrying the dataset makes that derivable rather than
/// re-accepted as an unchecked argument at decode time — which is what turns "rows from a futures
/// export tagged onto an equities-registered instrument" from a silent misattribution into a
/// construction-time error.
///
/// A caller holding a file obtained out of band constructs one explicitly with
/// [`new`](Self::new), which puts the obligation in the type rather than in prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseExport {
    path: PathBuf,
    dataset: LseDataset,
    symbol: String,
    timeframe: LseExportTimeframe,
    range: LseExportRange,
}

impl LseExport {
    /// Describe an export artifact already on disk.
    ///
    /// Prefer [`LseVaultClient::download_export`], which fills this in from the job it downloaded.
    /// Use this for a file obtained out of band — and note that the dataset you name here is what
    /// every decoded event's [`ExchangeId`] will be derived from, while `symbol` is what the
    /// decoder checks every row against.
    ///
    /// `timeframe` is taken on the caller's word, because nothing in a Parquet artifact records the
    /// resolution it was written at. The decoder measures bar spacing against it and rejects a file
    /// **finer** than declared — 1-minute bars declared `Day1`, where every derived `close_time`
    /// would stretch over the next 1,439 bars. The opposite mistake is undetectable and the worse
    /// of the two: daily bars declared `Min1` are spaced far wider than 60 seconds, so they pass,
    /// and each bar then enters the timeline a minute after its day opened rather than at the end
    /// of it — a full day of lookahead. See `parquet::read_export`, whose rustdoc states the reach
    /// of the check in full. (Not linked: that module is behind its own feature, so the link would
    /// be dead in a build without it.)
    pub fn new(
        path: impl Into<PathBuf>,
        dataset: LseDataset,
        symbol: impl Into<String>,
        timeframe: LseExportTimeframe,
        range: LseExportRange,
    ) -> Self {
        Self {
            path: path.into(),
            dataset,
            symbol: symbol.into(),
            timeframe,
            range,
        }
    }

    /// The artifact's location on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The dataset the artifact was exported from.
    pub fn dataset(&self) -> LseDataset {
        self.dataset
    }

    /// The vault display symbol the artifact was exported for.
    ///
    /// Every export is single-symbol — the provider offers no multi-symbol spelling — so this is
    /// the value every row's `symbol` column must carry.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// The timeframe the artifact contains.
    pub fn timeframe(&self) -> LseExportTimeframe {
        self.timeframe
    }

    /// The date range the artifact covers.
    pub fn range(&self) -> LseExportRange {
        self.range
    }

    /// The venue every event decoded from this artifact is stamped with.
    pub fn exchange_id(&self) -> ExchangeId {
        self.dataset.exchange_id()
    }
}

impl LseVaultClient {
    /// Submit an export job.
    ///
    /// Returns as soon as the provider accepts the job (`202`); the artifact is built
    /// asynchronously. Poll with [`export_status`](Self::export_status), or use
    /// [`await_export`](Self::await_export).
    ///
    /// # ⚠️ This spends allowance whether or not it succeeds
    /// The allowance is five exports per hour and a rejection still consumes one, which is why
    /// [`LseExportRequest`] validates at construction. Poll
    /// [`usage`](LseVaultClient::usage) to see where you are — the position is deliberately not
    /// fetched here, since that would add a network round trip to every submission.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if the range lies wholly in the future. The provider answers
    ///   such a request with a valid, **empty** artifact and no error, so it spends an export to
    ///   learn nothing — the same economics as the empty range [`LseExportRange::new`] rejects,
    ///   which is why it is rejected too. Checked here rather than at construction because the
    ///   answer depends on when the question is asked; see
    ///   [`LseExportRange::is_wholly_after`] for the day of slack that keeps a caller's local
    ///   "today" from tripping it.
    /// - [`LseError::QuotaExceeded`] when the export allowance is exhausted, carrying the
    ///   allowance position at the point of rejection.
    /// - [`LseError::Api`] for any other non-success status, with the provider's diagnostic
    ///   unwrapped from its envelope.
    /// - [`LseError::Deserialize`] if the response will not decode.
    pub async fn submit_export(
        &self,
        request: &LseExportRequest,
    ) -> Result<LseExportJob, LseError> {
        let today = chrono::Utc::now().date_naive();
        if request.range().is_wholly_after(today) {
            return Err(LseError::InvalidInput {
                message: format!(
                    "export range {} to {} (end exclusive) lies wholly in the future, and today is \
                     {today}: the provider would answer with a valid but EMPTY artifact, with no \
                     error, and still consume one of five hourly exports - check the year",
                    request.range().start(),
                    request.range().end()
                ),
            });
        }

        let body = request.to_body()?;

        info!(
            dataset = body.dataset,
            symbol = body.symbol,
            timeframe = body.timeframe,
            start = body.start,
            end = body.end,
            "submitting vault export job; this consumes one of the hourly export allowance"
        );

        self.post_json("export", &body).await
    }

    /// Fetch the current state of an export job.
    ///
    /// Polling costs nothing from the export allowance — only from the per-minute call budget.
    ///
    /// # Errors
    /// - [`LseError::RateLimited`] on a `429` — deliberately **not**
    ///   [`LseError::QuotaExceeded`], which is what
    ///   [`submit_export`](Self::submit_export) and [`download_export`](Self::download_export)
    ///   report. Those two spend metered allowance (an export, and bytes respectively), so a `429`
    ///   there means the allowance is gone; a status read spends neither, so a `429` here is the
    ///   per-minute limit. Reporting it as an exhausted allowance would also add a `usage()` round
    ///   trip to every iteration of a polling loop, and would assert a provider behaviour that has
    ///   not been measured on this endpoint.
    /// - [`LseError::Api`] for any other non-success status, including the `404` an unknown job id
    ///   returns.
    /// - [`LseError::Deserialize`] if the response will not decode.
    pub async fn export_status(&self, job_id: &str) -> Result<LseExportJobStatus, LseError> {
        self.get_json(&format!("export/{job_id}"), &[]).await
    }

    /// Poll an export job until it reaches a terminal state.
    ///
    /// # Cadence is the caller's
    /// `poll_interval` and `timeout` are both supplied by the caller and neither is defaulted:
    /// this integration exposes the allowance signal and never decides pacing on the consumer's
    /// behalf.
    ///
    /// `timeout` bounds the call whatever `poll_interval` is — the sleep is cut short at the deadline
    /// rather than run in full — so a long interval paired with a short timeout reports
    /// [`LseError::ExportTimeout`] promptly instead of blocking for an interval first. One status
    /// request is still in flight when the deadline passes, so the call can overrun it by that
    /// request's duration. A `poll_interval` or `timeout` so large that it would overflow the clock
    /// (`Duration::MAX`, used as a "no timeout" / "wait for the deadline" sentinel) saturates rather
    /// than panicking.
    ///
    /// # Errors
    /// - [`LseError::ExportFailed`] if the job reaches `failed` or `expired`.
    /// - [`LseError::ExportTimeout`] if `timeout` elapses first. The job keeps building — the
    ///   identifier stays valid, so a later [`export_status`](Self::export_status) can pick it up
    ///   without spending another export.
    /// - Anything [`export_status`](Self::export_status) returns.
    pub async fn await_export(
        &self,
        job_id: &str,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<LseExportJobStatus, LseError> {
        // `Instant + Duration` panics on overflow, and `Duration::MAX` is a plausible "no timeout"
        // sentinel for a caller to pass. Saturating at a far-future instant makes that mean what the
        // caller intended rather than aborting the process.
        let now = tokio::time::Instant::now();
        let deadline = now
            .checked_add(timeout)
            .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365));

        loop {
            let status = self.export_status(job_id).await?;

            match &status.status {
                LseExportStatus::Ready => return Ok(status),
                LseExportStatus::Failed | LseExportStatus::Expired => {
                    return Err(LseError::ExportFailed {
                        job_id: job_id.to_owned(),
                        status: status.status.to_string(),
                        message: status.error.unwrap_or_default(),
                    });
                }
                pending => {
                    debug!(job_id, status = %pending, "export job not ready");
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(LseError::ExportTimeout {
                    job_id: job_id.to_owned(),
                    status: status.status.to_string(),
                });
            }

            // Never sleeps past the deadline, so `timeout` bounds the call whatever `poll_interval`
            // is. Sleeping the full interval first would let `poll_interval = 60s, timeout = 5s`
            // block for a minute before reporting a timeout it had already passed.
            //
            // `checked_add` for the same reason as `deadline` above, and it is NOT covered by that
            // one: `.min(deadline)` clamps the result, but the addition producing it is evaluated
            // first, so a bare `+` here would abort the caller's task on a `Duration::MAX`
            // `poll_interval` — the "poll once, then wait for the deadline" spelling, and no less
            // plausible than the same sentinel for `timeout`. An interval that cannot be
            // represented is longer than any deadline, so falling back to the deadline is what it
            // means.
            let next_poll = tokio::time::Instant::now()
                .checked_add(poll_interval)
                .map_or(deadline, |instant| deadline.min(instant));
            tokio::time::sleep_until(next_poll).await;
        }
    }

    /// Download a `ready` export artifact to `destination`.
    ///
    /// # Transfer bound
    /// The shared client applies a 30 s **total** deadline, which `reqwest` runs through to the end
    /// of the response body — right for a page of JSON, fatal for an artifact, since a transfer
    /// longer than 30 s would abort mid-body however healthy the connection. This request therefore
    /// overrides it with a six-hour backstop (enough for a 7 GB artifact on a ~3 Mbit/s link) and
    /// relies on the client's **per-read** timeout to detect an actual stall in seconds. An
    /// interrupted transfer resumes rather than restarting, so a slower link than that still
    /// converges across calls.
    ///
    /// # Resume and integrity
    /// Downloads into `destination` + `.<job id>.part`, resuming an interrupted transfer with a
    /// `Range` request (the provider advertises `Accept-Ranges: bytes` and answers `206`). The
    /// in-progress file is scoped to the **job**, not to the destination alone, so a leftover can
    /// only ever be a prefix of the artifact this call is fetching — one job never resumes onto
    /// another's bytes. A `206` is
    /// accepted only if its `Content-Range` starts at the byte that was asked for; a `416` is
    /// treated as "the `.part` already holds the whole artifact" rather than as a failure, so a
    /// transfer interrupted between the final write and the rename still converges. On completion
    /// the artifact is verified against the job's `sha256` and `bytes`, then atomically renamed
    /// into place. **On a mismatch an error is returned and the destination is left untouched**,
    /// so it never contains a corrupt file.
    ///
    /// **Verification covers only the dimensions the job actually reports.** Both fields are
    /// [`Option`], and every measured `ready` job carried both — but that is an observation, not a
    /// guarantee the provider makes. A `ready` job missing one is downloaded and renamed with that
    /// check skipped, and a `warn!` naming the skipped check is emitted. Skipping is deliberate:
    /// refusing an artifact the provider considers ready, over metadata this integration merely
    /// expects, would forbid a correct download with no way around it. A job reporting **neither**
    /// field additionally ignores any pre-existing `.part` and transfers the artifact in full, so
    /// what lands at `destination` is at least something this call fetched end to end rather than a
    /// leftover accepted on the strength of its filename. A download consumes no export allowance,
    /// so that costs bandwidth only.
    ///
    /// # ⚠️ Caller obligation: one download per destination at a time
    /// Concurrent calls sharing a `destination` are **not supported**. Each reads the `.part` file's
    /// length to decide where to resume from, then opens it; two overlapping calls interleave writes
    /// against independently-primed hashers, corrupting the `.part` or failing verification
    /// spuriously. Serialise them, or give each its own destination.
    ///
    /// A `.part` that failed verification is **kept when it is incomplete and discarded when it is
    /// corrupt**, which is decided by *which* check failed rather than by whether this call happened
    /// to append anything. Shorter than the artifact means a truncated transfer, so the bytes are a
    /// valid prefix and a re-call resumes from them — for a multi-gigabyte artifact that is the
    /// difference between finishing and starting over. Longer than the artifact, or the right length
    /// at the wrong digest with nothing left to fetch, means corrupt: the file is removed so a
    /// re-call restarts rather than failing identically forever. [`LseError::IntegrityMismatch`]
    /// reports which happened via `discarded`.
    ///
    /// # ⚠️ Caller obligation: reclaiming abandoned `.part` files
    /// **Nothing here ever deletes a `.part` except the corrupt-file path above.** A `.part` is
    /// named for its job (see the resume section), which is what makes resuming safe — and what
    /// makes an abandoned one unreachable: a process killed mid-transfer leaves its `.part` behind,
    /// and a *re-export* of the same range gets a new job id, so the next call writes a new file
    /// and never looks at the old one. A 7 GB export killed at 6 GB occupies 6 GB indefinitely.
    ///
    /// This integration deliberately offers no cleanup or enumeration API: deleting files it did
    /// not write this call is a policy decision, and "which of these is still wanted" depends on
    /// whether the caller intends to resume — which only the caller knows. Reclaiming them is
    /// therefore yours. They are identifiable without help: they sit next to the destination, so a
    /// glob of `<destination>.*.part` finds every candidate. Matching a *specific* job needs one
    /// step of care — the id embedded in the name is the job id with every character outside
    /// `[A-Za-z0-9._-]` replaced by `_`, since a provider-supplied id is not assumed path-safe — so
    /// apply the same substitution before deciding which ids to spare.
    ///
    /// Downloads consume no export allowance, so discarding a `.part` you no longer want costs
    /// bandwidth on the next attempt and nothing else.
    ///
    /// The URL is built from the client's base URL and the job id rather than from the job's
    /// `download_url`, preserving this client's invariant that it only ever requests URLs it
    /// constructed itself.
    ///
    /// # ⚠️ A `ready` job may legitimately contain zero rows
    /// Check [`LseExportJobStatus::rows`]. A zero-row artifact is a well-formed Parquet file with
    /// the full schema, so nothing downstream will complain.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if the job is not `ready`.
    /// - [`LseError::IntegrityMismatch`] if the downloaded bytes do not match the job's `sha256`
    ///   or `bytes`.
    /// - [`LseError::Io`] for any filesystem failure.
    /// - [`LseError::QuotaExceeded`] / [`LseError::Api`] as for
    ///   [`submit_export`](Self::submit_export).
    pub async fn download_export(
        &self,
        job: &LseExportJobStatus,
        destination: impl AsRef<Path>,
        request: &LseExportRequest,
    ) -> Result<LseExport, LseError> {
        if job.status != LseExportStatus::Ready {
            return Err(LseError::InvalidInput {
                message: format!(
                    "export job {} is {}, not ready; nothing to download",
                    job.id, job.status
                ),
            });
        }

        verify_job_covers_request(job, request)?;

        let destination = destination.as_ref();
        let part = part_path(destination, &job.id);

        if let Some(parent) = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| LseError::Io {
                    message: format!("creating {}", parent.display()),
                    source,
                })?;
        }

        // A job reporting neither `bytes` nor `sha256` offers nothing to verify against, so a
        // pre-existing `.part` cannot be checked even once it is complete — resuming onto it would
        // rename an unverified file into place on the strength of its name alone, having fetched
        // none of it. Re-downloading costs bandwidth and **no allowance** (the five-per-hour limit
        // is on export *submits*), so the leftover is ignored and the transfer restarts. Every
        // measured `ready` job reported both fields, so this is the unmeasured path rather than the
        // normal one.
        let verifiable = job.bytes.is_some() || job.sha256.is_some();

        // Hash the already-downloaded prefix so a resumed transfer still verifies end to end.
        let (mut hasher, mut downloaded) = if verifiable {
            hash_existing_part(&part).await?
        } else {
            (Sha256::new(), 0)
        };

        // A `.part` that already holds the whole artifact means a previous run was interrupted
        // between the final write and the rename. Re-requesting would only earn a 416. The
        // `downloaded > 0` guard keeps a job reporting `bytes: 0` from satisfying this vacuously
        // when no `.part` exists at all, which would skip the fetch and then rename a file that was
        // never created.
        let mut complete = downloaded > 0 && job.bytes.is_some_and(|total| downloaded >= total);

        if !complete {
            let transferred = self.transfer_export(job, &part, hasher, downloaded).await?;

            hasher = transferred.hasher;
            downloaded = transferred.downloaded;
            // `transfer_export` cannot tell a finished stream from a truncated one on its own, so
            // it only ever reports `exhausted` for the `416` that proves there is nothing left. The
            // byte count settles the remaining case here, where `job.bytes` is in scope: a transfer
            // that produced the whole artifact is exhausted whether or not the server said so, and
            // treating it as resumable would keep a wrong-digest file that no further request can
            // ever repair.
            complete = transferred.exhausted
                || job
                    .bytes
                    .is_some_and(|total| transferred.downloaded >= total);
        }

        let digest = hex::encode(hasher.finalize());

        // The `.part` is scoped to this job (see `part_path`), so a leftover can only ever be a
        // prefix of *this* artifact. What to do with a file that failed verification therefore
        // follows from **which** check failed, rather than from the weaker "did this call append
        // anything?" question:
        //
        // - **Shorter than the artifact**: the transfer was truncated — a dropped connection, a
        //   closed stream. Those bytes are a valid prefix, so the file is KEPT and a re-call resumes
        //   from it. For a multi-gigabyte artifact that is the difference between finishing and
        //   starting over, and truncation is the common failure, not the rare one.
        // - **Longer than the artifact**: it cannot be a prefix of anything, so it is corrupt.
        //   DISCARDED, so a re-call restarts rather than failing identically forever.
        // - **Right length (or no length reported), wrong digest**: with nothing left to fetch —
        //   `complete`, i.e. the file already looked whole or the server answered the resume `Range`
        //   with a `416` — the bytes on disk are all there will ever be, so they are corrupt and are
        //   DISCARDED. Otherwise this call did append, and when no byte count was reported a short
        //   read is indistinguishable from corruption: the file is KEPT so a re-call can resume, and
        //   the call after that converges on the `416`.
        //
        // Discarding only ever removes a file this integration is the sole writer of.
        if let Some(expected) = job.bytes.filter(|expected| *expected != downloaded) {
            let corrupt = downloaded > expected;
            return Err(self
                .integrity_mismatch(
                    &part,
                    format!("{expected} bytes"),
                    format!("{downloaded} bytes"),
                    corrupt,
                )
                .await);
        }

        if let Some(expected) = job
            .sha256
            .as_ref()
            .filter(|expected| !expected.eq_ignore_ascii_case(&digest))
        {
            return Err(self
                .integrity_mismatch(&part, expected.clone(), digest, complete)
                .await);
        }

        // Verification is only as complete as the job's metadata. Every measured `ready` job
        // reported both fields, so this should not fire — which is exactly why it is worth saying
        // out loud if it ever does, rather than renaming an unverified artifact silently.
        if job.bytes.is_none() || job.sha256.is_none() {
            warn!(
                job_id = job.id,
                path = %destination.display(),
                checked_length = job.bytes.is_some(),
                checked_sha256 = job.sha256.is_some(),
                "export job reports no integrity metadata for one or both checks; accepting the \
                 artifact with that verification skipped"
            );
        }

        tokio::fs::rename(&part, destination)
            .await
            .map_err(|source| LseError::Io {
                message: format!("renaming {} to {}", part.display(), destination.display()),
                source,
            })?;

        info!(
            job_id = job.id,
            path = %destination.display(),
            bytes = downloaded,
            rows = job.rows.unwrap_or_default(),
            "export downloaded and verified"
        );

        Ok(LseExport::new(
            destination,
            request.dataset(),
            request.symbol(),
            request.timeframe(),
            request.range(),
        ))
    }

    /// Fetch whatever of `job`'s artifact is not already in `part`, appending to it.
    ///
    /// `hasher` must already cover the `downloaded` bytes on disk, so that a resumed transfer still
    /// verifies end to end.
    ///
    /// Split out of [`download_export`](Self::download_export) so that the decision of *what* to do
    /// with the resulting file — resume, discard, rename — reads on its own, apart from the
    /// mechanics of getting the bytes.
    ///
    /// # Errors
    /// [`LseError::QuotaExceeded`] on a `429`, [`LseError::Api`] for any other non-success status,
    /// [`LseError::Http`] for a transport failure mid-stream, and [`LseError::Io`] for a filesystem
    /// failure. In every case the `.part` is left as it stands; the caller decides its fate.
    async fn transfer_export(
        &self,
        job: &LseExportJobStatus,
        part: &Path,
        mut hasher: Sha256,
        mut downloaded: u64,
    ) -> Result<Transferred, LseError> {
        // Held for the whole transfer, body included: an artifact download occupies a connection
        // for as long as it runs, so counting it only while its headers are in flight would let
        // the client exceed the provider's concurrency ceiling for hours.
        let permit = self.enter_gate().await;

        let url = format!("{}/export/{}/download", self.base_url(), job.id);
        // Overrides the client's total deadline for this request only; see `DOWNLOAD_TIMEOUT`.
        let mut builder = self.http().get(&url).timeout(DOWNLOAD_TIMEOUT);
        if downloaded > 0 {
            builder = builder.header(reqwest::header::RANGE, format!("bytes={downloaded}-"));
        }

        let response = builder.send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Released before `quota_exceeded`, which issues a `usage` call that takes a permit of
            // its own: holding this one across it would deadlock a client built with
            // `with_concurrency(1)`.
            drop(permit);
            return Err(self.quota_exceeded().await);
        }

        // A `416` answers a `Range` starting at or past the artifact's length: the `.part` already
        // holds everything the server has. That is precisely the case the caller's `complete` check
        // cannot decide for itself, because it needs `job.bytes` and the job is entitled to report
        // `None` — verification tolerates that absence, so resuming must too. Failing here instead
        // would make the documented "re-calling resumes" never converge: every retry would
        // re-request the same unsatisfiable range. Whether those bytes are actually *this* job's
        // artifact is still decided by the caller's integrity checks.
        if downloaded > 0 && status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            warn!(
                job_id = job.id,
                downloaded,
                "range request rejected as unsatisfiable; the existing part file already holds the \
                 whole artifact"
            );

            return Ok(Transferred {
                hasher,
                downloaded,
                exhausted: true,
            });
        }

        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            return Err(LseError::Api {
                status: status.as_u16(),
                message: extract_detail(&body),
            });
        }

        // The server is entitled to ignore `Range` and answer `200` with the whole artifact.
        // Restarting is then the only correct response — appending would duplicate the prefix.
        let resuming = downloaded > 0 && status == reqwest::StatusCode::PARTIAL_CONTENT;
        if resuming {
            verify_resume_offset(&response, downloaded)?;
        }
        if downloaded > 0 && !resuming {
            warn!(
                job_id = job.id,
                downloaded, "range request answered in full; restarting the download"
            );
            hasher = Sha256::new();
            downloaded = 0;
        }

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(!resuming)
            .append(resuming)
            .open(part)
            .await
            .map_err(|source| LseError::Io {
                message: format!("opening {}", part.display()),
                source,
            })?;

        // Buffered because `tokio::fs` dispatches each write to the blocking pool: writing every
        // HTTP chunk straight through costs one round trip per chunk, and artifacts run to
        // gigabytes. An interrupted transfer loses at most one buffer of resume progress — the next
        // call re-reads the `.part`'s real on-disk length, so it resumes correctly from whatever
        // landed, just slightly further back.
        let mut file = tokio::io::BufWriter::with_capacity(DOWNLOAD_BUFFER_BYTES, file);

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            downloaded += chunk.len() as u64;
            file.write_all(&chunk)
                .await
                .map_err(|source| LseError::Io {
                    message: format!("writing {}", part.display()),
                    source,
                })?;

            // Stop as soon as the response outruns the length the job reported. Without this the
            // only bound on the body is the six-hour deadline: a misbehaving proxy streaming
            // 500 GB against a `bytes: 1_000_000` job fills the disk before the length check in
            // `download_export` ever runs, and on `ENOSPC` the write fails with the oversized
            // `.part` RETAINED — the one state that is neither resumable nor cleaned up.
            //
            // The overshoot is bounded by one chunk, and the chunk is written before the break on
            // purpose: `downloaded` then matches the file's real length, so the caller sees
            // `downloaded > expected` and takes its "longer than the artifact, therefore corrupt"
            // branch, which discards the `.part` and says so. Breaking before the write would land
            // on exactly `expected` bytes and report a digest mismatch instead — the same outcome
            // under a diagnosis that sends the reader after the wrong problem.
            //
            // A job reporting no `bytes` has nothing to bound against; that path stays as it was.
            if job.bytes.is_some_and(|total| downloaded > total) {
                warn!(
                    job_id = job.id,
                    downloaded,
                    expected = job.bytes,
                    "export download exceeded the length the job reports; abandoning the transfer \
                     rather than writing an unbounded body to disk"
                );
                break;
            }
        }

        file.flush().await.map_err(|source| LseError::Io {
            message: format!("flushing {}", part.display()),
            source,
        })?;

        Ok(Transferred {
            hasher,
            downloaded,
            // The stream ended, but nothing observed *here* distinguishes a complete artifact from
            // a truncated one — only the `416` above proves exhaustion. Reported as not exhausted
            // so that a digest failure keeps the `.part` for a resume; `download_export` overrides
            // this when the job's byte count shows the transfer already produced the whole
            // artifact, and otherwise the call after next converges on the `416`.
            exhausted: false,
        })
    }

    /// Issue an authenticated `POST` against a vault path and deserialise the JSON body.
    ///
    /// Accepts `202` as success — the export endpoint's normal answer.
    ///
    /// Rationed by the client's shared gate, exactly as `GET`s are: an export submission spends
    /// the same allowance a candle page does.
    ///
    /// # Errors
    /// Maps a `429` to [`LseError::QuotaExceeded`], any other non-success status to
    /// [`LseError::Api`], and an undecodable body to [`LseError::Deserialize`].
    pub(crate) async fn post_json<B, T>(&self, path: &str, body: &B) -> Result<T, LseError>
    where
        B: Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let permit = self.enter_gate().await;

        let url = format!("{}/{path}", self.base_url());
        let response = self.http().post(&url).json(body).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // See `transfer_export`: `quota_exceeded` takes a permit of its own, so this one must
            // go first or a single-permit client deadlocks.
            drop(permit);
            return Err(self.quota_exceeded().await);
        }

        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            return Err(LseError::Api {
                status: status.as_u16(),
                message: extract_detail(&body),
            });
        }

        let body = response.text().await?;
        debug!(len = body.len(), path, "vault response received");

        serde_json::from_str(&body).map_err(|error| LseError::Deserialize {
            message: format!("{path}: {error}"),
        })
    }

    /// Build a [`LseError::IntegrityMismatch`], discarding the partial file when it cannot be
    /// resumed from.
    ///
    /// `discard` is set when the file is **corrupt** rather than merely **incomplete**: longer than
    /// the artifact, or the right length at the wrong digest with nothing left to fetch. An
    /// incomplete file is a valid prefix and is kept, so a re-call resumes from it instead of
    /// re-transferring everything. A removal failure is logged rather than replacing the integrity
    /// error, which is the more useful diagnostic of the two.
    async fn integrity_mismatch(
        &self,
        part: &Path,
        expected: String,
        actual: String,
        discard: bool,
    ) -> LseError {
        if discard {
            warn!(
                path = %part.display(),
                %expected,
                %actual,
                "the partial file is corrupt rather than incomplete -- it is longer than the \
                 artifact, or complete at the wrong digest -- so it cannot be resumed; discarding \
                 it so a re-call restarts"
            );

            if let Err(error) = tokio::fs::remove_file(part).await {
                warn!(
                    path = %part.display(),
                    %error,
                    "could not remove the unusable partial file; remove it by hand, or this job \
                     will keep failing verification"
                );
            }
        }

        LseError::IntegrityMismatch {
            path: part.to_path_buf(),
            expected,
            actual,
            discarded: discard,
        }
    }

    /// Build a [`LseError::QuotaExceeded`] describing where the allowance stands.
    ///
    /// The export endpoints answer a `429` with no `Retry-After` and no rate-limit headers, so the
    /// position has to be fetched. A failed follow-up yields `status: None` rather than a fabricated
    /// [`QuotaStatus`](super::quota::QuotaStatus) — and, importantly, **still**
    /// [`LseError::QuotaExceeded`], so a caller pacing itself off that variant sees every exhausted
    /// allowance rather than silently missing the ones where the follow-up call happened to fail too.
    async fn quota_exceeded(&self) -> LseError {
        let status = match self.usage().await {
            Ok(status) => Some(status),
            Err(error) => {
                warn!(
                    %error,
                    "export allowance exhausted, and the follow-up usage call failed; reporting \
                     the rejection without an allowance position"
                );
                None
            }
        };

        LseError::QuotaExceeded { status }
    }
}

/// What one call to [`LseVaultClient::transfer_export`] left on disk.
struct Transferred {
    /// Advanced over every byte of the `.part`, prefix included, so the digest is end to end.
    hasher: Sha256,
    /// The `.part`'s length afterwards.
    downloaded: u64,
    /// `true` when the server confirmed there is nothing left to fetch, which is the one thing a
    /// byte count cannot say when the job reports none. It decides whether a later digest failure
    /// means "corrupt, discard" or "truncated, keep and resume".
    exhausted: bool,
}

/// Check that `job` describes `request`, on every dimension the provider echoed back.
///
/// This provider **silently substitutes defaults for parameters it does not recognise**, so a
/// misspelled field earns a `200` and an artifact covering something else — the failure mode a
/// wiremock test already pins for the `timeframe` name. The status response echoes `dataset`,
/// `symbol`, `timeframe`, `start` and `end`, and that echo is the only client-side evidence of what a
/// job actually covers. Checking it here turns "decoded the wrong instrument's tape" into an error at
/// the point the artifact is claimed, rather than a silent misattribution downstream.
///
/// A field the provider omits is not checked: absence is not disagreement, and refusing to download
/// over metadata the provider never promised would forbid a correct download. `symbol` is compared
/// case-insensitively — the provider publishes symbols uppercase but echoes back what was sent.
///
/// # ⚠️ `start`/`end` are compared as strings, against [`NaiveDate`]'s `Display`
/// This assumes the provider echoes dates as `YYYY-MM-DD`, which is measured but not guaranteed.
/// The assumption **fails safe**: a provider that switched to `DD/MM/YYYY` or added a time
/// component would make every comparison unequal, so the job is rejected as a mismatch rather than
/// accepted on a bad match. That is over-strict, not unsound — a correct download would start
/// failing loudly, and the fix is to parse both sides rather than to relax the check. Parsing is
/// not done today because the *purpose* here is detecting a substituted default, and an
/// unparseable echo is itself evidence of one.
///
/// [`NaiveDate`]: chrono::NaiveDate
fn verify_job_covers_request(
    job: &LseExportJobStatus,
    request: &LseExportRequest,
) -> Result<(), LseError> {
    let mismatch = |field: &str, requested: String, reported: &str| LseError::ExportJobMismatch {
        job_id: job.id.clone(),
        field: field.to_owned(),
        requested,
        reported: reported.to_owned(),
    };

    if let Some(reported) = job.dataset.as_deref() {
        let requested = request.dataset().as_catalog_str();
        if reported != requested {
            return Err(mismatch("dataset", requested.to_owned(), reported));
        }
    }

    if let Some(reported) = job.symbol.as_deref()
        && !reported.eq_ignore_ascii_case(request.symbol())
    {
        return Err(mismatch("symbol", request.symbol().to_owned(), reported));
    }

    // `as_lse_str` is fallible only for a resolution this integration cannot express, which
    // `LseExportRequest::new` already rejected -- so a request in hand always has one.
    if let Some(reported) = job.timeframe.as_deref()
        && let Ok(requested) = request.timeframe().as_lse_str()
        && reported != requested
    {
        return Err(mismatch("timeframe", requested.to_owned(), reported));
    }

    if let Some(reported) = job.start.as_deref() {
        let requested = request.range().start().to_string();
        if reported != requested {
            return Err(mismatch("start", requested, reported));
        }
    }

    if let Some(reported) = job.end.as_deref() {
        let requested = request.range().end().to_string();
        if reported != requested {
            return Err(mismatch("end", requested, reported));
        }
    }

    Ok(())
}

/// The in-progress filename for one **job's** artifact: `x.parquet` becomes `x.parquet.<job>.part`.
///
/// Appends to the whole filename rather than replacing the extension, so it never collides with a
/// sibling of a different type.
///
/// Scoped to the job rather than to the destination alone, which makes "a leftover in-progress file
/// is a prefix of the artifact being fetched" an invariant of the *name*. Sharing one `.part` across
/// jobs left that as an inference from whether the call had appended bytes — a test that cannot tell
/// this job's prefix from a different job's, so a `.part` from an earlier job at the same destination
/// was resumed onto, corrupting the result and costing a billed download to discover it.
///
/// The job id is an opaque provider token (measured: UUID-shaped). Any character outside
/// `[A-Za-z0-9._-]` is replaced with `_`, so an id cannot introduce path components or escape the
/// destination's directory.
fn part_path(destination: &Path, job_id: &str) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".");
    name.push(
        job_id
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>(),
    );
    name.push(".part");
    PathBuf::from(name)
}

/// Confirm a `206` resumes from exactly the offset that was requested.
///
/// A `206` starting anywhere else gets appended to the `.part` as though it continued it, leaving a
/// file that is neither the artifact nor a resumable prefix of it. The `bytes` and `sha256` checks
/// would normally catch that — but both are `Option` on the job, so a `ready` job reporting neither
/// would rename the corrupt result into place. Checking the header closes that gap where the
/// mistake is made, and says what actually went wrong instead of "digest mismatch".
///
/// A `206` carrying no parseable `Content-Range` is itself a protocol violation (RFC 9110 §15.3.7
/// requires the header), so it is surfaced rather than assumed benign.
fn verify_resume_offset(response: &reqwest::Response, expected_start: u64) -> Result<(), LseError> {
    let status = reqwest::StatusCode::PARTIAL_CONTENT.as_u16();

    let header = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| LseError::Api {
            status,
            message: "partial-content response carries no usable Content-Range header".to_owned(),
        })?;

    // `bytes <first>-<last>/<complete-length|*>`. Only `<first>` decides where these bytes belong.
    let first = header
        .trim()
        .strip_prefix("bytes ")
        .and_then(|range| range.split('-').next())
        .and_then(|first| first.trim().parse::<u64>().ok())
        .ok_or_else(|| LseError::Api {
            status,
            message: format!("unparseable Content-Range on a partial-content response: {header}"),
        })?;

    if first != expected_start {
        return Err(LseError::Api {
            status,
            message: format!(
                "partial-content response resumes at byte {first}, not the requested \
                 {expected_start}"
            ),
        });
    }

    Ok(())
}

/// Read an existing `.part` back into a hasher, returning it and the byte count.
///
/// A resumed download must hash the bytes it did not fetch, or the final digest is meaningless.
async fn hash_existing_part(part: &Path) -> Result<(Sha256, u64), LseError> {
    let mut hasher = Sha256::new();

    let mut file = match tokio::fs::File::open(part).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((hasher, 0)),
        Err(source) => {
            return Err(LseError::Io {
                message: format!("opening {}", part.display()),
                source,
            });
        }
    };

    let mut total = 0_u64;
    let mut buffer = vec![0_u8; HASH_READ_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| LseError::Io {
                message: format!("reading {}", part.display()),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }

    debug!(path = %part.display(), bytes = total, "resuming a partial export download");

    Ok((hasher, total))
}
