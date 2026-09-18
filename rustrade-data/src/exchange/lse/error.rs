use crate::exchange::lse::quota::QuotaStatus;
use crate::subscription::candle::CandleInterval;
use chrono::{DateTime, Utc};
use std::time::Duration;
use thiserror::Error;

/// Errors produced by the London Strategic Edge integration.
///
/// `#[non_exhaustive]`: further variants are added alongside the endpoints that raise them.
///
/// Deliberately not `Clone`/`PartialEq` — [`Http`](Self::Http) wraps a [`reqwest::Error`], which is
/// neither, matching every other REST-backed integration in this crate.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LseError {
    /// More than one distinct dataset series resolves to the requested slug, so no single dataset
    /// can be identified.
    ///
    /// Returned rather than picking one, because the provider's `/dataset/info` endpoint answers
    /// `200` for an ambiguous slug — silently serving whichever series it prefers. Two measured
    /// families produce this: eleven Eurex futures that publish both a bare and a `.F` series
    /// containing *different* data (the bare series is frequently the far larger one and has no
    /// slug of its own), and futures whose stripped symbol collides with an unrelated equity
    /// ticker.
    #[error(
        "ambiguous dataset slug {slug:?} for symbol {symbol:?}: more than one series resolves to \
         it, so it cannot identify a dataset - query the catalog and select explicitly"
    )]
    AmbiguousSlug { symbol: String, slug: String },

    /// The provided string does not name a known London Strategic Edge price dataset.
    #[error("unknown dataset {0:?}")]
    UnknownDataset(String),

    /// A required environment variable is not set, or does not hold valid UTF-8 (see
    /// [`LseVaultClient::from_env`]).
    ///
    /// The message names the variable and never its value, so a mis-encoded key cannot reach a log
    /// line through this error.
    ///
    /// [`LseVaultClient::from_env`]: super::vault::LseVaultClient::from_env
    #[error("environment variable error: {0}")]
    EnvVar(String),

    /// The supplied API key cannot be encoded as an HTTP header value (e.g. non-ASCII bytes).
    #[error("invalid credential: {0}")]
    InvalidCredential(String),

    /// The request could not be completed at the transport layer.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// The API returned a non-success status, or a success response this integration cannot use.
    ///
    /// `message` is the provider's own diagnostic, unwrapped from its response envelope and
    /// truncated to a bounded length — or, where `status` is a success code, this integration's own
    /// diagnostic for a response that violated the contract that status implies. A `206` whose
    /// `Content-Range` is missing, unparseable, or does not resume where the `Range` asked, and a
    /// `200` page that repeats the cursor it was given, all arrive here: the status is the one the
    /// provider sent, so it stays reportable, but the fault is one only the client can see.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// The request rate was exceeded.
    ///
    /// Terminal — this integration never sleeps and retries on the caller's behalf. A paged fetch
    /// yields this and **ends**; resume by re-requesting from the last `close_time` received.
    ///
    /// The provider permits [`calls_per_minute`](QuotaStatus::calls_per_minute) requests per
    /// minute, reported by [`usage`](super::vault::LseVaultClient::usage).
    ///
    /// # Caveat
    /// On the *export* endpoints an exhausted allowance is distinguished and reported as
    /// [`QuotaExceeded`](Self::QuotaExceeded). On the candle path the provider offers no way to
    /// tell a per-minute rate limit from an exhausted byte allowance, so both arrive here. Either
    /// way it is observable and terminal, and never silently retried.
    #[error("rate limited{}", match .retry_after {
        Some(delay) => format!("; retry after {}s", delay.as_secs()),
        None => String::new(),
    })]
    RateLimited { retry_after: Option<Duration> },

    /// A response body could not be decoded into the expected shape.
    #[error("invalid response: {message}")]
    Deserialize { message: String },

    /// The request is malformed in a way the caller must fix, detected before it is sent.
    #[error("invalid input: {message}")]
    InvalidInput { message: String },

    /// The requested resolution is not one the provider serves.
    ///
    /// [`CandleInterval`] is the venue-agnostic union of every resolution any connector in this
    /// crate serves; this provider serves 14 of them. Rejected before the request is sent rather
    /// than relayed as a `400`, so the caller gets a typed answer.
    #[error("unsupported candle interval {interval}: the provider does not serve this resolution")]
    UnsupportedInterval { interval: CandleInterval },

    /// A candle's period-end boundary is not representable.
    ///
    /// `close_time == open_time + interval` can overflow the representable [`DateTime<Utc>`]
    /// range. Surfaced rather than clamped: a silently substituted boundary would be a
    /// plausible-looking wrong timestamp on a real candle.
    #[error("candle boundary overflow: open time {open} + {interval} is not representable")]
    TimestampOverflow {
        open: DateTime<Utc>,
        interval: CandleInterval,
    },

    /// The pagination cursor could not be advanced past the last bar received.
    ///
    /// The cursor steps one second past the newest open time on a page, which is not representable
    /// within a second of [`DateTime::MAX_UTC`]. Distinct from
    /// [`TimestampOverflow`](Self::TimestampOverflow), which is a *candle's* period end: the addend
    /// here is the cursor step, not the interval, and naming the interval would send a reader
    /// looking at arithmetic the code never performed.
    #[error("pagination cursor overflow: open time {last_open} + 1s is not representable")]
    CursorOverflow { last_open: DateTime<Utc> },

    /// The vault served a candle closing past the requested `end`.
    ///
    /// The upper bound sent to the vault is exact by construction: it is `end - interval + 1s`
    /// against a parameter that is *exclusive* on open time, so the newest bar a compliant page can
    /// carry is the one whose close falls exactly on `end`. A later one means the range parameters
    /// were not honoured — the same silently-ignored-parameter failure as a page that repeats the
    /// cursor it was given, which is why both are terminal rather than quietly repaired.
    ///
    /// # Not symmetric with the lower bound, deliberately
    /// The lower bound is widened by one interval *on purpose*, to readmit the bar whose close
    /// equals `start`, so a page legitimately carries bars closing before `start` and those are
    /// trimmed without comment. Nothing widens the upper bound, so there is no benign reading of a
    /// bar past it.
    ///
    /// Every in-range bar on the offending page is yielded **before** this arrives: the page is
    /// scanned to the end first, so failing here costs none of the data the response did contain.
    #[error(
        "page {page} for {symbol:?} returned a candle closing after {end} (cursor {cursor}): the \
         range parameters appear to have been ignored"
    )]
    UnexpectedCandleRange {
        symbol: String,
        cursor: DateTime<Utc>,
        page: usize,
        end: DateTime<Utc>,
    },

    /// A candle page stepped **backwards** in open time.
    ///
    /// Ascending order is how the vault answers today, not something it guarantees, and the
    /// pagination cursor depends on it being at least non-descending *across* pages: the cursor
    /// advances past the newest open a page carried, so a page served newest-first advances it to
    /// the end of the range immediately. The next page is then empty, which is the documented
    /// end-of-data signal, and `collect_candles` returns `Ok` with the last page of a multi-million
    /// bar range and no indication that the rest was skipped.
    ///
    /// Terminal rather than repaired by sorting: a page arriving in an order the cursor arithmetic
    /// was not built for means the vault changed behaviour, and every later assumption in this
    /// module — the trim bounds, the cursor step, the end-of-data signal — was derived from the old
    /// one. The same property is a typed rejection on the Parquet path
    /// ([`NonMonotonicTimestamps`](Self::NonMonotonicTimestamps)).
    #[error(
        "page {page} for {symbol:?} stepped backwards in open time: {open} follows {previous_open}"
    )]
    NonMonotonicCandlePage {
        symbol: String,
        page: usize,
        previous_open: DateTime<Utc>,
        open: DateTime<Utc>,
    },

    /// Two consecutive candles are spaced closer together than the interval they were asked for.
    ///
    /// Impossible for a compliant fixed-interval series: consecutive bars are exactly one interval
    /// apart, and a gap (a weekend, a holiday, a quiet symbol) only ever makes the spacing *wider*.
    /// Spacing narrower than the interval means the candles are at a **finer resolution than the one
    /// requested**, so every `close_time` derived for them overlaps the bars that follow.
    ///
    /// # The failure this exists to catch, on the vault path
    /// The vault answers `200` to a misspelled resolution parameter and silently defaults to
    /// 1-minute bars, in a byte-identical response shape (see the `vault` module docs). A `Day1`
    /// request would then receive 1,440 one-minute rows per day, each stamped with a `close_time` of
    /// `open + 24h` by the `historical` module's boundary arithmetic — overlapping, wrong, and
    /// invisible to every other check there, because the range bounds are still satisfied and the
    /// series still ascends. Spacing is the one property that distinguishes it.
    ///
    /// # And on the export path
    /// `parquet::read_export` derives `close_time` from a **caller-supplied** interval, since
    /// nothing in a Parquet artifact records the resolution it was written at. The wrong resolution
    /// therefore reaches the same arithmetic from a different origin — the caller's declaration
    /// rather than the provider's answer — and produces the same overlapping bars, so the same
    /// check guards it: a file of 1-minute bars decoded as `Day1` is rejected rather than yielding
    /// 390 "daily" bars per trading day. Reported in terms of the artifact's `ts` column, which
    /// carries the bar's open — the close is a value that decoder derived, and only the open is
    /// something a caller can find in the file itself.
    ///
    /// # Limits
    /// Only the **finer-than-asked-for** direction is detectable. Coarser is not: daily bars taken
    /// for `Min1` are spaced 24 hours apart, which is wider than 60 seconds and indistinguishable
    /// from a genuine gap. A single candle is not checked at all, having no pair to measure.
    ///
    /// Only [`IntervalStep::Fixed`](crate::subscription::candle::IntervalStep::Fixed) resolutions
    /// are checked. A calendar step (`month`, `quarter`, `year`) has no single width to compare
    /// against, and February would false-positive against a 31-day one.
    #[error(
        "candles for {symbol:?} are spaced {actual} apart but {interval} was asked for ({open} \
         follows {previous_open}): they appear to be at a finer resolution than that"
    )]
    UnexpectedCandleResolution {
        symbol: String,
        interval: CandleInterval,
        previous_open: DateTime<Utc>,
        open: DateTime<Utc>,
        // `chrono::TimeDelta`, not the `std::time::Duration` imported above: the spacing is a
        // difference between two instants and the type that produces it is chrono's.
        actual: chrono::TimeDelta,
    },

    /// The shared allowance is exhausted.
    ///
    /// Carries the allowance state at the point of rejection so the caller can decide how to pace.
    /// Terminal — never retried internally.
    ///
    /// # Raised by the export path only
    /// The bulk-export endpoints report allowance exhaustion as a `429` carrying no `Retry-After`
    /// and no rate-limit headers, which this integration distinguishes from a per-minute rate
    /// limit and reports here, populating the position from
    /// [`usage`](super::vault::LseVaultClient::usage). A *candle* fetch that runs into a limit
    /// still surfaces as [`RateLimited`](Self::RateLimited) — the provider gives no way to tell
    /// the two apart on that path.
    /// `status` is `None` when the follow-up [`usage`](super::vault::LseVaultClient::usage) call
    /// itself failed. The allowance is still known to be exhausted — that is what the `429` said —
    /// but this integration will not fabricate a [`QuotaStatus`] to fill the field. One variant
    /// meaning "allowance gone, position unknown" is what lets a caller pace itself by matching a
    /// single variant; reporting the second case as a generic
    /// [`Api`](Self::Api)`{ status: 429 }` instead would make it silently miss half the cases.
    #[error("quota exceeded{}", match .status {
        Some(status) => format!(": {status:?}"),
        None => " (allowance position could not be retrieved)".to_owned(),
    })]
    QuotaExceeded { status: Option<QuotaStatus> },

    /// An export job reached a terminal state without producing an artifact.
    ///
    /// `expired` means the artifact was built and has since been reaped — roughly 48 hours after
    /// it was created. Both are terminal; re-exporting costs another export.
    #[error("export job {job_id} {status}{}", match .message.as_str() {
        "" => String::new(),
        message => format!(": {message}"),
    })]
    ExportFailed {
        job_id: String,
        status: String,
        message: String,
    },

    /// An export job did not become ready within the caller's timeout.
    ///
    /// **The job is not cancelled and the identifier stays valid** — it keeps building, so polling
    /// [`export_status`](super::vault::LseVaultClient::export_status) later picks it up without
    /// spending another export. This is a timeout on waiting, not on the job.
    #[error(
        "export job {job_id} still {status} when the caller's timeout elapsed; it keeps building, so poll it again rather than re-exporting"
    )]
    ExportTimeout { job_id: String, status: String },

    /// A downloaded artifact does not match the integrity metadata the job reported.
    ///
    /// The destination is left untouched rather than holding a corrupt artifact.
    ///
    /// `discarded` reports what happened to the partial file at `path`:
    /// - `false` — it is **retained**, because it is *incomplete* rather than wrong: shorter than the
    ///   artifact, so the bytes are a real prefix the next call resumes from with a `Range` request.
    ///   For a multi-gigabyte artifact that is the difference between finishing and starting over.
    /// - `true` — it was **removed**, because it is *corrupt*: longer than the artifact, or the right
    ///   length at the wrong digest with nothing left to fetch. Retaining it would fail identically
    ///   forever, so a re-call restarts instead.
    #[error("integrity check failed for {}: expected {expected}, got {actual} ({})", .path.display(), match .discarded {
        true => "unusable partial file discarded; re-call to restart",
        false => "partial file kept for resume",
    })]
    IntegrityMismatch {
        path: std::path::PathBuf,
        expected: String,
        actual: String,
        discarded: bool,
    },

    /// A `ready` export job does not describe the request it is being downloaded for.
    ///
    /// The provider **silently substitutes defaults for parameters it does not recognise**, so a
    /// request it partly ignored still yields a `ready` job — covering something other than what was
    /// asked for. The job record echoes `dataset`, `symbol`, `timeframe`, `start` and `end`, and that
    /// echo is the only client-side evidence of what the artifact actually contains. Downloading it
    /// anyway would attribute one instrument's or one range's data to another, silently.
    ///
    /// Not recoverable by re-downloading: the artifact is what it is. The request has to be corrected
    /// and re-exported, which costs one of the five hourly exports.
    #[error(
        "export job {job_id} does not describe this request: {field} is {reported:?}, requested {requested:?}"
    )]
    ExportJobMismatch {
        job_id: String,
        field: String,
        requested: String,
        reported: String,
    },

    /// A filesystem operation failed.
    ///
    /// `message` names the operation and the path; the underlying [`std::io::Error`] is retained as
    /// the error [source](std::error::Error::source), so a caller can match on
    /// [`std::io::ErrorKind`] rather than parse a string.
    #[error("io error: {message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    /// An export artifact's columns match none of the layouts this integration decodes.
    ///
    /// The provider's tick schema **varies by dataset** — `fx` publishes `bid`/`ask`, `stocks`
    /// publishes `price`/`volume`, and the synthetic classes publish `price`/`volume`/`ask` — so
    /// the layout is resolved from the columns present. Reported rather than guessed at: a wrong
    /// column guess yields plausible numbers in the wrong field.
    ///
    /// Also raised for a schema that is not **flat**. Columns are read by leaf index, which equals a
    /// top-level field's position only when every field is a primitive: one nested group contributes
    /// several leaves and shifts every index after it. Every measured artifact is flat, so rather
    /// than support a nesting the provider has never emitted, it is rejected — the alternative is a
    /// decoder whose column mapping is silently wrong on a file it accepted, which for a `bid`/`ask`
    /// pair means reading one as the other.
    #[error("unsupported export schema; columns were: {columns}")]
    UnsupportedSchema { columns: String },

    /// A recognised export column does not have the type this integration decodes it as.
    ///
    /// Distinct from [`UnsupportedSchema`](Self::UnsupportedSchema), which reports columns whose
    /// *names* match no known layout. Here the layout is recognised and one of its columns is the
    /// wrong type, so naming the column and both types is the whole diagnostic — reported up front
    /// rather than as an opaque decode failure on the first row.
    ///
    /// # Why a `ts` column that is not UTC-adjusted lands here
    /// `Timestamp { unit: MICROS, is_adjusted_to_utc: false }` is *physically identical* to the
    /// UTC-adjusted form — same `INT64`, same legacy `TIMESTAMP_MICROS` converted type — and differs
    /// only in the origin its values are measured against. Read as epoch microseconds, a local-time
    /// column shifts every event by the venue's UTC offset with nothing downstream able to notice:
    /// the timeline is still monotonic and the prices are still right, so a backtest simply trades
    /// on data it could not have had. This check is the only place that difference is visible.
    #[error("export column {column:?} is {found}, but this integration requires {required}")]
    UnsupportedColumnType {
        column: String,
        required: &'static str,
        found: String,
    },

    /// A column the resolved layout has no substitute for is null on some row.
    ///
    /// `ts`, `symbol` and the layout's price columns are `REQUIRED` on every measured artifact, but a
    /// writer change would make them nullable without altering anything else the decoder keys on, so
    /// the schema check accepts `OPTIONAL` and this reports a value that is actually missing. None of
    /// them is substitutable: a null timestamp has no place on a timeline, a null symbol cannot be
    /// checked against the descriptor — the check that catches a mis-described file — and a null
    /// price would have to be invented.
    #[error("export column {column:?} is null on a row that has no substitute for it")]
    NullValue { column: &'static str },

    /// A row's `symbol` column does not match the symbol the export descriptor names.
    ///
    /// Every export is single-symbol, so this means the file and its descriptor disagree — the
    /// artifact is not the one the caller thinks it is. Caught because attributing it to the
    /// descriptor's instrument would be silent misattribution, and because `BP` and `BP.L` are
    /// different instruments quoted in different currencies.
    #[error("export symbol mismatch: descriptor says {expected:?} but a row carries {found:?}")]
    SymbolMismatch { expected: String, found: String },

    /// An artifact's timestamps do not advance as its layout requires.
    ///
    /// A backtest fed an unsorted stream produces a non-monotonic clock and wrong results in
    /// release, with no failure point, so this is rejected at decode.
    ///
    /// # What counts as a violation depends on the layout
    /// - **Tick and quote** tapes are *non-decreasing*: several prints routinely share a
    ///   microsecond, so ties are permitted and only a step backwards is rejected.
    /// - **Candle** artifacts at a fixed resolution are *strictly ascending*: two bars of one
    ///   series cannot share an open, so they cannot share the close derived from it. A tie there
    ///   means the file holds more than one series, or repeats a row. It does **not** mean the
    ///   declared resolution is wrong — a fixed interval is a constant shift from open to close, so
    ///   it cancels out of this comparison entirely and a mis-declared one produces distinct,
    ///   strictly ascending closes. That is a separate check on bar *spacing*, which reports
    ///   [`UnexpectedCandleResolution`](Self::UnexpectedCandleResolution). Calendar intervals are
    ///   exempt from strictness: month arithmetic clamps day-of-month, so two distinct opens can
    ///   legitimately share a close.
    #[error("export timestamps are not ascending: {found} follows {previous}")]
    NonMonotonicTimestamps {
        previous: DateTime<Utc>,
        found: DateTime<Utc>,
    },

    /// A row's timestamp is outside the representable [`DateTime<Utc>`] range.
    #[error("export timestamp {micros}µs is not representable")]
    TimestampNotRepresentable { micros: i64 },

    /// A provider `f64` has no [`rust_decimal::Decimal`] representation.
    ///
    /// Surfaced rather than substituted: a zero or a clamp here would put a real-looking price
    /// into fees, PnL and risk notional.
    #[error("price {value} is not representable as a decimal: {message}")]
    PriceNotRepresentable { value: f64, message: String },

    /// No registered instrument on this exchange carries the requested display symbol.
    ///
    /// Raised when deriving an [`InstrumentIndex`] from the caller's registry rather than
    /// accepting one. That derivation is what makes a fabricated index unrepresentable — the index
    /// is a public, unbounded `usize` and engine state indexes positionally — and it is the only
    /// check that catches a symbol typo, which would otherwise leave one instrument silently
    /// receiving no data at all.
    ///
    /// [`InstrumentIndex`]: rustrade_instrument::instrument::InstrumentIndex
    #[error(
        "no instrument registered on {exchange} with exchange name {symbol:?}; registered there: [{registered}]"
    )]
    UnknownInstrument {
        symbol: String,
        exchange: rustrade_instrument::exchange::ExchangeId,
        registered: String,
    },

    /// The registered instrument prices this symbol in a different asset than the provider quotes
    /// it in.
    ///
    /// The provider publishes no unit alongside a price, so the asset the caller registered *is*
    /// the unit. Registering a `.L` listing against `gbp` rather than `gbx` is therefore not a
    /// naming preference: `BP.L` prints ~548 where BP trades around £5.48, so every notional, fee,
    /// unrealised PnL and balance derived from it is 100× wrong, and nothing downstream can tell.
    ///
    /// Raised at the same boundary as [`UnknownInstrument`](Self::UnknownInstrument), and for the
    /// same reason: it is the one place the provider's view of a symbol and the caller's registry
    /// meet, so it is the only place the disagreement is visible.
    ///
    /// Compared on [`AssetNameInternal`], because that is the identity the engine keys assets by —
    /// `GBX` and `gbx` are one asset, and `GBP` is a different one.
    ///
    /// # `expected` is derived, not published
    /// It comes from [`quote_asset`](super::market::quote_asset), which maps a venue suffix to an
    /// asset and **defaults to USD** for any suffix outside its table. So a correctly-registered
    /// listing on a venue that table does not cover is rejected here, with an `expected` the
    /// provider never stated. The message says "this integration derives" rather than "the provider
    /// quotes" for that reason. A caller in that position wants
    /// [`LseCandleSource::new`](super::backtest::LseCandleSource::new), which carries no check.
    ///
    /// [`AssetNameInternal`]: rustrade_instrument::asset::name::AssetNameInternal
    #[error(
        "instrument {symbol:?} on {exchange} is registered with quote asset {registered:?}, but \
         this integration derives {expected:?} for that symbol from its venue suffix (defaulting \
         to USD); prices carry no unit, so a mismatch misprices every notional, fee and balance"
    )]
    QuoteAssetMismatch {
        symbol: String,
        exchange: rustrade_instrument::exchange::ExchangeId,
        expected: String,
        registered: String,
    },

    /// The Parquet artifact could not be read.
    #[cfg(feature = "lse-parquet")]
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// A coarse classification of [`LseError`], for deciding what to do next.
///
/// [`LseError`] is `#[non_exhaustive]` and has a variant per measured provider behaviour, which is
/// the right shape for diagnosing one failure and the wrong shape for *reacting* to one: a consumer
/// writing retry logic wants five or six categories, not thirty, and should not have to revisit its
/// match every time an endpoint gains a variant. This is that projection, and it is what
/// [`DataError::Lse`](crate::error::DataError::Lse) carries so the classification survives being
/// flattened into a `String`.
///
/// Mirrors `DatabentoErrorKind` — not linked, since that lives behind the `databento` feature and
/// this type does not — plus the categories this integration's file and export surface adds.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum LseErrorKind {
    /// API key missing, malformed, or rejected. Retrying changes nothing until it is fixed.
    Authentication,
    /// Throttled, or a metered allowance is spent. Resumable once it replenishes — see
    /// [`LseError::RateLimited`]'s `retry_after` and [`LseError::QuotaExceeded`]'s status.
    RateLimit,
    /// Transport-level failure. Resumable; a retry may well succeed.
    Network,
    /// An export job did not reach a terminal state within the caller's timeout. Resumable: the
    /// job keeps building and its identifier stays valid, so polling again spends no allowance.
    Timeout,
    /// The provider rejected the request or the job failed. Not resumable by retrying alone.
    Api,
    /// The bytes could not be read as what they claim to be — a malformed body, an unsupported or
    /// mistyped Parquet schema, a value with no representation, an ordering violation, an
    /// integrity mismatch. Terminal: the data is what it is.
    Decode,
    /// The caller's request or instrument registry is wrong. Terminal until the caller changes it.
    InvalidInput,
    /// A local filesystem failure.
    Io,
}

impl LseErrorKind {
    /// Whether retrying the same operation could succeed without the caller changing anything.
    ///
    /// [`RateLimit`](Self::RateLimit) and [`Timeout`](Self::Timeout) require *waiting* first; this
    /// says the attempt is worth making again, not that it is worth making immediately.
    #[must_use]
    pub fn is_resumable(&self) -> bool {
        matches!(self, Self::RateLimit | Self::Network | Self::Timeout)
    }
}

impl std::fmt::Display for LseErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Authentication => "authentication",
            Self::RateLimit => "rate limit",
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::Api => "API",
            Self::Decode => "decode",
            Self::InvalidInput => "invalid input",
            Self::Io => "IO",
        };
        f.write_str(name)
    }
}

impl LseError {
    /// Classify this error for a consumer deciding whether to retry, wait, or stop.
    ///
    /// Exhaustive on purpose: a new variant must produce a compile error here rather than fall
    /// through to a default. Getting this wrong is not a visible failure — it silently retries
    /// something terminal, or abandons something that would have succeeded.
    #[must_use]
    pub fn kind(&self) -> LseErrorKind {
        match self {
            Self::EnvVar(_) | Self::InvalidCredential(_) => LseErrorKind::Authentication,
            Self::RateLimited { .. } | Self::QuotaExceeded { .. } => LseErrorKind::RateLimit,
            Self::Http(_) => LseErrorKind::Network,
            Self::ExportTimeout { .. } => LseErrorKind::Timeout,
            Self::Api { .. } | Self::ExportFailed { .. } => LseErrorKind::Api,
            Self::Io { .. } => LseErrorKind::Io,

            // The caller's request or registry, not the provider's answer.
            Self::InvalidInput { .. }
            | Self::UnsupportedInterval { .. }
            | Self::UnknownDataset(_)
            | Self::AmbiguousSlug { .. }
            | Self::UnknownInstrument { .. }
            | Self::QuoteAssetMismatch { .. } => LseErrorKind::InvalidInput,

            // Everything the provider sent that could not be read as what it claims to be. An
            // integrity or job mismatch belongs here rather than under `Api`: the request was
            // accepted and answered, and it is the payload that does not hold up.
            Self::Deserialize { .. }
            | Self::TimestampOverflow { .. }
            | Self::CursorOverflow { .. }
            | Self::UnexpectedCandleRange { .. }
            | Self::NonMonotonicCandlePage { .. }
            | Self::UnexpectedCandleResolution { .. }
            | Self::IntegrityMismatch { .. }
            | Self::ExportJobMismatch { .. }
            | Self::UnsupportedSchema { .. }
            | Self::UnsupportedColumnType { .. }
            | Self::NullValue { .. }
            | Self::SymbolMismatch { .. }
            | Self::NonMonotonicTimestamps { .. }
            | Self::TimestampNotRepresentable { .. }
            | Self::PriceNotRepresentable { .. } => LseErrorKind::Decode,

            #[cfg(feature = "lse-parquet")]
            Self::Parquet(_) => LseErrorKind::Decode,
        }
    }
}

/// Maximum retained length of a provider diagnostic, in bytes.
///
/// Well above any real message, and far below a pathological proxy error page.
const MAX_DETAIL_BYTES: usize = 2 * 1024;

/// Extract the provider's human-readable diagnostic from an error response body.
///
/// # Why this is not `body["detail"].as_str()`
///
/// The provider encodes errors inconsistently, and both forms are measured:
///
/// ```text
/// 400/404: {"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, ...\"}"}
/// 401:     {"detail":"invalid api key"}
/// ```
///
/// On `400`/`404` the value of `detail` is *itself* a JSON document encoded as a string, so
/// reading one level surfaces raw JSON in a user-facing error message. This unwraps repeatedly
/// until `detail` stops being re-encoded, and falls back to the whole body when the response is
/// not in either shape (a proxy or CDN page, say). The result is always truncated.
pub(crate) fn extract_detail(body: &str) -> String {
    let mut current = body.trim().to_owned();

    // Bounded rather than `loop`: two levels are measured, and a handful of iterations is ample for
    // any further nesting the provider might add without risking a pathological input spinning here.
    for _ in 0..4 {
        let Ok(serde_json::Value::Object(object)) =
            serde_json::from_str::<serde_json::Value>(&current)
        else {
            break;
        };
        let Some(serde_json::Value::String(detail)) = object.get("detail") else {
            break;
        };
        current = detail.trim().to_owned();
    }

    crate::exchange::http::truncate_str(&current, MAX_DETAIL_BYTES)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn single_encoded_detail_is_read_directly() {
        // The measured 401 shape.
        assert_eq!(
            extract_detail(r#"{"detail":"invalid api key"}"#),
            "invalid api key"
        );
    }

    #[test]
    fn double_encoded_detail_is_unwrapped_to_the_message() {
        // The measured 400 shape: `detail` is a JSON document encoded as a string. Reading one
        // level would put raw JSON in front of the user.
        let body = r#"{"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, 15s\"}"}"#;

        assert_eq!(
            extract_detail(body),
            "invalid timeframe '7q'; valid: 1s, 5s, 15s"
        );
    }

    #[test]
    fn double_encoded_not_found_is_unwrapped() {
        // The measured 404 shape.
        let body =
            r#"{"detail":"{\"detail\":\"'NOPE_XYZ' has no candle data; browse /catalog\"}"}"#;

        assert_eq!(
            extract_detail(body),
            "'NOPE_XYZ' has no candle data; browse /catalog"
        );
    }

    #[test]
    fn a_non_json_body_is_returned_as_is() {
        // A proxy or CDN error page is not in either shape, and is still the best diagnostic there is.
        assert_eq!(
            extract_detail("<html>502 Bad Gateway</html>"),
            "<html>502 Bad Gateway</html>"
        );
    }

    #[test]
    fn json_without_a_detail_field_is_returned_whole() {
        assert_eq!(extract_detail(r#"{"error":"nope"}"#), r#"{"error":"nope"}"#);
    }

    #[test]
    fn a_non_string_detail_is_not_unwrapped() {
        // Only a re-encoded *string* is a nesting level; an object is already the message.
        assert_eq!(
            extract_detail(r#"{"detail":{"code":7}}"#),
            r#"{"detail":{"code":7}}"#
        );
    }

    #[test]
    fn an_oversized_diagnostic_is_truncated() {
        let body = format!(r#"{{"detail":"{}"}}"#, "a".repeat(MAX_DETAIL_BYTES * 3));

        assert_eq!(extract_detail(&body).len(), MAX_DETAIL_BYTES);
    }

    #[test]
    fn nesting_deeper_than_the_budget_stops_exactly_at_the_budget() {
        // Six wrappers against a budget of four: it must stop mid-way rather than spin, and the
        // assertion pins *where* it stopped. Asserting only that the result is non-empty would
        // hold for any budget at all, including an off-by-one one.
        let wrap =
            |inner: &str| serde_json::to_string(&serde_json::json!({ "detail": inner })).unwrap();

        let mut body = r#"{"detail":"bottom"}"#.to_owned();
        for _ in 0..6 {
            body = wrap(&body);
        }

        // Four unwraps consume four of the six wrappers, leaving the two-deep remainder — still
        // encoded JSON, because the budget ran out before the message did.
        let mut expected = r#"{"detail":"bottom"}"#.to_owned();
        for _ in 0..2 {
            expected = wrap(&expected);
        }

        assert_eq!(extract_detail(&body), expected);
    }

    #[test]
    fn an_empty_body_yields_an_empty_diagnostic() {
        assert_eq!(extract_detail(""), "");
    }

    /// The classification a consumer actually branches on.
    ///
    /// `kind()` is exhaustive, so a *new* variant is a compile error — but nothing in the compiler
    /// checks that an existing variant is classified *correctly*, and getting that wrong is
    /// invisible: it silently retries something terminal, or abandons something that would have
    /// succeeded on the next attempt. These pin the pairs where that costs something.
    #[test]
    fn the_resumable_kinds_are_the_ones_a_retry_could_actually_fix() {
        // The whole point of carrying `kind` through `DataError::Lse`: a consumer must be able to
        // tell "wait and try again" from "stop" without substring-matching a `Display`.
        assert!(LseErrorKind::RateLimit.is_resumable());
        assert!(LseErrorKind::Network.is_resumable());
        assert!(LseErrorKind::Timeout.is_resumable());

        assert!(!LseErrorKind::Authentication.is_resumable());
        assert!(!LseErrorKind::Api.is_resumable());
        assert!(!LseErrorKind::Decode.is_resumable());
        assert!(!LseErrorKind::InvalidInput.is_resumable());
        assert!(!LseErrorKind::Io.is_resumable());
    }

    #[test]
    fn a_rate_limit_classifies_as_resumable_and_a_decode_failure_does_not() {
        // The pair the whole projection exists for, checked end to end from the variant rather
        // than from the kind: `RateLimited` is documented resumable, `Deserialize` is terminal,
        // and a consumer that confused them would either spin forever on a malformed body or
        // abandon a fetch that a 60-second wait would have completed.
        let rate_limited = LseError::RateLimited {
            retry_after: Some(Duration::from_secs(60)),
        };
        assert_eq!(rate_limited.kind(), LseErrorKind::RateLimit);
        assert!(rate_limited.kind().is_resumable());

        let decode = LseError::Deserialize {
            message: "unexpected end of input".to_owned(),
        };
        assert_eq!(decode.kind(), LseErrorKind::Decode);
        assert!(!decode.kind().is_resumable());
    }

    #[test]
    fn an_exhausted_allowance_is_a_rate_limit_and_an_export_timeout_is_a_timeout() {
        // Both are export-path variants that a naive reading would file under `Api` -- they arrive
        // as a 429 and as a job that never finished. Classifying either as terminal would stop a
        // consumer that only had to wait.
        assert_eq!(
            LseError::QuotaExceeded { status: None }.kind(),
            LseErrorKind::RateLimit
        );
        assert_eq!(
            LseError::ExportTimeout {
                job_id: "job-1".to_owned(),
                status: "running".to_owned(),
            }
            .kind(),
            LseErrorKind::Timeout
        );
    }

    #[test]
    fn a_payload_that_was_accepted_but_does_not_hold_up_is_a_decode_failure_not_an_api_error() {
        // The boundary that is easiest to get wrong: the request succeeded and the provider
        // answered, so `status` is a success code -- but the bytes are not what they claim to be.
        // Filing these under `Api` would tell a consumer to fix its request, which cannot help.
        let decode_variants = [
            LseError::IntegrityMismatch {
                path: std::path::PathBuf::from("/tmp/export.parquet.part"),
                expected: "abc".to_owned(),
                actual: "def".to_owned(),
                discarded: true,
            },
            LseError::ExportJobMismatch {
                job_id: "job-1".to_owned(),
                field: "symbol".to_owned(),
                requested: "BP.L".to_owned(),
                reported: "BP".to_owned(),
            },
            LseError::NonMonotonicTimestamps {
                previous: DateTime::UNIX_EPOCH,
                found: DateTime::UNIX_EPOCH,
            },
            LseError::SymbolMismatch {
                expected: "BP.L".to_owned(),
                found: "BP".to_owned(),
            },
        ];

        for error in decode_variants {
            assert_eq!(error.kind(), LseErrorKind::Decode, "{error}");
        }

        // The contrast: an error the provider itself reported about the request.
        assert_eq!(
            LseError::Api {
                status: 404,
                message: "no candle data".to_owned(),
            }
            .kind(),
            LseErrorKind::Api
        );
    }

    #[test]
    fn a_caller_side_mistake_is_invalid_input_rather_than_an_api_error() {
        // These are all detected before or independently of the request, so reporting them as the
        // provider's fault would send the caller looking in the wrong place.
        let caller_variants = [
            LseError::UnknownDataset("nope".to_owned()),
            LseError::UnsupportedInterval {
                interval: CandleInterval::Sec1,
            },
            LseError::QuoteAssetMismatch {
                symbol: "BP.L".to_owned(),
                exchange: rustrade_instrument::exchange::ExchangeId::Other,
                expected: "gbx".to_owned(),
                registered: "gbp".to_owned(),
            },
        ];

        for error in caller_variants {
            assert_eq!(error.kind(), LseErrorKind::InvalidInput, "{error}");
        }

        // A credential problem is its own category: also terminal, but the fix is a key rather
        // than a request.
        assert_eq!(
            LseError::EnvVar("LSE_API_KEY is not set".to_owned()).kind(),
            LseErrorKind::Authentication
        );
    }

    #[test]
    fn the_kind_survives_flattening_into_the_crate_error() {
        // The reason `DataError::Lse` is a struct variant at all. Once an LSE failure is flattened
        // for the generic stream helpers, `message` is a `String` and the classification would be
        // unrecoverable from it -- so `kind` has to be carried alongside, not derived later.
        let error = crate::error::DataError::from(LseError::RateLimited { retry_after: None });

        let crate::error::DataError::Lse { kind, message } = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        assert_eq!(*kind, LseErrorKind::RateLimit);
        assert!(kind.is_resumable());
        // The message is still the full diagnostic; `kind` is an addition to it, not a
        // replacement.
        assert!(message.contains("rate limited"), "{message}");
    }
}
