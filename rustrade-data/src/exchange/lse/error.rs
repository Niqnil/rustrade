use crate::subscription::candle::CandleInterval;
use chrono::{DateTime, Utc};
use smol_str::SmolStr;
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

    /// A market folder name this integration does not know, such as one the provider added since.
    #[error("unknown LSE market {0:?}")]
    UnknownMarket(String),

    /// A required environment variable is not set, or does not hold valid UTF-8 (see
    /// [`LseClient::from_env`]).
    ///
    /// The message names the variable and never its value, so a mis-encoded key cannot reach a log
    /// line through this error.
    ///
    /// [`LseClient::from_env`]: super::client::LseClient::from_env
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
    /// `code` is the provider's machine-readable refusal code where it sent one (`symbol-unknown`,
    /// `plan-required`, `history-limit`, `rows-exhausted`, `export-too-wide`, `date-missing`, ...):
    /// match on it rather than on `message`, whose wording the provider changes. `message` is the
    /// provider's own sentence, truncated to a bounded length.
    #[error("API error ({status}{}): {message}", match .code {
        Some(code) => format!(" {code}"),
        None => String::new(),
    })]
    Api {
        status: u16,
        code: Option<SmolStr>,
        message: String,
    },

    /// The provider answered `429`: a limit of the key's plan is reached.
    ///
    /// Terminal — this integration never sleeps and retries on the caller's behalf. A chunked
    /// fetch yields this and **ends**; resume by re-requesting from the last `close_time` received.
    ///
    /// `code` says which limit, where the provider named it: `calls-exhausted` (calls per minute),
    /// `connections-exhausted` or `exports-busy` (one download at a time per key), `rows-exhausted`
    /// (the plan's rows per day) or `allowance-exhausted` (bytes per week). `retry_after` is the
    /// provider's `Retry-After`, where it sent one in seconds. The figures are in
    /// [`usage`](super::client::LseClient::usage).
    #[error("rate limited{}{}", match .code {
        Some(code) => format!(" ({code})"),
        None => String::new(),
    }, match .retry_after {
        Some(delay) => format!("; retry after {}s", delay.as_secs()),
        None => String::new(),
    })]
    RateLimited {
        retry_after: Option<Duration>,
        code: Option<SmolStr>,
    },

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

    /// The provider served a candle opening outside the window this integration asked it for.
    ///
    /// Each chunk of a candle fetch asks for a window whose bounds sit on the interval's grid, and
    /// the provider aggregates exactly that window, so every bar it returns opens inside it. One
    /// that does not means the window was not honoured, and the bar may be a fragment of a period
    /// cut by the window's edge. Terminal rather than trimmed away.
    #[error(
        "a candle for {symbol:?} opens at {open}, outside the window [{window_start}, \
         {window_end}) it was asked for: the range parameters appear to have been ignored"
    )]
    UnexpectedCandleRange {
        symbol: String,
        open: DateTime<Utc>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    },

    /// Candles stepped **backwards**, or repeated a period, within a fetch.
    ///
    /// Compared on the bar's open for an intraday interval and on its session date for a daily,
    /// weekly or monthly one, across the chunks of one fetch as well as within each. Terminal
    /// rather than repaired by sorting: an order the chunking was not built for means the provider
    /// changed behaviour. The same property is a typed rejection on the Parquet path
    /// ([`NonMonotonicTimestamps`](Self::NonMonotonicTimestamps)).
    #[error(
        "candles for {symbol:?} are not ascending: a bar opening {open} follows one opening {previous_open}"
    )]
    NonMonotonicCandles {
        symbol: String,
        previous_open: DateTime<Utc>,
        open: DateTime<Utc>,
    },

    /// Two consecutive candles are spaced closer together than the interval they were asked for.
    ///
    /// Impossible for a compliant fixed-interval series: consecutive bars are exactly one interval
    /// apart, and a gap (a weekend, a holiday, a quiet symbol) only ever makes the spacing *wider*.
    /// Spacing narrower than the interval means the candles are at a **finer resolution than the
    /// one requested**, so every `close_time` derived for them overlaps the bars that follow.
    ///
    /// # The failure this exists to catch, on the REST path
    /// A provider that answered a resolution it does not recognise with its default of 1-minute
    /// bars, in the same response shape, would hand an `Hour1` request 60 rows an hour, each stamped
    /// with a `close_time` of `open + 1h` by the boundary arithmetic: overlapping, wrong, and
    /// invisible to every other check, because the bars ascend and fall inside the window. Spacing
    /// is the one property that distinguishes it. Checked for intraday intervals only: a daily or
    /// longer bar opens at its session's first trade, so the gap between two opens varies.
    ///
    /// # And on the export path
    /// `parquet::read_export` derives `close_time` from a **caller-supplied** interval, since
    /// nothing in a Parquet artifact records the resolution it was written at. The wrong resolution
    /// therefore reaches the same arithmetic from a different origin — the caller's declaration
    /// rather than the provider's answer — and produces the same overlapping bars, so the same
    /// check guards it: a file of 1-minute bars decoded as `Hour1` is rejected rather than yielding
    /// 60 overlapping "hourly" bars an hour. (Decoded as `Day1` it fails sooner: a daily file
    /// carries a `date` column that an intraday one lacks.) Reported in terms of the file's `time`
    /// column, which carries the bar's open — the close is a value the decoder derived, and only
    /// the open is something a caller can find in the file itself.
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

    /// An option print came back outside the window or underlying it was requested for.
    ///
    /// Means the provider ignored a request parameter (the window, the underlying or the order),
    /// or sent a whole page of prints at one instant, which paging by time cannot get past. Raised
    /// before any print of the offending page is yielded.
    #[error("option print of {ticker} does not match its request: {message}")]
    UnexpectedOptionFlowRow { ticker: SmolStr, message: String },

    /// A downloaded file's body does not hash to the SHA-256 the provider declared for it.
    ///
    /// Nothing from it is returned or left on disk: a download's `.part` file is removed and the
    /// destination is untouched. The provider ignores range requests, so calling again downloads
    /// the whole file again.
    #[error("downloaded file failed its checksum: declared SHA-256 {expected}, received {actual}")]
    IntegrityMismatch { expected: String, actual: String },

    /// A file answer carries no usable SHA-256 header, so its body cannot be verified.
    ///
    /// Refused rather than accepted unverified: a cut or damaged transfer would otherwise become
    /// data that looks whole.
    #[error(
        "the provider's file answer carries no usable SHA-256 header, so it cannot be verified"
    )]
    MissingChecksum,

    /// A download was asked not to replace a file, and one is already at its destination.
    ///
    /// Checked before the request is sent, so it spends none of the plan's allowance. See
    /// [`LseExistingFile`](super::export::LseExistingFile).
    #[error("{} already exists and the download was asked not to replace it", .path.display())]
    DestinationExists { path: std::path::PathBuf },

    /// A file answer describes something other than what was asked for.
    ///
    /// The provider echoes some of the selection in its answer's headers (whether prices are
    /// adjusted for splits, say); one that disagrees with the request means the file holds other
    /// data than the caller thinks, so it is refused rather than returned.
    #[error("the file answer reports {field} = {reported:?}, but {requested:?} was asked for")]
    ExportMismatch {
        field: &'static str,
        requested: String,
        reported: String,
    },

    /// The Parquet decoder does not decode this kind of export into market events.
    #[error("exports of {data} are not decoded into market events")]
    UnsupportedExportData { data: String },

    /// A trade-slice row carries a `kind` other than `trade`.
    ///
    /// Every row of every trade slice measured is a trade. Another kind (a quote, a correction)
    /// would not be a [`PublicTrade`](crate::subscription::trade::PublicTrade), so it is refused
    /// rather than decoded as one or skipped.
    #[error("trade-slice row has kind {kind:?}, but only \"trade\" rows are decoded")]
    UnexpectedTradeKind { kind: String },

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

    /// A file lacks a column the kind of export it was decoded as must carry.
    ///
    /// The decoder is told what a file holds (candles or a trade slice) and reads that kind's
    /// columns by name. A missing one means the file is another kind, or the provider changed its
    /// layout; reported rather than guessed at, since a wrong guess yields plausible numbers in the
    /// wrong field.
    ///
    /// Also raised for a schema that is not **flat**. Columns are read by leaf index, which equals a
    /// top-level field's position only when every field is a primitive: one nested group contributes
    /// several leaves and shifts every index after it. Every measured artifact is flat, so rather
    /// than support a nesting the provider has never emitted, it is rejected — the alternative is a
    /// decoder whose column mapping is silently wrong on a file it accepted, which for `open` and
    /// `close` means reading one as the other.
    #[error("unsupported export schema; columns were: {columns}")]
    UnsupportedSchema { columns: String },

    /// A recognised export column does not have the type this integration decodes it as.
    ///
    /// Distinct from [`UnsupportedSchema`](Self::UnsupportedSchema), which reports columns whose
    /// a file lacks. Here the column is present and has the wrong type, so naming the column and
    /// both types is the whole diagnostic — reported up front rather than as an opaque decode
    /// failure on the first row.
    ///
    /// # Why a `time` column that is not UTC-adjusted lands here
    /// `Timestamp { unit: MILLIS, is_adjusted_to_utc: false }` is *physically identical* to the
    /// UTC-adjusted form — same `INT64`, same legacy `TIMESTAMP_MILLIS` converted type — and
    /// differs only in the origin its values are measured against. Read as epoch milliseconds, a
    /// local-time column shifts every event by the venue's UTC offset with nothing downstream able
    /// to notice: the timeline is still monotonic and the prices are still right, so a backtest
    /// simply trades on data it could not have had. This check is the only place that difference
    /// is visible.
    #[error("export column {column:?} is {found}, but this integration requires {required}")]
    UnsupportedColumnType {
        column: String,
        required: &'static str,
        found: String,
    },

    /// A column that has no substitute is null on some row.
    ///
    /// A file's timestamp and price columns are `REQUIRED` in almost every measured file, but
    /// option candles already declare their prices `OPTIONAL`, and a writer change could do the
    /// same elsewhere without altering anything else the decoder keys on. The schema check
    /// therefore accepts `OPTIONAL` and this reports a value that is actually missing. None of them
    /// is substitutable: a null timestamp has no place on a timeline, and a null price would have
    /// to be invented.
    #[error("export column {column:?} is null on a row that has no substitute for it")]
    NullValue { column: &'static str },

    /// An artifact's timestamps do not advance as its layout requires.
    ///
    /// A backtest fed an unsorted stream produces a non-monotonic clock and wrong results in
    /// release, with no failure point, so this is rejected at decode.
    ///
    /// # What counts as a violation depends on the layout
    /// - **Trade slices** are *non-decreasing*: several prints routinely share a nanosecond, so
    ///   ties are permitted and only a step backwards is rejected.
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
    #[error("export timestamp {value} ({unit} since the Unix epoch) is not representable")]
    TimestampNotRepresentable { value: i64, unit: &'static str },

    /// A provider `f64` has no [`rust_decimal::Decimal`] representation.
    ///
    /// Surfaced rather than substituted: a zero or a clamp here would put a real-looking price
    /// into fees, PnL and risk notional.
    #[error("price {value} is not representable as a decimal: {message}")]
    PriceNotRepresentable { value: f64, message: String },

    /// No registered instrument is priced on this exchange under the requested display symbol:
    /// none has it as its data-side name there (its `DataVenue`'s, or its own when it declares
    /// none).
    ///
    /// Raised when deriving an [`InstrumentIndex`] from the caller's registry rather than
    /// accepting one. That derivation is what makes a fabricated index unrepresentable — the index
    /// is a public, unbounded `usize` and engine state indexes positionally — and it is the only
    /// check that catches a symbol typo, which would otherwise leave one instrument silently
    /// receiving no data at all.
    ///
    /// [`InstrumentIndex`]: rustrade_instrument::instrument::InstrumentIndex
    #[error(
        "no registered instrument is priced on {exchange} under {symbol:?}; symbols priced there: [{registered}]"
    )]
    UnknownInstrument {
        symbol: String,
        exchange: rustrade_instrument::exchange::ExchangeId,
        registered: String,
    },

    /// More than one registered instrument is priced on this exchange under the requested display
    /// symbol, such as one listing executed on two venues with both priced here.
    ///
    /// A candle source delivers its symbol's prices to a single instrument, so picking one would
    /// leave the other silently unpriced. `instruments` lists the `name_internal` of each.
    #[error(
        "{symbol:?} on {exchange} prices more than one registered instrument: [{instruments}]; a candle source feeds one"
    )]
    AmbiguousInstrument {
        symbol: String,
        exchange: rustrade_instrument::exchange::ExchangeId,
        instruments: String,
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
    /// [`LseError::RateLimited`]'s `retry_after`.
    RateLimit,
    /// Transport-level failure, including a downloaded file cut short or damaged in transit (it
    /// fails its length or checksum check). Resumable; a retry may well succeed.
    Network,
    /// The provider rejected the request. Not resumable by retrying alone.
    Api,
    /// The bytes could not be read as what they claim to be — a malformed body, an unsupported or
    /// mistyped Parquet schema, a value with no representation, an ordering violation. Terminal:
    /// the data is what it is.
    Decode,
    /// The caller's request or instrument registry is wrong. Terminal until the caller changes it.
    InvalidInput,
    /// A local filesystem failure.
    Io,
}

impl LseErrorKind {
    /// Whether retrying the same operation could succeed without the caller changing anything.
    ///
    /// [`RateLimit`](Self::RateLimit) requires *waiting* first; this says the attempt is worth
    /// making again, not that it is worth making immediately.
    #[must_use]
    pub fn is_resumable(&self) -> bool {
        matches!(self, Self::RateLimit | Self::Network)
    }
}

impl std::fmt::Display for LseErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Authentication => "authentication",
            Self::RateLimit => "rate limit",
            Self::Network => "network",
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
            Self::RateLimited { .. } => LseErrorKind::RateLimit,
            // A file cut short or damaged in transit: nothing from it was kept, and calling again
            // downloads it afresh.
            Self::Http(_) | Self::IntegrityMismatch { .. } => LseErrorKind::Network,
            // The request was valid and the provider cannot serve it whole; retrying changes nothing.
            Self::Api { .. } => LseErrorKind::Api,
            Self::Io { .. } => LseErrorKind::Io,

            // The caller's request or registry, not the provider's answer.
            Self::InvalidInput { .. }
            | Self::UnsupportedInterval { .. }
            | Self::UnknownDataset(_)
            | Self::UnknownMarket(_)
            | Self::AmbiguousSlug { .. }
            | Self::UnknownInstrument { .. }
            | Self::AmbiguousInstrument { .. }
            | Self::QuoteAssetMismatch { .. }
            | Self::DestinationExists { .. }
            | Self::UnsupportedExportData { .. } => LseErrorKind::InvalidInput,

            // Everything the provider sent that could not be read as what it claims to be. A file
            // describing another selection belongs here rather than under `Api`: the request was
            // accepted and answered, and it is the payload that does not hold up.
            Self::Deserialize { .. }
            | Self::TimestampOverflow { .. }
            | Self::UnexpectedCandleRange { .. }
            | Self::NonMonotonicCandles { .. }
            | Self::UnexpectedCandleResolution { .. }
            | Self::UnexpectedOptionFlowRow { .. }
            | Self::MissingChecksum
            | Self::ExportMismatch { .. }
            | Self::UnexpectedTradeKind { .. }
            | Self::UnsupportedSchema { .. }
            | Self::UnsupportedColumnType { .. }
            | Self::NullValue { .. }
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

/// Extract the provider's refusal code and sentence from an error response body.
///
/// # Shapes read
///
/// LSE Data 3.0 answers every refusal as one JSON object, measured on `/v3`:
///
/// ```text
/// 404: {"code": "symbol-unknown", "message": "symbol is not available"}
/// ```
///
/// Some refusals add figures (`required_plan`, `retry_after`, `earliest`, ...); those are left out
/// here, and the sentence names what matters. The pre-3.0 hosts answer with a `detail` that may be
/// a JSON document encoded as a string, one level inside another:
///
/// ```text
/// 410:     {"detail": "Look at the new documentation: ...", "status": 410}
/// 400/404: {"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, ...\"}"}
/// ```
///
/// so `detail` is unwrapped repeatedly until it stops being re-encoded. A body in neither shape (a
/// proxy or CDN page, say) is returned whole as the sentence, with no code. The sentence is always
/// truncated.
pub(crate) fn extract_refusal(body: &str) -> (Option<SmolStr>, String) {
    let mut current = body.trim().to_owned();
    let mut code = None;

    // Bounded rather than `loop`: two levels are measured, and a handful of iterations is ample for
    // any further nesting the provider might add without risking a pathological input spinning here.
    for _ in 0..4 {
        let Ok(serde_json::Value::Object(object)) =
            serde_json::from_str::<serde_json::Value>(&current)
        else {
            break;
        };
        if let Some(serde_json::Value::String(found)) = object.get("code") {
            code = Some(SmolStr::new(found.trim()));
        }
        let sentence = match (object.get("message"), object.get("detail")) {
            (Some(serde_json::Value::String(message)), _) => message,
            (_, Some(serde_json::Value::String(detail))) => detail,
            _ => break,
        };
        current = sentence.trim().to_owned();
    }

    (
        code.filter(|code| !code.is_empty()),
        crate::exchange::http::truncate_str(&current, MAX_DETAIL_BYTES),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn an_error_kind_displays_as_its_name() {
        assert_eq!(LseErrorKind::Network.to_string(), "network");
        assert_eq!(LseErrorKind::InvalidInput.to_string(), "invalid input");
    }

    /// The sentence alone, for the shapes that carry no code.
    fn detail(body: &str) -> String {
        extract_refusal(body).1
    }

    #[test]
    fn a_v3_refusal_yields_its_code_and_sentence() {
        // The measured `/v3` shape.
        assert_eq!(
            extract_refusal(r#"{"code": "symbol-unknown", "message": "symbol is not available"}"#),
            (
                Some(SmolStr::new("symbol-unknown")),
                "symbol is not available".to_owned()
            )
        );
    }

    #[test]
    fn a_v3_refusal_with_figures_keeps_its_code_and_sentence() {
        let body = r#"{"code": "plan-required", "message": "This market needs a plan.",
            "required_plan": {"name": "Nasdaq Lite"}}"#;

        assert_eq!(
            extract_refusal(body),
            (
                Some(SmolStr::new("plan-required")),
                "This market needs a plan.".to_owned()
            )
        );
    }

    #[test]
    fn a_retired_host_answer_carries_no_code() {
        // The measured 410 from the pre-3.0 data host: `status` is a number, not a code.
        assert_eq!(
            extract_refusal(r#"{"detail": "Look at the new documentation", "status": 410}"#),
            (None, "Look at the new documentation".to_owned())
        );
    }

    #[test]
    fn an_empty_or_non_string_code_is_no_code() {
        assert_eq!(extract_refusal(r#"{"code": "", "message": "m"}"#).0, None);
        assert_eq!(extract_refusal(r#"{"code": 7, "message": "m"}"#).0, None);
    }

    #[test]
    fn single_encoded_detail_is_read_directly() {
        // The measured 401 shape.
        assert_eq!(detail(r#"{"detail":"invalid api key"}"#), "invalid api key");
    }

    #[test]
    fn double_encoded_detail_is_unwrapped_to_the_message() {
        // The measured 400 shape: `detail` is a JSON document encoded as a string. Reading one
        // level would put raw JSON in front of the user.
        let body = r#"{"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, 15s\"}"}"#;

        assert_eq!(detail(body), "invalid timeframe '7q'; valid: 1s, 5s, 15s");
    }

    #[test]
    fn double_encoded_not_found_is_unwrapped() {
        // The measured 404 shape.
        let body =
            r#"{"detail":"{\"detail\":\"'NOPE_XYZ' has no candle data; browse /catalog\"}"}"#;

        assert_eq!(
            detail(body),
            "'NOPE_XYZ' has no candle data; browse /catalog"
        );
    }

    #[test]
    fn a_non_json_body_is_returned_as_is() {
        // A proxy or CDN error page is not in either shape, and is still the best diagnostic there is.
        assert_eq!(
            detail("<html>502 Bad Gateway</html>"),
            "<html>502 Bad Gateway</html>"
        );
    }

    #[test]
    fn json_without_a_detail_field_is_returned_whole() {
        assert_eq!(detail(r#"{"error":"nope"}"#), r#"{"error":"nope"}"#);
    }

    #[test]
    fn a_non_string_detail_is_not_unwrapped() {
        // Only a re-encoded *string* is a nesting level; an object is already the message.
        assert_eq!(
            detail(r#"{"detail":{"code":7}}"#),
            r#"{"detail":{"code":7}}"#
        );
    }

    #[test]
    fn an_oversized_diagnostic_is_truncated() {
        let body = format!(r#"{{"detail":"{}"}}"#, "a".repeat(MAX_DETAIL_BYTES * 3));

        assert_eq!(detail(&body).len(), MAX_DETAIL_BYTES);
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

        assert_eq!(detail(&body), expected);
    }

    #[test]
    fn an_empty_body_yields_an_empty_diagnostic() {
        assert_eq!(detail(""), "");
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
            code: None,
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
    fn a_file_damaged_in_transit_is_resumable() {
        // Nothing from such a file was kept, and the provider builds it afresh on the next call,
        // so filing these under `Decode` would stop a consumer that only had to call again.
        let error = LseError::IntegrityMismatch {
            expected: "abc".to_owned(),
            actual: "def".to_owned(),
        };
        assert_eq!(error.kind(), LseErrorKind::Network, "{error}");
        assert!(error.kind().is_resumable());
    }

    #[test]
    fn a_payload_that_was_accepted_but_does_not_hold_up_is_a_decode_failure_not_an_api_error() {
        // The boundary that is easiest to get wrong: the request succeeded and the provider
        // answered, so `status` is a success code -- but the bytes are not what they claim to be.
        // Filing these under `Api` would tell a consumer to fix its request, which cannot help.
        let decode_variants = [
            LseError::MissingChecksum,
            LseError::ExportMismatch {
                field: "adjusted",
                requested: "true".to_owned(),
                reported: "false".to_owned(),
            },
            LseError::UnexpectedTradeKind {
                kind: "quote".to_owned(),
            },
            LseError::NonMonotonicTimestamps {
                previous: DateTime::UNIX_EPOCH,
                found: DateTime::UNIX_EPOCH,
            },
        ];

        for error in decode_variants {
            assert_eq!(error.kind(), LseErrorKind::Decode, "{error}");
        }

        // The contrast: an error the provider itself reported about the request.
        assert_eq!(
            LseError::Api {
                status: 404,
                code: Some(SmolStr::new("symbol-unknown")),
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
            LseError::DestinationExists {
                path: std::path::PathBuf::from("/tmp/export.parquet"),
            },
            LseError::UnsupportedExportData {
                data: "greeks".to_owned(),
            },
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
        let error = crate::error::DataError::from(LseError::RateLimited {
            retry_after: None,
            code: None,
        });

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
