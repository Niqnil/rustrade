//! IBKR **Flex Web Service** corporate-action reconciliation.
//!
//! Fetches an account's *Activity* Flex statement over HTTPS and exposes its Corporate Actions
//! section as faithful [`IbkrFlexCorporateAction`] records. This is a **reconciliation / audit**
//! surface — broker-confirmed, account-scoped, and post-hoc — *not* a market-reference split source.
//! It does **not** derive split ratios; a caller that wants a ratio cross-references a dedicated
//! split source (and owns reconcile policy). See [`corporate_action`] for the record contract.
//!
//! # Transport (2-call Flex Web Service flow)
//!
//! The Flex Web Service generates statements asynchronously, so a fetch is two calls:
//!
//! 1. **SendRequest** — `GET {base}/SendRequest?t={token}&q={queryId}&v=3` returns a
//!    `<FlexStatementResponse>` with a `ReferenceCode` and a `Url` to poll (or a `Fail` status).
//! 2. **GetStatement** — `GET {Url}?t={token}&q={referenceCode}&v=3`, polled with a bounded number
//!    of attempts while the service replies `Warn`/`ErrorCode 1019` ("generation in progress"),
//!    until it returns the `<FlexQueryResponse>` statement (or a terminal error).
//!
//! Non-success statuses (e.g. `1003` invalid token, `1018` throttled, token-expired) surface as
//! [`IbkrFlexError::Flex`]; a non-success HTTP status whose body is *not* a recognizable Flex
//! envelope (e.g. a proxy/CDN error page) surfaces as [`IbkrFlexError::HttpStatus`]; exhausting the
//! poll budget surfaces as [`IbkrFlexError::PollTimeout`].
//!
//! This service uses an HTTPS token + saved-query id — it does **not** use IB Gateway / TWS, so it
//! is entirely independent of the socket [`IbkrStreamConfig`](crate::exchange::ibkr::IbkrStreamConfig).
//!
//! # Known limitations
//!
//! - **Post-hoc (T+1+).** A Flex statement reports actions *after* the broker books them; the
//!   records are for reconciliation/audit, not for injecting split events into a live/backtest
//!   timeline at the market execution instant.
//! - **Account-scoped.** Quantities are this account's deltas, not market-wide figures.
//! - **No library-derived ratio.** Flex carries no standardised split-ratio field; the wrapper
//!   derives/verifies ratios from a market-reference source.
//! - **Query-configuration-dependent date format.** The raw `dateTime` format depends on the saved
//!   query's settings (see [`IbkrFlexCorporateAction::date_time`]).
//! - **The saved Flex query must include the Corporate Actions section** over an account-activity
//!   scope, otherwise the statement contains no `<CorporateAction>` rows to return.
//!
//! # Credentials
//!
//! Construct from a [`IbkrFlexConfig`] (an Activity-statement Flex `token` + saved-query `query_id`).
//! [`IbkrFlexClient::from_env`] reads `IBKR_FLEX_TOKEN` and `IBKR_FLEX_QUERY_ID`.

mod corporate_action;

pub use corporate_action::{IbkrFlexCorporateAction, IbkrReorgType, parse_corporate_actions};

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped, truncate_str};
use quick_xml::{Reader, events::Event};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{env, fmt, time::Duration};
use thiserror::Error;
use tracing::debug;

/// Base URL of the IBKR Flex Web Service (`SendRequest` lives directly under it; the poll URL is
/// taken from the SendRequest response, not hard-coded).
const FLEX_BASE_URL: &str =
    "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService";

/// Flex Web Service protocol version (`v` query parameter).
const FLEX_VERSION: &str = "3";

/// Timeout for each individual HTTP request.
const REQUEST_TIMEOUT_SECS: u64 = 30;

/// Maximum number of GetStatement poll attempts before giving up.
const POLL_MAX_ATTEMPTS: u32 = 12;

/// Delay between GetStatement poll attempts while the statement is still generating.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Delay before the *first* GetStatement poll. The statement is generated asynchronously, so the
/// first poll almost always reports [`GENERATION_IN_PROGRESS_CODE`]; waiting once up front keeps
/// that near-certain miss from consuming one of the bounded poll attempts. Overridable via
/// [`FlexPollPolicy`].
const POLL_INITIAL_DELAY: Duration = Duration::from_secs(5);

/// Flex `ErrorCode` meaning "statement generation in progress, try again shortly" — the only
/// non-success status that should be retried rather than surfaced as an error.
const GENERATION_IN_PROGRESS_CODE: &str = "1019";

/// Maximum number of bytes of a non-Flex error body retained in [`IbkrFlexError::HttpStatus`]. Flex
/// and IBKR error envelopes are tiny; this only bounds an unexpectedly large proxy/CDN error page.
const MAX_ERROR_BODY_BYTES: usize = 1024;

// The redact-before-bound ordering in `sanitize_error_body` is only safe while the *gap* between the
// download and retained caps exceeds the token length. A token straddling the coarse
// `MAX_ERROR_BODY_DOWNLOAD_BYTES` pre-bound survives redaction as an unmatched prefix fragment pinned
// to the tail of the pre-bounded slice: it ends at the download cap, so a fragment of length `f`
// starts at `MAX_ERROR_BODY_DOWNLOAD_BYTES - f`. The final `MAX_ERROR_BODY_BYTES` truncation discards
// it when that start lands at or past the retained cap — i.e. when `f` is at most the gap
// `MAX_ERROR_BODY_DOWNLOAD_BYTES - MAX_ERROR_BODY_BYTES`. A straddling fragment is always shorter than
// the whole token, so once the gap exceeds the longest possible token every such fragment is
// truncated away. That is the invariant.
//
// The assert below is a proportional proxy for that gap, not a proof for arbitrary token lengths: a
// 4x ratio keeps the gap at a floor of 3 * MAX_ERROR_BODY_BYTES (3 KiB at the current retained cap;
// the actual current gap is far larger), which dwarfs any real Flex token (a short opaque string).
// Its job is to fail the build — the two constants live in different modules — if a future edit
// collapses the caps close enough together to shrink that margin below a plausible token, silently
// reopening the straddling leak.
const _: () = assert!(
    MAX_ERROR_BODY_DOWNLOAD_BYTES >= MAX_ERROR_BODY_BYTES * 4,
    "MAX_ERROR_BODY_DOWNLOAD_BYTES must stay well above MAX_ERROR_BODY_BYTES so a token straddling \
     the download-cap boundary is truncated away by the retained-cap bound (see sanitize_error_body)"
);

/// Errors from IBKR Flex Web Service operations.
///
/// `#[non_exhaustive]`: the Flex service may introduce new failure conditions over time, so new
/// variants can be added without a breaking change; downstream `match`es must include a wildcard arm.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum IbkrFlexError {
    /// A required environment variable is not set (see [`IbkrFlexConfig::from_env`]).
    #[error("environment variable error: {0}")]
    EnvVar(String),

    /// A supplied credential (the Flex `token` or `query_id`) is empty after trimming surrounding
    /// whitespace. Raised by [`IbkrFlexConfig::new`] so a malformed credential fails observably at
    /// construction, naming the offending field, rather than later as an opaque IBKR `1003`
    /// "invalid token" after a live fetch.
    #[error("invalid credential: {0}")]
    InvalidCredential(String),

    /// Transport-level HTTP error (connection, timeout, or body-decode failure).
    ///
    /// A non-2xx HTTP *status* no longer surfaces here: the client reads the response body **before**
    /// branching on status and reports a non-success status as [`IbkrFlexError::HttpStatus`] (or the
    /// richer [`IbkrFlexError::Flex`] when the body is a Flex error envelope). This variant is now
    /// purely a connection/timeout/decode failure.
    ///
    /// The inner [`reqwest::Error`] **never carries the request URL.** The Flex token is
    /// transmitted as `t=<TOKEN>` in every request URL, and `reqwest::Error`'s `Display` and
    /// `Debug` both embed that URL verbatim when present. The `From<reqwest::Error>` conversion
    /// for this type always strips it via [`reqwest::Error::without_url`] before the error is
    /// stored, covering every reqwest site that attaches a URL (`send`, body decode);
    /// `Client::build()` errors carry no URL, so the strip is a no-op for them. The `source()`
    /// chain (hyper/IO transport errors) is preserved and does not independently carry the URL.
    ///
    /// Safe to log or display without credential scrubbing.
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),

    /// The service returned a non-success HTTP status whose body was **not** a recognizable Flex
    /// envelope — e.g. a proxy/CDN/WAF error page or a gateway timeout served in front of the Flex
    /// Web Service.
    ///
    /// The body is read before the status is inspected (so the diagnostic is preserved rather than
    /// discarded by `error_for_status`), then bounded to 1 KiB and
    /// **token-scrubbed** before storage: the Flex token rides in the request URL's `t=` parameter,
    /// so a proxy that echoes the request line into its error page could otherwise reflect the
    /// credential into this body. The scrub is best-effort defense-in-depth — it redacts both the
    /// raw token and the encoded form it takes on the wire. An IBKR
    /// *application* error that arrives under a non-2xx status is still surfaced as the richer
    /// [`IbkrFlexError::Flex`], not here.
    #[error("HTTP status {status}: {body}")]
    HttpStatus {
        /// The non-success HTTP status code.
        status: u16,
        /// A bounded, token-scrubbed slice of the response body.
        body: String,
    },

    /// The Flex service returned a non-success status (e.g. invalid token, throttled, expired).
    ///
    /// `message` is the `<ErrorMessage>` element lifted from a server-supplied body, so — like
    /// [`IbkrFlexError::HttpStatus`] and [`IbkrFlexError::Parse`] — both fields are bounded to 1 KiB
    /// and **token-scrubbed** before storage: a proxy that reflected the request line into its
    /// `<ErrorMessage>` could otherwise carry the `t=` Flex token into it. The scrub is a no-op for a
    /// real numeric Flex code/message that does not contain the token.
    #[error("Flex service error ({code}): {message}")]
    Flex {
        /// The Flex `ErrorCode` (empty if the response carried none), bounded and token-scrubbed
        /// like `message`.
        code: String,
        /// The Flex `ErrorMessage` (or a synthesised description), bounded and token-scrubbed.
        message: String,
    },

    /// The statement was still generating after the configured poll budget was exhausted.
    #[error("Flex statement not ready after {attempts} poll attempts")]
    PollTimeout {
        /// Number of poll attempts made before giving up — the configured
        /// [`FlexPollPolicy::max_attempts`] (12 by default), since this error
        /// fires only after every attempt reported the statement still generating.
        attempts: u32,
    },

    /// The statement or status XML could not be parsed.
    ///
    /// A parse message embeds the deserialiser's own rendering of the offending input, so its
    /// length tracks the *document* rather than the failure, and a document that reflected the
    /// request line could carry the `t=` credential into it. This variant is therefore **always
    /// bounded** to 1 KiB, and **token-scrubbed** on every path where a token is in scope — the
    /// same protection [`IbkrFlexError::HttpStatus`] gets, for the same reason.
    ///
    /// Scrubbing covers every `Parse` returned by [`IbkrFlexClient::fetch_statement_xml`] and
    /// [`IbkrFlexClient::fetch_corporate_actions`]. The one path it cannot cover is calling
    /// [`parse_corporate_actions`] directly: there the caller supplies its own XML and no token
    /// exists to redact. Such a message is still bounded — just not scrubbed, since there is
    /// nothing to scrub against.
    #[error("Flex XML parse error: {0}")]
    Parse(String),
}

/// Convert a [`reqwest::Error`] into [`IbkrFlexError::Http`] **with its request URL stripped**.
///
/// The Flex `token` rides in the `t=` query parameter (IBKR's protocol), so it is part of every
/// request URL. `reqwest::Error`'s `Display`/`Debug` embed the stored URL verbatim — e.g.
/// `"... for url (https://.../SendRequest?t=<TOKEN>&q=...)"` — so an unstripped error would leak the
/// credential into any log that formats [`IbkrFlexError::Http`] (the shipped example does exactly
/// that).
///
/// Stripping lives in this single conversion rather than per-call-site discipline so the
/// "[`IbkrFlexError::Http`] never carries the URL" invariant is enforced by the type system: every
/// `?` on a `reqwest::Error` — at the request sites (`send`, body decode) *and* any path added later
/// — routes through here.
/// [`reqwest::Error::without_url`] sets the stored URL to `None` (a no-op for `Client::build()`
/// errors, which carry none) while preserving the diagnostic kind/status and `source()` chain.
impl From<reqwest::Error> for IbkrFlexError {
    fn from(error: reqwest::Error) -> Self {
        IbkrFlexError::Http(error.without_url())
    }
}

/// Poll timing for the GetStatement stage of [`IbkrFlexClient::fetch_statement_xml`].
///
/// The Flex Web Service generates statements asynchronously, so a fetch polls GetStatement until the
/// statement is ready. This policy controls that polling:
///
/// - `initial_delay` — waited **once** before the first poll. The statement is created
///   asynchronously, so the first GetStatement almost always reports `1019` ("still generating"); an
///   initial delay keeps that near-certain miss from consuming one of the bounded attempts.
/// - `interval` — waited between subsequent polls.
/// - `max_attempts` — the number of GetStatement polls before giving up with
///   [`IbkrFlexError::PollTimeout`].
///
/// The [`Default`] budget (`initial_delay` 5 s, `interval` 5 s, `max_attempts` 12) suits a typical
/// small activity statement. A caller fetching a large statement — which takes longer to generate —
/// can widen the budget via [`IbkrFlexClient::with_poll_policy`]. A `max_attempts` of `0` yields an
/// immediate [`IbkrFlexError::PollTimeout`] (an observable "no polling requested" outcome).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlexPollPolicy {
    /// Delay before the first GetStatement poll.
    pub initial_delay: Duration,
    /// Delay between subsequent GetStatement polls.
    pub interval: Duration,
    /// Maximum number of GetStatement poll attempts before [`IbkrFlexError::PollTimeout`].
    pub max_attempts: u32,
}

impl Default for FlexPollPolicy {
    fn default() -> Self {
        Self {
            initial_delay: POLL_INITIAL_DELAY,
            interval: POLL_INTERVAL,
            max_attempts: POLL_MAX_ATTEMPTS,
        }
    }
}

/// Credentials for the IBKR Flex Web Service: an Activity-statement Flex `token` and the id of a
/// saved Flex query that includes the Corporate Actions section.
///
/// `Debug` redacts the token so it never leaks through tracing or panic output.
#[derive(Clone)]
pub struct IbkrFlexConfig {
    token: String,
    query_id: String,
}

impl fmt::Debug for IbkrFlexConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IbkrFlexConfig")
            .field("token", &"[REDACTED]")
            .field("query_id", &self.query_id)
            .finish()
    }
}

impl IbkrFlexConfig {
    /// Create a config from an explicit Flex token and saved-query id.
    ///
    /// Both values are **trimmed** of surrounding whitespace and rejected if empty (or empty after
    /// trimming), so a malformed credential fails here — observably, naming the offending field —
    /// rather than later as an opaque IBKR `1003` "invalid token" at fetch time. This mirrors the
    /// validation [`from_env`](Self::from_env) performs, and matches the fallible-`new` shape used
    /// by the other exchange clients in this crate.
    ///
    /// # Errors
    ///
    /// Returns [`IbkrFlexError::InvalidCredential`] if either value is empty after trimming.
    pub fn new(
        token: impl Into<String>,
        query_id: impl Into<String>,
    ) -> Result<Self, IbkrFlexError> {
        Ok(Self {
            token: Self::require_nonempty_trimmed("token", token.into())?,
            query_id: Self::require_nonempty_trimmed("query_id", query_id.into())?,
        })
    }

    /// Create a config from environment variables.
    ///
    /// Reads `IBKR_FLEX_TOKEN` (the Flex Web Service token) and `IBKR_FLEX_QUERY_ID` (the saved
    /// query id), both required.
    ///
    /// # Errors
    ///
    /// Returns [`IbkrFlexError::EnvVar`] if either variable is missing or set but empty.
    pub fn from_env() -> Result<Self, IbkrFlexError> {
        let token = env::var("IBKR_FLEX_TOKEN")
            .map_err(|e| IbkrFlexError::EnvVar(format!("IBKR_FLEX_TOKEN: {e}")))?;
        let query_id = env::var("IBKR_FLEX_QUERY_ID")
            .map_err(|e| IbkrFlexError::EnvVar(format!("IBKR_FLEX_QUERY_ID: {e}")))?;
        // Trim before storing so surrounding whitespace can't silently end up in the `t=`/`q=`
        // query parameters (which IBKR would reject with a confusing 1003 "invalid token" at fetch
        // time rather than a clear configuration error here).
        let token = token.trim();
        let query_id = query_id.trim();
        if token.is_empty() {
            return Err(IbkrFlexError::EnvVar(
                "IBKR_FLEX_TOKEN is set but empty".to_owned(),
            ));
        }
        if query_id.is_empty() {
            return Err(IbkrFlexError::EnvVar(
                "IBKR_FLEX_QUERY_ID is set but empty".to_owned(),
            ));
        }
        // Route through `new` so there is a single construction/validation path. The env-var checks
        // above already produce variable-specific messages (`IBKR_FLEX_TOKEN is set but empty`),
        // which are more actionable than `new`'s generic field-level error, so they run first; the
        // re-trim/re-check inside `new` is then a cheap no-op.
        Self::new(token, query_id)
    }

    /// Trim `value` and reject it if empty (or empty after trimming), tagging the error with
    /// `field` so the failure names which credential was malformed. Allocates a new `String` only
    /// when trimming actually removed characters.
    fn require_nonempty_trimmed(
        field: &'static str,
        value: String,
    ) -> Result<String, IbkrFlexError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(IbkrFlexError::InvalidCredential(format!(
                "{field} is empty after trimming whitespace"
            )));
        }
        if trimmed.len() == value.len() {
            Ok(value)
        } else {
            Ok(trimmed.to_owned())
        }
    }
}

/// Client for fetching corporate-action records from the IBKR Flex Web Service.
///
/// `Debug` omits both the token (a credential) and the [`reqwest::Client`], so neither can leak.
#[derive(Clone)]
pub struct IbkrFlexClient {
    http: reqwest::Client,
    token: String,
    query_id: String,
    base_url: String,
    poll_policy: FlexPollPolicy,
}

impl fmt::Debug for IbkrFlexClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately omits the token (a credential) and the `reqwest::Client`.
        f.debug_struct("IbkrFlexClient")
            .field("query_id", &self.query_id)
            .field("base_url", &self.base_url)
            .field("poll_policy", &self.poll_policy)
            .finish_non_exhaustive()
    }
}

impl IbkrFlexClient {
    /// Create a client from a [`IbkrFlexConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`IbkrFlexError::Http`] if the underlying HTTP client cannot be built.
    pub fn new(config: IbkrFlexConfig) -> Result<Self, IbkrFlexError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            // Transport-layer token protection. The Flex `token` rides in every request URL's `t=`
            // query parameter, so it must never traverse an unencrypted connection. Two guards work
            // together: `redirect::Policy::none()` stops reqwest auto-following any redirect, so a 3xx
            // can never bounce the token to another (possibly `http://`) URL behind our back; and
            // `https_only(true)` rejects any request whose own URL is not `https`, catching a
            // misconfigured `http://` base URL before the token reaches the wire. This client issues
            // two GETs to known HTTPS endpoints and expects no redirects, so an unexpected 3xx is
            // returned unfollowed and surfaces downstream as an `IbkrFlexError::HttpStatus` (a
            // non-success status with a non-Flex body) — never as a silent scheme downgrade. (The
            // content-layer scheme check on the SendRequest poll URL cannot
            // intercept redirects reqwest would otherwise follow, which is why these guards exist.)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            token: config.token,
            query_id: config.query_id,
            base_url: FLEX_BASE_URL.to_owned(),
            poll_policy: FlexPollPolicy::default(),
        })
    }

    /// Override the GetStatement [`FlexPollPolicy`] (poll timing / budget).
    ///
    /// Builder-style; defaults to [`FlexPollPolicy::default`]. Widen the budget for a large
    /// statement (which takes longer to generate):
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use rustrade_data::exchange::ibkr::{IbkrFlexClient, FlexPollPolicy};
    /// # fn f() -> Result<(), Box<dyn std::error::Error>> {
    /// let _client = IbkrFlexClient::from_env()?.with_poll_policy(FlexPollPolicy {
    ///     initial_delay: Duration::from_secs(10),
    ///     interval: Duration::from_secs(10),
    ///     max_attempts: 30,
    /// });
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_poll_policy(mut self, policy: FlexPollPolicy) -> Self {
        self.poll_policy = policy;
        self
    }

    /// Create a client from environment variables (see [`IbkrFlexConfig::from_env`]).
    ///
    /// # Errors
    ///
    /// Returns [`IbkrFlexError::EnvVar`] if a required variable is missing, or
    /// [`IbkrFlexError::Http`] if the HTTP client cannot be built.
    pub fn from_env() -> Result<Self, IbkrFlexError> {
        Self::new(IbkrFlexConfig::from_env()?)
    }

    /// Run the full 2-call Flex flow and return the raw `<FlexQueryResponse>` statement XML.
    ///
    /// Issues SendRequest, then polls GetStatement until the statement is ready, governed by this
    /// client's [`FlexPollPolicy`] (an `initial_delay` before the first poll, `interval` between
    /// subsequent polls, and a `max_attempts` budget). Defaults to [`FlexPollPolicy::default`];
    /// override with [`with_poll_policy`](Self::with_poll_policy).
    ///
    /// # Security
    ///
    /// The Flex `token` is transmitted as the `t=` URL **query parameter** (as IBKR's protocol
    /// requires), so it is part of every request URL. Do not attach request-logging middleware to
    /// the underlying [`reqwest::Client`] that records full URLs, or the token will be captured in
    /// plaintext. The poll URL returned by SendRequest is required to be `https://` for the same
    /// reason — the token must never traverse an unencrypted connection.
    ///
    /// For the same reason, [`IbkrFlexError::Http`] must never embed the request URL: a
    /// `reqwest::Error`'s `Display` includes the URL it was building, which would carry `t=<TOKEN>`
    /// into any log that formats the error. The `From<reqwest::Error>` conversion for
    /// [`IbkrFlexError`] strips the URL via [`reqwest::Error::without_url`] before the error is
    /// stored, so a plain `?` is safe here and in any request path added later.
    ///
    /// Two IBKR hosts are involved: `ndcdyn.interactivebrokers.com` (the hard-coded `FLEX_BASE_URL`
    /// that serves `SendRequest`) and `gdcdyn.interactivebrokers.com` (the statement-download host
    /// that `SendRequest` returns in its poll URL). The poll host is deliberately **not** pinned to
    /// an allowlist — only its scheme is enforced (`https://`) — so IBKR can relocate statement
    /// serving without breaking this client, while the `https://` gate keeps the token encrypted
    /// regardless of which host is returned.
    ///
    /// # Errors
    ///
    /// - [`IbkrFlexError::Http`] on a transport failure.
    /// - [`IbkrFlexError::Flex`] if the service reports a terminal non-success status.
    /// - [`IbkrFlexError::HttpStatus`] if a non-success HTTP status carries a body that is not a
    ///   recognizable Flex envelope (e.g. a proxy/CDN error page).
    /// - [`IbkrFlexError::PollTimeout`] if the statement is still generating after the poll budget.
    /// - [`IbkrFlexError::Parse`] if a response cannot be parsed.
    pub async fn fetch_statement_xml(&self) -> Result<String, IbkrFlexError> {
        let send_url = format!("{}/SendRequest", self.base_url);
        debug!("Requesting IBKR Flex statement generation");

        let (status, body) = self
            .get_with_query(&send_url, self.query_id.as_str())
            .await?;
        let SendRequestOk {
            reference_code,
            url,
        } = interpret_send_response(status, &body, &self.token)?;

        poll_until_ready(&self.poll_policy, &self.token, async || {
            self.get_with_query(&url, reference_code.as_str()).await
        })
        .await
    }

    /// Issue a GET to `url` with the standard Flex query parameters (`t`/`q`/`v`) and read the
    /// response body **regardless of HTTP status**, returning the status alongside the body.
    ///
    /// Reading the body before branching on status is deliberate: `error_for_status` discards the
    /// body, but IBKR/proxy diagnostic bodies — and Flex application errors (e.g. `1019`) that arrive
    /// under a non-2xx status — are exactly what a caller needs to diagnose a failure. The status is
    /// returned so the caller ([`interpret_send_response`] / [`interpret_poll_response`]) can decide
    /// whether an unrecognized body under a non-success status is a transport error
    /// ([`IbkrFlexError::HttpStatus`]) versus a genuine Flex-envelope error.
    ///
    /// A **success** body is the statement payload (a `<FlexQueryResponse>`, which can be large) and
    /// is read in full. A **non-success** body is only a small Flex status/error envelope or a
    /// proxy/CDN diagnostic page — never the statement — so it is read only up to
    /// `MAX_ERROR_BODY_DOWNLOAD_BYTES`, bounding a pathological error page that would otherwise be
    /// buffered without limit (it is truncated further for storage anyway). The cap is far above any
    /// real Flex envelope, so a `1019`/error response the poll loop still parses is never truncated.
    ///
    /// The `?` on `send`/`chunk`/`text` still routes any `reqwest::Error` through the URL-stripping
    /// `From<reqwest::Error>` impl, so the token never leaks into an error.
    async fn get_with_query(
        &self,
        url: &str,
        q: &str,
    ) -> Result<(reqwest::StatusCode, String), IbkrFlexError> {
        let response = self
            .http
            .get(url)
            .query(&[("t", self.token.as_str()), ("q", q), ("v", FLEX_VERSION)])
            .send()
            .await?;
        read_status_and_body(response).await
    }

    /// Fetch the statement and parse its Corporate Actions section into faithful records.
    ///
    /// Convenience over [`fetch_statement_xml`](Self::fetch_statement_xml) +
    /// [`parse_corporate_actions`]. Returns **all** reorg rows; filtering to splits is the caller's
    /// job. See [`IbkrFlexCorporateAction`] for the record contract and limitations.
    ///
    /// # Errors
    ///
    /// Same as [`fetch_statement_xml`](Self::fetch_statement_xml).
    pub async fn fetch_corporate_actions(
        &self,
    ) -> Result<Vec<IbkrFlexCorporateAction>, IbkrFlexError> {
        let xml = self.fetch_statement_xml().await?;
        // The token is threaded *into* the parse rather than scrubbed off its result: redaction
        // matches the full token, so it must run before the message is bounded, and
        // `parse_corporate_actions` bounds before returning. Scrubbing afterwards would leave a
        // credential straddling the cap present only as an unmatched prefix fragment. This re-parses
        // a body that already crossed the network, and a misconfigured proxy that echoed the request
        // line into it would have put `t=<TOKEN>` in reach of a deserialiser message.
        corporate_action::parse_corporate_actions_scrubbed(&xml, Some(&self.token))
    }
}

/// Read a response's HTTP status and body, bounding the read on the error path.
///
/// Split out of [`IbkrFlexClient::get_with_query`] so this contract can be exercised against a
/// synthetic [`reqwest::Response`] with no live socket (reqwest exposes
/// `From<http::Response<_>>`), which the caller's `send()` cannot be. That matters because the
/// ordering here is the load-bearing part: the body is read **before** the status is judged, so a
/// Flex envelope arriving under a non-2xx status — notably the retryable `1019` "still generating"
/// — survives to be classified. `error_for_status()?.text().await?` would discard it and abort the
/// poll, and every test that drives [`interpret_poll_response`] with a synthetic `(status, body)`
/// tuple would still pass. See `read_status_and_body_*` in this module's tests.
///
/// A **success** body is the statement payload (a `<FlexQueryResponse>`, which can be large) and is
/// read in full. A **non-success** body is only a small Flex status/error envelope or a proxy/CDN
/// diagnostic page — never the statement — so it is capped at [`MAX_ERROR_BODY_DOWNLOAD_BYTES`],
/// far above any real Flex envelope.
async fn read_status_and_body(
    response: reqwest::Response,
) -> Result<(reqwest::StatusCode, String), IbkrFlexError> {
    let status = response.status();
    let body = if status.is_success() {
        response.text().await?
    } else {
        read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?
    };
    Ok((status, body))
}

/// Poll GetStatement until the statement is ready or `policy`'s attempt budget is exhausted,
/// delegating each raw fetch to `poll_once`.
///
/// Isolating the loop from the transport keeps its orchestration deterministically testable without
/// a network round trip: `poll_once` is any async producer of a `(status, body)` pair, so a scripted
/// responder can drive the exact wiring — the `max_attempts == 0` immediate-timeout case, the
/// `1..=max_attempts` bound, and the `attempt < max_attempts` inter-poll sleep guard — under
/// zero-duration policies. (The live two-call round trip stays covered by the `#[ignore]`d network
/// test.)
///
/// The `initial_delay` is waited once before the first poll: the statement is generated
/// asynchronously, so an immediate GetStatement almost always reports `1019` ("still generating") and
/// would burn one of the bounded attempts on a near-certain miss. It is skipped when no polls are
/// budgeted (`max_attempts == 0`), so that documented "no polling requested" case fails over
/// immediately with [`IbkrFlexError::PollTimeout`] rather than sleeping before an empty loop.
async fn poll_until_ready<F>(
    policy: &FlexPollPolicy,
    token: &str,
    mut poll_once: F,
) -> Result<String, IbkrFlexError>
where
    F: AsyncFnMut() -> Result<(reqwest::StatusCode, String), IbkrFlexError>,
{
    if policy.max_attempts > 0 {
        tokio::time::sleep(policy.initial_delay).await;
    }

    for attempt in 1..=policy.max_attempts {
        let (status, body) = poll_once().await?;
        match interpret_poll_response(status, &body, token)? {
            GetStatementOutcome::Ready => return Ok(body),
            GetStatementOutcome::InProgress => {
                debug!(
                    attempt,
                    max = policy.max_attempts,
                    "Flex statement still generating"
                );
                if attempt < policy.max_attempts {
                    tokio::time::sleep(policy.interval).await;
                }
            }
        }
    }

    Err(IbkrFlexError::PollTimeout {
        attempts: policy.max_attempts,
    })
}

// ============================================================================
// Pure, testable response classification
// ============================================================================

/// A successful SendRequest result: the reference code + poll URL for GetStatement.
#[derive(Debug)]
struct SendRequestOk {
    reference_code: String,
    url: String,
}

/// The outcome of classifying a GetStatement poll response. A terminal failure is returned as an
/// `Err` from [`classify_get_statement`], so this only models the two non-error outcomes.
#[derive(Debug, PartialEq, Eq)]
enum GetStatementOutcome {
    /// The body is a `<FlexQueryResponse>` statement, ready to parse.
    Ready,
    /// The service is still generating the statement; poll again.
    InProgress,
}

/// The `<FlexStatementResponse>` envelope (SendRequest result and GetStatement "not ready" / error
/// status). Its fields are child *elements*, not attributes.
#[derive(Debug, Deserialize)]
struct FlexStatementResponse {
    #[serde(rename = "Status", default)]
    status: String,
    #[serde(rename = "ReferenceCode", default)]
    reference_code: Option<String>,
    #[serde(rename = "Url", default)]
    url: Option<String>,
    #[serde(rename = "ErrorCode", default)]
    error_code: Option<String>,
    #[serde(rename = "ErrorMessage", default)]
    error_message: Option<String>,
}

/// Parse a SendRequest response: success yields the reference code + poll URL; any other status
/// becomes an [`IbkrFlexError::Flex`].
fn parse_send_request_response(xml: &str) -> Result<SendRequestOk, IbkrFlexError> {
    // A SendRequest response is always a `<FlexStatementResponse>` envelope. Reject any other root
    // element as a parse error up front — otherwise quick_xml's lenient, all-`#[serde(default)]`
    // deserialization coerces a non-Flex body (e.g. a proxy/CDN HTML error page) into an
    // empty-status envelope, which would surface as a confusing `Flex { code: "" }`. Mirrors the
    // root-element check `classify_get_statement` already does, and lets `interpret_send_response`
    // distinguish a non-Flex body (→ `HttpStatus` under a non-2xx status) from a genuine Flex error.
    match root_element_name(xml).as_deref() {
        Some("FlexStatementResponse") => {}
        Some(other) => {
            return Err(IbkrFlexError::Parse(format!(
                "unexpected Flex SendRequest root element <{other}>"
            )));
        }
        None => {
            return Err(IbkrFlexError::Parse(
                "empty or unreadable Flex SendRequest response".to_owned(),
            ));
        }
    }

    let resp: FlexStatementResponse = quick_xml::de::from_str(xml).map_err(|e| {
        IbkrFlexError::Parse(format!("failed to parse Flex SendRequest response: {e}"))
    })?;

    if resp.status.eq_ignore_ascii_case("Success") {
        match (nonempty(resp.reference_code), nonempty(resp.url)) {
            (Some(reference_code), Some(url)) => {
                // The poll URL is broker-supplied and is fetched next with the Flex token in its
                // `t=` query parameter. Refuse a non-HTTPS URL so the credential can never be sent
                // over an unencrypted connection (a scheme check only — no brittle host allowlist).
                if !url.starts_with("https://") {
                    return Err(IbkrFlexError::Parse(format!(
                        "Flex SendRequest returned a non-HTTPS poll URL; refusing to send credentials over it: {url}"
                    )));
                }
                Ok(SendRequestOk {
                    reference_code,
                    url,
                })
            }
            _ => Err(IbkrFlexError::Parse(
                "Flex SendRequest reported success but is missing ReferenceCode/Url".to_owned(),
            )),
        }
    } else {
        Err(flex_error(resp, "SendRequest"))
    }
}

/// Classify a GetStatement poll response by its root element: a `<FlexQueryResponse>` is the ready
/// statement; a `<FlexStatementResponse>` is either "still generating" (`ErrorCode 1019`) or a
/// terminal error.
fn classify_get_statement(xml: &str) -> Result<GetStatementOutcome, IbkrFlexError> {
    match root_element_name(xml).as_deref() {
        Some("FlexQueryResponse") => Ok(GetStatementOutcome::Ready),
        Some("FlexStatementResponse") => {
            let resp: FlexStatementResponse = quick_xml::de::from_str(xml).map_err(|e| {
                IbkrFlexError::Parse(format!(
                    "failed to parse Flex GetStatement status response: {e}"
                ))
            })?;
            // "Generation in progress" always arrives as ErrorCode 1019 *with* Status=Warn per the
            // IBKR Flex v3 protocol (see the `GET_IN_PROGRESS` fixture). Require both: a 1019 under
            // Status=Fail would be a terminal error IBKR mislabeled, and retrying it would only burn
            // the poll budget before timing out. The status compare is case-insensitive to tolerate
            // label-casing variance.
            if resp.error_code.as_deref() == Some(GENERATION_IN_PROGRESS_CODE)
                && resp.status.eq_ignore_ascii_case("warn")
            {
                Ok(GetStatementOutcome::InProgress)
            } else {
                Err(flex_error(resp, "GetStatement"))
            }
        }
        Some(other) => Err(IbkrFlexError::Parse(format!(
            "unexpected Flex GetStatement root element <{other}>"
        ))),
        None => Err(IbkrFlexError::Parse(
            "empty or unreadable Flex GetStatement response".to_owned(),
        )),
    }
}

/// Build an [`IbkrFlexError::Flex`] from a non-success status envelope.
fn flex_error(resp: FlexStatementResponse, stage: &str) -> IbkrFlexError {
    IbkrFlexError::Flex {
        code: resp.error_code.unwrap_or_default(),
        message: resp
            .error_message
            .unwrap_or_else(|| format!("Flex {stage} returned non-success status {}", resp.status)),
    }
}

/// Scrub the Flex `token` from, and bound, a terminal [`IbkrFlexError::Flex`]'s fields.
///
/// A `Flex` error's `message` is the `<ErrorMessage>` element lifted verbatim from a server-supplied
/// body — the same untrusted source [`IbkrFlexError::HttpStatus`] and [`IbkrFlexError::Parse`]
/// scrub. A body that reflected the request line (a misconfigured proxy echoing `?t=<TOKEN>` into
/// its `<ErrorMessage>`) could otherwise carry the credential into the stored error, and an
/// oversized element could bloat it. `code` is a short structured field in practice, but running it
/// through the same [`sanitize_error_body`] (redact-before-bound, capped at [`MAX_ERROR_BODY_BYTES`])
/// costs nothing on this error path and closes the gap uniformly — the scrub is a no-op for a real
/// numeric Flex code that does not contain the token.
fn scrub_flex_error(code: String, message: String, token: &str) -> IbkrFlexError {
    IbkrFlexError::Flex {
        code: sanitize_error_body(&code, token),
        message: sanitize_error_body(&message, token),
    }
}

/// Interpret a SendRequest response given its HTTP `status` and `body`.
///
/// The body is parsed as a Flex envelope **first**, so an IBKR application error that arrives under
/// a non-2xx status still surfaces as the richer [`IbkrFlexError::Flex`] (e.g. `1003` invalid
/// token). A body that is *not* a recognizable Flex envelope under a non-success status becomes
/// [`IbkrFlexError::HttpStatus`] (a proxy/CDN error page) rather than a misleading XML parse error;
/// the same malformed body under a 2xx status stays a genuine [`IbkrFlexError::Parse`] — with its
/// message token-scrubbed, since an XML deserialiser embeds fragments of the offending input. A
/// genuine Flex error envelope surfaces as [`IbkrFlexError::Flex`], its fields likewise scrubbed and
/// bounded ([`scrub_flex_error`]), since `<ErrorMessage>` is server-supplied free text.
fn interpret_send_response(
    status: reqwest::StatusCode,
    body: &str,
    token: &str,
) -> Result<SendRequestOk, IbkrFlexError> {
    match parse_send_request_response(body) {
        Err(IbkrFlexError::Parse(_)) if !status.is_success() => {
            Err(http_status_error(status, body, token))
        }
        Err(IbkrFlexError::Parse(message)) => Err(finish_parse_error(message, Some(token))),
        Err(IbkrFlexError::Flex { code, message }) => Err(scrub_flex_error(code, message, token)),
        other => other,
    }
}

/// Interpret a GetStatement poll response given its HTTP `status` and `body`.
///
/// The body is classified **first**, so a `1019` "still generating" that IBKR returns under a
/// non-2xx status is still recognized as retryable — previously `error_for_status` aborted the poll
/// before the body was classified. A body that is not a recognizable Flex envelope under a
/// non-success status becomes [`IbkrFlexError::HttpStatus`]; a terminal Flex error envelope under a
/// non-2xx status stays the richer [`IbkrFlexError::Flex`], its fields scrubbed and bounded
/// ([`scrub_flex_error`]) since `<ErrorMessage>` is server-supplied free text.
fn interpret_poll_response(
    status: reqwest::StatusCode,
    body: &str,
    token: &str,
) -> Result<GetStatementOutcome, IbkrFlexError> {
    match classify_get_statement(body) {
        Err(IbkrFlexError::Parse(_)) if !status.is_success() => {
            Err(http_status_error(status, body, token))
        }
        Err(IbkrFlexError::Parse(message)) => Err(finish_parse_error(message, Some(token))),
        Err(IbkrFlexError::Flex { code, message }) => Err(scrub_flex_error(code, message, token)),
        other => other,
    }
}

/// Build an [`IbkrFlexError::Parse`] from an **unbounded** message: redact `token` when one is in
/// scope, then bound the result to [`MAX_ERROR_BODY_BYTES`].
///
/// A parse failure message embeds the deserialiser's own rendering of the offending input —
/// quick-xml's `DeError::UnexpectedStart`, for instance, carries the raw tag bytes it choked on. A
/// body that reflected the request line (a misconfigured proxy echoing `?t=<TOKEN>`) could
/// therefore carry the credential into the stored error, which [`IbkrFlexError::HttpStatus`] is
/// already protected against but this variant was not.
///
/// This is the single **finaliser** for a `Parse` that leaves the module — not the single
/// construction point. The raw parsers [`parse_send_request_response`] and [`classify_get_statement`]
/// build *unbounded, unscrubbed* `Parse` values directly, but they are private and every production
/// caller routes their `Err(Parse(_))` back through here (or, for a non-2xx non-envelope body,
/// through [`http_status_error`]): see [`interpret_send_response`]/[`interpret_poll_response`], and
/// [`parse_corporate_actions_scrubbed`](corporate_action::parse_corporate_actions_scrubbed) for the
/// statement re-parse. Keeping the redact-before-bound ordering in exactly one place is what makes
/// that safe: it is a security invariant, not an implementation detail — redaction matches the full
/// token, so bounding first can leave a credential straddling the cap present only as an unmatched
/// prefix fragment. A finaliser duplicated across callers would be one edit away from diverging on
/// it.
///
/// Maintainer corollary: the guarantee is caller discipline over two private parsers, **not** a
/// property the type system enforces. A *new* call site for either raw parser must route its `Parse`
/// through this function (or `http_status_error`) too, or it reintroduces the unbounded/unscrubbed
/// leak this exists to prevent.
///
/// `message` must therefore arrive unbounded — a caller that has already truncated at
/// [`MAX_ERROR_BODY_BYTES`] has already lost the guarantee, and passing it here cannot recover it.
/// Both branches end at the same cap and differ only in whether a redaction pass runs first, so the
/// bound is applied exactly once either way.
fn finish_parse_error(message: String, token: Option<&str>) -> IbkrFlexError {
    IbkrFlexError::Parse(match token {
        Some(token) => sanitize_error_body(&message, token),
        None => truncate_str(&message, MAX_ERROR_BODY_BYTES),
    })
}

/// Build an [`IbkrFlexError::HttpStatus`] from a non-success response, bounding and token-scrubbing
/// the body.
fn http_status_error(status: reqwest::StatusCode, body: &str, token: &str) -> IbkrFlexError {
    IbkrFlexError::HttpStatus {
        status: status.as_u16(),
        body: sanitize_error_body(body, token),
    }
}

/// Redact the Flex `token` — both its raw form and its `application/x-www-form-urlencoded` wire
/// form — from `body`, then bound the result to [`MAX_ERROR_BODY_BYTES`] (on a UTF-8 char boundary).
///
/// The token rides in the request URL's `t=` query parameter, attached via reqwest's `.query(&[…])`,
/// which serialises through [`url::Url::query_pairs_mut`] — WHATWG form-urlencoding, which escapes
/// space as `+` and leaves a *narrower* byte set unescaped than RFC-3986 percent-encoding (e.g. `~`
/// is escaped here). A misconfigured proxy or WAF that echoes the request line into its error page
/// would reflect *that* wire form, not the raw token, so both are scrubbed. The wire form is computed
/// with [`url::form_urlencoded::byte_serialize`] — the same primitive `url`'s serializer (and thus
/// reqwest's `.query()`) uses internally, not a hand-rolled percent-encoding table — so it cannot
/// drift from what is actually sent (`sanitize_error_body_scrubs_reqwests_wire_encoded_token` pins
/// this against reqwest's real request encoding).
///
/// This is **best-effort defense-in-depth, not an absolute guarantee**: an intermediary could still
/// transform the reflected value in a way this scrub does not anticipate (further re-encoding,
/// HTML-entity escaping, or truncation that splits the token). For a real IBKR token — a numeric
/// string that encodes to itself — the raw scrub already covers it and the wire-form pass is a no-op.
///
/// How `body` is bounded depends on the caller, and only one of the two arrives pre-bounded:
///
/// - Via [`http_status_error`] (non-success status), `body` has already passed through
///   [`read_body_capped`] at [`MAX_ERROR_BODY_DOWNLOAD_BYTES`] (64 KiB) in
///   [`IbkrFlexClient::get_with_query`]. The two caps are complementary, not redundant — the outer
///   one bounds memory during the network read itself, this one bounds what is retained in the
///   stored error. The 64× gap between them also means a token fragment straddling the *download*
///   boundary cannot survive into the final message: the trailing partial token sits far past
///   [`MAX_ERROR_BODY_BYTES`] and is truncated away.
/// - Via [`finish_parse_error`] (2xx status), `body` is a deserialiser message — raised either
///   while interpreting a SendRequest/GetStatement response or while re-parsing a statement in
///   [`parse_corporate_actions_scrubbed`](corporate_action::parse_corporate_actions_scrubbed) —
///   derived from an **unbounded** `response.text()` read,
///   so no download-level cap applies to it. This function therefore applies the coarse
///   [`MAX_ERROR_BODY_DOWNLOAD_BYTES`] bound itself, on entry, reproducing the same 64× gap the
///   non-success path gets for free — wide enough that it cannot split a token whose fragment would
///   then survive into the retained message, but narrow enough that the redaction passes below do
///   not scan an unbounded string.
///
/// Callers must pass a message that has **not** already been truncated at
/// [`MAX_ERROR_BODY_BYTES`]: redaction matches the full token, so a caller-side cut at the same
/// width this function retains could leave a straddling credential as an unmatched prefix. Both 2xx
/// callers hand over their message unbounded for exactly this reason.
fn sanitize_error_body(body: &str, token: &str) -> String {
    // Bound the input before the redaction passes. `redact` allocates a full copy per pass even when
    // the needle is absent, and a 2xx `Parse` message derives from an unbounded read, so without
    // this an oversized message is copied twice before the trailing truncate discards nearly all of
    // it. Slicing (rather than `truncate_str`) keeps the common under-cap case allocation-free, and
    // the cap sits 64× above what is retained, so it cannot create the straddling-fragment problem
    // the redact-then-bound ordering below exists to avoid.
    let body = if body.len() > MAX_ERROR_BODY_DOWNLOAD_BYTES {
        &body[..body.floor_char_boundary(MAX_ERROR_BODY_DOWNLOAD_BYTES)]
    } else {
        body
    };
    // Redact the token over the (coarsely bounded) body first — order matters for correctness, not
    // just cost:
    // bounding first could leave a credential that straddles the byte cap present only as a prefix,
    // which the full-token `contains`/`replace` would then miss, and redacting after bounding could
    // also push the result back over the cap (`[REDACTED]` is longer than a short token). Scanning
    // the whole body only runs on the error path, so the extra pass is acceptable.
    let mut scrubbed = redact(body, token);
    // Also scrub the encoded form the token actually takes on the wire, so a reflected request line
    // is covered too — but only when it differs from the raw token (a no-op for numeric tokens).
    if !token.is_empty() {
        let wire_form: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
        if wire_form.as_str() != token {
            scrubbed = redact(&scrubbed, &wire_form);
        }
    }
    // Then bound to MAX_ERROR_BODY_BYTES so a large proxy/CDN error page can't bloat the error.
    // `floor_char_boundary` rounds the cap down to a UTF-8 char boundary (there is always one at or
    // below it), keeping the truncated string valid — which `String::truncate` requires. Matches the
    // idiom used across the crate (e.g. `massive::error`, `massive::rest`, `binance::error`).
    if scrubbed.len() > MAX_ERROR_BODY_BYTES {
        scrubbed.truncate(scrubbed.floor_char_boundary(MAX_ERROR_BODY_BYTES));
    }
    scrubbed
}

/// Replace every occurrence of `needle` in `body` with `[REDACTED]`, always returning an owned
/// `String` (the caller chains two redaction passes over the result, so a borrowing return would not
/// help). The `contains` guard skips only the `replace` scan when `needle` is absent — it does not
/// avoid the allocation. An empty `needle` is never treated as a match: `str::replace` with an empty
/// pattern would splice the replacement between every character.
fn redact(body: &str, needle: &str) -> String {
    if !needle.is_empty() && body.contains(needle) {
        body.replace(needle, "[REDACTED]")
    } else {
        body.to_owned()
    }
}

/// Read the name of the first (root) element of an XML document, ignoring the prolog/comments.
///
/// Returns a [`SmolStr`] so the expected root names (`FlexQueryResponse`, `FlexStatementResponse`)
/// are compared without a heap allocation (both fit `SmolStr`'s inline buffer).
fn root_element_name(xml: &str) -> Option<SmolStr> {
    let mut reader = Reader::from_str(xml);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                return Some(SmolStr::new(e.name().as_ref()));
            }
            Ok(Event::Eof) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

/// Trim an optional string, collapsing empty/whitespace-only values to `None`.
///
/// Reuses the original allocation when the value needs no trimming (the common case for
/// well-formed Flex attributes), only allocating when whitespace is actually stripped.
fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else if trimmed.len() == v.len() {
            Some(v)
        } else {
            Some(trimmed.to_owned())
        }
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Tests should panic on unexpected values.
mod tests {
    use super::*;

    const SEND_SUCCESS: &str = r#"<FlexStatementResponse timestamp="15 January, 2025 03:00 PM EST">
        <Status>Success</Status>
        <ReferenceCode>1234567890</ReferenceCode>
        <Url>https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement</Url>
    </FlexStatementResponse>"#;

    const SEND_FAIL: &str = r#"<FlexStatementResponse timestamp="15 January, 2025 03:00 PM EST">
        <Status>Fail</Status>
        <ErrorCode>1003</ErrorCode>
        <ErrorMessage>Statement could not be generated at this time. Please try again shortly.</ErrorMessage>
    </FlexStatementResponse>"#;

    const GET_IN_PROGRESS: &str = r#"<FlexStatementResponse timestamp="15 January, 2025 03:00 PM EST">
        <Status>Warn</Status>
        <ErrorCode>1019</ErrorCode>
        <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
    </FlexStatementResponse>"#;

    const GET_FAIL: &str = r#"<FlexStatementResponse>
        <Status>Fail</Status>
        <ErrorCode>1020</ErrorCode>
        <ErrorMessage>Invalid request or unable to validate request.</ErrorMessage>
    </FlexStatementResponse>"#;

    const GET_READY: &str = r#"<?xml version="1.0"?>
        <FlexQueryResponse queryName="Activity" type="AF">
          <FlexStatements count="1">
            <FlexStatement accountId="U1"><CorporateActions /></FlexStatement>
          </FlexStatements>
        </FlexQueryResponse>"#;

    #[test]
    fn send_request_success_extracts_reference_and_url() {
        let ok = parse_send_request_response(SEND_SUCCESS).unwrap();
        assert_eq!(ok.reference_code, "1234567890");
        assert_eq!(
            ok.url,
            "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement"
        );
    }

    #[test]
    fn send_request_failure_is_a_flex_error() {
        match parse_send_request_response(SEND_FAIL) {
            Err(IbkrFlexError::Flex { code, message }) => {
                assert_eq!(code, "1003");
                assert!(message.contains("could not be generated"));
            }
            other => panic!("expected Flex error, got {other:?}"),
        }
    }

    #[test]
    fn send_request_success_without_url_is_a_parse_error() {
        let xml = r#"<FlexStatementResponse><Status>Success</Status>
            <ReferenceCode>123</ReferenceCode></FlexStatementResponse>"#;
        assert!(matches!(
            parse_send_request_response(xml),
            Err(IbkrFlexError::Parse(_))
        ));
    }

    #[test]
    fn send_request_non_https_url_is_rejected() {
        // A success response whose poll URL is plain HTTP must be refused: the token rides in that
        // URL's `t=` query parameter and must never traverse an unencrypted connection.
        let xml = r#"<FlexStatementResponse><Status>Success</Status>
            <ReferenceCode>123</ReferenceCode>
            <Url>http://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement</Url></FlexStatementResponse>"#;
        assert!(matches!(
            parse_send_request_response(xml),
            Err(IbkrFlexError::Parse(_))
        ));
    }

    #[test]
    fn send_request_non_envelope_root_is_a_parse_error() {
        // A non-<FlexStatementResponse> body (e.g. a proxy/CDN HTML error page) must be a parse
        // error, not coerced by lenient all-`#[serde(default)]` deserialization into a confusing
        // empty-status `Flex` error. This is what lets `interpret_send_response` treat it as an
        // HttpStatus under a non-2xx status.
        for body in [
            "<html><body>502 Bad Gateway</body></html>",
            "<not-flex/>",
            "",
        ] {
            assert!(
                matches!(
                    parse_send_request_response(body),
                    Err(IbkrFlexError::Parse(_))
                ),
                "expected Parse for body {body:?}"
            );
        }
    }

    #[test]
    fn get_statement_ready_is_classified_ready() {
        assert_eq!(
            classify_get_statement(GET_READY).unwrap(),
            GetStatementOutcome::Ready
        );
    }

    #[test]
    fn get_statement_generation_in_progress_is_classified_in_progress() {
        assert_eq!(
            classify_get_statement(GET_IN_PROGRESS).unwrap(),
            GetStatementOutcome::InProgress
        );
    }

    #[test]
    fn get_statement_in_progress_code_is_case_insensitive_on_status() {
        // The `Status=Warn` check uses `eq_ignore_ascii_case`, so casing variance (e.g. all-caps
        // `WARN`) must still classify a 1019 as retryable rather than fail fast on a terminal error.
        const WARN_UPPER_1019: &str = r#"<FlexStatementResponse>
            <Status>WARN</Status>
            <ErrorCode>1019</ErrorCode>
            <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
        </FlexStatementResponse>"#;
        assert_eq!(
            classify_get_statement(WARN_UPPER_1019).unwrap(),
            GetStatementOutcome::InProgress
        );
    }

    #[test]
    fn get_statement_in_progress_code_under_fail_status_is_a_flex_error() {
        // ErrorCode 1019 is only retryable under Status=Warn. If IBKR ever returns 1019 under
        // Status=Fail, treat it as a terminal error instead of retrying a failure until the poll
        // budget is exhausted.
        const FAIL_1019: &str = r#"<FlexStatementResponse>
            <Status>Fail</Status>
            <ErrorCode>1019</ErrorCode>
            <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
        </FlexStatementResponse>"#;
        match classify_get_statement(FAIL_1019) {
            Err(IbkrFlexError::Flex { code, .. }) => assert_eq!(code, "1019"),
            other => panic!("expected terminal Flex error, got {other:?}"),
        }
    }

    #[test]
    fn get_statement_1019_without_status_is_a_flex_error() {
        // `Status` deserializes via `#[serde(default)]`, so a 1019 envelope with no `<Status>`
        // element yields `status == ""`. Retryability requires Status=Warn, so a missing status is
        // treated as terminal rather than retried until the poll budget is exhausted.
        const NO_STATUS_1019: &str = r#"<FlexStatementResponse>
            <ErrorCode>1019</ErrorCode>
            <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
        </FlexStatementResponse>"#;
        match classify_get_statement(NO_STATUS_1019) {
            Err(IbkrFlexError::Flex { code, .. }) => assert_eq!(code, "1019"),
            other => panic!("expected terminal Flex error, got {other:?}"),
        }
    }

    #[test]
    fn get_statement_1019_under_success_status_is_a_flex_error() {
        // 1019 is only retryable under Status=Warn. A 1019 reported under Status=Success is a
        // self-contradictory envelope; treat it as terminal rather than retrying it until the poll
        // budget is exhausted.
        const SUCCESS_1019: &str = r#"<FlexStatementResponse>
            <Status>Success</Status>
            <ErrorCode>1019</ErrorCode>
            <ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
        </FlexStatementResponse>"#;
        match classify_get_statement(SUCCESS_1019) {
            Err(IbkrFlexError::Flex { code, .. }) => assert_eq!(code, "1019"),
            other => panic!("expected terminal Flex error, got {other:?}"),
        }
    }

    #[test]
    fn get_statement_terminal_failure_is_a_flex_error() {
        match classify_get_statement(GET_FAIL) {
            Err(IbkrFlexError::Flex { code, message }) => {
                assert_eq!(code, "1020");
                assert!(message.contains("Invalid request"));
            }
            other => panic!("expected Flex error, got {other:?}"),
        }
    }

    #[test]
    fn root_element_name_skips_prolog() {
        assert_eq!(
            root_element_name(GET_READY).as_deref(),
            Some("FlexQueryResponse")
        );
        assert_eq!(
            root_element_name(SEND_SUCCESS).as_deref(),
            Some("FlexStatementResponse")
        );
        assert_eq!(root_element_name("").as_deref(), None);
    }

    #[test]
    fn config_debug_redacts_token() {
        let config =
            IbkrFlexConfig::new("super-secret-token", "987654").expect("valid credentials");
        let debug = format!("{config:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must be redacted"
        );
        assert!(debug.contains("987654"), "query_id is not a secret");
    }

    #[test]
    fn client_debug_hides_token() {
        let client = IbkrFlexClient::new(
            IbkrFlexConfig::new("super-secret-token", "987654").expect("valid credentials"),
        )
        .expect("HTTP client should build");
        let debug = format!("{client:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must not leak via Debug"
        );
        assert!(debug.contains("987654"));
    }

    #[test]
    fn new_rejects_empty_or_whitespace_token() {
        for token in ["", "   ", "\t\n"] {
            match IbkrFlexConfig::new(token, "987654") {
                Err(IbkrFlexError::InvalidCredential(msg)) => assert!(
                    msg.contains("token"),
                    "error should name the `token` field, got: {msg}"
                ),
                other => panic!("expected InvalidCredential for token {token:?}, got: {other:?}"),
            }
        }
    }

    #[test]
    fn new_rejects_empty_or_whitespace_query_id() {
        for query_id in ["", "   ", "\t\n"] {
            match IbkrFlexConfig::new("valid-token", query_id) {
                Err(IbkrFlexError::InvalidCredential(msg)) => assert!(
                    msg.contains("query_id"),
                    "error should name the `query_id` field, got: {msg}"
                ),
                other => {
                    panic!("expected InvalidCredential for query_id {query_id:?}, got: {other:?}")
                }
            }
        }
    }

    #[test]
    fn new_trims_surrounding_whitespace() {
        let config = IbkrFlexConfig::new("  super-secret-token  ", "  987654  ")
            .expect("padded-but-nonempty credentials are valid");
        // Debug redacts the token, so `query_id` is the only observable window into the stored
        // (trimmed) values. Confirm the surrounding whitespace was stripped before storage.
        let debug = format!("{config:?}");
        assert!(
            debug.contains("\"987654\""),
            "query_id should be trimmed to `987654`, got: {debug}"
        );
        assert!(
            !debug.contains("  987654"),
            "surrounding whitespace should not survive into the stored query_id, got: {debug}"
        );
    }

    // ----- status/body interpretation (part (a): read body before branching on status) -----

    #[test]
    fn interpret_send_response_success_parses() {
        let ok = interpret_send_response(reqwest::StatusCode::OK, SEND_SUCCESS, "tok").unwrap();
        assert_eq!(ok.reference_code, "1234567890");
    }

    #[test]
    fn interpret_send_response_flex_error_preserved_under_non_2xx() {
        // An IBKR application error (Fail/1003) that arrives under a non-success HTTP status must
        // still surface as the richer Flex error, not be masked as a raw transport status.
        match interpret_send_response(reqwest::StatusCode::UNAUTHORIZED, SEND_FAIL, "tok") {
            Err(IbkrFlexError::Flex { code, .. }) => assert_eq!(code, "1003"),
            other => panic!("expected Flex error, got {other:?}"),
        }
    }

    #[test]
    fn interpret_send_response_non_flex_body_under_non_2xx_is_http_status() {
        // A proxy/CDN error page (not a Flex envelope) under a non-2xx status becomes HttpStatus,
        // carrying the status + body rather than being discarded (as `error_for_status` did) or
        // surfacing as a misleading XML parse error.
        let body = "<html><body>502 Bad Gateway</body></html>";
        match interpret_send_response(reqwest::StatusCode::BAD_GATEWAY, body, "tok") {
            Err(IbkrFlexError::HttpStatus { status, body }) => {
                assert_eq!(status, 502);
                assert!(body.contains("Bad Gateway"), "diagnostic body preserved");
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[test]
    fn interpret_send_response_garbage_under_2xx_is_parse_error() {
        // A malformed body under a *success* status is a genuine parse failure, not a transport
        // status — the HttpStatus fallback must fire only on non-success statuses.
        assert!(matches!(
            interpret_send_response(reqwest::StatusCode::OK, "<not-flex/>", "tok"),
            Err(IbkrFlexError::Parse(_))
        ));
    }

    #[test]
    fn interpret_poll_response_ready_and_in_progress() {
        assert_eq!(
            interpret_poll_response(reqwest::StatusCode::OK, GET_READY, "tok").unwrap(),
            GetStatementOutcome::Ready
        );
        assert_eq!(
            interpret_poll_response(reqwest::StatusCode::OK, GET_IN_PROGRESS, "tok").unwrap(),
            GetStatementOutcome::InProgress
        );
    }

    #[test]
    fn interpret_poll_response_1019_under_non_2xx_is_still_retryable() {
        // R9 retry-path fix: a 1019 "still generating" that IBKR returns under a non-success HTTP
        // status must remain retryable. Previously `error_for_status` aborted the poll before the
        // body was ever classified, turning a transient generation delay into a hard failure.
        assert_eq!(
            interpret_poll_response(
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                GET_IN_PROGRESS,
                "tok"
            )
            .unwrap(),
            GetStatementOutcome::InProgress
        );
    }

    #[test]
    fn interpret_poll_response_terminal_flex_error_preserved_under_non_2xx() {
        match interpret_poll_response(reqwest::StatusCode::BAD_REQUEST, GET_FAIL, "tok") {
            Err(IbkrFlexError::Flex { code, .. }) => assert_eq!(code, "1020"),
            other => panic!("expected Flex error, got {other:?}"),
        }
    }

    #[test]
    fn interpret_send_response_flex_error_message_is_token_scrubbed() {
        // A genuine Flex error envelope's <ErrorMessage> is server-supplied free text — a
        // misconfigured proxy could reflect the request line (with the `t=` token) into it. The
        // stored Flex error must never carry the credential, the same protection HttpStatus/Parse
        // already have. The Flex code is still preserved so consumers can match on it.
        let token = "super-secret-token";
        let body = format!(
            "<FlexStatementResponse><Status>Fail</Status><ErrorCode>1003</ErrorCode>\
             <ErrorMessage>invalid token for GET /SendRequest?t={token}&amp;q=1&amp;v=3</ErrorMessage>\
             </FlexStatementResponse>"
        );
        match interpret_send_response(reqwest::StatusCode::OK, &body, token) {
            Err(IbkrFlexError::Flex { code, message }) => {
                assert_eq!(code, "1003", "the Flex code is preserved for matching");
                assert!(
                    !message.contains(token),
                    "token must be redacted, got: {message}"
                );
                assert!(message.contains("[REDACTED]"));
            }
            other => panic!("expected Flex, got {other:?}"),
        }
    }

    #[test]
    fn interpret_poll_response_flex_error_message_is_token_scrubbed() {
        // Sibling of the SendRequest case above, for the GetStatement poll path.
        let token = "super-secret-token";
        let body = format!(
            "<FlexStatementResponse><Status>Fail</Status><ErrorCode>1020</ErrorCode>\
             <ErrorMessage>bad reference for GET /GetStatement?t={token}&amp;q=1&amp;v=3</ErrorMessage>\
             </FlexStatementResponse>"
        );
        match interpret_poll_response(reqwest::StatusCode::OK, &body, token) {
            Err(IbkrFlexError::Flex { code, message }) => {
                assert_eq!(code, "1020");
                assert!(
                    !message.contains(token),
                    "token must be redacted, got: {message}"
                );
                assert!(message.contains("[REDACTED]"));
            }
            other => panic!("expected Flex, got {other:?}"),
        }
    }

    #[test]
    fn interpret_send_response_flex_error_code_is_token_scrubbed() {
        // `scrub_flex_error` runs `code` through the same redact-before-bound path as `message`, so a
        // token reflected into a `<ErrorCode>` (a no-op for the real numeric codes IBKR returns, but
        // the field is still server-supplied) must not survive into the stored error either.
        let token = "super-secret-token";
        let body = format!(
            "<FlexStatementResponse><Status>Fail</Status>\
             <ErrorCode>bogus t={token}</ErrorCode>\
             <ErrorMessage>rejected</ErrorMessage></FlexStatementResponse>"
        );
        match interpret_send_response(reqwest::StatusCode::OK, &body, token) {
            Err(IbkrFlexError::Flex { code, message }) => {
                assert!(
                    !code.contains(token),
                    "token must be redacted from code, got: {code}"
                );
                assert!(code.contains("[REDACTED]"));
                assert_eq!(message, "rejected");
            }
            other => panic!("expected Flex, got {other:?}"),
        }
    }

    #[test]
    fn flex_error_message_is_bounded() {
        // A `Flex` message derives from an unbounded 2xx `response.text()` read, so an adversarial or
        // buggy server returning a well-formed Flex error envelope with a huge <ErrorMessage> must be
        // bounded like HttpStatus/Parse, not stored whole. Multi-byte chars confirm the cap lands on
        // a char boundary rather than panicking.
        let body = format!(
            "<FlexStatementResponse><Status>Fail</Status><ErrorCode>1003</ErrorCode>\
             <ErrorMessage>{}</ErrorMessage></FlexStatementResponse>",
            "€".repeat(1000) // 3000 bytes, exceeds the 1024-byte cap
        );
        match interpret_send_response(reqwest::StatusCode::OK, &body, "tok") {
            Err(IbkrFlexError::Flex { message, .. }) => assert!(
                message.len() <= MAX_ERROR_BODY_BYTES,
                "message bounded to the cap, got {} bytes",
                message.len()
            ),
            other => panic!("expected Flex, got {other:?}"),
        }
    }

    #[test]
    fn interpret_poll_response_non_flex_body_under_non_2xx_is_http_status() {
        match interpret_poll_response(
            reqwest::StatusCode::GATEWAY_TIMEOUT,
            "upstream request timed out",
            "tok",
        ) {
            Err(IbkrFlexError::HttpStatus { status, body }) => {
                assert_eq!(status, 504);
                assert!(body.contains("timed out"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    /// Build a `reqwest::Response` directly, with no socket and no mock server, via reqwest's
    /// `From<http::Response<_>>` impl. This is what makes the transport-layer read testable without
    /// relaxing the client's `https_only(true)` guard — the Flex token rides in the request URL, so
    /// a plaintext test server would mean building a client that is not the production one.
    fn synthetic_response(status: u16, body: &'static str) -> reqwest::Response {
        reqwest::Response::from(
            http::Response::builder()
                .status(status)
                .body(body)
                .expect("status and body are valid"),
        )
    }

    #[tokio::test]
    async fn read_status_and_body_keeps_a_1019_body_under_a_non_2xx_status() {
        // The regression this pins, and the one no `interpret_*` test can catch: reverting
        // `get_with_query` to `error_for_status()?.text().await?` discards the body on a non-2xx,
        // so the retryable 1019 envelope never reaches `interpret_poll_response` and the poll
        // aborts instead of retrying. Here the body must survive the non-success status intact.
        let (status, body) = read_status_and_body(synthetic_response(503, GET_IN_PROGRESS))
            .await
            .expect("a non-success status must still yield its body");

        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            classify_get_statement(&body).expect("body is a Flex envelope"),
            GetStatementOutcome::InProgress,
            "a 1019 arriving under a non-2xx status must still classify as retryable"
        );
    }

    #[tokio::test]
    async fn read_status_and_body_reads_a_success_body_in_full() {
        // The success path is the statement payload and must not be routed through the error-path
        // cap, which would silently truncate a large `<FlexQueryResponse>`.
        let (status, body) = read_status_and_body(synthetic_response(200, GET_READY))
            .await
            .expect("success body reads");

        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(body, GET_READY);
    }

    #[test]
    fn http_status_body_is_token_scrubbed() {
        // A misconfigured proxy could reflect the request URL (with the `t=` token) into its error
        // page; the stored HttpStatus body must never carry the credential.
        let token = "super-secret-token";
        let body = format!("400 Bad Request: GET /SendRequest?t={token}&q=1&v=3");
        match interpret_send_response(reqwest::StatusCode::BAD_REQUEST, &body, token) {
            Err(IbkrFlexError::HttpStatus { body, .. }) => {
                assert!(!body.contains(token), "token must be redacted, got: {body}");
                assert!(body.contains("[REDACTED]"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[test]
    fn parse_message_is_token_scrubbed_under_2xx() {
        // The `HttpStatus` path is covered above; this pins the sibling `Parse` path, which is only
        // reachable under a *success* status and so never passes through `http_status_error`. A
        // deserialiser message embeds fragments of the offending input, so a body that reflected the
        // request line must not carry the credential into the stored error either. Both interpreters
        // route their `Parse` arm through `finish_parse_error`; assert both.
        let token = "super-secret-token";
        // The root-element check formats the tag name verbatim into the message, so naming the root
        // after the token is the shortest body that reflects it into a `Parse`.
        let body = format!("<{token}/>");

        match interpret_send_response(reqwest::StatusCode::OK, &body, token) {
            Err(IbkrFlexError::Parse(message)) => {
                assert!(
                    !message.contains(token),
                    "token must be redacted, got: {message}"
                );
                assert!(message.contains("[REDACTED]"));
            }
            other => panic!("expected Parse, got {other:?}"),
        }

        match interpret_poll_response(reqwest::StatusCode::OK, &body, token) {
            Err(IbkrFlexError::Parse(message)) => {
                assert!(
                    !message.contains(token),
                    "token must be redacted, got: {message}"
                );
                assert!(message.contains("[REDACTED]"));
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn parse_message_is_bounded_under_2xx() {
        // `Parse` messages derive from an unbounded 2xx `response.text()` read, so — unlike the
        // `HttpStatus` path — `sanitize_error_body`'s trailing truncate is the *only* thing bounding
        // them. Multi-byte chars confirm the cap lands on a char boundary rather than panicking.
        let body = format!("<{}/>", "€".repeat(1000)); // 3000 bytes of tag name, exceeds the cap
        match interpret_send_response(reqwest::StatusCode::OK, &body, "tok") {
            Err(IbkrFlexError::Parse(message)) => {
                // Guard against a vacuous pass: the short "empty or unreadable" fallback is under
                // the cap regardless, so confirm this really is the input-reflecting message.
                assert!(
                    message.starts_with("unexpected"),
                    "fixture must reach the root-element path, got: {message}"
                );
                assert!(
                    message.len() <= MAX_ERROR_BODY_BYTES,
                    "message bounded to the cap, got {} bytes",
                    message.len()
                );
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn http_status_body_is_bounded_on_char_boundary() {
        // A large body is truncated to the cap without splitting a multi-byte char (`€` is 3 bytes,
        // 1024 is not a multiple of 3, so a naive byte-slice would panic on an invalid boundary).
        let body = "€".repeat(1000); // 3000 bytes, exceeds the 1024-byte cap
        match interpret_send_response(reqwest::StatusCode::BAD_GATEWAY, &body, "tok") {
            Err(IbkrFlexError::HttpStatus { body, .. }) => {
                assert!(
                    body.len() <= MAX_ERROR_BODY_BYTES,
                    "body bounded to the cap"
                );
                assert!(
                    body.chars().all(|c| c == '€'),
                    "truncation must land on a char boundary (valid UTF-8)"
                );
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[test]
    fn http_status_body_scrubs_token_straddling_the_cap() {
        // Regression: the token must be redacted even when it straddles the MAX_ERROR_BODY_BYTES cap
        // in the *original* body. Redacting after bounding (the previous order) would leave only a
        // prefix of the token in the bounded slice, which the full-token match then misses — leaking
        // the fragment. Redacting first also keeps the result within the cap (`[REDACTED]` is longer
        // than a short token, so replacing after bounding could overrun it).
        let token = "SECRETTOKEN1234567890"; // 21 bytes
        let prefix = "x".repeat(MAX_ERROR_BODY_BYTES - 9); // token starts 9 bytes before the cap
        let body = format!("{prefix}{token}{}", "y".repeat(100)); // > cap; token spans the boundary
        match interpret_send_response(reqwest::StatusCode::BAD_REQUEST, &body, token) {
            Err(IbkrFlexError::HttpStatus { body, .. }) => {
                assert!(
                    !body.contains(token),
                    "full token must be redacted, got: {body}"
                );
                assert!(
                    !body.contains("SECRET"),
                    "no token fragment may survive the boundary, got: {body}"
                );
                assert!(
                    body.len() <= MAX_ERROR_BODY_BYTES,
                    "redaction must not push the body back over the cap, len = {}",
                    body.len()
                );
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[test]
    fn corporate_action_parse_scrubs_token_straddling_the_cap() {
        // Sibling of `http_status_body_scrubs_token_straddling_the_cap`, for the path that reaches
        // `Parse` through `fetch_corporate_actions`. Regression: `parse_corporate_actions` bounds its
        // own message, so scrubbing its *result* (the previous shape) redacted an already-truncated
        // string — a token straddling the cap survived as an unmatched prefix fragment, because both
        // truncations used MAX_ERROR_BODY_BYTES and left no protective gap. The token is now threaded
        // into the parse so redaction runs while the message is still unbounded.
        let token = "SECRETTOKEN1234567890"; // 21 bytes
        // Sweep the token across the cap rather than pinning one offset: the straddling position
        // depends on the byte length of the message's fixed prefix, which is not a contract and
        // shifts if the wording is ever edited. `straddled` guards against that drift silently
        // emptying this test — without it, a reworded message could move the boundary outside the
        // swept range and every assertion below would pass vacuously, losing the coverage that is
        // the entire reason this test exists.
        let mut straddled = false;
        for pad in (MAX_ERROR_BODY_BYTES - 80)..(MAX_ERROR_BODY_BYTES - 40) {
            let xml = format!("<{}{token}/>", "x".repeat(pad));
            // The unscrubbed message the same input would produce. A genuine straddle shows up as a
            // non-empty *proper prefix* of the token left at its end — distinct from the token
            // landing entirely past the cap (nothing of it remains) or entirely before it (all of it
            // remains), neither of which exercises the boundary.
            let Err(IbkrFlexError::Parse(plain)) =
                corporate_action::parse_corporate_actions_scrubbed(&xml, None)
            else {
                panic!("expected Parse from the token-less parse at pad {pad}");
            };
            straddled |= (1..token.len()).any(|cut| plain.ends_with(&token[..cut]));

            match corporate_action::parse_corporate_actions_scrubbed(&xml, Some(token)) {
                Err(IbkrFlexError::Parse(message)) => {
                    assert!(
                        !message.contains(token),
                        "full token must be redacted at pad {pad}"
                    );
                    // A straddling leak surfaces as a token *prefix* left at the truncation
                    // boundary, so check every prefix length rather than one fixed substring — a
                    // 5-byte fragment is as much a credential leak as a 6-byte one.
                    for cut in 1..token.len() {
                        assert!(
                            !message.ends_with(&token[..cut]),
                            "a {cut}-byte token fragment survived the boundary at pad {pad}, got: {message}"
                        );
                    }
                    assert!(
                        message.len() <= MAX_ERROR_BODY_BYTES,
                        "redaction must not push the message over the cap at pad {pad}, len = {}",
                        message.len()
                    );
                }
                other => panic!("expected Parse, got {other:?}"),
            }
        }
        assert!(
            straddled,
            "the swept range no longer places the token across the {MAX_ERROR_BODY_BYTES}-byte cap \
             — widen it, or this test proves nothing"
        );
    }

    #[test]
    fn corporate_action_parse_without_a_token_is_still_bounded() {
        // The public `parse_corporate_actions` has no credential in scope, so it only bounds. Pins
        // that routing both callers through one finaliser did not drop the bound on the `None` arm.
        let xml = format!("<{}/>", "€".repeat(1000)); // 3000 bytes of tag name, exceeds the cap
        match parse_corporate_actions(&xml) {
            Err(IbkrFlexError::Parse(message)) => assert!(
                message.len() <= MAX_ERROR_BODY_BYTES,
                "len = {}",
                message.len()
            ),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn max_error_body_bytes_is_the_documented_cap() {
        // Pinned against a literal because the public rustdoc on `IbkrFlexError` states this bound as
        // "1 KiB" in prose (it cannot link a private constant under
        // `deny(rustdoc::private_intra_doc_links)`). Changing the constant without the docs would
        // otherwise drift silently.
        assert_eq!(MAX_ERROR_BODY_BYTES, 1024);
    }

    #[test]
    fn flex_poll_policy_default_is_the_documented_budget() {
        // Asserted against literals, not against the constants the impl already reads: comparing
        // `Default::default()` to a struct built from those same constants is a tautology that
        // holds for any value, and — since `POLL_INITIAL_DELAY` and `POLL_INTERVAL` are both 5s —
        // would not even catch the two being transposed. These literals are the budget the public
        // rustdoc promises (~65s total), so a change here is a deliberate, visible contract change.
        assert_eq!(
            FlexPollPolicy::default(),
            FlexPollPolicy {
                initial_delay: Duration::from_secs(5),
                interval: Duration::from_secs(5),
                max_attempts: 12,
            }
        );
    }

    #[test]
    fn sanitize_error_body_scrubs_reqwests_wire_encoded_token() {
        // A token with characters that encode differently on the wire (space -> `+`; `+ / ~` -> `%..`).
        // If a proxy reflects the *encoded* request line into its error page, the raw-token scrub
        // alone would miss it. This pins two properties: (1) `byte_serialize` produces exactly what
        // reqwest's `.query()` puts on the wire — so the scrub can't silently drift from the real
        // encoding if reqwest/url change — and (2) that encoded form is redacted from the body.
        let token = "abc def+g/h~i";

        // What reqwest actually serialises onto the query string (`build()` only — nothing is sent).
        let request = reqwest::Client::new()
            .get("https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement")
            .query(&[("t", token), ("q", "1234567890"), ("v", FLEX_VERSION)])
            .build()
            .expect("request builds without sending");
        let wire_query = request.url().query().expect("query present");

        let wire_form: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
        assert_ne!(
            wire_form, token,
            "test token must actually encode to something different, else it proves nothing"
        );
        assert!(
            wire_query.contains(&format!("t={wire_form}")),
            "byte_serialize must match reqwest's actual query encoding, else the wire-form scrub \
             silently stops matching; query = {wire_query}, wire_form = {wire_form}"
        );

        // A proxy that echoes the received request line into its error page reflects the encoded form.
        let reflected = format!("502 Bad Gateway while proxying GET /GetStatement?{wire_query}");
        let scrubbed = sanitize_error_body(&reflected, token);
        assert!(
            !scrubbed.contains(&wire_form),
            "wire-encoded token must be redacted, got: {scrubbed}"
        );
        assert!(scrubbed.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn poll_until_ready_zero_max_attempts_times_out_without_polling() {
        // `max_attempts == 0` is the documented "no polling requested" case: it must return
        // immediately with `PollTimeout { attempts: 0 }` and issue zero GetStatement calls (and skip
        // the initial delay). Zero-duration policy keeps the test instant.
        let policy = FlexPollPolicy {
            initial_delay: Duration::ZERO,
            interval: Duration::ZERO,
            max_attempts: 0,
        };
        let calls = std::cell::Cell::new(0u32);
        let result = poll_until_ready(&policy, "tok", async || {
            calls.set(calls.get() + 1);
            Ok((reqwest::StatusCode::OK, GET_READY.to_owned()))
        })
        .await;
        assert_eq!(calls.get(), 0, "max_attempts == 0 must not issue any poll");
        assert!(matches!(
            result,
            Err(IbkrFlexError::PollTimeout { attempts: 0 })
        ));
    }

    #[tokio::test]
    async fn poll_until_ready_returns_as_soon_as_statement_is_ready() {
        // The loop must stop at the first Ready response, not run the full budget.
        let policy = FlexPollPolicy {
            initial_delay: Duration::ZERO,
            interval: Duration::ZERO,
            max_attempts: 5,
        };
        let calls = std::cell::Cell::new(0u32);
        let result = poll_until_ready(&policy, "tok", async || {
            calls.set(calls.get() + 1);
            // In progress on the first poll, ready on the second.
            let body = if calls.get() < 2 {
                GET_IN_PROGRESS
            } else {
                GET_READY
            };
            Ok((reqwest::StatusCode::OK, body.to_owned()))
        })
        .await;
        assert_eq!(calls.get(), 2, "must stop polling once Ready is seen");
        assert_eq!(result.expect("ready"), GET_READY);
    }

    #[tokio::test]
    async fn poll_until_ready_exhausts_budget_and_reports_attempt_count() {
        // Every poll reports in-progress: the loop must run exactly `max_attempts` times — proving
        // the `1..=max_attempts` bound and the `attempt < max_attempts` sleep guard don't over- or
        // under-run — and surface that count in `PollTimeout`.
        let policy = FlexPollPolicy {
            initial_delay: Duration::ZERO,
            interval: Duration::ZERO,
            max_attempts: 3,
        };
        let calls = std::cell::Cell::new(0u32);
        let result = poll_until_ready(&policy, "tok", async || {
            calls.set(calls.get() + 1);
            Ok((reqwest::StatusCode::OK, GET_IN_PROGRESS.to_owned()))
        })
        .await;
        assert_eq!(calls.get(), 3, "must poll exactly max_attempts times");
        assert!(matches!(
            result,
            Err(IbkrFlexError::PollTimeout { attempts: 3 })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn poll_until_ready_sleeps_initial_delay_first_then_interval_between_polls() {
        // The zero-duration tests above prove call counts and outcomes but not the *timing* wiring:
        // that `initial_delay` is awaited **before** the first poll and `interval` between subsequent
        // polls. Under `start_paused`, Tokio advances virtual time whenever the runtime is idle on a
        // timer, so each `sleep` completes deterministically and we can record the clock at every poll
        // without real sleeping.
        let policy = FlexPollPolicy {
            initial_delay: Duration::from_secs(5),
            interval: Duration::from_secs(2),
            max_attempts: 3,
        };
        let start = tokio::time::Instant::now();
        let elapsed_at_poll = std::cell::RefCell::new(Vec::<Duration>::new());
        let result = poll_until_ready(&policy, "tok", async || {
            elapsed_at_poll.borrow_mut().push(start.elapsed());
            // Always in-progress so the loop runs the full budget, exercising every inter-poll sleep.
            Ok((reqwest::StatusCode::OK, GET_IN_PROGRESS.to_owned()))
        })
        .await;
        assert!(matches!(
            result,
            Err(IbkrFlexError::PollTimeout { attempts: 3 })
        ));

        // Poll 1 at t=5s proves `initial_delay` elapsed *before* the first poll (not t=0); polls 2 and
        // 3 one `interval` (2s) apart prove the between-poll sleep. A regression that polled before
        // sleeping, or that dropped a sleep, would shift these timestamps.
        assert_eq!(
            *elapsed_at_poll.borrow(),
            vec![
                Duration::from_secs(5),
                Duration::from_secs(7),
                Duration::from_secs(9),
            ],
        );
    }
}
