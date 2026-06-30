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
//! [`IbkrFlexError::Flex`]; exhausting the poll budget surfaces as [`IbkrFlexError::PollTimeout`].
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

/// Flex `ErrorCode` meaning "statement generation in progress, try again shortly" — the only
/// non-success status that should be retried rather than surfaced as an error.
const GENERATION_IN_PROGRESS_CODE: &str = "1019";

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

    /// Transport-level HTTP error (connection, timeout, or non-2xx status).
    ///
    /// The inner [`reqwest::Error`] **never carries the request URL.** The Flex token is
    /// transmitted as `t=<TOKEN>` in every request URL, and `reqwest::Error`'s `Display` and
    /// `Debug` both embed that URL verbatim when present. The `From<reqwest::Error>` conversion
    /// for this type always strips it via [`reqwest::Error::without_url`] before the error is
    /// stored, covering every reqwest site that attaches a URL (`send`, `error_for_status`, body
    /// decode); `Client::build()` errors carry no URL, so the strip is a no-op for them. The
    /// `source()` chain (hyper/IO transport errors) is preserved and does not independently carry
    /// the URL.
    ///
    /// Safe to log or display without credential scrubbing.
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),

    /// The Flex service returned a non-success status (e.g. invalid token, throttled, expired).
    #[error("Flex service error ({code}): {message}")]
    Flex {
        /// The Flex `ErrorCode` (empty if the response carried none).
        code: String,
        /// The Flex `ErrorMessage` (or a synthesised description).
        message: String,
    },

    /// The statement was still generating after [`POLL_MAX_ATTEMPTS`] poll attempts.
    #[error("Flex statement not ready after {attempts} poll attempts")]
    PollTimeout {
        /// Number of poll attempts made before giving up. Currently always [`POLL_MAX_ATTEMPTS`],
        /// since this error fires only after every attempt reported the statement still generating.
        attempts: u32,
    },

    /// The statement or status XML could not be parsed.
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
/// `?` on a `reqwest::Error` — at the request sites *and* any path added later — routes through here.
/// [`reqwest::Error::without_url`] sets the stored URL to `None` (a no-op for `Client::build()`
/// errors, which carry none) while preserving the diagnostic kind/status and `source()` chain.
impl From<reqwest::Error> for IbkrFlexError {
    fn from(error: reqwest::Error) -> Self {
        IbkrFlexError::Http(error.without_url())
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
    /// `token` and `query_id` are stored **verbatim** — they are not trimmed or validated. Leading
    /// or trailing whitespace will be sent in the `t=`/`q=` query parameters and rejected by IBKR
    /// with a confusing `1003` "invalid token" at fetch time. Pass already-clean values, or use
    /// [`from_env`](Self::from_env), which trims and rejects empty values.
    pub fn new(token: impl Into<String>, query_id: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            query_id: query_id.into(),
        }
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
        Ok(Self::new(token, query_id))
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
}

impl fmt::Debug for IbkrFlexClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately omits the token (a credential) and the `reqwest::Client`.
        f.debug_struct("IbkrFlexClient")
            .field("query_id", &self.query_id)
            .field("base_url", &self.base_url)
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
            // returned unfollowed and surfaces downstream as a body-parse error — never as a silent
            // scheme downgrade. (The content-layer scheme check on the SendRequest poll URL cannot
            // intercept redirects reqwest would otherwise follow, which is why these guards exist.)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            token: config.token,
            query_id: config.query_id,
            base_url: FLEX_BASE_URL.to_owned(),
        })
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
    /// Issues SendRequest, then polls GetStatement (up to [`POLL_MAX_ATTEMPTS`] times, sleeping
    /// [`POLL_INTERVAL`] between attempts) until the statement is ready.
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
    /// - [`IbkrFlexError::PollTimeout`] if the statement is still generating after the poll budget.
    /// - [`IbkrFlexError::Parse`] if a response cannot be parsed.
    pub async fn fetch_statement_xml(&self) -> Result<String, IbkrFlexError> {
        let send_url = format!("{}/SendRequest", self.base_url);
        debug!("Requesting IBKR Flex statement generation");

        // Plain `?` is safe: `IbkrFlexError`'s `From<reqwest::Error>` strips the token-bearing
        // request URL from every error (see the impl above).
        let body = self
            .http
            .get(&send_url)
            .query(&[
                ("t", self.token.as_str()),
                ("q", self.query_id.as_str()),
                ("v", FLEX_VERSION),
            ])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let SendRequestOk {
            reference_code,
            url,
        } = parse_send_request_response(&body)?;

        for attempt in 1..=POLL_MAX_ATTEMPTS {
            // Plain `?` as above: the `From` impl strips the token-bearing URL from any reqwest error.
            let body = self
                .http
                .get(&url)
                .query(&[
                    ("t", self.token.as_str()),
                    ("q", reference_code.as_str()),
                    ("v", FLEX_VERSION),
                ])
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;

            match classify_get_statement(&body)? {
                GetStatementOutcome::Ready => return Ok(body),
                GetStatementOutcome::InProgress => {
                    debug!(
                        attempt,
                        max = POLL_MAX_ATTEMPTS,
                        "Flex statement still generating"
                    );
                    if attempt < POLL_MAX_ATTEMPTS {
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                }
            }
        }

        Err(IbkrFlexError::PollTimeout {
            attempts: POLL_MAX_ATTEMPTS,
        })
    }

    /// Fetch the statement and parse its Corporate Actions section into faithful records.
    ///
    /// Convenience over [`fetch_statement_xml`](Self::fetch_statement_xml) +
    /// [`parse_corporate_actions`]. Returns **all** reorg rows; filtering to splits is the caller's
    /// job. See [`corporate_action`] for the record contract and limitations.
    ///
    /// # Errors
    ///
    /// Same as [`fetch_statement_xml`](Self::fetch_statement_xml).
    pub async fn fetch_corporate_actions(
        &self,
    ) -> Result<Vec<IbkrFlexCorporateAction>, IbkrFlexError> {
        let xml = self.fetch_statement_xml().await?;
        parse_corporate_actions(&xml)
    }
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

/// Read the name of the first (root) element of an XML document, ignoring the prolog/comments.
///
/// Returns a [`SmolStr`] so the expected root names (`FlexQueryResponse`, `FlexStatementResponse`)
/// are compared without a heap allocation (both fit `SmolStr`'s inline buffer).
fn root_element_name(xml: &str) -> Option<SmolStr> {
    let mut reader = Reader::from_str(xml);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                return Some(SmolStr::new(String::from_utf8_lossy(e.name().as_ref())));
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
        let config = IbkrFlexConfig::new("super-secret-token", "987654");
        let debug = format!("{config:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must be redacted"
        );
        assert!(debug.contains("987654"), "query_id is not a secret");
    }

    #[test]
    fn client_debug_hides_token() {
        let client = IbkrFlexClient::new(IbkrFlexConfig::new("super-secret-token", "987654"))
            .expect("HTTP client should build");
        let debug = format!("{client:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must not leak via Debug"
        );
        assert!(debug.contains("987654"));
    }
}
