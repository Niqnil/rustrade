//! Authenticated, rationed REST transport behind [`LseClient`](super::client::LseClient).
//! Internal: the client is the public surface.
//!
//! [`LseHttpCore`] holds the `x-api-key` header, the `LSE_API_KEY` variable behind it, the
//! `User-Agent` the provider's CDN demands, the redirect policy that keeps the key off a
//! server-supplied host, the request rationing, the mapping from a non-success status to an
//! [`LseError`], and the verified download of a file answer.
//!
//! # ⚠️ One core is one ration pool
//!
//! Rationing belongs to a core, so two clients built separately ration **independently** and their
//! aggregate rate is the sum of theirs. The provider's limits are per key, not per client: a
//! consumer driving several clients with one key should clone one client instead.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_refusal};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};
use std::{env, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::{Instant, sleep_until},
};
use tracing::debug;

/// Header carrying the API key.
const API_KEY_HEADER: &str = "x-api-key";

/// Environment variable read by [`LseHttpCore::from_env`].
///
/// One key authenticates every surface, the websocket included, so all of them read this variable.
pub(crate) const API_KEY_ENV: &str = "LSE_API_KEY";

/// Total deadline for a JSON request: connect, send, and read the whole body.
///
/// Sound for the JSON endpoints, whose bodies are a page of rows at most. It is **not** sound for
/// a file answer, which is why [`get_file_into`](LseHttpCore::get_file_into) overrides it per
/// request — `reqwest` applies this as a total deadline "from when the request starts connecting
/// until the response body has finished", so a large transfer would abort mid-body however healthy
/// the connection.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-read timeout, applied to every request including a streaming download.
///
/// Resets after each successful read, so it detects a *stalled* connection without bounding total
/// transfer time — the correct tool for a body whose length is not known up front.
///
/// It also bounds the wait for the response headers, and a file answer sends none until the
/// provider has built the whole file. That build is short: exports at 87% and 98% of the
/// 2,500,000-row cap, as Parquet and as CSV, each answered within 1.1 s (measured 2026-11-28). A
/// file taking longer than this would fail as [`LseError::Http`] after its rows were charged.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// `User-Agent` sent on every request.
///
/// Set explicitly because the provider's hosts have sat behind a CDN that rejects some agents
/// outright — measured on its earlier hosts: a request carrying `Python-urllib`'s default agent
/// received `403` with `error code: 1010`, never reaching the API. `reqwest` sends no `User-Agent`
/// by default, which leaves that outcome to the CDN's discretion rather than to anything this crate
/// controls. An edge rejection is also invisible to the provider's allowance accounting, so it
/// fails in a way that looks nothing like an API error.
const USER_AGENT: &str = concat!("rustrade-data/", env!("CARGO_PKG_VERSION"));

/// Default minimum delay between the *starts* of two requests.
///
/// One second: the free plan's allowance of 60 calls per minute, which `/v3/usage` reports as
/// `calls_per_minute`. Every call counts against it, refused ones included. A paid plan's figure
/// is not documented; a caller on a larger allowance can lower the pace. This is *proactive
/// courtesy only* — it never inspects a `429`, never retries, and never adapts.
pub(crate) const DEFAULT_PACE: Duration = Duration::from_secs(1);

/// Default ceiling on requests in flight at once.
///
/// One, because the provider holds **one download open per key** on every plan and refuses a
/// second with `429 connections-exhausted` (or `exports-busy`). A JSON call does not take that
/// slot, so a caller issuing only JSON calls may raise it with `with_concurrency`.
pub(crate) const DEFAULT_CONCURRENCY: usize = 1;

/// Total deadline for a file download: connect, send, and read the whole body.
///
/// The provider caps one export at 2,500,000 rows, a few tens of megabytes as Parquet and a few
/// hundred as CSV, so ten minutes leaves room for a slow link while bounding how long a trickling
/// transfer can hold the key's single download slot. A *stalled* transfer is caught much sooner by
/// [`READ_TIMEOUT`].
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// Header carrying the SHA-256 of a file answer, as hex.
const CHECKSUM_HEADER: &str = "x-checksum-sha256";

/// Header carrying the rows the provider charged an export against the plan.
const ROWS_HEADER: &str = "x-export-rows";

/// Header carrying the provider's own identifier for an export, for a support request.
const EXPORT_ID_HEADER: &str = "x-export-id";

/// Header saying whether an export's prices are adjusted for splits (`1`) or as published (`0`).
const ADJUSTED_HEADER: &str = "x-lse3-adjusted";

/// What the provider's headers said about a verified file answer.
///
/// Built only after every byte has arrived and matched the declared length and SHA-256.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileHead {
    /// The body's length in bytes, as received.
    pub(crate) bytes: u64,
    /// The body's SHA-256, lowercase hex, as received and declared.
    pub(crate) sha256: String,
    /// `x-export-rows`: the rows charged, which for aggregated candles is not the file's row count.
    pub(crate) rows_charged: Option<u64>,
    /// `x-export-id`.
    pub(crate) export_id: Option<String>,
    /// The `filename` of `Content-Disposition`.
    pub(crate) file_name: Option<String>,
    /// `x-lse3-adjusted`, read as a boolean.
    pub(crate) adjusted: Option<bool>,
}

/// Shared gate every request passes through: bounds concurrency, then spaces request starts.
///
/// Held behind an [`Arc`] so that all clones of one client — and therefore every concurrent stream
/// built from one — queue at the *same* gate.
///
/// # Why this cannot be per-request state
/// Pacing a single paged fetch is a claim about that fetch only. A multi-instrument replay such as
/// [`replay_candles`](super::backtest::replay_candles) drives N fetches concurrently, so N
/// independently-paced streams produce an aggregate rate of N per `pace` — which is exactly what
/// [`DEFAULT_PACE`]'s calls-per-minute derivation says must not happen, and N in-flight
/// requests against the provider's one download per key. Both bounds therefore belong to the
/// client, not to any one call site, so that they hold for every entry point without the caller
/// re-deriving them.
#[derive(Debug)]
struct RequestGate {
    /// Configured in-flight ceiling. Retained separately because [`Semaphore`] reports only the
    /// permits *currently* available, which says nothing about the limit under load.
    concurrency: usize,
    /// One permit per allowed in-flight request.
    permits: Semaphore,
    /// Earliest instant at which the next request may start.
    ///
    /// Fair (FIFO) locking gives each waiter a distinct, increasing slot, so `n` requests leave at
    /// `pace` intervals rather than all reading the same "now".
    next_slot: Mutex<Instant>,
}

impl RequestGate {
    fn new(concurrency: usize) -> Self {
        Self {
            // A gate of zero permits would park every request forever. Clamping (rather than
            // erroring) keeps `with_concurrency` infallible for what is plainly a caller slip.
            concurrency: concurrency.max(1),
            permits: Semaphore::new(concurrency.max(1)),
            next_slot: Mutex::new(Instant::now()),
        }
    }

    /// Wait for a slot. The returned permit must be held for the whole request, body included.
    ///
    /// # The permit is acquired *before* the pacing wait, deliberately
    /// A caller therefore holds a permit while sleeping to its slot, so `concurrency` bounds
    /// **admitted** callers rather than requests actually in flight — with a `pace` of one second
    /// and a concurrency raised to 2, the steady state is one request on the wire and one waiting,
    /// not two on the wire. Acquiring in the other order would let an unbounded number of callers
    /// queue on the pacing mutex and then release together, which is the burst the concurrency
    /// limit exists to prevent. The cost is that `concurrency` reads as a slightly stricter limit
    /// than its name suggests; the benefit is that neither bound can be exceeded.
    ///
    /// `None` is returned only if the semaphore were closed, which cannot happen: it is private to
    /// this type and nothing closes it. Degrading to an unpaced-but-still-spaced request beats
    /// panicking a caller's run over an unreachable condition, and this crate denies
    /// `clippy::unwrap_used`.
    async fn enter(&self, pace: Duration) -> Option<SemaphorePermit<'_>> {
        let permit = self.permits.acquire().await.ok();

        let slot = {
            let mut next_slot = self.next_slot.lock().await;
            // `max(now)` so an idle client accrues no credit: a burst after a quiet period starts
            // immediately and then spaces out, instead of firing a backlog of "owed" requests.
            let slot = (*next_slot).max(Instant::now());
            *next_slot = slot + pace;
            slot
        };

        sleep_until(slot).await;

        permit
    }
}

/// Authenticated, rationed transport against the London Strategic Edge REST host.
///
/// Holds one configured [`reqwest::Client`] (auth header, agent, timeouts, redirect policy), the
/// base URL, and the [gate](RequestGate) that rations requests. See the
/// [module documentation](self) for what clones share and what separately built cores do not.
#[derive(Clone)]
pub(crate) struct LseHttpCore {
    http: reqwest::Client,
    base_url: String,
    pace: Duration,
    gate: Arc<RequestGate>,
}

impl std::fmt::Debug for LseHttpCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports.
        f.debug_struct("LseHttpCore")
            .field("base_url", &self.base_url)
            .field("pace", &self.pace)
            .field("concurrency", &self.gate.concurrency)
            .finish_non_exhaustive()
    }
}

impl LseHttpCore {
    /// Build a core for `base_url` with an explicit API key.
    ///
    /// # Redirects are not followed
    /// The key rides every request as a client-wide `x-api-key` default header, and reqwest's
    /// cross-host header stripping covers `Authorization`, `Cookie`, `Cookie2`,
    /// `Proxy-Authorization` and `WWW-Authenticate` — **not** a custom header. Under reqwest's
    /// default [`Policy::limited(10)`](reqwest::redirect::Policy::limited) a single server-issued
    /// 302 would therefore re-issue the request, live key attached, against whatever host the
    /// provider named. A download endpoint is exactly the shape that hands off to object storage,
    /// and in a CI workflow that key is a repository secret.
    ///
    /// [`Policy::none`](reqwest::redirect::Policy::none) makes the "this client only ever requests
    /// URLs it builds itself" invariant structural rather than a property of the provider's
    /// current behaviour — a followed redirect *is* a server-supplied URL. An unexpected 3xx
    /// surfaces as a typed error instead of being followed, and it applies **wholesale**: a
    /// same-origin 301/308 (trailing-slash normalisation by a proxy or CDN) is surfaced too, so a
    /// base URL set by a caller must serve responses directly.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidCredential`] if the key cannot be encoded as an HTTP header
    /// value (e.g. non-ASCII bytes), or [`LseError::Http`] if the HTTP client cannot be built.
    pub(crate) fn new(api_key: &str, base_url: impl Into<String>) -> Result<Self, LseError> {
        let mut headers = HeaderMap::new();
        let mut key = HeaderValue::from_str(api_key)
            .map_err(|error| LseError::InvalidCredential(format!("invalid API key: {error}")))?;
        // Marks the value as sensitive so `HeaderMap`'s own `Debug` prints it redacted, closing the
        // leak path that bypasses this type's `Debug`.
        key.set_sensitive(true);
        headers.insert(API_KEY_HEADER, key);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent(USER_AGENT)
            .timeout(REQUEST_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            // The key above is a client-wide default header, and reqwest strips only
            // `Authorization`, `Cookie`, `Cookie2`, `Proxy-Authorization` and `WWW-Authenticate`
            // when a redirect crosses hosts -- a custom `x-api-key` survives the hop.
            // `set_sensitive` redacts `Debug` output and does nothing here. Without this line the
            // default `Policy::limited(10)` would forward the live key to any host the provider
            // names, which is precisely the invariant a caller-supplied base URL claims to hold.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            http,
            base_url: base_url.into(),
            pace: DEFAULT_PACE,
            gate: Arc::new(RequestGate::new(DEFAULT_CONCURRENCY)),
        })
    }

    /// Build a core for `base_url` with the key from [`API_KEY_ENV`].
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub(crate) fn from_env(base_url: impl Into<String>) -> Result<Self, LseError> {
        let api_key = api_key_from_env().map_err(LseError::EnvVar)?;

        Self::new(&api_key, base_url)
    }

    /// Replace the base URL, for tests against a mock server or a proxy.
    pub(crate) fn set_base_url(&mut self, base_url: impl Into<String>) {
        self.base_url = base_url.into();
    }

    /// Replace the HTTP client, dropping the auth header, timeouts and redirect policy with it.
    ///
    /// Callers exposing this must repeat those three obligations in their own documentation.
    pub(crate) fn set_client(&mut self, client: reqwest::Client) {
        self.http = client;
    }

    /// Replace the minimum delay between the starts of two requests.
    pub(crate) fn set_pace(&mut self, pace: Duration) {
        self.pace = pace;
    }

    /// Install a **fresh** gate with `concurrency` permits.
    ///
    /// Clones taken before this call keep the old gate and ration separately, which is why every
    /// public wrapper for it carries a "call this before cloning" warning.
    pub(crate) fn set_concurrency(&mut self, concurrency: usize) {
        self.gate = Arc::new(RequestGate::new(concurrency));
    }

    /// Wait for this core's shared gate to admit one request.
    ///
    /// The returned permit bounds concurrency and must be held until the response body has been
    /// consumed — dropping it early lets another request start while this one is still on the wire.
    pub(crate) async fn enter_gate(&self) -> Option<SemaphorePermit<'_>> {
        self.gate.enter(self.pace).await
    }

    /// The configured base URL.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The configured minimum delay between request starts.
    pub(crate) fn pace(&self) -> Duration {
        self.pace
    }

    /// The configured in-flight ceiling.
    pub(crate) fn concurrency(&self) -> usize {
        self.gate.concurrency
    }

    /// Issue an authenticated `GET` against a path under the base URL and deserialise its JSON.
    ///
    /// Rationed by this core's shared gate, so a caller driving several of these concurrently does
    /// not have to pace them itself.
    ///
    /// # Errors
    /// See [`checked`](Self::checked); a body that will not decode is [`LseError::Deserialize`].
    pub(crate) async fn get_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, LseError>
    where
        T: serde::de::DeserializeOwned,
    {
        // Held until the body has been read below, so the permit measures a request's real
        // occupancy of a connection rather than just the time to get response headers back.
        let _permit = self.enter_gate().await;

        let url = format!("{}/{path}", self.base_url);
        let response = Self::checked(self.http.get(&url).query(query).send().await?).await?;

        // `bytes()`, not `text()`: `text()` copies the whole body into a fresh `String` purely to
        // validate UTF-8 that `serde_json` validates again.
        let body = response.bytes().await?;
        debug!(len = body.len(), path, "lse response received");

        serde_json::from_slice(&body).map_err(|error| LseError::Deserialize {
            message: format!("{path}: {error}"),
        })
    }

    /// Issue an authenticated `GET` for a file answer and hand back its verified body.
    ///
    /// The whole body is held in memory, so callers must bound what they ask for;
    /// [`fetch_candles`](super::client::LseClient::fetch_candles) asks for chunks of a bounded
    /// number of bars. See [`get_file_into`](Self::get_file_into) for what is verified.
    ///
    /// # Errors
    /// See [`get_file_into`](Self::get_file_into).
    pub(crate) async fn get_file(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<(Vec<u8>, FileHead), LseError> {
        let mut body = Vec::new();
        let head = self.get_file_into(path, query, &mut body).await?;
        Ok((body, head))
    }

    /// Issue an authenticated `GET` for a file answer and stream its body into `sink`, verifying
    /// it as it goes.
    ///
    /// The provider builds a file whole before sending it, then sends its exact length in
    /// `Content-Length` and its SHA-256 in `x-checksum-sha256`. Each chunk is hashed as it arrives
    /// and written to `sink`; after the last byte, the count and the digest must both match.
    ///
    /// A failure leaves whatever was written in `sink`: the caller discards it. The provider
    /// ignores range requests and keeps nothing once it has sent a file, so a cut or damaged
    /// transfer can only be requested again whole.
    ///
    /// `x-export-rows` is not checked: it is not the file's row count. A six-hour hourly read
    /// declared 6 rows and carried 6, but a three-day one declared 4,178 and carried 73 (measured
    /// 2026-11-27), so it appears to count the stored rows the bars were built from. The checksum
    /// already covers every byte of the body.
    ///
    /// Rationed by this core's gate, and the permit is held until the last byte: the provider holds
    /// one download open per key.
    ///
    /// # Errors
    /// See [`checked`](Self::checked) for a refusal. A success answer without a usable checksum is
    /// [`LseError::MissingChecksum`], and a body that hashes otherwise is
    /// [`LseError::IntegrityMismatch`]. A body cut short of its `Content-Length` is refused by the
    /// HTTP stack as [`LseError::Http`]. A failed write to `sink` is [`LseError::Io`].
    pub(crate) async fn get_file_into<W>(
        &self,
        path: &str,
        query: &[(&str, String)],
        sink: &mut W,
    ) -> Result<FileHead, LseError>
    where
        W: AsyncWrite + Unpin,
    {
        let _permit = self.enter_gate().await;

        let url = format!("{}/{path}", self.base_url);
        let request = self.http.get(&url).query(query).timeout(DOWNLOAD_TIMEOUT);
        let mut response = Self::checked(request.send().await?).await?;

        let headers = response.headers();
        let declared_sha256 = header_str(headers, CHECKSUM_HEADER)
            .map(str::to_ascii_lowercase)
            .filter(|digest| digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or(LseError::MissingChecksum)?;
        let rows_charged = header_str(headers, ROWS_HEADER).and_then(|rows| rows.parse().ok());
        let export_id = header_str(headers, EXPORT_ID_HEADER).map(str::to_owned);
        let file_name = header_str(headers, reqwest::header::CONTENT_DISPOSITION.as_str())
            .and_then(disposition_file_name);
        let adjusted = header_str(headers, ADJUSTED_HEADER).and_then(|flag| match flag {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        });

        let mut hasher = Sha256::new();
        let mut received: u64 = 0;
        while let Some(chunk) = response.chunk().await? {
            hasher.update(&chunk);
            received += chunk.len() as u64;
            sink.write_all(&chunk)
                .await
                .map_err(|source| LseError::Io {
                    message: format!("writing the body of {path}"),
                    source,
                })?;
        }
        sink.flush().await.map_err(|source| LseError::Io {
            message: format!("flushing the body of {path}"),
            source,
        })?;

        // A body cut short of its `Content-Length` never gets here: the HTTP stack refuses it. The
        // checksum covers every byte of what did arrive.
        let actual = hex::encode(hasher.finalize());
        if actual != declared_sha256 {
            return Err(LseError::IntegrityMismatch {
                expected: declared_sha256,
                actual,
            });
        }
        debug!(len = received, path, "lse file received and verified");

        Ok(FileHead {
            bytes: received,
            sha256: actual,
            rows_charged,
            export_id,
            file_name,
            adjusted,
        })
    }

    /// Pass a success response through, or turn a refusal into its typed error.
    ///
    /// # Errors
    /// A `429` is [`LseError::RateLimited`], carrying `Retry-After` and the provider's code when
    /// present. Any other non-success status, an unfollowed redirect included, is
    /// [`LseError::Api`] with the provider's code and sentence.
    async fn checked(response: reqwest::Response) -> Result<reqwest::Response, LseError> {
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Surfaced and terminal: pacing policy belongs to the caller, so this never sleeps and
            // retries on their behalf.
            let retry_after = parse_retry_after(response.headers());
            // A read failure here must not mask the rate limit, so it costs only the code.
            let code = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES)
                .await
                .ok()
                .and_then(|body| extract_refusal(&body).0);

            return Err(LseError::RateLimited { retry_after, code });
        }

        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            let (code, message) = extract_refusal(&body);
            return Err(LseError::Api {
                status: status.as_u16(),
                code,
                message,
            });
        }

        Ok(response)
    }
}

/// Read the API key from [`API_KEY_ENV`], or return a message that is safe to log.
///
/// Returns the failure as a `String` rather than a typed error so that both credential paths can
/// share it: the REST clients wrap it in [`LseError::EnvVar`] and the WebSocket connector in its
/// own `SocketError`. One key authenticates every surface, so the variable and the wording of a
/// failure to read it are properties of the provider, not of any one transport.
///
/// # ⚠️ Why the message is fixed rather than derived from the error
/// [`env::VarError`]'s own `Display` is **not** safe to interpolate: its `NotUnicode` arm is
/// "environment variable was not valid unicode: {:?}" and embeds the raw `OsString`, so a key with
/// one stray non-UTF-8 byte — a mis-encoded paste, a wrong-encoding `.env` — would put essentially
/// the whole key into an error string that callers routinely log. That would defeat the redaction
/// done everywhere else. Both arms are therefore reported by a fixed message that names the
/// variable and nothing more. Keeping this in one place is the point: a second copy of the
/// redaction is a second chance for it to drift.
pub(crate) fn api_key_from_env() -> Result<String, String> {
    env::var(API_KEY_ENV).map_err(|error| match error {
        env::VarError::NotPresent => format!("{API_KEY_ENV} is not set"),
        env::VarError::NotUnicode(_) => format!("{API_KEY_ENV} is set but is not valid UTF-8"),
    })
}

/// A header's value as trimmed text, if present and visible ASCII.
fn header_str<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

/// The `filename` parameter of a `Content-Disposition` value, quoted or not.
///
/// The provider sends `attachment; filename="AAPL_us_stocks_1m_....parquet"`. Only the plain
/// `filename` parameter is read; the provider sends no extended `filename*` form.
fn disposition_file_name(value: &str) -> Option<String> {
    value.split(';').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("filename") {
            return None;
        }
        let value = value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|quoted| quoted.strip_suffix('"'))
            .unwrap_or(value);
        (!value.is_empty()).then(|| value.to_owned())
    })
}

/// Parse the `Retry-After` header as a delay.
///
/// Only the delta-seconds form is read. The HTTP-date form is not parsed rather than being
/// guessed at: a misread date would produce a wildly wrong delay, and `None` (caller decides)
/// is safer than a confident wrong number.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    const TEST_BASE: &str = "https://example.invalid/base";

    #[test]
    fn debug_does_not_leak_the_api_key() {
        let core = LseHttpCore::new("super-secret-key", TEST_BASE).unwrap();

        assert!(!format!("{core:?}").contains("super-secret-key"));
    }

    #[test]
    fn a_key_carrying_a_control_character_is_a_typed_error_not_a_panic() {
        // The realistic case is a key copied with a trailing newline. It is rejected rather than
        // trimmed: silently mutating a credential would hide the malformed value from the user,
        // and the typed error names the problem.
        // Note a horizontal tab is *not* here: HTTP header values permit it, so it reaches the
        // provider and comes back as a plain `401` instead.
        for key in [
            "key-with-\nnewline",
            "key-with-\rcarriage",
            "key\u{0}with-nul",
        ] {
            assert!(
                matches!(
                    LseHttpCore::new(key, TEST_BASE).unwrap_err(),
                    LseError::InvalidCredential(_)
                ),
                "{key:?} should be rejected"
            );
        }
    }

    #[test]
    fn from_env_reports_the_variable_name_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let error = LseHttpCore::from_env(TEST_BASE).unwrap_err();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(error.to_string().contains(API_KEY_ENV));
        });
    }

    /// `VarError::NotUnicode`'s own `Display` embeds the raw `OsString`, so interpolating it would
    /// put a mis-encoded key straight into an error string that callers routinely log. Unix-only:
    /// the invalid value has to be built from raw bytes.
    #[cfg(unix)]
    #[test]
    fn from_env_never_reports_a_non_unicode_key() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        // A plausible mis-encoded paste: a real-looking key carrying one stray non-UTF-8 byte.
        let mut raw = b"lse-live-super-secret-".to_vec();
        raw.push(0xff);
        raw.extend_from_slice(b"-tail");

        temp_env::with_var(API_KEY_ENV, Some(OsString::from_vec(raw)), || {
            let error = LseHttpCore::from_env(TEST_BASE).unwrap_err();
            let message = error.to_string();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(message.contains(API_KEY_ENV));
            assert!(!message.contains("lse-live-super-secret-"));
        });
    }

    #[test]
    fn the_base_url_is_whatever_the_host_client_supplied() {
        // The core is host-agnostic: it holds the URL it was given and derives nothing from it.
        let core = LseHttpCore::new("k", "https://data-api.example.invalid").unwrap();

        assert_eq!(core.base_url(), "https://data-api.example.invalid");
    }

    #[test]
    fn defaults_are_the_measured_provider_limits() {
        let core = LseHttpCore::new("k", TEST_BASE).unwrap();

        assert_eq!(core.pace(), DEFAULT_PACE);
        assert_eq!(core.concurrency(), DEFAULT_CONCURRENCY);
    }

    #[test]
    fn a_zero_concurrency_is_clamped_rather_than_parking_every_request() {
        let mut core = LseHttpCore::new("k", TEST_BASE).unwrap();
        core.set_concurrency(0);

        assert_eq!(core.concurrency(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn two_cores_built_separately_do_not_share_a_gate() {
        // The claim the module documentation makes: rationing is per core, so two clients built
        // separately get the sum of the two rates, not one shared budget.
        let one = LseHttpCore::new("k", TEST_BASE).unwrap();
        let two = LseHttpCore::new("k", TEST_BASE).unwrap();

        // Each core admits its first request immediately; if they shared a gate the second would
        // be delayed by `DEFAULT_PACE`.
        let start = Instant::now();
        let _a = one.enter_gate().await;
        let _b = two.enter_gate().await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn clones_of_one_core_share_its_gate() {
        // The counterpart claim: cloning a client must not multiply its rate.
        // Two permits, so the second caller waits on the pace alone; with the default one it
        // would wait for the first permit, which this test never releases.
        let mut core = LseHttpCore::new("k", TEST_BASE).unwrap();
        core.set_concurrency(2);
        let clone = core.clone();

        let start = Instant::now();
        let _a = core.enter_gate().await;
        let _b = clone.enter_gate().await;

        assert_eq!(start.elapsed(), DEFAULT_PACE);
    }

    /// Both credential paths — the REST clients and the WebSocket connector — now read the key
    /// through this one helper, so its redaction is tested directly rather than only through the
    /// transport that happens to wrap it.
    #[test]
    fn api_key_from_env_names_the_variable_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let message = api_key_from_env().unwrap_err();

            assert!(message.contains(API_KEY_ENV), "{message}");
        });
    }

    #[test]
    fn api_key_from_env_returns_the_key_when_set() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            assert_eq!(api_key_from_env().unwrap(), "test-key");
        });
    }

    /// The reason the message is fixed rather than derived from `VarError`: its `NotUnicode` arm
    /// embeds the raw `OsString`, which would leak a mis-encoded key into anything that logs the
    /// error. Unix-only — the invalid value has to be built from raw bytes.
    #[cfg(unix)]
    #[test]
    fn api_key_from_env_never_reports_a_non_unicode_key() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        let mut raw = b"lse-live-super-secret-".to_vec();
        raw.push(0xff);
        raw.extend_from_slice(b"-tail");

        temp_env::with_var(API_KEY_ENV, Some(OsString::from_vec(raw)), || {
            let message = api_key_from_env().unwrap_err();

            assert!(message.contains(API_KEY_ENV), "{message}");
            assert!(!message.contains("lse-live-super-secret-"), "{message}");
        });
    }

    #[test]
    fn retry_after_reads_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("42"));

        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(42)));
    }

    #[test]
    fn retry_after_declines_to_guess_at_an_http_date() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT"),
        );

        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn retry_after_is_none_when_absent() {
        assert_eq!(parse_retry_after(&HeaderMap::new()), None);
    }

    /// The bound that makes the `with_concurrency` claim true: however many callers want in, only
    /// `concurrency` of them are ever inside at once.
    #[tokio::test]
    async fn the_gate_never_admits_more_callers_than_its_concurrency() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let gate = RequestGate::new(2);
        let in_flight = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        futures::future::join_all((0..8).map(|_| async {
            let _permit = gate.enter(Duration::ZERO).await;

            let entered = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(entered, Ordering::SeqCst);
            // Keeps the permit held across a suspension point, so a later caller gets the chance
            // to overlap; without it every caller would trivially run to completion alone.
            tokio::task::yield_now().await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
        }))
        .await;

        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    /// N concurrent callers share ONE schedule, so the aggregate rate is one request per `pace` —
    /// not N.
    #[tokio::test(start_paused = true)]
    async fn the_gate_spaces_concurrent_callers_against_one_shared_schedule() {
        let pace = Duration::from_millis(300);
        // Wide enough that the semaphore never blocks, isolating pacing from concurrency.
        let gate = RequestGate::new(8);
        let started = Instant::now();

        let admitted = futures::future::join_all((0..4).map(|_| async {
            let _permit = gate.enter(pace).await;
            started.elapsed()
        }))
        .await;

        assert_eq!(admitted, vec![Duration::ZERO, pace, pace * 2, pace * 3]);
    }

    /// An idle client must not bank credit: a burst after a quiet period should start at once and
    /// only then space out, rather than firing everything it "could have" sent while idle.
    #[tokio::test(start_paused = true)]
    async fn an_idle_gate_accrues_no_credit() {
        let pace = Duration::from_millis(300);
        let gate = RequestGate::new(8);

        drop(gate.enter(pace).await);
        tokio::time::sleep(Duration::from_secs(60)).await;

        let started = Instant::now();
        drop(gate.enter(pace).await);

        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
