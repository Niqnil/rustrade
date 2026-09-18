//! Authenticated REST transport for the London Strategic Edge vault.
//!
//! The vault (`api.londonstrategicedge.com/vault`) is the provider's data plane. It is a distinct
//! host from their catalog/discovery API, with a different symbol key: **the vault keys candles on
//! the display symbol** (`EUR/USD`, `AAPL`, `ES.F`), not on a dataset slug.
//!
//! # Authentication
//! Every vault endpoint requires an API key, sent as the `x-api-key` header. Supply it explicitly
//! via [`LseVaultClient::new`] or from `LSE_API_KEY` via [`LseVaultClient::from_env`]. Keys are
//! free and require no account.
//!
//! # ⚠️ Unknown query parameters are ignored, not rejected
//! The vault answers `200` to a request carrying a misspelled parameter, having silently applied
//! its default instead. Measured: `resolution=1d` returns **1-minute** bars, byte-identical in
//! shape to a correct response, and `from`/`since`/`after`/`begin`/`start_date`/`start_time`/`to`/
//! `until`/`end_date` are all ignored in favour of full history from page one. Only `symbol`,
//! `timeframe`, `start`, `end` and `limit` are honoured. **Any parameter added here must be
//! verified against a known-answer query** — a wrong name is never an error.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_detail};
use crate::exchange::lse::quota::QuotaStatus;
use reqwest::header::{HeaderMap, HeaderValue};
use std::{env, num::NonZeroU32, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::{Instant, sleep_until},
};
use tracing::debug;

/// Base URL of the vault data plane.
const VAULT_BASE_URL: &str = "https://api.londonstrategicedge.com/vault";

/// Header carrying the API key.
const API_KEY_HEADER: &str = "x-api-key";

/// Environment variable read by [`LseVaultClient::from_env`].
const API_KEY_ENV: &str = "LSE_API_KEY";

/// Total deadline for a JSON request: connect, send, and read the whole body.
///
/// Sound for the JSON endpoints, whose bodies are a page of candles at most. It is **not** sound for
/// an export artifact, which is why [`download_export`](super::export) overrides it per-request —
/// `reqwest` applies this as a total deadline "from when the request starts connecting until the
/// response body has finished", so a multi-gigabyte transfer would abort mid-body however healthy
/// the connection.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-read timeout, applied to every request including a streaming download.
///
/// Resets after each successful read, so it detects a *stalled* connection without bounding total
/// transfer time — the correct tool for a body whose length is not known up front.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// `User-Agent` sent on every vault request.
///
/// Set explicitly because the vault sits behind a CDN that rejects some agents outright —
/// measured: `Python-urllib` receives a `403` with `error code: 1010`, never reaching the API.
/// `reqwest` sends no `User-Agent` by default, which leaves that outcome to the CDN's discretion
/// rather than to anything this crate controls. An edge rejection is also invisible to the
/// provider's allowance accounting, so it fails in a way that looks nothing like an API error.
const USER_AGENT: &str = concat!("rustrade-data/", env!("CARGO_PKG_VERSION"));

/// Default minimum delay between the *starts* of two requests.
///
/// Derived from the provider's documented allowance of 200 calls per minute, which is one call per
/// 300ms. This is *proactive courtesy only* — it never inspects a `429`, never retries, and never
/// adapts. Override with [`LseVaultClient::with_pace`].
const DEFAULT_PACE: Duration = Duration::from_millis(300);

/// Default ceiling on requests in flight at once.
///
/// The provider reports its own ceiling as [`vault_concurrency`](QuotaStatus::vault_concurrency),
/// measured at 2. A key on a different plan may be allowed more — read
/// [`usage`](LseVaultClient::usage) and raise this with
/// [`with_concurrency`](LseVaultClient::with_concurrency) rather than assuming the default is your
/// key's limit.
const DEFAULT_CONCURRENCY: usize = 2;

/// Default rows requested per page of a paged fetch.
///
/// Matches the provider's measured
/// [`max_rows_per_request`](QuotaStatus::max_rows_per_request). The cap is enforced **silently** —
/// an over-large range returns exactly this many rows with a `200` and no truncation marker — which
/// is why pagination never treats a short page as the end of the data. A key on a different plan may
/// be allowed more: read [`usage`](LseVaultClient::usage) and raise this with
/// [`with_page_limit`](LseVaultClient::with_page_limit) rather than assuming the default is your
/// key's limit.
const DEFAULT_PAGE_LIMIT: NonZeroU32 = NonZeroU32::new(5000).unwrap();

/// Shared gate every request passes through: bounds concurrency, then spaces request starts.
///
/// Held behind an [`Arc`] so that all clones of one [`LseVaultClient`] — and therefore every
/// concurrent stream built from one — queue at the *same* gate.
///
/// # Why this cannot be per-request state
/// Pacing a single paged fetch is a claim about that fetch only. A multi-instrument replay such as
/// [`replay_candles`](super::backtest::replay_candles) drives N fetches concurrently, so N
/// independently-paced streams produce an aggregate rate of N per `pace` — which is exactly what
/// [`DEFAULT_PACE`]'s "200 calls per minute" derivation says must not happen, and N in-flight
/// requests against a `vault_concurrency` of 2. Both bounds therefore belong to the client, not to
/// any one call site, so that they hold for every entry point without the caller re-deriving them.
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
    /// **admitted** callers rather than requests actually in flight — with a `pace` of 300ms and a
    /// concurrency of 2, the steady state is one request on the wire and one waiting, not two on
    /// the wire. Acquiring in the other order would let an unbounded number of callers queue on
    /// the pacing mutex and then release together, which is the burst the concurrency limit exists
    /// to prevent. The cost is that `concurrency` reads as a slightly stricter limit than its name
    /// suggests; the benefit is that neither bound can be exceeded.
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

/// Authenticated client for the London Strategic Edge vault.
///
/// Holds one configured [`reqwest::Client`] (auth header + timeout) plus the vault base URL.
/// Endpoint families build on it: [`usage`](Self::usage) here, paged candles in
/// [`historical`](super::historical).
///
/// # Request rationing is per client, not per call
/// Every request this client issues — candle page, export submit, status poll, artifact download —
/// first passes a gate that bounds requests in flight to
/// [`with_concurrency`](Self::with_concurrency) and spaces their starts by
/// [`with_pace`](Self::with_pace). That gate is **shared by clones**, so driving N concurrent
/// fetches from one client (`Arc` it, or clone it) keeps the aggregate inside those two bounds
/// rather than multiplying them by N. Two clients built separately by `new` share nothing and
/// ration independently — one client per API key is the usable unit.
///
/// # ⚠️ Licensing
/// Data retrieved through this client is **not redistributable**. See the
/// [module documentation](super) and <https://londonstrategicedge.com/terms>.
#[derive(Clone)]
pub struct LseVaultClient {
    http: reqwest::Client,
    base_url: String,
    pace: Duration,
    page_limit: NonZeroU32,
    gate: Arc<RequestGate>,
}

impl std::fmt::Debug for LseVaultClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports.
        f.debug_struct("LseVaultClient")
            .field("base_url", &self.base_url)
            .field("pace", &self.pace)
            .field("page_limit", &self.page_limit)
            .field("concurrency", &self.gate.concurrency)
            .finish_non_exhaustive()
    }
}

impl LseVaultClient {
    /// Create a client with an explicit API key.
    ///
    /// # Redirects are not followed
    /// The key rides every request as a client-wide `x-api-key` default header, and reqwest's
    /// cross-host header stripping covers `Authorization`, `Cookie`, `Cookie2`,
    /// `Proxy-Authorization` and `WWW-Authenticate` — **not** a custom header. Under reqwest's
    /// default
    /// [`Policy::limited(10)`](reqwest::redirect::Policy::limited) a single server-issued 302 would
    /// therefore re-issue the request, live key attached, against whatever host the vault named.
    /// The download endpoint is exactly the shape that hands off to object storage, and in a CI
    /// workflow that key is a repository secret.
    ///
    /// [`Policy::none`](reqwest::redirect::Policy::none) makes the "this client only ever requests
    /// URLs it builds itself" invariant structural rather than a property of the vault's current
    /// behaviour — a followed redirect *is* a server-supplied URL. An unexpected 3xx surfaces as a
    /// typed error instead of being followed. This matches `MassiveRestClient::new` (not linked:
    /// it is behind the `massive` feature, which this client is not) and the IBKR Flex client, and
    /// it applies **wholesale**: a same-origin 301/308 (trailing-slash
    /// normalisation by a proxy or CDN) is surfaced too, so a base URL set via
    /// [`with_base_url`](Self::with_base_url) must serve responses directly.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidCredential`] if the key cannot be encoded as an HTTP header
    /// value (e.g. non-ASCII bytes), or [`LseError::Http`] if the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, LseError> {
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
            // when a redirect crosses hosts -- a custom `x-api-key` survives the hop. `set_sensitive`
            // redacts `Debug` output and does nothing here. Without this line the default
            // `Policy::limited(10)` would forward the live key to any host the vault names, which
            // is precisely the invariant `with_base_url` and `download_export` claim to hold.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            http,
            base_url: VAULT_BASE_URL.to_owned(),
            pace: DEFAULT_PACE,
            page_limit: DEFAULT_PAGE_LIMIT,
            gate: Arc::new(RequestGate::new(DEFAULT_CONCURRENCY)),
        })
    }

    /// Create a client from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub fn from_env() -> Result<Self, LseError> {
        // `VarError`'s own `Display` is NOT safe to interpolate here: its `NotUnicode` arm is
        // "environment variable was not valid unicode: {:?}" and embeds the raw `OsString`, so a
        // key with one stray non-UTF-8 byte (a mis-encoded paste, a wrong-encoding `.env`) would
        // put essentially the whole key into an error string that callers routinely log. That
        // would defeat the redaction this type does everywhere else. Both arms are therefore
        // reported by a fixed message that names the variable and nothing more.
        let api_key = env::var(API_KEY_ENV).map_err(|error| {
            LseError::EnvVar(match error {
                env::VarError::NotPresent => format!("{API_KEY_ENV} is not set"),
                env::VarError::NotUnicode(_) => {
                    format!("{API_KEY_ENV} is set but is not valid UTF-8")
                }
            })
        })?;

        Self::new(&api_key)
    }

    /// Override the vault base URL, for tests against a mock server or a proxy.
    ///
    /// Infallible by design: this client only ever requests URLs it builds itself from this base —
    /// the vault returns bare arrays with no cursor, so there is never a server-supplied URL to
    /// follow and no trusted origin to derive.
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Inject a pre-built [`reqwest::Client`].
    ///
    /// # ⚠️ The injected client must carry the `x-api-key` header itself
    /// This replaces the authenticated client built by [`new`](Self::new), auth header included.
    /// It is intended for transport configuration (proxy, TLS, a shared connection pool) where the
    /// caller supplies credentials; a client without the header will see every request `401`.
    ///
    /// # ⚠️ It must also carry its own timeouts
    /// [`new`](Self::new) configures a 30s total timeout and a 30s read timeout; `reqwest`'s
    /// default is **neither**. Replacing the client drops both, and a timeout-less client
    /// interacts badly with the [request gate](Self#request-rationing-is-per-client-not-per-call):
    /// a permit is held for the whole request, so with the default concurrency of 2, two stalled
    /// connections exhaust the gate *permanently* — for this client and every clone of it, with no
    /// error, no log, and nothing to observe but a run that stops making progress. Set
    /// [`timeout`](reqwest::ClientBuilder::timeout) and
    /// [`read_timeout`](reqwest::ClientBuilder::read_timeout) on any client passed here.
    ///
    /// # ⚠️ And its own redirect policy
    /// [`new`](Self::new) sets
    /// [`Policy::none`](reqwest::redirect::Policy::none) so the API key cannot ride a server-issued
    /// redirect to another host; reqwest's default is `Policy::limited(10)`, which strips only the
    /// standard auth headers and would forward a custom `x-api-key` intact. A client supplied here
    /// that carries the key **must** set `Policy::none` itself, or a single 302 from the vault (or
    /// from a proxy in front of it) leaks the credential with no error and no log.
    #[must_use]
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.http = client;
        self
    }

    /// Override the minimum delay between the starts of two requests.
    ///
    /// Applies to **every** request this client issues, not just successive pages of one fetch, and
    /// is enforced against the [gate](Self#request-rationing-is-per-client-not-per-call) this client
    /// shares with its clones — so N concurrent fetches still start one request per `pace` between
    /// them.
    ///
    /// Pass [`Duration::ZERO`] to disable pacing entirely, at which point staying within the
    /// provider's [`calls_per_minute`](QuotaStatus::calls_per_minute) allowance becomes the
    /// caller's responsibility. The default is derived from that documented allowance.
    #[must_use]
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.pace = pace;
        self
    }

    /// Override how many rows a paged fetch requests per page.
    ///
    /// The default is the provider's measured
    /// [`max_rows_per_request`](QuotaStatus::max_rows_per_request); raise it only against a key whose
    /// [`usage`](Self::usage) reports a higher one. Requesting **more** than your key allows is not
    /// an error — the vault caps the page silently, and pagination handles a short page — so an
    /// over-large value costs nothing but degrades into more, smaller pages. Requesting fewer than
    /// the cap is a legitimate way to bound per-page memory or response latency.
    ///
    /// # Why `NonZeroU32`, where [`with_concurrency`](Self::with_concurrency) clamps
    /// A concurrency of `0` parks every request — visibly broken, so clamping to `1` is a safe
    /// reading of a caller slip. A page limit of `0` is worse than broken: the vault answers `200`
    /// with an empty page, which pagination reads as the end of the data, so the fetch **completes
    /// successfully having returned nothing**. That is a silent wrong answer, and the type system
    /// rules it out rather than a clamp papering over it.
    #[must_use]
    pub fn with_page_limit(mut self, limit: NonZeroU32) -> Self {
        self.page_limit = limit;
        self
    }

    /// Override how many requests this client may have in flight at once.
    ///
    /// The default is the provider's measured [`vault_concurrency`](QuotaStatus::vault_concurrency);
    /// raise it only against a key whose [`usage`](Self::usage) reports a higher one. `0` is
    /// clamped to `1`, since a client that can never issue a request is never what was meant.
    ///
    /// # ⚠️ Call this before cloning
    /// This installs a **fresh** gate. Clones taken *before* the call keep the old one and ration
    /// separately from this client, which defeats the point of sharing; clones taken after share
    /// the new gate as usual.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.gate = Arc::new(RequestGate::new(concurrency));
        self
    }

    /// Wait for this client's shared gate to admit one request.
    ///
    /// The returned permit bounds concurrency and must be held until the response body has been
    /// consumed — dropping it early lets another request start while this one is still on the wire.
    /// Endpoint families outside this module call it before issuing a request that does not go
    /// through [`get_json`](Self::get_json).
    pub(crate) async fn enter_gate(&self) -> Option<SemaphorePermit<'_>> {
        self.gate.enter(self.pace).await
    }

    /// The configured vault base URL.
    ///
    /// Endpoint families build their own URLs from this, preserving the invariant that this client
    /// never follows a server-supplied URL.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The configured rows-per-page for a paged fetch.
    ///
    /// Endpoint families send this as the `limit` query parameter. The non-zero guarantee
    /// [`with_page_limit`](Self::with_page_limit) argues for is carried in the type rather than
    /// asserted here, so a caller cannot reintroduce the empty-page-reads-as-end-of-data case.
    pub(crate) fn page_limit(&self) -> NonZeroU32 {
        self.page_limit
    }

    /// The configured, authenticated HTTP client.
    ///
    /// For endpoint families that need a verb or a response handling this module's
    /// [`get_json`](Self::get_json) does not cover — a `POST` that answers `202`, or a streamed
    /// download with `Range` resume.
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Fetch the current allowance position.
    ///
    /// The allowance is **shared between streaming and bulk export**, so a consumer running both
    /// must budget against this single pool. See [`QuotaStatus`] for what is and is not reported —
    /// notably, no window reset time is available.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn usage(&self) -> Result<QuotaStatus, LseError> {
        self.get_json("usage", &[]).await
    }

    /// Issue an authenticated `GET` against a vault path and deserialise the JSON body.
    ///
    /// Rationed by this client's shared [gate](Self#request-rationing-is-per-client-not-per-call),
    /// so a caller driving several of these concurrently does not have to pace them itself.
    ///
    /// # Errors
    /// Maps a `429` to [`LseError::RateLimited`] (carrying `Retry-After` when present), any other
    /// non-success status to [`LseError::Api`] with the provider's diagnostic unwrapped from its
    /// envelope, and a body that will not decode to [`LseError::Deserialize`].
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
        let response = self.http.get(&url).query(query).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Surfaced and terminal: pacing policy belongs to the caller, so this never sleeps and
            // retries on their behalf.
            let retry_after = parse_retry_after(response.headers());

            // Drain before dropping, so the connection can go back to the pool. A 429 body is a
            // short JSON detail; dropping the response unread closes the connection instead, and
            // the caller's retry then pays a fresh TLS handshake for nothing. Discarded rather
            // than reported -- `retry_after` is already the actionable part, and a read failure
            // here must not mask the rate limit.
            let _ = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await;

            return Err(LseError::RateLimited { retry_after });
        }

        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            return Err(LseError::Api {
                status: status.as_u16(),
                message: extract_detail(&body),
            });
        }

        // `bytes()`, not `text()`: `text()` copies the whole body out of the response's buffer into a
        // fresh `String` first, purely to validate UTF-8 that `serde_json` validates again. A
        // multi-year one-minute backfill runs to hundreds of pages, so that copy is not free, and
        // `len` for the log line is available either way.
        let body = response.bytes().await?;
        debug!(len = body.len(), path, "vault response received");

        serde_json::from_slice(&body).map_err(|error| LseError::Deserialize {
            message: format!("{path}: {error}"),
        })
    }
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

    #[test]
    fn debug_does_not_leak_the_api_key() {
        let client = LseVaultClient::new("super-secret-key").unwrap();

        assert!(!format!("{client:?}").contains("super-secret-key"));
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
                    LseVaultClient::new(key).unwrap_err(),
                    LseError::InvalidCredential(_)
                ),
                "{key:?} should be rejected"
            );
        }
    }

    #[test]
    fn from_env_reports_the_variable_name_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let error = LseVaultClient::from_env().unwrap_err();

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
            let error = LseVaultClient::from_env().unwrap_err();
            let message = error.to_string();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(message.contains(API_KEY_ENV));
            assert!(
                !message.contains("super-secret"),
                "the key must never reach the error message: {message}"
            );
        });
    }

    #[test]
    fn from_env_builds_a_client_when_set() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            assert!(LseVaultClient::from_env().is_ok());
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

    /// The bound that makes the `vault_concurrency` claim true: however many callers want in, only
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

    /// Clones must queue together — otherwise handing a clone to each of N concurrent fetches would
    /// restore exactly the N × rate this gate exists to prevent.
    #[tokio::test(start_paused = true)]
    async fn a_cloned_client_queues_at_the_same_gate_as_its_original() {
        let pace = Duration::from_millis(300);
        let client = LseVaultClient::new("key").unwrap().with_pace(pace);
        let clone = client.clone();
        let started = Instant::now();

        let _first = client.enter_gate().await;
        let _second = clone.enter_gate().await;

        assert_eq!(started.elapsed(), pace);
    }

    #[test]
    fn a_concurrency_of_zero_is_clamped_rather_than_parking_every_request_forever() {
        let client = LseVaultClient::new("key").unwrap().with_concurrency(0);

        assert!(format!("{client:?}").contains("concurrency: 1"));
    }
}
