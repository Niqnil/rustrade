//! Authenticated, rationed REST transport shared by the London Strategic Edge hosts.
//!
//! The provider serves from **two hosts** that differ in path, payload shape and symbol key but
//! agree on everything this module covers: the `x-api-key` header, the `LSE_API_KEY` variable
//! behind it, the `User-Agent` their shared CDN demands, the redirect policy that keeps the key
//! off a server-supplied host, the request rationing, and the mapping from a non-success status to
//! an [`LseError`].
//!
//! [`LseHttpCore`] holds exactly that common part. The host-specific clients —
//! [`LseVaultClient`](super::vault::LseVaultClient) for the vault data plane — wrap one and add
//! their own endpoints and their own configuration on top.
//!
//! # ⚠️ One core is one ration pool
//!
//! Rationing belongs to a core, so two clients built separately ration **independently** and their
//! aggregate rate is the sum of theirs. That is the right default for two hosts, whose limits are
//! enforced separately, but it is not a claim that the provider's *allowance* is per-host: the
//! vault reports a single [`calls_per_minute`](super::quota::QuotaStatus::calls_per_minute) and
//! whether the other host draws on the same pool has not been measured. A consumer driving both
//! should budget as though it were shared.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_detail};
use reqwest::header::{HeaderMap, HeaderValue};
use std::{env, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::{Instant, sleep_until},
};
use tracing::debug;

/// Header carrying the API key. Identical on both hosts.
const API_KEY_HEADER: &str = "x-api-key";

/// Environment variable read by [`LseHttpCore::from_env`].
///
/// One key authenticates both hosts, so both clients read this same variable.
pub(crate) const API_KEY_ENV: &str = "LSE_API_KEY";

/// Total deadline for a JSON request: connect, send, and read the whole body.
///
/// Sound for the JSON endpoints, whose bodies are a page of rows at most. It is **not** sound for
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

/// `User-Agent` sent on every request to either host.
///
/// Set explicitly because both hosts sit behind a CDN that rejects some agents outright —
/// measured: a request carrying `Python-urllib`'s default agent receives `403` with
/// `error code: 1010` from **both** `api.londonstrategicedge.com` and
/// `data-api.londonstrategicedge.com`, never reaching the API. `reqwest` sends no `User-Agent` by
/// default, which leaves that outcome to the CDN's discretion rather than to anything this crate
/// controls. An edge rejection is also invisible to the provider's allowance accounting, so it
/// fails in a way that looks nothing like an API error.
const USER_AGENT: &str = concat!("rustrade-data/", env!("CARGO_PKG_VERSION"));

/// Default minimum delay between the *starts* of two requests.
///
/// Derived from the provider's documented allowance of 200 calls per minute, which is one call per
/// 300ms. This is *proactive courtesy only* — it never inspects a `429`, never retries, and never
/// adapts.
pub(crate) const DEFAULT_PACE: Duration = Duration::from_millis(300);

/// Default ceiling on requests in flight at once.
///
/// The provider reports its own ceiling as
/// [`vault_concurrency`](super::quota::QuotaStatus::vault_concurrency), measured at 2.
pub(crate) const DEFAULT_CONCURRENCY: usize = 2;

/// Shared gate every request passes through: bounds concurrency, then spaces request starts.
///
/// Held behind an [`Arc`] so that all clones of one client — and therefore every concurrent stream
/// built from one — queue at the *same* gate.
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

/// Authenticated, rationed transport against one London Strategic Edge host.
///
/// Holds one configured [`reqwest::Client`] (auth header, agent, timeouts, redirect policy), the
/// host's base URL, and the [gate](RequestGate) that rations requests. Host clients wrap one of
/// these; see the [module documentation](self) for what is shared and what is not.
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
    /// provider named. The vault's download endpoint is exactly the shape that hands off to object
    /// storage, and in a CI workflow that key is a repository secret.
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
            // when a redirect crosses hosts -- a custom `x-api-key` survives the hop. `set_sensitive`
            // redacts `Debug` output and does nothing here. Without this line the default
            // `Policy::limited(10)` would forward the live key to any host the provider names, which
            // is precisely the invariant a caller-supplied base URL and `download_export` claim to hold.
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

    /// The configured, authenticated HTTP client.
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// The configured minimum delay between request starts.
    pub(crate) fn pace(&self) -> Duration {
        self.pace
    }

    /// The configured in-flight ceiling.
    pub(crate) fn concurrency(&self) -> usize {
        self.gate.concurrency
    }

    /// Issue an authenticated `GET` against a path under the base URL and deserialise the JSON body.
    ///
    /// Rationed by this core's shared gate, so a caller driving several of these concurrently does
    /// not have to pace them itself.
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
        debug!(len = body.len(), path, "lse response received");

        serde_json::from_slice(&body).map_err(|error| LseError::Deserialize {
            message: format!("{path}: {error}"),
        })
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
        // The claim the module documentation makes: rationing is per core, so a consumer driving
        // both hosts gets the sum of the two rates, not one shared budget.
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
        let core = LseHttpCore::new("k", TEST_BASE).unwrap();
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
}
