//! Authenticated REST transport for the London Strategic Edge vault.
//!
//! The vault (`api.londonstrategicedge.com/vault`) is the provider's data plane. It is a distinct
//! host from their catalog/discovery API, with a different symbol key: **the vault keys candles on
//! the display symbol** (`EUR/USD`, `AAPL`, `ES.F`), not on a dataset slug.
//!
//! The HTTP plumbing both of the provider's hosts share — auth header, agent, timeouts, redirect
//! policy, request rationing and error mapping — lives in an internal transport that this client
//! wraps; the module here holds what is specific to the vault.
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

use crate::exchange::lse::error::LseError;
use crate::exchange::lse::quota::QuotaStatus;
use crate::exchange::lse::transport::LseHttpCore;
use std::{num::NonZeroU32, time::Duration};
use tokio::sync::SemaphorePermit;

/// Base URL of the vault data plane.
const VAULT_BASE_URL: &str = "https://api.londonstrategicedge.com/vault";

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

/// Authenticated client for the London Strategic Edge vault.
///
/// Wraps the internal transport shared with the provider's other host, pointed at the vault, and
/// adds the vault's own configuration and endpoints: [`usage`](Self::usage) here, paged candles in
/// [`historical`](super::historical), the catalog in [`reference`](super::reference), bulk export
/// in [`export`](super::export).
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
    core: LseHttpCore,
    page_limit: NonZeroU32,
}

impl std::fmt::Debug for LseVaultClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports. The core's own `Debug` redacts
        // too; the fields are restated here so this type's output does not depend on that.
        f.debug_struct("LseVaultClient")
            .field("base_url", &self.core.base_url())
            .field("pace", &self.core.pace())
            .field("page_limit", &self.page_limit)
            .field("concurrency", &self.core.concurrency())
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
    /// default [`Policy::limited(10)`](reqwest::redirect::Policy::limited) a single server-issued
    /// 302 would therefore re-issue the request, live key attached, against whatever host the
    /// vault named. The download endpoint is exactly the shape that hands off to object storage,
    /// and in a CI workflow that key is a repository secret.
    ///
    /// [`Policy::none`](reqwest::redirect::Policy::none) makes the "this client only ever requests
    /// URLs it builds itself" invariant structural rather than a property of the vault's current
    /// behaviour — a followed redirect *is* a server-supplied URL. An unexpected 3xx surfaces as a
    /// typed error instead of being followed, and it applies **wholesale**: a same-origin 301/308
    /// (trailing-slash normalisation by a proxy or CDN) is surfaced too, so a base URL set via
    /// [`with_base_url`](Self::with_base_url) must serve responses directly.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidCredential`] if the key cannot be encoded as an HTTP header
    /// value (e.g. non-ASCII bytes), or [`LseError::Http`] if the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, LseError> {
        Ok(Self {
            core: LseHttpCore::new(api_key, VAULT_BASE_URL)?,
            page_limit: DEFAULT_PAGE_LIMIT,
        })
    }

    /// Create a client from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub fn from_env() -> Result<Self, LseError> {
        Ok(Self {
            core: LseHttpCore::from_env(VAULT_BASE_URL)?,
            page_limit: DEFAULT_PAGE_LIMIT,
        })
    }

    /// Override the vault base URL, for tests against a mock server or a proxy.
    ///
    /// Infallible by design: this client only ever requests URLs it builds itself from this base —
    /// the vault returns bare arrays with no cursor, so there is never a server-supplied URL to
    /// follow and no trusted origin to derive.
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.core.set_base_url(base_url);
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
        self.core.set_client(client);
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
        self.core.set_pace(pace);
        self
    }

    /// Override how many rows a paged fetch requests per page.
    ///
    /// The default is the provider's measured
    /// [`max_rows_per_request`](QuotaStatus::max_rows_per_request); raise it only against a key whose
    /// [`usage`](Self::usage) reports a higher one. Requesting **more** than your key allows is not
    /// an error — the vault caps the page silently, and pagination handles a short page — so an
    /// over-large value costs nothing but degrades into more, smaller pages. The option print walk,
    /// which cannot page past a short page, bounds this by the cap [`usage`](Self::usage) reports. Requesting fewer than
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
        self.core.set_concurrency(concurrency);
        self
    }

    /// Wait for this client's shared gate to admit one request.
    ///
    /// The returned permit bounds concurrency and must be held until the response body has been
    /// consumed — dropping it early lets another request start while this one is still on the wire.
    /// Endpoint families outside this module call it before issuing a request that does not go
    /// through [`get_json`](Self::get_json).
    pub(crate) async fn enter_gate(&self) -> Option<SemaphorePermit<'_>> {
        self.core.enter_gate().await
    }

    /// The configured vault base URL.
    ///
    /// Endpoint families build their own URLs from this, preserving the invariant that this client
    /// never follows a server-supplied URL.
    pub(crate) fn base_url(&self) -> &str {
        self.core.base_url()
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
        self.core.http()
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
        self.core.get_json(path, query).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::lse::transport::API_KEY_ENV;
    use tokio::time::Instant;

    #[test]
    fn debug_does_not_leak_the_api_key() {
        let client = LseVaultClient::new("super-secret-key").unwrap();

        assert!(!format!("{client:?}").contains("super-secret-key"));
    }

    #[test]
    fn debug_reports_the_vault_configuration() {
        // The four fields callers actually diagnose with: where it points, how it is rationed, and
        // how large a page it asks for.
        let client = LseVaultClient::new("key").unwrap();
        let rendered = format!("{client:?}");

        assert!(rendered.contains(VAULT_BASE_URL), "{rendered}");
        assert!(rendered.contains("page_limit: 5000"), "{rendered}");
        assert!(rendered.contains("concurrency: 2"), "{rendered}");
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

    #[test]
    fn from_env_builds_a_client_when_set() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            assert!(LseVaultClient::from_env().is_ok());
        });
    }

    #[test]
    fn from_env_points_at_the_vault_not_at_whatever_the_core_defaults_to() {
        // The core is host-agnostic, so the vault base URL is this type's contribution and a
        // regression in it would silently send every vault call to the wrong host.
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            let client = LseVaultClient::from_env().unwrap();

            assert_eq!(client.base_url(), VAULT_BASE_URL);
        });
    }

    #[test]
    fn new_points_at_the_vault() {
        assert_eq!(
            LseVaultClient::new("key").unwrap().base_url(),
            VAULT_BASE_URL
        );
    }

    #[test]
    fn with_base_url_overrides_the_host() {
        let client = LseVaultClient::new("key")
            .unwrap()
            .with_base_url("http://127.0.0.1:1/mock");

        assert_eq!(client.base_url(), "http://127.0.0.1:1/mock");
    }

    #[test]
    fn with_page_limit_is_carried_independently_of_the_shared_core() {
        let limit = NonZeroU32::new(17).unwrap();
        let client = LseVaultClient::new("key").unwrap().with_page_limit(limit);

        assert_eq!(client.page_limit(), limit);
        assert!(format!("{client:?}").contains("page_limit: 17"));
    }

    /// Clones must queue together — otherwise handing a clone to each of N concurrent fetches would
    /// restore exactly the N × rate the gate exists to prevent.
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
