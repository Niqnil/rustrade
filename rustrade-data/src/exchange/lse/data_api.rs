//! Authenticated REST transport for the London Strategic Edge data API.
//!
//! The data API (`data-api.londonstrategicedge.com`) is the provider's **second** host, distinct
//! from the [vault](super::vault) in path, payload shape and symbol key. It serves the reference
//! families the vault has no candle path for: `GET /bond-yields` and `/bond-yields/stats` are
//! wrapped in [`bond_yield`](super::bond_yield).
//!
//! The HTTP plumbing both hosts share — auth header, agent, timeouts, redirect policy, request
//! rationing and error mapping — lives in an internal transport that this client wraps, exactly as
//! [`LseVaultClient`](super::vault::LseVaultClient) does; the module here holds what is specific to
//! the data API.
//!
//! # Authentication
//! One API key authenticates **both** hosts, read from the same `LSE_API_KEY` variable and sent as
//! the same `x-api-key` header. Supply it explicitly via [`LseDataApiClient::new`] or from the
//! environment via [`LseDataApiClient::from_env`]. Keys are free and require no account.
//!
//! # ⚠️ The two hosts disagree, and neither errors on the other's spelling
//! They are not interchangeable views of one dataset:
//!
//! - **Country codes differ.** The vault catalog spells the United Kingdom `GB`; this host spells
//!   it `UK`, and `GB` here returns `200` with zero rows rather than an error. See
//!   [`LseBondYieldStats::normalise_country`](super::bond_yield::LseBondYieldStats::normalise_country).
//! - **Dates differ.** The vault stamps to the microsecond
//!   (`2024-01-02 09:09:00.000000`); the bond-yield rows here carry a bare `YYYY-MM-DD`.
//! - **Volume differs.** The vault omits `volume` for FX entirely, while this host reports one for
//!   the same bar.
//!
//! Anything crossing between them must normalise deliberately. Nothing here does it implicitly.
//!
//! # ⚠️ Rationing is per client, not per key
//! This client and an [`LseVaultClient`](super::vault::LseVaultClient) built separately hold
//! **independent** gates, so a consumer driving both issues up to the sum of their two rates. That
//! is the right default for two hosts whose limits are enforced separately, but it is not a claim
//! that the provider's allowance is per-host — the vault reports a single
//! [`calls_per_minute`](super::quota::QuotaStatus::calls_per_minute), and whether this host draws
//! on the same pool has not been measured. Budget as though it were shared.
//!
//! # ⚠️ Licensing
//! Data retrieved through this client is **not redistributable**. See the
//! [module documentation](super) and <https://londonstrategicedge.com/terms>.

use crate::exchange::lse::error::LseError;
use crate::exchange::lse::transport::LseHttpCore;
use std::time::Duration;

/// Base URL of the data API host.
///
/// Note the absence of a path prefix, where the vault carries `/vault`: this host's endpoints hang
/// directly off the root.
const DATA_API_BASE_URL: &str = "https://data-api.londonstrategicedge.com";

/// Authenticated client for the London Strategic Edge data API.
///
/// Wraps the internal transport shared with the vault, pointed at this host, and adds its
/// endpoints: bond yields in [`bond_yield`](super::bond_yield).
///
/// # Request rationing is per client, not per call
/// Every request this client issues first passes a gate that bounds requests in flight to
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
pub struct LseDataApiClient {
    core: LseHttpCore,
}

impl std::fmt::Debug for LseDataApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports. The core's own `Debug` redacts
        // too; the fields are restated here so this type's output does not depend on that.
        f.debug_struct("LseDataApiClient")
            .field("base_url", &self.core.base_url())
            .field("pace", &self.core.pace())
            .field("concurrency", &self.core.concurrency())
            .finish_non_exhaustive()
    }
}

impl LseDataApiClient {
    /// Create a client with an explicit API key.
    ///
    /// # Redirects are not followed
    /// The key rides every request as a client-wide `x-api-key` default header, and reqwest's
    /// cross-host header stripping covers `Authorization`, `Cookie`, `Cookie2`,
    /// `Proxy-Authorization` and `WWW-Authenticate` — **not** a custom header. Under reqwest's
    /// default [`Policy::limited(10)`](reqwest::redirect::Policy::limited) a single server-issued
    /// 302 would therefore re-issue the request, live key attached, against whatever host this one
    /// named.
    ///
    /// [`Policy::none`](reqwest::redirect::Policy::none) makes the "this client only ever requests
    /// URLs it builds itself" invariant structural rather than a property of the provider's current
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
            core: LseHttpCore::new(api_key, DATA_API_BASE_URL)?,
        })
    }

    /// Create a client from the `LSE_API_KEY` environment variable.
    ///
    /// The same variable the vault client reads — one key authenticates both hosts.
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub fn from_env() -> Result<Self, LseError> {
        Ok(Self {
            core: LseHttpCore::from_env(DATA_API_BASE_URL)?,
        })
    }

    /// Override the data API base URL, for tests against a mock server or a proxy.
    ///
    /// Infallible by design: this client only ever requests URLs it builds itself from this base —
    /// the endpoints here answer with a self-contained `{count, data}` envelope carrying no cursor,
    /// so there is never a server-supplied URL to follow and no trusted origin to derive.
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
    /// [`new`](Self::new) sets [`Policy::none`](reqwest::redirect::Policy::none) so the API key
    /// cannot ride a server-issued redirect to another host; reqwest's default is
    /// `Policy::limited(10)`, which strips only the standard auth headers and would forward a
    /// custom `x-api-key` intact. A client supplied here that carries the key **must** set
    /// `Policy::none` itself, or a single 302 leaks the credential with no error and no log.
    #[must_use]
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.core.set_client(client);
        self
    }

    /// Override the minimum delay between the starts of two requests.
    ///
    /// Applies to **every** request this client issues, and is enforced against the
    /// [gate](Self#request-rationing-is-per-client-not-per-call) this client shares with its
    /// clones — so N concurrent fetches still start one request per `pace` between them.
    ///
    /// Pass [`Duration::ZERO`] to disable pacing entirely, at which point staying within the
    /// provider's allowance becomes the caller's responsibility. The default is derived from the
    /// allowance the **vault** documents; this host publishes no rate of its own, so the vault's
    /// figure is used rather than an unrationed default.
    #[must_use]
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.core.set_pace(pace);
        self
    }

    /// Override how many requests this client may have in flight at once.
    ///
    /// `0` is clamped to `1`, since a client that can never issue a request is never what was
    /// meant.
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

    /// Issue an authenticated `GET` against a data API path and deserialise the JSON body.
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
        let client = LseDataApiClient::new("super-secret-key").unwrap();

        assert!(!format!("{client:?}").contains("super-secret-key"));
    }

    #[test]
    fn debug_reports_the_data_api_configuration() {
        let client = LseDataApiClient::new("key").unwrap();
        let rendered = format!("{client:?}");

        assert!(rendered.contains(DATA_API_BASE_URL), "{rendered}");
        assert!(rendered.contains("concurrency: 2"), "{rendered}");
    }

    /// The core is host-agnostic, so pointing at this host is entirely this type's contribution —
    /// a regression in it would send every bond-yield call to the vault, which answers `404` for
    /// these paths rather than anything that looks like a mistake.
    #[test]
    fn new_points_at_the_data_api_not_at_the_vault() {
        let client = LseDataApiClient::new("key").unwrap();

        assert_eq!(client.core.base_url(), DATA_API_BASE_URL);
        assert!(
            !client.core.base_url().contains("/vault"),
            "{}",
            client.core.base_url()
        );
    }

    #[test]
    fn from_env_points_at_the_data_api() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            let client = LseDataApiClient::from_env().unwrap();

            assert_eq!(client.core.base_url(), DATA_API_BASE_URL);
        });
    }

    #[test]
    fn from_env_reports_the_variable_name_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let error = LseDataApiClient::from_env().unwrap_err();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(error.to_string().contains(API_KEY_ENV));
        });
    }

    #[test]
    fn with_base_url_overrides_the_host() {
        let client = LseDataApiClient::new("key")
            .unwrap()
            .with_base_url("http://127.0.0.1:1/mock");

        assert_eq!(client.core.base_url(), "http://127.0.0.1:1/mock");
    }

    #[test]
    fn a_key_carrying_a_control_character_is_a_typed_error_not_a_panic() {
        for key in [
            "key-with-\nnewline",
            "key-with-\rcarriage",
            "key\u{0}with-nul",
        ] {
            assert!(
                matches!(
                    LseDataApiClient::new(key).unwrap_err(),
                    LseError::InvalidCredential(_)
                ),
                "{key:?} should be rejected"
            );
        }
    }

    /// Clones must queue together — otherwise handing a clone to each of N concurrent fetches would
    /// restore exactly the N × rate the gate exists to prevent.
    #[tokio::test(start_paused = true)]
    async fn a_cloned_client_queues_at_the_same_gate_as_its_original() {
        let pace = Duration::from_millis(300);
        let client = LseDataApiClient::new("key").unwrap().with_pace(pace);
        let clone = client.clone();
        let started = Instant::now();

        let _first = client.core.enter_gate().await;
        let _second = clone.core.enter_gate().await;

        assert_eq!(started.elapsed(), pace);
    }

    /// The claim the module documentation makes: a consumer driving both hosts gets the sum of the
    /// two rates, not one shared budget.
    #[tokio::test(start_paused = true)]
    async fn this_client_and_a_vault_client_ration_independently() {
        use crate::exchange::lse::vault::LseVaultClient;

        let data_api = LseDataApiClient::new("key").unwrap();
        let vault = LseVaultClient::new("key").unwrap();

        let started = Instant::now();
        let _first = data_api.core.enter_gate().await;
        let _second = vault.enter_gate().await;

        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[test]
    fn a_concurrency_of_zero_is_clamped_rather_than_parking_every_request_forever() {
        let client = LseDataApiClient::new("key").unwrap().with_concurrency(0);

        assert!(format!("{client:?}").contains("concurrency: 1"));
    }
}
