//! The REST client for LSE Data 3.0: [`LseClient`].
//!
//! Every history call is a `GET` under `https://download.londonstrategicedge.com/v3`, authenticated
//! by the `x-api-key` header. The endpoint families live in their own modules and are methods on
//! this one client: the key's plan in [`usage`](LseClient::usage), the markets and instruments
//! in [`catalogue`](super::catalogue), candles in [`historical`](super::historical), and option
//! prints, the chain, option candles and Greeks in [`options`](super::options).
//!
//! # Authentication
//! Supply the key explicitly via [`LseClient::new`] or from `LSE_API_KEY` via
//! [`LseClient::from_env`]. A free key comes with a free account on the provider's site. The key
//! never goes in a URL.
//!
//! # ⚠️ Licensing
//! Data retrieved through this client is **not redistributable**, and use by or for an organisation
//! needs the provider's licence. See the [module documentation](super#licensing) and
//! <https://londonstrategicedge.com/licence>.

use crate::exchange::lse::error::LseError;
use crate::exchange::lse::transport::{FileHead, LseHttpCore};
use crate::exchange::lse::usage::LseUsage;
use std::{num::NonZeroU32, time::Duration};

/// Base URL of LSE Data 3.0.
pub(crate) const BASE_URL: &str = "https://download.londonstrategicedge.com/v3";

/// The code of the provider's `404` for a selection it stores nothing for.
const DATA_UNAVAILABLE: &str = "data-unavailable";

/// Default bars requested per chunk of a candle fetch.
///
/// A chunk is downloaded whole and verified before any of it is yielded, so this bounds the memory
/// a fetch holds (about 60 bytes a bar as CSV, so some 3 MB) and the work a cut transfer loses. It
/// sits far below the provider's per-export row cap, which [`usage`](LseClient::usage) reports as
/// [`rows.per_export`](super::usage::LseRowAllowance::per_export) (2,500,000 on every plan
/// measured).
const DEFAULT_CHUNK_BARS: NonZeroU32 = NonZeroU32::new(50_000).unwrap();

/// Authenticated client for LSE Data 3.0.
///
/// # Request rationing is per client, not per call
/// Every request this client issues first passes a gate that bounds requests in flight to
/// [`with_concurrency`](Self::with_concurrency) (default 1) and spaces their starts by
/// [`with_pace`](Self::with_pace). That gate is **shared by clones**, so driving N concurrent
/// fetches from one client (`Arc` it, or clone it) keeps the aggregate inside those two bounds
/// rather than multiplying them by N. Two clients built separately by `new` share nothing and
/// ration independently, though the provider's limits are per key: one client per API key is the
/// usable unit.
///
/// # ⚠️ Licensing
/// Data retrieved through this client is **not redistributable**. See the
/// [module documentation](super#licensing).
#[derive(Clone)]
pub struct LseClient {
    core: LseHttpCore,
    chunk_bars: NonZeroU32,
}

impl std::fmt::Debug for LseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports.
        f.debug_struct("LseClient")
            .field("base_url", &self.core.base_url())
            .field("pace", &self.core.pace())
            .field("chunk_bars", &self.chunk_bars)
            .field("concurrency", &self.core.concurrency())
            .finish_non_exhaustive()
    }
}

impl LseClient {
    /// Create a client with an explicit API key.
    ///
    /// # Redirects are not followed
    /// The key rides every request as a client-wide `x-api-key` default header, and reqwest's
    /// cross-host header stripping covers only the standard auth headers, **not** a custom one. A
    /// followed redirect would re-issue the request, live key attached, against whatever host the
    /// server named. [`Policy::none`](reqwest::redirect::Policy::none) makes "this client only ever
    /// requests URLs it builds itself" structural: an unexpected 3xx surfaces as
    /// [`LseError::Api`] instead of being followed. It applies wholesale, so a base URL set via
    /// [`with_base_url`](Self::with_base_url) must serve responses directly.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidCredential`] if the key cannot be encoded as an HTTP header
    /// value (e.g. non-ASCII bytes), or [`LseError::Http`] if the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, LseError> {
        Ok(Self {
            core: LseHttpCore::new(api_key, BASE_URL)?,
            chunk_bars: DEFAULT_CHUNK_BARS,
        })
    }

    /// Create a client from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub fn from_env() -> Result<Self, LseError> {
        Ok(Self {
            core: LseHttpCore::from_env(BASE_URL)?,
            chunk_bars: DEFAULT_CHUNK_BARS,
        })
    }

    /// Override the base URL (`…/v3` included), for tests against a mock server or a proxy.
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
    /// caller supplies credentials; a client without the header sees every request `401`.
    ///
    /// # ⚠️ It must also carry its own timeouts
    /// [`new`](Self::new) configures a 30s total timeout (600s for a file download) and a 30s read
    /// timeout; `reqwest`'s default is **neither**. A permit of the
    /// [request gate](Self#request-rationing-is-per-client-not-per-call) is held for the whole
    /// request, so with the default concurrency of 1 a single stalled connection stops this client
    /// and every clone of it, with no error and no log.
    ///
    /// # ⚠️ And its own redirect policy
    /// [`new`](Self::new) sets [`Policy::none`](reqwest::redirect::Policy::none) so the key cannot
    /// ride a server-issued redirect to another host; reqwest's default would forward a custom
    /// `x-api-key` intact. A client supplied here that carries the key **must** set `Policy::none`.
    #[must_use]
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.core.set_client(client);
        self
    }

    /// Override the minimum delay between the starts of two requests (default one second, the
    /// free plan's 60 calls per minute).
    ///
    /// Applies to **every** request this client issues and is enforced against the
    /// [gate](Self#request-rationing-is-per-client-not-per-call) this client shares with its
    /// clones, so N concurrent fetches still start one request per `pace` between them. Pass
    /// [`Duration::ZERO`] to disable pacing; staying within the plan's
    /// [`calls_per_minute`](LseUsage::calls_per_minute) then becomes the caller's responsibility.
    #[must_use]
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.core.set_pace(pace);
        self
    }

    /// Override how many bars a candle fetch requests per chunk (default 50,000).
    ///
    /// Each chunk is one download, held in memory and verified before any of it is yielded, so a
    /// smaller figure bounds memory and latency and a larger one saves requests. A chunk past the
    /// plan's [`per_export`](super::usage::LseRowAllowance::per_export) rows is refused by the
    /// provider with `413 export-too-wide`.
    #[must_use]
    pub fn with_chunk_bars(mut self, bars: NonZeroU32) -> Self {
        self.chunk_bars = bars;
        self
    }

    /// Override how many requests this client may have in flight at once (default 1).
    ///
    /// The provider holds one download open per key on every plan and refuses a second with
    /// `429 connections-exhausted`, so raise this only for a client that issues JSON calls alone.
    /// `0` is clamped to `1`.
    ///
    /// # ⚠️ Call this before cloning
    /// This installs a **fresh** gate. Clones taken *before* the call keep the old one and ration
    /// separately from this client; clones taken after share the new gate as usual.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.core.set_concurrency(concurrency);
        self
    }

    /// The configured base URL.
    #[cfg(test)]
    pub(crate) fn base_url(&self) -> &str {
        self.core.base_url()
    }

    /// The configured bars per candle chunk.
    pub(crate) fn chunk_bars(&self) -> NonZeroU32 {
        self.chunk_bars
    }

    /// Fetch the key's plan and what it has consumed.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn usage(&self) -> Result<LseUsage, LseError> {
        self.get_json("usage", &[]).await
    }

    /// `GET` a JSON answer from a path under the base URL.
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

    /// Download one `/v3/export` selection, verified against its headers, or `None` when the
    /// provider reports that it stores nothing for it.
    ///
    /// The provider answers a selection with no stored rows `404 data-unavailable` rather than
    /// with an empty file: a weekend, a holiday, a quiet contract, and also dates it has not
    /// loaded (its message names both causes). Reading that as "nothing here" keeps a chunked
    /// fetch's result independent of its chunk size, where an error would fail one-day chunks
    /// over a weekend that one wider chunk reads cleanly.
    pub(crate) async fn get_export(
        &self,
        query: &[(&str, String)],
    ) -> Result<Option<(Vec<u8>, FileHead)>, LseError> {
        no_stored_data_is_none(self.core.get_file("export", query).await)
    }

    /// Stream one `/v3/export` selection into `sink`, verified as it arrives, or `None` when the
    /// provider stores nothing for it (see [`get_export`](Self::get_export)).
    pub(crate) async fn get_export_into<W>(
        &self,
        query: &[(&str, String)],
        sink: &mut W,
    ) -> Result<Option<FileHead>, LseError>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        no_stored_data_is_none(self.core.get_file_into("export", query, sink).await)
    }
}

/// Read the provider's `404 data-unavailable` as "nothing stored here", leaving every other answer.
fn no_stored_data_is_none<T>(answer: Result<T, LseError>) -> Result<Option<T>, LseError> {
    match answer {
        Ok(value) => Ok(Some(value)),
        Err(LseError::Api {
            status: 404,
            code: Some(code),
            ..
        }) if code == DATA_UNAVAILABLE => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::lse::transport::API_KEY_ENV;

    #[test]
    fn debug_does_not_leak_the_api_key() {
        let client = LseClient::new("super-secret-key").unwrap();

        assert!(!format!("{client:?}").contains("super-secret-key"));
    }

    #[test]
    fn debug_reports_the_configuration() {
        let rendered = format!("{:?}", LseClient::new("key").unwrap());

        assert!(rendered.contains(BASE_URL), "{rendered}");
        assert!(rendered.contains("chunk_bars: 50000"), "{rendered}");
        assert!(rendered.contains("concurrency: 1"), "{rendered}");
    }

    #[test]
    fn a_key_carrying_a_control_character_is_a_typed_error_not_a_panic() {
        for key in ["key-with-\nnewline", "key\u{0}with-nul"] {
            assert!(matches!(
                LseClient::new(key).unwrap_err(),
                LseError::InvalidCredential(_)
            ));
        }
    }

    #[test]
    fn from_env_reports_the_variable_name_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let error = LseClient::from_env().unwrap_err();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(error.to_string().contains(API_KEY_ENV));
        });
    }

    #[test]
    fn from_env_points_at_v3() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            assert_eq!(LseClient::from_env().unwrap().base_url(), BASE_URL);
        });
    }

    #[test]
    fn with_base_url_overrides_the_host() {
        let client = LseClient::new("key")
            .unwrap()
            .with_base_url("http://127.0.0.1:1/v3");

        assert_eq!(client.base_url(), "http://127.0.0.1:1/v3");
    }

    #[test]
    fn with_chunk_bars_is_carried() {
        let bars = NonZeroU32::new(17).unwrap();
        let client = LseClient::new("key").unwrap().with_chunk_bars(bars);

        assert_eq!(client.chunk_bars(), bars);
    }
}
