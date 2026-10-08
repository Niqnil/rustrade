//! Hyperliquid's market metadata: the coin each perpetual and spot pair is subscribed by.
//!
//! A Hyperliquid subscription names its market by a coin, exactly as Hyperliquid spells it, and
//! a wrong coin gets no data. Worse, the server closes the connection, ending every other
//! subscription on it too. [`HyperliquidMeta`] reads the coins from Hyperliquid's info endpoint
//! and finds them by name:
//!
//! - [`HyperliquidMeta::perp_coin`] takes a perpetual's name in any ASCII case and returns the
//!   venue's spelling (`KPEPE` finds `kPEPE`).
//! - [`HyperliquidMeta::spot_pair`] takes a pair's base and quote tokens (`HYPE`, `USDC`) and
//!   returns its coin (`@107`).
//!
//! Build the subscription's instrument from the result with
//! [`MarketInstrumentData::hyperliquid_perp`](crate::instrument::MarketInstrumentData::hyperliquid_perp)
//! or
//! [`MarketInstrumentData::hyperliquid_spot`](crate::instrument::MarketInstrumentData::hyperliquid_spot).
//!
//! ```ignore
//! use rustrade_data::{
//!     exchange::hyperliquid::{HyperliquidMeta, Network},
//!     instrument::MarketInstrumentData,
//! };
//!
//! // The default perpetuals and spot pairs, plus builder deployer `xyz`'s perpetuals.
//! let meta = HyperliquidMeta::fetch(Network::Mainnet, &["xyz"]).await?;
//!
//! let pepe = meta.perp_coin("KPEPE").ok_or("no such perpetual")?;
//! let tsla = meta.perp_coin("xyz:TSLA").ok_or("no such perpetual")?;
//! let hype = meta.spot_pair("HYPE", "USDC").ok_or("no such pair")?;
//!
//! let perps = [
//!     MarketInstrumentData::hyperliquid_perp(0, pepe),
//!     MarketInstrumentData::hyperliquid_perp(1, tsla),
//! ];
//! let spot = MarketInstrumentData::hyperliquid_spot(2, hype);
//! ```
//!
//! # Snapshot
//!
//! A [`HyperliquidMeta`] is what the info endpoint listed when it was read. A market listed
//! since is missing from it, so read it again to find one.

use rustrade_instrument::hyperliquid::{Network, PerpCoin, Perps, SpotPair, SpotPairs};
use serde::de::DeserializeOwned;
use serde_json::json;
use smol_str::SmolStr;
use std::time::Duration;
use tracing::debug;

/// Hyperliquid's mainnet info endpoint.
const INFO_URL_MAINNET: &str = "https://api.hyperliquid.xyz/info";

/// Hyperliquid's testnet info endpoint.
const INFO_URL_TESTNET: &str = "https://api.hyperliquid-testnet.xyz/info";

/// How long one info request may take before [`HyperliquidMeta::fetch`] gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Why [`HyperliquidMeta::fetch`] failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HyperliquidMetaError {
    /// An info request failed, timed out, or returned a body that is not the expected response.
    #[error("Hyperliquid info request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Hyperliquid lists no builder deployer by this name. A name that is not one or more ASCII
    /// letters and digits, as every deployer's is, is rejected before any request is sent.
    #[error("Hyperliquid lists no builder deployer named {0:?}")]
    UnknownDeployer(SmolStr),
}

/// Hyperliquid's perpetuals and spot pairs, read from its info endpoint. See the
/// [module docs](self).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HyperliquidMeta {
    perps: Perps,
    spot_pairs: SpotPairs,
}

impl HyperliquidMeta {
    /// Read the perpetuals and spot pairs `network` lists.
    ///
    /// The perpetuals are the default ones plus those of each builder deployer (HIP-3) named in
    /// `deployers` (such as `xyz`), each named in ASCII letters and digits. Hyperliquid has a
    /// `meta` response per deployer, so only the deployers named are read. The requests run
    /// concurrently, each abandoned after 10 seconds.
    ///
    /// The data streams connect to mainnet only, so a testnet read serves other uses, such as
    /// orders on testnet.
    ///
    /// # Errors
    ///
    /// [`HyperliquidMetaError::UnknownDeployer`] if Hyperliquid lists no deployer of one of
    /// those names, and [`HyperliquidMetaError::Http`] if a request fails.
    pub async fn fetch(network: Network, deployers: &[&str]) -> Result<Self, HyperliquidMetaError> {
        let url = match network {
            Network::Mainnet => INFO_URL_MAINNET,
            Network::Testnet => INFO_URL_TESTNET,
        };
        Self::fetch_from(url, deployers).await
    }

    /// [`Self::fetch`] from the info endpoint at `url`.
    async fn fetch_from(url: &str, deployers: &[&str]) -> Result<Self, HyperliquidMetaError> {
        // Checked before any request: an empty or malformed name would read some other `meta`,
        // as an empty `dex` reads the default perpetuals'.
        if let Some(&malformed) = deployers.iter().find(|deployer| {
            deployer.is_empty() || !deployer.bytes().all(|byte| byte.is_ascii_alphanumeric())
        }) {
            return Err(HyperliquidMetaError::UnknownDeployer(malformed.into()));
        }

        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;

        let deployer_perps = futures::future::try_join_all(deployers.iter().map(|&deployer| {
            let client = &client;
            async move {
                // Hyperliquid answers `null` for a deployer it does not list.
                info::<Option<Perps>>(client, url, json!({"type": "meta", "dex": deployer}))
                    .await?
                    .ok_or_else(|| HyperliquidMetaError::UnknownDeployer(deployer.into()))
            }
        }));
        let (perps, spot_pairs, deployer_perps) = futures::try_join!(
            info::<Perps>(&client, url, json!({"type": "meta"})),
            info::<SpotPairs>(&client, url, json!({"type": "spotMeta"})),
            deployer_perps,
        )?;

        let meta = Self::new(
            std::iter::once(perps).chain(deployer_perps).collect(),
            spot_pairs,
        );
        debug!(
            perps = meta.perps.len(),
            spot_pairs = meta.spot_pairs.len(),
            ?deployers,
            "read Hyperliquid market metadata"
        );
        Ok(meta)
    }

    /// Market metadata from perpetuals and spot pairs already read, such as from a stored
    /// response.
    pub fn new(perps: Perps, spot_pairs: SpotPairs) -> Self {
        Self { perps, spot_pairs }
    }

    /// The perpetual named `name`, in the venue's spelling or differing from it only in ASCII
    /// case. A builder-deployed perpetual is named with its deployer (`xyz:TSLA`), and is found
    /// only if that deployer was read. See [`Perps::get`].
    pub fn perp_coin(&self, name: &str) -> Option<&PerpCoin> {
        self.perps.get(name)
    }

    /// The spot pair of `base` quoted in `quote`, named by their tokens in the venue's spelling
    /// or differing from it only in ASCII case. See [`SpotPairs::find`].
    pub fn spot_pair(&self, base: &str, quote: &str) -> Option<&SpotPair> {
        self.spot_pairs.find(base, quote)
    }

    /// Every perpetual read.
    pub fn perps(&self) -> &Perps {
        &self.perps
    }

    /// Every spot pair read.
    pub fn spot_pairs(&self) -> &SpotPairs {
        &self.spot_pairs
    }
}

/// Post `request` to the info endpoint at `url` and parse the response.
async fn info<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    request: serde_json::Value,
) -> Result<T, HyperliquidMetaError> {
    Ok(client
        .post(url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method},
    };

    /// Info responses in Hyperliquid's shape, with synthetic values.
    const META: &str = r#"{"universe": [{"name": "BTC"}, {"name": "kPEPE"}]}"#;
    const META_XYZ: &str = r#"{"universe": [{"name": "xyz:TSLA"}]}"#;
    const SPOT_META: &str = r#"{
        "universe": [
            {"name": "PURR/USDC", "tokens": [1, 0], "index": 0},
            {"name": "@107", "tokens": [150, 0], "index": 107}
        ],
        "tokens": [
            {"name": "USDC", "szDecimals": 8, "index": 0},
            {"name": "PURR", "szDecimals": 0, "index": 1},
            {"name": "HYPE", "szDecimals": 2, "index": 150}
        ]
    }"#;

    async fn info_server() -> MockServer {
        let server = MockServer::start().await;
        for (request, response) in [
            (json!({"type": "meta"}), META),
            (json!({"type": "meta", "dex": "xyz"}), META_XYZ),
            (json!({"type": "meta", "dex": "nope"}), "null"),
            (json!({"type": "spotMeta"}), SPOT_META),
        ] {
            Mock::given(method("POST"))
                .and(body_json(request))
                .respond_with(ResponseTemplate::new(200).set_body_raw(response, "application/json"))
                .mount(&server)
                .await;
        }
        server
    }

    #[tokio::test]
    async fn fetch_reads_the_default_perps_the_named_deployers_and_spot() {
        let server = info_server().await;

        let meta = HyperliquidMeta::fetch_from(&server.uri(), &["xyz"])
            .await
            .unwrap();

        assert_eq!(meta.perps().len(), 3);
        assert_eq!(meta.perp_coin("KPEPE").unwrap().coin(), "kPEPE");
        assert_eq!(meta.perp_coin("xyz:TSLA").unwrap().deployer(), Some("xyz"));
        assert_eq!(meta.spot_pair("hype", "usdc").unwrap().coin(), "@107");
        assert_eq!(meta.spot_pair("PURR", "USDC").unwrap().coin(), "PURR/USDC");
        assert_eq!(meta.spot_pairs().len(), 2);
    }

    #[tokio::test]
    async fn fetch_reads_no_deployer_unless_named() {
        let server = info_server().await;

        let meta = HyperliquidMeta::fetch_from(&server.uri(), &[])
            .await
            .unwrap();

        assert_eq!(meta.perps().len(), 2);
        assert_eq!(meta.perp_coin("xyz:TSLA"), None);
    }

    #[tokio::test]
    async fn fetch_rejects_a_deployer_hyperliquid_does_not_list() {
        let server = info_server().await;

        for deployer in ["nope", "", "xyz:"] {
            let error = HyperliquidMeta::fetch_from(&server.uri(), &["xyz", deployer])
                .await
                .unwrap_err();
            assert!(
                matches!(&error, HyperliquidMetaError::UnknownDeployer(name) if name == deployer),
                "{deployer:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn fetch_sends_no_request_for_a_malformed_deployer() {
        let server = MockServer::start().await;

        let error = HyperliquidMeta::fetch_from(&server.uri(), &["xyz", "a-b"])
            .await
            .unwrap_err();

        assert!(
            matches!(&error, HyperliquidMetaError::UnknownDeployer(name) if name == "a-b"),
            "{error:?}"
        );
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn fetch_fails_on_a_response_that_is_not_the_metadata() {
        for body in ["null", "{}"] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(body_json(json!({"type": "meta"})))
                .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(body_json(json!({"type": "spotMeta"})))
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(SPOT_META, "application/json"),
                )
                .mount(&server)
                .await;

            let error = HyperliquidMeta::fetch_from(&server.uri(), &[])
                .await
                .unwrap_err();

            assert!(
                matches!(error, HyperliquidMetaError::Http(_)),
                "{body}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn fetch_fails_on_an_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let error = HyperliquidMeta::fetch_from(&server.uri(), &[])
            .await
            .unwrap_err();

        assert!(matches!(error, HyperliquidMetaError::Http(_)), "{error:?}");
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn fetch_reads_mainnet() {
        let meta = HyperliquidMeta::fetch(Network::Mainnet, &["xyz"])
            .await
            .unwrap();

        assert_eq!(meta.perp_coin("KPEPE").unwrap().coin(), "kPEPE");
        assert_eq!(meta.perp_coin("xyz:TSLA").unwrap().coin(), "xyz:TSLA");
        assert_eq!(meta.spot_pair("HYPE", "USDC").unwrap().coin(), "@107");
        assert_eq!(meta.spot_pair("PURR", "USDC").unwrap().coin(), "PURR/USDC");
    }
}
