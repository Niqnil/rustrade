//! Resolution of Hyperliquid spot coins (`@107`) to the pairs they name.

use crate::error::UnindexedClientError;
use hyperliquid_rust_sdk::InfoClient;
use parking_lot::RwLock;
use rustrade_instrument::hyperliquid::{CoinKind, SpotPair, SpotPairs};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{info, warn};

/// The shortest time between two reads of `spotMeta` prompted by a spot coin missing from it.
///
/// Bounds the reads a coin that never resolves can cause to one per interval.
const REFETCH_INTERVAL: Duration = Duration::from_secs(10);

/// Hyperliquid's spot pairs, read from `spotMeta` and read again when a spot coin is missing.
///
/// Cheap to clone: clones share the pairs and the refetch state.
#[derive(Debug, Clone)]
pub(super) struct SpotCoins(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    info_client: Arc<InfoClient>,
    pairs: RwLock<Arc<SpotPairs>>,
    /// When `spotMeta` was last read again, if it has been. Held across that read, so concurrent
    /// misses read it once.
    refetched: tokio::sync::Mutex<Option<Instant>>,
}

impl SpotCoins {
    /// Read `spotMeta` through `info_client`.
    ///
    /// # Errors
    ///
    /// When the request fails or its response does not parse.
    pub(super) async fn fetch(info_client: Arc<InfoClient>) -> Result<Self, UnindexedClientError> {
        let pairs = fetch_spot_pairs(&info_client).await?;
        Ok(Self(Arc::new(Inner {
            info_client,
            pairs: RwLock::new(Arc::new(pairs)),
            refetched: tokio::sync::Mutex::new(None),
        })))
    }

    /// The pairs, read again first if one of `coins` is a spot coin they lack.
    ///
    /// Reads `spotMeta` at most once per [`REFETCH_INTERVAL`], however many callers miss at once.
    /// A miss within that interval of the last read, or a read that fails, returns the pairs as
    /// they are, so a coin can still be missing from the result.
    pub(super) async fn covering<'a, Coins>(&self, coins: Coins) -> Arc<SpotPairs>
    where
        Coins: IntoIterator<Item = &'a str> + Clone,
    {
        let lacks_one = |pairs: &SpotPairs| {
            coins
                .clone()
                .into_iter()
                .any(|coin| CoinKind::of(coin) == CoinKind::Spot && pairs.get(coin).is_none())
        };

        let pairs = self.current();
        if !lacks_one(&pairs) {
            return pairs;
        }

        let mut refetched = self.0.refetched.lock().await;
        // Another caller may have read `spotMeta` while this one waited for the lock.
        let pairs = self.current();
        if !lacks_one(&pairs) || refetched.is_some_and(|at| at.elapsed() < REFETCH_INTERVAL) {
            return pairs;
        }

        let result = fetch_spot_pairs(&self.0.info_client).await;
        *refetched = Some(Instant::now());
        match result {
            Ok(fetched) => {
                info!(
                    pairs = fetched.len(),
                    "Read Hyperliquid spotMeta again for a spot coin it lacked"
                );
                let fetched = Arc::new(fetched);
                *self.0.pairs.write() = Arc::clone(&fetched);
                fetched
            }
            Err(error) => {
                warn!(%error, "Hyperliquid spotMeta could not be read again");
                pairs
            }
        }
    }

    fn current(&self) -> Arc<SpotPairs> {
        Arc::clone(&self.0.pairs.read())
    }
}

/// The pair `coin` names, `None` if it names a market other than a spot pair.
///
/// # Errors
///
/// [`UnindexedClientError::Internal`] when `coin` is a spot coin missing from `pairs`. A caller
/// reading a complete list must fail rather than leave it out: the list would be short with
/// nothing to say so.
pub(super) fn spot_pair<'a>(
    pairs: &'a SpotPairs,
    coin: &str,
) -> Result<Option<&'a SpotPair>, UnindexedClientError> {
    if CoinKind::of(coin) != CoinKind::Spot {
        return Ok(None);
    }
    pairs.get(coin).map(Some).ok_or_else(|| {
        UnindexedClientError::Internal(format!(
            "Hyperliquid spot coin {coin} is not in spotMeta, even after reading it again"
        ))
    })
}

async fn fetch_spot_pairs(info_client: &InfoClient) -> Result<SpotPairs, UnindexedClientError> {
    super::common::info(
        info_client,
        "spotMeta",
        r#"{"type":"spotMeta"}"#.to_string(),
    )
    .await
}

/// A `spotMeta` response in Hyperliquid's shape, with synthetic values: PURR/USDC under its own
/// name, HYPE/USDC as `@107`, HYPE/USDT0 as `@207`.
#[cfg(test)]
pub(super) const TEST_SPOT_META: &str = r#"{
    "universe": [
        {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true},
        {"name": "@107", "tokens": [150, 0], "index": 107, "isCanonical": false},
        {"name": "@207", "tokens": [150, 268], "index": 207, "isCanonical": false}
    ],
    "tokens": [
        {"name": "USDC", "szDecimals": 8, "weiDecimals": 8, "index": 0, "isCanonical": true},
        {"name": "PURR", "szDecimals": 0, "weiDecimals": 5, "index": 1, "isCanonical": true},
        {"name": "HYPE", "szDecimals": 2, "weiDecimals": 8, "index": 150, "isCanonical": false},
        {"name": "USDT0", "szDecimals": 2, "weiDecimals": 8, "index": 268, "isCanonical": false}
    ]
}"#;

/// [`TEST_SPOT_META`]'s pairs.
#[cfg(test)]
pub(super) fn test_spot_pairs() -> SpotPairs {
    // Test code: a fixture that does not parse is a test bug.
    #[allow(clippy::expect_used)]
    serde_json::from_str(TEST_SPOT_META).expect("TEST_SPOT_META parses")
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::common::info_tests::info_client_against;
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// [`TEST_SPOT_META`] with HYPE/PURR listed as `@300`, as after a new listing.
    fn spot_meta_with_new_pair() -> String {
        let mut meta: serde_json::Value = serde_json::from_str(TEST_SPOT_META).unwrap();
        meta["universe"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!(
                {"name": "@300", "tokens": [150, 1], "index": 300, "isCanonical": false}
            ));
        meta.to_string()
    }

    /// A server answering `spotMeta` first with [`TEST_SPOT_META`], then with `later`.
    async fn serve(later: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_string_contains("spotMeta"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(TEST_SPOT_META, "application/json"),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(later)
            .with_priority(2)
            .mount(&server)
            .await;
        server
    }

    async fn spot_coins(server: &MockServer) -> SpotCoins {
        let info_client = info_client_against(server.uri()).await;
        SpotCoins::fetch(Arc::new(info_client)).await.unwrap()
    }

    async fn requests(server: &MockServer) -> usize {
        server.received_requests().await.unwrap().len()
    }

    fn new_pair() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_raw(spot_meta_with_new_pair(), "application/json")
    }

    #[tokio::test]
    async fn listed_and_non_spot_coins_read_spot_meta_once() {
        let server = serve(new_pair()).await;
        let coins = spot_coins(&server).await;

        let pairs = coins
            .covering(["@107", "PURR/USDC", "BTC", "xyz:TSLA", "#12"])
            .await;

        assert_eq!(pairs.get("@107").unwrap().base(), "HYPE");
        assert_eq!(requests(&server).await, 1, "only the read at creation");
    }

    #[tokio::test]
    async fn a_missing_spot_coin_reads_spot_meta_again_once_for_concurrent_callers() {
        let server = serve(new_pair()).await;
        let coins = spot_coins(&server).await;

        let (first, second) = tokio::join!(coins.covering(["@300"]), coins.covering(["@300"]));

        assert_eq!(first.get("@300").unwrap().quote(), "PURR");
        assert_eq!(second.get("@300").unwrap().quote(), "PURR");
        assert_eq!(requests(&server).await, 2, "one read again, shared");
        assert!(coins.covering(["@300"]).await.get("@300").is_some(), "kept");
        assert_eq!(requests(&server).await, 2);
    }

    #[tokio::test]
    async fn a_coin_still_missing_is_not_read_again_within_the_interval() {
        let server = serve(new_pair()).await;
        let coins = spot_coins(&server).await;

        assert!(coins.covering(["@999"]).await.get("@999").is_none());
        assert!(coins.covering(["@999"]).await.get("@999").is_none());

        assert_eq!(requests(&server).await, 2, "one read again, then none");
    }

    #[tokio::test]
    async fn a_failed_read_keeps_the_pairs() {
        let server = serve(ResponseTemplate::new(500)).await;
        let coins = spot_coins(&server).await;

        let pairs = coins.covering(["@300"]).await;

        assert!(pairs.get("@300").is_none());
        assert!(pairs.get("@107").is_some());
        assert_eq!(requests(&server).await, 2);
    }

    #[test]
    fn spot_pair_tells_other_markets_from_missing_spot_coins() {
        let pairs = test_spot_pairs();

        assert_eq!(spot_pair(&pairs, "@107").unwrap().unwrap().base(), "HYPE");
        assert!(spot_pair(&pairs, "BTC").unwrap().is_none());
        assert!(spot_pair(&pairs, "#12").unwrap().is_none());
        assert!(matches!(
            spot_pair(&pairs, "@999"),
            Err(UnindexedClientError::Internal(msg)) if msg.contains("@999")
        ));
    }
}
