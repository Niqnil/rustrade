//! Hyperliquid spot metadata resolution.
//!
//! Fetches and caches the `spotMeta` endpoint to resolve human-readable pair names
//! (e.g., "HYPE/USDC") to the `@{index}` format required by WebSocket subscriptions.
//!
//! # Usage
//!
//! ```ignore
//! use rustrade_data::exchange::hyperliquid::spot_meta::SpotMetaResolver;
//!
//! // Create resolver (fetches spotMeta on first resolution)
//! let resolver = SpotMetaResolver::mainnet();
//!
//! // Resolve pair name to @index format
//! let coin = resolver.resolve("hype", "usdc").await?; // Returns "@107"
//! ```
//!
//! # Caching
//!
//! The resolver caches the spotMeta response at first resolution. The cache is
//! immutable after initialization — newly-listed spot pairs require a process
//! restart to be picked up.

use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::debug;

/// Hyperliquid mainnet info endpoint.
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";

/// Hyperliquid testnet info endpoint.
const INFO_URL_TESTNET: &str = "https://api.hyperliquid-testnet.xyz/info";

/// Error type for spot metadata resolution.
#[derive(Debug, thiserror::Error)]
pub enum SpotMetaError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Token not found: {0}")]
    TokenNotFound(String),

    #[error("Pair not found: {0}/{1}")]
    PairNotFound(String, String),
}

/// Response from the spotMeta endpoint.
#[derive(Debug, Deserialize)]
struct SpotMetaResponse {
    tokens: Vec<TokenInfo>,
    universe: Vec<UniverseEntry>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    name: String,
    index: u32,
}

#[derive(Debug, Deserialize)]
struct UniverseEntry {
    // Required for serde deserialization of spotMeta universe entries; not read after construction.
    #[allow(dead_code)]
    name: String,
    index: u32,
    tokens: Vec<u32>,
}

/// Cached spot metadata for resolution.
#[derive(Debug, Clone)]
struct SpotMetaCache {
    /// Map from uppercase token name to token index
    token_indices: HashMap<String, u32>,
    /// Map from (base_token_idx, quote_token_idx) to spot pair index
    pair_indices: HashMap<(u32, u32), u32>,
}

impl SpotMetaCache {
    fn from_response(response: SpotMetaResponse) -> Self {
        let token_indices: HashMap<String, u32> = response
            .tokens
            .into_iter()
            .map(|t| (t.name.to_uppercase(), t.index))
            .collect();

        let pair_indices: HashMap<(u32, u32), u32> = response
            .universe
            .into_iter()
            .filter_map(|u| {
                if u.tokens.len() == 2 {
                    Some(((u.tokens[0], u.tokens[1]), u.index))
                } else {
                    None
                }
            })
            .collect();

        Self {
            token_indices,
            pair_indices,
        }
    }

    fn resolve(&self, base: &str, quote: &str) -> Result<String, SpotMetaError> {
        let base_upper = base.to_uppercase();
        let quote_upper = quote.to_uppercase();

        let base_idx = self
            .token_indices
            .get(&base_upper)
            .ok_or_else(|| SpotMetaError::TokenNotFound(base_upper.clone()))?;

        let quote_idx = self
            .token_indices
            .get(&quote_upper)
            .ok_or_else(|| SpotMetaError::TokenNotFound(quote_upper.clone()))?;

        // Try both orderings: empirically all SDK examples show [base, quote], but the API
        // docs don't formally guarantee this ordering. The defensive fallback lookup adds
        // minimal overhead (one extra HashMap lookup on miss) and prevents silent resolution
        // failures if the API ever deviates from the assumed ordering.
        let pair_idx = self
            .pair_indices
            .get(&(*base_idx, *quote_idx))
            .or_else(|| self.pair_indices.get(&(*quote_idx, *base_idx)))
            .ok_or_else(|| SpotMetaError::PairNotFound(base_upper, quote_upper))?;

        Ok(format!("@{}", pair_idx))
    }
}

/// Resolver for Hyperliquid spot pair names to `@{index}` format.
///
/// Fetches and caches the `spotMeta` endpoint. Thread-safe and cloneable.
/// Uses `OnceCell` for lock-free reads after the first initialization.
///
/// # Cache Lifetime
///
/// The cache is immutable after first initialization. Newly-listed spot pairs
/// will not be resolved until the process is restarted.
#[derive(Debug, Clone)]
pub struct SpotMetaResolver {
    client: Client,
    /// OnceCell ensures exactly one HTTP fetch; reads are lock-free after init.
    cache: Arc<OnceCell<Arc<SpotMetaCache>>>,
    testnet: bool,
}

impl Default for SpotMetaResolver {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SpotMetaResolver {
    fn new(testnet: bool) -> Self {
        Self {
            client: Client::new(),
            cache: Arc::new(OnceCell::new()),
            testnet,
        }
    }

    /// Create a resolver for mainnet.
    pub fn mainnet() -> Self {
        Self::new(false)
    }

    /// Create a resolver for testnet.
    pub fn testnet() -> Self {
        Self::new(true)
    }

    fn info_url(&self) -> &'static str {
        if self.testnet {
            INFO_URL_TESTNET
        } else {
            INFO_URL
        }
    }

    /// Fetch spotMeta from the network.
    async fn fetch(&self) -> Result<Arc<SpotMetaCache>, SpotMetaError> {
        let response: SpotMetaResponse = self
            .client
            .post(self.info_url())
            .json(&serde_json::json!({"type": "spotMeta"}))
            .send()
            .await?
            .json()
            .await?;

        let cache = SpotMetaCache::from_response(response);
        debug!(
            tokens = cache.token_indices.len(),
            pairs = cache.pair_indices.len(),
            "Fetched spot metadata"
        );

        Ok(Arc::new(cache))
    }

    /// Resolve a pair name to `@{index}` format.
    ///
    /// Fetches spotMeta on first call if not already cached. Concurrent callers
    /// during first init will race to populate the cache; `OnceCell` ensures
    /// exactly one wins. Subsequent calls are lock-free reads.
    ///
    /// # Arguments
    /// * `base` - Base token name (e.g., "hype", "HYPE")
    /// * `quote` - Quote token name (e.g., "usdc", "USDC")
    ///
    /// # Returns
    /// The spot index in `@{index}` format (e.g., "@107")
    ///
    /// # Cache Lifetime
    ///
    /// The cache is immutable after first initialization. Newly-listed spot pairs
    /// require a process restart to be resolved.
    pub async fn resolve(&self, base: &str, quote: &str) -> Result<String, SpotMetaError> {
        let cache = self.cache.get_or_try_init(|| self.fetch()).await?;
        cache.resolve(base, quote)
    }

    /// Check if a pair exists in the cache.
    ///
    /// Returns `None` if the cache hasn't been populated yet.
    pub fn pair_exists(&self, base: &str, quote: &str) -> Option<bool> {
        self.cache
            .get()
            .map(|cache| cache.resolve(base, quote).is_ok())
    }
}

/// Global spot metadata resolver instance.
///
/// Use this for convenience when you don't need separate testnet/mainnet resolvers.
static MAINNET_RESOLVER: std::sync::OnceLock<SpotMetaResolver> = std::sync::OnceLock::new();

/// Get the global mainnet spot metadata resolver.
pub fn mainnet_resolver() -> &'static SpotMetaResolver {
    MAINNET_RESOLVER.get_or_init(SpotMetaResolver::mainnet)
}

/// Resolve a spot pair name to `@{index}` format using the global mainnet resolver.
///
/// This is a convenience function for common use cases.
///
/// # Example
/// ```ignore
/// let coin = resolve_spot_pair("hype", "usdc").await?; // "@107"
/// ```
pub async fn resolve_spot_pair(base: &str, quote: &str) -> Result<String, SpotMetaError> {
    mainnet_resolver().resolve(base, quote).await
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_resolve_hype_usdc() {
        let resolver = SpotMetaResolver::mainnet();
        let result = resolver.resolve("hype", "usdc").await.unwrap();
        assert_eq!(result, "@107");
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_resolve_purr_usdc() {
        let resolver = SpotMetaResolver::mainnet();
        let result = resolver.resolve("purr", "usdc").await.unwrap();
        assert_eq!(result, "@0");
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_resolve_case_insensitive() {
        let resolver = SpotMetaResolver::mainnet();
        let r1 = resolver.resolve("HYPE", "USDC").await.unwrap();
        let r2 = resolver.resolve("hype", "usdc").await.unwrap();
        let r3 = resolver.resolve("Hype", "Usdc").await.unwrap();
        // Verify all variants resolve to the correct index
        assert_eq!(r1, "@107");
        assert_eq!(r2, "@107");
        assert_eq!(r3, "@107");
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_resolve_invalid_token() {
        let resolver = SpotMetaResolver::mainnet();
        let result = resolver.resolve("NOTAREAL", "usdc").await;
        assert!(matches!(result, Err(SpotMetaError::TokenNotFound(_))));
    }
}
