//! Configuration for the Hyperliquid execution client.

use ethers::signers::{LocalWallet, Signer};
use serde::{Deserialize, Serialize};

/// Configuration for the Hyperliquid execution client.
///
/// # Example
///
/// ```ignore
/// use barter_execution::client::hyperliquid::config::HyperliquidConfig;
/// use std::env;
///
/// let config = HyperliquidConfig::from_env().expect("HYPERLIQUID_PRIVATE_KEY must be set");
/// ```
#[derive(Debug, Clone)]
pub struct HyperliquidConfig {
    /// The wallet containing the private key for signing (ethers LocalWallet).
    pub wallet: LocalWallet,
    /// Whether to use testnet (true) or mainnet (false).
    pub testnet: bool,
}

impl HyperliquidConfig {
    /// Create a new config with the given wallet and network selection.
    pub fn new(wallet: LocalWallet, testnet: bool) -> Self {
        Self { wallet, testnet }
    }

    /// Create a config from environment variables.
    ///
    /// Reads:
    /// - `HYPERLIQUID_PRIVATE_KEY`: Hex-encoded private key (with or without 0x prefix)
    /// - `HYPERLIQUID_TESTNET`: Optional, set to "true" for testnet (default: mainnet)
    ///
    /// # Errors
    ///
    /// Returns an error if `HYPERLIQUID_PRIVATE_KEY` is not set or invalid.
    pub fn from_env() -> Result<Self, ConfigError> {
        let private_key =
            std::env::var("HYPERLIQUID_PRIVATE_KEY").map_err(|_| ConfigError::MissingPrivateKey)?;

        let testnet = std::env::var("HYPERLIQUID_TESTNET")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        Self::from_private_key(&private_key, testnet)
    }

    /// Create a config from a hex-encoded private key string.
    ///
    /// The private key can have an optional "0x" prefix.
    pub fn from_private_key(private_key: &str, testnet: bool) -> Result<Self, ConfigError> {
        let key = private_key.strip_prefix("0x").unwrap_or(private_key);

        let wallet: LocalWallet = key
            .parse()
            .map_err(|e| ConfigError::InvalidPrivateKey(format!("{e}")))?;

        Ok(Self { wallet, testnet })
    }

    /// Returns the wallet address as a hex string (0x-prefixed).
    pub fn wallet_address_hex(&self) -> String {
        format!("{:#x}", self.wallet.address())
    }
}

/// Serializable version of HyperliquidConfig for config files.
///
/// Does NOT include the private key for security reasons.
/// Use `HyperliquidConfig::from_env()` to load credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidConfigFile {
    /// Whether to use testnet (true) or mainnet (false).
    #[serde(default)]
    pub testnet: bool,
}

/// Errors that can occur when creating a HyperliquidConfig.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("HYPERLIQUID_PRIVATE_KEY environment variable not set")]
    MissingPrivateKey,

    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_from_private_key_with_prefix() {
        let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let config = HyperliquidConfig::from_private_key(key, false).unwrap();
        assert!(!config.testnet);
        assert!(config.wallet_address_hex().starts_with("0x"));
    }

    #[test]
    fn test_from_private_key_without_prefix() {
        let key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let config = HyperliquidConfig::from_private_key(key, true).unwrap();
        assert!(config.testnet);
    }

    #[test]
    fn test_invalid_private_key() {
        let result = HyperliquidConfig::from_private_key("invalid", false);
        assert!(result.is_err());
    }
}
