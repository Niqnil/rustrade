//! Configuration for the Hyperliquid execution client.

use ethers::signers::{LocalWallet, Signer};
use serde::{Deserialize, Serialize};

/// Configuration for the Hyperliquid execution client.
///
/// # Example
///
/// ```ignore
/// use rustrade_execution::client::hyperliquid::config::HyperliquidConfig;
/// use std::env;
///
/// let config = HyperliquidConfig::from_env().expect("failed to load Hyperliquid config from env");
/// ```
#[derive(Debug, Clone)]
pub struct HyperliquidConfig {
    /// The wallet containing the private key for signing (ethers LocalWallet).
    pub wallet: LocalWallet,
    /// Whether to use testnet (true) or mainnet (false).
    pub testnet: bool,
}

impl HyperliquidConfig {
    /// Create a new config with the given wallet and network selection (`testnet = true` ⇒ testnet,
    /// `false` ⇒ mainnet with **real funds**).
    pub fn new(wallet: LocalWallet, testnet: bool) -> Self {
        Self { wallet, testnet }
    }

    /// Build a config from environment variables.
    ///
    /// Reads:
    /// - `HYPERLIQUID_PRIVATE_KEY` (required) — hex-encoded private key (with or without `0x` prefix).
    /// - `HYPERLIQUID_TESTNET` (optional) — `"true"`/`"false"` (case-insensitive). **Absent ⇒ the
    ///   safe testnet environment.** Set `HYPERLIQUID_TESTNET=false` to target mainnet (real funds).
    ///
    /// # Errors
    ///
    /// Returns [`HyperliquidConfigError`] (never panics):
    /// - `HYPERLIQUID_PRIVATE_KEY` unset ([`MissingPrivateKey`](HyperliquidConfigError::MissingPrivateKey)),
    ///   non-UTF-8 ([`InvalidPrivateKeyVar`](HyperliquidConfigError::InvalidPrivateKeyVar)), or not a valid key
    ///   ([`InvalidPrivateKey`](HyperliquidConfigError::InvalidPrivateKey));
    /// - `HYPERLIQUID_TESTNET` is neither `true` nor `false`, or holds non-UTF-8
    ///   ([`InvalidTestnet`](HyperliquidConfigError::InvalidTestnet)).
    pub fn from_env() -> Result<Self, HyperliquidConfigError> {
        let private_key = match std::env::var("HYPERLIQUID_PRIVATE_KEY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => {
                return Err(HyperliquidConfigError::MissingPrivateKey);
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(HyperliquidConfigError::InvalidPrivateKeyVar);
            }
        };

        let testnet = match std::env::var("HYPERLIQUID_TESTNET") {
            Ok(value) => crate::parse_env_bool(&value)
                .ok_or(HyperliquidConfigError::InvalidTestnet(value))?,
            Err(std::env::VarError::NotPresent) => true,
            // The toggle value is not secret, so echo it (lossily) like the parse-failure arm above —
            // an actionable "got X" beats a hardcoded sentinel.
            Err(std::env::VarError::NotUnicode(value)) => {
                return Err(HyperliquidConfigError::InvalidTestnet(
                    value.to_string_lossy().into_owned(),
                ));
            }
        };

        Self::from_private_key(&private_key, testnet)
    }

    /// Create a config from a hex-encoded private key string.
    ///
    /// The private key can have an optional "0x" prefix.
    pub fn from_private_key(
        private_key: &str,
        testnet: bool,
    ) -> Result<Self, HyperliquidConfigError> {
        let key = private_key.strip_prefix("0x").unwrap_or(private_key);

        let wallet: LocalWallet = key
            .parse()
            .map_err(|e| HyperliquidConfigError::InvalidPrivateKey(format!("{e}")))?;

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
/// Use [`HyperliquidConfig::from_env`] to load credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidConfigFile {
    /// Whether to use testnet (true) or mainnet (false).
    ///
    /// An absent `testnet` field defaults to the **safe** testnet environment (`true`), matching
    /// [`HyperliquidConfig::from_env`] and the Alpaca/Binance config files.
    #[serde(default = "default_testnet")]
    pub testnet: bool,
}

/// Serde default for [`HyperliquidConfigFile::testnet`]: an absent `testnet` field deserializes to
/// the **safe** testnet environment (`true`).
///
/// `#[serde(default = "…")]` requires a named function (it cannot take a literal), so this exists
/// purely to supply that default to the derive.
fn default_testnet() -> bool {
    true
}

/// Errors that can occur when creating a HyperliquidConfig.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum HyperliquidConfigError {
    #[error("HYPERLIQUID_PRIVATE_KEY environment variable not set")]
    MissingPrivateKey,

    // No payload: the raw value is private-key material, so it must never be echoed into an error
    // message or log. The `Var` suffix distinguishes "the env var is non-UTF-8" from
    // `InvalidPrivateKey` below ("the var is readable but not a valid key").
    #[error("HYPERLIQUID_PRIVATE_KEY environment variable is not valid UTF-8")]
    InvalidPrivateKeyVar,

    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),

    #[error("HYPERLIQUID_TESTNET must be true or false, got {0}")]
    InvalidTestnet(String),
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

    // A valid secp256k1 key (Anvil/Hardhat account #0) for `from_env` tests.
    const TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    #[serial_test::serial]
    fn test_from_env_defaults_to_testnet() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_TESTNET", None),
            ],
            || {
                let cfg = HyperliquidConfig::from_env().unwrap();
                assert!(
                    cfg.testnet,
                    "absent toggle must default to the safe testnet"
                );
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_from_env_accepts_explicit_mainnet() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_TESTNET", Some("false")),
            ],
            || {
                let cfg = HyperliquidConfig::from_env().unwrap();
                assert!(!cfg.testnet);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_from_env_rejects_invalid_testnet() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_TESTNET", Some("maybe")),
            ],
            || {
                let err = HyperliquidConfig::from_env().unwrap_err();
                assert!(
                    matches!(err, HyperliquidConfigError::InvalidTestnet(value) if value == "maybe")
                );
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_from_env_drops_numeric_one_special_case() {
        // "1" was previously coerced to testnet; the shared env-bool policy is true/false-only,
        // so it must now be rejected rather than silently accepted.
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_TESTNET", Some("1")),
            ],
            || {
                let err = HyperliquidConfig::from_env().unwrap_err();
                assert!(
                    matches!(err, HyperliquidConfigError::InvalidTestnet(value) if value == "1")
                );
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_from_env_requires_private_key() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", None),
                ("HYPERLIQUID_TESTNET", Some("true")),
            ],
            || {
                let err = HyperliquidConfig::from_env().unwrap_err();
                assert!(matches!(err, HyperliquidConfigError::MissingPrivateKey));
            },
        );
    }

    #[test]
    fn test_config_file_absent_testnet_defaults_to_testnet() {
        let file: HyperliquidConfigFile = serde_json::from_str("{}").unwrap();
        assert!(
            file.testnet,
            "absent `testnet` field must default to safe testnet"
        );
    }
}
