//! Configuration for the Hyperliquid execution client.

use ethers::signers::{LocalWallet, Signer};
use hyperliquid_rust_sdk::BaseUrl;
pub use rustrade_instrument::hyperliquid::Network;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

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
    /// The network to trade on. [`Network::Mainnet`] trades **real funds**.
    pub network: Network,
    /// The builder-deployed (HIP-3) perpetual DEXs the perpetuals client trades besides
    /// Hyperliquid's default one, by the name Hyperliquid lists them under (`xyz`, `flx`), exactly
    /// as spelled there. Empty by default: only the default DEX.
    ///
    /// The perpetuals client reads each one's markets when it connects, and refuses to connect
    /// if Hyperliquid does not list one. See [`HyperliquidClient`](super::HyperliquidClient)'s
    /// HIP-3 docs. The spot client ignores this.
    pub dexes: Vec<SmolStr>,
}

impl HyperliquidConfig {
    /// Create a new config with the given wallet on `network` ([`Network::Mainnet`] trades **real
    /// funds**), trading only Hyperliquid's default perpetual DEX.
    pub fn new(wallet: LocalWallet, network: Network) -> Self {
        Self {
            wallet,
            network,
            dexes: Vec::new(),
        }
    }

    /// Trade the builder-deployed (HIP-3) perpetual DEXs `dexes` as well, replacing any named
    /// before. See [`dexes`](Self::dexes).
    #[must_use]
    pub fn with_dexes<Dex: Into<SmolStr>>(mut self, dexes: impl IntoIterator<Item = Dex>) -> Self {
        self.dexes = dexes.into_iter().map(Into::into).collect();
        self
    }

    /// Build a config from environment variables.
    ///
    /// Reads:
    /// - `HYPERLIQUID_PRIVATE_KEY` (required) — hex-encoded private key (with or without `0x` prefix).
    /// - `HYPERLIQUID_TESTNET` (optional) — `"true"`/`"false"` (case-insensitive). **Absent ⇒ the
    ///   safe testnet environment.** Set `HYPERLIQUID_TESTNET=false` to target mainnet (real funds).
    /// - `HYPERLIQUID_DEXES` (optional) — the HIP-3 DEXs to trade, comma-separated (`xyz,flx`).
    ///   Spaces around a name and empty entries are ignored. Absent ⇒ none.
    ///
    /// # Errors
    ///
    /// Returns [`HyperliquidConfigError`] (never panics):
    /// - `HYPERLIQUID_PRIVATE_KEY` unset ([`MissingPrivateKey`](HyperliquidConfigError::MissingPrivateKey)),
    ///   non-UTF-8 ([`InvalidPrivateKeyVar`](HyperliquidConfigError::InvalidPrivateKeyVar)), or not a valid key
    ///   ([`InvalidPrivateKey`](HyperliquidConfigError::InvalidPrivateKey));
    /// - `HYPERLIQUID_TESTNET` is neither `true` nor `false`, or holds non-UTF-8
    ///   ([`InvalidTestnet`](HyperliquidConfigError::InvalidTestnet));
    /// - `HYPERLIQUID_DEXES` holds non-UTF-8 ([`InvalidDexes`](HyperliquidConfigError::InvalidDexes)).
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

        let network = match std::env::var("HYPERLIQUID_TESTNET") {
            Ok(value) => match crate::parse_env_bool(&value) {
                Some(true) => Network::Testnet,
                Some(false) => Network::Mainnet,
                None => return Err(HyperliquidConfigError::InvalidTestnet(value)),
            },
            Err(std::env::VarError::NotPresent) => Network::Testnet,
            // The toggle value is not secret, so echo it (lossily) like the parse-failure arm above —
            // an actionable "got X" beats a hardcoded sentinel.
            Err(std::env::VarError::NotUnicode(value)) => {
                return Err(HyperliquidConfigError::InvalidTestnet(
                    value.to_string_lossy().into_owned(),
                ));
            }
        };

        let dexes = match std::env::var("HYPERLIQUID_DEXES") {
            Ok(value) => value
                .split(',')
                .map(str::trim)
                .filter(|dex| !dex.is_empty())
                .map(SmolStr::from)
                .collect(),
            Err(std::env::VarError::NotPresent) => Vec::new(),
            Err(std::env::VarError::NotUnicode(value)) => {
                return Err(HyperliquidConfigError::InvalidDexes(
                    value.to_string_lossy().into_owned(),
                ));
            }
        };

        Ok(Self::from_private_key(&private_key, network)?.with_dexes(dexes))
    }

    /// Create a config from a hex-encoded private key string.
    ///
    /// The private key can have an optional "0x" prefix. Trades only Hyperliquid's default
    /// perpetual DEX; add HIP-3 ones with [`with_dexes`](Self::with_dexes).
    pub fn from_private_key(
        private_key: &str,
        network: Network,
    ) -> Result<Self, HyperliquidConfigError> {
        let key = private_key.strip_prefix("0x").unwrap_or(private_key);

        let wallet: LocalWallet = key
            .parse()
            .map_err(|e| HyperliquidConfigError::InvalidPrivateKey(format!("{e}")))?;

        Ok(Self::new(wallet, network))
    }

    /// Returns the wallet address as a hex string (0x-prefixed).
    pub fn wallet_address_hex(&self) -> String {
        format!("{:#x}", self.wallet.address())
    }

    /// The SDK's base URL for [`network`](Self::network).
    pub(super) fn base_url(&self) -> BaseUrl {
        match self.network {
            Network::Mainnet => BaseUrl::Mainnet,
            Network::Testnet => BaseUrl::Testnet,
        }
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

    #[error("HYPERLIQUID_DEXES environment variable is not valid UTF-8: {0}")]
    InvalidDexes(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_from_private_key_with_prefix() {
        let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let config = HyperliquidConfig::from_private_key(key, Network::Mainnet).unwrap();
        assert_eq!(config.network, Network::Mainnet);
        assert!(config.dexes.is_empty());
        assert!(config.wallet_address_hex().starts_with("0x"));
    }

    #[test]
    fn test_from_private_key_without_prefix() {
        let key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let config = HyperliquidConfig::from_private_key(key, Network::Testnet).unwrap();
        assert_eq!(config.network, Network::Testnet);
    }

    #[test]
    fn test_invalid_private_key() {
        let result = HyperliquidConfig::from_private_key("invalid", Network::Mainnet);
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
                assert_eq!(
                    cfg.network,
                    Network::Testnet,
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
                assert_eq!(cfg.network, Network::Mainnet);
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
    #[serial_test::serial]
    fn from_env_reads_the_dexes_to_trade() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_TESTNET", None),
                ("HYPERLIQUID_DEXES", Some(" xyz, ,flx ,")),
            ],
            || {
                let cfg = HyperliquidConfig::from_env().unwrap();
                assert_eq!(cfg.dexes, ["xyz", "flx"]);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn from_env_trades_no_dex_when_none_is_named() {
        temp_env::with_vars(
            [
                ("HYPERLIQUID_PRIVATE_KEY", Some(TEST_KEY)),
                ("HYPERLIQUID_DEXES", None),
            ],
            || assert!(HyperliquidConfig::from_env().unwrap().dexes.is_empty()),
        );
    }

    #[test]
    fn with_dexes_replaces_the_dexes_named_before() {
        let config = HyperliquidConfig::from_private_key(TEST_KEY, Network::Testnet)
            .unwrap()
            .with_dexes(["xyz"])
            .with_dexes(["flx", "km"]);
        assert_eq!(config.dexes, ["flx", "km"]);
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
