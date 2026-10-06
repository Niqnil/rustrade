//! Hyperliquid coin names, shared by its data and execution integrations (behind the
//! `hyperliquid` feature).
//!
//! Hyperliquid names every market by a *coin*:
//! - a perpetual by its asset, as Hyperliquid spells it (`BTC`, `kPEPE`);
//! - a builder-deployed (HIP-3) perpetual by its deployer and asset (`xyz:TSLA`);
//! - a spot pair by `@{index}` (`@107` is HYPE/USDC), except PURR/USDC, which is named
//!   `PURR/USDC`.
//!
//! [`CoinKind::of`] tells these apart from the name alone. A spot coin such as `@107` does not
//! name its assets, so [`SpotPairs`], read from Hyperliquid's `spotMeta` info response, resolves
//! it to its base and quote.

use serde::Deserialize;
use smol_str::SmolStr;
use std::collections::HashMap;

/// The kind of market a Hyperliquid coin names, judged from the shape of its name alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoinKind {
    /// A perpetual: an asset (`BTC`, `kPEPE`), or a builder-deployed (HIP-3) perpetual named
    /// `deployer:ASSET` (`xyz:TSLA`).
    Perp,
    /// A spot pair: `@{index}` (`@107`), or `BASE/QUOTE` (`PURR/USDC`).
    Spot,
    /// Neither, such as an outcome coin (`#12`) or a shape Hyperliquid adds later.
    ///
    /// Never taken for a perpetual, so a new kind of market is not reported as one.
    Unknown,
}

impl CoinKind {
    /// The kind of market `coin` names.
    ///
    /// An asset, deployer or token name is one or more ASCII letters and digits, as every name
    /// Hyperliquid lists is; any other name is [`CoinKind::Unknown`].
    pub fn of(coin: &str) -> Self {
        fn is_name(name: &str) -> bool {
            !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_alphanumeric())
        }

        let (kind, valid) = if let Some(index) = coin.strip_prefix('@') {
            (
                Self::Spot,
                !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit()),
            )
        } else if let Some((base, quote)) = coin.split_once('/') {
            (Self::Spot, is_name(base) && is_name(quote))
        } else if let Some((deployer, asset)) = coin.split_once(':') {
            (Self::Perp, is_name(deployer) && is_name(asset))
        } else {
            (Self::Perp, is_name(coin))
        };

        if valid { kind } else { Self::Unknown }
    }
}

/// Hyperliquid's spot pairs, by coin, as its `spotMeta` info response lists them.
///
/// Deserializes from that response, `{"universe": [...], "tokens": [...]}`. A pair whose tokens
/// the response does not list is left out, so looking up its coin returns `None`.
///
/// Hyperliquid lists new pairs as they launch, so a set read earlier can lack a pair the account
/// now trades. A caller that meets a spot coin missing from its set should read `spotMeta` again
/// before treating the coin as unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "SpotMeta")]
pub struct SpotPairs(HashMap<SmolStr, SpotPair>);

impl SpotPairs {
    /// The pair Hyperliquid names `coin` (`@107`, or `PURR/USDC`), if listed.
    ///
    /// Exact: Hyperliquid's coin names are case-sensitive.
    pub fn get(&self, coin: &str) -> Option<&SpotPair> {
        self.0.get(coin)
    }

    /// The number of pairs listed.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no pair is listed.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every pair listed, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &SpotPair> {
        self.0.values()
    }
}

/// One Hyperliquid spot pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpotPair {
    coin: SmolStr,
    index: u32,
    base: SmolStr,
    quote: SmolStr,
}

impl SpotPair {
    /// The coin Hyperliquid names the pair by (`@107`, or `PURR/USDC`).
    pub fn coin(&self) -> &str {
        &self.coin
    }

    /// The pair's index in `spotMeta`'s universe. An order addresses it as asset
    /// `10000 + index`.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// The base token's name, as Hyperliquid spells it (`HYPE`).
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The quote token's name, as Hyperliquid spells it (`USDC`).
    pub fn quote(&self) -> &str {
        &self.quote
    }
}

/// The `spotMeta` info response, as far as [`SpotPairs`] reads it.
#[derive(Deserialize)]
struct SpotMeta {
    universe: Vec<SpotMetaPair>,
    tokens: Vec<SpotMetaToken>,
}

#[derive(Deserialize)]
struct SpotMetaPair {
    name: SmolStr,
    index: u32,
    /// The base and quote tokens' `index`es. These are not positions in `tokens`, which skips
    /// indexes.
    tokens: [u32; 2],
}

#[derive(Deserialize)]
struct SpotMetaToken {
    name: SmolStr,
    index: u32,
}

impl From<SpotMeta> for SpotPairs {
    fn from(meta: SpotMeta) -> Self {
        let tokens: HashMap<u32, SmolStr> = meta
            .tokens
            .into_iter()
            .map(|token| (token.index, token.name))
            .collect();

        Self(
            meta.universe
                .into_iter()
                .filter_map(|pair| {
                    let [base, quote] = pair.tokens;
                    let pair = SpotPair {
                        index: pair.index,
                        base: tokens.get(&base)?.clone(),
                        quote: tokens.get(&quote)?.clone(),
                        coin: pair.name,
                    };
                    Some((pair.coin.clone(), pair))
                })
                .collect(),
        )
    }
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A `spotMeta` response in Hyperliquid's shape, with synthetic values. Token indexes skip
    /// numbers, so they are not positions in `tokens`; one pair is quoted in a token other than
    /// USDC, and one names a token the response does not list.
    const SPOT_META: &str = r#"{
        "universe": [
            {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true},
            {"name": "@107", "tokens": [150, 0], "index": 107, "isCanonical": false},
            {"name": "@207", "tokens": [150, 268], "index": 207, "isCanonical": false},
            {"name": "@300", "tokens": [999, 0], "index": 300, "isCanonical": false}
        ],
        "tokens": [
            {"name": "USDC", "szDecimals": 8, "weiDecimals": 8, "index": 0, "isCanonical": true},
            {"name": "PURR", "szDecimals": 0, "weiDecimals": 5, "index": 1, "isCanonical": true},
            {"name": "HYPE", "szDecimals": 2, "weiDecimals": 8, "index": 150, "isCanonical": false},
            {"name": "USDT0", "szDecimals": 2, "weiDecimals": 8, "index": 268, "isCanonical": false}
        ]
    }"#;

    fn pairs() -> SpotPairs {
        serde_json::from_str(SPOT_META).unwrap()
    }

    #[test]
    fn spot_pairs_resolve_a_coin_to_its_tokens_by_index() {
        let pairs = pairs();

        let hype = pairs.get("@107").unwrap();
        assert_eq!(
            (hype.coin(), hype.index(), hype.base(), hype.quote()),
            ("@107", 107, "HYPE", "USDC")
        );
        let hype_usdt0 = pairs.get("@207").unwrap();
        assert_eq!((hype_usdt0.base(), hype_usdt0.quote()), ("HYPE", "USDT0"));
        let purr = pairs.get("PURR/USDC").unwrap();
        assert_eq!(
            (purr.index(), purr.base(), purr.quote()),
            (0, "PURR", "USDC")
        );
    }

    #[test]
    fn spot_pairs_leave_out_a_pair_with_an_unlisted_token() {
        let pairs = pairs();

        assert_eq!(pairs.get("@300"), None);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs.iter().count(), 3);
    }

    #[test]
    fn spot_pairs_match_a_coin_exactly() {
        let pairs = pairs();

        assert_eq!(pairs.get("purr/usdc"), None);
        assert_eq!(pairs.get("@0"), None, "PURR/USDC is named only PURR/USDC");
        assert_eq!(pairs.get("@999"), None);
        assert!(SpotPairs::default().is_empty());
    }

    #[test]
    fn coin_kind_judges_a_coin_by_its_shape() {
        for (coin, kind) in [
            ("BTC", CoinKind::Perp),
            ("kPEPE", CoinKind::Perp),
            ("xyz:TSLA", CoinKind::Perp),
            ("xyz:XYZ100", CoinKind::Perp),
            ("@107", CoinKind::Spot),
            ("@0", CoinKind::Spot),
            ("PURR/USDC", CoinKind::Spot),
            ("#12", CoinKind::Unknown),
            ("+12", CoinKind::Unknown),
            ("", CoinKind::Unknown),
            ("@", CoinKind::Unknown),
            ("@1x", CoinKind::Unknown),
            ("PURR/", CoinKind::Unknown),
            ("/USDC", CoinKind::Unknown),
            ("A/B/C", CoinKind::Unknown),
            (":TSLA", CoinKind::Unknown),
            ("xyz:", CoinKind::Unknown),
            ("a:b:c", CoinKind::Unknown),
            ("BTC-USD", CoinKind::Unknown),
        ] {
            assert_eq!(CoinKind::of(coin), kind, "{coin:?}");
        }
    }
}
