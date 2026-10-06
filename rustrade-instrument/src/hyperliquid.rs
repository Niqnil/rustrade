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
//! it to its base and quote. [`Perps`], read from its `meta` info responses, lists the
//! perpetuals and finds one by name whatever its case.
//!
//! Every type here is I/O-free: they deserialize from the info responses, which the data and
//! execution integrations read.

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;

/// The kind of market a Hyperliquid coin names, judged from the shape of its name alone.
///
/// Non-exhaustive: a kind Hyperliquid adds later may get its own variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
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

/// A Hyperliquid network. Serialized in lower case (`"mainnet"`, `"testnet"`).
///
/// No default: whether an unset network should mean real funds or test funds is the caller's
/// choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    /// Hyperliquid's mainnet, where real funds trade.
    Mainnet,
    /// Hyperliquid's testnet. Its markets, and their indexes, differ from mainnet's.
    Testnet,
}

/// Hyperliquid's perpetuals, by coin, as its `meta` info responses list them.
///
/// Deserializes from one `meta` response, `{"universe": [...], ...}`, which lists either the
/// default perpetuals or, read with a `dex` field, one builder deployer's (HIP-3) perpetuals,
/// named `deployer:ASSET`. Collect several sets, such as the defaults and each deployer's, into
/// one with [`FromIterator`]. A coin listed twice keeps the later listing; the default and
/// deployers' sets cannot share a coin, as a deployer's carry its prefix.
///
/// Delisted perpetuals are kept, marked by [`PerpCoin::is_delisted`]: their names stay taken.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "PerpMeta")]
pub struct Perps(HashMap<SmolStr, PerpCoin>);

impl Perps {
    /// The perpetual named `name`, as Hyperliquid spells it (`kPEPE`, `xyz:TSLA`) or differing
    /// from that only in ASCII case (`KPEPE`, `XYZ:tsla`).
    ///
    /// An exact match wins. Otherwise a name that matches several perpetuals once case is
    /// ignored matches none of them, rather than one picked arbitrarily. Hyperliquid lists no
    /// such pair today (checked against mainnet, October 2026).
    ///
    /// May return a delisted perpetual: check [`PerpCoin::is_delisted`].
    pub fn get(&self, name: &str) -> Option<&PerpCoin> {
        self.0
            .get(name)
            .or_else(|| unique(self.0.values(), |perp| perp.coin.eq_ignore_ascii_case(name)))
    }

    /// The number of perpetuals listed, delisted ones included.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no perpetual is listed.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every perpetual listed, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &PerpCoin> {
        self.0.values()
    }
}

impl FromIterator<PerpCoin> for Perps {
    fn from_iter<I: IntoIterator<Item = PerpCoin>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|perp| (perp.coin.clone(), perp))
                .collect(),
        )
    }
}

impl FromIterator<Perps> for Perps {
    fn from_iter<I: IntoIterator<Item = Perps>>(iter: I) -> Self {
        Self(iter.into_iter().flat_map(|perps| perps.0).collect())
    }
}

/// One Hyperliquid perpetual.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PerpCoin {
    coin: SmolStr,
    delisted: bool,
}

impl PerpCoin {
    /// The coin Hyperliquid names the perpetual by, in its spelling (`BTC`, `kPEPE`, `xyz:TSLA`).
    /// A subscription or order must use exactly this.
    pub fn coin(&self) -> &str {
        &self.coin
    }

    /// The builder deployer of a HIP-3 perpetual (`xyz` for `xyz:TSLA`), or `None` for a
    /// default perpetual.
    pub fn deployer(&self) -> Option<&str> {
        self.coin.split_once(':').map(|(deployer, _)| deployer)
    }

    /// Whether Hyperliquid has delisted the perpetual. A delisted perpetual does not trade, so
    /// a subscription to it receives nothing.
    pub fn is_delisted(&self) -> bool {
        self.delisted
    }
}

/// Hyperliquid's spot pairs, by coin, as its `spotMeta` info response lists them.
///
/// Deserializes from that response, `{"universe": [...], "tokens": [...]}`. A pair whose tokens
/// the response does not list is left out, so looking up its coin returns `None`.
///
/// Hyperliquid's token names are unique, and so is each pair's base and quote, so a pair's tokens
/// name it as well as its coin does (checked against mainnet, October 2026). Hyperliquid's SDK
/// relies on the same when it addresses an order by `BASE/QUOTE`.
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

    /// The pair of `base` quoted in `quote`, named by their tokens as Hyperliquid spells them
    /// (`HYPE`, `USDT0`) or differing from that only in ASCII case (`hype`, `usdt0`).
    ///
    /// An exact match wins. Otherwise names that match several pairs once case is ignored match
    /// none of them, rather than one picked arbitrarily. Hyperliquid lists no such pair today
    /// (checked against mainnet, October 2026).
    pub fn find(&self, base: &str, quote: &str) -> Option<&SpotPair> {
        unique(self.0.values(), |pair| {
            pair.base == base && pair.quote == quote
        })
        .or_else(|| {
            unique(self.0.values(), |pair| {
                pair.base.eq_ignore_ascii_case(base) && pair.quote.eq_ignore_ascii_case(quote)
            })
        })
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

/// The one item of `items` that `matches`, or `None` if none or several do.
fn unique<'a, T>(
    items: impl IntoIterator<Item = &'a T>,
    matches: impl Fn(&T) -> bool,
) -> Option<&'a T> {
    let mut found = items.into_iter().filter(|item| matches(item));
    let first = found.next()?;
    found.next().is_none().then_some(first)
}

/// A `meta` info response, as far as [`Perps`] reads it.
#[derive(Deserialize)]
struct PerpMeta {
    universe: Vec<PerpMetaAsset>,
}

#[derive(Deserialize)]
struct PerpMetaAsset {
    name: SmolStr,
    #[serde(default, rename = "isDelisted")]
    delisted: bool,
}

impl From<PerpMeta> for Perps {
    fn from(meta: PerpMeta) -> Self {
        meta.universe
            .into_iter()
            .map(|asset| PerpCoin {
                coin: asset.name,
                delisted: asset.delisted,
            })
            .collect()
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
    fn spot_pairs_find_a_pair_by_its_tokens_ignoring_case() {
        let pairs = pairs();

        assert_eq!(pairs.find("HYPE", "USDT0").unwrap().coin(), "@207");
        assert_eq!(pairs.find("hype", "usdc").unwrap().coin(), "@107");
        assert_eq!(pairs.find("Purr", "Usdc").unwrap().coin(), "PURR/USDC");
        assert_eq!(
            pairs.find("USDC", "HYPE"),
            None,
            "base and quote are not interchangeable"
        );
        assert_eq!(pairs.find("HYPE", "USDH"), None);
    }

    #[test]
    fn spot_pairs_find_no_pair_when_ignoring_case_matches_several() {
        let pairs: SpotPairs = serde_json::from_str(
            r#"{
                "universe": [
                    {"name": "@1", "tokens": [1, 0], "index": 1},
                    {"name": "@2", "tokens": [2, 0], "index": 2}
                ],
                "tokens": [
                    {"name": "USDC", "index": 0},
                    {"name": "ABC", "index": 1},
                    {"name": "abc", "index": 2}
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(pairs.find("ABC", "USDC").unwrap().coin(), "@1");
        assert_eq!(pairs.find("abc", "USDC").unwrap().coin(), "@2");
        assert_eq!(pairs.find("Abc", "USDC"), None);
    }

    /// A default `meta` response in Hyperliquid's shape, with synthetic values: a mixed-case
    /// coin and a delisted one.
    const META: &str = r#"{
        "universe": [
            {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
            {"szDecimals": 0, "name": "kPEPE", "maxLeverage": 10, "marginTableId": 10},
            {"szDecimals": 1, "name": "MATIC", "maxLeverage": 20, "marginTableId": 20,
             "isDelisted": true}
        ],
        "marginTables": [],
        "collateralToken": 0
    }"#;

    /// A builder deployer's `meta` response, read with `"dex": "xyz"`, with synthetic values.
    const META_XYZ: &str = r#"{
        "universe": [
            {"szDecimals": 3, "name": "xyz:TSLA", "maxLeverage": 20, "marginTableId": 20,
             "growthMode": "enabled", "deployerFeeScale": "1.0"}
        ],
        "marginTables": [],
        "collateralToken": 0
    }"#;

    fn perps() -> Perps {
        [META, META_XYZ]
            .into_iter()
            .map(|meta| serde_json::from_str::<Perps>(meta).unwrap())
            .collect()
    }

    #[test]
    fn perps_collect_the_default_and_a_deployers_perpetuals() {
        let perps = perps();

        assert_eq!(perps.len(), 4);
        assert_eq!(perps.iter().count(), 4);
        let tsla = perps.get("xyz:TSLA").unwrap();
        assert_eq!((tsla.coin(), tsla.deployer()), ("xyz:TSLA", Some("xyz")));
        let btc = perps.get("BTC").unwrap();
        assert_eq!((btc.deployer(), btc.is_delisted()), (None, false));
        assert!(perps.get("MATIC").unwrap().is_delisted());
        assert!(Perps::default().is_empty());
    }

    #[test]
    fn perps_get_returns_the_venue_spelling_whatever_the_case_asked() {
        let perps = perps();

        for name in ["kPEPE", "KPEPE", "kpepe"] {
            assert_eq!(perps.get(name).unwrap().coin(), "kPEPE", "{name:?}");
        }
        assert_eq!(perps.get("XYZ:tsla").unwrap().coin(), "xyz:TSLA");
        assert_eq!(
            perps.get("TSLA"),
            None,
            "a HIP-3 perpetual is named with its deployer"
        );
        assert_eq!(perps.get("ETH"), None);
    }

    #[test]
    fn perps_get_prefers_an_exact_match_and_refuses_an_ambiguous_one() {
        let perps: Perps =
            serde_json::from_str(r#"{"universe": [{"name": "ABC"}, {"name": "abc"}]}"#).unwrap();

        assert_eq!(perps.get("ABC").unwrap().coin(), "ABC");
        assert_eq!(perps.get("abc").unwrap().coin(), "abc");
        assert_eq!(perps.get("Abc"), None);
    }

    #[test]
    fn network_serializes_in_lower_case() {
        assert_eq!(
            serde_json::to_string(&Network::Mainnet).unwrap(),
            r#""mainnet""#
        );
        assert_eq!(
            serde_json::from_str::<Network>(r#""testnet""#).unwrap(),
            Network::Testnet
        );
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
