use crate::{
    Keyed,
    asset::name::{AssetNameExchange, AssetNameInternal},
    exchange::ExchangeId,
};
use derive_more::{Constructor, Display};
use serde::{Deserialize, Serialize};

/// Defines the [`AssetNameInternal`] and [`AssetNameExchange`] types, used as `SmolStr`
/// identifiers for an [`Asset`].
pub mod name;

/// Unique identifier for an [`Asset`].
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display,
)]
pub struct AssetId(pub u64);

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct AssetIndex(pub usize);

impl AssetIndex {
    pub fn index(&self) -> usize {
        self.0
    }
}

impl std::fmt::Display for AssetIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AssetIndex({})", self.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct ExchangeAsset<Asset> {
    pub exchange: ExchangeId,
    pub asset: Asset,
}

impl<Asset> ExchangeAsset<Asset> {
    pub fn new<A>(exchange: ExchangeId, asset: A) -> Self
    where
        A: Into<Asset>,
    {
        Self {
            exchange,
            asset: asset.into(),
        }
    }
}

impl<Ass, Asset, T> From<(ExchangeId, Ass, T)> for Keyed<ExchangeAsset<Asset>, T>
where
    Ass: Into<Asset>,
{
    fn from((exchange, asset, value): (ExchangeId, Ass, T)) -> Self {
        Self {
            key: ExchangeAsset::new(exchange, asset),
            value,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct Asset {
    pub name_internal: AssetNameInternal,
    pub name_exchange: AssetNameExchange,
}

impl<S> From<S> for Asset
where
    S: Into<AssetNameExchange>,
{
    fn from(value: S) -> Self {
        Self::new_from_exchange(value)
    }
}

impl Asset {
    pub fn new<Internal, Exchange>(name_internal: Internal, name_exchange: Exchange) -> Self
    where
        Internal: Into<AssetNameInternal>,
        Exchange: Into<AssetNameExchange>,
    {
        Self {
            name_internal: name_internal.into(),
            name_exchange: name_exchange.into(),
        }
    }

    pub fn new_from_exchange<S>(name_exchange: S) -> Self
    where
        S: Into<AssetNameExchange>,
    {
        let name_exchange = name_exchange.into();
        Self {
            name_internal: AssetNameInternal::from(name_exchange.name().clone()),
            name_exchange,
        }
    }
}

/// Whether two asset keys name the same asset on one exchange.
///
/// A venue reports an asset by its own spelling, which need not match the spelling an instrument
/// was built with (`BTC` and `btc`). For a key that carries a venue spelling, the same asset is the
/// same name ignoring ASCII case: the rule
/// [`IndexedInstruments`](crate::index::IndexedInstruments) enforces uniqueness under (it rejects
/// two assets on one exchange whose `name_exchange` differs only in ASCII case), and the rule an
/// indexed execution client resolves a venue's name to an [`AssetIndex`] by. Comparing unindexed
/// keys with this agrees with comparing the indices they resolve to.
///
/// Use it, not `==`, wherever generic code asks whether two keys are one asset, such as a fee
/// asset against an instrument's base. `PartialEq` on the key types stays exact, as map keys and
/// the spelling sent to a venue need.
///
/// # Contract
/// - Only keys of one exchange are comparable. Across exchanges, one name can mean two assets.
/// - Folding is ASCII only: `é` and `É` are different assets, as they are to the index.
/// - An implementation must be an equivalence relation, and must agree with `==` wherever `==`
///   holds.
pub trait SameAsset {
    /// Whether `self` and `other` name the same asset on one exchange.
    fn is_same_asset(&self, other: &Self) -> bool;
}

/// An index names one asset: the same asset is the same index.
impl SameAsset for AssetIndex {
    fn is_same_asset(&self, other: &Self) -> bool {
        self == other
    }
}

/// The venue's spelling, compared ignoring ASCII case.
impl SameAsset for AssetNameExchange {
    fn is_same_asset(&self, other: &Self) -> bool {
        self.name().eq_ignore_ascii_case(other.name())
    }
}

/// Already lowercase, so `==` is the same asset.
impl SameAsset for AssetNameInternal {
    fn is_same_asset(&self, other: &Self) -> bool {
        self == other
    }
}

/// The [`name_exchange`](Asset::name_exchange)s, compared as [`AssetNameExchange`]s are. The
/// [`name_internal`](Asset::name_internal) is not consulted: a venue reports an asset, as a fee or
/// a balance, by its exchange name, and that is what an indexed client resolves it by.
impl SameAsset for Asset {
    fn is_same_asset(&self, other: &Self) -> bool {
        self.name_exchange.is_same_asset(&other.name_exchange)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum AssetKind {
    Crypto,
    Fiat,
}

impl From<Asset> for AssetNameInternal {
    fn from(value: Asset) -> Self {
        value.name_internal
    }
}

/// Special type that represents a "base" [`Asset`].
///
/// Examples: <br>
/// a) Instrument = btc_usdt_spot, [`BaseAsset`] => btc <br>
/// b) Instrument = eth_btc_spot, [`BaseAsset`] => eth
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display)]
pub struct BaseAsset;

/// Special type that represents a "quote" [`Asset`].
///
/// Examples: <br>
/// a) Instrument = btc_usdt_spot, [`QuoteAsset`] => usdt <br>
/// b) Instrument = eth_btc_spot, [`QuoteAsset`] => btc
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display)]
pub struct QuoteAsset;

/// A marker names one asset, so every value is the same asset.
impl SameAsset for BaseAsset {
    fn is_same_asset(&self, _: &Self) -> bool {
        true
    }
}

/// A marker names one asset, so every value is the same asset.
impl SameAsset for QuoteAsset {
    fn is_same_asset(&self, _: &Self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange(name: &str) -> AssetNameExchange {
        AssetNameExchange::new(name)
    }

    #[test]
    fn an_exchange_name_is_the_same_asset_in_any_ascii_case() {
        assert!(exchange("BTC").is_same_asset(&exchange("BTC")));
        assert!(exchange("BTC").is_same_asset(&exchange("btc")));
        assert!(exchange("kPEPE").is_same_asset(&exchange("KPEPE")));
        assert!(!exchange("BTC").is_same_asset(&exchange("BNB")));
        assert!(!exchange("BTC").is_same_asset(&exchange("BTCB")));
    }

    /// Only ASCII is folded, as the index folds it: `É` and `é` are two assets.
    #[test]
    fn an_exchange_name_folds_only_ascii() {
        assert!(!exchange("CAFÉ").is_same_asset(&exchange("café")));
        assert!(exchange("CAFé").is_same_asset(&exchange("café")));
    }

    /// The exchange name decides: an internal name that differs does not make two assets, and one
    /// that matches does not make one.
    #[test]
    fn an_asset_is_the_same_asset_by_its_exchange_name() {
        assert!(Asset::new("btc", "XBT").is_same_asset(&Asset::new("bitcoin", "xbt")));
        assert!(!Asset::new("btc", "XBT").is_same_asset(&Asset::new("btc", "BTC")));
    }

    /// A marker names one asset, as a position keyed by it relies on to find its own carry.
    #[test]
    fn a_marker_is_always_the_same_asset() {
        assert!(BaseAsset.is_same_asset(&BaseAsset));
        assert!(QuoteAsset.is_same_asset(&QuoteAsset));
    }

    #[test]
    fn an_index_and_an_internal_name_are_the_same_asset_when_equal() {
        assert!(AssetIndex(3).is_same_asset(&AssetIndex(3)));
        assert!(!AssetIndex(3).is_same_asset(&AssetIndex(4)));
        // Lowercased on construction, so a spelling in another case is equal.
        assert!(AssetNameInternal::new("BTC").is_same_asset(&AssetNameInternal::new("btc")));
        assert!(!AssetNameInternal::new("btc").is_same_asset(&AssetNameInternal::new("eth")));
    }
}
