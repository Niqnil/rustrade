use rustrade_instrument::{
    Keyed,
    instrument::{
        Instrument,
        market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
        name::InstrumentNameExchange,
    },
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Instrument related data that defines an associated unique `Id`.
///
/// Verbose `InstrumentData` is often used to subscribe to market data feeds, but it's unique `Id`
/// can then be used to key consumed [MarketEvents](crate::event::MarketEvent), significantly reducing
/// duplication in the case of complex instruments (eg/ options).
pub trait InstrumentData
where
    Self: Clone + Debug + Send + Sync,
{
    type Key: Debug + Clone + Eq + Send + Sync;
    fn key(&self) -> &Self::Key;
    fn kind(&self) -> &MarketDataInstrumentKind;
}

impl<InstrumentKey> InstrumentData for Keyed<InstrumentKey, MarketDataInstrument>
where
    InstrumentKey: Debug + Clone + Eq + Send + Sync,
{
    type Key = InstrumentKey;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn kind(&self) -> &MarketDataInstrumentKind {
        &self.value.kind
    }
}

impl InstrumentData for MarketDataInstrument {
    type Key = Self;

    fn key(&self) -> &Self::Key {
        self
    }

    fn kind(&self) -> &MarketDataInstrumentKind {
        &self.kind
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct MarketInstrumentData<InstrumentKey> {
    pub key: InstrumentKey,
    pub name_exchange: InstrumentNameExchange,
    pub kind: MarketDataInstrumentKind,
}

#[cfg(feature = "hyperliquid")]
impl<InstrumentKey> MarketInstrumentData<InstrumentKey> {
    /// A Hyperliquid perpetual, keyed by `key`, to subscribe to with
    /// [`Hyperliquid`](crate::exchange::hyperliquid::Hyperliquid).
    ///
    /// Find `perp` with
    /// [`HyperliquidMeta::perp_coin`](crate::exchange::hyperliquid::HyperliquidMeta::perp_coin),
    /// so the subscription names its coin as Hyperliquid spells it.
    pub fn hyperliquid_perp(
        key: InstrumentKey,
        perp: &rustrade_instrument::hyperliquid::PerpCoin,
    ) -> Self {
        Self {
            key,
            name_exchange: InstrumentNameExchange::from(perp.coin()),
            kind: MarketDataInstrumentKind::Perpetual,
        }
    }

    /// A Hyperliquid spot pair, keyed by `key`, to subscribe to with
    /// [`HyperliquidSpot`](crate::exchange::hyperliquid::HyperliquidSpot).
    ///
    /// Find `pair` with
    /// [`HyperliquidMeta::spot_pair`](crate::exchange::hyperliquid::HyperliquidMeta::spot_pair),
    /// so the subscription names its coin (`@107`) rather than its tokens.
    pub fn hyperliquid_spot(
        key: InstrumentKey,
        pair: &rustrade_instrument::hyperliquid::SpotPair,
    ) -> Self {
        Self {
            key,
            name_exchange: InstrumentNameExchange::from(pair.coin()),
            kind: MarketDataInstrumentKind::Spot,
        }
    }
}

impl<InstrumentKey> InstrumentData for MarketInstrumentData<InstrumentKey>
where
    InstrumentKey: Debug + Clone + Eq + Send + Sync,
{
    type Key = InstrumentKey;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn kind(&self) -> &MarketDataInstrumentKind {
        &self.kind
    }
}

impl<InstrumentKey> std::fmt::Display for MarketInstrumentData<InstrumentKey>
where
    InstrumentKey: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.key,
            self.name_exchange.as_ref(),
            self.kind
        )
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey>
    From<&Keyed<InstrumentKey, Instrument<ExchangeKey, AssetKey>>>
    for MarketInstrumentData<InstrumentKey>
where
    InstrumentKey: Clone,
{
    fn from(value: &Keyed<InstrumentKey, Instrument<ExchangeKey, AssetKey>>) -> Self {
        Self {
            key: value.key.clone(),
            // The DATA venue's symbol, not the execution venue's. These differ whenever the two
            // venues spell one instrument differently (`BP.L` against `BP`), and subscribing under
            // the execution venue's name would silently yield no ticks -- or another instrument's.
            // Falls back to `name_exchange` for every single-venue instrument.
            name_exchange: value.value.data_name_exchange().clone(),
            kind: MarketDataInstrumentKind::from(&value.value.kind),
        }
    }
}
