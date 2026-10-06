//! The coin a Hyperliquid subscription names its market by.
//!
//! # Which instrument to subscribe with
//!
//! A [`MarketInstrumentData`] is subscribed by its `name_exchange` verbatim, so it works for every
//! market. Build it with [`MarketInstrumentData::hyperliquid_perp`] or
//! [`MarketInstrumentData::hyperliquid_spot`] from a
//! [`HyperliquidMeta`](super::HyperliquidMeta) lookup.
//!
//! A [`MarketDataInstrument`] (or one [`Keyed`] over it) derives the coin from its asset names,
//! which are stored lower-cased, so the venue's case is lost before the coin is built:
//! - A perpetual's coin is the base asset upper-cased. That is right for most perpetuals (`BTC`),
//!   but wrong for a mixed-case one (`kPEPE` becomes `KPEPE`) and for a builder-deployed (HIP-3)
//!   one, whose deployer is lower-case (`xyz:TSLA` becomes `XYZ:TSLA`).
//! - A spot pair's coin is `BASE/QUOTE` upper-cased, which names only PURR/USDC. Every other pair
//!   is named `@{index}`; pass that as the base asset (`@107`) to subscribe to it.
//!
//! A wrong coin gets no data, and Hyperliquid then closes the connection, ending the other
//! subscriptions on it too.

use super::{Hyperliquid, HyperliquidSpot};
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use rustrade_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// The coin a Hyperliquid subscription names its market by (`BTC`, `kPEPE`, `xyz:TSLA`, `@107`,
/// `PURR/USDC`).
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HyperliquidMarket(pub SmolStr);

/// An instrument representation a Hyperliquid perpetual subscription can name as a market.
///
/// Implemented for every instrument type this crate subscribes with: [`MarketDataInstrument`],
/// [`Keyed`] over it, and [`MarketInstrumentData`]. One blanket `Identifier` impl covers them all,
/// which is also what lets
/// [`DynamicStreams`](crate::streams::builder::dynamic::DynamicStreams) route to this integration
/// through a single bound on the instrument type.
pub trait HyperliquidInstrument {
    /// The perpetual market this instrument subscribes to.
    fn hyperliquid_market(&self) -> HyperliquidMarket;
}

impl<Instrument, Kind> Identifier<HyperliquidMarket> for Subscription<Hyperliquid, Instrument, Kind>
where
    Instrument: HyperliquidInstrument,
{
    fn id(&self) -> HyperliquidMarket {
        self.instrument.hyperliquid_market()
    }
}

impl HyperliquidInstrument for MarketDataInstrument {
    fn hyperliquid_market(&self) -> HyperliquidMarket {
        hyperliquid_market(&self.base)
    }
}

impl<InstrumentKey> HyperliquidInstrument for Keyed<InstrumentKey, MarketDataInstrument> {
    fn hyperliquid_market(&self) -> HyperliquidMarket {
        self.value.hyperliquid_market()
    }
}

/// The perpetual coin a [`MarketDataInstrument`] names: its base asset, upper-cased. See the
/// [module docs](self) for the coins this gets wrong.
fn hyperliquid_market(base: &AssetNameInternal) -> HyperliquidMarket {
    HyperliquidMarket(base.name().to_uppercase_smolstr())
}

impl<InstrumentKey> HyperliquidInstrument for MarketInstrumentData<InstrumentKey> {
    fn hyperliquid_market(&self) -> HyperliquidMarket {
        HyperliquidMarket(self.name_exchange.name().clone())
    }
}

impl AsRef<str> for HyperliquidMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// HyperliquidSpot implementations

impl<Kind> Identifier<HyperliquidMarket>
    for Subscription<HyperliquidSpot, MarketDataInstrument, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_spot_market(&self.instrument.base, &self.instrument.quote)
    }
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<HyperliquidSpot, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_spot_market(&self.instrument.value.base, &self.instrument.value.quote)
    }
}

/// The spot coin a [`MarketDataInstrument`] names: its base asset if that is an `@{index}`
/// coin, otherwise `BASE/QUOTE` upper-cased, which names only PURR/USDC. See the
/// [module docs](self).
fn hyperliquid_spot_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
) -> HyperliquidMarket {
    let base_name = base.name();
    if base_name.starts_with('@') {
        HyperliquidMarket(SmolStr::new(base_name))
    } else {
        HyperliquidMarket(format_smolstr!(
            "{}/{}",
            base_name.to_uppercase_smolstr(),
            quote.name().to_uppercase_smolstr()
        ))
    }
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<HyperliquidSpot, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        HyperliquidMarket(self.instrument.name_exchange.name().clone())
    }
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::subscription::trade::PublicTrades;
    use rustrade_instrument::{
        hyperliquid::{Perps, SpotPairs},
        instrument::market_data::kind::MarketDataInstrumentKind,
    };

    fn market<Exchange, Instrument>(exchange: Exchange, instrument: Instrument) -> String
    where
        Subscription<Exchange, Instrument, PublicTrades>: Identifier<HyperliquidMarket>,
    {
        Subscription::new(exchange, instrument, PublicTrades)
            .id()
            .0
            .to_string()
    }

    #[test]
    fn a_market_instrument_data_subscribes_by_the_venue_spelling() {
        let perps: Perps =
            serde_json::from_str(r#"{"universe": [{"name": "kPEPE"}, {"name": "xyz:TSLA"}]}"#)
                .unwrap();
        let spot_pairs: SpotPairs = serde_json::from_str(
            r#"{
                "universe": [{"name": "@107", "tokens": [150, 0], "index": 107}],
                "tokens": [{"name": "USDC", "index": 0}, {"name": "HYPE", "index": 150}]
            }"#,
        )
        .unwrap();

        for (name, coin) in [("KPEPE", "kPEPE"), ("XYZ:TSLA", "xyz:TSLA")] {
            let perp = MarketInstrumentData::hyperliquid_perp(0, perps.get(name).unwrap());
            assert_eq!(perp.kind, MarketDataInstrumentKind::Perpetual);
            assert_eq!(market(Hyperliquid, perp), coin);
        }
        let hype =
            MarketInstrumentData::hyperliquid_spot(0, spot_pairs.find("hype", "usdc").unwrap());
        assert_eq!(hype.kind, MarketDataInstrumentKind::Spot);
        assert_eq!(market(HyperliquidSpot, hype), "@107");
    }

    #[test]
    fn a_market_data_instrument_loses_the_venue_case() {
        // Documented limitation: asset names are stored lower-cased.
        let perp =
            |base| MarketDataInstrument::new(base, "usdc", MarketDataInstrumentKind::Perpetual);
        assert_eq!(market(Hyperliquid, perp("btc")), "BTC");
        assert_eq!(market(Hyperliquid, perp("kPEPE")), "KPEPE");

        let spot = |base| MarketDataInstrument::new(base, "usdc", MarketDataInstrumentKind::Spot);
        assert_eq!(market(HyperliquidSpot, spot("purr")), "PURR/USDC");
        assert_eq!(market(HyperliquidSpot, spot("@107")), "@107");
    }
}
