use super::{Hyperliquid, HyperliquidSpot};
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use rustrade_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Hyperliquid market identifier.
///
/// For perpetuals, this is just the base asset in uppercase (e.g., "BTC", "ETH").
/// For spot, this is the pair format (e.g., "PURR/USDC", "HYPE/USDC").
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

fn hyperliquid_spot_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
) -> HyperliquidMarket {
    let base_name = base.name();
    // Hyperliquid spot WebSocket uses "@index" format for all spot markets
    // (e.g., "@0" for PURR, "@107" for HYPE). Use SpotMetaResolver to map
    // token names to indices. If base already starts with "@", use it directly.
    if base_name.starts_with('@') {
        HyperliquidMarket(SmolStr::new(base_name))
    } else {
        // WARNING: This fallback produces "BASE/QUOTE" literal strings, which only
        // work for PURR/USDC (the sole exception that uses literal pair name).
        // All other spot pairs must be resolved via SpotMetaResolver to "@{index}"
        // format before calling — otherwise the subscription will silently receive
        // no data.
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
