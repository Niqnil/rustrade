use super::Hyperliquid;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt};

/// Hyperliquid market identifier.
///
/// For perpetuals, this is just the base asset in uppercase (e.g., "BTC", "ETH").
/// Hyperliquid perps are quoted in USDC, so the quote asset is implicit.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HyperliquidMarket(pub SmolStr);

impl<Kind> Identifier<HyperliquidMarket> for Subscription<Hyperliquid, MarketDataInstrument, Kind> {
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_market(&self.instrument.base)
    }
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<Hyperliquid, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_market(&self.instrument.value.base)
    }
}

fn hyperliquid_market(base: &AssetNameInternal) -> HyperliquidMarket {
    HyperliquidMarket(base.name().to_uppercase_smolstr())
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<Hyperliquid, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        HyperliquidMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for HyperliquidMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
