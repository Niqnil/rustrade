use super::{Alpaca, AlpacaServerCrypto, AlpacaServerIex, AlpacaServerSip};
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use rustrade_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Alpaca market identifier.
///
/// - Equities (IEX/SIP): uppercase symbol, e.g., `"AAPL"`, `"SPY"`
/// - Crypto: `"BASE/USD"` format, e.g., `"BTC/USD"`, `"ETH/USD"`
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct AlpacaMarket(pub SmolStr);

impl AsRef<str> for AlpacaMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn equities_market(base: &AssetNameInternal) -> AlpacaMarket {
    AlpacaMarket(base.name().to_uppercase_smolstr())
}

fn crypto_market(base: &AssetNameInternal, quote: &AssetNameInternal) -> AlpacaMarket {
    AlpacaMarket(format_smolstr!(
        "{}/{}",
        base.name().to_uppercase_smolstr(),
        quote.name().to_uppercase_smolstr()
    ))
}

impl<Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerIex>, MarketDataInstrument, Kind>
{
    fn id(&self) -> AlpacaMarket {
        equities_market(&self.instrument.base)
    }
}

impl<Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerSip>, MarketDataInstrument, Kind>
{
    fn id(&self) -> AlpacaMarket {
        equities_market(&self.instrument.base)
    }
}

impl<Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerCrypto>, MarketDataInstrument, Kind>
{
    fn id(&self) -> AlpacaMarket {
        crypto_market(&self.instrument.base, &self.instrument.quote)
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerIex>, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        equities_market(&self.instrument.value.base)
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerSip>, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        equities_market(&self.instrument.value.base)
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerCrypto>, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        crypto_market(&self.instrument.value.base, &self.instrument.value.quote)
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerIex>, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        AlpacaMarket(self.instrument.name_exchange.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerSip>, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        AlpacaMarket(self.instrument.name_exchange.name().clone())
    }
}

impl<InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<AlpacaServerCrypto>, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> AlpacaMarket {
        AlpacaMarket(self.instrument.name_exchange.name().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustrade_instrument::asset::name::AssetNameInternal;

    #[test]
    fn test_equities_market_format() {
        let base = AssetNameInternal::new("aapl");
        let market = equities_market(&base);
        assert_eq!(market.as_ref(), "AAPL");
    }

    #[test]
    fn test_crypto_market_format() {
        let base = AssetNameInternal::new("btc");
        let quote = AssetNameInternal::new("usd");
        let market = crypto_market(&base, &quote);
        assert_eq!(market.as_ref(), "BTC/USD");
    }
}
