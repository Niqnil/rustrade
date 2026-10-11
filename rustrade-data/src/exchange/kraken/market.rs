use super::Kraken;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use rustrade_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] market that can be subscribed to.
///
/// Kraken's WebSocket v2 symbol: `BASE/QUOTE` in upper case, eg/ `BTC/USD`. A
/// [`MarketInstrumentData`] subscription uses its `name_exchange` verbatim, so it must be spelled
/// the same way; v1 spellings such as `XBT/USD` are refused.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenMarket(pub SmolStr);

impl<Kind> Identifier<KrakenMarket> for Subscription<Kraken, MarketDataInstrument, Kind> {
    fn id(&self) -> KrakenMarket {
        kraken_market(&self.instrument.base, &self.instrument.quote)
    }
}

impl<InstrumentKey, Kind> Identifier<KrakenMarket>
    for Subscription<Kraken, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> KrakenMarket {
        kraken_market(&self.instrument.value.base, &self.instrument.value.quote)
    }
}

impl<InstrumentKey, Kind> Identifier<KrakenMarket>
    for Subscription<Kraken, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> KrakenMarket {
        KrakenMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for KrakenMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn kraken_market(base: &AssetNameInternal, quote: &AssetNameInternal) -> KrakenMarket {
    KrakenMarket(format_smolstr!("{base}/{quote}").to_uppercase_smolstr())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::trade::PublicTrades;
    use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

    #[test]
    fn a_base_and_quote_subscription_is_spelled_as_a_v2_symbol() {
        let subscription: Subscription<Kraken, MarketDataInstrument, PublicTrades> =
            Subscription::new(
                Kraken,
                ("btc", "usd", MarketDataInstrumentKind::Spot),
                PublicTrades,
            );

        let market: KrakenMarket = subscription.id();
        assert_eq!(market, KrakenMarket("BTC/USD".into()));
    }
}
