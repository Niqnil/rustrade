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

/// How an Alpaca feed spells the symbol it subscribes on.
///
/// Each feed uses exactly one spelling, so the shape is known statically from the connector type.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AlpacaSymbolShape {
    /// A bare uppercase ticker — the equities feeds: `AAPL`, `SPY`.
    Ticker,
    /// `BASE/QUOTE`, uppercase — the crypto feed: `BTC/USD`.
    Pair,
}

impl AlpacaSymbolShape {
    fn market(self, instrument: &MarketDataInstrument) -> AlpacaMarket {
        match self {
            Self::Ticker => equities_market(&instrument.base),
            Self::Pair => crypto_market(&instrument.base, &instrument.quote),
        }
    }
}

/// An Alpaca market data WebSocket server, and the symbol spelling its feed uses.
///
/// Implemented by [`AlpacaServerIex`], [`AlpacaServerSip`] and [`AlpacaServerCrypto`]. It exists so
/// each instrument representation spells its symbol once rather than once per server, and so each
/// feed's spelling is declared in exactly one place.
pub trait AlpacaServer: crate::exchange::ExchangeServer {
    /// The spelling this feed's symbols take.
    const SYMBOL_SHAPE: AlpacaSymbolShape;
}

impl AlpacaServer for AlpacaServerIex {
    const SYMBOL_SHAPE: AlpacaSymbolShape = AlpacaSymbolShape::Ticker;
}

impl AlpacaServer for AlpacaServerSip {
    const SYMBOL_SHAPE: AlpacaSymbolShape = AlpacaSymbolShape::Ticker;
}

impl AlpacaServer for AlpacaServerCrypto {
    const SYMBOL_SHAPE: AlpacaSymbolShape = AlpacaSymbolShape::Pair;
}

impl<Server, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<Server>, MarketDataInstrument, Kind>
where
    Server: AlpacaServer,
{
    fn id(&self) -> AlpacaMarket {
        Server::SYMBOL_SHAPE.market(&self.instrument)
    }
}

impl<Server, InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<Server>, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
where
    Server: AlpacaServer,
{
    fn id(&self) -> AlpacaMarket {
        Server::SYMBOL_SHAPE.market(&self.instrument.value)
    }
}

/// The exchange's own spelling, carried by the instrument, whatever the feed.
impl<Server, InstrumentKey, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<Server>, MarketInstrumentData<InstrumentKey>, Kind>
where
    Server: AlpacaServer,
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

    #[test]
    fn each_feed_spells_its_symbols_with_its_own_shape() {
        use crate::subscription::trade::PublicTrades;
        use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

        fn market<Server: AlpacaServer>(base: &str, quote: &str) -> AlpacaMarket {
            let subscription: Subscription<Alpaca<Server>, MarketDataInstrument, PublicTrades> =
                Subscription::from((
                    Alpaca::<Server>::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    PublicTrades,
                ));
            subscription.id()
        }

        assert_eq!(market::<AlpacaServerIex>("aapl", "usd").as_ref(), "AAPL");
        assert_eq!(market::<AlpacaServerSip>("aapl", "usd").as_ref(), "AAPL");
        assert_eq!(
            market::<AlpacaServerCrypto>("btc", "usd").as_ref(),
            "BTC/USD"
        );
    }
}
