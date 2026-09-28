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

/// An instrument representation an Alpaca subscription can spell as a feed symbol.
///
/// Implemented for every instrument type this crate subscribes with: [`MarketDataInstrument`],
/// [`Keyed`] over it, and [`MarketInstrumentData`]. Every [`Subscription`] to an [`Alpaca`] feed over
/// an implementor identifies its [`AlpacaMarket`] through it, so one blanket `Identifier` impl covers
/// every feed and every representation.
///
/// It is also what lets [`DynamicStreams`](crate::streams::builder::dynamic::DynamicStreams) route
/// to this integration: a bound on the instrument type alone implies the identifier for every
/// feed, where the alternative is one bound per feed and kind.
pub trait AlpacaInstrument {
    /// The symbol this instrument subscribes to on a feed spelling symbols as `shape`.
    fn alpaca_market(&self, shape: AlpacaSymbolShape) -> AlpacaMarket;
}

impl<Server, Instrument, Kind> Identifier<AlpacaMarket>
    for Subscription<Alpaca<Server>, Instrument, Kind>
where
    Server: AlpacaServer,
    Instrument: AlpacaInstrument,
{
    fn id(&self) -> AlpacaMarket {
        self.instrument.alpaca_market(Server::SYMBOL_SHAPE)
    }
}

impl AlpacaInstrument for MarketDataInstrument {
    fn alpaca_market(&self, shape: AlpacaSymbolShape) -> AlpacaMarket {
        shape.market(self)
    }
}

impl<InstrumentKey> AlpacaInstrument for Keyed<InstrumentKey, MarketDataInstrument> {
    fn alpaca_market(&self, shape: AlpacaSymbolShape) -> AlpacaMarket {
        self.value.alpaca_market(shape)
    }
}

/// Takes the exchange-side name as given, so it needs no per-feed shape: the caller has already
/// supplied Alpaca's own symbol, and there is nothing to reconstruct.
///
/// Case is still normalised, as the reconstruction path normalises it. Alpaca spells its symbols in
/// uppercase, and a subscribe is confirmed only once Alpaca's answer names each requested symbol
/// exactly — so a lowercase `name_exchange` would otherwise fail the subscribe with a timeout naming
/// it. On a correctly spelled exchange name the call is a no-op.
impl<InstrumentKey> AlpacaInstrument for MarketInstrumentData<InstrumentKey> {
    fn alpaca_market(&self, _shape: AlpacaSymbolShape) -> AlpacaMarket {
        AlpacaMarket(self.name_exchange.name().to_uppercase_smolstr())
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

    #[test]
    fn an_exchange_named_instrument_is_taken_verbatim() {
        assert_eq!(
            market_of_exchange_named::<AlpacaServerCrypto>("BTC/USD").as_ref(),
            "BTC/USD"
        );
        assert_eq!(
            market_of_exchange_named::<AlpacaServerIex>("AAPL").as_ref(),
            "AAPL"
        );
    }

    /// Alpaca confirms a subscribe only once its answer names each requested symbol exactly, in
    /// its own uppercase spelling, so a lowercase name left alone would time out unconfirmed.
    #[test]
    fn an_exchange_named_instrument_is_uppercased_like_a_reconstructed_one() {
        assert_eq!(
            market_of_exchange_named::<AlpacaServerCrypto>("btc/usd").as_ref(),
            "BTC/USD"
        );
        assert_eq!(
            market_of_exchange_named::<AlpacaServerSip>("brk.b").as_ref(),
            "BRK.B"
        );
    }

    fn market_of_exchange_named<Server: AlpacaServer>(name_exchange: &str) -> AlpacaMarket {
        use crate::subscription::trade::PublicTrades;
        use rustrade_instrument::instrument::{
            market_data::kind::MarketDataInstrumentKind, name::InstrumentNameExchange,
        };

        let subscription = Subscription {
            exchange: Alpaca::<Server>::default(),
            instrument: MarketInstrumentData {
                key: 0_usize,
                name_exchange: InstrumentNameExchange::new(name_exchange),
                kind: MarketDataInstrumentKind::Spot,
            },
            kind: PublicTrades,
        };

        subscription.id()
    }
}
