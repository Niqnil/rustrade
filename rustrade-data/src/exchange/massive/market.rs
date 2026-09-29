//! How each Massive cluster spells a market on its WebSocket.

use super::Massive;
use crate::{
    Identifier,
    exchange::{ExchangeServer, osi},
    instrument::MarketInstrumentData,
    subscription::Subscription,
};
use rustrade_instrument::{
    Keyed,
    asset::name::AssetNameInternal,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// A market as a Massive WebSocket spells it: `AAPL`, `BTC-USD`, `EUR/USD` or
/// `O:AAPL251219C00150000`, depending on the cluster.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct MassiveMarket(pub SmolStr);

impl AsRef<str> for MassiveMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MassiveMarket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a cluster spells its markets.
///
/// Each spelling is the one the cluster's data messages carry, so a subscription and the messages
/// it delivers name the market identically. Forex is the case in point: the cluster also accepts a
/// subscription to `EUR-USD`, but delivers it as `EUR/USD`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MassiveSymbols {
    /// The base asset alone, upper-case: `AAPL`.
    Ticker,
    /// Base and quote joined by a hyphen, upper-case: `BTC-USD`.
    CryptoPair,
    /// Base and quote joined by a slash, upper-case: `EUR/USD`.
    CurrencyPair,
    /// `O:` and the contract's OSI symbol, unpadded: `O:AAPL251219C00150000`.
    OptionContract,
}

/// Where option contracts start.
pub(super) const OPTION_PREFIX: &str = "O:";

impl MassiveSymbols {
    fn market(
        self,
        base: &AssetNameInternal,
        quote: &AssetNameInternal,
        kind: &MarketDataInstrumentKind,
    ) -> MassiveMarket {
        let base_name = base.name().to_uppercase_smolstr();

        MassiveMarket(match self {
            Self::Ticker => base_name,
            Self::CryptoPair => {
                format_smolstr!("{base_name}-{}", quote.name().to_uppercase_smolstr())
            }
            Self::CurrencyPair => {
                format_smolstr!("{base_name}/{}", quote.name().to_uppercase_smolstr())
            }
            Self::OptionContract => {
                let spelled = match kind {
                    MarketDataInstrumentKind::Option(contract) => {
                        osi::symbol(base.name(), contract)
                    }
                    _ => None,
                };

                // `Identifier::id` is infallible, so an instrument with no OSI spelling cannot fail
                // here. It gets a symbol saying why instead, which is not an option contract, so
                // the subscriber rejects the batch naming it before anything is sent.
                match spelled {
                    Some(symbol) => format_smolstr!("{OPTION_PREFIX}{symbol}"),
                    None => format_smolstr!("{base_name} (no OSI symbol for {kind})"),
                }
            }
        })
    }
}

/// Whether `market` spells an option contract.
pub(super) fn is_option_contract(market: &str) -> bool {
    market
        .strip_prefix(OPTION_PREFIX)
        .and_then(osi::root)
        .is_some()
}

/// A Massive WebSocket cluster: its endpoint, how it spells markets, and the channels it publishes.
///
/// Implemented by the four shipped clusters. A cluster reached at another endpoint — Massive's
/// legacy `socket.polygon.io` host, say — is a type of your own implementing this and
/// [`ExchangeServer`] with the constants of the cluster it mirrors.
pub trait MassiveServer: ExchangeServer {
    /// How the cluster spells its markets.
    const SYMBOLS: MassiveSymbols;

    /// The quote channel's event: `Q`, `XQ` or `C`.
    const QUOTES: &'static str;

    /// The per-second aggregate channel's event: `A`, `XAS` or `CAS`.
    const SECOND_AGGREGATES: &'static str;

    /// The per-minute aggregate channel's event: `AM`, `XA` or `CA`.
    const MINUTE_AGGREGATES: &'static str;
}

/// A Massive cluster that publishes trades. Forex does not.
pub trait MassiveTradeServer: MassiveServer {
    /// The trade channel's event: `T` or `XT`.
    const TRADES: &'static str;
}

/// An instrument representation a Massive subscription can spell as a market.
///
/// Implemented for every instrument type this crate subscribes with: [`MarketDataInstrument`],
/// [`Keyed`] over it, and [`MarketInstrumentData`].
pub trait MassiveInstrument {
    /// The market this instrument subscribes to on a cluster spelling markets as `symbols`.
    fn massive_market(&self, symbols: MassiveSymbols) -> MassiveMarket;
}

impl<Server, Instrument, Kind> Identifier<MassiveMarket>
    for Subscription<Massive<Server>, Instrument, Kind>
where
    Server: MassiveServer,
    Instrument: MassiveInstrument,
{
    fn id(&self) -> MassiveMarket {
        self.instrument.massive_market(Server::SYMBOLS)
    }
}

impl MassiveInstrument for MarketDataInstrument {
    fn massive_market(&self, symbols: MassiveSymbols) -> MassiveMarket {
        symbols.market(&self.base, &self.quote, &self.kind)
    }
}

impl<InstrumentKey> MassiveInstrument for Keyed<InstrumentKey, MarketDataInstrument> {
    fn massive_market(&self, symbols: MassiveSymbols) -> MassiveMarket {
        self.value.massive_market(symbols)
    }
}

/// Takes the exchange-side name as given: the caller has already supplied the cluster's own
/// spelling, and there is nothing to reconstruct.
impl<InstrumentKey> MassiveInstrument for MarketInstrumentData<InstrumentKey> {
    fn massive_market(&self, _symbols: MassiveSymbols) -> MassiveMarket {
        MassiveMarket(self.name_exchange.name().clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_instrument::instrument::{
        kind::option::{OptionExercise, OptionKind},
        market_data::kind::MarketDataOptionContract,
    };

    fn instrument(base: &str, quote: &str, kind: MarketDataInstrumentKind) -> MarketDataInstrument {
        MarketDataInstrument::new(base, quote, kind)
    }

    fn contract() -> MarketDataInstrumentKind {
        MarketDataInstrumentKind::Option(MarketDataOptionContract {
            kind: OptionKind::Call,
            exercise: OptionExercise::American,
            expiry: "2025-12-19T21:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            strike: dec!(150),
        })
    }

    #[test]
    fn each_cluster_spells_markets_as_its_messages_do() {
        let spot = |base, quote, symbols: MassiveSymbols| {
            instrument(base, quote, MarketDataInstrumentKind::Spot)
                .massive_market(symbols)
                .0
        };

        assert_eq!(spot("aapl", "usd", MassiveSymbols::Ticker), "AAPL");
        assert_eq!(spot("btc", "usd", MassiveSymbols::CryptoPair), "BTC-USD");
        assert_eq!(spot("eur", "usd", MassiveSymbols::CurrencyPair), "EUR/USD");
        assert_eq!(
            instrument("aapl", "usd", contract())
                .massive_market(MassiveSymbols::OptionContract)
                .0,
            "O:AAPL251219C00150000",
        );
    }

    #[test]
    fn an_instrument_with_no_option_symbol_is_not_mistaken_for_one() {
        let market = instrument("aapl", "usd", MarketDataInstrumentKind::Spot)
            .massive_market(MassiveSymbols::OptionContract);

        assert!(!is_option_contract(&market.0), "{market}");
        assert!(is_option_contract("O:AAPL251219C00150000"));
        assert!(!is_option_contract("AAPL251219C00150000"));
    }
}
