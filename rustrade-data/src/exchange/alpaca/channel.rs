use super::{Alpaca, AlpacaStockServer};
use crate::{
    Identifier,
    subscription::{
        Subscription, book::OrderBooksL1, price_band::PriceBands, quote::Quotes,
        trade::PublicTrades, trading_status::TradingStatuses,
    },
};

/// Alpaca WebSocket channel types.
///
/// Maps to the subscription arrays in the subscribe message:
/// ```json
/// {"action":"subscribe","trades":["AAPL"],"quotes":["AAPL"]}
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum AlpacaChannel {
    /// Real-time trades stream.
    Trades,
    /// Real-time quotes stream (NBBO for equities, bid/ask for crypto).
    Quotes,
    /// Trading status: halts, pauses and resumptions. Stock feeds only.
    Statuses,
    /// Limit Up-Limit Down price bands. Stock feeds only.
    Lulds,
}

impl AlpacaChannel {
    /// The channel [`AsRef<str>`] spells as `name`, if any.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "trades" => Some(Self::Trades),
            "quotes" => Some(Self::Quotes),
            "statuses" => Some(Self::Statuses),
            "lulds" => Some(Self::Lulds),
            _ => None,
        }
    }
}

impl AsRef<str> for AlpacaChannel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Trades => "trades",
            Self::Quotes => "quotes",
            Self::Statuses => "statuses",
            Self::Lulds => "lulds",
        }
    }
}

impl<Server, Instrument> Identifier<AlpacaChannel>
    for Subscription<Alpaca<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> AlpacaChannel {
        AlpacaChannel::Trades
    }
}

impl<Server, Instrument> Identifier<AlpacaChannel>
    for Subscription<Alpaca<Server>, Instrument, Quotes>
{
    fn id(&self) -> AlpacaChannel {
        AlpacaChannel::Quotes
    }
}

/// Alpaca's top of book is its quote, so an L1 stream reads the `quotes` channel. It shares the
/// `(channel, symbol)` pair with any [`Quotes`] stream on the same symbol and feed.
impl<Server, Instrument> Identifier<AlpacaChannel>
    for Subscription<Alpaca<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> AlpacaChannel {
        AlpacaChannel::Quotes
    }
}

/// Implemented for the stock feeds only: Alpaca's crypto feed publishes no trading status, so a
/// crypto subscription to it does not compile.
impl<Server, Instrument> Identifier<AlpacaChannel>
    for Subscription<Alpaca<Server>, Instrument, TradingStatuses>
where
    Server: AlpacaStockServer,
{
    fn id(&self) -> AlpacaChannel {
        AlpacaChannel::Statuses
    }
}

/// Implemented for the stock feeds only: Alpaca's crypto feed publishes no price bands, so a
/// crypto subscription to them does not compile.
impl<Server, Instrument> Identifier<AlpacaChannel>
    for Subscription<Alpaca<Server>, Instrument, PriceBands>
where
    Server: AlpacaStockServer,
{
    fn id(&self) -> AlpacaChannel {
        AlpacaChannel::Lulds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_as_ref() {
        assert_eq!(AlpacaChannel::Trades.as_ref(), "trades");
        assert_eq!(AlpacaChannel::Quotes.as_ref(), "quotes");
        assert_eq!(AlpacaChannel::Statuses.as_ref(), "statuses");
        assert_eq!(AlpacaChannel::Lulds.as_ref(), "lulds");
    }

    #[test]
    fn every_channel_round_trips_through_its_name() {
        for channel in [
            AlpacaChannel::Trades,
            AlpacaChannel::Quotes,
            AlpacaChannel::Statuses,
            AlpacaChannel::Lulds,
        ] {
            assert_eq!(AlpacaChannel::from_name(channel.as_ref()), Some(channel));
        }
        assert_eq!(AlpacaChannel::from_name("bars"), None);
    }
}
