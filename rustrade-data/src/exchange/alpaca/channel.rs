use super::Alpaca;
use crate::{
    Identifier,
    subscription::{Subscription, quote::Quotes, trade::PublicTrades},
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
}

impl AlpacaChannel {
    /// The channel [`AsRef<str>`] spells as `name`, if any.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "trades" => Some(Self::Trades),
            "quotes" => Some(Self::Quotes),
            _ => None,
        }
    }
}

impl AsRef<str> for AlpacaChannel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Trades => "trades",
            Self::Quotes => "quotes",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_as_ref() {
        assert_eq!(AlpacaChannel::Trades.as_ref(), "trades");
        assert_eq!(AlpacaChannel::Quotes.as_ref(), "quotes");
    }

    #[test]
    fn every_channel_round_trips_through_its_name() {
        for channel in [AlpacaChannel::Trades, AlpacaChannel::Quotes] {
            assert_eq!(AlpacaChannel::from_name(channel.as_ref()), Some(channel));
        }
        assert_eq!(AlpacaChannel::from_name("bars"), None);
    }
}
