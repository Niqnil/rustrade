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
}
