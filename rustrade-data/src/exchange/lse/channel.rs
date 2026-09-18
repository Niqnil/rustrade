use super::Lse;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL1, trade::PublicTrades},
};

/// The London Strategic Edge WebSocket channel.
///
/// # Why there is only one variant
/// The provider publishes exactly one data frame — the tick — and it carries the same seven keys
/// (`type`, `symbol`, `ts`, `price`, `bid`, `ask`, `volume`) on every dataset. There is no
/// per-channel subscription: a subscribe names a symbol and nothing else
/// (`{"action":"subscribe","symbol":"EUR/USD"}`), so both supported subscription kinds are decoded
/// from the same frame.
///
/// The variant therefore exists to supply the channel half of a
/// [`SubscriptionId`](rustrade_integration::subscription::SubscriptionId), not to select anything
/// on the wire. Both kinds map to it, and each stream carries its own instrument map, so the shared
/// identifier is unambiguous within a stream.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum LseChannel {
    /// The tick frame — a price, a bid, an ask and a size for one symbol.
    Tick,
}

impl AsRef<str> for LseChannel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Tick => "tick",
        }
    }
}

impl<Server, Instrument> Identifier<LseChannel>
    for Subscription<Lse<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> LseChannel {
        LseChannel::Tick
    }
}

impl<Server, Instrument> Identifier<LseChannel>
    for Subscription<Lse<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> LseChannel {
        LseChannel::Tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_as_ref() {
        assert_eq!(LseChannel::Tick.as_ref(), "tick");
    }
}
