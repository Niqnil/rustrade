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
/// The variant therefore exists because a [`Connector`](crate::exchange::Connector) must name a
/// channel, not to select anything on the wire. It is not part of the
/// [`SubscriptionId`](rustrade_integration::subscription::SubscriptionId) either: a tick is filed
/// under its bare symbol, since a channel that never varies would only lengthen the identifier —
/// see [`LseSubMapper`](super::mapper::LseSubMapper). Both kinds therefore share one identifier per
/// symbol, which is unambiguous because each stream carries its own instrument map.
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
