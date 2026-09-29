//! The Massive WebSocket channel each subscription kind reads.

use super::{
    Massive,
    market::{MassiveServer, MassiveTradeServer},
};
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::OrderBooksL1,
        candle::{CandleInterval, Candles},
        quote::Quotes,
        trade::PublicTrades,
    },
};

/// A Massive WebSocket channel, spelled as the event its messages carry: `XT`, `Q`, `CAS`.
///
/// A subscription is the channel and a market joined by a dot, `XT.BTC-USD`, and every message it
/// delivers carries the channel as its `ev`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum MassiveChannel {
    /// A channel the cluster publishes.
    Event(&'static str),
    /// Candles at an interval Massive does not aggregate at. It has aggregates per second and per
    /// minute only, so any other interval is refused before anything is sent.
    UnsupportedInterval(CandleInterval),
}

impl AsRef<str> for MassiveChannel {
    /// The event, or nothing for an unsupported interval, which is refused before it is spelled.
    fn as_ref(&self) -> &str {
        match self {
            Self::Event(event) => event,
            Self::UnsupportedInterval(_) => "",
        }
    }
}

impl<Server, Instrument> Identifier<MassiveChannel>
    for Subscription<Massive<Server>, Instrument, PublicTrades>
where
    Server: MassiveTradeServer,
{
    fn id(&self) -> MassiveChannel {
        MassiveChannel::Event(Server::TRADES)
    }
}

impl<Server, Instrument> Identifier<MassiveChannel>
    for Subscription<Massive<Server>, Instrument, Quotes>
where
    Server: MassiveServer,
{
    fn id(&self) -> MassiveChannel {
        MassiveChannel::Event(Server::QUOTES)
    }
}

/// The quote channel, read as a top of book.
impl<Server, Instrument> Identifier<MassiveChannel>
    for Subscription<Massive<Server>, Instrument, OrderBooksL1>
where
    Server: MassiveServer,
{
    fn id(&self) -> MassiveChannel {
        MassiveChannel::Event(Server::QUOTES)
    }
}

impl<Server, Instrument> Identifier<MassiveChannel>
    for Subscription<Massive<Server>, Instrument, Candles>
where
    Server: MassiveServer,
{
    fn id(&self) -> MassiveChannel {
        match self.kind.interval {
            CandleInterval::Sec1 => MassiveChannel::Event(Server::SECOND_AGGREGATES),
            CandleInterval::Min1 => MassiveChannel::Event(Server::MINUTE_AGGREGATES),
            other => MassiveChannel::UnsupportedInterval(other),
        }
    }
}
