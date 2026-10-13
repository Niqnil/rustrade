use super::Kraken;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL1, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] channel to be subscribed to.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct KrakenChannel(pub &'static str);

impl KrakenChannel {
    /// [`Kraken`] real-time trades channel name.
    ///
    /// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
    pub const TRADES: Self = Self("trade");

    /// [`Kraken`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// The ticker channel, subscribed with `event_trigger: "bbo"` so it updates on every change
    /// to the best bid or offer rather than on each trade.
    ///
    /// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker>
    pub const ORDER_BOOK_L1: Self = Self("ticker");
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, PublicTrades> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::TRADES
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, OrderBooksL1> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::ORDER_BOOK_L1
    }
}

impl AsRef<str> for KrakenChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
