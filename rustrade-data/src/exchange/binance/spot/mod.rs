use super::{Binance, ExchangeServer};
use crate::{
    exchange::{
        StreamSelector,
        binance::{
            BinanceWsStream,
            spot::l2::{
                BinanceSpotOrderBooksL2SnapshotFetcher, BinanceSpotOrderBooksL2Transformer,
            },
        },
    },
    instrument::InstrumentData,
    subscription::book::OrderBooksL2,
};
use rustrade_instrument::exchange::ExchangeId;
use std::fmt::{Display, Formatter};

/// Level 2 OrderBook types.
pub mod l2;

/// [`BinanceSpot`] WebSocket server base url.
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams>
pub const WEBSOCKET_BASE_URL_BINANCE_SPOT: &str = "wss://stream.binance.com:9443/ws";

/// [`Binance`] spot exchange.
///
/// # WebSocket connection limits
///
/// Existing Binance constraints a multi-symbol consumer must respect (not new — already handled by
/// `ReconnectingStream` + tungstenite's automatic pong, but worth stating):
/// - **1024 streams per connection.**
/// - **Incoming-message cap: 5 messages/second** (counts ping/pong + control frames) — space out
///   bulk `SUBSCRIBE`s.
/// - **Forced disconnect at 24h** (transparently re-established by `ReconnectingStream`).
/// - **Keepalive:** the server pings every **20s** with a **1-minute** pong deadline (tungstenite
///   auto-pongs, so no action) — note this is far tighter than futures' 3-min/10-min cadence.
/// - **300 connections per 5 minutes per IP.**
pub type BinanceSpot = Binance<BinanceServerSpot>;

/// [`Binance`] spot [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerSpot;

impl ExchangeServer for BinanceServerSpot {
    const ID: ExchangeId = ExchangeId::BinanceSpot;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BINANCE_SPOT
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for BinanceSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = BinanceSpotOrderBooksL2SnapshotFetcher;
    type Stream = BinanceWsStream<BinanceSpotOrderBooksL2Transformer<Instrument::Key>>;
}

impl Display for BinanceSpot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceSpot")
    }
}
