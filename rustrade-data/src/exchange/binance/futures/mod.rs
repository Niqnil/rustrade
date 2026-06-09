use self::liquidation::BinanceLiquidation;
use super::{Binance, ExchangeServer};
use crate::{
    NoInitialSnapshots,
    exchange::{
        StreamSelector,
        binance::{
            BinanceWsStream,
            futures::l2::{
                BinanceFuturesUsdOrderBooksL2SnapshotFetcher,
                BinanceFuturesUsdOrderBooksL2Transformer,
            },
            kline::BinanceContinuousKline,
        },
    },
    instrument::InstrumentData,
    subscription::{book::OrderBooksL2, candle::Candles, liquidation::Liquidations},
    transformer::stateless::StatelessTransformer,
};
use rustrade_instrument::exchange::ExchangeId;
use std::fmt::{Display, Formatter};

/// Level 2 OrderBook types.
pub mod l2;

/// Liquidation types.
pub mod liquidation;

/// [`BinanceFuturesUsd`] `/public`-tier WebSocket server base url.
///
/// Binance routes `fstream.binance.com` into mutually-exclusive path tiers; the legacy unrouted
/// `/ws` was deprecated and stopped delivering `/market`-tier streams (e.g. `@forceOrder`) on
/// 2026-04-23. Public-tier streams (`@trade`/`@bookTicker`/`@depth`) live here on `/public/ws`.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
pub const WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD: &str = "wss://fstream.binance.com/public/ws";

/// [`BinanceFuturesUsdMarket`] `/market`-tier WebSocket server base url.
///
/// The `/market` tier is a **distinct connection** from `/public` — a socket is bound to exactly
/// one tier and silently delivers zero frames for channels outside it. `@continuousKline_`
/// (klines) and `@forceOrder` (liquidations) are `/market`-tier streams.
pub const WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD_MARKET: &str =
    "wss://fstream.binance.com/market/ws";

/// [`Binance`] perpetual usd exchange (`/public` WS tier: trade/bookTicker/depth).
pub type BinanceFuturesUsd = Binance<BinanceServerFuturesUsd>;

/// [`Binance`] perpetual usd exchange on the `/market` WS tier (klines + liquidations).
///
/// Shares [`ExchangeId::BinanceFuturesUsd`] with [`BinanceFuturesUsd`] — the [`ExchangeId`] is the
/// *logical* exchange (used for event tagging and dynamic dispatch); the server type is a
/// connection-layer detail selecting the WS path. Streams that Binance routes to `/market`
/// (klines, liquidations) are implemented only on this type so routing them to the `/public`
/// tier is a compile error rather than a silent dead stream.
pub type BinanceFuturesUsdMarket = Binance<BinanceServerFuturesUsdMarket>;

/// [`Binance`] perpetual usd [`ExchangeServer`] (`/public` WS tier).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerFuturesUsd;

impl ExchangeServer for BinanceServerFuturesUsd {
    const ID: ExchangeId = ExchangeId::BinanceFuturesUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD
    }
}

/// [`Binance`] perpetual usd [`ExchangeServer`] for the `/market` WS tier.
///
/// Same [`const ID`](ExchangeServer::ID) as [`BinanceServerFuturesUsd`]; only the
/// [`websocket_url`](ExchangeServer::websocket_url) differs (`/market/ws`).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerFuturesUsdMarket;

impl ExchangeServer for BinanceServerFuturesUsdMarket {
    const ID: ExchangeId = ExchangeId::BinanceFuturesUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD_MARKET
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = BinanceFuturesUsdOrderBooksL2SnapshotFetcher;
    type Stream = BinanceWsStream<BinanceFuturesUsdOrderBooksL2Transformer<Instrument::Key>>;
}

// `@forceOrder` is a `/market`-tier stream, so `Liquidations` is implemented on the market-tier
// server type (not `BinanceFuturesUsd`). Routing a liquidation sub to `/public` is a compile error.
impl<Instrument> StreamSelector<Instrument, Liquidations> for BinanceFuturesUsdMarket
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, Liquidations, BinanceLiquidation>,
    >;
}

// Live futures klines: `@continuousKline_<interval>` on the `/market` tier.
impl<Instrument> StreamSelector<Instrument, Candles> for BinanceFuturesUsdMarket
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, Candles, BinanceContinuousKline>,
    >;
}

impl Display for BinanceFuturesUsd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceFuturesUsd")
    }
}

impl Display for BinanceFuturesUsdMarket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceFuturesUsdMarket")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two futures server types share one [`ExchangeId`] but must resolve distinct WS URLs:
    /// `/public/ws` (trade/book/depth) vs `/market/ws` (klines/liquidations). A regression here
    /// would silently dead-stream one tier.
    #[test]
    fn futures_server_types_resolve_distinct_tier_urls() {
        assert_eq!(
            BinanceServerFuturesUsd::websocket_url(),
            "wss://fstream.binance.com/public/ws"
        );
        assert_eq!(
            BinanceServerFuturesUsdMarket::websocket_url(),
            "wss://fstream.binance.com/market/ws"
        );
        assert_ne!(
            BinanceServerFuturesUsd::websocket_url(),
            BinanceServerFuturesUsdMarket::websocket_url()
        );
        assert_eq!(
            BinanceServerFuturesUsd::ID,
            BinanceServerFuturesUsdMarket::ID
        );
    }
}
