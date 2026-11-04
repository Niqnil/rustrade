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
/// 2026-04-23. Tier map: `@trade`/`@bookTicker`/`@depth` are `/public`-tier (here, on `/public/ws`);
/// `@forceOrder`/`@aggTrade`/`@continuousKline`/`@markPrice` are `/market`-tier (see
/// [`BinanceFuturesUsdMarket`]).
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
///
/// # A stream can go silent without ending
///
/// Binance has *demonstrated* that it re-routes which WS stream is served on which path tier (the
/// `/public` vs `/market` split — see [`BinanceFuturesUsdMarket`]), and futures `PublicTrades` here
/// rides the **undocumented** `@trade` stream. A tier change does **not** surface as an error: the
/// handshake still returns HTTP `101` and the socket then delivers **zero frames**, silently —
/// there is no `Err` and no disconnect. A subscription can also go quiet on a socket that stays
/// open and keeps answering pings.
///
/// The library ends such a stream when given a silence timeout:
/// [`MarketStreamOptions::with_silence_timeout`](crate::streams::consumer::MarketStreamOptions::with_silence_timeout),
/// set per connection. The stream then yields
/// [`DataError::SilenceTimeout`](crate::error::DataError::SilenceTimeout) and reconnects. Only the
/// caller knows how long its streams can be quiet when healthy, so the threshold is the caller's
/// and the timeout is off by default:
/// - **Dense streams:** a liquid symbol's `PublicTrades` delivers within seconds.
/// - **Candles:** only closed candles are delivered, so a 1-second candle stream delivers about
///   once a second per instrument and a 1-minute one once a minute. Set the threshold above the
///   interval.
/// - **One connection, one timer:** every subscription on a connection resets the same timer, so
///   a busy subscription keeps a silent one on the same connection from being noticed. Give a
///   stream that must be watched its own connection (its own
///   [`subscribe_with_options`](crate::streams::builder::StreamBuilder::subscribe_with_options)
///   call).
///
/// A trading system acting on this data should still treat a stream that keeps timing out as a
/// fault to alert or halt on: reconnecting does not bring back a stream Binance no longer serves on
/// that tier. If `@trade` ever goes dark, the known fix on rustrade's side is migrating futures
/// `PublicTrades` to `@aggTrade` on [`BinanceFuturesUsdMarket`] (`/market`); the `#[ignore]`d
/// `binance_ws_tier_canary` integration test re-verifies the live tier map on demand.
///
/// # WebSocket connection limits
///
/// Existing Binance constraints a multi-symbol consumer must respect (not new — already handled by
/// `ReconnectingStream` + tungstenite's automatic pong, but worth stating); they apply equally to
/// the `/market`-tier [`BinanceFuturesUsdMarket`] socket:
/// - **1024 streams per connection.**
/// - **Incoming-message cap: 10 messages/second** (counts ping/pong + control frames) — space out
///   bulk `SUBSCRIBE`s.
/// - **Forced disconnect at 24h** (transparently re-established by `ReconnectingStream`).
/// - **Keepalive:** the server pings every **3 minutes** with a **10-minute** pong deadline
///   (tungstenite auto-pongs, so no action) — note this is far looser than spot's 20s/1-min cadence.
pub type BinanceFuturesUsd = Binance<BinanceServerFuturesUsd>;

/// [`Binance`] perpetual usd exchange on the `/market` WS tier (klines + liquidations).
///
/// Shares [`ExchangeId::BinanceFuturesUsd`] with [`BinanceFuturesUsd`] — the [`ExchangeId`] is the
/// *logical* exchange (used for event tagging and dynamic dispatch); the server type is a
/// connection-layer detail selecting the WS path. Streams that Binance routes to `/market`
/// (klines, liquidations) are implemented only on this type so routing them to the `/public`
/// tier is a compile error rather than a silent dead stream.
///
/// 1-second continuous klines (`<pair>_perpetual@continuousKline_1s`) are delivered here and
/// nothing on the unrouted `/ws` path (observed November 2026). A stream that can go silent
/// without ending is covered under [`BinanceFuturesUsd`]'s "A stream can go silent without
/// ending".
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
