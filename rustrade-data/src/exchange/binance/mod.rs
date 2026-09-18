use self::{
    book::l1::BinanceOrderBookL1, channel::BinanceChannel, futures::BinanceFuturesUsd,
    kline::BinanceKline, market::BinanceMarket, spot::BinanceSpot,
    subscription::BinanceSubResponse, trade::BinanceTrade,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{
        Map,
        book::OrderBooksL1,
        candle::{CandleInterval, Candles},
        trade::PublicTrades,
    },
    transformer::stateless::StatelessTransformer,
};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::protocol::websocket::{WebSocketSerdeParser, WsMessage};
use std::{fmt::Debug, marker::PhantomData};
use url::Url;

/// OrderBook types common to both [`BinanceSpot`] and
/// [`BinanceFuturesUsd`].
pub mod book;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Dedicated error type for the Binance historical klines REST client.
mod error;
pub use error::BinanceDataError;

/// Historical klines (OHLCV candles) via Binance's public, unauthenticated REST
/// endpoints — spot (`/api/v3/klines`) and futures continuous
/// (`/fapi/v1/continuousKlines`).
pub mod historical;
pub use historical::BinanceHistoricalClient;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BinanceFuturesUsd`].
pub mod futures;

/// Live kline (candle) wire models common to [`BinanceSpot`] (`@kline_`)
/// and [`BinanceFuturesUsd`] (`@continuousKline_`).
pub mod kline;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BinanceSpot`].
pub mod spot;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](rustrade_integration::Validator) common to both [`BinanceSpot`]
/// and [`BinanceFuturesUsd`].
pub mod subscription;

/// Public trade types common to both [`BinanceSpot`] and
/// [`BinanceFuturesUsd`].
pub mod trade;

/// Convenient type alias for a Binance [`ExchangeWsStream`] using [`WebSocketSerdeParser`].
pub type BinanceWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// Whether Binance publishes klines at the given [`CandleInterval`].
///
/// [`CandleInterval`] is the venue-agnostic union of every resolution any connector
/// supports, so it is a **superset** of Binance's kline menu: Binance's only
/// sub-minute kline is `1s`, and it serves no `5s`/`15s`/`30s` stream (nor the
/// matching REST `interval`, which 400s).
///
/// # Where this is applied
///
/// Only the **dynamic** builder gates on this:
/// [`exchange_supports_instrument_kind_sub_kind`](crate::subscription::exchange_supports_instrument_kind_sub_kind)
/// consults it for [`SubKind::Candles`](crate::subscription::SubKind::Candles), so an unsupported
/// interval is rejected before a socket is opened.
///
/// The **typed** builder does not. Its validation goes through
/// [`exchange_supports_instrument_kind`](crate::subscription::exchange_supports_instrument_kind),
/// which never inspects the resolution, and
/// [`BinanceChannel::spot_candle`](channel::BinanceChannel::spot_candle) /
/// [`futures_candle`](channel::BinanceChannel::futures_candle) are infallible by the
/// [`Identifier`](crate::Identifier) contract — so a typed `5s` subscription builds a well-formed
/// `@kline_5s` channel name for a stream Binance does not publish, and is rejected only once
/// Binance answers the SUBSCRIBE (surfaced by
/// [`BinanceSubResponse`]'s
/// [`Validator`](rustrade_integration::Validator)).
///
/// The failure is observable either way; call this yourself before a typed subscription if you
/// want it before the socket rather than after.
#[must_use]
pub fn supports_candle_interval(interval: CandleInterval) -> bool {
    match interval {
        CandleInterval::Sec5 | CandleInterval::Sec15 | CandleInterval::Sec30 => false,
        CandleInterval::Sec1
        | CandleInterval::Min1
        | CandleInterval::Min3
        | CandleInterval::Min5
        | CandleInterval::Min15
        | CandleInterval::Min30
        | CandleInterval::Hour1
        | CandleInterval::Hour2
        | CandleInterval::Hour4
        | CandleInterval::Hour6
        | CandleInterval::Hour8
        | CandleInterval::Hour12
        | CandleInterval::Day1
        | CandleInterval::Day3
        | CandleInterval::Week1
        | CandleInterval::Month1 => true,
    }
}

/// Generic [`Binance<Server>`](Binance) exchange.
///
/// ### Notes
/// A `Server` [`ExchangeServer`] implementations exists for
/// [`BinanceSpot`] and [`BinanceFuturesUsd`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Binance<Server> {
    server: PhantomData<Server>,
}

impl<Server> Connector for Binance<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = BinanceChannel;
    type Market = BinanceMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = BinanceSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(Server::websocket_url())
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| {
                // Note:
                // Market must be lowercase when subscribing, but lowercase in general since
                // Binance sends message with uppercase MARKET (eg/ BTCUSDT).
                format!(
                    "{}{}",
                    sub.market.as_ref().to_lowercase(),
                    sub.channel.as_ref()
                )
            })
            .collect::<Vec<String>>();

        vec![WsMessage::text(
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": stream_names,
                "id": 1
            })
            .to_string(),
        )]
    }

    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

// `PublicTrades` and `OrderBooksL1` are implemented per-server (NOT blanket over
// `Binance<Server>`) so they ride only the `/public`-tier server types — `BinanceSpot` (`/ws`)
// and `BinanceFuturesUsd` (`/public/ws`). The market-tier `BinanceFuturesUsdMarket` (`/market/ws`)
// deliberately has no such impl, making a trade/L1 subscription on that tier — which Binance
// would silently dead-stream — a compile error instead. This mirrors `OrderBooksL2`, which is
// already per-server.
impl<Instrument> StreamSelector<Instrument, PublicTrades> for BinanceSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        BinanceWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, BinanceTrade>>;
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        BinanceWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, BinanceTrade>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for BinanceSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, BinanceOrderBookL1>,
    >;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, BinanceOrderBookL1>,
    >;
}

// Live spot klines: `@kline_<interval>` on `BinanceSpot`'s `/ws` tier.
impl<Instrument> StreamSelector<Instrument, Candles> for BinanceSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        BinanceWsStream<StatelessTransformer<Self, Instrument::Key, Candles, BinanceKline>>;
}

impl<'de, Server> serde::Deserialize<'de> for Binance<Server>
where
    Server: ExchangeServer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <String as serde::Deserialize>::deserialize(deserializer)?;

        if input.as_str() == Self::ID.as_str() {
            Ok(Self::default())
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(input.as_str()),
                &Self::ID.as_str(),
            ))
        }
    }
}

impl<Server> serde::Serialize for Binance<Server>
where
    Server: ExchangeServer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(Self::ID.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `CandleInterval` is a union across venues, so this guard must be re-reviewed
    /// whenever a variant is added — the exhaustive `match` inside
    /// `supports_candle_interval` is the compile gate, and this test pins the answer.
    #[test]
    fn supports_candle_interval_rejects_only_the_sub_minute_intervals_binance_lacks() {
        for interval in CandleInterval::ALL {
            let expected = !matches!(
                interval,
                CandleInterval::Sec5 | CandleInterval::Sec15 | CandleInterval::Sec30
            );
            assert_eq!(
                supports_candle_interval(interval),
                expected,
                "interval {interval}"
            );
        }
    }
}
