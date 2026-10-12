use self::{
    book::l1::KrakenOrderBookL1, channel::KrakenChannel, market::KrakenMarket,
    message::KrakenMessage, subscription::KrakenSubResponse, trade::KrakenTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{book::OrderBooksL1, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use derive_more::Display;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
    subscription::SubscriptionId,
};
use rustrade_macro::{DeExchange, SerExchange};
use serde_json::json;
use url::Url;

/// OrderBook types for [`Kraken`].
pub mod book;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`]  specific market used for generating [`Connector::requests`].
pub mod market;

/// [`KrakenMessage`] type for [`Kraken`].
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](rustrade_integration) for [`Kraken`].
pub mod subscription;

/// Public trade types for [`Kraken`].
pub mod trade;

/// [`Kraken`] WebSocket v2 server base url.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
pub const BASE_URL_KRAKEN: &str = "wss://ws.kraken.com/v2";

/// Convenient type alias for a Kraken [`ExchangeWsStream`] using [`WebSocketSerdeParser`].
pub type KrakenWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Kraken`] exchange, over its spot WebSocket v2 API.
///
/// Symbols are spelled `BASE/QUOTE` in upper case, eg/ `BTC/USD`. Kraken refuses v1 spellings
/// such as `XBT/USD` per symbol, and the failed subscription is named in the error.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct Kraken;

impl Connector for Kraken {
    const ID: ExchangeId = ExchangeId::Kraken;
    type Channel = KrakenChannel;
    type Market = KrakenMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = KrakenSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(BASE_URL_KRAKEN)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                let mut params = json!({
                    "channel": channel.as_ref(),
                    "symbol": [market.as_ref()],
                });
                if channel == KrakenChannel::ORDER_BOOK_L1 {
                    // Update on every change to the best bid or offer, and open with a snapshot
                    // of it, rather than updating on trades
                    params["event_trigger"] = json!("bbo");
                    params["snapshot"] = json!(true);
                } else if channel == KrakenChannel::TRADES {
                    // A trade snapshot replays recent trades, which would read as new ones
                    params["snapshot"] = json!(false);
                }

                WsMessage::text(
                    json!({
                        "method": "subscribe",
                        "params": params,
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn acknowledged_subscription(response: &Self::SubResponse) -> Option<SubscriptionId> {
        response.subscription_id()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Kraken
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        KrakenWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, KrakenTrades>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for Kraken
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = KrakenWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, KrakenOrderBookL1>,
    >;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::kraken::market::KrakenMarket;

    fn request(channel: KrakenChannel) -> serde_json::Value {
        let request = Kraken::requests(vec![ExchangeSub {
            channel,
            market: KrakenMarket("BTC/USD".into()),
        }])
        .remove(0);
        serde_json::from_str(request.to_text().unwrap()).unwrap()
    }

    #[test]
    fn requests_subscribe_in_the_v2_shape() {
        assert_eq!(
            request(KrakenChannel::TRADES),
            json!({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": false},
            })
        );
        assert_eq!(
            request(KrakenChannel::ORDER_BOOK_L1),
            json!({
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": ["BTC/USD"],
                    "event_trigger": "bbo",
                    "snapshot": true,
                },
            })
        );
    }
}
