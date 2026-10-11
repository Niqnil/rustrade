use super::channel::KrakenChannel;
use crate::{Identifier, exchange::ExchangeSub};
use rustrade_integration::subscription::SubscriptionId;
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeOwned, IgnoredAny, MapAccess, Visitor},
};
use smol_str::SmolStr;
use std::marker::PhantomData;

/// [`Kraken`](super::Kraken) message variants that can be received over
/// [`WebSocket`](rustrade_integration::protocol::websocket::WebSocket).
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
///
/// #### OrderBookL1
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker>
/// ```json
/// {
///     "channel": "ticker",
///     "type": "update",
///     "data": [
///         {
///             "symbol": "BTC/USD",
///             "bid": 5698.4,
///             "bid_qty": 1.01234567,
///             "ask": 5700.0,
///             "ask_qty": 0.98765432,
///             "last": 5699.1,
///             "volume": 1234.5,
///             "vwap": 5650.2,
///             "low": 5600.0,
///             "high": 5710.0,
///             "change": 12.3,
///             "change_pct": 0.22,
///             "timestamp": "2018-11-12T21:14:59.545897Z"
///         }
///     ]
/// }
/// ```
///
/// #### Trades
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
/// ```json
/// {
///     "channel": "trade",
///     "type": "update",
///     "data": [
///         {
///             "symbol": "BTC/USD",
///             "side": "sell",
///             "price": 5541.2,
///             "qty": 0.15850568,
///             "ord_type": "limit",
///             "trade_id": 1001,
///             "timestamp": "2018-08-18T17:40:57.321597Z"
///         }
///     ]
/// }
/// ```
///
/// #### Heartbeat
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat>
/// ```json
/// {
///     "channel": "heartbeat"
/// }
/// ```
///
/// #### Status
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/status>
/// ```json
/// {
///     "channel": "status",
///     "type": "update",
///     "data": [
///         {
///             "api_version": "v2",
///             "connection_id": 1234567890,
///             "system": "online",
///             "version": "2.0.0"
///         }
///     ]
/// }
/// ```
///
/// #### Error
/// ```json
/// {
///     "error": "Malformed request",
///     "method": "error",
///     "success": false,
///     "time_in": "2018-11-12T21:14:59.545897Z",
///     "time_out": "2018-11-12T21:14:59.545901Z"
/// }
/// ```
///
/// ### Parsing
/// A message is told apart by its `channel`: the data channel `T` streams, `heartbeat` or
/// `status`. One without a `channel` is a response to a request, such as a pong. Parsing fails,
/// with a reason naming the cause, for:
/// - a response carrying an `error`, which Kraken sends to a request it refused;
/// - a channel other than these;
/// - a data message the [`KrakenData`] rules refuse, or whose items do not parse.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize)]
#[serde(untagged)]
pub enum KrakenMessage<T> {
    Data(T),
    Event(KrakenEvent),
}

impl<T> Identifier<Option<SubscriptionId>> for KrakenMessage<T>
where
    T: Identifier<Option<SubscriptionId>>,
{
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Data(data) => data.id(),
            Self::Event(_) => None,
        }
    }
}

impl<'de, T> Deserialize<'de> for KrakenMessage<KrakenData<T>>
where
    T: KrakenChannelData,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(KrakenMessageVisitor(PhantomData))
    }
}

/// Dispatches a [`KrakenMessage`] on its `channel` as it reads the message, rather than buffering
/// it to try each variant in turn, so a refused data message keeps its own reason.
struct KrakenMessageVisitor<T>(PhantomData<T>);

impl<'de, T> Visitor<'de> for KrakenMessageVisitor<T>
where
    T: KrakenChannelData,
{
    type Value = KrakenMessage<KrakenData<T>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "a Kraken WebSocket v2 message")
    }

    fn visit_map<Map>(self, mut map: Map) -> Result<Self::Value, Map::Error>
    where
        Map: MapAccess<'de>,
    {
        let mut channel: Option<SmolStr> = None;
        let mut data: Option<Vec<T>> = None;
        // `data` read before `channel` named its type. Kraken sends `channel` first, so this is
        // only a fallback for a reordered message
        let mut data_unread: Option<serde_json::Value> = None;
        let mut method: Option<SmolStr> = None;
        let mut error: Option<String> = None;

        while let Some(key) = map.next_key::<SmolStr>()? {
            match key.as_str() {
                "channel" => channel = Some(map.next_value()?),
                "data" if channel.as_deref() == Some(T::CHANNEL.as_ref()) => {
                    data = Some(map.next_value()?)
                }
                "data" if channel.is_none() => data_unread = Some(map.next_value()?),
                "method" => method = Some(map.next_value()?),
                "error" => error = Some(map.next_value()?),
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        match (channel, method) {
            (Some(channel), _) if channel == T::CHANNEL.as_ref() => {
                let data = match (data, data_unread) {
                    (Some(data), _) => data,
                    (None, Some(unread)) => {
                        Vec::<T>::deserialize(unread).map_err(de::Error::custom)?
                    }
                    (None, None) => return Err(de::Error::missing_field("data")),
                };
                KrakenData::new(data)
                    .map(KrakenMessage::Data)
                    .map_err(de::Error::custom)
            }
            (Some(channel), _) => match channel.as_str() {
                "heartbeat" => Ok(KrakenMessage::Event(KrakenEvent::Heartbeat)),
                "status" => Ok(KrakenMessage::Event(KrakenEvent::Status)),
                other => Err(de::Error::custom(format_args!(
                    "unexpected Kraken channel {other} on a {} stream",
                    T::CHANNEL.as_ref()
                ))),
            },
            (None, Some(method)) => match error {
                None => Ok(KrakenMessage::Event(KrakenEvent::Response { method })),
                Some(error) => Err(de::Error::custom(format_args!(
                    "Kraken refused a request ({method}): {error}"
                ))),
            },
            (None, None) => Err(de::Error::custom(
                "Kraken message names neither a channel nor a method",
            )),
        }
    }
}

/// An item a [`Kraken`](super::Kraken) data channel streams, such as one trade.
pub trait KrakenChannelData: DeserializeOwned {
    /// The channel that streams this item.
    const CHANNEL: KrakenChannel;

    /// The symbol the item belongs to, eg/ `BTC/USD`.
    fn symbol(&self) -> &str;
}

/// One [`Kraken`](super::Kraken) data channel message: a batch of items for one symbol, with the
/// [`SubscriptionId`] they route to (eg/ "trade|BTC/USD").
///
/// A message whose items name more than one symbol is refused, since a message is routed to a
/// single subscription.
///
/// See [`KrakenMessage`] for full raw payload examples.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize)]
pub struct KrakenData<T> {
    /// The subscription the items route to. `None` for a message carrying no items, which is
    /// dropped.
    pub subscription_id: Option<SubscriptionId>,
    pub data: Vec<T>,
}

impl<T> KrakenData<T>
where
    T: KrakenChannelData,
{
    /// The batch `data`, routed by its items' symbol; an error naming two of them if they differ.
    fn new(data: Vec<T>) -> Result<Self, String> {
        let subscription_id = match data.split_first() {
            None => None,
            Some((first, rest)) => {
                if let Some(other) = rest.iter().find(|item| item.symbol() != first.symbol()) {
                    return Err(format!(
                        "Kraken {} message mixes symbols {} and {}",
                        T::CHANNEL.as_ref(),
                        first.symbol(),
                        other.symbol()
                    ));
                }
                Some(ExchangeSub::from((T::CHANNEL, first.symbol())).id())
            }
        };

        Ok(Self {
            subscription_id,
            data,
        })
    }
}

impl<T> Identifier<Option<SubscriptionId>> for KrakenData<T> {
    fn id(&self) -> Option<SubscriptionId> {
        self.subscription_id.clone()
    }
}

/// [`Kraken`](super::Kraken) messages received over the WebSocket which are not subscription data.
///
/// eg/ [`Kraken`](super::Kraken) sends a [`KrakenEvent::Heartbeat`] each second while
/// subscriptions are active and no other message is due.
///
/// See [`KrakenMessage`] for full raw payload examples.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat> <br>
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/status>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub enum KrakenEvent {
    /// The `heartbeat` channel.
    Heartbeat,
    /// The `status` channel, sent on connecting and when the venue's state changes.
    Status,
    /// A response to a request that succeeded, such as a pong.
    Response { method: SmolStr },
}

/// [`Kraken`](super::Kraken) error message received over the WebSocket, naming the symbol it
/// concerns when there is one.
///
/// Used as a [`KrakenSubResponse::Error`](super::subscription::KrakenSubResponse).
///
/// See [`KrakenMessage`] for full raw payload examples.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenError {
    #[serde(rename = "error")]
    pub message: String,
    #[serde(default)]
    pub symbol: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::kraken::trade::KrakenTrades;

    const TIMES: &str =
        r#""time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z""#;

    fn trade(symbol: &str) -> String {
        format!(
            r#"{{"symbol":"{symbol}","side":"buy","price":1.5,"qty":2,"ord_type":"limit","trade_id":1,"timestamp":"2018-11-12T21:14:59.545897Z"}}"#
        )
    }

    fn parse_error(message: &str) -> String {
        serde_json::from_str::<KrakenTrades>(message)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn heartbeat_and_status_are_events() {
        let heartbeat: KrakenTrades = serde_json::from_str(r#"{"channel":"heartbeat"}"#).unwrap();
        assert_eq!(heartbeat, KrakenMessage::Event(KrakenEvent::Heartbeat));

        let status: KrakenTrades = serde_json::from_str(
            r#"{"channel":"status","type":"update","data":[{"api_version":"v2","connection_id":1,"system":"online","version":"2.0.0"}]}"#,
        )
        .unwrap();
        assert_eq!(status, KrakenMessage::Event(KrakenEvent::Status));
    }

    #[test]
    fn a_successful_response_is_an_event() {
        let pong: KrakenTrades =
            serde_json::from_str(&format!(r#"{{"method":"pong","req_id":1,{TIMES}}}"#)).unwrap();
        assert_eq!(
            pong,
            KrakenMessage::Event(KrakenEvent::Response {
                method: "pong".into()
            })
        );
    }

    #[test]
    fn a_refused_request_is_an_error_quoting_kraken() {
        let error = parse_error(&format!(
            r#"{{"error":"Malformed request","method":"error","success":false,{TIMES}}}"#
        ));
        assert!(
            error.contains("Kraken refused a request (error): Malformed request"),
            "{error}"
        );
    }

    #[test]
    fn a_message_mixing_symbols_is_refused_naming_them() {
        let error = parse_error(&format!(
            r#"{{"channel":"trade","type":"update","data":[{},{}]}}"#,
            trade("BTC/USD"),
            trade("ETH/USD")
        ));
        assert!(
            error.contains("Kraken trade message mixes symbols BTC/USD and ETH/USD"),
            "{error}"
        );
    }

    #[test]
    fn another_data_channel_is_refused_naming_it() {
        let error = parse_error(
            r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":1.0,"bid_qty":1.0,"ask":2.0,"ask_qty":1.0,"timestamp":"2018-11-12T21:14:59.545897Z"}]}"#,
        );
        assert!(
            error.contains("unexpected Kraken channel ticker on a trade stream"),
            "{error}"
        );
    }

    #[test]
    fn an_item_that_does_not_parse_keeps_its_reason() {
        let error = parse_error(
            r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1.5,"qty":2,"ord_type":"limit","timestamp":"2018-11-12T21:14:59.545897Z"}]}"#,
        );
        assert!(error.contains("missing field `trade_id`"), "{error}");
    }

    #[test]
    fn data_before_its_channel_still_parses() {
        let trades: KrakenTrades = serde_json::from_str(&format!(
            r#"{{"data":[{}],"type":"update","channel":"trade"}}"#,
            trade("BTC/USD")
        ))
        .unwrap();

        let KrakenMessage::Data(trades) = trades else {
            panic!("expected data, got {trades:?}");
        };
        assert_eq!(
            trades.subscription_id,
            Some(SubscriptionId::from("trade|BTC/USD"))
        );
        assert_eq!(trades.data.len(), 1);
        assert_eq!(trades.data[0].symbol, "BTC/USD");
    }
}
