use super::channel::KrakenChannel;
use crate::{Identifier, exchange::ExchangeSub};
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use smol_str::SmolStr;

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
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
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
/// Deserialising fails for a message from another channel, and for one whose items name more
/// than one symbol, since a message is routed to a single subscription.
///
/// See [`KrakenMessage`] for full raw payload examples.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize)]
pub struct KrakenData<T> {
    /// The subscription the items route to. `None` for a message carrying no items, which is
    /// dropped.
    pub subscription_id: Option<SubscriptionId>,
    pub data: Vec<T>,
}

impl<T> Identifier<Option<SubscriptionId>> for KrakenData<T> {
    fn id(&self) -> Option<SubscriptionId> {
        self.subscription_id.clone()
    }
}

impl<'de, T> Deserialize<'de> for KrakenData<T>
where
    T: KrakenChannelData,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw<T> {
            channel: SmolStr,
            data: Vec<T>,
        }

        let Raw { channel, data } = Raw::<T>::deserialize(deserializer)?;
        if channel != T::CHANNEL.as_ref() {
            return Err(serde::de::Error::custom(format_args!(
                "expected a Kraken {} message, got {channel}",
                T::CHANNEL.as_ref()
            )));
        }

        let subscription_id = match data.split_first() {
            None => None,
            Some((first, rest)) => {
                if let Some(other) = rest.iter().find(|item| item.symbol() != first.symbol()) {
                    return Err(serde::de::Error::custom(format_args!(
                        "Kraken {channel} message mixes symbols {} and {}",
                        first.symbol(),
                        other.symbol()
                    )));
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

/// [`Kraken`](super::Kraken) messages received over the WebSocket which are not subscription data.
///
/// eg/ [`Kraken`](super::Kraken) sends a [`KrakenChannelEvent::Heartbeat`] each second while
/// subscriptions are active and no other message is due.
///
/// See [`KrakenMessage`] for full raw payload examples.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum KrakenEvent {
    /// A message on a channel that carries no subscription data.
    Channel(KrakenChannelEvent),
    /// A response to a request, such as a pong or an error.
    Response(KrakenResponse),
}

/// [`Kraken`](super::Kraken) channels that carry no subscription data.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat> <br>
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/status>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "snake_case")]
pub enum KrakenChannelEvent {
    Heartbeat,
    Status,
}

/// [`Kraken`](super::Kraken) response to a request, eg/ `{"method": "pong", ...}`, or an error
/// such as `{"error": "Malformed request", "method": "error", "success": false, ...}`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenResponse {
    pub method: SmolStr,
    #[serde(default)]
    pub error: Option<String>,
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

    #[test]
    fn heartbeat_and_status_are_events() {
        let heartbeat: KrakenMessage<()> =
            serde_json::from_str(r#"{"channel":"heartbeat"}"#).unwrap();
        assert_eq!(
            heartbeat,
            KrakenMessage::Event(KrakenEvent::Channel(KrakenChannelEvent::Heartbeat))
        );

        let status: KrakenMessage<()> = serde_json::from_str(
            r#"{"channel":"status","type":"update","data":[{"api_version":"v2","connection_id":1,"system":"online","version":"2.0.0"}]}"#,
        )
        .unwrap();
        assert_eq!(
            status,
            KrakenMessage::Event(KrakenEvent::Channel(KrakenChannelEvent::Status))
        );
    }

    #[test]
    fn responses_are_events() {
        let pong: KrakenMessage<()> = serde_json::from_str(
            r#"{"method":"pong","req_id":1,"time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        )
        .unwrap();
        assert_eq!(
            pong,
            KrakenMessage::Event(KrakenEvent::Response(KrakenResponse {
                method: "pong".into(),
                error: None,
            }))
        );

        let error: KrakenMessage<()> = serde_json::from_str(
            r#"{"error":"Malformed request","method":"error","success":false,"time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        )
        .unwrap();
        assert_eq!(
            error,
            KrakenMessage::Event(KrakenEvent::Response(KrakenResponse {
                method: "error".into(),
                error: Some("Malformed request".to_owned()),
            }))
        );
    }
}
