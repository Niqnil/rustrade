use super::message::KrakenError;
use crate::{Identifier, exchange::ExchangeSub};
use rustrade_integration::{Validator, error::SocketError, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};

/// [`Kraken`](super::Kraken) message received in response to WebSocket subscription requests.
///
/// Kraken answers each symbol of a request on its own, so one request for several symbols gets
/// one response per symbol.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
/// #### Subscription Trade Success
/// ```json
/// {
///   "method": "subscribe",
///   "result": {
///     "channel": "trade",
///     "snapshot": false,
///     "symbol": "BTC/USD"
///   },
///   "success": true,
///   "time_in": "2018-11-12T21:14:59.545897Z",
///   "time_out": "2018-11-12T21:14:59.545901Z"
/// }
/// ```
///
/// #### Subscription Trade Failure
/// ```json
/// {
///   "error": "Currency pair not supported XBT/USD",
///   "method": "subscribe",
///   "success": false,
///   "symbol": "XBT/USD",
///   "time_in": "2018-11-12T21:14:59.545897Z",
///   "time_out": "2018-11-12T21:14:59.545901Z"
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum KrakenSubResponse {
    Subscribed { result: KrakenSubResult },
    Error(KrakenError),
}

/// The subscription a successful [`KrakenSubResponse`] confirms.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenSubResult {
    pub channel: String,
    pub symbol: String,
}

impl KrakenSubResponse {
    /// The id of the subscription a confirmation answers: `{channel}|{symbol}`, the id its
    /// subscription was keyed under.
    ///
    /// Kraken echoes the channel and symbol as they were sent. `None` for a failure.
    pub fn subscription_id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Subscribed { result } => {
                Some(ExchangeSub::from((result.channel.as_str(), result.symbol.as_str())).id())
            }
            Self::Error(_) => None,
        }
    }
}

impl Validator for KrakenSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self {
            KrakenSubResponse::Subscribed { .. } => Ok(self),
            KrakenSubResponse::Error(KrakenError {
                message,
                symbol: Some(symbol),
            }) => Err(SocketError::Subscribe(format!(
                "received failure subscription response for {symbol}: {message}"
            ))),
            KrakenSubResponse::Error(KrakenError {
                message,
                symbol: None,
            }) => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {message}"
            ))),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn subscribed(channel: &str, symbol: &str) -> KrakenSubResponse {
        KrakenSubResponse::Subscribed {
            result: KrakenSubResult {
                channel: channel.to_owned(),
                symbol: symbol.to_owned(),
            },
        }
    }

    #[test]
    fn test_kraken_sub_response() {
        let success: KrakenSubResponse = serde_json::from_str(
            r#"{"method":"subscribe","result":{"channel":"ticker","event_trigger":"bbo","snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        )
        .unwrap();
        assert_eq!(success, subscribed("ticker", "BTC/USD"));

        let failure: KrakenSubResponse = serde_json::from_str(
            r#"{"error":"Currency pair not supported XBT/USD","method":"subscribe","success":false,"symbol":"XBT/USD","time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        )
        .unwrap();
        assert_eq!(
            failure,
            KrakenSubResponse::Error(KrakenError {
                message: "Currency pair not supported XBT/USD".to_owned(),
                symbol: Some("XBT/USD".to_owned()),
            })
        );

        let unnamed: KrakenSubResponse = serde_json::from_str(
            r#"{"error":"Subscription name invalid","method":"subscribe","success":false,"time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        )
        .unwrap();
        assert_eq!(
            unnamed,
            KrakenSubResponse::Error(KrakenError {
                message: "Subscription name invalid".to_owned(),
                symbol: None,
            })
        );
    }

    #[test]
    fn messages_that_answer_no_subscription_are_not_responses() {
        for message in [
            r#"{"channel":"heartbeat"}"#,
            r#"{"channel":"status","type":"update","data":[{"api_version":"v2","connection_id":1,"system":"online","version":"2.0.0"}]}"#,
            r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1.0,"qty":1.0,"ord_type":"limit","trade_id":1,"timestamp":"2018-11-12T21:14:59.545897Z"}]}"#,
        ] {
            assert!(
                serde_json::from_str::<KrakenSubResponse>(message).is_err(),
                "{message}"
            );
        }
    }

    #[test]
    fn an_echoed_subscription_names_the_id_it_was_keyed_under() {
        use crate::exchange::{
            Connector,
            kraken::{Kraken, channel::KrakenChannel, market::KrakenMarket},
        };

        /// Echo the subscription the request carries back as Kraken's confirmation, and check it
        /// names the id the mapper keyed the subscription under.
        fn round_trip(exchange_sub: ExchangeSub<KrakenChannel, KrakenMarket>) {
            let keyed = exchange_sub.id();
            let request = Kraken::requests(vec![exchange_sub]).remove(0);
            let request: serde_json::Value =
                serde_json::from_str(request.to_text().unwrap()).unwrap();
            let echoed = serde_json::json!({
                "method": "subscribe",
                "result": {
                    "channel": request["params"]["channel"],
                    "snapshot": false,
                    "symbol": request["params"]["symbol"][0],
                },
                "success": true,
            });

            let response: KrakenSubResponse = serde_json::from_value(echoed).unwrap();
            assert_eq!(response.subscription_id(), Some(keyed));
        }

        round_trip(ExchangeSub {
            channel: KrakenChannel::TRADES,
            market: KrakenMarket("BTC/USD".into()),
        });
        round_trip(ExchangeSub {
            channel: KrakenChannel::ORDER_BOOK_L1,
            market: KrakenMarket("ETH/EUR".into()),
        });
    }

    #[test]
    fn a_failure_names_no_subscription() {
        let failure = KrakenSubResponse::Error(KrakenError {
            message: "Currency pair not supported XBT/USD".to_owned(),
            symbol: Some("XBT/USD".to_owned()),
        });
        assert_eq!(failure.subscription_id(), None);
    }

    #[test]
    fn test_kraken_sub_response_validate() {
        assert!(subscribed("trade", "BTC/USD").validate().is_ok());

        let named = KrakenSubResponse::Error(KrakenError {
            message: "Already subscribed".to_owned(),
            symbol: Some("BTC/USD".to_owned()),
        });
        match named.validate() {
            Err(SocketError::Subscribe(message)) => assert_eq!(
                message,
                "received failure subscription response for BTC/USD: Already subscribed"
            ),
            other => panic!("expected Subscribe, got {other:?}"),
        }

        let unnamed = KrakenSubResponse::Error(KrakenError {
            message: "Subscription name invalid".to_owned(),
            symbol: None,
        });
        match unnamed.validate() {
            Err(SocketError::Subscribe(message)) => assert_eq!(
                message,
                "received failure subscription response: Subscription name invalid"
            ),
            other => panic!("expected Subscribe, got {other:?}"),
        }
    }
}
