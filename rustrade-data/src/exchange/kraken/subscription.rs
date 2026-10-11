use super::message::KrakenError;
use rustrade_integration::{Validator, error::SocketError, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;

/// [`Kraken`](super::Kraken) message received in response to WebSocket subscription requests.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/websockets/#message-subscriptionStatus>
/// #### Subscription Trade Success
/// ```json
/// {
///   "channelID": 10001,
///   "channelName": "ticker",
///   "event": "subscriptionStatus",
///   "pair": "XBT/EUR",
///   "status": "subscribed",
///   "subscription": {
///     "name": "ticker"
///   }
/// }
/// ```
///
/// #### Subscription Trade Failure
/// ```json
/// {
///   "errorMessage": "Subscription name invalid",
///   "event": "subscriptionStatus",
///   "pair": "XBT/USD",
///   "status": "error",
///   "subscription": {
///     "name": "trades"
///   }
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum KrakenSubResponse {
    Subscribed {
        #[serde(alias = "channelID")]
        channel_id: u64,
        #[serde(alias = "channelName")]
        channel_name: String,
        pair: String,
    },
    Error(KrakenError),
}

impl KrakenSubResponse {
    /// The id of the subscription a confirmation answers: `{channelName}|{pair}`, the id its
    /// subscription was keyed under.
    ///
    /// Kraken echoes the pair in its own spelling, `XBT/USD` for one subscribed as `BTC/USD`, so a
    /// subscription keyed under another spelling is not matched. `None` for a failure, which names
    /// no channel.
    pub fn subscription_id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Subscribed {
                channel_name, pair, ..
            } => Some(SubscriptionId(format_smolstr!("{channel_name}|{pair}"))),
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
            KrakenSubResponse::Error(error) => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {}",
                error.message
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_kraken_sub_response() {
            struct TestCase {
                input: &'static str,
                expected: Result<KrakenSubResponse, SocketError>,
            }

            let cases = vec![
                TestCase {
                    // TC0: input response is Subscribed
                    input: r#"
                    {
                        "channelID": 10001,
                        "channelName": "ticker",
                        "event": "subscriptionStatus",
                        "pair": "XBT/EUR",
                        "status": "subscribed",
                        "subscription": {
                            "name": "ticker"
                        }
                    }
                    "#,
                    expected: Ok(KrakenSubResponse::Subscribed {
                        channel_id: 10001,
                        channel_name: "ticker".to_string(),
                        pair: "XBT/EUR".to_string(),
                    }),
                },
                TestCase {
                    // TC1: input response is failed subscription
                    input: r#"
                    {
                        "errorMessage": "Subscription name invalid",
                        "event": "subscriptionStatus",
                        "pair": "XBT/USD",
                        "status": "error",
                        "subscription": {
                            "name": "trades"
                        }
                    }
                    "#,
                    expected: Ok(KrakenSubResponse::Error(KrakenError {
                        message: "Subscription name invalid".to_string(),
                    })),
                },
            ];

            for (index, test) in cases.into_iter().enumerate() {
                let actual = serde_json::from_str::<KrakenSubResponse>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!(
                            "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn an_echoed_subscription_names_the_id_it_was_keyed_under() {
        use crate::{
            Identifier,
            exchange::{
                Connector, ExchangeSub,
                kraken::{Kraken, channel::KrakenChannel, market::KrakenMarket},
            },
        };

        /// Echo the subscription the request carries back as Kraken's confirmation, and check it
        /// names the id the mapper keyed the subscription under.
        fn round_trip(exchange_sub: ExchangeSub<KrakenChannel, KrakenMarket>) {
            let keyed = exchange_sub.id();
            let request = Kraken::requests(vec![exchange_sub]).remove(0);
            let request: serde_json::Value =
                serde_json::from_str(request.to_text().unwrap()).unwrap();
            let echoed = serde_json::json!({
                "channelID": 10001,
                "channelName": request["subscription"]["name"],
                "event": "subscriptionStatus",
                "pair": request["pair"][0],
                "status": "subscribed",
                "subscription": request["subscription"],
            });

            let response: KrakenSubResponse = serde_json::from_value(echoed).unwrap();
            assert_eq!(response.subscription_id(), Some(keyed));
        }

        round_trip(ExchangeSub {
            channel: KrakenChannel::TRADES,
            market: KrakenMarket("XBT/USD".into()),
        });
        round_trip(ExchangeSub {
            channel: KrakenChannel::ORDER_BOOK_L1,
            market: KrakenMarket("ETH/EUR".into()),
        });
    }

    #[test]
    fn a_failure_names_no_subscription() {
        let failure = KrakenSubResponse::Error(KrakenError {
            message: "Currency pair not supported XBT/USX".to_string(),
        });
        assert_eq!(failure.subscription_id(), None);
    }

    #[test]
    fn test_kraken_sub_response_validate() {
        struct TestCase {
            input_response: KrakenSubResponse,
            is_valid: bool,
        }

        let cases = vec![
            TestCase {
                // TC0: input response is successful subscription
                input_response: KrakenSubResponse::Subscribed {
                    channel_id: 10001,
                    channel_name: "ticker".to_string(),
                    pair: "XBT/EUR".to_string(),
                },
                is_valid: true,
            },
            TestCase {
                // TC1: input response is failed subscription
                input_response: KrakenSubResponse::Error(KrakenError {
                    message: "Subscription name invalid".to_string(),
                }),
                is_valid: false,
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = test.input_response.validate().is_ok();
            assert_eq!(actual, test.is_valid, "TestCase {} failed", index);
        }
    }
}
