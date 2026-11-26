use super::{super::channel::BinanceChannel, BinanceLevel};
use crate::{
    Identifier,
    books::{OrderBook, OrderBookTimes},
    event::MarketEvent,
    exchange::subscription::ExchangeSub,
    subscription::book::OrderBookEvent,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{error::SocketError, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Constructor)]
pub struct BinanceOrderBookL2Meta<InstrumentKey, Sequencer> {
    pub key: InstrumentKey,
    pub sequencer: Sequencer,
}

/// [`Binance`](super::super::Binance) OrderBook Level2 snapshot HTTP message.
///
/// Used as the starting [`OrderBook`] before OrderBook Level2 delta WebSocket updates are
/// applied.
///
/// ### Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#order-book>
/// #### BinanceSpot OrderBookL2Snapshot
/// ```json
/// {
///     "lastUpdateId": 1027024,
///     "bids": [
///         ["4.00000000", "431.00000000"]
///     ],
///     "asks": [
///         ["4.00000200", "12.00000000"]
///     ]
/// }
/// ```
///
/// #### BinanceFuturesUsd OrderBookL2Snapshot
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#order-book>
/// ```json
/// {
///     "lastUpdateId": 1027024,
///     "E": 1589436922972,
///     "T": 1589436922959,
///     "bids": [
///         ["4.00000000", "431.00000000"]
///     ],
///     "asks": [
///         ["4.00000200", "12.00000000"]
///     ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceOrderBookL2Snapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    #[serde(default, rename = "E", with = "chrono::serde::ts_milliseconds_option")]
    pub time_exchange: Option<DateTime<Utc>>,
    #[serde(default, rename = "T", with = "chrono::serde::ts_milliseconds_option")]
    pub time_engine: Option<DateTime<Utc>>,
    pub bids: Vec<BinanceLevel>,
    pub asks: Vec<BinanceLevel>,
}

/// How long fetching one initial [`BinanceOrderBookL2Snapshot`] over HTTP may take, from
/// connecting until the body has arrived, before the stream initialisation fails with
/// [`SocketError::HttpTimeout`].
pub const HTTP_BOOK_L2_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetch one [`BinanceOrderBookL2Snapshot`] from `url`, giving up after `timeout`.
pub(crate) async fn fetch_l2_snapshot(
    url: &str,
    timeout: Duration,
) -> Result<BinanceOrderBookL2Snapshot, SocketError> {
    reqwest::Client::new()
        .get(url)
        .timeout(timeout)
        .send()
        .await?
        .json::<BinanceOrderBookL2Snapshot>()
        .await
        .map_err(SocketError::from)
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceOrderBookL2Snapshot)>
    for MarketEvent<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, snapshot): (ExchangeId, InstrumentKey, BinanceOrderBookL2Snapshot),
    ) -> Self {
        // `time_received` must be shared between the MarketEvent envelope and the
        // OrderBook's `OrderBookTimes`, so the book is built here (the only scope
        // where the binding exists) rather than via a `From<Snapshot>` that has no
        // timestamp argument. Spot `/api/v3/depth` carries neither "E" nor "T"
        // (both `None`) → `time_received` is the only liveness signal until the
        // first diff sets `time_exchange`.
        //
        // Deliberate envelope/book asymmetry: the `MarketEvent.time_exchange`
        // below falls back to `time_received` (its non-`Option` contract always
        // yields a value), whereas the book's `OrderBookTimes.time_exchange` keeps
        // the raw `snapshot.time_exchange` (`None` = "the venue gave nothing").
        let time_received = Utc::now();
        Self {
            time_exchange: snapshot.time_exchange.unwrap_or(time_received),
            time_received,
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(OrderBook::new(
                snapshot.last_update_id,
                OrderBookTimes {
                    time_engine: snapshot.time_engine,
                    time_exchange: snapshot.time_exchange,
                    time_received,
                },
                snapshot.bids,
                snapshot.asks,
            )),
        }
    }
}

/// Deserialize a
/// [`BinanceSpotOrderBookL2Update`](super::super::spot::l2::BinanceSpotOrderBookL2Update) or
/// [`BinanceFuturesOrderBookL2Update`](super::super::futures::l2::BinanceFuturesOrderBookL2Update)
/// "s" field (eg/ "BTCUSDT") as the associated [`SubscriptionId`]
///
/// eg/ "@depth@100ms|BTCUSDT"
pub fn de_ob_l2_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::ORDER_BOOK_L2, market)).id())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod de {
        use super::*;
        use rust_decimal_macros::dec;

        #[test]
        fn test_binance_order_book_l2_snapshot() {
            struct TestCase {
                input: &'static str,
                expected: BinanceOrderBookL2Snapshot,
            }

            let tests = vec![
                TestCase {
                    // TC0: valid Spot BinanceOrderBookL2Snapshot
                    input: r#"
                    {
                        "lastUpdateId": 1027024,
                        "bids": [
                            [
                                "4.00000000",
                                "431.00000000"
                            ]
                        ],
                        "asks": [
                            [
                                "4.00000200",
                                "12.00000000"
                            ]
                        ]
                    }
                    "#,
                    expected: BinanceOrderBookL2Snapshot {
                        last_update_id: 1027024,
                        time_exchange: Default::default(),
                        time_engine: Default::default(),
                        bids: vec![BinanceLevel {
                            price: dec!(4.00000000),
                            amount: dec!(431.00000000),
                        }],
                        asks: vec![BinanceLevel {
                            price: dec!(4.00000200),
                            amount: dec!(12.00000000),
                        }],
                    },
                },
                TestCase {
                    // TC1: valid FuturePerpetual BinanceOrderBookL2Snapshot
                    input: r#"
                    {
                        "lastUpdateId": 1027024,
                        "E": 1589436922972,
                        "T": 1589436922959,
                        "bids": [
                            [
                                "4.00000000",
                                "431.00000000"
                            ]
                        ],
                        "asks": [
                            [
                                "4.00000200",
                                "12.00000000"
                            ]
                        ]
                    }
                    "#,
                    expected: BinanceOrderBookL2Snapshot {
                        last_update_id: 1027024,
                        time_exchange: Some(
                            DateTime::from_timestamp_millis(1589436922972).unwrap(),
                        ),
                        time_engine: Some(DateTime::from_timestamp_millis(1589436922959).unwrap()),
                        bids: vec![BinanceLevel {
                            price: dec!(4.0),
                            amount: dec!(431.0),
                        }],
                        asks: vec![BinanceLevel {
                            price: dec!(4.00000200),
                            amount: dec!(12.0),
                        }],
                    },
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(
                    serde_json::from_str::<BinanceOrderBookL2Snapshot>(test.input).unwrap(),
                    test.expected,
                    "TC{} failed",
                    index
                );
            }
        }
    }

    mod seed {
        use super::*;
        use rust_decimal_macros::dec;

        fn snapshot(
            time_exchange: Option<DateTime<Utc>>,
            time_engine: Option<DateTime<Utc>>,
        ) -> BinanceOrderBookL2Snapshot {
            BinanceOrderBookL2Snapshot {
                last_update_id: 1,
                time_exchange,
                time_engine,
                bids: vec![BinanceLevel {
                    price: dec!(100),
                    amount: dec!(1),
                }],
                asks: vec![],
            }
        }

        #[test]
        fn futures_seed_carries_distinct_times_not_transposed() {
            // Futures REST seed carries both "E" and "T": the second site (with the
            // futures diff) where both are `Some` and differ — guards field ordering.
            let e = DateTime::from_timestamp_millis(1589436922972).unwrap(); // "E"
            let t = DateTime::from_timestamp_millis(1589436922959).unwrap(); // "T"
            assert_ne!(e, t);

            let event: MarketEvent<&str, OrderBookEvent> = (
                ExchangeId::BinanceFuturesUsd,
                "inst",
                snapshot(Some(e), Some(t)),
            )
                .into();
            let OrderBookEvent::Snapshot(book) = event.kind else {
                panic!("expected Snapshot");
            };
            assert_eq!(book.time_exchange(), Some(e), "time_exchange must be \"E\"");
            assert_eq!(book.time_engine(), Some(t), "time_engine must be \"T\"");
            // Book's local ingestion time matches the envelope's.
            assert_eq!(book.time_received(), event.time_received);
        }

        #[test]
        fn spot_seed_has_no_venue_times_only_time_received() {
            // Spot `/api/v3/depth` carries neither "E" nor "T" → both `None`;
            // `time_received` is the only liveness signal until the first diff.
            let event: MarketEvent<&str, OrderBookEvent> =
                (ExchangeId::BinanceSpot, "inst", snapshot(None, None)).into();
            let OrderBookEvent::Snapshot(book) = event.kind else {
                panic!("expected Snapshot");
            };
            assert_eq!(book.time_exchange(), None);
            assert_eq!(book.time_engine(), None);
            assert_eq!(book.time_received(), event.time_received);
        }
    }

    mod fetch_snapshot {
        use super::*;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        const BODY: &str = r#"{"lastUpdateId":7,"bids":[["4.0","43.0"]],"asks":[["4.1","12.0"]]}"#;

        async fn server(delay: Duration) -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/depth"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_raw(BODY, "application/json")
                        .set_delay(delay),
                )
                .mount(&server)
                .await;
            server
        }

        #[tokio::test]
        async fn a_snapshot_slower_than_the_timeout_is_an_http_timeout() {
            let server = server(Duration::from_secs(5)).await;

            let result = fetch_l2_snapshot(
                &format!("{}/depth", server.uri()),
                Duration::from_millis(200),
            )
            .await;

            assert!(
                matches!(result, Err(SocketError::HttpTimeout(_))),
                "{result:?}"
            );
        }

        #[tokio::test]
        async fn a_snapshot_within_the_timeout_is_returned() {
            let server = server(Duration::ZERO).await;

            let snapshot =
                fetch_l2_snapshot(&format!("{}/depth", server.uri()), Duration::from_secs(10))
                    .await
                    .unwrap();

            assert_eq!(snapshot.last_update_id, 7);
        }
    }
}
