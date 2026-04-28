use super::HyperliquidChannel;
use crate::{
    Identifier,
    books::{Level, OrderBook},
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::OrderBookEvent,
};
use barter_integration::{serde::de::de_str, subscription::SubscriptionId};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Hyperliquid L2 order book WebSocket message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "l2Book",
///     "data": {
///         "coin": "BTC",
///         "time": 1704067200000,
///         "levels": [
///             [{"px": "45000.0", "sz": "1.5", "n": 3}],
///             [{"px": "45100.0", "sz": "2.0", "n": 5}]
///         ]
///     }
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "camelCase")]
pub enum HyperliquidBookMessage {
    L2Book {
        data: HyperliquidL2BookData,
    },
    #[serde(other)]
    Other,
}

/// L2 order book data from Hyperliquid.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct HyperliquidL2BookData {
    pub coin: SmolStr,
    pub time: u64,
    pub levels: Vec<Vec<HyperliquidBookLevel>>,
}

/// Single price level in the order book.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct HyperliquidBookLevel {
    #[serde(deserialize_with = "de_str")]
    pub px: Decimal,
    #[serde(deserialize_with = "de_str")]
    pub sz: Decimal,
    pub n: u64,
}

/// Wrapper for Hyperliquid L2 book used by the transformer.
#[derive(Clone, PartialEq, Debug)]
pub struct HyperliquidL2Book {
    pub subscription_id: SubscriptionId,
    pub data: Option<HyperliquidL2BookData>,
}

impl Identifier<Option<SubscriptionId>> for HyperliquidL2Book {
    fn id(&self) -> Option<SubscriptionId> {
        // Pong/unknown channel messages deserialize with an empty subscription_id;
        // returning None lets the StatelessTransformer drop them silently instead of
        // emitting a SocketError::Unidentifiable("") on every keepalive cycle.
        if self.subscription_id.as_ref().is_empty() {
            None
        } else {
            Some(self.subscription_id.clone())
        }
    }
}

impl<'de> Deserialize<'de> for HyperliquidL2Book {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let message = HyperliquidBookMessage::deserialize(deserializer)?;

        match message {
            HyperliquidBookMessage::L2Book { data } => {
                let subscription_id =
                    ExchangeSub::from((HyperliquidChannel::L2Book, data.coin.as_str())).id();
                Ok(Self {
                    subscription_id,
                    data: Some(data),
                })
            }
            HyperliquidBookMessage::Other => Ok(Self {
                subscription_id: SubscriptionId::from(""),
                data: None,
            }),
        }
    }
}

impl<InstrumentKey>
    From<(
        barter_instrument::exchange::ExchangeId,
        InstrumentKey,
        HyperliquidL2Book,
    )> for MarketIter<InstrumentKey, OrderBookEvent>
where
    InstrumentKey: Clone,
{
    fn from(
        (exchange_id, instrument, book_msg): (
            barter_instrument::exchange::ExchangeId,
            InstrumentKey,
            HyperliquidL2Book,
        ),
    ) -> Self {
        let Some(data) = book_msg.data else {
            return Self(vec![]);
        };

        let time_exchange = match Utc.timestamp_millis_opt(data.time as i64).single() {
            Some(time) => time,
            None => {
                return Self(vec![Err(DataError::Socket(format!(
                    "Hyperliquid book timestamp {} out of range",
                    data.time
                )))]);
            }
        };

        let (bids, asks) = if data.levels.len() >= 2 {
            let bids = data.levels[0].iter().map(level_from).collect::<Vec<_>>();
            let asks = data.levels[1].iter().map(level_from).collect::<Vec<_>>();
            (bids, asks)
        } else {
            (vec![], vec![])
        };

        // Hyperliquid L2 book messages carry no sequence number; use the message
        // timestamp (millis) as a monotonic proxy for snapshot ordering.
        let order_book = OrderBook::new(data.time, Some(time_exchange), bids, asks);

        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: OrderBookEvent::Snapshot(order_book),
        })])
    }
}

fn level_from(level: &HyperliquidBookLevel) -> Level {
    Level::new(level.px, level.sz)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use barter_instrument::exchange::ExchangeId;
    use rust_decimal_macros::dec;

    #[test]
    fn test_de_hyperliquid_l2_book() {
        let input = r#"
        {
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1704067200000,
                "levels": [
                    [{"px": "45000.0", "sz": "1.5", "n": 3}, {"px": "44900.0", "sz": "2.0", "n": 5}],
                    [{"px": "45100.0", "sz": "1.0", "n": 2}, {"px": "45200.0", "sz": "3.0", "n": 4}]
                ]
            }
        }
        "#;

        let book: HyperliquidL2Book = serde_json::from_str(input).unwrap();
        assert!(book.data.is_some());
        let data = book.data.unwrap();
        assert_eq!(data.coin, "BTC");
        assert_eq!(data.levels.len(), 2);
        assert_eq!(data.levels[0].len(), 2);
        assert_eq!(data.levels[1].len(), 2);
    }

    #[test]
    fn test_l2_book_to_market_event() {
        let book = HyperliquidL2Book {
            subscription_id: SubscriptionId::from("l2Book|BTC"),
            data: Some(HyperliquidL2BookData {
                coin: SmolStr::new_static("BTC"),
                time: 1704067200000,
                levels: vec![
                    vec![HyperliquidBookLevel {
                        px: dec!(45000.0),
                        sz: dec!(1.5),
                        n: 3,
                    }],
                    vec![HyperliquidBookLevel {
                        px: dec!(45100.0),
                        sz: dec!(2.0),
                        n: 5,
                    }],
                ],
            }),
        };

        let market_iter: MarketIter<&str, OrderBookEvent> =
            (ExchangeId::HyperliquidPerp, "BTC", book).into();
        let events: Vec<_> = market_iter.0;
        assert_eq!(events.len(), 1);

        let event = events[0].as_ref().unwrap();
        match &event.kind {
            OrderBookEvent::Snapshot(order_book) => {
                assert_eq!(order_book.bids().levels().len(), 1);
                assert_eq!(order_book.asks().levels().len(), 1);
                assert_eq!(order_book.bids().best().unwrap().price, dec!(45000.0));
                assert_eq!(order_book.asks().best().unwrap().price, dec!(45100.0));
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn test_level_from() {
        let level = HyperliquidBookLevel {
            px: dec!(45000.5),
            sz: dec!(1.25),
            n: 3,
        };

        let parsed = level_from(&level);
        assert_eq!(parsed.price, dec!(45000.5));
        assert_eq!(parsed.amount, dec!(1.25));
    }
}
