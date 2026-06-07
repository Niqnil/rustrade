use crate::{
    books::{Level, OrderBook, OrderBookTimes},
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookEvent,
};
use chrono::Utc;
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};

use super::message::{BybitPayload, BybitPayloadKind};

/// Level 1 OrderBook types.
pub mod l1;

/// Level 2 OrderBook types.
pub mod l2;

/// Terse type alias for an [`BybitOrderBookMessage`] OrderBook WebSocket message.
pub type BybitOrderBookMessage = BybitPayload<BybitOrderBookInner>;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct BybitOrderBookInner {
    #[serde(rename = "b")]
    pub bids: Vec<BybitLevel>,

    #[serde(rename = "a")]
    pub asks: Vec<BybitLevel>,

    #[serde(rename = "u")]
    pub update_id: u64,

    #[serde(rename = "seq")]
    pub sequence: u64,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BybitOrderBookMessage)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, BybitOrderBookMessage),
    ) -> Self {
        let time_received = Utc::now();
        // Bybit's `ts` broadcast time feeds both the book's `time_exchange` and the
        // `MarketEvent` envelope's `time_exchange` below — bind once so the shared
        // source is explicit.
        let time_exchange = message.time;
        let orderbook = OrderBook::new(
            message.data.sequence,
            // `time_exchange` is Bybit's `ts` broadcast time → `time_exchange` only.
            // Bybit sends no matching-engine time, so `time_engine` is `None`.
            OrderBookTimes {
                time_engine: None,
                time_exchange: Some(time_exchange),
                time_received,
            },
            message.data.bids,
            message.data.asks,
        );

        let kind = match message.kind {
            BybitPayloadKind::Snapshot => OrderBookEvent::Snapshot(orderbook),
            BybitPayloadKind::Delta => OrderBookEvent::Update(orderbook),
        };

        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received,
            exchange,
            instrument,
            kind,
        })])
    }
}

/// [`Bybit`](super::Bybit) OrderBook level.
///
/// #### Raw Payload Examples
/// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook#response-parameters>
///
/// ```json
/// ["16493.50", "0.006"]
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BybitLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub amount: Decimal,
}

impl From<BybitLevel> for Level {
    fn from(level: BybitLevel) -> Self {
        Self {
            price: level.price,
            amount: level.amount,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod de {
        use super::*;
        use rust_decimal_macros::dec;

        #[test]
        fn test_bybit_level() {
            let input = r#"["16493.50", "0.006"]"#;
            assert_eq!(
                serde_json::from_str::<BybitLevel>(input).unwrap(),
                BybitLevel {
                    price: dec!(16493.50),
                    amount: dec!(0.006)
                },
            )
        }
    }
}
