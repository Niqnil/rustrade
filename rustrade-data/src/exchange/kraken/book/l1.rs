use super::super::{
    KrakenMessage,
    channel::KrakenChannel,
    message::{KrakenChannelData, KrakenData},
};
use crate::{
    books::Level,
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookL1,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Terse type alias for an [`Kraken`](super::super::Kraken) real-time OrderBook Level1
/// (top of books) WebSocket message.
pub type KrakenOrderBookL1 = KrakenMessage<KrakenData<KrakenTicker>>;

/// [`Kraken`](super::super::Kraken) best bid and ask, from the ticker channel.
///
/// The other ticker fields (last trade, volume, daily range) are not read.
///
/// See [`KrakenMessage`] for full raw payload examples.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker>
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct KrakenTicker {
    pub symbol: SmolStr,
    #[serde(
        rename = "bid",
        deserialize_with = "rust_decimal::serde::float::deserialize"
    )]
    pub best_bid_price: Decimal,
    #[serde(
        rename = "bid_qty",
        deserialize_with = "rust_decimal::serde::float::deserialize"
    )]
    pub best_bid_amount: Decimal,
    #[serde(
        rename = "ask",
        deserialize_with = "rust_decimal::serde::float::deserialize"
    )]
    pub best_ask_price: Decimal,
    #[serde(
        rename = "ask_qty",
        deserialize_with = "rust_decimal::serde::float::deserialize"
    )]
    pub best_ask_amount: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl KrakenChannelData for KrakenTicker {
    const CHANNEL: KrakenChannel = KrakenChannel::ORDER_BOOK_L1;

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, KrakenOrderBookL1)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from(
        (exchange_id, instrument, book): (ExchangeId, InstrumentKey, KrakenOrderBookL1),
    ) -> Self {
        match book {
            KrakenOrderBookL1::Data(book) => {
                let time_received = Utc::now();
                book.data
                    .into_iter()
                    .map(|ticker| {
                        // A side with no orders is sent as a zero price
                        let level = |price: Decimal, amount| {
                            (!price.is_zero()).then(|| Level::new(price, amount))
                        };

                        Ok(MarketEvent {
                            time_exchange: ticker.timestamp,
                            time_received,
                            exchange: exchange_id,
                            instrument: instrument.clone(),
                            kind: OrderBookL1 {
                                last_update_time: ticker.timestamp,
                                best_bid: level(ticker.best_bid_price, ticker.best_bid_amount),
                                best_ask: level(ticker.best_ask_price, ticker.best_ask_amount),
                            },
                        })
                    })
                    .collect()
            }
            KrakenOrderBookL1::Event(_) => MarketIter(vec![]),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use rustrade_integration::subscription::SubscriptionId;

    const TICKER: &str = r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":5698.4,"bid_qty":1.01234567,"ask":5700.0,"ask_qty":0.98765432,"last":5699.1,"volume":1234.5,"vwap":5650.2,"low":5600.0,"high":5710.0,"change":12.3,"change_pct":0.22,"timestamp":"2018-11-12T21:14:59.545897Z"}]}"#;

    fn time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2018, 11, 12, 21, 14, 59)
            .unwrap()
            .checked_add_signed(chrono::Duration::microseconds(545_897))
            .unwrap()
    }

    #[test]
    fn test_kraken_message_order_book_l1() {
        let actual: KrakenOrderBookL1 = serde_json::from_str(TICKER).unwrap();

        assert_eq!(
            actual,
            KrakenOrderBookL1::Data(KrakenData {
                subscription_id: Some(SubscriptionId::from("ticker|BTC/USD")),
                data: vec![KrakenTicker {
                    symbol: "BTC/USD".into(),
                    best_bid_price: dec!(5698.4),
                    best_bid_amount: dec!(1.01234567),
                    best_ask_price: dec!(5700),
                    best_ask_amount: dec!(0.98765432),
                    timestamp: time(),
                }],
            })
        );
    }

    #[test]
    fn a_ticker_becomes_the_top_of_book() {
        let book: KrakenOrderBookL1 = serde_json::from_str(TICKER).unwrap();

        let events = MarketIter::<&str, OrderBookL1>::from((ExchangeId::Kraken, "btc", book)).0;

        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.time_exchange, time());
        assert_eq!(
            event.kind,
            OrderBookL1 {
                last_update_time: time(),
                best_bid: Some(Level::new(dec!(5698.4), dec!(1.01234567))),
                best_ask: Some(Level::new(dec!(5700), dec!(0.98765432))),
            }
        );
    }

    #[test]
    fn a_zero_price_is_an_empty_side() {
        let input = TICKER.replace(r#""bid":5698.4"#, r#""bid":0"#);
        let book: KrakenOrderBookL1 = serde_json::from_str(&input).unwrap();

        let events = MarketIter::<&str, OrderBookL1>::from((ExchangeId::Kraken, "btc", book)).0;

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.best_bid, None);
        assert!(event.kind.best_ask.is_some());
    }
}
