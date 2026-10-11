use super::{
    KrakenMessage,
    channel::KrakenChannel,
    message::{KrakenChannelData, KrakenData},
};
use crate::{
    event::{MarketEvent, MarketIter},
    subscription::trade::PublicTrade,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};

/// Terse type alias for an [`Kraken`](super::Kraken) real-time trades WebSocket message.
pub type KrakenTrades = KrakenMessage<KrakenData<KrakenTrade>>;

/// [`Kraken`](super::Kraken) trade.
///
/// See [`KrakenMessage`] for full raw payload examples.
///
/// See docs: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade>
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct KrakenTrade {
    pub symbol: SmolStr,
    pub side: Side,
    #[serde(deserialize_with = "rust_decimal::serde::float::deserialize")]
    pub price: Decimal,
    #[serde(
        rename = "qty",
        deserialize_with = "rust_decimal::serde::float::deserialize"
    )]
    pub amount: Decimal,
    pub trade_id: u64,
    pub timestamp: DateTime<Utc>,
}

impl KrakenChannelData for KrakenTrade {
    const CHANNEL: KrakenChannel = KrakenChannel::TRADES;

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, KrakenTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, KrakenTrades)) -> Self {
        match trades {
            KrakenTrades::Data(trades) => {
                let time_received = Utc::now();
                trades
                    .data
                    .into_iter()
                    .map(|trade| {
                        Ok(MarketEvent {
                            time_exchange: trade.timestamp,
                            time_received,
                            exchange,
                            instrument: instrument.clone(),
                            kind: PublicTrade {
                                id: format_smolstr!("{}", trade.trade_id),
                                price: trade.price,
                                amount: trade.amount,
                                side: Some(trade.side),
                            },
                        })
                    })
                    .collect()
            }
            KrakenTrades::Event(_) => Self(vec![]),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::Identifier;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use rustrade_integration::subscription::SubscriptionId;

    fn trade(symbol: &str, side: &str, price: &str, qty: &str, trade_id: u64) -> String {
        format!(
            r#"{{"symbol":"{symbol}","side":"{side}","price":{price},"qty":{qty},"ord_type":"limit","trade_id":{trade_id},"timestamp":"2018-08-18T17:40:57.321597Z"}}"#
        )
    }

    fn message(trades: &[String]) -> String {
        format!(
            r#"{{"channel":"trade","type":"update","data":[{}]}}"#,
            trades.join(",")
        )
    }

    #[test]
    fn test_kraken_message_trades() {
        let input = message(&[
            trade("BTC/USD", "sell", "5541.2", "0.15850568", 1001),
            trade("BTC/USD", "buy", "6060", "5.006e-05", 1002),
        ]);

        let actual: KrakenTrades = serde_json::from_str(&input).unwrap();

        let time = Utc
            .with_ymd_and_hms(2018, 8, 18, 17, 40, 57)
            .unwrap()
            .checked_add_signed(chrono::Duration::microseconds(321_597))
            .unwrap();
        assert_eq!(
            actual,
            KrakenTrades::Data(KrakenData {
                subscription_id: Some(SubscriptionId::from("trade|BTC/USD")),
                data: vec![
                    KrakenTrade {
                        symbol: "BTC/USD".into(),
                        side: Side::Sell,
                        price: dec!(5541.2),
                        amount: dec!(0.15850568),
                        trade_id: 1001,
                        timestamp: time,
                    },
                    KrakenTrade {
                        symbol: "BTC/USD".into(),
                        side: Side::Buy,
                        price: dec!(6060),
                        amount: dec!(0.00005006),
                        trade_id: 1002,
                        timestamp: time,
                    },
                ],
            })
        );
    }

    #[test]
    fn trades_become_public_trades_keyed_by_kraken_trade_id() {
        let input = message(&[trade("ETH/USD", "sell", "2509.41", "0.02625345", 67552113)]);
        let trades: KrakenTrades = serde_json::from_str(&input).unwrap();

        let events = MarketIter::<&str, PublicTrade>::from((ExchangeId::Kraken, "eth", trades)).0;

        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.instrument, "eth");
        assert_eq!(
            event.kind,
            PublicTrade {
                id: "67552113".into(),
                price: dec!(2509.41),
                amount: dec!(0.02625345),
                side: Some(Side::Sell),
            }
        );
    }

    #[test]
    fn a_message_without_trades_routes_nowhere() {
        let trades: KrakenTrades = serde_json::from_str(&message(&[])).unwrap();
        assert_eq!(trades.id(), None);
    }

    #[test]
    fn messages_that_carry_no_trades_route_nowhere() {
        for message in [
            r#"{"channel":"heartbeat"}"#,
            r#"{"channel":"status","type":"update","data":[{"api_version":"v2","connection_id":1,"system":"online","version":"2.0.0"}]}"#,
            r#"{"method":"pong","time_in":"2018-11-12T21:14:59.545897Z","time_out":"2018-11-12T21:14:59.545901Z"}"#,
        ] {
            let trades: KrakenTrades = serde_json::from_str(message).unwrap();
            assert!(matches!(trades, KrakenMessage::Event(_)), "{message}");
            assert_eq!(trades.id(), None, "{message}");
        }
    }

    #[test]
    fn a_message_mixing_symbols_is_refused() {
        let input = message(&[
            trade("BTC/USD", "buy", "1", "1", 1),
            trade("ETH/USD", "buy", "1", "1", 2),
        ]);
        assert!(serde_json::from_str::<KrakenTrades>(&input).is_err());
    }

    #[test]
    fn a_ticker_message_is_not_trades() {
        let input = r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":1.0,"bid_qty":1.0,"ask":2.0,"ask_qty":1.0,"timestamp":"2018-08-18T17:40:57.321597Z"}]}"#;
        assert!(serde_json::from_str::<KrakenTrades>(input).is_err());
    }
}
