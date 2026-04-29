use super::HyperliquidChannel;
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::subscription::SubscriptionId;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};

/// Hyperliquid trades WebSocket message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "trades",
///     "data": [
///         {
///             "coin": "BTC",
///             "side": "A",
///             "px": "45250.5",
///             "sz": "0.5",
///             "hash": "0x...",
///             "time": 1704067200000,
///             "tid": 12345
///         }
///     ]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "camelCase")]
pub enum HyperliquidMessage {
    Trades {
        data: Vec<HyperliquidTradeData>,
    },
    #[serde(other)]
    Other,
}

/// Single trade data from Hyperliquid.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct HyperliquidTradeData {
    pub coin: SmolStr,
    pub side: SmolStr,
    #[serde(deserialize_with = "barter_integration::serde::de::de_str")]
    pub px: f64,
    #[serde(deserialize_with = "barter_integration::serde::de::de_str")]
    pub sz: f64,
    pub time: u64,
    pub tid: u64,
}

/// Wrapper for Hyperliquid trade used by the transformer.
#[derive(Clone, PartialEq, Debug)]
pub struct HyperliquidTrade {
    pub subscription_id: SubscriptionId,
    pub trades: Vec<HyperliquidTradeData>,
}

impl Identifier<Option<SubscriptionId>> for HyperliquidTrade {
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

impl<'de> Deserialize<'de> for HyperliquidTrade {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let message = HyperliquidMessage::deserialize(deserializer)?;

        match message {
            HyperliquidMessage::Trades { data } => {
                if data.is_empty() {
                    return Ok(Self {
                        subscription_id: SubscriptionId::from(""),
                        trades: vec![],
                    });
                }
                let coin = &data[0].coin;
                let subscription_id =
                    ExchangeSub::from((HyperliquidChannel::Trades, coin.as_str())).id();
                Ok(Self {
                    subscription_id,
                    trades: data,
                })
            }
            HyperliquidMessage::Other => Ok(Self {
                subscription_id: SubscriptionId::from(""),
                trades: vec![],
            }),
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, HyperliquidTrade)>
    for MarketIter<InstrumentKey, PublicTrade>
where
    InstrumentKey: Clone,
{
    fn from(
        (exchange_id, instrument, trade_msg): (ExchangeId, InstrumentKey, HyperliquidTrade),
    ) -> Self {
        let time_received = Utc::now();
        let events = trade_msg
            .trades
            .into_iter()
            .map(|trade| {
                // Hyperliquid encodes the aggressor side: "A" = ask-side aggressor (taker
                // sell), "B" = bid-side aggressor (taker buy). Surface unknown values as
                // an error rather than silently misclassifying trade direction.
                let side = match trade.side.as_str() {
                    "A" => Side::Sell,
                    "B" => Side::Buy,
                    other => {
                        return Err(DataError::Socket(format!(
                            "unexpected Hyperliquid trade side: {other}"
                        )));
                    }
                };

                let time_exchange = Utc
                    .timestamp_millis_opt(trade.time as i64)
                    .single()
                    .ok_or_else(|| {
                        DataError::Socket(format!(
                            "Hyperliquid trade timestamp {} out of range",
                            trade.time
                        ))
                    })?;

                Ok(MarketEvent {
                    time_exchange,
                    time_received,
                    exchange: exchange_id,
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: format_smolstr!("{}", trade.tid),
                        price: trade.px,
                        amount: trade.sz,
                        side,
                    },
                })
            })
            .collect();

        Self(events)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_de_hyperliquid_trade_message() {
        let input = r#"
        {
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "side": "A",
                    "px": "45250.5",
                    "sz": "0.5",
                    "time": 1704067200000,
                    "hash": "0xabc123",
                    "tid": 12345
                }
            ]
        }
        "#;

        let trade: HyperliquidTrade = serde_json::from_str(input).unwrap();
        assert_eq!(trade.trades.len(), 1);
        assert_eq!(trade.trades[0].coin, "BTC");
        assert_eq!(trade.trades[0].px, 45250.5);
        assert_eq!(trade.trades[0].sz, 0.5);
        assert_eq!(trade.trades[0].side, "A");
    }

    #[test]
    fn test_de_hyperliquid_trade_multiple() {
        let input = r#"
        {
            "channel": "trades",
            "data": [
                {"coin": "ETH", "side": "B", "px": "3000.0", "sz": "1.0", "time": 1704067200000, "hash": "0x1", "tid": 1},
                {"coin": "ETH", "side": "A", "px": "3001.0", "sz": "2.0", "time": 1704067200001, "hash": "0x2", "tid": 2}
            ]
        }
        "#;

        let trade: HyperliquidTrade = serde_json::from_str(input).unwrap();
        assert_eq!(trade.trades.len(), 2);
    }

    fn make_trade(side: &str) -> HyperliquidTradeData {
        HyperliquidTradeData {
            coin: SmolStr::new_static("BTC"),
            side: SmolStr::new(side),
            px: 100.0,
            sz: 1.0,
            time: 1704067200000,
            tid: 1,
        }
    }

    #[test]
    fn test_hyperliquid_side_mapping_sell() {
        let hl_trade = HyperliquidTrade {
            subscription_id: SubscriptionId::from("trades|BTC"),
            trades: vec![make_trade("A")],
        };

        let market_iter: MarketIter<&str, PublicTrade> =
            (ExchangeId::HyperliquidPerp, "BTC", hl_trade).into();
        let event = market_iter.0[0].as_ref().unwrap();
        assert_eq!(event.kind.side, Side::Sell);
    }

    #[test]
    fn test_hyperliquid_side_mapping_buy() {
        let hl_trade = HyperliquidTrade {
            subscription_id: SubscriptionId::from("trades|BTC"),
            trades: vec![make_trade("B")],
        };

        let market_iter: MarketIter<&str, PublicTrade> =
            (ExchangeId::HyperliquidPerp, "BTC", hl_trade).into();
        let event = market_iter.0[0].as_ref().unwrap();
        assert_eq!(event.kind.side, Side::Buy);
    }

    #[test]
    fn test_hyperliquid_side_mapping_unknown_errors() {
        let hl_trade = HyperliquidTrade {
            subscription_id: SubscriptionId::from("trades|BTC"),
            trades: vec![make_trade("X")],
        };

        let market_iter: MarketIter<&str, PublicTrade> =
            (ExchangeId::HyperliquidPerp, "BTC", hl_trade).into();
        assert!(matches!(market_iter.0[0], Err(DataError::Socket(_))));
    }

    #[test]
    fn test_de_hyperliquid_trade_empty_data() {
        let input = r#"{"channel": "trades", "data": []}"#;
        let trade: HyperliquidTrade = serde_json::from_str(input).unwrap();
        assert!(trade.trades.is_empty());
        assert!(trade.id().is_none());
    }
}
