use super::super::message::GateioMessage;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId};
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::format_smolstr;

/// Deserialize a signed integer (i64) as Decimal.
/// Gate.io Futures sends `size` as a bare JSON integer (e.g., -108 for sells).
fn de_i64_as_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    i64::deserialize(deserializer).map(Decimal::from)
}

/// Terse type alias for a `GateioFuturesUsdt`, `GateioFuturesBtc`, `GateioPerpetualUsdt` and
/// `GateioPerpetualBtc` real-time trades WebSocket message.
pub type GateioFuturesTrades = GateioMessage<Vec<GateioFuturesTradeInner>>;

/// `GateioFuturesUsdt`, `GateioFuturesBtc`, `GateioPerpetualUsdt` and `GateioPerpetualBtc`
/// real-time trade WebSocket message.
///
/// ### Raw Payload Examples
/// #### Future Sell Trade
/// See docs: <https://www.gate.io/docs/developers/delivery/ws/en/#trades-notification>
/// ```json
/// {
///   "id": 27753479,
///   "create_time": 1545136464,
///   "create_time_ms": 1545136464123,
///   "price": "96.4",
///   "size": -108,
///   "contract": "ETH_USDT_QUARTERLY_20201225"
/// }
/// ```
///
/// #### Future Perpetual Sell Trade
/// See docs: <https://www.gate.io/docs/developers/futures/ws/en/#trades-api>
/// ```json
/// {
///   "id": 27753479,
///   "create_time": 1545136464,
///   "create_time_ms": 1545136464123,
///   "price": "96.4",
///   "size": -108,
///   "contract": "BTC_USD"
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct GateioFuturesTradeInner {
    #[serde(rename = "contract")]
    pub market: String,
    #[serde(
        rename = "create_time_ms",
        deserialize_with = "rustrade_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    pub id: u64,
    #[serde(deserialize_with = "rustrade_integration::serde::de::de_str")]
    pub price: Decimal,
    #[serde(rename = "size", deserialize_with = "de_i64_as_decimal")]
    pub amount: Decimal,
}

impl Identifier<Option<SubscriptionId>> for GateioFuturesTrades {
    fn id(&self) -> Option<SubscriptionId> {
        self.data
            .first()
            .map(|trade| ExchangeSub::from((&self.channel, &trade.market)).id())
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, GateioFuturesTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from(
        (exchange, instrument, trades): (ExchangeId, InstrumentKey, GateioFuturesTrades),
    ) -> Self {
        trades
            .data
            .into_iter()
            .map(|trade| {
                Ok(MarketEvent {
                    time_exchange: trade.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: format_smolstr!("{}", trade.id),
                        price: trade.price,
                        amount: trade.amount.abs(),
                        side: Some(if trade.amount.is_sign_positive() {
                            Side::Buy
                        } else {
                            Side::Sell
                        }),
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_gateio_message_perpetual_trade() {
            let input = "{\"time\":1669843487,\"time_ms\":1669843487733,\"channel\":\"perpetual.trades\",\"event\":\"update\",\"result\":[{\"contract\":\"ETH_USDT\",\"create_time\":1669843487,\"create_time_ms\":1669843487724,\"id\":180276616,\"price\":\"1287\",\"size\":3}]}";
            serde_json::from_str::<GateioFuturesTrades>(input).unwrap();
        }

        #[test]
        fn test_gateio_message_futures_trade() {
            let input = r#"
            {
              "channel": "futures.trades",
              "event": "update",
              "time": 1541503698,
              "result": [
                {
                  "size": -108,
                  "id": 27753479,
                  "create_time": 1545136464,
                  "create_time_ms": 1545136464123,
                  "price": "96.4",
                  "contract": "ETH_USDT_QUARTERLY_20201225"
                }
              ]
            }"#;

            serde_json::from_str::<GateioFuturesTrades>(input).unwrap();
        }
    }
}
