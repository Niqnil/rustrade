use super::channel::AlpacaChannel;
use crate::{
    Identifier,
    error::DataError,
    event::MarketEvent,
    exchange::ExchangeSub,
    subscription::{Map, trade::PublicTrade},
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustrade_instrument::{Side, exchange::ExchangeId};
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::{Deserialize, Deserializer};
use smol_str::{SmolStr, format_smolstr};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// Deserialize "S" (symbol) field as the associated [`SubscriptionId`] for trades.
fn de_trade_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((AlpacaChannel::Trades, symbol)).id())
}

/// Alpaca trade message.
///
/// Unified struct for both equities and crypto trades with optional fields.
///
/// ### Equities Example (IEX/SIP)
/// ```json
/// {"T":"t","S":"AAPL","i":123,"x":"V","p":150.25,"s":100,"c":["@"],"z":"C","t":"2026-05-02T14:00:00Z"}
/// ```
///
/// ### Crypto Example
/// ```json
/// {"T":"t","S":"BTC/USD","i":456,"p":60000.50,"s":0.5,"tks":"B","t":"2026-05-02T14:00:00Z"}
/// ```
#[derive(Debug, Deserialize)]
pub struct AlpacaTrade {
    /// Subscription ID constructed from symbol during deserialization.
    /// Avoids per-event `format!` allocation in hot path.
    #[serde(rename = "S", deserialize_with = "de_trade_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(rename = "i")]
    pub id: u64,
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: f64,
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "x", default)]
    pub exchange: Option<SmolStr>,
    #[serde(rename = "z", default)]
    pub tape: Option<SmolStr>,
    #[serde(rename = "tks", default)]
    pub taker_side: Option<SmolStr>,
}

impl AlpacaTrade {
    /// Returns the taker side for crypto trades, or `Side::Buy` as a sentinel for equities.
    ///
    /// # Note
    /// Alpaca equities (IEX/SIP) do not provide taker side information — the `tks` field
    /// is only present on crypto trades. For equities, this returns `Side::Buy` as a
    /// placeholder. Downstream consumers should not rely on `side` for equity trades.
    // FIXME: Migrate PublicTrade::side to Option<Side> to properly represent this
    fn side(&self) -> Side {
        match self.taker_side.as_deref() {
            Some("B") => Side::Buy,
            Some("S") => Side::Sell,
            _ => Side::Buy,
        }
    }
}

/// Alpaca WebSocket message wrapper.
///
/// Alpaca sends all messages as JSON arrays: `[{"T":"t",...},{"T":"t",...}]`.
/// This wrapper deserializes the array and extracts trade messages.
#[derive(Debug)]
pub struct AlpacaTradeMessage(Vec<AlpacaTrade>);

/// Internal enum for single-pass deserialization of Alpaca array messages.
/// Uses `#[serde(tag = "T")]` to dispatch on message type in one parse pass,
/// avoiding the intermediate `Vec<serde_json::Value>` allocation.
#[derive(Deserialize)]
#[serde(tag = "T")]
enum AlpacaArrayTradeMsg {
    #[serde(rename = "t")]
    Trade(AlpacaTrade),
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for AlpacaTradeMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let messages = Vec::<AlpacaArrayTradeMsg>::deserialize(deserializer)?;
        let mut trades = Vec::with_capacity(messages.len());
        for msg in messages {
            if let AlpacaArrayTradeMsg::Trade(trade) = msg {
                trades.push(trade);
            }
        }
        Ok(AlpacaTradeMessage(trades))
    }
}

/// Custom transformer for Alpaca trade messages.
///
/// Handles array-wrapped messages and processes each trade individually,
/// looking up the correct instrument for each symbol.
#[derive(Debug)]
pub struct AlpacaTradeTransformer<Exchange, InstrumentKey> {
    instrument_map: Map<InstrumentKey>,
    exchange_id: ExchangeId,
    phantom: PhantomData<Exchange>,
}

#[async_trait]
impl<Exchange, InstrumentKey>
    ExchangeTransformer<Exchange, InstrumentKey, crate::subscription::trade::PublicTrades>
    for AlpacaTradeTransformer<Exchange, InstrumentKey>
where
    Exchange: crate::exchange::Connector + Send,
    InstrumentKey: Clone + Send,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, PublicTrade>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            instrument_map,
            exchange_id: Exchange::ID,
            phantom: PhantomData,
        })
    }
}

impl<Exchange, InstrumentKey> Transformer for AlpacaTradeTransformer<Exchange, InstrumentKey>
where
    Exchange: crate::exchange::Connector,
    InstrumentKey: Clone,
{
    type Error = DataError;
    type Input = AlpacaTradeMessage;
    type Output = MarketEvent<InstrumentKey, PublicTrade>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut results = Vec::with_capacity(input.0.len());
        let time_received = Utc::now();

        for trade in input.0 {
            match self.instrument_map.find(&trade.subscription_id) {
                Ok(instrument) => {
                    results.push(Ok(MarketEvent {
                        time_exchange: trade.timestamp,
                        time_received,
                        exchange: self.exchange_id,
                        instrument: instrument.clone(),
                        kind: PublicTrade {
                            id: format_smolstr!("{}", trade.id),
                            price: trade.price,
                            amount: trade.size,
                            side: trade.side(),
                        },
                    }));
                }
                Err(unidentified) => {
                    results.push(Err(DataError::Socket(unidentified.to_string())));
                }
            }
        }

        results
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_de_equities_trade() {
        let input = r#"{"T":"t","S":"AAPL","i":123,"x":"V","p":150.25,"s":100,"c":["@"],"z":"C","t":"2026-05-02T14:00:00Z"}"#;
        let trade: AlpacaTrade = serde_json::from_str(input).unwrap();

        assert_eq!(trade.subscription_id.as_ref(), "trades|AAPL");
        assert_eq!(trade.id, 123);
        assert_eq!(trade.price, 150.25);
        assert_eq!(trade.size, 100.0);
        assert_eq!(trade.exchange, Some(SmolStr::new("V")));
        assert_eq!(trade.tape, Some(SmolStr::new("C")));
        assert!(trade.taker_side.is_none());
    }

    #[test]
    fn test_de_crypto_trade() {
        let input = r#"{"T":"t","S":"BTC/USD","i":456,"p":60000.50,"s":0.5,"tks":"B","t":"2026-05-02T14:00:00Z"}"#;
        let trade: AlpacaTrade = serde_json::from_str(input).unwrap();

        assert_eq!(trade.subscription_id.as_ref(), "trades|BTC/USD");
        assert_eq!(trade.id, 456);
        assert_eq!(trade.price, 60000.50);
        assert_eq!(trade.size, 0.5);
        assert!(trade.exchange.is_none());
        assert!(trade.tape.is_none());
        assert_eq!(trade.taker_side, Some(SmolStr::new("B")));
        assert_eq!(trade.side(), Side::Buy);
    }

    #[test]
    fn test_crypto_side_sell() {
        let input = r#"{"T":"t","S":"ETH/USD","i":789,"p":3000.0,"s":1.0,"tks":"S","t":"2026-05-02T14:00:00Z"}"#;
        let trade: AlpacaTrade = serde_json::from_str(input).unwrap();
        assert_eq!(trade.side(), Side::Sell);
    }

    #[test]
    fn test_subscription_id() {
        let input = r#"{"T":"t","S":"AAPL","i":123,"p":150.25,"s":100,"t":"2026-05-02T14:00:00Z"}"#;
        let trade: AlpacaTrade = serde_json::from_str(input).unwrap();
        assert_eq!(trade.subscription_id.as_ref(), "trades|AAPL");
    }
}
