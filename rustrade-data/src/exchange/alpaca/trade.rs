use super::channel::AlpacaChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use chrono::{DateTime, Utc};
use rustrade_instrument::{Side, exchange::ExchangeId};
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};

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
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
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

impl Identifier<Option<SubscriptionId>> for AlpacaTrade {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, AlpacaTrade)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange_id, instrument, trade): (ExchangeId, InstrumentKey, AlpacaTrade)) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: trade.timestamp,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: PublicTrade {
                id: format_smolstr!("{}", trade.id),
                price: trade.price,
                amount: trade.size,
                side: trade.side(),
            },
        })])
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
