use super::channel::AlpacaChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::quote::Quote,
};
use chrono::{DateTime, Utc};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Deserialize "S" (symbol) field as the associated [`SubscriptionId`] for quotes.
fn de_quote_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((AlpacaChannel::Quotes, symbol)).id())
}

/// Alpaca quote message.
///
/// Unified struct for both equities and crypto quotes with optional fields.
///
/// ### Equities Example (IEX/SIP)
/// ```json
/// {"T":"q","S":"AAPL","ax":"V","ap":150.25,"as":100,"bx":"Q","bp":150.20,"bs":200,"c":["R"],"z":"C","t":"2026-05-02T14:00:00Z"}
/// ```
///
/// ### Crypto Example
/// ```json
/// {"T":"q","S":"BTC/USD","ap":60000.50,"as":1.0,"bp":60000.00,"bs":2.0,"t":"2026-05-02T14:00:00Z"}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct AlpacaQuote {
    /// Subscription ID constructed from symbol during deserialization.
    /// Avoids per-event `format!` allocation in hot path.
    #[serde(rename = "S", deserialize_with = "de_quote_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(rename = "bp")]
    pub bid_price: f64,
    #[serde(rename = "bs")]
    pub bid_size: f64,
    #[serde(rename = "ap")]
    pub ask_price: f64,
    #[serde(rename = "as")]
    pub ask_size: f64,
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "bx", default)]
    pub bid_exchange: Option<SmolStr>,
    #[serde(rename = "ax", default)]
    pub ask_exchange: Option<SmolStr>,
    #[serde(rename = "z", default)]
    pub tape: Option<SmolStr>,
}

impl Identifier<Option<SubscriptionId>> for AlpacaQuote {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, AlpacaQuote)>
    for MarketIter<InstrumentKey, Quote>
{
    fn from((exchange_id, instrument, quote): (ExchangeId, InstrumentKey, AlpacaQuote)) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: quote.timestamp,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Quote {
                bid_price: quote.bid_price,
                bid_amount: quote.bid_size,
                ask_price: quote.ask_price,
                ask_amount: quote.ask_size,
            },
        })])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_de_equities_quote() {
        let input = r#"{"T":"q","S":"AAPL","ax":"V","ap":150.25,"as":100,"bx":"Q","bp":150.20,"bs":200,"c":["R"],"z":"C","t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();

        assert_eq!(quote.subscription_id.as_ref(), "quotes|AAPL");
        assert_eq!(quote.bid_price, 150.20);
        assert_eq!(quote.bid_size, 200.0);
        assert_eq!(quote.ask_price, 150.25);
        assert_eq!(quote.ask_size, 100.0);
        assert_eq!(quote.bid_exchange, Some(SmolStr::new("Q")));
        assert_eq!(quote.ask_exchange, Some(SmolStr::new("V")));
        assert_eq!(quote.tape, Some(SmolStr::new("C")));
    }

    #[test]
    fn test_de_crypto_quote() {
        let input = r#"{"T":"q","S":"BTC/USD","ap":60000.50,"as":1.0,"bp":60000.00,"bs":2.0,"t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();

        assert_eq!(quote.subscription_id.as_ref(), "quotes|BTC/USD");
        assert_eq!(quote.bid_price, 60000.00);
        assert_eq!(quote.bid_size, 2.0);
        assert_eq!(quote.ask_price, 60000.50);
        assert_eq!(quote.ask_size, 1.0);
        assert!(quote.bid_exchange.is_none());
        assert!(quote.ask_exchange.is_none());
        assert!(quote.tape.is_none());
    }

    #[test]
    fn test_subscription_id() {
        let input = r#"{"T":"q","S":"SPY","bp":450.0,"bs":100,"ap":450.05,"as":50,"t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();
        assert_eq!(quote.subscription_id.as_ref(), "quotes|SPY");
    }
}
