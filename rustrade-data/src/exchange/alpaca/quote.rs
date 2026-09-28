use super::channel::AlpacaChannel;
use crate::{
    Identifier,
    books::Level,
    error::DataError,
    event::MarketEvent,
    exchange::ExchangeSub,
    subscription::{
        Map, SubscriptionKind,
        book::OrderBookL1,
        quote::{Quote, Quotes},
    },
    transformer::ExchangeTransformer,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::{Deserialize, Deserializer};
use smol_str::SmolStr;
use std::marker::PhantomData;
use tokio::sync::mpsc;

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
#[derive(Debug, Deserialize)]
pub struct AlpacaQuote {
    /// Subscription ID constructed from symbol during deserialization.
    /// Avoids per-event `format!` allocation in hot path.
    #[serde(rename = "S", deserialize_with = "de_quote_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(rename = "bp")]
    pub bid_price: Decimal,
    #[serde(rename = "bs")]
    pub bid_size: Decimal,
    #[serde(rename = "ap")]
    pub ask_price: Decimal,
    #[serde(rename = "as")]
    pub ask_size: Decimal,
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "bx", default)]
    pub bid_exchange: Option<SmolStr>,
    #[serde(rename = "ax", default)]
    pub ask_exchange: Option<SmolStr>,
    #[serde(rename = "z", default)]
    pub tape: Option<SmolStr>,
}

/// Alpaca WebSocket message wrapper for quotes.
///
/// Alpaca sends all messages as JSON arrays: `[{"T":"q",...},{"T":"q",...}]`.
/// This wrapper deserializes the array and extracts quote messages.
#[derive(Debug)]
pub struct AlpacaQuoteMessage(Vec<AlpacaQuote>);

/// Internal enum for single-pass deserialization of Alpaca array messages.
/// Uses `#[serde(tag = "T")]` to dispatch on message type in one parse pass,
/// avoiding the intermediate `Vec<serde_json::Value>` allocation.
#[derive(Deserialize)]
#[serde(tag = "T")]
enum AlpacaArrayQuoteMsg {
    #[serde(rename = "q")]
    Quote(AlpacaQuote),
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for AlpacaQuoteMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let messages = Vec::<AlpacaArrayQuoteMsg>::deserialize(deserializer)?;
        let mut quotes = Vec::with_capacity(messages.len());
        for msg in messages {
            if let AlpacaArrayQuoteMsg::Quote(quote) = msg {
                quotes.push(quote);
            }
        }
        Ok(AlpacaQuoteMessage(quotes))
    }
}

impl From<AlpacaQuote> for Quote {
    fn from(quote: AlpacaQuote) -> Self {
        Self {
            bid_price: quote.bid_price,
            bid_amount: quote.bid_size,
            ask_price: quote.ask_price,
            ask_amount: quote.ask_size,
        }
    }
}

/// The quote as a top-of-book, stamped with the quote's own time.
///
/// The book is the feed's, not the market's: IEX quotes are IEX's own top of book, SIP quotes are
/// the NBBO, and crypto quotes are Alpaca's crypto venue.
///
/// # A zero price is absent
/// Every Alpaca quote carries a price for both sides, so a side with nothing quoted on it can only
/// arrive as a zero price. A zero-priced level would assert someone will trade at zero,
/// and a mid-price averaged against it is wrong in a way nothing downstream can detect. So a zero
/// price maps to `None`, the rule the Binance and London Strategic Edge L1 decoders follow. A zero
/// *size* at a real price is kept: the price is still a quote.
impl From<AlpacaQuote> for OrderBookL1 {
    fn from(quote: AlpacaQuote) -> Self {
        Self {
            // Must equal the event's `time_exchange`, which the transformer takes from the same
            // field: downstream state orders L1 updates on this one.
            last_update_time: quote.timestamp,
            best_bid: quoted(quote.bid_price, quote.bid_size),
            best_ask: quoted(quote.ask_price, quote.ask_size),
        }
    }
}

/// One side of the book, or `None` where Alpaca published no quote for it.
fn quoted(price: Decimal, size: Decimal) -> Option<Level> {
    (!price.is_zero()).then(|| Level::new(price, size))
}

/// Transformer for Alpaca quote messages, producing `Kind`'s event from each quote.
///
/// Handles array-wrapped messages and processes each quote individually, looking up the correct
/// instrument for each symbol. Serves [`Quotes`] and
/// [`OrderBooksL1`](crate::subscription::book::OrderBooksL1), both read from Alpaca's `quotes`
/// channel.
#[derive(Debug)]
pub struct AlpacaQuoteTransformer<Exchange, InstrumentKey, Kind = Quotes> {
    instrument_map: Map<InstrumentKey>,
    exchange_id: ExchangeId,
    phantom: PhantomData<(Exchange, Kind)>,
}

impl<Exchange, InstrumentKey, Kind> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for AlpacaQuoteTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: crate::exchange::Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Kind::Event: From<AlpacaQuote>,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            instrument_map,
            exchange_id: Exchange::ID,
            phantom: PhantomData,
        })
    }
}

impl<Exchange, InstrumentKey, Kind> Transformer
    for AlpacaQuoteTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: crate::exchange::Connector,
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    Kind::Event: From<AlpacaQuote>,
{
    type Error = DataError;
    type Input = AlpacaQuoteMessage;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut results = Vec::with_capacity(input.0.len());
        let time_received = Utc::now();

        for quote in input.0 {
            match self.instrument_map.find(&quote.subscription_id) {
                Ok(instrument) => {
                    results.push(Ok(MarketEvent {
                        time_exchange: quote.timestamp,
                        time_received,
                        exchange: self.exchange_id,
                        instrument: instrument.clone(),
                        kind: Kind::Event::from(quote),
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
    use rust_decimal_macros::dec;

    #[test]
    fn test_de_equities_quote() {
        let input = r#"{"T":"q","S":"AAPL","ax":"V","ap":150.25,"as":100,"bx":"Q","bp":150.20,"bs":200,"c":["R"],"z":"C","t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();

        assert_eq!(quote.subscription_id.as_ref(), "quotes|AAPL");
        assert_eq!(quote.bid_price, dec!(150.20));
        assert_eq!(quote.bid_size, dec!(200));
        assert_eq!(quote.ask_price, dec!(150.25));
        assert_eq!(quote.ask_size, dec!(100));
        assert_eq!(quote.bid_exchange, Some(SmolStr::new("Q")));
        assert_eq!(quote.ask_exchange, Some(SmolStr::new("V")));
        assert_eq!(quote.tape, Some(SmolStr::new("C")));
    }

    #[test]
    fn test_de_crypto_quote() {
        let input = r#"{"T":"q","S":"BTC/USD","ap":60000.50,"as":1.0,"bp":60000.00,"bs":2.0,"t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();

        assert_eq!(quote.subscription_id.as_ref(), "quotes|BTC/USD");
        assert_eq!(quote.bid_price, dec!(60000.00));
        assert_eq!(quote.bid_size, dec!(2.0));
        assert_eq!(quote.ask_price, dec!(60000.50));
        assert_eq!(quote.ask_size, dec!(1.0));
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

    #[test]
    fn a_quote_becomes_a_book_stamped_with_its_own_time() {
        let input = r#"{"T":"q","S":"BTC/USD","ap":60000.50,"as":1.0,"bp":60000.00,"bs":2.0,"t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();
        let time = quote.timestamp;

        let book = OrderBookL1::from(quote);

        assert_eq!(book.last_update_time, time);
        assert_eq!(book.best_bid, Some(Level::new(dec!(60000.00), dec!(2.0))));
        assert_eq!(book.best_ask, Some(Level::new(dec!(60000.50), dec!(1.0))));
    }

    #[test]
    fn a_zero_priced_side_is_absent_but_a_zero_sized_one_is_kept() {
        let input =
            r#"{"T":"q","S":"AAPL","bp":0,"bs":0,"ap":150.25,"as":0,"t":"2026-05-02T14:00:00Z"}"#;
        let quote: AlpacaQuote = serde_json::from_str(input).unwrap();

        let book = OrderBookL1::from(quote);

        assert_eq!(book.best_bid, None);
        assert_eq!(book.best_ask, Some(Level::new(dec!(150.25), dec!(0))));
    }

    #[test]
    fn each_quote_in_a_frame_becomes_one_book_under_its_own_instrument() {
        use crate::{exchange::alpaca::AlpacaCrypto, subscription::book::OrderBooksL1};
        use rustrade_integration::subscription::SubscriptionId;

        let map = Map([
            (SubscriptionId::from("quotes|BTC/USD"), 1_u8),
            (SubscriptionId::from("quotes|ETH/USD"), 2_u8),
        ]
        .into_iter()
        .collect());
        let mut transformer = AlpacaQuoteTransformer::<AlpacaCrypto, u8, OrderBooksL1> {
            instrument_map: map,
            exchange_id: ExchangeId::AlpacaCrypto,
            phantom: PhantomData,
        };
        let frame: AlpacaQuoteMessage = serde_json::from_str(
            r#"[{"T":"q","S":"BTC/USD","ap":2,"as":1,"bp":1,"bs":1,"t":"2026-05-02T14:00:00Z"},
                {"T":"q","S":"ETH/USD","ap":4,"as":1,"bp":3,"bs":1,"t":"2026-05-02T14:00:01Z"}]"#,
        )
        .unwrap();

        let events = transformer
            .transform(frame)
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        assert_eq!(events.len(), 2);
        for (event, (instrument, bid)) in events.iter().zip([(1, dec!(1)), (2, dec!(3))]) {
            assert_eq!(event.exchange, ExchangeId::AlpacaCrypto);
            assert_eq!(event.instrument, instrument);
            assert_eq!(event.kind.last_update_time, event.time_exchange);
            assert_eq!(event.kind.best_bid.unwrap().price, bid);
        }
    }
}
