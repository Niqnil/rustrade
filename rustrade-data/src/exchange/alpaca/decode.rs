//! A transformer for Alpaca messages that may report nothing, or that need state kept across
//! messages to decode.
//!
//! The trade and quote transformers turn every message into an event. A status message may report
//! nothing a stream yields, and its decoder remembers which undocumented codes it has already
//! logged. [`AlpacaDecoder`] states that decoding once per channel, and [`AlpacaDecodeTransformer`]
//! does the rest: it finds each message's instrument and stamps its event.

use crate::{
    error::DataError,
    event::MarketEvent,
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use chrono::{DateTime, Utc};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// One Alpaca message, as the transformer routes it.
pub trait AlpacaMessage {
    /// The subscription the message belongs to: its channel and symbol.
    fn subscription_id(&self) -> &SubscriptionId;

    /// When the venue says the message's event happened.
    fn time_exchange(&self) -> DateTime<Utc>;
}

/// How one Alpaca channel's messages decode into events.
///
/// A decoder lives as long as its stream's connection: a reconnect starts a new one.
pub trait AlpacaDecoder: Default {
    /// A frame as Alpaca sends it: an array of messages.
    type Frame: for<'de> Deserialize<'de> + IntoIterator<Item = Self::Message>;
    /// One message in a frame.
    type Message: AlpacaMessage;
    /// What a message decodes into.
    type Event;

    /// The event `message` reports, or `None` if it reports nothing this stream yields.
    fn decode(&mut self, message: Self::Message) -> Option<Self::Event>;
}

/// Transformer for one Alpaca channel, producing each message's event with `Decoder`.
///
/// A message for a symbol the stream did not subscribe to is an error; one the decoder drops is
/// skipped without one.
#[derive(Debug)]
pub struct AlpacaDecodeTransformer<Exchange, InstrumentKey, Decoder> {
    instrument_map: Map<InstrumentKey>,
    exchange_id: ExchangeId,
    decoder: Decoder,
    phantom: PhantomData<Exchange>,
}

impl<Exchange, InstrumentKey, Decoder> AlpacaDecodeTransformer<Exchange, InstrumentKey, Decoder>
where
    Exchange: Connector,
    Decoder: AlpacaDecoder,
{
    fn new(instrument_map: Map<InstrumentKey>) -> Self {
        Self {
            instrument_map,
            exchange_id: Exchange::ID,
            decoder: Decoder::default(),
            phantom: PhantomData,
        }
    }
}

impl<Exchange, InstrumentKey, Kind, Decoder> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for AlpacaDecodeTransformer<Exchange, InstrumentKey, Decoder>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind<Event = Decoder::Event> + Send,
    Decoder: AlpacaDecoder + Send,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self::new(instrument_map))
    }
}

impl<Exchange, InstrumentKey, Decoder> Transformer
    for AlpacaDecodeTransformer<Exchange, InstrumentKey, Decoder>
where
    InstrumentKey: Clone,
    Decoder: AlpacaDecoder,
{
    type Error = DataError;
    type Input = Decoder::Frame;
    type Output = MarketEvent<InstrumentKey, Decoder::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let time_received = Utc::now();

        input
            .into_iter()
            .filter_map(|message| {
                // Found before decoding, so an unknown symbol is an error even when the decoder would
                // drop its message; cloned only for a message that becomes an event.
                let instrument = match self.instrument_map.find(message.subscription_id()) {
                    Ok(instrument) => instrument,
                    Err(unidentified) => {
                        return Some(Err(DataError::Socket(unidentified.to_string())));
                    }
                };
                let time_exchange = message.time_exchange();

                self.decoder.decode(message).map(|kind| {
                    Ok(MarketEvent {
                        time_exchange,
                        time_received,
                        exchange: self.exchange_id,
                        instrument: instrument.clone(),
                        kind,
                    })
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::alpaca::AlpacaIex;

    fn de_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <&str as Deserialize>::deserialize(deserializer)
            .map(|symbol| SubscriptionId::from(format!("test|{symbol}").as_str()))
    }

    /// A message carrying a number, which decodes to itself when even and to nothing when odd.
    #[derive(Debug, Deserialize)]
    struct Numbered {
        #[serde(rename = "S", deserialize_with = "de_id")]
        id: SubscriptionId,
        n: u32,
    }

    impl AlpacaMessage for Numbered {
        fn subscription_id(&self) -> &SubscriptionId {
            &self.id
        }

        fn time_exchange(&self) -> DateTime<Utc> {
            DateTime::<Utc>::from_timestamp(i64::from(self.n), 0).unwrap()
        }
    }

    #[derive(Debug, Default)]
    struct EvenOnly {
        decoded: usize,
    }

    #[derive(Debug, Deserialize)]
    struct Frame(Vec<Numbered>);

    impl IntoIterator for Frame {
        type Item = Numbered;
        type IntoIter = std::vec::IntoIter<Numbered>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.into_iter()
        }
    }

    impl AlpacaDecoder for EvenOnly {
        type Frame = Frame;
        type Message = Numbered;
        type Event = u32;

        fn decode(&mut self, message: Numbered) -> Option<u32> {
            self.decoded += 1;
            message.n.is_multiple_of(2).then_some(message.n)
        }
    }

    #[test]
    fn each_message_becomes_its_event_a_dropped_one_nothing_and_an_unknown_symbol_an_error() {
        let map = Map([(SubscriptionId::from("test|AAPL"), 1_u8)]
            .into_iter()
            .collect());
        let mut transformer = AlpacaDecodeTransformer::<AlpacaIex, u8, EvenOnly>::new(map);
        let frame: Frame = serde_json::from_str(
            r#"[{"S":"AAPL","n":2},{"S":"AAPL","n":3},{"S":"MSFT","n":4},{"S":"AAPL","n":6}]"#,
        )
        .unwrap();

        let results = transformer.transform(frame);

        assert_eq!(results.len(), 3, "{results:?}");
        let event = results[0].as_ref().unwrap();
        assert_eq!(
            (event.exchange, event.instrument, event.kind),
            (ExchangeId::AlpacaIex, 1, 2)
        );
        assert_eq!(event.time_exchange.timestamp(), 2);
        assert!(matches!(results[1], Err(DataError::Socket(_))));
        assert_eq!(results[2].as_ref().unwrap().kind, 6);
        // The unidentified message never reached the decoder.
        assert_eq!(transformer.decoder.decoded, 3);
    }
}
