//! Massive WebSocket market data messages, and the transformer turning them into market events.
//!
//! A Massive frame is a JSON array of messages, each naming its channel in `ev`. The shared
//! connection hands each stream frames holding only the messages of its own subscriptions, so a
//! stream decodes every message in a frame as its kind's message, and each one separately: one
//! that fails to decode is reported without losing the rest of the frame.

use super::transformer::{AggregateProvenance, deserialize_first_condition};
use crate::{
    books::Level,
    error::DataError,
    event::MarketEvent,
    exchange::Connector,
    subscriber::shared::elements::excerpt,
    subscription::{
        Map, SubscriptionKind,
        book::{OrderBookL1, OrderBooksL1},
        candle::{Candle, Candles},
        quote::{Quote, Quotes},
        trade::{PublicTrade, PublicTrades},
    },
    transformer::ExchangeTransformer,
};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::value::RawValue;
use smol_str::{SmolStr, format_smolstr};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// The crypto trade channel, the one whose conditions carry the aggressor's side.
const CRYPTO_TRADES: &str = "XT";

/// A frame of one stream's messages, each decoded on its own.
#[derive(Debug)]
pub struct MassiveMessages<Message>(pub Vec<Result<Message, DataError>>);

impl<'de, Message> Deserialize<'de> for MassiveMessages<Message>
where
    Message: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let elements = Vec::<&'de RawValue>::deserialize(deserializer)?;

        Ok(Self(
            elements
                .into_iter()
                .map(|raw| {
                    serde_json::from_str(raw.get()).map_err(|error| {
                        DataError::Socket(format!(
                            "failed to decode a Massive message: {error}: {}",
                            excerpt(raw.get())
                        ))
                    })
                })
                .collect(),
        ))
    }
}

/// A Massive market data message: what every kind's message has in common.
pub trait MassiveMessage: DeserializeOwned {
    /// The channel the message was published on: its `ev`.
    fn event(&self) -> &str;

    /// The market it concerns, spelled as the subscription spelled it.
    fn symbol(&self) -> &str;

    /// When the provider stamped it.
    fn time_exchange(&self) -> DateTime<Utc>;

    /// The subscription it belongs to, as the instrument map keys subscriptions.
    fn subscription_id(&self) -> SubscriptionId {
        // The spelling `ExchangeSub` gives the same channel and market.
        SubscriptionId(format_smolstr!("{}|{}", self.event(), self.symbol()))
    }
}

/// A subscription kind Massive's WebSocket serves, and the message it is decoded from.
pub trait MassiveKind: SubscriptionKind {
    /// The message the kind's channel publishes.
    type Message: MassiveMessage;

    /// The market event a message amounts to.
    fn event(message: Self::Message) -> Self::Event;
}

/// A trade: `{"ev":"XT","pair":"BTC-USD","p":82857.53,"s":0.01,"t":1759056000000,"c":[2],"i":"1"}`.
///
/// Stocks and options spell the market `sym`, crypto `pair`.
#[derive(Debug, Clone, Deserialize)]
pub struct MassiveTrade {
    #[serde(rename = "ev")]
    pub event: SmolStr,
    #[serde(alias = "sym", alias = "pair")]
    pub symbol: SmolStr,
    #[serde(rename = "p", with = "rust_decimal::serde::float")]
    pub price: Decimal,
    #[serde(rename = "s", with = "rust_decimal::serde::float")]
    pub size: Decimal,
    /// Milliseconds since the epoch.
    #[serde(rename = "t")]
    pub timestamp: i64,
    /// The first trade condition. On crypto, `1` is a sell and `2` a buy; on stocks and options
    /// the conditions describe the trade, not its side.
    #[serde(
        rename = "c",
        default,
        deserialize_with = "deserialize_first_condition"
    )]
    pub condition: Option<i32>,
    /// The trade's identifier, a string on every cluster measured, accepted as a number too.
    #[serde(rename = "i", default, deserialize_with = "deserialize_id")]
    pub id: Option<SmolStr>,
}

/// A quote.
///
/// `{"ev":"XQ","pair":"BTC-USD","bp":82857.5,"bs":0.1,"ap":82858.0,"as":0.2,"t":1759056000000}`,
/// or on forex, which spells the pair `p` and quotes no sizes,
/// `{"ev":"C","p":"EUR/USD","b":1.1702,"a":1.1703,"t":1759056000000}`. Stocks and options spell the
/// market `sym`.
#[derive(Debug, Clone, Deserialize)]
pub struct MassiveQuote {
    #[serde(rename = "ev")]
    pub event: SmolStr,
    /// `p` is forex's pair. It never meets a price named `p`: no quote carries one.
    #[serde(alias = "sym", alias = "pair", alias = "p")]
    pub symbol: SmolStr,
    #[serde(
        alias = "bp",
        alias = "b",
        with = "rust_decimal::serde::float",
        default
    )]
    pub bid_price: Decimal,
    /// Absent on forex.
    #[serde(alias = "bs", with = "rust_decimal::serde::float_option", default)]
    pub bid_size: Option<Decimal>,
    #[serde(
        alias = "ap",
        alias = "a",
        with = "rust_decimal::serde::float",
        default
    )]
    pub ask_price: Decimal,
    /// Absent on forex.
    #[serde(alias = "as", with = "rust_decimal::serde::float_option", default)]
    pub ask_size: Option<Decimal>,
    /// Milliseconds since the epoch.
    #[serde(rename = "t")]
    pub timestamp: i64,
}

/// A per-second or per-minute aggregate:
/// `{"ev":"XAS","pair":"BTC-USD","o":1.0,"h":1.2,"l":0.9,"c":1.1,"v":10.5,"s":1759056000000,"e":1759056001000}`.
///
/// Stocks and options spell the market `sym`, crypto and forex `pair`.
#[derive(Debug, Clone, Deserialize)]
pub struct MassiveAggregate {
    #[serde(rename = "ev")]
    pub event: SmolStr,
    #[serde(alias = "sym", alias = "pair")]
    pub symbol: SmolStr,
    #[serde(rename = "o", with = "rust_decimal::serde::float")]
    pub open: Decimal,
    #[serde(rename = "h", with = "rust_decimal::serde::float")]
    pub high: Decimal,
    #[serde(rename = "l", with = "rust_decimal::serde::float")]
    pub low: Decimal,
    #[serde(rename = "c", with = "rust_decimal::serde::float")]
    pub close: Decimal,
    /// On forex, a count of quote updates rather than a traded volume — see
    /// [`provenance`](Self::provenance).
    #[serde(rename = "v", with = "rust_decimal::serde::float")]
    pub volume: Decimal,
    /// The window's end in milliseconds since the epoch, exclusive: its start plus the interval.
    #[serde(rename = "e")]
    pub end_timestamp: i64,
    // `z` is deliberately not read. It is the window's average trade *size*, not a trade count, and
    // this message carries no count at all.
}

impl MassiveAggregate {
    /// What the aggregate was built from. Spot FX has no consolidated tape, so Massive builds its
    /// forex aggregates — `CA` and `CAS` — from quoted bid/ask updates.
    pub fn provenance(&self) -> AggregateProvenance {
        if self.event.starts_with('C') {
            AggregateProvenance::QuoteTape
        } else {
            AggregateProvenance::TradeTape
        }
    }
}

impl MassiveMessage for MassiveTrade {
    fn event(&self) -> &str {
        &self.event
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn time_exchange(&self) -> DateTime<Utc> {
        millis_to_datetime(self.timestamp)
    }
}

impl MassiveMessage for MassiveQuote {
    fn event(&self) -> &str {
        &self.event
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn time_exchange(&self) -> DateTime<Utc> {
        millis_to_datetime(self.timestamp)
    }
}

impl MassiveMessage for MassiveAggregate {
    fn event(&self) -> &str {
        &self.event
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    /// The window's close, which [`Candle::close_time`] is too: the event is stamped when the
    /// window it summarises ends.
    fn time_exchange(&self) -> DateTime<Utc> {
        millis_to_datetime(self.end_timestamp)
    }
}

impl MassiveKind for PublicTrades {
    type Message = MassiveTrade;

    fn event(trade: MassiveTrade) -> PublicTrade {
        // Only crypto's conditions say who took liquidity; on stocks and options `1` and `2` are
        // unrelated trade conditions.
        let side = (trade.event == CRYPTO_TRADES)
            .then_some(trade.condition)
            .flatten()
            .and_then(|condition| match condition {
                1 => Some(Side::Sell),
                2 => Some(Side::Buy),
                _ => None,
            });

        PublicTrade {
            id: trade.id.unwrap_or_default(),
            price: trade.price,
            amount: trade.size,
            side,
        }
    }
}

/// The quote as published. Forex quotes carry no sizes, so both amounts are zero there.
///
/// A [`Quote`] has no way to say a side is empty, so a side with nothing quoted on it — a zero
/// price, or none — arrives as a zero price. Check both prices before using
/// [`Quote::mid_price`] or [`Quote::spread`]; [`OrderBooksL1`] maps such a side to `None` instead.
impl MassiveKind for Quotes {
    type Message = MassiveQuote;

    fn event(quote: MassiveQuote) -> Quote {
        Quote {
            bid_price: quote.bid_price,
            bid_amount: quote.bid_size.unwrap_or(Decimal::ZERO),
            ask_price: quote.ask_price,
            ask_amount: quote.ask_size.unwrap_or(Decimal::ZERO),
        }
    }
}

/// The quote as a top of book, stamped with the quote's own time.
///
/// # A zero price is absent
/// A side with nothing quoted on it can only arrive as a zero price, or no price at all. A
/// zero-priced level would assert someone will trade at zero, and a mid-price averaged against it
/// is wrong in a way nothing downstream can detect. So a zero price maps to `None`, the rule the
/// other L1 decoders in this crate follow. A missing size at a real price — every forex quote — is
/// a zero amount: the price is still a quote.
impl MassiveKind for OrderBooksL1 {
    type Message = MassiveQuote;

    fn event(quote: MassiveQuote) -> OrderBookL1 {
        let side = |price: Decimal, size: Option<Decimal>| {
            (!price.is_zero()).then(|| Level::new(price, size.unwrap_or(Decimal::ZERO)))
        };

        OrderBookL1 {
            // Must equal the event's `time_exchange`: downstream state orders L1 updates on it.
            last_update_time: quote.time_exchange(),
            best_bid: side(quote.bid_price, quote.bid_size),
            best_ask: side(quote.ask_price, quote.ask_size),
        }
    }
}

/// The aggregate as a candle.
///
/// `close_time` is the wire's window end, which Massive supplies as the exclusive end of the
/// period (`e == s + interval`), so it satisfies the [`Candle::close_time`] contract as it stands.
/// A forex candle reports no volume, since its `v` counts quote updates, and no candle reports a
/// trade count, since the message carries none.
impl MassiveKind for Candles {
    type Message = MassiveAggregate;

    fn event(aggregate: MassiveAggregate) -> Candle {
        let provenance = aggregate.provenance();

        Candle {
            close_time: aggregate.time_exchange(),
            open: aggregate.open,
            high: aggregate.high,
            low: aggregate.low,
            close: aggregate.close,
            volume: provenance.volume(aggregate.volume),
            trade_count: None,
        }
    }
}

/// Transforms one stream's Massive messages into market events of its kind.
#[derive(Debug)]
pub struct MassiveTransformer<Exchange, InstrumentKey, Kind> {
    instrument_map: Map<InstrumentKey>,
    phantom: PhantomData<(Exchange, Kind)>,
}

impl<Exchange, InstrumentKey, Kind> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for MassiveTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: MassiveKind + Send,
    Kind::Event: Send,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            instrument_map,
            phantom: PhantomData,
        })
    }
}

impl<Exchange, InstrumentKey, Kind> Transformer
    for MassiveTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Kind: MassiveKind,
{
    type Error = DataError;
    type Input = MassiveMessages<Kind::Message>;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, MassiveMessages(messages): Self::Input) -> Self::OutputIter {
        let time_received = Utc::now();

        messages
            .into_iter()
            .map(|message| {
                let message = message?;
                let instrument = self
                    .instrument_map
                    .find(&message.subscription_id())
                    .map_err(|unidentified| DataError::Socket(unidentified.to_string()))?
                    .clone();

                Ok(MarketEvent {
                    time_exchange: message.time_exchange(),
                    time_received,
                    exchange: Exchange::ID,
                    instrument,
                    kind: Kind::event(message),
                })
            })
            .collect()
    }
}

/// A trade identifier, as a string or a number.
fn deserialize_id<'de, D>(deserializer: D) -> Result<Option<SmolStr>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Id {
        Text(SmolStr),
        Number(u64),
    }

    Ok(Option::<Id>::deserialize(deserializer)?.map(|id| match id {
        Id::Text(text) => text,
        Id::Number(number) => format_smolstr!("{number}"),
    }))
}

/// A millisecond timestamp as an instant, or the epoch, with a warning, if it is out of range.
fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_else(|| {
            tracing::warn!(
                millis,
                "out-of-range millisecond timestamp; using UNIX_EPOCH"
            );
            DateTime::<Utc>::UNIX_EPOCH
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        Identifier,
        exchange::massive::{
            Massive, MassiveServerCrypto, MassiveServerForex, MassiveServerStocks,
            channel::MassiveChannel,
            market::{MassiveMarket, MassiveServer},
        },
        subscriber::mapper::{SubscriptionMapper, WebSocketSubMapper},
        subscription::{Subscription, candle::CandleInterval},
    };
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        exchange::ExchangeId,
        instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
    };

    type Events<Kind> = Vec<
        Result<MarketEvent<MarketDataInstrument, <Kind as SubscriptionKind>::Event>, DataError>,
    >;

    /// Transform `frame` for a stream of `kind` holding `pairs` on `Server`'s cluster, keyed by the
    /// map the subscriber builds, so a message must name its subscription as the stream does.
    fn transform<Server, Kind>(pairs: &[(&str, &str)], kind: Kind, frame: &str) -> Events<Kind>
    where
        Server: MassiveServer,
        Kind: MassiveKind,
        Subscription<Massive<Server>, MarketDataInstrument, Kind>:
            Identifier<MassiveChannel> + Identifier<MassiveMarket>,
    {
        let subscriptions = pairs
            .iter()
            .map(|(base, quote)| {
                Subscription::new(
                    Massive::<Server>::default(),
                    MarketDataInstrument::new(*base, *quote, MarketDataInstrumentKind::Spot),
                    kind.clone(),
                )
            })
            .collect::<Vec<_>>();

        let mut transformer = MassiveTransformer::<Massive<Server>, _, Kind> {
            instrument_map: WebSocketSubMapper::map(&subscriptions).instrument_map,
            phantom: PhantomData,
        };

        transformer.transform(serde_json::from_str(frame).unwrap())
    }

    fn only<Event>(mut events: Vec<Result<Event, DataError>>) -> Event {
        assert_eq!(events.len(), 1);
        events.remove(0).unwrap()
    }

    #[test]
    fn a_crypto_trade_carries_its_aggressor_side() {
        let event = only(transform::<MassiveServerCrypto, _>(
            &[("btc", "usd")],
            PublicTrades,
            r#"[{"ev":"XT","pair":"BTC-USD","p":45230.5,"s":0.5,"t":1704067200000,"c":[2],"i":"trade123"}]"#,
        ));

        assert_eq!(event.instrument.base.name().as_str(), "btc");
        assert_eq!(event.exchange, ExchangeId::MassiveCrypto);
        assert_eq!(event.time_exchange.timestamp_millis(), 1704067200000);
        assert_eq!(event.kind.price, dec!(45230.5));
        assert_eq!(event.kind.amount, dec!(0.5));
        assert_eq!(event.kind.side, Some(Side::Buy));
        assert_eq!(event.kind.id, "trade123");
    }

    /// On stocks and options, conditions 1 and 2 describe the trade, not who took liquidity.
    #[test]
    fn a_stock_trade_has_no_side_whatever_its_conditions() {
        let event = only(transform::<MassiveServerStocks, _>(
            &[("aapl", "usd")],
            PublicTrades,
            r#"[{"ev":"T","sym":"AAPL","p":150.25,"s":100,"t":1704067200000,"c":[2],"i":12345}]"#,
        ));

        assert_eq!(event.exchange, ExchangeId::MassiveStocks);
        assert_eq!(event.kind.price, dec!(150.25));
        assert_eq!(event.kind.amount, dec!(100));
        assert_eq!(event.kind.side, None);
        // A numeric identifier is accepted too.
        assert_eq!(event.kind.id, "12345");
    }

    #[test]
    fn a_crypto_quote_is_a_quote_and_a_top_of_book() {
        let frame = r#"[{"ev":"XQ","pair":"BTC-USD","bp":45220.0,"bs":2.5,"ap":45240.0,"as":3.0,"lp":45230.0,"ls":0.1,"t":1704067200000}]"#;

        let quote = only(transform::<MassiveServerCrypto, _>(
            &[("btc", "usd")],
            Quotes,
            frame,
        ));
        assert_eq!(
            quote.kind,
            Quote {
                bid_price: dec!(45220.0),
                bid_amount: dec!(2.5),
                ask_price: dec!(45240.0),
                ask_amount: dec!(3.0),
            }
        );

        let book = only(transform::<MassiveServerCrypto, _>(
            &[("btc", "usd")],
            OrderBooksL1,
            frame,
        ));
        assert_eq!(
            book.kind.best_bid,
            Some(Level::new(dec!(45220.0), dec!(2.5)))
        );
        assert_eq!(
            book.kind.best_ask,
            Some(Level::new(dec!(45240.0), dec!(3.0)))
        );
        assert_eq!(book.kind.last_update_time, book.time_exchange);
    }

    /// Forex names the pair `p`, slashed, and quotes no sizes.
    #[test]
    fn a_forex_quote_has_zero_amounts() {
        let frame =
            r#"[{"ev":"C","p":"EUR/USD","b":1.085,"a":1.0852,"i":0,"x":48,"t":1704067200000}]"#;

        let quote = only(transform::<MassiveServerForex, _>(
            &[("eur", "usd")],
            Quotes,
            frame,
        ));
        assert_eq!(quote.exchange, ExchangeId::MassiveForex);
        assert_eq!(quote.kind.bid_price, dec!(1.085));
        assert_eq!(quote.kind.ask_price, dec!(1.0852));
        assert_eq!(quote.kind.bid_amount, Decimal::ZERO);
        assert_eq!(quote.kind.ask_amount, Decimal::ZERO);

        let book = only(transform::<MassiveServerForex, _>(
            &[("eur", "usd")],
            OrderBooksL1,
            frame,
        ));
        assert_eq!(
            book.kind.best_bid,
            Some(Level::new(dec!(1.085), Decimal::ZERO))
        );
    }

    #[test]
    fn a_zero_priced_side_is_absent_from_the_top_of_book() {
        let book = only(transform::<MassiveServerCrypto, _>(
            &[("btc", "usd")],
            OrderBooksL1,
            r#"[{"ev":"XQ","pair":"BTC-USD","bp":0,"bs":0,"ap":45240.0,"as":3.0,"t":1704067200000}]"#,
        ));

        assert_eq!(book.kind.best_bid, None);
        assert!(book.kind.best_ask.is_some());
    }

    /// `e` is the window's exclusive end, `s` plus the interval, and is the candle's close time.
    /// `z` is an average trade size, routinely fractional, and is not read as a count.
    #[test]
    fn an_aggregate_is_a_candle_closing_at_its_window_end() {
        let start = 1704067200000_i64;
        let candle = only(transform::<MassiveServerStocks, _>(
            &[("aapl", "usd")],
            Candles {
                interval: CandleInterval::Min1,
            },
            &format!(
                r#"[{{"ev":"AM","sym":"AAPL","o":150.1,"h":150.5,"l":150.05,"c":150.25,"v":1000,"vw":150.2,"s":{start},"e":{},"z":78.5}}]"#,
                start + 60_000
            ),
        ));

        assert_eq!(candle.time_exchange.timestamp_millis(), start + 60_000);
        assert_eq!(candle.kind.close_time, candle.time_exchange);
        assert_eq!(candle.kind.open, dec!(150.1));
        assert_eq!(candle.kind.high, dec!(150.5));
        assert_eq!(candle.kind.low, dec!(150.05));
        assert_eq!(candle.kind.close, dec!(150.25));
        assert_eq!(candle.kind.volume, Some(dec!(1000)));
        assert_eq!(candle.kind.trade_count, None);
    }

    /// Spot FX has no trade tape: Massive builds forex aggregates from quote updates, so `v` counts
    /// those, and reporting it as traded volume would be a silent lie.
    #[test]
    fn a_forex_aggregate_reports_no_volume() {
        let candle = only(transform::<MassiveServerForex, _>(
            &[("eur", "usd")],
            Candles {
                interval: CandleInterval::Sec1,
            },
            r#"[{"ev":"CAS","pair":"EUR/USD","o":1.1,"h":1.2,"l":1.0,"c":1.15,"v":4321,"s":1704067200000,"e":1704067201000}]"#,
        ));

        assert_eq!(candle.kind.volume, None);
        assert_eq!(candle.kind.trade_count, None);
    }

    /// Each message decodes on its own: one that fails is reported, and the rest of its frame is
    /// kept.
    #[test]
    fn a_message_that_fails_to_decode_loses_only_itself() {
        let events = transform::<MassiveServerCrypto, _>(
            &[("btc", "usd"), ("eth", "usd")],
            PublicTrades,
            r#"[{"ev":"XT","pair":"BTC-USD","p":1.0,"s":1.0,"t":1,"i":"1"},
                {"ev":"XT","pair":"ETH-USD","p":"not a price","s":1.0,"t":1},
                {"ev":"XT","pair":"ETH-USD","p":2.0,"s":1.0,"t":1,"i":"2"}]"#,
        );

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].as_ref().unwrap().kind.price, dec!(1.0));
        let error = events[1].as_ref().unwrap_err().to_string();
        assert!(
            error.contains("failed to decode a Massive message"),
            "{error}"
        );
        assert_eq!(events[2].as_ref().unwrap().kind.price, dec!(2.0));
    }

    #[test]
    fn a_message_for_no_subscription_of_the_stream_is_an_error() {
        let events = transform::<MassiveServerCrypto, _>(
            &[("btc", "usd")],
            PublicTrades,
            r#"[{"ev":"XT","pair":"ETH-USD","p":1.0,"s":1.0,"t":1}]"#,
        );

        assert!(events[0].is_err());
    }
}
