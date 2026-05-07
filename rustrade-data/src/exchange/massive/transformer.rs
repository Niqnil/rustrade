//! JSON to rustrade type transformations for Massive API responses.

use super::error::MassiveError;
use crate::{
    books::Level,
    subscription::{book::OrderBookL1, candle::Candle, trade::PublicTrade},
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;

/// Deserialize only the first element of a conditions array without allocating a Vec.
fn deserialize_first_condition<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{SeqAccess, Visitor};

    struct FirstElementVisitor;

    impl<'de> Visitor<'de> for FirstElementVisitor {
        type Value = Option<i32>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an array of integers or null")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_seq(self)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let first = seq.next_element()?;
            // Drain remaining elements without storing them
            while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
            Ok(first)
        }
    }

    deserializer.deserialize_option(FirstElementVisitor)
}

/// Raw aggregates response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct AggregatesResponse {
    /// Ticker symbol requested
    #[allow(dead_code)] // Retained for API schema; internal pagination uses next_url
    pub ticker: Option<String>,

    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// Request ID for debugging
    #[allow(dead_code)] // Retained for API schema completeness
    pub request_id: Option<String>,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Aggregate bars
    pub results: Option<Vec<AggregateBar>>,
}

/// Single OHLCV bar from Massive aggregates endpoint.
#[derive(Debug, Deserialize)]
pub struct AggregateBar {
    /// Open price
    #[serde(rename = "o", with = "rust_decimal::serde::float")]
    pub open: Decimal,

    /// High price
    #[serde(rename = "h", with = "rust_decimal::serde::float")]
    pub high: Decimal,

    /// Low price
    #[serde(rename = "l", with = "rust_decimal::serde::float")]
    pub low: Decimal,

    /// Close price
    #[serde(rename = "c", with = "rust_decimal::serde::float")]
    pub close: Decimal,

    /// Volume
    #[serde(rename = "v", with = "rust_decimal::serde::float")]
    pub volume: Decimal,

    /// Volume-weighted average price
    #[serde(rename = "vw", with = "rust_decimal::serde::float_option", default)]
    #[allow(dead_code)] // Retained for API schema completeness; not yet consumed
    pub vwap: Option<Decimal>,

    /// Unix timestamp in milliseconds (start of the bar)
    #[serde(rename = "t")]
    pub timestamp: i64,

    /// Number of trades in this bar
    #[serde(rename = "n")]
    pub trade_count: Option<u64>,
}

impl AggregateBar {
    /// Convert to rustrade Candle type.
    ///
    /// The `close_time` is calculated as `timestamp + (multiplier * timespan_duration)`.
    #[allow(dead_code)] // Public API; internal code uses into_candle_with_duration
    pub fn into_candle(self, multiplier: u32, timespan: &str) -> Candle {
        let duration = timespan_to_duration(multiplier, timespan);
        self.into_candle_with_duration(duration)
    }

    /// Convert to rustrade Candle type with a pre-computed duration.
    ///
    /// Use this variant when processing multiple bars with the same timespan
    /// to avoid recomputing the duration for each bar.
    pub fn into_candle_with_duration(self, duration: Duration) -> Candle {
        let start_time = Utc
            .timestamp_millis_opt(self.timestamp)
            .single()
            .unwrap_or_else(|| {
                tracing::warn!(
                    timestamp_ms = self.timestamp,
                    "AggregateBar has out-of-range timestamp; using UNIX_EPOCH"
                );
                DateTime::<Utc>::UNIX_EPOCH
            });
        let close_time = start_time + duration;

        Candle {
            close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: self.trade_count.unwrap_or(0),
        }
    }
}

/// Parse aggregates JSON response.
pub fn parse_aggregates_response(body: &str) -> Result<AggregatesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

/// Convert timespan string to chrono Duration.
pub fn timespan_to_duration(multiplier: u32, timespan: &str) -> Duration {
    let mult = multiplier as i64;
    match timespan {
        "second" => Duration::seconds(mult),
        "minute" => Duration::minutes(mult),
        "hour" => Duration::hours(mult),
        "day" => Duration::days(mult),
        "week" => Duration::weeks(mult),
        "month" => Duration::days(mult * 30),   // Approximate
        "quarter" => Duration::days(mult * 91), // Approximate
        "year" => Duration::days(mult * 365),   // Approximate
        _ => {
            tracing::warn!(timespan = %timespan, "unknown timespan, defaulting to minutes");
            Duration::minutes(mult)
        }
    }
}

// ============================================================================
// Trades
// ============================================================================

/// Raw trades response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct TradesResponse {
    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Trade results
    pub results: Option<Vec<TradeRecord>>,
}

/// Single trade from Massive trades endpoint.
#[derive(Debug, Deserialize)]
pub struct TradeRecord {
    /// First trade condition code (crypto: 1=sell, 2=buy; equities: SIP codes differ).
    ///
    /// Only the first condition is extracted; remaining conditions are ignored.
    #[serde(
        rename = "conditions",
        default,
        deserialize_with = "deserialize_first_condition"
    )]
    pub first_condition: Option<i32>,

    /// Exchange ID
    #[serde(rename = "exchange")]
    #[allow(dead_code)] // Retained for API schema completeness
    pub exchange: Option<i32>,

    /// Trade ID
    #[serde(rename = "id", default)]
    pub id: String,

    /// Participant timestamp (nanoseconds)
    #[serde(rename = "participant_timestamp", default)]
    #[allow(dead_code)] // Accessed via timestamp() method
    pub participant_timestamp: i64,

    /// Trade price
    #[serde(rename = "price", with = "rust_decimal::serde::float")]
    pub price: Decimal,

    /// Trade size
    #[serde(rename = "size", with = "rust_decimal::serde::float")]
    pub size: Decimal,

    /// SIP timestamp (nanoseconds) - when SIP received the trade
    #[serde(rename = "sip_timestamp", default)]
    #[allow(dead_code)] // Retained for API schema completeness
    pub sip_timestamp: i64,
}

impl TradeRecord {
    /// Convert to rustrade PublicTrade type.
    ///
    /// # Side Detection (Crypto Only)
    ///
    /// The `side` field is only meaningful for **crypto tickers** (`X:` prefix).
    /// Crypto condition codes: 1 = sell-initiated, 2 = buy-initiated.
    ///
    /// For equities and other asset classes, condition codes represent SIP/CTA
    /// tape conditions (e.g., 1 = Regular Trade) which are unrelated to trade
    /// direction. The `side` will be `None` or incorrect for non-crypto tickers.
    ///
    /// # Timestamps
    ///
    /// Trade timestamps are not preserved in [`PublicTrade`]. If timestamp
    /// information is required, access the raw [`TradeRecord`] via
    /// [`parse_trades_response`] directly.
    pub fn into_public_trade(self) -> PublicTrade {
        // Crypto condition codes: 1 = sell-initiated, 2 = buy-initiated
        // Note: This mapping is ONLY valid for crypto (X: prefix) tickers
        let side = self.first_condition.and_then(|c| match c {
            1 => Some(Side::Sell),
            2 => Some(Side::Buy),
            _ => None,
        });

        PublicTrade {
            id: SmolStr::from(self.id),
            price: self.price,
            amount: self.size,
            side,
        }
    }

    /// Get the exchange timestamp as DateTime<Utc>.
    #[allow(dead_code)] // Public API for consumers accessing raw TradeRecord
    pub fn timestamp(&self) -> DateTime<Utc> {
        nanos_to_datetime(self.participant_timestamp)
    }
}

/// Convert nanosecond timestamp to DateTime<Utc>.
///
/// Returns [`DateTime::<Utc>::UNIX_EPOCH`] for out-of-range timestamps.
/// Negative values (pre-epoch) are handled correctly using Euclidean division.
fn nanos_to_datetime(nanos: i64) -> DateTime<Utc> {
    let secs = nanos.div_euclid(1_000_000_000);
    // rem_euclid always returns non-negative value in [0, 999_999_999], fits u32
    #[allow(clippy::cast_possible_truncation)]
    let nsecs = nanos.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single().unwrap_or_else(|| {
        tracing::warn!(nanos, "out-of-range nanosecond timestamp; using UNIX_EPOCH");
        DateTime::<Utc>::UNIX_EPOCH
    })
}

/// Parse trades JSON response.
pub fn parse_trades_response(body: &str) -> Result<TradesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

// ============================================================================
// Quotes (BBO/NBBO)
// ============================================================================

/// Raw quotes response from Massive REST API.
#[derive(Debug, Deserialize)]
pub struct QuotesResponse {
    /// Number of results in this response
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,

    /// Status of the request
    #[serde(default)]
    #[allow(dead_code)] // HTTP status already checked in fetch_page_body
    pub status: String,

    /// URL for next page of results (pagination)
    pub next_url: Option<String>,

    /// Quote results
    pub results: Option<Vec<QuoteRecord>>,
}

/// Single quote from Massive quotes endpoint.
#[derive(Debug, Deserialize)]
pub struct QuoteRecord {
    /// Ask price
    #[serde(rename = "ask_price", with = "rust_decimal::serde::float")]
    pub ask_price: Decimal,

    /// Ask size (optional - forex quotes don't include size)
    #[serde(
        rename = "ask_size",
        default,
        with = "rust_decimal::serde::float_option"
    )]
    pub ask_size: Option<Decimal>,

    /// Bid price
    #[serde(rename = "bid_price", with = "rust_decimal::serde::float")]
    pub bid_price: Decimal,

    /// Bid size (optional - forex quotes don't include size)
    #[serde(
        rename = "bid_size",
        default,
        with = "rust_decimal::serde::float_option"
    )]
    pub bid_size: Option<Decimal>,

    /// Participant timestamp (nanoseconds)
    #[serde(rename = "participant_timestamp", default)]
    pub participant_timestamp: i64,

    /// SIP timestamp (nanoseconds)
    #[serde(rename = "sip_timestamp", default)]
    #[allow(dead_code)] // Retained for API schema completeness
    pub sip_timestamp: i64,
}

impl QuoteRecord {
    /// Convert to rustrade OrderBookL1 type.
    ///
    /// Forex quotes from Massive omit `bid_size`/`ask_size`; absent sizes are
    /// represented as `Decimal::ZERO` to satisfy the shared `Level` type.
    /// Callers handling venues that may report zero-size quotes should
    /// disambiguate via the source feed if required.
    pub fn into_order_book_l1(self) -> OrderBookL1 {
        let timestamp = self.timestamp();

        OrderBookL1 {
            last_update_time: timestamp,
            best_bid: Some(Level {
                price: self.bid_price,
                amount: self.bid_size.unwrap_or(Decimal::ZERO),
            }),
            best_ask: Some(Level {
                price: self.ask_price,
                amount: self.ask_size.unwrap_or(Decimal::ZERO),
            }),
        }
    }

    /// Get the exchange timestamp as DateTime<Utc>.
    pub fn timestamp(&self) -> DateTime<Utc> {
        nanos_to_datetime(self.participant_timestamp)
    }
}

/// Parse quotes JSON response.
pub fn parse_quotes_response(body: &str) -> Result<QuotesResponse, MassiveError> {
    serde_json::from_str(body).map_err(|e| MassiveError::Deserialize {
        message: e.to_string(),
        payload: body[..body.floor_char_boundary(512)].to_owned(),
    })
}

// ============================================================================
// WebSocket Messages
// ============================================================================

/// Parsed WebSocket message variants.
///
/// Uses serde's internally-tagged enum for single-pass deserialization.
#[derive(Debug, Deserialize)]
#[serde(tag = "ev")]
pub(crate) enum WsMessage {
    /// Stock trade
    #[serde(rename = "T")]
    TradeStocks(WsTradeMsg),
    /// Crypto trade
    #[serde(rename = "XT")]
    TradeCrypto(WsTradeMsg),
    /// Stock quote
    #[serde(rename = "Q")]
    QuoteStocks(WsQuoteMsg),
    /// Crypto quote
    #[serde(rename = "XQ")]
    QuoteCrypto(WsQuoteMsg),
    /// Forex quote
    #[serde(rename = "C")]
    QuoteForex(WsQuoteMsg),
    /// Stock per-second aggregate
    #[serde(rename = "A")]
    AggSecondStocks(WsAggregateMsg),
    /// Stock per-minute aggregate
    #[serde(rename = "AM")]
    AggMinuteStocks(WsAggregateMsg),
    /// Crypto per-second aggregate
    #[serde(rename = "XA")]
    AggSecondCrypto(WsAggregateMsg),
    /// Crypto per-minute aggregate
    #[serde(rename = "XAM")]
    AggMinuteCrypto(WsAggregateMsg),
    /// Forex per-second aggregate
    #[serde(rename = "CA")]
    AggSecondForex(WsAggregateMsg),
    /// Forex per-minute aggregate
    #[serde(rename = "CAM")]
    AggMinuteForex(WsAggregateMsg),
    /// Status message (auth, subscription confirmations).
    /// Inner fields are populated by serde but not inspected directly; we only match the variant
    /// and return None from ws_message_to_event.
    #[serde(rename = "status")]
    #[allow(dead_code)]
    // variant matched but inner type not inspected; serde needs the type for deserialization
    Status(WsStatusMsg),
}

/// WebSocket trade message.
///
/// Stocks: `{"ev":"T","sym":"AAPL","p":150.25,"s":100,"t":1682592000000,...}`
/// Crypto: `{"ev":"XT","pair":"BTC-USD","p":45230.50,"s":0.5,"t":1682592000000,...}`
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WsTradeMsg {
    /// Symbol (stocks: "sym", crypto: "pair")
    #[serde(alias = "sym", alias = "pair")]
    pub symbol: String,

    /// Trade price
    #[serde(rename = "p", with = "rust_decimal::serde::float")]
    pub price: Decimal,

    /// Trade size
    #[serde(rename = "s", with = "rust_decimal::serde::float")]
    pub size: Decimal,

    /// Timestamp in milliseconds
    #[serde(rename = "t")]
    pub timestamp: i64,

    /// First trade condition (optional). Crypto: 1 = sell, 2 = buy.
    #[serde(
        rename = "c",
        default,
        deserialize_with = "deserialize_first_condition"
    )]
    pub condition: Option<i32>,

    /// Trade ID (optional)
    #[serde(rename = "i", default)]
    pub id: Option<String>,
}

impl WsTradeMsg {
    /// Convert to PublicTrade with exchange timestamp.
    pub fn into_public_trade(self) -> (DateTime<Utc>, PublicTrade) {
        let time = millis_to_datetime(self.timestamp);

        // Crypto condition codes: 1 = sell, 2 = buy
        let side = self.condition.and_then(|c| match c {
            1 => Some(Side::Sell),
            2 => Some(Side::Buy),
            _ => None,
        });

        let trade = PublicTrade {
            id: SmolStr::from(self.id.unwrap_or_default()),
            price: self.price,
            amount: self.size,
            side,
        };

        (time, trade)
    }
}

/// WebSocket quote message.
///
/// Stocks: `{"ev":"Q","sym":"AAPL","bp":150.20,"bs":500,"ap":150.30,"as":1000,"t":1682592000000,...}`
/// Crypto: `{"ev":"XQ","pair":"BTC-USD","bp":45220.00,"bs":2.5,"ap":45240.00,"as":3.0,"t":1682592000000,...}`
/// Forex: `{"ev":"C","p":"EUR-USD","b":1.0850,"a":1.0852,"t":1682592000000,...}`
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WsQuoteMsg {
    /// Symbol (stocks: "sym", crypto: "pair", forex: "p").
    ///
    /// The `"p"` alias is for forex quotes (`ev="C"`), where it denotes the currency pair.
    /// This is distinct from `WsTradeMsg` where `"p"` is the trade price.
    ///
    /// No canonical `#[serde(rename)]` — wire name differs per market; all real names are aliases.
    /// This struct is deserialize-only; the canonical field name is never serialized.
    #[serde(alias = "sym", alias = "pair", alias = "p")]
    pub symbol: String,

    /// Bid price (stocks/crypto: "bp", forex: "b")
    #[serde(
        alias = "bp",
        alias = "b",
        with = "rust_decimal::serde::float",
        default
    )]
    pub bid_price: Decimal,

    /// Bid size (stocks/crypto: "bs", forex: not available)
    #[serde(alias = "bs", with = "rust_decimal::serde::float_option", default)]
    pub bid_size: Option<Decimal>,

    /// Ask price (stocks/crypto: "ap", forex: "a")
    #[serde(
        alias = "ap",
        alias = "a",
        with = "rust_decimal::serde::float",
        default
    )]
    pub ask_price: Decimal,

    /// Ask size (stocks/crypto: "as", forex: not available)
    #[serde(alias = "as", with = "rust_decimal::serde::float_option", default)]
    pub ask_size: Option<Decimal>,

    /// Timestamp in milliseconds
    #[serde(rename = "t")]
    pub timestamp: i64,
}

impl WsQuoteMsg {
    /// Convert to OrderBookL1 with exchange timestamp.
    pub fn into_order_book_l1(self) -> (DateTime<Utc>, OrderBookL1) {
        let time = millis_to_datetime(self.timestamp);

        let l1 = OrderBookL1 {
            last_update_time: time,
            best_bid: Some(Level {
                price: self.bid_price,
                amount: self.bid_size.unwrap_or(Decimal::ZERO),
            }),
            best_ask: Some(Level {
                price: self.ask_price,
                amount: self.ask_size.unwrap_or(Decimal::ZERO),
            }),
        };

        (time, l1)
    }
}

/// WebSocket aggregate message.
///
/// Stocks: `{"ev":"A","sym":"AAPL","o":150.10,"h":150.50,"l":150.05,"c":150.25,"v":1000,"s":1682592000000,"e":1682592001000,...}`
/// Crypto: `{"ev":"XA","pair":"BTC-USD","o":45200.0,"h":45250.0,"l":45180.0,"c":45230.0,"v":10.5,"s":1682592000000,"e":1682592001000,...}`
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WsAggregateMsg {
    /// Symbol (stocks: "sym", crypto: "pair", forex: "pair")
    #[serde(alias = "sym", alias = "pair")]
    pub symbol: String,

    /// Open price
    #[serde(rename = "o", with = "rust_decimal::serde::float")]
    pub open: Decimal,

    /// High price
    #[serde(rename = "h", with = "rust_decimal::serde::float")]
    pub high: Decimal,

    /// Low price
    #[serde(rename = "l", with = "rust_decimal::serde::float")]
    pub low: Decimal,

    /// Close price
    #[serde(rename = "c", with = "rust_decimal::serde::float")]
    pub close: Decimal,

    /// Volume
    #[serde(rename = "v", with = "rust_decimal::serde::float")]
    pub volume: Decimal,

    /// Window start timestamp in milliseconds.
    /// Deserialized for API completeness but unused; candle uses end_timestamp.
    #[serde(rename = "s")]
    #[allow(dead_code)] // deserialized for API completeness; candle uses end_timestamp
    pub start_timestamp: i64,

    /// Window end timestamp in milliseconds
    #[serde(rename = "e")]
    pub end_timestamp: i64,

    /// Number of trades (optional)
    #[serde(rename = "z", default)]
    pub trade_count: Option<u64>,
}

impl WsAggregateMsg {
    /// Convert to Candle with exchange timestamp.
    pub fn into_candle(self) -> (DateTime<Utc>, Candle) {
        let time = millis_to_datetime(self.end_timestamp);

        let candle = Candle {
            close_time: time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: self.trade_count.unwrap_or(0),
        };

        (time, candle)
    }
}

/// WebSocket status message.
///
/// Fields are deserialized for completeness but not read directly.
/// Auth/subscribe verification uses manual JSON parsing instead.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub(crate) struct WsStatusMsg {
    /// Status: "auth_success", "auth_failed", "success", etc.
    pub status: String,

    /// Optional message
    #[serde(default)]
    pub message: Option<String>,
}

/// Convert millisecond timestamp to DateTime<Utc>.
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

/// Parse a WebSocket message JSON string, skipping unknown event types.
///
/// Massive sends messages as JSON arrays: `[{...}, {...}, ...]`
/// Each element is parsed individually; unknown event types (e.g. "lagg") are
/// logged at trace level and skipped rather than failing the entire frame.
///
/// # Why RawValue
///
/// `&serde_json::value::RawValue` is a zero-copy borrowed slice into `text`.
/// Parsing the outer array to `Vec<&RawValue>` allocates only the Vec itself
/// (one allocation per frame regardless of element count). Each element's typed
/// parse is then independent.
pub(crate) fn parse_ws_message(text: &str) -> Result<Vec<WsMessage>, MassiveError> {
    // Parse the outer array, borrowing each element as a raw JSON slice.
    let raw_elements: Vec<&serde_json::value::RawValue> =
        serde_json::from_str(text).map_err(|e| MassiveError::Deserialize {
            message: e.to_string(),
            payload: text[..text.floor_char_boundary(512)].to_owned(),
        })?;

    let mut messages = Vec::with_capacity(raw_elements.len());
    for raw in raw_elements {
        match serde_json::from_str::<WsMessage>(raw.get()) {
            Ok(msg) => messages.push(msg),
            Err(_) => {
                // Extract the "ev" field for logging without allocating a full Value DOM.
                let ev = extract_ev_tag(raw.get());
                tracing::trace!(
                    ev = ev.unwrap_or("<missing>"),
                    "Skipping unknown WS event type"
                );
            }
        }
    }
    Ok(messages)
}

/// Extract the `"ev"` field value from a JSON object string without full Value allocation.
fn extract_ev_tag(json: &str) -> Option<&str> {
    #[derive(Deserialize)]
    struct EvOnly<'a> {
        ev: &'a str,
    }
    serde_json::from_str::<EvOnly<'_>>(json).ok().map(|e| e.ev)
}

// ============================================================================
// Fair Market Value
// ============================================================================

/// Fair Market Value - a calculated mid-price from Massive.
///
/// This is a dedicated type (not mapped to PublicTrade) because FMV represents
/// a calculated value, not an actual trade execution.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct FairMarketValue {
    /// Timestamp of the FMV calculation
    pub time: DateTime<Utc>,
    /// Calculated fair market value price
    pub price: Decimal,
}

impl FairMarketValue {
    /// Create a new FairMarketValue.
    pub fn new(time: DateTime<Utc>, price: Decimal) -> Self {
        Self { time, price }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Tests should panic on unexpected values
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const SAMPLE_AGGREGATES: &str = r#"{
        "ticker": "X:BTCUSD",
        "queryCount": 2,
        "resultsCount": 2,
        "adjusted": true,
        "results": [
            {
                "v": 234.5678,
                "vw": 65432.1,
                "o": 65000.0,
                "c": 65100.0,
                "h": 65200.0,
                "l": 64900.0,
                "t": 1704067200000,
                "n": 150
            },
            {
                "v": 345.6789,
                "vw": 65150.0,
                "o": 65100.0,
                "c": 65250.0,
                "h": 65300.0,
                "l": 65050.0,
                "t": 1704067260000,
                "n": 175
            }
        ],
        "status": "OK",
        "request_id": "abc123"
    }"#;

    #[test]
    fn test_parse_aggregates() {
        let response = parse_aggregates_response(SAMPLE_AGGREGATES).unwrap();
        assert_eq!(response.ticker, Some("X:BTCUSD".to_string()));
        assert_eq!(response.results_count, 2);
        assert_eq!(response.status, "OK");

        let results = response.results.unwrap();
        assert_eq!(results.len(), 2);

        let bar = &results[0];
        assert_eq!(bar.open, dec!(65000.0));
        assert_eq!(bar.close, dec!(65100.0));
        assert_eq!(bar.high, dec!(65200.0));
        assert_eq!(bar.low, dec!(64900.0));
        assert_eq!(bar.trade_count, Some(150));
    }

    #[test]
    fn test_aggregate_bar_to_candle() {
        let bar = AggregateBar {
            open: dec!(65000.0),
            high: dec!(65200.0),
            low: dec!(64900.0),
            close: dec!(65100.0),
            volume: dec!(234.5678),
            vwap: Some(dec!(65050.0)),
            timestamp: 1704067200000,
            trade_count: Some(150),
        };

        let candle = bar.into_candle(1, "minute");

        assert_eq!(candle.open, dec!(65000.0));
        assert_eq!(candle.close, dec!(65100.0));
        assert_eq!(candle.trade_count, 150);

        // close_time should be 1 minute after start
        let expected_close =
            Utc.timestamp_millis_opt(1704067200000).single().unwrap() + Duration::minutes(1);
        assert_eq!(candle.close_time, expected_close);
    }

    #[test]
    fn test_parse_with_next_url() {
        let json = r#"{
            "ticker": "X:BTCUSD",
            "resultsCount": 50000,
            "status": "OK",
            "next_url": "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/minute/1704067200000/1704153600000?cursor=abc123",
            "results": []
        }"#;

        let response = parse_aggregates_response(json).unwrap();
        assert!(response.next_url.is_some());
        assert!(response.next_url.unwrap().contains("cursor="));
    }

    #[test]
    fn test_parse_empty_results() {
        let json = r#"{
            "ticker": "X:BTCUSD",
            "resultsCount": 0,
            "status": "OK",
            "results": []
        }"#;

        let response = parse_aggregates_response(json).unwrap();
        assert_eq!(response.results_count, 0);
        assert!(response.results.unwrap().is_empty());
    }

    #[test]
    fn test_timespan_to_duration() {
        assert_eq!(timespan_to_duration(1, "second"), Duration::seconds(1));
        assert_eq!(timespan_to_duration(5, "minute"), Duration::minutes(5));
        assert_eq!(timespan_to_duration(1, "hour"), Duration::hours(1));
        assert_eq!(timespan_to_duration(1, "day"), Duration::days(1));
        assert_eq!(timespan_to_duration(1, "week"), Duration::weeks(1));
    }

    #[test]
    fn test_parse_trades() {
        let json = r#"{
            "results": [
                {
                    "conditions": [2],
                    "exchange": 1,
                    "id": "12345",
                    "participant_timestamp": 1704067200000000000,
                    "price": 65100.50,
                    "size": 0.5,
                    "sip_timestamp": 1704067200001000000
                }
            ],
            "status": "OK",
            "resultsCount": 1
        }"#;

        let response = parse_trades_response(json).unwrap();
        assert_eq!(response.results_count, 1);

        let results = response.results.unwrap();
        let trade = &results[0];
        assert_eq!(trade.price, dec!(65100.50));
        assert_eq!(trade.size, dec!(0.5));
        assert_eq!(trade.first_condition, Some(2)); // buy-initiated (crypto)

        let public_trade = results.into_iter().next().unwrap().into_public_trade();
        assert_eq!(public_trade.side, Some(Side::Buy));
    }

    #[test]
    fn test_parse_quotes() {
        let json = r#"{
            "results": [
                {
                    "ask_price": 65200.0,
                    "ask_size": 1.5,
                    "bid_price": 65100.0,
                    "bid_size": 2.0,
                    "participant_timestamp": 1704067200000000000,
                    "sip_timestamp": 1704067200001000000
                }
            ],
            "status": "OK",
            "resultsCount": 1
        }"#;

        let response = parse_quotes_response(json).unwrap();
        assert_eq!(response.results_count, 1);

        let results = response.results.unwrap();
        let quote = results.into_iter().next().unwrap();
        let l1 = quote.into_order_book_l1();

        assert_eq!(l1.best_bid.unwrap().price, dec!(65100.0));
        assert_eq!(l1.best_ask.unwrap().price, dec!(65200.0));
    }

    #[test]
    fn test_fair_market_value() {
        let fmv = FairMarketValue::new(
            Utc.timestamp_millis_opt(1704067200000).single().unwrap(),
            dec!(65150.0),
        );
        assert_eq!(fmv.price, dec!(65150.0));
    }

    // ========================================================================
    // WebSocket Message Tests
    // ========================================================================

    #[test]
    fn test_parse_ws_crypto_trade() {
        let json = r#"[{
            "ev": "XT",
            "pair": "BTC-USD",
            "p": 45230.50,
            "s": 0.5,
            "t": 1704067200000,
            "c": [2],
            "i": "trade123"
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::TradeCrypto(trade) => {
                assert_eq!(trade.symbol, "BTC-USD");
                assert_eq!(trade.price, dec!(45230.50));
                assert_eq!(trade.size, dec!(0.5));
                assert_eq!(trade.condition, Some(2));
            }
            _ => panic!("Expected crypto trade message"),
        }
    }

    #[test]
    fn test_parse_ws_stock_trade() {
        let json = r#"[{
            "ev": "T",
            "sym": "AAPL",
            "p": 150.25,
            "s": 100,
            "t": 1704067200000,
            "c": [],
            "i": "12345"
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::TradeStocks(trade) => {
                assert_eq!(trade.symbol, "AAPL");
                assert_eq!(trade.price, dec!(150.25));
                assert_eq!(trade.size, dec!(100));
            }
            _ => panic!("Expected stock trade message"),
        }
    }

    #[test]
    fn test_parse_ws_crypto_quote() {
        let json = r#"[{
            "ev": "XQ",
            "pair": "BTC-USD",
            "bp": 45220.00,
            "bs": 2.5,
            "ap": 45240.00,
            "as": 3.0,
            "t": 1704067200000
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::QuoteCrypto(quote) => {
                assert_eq!(quote.symbol, "BTC-USD");
                assert_eq!(quote.bid_price, dec!(45220.00));
                assert_eq!(quote.ask_price, dec!(45240.00));
            }
            _ => panic!("Expected crypto quote message"),
        }
    }

    #[test]
    fn test_parse_ws_forex_quote() {
        let json = r#"[{
            "ev": "C",
            "p": "EUR-USD",
            "b": 1.0850,
            "a": 1.0852,
            "t": 1704067200000
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::QuoteForex(quote) => {
                assert_eq!(quote.symbol, "EUR-USD");
                assert_eq!(quote.bid_price, dec!(1.0850));
                assert_eq!(quote.ask_price, dec!(1.0852));
            }
            _ => panic!("Expected forex quote message"),
        }
    }

    #[test]
    fn test_parse_ws_aggregate() {
        let json = r#"[{
            "ev": "XAM",
            "pair": "BTC-USD",
            "o": 45200.0,
            "h": 45250.0,
            "l": 45180.0,
            "c": 45230.0,
            "v": 10.5,
            "s": 1704067200000,
            "e": 1704067260000,
            "z": 150
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::AggMinuteCrypto(agg) => {
                assert_eq!(agg.symbol, "BTC-USD");
                assert_eq!(agg.open, dec!(45200.0));
                assert_eq!(agg.high, dec!(45250.0));
                assert_eq!(agg.low, dec!(45180.0));
                assert_eq!(agg.close, dec!(45230.0));
                assert_eq!(agg.volume, dec!(10.5));
                assert_eq!(agg.trade_count, Some(150));
            }
            _ => panic!("Expected crypto aggregate message"),
        }
    }

    #[test]
    fn test_parse_ws_status() {
        let json = r#"[{
            "ev": "status",
            "status": "auth_success",
            "message": "authenticated"
        }]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            WsMessage::Status(status) => {
                assert_eq!(status.status, "auth_success");
                assert_eq!(status.message, Some("authenticated".to_string()));
            }
            _ => panic!("Expected status message"),
        }
    }

    #[test]
    fn test_parse_ws_multiple_messages() {
        let json = r#"[
            {"ev": "XT", "pair": "BTC-USD", "p": 45230.50, "s": 0.5, "t": 1704067200000, "c": []},
            {"ev": "XQ", "pair": "BTC-USD", "bp": 45220.0, "bs": 2.5, "ap": 45240.0, "as": 3.0, "t": 1704067200000}
        ]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(matches!(&messages[0], WsMessage::TradeCrypto(_)));
        assert!(matches!(&messages[1], WsMessage::QuoteCrypto(_)));
    }

    #[test]
    fn test_ws_trade_to_public_trade() {
        let trade = WsTradeMsg {
            symbol: "BTC-USD".to_string(),
            price: dec!(45230.50),
            size: dec!(0.5),
            timestamp: 1704067200000,
            condition: Some(2),
            id: Some("trade123".to_string()),
        };

        let (time, public_trade) = trade.into_public_trade();

        assert_eq!(public_trade.price, dec!(45230.50));
        assert_eq!(public_trade.amount, dec!(0.5));
        assert_eq!(public_trade.side, Some(Side::Buy));
        assert_eq!(
            time,
            Utc.timestamp_millis_opt(1704067200000).single().unwrap()
        );
    }

    #[test]
    fn test_ws_quote_to_order_book_l1() {
        let quote = WsQuoteMsg {
            symbol: "BTC-USD".to_string(),
            bid_price: dec!(45220.00),
            bid_size: Some(dec!(2.5)),
            ask_price: dec!(45240.00),
            ask_size: Some(dec!(3.0)),
            timestamp: 1704067200000,
        };

        let (time, l1) = quote.into_order_book_l1();

        assert_eq!(l1.best_bid.unwrap().price, dec!(45220.00));
        assert_eq!(l1.best_bid.unwrap().amount, dec!(2.5));
        assert_eq!(l1.best_ask.unwrap().price, dec!(45240.00));
        assert_eq!(l1.best_ask.unwrap().amount, dec!(3.0));
        assert_eq!(
            time,
            Utc.timestamp_millis_opt(1704067200000).single().unwrap()
        );
    }

    #[test]
    fn test_ws_aggregate_to_candle() {
        let agg = WsAggregateMsg {
            symbol: "BTC-USD".to_string(),
            open: dec!(45200.0),
            high: dec!(45250.0),
            low: dec!(45180.0),
            close: dec!(45230.0),
            volume: dec!(10.5),
            start_timestamp: 1704067200000,
            end_timestamp: 1704067260000,
            trade_count: Some(150),
        };

        let (time, candle) = agg.into_candle();

        assert_eq!(candle.open, dec!(45200.0));
        assert_eq!(candle.high, dec!(45250.0));
        assert_eq!(candle.low, dec!(45180.0));
        assert_eq!(candle.close, dec!(45230.0));
        assert_eq!(candle.volume, dec!(10.5));
        assert_eq!(candle.trade_count, 150);
        assert_eq!(
            time,
            Utc.timestamp_millis_opt(1704067260000).single().unwrap()
        );
    }

    #[test]
    fn test_parse_ws_unknown_event_skipped() {
        // A frame with known trades plus an unknown "lagg" event.
        // The trades must be returned; "lagg" must be silently skipped.
        let json = r#"[
            {"ev":"XT","pair":"BTC-USD","p":45000.0,"s":0.1,"t":1704067200000,"c":[]},
            {"ev":"lagg","data":"some lag info"},
            {"ev":"XT","pair":"ETH-USD","p":2500.0,"s":1.0,"t":1704067200001,"c":[]}
        ]"#;

        let messages = parse_ws_message(json).unwrap();
        assert_eq!(messages.len(), 2, "expected 2 known messages, lagg skipped");
        assert!(matches!(&messages[0], WsMessage::TradeCrypto(t) if t.symbol == "BTC-USD"));
        assert!(matches!(&messages[1], WsMessage::TradeCrypto(t) if t.symbol == "ETH-USD"));
    }

    #[test]
    fn test_parse_ws_malformed_outer_array_is_error() {
        // Malformed JSON is a hard error, not a skip.
        let result = parse_ws_message("{not an array}");
        assert!(result.is_err());
    }
}
