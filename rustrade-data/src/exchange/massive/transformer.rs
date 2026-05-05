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

/// Deserialize only the first element of a conditions array.
fn deserialize_first_condition<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let conditions: Option<Vec<i32>> = Option::deserialize(deserializer)?;
    Ok(conditions.and_then(|v| v.into_iter().next()))
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

    /// Ask size
    #[serde(rename = "ask_size", with = "rust_decimal::serde::float")]
    pub ask_size: Decimal,

    /// Bid price
    #[serde(rename = "bid_price", with = "rust_decimal::serde::float")]
    pub bid_price: Decimal,

    /// Bid size
    #[serde(rename = "bid_size", with = "rust_decimal::serde::float")]
    pub bid_size: Decimal,

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
    pub fn into_order_book_l1(self) -> OrderBookL1 {
        let timestamp = self.timestamp();

        OrderBookL1 {
            last_update_time: timestamp,
            best_bid: Some(Level {
                price: self.bid_price,
                amount: self.bid_size,
            }),
            best_ask: Some(Level {
                price: self.ask_price,
                amount: self.ask_size,
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
}
