//! Historical data fetcher for Hyperliquid.
//!
//! Provides one-shot historical candle data via `HyperliquidHistoricalData::fetch_candles()`.
//! This is a separate utility API, not part of the WebSocket streaming connector.
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::hyperliquid::historical::{
//!     HyperliquidHistoricalData, HistoricalRequest, CandleInterval,
//! };
//! use chrono::{Duration, Utc};
//!
//! let client = HyperliquidHistoricalData::new(false).await?; // mainnet
//!
//! let request = HistoricalRequest {
//!     coin: "BTC".to_string(),
//!     interval: CandleInterval::Hour1,
//!     start_time: Utc::now() - Duration::days(7),
//!     end_time: Utc::now(),
//! };
//!
//! let candles = client.fetch_candles(request).await?;
//! ```

use crate::{error::DataError, subscription::candle::Candle};
use chrono::{DateTime, TimeZone, Utc};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient};
use tracing::debug;

/// Historical data fetcher for Hyperliquid.
///
/// Wraps the SDK's `InfoClient` for fetching historical OHLCV candles.
#[derive(Debug)]
pub struct HyperliquidHistoricalData {
    client: InfoClient,
}

impl HyperliquidHistoricalData {
    /// Create a new historical data client.
    ///
    /// # Arguments
    ///
    /// * `testnet` - If true, connect to testnet; otherwise mainnet.
    ///
    /// # Errors
    ///
    /// Returns error if client creation fails.
    pub async fn new(testnet: bool) -> Result<Self, DataError> {
        let base_url = if testnet {
            Some(BaseUrl::Testnet)
        } else {
            None
        };
        let client = InfoClient::new(None, base_url)
            .await
            .map_err(|e| DataError::Socket(format!("InfoClient creation: {e}")))?;
        Ok(Self { client })
    }

    /// Create from an existing `InfoClient`.
    ///
    /// Useful for sharing a client with other code or custom configuration.
    pub fn from_client(client: InfoClient) -> Self {
        Self { client }
    }

    /// Fetch historical candles for the given request.
    ///
    /// # Arguments
    ///
    /// * `request` - Historical data request parameters
    ///
    /// # Returns
    ///
    /// Vector of candles in chronological order (oldest first).
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if the API request fails.
    pub async fn fetch_candles(
        &self,
        request: HistoricalRequest,
    ) -> Result<Vec<Candle>, DataError> {
        debug!(
            coin = %request.coin,
            interval = %request.interval.as_str(),
            start = %request.start_time,
            end = %request.end_time,
            "Fetching historical candles"
        );

        #[allow(clippy::cast_sign_loss)] // Timestamps after 1970 are always positive
        let start_ms = request.start_time.timestamp_millis() as u64;
        #[allow(clippy::cast_sign_loss)] // Timestamps after 1970 are always positive
        let end_ms = request.end_time.timestamp_millis() as u64;

        let response = self
            .client
            .candles_snapshot(
                request.coin.clone(),
                request.interval.as_str().to_string(),
                start_ms,
                end_ms,
            )
            .await
            .map_err(|e| DataError::Socket(format!("Hyperliquid candles: {e}")))?;

        let mut candles = Vec::with_capacity(response.len());
        for sdk_candle in response {
            candles.push(sdk_candle_to_candle(&sdk_candle)?);
        }

        debug!(coin = %request.coin, count = candles.len(), "Received historical candles");

        Ok(candles)
    }
}

/// Parameters for a historical candle request.
#[derive(Debug, Clone)]
pub struct HistoricalRequest {
    /// Asset symbol (e.g., "BTC", "ETH").
    pub coin: String,

    /// Candle interval/resolution.
    pub interval: CandleInterval,

    /// Start time for the data range (inclusive).
    pub start_time: DateTime<Utc>,

    /// End time for the data range (inclusive).
    pub end_time: DateTime<Utc>,
}

impl HistoricalRequest {
    /// Create a request for hourly candles over the last N days.
    pub fn hourly(coin: impl Into<String>, days: i64) -> Self {
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::days(days);
        Self {
            coin: coin.into(),
            interval: CandleInterval::Hour1,
            start_time,
            end_time,
        }
    }

    /// Create a request for daily candles over the last N days.
    pub fn daily(coin: impl Into<String>, days: i64) -> Self {
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::days(days);
        Self {
            coin: coin.into(),
            interval: CandleInterval::Day1,
            start_time,
            end_time,
        }
    }
}

/// Candle interval/resolution for historical data requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandleInterval {
    /// 1 minute
    Min1,
    /// 3 minutes
    Min3,
    /// 5 minutes
    Min5,
    /// 15 minutes
    Min15,
    /// 30 minutes
    Min30,
    /// 1 hour
    Hour1,
    /// 2 hours
    Hour2,
    /// 4 hours
    Hour4,
    /// 8 hours
    Hour8,
    /// 12 hours
    Hour12,
    /// 1 day
    Day1,
    /// 3 days
    Day3,
    /// 1 week
    Week1,
    /// 1 month
    Month1,
}

impl CandleInterval {
    /// Get the string representation for the API.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Min1 => "1m",
            Self::Min3 => "3m",
            Self::Min5 => "5m",
            Self::Min15 => "15m",
            Self::Min30 => "30m",
            Self::Hour1 => "1h",
            Self::Hour2 => "2h",
            Self::Hour4 => "4h",
            Self::Hour8 => "8h",
            Self::Hour12 => "12h",
            Self::Day1 => "1d",
            Self::Day3 => "3d",
            Self::Week1 => "1w",
            Self::Month1 => "1M",
        }
    }
}

/// Convert SDK's `CandlesSnapshotResponse` to rustrade's `Candle`.
fn sdk_candle_to_candle(
    sdk: &hyperliquid_rust_sdk::CandlesSnapshotResponse,
) -> Result<Candle, DataError> {
    let close_time = Utc
        .timestamp_millis_opt(sdk.time_close as i64)
        .single()
        .ok_or_else(|| {
            DataError::Socket(format!(
                "Hyperliquid timestamp {} out of range",
                sdk.time_close
            ))
        })?;

    let open = sdk
        .open
        .parse::<f64>()
        .map_err(|e| DataError::Socket(format!("parse open: {e}")))?;
    let high = sdk
        .high
        .parse::<f64>()
        .map_err(|e| DataError::Socket(format!("parse high: {e}")))?;
    let low = sdk
        .low
        .parse::<f64>()
        .map_err(|e| DataError::Socket(format!("parse low: {e}")))?;
    let close = sdk
        .close
        .parse::<f64>()
        .map_err(|e| DataError::Socket(format!("parse close: {e}")))?;
    let volume = sdk
        .vlm
        .parse::<f64>()
        .map_err(|e| DataError::Socket(format!("parse volume: {e}")))?;

    Ok(Candle {
        close_time,
        open,
        high,
        low,
        close,
        volume,
        trade_count: sdk.num_trades,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_candle_interval_as_str() {
        assert_eq!(CandleInterval::Min1.as_str(), "1m");
        assert_eq!(CandleInterval::Hour1.as_str(), "1h");
        assert_eq!(CandleInterval::Day1.as_str(), "1d");
        assert_eq!(CandleInterval::Month1.as_str(), "1M");
    }

    #[test]
    fn test_historical_request_hourly_builder() {
        let request = HistoricalRequest::hourly("BTC", 7);
        assert_eq!(request.coin, "BTC");
        assert_eq!(request.interval, CandleInterval::Hour1);
        assert!(request.start_time < request.end_time);
    }

    #[test]
    fn test_historical_request_daily_builder() {
        let request = HistoricalRequest::daily("ETH", 30);
        assert_eq!(request.coin, "ETH");
        assert_eq!(request.interval, CandleInterval::Day1);
    }

    #[test]
    fn test_sdk_candle_to_candle() {
        let sdk_candle = hyperliquid_rust_sdk::CandlesSnapshotResponse {
            time_open: 1704067200000,
            time_close: 1704070800000,
            coin: "BTC".to_string(),
            candle_interval: "1h".to_string(),
            open: "45000.5".to_string(),
            high: "45500.0".to_string(),
            low: "44800.0".to_string(),
            close: "45250.0".to_string(),
            vlm: "1234.56".to_string(),
            num_trades: 5000,
        };

        let candle = sdk_candle_to_candle(&sdk_candle).unwrap();

        assert_eq!(candle.open, 45000.5);
        assert_eq!(candle.high, 45500.0);
        assert_eq!(candle.low, 44800.0);
        assert_eq!(candle.close, 45250.0);
        assert_eq!(candle.volume, 1234.56);
        assert_eq!(candle.trade_count, 5000);
    }
}
