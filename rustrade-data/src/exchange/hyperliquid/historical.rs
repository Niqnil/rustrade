//! Historical data fetcher for Hyperliquid.
//!
//! Provides one-shot historical candle data via `HyperliquidHistoricalData::fetch_candles()`.
//! This is a separate utility API, not part of the WebSocket streaming connector.
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::hyperliquid::historical::{
//!     HyperliquidHistoricalData, HistoricalRequest,
//! };
//! use rustrade_data::subscription::candle::CandleInterval;
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

use crate::{
    error::DataError,
    subscription::candle::{Candle, CandleInterval, close_time_from_open, open_time_from_close},
};
use chrono::{DateTime, TimeZone, Utc};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient};
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;
use tracing::debug;

/// Historical data fetcher for Hyperliquid.
///
/// Wraps the SDK's `InfoClient` for fetching historical OHLCV candles.
///
/// Not every [`CandleInterval`] variant is supported — Hyperliquid's
/// `candleSnapshot` API rejects some resolutions; see [`fetch_candles`] for the
/// per-interval availability contract.
///
/// [`fetch_candles`]: Self::fetch_candles
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
    /// # Range contract
    ///
    /// Returns exactly the candles whose exclusive `close_time` falls within
    /// `[request.start_time, request.end_time]` (both inclusive) — matched on
    /// `close_time`, the field consumers receive (see
    /// [`Candle::close_time`](crate::subscription::candle::Candle)). Hyperliquid's
    /// API natively filters by the candle's open-time bucket, so this method
    /// widens the request by one interval and trims the result by `close_time` to
    /// honour the contract uniformly with the library's other historical fetches.
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
    /// Returns [`DataError::UnsupportedInterval`] if the requested interval is not
    /// supported by Hyperliquid (`Sec1`/`Hour6` — the shared [`CandleInterval`] is
    /// a venue-agnostic union, so this guard is required: the typed-path validator
    /// checks instrument kind only, not interval). Returns `DataError::Socket` if
    /// the API request fails or a candle's `close_time` cannot be computed
    /// (overflow).
    pub async fn fetch_candles(
        &self,
        request: HistoricalRequest,
    ) -> Result<Vec<Candle>, DataError> {
        // Hyperliquid does not offer 1-second or 6-hour candles. The shared
        // `CandleInterval` is the union across all venues, so reject the variants
        // Hyperliquid cannot serve before they reach the API (which would 400).
        ensure_hyperliquid_supports(request.interval)?;

        debug!(
            coin = %request.coin,
            interval = %request.interval.as_str(),
            start = %request.start_time,
            end = %request.end_time,
            "Fetching historical candles"
        );

        let step = request.interval.to_step();

        // Range contract: callers receive candles whose `close_time ∈ [start, end]`.
        // Hyperliquid filters by the candle's native open-time bucket
        // `[open, open + interval)`, NOT by our normalised `close_time`, so the
        // candle whose `close_time == start` (open == start − interval) would be
        // dropped. Widen the request lower bound by one interval to capture it,
        // then trim the result by `close_time` below.
        // `None` (underflow near DateTime::MIN_UTC) is not an error: the boundary
        // candle would have an unrepresentable open and so cannot exist, making the
        // un-widened bound already correct. See `open_time_from_close`.
        let request_start =
            open_time_from_close(request.start_time, step).unwrap_or(request.start_time);

        #[allow(clippy::cast_sign_loss)] // Timestamps after 1970 are always positive
        let start_ms = request_start.timestamp_millis() as u64;
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
            let candle = sdk_candle_to_candle(&sdk_candle, request.interval)?;
            // Trim to the close_time range contract (the venue may return one
            // extra candle past either boundary due to open-bucket filtering).
            if candle.close_time >= request.start_time && candle.close_time <= request.end_time {
                candles.push(candle);
            }
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

/// Reject [`CandleInterval`] variants that Hyperliquid cannot serve.
///
/// The shared [`CandleInterval`] is the venue-agnostic union across all
/// exchanges, so each venue must guard the intervals it does not support before
/// they reach the API (Hyperliquid 400s on `Sec1`/`Hour6`). The typed-path
/// validator checks instrument kind only, never the interval, so this guard is
/// the sole gate.
///
/// The match is intentionally exhaustive (no `_` arm): adding a variant to
/// [`CandleInterval`] is a compile error here, forcing a conscious decision on
/// whether Hyperliquid supports it rather than silently passing it to the API.
fn ensure_hyperliquid_supports(interval: CandleInterval) -> Result<(), DataError> {
    match interval {
        CandleInterval::Sec1 | CandleInterval::Hour6 => Err(DataError::UnsupportedInterval {
            // 1s/6h are unsupported across the whole Hyperliquid candleSnapshot
            // API (perp and spot alike); this client is coin-keyed, not
            // venue-keyed, so `HyperliquidPerp` is a representative venue label.
            exchange: ExchangeId::HyperliquidPerp,
            interval,
        }),
        CandleInterval::Min1
        | CandleInterval::Min3
        | CandleInterval::Min5
        | CandleInterval::Min15
        | CandleInterval::Min30
        | CandleInterval::Hour1
        | CandleInterval::Hour2
        | CandleInterval::Hour4
        | CandleInterval::Hour8
        | CandleInterval::Hour12
        | CandleInterval::Day1
        | CandleInterval::Day3
        | CandleInterval::Week1
        | CandleInterval::Month1 => Ok(()),
    }
}

/// Convert SDK's `CandlesSnapshotResponse` to rustrade's `Candle`.
///
/// `close_time` is computed library-side as the exclusive end-of-period boundary
/// (`time_open + interval`) via the shared [`close_time_from_open`] helper — the
/// venue's raw `time_close` is **discarded** because Hyperliquid reports it as
/// `period-end − 1ms` (the inclusive-last-ms convention, verified against the live
/// `candleSnapshot` API), which does not satisfy the [`Candle`] contract.
fn sdk_candle_to_candle(
    sdk: &hyperliquid_rust_sdk::CandlesSnapshotResponse,
    interval: CandleInterval,
) -> Result<Candle, DataError> {
    let open_time = Utc
        .timestamp_millis_opt(sdk.time_open as i64)
        .single()
        .ok_or_else(|| {
            DataError::Socket(format!(
                "Hyperliquid open timestamp {} out of range",
                sdk.time_open
            ))
        })?;

    let close_time = close_time_from_open(open_time, interval.to_step()).ok_or_else(|| {
        DataError::Socket(format!(
            "Hyperliquid candle close_time overflow: open={open_time}, interval={}",
            interval.as_str()
        ))
    })?;

    let open = sdk
        .open
        .parse::<Decimal>()
        .map_err(|e| DataError::Socket(format!("parse open: {e}")))?;
    let high = sdk
        .high
        .parse::<Decimal>()
        .map_err(|e| DataError::Socket(format!("parse high: {e}")))?;
    let low = sdk
        .low
        .parse::<Decimal>()
        .map_err(|e| DataError::Socket(format!("parse low: {e}")))?;
    let close = sdk
        .close
        .parse::<Decimal>()
        .map_err(|e| DataError::Socket(format!("parse close: {e}")))?;
    let volume = sdk
        .vlm
        .parse::<Decimal>()
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
    fn ensure_hyperliquid_supports_rejects_sec1_and_hour6() {
        // `Sec1`/`Hour6` exist on the shared union but Hyperliquid cannot serve
        // them — the guard must reject them with a typed error.
        for interval in [CandleInterval::Sec1, CandleInterval::Hour6] {
            let err = ensure_hyperliquid_supports(interval).unwrap_err();
            assert!(
                matches!(err, DataError::UnsupportedInterval { interval: i, .. } if i == interval),
                "expected UnsupportedInterval for {interval}, got {err:?}"
            );
        }
    }

    #[test]
    fn ensure_hyperliquid_supports_accepts_the_rest() {
        // Every variant except the two Hyperliquid cannot serve must be accepted.
        for interval in CandleInterval::ALL {
            let supported = !matches!(interval, CandleInterval::Sec1 | CandleInterval::Hour6);
            assert_eq!(
                ensure_hyperliquid_supports(interval).is_ok(),
                supported,
                "interval {interval}"
            );
        }
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
    fn test_sdk_candle_to_candle_normalises_close_time_from_open() {
        use rust_decimal_macros::dec;

        // Hyperliquid reports `time_close` as `time_open + interval − 1ms` (the
        // inclusive-last-ms convention, verified against the live API). The
        // library must IGNORE that raw value and compute the exclusive boundary
        // `time_open + interval`. Here time_open = 2024-01-01 00:00:00 UTC and the
        // raw venue time_close is the `−1ms` value, which must NOT be used.
        let sdk_candle = hyperliquid_rust_sdk::CandlesSnapshotResponse {
            time_open: 1_704_067_200_000,  // 2024-01-01 00:00:00.000 UTC
            time_close: 1_704_070_799_999, // 2024-01-01 00:59:59.999 UTC (1h − 1ms; ignored)
            coin: "BTC".to_string(),
            candle_interval: "1h".to_string(),
            open: "45000.5".to_string(),
            high: "45500.0".to_string(),
            low: "44800.0".to_string(),
            close: "45250.0".to_string(),
            vlm: "1234.56".to_string(),
            num_trades: 5000,
        };

        let candle = sdk_candle_to_candle(&sdk_candle, CandleInterval::Hour1).unwrap();

        // Normalised boundary: exactly time_open + 1h, NOT the raw −1ms value.
        assert_eq!(candle.close_time.timestamp_millis(), 1_704_070_800_000);
        assert_eq!(candle.open, dec!(45000.5));
        assert_eq!(candle.high, dec!(45500.0));
        assert_eq!(candle.low, dec!(44800.0));
        assert_eq!(candle.close, dec!(45250.0));
        assert_eq!(candle.volume, dec!(1234.56));
        assert_eq!(candle.trade_count, 5000);
    }

    #[test]
    fn test_sdk_candle_to_candle_monthly_calendar_boundary() {
        // A January monthly bar must close at Feb 1 00:00 UTC via calendar
        // arithmetic (Months(1)), not open + 30 days.
        let sdk_candle = hyperliquid_rust_sdk::CandlesSnapshotResponse {
            time_open: 1_704_067_200_000,  // 2024-01-01 00:00:00 UTC
            time_close: 1_706_745_599_999, // raw venue value (ignored)
            coin: "BTC".to_string(),
            candle_interval: "1M".to_string(),
            open: "45000.0".to_string(),
            high: "45000.0".to_string(),
            low: "45000.0".to_string(),
            close: "45000.0".to_string(),
            vlm: "1.0".to_string(),
            num_trades: 1,
        };

        let candle = sdk_candle_to_candle(&sdk_candle, CandleInterval::Month1).unwrap();

        // 2024-02-01 00:00:00 UTC.
        assert_eq!(candle.close_time.timestamp_millis(), 1_706_745_600_000);
    }
}
