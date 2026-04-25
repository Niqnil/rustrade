//! Historical data fetcher for Interactive Brokers.
//!
//! Provides one-shot historical bar data via `IbkrHistoricalData::fetch_candles()`.
//! This is a separate utility API, not part of the streaming [`IbkrMarketStream`].
//!
//! # Example
//!
//! ```ignore
//! use barter_data::exchange::ibkr::historical::{IbkrHistoricalData, HistoricalRequest};
//! use ibapi::market_data::historical::{BarSize, WhatToShow, ToDuration};
//!
//! let client = IbkrHistoricalData::connect("127.0.0.1:4002", 101)?;
//! let contract = ibapi::contracts::Contract::stock("AAPL").build();
//!
//! let request = HistoricalRequest {
//!     contract,
//!     end_date: None, // Current time
//!     duration: 30.days(),
//!     bar_size: BarSize::Day,
//!     what_to_show: WhatToShow::Trades,
//!     regular_trading_hours_only: true,
//! };
//!
//! let candles = client.fetch_candles(request).await?;
//! ```
//!
//! [`IbkrMarketStream`]: super::IbkrMarketStream

use crate::{error::DataError, subscription::candle::Candle};
use chrono::DateTime;
use ibapi::{
    client::blocking::Client,
    contracts::Contract,
    market_data::{
        TradingHours,
        historical::{BarSize, Duration, WhatToShow},
    },
};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info};

/// Re-export commonly used types from ibapi for caller convenience.
///
/// Note: Importing this trait couples your code to ibapi's API surface.
/// This is intentional — the trait provides `.days()`, `.months()` etc.
/// syntax needed for `HistoricalRequest::duration`.
pub use ibapi::market_data::historical::ToDuration;

/// Historical data fetcher for Interactive Brokers.
///
/// Wraps an IB client connection for fetching historical OHLCV bars.
/// Unlike [`IbkrMarketStream`], this is a one-shot request/response API.
///
/// [`IbkrMarketStream`]: super::IbkrMarketStream
#[derive(Debug)]
pub struct IbkrHistoricalData {
    client: Arc<Client>,
}

impl IbkrHistoricalData {
    /// Connect to TWS/Gateway for historical data requests.
    ///
    /// # Arguments
    ///
    /// * `url` - Host and port (e.g., "127.0.0.1:4002" for Gateway paper)
    /// * `client_id` - Unique client ID (must differ from other connections)
    ///
    /// # Errors
    ///
    /// Returns error if connection fails.
    pub fn connect(url: &str, client_id: i32) -> Result<Self, DataError> {
        info!(%url, client_id, "Connecting to IB for historical data");

        let client = Client::connect(url, client_id)
            .map_err(|e| DataError::Socket(format!("IB connect: {e}")))?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Create from an existing client connection.
    ///
    /// # Shared Connection Hazards
    ///
    /// IB multiplexes all requests over a single TCP connection per [`Client`].
    /// Sharing this `Arc<Client>` with an active streaming subscription (quotes,
    /// trades, depth via [`IbkrMarketStream`]) will contend on the same wire.
    /// Concurrent historical requests may also trigger IB pacing violations
    /// (error 162: "Historical data request pacing violation").
    ///
    /// For production use, prefer a dedicated [`Client`] connection (separate
    /// `client_id`) for historical data fetching.
    ///
    /// [`IbkrMarketStream`]: super::IbkrMarketStream
    pub fn from_client(client: Arc<Client>) -> Self {
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
    /// Returns `DataError::Socket` if:
    /// - IB rejects the request (invalid contract, pacing violation, no data permission)
    /// - Network error during request
    ///
    /// Note: `DataError::Socket` is used for all IB API errors because `ibapi`
    /// errors are unstructured strings. Callers cannot distinguish transient
    /// network errors from permanent API rejections.
    ///
    /// # Notes
    ///
    /// - IB rate-limits historical data requests (pacing violations)
    /// - Some data requires paid market data subscriptions
    /// - Volume and trade count are only available for `WhatToShow::Trades`
    pub async fn fetch_candles(
        &self,
        request: HistoricalRequest,
    ) -> Result<Vec<Candle>, DataError> {
        let client = self.client.clone();
        let symbol = request.contract.symbol.clone();

        debug!(
            symbol = %symbol,
            bar_size = ?request.bar_size,
            duration = ?request.duration,
            "Fetching historical data"
        );

        let candles = tokio::task::spawn_blocking(move || {
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };

            let historical_data = client
                .historical_data(
                    &request.contract,
                    request.end_date,
                    request.duration,
                    request.bar_size,
                    request.what_to_show,
                    trading_hours,
                )
                .map_err(|e| DataError::Socket(format!("historical data: {e}")))?;

            let mut candles = Vec::with_capacity(historical_data.bars.len());
            for bar in &historical_data.bars {
                candles.push(bar_to_candle(bar)?);
            }

            Ok::<_, DataError>(candles)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("historical_data task panicked: {e}"))
            } else {
                DataError::Socket(format!("historical_data task cancelled: {e}"))
            }
        })??;

        debug!(symbol = %symbol, count = candles.len(), "Received historical bars");

        Ok(candles)
    }
}

/// Parameters for a historical data request.
#[derive(Debug, Clone)]
pub struct HistoricalRequest {
    /// IB contract to fetch data for.
    pub contract: Contract,

    /// End date/time for the data range.
    ///
    /// Pass `None` to use the current time.
    ///
    /// Requires the `time` crate for constructing `OffsetDateTime` values.
    pub end_date: Option<OffsetDateTime>,

    /// Duration of data to fetch (e.g., `30.days()`, `1.years()`).
    ///
    /// Use the [`ToDuration`] trait for convenient construction.
    pub duration: Duration,

    /// Bar size/resolution (e.g., `BarSize::Day`, `BarSize::Hour`).
    pub bar_size: BarSize,

    /// What prices to use for bar construction.
    ///
    /// - `Trades`: Trade prices (includes volume and count)
    /// - `MidPoint`: Bid/ask midpoint
    /// - `Bid`/`Ask`: Bid or ask prices only
    pub what_to_show: WhatToShow,

    /// If true, only include data from regular trading hours.
    ///
    /// When false, includes pre-market and after-hours data.
    pub regular_trading_hours_only: bool,
}

impl HistoricalRequest {
    /// Create a request for daily trade bars.
    ///
    /// Convenience constructor for the most common use case.
    pub fn daily_trades(contract: Contract, days: i32) -> Self {
        Self {
            contract,
            end_date: None,
            duration: days.days(),
            bar_size: BarSize::Day,
            what_to_show: WhatToShow::Trades,
            regular_trading_hours_only: true,
        }
    }
}

/// Convert an ibapi `Bar` to a barter `Candle`.
///
/// Note: `Bar::wap` (volume-weighted average price) is not mapped to `Candle`
/// as barter's `Candle` type does not include VWAP.
fn bar_to_candle(bar: &ibapi::market_data::historical::Bar) -> Result<Candle, DataError> {
    let close_time = DateTime::from_timestamp(bar.date.unix_timestamp(), bar.date.nanosecond())
        .ok_or_else(|| {
            DataError::Socket(format!(
                "IB timestamp {} out of DateTime<Utc> range",
                bar.date.unix_timestamp()
            ))
        })?;

    Ok(Candle {
        close_time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        #[allow(clippy::cast_sign_loss)] // IB returns -1 when unavailable; .max(0) guarantees non-negative
        trade_count: bar.count.max(0) as u64,
    })
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use time::macros::datetime;

    #[test]
    fn bar_to_candle_converts_all_fields() {
        let bar = ibapi::market_data::historical::Bar {
            date: datetime!(2024-01-15 16:00 UTC),
            open: 150.0,
            high: 155.0,
            low: 149.0,
            close: 153.5,
            volume: 1_000_000.0,
            wap: 152.0,
            count: 50_000,
        };

        let candle = bar_to_candle(&bar).unwrap();

        assert_eq!(candle.open, 150.0);
        assert_eq!(candle.high, 155.0);
        assert_eq!(candle.low, 149.0);
        assert_eq!(candle.close, 153.5);
        assert_eq!(candle.volume, 1_000_000.0);
        assert_eq!(candle.trade_count, 50_000);

        // Check timestamp conversion
        assert_eq!(candle.close_time.year(), 2024);
        assert_eq!(candle.close_time.month(), 1); // January = 1
        assert_eq!(candle.close_time.day(), 15);
        assert_eq!(candle.close_time.timestamp(), bar.date.unix_timestamp());
    }

    #[test]
    fn bar_to_candle_handles_negative_count() {
        let bar = ibapi::market_data::historical::Bar {
            date: datetime!(2024-01-15 16:00 UTC),
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 0.0,
            wap: 0.0,
            count: -1, // IB sometimes returns -1 for "not available"
        };

        let candle = bar_to_candle(&bar).unwrap();

        // Negative count should clamp to 0
        assert_eq!(candle.trade_count, 0);
    }

    #[test]
    fn historical_request_daily_trades_builder() {
        let contract = Contract::stock("AAPL").build();
        let request = HistoricalRequest::daily_trades(contract, 30);

        assert_eq!(request.contract.symbol.as_str(), "AAPL");
        assert!(request.end_date.is_none());
        assert!(matches!(request.bar_size, BarSize::Day));
        assert!(matches!(request.what_to_show, WhatToShow::Trades));
        assert!(request.regular_trading_hours_only);
    }
}
