//! Historical data fetcher for Interactive Brokers.
//!
//! Provides one-shot historical bar data via `IbkrHistoricalData::fetch_candles()`
//! and tick-level data via `fetch_historical_ticks()` / `fetch_historical_bid_ask()`.
//! This is a separate utility API, not part of the streaming [`IbkrMarketStream`].
//!
//! # Historical Bars Example
//!
//! ```ignore
//! use rustrade_data::exchange::ibkr::historical::{IbkrHistoricalData, HistoricalRequest};
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
//! # Historical Ticks Example
//!
//! ```ignore
//! use rustrade_data::exchange::ibkr::historical::{IbkrHistoricalData, HistoricalTickRequest};
//! use time::macros::datetime;
//!
//! let client = IbkrHistoricalData::connect("127.0.0.1:4002", 102)?;
//! let contract = ibapi::contracts::Contract::stock("AAPL").build();
//!
//! let request = HistoricalTickRequest {
//!     contract,
//!     start: Some(datetime!(2024-01-15 14:30 UTC)),
//!     end: None,
//!     number_of_ticks: 100,
//!     regular_trading_hours_only: true,
//! };
//!
//! let trades = client.fetch_historical_ticks(request).await?;
//! ```
//!
//! # Why Vec Instead of Stream
//!
//! The historical tick methods return `Vec<T>` rather than `Stream<Item = T>` because
//! ibapi's underlying API is batch-oriented: you request up to 1000 ticks and IB sends
//! them all in one response. Wrapping a fully-received batch in a Stream would add
//! complexity without benefit. For large date ranges requiring multiple requests,
//! consider implementing pagination at the caller level.
//!
//! [`IbkrMarketStream`]: super::IbkrMarketStream

use super::options::{OptionChainEntry, OptionGreeks};
use crate::{
    books::Level,
    error::DataError,
    subscription::{book::OrderBookL1, candle::Candle, trade::PublicTrade},
};
use chrono::{DateTime, Utc};
use ibapi::{
    client::blocking::Client,
    contracts::{Contract, SecurityType},
    market_data::{
        TradingHours,
        historical::{BarSize, Duration, TickBidAsk, TickLast, WhatToShow},
    },
};
use rust_decimal::Decimal;
use smol_str::format_smolstr;
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

/// Re-export commonly used types from ibapi for caller convenience.
///
/// Note: Importing this trait couples your code to ibapi's API surface.
/// This is intentional — the trait provides `.days()`, `.months()` etc.
/// syntax needed for `HistoricalRequest::duration`.
pub use ibapi::market_data::historical::ToDuration;

/// Historical data fetcher for Interactive Brokers.
///
/// Wraps an IB client connection for fetching historical OHLCV bars and
/// tick-level data (trades, bid/ask quotes). Unlike [`IbkrMarketStream`],
/// this is a one-shot request/response API.
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

    /// Disconnect from IB Gateway.
    ///
    /// Signals the ibapi client to shut down and releases the client ID for reuse.
    /// When constructed via [`Self::connect`], `Drop` already calls this automatically
    /// once this is the sole owner of the `Arc<Client>` — explicit calls are typically
    /// unnecessary.
    ///
    /// When constructed via [`Self::from_client`] with a shared `Arc<Client>`, calling
    /// `disconnect()` **terminates the connection for all owners** (other
    /// [`IbkrHistoricalData`] instances or external holders of the same `Arc`).
    ///
    /// This is idempotent — calling it multiple times is safe.
    pub fn disconnect(&self) {
        debug!("Disconnecting IbkrHistoricalData");
        self.client.disconnect();
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

    /// Fetch historical trade ticks for the given request.
    ///
    /// Returns individual trade executions (time & sales data) as [`PublicTrade`]s.
    ///
    /// # Arguments
    ///
    /// * `request` - Historical tick request parameters
    ///
    /// # Returns
    ///
    /// Vector of trades in chronological order. Invalid ticks (non-finite prices)
    /// are filtered out with a warning.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request or network error occurs.
    ///
    /// # Notes
    ///
    /// - Maximum 1000 ticks per request (IB limit)
    /// - Trade side is not available from IB historical data
    /// - For larger ranges, paginate using last tick's timestamp as new `start`
    /// - Tick IDs are unique within a single returned batch. Across separate
    ///   calls (e.g. pagination), IDs are not guaranteed unique — ticks with
    ///   identical `(timestamp, price, size)` may collide across batches.
    ///   IB timestamps have only 1-second resolution and IB provides no
    ///   native unique identifier.
    pub async fn fetch_historical_ticks(
        &self,
        request: HistoricalTickRequest,
    ) -> Result<Vec<PublicTrade>, DataError> {
        if request.start.is_none() && request.end.is_none() {
            return Err(DataError::Socket(
                "HistoricalTickRequest: at least one of `start` or `end` must be set".into(),
            ));
        }

        let client = self.client.clone();
        let symbol = request.contract.symbol.clone();

        debug!(
            symbol = %symbol,
            number_of_ticks = request.number_of_ticks,
            "Fetching historical trade ticks"
        );

        let trades = tokio::task::spawn_blocking(move || {
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };

            let subscription = client
                .historical_ticks_trade(
                    &request.contract,
                    request.start,
                    request.end,
                    request.number_of_ticks,
                    trading_hours,
                )
                .map_err(|e| DataError::Socket(format!("historical_ticks_trade: {e}")))?;

            let mut trades =
                Vec::with_capacity(usize::try_from(request.number_of_ticks).unwrap_or(0));
            for (seq, tick) in subscription.into_iter().enumerate() {
                if let Some(trade) = tick_last_to_public_trade(&tick, seq) {
                    trades.push(trade);
                }
            }

            Ok::<_, DataError>(trades)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("historical_ticks_trade task panicked: {e}"))
            } else {
                DataError::Socket(format!("historical_ticks_trade task cancelled: {e}"))
            }
        })??;

        debug!(symbol = %symbol, count = trades.len(), "Received historical trade ticks");

        Ok(trades)
    }

    /// Fetch historical bid/ask ticks for the given request.
    ///
    /// Returns historical best bid/ask quotes as [`OrderBookL1`] snapshots.
    ///
    /// # Arguments
    ///
    /// * `request` - Historical tick request parameters
    /// * `ignore_size` - If true, return ticks even when size is zero
    ///
    /// # Returns
    ///
    /// Vector of L1 quotes in chronological order. Invalid ticks (non-finite prices)
    /// are filtered out with a warning.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request or network error occurs.
    ///
    /// # Notes
    ///
    /// - Maximum 1000 ticks per request (IB limit)
    /// - For larger ranges, paginate using last tick's timestamp as new `start`
    pub async fn fetch_historical_bid_ask(
        &self,
        request: HistoricalTickRequest,
        ignore_size: bool,
    ) -> Result<Vec<OrderBookL1>, DataError> {
        if request.start.is_none() && request.end.is_none() {
            return Err(DataError::Socket(
                "HistoricalTickRequest: at least one of `start` or `end` must be set".into(),
            ));
        }

        let client = self.client.clone();
        let symbol = request.contract.symbol.clone();

        debug!(
            symbol = %symbol,
            number_of_ticks = request.number_of_ticks,
            ignore_size,
            "Fetching historical bid/ask ticks"
        );

        let quotes = tokio::task::spawn_blocking(move || {
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };

            let subscription = client
                .historical_ticks_bid_ask(
                    &request.contract,
                    request.start,
                    request.end,
                    request.number_of_ticks,
                    trading_hours,
                    ignore_size,
                )
                .map_err(|e| DataError::Socket(format!("historical_ticks_bid_ask: {e}")))?;

            let mut quotes =
                Vec::with_capacity(usize::try_from(request.number_of_ticks).unwrap_or(0));
            for tick in subscription {
                if let Some(l1) = tick_bid_ask_to_order_book_l1(&tick) {
                    quotes.push(l1);
                }
            }

            Ok::<_, DataError>(quotes)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("historical_ticks_bid_ask task panicked: {e}"))
            } else {
                DataError::Socket(format!("historical_ticks_bid_ask task cancelled: {e}"))
            }
        })??;

        debug!(symbol = %symbol, count = quotes.len(), "Received historical bid/ask ticks");

        Ok(quotes)
    }

    // ========================================================================
    // Option Greeks Calculators (Phase 5A)
    // ========================================================================

    /// Calculate theoretical option Greeks given volatility and underlying price.
    ///
    /// This is a **calculator** — you provide the volatility and underlying price,
    /// and IB computes the theoretical Greeks. This does NOT fetch market data.
    ///
    /// For real-time Greeks based on live market prices, use the streaming API
    /// (Phase 5B) with `TickTypes::OptionComputation`.
    ///
    /// # Arguments
    ///
    /// * `contract` - Option contract (must be SecurityType::Option)
    /// * `volatility` - Implied volatility to use (e.g., 0.25 for 25%)
    /// * `underlying_price` - Underlying price to use for calculation
    ///
    /// # Returns
    ///
    /// [`OptionGreeks`] containing computed delta, gamma, theta, vega, and
    /// theoretical option price.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request (invalid contract,
    /// missing subscription, etc.).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = IbkrHistoricalData::connect("127.0.0.1:4002", 102)?;
    /// let option = Contract::call("AAPL").strike(150.0).expires_on(2024, 12, 20).build();
    ///
    /// let greeks = client.calculate_theoretical_greeks(&option, 0.25, 148.0).await?;
    /// if let Some(delta) = greeks.delta {
    ///     println!("Delta: {:.3}", delta);
    /// }
    /// ```
    pub async fn calculate_theoretical_greeks(
        &self,
        contract: &Contract,
        volatility: f64,
        underlying_price: f64,
    ) -> Result<OptionGreeks, DataError> {
        let client = self.client.clone();
        let contract = contract.clone();
        let symbol = contract.symbol.to_string();

        debug!(
            symbol = %symbol,
            volatility,
            underlying_price,
            "Calculating theoretical option Greeks"
        );

        let greeks = tokio::task::spawn_blocking(move || {
            let computation = client
                .calculate_option_price(&contract, volatility, underlying_price)
                .map_err(|e| DataError::Socket(format!("calculate_option_price: {e}")))?;

            Ok::<_, DataError>(OptionGreeks::from_ib(&computation))
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("calculate_option_price task panicked: {e}"))
            } else {
                DataError::Socket(format!("calculate_option_price task cancelled: {e}"))
            }
        })??;

        debug!(
            symbol = %symbol,
            delta = ?greeks.delta,
            gamma = ?greeks.gamma,
            "Calculated option Greeks"
        );

        Ok(greeks)
    }

    /// Calculate implied volatility from option price and underlying price.
    ///
    /// This is a **calculator** — you provide the option and underlying prices,
    /// and IB computes the implied volatility using its options model.
    ///
    /// # Arguments
    ///
    /// * `contract` - Option contract (must be SecurityType::Option)
    /// * `option_price` - Current or hypothetical option price
    /// * `underlying_price` - Current or hypothetical underlying price
    ///
    /// # Returns
    ///
    /// Implied volatility as a decimal (e.g., 0.25 for 25% IV).
    /// Also returns other Greeks computed at this IV level.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request or if IV cannot
    /// be computed (e.g., option price is below intrinsic value).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = IbkrHistoricalData::connect("127.0.0.1:4002", 102)?;
    /// let option = Contract::call("AAPL").strike(150.0).expires_on(2024, 12, 20).build();
    ///
    /// let greeks = client.calculate_implied_volatility(&option, 7.50, 155.0).await?;
    /// if let Some(iv) = greeks.implied_volatility {
    ///     println!("Implied Volatility: {:.1}%", iv * 100.0);
    /// }
    /// ```
    pub async fn calculate_implied_volatility(
        &self,
        contract: &Contract,
        option_price: f64,
        underlying_price: f64,
    ) -> Result<OptionGreeks, DataError> {
        let client = self.client.clone();
        let contract = contract.clone();
        let symbol = contract.symbol.to_string();

        debug!(
            symbol = %symbol,
            option_price,
            underlying_price,
            "Calculating implied volatility"
        );

        let greeks = tokio::task::spawn_blocking(move || {
            let computation = client
                .calculate_implied_volatility(&contract, option_price, underlying_price)
                .map_err(|e| DataError::Socket(format!("calculate_implied_volatility: {e}")))?;

            Ok::<_, DataError>(OptionGreeks::from_ib(&computation))
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("calculate_implied_volatility task panicked: {e}"))
            } else {
                DataError::Socket(format!("calculate_implied_volatility task cancelled: {e}"))
            }
        })??;

        debug!(
            symbol = %symbol,
            iv = ?greeks.implied_volatility,
            "Calculated implied volatility"
        );

        Ok(greeks)
    }

    /// Fetch option chain metadata for an underlying security.
    ///
    /// Returns available expiration dates and strike prices for options on
    /// the specified underlying. Does NOT return Greeks or prices — use
    /// market data subscriptions for that.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Underlying symbol (e.g., "AAPL")
    /// * `exchange` - Exchange to query (e.g., "SMART", "CBOE")
    /// * `security_type` - Type of underlying (typically `SecurityType::Stock`)
    /// * `contract_id` - IB contract ID of the underlying (0 to search by symbol)
    ///
    /// # Returns
    ///
    /// Vector of [`OptionChainEntry`] for each exchange/trading class combination.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = IbkrHistoricalData::connect("127.0.0.1:4002", 102)?;
    ///
    /// let chains = client.fetch_option_chain("AAPL", "SMART", SecurityType::Stock, 0).await?;
    /// for chain in chains {
    ///     println!("Exchange: {}, Expirations: {:?}", chain.exchange, chain.expirations);
    /// }
    /// ```
    pub async fn fetch_option_chain(
        &self,
        symbol: &str,
        exchange: &str,
        security_type: SecurityType,
        contract_id: i32,
    ) -> Result<Vec<OptionChainEntry>, DataError> {
        let client = self.client.clone();
        let symbol = symbol.to_string();
        let exchange = exchange.to_string();

        debug!(
            symbol = %symbol,
            exchange = %exchange,
            "Fetching option chain"
        );

        let chains = tokio::task::spawn_blocking(move || {
            let subscription = client
                .option_chain(&symbol, &exchange, security_type, contract_id)
                .map_err(|e| DataError::Socket(format!("option_chain: {e}")))?;

            let mut entries = Vec::with_capacity(16);
            for chain in subscription {
                entries.push(OptionChainEntry::from_ib(&chain));
            }

            debug!(symbol = %symbol, count = entries.len(), "Received option chain entries");

            Ok::<_, DataError>(entries)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("option_chain task panicked: {e}"))
            } else {
                DataError::Socket(format!("option_chain task cancelled: {e}"))
            }
        })??;

        Ok(chains)
    }
}

impl Drop for IbkrHistoricalData {
    fn drop(&mut self) {
        // Only disconnect when we are the sole owner of the Arc<Client>.
        // `from_client(Arc<Client>)` lets callers share the client; disconnecting
        // here would terminate the connection for other owners too.
        //
        // `Arc::strong_count` is approximate under concurrent clone/drop on other
        // threads. A spurious skip is harmless (ibapi's own `Client::Drop` will
        // eventually run when the last Arc drops); a spurious disconnect is also
        // benign because `disconnect()` is idempotent.
        if Arc::strong_count(&self.client) == 1 {
            debug!("Dropping IbkrHistoricalData (sole owner), disconnecting client");
            self.client.disconnect();
        }
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

/// Parameters for a historical tick data request.
///
/// Unlike [`HistoricalRequest`] which fetches OHLCV bars, this requests
/// individual tick-level data (trades or bid/ask quotes).
///
/// # Time Range
///
/// Either `start` or `end` must be specified (not both `None`).
/// IB returns ticks forward from `start` or backward from `end`.
///
/// # Limits
///
/// IB limits requests to 1000 ticks maximum. For larger ranges,
/// make multiple requests using the last tick's timestamp as the
/// new `start` time.
#[derive(Debug, Clone)]
pub struct HistoricalTickRequest {
    /// IB contract to fetch tick data for.
    pub contract: Contract,

    /// Start time for the tick range (fetch forward from here).
    ///
    /// Either `start` or `end` must be specified.
    pub start: Option<OffsetDateTime>,

    /// End time for the tick range (fetch backward to here).
    ///
    /// Either `start` or `end` must be specified.
    pub end: Option<OffsetDateTime>,

    /// Maximum number of ticks to return (max 1000).
    pub number_of_ticks: i32,

    /// If true, only include ticks from regular trading hours.
    pub regular_trading_hours_only: bool,
}

impl HistoricalTickRequest {
    /// Create a request for recent ticks ending now.
    ///
    /// Fetches the most recent `count` ticks (up to 1000) ending at current time.
    pub fn recent(contract: Contract, count: i32) -> Self {
        Self {
            contract,
            start: None,
            end: Some(OffsetDateTime::now_utc()),
            number_of_ticks: count.min(1000),
            regular_trading_hours_only: true,
        }
    }
}

/// Convert an ibapi `TickLast` (historical trade) to a rustrade `PublicTrade`.
///
/// # Arguments
///
/// * `tick` - The IB tick data
/// * `seq` - Sequence index within the batch (for unique ID generation)
///
/// # Side Field
///
/// IB historical tick data does not include trade side (buyer/seller initiated).
/// The `side` field is set to `None`.
///
/// # Returns
///
/// Returns `None` if price is non-finite (invalid data from IB).
fn tick_last_to_public_trade(tick: &TickLast, seq: usize) -> Option<PublicTrade> {
    if !tick.price.is_finite() {
        warn!(
            price = tick.price,
            "Historical tick has non-finite price, skipping"
        );
        return None;
    }

    let price = Decimal::try_from(tick.price).ok()?;
    let amount = Decimal::from(tick.size);

    Some(PublicTrade {
        id: generate_tick_id(tick.timestamp, tick.price, tick.size, seq),
        price,
        amount,
        side: None,
    })
}

/// Parse timestamp from a historical tick, preserving sub-second precision.
///
/// Returns `None` if the Unix timestamp is out of `DateTime<Utc>` range.
/// Callers should drop the tick rather than substitute a fabricated time
/// (which would corrupt chronological ordering).
fn parse_tick_timestamp(timestamp: OffsetDateTime) -> Option<DateTime<Utc>> {
    let unix_secs = timestamp.unix_timestamp();
    let dt = DateTime::from_timestamp(unix_secs, timestamp.nanosecond());
    if dt.is_none() {
        warn!(
            unix_timestamp = unix_secs,
            "Invalid tick timestamp from IB, skipping tick"
        );
    }
    dt
}

/// Generate a unique ID for a historical tick.
///
/// IB doesn't provide tick IDs, so we generate one from a hash of
/// time + price + size + sequence index. The sequence index ensures
/// uniqueness within a batch when multiple trades have identical
/// (timestamp, price, size) — common since IB timestamps have only
/// 1-second resolution.
fn generate_tick_id(
    timestamp: OffsetDateTime,
    price: f64,
    size: i32,
    seq: usize,
) -> smol_str::SmolStr {
    let mut hasher = fnv::FnvHasher::default();
    timestamp.unix_timestamp_nanos().hash(&mut hasher);
    price.to_bits().hash(&mut hasher);
    size.hash(&mut hasher);
    seq.hash(&mut hasher);
    format_smolstr!("{:016x}", hasher.finish())
}

/// Convert an ibapi `TickBidAsk` to a rustrade `OrderBookL1`.
///
/// # Returns
///
/// Returns `None` if any price is non-finite (invalid data from IB).
fn tick_bid_ask_to_order_book_l1(tick: &TickBidAsk) -> Option<OrderBookL1> {
    if !tick.price_bid.is_finite() || !tick.price_ask.is_finite() {
        warn!(
            bid = tick.price_bid,
            ask = tick.price_ask,
            "Historical tick has non-finite price, skipping"
        );
        return None;
    }

    let bid_price = Decimal::try_from(tick.price_bid).ok()?;
    let ask_price = Decimal::try_from(tick.price_ask).ok()?;
    let bid_amount = Decimal::from(tick.size_bid);
    let ask_amount = Decimal::from(tick.size_ask);

    Some(OrderBookL1 {
        last_update_time: parse_tick_timestamp(tick.timestamp)?,
        best_bid: Some(Level::new(bid_price, bid_amount)),
        best_ask: Some(Level::new(ask_price, ask_amount)),
    })
}

/// Convert an ibapi `Bar` to a rustrade `Candle`.
///
/// Note: `Bar::wap` (volume-weighted average price) is not mapped to `Candle`
/// as rustrade's `Candle` type does not include VWAP.
///
/// Returns `Err(DataError::Socket(...))` if any price/volume value cannot be
/// converted to Decimal (e.g., NaN, Infinity from the IB API).
fn bar_to_candle(bar: &ibapi::market_data::historical::Bar) -> Result<Candle, DataError> {
    let close_time = DateTime::from_timestamp(bar.date.unix_timestamp(), bar.date.nanosecond())
        .ok_or_else(|| {
            DataError::Socket(format!(
                "IB timestamp {} out of DateTime<Utc> range",
                bar.date.unix_timestamp()
            ))
        })?;

    let open =
        Decimal::try_from(bar.open).map_err(|e| DataError::Socket(format!("parse open: {e}")))?;
    let high =
        Decimal::try_from(bar.high).map_err(|e| DataError::Socket(format!("parse high: {e}")))?;
    let low =
        Decimal::try_from(bar.low).map_err(|e| DataError::Socket(format!("parse low: {e}")))?;
    let close =
        Decimal::try_from(bar.close).map_err(|e| DataError::Socket(format!("parse close: {e}")))?;
    let volume = Decimal::try_from(bar.volume)
        .map_err(|e| DataError::Socket(format!("parse volume: {e}")))?;

    Ok(Candle {
        close_time,
        open,
        high,
        low,
        close,
        volume,
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
        use rust_decimal_macros::dec;

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

        assert_eq!(candle.open, dec!(150));
        assert_eq!(candle.high, dec!(155));
        assert_eq!(candle.low, dec!(149));
        assert_eq!(candle.close, dec!(153.5));
        assert_eq!(candle.volume, dec!(1_000_000));
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

    #[test]
    fn historical_tick_request_recent_builder() {
        let contract = Contract::stock("AAPL").build();
        let request = HistoricalTickRequest::recent(contract, 500);

        assert_eq!(request.contract.symbol.as_str(), "AAPL");
        assert!(request.start.is_none());
        assert!(request.end.is_some()); // IB requires start or end to be set
        assert_eq!(request.number_of_ticks, 500);
        assert!(request.regular_trading_hours_only);
    }

    #[test]
    fn historical_tick_request_recent_clamps_to_1000() {
        let contract = Contract::stock("AAPL").build();
        let request = HistoricalTickRequest::recent(contract, 5000);

        assert_eq!(request.number_of_ticks, 1000);
    }

    // ========================================================================
    // Historical Tick Conversion Tests
    // ========================================================================

    fn make_tick_last(unix_time: i64, price: f64, size: i32) -> TickLast {
        use ibapi::market_data::historical::TickAttributeLast;

        TickLast {
            timestamp: time::OffsetDateTime::from_unix_timestamp(unix_time)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
            tick_attribute_last: TickAttributeLast {
                past_limit: false,
                unreported: false,
            },
            price,
            size,
            exchange: String::new(),
            special_conditions: String::new(),
        }
    }

    fn make_tick_bid_ask(
        unix_time: i64,
        bid_price: f64,
        bid_size: i32,
        ask_price: f64,
        ask_size: i32,
    ) -> TickBidAsk {
        use ibapi::market_data::historical::TickAttributeBidAsk;

        TickBidAsk {
            timestamp: time::OffsetDateTime::from_unix_timestamp(unix_time)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
            tick_attribute_bid_ask: TickAttributeBidAsk {
                bid_past_low: false,
                ask_past_high: false,
            },
            price_bid: bid_price,
            price_ask: ask_price,
            size_bid: bid_size,
            size_ask: ask_size,
        }
    }

    #[test]
    fn tick_last_converts_to_public_trade() {
        use rust_decimal_macros::dec;

        let tick = make_tick_last(1700000000, 150.25, 100);
        let trade = tick_last_to_public_trade(&tick, 0).unwrap();

        assert_eq!(trade.price, dec!(150.25));
        assert_eq!(trade.amount, dec!(100));
        assert!(trade.side.is_none());
        assert!(!trade.id.is_empty());
    }

    #[test]
    fn tick_last_rejects_non_finite_price() {
        let tick = make_tick_last(1700000000, f64::NAN, 100);
        assert!(tick_last_to_public_trade(&tick, 0).is_none());

        let tick = make_tick_last(1700000000, f64::INFINITY, 100);
        assert!(tick_last_to_public_trade(&tick, 0).is_none());
    }

    #[test]
    fn tick_last_generates_unique_ids() {
        let tick1 = make_tick_last(1700000000, 150.25, 100);
        let tick2 = make_tick_last(1700000001, 150.25, 100);
        let tick3 = make_tick_last(1700000000, 150.26, 100);

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 0);
        let id3 = generate_tick_id(tick3.timestamp, tick3.price, tick3.size, 0);

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id2, id3);
    }

    #[test]
    fn tick_last_same_data_same_seq_same_id() {
        let tick1 = make_tick_last(1700000000, 150.25, 100);
        let tick2 = make_tick_last(1700000000, 150.25, 100);

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 0);

        assert_eq!(id1, id2);
    }

    #[test]
    fn tick_last_same_data_different_seq_different_id() {
        let tick1 = make_tick_last(1700000000, 150.25, 100);
        let tick2 = make_tick_last(1700000000, 150.25, 100);

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 1);

        assert_ne!(id1, id2);
    }

    #[test]
    fn tick_bid_ask_converts_to_order_book_l1() {
        use rust_decimal_macros::dec;

        let tick = make_tick_bid_ask(1700000000, 150.00, 500, 150.05, 300);
        let l1 = tick_bid_ask_to_order_book_l1(&tick).unwrap();

        let bid = l1.best_bid.unwrap();
        let ask = l1.best_ask.unwrap();

        assert_eq!(bid.price, dec!(150.00));
        assert_eq!(bid.amount, dec!(500));
        assert_eq!(ask.price, dec!(150.05));
        assert_eq!(ask.amount, dec!(300));
        assert_eq!(l1.last_update_time.timestamp(), 1700000000);
    }

    #[test]
    fn tick_bid_ask_rejects_non_finite_prices() {
        let tick = make_tick_bid_ask(1700000000, f64::NAN, 500, 150.05, 300);
        assert!(tick_bid_ask_to_order_book_l1(&tick).is_none());

        let tick = make_tick_bid_ask(1700000000, 150.00, 500, f64::INFINITY, 300);
        assert!(tick_bid_ask_to_order_book_l1(&tick).is_none());
    }

    #[test]
    fn parse_tick_timestamp_converts_correctly() {
        let ts = time::OffsetDateTime::from_unix_timestamp(1700000000).unwrap();
        let dt = parse_tick_timestamp(ts).unwrap();

        assert_eq!(dt.timestamp(), 1700000000);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn parse_tick_timestamp_preserves_subsecond_nanos() {
        let ts =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_456_789).unwrap();
        let dt = parse_tick_timestamp(ts).unwrap();

        assert_eq!(dt.timestamp(), 1700000000);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);
    }
}
