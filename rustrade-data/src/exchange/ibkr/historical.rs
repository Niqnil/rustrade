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
//! let fetched = client.fetch_historical_ticks(request).await?;
//! let trades = fetched.ticks; // `fetched.truncation_error` flags a confirmed short read
//! ```
//!
//! # Why Vec Instead of Stream
//!
//! The historical tick methods return a `Vec<T>` (wrapped in [`HistoricalTicks`])
//! rather than `Stream<Item = T>` because ibapi's underlying API is batch-oriented:
//! you request up to 1000 ticks and IB sends them all in one response. Wrapping a
//! fully-received batch in a Stream would add complexity without benefit. For large
//! date ranges requiring multiple requests, consider implementing pagination at the
//! caller level.
//!
//! [`IbkrMarketStream`]: super::IbkrMarketStream

use super::options::{OptionChainEntry, OptionGreeks};
use crate::{
    books::Level,
    error::DataError,
    subscription::{
        book::OrderBookL1,
        candle::{Candle, IntervalStep, close_time_from_open},
        trade::PublicTrade,
    },
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use ibapi::{
    client::blocking::Client,
    contracts::{Contract, OptionChain, SecurityType},
    market_data::{
        IgnoreSize, TradingHours,
        historical::{BarSize, Duration, TickBidAsk, TickLast, TickSubscription, WhatToShow},
    },
    subscriptions::{Subscription, SubscriptionItem},
};
use rust_decimal::Decimal;
use smol_str::format_smolstr;
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration as StdDuration, Instant},
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
    /// The bound on the requests read from a subscription, which `ibapi`'s `one_shot_timeout`
    /// does not cover. See [`IbkrHistoricalDataConfig::request_timeout`].
    request_timeout: Option<StdDuration>,
}

/// How [`IbkrHistoricalData::connect_with`] sets up its connection.
///
/// Build it with [`default`](Self::default) and the `with_*` setters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IbkrHistoricalDataConfig {
    /// How long each [`IbkrHistoricalData`] request waits for IB's complete answer before it
    /// fails with [`DataError::RequestTimeout`]. `None` waits without a limit. The requests:
    /// [`fetch_candles`](IbkrHistoricalData::fetch_candles),
    /// [`fetch_historical_ticks`](IbkrHistoricalData::fetch_historical_ticks),
    /// [`fetch_historical_bid_ask`](IbkrHistoricalData::fetch_historical_bid_ask),
    /// [`calculate_theoretical_greeks`](IbkrHistoricalData::calculate_theoretical_greeks),
    /// [`calculate_implied_volatility`](IbkrHistoricalData::calculate_implied_volatility) and
    /// [`fetch_option_chain`](IbkrHistoricalData::fetch_option_chain).
    ///
    /// The bound covers the whole call, from when it is made. For the candle and calculator
    /// requests it also covers a wait for the connection to come back and the request to be
    /// resent after it drops. A request that runs out of time fails whole: the ticks or chain
    /// entries received before then are not returned.
    ///
    /// Without a bound, a call IB never answers never returns, and a candle or calculator call
    /// caught by a dropped connection waits through every reconnect attempt. Either way it holds
    /// a thread of tokio's blocking pool.
    ///
    /// Defaults to [`DEFAULT_REQUEST_TIMEOUT`](Self::DEFAULT_REQUEST_TIMEOUT). A long window or a
    /// cold historical data server can make IB slow to answer, so raise it for large fetches.
    pub request_timeout: Option<StdDuration>,
}

impl IbkrHistoricalDataConfig {
    /// The default [`request_timeout`](Self::request_timeout): two minutes.
    pub const DEFAULT_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(120);

    /// This config with `request_timeout`. `None` waits without a limit. A zero timeout makes
    /// [`IbkrHistoricalData::connect_with`] fail.
    #[must_use]
    pub fn with_request_timeout(self, request_timeout: Option<StdDuration>) -> Self {
        Self { request_timeout }
    }
}

impl Default for IbkrHistoricalDataConfig {
    fn default() -> Self {
        Self {
            request_timeout: Some(Self::DEFAULT_REQUEST_TIMEOUT),
        }
    }
}

impl IbkrHistoricalData {
    /// Connect to TWS/Gateway for historical data requests, with the default
    /// [`IbkrHistoricalDataConfig`].
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
        Self::connect_with(url, client_id, IbkrHistoricalDataConfig::default())
    }

    /// Connect to TWS/Gateway for historical data requests, set up by `config`.
    ///
    /// # Arguments
    ///
    /// * `url` - Host and port (e.g., "127.0.0.1:4002" for Gateway paper)
    /// * `client_id` - Unique client ID (must differ from other connections)
    /// * `config` - The connection's settings, such as its request timeout
    ///
    /// # Errors
    ///
    /// [`DataError::Socket`] if the connection fails, or if `config` has a zero
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
    pub fn connect_with(
        url: &str,
        client_id: i32,
        config: IbkrHistoricalDataConfig,
    ) -> Result<Self, DataError> {
        info!(
            %url,
            client_id,
            request_timeout = ?config.request_timeout,
            "Connecting to IB for historical data"
        );

        let builder = Client::builder().address(url).client_id(client_id);
        let builder = match config.request_timeout {
            Some(timeout) => builder.one_shot_timeout(timeout),
            None => builder,
        };
        let client = builder
            .connect()
            .map_err(|e| DataError::Socket(format!("IB connect: {e}")))?;

        Ok(Self {
            client: Arc::new(client),
            request_timeout: config.request_timeout,
        })
    }

    /// Create from an existing client connection.
    ///
    /// # Request timeout
    ///
    /// The caller sets the timeout of the candle and calculator requests
    /// ([`fetch_candles`](Self::fetch_candles),
    /// [`calculate_theoretical_greeks`](Self::calculate_theoretical_greeks),
    /// [`calculate_implied_volatility`](Self::calculate_implied_volatility)): the client's own
    /// `ibapi` `one_shot_timeout` (`Client::builder().one_shot_timeout(..)`) bounds them. A
    /// client built without one, such as one from `Client::connect`, leaves them without a
    /// limit: see [`IbkrHistoricalDataConfig::request_timeout`] for what that risks.
    ///
    /// The tick and option chain requests, which `one_shot_timeout` does not cover, are bounded
    /// by [`IbkrHistoricalDataConfig::DEFAULT_REQUEST_TIMEOUT`]. Use
    /// [`connect_with`](Self::connect_with) to set one timeout for every request.
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
        Self {
            client,
            request_timeout: Some(IbkrHistoricalDataConfig::DEFAULT_REQUEST_TIMEOUT),
        }
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
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
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
    /// - Each candle's `close_time` is the exclusive end-of-period boundary
    ///   `bar_open + interval`, **not** the bar's own start (IB returns the start)
    ///   and **not** the exchange session close (the library has no session
    ///   calendar). For daily/weekly/monthly bars this is the **next** UTC period
    ///   start: a Jan 15 daily bar → `close_time = Jan 16 00:00 UTC`; a January
    ///   monthly bar → `Feb 1 00:00 UTC` (calendar arithmetic). The bar's own date
    ///   is `close_time − interval`, so do **not** compare daily bars by
    ///   `close_time.date()` — it shifts forward by one interval. See
    ///   [`Candle::close_time`](crate::subscription::candle::Candle).
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

            // ibapi 3.x: historical bars are requested via a builder. `ending` is
            // optional — omitting it anchors the window at the current time, matching
            // the previous `end_date: None` behavior.
            let mut builder = client
                .historical_data(&request.contract, request.bar_size)
                .what_to_show(request.what_to_show)
                .duration(request.duration)
                .trading_hours(trading_hours);
            if let Some(end_date) = request.end_date {
                builder = builder.ending(end_date);
            }
            let historical_data = builder
                .fetch()
                .map_err(|e| request_error("fetch_candles", e))?;

            let mut candles = Vec::with_capacity(historical_data.bars.len());
            for bar in &historical_data.bars {
                candles.push(bar_to_candle(bar, request.bar_size)?);
            }

            Ok::<_, DataError>(candles)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("fetch_candles task panicked: {e}"))
            } else {
                DataError::Socket(format!("fetch_candles task cancelled: {e}"))
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
    /// A [`HistoricalTicks`] carrying the trades in chronological order plus a
    /// `truncation_error` flag. Invalid ticks (non-finite prices) are filtered
    /// out with a warning.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request or network error occurs.
    ///
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
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
    /// - A mid-stream IB API error is surfaced two ways: ibapi 3.1+ yields it as
    ///   an `Err` on the tick iterator, so this method logs a `warn!`, ends
    ///   iteration, and sets [`HistoricalTicks::truncation_error`] on the
    ///   returned value (the ticks received before the error are still returned).
    ///   Separately, this method also emits a `warn!` when the number of raw ticks
    ///   received is less than `request.number_of_ticks`. Because a short batch is
    ///   also a normal end-of-data signal, treat that warning as a flag for
    ///   investigation (or a prompt to paginate), **not** as proof of an error;
    ///   `truncation_error.is_some()` is the authoritative signal for a confirmed error.
    pub async fn fetch_historical_ticks(
        &self,
        request: HistoricalTickRequest,
    ) -> Result<HistoricalTicks<PublicTrade>, DataError> {
        if request.start.is_none() && request.end.is_none() {
            return Err(DataError::Socket(
                "HistoricalTickRequest: at least one of `start` or `end` must be set".into(),
            ));
        }

        let deadline = deadline(self.request_timeout);
        let client = self.client.clone();
        let symbol = request.contract.symbol.clone();

        debug!(
            symbol = %symbol,
            number_of_ticks = request.number_of_ticks,
            "Fetching historical trade ticks"
        );

        let fetched = tokio::task::spawn_blocking(move || {
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };

            // ibapi 3.x: historical ticks use a builder; the tick type is selected
            // by the terminal method (`.trade()` here). At least one of
            // `starting`/`ending` is required (validated above).
            let mut builder = client
                .historical_ticks(&request.contract, request.number_of_ticks)
                .trading_hours(trading_hours);
            if let Some(start) = request.start {
                builder = builder.starting(start);
            }
            if let Some(end) = request.end {
                builder = builder.ending(end);
            }
            let subscription = builder
                .trade()
                .map_err(|e| DataError::Socket(format!("fetch_historical_ticks: {e}")))?;

            // ibapi 3.1+ wraps tick items in a `SubscriptionItem` envelope and
            // surfaces mid-stream IB errors as `Err` on the iterator (3.0.1 dropped
            // them silently). `DataUntil` filters non-fatal notices (logged at
            // `warn!`) and yields `Result<TickLast, Error>` until the deadline;
            // `collect_ticks` drains it, stops at the first `Err`, and records the reason.
            let mut data = DataUntil::new(&subscription, deadline, "fetch_historical_ticks");
            let fetched = collect_ticks(
                &mut data,
                request.contract.symbol.0.as_str(),
                request.number_of_ticks,
                tick_last_to_public_trade,
                |e| {
                    warn!(
                        symbol = %request.contract.symbol.0,
                        error = %e,
                        "Historical trade tick stream ended with an IB error; \
                         results may be truncated"
                    );
                },
            );
            data.timed_out()?;
            Ok::<_, DataError>(fetched)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("fetch_historical_ticks task panicked: {e}"))
            } else {
                DataError::Socket(format!("fetch_historical_ticks task cancelled: {e}"))
            }
        })??;

        debug!(
            symbol = %symbol,
            count = fetched.ticks.len(),
            truncation_error = ?fetched.truncation_error,
            "Received historical trade ticks"
        );

        Ok(fetched)
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
    /// A [`HistoricalTicks`] carrying the L1 quotes in chronological order plus a
    /// `truncation_error` flag. Invalid ticks (non-finite prices) are filtered
    /// out with a warning.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request or network error occurs.
    ///
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
    ///
    /// # Notes
    ///
    /// - Maximum 1000 ticks per request (IB limit)
    /// - For larger ranges, paginate using last tick's timestamp as new `start`
    /// - A mid-stream IB API error is surfaced two ways: ibapi 3.1+ yields it as
    ///   an `Err` on the tick iterator, so this method logs a `warn!`, ends
    ///   iteration, and sets [`HistoricalTicks::truncation_error`] on the
    ///   returned value (the ticks received before the error are still returned).
    ///   Separately, this method also emits a `warn!` when the number of raw ticks
    ///   received is less than `request.number_of_ticks`. Because a short batch is
    ///   also a normal end-of-data signal, treat that warning as a flag for
    ///   investigation (or a prompt to paginate), **not** as proof of an error;
    ///   `truncation_error.is_some()` is the authoritative signal for a confirmed error.
    pub async fn fetch_historical_bid_ask(
        &self,
        request: HistoricalTickRequest,
        ignore_size: bool,
    ) -> Result<HistoricalTicks<OrderBookL1>, DataError> {
        if request.start.is_none() && request.end.is_none() {
            return Err(DataError::Socket(
                "HistoricalTickRequest: at least one of `start` or `end` must be set".into(),
            ));
        }

        let deadline = deadline(self.request_timeout);
        let client = self.client.clone();
        let symbol = request.contract.symbol.clone();

        debug!(
            symbol = %symbol,
            number_of_ticks = request.number_of_ticks,
            ignore_size,
            "Fetching historical bid/ask ticks"
        );

        let fetched = tokio::task::spawn_blocking(move || {
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };

            // ibapi 3.x: same builder as trades, with `.bid_ask(IgnoreSize)` as the
            // terminal. At least one of `starting`/`ending` is required (validated above).
            let mut builder = client
                .historical_ticks(&request.contract, request.number_of_ticks)
                .trading_hours(trading_hours);
            if let Some(start) = request.start {
                builder = builder.starting(start);
            }
            if let Some(end) = request.end {
                builder = builder.ending(end);
            }
            let ignore_size = if ignore_size {
                IgnoreSize::Yes
            } else {
                IgnoreSize::No
            };
            let subscription = builder
                .bid_ask(ignore_size)
                .map_err(|e| DataError::Socket(format!("fetch_historical_bid_ask: {e}")))?;

            // ibapi 3.1+ wraps tick items in a `SubscriptionItem` envelope and
            // surfaces mid-stream IB errors as `Err` on the iterator (3.0.1 dropped
            // them silently). `DataUntil` filters non-fatal notices (logged at
            // `warn!`) and yields `Result<TickBidAsk, Error>` until the deadline;
            // `collect_ticks` drains it, stops at the first `Err`, and records the
            // reason. The bid/ask decode needs no sequence index, so `seq` is ignored.
            let mut data = DataUntil::new(&subscription, deadline, "fetch_historical_bid_ask");
            let fetched = collect_ticks(
                &mut data,
                request.contract.symbol.0.as_str(),
                request.number_of_ticks,
                |tick, _seq| tick_bid_ask_to_order_book_l1(tick),
                |e| {
                    warn!(
                        symbol = %request.contract.symbol.0,
                        error = %e,
                        "Historical bid/ask tick stream ended with an IB error; \
                         results may be truncated"
                    );
                },
            );
            data.timed_out()?;
            Ok::<_, DataError>(fetched)
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("fetch_historical_bid_ask task panicked: {e}"))
            } else {
                DataError::Socket(format!("fetch_historical_bid_ask task cancelled: {e}"))
            }
        })??;

        debug!(
            symbol = %symbol,
            count = fetched.ticks.len(),
            truncation_error = ?fetched.truncation_error,
            "Received historical bid/ask ticks"
        );

        Ok(fetched)
    }

    // ========================================================================
    // Option Greeks Calculators
    // ========================================================================

    /// Calculate theoretical option Greeks given volatility and underlying price.
    ///
    /// This is a **calculator** — you provide the volatility and underlying price,
    /// and IB computes the theoretical Greeks. This does NOT fetch market data.
    ///
    /// For real-time Greeks based on live market prices, use the streaming API
    /// with `TickTypes::OptionComputation`.
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
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
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
                .map_err(|e| request_error("calculate_theoretical_greeks", e))?;

            Ok::<_, DataError>(OptionGreeks::from_ib(&computation))
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("calculate_theoretical_greeks task panicked: {e}"))
            } else {
                DataError::Socket(format!("calculate_theoretical_greeks task cancelled: {e}"))
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
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
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
                .map_err(|e| request_error("calculate_implied_volatility", e))?;

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
    /// * `exchange` - TWS's `fut_fop_exchange` filter, which narrows the chain
    ///   to a *futures* options exchange. Pass `None` for every underlying that
    ///   is not a future: TWS then returns one entry per listing exchange.
    ///   Naming a routing exchange — `Some("SMART")` included — filters the
    ///   result to zero rows. Only pass `Some(..)` for a futures underlying,
    ///   e.g. `Some("CME")`.
    /// * `security_type` - Type of underlying (typically `SecurityType::Stock`)
    /// * `contract_id` - IB contract ID of the underlying. Must be a valid
    ///   conId; IBKR's `reqSecDefOptParams` rejects `0` with
    ///   `[321] Invalid contract id`. Resolve it first via contract details.
    ///
    /// # Returns
    ///
    /// An [`OptionChainResult`] carrying one [`OptionChainEntry`] per
    /// exchange/trading-class combination plus a
    /// [`truncation_error`](OptionChainResult::truncation_error) flag.
    ///
    /// # Errors
    ///
    /// Returns `DataError::Socket` if IB rejects the request up front.
    ///
    /// [`DataError::RequestTimeout`] if IB gives no complete answer within the connection's
    /// [`request_timeout`](IbkrHistoricalDataConfig::request_timeout).
    ///
    /// # Notes
    ///
    /// A mid-stream IB error (pacing/permission error, decode failure,
    /// disconnect) does **not** discard the entries already decoded: each entry
    /// is built from one complete IB message, so every received entry is valid
    /// in isolation. This method logs a `warn!`, ends enumeration, and sets
    /// [`OptionChainResult::truncation_error`] on the returned value — the
    /// same contract as [`fetch_historical_ticks`](Self::fetch_historical_ticks).
    /// Check `truncation_error.is_none()` before treating the entry list as the
    /// complete chain catalog.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = IbkrHistoricalData::connect("127.0.0.1:4002", 102)?;
    ///
    /// // 265598 is AAPL's underlying conId; resolve via contract details for other symbols.
    /// // `None` returns every listing exchange's option parameters.
    /// let chains = client
    ///     .fetch_option_chain("AAPL", None, SecurityType::Stock, 265598)
    ///     .await?;
    /// if let Some(reason) = &chains.truncation_error {
    ///     eprintln!("chain enumeration was cut short: {reason}");
    /// }
    /// for chain in chains {
    ///     println!("Exchange: {}, Expirations: {:?}", chain.exchange, chain.expirations);
    /// }
    /// ```
    pub async fn fetch_option_chain(
        &self,
        symbol: &str,
        exchange: Option<&str>,
        security_type: SecurityType,
        contract_id: i32,
    ) -> Result<OptionChainResult, DataError> {
        let deadline = deadline(self.request_timeout);
        let client = self.client.clone();
        let symbol = symbol.to_string();
        let exchange = exchange.map(str::to_string);

        debug!(
            symbol = %symbol,
            exchange = ?exchange,
            "Fetching option chain"
        );

        let chains = tokio::task::spawn_blocking(move || {
            // ibapi 4.0: `option_chain` returns a builder and `exchange` is a
            // setter rather than a positional argument. An unset exchange is
            // now omitted from the wire entirely instead of being sent as an
            // empty string; TWS treats the two identically.
            let builder = client.option_chain(&symbol, security_type, contract_id);
            let builder = match exchange.as_deref() {
                Some(exchange) => builder.exchange(exchange),
                None => builder,
            };
            let subscription = builder
                .subscribe()
                .map_err(|e| DataError::Socket(format!("fetch_option_chain: {e}")))?;

            // `DataUntil` yields `Result<OptionChain, Error>` until the deadline,
            // filtering subscription-level notices, so an `Err` here is a
            // genuine mid-stream failure. Option chains use the same
            // Subscription/StreamDecoder machinery as historical ticks (N
            // complete per-exchange messages + an end sentinel swallowed as
            // `EndOfStream`), so mirror the tick methods: keep the entries
            // already decoded — each is a complete, valid message — record the
            // reason, and stop, instead of discarding them with a fail-fast Err.
            let mut data = DataUntil::new(&subscription, deadline, "fetch_option_chain");
            let (entries, _received, truncation_error) = drain_until_error(
                &mut data,
                16,
                |chain, _seq| Some(OptionChainEntry::from_ib(chain)),
                |e| {
                    warn!(
                        symbol = %symbol,
                        error = %e,
                        "Option chain enumeration ended with an IB error; \
                         entries may be incomplete"
                    );
                },
            );
            data.timed_out()?;

            debug!(
                symbol = %symbol,
                count = entries.len(),
                truncation_error = ?truncation_error,
                "Received option chain entries"
            );

            Ok::<_, DataError>(OptionChainResult {
                entries,
                truncation_error,
            })
        })
        .await
        .map_err(|e| {
            if e.is_panic() {
                DataError::Socket(format!("fetch_option_chain task panicked: {e}"))
            } else {
                DataError::Socket(format!("fetch_option_chain task cancelled: {e}"))
            }
        })??;

        Ok(chains)
    }
}

/// When a request made now must be answered by, if `request_timeout` bounds it.
fn deadline(request_timeout: Option<StdDuration>) -> Option<Instant> {
    request_timeout.map(|timeout| Instant::now() + timeout)
}

/// The error for a failed `ibapi` request named `operation`: [`DataError::RequestTimeout`] when it
/// ran out of time, else [`DataError::Socket`].
fn request_error(operation: &str, error: ibapi::Error) -> DataError {
    match error {
        ibapi::Error::Timeout => DataError::RequestTimeout {
            operation: operation.to_owned(),
        },
        other => DataError::Socket(format!("{operation}: {other}")),
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

/// Outcome of a historical tick fetch.
///
/// Wraps the collected ticks together with whether the stream was cut short by a
/// mid-stream IB error. A short `ticks` count on its own is ambiguous — it is
/// also the normal end-of-data signal — so
/// [`truncation_error`](Self::truncation_error) disambiguates the *confirmed*
/// error case, letting callers react programmatically (retry, alert, paginate)
/// instead of parsing logs.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalTicks<T> {
    /// Ticks collected in chronological order, up to the point the stream ended.
    ///
    /// Invalid ticks (non-finite prices) are filtered out, so this may be shorter
    /// than the number of raw ticks IB sent.
    pub ticks: Vec<T>,

    /// `Some(reason)` if a mid-stream IB error ended the stream early — `reason`
    /// is the formatted `ibapi::Error` — meaning [`ticks`](Self::ticks) may be
    /// incomplete. `None` on a clean end-of-data, which can still return fewer
    /// ticks than requested when that much history simply isn't available.
    ///
    /// The reason is a formatted string rather than a typed error, matching this
    /// module's existing `DataError::Socket(String)` convention: `ibapi::Error`
    /// is `#[non_exhaustive]` and not meaningfully sub-classifiable for this use
    /// case, so a typed wrapper would create false precision and a second point
    /// that must be kept in sync with `ibapi`'s version. Do not match on the
    /// string contents — treat it as human-facing detail and use `.is_some()`
    /// for the programmatic "was it truncated?" check.
    pub truncation_error: Option<String>,
}

/// Consumes the wrapper and iterates its [`ticks`](HistoricalTicks::ticks),
/// discarding [`truncation_error`](HistoricalTicks::truncation_error). Lets
/// callers that only need the data write `for tick in fetched { .. }`, matching
/// the pre-`HistoricalTicks` `Vec<T>` ergonomics; inspect the flag before
/// iterating when truncation matters.
impl<T> IntoIterator for HistoricalTicks<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.ticks.into_iter()
    }
}

/// Outcome of an option chain fetch.
///
/// Wraps the collected [`OptionChainEntry`]s together with whether enumeration
/// was cut short by a mid-stream IB error. An empty `entries` on its own is
/// ambiguous — a wrong `exchange` filter also legitimately returns zero rows —
/// so [`truncation_error`](Self::truncation_error) disambiguates the
/// *confirmed* error case, letting callers react programmatically (retry,
/// alert) instead of parsing logs.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct OptionChainResult {
    /// Entries received before enumeration ended, one per
    /// (exchange, trading class) combination.
    ///
    /// Each entry is decoded from one complete IB message, so every entry
    /// present is valid in isolation even when the enumeration was truncated.
    pub entries: Vec<OptionChainEntry>,

    /// `Some(reason)` if a mid-stream IB error ended enumeration early —
    /// `reason` is the formatted `ibapi::Error` — meaning
    /// [`entries`](Self::entries) may be incomplete. `None` on a clean
    /// end-of-enumeration; an empty `entries` with `None` means the underlying
    /// genuinely has no listed options for the requested `exchange` filter,
    /// not an error.
    ///
    /// The reason is a formatted string rather than a typed error for the same
    /// reasons as [`HistoricalTicks::truncation_error`]: do not match on the
    /// string contents — treat it as human-facing detail and use `.is_some()`
    /// as the programmatic "was this truncated?" check.
    pub truncation_error: Option<String>,
}

/// Consumes the wrapper and iterates its [`entries`](OptionChainResult::entries),
/// discarding [`truncation_error`](OptionChainResult::truncation_error). Lets
/// callers that only need the data write `for chain in chains { .. }`, matching
/// the pre-`OptionChainResult` `Vec<OptionChainEntry>` ergonomics; inspect the
/// flag before iterating when truncation matters.
impl IntoIterator for OptionChainResult {
    type Item = OptionChainEntry;
    type IntoIter = std::vec::IntoIter<OptionChainEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

/// What [`DataUntil`] reads: one of the `ibapi` subscription types the historical requests
/// return. Their decoder traits are private to `ibapi`, so each concrete type is listed.
trait TimedItems {
    type Item;
    /// The next item, blocking until one arrives or the subscription ends (`None`).
    fn next_item(&self) -> Option<Result<SubscriptionItem<Self::Item>, ibapi::Error>>;
    /// The next item, blocking up to `timeout`. `None` when the time is up or the subscription
    /// ended; [`ended`](Self::ended) tells which.
    fn next_item_within(
        &self,
        timeout: StdDuration,
    ) -> Option<Result<SubscriptionItem<Self::Item>, ibapi::Error>>;
    /// Whether the subscription ended, with nothing left to read.
    fn ended(&self) -> bool;
}

macro_rules! timed_items {
    ($subscription:ty, $item:ty) => {
        impl TimedItems for $subscription {
            type Item = $item;
            fn next_item(&self) -> Option<Result<SubscriptionItem<$item>, ibapi::Error>> {
                self.next()
            }
            fn next_item_within(
                &self,
                timeout: StdDuration,
            ) -> Option<Result<SubscriptionItem<$item>, ibapi::Error>> {
                self.next_timeout(timeout)
            }
            fn ended(&self) -> bool {
                self.is_terminated()
            }
        }
    };
}

timed_items!(TickSubscription<TickLast>, TickLast);
timed_items!(TickSubscription<TickBidAsk>, TickBidAsk);
timed_items!(Subscription<OptionChain>, OptionChain);

/// The data items of a subscription, read until it ends or `deadline` passes.
///
/// Notices are logged at `warn!` and skipped, as `ibapi`'s `iter_data()` does. Once `deadline`
/// passes, the next item is `Err(ibapi::Error::Timeout)` and iteration ends; the caller then fails
/// the request through [`timed_out`](Self::timed_out).
///
/// A subscription whose channel closes before IB's end marker (the connection dropped, or the
/// request was cancelled from elsewhere) yields `Err(ibapi::Error::UnexpectedEndOfStream)`, with
/// or without a deadline, so the caller records a truncation instead of a complete result.
/// `iter_data()` ends quietly there.
struct DataUntil<'a, S> {
    subscription: &'a S,
    deadline: Option<Instant>,
    operation: &'static str,
    timed_out: bool,
}

impl<'a, S: TimedItems> DataUntil<'a, S> {
    fn new(subscription: &'a S, deadline: Option<Instant>, operation: &'static str) -> Self {
        Self {
            subscription,
            deadline,
            operation,
            timed_out: false,
        }
    }

    /// [`DataError::RequestTimeout`] naming the request if the deadline passed before the
    /// subscription ended.
    fn timed_out(&self) -> Result<(), DataError> {
        if self.timed_out {
            Err(DataError::RequestTimeout {
                operation: self.operation.to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

impl<S: TimedItems> Iterator for DataUntil<'_, S> {
    type Item = Result<S::Item, ibapi::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.timed_out {
            return None;
        }
        loop {
            let item = match self.deadline {
                None => self.subscription.next_item(),
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        None
                    } else {
                        self.subscription.next_item_within(remaining)
                    }
                }
            };
            // `ibapi` answers `None` for three things: the end marker (the subscription has
            // ended), the time running out, and a closed channel. A closed channel answers at
            // once, so only a `None` at or after the deadline is the time running out.
            let Some(item) = item else {
                if self.subscription.ended() {
                    return None;
                }
                if self
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    self.timed_out = true;
                    return Some(Err(ibapi::Error::Timeout));
                }
                return Some(Err(ibapi::Error::UnexpectedEndOfStream));
            };
            match item {
                Ok(SubscriptionItem::Data(data)) => return Some(Ok(data)),
                Ok(SubscriptionItem::Notice(notice)) => {
                    warn!(
                        %notice,
                        operation = self.operation,
                        "IB notice during a historical request"
                    );
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Drain a subscription iterator, stopping at the first `Err` yielded by
/// `iter`. Shared by `collect_ticks` and `fetch_option_chain` — both consume
/// the same `Subscription`/`iter_data()` machinery, whose `Err` is a genuine
/// mid-stream failure, not a warning-level notice (`iter_data()` already
/// filters non-fatal notices).
///
/// `map(&raw, seq)` decodes/validates a raw item; `seq` is the zero-based
/// index among *raw* items received so far (gaps from rejected items are not
/// reflected in `seq`, matching the numbering `tick_last_to_public_trade` uses
/// for ID generation). Returning `None` from `map` silently drops an invalid
/// item — a data-quality filter that does **not** count as truncation.
///
/// `on_error(&err)` runs exactly once, when `iter` yields `Err`, so callers
/// can log a stream-specific message before this function records the
/// formatted reason and stops.
///
/// Returns `(items, received, truncation_error)`, where `received` counts
/// *raw* items pulled from `iter` before `map`'s filtering — so it reflects
/// wire truncation rather than data-quality drops.
fn drain_until_error<Raw, Out>(
    iter: impl Iterator<Item = Result<Raw, ibapi::Error>>,
    capacity: usize,
    mut map: impl FnMut(&Raw, usize) -> Option<Out>,
    on_error: impl FnOnce(&ibapi::Error),
) -> (Vec<Out>, usize, Option<String>) {
    let mut items = Vec::with_capacity(capacity);
    let mut received = 0usize;
    let mut truncation_error = None;

    for (seq, item) in iter.enumerate() {
        let raw = match item {
            Ok(raw) => raw,
            // A timeout stops the drain without `on_error`'s truncation message. The reason is
            // still recorded, so `collect_ticks` skips its short-fetch warning, but no caller
            // returns it: each fails the whole request through `DataUntil::timed_out`.
            Err(ibapi::Error::Timeout) => {
                truncation_error = Some(ibapi::Error::Timeout.to_string());
                break;
            }
            Err(e) => {
                on_error(&e);
                truncation_error = Some(e.to_string());
                break;
            }
        };
        received += 1;
        if let Some(out) = map(&raw, seq) {
            items.push(out);
        }
    }

    (items, received, truncation_error)
}

/// Drain a tick iterator into a `HistoricalTicks<Out>` via
/// [`drain_until_error`], layering the tick-specific short-fetch heuristic on
/// top: the generic warning (`warn_if_short_tick_fetch`) fires only on a clean
/// end-of-data — never alongside a confirmed truncation — so call sites no
/// longer need to guard against a double warning themselves.
fn collect_ticks<Raw, Out>(
    iter: impl Iterator<Item = Result<Raw, ibapi::Error>>,
    symbol: &str,
    requested: i32,
    map: impl FnMut(&Raw, usize) -> Option<Out>,
    on_error: impl FnOnce(&ibapi::Error),
) -> HistoricalTicks<Out> {
    // Pre-size to the requested count (IB caps this at 1000); a mid-stream error
    // or short end-of-data only ever leaves this over-allocated, never under.
    let capacity = usize::try_from(requested).unwrap_or(0);
    let (ticks, received, truncation_error) = drain_until_error(iter, capacity, map, on_error);

    // Only run the ambiguous short-batch heuristic when the stream ended
    // gracefully; a confirmed truncation is already recorded and logged above.
    if truncation_error.is_none() {
        warn_if_short_tick_fetch(symbol, received, requested);
    }

    HistoricalTicks {
        ticks,
        truncation_error,
    }
}

/// Warn when a historical tick fetch returned fewer raw ticks than requested.
///
/// With ibapi 3.1+, an explicit mid-stream IB error is surfaced as an `Err` on
/// the tick iterator and logged separately by the fetch loop. This warning
/// therefore covers the *residual* ambiguous case: the stream ended gracefully
/// (end-of-data) with fewer than `requested` ticks, which is indistinguishable
/// through the public API from a genuine end-of-data within the requested range.
///
/// This is therefore a **best-effort, flag-for-investigation** signal, **not** a
/// precise error indicator: it fires on every legitimately short batch (e.g. the
/// available history ends before `requested` ticks) as well as on truncation.
/// Callers needing completeness guarantees should treat it as a prompt to retry
/// or paginate, not as proof of an error.
///
/// `received` is the count of *raw* ticks pulled from the subscription, before
/// non-finite-price filtering — so this reflects wire truncation rather than
/// data-quality drops (which are warned per-tick elsewhere).
fn warn_if_short_tick_fetch(symbol: &str, received: usize, requested: i32) {
    let requested = usize::try_from(requested).unwrap_or(0);
    if received < requested {
        warn!(
            symbol = %symbol,
            received,
            requested,
            "Historical tick fetch returned fewer ticks than requested; this is \
             usually a legitimate end-of-data boundary (any mid-stream IB error is \
             logged separately). Investigate or paginate if completeness is required."
        );
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
/// Returns `None` if the price is non-finite, or if the tick carries no usable
/// size (invalid data from IB — see the note on sizes below).
///
/// # Sizes are optional as of ibapi 4.0
///
/// `TickLast::size` is `Option<f64>`, not `i32`. `None` means TWS sent no value
/// at all — the field was absent, empty, or one of TWS's "unset" sentinels —
/// which is distinct from a real `Some(0.0)`. [`PublicTrade`] has no encoding
/// for "size unknown", and substituting zero would reintroduce exactly the
/// silent data loss the `Option<f64>` change fixed, so such a tick is dropped
/// with a `warn!` rather than fabricated. Fractional sizes (crypto, fractional
/// shares) now survive: the old `i32` decode truncated `0.5` to `0`.
fn tick_last_to_public_trade(tick: &TickLast, seq: usize) -> Option<PublicTrade> {
    if !tick.price.is_finite() {
        warn!(
            price = tick.price,
            "Historical tick has non-finite price, skipping"
        );
        return None;
    }

    let Some(size) = tick.size.filter(|size| size.is_finite()) else {
        warn!(
            size = ?tick.size,
            "Historical tick has no usable size, skipping"
        );
        return None;
    };

    let price = Decimal::try_from(tick.price).ok()?;
    let amount = Decimal::try_from(size).ok()?;

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
///
/// `size` is hashed through [`f64::to_bits`], as `price` already was, because
/// `f64` is not [`Hash`]. Hashing the `Option` rather than an unwrapped value
/// keeps "TWS sent no size" distinct from a real zero. Note that this makes the
/// generated id a function of the ibapi wire type: the same logical tick hashed
/// as `i32` under ibapi 3.x and as `Option<f64>` here produces different ids.
fn generate_tick_id(
    timestamp: OffsetDateTime,
    price: f64,
    size: Option<f64>,
    seq: usize,
) -> smol_str::SmolStr {
    let mut hasher = fnv::FnvHasher::default();
    timestamp.unix_timestamp_nanos().hash(&mut hasher);
    price.to_bits().hash(&mut hasher);
    size.map(f64::to_bits).hash(&mut hasher);
    seq.hash(&mut hasher);
    format_smolstr!("{:016x}", hasher.finish())
}

/// Convert an ibapi `TickBidAsk` to a rustrade `OrderBookL1`.
///
/// # Returns
///
/// Returns `None` if any price is non-finite, or if either side carries no
/// usable size (invalid data from IB).
///
/// As of ibapi 4.0 `size_bid`/`size_ask` are `Option<f64>`; see
/// [`tick_last_to_public_trade`] for why an absent size drops the tick rather
/// than decoding as zero. Both sides are required because [`Level`] has no
/// encoding for a known price at an unknown size, and a substituted zero would
/// misrepresent the book.
fn tick_bid_ask_to_order_book_l1(tick: &TickBidAsk) -> Option<OrderBookL1> {
    if !tick.price_bid.is_finite() || !tick.price_ask.is_finite() {
        warn!(
            bid = tick.price_bid,
            ask = tick.price_ask,
            "Historical tick has non-finite price, skipping"
        );
        return None;
    }

    let (Some(size_bid), Some(size_ask)) = (
        tick.size_bid.filter(|size| size.is_finite()),
        tick.size_ask.filter(|size| size.is_finite()),
    ) else {
        warn!(
            size_bid = ?tick.size_bid,
            size_ask = ?tick.size_ask,
            "Historical tick has no usable size, skipping"
        );
        return None;
    };

    let bid_price = Decimal::try_from(tick.price_bid).ok()?;
    let ask_price = Decimal::try_from(tick.price_ask).ok()?;
    let bid_amount = Decimal::try_from(size_bid).ok()?;
    let ask_amount = Decimal::try_from(size_ask).ok()?;

    Some(OrderBookL1 {
        last_update_time: parse_tick_timestamp(tick.timestamp)?,
        best_bid: Some(Level::new(bid_price, bid_amount)),
        best_ask: Some(Level::new(ask_price, ask_amount)),
    })
}

/// Map an ibapi [`BarSize`] to the shared [`IntervalStep`] used to compute a
/// candle's exclusive `close_time` boundary.
///
/// Every bar size is a fixed-length step except `Month`, which is a calendar
/// month (variable length) and must use [`IntervalStep::Months`]. A `Day`/`Week`
/// step is fixed but can still cross a month boundary (e.g. a Jan 31 daily bar →
/// `Feb 1 00:00 UTC`).
fn bar_size_to_step(bar_size: BarSize) -> IntervalStep {
    match bar_size {
        BarSize::Sec => IntervalStep::Fixed(ChronoDuration::seconds(1)),
        BarSize::Sec5 => IntervalStep::Fixed(ChronoDuration::seconds(5)),
        BarSize::Sec10 => IntervalStep::Fixed(ChronoDuration::seconds(10)),
        BarSize::Sec15 => IntervalStep::Fixed(ChronoDuration::seconds(15)),
        BarSize::Sec30 => IntervalStep::Fixed(ChronoDuration::seconds(30)),
        BarSize::Min => IntervalStep::Fixed(ChronoDuration::minutes(1)),
        BarSize::Min2 => IntervalStep::Fixed(ChronoDuration::minutes(2)),
        BarSize::Min3 => IntervalStep::Fixed(ChronoDuration::minutes(3)),
        BarSize::Min4 => IntervalStep::Fixed(ChronoDuration::minutes(4)),
        BarSize::Min5 => IntervalStep::Fixed(ChronoDuration::minutes(5)),
        BarSize::Min10 => IntervalStep::Fixed(ChronoDuration::minutes(10)),
        BarSize::Min15 => IntervalStep::Fixed(ChronoDuration::minutes(15)),
        BarSize::Min20 => IntervalStep::Fixed(ChronoDuration::minutes(20)),
        BarSize::Min30 => IntervalStep::Fixed(ChronoDuration::minutes(30)),
        BarSize::Hour => IntervalStep::Fixed(ChronoDuration::hours(1)),
        BarSize::Hour2 => IntervalStep::Fixed(ChronoDuration::hours(2)),
        BarSize::Hour3 => IntervalStep::Fixed(ChronoDuration::hours(3)),
        BarSize::Hour4 => IntervalStep::Fixed(ChronoDuration::hours(4)),
        BarSize::Hour8 => IntervalStep::Fixed(ChronoDuration::hours(8)),
        BarSize::Day => IntervalStep::Fixed(ChronoDuration::days(1)),
        BarSize::Week => IntervalStep::Fixed(ChronoDuration::weeks(1)),
        BarSize::Month => IntervalStep::Months(1),
    }
}

/// Convert an ibapi `Bar` to a rustrade [`Candle`].
///
/// `Bar.date` is the bar's **open** instant; the returned [`Candle::close_time`]
/// is the exclusive end-of-period boundary `open + interval`, computed via the
/// shared [`close_time_from_open`] helper from the requested `bar_size`.
///
/// Consequences of the boundary contract for IBKR consumers:
/// - **Daily/weekly/monthly** bars carry a calendar `Date` (anchored to midnight
///   UTC). The boundary is the **next** UTC period start, **not** the exchange
///   session close — e.g. a Jan 15 daily bar → `close_time = Jan 16 00:00 UTC`,
///   and a monthly Jan bar → `Feb 1 00:00 UTC` (calendar arithmetic, not
///   `+ Duration`). The bar's own date is therefore `close_time − interval`;
///   `close_time.date()` no longer equals the bar's date, so any
///   `group_by(close_time.date())` shifts by one interval. Do **not** compare
///   daily bars by `close_time.date()`.
/// - **Intraday** bars carry a full `OffsetDateTime`; the boundary is `open + interval`.
///
/// Note: `Bar::wap` (volume-weighted average price) is not mapped — rustrade's
/// `Candle` has no VWAP field.
///
/// # Errors
///
/// Returns `Err(DataError::Socket(...))` if the open timestamp or the computed
/// `close_time` falls outside the representable [`DateTime<Utc>`] range, or if any
/// price/volume value cannot be converted to `Decimal` (e.g. NaN/Infinity).
fn bar_to_candle(
    bar: &ibapi::market_data::historical::Bar,
    bar_size: BarSize,
) -> Result<Candle, DataError> {
    use ibapi::market_data::historical::BarTimestamp;

    // `Bar.date` is the bar's OPEN instant: daily/weekly/monthly bars carry a
    // calendar `Date` (anchored here to midnight UTC), intraday bars a full
    // `OffsetDateTime`.
    let (secs, nanos) = match bar.date {
        BarTimestamp::DateTime(dt) => (dt.unix_timestamp(), dt.nanosecond()),
        BarTimestamp::Date(d) => (d.midnight().assume_utc().unix_timestamp(), 0),
    };
    let open_time = DateTime::from_timestamp(secs, nanos).ok_or_else(|| {
        DataError::Socket(format!("IB timestamp {secs} out of DateTime<Utc> range"))
    })?;

    // close_time is the exclusive end-of-period boundary (open + interval).
    let close_time =
        close_time_from_open(open_time, bar_size_to_step(bar_size)).ok_or_else(|| {
            DataError::Socket(format!(
                "IB candle close_time overflow: open={open_time}, bar_size={bar_size:?}"
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
    // IB uses `-1` as the "not available" sentinel for both volume and trade
    // count (e.g. on MIDPOINT/BID/ASK bars). Map the sentinel to `None` (unknown)
    // rather than fabricating a zero — or, worse, a negative volume — that a
    // consumer could not distinguish from a genuine value.
    let volume = if bar.volume < 0.0 {
        None
    } else {
        Some(
            Decimal::try_from(bar.volume)
                .map_err(|e| DataError::Socket(format!("parse volume: {e}")))?,
        )
    };
    let trade_count = if bar.count < 0 {
        None
    } else {
        // Non-negative by the guard above.
        #[allow(clippy::cast_sign_loss)] // guarded: bar.count >= 0 in this branch
        Some(bar.count as u64)
    };

    Ok(Candle {
        close_time,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
    })
}

#[cfg(test)]
// Test code may unwrap/expect freely since panics indicate test failure
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use time::macros::datetime;

    /// Build an ibapi `Bar` at the given `BarTimestamp` with placeholder OHLCV.
    fn bar_at(
        date: ibapi::market_data::historical::BarTimestamp,
    ) -> ibapi::market_data::historical::Bar {
        ibapi::market_data::historical::Bar {
            date,
            open: 150.0,
            high: 155.0,
            low: 149.0,
            close: 153.5,
            volume: 1_000_000.0,
            wap: 152.0,
            count: 50_000,
        }
    }

    /// Requests are bounded by default, and the bound can be changed or removed.
    #[test]
    fn the_default_config_bounds_requests_at_two_minutes() {
        let config = IbkrHistoricalDataConfig::default();
        assert_eq!(config.request_timeout, Some(StdDuration::from_secs(120)));
        assert_eq!(
            config
                .with_request_timeout(Some(StdDuration::from_secs(5)))
                .request_timeout,
            Some(StdDuration::from_secs(5))
        );
        assert_eq!(config.with_request_timeout(None).request_timeout, None);
    }

    /// A zero timeout is refused before any connection is tried.
    #[test]
    fn a_zero_request_timeout_fails_connect() {
        let config =
            IbkrHistoricalDataConfig::default().with_request_timeout(Some(StdDuration::ZERO));
        let error = IbkrHistoricalData::connect_with("127.0.0.1:1", 1, config).unwrap_err();
        assert!(
            matches!(&error, DataError::Socket(message) if message.contains("one_shot_timeout")),
            "{error:?}"
        );
    }

    /// A bounded request's deadline is its timeout from now; an unbounded one has none.
    #[test]
    fn a_requests_deadline_is_its_timeout_from_now() {
        let before = Instant::now();
        let deadline = deadline(Some(StdDuration::from_secs(60))).unwrap();
        assert!(deadline >= before + StdDuration::from_secs(60));
        assert!(deadline <= Instant::now() + StdDuration::from_secs(60));
        assert_eq!(super::deadline(None), None);
    }

    /// A request that ran out of time is a timeout naming it; any other `ibapi` error stays a
    /// socket error.
    #[test]
    fn a_timed_out_request_is_a_request_timeout() {
        assert_eq!(
            request_error("fetch_candles", ibapi::Error::Timeout),
            DataError::RequestTimeout {
                operation: "fetch_candles".to_owned()
            }
        );
        assert_eq!(
            request_error("fetch_candles", ibapi::Error::Simple("no".to_owned())),
            DataError::Socket(format!(
                "fetch_candles: {}",
                ibapi::Error::Simple("no".to_owned())
            ))
        );
    }

    #[test]
    fn bar_to_candle_converts_all_fields() {
        use rust_decimal_macros::dec;

        // Intraday hourly bar opening at 16:00 → close_time = open + 1h = 17:00.
        let open = datetime!(2024-01-15 16:00 UTC);
        let bar = bar_at(open.into());

        let candle = bar_to_candle(&bar, BarSize::Hour).unwrap();

        assert_eq!(candle.open, dec!(150));
        assert_eq!(candle.high, dec!(155));
        assert_eq!(candle.low, dec!(149));
        assert_eq!(candle.close, dec!(153.5));
        assert_eq!(candle.volume, Some(dec!(1_000_000)));
        assert_eq!(candle.trade_count, Some(50_000));

        // close_time is the exclusive boundary: open + 1 hour.
        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp(open.unix_timestamp(), 0).unwrap() + ChronoDuration::hours(1)
        );
        assert_eq!(candle.close_time.hour(), 17);
    }

    #[test]
    fn bar_to_candle_maps_unavailable_sentinel_to_none() {
        let mut bar = bar_at(datetime!(2024-01-15 16:00 UTC).into());
        bar.count = -1; // IB returns -1 for "not available"
        bar.volume = -1.0; // same sentinel on volume-less bar types

        let candle = bar_to_candle(&bar, BarSize::Hour).unwrap();

        // The -1 sentinel is "unknown", reported as `None` — never a fabricated
        // zero (trade count) or a nonsensical negative (volume).
        assert_eq!(candle.trade_count, None);
        assert_eq!(candle.volume, None);
    }

    #[test]
    fn bar_to_candle_four_minute_boundary() {
        // `BarSize::Min4` arrived in ibapi 3.3.0. Pin its step here so the arm
        // added to `bar_size_to_step` is the 4-minute one and not a copy-paste
        // of a neighbouring arm — a wrong step silently mislabels every
        // `close_time` for this resolution rather than failing loudly.
        let open = datetime!(2024-01-15 16:00 UTC);
        let bar = bar_at(open.into());

        let candle = bar_to_candle(&bar, BarSize::Min4).unwrap();

        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp(open.unix_timestamp(), 0).unwrap()
                + ChronoDuration::minutes(4)
        );
        assert_eq!(candle.close_time.minute(), 4);
    }

    #[test]
    fn bar_to_candle_daily_boundary_is_next_day() {
        use ibapi::market_data::historical::BarTimestamp;
        use time::macros::date;

        // A Jan 15 daily bar (date-only, midnight UTC) → close_time = Jan 16 00:00 UTC.
        let bar = bar_at(BarTimestamp::Date(date!(2024 - 01 - 15)));
        let candle = bar_to_candle(&bar, BarSize::Day).unwrap();

        assert_eq!(candle.close_time.year(), 2024);
        assert_eq!(candle.close_time.month(), 1);
        assert_eq!(candle.close_time.day(), 16);
        assert_eq!(candle.close_time.hour(), 0);
        // close_time.date() no longer equals the bar's own date (it shifted +1 day).
        assert_ne!(candle.close_time.day(), 15);
    }

    #[test]
    fn bar_to_candle_daily_boundary_crosses_month() {
        use ibapi::market_data::historical::BarTimestamp;
        use time::macros::date;

        // A Jan 31 daily bar → Feb 1 00:00 UTC via Fixed(1 day).
        let bar = bar_at(BarTimestamp::Date(date!(2024 - 01 - 31)));
        let candle = bar_to_candle(&bar, BarSize::Day).unwrap();

        assert_eq!(candle.close_time.month(), 2);
        assert_eq!(candle.close_time.day(), 1);
    }

    #[test]
    fn bar_to_candle_monthly_boundary_is_calendar_month() {
        use ibapi::market_data::historical::BarTimestamp;
        use time::macros::date;

        // A January monthly bar → Feb 1 00:00 UTC via Months(1), NOT +30 days.
        let bar = bar_at(BarTimestamp::Date(date!(2024 - 01 - 01)));
        let candle = bar_to_candle(&bar, BarSize::Month).unwrap();

        assert_eq!(candle.close_time.year(), 2024);
        assert_eq!(candle.close_time.month(), 2);
        assert_eq!(candle.close_time.day(), 1);
        // Overflow / out-of-range boundary is surfaced as DataError, never a
        // silent fallback — covered directly by the helper's unit tests
        // (`close_time_from_open` returns None → DataError here).
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

    fn make_tick_last(unix_time: i64, price: f64, size: Option<f64>) -> TickLast {
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
        bid_size: Option<f64>,
        ask_price: f64,
        ask_size: Option<f64>,
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

        let tick = make_tick_last(1700000000, 150.25, Some(100.0));
        let trade = tick_last_to_public_trade(&tick, 0).unwrap();

        assert_eq!(trade.price, dec!(150.25));
        assert_eq!(trade.amount, dec!(100));
        assert!(trade.side.is_none());
        assert!(!trade.id.is_empty());
    }

    #[test]
    fn tick_last_rejects_non_finite_price() {
        let tick = make_tick_last(1700000000, f64::NAN, Some(100.0));
        assert!(tick_last_to_public_trade(&tick, 0).is_none());

        let tick = make_tick_last(1700000000, f64::INFINITY, Some(100.0));
        assert!(tick_last_to_public_trade(&tick, 0).is_none());
    }

    /// The point of ibapi 4.0's `Option<f64>` size: the old `i32` decode
    /// truncated a fractional wire size such as `0.5` to `0`, silently losing
    /// real data on crypto and fractional-share feeds.
    #[test]
    fn tick_last_preserves_fractional_size() {
        use rust_decimal_macros::dec;

        let tick = make_tick_last(1700000000, 150.25, Some(0.5));
        let trade = tick_last_to_public_trade(&tick, 0).expect("fractional size is valid");

        assert_eq!(trade.amount, dec!(0.5));
    }

    /// `None` is TWS sending no size at all, which is distinct from a real
    /// zero. `PublicTrade` cannot encode "size unknown", so the tick is dropped
    /// rather than fabricated as zero.
    #[test]
    fn tick_last_without_size_is_dropped() {
        assert!(tick_last_to_public_trade(&make_tick_last(1700000000, 150.25, None), 0).is_none());
    }

    /// A non-finite size is as unusable as a non-finite price.
    #[test]
    fn tick_last_with_non_finite_size_is_dropped() {
        let nan = make_tick_last(1700000000, 150.25, Some(f64::NAN));
        assert!(tick_last_to_public_trade(&nan, 0).is_none());

        let inf = make_tick_last(1700000000, 150.25, Some(f64::INFINITY));
        assert!(tick_last_to_public_trade(&inf, 0).is_none());
    }

    /// `Some(0.0)` is a genuine zero TWS reported, not an absent field, so it
    /// must survive where `None` is dropped.
    #[test]
    fn tick_last_keeps_a_real_zero_size() {
        use rust_decimal_macros::dec;

        let tick = make_tick_last(1700000000, 150.25, Some(0.0));
        let trade = tick_last_to_public_trade(&tick, 0).expect("a reported zero is real data");

        assert_eq!(trade.amount, dec!(0));
    }

    /// The generated id hashes the `Option`, so "no size" and a real zero do
    /// not collide.
    #[test]
    fn tick_id_distinguishes_absent_size_from_zero() {
        let absent = make_tick_last(1700000000, 150.25, None);
        let zero = make_tick_last(1700000000, 150.25, Some(0.0));

        assert_ne!(
            generate_tick_id(absent.timestamp, absent.price, absent.size, 0),
            generate_tick_id(zero.timestamp, zero.price, zero.size, 0)
        );
    }

    /// Either side missing a size makes the whole L1 snapshot unusable: `Level`
    /// has no encoding for a known price at an unknown size.
    #[test]
    fn tick_bid_ask_without_size_is_dropped() {
        let no_bid = make_tick_bid_ask(1700000000, 150.00, None, 150.05, Some(300.0));
        assert!(tick_bid_ask_to_order_book_l1(&no_bid).is_none());

        let no_ask = make_tick_bid_ask(1700000000, 150.00, Some(500.0), 150.05, None);
        assert!(tick_bid_ask_to_order_book_l1(&no_ask).is_none());
    }

    /// Fractional book sizes survive the same way trade sizes do.
    #[test]
    fn tick_bid_ask_preserves_fractional_sizes() {
        use rust_decimal_macros::dec;

        let tick = make_tick_bid_ask(1700000000, 150.00, Some(0.25), 150.05, Some(0.75));
        let l1 = tick_bid_ask_to_order_book_l1(&tick).expect("fractional sizes are valid");

        assert_eq!(l1.best_bid.expect("bid").amount, dec!(0.25));
        assert_eq!(l1.best_ask.expect("ask").amount, dec!(0.75));
    }

    #[test]
    fn tick_last_generates_unique_ids() {
        let tick1 = make_tick_last(1700000000, 150.25, Some(100.0));
        let tick2 = make_tick_last(1700000001, 150.25, Some(100.0));
        let tick3 = make_tick_last(1700000000, 150.26, Some(100.0));

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 0);
        let id3 = generate_tick_id(tick3.timestamp, tick3.price, tick3.size, 0);

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id2, id3);
    }

    #[test]
    fn tick_last_same_data_same_seq_same_id() {
        let tick1 = make_tick_last(1700000000, 150.25, Some(100.0));
        let tick2 = make_tick_last(1700000000, 150.25, Some(100.0));

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 0);

        assert_eq!(id1, id2);
    }

    #[test]
    fn tick_last_same_data_different_seq_different_id() {
        let tick1 = make_tick_last(1700000000, 150.25, Some(100.0));
        let tick2 = make_tick_last(1700000000, 150.25, Some(100.0));

        let id1 = generate_tick_id(tick1.timestamp, tick1.price, tick1.size, 0);
        let id2 = generate_tick_id(tick2.timestamp, tick2.price, tick2.size, 1);

        assert_ne!(id1, id2);
    }

    #[test]
    fn tick_bid_ask_converts_to_order_book_l1() {
        use rust_decimal_macros::dec;

        let tick = make_tick_bid_ask(1700000000, 150.00, Some(500.0), 150.05, Some(300.0));
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
        let tick = make_tick_bid_ask(1700000000, f64::NAN, Some(500.0), 150.05, Some(300.0));
        assert!(tick_bid_ask_to_order_book_l1(&tick).is_none());

        let tick = make_tick_bid_ask(1700000000, 150.00, Some(500.0), f64::INFINITY, Some(300.0));
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

    // ========================================================================
    // Short-fetch completeness mitigation (#122)
    // ========================================================================
    //
    // `warn_if_short_tick_fetch` has no return value to assert on — it only
    // emits a `tracing` warning. These tests exercise the threshold/conversion
    // logic for panic-freedom and document the intended boundary (fires on
    // `received < requested`, silent on `>=`, and the negative-`i32` guard).

    #[test]
    fn warn_if_short_tick_fetch_threshold_is_panic_free() {
        // received < requested → would warn
        warn_if_short_tick_fetch("AAPL", 500, 1000);
        // received == requested → silent
        warn_if_short_tick_fetch("AAPL", 1000, 1000);
        // received > requested (defensive; IB caps at requested) → silent
        warn_if_short_tick_fetch("AAPL", 1001, 1000);
        // zero requested → silent (0 < 0 is false)
        warn_if_short_tick_fetch("AAPL", 0, 0);
    }

    #[test]
    fn warn_if_short_tick_fetch_clamps_negative_requested_to_zero() {
        // A negative `number_of_ticks` must not panic on the i32→usize cast;
        // it clamps to 0, so any non-negative `received` is treated as complete.
        warn_if_short_tick_fetch("AAPL", 0, -1);
        warn_if_short_tick_fetch("AAPL", 5, i32::MIN);
    }

    // ========================================================================
    // collect_ticks: mid-stream error / truncation handling
    // ========================================================================
    //
    // These exercise the `Err`/truncation branch that the live integration
    // tests cannot reach without a real IB Gateway. `ibapi::Error::Simple` is a
    // trivially constructible variant, so a fake `vec![Ok, Err, Ok]` iterator
    // drives the exact match/break/bookkeeping the fetch methods rely on.

    #[test]
    fn collect_ticks_stops_and_records_reason_on_mid_stream_error() {
        // Two good ticks, then an IB error, then a tick that must never be
        // reached because iteration stops at the first `Err`.
        let items: Vec<Result<TickLast, ibapi::Error>> = vec![
            Ok(make_tick_last(1_700_000_000, 100.0, Some(10.0))),
            Ok(make_tick_last(1_700_000_001, 101.0, Some(10.0))),
            Err(ibapi::Error::Simple("boom".into())),
            Ok(make_tick_last(1_700_000_002, 999.0, Some(10.0))),
        ];
        let mut on_error_calls = 0;

        let fetched = collect_ticks(
            items.into_iter(),
            "TEST",
            10,
            tick_last_to_public_trade,
            |_e| on_error_calls += 1,
        );

        assert_eq!(
            fetched.ticks.len(),
            2,
            "ticks after the error must be dropped"
        );
        assert_eq!(
            fetched.truncation_error.as_deref(),
            Some("error occurred: boom"),
            "the formatted ibapi::Error must be recorded"
        );
        assert_eq!(on_error_calls, 1, "on_error fires exactly once");
    }

    #[test]
    fn collect_ticks_clean_end_of_data_is_not_truncated() {
        let items: Vec<Result<TickLast, ibapi::Error>> = vec![
            Ok(make_tick_last(1_700_000_000, 100.0, Some(10.0))),
            Ok(make_tick_last(1_700_000_001, 101.0, Some(10.0))),
        ];

        let fetched = collect_ticks(
            items.into_iter(),
            "TEST",
            2,
            tick_last_to_public_trade,
            |_e| panic!("on_error must not fire when the stream never yields Err"),
        );

        assert_eq!(fetched.ticks.len(), 2);
        assert_eq!(fetched.truncation_error, None);
    }

    #[test]
    fn collect_ticks_filters_non_finite_without_flagging_truncation() {
        // A NaN price is a data-quality drop, not a wire truncation: the tick is
        // filtered by the map closure but `truncation_error` stays `None`.
        let items: Vec<Result<TickLast, ibapi::Error>> = vec![
            Ok(make_tick_last(1_700_000_000, f64::NAN, Some(10.0))),
            Ok(make_tick_last(1_700_000_001, 100.0, Some(10.0))),
        ];

        let fetched = collect_ticks(
            items.into_iter(),
            "TEST",
            2,
            tick_last_to_public_trade,
            |_e| panic!("no error in this stream"),
        );

        assert_eq!(
            fetched.ticks.len(),
            1,
            "NaN tick dropped by the map closure"
        );
        assert_eq!(
            fetched.truncation_error, None,
            "filtering is not truncation"
        );
    }

    // ========================================================================
    // drain_until_error: shared drain loop (ticks + option chains)
    // ========================================================================
    //
    // The `collect_ticks` tests above exercise the helper through the tick
    // path; these drive it directly with `OptionChain` items — the
    // `fetch_option_chain` usage, whose truncation branch the live
    // integration tests cannot reach without a real IB Gateway.

    fn make_option_chain(exchange: &str) -> ibapi::contracts::OptionChain {
        ibapi::contracts::OptionChain {
            underlying_contract_id: 265598,
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            exchange: exchange.to_string(),
            expirations: vec!["20300118".to_string()],
            strikes: vec![100.0, 105.0],
        }
    }

    #[test]
    fn drain_until_error_keeps_entries_decoded_before_the_error() {
        // Two good chains, then an IB error, then a chain that must never be
        // reached because iteration stops at the first `Err`.
        let items: Vec<Result<ibapi::contracts::OptionChain, ibapi::Error>> = vec![
            Ok(make_option_chain("SMART")),
            Ok(make_option_chain("CBOE")),
            Err(ibapi::Error::Simple("pacing violation".into())),
            Ok(make_option_chain("NASDAQOM")),
        ];
        let mut on_error_calls = 0;

        let (entries, received, truncation_error) = drain_until_error(
            items.into_iter(),
            16,
            |chain, _seq| Some(OptionChainEntry::from_ib(chain)),
            |_e| on_error_calls += 1,
        );

        assert_eq!(
            entries
                .iter()
                .map(|e| e.exchange.as_str())
                .collect::<Vec<_>>(),
            vec!["SMART", "CBOE"],
            "entries decoded before the error are preserved; later ones dropped"
        );
        assert_eq!(received, 2, "raw items received before the error");
        // Substring, not exact match: the "error occurred: " prefix is
        // ibapi's thiserror wording, and `truncation_error`'s own contract
        // says not to match on the string contents.
        assert!(
            truncation_error
                .as_deref()
                .is_some_and(|reason| reason.contains("pacing violation")),
            "the formatted ibapi::Error must be recorded, got {truncation_error:?}"
        );
        assert_eq!(on_error_calls, 1, "on_error fires exactly once");
    }

    #[test]
    fn drain_until_error_clean_end_reports_no_truncation() {
        let items: Vec<Result<ibapi::contracts::OptionChain, ibapi::Error>> = vec![
            Ok(make_option_chain("SMART")),
            Ok(make_option_chain("CBOE")),
        ];

        let (entries, received, truncation_error) = drain_until_error(
            items.into_iter(),
            16,
            |chain, _seq| Some(OptionChainEntry::from_ib(chain)),
            |_e| panic!("on_error must not fire when the stream never yields Err"),
        );

        assert_eq!(entries.len(), 2);
        assert_eq!(received, 2);
        assert_eq!(truncation_error, None);
    }
    /// A timeout ends the drain without `on_error`'s truncation message. Its reason is still
    /// recorded, which keeps `collect_ticks` from warning of a short fetch; the caller then fails
    /// the request instead of returning it.
    #[test]
    fn drain_until_error_stops_at_a_timeout_without_on_error() {
        let items: Vec<Result<ibapi::contracts::OptionChain, ibapi::Error>> =
            vec![Ok(make_option_chain("SMART")), Err(ibapi::Error::Timeout)];

        let (entries, received, truncation_error) = drain_until_error(
            items.into_iter(),
            16,
            |chain, _seq| Some(OptionChainEntry::from_ib(chain)),
            |_e| panic!("on_error must not fire for a timeout"),
        );

        assert_eq!((entries.len(), received), (1, 1));
        assert!(truncation_error.is_some());
    }

    /// One answer of a [`Scripted`] subscription.
    enum Answer {
        Item(Result<SubscriptionItem<u32>, ibapi::Error>),
        /// Nothing arrives: a bounded read waits out its timeout, then answers `None`.
        Silent,
        /// The channel closed before the end marker: `None` at once, not ended.
        Closed,
        /// The end marker: `None`, ended.
        End,
    }

    /// A scripted subscription: each read pops the next answer, and the end once they run out.
    struct Scripted {
        answers: std::cell::RefCell<std::collections::VecDeque<Answer>>,
        ended: std::cell::Cell<bool>,
    }

    impl Scripted {
        fn new(answers: Vec<Answer>) -> Self {
            Self {
                answers: std::cell::RefCell::new(answers.into()),
                ended: std::cell::Cell::new(false),
            }
        }

        fn read(
            &self,
            timeout: Option<StdDuration>,
        ) -> Option<Result<SubscriptionItem<u32>, ibapi::Error>> {
            let answer = self.answers.borrow_mut().pop_front().unwrap_or(Answer::End);
            match answer {
                Answer::Item(item) => Some(item),
                Answer::Silent => {
                    let timeout = timeout.expect("an unbounded read of silence never returns");
                    std::thread::sleep(timeout);
                    None
                }
                Answer::Closed => None,
                Answer::End => {
                    self.ended.set(true);
                    None
                }
            }
        }
    }

    impl TimedItems for Scripted {
        type Item = u32;
        fn next_item(&self) -> Option<Result<SubscriptionItem<u32>, ibapi::Error>> {
            self.read(None)
        }
        fn next_item_within(
            &self,
            timeout: StdDuration,
        ) -> Option<Result<SubscriptionItem<u32>, ibapi::Error>> {
            self.read(Some(timeout))
        }
        fn ended(&self) -> bool {
            self.ended.get()
        }
    }

    fn notice() -> ibapi::Notice {
        ibapi::Notice {
            request_id: None,
            code: 2104,
            message: "farm ok".to_owned(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }
    }

    fn far_deadline() -> Option<Instant> {
        Some(Instant::now() + StdDuration::from_secs(60))
    }

    /// Data is yielded, notices skipped, and a subscription that ends before the deadline is not
    /// a timeout.
    #[test]
    fn data_until_reads_data_to_the_end() {
        for deadline in [None, far_deadline()] {
            let scripted = Scripted::new(vec![
                Answer::Item(Ok(SubscriptionItem::Data(1))),
                Answer::Item(Ok(SubscriptionItem::Notice(notice()))),
                Answer::Item(Ok(SubscriptionItem::Data(2))),
                Answer::End,
            ]);
            let mut data = DataUntil::new(&scripted, deadline, "test");
            let items: Vec<_> = (&mut data).collect::<Result<_, _>>().unwrap();
            assert_eq!(items, vec![1, 2], "{deadline:?}");
            assert_eq!(data.timed_out(), Ok(()));
        }
    }

    /// Nothing arriving while the subscription is still open is the time running out: one
    /// `Timeout` item, then the end, and the request fails with `RequestTimeout`.
    #[test]
    fn data_until_times_out_when_nothing_arrives_in_time() {
        let scripted = Scripted::new(vec![
            Answer::Item(Ok(SubscriptionItem::Data(1))),
            Answer::Silent,
        ]);
        let deadline = Instant::now() + StdDuration::from_millis(20);
        let mut data = DataUntil::new(&scripted, Some(deadline), "test");
        assert!(matches!(data.next(), Some(Ok(1))));
        assert!(matches!(data.next(), Some(Err(ibapi::Error::Timeout))));
        assert!(data.next().is_none());
        assert_eq!(
            data.timed_out(),
            Err(DataError::RequestTimeout {
                operation: "test".to_owned()
            })
        );
    }

    /// A deadline already past times out without reading.
    #[test]
    fn data_until_times_out_at_a_passed_deadline() {
        let scripted = Scripted::new(vec![Answer::Item(Ok(SubscriptionItem::Data(1)))]);
        let mut data = DataUntil::new(&scripted, Some(Instant::now()), "test");
        assert!(matches!(data.next(), Some(Err(ibapi::Error::Timeout))));
        assert_eq!(scripted.answers.borrow().len(), 1, "nothing was read");
    }

    /// A channel that closes before the end marker, well before any deadline, is an unexpected
    /// end, not a timeout: the caller records a truncation and the request does not fail.
    #[test]
    fn data_until_reports_a_closed_channel_as_an_unexpected_end() {
        for deadline in [None, far_deadline()] {
            let scripted = Scripted::new(vec![
                Answer::Item(Ok(SubscriptionItem::Data(1))),
                Answer::Closed,
            ]);
            let mut data = DataUntil::new(&scripted, deadline, "test");
            assert!(matches!(data.next(), Some(Ok(1))), "{deadline:?}");
            assert!(
                matches!(data.next(), Some(Err(ibapi::Error::UnexpectedEndOfStream))),
                "{deadline:?}"
            );
            assert_eq!(data.timed_out(), Ok(()), "{deadline:?}");
        }
    }

    /// A subscription error passes through, and is not a timeout.
    #[test]
    fn data_until_passes_a_subscription_error_through() {
        let scripted = Scripted::new(vec![Answer::Item(Err(ibapi::Error::Simple(
            "no".to_owned(),
        )))]);
        let mut data = DataUntil::new(&scripted, far_deadline(), "test");
        assert!(matches!(data.next(), Some(Err(ibapi::Error::Simple(_)))));
        assert_eq!(data.timed_out(), Ok(()));
    }
}
