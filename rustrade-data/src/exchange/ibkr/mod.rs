//! Interactive Brokers market data connector.
//!
//! Provides streaming market data from IB TWS/Gateway via the `ibapi` crate.
//!
//! # Testing Status
//!
//! **NOT TESTED in CI.** IBKR has not confirmed permission to use credentials
//! for CI, and requires IB Gateway/TWS running locally.
//!
//! **Tested locally (free subscriptions via IBKR Pro):**
//! - Tier 0: Connection, contract resolution
//! - Tier 1: Historical bars/ticks, L1 quotes, Greeks calculator, option chains
//!
//! **NOT tested locally (paid subscriptions):**
//! - Tier 2: L2 Market Depth — exchange-specific fees
//! - Tier 3: OPRA US Options — paid subscription
//!
//! Tests are organized by subscription tier (see `ibkr_integration.rs`).
//!
//! # Connection
//!
//! Requires TWS or IB Gateway running locally with API enabled:
//!
//! | Application | Live Port | Paper Port |
//! |-------------|-----------|------------|
//! | TWS         | 7496      | 7497       |
//! | IB Gateway  | 4001      | 4002       |
//!
//! Enable API in TWS/Gateway: Configure → API → Settings → Enable ActiveX and Socket Clients.
//!
//! # Architecture
//!
//! Unlike WebSocket-based exchanges, IB uses TCP sockets with a blocking API.
//! This module implements [`Stream`](futures::Stream) directly rather than using the [`Connector`]
//! trait designed for WebSocket exchanges.
//!
//! # Supported Data Types
//!
//! - **Quotes** ([`OrderBookL1`]): Best bid/ask via `market_data()` subscription
//! - **Depth** ([`OrderBookEvent`]): L2 order book via `market_depth()` subscription
//! - **Trades** ([`PublicTrade`]): Tick-by-tick trades via `tick_by_tick_all_last()` subscription
//! - **Historical** ([`Candle`]): OHLCV bars via `historical::IbkrHistoricalData`
//!
//! # Limitations
//!
//! - **Market depth limit**: IB allows max 3 concurrent depth subscriptions (error 309)
//! - **Trade side**: IB doesn't provide trade side; defaults to `Side::Buy`
//! - **Blocking API**: Uses `std::thread::spawn` for long-lived subscriptions
//! - **No auto-reconnect**: Caller responsibility per library philosophy
//!
//! # See Also
//!
//! - [IB API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/trader-workstation-api/)
//! - `rustrade_execution::client::ibkr` for order execution
//!
//! [`Connector`]: crate::exchange::Connector
//! [`OrderBookL1`]: crate::subscription::book::OrderBookL1
//! [`OrderBookEvent`]: crate::subscription::book::OrderBookEvent
//! [`PublicTrade`]: crate::subscription::trade::PublicTrade
//! [`Candle`]: crate::subscription::candle::Candle

pub mod depth;
pub mod greeks;
pub mod historical;
pub mod options;
pub mod quotes;
pub mod subscription;
pub mod trades;

use crate::{
    error::DataError,
    event::{DataKind, MarketEvent},
};
use chrono::Utc;
use depth::DepthAggregator;
use greeks::GreeksAggregator;
use ibapi::{client::blocking::Client, contracts::SecurityType};
use quotes::QuoteAggregator;
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, ibkr::ContractRegistry};
use serde::{Deserialize, Serialize};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use subscription::{IbkrSubscription, IbkrSubscriptionKind};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for the IBKR market data stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrStreamConfig {
    /// TWS/Gateway host (e.g., "127.0.0.1")
    pub host: String,
    /// TWS/Gateway port (7496=TWS live, 7497=TWS paper, 4001=GW live, 4002=GW paper)
    pub port: u16,
    /// Client ID (must be unique per connection).
    ///
    /// **Important**: The default value (100) is a placeholder. If you have multiple
    /// IB connections (e.g., both market data and execution), each must use a different
    /// client_id or IB will reject the second connection.
    pub client_id: i32,
}

impl Default for IbkrStreamConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 100,
        }
    }
}

/// Market data stream from Interactive Brokers.
///
/// Wraps an `mpsc::UnboundedReceiver` that receives events from background
/// worker threads running ibapi subscriptions.
///
/// # Thread Architecture
///
/// - One worker thread is spawned per subscription during `init()`
/// - Worker threads are detached (their `JoinHandle`s are not stored)
/// - Worker threads terminate when:
///   - The channel closes (this stream is dropped)
///   - The IB subscription ends (disconnect or server-side cancel)
///
/// # Memory Usage
///
/// The internal channel is **unbounded**. If the consumer processes events
/// slower than IB produces them, memory usage will grow without limit. This
/// design prioritizes data completeness over memory safety — a slow consumer
/// causes heap growth rather than dropped events. Callers should ensure
/// adequate processing capacity.
///
/// # Panic Handling
///
/// Worker threads are wrapped in `catch_unwind`. If a worker panics, a terminal
/// `DataError::Socket` is sent to the stream describing the panic. The channel
/// remains open for other subscriptions. No automatic recovery or reconnection.
#[derive(Debug)]
pub struct IbkrMarketStream<K> {
    rx: mpsc::UnboundedReceiver<Result<MarketEvent<K, DataKind>, DataError>>,
    client: Arc<Client>,
}

impl<K> futures::Stream for IbkrMarketStream<K> {
    type Item = Result<MarketEvent<K, DataKind>, DataError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_recv(cx)
    }
}

impl<K> IbkrMarketStream<K>
where
    K: Clone + Send + 'static,
{
    /// Initialize a market data stream with the given subscriptions.
    ///
    /// # Arguments
    ///
    /// * `config` - Connection configuration
    /// * `contracts` - Contract registry for instrument resolution
    /// * `subscriptions` - List of subscriptions to activate
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Connection to IB fails
    /// - No subscriptions could be activated (all contracts missing from registry)
    pub fn init(
        config: IbkrStreamConfig,
        contracts: Arc<ContractRegistry>,
        subscriptions: Vec<IbkrSubscription<K>>,
    ) -> Result<Self, DataError> {
        let url = format!("{}:{}", config.host, config.port);
        info!(%url, client_id = config.client_id, "Connecting to IB for market data");

        let client = Client::connect(&url, config.client_id)
            .map_err(|e| DataError::Socket(format!("IB connect: {e}")))?;

        let client = Arc::new(client);
        let (tx, rx) = mpsc::unbounded_channel();

        let mut active_subscriptions = 0;

        // Spawn a worker thread for each subscription
        for sub in subscriptions {
            let contract = match contracts.get_contract(&sub.instrument) {
                Some(c) => c,
                None => {
                    warn!(
                        instrument = %sub.instrument,
                        "Contract not found in registry, skipping subscription"
                    );
                    continue;
                }
            };

            let spawn_result = match sub.kind {
                IbkrSubscriptionKind::Quotes => Self::run_quotes_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    tx.clone(),
                ),
                IbkrSubscriptionKind::Depth { rows } => Self::run_depth_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    rows,
                    tx.clone(),
                ),
                IbkrSubscriptionKind::Trades => Self::run_trades_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    tx.clone(),
                ),
                IbkrSubscriptionKind::OptionGreeks => Self::run_option_greeks_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    tx.clone(),
                ),
            };

            match spawn_result {
                Ok(()) => active_subscriptions += 1,
                Err(e) => {
                    warn!(instrument = %sub.instrument, error = %e, "Failed to spawn subscription worker");
                }
            }
        }

        if active_subscriptions == 0 {
            return Err(DataError::Socket(
                "No subscriptions activated (check logs for per-subscription errors)".to_string(),
            ));
        }

        Ok(Self { rx, client })
    }

    /// Disconnect from IB Gateway.
    ///
    /// Signals the client to shut down, which will cause worker threads to exit
    /// when they next attempt an IB operation. This releases the client ID for reuse.
    ///
    /// Call this before dropping to ensure IB Gateway releases the connection promptly.
    /// Worker threads will terminate when they observe the disconnected state.
    ///
    /// This is idempotent — calling it multiple times is safe.
    pub fn disconnect(&self) {
        debug!("Disconnecting IbkrMarketStream");
        self.client.disconnect();
    }

    fn run_quotes_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        key: K,
        tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    ) -> Result<(), DataError> {
        let symbol = contract.symbol.to_string();
        let symbol_clone = symbol.clone();
        let tx_panic = tx.clone();

        std::thread::Builder::new()
            .name(format!("ibkr-quotes-{symbol}"))
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, etc.) remains usable. On panic the thread exits,
                // tx is dropped, and the stream closes — caller observes EOF.
                let result = catch_unwind(AssertUnwindSafe(|| {
                    debug!(symbol = %symbol, "Starting quotes subscription");

                    let sub = match client.market_data(&contract).subscribe() {
                        Ok(s) => s,
                        Err(e) => {
                            error!(symbol = %symbol, error = %e, "Failed to subscribe to quotes");
                            let _ = tx.send(Err(DataError::Socket(format!(
                                "quotes subscription {symbol}: {e}"
                            ))));
                            return;
                        }
                    };

                    let mut aggregator = QuoteAggregator::new();

                    // ibapi 3.x: `iter_data()` yields `Result<TickTypes, Error>`,
                    // filtering subscription-level notices. Surface errors as a
                    // terminal event (observable failures over silent ones).
                    for tick in sub.iter_data() {
                        let tick = match tick {
                            Ok(t) => t,
                            Err(e) => {
                                error!(symbol = %symbol, error = %e, "Quotes subscription error");
                                let _ = tx.send(Err(DataError::Socket(format!(
                                    "quotes subscription {symbol}: {e}"
                                ))));
                                break;
                            }
                        };
                        let now = Utc::now();
                        if let Some(l1) = aggregator.update(&tick, now) {
                            let event = MarketEvent {
                                time_exchange: l1.last_update_time,
                                time_received: now,
                                exchange: ExchangeId::Ibkr,
                                instrument: key.clone(),
                                kind: DataKind::OrderBookL1(l1),
                            };

                            if tx.send(Ok(event)).is_err() {
                                break;
                            }
                        }
                    }

                    debug!(symbol = %symbol, "Quotes subscription ended");
                }));

                if let Err(panic_info) = result {
                    let msg = panic_info
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_info.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!(symbol = %symbol_clone, "Quotes worker panicked: {msg}");
                    let _ = tx_panic.send(Err(DataError::Socket(format!(
                        "quotes subscription {symbol_clone} panicked: {msg}"
                    ))));
                }
            })
            .map_err(|e| DataError::Socket(format!("Failed to spawn quotes thread: {e}")))?;
        Ok(())
    }

    fn run_depth_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        key: K,
        rows: i32,
        tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    ) -> Result<(), DataError> {
        let symbol = contract.symbol.to_string();
        let symbol_clone = symbol.clone();
        let tx_panic = tx.clone();

        std::thread::Builder::new()
            .name(format!("ibkr-depth-{symbol}"))
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, etc.) remains usable. On panic the thread exits,
                // tx is dropped, and the stream closes — caller observes EOF.
                let result = catch_unwind(AssertUnwindSafe(|| {
                    debug!(symbol = %symbol, rows, "Starting depth subscription");

                    // Default SmartDepth::No: we aggregate into a simple anonymous book
                    // without tracking market maker attribution from MarketDepthL2 events.
                    let sub = match client.market_depth(&contract, rows).subscribe() {
                        Ok(s) => s,
                        Err(e) => {
                            error!(symbol = %symbol, error = %e, "Failed to subscribe to depth");
                            let _ = tx.send(Err(DataError::Socket(format!(
                                "depth subscription {symbol}: {e}"
                            ))));
                            return;
                        }
                    };

                    let mut aggregator = DepthAggregator::new();

                    for depth in sub.iter_data() {
                        let depth = match depth {
                            Ok(d) => d,
                            Err(e) => {
                                error!(symbol = %symbol, error = %e, "Depth subscription error");
                                let _ = tx.send(Err(DataError::Socket(format!(
                                    "depth subscription {symbol}: {e}"
                                ))));
                                break;
                            }
                        };
                        if let Some(book_event) = aggregator.update(&depth) {
                            // IB's MarketDepth events have no timestamp. We use time_received
                            // for both fields. Consumers should NOT interpret time_exchange
                            // as actual exchange timestamp for depth events.
                            let now = Utc::now();
                            let event = MarketEvent {
                                time_exchange: now,
                                time_received: now,
                                exchange: ExchangeId::Ibkr,
                                instrument: key.clone(),
                                kind: DataKind::OrderBook(book_event),
                            };

                            if tx.send(Ok(event)).is_err() {
                                break;
                            }
                        }
                    }

                    debug!(symbol = %symbol, "Depth subscription ended");
                }));

                if let Err(panic_info) = result {
                    let msg = panic_info
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_info.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!(symbol = %symbol_clone, "Depth worker panicked: {msg}");
                    let _ = tx_panic.send(Err(DataError::Socket(format!(
                        "depth subscription {symbol_clone} panicked: {msg}"
                    ))));
                }
            })
            .map_err(|e| DataError::Socket(format!("Failed to spawn depth thread: {e}")))?;
        Ok(())
    }

    fn run_trades_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        key: K,
        tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    ) -> Result<(), DataError> {
        let symbol = contract.symbol.to_string();
        let symbol_clone = symbol.clone();
        let tx_panic = tx.clone();

        std::thread::Builder::new()
            .name(format!("ibkr-trades-{symbol}"))
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, etc.) remains usable. On panic the thread exits,
                // tx is dropped, and the stream closes — caller observes EOF.
                let result = catch_unwind(AssertUnwindSafe(|| {
                    debug!(symbol = %symbol, "Starting trades subscription");

                    let sub = match client.tick_by_tick(&contract, 0).all_last() {
                        Ok(s) => s,
                        Err(e) => {
                            error!(symbol = %symbol, error = %e, "Failed to subscribe to trades");
                            let _ = tx.send(Err(DataError::Socket(format!(
                                "trades subscription {symbol}: {e}"
                            ))));
                            return;
                        }
                    };

                    for trade in sub.iter_data() {
                        let trade = match trade {
                            Ok(t) => t,
                            Err(e) => {
                                error!(symbol = %symbol, error = %e, "Trades subscription error");
                                let _ = tx.send(Err(DataError::Socket(format!(
                                    "trades subscription {symbol}: {e}"
                                ))));
                                break;
                            }
                        };
                        let now = Utc::now();
                        let public_trade = match trades::from_ib_trade(&trade) {
                            Some(t) => t,
                            None => continue, // Skip invalid trades (NaN/Inf price or size)
                        };
                        let time_exchange = trades::parse_trade_time(&trade, now);

                        let event = MarketEvent {
                            time_exchange,
                            time_received: now,
                            exchange: ExchangeId::Ibkr,
                            instrument: key.clone(),
                            kind: DataKind::Trade(public_trade),
                        };

                        if tx.send(Ok(event)).is_err() {
                            break;
                        }
                    }

                    debug!(symbol = %symbol, "Trades subscription ended");
                }));

                if let Err(panic_info) = result {
                    let msg = panic_info
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_info.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!(symbol = %symbol_clone, "Trades worker panicked: {msg}");
                    let _ = tx_panic.send(Err(DataError::Socket(format!(
                        "trades subscription {symbol_clone} panicked: {msg}"
                    ))));
                }
            })
            .map_err(|e| DataError::Socket(format!("Failed to spawn trades thread: {e}")))?;
        Ok(())
    }

    fn run_option_greeks_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        key: K,
        tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    ) -> Result<(), DataError> {
        if contract.security_type != SecurityType::Option {
            return Err(DataError::Socket(format!(
                "option Greeks subscription requires SecurityType::Option, got {:?} for {}",
                contract.security_type, contract.symbol
            )));
        }

        let symbol = contract.symbol.to_string();
        let symbol_clone = symbol.clone();
        let tx_panic = tx.clone();

        std::thread::Builder::new()
            .name(format!("ibkr-greeks-{symbol}"))
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, etc.) remains usable. On panic the thread exits,
                // tx is dropped, and the stream closes — caller observes EOF.
                let result = catch_unwind(AssertUnwindSafe(|| {
                    debug!(symbol = %symbol, "Starting option Greeks subscription");

                    let sub = match client.market_data(&contract).subscribe() {
                        Ok(s) => s,
                        Err(e) => {
                            error!(symbol = %symbol, error = %e, "Failed to subscribe to option Greeks");
                            let _ = tx.send(Err(DataError::Socket(format!(
                                "option Greeks subscription {symbol}: {e}"
                            ))));
                            return;
                        }
                    };

                    let aggregator = GreeksAggregator::new();

                    for tick in sub.iter_data() {
                        let tick = match tick {
                            Ok(t) => t,
                            Err(e) => {
                                error!(symbol = %symbol, error = %e, "Option Greeks subscription error");
                                let _ = tx.send(Err(DataError::Socket(format!(
                                    "option Greeks subscription {symbol}: {e}"
                                ))));
                                break;
                            }
                        };
                        if let Some(greeks) = aggregator.update(&tick) {
                            let now = Utc::now();
                            let event = MarketEvent {
                                time_exchange: now,
                                time_received: now,
                                exchange: ExchangeId::Ibkr,
                                instrument: key.clone(),
                                kind: DataKind::OptionGreeks(greeks),
                            };

                            if tx.send(Ok(event)).is_err() {
                                break;
                            }
                        }
                    }

                    debug!(symbol = %symbol, "Option Greeks subscription ended");
                }));

                if let Err(panic_info) = result {
                    let msg = panic_info
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_info.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    error!(symbol = %symbol_clone, "Option Greeks worker panicked: {msg}");
                    let _ = tx_panic.send(Err(DataError::Socket(format!(
                        "option Greeks subscription {symbol_clone} panicked: {msg}"
                    ))));
                }
            })
            .map_err(|e| DataError::Socket(format!("Failed to spawn option Greeks thread: {e}")))?;
        Ok(())
    }
}

impl<K> Drop for IbkrMarketStream<K> {
    fn drop(&mut self) {
        debug!("Dropping IbkrMarketStream, disconnecting client");
        self.client.disconnect();
    }
}

/// Convert f64 to Decimal, returning None for invalid values.
///
/// Returns `None` for NaN, Infinity, and values that cannot be represented
/// as Decimal (e.g., IB's DBL_MAX sentinel for "not available").
///
/// Note: f64→Decimal conversion introduces representation error (e.g., 0.1_f64
/// becomes 0.1000000000000000055511151231257827021181583404541015625). This is
/// unavoidable with IB's f64-based API.
pub(crate) fn decimal_from_f64(value: f64) -> Option<Decimal> {
    if !value.is_finite() {
        return None;
    }
    Decimal::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_from_f64_handles_special_values() {
        // Normal values
        assert!(decimal_from_f64(100.0).is_some());
        assert!(decimal_from_f64(-50.5).is_some());
        assert!(decimal_from_f64(0.0).is_some());

        // NaN and Infinity
        assert!(decimal_from_f64(f64::NAN).is_none());
        assert!(decimal_from_f64(f64::INFINITY).is_none());
        assert!(decimal_from_f64(f64::NEG_INFINITY).is_none());

        // f64::MAX is too large for Decimal
        assert!(
            decimal_from_f64(f64::MAX).is_none(),
            "f64::MAX should not convert to Decimal"
        );
    }
}
