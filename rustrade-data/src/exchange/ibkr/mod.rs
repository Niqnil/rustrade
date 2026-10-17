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
// `flex` is private: its public surface is re-exported flat below (matching the `massive`
// reconciliation module), so callers use a single `ibkr::IbkrFlex*` path rather than the
// internal submodule layout.
mod flex;
pub mod greeks;
pub mod historical;
pub mod options;
pub mod quotes;
pub mod subscription;
pub mod trades;

pub use flex::{
    FlexPollPolicy, IbkrFlexClient, IbkrFlexConfig, IbkrFlexCorporateAction, IbkrFlexError,
    IbkrReorgType, parse_corporate_actions,
};

use crate::{
    error::DataError,
    event::{DataKind, MarketEvent},
};
use chrono::Utc;
use depth::DepthAggregator;
use greeks::GreeksAggregator;
use ibapi::{
    client::blocking::Client, contracts::SecurityType, market_data::realtime::MarketDepths,
    subscriptions::SubscriptionItem,
};
use quotes::QuoteAggregator;
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, ibkr::ContractRegistry};
use serde::{Deserialize, Serialize};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
/// - The stream ends once every worker has exited. A worker ended by an error first sends it as
///   a terminal `DataError::Socket`, except when [`disconnect`](Self::disconnect) ended it.
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
    /// Set by [`disconnect`](Self::disconnect) before it closes the connection, so that a worker
    /// can tell the caller's own close from a shutdown `ibapi` or TWS/Gateway caused: `ibapi`
    /// reports both alike, as `Error::Shutdown` to a running subscription and as
    /// `Error::ConnectionReset` to one still being sent. Once it is set, any error that ends a
    /// worker is taken for the caller's close, even one `ibapi` raised on its own just before.
    closing: Arc<AtomicBool>,
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
    /// * `contracts` - Contract registry for instrument resolution. Subscriptions look their
    ///   contracts up by name only, and IB resolves each contract description on request, so
    ///   contracts registered with
    ///   [`register_by_name_only`](rustrade_instrument::ibkr::ContractRegistry::register_by_name_only)
    ///   need not be resolved first.
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
        let closing = Arc::new(AtomicBool::new(false));
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
                    closing.clone(),
                ),
                IbkrSubscriptionKind::Depth { rows } => Self::run_depth_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    rows,
                    tx.clone(),
                    closing.clone(),
                ),
                IbkrSubscriptionKind::Trades => Self::run_trades_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    tx.clone(),
                    closing.clone(),
                ),
                IbkrSubscriptionKind::OptionGreeks => Self::run_option_greeks_subscription(
                    client.clone(),
                    contract,
                    sub.key.clone(),
                    tx.clone(),
                    closing.clone(),
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

        Ok(Self {
            rx,
            client,
            closing,
        })
    }

    fn run_quotes_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        key: K,
        tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
        closing: Arc<AtomicBool>,
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
                            report_worker_error(&closing, &tx, "quotes subscription", &symbol, e);
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
                                report_worker_error(
                                    &closing,
                                    &tx,
                                    "quotes subscription",
                                    &symbol,
                                    e,
                                );
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
        closing: Arc<AtomicBool>,
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
                            report_worker_error(&closing, &tx, "depth subscription", &symbol, e);
                            return;
                        }
                    };

                    let mut aggregator = DepthAggregator::new();

                    // `iter()`, not `iter_data()`, so that the subscription's notices are
                    // logged below. The venue reset (317) is not one of them: `ibapi`
                    // delivers it as `MarketDepths::Reset` data.
                    for item in sub.iter() {
                        let item = match item {
                            Ok(i) => i,
                            Err(e) => {
                                report_worker_error(
                                    &closing,
                                    &tx,
                                    "depth subscription",
                                    &symbol,
                                    e,
                                );
                                break;
                            }
                        };

                        let depth = match item {
                            SubscriptionItem::Data(d) => {
                                if matches!(d, MarketDepths::Reset) {
                                    // `aggregator.update` drops the local book and returns
                                    // the emptied one, which is forwarded below.
                                    warn!(
                                        symbol = %symbol,
                                        "Market depth RESET by venue; dropping local book"
                                    );
                                }
                                d
                            }
                            // A notice is non-fatal and carries no depth
                            // payload. `iter_data()` used to log these at `warn!`; keep
                            // that, so switching iterators loses no observability.
                            SubscriptionItem::Notice(notice) => {
                                warn!(
                                    symbol = %symbol,
                                    code = notice.code,
                                    message = %notice.message,
                                    "Depth subscription notice"
                                );
                                continue;
                            }
                        };
                        // IB's MarketDepth events have no timestamp. Stamp `now`
                        // BEFORE aggregating so the book's `time_received` (threaded
                        // into the OrderBook) matches the MarketEvent envelope's.
                        // Consumers should NOT interpret the MarketEvent's
                        // time_exchange as a real venue timestamp for depth events.
                        let now = Utc::now();
                        if let Some(book_event) = aggregator.update(&depth, now) {
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
        closing: Arc<AtomicBool>,
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
                            report_worker_error(&closing, &tx, "trades subscription", &symbol, e);
                            return;
                        }
                    };

                    for trade in sub.iter_data() {
                        let trade = match trade {
                            Ok(t) => t,
                            Err(e) => {
                                report_worker_error(
                                    &closing,
                                    &tx,
                                    "trades subscription",
                                    &symbol,
                                    e,
                                );
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
        closing: Arc<AtomicBool>,
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
                            report_worker_error(
                                &closing,
                                &tx,
                                "option Greeks subscription",
                                &symbol,
                                e,
                            );
                            return;
                        }
                    };

                    let aggregator = GreeksAggregator::new();

                    for tick in sub.iter_data() {
                        let tick = match tick {
                            Ok(t) => t,
                            Err(e) => {
                                report_worker_error(
                                    &closing,
                                    &tx,
                                    "option Greeks subscription",
                                    &symbol,
                                    e,
                                );
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

impl<K> IbkrMarketStream<K> {
    /// Disconnect from IB Gateway.
    ///
    /// Closes the connection to IB Gateway, which ends every worker's subscription at once. The
    /// stream then ends (`None`) once every worker has exited, with no error item for the close:
    /// a close the caller asked for is not a failure. A shutdown `ibapi` or TWS/Gateway causes
    /// instead, such as reconnection given up or the API session ended, still reaches the stream
    /// as one terminal [`DataError::Socket`] per subscription; one that comes just before this
    /// call, but reaches a worker only after it, ends that worker quietly too. IB Gateway frees the
    /// client ID once it has processed the close, which can take a moment. Dropping the stream
    /// disconnects too.
    ///
    /// This is idempotent — calling it multiple times is safe.
    pub fn disconnect(&self) {
        debug!("Disconnecting IbkrMarketStream");
        self.closing.store(true, Ordering::Release);
        self.client.disconnect();
    }
}

impl<K> Drop for IbkrMarketStream<K> {
    fn drop(&mut self) {
        debug!("Dropping IbkrMarketStream, disconnecting client");
        self.disconnect();
    }
}

/// End a worker on `error`: log it, and send it to the stream as the worker's terminal
/// [`DataError::Socket`], `"{stage} {symbol}: {error}"`.
///
/// Once `closing` is set, the stream's own [`disconnect`](IbkrMarketStream::disconnect) has
/// begun, and any error is logged at debug and nothing is sent: `ibapi` reports that close as
/// more than one error, depending on how far the worker got, and the caller is ending the
/// stream anyway.
fn report_worker_error<T>(
    closing: &AtomicBool,
    tx: &mpsc::UnboundedSender<Result<T, DataError>>,
    stage: &str,
    symbol: &str,
    error: ibapi::Error,
) {
    if closing.load(Ordering::Acquire) {
        debug!(symbol, error = %error, "{stage} ended by disconnect");
        return;
    }
    error!(symbol, error = %error, "{stage} failed");
    let _ = tx.send(Err(DataError::Socket(format!("{stage} {symbol}: {error}"))));
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

    fn worker_error_sent(closing: bool, error: ibapi::Error) -> Option<String> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Result<(), DataError>>();
        report_worker_error(
            &AtomicBool::new(closing),
            &tx,
            "quotes subscription",
            "AAPL",
            error,
        );
        match rx.try_recv() {
            Ok(Err(DataError::Socket(msg))) => Some(msg),
            Ok(other) => panic!("unexpected item: {other:?}"),
            Err(_) => None,
        }
    }

    #[test]
    fn a_shutdown_the_caller_did_not_ask_for_reaches_the_stream() {
        assert_eq!(
            worker_error_sent(false, ibapi::Error::Shutdown),
            Some(format!(
                "quotes subscription AAPL: {}",
                ibapi::Error::Shutdown
            ))
        );
    }

    #[test]
    fn a_connection_reset_the_caller_did_not_ask_for_reaches_the_stream() {
        assert!(worker_error_sent(false, ibapi::Error::ConnectionReset).is_some());
    }

    #[test]
    fn the_callers_own_disconnect_ends_a_running_worker_without_an_error_item() {
        assert_eq!(worker_error_sent(true, ibapi::Error::Shutdown), None);
    }

    #[test]
    fn the_callers_own_disconnect_ends_a_subscribing_worker_without_an_error_item() {
        // `ibapi` refuses a subscription sent after the close began with `ConnectionReset`.
        assert_eq!(worker_error_sent(true, ibapi::Error::ConnectionReset), None);
    }

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
