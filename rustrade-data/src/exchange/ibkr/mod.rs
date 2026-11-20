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
//! - Tier 1: Historical bars/ticks, L1 quotes, Greeks calculator, option chains, delayed option
//!   L1 quotes on many lines at once (no OPRA subscription needed)
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
//! - **Market data lines**: IB caps concurrent subscriptions per username, shared by TWS and
//!   every API connection on it, and offers no API to read the cap or the lines in use. A
//!   subscription past the cap ends with [`DataError::SubscriptionLimitReached`] (IB code 101);
//!   the stream's other subscriptions continue
//! - **Market depth limit**: IB allows max 3 concurrent depth subscriptions at the minimum line
//!   allocation; past it a depth subscription ends with
//!   [`DataError::SubscriptionLimitReached`] (IB code 309)
//! - **Entitlements**: a subscription the account has no market data for ends with
//!   [`DataError::MarketDataNotAvailable`]. Without a live subscription (such as OPRA for US
//!   options), ask for delayed data with
//!   [`IbkrStreamConfig::with_market_data_type`]
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
//! [`DataError::SubscriptionLimitReached`]: crate::error::DataError::SubscriptionLimitReached
//! [`DataError::MarketDataNotAvailable`]: crate::error::DataError::MarketDataNotAvailable
//! [`IbkrStreamConfig::with_market_data_type`]: crate::exchange::ibkr::IbkrStreamConfig::with_market_data_type

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
use chrono::{DateTime, Utc};
use depth::DepthAggregator;
use greeks::GreeksAggregator;
use ibapi::{
    Notice,
    client::blocking::Client,
    contracts::SecurityType,
    market_data::{MarketDataType, realtime::MarketDepths},
    subscriptions::SubscriptionItem,
};
use quotes::QuoteAggregator;
use rust_decimal::Decimal;
use rustrade_instrument::{
    exchange::ExchangeId,
    ibkr::{ContractRegistry, codes},
    instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use std::{
    ops::ControlFlow,
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

/// Which market data IB sends on a connection.
///
/// IB applies it to every subscription on the connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IbkrMarketDataType {
    /// Live data. It needs the account's live market data subscription for the instrument, and
    /// without one the subscription ends with [`DataError::MarketDataNotAvailable`].
    #[default]
    Live,
    /// While the market is closed, the last live data from the close. It needs the same
    /// subscription as [`Live`](Self::Live).
    Frozen,
    /// Delayed data, about 15 to 20 minutes old, which IB provides without a subscription for
    /// most instruments. Where the account has the live subscription, IB sends live data instead.
    Delayed,
    /// [`Delayed`](Self::Delayed), and while the market is closed the last delayed data from the
    /// close.
    DelayedFrozen,
}

impl From<IbkrMarketDataType> for MarketDataType {
    fn from(value: IbkrMarketDataType) -> Self {
        match value {
            IbkrMarketDataType::Live => MarketDataType::Realtime,
            IbkrMarketDataType::Frozen => MarketDataType::Frozen,
            IbkrMarketDataType::Delayed => MarketDataType::Delayed,
            IbkrMarketDataType::DelayedFrozen => MarketDataType::DelayedFrozen,
        }
    }
}

/// Configuration for the IBKR market data stream.
///
/// Build one with [`new`](Self::new) (or [`Default`]) and the `with_*` methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// Which market data IB sends, for every subscription on the stream. Default
    /// [`Live`](IbkrMarketDataType::Live).
    ///
    /// Events built from delayed data carry no mark that they are delayed, and their times are
    /// when they arrived, not when the venue quoted or traded.
    #[serde(default)]
    pub market_data_type: IbkrMarketDataType,
}

impl IbkrStreamConfig {
    /// A configuration connecting to `host`:`port` as `client_id`, for live market data.
    pub fn new(host: impl Into<String>, port: u16, client_id: i32) -> Self {
        Self {
            host: host.into(),
            port,
            client_id,
            market_data_type: IbkrMarketDataType::default(),
        }
    }

    /// Ask IB for `market_data_type` rather than live data.
    pub fn with_market_data_type(mut self, market_data_type: IbkrMarketDataType) -> Self {
        self.market_data_type = market_data_type;
        self
    }
}

impl Default for IbkrStreamConfig {
    fn default() -> Self {
        Self::new("127.0.0.1", 4002, 100)
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
    /// - Connection to IB fails, or IB refuses the configured market data type
    /// - No subscriptions could be activated (all contracts missing from registry)
    ///
    /// A subscription IB refuses after this returns ends with an error item on the stream instead:
    /// [`DataError::SubscriptionLimitReached`] when the account's market data lines or depth
    /// requests are all in use, [`DataError::MarketDataNotAvailable`] when the account has no
    /// market data for the instrument, and [`DataError::Socket`] otherwise.
    pub fn init(
        config: IbkrStreamConfig,
        contracts: Arc<ContractRegistry>,
        subscriptions: Vec<IbkrSubscription<K>>,
    ) -> Result<Self, DataError> {
        let url = format!("{}:{}", config.host, config.port);
        info!(%url, client_id = config.client_id, "Connecting to IB for market data");

        let client = Client::connect(&url, config.client_id)
            .map_err(|e| DataError::Socket(format!("IB connect: {e}")))?;

        // Live is what IB sends when nothing was asked for, so only another type is requested.
        if config.market_data_type != IbkrMarketDataType::Live {
            client
                .switch_market_data_type(config.market_data_type.into())
                .map_err(|e| DataError::Socket(format!("IB market data type: {e}")))?;
        }

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

            let worker = |stage| Worker {
                stage,
                instrument: sub.instrument.clone(),
                key: sub.key.clone(),
                tx: tx.clone(),
                closing: closing.clone(),
            };
            let spawn_result = match sub.kind {
                IbkrSubscriptionKind::Quotes => {
                    Self::run_quotes_subscription(client.clone(), contract, worker("quotes"))
                }
                IbkrSubscriptionKind::Depth { rows } => {
                    Self::run_depth_subscription(client.clone(), contract, rows, worker("depth"))
                }
                IbkrSubscriptionKind::Trades => {
                    Self::run_trades_subscription(client.clone(), contract, worker("trades"))
                }
                IbkrSubscriptionKind::OptionGreeks => Self::run_option_greeks_subscription(
                    client.clone(),
                    contract,
                    worker("option Greeks"),
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
        worker: Worker<K>,
    ) -> Result<(), DataError> {
        worker.spawn(move |worker| {
            let sub = match client.market_data(&contract).subscribe() {
                Ok(s) => s,
                Err(e) => return worker.fail(e),
            };

            let mut aggregator = QuoteAggregator::new(&contract.security_type);

            for item in sub.iter() {
                let tick = match item {
                    Ok(SubscriptionItem::Data(tick)) => tick,
                    Ok(SubscriptionItem::Notice(notice)) => match worker.notice(&notice) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(()) => break,
                    },
                    Err(e) => {
                        worker.fail(e);
                        break;
                    }
                };
                let now = Utc::now();
                if let Some(l1) = aggregator.update(&tick, now) {
                    let time_exchange = l1.last_update_time;
                    if worker
                        .send(time_exchange, now, DataKind::OrderBookL1(l1))
                        .is_break()
                    {
                        break;
                    }
                }
            }
        })
    }

    fn run_depth_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        rows: i32,
        worker: Worker<K>,
    ) -> Result<(), DataError> {
        worker.spawn(move |worker| {
            // Default SmartDepth::No: we aggregate into a simple anonymous book
            // without tracking market maker attribution from MarketDepthL2 events.
            let sub = match client.market_depth(&contract, rows).subscribe() {
                Ok(s) => s,
                Err(e) => return worker.fail(e),
            };

            let mut aggregator = DepthAggregator::new();

            // The venue reset (317) is not a notice: `ibapi` delivers it as `MarketDepths::Reset`
            // data.
            for item in sub.iter() {
                let depth = match item {
                    Ok(SubscriptionItem::Data(depth)) => {
                        if matches!(depth, MarketDepths::Reset) {
                            // `aggregator.update` drops the local book and returns
                            // the emptied one, which is forwarded below.
                            warn!(
                                instrument = %worker.instrument,
                                "Market depth RESET by venue; dropping local book"
                            );
                        }
                        depth
                    }
                    Ok(SubscriptionItem::Notice(notice)) => match worker.notice(&notice) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(()) => break,
                    },
                    Err(e) => {
                        worker.fail(e);
                        break;
                    }
                };
                // IB's MarketDepth events have no timestamp. Stamp `now`
                // BEFORE aggregating so the book's `time_received` (threaded
                // into the OrderBook) matches the MarketEvent envelope's.
                // Consumers should NOT interpret the MarketEvent's
                // time_exchange as a real venue timestamp for depth events.
                let now = Utc::now();
                if let Some(book_event) = aggregator.update(&depth, now)
                    && worker
                        .send(now, now, DataKind::OrderBook(book_event))
                        .is_break()
                {
                    break;
                }
            }
        })
    }

    fn run_trades_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        worker: Worker<K>,
    ) -> Result<(), DataError> {
        worker.spawn(move |worker| {
            let sub = match client.tick_by_tick(&contract, 0).all_last() {
                Ok(s) => s,
                Err(e) => return worker.fail(e),
            };

            for item in sub.iter() {
                let trade = match item {
                    Ok(SubscriptionItem::Data(trade)) => trade,
                    Ok(SubscriptionItem::Notice(notice)) => match worker.notice(&notice) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(()) => break,
                    },
                    Err(e) => {
                        worker.fail(e);
                        break;
                    }
                };
                let now = Utc::now();
                let public_trade = match trades::from_ib_trade(&trade) {
                    Some(t) => t,
                    None => continue, // Skip invalid trades (NaN/Inf price or size)
                };
                let time_exchange = trades::parse_trade_time(&trade, now);

                if worker
                    .send(time_exchange, now, DataKind::Trade(public_trade))
                    .is_break()
                {
                    break;
                }
            }
        })
    }

    fn run_option_greeks_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        worker: Worker<K>,
    ) -> Result<(), DataError> {
        if contract.security_type != SecurityType::Option {
            return Err(DataError::Socket(format!(
                "option Greeks subscription requires SecurityType::Option, got {:?} for {}",
                contract.security_type, worker.instrument
            )));
        }

        worker.spawn(move |worker| {
            let sub = match client.market_data(&contract).subscribe() {
                Ok(s) => s,
                Err(e) => return worker.fail(e),
            };

            let aggregator = GreeksAggregator::new();

            for item in sub.iter() {
                let tick = match item {
                    Ok(SubscriptionItem::Data(tick)) => tick,
                    Ok(SubscriptionItem::Notice(notice)) => match worker.notice(&notice) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(()) => break,
                    },
                    Err(e) => {
                        worker.fail(e);
                        break;
                    }
                };
                if let Some(greeks) = aggregator.update(&tick) {
                    let now = Utc::now();
                    if worker
                        .send(now, now, DataKind::OptionGreeks(greeks))
                        .is_break()
                    {
                        break;
                    }
                }
            }
        })
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

/// One subscription's worker: the line it serves, and how it reaches the stream.
struct Worker<K> {
    /// The kind of subscription, such as `"quotes"`, for log lines and error messages.
    stage: &'static str,
    /// The subscription's instrument, which names it in log lines and errors: the contract's
    /// symbol alone would not tell one option on an underlying from another.
    instrument: InstrumentNameExchange,
    /// The key the caller gave the subscription, put on each event.
    key: K,
    tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    /// The stream's close flag; see [`IbkrMarketStream::disconnect`].
    closing: Arc<AtomicBool>,
}

impl<K> Worker<K>
where
    K: Clone + Send + 'static,
{
    /// Run `body` on a new thread, which ends when `body` returns.
    ///
    /// A panic in `body` is sent to the stream as a terminal [`DataError::Socket`] naming the
    /// subscription; the stream's other subscriptions continue. Panic safety: parking_lot mutexes
    /// do not poison on panic, so shared state (ContractRegistry, etc.) remains usable.
    fn spawn(self, body: impl FnOnce(&Self) + Send + 'static) -> Result<(), DataError> {
        let stage = self.stage;
        std::thread::Builder::new()
            .name(format!("ibkr-{stage}-{}", self.instrument))
            .spawn(move || {
                debug!(instrument = %self.instrument, "Starting {stage} subscription");
                match catch_unwind(AssertUnwindSafe(|| body(&self))) {
                    Ok(()) => debug!(instrument = %self.instrument, "{stage} subscription ended"),
                    Err(panic_info) => {
                        let msg = panic_info
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| panic_info.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown panic".to_string());
                        error!(instrument = %self.instrument, "{stage} worker panicked: {msg}");
                        let _ = self.tx.send(Err(DataError::Socket(format!(
                            "{stage} subscription {} panicked: {msg}",
                            self.instrument
                        ))));
                    }
                }
            })
            .map(|_| ())
            .map_err(|e| DataError::Socket(format!("Failed to spawn {stage} thread: {e}")))
    }

    /// Send an event for this subscription; `Break` once the stream has been dropped.
    fn send(
        &self,
        time_exchange: DateTime<Utc>,
        time_received: DateTime<Utc>,
        kind: DataKind,
    ) -> ControlFlow<()> {
        let event = MarketEvent {
            time_exchange,
            time_received,
            exchange: ExchangeId::Ibkr,
            instrument: self.key.clone(),
            kind,
        };
        match self.tx.send(Ok(event)) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// End the worker on `error`: log it, and send it to the stream as the worker's terminal error
    /// ([`subscription_error`]).
    ///
    /// Once `closing` is set, the stream's own [`disconnect`](IbkrMarketStream::disconnect) has
    /// begun, and any error is logged at debug and nothing is sent: `ibapi` reports that close as
    /// more than one error, depending on how far the worker got, and the caller is ending the
    /// stream anyway.
    fn fail(&self, error: ibapi::Error) {
        let stage = self.stage;
        if self.closing.load(Ordering::Acquire) {
            debug!(instrument = %self.instrument, error = %error, "{stage} subscription ended by disconnect");
            return;
        }
        error!(instrument = %self.instrument, error = %error, "{stage} subscription failed");
        let _ = self
            .tx
            .send(Err(subscription_error(stage, &self.instrument, error)));
    }

    /// Act on a notice IB sent for this subscription without ending it: `Break` after sending a
    /// [`DataError::MarketDataNotAvailable`] when it says no data will come (IB sends 10089 this
    /// way); otherwise log it and `Continue`.
    fn notice(&self, notice: &Notice) -> ControlFlow<()> {
        if let Some(error) = notice_error(&self.instrument, notice) {
            let stage = self.stage;
            error!(instrument = %self.instrument, code = notice.code, message = %notice.message, "{stage} subscription refused");
            let _ = self.tx.send(Err(error));
            return ControlFlow::Break(());
        }
        warn!(
            instrument = %self.instrument,
            code = notice.code,
            message = %notice.message,
            "{} subscription notice",
            self.stage
        );
        ControlFlow::Continue(())
    }
}

/// The stream error for an `ibapi` error that ended the `stage` subscription to `instrument`: the
/// typed error for a notice that has one ([`notice_error`]), else [`DataError::Socket`],
/// `"{stage} subscription {instrument}: {error}"`.
fn subscription_error(
    stage: &str,
    instrument: &InstrumentNameExchange,
    error: ibapi::Error,
) -> DataError {
    if let ibapi::Error::Notice(notice) = &error
        && let Some(typed) = notice_error(instrument, notice)
    {
        return typed;
    }
    DataError::Socket(format!("{stage} subscription {instrument}: {error}"))
}

/// The typed error for a notice refusing the subscription to `instrument`, if it has one:
/// [`DataError::SubscriptionLimitReached`] for the line and depth caps,
/// [`DataError::MarketDataNotAvailable`] for no market data on the account.
fn notice_error(instrument: &InstrumentNameExchange, notice: &Notice) -> Option<DataError> {
    let (exchange, instrument, code, message) = (
        ExchangeId::Ibkr,
        instrument.clone(),
        notice.code,
        notice.message.clone(),
    );
    if codes::SUBSCRIPTION_LIMIT_CODES.contains(&notice.code) {
        Some(DataError::SubscriptionLimitReached {
            exchange,
            instrument,
            code,
            message,
        })
    } else if codes::MARKET_DATA_NOT_SUBSCRIBED_CODES.contains(&notice.code)
        || notice.code == codes::COMPETING_LIVE_SESSION
    {
        Some(DataError::MarketDataNotAvailable {
            exchange,
            instrument,
            code,
            message,
        })
    } else {
        None
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
// Test code: a panic is the failure report.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A quotes worker for `instrument`, and the stream's receiving end.
    fn worker(
        instrument: &str,
        closing: bool,
    ) -> (
        Worker<()>,
        mpsc::UnboundedReceiver<Result<MarketEvent<(), DataKind>, DataError>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker {
            stage: "quotes",
            instrument: InstrumentNameExchange::new(instrument),
            key: (),
            tx,
            closing: Arc::new(AtomicBool::new(closing)),
        };
        (worker, rx)
    }

    /// The error item a worker for AAPL sends when `error` ends it, if any.
    fn worker_error_sent(closing: bool, error: ibapi::Error) -> Option<DataError> {
        let (worker, mut rx) = worker("AAPL", closing);
        worker.fail(error);
        match rx.try_recv() {
            Ok(Err(error)) => Some(error),
            Ok(Ok(event)) => panic!("unexpected event: {event:?}"),
            Err(_) => None,
        }
    }

    fn notice(code: i32, message: &str) -> Notice {
        Notice {
            request_id: Some(7),
            code,
            message: message.to_owned(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }
    }

    #[test]
    fn a_shutdown_the_caller_did_not_ask_for_reaches_the_stream() {
        assert_eq!(
            worker_error_sent(false, ibapi::Error::Shutdown),
            Some(DataError::Socket(format!(
                "quotes subscription AAPL: {}",
                ibapi::Error::Shutdown
            )))
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
    fn the_line_and_depth_caps_end_a_subscription_as_a_subscription_limit() {
        for (code, message) in [
            (101, "Max number of tickers has been reached"),
            (
                309,
                "Max number (3) of market depth requests has been reached",
            ),
        ] {
            let error = ibapi::Error::Notice(notice(code, message));

            assert_eq!(
                worker_error_sent(false, error),
                Some(DataError::SubscriptionLimitReached {
                    exchange: ExchangeId::Ibkr,
                    instrument: InstrumentNameExchange::new("AAPL"),
                    code,
                    message: message.to_owned(),
                })
            );
        }
    }

    #[test]
    fn no_market_data_on_the_account_ends_a_subscription_as_not_available() {
        for code in [354, 10089, 10168, 10197] {
            let error = ibapi::Error::Notice(notice(code, "refused"));

            assert_eq!(
                worker_error_sent(false, error),
                Some(DataError::MarketDataNotAvailable {
                    exchange: ExchangeId::Ibkr,
                    instrument: InstrumentNameExchange::new("AAPL"),
                    code,
                    message: "refused".to_owned(),
                }),
                "code {code}"
            );
        }
    }

    #[test]
    fn another_refusal_ends_a_subscription_as_a_socket_error_naming_it() {
        let error = ibapi::Error::Notice(notice(200, "No security definition has been found"));
        let expected = format!("quotes subscription AAPL: {error}");

        assert_eq!(
            worker_error_sent(false, error),
            Some(DataError::Socket(expected))
        );
    }

    #[test]
    fn a_refusal_names_the_option_not_its_underlying() {
        let (worker, mut rx) = worker("SPY 261113C00500000", false);

        worker.fail(ibapi::Error::Notice(notice(101, "Max number of tickers")));

        let Ok(Err(DataError::SubscriptionLimitReached { instrument, .. })) = rx.try_recv() else {
            panic!("expected a subscription limit");
        };
        assert_eq!(
            instrument,
            InstrumentNameExchange::new("SPY 261113C00500000")
        );
    }

    #[test]
    fn a_notice_saying_no_data_will_come_ends_the_subscription() {
        let (worker, mut rx) = worker("AAPL", false);

        let flow = worker.notice(&notice(
            10089,
            "Requested market data requires additional subscription",
        ));

        assert_eq!(flow, ControlFlow::Break(()));
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(DataError::MarketDataNotAvailable { code: 10089, .. }))
        ));
    }

    #[test]
    fn a_notice_the_data_follows_is_logged_and_the_subscription_continues() {
        let (worker, mut rx) = worker("AAPL", false);

        // 10167: delayed data shown in place of live; the ticks follow it.
        let flow = worker.notice(&notice(10167, "Displaying delayed market data"));

        assert_eq!(flow, ControlFlow::Continue(()));
        assert!(
            rx.try_recv().is_err(),
            "a notice that ends nothing sends nothing"
        );
    }

    #[tokio::test]
    async fn a_spawned_worker_runs_its_body_on_its_own_thread() {
        let (worker, mut rx) = worker("AAPL", false);
        let caller = std::thread::current().id();

        worker
            .spawn(move |worker| {
                assert_ne!(std::thread::current().id(), caller);
                let now = Utc::now();
                let _ = worker.send(now, now, DataKind::OrderBookL1(Default::default()));
            })
            .unwrap();

        let event = rx.recv().await.expect("the body's event").unwrap();
        assert_eq!(event.exchange, ExchangeId::Ibkr);
        assert!(
            rx.recv().await.is_none(),
            "the stream ends when the worker does"
        );
    }

    #[tokio::test]
    async fn a_worker_that_panics_ends_its_subscription_with_an_error_naming_it() {
        let (worker, mut rx) = worker("SPY 261113C00500000", false);

        worker.spawn(|_| panic!("decoder bug")).unwrap();

        assert_eq!(
            rx.recv().await,
            Some(Err(DataError::Socket(
                "quotes subscription SPY 261113C00500000 panicked: decoder bug".to_owned()
            )))
        );
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn a_market_data_type_maps_to_ibs() {
        for (ours, ibs) in [
            (IbkrMarketDataType::Live, MarketDataType::Realtime),
            (IbkrMarketDataType::Frozen, MarketDataType::Frozen),
            (IbkrMarketDataType::Delayed, MarketDataType::Delayed),
            (
                IbkrMarketDataType::DelayedFrozen,
                MarketDataType::DelayedFrozen,
            ),
        ] {
            assert_eq!(MarketDataType::from(ours), ibs);
        }
    }

    #[test]
    fn a_configuration_asks_for_live_data_unless_told_otherwise() {
        let config = IbkrStreamConfig::new("127.0.0.1", 4002, 7);
        assert_eq!(config.market_data_type, IbkrMarketDataType::Live);
        assert_eq!(
            IbkrStreamConfig::default().market_data_type,
            IbkrMarketDataType::Live
        );

        let delayed = config.with_market_data_type(IbkrMarketDataType::Delayed);
        assert_eq!(delayed.market_data_type, IbkrMarketDataType::Delayed);
        assert_eq!(
            (delayed.host.as_str(), delayed.port, delayed.client_id),
            ("127.0.0.1", 4002, 7)
        );
    }

    #[test]
    fn a_configuration_saved_before_the_market_data_type_still_loads() {
        let config: IbkrStreamConfig =
            serde_json::from_str(r#"{"host":"127.0.0.1","port":4002,"client_id":7}"#).unwrap();
        assert_eq!(config.market_data_type, IbkrMarketDataType::Live);
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
