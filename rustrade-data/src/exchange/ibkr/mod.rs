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
//! trait designed for WebSocket exchanges. [`IbkrMarketStream`] still reconnects as the other
//! venues' market streams do: it yields the same [`reconnect::Event`]s, and takes the same
//! [`MarketStreamOptions`].
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
//! - **Trade side**: IB doesn't report which side was the aggressor, so a trade's `side` is `None`
//! - **Blocking API**: Uses `std::thread::spawn` for long-lived subscriptions
//! - **Reconnection**: a lost connection (the socket to TWS/Gateway, or TWS/Gateway's link to IB,
//!   codes 1100 and 1101) is reported as [`DataError::ConnectionLost`]; the stream then connects
//!   again and makes every subscription anew, paced by
//!   [`IbkrStreamConfig::max_messages_per_second`]. See [`IbkrMarketStream`]
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
//! [`IbkrStreamConfig::max_messages_per_second`]: crate::exchange::ibkr::IbkrStreamConfig::max_messages_per_second
//! [`DataError::ConnectionLost`]: crate::error::DataError::ConnectionLost
//! [`IbkrMarketStream`]: crate::exchange::ibkr::IbkrMarketStream
//! [`reconnect::Event`]: crate::streams::reconnect::Event
//! [`MarketStreamOptions`]: crate::streams::consumer::MarketStreamOptions

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
    streams::{
        consumer::{MarketStreamOptions, MarketStreamResult, StreamKey, market_stream_events},
        reconnect::{
            self,
            stream::{ReconnectionBackoffPolicy, init_reconnecting_stream},
        },
    },
};
use chrono::{DateTime, Utc};
use depth::DepthAggregator;
use futures::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use greeks::GreeksAggregator;
use ibapi::{
    Notice, RateLimiter, TRANSPORT_RECONNECT_CODE,
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
    num::NonZeroU32,
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
    /// The most messages the stream sends to TWS/Gateway in any one second, counting every
    /// subscription request, on the first connection and on each reconnect. Default
    /// [`DEFAULT_MAX_MESSAGES_PER_SECOND`].
    ///
    /// IB documents 50 messages per second, counted across every client connected to one
    /// TWS/Gateway together, and answers the excess with error 100, which can end the session.
    /// The default leaves room for another client, such as an execution client, on the same
    /// gateway. A request over the limit waits; none is dropped.
    #[serde(default = "default_max_messages_per_second")]
    pub max_messages_per_second: NonZeroU32,
}

/// The default [`IbkrStreamConfig::max_messages_per_second`]: 40.
pub const DEFAULT_MAX_MESSAGES_PER_SECOND: NonZeroU32 = match NonZeroU32::new(40) {
    Some(n) => n,
    None => unreachable!(),
};

fn default_max_messages_per_second() -> NonZeroU32 {
    DEFAULT_MAX_MESSAGES_PER_SECOND
}

impl IbkrStreamConfig {
    /// A configuration connecting to `host`:`port` as `client_id`, for live market data.
    pub fn new(host: impl Into<String>, port: u16, client_id: i32) -> Self {
        Self {
            host: host.into(),
            port,
            client_id,
            market_data_type: IbkrMarketDataType::default(),
            max_messages_per_second: DEFAULT_MAX_MESSAGES_PER_SECOND,
        }
    }

    /// Ask IB for `market_data_type` rather than live data.
    pub fn with_market_data_type(mut self, market_data_type: IbkrMarketDataType) -> Self {
        self.market_data_type = market_data_type;
        self
    }

    /// Send at most `max_messages_per_second` messages to TWS/Gateway in any one second; see
    /// [`max_messages_per_second`](Self::max_messages_per_second).
    pub fn with_max_messages_per_second(mut self, max_messages_per_second: NonZeroU32) -> Self {
        self.max_messages_per_second = max_messages_per_second;
        self
    }
}

impl Default for IbkrStreamConfig {
    fn default() -> Self {
        Self::new("127.0.0.1", 4002, 100)
    }
}

/// Market data stream from Interactive Brokers, which reconnects and resubscribes by itself.
///
/// Built with [`init`](Self::init). It yields [`MarketStreamResult`]s:
/// - `Event::Item(Ok(event))` for each market event.
/// - `Event::Item(Err(error))` for a subscription that ended while the connection stays up: see
///   [`init`](Self::init)'s refusals, which end only their own line.
/// - `Event::Item(Err(`[`DataError::ConnectionLost`]`))` when the connection is lost, then
///   `Event::Reconnecting(ExchangeId::Ibkr)`. The stream then connects again with the reconnect
///   backoff in its [`MarketStreamOptions`] and makes every subscription again. Each failed attempt
///   yields `Event::Item(Err(`[`DataError::ReinitFailed`]`))`.
///
/// It never ends by itself: drop it to stop it, which closes the connection. Nor does it end
/// when every subscription has ended, such as when IB refused them all: it stays open, silent,
/// until the connection is lost or the caller drops it.
///
/// # What counts as a lost connection
///
/// - The socket to TWS/Gateway closing, whether `ibapi` would reconnect it or not. Every
///   subscription on a socket ends with it, so the stream connects anew rather than waiting for
///   `ibapi`'s own reconnect.
/// - TWS/Gateway losing its link to IB's servers (code 1100), or regaining it with its market data
///   subscriptions lost (1101).
/// - TWS/Gateway regaining the link with its subscriptions kept (1102), when the connection made
///   its subscriptions while the link was down (it reconnected on the 1100): those are made again
///   once the link is back. A 1102 ends nothing otherwise.
/// - The `ibapi` client shutting down without this stream asking.
///
/// Events a lost connection had queued after it reported the loss are dropped with it.
///
/// # After a reconnect
///
/// - **Every subscription is made again**, including one refused before: a line past the cap may
///   fit now, and one IB has no data for is refused again, as a new error item.
/// - **Books and quotes start empty.** A quote or depth book is rebuilt from what IB sends on the
///   new subscription, so the first event after a reconnect does not carry a side over from before.
/// - **The client ID is reused.** The old connection's close finishes before the new one connects,
///   but IB Gateway may take a moment longer to free the ID, so an attempt may be refused (IB code
///   326) and yield `ReinitFailed` before a later one succeeds. The same holds for a new stream
///   made with the client ID of one just dropped, which does not wait for the old close at all.
/// - **A connection that ends soon after opening delays the next.** One that ends within 30
///   seconds of opening, such as one TWS/Gateway accepts and then ends at once, makes the next
///   attempt wait one more step of the reconnect backoff for each such end in a row, since a
///   connection that opens otherwise resets the backoff. Only the first attempt after it waits:
///   should that attempt fail, the ones after wait only the reconnect backoff.
/// - **The market data type** of the [`IbkrStreamConfig`] is asked for again on each connection.
///
/// # Pacing
///
/// Every request a connection sends passes its rate limiter
/// ([`IbkrStreamConfig::max_messages_per_second`]), so subscribing hundreds of lines, at first and
/// after each reconnect, does not exceed IB's message rate (error 100, which can end the session).
/// IB counts that rate across every client of one TWS/Gateway together; this limiter counts only
/// this stream's messages. Each connection has its own, so a lost connection's requests still
/// waiting for a slot do not delay the next connection's.
///
/// # Thread architecture
///
/// Each connection runs one thread per subscription and one watching the connection's global
/// notices. They are detached, and end when the connection closes. The close itself runs on a
/// thread of its own, so dropping the stream, or a connection being replaced, never blocks the
/// async task that does it. The stream's events pass
/// through an **unbounded** channel per connection: a consumer slower than IB grows the heap
/// rather than dropping events.
///
/// # Panic handling
///
/// A subscription thread that panics ends its subscription with a [`DataError::Socket`] naming it;
/// the stream's other subscriptions continue.
pub struct IbkrMarketStream<K> {
    inner: BoxStream<'static, MarketStreamResult<K, DataKind>>,
}

impl<K> std::fmt::Debug for IbkrMarketStream<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbkrMarketStream").finish_non_exhaustive()
    }
}

impl<K> futures::Stream for IbkrMarketStream<K> {
    type Item = MarketStreamResult<K, DataKind>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<K> IbkrMarketStream<K>
where
    K: Clone + Send + 'static,
{
    /// Connect to TWS/Gateway and make `subscriptions`, returning the stream once the first
    /// connection is up.
    ///
    /// # Arguments
    ///
    /// * `config` - Connection configuration
    /// * `contracts` - Contract registry for instrument resolution. Subscriptions look their
    ///   contracts up by name only, and IB resolves each contract description on request, so
    ///   contracts registered with
    ///   [`register_by_name_only`](rustrade_instrument::ibkr::ContractRegistry::register_by_name_only)
    ///   need not be resolved first. It is read again on each reconnect.
    /// * `subscriptions` - List of subscriptions to activate
    /// * `options` - How the stream reconnects, and whether it is ended when it stays silent; see
    ///   [`MarketStreamOptions`]. A silence timeout counts the market events of all subscriptions
    ///   together, so set it longer than the quietest market the stream carries is ever silent.
    ///
    /// # Errors
    ///
    /// The first connection's error:
    /// - [`DataError::SubscriptionsEmpty`] if `subscriptions` is empty.
    /// - [`DataError::Socket`] if the connection to IB fails, the configured market data type
    ///   cannot be sent, or no subscription could be activated (every contract missing from the
    ///   registry).
    ///
    /// A failed reconnect is not returned: the stream yields it, as above, and tries again.
    ///
    /// IB does not answer the market data type itself; what the account may receive shows per
    /// subscription. A subscription IB refuses ends with an error item on the stream instead:
    /// [`DataError::SubscriptionLimitReached`] when the account's market data lines or depth
    /// requests are all in use, [`DataError::MarketDataNotAvailable`] when the account has no
    /// market data for the instrument, and [`DataError::Socket`] otherwise.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime: the blocking connection is made on tokio's blocking thread pool.
    pub async fn init(
        config: IbkrStreamConfig,
        contracts: Arc<ContractRegistry>,
        subscriptions: Vec<IbkrSubscription<K>>,
        options: MarketStreamOptions,
    ) -> Result<Self, DataError> {
        if subscriptions.is_empty() {
            return Err(DataError::SubscriptionsEmpty);
        }
        let shared = Shared::default();
        let backoff = options.reconnect.clone();
        info!(
            ?options,
            subscriptions = subscriptions.len(),
            "IbkrMarketStream with auto reconnect initialising"
        );

        let attempts = init_reconnecting_stream(move || {
            let config = config.clone();
            let contracts = contracts.clone();
            let subscriptions = subscriptions.clone();
            let shared = shared.clone();
            let delay = shared.quick_ends.lock().take_delay(&backoff);
            async move {
                wait_before_open(delay).await;
                tokio::task::spawn_blocking(move || {
                    Connection::open(&config, &shared, &contracts, subscriptions)
                })
                .await
                .map_err(|e| DataError::Socket(format!("IB connect task: {e}")))?
                .map(StreamExt::boxed)
            }
        })
        .await?
        .boxed();

        Ok(Self {
            inner: reconnecting_events(attempts, options),
        })
    }
}

/// Wait `delay` ([`QuickEnds::take_delay`]) before an attempt to open a connection.
async fn wait_before_open(delay: std::time::Duration) {
    if delay.is_zero() {
        return;
    }
    debug!(
        ?delay,
        "IB connection ended soon after opening; waiting to reconnect"
    );
    tokio::time::sleep(delay).await;
}

/// The events a consumer receives from a stream's connection attempts: each connection's items up
/// to and including a [`DataError::ConnectionLost`], then the shared reconnect handling
/// ([`market_stream_events`]).
fn reconnecting_events<T>(
    attempts: BoxStream<'static, Result<BoxStream<'static, Result<T, DataError>>, DataError>>,
    options: MarketStreamOptions,
) -> BoxStream<'static, reconnect::Event<ExchangeId, Result<T, DataError>>>
where
    T: Send + 'static,
{
    let exchange = ExchangeId::Ibkr;
    let attempts = attempts
        .map(|attempt| attempt.map(|connection| end_after_connection_lost(connection).boxed()))
        .boxed();
    market_stream_events(
        attempts,
        options,
        StreamKey::new_general("market_stream", exchange),
        exchange,
    )
    .boxed()
}

/// `connection`'s items, ending after the first [`DataError::ConnectionLost`] without polling it
/// again, and dropping it then, which closes its connection.
///
/// Every subscription of a lost connection may report the loss, and a link loss leaves the others
/// silent rather than ended, so the first report is the end.
fn end_after_connection_lost<St, T>(connection: St) -> impl Stream<Item = Result<T, DataError>>
where
    St: Stream<Item = Result<T, DataError>> + Unpin,
{
    stream::unfold(Some(connection), |connection| async move {
        let mut connection = connection?;
        let item = connection.next().await?;
        let lost = matches!(item, Err(DataError::ConnectionLost { .. }));
        Some((item, (!lost).then_some(connection)))
    })
}

/// One connection to TWS/Gateway with its subscriptions: the inner piece of [`IbkrMarketStream`],
/// which opens a new one after each loss.
///
/// Its items are each subscription's events and errors, and a [`DataError::ConnectionLost`] when
/// the connection is lost. It ends once every worker and the notice watcher have exited, and the
/// watcher exits only after reporting a loss or when the client shuts down: a connection whose
/// subscriptions have all ended stays open, silent.
struct Connection<K> {
    rx: mpsc::UnboundedReceiver<Result<MarketEvent<K, DataKind>, DataError>>,
    client: Arc<Client>,
    /// Set by [`close`](Self::close) before it closes the connection, so that a worker or the
    /// watcher can tell this stream's own close from a shutdown `ibapi` or TWS/Gateway caused:
    /// `ibapi` reports both alike, as `Error::Shutdown` to a running subscription and as
    /// `Error::ConnectionReset` to one still being sent. Once it is set, nothing more is reported.
    closing: Arc<AtomicBool>,
    /// What the stream's connections share: where [`close`](Self::close) leaves the close
    /// running, and how long connections last.
    shared: Shared,
    /// When `open` handed the connection out; `None` while it is still being opened, so that a
    /// failed open is not counted as a connection that ended.
    opened_at: Option<std::time::Instant>,
}

/// What a stream's connections share, one after another.
#[derive(Debug, Clone, Default)]
struct Shared {
    /// Whether TWS/Gateway's link to IB's servers is down, as the last link notice any of the
    /// stream's connections read said: see [`link_notice`].
    link_down: Arc<AtomicBool>,
    pending_close: PendingClose,
    quick_ends: Arc<parking_lot::Mutex<QuickEnds>>,
}

/// A connection that ends sooner than this after it opened counts as a quick end; see
/// [`QuickEnds`].
const QUICK_END: std::time::Duration = std::time::Duration::from_secs(30);

/// How many of a stream's connections in a row ended within [`QUICK_END`] of opening, and whether
/// the attempt after the last one has yet to wait for it.
///
/// The shared reconnect handling resets its backoff whenever a connection opens, so a connection
/// that TWS/Gateway accepts and then ends at once (an 1100 sent to each new connection while the
/// link is down, say) would otherwise be remade with no wait, again and again. Each quick end in a
/// row delays the next attempt by one more step of the reconnect backoff. Only that attempt waits:
/// should it fail, the shared reconnect handling's own backoff spaces out the ones after, and a
/// second wait would stack on it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct QuickEnds {
    in_a_row: u32,
    /// Whether the next attempt has yet to wait for the last quick end.
    owed: bool,
}

impl QuickEnds {
    /// Count a connection that ended after `lived`.
    fn record(&mut self, lived: std::time::Duration) {
        if lived < QUICK_END {
            self.in_a_row = self.in_a_row.saturating_add(1);
            self.owed = true;
        } else {
            *self = Self::default();
        }
    }

    /// The wait before the next attempt, which it takes: the backoff `policy` gives the attempt
    /// after as many failures as there were quick ends in a row, once after each quick end, else
    /// none.
    fn take_delay(&mut self, policy: &ReconnectionBackoffPolicy) -> std::time::Duration {
        if !std::mem::take(&mut self.owed) {
            return std::time::Duration::ZERO;
        }
        let steps = self.in_a_row.saturating_sub(1);
        let multiplier = u64::from(policy.backoff_multiplier);
        let mut millis = policy.backoff_ms_initial;
        for _ in 0..steps {
            if millis >= policy.backoff_ms_max {
                break;
            }
            millis = millis.saturating_mul(multiplier);
        }
        std::time::Duration::from_millis(millis.min(policy.backoff_ms_max))
    }
}

/// The close of a stream's last connection, which runs on a thread of its own, since
/// `ibapi`'s disconnect blocks until the client's threads have exited: dropping a connection must
/// not block the async task that drops it. The next connection waits for it, so that it does not
/// connect with the same client ID while the old connection is still open.
#[derive(Debug, Clone, Default)]
struct PendingClose(Arc<parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>>);

impl PendingClose {
    /// Run `close` on a new thread, after any close started before it. If no thread can be
    /// started, the earlier closes and `close` run here instead, blocking the caller.
    fn start(&self, close: impl FnOnce() + Send + 'static) {
        let mut pending = self.0.lock();
        let earlier = pending.take();
        let job = move || {
            if let Some(earlier) = earlier {
                let _ = earlier.join();
            }
            close();
        };
        // Shared with the thread, so that the job is still here to run if the spawn fails.
        let job = Arc::new(parking_lot::Mutex::new(Some(job)));
        let on_thread = job.clone();
        let spawned = std::thread::Builder::new()
            .name("ibkr-close".to_owned())
            .spawn(move || {
                if let Some(job) = on_thread.lock().take() {
                    job();
                }
            });
        match spawned {
            Ok(handle) => *pending = Some(handle),
            Err(error) => {
                drop(pending);
                warn!(%error, "Failed to spawn the IB close thread; closing on this one");
                if let Some(job) = job.lock().take() {
                    job();
                }
            }
        }
    }

    /// Block until the close started last, and every one before it, has finished.
    fn wait(&self) {
        let pending = self.0.lock().take();
        if let Some(pending) = pending {
            let _ = pending.join();
        }
    }
}

impl<K> futures::Stream for Connection<K> {
    type Item = Result<MarketEvent<K, DataKind>, DataError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_recv(cx)
    }
}

impl<K> Connection<K>
where
    K: Clone + Send + 'static,
{
    /// Connect as `config` says and start a worker per subscription, plus the notice watcher,
    /// once the stream's previous connection has closed.
    ///
    /// Blocks for the previous close and the handshake.
    fn open(
        config: &IbkrStreamConfig,
        shared: &Shared,
        contracts: &ContractRegistry,
        subscriptions: Vec<IbkrSubscription<K>>,
    ) -> Result<Self, DataError> {
        shared.pending_close.wait();

        let url = format!("{}:{}", config.host, config.port);
        info!(%url, client_id = config.client_id, "Connecting to IB for market data");

        let (client, notices) = Client::builder()
            .address(&url)
            .client_id(config.client_id)
            .rate_limiter(RateLimiter::per_second(
                config.max_messages_per_second.get(),
            ))
            .connect_with_notice_stream()
            .map_err(|e| DataError::Socket(format!("IB connect: {e}")))?;
        let (tx, rx) = mpsc::unbounded_channel();
        // Built before anything else can fail, so that its `Drop` closes the client on every
        // early return below.
        let connection = Self {
            rx,
            client: Arc::new(client),
            closing: Arc::new(AtomicBool::new(false)),
            shared: shared.clone(),
            opened_at: None,
        };

        // Live is what IB sends when nothing was asked for, so only another type is requested.
        if config.market_data_type != IbkrMarketDataType::Live {
            connection
                .client
                .switch_market_data_type(config.market_data_type.into())
                .map_err(|e| DataError::Socket(format!("IB market data type: {e}")))?;
        }

        // Read before any subscription is made: whether they are made while the link is down.
        let watcher = NoticeWatcher {
            tx: tx.clone(),
            closing: connection.closing.clone(),
            link_down: shared.link_down.clone(),
            down_at_open: shared.link_down.load(Ordering::Acquire),
        };
        std::thread::Builder::new()
            .name("ibkr-notices".to_owned())
            .spawn(move || watcher.watch(notices.iter()))
            .map_err(|e| DataError::Socket(format!("Failed to spawn IB notice thread: {e}")))?;

        let mut active_subscriptions = 0;

        // Spawn a worker thread for each subscription
        for IbkrSubscription {
            instrument,
            key,
            kind,
        } in subscriptions
        {
            let contract = match contracts.get_contract(&instrument) {
                Some(c) => c,
                None => {
                    warn!(
                        %instrument,
                        "Contract not found in registry, skipping subscription"
                    );
                    continue;
                }
            };

            let worker = |stage| Worker {
                stage,
                instrument: instrument.clone(),
                key,
                tx: tx.clone(),
                closing: connection.closing.clone(),
            };
            let client = connection.client.clone();
            let spawn_result = match kind {
                IbkrSubscriptionKind::Quotes => {
                    Self::run_quotes_subscription(client, contract, worker("quotes"))
                }
                IbkrSubscriptionKind::Depth { rows } => {
                    Self::run_depth_subscription(client, contract, rows, worker("depth"))
                }
                IbkrSubscriptionKind::Trades => {
                    Self::run_trades_subscription(client, contract, worker("trades"))
                }
                IbkrSubscriptionKind::OptionGreeks => {
                    Self::run_option_greeks_subscription(client, contract, worker("option Greeks"))
                }
            };

            match spawn_result {
                Ok(()) => active_subscriptions += 1,
                Err(e) => {
                    warn!(%instrument, error = %e, "Failed to spawn subscription worker");
                }
            }
        }

        if active_subscriptions == 0 {
            return Err(DataError::Socket(
                "No subscriptions activated (check logs for per-subscription errors)".to_string(),
            ));
        }

        let mut connection = connection;
        connection.opened_at = Some(std::time::Instant::now());
        Ok(connection)
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

impl<K> Connection<K> {
    /// Close the connection, which ends every worker's subscription and the notice watcher, with
    /// nothing reported for it. Returns at once: the close runs on a thread of its own
    /// ([`PendingClose`]), which the stream's next connection waits for.
    fn close(&self) {
        debug!("Closing IB market data connection");
        // Set here, not on the close thread, so that no worker reports the close as a failure.
        self.closing.store(true, Ordering::Release);
        let client = self.client.clone();
        self.shared.pending_close.start(move || client.disconnect());
    }
}

impl<K> Drop for Connection<K> {
    fn drop(&mut self) {
        if let Some(opened_at) = self.opened_at {
            self.shared.quick_ends.lock().record(opened_at.elapsed());
        }
        self.close();
    }
}

/// Watches one connection's global notices (those with no request ID), and reports the
/// connection lost when a notice says so, or when the notices end because the client shut down.
struct NoticeWatcher<K> {
    tx: mpsc::UnboundedSender<Result<MarketEvent<K, DataKind>, DataError>>,
    /// The connection's close flag; see [`Connection::close`].
    closing: Arc<AtomicBool>,
    /// The stream's view of TWS/Gateway's link to IB; see [`link_notice`].
    link_down: Arc<AtomicBool>,
    /// Whether `link_down` was set when the connection made its subscriptions.
    down_at_open: bool,
}

impl<K> NoticeWatcher<K> {
    /// Read `notices` until one ends the connection ([`link_notice`]) or they end, and report the
    /// loss either way, unless the connection's own close ended them.
    fn watch(self, notices: impl IntoIterator<Item = Notice>) {
        for notice in notices {
            // A closing connection's notices change nothing: the next connection may be open.
            if self.closing.load(Ordering::Acquire) {
                return;
            }
            info!(code = notice.code, message = %notice.message, "IB notice");
            if let Some(reason) = link_notice(&notice, &self.link_down, self.down_at_open) {
                self.report_lost(reason);
                return;
            }
        }
        self.report_lost("the IB client shut down".to_owned());
    }

    fn report_lost(&self, reason: String) {
        if self.closing.load(Ordering::Acquire) {
            debug!(%reason, "IB notices ended by the connection's own close");
            return;
        }
        warn!(%reason, "IB market data connection lost");
        let _ = self.tx.send(Err(DataError::ConnectionLost {
            exchange: ExchangeId::Ibkr,
            reason,
        }));
    }
}

/// Why global `notice` ends the connection, if it does, updating `link_down`, the stream's view
/// of whether TWS/Gateway's link to IB's servers is down. `down_at_open` says whether it was down
/// when this connection made its subscriptions.
///
/// - 1100 (link lost) ends it. Should TWS/Gateway say so again to each connection made during the
///   outage, [`QuickEnds`] spaces the reconnects out.
/// - 1101 (restored, subscriptions lost) ends it.
/// - 1102 (restored, subscriptions kept) ends it only if its subscriptions were made while the
///   link was down, so that they are made again with the link up.
/// - `ibapi`'s own socket reconnect ends it: every subscription on the socket ended.
fn link_notice(notice: &Notice, link_down: &AtomicBool, down_at_open: bool) -> Option<String> {
    let what = match notice.code {
        codes::LINK_LOST => {
            link_down.store(true, Ordering::Release);
            "TWS/Gateway lost its link to IB's servers"
        }
        codes::LINK_RESTORED_DATA_LOST => {
            link_down.store(false, Ordering::Release);
            "TWS/Gateway regained its link to IB's servers but lost its market data subscriptions"
        }
        codes::LINK_RESTORED_DATA_KEPT => {
            link_down.store(false, Ordering::Release);
            if !down_at_open {
                return None;
            }
            "TWS/Gateway regained its link to IB's servers, and this connection's subscriptions \
             were made while it was down"
        }
        TRANSPORT_RECONNECT_CODE => {
            "ibapi reconnected the socket to TWS/Gateway, which ended every subscription"
        }
        _ => return None,
    };
    Some(format!("{what} ({} {})", notice.code, notice.message))
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
    /// The connection's close flag; see [`Connection::close`].
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

    /// End the worker on `error`: log it, and send it to the stream as the worker's terminal error,
    /// [`DataError::ConnectionLost`] when the error says the connection is gone
    /// ([`ends_connection`]), else the subscription's own ([`subscription_error`]).
    ///
    /// Once `closing` is set, the connection's own [`close`](Connection::close) has begun, and any
    /// error is logged at debug and nothing is sent: `ibapi` reports that close as more than one
    /// error, depending on how far the worker got, and the stream is ending the connection anyway.
    fn fail(&self, error: ibapi::Error) {
        let stage = self.stage;
        if self.closing.load(Ordering::Acquire) {
            debug!(instrument = %self.instrument, error = %error, "{stage} subscription ended by the connection's close");
            return;
        }
        let item = if ends_connection(&error) {
            warn!(instrument = %self.instrument, error = %error, "{stage} subscription ended by a lost connection");
            DataError::ConnectionLost {
                exchange: ExchangeId::Ibkr,
                reason: format!("{stage} subscription {}: {error}", self.instrument),
            }
        } else {
            error!(instrument = %self.instrument, error = %error, "{stage} subscription failed");
            subscription_error(stage, &self.instrument, error)
        };
        let _ = self.tx.send(Err(item));
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

/// Whether `error` says the connection itself is gone, rather than one subscription refused: the
/// socket closed (whether `ibapi` would reconnect it or not), or the client shut down.
fn ends_connection(error: &ibapi::Error) -> bool {
    error.is_connection_lost()
        || matches!(
            error,
            ibapi::Error::Shutdown | ibapi::Error::ConnectionFailed
        )
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
    fn a_shutdown_the_stream_did_not_ask_for_reports_the_connection_lost() {
        assert_eq!(
            worker_error_sent(false, ibapi::Error::Shutdown),
            Some(DataError::ConnectionLost {
                exchange: ExchangeId::Ibkr,
                reason: format!("quotes subscription AAPL: {}", ibapi::Error::Shutdown),
            })
        );
    }

    #[test]
    fn a_closed_socket_reports_the_connection_lost() {
        for error in [
            ibapi::Error::ConnectionReset,
            ibapi::Error::ConnectionFailed,
            ibapi::Error::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        ] {
            let shown = error.to_string();
            assert!(
                matches!(
                    worker_error_sent(false, error),
                    Some(DataError::ConnectionLost { .. })
                ),
                "{shown}"
            );
        }
    }

    #[test]
    fn the_connections_own_close_ends_a_running_worker_without_an_error_item() {
        assert_eq!(worker_error_sent(true, ibapi::Error::Shutdown), None);
    }

    #[test]
    fn the_connections_own_close_ends_a_subscribing_worker_without_an_error_item() {
        // `ibapi` refuses a subscription sent after the close began with `ConnectionReset`.
        assert_eq!(worker_error_sent(true, ibapi::Error::ConnectionReset), None);
    }

    fn global_notice(code: i32, message: &str) -> Notice {
        Notice {
            request_id: None,
            ..notice(code, message)
        }
    }

    /// What a watcher sends when it reads `notices`, with the connection closing or not, and the
    /// stream's link state, which it updates.
    fn watched_with(
        closing: bool,
        link_down: &Arc<AtomicBool>,
        notices: Vec<Notice>,
    ) -> Vec<DataError> {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<Result<MarketEvent<(), DataKind>, DataError>>();
        NoticeWatcher {
            tx,
            closing: Arc::new(AtomicBool::new(closing)),
            link_down: link_down.clone(),
            down_at_open: link_down.load(Ordering::Acquire),
        }
        .watch(notices);
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|item| item.expect_err("a watcher sends only errors"))
            .collect()
    }

    fn watched(closing: bool, notices: Vec<Notice>) -> Vec<DataError> {
        watched_with(closing, &Arc::new(AtomicBool::new(false)), notices)
    }

    /// Why `code` ends a connection, given the link state before it and at the connection's
    /// open, and the link state after it.
    fn link_after(code: i32, down_before: bool, down_at_open: bool) -> (Option<String>, bool) {
        let link_down = AtomicBool::new(down_before);
        let ends = link_notice(&global_notice(code, "IB says"), &link_down, down_at_open);
        (ends, link_down.load(Ordering::Acquire))
    }

    #[test]
    fn a_link_loss_ends_the_connection_and_marks_the_link_down() {
        for (down_before, down_at_open) in [(false, false), (true, true)] {
            let (ends, down) = link_after(1100, down_before, down_at_open);
            let reason = ends.expect("an 1100 ends the connection");
            assert!(
                reason.contains("lost its link") && reason.contains("(1100 IB says)"),
                "{reason}"
            );
            assert!(down);
        }
    }

    #[test]
    fn a_restore_with_subscriptions_lost_ends_the_connection_and_marks_the_link_up() {
        for down_at_open in [false, true] {
            let (ends, down) = link_after(1101, true, down_at_open);
            assert!(
                ends.expect("1101 ends")
                    .contains("lost its market data subscriptions")
            );
            assert!(!down);
        }
    }

    #[test]
    fn a_restore_with_subscriptions_kept_ends_only_a_connection_made_during_the_outage() {
        assert_eq!(link_after(1102, true, false), (None, false));
        assert_eq!(link_after(1102, false, false), (None, false));

        let (ends, down) = link_after(1102, true, true);
        assert!(ends.expect("ends").contains("made while it was down"));
        assert!(!down);
    }

    #[test]
    fn ibapis_own_socket_reconnect_ends_the_connection() {
        let (ends, down) = link_after(TRANSPORT_RECONNECT_CODE, false, false);
        assert!(ends.expect("ends").contains("ended every subscription"));
        assert!(!down);
    }

    #[test]
    fn a_farm_notice_ends_nothing_and_leaves_the_link_state() {
        for code in [2104, 2106, 2158, 2103] {
            assert_eq!(link_after(code, false, false), (None, false), "{code}");
            assert_eq!(link_after(code, true, true), (None, true), "{code}");
        }
    }

    #[test]
    fn a_watcher_reports_the_first_link_loss_once_and_stops_reading() {
        let sent = watched(
            false,
            vec![
                global_notice(2104, "Market data farm connection is OK"),
                global_notice(1101, "Connectivity restored - data lost"),
                global_notice(1100, "Connectivity lost"),
            ],
        );

        let [DataError::ConnectionLost { exchange, reason }] = sent.as_slice() else {
            panic!("expected one connection loss, got {sent:?}");
        };
        assert_eq!(*exchange, ExchangeId::Ibkr);
        assert!(reason.contains("1101"), "{reason}");
    }

    #[test]
    fn a_connection_made_during_a_link_outage_is_remade_when_the_link_is_back() {
        let link_down = Arc::new(AtomicBool::new(false));
        let lost = |sent: &[DataError], says: &str| matches!(sent, [DataError::ConnectionLost { reason, .. }] if reason.contains(says));

        // The first connection: the link goes down.
        let first = watched_with(false, &link_down, vec![global_notice(1100, "lost")]);
        assert!(lost(&first, "1100"), "{first:?}");

        // The next, made during the outage: the restore ends it, so that its subscriptions are
        // made again with the link up.
        let second = watched_with(false, &link_down, vec![global_notice(1102, "maintained")]);
        assert!(lost(&second, "1102"), "{second:?}");

        // The one after: a 1102 ends nothing, so only the notices ending report a loss.
        let third = watched_with(false, &link_down, vec![global_notice(1102, "maintained")]);
        assert!(lost(&third, "shut down"), "{third:?}");
    }

    #[test]
    fn a_closing_connections_notices_leave_the_link_state_alone() {
        let link_down = Arc::new(AtomicBool::new(true));

        let sent = watched_with(true, &link_down, vec![global_notice(1102, "maintained")]);

        assert_eq!(sent, []);
        assert!(link_down.load(Ordering::Acquire));
    }

    const BACKOFF: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
        backoff_ms_initial: 100,
        backoff_multiplier: 2,
        backoff_ms_max: 1_000,
    };

    #[test]
    fn a_connection_that_lasted_delays_nothing() {
        let mut ends = QuickEnds::default();
        assert_eq!(ends.take_delay(&BACKOFF), std::time::Duration::ZERO);

        ends.record(QUICK_END);
        assert_eq!(ends.take_delay(&BACKOFF), std::time::Duration::ZERO);
    }

    #[test]
    fn quick_ends_in_a_row_step_through_the_backoff_up_to_its_cap() {
        let mut ends = QuickEnds::default();
        let mut delays = Vec::new();
        for _ in 0..6 {
            ends.record(QUICK_END - std::time::Duration::from_millis(1));
            delays.push(ends.take_delay(&BACKOFF).as_millis());
        }

        assert_eq!(delays, [100, 200, 400, 800, 1_000, 1_000]);
    }

    #[test]
    fn a_connection_that_lasted_resets_the_quick_ends() {
        let mut ends = QuickEnds::default();
        ends.record(std::time::Duration::ZERO);
        ends.record(std::time::Duration::ZERO);

        ends.record(QUICK_END);

        assert_eq!(ends, QuickEnds::default());
    }

    #[test]
    fn only_the_first_attempt_after_a_quick_end_waits() {
        let mut ends = QuickEnds::default();
        ends.record(std::time::Duration::ZERO);
        ends.record(std::time::Duration::ZERO);

        // The first attempt after waits; the ones after it, should it fail, do not.
        assert_eq!(ends.take_delay(&BACKOFF).as_millis(), 200);
        assert_eq!(ends.take_delay(&BACKOFF), std::time::Duration::ZERO);

        // The count stays until a connection lasts, so the next quick end waits one step more.
        ends.record(std::time::Duration::ZERO);
        assert_eq!(ends.take_delay(&BACKOFF).as_millis(), 400);
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_before_an_open_waits_the_delay() {
        let start = tokio::time::Instant::now();
        wait_before_open(std::time::Duration::from_millis(500)).await;
        assert_eq!(start.elapsed(), std::time::Duration::from_millis(500));

        let start = tokio::time::Instant::now();
        wait_before_open(std::time::Duration::ZERO).await;
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
    }

    #[test]
    fn notices_ending_without_a_loss_mean_the_client_shut_down() {
        let sent = watched(false, vec![global_notice(2104, "farm OK")]);

        assert_eq!(
            sent,
            [DataError::ConnectionLost {
                exchange: ExchangeId::Ibkr,
                reason: "the IB client shut down".to_owned(),
            }]
        );
    }

    #[test]
    fn a_watcher_reports_nothing_once_the_connection_is_closing() {
        assert_eq!(watched(true, vec![global_notice(1100, "lost")]), []);
        assert_eq!(watched(true, vec![]), []);
    }

    #[test]
    fn a_close_runs_off_the_callers_thread_and_the_next_connection_waits_for_it() {
        let pending = PendingClose::default();
        let (started, release) = std::sync::mpsc::channel::<()>();
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let done = Arc::new(AtomicBool::new(false));
        let off_caller = Arc::new(AtomicBool::new(false));
        let caller = std::thread::current().id();

        let (on_close, on_other_thread) = (done.clone(), off_caller.clone());
        pending.start(move || {
            on_other_thread.store(std::thread::current().id() != caller, Ordering::Release);
            let _ = started.send(());
            // Held until the test lets it go, so `start` is seen to return before it ends.
            let _ = held.recv();
            on_close.store(true, Ordering::Release);
        });
        release.recv().expect("the close started");
        assert!(
            !done.load(Ordering::Acquire),
            "start returned before the close finished"
        );
        hold.send(()).unwrap();

        pending.wait();
        assert!(
            done.load(Ordering::Acquire),
            "wait returns once the close has finished"
        );
        assert!(
            off_caller.load(Ordering::Acquire),
            "the close ran off the caller's thread"
        );
        pending.wait();
    }

    #[test]
    fn closes_run_in_the_order_they_were_started() {
        let pending = PendingClose::default();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        for (index, pause) in [(1, 30), (2, 0)] {
            let order = order.clone();
            pending.start(move || {
                std::thread::sleep(std::time::Duration::from_millis(pause));
                order.lock().push(index);
            });
        }

        pending.wait();
        assert_eq!(*order.lock(), [1, 2]);
    }

    type Item = Result<u8, DataError>;

    fn lost() -> Item {
        Err(DataError::ConnectionLost {
            exchange: ExchangeId::Ibkr,
            reason: "socket closed".to_owned(),
        })
    }

    fn limit() -> Item {
        Err(DataError::SubscriptionLimitReached {
            exchange: ExchangeId::Ibkr,
            instrument: InstrumentNameExchange::new("AAPL"),
            code: 101,
            message: "Max number of tickers".to_owned(),
        })
    }

    /// A connection yielding `items`, then nothing, without ending: as a lost connection whose
    /// other workers stay silent.
    fn connection(items: Vec<Item>) -> BoxStream<'static, Item> {
        stream::iter(items).chain(stream::pending()).boxed()
    }

    /// The first `count` items of `stream`, failing the test if they have not all arrived within
    /// a day of tokio's (paused) clock.
    async fn first<T>(stream: impl Stream<Item = T>, count: usize) -> Vec<T> {
        tokio::time::timeout(
            std::time::Duration::from_secs(86_400),
            stream.take(count).collect::<Vec<_>>(),
        )
        .await
        .expect("the stream stalled")
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_ends_after_its_first_loss_without_waiting_for_more() {
        let items = end_after_connection_lost(connection(vec![Ok(1), limit(), lost(), Ok(2)]));

        let collected = tokio::time::timeout(
            std::time::Duration::from_secs(86_400),
            items.collect::<Vec<_>>(),
        )
        .await
        .expect("a lost connection ends");

        assert_eq!(collected, [Ok(1), limit(), lost()]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_connection_is_reported_then_reopened_and_a_line_refusal_is_not() {
        const POLICY: crate::streams::reconnect::stream::ReconnectionBackoffPolicy =
            crate::streams::reconnect::stream::ReconnectionBackoffPolicy {
                backoff_ms_initial: 1,
                backoff_multiplier: 2,
                backoff_ms_max: 10,
            };
        let refused = DataError::Socket("IB connect: connection refused".to_owned());
        let attempts = stream::iter([
            Ok(connection(vec![Ok(1), limit(), lost(), Ok(99)])),
            Err(refused.clone()),
            Ok(connection(vec![Ok(2)])),
        ])
        .chain(stream::pending())
        .boxed();

        let events = first(
            reconnecting_events(
                attempts,
                MarketStreamOptions::default().with_reconnect(POLICY),
            ),
            6,
        )
        .await;

        assert_eq!(
            events,
            [
                reconnect::Event::Item(Ok(1)),
                reconnect::Event::Item(limit()),
                reconnect::Event::Item(lost()),
                reconnect::Event::Reconnecting(ExchangeId::Ibkr),
                reconnect::Event::Item(Err(DataError::ReinitFailed {
                    attempt: 1,
                    error: Box::new(refused),
                })),
                reconnect::Event::Item(Ok(2)),
            ]
        );
    }

    #[tokio::test]
    async fn a_market_stream_yields_its_reconnecting_events_and_names_itself() {
        let inner = stream::iter([
            reconnect::Event::Reconnecting(ExchangeId::Ibkr),
            reconnect::Event::Item(Err(DataError::SubscriptionsEmpty)),
        ]);
        let stream = IbkrMarketStream::<()> {
            inner: inner.boxed(),
        };

        assert_eq!(format!("{stream:?}"), "IbkrMarketStream { .. }");
        let events: Vec<_> = stream.collect().await;
        assert_eq!(
            events,
            [
                reconnect::Event::Reconnecting(ExchangeId::Ibkr),
                reconnect::Event::Item(Err(DataError::SubscriptionsEmpty)),
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_with_no_subscriptions_is_refused_before_connecting() {
        let result = IbkrMarketStream::<()>::init(
            IbkrStreamConfig::default(),
            Arc::new(ContractRegistry::new()),
            vec![],
            MarketStreamOptions::default(),
        )
        .await;

        assert_eq!(result.err(), Some(DataError::SubscriptionsEmpty));
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
        assert_eq!(delayed.max_messages_per_second.get(), 40);
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
        assert_eq!(
            config.max_messages_per_second,
            DEFAULT_MAX_MESSAGES_PER_SECOND
        );
    }

    #[test]
    fn a_configuration_sets_its_message_rate() {
        let rate = NonZeroU32::new(25).unwrap();
        let config = IbkrStreamConfig::default().with_max_messages_per_second(rate);
        assert_eq!(config.max_messages_per_second, rate);

        let loaded: IbkrStreamConfig = serde_json::from_str(
            r#"{"host":"h","port":1,"client_id":2,"max_messages_per_second":25}"#,
        )
        .unwrap();
        assert_eq!(loaded.max_messages_per_second, rate);
    }

    #[test]
    fn a_configuration_with_no_message_rate_is_refused() {
        let zero = serde_json::from_str::<IbkrStreamConfig>(
            r#"{"host":"h","port":1,"client_id":2,"max_messages_per_second":0}"#,
        );
        assert!(zero.is_err());
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
