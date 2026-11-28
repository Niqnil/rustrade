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
//! - Live candles: they need a real-time market data subscription for the instrument
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
//! - **Quotes** ([`OrderBookL1`]): Best bid/ask via `market_data()` subscription. The same
//!   subscription reports halts as [`TradingStatus`] events (IB's Halted tick), on change only,
//!   and again as the first after a reconnect; IB sends no reason beyond regulatory or volatility,
//!   and no Limit Up-Limit Down price bands
//! - **Depth** ([`OrderBookEvent`]): L2 order book via `market_depth()` subscription
//! - **Trades** ([`PublicTrade`]): Tick-by-tick trades via `tick_by_tick_all_last()` subscription
//! - **Candles** ([`Candle`]): completed live bars via IB's historical bars kept up to date; see
//!   [`candles`](crate::exchange::ibkr::candles)
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
//! [`TradingStatus`]: crate::subscription::trading_status::TradingStatus
//! [`Candle`]: crate::subscription::candle::Candle
//! [`DataError::SubscriptionLimitReached`]: crate::error::DataError::SubscriptionLimitReached
//! [`DataError::MarketDataNotAvailable`]: crate::error::DataError::MarketDataNotAvailable
//! [`IbkrStreamConfig::with_market_data_type`]: crate::exchange::ibkr::IbkrStreamConfig::with_market_data_type
//! [`IbkrStreamConfig::max_messages_per_second`]: crate::exchange::ibkr::IbkrStreamConfig::max_messages_per_second
//! [`DataError::ConnectionLost`]: crate::error::DataError::ConnectionLost
//! [`IbkrMarketStream`]: crate::exchange::ibkr::IbkrMarketStream
//! [`reconnect::Event`]: crate::streams::reconnect::Event
//! [`MarketStreamOptions`]: crate::streams::consumer::MarketStreamOptions

mod bar_assembler;
pub mod candles;
pub mod depth;
// `flex` is private: its public surface is re-exported flat below (matching the `massive`
// reconciliation module), so callers use a single `ibkr::IbkrFlex*` path rather than the
// internal submodule layout.
mod flex;
pub mod greeks;
pub mod historical;
mod ib_clock;
pub mod options;
pub mod quotes;
mod status;
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
        consumer::{
            MarketStreamOptions, MarketStreamResult, StreamKey, market_stream_events,
            within_init_timeout,
        },
        reconnect::{
            self,
            stream::{ReconnectionBackoffPolicy, init_reconnecting_stream},
        },
    },
};
use bar_assembler::{BarAssembler, Output, ResumePoint};
use candles::{BAR_SETTLE, IbkrCandleRequest};
use chrono::{DateTime, Utc};
use depth::DepthAggregator;
use futures::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use greeks::GreeksAggregator;
use ib_clock::IbClock;
use ibapi::{
    Notice, RateLimiter, TRANSPORT_RECONNECT_CODE,
    client::blocking::Client,
    contracts::SecurityType,
    market_data::{
        MarketDataType, TradingHours,
        historical::{BarSize, HistoricalBarUpdate},
        realtime::{MarketDepths, TickTypes},
    },
    subscriptions::SubscriptionItem,
};
use quotes::QuoteAggregator;
use rust_decimal::Decimal;
use rustrade_instrument::{
    exchange::ExchangeId,
    ibkr::{ContractLookup, codes},
    instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
/// # Candles
///
/// A candle subscription ([`IbkrSubscription::candles`]) yields completed candles only, each once,
/// with its `close_time` as the event's `time_exchange`; [`candles`] says when a bar counts as
/// complete. Each candle is meant to equal the one a later history read gives for that period:
/// both come from IB's historical bar service.
///
/// - **Start:** the bar forming when the stream first connects, or
///   [`since`](IbkrCandleRequest::since).
/// - **After a reconnect** each candle subscription resumes after the last candle the stream
///   yielded. Its request reaches back over the gap, the bars IB returns are deduplicated by
///   open, and a bar that was forming at the loss is emitted once complete. A candle the lost
///   connection had queued but the stream never yielded is fetched again. IB limits how far back
///   a request may reach for each bar size, and refuses a gap past it, which ends the
///   subscription.
/// - **Bar ends are timed on IB's clock.** IB's server time is sampled when a connection with
///   candle subscriptions opens, before they are made, and again every ten minutes on a thread of
///   its own, so that no worker waits on it. It is counted forward on the monotonic clock, so a
///   step of the host's clock moves no deadline. The bar forming at connect, the default start,
///   is the one forming on IB's clock. An offset from the host's clock larger than half
///   of [`BAR_SETTLE`] is logged as a warning, since it points at the host's time
///   synchronisation. A failed sample keeps the last estimate, and is tried again ten minutes
///   later. If the first fails, the host's clock stands in until then, the default start
///   included.
/// - **Without a live market data subscription** IB sends the history and then 2188, after which
///   no live bar comes: the subscription ends with [`DataError::MarketDataNotAvailable`].
///
/// Known limitations:
/// - A candle the timer emitted misses an update IB sends for it later than [`BAR_SETTLE`] after
///   its end. That update is dropped with a warning.
/// - A bar IB sends with a value that does not convert to a decimal (NaN) is skipped with a
///   warning, as is one dated out of range.
/// - IB adjusts its history afterwards, so a later read may differ from a candle emitted live.
/// - IB counts an identical request for bars of 30 seconds or less within 15 seconds as a pacing
///   violation, which a reconnect that quick may meet.
/// - Between candles the subscription is silent for up to a bar plus [`BAR_SETTLE`], and for the
///   whole closure while the session is closed (overnight and weekends, in regular hours only):
///   a silence timeout in the [`MarketStreamOptions`] must allow for both.
/// - Whether a candle subscription takes one of the account's market data lines is not stated by
///   IB.
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
    /// * `contracts` - Where each subscription's contract is looked up by its instrument name.
    ///   IB resolves each contract description on request, so a contract need not be resolved
    ///   first: a [`ContractsByName`] can hold one built locally. The stream only reads it, so an
    ///   execution client's [`ContractRegistry`] can be passed as well. It is read again on each
    ///   reconnect, so a contract added through a clone of it before then is used.
    /// * `subscriptions` - List of subscriptions to activate
    /// * `options` - How the stream reconnects, and whether it is ended when it stays silent; see
    ///   [`MarketStreamOptions`]. A silence timeout counts the market events of all subscriptions
    ///   together, so set it longer than the quietest market the stream carries is ever silent.
    ///   The options' [`init_timeout`](MarketStreamOptions::init_timeout) bounds each attempt to
    ///   open a connection; see below.
    ///
    /// # Errors
    ///
    /// The first connection's error:
    /// - [`DataError::SubscriptionsEmpty`] if `subscriptions` is empty.
    /// - [`DataError::InvalidSubscription`] for the first subscription the stream cannot make:
    ///   depth rows outside 1-20, or a candle `since` later than one bar from now
    ///   ([`IbkrCandleRequest::since`]). Nothing is connected.
    /// - [`DataError::Socket`] if the connection to IB fails, the configured market data type
    ///   cannot be sent, or no subscription could be activated (no contract found in `contracts`).
    /// - [`DataError::InitTimeout`] if the connection is not open within the options'
    ///   [`init_timeout`](MarketStreamOptions::init_timeout).
    ///
    /// A failed reconnect is not returned: the stream yields it, as above, and tries again.
    ///
    /// # Initialisation timeout
    ///
    /// An attempt connects, completes IB's handshake, sends the market data type, reads IB's
    /// server time when the stream has a candle subscription, and starts the subscriptions. These
    /// steps block, so they run on tokio's blocking thread pool, where a step cannot be
    /// interrupted. An attempt that times out is abandoned instead: it stops at its next check
    /// (before connecting, before sending the market data type, before reading the server time,
    /// and before starting the subscriptions) and closes whatever it connected, holding its
    /// blocking thread until then. The stream's next attempt waits for that, so that two of its
    /// connections never use the same client ID at once, and the wait counts toward the next
    /// attempt's own timeout.
    ///
    /// That wait is within one stream. TWS/Gateway refuses a connection whose client ID another
    /// connection holds, and it can go on holding the ID for a moment after that connection has
    /// closed. So a new stream, or another `init` call, with the same client ID can be refused:
    /// - when the first attempt times out, `init` returns while the abandoned attempt may still
    ///   be connecting, which can last past the timeout by as long as the TCP connect waits (see
    ///   below; the operating system's default is about two minutes on Linux);
    /// - when a stream is dropped, its connection closes on a thread of its own, after the drop
    ///   has returned.
    ///
    /// The refusal is a [`DataError::IbkrClientIdInUse`]. `init` returns the first attempt's
    /// refusal without retrying it; a retry succeeds once the earlier connection has closed and
    /// TWS/Gateway has let the ID go, so retry with a backoff, or use another client ID. Once the
    /// stream has started, a refused reconnect is yielded and retried after the stream's backoff,
    /// as any other failed reconnect.
    ///
    /// `ibapi` bounds each read of the handshake and the server time request (30 s each), but not
    /// the TCP connect, which waits as long as the operating system allows. Without an
    /// initialisation timeout an attempt can therefore stall that long on an unreachable host.
    /// A timeout shorter than a healthy open takes abandons every attempt, so keep it well above
    /// the time TWS/Gateway takes to answer.
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
    ///
    /// [`ContractsByName`]: rustrade_instrument::ibkr::ContractsByName
    /// [`ContractRegistry`]: rustrade_instrument::ibkr::ContractRegistry
    pub async fn init(
        config: IbkrStreamConfig,
        contracts: impl ContractLookup + Send + Sync + 'static,
        subscriptions: Vec<IbkrSubscription<K>>,
        options: MarketStreamOptions,
    ) -> Result<Self, DataError> {
        if subscriptions.is_empty() {
            return Err(DataError::SubscriptionsEmpty);
        }
        let now = Utc::now();
        if let Some((instrument, reason)) = subscriptions
            .iter()
            .find_map(|s| Some((&s.instrument, s.kind.refusal(now)?)))
        {
            return Err(DataError::InvalidSubscription {
                exchange: ExchangeId::Ibkr,
                instrument: instrument.clone(),
                reason,
            });
        }
        let contracts: Arc<dyn ContractLookup + Send + Sync> = Arc::new(contracts);
        let shared = Shared::default();
        let backoff = options.reconnect.clone();
        info!(
            ?options,
            subscriptions = subscriptions.len(),
            "IbkrMarketStream with auto reconnect initialising"
        );

        let init_timeout = options.init_timeout();

        let attempts = init_reconnecting_stream(move || {
            let config = config.clone();
            let contracts = contracts.clone();
            let subscriptions = subscriptions.clone();
            let shared = shared.clone();
            let delay = shared.quick_ends.lock().take_delay(&backoff);
            async move {
                // Outside the timeout: the wait is the stream's own backoff, which can be as long
                // as the timeout itself.
                wait_before_open(delay).await;
                let opening = shared.opening.clone();
                within_init_timeout(
                    init_timeout,
                    opening.open(move |abandoned| {
                        Connection::open(
                            &config,
                            &shared,
                            contracts.as_ref(),
                            subscriptions,
                            abandoned,
                        )
                    }),
                )
                .await
                .map(|connection| connection.handed_out().boxed())
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
    rx: mpsc::UnboundedReceiver<Envelope<K>>,
    client: Arc<Client>,
    /// Set by [`close`](Self::close) before it closes the connection, so that a worker or the
    /// watcher can tell this stream's own close from a shutdown `ibapi` or TWS/Gateway caused:
    /// `ibapi` reports both alike, as `Error::Shutdown` to a running subscription and as
    /// `Error::ConnectionReset` to one still being sent. Once it is set, nothing more is reported.
    closing: Arc<AtomicBool>,
    /// What the stream's connections share: where [`close`](Self::close) leaves the close
    /// running, and how long connections last.
    shared: Shared,
    /// When the connection was handed to the stream ([`handed_out`](Self::handed_out)); `None`
    /// until then, so that neither a failed open nor one abandoned after it completed is counted
    /// as a connection that ended.
    opened_at: Option<std::time::Instant>,
}

/// What a stream's connections share, one after another.
#[derive(Debug, Clone, Default)]
struct Shared {
    /// Whether TWS/Gateway's link to IB's servers is down, as the last link notice any of the
    /// stream's connections read said: see [`link_notice`].
    link_down: Arc<AtomicBool>,
    /// The connection being opened, which the next open waits for.
    opening: Opening,
    pending_close: PendingClose,
    quick_ends: Arc<parking_lot::Mutex<QuickEnds>>,
    /// Each candle subscription's [`ResumePoint`], by its index in the stream's subscriptions:
    /// set to its start when it is first made, and to a candle's open when the stream yields that
    /// candle. A connection made after a loss resumes each from here, so that a candle the lost
    /// connection had queued but the stream never yielded is fetched again rather than skipped.
    candle_resume: Arc<parking_lot::Mutex<HashMap<usize, ResumePoint>>>,
}

impl Shared {
    /// The item `envelope` carries, as the stream yields it, recording the resume point a candle
    /// delivers.
    fn yield_item<K>(&self, envelope: Envelope<K>) -> Result<MarketEvent<K, DataKind>, DataError> {
        if let Some((index, open)) = envelope.delivers {
            self.candle_resume
                .lock()
                .insert(index, ResumePoint::After(open));
        }
        envelope.item
    }

    /// Where the candle subscription `index` making `request` starts or resumes: its resume point,
    /// set first to `request.since`, or else the open of the bar forming at `now`.
    fn candle_resume_point(
        &self,
        index: usize,
        request: &IbkrCandleRequest,
        now: DateTime<Utc>,
    ) -> ResumePoint {
        *self.candle_resume.lock().entry(index).or_insert_with(|| {
            ResumePoint::From(
                request
                    .since
                    .unwrap_or_else(|| request.bar_size.bar_open(now)),
            )
        })
    }
}

/// What a connection's workers and watcher send to it: an item for the stream, and for a candle,
/// what yielding it delivers.
struct Envelope<K> {
    item: Result<MarketEvent<K, DataKind>, DataError>,
    /// For a candle: its subscription's index and its open, which become that subscription's
    /// resume point ([`Shared::candle_resume`]) once the stream yields it.
    delivers: Option<(usize, DateTime<Utc>)>,
}

impl<K> From<Result<MarketEvent<K, DataKind>, DataError>> for Envelope<K> {
    fn from(item: Result<MarketEvent<K, DataKind>, DataError>) -> Self {
        Self {
            item,
            delivers: None,
        }
    }
}

/// The sending end of a connection's channel.
type Sender<K> = mpsc::UnboundedSender<Envelope<K>>;

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

    /// The wait before the next attempt, which it takes: the wait `policy` gives after as many
    /// failures in a row as there were quick ends in a row, jitter included, once after each quick
    /// end, else none.
    fn take_delay(&mut self, policy: &ReconnectionBackoffPolicy) -> std::time::Duration {
        if !std::mem::take(&mut self.owed) {
            return std::time::Duration::ZERO;
        }
        policy.wait(self.in_a_row)
    }
}

/// What a quote subscription's ticks report: the quote, and the trading status from IB's Halted
/// tick.
struct QuoteTicks {
    aggregator: QuoteAggregator,
    statuses: status::StatusChanges,
}

impl QuoteTicks {
    fn new(security_type: &SecurityType) -> Self {
        Self {
            aggregator: QuoteAggregator::new(security_type),
            statuses: status::StatusChanges::default(),
        }
    }

    /// The event `tick`, received at `now`, makes, with its exchange time, if it makes one.
    fn update(
        &mut self,
        tick: &TickTypes,
        now: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, DataKind)> {
        // The quote aggregator reads no generic tick, so a generic tick is only ever a status.
        if let TickTypes::Generic(generic) = tick {
            // IB's tick carries no time of its own.
            return self
                .statuses
                .change(generic)
                .map(|status| (now, DataKind::TradingStatus(status)));
        }
        self.aggregator
            .update(tick, now)
            .map(|l1| (l1.last_update_time, DataKind::OrderBookL1(l1)))
    }
}

/// The stream's attempts to open a connection, run one at a time: an attempt waits until the one
/// before it has returned, including one abandoned while it was still blocked, so that it does not
/// connect with the same client ID while the earlier connection may still open.
///
/// Scoped to one stream on purpose, as [`PendingClose`] is: a guard shared across streams could
/// not cover the other holders of a client ID (an execution client, another process) nor the
/// moment TWS/Gateway keeps an ID after its connection closes, and a client ID shared by two
/// streams is the caller's configuration error.
#[derive(Debug, Clone, Default)]
struct Opening(Arc<tokio::sync::Mutex<()>>);

impl Opening {
    /// Run `open` on tokio's blocking thread pool once the stream's previous open has returned,
    /// and return its result. The turn passes on when this returns.
    ///
    /// If the returned future is dropped before `open` returns (an initialisation timeout, or the
    /// stream dropped), `open`'s [`OpenAbandoned`] is set, so that it stops at its next check. The
    /// stream's next open then waits until `open` has returned and whatever it returned, such as a
    /// connection that opened just too late, has been dropped and so closed. Dropped while still
    /// waiting for its turn, the future runs no `open` at all.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime.
    async fn open<T>(
        &self,
        open: impl FnOnce(&OpenAbandoned) -> Result<T, DataError> + Send + 'static,
    ) -> Result<T, DataError>
    where
        T: Send + 'static,
    {
        let turn = match self.0.clone().try_lock_owned() {
            Ok(turn) => turn,
            Err(_) => {
                info!("Waiting for an abandoned IB connection attempt to stop before connecting");
                self.0.clone().lock_owned().await
            }
        };
        let abandoned = OpenAbandoned::default();
        let _abandon_on_drop = abandoned.on_drop();
        // `turn` goes back with the result, after it in the tuple, so that it is released only
        // once the result has been dropped. If this future was dropped first, tokio drops both:
        // on the blocking thread when `open` returns, or, if `open` had already returned, on the
        // thread that dropped this future, an async worker (so the result's `Drop` must not
        // block).
        let (result, _turn) = tokio::task::spawn_blocking(move || (open(&abandoned), turn))
            .await
            .map_err(|e| DataError::Socket(format!("IB connect task: {e}")))?;
        result
    }
}

/// Whether the attempt that started an open has been abandoned, so that the open stops at its next
/// step; see [`Opening::open`].
#[derive(Debug, Clone, Default)]
struct OpenAbandoned(Arc<AtomicBool>);

impl OpenAbandoned {
    /// A guard that sets the flag when dropped.
    fn on_drop(&self) -> AbandonOnDrop {
        AbandonOnDrop(self.0.clone())
    }

    /// An error if the attempt has been abandoned, to stop the open before `step`.
    ///
    /// The error is never delivered: the attempt that would have returned it is gone. The log line
    /// is what shows it.
    fn check(&self, step: &str) -> Result<(), DataError> {
        if !self.0.load(Ordering::Relaxed) {
            return Ok(());
        }
        debug!(
            step,
            "IB connection attempt abandoned; stopping it before this step"
        );
        Err(DataError::Socket(format!(
            "IB connection attempt abandoned before {step}"
        )))
    }
}

/// Sets an [`OpenAbandoned`] when dropped: held by the attempt, so that the open learns when the
/// attempt is dropped.
struct AbandonOnDrop(Arc<AtomicBool>);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        // A stop flag guarding no other data.
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The close of a stream's last connection, which runs on a thread of its own, since
/// `ibapi`'s disconnect blocks until the client's threads have exited: dropping a connection must
/// not block the async task that drops it. The next connection waits for it, so that it does not
/// connect with the same client ID while the old connection is still open.
#[derive(Debug, Clone, Default)]
struct PendingClose(Arc<parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>>);

/// A close for [`PendingClose`] to run, with any close started before it.
type CloseJob = Box<dyn FnOnce() + Send + 'static>;

/// Start a thread for the close, as `std::thread::Builder::spawn` does: on an error, `job` is
/// dropped without having run.
fn spawn_close_thread(job: CloseJob) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("ibkr-close".to_owned())
        .spawn(job)
}

impl PendingClose {
    /// Run `close` on a new thread, after any close started before it.
    ///
    /// If no thread can be started (the OS refuses one), the earlier closes and `close` run here
    /// instead, so the call blocks for as long as they take: on an async worker, that blocks the
    /// worker. A [`wait`](Self::wait) meanwhile blocks until they have finished.
    fn start(&self, close: impl FnOnce() + Send + 'static) {
        self.start_with(spawn_close_thread, close);
    }

    /// [`start`](Self::start), with the thread started by `spawn`.
    fn start_with(
        &self,
        spawn: impl FnOnce(CloseJob) -> std::io::Result<std::thread::JoinHandle<()>>,
        close: impl FnOnce() + Send + 'static,
    ) {
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
        let spawned = spawn(Box::new(move || {
            if let Some(job) = on_thread.lock().take() {
                job();
            }
        }));
        match spawned {
            Ok(handle) => *pending = Some(handle),
            Err(error) => {
                warn!(%error, "Failed to spawn the IB close thread; closing on this one");
                // Run with the slot still locked, so that the next connection's `wait` blocks
                // until this close has finished, as it would for a close thread. The job takes no
                // lock of this slot, so it cannot deadlock on it.
                if let Some(job) = job.lock().take() {
                    job();
                }
                drop(pending);
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
        let envelope = std::task::ready!(Pin::new(&mut self.rx).poll_recv(cx));
        Poll::Ready(envelope.map(|envelope| self.shared.yield_item(envelope)))
    }
}

impl<K> Connection<K>
where
    K: Clone + Send + 'static,
{
    /// Connect as `config` says and start a worker per subscription, plus the notice watcher,
    /// once the stream's previous connection has closed.
    ///
    /// Blocks for the previous close, the handshake, and IB's server time if a subscription is for
    /// candles. Once `abandoned` is set, it stops before its next step with an error, closing the
    /// client if it has connected one.
    fn open(
        config: &IbkrStreamConfig,
        shared: &Shared,
        contracts: &dyn ContractLookup,
        subscriptions: Vec<IbkrSubscription<K>>,
        abandoned: &OpenAbandoned,
    ) -> Result<Self, DataError> {
        shared.pending_close.wait();
        abandoned.check("connecting")?;

        let url = format!("{}:{}", config.host, config.port);
        info!(%url, client_id = config.client_id, "Connecting to IB for market data");

        let (client, notices) = Client::builder()
            .address(&url)
            .client_id(config.client_id)
            .rate_limiter(RateLimiter::per_second(
                config.max_messages_per_second.get(),
            ))
            .connect_with_notice_stream()
            .map_err(connect_error)?;
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
        abandoned.check("configuring the connection")?;

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
        // IB's time for the connection's candle subscriptions, sampled before any is made, so that
        // each starts on IB's time and no worker waits on the sample.
        let clock = Arc::new(IbClock::new(BAR_SETTLE));
        if subscriptions
            .iter()
            .any(|s| matches!(s.kind, IbkrSubscriptionKind::Candles(_)))
        {
            abandoned.check("reading IB's server time")?;
            clock.sync(|| server_time(&connection.client));
        }
        abandoned.check("starting the subscriptions")?;

        // Spawn a worker thread for each subscription
        for (
            index,
            IbkrSubscription {
                instrument,
                key,
                kind,
            },
        ) in subscriptions.into_iter().enumerate()
        {
            let contract = match contracts.get_contract(&instrument) {
                Some(c) => c,
                None => {
                    warn!(
                        %instrument,
                        "Contract not found by name, skipping subscription"
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
                IbkrSubscriptionKind::Candles(request) => {
                    let resume = shared.candle_resume_point(index, &request, clock.now());
                    let job = CandleJob {
                        request,
                        index,
                        resume,
                        clock: clock.clone(),
                    };
                    Self::run_candles_subscription(client, contract, job, worker("candles"))
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

        Ok(connection)
    }

    /// The connection, as handed to the stream now: from here on its end counts toward
    /// [`QuickEnds`].
    fn handed_out(mut self) -> Self {
        self.opened_at = Some(std::time::Instant::now());
        self
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

            let mut ticks = QuoteTicks::new(&contract.security_type);

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
                if let Some((time_exchange, kind)) = ticks.update(&tick, now)
                    && worker.send(time_exchange, now, kind).is_break()
                {
                    break;
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

impl<K> Connection<K>
where
    K: Clone + Send + 'static,
{
    /// Run a candle subscription: a keepUpToDate bar request reaching back to `job.resume`, its
    /// bars passed through a [`BarAssembler`] timed on IB's clock, and each completed bar sent as
    /// a candle.
    ///
    /// 2188 ends it with [`DataError::MarketDataNotAvailable`]: IB keeps the request open but sends
    /// no live bar after it.
    fn run_candles_subscription(
        client: Arc<Client>,
        contract: ibapi::contracts::Contract,
        job: CandleJob,
        worker: Worker<K>,
    ) -> Result<(), DataError> {
        worker.spawn(move |worker| {
            let CandleJob {
                request,
                index,
                resume,
                clock,
            } = job;
            let bar_size = BarSize::from(request.bar_size);
            let step = request.bar_size.duration();
            let ib_now = || clock.now();
            let duration =
                candles::history_duration(resume.first_open(step), ib_now(), request.bar_size);
            let trading_hours = if request.regular_trading_hours_only {
                TradingHours::Regular
            } else {
                TradingHours::Extended
            };
            let sub = match client
                .historical_data(&contract, bar_size)
                .what_to_show(request.source.into())
                .duration(duration)
                .trading_hours(trading_hours)
                .stream()
            {
                Ok(s) => s,
                Err(e) => return worker.fail(e),
            };
            debug!(instrument = %worker.instrument, ?resume, %duration, "Candle subscription made");

            let assembler = BarAssembler::new(step, BAR_SETTLE, resume);
            let resync = || {
                let resync_client = client.clone();
                clock.resync(move || server_time(&resync_client));
            };
            worker.run_candles(&sub, assembler, index, bar_size, ib_now, resync);
        })
    }
}

/// How long a candle worker waits for IB when no bar is forming before it looks again. It need not
/// be short: a close or a lost connection ends the subscription, which wakes the wait at once, and
/// a dropped stream is seen at the next candle sent.
const CANDLE_IDLE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// What a candle worker reads from its subscription in one look.
enum Next {
    /// An item IB sent.
    Item(Result<SubscriptionItem<HistoricalBarUpdate>, ibapi::Error>),
    /// Nothing yet: nothing was queued, or the wait passed without an item.
    Nothing,
    /// The subscription ended: it delivered its end or a terminal error.
    Ended,
    /// The subscription's channel closed without an end or an error, which `ibapi` reports as
    /// nothing at all, so that a worker reading it as [`Nothing`](Self::Nothing) would look again
    /// at once, and again, without end.
    Closed,
}

/// The candle worker's view of its subscription, so that [`Worker::run_candles`] can be driven
/// by a scripted feed in tests.
trait BarFeed {
    /// What is queued now, without waiting. It cannot tell a closed channel from an empty one;
    /// [`next_within`](Self::next_within) can.
    fn next_now(&self) -> Next;

    /// The next item, waiting up to `wait`; a zero `wait` returns [`Next::Nothing`] at once.
    fn next_within(&self, wait: std::time::Duration) -> Next;
}

impl BarFeed for ibapi::subscriptions::sync::Subscription<HistoricalBarUpdate> {
    fn next_now(&self) -> Next {
        next_from(self.try_next(), self.is_terminated(), true)
    }

    fn next_within(&self, wait: std::time::Duration) -> Next {
        let started = std::time::Instant::now();
        let item = self.next_timeout(wait);
        next_from(item, self.is_terminated(), started.elapsed() >= wait)
    }
}

/// What a look at a subscription returned, as a [`Next`]: `item`, else whether the subscription
/// is `terminated`, else whether the look `waited_out` its wait. `ibapi` returns nothing before
/// the wait is out, from a subscription it has not ended, only when the channel has closed (as
/// read in `ibapi` 5.7's source: recheck on an upgrade).
fn next_from(
    item: Option<Result<SubscriptionItem<HistoricalBarUpdate>, ibapi::Error>>,
    terminated: bool,
    waited_out: bool,
) -> Next {
    match item {
        Some(item) => Next::Item(item),
        None if terminated => Next::Ended,
        None if waited_out => Next::Nothing,
        None => Next::Closed,
    }
}

/// What a candle worker needs besides its contract and [`Worker`].
struct CandleJob {
    request: IbkrCandleRequest,
    /// The subscription's index in the stream's subscriptions: its key in
    /// [`Shared::candle_resume`].
    index: usize,
    /// Where the subscription starts or resumes.
    resume: ResumePoint,
    /// IB's time for the connection.
    clock: Arc<IbClock>,
}

/// IB's server time to the millisecond, as a sample for the [`IbClock`].
fn server_time(client: &Client) -> Result<DateTime<Utc>, String> {
    let time = client.server_time_millis().map_err(|e| e.to_string())?;
    DateTime::from_timestamp(time.unix_timestamp(), time.nanosecond())
        .ok_or_else(|| format!("IB server time {time} out of range"))
}

impl<K> Worker<K>
where
    K: Clone + Send + 'static,
{
    /// The candle subscription's loop: read `feed`, pass its bars through `assembler` at IB's time
    /// `ib_now`, and send each completed bar as a candle of `bar_size` for the subscription
    /// `index`, calling `resync` before each look. It ends when the subscription does, when a
    /// notice or an error ends it (sent as the worker's terminal error), when the channel closes
    /// without an end ([`DataError::ConnectionLost`]), or once the stream has been dropped.
    ///
    /// Between bars it waits for IB until the forming bar's timer, or at most
    /// [`CANDLE_IDLE_WAIT`].
    fn run_candles(
        &self,
        feed: &impl BarFeed,
        mut assembler: BarAssembler<ibapi::market_data::historical::Bar>,
        index: usize,
        bar_size: BarSize,
        ib_now: impl Fn() -> DateTime<Utc>,
        resync: impl Fn(),
    ) {
        let mut out = Vec::new();
        loop {
            resync();
            let wait = assembler
                .next_deadline()
                .map_or(CANDLE_IDLE_WAIT, |deadline| {
                    deadline
                        .signed_duration_since(ib_now())
                        .to_std()
                        .unwrap_or(std::time::Duration::ZERO)
                        .min(CANDLE_IDLE_WAIT)
                });
            // What IB has already sent comes first, so that a due timer never runs ahead of a
            // queued closing update.
            let next = match feed.next_now() {
                Next::Nothing => feed.next_within(wait),
                next => next,
            };
            let now = ib_now();
            match next {
                Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Historical(data)))) => {
                    let bars = data.bars.into_iter().filter_map(|bar| self.dated(bar));
                    assembler.on_history(bars, now, &mut out);
                }
                Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Update(bar)))) => {
                    if let Some((open, bar)) = self.dated(bar) {
                        assembler.on_update(open, bar, now, &mut out);
                    }
                }
                Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::End { .. }))) => {}
                Next::Item(Ok(SubscriptionItem::Notice(notice))) => {
                    if self.candle_notice(&notice).is_break() {
                        break;
                    }
                }
                Next::Item(Err(e)) => {
                    self.fail(e);
                    break;
                }
                Next::Ended => break,
                Next::Closed => {
                    self.channel_closed();
                    break;
                }
                Next::Nothing => assembler.on_tick(now, &mut out),
            }
            if self.deliver(index, bar_size, &mut out).is_break() {
                break;
            }
        }
    }
}

impl<K> Connection<K> {
    /// Close the connection, which ends every worker's subscription and the notice watcher, with
    /// nothing reported for it. Returns at once: the close runs on a thread of its own
    /// ([`PendingClose`]), which the stream's next connection waits for. If the OS refuses that
    /// thread, the close runs here instead and blocks the caller, an async worker when the
    /// connection is dropped on one, until `ibapi`'s disconnect returns.
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
    tx: Sender<K>,
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
        let _ = self.tx.send(
            Err(DataError::ConnectionLost {
                exchange: ExchangeId::Ibkr,
                reason,
            })
            .into(),
        );
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
    tx: Sender<K>,
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
    /// do not poison on panic, so shared state (the contract lookup, etc.) remains usable.
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
                        let _ = self.tx.send(
                            Err(DataError::Socket(format!(
                                "{stage} subscription {} panicked: {msg}",
                                self.instrument
                            )))
                            .into(),
                        );
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
        self.send_envelope(time_exchange, time_received, kind, None)
    }

    fn send_envelope(
        &self,
        time_exchange: DateTime<Utc>,
        time_received: DateTime<Utc>,
        kind: DataKind,
        delivers: Option<(usize, DateTime<Utc>)>,
    ) -> ControlFlow<()> {
        let event = MarketEvent {
            time_exchange,
            time_received,
            exchange: ExchangeId::Ibkr,
            instrument: self.key.clone(),
            kind,
        };
        match self.tx.send(Envelope {
            item: Ok(event),
            delivers,
        }) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// Send each completed bar in `out` as a candle of `bar_size`, stamped with its
    /// `close_time`, which sets the candle subscription `index`'s resume point once the stream
    /// yields it; warn of each late update. Drains `out`; `Break` once the stream has been dropped.
    ///
    /// A bar that does not convert (a price IB sent as NaN, say) is skipped with a warning.
    fn deliver(
        &self,
        index: usize,
        bar_size: BarSize,
        out: &mut Vec<Output<ibapi::market_data::historical::Bar>>,
    ) -> ControlFlow<()> {
        for output in out.drain(..) {
            match output {
                Output::Final { open, bar } => match historical::bar_to_candle(&bar, bar_size) {
                    Ok(candle) => {
                        let close_time = candle.close_time;
                        let kind = DataKind::Candle(candle);
                        let delivers = Some((index, open));
                        if self
                            .send_envelope(close_time, Utc::now(), kind, delivers)
                            .is_break()
                        {
                            return ControlFlow::Break(());
                        }
                    }
                    Err(error) => {
                        warn!(instrument = %self.instrument, %open, %error, "Skipping a candle IB sent with a value that does not convert");
                    }
                },
                Output::Late { open } => {
                    warn!(instrument = %self.instrument, %open, "Dropping an update for a candle already emitted, or older than the one forming; that candle went out without it");
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// `bar` with its open, or `None` with a warning if its time is out of range.
    fn dated(
        &self,
        bar: ibapi::market_data::historical::Bar,
    ) -> Option<(DateTime<Utc>, ibapi::market_data::historical::Bar)> {
        match historical::bar_open_time(&bar) {
            Ok(open) => Some((open, bar)),
            Err(error) => {
                warn!(instrument = %self.instrument, %error, "Skipping a bar IB sent with a time out of range");
                None
            }
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
        if self.closed_by_connection(&error) {
            return;
        }
        if ends_connection(&error) {
            self.report_lost(&error);
            return;
        }
        let stage = self.stage;
        error!(instrument = %self.instrument, error = %error, "{stage} subscription failed");
        let _ = self
            .tx
            .send(Err(subscription_error(stage, &self.instrument, error)).into());
    }

    /// End the worker because its subscription's channel closed without an end or an error
    /// ([`Next::Closed`]): send [`DataError::ConnectionLost`], so that the stream makes a new
    /// connection, unless the connection's own close has begun, as for [`fail`](Self::fail).
    fn channel_closed(&self) {
        let reason = "the subscription's channel closed without an end or an error";
        if !self.closed_by_connection(&reason) {
            self.report_lost(&reason);
        }
    }

    /// Whether the connection's own [`close`](Connection::close) has begun, which explains
    /// `error`: if so, log it at debug.
    fn closed_by_connection(&self, error: &dyn std::fmt::Display) -> bool {
        let closing = self.closing.load(Ordering::Acquire);
        if closing {
            debug!(instrument = %self.instrument, error = %error, "{} subscription ended by the connection's close", self.stage);
        }
        closing
    }

    /// Send [`DataError::ConnectionLost`] naming the subscription and `error`, with a warning.
    fn report_lost(&self, error: &dyn std::fmt::Display) {
        let stage = self.stage;
        warn!(instrument = %self.instrument, error = %error, "{stage} subscription ended by a lost connection");
        let _ = self.tx.send(
            Err(DataError::ConnectionLost {
                exchange: ExchangeId::Ibkr,
                reason: format!("{stage} subscription {}: {error}", self.instrument),
            })
            .into(),
        );
    }

    /// Act on a notice IB sent for this subscription without ending it: `Break` after sending a
    /// [`DataError::MarketDataNotAvailable`] when it says no data will come (IB sends 10089 this
    /// way); otherwise log it and `Continue`.
    fn notice(&self, notice: &Notice) -> ControlFlow<()> {
        if let Some(error) = notice_error(&self.instrument, notice) {
            self.refuse(notice, error);
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

    /// [`notice`](Self::notice) for a candle subscription, which 2188 also ends, with a
    /// [`DataError::MarketDataNotAvailable`]: IB keeps the request open but sends no live bar.
    fn candle_notice(&self, notice: &Notice) -> ControlFlow<()> {
        if notice.code == codes::RECENT_HISTORY_NOT_SUBSCRIBED {
            let error = DataError::MarketDataNotAvailable {
                exchange: ExchangeId::Ibkr,
                instrument: self.instrument.clone(),
                code: notice.code,
                message: notice.message.clone(),
            };
            self.refuse(notice, error);
            return ControlFlow::Break(());
        }
        self.notice(notice)
    }

    /// End the worker on `notice`, which refuses the subscription: log it, and send `error`.
    fn refuse(&self, notice: &Notice, error: DataError) {
        let stage = self.stage;
        error!(instrument = %self.instrument, code = notice.code, message = %notice.message, "{stage} subscription refused");
        let _ = self.tx.send(Err(error).into());
    }
}

/// The stream error for an `ibapi` error that failed the connect: [`DataError::IbkrClientIdInUse`]
/// for TWS/Gateway's client ID refusal, else [`DataError::Socket`], `"IB connect: {error}"`.
fn connect_error(error: ibapi::Error) -> DataError {
    match error {
        ibapi::Error::ClientIdInUse { client_id } => DataError::IbkrClientIdInUse { client_id },
        other => DataError::Socket(format!("IB connect: {other}")),
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
    } else if codes::MARKET_DATA_NOT_AVAILABLE_CODES.contains(&notice.code) {
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
    use rustrade_instrument::ibkr::ContractsByName;

    mod quote_ticks {
        use super::*;
        use crate::subscription::trading_status::TradingState;
        use ibapi::{
            contracts::tick_types::TickType,
            market_data::realtime::{TickAttribute, TickGeneric, TickPrice},
        };

        fn halted(value: f64) -> TickTypes {
            TickTypes::Generic(TickGeneric {
                tick_type: TickType::Halted,
                value,
            })
        }

        fn bid(price: f64) -> TickTypes {
            TickTypes::Price(TickPrice {
                tick_type: TickType::Bid,
                price,
                attributes: TickAttribute::default(),
            })
        }

        fn at(seconds: i64) -> DateTime<Utc> {
            DateTime::from_timestamp(seconds, 0).unwrap()
        }

        #[test]
        fn a_halted_tick_is_a_status_stamped_when_received() {
            let mut ticks = QuoteTicks::new(&SecurityType::Stock);

            let event = ticks.update(&halted(1.0), at(5));

            let (time_exchange, kind) = event.unwrap();
            assert_eq!(time_exchange, at(5));
            let DataKind::TradingStatus(status) = kind else {
                panic!("not a status: {kind:?}");
            };
            assert_eq!(status.state, TradingState::Halted);
        }

        #[test]
        fn a_repeated_halted_tick_makes_no_event() {
            let mut ticks = QuoteTicks::new(&SecurityType::Stock);
            ticks.update(&halted(1.0), at(5));

            assert_eq!(ticks.update(&halted(1.0), at(6)), None);
        }

        #[test]
        fn a_price_tick_is_a_quote_and_a_halted_tick_leaves_it_alone() {
            let mut ticks = QuoteTicks::new(&SecurityType::Stock);
            ticks.update(&halted(0.0), at(5));

            let (_, kind) = ticks.update(&bid(100.5), at(6)).unwrap();

            let DataKind::OrderBookL1(l1) = kind else {
                panic!("not a quote: {kind:?}");
            };
            assert_eq!(
                l1.best_bid.map(|level| level.price),
                Some(Decimal::new(1005, 1))
            );
            assert_eq!(
                ticks.update(&halted(0.0), at(7)),
                None,
                "the quote does not reset the status"
            );
        }
    }

    /// The receiving end of a connection's channel, reading the items alone.
    struct Items(mpsc::UnboundedReceiver<Envelope<()>>);

    impl Items {
        fn try_recv(
            &mut self,
        ) -> Result<Result<MarketEvent<(), DataKind>, DataError>, mpsc::error::TryRecvError>
        {
            self.0.try_recv().map(|envelope| envelope.item)
        }

        async fn recv(&mut self) -> Option<Result<MarketEvent<(), DataKind>, DataError>> {
            self.0.recv().await.map(|envelope| envelope.item)
        }
    }

    /// A quotes worker for `instrument`, and the stream's receiving end.
    fn worker(instrument: &str, closing: bool) -> (Worker<()>, Items) {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = Worker {
            stage: "quotes",
            instrument: InstrumentNameExchange::new(instrument),
            key: (),
            tx,
            closing: Arc::new(AtomicBool::new(closing)),
        };
        (worker, Items(rx))
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
        let (tx, rx) = mpsc::unbounded_channel::<Envelope<()>>();
        let mut rx = Items(rx);
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
        backoff_jitter_percent: 0,
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

    /// A spawner that fails as `std::thread::Builder::spawn` does when the OS refuses a thread:
    /// the job is dropped without having run.
    fn refuse_thread(job: CloseJob) -> std::io::Result<std::thread::JoinHandle<()>> {
        drop(job);
        Err(std::io::Error::other("no thread for you"))
    }

    #[test]
    fn a_close_runs_on_the_callers_thread_when_no_thread_can_be_started() {
        let pending = PendingClose::default();
        let ran_on = Arc::new(parking_lot::Mutex::new(None));

        let on_close = ran_on.clone();
        pending.start_with(refuse_thread, move || {
            *on_close.lock() = Some(std::thread::current().id());
        });

        assert_eq!(
            *ran_on.lock(),
            Some(std::thread::current().id()),
            "the close ran, on the caller's thread, before start_with returned"
        );
    }

    #[test]
    fn a_close_run_on_the_callers_thread_waits_for_the_earlier_close_first() {
        let pending = PendingClose::default();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let earlier = order.clone();
        pending.start(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            earlier.lock().push(1);
        });
        let later = order.clone();
        pending.start_with(refuse_thread, move || later.lock().push(2));

        assert_eq!(*order.lock(), [1, 2]);
    }

    #[test]
    fn closes_start_on_threads_again_after_a_refused_thread() {
        let pending = PendingClose::default();
        pending.start_with(refuse_thread, || {});

        let caller = std::thread::current().id();
        let off_caller = Arc::new(AtomicBool::new(false));
        let on_close = off_caller.clone();
        pending.start(move || {
            on_close.store(std::thread::current().id() != caller, Ordering::Release);
        });
        pending.wait();

        assert!(
            off_caller.load(Ordering::Acquire),
            "the next close ran, off the caller's thread"
        );
    }

    #[test]
    fn a_wait_blocks_until_a_close_on_the_callers_thread_has_finished() {
        let pending = PendingClose::default();
        let (started, close_started) = std::sync::mpsc::channel::<()>();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let done = Arc::new(AtomicBool::new(false));

        let (closer, on_close) = (pending.clone(), done.clone());
        let closing = std::thread::spawn(move || {
            closer.start_with(refuse_thread, move || {
                let _ = started.send(());
                let _ = released.recv();
                on_close.store(true, Ordering::Release);
            });
        });
        close_started.recv().expect("the close started");

        let (returned, wait_returned) = std::sync::mpsc::channel::<bool>();
        let (waiter, seen) = (pending.clone(), done.clone());
        let waiting = std::thread::spawn(move || {
            waiter.wait();
            let _ = returned.send(seen.load(Ordering::Acquire));
        });
        assert_eq!(
            wait_returned.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "wait returned while the close was still running"
        );

        release.send(()).unwrap();
        assert_eq!(
            wait_returned.recv(),
            Ok(true),
            "wait returned after the close"
        );
        closing.join().unwrap();
        waiting.join().unwrap();
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
                backoff_jitter_percent: 0,
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
            ContractsByName::new(),
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
    fn a_client_id_refusal_keeps_its_type_and_client_id() {
        let error = connect_error(ibapi::Error::ClientIdInUse { client_id: 7 });

        assert_eq!(error, DataError::IbkrClientIdInUse { client_id: 7 });
    }

    #[test]
    fn another_connect_failure_is_a_socket_error_naming_the_connect() {
        let error = ibapi::Error::Simple("connection refused".to_owned());
        let expected = format!("IB connect: {error}");

        assert_eq!(connect_error(error), DataError::Socket(expected));
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

    /// 14:30 UTC on 2026-11-23 plus `seconds`.
    fn t(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_795_444_200 + seconds, 0).unwrap()
    }

    /// An intraday ibapi bar opening at `open`, closing at `close`.
    fn ib_bar(open: DateTime<Utc>, close: f64) -> ibapi::market_data::historical::Bar {
        ibapi::market_data::historical::Bar {
            date: ibapi::market_data::historical::BarTimestamp::DateTime(
                time::OffsetDateTime::from_unix_timestamp(open.timestamp()).unwrap(),
            ),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close,
            volume: 1_000.0,
            wap: 100.25,
            count: 12,
        }
    }

    fn min1_trades() -> IbkrCandleRequest {
        IbkrCandleRequest::new(
            candles::IbkrLiveBarSize::Min1,
            candles::IbkrBarSource::Trades,
        )
    }

    #[test]
    fn the_fixed_time_is_1430_utc() {
        assert_eq!(t(0).to_rfc3339(), "2026-11-23T14:30:00+00:00");
    }

    #[test]
    fn yielding_a_candle_records_its_open_as_the_resume_point() {
        let shared = Shared::default();
        let event = MarketEvent {
            time_exchange: t(60),
            time_received: t(61),
            exchange: ExchangeId::Ibkr,
            instrument: (),
            kind: DataKind::Candle(
                historical::bar_to_candle(&ib_bar(t(0), 100.5), BarSize::Min).unwrap(),
            ),
        };
        let item = shared.yield_item(Envelope {
            item: Ok(event.clone()),
            delivers: Some((3, t(0))),
        });
        assert_eq!(item, Ok(event));
        assert_eq!(
            shared.candle_resume.lock().get(&3),
            Some(&ResumePoint::After(t(0)))
        );
    }

    #[test]
    fn yielding_another_item_records_nothing() {
        let shared = Shared::default();
        let item = shared.yield_item::<()>(Err(DataError::SubscriptionsEmpty).into());
        assert_eq!(item, Err(DataError::SubscriptionsEmpty));
        assert!(shared.candle_resume.lock().is_empty());
    }

    #[test]
    fn a_candle_subscription_starts_with_the_forming_bar_and_keeps_its_start() {
        let shared = Shared::default();
        let request = min1_trades();
        assert_eq!(
            shared.candle_resume_point(0, &request, t(75)),
            ResumePoint::From(t(60))
        );
        // A later connection resumes from the same start, not the bar forming then.
        assert_eq!(
            shared.candle_resume_point(0, &request, t(500)),
            ResumePoint::From(t(60))
        );
        // Each subscription has its own.
        assert_eq!(
            shared.candle_resume_point(1, &request, t(500)),
            ResumePoint::From(t(480))
        );
    }

    #[test]
    fn a_candle_subscription_starts_at_since_and_resumes_after_the_last_candle_yielded() {
        let shared = Shared::default();
        let request = min1_trades().with_since(t(-600));
        assert_eq!(
            shared.candle_resume_point(2, &request, t(75)),
            ResumePoint::From(t(-600))
        );
        let _ = shared.yield_item::<()>(Envelope {
            item: Err(DataError::SubscriptionsEmpty),
            delivers: Some((2, t(-120))),
        });
        assert_eq!(
            shared.candle_resume_point(2, &request, t(75)),
            ResumePoint::After(t(-120))
        );
    }

    #[test]
    fn deliver_sends_each_completed_bar_as_a_candle_stamped_with_its_close() {
        let (worker, mut rx) = worker("AAPL", false);
        let mut out = vec![
            Output::Final {
                open: t(0),
                bar: ib_bar(t(0), 100.5),
            },
            Output::Late { open: t(0) },
        ];

        let flow = worker.deliver(4, BarSize::Min, &mut out);

        assert_eq!(flow, ControlFlow::Continue(()));
        assert!(out.is_empty());
        let envelope = rx.0.try_recv().unwrap();
        assert_eq!(envelope.delivers, Some((4, t(0))));
        let event = envelope.item.unwrap();
        assert_eq!(event.time_exchange, t(60));
        let DataKind::Candle(candle) = event.kind else {
            panic!("expected a candle: {:?}", event.kind);
        };
        assert_eq!(candle.close_time, t(60));
        assert_eq!(candle.close, rust_decimal_macros::dec!(100.5));
        assert_eq!(candle.trade_count, Some(12));
        assert!(rx.try_recv().is_err(), "a late update sends nothing");
    }

    #[test]
    fn deliver_skips_a_bar_that_does_not_convert() {
        let (worker, mut rx) = worker("AAPL", false);
        let mut out = vec![
            Output::Final {
                open: t(0),
                bar: ib_bar(t(0), f64::NAN),
            },
            Output::Final {
                open: t(60),
                bar: ib_bar(t(60), 100.0),
            },
        ];

        assert_eq!(
            worker.deliver(0, BarSize::Min, &mut out),
            ControlFlow::Continue(())
        );

        let envelope = rx.0.try_recv().unwrap();
        assert_eq!(envelope.delivers, Some((0, t(60))));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn deliver_stops_once_the_stream_is_dropped() {
        let (worker, rx) = worker("AAPL", false);
        drop(rx);
        let mut out = vec![Output::Final {
            open: t(0),
            bar: ib_bar(t(0), 100.0),
        }];

        assert_eq!(
            worker.deliver(0, BarSize::Min, &mut out),
            ControlFlow::Break(())
        );
    }

    #[test]
    fn a_bar_is_dated_by_its_open() {
        let (worker, _rx) = worker("AAPL", false);
        let (open, _) = worker.dated(ib_bar(t(120), 100.0)).unwrap();
        assert_eq!(open, t(120));
    }

    #[test]
    fn notice_2188_ends_a_candle_subscription_as_market_data_not_available() {
        let (worker, mut rx) = worker("AAPL", false);
        let message = "Historical data requests within 15 minutes of the current time require a \
                       valid market data subscription";

        assert_eq!(
            worker.candle_notice(&notice(2188, message)),
            ControlFlow::Break(())
        );
        assert_eq!(
            rx.try_recv(),
            Ok(Err(DataError::MarketDataNotAvailable {
                exchange: ExchangeId::Ibkr,
                instrument: InstrumentNameExchange::new("AAPL"),
                code: 2188,
                message: message.to_owned(),
            }))
        );
    }

    #[test]
    fn a_candle_subscription_takes_other_notices_as_any_subscription_does() {
        let (worker, mut rx) = worker("AAPL", false);
        // A farm warning ends nothing.
        assert_eq!(
            worker.candle_notice(&notice(2107, "HMDS data farm connection is inactive")),
            ControlFlow::Continue(())
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(
            worker.candle_notice(&notice(
                10089,
                "Requested market data requires additional subscription"
            )),
            ControlFlow::Break(())
        );
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(DataError::MarketDataNotAvailable { code: 10089, .. }))
        ));
    }

    /// A candle worker for `instrument`, and the stream's receiving end.
    fn candle_worker(instrument: &str, closing: bool) -> (Worker<()>, Items) {
        let (mut worker, rx) = worker(instrument, closing);
        worker.stage = "candles";
        (worker, rx)
    }

    /// One look a [`ScriptedFeed`] expects, and what it returns: `Next::Nothing` from `next_now`
    /// for a look that needs a wait.
    struct Look {
        /// Whether the look is a wait ([`BarFeed::next_within`]) rather than a queued item.
        waits: bool,
        /// IB's time once the look returns.
        at: DateTime<Utc>,
        next: Next,
    }

    fn queued(at: DateTime<Utc>, next: Next) -> Look {
        Look {
            waits: false,
            at,
            next,
        }
    }

    fn waited(at: DateTime<Utc>, next: Next) -> Look {
        Look {
            waits: true,
            at,
            next,
        }
    }

    /// A candle subscription that plays a script of looks, then ends; it moves IB's time, and
    /// records each wait it is asked for.
    struct ScriptedFeed {
        script: std::cell::RefCell<std::collections::VecDeque<Look>>,
        clock: std::cell::Cell<DateTime<Utc>>,
        waits: std::cell::RefCell<Vec<std::time::Duration>>,
        looks: std::cell::Cell<usize>,
    }

    impl ScriptedFeed {
        fn new(start: DateTime<Utc>, script: Vec<Look>) -> Self {
            Self {
                script: std::cell::RefCell::new(script.into()),
                clock: std::cell::Cell::new(start),
                waits: std::cell::RefCell::new(Vec::new()),
                looks: std::cell::Cell::new(0),
            }
        }

        /// Count a look, failing a worker that spins.
        fn look(&self) {
            self.looks.set(self.looks.get() + 1);
            assert!(self.looks.get() < 100, "the worker is spinning");
        }

        fn play(&self) -> Next {
            let look = self.script.borrow_mut().pop_front().unwrap();
            self.clock.set(look.at);
            look.next
        }

        fn remaining(&self) -> usize {
            self.script.borrow().len()
        }

        /// Run `worker`'s candle loop for one-minute bars of subscription 4 from `resume` on this
        /// feed.
        fn run(&self, worker: &Worker<()>, resume: ResumePoint) {
            let assembler = BarAssembler::new(chrono::TimeDelta::minutes(1), BAR_SETTLE, resume);
            worker.run_candles(self, assembler, 4, BarSize::Min, || self.clock.get(), || {});
        }
    }

    impl BarFeed for ScriptedFeed {
        fn next_now(&self) -> Next {
            self.look();
            match self.script.borrow().front() {
                None => return Next::Ended,
                Some(look) if look.waits => return Next::Nothing,
                Some(_) => {}
            }
            self.play()
        }

        fn next_within(&self, wait: std::time::Duration) -> Next {
            self.look();
            self.waits.borrow_mut().push(wait);
            if wait.is_zero() {
                return Next::Nothing;
            }
            let front = self.script.borrow().front().map(|look| look.waits);
            assert_eq!(front, Some(true), "a wait the script does not expect");
            self.play()
        }
    }

    fn history(bars: Vec<ibapi::market_data::historical::Bar>) -> Next {
        Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Historical(
            ibapi::market_data::historical::HistoricalData {
                start: time::OffsetDateTime::UNIX_EPOCH,
                end: time::OffsetDateTime::UNIX_EPOCH,
                bars,
            },
        ))))
    }

    fn update(bar: ibapi::market_data::historical::Bar) -> Next {
        Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Update(bar))))
    }

    fn candle_notice_item(code: i32, message: &str) -> Next {
        Next::Item(Ok(SubscriptionItem::Notice(notice(code, message))))
    }

    /// The candles a worker sent, by open and close price; panics on an error item.
    fn candles_sent(rx: &mut Items) -> Vec<(DateTime<Utc>, Decimal)> {
        let mut candles = Vec::new();
        while let Ok(envelope) = rx.0.try_recv() {
            let (index, open) = envelope.delivers.expect("a candle");
            assert_eq!(index, 4);
            let DataKind::Candle(candle) = envelope.item.unwrap().kind else {
                panic!("expected a candle");
            };
            candles.push((open, candle.close));
        }
        candles
    }

    fn secs(seconds: u64) -> std::time::Duration {
        std::time::Duration::from_secs(seconds)
    }

    #[test]
    fn a_due_timer_emits_the_forming_bar_after_waiting_until_it() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(20),
            vec![
                queued(t(30), history(vec![ib_bar(t(0), 100.0)])),
                waited(t(62), Next::Nothing),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        // The bar opening at t(0) is due at its end plus the settle: 32 s after t(30).
        assert_eq!(*feed.waits.borrow(), vec![secs(32)]);
        assert_eq!(
            candles_sent(&mut rx),
            vec![(t(0), rust_decimal_macros::dec!(100))]
        );
    }

    #[test]
    fn a_queued_closing_update_is_read_before_the_due_timer_runs() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(20),
            vec![
                queued(t(30), history(vec![ib_bar(t(0), 100.0)])),
                // A notice ends the wait at the bar's timer, and runs no timer itself.
                waited(
                    t(62),
                    candle_notice_item(2107, "HMDS data farm connection is inactive"),
                ),
                queued(t(62), update(ib_bar(t(0), 100.7))),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        // The timer was due, but the closing update queued behind the notice went in first.
        assert_eq!(
            candles_sent(&mut rx),
            vec![(t(0), rust_decimal_macros::dec!(100.7))]
        );
        // The only wait was the one the notice ended.
        assert_eq!(*feed.waits.borrow(), vec![secs(32)]);
    }

    #[test]
    fn a_timer_already_past_waits_nothing() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(20),
            vec![
                queued(t(30), history(vec![ib_bar(t(0), 100.0)])),
                waited(
                    t(65),
                    candle_notice_item(2107, "HMDS data farm connection is inactive"),
                ),
                // Nothing queued when the timer is past; a zero wait does not play this look.
                waited(t(200), Next::Nothing),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert_eq!(
            *feed.waits.borrow(),
            vec![secs(32), std::time::Duration::ZERO, CANDLE_IDLE_WAIT]
        );
        assert_eq!(
            candles_sent(&mut rx),
            vec![(t(0), rust_decimal_macros::dec!(100))]
        );
    }

    #[test]
    fn the_wait_is_at_most_the_idle_wait() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(0),
            vec![
                // No bar forming yet.
                waited(t(60), Next::Nothing),
                queued(t(61), history(vec![ib_bar(t(60), 100.0)])),
                // A bar forming 61 s from its timer.
                waited(t(121), Next::Nothing),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(60)));

        assert_eq!(*feed.waits.borrow(), vec![CANDLE_IDLE_WAIT; 2]);
        assert!(candles_sent(&mut rx).is_empty());
    }

    #[test]
    fn an_ended_subscription_ends_the_worker_without_waiting_or_emitting() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(20),
            vec![
                queued(t(30), history(vec![ib_bar(t(0), 100.0)])),
                queued(t(31), Next::Ended),
                queued(t(90), update(ib_bar(t(0), 100.7))),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert!(feed.waits.borrow().is_empty());
        assert_eq!(feed.remaining(), 1, "the worker read past the end");
        assert!(rx.try_recv().is_err(), "the forming bar is not emitted");
    }

    #[test]
    fn a_closed_channel_ends_the_worker_with_the_connection_lost() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(0),
            vec![
                waited(t(1), Next::Closed),
                queued(t(2), history(vec![ib_bar(t(0), 100.0)])),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert_eq!(feed.remaining(), 1, "the worker looked again");
        let Ok(Err(DataError::ConnectionLost { exchange, reason })) = rx.try_recv() else {
            panic!("expected the connection lost");
        };
        assert_eq!(exchange, ExchangeId::Ibkr);
        assert!(
            reason.starts_with("candles subscription AAPL: ") && reason.contains("channel closed"),
            "{reason}"
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_closed_channel_during_the_connections_close_reports_nothing() {
        let (worker, mut rx) = candle_worker("AAPL", true);
        let feed = ScriptedFeed::new(t(0), vec![waited(t(1), Next::Closed)]);

        feed.run(&worker, ResumePoint::From(t(0)));

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn an_error_ends_the_worker_with_it() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(0),
            vec![
                queued(t(1), Next::Item(Err(ibapi::Error::Shutdown))),
                queued(t(2), history(vec![ib_bar(t(0), 100.0)])),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert_eq!(feed.remaining(), 1);
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(DataError::ConnectionLost { .. }))
        ));
    }

    #[test]
    fn notice_2188_ends_the_worker_and_other_notices_and_the_end_marker_do_not() {
        let (worker, mut rx) = candle_worker("AAPL", false);
        let feed = ScriptedFeed::new(
            t(0),
            vec![
                queued(
                    t(1),
                    candle_notice_item(2107, "HMDS data farm connection is inactive"),
                ),
                queued(
                    t(2),
                    Next::Item(Ok(SubscriptionItem::Data(HistoricalBarUpdate::End {
                        start: time::OffsetDateTime::UNIX_EPOCH,
                        end: time::OffsetDateTime::UNIX_EPOCH,
                    }))),
                ),
                queued(
                    t(3),
                    candle_notice_item(2188, "requires a market data subscription"),
                ),
                queued(t(4), history(vec![ib_bar(t(0), 100.0)])),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert_eq!(feed.remaining(), 1);
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(DataError::MarketDataNotAvailable { code: 2188, .. }))
        ));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_dropped_stream_ends_the_worker_at_the_next_candle() {
        let (worker, rx) = candle_worker("AAPL", false);
        drop(rx);
        let feed = ScriptedFeed::new(
            t(0),
            vec![
                queued(
                    t(70),
                    history(vec![ib_bar(t(0), 100.0), ib_bar(t(60), 101.0)]),
                ),
                queued(t(71), update(ib_bar(t(60), 101.5))),
            ],
        );

        feed.run(&worker, ResumePoint::From(t(0)));

        assert_eq!(
            feed.remaining(),
            1,
            "the worker read on after the stream was dropped"
        );
    }

    #[test]
    fn a_look_reads_an_item_then_the_end_then_whether_it_waited_out() {
        let item = || Some(Err(ibapi::Error::Shutdown));
        for (terminated, waited_out) in [(false, false), (false, true), (true, false), (true, true)]
        {
            assert!(matches!(
                next_from(item(), terminated, waited_out),
                Next::Item(Err(ibapi::Error::Shutdown))
            ));
            let expected = match (terminated, waited_out) {
                (true, _) => "ended",
                (false, true) => "nothing",
                (false, false) => "closed",
            };
            let actual = match next_from(None, terminated, waited_out) {
                Next::Ended => "ended",
                Next::Nothing => "nothing",
                Next::Closed => "closed",
                Next::Item(_) => "item",
            };
            assert_eq!(
                actual, expected,
                "terminated {terminated}, waited out {waited_out}"
            );
        }
    }

    #[tokio::test]
    async fn a_stream_with_an_invalid_subscription_is_refused_before_connecting() {
        // A struct literal skips the constructor's check; the stream makes it.
        let depth = IbkrSubscription {
            key: (),
            instrument: InstrumentNameExchange::new("AAPL"),
            kind: IbkrSubscriptionKind::Depth { rows: 0 },
        };
        let candles = IbkrSubscription::candles(
            (),
            InstrumentNameExchange::new("MSFT"),
            min1_trades().with_since(Utc::now() + chrono::TimeDelta::hours(1)),
        );
        for (subscription, instrument, needle) in [
            (depth, "AAPL", "got 0"),
            (candles, "MSFT", "later than one bar"),
        ] {
            let result = IbkrMarketStream::<()>::init(
                IbkrStreamConfig::new("127.0.0.1", 1, 1),
                ContractsByName::new(),
                vec![
                    IbkrSubscription::quotes((), InstrumentNameExchange::new("SPY")),
                    subscription,
                ],
                MarketStreamOptions::default(),
            )
            .await;

            let Err(DataError::InvalidSubscription {
                exchange,
                instrument: refused,
                reason,
            }) = result
            else {
                panic!("expected an invalid subscription: {:?}", result.err());
            };
            assert_eq!(exchange, ExchangeId::Ibkr);
            assert_eq!(refused, InstrumentNameExchange::new(instrument));
            assert!(reason.contains(needle), "{reason}");
        }
    }

    /// How long the tests below give an attempt to open before abandoning it. Each attempt they
    /// abandon stays blocked until the test releases it, so the length only sets how long they
    /// take.
    const ABANDON_AFTER: std::time::Duration = std::time::Duration::from_millis(50);

    fn abandon_after() -> DataError {
        DataError::InitTimeout {
            timeout: ABANDON_AFTER,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_open_that_returns_in_time_gives_its_result() {
        let result = Opening::default()
            .open(|abandoned| Ok(abandoned.check("the next step").is_ok()))
            .await;

        assert_eq!(result, Ok(true));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_open_blocked_past_the_init_timeout_is_abandoned_at_its_next_step() {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let (seen, saw) = tokio::sync::oneshot::channel();
        let opening = Opening::default();
        let attempt = opening.open(move |abandoned| {
            let _ = released.recv();
            let _ = seen.send(abandoned.check("the next step"));
            Ok(())
        });

        let result = within_init_timeout(Some(ABANDON_AFTER), attempt).await;
        assert_eq!(result, Err(abandon_after()));

        release.send(()).unwrap();
        let next_step = saw.await.unwrap();
        assert!(
            matches!(&next_step, Err(DataError::Socket(reason)) if reason.contains("abandoned")),
            "{next_step:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_next_open_waits_until_an_abandoned_one_has_returned_and_its_result_is_dropped() {
        /// A result that takes a while to drop, as a connection closing does, and records when it
        /// has.
        struct SlowDrop(Arc<parking_lot::Mutex<Vec<&'static str>>>);
        impl Drop for SlowDrop {
            fn drop(&mut self) {
                std::thread::sleep(ABANDON_AFTER);
                self.0.lock().push("first dropped");
            }
        }

        let opening = Opening::default();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (release, released) = std::sync::mpsc::channel::<()>();
        let first = {
            let order = order.clone();
            opening.open(move |_| {
                let _ = released.recv();
                Ok(SlowDrop(order))
            })
        };
        assert!(
            within_init_timeout(Some(ABANDON_AFTER), first)
                .await
                .is_err()
        );

        let second = {
            let (opening, order) = (opening.clone(), order.clone());
            tokio::spawn(async move {
                opening
                    .open(move |_| {
                        order.lock().push("second opened");
                        Ok(())
                    })
                    .await
            })
        };
        tokio::time::sleep(ABANDON_AFTER).await;
        assert!(order.lock().is_empty(), "the second open ran first");

        release.send(()).unwrap();
        assert_eq!(second.await.unwrap(), Ok(()));
        assert_eq!(*order.lock(), ["first dropped", "second opened"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_attempt_abandoned_while_waiting_for_its_turn_runs_no_open() {
        let opening = Opening::default();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let first = opening.open(move |_| {
            let _ = released.recv();
            Ok(())
        });
        assert!(
            within_init_timeout(Some(ABANDON_AFTER), first)
                .await
                .is_err()
        );

        let ran = Arc::new(AtomicBool::new(false));
        let second = {
            let ran = ran.clone();
            opening.open(move |_| {
                ran.store(true, Ordering::Relaxed);
                Ok(())
            })
        };
        let result = within_init_timeout(Some(ABANDON_AFTER), second).await;
        assert_eq!(result, Err(abandon_after()));

        release.send(()).unwrap();
        // Opens take turns in order, so a second still queued would run before this one.
        assert_eq!(opening.open(|_| Ok(())).await, Ok(()));
        assert!(!ran.load(Ordering::Relaxed), "the abandoned attempt opened");
    }

    /// A stand-in for TWS/Gateway on a local port that accepts connections and never answers,
    /// and the port.
    fn silent_gateway() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_whose_handshake_stalls_is_refused_at_the_init_timeout() {
        let (listener, port) = silent_gateway();
        let (hang_up, hung_up) = std::sync::mpsc::channel::<()>();
        let gateway = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let _ = hung_up.recv();
            // Closing the socket ends the abandoned handshake, rather than leaving it to `ibapi`'s
            // own read timeout.
            drop(socket);
        });

        let result = IbkrMarketStream::init(
            IbkrStreamConfig::new("127.0.0.1", port, 1),
            ContractsByName::new(),
            vec![IbkrSubscription::quotes(
                (),
                InstrumentNameExchange::new("SPY"),
            )],
            MarketStreamOptions::default()
                .with_init_timeout(ABANDON_AFTER)
                .unwrap(),
        )
        .await;

        assert_eq!(result.err(), Some(abandon_after()));
        // An open abandoned before it connected never reached the gateway: connect here so that
        // its accept returns either way.
        let _unblock = std::net::TcpStream::connect(("127.0.0.1", port));
        hang_up.send(()).unwrap();
        gateway.join().unwrap();
    }

    #[test]
    fn an_open_abandoned_before_it_connects_makes_no_connection() {
        // A port nothing listens on, so that an attempt to connect is refused at once, and says so.
        let (listener, port) = silent_gateway();
        drop(listener);
        let abandoned = OpenAbandoned::default();
        drop(abandoned.on_drop());

        let result = Connection::<()>::open(
            &IbkrStreamConfig::new("127.0.0.1", port, 1),
            &Shared::default(),
            &ContractsByName::new(),
            vec![IbkrSubscription::quotes(
                (),
                InstrumentNameExchange::new("SPY"),
            )],
            &abandoned,
        );

        let Err(DataError::Socket(reason)) = &result else {
            panic!("expected the open to stop: {:?}", result.err());
        };
        assert_eq!(reason, "IB connection attempt abandoned before connecting");
    }
}
