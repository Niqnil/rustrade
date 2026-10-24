use crate::execution::{
    AccountStreamEvent,
    backfill::{FillRecoveryConfig, Watermark, backfill},
    error::ExecutionError,
    request::{ExecutionRequest, RequestFuture},
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::FnvHashSet;
use futures::{
    FutureExt, Stream, StreamExt,
    future::Either,
    stream::{BoxStream, FuturesUnordered},
};
use rustrade_data::streams::{
    consumer::{STREAM_RECONNECTION_POLICY, StreamKey},
    reconnect::stream::{
        ReconnectingStream, ReconnectionBackoffPolicy, ReinitFailure, init_reconnecting_stream,
    },
};
use rustrade_execution::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    client::{ExecutionClient, dedup::AccountEventDeduper},
    error::{AccountReinitFailure, AccountStreamInitError, ConnectivityError, OrderError},
    indexer::{AccountEventIndexer, AccountIndexError},
    map::ExecutionInstrumentMap,
    order::{
        Order, OrderKey, UnindexedOrderKey,
        request::{
            OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, UnindexedOrderResponseCancel,
        },
        state::{OrderState, UnindexedOrderState},
    },
};
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::{
    channel::{Tx, UnboundedTx, mpsc_unbounded},
    collection::snapshot::Snapshot,
};
use std::{
    fmt::{self, Debug},
    sync::Arc,
};
use tracing::{debug, error, info, warn};

/// How an [`ExecutionManager`] keeps its account stream going: how it reconnects, how it reads the
/// fills missed while the stream was down, and the clock it measures that on.
///
/// Set per venue, through [`ExecutionManager::init`] or
/// [`ExecutionBuilder::add_live_with`](super::builder::ExecutionBuilder::add_live_with), since
/// venues differ in how many fills one call returns and how long they keep them. Built from
/// [`Default`] and the `with_*` methods, so fields can be added without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AccountStreamOptions {
    /// The backoff between attempts to re-initialise an account stream that ended.
    pub reconnect: ReconnectionBackoffPolicy,
    /// The budget for reading the fills missed while the stream was down.
    pub fill_recovery: FillRecoveryConfig,
    /// Where "now" comes from: for the end of a read's span, and for the watermark it starts
    /// from. It must be the clock the venue stamps its fills on, so on a simulated venue it is
    /// the simulation's.
    pub clock: Clock,
}

impl Default for AccountStreamOptions {
    /// [`STREAM_RECONNECTION_POLICY`], [`FillRecoveryConfig::default`] and the system clock.
    fn default() -> Self {
        Self {
            reconnect: STREAM_RECONNECTION_POLICY,
            fill_recovery: FillRecoveryConfig::default(),
            clock: Clock::default(),
        }
    }
}

impl AccountStreamOptions {
    /// Sets [`reconnect`](Self::reconnect).
    #[must_use]
    pub fn with_reconnect(self, reconnect: ReconnectionBackoffPolicy) -> Self {
        Self { reconnect, ..self }
    }

    /// Sets [`fill_recovery`](Self::fill_recovery).
    #[must_use]
    pub fn with_fill_recovery(self, fill_recovery: FillRecoveryConfig) -> Self {
        Self {
            fill_recovery,
            ..self
        }
    }

    /// Sets [`clock`](Self::clock).
    #[must_use]
    pub fn with_clock(self, clock: Clock) -> Self {
        Self { clock, ..self }
    }
}

/// A source of the current time, injectable so a simulation or a test controls it.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>);

impl Clock {
    /// A clock that calls `now`.
    pub fn new(now: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        Self(Arc::new(now))
    }

    /// The current time.
    pub fn now(&self) -> DateTime<Utc> {
        (self.0)()
    }
}

impl Default for Clock {
    /// The system clock.
    fn default() -> Self {
        Self::new(Utc::now)
    }
}

impl Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Clock(..)")
    }
}

/// Per-exchange execution manager that actions order requests from the Engine and forwards back
/// responses.
///
/// Processes indexed Engine [`ExecutionRequest`]s by:
/// - Transforming the requests to use the associated exchange's asset and instrument names.
/// - Issues the request via it's associated exchange [`ExecutionClient`],
/// - Answers every request exactly once, keyed by the request: with the client's response, or with
///   a timeout once `request_timeout` elapses. Only [`ExecutionRequest::Shutdown`] abandons
///   requests in flight.
#[derive(Constructor)]
pub struct ExecutionManager<RequestStream, Client> {
    /// `Stream` of incoming Engine [`ExecutionRequest`]s.
    pub request_stream: RequestStream,

    /// Maximum `Duration` to wait for execution request responses from the [`ExecutionClient`].
    pub request_timeout: std::time::Duration,

    /// Transmitter for sending execution request responses back to the Engine.
    pub response_tx: UnboundedTx<AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,

    /// Exchange-specific [`ExecutionClient`] for executing orders.
    pub client: Arc<Client>,

    /// Mapper for converting between exchange-specific and index identifiers.
    ///
    /// For example, `InstrumentNameExchange` -> `InstrumentIndex`.
    pub indexer: AccountEventIndexer,

    /// Reconnecting exchange AccountStream (snapshot + updates), owned by this manager.
    ///
    /// Held here rather than handed to the caller so that [`run`](Self::run) can *sequence* it
    /// against the execution responses: the manager forwards both into `response_tx`, and on
    /// shutdown drains this stream before dropping that sender. A manager that does not hold the
    /// account stream cannot do that — it has no access to the thing it must drain first.
    pub account_stream:
        BoxStream<'static, AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,
}

/// Manual because [`BoxStream`] is not [`Debug`]; every other field is forwarded.
impl<RequestStream, Client> Debug for ExecutionManager<RequestStream, Client>
where
    RequestStream: Debug,
    Client: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionManager")
            .field("request_stream", &self.request_stream)
            .field("request_timeout", &self.request_timeout)
            .field("response_tx", &self.response_tx)
            .field("client", &self.client)
            .field("indexer", &self.indexer)
            .field("account_stream", &"BoxStream<AccountStreamEvent>")
            .finish()
    }
}

impl<RequestStream, Client> ExecutionManager<RequestStream, Client>
where
    // `'static` because the manager owns its AccountStream as a `BoxStream<'static, _>`, built
    // from these parameters -- and because it is spawned as a task, which requires it regardless.
    RequestStream:
        Stream<Item = ExecutionRequest<ExchangeIndex, InstrumentIndex>> + Unpin + 'static,
    Client: ExecutionClient + Send + Sync + 'static,
    Client::AccountStream: Send,
{
    /// Initialises a new `ExecutionManager` and the `Stream` of events it will emit.
    ///
    /// The returned `Stream` carries **everything** the manager produces: the exchange
    /// AccountStream (whose first item is a full account snapshot) *and* the responses to the
    /// [`ExecutionRequest`]s it actions. Both are forwarded by [`run`](Self::run) through a single
    /// channel, so the returned `Stream` ends exactly when the manager has finished — which is what
    /// makes a drained shutdown observable to the caller.
    ///
    /// # Why the AccountStream is not returned separately
    /// It used to be merged with the response channel by the caller, which ended the combined
    /// `Stream` as soon as *either* side finished and left the manager unable to sequence the two.
    /// A response resolving the last in-flight request could then tear the run down while the trade
    /// that opens the position was still unread on the account side, truncating both the trade and
    /// the balance ledger non-deterministically.
    ///
    /// # Re-initialisation
    /// When the AccountStream ends, the returned `Stream` yields one
    /// [`Reconnecting`](rustrade_data::streams::reconnect::Event::Reconnecting), and the manager
    /// re-initialises it with `options.reconnect`'s backoff, for as long as it runs. Each failed
    /// attempt is yielded as an [`AccountEventKind::ReinitFailed`], with no further
    /// `Reconnecting`. Whether to stop waiting is the caller's decision.
    ///
    /// An attempt the venue refused for a rate limit, with a wait it named
    /// ([`AccountStreamInitError::retry_after`]), is not followed by another until that wait has
    /// passed, even when the backoff is shorter.
    ///
    /// # Recovering missed fills
    ///
    /// Each re-initialisation after the first reads the fills made while the stream was down,
    /// with [`ExecutionClient::fetch_trades`], from the last moment the manager knows it was in
    /// sync (less `options.fill_recovery.overlap`). They are delivered as
    /// [`AccountEventKind::Trade`] events before the new
    /// [`AccountEventKind::Snapshot`], so the snapshot is the state after them. A span the read
    /// could not cover (the venue no longer holds the fills, the budget in
    /// `options.fill_recovery` ran out, or a call failed) is reported as an
    /// [`AccountEventKind::FillRecoveryGaveUp`] and the stream still opens: what to do about a
    /// gap is the caller's policy. The first open reads nothing, because the snapshot is its
    /// starting point.
    ///
    /// Fills are deduplicated by instrument and trade id, across the read and the new stream and
    /// across re-initialisations, with the 10,000 most recent kept. An order that ended without a
    /// fill in the gap is the snapshot's to show, as before.
    ///
    /// # Errors
    /// [`ExecutionError::Config`] if `client` and `indexer` are for different exchanges, and
    /// [`ExecutionError::AccountStreamInit`] if the first attempt to initialise the AccountStream
    /// fails.
    pub async fn init(
        request_stream: RequestStream,
        request_timeout: std::time::Duration,
        client: Arc<Client>,
        indexer: AccountEventIndexer,
        options: AccountStreamOptions,
    ) -> Result<(Self, impl Stream<Item = AccountStreamEvent> + Send), ExecutionError> {
        let AccountStreamOptions {
            reconnect: reconnect_policy,
            fill_recovery,
            clock,
        } = options;

        // Determine StreamKey & ExchangeId for use in logging
        let stream_key = Self::determine_account_stream_key(&indexer.map)?;

        info!(
            exchange_index = %indexer.map.exchange.key,
            exchange_id = %indexer.map.exchange.value,
            policy = ?reconnect_policy,
            ?stream_key,
            "AccountStream with auto reconnect initialising"
        );

        // Initialise reconnecting IndexedAccountStream (snapshot + updates)
        let client_clone = Arc::clone(&client);
        let indexer_clone = indexer.clone();
        // Shared by every re-initialisation: the closure is `Fn`, and a fill read after one
        // reconnect can be one an earlier stream delivered.
        let deduper = AccountEventDeduper::new();
        let watermark = Arc::new(parking_lot::Mutex::new(None::<Watermark>));
        let account_stream = init_reconnecting_stream(move || {
            let client = client_clone.clone();
            let indexer = indexer_clone.clone();
            let deduper = deduper.clone();
            let watermark = Arc::clone(&watermark);
            let clock = clock.clone();
            async move {
                // Allocate AssetNameExchanges & InstrumentNameExchanges to avoid lifetime issues
                let assets = indexer.map.exchange_assets().cloned().collect::<Vec<_>>();
                let instruments = indexer
                    .map
                    .exchange_instruments()
                    .cloned()
                    .collect::<Vec<_>>();

                // When this attempt began: what the watermark is on a first open, so a fill made
                // while the snapshot is fetched is inside the span of the next read.
                let opened = clock.now();

                // Where this open reads from: `None` on the first open. Taken before the stream
                // opens, so the span runs from the last event delivered to the moment it reads.
                let read_from = watermark
                    .lock()
                    .as_ref()
                    .map(|watermark| watermark.read_from(&fill_recovery));

                // Initialise AccountStream & apply indexing. Opened before the read, so a fill
                // made during it arrives on the stream.
                let updates = Self::init_indexed_account_stream(
                    &client,
                    indexer.clone(),
                    &deduper,
                    &assets,
                    &instruments,
                )
                .await?;

                // Fills missed while the stream was down, before the snapshot that follows them.
                let missed = match read_from {
                    Some(start) => {
                        let read = backfill(
                            |from, to| client.fetch_trades(from, to, &instruments),
                            &instruments,
                            start,
                            clock.now(),
                            &fill_recovery,
                        )
                        .await;
                        let exchange = indexer.map.exchange.value;
                        let trades = read.trades.into_iter().map(|trade| UnindexedAccountEvent {
                            exchange,
                            kind: AccountEventKind::Trade(trade),
                        });
                        let gaps = read.gaps.into_iter().map(|gap| UnindexedAccountEvent {
                            exchange,
                            kind: AccountEventKind::FillRecoveryGaveUp(gap),
                        });
                        trades.chain(gaps).collect::<Vec<_>>()
                    }
                    None => Vec::new(),
                };
                // Deduplicated as each is delivered, so a read dropped part-way marks none of
                // the fills it did not deliver.
                let missed = Self::index_account_stream(
                    futures::stream::iter(missed).filter({
                        let deduper = deduper.clone();
                        move |event| std::future::ready(!deduper.is_duplicate(event))
                    }),
                    indexer.clone(),
                );

                // Fetch AccountSnapshot & index
                let snapshot =
                    Self::fetch_indexed_account_snapshot(&client, &indexer, &assets, &instruments)
                        .await?;

                // Nothing delivered counts until it is delivered: the watermark moves as events
                // pass, so an attempt that fails above leaves it where it was.
                watermark.lock().get_or_insert(Watermark::opened(opened));

                // It's expected downstream consumers (eg/ EngineState will sync updates)
                // Boxed: the chain is named by every layer above it, once per client type.
                Ok(missed
                    .chain(futures::stream::once(std::future::ready(snapshot)))
                    .chain(updates)
                    .inspect(move |event| {
                        if let Some(watermark) = watermark.lock().as_mut() {
                            watermark.delivered(clock.now(), event);
                        }
                    })
                    .boxed())
            }
        })
        .await?;

        // Construct channel to communicate ExecutionRequest responses (ie/ AccountEvents) to Engine
        let (response_tx, response_rx) = mpsc_unbounded();

        // Boxed so the manager can poll it inline in `run`'s `select!` -- see `account_stream`.
        // A failed re-init is sent in-band, in order with the account events, so the consumer can
        // tell a reconnect that keeps failing from one still in its backoff. One the venue refused
        // for a rate limit is not retried before the wait it named, which also outlasts a stream
        // that ended for one (`StreamTerminationReason::RateLimited`): retrying sooner fails unsent
        // at best, and lengthens a ban at worst.
        let exchange = indexer.map.exchange.key;
        let account_stream = Box::pin(
            account_stream
                .with_reconnect_backoff_reporting_hinted(
                    reconnect_policy,
                    stream_key,
                    AccountStreamInitError::retry_after,
                )
                .with_reconnection_events_reporting(
                    indexer.map.exchange.value,
                    move |ReinitFailure { attempt, error }| AccountEvent {
                        exchange,
                        kind: AccountEventKind::ReinitFailed(AccountReinitFailure::new(
                            attempt, error,
                        )),
                    },
                ),
        );

        Ok((
            Self::new(
                request_stream,
                request_timeout,
                response_tx,
                client,
                indexer,
                account_stream,
            ),
            response_rx.into_stream(),
        ))
    }

    fn determine_account_stream_key(
        instrument_map: &Arc<ExecutionInstrumentMap>,
    ) -> Result<StreamKey, ExecutionError> {
        match (Client::EXCHANGE, instrument_map.exchange.value) {
            (ExchangeId::Mock, instrument_exchange) => Ok(StreamKey::new_general(
                "account_stream_mock",
                instrument_exchange,
            )),
            (ExchangeId::Simulated, instrument_exchange) => Ok(StreamKey::new_general(
                "account_stream_simulated",
                instrument_exchange,
            )),
            (client, instrument_exchange) if client == instrument_exchange => {
                Ok(StreamKey::new_general("account_stream", client))
            }
            (client, instrument_exchange) => Err(ExecutionError::Config(format!(
                "ExecutionManager Client ExchangeId: {client} does not match \
                    ExecutionInstrumentMap ExchangeId: {instrument_exchange}"
            ))),
        }
    }

    async fn fetch_indexed_account_snapshot(
        client: &Arc<Client>,
        indexer: &AccountEventIndexer,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<AccountEvent, AccountStreamInitError> {
        match client.account_snapshot(assets, instruments).await {
            Ok(snapshot) => {
                let indexed_snapshot = indexer.snapshot(snapshot)?;
                Ok(AccountEvent {
                    exchange: indexer.map.exchange.key,
                    kind: AccountEventKind::Snapshot(indexed_snapshot),
                })
            }
            Err(error) => Err(AccountStreamInitError::Client(indexer.client_error(error))),
        }
    }

    async fn init_indexed_account_stream(
        client: &Arc<Client>,
        indexer: AccountEventIndexer,
        deduper: &AccountEventDeduper,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<impl Stream<Item = AccountEvent> + use<RequestStream, Client>, AccountStreamInitError>
    {
        let stream = match client.account_stream(assets, instruments).await {
            Ok(stream) => stream,
            Err(error) => {
                return Err(AccountStreamInitError::Client(indexer.client_error(error)));
            }
        };

        // Deduplicated before indexing: the deduper keys on the venue's own names, and a fill the
        // stream repeats from the read, or from an earlier stream, is dropped here.
        let deduper = deduper.clone();
        Ok(Self::index_account_stream(
            stream.filter(move |event| std::future::ready(!deduper.is_duplicate(event))),
            indexer,
        ))
    }

    /// Index an unindexed account stream, logging and dropping each event the indexer rejects.
    ///
    /// A balance for an asset the map does not register is not a rejection: the first one per
    /// asset is a `warn!`, naming the asset, and later ones are `debug!`. The set of warned assets
    /// belongs to the stream, so a reconnected stream warns again.
    fn index_account_stream(
        stream: impl Stream<Item = UnindexedAccountEvent>,
        indexer: AccountEventIndexer,
    ) -> impl Stream<Item = AccountEvent> {
        let mut warned = FnvHashSet::<AssetNameExchange>::default();
        stream.filter_map(move |event| {
            std::future::ready(match indexer.index_account_event(event) {
                Ok(indexed_event) => Some(indexed_event),
                Err(AccountIndexError::UntrackedBalance(asset)) => {
                    if warned.contains(&asset) {
                        debug!(%asset, "dropped a balance for an asset the map does not register");
                    } else {
                        warn!(
                            %asset,
                            "dropped a balance for an asset the map does not register; later \
                             ones for it are logged at debug"
                        );
                        warned.insert(asset);
                    }
                    None
                }
                Err(AccountIndexError::Rejected(error)) => {
                    error!(?error, "dropped an account event the indexer rejected");
                    None
                }
            })
        })
    }

    /// Run the `ExecutionManager`, processing execution requests and forwarding both their
    /// responses and the exchange AccountStream into this manager's event channel.
    ///
    /// # Two ways to stop
    /// [`ExecutionRequest::Shutdown`] breaks out at once, abandoning anything in flight — the
    /// abrupt stop a live system wants.
    ///
    /// [`ExecutionRequest::Drain`] (or the request `Stream` ending) instead stops the manager
    /// *accepting* new requests, then:
    ///
    /// 1. every already in-flight open and cancel is awaited to completion, each response
    ///    forwarded — these are bounded by `request_timeout`, so this cannot hang;
    /// 2. the account tail — every event the exchange has already made available — is drained;
    /// 3. only then does the loop end, dropping `response_tx` and ending the `Stream` returned by
    ///    [`init`](Self::init).
    ///
    /// That last drop is what a caller observes as "this manager is finished", so performing it
    /// after the drain rather than before is what stops a shutdown from truncating the ledgers.
    pub async fn run(mut self) {
        let mut in_flight_cancels = FuturesUnordered::new();
        let mut in_flight_opens = FuturesUnordered::new();

        // Set once a Shutdown (or a closed request Stream) has been observed. New requests are no
        // longer accepted, but the loop keeps running until nothing is outstanding.
        let mut draining = false;

        // Set if the AccountStream ever ends. It reconnects indefinitely, so in practice this only
        // happens once the manager itself has torn the client down.
        let mut account_stream_done = false;

        loop {
            if draining && in_flight_cancels.is_empty() && in_flight_opens.is_empty() {
                // Every response this manager owed has been forwarded. Anything the exchange sent
                // alongside those responses is already queued on the account side, so take it
                // before the channel closes.
                if !account_stream_done {
                    // Borrows `account_stream` and `response_tx` as disjoint fields: the in-flight
                    // `FuturesUnordered` still hold a borrow of `self.client` until end of scope,
                    // so a `&mut self` method would not compile here even though both are empty.
                    Self::drain_ready_account_events(
                        &mut self.account_stream,
                        &self.response_tx,
                        self.indexer.map.exchange.value,
                    );
                }
                break;
            }

            let next_cancel_response = if in_flight_cancels.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_cancels.select_next_some())
            };

            let next_open_response = if in_flight_opens.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_opens.select_next_some())
            };

            tokio::select! {
                // Process exchange AccountStream events (balances, trades, order updates)
                event = self.account_stream.next(), if !account_stream_done => match event {
                    Some(event) => {
                        if self.response_tx.send(event).is_err() {
                            break;
                        }
                    }
                    None => {
                        account_stream_done = true;
                    }
                },

                // Process Engine ExecutionRequests
                request = self.request_stream.next(), if !draining => match request {
                    // Abrupt stop: whatever is in flight is abandoned, as the variant documents.
                    Some(ExecutionRequest::Shutdown) => {
                        break;
                    }
                    // Graceful stop. A closed request Stream is treated the same way: the Engine is
                    // gone, but anything already owed is still worth forwarding, and doing so is
                    // bounded by `request_timeout`.
                    Some(ExecutionRequest::Drain) | None => {
                        draining = true;
                    }
                    Some(ExecutionRequest::Cancel(request)) => {
                        // Panic since the system is set up incorrectly, so it's foolish to continue
                        let client_request = self
                            .indexer
                            .order_request(&request)
                            .unwrap_or_else(|error| panic!(
                                "ExecutionManager received cancel request for non-configured key: {error}"
                            ));

                        in_flight_cancels.push(RequestFuture::new(
                            self.client.cancel_order(client_request),
                            self.request_timeout,
                            request,
                        ))
                    },
                    Some(ExecutionRequest::Open(request)) => {
                        // Panic since the system is set up incorrectly, so it's foolish to continue
                        let client_request = self
                            .indexer
                            .order_request(&request)
                            .unwrap_or_else(|error| panic!(
                                "ExecutionManager received open request for non-configured key: {error}"
                            ));

                        in_flight_opens.push(RequestFuture::new(
                            self.client.open_order(client_request),
                            self.request_timeout,
                            request,
                        ))
                    }
                },

                // Process next ExecutionRequest::Cancel response
                (request, response) = next_cancel_response => {
                    let event = match response {
                        Ok(response) => self.process_cancel_response(request, response),
                        Err(_elapsed) => Self::process_cancel_timeout(request),
                    };

                    if self.response_tx.send(event).is_err() {
                        break;
                    }
                },

                // Process next ExecutionRequest::Open response
                (request, response) = next_open_response => {
                    let event = match response {
                        Ok(response) => self.process_open_response(request, response),
                        Err(_elapsed) => Self::process_open_timeout(request),
                    };

                    if self.response_tx.send(event).is_err() {
                        break;
                    }
                }
            }
        }

        info!(
            exchange = %self.indexer.map.exchange.value,
            "ExecutionManager shutting down"
        )
    }

    /// Forward every AccountStream event that is **already available**, then return.
    ///
    /// This is the shutdown tail. It polls the account stream only while it is `Ready`, so it
    /// terminates without a timer and without waiting on the exchange — it cannot hang, and it
    /// cannot make a shutdown slower than the events already queued for it.
    ///
    /// # Why "already available" is the right stopping point
    /// A simulated exchange emits an order's account events *before* it resolves that order's
    /// response. By the time the last in-flight response has been forwarded, every event those
    /// orders produced is therefore sitting in the account channel, ready. Draining to `Pending`
    /// collects exactly that tail and nothing more.
    ///
    /// A live exchange is not bound by that ordering and may still owe events. Waiting for them is
    /// deliberately not attempted: there is no point at which a venue can be said to owe nothing,
    /// so a manager that waited would be waiting on an unbounded condition. Live shutdowns use
    /// [`Shutdown::Immediate`](crate::shutdown::Shutdown::Immediate) and accept that.
    ///
    /// Returns the number of events forwarded, for logging.
    fn drain_ready_account_events(
        account_stream: &mut BoxStream<
            'static,
            AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>,
        >,
        response_tx: &UnboundedTx<AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,
        exchange: ExchangeId,
    ) -> usize {
        let mut drained = 0;

        while let Some(event) = account_stream.next().now_or_never() {
            let Some(event) = event else {
                // AccountStream ended.
                break;
            };

            drained += 1;

            if response_tx.send(event).is_err() {
                break;
            }
        }

        if drained > 0 {
            info!(
                %exchange,
                drained,
                "ExecutionManager drained AccountStream tail before shutting down"
            );
        }

        drained
    }

    /// Index a cancel response under the key of the request it answers.
    ///
    /// The response's own key should echo the request's. When it does not, or cannot be indexed,
    /// that is a client bug: it is logged, and the response is still delivered under the request's
    /// key, since nothing else would ever settle the request.
    fn process_cancel_response(
        &self,
        request: OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
        response: UnindexedOrderResponseCancel,
    ) -> AccountStreamEvent {
        let OrderResponseCancel { key, state } = response;
        self.check_response_key(&request.key, key, "cancel");

        AccountStreamEvent::Item(AccountEvent {
            exchange: request.key.exchange,
            kind: AccountEventKind::OrderCancelled(OrderResponseCancel {
                key: request.key,
                state: state.map_err(|error| self.indexer.order_error(error)),
            }),
        })
    }

    /// Log an `error!` if a response's key does not index to the key of the request it answers.
    fn check_response_key(
        &self,
        request_key: &OrderKey<ExchangeIndex, InstrumentIndex>,
        response_key: UnindexedOrderKey,
        kind: &'static str,
    ) {
        match self.indexer.order_key(response_key) {
            Ok(response_key) if response_key == *request_key => {}
            response_key => error!(
                exchange = %self.indexer.map.exchange.value,
                kind,
                ?request_key,
                ?response_key,
                "ExecutionManager received a response whose order key differs from its request's - delivering it under the request's key"
            ),
        }
    }

    fn process_cancel_timeout(
        order: OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
    ) -> AccountStreamEvent {
        let OrderRequestCancel { key, state: _ } = order;

        AccountStreamEvent::Item(AccountEvent {
            exchange: key.exchange,
            kind: AccountEventKind::OrderCancelled(OrderResponseCancel {
                key,
                state: Err(OrderError::Connectivity(ConnectivityError::Timeout)),
            }),
        })
    }

    /// Index an open response under the key of the request it answers, as
    /// [`process_cancel_response`](Self::process_cancel_response) does. The rest of the order is
    /// the venue's answer, taken from the response.
    fn process_open_response(
        &self,
        request: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        response: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    ) -> AccountStreamEvent {
        let Order {
            key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        } = response;
        self.check_response_key(&request.key, key, "open");

        AccountStreamEvent::Item(AccountEvent {
            exchange: request.key.exchange,
            kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
                key: request.key,
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state: self.indexer.order_state(state),
            })),
        })
    }

    fn process_open_timeout(
        order: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
    ) -> AccountStreamEvent {
        let OrderRequestOpen { key, state } = order;

        AccountStreamEvent::Item(AccountEvent {
            exchange: key.exchange,
            kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
                key,
                side: state.side,
                price: state.price,
                quantity: state.quantity,
                kind: state.kind,
                time_in_force: state.time_in_force,
                state: OrderState::inactive(OrderError::Connectivity(ConnectivityError::Timeout)),
            })),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::execution::request::ExecutionRequest;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        AccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
        balance::AssetBalance,
        error::{ApiError, ClientError, UnindexedClientError, UnindexedOrderError},
        map::generate_execution_instrument_map,
        order::{
            OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, OrderId, StrategyId},
            request::{RequestCancel, RequestOpen},
            state::Open,
        },
        trade::{AssetFees, Trade, TradeId, TradesRead},
    };
    use rustrade_instrument::{
        Side, index::IndexedInstruments, instrument::kind::InstrumentKindDiscriminant,
        test_utils::instrument,
    };

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    /// A venue rejection naming an asset the instrument map does not hold — an instrument name,
    /// as Binance's error mapping once put there.
    fn unresolvable_rejection() -> UnindexedOrderError {
        OrderError::Rejected(ApiError::BalanceInsufficient(
            Some(AssetNameExchange::new("ETH_USDT")),
            "insufficient balance".to_string(),
        ))
    }

    /// Answers every open and cancel with [`unresolvable_rejection`]. [`ExecutionManager::run`]
    /// calls nothing else.
    #[derive(Debug, Clone, Default)]
    struct RejectingClient {
        /// When set, every response names this instrument instead of echoing the request's.
        answer_instrument: Option<InstrumentNameExchange>,
        /// When set, every response names this client order id instead of echoing the request's.
        answer_cid: Option<ClientOrderId>,
    }

    impl RejectingClient {
        fn answer_key(
            &self,
            key: OrderKey<ExchangeId, &InstrumentNameExchange>,
        ) -> OrderKey<ExchangeId, InstrumentNameExchange> {
            OrderKey {
                exchange: key.exchange,
                instrument: self
                    .answer_instrument
                    .clone()
                    .unwrap_or_else(|| key.instrument.clone()),
                strategy: key.strategy,
                cid: self.answer_cid.clone().unwrap_or(key.cid),
            }
        }
    }

    impl ExecutionClient for RejectingClient {
        const EXCHANGE: ExchangeId = EXCHANGE;
        const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[];
        type Config = ();
        type AccountStream = futures::stream::Empty<UnindexedAccountEvent>;

        fn new(_: Self::Config) -> Self {
            Self::default()
        }

        async fn account_snapshot(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
            unreachable!("ExecutionManager::run does not fetch account snapshots")
        }

        async fn account_stream(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<Self::AccountStream, UnindexedClientError> {
            unreachable!("ExecutionManager::run does not open account streams")
        }

        async fn cancel_order(
            &self,
            request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
        ) -> UnindexedOrderResponseCancel {
            OrderResponseCancel {
                key: self.answer_key(request.key),
                state: Err(unresolvable_rejection()),
            }
        }

        async fn open_order(
            &self,
            request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            let OrderRequestOpen { key, state } = request;
            Order {
                key: self.answer_key(key),
                side: state.side,
                price: state.price,
                quantity: state.quantity,
                kind: state.kind,
                time_in_force: state.time_in_force,
                state: OrderState::inactive(unresolvable_rejection()),
            }
        }

        async fn fetch_balances(
            &self,
            _: &[AssetNameExchange],
        ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
            unreachable!("ExecutionManager::run does not fetch balances")
        }

        async fn fetch_open_orders(
            &self,
            _: &[InstrumentNameExchange],
        ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>
        {
            unreachable!("ExecutionManager::run does not fetch open orders")
        }

        async fn fetch_trades(
            &self,
            _: DateTime<Utc>,
            _: DateTime<Utc>,
            _: &[InstrumentNameExchange],
        ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>
        {
            unreachable!("ExecutionManager::run does not fetch trades")
        }
    }

    /// Send one open and one cancel for the same order through a manager over `client`, and
    /// collect everything it forwards once the request stream ends and it drains.
    async fn run_open_and_cancel(
        client: RejectingClient,
    ) -> (
        OrderKey,
        Vec<AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,
    ) {
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
            panic!("the instrument map should build");
        };
        let indexer = AccountEventIndexer::new(Arc::new(map));
        let key = OrderKey {
            exchange: indexer.map.exchange.key,
            instrument: InstrumentIndex(0),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::random(),
        };
        let requests = futures::stream::iter([
            ExecutionRequest::Open(OrderRequestOpen {
                key: key.clone(),
                state: RequestOpen::new(
                    Side::Sell,
                    None,
                    dec!(1),
                    OrderKind::Market,
                    TimeInForce::ImmediateOrCancel,
                ),
            }),
            ExecutionRequest::Cancel(OrderRequestCancel {
                key: key.clone(),
                state: RequestCancel { id: None },
            }),
        ]);
        let (response_tx, response_rx) = mpsc_unbounded();

        // The request stream ends after the two requests, so the manager drains and returns.
        ExecutionManager {
            request_stream: requests,
            request_timeout: std::time::Duration::from_secs(5),
            response_tx,
            client: Arc::new(client),
            indexer,
            account_stream: futures::stream::empty().boxed(),
        }
        .run()
        .await;

        (key, response_rx.into_stream().collect::<Vec<_>>().await)
    }

    /// Assert `events` holds exactly one open snapshot and one cancel response, both under `key`
    /// and both carrying `rejection`.
    fn assert_both_answered(
        key: &OrderKey,
        events: &[AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>],
        rejection: &OrderError<AssetIndex, InstrumentIndex>,
    ) {
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(
            events.iter().any(|event| matches!(
                event,
                AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::OrderSnapshot(Snapshot(order)),
                    ..
                }) if order.key == *key && order.state == OrderState::inactive(rejection.clone())
            )),
            "the open's answer must reach the Engine under the request's key: {events:?}"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                AccountStreamEvent::Item(AccountEvent {
                    kind: AccountEventKind::OrderCancelled(response),
                    ..
                }) if response.key == *key && response.state == Err(rejection.clone())
            )),
            "the cancel's answer must reach the Engine under the request's key: {events:?}"
        );
    }

    /// A rejection whose error names something the instrument map does not hold must still reach
    /// the Engine, settling the order, rather than be discarded and leave it in flight for good.
    #[tokio::test]
    async fn a_rejection_naming_an_unresolvable_asset_still_reaches_the_engine() {
        let (key, events) = run_open_and_cancel(RejectingClient::default()).await;

        // The asset is dropped, the rejection kept.
        let rejection = OrderError::Rejected(ApiError::BalanceInsufficient(
            None,
            "insufficient balance".to_string(),
        ));
        assert_both_answered(&key, &events, &rejection);
    }

    /// A response whose own key names an instrument the map does not hold is a client bug, but it
    /// still answers the request: it must reach the Engine under the request's key, rather than be
    /// discarded and leave the order in flight for good.
    #[tokio::test]
    async fn a_response_keyed_to_an_unknown_instrument_still_answers_its_request() {
        let client = RejectingClient {
            answer_instrument: Some(InstrumentNameExchange::new("not_in_the_map")),
            ..RejectingClient::default()
        };
        let (key, events) = run_open_and_cancel(client).await;

        let rejection = OrderError::Rejected(ApiError::BalanceInsufficient(
            None,
            "insufficient balance".to_string(),
        ));
        assert_both_answered(&key, &events, &rejection);
    }

    /// Opens an account stream that fails on the attempts in `fail_on`, counted from 0. A stream
    /// opened before attempt `open_from` ends at once; one opened from it on stays open.
    #[derive(Debug, Clone, Default)]
    struct FlakyAccountClient {
        fail_on: &'static [usize],
        open_from: usize,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
        /// When set, a failing attempt is a rate-limit refusal naming this wait, not a timeout.
        rate_limited_for: Option<std::time::Duration>,
        /// What a stream that stays open sends before it goes quiet.
        live: Vec<UnindexedAccountEvent>,
        /// What a stream that ends sends before it ends.
        ending: Vec<UnindexedAccountEvent>,
        /// The account snapshot calls that fail, counted from 0.
        snapshot_fail_on: &'static [usize],
        snapshots: Arc<std::sync::atomic::AtomicUsize>,
        /// The answers `fetch_trades` gives, in turn; an empty read once they run out.
        reads: Arc<
            std::sync::Mutex<
                std::collections::VecDeque<
                    Result<
                        TradesRead<AssetNameExchange, InstrumentNameExchange>,
                        UnindexedClientError,
                    >,
                >,
            >,
        >,
        /// The span of each `fetch_trades` call.
        fetched: Arc<std::sync::Mutex<Vec<(DateTime<Utc>, DateTime<Utc>)>>>,
    }

    impl ExecutionClient for FlakyAccountClient {
        const EXCHANGE: ExchangeId = EXCHANGE;
        const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[];
        type Config = ();
        type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

        fn new(_: Self::Config) -> Self {
            Self::default()
        }

        async fn account_snapshot(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
            let call = self
                .snapshots
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.snapshot_fail_on.contains(&call) {
                return Err(UnindexedClientError::Connectivity(
                    ConnectivityError::Timeout,
                ));
            }
            Ok(UnindexedAccountSnapshot::new(EXCHANGE, vec![], vec![]))
        }

        async fn account_stream(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<Self::AccountStream, UnindexedClientError> {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.fail_on.contains(&attempt) {
                Err(match self.rate_limited_for {
                    Some(wait) => UnindexedClientError::Api(ApiError::rate_limited(Some(wait))),
                    None => UnindexedClientError::Connectivity(ConnectivityError::Timeout),
                })
            } else if attempt < self.open_from {
                Ok(futures::stream::iter(self.ending.clone()).boxed())
            } else {
                Ok(futures::stream::iter(self.live.clone())
                    .chain(futures::stream::pending())
                    .boxed())
            }
        }

        async fn cancel_order(
            &self,
            _: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
        ) -> UnindexedOrderResponseCancel {
            unreachable!("the account stream does not cancel orders")
        }

        async fn open_order(
            &self,
            _: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            unreachable!("the account stream does not open orders")
        }

        async fn fetch_balances(
            &self,
            _: &[AssetNameExchange],
        ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
            unreachable!("the account stream does not fetch balances")
        }

        async fn fetch_open_orders(
            &self,
            _: &[InstrumentNameExchange],
        ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>
        {
            unreachable!("the account stream does not fetch open orders")
        }

        async fn fetch_trades(
            &self,
            start: DateTime<Utc>,
            end: DateTime<Utc>,
            _: &[InstrumentNameExchange],
        ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>
        {
            self.fetched.lock().unwrap().push((start, end));
            self.reads
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(TradesRead::complete(Vec::new())))
        }
    }

    /// Initialise a manager over `client`, with a 1 ms backoff.
    async fn init_flaky(
        client: FlakyAccountClient,
    ) -> Result<
        ExecutionManager<
            futures::stream::Pending<ExecutionRequest<ExchangeIndex, InstrumentIndex>>,
            FlakyAccountClient,
        >,
        ExecutionError,
    > {
        init_flaky_recovering(client, Clock::default()).await
    }

    /// As [`init_flaky`], with `clock` for the re-initialisations' reads.
    async fn init_flaky_recovering(
        client: FlakyAccountClient,
        clock: Clock,
    ) -> Result<
        ExecutionManager<
            futures::stream::Pending<ExecutionRequest<ExchangeIndex, InstrumentIndex>>,
            FlakyAccountClient,
        >,
        ExecutionError,
    > {
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
            panic!("the instrument map should build");
        };
        ExecutionManager::init(
            futures::stream::pending(),
            std::time::Duration::from_secs(5),
            Arc::new(client),
            AccountEventIndexer::new(Arc::new(map)),
            AccountStreamOptions::default()
                .with_reconnect(ReconnectionBackoffPolicy::new(1, 1, 1))
                .with_clock(clock),
        )
        .await
        .map(|(manager, _events)| manager)
    }

    /// Each failed re-init reaches the consumer in order, as a `ReinitFailed` counting
    /// consecutive failures, with no `Reconnecting` beyond the one that marked the disconnect.
    /// The count restarts after a stream initialises.
    #[tokio::test]
    async fn each_failed_account_stream_reinit_is_sent_in_order() {
        let Ok(manager) = init_flaky(FlakyAccountClient {
            fail_on: &[1, 2, 4],
            open_from: 5,
            ..FlakyAccountClient::default()
        })
        .await
        else {
            panic!("the first attempt succeeds, so init should too");
        };
        let exchange = manager.indexer.map.exchange.key;

        let events = manager.account_stream.take(8).collect::<Vec<_>>().await;

        let snapshot = || {
            AccountStreamEvent::Item(AccountEvent {
                exchange,
                kind: AccountEventKind::Snapshot(AccountSnapshot::new(exchange, vec![], vec![])),
            })
        };
        let reinit_failed = |attempt| {
            AccountStreamEvent::Item(AccountEvent {
                exchange,
                kind: AccountEventKind::ReinitFailed(AccountReinitFailure::new(
                    attempt,
                    AccountStreamInitError::Client(ClientError::Connectivity(
                        ConnectivityError::Timeout,
                    )),
                )),
            })
        };
        assert_eq!(
            events,
            [
                snapshot(),
                AccountStreamEvent::Reconnecting(EXCHANGE),
                reinit_failed(1),
                reinit_failed(2),
                snapshot(),
                AccountStreamEvent::Reconnecting(EXCHANGE),
                reinit_failed(1),
                snapshot(),
            ]
        );
    }

    fn missed_trade(id: &str, secs: i64) -> Trade<AssetNameExchange, InstrumentNameExchange> {
        Trade::new(
            TradeId::new(id),
            OrderId::new("o"),
            InstrumentNameExchange::new("btc_usdt"),
            StrategyId::unknown(),
            DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap(),
            Side::Buy,
            dec!(1),
            dec!(1),
            None,
            AssetFees::new(AssetNameExchange::from("usdt"), dec!(0), None),
        )
    }

    fn live_trade(id: &str, secs: i64) -> UnindexedAccountEvent {
        UnindexedAccountEvent {
            exchange: EXCHANGE,
            kind: AccountEventKind::Trade(missed_trade(id, secs)),
        }
    }

    /// One word per event: what a test about order and content needs to see.
    fn describe(event: &AccountStreamEvent) -> String {
        match event {
            AccountStreamEvent::Reconnecting(_) => "reconnecting".to_owned(),
            AccountStreamEvent::Item(event) => match &event.kind {
                AccountEventKind::Snapshot(_) => "snapshot".to_owned(),
                AccountEventKind::Trade(trade) => format!("trade {}", trade.id.0),
                AccountEventKind::FillRecoveryGaveUp(gap) => format!("gap {}", gap.reason),
                other => format!("{other:?}"),
            },
        }
    }

    fn at_clock(secs: i64) -> Clock {
        Clock::new(move || DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap())
    }

    /// The fills read for the span a stream was down come before the snapshot that follows them,
    /// the read starts at the watermark less the overlap and runs to now, and a fill the new
    /// stream sends again, read already, is delivered once.
    #[tokio::test]
    async fn a_reopened_stream_delivers_the_missed_fills_before_its_snapshot() {
        let client = FlakyAccountClient {
            open_from: 1,
            live: vec![live_trade("b", 20), live_trade("c", 30)],
            reads: Arc::new(std::sync::Mutex::new(
                [Ok(TradesRead::complete(vec![
                    missed_trade("b", 20),
                    missed_trade("a", 10),
                ]))]
                .into(),
            )),
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, at_clock(100)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(6).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            [
                "snapshot",
                "reconnecting",
                "trade a",
                "trade b",
                "snapshot",
                "trade c",
            ]
        );
        let at = |secs: i64| DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap();
        // The first open reads nothing; the re-init reads from the watermark less 2 s to now.
        assert_eq!(*fetched.lock().unwrap(), [(at(98), at(100))]);
    }

    /// A read that fails does not stop the stream opening: its span is reported and the snapshot
    /// follows.
    #[tokio::test]
    async fn a_failed_read_is_reported_as_a_gap_and_the_stream_still_opens() {
        let client = FlakyAccountClient {
            open_from: 1,
            reads: Arc::new(std::sync::Mutex::new(
                [Err(UnindexedClientError::Connectivity(
                    ConnectivityError::Timeout,
                ))]
                .into(),
            )),
            ..FlakyAccountClient::default()
        };
        let Ok(manager) = init_flaky_recovering(client, at_clock(100)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(4).collect::<Vec<_>>().await;

        let words = events.iter().map(describe).collect::<Vec<_>>();
        assert_eq!(words.len(), 4);
        assert_eq!(&words[..2], ["snapshot", "reconnecting"]);
        assert!(words[2].starts_with("gap request failed"), "{words:?}");
        assert_eq!(words[3], "snapshot");
    }

    /// A clock that starts at `secs` and moves on 1000 s at each reading.
    fn stepping_clock(secs: i64) -> Clock {
        let readings = Arc::new(std::sync::atomic::AtomicI64::new(0));
        Clock::new(move || {
            let reading = readings.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            DateTime::<Utc>::from_timestamp(1_800_000_000 + secs + 1000 * reading, 0).unwrap()
        })
    }

    /// The watermark moves as events are delivered: after a stream that delivered a fill ends,
    /// the read starts at the moment of that delivery, not at the moment the stream opened.
    #[tokio::test]
    async fn the_read_starts_from_the_last_event_delivered_not_from_the_open() {
        let client = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 0)],
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, stepping_clock(0)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        // snapshot, the fill, the reconnect, then the re-init's own snapshot.
        let events = manager.account_stream.take(4).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "trade x", "reconnecting", "snapshot"]
        );
        let opened = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap();
        let [(start, _)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        assert!(start > opened, "{start} is not after the open at {opened}");
    }

    /// A re-init whose snapshot fails after the read leaves the watermark where it was and marks
    /// none of the fills it read: the next attempt reads from the same start and delivers them.
    #[tokio::test]
    async fn a_failed_reinit_after_the_read_is_read_again_from_the_same_start() {
        let reads = (0..2)
            .map(|_| Ok(TradesRead::complete(vec![missed_trade("a", 10)])))
            .collect();
        let client = FlakyAccountClient {
            open_from: 1,
            snapshot_fail_on: &[1],
            reads: Arc::new(std::sync::Mutex::new(reads)),
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, stepping_clock(0)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        // Bounded, so a fill dropped by the retry fails the test rather than hanging it.
        let events = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.account_stream.take(5).collect::<Vec<_>>(),
        )
        .await
        .expect("the retry delivers the fill and the snapshot");

        let words = events.iter().map(describe).collect::<Vec<_>>();
        assert_eq!(words[0], "snapshot");
        assert_eq!(words[1], "reconnecting");
        assert!(words[2].starts_with("ReinitFailed"), "{words:?}");
        assert_eq!(&words[3..], ["trade a", "snapshot"]);
        let fetched = fetched.lock().unwrap().clone();
        let [(first, _), (second, _)] = fetched[..] else {
            panic!("two reads, got {fetched:?}")
        };
        assert_eq!(first, second);
    }

    /// A re-init the venue refused for a rate limit is not retried before the wait it named,
    /// although the backoff is 1 ms.
    #[tokio::test]
    async fn a_rate_limited_reinit_waits_out_the_venues_wait() {
        let wait = std::time::Duration::from_millis(300);
        let Ok(manager) = init_flaky(FlakyAccountClient {
            fail_on: &[1],
            open_from: 2,
            rate_limited_for: Some(wait),
            ..FlakyAccountClient::default()
        })
        .await
        else {
            panic!("the first attempt succeeds, so init should too");
        };
        let mut events = manager.account_stream;
        let mut next = async || {
            let Ok(Some(event)) =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.next()).await
            else {
                panic!("the stream yields within the test's budget, and does not end");
            };
            event
        };

        assert!(matches!(
            next().await,
            AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::Snapshot(_),
                ..
            })
        ));
        assert!(matches!(next().await, AccountStreamEvent::Reconnecting(_)));
        // Before the refused attempt is made, so the wait it starts cannot begin earlier.
        let refused = std::time::Instant::now();
        match next().await {
            AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::ReinitFailed(failure),
                ..
            }) => assert_eq!(failure.error.retry_after(), Some(wait)),
            other => panic!("expected the refused re-init, got {other:?}"),
        }
        assert!(matches!(
            next().await,
            AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::Snapshot(_),
                ..
            })
        ));
        assert!(
            refused.elapsed() >= wait,
            "re-initialised after {:?}, before the venue's {wait:?}",
            refused.elapsed()
        );
    }

    /// A failure of the first attempt is returned from `init`, not sent on the stream.
    #[tokio::test]
    async fn a_failed_first_account_stream_init_fails_init() {
        let result = init_flaky(FlakyAccountClient {
            fail_on: &[0],
            ..FlakyAccountClient::default()
        })
        .await;

        assert_eq!(
            result.err(),
            Some(ExecutionError::AccountStreamInit(
                AccountStreamInitError::Client(ClientError::Connectivity(
                    ConnectivityError::Timeout
                ))
            ))
        );
    }

    /// A response whose own key resolves, but to a different order, is a client bug too: it must
    /// reach the Engine under the request's key, not settle the order it names.
    #[tokio::test]
    async fn a_response_keyed_to_another_order_still_answers_its_request() {
        let client = RejectingClient {
            answer_cid: Some(ClientOrderId::random()),
            ..RejectingClient::default()
        };
        let (key, events) = run_open_and_cancel(client).await;

        let rejection = OrderError::Rejected(ApiError::BalanceInsufficient(
            None,
            "insufficient balance".to_string(),
        ));
        assert_both_answered(&key, &events, &rejection);
    }

    /// A mock venue's account stream that ends on broadcast lag loses the events in its buffer.
    /// On a simulated clock the fill among them is read back, before the new snapshot, because
    /// the read spans the simulation's time, the time the venue stamps its fills with.
    #[tokio::test]
    async fn a_lagged_mock_stream_reads_back_its_lost_fill_on_the_simulated_clock() {
        use rustrade_execution::{
            client::mock::MockExecution,
            exchange::mock::request::{MockExchangeRequest, MockExchangeRequestKind},
        };
        use std::sync::atomic::{AtomicI64, Ordering};

        let sim_secs = Arc::new(AtomicI64::new(0));
        let sim_now = {
            let sim_secs = Arc::clone(&sim_secs);
            move || {
                DateTime::<Utc>::from_timestamp(1_800_000_000 + sim_secs.load(Ordering::Relaxed), 0)
                    .unwrap()
            }
        };
        // Capacity 1, so three events sent while the stream is not read lag it.
        let (event_tx, event_rx) = tokio::sync::broadcast::channel(1);
        let (request_tx, mut request_rx) = tokio::sync::mpsc::unbounded_channel();
        let client = MockExecution::new(ExchangeId::Mock, sim_now.clone(), request_tx, event_rx);

        // The venue: an empty snapshot, and its one fill when a read's span holds its time.
        let fill = missed_trade("a", 10);
        let venue = tokio::spawn({
            let fill = fill.clone();
            async move {
                while let Some(MockExchangeRequest { kind, .. }) = request_rx.recv().await {
                    match kind {
                        MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                            let _ = response_tx.send(UnindexedAccountSnapshot::new(
                                ExchangeId::Mock,
                                vec![],
                                vec![],
                            ));
                        }
                        MockExchangeRequestKind::FetchTrades {
                            response_tx,
                            start,
                            end,
                        } => {
                            let held = (start..=end).contains(&fill.time_exchange);
                            let _ =
                                response_tx.send(held.then(|| fill.clone()).into_iter().collect());
                        }
                        other => panic!("unexpected request {other:?}"),
                    }
                }
            }
        });

        let instruments = IndexedInstruments::new([instrument(ExchangeId::Mock, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, ExchangeId::Mock) else {
            panic!("the instrument map should build");
        };
        let Ok((manager, _responses)) = ExecutionManager::init(
            futures::stream::pending::<ExecutionRequest<ExchangeIndex, InstrumentIndex>>(),
            std::time::Duration::from_secs(5),
            Arc::new(client),
            AccountEventIndexer::new(Arc::new(map)),
            AccountStreamOptions::default()
                .with_reconnect(ReconnectionBackoffPolicy::new(1, 1, 1))
                .with_clock(Clock::new(sim_now)),
        )
        .await
        else {
            panic!("the first open should succeed");
        };
        let mut events = manager.account_stream;
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), events.next())
            .await
            .expect("the first snapshot arrives");
        assert_eq!(describe(&first.unwrap()), "snapshot");

        // The fill and two more, sent while the stream is not read: the buffer laps.
        sim_secs.store(10, Ordering::Relaxed);
        for _ in 0..3 {
            event_tx
                .send(UnindexedAccountEvent {
                    exchange: ExchangeId::Mock,
                    kind: AccountEventKind::Trade(fill.clone()),
                })
                .unwrap();
        }
        sim_secs.store(20, Ordering::Relaxed);

        // Each wait bounded, so a fill the read misses fails the test rather than hanging it.
        let mut rest = Vec::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(std::time::Duration::from_secs(1), events.next()).await
        {
            rest.push(describe(&event));
            if rest.last().is_some_and(|last| last == "snapshot") {
                break;
            }
        }
        assert_eq!(
            rest,
            [
                "StreamTerminated(Error(\"mock broadcast stream lagged\"))",
                "reconnecting",
                "trade a",
                "snapshot"
            ]
        );
        venue.abort();
    }

    #[test]
    fn account_stream_options_default_to_the_stream_policy_and_the_default_recovery() {
        let options = AccountStreamOptions::default();
        assert_eq!(options.reconnect, STREAM_RECONNECTION_POLICY);
        assert_eq!(options.fill_recovery, FillRecoveryConfig::default());
    }

    #[test]
    fn account_stream_options_setters_set_their_field() {
        let reconnect = ReconnectionBackoffPolicy::new(7, 3, 9);
        let fill_recovery = FillRecoveryConfig::default().with_max_pages(3);
        let options = AccountStreamOptions::default()
            .with_reconnect(reconnect.clone())
            .with_fill_recovery(fill_recovery)
            .with_clock(at_clock(5));
        assert_eq!(options.reconnect, reconnect);
        assert_eq!(options.fill_recovery, fill_recovery);
        assert_eq!(options.clock.now(), at_clock(5).now());
    }

    /// Index `events` through `index_account_stream`, returning how many events came out and how
    /// many `WARN`, `DEBUG` and `ERROR` events it logged: `(indexed, warn, debug, error)`.
    fn index_and_count_logs(
        indexer: AccountEventIndexer,
        events: Vec<UnindexedAccountEvent>,
    ) -> (usize, usize, usize, usize) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default, Clone)]
        struct Counts([Arc<AtomicUsize>; 3]);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Counts {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let slot = match *event.metadata().level() {
                    tracing::Level::WARN => 0,
                    tracing::Level::DEBUG => 1,
                    tracing::Level::ERROR => 2,
                    _ => return,
                };
                self.0[slot].fetch_add(1, Ordering::Relaxed);
            }
        }

        let counts = Counts::default();
        // Thread-local, so tests running in parallel are not counted.
        let indexed = tracing::subscriber::with_default(
            tracing_subscriber::registry().with(counts.clone()),
            || {
                futures::executor::block_on(
                    ExecutionManager::<
                        futures::stream::Pending<ExecutionRequest<ExchangeIndex, InstrumentIndex>>,
                        RejectingClient,
                    >::index_account_stream(
                        futures::stream::iter(events), indexer
                    )
                    .count(),
                )
            },
        );
        let [warn, debug, error] = counts.0.each_ref().map(|n| n.load(Ordering::Relaxed));
        (indexed, warn, debug, error)
    }

    #[test]
    fn a_balance_for_an_unregistered_asset_warns_once_per_asset_then_logs_at_debug() {
        use rustrade_execution::{
            balance::{AssetBalanceUpdate, Balance, BalanceUpdate},
            cash_flow::{CashFlow, CashFlowKind},
        };

        let instruments = IndexedInstruments::new(vec![instrument(EXCHANGE, "btc", "usdt")]);
        let map = generate_execution_instrument_map(&instruments, EXCHANGE).unwrap();
        let indexer = AccountEventIndexer::new(Arc::new(map));
        let update = |asset: &str| {
            UnindexedAccountEvent::new(
                EXCHANGE,
                AccountEventKind::BalanceStreamUpdate(Snapshot(AssetBalanceUpdate::new(
                    AssetNameExchange::new(asset),
                    BalanceUpdate::new(dec!(1), dec!(0)),
                    DateTime::<Utc>::MIN_UTC,
                ))),
            )
        };

        let snapshot = UnindexedAccountEvent::new(
            EXCHANGE,
            AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance::new(
                AssetNameExchange::new("sol"),
                Balance::new(dec!(1), dec!(1)),
                DateTime::<Utc>::MIN_UTC,
            ))),
        );
        // A cash flow in an unregistered asset loses ledger data, so it stays an error.
        let cash_flow = UnindexedAccountEvent::new(
            EXCHANGE,
            AccountEventKind::CashFlow(CashFlow::new(
                CashFlowKind::Rebate,
                AssetNameExchange::new("bnb"),
                dec!(1),
                None,
                DateTime::<Utc>::MIN_UTC,
                None,
            )),
        );

        let (indexed, warn, debug, error) = index_and_count_logs(
            indexer,
            vec![
                update("bnb"),
                update("bnb"),
                update("usdt"),
                update("bnb"),
                update("eth"),
                snapshot,
                cash_flow,
            ],
        );

        assert_eq!(indexed, 1, "only the registered asset's balance is kept");
        assert_eq!(warn, 3, "one warning for each of bnb, eth and sol");
        assert_eq!(debug, 2, "the repeats of bnb are logged at debug");
        assert_eq!(error, 1, "the cash flow is still an error");
    }

    #[test]
    fn the_clock_has_a_debug_form() {
        assert_eq!(format!("{:?}", Clock::default()), "Clock(..)");
    }
}
