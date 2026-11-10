use crate::execution::{
    AccountStreamEvent,
    backfill::{FillRecoveryConfig, Watermark, backfill},
    error::ExecutionError,
    request::{ExecutionRequest, RequestFuture, dropped_open},
    unknown_outcome::{
        AmendLookupOutcome, LookupOutcome, UnknownOutcomeLookup, look_up_unknown_amend,
        look_up_unknown_outcome,
    },
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::{FnvHashMap, FnvHashSet};
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
    AccountEvent, AccountEventKind, UnindexedAccountEvent, UnindexedAccountSnapshot,
    client::{ExecutionClient, OrderStatusClient, dedup::AccountEventDeduper},
    error::{
        AccountReinitFailure, AccountStreamInitError, AmendRefusal, ConnectivityError,
        NotPlacedReason, OrderError,
    },
    indexer::{AccountEventIndexer, AccountIndexError},
    map::ExecutionInstrumentMap,
    order::{
        Order, OrderEvent, OrderKey, UnindexedOrderKey,
        amend::Amended,
        id::ClientOrderId,
        request::{
            CancelReplaceAnswer, CancelThenOpen, OnCancelFailure, OrderRequestAmend,
            OrderRequestCancel, OrderRequestOpen, OrderResponseAmend, OrderResponseCancel,
            UnindexedOrderResponseAmend, UnindexedOrderResponseCancel,
        },
        state::{OrderState, UnindexedOrderState},
    },
};
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::error::IndexError,
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
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// How an [`ExecutionManager`] keeps its account stream going: how it reconnects, how it reads the
/// fills missed while the stream was down, and the clock it measures that on. Also how it settles
/// an open request whose outcome is unknown.
///
/// Set per venue, through [`ExecutionManager::init`] or
/// [`ExecutionBuilder::add_live_with`](super::builder::ExecutionBuilder::add_live_with), since
/// venues differ in how many fills one call returns and how long they keep them, and in how late
/// an order they accepted can show up. Built from
/// [`Default`] and the `with_*` methods, so fields can be added without a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AccountStreamOptions {
    /// The backoff between attempts to re-initialise an account stream that ended.
    pub reconnect: ReconnectionBackoffPolicy,
    /// The budget for reading the fills missed while the stream was down.
    pub fill_recovery: FillRecoveryConfig,
    /// Where "now" comes from: the local time each event arrived and each read is made. A read
    /// starts no later than this clock's time at the last event delivered, and ends no earlier
    /// than its time at the read, whatever the venue's clock is estimated to read. On a simulated
    /// venue it must be the simulation's: the estimate counts on from the venue's last fill time
    /// on tokio's clock, which a simulation does not drive, so it can warn of a clock difference
    /// that is the simulation's pace, while this clock keeps the read covering the simulated
    /// span.
    pub clock: Clock,
    /// How an open request whose outcome is unknown is settled, by looking the order up at the
    /// venue.
    pub unknown_outcome: UnknownOutcomeLookup,
}

impl Default for AccountStreamOptions {
    /// [`STREAM_RECONNECTION_POLICY`], [`FillRecoveryConfig::default`] and the system clock.
    fn default() -> Self {
        Self {
            reconnect: STREAM_RECONNECTION_POLICY,
            fill_recovery: FillRecoveryConfig::default(),
            clock: Clock::default(),
            unknown_outcome: UnknownOutcomeLookup::default(),
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

    /// Sets [`unknown_outcome`](Self::unknown_outcome).
    #[must_use]
    pub fn with_unknown_outcome(self, unknown_outcome: UnknownOutcomeLookup) -> Self {
        Self {
            unknown_outcome,
            ..self
        }
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
/// - Sequences each [`ExecutionRequest::CancelThenOpen`] group: sends its cancels, holds its opens
///   until every cancel has been answered, then sends them, or answers each as dropped when a
///   cancel failed and the group says so. A group of one cancel and one open goes instead as one
///   venue request when the client offers one for it ([`ExecutionClient::cancel_replace`]), and
///   each leg is answered from it. Every other request is sent as it arrives, concurrently.
/// - Settles an open whose answer leaves its outcome unknown (a timeout, or a connectivity error:
///   see [`OrderError::is_outcome_unknown`]) by looking the order up at the venue with
///   [`OrderStatusClient::fetch_order_status`], as its
///   [`UnknownOutcomeLookup`] says, and forwards
///   what it finds as one more order snapshot under the request's key: the order live, how it
///   ended, or [`NotPlacedReason::UnknownToVenue`]. Lookups that run out forward nothing more.
///
/// The client must implement [`OrderStatusClient`] for that lookup.
#[derive(Constructor)]
pub struct ExecutionManager<RequestStream, Client> {
    /// `Stream` of incoming Engine [`ExecutionRequest`]s.
    pub request_stream: RequestStream,

    /// Maximum `Duration` to wait for execution request responses from the [`ExecutionClient`].
    pub request_timeout: std::time::Duration,

    /// How an open request whose outcome is unknown is settled.
    pub unknown_outcome: UnknownOutcomeLookup,

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
            .field("unknown_outcome", &self.unknown_outcome)
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
    Client: ExecutionClient + OrderStatusClient + Send + Sync + 'static,
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
    /// The read is a span of the venue's clock, which can differ from this host's, so it is
    /// placed on the venue's clock: estimated from the times the venue stamped on the fills
    /// delivered, and counted on from there on tokio's monotonic clock. A step of this host's
    /// wall clock, before the stream ended or while it was down, does not move it. A difference
    /// between the two clocks beyond `options.fill_recovery.max_clock_skew` is logged as a
    /// warning at each re-initialisation. Until a fill has been delivered the venue's clock is
    /// unknown, and the read allows for that much difference instead.
    ///
    /// Fills are deduplicated by instrument and trade id, across the read and the new stream and
    /// across re-initialisations, with the 10,000 most recent kept. An order that ended without a
    /// fill in the gap is the snapshot's to show, as before.
    ///
    /// Each open's events then pass through `client`'s
    /// [`ExecutionClient::process_account_events`], in order, before they are indexed: the read's
    /// fills and gaps, the snapshot and the stream's updates. Its rustdoc says what the manager
    /// does around it.
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
            unknown_outcome,
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

        // Initialise the reconnecting, indexed account stream (snapshot + updates)
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
                let opened = (clock.now(), Instant::now());

                // What this open reads from: `None` on the first open. Taken before the stream
                // opens, so the span runs from the last event delivered to the moment it reads.
                let last = *watermark.lock();

                // Initialise the AccountStream, deduplicated. Opened before the read, so a fill
                // made during it arrives on the stream.
                let updates =
                    Self::init_account_stream(&client, &indexer, &deduper, &assets, &instruments)
                        .await?;

                // Fills missed while the stream was down, before the snapshot that follows them.
                let missed = match last {
                    Some(last) => {
                        let (now, instant) = (clock.now(), Instant::now());
                        if let Some(lead) = last.skew_beyond_allowance(now, instant, &fill_recovery)
                        {
                            warn!(
                                exchange_id = %indexer.map.exchange.value,
                                host_lead_ms = lead.num_milliseconds(),
                                max_clock_skew = ?fill_recovery.max_clock_skew,
                                "AccountStream fill recovery: this host's clock differs from \
                                 the venue's by more than the allowance, or the last fill \
                                 sampled was delivered late (a replay); the read is placed on \
                                 the venue's clock"
                            );
                        }
                        let read = backfill(
                            |from, to| client.fetch_trades(from, to, &instruments),
                            &instruments,
                            last.read_from(&fill_recovery),
                            last.read_to(now, instant, &fill_recovery),
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
                let missed = futures::stream::iter(missed).filter({
                    let deduper = deduper.clone();
                    move |event| std::future::ready(!deduper.is_duplicate(event))
                });

                // Fetch the AccountSnapshot, checked to index, so one that cannot fails the open
                let snapshot =
                    Self::fetch_account_snapshot(&client, &indexer, &assets, &instruments).await?;

                // Nothing delivered counts until it is delivered: the watermark moves as events
                // pass, so an attempt that fails above leaves it where it was.
                watermark
                    .lock()
                    .get_or_insert(Watermark::opened(opened.0, opened.1));

                // The watermark is sampled before the client's stage: what the venue sent, as
                // it arrived. A fill the stage drops or holds does not widen the next read, and
                // the stage's own delay does not skew the estimate of the venue's clock.
                let received = missed
                    .chain(futures::stream::once(std::future::ready(snapshot)))
                    .chain(updates)
                    .inspect(move |event| {
                        let (now, instant) = (clock.now(), Instant::now());
                        if let Some(watermark) = watermark.lock().as_mut() {
                            watermark.delivered(now, instant, event, &fill_recovery);
                        }
                    })
                    .boxed();

                // Every fill the stage passes on is recorded as seen, none dropped: one it added
                // that the venue sends later is then delivered once.
                let processed = client.process_account_events(received).inspect({
                    let deduper = deduper.clone();
                    move |event| deduper.record(event)
                });

                // It's expected downstream consumers (eg/ EngineState will sync updates)
                // Boxed: the chain is named by every layer above it, once per client type.
                Ok(Self::index_account_stream(processed, indexer).boxed())
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
                unknown_outcome,
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

    /// The account snapshot, unindexed for the client's stage, once it is known to index: one that
    /// cannot fails the open, which is retried.
    async fn fetch_account_snapshot(
        client: &Arc<Client>,
        indexer: &AccountEventIndexer,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountEvent, AccountStreamInitError> {
        match client.account_snapshot(assets, instruments).await {
            Ok(snapshot) => {
                // A copy, as the snapshot is indexed again after the stage: once per open.
                indexer.snapshot_partitioned(snapshot.clone())?;
                Ok(UnindexedAccountEvent {
                    exchange: indexer.map.exchange.value,
                    kind: AccountEventKind::Snapshot(snapshot),
                })
            }
            Err(error) => Err(AccountStreamInitError::Client(indexer.client_error(error))),
        }
    }

    /// Indexes an account snapshot, dropping and logging the balances of assets the map does not
    /// register.
    fn index_snapshot(
        indexer: &AccountEventIndexer,
        snapshot: UnindexedAccountSnapshot,
    ) -> Result<AccountEvent, IndexError> {
        let partitioned = indexer.snapshot_partitioned(snapshot)?;
        let (indexed_snapshot, untracked) = (partitioned.snapshot, partitioned.untracked);
        let untracked: Vec<&str> = untracked
            .iter()
            .map(|asset| asset.name().as_str())
            .collect();
        if !untracked.is_empty() {
            if indexed_snapshot.balances.is_empty() {
                // Every balance was dropped: more likely the asset names do not match the
                // venue's than an account holding only unregistered assets, though that is
                // possible too.
                error!(
                    ?untracked,
                    "Account snapshot kept no balance: none of its assets is registered in the \
                     ExecutionInstrumentMap"
                );
            } else {
                warn!(
                    ?untracked,
                    "Account snapshot balances dropped for assets the ExecutionInstrumentMap \
                     does not register"
                );
            }
        }
        Ok(AccountEvent {
            exchange: indexer.map.exchange.key,
            kind: AccountEventKind::Snapshot(indexed_snapshot),
        })
    }

    async fn init_account_stream(
        client: &Arc<Client>,
        indexer: &AccountEventIndexer,
        deduper: &AccountEventDeduper,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<
        impl Stream<Item = UnindexedAccountEvent> + use<RequestStream, Client>,
        AccountStreamInitError,
    > {
        let stream = match client.account_stream(assets, instruments).await {
            Ok(stream) => stream,
            Err(error) => {
                return Err(AccountStreamInitError::Client(indexer.client_error(error)));
            }
        };

        // Deduplicated before the client's stage and indexing: the deduper keys on the venue's
        // own names, and a fill the stream repeats from the read, or from an earlier stream, is
        // dropped here.
        let deduper = deduper.clone();
        Ok(stream.filter(move |event| std::future::ready(!deduper.is_duplicate(event))))
    }

    /// Index an unindexed account stream, logging and dropping each event the indexer rejects.
    ///
    /// A balance for an asset the map does not register is not a rejection: the first one per
    /// asset is a `warn!`, naming the asset, and later ones are `debug!`. The set of warned assets
    /// belongs to the stream, so a reconnected stream warns again. In a snapshot, such balances
    /// are dropped and the snapshot kept, as [`index_snapshot`](Self::index_snapshot) logs.
    fn index_account_stream(
        stream: impl Stream<Item = UnindexedAccountEvent>,
        indexer: AccountEventIndexer,
    ) -> impl Stream<Item = AccountEvent> {
        let mut warned = FnvHashSet::<AssetNameExchange>::default();
        stream.filter_map(move |event| {
            let indexed = match event {
                UnindexedAccountEvent {
                    kind: AccountEventKind::Snapshot(snapshot),
                    ..
                } => Self::index_snapshot(&indexer, snapshot).map_err(AccountIndexError::Rejected),
                event => indexer.index_account_event(event),
            };
            std::future::ready(match indexed {
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
    ///    forwarded — these are bounded by `request_timeout` — and so is every lookup of an open
    ///    whose outcome came back unknown, which its `UnknownOutcomeLookup` bounds, so this cannot
    ///    hang. An open held behind the cancels of a `CancelThenOpen` group is sent, or dropped,
    ///    once they are answered, and then awaited like the rest;
    /// 2. the account tail — every event the exchange has already made available — is drained;
    /// 3. only then does the loop end, dropping `response_tx` and ending the `Stream` returned by
    ///    [`init`](Self::init).
    ///
    /// That last drop is what a caller observes as "this manager is finished", so performing it
    /// after the drain rather than before is what stops a shutdown from truncating the ledgers.
    // One `select!` over independent arms, each handing its work to a method. The lint counts
    // the macro's expansion, so splitting the loop further would only scatter its control flow.
    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) {
        let mut in_flight_cancels = FuturesUnordered::new();
        let mut in_flight_opens = FuturesUnordered::new();
        // Lookups of the orders whose open came back with its outcome unknown.
        let mut in_flight_lookups = FuturesUnordered::new();
        // The opens of each `CancelThenOpen` group whose cancels are not all answered yet.
        let mut held_opens = HeldGroups::default();
        // The `CancelThenOpen` groups sent as one venue request each.
        let mut in_flight_cancel_replaces = FuturesUnordered::new();
        let mut in_flight_amends = FuturesUnordered::new();
        // Lookups of the orders whose amend came back with its outcome unknown.
        let mut in_flight_amend_lookups = FuturesUnordered::new();

        // Set once a Shutdown (or a closed request Stream) has been observed. New requests are no
        // longer accepted, but the loop keeps running until nothing is outstanding.
        let mut draining = false;

        // Set if the AccountStream ever ends. It reconnects indefinitely, so in practice this only
        // happens once the manager itself has torn the client down.
        let mut account_stream_done = false;

        loop {
            if draining
                && in_flight_cancels.is_empty()
                && in_flight_opens.is_empty()
                && in_flight_cancel_replaces.is_empty()
                && in_flight_lookups.is_empty()
                && in_flight_amends.is_empty()
                && in_flight_amend_lookups.is_empty()
            {
                // A group is held only while one of its cancels is in flight: its cancels are sent
                // when it is held, and the answer to its last one releases it. So no in-flight
                // cancel means no held group, and waiting on one here could only hang.
                debug_assert!(
                    held_opens.held.is_empty(),
                    "a CancelThenOpen group is held with none of its cancels in flight: {held_opens:?}"
                );
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

            let next_cancel_replace = if in_flight_cancel_replaces.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_cancel_replaces.select_next_some())
            };

            let next_lookup = if in_flight_lookups.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_lookups.select_next_some())
            };

            let next_amend_response = if in_flight_amends.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_amends.select_next_some())
            };

            let next_amend_lookup = if in_flight_amend_lookups.is_empty() {
                Either::Left(std::future::pending())
            } else {
                Either::Right(in_flight_amend_lookups.select_next_some())
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
                        in_flight_cancels.push(Self::send_cancel(
                            &self.client, &self.indexer, self.request_timeout, request, None,
                        ));
                    },
                    Some(ExecutionRequest::Open(request)) => {
                        in_flight_opens.push(Self::send_open(
                            &self.client, &self.indexer, self.request_timeout, request,
                        ));
                    }
                    Some(ExecutionRequest::Amend(request)) => {
                        match Self::send_amend(
                            &self.client, &self.indexer, self.request_timeout, request,
                        ) {
                            Ok(amend) => in_flight_amends.push(amend),
                            // Refused here, unsent: the client does not declare it.
                            Err(refused) => {
                                if self.response_tx.send(*refused).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(ExecutionRequest::CancelThenOpen(group)) => {
                        // Index every open now, so a misconfigured key panics on the request that
                        // carried it rather than once its cancels are answered.
                        group.opens.iter().for_each(|open| {
                            Self::index_request(&self.indexer, open, "open");
                        });
                        let group = match Self::send_cancel_replace(
                            &self.client, &self.indexer, self.request_timeout, group,
                        ) {
                            Ok(cancel_replace) => {
                                in_flight_cancel_replaces.push(cancel_replace);
                                continue;
                            }
                            Err(group) => group,
                        };
                        match held_opens.hold(group) {
                            Hold::Cancels { group, cancels } => {
                                in_flight_cancels.extend(cancels.into_iter().map(|cancel| {
                                    Self::send_cancel(
                                        &self.client,
                                        &self.indexer,
                                        self.request_timeout,
                                        cancel,
                                        Some(group),
                                    )
                                }));
                            }
                            Hold::Opens(opens) => {
                                in_flight_opens.extend(opens.into_iter().map(|open| {
                                    Self::send_open(
                                        &self.client, &self.indexer, self.request_timeout, open,
                                    )
                                }));
                            }
                        }
                    }
                },

                // Process next ExecutionRequest::Cancel response
                ((request, group), response) = next_cancel_response => {
                    // Any error fails the cancel, a timeout included: see `OnCancelFailure`.
                    let failed = !matches!(&response, Ok(response) if response.state.is_ok());
                    let cid = request.key.cid.clone();
                    let event = match response {
                        Ok(response) => self.process_cancel_response(request, response),
                        Err(_elapsed) => Self::process_cancel_timeout(request),
                    };

                    if self.response_tx.send(event).is_err() {
                        break;
                    }

                    // The cancel's answer goes first, so the opens it released, or the reports of
                    // the opens it dropped, follow it.
                    let Some(group) = group else { continue };
                    let Some(opens) = self.release(held_opens.answer(group, cid, failed)) else {
                        break;
                    };
                    in_flight_opens.extend(opens.into_iter().map(|open| {
                        Self::send_open(&self.client, &self.indexer, self.request_timeout, open)
                    }));
                },

                // Process the answer to a group sent as one venue request
                ((cancel, open, sent), answer) = next_cancel_replace => {
                    let (cancel_event, open_answer) =
                        self.process_cancel_replace_answer(cancel, open, answer);
                    let (open_event, lookup) = match open_answer {
                        NativeOpen::Answered(open, response) => {
                            self.process_open_answer(open, sent, response)
                        }
                        NativeOpen::Dropped(dropped) => (dropped, None),
                    };
                    // The cancel's answer goes first, as when the group is sequenced here.
                    let mut events = [cancel_event, open_event].into_iter();
                    if events.any(|event| self.response_tx.send(event).is_err()) {
                        break;
                    }
                    in_flight_lookups.extend(lookup);
                }

                // Process next ExecutionRequest::Open response
                ((request, sent), response) = next_open_response => {
                    let (event, lookup) = self.process_open_answer(request, sent, response);

                    if self.response_tx.send(event).is_err() {
                        break;
                    }

                    in_flight_lookups.extend(lookup);
                }

                // Process the next lookup of an order whose open outcome was unknown
                (request, outcome) = next_lookup => {
                    if let Some(event) = self.process_lookup_outcome(request, outcome)
                        && self.response_tx.send(event).is_err()
                    {
                        break;
                    }
                }

                // Process next ExecutionRequest::Amend response
                ((request, sent), response) = next_amend_response => {
                    let (event, lookup) = self.process_amend_answer(request, sent, response);

                    if self.response_tx.send(event).is_err() {
                        break;
                    }

                    in_flight_amend_lookups.extend(lookup);
                }

                // Process the next lookup of an order whose amend outcome was unknown
                (request, outcome) = next_amend_lookup => {
                    if let Some(event) = self.process_amend_lookup_outcome(request, outcome)
                        && self.response_tx.send(event).is_err()
                    {
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

    /// Index `request` for the client.
    ///
    /// # Panics
    /// If its key is not configured for this manager: the system is set up incorrectly, so it is
    /// foolish to continue.
    ///
    /// Takes the indexer rather than `&self`, as do [`send_cancel`](Self::send_cancel) and
    /// [`send_open`](Self::send_open), so the futures they return borrow only the fields they use
    /// while [`run`](Self::run) polls the streams the others hold.
    fn index_request<'a, Kind>(
        indexer: &'a AccountEventIndexer,
        request: &OrderEvent<Kind, ExchangeIndex, InstrumentIndex>,
        kind: &'static str,
    ) -> OrderEvent<Kind, ExchangeId, &'a InstrumentNameExchange>
    where
        Kind: Clone,
    {
        indexer.order_request(request).unwrap_or_else(|error| {
            panic!("ExecutionManager received {kind} request for non-configured key: {error}")
        })
    }

    /// Send `request` to the client, bounded by `request_timeout`, tagged with the
    /// `CancelThenOpen` group it belongs to, if any.
    ///
    /// # Panics
    /// See [`index_request`](Self::index_request).
    fn send_cancel<'a>(
        client: &'a Client,
        indexer: &'a AccountEventIndexer,
        timeout: std::time::Duration,
        request: OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
        group: Option<u64>,
    ) -> RequestFuture<
        (
            OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
            Option<u64>,
        ),
        impl Future<Output = UnindexedOrderResponseCancel> + 'a,
    > {
        let client_request = Self::index_request(indexer, &request, "cancel");
        RequestFuture::new(
            client.cancel_order(client_request),
            timeout,
            (request, group),
        )
    }

    /// Send `request` to the client, bounded by `request_timeout`, with the instant it was sent,
    /// for the lookup an unknown outcome starts.
    ///
    /// # Panics
    /// See [`index_request`](Self::index_request).
    fn send_open<'a>(
        client: &'a Client,
        indexer: &'a AccountEventIndexer,
        timeout: std::time::Duration,
        request: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
    ) -> RequestFuture<
        (OrderRequestOpen<ExchangeIndex, InstrumentIndex>, Instant),
        impl Future<Output = Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> + 'a,
    > {
        let client_request = Self::index_request(indexer, &request, "open");
        RequestFuture::new(
            client.open_order(client_request),
            timeout,
            (request, Instant::now()),
        )
    }

    /// Send `group` as one venue request, bounded by `timeout`, when it is one cancel and one
    /// open and the client offers a request for them ([`ExecutionClient::cancel_replace`]); hand
    /// it back to be sequenced here otherwise. Carries both requests, and the instant they were
    /// sent, for the lookup an unknown outcome of the open starts.
    ///
    /// # Panics
    /// See [`index_request`](Self::index_request).
    #[allow(clippy::type_complexity)] // The tuple names what the answer arm needs; an alias would only hide it.
    fn send_cancel_replace<'a>(
        client: &'a Client,
        indexer: &'a AccountEventIndexer,
        timeout: std::time::Duration,
        group: CancelThenOpen<ExchangeIndex, InstrumentIndex>,
    ) -> Result<
        RequestFuture<
            (
                OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
                OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
                Instant,
            ),
            impl Future<Output = CancelReplaceAnswer> + 'a,
        >,
        CancelThenOpen<ExchangeIndex, InstrumentIndex>,
    > {
        let ([cancel], [open]) = (group.cancels.as_slice(), group.opens.as_slice()) else {
            return Err(group);
        };
        let Some(answer) = client.cancel_replace(
            Self::index_request(indexer, cancel, "cancel"),
            Self::index_request(indexer, open, "open"),
            group.on_cancel_failure,
        ) else {
            return Err(group);
        };
        let CancelThenOpen {
            mut cancels,
            mut opens,
            ..
        } = group;
        let (Some(cancel), Some(open)) = (cancels.pop(), opens.pop()) else {
            unreachable!("a group of exactly one cancel and one open was matched above");
        };
        Ok(RequestFuture::new(
            answer,
            timeout,
            (cancel, open, Instant::now()),
        ))
    }

    /// The opens to send now that a group's cancel has been answered: those `release` sends, and
    /// none before the group's last answer. Each open it drops is answered as dropped first.
    ///
    /// `None` if the channel to the Engine closed while answering a dropped open.
    fn release(
        &self,
        release: Option<Release>,
    ) -> Option<Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>> {
        match release {
            None => Some(Vec::new()),
            Some(Release::Send(opens)) => Some(opens),
            Some(Release::Drop { opens, cancel }) => {
                self.forward_dropped(opens, &cancel).then(Vec::new)
            }
        }
    }

    /// Answer each of `opens` as dropped, because the cancel `cancel` of their group failed.
    ///
    /// Returns `false` if the channel to the Engine has closed.
    fn forward_dropped(
        &self,
        opens: Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
        cancel: &ClientOrderId,
    ) -> bool {
        opens
            .into_iter()
            .all(|open| self.response_tx.send(self.dropped(open, cancel)).is_ok())
    }

    /// The answer to `open` as dropped, because the cancel `cancel` of its group failed.
    fn dropped(
        &self,
        open: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        cancel: &ClientOrderId,
    ) -> AccountStreamEvent {
        warn!(
            exchange = %self.indexer.map.exchange.value,
            instrument = ?open.key.instrument,
            cid = %open.key.cid,
            %cancel,
            "ExecutionManager dropped an open held behind a cancel that failed"
        );
        AccountStreamEvent::Item(AccountEvent {
            exchange: open.key.exchange,
            kind: AccountEventKind::OrderSnapshot(Snapshot(dropped_open(open, cancel.clone()))),
        })
    }

    /// The event that answers the cancel of a group sent as one venue request, from the client's
    /// `answer` or its timeout; and the open's answer, to process as an open sent alone, or the
    /// event that answers it as dropped when the venue did not attempt it.
    ///
    /// A timeout fails the cancel and leaves the open's outcome unknown.
    fn process_cancel_replace_answer(
        &self,
        cancel: OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
        open: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        answer: Result<CancelReplaceAnswer, tokio::time::error::Elapsed>,
    ) -> (AccountStreamEvent, NativeOpen) {
        match answer {
            Ok(CancelReplaceAnswer {
                cancel: response,
                open: None,
            }) => {
                let failed = response.state.is_err();
                let cid = cancel.key.cid.clone();
                let cancel_event = self.process_cancel_response(cancel, response);
                if !failed {
                    // The client broke `cancel_replace`'s contract. By its word the venue placed
                    // nothing, so the open is still answered as not placed.
                    error!(
                        exchange = %self.indexer.map.exchange.value,
                        cid = %open.key.cid,
                        cancel = %cid,
                        "ExecutionManager received a cancel-replace that did not attempt its open although its cancel succeeded - answering the open as dropped"
                    );
                }
                (cancel_event, NativeOpen::Dropped(self.dropped(open, &cid)))
            }
            Ok(CancelReplaceAnswer {
                cancel: response,
                open: Some(opened),
            }) => (
                self.process_cancel_response(cancel, response),
                NativeOpen::Answered(open, Ok(opened)),
            ),
            Err(elapsed) => (
                Self::process_cancel_timeout(cancel),
                NativeOpen::Answered(open, Err(elapsed)),
            ),
        }
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

    /// The event that answers the open `request`, sent at `sent`, from the client's `response`
    /// or its timeout; and, when that answer leaves the outcome unknown, the lookup of the order
    /// to start.
    ///
    /// The answer is forwarded as it is either way: the engine keeps the order in flight on an
    /// unknown outcome, and the lookup's finding settles it.
    fn process_open_answer(
        &self,
        request: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        sent: Instant,
        response: Result<
            Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
            tokio::time::error::Elapsed,
        >,
    ) -> (
        AccountStreamEvent,
        Option<
            impl Future<
                Output = (
                    OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
                    LookupOutcome,
                ),
            > + use<RequestStream, Client>,
        >,
    ) {
        let (event, unknown) = match response {
            Ok(response) => {
                let unknown = response.state.is_outcome_unknown().then(|| request.clone());
                (self.process_open_response(request, response), unknown)
            }
            Err(_elapsed) => (Self::process_open_timeout(request.clone()), Some(request)),
        };

        let lookup = unknown.and_then(|request| {
            let key = self.lookup_key(&request)?;
            if self.unknown_outcome.attempts == 0 {
                warn!(
                    exchange = %key.exchange,
                    instrument = %key.instrument,
                    cid = %key.cid,
                    "ExecutionManager received an open whose outcome is unknown - lookups are off, so it stays in flight"
                );
                return None;
            }
            warn!(
                exchange = %key.exchange,
                instrument = %key.instrument,
                cid = %key.cid,
                "ExecutionManager received an open whose outcome is unknown - looking the order up at the venue"
            );
            Some(Self::look_up(
                Arc::clone(&self.client),
                key,
                request,
                sent,
                self.unknown_outcome,
                self.request_timeout,
            ))
        });

        (event, lookup)
    }

    /// The key `request` was sent under, as the venue knows it, to look its order up by.
    ///
    /// The request was indexed when it was sent, so this does not fail in practice; if it did, the
    /// order is not looked up and the engine's in-flight deadline flags it.
    fn lookup_key<Kind>(
        &self,
        request: &OrderEvent<Kind, ExchangeIndex, InstrumentIndex>,
    ) -> Option<UnindexedOrderKey>
    where
        Kind: Clone + Debug,
    {
        match self.indexer.order_request(request) {
            Ok(request) => Some(UnindexedOrderKey {
                exchange: request.key.exchange,
                instrument: request.key.instrument.clone(),
                strategy: request.key.strategy,
                cid: request.key.cid,
            }),
            Err(error) => {
                error!(
                    exchange = %self.indexer.map.exchange.value,
                    ?request,
                    %error,
                    "ExecutionManager cannot look up an order whose request's outcome is unknown: its key does not index"
                );
                None
            }
        }
    }

    /// Look up at the venue the order `request` placed under `key`, sent at `sent`, whose outcome
    /// came back unknown.
    ///
    /// Owns what it uses, so every lookup is one future type that outlives the borrow of `self`.
    async fn look_up(
        client: Arc<Client>,
        key: UnindexedOrderKey,
        request: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        sent: Instant,
        policy: UnknownOutcomeLookup,
        lookup_timeout: std::time::Duration,
    ) -> (
        OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        LookupOutcome,
    ) {
        let outcome =
            look_up_unknown_outcome(client.as_ref(), &key, sent, policy, lookup_timeout).await;
        (request, outcome)
    }

    /// The order snapshot that reports what a lookup found, under the key of the request it was
    /// for; `None` if the lookups ran out.
    fn process_lookup_outcome(
        &self,
        request: OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        outcome: LookupOutcome,
    ) -> Option<AccountStreamEvent> {
        let OrderRequestOpen { key, state } = request;
        let order = match outcome {
            LookupOutcome::Live(order) => {
                let Order {
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: open,
                    ..
                } = *order;
                Order {
                    key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: OrderState::active(open),
                }
            }
            LookupOutcome::Ended(order) => {
                let Order {
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: ended,
                    ..
                } = *order;
                Order {
                    key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: self
                        .indexer
                        .order_state(UnindexedOrderState::Inactive(ended)),
                }
            }
            LookupOutcome::NeverPlaced => Order {
                key,
                side: state.side,
                price: state.price,
                quantity: state.quantity,
                kind: state.kind,
                time_in_force: state.time_in_force,
                state: OrderState::inactive(OrderError::NotPlaced(NotPlacedReason::UnknownToVenue)),
            },
            LookupOutcome::Exhausted => {
                warn!(
                    exchange = %self.indexer.map.exchange.value,
                    instrument = ?key.instrument,
                    cid = %key.cid,
                    "ExecutionManager could not settle an order whose open outcome is unknown - it stays in flight"
                );
                return None;
            }
        };

        info!(
            exchange = %self.indexer.map.exchange.value,
            instrument = ?order.key.instrument,
            cid = %order.key.cid,
            state = ?order.state,
            "ExecutionManager settled an order whose open outcome was unknown"
        );
        Some(AccountStreamEvent::Item(AccountEvent {
            exchange: order.key.exchange,
            kind: AccountEventKind::OrderSnapshot(Snapshot(order)),
        }))
    }

    /// Send the amend `request` to the client, bounded by `timeout`, with the instant it was
    /// sent; or, when the client does not declare it ([`ExecutionClient::amend_support`]), the
    /// answer refusing it, unsent.
    ///
    /// # Panics
    /// See [`index_request`](Self::index_request).
    fn send_amend<'a>(
        client: &'a Client,
        indexer: &'a AccountEventIndexer,
        timeout: std::time::Duration,
        request: OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
    ) -> Result<
        RequestFuture<
            (OrderRequestAmend<ExchangeIndex, InstrumentIndex>, Instant),
            impl Future<Output = UnindexedOrderResponseAmend> + 'a,
        >,
        Box<AccountStreamEvent>,
    > {
        let client_request = Self::index_request(indexer, &request, "amend");
        if let Err(refusal) = client.amend_support().permits(&client_request.state) {
            debug!(
                exchange = %indexer.map.exchange.value,
                instrument = ?request.key.instrument,
                cid = %request.key.cid,
                %refusal,
                "ExecutionManager refused an amend the client does not declare - not sent"
            );
            return Err(Box::new(Self::amend_event(
                request.key,
                Err(OrderError::AmendRefused(refusal)),
            )));
        }
        Ok(RequestFuture::new(
            client.amend_order(client_request),
            timeout,
            (request, Instant::now()),
        ))
    }

    /// The `OrderAmended` event answering an amend of the order under `key`.
    fn amend_event(
        key: OrderKey<ExchangeIndex, InstrumentIndex>,
        state: Result<Amended, OrderError<AssetIndex, InstrumentIndex>>,
    ) -> AccountStreamEvent {
        AccountStreamEvent::Item(AccountEvent {
            exchange: key.exchange,
            kind: AccountEventKind::OrderAmended(OrderResponseAmend { key, state }),
        })
    }

    /// The event that answers the amend `request`, sent at `sent`, from the client's `response`
    /// or its timeout; and, when that answer leaves the outcome unknown, the lookup of the order
    /// to start, as [`process_open_answer`](Self::process_open_answer) does for an open.
    fn process_amend_answer(
        &self,
        request: OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
        sent: Instant,
        response: Result<UnindexedOrderResponseAmend, tokio::time::error::Elapsed>,
    ) -> (
        AccountStreamEvent,
        Option<
            impl Future<
                Output = (
                    OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
                    AmendLookupOutcome,
                ),
            > + use<RequestStream, Client>,
        >,
    ) {
        let state = match response {
            Ok(OrderResponseAmend { key, state }) => {
                self.check_response_key(&request.key, key, "amend");
                state.map_err(|error| self.indexer.order_error(error))
            }
            Err(_elapsed) => Err(OrderError::Connectivity(ConnectivityError::Timeout)),
        };
        let unknown = matches!(&state, Err(error) if error.is_outcome_unknown());
        let event = Self::amend_event(request.key.clone(), state);

        let lookup = unknown.then_some(request).and_then(|request| {
            let key = self.lookup_key(&request)?;
            if self.unknown_outcome.attempts == 0 {
                warn!(
                    exchange = %key.exchange,
                    instrument = %key.instrument,
                    cid = %key.cid,
                    "ExecutionManager received an amend whose outcome is unknown - lookups are off, so it stays in flight"
                );
                return None;
            }
            warn!(
                exchange = %key.exchange,
                instrument = %key.instrument,
                cid = %key.cid,
                "ExecutionManager received an amend whose outcome is unknown - looking the order up at the venue"
            );
            Some(Self::look_up_amend(
                Arc::clone(&self.client),
                key,
                request,
                sent,
                self.unknown_outcome,
                self.request_timeout,
            ))
        });

        (event, lookup)
    }

    /// Look up at the venue the order an amend whose outcome came back unknown was for.
    ///
    /// Owns what it uses, as [`look_up`](Self::look_up) does.
    async fn look_up_amend(
        client: Arc<Client>,
        key: UnindexedOrderKey,
        request: OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
        sent: Instant,
        policy: UnknownOutcomeLookup,
        lookup_timeout: std::time::Duration,
    ) -> (
        OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
        AmendLookupOutcome,
    ) {
        let outcome = look_up_unknown_amend(
            client.as_ref(),
            &key,
            &request.state.current,
            sent,
            policy,
            lookup_timeout,
        )
        .await;
        (request, outcome)
    }

    /// The event that reports what the lookup of an amended order found: the order as the venue
    /// lists it, as the amend's answer; its end, as an order snapshot; or the amend not applied.
    /// `None` if the lookups ran out, leaving the amend in flight.
    fn process_amend_lookup_outcome(
        &self,
        request: OrderRequestAmend<ExchangeIndex, InstrumentIndex>,
        outcome: AmendLookupOutcome,
    ) -> Option<AccountStreamEvent> {
        let OrderRequestAmend { key, state: amend } = request;
        let event = match outcome {
            AmendLookupOutcome::Live(order) => {
                let Order {
                    price,
                    quantity,
                    kind,
                    state: open,
                    ..
                } = *order;
                let previous_id = open.id.contradicts(&amend.id).then_some(amend.id);
                Self::amend_event(
                    key,
                    Ok(Amended::new(open, price, quantity, kind, previous_id)),
                )
            }
            AmendLookupOutcome::Ended(order) => {
                let Order {
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: ended,
                    ..
                } = *order;
                AccountStreamEvent::Item(AccountEvent {
                    exchange: key.exchange,
                    kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
                        key,
                        side,
                        price,
                        quantity,
                        kind,
                        time_in_force,
                        state: self
                            .indexer
                            .order_state(UnindexedOrderState::Inactive(ended)),
                    })),
                })
            }
            AmendLookupOutcome::NotApplied => {
                Self::amend_event(key, Err(OrderError::AmendRefused(AmendRefusal::NotApplied)))
            }
            AmendLookupOutcome::Exhausted => {
                warn!(
                    exchange = %self.indexer.map.exchange.value,
                    instrument = ?key.instrument,
                    cid = %key.cid,
                    "ExecutionManager could not settle an amend whose outcome is unknown - it stays in flight"
                );
                return None;
            }
        };

        info!(
            exchange = %self.indexer.map.exchange.value,
            "ExecutionManager settled an amend whose outcome was unknown"
        );
        Some(event)
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

/// The opens of each [`ExecutionRequest::CancelThenOpen`] group whose cancels are not all answered
/// yet, keyed by the group id its cancels carry while in flight.
#[derive(Debug, Default)]
struct HeldGroups {
    held: FnvHashMap<u64, HeldOpens>,
    next: u64,
}

/// The opens of one group, held until every cancel of the group has been answered.
#[derive(Debug)]
struct HeldOpens {
    /// How many of the group's cancels are still unanswered.
    unanswered: usize,
    /// The first of the group's cancels whose answer was an error, if any has been.
    failed: Option<ClientOrderId>,
    opens: Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    on_cancel_failure: OnCancelFailure,
}

/// What to send of a group just received.
#[derive(Debug)]
enum Hold {
    /// Its cancels, tagged with `group`; its opens are held.
    Cancels {
        group: u64,
        cancels: Vec<OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
    },
    /// Its opens, at once: it has no cancel to wait on. The Engine refuses such a group, so this
    /// is a caller sending its own requests.
    Opens(Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>),
}

/// What to do with a group's opens once its last cancel has been answered.
#[derive(Debug, PartialEq)]
enum Release {
    /// Send them.
    Send(Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>),
    /// Answer each as dropped, because `cancel` failed.
    Drop {
        opens: Vec<OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
        cancel: ClientOrderId,
    },
}

/// The open of a group sent as one venue request, as the request's answer leaves it.
// Returned and matched at once, never stored, so the variants' size difference costs nothing,
// where boxing would allocate on every answer.
#[allow(clippy::large_enum_variant)]
enum NativeOpen {
    /// Answered, or timed out with its group: processed as an open sent alone.
    Answered(
        OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        Result<
            Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
            tokio::time::error::Elapsed,
        >,
    ),
    /// Not attempted by the venue: the event that answers it as dropped.
    Dropped(AccountStreamEvent),
}

impl HeldGroups {
    /// Hold `group`'s opens until its cancels are answered, returning the cancels to send.
    fn hold(&mut self, group: CancelThenOpen<ExchangeIndex, InstrumentIndex>) -> Hold {
        let CancelThenOpen {
            cancels,
            opens,
            on_cancel_failure,
            ..
        } = group;
        if cancels.is_empty() {
            return Hold::Opens(opens);
        }
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.held.insert(
            id,
            HeldOpens {
                unanswered: cancels.len(),
                failed: None,
                opens,
                on_cancel_failure,
            },
        );
        Hold::Cancels { group: id, cancels }
    }

    /// Record that a cancel of `group`, `cid`, has been answered, `failed` or not. Returns what to
    /// do with the group's opens once that was its last cancel, releasing the group, and `None`
    /// before then.
    fn answer(&mut self, group: u64, cid: ClientOrderId, failed: bool) -> Option<Release> {
        let entry = self.held.get_mut(&group)?;
        if failed && entry.failed.is_none() {
            entry.failed = Some(cid);
        }
        entry.unanswered = entry.unanswered.saturating_sub(1);
        if entry.unanswered > 0 {
            return None;
        }
        let HeldOpens {
            failed,
            opens,
            on_cancel_failure,
            ..
        } = self.held.remove(&group)?;
        Some(match (failed, on_cancel_failure) {
            (Some(cancel), OnCancelFailure::DropOpens) => Release::Drop { opens, cancel },
            (None, _) | (Some(_), OnCancelFailure::SendOpens) => Release::Send(opens),
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
        AccountSnapshot, FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope,
        UnindexedAccountEvent, UnindexedAccountSnapshot,
        balance::AssetBalance,
        error::{AmendField, ApiError, ClientError, UnindexedClientError, UnindexedOrderError},
        map::generate_execution_instrument_map,
        order::{
            OrderKey, OrderKind, TimeInForce, UnindexedInactiveOrder,
            amend::{AmendAllowed, AmendPatch, AmendSupport, AmendTarget, RequestAmend},
            id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
            request::{RequestCancel, RequestOpen},
            state::{ActiveOrderState, Cancelled, InactiveOrderState, Open},
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

    impl OrderStatusClient for RejectingClient {
        async fn fetch_ended_orders(
            &self,
            _: &[UnindexedOrderKey],
        ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
            unreachable!("a definite answer to an open starts no lookup")
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
            unknown_outcome: UnknownOutcomeLookup::default(),
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
        /// The assets the account snapshot holds a balance for.
        snapshot_balances: Vec<AssetNameExchange>,
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
        /// What `process_account_events` does; the identity when unset.
        stage: Option<Stage>,
    }

    type Events = BoxStream<'static, UnindexedAccountEvent>;

    type StageFn = dyn Fn(Events) -> Events + Send + Sync;

    /// A client's stage over each open's account events, as a test sets it.
    #[derive(Clone)]
    struct Stage(Arc<StageFn>);

    /// Manual because a closure is not [`Debug`].
    impl std::fmt::Debug for Stage {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Stage")
        }
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
            Ok(UnindexedAccountSnapshot::new(
                EXCHANGE,
                self.snapshot_balances
                    .iter()
                    .map(|asset| {
                        AssetBalance::new(
                            asset.clone(),
                            rustrade_execution::balance::Balance::new(dec!(1), dec!(1)),
                            DateTime::<Utc>::MIN_UTC,
                        )
                    })
                    .collect(),
                vec![],
            ))
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

        fn process_account_events(
            &self,
            events: BoxStream<'static, UnindexedAccountEvent>,
        ) -> BoxStream<'static, UnindexedAccountEvent> {
            match &self.stage {
                Some(stage) => (stage.0)(events),
                None => events,
            }
        }
    }

    /// Initialise a manager over `client`, with a 1 ms backoff.
    impl OrderStatusClient for FlakyAccountClient {
        async fn fetch_ended_orders(
            &self,
            _: &[UnindexedOrderKey],
        ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
            unreachable!("the account stream tests send no open")
        }
    }

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
            AccountStreamEvent::Item(event) => describe_kind(&event.kind),
        }
    }

    /// As [`describe`], for an event before it is indexed: what a client's stage sees.
    fn describe_unindexed(event: &UnindexedAccountEvent) -> String {
        describe_kind(&event.kind)
    }

    fn describe_kind<ExchangeKey, AssetKey, InstrumentKey>(
        kind: &AccountEventKind<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> String
    where
        AccountEventKind<ExchangeKey, AssetKey, InstrumentKey>: std::fmt::Debug,
    {
        match kind {
            AccountEventKind::Snapshot(_) => "snapshot".to_owned(),
            AccountEventKind::Trade(trade) => format!("trade {}", trade.id.0),
            AccountEventKind::FillRecoveryGaveUp(gap) => format!("gap {}", gap.reason),
            other => format!("{other:?}"),
        }
    }

    fn at_clock(secs: i64) -> Clock {
        Clock::new(move || DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap())
    }

    /// The fills read for the span a stream was down come before the snapshot that follows them,
    /// and a fill the new stream sends again, read already, is delivered once. With no fill
    /// delivered before the stream ended, the venue's clock is unknown, so the read allows the
    /// 30 s skew allowance on both sides of the local clock, and 2 s of overlap before it.
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
        // The first open reads nothing; the re-init reads around the watermark, to past the
        // allowance after the local time counted on from it on tokio's clock. The end's margin
        // above 130 is real time elapsed, generous so a loaded runner does not fail it.
        let [(start, end)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        assert_eq!(start, at(68));
        assert!(at(130) <= end && end < at(160), "the read ends at {end}");
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
            // Stamped when the venue's clock is at the stepping clock's third reading.
            ending: vec![live_trade("x", 2000)],
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

    /// A client's stage sees each open's events once, in order: the read's fills, the snapshot,
    /// then the stream's updates, with the fill the stream repeats from the read already dropped.
    /// The manager's own reports of the reconnect do not pass through it.
    #[tokio::test]
    async fn the_clients_stage_sees_each_opens_events_once_in_order() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
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
            stage: Some(Stage(Arc::new({
                let seen = Arc::clone(&seen);
                move |events| {
                    let open = {
                        let mut seen = seen.lock().unwrap();
                        seen.push(Vec::new());
                        seen.len() - 1
                    };
                    let seen = Arc::clone(&seen);
                    events
                        .inspect(move |event| {
                            seen.lock().unwrap()[open].push(describe_unindexed(event));
                        })
                        .boxed()
                }
            }))),
            ..FlakyAccountClient::default()
        };
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
        assert_eq!(
            *seen.lock().unwrap(),
            [
                vec!["snapshot"],
                vec!["trade a", "trade b", "snapshot", "trade c"],
            ]
        );
    }

    /// A fill the client's stage adds is recorded as seen, so when the venue sends it later the
    /// consumer gets it once.
    #[tokio::test]
    async fn a_fill_the_clients_stage_adds_is_not_delivered_again_when_the_venue_sends_it() {
        let client = FlakyAccountClient {
            live: vec![live_trade("z", 5), live_trade("c", 30)],
            stage: Some(Stage(Arc::new(|events| {
                events
                    .flat_map(|event| {
                        let events = match event.kind {
                            AccountEventKind::Snapshot(_) => vec![event, live_trade("z", 5)],
                            _ => vec![event],
                        };
                        futures::stream::iter(events)
                    })
                    .boxed()
            }))),
            ..FlakyAccountClient::default()
        };
        let Ok(manager) = init_flaky(client).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(3).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "trade z", "trade c"]
        );
    }

    /// The watermark is sampled before the client's stage: a fill the stage drops still sets the
    /// venue's clock for the next read. The host's clock is 190 s ahead of the venue's, so a read
    /// placed without the fill would start past it, 32 s before the host's time.
    #[tokio::test]
    async fn the_read_is_placed_by_a_fill_received_even_if_the_stage_drops_it() {
        let client = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 10)],
            stage: Some(Stage(Arc::new(|events| {
                events
                    .filter(|event| {
                        std::future::ready(!matches!(event.kind, AccountEventKind::Trade(_)))
                    })
                    .boxed()
            }))),
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, at_clock(200)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(3).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "reconnecting", "snapshot"]
        );
        let fill = DateTime::<Utc>::from_timestamp(1_800_000_010, 0).unwrap();
        let [(start, _)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        assert!(
            start <= fill,
            "the read starts at {start}, after the fill at {fill}"
        );
    }

    /// A stage that ends its stream early makes the manager reopen, and a fill the stage took
    /// and did not pass on is not offered to it again: the read and the new stream both send it,
    /// and both copies are dropped as seen.
    #[tokio::test]
    async fn a_fill_a_stage_took_before_ending_early_is_not_offered_again_on_the_reopen() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
        let client = FlakyAccountClient {
            live: vec![live_trade("x", 10)],
            reads: Arc::new(std::sync::Mutex::new(
                [Ok(TradesRead::complete(vec![missed_trade("x", 10)]))].into(),
            )),
            // Takes the snapshot and the fill, passes on the snapshot only, then ends.
            stage: Some(Stage(Arc::new({
                let seen = Arc::clone(&seen);
                move |events| {
                    let open = {
                        let mut seen = seen.lock().unwrap();
                        seen.push(Vec::new());
                        seen.len() - 1
                    };
                    let seen = Arc::clone(&seen);
                    events
                        .inspect(move |event| {
                            seen.lock().unwrap()[open].push(describe_unindexed(event));
                        })
                        .take(2)
                        .filter(|event| {
                            std::future::ready(!matches!(event.kind, AccountEventKind::Trade(_)))
                        })
                        .boxed()
                }
            }))),
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, at_clock(100)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(3).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "reconnecting", "snapshot"]
        );
        assert_eq!(fetched.lock().unwrap().len(), 1, "the reopen reads the gap");
        assert_eq!(
            *seen.lock().unwrap(),
            [vec!["snapshot", "trade x"], vec!["snapshot"]]
        );
    }

    /// A fill the stage gives up on, reported as the contract asks, reaches the consumer as a
    /// gap indexed like any other: scoped to the fill's instrument, at its time.
    #[tokio::test]
    async fn a_fill_the_stage_gives_up_on_reaches_the_consumer_as_an_indexed_gap() {
        let client = FlakyAccountClient {
            live: vec![live_trade("x", 10)],
            stage: Some(Stage(Arc::new(|events| {
                events
                    .map(|event| match event.kind {
                        AccountEventKind::Trade(trade) => UnindexedAccountEvent {
                            exchange: event.exchange,
                            kind: AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                                FillRecoveryScope::Instruments(vec![trade.instrument]),
                                trade.time_exchange,
                                trade.time_exchange,
                                3,
                                FillRecoveryFailure::Processing("no order".to_owned()),
                            )),
                        },
                        _ => event,
                    })
                    .boxed()
            }))),
            ..FlakyAccountClient::default()
        };
        let Ok(manager) = init_flaky(client).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(2).collect::<Vec<_>>().await;

        let [_, AccountStreamEvent::Item(event)] = &events[..] else {
            panic!("expected the snapshot then an event, got {events:?}");
        };
        let time = DateTime::<Utc>::from_timestamp(1_800_000_010, 0).unwrap();
        assert_eq!(
            event.kind,
            AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                FillRecoveryScope::Instruments(vec![InstrumentIndex(0)]),
                time,
                time,
                3,
                FillRecoveryFailure::Processing("no order".to_owned()),
            ))
        );
    }

    /// A host clock 190 s ahead of the venue's, far beyond the 30 s allowance, still reads the
    /// fill the venue stamped while the stream was down: the read is placed on the venue's clock,
    /// from the time it stamped on the last fill delivered.
    #[tokio::test]
    async fn a_host_clock_ahead_of_the_venue_beyond_the_allowance_still_recovers_the_missed_fill() {
        let client = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 10)],
            reads: Arc::new(std::sync::Mutex::new(
                [Ok(TradesRead::complete(vec![missed_trade("m", 15)]))].into(),
            )),
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let Ok(manager) = init_flaky_recovering(client, at_clock(200)).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(5).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "trade x", "reconnecting", "trade m", "snapshot"]
        );
        let at = |secs: i64| DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap();
        let [(start, end)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        assert!(
            start <= at(15) && at(15) <= end,
            "[{start}, {end}] misses the fill at {}",
            at(15)
        );
    }

    /// A host clock that steps back 100 s while the stream is down reads, at the reopening, a
    /// time before the outage began. The read still runs past the venue's time: the end is
    /// counted on from the last fill on tokio's clock, which does not step.
    #[tokio::test]
    async fn a_host_clock_stepped_back_before_the_reopening_still_reads_to_the_venues_time() {
        let client = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 100)],
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        // 100 until the stream reopens (the second open), then 0.
        let attempts = Arc::clone(&client.attempts);
        let clock = Clock::new(move || {
            let reopened = attempts.load(std::sync::atomic::Ordering::Relaxed) >= 2;
            DateTime::<Utc>::from_timestamp(1_800_000_000 + if reopened { 0 } else { 100 }, 0)
                .unwrap()
        });
        let Ok(manager) = init_flaky_recovering(client, clock).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(4).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "trade x", "reconnecting", "snapshot"]
        );
        let at = |secs: i64| DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap();
        let [(start, end)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        // From the fill, at the time both clocks agreed on, less the overlap.
        assert_eq!(start, at(98));
        // Past the venue's time of the fill by the skew allowance, not ended at the host's 0.
        assert!(end >= at(130), "the read ends at {end}");
    }

    /// As the test above, on a stream that delivered no fill: the venue's clock is unknown, but
    /// the read still ends past the host's time at the last event, counted on on tokio's clock.
    #[tokio::test]
    async fn a_host_clock_stepped_back_before_the_reopening_with_no_fill_still_reads_past_it() {
        let client = FlakyAccountClient {
            open_from: 1,
            ..FlakyAccountClient::default()
        };
        let fetched = Arc::clone(&client.fetched);
        let attempts = Arc::clone(&client.attempts);
        let clock = Clock::new(move || {
            let reopened = attempts.load(std::sync::atomic::Ordering::Relaxed) >= 2;
            DateTime::<Utc>::from_timestamp(1_800_000_000 + if reopened { 0 } else { 100 }, 0)
                .unwrap()
        });
        let Ok(manager) = init_flaky_recovering(client, clock).await else {
            panic!("the first attempt succeeds, so init should too");
        };

        let events = manager.account_stream.take(3).collect::<Vec<_>>().await;

        assert_eq!(
            events.iter().map(describe).collect::<Vec<_>>(),
            ["snapshot", "reconnecting", "snapshot"]
        );
        let at = |secs: i64| DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap();
        let [(_, end)] = fetched.lock().unwrap().clone()[..] else {
            panic!("one read")
        };
        assert!(end >= at(130), "the read ends at {end}");
    }

    /// How many `WARN` events the manager's fill recovery logs about the clock difference while
    /// `client`'s stream is read for `events` events, on `clock`.
    async fn clock_warnings(client: FlakyAccountClient, clock: Clock, events: usize) -> usize {
        // The current-thread test runtime polls the stream on this thread, so the capture sees
        // every event.
        let capture = rustrade_instrument::test_utils::logs::Capture::start();
        let Ok(manager) = init_flaky_recovering(client, clock).await else {
            panic!("the first attempt succeeds, so init should too");
        };
        let _ = manager
            .account_stream
            .take(events)
            .collect::<Vec<_>>()
            .await;
        capture
            .events()
            .iter()
            .filter(|event| {
                event.level == tracing::Level::WARN
                    && event.message().contains("differs from the venue's")
            })
            .count()
    }

    /// The warning fires at the reopening when the host's clock leads the venue's by 190 s, and
    /// not when they agree.
    #[tokio::test]
    async fn the_manager_warns_of_a_clock_difference_beyond_the_allowance_only() {
        let ahead = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 10)],
            ..FlakyAccountClient::default()
        };
        let agreeing = FlakyAccountClient {
            open_from: 1,
            ending: vec![live_trade("x", 200)],
            ..FlakyAccountClient::default()
        };

        assert_eq!(clock_warnings(ahead, at_clock(200), 4).await, 1);
        assert_eq!(clock_warnings(agreeing, at_clock(200), 4).await, 0);
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
        let client = MockExecution::new(
            ExchangeId::Mock,
            sim_now.clone(),
            request_tx,
            event_rx,
            AmendSupport::NONE,
        );

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

    /// Run `f`, returning its result and how many `WARN`, `DEBUG` and `ERROR` events it logged:
    /// `(result, warn, debug, error)`.
    fn with_log_counts<R>(f: impl FnOnce() -> R) -> (R, usize, usize, usize) {
        let (result, events) = rustrade_instrument::test_utils::logs::capture(f);
        let [warn, debug, error] = [
            tracing::Level::WARN,
            tracing::Level::DEBUG,
            tracing::Level::ERROR,
        ]
        .map(|level| events.iter().filter(|event| event.level == level).count());
        (result, warn, debug, error)
    }

    /// Index `events` through `index_account_stream`, returning how many events came out and how
    /// many `WARN`, `DEBUG` and `ERROR` events it logged: `(indexed, warn, debug, error)`.
    fn index_and_count_logs(
        indexer: AccountEventIndexer,
        events: Vec<UnindexedAccountEvent>,
    ) -> (usize, usize, usize, usize) {
        with_log_counts(|| {
            futures::executor::block_on(
                ExecutionManager::<
                    futures::stream::Pending<ExecutionRequest<ExchangeIndex, InstrumentIndex>>,
                    RejectingClient,
                >::index_account_stream(futures::stream::iter(events), indexer)
                .count(),
            )
        })
    }

    /// Fetch the account snapshot of a client holding balances for `held`, and index it, with the
    /// map registering `btc` and `usdt` only: `(kept, warn, debug, error)`, the balances kept and
    /// the logs.
    fn snapshot_balances_kept(held: &[&str]) -> (usize, usize, usize, usize) {
        type Manager = ExecutionManager<
            futures::stream::Pending<ExecutionRequest<ExchangeIndex, InstrumentIndex>>,
            FlakyAccountClient,
        >;
        let instruments = IndexedInstruments::new(vec![instrument(EXCHANGE, "btc", "usdt")]);
        let map = generate_execution_instrument_map(&instruments, EXCHANGE).unwrap();
        let indexer = AccountEventIndexer::new(Arc::new(map));
        let client = Arc::new(FlakyAccountClient {
            snapshot_balances: held.iter().map(|a| AssetNameExchange::new(*a)).collect(),
            ..Default::default()
        });
        with_log_counts(|| {
            let event = futures::executor::block_on(Manager::fetch_account_snapshot(
                &client,
                &indexer,
                &[],
                &[],
            ))
            .unwrap();
            let AccountEventKind::Snapshot(snapshot) = event.kind else {
                panic!("expected a snapshot");
            };
            let indexed = Manager::index_snapshot(&indexer, snapshot).unwrap();
            let AccountEventKind::Snapshot(snapshot) = indexed.kind else {
                panic!("expected a snapshot");
            };
            snapshot.balances.len()
        })
    }

    #[test]
    fn a_snapshot_drops_an_unregistered_balance_with_one_warning() {
        // The registered balances are kept; bnb and sol are named in a single warning.
        assert_eq!(
            snapshot_balances_kept(&["usdt", "bnb", "btc", "sol"]),
            (2, 1, 0, 0)
        );
        // Nothing to drop, nothing logged.
        assert_eq!(snapshot_balances_kept(&["usdt", "btc"]), (2, 0, 0, 0));
        // An account holding nothing is not a mismatch.
        assert_eq!(snapshot_balances_kept(&[]), (0, 0, 0, 0));
    }

    /// A snapshot a client's stage changed so that it no longer indexes is dropped with an error,
    /// as any other rejected event is.
    #[test]
    fn a_snapshot_that_no_longer_indexes_after_the_stage_is_dropped_with_an_error() {
        let instruments = IndexedInstruments::new(vec![instrument(EXCHANGE, "btc", "usdt")]);
        let map = generate_execution_instrument_map(&instruments, EXCHANGE).unwrap();
        let snapshot = UnindexedAccountEvent {
            exchange: EXCHANGE,
            kind: AccountEventKind::Snapshot(UnindexedAccountSnapshot::new(
                ExchangeId::Kraken,
                vec![],
                vec![],
            )),
        };

        assert_eq!(
            index_and_count_logs(AccountEventIndexer::new(Arc::new(map)), vec![snapshot]),
            (0, 0, 0, 1)
        );
    }

    #[test]
    fn a_snapshot_that_keeps_no_balance_is_an_error() {
        assert_eq!(snapshot_balances_kept(&["bnb", "sol"]), (0, 0, 0, 1));
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
    fn the_unknown_outcome_option_is_set_and_shown() {
        let policy = UnknownOutcomeLookup::default().with_attempts(9);
        let options = AccountStreamOptions::default().with_unknown_outcome(policy);
        assert_eq!(options.unknown_outcome, policy);
        assert_eq!(
            AccountStreamOptions::default().unknown_outcome,
            UnknownOutcomeLookup::default()
        );

        let (response_tx, _response_rx) = mpsc_unbounded();
        let manager = ExecutionManager {
            request_stream: futures::stream::empty::<ExecutionRequest>(),
            request_timeout: std::time::Duration::from_secs(5),
            unknown_outcome: policy,
            response_tx,
            client: Arc::new(RejectingClient::default()),
            indexer: AccountEventIndexer::new(Arc::new(
                generate_execution_instrument_map(
                    &IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]),
                    EXCHANGE,
                )
                .unwrap(),
            )),
            account_stream: futures::stream::empty().boxed(),
        };
        let debug = format!("{manager:?}");
        assert!(debug.starts_with("ExecutionManager"), "{debug}");
        assert!(debug.contains("attempts: 9"), "{debug}");
    }

    #[test]
    fn the_clock_has_a_debug_form() {
        assert_eq!(format!("{:?}", Clock::default()), "Clock(..)");
    }

    /// What [`LookupClient`] answers to one lookup.
    #[derive(Debug, Clone)]
    enum Lookup {
        /// Both reads fail.
        Fail,
        /// Neither read finds the order.
        Unknown,
        /// The open-order listing holds the order, with this much filled.
        Live(rust_decimal::Decimal),
        /// The ended-order read finds the order cancelled.
        Cancelled,
        /// The open-order listing holds the order at this limit price, nothing filled.
        LiveAt(rust_decimal::Decimal),
    }

    /// Answers every open with `open_answer`, or never, and each lookup of the order with the next
    /// of `lookups`, through the reads the provided
    /// [`OrderStatusClient::fetch_order_status`] makes.
    #[derive(Debug, Clone)]
    struct LookupClient {
        /// `None` never answers an open, so the manager times it out.
        open_answer: Option<UnindexedOrderError>,
        lookups: Arc<std::sync::Mutex<std::collections::VecDeque<Lookup>>>,
        amend_support: AmendSupport,
        /// `None` never answers an amend, so the manager times it out.
        amend_answer: Option<Result<Amended, UnindexedOrderError>>,
        amends_sent: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl LookupClient {
        fn new(open_answer: Option<UnindexedOrderError>, lookups: Vec<Lookup>) -> Self {
            Self {
                open_answer,
                lookups: Arc::new(std::sync::Mutex::new(lookups.into())),
                amend_support: AmendSupport::NONE,
                amend_answer: None,
                amends_sent: Arc::default(),
            }
        }

        fn with_amends(
            self,
            support: AmendSupport,
            answer: Option<Result<Amended, UnindexedOrderError>>,
        ) -> Self {
            Self {
                amend_support: support,
                amend_answer: answer,
                ..self
            }
        }

        fn amends_sent(&self) -> usize {
            self.amends_sent.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn lookups_left(&self) -> usize {
            self.lookups.lock().unwrap().len()
        }
    }

    impl ExecutionClient for LookupClient {
        const EXCHANGE: ExchangeId = EXCHANGE;
        const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[];
        type Config = ();
        type AccountStream = futures::stream::Empty<UnindexedAccountEvent>;

        fn new(_: Self::Config) -> Self {
            unreachable!("built by the tests")
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
            _: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
        ) -> UnindexedOrderResponseCancel {
            unreachable!("the tests send no cancel")
        }

        async fn open_order(
            &self,
            request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            let Some(error) = self.open_answer.clone() else {
                return std::future::pending().await;
            };
            let OrderRequestOpen { key, state } = request;
            Order {
                key: OrderKey {
                    exchange: key.exchange,
                    instrument: key.instrument.clone(),
                    strategy: key.strategy,
                    cid: key.cid,
                },
                side: state.side,
                price: state.price,
                quantity: state.quantity,
                kind: state.kind,
                time_in_force: state.time_in_force,
                state: OrderState::inactive(error),
            }
        }

        async fn fetch_balances(
            &self,
            _: &[AssetNameExchange],
        ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
            unreachable!("ExecutionManager::run does not fetch balances")
        }

        /// The second read of a lookup: takes its answer.
        async fn fetch_open_orders(
            &self,
            instruments: &[InstrumentNameExchange],
        ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>
        {
            let lookup = self.lookups.lock().unwrap().pop_front();
            Ok(match lookup {
                Some(Lookup::LiveAt(price)) => vec![Order {
                    key: OrderKey {
                        exchange: EXCHANGE,
                        instrument: instruments[0].clone(),
                        strategy: StrategyId::unknown(),
                        cid: ClientOrderId::new(LOOKUP_CID),
                    },
                    side: Side::Buy,
                    price: Some(price),
                    quantity: dec!(1),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open::new(
                        VenueOrderId::Assigned(OrderId::new("venue-id")),
                        DateTime::<Utc>::MIN_UTC,
                        dec!(0),
                    ),
                }],
                Some(Lookup::Live(filled_quantity)) => vec![Order {
                    key: OrderKey {
                        exchange: EXCHANGE,
                        instrument: instruments[0].clone(),
                        strategy: StrategyId::unknown(),
                        cid: ClientOrderId::new(LOOKUP_CID),
                    },
                    side: Side::Buy,
                    price: Some(dec!(100)),
                    quantity: dec!(1),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open::new(
                        VenueOrderId::Assigned(OrderId::new("venue-id")),
                        DateTime::<Utc>::MIN_UTC,
                        filled_quantity,
                    ),
                }],
                _ => Vec::new(),
            })
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

        fn amend_support(&self) -> AmendSupport {
            self.amend_support
        }

        async fn amend_order(
            &self,
            request: OrderRequestAmend<ExchangeId, &InstrumentNameExchange>,
        ) -> UnindexedOrderResponseAmend {
            self.amends_sent
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let Some(answer) = self.amend_answer.clone() else {
                return std::future::pending().await;
            };
            OrderResponseAmend {
                key: request.key.into_owned_instrument(),
                state: answer,
            }
        }
    }

    impl OrderStatusClient for LookupClient {
        /// The first read of a lookup: takes its answer when it is a failure or an end, and leaves
        /// the rest to [`fetch_open_orders`](ExecutionClient::fetch_open_orders).
        async fn fetch_ended_orders(
            &self,
            orders: &[UnindexedOrderKey],
        ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
            let mut lookups = self.lookups.lock().unwrap();
            match lookups.front() {
                Some(Lookup::Fail) => {
                    lookups.pop_front();
                    Err(UnindexedClientError::Connectivity(
                        ConnectivityError::Timeout,
                    ))
                }
                Some(Lookup::Cancelled) => {
                    lookups.pop_front();
                    Ok(vec![Order {
                        key: orders[0].clone(),
                        side: Side::Buy,
                        price: Some(dec!(100)),
                        quantity: dec!(1),
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                        state: InactiveOrderState::Cancelled(Cancelled::new(
                            OrderId::new("venue-id"),
                            DateTime::<Utc>::MIN_UTC,
                            Some(dec!(0)),
                        )),
                    }])
                }
                _ => Ok(Vec::new()),
            }
        }
    }

    const LOOKUP_CID: &str = "unknown-outcome";

    /// Send one open through a manager over `client`, with `policy`, and collect the order
    /// snapshots it forwards once the request stream ends and it drains.
    async fn run_open_with_lookups(
        client: LookupClient,
        policy: UnknownOutcomeLookup,
    ) -> Vec<Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>>> {
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
            panic!("the instrument map should build");
        };
        let indexer = AccountEventIndexer::new(Arc::new(map));
        let requests = futures::stream::iter([ExecutionRequest::Open(OrderRequestOpen {
            key: OrderKey {
                exchange: indexer.map.exchange.key,
                instrument: InstrumentIndex(0),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(LOOKUP_CID),
            },
            state: RequestOpen::new(
                Side::Buy,
                Some(dec!(100)),
                dec!(1),
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: false },
            ),
        })]);
        let (response_tx, response_rx) = mpsc_unbounded();

        ExecutionManager {
            request_stream: requests,
            request_timeout: std::time::Duration::from_secs(5),
            unknown_outcome: policy,
            response_tx,
            client: Arc::new(client),
            indexer,
            account_stream: futures::stream::empty().boxed(),
        }
        .run()
        .await;

        response_rx
            .into_stream()
            .filter_map(|event| async move {
                match event {
                    rustrade_data::streams::reconnect::Event::Item(AccountEvent {
                        kind: AccountEventKind::OrderSnapshot(Snapshot(order)),
                        ..
                    }) => Some(order),
                    _ => None,
                }
            })
            .collect()
            .await
    }

    /// The order the amend tests amend: a limit buy of 1 at 100, as [`LookupClient`] lists it.
    fn amend_target() -> AmendTarget {
        AmendTarget {
            side: Side::Buy,
            price: Some(dec!(100)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            reduce_only: Some(rustrade_execution::order::request::ReduceOnly::No),
        }
    }

    /// Send one amend, repricing the order to 99, through a manager over `client`, with `policy`,
    /// and collect the amend answers and order snapshots it forwards once it drains.
    async fn run_amend_with_lookups(
        client: LookupClient,
        policy: UnknownOutcomeLookup,
    ) -> Vec<AccountEventKind<ExchangeIndex, AssetIndex, InstrumentIndex>> {
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
            panic!("the instrument map should build");
        };
        let indexer = AccountEventIndexer::new(Arc::new(map));
        let requests = futures::stream::iter([ExecutionRequest::Amend(OrderRequestAmend {
            key: OrderKey {
                exchange: indexer.map.exchange.key,
                instrument: InstrumentIndex(0),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(LOOKUP_CID),
            },
            state: RequestAmend::new(
                VenueOrderId::Assigned(OrderId::new("venue-id")),
                AmendPatch::default().with_price(dec!(99)),
                amend_target(),
                dec!(0),
            ),
        })]);
        let (response_tx, response_rx) = mpsc_unbounded();

        ExecutionManager {
            request_stream: requests,
            request_timeout: std::time::Duration::from_secs(5),
            unknown_outcome: policy,
            response_tx,
            client: Arc::new(client),
            indexer,
            account_stream: futures::stream::empty().boxed(),
        }
        .run()
        .await;

        response_rx
            .into_stream()
            .filter_map(|event| async move {
                match event {
                    rustrade_data::streams::reconnect::Event::Item(AccountEvent {
                        kind, ..
                    }) => Some(kind),
                    _ => None,
                }
            })
            .collect()
            .await
    }

    fn amended_at(price: rust_decimal::Decimal) -> Amended {
        Amended::new(
            Open::new(
                VenueOrderId::Assigned(OrderId::new("venue-id")),
                DateTime::<Utc>::MIN_UTC,
                dec!(0),
            ),
            Some(price),
            dec!(1),
            OrderKind::Limit,
            None,
        )
    }

    fn amend_answer(
        event: &AccountEventKind<ExchangeIndex, AssetIndex, InstrumentIndex>,
    ) -> &Result<Amended, OrderError<AssetIndex, InstrumentIndex>> {
        match event {
            AccountEventKind::OrderAmended(response) => {
                assert_eq!(
                    response.key.strategy,
                    StrategyId::new("test"),
                    "the request's key"
                );
                &response.state
            }
            other => panic!("expected an amend's answer, got {other:?}"),
        }
    }

    /// An amend the client does not declare is answered as refused, without reaching it.
    #[tokio::test(start_paused = true)]
    async fn an_amend_the_client_does_not_declare_is_refused_unsent() {
        let client = LookupClient::new(None, vec![]).with_amends(
            AmendSupport::ALL.with_price(AmendAllowed::Unsupported),
            Some(Ok(amended_at(dec!(99)))),
        );

        let events = run_amend_with_lookups(client.clone(), UnknownOutcomeLookup::default()).await;

        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(
            amend_answer(&events[0]),
            &Err(OrderError::AmendRefused(AmendRefusal::Unsupported {
                field: AmendField::Price
            }))
        );
        assert_eq!(client.amends_sent(), 0);
    }

    /// A declared amend is sent, and its answer forwarded under the request's key.
    #[tokio::test(start_paused = true)]
    async fn a_declared_amend_is_sent_and_answered() {
        let client = LookupClient::new(None, vec![])
            .with_amends(AmendSupport::ALL, Some(Ok(amended_at(dec!(99)))));

        let events = run_amend_with_lookups(client.clone(), UnknownOutcomeLookup::default()).await;

        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(amend_answer(&events[0]), &Ok(amended_at(dec!(99))));
        assert_eq!(client.amends_sent(), 1);
    }

    /// An amend that times out is answered as unknown, then the order is looked up: listed as it
    /// was, it is asked about again; listed amended, its terms are the amend's answer.
    #[tokio::test(start_paused = true)]
    async fn an_amend_that_times_out_is_settled_by_the_order_the_venue_lists() {
        let client = LookupClient::new(
            None,
            vec![Lookup::LiveAt(dec!(100)), Lookup::LiveAt(dec!(99))],
        )
        .with_amends(AmendSupport::ALL, None);

        let events = run_amend_with_lookups(
            client.clone(),
            UnknownOutcomeLookup::default()
                .with_grace(std::time::Duration::ZERO)
                .with_unknowns_to_conclude(2),
        )
        .await;

        assert_eq!(events.len(), 2, "{events:#?}");
        assert!(
            matches!(amend_answer(&events[0]), Err(error) if error.is_outcome_unknown()),
            "{:?}",
            events[0]
        );
        assert_eq!(amend_answer(&events[1]), &Ok(amended_at(dec!(99))));
        assert_eq!(client.lookups_left(), 0);
    }

    /// Listed as it was before the amend as many times as the policy asks, the amend is concluded
    /// not made.
    #[tokio::test(start_paused = true)]
    async fn an_amend_the_venue_never_shows_is_concluded_not_applied() {
        let client = LookupClient::new(
            None,
            vec![
                Lookup::LiveAt(dec!(100)),
                Lookup::Unknown,
                Lookup::LiveAt(dec!(100)),
            ],
        )
        .with_amends(AmendSupport::ALL, None);

        let events = run_amend_with_lookups(
            client.clone(),
            UnknownOutcomeLookup::default()
                .with_grace(std::time::Duration::ZERO)
                .with_unknowns_to_conclude(2),
        )
        .await;

        assert_eq!(events.len(), 2, "{events:#?}");
        assert_eq!(
            amend_answer(&events[1]),
            &Err(OrderError::AmendRefused(AmendRefusal::NotApplied)),
            "a venue that does not know the order breaks no run"
        );
        assert_eq!(client.lookups_left(), 0);
    }

    /// Found ended, the order's end is forwarded as its snapshot.
    #[tokio::test(start_paused = true)]
    async fn an_amended_order_found_ended_is_reported_ended() {
        let client =
            LookupClient::new(None, vec![Lookup::Cancelled]).with_amends(AmendSupport::ALL, None);

        let events = run_amend_with_lookups(client, UnknownOutcomeLookup::default()).await;

        assert_eq!(events.len(), 2, "{events:#?}");
        assert!(matches!(
            &events[1],
            AccountEventKind::OrderSnapshot(Snapshot(order))
                if matches!(order.state, OrderState::Inactive(InactiveOrderState::Cancelled(_)))
        ));
    }

    fn is_unknown_outcome(
        order: &Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>>,
    ) -> bool {
        order.state.is_outcome_unknown()
    }

    fn socket_reset() -> Option<UnindexedOrderError> {
        Some(OrderError::Connectivity(ConnectivityError::Socket(
            "reset".to_string(),
        )))
    }

    /// An open whose outcome is unknown is forwarded as it is, then the order is looked up, and
    /// the venue's live state of it is forwarded under the request's key.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_outcome_is_looked_up_and_a_live_order_reported_open() {
        let client = LookupClient::new(socket_reset(), vec![Lookup::Live(dec!(0.25))]);

        let orders = run_open_with_lookups(client.clone(), UnknownOutcomeLookup::default()).await;

        assert_eq!(orders.len(), 2, "{orders:#?}");
        assert!(is_unknown_outcome(&orders[0]), "{:?}", orders[0]);
        assert_eq!(
            orders[1].key.strategy,
            StrategyId::new("test"),
            "the request's key"
        );
        match &orders[1].state {
            OrderState::Active(ActiveOrderState::Open(open)) => {
                assert_eq!(open.filled_quantity, dec!(0.25));
                assert_eq!(open.id, VenueOrderId::Assigned(OrderId::new("venue-id")));
            }
            other => panic!("expected the venue's open state, got {other:?}"),
        }
        assert_eq!(client.lookups_left(), 0);
    }

    /// A failed lookup is not an answer: the next one settles the order, here as ended.
    #[tokio::test(start_paused = true)]
    async fn a_failed_lookup_is_retried_and_an_ended_order_reported_ended() {
        let client = LookupClient::new(socket_reset(), vec![Lookup::Fail, Lookup::Cancelled]);

        let orders = run_open_with_lookups(client, UnknownOutcomeLookup::default()).await;

        assert_eq!(orders.len(), 2, "{orders:#?}");
        assert!(
            matches!(
                orders[1].state,
                OrderState::Inactive(InactiveOrderState::Cancelled(_))
            ),
            "{:?}",
            orders[1]
        );
    }

    /// An open the client never answers is timed out, and the timeout's unknown outcome is looked
    /// up like any other.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_open_is_looked_up() {
        let client = LookupClient::new(None, vec![Lookup::Live(dec!(0))]);

        let orders = run_open_with_lookups(client, UnknownOutcomeLookup::default()).await;

        assert_eq!(orders.len(), 2, "{orders:#?}");
        assert_eq!(
            orders[0].state,
            OrderState::inactive(OrderError::Connectivity(ConnectivityError::Timeout))
        );
        assert!(matches!(
            orders[1].state,
            OrderState::Active(ActiveOrderState::Open(_))
        ));
    }

    /// `Unknown` answers conclude the order was never placed only once enough of them come in a
    /// row, each after the grace period: with the default policy the lookups at 1 s and 3 s are
    /// too early, and those at 7, 15 and 31 s conclude.
    #[tokio::test(start_paused = true)]
    async fn unknown_answers_after_the_grace_period_conclude_never_placed() {
        let client = LookupClient::new(socket_reset(), vec![Lookup::Unknown; 5]);

        let orders = run_open_with_lookups(client.clone(), UnknownOutcomeLookup::default()).await;

        assert_eq!(orders.len(), 2, "{orders:#?}");
        assert_eq!(
            orders[1].state,
            OrderState::inactive(OrderError::NotPlaced(NotPlacedReason::UnknownToVenue))
        );
        assert_eq!(client.lookups_left(), 0, "all five lookups were needed");
    }

    fn never_placed(
        orders: &[Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>>],
    ) -> bool {
        matches!(
            orders,
            [_, last] if last.state
                == OrderState::inactive(OrderError::NotPlaced(NotPlacedReason::UnknownToVenue))
        )
    }

    /// An `Unknown` answer received exactly as the grace period ends counts; one received before
    /// it does not.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_answer_counts_from_the_end_of_the_grace_period() {
        let policy = UnknownOutcomeLookup::default()
            .with_attempts(1)
            .with_first_delay(std::time::Duration::from_secs(1))
            .with_unknowns_to_conclude(1);

        let at_the_end = policy.with_grace(std::time::Duration::from_secs(1));
        let client = LookupClient::new(socket_reset(), vec![Lookup::Unknown]);
        let orders = run_open_with_lookups(client, at_the_end).await;
        assert!(never_placed(&orders), "{orders:#?}");

        let before_the_end = policy.with_grace(std::time::Duration::from_millis(1_001));
        let client = LookupClient::new(socket_reset(), vec![Lookup::Unknown]);
        let orders = run_open_with_lookups(client, before_the_end).await;
        assert_eq!(orders.len(), 1, "too early to count: {orders:#?}");
    }

    /// The count of `Unknown` answers that concludes is the policy's, and zero is taken as one.
    #[tokio::test(start_paused = true)]
    async fn the_policys_count_of_unknown_answers_concludes() {
        let policy = UnknownOutcomeLookup::default().with_grace(std::time::Duration::ZERO);
        for (count, needed) in [(0, 1), (1, 1), (2, 2)] {
            let client = LookupClient::new(
                socket_reset(),
                vec![Lookup::Unknown, Lookup::Unknown, Lookup::Live(dec!(0))],
            );
            let orders =
                run_open_with_lookups(client.clone(), policy.with_unknowns_to_conclude(count))
                    .await;
            assert!(never_placed(&orders), "{count}: {orders:#?}");
            assert_eq!(client.lookups_left(), 3 - needed, "{count}");
        }
    }

    /// An `Unknown` answer within the grace period does not count toward the conclusion, so the
    /// venue's later report of the order still settles it.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_answer_within_the_grace_period_does_not_count() {
        // Lookups at 1, 3 and 7 s: only the first is within the grace period.
        let policy = UnknownOutcomeLookup::default()
            .with_unknowns_to_conclude(2)
            .with_grace(std::time::Duration::from_secs(2));
        let client = LookupClient::new(
            socket_reset(),
            vec![Lookup::Unknown, Lookup::Unknown, Lookup::Live(dec!(0))],
        );

        let orders = run_open_with_lookups(client, policy).await;

        assert_eq!(orders.len(), 2, "{orders:#?}");
        assert!(
            matches!(
                orders[1].state,
                OrderState::Active(ActiveOrderState::Open(_))
            ),
            "{:?}",
            orders[1]
        );
    }

    /// A failed lookup breaks a run of `Unknown` answers, so lookups that run out without a
    /// conclusion report nothing more: the order stays in flight for the engine to flag.
    #[tokio::test(start_paused = true)]
    async fn a_failed_lookup_breaks_the_run_and_exhausted_lookups_report_nothing() {
        let client = LookupClient::new(
            socket_reset(),
            vec![
                Lookup::Unknown,
                Lookup::Unknown,
                Lookup::Unknown,
                Lookup::Fail,
                Lookup::Unknown,
            ],
        );

        let orders = run_open_with_lookups(client.clone(), UnknownOutcomeLookup::default()).await;

        assert_eq!(orders.len(), 1, "only the unknown outcome: {orders:#?}");
        assert_eq!(client.lookups_left(), 0);
    }

    /// The provided lookup reads ended orders first and then the open-order listing, reports a
    /// found order under the key it was asked about, and never reads a failed read as `Unknown`.
    #[tokio::test]
    async fn the_provided_order_status_lookup_reads_both_and_keeps_the_key() {
        use rustrade_execution::client::VenueOrderStatus;

        let key = UnindexedOrderKey {
            exchange: EXCHANGE,
            instrument: InstrumentNameExchange::new("btc_usdt"),
            strategy: StrategyId::new("asked"),
            cid: ClientOrderId::new(LOOKUP_CID),
        };
        let client = LookupClient::new(
            None,
            vec![
                Lookup::Live(dec!(0)),
                Lookup::Cancelled,
                Lookup::Unknown,
                Lookup::Fail,
            ],
        );

        match client.fetch_order_status(&key).await {
            Ok(VenueOrderStatus::Live(order)) => assert_eq!(order.key, key),
            other => panic!("expected live, got {other:?}"),
        }
        match client.fetch_order_status(&key).await {
            Ok(VenueOrderStatus::Ended(order)) => assert_eq!(order.key, key),
            other => panic!("expected ended, got {other:?}"),
        }
        assert_eq!(
            client.fetch_order_status(&key).await,
            Ok(VenueOrderStatus::Unknown)
        );
        assert!(client.fetch_order_status(&key).await.is_err());
    }

    /// With no attempts, nothing is looked up.
    #[tokio::test(start_paused = true)]
    async fn no_attempts_look_nothing_up() {
        let client = LookupClient::new(socket_reset(), vec![Lookup::Live(dec!(0))]);

        let orders = run_open_with_lookups(
            client.clone(),
            UnknownOutcomeLookup::default().with_attempts(0),
        )
        .await;

        assert_eq!(orders.len(), 1, "{orders:#?}");
        assert_eq!(client.lookups_left(), 1, "never asked");
    }

    /// [`ExecutionRequest::CancelThenOpen`]: the opens wait for every cancel's answer, and are
    /// sent or dropped as the group says.
    mod cancel_then_open {
        use super::*;
        use rustrade_execution::client::VenueOrderStatus;
        use std::sync::Mutex;

        /// How the scripted client answers a cancel, after its delay.
        #[derive(Debug, Clone, Copy)]
        enum Answer {
            Cancelled,
            AlreadyFilled,
            /// Never answers, so the manager's timeout does.
            Hangs,
        }

        /// How the scripted client answers a group it sends as one venue request, a second after
        /// it was sent.
        #[derive(Debug, Clone, Copy)]
        enum Native {
            /// The cancel succeeded and the open rests.
            Both,
            /// The cancel failed, the order having filled, and the open was not attempted.
            CancelFailed,
            /// The cancel succeeded and the open was rejected.
            OpenRejected,
            /// The answer was lost: the cancel's is a connectivity error, the open's unknown.
            Lost,
            /// Never answers, so the manager's timeout does.
            Hangs,
            /// Breaks the contract: the cancel succeeded, yet the open is said not attempted.
            OpenSkipped,
        }

        /// Answers each cancel as scripted by its client order id, and accepts every open. Logs
        /// each call as it starts and each cancel as it answers, in order. With a `native`
        /// script, it also sends every group of one cancel and one open as one venue request,
        /// and finds every order it is asked to look up live.
        #[derive(Debug, Clone, Default)]
        struct ScriptedClient {
            script: Vec<(&'static str, u64, Answer)>,
            native: Option<Native>,
            log: Arc<Mutex<Vec<String>>>,
        }

        impl ScriptedClient {
            fn new(script: Vec<(&'static str, u64, Answer)>) -> Self {
                Self {
                    script,
                    native: None,
                    log: Arc::default(),
                }
            }

            fn native(native: Native) -> Self {
                Self {
                    native: Some(native),
                    ..Self::new(vec![])
                }
            }

            fn log(&self, entry: String) {
                self.log.lock().unwrap().push(entry);
            }

            fn logged(&self) -> Vec<String> {
                self.log.lock().unwrap().clone()
            }
        }

        impl ExecutionClient for ScriptedClient {
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
                let cid = request.key.cid.to_string();
                self.log(format!("cancel {cid}"));
                let (_, delay, answer) = *self
                    .script
                    .iter()
                    .find(|(scripted, _, _)| *scripted == cid)
                    .expect("every cancel is scripted");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                let state = match answer {
                    Answer::Cancelled => Ok(Cancelled::new(
                        OrderId::new("venue-id"),
                        DateTime::<Utc>::MIN_UTC,
                        Some(dec!(0)),
                    )),
                    Answer::AlreadyFilled => {
                        Err(OrderError::Rejected(ApiError::OrderAlreadyFullyFilled))
                    }
                    Answer::Hangs => std::future::pending().await,
                };
                self.log(format!("cancel {cid} answered"));
                OrderResponseCancel {
                    key: request.key.into_owned_instrument(),
                    state,
                }
            }

            async fn open_order(
                &self,
                request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
            ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
                self.log(format!("open {}", request.key.cid));
                answered(request, OrderState::active(resting()))
            }

            fn cancel_replace(
                &self,
                cancel: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
                open: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
                on_cancel_failure: OnCancelFailure,
            ) -> Option<impl Future<Output = CancelReplaceAnswer> + Send> {
                let native = self.native?;
                self.log(format!(
                    "cancel-replace {} {} {on_cancel_failure:?}",
                    cancel.key.cid, open.key.cid
                ));
                Some(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let cancelled = Ok(Cancelled::new(
                        OrderId::new("venue-id"),
                        DateTime::<Utc>::MIN_UTC,
                        Some(dec!(0)),
                    ));
                    let (cancel_state, open_state) = match native {
                        Native::Both => (cancelled, Some(OrderState::active(resting()))),
                        Native::CancelFailed => (
                            Err(OrderError::Rejected(ApiError::OrderAlreadyFullyFilled)),
                            None,
                        ),
                        Native::OpenRejected => (
                            cancelled,
                            Some(OrderState::inactive(OrderError::Rejected(
                                ApiError::OrderRejected("insufficient balance".to_string()),
                            ))),
                        ),
                        Native::Lost => (
                            Err(OrderError::Connectivity(ConnectivityError::Socket(
                                "dropped".to_string(),
                            ))),
                            Some(OrderState::inactive(OrderError::Connectivity(
                                ConnectivityError::Socket("dropped".to_string()),
                            ))),
                        ),
                        Native::Hangs => std::future::pending().await,
                        Native::OpenSkipped => (cancelled, None),
                    };
                    CancelReplaceAnswer {
                        cancel: OrderResponseCancel {
                            key: cancel.key.into_owned_instrument(),
                            state: cancel_state,
                        },
                        open: open_state.map(|state| answered(open, state)),
                    }
                })
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

        impl OrderStatusClient for ScriptedClient {
            async fn fetch_ended_orders(
                &self,
                _: &[UnindexedOrderKey],
            ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
                unreachable!("lookups go through fetch_order_status")
            }

            async fn fetch_order_status(
                &self,
                key: &UnindexedOrderKey,
            ) -> Result<VenueOrderStatus, UnindexedClientError> {
                self.log(format!("lookup {}", key.cid));
                Ok(VenueOrderStatus::Live(Box::new(Order {
                    key: key.clone(),
                    side: Side::Sell,
                    price: None,
                    quantity: dec!(1),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    state: resting(),
                })))
            }
        }

        /// A resting order's state as the scripted venue reports it.
        fn resting() -> Open {
            Open::new(
                VenueOrderId::Assigned(OrderId::new("venue-id")),
                DateTime::<Utc>::MIN_UTC,
                dec!(0),
            )
        }

        /// The answer to the open `request`, in `state`.
        fn answered(
            request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
            state: UnindexedOrderState,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            let OrderRequestOpen { key, state: open } = request;
            Order {
                key: key.into_owned_instrument(),
                side: open.side,
                price: open.price,
                quantity: open.quantity,
                kind: open.kind,
                time_in_force: open.time_in_force,
                state,
            }
        }

        fn key(cid: &str) -> OrderKey {
            OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(cid),
            }
        }

        fn cancel(cid: &str) -> OrderRequestCancel {
            OrderRequestCancel {
                key: key(cid),
                state: RequestCancel {
                    id: Some(VenueOrderId::Assigned(OrderId::new("venue-id"))),
                },
            }
        }

        fn open(cid: &str) -> OrderRequestOpen {
            OrderRequestOpen {
                key: key(cid),
                state: RequestOpen::new(
                    Side::Sell,
                    None,
                    dec!(1),
                    OrderKind::Market,
                    TimeInForce::ImmediateOrCancel,
                ),
            }
        }

        fn group(cancels: &[&str], opens: &[&str]) -> CancelThenOpen {
            CancelThenOpen::new(
                cancels.iter().map(|cid| cancel(cid)).collect(),
                opens.iter().map(|cid| open(cid)).collect(),
            )
        }

        /// Run `requests` through a manager over `client`, with a 5 s request timeout, and
        /// describe everything it forwards once the request stream ends and it drains.
        async fn run(client: &ScriptedClient, requests: Vec<ExecutionRequest>) -> Vec<String> {
            let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
            let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
                panic!("the instrument map should build");
            };
            let (response_tx, response_rx) = mpsc_unbounded();

            ExecutionManager {
                request_stream: futures::stream::iter(requests),
                request_timeout: std::time::Duration::from_secs(5),
                unknown_outcome: UnknownOutcomeLookup::default(),
                response_tx,
                client: Arc::new(client.clone()),
                indexer: AccountEventIndexer::new(Arc::new(map)),
                account_stream: futures::stream::empty().boxed(),
            }
            .run()
            .await;

            response_rx
                .into_stream()
                .map(|event| match event {
                    AccountStreamEvent::Item(AccountEvent {
                        kind: AccountEventKind::OrderCancelled(response),
                        ..
                    }) => format!(
                        "cancelled {} {}",
                        response.key.cid,
                        match response.state {
                            Ok(_) => "ok".to_string(),
                            Err(error) => format!("{error:?}"),
                        }
                    ),
                    AccountStreamEvent::Item(AccountEvent {
                        kind: AccountEventKind::OrderSnapshot(Snapshot(order)),
                        ..
                    }) => format!(
                        "snapshot {} {}",
                        order.key.cid,
                        match order.state {
                            OrderState::Active(_) => "active".to_string(),
                            OrderState::Inactive(state) => format!("{state:?}"),
                        }
                    ),
                    other => panic!("unexpected event: {other:?}"),
                })
                .collect()
                .await
        }

        fn dropped(cancel: &str) -> String {
            format!(
                "{:?}",
                InactiveOrderState::<AssetIndex, InstrumentIndex>::OpenFailed(
                    OrderError::NotPlaced(NotPlacedReason::CancelFailed {
                        cancel: ClientOrderId::new(cancel),
                    })
                )
            )
        }

        /// The open leaves only once the cancel has answered, and the cancel's answer is forwarded
        /// first. A plain open sent after the group is not held behind it. The request stream ends
        /// at once, so this also shows a drain waits for the held open.
        #[tokio::test(start_paused = true)]
        async fn the_opens_leave_only_after_the_cancel_has_answered() {
            let client = ScriptedClient::new(vec![("stop", 1, Answer::Cancelled)]);
            let events = run(
                &client,
                vec![
                    ExecutionRequest::CancelThenOpen(group(&["stop"], &["exit"])),
                    ExecutionRequest::Open(open("other")),
                ],
            )
            .await;

            // Which of the first two calls starts first is up to the manager's `select!`, which
            // polls its branches in a random order. Both start before the cancel answers.
            let logged = client.logged();
            let (first_two, rest) = logged.split_at(2);
            let mut first_two = first_two.to_vec();
            first_two.sort();
            assert_eq!(first_two, ["cancel stop", "open other"], "{logged:?}");
            assert_eq!(rest, ["cancel stop answered", "open exit"], "{logged:?}");
            assert_eq!(
                events,
                [
                    "snapshot other active",
                    "cancelled stop ok",
                    "snapshot exit active"
                ]
            );
        }

        /// With several cancels, the opens wait for the last answer. When cancels fail, none of
        /// the opens is sent, and each is reported dropped, naming the first cancel that failed,
        /// after every cancel's own answer.
        #[tokio::test(start_paused = true)]
        async fn a_failed_cancel_drops_every_open_naming_the_first_failure() {
            let client = ScriptedClient::new(vec![
                ("ok", 0, Answer::Cancelled),
                ("filled", 1, Answer::AlreadyFilled),
                ("filled-too", 2, Answer::AlreadyFilled),
            ]);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["ok", "filled", "filled-too"],
                    &["exit", "stop"],
                ))],
            )
            .await;

            assert!(
                !client.logged().iter().any(|call| call.starts_with("open")),
                "no open sent: {:?}",
                client.logged()
            );
            let failure = format!(
                "{:?}",
                OrderError::<AssetIndex, InstrumentIndex>::Rejected(
                    ApiError::OrderAlreadyFullyFilled
                )
            );
            assert_eq!(
                events,
                [
                    "cancelled ok ok".to_string(),
                    format!("cancelled filled {failure}"),
                    format!("cancelled filled-too {failure}"),
                    format!("snapshot exit {}", dropped("filled")),
                    format!("snapshot stop {}", dropped("filled")),
                ]
            );
        }

        /// A cancel the venue never answers times out, and the timeout fails the group.
        #[tokio::test(start_paused = true)]
        async fn a_timed_out_cancel_drops_the_opens() {
            let client = ScriptedClient::new(vec![("stop", 0, Answer::Hangs)]);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(client.logged(), ["cancel stop"]);
            assert_eq!(
                events,
                [
                    format!(
                        "cancelled stop {:?}",
                        OrderError::<AssetIndex, InstrumentIndex>::Connectivity(
                            ConnectivityError::Timeout
                        )
                    ),
                    format!("snapshot exit {}", dropped("stop")),
                ]
            );
        }

        /// `SendOpens` sends the opens once every cancel has answered, failed or not.
        #[tokio::test(start_paused = true)]
        async fn send_opens_sends_the_opens_after_a_failed_cancel() {
            let client = ScriptedClient::new(vec![("stop", 1, Answer::AlreadyFilled)]);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(
                    group(&["stop"], &["exit"]).with_on_cancel_failure(OnCancelFailure::SendOpens),
                )],
            )
            .await;

            assert_eq!(
                client.logged(),
                ["cancel stop", "cancel stop answered", "open exit"]
            );
            assert_eq!(events.len(), 2, "{events:?}");
            assert_eq!(events[1], "snapshot exit active");
        }

        /// A group with no cancel has nothing to wait on: its opens go out at once. The Engine
        /// refuses such a group, so this is a caller sending its own requests.
        #[tokio::test(start_paused = true)]
        async fn a_group_without_cancels_sends_its_opens_at_once() {
            let client = ScriptedClient::new(vec![]);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(&[], &["exit"]))],
            )
            .await;

            assert_eq!(client.logged(), ["open exit"]);
            assert_eq!(events, ["snapshot exit active"]);
        }

        /// `Shutdown` abandons a held open with everything else in flight: it is never sent, and
        /// nothing more is forwarded.
        #[tokio::test(start_paused = true)]
        async fn shutdown_abandons_held_opens() {
            let client = ScriptedClient::new(vec![("stop", 1, Answer::Cancelled)]);
            let events = run(
                &client,
                vec![
                    ExecutionRequest::CancelThenOpen(group(&["stop"], &["exit"])),
                    ExecutionRequest::Shutdown,
                ],
            )
            .await;

            assert!(
                !client.logged().iter().any(|call| call.starts_with("open")),
                "{:?}",
                client.logged()
            );
            assert!(events.is_empty(), "{events:?}");
        }

        /// Two groups are sequenced apart: one's failure drops only its own opens.
        #[tokio::test(start_paused = true)]
        async fn groups_are_sequenced_apart() {
            let client = ScriptedClient::new(vec![
                ("filled", 1, Answer::AlreadyFilled),
                ("ok", 2, Answer::Cancelled),
            ]);
            let events = run(
                &client,
                vec![
                    ExecutionRequest::CancelThenOpen(group(&["filled"], &["dropped"])),
                    ExecutionRequest::CancelThenOpen(group(&["ok"], &["sent"])),
                ],
            )
            .await;

            assert_eq!(
                client.logged(),
                [
                    "cancel filled",
                    "cancel ok",
                    "cancel filled answered",
                    "cancel ok answered",
                    "open sent"
                ]
            );
            assert_eq!(events[1], format!("snapshot dropped {}", dropped("filled")));
            assert_eq!(events[3], "snapshot sent active");
        }

        /// The answer to `cid`'s open as a connectivity error leaves it: in flight, unknown.
        fn unknown(cid: &str, error: ConnectivityError) -> String {
            format!(
                "snapshot {cid} {:?}",
                InactiveOrderState::<AssetIndex, InstrumentIndex>::OpenFailed(
                    OrderError::Connectivity(error)
                )
            )
        }

        /// A group of one cancel and one open goes as one venue request when the client offers
        /// one, carrying the group's policy. Each leg is answered from it, the cancel first, and a
        /// drain waits for the answer.
        #[tokio::test(start_paused = true)]
        async fn a_native_cancel_replace_answers_each_leg_from_one_request() {
            let client = ScriptedClient::native(Native::Both);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(client.logged(), ["cancel-replace stop exit DropOpens"]);
            assert_eq!(events, ["cancelled stop ok", "snapshot exit active"]);
        }

        /// A group's `SendOpens` reaches the client.
        #[tokio::test(start_paused = true)]
        async fn a_native_cancel_replace_carries_send_opens() {
            let client = ScriptedClient::native(Native::Both);
            run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(
                    group(&["stop"], &["exit"]).with_on_cancel_failure(OnCancelFailure::SendOpens),
                )],
            )
            .await;

            assert_eq!(client.logged(), ["cancel-replace stop exit SendOpens"]);
        }

        /// A group of more than one cancel, or more than one open, is never offered: the manager
        /// sequences it.
        #[tokio::test(start_paused = true)]
        async fn only_a_group_of_one_cancel_and_one_open_goes_native() {
            let client = ScriptedClient {
                script: vec![("a", 0, Answer::Cancelled), ("b", 0, Answer::Cancelled)],
                ..ScriptedClient::native(Native::Both)
            };
            run(
                &client,
                vec![
                    ExecutionRequest::CancelThenOpen(group(&["a", "b"], &["exit"])),
                    ExecutionRequest::CancelThenOpen(group(&["a"], &["one", "two"])),
                ],
            )
            .await;

            assert!(
                !client
                    .logged()
                    .iter()
                    .any(|call| call.starts_with("cancel-replace")),
                "{:?}",
                client.logged()
            );
        }

        /// A failed cancel whose venue did not attempt the open: the open is answered as dropped,
        /// naming the cancel, after the cancel's own answer.
        #[tokio::test(start_paused = true)]
        async fn a_native_cancel_failure_answers_the_open_as_dropped() {
            let client = ScriptedClient::native(Native::CancelFailed);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(
                events,
                [
                    format!(
                        "cancelled stop {:?}",
                        OrderError::<AssetIndex, InstrumentIndex>::Rejected(
                            ApiError::OrderAlreadyFullyFilled
                        )
                    ),
                    format!("snapshot exit {}", dropped("stop")),
                ]
            );
        }

        /// A cancel that succeeded with an open the venue rejected: each leg has its own answer.
        #[tokio::test(start_paused = true)]
        async fn a_native_open_rejection_is_the_opens_own_answer() {
            let client = ScriptedClient::native(Native::OpenRejected);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(
                events,
                [
                    "cancelled stop ok".to_string(),
                    format!(
                        "snapshot exit {:?}",
                        InactiveOrderState::<AssetIndex, InstrumentIndex>::OpenFailed(
                            OrderError::Rejected(ApiError::OrderRejected(
                                "insufficient balance".to_string()
                            ))
                        )
                    ),
                ]
            );
        }

        /// A lost answer leaves the open's outcome unknown, so the open is looked up, as one sent
        /// alone would be, and what the lookup finds follows.
        #[tokio::test(start_paused = true)]
        async fn a_lost_native_answer_looks_the_open_up() {
            let client = ScriptedClient::native(Native::Lost);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(
                client.logged(),
                ["cancel-replace stop exit DropOpens", "lookup exit"]
            );
            let socket = || ConnectivityError::Socket("dropped".to_string());
            assert_eq!(
                events,
                [
                    format!(
                        "cancelled stop {:?}",
                        OrderError::<AssetIndex, InstrumentIndex>::Connectivity(socket())
                    ),
                    unknown("exit", socket()),
                    "snapshot exit active".to_string(),
                ]
            );
        }

        /// A request the venue never answers times out: the cancel fails, and the open's outcome
        /// is unknown, so it is looked up.
        #[tokio::test(start_paused = true)]
        async fn a_timed_out_native_request_fails_the_cancel_and_looks_the_open_up() {
            let client = ScriptedClient::native(Native::Hangs);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(
                client.logged(),
                ["cancel-replace stop exit DropOpens", "lookup exit"]
            );
            assert_eq!(
                events,
                [
                    format!(
                        "cancelled stop {:?}",
                        OrderError::<AssetIndex, InstrumentIndex>::Connectivity(
                            ConnectivityError::Timeout
                        )
                    ),
                    unknown("exit", ConnectivityError::Timeout),
                    "snapshot exit active".to_string(),
                ]
            );
        }

        /// A client that says it did not attempt the open although its cancel succeeded breaks
        /// its contract. Nothing was placed by its word, so the open is still answered, as
        /// dropped.
        #[tokio::test(start_paused = true)]
        async fn an_open_skipped_after_a_good_cancel_is_still_answered() {
            let client = ScriptedClient::native(Native::OpenSkipped);
            let events = run(
                &client,
                vec![ExecutionRequest::CancelThenOpen(group(
                    &["stop"],
                    &["exit"],
                ))],
            )
            .await;

            assert_eq!(
                events,
                [
                    "cancelled stop ok".to_string(),
                    format!("snapshot exit {}", dropped("stop")),
                ]
            );
        }
    }
}
