use crate::execution::{
    AccountStreamEvent,
    error::ExecutionError,
    request::{ExecutionRequest, RequestFuture},
};
use derive_more::Constructor;
use futures::{
    FutureExt, Stream, StreamExt,
    future::Either,
    stream::{BoxStream, FuturesUnordered},
};
use rustrade_data::streams::{
    consumer::StreamKey,
    reconnect::stream::{ReconnectingStream, ReconnectionBackoffPolicy, init_reconnecting_stream},
};
use rustrade_execution::{
    AccountEvent, AccountEventKind,
    client::ExecutionClient,
    error::{ConnectivityError, OrderError},
    indexer::{AccountEventIndexer, IndexedAccountStream},
    map::ExecutionInstrumentMap,
    order::{
        Order,
        request::{
            OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, UnindexedOrderResponseCancel,
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
use std::{fmt::Debug, sync::Arc};
use tracing::{error, info, warn};

/// Per-exchange execution manager that actions order requests from the Engine and forwards back
/// responses.
///
/// Processes indexed Engine [`ExecutionRequest`]s by:
/// - Transforming the requests to use the associated exchange's asset and instrument names.
/// - Issues the request via it's associated exchange [`ExecutionClient`],
/// - Tracks requests and returns timeouts to the Engine where necessary.
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
    pub async fn init(
        request_stream: RequestStream,
        request_timeout: std::time::Duration,
        client: Arc<Client>,
        indexer: AccountEventIndexer,
        reconnect_policy: ReconnectionBackoffPolicy,
    ) -> Result<(Self, impl Stream<Item = AccountStreamEvent> + Send), ExecutionError> {
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
        let account_stream = init_reconnecting_stream(move || {
            let client = client_clone.clone();
            let indexer = indexer_clone.clone();
            async move {
                // Allocate AssetNameExchanges & InstrumentNameExchanges to avoid lifetime issues
                let assets = indexer.map.exchange_assets().cloned().collect::<Vec<_>>();
                let instruments = indexer
                    .map
                    .exchange_instruments()
                    .cloned()
                    .collect::<Vec<_>>();

                // Initialise AccountStream & apply indexing
                let updates = Self::init_indexed_account_stream(
                    &client,
                    indexer.clone(),
                    &assets,
                    &instruments,
                )
                .await?;

                // Fetch AccountSnapshot & index
                let snapshot =
                    Self::fetch_indexed_account_snapshot(&client, &indexer, &assets, &instruments)
                        .await?;

                // It's expected downstream consumers (eg/ EngineState will sync updates)
                Ok(futures::stream::once(std::future::ready(snapshot)).chain(updates))
            }
        })
        .await?;

        // Construct channel to communicate ExecutionRequest responses (ie/ AccountEvents) to Engine
        let (response_tx, response_rx) = mpsc_unbounded();

        // Boxed so the manager can poll it inline in `run`'s `select!` -- see `account_stream`.
        let account_stream = Box::pin(
            account_stream
                .with_reconnect_backoff::<_, ExecutionError>(reconnect_policy, stream_key)
                .with_reconnection_events(indexer.map.exchange.value),
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
    ) -> Result<AccountEvent, ExecutionError> {
        match client.account_snapshot(assets, instruments).await {
            Ok(snapshot) => {
                let indexed_snapshot = indexer.snapshot(snapshot)?;
                Ok(AccountEvent {
                    exchange: indexer.map.exchange.key,
                    kind: AccountEventKind::Snapshot(indexed_snapshot),
                })
            }
            Err(error) => Err(ExecutionError::Client(indexer.client_error(error)?)),
        }
    }

    async fn init_indexed_account_stream(
        client: &Arc<Client>,
        indexer: AccountEventIndexer,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<impl Stream<Item = AccountEvent> + use<RequestStream, Client>, ExecutionError> {
        let stream = match client.account_stream(assets, instruments).await {
            Ok(stream) => stream,
            Err(error) => return Err(ExecutionError::Client(indexer.client_error(error)?)),
        };

        Ok(
            IndexedAccountStream::new(stream, indexer).filter_map(|result| {
                std::future::ready(match result {
                    Ok(indexed_event) => Some(indexed_event),
                    Err(error) => {
                        error!(
                            ?error,
                            "filtered IndexError produced by IndexedAccountStream"
                        );
                        None
                    }
                })
            }),
        )
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
                response_cancel = next_cancel_response => {
                    match response_cancel {
                        Ok(Some(response)) => {
                            let event = match self.process_cancel_response(response) {
                                Ok(indexed_event) => indexed_event,
                                Err(error) => {
                                    warn!(
                                        exchange = %self.indexer.map.exchange.value,
                                        ?error,
                                        "ExecutionManager filtering cancel response due to unrecognised index"
                                    );
                                    continue
                                }
                            };

                            if self.response_tx.send(event).is_err() {
                                break;
                            }
                        }
                        Err(request) => {
                            let event = Self::process_cancel_timeout(request);

                            if self.response_tx.send(event).is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            // Do nothing
                        }
                    };
                },

                // Process next ExecutionRequest::Open response
                response_open = next_open_response => {
                    match response_open {
                        Ok(Some(response)) => {
                            let event = match self.process_open_response(response) {
                                Ok(indexed_event) => indexed_event,
                                Err(error) => {
                                    warn!(
                                        exchange = %self.indexer.map.exchange.value,
                                        ?error,
                                        "ExecutionManager filtering open response due to unrecognised index"
                                    );
                                    continue
                                }
                            };

                            if self.response_tx.send(event).is_err() {
                                break;
                            }
                        }
                        Err(request) => {
                            let event = Self::process_open_timeout(request);

                            if self.response_tx.send(event).is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            // Do nothing
                        }
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

    fn process_cancel_response(
        &self,
        order: UnindexedOrderResponseCancel,
    ) -> Result<AccountStreamEvent, IndexError> {
        let order = self.indexer.order_response_cancel(order)?;

        Ok(AccountStreamEvent::Item(AccountEvent {
            exchange: order.key.exchange,
            kind: AccountEventKind::OrderCancelled(order),
        }))
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

    fn process_open_response(
        &self,
        order: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    ) -> Result<AccountStreamEvent, IndexError> {
        let Order {
            key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        } = order;

        let key = self.indexer.order_key(key)?;
        let state = self.indexer.order_state(state)?;

        Ok(AccountStreamEvent::Item(AccountEvent {
            exchange: key.exchange,
            kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
                key,
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state,
            })),
        }))
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
