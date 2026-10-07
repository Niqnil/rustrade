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
    reconnect::stream::{
        ReconnectingStream, ReconnectionBackoffPolicy, ReinitFailure, init_reconnecting_stream,
    },
};
use rustrade_execution::{
    AccountEvent, AccountEventKind,
    client::ExecutionClient,
    error::{AccountReinitFailure, AccountStreamInitError, ConnectivityError, OrderError},
    indexer::{AccountEventIndexer, IndexedAccountStream},
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
use std::{fmt::Debug, sync::Arc};
use tracing::{error, info};

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
    /// re-initialises it with `reconnect_policy`'s backoff, for as long as it runs. Each failed
    /// attempt is yielded as an [`AccountEventKind::ReinitFailed`], with no further
    /// `Reconnecting`. Whether to stop waiting is the caller's decision.
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
        // A failed re-init is sent in-band, in order with the account events, so the consumer can
        // tell a reconnect that keeps failing from one still in its backoff.
        let exchange = indexer.map.exchange.key;
        let account_stream = Box::pin(
            account_stream
                .with_reconnect_backoff_reporting(reconnect_policy, stream_key)
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
            id::{ClientOrderId, StrategyId},
            request::{RequestCancel, RequestOpen},
            state::Open,
        },
        trade::TradesRead,
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
                state: RequestOpen {
                    side: Side::Sell,
                    price: None,
                    quantity: dec!(1),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    position_id: None,
                    reduce_only: false,
                    market: None,
                },
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
                Err(UnindexedClientError::Connectivity(
                    ConnectivityError::Timeout,
                ))
            } else if attempt < self.open_from {
                Ok(futures::stream::empty().boxed())
            } else {
                Ok(futures::stream::pending().boxed())
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
            _: DateTime<Utc>,
            _: DateTime<Utc>,
            _: &[InstrumentNameExchange],
        ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>
        {
            unreachable!("the account stream does not fetch trades")
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
        let instruments = IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")]);
        let Ok(map) = generate_execution_instrument_map(&instruments, EXCHANGE) else {
            panic!("the instrument map should build");
        };
        ExecutionManager::init(
            futures::stream::pending(),
            std::time::Duration::from_secs(5),
            Arc::new(client),
            AccountEventIndexer::new(Arc::new(map)),
            ReconnectionBackoffPolicy::new(1, 1, 1),
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
}
