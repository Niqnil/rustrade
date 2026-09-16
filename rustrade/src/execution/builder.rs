use crate::{
    engine::{clock::EngineClock, execution_tx::MultiExchangeTxMap},
    error::BarterError,
    execution::{
        AccountStreamEvent, Execution, error::ExecutionError, manager::ExecutionManager,
        request::ExecutionRequest,
    },
    shutdown::AsyncShutdown,
};
use fnv::FnvHashMap;
use futures::{FutureExt, future::try_join_all};
use rustrade_data::streams::{
    consumer::STREAM_RECONNECTION_POLICY, reconnect::stream::ReconnectingStream,
};
use rustrade_execution::{
    UnindexedAccountEvent,
    client::{
        ExecutionClient,
        mock::{MockExecution, MockExecutionClientConfig, MockExecutionConfig},
    },
    exchange::mock::{MockExchange, request::MockExchangeRequest},
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
};
use rustrade_instrument::{
    Keyed, Underlying,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::{InstrumentKind, InstrumentKindDiscriminant, cfd::CfdContract},
        name::InstrumentNameExchange,
        spec::{InstrumentSpec, InstrumentSpecQuantity, OrderQuantityUnits},
    },
};
use rustrade_integration::channel::{Channel, UnboundedTx, mpsc_unbounded};
use std::{collections::BTreeSet, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    task::{JoinError, JoinHandle},
};

type ExecutionInitFuture =
    Pin<Box<dyn Future<Output = Result<(RunFuture, RunFuture), ExecutionError>> + Send>>;
type RunFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Full execution infrastructure builder.
///
/// Add Mock and Live [`ExecutionClient`] configurations and let the builder set up the required
/// infrastructure.
///
/// Once you have added all the configurations, call [`ExecutionBuilder::build`] to return the
/// full [`ExecutionBuild`]. Then calling [`ExecutionBuild::init`] will then initialise
/// the built infrastructure.
///
/// Handles:
/// - Building mock execution managers (mocks a specific exchange internally via the [`MockExchange`]).
/// - Building live execution managers, setting up an external connection to each exchange.
/// - Constructs a [`MultiExchangeTxMap`] with an entry for each mock/live execution manager.
/// - Combines all exchange account streams into a unified [`AccountStreamEvent`] `Stream`.
// `mock_exchange_futures` and `execution_init_futures` hold `Pin<Box<dyn Future>>`, which has no
// `Debug` to delegate to. A hand-written impl could only print a placeholder for the two fields
// that carry the builder's actual pending work, so it would say less than the type's name already does.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuilder<'a> {
    instruments: &'a IndexedInstruments,
    execution_txs: FnvHashMap<ExchangeId, (ExchangeIndex, UnboundedTx<ExecutionRequest>)>,
    merged_channel: Channel<AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,
    mock_exchange_futures: Vec<RunFuture>,
    execution_init_futures: Vec<ExecutionInitFuture>,
}

impl<'a> ExecutionBuilder<'a> {
    /// Construct a new `ExecutionBuilder` using the provided `IndexedInstruments`.
    pub fn new(instruments: &'a IndexedInstruments) -> Self {
        Self {
            instruments,
            execution_txs: FnvHashMap::default(),
            merged_channel: Channel::default(),
            mock_exchange_futures: Vec::default(),
            execution_init_futures: Vec::default(),
        }
    }

    /// Adds an [`ExecutionManager`] for a mocked exchange, setting up a [`MockExchange`]
    /// internally.
    ///
    /// The provided [`MockExecutionConfig`] is used to configure the [`MockExchange`] and provide
    /// the initial account state.
    ///
    /// # Errors
    /// Returns [`BarterError::ExecutionBuilder`] if any indexed instrument executed on
    /// `mocked_exchange` has an [`InstrumentKind`] other than `Spot` or `Cfd`, per
    /// [`MockExecution::SUPPORTED_KINDS`]. `MockExchange` models no expiry, funding or contract
    /// chain, so a `Perpetual`, `Future` or `Option` has no faithful projection. This is rejected
    /// here, at build time, rather than at first order — an unbacktestable instrument set is a
    /// configuration error, and deferring it would surface as a rejected order mid-run.
    ///
    /// # Panics
    /// Panics if an instrument references a settlement asset absent from the index, which
    /// [`IndexedInstruments`] construction already rules out.
    ///
    /// [`InstrumentKind`]: rustrade_instrument::instrument::kind::InstrumentKind
    /// [`MockExecution::SUPPORTED_KINDS`]: rustrade_execution::client::ExecutionClient::SUPPORTED_KINDS
    /// [`IndexedInstruments`]: rustrade_instrument::index::IndexedInstruments
    pub fn add_mock<Clock>(
        self,
        config: MockExecutionConfig,
        clock: Clock,
    ) -> Result<Self, BarterError>
    where
        Clock: EngineClock + Clone + Send + Sync + 'static,
    {
        const ACCOUNT_STREAM_CAPACITY: usize = 256;
        const DUMMY_EXECUTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = broadcast::channel(ACCOUNT_STREAM_CAPACITY);

        let mock_execution_client_config = MockExecutionClientConfig {
            mocked_exchange: config.mocked_exchange,
            clock: move || clock.time(),
            request_tx,
            event_rx,
        };

        // `add_execution` runs first so its supported-kind check rejects an unbacktestable
        // instrument set before `init_mock_exchange` projects it. The projection panics on a kind
        // the mock cannot model, so building it first would turn a returnable `Err` into an abort.
        // The two futures are collected into separate queues, so their relative order is
        // immaterial to the built infrastructure.
        let mut this = self.add_execution::<MockExecution<_>>(
            mock_execution_client_config.mocked_exchange,
            mock_execution_client_config,
            DUMMY_EXECUTION_REQUEST_TIMEOUT,
        )?;

        // Register MockExchange init Future
        let mock_exchange_future = this.init_mock_exchange(config, request_rx, event_tx);
        this.mock_exchange_futures.push(mock_exchange_future);

        Ok(this)
    }

    fn init_mock_exchange(
        &self,
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
    ) -> RunFuture {
        let instruments =
            generate_mock_exchange_instruments(self.instruments, config.mocked_exchange);
        Box::pin(MockExchange::new(config, request_rx, event_tx, instruments).run())
    }

    /// Adds an [`ExecutionManager`] for a live exchange.
    pub fn add_live<Client>(
        self,
        config: Client::Config,
        request_timeout: Duration,
    ) -> Result<Self, BarterError>
    where
        Client: ExecutionClient + Send + Sync + 'static,
        Client::AccountStream: Send,
        Client::Config: Send,
    {
        self.add_execution::<Client>(Client::EXCHANGE, config, request_timeout)
    }

    fn add_execution<Client>(
        mut self,
        exchange: ExchangeId,
        config: Client::Config,
        request_timeout: Duration,
    ) -> Result<Self, BarterError>
    where
        Client: ExecutionClient + Send + Sync + 'static,
        Client::AccountStream: Send,
        Client::Config: Send,
    {
        validate_supported_instrument_kinds(self.instruments, exchange, Client::SUPPORTED_KINDS)?;

        let instrument_map = generate_execution_instrument_map(self.instruments, exchange)?;

        let (execution_tx, execution_rx) = mpsc_unbounded();

        if self
            .execution_txs
            .insert(exchange, (instrument_map.exchange.key, execution_tx))
            .is_some()
        {
            return Err(BarterError::ExecutionBuilder(format!(
                "ExecutionBuilder does not support duplicate mocked ExecutionManagers: {exchange}"
            )));
        }

        let merged_tx = self.merged_channel.tx.clone();

        // Init ExecutionManager Future
        let future_result = ExecutionManager::init(
            execution_rx.into_stream(),
            request_timeout,
            Arc::new(Client::new(config)),
            AccountEventIndexer::new(Arc::new(instrument_map)),
            STREAM_RECONNECTION_POLICY,
        );

        let future_result = future_result.map(|result| {
            result.map(|(manager, account_stream)| {
                let manager_future: RunFuture = Box::pin(manager.run());
                let stream_future: RunFuture = Box::pin(account_stream.forward_to(merged_tx));

                (manager_future, stream_future)
            })
        });

        self.execution_init_futures.push(Box::pin(future_result));

        Ok(self)
    }

    /// Consume this `ExecutionBuilder` and build a full [`ExecutionBuild`] containing all the
    /// [`ExecutionManager`] (mock & live) and [`MockExchange`] futures.
    ///
    /// **For most users, calling [`ExecutionBuild::init`] after this is satisfactory.**
    ///
    /// If you want more control over what runtime drives the futures to completion, you can
    /// call [`ExecutionBuild::init_with_runtime`].
    pub fn build(mut self) -> ExecutionBuild {
        // Construct indexed ExecutionTx map
        let execution_tx_map = self
            .instruments
            .exchanges()
            .iter()
            .map(|exchange| {
                // If IndexedInstruments execution not used for execution, add None to map
                let Some((added_execution_exchange_index, added_execution_exchange_tx)) =
                    self.execution_txs.remove(&exchange.value)
                else {
                    return (exchange.value, None);
                };

                assert_eq!(
                    exchange.key, added_execution_exchange_index,
                    "execution ExchangeIndex != IndexedInstruments Keyed<ExchangeIndex, ExchangeId>"
                );

                // If execution has been added, add Some(ExecutionTx) to map
                (exchange.value, Some(added_execution_exchange_tx))
            })
            .collect();

        ExecutionBuild {
            execution_tx_map,
            account_channel: self.merged_channel,
            futures: ExecutionBuildFutures {
                mock_exchange_run_futures: self.mock_exchange_futures,
                execution_init_futures: self.execution_init_futures,
            },
        }
    }
}

/// Container holding execution infrastructure components ready to be initialised.
///
/// Call [`ExecutionBuild::init`] to run all the required execution component futures on tokio
/// tasks - returns the [`MultiExchangeTxMap`] and multi-exchange [`AccountStreamEvent`] stream.
// Holds an `ExecutionBuildFutures`, whose boxed futures have no `Debug` — see below.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuild {
    pub execution_tx_map: MultiExchangeTxMap,
    pub account_channel: Channel<AccountStreamEvent>,
    pub futures: ExecutionBuildFutures,
}

impl ExecutionBuild {
    /// Initialises all execution components on the current tokio runtime.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runners tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Returns the `MultiExchangeTxMap` and multi-exchange AccountStream.
    pub async fn init(self) -> Result<Execution, BarterError> {
        self.init_internal(tokio::runtime::Handle::current()).await
    }

    /// Initialises all execution components on the provided tokio runtime.
    ///
    /// Use this method if you want more control over which tokio runtime handles running
    /// execution components.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runners tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Returns the `MultiExchangeTxMap` and multi-exchange AccountStream.
    pub async fn init_with_runtime(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<Execution, BarterError> {
        self.init_internal(runtime).await
    }

    async fn init_internal(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<Execution, BarterError> {
        let handles = self.futures.init_with_runtime(runtime).await?;

        Ok(Execution {
            execution_txs: self.execution_tx_map,
            account_channel: self.account_channel,
            handles,
        })
    }
}

// Both fields are collections of `Pin<Box<dyn Future>>`, which has no `Debug` to delegate to.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuildFutures {
    pub mock_exchange_run_futures: Vec<RunFuture>,
    pub execution_init_futures: Vec<ExecutionInitFuture>,
}

impl ExecutionBuildFutures {
    /// Initialises all execution components on the current tokio runtime.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runner tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Spawns tokio tasks to forward AccountStreams to multi-exchange AccountStream
    pub async fn init(self) -> Result<ExecutionHandles, BarterError> {
        self.init_internal(tokio::runtime::Handle::current()).await
    }

    /// Initialises all execution components on the provided tokio runtime.
    ///
    /// Use this method if you want more control over which tokio runtime handles running
    /// execution components.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runner tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Spawns tokio tasks to forward AccountStreams to multi-exchange AccountStream
    pub async fn init_with_runtime(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<ExecutionHandles, BarterError> {
        self.init_internal(runtime).await
    }

    async fn init_internal(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<ExecutionHandles, BarterError> {
        let mock_exchanges = self
            .mock_exchange_run_futures
            .into_iter()
            .map(|mock_exchange_run_future| runtime.spawn(mock_exchange_run_future))
            .collect();

        // Await ExecutionManager build futures and ensure success
        let (managers, account_to_engines) =
            futures::future::try_join_all(self.execution_init_futures)
                .await?
                .into_iter()
                .map(|(manager_run_future, account_event_forward_future)| {
                    (
                        runtime.spawn(manager_run_future),
                        runtime.spawn(account_event_forward_future),
                    )
                })
                .unzip();

        Ok(ExecutionHandles {
            mock_exchanges,
            managers,
            account_to_engines,
        })
    }
}

#[derive(Debug)]
pub struct ExecutionHandles {
    pub mock_exchanges: Vec<JoinHandle<()>>,
    pub managers: Vec<JoinHandle<()>>,
    pub account_to_engines: Vec<JoinHandle<()>>,
}

impl AsyncShutdown for ExecutionHandles {
    type Result = Result<(), JoinError>;

    async fn shutdown(&mut self) -> Self::Result {
        let handles = self
            .mock_exchanges
            .drain(..)
            .chain(self.managers.drain(..))
            .chain(self.account_to_engines.drain(..));

        try_join_all(handles).await?;
        Ok(())
    }
}

impl IntoIterator for ExecutionHandles {
    type Item = JoinHandle<()>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.mock_exchanges
            .into_iter()
            .chain(self.managers)
            .chain(self.account_to_engines)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Project indexed instruments onto the `MockExchange`'s own instrument representation.
///
/// # Supported [`InstrumentKind`]s
/// `MockExchange` models `Spot` and `Cfd`, and **panics** on any other kind. This is a capability
/// limit of the mock — it fills at price × quantity with no expiry, settlement or contract chain —
/// not a statement about which kinds are executable in general.
///
/// The same limit is declared as `MockExecution::SUPPORTED_KINDS`, which
/// [`ExecutionBuilder::add_mock`] checks first, so through the builder an unsupported kind is a
/// returned error and this panic is unreachable. Keep the two in step: widening `SUPPORTED_KINDS`
/// without widening the match below converts that error back into a panic.
///
/// `Cfd` is supported because the mock accounts for a cash-settled position directly: it applies
/// `contract_size` to the notional and the fee, and debits the **quote** asset in both directions.
/// Without it every CFD-quoted dataset would panic at execution-build time and be unbacktestable.
///
/// `settlement_asset` is re-resolved to its exchange name so the projected instrument describes
/// itself faithfully, but the mock does not settle in it — see [`MockExchange`]'s limitations for
/// why, and for the balances a caller must fund.
///
/// [`MockExchange`]: rustrade_execution::exchange::mock::MockExchange
// Invariant: IndexedInstruments - all referenced assets exist; panics for unsupported InstrumentKind
#[allow(clippy::unwrap_used)]
pub(crate) fn generate_mock_exchange_instruments(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    instruments
        .instruments()
        .iter()
        .filter_map(
            |Keyed {
                 key: _,
                 value: instrument,
             }| {
                if instrument.exchange.value != exchange {
                    return None;
                }

                let Instrument {
                    exchange,
                    name_internal,
                    name_exchange,
                    underlying,
                    quote,
                    kind,
                    spec,
                    // Bound explicitly rather than via `..` so a future `Instrument` field forces a
                    // decision here instead of being silently dropped from the projection. See the
                    // construction below for why this one is.
                    data_venue: _,
                } = instrument;

                let kind = match kind {
                    InstrumentKind::Spot => InstrumentKind::Spot,
                    // A CFD is cash-settled in an account currency that is routinely not the quote
                    // asset, so the settlement asset is re-resolved to its exchange name here
                    // rather than dropped.
                    InstrumentKind::Cfd(contract) => InstrumentKind::Cfd(CfdContract {
                        contract_size: contract.contract_size,
                        settlement_asset: instruments
                            .find_asset(contract.settlement_asset)
                            .unwrap()
                            .asset
                            .name_exchange
                            .clone(),
                    }),
                    // Unreachable through `ExecutionBuilder`: `add_execution` rejects a kind
                    // outside `MockExecution::SUPPORTED_KINDS` before this projection runs. Kept as
                    // an assertion rather than a silent skip so that a `SUPPORTED_KINDS` widened
                    // past what this match can project fails loudly here instead of dropping the
                    // instrument from the mock exchange, which would look like a venue that simply
                    // never fills it.
                    unsupported => {
                        panic!("MockExchange does not support: {unsupported:?}")
                    }
                };

                let spec = match spec {
                    Some(spec) => {
                        let InstrumentSpec {
                            price,
                            quantity:
                                InstrumentSpecQuantity {
                                    unit,
                                    min,
                                    increment,
                                },
                            notional,
                        } = spec;

                        let unit = match unit {
                            OrderQuantityUnits::Asset(asset) => {
                                let quantity_asset = instruments
                                    .find_asset(*asset)
                                    .unwrap()
                                    .asset
                                    .name_exchange
                                    .clone();
                                OrderQuantityUnits::Asset(quantity_asset)
                            }
                            OrderQuantityUnits::Contract => OrderQuantityUnits::Contract,
                            OrderQuantityUnits::Quote => OrderQuantityUnits::Quote,
                        };

                        Some(InstrumentSpec {
                            price: *price,
                            quantity: InstrumentSpecQuantity {
                                unit,
                                min: *min,
                                increment: *increment,
                            },
                            notional: *notional,
                        })
                    }
                    None => None,
                };

                let underlying_base = instruments
                    .find_asset(underlying.base)
                    .unwrap()
                    .asset
                    .name_exchange
                    .clone();

                let underlying_quote = instruments
                    .find_asset(underlying.quote)
                    .unwrap()
                    .asset
                    .name_exchange
                    .clone();

                let instrument = Instrument {
                    exchange: exchange.value,
                    name_internal: name_internal.clone(),
                    name_exchange: name_exchange.clone(),
                    underlying: Underlying {
                        base: underlying_base,
                        quote: underlying_quote,
                    },
                    quote: *quote,
                    kind,
                    spec,
                    // Deliberately dropped. This projection describes the instrument to the
                    // `MockExchange`, which fills orders — a `DataVenue` states where *prices* are
                    // sourced from, which the mock neither reads nor can act on. Carrying it here
                    // would imply the mock fills against that venue, which it does not.
                    data_venue: None,
                };

                Some((instrument.name_exchange.clone(), instrument))
            },
        )
        .collect()
}

/// Rejects an instrument set containing a kind the execution client for `exchange` cannot trade.
///
/// An [`ExecutionClient`] addresses instruments by [`InstrumentNameExchange`] and never inspects
/// their [`InstrumentKind`], so an unsupported kind cannot fail at the type level. Left unchecked
/// it surfaces at the first order, as a venue rejection or — where the wrong contract is nameable
/// on the venue — as a fill against something the strategy did not intend to trade. Both are worse
/// than refusing to build.
///
/// Only instruments whose **execution** venue is `exchange` are considered. An instrument merely
/// priced on `exchange` (see [`Instrument::data_exchange`]) is never ordered through this client,
/// so its kind is not this client's concern.
///
/// [`Instrument::data_exchange`]: rustrade_instrument::instrument::Instrument::data_exchange
pub(crate) fn validate_supported_instrument_kinds(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
    supported: &[InstrumentKindDiscriminant],
) -> Result<(), BarterError> {
    // BTreeSet so a set with many offending instruments reports each distinct kind once, in a
    // deterministic order.
    let unsupported = instruments
        .instruments()
        .iter()
        .filter(|keyed| keyed.value.exchange.value == exchange)
        .map(|keyed| keyed.value.kind.discriminant())
        .filter(|kind| !supported.contains(kind))
        .collect::<BTreeSet<_>>();

    if unsupported.is_empty() {
        return Ok(());
    }

    Err(BarterError::ExecutionBuilder(format!(
        "{exchange} execution client cannot trade instrument kind(s): {}. It supports: {}",
        join_kinds(unsupported.iter().copied()),
        join_kinds(supported.iter().copied()),
    )))
}

fn join_kinds(kinds: impl Iterator<Item = InstrumentKindDiscriminant>) -> String {
    kinds
        .map(|kind| kind.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::engine::clock::HistoricalClock;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{AccountSnapshot, client::mock::MockExecutionConfig};
    use rustrade_instrument::{
        asset::Asset,
        index::builder::IndexedInstrumentsBuilder,
        instrument::{
            data_venue::DataVenue, kind::future::FutureContract, quote::InstrumentQuoteAsset,
        },
        test_utils::asset,
    };

    const EXCHANGE: ExchangeId = ExchangeId::LseCfd;

    /// A venue that only ever prices the fixture instrument.
    const DATA_VENUE: ExchangeId = ExchangeId::LseEquities;

    fn at(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    /// A single `spx500_usd` instrument of the given kind, registered on [`EXCHANGE`].
    fn instruments(kind: InstrumentKind<Asset>) -> IndexedInstruments {
        build_instruments(kind, None)
    }

    /// As [`instruments`], but priced on [`DATA_VENUE`] while still executed on [`EXCHANGE`], so
    /// the two venues are distinguishable.
    fn instruments_priced_on_another_venue(kind: InstrumentKind<Asset>) -> IndexedInstruments {
        build_instruments(kind, Some(DATA_VENUE))
    }

    fn build_instruments(
        kind: InstrumentKind<Asset>,
        data_venue: Option<ExchangeId>,
    ) -> IndexedInstruments {
        let instrument = Instrument::new(
            EXCHANGE,
            "spx500_usd",
            "spx500_usd",
            Underlying::new(asset("spx500"), asset("usd")),
            InstrumentQuoteAsset::UnderlyingQuote,
            kind,
            None,
        );

        let instrument = match data_venue {
            Some(exchange) => instrument.with_data_venue(DataVenue::new_same_name(exchange)),
            None => instrument,
        };

        IndexedInstrumentsBuilder::default()
            .add_instrument(instrument)
            .build()
    }

    /// A CFD settling in an asset that is **not** the quote — a GBP-denominated account trading a
    /// USD-quoted index CFD, which is the case that makes re-resolving the settlement asset
    /// load-bearing rather than incidental.
    fn cfd() -> InstrumentKind<Asset> {
        InstrumentKind::Cfd(CfdContract {
            contract_size: dec!(25),
            settlement_asset: asset("gbp"),
        })
    }

    fn mock_config() -> MockExecutionConfig {
        MockExecutionConfig {
            mocked_exchange: EXCHANGE,
            initial_state: AccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![],
                instruments: vec![],
            },
            latency_ms: 0,
            fee_model: Default::default(),
            fill_model: Default::default(),
        }
    }

    /// A CFD survives the projection with its multiplier intact and its settlement asset
    /// re-resolved to the exchange-facing name — not dropped, and not silently replaced by the
    /// quote asset.
    #[test]
    fn mock_instruments_map_a_cfd_preserving_contract_size_and_settlement_asset() {
        let instruments = instruments(cfd());

        let mocked = generate_mock_exchange_instruments(&instruments, EXCHANGE);

        let instrument = mocked
            .get(&InstrumentNameExchange::from("spx500_usd"))
            .expect("the CFD instrument must be projected onto the mock exchange");

        let InstrumentKind::Cfd(contract) = &instrument.kind else {
            panic!("expected a Cfd kind, got {:?}", instrument.kind)
        };
        assert_eq!(contract.contract_size, dec!(25));
        assert_eq!(
            contract.settlement_asset,
            AssetNameExchange::from("gbp"),
            "settlement is the account currency, not the quote asset"
        );
        assert_eq!(instrument.underlying.quote, AssetNameExchange::from("usd"));
    }

    /// The regression that made CFD-quoted datasets unbacktestable: the projection ran during
    /// `add_mock`, so a CFD instrument panicked at execution-build time — before a single event was
    /// replayed. Asserting on `add_mock` rather than the private projection is deliberate; that is
    /// the call site a backtest actually reaches.
    #[test]
    fn add_mock_accepts_a_cfd_instrument() {
        let instruments = instruments(cfd());

        ExecutionBuilder::new(&instruments)
            .add_mock(
                mock_config(),
                HistoricalClock::new(at("2025-03-24T22:00:00Z")),
            )
            .expect("building mock execution over a CFD instrument must succeed");
    }

    #[test]
    fn mock_instruments_map_spot_unchanged() {
        let instruments = instruments(InstrumentKind::Spot);

        let mocked = generate_mock_exchange_instruments(&instruments, EXCHANGE);

        let instrument = mocked
            .get(&InstrumentNameExchange::from("spx500_usd"))
            .unwrap();
        assert_eq!(instrument.kind, InstrumentKind::Spot);
    }

    /// The projection's own assertion, reached here by calling it directly. Through
    /// `ExecutionBuilder` this input is rejected earlier — see
    /// `add_mock_rejects_an_unsupported_kind_without_panicking` — so this pins the inner guard
    /// against a future `SUPPORTED_KINDS` that outgrows what the match can project.
    #[test]
    #[should_panic(expected = "MockExchange does not support")]
    fn mock_instruments_panic_on_an_unsupported_kind() {
        let instruments = instruments(future());

        let _ = generate_mock_exchange_instruments(&instruments, EXCHANGE);
    }

    /// Instruments on another venue are filtered out before the kind match, so an unsupported kind
    /// elsewhere in the registry cannot panic a mock that does not serve it.
    #[test]
    fn mock_instruments_exclude_other_exchanges() {
        let instruments = instruments(cfd());

        let mocked = generate_mock_exchange_instruments(&instruments, ExchangeId::BinanceSpot);

        assert!(mocked.is_empty());
    }

    fn future() -> InstrumentKind<Asset> {
        InstrumentKind::Future(FutureContract {
            contract_size: dec!(1),
            settlement_asset: asset("usd"),
            expiry: at("2025-06-27T00:00:00Z"),
        })
    }

    /// The behaviour `SUPPORTED_KINDS` exists to produce: a kind the mock cannot model is a
    /// returnable configuration error, not an abort. Asserting on `add_mock` rather than the
    /// validator is deliberate — it pins the ordering against `init_mock_exchange`, whose
    /// projection still panics on the same input.
    #[test]
    fn add_mock_rejects_an_unsupported_kind_without_panicking() {
        let instruments = instruments(future());

        // `ExecutionBuilder` has no `Debug`, so the success arm cannot be unwrapped via
        // `expect_err`.
        let Err(error) = ExecutionBuilder::new(&instruments).add_mock(
            mock_config(),
            HistoricalClock::new(at("2025-03-24T22:00:00Z")),
        ) else {
            panic!("a Future instrument is not backtestable on MockExchange")
        };

        let BarterError::ExecutionBuilder(message) = error else {
            panic!("expected an ExecutionBuilder error, got {error:?}")
        };
        assert!(
            message.contains("future"),
            "the error must name the offending kind, got: {message}"
        );
        // Asserted on the tail alone, after the "It supports: " marker. `EXCHANGE` renders as
        // "lse_cfd" and the message leads with it, so a bare `contains("cfd")` over the whole
        // string passes on the venue name even if the client declared no `Cfd` support at all.
        let supported = message
            .split_once("It supports: ")
            .expect("the error must name the supported kinds")
            .1;
        assert!(
            supported.contains("spot") && supported.contains("cfd"),
            "the error must name the kinds that would work, got: {message}"
        );
    }

    /// Only the *execution* venue's instruments are the client's concern. An instrument executed
    /// elsewhere but priced here must not be judged against this client's capabilities — otherwise
    /// adding a data venue could break an execution setup that trades nothing on it.
    #[test]
    fn validation_ignores_instruments_executed_on_another_exchange() {
        // The instrument is priced on `DATA_VENUE` and executed on `EXCHANGE`, so the two
        // predicates disagree: filtering on `data_exchange()` instead of `exchange` would reject
        // this build. A fixture with no `DataVenue` cannot tell them apart, since its
        // `data_exchange()` IS its execution venue.
        let instruments = instruments_priced_on_another_venue(future());

        validate_supported_instrument_kinds(
            &instruments,
            DATA_VENUE,
            &[InstrumentKindDiscriminant::Spot],
        )
        .expect("the Future is executed on EXCHANGE, not on the venue being validated");

        // The other half of the same rule: at its execution venue it very much is this client's
        // concern, so the filter must not be inert.
        validate_supported_instrument_kinds(
            &instruments,
            EXCHANGE,
            &[InstrumentKindDiscriminant::Spot],
        )
        .expect_err("the Future is executed on EXCHANGE, so it must be judged there");
    }

    /// Each distinct offending kind is reported once, in a deterministic order, rather than once
    /// per instrument.
    #[test]
    fn validation_reports_each_unsupported_kind_once() {
        let instruments = IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::new(
                EXCHANGE,
                "a",
                "a",
                Underlying::new(asset("spx500"), asset("usd")),
                InstrumentQuoteAsset::UnderlyingQuote,
                future(),
                None,
            ))
            .add_instrument(Instrument::new(
                EXCHANGE,
                "b",
                "b",
                Underlying::new(asset("ftse100"), asset("usd")),
                InstrumentQuoteAsset::UnderlyingQuote,
                future(),
                None,
            ))
            .build();

        let error = validate_supported_instrument_kinds(
            &instruments,
            EXCHANGE,
            &[InstrumentKindDiscriminant::Spot],
        )
        .expect_err("both instruments are Futures on a spot-only client");

        let BarterError::ExecutionBuilder(message) = error else {
            panic!("expected an ExecutionBuilder error, got {error:?}")
        };
        assert_eq!(
            message.matches("future").count(),
            1,
            "the offending kind must be deduplicated, got: {message}"
        );
    }
}
