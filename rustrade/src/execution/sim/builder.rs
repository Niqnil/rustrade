use crate::{
    engine::execution_tx::MultiExchangeTxMap,
    error::BarterError,
    execution::{
        builder::{generate_mock_exchange_instruments, validate_supported_instrument_kinds},
        request::ExecutionRequest,
        sim::SimVenue,
    },
};
use chrono::TimeDelta;
use fnv::FnvHashMap;
use rustrade_execution::{
    client::{
        ExecutionClient,
        mock::{MockExecution, MockExecutionConfig},
    },
    exchange::mock::SimulatedVenue,
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
};
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
};
use rustrade_integration::{
    channel::{UnboundedTx, mpsc_unbounded},
    collection::FnvIndexMap,
};
use std::sync::Arc;

/// Builds the execution half of a deterministic simulation: one [`SimulatedVenue`] per configured
/// exchange, plus the [`MultiExchangeTxMap`] the `Engine` routes requests through.
///
/// This is the deterministic counterpart to [`ExecutionBuilder`], and the difference is what it
/// does with each venue's request receiver. [`ExecutionBuilder`] hands it to an
/// [`ExecutionManager`] running on its own task, which is what makes response timing a function of
/// the tokio scheduler. This builder **keeps** it, so that [`SimRunner`] can drain it inline on the
/// engine's own thread and schedule what it produces in simulated time.
///
/// Nothing here is spawned, connected or awaited: [`build`](Self::build) is synchronous and
/// infallible once the configurations have been accepted.
///
/// [`ExecutionBuilder`]: crate::execution::builder::ExecutionBuilder
/// [`ExecutionManager`]: crate::execution::manager::ExecutionManager
/// [`SimRunner`]: crate::execution::sim::SimRunner
#[derive(Debug)]
pub struct SimExecutionBuilder<'a> {
    instruments: &'a IndexedInstruments,
    venues: FnvIndexMap<ExchangeIndex, SimVenue>,
    execution_txs: FnvHashMap<ExchangeId, (ExchangeIndex, UnboundedTx<ExecutionRequest>)>,
}

impl<'a> SimExecutionBuilder<'a> {
    /// Construct a new `SimExecutionBuilder` using the provided [`IndexedInstruments`].
    pub fn new(instruments: &'a IndexedInstruments) -> Self {
        Self {
            instruments,
            venues: FnvIndexMap::default(),
            execution_txs: FnvHashMap::default(),
        }
    }

    /// Adds a [`SimulatedVenue`] for the exchange the provided [`MockExecutionConfig`] mocks.
    ///
    /// # Latency
    /// `config.latency_ms` is split evenly into the two offsets [`SimRunner`] schedules with: half
    /// on the way to the venue, half on the way back. Both are **simulated** — no sleeping occurs
    /// anywhere on this path, so a run's wall-clock duration does not scale with the latency being
    /// modelled.
    ///
    /// # Errors
    /// Returns [`BarterError::ExecutionBuilder`] if any indexed instrument executed on this
    /// exchange has an [`InstrumentKind`] outside [`MockExecution::SUPPORTED_KINDS`], or if a
    /// configuration for this exchange has already been added. The kind check runs *before* the
    /// instrument projection below, which panics on a kind it cannot model — so an unbacktestable
    /// instrument set is a returned error here rather than an abort.
    ///
    /// # Panics
    /// Panics if an instrument references a settlement asset absent from the index, which
    /// [`IndexedInstruments`] construction already rules out.
    ///
    /// [`InstrumentKind`]: rustrade_instrument::instrument::kind::InstrumentKind
    /// [`MockExecution::SUPPORTED_KINDS`]: rustrade_execution::client::ExecutionClient::SUPPORTED_KINDS
    /// [`SimRunner`]: crate::execution::sim::SimRunner
    pub fn add_venue(mut self, config: MockExecutionConfig) -> Result<Self, BarterError> {
        let exchange = config.mocked_exchange;

        validate_supported_instrument_kinds(
            self.instruments,
            exchange,
            <MockExecution<fn() -> chrono::DateTime<chrono::Utc>> as ExecutionClient>::SUPPORTED_KINDS,
        )?;

        let instrument_map = generate_execution_instrument_map(self.instruments, exchange)?;
        let index = instrument_map.exchange.key;

        let (execution_tx, execution_rx) = mpsc_unbounded();

        if self
            .execution_txs
            .insert(exchange, (index, execution_tx))
            .is_some()
        {
            return Err(BarterError::ExecutionBuilder(format!(
                "SimExecutionBuilder does not support duplicate simulated venues: {exchange}"
            )));
        }

        // Halved, matching the async driver's split of one round trip into two legs. Integer
        // division truncates, so an odd `latency_ms` loses its final millisecond on each leg rather
        // than skewing one direction.
        let leg = TimeDelta::milliseconds((config.latency_ms / 2) as i64);

        let venue = SimVenue {
            // Market-driven: `SimRunner::poll_next` routes every source market event to this
            // venue before the `Engine` sees it, which is what lets it accept resting orders.
            venue: SimulatedVenue::new_market_driven(
                &config,
                generate_mock_exchange_instruments(self.instruments, exchange),
            ),
            indexer: AccountEventIndexer::new(Arc::new(instrument_map)),
            request_rx: execution_rx,
            to_venue: leg,
            from_venue: leg,
        };

        self.venues.insert(index, venue);

        Ok(self)
    }

    /// Consume this builder, returning the venues and the [`MultiExchangeTxMap`] to hand the
    /// `Engine`.
    ///
    /// Every exchange the [`IndexedInstruments`] knows about gets a slot; one with no simulated
    /// venue gets `None`, which is what keeps the [`ExchangeIndex`]es valid.
    ///
    /// # Panics
    /// Panics if an exchange's [`ExchangeIndex`] disagrees with the one
    /// [`generate_execution_instrument_map`] derived for it. That would misroute every request for
    /// that venue, so it is asserted rather than tolerated.
    pub fn build(mut self) -> SimExecutionBuild {
        let execution_tx_map = self
            .instruments
            .exchanges()
            .iter()
            .map(|exchange| {
                let Some((added_index, added_tx)) = self.execution_txs.remove(&exchange.value)
                else {
                    return (exchange.value, None);
                };

                assert_eq!(
                    exchange.key, added_index,
                    "execution ExchangeIndex != IndexedInstruments Keyed<ExchangeIndex, ExchangeId>"
                );

                (exchange.value, Some(added_tx))
            })
            .collect();

        SimExecutionBuild {
            execution_tx_map,
            venues: self.venues,
        }
    }
}

/// The execution half of a deterministic simulation, ready to be driven.
///
/// Pair `venues` with a market source to build a [`SimRunner`], and hand `execution_tx_map` to the
/// [`Engine`](crate::engine::Engine) that will feed it.
///
/// [`SimRunner`]: crate::execution::sim::SimRunner
#[derive(Debug)]
pub struct SimExecutionBuild {
    /// Routes the `Engine`'s [`ExecutionRequest`]s to the matching simulated venue.
    pub execution_tx_map: MultiExchangeTxMap,
    /// One entry per configured simulated venue, keyed by [`ExchangeIndex`].
    pub venues: FnvIndexMap<ExchangeIndex, SimVenue>,
}
