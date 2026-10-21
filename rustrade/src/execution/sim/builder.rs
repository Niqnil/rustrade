use crate::{
    engine::{execution_tx::MultiExchangeTxMap, state::position::PositionSeed},
    error::BarterError,
    execution::{
        builder::{generate_mock_exchange_instruments, validate_supported_instrument_kinds},
        request::ExecutionRequest,
        sim::{SimStart, SimStartError, SimVenue},
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
    fill::FillModel,
    holding_cost::HoldingCostModel,
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
    margin::MarginInterestModel,
    shortability::ShortabilityProvider,
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

/// What a simulated venue models beyond its [`MockExecutionConfig`]: the parts that are behaviour
/// or data rather than settings, and so cannot live in a serialisable configuration.
///
/// Passed to [`SimExecutionBuilder::add_venue_with_options`]. The default models nothing extra,
/// which is what [`SimExecutionBuilder::add_venue`] uses.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct SimVenueOptions {
    /// What holding a position on the venue costs over time — see
    /// [`SimulatedVenue::with_holding_costs`]. Charged at each model's boundaries as they fall,
    /// since [`SimRunner`](crate::execution::sim::SimRunner) wakes the venue for each one.
    ///
    /// Shared rather than owned, so the models, and the rate histories they carry, can be built
    /// once for a whole sweep of backtests.
    pub holding_costs: Vec<Arc<dyn HoldingCostModel>>,
    /// What the venue may lend for a CFD short, and when a short-sale restriction is in effect —
    /// see [`SimulatedVenue::with_shortability`]. `None` lends without limit.
    pub shortability: Option<Arc<dyn ShortabilityProvider>>,
    /// Prices each taker fill in place of the configuration's
    /// [`fill_model`](MockExecutionConfig::fill_model) — see
    /// [`SimulatedVenue::with_fill_model`]. `None` uses the configured one. The configured
    /// [`slippage`](MockExecutionConfig::slippage) applies to either. Like every option here, it
    /// is not part of the serialisable configuration.
    pub fill_model: Option<Arc<dyn FillModel>>,
    /// What the venue's spot margin loans cost in interest — see
    /// [`SimulatedVenue::with_margin_interest`]. `None` charges none. Needs
    /// [`MockExecutionConfig::spot_margin`]: without it the venue takes no loans, and adding the
    /// venue is refused.
    pub margin_interest: Option<Arc<dyn MarginInterestModel>>,
}

impl SimVenueOptions {
    /// These options, also charging what `model` says holding a position costs.
    #[must_use]
    pub fn with_holding_cost(mut self, model: Arc<dyn HoldingCostModel>) -> Self {
        self.holding_costs.push(model);
        self
    }

    /// These options, refusing a CFD short that `provider` does not allow.
    #[must_use]
    pub fn with_shortability(mut self, provider: Arc<dyn ShortabilityProvider>) -> Self {
        self.shortability = Some(provider);
        self
    }

    /// These options, pricing each taker fill with `model` — see [`FillModel`] for what an
    /// implementation must uphold.
    #[must_use]
    pub fn with_fill_model(mut self, model: Arc<dyn FillModel>) -> Self {
        self.fill_model = Some(model);
        self
    }

    /// These options, charging interest on the venue's spot margin loans as `model` says — see
    /// [`MarginInterestModel`].
    #[must_use]
    pub fn with_margin_interest(mut self, model: Arc<dyn MarginInterestModel>) -> Self {
        self.margin_interest = Some(model);
        self
    }
}

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
    pub fn add_venue(self, config: MockExecutionConfig) -> Result<Self, BarterError> {
        self.add_venue_with_options(config, SimVenueOptions::default())
    }

    /// Adds a [`SimulatedVenue`] for the exchange `config` mocks, modelling what `options` add
    /// to it — see [`SimVenueOptions`].
    ///
    /// Otherwise exactly [`add_venue`](Self::add_venue), whose latency, errors and panics apply.
    ///
    /// # Errors
    /// Also returns [`BarterError::ExecutionBuilder`] if `options` carries a
    /// [`margin_interest`](SimVenueOptions::margin_interest) model while `config` has no
    /// [`spot_margin`](MockExecutionConfig::spot_margin).
    pub fn add_venue_with_options(
        self,
        config: MockExecutionConfig,
        options: SimVenueOptions,
    ) -> Result<Self, BarterError> {
        self.insert_venue(config, options, None)
    }

    /// Add the venue `config` mocks, recording `position_seeds` as the positions it starts with.
    fn insert_venue(
        mut self,
        config: MockExecutionConfig,
        options: SimVenueOptions,
        position_seeds: Option<Vec<PositionSeed>>,
    ) -> Result<Self, BarterError> {
        let SimVenueOptions {
            holding_costs,
            shortability,
            fill_model,
            margin_interest,
        } = options;

        let exchange = config.mocked_exchange;

        if margin_interest.is_some() && config.spot_margin.is_none() {
            return Err(BarterError::ExecutionBuilder(format!(
                "simulated venue {exchange} has a margin interest model but no spot_margin: a \
                 cash account takes no loans to charge"
            )));
        }

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

        // Market-driven: `SimRunner::poll_next` routes every source market event to this venue
        // before the `Engine` sees it, which is what lets it accept resting orders.
        let mut venue = SimulatedVenue::new_market_driven(
            &config,
            generate_mock_exchange_instruments(self.instruments, exchange),
        )
        .with_holding_costs(holding_costs);
        if let Some(provider) = shortability {
            venue = venue.with_shortability(provider);
        }
        if let Some(model) = fill_model {
            venue = venue.with_fill_model(model);
        }
        if let Some(model) = margin_interest {
            venue = venue.with_margin_interest(model);
        }

        let venue = SimVenue {
            venue,
            indexer: AccountEventIndexer::new(Arc::new(instrument_map)),
            request_rx: execution_rx,
            to_venue: leg,
            from_venue: leg,
            position_seeds,
        };

        self.venues.insert(index, venue);

        Ok(self)
    }

    /// Adds a [`SimulatedVenue`] for the exchange `config` mocks, starting from `start`: its
    /// opening account is derived from `start`, and so are the positions the `Engine` must be
    /// seeded with, which [`SimExecutionBuild::position_seeds`] returns. See [`SimStart`] for how
    /// each side is derived.
    ///
    /// Otherwise exactly [`add_venue_with_options`](Self::add_venue_with_options).
    ///
    /// # Errors
    /// Those of [`add_venue`](Self::add_venue), and [`BarterError::SimStart`] if `start` cannot
    /// be held by this venue — see [`SimStartError`],
    /// including when `config.initial_state` is not empty, since there would then be two
    /// starting states.
    ///
    /// [`SimStart`]: crate::execution::sim::SimStart
    pub fn add_venue_with_start(
        self,
        mut config: MockExecutionConfig,
        options: SimVenueOptions,
        start: SimStart,
    ) -> Result<Self, BarterError> {
        let exchange = config.mocked_exchange;

        if !config.initial_state.balances.is_empty() || !config.initial_state.instruments.is_empty()
        {
            return Err(SimStartError::InitialStateNotEmpty { exchange }.into());
        }

        // Before the projection below, which panics on a kind it cannot model.
        validate_supported_instrument_kinds(
            self.instruments,
            exchange,
            <MockExecution<fn() -> chrono::DateTime<chrono::Utc>> as ExecutionClient>::SUPPORTED_KINDS,
        )?;

        let (initial_state, seeds) = start.resolve(
            exchange,
            &generate_mock_exchange_instruments(self.instruments, exchange),
        )?;
        config.initial_state = initial_state;

        self.insert_venue(config, options, Some(seeds))
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

impl SimExecutionBuild {
    /// Every position the venues were started with by a
    /// [`SimStart`], for [`EngineStateBuilder::positions`], so
    /// the `Engine` starts holding what the venues hold.
    ///
    /// Empty when no venue was added with
    /// [`add_venue_with_start`](SimExecutionBuilder::add_venue_with_start).
    ///
    /// [`EngineStateBuilder::positions`]: crate::engine::state::builder::EngineStateBuilder::positions
    pub fn position_seeds(&self) -> Vec<PositionSeed> {
        self.venues
            .values()
            .filter_map(|slot| slot.position_seeds.as_ref())
            .flatten()
            .cloned()
            .collect()
    }
}
