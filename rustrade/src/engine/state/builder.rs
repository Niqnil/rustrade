use crate::{
    engine::state::{
        EngineState,
        asset::generate_empty_indexed_asset_states,
        connectivity::generate_empty_indexed_connectivity_states,
        instrument::generate_indexed_instrument_states,
        order::Orders,
        position::{OmsMode, PositionManager, PositionSeed, PositionSeedError},
        trading::TradingState,
    },
    statistic::summary::asset::BalanceBasis,
};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use rustrade_execution::balance::{AssetBalance, Balance};
use rustrade_instrument::{
    Keyed,
    asset::{AssetIndex, ExchangeAsset, name::AssetNameInternal},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex},
};
use rustrade_integration::collection::snapshot::Snapshot;
use tracing::debug;

/// Builder utility for an [`EngineState`] instance.
#[derive(Debug, Clone)]
pub struct EngineStateBuilder<'a, GlobalData, FnInstrumentData> {
    instruments: &'a IndexedInstruments,
    trading_state: Option<TradingState>,
    time_engine_start: Option<DateTime<Utc>>,
    global: GlobalData,
    balances: FnvHashMap<ExchangeAsset<AssetNameInternal>, Balance>,
    instrument_data_init: FnInstrumentData,
    /// OMS mode applied to every instrument's [`PositionManager`] at construction.
    ///
    /// Defaults to [`OmsMode::Netting`]. Use [`OmsMode::Hedging`] for strategies that hold
    /// simultaneous long and short positions on the same instrument (e.g. options writing).
    oms_mode: OmsMode,
    /// [`BalanceBasis`] applied to every asset's `TearSheetAssetGenerator` at construction.
    ///
    /// Defaults to [`BalanceBasis::Gross`] (no change for existing/cash users). Use
    /// [`BalanceBasis::NetAsset`] to compute drawdown and end-of-session balance from net asset
    /// value on margin accounts — see its docs for the net-must-stay-positive precondition.
    balance_basis: BalanceBasis,
    /// Venues with a registered execution client, if known — see
    /// [`EngineStateBuilder::execution_venues`].
    execution_venues: Option<FnvHashSet<ExchangeId>>,
    /// Open positions to place in the built state — see [`EngineStateBuilder::positions`].
    positions: Vec<PositionSeed>,
}

impl<'a, GlobalData, FnInstrumentData> EngineStateBuilder<'a, GlobalData, FnInstrumentData> {
    /// Construct a new `EngineStateBuilder` with a layout derived from [`IndexedInstruments`].
    ///
    /// Note that the rest of the [`EngineState`] data can be generated from defaults if that
    /// is all that is needed.
    ///
    /// Note that `ConnectivityStates` will be generated with
    /// [`generate_empty_indexed_connectivity_states`], defaulting to `Health::Reconnecting`.
    pub fn new(
        instruments: &'a IndexedInstruments,
        global: GlobalData,
        instrument_data_init: FnInstrumentData,
    ) -> Self {
        Self {
            instruments,
            time_engine_start: None,
            trading_state: None,
            global,
            balances: FnvHashMap::default(),
            instrument_data_init,
            oms_mode: OmsMode::Netting,
            balance_basis: BalanceBasis::default(),
            execution_venues: None,
            positions: Vec::new(),
        }
    }

    /// Declare which venues have a registered execution client.
    ///
    /// Only these venues are given an account connection to wait on. Without this, the account
    /// dimension is approximated from the instrument model — "is the execution venue of at least one
    /// instrument" — which is correct whenever every venue holding instruments is also traded on,
    /// and wrong for a venue that supplies prices without executing anything.
    ///
    /// That distinction matters for any configuration that prices an instrument on one venue and
    /// trades a *different* instrument on another: the pricing venue would otherwise be assigned
    /// [`VenueRole::Both`](crate::engine::state::connectivity::VenueRole::Both), wait forever on an
    /// account connection nothing will ever establish, and hold
    /// [`ConnectivityStates::global()`](crate::engine::state::connectivity::ConnectivityStates::global())
    /// at [`Health::Reconnecting`](crate::engine::state::connectivity::Health::Reconnecting) for the
    /// life of the run.
    ///
    /// [`SystemBuilder`](crate::system::builder::SystemBuilder) calls this for you from the
    /// execution clients it registers, and [`backtest`](crate::backtest::backtest) reconciles the
    /// roles itself against the clients it builds per run. Provide it by hand when constructing an
    /// `EngineState` for an engine you drive directly, where neither of those applies.
    pub fn execution_venues<Iter>(mut self, venues: Iter) -> Self
    where
        Iter: IntoIterator<Item = ExchangeId>,
    {
        self.execution_venues = Some(venues.into_iter().collect());
        self
    }

    /// Optionally provide the initial `TradingState`.
    ///
    /// Defaults to `TradingState::Disabled`.
    pub fn trading_state(self, value: TradingState) -> Self {
        Self {
            trading_state: Some(value),
            ..self
        }
    }

    /// Optionally provide the `time_engine_start`.
    ///
    /// Providing this is useful for back-test scenarios where the time should be seeded with a
    /// "historical" clock time (eg/ from first historical `MarketEvent`).
    ///
    /// Defaults to `Utc::now`
    pub fn time_engine_start(self, value: DateTime<Utc>) -> Self {
        Self {
            time_engine_start: Some(value),
            ..self
        }
    }

    /// Optionally set the [`OmsMode`] for all instrument [`PositionManager`]s.
    ///
    /// Defaults to [`OmsMode::Netting`] (at most one position per instrument, backward-compatible).
    /// Set to [`OmsMode::Hedging`] for strategies that simultaneously hold long and short
    /// positions on the same instrument (e.g. options writing alongside long positions).
    ///
    /// # Note — `OmsMode::Hedging` and non-option instruments
    ///
    /// `OmsMode` is applied uniformly to all instruments. Hedging mode is intended for
    /// instruments where multiple concurrent positions are semantically valid (e.g. individual
    /// options legs). Applying it to spot or futures instruments will track each order's fills
    /// as a separate position slot (keyed by order ID) rather than a single net position,
    /// which is almost certainly not what you want for those asset classes.
    pub fn oms_mode(self, mode: OmsMode) -> Self {
        Self {
            oms_mode: mode,
            ..self
        }
    }

    /// Optionally set the [`BalanceBasis`] for all asset `TearSheetAssetGenerator`s.
    ///
    /// Defaults to [`BalanceBasis::Gross`] (drawdown and end-of-session balance computed from gross
    /// `Balance::total`), which is unchanged behaviour for existing and cash users. Set to
    /// [`BalanceBasis::NetAsset`] to compute them from net asset value (`total - borrowed`) on
    /// margin accounts.
    ///
    /// # Precondition (`NetAsset`)
    /// Net-asset drawdown is only well-defined while net asset stays **strictly positive**; see
    /// [`BalanceBasis::NetAsset`] for the silent-failure modes when it is not.
    pub fn balance_basis(self, basis: BalanceBasis) -> Self {
        Self {
            balance_basis: basis,
            ..self
        }
    }

    /// Optionally provide initial exchange asset `Balance`s.
    ///
    /// Useful for back-test scenarios where seeding EngineState with initial `Balance`s is
    /// required.
    ///
    /// Note the internal implementation uses a `HashMap`, so duplicate
    /// `ExchangeAsset<AssetNameInternal>` keys are overwritten.
    pub fn balances<BalanceIter, KeyedBalance>(mut self, balances: BalanceIter) -> Self
    where
        BalanceIter: IntoIterator<Item = KeyedBalance>,
        KeyedBalance: Into<Keyed<ExchangeAsset<AssetNameInternal>, Balance>>,
    {
        self.balances.extend(balances.into_iter().map(|keyed| {
            let Keyed { key, value } = keyed.into();

            (key, value)
        }));
        self
    }

    /// Optionally provide open positions the engine starts with.
    ///
    /// The engine builds positions only from fills, so without this a position held across a
    /// process restart is invisible to it: the strategy cannot close it, and risk checks do not
    /// see it. Seed each one here, once, before the engine starts. Also useful for back-tests
    /// that start from an existing portfolio. See [`PositionSeed`] for what a seeded position
    /// holds, and why its entry price must come from the caller.
    ///
    /// Seeds are validated by [`Self::try_build`] against the [`OmsMode`] set with
    /// [`Self::oms_mode`], whichever order the two are called in. Repeated calls append.
    pub fn positions<Iter>(mut self, positions: Iter) -> Self
    where
        Iter: IntoIterator<Item = PositionSeed>,
    {
        self.positions.extend(positions);
        self
    }

    /// Use the builder data to generate the associated [`EngineState`].
    ///
    /// If optional data is not provided (eg/ Balances), default values are used (eg/ zero Balance).
    ///
    /// # Panics
    /// Panics if a [`PositionSeed`] provided via [`Self::positions`] is invalid — see
    /// [`Self::try_build`], which returns that as a [`PositionSeedError`] instead. A builder with
    /// no position seeds never panics.
    pub fn build<InstrumentData>(self) -> EngineState<GlobalData, InstrumentData>
    where
        FnInstrumentData: Fn(
            &'a Keyed<InstrumentIndex, Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
        ) -> InstrumentData,
    {
        #[allow(clippy::panic)] // Documented in this method's `# Panics` section.
        self.try_build()
            .unwrap_or_else(|error| panic!("failed to build EngineState: {error}"))
    }

    /// Use the builder data to generate the associated [`EngineState`], returning an error for
    /// an invalid [`PositionSeed`] instead of panicking.
    ///
    /// A seed is rejected if it names an instrument the builder was not given, has a
    /// non-positive quantity, targets a slot another seed already filled, or names a slot that
    /// does not fit the [`OmsMode`] — see [`PositionSeedError`].
    pub fn try_build<InstrumentData>(
        self,
    ) -> Result<EngineState<GlobalData, InstrumentData>, PositionSeedError>
    where
        FnInstrumentData: Fn(
            &'a Keyed<InstrumentIndex, Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
        ) -> InstrumentData,
    {
        let Self {
            instruments,
            time_engine_start,
            trading_state,
            global,
            balances,
            instrument_data_init,
            oms_mode,
            balance_basis,
            execution_venues,
            positions,
        } = self;

        // Default if not provided
        let time_engine_start = time_engine_start.unwrap_or_else(|| {
            debug!("EngineStateBuilder using Utc::now as time_engine_start default");
            Utc::now()
        });
        let trading = trading_state.unwrap_or_default();

        // Construct empty ConnectivityStates
        let connectivity =
            generate_empty_indexed_connectivity_states(instruments, execution_venues.as_ref());

        // Update empty AssetStates from provided exchange asset Balances
        let mut assets = generate_empty_indexed_asset_states(instruments, balance_basis);
        for (key, balance) in balances {
            assets
                .asset_mut(&key)
                .update_from_balance(Snapshot(&AssetBalance {
                    asset: key.asset,
                    balance,
                    time_exchange: time_engine_start,
                }))
        }

        // Generate empty InstrumentStates using provided FnInstrumentData etc.
        let mut instruments = generate_indexed_instrument_states(
            instruments,
            time_engine_start,
            move || PositionManager::new(oms_mode),
            Orders::default,
            instrument_data_init,
        );

        // Seed open positions into their instruments' PositionManagers
        for seed in positions {
            let Some(state) = instruments.0.get_mut(&seed.instrument) else {
                return Err(PositionSeedError::UnknownInstrument {
                    instrument: seed.instrument,
                });
            };
            let contract_size = state.instrument.kind.contract_size();
            let quote = state.instrument.underlying.quote;
            seed.seed_into(&mut state.position, state.key, quote, contract_size)?;
        }

        Ok(EngineState {
            trading,
            global,
            connectivity,
            assets,
            instruments,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        engine::state::{
            EngineState,
            connectivity::VenueRole,
            position::{PnlUnrealisedUpdate, PositionId},
        },
        statistic::{summary::TradingSummaryGenerator, time::Annual365},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::id::{ClientOrderId, OrderId, StrategyId},
        trade::{AssetFees, Trade, TradeId},
    };
    use rustrade_instrument::{
        Side, Underlying,
        asset::Asset,
        instrument::{
            Instrument,
            kind::{InstrumentKind, perpetual::PerpetualContract},
            name::InstrumentNameInternal,
            quote::InstrumentQuoteAsset,
        },
    };

    const SPOT: &str = "binance_spot_btc_usdt";
    const PERP: &str = "binance_futures_usd_btc_usdt_perp";

    /// A spot instrument (contract size 1) and a perpetual with contract size 10, so a seed's
    /// `contract_size` can be seen to come from its own instrument.
    fn seed_instruments() -> IndexedInstruments {
        IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                SPOT,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .add_instrument(Instrument::new(
                ExchangeId::BinanceFuturesUsd,
                PERP,
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Perpetual(PerpetualContract {
                    contract_size: dec!(10),
                    settlement_asset: Asset::new_from_exchange("usdt"),
                }),
                None,
            ))
            .build()
    }

    fn time(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn long_seed(instrument: &str, quantity_abs: Decimal) -> PositionSeed {
        PositionSeed::new(
            instrument,
            Side::Buy,
            quantity_abs,
            dec!(50_000),
            time(1_000),
        )
    }

    fn try_build(
        instruments: &IndexedInstruments,
        oms_mode: OmsMode,
        seeds: Vec<PositionSeed>,
    ) -> Result<EngineState<(), ()>, PositionSeedError> {
        EngineState::builder(instruments, (), |_| ())
            .oms_mode(oms_mode)
            .positions(seeds)
            .try_build()
    }

    /// End-to-end seam: `EngineStateBuilder::balance_basis(NetAsset)` rides the asset generators
    /// into the on-demand `TradingSummaryGenerator` (which clones `AssetState.statistics`) and is
    /// stamped onto the output `TradingSummary.basis`.
    #[test]
    fn balance_basis_reaches_output_trading_summary() {
        let instruments = IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                "binance_spot_btc_usdt",
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .build();

        let state: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ())
            .balance_basis(BalanceBasis::NetAsset)
            .build();

        // Summary generator is built on-demand by cloning the per-asset generators, so the basis
        // selected on the builder must surface on the generated summary.
        let mut generator = TradingSummaryGenerator::init(
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC,
            &state.instruments,
            &state.assets,
        );

        assert_eq!(generator.generate(Annual365).basis, BalanceBasis::NetAsset);
    }

    /// End-to-end seam for [`EngineStateBuilder::execution_venues`]: the declared set has to reach
    /// [`generate_empty_indexed_connectivity_states`] for the built state's roles to differ from
    /// the instrument-model approximation. The generator is unit-tested at its own seam; this pins
    /// the threading between the two, which is otherwise verified nowhere.
    #[test]
    fn declared_execution_venues_reach_the_built_connectivity_states() {
        const DATA: ExchangeId = ExchangeId::BinanceSpot;
        const EXECUTION: ExchangeId = ExchangeId::Coinbase;

        // Two-instrument pattern: `DATA` prices an instrument that is never traded, `EXECUTION`
        // trades a different one. Both are some instrument's `exchange`, so the instrument model
        // alone claims both hold an account.
        let instruments = IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                DATA,
                "data_venue_xau_usd",
                "XAUUSD",
                Underlying::new("xau", "usd"),
                None,
            ))
            .add_instrument(Instrument::spot(
                EXECUTION,
                "execution_venue_btc_usdt",
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .build();

        let declared: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ())
            .execution_venues([EXECUTION])
            .build();

        assert_eq!(
            declared.connectivity.connectivity(&DATA).role(),
            VenueRole::DataOnly,
            "the declared set is what says nothing executes on the pricing venue"
        );
        assert_eq!(
            declared.connectivity.connectivity(&EXECUTION).role(),
            VenueRole::Both
        );

        // Non-vacuous: without the declaration the same collection approximates `DATA` as `Both`,
        // so the assertion above can only pass if the set was actually threaded through.
        let approximated: EngineState<(), ()> =
            EngineState::builder(&instruments, (), |_| ()).build();

        assert_eq!(
            approximated.connectivity.connectivity(&DATA).role(),
            VenueRole::Both
        );
    }

    /// A netting seed lands in the netting slot with the documented starting values, and takes
    /// its `contract_size` and fee asset from its own instrument.
    #[test]
    fn netting_seed_opens_position_with_documented_starting_values() {
        let instruments = seed_instruments();
        let state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![
                long_seed(SPOT, dec!(0.5)),
                PositionSeed::new(PERP, Side::Sell, dec!(3), dec!(60_000), time(2_000)),
            ],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument(&InstrumentNameInternal::new(SPOT));
        let position = &spot.position.positions[&PositionId::NETTING];
        let quote = spot.instrument.underlying.quote;

        assert_eq!(spot.position.positions.len(), 1);
        assert_eq!(position.instrument, spot.key);
        assert_eq!(position.side, Side::Buy);
        assert_eq!(position.price_entry_average, dec!(50_000));
        assert_eq!(position.quantity_abs, dec!(0.5));
        assert_eq!(position.quantity_abs_max, dec!(0.5));
        assert_eq!(position.pnl_unrealised, Decimal::ZERO);
        assert_eq!(position.pnl_realised, Decimal::ZERO);
        assert_eq!(
            position.fees_enter,
            AssetFees::new(quote, Decimal::ZERO, Some(Decimal::ZERO))
        );
        assert_eq!(position.fees_exit, position.fees_enter);
        assert_eq!(position.time_enter, time(1_000));
        assert_eq!(position.time_exchange_update, time(1_000));
        assert!(position.trades.is_empty());
        assert_eq!(position.contract_size, Decimal::ONE);

        let perp = state
            .instruments
            .instrument(&InstrumentNameInternal::new(PERP));
        let position = &perp.position.positions[&PositionId::NETTING];
        assert_eq!(position.side, Side::Sell);
        assert_eq!(position.contract_size, dec!(10));
    }

    /// A seeded position behaves as one opened by a fill: a closing trade exits it, with realised
    /// PnL measured from the seeded entry price.
    #[test]
    fn seeded_netting_position_is_closed_by_a_fill() {
        let instruments = seed_instruments();
        let mut state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![long_seed(SPOT, dec!(0.5))],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument_mut(&InstrumentNameInternal::new(SPOT));
        let quote = spot.instrument.underlying.quote;
        let exited = spot
            .update_from_trade(&Trade {
                id: TradeId::new("close"),
                order_id: OrderId::new("close"),
                instrument: spot.key,
                strategy: StrategyId::new("strategy"),
                time_exchange: time(3_000),
                side: Side::Sell,
                price: dec!(52_000),
                quantity: dec!(0.5),
                order_filled_quantity: None,
                fees: AssetFees::new(quote, Decimal::ZERO, Some(Decimal::ZERO)),
            })
            .expect("a fill for the whole quantity exits the seeded position");

        assert_eq!(exited.position_id, PositionId::NETTING);
        assert_eq!(exited.side, Side::Buy);
        assert_eq!(exited.price_entry_average, dec!(50_000));
        assert_eq!(exited.pnl_realised, dec!(1_000));
        assert_eq!(exited.time_enter, time(1_000));
        assert!(spot.position.positions.is_empty());
    }

    /// Seeded entry fees are held in the quote asset and realised when paid, and unrealised PnL
    /// deducts an exit-fee estimate scaled from them, as for a position opened by a fill.
    #[test]
    fn seeded_entry_fees_are_realised_up_front() {
        let instruments = seed_instruments();
        let state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![long_seed(SPOT, dec!(0.5)).with_fees_enter(dec!(5))],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument(&InstrumentNameInternal::new(SPOT));
        let position = &spot.position.positions[&PositionId::NETTING];
        let quote = spot.instrument.underlying.quote;

        assert_eq!(
            position.fees_enter,
            AssetFees::new(quote, dec!(5), Some(dec!(5)))
        );
        assert_eq!(
            position.fees_exit,
            AssetFees::new(quote, Decimal::ZERO, Some(Decimal::ZERO))
        );
        assert_eq!(position.pnl_realised, dec!(-5));

        let mut position = position.clone();
        assert_eq!(
            position.update_pnl_unrealised(dec!(52_000)),
            PnlUnrealisedUpdate::Updated
        );
        // 0.5 * (52_000 - 50_000) = 1_000, less exit fees estimated at 5 * (0.5 / 0.5).
        assert_eq!(position.pnl_unrealised, dec!(995));
    }

    /// A net rebate on entry is seeded as negative fees, as a fill carries one, and starts
    /// realised PnL above zero.
    #[test]
    fn seeded_entry_rebate_starts_realised_pnl_above_zero() {
        let instruments = seed_instruments();
        let state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![long_seed(SPOT, dec!(0.5)).with_fees_enter(dec!(-0.25))],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument(&InstrumentNameInternal::new(SPOT));
        let position = &spot.position.positions[&PositionId::NETTING];

        assert_eq!(position.fees_enter.fees, dec!(-0.25));
        assert_eq!(position.pnl_realised, dec!(0.25));
    }

    /// A seed serialised without `fees_enter` still deserialises, with zero entry fees.
    #[test]
    fn seed_without_fees_enter_deserialises_with_zero_fees() {
        let seed = long_seed(SPOT, dec!(0.5));
        let mut json = serde_json::to_value(&seed).unwrap();
        json.as_object_mut()
            .unwrap()
            .remove("fees_enter")
            .expect("a serialised seed holds fees_enter");

        let deserialised: PositionSeed = serde_json::from_value(json).unwrap();

        assert_eq!(deserialised.fees_enter, Decimal::ZERO);
        assert_eq!(deserialised, seed);
    }

    /// Closing a position seeded with entry fees nets them out of its realised PnL, together with
    /// the closing fill's own fees.
    #[test]
    fn seeded_entry_fees_are_netted_out_at_close() {
        let instruments = seed_instruments();
        let mut state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![long_seed(SPOT, dec!(0.5)).with_fees_enter(dec!(5))],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument_mut(&InstrumentNameInternal::new(SPOT));
        let quote = spot.instrument.underlying.quote;
        let exited = spot
            .update_from_trade(&Trade {
                id: TradeId::new("close"),
                order_id: OrderId::new("close"),
                instrument: spot.key,
                strategy: StrategyId::new("strategy"),
                time_exchange: time(3_000),
                side: Side::Sell,
                price: dec!(52_000),
                quantity: dec!(0.5),
                order_filled_quantity: None,
                fees: AssetFees::new(quote, dec!(3), Some(dec!(3))),
            })
            .expect("a fill for the whole quantity exits the seeded position");

        // 0.5 * (52_000 - 50_000) = 1_000, less 5 entry and 3 exit fees.
        assert_eq!(exited.pnl_realised, dec!(992));
        assert_eq!(
            exited.fees_enter,
            AssetFees::new(quote, dec!(5), Some(dec!(5)))
        );
        assert_eq!(
            exited.fees_exit,
            AssetFees::new(quote, dec!(3), Some(dec!(3)))
        );
    }

    /// Under Hedging, seeds occupy the slots they name, and `oms_mode` may be set after
    /// `positions`: the seeds are checked against the mode at build time.
    #[test]
    fn hedging_seeds_occupy_named_slots_whatever_the_call_order() {
        let instruments = seed_instruments();
        let state: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ())
            .positions([
                long_seed(SPOT, dec!(1)).with_position_id(PositionId::new("a")),
                long_seed(SPOT, dec!(2)).with_position_id(PositionId::new("b")),
            ])
            .oms_mode(OmsMode::Hedging)
            .try_build()
            .unwrap();

        let positions = &state
            .instruments
            .instrument(&InstrumentNameInternal::new(SPOT))
            .position
            .positions;
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[&PositionId::new("a")].quantity_abs, dec!(1));
        assert_eq!(positions[&PositionId::new("b")].quantity_abs, dec!(2));
    }

    /// Under Hedging, a fill for an order submitted with a seeded slot's `PositionId` reduces and
    /// then closes that slot, as the `PositionSeed::position_id` rustdoc promises.
    #[test]
    fn seeded_hedging_slot_is_reduced_and_closed_by_its_orders_fills() {
        let instruments = seed_instruments();
        let mut state = try_build(
            &instruments,
            OmsMode::Hedging,
            vec![
                long_seed(SPOT, dec!(3)).with_position_id(PositionId::new("a")),
                long_seed(SPOT, dec!(5)).with_position_id(PositionId::new("b")),
            ],
        )
        .unwrap();

        let spot = state
            .instruments
            .instrument_mut(&InstrumentNameInternal::new(SPOT));
        let (key, quote) = (spot.key, spot.instrument.underlying.quote);

        // Routing the engine records when an order is submitted with `position_id: "a"`
        // and acknowledged under exchange order id "close_a".
        let cid = ClientOrderId::new("close_a");
        spot.position_ids.insert(cid.clone(), PositionId::new("a"));
        spot.exchange_id_to_cid.insert(OrderId::new("close_a"), cid);

        let sell = |id: &str, quantity| Trade {
            id: TradeId::new(id),
            order_id: OrderId::new("close_a"),
            instrument: key,
            strategy: StrategyId::new("strategy"),
            time_exchange: time(3_000),
            side: Side::Sell,
            price: dec!(52_000),
            quantity,
            order_filled_quantity: None,
            fees: AssetFees::new(quote, Decimal::ZERO, Some(Decimal::ZERO)),
        };

        let first = sell("fill_1", dec!(1));
        assert!(spot.update_from_trade(&first).is_none());
        assert_eq!(
            spot.position.positions[&PositionId::new("a")].quantity_abs,
            dec!(2)
        );

        let second = sell("fill_2", dec!(2));
        let exited = spot
            .update_from_trade(&second)
            .expect("the remaining quantity closes slot a");
        assert_eq!(exited.position_id, PositionId::new("a"));
        assert_eq!(exited.pnl_realised, dec!(6_000));

        // Slot b is untouched.
        assert_eq!(spot.position.positions.len(), 1);
        assert_eq!(
            spot.position.positions[&PositionId::new("b")].quantity_abs,
            dec!(5)
        );
    }

    /// Naming the netting slot explicitly is the same as naming none.
    #[test]
    fn netting_seed_may_name_the_netting_slot() {
        let instruments = seed_instruments();
        let state = try_build(
            &instruments,
            OmsMode::Netting,
            vec![long_seed(SPOT, dec!(1)).with_position_id(PositionId::NETTING)],
        )
        .unwrap();

        assert!(
            state
                .instruments
                .instrument(&InstrumentNameInternal::new(SPOT))
                .position
                .positions
                .contains_key(&PositionId::NETTING)
        );
    }

    #[test]
    fn invalid_seeds_are_rejected() {
        let instruments = seed_instruments();
        let spot = InstrumentNameInternal::new(SPOT);
        let cases = [
            (
                OmsMode::Netting,
                vec![long_seed("unknown", dec!(1))],
                PositionSeedError::UnknownInstrument {
                    instrument: InstrumentNameInternal::new("unknown"),
                },
            ),
            (
                OmsMode::Netting,
                vec![long_seed(SPOT, Decimal::ZERO)],
                PositionSeedError::NonPositiveQuantity {
                    instrument: spot.clone(),
                    quantity_abs: Decimal::ZERO,
                },
            ),
            (
                OmsMode::Netting,
                vec![long_seed(SPOT, dec!(-1))],
                PositionSeedError::NonPositiveQuantity {
                    instrument: spot.clone(),
                    quantity_abs: dec!(-1),
                },
            ),
            (
                OmsMode::Netting,
                vec![long_seed(SPOT, dec!(1)), long_seed(SPOT, dec!(2))],
                PositionSeedError::DuplicateSlot {
                    instrument: spot.clone(),
                    position_id: PositionId::NETTING,
                },
            ),
            (
                OmsMode::Netting,
                vec![long_seed(SPOT, dec!(1)).with_position_id(PositionId::new("a"))],
                PositionSeedError::PositionIdInNetting {
                    instrument: spot.clone(),
                    position_id: PositionId::new("a"),
                },
            ),
            (
                OmsMode::Hedging,
                vec![long_seed(SPOT, dec!(1))],
                PositionSeedError::MissingPositionIdInHedging {
                    instrument: spot.clone(),
                },
            ),
            (
                OmsMode::Hedging,
                vec![
                    long_seed(SPOT, dec!(1)).with_position_id(PositionId::new("a")),
                    long_seed(SPOT, dec!(2)).with_position_id(PositionId::new("a")),
                ],
                PositionSeedError::DuplicateSlot {
                    instrument: spot.clone(),
                    position_id: PositionId::new("a"),
                },
            ),
        ];

        for (oms_mode, seeds, expected) in cases {
            assert_eq!(
                try_build(&instruments, oms_mode, seeds).unwrap_err(),
                expected
            );
        }
    }

    #[test]
    #[should_panic(
        expected = "failed to build EngineState: position seed names unknown instrument"
    )]
    fn build_panics_on_an_invalid_seed() {
        let instruments = seed_instruments();
        let _: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ())
            .positions([long_seed("unknown", dec!(1))])
            .build();
    }
}
