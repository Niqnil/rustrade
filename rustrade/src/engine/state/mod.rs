use crate::engine::{
    Processor,
    state::{
        asset::{AssetStates, filter::AssetFilter},
        builder::EngineStateBuilder,
        connectivity::{ConnectivityStates, UntrackedExchange},
        instrument::{
            FillsApplied, InstrumentStates, data::InstrumentDataState, filter::InstrumentFilter,
            generate_unindexed_instrument_account_snapshot,
        },
        position::PositionDrift,
        trading::TradingState,
    },
};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_data::event::MarketEvent;
use rustrade_execution::{
    AccountEvent, AccountEventKind, AccountSnapshot, UnindexedAccountSnapshot,
    balance::AssetBalance,
    market::MarketSnapshot,
    order::{id::ClientOrderId, request::ReduceOnly},
    trade::TradeId,
};
use rustrade_instrument::{
    Keyed,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex, kind::InstrumentKind},
};
use rustrade_integration::collection::{one_or_many::OneOrMany, snapshot::Snapshot};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{debug, error, info, warn};

/// Asset-centric state and associated state management logic.
pub mod asset;

/// Connectivity state that tracks global connection health as well as the status of market data
/// and account connections for each exchange.
pub mod connectivity;

/// Instrument-level state and associated state management logic.
pub mod instrument;

/// Fees charged in a third asset, and how the engine values them in quote.
pub mod fee;

/// Defines a synchronous `OrderManager` that tracks the lifecycle of exchange orders.
pub mod order;

/// Position data structures and their associated state management logic.
pub mod position;

/// Defines the `TradingState` of the `Engine` (ie/ trading enabled & trading disabled), and it's
/// update logic.
pub mod trading;

/// [`EngineState`] builder utility.
pub mod builder;

/// Defines a default `GlobalData` implementation that can be used for systems which require no
/// specific global data.
pub mod global;

/// Supplies the market state an order request is stamped with as it is sent.
///
/// The `Engine` samples this for each open request it emits, and the result travels with the order
/// on [`RequestOpen::market`](rustrade_execution::order::request::RequestOpen::market). It exists so
/// a simulated venue — which owns no book — has a price to fill against; live venues ignore it.
///
/// # Why the `Engine` samples it rather than the venue
///
/// The market feed is forwarded into an unbounded, unpaced channel by an independent task, so a
/// venue reading that feed itself would race ahead of the engine and fill orders against prices
/// from arbitrarily far in the future. The instant the engine emits a request is the one point with
/// a well-defined position on the simulated timeline, so that is where the sample is taken.
///
/// # Implementing this
///
/// [`EngineState`] implements it already, delegating to
/// [`InstrumentDataState::market_snapshot`], so the standard engine needs nothing. Implement it on
/// a custom `State` to make simulated fills work there too; returning `None` keeps the pre-existing
/// behaviour, in which a simulated venue can price a limit order and must reject a market one.
///
/// # Type Parameters
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait MarketSnapshotSource<InstrumentKey = InstrumentIndex> {
    /// Market state for `key` right now, or `None` if this state tracks none for it.
    fn market_snapshot(&self, key: &InstrumentKey) -> Option<MarketSnapshot>;
}

impl<GlobalData, InstrumentData> MarketSnapshotSource<InstrumentIndex>
    for EngineState<GlobalData, InstrumentData>
where
    InstrumentData: InstrumentDataState,
{
    /// Delegates to the instrument's own [`InstrumentDataState::market_snapshot`], or returns
    /// `None` if `key` is not a tracked instrument.
    ///
    /// The `Engine` never asks for an untracked one: it rejects an order request for an instrument
    /// this state does not track before stamping it (see [`TracksInstrument`]).
    fn market_snapshot(&self, key: &InstrumentIndex) -> Option<MarketSnapshot> {
        self.instruments
            .get_index(key)
            .map(|state| state.data.market_snapshot())
    }
}

/// Reports whether a `State` tracks an instrument.
///
/// Order requests reach the `Engine` from code it does not control — an
/// [`AlgoStrategy`](crate::strategy::algo::AlgoStrategy), a
/// [`ClosePositionsStrategy`](crate::strategy::close_positions::ClosePositionsStrategy), or a
/// [`Command`](crate::engine::command::Command) — and any of them can carry a key the `Engine` was
/// not built with. The `Engine` checks each request against this before sending it, and rejects
/// one for an untracked instrument as
/// [`RecoverableEngineError::UnknownInstrument`](crate::engine::error::RecoverableEngineError::UnknownInstrument)
/// in the action output's `errors`, so nothing reaches the venue that the state could not record.
///
/// The check runs before anything else on the request, including the lookup of its exchange's
/// execution channel, so a request naming both an unknown instrument and an unknown exchange is
/// rejected as an unknown instrument. It does not check that the instrument belongs to that
/// exchange.
///
/// # Implementing this
/// Returning `true` for a key promises that this state's
/// [`InFlightRequestRecorder`](order::in_flight_recorder::InFlightRequestRecorder) and
/// [`MarketSnapshotSource`] accept it: the `Engine` records a request only after it has been sent,
/// so a recorder that panics on a key this reported as tracked fails after the venue has the
/// request.
///
/// # Type Parameters
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait TracksInstrument<InstrumentKey = InstrumentIndex> {
    /// Whether this state holds `key`.
    fn tracks_instrument(&self, key: &InstrumentKey) -> bool;
}

impl<GlobalData, InstrumentData> TracksInstrument<InstrumentIndex>
    for EngineState<GlobalData, InstrumentData>
{
    /// Whether `key` resolves through [`InstrumentStates::get_index`].
    ///
    /// An index from another `IndexedInstruments` set can resolve to a different instrument here,
    /// which no lookup can detect.
    fn tracks_instrument(&self, key: &InstrumentIndex) -> bool {
        self.instruments.get_index(key).is_some()
    }
}

/// Reports whether a `State` tracks an order under a client order id.
///
/// A [`ClientOrderId`] names one order at a time, and the `Engine` keys the orders it tracks for an
/// instrument on it. An open request under an id an order it tracks already holds would make the
/// engine's record of that order ambiguous, and a venue refuses one anyway (see
/// [`ApiError::DuplicateClientOrderId`]). So the `Engine` checks each open request against this,
/// after [`TracksInstrument`], and rejects one under a tracked id as
/// [`RecoverableEngineError::DuplicateClientOrderId`](crate::engine::error::RecoverableEngineError::DuplicateClientOrderId)
/// in the action output's `errors`, unsent and unrecorded. It rejects a second open in one batch
/// under the same instrument and id in the same way.
///
/// An id is free again once the `Engine` stops tracking its order, which is when the order ends.
/// The check is per instrument, as the tracking is: an id in use on another instrument is left to
/// the venue, which rejects it if it keys ids across instruments.
///
/// # Implementing this
/// Return `true` for every id under which the state's
/// [`InFlightRequestRecorder`](order::in_flight_recorder::InFlightRequestRecorder) already holds an
/// order for `instrument`. The `Engine` asks only about an instrument [`TracksInstrument`] reported
/// as tracked.
///
/// [`ApiError::DuplicateClientOrderId`]: rustrade_execution::error::ApiError::DuplicateClientOrderId
pub trait TracksOrder<InstrumentKey = InstrumentIndex> {
    /// Whether this state tracks an order for `instrument` under `cid`.
    fn tracks_order(&self, instrument: &InstrumentKey, cid: &ClientOrderId) -> bool;
}

impl<GlobalData, InstrumentData> TracksOrder<InstrumentIndex>
    for EngineState<GlobalData, InstrumentData>
{
    /// Whether the instrument's [`Orders`](order::Orders) track an order under `cid`.
    fn tracks_order(&self, instrument: &InstrumentIndex, cid: &ClientOrderId) -> bool {
        self.instruments
            .get_index(instrument)
            .is_some_and(|state| state.orders.0.contains_key(cid))
    }
}

/// Algorithmic trading `Engine` state.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Constructor)]
pub struct EngineState<GlobalData, InstrumentData> {
    /// Current `TradingState` of the `Engine`.
    pub trading: TradingState,

    /// Configurable `GlobalData` state.
    pub global: GlobalData,

    /// Global connection [`Health`](connectivity::Health), and health of the market data and
    /// account connections for each exchange.
    pub connectivity: ConnectivityStates,

    /// State of every asset (eg/ "btc", "usdt", etc.) being tracked by the `Engine`.
    pub assets: AssetStates,

    /// State of every instrument (eg/ "okx_spot_btc_usdt", "bybit_perpetual_btc_usdt", etc.)
    /// being tracked by the `Engine`.
    pub instruments: InstrumentStates<InstrumentData, ExchangeIndex, AssetIndex, InstrumentIndex>,
}

impl<GlobalData, InstrumentData> EngineState<GlobalData, InstrumentData> {
    /// Whether any instrument has an order awaiting a response from its exchange.
    ///
    /// This is the `Engine`'s own record of what it is owed: entries are created only for requests
    /// that were successfully *sent* (`record_in_flight_opens(opens.sent_iter())`), so a request
    /// the engine failed to send leaves nothing behind to wait on.
    ///
    /// See [`Orders::has_request_in_flight`](order::Orders::has_request_in_flight) for what counts
    /// as in flight.
    ///
    /// # Not a shutdown signal
    /// This deliberately does **not** decide when a [`Shutdown::AfterDrain`] run has finished. A
    /// response clears an order from flight, but the `Trade` and balance that the fill consists of
    /// are delivered separately and may still be unread, so quiescence here is reached while the
    /// run is genuinely incomplete. The `ExecutionManager`s own that decision instead.
    ///
    /// [`Shutdown::AfterDrain`]: crate::shutdown::Shutdown::AfterDrain
    pub fn has_requests_in_flight(&self) -> bool {
        self.instruments
            .0
            .values()
            .any(|instrument| instrument.orders.has_request_in_flight())
    }

    /// Construct an [`EngineStateBuilder`] to assist with `EngineState` initialisation.
    pub fn builder<FnInstrumentData>(
        instruments: &IndexedInstruments,
        global: GlobalData,
        instrument_data_init: FnInstrumentData,
    ) -> EngineStateBuilder<'_, GlobalData, FnInstrumentData>
    where
        FnInstrumentData: Fn(
            &Keyed<InstrumentIndex, Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
        ) -> InstrumentData,
    {
        EngineStateBuilder::new(instruments, global, instrument_data_init)
    }

    /// Updates the internal state from an exchange's AccountStream reporting that it is
    /// reconnecting.
    ///
    /// Marks the exchange's account connectivity as reconnecting, then has every instrument on it
    /// record the orders it holds `Open`
    /// ([`InstrumentState::begin_account_resync`](instrument::InstrumentState::begin_account_resync)).
    /// The account snapshot the reconnect produces may retire only those orders, which is what
    /// keeps an order accepted while the venue was being re-read from being mistaken for one that
    /// has gone.
    ///
    /// # Errors
    /// Returns [`UntrackedExchange`] if the exchange has no
    /// `ConnectivityState`, having mutated nothing.
    pub fn update_from_account_reconnecting(
        &mut self,
        exchange: &ExchangeId,
    ) -> Result<(), UntrackedExchange> {
        let index = self
            .connectivity
            .update_from_account_reconnecting(exchange)?;

        for instrument in self
            .instruments
            .instruments_mut(&InstrumentFilter::exchanges([index]))
        {
            instrument.begin_account_resync();
        }

        Ok(())
    }

    /// The base `asset` the account holds outside every spot position on its exchange: what is
    /// left of its balance once its debt and the positions are taken out, capped at what is free
    /// to trade. `None` while this state holds no balance for `asset`.
    ///
    /// It is the balance's `total`, less [`MarginDetails`](rustrade_execution::balance::MarginDetails)
    /// `borrowed` and `interest` where the venue reports debt, less the signed net quantity
    /// ([`PositionManager::quantity_net`](position::PositionManager::quantity_net)) of every
    /// [`InstrumentKind::Spot`] instrument whose base is `asset`, then capped at the balance's
    /// `free`. An [`AssetIndex`] names an asset on one exchange, so only that exchange's
    /// instruments count. A derivative's position is counted in contracts, not held in the base,
    /// so it is not taken out.
    ///
    /// Signed. It is negative when the account owes base outside its positions, or the positions
    /// count more base than it holds net of debt:
    /// - **A short's interest.** On margin, interest on a short's loan that the venue charges in
    ///   base is owed outside the short, which does not count it. With a short open it reads
    ///   negative by that interest, so a buy sized at the short plus its negative closes the
    ///   short and covers the interest.
    /// - **Timing or drift.** A fill has reached a position but not yet the balance, or the
    ///   positions have drifted from the venue's. A sell sized at a long plus this then still
    ///   sells no more than the account holds.
    ///
    /// # What it leaves out
    /// - **Orders in flight.** Base a resting order locks is already out of `free`, but an open
    ///   the venue has not yet acknowledged is not taken out. Nor is a fill whose balance update
    ///   has not arrived. Read it while no order on `asset` is working, or after an account
    ///   snapshot. Through the cap at `free`, a resting order that locks more base than the
    ///   positions hold lowers the read by the difference. One that locks no more, such as a
    ///   resting sell that only closes a long, leaves it unchanged.
    /// - **Pairs quoted in the asset.** Only positions whose base is `asset` are taken out. What
    ///   a pair quoted in it holds, such as a short of `ETHBTC`'s proceeds in BTC, reads as free.
    /// - **Isolated margin.** The engine stores no isolated balances, so a position in an
    ///   isolated margin pair is taken out of a balance that does not hold it. Do not read it for
    ///   an asset traded isolated.
    /// - **Debt freshness.** The debt is as fresh as [`Balance::net_asset`](rustrade_execution::balance::Balance::net_asset)'s:
    ///   see its rustdoc for a venue that streams no debt changes.
    ///
    /// # The base-fee remainder
    /// A spot fill whose fee the venue charges in base leaves the position rounded toward zero to
    /// the quantity step (see the base-asset fee section of
    /// [`InstrumentState::update_from_trade`](instrument::InstrumentState::update_from_trade)).
    /// The sub-step remainder stays in the balance, outside the position, and is held rather than
    /// charged to PnL. Over many fills it gathers, and once a step or more of it has gathered it
    /// can be sold.
    /// - **A long-only account sweeps it** with its close: a sell sized at the long plus this,
    ///   rounded toward zero to the step, sent
    ///   [`ReduceOnly::PositionAndFreeBase`].
    ///   The engine closes the long and books the rest as free base sold, at INFO, rather than as
    ///   a reduce-only mistake. Below one step there is nothing to sweep. The same close, sized
    ///   from a short, is a buy that also covers the base the account owes outside it.
    /// - **With a short of the asset open on cross margin**, base the account holds lowers the
    ///   loan: a sale that borrows sells free base before it borrows, and a repayment can spend
    ///   it. Repaying the debt with it is then usually the better use, though which to do is the
    ///   caller's choice.
    ///
    /// ```rust
    /// use rust_decimal::Decimal;
    /// use rustrade::engine::state::EngineState;
    /// use rustrade_instrument::instrument::{InstrumentIndex, round_toward_zero_to_step};
    ///
    /// /// What a long-only account sells to close its long on `instrument` and sweep the free base
    /// /// the long leaves out, to send with `ReduceOnly::PositionAndFreeBase`. `None` when there
    /// /// is nothing to sell.
    /// fn sweep_quantity<GlobalData, InstrumentData>(
    ///     state: &EngineState<GlobalData, InstrumentData>,
    ///     instrument: &InstrumentIndex,
    /// ) -> Option<Decimal> {
    ///     let held = state.instruments.instrument_index(instrument);
    ///     let free_base = state.free_base(&held.instrument.underlying.base)?;
    ///     let quantity = held.position.quantity_net() + free_base;
    ///     let quantity = held
    ///         .instrument
    ///         .quantity_step()
    ///         .map_or(quantity, |step| round_toward_zero_to_step(quantity, step));
    ///     (quantity > Decimal::ZERO).then_some(quantity)
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if this state does not track `asset`, as [`AssetStates::asset_index`] does.
    pub fn free_base(&self, asset: &AssetIndex) -> Option<Decimal> {
        let balance = self.assets.asset_index(asset).balance.as_ref()?.value;
        let interest = balance
            .margin
            .map_or(Decimal::ZERO, |margin| margin.interest);
        let positions: Decimal = self
            .instruments
            .0
            .values()
            .filter(|state| {
                matches!(state.instrument.kind, InstrumentKind::Spot)
                    && state.instrument.underlying.base == *asset
            })
            .map(|state| state.position.quantity_net())
            .sum();
        Some((balance.net_asset() - interest - positions).min(balance.free))
    }

    /// Each instrument whose position `snapshot` reports and differs from this state's.
    ///
    /// Only an instrument the venue reported as
    /// [`Flat`](rustrade_execution::position::PositionReport::Flat) or
    /// [`Open`](rustrade_execution::position::PositionReport::Open) is compared:
    /// [`Unreported`](rustrade_execution::position::PositionReport::Unreported), and an instrument
    /// the snapshot does not list, say nothing about the venue's position. The venue's signed quantity
    /// is compared with [`PositionManager::quantity_net`](position::PositionManager::quantity_net)
    /// and must match exactly; entry prices are carried in each [`PositionDrift`] but not
    /// compared.
    ///
    /// A difference is not necessarily a fault. A fill the venue has applied but whose trade the
    /// engine has not yet processed, such as one made while the snapshot was being fetched, shows
    /// as drift until the trade arrives.
    ///
    /// # Panics
    /// Panics if the snapshot names an instrument this state does not track, as
    /// [`InstrumentStates::instrument_index`] does.
    pub fn position_drift(&self, snapshot: &AccountSnapshot) -> Vec<PositionDrift> {
        snapshot
            .instruments
            .iter()
            .filter_map(|instrument| {
                let quantity_venue = instrument.position.quantity()?;
                let manager = &self
                    .instruments
                    .instrument_index(&instrument.instrument)
                    .position;
                let quantity_engine = manager.quantity_net();
                (quantity_engine != quantity_venue).then(|| {
                    PositionDrift::new(
                        instrument.instrument,
                        quantity_engine,
                        quantity_venue,
                        manager.price_entry_single(),
                        instrument
                            .position
                            .open()
                            .and_then(|position| position.entry_price),
                    )
                })
            })
            .collect()
    }

    /// Updates the internal state from an `AccountEvent`.
    ///
    /// Returns what the event's fills did to positions: the
    /// [`PositionExited`](position::PositionExited) one closed, each reduce-only fill cut
    /// (see [`InstrumentState::update_from_trade`](instrument::InstrumentState::update_from_trade)),
    /// and how a fee in a third asset was valued (below).
    ///
    /// # A fee charged in a third asset
    /// A fill's fee can be charged in an asset that is neither the instrument's base nor its
    /// quote, such as BNB on a `BTCUSDT` fill with Binance's "pay fees with BNB" on. The execution
    /// indexer gives such a fee no quote equivalent
    /// ([`AssetFees::fees_quote`](rustrade_execution::trade::AssetFees::fees_quote) is `None`), so
    /// PnL would leave it out. Before applying the fill, this method values it in quote when it
    /// can:
    /// - **The price** is [`InstrumentDataState::price`] of a spot instrument pairing the fee
    ///   asset with the fill's quote asset: `BNBUSDT` for a BNB fee on `BTCUSDT`, or the
    ///   reciprocal of a `USDTBNB`. Assets are indexed per exchange, so only an instrument on the
    ///   fill's exchange matches. Of several, the lowest [`InstrumentIndex`] is used, even while
    ///   it has no price and another has, so the choice never depends on which data arrived
    ///   first. The price is the one held when the fill is applied, however old. Cross rates
    ///   through a third asset (BNB to BTC to USDT) are not used.
    /// - **With a positive price,** `fees_quote` is the fee times that price, and the fill counts
    ///   it in PnL as it would a fee in quote.
    /// - **Without one,** for want of the instrument or of a price, `fees_quote` stays `None` and
    ///   the fee is left out of PnL. The fill is applied all the same, and counted on the
    ///   instrument's
    ///   [`TearSheet::fills_fee_unconverted`](crate::statistic::summary::instrument::TearSheet::fills_fee_unconverted).
    ///
    /// Either way the returned [`FillsApplied::fee_conversion`] records it: the pricing instrument
    /// and the price, or why there was none. The `Engine` emits it as
    /// [`EngineOutput::FeeConversion`](crate::engine::EngineOutput::FeeConversion). A zero fee
    /// needs no price, so it is valued at zero and not recorded. A fee already given a quote
    /// equivalent is kept as it is.
    ///
    /// To have fees valued, subscribe the pricing instrument's market data. Only the fee is
    /// valued: the fee asset's balance is not, since the engine keeps no equity across assets.
    /// Only this method values fees. [`InstrumentState::update_from_trade`](instrument::InstrumentState::update_from_trade),
    /// called directly, applies the fill's fee as it finds it.
    ///
    /// # A cash flow on the account as a whole
    /// A [`CashFlow`](AccountEventKind::CashFlow) whose
    /// [`instrument`](rustrade_execution::cash_flow::CashFlow::instrument) is set goes to that
    /// instrument's open position's carry (see
    /// [`InstrumentState::update_from_cash_flow`](instrument::InstrumentState::update_from_cash_flow)).
    /// One with `instrument: None` is logged at DEBUG and applied to nothing. The engine does not
    /// guess an instrument for it, because no rule is right for every account:
    /// - Several pairs can share a base asset, so the flow's asset alone may not name one.
    /// - Cross-margin interest is charged on the account's whole net loan, which can include
    ///   borrowing that no open position caused.
    /// - Interest on a quote asset is shared by every pair quoted in it.
    ///
    /// A consumer whose account rules out these cases can route a flow itself, before the event
    /// reaches the engine. For example, it can give the flow to the only instrument on the
    /// exchange whose base asset is the flow's asset and that has an open position, and leave the
    /// flow on the account otherwise:
    /// ```rust
    /// use rustrade::engine::state::{EngineState, instrument::filter::InstrumentFilter};
    /// use rustrade_execution::{AccountEvent, AccountEventKind};
    ///
    /// fn route_to_base<GlobalData, InstrumentData>(
    ///     state: &EngineState<GlobalData, InstrumentData>,
    ///     event: &mut AccountEvent,
    /// ) {
    ///     let filter = InstrumentFilter::exchanges([event.exchange]);
    ///     let AccountEventKind::CashFlow(flow) = &mut event.kind else { return };
    ///     if flow.instrument.is_some() {
    ///         return;
    ///     }
    ///     let asset = flow.asset;
    ///     let mut held = state.instruments.instruments(&filter).filter(|instrument| {
    ///         instrument.instrument.underlying.base == asset
    ///             && !instrument.position.positions.is_empty()
    ///     });
    ///     if let (Some(only), None) = (held.next(), held.next()) {
    ///         flow.instrument = Some(only.key);
    ///     }
    /// }
    /// ```
    ///
    /// This method:
    /// - Sets the account [`ConnectivityState`](connectivity::ConnectivityState) to
    ///   [`Health::Healthy`](connectivity::Health::Healthy) if it was not previously, unless the
    ///   event is [`ReinitFailed`](AccountEventKind::ReinitFailed) or
    ///   [`StreamTerminated`](AccountEventKind::StreamTerminated), which report on the account link
    ///   without showing it is up.
    /// - Updates the `GlobalData` with the `AccountEvent`.
    /// - Updates the associated `AssetStates` and `InstrumentStates` with the `AccountEvent`.
    pub fn update_from_account(&mut self, event: &AccountEvent) -> FillsApplied<AssetIndex>
    where
        GlobalData: for<'a> Processor<&'a AccountEvent>,
        InstrumentData: InstrumentDataState,
    {
        self.replay_account(event, &[])
    }

    /// [`Self::update_from_account`] for an event the `Engine` already processed, as a replica
    /// of its state does, taking the fills `reduce_only_fills` names as sent as each says.
    ///
    /// A replica records no requests in flight, so it cannot tell a reduce-only order's fill from
    /// another. Pass the [`ReduceOnlyCapped::trade_id`](instrument::ReduceOnlyCapped::trade_id)
    /// and [`ReduceOnlyCapped::reduce_only`](instrument::ReduceOnlyCapped::reduce_only) of every
    /// [`EngineOutput::ReduceOnlyCapped`](crate::engine::EngineOutput::ReduceOnlyCapped) the
    /// `Engine` emitted for this event, and the replica cuts and counts those fills as the
    /// `Engine` did.
    /// [`StateReplicaManager`](crate::engine::audit::state_replica::StateReplicaManager) does so.
    pub fn replay_account(
        &mut self,
        event: &AccountEvent,
        reduce_only_fills: &[(TradeId, ReduceOnly)],
    ) -> FillsApplied<AssetIndex>
    where
        GlobalData: for<'a> Processor<&'a AccountEvent>,
        InstrumentData: InstrumentDataState,
    {
        // Set exchange account connectivity to Healthy if it was Reconnecting. A failed re-init
        // or an ended stream reports on the link without showing it is up, so neither counts.
        if !matches!(
            event.kind,
            AccountEventKind::ReinitFailed(_) | AccountEventKind::StreamTerminated(_)
        ) {
            self.connectivity.update_from_account_event(&event.exchange);
        }

        let output = match &event.kind {
            AccountEventKind::Snapshot(snapshot) => {
                for balance in &snapshot.balances {
                    self.assets
                        .asset_index_mut(&balance.asset)
                        .update_from_balance(Snapshot(balance))
                }
                for instrument in &snapshot.instruments {
                    let instrument_state = self
                        .instruments
                        .instrument_index_mut(&instrument.instrument);

                    instrument_state.update_from_account_snapshot(instrument);
                    instrument_state.data.process(event);
                }
                FillsApplied::default()
            }
            AccountEventKind::BalanceSnapshot(balance) => {
                self.assets
                    .asset_index_mut(&balance.0.asset)
                    .update_from_balance(balance.as_ref());
                FillsApplied::default()
            }
            AccountEventKind::BalanceStreamUpdate(update) => {
                self.assets
                    .asset_index_mut(&update.0.asset)
                    .apply_balance_update(update.as_ref());
                FillsApplied::default()
            }
            AccountEventKind::OrderSnapshot(order) => {
                let instrument_state = self
                    .instruments
                    .instrument_index_mut(&order.value().key.instrument);

                // Propagate any PositionExited from deferred fill replay (fill-before-ack
                // race in OmsMode::Hedging). update_from_trade side effects (tearsheet,
                // position removal) are correct regardless; this ensures the output event
                // also reaches EngineOutput::PositionExit consumers.
                let applied =
                    instrument_state.replay_order_snapshot(order.as_ref(), reduce_only_fills);
                instrument_state.data.process(event);
                applied
            }
            AccountEventKind::OrderCancelled(response) => {
                let instrument_state = self
                    .instruments
                    .instrument_index_mut(&response.key.instrument);

                instrument_state.update_from_cancel_response(response);
                instrument_state.data.process(event);
                FillsApplied::default()
            }
            AccountEventKind::Trade(trade) => {
                // Valued before the fill is applied, with the prices held when it arrives, so a
                // fill queued for replay keeps the value it had then.
                let (valued_trade, conversion) = match self.instruments.value_third_asset_fee(trade)
                {
                    Some(valued) => {
                        let mut valued_trade = trade.clone();
                        valued_trade.fees.fees_quote = valued.fees_quote();
                        (Some(valued_trade), valued.into_conversion())
                    }
                    None => (None, None),
                };
                let trade = valued_trade.as_ref().unwrap_or(trade);

                let instrument_state = self.instruments.instrument_index_mut(&trade.instrument);
                if let Some(conversion) = &conversion {
                    conversion.log();
                    if conversion.outcome.fees_quote().is_none() {
                        instrument_state.tear_sheet.record_fill_fee_unconverted();
                    }
                }

                instrument_state.data.process(event);
                let mut applied = instrument_state.replay_trade(trade, reduce_only_fills);
                applied.fee_conversion = conversion;
                applied
            }
            AccountEventKind::StreamTerminated(reason) => {
                // Observe stream death rather than silently dropping it. The engine does not own
                // recovery policy (reconnect/re-sync/halt is consumer-specific), but a terminated
                // account feed means subsequent state may go stale — surface it loudly.
                warn!(
                    exchange = ?event.exchange,
                    %reason,
                    "account event stream terminated — no further account events will arrive on it",
                );
                FillsApplied::default()
            }
            AccountEventKind::FillRecoveryGaveUp(gap) => {
                // Fills in the span may never arrive, so positions and orders may be stale. What to
                // do (reconcile with fetch_trades, alert, halt) is the consumer's policy; the engine
                // only makes it loud.
                error!(
                    exchange = ?event.exchange,
                    scope = ?gap.scope,
                    start = %gap.start,
                    end = %gap.end,
                    attempts = gap.attempts,
                    reason = %gap.reason,
                    "account stream fill recovery gave up — fills in this span may be missing",
                );
                FillsApplied::default()
            }
            AccountEventKind::ReinitFailed(failure) => {
                // The account link is still down, so state may be going stale. Whether to keep
                // waiting, alert or halt is the consumer's policy; the engine only makes it loud.
                error!(
                    exchange = ?event.exchange,
                    attempt = failure.attempt,
                    error = %failure.error,
                    "account stream re-initialisation failed — retrying with backoff",
                );
                FillsApplied::default()
            }
            AccountEventKind::TradeAmended(amendment) => {
                // The amended trade was applied as first reported, so positions and PnL built on
                // it are wrong until it is reversed. Reversing it is the consumer's policy (the
                // engine holds no ledger of trades by id to reverse); the engine only makes it
                // loud.
                error!(
                    exchange = ?event.exchange,
                    instrument = ?amendment.instrument,
                    order_id = %amendment.order_id,
                    original = ?amendment.original,
                    kind = ?amendment.kind,
                    "venue amended a trade it reported earlier — state built on that trade is stale",
                );
                FillsApplied::default()
            }
            AccountEventKind::CashFlow(flow) => {
                // The venue's balances already include what it has posted, so a flow is applied
                // to no balance (that would count it twice). One attributed to an instrument goes
                // to its open position's carry, when it can be attributed without guessing.
                match &flow.instrument {
                    Some(instrument) => {
                        let instrument_state = self.instruments.instrument_index_mut(instrument);
                        // Not applied: already logged, and the flow stays on the account feed.
                        let _ = instrument_state.update_from_cash_flow(flow);
                        instrument_state.data.process(event);
                    }
                    None => debug!(
                        exchange = ?event.exchange,
                        kind = ?flow.kind,
                        asset = ?flow.asset,
                        amount = %flow.amount,
                        time_exchange = %flow.time_exchange,
                        "account-level cash flow received — not applied to positions or balances",
                    ),
                }
                FillsApplied::default()
            }
            AccountEventKind::Notice(notice) => {
                // Reported, not acted on: what to do about a margin call or liquidation is the
                // consumer's policy. A consumer that wants the latest notice keeps it from the
                // `GlobalData` update below, which every account event reaches. Not routed to an
                // instrument's data state: a notice changes nothing there.
                if notice.kind.is_escalation() {
                    warn!(
                        exchange = ?event.exchange,
                        kind = %notice.kind,
                        status = %notice.status,
                        instrument = ?notice.instrument,
                        margin_level = ?notice.margin_level,
                        time_exchange = %notice.time_exchange,
                        "venue reported rising margin risk on the account",
                    );
                } else {
                    info!(
                        exchange = ?event.exchange,
                        kind = %notice.kind,
                        status = %notice.status,
                        instrument = ?notice.instrument,
                        margin_level = ?notice.margin_level,
                        time_exchange = %notice.time_exchange,
                        "venue reported a change in the account's margin state",
                    );
                }
                FillsApplied::default()
            }
            AccountEventKind::MarginDebt(update) => {
                match &update.instrument {
                    None => self
                        .assets
                        .asset_index_mut(&update.asset)
                        .apply_margin_debt(update),
                    // An isolated pair's debt: the engine stores no isolated balances, as it
                    // stores no `InstrumentBalanceUpdate`. A consumer reads it off the feed.
                    Some(instrument) => debug!(
                        exchange = ?event.exchange,
                        ?instrument,
                        asset = ?update.asset,
                        kind = %update.kind,
                        "isolated margin debt update received — not applied to asset balances",
                    ),
                }
                FillsApplied::default()
            }
            _ => FillsApplied::default(),
        };

        // Update any user provided GlobalData State
        self.global.process(event);

        output
    }

    /// Updates the internal state from a `MarketEvent`.
    ///
    /// This method:
    /// - Sets the market data [`ConnectivityState`](connectivity::ConnectivityState) to
    ///   [`Health::Healthy`](connectivity::Health::Healthy) if it was not previously.
    /// - Updates the `GlobalData` with the `MarketEvent`.
    /// - Refreshes the associated instrument via
    ///   [`InstrumentState::update_from_market`](instrument::InstrumentState::update_from_market),
    ///   which updates its [`InstrumentDataState`] and then, for each open position, re-computes
    ///   `pnl_unrealised` and advances `time_exchange_update` from the new market price.
    ///
    /// # Errors
    /// Returns [`UntrackedExchange`] if the event is tagged with an exchange the engine was not
    /// built against. Nothing is mutated and the event is dropped — see that type for why the
    /// instrument update is skipped too, rather than merely the connectivity one.
    pub fn update_from_market(
        &mut self,
        event: &MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>,
    ) -> Result<(), UntrackedExchange>
    where
        GlobalData:
            for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>,
        InstrumentData: InstrumentDataState,
    {
        // Set exchange market data connectivity to Healthy if it was Reconnecting. Resolved first,
        // and propagated rather than logged: the `InstrumentIndex` on an event from an untracked
        // exchange is not ours to trust either, and `instrument_index_mut` below is a positional
        // lookup that would panic on it -- or silently credit the print to another instrument.
        self.connectivity
            .update_from_market_event(&event.exchange)?;

        let instrument_state = self.instruments.instrument_index_mut(&event.instrument);

        self.global.process(event);
        // Refreshes `data` (via `self.data.process`) AND re-computes each open position's
        // `pnl_unrealised` + advances `time_exchange_update` — previously only `data.process` ran,
        // leaving `pnl_unrealised` stale between fills despite its documented per-tick contract.
        instrument_state.update_from_market(event);

        Ok(())
    }
}

/// Snapshots the account state the engine holds at each exchange that *has* an account.
///
/// # Data-only venues are absent, not empty
/// A [`VenueRole::DataOnly`](connectivity::VenueRole::DataOnly) venue prices instruments but is
/// executed on by none, so it holds no balances and no positions. It is omitted from the map
/// entirely rather than mapped to an empty [`UnindexedAccountSnapshot`], because those two claims
/// are not the same one: an empty snapshot asserts a known, funded-then-drained account, which a
/// consumer cannot tell apart from a real one that has gone to zero. Seeding a
/// [`MockExecutionConfig`](rustrade_execution::client::mock::MockExecutionConfig) from an empty
/// snapshot would stand up a mock account at a venue nothing trades on.
///
/// Callers must therefore treat a missing `ExchangeId` as "no account at this venue", and not
/// assume every exchange the engine tracks appears as a key.
impl<GlobalData, InstrumentData> From<&EngineState<GlobalData, InstrumentData>>
    for FnvHashMap<ExchangeId, UnindexedAccountSnapshot>
{
    fn from(value: &EngineState<GlobalData, InstrumentData>) -> Self {
        let EngineState {
            trading: _,
            global: _,
            connectivity,
            assets,
            instruments,
        } = value;

        // Upper bound: venues without an account are skipped below.
        let mut snapshots = FnvHashMap::with_capacity_and_hasher(
            connectivity.exchanges().len(),
            Default::default(),
        );

        // Insert UnindexedAccountSnapshot for each exchange that holds an account.
        //
        // `enumerate` deliberately runs *before* the role filter. `ExchangeIndex` is positional
        // into `connectivity.exchanges`, so numbering only the surviving venues would shift every
        // index past the first skipped one and silently attribute one exchange's instruments to
        // another.
        for (index, (exchange, state)) in connectivity.exchanges().iter().enumerate() {
            if !state.role().has_account() {
                continue;
            }

            snapshots.insert(
                *exchange,
                UnindexedAccountSnapshot {
                    exchange: *exchange,
                    balances: assets
                        .filtered(&AssetFilter::Exchanges(OneOrMany::One(*exchange)))
                        .map(AssetBalance::from)
                        .collect(),
                    instruments: instruments
                        .instruments(&InstrumentFilter::Exchanges(OneOrMany::One(ExchangeIndex(
                            index,
                        ))))
                        .map(|snapshot| {
                            generate_unindexed_instrument_account_snapshot(*exchange, snapshot)
                        })
                        .collect::<Vec<_>>(),
                },
            );
        }

        snapshots
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::engine::state::EngineState;
    use rustrade_instrument::{
        instrument::data_venue::DataVenue, test_utils::instrument as test_instrument,
    };

    const DATA: ExchangeId = ExchangeId::BinanceSpot;
    const EXECUTION: ExchangeId = ExchangeId::Coinbase;

    #[test]
    fn account_snapshots_omit_a_data_only_venue_and_keep_the_execution_venues_instruments() {
        // Priced on DATA, executed on EXECUTION: DATA is tracked for connectivity but holds no
        // account at all.
        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")
            .with_data_venue(DataVenue::new_same_name(DATA))]);
        let state: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ()).build();

        let snapshots = FnvHashMap::<ExchangeId, UnindexedAccountSnapshot>::from(&state);

        assert!(
            !snapshots.contains_key(&DATA),
            "a data-only venue has no account, so it must be absent rather than empty"
        );
        assert_eq!(snapshots.len(), 1);

        // The property below only bites while the skipped venue sorts FIRST, and `ExchangeId`
        // orders by declaration position — so assert it rather than assume it. Were the two to
        // invert, this test would keep passing while guarding nothing.
        assert!(DATA < EXECUTION, "the skipped venue must sort first");

        // Guards the enumerate-before-filter ordering: `DATA` sorts ahead of `EXECUTION`, so
        // numbering the surviving venues instead would hand `EXECUTION` the skipped venue's
        // `ExchangeIndex` and its instrument list would come back empty.
        let execution = snapshots.get(&EXECUTION).unwrap();
        assert_eq!(execution.exchange, EXECUTION);
        assert_eq!(execution.instruments.len(), 1);
    }
    /// A given-up fill recovery is reported, not acted on: no position exits and the account
    /// state is unchanged. What to do about the missing fills is the consumer's policy.
    #[test]
    fn a_fill_recovery_give_up_changes_no_account_state() {
        use crate::engine::state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
        };
        use chrono::{DateTime, Utc};
        use rustrade_execution::{FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope};

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .build();
        let before = state.clone();
        let event = AccountEvent::new(
            ExchangeIndex::new(0),
            AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                FillRecoveryScope::Instruments(vec![InstrumentIndex::new(0)]),
                DateTime::<Utc>::MIN_UTC,
                DateTime::<Utc>::MAX_UTC,
                6,
                FillRecoveryFailure::TimedOut { timeout_secs: 30 },
            )),
        );

        assert_eq!(state.update_from_account(&event), FillsApplied::default());
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);
        assert_eq!(state.trading, before.trading);
    }

    /// A failed re-init and an ended stream report on the account link without showing it is
    /// up, so neither marks it Healthy or changes account state. The next event from a live
    /// stream marks it Healthy.
    #[test]
    fn only_events_from_a_live_account_stream_mark_the_account_healthy() {
        use crate::engine::state::{
            connectivity::Health, global::DefaultGlobalData,
            instrument::data::DefaultInstrumentMarketData,
        };
        use chrono::{DateTime, Utc};
        use rustrade_execution::{
            FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope,
            error::{
                AccountReinitFailure, AccountStreamInitError, ClientError, ConnectivityError,
                StreamTerminationReason,
            },
        };

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .build();
        let exchange = ExchangeIndex::new(0);
        let account_health =
            |state: &EngineState<_, _>| state.connectivity.connectivity_index(&exchange).account();
        assert_eq!(account_health(&state), Health::Reconnecting);
        let before = state.clone();

        let reinit_failed = AccountEvent::new(
            exchange,
            AccountEventKind::ReinitFailed(AccountReinitFailure::new(
                1,
                AccountStreamInitError::Client(ClientError::Connectivity(
                    ConnectivityError::Timeout,
                )),
            )),
        );
        let terminated = AccountEvent::new(
            exchange,
            AccountEventKind::StreamTerminated(StreamTerminationReason::Error(
                "socket closed".to_owned(),
            )),
        );
        for event in [&reinit_failed, &terminated] {
            assert_eq!(state.update_from_account(event), FillsApplied::default());
            assert_eq!(account_health(&state), Health::Reconnecting, "{event:?}");
        }
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);
        assert_eq!(state.trading, before.trading);

        let from_live_stream = AccountEvent::new(
            exchange,
            AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                FillRecoveryScope::AllInstruments,
                DateTime::<Utc>::MIN_UTC,
                DateTime::<Utc>::MAX_UTC,
                1,
                FillRecoveryFailure::TimedOut { timeout_secs: 30 },
            )),
        );
        assert_eq!(
            state.update_from_account(&from_live_stream),
            FillsApplied::default()
        );
        assert_eq!(account_health(&state), Health::Healthy);
    }

    /// A trade amendment is reported, not acted on: no position exits and the account state is
    /// unchanged. Reversing the trade is the consumer's policy.
    #[test]
    fn a_trade_amendment_changes_no_account_state() {
        use crate::engine::state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
        };
        use chrono::{DateTime, Utc};
        use rust_decimal::Decimal;
        use rustrade_execution::{
            order::id::OrderId,
            trade::{TradeAmendment, TradeAmendmentKind, TradeId},
        };

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .build();
        let before = state.clone();
        let event = AccountEvent::new(
            ExchangeIndex::new(0),
            AccountEventKind::TradeAmended(TradeAmendment::new(
                InstrumentIndex::new(0),
                OrderId::new("ord-1"),
                DateTime::<Utc>::MIN_UTC,
                Some(TradeId::new("t-1")),
                TradeAmendmentKind::Busted {
                    quantity: Some(Decimal::ONE),
                },
            )),
        );

        assert_eq!(state.update_from_account(&event), FillsApplied::default());
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);
        assert_eq!(state.trading, before.trading);
    }

    /// A notice, rising risk or not, attributed or not, changes no engine state.
    #[test]
    fn a_notice_changes_no_state() {
        use crate::engine::state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
        };
        use chrono::{DateTime, Utc};
        use rust_decimal_macros::dec;
        use rustrade_execution::notice::{AccountNotice, NoticeKind};
        use smol_str::SmolStr;

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .build();
        let before = state.clone();

        for (kind, instrument) in [
            (NoticeKind::Liquidation, Some(InstrumentIndex::new(0))),
            (NoticeKind::MarginCall, None),
            (NoticeKind::MarginRestored, None),
        ] {
            let event = AccountEvent::new(
                ExchangeIndex::new(0),
                AccountEventKind::Notice(AccountNotice::new(
                    kind,
                    instrument,
                    DateTime::<Utc>::MIN_UTC,
                    Some(dec!(1.05)),
                    SmolStr::new("STATUS"),
                )),
            );
            assert_eq!(
                state.update_from_account(&event),
                FillsApplied::default(),
                "{kind}"
            );
        }
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);
        assert_eq!(state.trading, before.trading);
    }

    /// An account-level debt update replaces its asset's debt and fills nothing; one naming an
    /// isolated pair changes no state.
    #[test]
    fn a_margin_debt_update_reaches_only_an_account_level_asset_balance() {
        use crate::engine::state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
        };
        use chrono::{DateTime, Utc};
        use rust_decimal_macros::dec;
        use rustrade_execution::{MarginDebtKind, MarginDebtUpdate, balance::MarginDetails};
        use smol_str::SmolStr;

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let base = instruments.instruments()[0].value.underlying.base;
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .build();
        let debt = MarginDetails::new(dec!(0.00006), dec!(0.00000002));
        let update = |instrument| {
            AccountEvent::new(
                ExchangeIndex::new(0),
                AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                    base,
                    instrument,
                    debt,
                    MarginDebtKind::Interest,
                    SmolStr::new("INTEREST"),
                    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
                )),
            )
        };

        let before = state.clone();
        assert_eq!(
            state.update_from_account(&update(Some(InstrumentIndex::new(0)))),
            FillsApplied::default()
        );
        assert_eq!(state.assets, before.assets);

        assert_eq!(
            state.update_from_account(&update(None)),
            FillsApplied::default()
        );
        let balance = state.assets.asset_index(&base).balance.unwrap();
        assert_eq!(balance.value.margin, Some(debt));
        assert_eq!(state.instruments, before.instruments);
        assert_eq!(state.trading, before.trading);
    }

    /// A cash flow attributed to an instrument reaches its open position's carry and nothing
    /// else; an account-level one changes no state.
    #[test]
    fn a_cash_flow_reaches_only_its_instruments_open_position() {
        use crate::engine::state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
            position::PositionSeed,
        };
        use chrono::{DateTime, Utc};
        use rust_decimal::Decimal;
        use rust_decimal_macros::dec;
        use rustrade_execution::cash_flow::{CashFlow, CashFlowKind};
        use rustrade_instrument::Side;

        let instruments = IndexedInstruments::new([test_instrument(EXECUTION, "btc", "usdt")]);
        let name = instruments.instruments()[0].value.name_internal.clone();
        let quote = instruments.instruments()[0].value.underlying.quote;
        let time = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let mut state: EngineState<DefaultGlobalData, DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, DefaultGlobalData, |_| {
                DefaultInstrumentMarketData::default()
            })
            .time_engine_start(time)
            .positions([PositionSeed::new(
                name,
                Side::Sell,
                dec!(1),
                dec!(100),
                time,
            )])
            .try_build()
            .unwrap();
        let flow = |instrument| {
            AccountEvent::new(
                ExchangeIndex::new(0),
                AccountEventKind::CashFlow(CashFlow::new(
                    CashFlowKind::Funding {
                        rate: None,
                        position_quantity: None,
                    },
                    quote,
                    dec!(0.75),
                    instrument,
                    time,
                    None,
                )),
            )
        };

        let before = state.clone();
        assert_eq!(
            state.update_from_account(&flow(None)),
            FillsApplied::default()
        );
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);

        assert_eq!(
            state.update_from_account(&flow(Some(InstrumentIndex::new(0)))),
            FillsApplied::default()
        );
        assert_eq!(state.assets, before.assets);
        let positions = &state
            .instruments
            .instrument_index(&InstrumentIndex::new(0))
            .position
            .positions;
        assert_eq!(positions.len(), 1);
        let position = positions.values().next().unwrap();
        assert_eq!(position.carry, dec!(0.75));
        assert_eq!(position.pnl_realised, Decimal::ZERO);
    }

    /// Instruments on `EXECUTION`: `btc_usdt` spot, a `btc_usdc` perpetual and `eth_btc` spot,
    /// and `btc_usdt` spot on `DATA`, with a signed position on each `(exchange, name)` listed,
    /// and `balance` for btc on `EXECUTION`.
    fn free_base_state(
        positions: &[(ExchangeId, &str, Decimal)],
        balance: Option<rustrade_execution::balance::Balance>,
    ) -> (EngineState<(), ()>, AssetIndex) {
        use crate::Timed;
        use chrono::{DateTime, Utc};
        use rustrade_execution::{
            order::id::{OrderId, StrategyId},
            trade::{AssetFees, Trade},
        };
        use rustrade_instrument::{
            Side,
            instrument::{kind::perpetual::PerpetualContract, name::InstrumentNameInternal},
            test_utils::asset,
        };

        let mut perpetual = test_instrument(EXECUTION, "btc", "usdc");
        perpetual.kind = InstrumentKind::Perpetual(PerpetualContract {
            contract_size: Decimal::ONE,
            settlement_asset: asset("usdc"),
        });
        let instruments = IndexedInstruments::new([
            test_instrument(EXECUTION, "btc", "usdt"),
            perpetual,
            test_instrument(EXECUTION, "eth", "btc"),
            test_instrument(DATA, "btc", "usdt"),
        ]);
        let btc = instruments
            .find_asset_index(EXECUTION, &"btc".into())
            .unwrap();
        let mut state: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ()).build();

        for (exchange, name, quantity) in positions {
            let index = instruments
                .find_instrument_index(
                    *exchange,
                    &InstrumentNameInternal::new_from_exchange(*exchange, *name),
                )
                .unwrap();
            let instrument = state.instruments.instrument_index_mut(&index);
            let quote = instrument.instrument.underlying.quote;
            let (side, quantity) = if quantity.is_sign_negative() {
                (Side::Sell, -*quantity)
            } else {
                (Side::Buy, *quantity)
            };
            let _ = instrument.update_from_trade(&Trade::new(
                TradeId::new(format!("t{index}")),
                OrderId::new(format!("o{index}")),
                index,
                StrategyId::new("s"),
                DateTime::<Utc>::MIN_UTC,
                side,
                Decimal::ONE_HUNDRED,
                quantity,
                None,
                AssetFees {
                    asset: quote,
                    fees: Decimal::ZERO,
                    fees_quote: Some(Decimal::ZERO),
                },
            ));
        }
        state.assets.asset_index_mut(&btc).balance =
            balance.map(|balance| Timed::new(balance, DateTime::<Utc>::MIN_UTC));
        (state, btc)
    }

    /// The base held outside the spot positions with that base on the asset's exchange: not a
    /// derivative's contracts, a pair that quotes in it, or another exchange's pair.
    #[test]
    fn free_base_takes_out_only_the_spot_positions_on_the_asset() {
        use rust_decimal_macros::dec;
        use rustrade_execution::balance::Balance;

        let (state, btc) = free_base_state(
            &[
                (EXECUTION, "btc_usdt", dec!(1)),
                (EXECUTION, "btc_usdc", dec!(5)),
                (EXECUTION, "eth_btc", dec!(3)),
                (DATA, "btc_usdt", dec!(2)),
            ],
            Some(Balance::new(dec!(1.00991), dec!(1.00991))),
        );

        assert_eq!(state.free_base(&btc), Some(dec!(0.00991)));
    }

    /// Base locked by a resting order is not free to trade, so the read is capped at `free`.
    #[test]
    fn free_base_is_capped_at_what_is_free() {
        use rust_decimal_macros::dec;
        use rustrade_execution::balance::Balance;

        let (state, btc) = free_base_state(
            &[(EXECUTION, "btc_usdt", dec!(1))],
            Some(Balance::new(dec!(1.5), dec!(0.3))),
        );

        assert_eq!(state.free_base(&btc), Some(dec!(0.3)));
    }

    /// With a cross-margin short open, the debt and its interest come out, and the short's own
    /// quantity goes back in: what is left is the base the short does not owe.
    #[test]
    fn free_base_takes_out_debt_and_interest_against_a_short() {
        use rust_decimal_macros::dec;
        use rustrade_execution::balance::Balance;

        let (state, btc) = free_base_state(
            &[(EXECUTION, "btc_usdt", dec!(-1))],
            Some(Balance::new_margin(
                dec!(0.00991),
                dec!(0.00991),
                dec!(1),
                dec!(0.001),
            )),
        );

        assert_eq!(state.free_base(&btc), Some(dec!(0.00891)));
    }

    /// A short holding no base of its own reads negative by its interest: the base owed outside
    /// it, which a buy sized at the short plus the read covers.
    #[test]
    fn free_base_reads_a_shorts_interest_as_owed() {
        use rust_decimal_macros::dec;
        use rustrade_execution::balance::Balance;

        let (state, btc) = free_base_state(
            &[(EXECUTION, "btc_usdt", dec!(-1))],
            Some(Balance::new_margin(
                Decimal::ZERO,
                Decimal::ZERO,
                dec!(1),
                dec!(0.001),
            )),
        );

        assert_eq!(state.free_base(&btc), Some(dec!(-0.001)));
    }

    /// Positions that count more than the account holds read negative, so a sweep sized from it
    /// sells no more than is held.
    #[test]
    fn free_base_is_negative_when_the_positions_count_more_than_is_held() {
        use rust_decimal_macros::dec;
        use rustrade_execution::balance::Balance;

        let (state, btc) = free_base_state(
            &[(EXECUTION, "btc_usdt", dec!(1))],
            Some(Balance::new(dec!(0.99), dec!(0.99))),
        );

        assert_eq!(state.free_base(&btc), Some(dec!(-0.01)));
    }

    #[test]
    fn free_base_is_none_without_a_balance() {
        use rust_decimal_macros::dec;

        let (state, btc) = free_base_state(&[(EXECUTION, "btc_usdt", dec!(1))], None);

        assert_eq!(state.free_base(&btc), None);
    }
}
