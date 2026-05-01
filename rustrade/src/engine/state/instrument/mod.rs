use crate::{
    engine::state::{
        instrument::{data::InstrumentDataState, filter::InstrumentFilter},
        order::{Orders, manager::OrderManager},
        position::{OmsMode, PositionExited, PositionManager},
    },
    statistic::summary::instrument::TearSheetGenerator,
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use itertools::Either;
use rustrade_data::event::MarketEvent;
use rustrade_execution::{
    FeeModel, FeeModelConfig, InstrumentAccountSnapshot,
    order::{
        Order, OrderKey,
        id::{ClientOrderId, OrderId, PositionId},
        request::OrderResponseCancel,
        state::{ActiveOrderState, OrderState},
    },
    trade::Trade,
};
use rustrade_instrument::{
    Keyed,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        name::{InstrumentNameExchange, InstrumentNameInternal},
    },
};
use rustrade_integration::collection::{FnvIndexMap, snapshot::Snapshot};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{debug, warn};

/// Defines the state interface [`InstrumentDataState`] that can be implemented for custom
/// instrument level data state.
pub mod data;

/// Defines an `InstrumentFilter`, used to filter instrument-centric data structures.
pub mod filter;

/// Collection of [`InstrumentState`]s indexed by [`InstrumentIndex`].
///
/// Note that the same instruments with the same [`InstrumentNameExchange`] (eg/ "btc_usdt") but
/// on different exchanges will have their own [`InstrumentState`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InstrumentStates<
    InstrumentData,
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
>(
    pub  FnvIndexMap<
        InstrumentNameInternal,
        InstrumentState<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>,
    >,
);

impl<InstrumentData> InstrumentStates<InstrumentData> {
    /// Return a reference to the `InstrumentState` associated with an `InstrumentIndex`.
    ///
    /// Panics if `InstrumentState` associated with the `InstrumentIndex` does not exist.
    pub fn instrument_index(&self, key: &InstrumentIndex) -> &InstrumentState<InstrumentData> {
        self.0
            .get_index(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("InstrumentStates does not contain: {key}"))
    }

    /// Return a mutable reference to the `InstrumentState` associated with an `InstrumentIndex`.
    ///
    /// Panics if `InstrumentState` associated with the `InstrumentIndex` does not exist.
    pub fn instrument_index_mut(
        &mut self,
        key: &InstrumentIndex,
    ) -> &mut InstrumentState<InstrumentData> {
        self.0
            .get_index_mut(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("InstrumentStates does not contain: {key}"))
    }

    /// Return a reference to the `InstrumentState` associated with an `InstrumentNameInternal`.
    ///
    /// Panics if `InstrumentState` associated with the `InstrumentNameInternal` does not exist.
    pub fn instrument(&self, key: &InstrumentNameInternal) -> &InstrumentState<InstrumentData> {
        self.0
            .get(key)
            .unwrap_or_else(|| panic!("InstrumentStates does not contain: {key}"))
    }

    /// Return a mutable reference to the `InstrumentState` associated with an
    /// `InstrumentNameInternal`.
    ///
    /// Panics if `InstrumentState` associated with the `InstrumentNameInternal` does not exist.
    pub fn instrument_mut(
        &mut self,
        key: &InstrumentNameInternal,
    ) -> &mut InstrumentState<InstrumentData> {
        self.0
            .get_mut(key)
            .unwrap_or_else(|| panic!("InstrumentStates does not contain: {key}"))
    }

    /// Return an `Iterator` of references to `InstrumentState`s being tracked, optionally filtered
    /// by the provided `InstrumentFilter`.
    pub fn instruments<'a>(
        &'a self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a InstrumentState<InstrumentData>> {
        self.filtered(filter)
    }

    /// Return an `Iterator` of mutable references to `InstrumentState`s being tracked, optionally
    /// filtered by the provided `InstrumentFilter`.
    pub fn instruments_mut<'a>(
        &'a mut self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a mut InstrumentState<InstrumentData>> {
        self.filtered_mut(filter)
    }

    /// Return an `Iterator` of references to instrument `TearSheetGenerator`s, optionally
    /// filtered by the provided `InstrumentFilter`.
    pub fn tear_sheets<'a>(
        &'a self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a TearSheetGenerator>
    where
        InstrumentData: 'a,
    {
        self.filtered(filter).map(|state| &state.tear_sheet)
    }

    /// Return an `Iterator` of references to instrument `PositionManager`s, optionally
    /// filtered by the provided `InstrumentFilter`.
    pub fn positions<'a>(
        &'a self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a PositionManager>
    where
        InstrumentData: 'a,
    {
        self.filtered(filter).map(|state| &state.position)
    }

    /// Return an `Iterator` of references to instrument `Orders`, optionally filtered by the
    /// provided `InstrumentFilter`.
    pub fn orders<'a>(&'a self, filter: &'a InstrumentFilter) -> impl Iterator<Item = &'a Orders>
    where
        InstrumentData: 'a,
    {
        self.filtered(filter).map(|state| &state.orders)
    }

    /// Return an `Iterator` of references to custom instrument level data state, optionally
    /// filtered by the provided `InstrumentFilter`.
    pub fn instrument_datas<'a>(
        &'a self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a InstrumentData>
    where
        InstrumentData: 'a,
    {
        self.filtered(filter).map(|state| &state.data)
    }

    /// Return an `Iterator` of mutable references to custom instrument level data state,
    /// optionally filtered by the provided `InstrumentFilter`.
    pub fn instrument_datas_mut<'a>(
        &'a mut self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a mut InstrumentData>
    where
        InstrumentData: 'a,
    {
        self.filtered_mut(filter).map(|state| &mut state.data)
    }

    /// Return a filtered `Iterator` of `InstrumentState`s based on the provided `InstrumentFilter`.
    fn filtered<'a>(
        &'a self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a InstrumentState<InstrumentData>>
    where
        InstrumentData: 'a,
    {
        use filter::InstrumentFilter::*;
        match filter {
            None => Either::Left(Either::Left(self.0.values())),
            Exchanges(exchanges) => Either::Left(Either::Right(
                self.0
                    .values()
                    .filter(|state| exchanges.contains(&state.instrument.exchange)),
            )),
            Instruments(instruments) => Either::Right(Either::Right(
                self.0
                    .values()
                    .filter(|state| instruments.contains(&state.key)),
            )),
            Underlyings(underlying) => Either::Right(Either::Left(
                self.0
                    .values()
                    .filter(|state| underlying.contains(&state.instrument.underlying)),
            )),
        }
    }

    /// Return a filtered `Iterator` of mutable `InstrumentState`s based on the
    /// provided `InstrumentFilter`.
    fn filtered_mut<'a>(
        &'a mut self,
        filter: &'a InstrumentFilter,
    ) -> impl Iterator<Item = &'a mut InstrumentState<InstrumentData>>
    where
        InstrumentData: 'a,
    {
        use filter::InstrumentFilter::*;
        match filter {
            None => Either::Left(Either::Left(self.0.values_mut())),
            Exchanges(exchanges) => Either::Left(Either::Right(
                self.0
                    .values_mut()
                    .filter(|state| exchanges.contains(&state.instrument.exchange)),
            )),
            Instruments(instruments) => Either::Right(Either::Right(
                self.0
                    .values_mut()
                    .filter(|state| instruments.contains(&state.key)),
            )),
            Underlyings(underlying) => Either::Right(Either::Left(
                self.0
                    .values_mut()
                    .filter(|state| underlying.contains(&state.instrument.underlying)),
            )),
        }
    }
}

/// Represents the current state of an instrument, including its [`Position`](super::position::Position), [`Orders`], and
/// user provided instrument data.
///
/// This aggregates all the state and data for a single instrument, providing a comprehensive
/// view of the instrument.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InstrumentState<
    InstrumentData,
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    /// Unique `InstrumentKey` identifier for the instrument this state is associated with.
    pub key: InstrumentKey,

    /// Complete instrument definition.
    pub instrument: Instrument<ExchangeKey, AssetKey>,

    /// TearSheet generator for summarising the trading performance associated with an Instrument.
    pub tear_sheet: TearSheetGenerator,

    /// Current `PositionManager`.
    pub position: PositionManager<AssetKey, InstrumentKey>,

    /// Active orders and associated order management.
    pub orders: Orders<ExchangeKey, InstrumentKey>,

    /// User provided instrument level data state. This can include market data, strategy data,
    /// risk data, option pricing data, or any other instrument-specific information.
    pub data: InstrumentData,

    /// Commission model applied to each fill before it reaches the `PositionManager`.
    ///
    /// The computed fee is added to `Trade.fees.fees` on a cloned trade so that
    /// `Position` PnL calculations include exchange commissions. Defaults to
    /// [`FeeModelConfig::Zero`] (no commission). Override with
    /// [`FeeModelConfig::PerContract`] for options brokers that charge per-contract.
    ///
    /// Only enable [`FeeModelConfig::PerContract`] when the `ExecutionClient` reports
    /// `Trade.fees.fees = 0` (i.e., commission is not already embedded in fill reports).
    /// If the client already includes commission and `PerContract` is also active,
    /// fees will be double-counted.
    #[serde(default)]
    pub fee_model: FeeModelConfig,

    /// Set to `true` once a `ContractExpiry` event has been fully processed for this instrument.
    ///
    /// Subsequent `ContractExpiry` events are ignored (idempotent). Callers should treat
    /// an instrument with this flag set as settled and remove it from their active instrument
    /// set when appropriate.
    #[serde(default)]
    pub expiration_processed: bool,

    /// Maps `ClientOrderId` → `PositionId` for hedging-mode fill routing.
    ///
    /// Populated by [`InFlightRequestRecorder::record_in_flight_open`] when an order carrying
    /// a [`RequestOpen::position_id`](rustrade_execution::order::request::RequestOpen::position_id)
    /// is submitted. Used by [`Self::update_from_trade`] to resolve the correct position slot
    /// for a fill in `OmsMode::Hedging`.
    #[serde(default)]
    pub position_ids: FnvHashMap<ClientOrderId, PositionId>,

    /// Pending fills that arrived before the order ack (`OpenInFlight` state) in
    /// `OmsMode::Hedging`. Keyed by exchange `OrderId` (filled when ack arrives).
    ///
    /// # Fill-before-ack race
    ///
    /// In a REST-submit + WebSocket-fill architecture (e.g., Alpaca), the WebSocket fill
    /// notification for a fast-filling market order can arrive before the REST ack response
    /// that contains the exchange `OrderId`. Without queuing, the first fill would open a
    /// spurious position under the raw exchange `OrderId` instead of the strategy's chosen
    /// `PositionId`, splitting PnL permanently across two position slots.
    ///
    /// When a fill arrives and no `Open`/`CancelInFlight` order matches the exchange
    /// `OrderId`, but at least one `OpenInFlight` order exists, the fill is buffered here.
    /// On the next `OpenInFlight → Open` transition (the ack), fills with matching
    /// exchange `OrderId`s are replayed in order through the normal routing path.
    ///
    /// In `OmsMode::Netting` this field is always empty (netting positions use a fixed key;
    /// fill-before-ack does not cause split slots).
    #[serde(default = "Vec::new")]
    pub pending_fills: Vec<Trade<AssetKey, InstrumentKey>>,

    /// Reverse index: exchange `OrderId` → `ClientOrderId` for O(1) fill routing in
    /// `OmsMode::Hedging`.
    ///
    /// Populated in [`Self::update_from_order_snapshot`] on every `OpenInFlight → Open`
    /// transition. Cleaned up by [`Self::cleanup_routing_tables`] when orders leave
    /// `self.orders`.
    ///
    /// Without this index, `update_from_trade` must scan all active orders on every fill
    /// to find the order whose exchange `OrderId` matches `trade.order_id` — O(active orders)
    /// per fill. This index reduces that to two O(1) hash-map lookups.
    #[serde(default = "FnvHashMap::default")]
    pub exchange_id_to_cid: FnvHashMap<OrderId, ClientOrderId>,
}

impl<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>
    InstrumentState<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>
{
    /// Updates the instrument state using an account snapshot from the exchange.
    ///
    /// This updates active orders for the instrument, using timestamps where relevant to ensure
    /// the most recent order state is applied.
    pub fn update_from_account_snapshot(
        &mut self,
        snapshot: &InstrumentAccountSnapshot<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
        AssetKey: Debug + Clone,
    {
        for order in &snapshot.orders {
            // PositionExited from deferred fill replay is not propagated here: the
            // Snapshot event path in EngineState::update_from_account already returns
            // None unconditionally. This is a pre-existing limitation — snapshot
            // reconciliation at startup does not emit PositionExit output events.
            let _ = self.update_from_order_snapshot(Snapshot(order));
        }
        self.cleanup_routing_tables();
    }

    /// Drop stale entries from `position_ids` and `exchange_id_to_cid` whose
    /// `ClientOrderId` is no longer present in `self.orders`. Called after every
    /// mutation that may transition an order to a terminal state — prevents both
    /// maps from growing unboundedly across the lifetime of a long-running engine
    /// in Hedging mode.
    ///
    /// # Known limitation — terminal-state late fills (Hedging mode)
    ///
    /// When an order transitions from `Open` to a terminal state (e.g. `FullyFilled`
    /// via an exchange snapshot), this method removes the `exchange_id → CID` entry
    /// from `exchange_id_to_cid`. Any fill event that arrives *after* the terminal
    /// snapshot for the same exchange `OrderId` will fall through to a linear scan
    /// and, finding nothing, may open a spurious position in Hedging mode.
    ///
    /// Primary mitigation: `AlpacaClient`'s dedup LRU cache filters fills whose
    /// `{order_id}:{filled_qty}` key was already processed, covering the most
    /// common duplicate-fill scenario. A full fix requires a "recently closed"
    /// map with TTL semantics and is deferred until Hedging mode production use.
    fn cleanup_routing_tables(&mut self) {
        if !self.position_ids.is_empty() {
            self.position_ids
                .retain(|cid, _| self.orders.0.contains_key(cid));
        }
        if !self.exchange_id_to_cid.is_empty() {
            self.exchange_id_to_cid
                .retain(|_, cid| self.orders.0.contains_key(cid));
        }
    }

    /// Updates the instrument state from an [`Order`] snapshot.
    ///
    /// Returns a [`PositionExited`] if a deferred fill (queued during a fill-before-ack
    /// race in `OmsMode::Hedging`) closes a position when replayed on this ack transition.
    /// Callers must propagate this value to the engine's output path.
    ///
    /// # Known limitation — single exit per deferred replay
    ///
    /// At most one `PositionExited` is returned per call. In normal `OmsMode::Hedging`
    /// usage (no position flips), a single order's fills can produce at most one close
    /// event, so this is sufficient.
    ///
    /// **Edge case (not supported):** If a deferred replay batch contains fills that
    /// flip positions (quantity crossing zero) multiple times, only the last
    /// `PositionExited` is returned; earlier exits are silently dropped. This edge
    /// case requires position flips, which are documented as undefined behaviour in
    /// `OmsMode::Hedging`. NautilusTrader similarly emits single `PositionClosed`
    /// events per state transition rather than batching multiple closes.
    pub fn update_from_order_snapshot(
        &mut self,
        order: Snapshot<&Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>,
    ) -> Option<PositionExited<AssetKey, InstrumentKey>>
    where
        ExchangeKey: Debug + Clone,
        AssetKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // Detect an OpenInFlight → Open transition BEFORE mutating orders so we can
        // capture both the CID and the new exchange OrderId in a single pass.
        //
        // This drives two improvements:
        // (a) PERF-1: Populate exchange_id_to_cid for O(1) fill routing.
        // (b) OPEN-1: Replay fills that arrived before the ack (pending_fills).
        //
        // Use references for all lookups — clone is deferred to the OpenInFlight→Open
        // transition branch below so the common steady-state path (Open or terminal
        // orders) avoids one UUID-length SmolStr heap allocation per call.

        // Capture the CID → PositionId mapping BEFORE the orders update so we can restore
        // it if needed for deferred fill replay (C1 race: fully-filled-on-ack).
        //
        // When the REST ack arrives with filled_quantity == quantity, Orders::update_from_order_snapshot
        // removes the order from orders.0 (zero remaining quantity). cleanup_routing_tables then
        // removes position_ids[cid] because the CID is no longer in orders.0. The deferred fill
        // replay in step (b) then calls update_from_trade, whose fast path finds the CID via
        // exchange_id_to_cid but gets None from position_ids, falling back to opening a spurious
        // position under the raw OrderId instead of the strategy's chosen PositionId.
        let pre_update_pos_id = self.position_ids.get(&order.0.key.cid).cloned();

        let currently_open_in_flight = self
            .orders
            .0
            .get(&order.0.key.cid)
            .map(|o| matches!(o.state, ActiveOrderState::OpenInFlight(_)))
            .unwrap_or(false);

        let ack_exchange_id: Option<OrderId> = if currently_open_in_flight {
            match &order.0.state {
                OrderState::Active(ActiveOrderState::Open(open)) => Some(open.id.clone()),
                _ => None,
            }
        } else {
            None
        };

        self.orders.update_from_order_snapshot(order);
        self.cleanup_routing_tables();

        // On OpenInFlight → Open: update reverse index and replay pending fills.
        if let Some(exchange_id) = ack_exchange_id {
            // Clone the CID here, not at method entry — paid only on the
            // OpenInFlight→Open transition, not on every call.
            let cid = order.0.key.cid.clone();
            // (a) PERF-1: O(1) reverse index for subsequent fill routing.
            self.exchange_id_to_cid
                .insert(exchange_id.clone(), cid.clone());

            // C1 fix: restore the CID → PositionId entry if cleanup_routing_tables removed it
            // because the order was fully filled (removed from orders.0) before deferred replay.
            if let Some(pos_id) = pre_update_pos_id {
                self.position_ids.entry(cid).or_insert(pos_id);
            }

            // (b) OPEN-1: Replay fills that arrived before this ack.
            if !self.pending_fills.is_empty() {
                // Collect matching fills first to avoid borrow-checker conflict
                // between pending_fills drain and update_from_trade's &mut self.
                let deferred: Vec<Trade<AssetKey, InstrumentKey>> = self
                    .pending_fills
                    .iter()
                    .filter(|f| f.order_id == exchange_id)
                    .cloned()
                    .collect();
                self.pending_fills.retain(|f| f.order_id != exchange_id);

                let mut deferred_exit = None;
                for fill in deferred {
                    debug!(
                        order_id = %fill.order_id,
                        "Replaying deferred fill after order ack"
                    );
                    if let Some(exited) = self.update_from_trade(&fill) {
                        if deferred_exit.is_some() {
                            // Known limitation: only the last PositionExited from a
                            // deferred replay is returned. If multiple fills each close
                            // a separate position, earlier exits are applied to the tear
                            // sheet but their PositionExited events are not emitted.
                            warn!(
                                order_id = %fill.order_id,
                                "deferred fill replay: dropping earlier PositionExited — \
                                 only the final exit event will be returned to the caller"
                            );
                        }
                        deferred_exit = Some(exited);
                    }
                }

                // BUG-3 fix: after deferred replay the order may have been fully
                // consumed (removed from orders.0 by the fill). The exchange_id entry
                // inserted above (line 447) would then become stale — its CID is no
                // longer in orders.0, so cleanup_routing_tables cannot remove it via
                // the normal post-ack path. Prune it explicitly here.
                self.cleanup_routing_tables();

                return deferred_exit;
            }
        }

        None
    }

    /// Updates the instrument state from an
    /// [`OrderRequestCancel`](rustrade_execution::order::request::OrderRequestCancel) response.
    ///
    /// # Late-fill race after cancel ack
    ///
    /// When the cancel ack arrives, `cleanup_routing_tables` removes the
    /// `CID → PositionId` mapping for the cancelled order. If a fill for the same
    /// order was in-flight when the cancel was sent (exchange race), that late fill
    /// will not find a routing entry and falls back to opening a position keyed by the
    /// raw `OrderId` — logged as a warning by `update_from_trade`. This is a known
    /// exchange protocol limitation; the internal state remains consistent.
    ///
    /// # Cancel-before-ack and `pending_fills`
    ///
    /// In `OmsMode::Hedging`, fills that arrive before the REST order ack are buffered
    /// in `pending_fills` and replayed on the `OpenInFlight → Open` transition. If the
    /// order is cancelled before that ack arrives, those fills can never be replayed.
    /// This method drains `pending_fills` when no `OpenInFlight` orders remain after
    /// the cancel, preventing unbounded accumulation.
    ///
    /// **Limitation:** when multiple orders are concurrently `OpenInFlight`, pending fills
    /// for all of them share the same `Vec` and cannot be distinguished by the cancelled
    /// order's exchange `OrderId` (which is unknown at cancel time). The drain is therefore
    /// deferred until the last `OpenInFlight` order is resolved, at which point any
    /// remaining unmatched fills are discarded with a warning.
    pub fn update_from_cancel_response(
        &mut self,
        response: &OrderResponseCancel<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        ExchangeKey: Debug + Clone,
        AssetKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
    {
        self.orders
            .update_from_cancel_response::<AssetKey>(response);
        self.cleanup_routing_tables();

        // Drain orphaned pending fills once no OpenInFlight orders remain.
        if !self.pending_fills.is_empty() {
            let still_has_in_flight = self
                .orders
                .0
                .values()
                .any(|o| matches!(o.state, ActiveOrderState::OpenInFlight(_)));
            if !still_has_in_flight {
                warn!(
                    count = self.pending_fills.len(),
                    "Draining pending fills: no OpenInFlight orders remain after cancel ack \
                     (cancel-before-ack race). Fills are unrecoverable."
                );
                self.pending_fills.clear();
            }
        }
    }

    /// Updates the instrument state based on a new trade.
    ///
    /// This method handles:
    /// - Computing and applying the configured fee model to the trade.
    /// - Opening/updating the current position state based on a new trade.
    /// - Updating the internal [`TearSheetGenerator`] if a position is exited.
    ///
    /// # Hedging mode caveat
    ///
    /// In `OmsMode::Hedging`, position flips (a fill that crosses zero) are
    /// **undefined**. The current implementation re-inserts the flipped
    /// opposite-direction position under the same `PositionId`, after which
    /// subsequent fills routed to that ID will update the wrong-direction
    /// position. Strategies running in Hedging mode must close existing
    /// positions explicitly rather than rely on flip semantics.
    pub fn update_from_trade(
        &mut self,
        trade: &Trade<AssetKey, InstrumentKey>,
    ) -> Option<PositionExited<AssetKey, InstrumentKey>>
    where
        AssetKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // Step 1: Resolve PositionId.
        //
        // Done BEFORE fee computation so we can return early (queue the fill) without
        // cloning the trade unnecessarily.
        //
        // In Netting mode the ID is always NETTING. In Hedging mode we use a two-level
        // lookup: first an O(1) reverse index (exchange_id → CID → PositionId), then a
        // fallback O(n) scan for CancelInFlight orders and orders not yet indexed.
        let position_id: PositionId = match self.position.mode {
            OmsMode::Netting => PositionId::NETTING,
            OmsMode::Hedging => {
                // Fast path: O(1) via the reverse index built in update_from_order_snapshot.
                let fast_cid = self.exchange_id_to_cid.get(&trade.order_id);
                let fast_pos_id = fast_cid.and_then(|cid| self.position_ids.get(cid)).cloned();

                if let Some(pos_id) = fast_pos_id {
                    pos_id
                } else {
                    // Slow path: O(active_orders) scan via find_map with early exit.
                    // Needed for CancelInFlight orders and any orders not yet in the index
                    // (e.g., pre-existing at startup, or external orders).
                    //
                    // Returns Option<Option<PositionId>>:
                    //   - None: no matching order found
                    //   - Some(None): match found but no position_id mapping
                    //   - Some(Some(pos_id)): match found with position_id
                    let matched =
                        self.orders
                            .0
                            .iter()
                            .find_map(|(cid, order)| match &order.state {
                                ActiveOrderState::Open(open) if open.id == trade.order_id => {
                                    Some(self.position_ids.get(cid).cloned())
                                }
                                ActiveOrderState::CancelInFlight(cf)
                                    if cf
                                        .order
                                        .as_ref()
                                        .is_some_and(|o| o.id == trade.order_id) =>
                                {
                                    Some(self.position_ids.get(cid).cloned())
                                }
                                _ => None,
                            });

                    match matched {
                        Some(Some(pos_id)) => pos_id,
                        Some(None) => {
                            // Found matching order but no position_id mapping. This occurs
                            // for external orders (placed outside this engine) or orders
                            // restored from exchange snapshot after restart. Route to a
                            // position keyed by the raw OrderId.
                            let pos_id = PositionId::new(trade.order_id.0.clone());
                            warn!(
                                order_id = %trade.order_id,
                                position_id = %pos_id,
                                "Hedging fill: order found but no position_id mapping — \
                                 using raw OrderId as position key"
                            );
                            pos_id
                        }
                        None => {
                            // No Open/CancelInFlight order matched. Two cases:
                            //
                            // (a) Fill-before-ack race: fill arrived before the REST ack
                            //     that maps its exchange OrderId to this order's ClientOrderId.
                            //     The order is still OpenInFlight. Queue for replay after ack.
                            //
                            // (b) Truly external order (not submitted through this engine,
                            //     or removed by snapshot reconciliation). Fall back to raw
                            //     OrderId as a best-effort position key.
                            //
                            // Check for OpenInFlight only in this no-match case (avoids
                            // unnecessary scan when match is found in the common case).
                            let has_in_flight = self.orders.0.values().any(|order| {
                                matches!(order.state, ActiveOrderState::OpenInFlight(_))
                            });
                            if has_in_flight {
                                debug!(
                                    order_id = %trade.order_id,
                                    "Hedging fill arrived before order ack (OpenInFlight \
                                     race) — queuing for replay after ack"
                                );
                                self.pending_fills.push(trade.clone());
                                return None;
                            }

                            let pos_id = PositionId::new(trade.order_id.0.clone());
                            warn!(
                                order_id = %trade.order_id,
                                position_id = %pos_id,
                                "Hedging fill routing: no order match — opening new \
                                 position under raw order ID. Occurs for externally-placed \
                                 orders or orders removed by snapshot reconciliation."
                            );
                            pos_id
                        }
                    }
                }
            }
        };

        // Step 2: Extract contract_size and apply fee model to the trade.
        //
        // contract_size is the multiplier for derivatives (options, futures, perpetuals).
        // For spot instruments this is 1. Used for both fee computation and PnL calculation.
        let contract_size = self.instrument.kind.contract_size();

        let computed_fee = self
            .fee_model
            .compute_fee(trade.price, trade.quantity, contract_size);

        let augmented;
        let effective_trade = if computed_fee.is_zero() {
            trade
        } else {
            augmented = Trade {
                fees: rustrade_execution::trade::AssetFees {
                    asset: trade.fees.asset.clone(),
                    fees: trade.fees.fees + computed_fee,
                    // computed_fee is in quote terms; add to fees_quote if available
                    fees_quote: trade.fees.fees_quote.map(|fq| fq + computed_fee),
                },
                ..trade.clone()
            };
            &augmented
        };

        // Step 3: Update the position.
        //
        // Pass &position_id (not owned) so callers avoid one SmolStr heap-allocation
        // per fill in Hedging mode with UUID-length PositionIds (PERF-3).
        // Pass contract_size so PnL is computed with the correct multiplier.
        let exited = self
            .position
            .update_from_trade_with_id(effective_trade, &position_id, contract_size)
            .inspect(|closed| self.tear_sheet.update_from_position(closed));

        // Step 4: Cleanup — remove CID→PositionId entries for the closed position,
        // but only for CIDs no longer tracked in orders.0.
        //
        // Multiple CIDs may reference the same position_id in Hedging mode (e.g., an
        // opening order and one or more closing orders all routing to the same PositionId).
        // Removing all matching entries indiscriminately would prune routing for still-active
        // closing orders; their subsequent fills would fall through to the raw-OrderId
        // fallback and open spurious positions. Preserving entries for CIDs still in
        // orders.0 ensures correct routing for any pending fills on those orders.
        if exited.is_some() {
            self.position_ids
                .retain(|cid, v| *v != position_id || self.orders.0.contains_key(cid));
        }

        exited
    }

    /// Updates the instrument state based on a new market event.
    ///
    /// If the market event has a price associated with it (eg/ `PublicTrade`, `OrderBookL1`), any
    /// open [`Position`](super::position::Position) `pnl_unrealised` is re-calculated.
    pub fn update_from_market(
        &mut self,
        event: &MarketEvent<InstrumentKey, InstrumentData::MarketEventKind>,
    ) where
        InstrumentData: InstrumentDataState<ExchangeKey, AssetKey, InstrumentKey>,
    {
        self.data.process(event);

        let Some(price) = self.data.price() else {
            return;
        };

        for position in self.position.positions.values_mut() {
            position.update_pnl_unrealised(price);
        }
    }
}

pub fn generate_unindexed_instrument_account_snapshot<
    InstrumentData,
    ExchangeKey,
    AssetKey,
    InstrumentKey,
>(
    exchange: ExchangeId,
    state: &InstrumentState<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>,
) -> InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    let InstrumentState {
        key: _,
        instrument,
        tear_sheet: _,
        position: _,
        orders,
        data: _,
        fee_model: _,
        expiration_processed: _,
        position_ids: _,
        pending_fills: _,
        exchange_id_to_cid: _,
    } = state;

    InstrumentAccountSnapshot {
        instrument: instrument.name_exchange.clone(),
        orders: orders
            .orders()
            .filter_map(|order| {
                let Order {
                    key,
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state: ActiveOrderState::Open(open),
                } = order
                else {
                    return None;
                };

                Some(Order {
                    key: OrderKey {
                        exchange,
                        instrument: instrument.name_exchange.clone(),
                        strategy: key.strategy.clone(),
                        cid: key.cid.clone(),
                    },
                    side: *side,
                    price: *price,
                    quantity: *quantity,
                    kind: *kind,
                    time_in_force: *time_in_force,
                    state: OrderState::active(open.clone()),
                })
            })
            .collect(),
        position: None,
    }
}

/// Generates an indexed [`InstrumentStates`]. Uses default values for
pub fn generate_indexed_instrument_states<'a, FnPosMan, FnOrders, FnInsData, InstrumentData>(
    instruments: &'a IndexedInstruments,
    time_engine_start: DateTime<Utc>,
    position_manager_init: FnPosMan,
    orders_init: FnOrders,
    instrument_data_init: FnInsData,
) -> InstrumentStates<InstrumentData>
where
    FnPosMan: Fn() -> PositionManager,
    FnOrders: Fn() -> Orders,
    FnInsData: Fn(
        &'a Keyed<InstrumentIndex, Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
    ) -> InstrumentData,
{
    InstrumentStates(
        instruments
            .instruments()
            .iter()
            .map(|instrument| {
                let exchange_index = instrument.value.exchange.key;

                (
                    instrument.value.name_internal.clone(),
                    InstrumentState {
                        key: instrument.key,
                        instrument: instrument.value.clone().map_exchange_key(exchange_index),
                        tear_sheet: TearSheetGenerator::init(time_engine_start),
                        position: position_manager_init(),
                        orders: orders_init(),
                        data: instrument_data_init(instrument),
                        fee_model: FeeModelConfig::default(),
                        expiration_processed: false,
                        position_ids: FnvHashMap::default(),
                        pending_fills: Vec::new(),
                        exchange_id_to_cid: FnvHashMap::default(),
                    },
                )
            })
            .collect(),
    )
}
