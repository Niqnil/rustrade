use crate::{
    engine::{
        UnsupportedCorporateActionReason,
        state::{
            instrument::{data::InstrumentDataState, filter::InstrumentFilter},
            order::{Orders, manager::OrderManager},
            position::{
                OmsMode, PnlUnrealisedUpdate, PositionExited, PositionManager, PreparedSplit,
                SplitError, SplitRoundingPolicy,
            },
        },
    },
    statistic::summary::instrument::TearSheetGenerator,
};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use itertools::Either;
use rust_decimal::Decimal;
use rustrade_data::event::MarketEvent;
use rustrade_execution::{
    FeeModel, FeeModelConfig, InstrumentAccountSnapshot, Liquidity,
    order::{
        Order, OrderKey,
        id::{ClientOrderId, OrderId, PositionId, VenueOrderId},
        request::OrderResponseCancel,
        state::{ActiveOrderState, InactiveOrderState, OrderState},
    },
    trade::Trade,
};
use rustrade_instrument::{
    Keyed,
    asset::{AssetIndex, name::AssetNameExchange},
    corporate_action::SplitRatio,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::InstrumentKind,
        name::{InstrumentNameExchange, InstrumentNameInternal},
    },
};
use rustrade_integration::collection::{FnvIndexMap, snapshot::Snapshot};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
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

/// The full, pre-computed mutation a stock split applies — produced by
/// [`InstrumentStates::prepare_corporate_action_split`] with **no** mutation, then committed by the
/// corporate-action handler (and its audit replica).
///
/// Splitting *prepare* (fallible arithmetic) from *commit* (infallible writes) makes the whole
/// action atomic by construction: every `Decimal` overflow — including the option **strike**
/// division — and every corrupted option-contract count is caught while building this plan, before
/// any position or strike is touched, so a partially-applied split is impossible. Both the live
/// handler and the audit replica build the plan from the same single-sourced pass, so they reach the
/// identical accept/reject decision and identical committed values.
pub(crate) struct SplitPlan {
    /// Pre-computed rescale for every open position on the splitting equity, in position-map order.
    pub(crate) equity_positions: Vec<(PositionId, PreparedSplit)>,
    /// One entry per registered option on the underlying (held OR unheld) for a **standard** split;
    /// empty for a non-standard split (which touches no option state).
    pub(crate) options: Vec<OptionSplitPlan>,
    /// Registered options on the underlying that already carry this action's `id` in their own
    /// `corporate_actions_processed` set, and are therefore **excluded** from `options` — this
    /// action already adjusted them, so re-adjusting would double-divide the strike.
    ///
    /// Carried (rather than dropped) purely so the live handler can surface each suppression as an
    /// observable; the audit replica mirrors state, not outputs, and ignores it.
    pub(crate) options_already_adjusted: Vec<InstrumentIndex>,
}

/// The pre-computed standard-split adjustment for a single option instrument on the splitting
/// underlying: its checked post-split strike plus a [`PreparedSplit`] for each held position.
pub(crate) struct OptionSplitPlan {
    /// The option instrument to adjust in place.
    pub(crate) key: InstrumentIndex,
    /// `strike ÷ ratio`, pre-checked for `Decimal` overflow. Applied to the option whether it is
    /// held OR unheld, so the registry stays consistent for positions opened later.
    pub(crate) strike_post_split: Decimal,
    /// Pre-computed rescale for each held position on this option, in position-map order. Empty for
    /// an unheld option (the strike correction is its whole adjustment).
    pub(crate) positions: Vec<(PositionId, PreparedSplit)>,
}

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

    /// Pre-compute a stock split against **every** position and option it would mutate, **without
    /// mutating anything**, returning the full [`SplitPlan`] the handler will commit — so
    /// `process_corporate_action` (and its audit replica) can reject an un-applicable action
    /// *atomically*: no partial rescaling on an overflowing feed, a corrupted option contract count,
    /// or an option strike that would overflow on division.
    ///
    /// Single-sourced so the live handler and the audit replica reach the identical decision — and
    /// the identical pre-computed values — by construction, not by hand-mirrored vigilance. Checks,
    /// in the same order the handler commits:
    /// - `equity` is the **unique** split-eligible instrument on its `(base, quote, exchange)`
    ///   underlying identity ⇒ otherwise [`UnsupportedCorporateActionReason::AmbiguousSplitTarget`]
    ///   (see that variant for why a second eligible instrument makes the option scan unsound);
    /// - every open position on the splitting `equity` rescales without `Decimal` overflow (equity
    ///   quantities may legitimately be fractional, so there is no integer check here);
    /// - iff `adjust_options_in_place` (a standard, whole-number forward split), for every registered
    ///   option on the same underlying (held **or** unheld): its **strike** divides by `ratio`
    ///   without `Decimal` overflow, and — for each **held** position — the contract count is an
    ///   **integer** (a non-integer count is state corruption) and rescales without overflow.
    ///
    /// # Per-option idempotency
    /// An option that already carries `id` in its **own** `corporate_actions_processed` set was
    /// already adjusted by this action, so it is excluded from [`SplitPlan::options`] (and listed in
    /// [`SplitPlan::options_already_adjusted`] for the caller to surface) rather than having its
    /// strike divided a second time. The target's own set is *not* consulted here — the caller
    /// guards that before calling.
    ///
    /// The strike check is why unheld options are pre-computed here at all: the handler divides the
    /// strike of *every* registered option in place (so a position opened later settles against the
    /// correct strike), and that division is otherwise unchecked. A non-standard split touches no
    /// option state, so only the equity positions are pre-computed for it.
    ///
    /// # Preconditions
    /// `equity` must already have been established as split-eligible
    /// ([`InstrumentKind::is_split_eligible`]) by the caller — this function **assumes** it rather
    /// than checking it, and `debug_assert!`s the assumption. It is a precondition and not a
    /// returned [`UnsupportedCorporateActionReason::InstrumentKindNotSupported`] because both
    /// callers must reject an ineligible target *before* the action kind is matched, so that a
    /// split delivered against an option is attributed to the instrument rather than to the
    /// action — an ordering this function is called too late to produce.
    ///
    /// Returns the [`UnsupportedCorporateActionReason`] the caller should surface on the first
    /// failure, or the [`SplitPlan`] to commit when the whole action can be applied.
    ///
    /// [`InstrumentKind::is_split_eligible`]: rustrade_instrument::instrument::kind::InstrumentKind::is_split_eligible
    pub(crate) fn prepare_corporate_action_split(
        &self,
        id: &SmolStr,
        equity: &InstrumentIndex,
        ratio: SplitRatio,
        policy: SplitRoundingPolicy,
        adjust_options_in_place: bool,
    ) -> Result<SplitPlan, UnsupportedCorporateActionReason> {
        let equity_state = self.instrument_index(equity);

        // Asserted, not checked: see `# Preconditions`. It earns an assertion because an ineligible
        // target is not inert here — the underlying identity below is derived FROM the target, so a
        // derivative reaching this function would have its own positions rescaled through the equity
        // leg (bypassing the option path's integer-contract check), and an option target would then
        // match its own scan and be adjusted a second time.
        debug_assert!(
            equity_state.instrument.kind.is_split_eligible(),
            "prepare_corporate_action_split: target is not split-eligible: {:?}",
            equity_state.instrument.kind
        );

        // The underlying identity every option scan below (and the handler's non-standard signal)
        // resolves against. Derived from the TARGET, so it is only a sound proxy for "this option
        // chain" while the target is the sole split-eligible instrument carrying it — which is
        // exactly what the next guard establishes.
        let base = equity_state.instrument.underlying.base;
        let quote = equity_state.instrument.underlying.quote;
        let exchange = equity_state.instrument.exchange;

        // Guard: the target must be the UNIQUE split-eligible instrument on that identity. A second
        // one is an equally valid trigger for adjusting the whole option chain, so the same chain
        // could be adjusted once per eligible instrument — each pass silent for unheld options and
        // recorded only against its own trigger. Reject the ambiguity instead of picking a winner:
        // nothing here can tell which instrument the chain is actually written on. Runs FIRST so an
        // ambiguous target is reported as such even when the arithmetic below would also fail.
        if self.0.values().any(|state| {
            state.key != *equity && state.is_split_eligible_on_underlying(&base, &quote, &exchange)
        }) {
            return Err(UnsupportedCorporateActionReason::AmbiguousSplitTarget);
        }

        // Equity positions: overflow only. A fractional equity quantity is legitimate (fractional-
        // share brokers), so there is no integer invariant to check here — unlike option contracts.
        let mut equity_positions = Vec::with_capacity(equity_state.position.positions.len());
        for (pos_id, position) in &equity_state.position.positions {
            // Match the variant explicitly (not `|_|`): the irrefutable `|SplitError::Overflow|`
            // closure pattern stops compiling (E0005) if a future non-overflow variant is added, so a
            // new cause surfaces here as a compile error rather than being silently mislabelled
            // `ArithmeticOverflow`. (Ordinary intra-crate exhaustiveness — `#[non_exhaustive]` only
            // governs downstream crates.)
            let prepared =
                position
                    .prepare_split(ratio, policy)
                    .map_err(|SplitError::Overflow| {
                        UnsupportedCorporateActionReason::ArithmeticOverflow
                    })?;
            equity_positions.push((pos_id.clone(), prepared));
        }

        // Non-standard splits leave all option state untouched (the handler only emits a signal),
        // so there is nothing further to pre-compute.
        if !adjust_options_in_place {
            return Ok(SplitPlan {
                equity_positions,
                options: Vec::new(),
                options_already_adjusted: Vec::new(),
            });
        }

        // Standard split: mirror the handler's option scan (base + quote + exchange identity) and
        // pre-compute, for EVERY registered option on the underlying (held OR unheld), the checked
        // post-split strike (`strike ÷ ratio`) plus the integer-contract invariant + overflow-safe
        // rescale of every HELD option position — all BEFORE the handler mutates any of them.
        let ratio_decimal = ratio.get();
        let mut options = Vec::new();
        let mut options_already_adjusted = Vec::new();
        for option_state in self
            .0
            .values()
            .filter(|state| state.is_option_on_underlying(&base, &quote, &exchange))
        {
            // Per-option idempotency: this action already adjusted this option (strike, and any
            // held positions), so re-running it would divide the strike a second time. Exclude it
            // from the plan and hand the key back for the caller to surface — a deliberate
            // no-op, not a failure, so it does not reject the action.
            if option_state.corporate_actions_processed.contains(id) {
                options_already_adjusted.push(option_state.key);
                continue;
            }

            // Strike overflow: pre-check the `strike ÷ ratio` the handler applies in place to every
            // registered option, held OR unheld — the fix for the previously unchecked strike
            // `DivAssign` (which could panic on a degenerate-but-positive ratio and was never
            // validated: the old pass checked only positions, skipping unheld options entirely).
            // `is_option_on_underlying` matched only Option instruments, so the `else` is a
            // structural invariant, mirroring the handler's loud arm.
            let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
                unreachable!(
                    "is_option_on_underlying matched a non-Option instrument {:?} (ratio={ratio})",
                    option_state.key
                );
            };
            let strike_post_split = contract
                .strike
                .checked_div(ratio_decimal)
                .ok_or(UnsupportedCorporateActionReason::ArithmeticOverflow)?;

            let mut positions = Vec::with_capacity(option_state.position.positions.len());
            for (pos_id, position) in &option_state.position.positions {
                // Option contract counts are whole; a non-integer count is corruption the handler
                // must not silently floor/carry. Surfaced as an observable rejection, not a panic.
                if !position.quantity_abs.fract().is_zero() {
                    return Err(UnsupportedCorporateActionReason::PositionStateInvalid);
                }
                // Held option legs rescale with `Fractional` (the integer invariant above makes the
                // equity `policy` a no-op), matching the handler's per-option commit. Variant match
                // (not `|_|`) so a future `SplitError` variant is a compile error here, not a silent
                // `ArithmeticOverflow` mislabel — same intra-crate exhaustiveness as the equity leg above.
                let prepared = position
                    .prepare_split(ratio, SplitRoundingPolicy::Fractional)
                    .map_err(|SplitError::Overflow| {
                        UnsupportedCorporateActionReason::ArithmeticOverflow
                    })?;
                positions.push((pos_id.clone(), prepared));
            }

            options.push(OptionSplitPlan {
                key: option_state.key,
                strike_post_split,
                positions,
            });
        }

        Ok(SplitPlan {
            equity_positions,
            options,
            options_already_adjusted,
        })
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

/// Maximum number of retired orders for which [`InstrumentState::retired_routing`] keeps fill
/// routing, per instrument.
///
/// The map exists to cover the gap between a venue reporting an order finished and reporting its
/// last fill, so the bound only has to span the orders that retire *within* that gap. Sixty-four
/// is generous for that and costs a few kilobytes per instrument at worst.
///
/// Eviction is oldest-retirement-first. A fill for an order evicted before it arrived routes by
/// the same fallback as before, and is counted by
/// [`TearSheet::fills_unmatched`](crate::statistic::summary::instrument::TearSheet::fills_unmatched).
pub const MAX_RETIRED_ROUTING: usize = 64;

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

    /// Set of corporate-action `id`s already applied to this instrument (idempotency key).
    ///
    /// A `CorporateAction` event carries a caller-assigned unique `id`; the handler records it
    /// here once applied and skips (with a warning) any `id` already present. Scope is
    /// per-instrument — naturally bounded by the number of corporate actions an instrument sees,
    /// so no global store / LRU is required.
    ///
    /// # Recorded on every instrument the action mutated, not only its target
    /// A stock split names one target instrument but also adjusts the strike of every registered
    /// option on that underlying, so the `id` is recorded on the target **and** on each option it
    /// adjusted. Each option's set is consulted independently before adjusting it, which is what
    /// makes a second trigger for the same `id` a no-op on the chain rather than a second strike
    /// division. Two consequences worth knowing:
    /// - an option carrying an `id` is evidence *that option* was adjusted — not that it was the
    ///   action's target;
    /// - a (nonsensical) action re-using that `id` and targeting the option directly is reported as
    ///   an idempotent skip rather than an unsupported instrument kind, because the option genuinely
    ///   did process that action.
    ///
    /// Revisit for future multi-instrument actions (e.g. spin-offs), which may need to record
    /// participation more richly than a flat `id` set.
    ///
    /// Rejected actions (unsupported instrument/action kind, ambiguous target, failed
    /// pre-validation) are deliberately **not** recorded, so they remain retryable once the
    /// blocking condition is resolved.
    ///
    /// A hash set (insertion order is never read — only `contains`/`insert`), consistent with the
    /// sibling `FnvHashMap` routing fields below.
    ///
    /// # Schema migration
    ///
    /// `#[serde(default)]` lets snapshots taken before this field existed deserialize with an empty
    /// set. Consequence: a consumer that snapshots an `InstrumentState` **after** applying a
    /// corporate action, then reloads it and re-injects the **same** action `id`, finds the set
    /// empty and applies the action twice (quantity doubled again, basis halved again). Idempotency
    /// holds within a live session; deduping replay across a pre-field snapshot is the consumer's
    /// responsibility (e.g. pre-populate this set with already-applied ids on upgrade).
    #[serde(default)]
    pub corporate_actions_processed: FnvHashSet<SmolStr>,

    /// Maps `ClientOrderId` → `PositionId` for hedging-mode fill routing.
    ///
    /// Populated by `InFlightRequestRecorder::record_in_flight_open` when an order carrying
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
    /// transition. Cleaned up by `cleanup_routing_tables` when orders leave
    /// `self.orders`.
    ///
    /// Without this index, `update_from_trade` must scan all active orders on every fill
    /// to find the order whose exchange `OrderId` matches `trade.order_id` — O(active orders)
    /// per fill. This index reduces that to two O(1) hash-map lookups.
    #[serde(default = "FnvHashMap::default")]
    pub exchange_id_to_cid: FnvHashMap<OrderId, ClientOrderId>,

    /// Fill routing for orders that have already left [`Self::orders`], so a fill arriving *after*
    /// its order's terminal snapshot still reaches the `PositionId` the strategy chose.
    ///
    /// # Why this is separate from the live maps
    ///
    /// [`Self::position_ids`] and [`Self::exchange_id_to_cid`] are keyed by orders still tracked in
    /// [`Self::orders`], and `cleanup_routing_tables` prunes them to exactly that set. Retiring an
    /// order therefore destroys the routing a late fill for it needs. `update_from_order_snapshot`
    /// compensates by re-inserting the entries for the order it has just retired — but into those
    /// same live maps, so retiring the *next* order prunes them again. That compensation covers one
    /// order per instrument; this map covers the rest.
    ///
    /// # Population
    ///
    /// By demotion, not by a second write path: `cleanup_routing_tables` joins each
    /// `exchange_id_to_cid` entry it is about to drop against `position_ids`, and moves the
    /// successful joins here. A join that fails contributes nothing, which is what keeps the
    /// corporate-action case out — that path prunes `position_ids` **by value** while leaving the
    /// order and its `exchange_id_to_cid` entry in place, so the join has no `PositionId` to find
    /// and the resting order's fills keep falling through to the fallback, as they must.
    ///
    /// Only populated under [`OmsMode::Hedging`], where a fill
    /// has more than one position slot it could belong to. Always empty under `Netting`.
    ///
    /// # Lookup order and precedence
    ///
    /// [`Self::update_from_trade`] consults this map only once both live lookups have missed, so a
    /// live order always wins over a retired one — relevant because exchange `OrderId` uniqueness
    /// is a venue guarantee this library does not enforce. It is consulted **before** the
    /// `OpenInFlight` check that buffers a fill into [`Self::pending_fills`], because a fill for an
    /// order that has already retired can never be released by a later ack.
    ///
    /// # Bound
    ///
    /// Insertion-ordered and capped at [`MAX_RETIRED_ROUTING`]; inserting at capacity evicts the
    /// oldest entry. Insertion order is retirement order, which is the order in which entries stop
    /// being worth keeping. There is deliberately no time-based reap: a fill belongs to its order's
    /// position however late it arrives, so retaining an entry on a quiet instrument costs only
    /// memory, and memory is what the cap already bounds.
    ///
    /// # Schema migration
    ///
    /// `#[serde(default)]` lets snapshots taken before this field existed deserialize with an empty
    /// map. A fill for an order retired before such a snapshot then routes by the fallback, exactly
    /// as it did before this field existed.
    #[serde(default = "FnvIndexMap::default")]
    pub retired_routing: FnvIndexMap<OrderId, PositionId>,

    /// The orders that were `Open` when this instrument's account stream last began to
    /// resynchronise, each with the venue order it was: the only orders the next account snapshot
    /// may retire by leaving them out.
    ///
    /// Recorded by [`Self::begin_account_resync`] and consumed by
    /// [`Self::update_from_account_snapshot`]. Empty until the first resync, so the snapshot that
    /// starts a run retires nothing. A snapshot that does not cover this instrument never reaches
    /// it, so the record then waits for the next resync, which replaces it. That is safe: every
    /// order in it was accepted before any later re-read of the venue.
    ///
    /// The venue order is kept so that a client id reused for a new order before the snapshot
    /// arrives does not retire the new order in the old one's place. Only a venue identifier can
    /// prove the tracked order is the one recorded, so an order the venue never named
    /// ([`VenueOrderId::ClientAssigned`]) is never retired this way.
    ///
    /// # Why only these
    ///
    /// A snapshot is read from the venue while order requests are still being answered, so an
    /// order the venue accepts just after the read can have its `Open` response reach the engine
    /// before the snapshot does. That order is live, yet the snapshot cannot list it. An order
    /// that was already `Open` when the resync began was accepted before the read began, so a
    /// complete snapshot that leaves it out is evidence that it is gone. The comparison needs no
    /// clock, only the order in which the engine received events.
    ///
    /// That rests on the account stream delivering its reconnect notice before it starts
    /// re-reading the venue, which `ExecutionManager` does. A consumer feeding the engine account
    /// events some other way must do the same.
    #[serde(default)]
    pub orders_open_at_resync: FnvHashMap<ClientOrderId, VenueOrderId>,
}

impl<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>
    InstrumentState<InstrumentData, ExchangeKey, AssetKey, InstrumentKey>
{
    /// `true` if this is an **option** written on the underlying `(base, quote)` traded on
    /// `exchange` — held or not. A standard (whole-number forward) split must divide the strike of
    /// **every** such registered option, not only those currently holding a position: the
    /// instrument set is fixed at construction, so an option that is unheld at split time can still
    /// have a position opened later and then settle at expiry against its strike. Leaving an unheld
    /// option on its pre-split strike would mis-settle that future position.
    ///
    /// Single-sources the option scan shared by the live engine handler
    /// (`process_corporate_action`) and the audit replica, so the predicate cannot drift between
    /// them. [`Self::is_affected_option_on_underlying`] layers the holds-a-position gate on top.
    pub(crate) fn is_option_on_underlying(
        &self,
        base: &AssetKey,
        quote: &AssetKey,
        exchange: &ExchangeKey,
    ) -> bool
    where
        AssetKey: PartialEq,
        ExchangeKey: PartialEq,
    {
        matches!(&self.instrument.kind, InstrumentKind::Option(_))
            && self.is_on_underlying(base, quote, exchange)
    }

    /// `true` if this instrument is itself **split-eligible** (the deliverable equity — see
    /// [`InstrumentKind::is_split_eligible`]) on the underlying `(base, quote)` traded on
    /// `exchange`.
    ///
    /// Used to establish that a corporate action's target is the **only** such instrument on that
    /// identity. It has to be, because the option chain is resolved by that identity alone: a
    /// second eligible instrument carrying it would be an equally valid trigger for adjusting the
    /// same options, with nothing in the state able to say which of the two the chain is written
    /// on. Shared by the live handler and the audit replica via
    /// [`InstrumentStates::prepare_corporate_action_split`].
    pub(crate) fn is_split_eligible_on_underlying(
        &self,
        base: &AssetKey,
        quote: &AssetKey,
        exchange: &ExchangeKey,
    ) -> bool
    where
        AssetKey: PartialEq,
        ExchangeKey: PartialEq,
    {
        self.instrument.kind.is_split_eligible() && self.is_on_underlying(base, quote, exchange)
    }

    /// `true` if this instrument's underlying pair is `(base, quote)` **and** it trades on
    /// `exchange` — the identity match shared by the kind-specific predicates above, with no
    /// [`InstrumentKind`] constraint of its own.
    ///
    /// Both `base` AND `quote` are matched: [`Underlying`](rustrade_instrument::instrument::Underlying)
    /// is a full pair identity, so without the quote filter a BTC/USDT action would also reach
    /// BTC/USDC instruments.
    fn is_on_underlying(&self, base: &AssetKey, quote: &AssetKey, exchange: &ExchangeKey) -> bool
    where
        AssetKey: PartialEq,
        ExchangeKey: PartialEq,
    {
        self.instrument.underlying.base == *base
            && self.instrument.underlying.quote == *quote
            && self.instrument.exchange == *exchange
    }

    /// `true` if this is an **option** on the underlying `(base, quote)` traded on `exchange` that
    /// currently **holds at least one open position** — i.e. the options whose held positions a
    /// corporate action must event-adjust (per-position rescale + observables) or, on a
    /// non-standard split, flag for a wrapper-side identity change.
    ///
    /// This is [`Self::is_option_on_underlying`] plus the non-empty-position gate. The strike
    /// correction on a standard split uses the broader [`Self::is_option_on_underlying`] (it must
    /// also reach unheld options); this narrower predicate selects the options that carry a
    /// position event.
    pub(crate) fn is_affected_option_on_underlying(
        &self,
        base: &AssetKey,
        quote: &AssetKey,
        exchange: &ExchangeKey,
    ) -> bool
    where
        AssetKey: PartialEq,
        ExchangeKey: PartialEq,
    {
        self.is_option_on_underlying(base, quote, exchange) && !self.position.positions.is_empty()
    }

    /// Updates the instrument state using an account snapshot from the exchange.
    ///
    /// Each order the snapshot lists is applied as an order snapshot, using timestamps where
    /// relevant so the most recent order state wins.
    ///
    /// # Orders the snapshot leaves out
    ///
    /// When the snapshot declares its order list complete
    /// ([`InstrumentAccountSnapshot::orders_complete`]), an order it does not list is no longer
    /// open at the venue: it filled, was cancelled or expired while the account stream was down.
    /// Such an order is retired, with a `warn!` naming it, provided it is in
    /// [`Self::orders_open_at_resync`] and is still `Open` as the venue order recorded there
    /// ([`Orders::remove_open`]). An `Open` order that fails only the last test is kept with a
    /// `warn!` of its own, since it may be a new order or may be gone. Absence cannot say how the order ended,
    /// so its `filled_quantity` is not touched; fills reach the position through the fill path,
    /// and a late one still routes through [`Self::retired_routing`].
    ///
    /// Left alone whatever the snapshot says:
    /// - an order in flight (`OpenInFlight`, `CancelInFlight`): its request is still being
    ///   answered, and the answer settles it;
    /// - an order that became `Open` after the resync began, which the snapshot may predate;
    /// - every order, when the list is not declared complete.
    ///
    /// The resync record is consumed either way, so each resync gives one snapshot the chance to
    /// retire orders. (A snapshot that does not cover the instrument does not call this; see
    /// [`Self::orders_open_at_resync`].)
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

        let open_at_resync = std::mem::take(&mut self.orders_open_at_resync);
        if snapshot.orders_complete && !open_at_resync.is_empty() {
            let listed = snapshot
                .orders
                .iter()
                .map(|order| &order.key.cid)
                .collect::<FnvHashSet<_>>();

            for (cid, id) in open_at_resync
                .iter()
                .filter(|(cid, _)| !listed.contains(cid))
            {
                if let Some(order) = self.orders.remove_open(cid, id) {
                    warn!(
                        instrument = ?order.key.instrument,
                        strategy = %order.key.strategy,
                        %cid,
                        state = ?order.state,
                        "InstrumentState retiring an Open order that a complete account snapshot \
                         no longer lists - it filled, was cancelled or expired while the account \
                         stream was down"
                    );
                } else if let Some(order) = self
                    .orders
                    .0
                    .get(cid)
                    .filter(|order| matches!(order.state, ActiveOrderState::Open(_)))
                {
                    // Kept, but possibly gone: say so rather than leave a stale order unremarked.
                    warn!(
                        instrument = ?order.key.instrument,
                        strategy = %order.key.strategy,
                        %cid,
                        recorded = %id,
                        state = ?order.state,
                        "InstrumentState keeping an Open order that a complete account snapshot \
                         does not list - its venue order id does not prove it is the order \
                         recorded when the account stream reconnected (a reused client id, or an \
                         order the venue never named)"
                    );
                }
            }
        }

        self.cleanup_routing_tables();
    }

    /// Records the orders that are `Open` as the account stream begins to resynchronise, making
    /// them the ones the next complete account snapshot may retire by leaving them out.
    ///
    /// Call it when the account stream reports that it is reconnecting, before the snapshot that
    /// the reconnect produces is applied. [`EngineState::update_from_account_reconnecting`] does
    /// this for each instrument on the exchange. See [`Self::orders_open_at_resync`] for why only
    /// these orders.
    ///
    /// A second call before a snapshot replaces the record: every order `Open` at the later call
    /// was accepted before any re-read that follows it.
    ///
    /// [`EngineState::update_from_account_reconnecting`]: crate::engine::state::EngineState::update_from_account_reconnecting
    pub fn begin_account_resync(&mut self) {
        self.orders_open_at_resync = self
            .orders
            .0
            .iter()
            .filter_map(|(cid, order)| match &order.state {
                ActiveOrderState::Open(open) => Some((cid.clone(), open.id.clone())),
                _ => None,
            })
            .collect();
    }

    /// Drop stale entries from `position_ids` and `exchange_id_to_cid` whose
    /// `ClientOrderId` is no longer present in `self.orders`. Called after every
    /// mutation that may transition an order to a terminal state — prevents both
    /// maps from growing unboundedly across the lifetime of a long-running engine
    /// in Hedging mode.
    ///
    /// # Terminal-state late fills (Hedging mode)
    ///
    /// Routing-table lifetime is coupled to membership of `self.orders`, so retiring an order
    /// would drop the `exchange_id → CID → PositionId` chain that a fill arriving *after* its
    /// terminal snapshot needs. Rather than dropping it, this method **demotes** it into
    /// [`Self::retired_routing`], which is not keyed by `self.orders` and therefore survives
    /// later retirements. [`Self::update_from_trade`] consults it once both live lookups miss.
    ///
    /// Only a chain that resolves end to end is demoted, and that is what keeps the
    /// corporate-action case out. That path prunes `position_ids` **by value** while leaving the
    /// order and its `exchange_id_to_cid` entry in place, so the join finds no `PositionId` and
    /// keeps nothing — deliberately, because routing a late fill there would reopen a position a
    /// reverse split floored to zero.
    ///
    /// [`Self::update_from_order_snapshot`] separately restores the *live* entries for the order
    /// it has just retired. That remains necessary rather than redundant: an order that fills
    /// fully on ack has no `exchange_id_to_cid` entry at the moment this method runs — it is
    /// written afterwards — so there is nothing here to demote, and those restored entries are
    /// what the *next* retirement's demotion finds.
    ///
    /// The window is bounded at [`MAX_RETIRED_ROUTING`] retirements per instrument, oldest
    /// evicted first. A fill for an order retired beyond that bound still opens a position under
    /// the raw `OrderId`.
    ///
    /// Additional mitigation: `AlpacaClient`'s dedup LRU cache filters fills whose
    /// `{order_id}:{filled_qty}` key was already processed, covering the most
    /// common duplicate-fill scenario.
    fn cleanup_routing_tables(&mut self) {
        // Demote before pruning. An entry about to be dropped is precisely the routing a fill
        // arriving after its order's terminal snapshot would need, so it moves to
        // `retired_routing` instead of being discarded.
        //
        // The join is what keeps this honest. Only an entry that still resolves all the way to a
        // `PositionId` is kept; the corporate-action split path prunes `position_ids` by value
        // while leaving the order and its `exchange_id_to_cid` entry in place, so its join fails
        // here and a late fill on that resting order still reaches the fallback, which is the
        // behaviour that stops a floored-out position being silently reopened.
        //
        // Hedging only: under `Netting` every fill keys to one slot, so there is no routing
        // decision to preserve. Collecting allocates nothing until there is something to demote,
        // which is only on a retirement -- not on the steady-state calls that dominate.
        if matches!(self.position.mode, OmsMode::Hedging) && !self.exchange_id_to_cid.is_empty() {
            let demoted: Vec<(OrderId, PositionId)> = self
                .exchange_id_to_cid
                .iter()
                .filter(|(_, cid)| !self.orders.0.contains_key(*cid))
                .filter_map(|(exchange_id, cid)| {
                    self.position_ids
                        .get(cid)
                        .map(|pos_id| (exchange_id.clone(), pos_id.clone()))
                })
                .collect();

            for (exchange_id, position_id) in demoted {
                self.insert_retired_routing(exchange_id, position_id);
            }
        }

        if !self.position_ids.is_empty() {
            self.position_ids
                .retain(|cid, _| self.orders.0.contains_key(cid));
        }
        if !self.exchange_id_to_cid.is_empty() {
            self.exchange_id_to_cid
                .retain(|_, cid| self.orders.0.contains_key(cid));
        }
    }

    /// Record fill routing for a retired order, evicting the oldest entry at
    /// [`MAX_RETIRED_ROUTING`].
    ///
    /// Re-recording an order already present refreshes its `PositionId` without moving it in the
    /// insertion order and without growing the map, which `IndexMap::insert` gives for free. That
    /// is the routine case rather than an edge one: an order tracked as `Open` is demoted at its
    /// own retirement, `update_from_order_snapshot` then restores its live entries, and the next
    /// retirement's cleanup demotes it a second time.
    fn insert_retired_routing(&mut self, exchange_id: OrderId, position_id: PositionId) {
        let at_capacity = !self.retired_routing.contains_key(&exchange_id)
            && self.retired_routing.len() >= MAX_RETIRED_ROUTING;

        if at_capacity
            && let Some((evicted_id, evicted_position)) = self.retired_routing.shift_remove_index(0)
        {
            debug!(
                evicted_order_id = %evicted_id,
                evicted_position_id = %evicted_position,
                capacity = MAX_RETIRED_ROUTING,
                "Retired-order fill routing at capacity — evicting oldest retirement. A fill \
                 for the evicted order would now open a position under its raw order ID."
            );
        }

        self.retired_routing.insert(exchange_id, position_id);
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
    ///
    /// # Late fills in `OmsMode::Hedging`
    ///
    /// A terminal update untracks the order, which would otherwise drop the routing entry that a
    /// fill arriving *after* it needs. This method re-establishes that entry for the order it has
    /// just retired, so the common ordering — venue reports the order finished, then reports the
    /// fill — still routes to the `PositionId` the caller supplied on the open request.
    ///
    /// Those restored entries live in the maps keyed by `self.orders`, so the *next* retirement
    /// prunes them. What outlives it is [`Self::retired_routing`], which `cleanup_routing_tables`
    /// demotes them into at that moment — so routing survives up to [`MAX_RETIRED_ROUTING`]
    /// retirements per instrument rather than one. Beyond that bound, or where a corporate action
    /// deliberately dropped the order's `PositionId` mapping, a fill opens a position under
    /// `PositionId::new(trade.order_id)`; see [`Self::update_from_trade`] for which tear-sheet
    /// counter records it and why they are counted apart.
    pub fn update_from_order_snapshot(
        &mut self,
        order: Snapshot<&Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>,
    ) -> Option<PositionExited<AssetKey, InstrumentKey>>
    where
        ExchangeKey: Debug + Clone,
        AssetKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // Count a rejected open before anything downstream consumes the snapshot. This is the
        // difference between "the strategy chose not to trade" and "this session could not
        // trade", and once the snapshot is dropped nothing can tell the two apart. Debug rather
        // than Display for the reason: `OrderError`'s Display needs `AssetKey: Display` and
        // `InstrumentKey: Display`, and requiring those here would narrow this method's callers.
        if let OrderState::Inactive(InactiveOrderState::OpenFailed(error)) = &order.0.state {
            self.tear_sheet.record_open_rejected(format!("{error:?}"));
        }

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

        // An order already tracked as `Open` whose life this update ends needs the same
        // treatment, for the opposite reason. The mapping is not being learned here, it is about
        // to be destroyed: `Orders::update_from_order_snapshot` untracks the order, so the
        // `cleanup_routing_tables` call below prunes both `exchange_id_to_cid` and
        // `position_ids` for its CID. A fill arriving after the terminal snapshot would then
        // resolve to nothing and open a position under the raw `OrderId`.
        //
        // The orderings are both real. A venue may report the fill before the terminal state
        // (handled by `currently_open_in_flight` above) or after it -- IBKR reports a working
        // `orderStatus` carrying `filled == quantity` while deliberately withholding the `Trade`
        // until the matching `CommissionReport` lands, and REST reconciliation can serialise an
        // order that completed mid-request on any venue.
        let currently_open = self
            .orders
            .0
            .get(&order.0.key.cid)
            .map(|o| matches!(o.state, ActiveOrderState::Open(_)))
            .unwrap_or(false);

        // Whether this update ends the order's life at the exchange. An explicitly terminal state,
        // or an `Open` with nothing left to fill -- some venues report a completed order that way
        // rather than with a distinct state, and it denotes the same fact.
        let update_retires_order = match &order.0.state {
            OrderState::Inactive(_) => true,
            OrderState::Active(ActiveOrderState::Open(open)) => {
                open.quantity_remaining(order.0.quantity).is_zero()
            }
            OrderState::Active(_) => false,
        };

        // Two kinds of update settle the CID <-> OrderId mapping, and both must drive (a) and
        // (b) below: a transition out of OpenInFlight that carries the exchange OrderId, which
        // learns the mapping, and a terminal update on an order tracked as Open, which is about
        // to lose it.
        //
        // FullyFilled is not an edge case: a venue that answers the REST open with an
        // already-filled order reports the fill and the ack in the same response, and never
        // publishes an intermediate Open. Binance (`newOrderRespType=FULL`), Alpaca and IBKR all
        // do this for marketable orders. When the corresponding Trade arrives on the websocket
        // first -- the common case, websockets being faster than a REST round trip -- it is parked
        // in `pending_fills` awaiting an ack that, if only Open were matched here, would never
        // qualify. The fill would then sit unreplayed for the rest of the run: the position never
        // opens while the balance is debited, leaving the two ledgers disagreeing.
        //
        // Nothing else qualifies. A steady-state `Open` -> `Open` update that leaves the order
        // working needs neither: its mapping is already indexed and no pruning is coming, so
        // widening this would pay a `ClientOrderId` clone on every call to re-insert what is
        // already there.
        let ack_exchange_id: Option<OrderId> =
            if currently_open_in_flight || (currently_open && update_retires_order) {
                match &order.0.state {
                    OrderState::Active(ActiveOrderState::Open(open)) => open.id.assigned().cloned(),
                    OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => {
                        Some(filled.id.clone())
                    }
                    _ => None,
                }
            } else {
                None
            };

        self.orders.update_from_order_snapshot(order);
        self.cleanup_routing_tables();

        // Mapping settled: index it (or re-index what cleanup just pruned) and replay any fills
        // that were waiting on it.
        if let Some(exchange_id) = ack_exchange_id {
            // Clone the CID here, not at method entry — paid only on the transitions that settle
            // the mapping, not on every call.
            let cid = order.0.key.cid.clone();
            // (a) PERF-1: O(1) reverse index for subsequent fill routing.
            self.exchange_id_to_cid
                .insert(exchange_id.clone(), cid.clone());

            // C1 fix: restore the CID → PositionId entry that cleanup_routing_tables removed
            // because this update retired the order (removed it from orders.0). Needed both by
            // the deferred replay below and by a fill still to come: without it
            // `update_from_trade` resolves the exchange OrderId to a CID that has no PositionId,
            // and falls back to opening a position under the raw OrderId.
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
    ///
    /// # Unroutable fills in Hedging mode
    ///
    /// A fill whose `PositionId` cannot be resolved opens a position keyed by its raw exchange
    /// `OrderId`. That **splits the order's PnL across two position slots** — the fill starts a
    /// second position rather than joining the one its own order opened — and no later event
    /// rejoins them.
    ///
    /// Two distinct things reach it:
    ///
    /// - The **late-fill window** documented on `cleanup_routing_tables`. Routing for a retired
    ///   order is demoted into [`Self::retired_routing`] rather than dropped, and this method
    ///   consults it once both live lookups have missed, so the window spans
    ///   [`MAX_RETIRED_ROUTING`] retirements per instrument instead of one. A fill for an order
    ///   retired beyond that bound still reaches the fallback.
    /// - The **corporate-action split path**, which deliberately drops a resting order's
    ///   `PositionId` mapping because retaining it would let a late fill reopen a position the
    ///   split floored to zero. This one is reachable by design, so the fallback is load-bearing
    ///   and no lifetime change removes it. Demotion cannot mask it either: demoting requires a
    ///   `PositionId` to join against, which is exactly what that path removed.
    ///
    /// What the caller is owed meanwhile is that it be visible, so each occurrence increments a
    /// counter on the [`TearSheetGenerator`] rather than only emitting a `warn!`:
    ///
    /// - [`TearSheet::fills_routed_by_fallback`](crate::statistic::summary::instrument::TearSheet::fills_routed_by_fallback)
    ///   — an order was found, but nothing said where its fills belong. This is the split.
    /// - [`TearSheet::fills_unmatched`](crate::statistic::summary::instrument::TearSheet::fills_unmatched)
    ///   — no order matched at all, so the fill is external or was reconciled away. One position
    ///   per external order is a defensible reading rather than a split.
    ///
    /// Both are `0` in `OmsMode::Netting`, where every fill keys to a single slot and no routing
    /// failure is possible. A consumer that reconciles positions should treat the first as an
    /// error signal and the second as expected only if it knows the account is traded elsewhere.
    /// The positions themselves are listed on
    /// [`TearSheet::fallback_positions`](crate::statistic::summary::instrument::TearSheet::fallback_positions),
    /// and both counters are summed onto
    /// [`TradingSummary`](crate::statistic::summary::TradingSummary) for the session.
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
                                ActiveOrderState::Open(open)
                                    if open.id.assigned() == Some(&trade.order_id) =>
                                {
                                    Some(self.position_ids.get(cid).cloned())
                                }
                                ActiveOrderState::CancelInFlight(cf)
                                    if cf.order.as_ref().is_some_and(|o| {
                                        o.id.assigned() == Some(&trade.order_id)
                                    }) =>
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
                            // Counted, not just logged: this splits the order's PnL across two
                            // position slots, and a log line is not something a consumer can
                            // reconcile against after the fact.
                            self.tear_sheet.record_fill_routed_by_fallback(&pos_id, || {
                                format!("no PositionId mapping for order {}", trade.order_id)
                            });
                            pos_id
                        }
                        None => {
                            // A fill for an order this engine has already retired. Its live routing was pruned
                            // when the order left `orders`, but `cleanup_routing_tables` demoted it
                            // rather than dropping it, so the `PositionId` the strategy chose is
                            // still recoverable and the order's PnL stays in one slot.
                            //
                            // Checked before the `OpenInFlight` buffer below, and that order
                            // matters: a buffered fill is only ever released by an ack for an order
                            // still tracked in `orders`, so a fill buffered for an already-retired
                            // order would never be replayed and never dropped.
                            if let Some(pos_id) = self.retired_routing.get(&trade.order_id).cloned()
                            {
                                debug!(
                                    order_id = %trade.order_id,
                                    position_id = %pos_id,
                                    "Hedging fill arrived after its order retired — routed by \
                                     retired-order routing"
                                );
                                pos_id
                            } else {
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
                                // Counted separately from the case above: the cause is a fill this
                                // engine has no order for, not a mapping it lost, and one position per
                                // external order is a defensible outcome rather than a split.
                                self.tear_sheet.record_fill_unmatched(&pos_id, || {
                                    format!("no order matched {}", trade.order_id)
                                });
                                pos_id
                            }
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

        // Taker, because a `Trade` does not say which side of the book it was on. Every execution
        // client reports fills without a maker/taker flag, so there is nothing here to read one
        // from, and guessing taker is the conservative direction: it overstates cost rather than
        // inventing profit. A venue that reports its own fees is the accurate path — see the
        // double-counting warning on `FeeModelConfig`.
        let computed_fee = self.fee_model.compute_fee(
            trade.price,
            trade.quantity,
            contract_size,
            Liquidity::Taker,
        );

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

        // Step 5: Advance the order this fill was against.
        //
        // Deliberately last. Advancing may retire the order, and every step above needs it still
        // tracked: the routing resolved in step 1 reads it, and step 4's join asks whether its CID
        // is still present. The same ordering an execution and its order snapshot are emitted in --
        // the execution first -- for the same reason.
        //
        // Only a venue that reports the order's cumulative alongside the execution reaches this;
        // `None` means the venue does not, and the order's state must still be learned from a
        // snapshot or a reconciliation fetch.
        if let Some(reported) = trade.order_filled_quantity {
            // O(1) through the reverse index, which is maintained in both OMS modes. The scan is
            // the same fallback step 1 uses: an order acknowledged but not yet indexed, or one
            // whose index entry a prune removed.
            let cid = self
                .exchange_id_to_cid
                .get(&trade.order_id)
                .cloned()
                .or_else(|| {
                    self.orders
                        .0
                        .iter()
                        .find_map(|(cid, order)| match &order.state {
                            ActiveOrderState::Open(open)
                                if open.id.assigned() == Some(&trade.order_id) =>
                            {
                                Some(cid.clone())
                            }
                            _ => None,
                        })
                });

            if let Some(cid) = cid
                && self
                    .orders
                    .update_from_fill(&cid, &trade.order_id, reported)
            {
                // The order was untracked, so the routing that pointed at it is now stale. This is
                // the same obligation a terminal order snapshot discharges.
                self.cleanup_routing_tables();
            }
        }

        exited
    }

    /// Updates the instrument state based on a new market event.
    ///
    /// If the market event has a price associated with it (eg/ `PublicTrade`, `OrderBookL1`), each
    /// open [`Position`](super::position::Position) has its `pnl_unrealised` re-calculated and its
    /// `time_exchange_update` advanced to the event's exchange timestamp.
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

        // The event is dispatched to this instrument, so `self.instrument` names it. Unlike the
        // generic `InstrumentKey`, `InstrumentNameInternal` is unconditionally `Display` — so the
        // diagnostic needs no `Debug` bound on the public method and logs a readable name
        // (`btc_usdt`) rather than an opaque index.
        let instrument = &self.instrument.name_internal;

        for position in self.position.positions.values_mut() {
            // A market fact landed regardless of whether the derived PnL turned out to be
            // representable, so advance the update clock unconditionally. This also finally
            // honours `Position::time_exchange_update`'s documented contract that a market-price
            // update advances it (previously no code did).
            position.time_exchange_update = event.time_exchange;

            if position.update_pnl_unrealised(price) == PnlUnrealisedUpdate::Overflowed {
                warn!(
                    %instrument,
                    %price,
                    "pnl_unrealised recompute overflowed Decimal; holding last-good value"
                );
            }
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
        corporate_actions_processed: _,
        position_ids: _,
        pending_fills: _,
        exchange_id_to_cid: _,
        retired_routing: _,
        orders_open_at_resync: _,
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
        // The engine's own record, not a read of the venue: it leaves out orders in flight, and
        // may still hold orders the venue has finished.
        orders_complete: false,
        position: None,
        isolated: None,
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
                (
                    instrument.value.name_internal.clone(),
                    InstrumentState {
                        key: instrument.key,
                        instrument: instrument
                            .value
                            .clone()
                            .map_exchange_key(|exchange| exchange.key),
                        tear_sheet: TearSheetGenerator::init(time_engine_start),
                        position: position_manager_init(),
                        orders: orders_init(),
                        data: instrument_data_init(instrument),
                        fee_model: FeeModelConfig::default(),
                        expiration_processed: false,
                        corporate_actions_processed: FnvHashSet::default(),
                        position_ids: FnvHashMap::default(),
                        pending_fills: Vec::new(),
                        exchange_id_to_cid: FnvHashMap::default(),
                        retired_routing: FnvIndexMap::default(),
                        orders_open_at_resync: FnvHashMap::default(),
                    },
                )
            })
            .collect(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use crate::engine::state::EngineState;
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::{
            OrderKind, TimeInForce,
            id::{ClientOrderId, OrderId, PositionId, StrategyId, VenueOrderId},
            state::{CancelInFlight, Cancelled, Open, OpenInFlight},
        },
        trade::{AssetFees, Trade, TradeId},
    };
    use rustrade_instrument::{Side, test_utils::instrument as test_instrument};

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
    const TIME: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

    /// The single instrument every test here trades, as an owned [`InstrumentState`] in the
    /// requested [`OmsMode`].
    ///
    /// Built through [`EngineState::builder`] rather than as a struct literal so the routing tables
    /// start in exactly the state the engine gives them, and so a field added to
    /// [`InstrumentState`] later cannot bypass this harness — the real construction path chooses
    /// its initial value, not a literal here that would silently keep compiling.
    fn instrument_state(
        mode: OmsMode,
    ) -> InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex> {
        let instruments = IndexedInstruments::new([test_instrument(EXCHANGE, "btc", "usdt")]);

        let state: EngineState<(), ()> = EngineState::builder(&instruments, (), |_| ())
            .oms_mode(mode)
            .time_engine_start(TIME)
            .build();

        state
            .instruments
            .0
            .into_iter()
            .next()
            .expect("builder was handed exactly one instrument")
            .1
    }

    /// An order snapshot in whichever `state` the caller needs, for the harness instrument.
    fn order(
        cid: ClientOrderId,
        state: OrderState<AssetIndex, InstrumentIndex>,
    ) -> Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>> {
        Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: StrategyId::new("strategy"),
                cid,
            },
            side: Side::Buy,
            price: Some(dec!(100)),
            quantity: dec!(10),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state,
        }
    }

    /// A fill reported against exchange `order_id`, with zero fees so an assertion reads the
    /// routing outcome rather than the fee model.
    /// The cumulative filled quantity the engine currently tracks for `cid`, or `None` if the
    /// order is no longer tracked.
    fn tracked_filled(
        state: &InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex>,
        cid: &ClientOrderId,
    ) -> Option<Decimal> {
        match &state.orders.0.get(cid)?.state {
            ActiveOrderState::Open(open) => Some(open.filled_quantity),
            _ => None,
        }
    }

    /// A venue that reports the order's cumulative on the fill advances the order from the fill
    /// alone, with no order snapshot involved. Both OMS modes: `Netting` resolves its position
    /// without ever looking the order up, so the advance has to be independent of that lookup.
    #[test]
    fn a_fill_reporting_its_cumulative_advances_the_order() {
        for mode in [OmsMode::Netting, OmsMode::Hedging] {
            let mut state = instrument_state(mode);
            let cid = ClientOrderId::new("cid-1");
            let exchange_id = OrderId::new("ex-1");
            let position_id = PositionId::new("pos-1");
            rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

            assert_eq!(
                tracked_filled(&state, &cid),
                Some(Decimal::ZERO),
                "{mode:?}"
            );

            state.update_from_trade(&fill_reporting(
                exchange_id.clone(),
                Side::Buy,
                dec!(3),
                dec!(3),
            ));
            assert_eq!(tracked_filled(&state, &cid), Some(dec!(3)), "{mode:?}");

            // The second fill reports the running total, not its own size.
            state.update_from_trade(&fill_reporting(
                exchange_id.clone(),
                Side::Buy,
                dec!(4),
                dec!(7),
            ));
            assert_eq!(tracked_filled(&state, &cid), Some(dec!(7)), "{mode:?}");
        }
    }

    /// Advancing to a cumulative is idempotent. A re-delivered fill, or two arriving out of
    /// order, must not move the order backwards or double-count -- which is the whole reason the
    /// venue's running total is carried rather than each execution's size accumulated.
    #[test]
    fn a_redelivered_or_out_of_order_fill_cannot_move_the_order_backwards() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("ex-1");
        let position_id = PositionId::new("pos-1");
        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        let fill = fill_reporting(exchange_id.clone(), Side::Buy, dec!(5), dec!(5));
        state.update_from_trade(&fill);
        assert_eq!(tracked_filled(&state, &cid), Some(dec!(5)));

        state.update_from_trade(&fill);
        assert_eq!(
            tracked_filled(&state, &cid),
            Some(dec!(5)),
            "re-delivering the same fill must not advance the order"
        );

        state.update_from_trade(&fill_reporting(
            exchange_id.clone(),
            Side::Buy,
            dec!(2),
            dec!(2),
        ));
        assert_eq!(
            tracked_filled(&state, &cid),
            Some(dec!(5)),
            "a fill reporting an older cumulative must not move the order backwards"
        );
    }

    /// A fill that leaves nothing to fill retires the order, exactly as a snapshot reporting the
    /// same thing does. Previously only a snapshot could, so a venue that sent neither left a
    /// resting order behind that no longer existed at the exchange.
    #[test]
    fn a_fill_completing_the_order_retires_it() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("ex-1");
        let position_id = PositionId::new("pos-1");
        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        // The test order's quantity is 10.
        state.update_from_trade(&fill_reporting(
            exchange_id.clone(),
            Side::Buy,
            dec!(10),
            dec!(10),
        ));

        assert!(
            !state.orders.0.contains_key(&cid),
            "an order with nothing left to fill must not stay tracked"
        );
        assert!(
            !state.position_ids.contains_key(&cid),
            "retiring the order must prune the routing that referred to it"
        );
    }

    /// A fill can advance an order but never create one, so a fill arriving after its order
    /// retired is inert. An order *snapshot* in the same position reaches a vacant-entry arm that
    /// inserts, which is why that path needs a liveness gate at the client and this one does not.
    #[test]
    fn a_fill_for_a_retired_order_cannot_resurrect_it() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("ex-1");
        let position_id = PositionId::new("pos-1");
        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        state.update_from_trade(&fill_reporting(
            exchange_id.clone(),
            Side::Buy,
            dec!(10),
            dec!(10),
        ));
        assert!(!state.orders.0.contains_key(&cid), "precondition: retired");

        state.update_from_trade(&fill_reporting(
            exchange_id.clone(),
            Side::Buy,
            dec!(10),
            dec!(10),
        ));
        assert!(
            !state.orders.0.contains_key(&cid),
            "a fill must not re-insert an order that has retired"
        );
    }

    /// `None` means the venue did not report a cumulative -- not that nothing is filled. The
    /// order is left for a snapshot or a reconciliation fetch to update, exactly as before.
    #[test]
    fn a_fill_without_a_reported_cumulative_leaves_the_order_untouched() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("ex-1");
        let position_id = PositionId::new("pos-1");
        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        assert_eq!(
            tracked_filled(&state, &cid),
            Some(Decimal::ZERO),
            "a fill carrying no cumulative must not be read as one"
        );
    }

    fn fill(
        order_id: OrderId,
        side: Side,
        quantity: Decimal,
    ) -> Trade<AssetIndex, InstrumentIndex> {
        fill_inner(order_id, side, quantity, None)
    }

    /// A fill whose venue also reported the order's cumulative filled quantity.
    fn fill_reporting(
        order_id: OrderId,
        side: Side,
        quantity: Decimal,
        cumulative: Decimal,
    ) -> Trade<AssetIndex, InstrumentIndex> {
        fill_inner(order_id, side, quantity, Some(cumulative))
    }

    fn fill_inner(
        order_id: OrderId,
        side: Side,
        quantity: Decimal,
        order_filled_quantity: Option<Decimal>,
    ) -> Trade<AssetIndex, InstrumentIndex> {
        Trade {
            order_filled_quantity,
            id: TradeId::new("trade"),
            order_id,
            instrument: InstrumentIndex(0),
            strategy: StrategyId::new("strategy"),
            time_exchange: TIME,
            side,
            price: dec!(100),
            quantity,
            fees: AssetFees {
                asset: AssetIndex(0),
                fees: Decimal::ZERO,
                fees_quote: Some(Decimal::ZERO),
            },
        }
    }

    /// Drive an order from submission to resting `Open` at the exchange, learning the
    /// `OrderId → ClientOrderId` mapping exactly as a venue ack would.
    ///
    /// `position_id` is what the strategy asked the fill to be booked against; passing `None`
    /// models an order this engine never submitted (external, or restored by reconciliation),
    /// which is precisely the state the raw-`OrderId` fallback exists to catch.
    fn rest_order_at_exchange(
        state: &mut InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex>,
        cid: &ClientOrderId,
        exchange_id: &OrderId,
        position_id: Option<&PositionId>,
    ) {
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(OpenInFlight),
        )));

        // `record_in_flight_open` writes this when the submitted request carries a `PositionId`.
        // It has to land while the CID is tracked in `orders.0`, or `cleanup_routing_tables`
        // prunes it on the very next snapshot.
        if let Some(position_id) = position_id {
            state.position_ids.insert(cid.clone(), position_id.clone());
        }

        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(Open::new(
                VenueOrderId::Assigned(exchange_id.clone()),
                TIME,
                Decimal::ZERO,
            )),
        )));
    }

    /// The `PositionId`s currently holding an open position, in map order.
    fn open_position_ids(
        state: &InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex>,
    ) -> Vec<PositionId> {
        state.position.positions.keys().cloned().collect()
    }

    // --- Routing that works -------------------------------------------------------------------
    //
    // These three pin the paths a correct fix must leave untouched.

    #[test]
    fn a_fill_routes_to_the_strategys_position_through_the_reverse_index() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        // Both tables are warm, so this is the O(1) fast path.
        assert_eq!(state.exchange_id_to_cid.get(&exchange_id), Some(&cid));
        assert_eq!(state.position_ids.get(&cid), Some(&position_id));

        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![position_id],
            "a routable fill belongs to the position the strategy named"
        );
    }

    #[test]
    fn a_fill_routes_to_the_strategys_position_when_the_reverse_index_is_cold() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        // Evict only the reverse index, leaving the order resting and its PositionId mapped. This
        // is the state an order restored by snapshot reconciliation is in, and it forces the O(n)
        // scan rather than the fast path.
        state.exchange_id_to_cid.clear();

        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![position_id],
            "the slow-path scan must reach the same position as the reverse index"
        );
    }

    #[test]
    fn a_fill_routes_to_the_strategys_position_while_its_cancel_is_in_flight() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        // A cancel is sent but not yet acked. The order is still working at the venue, so it can
        // still fill — the race the `CancelInFlight` arm of the scan exists for.
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(CancelInFlight {
                order: Some(Open::new(
                    VenueOrderId::Assigned(exchange_id.clone()),
                    TIME,
                    Decimal::ZERO,
                )),
            }),
        )));

        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![position_id],
            "a fill racing a cancel still belongs to the strategy's position"
        );
    }

    // --- The raw-`OrderId` fallback ------------------------------------------------------------
    //
    // Characterisation, not endorsement: these pin what the engine does today so a change to it is
    // visible as a diff here rather than as a silently different PnL split.

    #[test]
    fn a_fill_whose_order_lost_its_mapping_opens_a_position_under_the_raw_order_id() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        rest_order_at_exchange(&mut state, &cid, &exchange_id, Some(&position_id));

        // Open a position the fill below ought to join.
        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));
        assert_eq!(open_position_ids(&state), vec![position_id.clone()]);

        // Now lose just the CID → PositionId mapping, with the order still resting and still
        // indexed. `Some(None)`: the scan finds the order but nothing says where its fills go.
        state.position_ids.remove(&cid);

        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![position_id, PositionId::new(exchange_id.0.clone())],
            "TODAY: the second fill opens a phantom position keyed by the raw exchange OrderId \
             instead of joining the position its own order opened, splitting the PnL across two \
             slots with only a warning to say so"
        );
    }

    #[test]
    fn a_fill_for_an_untracked_order_opens_a_position_under_the_raw_order_id() {
        let mut state = instrument_state(OmsMode::Hedging);
        let exchange_id = OrderId::new("oid-external");

        // No order at all: placed outside this engine, or dropped by snapshot reconciliation.
        // Nothing is `OpenInFlight`, so the fill cannot be queued for a later ack either.
        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![PositionId::new(exchange_id.0.clone())],
            "TODAY: an unroutable fill opens a position keyed by the raw exchange OrderId"
        );
        assert!(
            state.pending_fills.is_empty(),
            "with nothing in flight there is no ack to wait for, so the fill is not queued"
        );
    }

    // --- Fill-before-ack, which is routed correctly ---------------------------------------------

    #[test]
    fn a_fill_arriving_before_the_ack_is_queued_rather_than_routed() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(OpenInFlight),
        )));
        state.position_ids.insert(cid.clone(), position_id.clone());

        // The websocket fill beats the REST ack, so no order carries this exchange OrderId yet.
        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        assert!(
            open_position_ids(&state).is_empty(),
            "the fill must not open a position before the ack says where it belongs"
        );
        assert_eq!(
            state.pending_fills.len(),
            1,
            "it is held until the ack supplies the OrderId → CID mapping"
        );
    }

    #[test]
    fn a_queued_fill_is_replayed_to_the_strategys_position_when_the_ack_lands() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");
        let position_id = PositionId::new("strategy-chosen");

        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(OpenInFlight),
        )));
        state.position_ids.insert(cid.clone(), position_id.clone());
        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        // The ack arrives and carries the exchange OrderId, settling the mapping.
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(Open::new(
                VenueOrderId::Assigned(exchange_id.clone()),
                TIME,
                dec!(4),
            )),
        )));

        assert!(
            state.pending_fills.is_empty(),
            "the ack drains everything queued against its OrderId"
        );
        assert_eq!(
            open_position_ids(&state),
            vec![position_id],
            "the replayed fill lands in the strategy's position, not under the raw OrderId"
        );
    }

    // --- The fallback is counted, not only logged ----------------------------------------------

    #[test]
    fn a_fill_whose_order_lost_its_mapping_is_counted_as_a_fallback_routing() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");

        rest_order_at_exchange(
            &mut state,
            &cid,
            &exchange_id,
            Some(&PositionId::new("strategy-chosen")),
        );
        state.position_ids.remove(&cid);

        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        assert_eq!(
            state.tear_sheet.fills_routed_by_fallback, 1,
            "a split position must be countable, not just greppable in the logs"
        );
        assert_eq!(
            state.tear_sheet.fills_unmatched, 0,
            "an order WAS found, so this is not the unmatched case"
        );
        assert_eq!(
            state.tear_sheet.first_fallback_detail.as_deref(),
            Some("no PositionId mapping for order oid-1"),
            "the first occurrence names the cause and the order it happened to"
        );
    }

    #[test]
    fn a_fill_for_an_untracked_order_is_counted_as_unmatched() {
        let mut state = instrument_state(OmsMode::Hedging);

        state.update_from_trade(&fill(OrderId::new("oid-external"), Side::Buy, dec!(4)));

        assert_eq!(state.tear_sheet.fills_unmatched, 1);
        assert_eq!(
            state.tear_sheet.fills_routed_by_fallback, 0,
            "no order was found, so this is not the lost-mapping case"
        );
        assert_eq!(
            state.tear_sheet.first_fallback_detail.as_deref(),
            Some("no order matched oid-external")
        );
    }

    #[test]
    fn a_routable_fill_counts_against_neither_cause() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");

        rest_order_at_exchange(
            &mut state,
            &cid,
            &exchange_id,
            Some(&PositionId::new("strategy-chosen")),
        );

        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        assert_eq!(state.tear_sheet.fills_routed_by_fallback, 0);
        assert_eq!(state.tear_sheet.fills_unmatched, 0);
        assert_eq!(
            state.tear_sheet.first_fallback_detail, None,
            "a clean session must leave the diagnostic empty, or it means nothing"
        );
    }

    #[test]
    fn the_first_fallback_detail_survives_a_later_failure_of_the_other_cause() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-1");
        let exchange_id = OrderId::new("oid-1");

        rest_order_at_exchange(
            &mut state,
            &cid,
            &exchange_id,
            Some(&PositionId::new("strategy-chosen")),
        );
        state.position_ids.remove(&cid);
        state.update_from_trade(&fill(exchange_id, Side::Buy, dec!(4)));

        // A different cause, later. The detail is shared between both counters, so this pins that
        // it is genuinely first-wins rather than last-wins.
        state.update_from_trade(&fill(OrderId::new("oid-external"), Side::Buy, dec!(4)));

        assert_eq!(state.tear_sheet.fills_routed_by_fallback, 1);
        assert_eq!(state.tear_sheet.fills_unmatched, 1);
        assert_eq!(
            state.tear_sheet.first_fallback_detail.as_deref(),
            Some("no PositionId mapping for order oid-1"),
            "the earliest cause is the one that identifies the session's problem"
        );
    }

    #[test]
    fn netting_mode_counts_no_fallback_routing_for_an_untracked_order() {
        let mut state = instrument_state(OmsMode::Netting);

        state.update_from_trade(&fill(OrderId::new("oid-external"), Side::Buy, dec!(4)));

        assert_eq!(
            (
                state.tear_sheet.fills_routed_by_fallback,
                state.tear_sheet.fills_unmatched
            ),
            (0, 0),
            "Netting resolves every fill to one slot, so neither counter can move"
        );
    }

    #[test]
    fn the_fallback_counters_reach_the_generated_tear_sheet() {
        use crate::statistic::time::Daily;

        let mut state = instrument_state(OmsMode::Hedging);
        state.update_from_trade(&fill(OrderId::new("oid-external"), Side::Buy, dec!(4)));

        // A counter the summary never carries is a counter nobody can read.
        let sheet = state.tear_sheet.generate(Decimal::ZERO, Daily);

        assert_eq!(sheet.fills_unmatched, 1);
        assert_eq!(sheet.fills_routed_by_fallback, 0);
        assert_eq!(
            sheet.first_fallback_detail.as_deref(),
            Some("no order matched oid-external")
        );
    }

    #[test]
    fn a_fallback_routing_records_the_position_it_opened_so_it_can_be_looked_up() {
        let mut state = instrument_state(OmsMode::Hedging);
        let exchange_id = OrderId::new("oid-external");

        state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(4)));

        assert_eq!(
            state.tear_sheet.fallback_positions,
            vec![PositionId::new(exchange_id.0.clone())],
            "a consumer reconciling this instrument needs the id, not a sentence to parse"
        );
        assert_eq!(
            open_position_ids(&state),
            state.tear_sheet.fallback_positions,
            "the recorded id must be the position that actually exists"
        );
    }

    #[test]
    fn repeated_fills_through_the_fallback_record_one_position_but_count_every_fill() {
        let mut state = instrument_state(OmsMode::Hedging);
        let exchange_id = OrderId::new("oid-external");

        for _ in 0..3 {
            state.update_from_trade(&fill(exchange_id.clone(), Side::Buy, dec!(1)));
        }

        assert_eq!(
            state.tear_sheet.fallback_positions.len(),
            1,
            "one order filling three times is one suspect position, not three"
        );
        assert_eq!(
            state.tear_sheet.fills_unmatched, 3,
            "the counter stays exact even though the list deduplicates"
        );
    }

    #[test]
    fn the_recorded_position_list_stops_at_its_cap_while_the_counter_does_not() {
        use crate::statistic::summary::instrument::MAX_FALLBACK_POSITIONS;

        let mut state = instrument_state(OmsMode::Hedging);
        let overshoot = MAX_FALLBACK_POSITIONS + 5;

        // A distinct external order each time, so every one is a new position.
        for i in 0..overshoot {
            state.update_from_trade(&fill(OrderId::new(format!("oid-{i}")), Side::Buy, dec!(1)));
        }

        assert_eq!(
            state.tear_sheet.fallback_positions.len(),
            MAX_FALLBACK_POSITIONS,
            "the list is bounded so a pathological session cannot grow the tear sheet without limit"
        );
        assert_eq!(
            state.tear_sheet.fills_unmatched, overshoot,
            "the counter remains authoritative for how many fills were misrouted"
        );
        assert_eq!(
            state.tear_sheet.fallback_positions.first(),
            Some(&PositionId::new("oid-0")),
            "the retained entries are the first seen, so the list pairs with first_fallback_detail"
        );
    }

    // --- Netting, where the fallback is unreachable ---------------------------------------------

    #[test]
    fn netting_mode_routes_an_unroutable_fill_to_the_single_netting_slot() {
        let mut state = instrument_state(OmsMode::Netting);

        // The same untracked fill that opens a phantom position under Hedging.
        state.update_from_trade(&fill(OrderId::new("oid-external"), Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![PositionId::NETTING],
            "Netting keys every fill to one slot, so no routing failure can split a position"
        );
        assert!(state.pending_fills.is_empty());
    }

    // --- Late-fill routing after retirement --------------------------------------------------
    //
    // `cleanup_routing_tables` demotes a retiring order's routing into `retired_routing` instead
    // of dropping it, so the window covers `MAX_RETIRED_ROUTING` retirements rather than the one
    // that `update_from_order_snapshot` restores live entries for.

    /// Retire an order by reporting it complete — an `Open` carrying its whole quantity as
    /// filled, which is how IBKR and REST reconciliation report a finished order.
    ///
    /// This is the path `update_from_order_snapshot` restores live routing for, so after this the
    /// order is reachable both live and (from the next retirement onwards) via `retired_routing`.
    fn complete_order(
        state: &mut InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex>,
        cid: &ClientOrderId,
        exchange_id: &OrderId,
    ) {
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(Open::new(
                VenueOrderId::Assigned(exchange_id.clone()),
                TIME,
                dec!(10),
            )),
        )));
    }

    /// Retire an order by cancelling it.
    ///
    /// Unlike completion, a `Cancelled` snapshot carries no `OrderId` that
    /// `update_from_order_snapshot`'s compensation recognises, so it strips the order's live
    /// routing outright rather than restoring it. A venue reporting an IOC's partial fill after
    /// the cancel that closed it lands exactly here — which is why `Cancelled` carries a
    /// `filled_quantity` at all.
    fn cancel_order(
        state: &mut InstrumentState<(), ExchangeIndex, AssetIndex, InstrumentIndex>,
        cid: &ClientOrderId,
        exchange_id: &OrderId,
    ) {
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::inactive(Cancelled::new(exchange_id.clone(), TIME, Decimal::ZERO)),
        )));
    }

    #[test]
    fn a_late_fill_routes_after_a_later_order_has_also_retired() {
        let mut state = instrument_state(OmsMode::Hedging);
        let (cid_a, oid_a, pos_a) = (
            ClientOrderId::new("cid-a"),
            OrderId::new("oid-a"),
            PositionId::new("position-a"),
        );
        let (cid_b, oid_b, pos_b) = (
            ClientOrderId::new("cid-b"),
            OrderId::new("oid-b"),
            PositionId::new("position-b"),
        );

        rest_order_at_exchange(&mut state, &cid_a, &oid_a, Some(&pos_a));
        complete_order(&mut state, &cid_a, &oid_a);

        // Retiring the *next* order is what used to close the window: its `cleanup_routing_tables`
        // call prunes the entries restored for A before any fill of A's could use them.
        rest_order_at_exchange(&mut state, &cid_b, &oid_b, Some(&pos_b));
        complete_order(&mut state, &cid_b, &oid_b);

        assert!(
            !state.exchange_id_to_cid.contains_key(&oid_a),
            "A's live routing is gone — precisely the state the retired map exists to cover"
        );
        assert_eq!(state.retired_routing.get(&oid_a), Some(&pos_a));

        state.update_from_trade(&fill(oid_a, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![pos_a],
            "a fill arriving after a later order also retired still belongs to its own position"
        );
        assert_eq!(state.tear_sheet.fills_routed_by_fallback, 0);
        assert_eq!(state.tear_sheet.fills_unmatched, 0);
    }

    #[test]
    fn a_late_fill_on_the_most_recently_completed_order_still_routes() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-a");
        let oid = OrderId::new("oid-a");
        let pos = PositionId::new("position-a");

        rest_order_at_exchange(&mut state, &cid, &oid, Some(&pos));
        complete_order(&mut state, &cid, &oid);

        state.update_from_trade(&fill(oid, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![pos],
            "the one-order compensation must keep working — demotion supplements it, not replaces it"
        );
        assert_eq!(state.tear_sheet.fills_unmatched, 0);
    }

    #[test]
    fn a_late_fill_after_a_cancel_routes() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-a");
        let oid = OrderId::new("oid-a");
        let pos = PositionId::new("position-a");

        rest_order_at_exchange(&mut state, &cid, &oid, Some(&pos));
        cancel_order(&mut state, &cid, &oid);

        // No compensation runs for a cancel, so before demotion existed this order's window was
        // zero retirements wide, not one.
        assert!(!state.exchange_id_to_cid.contains_key(&oid));
        assert_eq!(state.retired_routing.get(&oid), Some(&pos));

        state.update_from_trade(&fill(oid, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![pos],
            "an IOC's fill reported after the cancel that closed it belongs to its own position"
        );
        assert_eq!(state.tear_sheet.fills_unmatched, 0);
    }

    #[test]
    fn a_late_fill_is_not_buffered_behind_an_unrelated_in_flight_order() {
        let mut state = instrument_state(OmsMode::Hedging);
        let (cid_a, oid_a, pos_a) = (
            ClientOrderId::new("cid-a"),
            OrderId::new("oid-a"),
            PositionId::new("position-a"),
        );

        rest_order_at_exchange(&mut state, &cid_a, &oid_a, Some(&pos_a));
        cancel_order(&mut state, &cid_a, &oid_a);

        // An unrelated order still awaiting its ack. The no-match arm buffers a fill into
        // `pending_fills` whenever one of these exists — but a buffered fill is only released by
        // an ack for an order still tracked in `orders`, so a fill for the already-retired A would
        // never be replayed and never dropped.
        state.update_from_order_snapshot(Snapshot(&order(
            ClientOrderId::new("cid-c"),
            OrderState::active(OpenInFlight),
        )));

        state.update_from_trade(&fill(oid_a, Side::Buy, dec!(4)));

        assert!(
            state.pending_fills.is_empty(),
            "the retired map is consulted before the buffer, so nothing is parked here to leak"
        );
        assert_eq!(open_position_ids(&state), vec![pos_a]);
    }

    #[test]
    fn a_corporate_action_pruned_mapping_is_never_demoted() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-a");
        let oid = OrderId::new("oid-a");
        let pos = PositionId::new("position-a");

        rest_order_at_exchange(&mut state, &cid, &oid, Some(&pos));

        // What the corporate-action split handler does to a position it floors to zero: prune the
        // routing entry BY VALUE, leaving the resting order itself in place for the broker to
        // price-adjust.
        state.position_ids.retain(|_, mapped| *mapped != pos);

        // Retiring it now makes it a demotion candidate. The join must still fail.
        complete_order(&mut state, &cid, &oid);

        assert!(
            state.retired_routing.is_empty(),
            "demotion joins through `position_ids`, which is exactly what the split path removed"
        );

        state.update_from_trade(&fill(oid.clone(), Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![PositionId::new(oid.0.clone())],
            "the fill must reach the fallback, not silently reopen the floored-out position"
        );
        assert_eq!(state.tear_sheet.fills_unmatched, 1);
    }

    #[test]
    fn a_live_order_outranks_a_retired_entry_for_the_same_exchange_id() {
        let mut state = instrument_state(OmsMode::Hedging);
        let cid = ClientOrderId::new("cid-live");
        let oid = OrderId::new("oid-shared");
        let pos_live = PositionId::new("position-live");

        // Exchange `OrderId` uniqueness is a venue guarantee this library does not enforce, so
        // pin the precedence rather than assuming the collision cannot happen.
        state
            .retired_routing
            .insert(oid.clone(), PositionId::new("position-stale"));

        rest_order_at_exchange(&mut state, &cid, &oid, Some(&pos_live));
        state.update_from_trade(&fill(oid, Side::Buy, dec!(4)));

        assert_eq!(
            open_position_ids(&state),
            vec![pos_live],
            "a live order's routing wins over a retired entry for the same exchange id"
        );
    }

    #[test]
    fn retired_routing_evicts_the_oldest_retirement_at_capacity() {
        let mut state = instrument_state(OmsMode::Hedging);

        let ids: Vec<(ClientOrderId, OrderId, PositionId)> = (0..=MAX_RETIRED_ROUTING)
            .map(|i| {
                (
                    ClientOrderId::new(format!("cid-{i}")),
                    OrderId::new(format!("oid-{i}")),
                    PositionId::new(format!("position-{i}")),
                )
            })
            .collect();

        for (cid, oid, pos) in &ids {
            rest_order_at_exchange(&mut state, cid, oid, Some(pos));
            cancel_order(&mut state, cid, oid);
        }

        assert_eq!(
            state.retired_routing.len(),
            MAX_RETIRED_ROUTING,
            "one retirement past capacity must not grow the map"
        );

        let (_, oldest_oid, _) = &ids[0];
        let (_, newest_oid, newest_pos) = &ids[MAX_RETIRED_ROUTING];
        assert!(
            !state.retired_routing.contains_key(oldest_oid),
            "eviction is oldest-retirement-first"
        );
        assert_eq!(state.retired_routing.get(newest_oid), Some(newest_pos));

        // A fill for the evicted order routes as it did before this map existed, and is counted.
        state.update_from_trade(&fill(oldest_oid.clone(), Side::Buy, dec!(4)));
        assert_eq!(state.tear_sheet.fills_unmatched, 1);
        assert_eq!(
            open_position_ids(&state),
            vec![PositionId::new(oldest_oid.0.clone())]
        );
    }

    #[test]
    fn netting_mode_never_populates_retired_routing() {
        let mut state = instrument_state(OmsMode::Netting);
        let cid = ClientOrderId::new("cid-a");
        let oid = OrderId::new("oid-a");
        let pos = PositionId::new("position-a");

        rest_order_at_exchange(&mut state, &cid, &oid, Some(&pos));
        cancel_order(&mut state, &cid, &oid);

        assert!(
            state.retired_routing.is_empty(),
            "Netting keys every fill to one slot, so there is no routing decision to preserve"
        );

        state.update_from_trade(&fill(oid, Side::Buy, dec!(4)));
        assert_eq!(open_position_ids(&state), vec![PositionId::NETTING]);
    }

    // --- Retiring orders a complete account snapshot no longer lists --------------------------

    /// An account snapshot for the harness instrument listing `orders`, declared complete or not.
    fn account_snapshot(
        orders: Vec<Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>>>,
        orders_complete: bool,
    ) -> InstrumentAccountSnapshot<ExchangeIndex, AssetIndex, InstrumentIndex> {
        InstrumentAccountSnapshot {
            instrument: InstrumentIndex(0),
            orders,
            orders_complete,
            position: None,
            isolated: None,
        }
    }

    /// `cid` as the venue lists it: `Open` under `exchange_id`, nothing filled.
    fn listed(
        cid: &ClientOrderId,
        exchange_id: &OrderId,
    ) -> Order<ExchangeIndex, InstrumentIndex, OrderState<AssetIndex, InstrumentIndex>> {
        order(
            cid.clone(),
            OrderState::active(Open::new(
                VenueOrderId::Assigned(exchange_id.clone()),
                TIME,
                Decimal::ZERO,
            )),
        )
    }

    #[test]
    fn a_complete_snapshot_retires_an_open_order_it_no_longer_lists() {
        let mut state = instrument_state(OmsMode::Hedging);
        let (gone, gone_oid, gone_pos) = (
            ClientOrderId::new("cid-gone"),
            OrderId::new("oid-gone"),
            PositionId::new("position-gone"),
        );
        let (kept, kept_oid) = (ClientOrderId::new("cid-kept"), OrderId::new("oid-kept"));
        rest_order_at_exchange(&mut state, &gone, &gone_oid, Some(&gone_pos));
        rest_order_at_exchange(&mut state, &kept, &kept_oid, None);

        state.begin_account_resync();
        state.update_from_account_snapshot(&account_snapshot(vec![listed(&kept, &kept_oid)], true));

        assert!(!state.orders.0.contains_key(&gone));
        assert!(state.orders.0.contains_key(&kept));
        assert_eq!(
            state.retired_routing.get(&gone_oid),
            Some(&gone_pos),
            "a fill recovered after the retirement must still reach the order's position"
        );
        assert!(
            state.orders_open_at_resync.is_empty(),
            "the record is consumed"
        );
    }

    #[test]
    fn a_snapshot_not_declared_complete_retires_nothing() {
        let mut state = instrument_state(OmsMode::Netting);
        let (cid, oid) = (ClientOrderId::new("cid-1"), OrderId::new("oid-1"));
        rest_order_at_exchange(&mut state, &cid, &oid, None);

        state.begin_account_resync();
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), false));

        assert!(state.orders.0.contains_key(&cid));
        assert!(
            state.orders_open_at_resync.is_empty(),
            "the record is consumed"
        );
    }

    /// The race the resync record exists for: the venue accepts an order just after the snapshot
    /// read it, and the order's `Open` response reaches the engine before the snapshot does.
    #[test]
    fn an_order_opened_after_the_resync_began_survives_a_snapshot_that_cannot_list_it() {
        let mut state = instrument_state(OmsMode::Netting);
        let (cid, oid) = (ClientOrderId::new("cid-new"), OrderId::new("oid-new"));

        state.begin_account_resync();
        rest_order_at_exchange(&mut state, &cid, &oid, None);
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert!(state.orders.0.contains_key(&cid));
    }

    #[test]
    fn orders_in_flight_survive_a_complete_snapshot_that_leaves_them_out() {
        let mut state = instrument_state(OmsMode::Netting);

        // In flight before the resync began: never recorded.
        let opening = ClientOrderId::new("cid-opening");
        state.update_from_order_snapshot(Snapshot(&order(
            opening.clone(),
            OrderState::active(OpenInFlight),
        )));

        // `Open` when the resync began, with a cancel sent before the snapshot arrives: recorded,
        // but no longer `Open`, and the cancel's answer settles it.
        let (cancelling, cancelling_oid) = (
            ClientOrderId::new("cid-cancelling"),
            OrderId::new("oid-cancelling"),
        );
        rest_order_at_exchange(&mut state, &cancelling, &cancelling_oid, None);

        state.begin_account_resync();
        state.update_from_order_snapshot(Snapshot(&order(
            cancelling.clone(),
            OrderState::active(CancelInFlight { order: None }),
        )));
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert!(state.orders.0.contains_key(&opening));
        assert!(state.orders.0.contains_key(&cancelling));
    }

    /// A client id reused for a new order between the resync and the snapshot names a different
    /// venue order than the one recorded, so the new order is not retired in the old one's place.
    #[test]
    fn a_reused_client_id_does_not_retire_the_new_order() {
        let mut state = instrument_state(OmsMode::Netting);
        let cid = ClientOrderId::new("cid-reused");
        let (old_oid, new_oid) = (OrderId::new("oid-old"), OrderId::new("oid-new"));
        rest_order_at_exchange(&mut state, &cid, &old_oid, None);

        state.begin_account_resync();
        cancel_order(&mut state, &cid, &old_oid);
        rest_order_at_exchange(&mut state, &cid, &new_oid, None);
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert_eq!(
            state.orders.0.get(&cid).map(|order| &order.state),
            Some(&ActiveOrderState::Open(Open::new(
                VenueOrderId::Assigned(new_oid),
                TIME,
                Decimal::ZERO,
            )))
        );
    }

    /// An order the venue never named cannot be shown to be the one recorded, so even a complete
    /// snapshot that leaves it out keeps it.
    #[test]
    fn an_order_the_venue_never_named_survives_a_complete_snapshot_that_leaves_it_out() {
        let mut state = instrument_state(OmsMode::Netting);
        let cid = ClientOrderId::new("cid-unnamed");
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::active(OpenInFlight),
        )));
        let resting =
            ActiveOrderState::Open(Open::new(VenueOrderId::ClientAssigned, TIME, Decimal::ZERO));
        state.update_from_order_snapshot(Snapshot(&order(
            cid.clone(),
            OrderState::Active(resting.clone()),
        )));

        state.begin_account_resync();
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert_eq!(
            state.orders.0.get(&cid).map(|order| &order.state),
            Some(&resting)
        );
        assert!(state.orders_open_at_resync.is_empty());
    }

    /// The snapshot that starts a run has no resync before it, so it retires nothing.
    #[test]
    fn a_complete_snapshot_without_a_resync_retires_nothing() {
        let mut state = instrument_state(OmsMode::Netting);
        let (cid, oid) = (ClientOrderId::new("cid-1"), OrderId::new("oid-1"));
        rest_order_at_exchange(&mut state, &cid, &oid, None);

        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert!(state.orders.0.contains_key(&cid));
    }

    #[test]
    fn a_resync_gives_only_the_next_snapshot_the_chance_to_retire() {
        let mut state = instrument_state(OmsMode::Netting);
        let (cid, oid) = (ClientOrderId::new("cid-1"), OrderId::new("oid-1"));
        rest_order_at_exchange(&mut state, &cid, &oid, None);

        state.begin_account_resync();
        state.update_from_account_snapshot(&account_snapshot(vec![listed(&cid, &oid)], true));
        state.update_from_account_snapshot(&account_snapshot(Vec::new(), true));

        assert!(state.orders.0.contains_key(&cid));
    }

    /// The engine arms the record on every instrument of the reconnecting exchange, and on no
    /// other.
    #[test]
    fn an_account_reconnect_records_open_orders_on_that_exchange_only() {
        const OTHER: ExchangeId = ExchangeId::Kraken;
        let instruments = IndexedInstruments::new([
            test_instrument(EXCHANGE, "btc", "usdt"),
            test_instrument(OTHER, "eth", "usdt"),
        ]);
        let mut engine: EngineState<(), ()> =
            EngineState::builder(&instruments, (), |_| ()).build();
        let cid = ClientOrderId::new("cid-1");
        for (index, instrument) in engine.instruments.0.values_mut().enumerate() {
            let mut resting = listed(&cid, &OrderId::new("oid-1"));
            resting.key.exchange = ExchangeIndex(index);
            resting.key.instrument = InstrumentIndex(index);
            instrument.update_from_order_snapshot(Snapshot(&resting));
        }

        engine
            .update_from_account_reconnecting(&EXCHANGE)
            .expect("the harness exchange is tracked");

        let recorded = |exchange: ExchangeId| {
            engine
                .instruments
                .0
                .values()
                .find(|state| {
                    engine.connectivity.exchanges().get_index_of(&exchange)
                        == Some(state.instrument.exchange.index())
                })
                .expect("one instrument per exchange")
                .orders_open_at_resync
                .contains_key(&cid)
        };
        assert!(recorded(EXCHANGE));
        assert!(!recorded(OTHER));
    }
}
