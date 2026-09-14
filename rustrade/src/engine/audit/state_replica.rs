use crate::{
    EngineEvent,
    engine::{
        EngineMeta, EngineOutput, Processor, SplitCommitContext,
        audit::{AuditTick, EngineAudit, ProcessAudit, context::EngineContext},
        classify_option_split, split_plan_position_mut,
        state::{EngineState, instrument::data::InstrumentDataState},
    },
    execution::AccountStreamEvent,
};
use rustrade_integration::collection::none_one_or_many::NoneOneOrMany;
// (used by `update_from_event` to inspect the live engine's outputs)
use rustrade_data::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use rustrade_execution::AccountEvent;
use rustrade_instrument::{
    corporate_action::CorporateActionKind,
    instrument::{InstrumentIndex, kind::InstrumentKind},
};
use rustrade_integration::Terminal;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{info, info_span};

pub const AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME: &str = "audit_replica_state_update_span";

/// Manages a replica of an `EngineState` instance by processing AuditStream events produced by
/// the `Engine`.
///
/// Useful for supporting non-hot path trading system components such as UIs, web apps, etc.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct StateReplicaManager<State, Updates> {
    pub meta_start: EngineMeta,
    pub state_replica: AuditTick<State, EngineContext>,
    pub updates: Updates,
}

impl<State, Updates> StateReplicaManager<State, Updates> {
    /// Construct a new `StateReplicaManager` using the provided `EngineState` snapshot as a seed.
    pub fn new(snapshot: AuditTick<State>, updates: Updates) -> Self {
        Self {
            meta_start: EngineMeta {
                time_start: snapshot.context.time,
                sequence: snapshot.context.sequence,
                // The replica seeds from a snapshot taken before any drain begins, and follows the
                // live Engine's stop decision via `ProcessAudit::shutdown` rather than tracking a
                // drain of its own.
                draining: false,
            },
            state_replica: snapshot,
            updates,
        }
    }
}

impl<GlobalData, InstrumentData, Updates>
    StateReplicaManager<EngineState<GlobalData, InstrumentData>, Updates>
where
    InstrumentData: InstrumentDataState,
    GlobalData: for<'a> Processor<&'a AccountEvent>
        + for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>,
{
    /// Run the `StateReplicaManager`, managing a replica of an `EngineState` instance by processing
    /// AuditStream events produced by an `Engine`.
    pub fn run<OnDisable, OnDisconnect>(&mut self) -> Result<(), String>
    where
        Updates: Iterator<
            Item = AuditTick<
                EngineAudit<
                    EngineEvent<InstrumentData::MarketEventKind>,
                    EngineOutput<OnDisable, OnDisconnect>,
                >,
            >,
        >,
        OnDisable: Debug,
        OnDisconnect: Debug,
    {
        info!("StateReplicaManager running");

        // Create Tracing Span used to filter duplicate replica EngineState update logs
        let audit_span = info_span!(AUDIT_REPLICA_STATE_UPDATE_SPAN_NAME);
        let audit_span_guard = audit_span.enter();

        let shutdown_audit = loop {
            let Some(AuditTick {
                event: EngineAudit::Process(audit),
                context,
            }) = self.updates.next()
            else {
                break "FeedEnded";
            };

            if self.state_replica.context.sequence >= context.sequence {
                continue;
            } else {
                self.validate_and_update_context(context)?;
            }

            let shutdown = audit.is_terminal();

            let ProcessAudit { event, outputs, .. } = audit;
            self.update_from_event(event, &outputs);

            if shutdown {
                break "EngineEvent::Shutdown";
            }
        };

        // End Tracing Span used to filter duplicate EngineState update logs
        drop(audit_span_guard);

        info!(%shutdown_audit, "AuditManager stopped");

        Ok(())
    }

    fn validate_and_update_context(&mut self, next: EngineContext) -> Result<(), String> {
        if self.state_replica.context.sequence.value() != next.sequence.value() - 1 {
            return Err(format!(
                "AuditManager | out-of-order AuditStream | next: {:?} does not follow from {:?}",
                next.sequence, self.state_replica.context.sequence,
            ));
        }

        self.state_replica.context = next;
        Ok(())
    }

    /// Updates the internal `EngineState` using the provided `EngineEvent` and the
    /// `outputs` produced by the live engine for that same event.
    ///
    /// # Two distinct replay strategies
    ///
    /// Different event types use different update strategies — intentionally:
    ///
    /// - **`OrderSnapshot`** (via `Account` arm): replays the event directly on the replica
    ///   state. This is correct because the replica can independently compute the same state
    ///   transition as the live engine (order state machine is deterministic given the event).
    ///   Deferred fill replay (`update_from_order_snapshot`) also runs here, keeping the
    ///   replica's `pending_fills` in sync without needing the live engine's outputs.
    ///
    /// - **`ContractExpiry`**: consults `outputs` from the live engine rather than replaying
    ///   the event. This is necessary because `process_contract_expiry` is *conditional*: it
    ///   bails early (returning no exits) when the underlying spot price is unavailable. The
    ///   replica cannot independently determine which branch the live engine took, so it
    ///   mirrors the decision by inspecting `PositionExit` outputs.
    ///
    /// - **`CorporateAction`**: a *hybrid*. `Position::apply_split` is deterministic, so the
    ///   adjustment (quantity / basis / unrealised PnL) is **event-replayed** for every position
    ///   from the payload — after re-running the same guards (idempotency / non-`Spot` /
    ///   unsupported-kind, plus the ambiguous-target and pre-validation guards inside the shared
    ///   prepare pass, all of which skip without mutating). The Floor-to-zero **close** is
    ///   **output-mirrored** (like `ContractExpiry`): the live handler stamps the closing
    ///   `PositionExit.time_exit` with `self.time()`, which is wall-clock-derived on
    ///   `HistoricalClock` and therefore cannot be reproduced from the payload, so the replica
    ///   removes the slot and folds the *live* `PositionExit` into the tear sheet. Parity (incl.
    ///   tear sheets) is asserted by the replica-parity integration test, which guards against the
    ///   live handler and this arm drifting apart.
    ///
    /// Adding a new event type: choose event replay (deterministic transition) or output
    /// mirroring (conditional/non-deterministic) based on whether the replica can reproduce
    /// the live engine's branching from the event alone.
    pub fn update_from_event<OnDisable, OnDisconnect>(
        &mut self,
        event: EngineEvent<InstrumentData::MarketEventKind>,
        outputs: &NoneOneOrMany<EngineOutput<OnDisable, OnDisconnect>>,
    ) {
        match event {
            EngineEvent::Shutdown(_) | EngineEvent::Command(_) => {
                // No action required
            }
            EngineEvent::TradingStateUpdate(trading_state) => {
                let _audit = self
                    .replica_engine_state_mut()
                    .trading
                    .update(trading_state);
            }
            // The three `UntrackedExchange` results below are event-replayed, not output-mirrored:
            // the replica's `ConnectivityStates` is a clone of the live engine's, so the same
            // lookup against the same map reaches the same verdict from the event alone. On `Err`
            // the live engine mutated nothing, and so does the replica — discarding the result
            // *is* the mirror. (The live engine already emitted the observable and logged it; the
            // replica mirrors state, not outputs.)
            EngineEvent::Account(event) => match event {
                AccountStreamEvent::Reconnecting(exchange) => {
                    let _untracked = self
                        .replica_engine_state_mut()
                        .connectivity
                        .update_from_account_reconnecting(&exchange);
                }
                AccountStreamEvent::Item(event) => {
                    self.replica_engine_state_mut().update_from_account(&event);
                }
            },
            EngineEvent::Market(event) => match event {
                MarketStreamEvent::Reconnecting(exchange) => {
                    let _untracked = self
                        .replica_engine_state_mut()
                        .connectivity
                        .update_from_market_reconnecting(&exchange);
                }
                MarketStreamEvent::Item(event) => {
                    let _untracked = self.replica_engine_state_mut().update_from_market(&event);
                }
            },
            EngineEvent::ContractExpiry(key) => {
                // The live engine's `process_contract_expiry` is conditional: if the
                // underlying spot price is unavailable, it returns early without
                // mutating state and emits no `PositionExit` outputs. The replica
                // mirrors this by deciding from the outputs of *this* audit tick:
                //
                // - Any `PositionExit` output → live engine processed expiry → clear
                //   positions and mark processed.
                // - No `PositionExit` outputs but instrument has no positions → live
                //   engine took the empty branch and marked it processed → mark only.
                // - No `PositionExit` outputs and positions exist → live engine bailed
                //   on missing spot price → leave state untouched (event is retryable).
                let state = self.replica_engine_state_mut();
                let instrument_state = state.instruments.instrument_index_mut(&key);
                let any_exit = outputs
                    .iter()
                    .any(|o| matches!(o, EngineOutput::PositionExit(_)));
                // Mirror the live engine's per-position loop: remove exactly the
                // positions that were reported as exited via PositionExit outputs,
                // rather than clearing all positions atomically. This ensures the
                // replica stays correct even if the live engine's loop skips a
                // position slot (e.g., due to a race condition or future partial-
                // settlement logic).
                if any_exit {
                    for output in outputs.iter() {
                        if let EngineOutput::PositionExit(exit) = output {
                            // Guard: only remove positions that belong to the expiring
                            // instrument. Without this, if ContractExpiry ever produces
                            // PositionExit outputs for other instruments (e.g. future
                            // cross-instrument settlement logic), we would call
                            // shift_remove on the wrong instrument's position map.
                            if exit.instrument == key {
                                instrument_state
                                    .position
                                    .positions
                                    .shift_remove(&exit.position_id);
                            }
                        }
                    }
                    // Eagerly clear orders in replica: cancel acks for expiry-cancelled
                    // orders arrive async in the live engine (and are processed benignly),
                    // but the replica doesn't need to process them.
                    instrument_state.orders.clear();
                    instrument_state.exchange_id_to_cid.clear();
                    instrument_state.position_ids.clear();
                    instrument_state.pending_fills.clear();
                    instrument_state.expiration_processed = true;
                } else if instrument_state.position.positions.is_empty() {
                    instrument_state.orders.clear();
                    instrument_state.exchange_id_to_cid.clear();
                    instrument_state.position_ids.clear();
                    instrument_state.pending_fills.clear();
                    instrument_state.expiration_processed = true;
                }
            }
            EngineEvent::CorporateAction {
                id,
                instrument,
                kind,
                policy,
                effective_time: _,
            } => {
                // Hybrid replay (mirrors the live `process_corporate_action`):
                //  - the split rescale is deterministic ⇒ event-replay it for every position
                //    (commit the shared `prepare_corporate_action_split` plan via `commit_split`)
                //    to reproduce the quantity / basis / unrealised-PnL adjustment exactly.
                //  - the Floor-to-zero close is OUTPUT-MIRRORED (like the `ContractExpiry` arm),
                //    because the live handler stamps the closing `PositionExit.time_exit` with
                //    `self.time()`, which is wall-clock-derived on `HistoricalClock` and cannot be
                //    reproduced from the payload. Folding the *live* exit keeps tear-sheet parity
                //    exact. Keep this branch structurally symmetric with the live handler — the
                //    replica-parity test fails if they drift. Warnings are intentionally omitted
                //    (the live engine already logged them for this event).

                // Guards — each skips WITHOUT mutating or recording `id`, exactly as the live
                // handler does, so the replica never applies an action the live engine rejected.
                {
                    let instrument_state = self
                        .replica_engine_state_mut()
                        .instruments
                        .instrument_index_mut(&instrument);
                    // Idempotency: already applied (the set mirrors the live engine's).
                    if instrument_state.corporate_actions_processed.contains(&id) {
                        return;
                    }
                    // Unsupported instrument kind (checked first, like the live handler): equity
                    // splits only apply to the deliverable equity. `id` not recorded ⇒ retryable.
                    // Shares `is_split_eligible` with the live handler so the two cannot drift.
                    if !instrument_state.instrument.kind.is_split_eligible() {
                        return;
                    }
                }
                // Unsupported action kind — the compiler-mandated arm for the `#[non_exhaustive]`
                // `CorporateActionKind` (runtime-unreachable in this phase). `id` not recorded.
                let CorporateActionKind::StockSplit { ratio } = kind else {
                    return;
                };
                // The typed `SplitRatio` (`ratio`) threads into `prepare_corporate_action_split`
                // and each `commit_split`; the split arithmetic (including the option strike
                // division) now lives in the shared prepare pass, so — like the live handler — the
                // replica no longer extracts the inner `Decimal` here.

                // Classify for option handling ONCE, up front (mirrors the live handler's Step 2c),
                // via the shared `classify_option_split` so the replica and the live handler collapse
                // `SplitAdjustmentKind` into the same Standard→adjust-in-place decision by
                // construction. Only a Standard split mutates option state here; NonStandard, any
                // future `#[non_exhaustive]` variant, and the unreachable non-split (all `Err(_)` ⇒
                // `false`) deliberately skip — the live handler emits only a signal output for them,
                // which the replica (mirroring state, not outputs) has nothing to apply. No `warn!`
                // (the live engine already logged it). Hoisted above the mutation so pre-validation
                // and application share one classification and cannot drift.
                let adjust_options_in_place = classify_option_split(&kind).unwrap_or(false);

                // Pre-compute the whole action BEFORE mutating anything, mirroring the live
                // handler's Step 2c: if the live engine rejected it (an ambiguous split target, a
                // Decimal overflow — equity, option strike, or option position — or a corrupted
                // option contract count) it recorded no `id` and mutated nothing, so the replica
                // must do the same or state parity drifts. Single-sourced via the same
                // `prepare_corporate_action_split` the live handler calls, so both reach the
                // identical accept/reject decision AND the identical committed values — including
                // which options are excluded as already-adjusted by this `id`. No output emitted —
                // the replica mirrors state, and the live engine already logged the rejection.
                let split_plan = match self
                    .replica_engine_state()
                    .instruments
                    .prepare_corporate_action_split(
                        &id,
                        &instrument,
                        ratio,
                        policy,
                        adjust_options_in_place,
                    ) {
                    Ok(plan) => plan,
                    Err(_) => return,
                };

                // Last price for the eager `pnl_unrealised` recompute — same source the live
                // handler reads. Identical here because the replica replayed the same market data.
                let last_price = self
                    .replica_engine_state()
                    .instruments
                    .instrument_index(&instrument)
                    .data
                    .price();

                // Commit the split to ALL positions (N in Hedging), driving from the plan's
                // pre-computed rescales (mirrors the live handler). The plan already carries owned
                // ids, so no separate collection is needed to avoid the per-position re-borrow.
                for (pos_id, prepared) in split_plan.equity_positions {
                    let instrument_state = self
                        .replica_engine_state_mut()
                        .instruments
                        .instrument_index_mut(&instrument);
                    // Fail loudly on a missing id (see `split_plan_position_mut`): the plan collected
                    // `pos_id` from this same replayed map, so a miss is plan/state corruption that
                    // must crash rather than let replica state drift from the live engine.
                    let position = split_plan_position_mut(
                        &mut instrument_state.position.positions,
                        &pos_id,
                        SplitCommitContext {
                            leg: "replica equity",
                            instrument: &instrument,
                            id: &id,
                            ratio,
                        },
                    );
                    // Infallible: the shared prepare pass already computed (and overflow-checked)
                    // this position's rescale into `prepared`. The replica discards the returned
                    // `SplitResult` — it mirrors state, not outputs.
                    position.commit_split(prepared, last_price);
                }

                // Mirror Floor-to-zero closes from the live `PositionExit` outputs: remove the slot
                // and fold the live exit into the tear sheet (carrying the live `time_exit`).
                for output in outputs.iter() {
                    if let EngineOutput::PositionExit(exit) = output
                        && exit.instrument == instrument
                    {
                        let instrument_state = self
                            .replica_engine_state_mut()
                            .instruments
                            .instrument_index_mut(&instrument);
                        instrument_state
                            .position
                            .positions
                            .shift_remove(&exit.position_id);
                        instrument_state.tear_sheet.update_from_position(exit);
                        // Mirror the live handler's Hedging fill-routing prune on floor-to-zero
                        // (engine/mod.rs): drop the now-dangling `cid → removed_pos_id` mapping by
                        // VALUE. Without this, the replica retains a stale routing entry the live
                        // engine has already pruned, so the full-state `assert_eq!` over
                        // `InstrumentState` (which includes `position_ids`) diverges for any
                        // `OmsMode::Hedging` position floored to zero by a reverse split.
                        instrument_state
                            .position_ids
                            .retain(|_, mapped| *mapped != exit.position_id);
                    }
                }

                // Standard option adjustment (mirrors the live handler's option-handling step).
                // For a STANDARD (whole-number forward) split, the engine adjusts each option on
                // this underlying: the pre-computed post-split strike is assigned and each held
                // position is committed via `commit_split`. This is fully deterministic (the shared
                // prepare pass already overflow-checked the strike; commit reads the option's own
                // replayed price), so it is PURE event-replay — options never floor-to-zero on a
                // forward split, so no output-mirror is needed (unlike the equity Floor-to-zero
                // close above). NON-standard splits leave options untouched (the live handler only
                // emits a signal and mutates no option state), so this whole block is skipped.
                // Branches on `adjust_options_in_place` computed up front (see the hoist above).
                if adjust_options_in_place {
                    // Drive from the pre-computed `SplitPlan` (mirrors the live handler): one
                    // `OptionSplitPlan` per registered option on the underlying (held OR unheld),
                    // carrying the CHECKED post-split strike and a `PreparedSplit` per held position.
                    // No re-scan and no in-place `strike /=` — the shared prepare pass already
                    // overflow-checked the strike division, so the replica commits it infallibly.
                    // Unheld options get the strike fix only; held options additionally commit each
                    // per-position rescale.
                    for opt_plan in split_plan.options {
                        let opt_key = opt_plan.key;
                        let option_state = self
                            .replica_engine_state_mut()
                            .instruments
                            .instrument_index_mut(&opt_key);
                        // Mirror the live handler's loud unreachable: the plan carried only Option
                        // instruments, so a non-Option here is corruption — fail observably rather
                        // than commit a position rescale without adjusting the strike.
                        let InstrumentKind::Option(ref mut contract) = option_state.instrument.kind
                        else {
                            unreachable!(
                                "replica: SplitPlan carried a non-Option instrument {opt_key:?} \
                                 (id={id}, ratio={ratio})"
                            );
                        };
                        contract.strike = opt_plan.strike_post_split;

                        // Record `id` on the OPTION's own set (mirrors the live handler): the
                        // replica's `InstrumentState` is asserted field-for-field against the live
                        // engine's, and this set now gates whether a later delivery of the same
                        // action re-adjusts the option — so omitting it would both fail parity and
                        // let the replica double-divide a strike the live engine skipped. Written
                        // before the unheld early-out, exactly as the live handler does.
                        option_state.corporate_actions_processed.insert(id.clone());

                        // Unheld option: strike correction is the whole job (silent registry fix),
                        // mirroring the live handler.
                        if opt_plan.positions.is_empty() {
                            continue;
                        }
                        let option_last_price = option_state.data.price();
                        for (pos_id, prepared) in opt_plan.positions {
                            // Fail loudly on a missing id (see `split_plan_position_mut`), mirroring
                            // the live handler.
                            let position = split_plan_position_mut(
                                &mut option_state.position.positions,
                                &pos_id,
                                SplitCommitContext {
                                    leg: "replica option",
                                    instrument: &opt_key,
                                    id: &id,
                                    ratio,
                                },
                            );
                            // Infallible: the shared prepare pass pre-computed this option leg's
                            // rescale into `prepared` (with `Fractional`, as the live handler does —
                            // the equity's whole-share-lot policy does not govern option legs). The
                            // replica discards the returned `SplitResult` — it mirrors state.
                            position.commit_split(prepared, option_last_price);
                        }
                    }
                }

                // Record `id` — the action has now been applied (mirrors the live handler).
                self.replica_engine_state_mut()
                    .instruments
                    .instrument_index_mut(&instrument)
                    .corporate_actions_processed
                    .insert(id);
            }
        }
    }

    /// Returns a reference to the `EngineState` replica.
    pub fn replica_engine_state(&self) -> &EngineState<GlobalData, InstrumentData> {
        &self.state_replica.event
    }

    fn replica_engine_state_mut(&mut self) -> &mut EngineState<GlobalData, InstrumentData> {
        &mut self.state_replica.event
    }
}
