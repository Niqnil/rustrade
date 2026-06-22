use crate::{
    EngineEvent,
    engine::{
        EngineMeta, EngineOutput, Processor,
        audit::{AuditTick, EngineAudit, ProcessAudit, context::EngineContext},
        state::{EngineState, instrument::data::InstrumentDataState, position::PositionId},
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
    ///   unsupported-kind, all of which skip without mutating). The Floor-to-zero **close** is
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
            EngineEvent::Account(event) => match event {
                AccountStreamEvent::Reconnecting(exchange) => {
                    self.replica_engine_state_mut()
                        .connectivity
                        .update_from_account_reconnecting(&exchange);
                }
                AccountStreamEvent::Item(event) => {
                    self.replica_engine_state_mut().update_from_account(&event);
                }
            },
            EngineEvent::Market(event) => match event {
                MarketStreamEvent::Reconnecting(exchange) => {
                    self.replica_engine_state_mut()
                        .connectivity
                        .update_from_market_reconnecting(&exchange);
                }
                MarketStreamEvent::Item(event) => {
                    self.replica_engine_state_mut().update_from_market(&event);
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
                //  - `apply_split` is deterministic ⇒ event-replay it for every position to
                //    reproduce the quantity / basis / unrealised-PnL adjustment exactly.
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
                    // splits only apply to `Spot`. `id` not recorded ⇒ retryable.
                    if !matches!(instrument_state.instrument.kind, InstrumentKind::Spot) {
                        return;
                    }
                }
                // Unsupported action kind — the compiler-mandated arm for the `#[non_exhaustive]`
                // `CorporateActionKind` (runtime-unreachable in this phase). `id` not recorded.
                let CorporateActionKind::StockSplit { ratio } = kind else {
                    return;
                };

                // Last price for the eager `pnl_unrealised` recompute — same source the live
                // handler reads. Identical here because the replica replayed the same market data.
                let last_price = self
                    .replica_engine_state()
                    .instruments
                    .instrument_index(&instrument)
                    .data
                    .price();

                // Event-replay `apply_split` on ALL positions (N in Hedging). Collect ids first to
                // avoid a borrow conflict with the per-position re-borrow, like the live handler.
                let position_ids: Vec<PositionId> = self
                    .replica_engine_state()
                    .instruments
                    .instrument_index(&instrument)
                    .position
                    .positions
                    .keys()
                    .cloned()
                    .collect();
                for pos_id in position_ids {
                    let instrument_state = self
                        .replica_engine_state_mut()
                        .instruments
                        .instrument_index_mut(&instrument);
                    if let Some(position) = instrument_state.position.positions.get_mut(&pos_id) {
                        position.apply_split(ratio, policy, last_price);
                    }
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
