use crate::{
    EngineEvent, Sequence,
    engine::{
        action::{
            ActionOutput,
            cancel_orders::CancelOrders,
            close_positions::ClosePositions,
            generate_algo_orders::{GenerateAlgoOrders, GenerateAlgoOrdersOutput},
            send_requests::SendRequests,
        },
        audit::{AuditTick, Auditor, EngineAudit, ProcessAudit, context::EngineContext},
        clock::EngineClock,
        command::Command,
        execution_tx::ExecutionTxMap,
        state::{
            EngineState,
            instrument::data::InstrumentDataState,
            order::{in_flight_recorder::InFlightRequestRecorder, manager::OrderManager},
            position::{PositionExited, PositionId, SplitRoundingPolicy},
            trading::TradingState,
        },
    },
    execution::{AccountStreamEvent, request::ExecutionRequest},
    risk::RiskManager,
    shutdown::SyncShutdown,
    statistic::summary::TradingSummaryGenerator,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_data::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use rustrade_execution::{
    AccountEvent,
    order::{Order, id::ClientOrderId},
    trade::{AssetFees, Trade, TradeId},
};
use rustrade_instrument::{
    Side,
    asset::AssetIndex,
    corporate_action::{CorporateActionKind, SplitAdjustmentKind, SplitRatio},
    exchange::ExchangeIndex,
    instrument::{
        InstrumentIndex,
        kind::{InstrumentKind, option::OptionKind},
    },
};
use rustrade_integration::channel::Tx;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt::Debug;
use tracing::{info, warn};

/// Defines how the [`Engine`] actions a [`Command`], and the associated outputs.
pub mod action;

/// Defines an `Engine` audit types as well as utilities for handling the `Engine` `AuditStream`.
///
/// eg/ `StateReplicaManager` component can be used to maintain an `EngineState` replica.
pub mod audit;

/// Defines the [`EngineClock`] interface used to determine the current `Engine` time.
///
/// This flexibility enables back-testing runs to use approximately correct historical timestamps.
pub mod clock;

/// Defines an [`Engine`] [`Command`] - used to give trading directives to the `Engine` from an
/// external process (eg/ ClosePositions).
pub mod command;

/// Defines all possible errors that can occur in the [`Engine`].
pub mod error;

/// Defines the [`ExecutionTxMap`] interface that models a collection of transmitters used to route
/// can `ExecutionRequest` to the appropriate `ExecutionManagers`.
pub mod execution_tx;

/// Defines all state used by the`Engine` to algorithmically trade.
///
/// eg/ `ConnectivityStates`, `AssetStates`, `InstrumentStates`, `Position`, etc.
pub mod state;

/// `Engine` runners for processing input `Events`.
///
/// eg/ `fn sync_run`, `fn sync_run_with_audit`, `fn async_run`, `fn async_run_with_audit`,
pub mod run;

/// Defines how a component processing an input Event and generates an appropriate Audit.
pub trait Processor<Event> {
    type Audit;
    fn process(&mut self, event: Event) -> Self::Audit;
}

/// Process and `Event` with the `Engine` and product an [`AuditTick`] of work done.
pub fn process_with_audit<Event, Engine>(
    engine: &mut Engine,
    event: Event,
) -> AuditTick<Engine::Audit, EngineContext>
where
    Engine: Processor<Event> + Auditor<Engine::Audit, Context = EngineContext>,
{
    let output = engine.process(event);
    engine.audit(output)
}

/// Algorithmic trading `Engine`.
///
/// The `Engine`:
/// * Processes input [`EngineEvent`] (or custom events if implemented).
/// * Maintains the internal [`EngineState`] (instrument data state, open orders, positions, etc.).
/// * Generates algo orders (if `TradingState::Enabled`).
///
/// # Type Parameters
/// * `Clock` - [`EngineClock`] implementation.
/// * `State` - Engine `State` implementation (eg/ [`EngineState`]).
/// * `ExecutionTxs` - [`ExecutionTxMap`] implementation for sending execution requests.
/// * `Strategy` - Trading Strategy implementation (see [`super::strategy`]).
/// * `Risk` - [`RiskManager`] implementation.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Engine<Clock, State, ExecutionTxs, Strategy, Risk> {
    pub clock: Clock,
    pub meta: EngineMeta,
    pub state: State,
    pub execution_txs: ExecutionTxs,
    pub strategy: Strategy,
    pub risk: Risk,
}

/// Running [`Engine`] metadata.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct EngineMeta {
    /// [`EngineClock`] start timestamp of the current [`Engine`] `run`.
    pub time_start: DateTime<Utc>,
    /// Monotonically increasing [`Sequence`] associated with the number of events processed.
    pub sequence: Sequence,
}

impl<Clock, GlobalData, InstrumentData, ExecutionTxs, Strategy, Risk>
    Processor<EngineEvent<InstrumentData::MarketEventKind>>
    for Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>
where
    Clock: EngineClock + for<'a> Processor<&'a EngineEvent<InstrumentData::MarketEventKind>>,
    InstrumentData: InstrumentDataState,
    GlobalData: for<'a> Processor<&'a AccountEvent>
        + for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>,
    ExecutionTxs: ExecutionTxMap<ExchangeIndex, InstrumentIndex>,
    Strategy: OnTradingDisabled<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Risk>
        + OnDisconnectStrategy<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Risk>
        + AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>>,
{
    type Audit = EngineAudit<
        EngineEvent<InstrumentData::MarketEventKind>,
        EngineOutput<Strategy::OnTradingDisabled, Strategy::OnDisconnect>,
    >;

    fn process(&mut self, event: EngineEvent<InstrumentData::MarketEventKind>) -> Self::Audit {
        self.clock.process(&event);

        let process_audit = match &event {
            EngineEvent::Shutdown(_) => return EngineAudit::process(event),
            EngineEvent::Command(command) => {
                let output = self.action(command);

                if let Some(unrecoverable) = output.unrecoverable_errors() {
                    return EngineAudit::process_with_output_and_errs(event, unrecoverable, output);
                } else {
                    ProcessAudit::with_output(event, output)
                }
            }
            EngineEvent::TradingStateUpdate(trading_state) => {
                let trading_disabled = self.update_from_trading_state_update(*trading_state);
                ProcessAudit::with_trading_state_update(event, trading_disabled)
            }
            EngineEvent::Account(account) => {
                let output = self.update_from_account_stream(account);
                ProcessAudit::with_account_update(event, output)
            }
            EngineEvent::Market(market) => {
                let output = self.update_from_market_stream(market);
                ProcessAudit::with_market_update(event, output)
            }
            EngineEvent::ContractExpiry(key) => {
                let exited = self.process_contract_expiry(key);
                // Fold all closed positions into the audit as separate PositionExit outputs.
                // In Netting mode this is 0 or 1 entries; in Hedging mode it may be N.
                let mut audit = ProcessAudit::with_event(event);
                for position_exited in exited {
                    audit = audit.add_output(position_exited);
                }
                // ContractExpiry settles regardless of TradingState and does not
                // trigger algo order generation — return early before that check.
                return EngineAudit::from(audit);
            }
            EngineEvent::CorporateAction {
                id,
                instrument,
                kind,
                policy,
                effective_time: _,
            } => {
                let outputs = self.process_corporate_action(id, instrument, kind, *policy);
                // Fold each observable output (SplitRemainder / OpenOrdersAtSplit /
                // OptionPositionAdjustedForSplit / OptionPositionsRequireIdentityChange /
                // PositionExit / UnsupportedCorporateAction) into the audit. Like ContractExpiry, a
                // corporate action is engine-driven and
                // settles regardless of TradingState — return early before algo generation.
                let mut audit = ProcessAudit::with_event(event);
                for output in outputs {
                    audit = audit.add_output(output);
                }
                return EngineAudit::from(audit);
            }
        };

        if let TradingState::Enabled = self.state.trading {
            let output = self.generate_algo_orders();

            if output.is_empty() {
                EngineAudit::from(process_audit)
            } else if let Some(unrecoverable) = output.unrecoverable_errors() {
                EngineAudit::Process(process_audit.add_errors(unrecoverable))
            } else {
                EngineAudit::from(process_audit.add_output(output))
            }
        } else {
            EngineAudit::from(process_audit)
        }
    }
}

impl<Clock, GlobalData, InstrumentData, ExecutionTxs, Strategy, Risk> SyncShutdown
    for Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>
where
    ExecutionTxs: ExecutionTxMap,
{
    type Result = ();

    fn shutdown(&mut self) -> Self::Result {
        self.execution_txs.iter().for_each(|execution_tx| {
            let _send_result = execution_tx.send(ExecutionRequest::Shutdown);
        });
    }
}

impl<Clock, GlobalData, InstrumentData, ExecutionTxs, Strategy, Risk>
    Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>
{
    /// Action an `Engine` [`Command`], producing an [`ActionOutput`] of work done.
    pub fn action(&mut self, command: &Command) -> ActionOutput
    where
        InstrumentData: InFlightRequestRecorder,
        ExecutionTxs: ExecutionTxMap,
        Strategy: ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>,
        Risk: RiskManager,
    {
        match &command {
            Command::SendCancelRequests(requests) => {
                info!(
                    ?requests,
                    "Engine actioning user Command::SendCancelRequests"
                );
                let output = self.send_requests(requests.clone());
                self.state.record_in_flight_cancels(&output.sent);
                ActionOutput::CancelOrders(output)
            }
            Command::SendOpenRequests(requests) => {
                info!(?requests, "Engine actioning user Command::SendOpenRequests");
                let output = self.send_requests(requests.clone());
                self.state.record_in_flight_opens(&output.sent);
                ActionOutput::OpenOrders(output)
            }
            Command::ClosePositions(filter) => {
                info!(?filter, "Engine actioning user Command::ClosePositions");
                ActionOutput::ClosePositions(self.close_positions(filter))
            }
            Command::CancelOrders(filter) => {
                info!(?filter, "Engine actioning user Command::CancelOrders");
                ActionOutput::CancelOrders(self.cancel_orders(filter))
            }
        }
    }

    /// Update the `Engine` [`TradingState`].
    ///
    /// If the `TradingState` transitions to `TradingState::Disabled`, the `Engine` will call
    /// the configured [`OnTradingDisabled`] strategy logic.
    pub fn update_from_trading_state_update(
        &mut self,
        update: TradingState,
    ) -> Option<Strategy::OnTradingDisabled>
    where
        Strategy:
            OnTradingDisabled<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Risk>,
    {
        self.state
            .trading
            .update(update)
            .transitioned_to_disabled()
            .then(|| Strategy::on_trading_disabled(self))
    }

    /// Update the [`Engine`] from an [`AccountStreamEvent`].
    ///
    /// If the input `AccountStreamEvent` indicates the exchange execution link has disconnected,
    /// the `Engine` will call the configured [`OnDisconnectStrategy`] strategy logic.
    pub fn update_from_account_stream(
        &mut self,
        event: &AccountStreamEvent,
    ) -> UpdateFromAccountOutput<Strategy::OnDisconnect>
    where
        InstrumentData: for<'a> Processor<&'a AccountEvent>,
        GlobalData: for<'a> Processor<&'a AccountEvent>,
        Strategy: OnDisconnectStrategy<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Risk>,
    {
        match event {
            AccountStreamEvent::Reconnecting(exchange) => {
                self.state
                    .connectivity
                    .update_from_account_reconnecting(exchange);

                UpdateFromAccountOutput::OnDisconnect(Strategy::on_disconnect(self, *exchange))
            }
            AccountStreamEvent::Item(event) => self
                .state
                .update_from_account(event)
                .map(UpdateFromAccountOutput::PositionExit)
                .unwrap_or(UpdateFromAccountOutput::None),
        }
    }

    /// Update the [`Engine`] from a [`MarketStreamEvent`].
    ///
    /// If the input `MarketStreamEvent` indicates the exchange market data link has disconnected,
    /// the `Engine` will call the configured [`OnDisconnectStrategy`] strategy logic.
    pub fn update_from_market_stream(
        &mut self,
        event: &MarketStreamEvent<InstrumentIndex, InstrumentData::MarketEventKind>,
    ) -> UpdateFromMarketOutput<Strategy::OnDisconnect>
    where
        InstrumentData: InstrumentDataState,
        GlobalData:
            for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>,
        Strategy: OnDisconnectStrategy<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Risk>,
    {
        match event {
            MarketStreamEvent::Reconnecting(exchange) => {
                self.state
                    .connectivity
                    .update_from_market_reconnecting(exchange);

                UpdateFromMarketOutput::OnDisconnect(Strategy::on_disconnect(self, *exchange))
            }
            MarketStreamEvent::Item(event) => {
                self.state.update_from_market(event);
                UpdateFromMarketOutput::None
            }
        }
    }

    /// Returns a [`TradingSummaryGenerator`] for the current trading session.
    pub fn trading_summary_generator(&self, risk_free_return: Decimal) -> TradingSummaryGenerator
    where
        Clock: EngineClock,
    {
        TradingSummaryGenerator::init(
            risk_free_return,
            self.meta.time_start,
            self.time(),
            &self.state.instruments,
            &self.state.assets,
        )
    }

    /// Processes a `ContractExpiry` event for the given `InstrumentIndex`.
    ///
    /// # Algorithm
    /// 1. Guards on `expiration_processed` (idempotent).
    /// 2. Cancels all open orders for the instrument by sending `ExecutionRequest::Cancel` for each.
    /// 3. Derives settlement price from instrument data and contract specification:
    ///    - OTM: settlement price = 0
    ///    - ITM call: settlement = spot - strike (per-contract intrinsic value)
    ///    - ITM put: settlement = strike - spot (per-contract intrinsic value)
    /// 4. Synthesises a closing `Trade` at the settlement price and routes it through
    ///    `instrument_state.update_from_trade`.
    /// 5. Sets `instrument_state.expiration_processed = true`.
    ///
    /// If no position is open for the instrument, steps 3–4 are skipped.
    /// If no market price is available, settlement cannot be computed and the method
    /// logs a warning and returns without synthesising a fill. The `expiration_processed`
    /// flag is **not** set in this case, making the event **retryable** — re-inject
    /// `ContractExpiry` once the underlying spot instrument has received market data.
    ///
    /// # Not modelled (deferred)
    ///
    /// - **Assignment for short writers:** short positions at expiry are closed at intrinsic
    ///   value identical to long positions (OTM at 0, ITM at `|spot − strike|`). True
    ///   assignment — where the short writer is obligated to deliver the underlying — is not
    ///   modelled. A future enhancement would detect net-short positions and open a synthetic
    ///   underlying position instead of (or in addition to) closing the option position.
    ///
    /// - **Physical settlement:** all settlements are cash-equivalent (a synthetic fill at the
    ///   settlement price adjusts PnL). No separate "deliver/receive underlying" position is
    ///   opened. Physically-settled contracts (e.g. some futures-style options) are out of scope
    ///   until this is revisited.
    pub fn process_contract_expiry(
        &mut self,
        key: &InstrumentIndex,
    ) -> Vec<PositionExited<AssetIndex, InstrumentIndex>>
    where
        Clock: EngineClock,
        InstrumentData: InstrumentDataState + InFlightRequestRecorder,
        ExecutionTxs: ExecutionTxMap,
    {
        let instrument_state = self.state.instruments.instrument_index_mut(key);

        // Guard: idempotent — ignore duplicates after first processing.
        if instrument_state.expiration_processed {
            return vec![];
        }

        // Step 2: Cancel all open orders for this instrument.
        let cancel_requests: Vec<_> = instrument_state
            .orders
            .orders()
            .filter_map(Order::to_request_cancel)
            .collect();
        let cancels = self.send_requests(cancel_requests);
        self.state.record_in_flight_cancels(&cancels.sent);

        // Re-borrow after send_requests (which takes &self for execution_txs).
        let instrument_state = self.state.instruments.instrument_index_mut(key);

        // Step 3–4: Synthesise settlement fills only if positions are open.
        if instrument_state.position.positions.is_empty() {
            instrument_state.expiration_processed = true;
            instrument_state.orders.clear();
            instrument_state.exchange_id_to_cid.clear();
            instrument_state.position_ids.clear();
            // Clear pending_fills even when no positions exist — a fill-before-ack race
            // that was in progress at expiry should not accumulate orphaned fills.
            instrument_state.pending_fills.clear();
            return vec![];
        }

        // Derive settlement price from the underlying spot price and contract spec.
        // For options, ITM/OTM determination requires the *underlying's* price, not the
        // option's own market price (which includes premium and would give wrong results).
        // We find the underlying spot instrument by matching the option's underlying.base.
        // Capture both the underlying base key and the exchange so we can filter
        // the spot scan to the same exchange. Without the exchange filter, a
        // multi-exchange setup (e.g. BTC/USD on both Binance and Alpaca) would
        // silently use the wrong exchange's price.
        let option_spec = match &instrument_state.instrument.kind {
            InstrumentKind::Option(_) => Some((
                instrument_state.instrument.underlying.base,
                instrument_state.instrument.underlying.quote,
                instrument_state.instrument.exchange,
            )),
            _ => None,
        };

        let spot_price = match option_spec {
            Some((base_key, quote_key, exchange)) => {
                // Find the spot instrument on the same exchange whose underlying matches
                // the option's underlying base AND quote. Both are required: without the
                // quote filter, BTC/USDT and BTC/USDC options on the same exchange would
                // silently share the same spot price (M3).
                // Single-pass: collect all matching spot instruments so we can both
                // warn on ambiguity (visible in production) and use the first match,
                // without scanning the instrument list twice.
                let spot_matches: Vec<_> = self
                    .state
                    .instruments
                    .0
                    .values()
                    .filter(|s| {
                        matches!(&s.instrument.kind, InstrumentKind::Spot)
                            && s.instrument.underlying.base == base_key
                            && s.instrument.underlying.quote == quote_key
                            && s.instrument.exchange == exchange
                    })
                    .collect();
                if spot_matches.len() > 1 {
                    warn!(
                        count = spot_matches.len(),
                        "process_contract_expiry: multiple Spot instruments match the option \
                         underlying — using the first. Deduplicate your instrument config."
                    );
                }
                spot_matches.into_iter().next().and_then(|s| s.data.price())
            }
            // Non-option instruments: use the instrument's own last price.
            None => self.state.instruments.instrument_index(key).data.price(),
        };

        // Re-borrow mutably after the immutable scan above.
        let instrument_state = self.state.instruments.instrument_index_mut(key);

        let Some(spot_price) = spot_price else {
            warn!(
                instrument = ?key,
                "ContractExpiry: underlying price unavailable — cannot compute settlement. \
                 Ensure the underlying spot instrument is subscribed. \
                 Re-inject ContractExpiry once market data arrives."
            );
            // Do NOT set expiration_processed — the event is retryable once data is available.
            return vec![];
        };

        let settlement_price = match &instrument_state.instrument.kind {
            InstrumentKind::Option(contract) => {
                match contract.kind {
                    OptionKind::Call => {
                        // ITM call: intrinsic = underlying_spot - strike (per-share)
                        // ATM (spot == strike): intrinsic = 0 by cash-settlement convention.
                        if spot_price > contract.strike {
                            spot_price - contract.strike
                        } else {
                            Decimal::ZERO
                        }
                    }
                    OptionKind::Put => {
                        // ITM put: intrinsic = strike - underlying_spot (per-share)
                        // ATM (spot == strike): intrinsic = 0 by cash-settlement convention.
                        if contract.strike > spot_price {
                            contract.strike - spot_price
                        } else {
                            Decimal::ZERO
                        }
                    }
                }
            }
            // Non-option instruments: settlement at current market price.
            _ => spot_price,
        };

        // Collect all position IDs before iterating so we can re-borrow instrument_state
        // mutably inside the loop without conflicting with the keys() borrow.
        let position_ids: Vec<PositionId> = instrument_state
            .position
            .positions
            .keys()
            .cloned()
            .collect();

        // Engine clock time for all synthetic trades in this expiry batch.
        // Using self.time() (not Utc::now()) keeps backtests deterministic.
        let engine_time = self.time();

        let mut exited = Vec::with_capacity(position_ids.len());

        for pos_id in position_ids {
            let instrument_state = self.state.instruments.instrument_index_mut(key);

            let Some(open_position) = instrument_state.position.positions.get(&pos_id) else {
                continue;
            };

            let closing_side = match open_position.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let closing_quantity = open_position.quantity_abs;

            // Each synthetic trade gets a unique ID derived from its position ID and
            // the engine clock timestamp. The timestamp component prevents dedup key
            // collisions across engine restarts where expiration_processed was not
            // persisted (Netting mode always uses the same pos_id = "netting").
            // Always heap-allocates (>22 chars): use String directly rather than SmolStr.
            let trade_tag = format!(
                "expiry-settlement-{}-{}",
                pos_id,
                engine_time.timestamp_micros()
            );
            // Use the instrument's quote asset for fee tracking (amount is zero)
            let quote_asset = instrument_state.instrument.underlying.quote;
            let settlement_trade = Trade {
                id: TradeId::new(&trade_tag),
                order_id: rustrade_execution::order::id::OrderId::new(&trade_tag),
                instrument: *key,
                strategy: rustrade_execution::order::id::StrategyId::ENGINE_EXPIRY,
                time_exchange: engine_time,
                side: closing_side,
                price: settlement_price,
                quantity: closing_quantity,
                fees: AssetFees {
                    asset: quote_asset,
                    fees: Decimal::ZERO,
                    fees_quote: Some(Decimal::ZERO),
                },
            };

            // Route settlement directly to the correct position by ID.
            // We bypass InstrumentState::update_from_trade (which calls update_from_trade
            // without a PositionId) because in Hedging mode that would derive the ID from
            // trade.order_id, opening a spurious new position instead of closing the real one.
            //
            // Fee model bypass: settlement_trade.fees is always Decimal::ZERO (set above).
            // The fee model is intentionally not applied — exchange settlement commission,
            // if any, must be accounted for separately by the caller. Callers that configure
            // FeeModelConfig::PerContract for options should note this invariant.
            debug_assert_eq!(
                settlement_trade.fees.fees,
                Decimal::ZERO,
                "settlement trade must carry zero fees before update_from_trade_with_id"
            );
            let contract_size = instrument_state.instrument.kind.contract_size();
            if let Some(exit) = instrument_state.position.update_from_trade_with_id(
                &settlement_trade,
                &pos_id,
                contract_size,
            ) {
                instrument_state.tear_sheet.update_from_position(&exit);
                exited.push(exit);
            }
        }

        // Step 5: Mark as processed and clear all routing tables.
        // No fills will arrive for this instrument post-expiry. Cancel-ack messages
        // for the orders cancelled in step 2 may never arrive (exchanges silently void
        // them), so cleanup_routing_tables() cannot remove the CancelInFlight entries —
        // they would accumulate indefinitely across expiry cycles in Hedging mode.
        // Clear orders, position_ids, exchange_id_to_cid, and pending_fills explicitly,
        // matching the replica's eager-clear logic in StateReplicaManager::update_from_event.
        let instrument_state = self.state.instruments.instrument_index_mut(key);
        instrument_state.expiration_processed = true;
        instrument_state.orders.clear();
        instrument_state.exchange_id_to_cid.clear();
        instrument_state.position_ids.clear();
        // Clear any fills buffered in a fill-before-ack race that was in progress at
        // expiry. Without this, orphaned pending_fills accumulate across expiry cycles
        // in a long-running engine with persisted state.
        instrument_state.pending_fills.clear();

        exited
    }

    /// Processes a `CorporateAction` (stock split / reverse split) for the given instrument,
    /// adjusting **every** open position and returning the observable outputs to fold into the
    /// audit.
    ///
    /// Unlike `process_contract_expiry`, this **does not** cancel resting orders (a real broker
    /// price-adjusts them, so an engine-side cancel would diverge) and **does not** require a
    /// market price (the split is applied regardless; see below).
    ///
    /// All outputs (and any folded `PositionExit`) are stamped at the engine clock's current time.
    /// The caller is expected to have advanced the clock to the event's `effective_time` before
    /// dispatch; the engine's event loop does this automatically via the clock's time-exchange
    /// handling, so a direct caller (e.g. a test) must advance the clock first.
    ///
    /// Note this is clock-kind dependent: under `HistoricalClock` (backtest) the clock is advanced
    /// to `effective_time`, so outputs carry it; under `LiveClock` the clock is **not** advanceable
    /// and outputs are stamped at `Utc::now()`, not `effective_time` (correct for live — the
    /// adjustment happens now — but worth noting if you compare live and replayed stamps).
    ///
    /// # Algorithm
    /// 1. Idempotency guard on `id` (per-instrument `corporate_actions_processed` set). A
    ///    duplicate `id` is skipped with a warning. This holds within a live session but does **not**
    ///    survive a snapshot taken before the set existed; see the `corporate_actions_processed`
    ///    field on [`InstrumentState`](crate::engine::state::instrument::InstrumentState) for the
    ///    migration caveat.
    /// 2. **Unsupported guards (no silent no-op, `id` not recorded ⇒ retryable).** The
    ///    instrument-kind check runs **first** so a split on an option is attributed to the
    ///    instrument, not the (supported) kind:
    ///    - target instrument is not `Spot` ⇒ [`UnsupportedCorporateActionReason::InstrumentKindNotSupported`];
    ///    - action kind is not a stock split ⇒ [`UnsupportedCorporateActionReason::ActionKindNotSupported`]
    ///      (the compiler-mandated arm for the `#[non_exhaustive]` [`CorporateActionKind`]).
    /// 3. Snapshot resting orders into [`EngineOutput::OpenOrdersAtSplit`] (no cancellation).
    /// 4. Apply the split to every open position via
    ///    [`Position::apply_split`](crate::engine::state::position::Position::apply_split); emit one
    ///    [`EngineOutput::SplitRemainder`] per position that disposed a fractional sliver. A
    ///    position floored to zero quantity is removed and folded as an
    ///    [`EngineOutput::PositionExit`].
    /// 5. Scan for option positions on the same underlying and handle them per the OCC
    ///    standard/non-standard rule ([`CorporateActionKind::split_kind`]):
    ///    - **standard** (whole-number forward split) ⇒ adjust each option **in place**
    ///      (strike ÷ `ratio`, contracts × `ratio` via
    ///      [`Position::apply_split`](crate::engine::state::position::Position::apply_split), multiplier
    ///      unchanged), emitting one [`EngineOutput::OptionPositionAdjustedForSplit`] per position
    ///      plus — if the option has resting orders — an [`EngineOutput::OpenOrdersAtSplit`] for them
    ///      (a real broker price-adjusts them; the engine cancels nothing). Because option positions
    ///      are held in **whole contracts**, a whole-number ratio applied to them disposes no
    ///      fractional sliver, so — unlike the equity path above, where a fractional remainder can
    ///      arise — **no** [`EngineOutput::SplitRemainder`] is emitted on this path;
    ///    - **non-standard** (every reverse split, every fractional forward split) ⇒ the engine
    ///      **cannot** adjust them (the OCC assigns a new contract identity), so it emits
    ///      [`EngineOutput::OptionPositionsRequireIdentityChange`] and leaves them at pre-split
    ///      terms. The equity split is applied and the `id` recorded **regardless**.
    /// 6. Record `id` in `corporate_actions_processed`.
    ///
    /// # Missing last price
    /// If the instrument's last price is unavailable the split is **still applied** and the `id`
    /// **recorded** (the quantity/basis arithmetic needs no price); `pnl_unrealised` is set to
    /// zero with a warning and corrected on the next market tick. This is **not** retryable —
    /// contrast `process_contract_expiry`, which bails and is retryable.
    ///
    /// # Approximate `pnl_unrealised` immediately after the split
    /// Even *with* a last price, the eagerly recomputed `pnl_unrealised` is approximate until the
    /// next market tick: in live the split arrives before the first post-split print, so a pre-split
    /// price is valued against the post-split basis (overstated for forward splits, understated for
    /// reverse). Do not drive hard risk checks off the immediate post-split snapshot — see
    /// [`Position::apply_split`](crate::engine::state::position::Position::apply_split).
    pub fn process_corporate_action<OnTradingDisabled, OnDisconnect>(
        &mut self,
        id: &SmolStr,
        key: &InstrumentIndex,
        kind: &CorporateActionKind,
        policy: SplitRoundingPolicy,
    ) -> Vec<EngineOutput<OnTradingDisabled, OnDisconnect>>
    where
        Clock: EngineClock,
        InstrumentData: InstrumentDataState,
    {
        let mut outputs = Vec::new();
        // Engine clock time for deterministic stamping (the clock was already advanced to the
        // event's `effective_time` before this handler runs).
        let engine_time = self.time();

        let instrument_state = self.state.instruments.instrument_index_mut(key);

        // Step 1: idempotency guard (keyed on `id` alone). Warn on suppression — a wrapper-reused
        // `id` would otherwise silently drop a real action.
        if instrument_state.corporate_actions_processed.contains(id) {
            warn!(
                %id,
                instrument = ?key,
                effective_time = ?engine_time,
                "CorporateAction id already processed — skipping (idempotent). Ensure each \
                 action (incl. same-day corrections) carries a unique id."
            );
            return outputs;
        }

        // Step 2a: reject non-Spot targets (checked BEFORE the action kind so a split on an
        // option is attributed to the instrument, not the supported kind). Do NOT record `id`.
        if !matches!(instrument_state.instrument.kind, InstrumentKind::Spot) {
            warn!(
                %id,
                instrument = ?key,
                kind = ?instrument_state.instrument.kind,
                "CorporateAction targets a non-Spot instrument — unsupported (equity splits \
                 only). Emitting UnsupportedCorporateAction; id NOT recorded (retryable)."
            );
            outputs.push(EngineOutput::UnsupportedCorporateAction {
                instrument: *key,
                kind: kind.clone(),
                reason: UnsupportedCorporateActionReason::InstrumentKindNotSupported,
            });
            return outputs;
        }

        // Step 2b: extract the split ratio. `CorporateActionKind` is `#[non_exhaustive]` and
        // defined in another crate, so the compiler mandates this `else` arm even though
        // `StockSplit` is currently the only variant. It is runtime-unreachable until a second
        // kind exists, then becomes the "action kind not supported" rejection. Do NOT record `id`.
        let CorporateActionKind::StockSplit { ratio } = kind else {
            warn!(
                %id,
                instrument = ?key,
                kind = ?kind,
                "CorporateAction kind not supported (stock splits only). Emitting \
                 UnsupportedCorporateAction; id NOT recorded (retryable once supported)."
            );
            outputs.push(EngineOutput::UnsupportedCorporateAction {
                instrument: *key,
                kind: kind.clone(),
                reason: UnsupportedCorporateActionReason::ActionKindNotSupported,
            });
            return outputs;
        };
        // `ratio` is a validated `SplitRatio` (always `> 0`, enforced at construction and on
        // deserialization), so the engine's split arithmetic can never receive a degenerate ratio
        // through the event path. Copy the `SplitRatio` out (it is `Copy`) to carry the type-level
        // invariant unbroken into the output records, and extract the inner `Decimal` for the
        // arithmetic below.
        let ratio: SplitRatio = *ratio;
        let ratio_decimal = ratio.get();

        // Step 3: snapshot resting orders as an observable — do NOT cancel (a broker price-adjusts
        // them, so an engine-side cancel would diverge from the broker).
        let resting_orders: Vec<OpenOrderAtSplit> = instrument_state
            .orders
            .orders()
            .map(|order| OpenOrderAtSplit {
                cid: order.key.cid.clone(),
                price_pre_split: order.price,
                quantity_pre_split: order.quantity,
            })
            .collect();

        // Last price for the eager `pnl_unrealised` recompute. None ⇒ 0 (+ warn); the split is
        // still applied and the `id` recorded (NOT retryable).
        let last_price = instrument_state.data.price();
        if last_price.is_none() {
            warn!(
                %id,
                instrument = ?key,
                "CorporateAction: instrument last price unavailable — pnl_unrealised set to 0, \
                 corrected on the next market tick. The split is still applied (not retryable)."
            );
        }

        // Step 4: apply to ALL open positions (N in Hedging mode). Collect ids first to avoid a
        // borrow conflict with the per-position re-borrow, mirroring process_contract_expiry.
        let position_ids: Vec<PositionId> = instrument_state
            .position
            .positions
            .keys()
            .cloned()
            .collect();
        // Pre-size: up to a SplitRemainder + a PositionExit per position, plus the equity
        // OpenOrdersAtSplit and the option-handling observable(s).
        outputs.reserve(position_ids.len() * 2 + 2);

        for pos_id in position_ids {
            let instrument_state = self.state.instruments.instrument_index_mut(key);
            let Some(position) = instrument_state.position.positions.get_mut(&pos_id) else {
                continue;
            };

            let side = position.side;
            let result = position.apply_split(ratio_decimal, policy, last_price);
            // Read the post-split basis AFTER apply_split overwrote it (Convention A): quantity
            // and price then share the post-split era, valuing the disposed sliver with one
            // multiply and no ratio knowledge.
            let price_entry_average_post_split = position.price_entry_average;
            let quantity_abs_after = position.quantity_abs;

            if result.remainder > Decimal::ZERO {
                outputs.push(EngineOutput::SplitRemainder {
                    instrument: *key,
                    position_id: pos_id.clone(),
                    side,
                    quantity_fractional_disposed: result.remainder,
                    price_entry_average_post_split,
                });
            }

            // Floor whole-position disposal: a reverse split under `Floor` can round the quantity
            // to zero. Remove the slot and fold a PositionExit so no zero-qty zombie strands its
            // pnl_realised (which would later leak into a fresh post-split buy via VWAP-from-zero).
            if quantity_abs_after.is_zero() {
                let instrument_state = self.state.instruments.instrument_index_mut(key);
                if let Some(closed) = instrument_state.position.positions.shift_remove(&pos_id) {
                    let mut exit = PositionExited::from(closed);
                    exit.position_id = pos_id.clone();
                    exit.time_exit = engine_time;
                    instrument_state.tear_sheet.update_from_position(&exit);
                    outputs.push(EngineOutput::PositionExit(exit));

                    // Prune the hedging fill-routing map for the removed position. This must prune
                    // by VALUE (the removed `PositionId`), NOT via `cleanup_routing_tables` (which
                    // retains by `ClientOrderId` still present in `self.orders`): the split
                    // deliberately leaves the position's resting order in place (a broker
                    // price-adjusts it), so its CID is still in `orders` and `cleanup_routing_tables`
                    // would keep the now-dangling `cid → removed_pos_id` mapping. Left stale, an
                    // `OmsMode::Hedging` late fill on that order would resolve the dead id and
                    // silently reopen a floored-out position.
                    instrument_state
                        .position_ids
                        .retain(|_, mapped| *mapped != pos_id);
                }
            }
        }

        // Emit the observable resting-orders snapshot if any rest.
        if !resting_orders.is_empty() {
            outputs.push(EngineOutput::OpenOrdersAtSplit {
                instrument: *key,
                orders: resting_orders,
            });
        }

        // Step 5: options-on-underlying handling. A split targets the underlying equity; listed
        // options on that underlying must also be adjusted (standard split) or flagged for a
        // wrapper-side identity change (non-standard split). The scan mirrors the spot-given-option
        // scan in process_contract_expiry, inverted: find Option instruments whose underlying
        // matches this equity's, on the same exchange.
        //
        // Both `base` AND `quote` are matched: `Underlying` is a full pair identity, and a
        // `CorporateAction` targets a single `InstrumentIndex`. Without the quote filter, a
        // BTC/USDT split would also touch BTC/USDC options the engine never adjusted — wrong.
        // Mirrors the base+quote filter in process_contract_expiry's underlying-spot scan.
        let (equity_base, equity_quote, equity_exchange) = {
            let equity = self.state.instruments.instrument_index(key);
            (
                equity.instrument.underlying.base,
                equity.instrument.underlying.quote,
                equity.instrument.exchange,
            )
        };

        // Classify the split for OPTION handling up front (cheap; only `warn!`s on an unexpected
        // classification). `true` ⇒ adjust the options in place (standard split); `false` ⇒ signal
        // a downstream identity change. A future `#[non_exhaustive]` `SplitAdjustmentKind` variant
        // is handled conservatively as non-standard and `warn!`-surfaced (observable beats silent)
        // — see [`option_split_adjust_in_place`].
        if option_split_adjust_in_place(id, key, kind) {
            // STANDARD (whole-number forward split, OCC Art. VI §11): the option keeps its contract
            // identity, so the adjustment is purely mechanical (strike ÷ ratio; contracts × ratio
            // for held positions; multiplier/contract_size unchanged). Adjust the strike of EVERY
            // registered option on the underlying — held OR unheld. The instrument set is fixed at
            // construction, so an option that is unheld at split time can have a position opened
            // later and then settle at expiry against its strike; leaving it on the pre-split strike
            // would mis-settle that future position. Unheld options get the strike correction
            // SILENTLY (a registry fix, no position event); held options additionally get
            // per-position `apply_split` + observables.
            let options_on_underlying: Vec<InstrumentIndex> = self
                .state
                .instruments
                .0
                .values()
                .filter(|state| {
                    state.is_option_on_underlying(&equity_base, &equity_quote, &equity_exchange)
                })
                .map(|state| state.key)
                .collect();

            for opt_key in options_on_underlying {
                let option_state = self.state.instruments.instrument_index_mut(&opt_key);

                // Adjust the strike in place. The scan only matched Option instruments, so the
                // `else` arm is unreachable; fail loudly rather than silently misadjust if that
                // invariant is ever broken (a re-borrow on a freshly-collected key cannot miss).
                let InstrumentKind::Option(ref mut contract) = option_state.instrument.kind else {
                    unreachable!(
                        "is_option_on_underlying matched a non-Option instrument {opt_key:?}"
                    );
                };
                let strike_pre_split = contract.strike;
                contract.strike /= ratio_decimal;
                let strike_post_split = contract.strike;

                // Unheld option: the strike correction above is the whole job — no positions to
                // split, no observable (a silent registry fix; the user-chosen behaviour).
                if option_state.position.positions.is_empty() {
                    continue;
                }

                // Snapshot the held option's resting orders as an observable BEFORE adjusting its
                // positions — a broker price-adjusts them, so the engine cancels nothing, but the
                // wrapper must know (and a backtest MockExchange would otherwise fill a
                // now-stale-premium resting order against the post-split print).
                let option_orders: Vec<OpenOrderAtSplit> = option_state
                    .orders
                    .orders()
                    .map(|order| OpenOrderAtSplit {
                        cid: order.key.cid.clone(),
                        price_pre_split: order.price,
                        quantity_pre_split: order.quantity,
                    })
                    .collect();

                // The OPTION's own last price (premium) for the eager pnl_unrealised recompute —
                // not the underlying equity's. None ⇒ 0 (+ warn), mirroring the equity path: the
                // split is still applied, corrected on the next market tick.
                let option_last_price = option_state.data.price();
                if option_last_price.is_none() {
                    warn!(
                        %id,
                        instrument = ?opt_key,
                        "CorporateAction: option last price unavailable — pnl_unrealised set to 0 \
                         after the standard-split adjustment, corrected on the next market tick. \
                         The split is still applied (not retryable)."
                    );
                }

                // Apply the split to EACH of the option's positions (N in Hedging) and emit one
                // per-position record. A whole-number ratio × an integer contract count leaves a
                // whole contract count ⇒ no fractional remainder, no CIL, no floor-to-zero,
                // regardless of `policy`.
                let opt_position_ids: Vec<PositionId> =
                    option_state.position.positions.keys().cloned().collect();
                for pos_id in opt_position_ids {
                    // `pos_id` was just collected from this same positions map in a single-threaded
                    // engine, so the re-borrow cannot miss; fail loudly (matching the strike-adjust
                    // `unreachable!` above) if that ever changes, rather than silently skipping an
                    // OptionPositionAdjustedForSplit whose contract asserts the position WAS adjusted.
                    let Some(position) = option_state.position.positions.get_mut(&pos_id) else {
                        unreachable!(
                            "position id {pos_id:?} collected from this option's positions map"
                        );
                    };

                    // Option contract counts are whole, and a standard split is a whole-number
                    // ratio, so `integer × integer` stays integer with no fractional remainder —
                    // the equity's `SplitRoundingPolicy` (a whole-share-lot concept) does not govern
                    // option legs. Enforce that invariant on the INPUT with a hard `assert!` (NOT
                    // `debug_assert!`, which compiles out in release): a non-integer contract count
                    // is state corruption that must fail observably, never be silently floored
                    // (under `Floor`) or silently carried (under `Fractional`). Asserting the input
                    // — rather than the post-split `remainder` — is the meaningful check: under
                    // `Fractional` the remainder is always zero by construction, so a remainder
                    // assert would be vacuous. With the invariant upheld the disposal is provably
                    // zero, so no SplitRemainder/CIL or floor-to-zero close is possible here.
                    assert!(
                        position.quantity_abs.fract().is_zero(),
                        "option position {pos_id:?} holds a non-integer contract count {} before a \
                         standard split — data corruption",
                        position.quantity_abs,
                    );
                    // Pass `Fractional` explicitly, NOT the equity's `policy`: the assert above
                    // guarantees an integer contract count so no remainder can arise (the policy is
                    // a no-op here), and threading the equity's whole-share-lot policy into the
                    // option path would falsely imply it governs option legs. It does not.
                    position.apply_split(
                        ratio_decimal,
                        SplitRoundingPolicy::Fractional,
                        option_last_price,
                    );
                    outputs.push(EngineOutput::OptionPositionAdjustedForSplit {
                        option_instrument: opt_key,
                        ratio,
                        strike_pre_split,
                        strike_post_split,
                        position_id: pos_id,
                    });
                }

                if !option_orders.is_empty() {
                    outputs.push(EngineOutput::OpenOrdersAtSplit {
                        instrument: opt_key,
                        orders: option_orders,
                    });
                }
            }
        } else {
            // NON-STANDARD (every reverse split, every fractional forward split): the OCC assigns a
            // new contract identity (new deliverable / symbol e.g. MSFT → MSFT1), which the engine
            // cannot represent (fixed-at-construction instrument set) — and there is no correct
            // in-place strike fix, since the deliverable itself changes. Touch NO option strikes;
            // only signal the wrapper about HELD option positions that need an identity change. The
            // equity split itself is still applied and the `id` recorded below.
            let affected_options: Vec<InstrumentIndex> = self
                .state
                .instruments
                .0
                .values()
                .filter(|state| {
                    state.is_affected_option_on_underlying(
                        &equity_base,
                        &equity_quote,
                        &equity_exchange,
                    )
                })
                .map(|state| state.key)
                .collect();
            if !affected_options.is_empty() {
                warn!(
                    %id,
                    instrument = ?key,
                    count = affected_options.len(),
                    %ratio,
                    "CorporateAction: non-standard split — option positions on the splitting \
                     underlying require a new contract identity the engine cannot apply. Emitting \
                     OptionPositionsRequireIdentityChange; options left at pre-split terms."
                );
                outputs.push(EngineOutput::OptionPositionsRequireIdentityChange {
                    split_instrument: *key,
                    ratio,
                    affected_options,
                });
            }
        }

        // Step 6: record the `id` — the action has now been applied.
        self.state
            .instruments
            .instrument_index_mut(key)
            .corporate_actions_processed
            .insert(id.clone());

        outputs
    }
}

/// Classify a stock split for **option** handling: `true` ⇒ the engine can adjust the affected
/// options **in place** (a standard whole-number forward split, OCC Art. VI §11); `false` ⇒ it must
/// leave them at pre-split terms and signal a downstream identity change (every reverse split, every
/// fractional forward split, and the `ratio == 1` no-op).
///
/// [`SplitAdjustmentKind`] is `#[non_exhaustive]` and defined in another crate, so it cannot be
/// matched exhaustively here — a future variant will **not** raise a compile error. Rather than let
/// one silently take the non-standard path, this names the known classifications and routes anything
/// unexpected through the conservative non-standard path with a `warn!` (observable over silent). The
/// `None` arm (kind is not a split) is unreachable at the call site — the `StockSplit` is destructured
/// before this runs — but is folded into the same conservative branch.
fn option_split_adjust_in_place(
    id: &SmolStr,
    key: &InstrumentIndex,
    kind: &CorporateActionKind,
) -> bool {
    match kind.split_kind() {
        Some(SplitAdjustmentKind::Standard) => true,
        Some(SplitAdjustmentKind::NonStandard) => false,
        other => {
            warn!(
                %id,
                instrument = ?key,
                classification = ?other,
                "CorporateAction: unexpected option split classification — handling conservatively \
                 as non-standard (engine cannot adjust options in place)."
            );
            false
        }
    }
}

impl<Clock, State, ExecutionTxs, Strategy, Risk> Engine<Clock, State, ExecutionTxs, Strategy, Risk>
where
    Clock: EngineClock,
{
    /// Construct a new `Engine`.
    ///
    /// An initial [`EngineMeta`] is constructed form the provided `clock` and `Sequence(0)`.
    pub fn new(
        clock: Clock,
        state: State,
        execution_txs: ExecutionTxs,
        strategy: Strategy,
        risk: Risk,
    ) -> Self {
        Self {
            meta: EngineMeta {
                time_start: clock.time(),
                sequence: Sequence(0),
            },
            clock,
            state,
            execution_txs,
            strategy,
            risk,
        }
    }

    /// Return `Engine` clock time.
    pub fn time(&self) -> DateTime<Utc> {
        self.clock.time()
    }

    /// Reset the internal `EngineMeta` to the `clock` time and `Sequence(0)`.
    pub fn reset_metadata(&mut self) {
        self.meta.time_start = self.clock.time();
        self.meta.sequence = Sequence(0);
    }
}

/// Output produced by [`Engine`] operations, used to construct an `Engine` [`EngineAudit`].
///
/// `#[non_exhaustive]`: engine-driven outputs (e.g. corporate-action observables) can be added
/// without breaking downstream exhaustive matches. External matchers must carry a wildcard arm.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum EngineOutput<
    OnTradingDisabled,
    OnDisconnect,
    ExchangeKey = ExchangeIndex,
    InstrumentKey = InstrumentIndex,
> {
    Commanded(ActionOutput<ExchangeKey, InstrumentKey>),
    OnTradingDisabled(OnTradingDisabled),
    AccountDisconnect(OnDisconnect),
    PositionExit(PositionExited<AssetIndex, InstrumentKey>),
    MarketDisconnect(OnDisconnect),
    AlgoOrders(GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey>),

    /// Cash-in-lieu observable: a corporate-action split disposed a fractional share quantity
    /// (under [`SplitRoundingPolicy::Floor`]) from one open position. Emitted **per position**
    /// (`position_id` attributes it in hedging mode). The library writes no balances — the
    /// wrapper reconciles this against the broker's CIL payment.
    ///
    /// `quantity_fractional_disposed × price_entry_average_post_split` is the cost basis of the
    /// disposed sliver; both fields share the post-split era (Decimal arithmetic, no ratio).
    ///
    /// # Relationship to [`PositionExit`](Self::PositionExit)
    /// A reverse split under `Floor` can round a position's quantity to zero. When that happens the
    /// handler emits **both** a `SplitRemainder` **and** a [`PositionExit`](Self::PositionExit) for
    /// the same `instrument` + `position_id` — there is no value double-count: the `PositionExit`
    /// closes the now-zero-quantity slot and books its realised PnL, while this `SplitRemainder`
    /// records the disposed fractional sliver as cash-in-lieu. So when both share a `position_id`,
    /// this `SplitRemainder` is the position's **entire** remaining disposal.
    ///
    /// **Ordering contract:** within the handler's output `Vec` the `SplitRemainder` is always
    /// emitted **before** its paired `PositionExit`, so a consumer that books cash-in-lieu before
    /// finalising the close can rely on encountering them in that order.
    SplitRemainder {
        /// Instrument whose position was split.
        instrument: InstrumentKey,
        /// Position the remainder was disposed from.
        position_id: PositionId,
        /// Direction of the position (sign of the disposed sliver).
        side: Side,
        /// Post-split fractional shares disposed by `Floor` rounding (always `< 1`, `> 0`).
        quantity_fractional_disposed: Decimal,
        /// The position's split-adjusted per-share basis (`= old_avg / ratio`), read **after**
        /// the split is applied.
        price_entry_average_post_split: Decimal,
    },

    /// Observable snapshot of the resting orders an instrument had at the moment a split was
    /// applied. The handler **does not** cancel or re-price them — a real broker price-adjusts
    /// resting orders, so an engine-side cancel would diverge. The wrapper decides cancel-vs-keep
    /// (and **must** cancel in backtest, or the mock exchange fills stale-priced orders).
    OpenOrdersAtSplit {
        /// Instrument whose resting orders are listed.
        instrument: InstrumentKey,
        /// The resting orders, captured at pre-split prices/quantities.
        orders: Vec<OpenOrderAtSplit>,
    },

    /// Observable record that a **standard** (OCC even / whole-number-forward) split on an
    /// underlying equity was applied to one option position on that underlying **in place**: the
    /// option's strike was divided by `ratio`, its contract count multiplied by `ratio` (via
    /// [`Position::apply_split`](crate::engine::state::position::Position::apply_split)), and its
    /// cost basis divided by `ratio`. The deliverable/multiplier (`contract_size`) is **unchanged**
    /// — the OCC standard rule keeps the same contract identity, so the engine's ledger stays valid
    /// without any instrument re-registration.
    ///
    /// Emitted **per position** (`position_id` attributes the adjusted slot in hedging mode, where
    /// one option instrument can hold N independent positions) — the same granularity as
    /// [`EngineOutput::SplitRemainder`], because this is an audit record of a completed per-slot
    /// mutation rather than a directive. The strike change is an instrument-level fact repeated in
    /// each per-position record: `strike_pre_split`/`strike_post_split` are identical across all
    /// records for the same `option_instrument` (only `position_id` differs), so a consumer needing
    /// just the strike adjustment can read the first record and ignore the rest.
    ///
    /// The same standard adjustment **also** emits an [`EngineOutput::OpenOrdersAtSplit`] for the
    /// adjusted option's own resting orders, if it has any (their premium is now stale-priced) —
    /// observable, never cancelled, exactly as on the equity path. No [`EngineOutput::SplitRemainder`]
    /// accompanies it: a whole-number ratio applied to an integer contract count disposes no
    /// fractional sliver.
    OptionPositionAdjustedForSplit {
        /// The option instrument whose strike and position were adjusted.
        option_instrument: InstrumentKey,
        /// The forward split ratio that was applied, as a validated [`SplitRatio`] (always `> 0`;
        /// in practice a whole number `> 1`, since this variant is emitted only for standard splits).
        ratio: SplitRatio,
        /// The option strike before the split was applied.
        strike_pre_split: Decimal,
        /// The option strike after the split (`= strike_pre_split / ratio`).
        strike_post_split: Decimal,
        /// The specific position slot that was adjusted (attribution in hedging mode).
        position_id: PositionId,
    },

    /// Observable signal that a **non-standard** split (every reverse split, and every fractional
    /// forward split) on an underlying equity affected option positions the engine **cannot**
    /// adjust in place. The OCC assigns such options a new contract identity (new deliverable, new
    /// symbol e.g. `MSFT` → `MSFT1`), which would require runtime instrument re-registration — a
    /// wrapper concern, not a library one.
    ///
    /// Unlike [`EngineOutput::UnsupportedCorporateAction`], the underlying equity split **was**
    /// applied and its `id` **was** recorded (the `Spot` target always splits); only the listed
    /// option positions are left at their pre-split terms. This is therefore **not** retryable —
    /// the wrapper must close the listed options (and/or open positions under a pre-declared new
    /// identity), it must not re-inject the same action.
    ///
    /// # Backtest pattern
    /// This is fully backtestable through the same aux-event seam used for the split itself, because
    /// a backtest replays *known* history: pre-declare **both** identities at construction (each with
    /// its own data series — the new identity naturally has prints only post-split), then inject BOTH
    /// the [`CorporateAction`](crate::EngineEvent::CorporateAction) and a flatten
    /// [`Command::ClosePositions`](crate::engine::command::Command::ClosePositions) for the old
    /// identity at the split boundary. **Caveat** (identical to the
    /// [`EngineOutput::OpenOrdersAtSplit`] backtest caveat): in backtest the close trigger is
    /// *pre-planned* (the split date is known ahead), not *reactive* from this observable — the
    /// backtest harness disables the audit stream, so this output is invisible there; the reactive
    /// decision code runs live. Fill timing and P&L stay faithful (the aux merge respects each
    /// event's time).
    OptionPositionsRequireIdentityChange {
        /// The underlying equity instrument that was split.
        split_instrument: InstrumentKey,
        /// The split ratio that was applied to the underlying, as a validated [`SplitRatio`]
        /// (always `> 0`).
        ratio: SplitRatio,
        /// Option instruments (holding positions) on that underlying that require a wrapper-side
        /// identity change; left at pre-split terms by the engine.
        affected_options: Vec<InstrumentKey>,
    },

    /// Observable signal that a [`CorporateAction`](crate::EngineEvent::CorporateAction) could
    /// **not** be processed. The action was **not** applied and its `id` was **not** recorded, so
    /// it remains retryable once support is added (e.g. a corrected instrument key, or a future
    /// library version). See [`UnsupportedCorporateActionReason`].
    UnsupportedCorporateAction {
        /// The instrument the rejected action targeted.
        instrument: InstrumentKey,
        /// The action's market-fact kind (carried verbatim for the consumer).
        kind: CorporateActionKind,
        /// Why the action could not be processed.
        reason: UnsupportedCorporateActionReason,
    },
}

/// A single resting order captured in an [`EngineOutput::OpenOrdersAtSplit`] observable.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct OpenOrderAtSplit {
    /// Client order id of the resting order.
    pub cid: ClientOrderId,
    /// Limit price before the split (`None` for market/stop orders).
    pub price_pre_split: Option<Decimal>,
    /// Order quantity before the split.
    pub quantity_pre_split: Decimal,
}

/// Reason a [`CorporateAction`](crate::EngineEvent::CorporateAction) could not be processed,
/// carried by [`EngineOutput::UnsupportedCorporateAction`].
///
/// `#[non_exhaustive]`: further rejection causes (e.g. a non-standard option split requiring a
/// symbol-identity change) can be added without breaking downstream exhaustive matches.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum UnsupportedCorporateActionReason {
    /// The target instrument is not a `Spot` equity (e.g. an option, perpetual, or future).
    /// Equity-split arithmetic is invalid for these; option splits are handled separately.
    InstrumentKindNotSupported,
    /// The corporate-action kind is not handled by this engine version (only stock splits are
    /// supported). Reachable once non-split kinds (dividends, spin-offs, …) are delivered.
    ActionKindNotSupported,
}

/// Output produced by the [`Engine`] updating from an [`TradingState`], used to construct
/// an `Engine` [`EngineAudit`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum UpdateTradingStateOutput<OnTradingDisabled> {
    None,
    OnTradingDisabled(OnTradingDisabled),
}

/// Output produced by the [`Engine`] updating from an [`AccountStreamEvent`], used to construct
/// an `Engine` [`EngineAudit`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)] // PositionExit is rare; avoiding Box keeps API simple
pub enum UpdateFromAccountOutput<OnDisconnect, InstrumentKey = InstrumentIndex> {
    None,
    OnDisconnect(OnDisconnect),
    PositionExit(PositionExited<AssetIndex, InstrumentKey>),
}

/// Output produced by the [`Engine`] updating from an [`MarketStreamEvent`], used to construct
/// an `Engine` [`EngineAudit`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum UpdateFromMarketOutput<OnDisconnect> {
    None,
    OnDisconnect(OnDisconnect),
}

impl<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
    From<ActionOutput<ExchangeKey, InstrumentKey>>
    for EngineOutput<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
{
    fn from(value: ActionOutput<ExchangeKey, InstrumentKey>) -> Self {
        Self::Commanded(value)
    }
}

impl<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
    From<PositionExited<AssetIndex, InstrumentKey>>
    for EngineOutput<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
{
    fn from(value: PositionExited<AssetIndex, InstrumentKey>) -> Self {
        Self::PositionExit(value)
    }
}

impl<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
    From<GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey>>
    for EngineOutput<OnTradingDisabled, OnDisconnect, ExchangeKey, InstrumentKey>
{
    fn from(value: GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey>) -> Self {
        Self::AlgoOrders(value)
    }
}
