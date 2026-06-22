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
    corporate_action::CorporateActionKind,
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
                // OptionPositionsUnadjustedForSplit / PositionExit / UnsupportedCorporateAction)
                // into the audit. Like ContractExpiry, a corporate action is engine-driven and
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
    /// # Algorithm
    /// 1. Idempotency guard on `id` (per-instrument `corporate_actions_processed` set). A
    ///    duplicate `id` is skipped with a warning.
    /// 2. **Unsupported guards (no silent no-op, `id` not recorded ⇒ retryable).** The
    ///    instrument-kind check runs **first** so a split on an option is attributed to the
    ///    instrument, not the (supported) kind:
    ///    - target instrument is not `Spot` ⇒ [`UnsupportedCorporateActionReason::InstrumentKindNotSupported`];
    ///    - action kind is not a stock split ⇒ [`UnsupportedCorporateActionReason::ActionKindNotSupported`]
    ///      (the compiler-mandated arm for the `#[non_exhaustive]` [`CorporateActionKind`]).
    /// 3. Snapshot resting orders into [`EngineOutput::OpenOrdersAtSplit`] (no cancellation).
    /// 4. Apply the split to every open position via [`Position::apply_split`]; emit one
    ///    [`EngineOutput::SplitRemainder`] per position that disposed a fractional sliver. A
    ///    position floored to zero quantity is removed and folded as an
    ///    [`EngineOutput::PositionExit`].
    /// 5. Scan for option positions on the same underlying and emit
    ///    [`EngineOutput::OptionPositionsUnadjustedForSplit`] if any (options are unadjusted in
    ///    this phase).
    /// 6. Record `id` in `corporate_actions_processed`.
    ///
    /// # Missing last price
    /// If the instrument's last price is unavailable the split is **still applied** and the `id`
    /// **recorded** (the quantity/basis arithmetic needs no price); `pnl_unrealised` is set to
    /// zero with a warning and corrected on the next market tick. This is **not** retryable —
    /// contrast `process_contract_expiry`, which bails and is retryable.
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
        let ratio = *ratio;

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
        // Pre-size: up to a SplitRemainder + a PositionExit per position, plus the
        // OpenOrdersAtSplit and OptionPositionsUnadjustedForSplit observables.
        outputs.reserve(position_ids.len() * 2 + 2);

        for pos_id in position_ids {
            let instrument_state = self.state.instruments.instrument_index_mut(key);
            let Some(position) = instrument_state.position.positions.get_mut(&pos_id) else {
                continue;
            };

            let side = position.side;
            let result = position.apply_split(ratio, policy, last_price);
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

        // Step 5: options-on-underlying guard. A split targets the underlying equity; option
        // positions on that underlying are left unadjusted in this phase. Mirror of the
        // spot-given-option scan in process_contract_expiry: find Option instruments whose
        // underlying matches this equity's, on the same exchange, that hold positions.
        //
        // Both `base` AND `quote` are matched: `Underlying` is a full pair identity, and a
        // `CorporateAction` targets a single `InstrumentIndex` (only that instrument's positions
        // are split). Without the quote filter, a BTC/USDT split would also flag BTC/USDC option
        // positions that the engine itself never adjusted — false-positive noise. Mirrors the
        // base+quote filter in process_contract_expiry's underlying-spot scan.
        let (equity_base, equity_quote, equity_exchange) = {
            let equity = self.state.instruments.instrument_index(key);
            (
                equity.instrument.underlying.base,
                equity.instrument.underlying.quote,
                equity.instrument.exchange,
            )
        };
        let affected_options: Vec<InstrumentIndex> = self
            .state
            .instruments
            .0
            .values()
            .filter(|state| {
                matches!(&state.instrument.kind, InstrumentKind::Option(_))
                    && state.instrument.underlying.base == equity_base
                    && state.instrument.underlying.quote == equity_quote
                    && state.instrument.exchange == equity_exchange
                    && !state.position.positions.is_empty()
            })
            .map(|state| state.key)
            .collect();
        if !affected_options.is_empty() {
            warn!(
                %id,
                instrument = ?key,
                count = affected_options.len(),
                "CorporateAction: option positions on the splitting underlying are left \
                 UNADJUSTED (option corporate-action handling is deferred). Emitting \
                 OptionPositionsUnadjustedForSplit."
            );
            outputs.push(EngineOutput::OptionPositionsUnadjustedForSplit {
                split_instrument: *key,
                ratio,
                affected_options,
            });
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

    /// Observable signal that option positions on a splitting underlying were left **unadjusted**
    /// (option corporate-action handling is deferred). The wrapper decides what to do.
    OptionPositionsUnadjustedForSplit {
        /// The underlying equity instrument that was split.
        split_instrument: InstrumentKey,
        /// The split ratio that was applied to the underlying.
        ratio: Decimal,
        /// Option instruments (holding positions) on that underlying, left unadjusted.
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
