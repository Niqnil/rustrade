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
            EngineState, MarketSnapshotSource,
            connectivity::UntrackedExchange,
            instrument::{OptionSplitPlan, data::InstrumentDataState},
            order::{Orders, in_flight_recorder::InFlightRequestRecorder, manager::OrderManager},
            position::{Position, PositionExited, PositionId, SplitRoundingPolicy},
            trading::TradingState,
        },
    },
    execution::{AccountStreamEvent, request::ExecutionRequest},
    risk::RiskManager,
    shutdown::{Shutdown, SyncShutdown},
    statistic::summary::TradingSummaryGenerator,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use indexmap::IndexMap;
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

    /// Whether a [`Shutdown::AfterDrain`] is in progress.
    ///
    /// While set, the `Engine` generates no new orders and ends the run as soon as nothing is
    /// left in flight. Cleared by [`Engine::reset_metadata`].
    ///
    /// `#[serde(default)]` so metadata serialised before this field existed still loads.
    #[serde(default)]
    pub draining: bool,
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

        // Whether this event may provoke new algo orders. `ContractExpiry` and `CorporateAction`
        // are engine-driven settlements that apply regardless of `TradingState` and never do.
        let mut generates_algo_orders = true;

        let process_audit = match &event {
            EngineEvent::Shutdown(Shutdown::Immediate) => return EngineAudit::process(event),
            EngineEvent::Shutdown(Shutdown::AfterDrain) => {
                // Stop generating new orders, and tell every ExecutionManager to drain. The run
                // ends when the account feed ends, not here: each manager finishes its in-flight
                // requests, forwards the account events those produced, and only then closes its
                // channel, which ends the feed and terminates this Engine with `FeedEnded`.
                //
                // The Engine deliberately does *not* decide the moment itself. Its only local
                // signal is request quiescence, and a response resolves an in-flight request
                // before the trade that opens the position has necessarily been read — stopping
                // on it truncated the trade and balance ledgers by a varying amount every run.
                self.meta.draining = true;
                self.drain_execution();
                return EngineAudit::process(event);
            }
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
                // trigger algo order generation.
                generates_algo_orders = false;
                audit
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
                // corporate action is engine-driven and settles regardless of TradingState.
                let mut audit = ProcessAudit::with_event(event);
                for output in outputs {
                    audit = audit.add_output(output);
                }
                generates_algo_orders = false;
                audit
            }
        };

        // A drain in progress outranks everything below: no new orders. The run is *not* ended
        // here — see the `Shutdown::AfterDrain` arm above for why the execution side owns that
        // decision.
        if self.meta.draining {
            return EngineAudit::from(process_audit);
        }

        if generates_algo_orders && matches!(self.state.trading, TradingState::Enabled) {
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
    /// Tell every `ExecutionManager` to finish what it has in flight and then stop.
    ///
    /// Sends [`ExecutionRequest::Drain`], the graceful counterpart to the
    /// [`ExecutionRequest::Shutdown`] that [`SyncShutdown::shutdown`] sends. Used on entry to a
    /// [`Shutdown::AfterDrain`]: the managers, not the
    /// `Engine`, decide when the run has genuinely finished, because only they can see whether the
    /// account events belonging to a completed request have been read yet.
    ///
    /// Send failures are ignored — a manager that has already stopped needs no telling.
    ///
    /// [`Shutdown::AfterDrain`]: crate::shutdown::Shutdown::AfterDrain
    pub fn drain_execution(&self)
    where
        ExecutionTxs: ExecutionTxMap,
    {
        self.execution_txs.iter().for_each(|execution_tx| {
            let _send_result = execution_tx.send(ExecutionRequest::Drain);
        });
    }

    /// Action an `Engine` [`Command`], producing an [`ActionOutput`] of work done.
    pub fn action(&mut self, command: &Command) -> ActionOutput
    where
        InstrumentData: InstrumentDataState + InFlightRequestRecorder,
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
                self.state.record_in_flight_cancels(output.sent_iter());
                ActionOutput::CancelOrders(output)
            }
            Command::SendOpenRequests(requests) => {
                info!(?requests, "Engine actioning user Command::SendOpenRequests");
                // Stamped like any other open the Engine emits -- a user command is no less a
                // decision point than an algo order, and a simulated venue needs a price for it
                // just the same. See `MarketSnapshotSource`.
                let output = self.send_requests(requests.iter().cloned().map(|mut open| {
                    open.state.market = self.state.market_snapshot(&open.key.instrument);
                    open
                }));
                self.state.record_in_flight_opens(output.sent_iter());
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
                // An untracked exchange skips `on_disconnect` as well as the state update: the
                // strategy has no link to that venue to react to, and the engine's own health is
                // unaffected by one it never tracked.
                match self.state.update_from_account_reconnecting(exchange) {
                    Ok(()) => UpdateFromAccountOutput::OnDisconnect(Strategy::on_disconnect(
                        self, *exchange,
                    )),
                    Err(untracked) => UpdateFromAccountOutput::UntrackedExchange(untracked),
                }
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
                // An untracked exchange skips `on_disconnect` as well as the state update — see the
                // equivalent account-stream arm above.
                match self
                    .state
                    .connectivity
                    .update_from_market_reconnecting(exchange)
                {
                    Ok(()) => UpdateFromMarketOutput::OnDisconnect(Strategy::on_disconnect(
                        self, *exchange,
                    )),
                    Err(untracked) => UpdateFromMarketOutput::UntrackedExchange(untracked),
                }
            }
            MarketStreamEvent::Item(event) => match self.state.update_from_market(event) {
                Ok(()) => UpdateFromMarketOutput::None,
                Err(untracked) => UpdateFromMarketOutput::UntrackedExchange(untracked),
            },
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

        // Guard: idempotent — ignore duplicates after first processing. Warn so the skip is
        // observable in logs (this settlement path returns PositionExits, not EngineOutputs, so it
        // has no audit-stream signal to emit — unlike process_corporate_action's duplicate-id path).
        if instrument_state.expiration_processed {
            warn!(
                instrument = ?key,
                "ContractExpiry already processed — skipping (idempotent). Re-injecting the same \
                 expiry is a no-op."
            );
            return vec![];
        }

        // Step 2: Cancel all open orders for this instrument.
        let cancel_requests: Vec<_> = instrument_state
            .orders
            .orders()
            .filter_map(Order::to_request_cancel)
            .collect();
        let cancels = self.send_requests(cancel_requests);
        self.state.record_in_flight_cancels(cancels.sent_iter());

        // Re-borrow after send_requests (which takes &self for execution_txs).
        let instrument_state = self.state.instruments.instrument_index_mut(key);

        // Step 3–4: Synthesise settlement fills only if positions are open.
        if instrument_state.position.positions.is_empty() {
            instrument_state.expiration_processed = true;
            instrument_state.orders.clear();
            instrument_state.exchange_id_to_cid.clear();
            instrument_state.retired_routing.clear();
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

        // Capture the contract expiry while `instrument_state` is already borrowed (used further
        // below to advance the engine clock) — avoids a second `instrument_index` lookup for `kind`.
        let expiry = instrument_state.instrument.kind.expiry();

        let spot_price = match option_spec {
            Some((base_key, quote_key, exchange)) => {
                // Find the spot instrument on the same exchange whose underlying matches
                // the option's underlying base AND quote. Both are required: without the
                // quote filter, BTC/USDT and BTC/USDC options on the same exchange would
                // silently share the same spot price (M3).
                // Single-pass: collect all matching spot instruments so we can both
                // warn on ambiguity (visible in production) and use the first match,
                // without scanning the instrument list twice.
                //
                // The rule here is "the settlement reference is the DELIVERABLE underlying" —
                // `InstrumentKind::Spot` is only its current spelling. An option settles into, or
                // cash-settles against, the deliverable; a `Cfd` is never deliverable (an AAPL
                // option settles against AAPL shares, not an AAPL CFD), so it is excluded
                // permanently, not pending a future CFD broker. Admitting a second eligible kind
                // would also arbitrate the most consequential number in this function by map
                // insertion order, ie/ by the order the user happened to declare instruments.
                //
                // Deliberately NOT the same predicate as `is_split_eligible`. The two agree on
                // every variant today, but they answer different questions — "is this the
                // deliverable an option settles against" versus "can split arithmetic be applied
                // to this" — so folding them would tie each one's future to the other's. Widen
                // either on its own rule, not on the fact that they currently match.
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

        // Advance the engine clock to the contract's expiry instant before stamping the synthetic
        // settlement trades below. The `ContractExpiry` event carries no timestamp on its payload
        // (unlike `CorporateAction`, whose `effective_time` advances the clock via `TimeExchange`),
        // so without this a backtest would stamp the settlement fill at the prior market tick. The
        // expiry instant is engine-side ground truth on the instrument's kind (captured above).
        // `advance_to` is monotonic and a no-op on `LiveClock`; non-expiring kinds yield `None`.
        if let Some(expiry) = expiry {
            self.clock.advance_to(expiry);
        }

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
                // Engine-generated settlement, not a venue execution: there is no order for it
                // to advance.
                order_filled_quantity: None,
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
        instrument_state.retired_routing.clear();
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
    ///      (the compiler-mandated arm for the `#[non_exhaustive]` [`CorporateActionKind`]);
    ///    - another registered instrument is also split-eligible on the target's
    ///      `(base, quote, exchange)` ⇒ [`UnsupportedCorporateActionReason::AmbiguousSplitTarget`],
    ///      because the option chain below is resolved by that identity alone.
    /// 3. Snapshot resting orders into [`EngineOutput::OpenOrdersAtSplit`] (no cancellation).
    /// 4. Apply the split to every open position — the same per-position rescale as
    ///    [`Position::apply_split`](crate::engine::state::position::Position::apply_split), but the
    ///    handler does **not** call it directly: it pre-computes every affected position atomically
    ///    (prepare) then writes each infallibly (commit), so a mid-batch overflow rejects the whole
    ///    action rather than leaving a subset rescaled. Emit one
    ///    [`EngineOutput::SplitRemainder`] per position that disposed a fractional sliver. A
    ///    position floored to zero quantity is removed and folded as an
    ///    [`EngineOutput::PositionExit`].
    /// 5. Scan for option positions on the same underlying and handle them per the OCC
    ///    standard/non-standard rule ([`CorporateActionKind::split_kind`]):
    ///    - **standard** (whole-number forward split) ⇒ adjust each option **in place**
    ///      (strike ÷ `ratio`, contracts × `ratio` — the same rescale as
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
    ///
    ///    On the **standard** path `id` is recorded in each adjusted option's *own*
    ///    `corporate_actions_processed`, so an option already carrying it is skipped rather than
    ///    strike-divided twice — surfaced as a per-option
    ///    [`EngineOutput::CorporateActionAlreadyProcessed`]. The non-standard path mutates no option
    ///    state and therefore records nothing on the options.
    /// 6. Record `id` in the **target's** `corporate_actions_processed`.
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
            // Surface the idempotent skip on the output/audit stream (distinct from the
            // rejection paths' UnsupportedCorporateAction — this is a non-mutating no-op, not a
            // retryable rejection), so a consumer watching only the stream can tell it apart from a
            // successful split that had nothing to adjust.
            outputs.push(EngineOutput::CorporateActionAlreadyProcessed {
                instrument: *key,
                kind: kind.clone(),
                id: id.clone(),
            });
            return outputs;
        }

        // Step 2a: reject targets that are not the deliverable equity (checked BEFORE the action
        // kind so a split on an option is attributed to the instrument, not the supported kind).
        // Do NOT record `id`.
        //
        // The rule is "the deliverable equity" — `InstrumentKind::Spot` is merely its current
        // spelling — and it lives in `InstrumentKind::is_split_eligible`, which the audit replica
        // calls too. Single-sourced deliberately: the split arithmetic these guards protect was
        // already single-sourced into `prepare_corporate_action_split` for the same reason, so
        // the replica cannot drift from the live engine by hand-mirroring vigilance.
        if !instrument_state.instrument.kind.is_split_eligible() {
            warn!(
                %id,
                instrument = ?key,
                kind = ?instrument_state.instrument.kind,
                "CorporateAction targets an instrument that is not the deliverable equity — \
                 unsupported (equity splits only). Emitting UnsupportedCorporateAction; id NOT \
                 recorded (retryable)."
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
        // invariant unbroken into the output records and into pre-validation; the split arithmetic
        // (including the option strike division) now lives in `prepare_corporate_action_split`, so
        // the handler no longer extracts the inner `Decimal` here.
        let ratio: SplitRatio = *ratio;

        // Classify the split for OPTION handling ONCE, up front: `true` ⇒ adjust the options in
        // place (standard whole-number forward split); `false` ⇒ signal a downstream identity
        // change. Computed here so pre-validation (Step 2c) and application (Step 5) share the
        // identical classification and can never drift.
        let adjust_options_in_place = option_split_adjust_in_place(id, key, kind);

        // Step 2c: PRE-COMPUTE the whole split against EVERY position and option strike it would
        // mutate, WITHOUT mutating anything, BEFORE Step 3's output push and Step 4's mutation —
        // producing a `SplitPlan` the commit phase (Steps 4–5) applies INFALLIBLY. A feed that would
        // overflow `Decimal` (an equity/option-position rescale OR an option strike division), or a
        // corrupted (non-integer) option contract count, rejects the action ATOMICALLY here — no
        // position or strike is partially mutated and the `id` is NOT recorded (retryable once the
        // blocking condition is resolved). Single-sourced with the audit replica via
        // `InstrumentStates::prepare_corporate_action_split`, so both reach the identical
        // accept/reject decision AND the identical committed values by construction rather than by
        // hand-mirrored vigilance. Because every fallible arithmetic step ran here, the commit loops
        // below carry no overflow error path — the previous per-position `apply_split` `unreachable!`
        // arms are gone by construction.
        let split_plan = match self.state.instruments.prepare_corporate_action_split(
            id,
            key,
            ratio,
            policy,
            adjust_options_in_place,
        ) {
            Ok(plan) => plan,
            Err(reason) => {
                warn!(
                    %id,
                    instrument = ?key,
                    ?reason,
                    "CorporateAction rejected by pre-validation (an ambiguous target, or a \
                     mutation that could not be applied atomically) — nothing was mutated. \
                     Emitting UnsupportedCorporateAction; id NOT recorded (retryable once the \
                     blocking condition is resolved)."
                );
                outputs.push(EngineOutput::UnsupportedCorporateAction {
                    instrument: *key,
                    kind: kind.clone(),
                    reason,
                });
                return outputs;
            }
        };

        // Step 3: snapshot resting orders as an observable — do NOT cancel (a broker price-adjusts
        // them, so an engine-side cancel would diverge from the broker). Re-borrow the instrument
        // state: pre-validation above released the earlier borrow.
        let instrument_state = self.state.instruments.instrument_index(key);
        let resting_orders = snapshot_open_orders_at_split(&instrument_state.orders);

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

        // Step 4: COMMIT the split to ALL open positions (N in Hedging mode). Drive the loop from the
        // `SplitPlan`'s pre-computed rescales — one `(PositionId, PreparedSplit)` per open position,
        // captured in position-map order — instead of re-collecting ids: the plan already snapshots
        // exactly the positions Step 2c pre-computed, so committing them cannot overflow (the
        // fallible arithmetic is already done). The per-position re-borrow still needs owned ids,
        // which the plan carries.
        // Pre-size for the equity leg: up to a SplitRemainder + a PositionExit per position, plus the
        // equity OpenOrdersAtSplit and one option-handling observable. This is a lower-bound hint, not
        // an exact count — the standard-option path may push further OptionPositionAdjustedForSplit /
        // per-option OpenOrdersAtSplit outputs, which the `Vec` accommodates by growing as needed.
        outputs.reserve(split_plan.equity_positions.len() * 2 + 2);

        for (pos_id, prepared) in split_plan.equity_positions {
            let instrument_state = self.state.instruments.instrument_index_mut(key);
            // Fail loudly on a missing id (see `split_plan_position_mut`): the plan collected
            // `pos_id` from this same map (only Step 3's read-only order snapshot ran since), so a
            // miss is plan/state corruption that must crash, never silently continue.
            let position = split_plan_position_mut(
                &mut instrument_state.position.positions,
                &pos_id,
                SplitCommitContext {
                    leg: "equity",
                    instrument: key,
                    id,
                    ratio,
                },
            );

            let side = position.side;
            // Infallible: Step 2c pre-computed (and overflow-checked) this position's rescale into
            // `prepared`, so the commit only writes fields + does the self-correcting `pnl_unrealised`
            // recompute (which degrades to 0 on an extreme `last_price`, flagged on the result).
            let result = position.commit_split(prepared, last_price);
            if result.pnl_unrealised_overflowed {
                warn!(
                    %id,
                    instrument = ?key,
                    position_id = ?pos_id,
                    "CorporateAction: post-split pnl_unrealised recompute overflowed Decimal \
                     (extreme last price) — set to 0, corrected on the next market tick. The split \
                     (quantity/basis) was still applied atomically."
                );
            }
            // Read the post-split basis AFTER commit_split overwrote it: quantity
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
        // wrapper-side identity change (non-standard split).
        //
        // Step 5 branches on the option classification computed up front in Step 2c — reused here
        // (rather than recomputed) so application can never diverge from what pre-validation
        // checked. `true` ⇒ adjust the options in place (standard split); `false` ⇒ signal a
        // downstream identity change (see [`option_split_adjust_in_place`]).
        if adjust_options_in_place {
            // Surface every option this action had ALREADY adjusted (its own
            // `corporate_actions_processed` carries `id`), which pre-validation excluded from the
            // plan so the strike is not divided twice. Reported as the same non-mutating
            // `CorporateActionAlreadyProcessed` the target-level guard emits — same meaning, per
            // instrument — so a consumer watching only the stream can tell a suppressed re-adjust
            // from an option that simply had nothing to adjust. One aggregate warning, not one per
            // option: a full chain can be large.
            if !split_plan.options_already_adjusted.is_empty() {
                warn!(
                    %id,
                    instrument = ?key,
                    count = split_plan.options_already_adjusted.len(),
                    "CorporateAction: options on the splitting underlying already carry this id \
                     and were skipped (idempotent) — they were adjusted by an earlier delivery of \
                     this same action."
                );
                outputs.extend(split_plan.options_already_adjusted.into_iter().map(
                    |option_instrument| EngineOutput::CorporateActionAlreadyProcessed {
                        instrument: option_instrument,
                        kind: kind.clone(),
                        id: id.clone(),
                    },
                ));
            }
            self.commit_standard_option_split(id, ratio, split_plan.options, &mut outputs);
        } else {
            self.emit_option_identity_change(id, key, ratio, &mut outputs);
        }

        // Step 6: record the `id` — the action has now been applied.
        self.state
            .instruments
            .instrument_index_mut(key)
            .corporate_actions_processed
            .insert(id.clone());

        outputs
    }

    /// Commit the **standard**-split option leg: adjust every registered option on the splitting
    /// underlying in place.
    ///
    /// A standard split (whole-number forward, OCC Art. VI §11) leaves the option's contract
    /// identity intact, so the adjustment is purely mechanical (strike ÷ ratio; contracts × ratio for
    /// held positions; multiplier/contract_size unchanged). The strike of EVERY registered option on
    /// the underlying — held OR unheld — is adjusted. The instrument set is fixed at construction, so
    /// an option that is unheld at split time can have a position opened later and then settle at
    /// expiry against its strike; leaving it on the pre-split strike would mis-settle that future
    /// position. Unheld options get the strike correction SILENTLY (a registry fix, no position
    /// event); held options additionally get per-position commit + observables.
    ///
    /// Driven entirely from the pre-computed [`SplitPlan`](crate::engine::state::instrument::SplitPlan):
    /// one [`OptionSplitPlan`] per registered option on the underlying (held OR unheld), each
    /// carrying the CHECKED post-split strike and a `PreparedSplit` for every held position. No
    /// re-scan and no in-place `strike /=` (that previously-unchecked division is overflow-checked
    /// while preparing the plan) — the plan is authoritative, so this commit phase is infallible.
    /// Options this action had already adjusted are absent from the plan (see
    /// [`SplitPlan::options_already_adjusted`](crate::engine::state::instrument::SplitPlan)), so
    /// every entry reaching this loop is one to mutate.
    ///
    /// Each adjusted option records `id` in its **own** `corporate_actions_processed`, which is what
    /// makes that exclusion possible on a later delivery of the same action.
    fn commit_standard_option_split<OnTradingDisabled, OnDisconnect>(
        &mut self,
        id: &SmolStr,
        ratio: SplitRatio,
        options: Vec<OptionSplitPlan>,
        outputs: &mut Vec<EngineOutput<OnTradingDisabled, OnDisconnect>>,
    ) where
        InstrumentData: InstrumentDataState,
    {
        for opt_plan in options {
            let opt_key = opt_plan.key;
            let option_state = self.state.instruments.instrument_index_mut(&opt_key);

            // Commit the pre-checked strike in place. The plan was built only from Option
            // instruments (`is_option_on_underlying`), so the `else` is a structural invariant;
            // fail loudly rather than silently misadjust if it is ever broken.
            let InstrumentKind::Option(ref mut contract) = option_state.instrument.kind else {
                unreachable!(
                    "SplitPlan carried a non-Option instrument {opt_key:?} \
                         (id={id}, ratio={ratio})"
                );
            };
            let strike_pre_split = contract.strike;
            contract.strike = opt_plan.strike_post_split;
            let strike_post_split = opt_plan.strike_post_split;

            // Record `id` on the OPTION's own set, not just the target's: this option's strike has
            // now been divided, and only its own record can stop a second trigger for the same
            // action from dividing it again. Written here — before the unheld early-out below — so
            // an unheld option, whose adjustment is otherwise a silent registry fix with no
            // position event, carries the same evidence a held one does.
            option_state.corporate_actions_processed.insert(id.clone());

            // Snapshot the option's resting orders as an observable BEFORE the unheld early-out
            // and before adjusting any positions — a broker price-adjusts them, so the engine
            // cancels nothing, but the wrapper must know (and a backtest MockExchange would
            // otherwise fill a now-stale-premium resting order against the post-split print).
            // Mirrors the equity leg, which snapshots regardless of position state: an UNHELD
            // option can still carry a resting order to *open* a position, whose premium is now
            // stale against the post-split strike, so it must be surfaced just the same.
            let option_orders = snapshot_open_orders_at_split(&option_state.orders);

            // Unheld option: the strike correction + resting-order snapshot above are the whole
            // job — no positions to split (`opt_plan.positions` is empty). Still surface any
            // resting orders (equity-leg parity), then move on; no per-position observable applies.
            if opt_plan.positions.is_empty() {
                if !option_orders.is_empty() {
                    outputs.push(EngineOutput::OpenOrdersAtSplit {
                        instrument: opt_key,
                        orders: option_orders,
                    });
                }
                continue;
            }

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

            // Commit the split to EACH of the option's positions (N in Hedging) and emit one
            // per-position record, driving from the plan's pre-computed rescales. A whole-number
            // ratio × an integer contract count leaves a whole contract count ⇒ no fractional
            // remainder, no CIL, no floor-to-zero, regardless of `policy` — Step 2c pre-computed
            // each leg with `Fractional` (the equity's whole-share-lot `SplitRoundingPolicy` does
            // not govern option legs) having already rejected any non-integer contract count as
            // `PositionStateInvalid`.
            for (pos_id, prepared) in opt_plan.positions {
                // Fail loudly on a missing id (see `split_plan_position_mut`), matching the
                // strike-adjust `unreachable!` above rather than silently skipping an
                // OptionPositionAdjustedForSplit whose contract asserts the position WAS adjusted.
                let position = split_plan_position_mut(
                    &mut option_state.position.positions,
                    &pos_id,
                    SplitCommitContext {
                        leg: "option",
                        instrument: &opt_key,
                        id,
                        ratio,
                    },
                );

                // Infallible: Step 2c pre-computed (and overflow-checked) this option leg's
                // rescale into `prepared`; the commit only writes fields + the self-correcting
                // `pnl_unrealised` recompute (degrades to 0 on an extreme premium, flagged).
                let opt_result = position.commit_split(prepared, option_last_price);
                if opt_result.pnl_unrealised_overflowed {
                    warn!(
                        %id,
                        instrument = ?opt_key,
                        position_id = ?pos_id,
                        "CorporateAction: post-split option pnl_unrealised recompute overflowed \
                         Decimal (extreme premium) — set to 0, corrected on the next market \
                         tick. The option adjustment (contract count/basis) was still applied \
                         atomically."
                    );
                }
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
    }

    /// Emit the **non-standard**-split option signal, adjusting no option state.
    ///
    /// On a non-standard split (every reverse split, every fractional forward split) the OCC assigns
    /// a new contract identity (new deliverable / symbol e.g. MSFT → MSFT1), which the engine cannot
    /// represent (fixed-at-construction instrument set) — and there is no correct in-place strike
    /// fix, since the deliverable itself changes. So this touches NO option strikes; it only signals
    /// the wrapper about HELD option positions that need an identity change, leaving them at
    /// pre-split terms. The equity split itself is still applied and the `id` still recorded by the
    /// caller.
    fn emit_option_identity_change<OnTradingDisabled, OnDisconnect>(
        &self,
        id: &SmolStr,
        key: &InstrumentIndex,
        ratio: SplitRatio,
        outputs: &mut Vec<EngineOutput<OnTradingDisabled, OnDisconnect>>,
    ) {
        // The base/quote/exchange identity is computed here because only the non-standard path
        // needs it — the standard path's option scan lives in the pre-computed `SplitPlan`. Both
        // `base` AND `quote` are matched: `Underlying` is a full pair identity, and a
        // `CorporateAction` targets a single `InstrumentIndex`; without the quote filter a BTC/USDT
        // split would also touch BTC/USDC options the engine never adjusted — wrong.
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
}

/// Collapse a stock split's OCC [`SplitAdjustmentKind`] into the engine **policy** decision of
/// whether this engine can adjust affected option positions **in place**:
/// - `Ok(true)` — a **standard** whole-number forward split (OCC Art. VI §11): strike ÷ ratio,
///   contracts × ratio, same contract identity ⇒ adjustable in place;
/// - `Ok(false)` — a **non-standard** split (every reverse split, every fractional forward split, and
///   the `ratio == 1` no-op): the OCC assigns a new contract identity the engine cannot represent ⇒
///   leave the options at pre-split terms;
/// - `Err(other)` — the kind is not a split (unreachable once `StockSplit` is destructured) **or** a
///   future `#[non_exhaustive]` [`SplitAdjustmentKind`] variant this engine does not yet classify.
///   The conservative fallback (treat as non-standard) and any logging are the **caller's** choice, so
///   this stays pure and side-effect-free — letting the live handler `warn!` while the audit replica
///   (which re-derives state silently) does not.
///
/// Single-sourced so the live [`Engine::process_corporate_action`] and the audit replica reach the
/// identical Standard→in-place decision by construction rather than by two hand-mirrored matches. This
/// is engine **policy**, not an OCC/domain fact (the fact is [`CorporateActionKind::split_kind`]), so
/// it lives here in the engine crate, `pub(crate)`.
pub(crate) fn classify_option_split(
    kind: &CorporateActionKind,
) -> Result<bool, Option<SplitAdjustmentKind>> {
    match kind.split_kind() {
        Some(SplitAdjustmentKind::Standard) => Ok(true),
        Some(SplitAdjustmentKind::NonStandard) => Ok(false),
        other => Err(other),
    }
}

/// The live handler's thin wrapper over [`classify_option_split`]: same Standard→`true` /
/// non-standard→`false` decision, but `warn!`ing (observable over silent) when the classification is
/// an unexpected/future variant before conservatively taking the non-standard path. The audit replica
/// calls [`classify_option_split`] directly (no `warn!` — the live engine already logged it).
fn option_split_adjust_in_place(
    id: &SmolStr,
    key: &InstrumentIndex,
    kind: &CorporateActionKind,
) -> bool {
    classify_option_split(kind).unwrap_or_else(|other| {
        warn!(
            %id,
            instrument = ?key,
            classification = ?other,
            "CorporateAction: unexpected option split classification — handling conservatively \
             as non-standard (engine cannot adjust options in place)."
        );
        false
    })
}

/// Diagnostic context threaded into [`split_plan_position_mut`]'s cold `unreachable!` path. Every
/// field feeds **only** the panic message; grouping them here keeps the hot lookup signature down to
/// the map and the position id. `leg` and `instrument` distinguish which of the four commit loops (live +
/// replica × equity + option) diverged; `id` and `ratio` identify the split.
pub(crate) struct SplitCommitContext<'a, InstrumentKey> {
    /// Which commit loop hit the divergence: `"equity"`, `"option"`, `"replica equity"`, or
    /// `"replica option"`.
    pub leg: &'a str,
    /// The underlying/option instrument key whose positions map the plan indexed.
    pub instrument: &'a InstrumentKey,
    /// The corporate-action `id` being committed.
    pub id: &'a SmolStr,
    /// The (validated, strictly positive) split ratio.
    pub ratio: SplitRatio,
}

/// Look up a position the pre-validated [`SplitPlan`](crate::engine::state::instrument::SplitPlan)
/// committed the handler to rescaling, panicking loudly if it is absent.
///
/// The plan collected `pos_id` from this same positions map earlier in the same single-threaded
/// commit, so a miss is a plan/state divergence = state corruption, which **must** crash rather than
/// silently drop a position and let the live engine and its audit replica drift (per CQRS: a
/// post-validation application failure is corruption, never a retryable domain error). Shared by all
/// four split commit loops (live + replica × equity + option) so they fail identically.
///
/// `#[track_caller]` so the panic reports the **calling** commit loop's location, not this shared
/// helper's — preserving the per-site diagnosability of the four distinct `unreachable!`s it replaces.
/// The [`SplitCommitContext`] carries each site's distinguishing `leg` (e.g. `"equity"` /
/// `"replica option"`) and `instrument` key, preserving per-site diagnosability under one shared,
/// normalized message template.
#[track_caller]
pub(crate) fn split_plan_position_mut<'m, AssetKey, InstrumentKey>(
    positions: &'m mut IndexMap<PositionId, Position<AssetKey, InstrumentKey>>,
    pos_id: &PositionId,
    context: SplitCommitContext<'_, InstrumentKey>,
) -> &'m mut Position<AssetKey, InstrumentKey>
where
    InstrumentKey: Debug,
{
    match positions.get_mut(pos_id) {
        Some(position) => position,
        // Plan/state divergence = corruption (see above): `unreachable!`, matching the sibling
        // invariant checks and the four inline `unreachable!`s this helper replaces.
        None => {
            let SplitCommitContext {
                leg,
                instrument,
                id,
                ratio,
            } = context;
            unreachable!(
                "SplitPlan carried {leg} position id {pos_id:?} absent from instrument \
                 {instrument:?}'s positions map (id={id}, ratio={ratio})"
            )
        }
    }
}

/// Snapshot every resting order in `orders` into the [`OpenOrderAtSplit`] records carried by an
/// [`EngineOutput::OpenOrdersAtSplit`] observable. Shared by the equity and held-option snapshot
/// sites in `process_corporate_action` so both capture exactly the same fields — a split cancels
/// nothing (a broker price-adjusts resting orders), it only reports them.
fn snapshot_open_orders_at_split(
    orders: &Orders<ExchangeIndex, InstrumentIndex>,
) -> Vec<OpenOrderAtSplit> {
    orders
        .orders()
        .map(|order| OpenOrderAtSplit {
            cid: order.key.cid.clone(),
            price_pre_split: order.price,
            quantity_pre_split: order.quantity,
        })
        .collect()
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
                draining: false,
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
        self.meta.draining = false;
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
    /// Output of an actioned [`Command`].
    ///
    /// **Inline.** [`ActionOutput`]'s inner order payloads are boxed at the root (#195), so
    /// `ActionOutput` is only ~96 B — well under the ~232 B [`PositionExit`](Self::PositionExit)
    /// variant that floors this enum's size. Carrying it inline therefore costs `EngineOutput`
    /// nothing in stack size (and thus in the per-tick [`ProcessAudit`] copy), while avoiding a
    /// heap allocation and pointer indirection on the command path that an outer `Box` would add.
    /// The audit wire format is unchanged (a newtype variant serializes its payload identically
    /// whether boxed or not).
    Commanded(ActionOutput<ExchangeKey, InstrumentKey>),
    OnTradingDisabled(OnTradingDisabled),
    AccountDisconnect(OnDisconnect),
    PositionExit(PositionExited<AssetIndex, InstrumentKey>),
    MarketDisconnect(OnDisconnect),

    /// Summary of algorithmic orders generated for a processed event (the per-tick auto-generation
    /// path).
    ///
    /// **Inline.** `GenerateAlgoOrdersOutput`'s inner order payloads are boxed at the root (#195),
    /// so it is only ~144 B — under the ~232 B [`PositionExit`](Self::PositionExit) variant that
    /// floors this enum's size. Carrying it inline therefore costs `EngineOutput` nothing in stack
    /// size (and thus in the per-tick [`ProcessAudit`] copy). The only allocation is the root
    /// boxing of the orders themselves, paid solely when the strategy actually emits some — the
    /// empty no-order case short-circuits before this variant is even constructed. The audit wire
    /// format is unchanged (a newtype variant serializes its payload identically whether boxed or
    /// not).
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
    /// option's strike was divided by `ratio`, its contract count multiplied by `ratio` (the same
    /// rescale as [`Position::apply_split`](crate::engine::state::position::Position::apply_split),
    /// which the handler does not call directly), and its
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
    /// **Resting option orders are the caller's responsibility here.** Unlike the standard-split
    /// path (and the equity path), this non-standard branch emits **no**
    /// [`EngineOutput::OpenOrdersAtSplit`] for the affected options — the engine touches no option
    /// state on a non-standard split. Any resting orders on the old option identity are therefore
    /// **not** surfaced through this observable; the wrapper must cancel them as part of the
    /// identity change, since they reference a contract identity that no longer trades post-split.
    ///
    /// # Backtest pattern
    /// This is fully backtestable through the same aux-event seam used for the split itself, because
    /// a backtest replays *known* history: pre-declare **both** identities at construction (each with
    /// its own data series — the new identity naturally has prints only post-split), then inject BOTH
    /// the [`CorporateAction`](crate::EngineEvent::CorporateAction) and a flatten
    /// [`Command::ClosePositions`] for the old identity at the split boundary. **Caveat**
    /// (identical to the
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
    /// it remains retryable once the blocking condition is resolved — a corrected instrument key, a
    /// future library version, or a reconciled position. Note some reasons are only retryable after
    /// upstream state is fixed (not automatically on the next identical event); see the per-variant
    /// notes on [`UnsupportedCorporateActionReason`].
    UnsupportedCorporateAction {
        /// The instrument the rejected action targeted.
        instrument: InstrumentKey,
        /// The action's market-fact kind (carried verbatim for the consumer).
        kind: CorporateActionKind,
        /// Why the action could not be processed.
        reason: UnsupportedCorporateActionReason,
    },

    /// Observable, **non-mutating** signal that a
    /// [`CorporateAction`](crate::EngineEvent::CorporateAction) was skipped because its `id` had
    /// already been processed (the handler's idempotency guard fired).
    ///
    /// This is deliberately a **distinct** variant from
    /// [`EngineOutput::UnsupportedCorporateAction`], not a new reason under it: an idempotent skip
    /// is the *opposite* of a rejection on both axes. The `id` **was** recorded — by a **prior,
    /// successful** application — and the action is **not** retryable (re-submitting the same `id`
    /// will always hit this same guard). Reporting it as `UnsupportedCorporateAction` would falsely
    /// tell a consumer the action was not applied and remains retryable.
    ///
    /// Emitted so a consumer watching only the output/audit stream can distinguish "duplicate `id`,
    /// correctly ignored" from "a successful split on an instrument with nothing to adjust" — both
    /// otherwise produce no mutation observables.
    ///
    /// **`instrument` was not mutated.** A consumer that folds
    /// [`SplitRemainder`](Self::SplitRemainder) / [`PositionExit`](Self::PositionExit) /
    /// [`OptionPositionAdjustedForSplit`](Self::OptionPositionAdjustedForSplit) into a mutation
    /// tally must **not** count this variant.
    ///
    /// # Two emitters — read `instrument`, not the variant
    /// - The **target** guard: the action named `instrument` and its `id` was already applied
    ///   there. Nothing was mutated anywhere, and this is the only output of the event.
    /// - The **per-option** guard on a standard split: `instrument` is an *option* on the splitting
    ///   underlying that this same `id` had already adjusted, so its strike was not divided again.
    ///   The rest of the action still proceeds, so this arrives **alongside** the target's mutation
    ///   observables — one per suppressed option.
    CorporateActionAlreadyProcessed {
        /// The instrument that was skipped — the duplicate action's target, or an option on the
        /// splitting underlying this action had already adjusted (see above).
        instrument: InstrumentKey,
        /// The re-submitted action's market-fact kind (carried verbatim for the consumer). This is
        /// the kind of the duplicate event, which — should a caller violate the idempotency contract
        /// by reusing an `id` with a different `kind` — is not necessarily the originally-applied one.
        kind: CorporateActionKind,
        /// The already-processed action `id` that was skipped.
        id: SmolStr,
    },

    /// Observable, **non-mutating** signal that a market or account stream event named an exchange
    /// the engine was not built against, so it could not be routed and was dropped.
    ///
    /// Emitted in place of the panic this path used to take. The check is now performed on **every**
    /// market event rather than only while global connectivity is degraded, so a misconfigured
    /// exchange is reported on its first event instead of surfacing hours later on a reconnect —
    /// see [`UntrackedExchange`] for the full rationale and for exactly what was left untouched.
    ///
    /// **No state was mutated**, including
    /// [`ConnectivityStates::global`](crate::engine::state::connectivity::ConnectivityStates::global).
    /// A consumer folding
    /// observables into a mutation tally must not count this variant — the same treatment as
    /// [`CorporateActionAlreadyProcessed`](Self::CorporateActionAlreadyProcessed).
    ///
    /// Repeats per offending event, not once per exchange: the engine holds no "already reported"
    /// set for untracked venues, since by definition it has no state keyed on them. A consumer that
    /// wants one alert per venue must deduplicate on `exchange` itself.
    UntrackedExchange(UntrackedExchange),
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
    /// The target instrument is not the deliverable equity (e.g. an option, perpetual, future, or
    /// **CFD**). Equity-split arithmetic is invalid for these; option splits are handled
    /// separately, as part of processing the underlying equity's action.
    ///
    /// The eligibility rule is `InstrumentKind::is_split_eligible`; `Spot` is its only current
    /// spelling, but the rule is the deliverable equity, so judge a new `InstrumentKind` against
    /// the rule rather than against today's arm.
    InstrumentKindNotSupported,
    /// The corporate-action kind is not handled by this engine version (only stock splits are
    /// supported). Reachable once non-split kinds (dividends, spin-offs, …) are delivered.
    ActionKindNotSupported,
    /// A position the split would mutate is in an invalid state that pre-validation rejected
    /// **before any mutation** — specifically a **held option position with a non-integer contract
    /// count** facing a standard (whole-number) split. Option contracts are whole; a fractional
    /// count is state corruption, so the action is rejected atomically rather than silently floored
    /// (under `Floor`) or carried (under `Fractional`). **Not** self-healing on retry: the
    /// corrupted position must be reconciled before the action can succeed.
    PositionStateInvalid,
    /// The split arithmetic would overflow `Decimal` for at least one affected position — an
    /// extreme ratio or quantity driving a rescaled field past `Decimal::MAX`. Detected by the
    /// read-only pre-validation pass, so **no** position was mutated. Indicates a corrupt or
    /// adversarial feed rather than a transient condition.
    ArithmeticOverflow,
    /// The action's target is **not the unique split-eligible instrument** on its
    /// `(base, quote, exchange)` underlying identity — at least one other registered instrument is
    /// also split-eligible on the same identity.
    ///
    /// The option chain a split must adjust is resolved by that identity alone — every
    /// [`InstrumentState`](crate::engine::state::instrument::InstrumentState) whose underlying pair
    /// and exchange match the target's — so a second eligible instrument is an equally valid
    /// trigger for adjusting the *same* chain:
    /// nothing in the engine state can say which of the two the options are actually written on.
    /// Applying the split anyway would divide every strike on the chain once per trigger, silently
    /// for unheld options. The action is rejected before any mutation and the `id` is not recorded.
    ///
    /// **Not** self-healing on retry: the instrument set is fixed at construction, so resolving it
    /// means constructing the engine with one deliverable instrument per underlying identity — the
    /// registry is wrong, not the action.
    AmbiguousSplitTarget,
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
///
/// `#[non_exhaustive]`: matches [`EngineOutput`], which this feeds — engine-driven observables can
/// be added without breaking downstream exhaustive matches.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)] // PositionExit is rare; avoiding Box keeps API simple
#[non_exhaustive]
pub enum UpdateFromAccountOutput<OnDisconnect, InstrumentKey = InstrumentIndex> {
    None,
    OnDisconnect(OnDisconnect),
    PositionExit(PositionExited<AssetIndex, InstrumentKey>),

    /// The event named an exchange the engine does not track; nothing was updated, and
    /// `on_disconnect` was **not** called. See [`UntrackedExchange`].
    UntrackedExchange(UntrackedExchange),
}

/// Output produced by the [`Engine`] updating from an [`MarketStreamEvent`], used to construct
/// an `Engine` [`EngineAudit`].
///
/// `#[non_exhaustive]`: see [`UpdateFromAccountOutput`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum UpdateFromMarketOutput<OnDisconnect> {
    None,
    OnDisconnect(OnDisconnect),

    /// The event named an exchange the engine does not track; nothing was updated, and neither
    /// `on_disconnect` nor any instrument state was touched. See [`UntrackedExchange`].
    UntrackedExchange(UntrackedExchange),
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

#[cfg(test)]
mod size_guard {
    use super::*;

    /// Regression guard: `EngineOutput` stays small so the per-tick `ProcessAudit` copy is cheap. Its
    /// floor is the largest variant `PositionExit(PositionExited)` (~232 B). The `AlgoOrders`/
    /// `Commanded` variants are carried inline, but their order payloads are boxed at the root (#195)
    /// — `GenerateAlgoOrdersOutput` ~144 B, `ActionOutput` ~96 B — so both sit under the
    /// `PositionExit` floor and neither dominates. This bound (<= 256 B) catches a large regression —
    /// a new big inline variant, or a re-inlined order payload that lifts `AlgoOrders`/`Commanded`
    /// above the floor — while leaving headroom for unrelated growth.
    #[test]
    fn engine_output_stays_small() {
        // Measured 232 B, floored by the PositionExit variant.
        let size = std::mem::size_of::<EngineOutput<(), (), ExchangeIndex, InstrumentIndex>>();
        assert!(
            size <= 256,
            "EngineOutput grew to {size} B (expected <= 256): a variant gained a large inline \
             payload. AlgoOrders/Commanded order payloads must stay root-boxed (#195) so those \
             variants sit under the ~232 B PositionExit floor."
        );
    }
}
