use crate::engine::{
    Processor,
    state::{
        asset::{AssetStates, filter::AssetFilter},
        builder::EngineStateBuilder,
        connectivity::{ConnectivityStates, UntrackedExchange},
        instrument::{
            InstrumentStates, data::InstrumentDataState, filter::InstrumentFilter,
            generate_unindexed_instrument_account_snapshot,
        },
        position::{PositionDrift, PositionExited},
        trading::TradingState,
    },
};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rustrade_data::event::MarketEvent;
use rustrade_execution::{
    AccountEvent, AccountEventKind, AccountSnapshot, UnindexedAccountSnapshot,
    balance::AssetBalance, market::MarketSnapshot, order::id::ClientOrderId,
};
use rustrade_instrument::{
    Keyed,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex},
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
    /// If the `AccountEvent` results in a new [`PositionExited`], that is returned.
    ///
    /// This method:
    /// - Sets the account [`ConnectivityState`](connectivity::ConnectivityState) to
    ///   [`Health::Healthy`](connectivity::Health::Healthy) if it was not previously, unless the
    ///   event is [`ReinitFailed`](AccountEventKind::ReinitFailed) or
    ///   [`StreamTerminated`](AccountEventKind::StreamTerminated), which report on the account link
    ///   without showing it is up.
    /// - Updates the `GlobalData` with the `AccountEvent`.
    /// - Updates the associated `AssetStates` and `InstrumentStates` with the `AccountEvent`.
    pub fn update_from_account(
        &mut self,
        event: &AccountEvent,
    ) -> Option<PositionExited<AssetIndex>>
    where
        GlobalData: for<'a> Processor<&'a AccountEvent>,
        InstrumentData: for<'a> Processor<&'a AccountEvent>,
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
                None
            }
            AccountEventKind::BalanceSnapshot(balance) => {
                self.assets
                    .asset_index_mut(&balance.0.asset)
                    .update_from_balance(balance.as_ref());
                None
            }
            AccountEventKind::BalanceStreamUpdate(update) => {
                self.assets
                    .asset_index_mut(&update.0.asset)
                    .apply_balance_update(update.as_ref());
                None
            }
            AccountEventKind::OrderSnapshot(order) => {
                let instrument_state = self
                    .instruments
                    .instrument_index_mut(&order.value().key.instrument);

                // Propagate any PositionExited from deferred fill replay (fill-before-ack
                // race in OmsMode::Hedging). update_from_trade side effects (tearsheet,
                // position removal) are correct regardless; this ensures the output event
                // also reaches EngineOutput::PositionExit consumers.
                let exited = instrument_state.update_from_order_snapshot(order.as_ref());
                instrument_state.data.process(event);
                exited
            }
            AccountEventKind::OrderCancelled(response) => {
                let instrument_state = self
                    .instruments
                    .instrument_index_mut(&response.key.instrument);

                instrument_state.update_from_cancel_response(response);
                instrument_state.data.process(event);
                None
            }
            AccountEventKind::Trade(trade) => {
                let instrument_state = self.instruments.instrument_index_mut(&trade.instrument);

                instrument_state.data.process(event);
                instrument_state.update_from_trade(trade)
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
                None
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
                None
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
                None
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
                None
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
                None
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
                None
            }
            _ => None,
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

        assert_eq!(state.update_from_account(&event), None);
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
            assert_eq!(state.update_from_account(event), None);
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
        assert_eq!(state.update_from_account(&from_live_stream), None);
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

        assert_eq!(state.update_from_account(&event), None);
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
            assert_eq!(state.update_from_account(&event), None, "{kind}");
        }
        assert_eq!(state.assets, before.assets);
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
        assert_eq!(state.update_from_account(&flow(None)), None);
        assert_eq!(state.assets, before.assets);
        assert_eq!(state.instruments, before.instruments);

        assert_eq!(
            state.update_from_account(&flow(Some(InstrumentIndex::new(0)))),
            None
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
}
