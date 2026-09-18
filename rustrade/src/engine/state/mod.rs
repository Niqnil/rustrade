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
        position::PositionExited,
        trading::TradingState,
    },
};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rustrade_data::event::MarketEvent;
use rustrade_execution::{
    AccountEvent, AccountEventKind, UnindexedAccountSnapshot, balance::AssetBalance,
    market::MarketSnapshot,
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
use tracing::warn;

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
    /// Delegates to the instrument's own [`InstrumentDataState::market_snapshot`].
    ///
    /// # Panics
    /// Panics if `key` is not a tracked instrument, as
    /// [`InstrumentStates::instrument_index`] does. Every key reaching here came off an order the
    /// `Engine` generated from this same state, so an untracked one is a corrupted index rather
    /// than ordinary input.
    fn market_snapshot(&self, key: &InstrumentIndex) -> Option<MarketSnapshot> {
        Some(
            self.instruments
                .instrument_index(key)
                .data
                .market_snapshot(),
        )
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

    /// Updates the internal state from an `AccountEvent`.
    ///
    /// If the `AccountEvent` results in a new [`PositionExited`], that is returned.
    ///
    /// This method:
    /// - Sets the account [`ConnectivityState`](connectivity::ConnectivityState) to
    ///   [`Health::Healthy`](connectivity::Health::Healthy) if it was not previously.
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
        // Set exchange account connectivity to Healthy if it was Reconnecting
        self.connectivity.update_from_account_event(&event.exchange);

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
        let mut snapshots =
            FnvHashMap::with_capacity_and_hasher(connectivity.exchanges.len(), Default::default());

        // Insert UnindexedAccountSnapshot for each exchange that holds an account.
        //
        // `enumerate` deliberately runs *before* the role filter. `ExchangeIndex` is positional
        // into `connectivity.exchanges`, so numbering only the surviving venues would shift every
        // index past the first skipped one and silently attribute one exchange's instruments to
        // another.
        for (index, (exchange, state)) in connectivity.exchanges.iter().enumerate() {
            if !state.role.has_account() {
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
}
