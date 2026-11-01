#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! The `Engine` asks its strategy's `CashFlowAttributionStrategy` which position a cash flow on
//! the account as a whole belongs to, before the flow updates the state or reaches the audit.
//!
//! Each test pins one part of that contract: an answer sends the flow to that position's carry and
//! into the audit; `None` leaves it on the account; a flow the venue attributed, or any other
//! event, is never asked about; an answer naming no instrument tracked on the flow's exchange is
//! ignored; a replica fed the audit applies the flow as the `Engine` did; and `route_to_base`
//! answers only when exactly one held instrument on the exchange has the flow's asset as its base.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent, Sequence,
    engine::{
        Engine, EngineOutput, Processor,
        audit::{
            AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
        },
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        in_flight::InFlightDeadlines,
        state::{
            EngineState,
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            position::PositionSeed,
            trading::TradingState,
        },
    },
    execution::AccountStreamEvent,
    risk::DefaultRiskManager,
    strategy::{
        algo::AlgoStrategy,
        cash_flow_attribution::{CashFlowAttributionStrategy, route_to_base},
        close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy,
        on_trading_disabled::OnTradingDisabled,
    },
};
use rustrade_execution::{
    AccountEvent, AccountEventKind,
    balance::{AssetBalance, Balance},
    cash_flow::{CashFlow, CashFlowKind},
    order::request::{OrderRequestCancel, OrderRequestOpen},
};
use rustrade_instrument::{
    Side,
    asset::AssetIndex,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::InstrumentIndex,
    test_utils::instrument,
};
use rustrade_integration::collection::{none_one_or_many::NoneOneOrMany, snapshot::Snapshot};

const BINANCE: ExchangeIndex = ExchangeIndex(0);
const OKX: ExchangeIndex = ExchangeIndex(1);
const BTC_USDT: InstrumentIndex = InstrumentIndex(0);
const BTC_ETH: InstrumentIndex = InstrumentIndex(1);
const ETH_USDT: InstrumentIndex = InstrumentIndex(2);
/// BTC/USDT on a second exchange.
const OKX_BTC_USDT: InstrumentIndex = InstrumentIndex(3);

type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type TestEngine =
    Engine<HistoricalClock, State, MultiExchangeTxMap, Answers, DefaultRiskManager<State>>;

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn instruments() -> IndexedInstruments {
    IndexedInstruments::new([
        instrument(ExchangeId::BinanceSpot, "btc", "usdt"),
        instrument(ExchangeId::BinanceSpot, "btc", "eth"),
        instrument(ExchangeId::BinanceSpot, "eth", "usdt"),
        instrument(ExchangeId::Okx, "btc", "usdt"),
    ])
}

/// A short of 1 base unit at 100 in each of `held`.
fn state(held: &[InstrumentIndex]) -> State {
    let instruments = instruments();
    let seeds = held
        .iter()
        .map(|key| {
            let name = instruments.instruments()[key.index()]
                .value
                .name_internal
                .clone();
            PositionSeed::new(name, Side::Sell, dec!(1), dec!(100), at(0))
        })
        .collect::<Vec<_>>();
    EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(at(0))
    .trading_state(TradingState::Disabled)
    .positions(seeds)
    .build()
}

fn engine(held: &[InstrumentIndex], answer: Option<InstrumentIndex>) -> TestEngine {
    Engine::new(
        HistoricalClock::new(at(0)),
        state(held),
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, None), (ExchangeId::Okx, None)]),
        InFlightDeadlines::default(),
        Answers {
            answer,
            asked: Vec::new(),
        },
        DefaultRiskManager::default(),
    )
}

/// The flow `amount` of `asset` (an index into the `IndexedInstruments` assets), naming
/// `instrument`.
fn flow(
    asset: AssetIndex,
    amount: Decimal,
    instrument: Option<InstrumentIndex>,
) -> CashFlow<AssetIndex, InstrumentIndex> {
    CashFlow::new(
        CashFlowKind::MarginInterest {
            rate: None,
            principal: None,
            timing: None,
        },
        asset,
        amount,
        instrument,
        at(3600),
        None,
    )
}

/// The base asset of `instrument`, as indexed for its exchange.
fn base(state: &State, instrument: InstrumentIndex) -> AssetIndex {
    state
        .instruments
        .instrument_index(&instrument)
        .instrument
        .underlying
        .base
}

/// The quote asset of `instrument`, as indexed for its exchange.
fn quote(state: &State, instrument: InstrumentIndex) -> AssetIndex {
    state
        .instruments
        .instrument_index(&instrument)
        .instrument
        .underlying
        .quote
}

fn account(
    exchange: ExchangeIndex,
    kind: AccountEventKind<ExchangeIndex, AssetIndex, InstrumentIndex>,
) -> EngineEvent {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent { exchange, kind }))
}

/// `instrument` the flow in the audited event names, panicking if the audit records no flow.
fn audited_instrument(audit: &EngineAudit<EngineEvent, Output>) -> Option<InstrumentIndex> {
    match audit {
        EngineAudit::Process(process) => match &process.event {
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                kind: AccountEventKind::CashFlow(flow),
                ..
            })) => flow.instrument,
            other => panic!("audited a non cash flow event: {other:?}"),
        },
        other => panic!("not a process audit: {other:?}"),
    }
}

/// The base asset carry of the position held in `instrument`, checking no other carry accrued.
fn carry_base(state: &State, instrument: InstrumentIndex) -> Decimal {
    let position = state
        .instruments
        .instrument_index(&instrument)
        .position
        .positions
        .values()
        .next()
        .expect("position held");
    assert_eq!(position.carry, Decimal::ZERO);
    assert!(position.carry_assets.other.is_empty());
    position.carry_assets.base
}

#[test]
fn an_answer_sends_the_flow_to_that_positions_carry_and_into_the_audit() {
    let mut engine = engine(&[BTC_USDT], Some(BTC_USDT));
    let btc = base(&engine.state, BTC_USDT);
    let interest = flow(btc, dec!(-0.0001), None);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(interest.clone()),
    ));

    assert_eq!(engine.strategy.asked, vec![(BINANCE, interest)]);
    assert_eq!(audited_instrument(&audit), Some(BTC_USDT));
    assert_eq!(carry_base(&engine.state, BTC_USDT), dec!(-0.0001));
}

#[test]
fn no_answer_leaves_the_flow_on_the_account() {
    let mut engine = engine(&[BTC_USDT], None);
    let btc = base(&engine.state, BTC_USDT);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), None)),
    ));

    assert_eq!(engine.strategy.asked.len(), 1);
    assert_eq!(audited_instrument(&audit), None);
    assert_eq!(carry_base(&engine.state, BTC_USDT), Decimal::ZERO);
}

#[test]
fn a_flow_the_venue_attributed_and_any_other_event_are_never_asked_about() {
    // An answer that would move the flow elsewhere, to show it is not consulted.
    let mut engine = engine(&[BTC_USDT, OKX_BTC_USDT], Some(OKX_BTC_USDT));
    let btc = base(&engine.state, BTC_USDT);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), Some(BTC_USDT))),
    ));
    engine.process(account(
        BINANCE,
        AccountEventKind::BalanceSnapshot(Snapshot::new(AssetBalance::new(
            btc,
            Balance::new(dec!(1), dec!(1)),
            at(3600),
        ))),
    ));

    assert_eq!(engine.strategy.asked, vec![]);
    assert_eq!(audited_instrument(&audit), Some(BTC_USDT));
    assert_eq!(carry_base(&engine.state, BTC_USDT), dec!(-0.0001));
    assert_eq!(carry_base(&engine.state, OKX_BTC_USDT), Decimal::ZERO);
}

#[test]
fn an_answer_naming_an_instrument_on_another_exchange_is_ignored() {
    let mut engine = engine(&[BTC_USDT, OKX_BTC_USDT], Some(OKX_BTC_USDT));
    let btc = base(&engine.state, BTC_USDT);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), None)),
    ));

    assert_eq!(engine.strategy.asked.len(), 1);
    assert_eq!(audited_instrument(&audit), None);
    assert_eq!(carry_base(&engine.state, BTC_USDT), Decimal::ZERO);
    assert_eq!(carry_base(&engine.state, OKX_BTC_USDT), Decimal::ZERO);
}

#[test]
fn an_answer_naming_no_tracked_instrument_is_ignored() {
    let mut engine = engine(&[BTC_USDT], Some(InstrumentIndex(99)));
    let btc = base(&engine.state, BTC_USDT);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), None)),
    ));

    assert_eq!(audited_instrument(&audit), None);
    assert_eq!(carry_base(&engine.state, BTC_USDT), Decimal::ZERO);
}

#[test]
fn an_answer_naming_an_instrument_holding_no_position_names_it_but_changes_no_carry() {
    let mut engine = engine(&[], Some(BTC_USDT));
    let btc = base(&engine.state, BTC_USDT);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), None)),
    ));

    assert_eq!(audited_instrument(&audit), Some(BTC_USDT));
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&BTC_USDT)
            .position
            .positions
            .is_empty()
    );
}

#[test]
fn a_replica_fed_the_audit_applies_the_attributed_flow_as_the_engine_did() {
    let mut engine = engine(&[BTC_USDT], Some(BTC_USDT));
    let btc = base(&engine.state, BTC_USDT);
    let seed = AuditTick {
        event: engine.state.clone(),
        context: EngineContext {
            time: at(0),
            sequence: Sequence(0),
        },
    };
    let updates: std::iter::Empty<AuditTick<EngineAudit<EngineEvent, Output>, EngineContext>> =
        std::iter::empty();
    let mut replica = StateReplicaManager::new(seed, updates);

    let audit = engine.process(account(
        BINANCE,
        AccountEventKind::CashFlow(flow(btc, dec!(-0.0001), None)),
    ));
    let EngineAudit::Process(process) = audit else {
        panic!("not a process audit");
    };
    replica.update_from_event(process.event, &NoneOneOrMany::<Output>::None);

    assert_eq!(
        carry_base(replica.replica_engine_state(), BTC_USDT),
        dec!(-0.0001)
    );
}

#[test]
fn route_to_base_names_the_only_held_instrument_on_the_exchange_based_in_the_flows_asset() {
    // BTC/ETH is not held.
    let held = state(&[BTC_USDT, ETH_USDT, OKX_BTC_USDT]);
    let btc = flow(base(&held, BTC_USDT), dec!(-0.0001), None);
    let eth = flow(base(&held, ETH_USDT), dec!(-0.0001), None);
    let okx_btc = flow(base(&held, OKX_BTC_USDT), dec!(-0.0001), None);

    assert_eq!(route_to_base(&held, BINANCE, &btc), Some(BTC_USDT));
    assert_eq!(route_to_base(&held, BINANCE, &eth), Some(ETH_USDT));
    assert_eq!(route_to_base(&held, OKX, &okx_btc), Some(OKX_BTC_USDT));
}

#[test]
fn route_to_base_names_nothing_when_no_held_instrument_or_several_qualify() {
    let none_held = state(&[]);
    let both_held = state(&[BTC_USDT, BTC_ETH]);
    let usdt_held = state(&[BTC_USDT]);
    let okx_held = state(&[OKX_BTC_USDT]);
    let btc = flow(base(&none_held, BTC_USDT), dec!(-0.0001), None);
    let usdt = flow(quote(&usdt_held, BTC_USDT), dec!(-0.0001), None);
    let okx_btc = flow(base(&okx_held, OKX_BTC_USDT), dec!(-0.0001), None);

    assert_eq!(route_to_base(&none_held, BINANCE, &btc), None);
    assert_eq!(route_to_base(&both_held, BINANCE, &btc), None);
    // No pair is based in the quote asset.
    assert_eq!(route_to_base(&usdt_held, BINANCE, &usdt), None);
    // The only instrument based in the asset is on another exchange.
    assert_eq!(route_to_base(&okx_held, BINANCE, &okx_btc), None);
}

/// Returns `answer` to every question, and records each.
#[derive(Debug)]
struct Answers {
    answer: Option<InstrumentIndex>,
    asked: Vec<(ExchangeIndex, CashFlow<AssetIndex, InstrumentIndex>)>,
}

impl CashFlowAttributionStrategy for Answers {
    type State = State;

    fn attribute_cash_flow(
        &mut self,
        _state: &Self::State,
        exchange: ExchangeIndex,
        flow: &CashFlow<AssetIndex, InstrumentIndex>,
    ) -> Option<InstrumentIndex> {
        self.asked.push((exchange, flow.clone()));
        self.answer
    }
}

impl AlgoStrategy for Answers {
    type State = State;

    fn generate_algo_orders(
        &self,
        _state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        (std::iter::empty(), std::iter::empty())
    }
}

impl ClosePositionsStrategy for Answers {
    type State = State;

    fn close_positions_requests<'a>(
        &'a self,
        _state: &'a Self::State,
        _filter: &'a InstrumentFilter,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        (std::iter::empty(), std::iter::empty())
    }
}

type Output = EngineOutput<(), ()>;

impl OnDisconnectStrategy<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for Answers
{
    type OnDisconnect = ();

    fn on_disconnect(_: &mut TestEngine, _: ExchangeId) -> Self::OnDisconnect {}
}

impl OnTradingDisabled<HistoricalClock, State, MultiExchangeTxMap, DefaultRiskManager<State>>
    for Answers
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(_: &mut TestEngine) -> Self::OnTradingDisabled {}
}
