#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! A backtest holds a long option on a simulated venue, and the venue settles it at expiry.
//!
//! The engine runs `ExpiryHandling::Venue` for the exchange, so it synthesises nothing at expiry:
//! the venue's own settlement trades, delivered like any fill, are what close the option and, for
//! a physically settled one, open the underlying. These runs are the first callers of the whole
//! chain outside a unit test: an option instrument through the execution builder's projection,
//! the mock execution client, `SimRunner` moving the venue's clock past the expiry, the
//! account-event indexer, and the engine's positions.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent,
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::NoAuxEvents, backtest,
        market_data::MarketDataInMemory, summary::BacktestResult,
    },
    engine::{
        Engine,
        clock::HistoricalClock,
        in_flight::InFlightDeadlines,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{
                ExpiryHandling,
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            position::PositionSeed,
            trading::TradingState,
        },
    },
    execution::{
        request::ExecutionRequest,
        sim::{
            SimEngine, SimExecutionBuild, SimExecutionBuilder, SimStart, SimStepper,
            SimVenueOptions, StartBalance,
        },
    },
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::{
        DefaultStrategy, algo::AlgoStrategy, cash_flow_attribution::CashFlowAttributionStrategy,
        close_positions::ClosePositionsStrategy, on_disconnect::OnDisconnectStrategy,
        on_trading_disabled::OnTradingDisabled,
    },
    system::config::ExecutionConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountSnapshot,
    balance::{AssetBalance, Balance},
    cash_flow::CashFlow,
    client::mock::MockExecutionConfig,
    fee::FeeModelConfig,
    fill::SimFillConfig,
    order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
    },
};
use rustrade_instrument::{
    Side, Underlying,
    asset::{AssetIndex, ExchangeAsset, name::AssetNameExchange, name::AssetNameInternal},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::{
            InstrumentKind,
            option::{OptionContract, OptionExercise, OptionKind, OptionSettlement},
        },
        quote::InstrumentQuoteAsset,
    },
};

const EXCHANGE: ExchangeId = ExchangeId::Simulated;

/// The call's internal name, as the engine and a `SimStart` name it.
const CALL: &str = "simulated_spy_c100";

/// What the account starts with, in USD: enough for the premium and a delivery at the strike.
const FUNDING: Decimal = dec!(20_000);

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

fn expiry() -> DateTime<Utc> {
    ts("2025-03-25T20:00:00Z")
}

fn funded(asset: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: AssetNameExchange::new(asset),
        balance: Balance::new(amount, amount),
        time_exchange: ts("2025-03-24T22:00:00Z"),
    }
}

/// A venue opening with `balances`.
fn mock_config(balances: Vec<AssetBalance<AssetNameExchange>>) -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances,
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model: FeeModelConfig::default(),
        fill_model: SimFillConfig::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: None,
        fee_asset: None,
        long_option_expiry: Default::default(),
        short_put_collateral: Default::default(),
        amend: Default::default(),
    }
}

type BacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type BacktestTxMap = rustrade::engine::execution_tx::MultiExchangeTxMap<
    rustrade_integration::channel::UnboundedTx<ExecutionRequest>,
>;

/// Buys one option contract at the first option price it sees, or sells one when `sell`, and
/// does nothing else.
#[derive(Debug, Default)]
struct BuyOneOption {
    sell: bool,
    sent: AtomicUsize,
}

impl AlgoStrategy for BuyOneOption {
    type State = BacktestState;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let Some(option) = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .find(|instrument| matches!(instrument.instrument.kind, InstrumentKind::Option(_)))
        else {
            return (std::iter::empty(), None);
        };
        if option.data.price().is_none() || self.sent.fetch_add(1, Ordering::Relaxed) > 0 {
            return (std::iter::empty(), None);
        }

        let open = OrderRequestOpen {
            key: OrderKey {
                exchange: option.instrument.exchange,
                instrument: option.key,
                strategy: StrategyId::new("buy-one-option"),
                cid: ClientOrderId::random(),
            },
            state: RequestOpen::new(
                if self.sell { Side::Sell } else { Side::Buy },
                None,
                dec!(1),
                OrderKind::Market,
                TimeInForce::ImmediateOrCancel,
            ),
        };
        (std::iter::empty(), Some(open))
    }
}

impl ClosePositionsStrategy for BuyOneOption {
    type State = BacktestState;

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

impl CashFlowAttributionStrategy for BuyOneOption {
    type State = BacktestState;

    fn attribute_cash_flow(
        &mut self,
        _: &Self::State,
        _: ExchangeIndex,
        _: &CashFlow<AssetIndex, InstrumentIndex>,
    ) -> Option<InstrumentIndex> {
        None
    }
}

impl
    OnDisconnectStrategy<
        HistoricalClock,
        BacktestState,
        BacktestTxMap,
        DefaultRiskManager<BacktestState>,
    > for BuyOneOption
{
    type OnDisconnect = ();

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            BacktestState,
            BacktestTxMap,
            Self,
            DefaultRiskManager<BacktestState>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
    }
}

impl
    OnTradingDisabled<
        HistoricalClock,
        BacktestState,
        BacktestTxMap,
        DefaultRiskManager<BacktestState>,
    > for BuyOneOption
{
    type OnTradingDisabled = ();

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            BacktestState,
            BacktestTxMap,
            Self,
            DefaultRiskManager<BacktestState>,
        >,
    ) -> Self::OnTradingDisabled {
    }
}

/// SPY and a 100-share SPY call struck at 100, expiring at [`expiry`], settled as `settlement`.
fn instruments(settlement: OptionSettlement) -> IndexedInstruments {
    instruments_of(OptionKind::Call, settlement)
}

/// SPY and a 100-share SPY option of `kind` struck at 100, expiring at [`expiry`], settled as
/// `settlement`. Named as [`instruments`]'s call is, whatever its kind.
fn instruments_of(kind: OptionKind, settlement: OptionSettlement) -> IndexedInstruments {
    let spy = Instrument::spot(
        EXCHANGE,
        "simulated_spy_usd",
        "SPY",
        Underlying::new("spy", "usd"),
        None,
    );
    let call = Instrument::new(
        EXCHANGE,
        CALL,
        "SPY_C100",
        Underlying::new("spy", "usd"),
        InstrumentQuoteAsset::UnderlyingQuote,
        InstrumentKind::Option(OptionContract {
            contract_size: dec!(100),
            settlement_asset: "usd".into(),
            kind,
            exercise: OptionExercise::American,
            settlement,
            expiry: expiry(),
            strike: dec!(100),
        }),
        None,
    );
    IndexedInstruments::new([spy, call])
}

/// The key of the instrument named `name_exchange` at the exchange.
fn key(instruments: &IndexedInstruments, name_exchange: &str) -> InstrumentIndex {
    instruments
        .instruments()
        .iter()
        .find(|keyed| keyed.value.name_exchange.as_ref() == name_exchange)
        .unwrap()
        .key
}

/// A public trade on `instrument` at `price`, at `time`.
fn trade(
    time: &str,
    instrument: InstrumentIndex,
    price: Decimal,
) -> MarketStreamEvent<InstrumentIndex, DataKind> {
    let time = ts(time);
    MarketStreamEvent::Item(MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: EXCHANGE,
        instrument,
        kind: DataKind::Trade(PublicTrade {
            id: "t".into(),
            price,
            amount: dec!(1),
            side: None,
        }),
    })
}

/// A run holding SPY and the call of [`instruments`], settled as `settlement`.
///
/// SPY trades at 100 and the call at 1.50 on the first evening, when the strategy buys one call
/// for 150. SPY trades at 110 an hour before the call expires at 20:00, and again an hour after,
/// so the run's clock passes the expiry with the underlying priced at 110.
async fn run(settlement: OptionSettlement) -> BacktestResult<Daily, BacktestState> {
    run_with(OptionKind::Call, settlement, false, dec!(110)).await
}

/// A run as [`run`]'s, of an option of `kind`, which the strategy sells when `sell`, with SPY at
/// `spy_at_expiry` around the expiry.
async fn run_with(
    kind: OptionKind,
    settlement: OptionSettlement,
    sell: bool,
    spy_at_expiry: Decimal,
) -> BacktestResult<Daily, BacktestState> {
    let instruments = instruments_of(kind, settlement);
    let (spy, call) = (key(&instruments, "SPY"), key(&instruments, "SPY_C100"));

    let market_events = vec![
        trade("2025-03-24T22:00:00Z", spy, dec!(100)),
        trade("2025-03-24T22:01:00Z", call, dec!(1.5)),
        trade("2025-03-25T19:00:00Z", spy, spy_at_expiry),
        trade("2025-03-25T21:00:00Z", spy, spy_at_expiry),
    ];

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .trading_state(TradingState::Enabled)
    .expiry_handling(EXCHANGE, ExpiryHandling::Venue)
    .build();

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        venue_options: FnvHashMap::default(),
        executions: vec![ExecutionConfig::Mock(mock_config(vec![
            funded("usd", FUNDING),
            funded("spy", Decimal::ZERO),
        ]))],
        market_data: MarketDataInMemory::new(Arc::new(market_events)),
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    backtest(
        args_constant,
        BacktestArgsDynamic {
            id: "option-expiry".into(),
            risk_free_return: Decimal::ZERO,
            strategy: BuyOneOption {
                sell,
                ..BuyOneOption::default()
            },
            risk: DefaultRiskManager::default(),
        },
    )
    .await
    .expect("an option backtest must complete")
}

fn balance_end(result: &BacktestResult<Daily, BacktestState>, asset: &str) -> Balance {
    result
        .summary
        .trading_summary
        .assets
        .get(&ExchangeAsset::<AssetNameInternal>::new(EXCHANGE, asset))
        .expect("a funded asset is summarised")
        .balance_end
        .expect("a funded asset has a closing balance")
}

/// Each open position the engine ends with, as (exchange name, signed quantity, entry price).
fn open_positions(
    result: &BacktestResult<Daily, BacktestState>,
) -> Vec<(String, Decimal, Decimal)> {
    result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| {
            instrument.position.positions.values().map(|position| {
                let signed = match position.side {
                    Side::Buy => position.quantity_abs,
                    Side::Sell => -position.quantity_abs,
                };
                (
                    instrument.instrument.name_exchange.to_string(),
                    signed,
                    position.price_entry_average,
                )
            })
        })
        .collect()
}

/// The option's realised PnL on the tear sheet.
fn option_pnl(result: &BacktestResult<Daily, BacktestState>) -> Decimal {
    result
        .summary
        .trading_summary
        .instruments
        .iter()
        .find(|(name, _)| name.to_string() == CALL)
        .expect("the option traded, so it has a tear sheet")
        .1
        .pnl
}

/// The call is 10 in the money at expiry, so the venue exercises it: it closes the option at 0
/// and buys 100 SPY at the strike. The engine ends holding exactly that, the option's PnL is the
/// premium lost, and the balances are the premium and the strike paid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_exercised_call_ends_holding_the_underlying_bought_at_the_strike() {
    let result = run(OptionSettlement::Physical).await;

    assert_eq!(
        open_positions(&result),
        [("SPY".to_string(), dec!(100), dec!(100))]
    );
    assert_eq!(option_pnl(&result), dec!(-150));
    assert_eq!(
        balance_end(&result, "usd").total,
        FUNDING - dec!(150) - dec!(10_000)
    );
    assert_eq!(balance_end(&result, "spy").total, dec!(100));
}

/// Settled in cash, the same call pays its intrinsic value, 10 × 100, and the run ends flat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_cash_settled_call_ends_flat_with_its_intrinsic_value_paid() {
    let result = run(OptionSettlement::Cash).await;

    assert_eq!(open_positions(&result), []);
    assert_eq!(option_pnl(&result), dec!(1_000) - dec!(150));
    assert_eq!(
        balance_end(&result, "usd").total,
        FUNDING - dec!(150) + dec!(1_000)
    );
    assert_eq!(balance_end(&result, "spy").total, Decimal::ZERO);
}

/// A stepped session starting from a `SimStart` that holds one long call, bought at 1.50 before
/// the start. The venue and the `Engine` both hold it until the venue settles it at expiry, in
/// cash at its intrinsic value of 10, and the `Engine` closes its position on the venue's trade.
#[test]
fn a_start_holding_a_long_call_holds_it_on_both_sides_until_the_venue_settles_it() {
    type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

    let instruments = instruments(OptionSettlement::Cash);
    let (spy, call) = (key(&instruments, "SPY"), key(&instruments, "SPY_C100"));
    let start_time = ts("2025-03-24T22:00:00Z");
    let start = SimStart::new(
        vec![StartBalance::new(AssetNameExchange::new("usd"), FUNDING)],
        vec![PositionSeed::new(
            CALL,
            Side::Buy,
            dec!(1),
            dec!(1.5),
            start_time,
        )],
    );
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(mock_config(vec![]), SimVenueOptions::default(), start)
        .unwrap()
        .build();
    let seeds = build.position_seeds();
    let SimExecutionBuild {
        execution_tx_map,
        venues,
    } = build;

    let clock = HistoricalClock::new(start_time);
    let state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(start_time)
    .trading_state(TradingState::Enabled)
    .expiry_handling(EXCHANGE, ExpiryHandling::Venue)
    .positions(seeds)
    .build();
    let mut engine: SimEngine<
        DefaultGlobalData,
        DefaultInstrumentMarketData,
        DefaultStrategy<State>,
        DefaultRiskManager<State>,
    > = Engine::new(
        clock.clone(),
        state,
        execution_tx_map,
        InFlightDeadlines::default(),
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );
    let mut stepper = SimStepper::new(venues, clock).unwrap();

    let held = |engine: &SimEngine<_, _, _, _>| {
        engine
            .state
            .instruments
            .instrument_index(&call)
            .position
            .positions
            .values()
            .map(|position| {
                (
                    position.side,
                    position.quantity_abs,
                    position.price_entry_average,
                )
            })
            .collect::<Vec<_>>()
    };
    let usd = |engine: &SimEngine<_, _, _, _>| {
        engine
            .state
            .assets
            .assets()
            .find(|state| state.asset.name_internal.as_ref() == "usd")
            .and_then(|state| state.balance.as_ref())
            .map(|balance| balance.value.total)
    };

    let before = "2025-03-25T19:00:00Z";
    stepper
        .step(
            &mut engine,
            ts(before),
            [EngineEvent::Market(trade(before, spy, dec!(110)))],
        )
        .unwrap();
    assert_eq!(held(&engine), [(Side::Buy, dec!(1), dec!(1.5))]);

    let after = "2025-03-25T21:00:00Z";
    stepper
        .step(
            &mut engine,
            ts(after),
            [EngineEvent::Market(trade(after, spy, dec!(110)))],
        )
        .unwrap();
    assert_eq!(held(&engine), []);
    assert_eq!(usd(&engine), Some(FUNDING + dec!(1_000)));
}

/// A cash-secured put, sold for 1.50, is 10 in the money at expiry, so the venue assigns it: it
/// closes the option at 0 and buys 100 SPY at the strike with the cash the put held. The engine
/// ends holding the shares, and the option's PnL is the premium kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_assigned_short_put_ends_holding_the_shares_bought_at_the_strike() {
    let result = run_with(OptionKind::Put, OptionSettlement::Physical, true, dec!(90)).await;

    assert_eq!(
        open_positions(&result),
        [("SPY".to_string(), dec!(100), dec!(100))]
    );
    assert_eq!(option_pnl(&result), dec!(150));
    let usd = balance_end(&result, "usd");
    assert_eq!(
        (usd.total, usd.free),
        (
            FUNDING + dec!(150) - dec!(10_000),
            FUNDING + dec!(150) - dec!(10_000)
        )
    );
    assert_eq!(balance_end(&result, "spy").total, dec!(100));
}

/// Out of the money at expiry, the same put expires: the run ends flat, keeping the premium, and
/// the cash it held is free again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_short_put_expiring_out_of_the_money_keeps_the_premium() {
    let result = run_with(OptionKind::Put, OptionSettlement::Physical, true, dec!(110)).await;

    assert_eq!(open_positions(&result), []);
    assert_eq!(option_pnl(&result), dec!(150));
    let usd = balance_end(&result, "usd");
    assert_eq!(
        (usd.total, usd.free),
        (FUNDING + dec!(150), FUNDING + dec!(150))
    );
}

/// A stepped session starting from a `SimStart` that holds 100 SPY and one call written against
/// them. The call is 10 in the money at expiry, so the venue assigns it and delivers the shares at
/// the strike: the `Engine` closes both positions on the venue's trades, and the account holds the
/// strike's cash.
#[test]
fn a_start_holding_a_covered_call_ends_flat_once_the_shares_are_delivered() {
    type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

    let instruments = instruments(OptionSettlement::Physical);
    let spy = key(&instruments, "SPY");
    let start_time = ts("2025-03-24T22:00:00Z");
    let start = SimStart::new(
        vec![StartBalance::new(AssetNameExchange::new("usd"), FUNDING)],
        vec![
            PositionSeed::new(
                "simulated_spy_usd",
                Side::Buy,
                dec!(100),
                dec!(95),
                start_time,
            ),
            PositionSeed::new(CALL, Side::Sell, dec!(1), dec!(1.5), start_time),
        ],
    );
    let build = SimExecutionBuilder::new(&instruments)
        .add_venue_with_start(mock_config(vec![]), SimVenueOptions::default(), start)
        .unwrap()
        .build();
    let seeds = build.position_seeds();
    let SimExecutionBuild {
        execution_tx_map,
        venues,
    } = build;

    let clock = HistoricalClock::new(start_time);
    let state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(start_time)
    .trading_state(TradingState::Enabled)
    .expiry_handling(EXCHANGE, ExpiryHandling::Venue)
    .positions(seeds)
    .build();
    let mut engine: SimEngine<
        DefaultGlobalData,
        DefaultInstrumentMarketData,
        DefaultStrategy<State>,
        DefaultRiskManager<State>,
    > = Engine::new(
        clock.clone(),
        state,
        execution_tx_map,
        InFlightDeadlines::default(),
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );
    let mut stepper = SimStepper::new(venues, clock).unwrap();

    let open = |engine: &SimEngine<_, _, _, _>| {
        engine
            .state
            .instruments
            .instruments(&InstrumentFilter::None)
            .map(|instrument| instrument.position.positions.len())
            .sum::<usize>()
    };
    let asset = |engine: &SimEngine<_, _, _, _>, name: &str| {
        engine
            .state
            .assets
            .assets()
            .find(|state| state.asset.name_internal.as_ref() == name)
            .and_then(|state| state.balance.as_ref())
            .map(|balance| balance.value.total)
    };

    let before = "2025-03-25T19:00:00Z";
    stepper
        .step(
            &mut engine,
            ts(before),
            [EngineEvent::Market(trade(before, spy, dec!(110)))],
        )
        .unwrap();
    assert_eq!(open(&engine), 2, "the shares and the call");

    let after = "2025-03-25T21:00:00Z";
    stepper
        .step(
            &mut engine,
            ts(after),
            [EngineEvent::Market(trade(after, spy, dec!(110)))],
        )
        .unwrap();
    assert_eq!(open(&engine), 0);
    assert_eq!(asset(&engine, "usd"), Some(FUNDING + dec!(10_000)));
    assert_eq!(asset(&engine, "spy"), Some(Decimal::ZERO));
}
