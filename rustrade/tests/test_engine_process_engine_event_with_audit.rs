#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent, Sequence, Timed,
    engine::{
        Engine, EngineOutput, Processor, UnsupportedCorporateActionReason,
        action::{
            ActionOutput,
            generate_algo_orders::GenerateAlgoOrdersOutput,
            send_requests::{SendCancelsAndOpensOutput, SendRequestsOutput},
        },
        audit::{AuditTick, EngineAudit},
        clock::HistoricalClock,
        command::Command,
        execution_tx::MultiExchangeTxMap,
        process_with_audit,
        state::{
            EngineState,
            asset::AssetStates,
            connectivity::{ConnectivityDimension, Health, UntrackedExchange},
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            position::{OmsMode, PositionExited, SplitRoundingPolicy},
            trading::TradingState,
        },
    },
    execution::{AccountStreamEvent, request::ExecutionRequest},
    risk::DefaultRiskManager,
    statistic::time::Annual365,
    strategy::{
        algo::AlgoStrategy,
        close_positions::{ClosePositionsStrategy, close_open_positions_with_market_orders},
        on_disconnect::OnDisconnectStrategy,
        on_trading_disabled::OnTradingDisabled,
    },
    test_utils::time_plus_days,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountEvent, AccountEventKind, AccountSnapshot, FeeModelConfig, PerContractFeeModel,
    balance::{AssetBalance, AssetBalanceUpdate, Balance, BalanceUpdate},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, PositionId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, RequestOpen},
        state::{ActiveOrderState, Cancelled, Filled, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use rustrade_instrument::{
    Side, Underlying,
    asset::AssetIndex,
    corporate_action::{CorporateActionKind, SplitRatio},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::{
            InstrumentKind,
            option::{OptionContract, OptionExercise, OptionKind},
        },
        spec::{
            InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            OrderQuantityUnits,
        },
    },
};
use rustrade_integration::{
    channel::{UnboundedTx, mpsc_unbounded},
    collection::{none_one_or_many::NoneOneOrMany, one_or_many::OneOrMany, snapshot::Snapshot},
};

const STARTING_TIMESTAMP: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;
const RISK_FREE_RETURN: Decimal = dec!(0.05);
const STARTING_BALANCE_USDT: Balance = Balance::new(dec!(40_000.0), dec!(40_000.0));
const STARTING_BALANCE_BTC: Balance = Balance::new(dec!(1.0), dec!(1.0));
const STARTING_BALANCE_ETH: Balance = Balance::new(dec!(10.0), dec!(10.0));
const QUOTE_FEES_PERCENT: f64 = 0.1; // 10%

// Asset indices after alphabetical sorting: btc(0), eth(1), usdt(2)
// For BTCUSDT (instrument 0): quote = usdt = AssetIndex(2)
// For ETHBTC (instrument 1): quote = btc = AssetIndex(0)
fn quote_asset_index(instrument: usize) -> AssetIndex {
    match instrument {
        0 => AssetIndex(2), // BTCUSDT → usdt
        1 => AssetIndex(0), // ETHBTC → btc
        other => panic!(
            "quote_asset_index: unknown instrument index {other}; update test setup if a new instrument is added"
        ),
    }
}

/// Create AssetFees with proper AssetIndex (simulates quote-denominated fees)
fn asset_fees(instrument: usize, amount: Decimal) -> AssetFees<AssetIndex> {
    AssetFees::new(quote_asset_index(instrument), amount, Some(amount))
}

// Type alias to avoid clippy::type_complexity warnings in test helper functions
type TestEngine = Engine<
    HistoricalClock,
    EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
    TestBuyAndHoldStrategy,
    DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
>;

// Empty audit-update iterator for `StateReplicaManager::new` in the parity tests, aliased so the
// deeply-nested type (and its `clippy::type_complexity` allow) isn't repeated at each call site.
type DummyAuditUpdates = std::iter::Empty<
    AuditTick<
        EngineAudit<
            EngineEvent<DataKind>,
            EngineOutput<OnTradingDisabledOutput, OnDisconnectOutput>,
        >,
    >,
>;

#[test]
fn test_engine_process_engine_event_with_audit() {
    let (execution_tx, mut execution_rx) = mpsc_unbounded();

    let mut engine = build_engine(TradingState::Disabled, execution_tx);
    assert_eq!(engine.meta.sequence, Sequence(0));
    assert_eq!(engine.state.connectivity.global, Health::Reconnecting);

    // Simulate AccountSnapshot from ExecutionManager::init
    let event = account_event_snapshot(&engine.state.assets);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(0));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(engine.state.connectivity.global, Health::Reconnecting);

    // Process 1st MarketEvent for btc_usdt
    let event = market_event_trade(1, 0, dec!(10_000));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(1));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(engine.state.connectivity.global, Health::Healthy);

    // Process 1st MarketEvent for eth_btc
    let event = market_event_trade(1, 1, dec!(0.1));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(2));
    assert_eq!(audit.event, EngineAudit::process(event));

    // TradingState::Enabled -> expect BuyAndHoldStrategy to open Buy orders
    let event = EngineEvent::TradingStateUpdate(TradingState::Enabled);
    let audit = process_with_audit(&mut engine, event);
    assert_eq!(audit.context.sequence, Sequence(3));
    let btc_usdt_buy_order = OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            cid: gen_cid(0),
        },
        state: RequestOpen {
            side: Side::Buy,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            price: None,
            quantity: dec!(1),
            position_id: None,
            reduce_only: false,
        },
    };
    let eth_btc_buy_order = OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(1),
            strategy: strategy_id(),
            cid: gen_cid(1),
        },
        state: RequestOpen {
            side: Side::Buy,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            price: None,
            quantity: dec!(1),
            position_id: None,
            reduce_only: false,
        },
    };
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            EngineEvent::TradingStateUpdate(TradingState::Enabled),
            EngineOutput::AlgoOrders(GenerateAlgoOrdersOutput {
                cancels_and_opens: SendCancelsAndOpensOutput {
                    cancels: SendRequestsOutput::default(),
                    opens: SendRequestsOutput {
                        sent: NoneOneOrMany::Many(vec![
                            Box::new(btc_usdt_buy_order.clone()),
                            Box::new(eth_btc_buy_order.clone()),
                        ]),
                        errors: NoneOneOrMany::None,
                    },
                },
                ..Default::default()
            })
        )
    );

    // Ensure ExecutionRequests were sent to ExecutionManager
    assert_eq!(
        execution_rx.next().unwrap(),
        ExecutionRequest::Open(btc_usdt_buy_order)
    );
    assert_eq!(
        execution_rx.next().unwrap(),
        ExecutionRequest::Open(eth_btc_buy_order)
    );

    // TradingState::Disabled
    let event = EngineEvent::TradingStateUpdate(TradingState::Disabled);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(4));
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::OnTradingDisabled(OnTradingDisabledOutput)
        )
    );

    // Simulate OpenOrder response for Sequence(3) btc_usdt_buy_order
    let event = account_event_order_response(0, 2, Side::Buy, 1.0, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(5));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .orders
            .0
            .is_empty()
    );

    // Simulate Trade update for Sequence(3) btc_usdt_buy_order (fees 10% -> 1000usdt)
    let event = account_event_trade(0, 2, Side::Buy, 10_000.0, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(6));
    assert_eq!(audit.event, EngineAudit::process(event));

    // Simulate Balance update for Sequence(3) btc_usdt_buy_order, AssetIndex(2)/usdt reduction
    let event = account_event_balance(2, 2, 9_000.0, 9_000.0); // 10k - 10% fees
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(7));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(2))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(9_000.0), dec!(9_000.0)),
            time_plus_days(STARTING_TIMESTAMP, 2)
        )
    );
    // Simulate Balance update for Sequence(3) btc_usdt_buy_order, AssetIndex(0)/btc increase
    let event = account_event_balance(0, 2, 2.0, 2.0); // 1btc + 1btc
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(8));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(0))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(2.0), dec!(2.0)),
            time_plus_days(STARTING_TIMESTAMP, 2)
        )
    );

    // Simulate OpenOrder response for Sequence(3) eth_btc_buy_order
    let event = account_event_order_response(1, 2, Side::Buy, 1.0, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(9));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .orders
            .0
            .is_empty()
    );

    // Simulate Trade update for Sequence(3) eth_btc_buy_order (fees 10% -> 0.01btc)
    let event = account_event_trade(1, 2, Side::Buy, 0.1, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(10));
    assert_eq!(audit.event, EngineAudit::process(event));

    // Simulate Balance update for Sequence(3) eth_btc_buy_order, AssetIndex(0)/btc reduction
    let event = account_event_balance(0, 2, 0.99, 0.99); // 1btc - 10% fees
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(11));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(0))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(0.99), dec!(0.99)),
            time_plus_days(STARTING_TIMESTAMP, 2)
        )
    );

    // Simulate Balance update for Sequence(3) eth_btc_buy_order, AssetIndex(1)/eth increase
    let event = account_event_balance(1, 2, 11.0, 11.0); // 10eth + 1eth
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(12));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(1))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(11.0), dec!(11.0)),
            time_plus_days(STARTING_TIMESTAMP, 2)
        )
    );

    // Process 2nd MarketEvent for btc_usdt
    let event = market_event_trade(2, 0, dec!(20_000));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(13));
    assert_eq!(audit.event, EngineAudit::process(event));

    // Process 2nd MarketEvent for eth_btc
    let event = market_event_trade(2, 1, dec!(0.05));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(14));
    assert_eq!(audit.event, EngineAudit::process(event));

    // Send ClosePositionsCommand for btc_usdt
    let event = command_close_position(0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(15));
    let btc_usdt_sell_order = OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            // ClosePositionsStrategy uses pos_id.0.as_str() as the CID; for netting
            // mode the PositionId is PositionId::NETTING whose inner value is "netting".
            cid: ClientOrderId::new("netting"),
        },
        state: RequestOpen {
            side: Side::Sell,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            price: None,
            quantity: dec!(1),
            position_id: Some(PositionId::NETTING),
            reduce_only: true, // closing position
        },
    };
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::Commanded(ActionOutput::ClosePositions(SendCancelsAndOpensOutput {
                cancels: SendRequestsOutput::default(),
                opens: SendRequestsOutput {
                    sent: NoneOneOrMany::One(Box::new(btc_usdt_sell_order.clone())),
                    errors: NoneOneOrMany::None,
                },
            }))
        )
    );

    // Ensure ClosePositions ExecutionRequest was sent to ExecutionManager
    assert_eq!(
        execution_rx.next().unwrap(),
        ExecutionRequest::Open(btc_usdt_sell_order)
    );

    // Simulate OpenOrder response for Sequence(15) ClosePositionsCommand btc_usdt_sell_order.
    // CID must be "netting" (PositionId::NETTING.0) to match the order the engine placed.
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: strategy_id(),
                cid: ClientOrderId::new("netting"),
            },
            side: Side::Sell,
            price: None,
            quantity: dec!(1),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            state: OrderState::active(Open {
                id: gen_order_id(0),
                time_exchange: time_plus_days(STARTING_TIMESTAMP, 3),
                filled_quantity: dec!(1),
            }),
        })),
    }));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(16));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .orders
            .0
            .is_empty()
    );

    // Simulate Balance update for Sequence(15) btc_usdt_sell_order, AssetIndex(2)/usdt increase
    let event = account_event_balance(2, 3, 27_000.0, 27_000.0); // 9k + 20k - 10% fees
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(17));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(2))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(27_000.0), dec!(27_000.0)),
            time_plus_days(STARTING_TIMESTAMP, 3)
        )
    );

    // Simulate Balance update for Sequence(15) btc_usdt_sell_order, AssetIndex(0)/btc decrease
    let event = account_event_balance(0, 3, 1.0, 1.0); // 2btc - 1btc
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(18));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(0))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(1.0), dec!(1.0)),
            time_plus_days(STARTING_TIMESTAMP, 3)
        )
    );

    // Simulate Trade update for Sequence(15) btc_usdt_sell_order (fees 10% -> 2000usdt)
    let event = account_event_trade(0, 3, Side::Sell, 20_000.0, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(19));
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            PositionExited {
                position_id: PositionId::NETTING,
                instrument: InstrumentIndex(0),
                side: Side::Buy,
                price_entry_average: dec!(10_000.0),
                quantity_abs_max: dec!(1.0),
                pnl_realised: dec!(7000.0), // (-10k entry - 1k fees)+(20k exit - 2k fees) = 7k
                fees_enter: asset_fees(0, dec!(1_000.0)),
                fees_exit: asset_fees(0, dec!(2_000.0)),
                time_enter: time_plus_days(STARTING_TIMESTAMP, 2),
                time_exit: time_plus_days(STARTING_TIMESTAMP, 3),
                trades: vec![gen_trade_id(0), gen_trade_id(0)],
            }
        )
    );

    // Simulate exchange disconnection
    let event = EngineEvent::Market(MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(20));
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(event, EngineOutput::MarketDisconnect(OnDisconnectOutput))
    );
    assert_eq!(engine.state.connectivity.global, Health::Reconnecting);
    assert_eq!(
        engine
            .state
            .connectivity
            .connectivity(&ExchangeId::BinanceSpot)
            .market_data,
        Health::Reconnecting
    );
    assert_eq!(
        engine
            .state
            .connectivity
            .connectivity(&ExchangeId::BinanceSpot)
            .account,
        Health::Healthy
    );

    // Issue Command::SendOpenRequests OrderKind::LIMIT to close eth_btc position
    let eth_btc_sell_order = OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(1),
            strategy: strategy_id(),
            cid: gen_cid(1),
        },
        state: RequestOpen {
            side: Side::Sell,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            price: Some(dec!(0.05)),
            quantity: dec!(1),
            position_id: None,
            reduce_only: true, // closing position
        },
    };
    let event = EngineEvent::Command(Command::SendOpenRequests(OneOrMany::One(
        eth_btc_sell_order.clone(),
    )));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(21));
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::Commanded(ActionOutput::OpenOrders(SendRequestsOutput {
                sent: NoneOneOrMany::One(Box::new(eth_btc_sell_order.clone())),
                errors: NoneOneOrMany::None,
            }))
        )
    );

    // Ensure ExecutionRequest for Sequence(21) Command::SendOpenRequests was sent to ExecutionManager
    assert_eq!(
        execution_rx.next().unwrap(),
        ExecutionRequest::Open(eth_btc_sell_order)
    );

    // Simulate LIMIT OpenOrder response for Sequence(21) eth_btc_sell_order (0/1 quantity filled)
    let event = account_event_order_response(1, 4, Side::Sell, 1.0, 0.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(22));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .orders
            .0
            .len(),
        1
    );
    assert_eq!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .orders
            .0
            .get(&gen_cid(1))
            .unwrap(),
        &Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(1),
                strategy: strategy_id(),
                cid: gen_cid(1),
            },
            side: Side::Sell,
            price: Some(dec!(0.05)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            state: ActiveOrderState::Open(Open {
                id: gen_order_id(1),
                time_exchange: time_plus_days(STARTING_TIMESTAMP, 4),
                filled_quantity: dec!(0),
            }),
        }
    );

    // Simulate Balance update for Sequence(21) eth_btc_sell_order, AssetIndex(1)/eth free reduction
    let event = account_event_balance(1, 4, 11.0, 10.0); // 1eth in order
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(23));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(1))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(11.0), dec!(10.0)),
            time_plus_days(STARTING_TIMESTAMP, 4)
        )
    );

    // Simulate Order FullyFilled update for Sequence(21) LIMIT eth_btc_sell_order
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(1),
                strategy: strategy_id(),
                cid: gen_cid(1),
            },
            side: Side::Sell,
            price: Some(dec!(0.05)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            state: OrderState::fully_filled(Filled::new(
                OrderId::new("eth_btc_sell_order"),
                time_plus_days(STARTING_TIMESTAMP, 4),
                dec!(1),
                None,
            )),
        })),
    }));
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(24));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .orders
            .0
            .is_empty()
    );

    // Simulate Trade update for Sequence(21) LIMIT eth_btc_sell_order (fees 10% -> 0.05btc)
    let event = account_event_trade(1, 5, Side::Sell, 0.05, 1.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(25));
    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            PositionExited {
                position_id: PositionId::NETTING,
                instrument: InstrumentIndex(1),
                side: Side::Buy,
                price_entry_average: dec!(0.1),
                quantity_abs_max: dec!(1.0),
                pnl_realised: dec!(-0.065), // 0.05 - 0.01 - 0.01 entry fees - 0.005 exit fees
                fees_enter: asset_fees(1, dec!(0.01)), // 0.01 btc
                fees_exit: asset_fees(1, dec!(0.005)), // 0.005 btc
                time_enter: time_plus_days(STARTING_TIMESTAMP, 2),
                time_exit: time_plus_days(STARTING_TIMESTAMP, 5),
                trades: vec![gen_trade_id(1), gen_trade_id(1)],
            }
        )
    );

    // Simulate Balance update for Sequence(21) eth_btc_sell_order Trade, AssetIndex(1)/eth total decrease
    let event = account_event_balance(1, 5, 10.0, 10.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(26));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(
        engine
            .state
            .assets
            .asset_index(&AssetIndex(1))
            .balance
            .unwrap(),
        Timed::new(
            Balance::new(dec!(10.0), dec!(10.0)),
            time_plus_days(STARTING_TIMESTAMP, 5)
        )
    );

    // End trading session and produce TradingSummaryGenerator
    let mut summary = engine.trading_summary_generator(RISK_FREE_RETURN);
    summary.update_time_now(time_plus_days(STARTING_TIMESTAMP, 5));

    assert_eq!(summary.risk_free_return, RISK_FREE_RETURN);
    assert_eq!(
        summary.time_engine_now,
        time_plus_days(STARTING_TIMESTAMP, 5)
    );

    let btc_usdt_tear = summary.instruments.get_index(0).unwrap().1;
    assert_eq!(btc_usdt_tear.pnl_returns.pnl_raw, dec!(7000.0));

    let eth_btc_tear = summary.instruments.get_index(1).unwrap().1;
    assert_eq!(eth_btc_tear.pnl_returns.pnl_raw, dec!(-0.065));

    // Generate TradingSummary with Annual365 interval (crypto 24/7 trading)
    let trading_summary = summary.generate(Annual365);

    // Verify time bounds are consistent with the generator
    assert_eq!(trading_summary.time_engine_start, summary.time_engine_start);
    assert_eq!(trading_summary.time_engine_end, summary.time_engine_now);
    // Trading duration should be ~5 days (timestamps derived from STARTING_TIMESTAMP,
    // but engine processing introduces nanosecond-level drift)
    let duration = trading_summary.trading_duration();
    let five_days = TimeDelta::days(5);
    let drift = (five_days - duration).abs();
    assert!(
        drift < TimeDelta::milliseconds(1),
        "Expected ~5 days (within 1ms), got {:?} (drift: {:?})",
        duration,
        drift
    );

    // Verify instrument TearSheets were generated with correct PnL
    let btc_usdt_sheet = trading_summary.instruments.get_index(0).unwrap().1;
    assert_eq!(btc_usdt_sheet.pnl, dec!(7000.0));

    let eth_btc_sheet = trading_summary.instruments.get_index(1).unwrap().1;
    assert_eq!(eth_btc_sheet.pnl, dec!(-0.065));
}

/// Regression for #186: an open position's `pnl_unrealised` (and `time_exchange_update`) must
/// refresh on **every market tick**, not only on trade fills.
///
/// Before the wiring fix, `EngineState::update_from_market` called only
/// `instrument_state.data.process(event)` and never `InstrumentState::update_from_market` (its only
/// caller repo-wide), so `pnl_unrealised` stayed frozen at its post-fill value between fills — `0`
/// for a freshly opened position, regardless of how far the market moved. This drives a bare market
/// tick (no trade) through the real engine dispatch and asserts the position revalues.
#[test]
fn test_pnl_unrealised_updates_on_market_tick() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    let read_btc_position = |engine: &TestEngine| {
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .get(&PositionId::NETTING)
            .cloned()
            .expect("btc_usdt netting position should be open")
    };

    // Seed the account snapshot, then a first market price so btc_usdt is Healthy and priced.
    // (Bind the snapshot first — it borrows `engine.state` immutably, which cannot overlap the
    // `&mut engine` the call needs.)
    let snapshot = account_event_snapshot(&engine.state.assets);
    let _ = process_with_audit(&mut engine, snapshot);
    let _ = process_with_audit(&mut engine, market_event_trade(1, 0, dec!(10_000)));

    // Open a 1-unit btc_usdt LONG at 10_000 via an account Trade at day 2 (fees_quote = 10% of
    // notional = 1_000 usdt). `Position::from` seeds `pnl_unrealised = 0` — it is NOT recomputed on
    // open — so the position starts flat and only a later revaluation can move it.
    let _ = process_with_audit(
        &mut engine,
        account_event_trade(0, 2, Side::Buy, 10_000.0, 1.0),
    );

    let opened = read_btc_position(&engine);
    assert_eq!(opened.pnl_unrealised, dec!(0));
    assert_eq!(
        opened.time_exchange_update,
        time_plus_days(STARTING_TIMESTAMP, 2)
    );

    // A bare market tick at 20_000 (day 5, no trade) must now revalue the open position AND advance
    // its update clock — the exact refresh the pre-#186 engine skipped.
    let _ = process_with_audit(&mut engine, market_event_trade(5, 0, dec!(20_000)));

    let ticked = read_btc_position(&engine);
    // (20_000 − 10_000) × 1 − exit_fee, exit_fee = (qty/qty_max) × fees_enter.fees_quote
    //   = (1/1) × 1_000 = 1_000  ⇒  10_000 − 1_000 = 9_000.
    assert_eq!(ticked.pnl_unrealised, dec!(9_000));
    // Advanced to the tick's exchange time — proving a market tick, not a trade, refreshed it.
    assert_eq!(
        ticked.time_exchange_update,
        time_plus_days(STARTING_TIMESTAMP, 5)
    );
}

/// Routes a `BalanceStreamUpdate` (the WS-sourced partial) end-to-end through
/// `EngineState::update_from_account` → `AssetState::apply_balance_update`, asserting both the
/// resulting balance state and the audit trail.
///
/// Complements 17.3.6's isolated unit tests by exercising the full dispatch path
/// (`indexer` → `EngineState` → `AssetState::apply_balance_update`). The second scenario validates
/// the Design-decision-#4 no-clobber contract end-to-end: a REST `BalanceSnapshot` seeds margin
/// debt, then a WS `BalanceStreamUpdate` keeps `free`/`locked` live without erasing `margin`.
#[test]
fn test_engine_process_balance_stream_update_with_audit() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    // --- Scenario 1: cash stream update applies free/locked, recomputes total, no margin ---
    // usdt = AssetIndex(2); free=9_000 + locked=1_000 → total=10_000.
    let event = account_event_balance_update(2, 1, 9_000.0, 1_000.0);
    let audit = process_with_audit(&mut engine, event.clone());
    // Balance stream updates produce no engine output, like a BalanceSnapshot.
    assert_eq!(audit.event, EngineAudit::process(event));

    let usdt = engine
        .state
        .assets
        .asset_index(&AssetIndex(2))
        .balance
        .unwrap();
    assert_eq!(
        usdt,
        Timed::new(
            Balance::new(dec!(10_000.0), dec!(9_000.0)),
            time_plus_days(STARTING_TIMESTAMP, 1)
        )
    );
    // Cash context: no debt was ever reported, so margin stays absent.
    assert_eq!(usdt.value.margin, None);

    // --- Scenario 2: snapshot seeds debt, stream update preserves margin (no-clobber) ---
    // btc = AssetIndex(0); REST snapshot: total=2, free=2, borrowed=1, interest=0.01.
    let event = account_event_balance_margin(0, 2, 2.0, 2.0, 1.0, 0.01);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.event, EngineAudit::process(event));

    // WS partial: free=1.5 + locked=0.5 → total=2.0. Carries no debt.
    let event = account_event_balance_update(0, 3, 1.5, 0.5);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.event, EngineAudit::process(event));

    let btc = engine
        .state
        .assets
        .asset_index(&AssetIndex(0))
        .balance
        .unwrap();
    // free/locked are live from the WS update; borrowed/interest survive from the snapshot.
    assert_eq!(
        btc,
        Timed::new(
            Balance::new_margin(dec!(2.0), dec!(1.5), dec!(1.0), dec!(0.01)),
            time_plus_days(STARTING_TIMESTAMP, 3)
        )
    );
    // net_asset reflects the preserved debt: total - borrowed = 2 - 1.
    assert_eq!(btc.value.net_asset(), dec!(1.0));
}

struct TestBuyAndHoldStrategy {
    id: StrategyId,
}

impl AlgoStrategy for TestBuyAndHoldStrategy {
    type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let opens = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .filter_map(|state| {
                // Don't open more if we have a Position already
                if !state.position.positions.is_empty() {
                    return None;
                }

                // Don't open more orders if there are already some InFlight
                if !state.orders.0.is_empty() {
                    return None;
                }

                // Don't open if there is no instrument market price available
                state.data.price()?;

                // Generate Market order to buy the minimum allowed quantity
                Some(OrderRequestOpen {
                    key: OrderKey {
                        exchange: state.instrument.exchange,
                        instrument: state.key,
                        strategy: self.id.clone(),
                        cid: gen_cid(state.key.index()),
                    },
                    state: RequestOpen {
                        side: Side::Buy,
                        kind: OrderKind::Market,
                        time_in_force: TimeInForce::ImmediateOrCancel,
                        price: None, // Market orders don't have a limit price
                        quantity: dec!(1),
                        position_id: None,
                        reduce_only: false,
                    },
                })
            });

        (std::iter::empty(), opens)
    }
}

fn strategy_id() -> StrategyId {
    StrategyId::new("TestBuyAndHoldStrategy")
}

fn gen_cid(instrument: usize) -> ClientOrderId {
    ClientOrderId::new(InstrumentIndex(instrument).to_string())
}

fn gen_trade_id(instrument: usize) -> TradeId {
    TradeId::new(InstrumentIndex(instrument).to_string())
}

fn gen_order_id(instrument: usize) -> OrderId {
    OrderId::new(InstrumentIndex(instrument).to_string())
}

impl ClosePositionsStrategy for TestBuyAndHoldStrategy {
    type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

    fn close_positions_requests<'a>(
        &'a self,
        state: &'a Self::State,
        filter: &'a InstrumentFilter<ExchangeIndex, AssetIndex, InstrumentIndex>,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
    )
    where
        ExchangeIndex: 'a,
        AssetIndex: 'a,
        InstrumentIndex: 'a,
    {
        close_open_positions_with_market_orders(&self.id, state, filter, |_, pos_id| {
            ClientOrderId::new(pos_id.0.as_str())
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OnDisconnectOutput;
impl
    OnDisconnectStrategy<
        HistoricalClock,
        EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
        DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
    > for TestBuyAndHoldStrategy
{
    type OnDisconnect = OnDisconnectOutput;

    fn on_disconnect(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
            MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
        >,
        _: ExchangeId,
    ) -> Self::OnDisconnect {
        OnDisconnectOutput
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OnTradingDisabledOutput;
impl
    OnTradingDisabled<
        HistoricalClock,
        EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
        DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
    > for TestBuyAndHoldStrategy
{
    type OnTradingDisabled = OnTradingDisabledOutput;

    fn on_trading_disabled(
        _: &mut Engine<
            HistoricalClock,
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
            MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
            Self,
            DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
        >,
    ) -> Self::OnTradingDisabled {
        OnTradingDisabledOutput
    }
}

fn build_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> TestEngine {
    build_engine_with_oms(trading_state, execution_tx, OmsMode::Netting)
}

fn build_engine_with_oms(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
    oms_mode: OmsMode,
) -> TestEngine {
    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdt",
            "BTCUSDT",
            Underlying::new("btc", "usdt"),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
                InstrumentSpecQuantity::new(
                    OrderQuantityUnits::Quote,
                    dec!(0.00001),
                    dec!(0.00001),
                ),
                InstrumentSpecNotional::new(dec!(5.0)),
            )),
        ))
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_eth_btc",
            "ETHBTC",
            Underlying::new("eth", "btc"),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.00001), dec!(0.00001)),
                InstrumentSpecQuantity::new(OrderQuantityUnits::Quote, dec!(0.0001), dec!(0.0001)),
                InstrumentSpecNotional::new(dec!(0.0001)),
            )),
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);

    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .oms_mode(oms_mode)
    .balances([
        (ExchangeId::BinanceSpot, "usdt", STARTING_BALANCE_USDT),
        (ExchangeId::BinanceSpot, "btc", STARTING_BALANCE_BTC),
        (ExchangeId::BinanceSpot, "eth", STARTING_BALANCE_ETH),
    ])
    .build();

    let initial_account = FnvHashMap::from(&state);
    assert_eq!(initial_account.len(), 1);

    let execution_txs =
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]);

    Engine::new(
        clock,
        state,
        execution_txs,
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

fn account_event_snapshot(assets: &AssetStates) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Snapshot(AccountSnapshot {
            exchange: ExchangeIndex(0),
            balances: assets
                .0
                .iter()
                .enumerate()
                .map(|(index, (_, state))| AssetBalance {
                    asset: AssetIndex(index),
                    balance: state.balance.unwrap().value,
                    time_exchange: state.balance.unwrap().time,
                })
                .collect(),
            instruments: vec![],
        }),
    }))
}

fn market_event_trade(time_plus: u64, instrument: usize, price: Decimal) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        time_received: time_plus_days(STARTING_TIMESTAMP, time_plus),
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex(instrument),
        kind: DataKind::Trade(PublicTrade {
            id: time_plus.to_string().into(),
            price,
            amount: Decimal::ONE,
            side: Some(Side::Buy),
        }),
    }))
}

fn account_event_order_response(
    instrument: usize,
    time_plus: u64,
    side: Side,
    quantity: f64,
    filled: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(instrument),
                strategy: strategy_id(),
                cid: gen_cid(instrument),
            },
            side,
            price: None, // Market orders don't have a limit price
            quantity: Decimal::try_from(quantity).unwrap(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            state: OrderState::active(Open {
                id: gen_order_id(instrument),
                time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
                filled_quantity: Decimal::try_from(filled).unwrap(),
            }),
        })),
    }))
}

fn account_event_balance(
    asset: usize,
    time_plus: u64,
    total: f64,
    free: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance {
            asset: AssetIndex(asset),
            balance: Balance::new(
                Decimal::try_from(total).unwrap(),
                Decimal::try_from(free).unwrap(),
            ),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        })),
    }))
}

/// WS partial balance update (`free`/`locked` only) — sibling to [`account_event_balance`],
/// which emits a full REST [`AccountEventKind::BalanceSnapshot`]. This emits the WS-sourced
/// [`AccountEventKind::BalanceStreamUpdate`] that the engine applies via
/// `AssetState::apply_balance_update` (free/locked live, existing `margin` preserved).
fn account_event_balance_update(
    asset: usize,
    time_plus: u64,
    free: f64,
    locked: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::BalanceStreamUpdate(Snapshot(AssetBalanceUpdate {
            asset: AssetIndex(asset),
            update: BalanceUpdate::new(
                Decimal::try_from(free).unwrap(),
                Decimal::try_from(locked).unwrap(),
            ),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        })),
    }))
}

/// Full REST [`AccountEventKind::BalanceSnapshot`] carrying margin debt (`borrowed`/`interest`).
/// Used to seed debt before a [`account_event_balance_update`] to verify the WS partial does
/// not clobber it (Design decision #4 no-clobber contract).
fn account_event_balance_margin(
    asset: usize,
    time_plus: u64,
    total: f64,
    free: f64,
    borrowed: f64,
    interest: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance {
            asset: AssetIndex(asset),
            balance: Balance::new_margin(
                Decimal::try_from(total).unwrap(),
                Decimal::try_from(free).unwrap(),
                Decimal::try_from(borrowed).unwrap(),
                Decimal::try_from(interest).unwrap(),
            ),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        })),
    }))
}

fn account_event_trade(
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: f64,
    quantity: f64,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: gen_trade_id(instrument),
            order_id: gen_order_id(instrument),
            instrument: InstrumentIndex(instrument),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
            side,
            price: Decimal::try_from(price).unwrap(),
            quantity: Decimal::try_from(quantity).unwrap(),
            fees: asset_fees(
                instrument,
                Decimal::try_from(price * quantity * QUOTE_FEES_PERCENT).unwrap(),
            ),
        }),
    }))
}

fn command_close_position(instrument: usize) -> EngineEvent<DataKind> {
    EngineEvent::Command(Command::ClosePositions(InstrumentFilter::Instruments(
        OneOrMany::One(InstrumentIndex(instrument)),
    )))
}

// ---------------------------------------------------------------------------
// ContractExpiry integration tests
// ---------------------------------------------------------------------------

/// Build an engine with one BTC/USD spot instrument and one BTC call option
/// (strike 50_000). Both use BinanceSpot as the exchange.
///
/// After `IndexedInstrumentsBuilder::build()` sorts instruments alphabetically,
/// the resulting indices are:
///   InstrumentIndex(0) = Option ("binance_btc_call_50k" sorts before "binance_spot_btc_usd")
///   InstrumentIndex(1) = Spot
fn build_option_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> TestEngine {
    build_option_engine_with_oms(trading_state, execution_tx, OmsMode::Netting)
}

fn build_option_engine_with_oms(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
    oms_mode: OmsMode,
) -> TestEngine {
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let instruments = IndexedInstruments::builder()
        // index 0 (after sort): Option — "binance_btc_call_50k" < "binance_spot_btc_usd"
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_call_50k",
            "BTC-50000-C",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(50_000),
            }),
            None,
        ))
        // index 1 (after sort): Spot — "binance_spot_btc_usd" sorts after the option
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd",
            "BTCUSD",
            Underlying::new("btc", "usd"),
            None,
        ))
        // index 2 (after sort): a second split-eligible Spot on the same base and exchange but a
        // DIFFERENT quote. Present in every test on this fixture so the split-target uniqueness
        // guard is continuously exercised against a near-miss, rather than only against instruments
        // that share a full `(base, quote, exchange)` identity — see
        // `test_corporate_action_split_is_not_ambiguous_across_a_quote_boundary`. Inert otherwise:
        // it is never priced, never traded, and carries no position.
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdc",
            "BTCUSDC",
            Underlying::new("btc", "usdc"),
            None,
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);

    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .oms_mode(oms_mode)
    .balances([
        (ExchangeId::BinanceSpot, "usd", STARTING_BALANCE_USDT),
        (ExchangeId::BinanceSpot, "btc", STARTING_BALANCE_BTC),
    ])
    .build();

    let execution_txs =
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]);

    Engine::new(
        clock,
        state,
        execution_txs,
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

/// Send a market trade event to set the spot price for instrument at index `instrument`.
fn send_spot_price(engine: &mut TestEngine, instrument: usize, price: Decimal) {
    let event = market_event_trade(1, instrument, price);
    engine.process(event);
}

/// Open a long position in the option instrument (index 0) by sending a buy trade.
fn open_option_position(engine: &mut TestEngine, quantity: Decimal, price: Decimal) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new("opt-trade-open"),
            order_id: gen_order_id(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
            side: Side::Buy,
            price,
            quantity,
            // Option instrument quote is USD = AssetIndex(1) in option engine
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }));
    engine.process(event);
}

#[test]
fn test_contract_expiry_otm_call() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Set underlying spot price BELOW strike (50_000) → OTM (spot is at index 1)
    send_spot_price(&mut engine, 1, dec!(45_000));

    // Open a long call position with 2 contracts at premium 1_000
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // Verify position exists before expiry (option is at index 0)
    assert!(
        !engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty()
    );

    // Process ContractExpiry
    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    // OTM: settlement price is 0, position closes at zero value → position exits
    assert_eq!(exited.len(), 1);
    assert_eq!(exited[0].pnl_realised, dec!(-2_000)); // bought at 1000*2, settled at 0

    // Position should be cleared
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty()
    );

    // expiration_processed flag should be set
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );
}

#[test]
fn test_contract_expiry_itm_call() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Set underlying spot price ABOVE strike (50_000) → ITM (spot is at index 1)
    // Intrinsic value = spot - strike = 55_000 - 50_000 = 5_000
    send_spot_price(&mut engine, 1, dec!(55_000));

    // Open a long call position with 1 contract at premium 2_000
    open_option_position(&mut engine, dec!(1), dec!(2_000));

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    // ITM: 1 contract closed at intrinsic value 5_000
    assert_eq!(exited.len(), 1);
    // Entry: 1 * 2_000 = 2_000, Exit: 1 * 5_000 = 5_000 → pnl = 3_000
    assert_eq!(exited[0].pnl_realised, dec!(3_000));

    // Position should be cleared (consistency with OTM test)
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty()
    );

    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );
}

/// Regression for #166: `ContractExpiry` advances the `HistoricalClock` to the expiring
/// instrument's `expiry` — engine-side ground truth on its `InstrumentKind` — so the synthetic
/// settlement fill is stamped at the expiry instant rather than the prior market tick. The
/// `ContractExpiry` event carries no timestamp on its payload (unlike `CorporateAction`'s
/// `effective_time`), so the advance is derived in the handler from state, not the event.
#[test]
fn test_contract_expiry_advances_clock_to_expiry() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // The option's expiry, matching `build_option_engine`. The clock starts at
    // `STARTING_TIMESTAMP` (MIN_UTC), far before this, so a genuine forward advance is exercised.
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    // ITM call: spot (index 1) above strike (50_000) → settlement produces a closing fill.
    send_spot_price(&mut engine, 1, dec!(55_000));
    open_option_position(&mut engine, dec!(1), dec!(2_000));

    // Pre-expiry the clock reflects only the ~MIN_UTC market/open events, nowhere near 2030.
    assert!(
        engine.time() < expiry,
        "precondition: clock has not yet advanced to expiry"
    );

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));
    assert_eq!(exited.len(), 1);

    // The settlement fill is stamped at ~expiry: its `time_exit` derives from the synthetic
    // trade's `time_exchange = self.time()`, read after the clock advanced. An exact `==` is
    // impossible by construction — `HistoricalClock::time()` adds the sub-second wall-clock delta
    // since the advance — so a tight forward bound is the honest assertion (mirrors the
    // `CorporateAction` clock test).
    let fill_drift = exited[0].time_exit.signed_duration_since(expiry);
    assert!(
        fill_drift >= TimeDelta::zero() && fill_drift < TimeDelta::seconds(1),
        "settlement fill stamped at ~expiry (drift {fill_drift})"
    );

    // The engine clock itself advanced to ~expiry (monotonic; no look-ahead beyond the expiry).
    let clock_drift = engine.time().signed_duration_since(expiry);
    assert!(
        clock_drift >= TimeDelta::zero() && clock_drift < TimeDelta::seconds(1),
        "clock advanced to ~expiry (drift {clock_drift})"
    );
}

#[test]
fn test_contract_expiry_idempotent() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    send_spot_price(&mut engine, 1, dec!(45_000));
    open_option_position(&mut engine, dec!(1), dec!(1_000));

    // First expiry processes the position
    let exited_first = engine.process_contract_expiry(&InstrumentIndex(0));
    assert_eq!(exited_first.len(), 1);
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );

    // Second call: idempotent — returns empty vec, does not panic
    let exited_second = engine.process_contract_expiry(&InstrumentIndex(0));
    assert!(exited_second.is_empty());
}

#[test]
fn test_contract_expiry_no_position() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    send_spot_price(&mut engine, 1, dec!(45_000));

    // No position open — expiry should still mark as processed
    let exited = engine.process_contract_expiry(&InstrumentIndex(0));
    assert!(exited.is_empty());
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );
}

#[test]
fn test_contract_expiry_missing_spot_price() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Do NOT send any market data for the spot instrument

    // Open a position so expiry has something to settle
    open_option_position(&mut engine, dec!(1), dec!(1_000));

    // Without spot price, expiry cannot compute settlement — returns empty
    let exited = engine.process_contract_expiry(&InstrumentIndex(0));
    assert!(exited.is_empty());

    // expiration_processed must NOT be set (event is retryable)
    assert!(
        !engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );
}

#[test]
fn test_contract_expiry_replica_state_cleared() {
    use rustrade::{
        engine::audit::state_replica::StateReplicaManager,
        engine::audit::{AuditTick, EngineAudit, context::EngineContext},
    };
    use rustrade_integration::collection::none_one_or_many::NoneOneOrMany;

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    send_spot_price(&mut engine, 1, dec!(45_000));
    open_option_position(&mut engine, dec!(1), dec!(1_000));

    // Process ContractExpiry on the live engine to get the real audit outputs.
    let expiry_event = EngineEvent::ContractExpiry(InstrumentIndex(0));
    let audit_tick = process_with_audit(&mut engine, expiry_event.clone());

    // Build a separate replica state that mirrors the pre-expiry state.
    let (execution_tx2, _) = mpsc_unbounded();
    let mut replica_engine = build_option_engine(TradingState::Disabled, execution_tx2);
    send_spot_price(&mut replica_engine, 1, dec!(45_000));
    open_option_position(&mut replica_engine, dec!(1), dec!(1_000));

    let seed_context = EngineContext {
        time: STARTING_TIMESTAMP,
        sequence: Sequence(0),
    };
    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: replica_engine.state.clone(),
        context: seed_context,
    };

    // Type annotation required for StateReplicaManager::new to infer the iterator element type.
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    // Extract outputs from the audit to drive the replica update_from_event.
    // We reconstruct the outputs as a fresh NoneOneOrMany from the PositionExit items.
    let outputs: NoneOneOrMany<EngineOutput<OnTradingDisabledOutput, OnDisconnectOutput>> =
        match &audit_tick.event {
            EngineAudit::Process(audit) => {
                let exits: Vec<_> = audit
                    .outputs
                    .iter()
                    .filter_map(|o| match o {
                        EngineOutput::PositionExit(p) => {
                            Some(EngineOutput::PositionExit(p.clone()))
                        }
                        _ => None,
                    })
                    .collect();
                if exits.is_empty() {
                    NoneOneOrMany::None
                } else if exits.len() == 1 {
                    NoneOneOrMany::One(exits.into_iter().next().unwrap())
                } else {
                    NoneOneOrMany::Many(exits)
                }
            }
            _ => NoneOneOrMany::None,
        };

    // Directly call update_from_event (same path the StateReplicaManager::run uses).
    replica_manager.update_from_event(expiry_event, &outputs);

    let replica_instrument = replica_manager
        .replica_engine_state()
        .instruments
        .instrument_index(&InstrumentIndex(0));

    // Positions must be cleared
    assert!(replica_instrument.position.positions.is_empty());
    // Orders map must be cleared
    assert!(replica_instrument.orders.0.is_empty());
    // expiration_processed must be set
    assert!(replica_instrument.expiration_processed);
}

// ---------------------------------------------------------------------------
// CorporateAction (stock split) audit-replica parity tests
// ---------------------------------------------------------------------------

/// Open a position via a bare account `Trade` with an explicit `order_id`/`trade_id` tag.
///
/// `tag` populates **both** the `TradeId` and the `OrderId`. In `OmsMode::Hedging` the position
/// slot is keyed by `trade.order_id`, so distinct tags open distinct positions on the same
/// instrument (the routing "no order match" fallback). Fees are zero to keep `pnl_realised` clean
/// for the parity assertions.
fn open_position_via_trade(
    engine: &mut TestEngine,
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    tag: &str,
) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new(tag),
            order_id: OrderId::new(tag),
            instrument: InstrumentIndex(instrument),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
            side,
            price,
            quantity,
            fees: asset_fees(instrument, dec!(0)),
        }),
    }));
    engine.process(event);
}

/// Live engine vs audit-replica parity for a `CorporateAction` reverse split under `Floor`,
/// across multiple positions (Hedging) — one surviving, one floored to zero.
///
/// The replica arm event-replays `apply_split` from the payload (it does **not** mirror outputs),
/// so this test guards against the live handler (`process_corporate_action`) and the replica
/// (`StateReplicaManager::update_from_event`) drifting apart. The gold-standard assertion is full
/// `InstrumentState` equality, which also covers tear-sheet parity for the closed position.
#[test]
fn test_corporate_action_replica_parity_floor_split() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine_with_oms(TradingState::Disabled, execution_tx, OmsMode::Hedging);

    // Set a last price so apply_split eagerly recomputes pnl_unrealised (not the None⇒0 path).
    engine.process(market_event_trade(1, 0, dec!(100)));

    // Two long positions on the same Spot instrument (index 0). Under a 1:2 reverse split (Floor):
    //   - "survivor" 21 → floor(10.5) = 10 (survives, with a 0.5 fractional remainder)
    //   - "dust"      1 → floor(0.5)  = 0  (floored to zero ⇒ removed + PositionExit)
    open_position_via_trade(
        &mut engine,
        0,
        2,
        Side::Buy,
        dec!(100),
        dec!(21),
        "survivor",
    );
    open_position_via_trade(&mut engine, 0, 3, Side::Buy, dec!(100), dec!(1), "dust");

    // Seed the replica from the exact pre-split state.
    let pre_split_state = engine.state.clone();

    // Process the split on the live engine.
    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-reverse-1-2".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    // Seed the replica from the pre-split snapshot. The CorporateAction arm output-mirrors the
    // Floor-to-zero close (folding the live PositionExit), so it does not depend on the seed time.
    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    // Type annotation required for StateReplicaManager::new to infer the iterator element type.
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    // Drive the replica with the live outputs. The CorporateAction arm event-replays and ignores
    // them, but pass them through to exercise the real run() contract faithfully.
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &outputs);

    let live_instrument = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let replica_instrument = replica_manager
        .replica_engine_state()
        .instruments
        .instrument_index(&InstrumentIndex(0));

    // Gold-standard: full InstrumentState parity (positions, tear sheet, processed-id set, …).
    assert_eq!(replica_instrument, live_instrument);

    // Explicit checks that the LIVE handler did what we expect — so the parity assert above is
    // not vacuously comparing two unchanged states.
    let survivor_id = PositionId::new("survivor");
    let dust_id = PositionId::new("dust");

    // "dust" floored to zero ⇒ slot removed in both.
    assert!(!live_instrument.position.positions.contains_key(&dust_id));
    assert!(!replica_instrument.position.positions.contains_key(&dust_id));

    // "survivor" persists, split-adjusted: qty floor(21*0.5)=10, avg 100/0.5=200, max 21*0.5=10.5
    // (unfloored), pnl_unrealised recomputed from last price 100 = (100-200)*10 = -1000.
    let survivor = live_instrument
        .position
        .positions
        .get(&survivor_id)
        .expect("survivor position should persist");
    assert_eq!(survivor.quantity_abs, dec!(10));
    assert_eq!(survivor.price_entry_average, dec!(200));
    assert_eq!(survivor.quantity_abs_max, dec!(10.5));
    assert_eq!(survivor.pnl_unrealised, dec!(-1000));

    // Idempotency key recorded in both.
    assert!(
        live_instrument
            .corporate_actions_processed
            .contains("BTCUSDT-reverse-1-2")
    );
    assert!(
        replica_instrument
            .corporate_actions_processed
            .contains("BTCUSDT-reverse-1-2")
    );

    // Output ordering: within a single position's processing a `SplitRemainder` is pushed BEFORE its
    // paired `PositionExit`. "dust" (1 → floor(0.5) = 0) both disposes a 0.5 fractional sliver AND
    // floors to zero, so it emits BOTH observables — the cash-in-lieu record must precede the close
    // so a consumer can credit the CIL against a still-referenced position id before seeing it exit.
    let dust_remainder_idx = outputs
        .iter()
        .position(|o| {
            matches!(
                o,
                EngineOutput::SplitRemainder { position_id, .. } if *position_id == dust_id
            )
        })
        .expect("dust must emit a SplitRemainder (0.5 disposed)");
    let dust_exit_idx = outputs
        .iter()
        .position(|o| {
            matches!(
                o,
                EngineOutput::PositionExit(exit) if exit.position_id == dust_id
            )
        })
        .expect("dust floored to zero must emit a PositionExit");
    assert!(
        dust_remainder_idx < dust_exit_idx,
        "SplitRemainder (idx {dust_remainder_idx}) must precede its paired PositionExit \
         (idx {dust_exit_idx}) for the same floored-to-zero position"
    );
}

/// End-to-end idempotency for `CorporateAction`: the identical event fired twice must apply the
/// split exactly **once**. The second call hits the per-instrument `corporate_actions_processed`
/// guard (`process_corporate_action` step 1) and must be a no-op — it re-emits no
/// `SplitRemainder`/`PositionExit` and leaves `quantity_abs`/`price_entry_average` unchanged (a
/// double-apply would re-scale the basis and re-dispose a remainder). Mirrors
/// `test_contract_expiry_idempotent` for the split path, exercising the guard end-to-end through
/// `process_with_audit`.
#[test]
fn test_corporate_action_idempotent() {
    use rustrade::engine::audit::EngineAudit;

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine_with_oms(TradingState::Disabled, execution_tx, OmsMode::Hedging);

    // Last price so apply_split takes the eager pnl_unrealised recompute path (not None ⇒ 0).
    engine.process(market_event_trade(1, 0, dec!(100)));

    // One long position; a 1:2 reverse split under Floor leaves a fractional remainder
    // (21 → floor(10.5) = 10), so the FIRST application emits a SplitRemainder we can assert is
    // NOT re-emitted by the second (idempotent) call.
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(21), "pos");

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "AAPL-2026-split".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };

    // First application: split applied, id recorded, SplitRemainder emitted.
    let first = process_with_audit(&mut engine, ca_event.clone());
    let first_outputs = match &first.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    assert!(
        first_outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::SplitRemainder { .. })),
        "first application should emit a SplitRemainder (21 → floor(10.5) = 10, 0.5 disposed)"
    );

    // Snapshot the split-adjusted position after the first application.
    let pos_id = PositionId::new("pos");
    let after_first = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .position
        .positions
        .get(&pos_id)
        .expect("position should persist after the first split")
        .clone();
    assert_eq!(after_first.quantity_abs, dec!(10));
    assert_eq!(after_first.price_entry_average, dec!(200));

    // Second application of the IDENTICAL event: suppressed by the idempotency guard.
    let second = process_with_audit(&mut engine, ca_event.clone());
    let second_outputs = match &second.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    assert!(
        !second_outputs.iter().any(|o| matches!(
            o,
            EngineOutput::SplitRemainder { .. } | EngineOutput::PositionExit(_)
        )),
        "second (idempotent) application must re-emit no SplitRemainder/PositionExit"
    );

    // The idempotent skip IS surfaced on the output/audit stream as exactly one
    // CorporateActionAlreadyProcessed carrying the re-submitted event's instrument/kind/id — the
    // observable that lets a stream-only consumer distinguish a duplicate-id skip from a successful
    // split that had nothing to adjust.
    assert_eq!(
        second_outputs
            .iter()
            .filter(|o| matches!(o, EngineOutput::CorporateActionAlreadyProcessed { .. }))
            .count(),
        1,
        "second (idempotent) application must emit exactly one CorporateActionAlreadyProcessed"
    );
    assert!(
        second_outputs.iter().any(|o| matches!(
            o,
            EngineOutput::CorporateActionAlreadyProcessed { instrument, kind, id }
                if *instrument == InstrumentIndex(0)
                    && id.as_str() == "AAPL-2026-split"
                    && matches!(
                        kind,
                        CorporateActionKind::StockSplit { ratio }
                            if *ratio == SplitRatio::new(dec!(0.5)).unwrap()
                    )
        )),
        "CorporateActionAlreadyProcessed must carry the re-submitted event's instrument/kind/id"
    );

    // State unchanged by the second call (no double basis-adjust / re-dispose).
    let after_second = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .position
        .positions
        .get(&pos_id)
        .expect("position should still persist")
        .clone();
    assert_eq!(after_second, after_first);
}

/// Live engine vs audit-replica parity for a `CorporateAction` reverse split under `Floor` on
/// **short** positions (`Side::Sell`), across multiple positions (Hedging) — one surviving, one
/// floored to zero. `position.rs` unit tests cover shorts, but every other handler/replica parity
/// test opens `Side::Buy`; this exercises the opposite `pnl_unrealised` sign branch and the
/// floor-to-zero `PositionExit` for a short at the integration level.
#[test]
fn test_corporate_action_replica_parity_short_reverse_split() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine_with_oms(TradingState::Disabled, execution_tx, OmsMode::Hedging);

    // Last price so apply_split eagerly recomputes pnl_unrealised (not the None ⇒ 0 path).
    engine.process(market_event_trade(1, 0, dec!(100)));

    // Two SHORT positions (Side::Sell) on the same Spot instrument (index 0). Under a 1:2 reverse
    // split (Floor):
    //   - "survivor" 21 → floor(10.5) = 10 (survives, with a 0.5 fractional remainder)
    //   - "dust"      1 → floor(0.5)  = 0  (floored to zero ⇒ removed + PositionExit)
    open_position_via_trade(
        &mut engine,
        0,
        2,
        Side::Sell,
        dec!(100),
        dec!(21),
        "survivor",
    );
    open_position_via_trade(&mut engine, 0, 3, Side::Sell, dec!(100), dec!(1), "dust");

    // Seed the replica from the exact pre-split state.
    let pre_split_state = engine.state.clone();

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-reverse-1-2-short".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    // Type annotation required for StateReplicaManager::new to infer the iterator element type.
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &outputs);

    let live_instrument = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let replica_instrument = replica_manager
        .replica_engine_state()
        .instruments
        .instrument_index(&InstrumentIndex(0));

    // Gold-standard: full InstrumentState parity (positions, tear sheet, processed-id set, …).
    assert_eq!(replica_instrument, live_instrument);

    let survivor_id = PositionId::new("survivor");
    let dust_id = PositionId::new("dust");

    // "dust" short floored to zero ⇒ slot removed in both.
    assert!(!live_instrument.position.positions.contains_key(&dust_id));
    assert!(!replica_instrument.position.positions.contains_key(&dust_id));

    // "survivor" short persists, split-adjusted: qty floor(21*0.5)=10, avg 100/0.5=200,
    // max 21*0.5=10.5 (unfloored). pnl_unrealised uses the SHORT branch:
    // (entry - price) * qty = (200 - 100) * 10 = +1000 (a short gains as basis > current price).
    let survivor = live_instrument
        .position
        .positions
        .get(&survivor_id)
        .expect("survivor short position should persist");
    assert_eq!(survivor.side, Side::Sell);
    assert_eq!(survivor.quantity_abs, dec!(10));
    assert_eq!(survivor.price_entry_average, dec!(200));
    assert_eq!(survivor.quantity_abs_max, dec!(10.5));
    assert_eq!(survivor.pnl_unrealised, dec!(1000));
}

/// Live vs replica parity for a `CorporateAction` targeting a non-`Spot` instrument: both reject
/// (no state change, `id` not recorded), so the replica's guards must stay symmetric with the
/// handler's. Validates the `InstrumentKindNotSupported` rejection path.
#[test]
fn test_corporate_action_replica_parity_unsupported_non_spot() {
    use rustrade::engine::{
        UnsupportedCorporateActionReason,
        audit::{
            AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
        },
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Option position at index 0 (non-Spot). Spot underlying is index 1.
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    let pre_state = engine.state.clone();

    // Target the OPTION (index 0) — unsupported (equity splits apply to Spot only).
    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "opt-split".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    // Live must emit the dedicated rejection output (and NOT record the id ⇒ retryable).
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::UnsupportedCorporateAction {
                reason: UnsupportedCorporateActionReason::InstrumentKindNotSupported,
                ..
            }
        )),
        "expected UnsupportedCorporateAction(InstrumentKindNotSupported)"
    );

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    // Type annotation required for StateReplicaManager::new to infer the iterator element type.
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);
    replica_manager.update_from_event(ca_event, &outputs);

    let live_instrument = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let replica_instrument = replica_manager
        .replica_engine_state()
        .instruments
        .instrument_index(&InstrumentIndex(0));

    // Rejected ⇒ unchanged and symmetric: replica == live, position intact, id NOT recorded.
    assert_eq!(replica_instrument, live_instrument);
    assert_eq!(live_instrument.position.positions.len(), 1);
    assert!(live_instrument.corporate_actions_processed.is_empty());
    assert!(replica_instrument.corporate_actions_processed.is_empty());
}

/// End-to-end: a `CorporateAction` injected **mid-stream** through the audit path — the engine-level
/// behaviour of the backtest aux-injection seam, asserting what the `backtest()` smoke tests cannot.
///
/// `backtest()` hardcodes `AuditMode::Disabled`, so it produces no output/audit stream; the
/// post-split position, the `SplitRemainder` observable, and the **exact clock stamp** (Option A:
/// the `HistoricalClock` advances to `effective_time` on the event) are asserted here by driving a
/// pre-merged, time-ordered event sequence through `process_with_audit`. Ordering is exercised by a
/// market event *after* the split that must see the post-split position scale.
#[test]
fn test_corporate_action_injected_mid_stream_clock_and_outputs() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx); // Netting

    // Pre-split market context: a trade at t+1 sets the instrument's last (mark) price to 105 — the
    // source `apply_split` reads for the eager pnl_unrealised recompute, deliberately distinct from
    // the position's fill basis (100 below) so the pnl assertion pins the mark-price source (not the
    // entry price reused as the mark) — and advances the clock to t+1.
    engine.process(market_event_trade(1, 0, dec!(105)));

    // One long position of 101 @ 100 on Spot instrument 0.
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(101), "pos");

    // The split, time-ordered after the t+1 market event and before the t+11 one below — exactly
    // where the backtest seam's merge interleaves it. 1:2 reverse, Floor: floor(101*0.5)=50 survives
    // with a 0.5 fractional sliver disposed.
    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-reverse-1-2".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    // Clock stamp (Option A): the `HistoricalClock` advanced to effective_time on the split. Its
    // `time()` derives from `time_exchange_last + (now − last_event_live_time)`, so it reads
    // effective_time plus only the sub-second test-execution delta — proving the split advanced the
    // clock to its own instant (not the prior market event's t+1, and not beyond), the no-look-ahead
    // guarantee. An exact `==` is impossible by construction (the wall-clock term is nondeterministic,
    // the same drift the audit-replica parity tests output-mirror around); a tight bound is the
    // honest assertion. `context.time` is captured the same way during `audit()`.
    let drift = engine.time().signed_duration_since(effective_time);
    assert!(
        drift >= TimeDelta::zero() && drift < TimeDelta::seconds(1),
        "clock advanced to ~effective_time (drift {drift})"
    );

    // The SplitRemainder observable is emitted with post-split-era fields (Convention A).
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };
    let remainder = outputs
        .iter()
        .find_map(|o| match o {
            EngineOutput::SplitRemainder {
                instrument,
                side,
                quantity_fractional_disposed,
                price_entry_average_post_split,
                ..
            } => Some((
                *instrument,
                *side,
                *quantity_fractional_disposed,
                *price_entry_average_post_split,
            )),
            _ => None,
        })
        .expect("a SplitRemainder output must be emitted for the floored fractional sliver");
    assert_eq!(remainder.0, InstrumentIndex(0));
    assert_eq!(remainder.1, Side::Buy);
    assert_eq!(remainder.2, dec!(0.5)); // floor(50.5) disposes 0.5 (post-split units)
    assert_eq!(remainder.3, dec!(200)); // post-split basis = old_avg / ratio = 100 / 0.5

    // Post-split position: qty floored to 50, avg 200, max 50.5 (unfloored even under Floor),
    // pnl_unrealised recomputed from the last (mark) price 105 = (105 - 200) * 50 = -4750.
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instrument_state.position.positions.len(), 1);
    let position = instrument_state.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(50));
    assert_eq!(position.price_entry_average, dec!(200));
    assert_eq!(position.quantity_abs_max, dec!(50.5));
    assert_eq!(position.pnl_unrealised, dec!(-4750));
    assert!(
        instrument_state
            .corporate_actions_processed
            .contains("BTCUSDT-reverse-1-2")
    );

    // The split read the last price the t+1 market event set (105) for its eager pnl recompute
    // (-4750 above), proving that earlier market event was processed *before* the split. A market
    // event AFTER the split (t+11) is then processed in order and updates the instrument's last
    // price — confirming the split landed mid-stream, between the two. (Position `pnl_unrealised` is
    // deliberately *not* re-checked here: the engine's market path updates instrument data but does
    // not re-walk open positions each tick — the very reason `apply_split` recomputes pnl eagerly.)
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instrument_state.data.price(), Some(dec!(105)));
    engine.process(market_event_trade(11, 0, dec!(90)));
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instrument_state.data.price(), Some(dec!(90)));
}

// ---------------------------------------------------------------------------
// CorporateAction — option standard / non-standard split handling
// ---------------------------------------------------------------------------

/// Open a long option position (index 0) via a tagged account `Trade`, so that distinct `tag`s
/// create distinct Hedging slots (the slot is keyed by `order_id`). Mirrors
/// [`open_position_via_trade`] but uses the option engine's USD quote asset (`AssetIndex(1)`) for
/// fees — `asset_fees`/`quote_asset_index` map the *spot* engine's instruments only.
fn open_option_position_via_trade(
    engine: &mut TestEngine,
    time_plus: u64,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    tag: &str,
) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new(tag),
            order_id: OrderId::new(tag),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
            side,
            price,
            quantity,
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }));
    engine.process(event);
}

/// Standard (whole-number forward) split on the underlying equity adjusts an option position on
/// that underlying **in place**: strike ÷ ratio, contracts × ratio, premium basis ÷ ratio, and
/// `pnl_unrealised` recomputed from the **option's own** mark — `contract_size` (the multiplier)
/// stays unchanged. One `OptionPositionAdjustedForSplit` per position, plus an `OpenOrdersAtSplit`
/// for the option's own resting orders; no `SplitRemainder` (integer ratio × integer contracts).
/// The equity `id` is recorded on the `Spot` target.
#[test]
fn test_corporate_action_option_standard_split_adjusts_in_place() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Distinct marks: the option (idx0) at 1200, the spot (idx1) at 60_000. The split must use the
    // OPTION's mark for the pnl recompute — distinct values pin that the option's price is read,
    // not the splitting equity's.
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // Long call: 2 contracts @ premium 1000.
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // A resting order on the OPTION (idx0) — must be surfaced via OpenOrdersAtSplit (Q1).
    let event = account_event_order_response(0, 1, Side::Buy, 1.0, 0.0);
    engine.process(event);

    // 2:1 standard forward split, targeting the Spot underlying (idx1).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    // Option (idx0) adjusted in place: strike halved, multiplier (contract_size) untouched.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
    assert_eq!(contract.contract_size, dec!(1));

    // Position: contracts ×2 = 4, premium basis ÷2 = 500, pnl recomputed from the OPTION mark 1200:
    // 4 * (1200 - 500) * contract_size(1) = 2800.
    assert_eq!(option_state.position.positions.len(), 1);
    let position = option_state.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(4));
    assert_eq!(position.price_entry_average, dec!(500));
    assert_eq!(position.pnl_unrealised, dec!(2_800));

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Exactly one OptionPositionAdjustedForSplit, carrying the expected per-position fields.
    let adjusted: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::OptionPositionAdjustedForSplit {
                option_instrument,
                ratio,
                strike_pre_split,
                strike_post_split,
                position_id,
            } => Some((
                *option_instrument,
                ratio.get(),
                *strike_pre_split,
                *strike_post_split,
                position_id.clone(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(adjusted.len(), 1);
    assert_eq!(adjusted[0].0, InstrumentIndex(0));
    assert_eq!(adjusted[0].1, dec!(2));
    assert_eq!(adjusted[0].2, dec!(50_000));
    assert_eq!(adjusted[0].3, dec!(25_000));
    assert_eq!(adjusted[0].4, PositionId::NETTING);

    // The option's resting order is surfaced (Q1) — OpenOrdersAtSplit for idx0.
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OpenOrdersAtSplit { instrument, .. } if *instrument == InstrumentIndex(0)
        )),
        "expected OpenOrdersAtSplit for the option's resting order"
    );

    // No CIL on a standard option adjustment (integer ratio × integer contracts).
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::SplitRemainder { .. })),
        "a standard option split must not produce a SplitRemainder"
    );

    // The equity split was applied + recorded on the Spot target (idx1).
    let spot_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(1));
    assert!(
        spot_state
            .corporate_actions_processed
            .contains("BTCUSD-2-1-split")
    );
}

/// Standard split on an option with **no resting orders**: the `if !option_orders.is_empty()`
/// suppression on the option path must withhold `OpenOrdersAtSplit` for that option, while the
/// in-place `OptionPositionAdjustedForSplit` is still emitted. Pins that the empty-orders guard
/// gates *only* the order observable, never the position adjustment — deleting the guard, or
/// letting it suppress the adjustment too, both fail here.
#[test]
fn test_corporate_action_option_standard_split_no_orders_suppresses_open_orders_at_split() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Marks for the option (idx0) and the splitting spot underlying (idx1).
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // Open a long option position but place NO resting order on it (the case the guard suppresses).
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // 2:1 standard forward split on the Spot underlying (idx1).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // No resting orders ⇒ the suppression guard withholds OpenOrdersAtSplit for the option.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OpenOrdersAtSplit { instrument, .. } if *instrument == InstrumentIndex(0)
        )),
        "an option with no resting orders must not emit OpenOrdersAtSplit"
    );

    // Load-bearing: the guard gates ONLY the order observable — the in-place adjustment is still
    // emitted for the option position even when there are no resting orders.
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OptionPositionAdjustedForSplit { option_instrument, .. }
                if *option_instrument == InstrumentIndex(0)
        )),
        "the option position must still be adjusted in place even with no resting orders"
    );
}

/// Hedging: a standard split emits one `OptionPositionAdjustedForSplit` **per option position**
/// (per-`position_id` granularity, like `SplitRemainder`), each carrying the same instrument-level
/// strike change.
#[test]
fn test_corporate_action_option_standard_split_hedging_per_position() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine =
        build_option_engine_with_oms(TradingState::Disabled, execution_tx, OmsMode::Hedging);

    engine.process(market_event_trade(1, 0, dec!(1200)));

    // Two independent option positions — distinct Hedging slots keyed by order_id/tag.
    open_option_position_via_trade(&mut engine, 1, Side::Buy, dec!(1_000), dec!(2), "opt-a");
    open_option_position_via_trade(&mut engine, 1, Side::Buy, dec!(1_000), dec!(3), "opt-b");

    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    let adjusted: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::OptionPositionAdjustedForSplit {
                option_instrument,
                strike_pre_split,
                strike_post_split,
                position_id,
                ..
            } => Some((
                *option_instrument,
                *strike_pre_split,
                *strike_post_split,
                position_id.clone(),
            )),
            _ => None,
        })
        .collect();

    // One record per position (two positions ⇒ two records), each with the same strike change.
    assert_eq!(adjusted.len(), 2);
    for (instrument, strike_pre, strike_post, _pos_id) in &adjusted {
        assert_eq!(*instrument, InstrumentIndex(0));
        assert_eq!(*strike_pre, dec!(50_000));
        assert_eq!(*strike_post, dec!(25_000));
    }
    // Distinct per-position attribution (Hedging slot id == the order/trade tag).
    let ids: Vec<_> = adjusted.iter().map(|a| a.3.clone()).collect();
    assert!(ids.contains(&PositionId::new("opt-a")));
    assert!(ids.contains(&PositionId::new("opt-b")));
}

/// Non-standard splits — every fractional forward (3:2) and every reverse split (1:2) — cannot
/// adjust an option in place (the OCC assigns a new contract identity). The option is left
/// **unchanged** and a single `OptionPositionsRequireIdentityChange` is emitted; the equity split
/// is still applied and its `id` recorded.
#[test]
fn test_corporate_action_option_non_standard_requires_identity_change() {
    for (ratio, id) in [
        (dec!(1.5), "BTCUSD-3-2-split"),   // fractional forward (3:2)
        (dec!(0.5), "BTCUSD-1-2-reverse"), // reverse (1:2)
    ] {
        let (execution_tx, _execution_rx) = mpsc_unbounded();
        let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

        engine.process(market_event_trade(1, 0, dec!(1200)));
        open_option_position(&mut engine, dec!(2), dec!(1_000));

        let ca_event = EngineEvent::CorporateAction {
            id: id.into(),
            instrument: InstrumentIndex(1),
            kind: CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(ratio).unwrap(),
            },
            policy: SplitRoundingPolicy::Floor,
            effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
        };
        let audit_tick = process_with_audit(&mut engine, ca_event);

        // Option (idx0) UNCHANGED: strike, qty, and basis all at pre-split terms.
        let option_state = engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0));
        let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
            panic!("instrument 0 must be an option");
        };
        assert_eq!(
            contract.strike,
            dec!(50_000),
            "ratio {ratio}: option strike must be untouched"
        );
        let position = option_state.position.positions.values().next().unwrap();
        assert_eq!(position.quantity_abs, dec!(2), "ratio {ratio}");
        assert_eq!(position.price_entry_average, dec!(1_000), "ratio {ratio}");

        let outputs = match &audit_tick.event {
            EngineAudit::Process(audit) => &audit.outputs,
            _ => panic!("expected EngineAudit::Process"),
        };

        // Exactly one identity-change signal listing the option.
        let signals: Vec<_> = outputs
            .iter()
            .filter_map(|o| match o {
                EngineOutput::OptionPositionsRequireIdentityChange {
                    split_instrument,
                    ratio: r,
                    affected_options,
                } => Some((*split_instrument, r.get(), affected_options.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(signals.len(), 1, "ratio {ratio}");
        assert_eq!(signals[0].0, InstrumentIndex(1));
        assert_eq!(signals[0].1, ratio);
        assert!(
            signals[0].2.contains(&InstrumentIndex(0)),
            "ratio {ratio}: affected_options must list the option"
        );

        // No in-place option adjustment occurred.
        assert!(
            !outputs
                .iter()
                .any(|o| matches!(o, EngineOutput::OptionPositionAdjustedForSplit { .. })),
            "ratio {ratio}: must not adjust the option in place"
        );

        // The equity split is still applied + recorded (the Spot target always splits).
        let spot_state = engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1));
        assert!(
            spot_state.corporate_actions_processed.contains(id),
            "ratio {ratio}: equity id must be recorded"
        );
    }
}

/// A **non-standard** split where the affected option holds a **resting order** must NOT emit an
/// `OpenOrdersAtSplit` for that option. The non-standard path signals a wrapper-side identity change
/// and touches no option state — it never snapshots the option's resting orders (unlike the standard
/// path, which price-adjusts them and reports them). Pins that the order snapshot is exclusive to the
/// in-place-adjustment branch: a resting order on an option facing a reverse/fractional split is
/// surfaced only via the identity-change signal, never as a now-meaningless `OpenOrdersAtSplit`.
#[test]
fn test_corporate_action_option_non_standard_no_open_orders_at_split() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // Long option position AND a resting order on the OPTION (idx0). The standard path WOULD surface
    // this order via OpenOrdersAtSplit (see the adjusts-in-place test); the non-standard path must not.
    open_option_position(&mut engine, dec!(2), dec!(1_000));
    let order_event = account_event_order_response(0, 1, Side::Buy, 1.0, 0.0);
    engine.process(order_event);

    // 1:2 reverse split on the Spot underlying (idx1) — non-standard (OCC assigns a new identity).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-1-2-reverse".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // No OpenOrdersAtSplit for the option (idx0): the non-standard path snapshots no option orders.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OpenOrdersAtSplit { instrument, .. } if *instrument == InstrumentIndex(0)
        )),
        "a non-standard split must not emit OpenOrdersAtSplit for an option holding a resting order"
    );

    // Sanity: the identity-change signal IS emitted for the option, and no in-place adjustment.
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OptionPositionsRequireIdentityChange { affected_options, .. }
                if affected_options.contains(&InstrumentIndex(0))
        )),
        "the option must be listed for a wrapper-side identity change"
    );
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::OptionPositionAdjustedForSplit { .. })),
        "a non-standard split must not adjust the option in place"
    );
}

/// Pre-validation rejects a standard split **atomically** when a held option position carries a
/// non-integer contract count (state corruption): the engine emits
/// `UnsupportedCorporateAction { reason: PositionStateInvalid }`, mutates **nothing** (option strike
/// and the corrupt quantity are left as-is), and does **not** record the `id` — so the action stays
/// retryable once the position is reconciled. Directly exercises the read-only pre-computation pass
/// (`prepare_corporate_action_split`) and the new rejection reason.
#[test]
fn test_corporate_action_pre_validation_rejects_non_integer_option_contracts() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // Open a normal (integer) option position, then PLANT corruption: force a fractional contract
    // count directly on the position slot (a valid feed can never produce this).
    open_option_position(&mut engine, dec!(2), dec!(1_000));
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .position
        .positions
        .get_mut(&PositionId::NETTING)
        .expect("option position should exist")
        .quantity_abs = dec!(2.5);

    // 2:1 STANDARD forward split on the Spot underlying (idx1) — would adjust the option in place,
    // but pre-validation must reject it because idx0 holds 2.5 contracts.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Exactly the rejection observable, attributed to the splitting equity with the corruption reason.
    let rejections: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::UnsupportedCorporateAction {
                instrument, reason, ..
            } => Some((*instrument, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(rejections.len(), 1, "expected exactly one rejection");
    assert_eq!(rejections[0].0, InstrumentIndex(1));
    assert_eq!(
        rejections[0].1,
        UnsupportedCorporateActionReason::PositionStateInvalid
    );

    // Atomic: nothing was mutated. Option strike still pre-split, corrupt quantity untouched.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(
        contract.strike,
        dec!(50_000),
        "option strike must be untouched"
    );
    assert_eq!(
        option_state
            .position
            .positions
            .get(&PositionId::NETTING)
            .expect("option position should persist")
            .quantity_abs,
        dec!(2.5),
        "the corrupt contract count must be left as-is (no partial rescale)"
    );

    // The `id` was NOT recorded ⇒ retryable once the position is reconciled.
    let spot_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(1));
    assert!(
        !spot_state
            .corporate_actions_processed
            .contains("BTCUSD-2-1-split"),
        "a rejected action must not record its id"
    );

    // No adjustment or CIL leaked through.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OptionPositionAdjustedForSplit { .. }
                | EngineOutput::SplitRemainder { .. }
        )),
        "a rejected split must emit no adjustment or remainder"
    );
}

/// Pre-validation rejects a stock split **atomically** when rescaling an affected position would
/// overflow `Decimal` (an extreme/adversarial quantity, not a transient condition): the engine
/// emits `UnsupportedCorporateAction { reason: ArithmeticOverflow }`, mutates **nothing**, and does
/// **not** record the `id`. The end-to-end handler counterpart to the unit-level
/// `position::tests::test_apply_split_ratio_overflow_is_err_and_leaves_position_unmutated` —
/// exercising the overflow branch of the read-only pre-computation pass
/// (`prepare_corporate_action_split`) through the full `process_corporate_action` path.
#[test]
fn test_corporate_action_pre_validation_rejects_arithmetic_overflow() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx); // Netting spot

    // Mark 100, long 10 @ 100 on Spot instrument 0.
    engine.process(market_event_trade(1, 0, dec!(100)));
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(10), "pos");

    // PLANT an adversarial quantity a forward split cannot rescale without overflowing `Decimal`
    // (`Decimal::MAX * 2` has no representation). A valid feed can never produce this.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .position
        .positions
        .values_mut()
        .next()
        .expect("spot position should exist")
        .quantity_abs = Decimal::MAX;

    // 2:1 STANDARD forward split on the same Spot instrument — pre-validation must reject it because
    // rescaling the planted quantity overflows before any mutation.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-overflow-2-1".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Exactly the rejection observable, attributed to the splitting equity with the overflow reason.
    let rejections: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::UnsupportedCorporateAction {
                instrument, reason, ..
            } => Some((*instrument, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(rejections.len(), 1, "expected exactly one rejection");
    assert_eq!(rejections[0].0, InstrumentIndex(0));
    assert_eq!(
        rejections[0].1,
        UnsupportedCorporateActionReason::ArithmeticOverflow
    );

    // Atomic: nothing was mutated. The planted quantity and its untouched basis / high-water mark
    // are all left exactly as they were (no partial rescale).
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let position = instrument_state
        .position
        .positions
        .values()
        .next()
        .expect("spot position should persist");
    assert_eq!(
        position.quantity_abs,
        Decimal::MAX,
        "the planted quantity must be left as-is (no partial rescale)"
    );
    assert_eq!(
        position.price_entry_average,
        dec!(100),
        "basis must be untouched"
    );
    assert_eq!(
        position.quantity_abs_max,
        dec!(10),
        "high-water mark must be untouched"
    );

    // The `id` was NOT recorded ⇒ retryable once the feed is corrected.
    assert!(
        !instrument_state
            .corporate_actions_processed
            .contains("BTCUSDT-overflow-2-1"),
        "a rejected action must not record its id"
    );

    // No rescale, remainder, or open-orders snapshot leaked through the rejection.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::SplitRemainder { .. } | EngineOutput::OpenOrdersAtSplit { .. }
        )),
        "a rejected split must emit no remainder or open-orders snapshot"
    );
}

/// A **standard** split on an underlying whose option leg would overflow `Decimal` on rescale is
/// rejected **atomically** — the option's strike is left at its pre-split value rather than being
/// divided in place before the overflow. This is the option-path analog of
/// `test_corporate_action_pre_validation_rejects_arithmetic_overflow` (which plants the overflow on
/// the equity leg), and it exercises the fix that extended the read-only pre-computation pass
/// (`prepare_corporate_action_split`) to cover **every option on the underlying** before any
/// mutation: the handler used to divide `contract.strike` in place first, then panic when a held
/// option position could not be rescaled, leaving a half-adjusted strike behind.
///
/// Note on the strike arithmetic itself: a standard split's `ratio` is always a whole number ≥ 2
/// (OCC classification, `SplitAdjustmentKind::Standard`), so `strike ÷ ratio` only ever *shrinks*
/// the strike and cannot itself overflow — its `checked_div` is defence-in-depth. The reachable
/// option-leg overflow is the position's `quantity_abs × ratio`, planted here on a held option; the
/// pre-computation still rejects the whole action before the strike (or anything else) is touched.
#[test]
fn test_corporate_action_pre_validation_rejects_option_leg_overflow() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Marks for the option (idx0) and the spot underlying (idx1); open a small long call on the
    // option so it is HELD (an unheld option's only split arithmetic is the strike division, which
    // cannot overflow — see the doc comment).
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // PLANT an adversarial contract count a forward split cannot rescale without overflowing
    // `Decimal` (`Decimal::MAX × 2` has no representation). It is still a whole number, so it passes
    // the integer-contract invariant and is rejected on the overflow branch, not `PositionStateInvalid`.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .position
        .positions
        .values_mut()
        .next()
        .expect("option position should exist")
        .quantity_abs = Decimal::MAX;

    // 2:1 STANDARD forward split on the Spot underlying (idx1). Pre-computation reaches the option
    // scan (standard classification), pre-checks the strike (fine), then overflows on the option
    // position rescale — rejecting the whole action.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-option-overflow-2-1".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Exactly the rejection observable, attributed to the splitting equity with the overflow reason.
    let rejections: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::UnsupportedCorporateAction {
                instrument, reason, ..
            } => Some((*instrument, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(rejections.len(), 1, "expected exactly one rejection");
    assert_eq!(rejections[0].0, InstrumentIndex(1));
    assert_eq!(
        rejections[0].1,
        UnsupportedCorporateActionReason::ArithmeticOverflow
    );

    // Atomic — the key #181 assertion: the option's STRIKE was NOT divided in place. Before the fix
    // the handler halved it (50_000 → 25_000) before hitting the un-rescalable position; now the
    // whole action is rejected before any strike or position is touched.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(
        contract.strike,
        dec!(50_000),
        "the option strike must be left at its pre-split value (no partial in-place division)"
    );
    let position = option_state
        .position
        .positions
        .values()
        .next()
        .expect("option position should persist");
    assert_eq!(
        position.quantity_abs,
        Decimal::MAX,
        "the planted contract count must be left as-is (no partial rescale)"
    );

    // The `id` was NOT recorded on the Spot target ⇒ retryable once the feed is corrected.
    assert!(
        !engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .corporate_actions_processed
            .contains("BTCUSD-option-overflow-2-1"),
        "a rejected action must not record its id"
    );

    // No option adjustment, remainder, or open-orders snapshot leaked through the rejection.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OptionPositionAdjustedForSplit { .. }
                | EngineOutput::SplitRemainder { .. }
                | EngineOutput::OpenOrdersAtSplit { .. }
        )),
        "a rejected split must emit no option adjustment, remainder, or open-orders snapshot"
    );
}

/// Live engine vs audit-replica parity for a **standard** option adjustment: the replica
/// commits the pre-computed `SplitPlan` (post-split strike + per-position rescale) deterministically
/// (no output-mirror). Full `InstrumentState` equality for BOTH the option (idx0) and the spot (idx1)
/// guards against the handler and the replica drifting apart on the option path.
#[test]
fn test_corporate_action_option_replica_parity_standard_split() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Option mark (idx0) for the eager pnl recompute; spot mark (idx1) deliberately different.
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // Seed the replica from the exact pre-split state.
    let pre_split_state = engine.state.clone();

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    // Type annotation required for StateReplicaManager::new to infer the iterator element type.
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    // Drive the replica with the live outputs. The standard option adjustment is pure event-replay
    // (it ignores the outputs), but pass them through to exercise the real run() contract faithfully.
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &outputs);

    // Gold-standard: full InstrumentState parity for BOTH the adjusted option (idx0) and the split
    // equity (idx1) — positions, instrument kind (strike), tear sheet, processed-id set, …
    for idx in [InstrumentIndex(0), InstrumentIndex(1)] {
        let live = engine.state.instruments.instrument_index(&idx);
        let replica = replica_manager
            .replica_engine_state()
            .instruments
            .instrument_index(&idx);
        assert_eq!(replica, live, "replica/live divergence at {idx:?}");
    }

    // Explicit checks that the LIVE handler actually adjusted the option — so the parity assert
    // above is not vacuously comparing two unchanged states.
    let option_live = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_live.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
    let position = option_live.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(4));
    assert_eq!(position.price_entry_average, dec!(500));
}

/// Replica-parity for the **non-standard** option-split branch: a reverse (or fractional-forward)
/// split leaves held options UNTOUCHED (the OCC assigns a new contract identity the engine cannot
/// apply in place), so the replica's `adjust_options_in_place == false` skip branch must match the
/// live handler byte-for-byte. Complements `test_corporate_action_option_replica_parity_standard_split`,
/// which only exercises the Standard (mutating) branch — this covers the "skip" branch the author's
/// comments worry could silently drift.
#[test]
fn test_corporate_action_option_replica_parity_non_standard_split() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    // Seed the replica from the exact pre-split state.
    let pre_split_state = engine.state.clone();

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    // 1:2 reverse split (non-standard) on the Spot underlying (idx1).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-1-2-reverse".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &outputs);

    // Gold-standard: full InstrumentState parity for BOTH the untouched option (idx0) and the split
    // equity (idx1) — the replica's "skip" branch must reproduce live byte-for-byte.
    for idx in [InstrumentIndex(0), InstrumentIndex(1)] {
        let live = engine.state.instruments.instrument_index(&idx);
        let replica = replica_manager
            .replica_engine_state()
            .instruments
            .instrument_index(&idx);
        assert_eq!(replica, live, "replica/live divergence at {idx:?}");
    }

    // Non-vacuous: the LIVE handler actually took the non-standard branch — the held option was left
    // UNTOUCHED (strike + position identical to pre-split) and an identity-change signal was emitted.
    let option_live = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_live.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(
        contract.strike,
        dec!(50_000),
        "a non-standard split must NOT touch the option strike"
    );
    let position = option_live.position.positions.values().next().unwrap();
    assert_eq!(
        position.quantity_abs,
        dec!(2),
        "held option position must be untouched"
    );
    assert_eq!(position.price_entry_average, dec!(1_000));
    assert!(
        outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::OptionPositionsRequireIdentityChange { .. })),
        "a non-standard split with a held option must emit OptionPositionsRequireIdentityChange"
    );
    // The equity split itself WAS applied and its id recorded on the Spot target (idx1).
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .corporate_actions_processed
            .contains("BTCUSD-1-2-reverse")
    );
}

/// Build an engine with TWO BTC/USD call options on the same underlying plus the BTC/USD spot, for
/// the non-standard-split wrapper-handling flow. The "old" option is the one held at split time; the
/// "new" identity is **pre-declared at construction** (never runtime-registered) and only trades
/// after the wrapper migrates exposure to it.
///
/// Indices after the alphabetical `name_internal` sort:
///   InstrumentIndex(0) = old option   ("binance_btc_call_50k")
///   InstrumentIndex(1) = new identity  ("binance_btc_call_50k_new")
///   InstrumentIndex(2) = spot          ("binance_spot_btc_usd")
fn build_two_option_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> TestEngine {
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let instruments = IndexedInstruments::builder()
        // index 0: old option — the identity held across the split.
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_call_50k",
            "BTC-50000-C",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(50_000),
            }),
            None,
        ))
        // index 1: pre-declared new identity (distinct OCC contract after a non-standard split).
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_call_50k_new",
            "BTC1-33333-C",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(33_333),
            }),
            None,
        ))
        // index 2: spot underlying — the CorporateAction target.
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd",
            "BTCUSD",
            Underlying::new("btc", "usd"),
            None,
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);
    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .balances([
        (ExchangeId::BinanceSpot, "usd", STARTING_BALANCE_USDT),
        (ExchangeId::BinanceSpot, "btc", STARTING_BALANCE_BTC),
    ])
    .build();

    Engine::new(
        clock,
        state,
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]),
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

/// Build an option-engine Account `Trade` for an arbitrary instrument index, with USD-quote
/// (`AssetIndex(1)`) zero fees. Returned (not processed) so the caller can `engine.process` an open
/// or route a close fill through `process_with_audit` when the resulting `PositionExit` matters.
fn option_trade_event(
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    tag: &str,
) -> EngineEvent<DataKind> {
    EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new(tag),
            order_id: OrderId::new(tag),
            instrument: InstrumentIndex(instrument),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
            side,
            price,
            quantity,
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }))
}

/// End-to-end proof that a downstream wrapper can handle a **non-standard** split's option-identity
/// change with existing primitives only — no runtime instrument re-registration. Both the old option
/// and the new identity are pre-declared at construction; the engine emits the signal, the wrapper
/// closes the old option via the normal `Command` path, and trades the pre-declared new identity on
/// its post-split data.
///
/// NOTE: the close trigger here is *pre-planned* (the test injects the `Command` directly), not
/// *reactive-from-output* — the reactive decision logic lives in the wrapper and is unit-tested
/// there. This direct `process_with_audit` path is the only one where the observable is visible
/// (`backtest()` hardcodes `AuditMode::Disabled`); the backtest counterpart asserts plumbing only.
#[test]
fn test_corporate_action_option_non_standard_wrapper_close_and_new_identity() {
    let (execution_tx, mut execution_rx) = mpsc_unbounded();
    let mut engine = build_two_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Hold the OLD option (idx0): long 2 contracts @ premium 1000. The NEW identity (idx1) holds
    // nothing at split time.
    engine.process(option_trade_event(
        0,
        1,
        Side::Buy,
        dec!(1_000),
        dec!(2),
        "old-open",
    ));
    // A live mark on the old option — `close_open_positions_with_market_orders` only flattens
    // instruments with a current price feed (it skips stale/price-less instruments).
    engine.process(market_event_trade(2, 0, dec!(1_200)));

    // --- Step 1: a NON-STANDARD split on the spot underlying (idx2) signals an identity change. ---
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-3-2-split".into(),
        instrument: InstrumentIndex(2),
        // 3:2 fractional forward ⇒ non-standard (the OCC assigns a new contract identity).
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(1.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Exactly one identity-change signal, naming the OLD option and NOT the position-less new identity.
    let signals: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::OptionPositionsRequireIdentityChange {
                split_instrument,
                ratio,
                affected_options,
            } => Some((*split_instrument, ratio.get(), affected_options.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].0, InstrumentIndex(2));
    assert_eq!(signals[0].1, dec!(1.5));
    assert!(
        signals[0].2.contains(&InstrumentIndex(0)),
        "the held old option must be flagged"
    );
    assert!(
        !signals[0].2.contains(&InstrumentIndex(1)),
        "the position-less pre-declared new identity must NOT be flagged"
    );

    // The old option is untouched (the engine cannot mechanically adjust a non-standard split).
    let old_option = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &old_option.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(50_000));
    assert_eq!(
        old_option
            .position
            .positions
            .values()
            .next()
            .unwrap()
            .quantity_abs,
        dec!(2)
    );

    // The equity split is applied + id recorded on the Spot target (idx2) regardless.
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(2))
            .corporate_actions_processed
            .contains("BTCUSD-3-2-split")
    );

    // --- Step 2: the wrapper closes the OLD option via the normal Command path. ---
    let audit_tick = process_with_audit(&mut engine, command_close_position(0));
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };
    // A Commanded(ClosePositions) output fires, carrying a reduce-only Sell order for the old option.
    let commanded_sell = outputs.iter().any(|o| match o {
        // `Commanded` now carries `ActionOutput` inline, so the nested variant is matched directly
        // (no `Box` deref / unstable box pattern needed).
        EngineOutput::Commanded(ActionOutput::ClosePositions(out)) => {
            out.opens.sent_iter().any(|order| {
                order.key.instrument == InstrumentIndex(0) && order.state.side == Side::Sell
            })
        }
        _ => false,
    });
    assert!(
        commanded_sell,
        "expected a Commanded ClosePositions Sell for the old option"
    );
    // The close order is actually dispatched to the execution manager.
    assert!(matches!(
        execution_rx.next().unwrap(),
        ExecutionRequest::Open(order)
            if order.key.instrument == InstrumentIndex(0) && order.state.side == Side::Sell
    ));

    // Simulate the close fill: a Sell trade of the full quantity nets the old position to zero.
    let audit_tick = process_with_audit(
        &mut engine,
        option_trade_event(0, 11, Side::Sell, dec!(1_500), dec!(2), "old-close"),
    );
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::PositionExit(p) if p.instrument == InstrumentIndex(0)
        )),
        "closing the old option must emit a PositionExit"
    );
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty(),
        "the old option slot must be empty after the close fill"
    );

    // --- Step 3: the pre-declared new identity trades on its post-split data. ---
    // The new identity prints only after the split (natural — it did not exist before).
    engine.process(market_event_trade(12, 1, dec!(900)));
    engine.process(option_trade_event(
        1,
        13,
        Side::Buy,
        dec!(800),
        dec!(3),
        "new-open",
    ));

    assert_eq!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .position
            .positions
            .len(),
        1,
        "the pre-declared new identity must trade once exposure migrates to it"
    );
    // Exposure migrated entirely from the old identity to the new one — no runtime re-registration.
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// T1: ITM Put option expiry
// ---------------------------------------------------------------------------

/// Build an engine with one BTC/USD put option (strike 50_000) and one BTC/USD spot.
/// Index assignment after alphabetical sort:
///   InstrumentIndex(0) = Option  ("binance_btc_put_50k" < "binance_spot_btc_usd")
///   InstrumentIndex(1) = Spot
fn build_put_option_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> TestEngine {
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_put_50k",
            "BTC-50000-P",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Put,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(50_000),
            }),
            None,
        ))
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd",
            "BTCUSD",
            Underlying::new("btc", "usd"),
            None,
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);
    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .balances([(ExchangeId::BinanceSpot, "usd", STARTING_BALANCE_USDT)])
    .build();

    Engine::new(
        clock,
        state,
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]),
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

/// Open a long or short option position at instrument index 0.
fn open_option_position_side(
    engine: &mut TestEngine,
    side: Side,
    quantity: Decimal,
    price: Decimal,
) {
    let trade_id = match side {
        Side::Buy => TradeId::new("opt-trade-open-buy"),
        Side::Sell => TradeId::new("opt-trade-open-sell"),
    };
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: trade_id,
            order_id: gen_order_id(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
            side,
            price,
            quantity,
            // Put option instrument quote is USD = AssetIndex(1)
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }));
    engine.process(event);
}

#[test]
fn test_contract_expiry_itm_put() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_put_option_engine(TradingState::Disabled, execution_tx);

    // Spot BELOW strike (50_000) → ITM for put.
    // Intrinsic value = strike - spot = 50_000 - 45_000 = 5_000
    send_spot_price(&mut engine, 1, dec!(45_000));
    open_option_position(&mut engine, dec!(1), dec!(2_000)); // bought at 2_000 premium

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    assert_eq!(exited.len(), 1);
    // Entry: 1 * 2_000, Exit: 1 * 5_000 → pnl = 3_000
    assert_eq!(exited[0].pnl_realised, dec!(3_000));
    assert_eq!(exited[0].side, Side::Buy);
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .is_empty()
    );
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .expiration_processed
    );
}

#[test]
fn test_contract_expiry_otm_put() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_put_option_engine(TradingState::Disabled, execution_tx);

    // Spot ABOVE strike → OTM for put → settlement = 0
    send_spot_price(&mut engine, 1, dec!(55_000));
    open_option_position(&mut engine, dec!(1), dec!(2_000));

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    assert_eq!(exited.len(), 1);
    // Bought at 2_000, settled at 0 → loss of 2_000
    assert_eq!(exited[0].pnl_realised, dec!(-2_000));
}

// ---------------------------------------------------------------------------
// T2: Short position expiry
// ---------------------------------------------------------------------------

#[test]
fn test_contract_expiry_short_call_itm() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Spot ABOVE strike (50_000) → ITM for call.
    // Intrinsic = 55_000 - 50_000 = 5_000
    // Short writer must "pay" intrinsic at settlement: pnl = premium_received - intrinsic
    send_spot_price(&mut engine, 1, dec!(55_000));
    open_option_position_side(&mut engine, Side::Sell, dec!(1), dec!(2_000)); // sold at 2_000 premium

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    assert_eq!(exited.len(), 1);
    assert_eq!(exited[0].side, Side::Sell);
    // Entry (sell): +2_000 premium. Closing buy at 5_000 intrinsic → loss of 3_000.
    // pnl = 2_000 - 5_000 = -3_000
    assert_eq!(exited[0].pnl_realised, dec!(-3_000));
}

#[test]
fn test_contract_expiry_short_call_otm() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Spot BELOW strike → OTM → settlement = 0 → short writer keeps full premium
    send_spot_price(&mut engine, 1, dec!(45_000));
    open_option_position_side(&mut engine, Side::Sell, dec!(1), dec!(2_000));

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    assert_eq!(exited.len(), 1);
    assert_eq!(exited[0].side, Side::Sell);
    // Sold at 2_000, closed at 0 → profit of 2_000
    assert_eq!(exited[0].pnl_realised, dec!(2_000));
}

// ---------------------------------------------------------------------------
// T3: Hedging mode fill routing
// ---------------------------------------------------------------------------

type HedgingTestEngine = Engine<
    HistoricalClock,
    EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
    TestBuyAndHoldStrategy,
    DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
>;

fn build_hedging_option_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> HedgingTestEngine {
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_call_50k",
            "BTC-50000-C",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(50_000),
            }),
            None,
        ))
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd",
            "BTCUSD",
            Underlying::new("btc", "usd"),
            None,
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);
    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .oms_mode(OmsMode::Hedging)
    .balances([(ExchangeId::BinanceSpot, "usd", STARTING_BALANCE_USDT)])
    .build();

    Engine::new(
        clock,
        state,
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]),
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

/// Send an open order request with an explicit PositionId and return the CID used.
fn send_open_order_with_position_id(
    engine: &mut HedgingTestEngine,
    cid: ClientOrderId,
    position_id: PositionId,
    side: Side,
    price: Decimal,
    reduce_only: bool,
) {
    let request = OrderRequestOpen {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            cid,
        },
        state: RequestOpen {
            side,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            price: Some(price),
            quantity: dec!(1),
            position_id: Some(position_id),
            reduce_only,
        },
    };
    let event = EngineEvent::Command(Command::SendOpenRequests(OneOrMany::One(request)));
    engine.process(event);
}

/// Simulate the exchange acknowledging an open order (assigns exchange OrderId).
/// `side` must match the side of the original open request to reflect real exchange behaviour.
fn send_order_ack(
    engine: &mut HedgingTestEngine,
    cid: ClientOrderId,
    exchange_order_id: OrderId,
    side: Side,
) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: strategy_id(),
                cid,
            },
            side,
            price: Some(dec!(1_000)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: OrderState::active(Open {
                id: exchange_order_id,
                time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
                filled_quantity: dec!(0),
            }),
        })),
    }));
    engine.process(event);
}

/// Send a fill for an order identified by its exchange OrderId.
fn send_fill(
    engine: &mut HedgingTestEngine,
    exchange_order_id: OrderId,
    side: Side,
    price: Decimal,
) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new(format!("fill-{}", exchange_order_id.0.as_str())),
            order_id: exchange_order_id,
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 2),
            side,
            price,
            quantity: dec!(1),
            // BTCUSDT (instrument 0) quote = usdt = AssetIndex(2). Fee is zero here regardless, but
            // use the correct quote asset for the instrument this helper always fills (index 0).
            fees: asset_fees(0, Decimal::ZERO),
        }),
    }));
    engine.process(event);
}

/// Simulate the exchange confirming that an order was fully filled (terminal snapshot).
///
/// This removes the order from `orders.0` (via `Orders::update_from_order_snapshot`)
/// and triggers `cleanup_routing_tables`, which prunes the corresponding CID entries
/// from `position_ids` and `exchange_id_to_cid`. Call this after `send_fill` to mirror
/// real exchange behaviour: exchanges send both a Trade event AND an updated OrderSnapshot
/// once an order is fully filled.
fn send_fully_filled_snapshot(engine: &mut HedgingTestEngine, cid: ClientOrderId) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: strategy_id(),
                cid,
            },
            side: Side::Buy, // side is unused by the terminal-state transition
            price: Some(dec!(0)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: OrderState::fully_filled(Filled::new(
                OrderId::new("test_order"),
                time_plus_days(STARTING_TIMESTAMP, 2),
                dec!(1),
                None,
            )),
        })),
    }));
    engine.process(event);
}

#[test]
fn test_hedging_fill_routing_to_correct_position_id() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    let cid_a = ClientOrderId::new("cid-a");
    let pos_id_a = PositionId::new("leg-a");
    let exchange_id_a = OrderId::new("exch-a");

    // Submit order with explicit PositionId → populates position_ids map.
    send_open_order_with_position_id(
        &mut engine,
        cid_a.clone(),
        pos_id_a.clone(),
        Side::Buy,
        dec!(1_000),
        false,
    );

    // Verify CID→PositionId was recorded.
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position_ids
            .contains_key(&cid_a)
    );

    // Exchange ack: order now has an exchange OrderId.
    send_order_ack(&mut engine, cid_a.clone(), exchange_id_a.clone(), Side::Buy);

    // Fill arrives with exchange OrderId → routes to pos_id_a.
    send_fill(&mut engine, exchange_id_a, Side::Buy, dec!(1_000));

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        instr.position.positions.contains_key(&pos_id_a),
        "position should exist under the caller-supplied PositionId"
    );
    assert_eq!(instr.position.positions.len(), 1);
}

#[test]
fn test_hedging_fill_routing_fallback_for_unknown_order() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    // Send fill with an OrderId that has no matching entry in orders map.
    let unknown_order_id = OrderId::new("external-order-99");
    send_fill(
        &mut engine,
        unknown_order_id.clone(),
        Side::Buy,
        dec!(1_000),
    );

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    // Fallback: position opened under the raw order ID.
    let expected_pos_id = PositionId::new(unknown_order_id.0.clone());
    assert!(
        instr.position.positions.contains_key(&expected_pos_id),
        "fallback should open position under raw order ID"
    );
}

#[test]
fn test_hedging_position_ids_cleanup_on_position_exit() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    let cid_a = ClientOrderId::new("cid-a");
    let pos_id_a = PositionId::new("leg-a");
    let exchange_id_a = OrderId::new("exch-a");

    // Open a position.
    send_open_order_with_position_id(
        &mut engine,
        cid_a.clone(),
        pos_id_a.clone(),
        Side::Buy,
        dec!(1_000),
        false,
    );
    send_order_ack(&mut engine, cid_a.clone(), exchange_id_a.clone(), Side::Buy);
    send_fill(&mut engine, exchange_id_a.clone(), Side::Buy, dec!(1_000));
    // Exchange confirms cid_a is fully filled — removes it from orders.0 and cleans up routing tables.
    send_fully_filled_snapshot(&mut engine, cid_a.clone());

    // Close the same position with a sell fill using a new CID/order.
    let cid_b = ClientOrderId::new("cid-b");
    let pos_id_b_same = pos_id_a.clone(); // deliberately route close to same position
    let exchange_id_b = OrderId::new("exch-b");
    send_open_order_with_position_id(
        &mut engine,
        cid_b.clone(),
        pos_id_b_same,
        Side::Sell,
        dec!(2_000),
        true,
    );
    send_order_ack(
        &mut engine,
        cid_b.clone(),
        exchange_id_b.clone(),
        Side::Sell,
    );
    send_fill(&mut engine, exchange_id_b, Side::Sell, dec!(2_000));
    // Exchange confirms cid_b is fully filled — removes it from orders.0 and cleans up routing tables.
    send_fully_filled_snapshot(&mut engine, cid_b.clone());

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    // Position exited — no open positions.
    assert!(
        instr.position.positions.is_empty(),
        "position should be closed"
    );
    // All position_ids entries that routed to the closed position are cleaned up once
    // both orders' terminal snapshots have arrived (mirroring real exchange behaviour).
    assert!(
        !instr.position_ids.values().any(|v| *v == pos_id_a),
        "position_ids entries for closed position should be removed"
    );
}

// ---------------------------------------------------------------------------
// T4: Multi-position Hedging expiry
// ---------------------------------------------------------------------------

#[test]
fn test_contract_expiry_hedging_multi_position() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    // Set spot price above strike → ITM, intrinsic = 5_000
    send_spot_price(&mut engine, 1, dec!(55_000));

    // Open two independent long positions (leg-a and leg-b).
    let cid_a = ClientOrderId::new("cid-a");
    let pos_id_a = PositionId::new("leg-a");
    let exchange_id_a = OrderId::new("exch-a");

    let cid_b = ClientOrderId::new("cid-b");
    let pos_id_b = PositionId::new("leg-b");
    let exchange_id_b = OrderId::new("exch-b");

    send_open_order_with_position_id(
        &mut engine,
        cid_a.clone(),
        pos_id_a.clone(),
        Side::Buy,
        dec!(2_000),
        false,
    );
    send_order_ack(&mut engine, cid_a, exchange_id_a.clone(), Side::Buy);
    send_fill(&mut engine, exchange_id_a, Side::Buy, dec!(2_000));

    send_open_order_with_position_id(
        &mut engine,
        cid_b.clone(),
        pos_id_b.clone(),
        Side::Buy,
        dec!(3_000),
        false,
    );
    send_order_ack(&mut engine, cid_b, exchange_id_b.clone(), Side::Buy);
    send_fill(&mut engine, exchange_id_b, Side::Buy, dec!(3_000));

    assert_eq!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .position
            .positions
            .len(),
        2,
        "two open positions before expiry"
    );

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));

    // Both positions must be settled.
    assert_eq!(
        exited.len(),
        2,
        "both positions should be settled at expiry"
    );

    // Collect pnls regardless of order.
    let mut pnls: Vec<Decimal> = exited.iter().map(|e| e.pnl_realised).collect();
    pnls.sort();
    // leg-a: bought 2_000, settled 5_000 → +3_000
    // leg-b: bought 3_000, settled 5_000 → +2_000
    assert_eq!(pnls, vec![dec!(2_000), dec!(3_000)]);

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(instr.position.positions.is_empty());
    assert!(instr.expiration_processed);
    // position_ids must be cleared post-expiry (H2 fix).
    assert!(instr.position_ids.is_empty());
}

// ---------------------------------------------------------------------------
// T5: FeeModelConfig::PerContract integration through InstrumentState
// ---------------------------------------------------------------------------

#[test]
fn test_fee_model_per_contract_augments_trade_fees() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Enable PerContract fee model: $0.65 per contract.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(0))
        .fee_model = FeeModelConfig::PerContract(PerContractFeeModel {
        commission_per_contract: dec!(0.65),
    });

    // Open a position via a fill — fee reported by exchange is 0, but PerContract
    // should augment it with 1 contract × $0.65 = $0.65.
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new("fee-test-trade"),
            order_id: gen_order_id(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
            side: Side::Buy,
            price: dec!(1_000),
            quantity: dec!(1),
            // Option engine: quote is USD = AssetIndex(1). Exchange reports zero commission.
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }));
    engine.process(event);

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let pos = instr
        .position
        .positions
        .get(&PositionId::NETTING) // Netting mode engine
        .expect("position should be open");

    // fees_enter should reflect the PerContract commission (0.65 per contract × 1 contract).
    assert_eq!(pos.fees_enter.fees, dec!(0.65));
    // pnl_realised starts negative equal to fees paid.
    assert_eq!(pos.pnl_realised, dec!(-0.65));
}

#[test]
fn test_fee_model_zero_no_fees_on_trade() {
    let (execution_tx, _) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);
    // Default fee model is Zero — exchange-reported zero fees stay zero.
    open_option_position(&mut engine, dec!(1), dec!(1_000));

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let pos = instr
        .position
        .positions
        .get(&PositionId::NETTING)
        .expect("position should be open");
    assert_eq!(pos.fees_enter.fees, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// T6: pending_fills mechanism tests
// ---------------------------------------------------------------------------

/// Helper: send a cancel ack for an order (marks it as cancelled via OrderResponseCancel).
fn send_cancel_ack(engine: &mut HedgingTestEngine, cid: ClientOrderId, exchange_order_id: OrderId) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::OrderCancelled(OrderResponseCancel {
            key: OrderKey {
                exchange: ExchangeIndex(0),
                instrument: InstrumentIndex(0),
                strategy: strategy_id(),
                cid,
            },
            state: Ok(Cancelled {
                id: exchange_order_id,
                time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
                filled_quantity: dec!(0),
            }),
        }),
    }));
    engine.process(event);
}

/// Tests the core pending_fills mechanism: fill arrives before ack, gets buffered,
/// then replayed when ack arrives, creating position under the correct PositionId.
#[test]
fn test_hedging_pending_fill_replayed_on_ack() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    let cid = ClientOrderId::new("cid-pending");
    let pos_id = PositionId::new("leg-pending");
    let exchange_id = OrderId::new("exch-pending");

    // Step 1: Submit order — creates OpenInFlight state, records position_ids[cid] = pos_id.
    send_open_order_with_position_id(
        &mut engine,
        cid.clone(),
        pos_id.clone(),
        Side::Buy,
        dec!(1_000),
        false,
    );

    // Step 2: Fill arrives BEFORE ack — should be buffered in pending_fills.
    send_fill(&mut engine, exchange_id.clone(), Side::Buy, dec!(1_000));

    // Verify: no position yet (fill is buffered), pending_fills should have the fill.
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        instr.position.positions.is_empty(),
        "position should NOT be created yet — fill is pending"
    );
    assert_eq!(
        instr.pending_fills.len(),
        1,
        "fill should be buffered in pending_fills"
    );

    // Step 3: Ack arrives — should replay the pending fill and create position.
    send_order_ack(&mut engine, cid.clone(), exchange_id.clone(), Side::Buy);

    // Verify: position now exists under the correct PositionId, pending_fills drained.
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        instr.position.positions.contains_key(&pos_id),
        "position should exist under caller-supplied PositionId after ack"
    );
    assert!(
        instr.pending_fills.is_empty(),
        "pending_fills should be drained after replay"
    );
}

/// Tests that pending_fills is drained safely when cancel ack arrives instead of open ack.
/// This prevents unbounded accumulation of orphaned fills.
#[test]
fn test_hedging_pending_fill_drained_on_cancel_ack() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    let cid = ClientOrderId::new("cid-cancel-race");
    let pos_id = PositionId::new("leg-cancel-race");
    let exchange_id = OrderId::new("exch-cancel-race");

    // Step 1: Submit order.
    send_open_order_with_position_id(
        &mut engine,
        cid.clone(),
        pos_id.clone(),
        Side::Buy,
        dec!(1_000),
        false,
    );

    // Step 2: Fill arrives before any ack — buffered.
    send_fill(&mut engine, exchange_id.clone(), Side::Buy, dec!(1_000));

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instr.pending_fills.len(), 1, "fill should be buffered");

    // Step 3: Cancel ack arrives (order was cancelled, not opened).
    // This simulates: user submitted order, exchange filled it, then user cancelled,
    // but the cancel ack arrived before the open ack (race condition).
    send_cancel_ack(&mut engine, cid.clone(), exchange_id.clone());

    // Verify: pending_fills cleared, no position created (the fill is orphaned).
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    // Note: with current logic, pending_fills is only drained when no OpenInFlight orders remain.
    // After cancel ack, the order is removed from orders.0, so no OpenInFlight remains.
    // The drain path in update_from_cancel checks `still_has_in_flight` and clears if false.
    assert!(
        instr.pending_fills.is_empty(),
        "pending_fills should be cleared when no OpenInFlight orders remain"
    );
}

/// Tests that pending_fills is cleared during contract expiry.
/// Orphaned fills from in-progress fill-before-ack races must not accumulate.
#[test]
fn test_contract_expiry_clears_pending_fills() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_hedging_option_engine(TradingState::Disabled, execution_tx);

    let cid = ClientOrderId::new("cid-expiry-pending");
    let pos_id = PositionId::new("leg-expiry-pending");
    let exchange_id = OrderId::new("exch-expiry-pending");

    // Submit order and send fill before ack — creates pending_fills entry.
    send_open_order_with_position_id(
        &mut engine,
        cid.clone(),
        pos_id.clone(),
        Side::Buy,
        dec!(1_000),
        false,
    );
    send_fill(&mut engine, exchange_id.clone(), Side::Buy, dec!(1_000));

    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instr.pending_fills.len(), 1, "setup: pending fill exists");

    // Set spot price for ITM settlement and trigger expiry.
    send_spot_price(&mut engine, 1, dec!(55_000)); // ITM for strike 50_000

    let expiry_event = EngineEvent::ContractExpiry(InstrumentIndex(0));
    engine.process(expiry_event);

    // Verify: expiry processed to completion (not early-returned due to missing spot price).
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        instr.expiration_processed,
        "expiry should be processed — if this fails, the spot price lookup failed"
    );
    // pending_fills cleared by expiry cleanup at line 601 in engine/mod.rs.
    assert!(
        instr.pending_fills.is_empty(),
        "pending_fills must be cleared during contract expiry"
    );
}

// ---------------------------------------------------------------------------
// CorporateAction — additional audit-path coverage:
//   equity-leg OpenOrdersAtSplit, forward split on a populated spot, the
//   Fractional rounding policy, a post-split unheld-option strike fix, and the
//   hedging routing-table prune on a floor-to-zero reverse split.
// ---------------------------------------------------------------------------

/// Equity/spot path: a resting order on the splitting instrument is surfaced via
/// `OpenOrdersAtSplit` (and left in the book — a broker price-adjusts it, the engine never cancels).
/// The option path already asserts this observable; this pins it on the equity leg too.
#[test]
fn test_corporate_action_spot_split_surfaces_open_orders_at_split() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx); // Netting spot

    // Last price for the eager pnl recompute, plus a long position on Spot instrument 0.
    engine.process(market_event_trade(1, 0, dec!(100)));
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(10), "pos");

    // A resting order on the same instrument (cid = gen_cid(0)), left unfilled (0 of 1 filled).
    engine.process(account_event_order_response(0, 1, Side::Buy, 1.0, 0.0));

    // 1:2 reverse split on instrument 0.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-reverse-1-2".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The resting order is surfaced for the equity instrument, carrying its pre-split terms.
    let surfaced = outputs
        .iter()
        .find_map(|o| match o {
            EngineOutput::OpenOrdersAtSplit { instrument, orders }
                if *instrument == InstrumentIndex(0) =>
            {
                Some(orders.clone())
            }
            _ => None,
        })
        .expect("equity split must surface the instrument's resting orders via OpenOrdersAtSplit");
    assert!(
        surfaced
            .iter()
            .any(|order| order.cid == gen_cid(0) && order.quantity_pre_split == dec!(1)),
        "OpenOrdersAtSplit must carry the resting order at its pre-split quantity"
    );

    // The order is NOT cancelled — it still rests in the engine's order book post-split.
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .orders
            .0
            .contains_key(&gen_cid(0)),
        "the resting order must be left in place (surfaced, not cancelled)"
    );
}

/// Forward split (`ratio > 1`) on a populated spot position via the audit path: contracts scale up,
/// basis scales down, the high-water mark scales (unfloored), and pnl is recomputed from the mark —
/// with NO `SplitRemainder` (an integer ratio on a whole quantity disposes nothing). Existing
/// coverage exercises only reverse (`0.5`) splits on populated spot, or forward on empty spot.
#[test]
fn test_corporate_action_forward_split_populated_spot() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx); // Netting spot

    // Mark 100, long 10 @ 100 on Spot instrument 0.
    engine.process(market_event_trade(1, 0, dec!(100)));
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(10), "pos");

    // 2:1 forward split.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-forward-2-1".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Position scaled: qty 10×2 = 20, avg 100/2 = 50, high-water 10×2 = 20 (unfloored),
    // pnl_unrealised recomputed from the mark 100 = (100 - 50) × 20 = 1000.
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert_eq!(instrument_state.position.positions.len(), 1);
    let position = instrument_state.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(20));
    assert_eq!(position.price_entry_average, dec!(50));
    assert_eq!(position.quantity_abs_max, dec!(20));
    assert_eq!(position.pnl_unrealised, dec!(1000));

    // A whole-number forward split on a whole quantity disposes no fractional sliver.
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::SplitRemainder { .. })),
        "a forward split on a whole quantity must not emit a SplitRemainder"
    );

    // The split id is recorded on the target.
    assert!(
        instrument_state
            .corporate_actions_processed
            .contains("BTCUSDT-forward-2-1")
    );
}

/// `SplitRoundingPolicy::Fractional` (fractional-share brokers, e.g. Alpaca) keeps the exact
/// fractional share count: the quantity is NOT floored and NO `SplitRemainder` is emitted, even when
/// the scaled quantity is fractional. Previously only smoke-tested through the audit-disabled
/// backtest path; asserted here on the observable audit path, the `Floor` counterpart's mirror.
#[test]
fn test_corporate_action_fractional_policy_keeps_fractional_quantity() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx); // Netting spot

    // Mark 100, long 21 @ 100 on Spot instrument 0.
    engine.process(market_event_trade(1, 0, dec!(100)));
    open_position_via_trade(&mut engine, 0, 2, Side::Buy, dec!(100), dec!(21), "pos");

    // 1:2 reverse split under Fractional: 21 × 0.5 = 10.5 kept (NOT floored to 10).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-reverse-frac".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Fractional,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The fractional quantity is preserved exactly: qty 10.5, avg 100/0.5 = 200,
    // pnl_unrealised = (100 - 200) × 10.5 = -1050.
    let instrument_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let position = instrument_state.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(10.5));
    assert_eq!(position.price_entry_average, dec!(200));
    assert_eq!(position.pnl_unrealised, dec!(-1050));

    // Fractional disposes nothing — no cash-in-lieu observable, and the slot is not floored to zero.
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::SplitRemainder { .. })),
        "Fractional policy must not dispose a remainder (no SplitRemainder)"
    );
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::PositionExit(_))),
        "a Fractional reverse split must not close the position"
    );
}

/// A standard split silently corrects the strike of an UNHELD option on the underlying (a registry
/// fix, no observable), so a position opened on that option AFTER the split settles at expiry against
/// the POST-split strike. Proves the strike adjustment is not gated on holding a position at split
/// time — the instrument set is fixed at construction, so an unheld option can be traded later.
#[test]
fn test_corporate_action_unheld_option_strike_adjusted_then_settles_post_split() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Marks for the option (idx0) and spot underlying (idx1). NO option position is opened — the
    // option is unheld at split time.
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // 2:1 standard forward split on the Spot underlying (idx1).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The unheld option's strike was silently halved (50_000 → 25_000) ...
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
    // ... with NO per-position observable (nothing was held to adjust).
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::OptionPositionAdjustedForSplit { .. })),
        "an unheld option's strike fix must be silent (no OptionPositionAdjustedForSplit)"
    );

    // Now open a fresh position on the option (post-split) and settle it at expiry. Spot at 30_000 is
    // ITM against the POST-split strike 25_000 (intrinsic 5_000) but would be OTM against the stale
    // 50_000 strike — so the settlement pnl pins which strike was used.
    open_option_position_via_trade(
        &mut engine,
        11,
        Side::Buy,
        dec!(1_000),
        dec!(1),
        "post-split",
    );
    engine.process(market_event_trade(12, 1, dec!(30_000)));

    let exited = engine.process_contract_expiry(&InstrumentIndex(0));
    assert_eq!(exited.len(), 1);
    // Intrinsic 5_000 (= 30_000 − post-split strike 25_000), premium 1_000, 1 contract:
    // pnl = (5_000 − 1_000) × 1 = 4_000. Against the stale 50_000 strike it would be −1_000.
    assert_eq!(exited[0].pnl_realised, dec!(4_000));
}

/// An **unheld** option (zero positions) that carries a **resting order** must still have that order
/// surfaced via `OpenOrdersAtSplit` when its underlying splits — mirroring the equity leg, which
/// snapshots resting orders regardless of position state. A working order to *open* an option
/// position is a realistic pre-split state; the standard split silently corrects the strike, so the
/// resting order is now against the adjusted contract at a stale premium and the wrapper must be told
/// (and a backtest MockExchange would otherwise fill it against the post-split print). Regression: the
/// option loop previously `continue`d on `positions.is_empty()` BEFORE snapshotting orders.
#[test]
fn test_corporate_action_unheld_option_with_resting_order_surfaces_open_orders_at_split() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // Marks for the option (idx0) and spot underlying (idx1). NO option position is opened.
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    // A resting order on the UNHELD option (idx0) — a working order to open a position.
    engine.process(account_event_order_response(0, 1, Side::Buy, 1.0, 0.0));

    // 2:1 standard forward split on the Spot underlying (idx1).
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The resting order on the UNHELD option IS surfaced (the fix — equity-leg parity).
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OpenOrdersAtSplit { instrument, orders }
                if *instrument == InstrumentIndex(0) && !orders.is_empty()
        )),
        "an unheld option's resting order must be surfaced via OpenOrdersAtSplit"
    );

    // No per-position observable (nothing was held to adjust) ...
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::OptionPositionAdjustedForSplit { .. })),
        "an unheld option has no position to adjust"
    );

    // ... and the strike was still corrected (the registry fix runs regardless of position state).
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
}

/// A **rejected** CorporateAction still advances the historical clock to its `effective_time` (the
/// clock is processed before the handler validates the action), but this does NOT skew the clock for
/// subsequent in-order events — a later market event still advances it. On the supported backtest
/// path the merge enforces `effective_time == Timed::time` and emits the action only when
/// `effective_time <= the next market event`, so `effective_time` is always ≤ every later event and a
/// rejected action causes no "permanent skew". This locks that behavior in: moving the clock advance
/// to accept-only would flip the second assertion and force a conscious re-decision.
#[test]
fn test_corporate_action_rejected_does_not_skew_clock() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    // idx0 = option, idx1 = spot. Targeting the option rejects with InstrumentKindNotSupported.
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // A pre-split market event establishes the clock near STARTING+1d (< effective_time).
    let m1 = process_with_audit(&mut engine, market_event_trade(1, 1, dec!(60_000)));
    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    assert!(m1.context.time < effective_time);

    // A CorporateAction targeting the OPTION (idx0) → rejected (InstrumentKindNotSupported).
    let ca_event = EngineEvent::CorporateAction {
        id: "reject-me".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let ca_tick = process_with_audit(&mut engine, ca_event);
    let ca_outputs = match &ca_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };
    // Non-vacuous: the action really was rejected (nothing mutated, id not recorded).
    assert!(
        ca_outputs.iter().any(|o| matches!(
            o,
            EngineOutput::UnsupportedCorporateAction {
                reason: UnsupportedCorporateActionReason::InstrumentKindNotSupported,
                ..
            }
        )),
        "targeting the option must be rejected as InstrumentKindNotSupported"
    );
    // Despite the rejection, the clock advanced to effective_time (current, intended behavior).
    assert!(
        ca_tick.context.time >= effective_time,
        "a rejected CorporateAction still advances the clock to effective_time"
    );

    // A later, in-order market event still advances the clock past effective_time — no skew.
    let later_time = time_plus_days(STARTING_TIMESTAMP, 20);
    let m2 = process_with_audit(&mut engine, market_event_trade(20, 1, dec!(70_000)));
    assert!(
        m2.context.time >= later_time,
        "a later market event after a rejected action must still advance the clock (no permanent skew)"
    );
    assert!(
        m2.context.time >= ca_tick.context.time,
        "clock must remain monotonic non-decreasing across a rejected action"
    );
}

/// A reverse split that floors a Hedging position to zero must prune the `position_ids` routing entry
/// by VALUE (the removed `PositionId`), not via the `cleanup_routing_tables` CID-retention path: the
/// split deliberately leaves the position's resting order in place, so its CID is still in `orders`.
/// Without the value-prune a late fill on that order would resolve the dead id and silently REOPEN
/// the floored-out position.
#[test]
fn test_corporate_action_floor_to_zero_prunes_hedging_routing() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine_with_oms(TradingState::Disabled, execution_tx, OmsMode::Hedging);

    // Last price for the eager pnl recompute on Spot instrument 0.
    engine.process(market_event_trade(1, 0, dec!(100)));

    // Open a Hedging position routed through an explicit PositionId, leaving the order RESTING (acked
    // + filled, but with no terminal fully-filled snapshot ⇒ the CID stays in `orders`).
    let cid = ClientOrderId::new("cid-floor");
    let pos_id = PositionId::new("leg-floor");
    let exchange_id = OrderId::new("exch-floor");
    send_open_order_with_position_id(
        &mut engine,
        cid.clone(),
        pos_id.clone(),
        Side::Buy,
        dec!(100),
        false,
    );
    send_order_ack(&mut engine, cid.clone(), exchange_id.clone(), Side::Buy);
    send_fill(&mut engine, exchange_id.clone(), Side::Buy, dec!(100));

    // Pre-split sanity: position open, routing entry present, order resting.
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(instr.position.positions.contains_key(&pos_id));
    assert!(instr.position_ids.values().any(|v| *v == pos_id));
    assert!(instr.orders.0.contains_key(&cid));

    // 1:2 reverse split under Floor: qty 1 → floor(0.5) = 0 ⇒ slot removed.
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSDT-floor-to-zero".into(),
        instrument: InstrumentIndex(0),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(0.5)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The floored position exited, and its routing entry was pruned by value ...
    assert!(
        outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::PositionExit(p) if p.position_id == pos_id)),
        "the floored-to-zero position must emit a PositionExit"
    );
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        !instr.position.positions.contains_key(&pos_id),
        "floored position must be removed"
    );
    assert!(
        !instr.position_ids.values().any(|v| *v == pos_id),
        "the floored position's routing entry must be pruned by value"
    );
    // ... while the resting order itself is left in place (the split price-adjusts, never cancels).
    assert!(
        instr.orders.0.contains_key(&cid),
        "the split must leave the resting order in the book"
    );

    // A late fill on that still-resting order must NOT resurrect the floored PositionId. With the
    // routing entry pruned, the fill routes via the no-mapping fallback to a position keyed by the
    // raw exchange OrderId — a fresh slot, never the dead `leg-floor`.
    send_fill(&mut engine, exchange_id.clone(), Side::Buy, dec!(100));
    let instr = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    assert!(
        !instr.position.positions.contains_key(&pos_id),
        "a late fill must not silently reopen the floored-out position"
    );
    assert!(
        instr
            .position
            .positions
            .contains_key(&PositionId::new(exchange_id.0.clone())),
        "the late fill opens a fresh slot under the raw order id (no-mapping fallback)"
    );
}

/// Engine whose instrument set contains **two** split-eligible (`Spot`) instruments on the *same*
/// `(exchange, base, quote)` identity, plus one option written on that underlying.
///
/// Post-sort indices (instruments on one exchange sort by `name_internal`):
/// - `0` — the option `BTC-50000-C`
/// - `1` — spot `BTCUSD`
/// - `2` — spot `BTCUSD.ALT`, the second deliverable on the identical underlying
///
/// This is the registry shape a corporate action cannot be resolved against: either spot is an
/// equally valid trigger for adjusting index 0, and nothing in the state says which one the option
/// is written on.
fn build_ambiguous_underlying_engine(
    trading_state: TradingState,
    execution_tx: UnboundedTx<ExecutionRequest>,
) -> TestEngine {
    let expiry = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_btc_call_50k",
            "BTC-50000-C",
            Underlying::new("btc", "usd"),
            rustrade_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Option(OptionContract {
                contract_size: dec!(1),
                settlement_asset: "usd".into(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: dec!(50_000),
            }),
            None,
        ))
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd",
            "BTCUSD",
            Underlying::new("btc", "usd"),
            None,
        ))
        // Same exchange, same underlying pair, different listing — the ambiguity under test.
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usd_alt",
            "BTCUSD.ALT",
            Underlying::new("btc", "usd"),
            None,
        ))
        .build();

    let clock = HistoricalClock::new(STARTING_TIMESTAMP);

    let state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(STARTING_TIMESTAMP)
    .trading_state(trading_state)
    .oms_mode(OmsMode::Netting)
    .balances([
        (ExchangeId::BinanceSpot, "usd", STARTING_BALANCE_USDT),
        (ExchangeId::BinanceSpot, "btc", STARTING_BALANCE_BTC),
    ])
    .build();

    let execution_txs =
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]);

    Engine::new(
        clock,
        state,
        execution_txs,
        TestBuyAndHoldStrategy { id: strategy_id() },
        DefaultRiskManager::default(),
    )
}

/// Open a position on an arbitrary instrument index (the option helpers above are pinned to idx0).
fn open_position_on(
    engine: &mut TestEngine,
    instrument: usize,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    tag: &str,
) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new(tag),
            order_id: OrderId::new(tag),
            instrument: InstrumentIndex(instrument),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
            side,
            price,
            quantity,
            // Quote asset of every instrument in the ambiguous fixture is usd = AssetIndex(1).
            fees: AssetFees::new(AssetIndex(1), Decimal::ZERO, Some(Decimal::ZERO)),
        }),
    }));
    engine.process(event);
}

/// A split whose target is **not the unique** split-eligible instrument on its
/// `(base, quote, exchange)` is rejected with `AmbiguousSplitTarget` before anything is mutated.
///
/// The option chain a split must adjust is resolved by that identity alone, so a second deliverable
/// listing is an equally valid trigger for adjusting the *same* chain — applying either would divide
/// every strike on it once per trigger, silently for unheld options and recorded only against
/// whichever instrument happened to be named. Rejecting is the only sound answer: nothing in the
/// state can say which listing the options are written on.
#[test]
fn test_corporate_action_ambiguous_split_target_rejected_without_mutating() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_ambiguous_underlying_engine(TradingState::Disabled, execution_tx);

    // Pin the post-sort layout the assertions below index by.
    assert!(matches!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .instrument
            .kind,
        InstrumentKind::Option(_)
    ));

    // Marks for the option (idx0) and both spot listings (idx1, idx2).
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    engine.process(market_event_trade(1, 2, dec!(60_000)));

    // Held positions on the option and on the targeted spot, so a partial application would show.
    open_position_on(&mut engine, 0, Side::Buy, dec!(1_000), dec!(2), "opt-open");
    open_position_on(
        &mut engine,
        1,
        Side::Buy,
        dec!(50_000),
        dec!(3),
        "spot-open",
    );

    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // Rejected, attributed to the named target, with the ambiguity reason.
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::UnsupportedCorporateAction {
                instrument,
                reason: UnsupportedCorporateActionReason::AmbiguousSplitTarget,
                ..
            } if *instrument == InstrumentIndex(1)
        )),
        "an ambiguous split target must be rejected as AmbiguousSplitTarget"
    );

    // Nothing was mutated — not the targeted equity's position ...
    let spot_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(1));
    let spot_position = spot_state.position.positions.values().next().unwrap();
    assert_eq!(spot_position.quantity_abs, dec!(3));
    assert_eq!(spot_position.price_entry_average, dec!(50_000));

    // ... nor the option chain the ambiguity makes unresolvable.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(50_000));
    let option_position = option_state.position.positions.values().next().unwrap();
    assert_eq!(option_position.quantity_abs, dec!(2));

    // The `id` is recorded nowhere, so the action stays retryable once the registry is corrected.
    for idx in [InstrumentIndex(0), InstrumentIndex(1), InstrumentIndex(2)] {
        assert!(
            !engine
                .state
                .instruments
                .instrument_index(&idx)
                .corporate_actions_processed
                .contains("BTCUSD-2-1-split"),
            "a rejected action must record its id nowhere ({idx:?})"
        );
    }

    // And no mutation observable was emitted alongside the rejection.
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::OptionPositionAdjustedForSplit { .. }
                | EngineOutput::SplitRemainder { .. }
                | EngineOutput::OpenOrdersAtSplit { .. }
                | EngineOutput::OptionPositionsRequireIdentityChange { .. }
        )),
        "a rejected split must emit no mutation observable"
    );

    // The attack the per-option `id` record alone does NOT close: a wrapper emitting a DISTINCT id
    // per held instrument defeats every idempotency guard, because no guard has seen that id before.
    // Naming the *other* listing with a different id is the same ambiguity wearing a different hat,
    // and must be rejected on the same grounds -- the uniqueness check reads only
    // `(base, quote, exchange)`, never the id, which is what makes it immune here.
    let audit_tick = process_with_audit(
        &mut engine,
        EngineEvent::CorporateAction {
            id: "BTCUSD-2-1-split-listing-2".into(),
            instrument: InstrumentIndex(2),
            kind: CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(dec!(2)).unwrap(),
            },
            policy: SplitRoundingPolicy::Floor,
            effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
        },
    );
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::UnsupportedCorporateAction {
                instrument,
                reason: UnsupportedCorporateActionReason::AmbiguousSplitTarget,
                ..
            } if *instrument == InstrumentIndex(2)
        )),
        "a fresh id against the second ambiguous listing must be rejected on the same grounds"
    );

    // Still untouched -- in particular the option chain, which a second silent pass would have
    // adjusted a second time.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(50_000));
}

/// A target that is **both** ambiguous and arithmetically un-rescalable is reported as
/// `AmbiguousSplitTarget`, pinning the order the two guards run in.
///
/// `prepare_corporate_action_split` scans for a second split-eligible listing *before* it walks the
/// equity positions, so the reason a caller receives names the condition it can actually act on:
/// the instrument registry is ambiguous, and no rescaling arithmetic — overflowing or not — was
/// ever reachable. Reversed, the same action would be reported as `ArithmeticOverflow`, sending a
/// caller after a position quantity when the registry is what needs correcting. Every other split
/// fixture triggers exactly one guard, so nothing else distinguishes the two orderings.
#[test]
fn test_corporate_action_ambiguity_is_reported_ahead_of_overflow() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_ambiguous_underlying_engine(TradingState::Disabled, execution_tx);

    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_position_on(
        &mut engine,
        1,
        Side::Buy,
        dec!(50_000),
        dec!(3),
        "spot-open",
    );

    // Plant the same adversarial quantity as `test_corporate_action_pre_validation_rejects_
    // arithmetic_overflow`, so the equity-position pass would overflow if it were ever reached.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(1))
        .position
        .positions
        .values_mut()
        .next()
        .expect("spot position should exist")
        .quantity_abs = Decimal::MAX;

    let audit_tick = process_with_audit(
        &mut engine,
        EngineEvent::CorporateAction {
            id: "BTCUSD-2-1-split-ambiguous-and-overflowing".into(),
            instrument: InstrumentIndex(1),
            kind: CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(dec!(2)).unwrap(),
            },
            policy: SplitRoundingPolicy::Floor,
            effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
        },
    );
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    let rejections: Vec<_> = outputs
        .iter()
        .filter_map(|o| match o {
            EngineOutput::UnsupportedCorporateAction {
                instrument, reason, ..
            } => Some((*instrument, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(rejections.len(), 1, "expected exactly one rejection");
    assert_eq!(rejections[0].0, InstrumentIndex(1));
    assert_eq!(
        rejections[0].1,
        UnsupportedCorporateActionReason::AmbiguousSplitTarget,
        "the ambiguity guard must run before the position arithmetic"
    );

    // Atomic on both counts: the planted quantity is left exactly as planted.
    assert_eq!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .position
            .positions
            .values()
            .next()
            .unwrap()
            .quantity_abs,
        Decimal::MAX
    );
}

/// A **held** option adjusted by a standard split records that action's `id` in its **own**
/// `corporate_actions_processed`, and a second delivery of the same `id` therefore leaves it alone.
///
/// The target's set is cleared in between to reproduce the door this closes: a same-`id` replay
/// whose record survived on the options but not on the equity (e.g. state restored from a snapshot
/// taken before the field existed). Without the per-option record the chain is re-adjusted —
/// strike divided twice, contracts doubled twice — with the equity's guard powerless to stop it.
#[test]
fn test_corporate_action_option_already_carrying_the_id_is_not_readjusted() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };

    // First delivery: the option is adjusted in place AND records the id itself.
    process_with_audit(&mut engine, ca_event.clone());
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
    assert!(
        option_state
            .corporate_actions_processed
            .contains("BTCUSD-2-1-split"),
        "an adjusted option must record the action id on its own set"
    );

    // Lose the TARGET's record only — its idempotency guard can no longer fire.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(1))
        .corporate_actions_processed
        .clear();

    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    // The option is untouched by the second pass: strike NOT halved again, contracts NOT doubled.
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
    let position = option_state.position.positions.values().next().unwrap();
    assert_eq!(position.quantity_abs, dec!(4));
    assert_eq!(position.price_entry_average, dec!(500));

    // The suppression is observable, not silent — per skipped option.
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::CorporateActionAlreadyProcessed { instrument, .. }
                if *instrument == InstrumentIndex(0)
        )),
        "a suppressed option re-adjust must be surfaced, not silently dropped"
    );
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, EngineOutput::OptionPositionAdjustedForSplit { .. })),
        "a suppressed option must emit no adjustment observable"
    );

    // The equity leg still ran (its own guard was cleared) — the option protection is independent
    // of the target's record, which is the whole point of keeping it per-option.
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(1))
            .corporate_actions_processed
            .contains("BTCUSD-2-1-split")
    );
}

/// The same per-option record protects an **unheld** option — the case the original defect made
/// silent, since an unheld option's strike fix emits no position observable at all. A second
/// delivery of the same `id` must not divide its strike a second time (50_000 → 25_000, never
/// → 12_500), or every position opened on it later mis-settles at expiry.
#[test]
fn test_corporate_action_unheld_option_strike_not_divided_twice_by_the_same_id() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // No option position is opened — the strike fix is the option's whole adjustment.
    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));

    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
    };

    process_with_audit(&mut engine, ca_event.clone());
    assert!(
        engine
            .state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .corporate_actions_processed
            .contains("BTCUSD-2-1-split"),
        "an unheld option's silent strike fix must still record the action id"
    );

    // Lose the target's record, then re-deliver the same action.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(1))
        .corporate_actions_processed
        .clear();
    let audit_tick = process_with_audit(&mut engine, ca_event);
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(
        contract.strike,
        dec!(25_000),
        "the unheld option's strike must not be divided a second time"
    );
    assert!(
        outputs.iter().any(|o| matches!(
            o,
            EngineOutput::CorporateActionAlreadyProcessed { instrument, .. }
                if *instrument == InstrumentIndex(0)
        )),
        "the suppressed strike fix must be surfaced"
    );
}

/// A second split-eligible instrument on the same `(base, exchange)` but a **different quote** must
/// not trip the split-target uniqueness guard.
///
/// The guard scans the entire instrument registry, so it is only as narrow as the identity match it
/// applies. `is_on_underlying`'s own doc warns that "without the quote filter a BTC/USDT action
/// would also reach BTC/USDC instruments" — drop that filter and a perfectly resolvable action is
/// rejected as ambiguous, blocking a legitimate corporate action outright. No fixture whose
/// instruments all share one `Underlying` can detect that, since base and quote agree everywhere;
/// this one holds `btc/usd` and `btc/usdc` side by side and asserts the `btc/usd` split still
/// applies in full.
#[test]
fn test_corporate_action_split_is_not_ambiguous_across_a_quote_boundary() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    // `BTCUSDC` (index 2) is `Spot`, so split-eligible, and shares the target's base and exchange.
    // Only its quote differs.
    let usdc_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(2));
    assert_eq!(usdc_state.instrument.underlying.quote, AssetIndex(2));
    assert!(
        matches!(usdc_state.instrument.kind, InstrumentKind::Spot),
        "the near-miss instrument must itself be split-eligible, or this proves nothing"
    );

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    let audit_tick = process_with_audit(
        &mut engine,
        EngineEvent::CorporateAction {
            id: "BTCUSD-2-1-split".into(),
            instrument: InstrumentIndex(1),
            kind: CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(dec!(2)).unwrap(),
            },
            policy: SplitRoundingPolicy::Floor,
            effective_time: time_plus_days(STARTING_TIMESTAMP, 10),
        },
    );
    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => &audit.outputs,
        _ => panic!("expected EngineAudit::Process"),
    };

    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            EngineOutput::UnsupportedCorporateAction {
                reason: UnsupportedCorporateActionReason::AmbiguousSplitTarget,
                ..
            }
        )),
        "a differing quote is a different underlying - the target is still unique"
    );

    // The split applied in full, not merely "was not rejected".
    let option_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0));
    let InstrumentKind::Option(contract) = &option_state.instrument.kind else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));

    // ...and the near-miss instrument was left entirely alone by an action on its neighbour.
    let usdc_state = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(2));
    assert!(
        usdc_state.corporate_actions_processed.is_empty(),
        "an instrument on a different quote must not record the action"
    );
    assert!(usdc_state.position.positions.is_empty());
}

/// Live engine vs audit-replica parity for the **rejection** path: an ambiguous split target must
/// be refused identically by both, or the replica applies a split the live engine never did. Full
/// `InstrumentState` equality across the whole ambiguous registry.
#[test]
fn test_corporate_action_replica_parity_ambiguous_split_target() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_ambiguous_underlying_engine(TradingState::Disabled, execution_tx);

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_position_on(&mut engine, 0, Side::Buy, dec!(1_000), dec!(2), "opt-open");
    open_position_on(
        &mut engine,
        1,
        Side::Buy,
        dec!(50_000),
        dec!(3),
        "spot-open",
    );

    let pre_split_state = engine.state.clone();

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };
    let audit_tick = process_with_audit(&mut engine, ca_event.clone());

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    let outputs = match &audit_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &outputs);

    for idx in [InstrumentIndex(0), InstrumentIndex(1), InstrumentIndex(2)] {
        let live = engine.state.instruments.instrument_index(&idx);
        let replica = replica_manager
            .replica_engine_state()
            .instruments
            .instrument_index(&idx);
        assert_eq!(replica, live, "replica/live divergence at {idx:?}");
    }

    // Non-vacuous: the live engine really did reject, leaving the option chain at pre-split terms.
    let InstrumentKind::Option(contract) = &engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .instrument
        .kind
    else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(50_000));
}

/// Live engine vs audit-replica parity for the **per-option idempotency** path. The replica records
/// the action `id` on each option it adjusts, exactly as the live handler does, so a re-delivered
/// action is suppressed on both sides. A replica missing that record would divide the strike twice
/// and diverge — which the strike assertion at the end makes non-vacuous.
#[test]
fn test_corporate_action_option_replica_parity_suppressed_readjust() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx); // Netting

    engine.process(market_event_trade(1, 0, dec!(1200)));
    engine.process(market_event_trade(1, 1, dec!(60_000)));
    open_option_position(&mut engine, dec!(2), dec!(1_000));

    let pre_split_state = engine.state.clone();

    let effective_time = time_plus_days(STARTING_TIMESTAMP, 10);
    let ca_event = EngineEvent::CorporateAction {
        id: "BTCUSD-2-1-split".into(),
        instrument: InstrumentIndex(1),
        kind: CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(dec!(2)).unwrap(),
        },
        policy: SplitRoundingPolicy::Floor,
        effective_time,
    };

    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: pre_split_state,
        context: EngineContext {
            time: effective_time,
            sequence: Sequence(0),
        },
    };
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    // First delivery, driven through both.
    let first_tick = process_with_audit(&mut engine, ca_event.clone());
    let first_outputs = match &first_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event.clone(), &first_outputs);

    // Lose the TARGET's record on both sides, mirroring a snapshot restored without it.
    engine
        .state
        .instruments
        .instrument_index_mut(&InstrumentIndex(1))
        .corporate_actions_processed
        .clear();
    replica_manager
        .state_replica
        .event
        .instruments
        .instrument_index_mut(&InstrumentIndex(1))
        .corporate_actions_processed
        .clear();

    // Second delivery of the same action.
    let second_tick = process_with_audit(&mut engine, ca_event.clone());
    let second_outputs = match &second_tick.event {
        EngineAudit::Process(audit) => audit.outputs.clone(),
        _ => panic!("expected EngineAudit::Process"),
    };
    replica_manager.update_from_event(ca_event, &second_outputs);

    for idx in [InstrumentIndex(0), InstrumentIndex(1)] {
        let live = engine.state.instruments.instrument_index(&idx);
        let replica = replica_manager
            .replica_engine_state()
            .instruments
            .instrument_index(&idx);
        assert_eq!(replica, live, "replica/live divergence at {idx:?}");
    }

    // Non-vacuous: one adjustment happened, not two.
    let InstrumentKind::Option(contract) = &engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .instrument
        .kind
    else {
        panic!("instrument 0 must be an option");
    };
    assert_eq!(contract.strike, dec!(25_000));
}

/// A market `Item` from an exchange the engine was never built against is **reported**, and its
/// `InstrumentIndex` is never dereferenced.
///
/// This is the ordering `EngineState::update_from_market` exists to enforce: the exchange is
/// resolved, and the result propagated, *before* `instrument_index_mut`. An event from an untracked
/// exchange carries an `InstrumentIndex` from a collection the engine does not share, so the
/// positional lookup would either panic on an out-of-range index or credit the print to whichever
/// instrument occupies that slot — a wrong price on a real position, reported nowhere. Move the `?`
/// below the lookup and only this test fails.
#[test]
fn test_untracked_exchange_market_item_is_reported_without_resolving_its_instrument() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    // The engine tracks two `BinanceSpot` instruments, indices 0 and 1.
    const UNTRACKED: ExchangeId = ExchangeId::Coinbase;
    let out_of_range = InstrumentIndex(99);

    let connectivity_before = engine.state.connectivity.clone();
    let instruments_before = engine.state.instruments.clone();

    let event = EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
        time_received: time_plus_days(STARTING_TIMESTAMP, 1),
        exchange: UNTRACKED,
        instrument: out_of_range,
        kind: DataKind::Trade(PublicTrade {
            id: "untracked".into(),
            price: dec!(10_000),
            amount: Decimal::ONE,
            side: Some(Side::Buy),
        }),
    }));

    let audit = process_with_audit(&mut engine, event.clone());

    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::UntrackedExchange(UntrackedExchange::new(
                UNTRACKED,
                ConnectivityDimension::MarketData
            ))
        ),
        "the market-data dimension must be named"
    );
    assert_eq!(
        engine.state.connectivity, connectivity_before,
        "an untracked exchange must not mutate connectivity state"
    );
    assert_eq!(
        engine.state.instruments, instruments_before,
        "no instrument state may be read or written for an untrusted InstrumentIndex"
    );
}

/// Live engine vs audit-replica parity across all three `UntrackedExchange` paths.
///
/// The replica arm **discards** each `Result` (`let _untracked = …`), on the reasoning that its
/// `ConnectivityStates` is a clone of the live engine's, so the same lookup against the same map
/// reaches the same verdict from the event alone — and on `Err` neither side mutates. That holds by
/// inspection today, but it is the only conditional replica arm in `update_from_event` with no
/// parity test behind it, and `AuditMode::Enabled` is a live-trading mode `backtest` never
/// exercises (it hardcodes `Disabled`). Without this, someone threading new `on_disconnect`-adjacent
/// state through the replica would ship the divergence and discover it in production rather than in
/// CI.
#[test]
fn test_untracked_exchange_replica_parity_across_all_three_paths() {
    use rustrade::engine::audit::{
        AuditTick, EngineAudit, context::EngineContext, state_replica::StateReplicaManager,
    };

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    const UNTRACKED: ExchangeId = ExchangeId::Coinbase;

    // Seed the replica from the engine's own starting state, so the two maps genuinely share a key
    // set — the premise the discard relies on.
    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: engine.state.clone(),
        context: EngineContext {
            time: STARTING_TIMESTAMP,
            sequence: Sequence(0),
        },
    };
    let dummy_updates: DummyAuditUpdates = std::iter::empty();
    let mut replica_manager = StateReplicaManager::new(seed_tick, dummy_updates);

    let drive = |engine: &mut TestEngine,
                 replica: &mut StateReplicaManager<_, DummyAuditUpdates>,
                 event: EngineEvent<DataKind>| {
        let audit_tick = process_with_audit(engine, event.clone());
        let outputs = match &audit_tick.event {
            EngineAudit::Process(audit) => audit.outputs.clone(),
            _ => panic!("expected EngineAudit::Process"),
        };
        replica.update_from_event(event, &outputs);
    };

    // Non-vacuity first: a TRACKED market event moves connectivity off its default on both sides.
    // Without this the parity assertions below would compare two states neither side ever touched,
    // and would pass even if `update_from_event` ignored connectivity entirely.
    drive(
        &mut engine,
        &mut replica_manager,
        market_event_trade(1, 0, dec!(100)),
    );
    assert_eq!(
        engine
            .state
            .connectivity
            .connectivity(&ExchangeId::BinanceSpot)
            .market_data,
        Health::Healthy,
        "the tracked event must actually mutate, or the parity asserts below are vacuous"
    );
    assert_eq!(
        replica_manager.replica_engine_state().connectivity,
        engine.state.connectivity,
    );

    let connectivity_before = engine.state.connectivity.clone();

    // All three paths, in the order they appear in `update_from_event`.
    for event in [
        EngineEvent::Account(AccountStreamEvent::Reconnecting(UNTRACKED)),
        EngineEvent::Market(MarketStreamEvent::Reconnecting(UNTRACKED)),
        // The market `Item` carries an `InstrumentIndex` from a collection neither side shares, so
        // this also pins that the replica stops before its own positional lookup.
        EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 2),
            time_received: time_plus_days(STARTING_TIMESTAMP, 2),
            exchange: UNTRACKED,
            instrument: InstrumentIndex(99),
            kind: DataKind::Trade(PublicTrade {
                id: "untracked".into(),
                price: dec!(10_000),
                amount: Decimal::ONE,
                side: Some(Side::Buy),
            }),
        })),
    ] {
        drive(&mut engine, &mut replica_manager, event);
    }

    assert_eq!(
        replica_manager.replica_engine_state().connectivity,
        engine.state.connectivity,
        "replica/live connectivity divergence across the untracked paths"
    );
    assert_eq!(
        replica_manager.replica_engine_state().instruments,
        engine.state.instruments,
        "replica/live instrument divergence — the out-of-range index must not be dereferenced \
         on either side"
    );
    assert_eq!(
        engine.state.connectivity, connectivity_before,
        "and none of the three may have mutated anything at all"
    );
}

/// A `Reconnecting` event for an exchange the engine was never built against is **reported**, and
/// nothing else happens: no `on_disconnect`, no connectivity mutation, no new tracked exchange.
///
/// `ConnectivityStates` is unit-tested at its own seam; this pins the two layers above it, which are
/// where the decision actually lands. `Engine::update_from_{market,account}_stream` matches on the
/// `Result` to choose between calling `Strategy::on_disconnect` and reporting, and `ProcessAudit`
/// translates that choice into an `EngineOutput`. Swap those arms — fire `on_disconnect` for a venue
/// the strategy has no link to, or drop the report instead of emitting it — and every other test in
/// this suite still passes, because none of them names an exchange the engine does not track.
#[test]
fn test_untracked_exchange_reconnecting_is_reported_without_disconnect_or_mutation() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    // The engine is built from `BinanceSpot` instruments only. Nothing about this event is
    // malformed — it is the shape a misconfigured subscription or execution client produces.
    const UNTRACKED: ExchangeId = ExchangeId::Coinbase;
    let tracked_before = engine.state.connectivity.clone();

    let event = EngineEvent::Market(MarketStreamEvent::Reconnecting(UNTRACKED));
    let audit = process_with_audit(&mut engine, event.clone());

    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::UntrackedExchange(UntrackedExchange::new(
                UNTRACKED,
                ConnectivityDimension::MarketData
            ))
        ),
        "the market-data dimension must be named, and no MarketDisconnect emitted"
    );

    let event = EngineEvent::Account(AccountStreamEvent::Reconnecting(UNTRACKED));
    let audit = process_with_audit(&mut engine, event.clone());

    assert_eq!(
        audit.event,
        EngineAudit::process_with_output(
            event,
            EngineOutput::UntrackedExchange(UntrackedExchange::new(
                UNTRACKED,
                ConnectivityDimension::Account
            ))
        ),
        "the account dimension must be named, and no AccountDisconnect emitted"
    );

    // Reporting is the whole effect: an untracked exchange is not silently adopted into the
    // collection, and the venues the engine does track are left exactly as they were.
    assert_eq!(
        engine.state.connectivity, tracked_before,
        "an untracked exchange must not mutate connectivity state"
    );
    assert!(
        !engine.state.connectivity.exchanges.contains_key(&UNTRACKED),
        "an untracked exchange must not become tracked by reporting it"
    );
}

// ---------------------------------------------------------------------------------------------
// Shutdown semantics
//
// `Shutdown` carries two meanings and the difference between them is only observable when
// something is in flight, so that is what these pin down. The `Immediate` case matters most: it is
// what live trading relies on, and a refactor of the drain could quietly make stopping wait on a
// venue that may be slow or unreachable without any of the backtest tests noticing.
// ---------------------------------------------------------------------------------------------

/// Drives the engine until it has an order in flight, returning it ready for a shutdown event.
fn engine_with_an_order_in_flight() -> TestEngine {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Enabled, execution_tx);

    // The strategy opens on the first priced tick; the request is recorded in flight as it is sent.
    engine.process(market_event_trade(1, 0, dec!(100)));

    assert!(
        engine.state.has_requests_in_flight(),
        "fixture is only meaningful with an order in flight"
    );

    engine
}

#[test]
fn test_shutdown_immediate_is_terminal_even_with_an_order_in_flight() {
    use rustrade::shutdown::Shutdown;
    use rustrade_integration::Terminal;

    let mut engine = engine_with_an_order_in_flight();

    let audit = engine.process(EngineEvent::Shutdown(Shutdown::Immediate));

    assert!(
        audit.is_terminal(),
        "Immediate must stop at once rather than wait on the venue"
    );
    assert!(
        !engine.meta.draining,
        "Immediate must not put the engine into a drain"
    );
}

#[test]
fn test_shutdown_after_drain_waits_while_an_order_is_in_flight() {
    use rustrade::shutdown::Shutdown;
    use rustrade_integration::Terminal;

    let mut engine = engine_with_an_order_in_flight();

    let audit = engine.process(EngineEvent::Shutdown(Shutdown::AfterDrain));

    assert!(
        !audit.is_terminal(),
        "AfterDrain must keep processing while a response is still owed"
    );
    assert!(engine.meta.draining, "the engine should now be draining");
}

#[test]
fn test_shutdown_after_drain_is_terminal_when_nothing_is_in_flight() {
    use rustrade::shutdown::Shutdown;
    use rustrade_integration::Terminal;

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_engine(TradingState::Disabled, execution_tx);

    assert!(!engine.state.has_requests_in_flight());

    let audit = engine.process(EngineEvent::Shutdown(Shutdown::AfterDrain));

    assert!(
        audit.is_terminal(),
        "with nothing owed there is nothing to drain, so this is an immediate stop"
    );
}
