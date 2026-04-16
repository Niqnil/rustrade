use barter::{
    EngineEvent, Sequence, Timed,
    engine::{
        Engine, EngineOutput, Processor,
        action::{
            ActionOutput,
            generate_algo_orders::GenerateAlgoOrdersOutput,
            send_requests::{SendCancelsAndOpensOutput, SendRequestsOutput},
        },
        audit::EngineAudit,
        clock::HistoricalClock,
        command::Command,
        execution_tx::MultiExchangeTxMap,
        process_with_audit,
        state::{
            EngineState,
            asset::AssetStates,
            connectivity::Health,
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            position::{OmsMode, PositionExited},
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
use barter_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use barter_execution::{
    AccountEvent, AccountEventKind, AccountSnapshot, FeeModelConfig, PerContractFeeModel,
    balance::{AssetBalance, Balance},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, PositionId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, RequestOpen},
        state::{ActiveOrderState, Cancelled, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side, Underlying,
    asset::AssetIndex,
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
use barter_integration::{
    channel::{UnboundedTx, mpsc_unbounded},
    collection::{none_one_or_many::NoneOneOrMany, one_or_many::OneOrMany, snapshot::Snapshot},
};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const STARTING_TIMESTAMP: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;
const RISK_FREE_RETURN: Decimal = dec!(0.05);
const STARTING_BALANCE_USDT: Balance = Balance {
    total: dec!(40_000.0),
    free: dec!(40_000.0),
};
const STARTING_BALANCE_BTC: Balance = Balance {
    total: dec!(1.0),
    free: dec!(1.0),
};
const STARTING_BALANCE_ETH: Balance = Balance {
    total: dec!(10.0),
    free: dec!(10.0),
};
const QUOTE_FEES_PERCENT: f64 = 0.1; // 10%

// Type alias to avoid clippy::type_complexity warnings in test helper functions
type TestEngine = Engine<
    HistoricalClock,
    EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
    TestBuyAndHoldStrategy,
    DefaultRiskManager<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>,
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
    let event = market_event_trade(1, 0, 10_000.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(1));
    assert_eq!(audit.event, EngineAudit::process(event));
    assert_eq!(engine.state.connectivity.global, Health::Healthy);

    // Process 1st MarketEvent for eth_btc
    let event = market_event_trade(1, 1, 0.1);
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
            price: dec!(10_000),
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
            price: dec!(0.1),
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
                            btc_usdt_buy_order.clone(),
                            eth_btc_buy_order.clone(),
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
    let event = account_event_order_response(0, 2, Side::Buy, 10_000.0, 1.0, 1.0);
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
    let event = account_event_order_response(1, 2, Side::Buy, 0.1, 1.0, 1.0);
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
    let event = market_event_trade(2, 0, 20_000.0);
    let audit = process_with_audit(&mut engine, event.clone());
    assert_eq!(audit.context.sequence, Sequence(13));
    assert_eq!(audit.event, EngineAudit::process(event));

    // Process 2nd MarketEvent for eth_btc
    let event = market_event_trade(2, 1, 0.05);
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
            price: dec!(20_000),
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
                    sent: NoneOneOrMany::One(btc_usdt_sell_order.clone()),
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
            price: dec!(20_000),
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
                fees_enter: AssetFees::quote_fees(dec!(1_000.0)),
                fees_exit: AssetFees::quote_fees(dec!(2_000.0)),
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
            price: dec!(0.05),
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
                sent: NoneOneOrMany::One(eth_btc_sell_order.clone()),
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
    let event = account_event_order_response(1, 4, Side::Sell, 0.05, 1.0, 0.0);
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
            price: dec!(0.05),
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
            price: dec!(0.05),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            state: OrderState::fully_filled(),
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
                fees_enter: AssetFees::quote_fees(dec!(0.01)), // 0.01 btc
                fees_exit: AssetFees::quote_fees(dec!(0.005)), // 0.005 btc
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
                let price = state.data.price()?;

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
                        price,
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

fn market_event_trade(time_plus: u64, instrument: usize, price: f64) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: time_plus_days(STARTING_TIMESTAMP, time_plus),
        time_received: time_plus_days(STARTING_TIMESTAMP, time_plus),
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex(instrument),
        kind: DataKind::Trade(PublicTrade {
            id: time_plus.to_string(),
            price,
            amount: 1.0,
            side: Side::Buy,
        }),
    }))
}

fn account_event_order_response(
    instrument: usize,
    time_plus: u64,
    side: Side,
    price: f64,
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
            price: Decimal::try_from(price).unwrap(),
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
            fees: AssetFees::quote_fees(
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
            barter_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
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
fn send_spot_price(engine: &mut TestEngine, instrument: usize, price: f64) {
    let event = market_event_trade(1, instrument, price);
    engine.process(event);
}

/// Open a long position in the option instrument (index 0) by sending a buy trade.
fn open_option_position(engine: &mut TestEngine, quantity: f64, price: f64) {
    let event = EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
        exchange: ExchangeIndex(0),
        kind: AccountEventKind::Trade(Trade {
            id: TradeId::new("opt-trade-open"),
            order_id: gen_order_id(0),
            instrument: InstrumentIndex(0),
            strategy: strategy_id(),
            time_exchange: time_plus_days(STARTING_TIMESTAMP, 1),
            side: Side::Buy,
            price: Decimal::try_from(price).unwrap(),
            quantity: Decimal::try_from(quantity).unwrap(),
            fees: AssetFees::quote_fees(Decimal::ZERO),
        }),
    }));
    engine.process(event);
}

#[test]
fn test_contract_expiry_otm_call() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    // Set underlying spot price BELOW strike (50_000) → OTM (spot is at index 1)
    send_spot_price(&mut engine, 1, 45_000.0);

    // Open a long call position with 2 contracts at premium 1_000
    open_option_position(&mut engine, 2.0, 1_000.0);

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
    send_spot_price(&mut engine, 1, 55_000.0);

    // Open a long call position with 1 contract at premium 2_000
    open_option_position(&mut engine, 1.0, 2_000.0);

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

#[test]
fn test_contract_expiry_idempotent() {
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    send_spot_price(&mut engine, 1, 45_000.0);
    open_option_position(&mut engine, 1.0, 1_000.0);

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

    send_spot_price(&mut engine, 1, 45_000.0);

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
    open_option_position(&mut engine, 1.0, 1_000.0);

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
    use barter::{
        engine::audit::state_replica::StateReplicaManager,
        engine::audit::{AuditTick, EngineAudit, context::EngineContext},
    };
    use barter_integration::collection::none_one_or_many::NoneOneOrMany;

    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let mut engine = build_option_engine(TradingState::Disabled, execution_tx);

    send_spot_price(&mut engine, 1, 45_000.0);
    open_option_position(&mut engine, 1.0, 1_000.0);

    // Process ContractExpiry on the live engine to get the real audit outputs.
    let expiry_event = EngineEvent::ContractExpiry(InstrumentIndex(0));
    let audit_tick = process_with_audit(&mut engine, expiry_event.clone());

    // Build a separate replica state that mirrors the pre-expiry state.
    let (execution_tx2, _) = mpsc_unbounded();
    let mut replica_engine = build_option_engine(TradingState::Disabled, execution_tx2);
    send_spot_price(&mut replica_engine, 1, 45_000.0);
    open_option_position(&mut replica_engine, 1.0, 1_000.0);

    let seed_context = EngineContext {
        time: STARTING_TIMESTAMP,
        sequence: Sequence(0),
    };
    let seed_tick: AuditTick<_, EngineContext> = AuditTick {
        event: replica_engine.state.clone(),
        context: seed_context,
    };

    // Type annotation required for StateReplicaManager::new to infer the iterator element type
    #[allow(clippy::type_complexity)]
    let dummy_updates: std::iter::Empty<
        AuditTick<
            EngineAudit<
                EngineEvent<DataKind>,
                EngineOutput<OnTradingDisabledOutput, OnDisconnectOutput>,
            >,
        >,
    > = std::iter::empty();
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
            barter_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
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
fn open_option_position_side(engine: &mut TestEngine, side: Side, quantity: f64, price: f64) {
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
            price: Decimal::try_from(price).unwrap(),
            quantity: Decimal::try_from(quantity).unwrap(),
            fees: AssetFees::quote_fees(Decimal::ZERO),
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
    send_spot_price(&mut engine, 1, 45_000.0);
    open_option_position(&mut engine, 1.0, 2_000.0); // bought at 2_000 premium

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
    send_spot_price(&mut engine, 1, 55_000.0);
    open_option_position(&mut engine, 1.0, 2_000.0);

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
    send_spot_price(&mut engine, 1, 55_000.0);
    open_option_position_side(&mut engine, Side::Sell, 1.0, 2_000.0); // sold at 2_000 premium

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
    send_spot_price(&mut engine, 1, 45_000.0);
    open_option_position_side(&mut engine, Side::Sell, 1.0, 2_000.0);

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
            barter_instrument::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
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
            price,
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
            price: dec!(1_000),
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
            fees: AssetFees::quote_fees(Decimal::ZERO),
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
            price: dec!(0),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: OrderState::fully_filled(),
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
    send_spot_price(&mut engine, 1, 55_000.0);

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
            fees: AssetFees::quote_fees(Decimal::ZERO), // Exchange reports zero commission
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
    open_option_position(&mut engine, 1.0, 1_000.0);

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
    send_spot_price(&mut engine, 1, 55_000.0); // ITM for strike 50_000

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
