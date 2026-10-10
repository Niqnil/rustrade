#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! The simulated venue's CFD ledger and the engine's position accounting describe the same trades.
//!
//! `SimulatedVenue` keeps a net position per CFD instrument so that a closing fill pays back the
//! margin its opening posted, plus the realised PnL. The engine keeps its own `Position` from the
//! same fills. If the two disagreed, a backtest's balances and its positions would tell different
//! stories about one run, and nothing would say so.
//!
//! The end-to-end test is the first caller of the whole CFD chain outside a unit test: a CFD
//! instrument, the mock execution client, `SimRunner` and the engine. It is funded with exactly one
//! position's margin, so it can only finish flat if closing pays that margin back.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::NoAuxEvents, backtest,
        market_data::MarketDataInMemory, summary::BacktestResult,
    },
    engine::{
        Engine,
        clock::HistoricalClock,
        state::{
            EngineState,
            builder::EngineStateBuilder,
            global::DefaultGlobalData,
            instrument::{
                data::{DefaultInstrumentMarketData, InstrumentDataState},
                filter::InstrumentFilter,
            },
            position::{OmsMode, PositionId, PositionManager},
            trading::TradingState,
        },
    },
    execution::{request::ExecutionRequest, sim::SimVenueOptions},
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::config::ExecutionConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountEventKind, AccountSnapshot,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PercentageFeeModel},
    fill::SimFillConfig,
    holding_cost::{FundingModel, RateSeries},
    market::MarketSnapshot,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
    },
    trade::Trade,
};
use rustrade_instrument::{
    Side, Underlying,
    asset::{
        AssetIndex, ExchangeAsset,
        name::{AssetNameExchange, AssetNameInternal},
    },
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::{InstrumentKind, cfd::CfdContract},
        name::{InstrumentNameExchange, InstrumentNameInternal},
        quote::InstrumentQuoteAsset,
    },
    test_utils::instrument,
};

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

fn contract_size() -> Decimal {
    dec!(25)
}

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

fn usd() -> AssetNameExchange {
    AssetNameExchange::new("usd")
}

fn funded(amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: usd(),
        balance: Balance::new(amount, amount),
        time_exchange: ts("2025-03-24T22:00:00Z"),
    }
}

fn mock_config(usd_balance: Decimal, fee_model: FeeModelConfig) -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![funded(usd_balance)],
            instruments: vec![],
        },
        latency_ms: 0,
        fee_model,
        fill_model: SimFillConfig::default(),
    }
}

// --- The venue's ledger against the engine's position -------------------------------------------

fn cfd_name() -> InstrumentNameExchange {
    InstrumentNameExchange::new("spx500_usd")
}

fn venue_instrument() -> Instrument<ExchangeId, AssetNameExchange> {
    Instrument {
        exchange: EXCHANGE,
        name_internal: InstrumentNameInternal::new("spx500_usd"),
        name_exchange: cfd_name(),
        underlying: Underlying {
            base: AssetNameExchange::new("spx500"),
            quote: usd(),
        },
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Cfd(CfdContract {
            contract_size: contract_size(),
            settlement_asset: usd(),
        }),
        spec: None,
        data_venue: None,
    }
}

/// Fills one market order at `price` and returns the trade it printed.
fn fill(
    venue: &mut SimulatedVenue,
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> Trade<AssetNameExchange, InstrumentNameExchange> {
    let snapshot = MarketSnapshot {
        best_bid: Some(price),
        best_ask: Some(price),
        last_price: Some(price),
    };
    let outcome = venue.open_order(OrderEvent {
        key: OrderKey {
            exchange: EXCHANGE,
            instrument: cfd_name(),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::random(),
        },
        state: RequestOpen {
            side,
            price: None,
            quantity,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            position_id: None,
            reduce_only: false,
            market: Some(snapshot),
        },
    });
    assert!(
        outcome.response.state.is_accepted(),
        "{side:?} {quantity} @ {price}: {:?}",
        outcome.response.state
    );

    outcome
        .events
        .into_iter()
        .find_map(|event| match event.kind {
            AccountEventKind::Trade(trade) => Some(trade),
            _ => None,
        })
        .expect("a filled order prints a trade")
}

fn venue_position(venue: &SimulatedVenue) -> Option<(Decimal, Decimal)> {
    let snapshot = venue.account_snapshot();
    let instrument = snapshot
        .instruments
        .iter()
        .find(|instrument| instrument.instrument == cfd_name())
        .expect("a CFD instrument is always in the snapshot");
    instrument
        .position
        .open()
        .map(|position| (position.quantity, position.entry_price.unwrap()))
}

fn engine_position(
    positions: &PositionManager<AssetNameExchange, InstrumentNameExchange>,
) -> Option<(Decimal, Decimal)> {
    positions
        .positions
        .get(&PositionId::NETTING)
        .map(|position| {
            let quantity = match position.side {
                Side::Buy => position.quantity_abs,
                Side::Sell => -position.quantity_abs,
            };
            (quantity, position.price_entry_average)
        })
}

/// Opening, averaging up, flipping through zero and closing: after every fill the venue's net
/// position matches the engine's, and over the whole sequence the balance moves by exactly the
/// PnL the engine realised, fees included.
#[test]
fn the_venue_ledger_and_the_engine_position_agree_fill_by_fill() {
    let initial = dec!(1_000_000);
    let mut venue = SimulatedVenue::new(
        &mock_config(
            initial,
            FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.001))),
        ),
        [(cfd_name(), venue_instrument())].into_iter().collect(),
    );
    let mut engine = PositionManager::new(OmsMode::Netting);
    let mut realised = Decimal::ZERO;

    let fills = [
        (Side::Buy, dec!(2), dec!(5000)),  // open long 2
        (Side::Buy, dec!(1), dec!(5300)),  // average up to 3 @ 5100
        (Side::Sell, dec!(4), dec!(5200)), // close 3, flip to short 1 @ 5200
        (Side::Buy, dec!(1), dec!(5000)),  // close the short
    ];

    for (side, quantity, price) in fills {
        let trade = fill(&mut venue, side, quantity, price);
        if let Some(exited) = engine.update_from_trade(&trade, contract_size()) {
            realised += exited.pnl_realised;
        }

        assert_eq!(
            venue_position(&venue),
            engine_position(&engine),
            "after {side:?} {quantity} @ {price}"
        );
    }

    assert_eq!(venue_position(&venue), None, "the sequence ends flat");
    let balance = venue.balances(&[usd()]).remove(0).balance;
    assert_eq!(balance.total, balance.free, "nothing is left held");
    assert_eq!(
        balance.total - initial,
        realised,
        "the balance moved by the PnL the engine realised"
    );
    assert!(!realised.is_zero(), "the fixture must realise something");
}

// --- End to end: a CFD round trip through a backtest ---------------------------------------------

type BacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;
type BacktestTxMap = rustrade::engine::execution_tx::MultiExchangeTxMap<
    rustrade_integration::channel::UnboundedTx<ExecutionRequest>,
>;

/// Opens a long of 1 at the first price it sees, and closes it once the price reaches
/// [`RoundTrip::CLOSE_AT`].
#[derive(Debug, Default)]
struct RoundTrip {
    sent: AtomicUsize,
}

impl RoundTrip {
    const CLOSE_AT: Decimal = dec!(5200);
}

impl AlgoStrategy for RoundTrip {
    type State = BacktestState;

    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>>,
    ) {
        let Some(instrument) = state
            .instruments
            .instruments(&InstrumentFilter::None)
            .next()
        else {
            return (std::iter::empty(), None);
        };
        let Some(price) = instrument.data.price() else {
            return (std::iter::empty(), None);
        };
        let holding = !instrument.position.positions.is_empty();

        let side = match self.sent.load(Ordering::Relaxed) {
            0 => Side::Buy,
            1 if holding && price >= Self::CLOSE_AT => Side::Sell,
            _ => return (std::iter::empty(), None),
        };
        self.sent.fetch_add(1, Ordering::Relaxed);

        let open = OrderRequestOpen {
            key: OrderKey {
                exchange: instrument.instrument.exchange,
                instrument: instrument.key,
                strategy: StrategyId::new("round-trip"),
                cid: ClientOrderId::random(),
            },
            state: RequestOpen {
                side,
                price: None,
                quantity: dec!(1),
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market: None,
            },
        };

        (std::iter::empty(), Some(open))
    }
}

impl ClosePositionsStrategy for RoundTrip {
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

impl
    OnDisconnectStrategy<
        HistoricalClock,
        BacktestState,
        BacktestTxMap,
        DefaultRiskManager<BacktestState>,
    > for RoundTrip
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
    > for RoundTrip
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

/// A round trip of 1 contract: opened at 5,000 at 22:00, through 5,100 at 22:30, closed at 5,200
/// at 23:00, from an account funded with `margin` and charging what `venue_options` configure.
async fn round_trip(
    margin: Decimal,
    venue_options: FnvHashMap<ExchangeId, SimVenueOptions>,
) -> BacktestResult<Daily, BacktestState> {
    let mut cfd = instrument(EXCHANGE, "spx500", "usd");
    cfd.kind = InstrumentKind::Cfd(CfdContract {
        contract_size: contract_size(),
        settlement_asset: cfd.underlying.quote.clone(),
    });
    let instruments = IndexedInstruments::new([cfd]);
    let key = instruments.instruments()[0].key;

    let market_events = [
        ("2025-03-24T22:00:00Z", dec!(5000)),
        ("2025-03-24T22:30:00Z", dec!(5100)),
        ("2025-03-24T23:00:00Z", dec!(5200)),
    ]
    .into_iter()
    .map(|(time, price)| {
        let time = ts(time);
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: EXCHANGE,
            instrument: key,
            kind: DataKind::Trade(PublicTrade {
                id: "t".into(),
                price,
                amount: dec!(1),
                side: None,
            }),
        })
    })
    .collect::<Vec<_>>();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .trading_state(TradingState::Enabled)
    .build();

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        venue_options,
        executions: vec![ExecutionConfig::Mock(mock_config(
            margin,
            FeeModelConfig::default(),
        ))],
        market_data: MarketDataInMemory::new(Arc::new(market_events)),
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    backtest(
        args_constant,
        BacktestArgsDynamic {
            id: "cfd-round-trip".into(),
            risk_free_return: Decimal::ZERO,
            strategy: RoundTrip::default(),
            risk: DefaultRiskManager::default(),
        },
    )
    .await
    .expect("a CFD round trip must complete")
}

fn usd_end(result: &BacktestResult<Daily, BacktestState>) -> Balance {
    result
        .summary
        .trading_summary
        .assets
        .get(&ExchangeAsset::<AssetNameInternal>::new(EXCHANGE, "usd"))
        .expect("the run is funded in usd, so it must be summarised")
        .balance_end
        .expect("a funded asset has a closing balance")
}

/// Funded with exactly the margin one contract posts at 5,000 (1 × 25 × 5,000). Opening takes the
/// whole balance, so the close only goes through because it is paid for by the position it closes,
/// and the run ends flat with the realised PnL, 1 × 25 × (5,200 − 5,000), in the balance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_opens_and_closes_a_cfd_with_one_positions_margin() {
    let margin = dec!(125_000);
    let result = round_trip(margin, FnvHashMap::default()).await;

    let positions = result
        .engine_state
        .instruments
        .instruments(&InstrumentFilter::None)
        .flat_map(|instrument| instrument.position.positions.values())
        .count();
    assert_eq!(positions, 0, "the close filled, so the run ends flat");

    let usd_end = usd_end(&result);
    assert_eq!(usd_end.total, margin + dec!(5_000));
    assert_eq!(usd_end.free, usd_end.total, "nothing is left held");
}

/// Hourly funding charges the long once, at 23:00: the boundary falls between no two ticks the
/// strategy acts on, so the runner wakes the venue for it, and it is charged before the tick at
/// that instant closes the position. It is valued at the market as of the boundary, 5,100, so
/// the long pays 1 × 25 × 5,100 × 0.0001 = 12.75.
///
/// The venue's balance and the engine's carry describe that one charge: the balance ends 12.75
/// short of the PnL, the tear sheet reports it as carry, and its PnL is net of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backtest_charges_a_held_cfd_its_funding_and_the_engine_carries_it() {
    let funding = FundingModel::new(TimeDelta::hours(1)).with_rates(
        InstrumentNameExchange::new("spx500_usd"),
        RateSeries::constant(dec!(0.0001)),
    );
    let options = FnvHashMap::from_iter([(
        EXCHANGE,
        SimVenueOptions::default().with_holding_cost(Arc::new(funding)),
    )]);

    let margin = dec!(200_000);
    let result = round_trip(margin, options).await;

    assert_eq!(usd_end(&result).total, margin + dec!(5_000) - dec!(12.75));

    let tear_sheet = result
        .summary
        .trading_summary
        .instruments
        .values()
        .next()
        .expect("one instrument traded");
    assert_eq!(tear_sheet.carry, dec!(-12.75));
    assert_eq!(tear_sheet.pnl, dec!(5_000) - dec!(12.75));
}

/// Options for an exchange no execution configuration mocks are a misconfiguration: refused
/// rather than ignored, which would run the backtest without the costs its author configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn venue_options_for_an_exchange_with_no_simulated_venue_are_refused() {
    let mut cfd = instrument(EXCHANGE, "spx500", "usd");
    cfd.kind = InstrumentKind::Cfd(CfdContract {
        contract_size: contract_size(),
        settlement_asset: cfd.underlying.quote.clone(),
    });
    let instruments = IndexedInstruments::new([cfd]);
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(ts("2025-03-24T22:00:00Z"))
    .build();
    let market_events = vec![MarketStreamEvent::Item(MarketEvent {
        time_exchange: ts("2025-03-24T22:00:00Z"),
        time_received: ts("2025-03-24T22:00:00Z"),
        exchange: EXCHANGE,
        instrument: instruments.instruments()[0].key,
        kind: DataKind::Trade(PublicTrade {
            id: "t".into(),
            price: dec!(5000),
            amount: dec!(1),
            side: None,
        }),
    })];

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        venue_options: FnvHashMap::from_iter([(ExchangeId::Kraken, SimVenueOptions::default())]),
        executions: vec![ExecutionConfig::Mock(mock_config(
            dec!(125_000),
            FeeModelConfig::default(),
        ))],
        market_data: MarketDataInMemory::new(Arc::new(market_events)),
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    let error = backtest(
        args_constant,
        BacktestArgsDynamic {
            id: "misconfigured".into(),
            risk_free_return: Decimal::ZERO,
            strategy: RoundTrip::default(),
            risk: DefaultRiskManager::default(),
        },
    )
    .await
    .expect_err("options nothing would apply must be refused");

    assert!(
        error
            .to_string()
            .contains("no execution configuration mocks"),
        "unexpected error: {error}"
    );
}
