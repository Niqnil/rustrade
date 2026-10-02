//! Per-event cost of [`EngineState::update_from_market`] as the number of open positions on the
//! instrument grows.
//!
//! Every market event carrying a price recomputes `pnl_unrealised` and advances
//! `time_exchange_update` for each open position on its instrument. This measures what that loop
//! costs next to the rest of the per-event work, with the price changing on every event and with it
//! held constant, the case a skip-if-unchanged optimisation would serve.
#![allow(clippy::unwrap_used)] // Benchmark setup: a panic on bad fixture data is acceptable

use chrono::{DateTime, Utc};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::engine::state::{
    EngineState,
    global::DefaultGlobalData,
    instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
    position::{OmsMode, Position},
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    order::id::{OrderId, PositionId, StrategyId},
    trade::{AssetFees, Trade, TradeId},
};
use rustrade_instrument::{
    Side, Underlying,
    asset::AssetIndex,
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex},
};
use std::hint::black_box;

criterion::criterion_main!(benchmark_market_event);

/// Events per measured batch. Large enough that the per-batch overhead is negligible.
const EVENTS: usize = 1_000;

/// Open positions on the instrument. 1 is Netting's maximum; the rest need Hedging.
const POSITIONS: [usize; 5] = [0, 1, 4, 16, 64];

const T0: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;

fn benchmark_market_event() {
    let mut c = Criterion::default().configure_from_args();
    bench_update_from_market(&mut c);
    c.final_summary();
}

fn bench_update_from_market(c: &mut Criterion) {
    let mut group = c.benchmark_group("EngineState::update_from_market");
    group.throughput(Throughput::Elements(EVENTS as u64));

    for price in [Price::Changing, Price::Constant] {
        let events = market_events(price);
        assert_prices_reach_the_state(price, &events);

        for positions in POSITIONS {
            group.bench_with_input(
                BenchmarkId::new(price.label(), positions),
                &positions,
                |b, &positions| {
                    // A fresh state per iteration, built outside the timing. Replaying the same
                    // events into one state would measure the first pass only: from then on every
                    // event is no newer than the trade already held, so the instrument's recency
                    // guard skips it and the price never changes again.
                    let state = state_with_positions(positions);
                    b.iter_batched_ref(
                        || state.clone(),
                        |state| {
                            for event in &events {
                                state.update_from_market(black_box(event)).unwrap();
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

/// Panics unless each event's price is what the instrument holds after it, so a mode cannot
/// silently measure a different workload from the one it is named for. The state is a clone, as
/// in the bench, so a clone that carried a trade over would be caught too.
fn assert_prices_reach_the_state(price: Price, events: &[MarketEvent<InstrumentIndex, DataKind>]) {
    let mut state = state_with_positions(1).clone();
    for (index, event) in events.iter().enumerate() {
        state.update_from_market(event).unwrap();
        let held = state
            .instruments
            .instrument_index(&InstrumentIndex(0))
            .data
            .price();
        assert_eq!(
            held,
            Some(price.at(index)),
            "{} event {index}",
            price.label()
        );
    }
}

#[derive(Debug, Copy, Clone)]
enum Price {
    /// Alternates between two prices, so every event after the first moves the mark.
    Changing,
    /// One price throughout, so every recompute produces the value it already held.
    Constant,
}

impl Price {
    fn label(self) -> &'static str {
        match self {
            Price::Changing => "price_changing",
            Price::Constant => "price_constant",
        }
    }

    fn at(self, index: usize) -> Decimal {
        match self {
            Price::Changing if index % 2 == 1 => dec!(101),
            _ => dec!(100),
        }
    }
}

fn instruments() -> IndexedInstruments {
    IndexedInstruments::new([Instrument::spot(
        ExchangeId::BinanceSpot,
        "binance_spot_btc_usdt",
        "BTCUSDT",
        Underlying::new("btc", "usdt"),
        None,
    )])
}

fn state_with_positions(
    positions: usize,
) -> EngineState<DefaultGlobalData, DefaultInstrumentMarketData> {
    let instruments = instruments();
    let mut state = EngineState::builder(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(T0)
    .oms_mode(OmsMode::Hedging)
    .build();

    let instrument = state.instruments.instrument_index_mut(&InstrumentIndex(0));
    for index in 0..positions {
        let trade = Trade::new(
            TradeId::new(index.to_string()),
            OrderId::new(index.to_string()),
            InstrumentIndex(0),
            StrategyId::new("bench"),
            T0,
            Side::Buy,
            dec!(100),
            dec!(1),
            None,
            AssetFees::new(AssetIndex(0), Decimal::ZERO, Some(Decimal::ZERO)),
        );
        instrument
            .position
            .positions
            .insert(PositionId::new(index.to_string()), Position::from(&trade));
    }

    state
}

fn market_events(price: Price) -> Vec<MarketEvent<InstrumentIndex, DataKind>> {
    (0..EVENTS)
        .map(|index| {
            let time = T0 + chrono::TimeDelta::milliseconds(index as i64);
            MarketEvent {
                time_exchange: time,
                time_received: time,
                exchange: ExchangeId::BinanceSpot,
                instrument: InstrumentIndex(0),
                kind: DataKind::Trade(PublicTrade {
                    id: index.to_string().into(),
                    price: price.at(index),
                    amount: Decimal::ONE,
                    side: Some(Side::Buy),
                }),
            }
        })
        .collect()
}
