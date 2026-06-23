#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Inject an `EngineEvent::CorporateAction` (a stock split) into the engine — backtest and live.
//!
//! The engine never sources corporate actions itself: positions are fill-derived, so even in live
//! trading a broker applying a split overnight does **not** fix the engine's internal
//! `quantity`/`price_entry_average`/`pnl_unrealised`. A wrapper detects the action and injects an
//! `EngineEvent::CorporateAction`; the engine adjusts every open position on the target instrument
//! and emits observables (`SplitRemainder`, `OpenOrdersAtSplit`, `OptionPositionsUnadjustedForSplit`,
//! `UnsupportedCorporateAction`).
//!
//! This example shows **how to construct and inject the event**, on both paths:
//! - **Backtest** (`main`, runnable): the split is supplied via an [`AuxEventsInMemory`] aux source,
//!   merged with the market stream in simulated-time order, and the backtest runs to completion.
//! - **Live** ([`live_injection_sketch`], shown but not executed — it needs a running broker
//!   connection): the same event is sent directly through the public `System.feed_tx` channel.
//!
//! It is deliberately **not** an auto-injecting driver and does **no sourcing** (resolving tickers,
//! fetching split ratios, deciding rounding policy, and choosing *when* to inject all remain wrapper
//! concerns). The four caller obligations the event's rustdoc spells out are demonstrated here:
//!   1. assign a **unique `id`** per action (the sole idempotency key);
//!   2. resolve the ticker to the engine's `InstrumentKey`;
//!   3. supply the rounding [`SplitRoundingPolicy`] matching the broker (whole-share vs fractional);
//!   4. resolve the effective date to an `effective_time` instant via [`split_effective_instant`].
//!
//! The focus is the injection mechanics, not the position adjustment: this backtest opens no
//! position before the split (the default strategy trades nothing), so the split is a structural
//! no-op on positions here and emits no `SplitRemainder`. To *observe* the adjustment and emitted
//! outputs you must (a) have an open position on the instrument and (b) consume the audit stream —
//! which `backtest` disables (`AuditMode::Disabled`). See
//! `examples/engine_sync_with_audit_replica_engine_state.rs` for audit consumption, and the
//! `test_engine_process_engine_event_with_audit` integration tests for the split adjustment and
//! outputs asserted directly against an open position.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    EngineEvent, SplitRoundingPolicy, Timed,
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic, aux_events::AuxEventsInMemory, backtest,
        market_data::MarketDataInMemory,
    },
    engine::state::{
        EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData, trading::TradingState,
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use rustrade_instrument::{
    corporate_action::{CorporateActionKind, split_effective_instant},
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use rustrade_integration::channel::{Tx, UnboundedTx};
use serde::Deserialize;

const CONFIG_PATH: &str = "rustrade/examples/config/backtest_config.json";

#[derive(Deserialize)]
struct Config {
    // `risk_free_return` is also present in the JSON but unused here; serde ignores it.
    system: SystemConfig,
}

#[tokio::main]
async fn main() {
    init_logging();

    let Config {
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    // Obligation 2: the wrapper resolves the ticker to the engine's `InstrumentKey`. Here the first
    // configured instrument (index 0) plays the part of the splitting equity.
    let instruments = IndexedInstruments::new(instruments);
    let split_instrument = InstrumentIndex::new(0);

    // Self-contained market data: four trades spanning 22:00–23:00 on the effective date. A real
    // backtest loads these from disk — see `examples/backtests_concurrent.rs`.
    let market_data = MarketDataInMemory::new(Arc::new(synthetic_trades(split_instrument)));
    // Seeds only `EngineState` metadata (the trading-summary start). The backtest's
    // `HistoricalClock` is seeded independently from `min(first market event, first aux event)` —
    // here midnight UTC (the split instant), *before* this 22:00 value.
    let time_engine_start = ts("2025-03-24T22:00:00Z");

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    // Construct the corporate-action event. Obligations 1, 3, 4 are all visible here.
    let effective_date = NaiveDate::from_ymd_opt(2025, 3, 24).unwrap();
    // 4: resolve the effective *date* to the effective *instant*. Midnight UTC lands the adjustment
    //    in the overnight gap, after the prior session and before the effective one — where a broker
    //    applies it. The clock advances to this instant in backtest. Computed once: it is both the
    //    event's `effective_time` and the merge-sort key (the same instant, so the split interleaves
    //    at the right point).
    let effective_time = split_effective_instant(effective_date);
    let split = Timed::new(
        EngineEvent::CorporateAction {
            // 1: a unique action id (the sole idempotency key). A same-day correction would be a
            //    second event with a *different* id (a reversal followed by the corrected split).
            id: "EXAMPLE-2025-03-24-split".into(),
            instrument: split_instrument,
            // A 2-for-1 forward split: `ratio = split_to / split_from = 2`.
            kind: CorporateActionKind::StockSplit { ratio: dec!(2) },
            // 3: rounding policy matching the broker. `Floor` = whole-share broker (disposes the
            //    fractional sliver as cash-in-lieu, reported via `SplitRemainder`); `Fractional` =
            //    fractional-share broker (no remainder).
            policy: SplitRoundingPolicy::Floor,
            effective_time,
        },
        effective_time,
    );

    // Supply the split via the aux source. `AuxEventsInMemory::new` asserts ascending-by-time order;
    // a single event is trivially sorted. `NoAuxEvents` (the default) would inject nothing.
    let aux_events = AuxEventsInMemory::new(Arc::new(vec![split]));

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events,
    });

    let args_dynamic = BacktestArgsDynamic {
        id: "corporate-action-injection".into(),
        risk_free_return: dec!(0.05),
        strategy: DefaultStrategy::<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>::default(),
        risk: DefaultRiskManager::<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>::default(),
    };

    let summary = backtest(args_constant, args_dynamic)
        .await
        .expect("backtest with an injected corporate action must complete");

    println!(
        "Backtest '{}' completed with an injected 2:1 split.",
        summary.id
    );
    summary.trading_summary.print_summary();
}

/// Live-injection sketch — **not executed** by `main` (it needs a running broker/market connection),
/// shown to document the live path.
///
/// In a live `System` (built via `SystemBuilder`, e.g.
/// `examples/engine_sync_with_live_market_data_and_mock_execution_and_audit.rs`), `system.feed_tx`
/// is a public [`UnboundedTx`] of `EngineEvent`. Once the wrapper has confirmed the broker applied a
/// split, it constructs the *same* event as above and sends it directly. There is no `From`
/// shortcut — the variant is `#[from(skip)]` precisely so every field (`id`, `policy`,
/// `effective_time`) is supplied consciously.
///
/// Inject **once**, after the broker has applied the action and before processing new fills on the
/// post-split scale.
// Defined for documentation; never called (a live System cannot be constructed in this offline
// example), so it would otherwise trip `dead_code`.
#[allow(dead_code)]
fn live_injection_sketch(
    feed_tx: &UnboundedTx<EngineEvent>,
    instrument: InstrumentIndex,
    ratio: Decimal,
    effective_date: NaiveDate,
) {
    let event = EngineEvent::CorporateAction {
        id: format!("AAPL-{effective_date}-split").into(),
        instrument,
        kind: CorporateActionKind::StockSplit { ratio },
        policy: SplitRoundingPolicy::Fractional,
        effective_time: split_effective_instant(effective_date),
    };

    // `Tx::send` accepts anything `Into<EngineEvent>`; the fully-constructed event qualifies
    // reflexively. The engine must be alive to receive it.
    feed_tx
        .send(event)
        .expect("engine feed receiver must be alive");
}

/// Four trades for `instrument` spanning 22:00–23:00; the midnight-UTC split sorts before all of
/// them on the effective date, so the adjustment is applied before the day's first fill.
fn synthetic_trades(
    instrument: InstrumentIndex,
) -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    [
        ("2025-03-24T22:00:00Z", dec!(60_000)),
        ("2025-03-24T22:15:00Z", dec!(60_100)),
        ("2025-03-24T22:45:00Z", dec!(60_200)),
        ("2025-03-24T23:00:00Z", dec!(60_300)),
    ]
    .into_iter()
    .map(|(time, price)| {
        let time = ts(time);
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceSpot,
            instrument,
            kind: DataKind::Trade(PublicTrade {
                id: "trade".into(),
                price,
                amount: dec!(0.01),
                side: None,
            }),
        })
    })
    .collect()
}

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

fn load_config() -> Config {
    let file = std::fs::File::open(CONFIG_PATH).expect("backtest_config.json must exist");
    serde_json::from_reader(std::io::BufReader::new(file))
        .expect("backtest_config.json must deserialize")
}
