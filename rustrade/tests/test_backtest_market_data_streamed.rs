#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the streaming backtest market-data seam.
//!
//! These tests drive [`MarketDataStreamed`] end-to-end through the public [`backtest`] /
//! [`run_backtests`] API — the only place the *whole* failure contract is observable. The pieces
//! are unit-tested elsewhere and neither piece proves the contract on its own:
//! - `backtest::market_data::tests` proves the source resolves, caches and re-yields correctly.
//! - `backtest::tests` proves the merge records a source error in the shared slot and ends.
//!
//! What only this file proves is the join between them: that a mid-stream source failure travels
//! out of a stream consumed by a **spawned forwarding task with no return path**, survives a normal
//! shutdown, and comes back to the caller as `Err` in place of a `BacktestSummary`. That is the
//! whole point of making the stream item fallible — a truncated read must never be reported as a
//! complete run — and it is one `if let` in `backtest` away from silently regressing to a summary
//! computed over partial data.
//!
//! The happy-path test doubles as end-to-end coverage for `DefaultInstrumentMarketData` consuming
//! `DataKind::Candle`: the struct-level precedence rules are unit-tested in
//! `engine::state::instrument::data::tests`, but nothing else proves a candle actually reaches
//! instrument state through the real merge/engine path.

use std::{
    fs::File,
    io::BufReader,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataStreamed},
        run_backtests,
    },
    engine::state::{
        EngineState,
        builder::EngineStateBuilder,
        global::DefaultGlobalData,
        instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
        trading::TradingState,
    },
    error::BarterError,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::candle::Candle,
};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use serde::Deserialize;

const CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/config/backtest_config.json"
);

type StreamedBacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

/// One scripted market stream, replayed verbatim by the factory on every call.
type Script = Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>>;
type ScriptStream = futures::stream::Iter<std::vec::IntoIter<ScriptItem>>;
type ScriptItem = Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>;

#[derive(Deserialize)]
struct Config {
    // `risk_free_return` is also present in the JSON but unused here; serde ignores it.
    system: SystemConfig,
}

fn load_config() -> Config {
    let reader = BufReader::new(File::open(CONFIG_PATH).expect("backtest_config.json must exist"));
    serde_json::from_reader(reader).expect("backtest_config.json must deserialize")
}

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// A candle `MarketEvent` for BTCUSDT (instrument index 0), stamped at its `close_time`.
fn candle(close_time: &str, close: Decimal) -> ScriptItem {
    candle_at(ts(close_time), close)
}

/// [`candle`] for a timestamp that is computed rather than written out.
fn candle_at(close_time: DateTime<Utc>, close: Decimal) -> ScriptItem {
    Ok(MarketStreamEvent::Item(MarketEvent {
        time_exchange: close_time,
        time_received: close_time,
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex::new(0),
        kind: DataKind::Candle(Candle {
            close_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }),
    }))
}

/// The failure a streaming source reports mid-read: a page fetch, a decode, a truncated file.
fn source_failure() -> BarterError {
    BarterError::BacktestMarketData("page 3 fetch failed".to_string())
}

/// A stream factory replaying `script`, counting how many times it is called.
///
/// Stands in for a real streaming source (a Parquet reader, a paginated provider fetch) without
/// touching the network or the filesystem — the source-agnostic half of the seam is all `backtest`
/// can observe.
fn factory(
    script: Script,
    calls: Arc<AtomicUsize>,
) -> impl Fn() -> std::future::Ready<Result<ScriptStream, BarterError>> {
    move || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(futures::stream::iter(script.clone())))
    }
}

/// A factory whose stream yields one item per `pace`, counting items as they leave it.
///
/// Stands in for a source that is genuinely mid-fetch — a paginated provider call between pages —
/// which is the only state in which cancelling a run is observable at all. `futures::stream::iter`
/// is ready on every poll, so a run backed by it can finish before a sibling has even failed.
/// `mid_stream` fires once this stream has yielded `signal_after` items, which is what lets a
/// sibling fail at a point where this run is provably mid-read rather than not yet started.
fn paced_factory(
    script: Script,
    pace: std::time::Duration,
    yielded: Arc<AtomicUsize>,
    mid_stream: Arc<tokio::sync::Notify>,
    signal_after: usize,
) -> impl Fn() -> std::future::Ready<Result<futures::stream::BoxStream<'static, ScriptItem>, BarterError>>
{
    move || {
        let yielded = Arc::clone(&yielded);
        let mid_stream = Arc::clone(&mid_stream);
        // Per-stream, not shared: the factory is called once at construction (which reads a single
        // item and stops) and again per run, and only a run's own progress means "mid-read".
        let this_stream = Arc::new(AtomicUsize::new(0));

        let stream = futures::StreamExt::then(futures::stream::iter(script.clone()), move |item| {
            let yielded = Arc::clone(&yielded);
            let mid_stream = Arc::clone(&mid_stream);
            let this_stream = Arc::clone(&this_stream);
            async move {
                tokio::time::sleep(pace).await;
                yielded.fetch_add(1, Ordering::SeqCst);
                if this_stream.fetch_add(1, Ordering::SeqCst) + 1 == signal_after {
                    mid_stream.notify_one();
                }
                item
            }
        });

        std::future::ready(Ok(futures::StreamExt::boxed(stream)))
    }
}

/// A factory whose stream replays `script` and then blocks on `mid_stream` before failing.
///
/// The handshake is what makes "cancelled mid-stream" a fact rather than a race: without it this
/// source fails within a poll or two, long before a paced sibling has read anything, and the run
/// under test is cancelled before it ever started.
fn gated_failure_factory(
    script: Script,
    mid_stream: Arc<tokio::sync::Notify>,
) -> impl Fn() -> std::future::Ready<Result<futures::stream::BoxStream<'static, ScriptItem>, BarterError>>
{
    move || {
        let mid_stream = Arc::clone(&mid_stream);
        let stream = futures::StreamExt::chain(
            futures::stream::iter(script.clone()),
            futures::stream::once(async move {
                mid_stream.notified().await;
                Err(source_failure())
            }),
        );

        std::future::ready(Ok(futures::StreamExt::boxed(stream)))
    }
}

/// Build backtest constants around any market data source, seeding the clock from its first event.
async fn args_constant<MarketData>(
    market_data: MarketData,
) -> Arc<BacktestArgsConstant<MarketData, Daily, StreamedBacktestState, NoAuxEvents>>
where
    MarketData: BacktestMarketData<Kind = DataKind>,
{
    let Config {
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    let instruments = IndexedInstruments::new(instruments);
    let time_engine_start = market_data.time_first_event().await.unwrap();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    })
}

fn args_dynamic(
    id: &str,
) -> BacktestArgsDynamic<
    DefaultStrategy<StreamedBacktestState>,
    DefaultRiskManager<StreamedBacktestState>,
> {
    BacktestArgsDynamic {
        id: id.into(),
        risk_free_return: dec!(0.05),
        strategy: DefaultStrategy::default(),
        risk: DefaultRiskManager::default(),
    }
}

/// The healthy path: a streamed source runs to completion and its events reach instrument state.
///
/// Asserting the terminal `data` — not merely that the run finished — is what distinguishes "the
/// stream was consumed" from "the stream was opened and dropped": a source that yielded nothing
/// would still produce a summary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_source_drives_a_complete_backtest_and_reaches_instrument_state() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            candle("2025-03-24T22:30:00Z", dec!(60_100)),
            candle("2025-03-24T23:00:00Z", dec!(60_200)),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let result = backtest(args_constant(market_data).await, args_dynamic("streamed"))
        .await
        .expect("a healthy streamed source must run to completion");

    assert_eq!(result.summary.id, "streamed");

    let data = &result
        .engine_state
        .instruments
        .instrument_index(&InstrumentIndex::new(0))
        .data;

    // The last scripted candle is the one held, and `price()` resolves to its close — proving the
    // candles traversed factory -> merge -> forwarding task -> engine -> instrument state.
    assert_eq!(
        data.candle.unwrap().close_time,
        ts("2025-03-24T23:00:00Z"),
        "the most recent streamed candle must be the one retained"
    );
    assert_eq!(data.price(), Some(dec!(60_200)));
}

/// **The contract this file exists for.** A mid-stream source failure aborts the run and surfaces
/// as `Err`, rather than a `BacktestSummary` computed over the prefix that happened to be read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_stream_source_failure_aborts_the_backtest() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            candle("2025-03-24T22:30:00Z", dec!(60_100)),
            Err(source_failure()),
            candle("2025-03-24T23:00:00Z", dec!(60_200)),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let error = backtest(args_constant(market_data).await, args_dynamic("truncated"))
        .await
        .expect_err("a mid-stream source failure must abort the run, not summarise a prefix");

    assert_eq!(error, source_failure());
}

/// The abort propagates out of the concurrent sweep too — `run_backtests` must not return a
/// `MultiBacktestSummary` in which one run silently covered less data than the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_stream_source_failure_aborts_a_run_backtests_sweep() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            Err(source_failure()),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let error = run_backtests(
        args_constant(market_data).await,
        [args_dynamic("sweep-a"), args_dynamic("sweep-b")],
    )
    .await
    .expect_err("a failing source must fail the whole sweep");

    assert_eq!(error, source_failure());
}

/// A run cancelled because a sibling failed must take its whole task tree with it.
///
/// `run_backtests` short-circuits on the first `Err`, dropping the other runs' futures. Dropping a
/// `JoinHandle` detaches its task rather than cancelling it, so without the abort guard in
/// `backtest` the cancelled run's engine, execution-manager, mock-exchange and account-forwarding
/// tasks survive — and cannot finish on their own, because the `Shutdown` that ends the engine and
/// the abort that ends `account_to_engine` are both sent by the graceful path the drop skipped.
/// They then park forever holding their `EngineState`, and every failing sweep adds another set.
///
/// This drives `try_join_all` directly rather than `run_backtests`, because `run_backtests` takes
/// one `BacktestArgsConstant` for the whole batch and so cannot give two runs different sources.
/// The combinator is the entirety of what `run_backtests` adds over `backtest`, so exercising it
/// with two independent runs is the same code path — and it is the only way to have one run fail
/// while another is provably still mid-stream. The existing sweep test above gives both runs the
/// same failing script, so both fail at once and neither is ever cancelled mid-flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_cancelled_by_a_failing_sibling_does_not_leak_its_tasks() {
    const VICTIM_EVENTS: usize = 200;
    /// How far into its source the victim must be before the trigger is allowed to fail.
    ///
    /// Small next to `VICTIM_EVENTS`, so the run is unambiguously *mid*-stream in both directions:
    /// it has provably started, and 195 events remain for the cancellation to cut short.
    const VICTIM_READS_BEFORE_FAILURE: usize = 5;

    // Fails only once the victim signals it is mid-read; see `gated_failure_factory`.
    let mid_stream = Arc::new(tokio::sync::Notify::new());
    let trigger = MarketDataStreamed::<_, DataKind>::new(gated_failure_factory(
        vec![candle("2025-03-24T22:00:00Z", dec!(60_000))],
        Arc::clone(&mid_stream),
    ))
    .await
    .unwrap();

    // Long and slow, so it is unambiguously still reading when the trigger fails.
    let yielded = Arc::new(AtomicUsize::new(0));
    let first_close = ts("2025-03-24T22:00:00Z");
    let victim = MarketDataStreamed::<_, DataKind>::new(paced_factory(
        (0..VICTIM_EVENTS)
            .map(|index| {
                candle_at(
                    first_close + chrono::TimeDelta::minutes(index as i64),
                    dec!(60_000),
                )
            })
            .collect(),
        std::time::Duration::from_millis(20),
        Arc::clone(&yielded),
        Arc::clone(&mid_stream),
        VICTIM_READS_BEFORE_FAILURE,
    ))
    .await
    .unwrap();

    let metrics = tokio::runtime::Handle::current().metrics();
    let baseline = metrics.num_alive_tasks();

    let trigger_args = args_constant(trigger).await;
    let victim_args = args_constant(victim).await;
    let consumed_before = yielded.load(Ordering::SeqCst);

    let error = futures::future::try_join_all([
        futures::future::Either::Left(backtest(trigger_args, args_dynamic("trigger"))),
        futures::future::Either::Right(backtest(victim_args, args_dynamic("victim"))),
    ])
    .await
    .expect_err("the failing run must fail the join");
    assert_eq!(error, source_failure());

    // Aborts land asynchronously, and a multi-threaded runtime's task count is not instantaneously
    // consistent, so settle rather than sample once. Under a leak this never converges: the parked
    // tasks are waiting on channels that can no longer be closed by anyone.
    //
    // The deadline bounds only that leaking case — a healthy run converges within a poll or two of
    // the abort, and pays none of it. It is therefore set for headroom on a contended CI runner
    // (many test binaries, each with its own multi-threaded runtime) rather than tuned to how long
    // the teardown actually takes: a tight ceiling here buys nothing and turns scheduler latency
    // into a flake.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while metrics.num_alive_tasks() > baseline && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(
        metrics.num_alive_tasks(),
        baseline,
        "the cancelled run left tasks alive"
    );

    // The point of cancelling rather than letting siblings finish: a metered source stops being
    // read. Without that, this reaches the full script.
    let consumed = yielded.load(Ordering::SeqCst) - consumed_before;
    assert!(
        consumed < VICTIM_EVENTS,
        "the cancelled run consumed its whole source ({consumed} of {VICTIM_EVENTS} events)"
    );
    // Both bounds, because `consumed < VICTIM_EVENTS` alone is also satisfied by `consumed == 0` --
    // a victim whose stream never started at all, which would make this test pass while proving
    // nothing about cancellation *mid-stream*, the only interesting case. The handshake guarantees
    // the lower bound rather than leaving it to scheduling.
    assert!(
        consumed >= VICTIM_READS_BEFORE_FAILURE,
        "the cancelled run had read {consumed} events, fewer than the {VICTIM_READS_BEFORE_FAILURE} \
         it signalled at, so this proves nothing about mid-stream cancellation"
    );
}

/// The documented 1 + N cost model, measured through the real `backtest` path rather than by
/// calling `stream()` directly: construction reads once, and every run reads again.
///
/// Also covers the per-run error slot: the second run succeeds on an `args_constant` a first run
/// has already consumed, which a slot shared across runs (rather than created inside `backtest`)
/// would not guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_backtest_re_reads_the_source() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![candle("2025-03-24T22:00:00Z", dec!(60_000))],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "construction reads once");

    let args_constant = args_constant(market_data).await;
    backtest(Arc::clone(&args_constant), args_dynamic("first"))
        .await
        .unwrap();
    backtest(args_constant, args_dynamic("second"))
        .await
        .unwrap();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "1 construction + 1 read per backtest - the documented 1 + N cost of a streamed source"
    );
}
