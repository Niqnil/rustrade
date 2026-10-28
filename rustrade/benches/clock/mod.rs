#![allow(clippy::unwrap_used, clippy::expect_used)] // Benchmark code: panics acceptable

//! The cost of a `HistoricalClock`'s reads and advances, which a backtest and a `SimStepper` make
//! for every event.
//!
//! - `read`: one `time()`.
//! - `advance_and_read`: one `advance_to` a later instant, then three `time()`, roughly what one
//!   event costs the engine and the simulated venue.
//! - `process_same_instant`: one `process` of an event at the clock's instant, as for every event
//!   after the first that shares a timestamp.
//! - `per_thread/<n>`: `advance_and_read` on `n` threads at once, each on its own clock, the
//!   clocks allocated one after another, as parallel backtests or training workers in one process
//!   run. Reported per thread, so a value above the single-threaded one is interference between
//!   the clocks, such as two sharing a cache line.

use chrono::{DateTime, TimeDelta, Utc};
use criterion::Criterion;
use rustrade::{
    EngineEvent,
    engine::{
        Processor,
        clock::{EngineClock, HistoricalClock},
    },
};
use rustrade_data::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};
use std::{
    hint::black_box,
    sync::Barrier,
    time::{Duration, Instant},
};

criterion::criterion_main!(benchmark_clock);

criterion::criterion_group!(
    name = benchmark_clock;
    config = Criterion::default();
    targets = bench_single_thread, bench_per_thread
);

const THREADS: [usize; 2] = [2, 8];

fn start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap()
}

/// One advance to the next nanosecond, then three reads.
fn advance_and_read(clock: &HistoricalClock, time: &mut DateTime<Utc>) {
    *time += TimeDelta::nanoseconds(1);
    clock.advance_to(black_box(*time));
    for _ in 0..3 {
        black_box(clock.time());
    }
}

fn bench_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("clock");

    let clock = HistoricalClock::new(start());
    group.bench_function("read", |b| b.iter(|| black_box(&clock).time()));

    let clock = HistoricalClock::new(start());
    let mut time = start();
    group.bench_function("advance_and_read", |b| {
        b.iter(|| advance_and_read(black_box(&clock), &mut time))
    });

    let mut clock = HistoricalClock::new(start());
    let event: EngineEvent<()> = EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: start(),
        time_received: start(),
        exchange: ExchangeId::Mock,
        instrument: InstrumentIndex::new(0),
        kind: (),
    }));
    group.bench_function("process_same_instant", |b| {
        b.iter(|| black_box(&mut clock).process(black_box(&event)))
    });

    group.finish();
}

fn bench_per_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("clock/per_thread");
    for threads in THREADS {
        group.bench_function(threads.to_string(), |b| {
            b.iter_custom(|iters| per_thread(threads, iters))
        });
    }
    group.finish();
}

/// Runs `iters` of `advance_and_read` on each of `threads` threads, each on its own clock, and
/// returns the slowest thread's time.
fn per_thread(threads: usize, iters: u64) -> Duration {
    // Allocated back to back, before any thread starts.
    let clocks: Vec<_> = (0..threads).map(|_| HistoricalClock::new(start())).collect();
    let barrier = Barrier::new(threads);

    std::thread::scope(|scope| {
        let handles: Vec<_> = clocks
            .iter()
            .map(|clock| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut time = start();
                    barrier.wait();
                    let begin = Instant::now();
                    for _ in 0..iters {
                        advance_and_read(clock, &mut time);
                    }
                    begin.elapsed()
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .max()
            .unwrap()
    })
}
