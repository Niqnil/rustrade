//! Decoder tests for London Strategic Edge bulk-export artifacts.
//!
//! Every fixture here is **written by this test file** using the `parquet` crate's own writer,
//! shaped from measurements of the live API. No provider data is committed — the provider
//! prohibits redistribution (<https://londonstrategicedge.com/terms>).
//!
//! What is measured is the *shape*: which columns each dataset publishes, their types and
//! nullability, and what `ts` means. **Every number is invented** — deliberately round and
//! obviously synthetic, so that no row here can be mistaken for a real quote or bar. Decoding is
//! value-independent, so nothing is lost by that: a fixture priced at `1.1` exercises exactly the
//! same code path as one priced at a real tick.
//!
//! The measured facts these encode:
//!
//! - The tick schema **varies by dataset**: `fx` is `{ts, symbol, bid, ask}`, `stocks` is
//!   `{ts, symbol, price, volume}`, and the synthetic classes are `{ts, symbol, price, volume,
//!   ask}` with `volume`/`ask` nullable.
//! - On those synthetic classes `volume` was measured as a literal `0.0` on **every** sampled row —
//!   never null, despite the column being nullable. The layout discards the column and reports only
//!   a value other than that zero, so keying the report on non-null instead would fire on every
//!   real artifact.
//! - The candle schema varies too: `etf` carries `volume`, `fx` omits the column entirely.
//! - `price` is the **bid** (the provider's price endpoint returns `price == bid` on every symbol
//!   tested), so `price` beside an `ask` is a quote.
//! - `ts` is the bar's **open** time, and timestamps are **non-decreasing**, not strictly
//!   ascending — measured on the *tick* tapes, where several prints routinely share a microsecond.
//!   Candle artifacts are a different shape: two bars of one series can share neither an open nor
//!   the close derived from it, and their spacing is the only evidence of the resolution the file
//!   was written at, since nothing in an artifact records it.
//!
//! Run with: `cargo test --test lse_parquet --features lse-parquet`

#![cfg(feature = "lse-parquet")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::export::{LseExport, LseExportRange, LseExportTimeframe};
use rustrade_data::exchange::lse::market::{LseDataset, instrument_index_for};
use rustrade_data::exchange::lse::parquet::{read_export, symbols_in_export};
use rustrade_data::subscription::candle::CandleInterval;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;
use rustrade_instrument::instrument::InstrumentIndex;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_subscriber::layer::SubscriberExt;

/// A column of values to write: `None` entries become SQL nulls (OPTIONAL columns only).
enum Col {
    Ts(Vec<i64>),
    Sym(Vec<&'static str>),
    Dbl(Vec<f64>),
    OptDbl(Vec<Option<f64>>),
}

/// Write a Parquet file with the given schema and columns, as one row group.
fn write_parquet(path: &Path, message: &str, columns: Vec<Col>) {
    write_parquet_row_groups(path, message, vec![columns]);
}

/// Write a Parquet file whose rows are split across one row group per element of `groups`.
///
/// Every measured artifact holds exactly one, but that is a property of the provider's writer and
/// not a guarantee of the format, so the decoder walks groups — and something has to exercise the
/// walk.
fn write_parquet_row_groups(path: &Path, message: &str, groups: Vec<Vec<Col>>) {
    write_parquet_row_groups_with(path, message, groups, EnabledStatistics::Page);
}

/// As [`write_parquet_row_groups`], but choosing whether the writer emits column statistics.
///
/// The discarded-`volume` check reads column-chunk statistics rather than decoding the column, so
/// the branch where a writer emits none is reachable only through here. Every other fixture takes
/// the default, which does emit them.
fn write_parquet_row_groups_with(
    path: &Path,
    message: &str,
    groups: Vec<Vec<Col>>,
    statistics: EnabledStatistics,
) {
    let schema = Arc::new(parse_message_type(message).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_statistics_enabled(statistics)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();

    for columns in groups {
        let mut group = writer.next_row_group().unwrap();

        for column in columns {
            let mut col = group.next_column().unwrap().unwrap();
            match column {
                Col::Ts(values) => {
                    col.typed::<Int64Type>()
                        .write_batch(&values, None, None)
                        .unwrap();
                }
                Col::Sym(values) => {
                    let values: Vec<ByteArray> = values.iter().map(|s| (*s).into()).collect();
                    col.typed::<ByteArrayType>()
                        .write_batch(&values, None, None)
                        .unwrap();
                }
                Col::Dbl(values) => {
                    col.typed::<DoubleType>()
                        .write_batch(&values, None, None)
                        .unwrap();
                }
                Col::OptDbl(values) => {
                    // Top-level OPTIONAL: definition level 1 means present, 0 means null, and only
                    // the present values are supplied.
                    let def: Vec<i16> = values.iter().map(|v| i16::from(v.is_some())).collect();
                    let present: Vec<f64> = values.iter().flatten().copied().collect();
                    col.typed::<DoubleType>()
                        .write_batch(&present, Some(&def), None)
                        .unwrap();
                }
            }
            col.close().unwrap();
        }

        group.close().unwrap();
    }

    writer.close().unwrap();
}

fn dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn range() -> LseExportRange {
    LseExportRange::new("2026-07-01".parse().unwrap(), "2026-07-03".parse().unwrap()).unwrap()
}

/// 2026-07-01T00:00:00Z in microseconds — the first day of [`range`].
const T0: i64 = 1_782_864_000_000_000;
const MINUTE: i64 = 60_000_000;
const HOUR: i64 = 3_600_000_000;
const DAY: i64 = 86_400_000_000;

/// 2026-01-30T00:00:00Z in microseconds.
///
/// January, not July, because the calendar-interval exemptions are only visible across a short
/// month: `2026-01-30 + 1mo` and `2026-01-31 + 1mo` both clamp to `2026-02-28`. The decoder does
/// not check rows against the descriptor's range, so a fixture outside [`range`] is decoded the
/// same as one inside it.
const JAN_30: i64 = 1_769_731_200_000_000;

fn export(path: PathBuf, dataset: LseDataset, symbol: &str, tf: LseExportTimeframe) -> LseExport {
    LseExport::new(path, dataset, symbol, tf, range())
}

fn idx() -> InstrumentIndex {
    InstrumentIndex::new(0)
}

/// Run `decode`, returning its value and the number of `WARN` events it emitted on this thread.
///
/// Some decoder facts are reported *only* as a log line, because they are worth telling the caller
/// about but must not fail the decode. Those are unobservable in the decoded output by
/// construction, so a test that asserts only on output cannot tell a working report from a deleted
/// one.
fn count_warnings<T>(decode: impl FnOnce() -> T) -> (T, usize) {
    #[derive(Default)]
    struct CountWarnings(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountWarnings {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let layer = CountWarnings::default();
    let count = Arc::clone(&layer.0);

    // Thread-local rather than global: the default subscriber can only be set once per process, and
    // every other test in this binary must stay unaffected.
    let value =
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), decode);

    (value, count.load(Ordering::Relaxed))
}

// ── the three measured tick layouts ──────────────────────────────────────────

const FX_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE bid;
  REQUIRED DOUBLE ask;
}";

const STOCK_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

const SYNTH_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  OPTIONAL DOUBLE volume;
  OPTIONAL DOUBLE ask;
}";

const CANDLE_WITH_VOLUME: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE open;
  REQUIRED DOUBLE high;
  REQUIRED DOUBLE low;
  REQUIRED DOUBLE close;
  REQUIRED DOUBLE volume;
}";

const CANDLE_NO_VOLUME: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE open;
  REQUIRED DOUBLE high;
  REQUIRED DOUBLE low;
  REQUIRED DOUBLE close;
}";

#[test]
fn an_fx_tick_export_decodes_to_a_two_sided_quote() {
    let dir = dir();
    let path = dir.path().join("fx.parquet");
    write_parquet(
        &path,
        FX_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR]),
            Col::Sym(vec!["EUR/USD", "EUR/USD"]),
            Col::Dbl(vec![1.1, 1.3]),
            Col::Dbl(vec![1.2, 1.4]),
        ],
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].exchange, ExchangeId::LseFx);
    let DataKind::OrderBookL1(book) = &events[0].kind else {
        panic!("expected OrderBookL1, got {:?}", events[0].kind);
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(1.1));
    assert_eq!(book.best_ask.unwrap().price, dec!(1.2));
}

#[test]
fn a_stocks_tick_export_decodes_to_a_trade() {
    let dir = dir();
    let path = dir.path().join("stocks.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![20.5]),
            Col::Dbl(vec![100.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events[0].exchange, ExchangeId::LseEquities);
    let DataKind::Trade(trade) = &events[0].kind else {
        panic!("expected Trade, got {:?}", events[0].kind);
    };
    assert_eq!(trade.price, dec!(20.5));
    assert_eq!(trade.amount, dec!(100));
    // No aggressor side is published, and a bid-side price cannot imply one.
    assert!(trade.side.is_none());
    // No trade identifier exists on the tape; timestamps tie, so one cannot be synthesised.
    assert!(trade.id.is_empty());
}

#[test]
fn a_synth_tick_export_decodes_price_as_the_bid() {
    // `price` sits beside an `ask` and the provider's price endpoint reports `price == bid`, so
    // this layout is a quote despite the column name.
    let dir = dir();
    let path = dir.path().join("synth.parquet");
    write_parquet(
        &path,
        SYNTH_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR]),
            Col::Sym(vec!["VIX/USD", "VIX/USD"]),
            Col::Dbl(vec![2.0, 2.5]),
            // The measured state of this column: a literal `0.0`, not a null. The layout discards
            // it and warns only on a real size — see the sibling test below.
            Col::OptDbl(vec![Some(0.0), None]),
            Col::OptDbl(vec![Some(2.1), None]),
        ],
    );

    let export = export(
        path,
        LseDataset::Volatility,
        "VIX/USD",
        LseExportTimeframe::Tick,
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events[0].exchange, ExchangeId::LseCfd);
    let DataKind::OrderBookL1(book) = &events[0].kind else {
        panic!("expected OrderBookL1");
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(2.0));
    assert_eq!(book.best_ask.unwrap().price, dec!(2.1));

    // A null ask yields a one-sided book rather than a fabricated price.
    let DataKind::OrderBookL1(book) = &events[1].kind else {
        panic!("expected OrderBookL1");
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(2.5));
    assert!(book.best_ask.is_none());
}

/// Decode a synth tick artifact whose `volume` column holds `volumes`, returning the decoded
/// events and how many `WARN`s the decode emitted.
fn decode_synth_with_volumes(name: &str, volumes: Vec<Option<f64>>) -> (Vec<DataKind>, usize) {
    decode_synth_with_volumes_stats(name, volumes, EnabledStatistics::Page)
}

fn decode_synth_with_volumes_stats(
    name: &str,
    volumes: Vec<Option<f64>>,
    statistics: EnabledStatistics,
) -> (Vec<DataKind>, usize) {
    let dir = dir();
    let path = dir.path().join(name);
    let rows = volumes.len();
    write_parquet_row_groups_with(
        &path,
        SYNTH_TICK,
        vec![vec![
            Col::Ts((0..rows as i64).map(|row| T0 + row * HOUR).collect()),
            Col::Sym(vec!["VIX/USD"; rows]),
            Col::Dbl(vec![2.0; rows]),
            Col::OptDbl(volumes),
            Col::OptDbl(vec![Some(2.1); rows]),
        ]],
        statistics,
    );

    let export = export(
        path,
        LseDataset::Volatility,
        "VIX/USD",
        LseExportTimeframe::Tick,
    );

    let (events, warnings) = count_warnings(|| {
        read_export(&export, idx())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    });

    (
        events.into_iter().map(|event| event.kind).collect(),
        warnings,
    )
}

/// A `volume` this layout has nowhere to put must not become a decode failure — and the drift
/// report must fire on a real size, stay silent on the measured zero, and do so once per artifact.
///
/// The `warn!` is the *only* observable difference the check makes: it cannot alter the decoded
/// quote, by design. So asserting on decoded output alone would pass identically whether the
/// trigger keyed on non-zero, on non-null, or on nothing at all — which is exactly how this check
/// could regress unnoticed. Counting the event is what pins it.
#[test]
fn a_populated_volume_warns_once_per_artifact_while_the_measured_zero_stays_silent() {
    // Every fixture below writes the same bid and ask, so the quote is checked identically in all
    // three: whatever `volume` holds, a column this layout discards must cost observability only.
    let assert_quotes = |kinds: &[DataKind], rows: usize| {
        assert_eq!(kinds.len(), rows);
        for kind in kinds {
            let DataKind::OrderBookL1(book) = kind else {
                panic!("expected OrderBookL1, got {kind:?}");
            };
            assert_eq!(book.best_bid.unwrap().price, dec!(2.0));
            assert_eq!(book.best_ask.unwrap().price, dec!(2.1));
        }
    };

    // The measured state of the column. Reporting this would warn on every real artifact.
    let (kinds, warnings) =
        decode_synth_with_volumes("zero.parquet", vec![Some(0.0), Some(0.0), Some(0.0)]);
    assert_eq!(
        warnings, 0,
        "a measured zero is not drift and must not warn"
    );
    assert_quotes(&kinds, 3);

    // Absent entirely — also not drift.
    let (kinds, warnings) = decode_synth_with_volumes("null.parquet", vec![None, None]);
    assert_eq!(warnings, 0, "a null volume must not warn");
    assert_quotes(&kinds, 2);

    // Real sizes on every row: the provider started using a column this layout discards.
    let (kinds, warnings) =
        decode_synth_with_volumes("sized.parquet", vec![Some(500.0), Some(600.0), Some(700.0)]);
    assert_eq!(
        warnings, 1,
        "drift is reported once per artifact, not once per row"
    );
    assert_quotes(&kinds, 3);
}

/// The check reads column-chunk statistics, so a writer that emits none turns it off entirely.
///
/// That is a deliberate trade — decoding the column to recover the warning would reinstate the whole
/// cost the statistics path exists to avoid — but it is also the one way this check can silently
/// degrade to "keys on nothing at all" while every fixture above stays green, since they all take
/// the writer default, which does emit statistics. Pinned so the silence stays deliberate, and so
/// the decoded quote is still asserted correct on the path where the observability is gone.
#[test]
fn a_populated_volume_stays_silent_when_the_writer_emitted_no_statistics() {
    let (kinds, warnings) = decode_synth_with_volumes_stats(
        "no_stats.parquet",
        vec![Some(500.0), Some(600.0), Some(700.0)],
        EnabledStatistics::None,
    );

    assert_eq!(
        warnings, 0,
        "with no statistics to read, the drift check cannot fire — documented on the method"
    );
    assert_eq!(kinds.len(), 3);
    for kind in &kinds {
        let DataKind::OrderBookL1(book) = kind else {
            panic!("expected OrderBookL1, got {kind:?}");
        };
        assert_eq!(book.best_bid.unwrap().price, dec!(2.0));
        assert_eq!(book.best_ask.unwrap().price, dec!(2.1));
    }
}

// ── candles ──────────────────────────────────────────────────────────────────

#[test]
fn a_candle_export_derives_close_time_from_the_bars_open_time() {
    // `ts` is the bar's OPEN time; the library's contract is an exclusive `close_time`, so passing
    // `ts` through would shift every bar by one period.
    let dir = dir();
    let path = dir.path().join("etf.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["SPY"]),
            Col::Dbl(vec![100.0]),
            Col::Dbl(vec![110.0]),
            Col::Dbl(vec![90.0]),
            Col::Dbl(vec![105.0]),
            Col::Dbl(vec![1000.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Etf,
        "SPY",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    // The `ts` column held 2026-07-01T00:00Z, so the exclusive close is the next midnight.
    assert_eq!(candle.close_time.to_rfc3339(), "2026-07-02T00:00:00+00:00");
    assert_eq!(candle.open, dec!(100));
    assert_eq!(candle.close, dec!(105));
    assert_eq!(candle.volume, Some(dec!(1000)));
    // Never published on any dataset.
    assert!(candle.trade_count.is_none());
    // `time_exchange` is the close, NOT the `ts` it was read from: a bar enters the timeline when
    // its period ends. Stamping the open would be silent lookahead, and would put this decoder out
    // of step with the candle replay path it is merged against.
    assert_eq!(events[0].time_exchange, candle.close_time);
    assert_eq!(events[0].time_received, candle.close_time);
}

#[test]
fn an_fx_candle_export_omitting_the_volume_column_yields_none() {
    // Measured: `candles_fx_1d` has SIX columns. A fixed-arity schema assert would reject the
    // provider's flagship dataset outright.
    let dir = dir();
    let path = dir.path().join("fxcandle.parquet");
    write_parquet(
        &path,
        CANDLE_NO_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["EUR/USD"]),
            Col::Dbl(vec![1.1]),
            Col::Dbl(vec![1.3]),
            Col::Dbl(vec![0.9]),
            Col::Dbl(vec![1.2]),
        ],
    );

    let export = export(
        path,
        LseDataset::Fx,
        "EUR/USD",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    assert_eq!(candle.volume, None);
}

#[test]
fn a_literal_zero_volume_is_passed_through_rather_than_rewritten_to_none() {
    // The provider reports 0 for a majority of one-minute equity bars that demonstrably had
    // trades. Rewriting that to `None` would be this library inventing a fact; `None` is reserved
    // for a column the provider does not publish at all.
    let dir = dir();
    let path = dir.path().join("zerovol.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![100.0]),
            Col::Dbl(vec![110.0]),
            Col::Dbl(vec![90.0]),
            Col::Dbl(vec![105.0]),
            Col::Dbl(vec![0.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Min1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    assert_eq!(candle.volume, Some(dec!(0)));
}

// ── batching and row groups ──────────────────────────────────────────────────

/// Mirrors the decoder's private batch width.
///
/// The decoder reads a fixed number of rows per column-reader call rather than a whole row group,
/// which is what bounds its memory over a multi-gigabyte artifact. Nothing below asserts this exact
/// figure — only that crossing it changes nothing — so the tests stay correct if it is retuned;
/// they merely stop straddling the boundary, which is why it is worth keeping in step.
const BATCH_ROWS: usize = 8 * 1024;

#[test]
fn an_artifact_longer_than_one_read_batch_decodes_every_row_in_order() {
    // Deliberately not a round multiple: the final batch is partial, and the row after the last
    // one has to end the iterator rather than read off the end of the buffers.
    let rows = BATCH_ROWS * 2 + 7;

    let mut ts = Vec::with_capacity(rows);
    let mut price = Vec::with_capacity(rows);
    let mut ask = Vec::with_capacity(rows);
    for index in 0..rows {
        let index = i32::try_from(index).unwrap();

        ts.push(T0 + i64::from(index));
        // Distinct per row, so a value paired with the wrong timestamp is visible rather than
        // coincidentally equal to the right one.
        price.push(f64::from(index));
        // Alternating nulls: an OPTIONAL column's value buffer is shorter than its batch, so the
        // per-column cursor has to survive a batch boundary landing mid-pattern. A cursor reset one
        // row early or late shifts every subsequent ask onto the wrong row.
        ask.push((index % 2 == 0).then(|| f64::from(index) + 0.5));
    }

    let dir = dir();
    let path = dir.path().join("long.parquet");
    write_parquet(
        &path,
        SYNTH_TICK,
        vec![
            Col::Ts(ts),
            Col::Sym(vec!["VIX/USD"; rows]),
            Col::Dbl(price),
            Col::OptDbl(vec![None; rows]),
            Col::OptDbl(ask),
        ],
    );

    let export = export(
        path,
        LseDataset::Volatility,
        "VIX/USD",
        LseExportTimeframe::Tick,
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(events.len(), rows);

    for (index, event) in events.iter().enumerate() {
        let expected = Decimal::from(i64::try_from(index).unwrap());

        assert_eq!(
            event.time_exchange.timestamp_micros(),
            T0 + i64::try_from(index).unwrap(),
            "row {index} decoded against the wrong timestamp"
        );

        let DataKind::OrderBookL1(book) = &event.kind else {
            panic!("expected OrderBookL1 on row {index}, got {:?}", event.kind);
        };
        assert_eq!(
            book.best_bid.unwrap().price,
            expected,
            "row {index} decoded the wrong bid"
        );
        assert_eq!(
            book.best_ask.map(|level| level.price),
            (index % 2 == 0).then(|| expected + dec!(0.5)),
            "row {index} decoded the wrong ask"
        );
    }
}

#[test]
fn an_artifact_written_as_several_row_groups_decodes_every_row_in_order() {
    let dir = dir();
    let path = dir.path().join("groups.parquet");
    write_parquet_row_groups(
        &path,
        FX_TICK,
        (0..3)
            .map(|group| {
                let base = T0 + i64::from(group) * DAY;

                vec![
                    Col::Ts(vec![base, base + HOUR]),
                    Col::Sym(vec!["EUR/USD"; 2]),
                    Col::Dbl(vec![f64::from(group), f64::from(group) + 0.5]),
                    Col::Dbl(vec![f64::from(group) + 0.1, f64::from(group) + 0.6]),
                ]
            })
            .collect(),
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Every group is walked, in file order, and the boundary between two of them is not an end.
    assert_eq!(events.len(), 6);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].time_exchange <= pair[1].time_exchange),
        "row groups must be decoded in file order"
    );

    let bids: Vec<_> = events
        .iter()
        .map(|event| {
            let DataKind::OrderBookL1(book) = &event.kind else {
                panic!("expected OrderBookL1, got {:?}", event.kind);
            };
            book.best_bid.unwrap().price
        })
        .collect();
    assert_eq!(
        bids,
        vec![dec!(0), dec!(0.5), dec!(1), dec!(1.5), dec!(2), dec!(2.5)]
    );
}

// ── invariants ───────────────────────────────────────────────────────────────

#[test]
fn tied_timestamps_are_accepted_because_they_are_the_common_case() {
    // 68% of adjacent rows tie on an equity tape. A strict-ascent assert would reject valid files.
    let dir = dir();
    let path = dir.path().join("ties.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0, T0, T0 + HOUR]),
            Col::Sym(vec!["AAPL"; 4]),
            Col::Dbl(vec![1.0, 2.0, 3.0, 4.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 4);
}

#[test]
fn tied_timestamps_on_a_candle_artifact_are_rejected_because_two_bars_cannot_share_an_open() {
    // The mirror of the test above, and the reason the rule is layout-dependent rather than one
    // constant. Two bars of one series cannot share an open, so a tie here is not the common case:
    // it means the file holds more than one series, or repeats a row.
    //
    // It does NOT mean the declared resolution is wrong — a fixed interval is a constant shift from
    // open to close, so it cancels out of this comparison entirely. That is a separate check on bar
    // spacing, covered below.
    let dir = dir();
    let path = dir.path().join("candleties.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0, T0]),
            Col::Sym(vec!["AAPL", "AAPL"]),
            Col::Dbl(vec![100.0, 100.0]),
            Col::Dbl(vec![110.0, 110.0]),
            Col::Dbl(vec![90.0, 90.0]),
            Col::Dbl(vec![105.0, 105.0]),
            Col::Dbl(vec![1000.0, 1000.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    assert!(matches!(error, LseError::NonMonotonicTimestamps { .. }));
}

#[test]
fn a_candle_artifact_finer_than_the_declared_interval_is_rejected() {
    // Nothing in a Parquet artifact records the resolution it was written at, so `close_time` is
    // derived from the caller's word. Declaring `Day1` over a file of 1-minute bars used to decode
    // cleanly: the bars ascend, the closes are distinct, and each one silently claims a 24-hour
    // period overlapping the next 1,439. Spacing is the only property that gives it away.
    let dir = dir();
    let path = dir.path().join("finer.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0, T0 + MINUTE]),
            Col::Sym(vec!["AAPL", "AAPL"]),
            Col::Dbl(vec![100.0, 101.0]),
            Col::Dbl(vec![110.0, 111.0]),
            Col::Dbl(vec![90.0, 91.0]),
            Col::Dbl(vec![105.0, 106.0]),
            Col::Dbl(vec![1000.0, 1000.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::UnexpectedCandleResolution {
        interval,
        previous_open,
        open,
        actual,
        ..
    } = error
    else {
        panic!("expected UnexpectedCandleResolution, got {error:?}");
    };
    assert_eq!(interval, CandleInterval::Day1);
    assert_eq!(actual, chrono::TimeDelta::minutes(1));
    // Reported in terms of the `ts` column the artifact actually carries, NOT the closes the
    // decoder derived from it: only the opens are values a caller can find in their own file.
    assert_eq!(previous_open.to_rfc3339(), "2026-07-01T00:00:00+00:00");
    assert_eq!(open.to_rfc3339(), "2026-07-01T00:01:00+00:00");
}

#[test]
fn bars_spaced_exactly_the_declared_interval_or_wider_decode() {
    // The other side of the boundary, and the reason the comparison is `<` rather than `<=`.
    // Consecutive bars of a compliant series are spaced EXACTLY one interval apart, so a rule that
    // rejected equality would reject every well-formed candle artifact there is.
    //
    // The third bar opens three days after the second: a weekend, a holiday or a quiet symbol only
    // ever makes spacing WIDER, which is why a gap cannot false-positive here.
    let dir = dir();
    let path = dir.path().join("spacing.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0, T0 + DAY, T0 + 4 * DAY]),
            Col::Sym(vec!["AAPL", "AAPL", "AAPL"]),
            Col::Dbl(vec![100.0, 101.0, 102.0]),
            Col::Dbl(vec![110.0, 111.0, 112.0]),
            Col::Dbl(vec![90.0, 91.0, 92.0]),
            Col::Dbl(vec![105.0, 106.0, 107.0]),
            Col::Dbl(vec![1000.0, 1000.0, 1000.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 3);
}

#[test]
fn a_calendar_interval_is_exempt_from_both_the_spacing_and_the_strictness_rules() {
    // `Month1` has no single width, so there is nothing to compare a spacing against — February
    // would false-positive against a 31-day one. Both bars here open one day apart, far inside any
    // month, and must still decode.
    //
    // They also close on the SAME instant, which is legitimate rather than a repeated row: month
    // arithmetic clamps day-of-month, so `2026-01-30 + 1mo` and `2026-01-31 + 1mo` both land on
    // `2026-02-28`. Strict ascent would reject a valid file, which is why it too is skipped here.
    let dir = dir();
    let path = dir.path().join("monthly.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![JAN_30, JAN_30 + DAY]),
            Col::Sym(vec!["AAPL", "AAPL"]),
            Col::Dbl(vec![100.0, 101.0]),
            Col::Dbl(vec![110.0, 111.0]),
            Col::Dbl(vec![90.0, 91.0]),
            Col::Dbl(vec![105.0, 106.0]),
            Col::Dbl(vec![1000.0, 1000.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Month1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].time_exchange.to_rfc3339(),
        "2026-02-28T00:00:00+00:00"
    );
    assert_eq!(events[1].time_exchange, events[0].time_exchange);
}

#[test]
fn a_backwards_timestamp_is_rejected_rather_than_replayed_out_of_order() {
    // An unsorted backtest feed produces a non-monotonic clock and wrong results, silently.
    let dir = dir();
    let path = dir.path().join("unsorted.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0 + HOUR, T0]),
            Col::Sym(vec!["AAPL", "AAPL"]),
            Col::Dbl(vec![1.0, 2.0]),
            Col::Dbl(vec![1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    assert!(matches!(error, LseError::NonMonotonicTimestamps { .. }));
}

#[test]
fn the_iterator_ends_at_the_first_error_rather_than_resuming() {
    // The third row would otherwise decode cleanly: the ordering high-water mark is only advanced
    // on success, so it is still `T0 + HOUR` and `T0 + 2*HOUR` passes. Continuing would hand a
    // caller who discards errors a silently truncated view of a file already proven corrupt.
    let dir = dir();
    let path = dir.path().join("poisoned.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0 + HOUR, T0, T0 + 2 * HOUR]),
            Col::Sym(vec!["AAPL", "AAPL", "AAPL"]),
            Col::Dbl(vec![1.0, 2.0, 3.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let mut events = read_export(&export, idx()).unwrap();

    assert!(matches!(events.next(), Some(Ok(_))));
    assert!(matches!(
        events.next(),
        Some(Err(LseError::NonMonotonicTimestamps { .. }))
    ));
    assert!(
        events.next().is_none(),
        "must not resume past a proven integrity violation"
    );
    // Fused: once ended, it stays ended.
    assert!(events.next().is_none());
}

#[test]
fn a_row_whose_symbol_contradicts_the_descriptor_is_rejected() {
    // The file and its descriptor disagree, so the artifact is not the one the caller thinks it
    // is. `BP` and `BP.L` are different instruments in different currencies -- a silent
    // misattribution here is a 100x pricing error.
    //
    // The offending row is deliberately NOT the first: the check is documented as applying to
    // every row, and a single-row fixture is equally satisfied by an implementation that inspects
    // only row 0 and then trusts the rest of the file.
    let dir = dir();
    let path = dir.path().join("wrong.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR, T0 + DAY]),
            Col::Sym(vec!["BP.L", "BP.L", "BP"]),
            Col::Dbl(vec![10.0, 11.0, 12.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "BP.L", LseExportTimeframe::Tick);
    let mut events = read_export(&export, idx()).unwrap();

    // The matching rows decode; the contradiction is what stops the stream.
    assert!(events.next().unwrap().is_ok());
    assert!(events.next().unwrap().is_ok());

    let error = events.next().unwrap().unwrap_err();
    let LseError::SymbolMismatch { expected, found } = error else {
        panic!("expected SymbolMismatch");
    };
    assert_eq!(expected, "BP.L");
    assert_eq!(found, "BP");
}

// ── typed decode failures ────────────────────────────────────────────────────
//
// Each of these is a silent-corruption class rather than a crash: nothing downstream can tell a
// millisecond timestamp read as microseconds, or a zero substituted for a NaN, from real data. The
// decoder turns every one into a typed error, and these pin that so a refactor or a `parquet`
// upgrade cannot quietly reintroduce the guess.

/// `STOCK_TICK` with `ts` in milliseconds — a 1000x time error if it were read as microseconds.
const STOCK_TICK_TS_MILLIS: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MILLIS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with `ts` in local time — epoch microseconds against an unknown zone.
const STOCK_TICK_TS_NOT_UTC: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,false));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with an integer `price` rather than a `DOUBLE`.
const STOCK_TICK_INT_PRICE: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED INT64 price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with a nullable `price`, which the tick layout has no substitute for.
const STOCK_TICK_NULLABLE_PRICE: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  OPTIONAL DOUBLE price;
  REQUIRED DOUBLE volume;
}";

#[test]
fn a_millisecond_timestamp_column_is_rejected_rather_than_read_as_microseconds() {
    let dir = dir();
    let path = dir.path().join("millis.parquet");
    write_parquet(
        &path,
        STOCK_TICK_TS_MILLIS,
        vec![
            Col::Ts(vec![T0 / 1000]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType { column, .. } = &error else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "ts");
}

#[test]
fn a_timestamp_column_not_adjusted_to_utc_is_rejected() {
    // The half of the check that has no downstream symptom at all: a local-time column is the right
    // physical type, the right unit, and off by the writer's UTC offset on every row.
    let dir = dir();
    let path = dir.path().join("local.parquet");
    write_parquet(
        &path,
        STOCK_TICK_TS_NOT_UTC,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType { column, .. } = &error else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "ts");
}

#[test]
fn a_value_column_of_the_wrong_physical_type_is_rejected() {
    let dir = dir();
    let path = dir.path().join("intprice.parquet");
    write_parquet(
        &path,
        STOCK_TICK_INT_PRICE,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Ts(vec![1]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType {
        column, required, ..
    } = &error
    else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "price");
    assert_eq!(*required, "DOUBLE");
}

#[test]
fn a_null_in_a_column_the_layout_has_no_substitute_for_is_an_error() {
    // Nullable `volume`/`ask` map to `None` by design; `price` does not — a tick with no price is
    // not a tick, and defaulting it would put a zero into the money path.
    let dir = dir();
    let path = dir.path().join("nullprice.parquet");
    write_parquet(
        &path,
        STOCK_TICK_NULLABLE_PRICE,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::OptDbl(vec![None]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::NullValue { column } = error else {
        panic!("expected NullValue, got {error:?}");
    };
    assert_eq!(column, "price");
}

#[test]
fn a_price_with_no_decimal_representation_is_surfaced_not_zeroed() {
    // `f64::NAN` has no `Decimal`. Substituting zero would put a real-looking price into the money
    // path -- and a zero price is not obviously wrong to anything downstream.
    let dir = dir();
    let path = dir.path().join("nan.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![f64::NAN]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::PriceNotRepresentable { value, .. } = error else {
        panic!("expected PriceNotRepresentable, got {error:?}");
    };
    assert!(value.is_nan());
}

#[test]
fn an_infinite_price_is_surfaced_too() {
    let dir = dir();
    let path = dir.path().join("inf.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![f64::INFINITY]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    assert!(matches!(error, LseError::PriceNotRepresentable { .. }));
}

#[test]
fn a_timestamp_outside_the_representable_range_is_an_error_not_a_wrapped_instant() {
    let dir = dir();
    let path = dir.path().join("farfuture.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![i64::MAX]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::TimestampNotRepresentable { micros } = error else {
        panic!("expected TimestampNotRepresentable, got {error:?}");
    };
    assert_eq!(micros, i64::MAX);
}

#[test]
fn an_unrecognised_schema_is_a_typed_error_not_a_mis_decode() {
    let dir = dir();
    let path = dir.path().join("odd.parquet");
    write_parquet(
        &path,
        "
        message schema {
          REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
          REQUIRED BYTE_ARRAY symbol (STRING);
          REQUIRED DOUBLE something_else;
        }",
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    assert!(matches!(error, LseError::UnsupportedSchema { .. }));
    assert!(error.to_string().contains("something_else"));
}

#[test]
fn a_zero_row_artifact_decodes_to_no_events_without_erroring() {
    // The measured result of a `symbol: "all"` export: a well-formed file with the full schema and
    // nothing in it.
    let dir = dir();
    let path = dir.path().join("empty.parquet");
    write_parquet(
        &path,
        FX_TICK,
        vec![
            Col::Ts(vec![]),
            Col::Sym(vec![]),
            Col::Dbl(vec![]),
            Col::Dbl(vec![]),
        ],
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert!(events.is_empty());
}

// ── helpers ──────────────────────────────────────────────────────────────────

#[test]
fn symbols_in_export_lists_the_files_symbols_without_decoding_it() {
    // Deliberately multi-symbol, with the repeat in the middle. An all-one-symbol fixture is
    // satisfied by an implementation that reads row 0 and stops, and a caller using this to decide
    // which instruments to index would then silently drop every symbol but the first.
    let dir = dir();
    let path = dir.path().join("syms.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR, T0 + DAY]),
            Col::Sym(vec!["AAPL", "AAPL", "MSFT"]),
            Col::Dbl(vec![1.0, 2.0, 3.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    // Deduplicated, in first-seen order.
    assert_eq!(
        symbols_in_export(&path).unwrap(),
        vec!["AAPL".to_owned(), "MSFT".to_owned()]
    );
}

#[test]
fn symbols_in_export_walks_every_row_group_not_just_the_first() {
    // The provider's writer emits one row group per artifact, so a decoder that stopped after the
    // first would pass every other test in this file.
    let dir = dir();
    let path = dir.path().join("syms_groups.parquet");
    write_parquet_row_groups(
        &path,
        STOCK_TICK,
        vec![
            vec![
                Col::Ts(vec![T0]),
                Col::Sym(vec!["AAPL"]),
                Col::Dbl(vec![1.0]),
                Col::Dbl(vec![1.0]),
            ],
            vec![
                Col::Ts(vec![T0 + DAY]),
                Col::Sym(vec!["MSFT"]),
                Col::Dbl(vec![2.0]),
                Col::Dbl(vec![1.0]),
            ],
        ],
    );

    assert_eq!(
        symbols_in_export(&path).unwrap(),
        vec!["AAPL".to_owned(), "MSFT".to_owned()]
    );
}

#[test]
fn instrument_index_is_derived_from_the_registry_so_a_typo_cannot_be_silent() {
    use rustrade_instrument::Underlying;
    use rustrade_instrument::instrument::name::{InstrumentNameExchange, InstrumentNameInternal};
    use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
    use rustrade_instrument::instrument::{Instrument, kind::InstrumentKind};
    use rustrade_instrument::test_utils::asset;

    let name = InstrumentNameExchange::from("BP.L");
    let instruments = IndexedInstrumentsBuilder::default()
        .add_instrument(Instrument::new(
            ExchangeId::LseEquities,
            InstrumentNameInternal::new_from_exchange(ExchangeId::LseEquities, name.clone()),
            name,
            Underlying::new(asset("bp.l"), asset("gbx")),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Spot,
            None,
        ))
        .build();

    assert_eq!(
        instrument_index_for(&instruments, ExchangeId::LseEquities, "BP.L").unwrap(),
        InstrumentIndex::new(0)
    );

    // The typo that would otherwise leave one instrument silently receiving nothing.
    let error = instrument_index_for(&instruments, ExchangeId::LseEquities, "BP").unwrap_err();
    assert!(matches!(error, LseError::UnknownInstrument { .. }));
    // The message names what IS registered, so the fix is obvious.
    assert!(error.to_string().contains("BP.L"));
}

#[test]
fn a_london_listing_registered_in_pounds_is_rejected_rather_than_booked_100x_wrong() {
    use rustrade_instrument::Underlying;
    use rustrade_instrument::instrument::name::{InstrumentNameExchange, InstrumentNameInternal};
    use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
    use rustrade_instrument::instrument::{Instrument, kind::InstrumentKind};
    use rustrade_instrument::test_utils::asset;

    // `BP.L` prints ~548 where BP trades around £5.48. The provider sends no unit, so the
    // registered asset IS the unit: booking that 548 as GBP inflates notional, fees, unrealised
    // PnL and every balance by 100×, and no later layer can tell. This registry boundary is the
    // only place the provider's view of the symbol and the caller's registry meet.
    let register = |quote: &str| {
        let name = InstrumentNameExchange::from("BP.L");
        IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::new(
                ExchangeId::LseEquities,
                InstrumentNameInternal::new_from_exchange(ExchangeId::LseEquities, name.clone()),
                name,
                Underlying::new(asset("bp.l"), asset(quote)),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            ))
            .build()
    };

    let instruments = register("gbp");
    let error = instrument_index_for(&instruments, ExchangeId::LseEquities, "BP.L").unwrap_err();
    let LseError::QuoteAssetMismatch {
        expected,
        registered,
        ..
    } = &error
    else {
        panic!("expected QuoteAssetMismatch, got {error:?}");
    };
    // Both sides are named: "wrong quote asset" alone leaves the reader guessing which way.
    assert_eq!(expected, "gbx");
    assert_eq!(registered, "gbp");

    // Case is not the distinction — asset identity is lowercased internally, so `GBX` and `gbx`
    // are one asset. Rejecting the correct registration on spelling would be the worse failure.
    for spelling in ["gbx", "GBX"] {
        let instruments = register(spelling);
        assert_eq!(
            instrument_index_for(&instruments, ExchangeId::LseEquities, "BP.L").unwrap(),
            InstrumentIndex::new(0),
            "{spelling} names the same asset as gbx"
        );
    }

    // A US listing carries no venue suffix and is quoted in USD, so the same check must pass
    // there rather than only ever firing on London.
    let name = InstrumentNameExchange::from("AAPL");
    let instruments = IndexedInstrumentsBuilder::default()
        .add_instrument(Instrument::new(
            ExchangeId::LseEquities,
            InstrumentNameInternal::new_from_exchange(ExchangeId::LseEquities, name.clone()),
            name,
            Underlying::new(asset("aapl"), asset("usd")),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Spot,
            None,
        ))
        .build();
    assert_eq!(
        instrument_index_for(&instruments, ExchangeId::LseEquities, "AAPL").unwrap(),
        InstrumentIndex::new(0)
    );
}
