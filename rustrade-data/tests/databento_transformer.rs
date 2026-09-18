//! Offline tests for Databento DBN → rustrade event transformation.
//!
//! # Fixtures are synthetic, and must stay that way
//!
//! These tests build their own DBN files at run time instead of reading committed samples.
//! That is a licensing constraint, not a stylistic one.
//!
//! Databento market data is licensed per subscriber. The Databento User Agreement defines
//! "Redistribution" to cover the publication or distribution of covered data and "all other means
//! of furnishing such data or other information derived from the same to entities other than
//! Customer", and requires the customer to limit use to internal purposes and "not to engage in
//! any Redistribution of Third-Party Data" without prior written approval from both Databento and
//! the relevant exchange. Exchange-sourced samples — GLBX.MDP3 is CME Group data — therefore
//! cannot be committed to this public repository. See
//! <https://databento.com/legal/databento-user-agreement>.
//!
//! The records below are invented. They are deliberately ES-shaped — price magnitude, 0.25 tick,
//! one-tick spreads — so the assertions read meaningfully, but no value here came from a market
//! data feed. Do not "improve" these fixtures by replacing them with a real capture.
//!
//! # What this does and does not cover
//!
//! Generating with `dbn`'s encoder and reading back through our loader exercises the full decode
//! path — zstd framing, DBN metadata, record headers, rtype dispatch, fixed-point price scaling
//! and our transformation — but it is a round-trip against the same library version rather than a
//! replay of bytes produced by Databento's own encoder. Wire-format drift that affects both halves
//! equally would not be caught here; that is the cost of not being able to ship real captures.

#![cfg(feature = "databento")]

use databento::dbn::{
    self, BidAskPair, FlagSet, Mbp1Msg, Metadata, RecordHeader, SType, Schema, TradeMsg,
    encode::{DbnEncoder, EncodeRecord},
    enums::rtype,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_data::exchange::databento::{load_quotes_from_dbn, load_trades_from_dbn};
use rustrade_instrument::exchange::ExchangeId;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// DBN encodes prices as fixed-point with nine implied decimals, which is what the transformer
/// reverses with `Decimal::new(px, 9)`.
const PRICE_SCALE: i64 = 1_000_000_000;
/// ES trades on a 0.25 tick.
const TICK: i64 = PRICE_SCALE / 4;
/// A plausible ES level. Arbitrary, but inside the (1000, 10000) band the assertions use as a
/// sanity check against a mis-scaled price.
const BASE_PRICE: i64 = 5_400 * PRICE_SCALE;
/// Nanoseconds since the Unix epoch for the first record; the rest step forward from here.
const START_TS: u64 = 1_718_000_000_000_000_000;
/// Spacing between consecutive records, in nanoseconds.
const TS_STEP: u64 = 1_000_000;

const PUBLISHER_ID: u16 = 1;
const INSTRUMENT_ID: u32 = 42;

/// Metadata common to both fixtures. `schema` differs per file so the header describes what the
/// records actually are.
fn metadata(schema: Schema) -> Metadata {
    Metadata::builder()
        .dataset("GLBX.MDP3")
        .schema(Some(schema))
        .start(START_TS)
        .stype_in(Some(SType::RawSymbol))
        .stype_out(SType::InstrumentId)
        .symbols(vec!["ESM4".to_owned()])
        .build()
}

/// Writes `count` synthetic trades as a zstd-compressed DBN file.
///
/// Prices oscillate around [`BASE_PRICE`] in whole ticks and timestamps advance by [`TS_STEP`],
/// which satisfies the monotonic-ordering assertion without making every record identical.
fn write_trades(path: &Path, count: u32) {
    let file = File::create(path).expect("create trades fixture");
    let mut encoder =
        DbnEncoder::with_zstd(file, &metadata(Schema::Trades)).expect("trades header");

    for i in 0..count {
        let ts = START_TS + u64::from(i) * TS_STEP;
        // A small sawtooth so prices vary but stay near the base level.
        let price = BASE_PRICE + i64::from(i % 5) * TICK;
        let msg = TradeMsg {
            hd: RecordHeader::new::<TradeMsg>(rtype::MBP_0, PUBLISHER_ID, INSTRUMENT_ID, ts),
            price,
            size: 1 + i % 7,
            action: b'T' as std::ffi::c_char,
            side: if i % 2 == 0 { b'B' } else { b'A' } as std::ffi::c_char,
            flags: FlagSet::empty(),
            depth: 0,
            ts_recv: ts,
            ts_in_delta: 0,
            sequence: i,
        };
        encoder.encode_record(&msg).expect("encode trade");
    }
}

/// Writes `count` synthetic MBP-1 quotes as a zstd-compressed DBN file.
///
/// Each book is a one-tick spread straddling the base level, so `ask >= bid` holds and the spread
/// stays well inside the sanity bound the tests assert.
fn write_quotes(path: &Path, count: u32) {
    let file = File::create(path).expect("create quotes fixture");
    let mut encoder = DbnEncoder::with_zstd(file, &metadata(Schema::Mbp1)).expect("quotes header");

    for i in 0..count {
        let ts = START_TS + u64::from(i) * TS_STEP;
        let bid_px = BASE_PRICE + i64::from(i % 5) * TICK;
        let ask_px = bid_px + TICK;
        let msg = Mbp1Msg {
            hd: RecordHeader::new::<Mbp1Msg>(rtype::MBP_1, PUBLISHER_ID, INSTRUMENT_ID, ts),
            price: ask_px,
            size: 1 + i % 3,
            action: b'A' as std::ffi::c_char,
            side: b'B' as std::ffi::c_char,
            flags: FlagSet::empty(),
            depth: 0,
            ts_recv: ts,
            ts_in_delta: 0,
            sequence: i,
            levels: [BidAskPair {
                bid_px,
                ask_px,
                bid_sz: 10 + i % 4,
                ask_sz: 12 + i % 4,
                bid_ct: 2,
                ask_ct: 3,
            }],
        };
        encoder.encode_record(&msg).expect("encode quote");
    }
}

/// A generated fixture plus the directory keeping it alive; dropping this removes the file.
struct Fixture {
    _dir: TempDir,
    path: PathBuf,
}

fn trades_fixture(count: u32) -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("es_trades_sample.dbn.zst");
    write_trades(&path, count);
    Fixture { _dir: dir, path }
}

fn quotes_fixture(count: u32) -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("es_quotes_sample.dbn.zst");
    write_quotes(&path, count);
    Fixture { _dir: dir, path }
}

#[test]
fn test_load_trades_from_dbn_fixture() {
    let fixture = trades_fixture(250);

    let trades: Vec<_> = load_trades_from_dbn(&fixture.path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .collect();

    assert!(!trades.is_empty(), "Expected at least one trade record");

    let (successes, failures): (Vec<_>, Vec<_>) = trades.into_iter().partition(|r| r.is_ok());

    // Unlike the old committed capture, the input here is fully known, so every record must
    // transform. A failure means the decode path broke, not that the sample was ragged.
    assert!(
        failures.is_empty(),
        "Expected every synthetic trade to transform, got {} failures",
        failures.len()
    );
    assert_eq!(successes.len(), 250, "Expected all records back");

    let first_trade = successes.into_iter().next().unwrap().unwrap();

    assert_eq!(first_trade.exchange, ExchangeId::DatabentoGlbx);
    assert_eq!(first_trade.instrument, "ESM4");
    assert!(
        first_trade.kind.price > Decimal::ZERO,
        "Price should be positive"
    );
    assert!(
        first_trade.kind.amount > Decimal::ZERO,
        "Amount should be positive"
    );

    // Catches a mis-scaled fixed-point conversion: a dropped or extra factor of 1e9 lands far
    // outside this band.
    assert!(
        first_trade.kind.price > dec!(1000) && first_trade.kind.price < dec!(10000),
        "ES price {} outside expected range",
        first_trade.kind.price
    );
}

#[test]
fn test_load_quotes_from_dbn_fixture() {
    let fixture = quotes_fixture(250);

    let quotes: Vec<_> = load_quotes_from_dbn(&fixture.path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .collect();

    assert!(!quotes.is_empty(), "Expected at least one quote record");

    let (successes, failures): (Vec<_>, Vec<_>) = quotes.into_iter().partition(|r| r.is_ok());

    assert!(
        failures.is_empty(),
        "Expected every synthetic quote to transform, got {} failures",
        failures.len()
    );
    assert_eq!(successes.len(), 250, "Expected all records back");

    let first_quote = successes.into_iter().next().unwrap().unwrap();

    assert_eq!(first_quote.exchange, ExchangeId::DatabentoGlbx);
    assert_eq!(first_quote.instrument, "ESM4");

    let quote = &first_quote.kind;
    assert!(
        quote.bid_price > Decimal::ZERO,
        "Bid price should be positive"
    );
    assert!(
        quote.ask_price > Decimal::ZERO,
        "Ask price should be positive"
    );
    assert!(
        quote.ask_price >= quote.bid_price,
        "Ask should be >= bid, got bid={} ask={}",
        quote.bid_price,
        quote.ask_price
    );

    assert!(
        quote.bid_price > dec!(1000) && quote.bid_price < dec!(10000),
        "ES bid {} outside expected range",
        quote.bid_price
    );
}

#[test]
fn test_trade_timestamp_ordering() {
    let fixture = trades_fixture(100);

    let trades: Vec<_> = load_trades_from_dbn(&fixture.path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .filter_map(|r| r.ok())
        .collect();

    assert_eq!(trades.len(), 100, "Expected all records back");

    for window in trades.windows(2) {
        assert!(
            window[1].time_exchange >= window[0].time_exchange,
            "Timestamps should be monotonically increasing"
        );
    }
}

#[test]
fn test_quote_spread_is_reasonable() {
    let fixture = quotes_fixture(100);

    let quotes: Vec<_> = load_quotes_from_dbn(&fixture.path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("Failed to open DBN file")
        .filter_map(|r| r.ok())
        .collect();

    assert_eq!(quotes.len(), 100, "Expected all records back");

    for quote in &quotes {
        let spread = quote.kind.ask_price - quote.kind.bid_price;
        // One tick by construction; the bound is loose so it still reads as a sanity check on the
        // decode rather than a restatement of the generator.
        assert!(
            spread >= Decimal::ZERO && spread < dec!(10),
            "Spread {} is unreasonable for ES futures",
            spread
        );
    }
}

/// The generator is the fixture's specification, so pin the two properties the assertions above
/// lean on. If this fails, the tests below it are checking something other than what they claim.
#[test]
fn synthetic_fixtures_have_the_shape_the_assertions_assume() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("probe.dbn.zst");
    write_quotes(&path, 5);

    let quotes: Vec<_> = load_quotes_from_dbn(&path, ExchangeId::DatabentoGlbx, "ESM4")
        .expect("open")
        .filter_map(|r| r.ok())
        .collect();

    // Exactly one tick of spread, and a price that survives the 1e9 round trip intact.
    assert_eq!(
        quotes[0].kind.ask_price - quotes[0].kind.bid_price,
        dec!(0.25)
    );
    assert_eq!(quotes[0].kind.bid_price, dec!(5400));
    assert_eq!(dbn::UNDEF_PRICE, i64::MAX, "undefined-price sentinel moved");
}
