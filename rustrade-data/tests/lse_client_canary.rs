//! London Strategic Edge REST shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for this integration runs against synthetic `wiremock` fixtures, because the
//! provider's licence forbids passing its data on (<https://londonstrategicedge.com/licence>,
//! <https://londonstrategicedge.com/terms>), so no recorded response may be committed. Fixtures
//! check the decoder against *our own assumptions* about the wire format. This canary checks those
//! assumptions against the **real API**.
//!
//! # ⚠️ It asserts SPACING and CHUNKING, not merely shape
//!
//! A candle answer can be well formed and still wrong:
//!
//! - **A finer resolution than asked for** keeps the exact column set, so only the spacing of the
//!   bars shows it. The daily test bounds the shortest gap and the bar count; the hourly test
//!   requires every gap to be a whole number of hours and at least one to be exactly one.
//! - **A window the server aggregates differently** from how the fetch cuts it returns fragments
//!   that look whole. The server builds each bar from the window it is asked for, so the fetch
//!   aligns its chunks to period boundaries; the chunk-invariance tests read one range in one-bar
//!   chunks and in one chunk, and require the same bars. Were the alignment wrong, or the server's
//!   bucketing to change, the edges of the small chunks would disagree with the big one.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! # Why every test here is `#[serial]`
//!
//! The provider allows one download per key at a time and answers a second with `429`, which
//! `LseClient` reports as `LseError::RateLimited` rather than retrying. The harness runs a file's
//! tests in parallel, and clients built separately do not share a request gate, so unserialised
//! tests would race each other into a `429` that reads like provider drift and is not.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_client_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the key's allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc, Weekday};
use rustrade_data::exchange::lse::catalogue::LsePairSearch;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::historical::LseCandleRequest;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::subscription::candle::{Candle, CandleInterval};
use serial_test::serial;
use std::num::NonZeroU32;

const KEY_ENV: &str = "LSE_API_KEY";

/// The forex pair, spelt as the REST routes take it.
const FOREX: &str = "EUR_USD";

/// The bars `LseClient` asks for in one download by default. A plan that caps an export below it
/// would have every default chunk refused.
const DEFAULT_CHUNK_BARS: u64 = 50_000;

/// Build a client, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable*, such as a stray newline from a
/// `.env` edit, is a misconfiguration, and reporting it as a skip would let this canary pass green
/// while never reaching the provider. The error is safe to print: `LseError` redacts the key.
fn client() -> Option<LseClient> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseClient::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

/// Candles must arrive strictly ascending by `close_time`: the order every consumer, and the
/// backtest replay in particular, relies on.
fn assert_ascending(candles: &[Candle], label: &str) {
    assert!(
        candles
            .windows(2)
            .all(|pair| pair[0].close_time < pair[1].close_time),
        "{label}: candles are not strictly ascending by close_time"
    );
}

fn gaps(candles: &[Candle]) -> Vec<Duration> {
    candles
        .windows(2)
        .map(|pair| pair[1].close_time - pair[0].close_time)
        .collect()
}

/// A daily bar is stamped at its session's first trade, which drifts by minutes from day to day
/// (forex opens near `22:00` UTC the evening before), so daily gaps are close to, not exactly, a
/// day. Twelve hours is far above that drift and far below a day: an hourly answer to a daily
/// request fails it.
fn assert_daily(candles: &[Candle], days: i64, label: &str) {
    assert!(
        candles.len() >= 3,
        "{label}: {} daily candles in {days} days; too few to check spacing",
        candles.len()
    );
    assert!(
        candles.len() <= usize::try_from(days + 1).unwrap(),
        "{label}: {} candles in {days} days - more than one a day, so a finer resolution was \
         served than asked for",
        candles.len()
    );
    let shortest = gaps(candles).into_iter().min().unwrap();
    assert!(
        shortest > Duration::hours(12),
        "{label}: shortest gap is {shortest} - a finer resolution was served than asked for"
    );
}

/// The most recent Tuesday to Thursday at least two days back: a full forex and US-equity session
/// on either side, read long after the provider has ingested it.
fn settled_midweek_day() -> NaiveDate {
    let mut day = Utc::now().date_naive() - Duration::days(2);
    while !matches!(day.weekday(), Weekday::Tue | Weekday::Wed | Weekday::Thu) {
        day -= Duration::days(1);
    }
    day
}

fn at(day: NaiveDate, hour: u32) -> DateTime<Utc> {
    day.and_time(NaiveTime::from_hms_opt(hour, 0, 0).unwrap())
        .and_utc()
}

fn days_back(days: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let end = Utc::now();
    (end - Duration::days(days), end)
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn daily_forex_candles_arrive_daily_with_volume_and_trades() {
    let Some(client) = client() else { return };
    let (start, end) = days_back(30);

    let candles = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::Forex,
            FOREX,
            CandleInterval::Day1,
            start,
            end,
        ))
        .await
        .expect("daily forex candles");

    assert_ascending(&candles, "daily forex");
    assert_daily(&candles, 30, "daily forex");
    assert!(
        candles.iter().all(|candle| candle.low <= candle.high),
        "daily forex: inverted OHLC"
    );
    // Forex used to carry no volume on this provider; it now reports both. A blanket `None` would
    // mean the columns had stopped being decoded, or had stopped being sent.
    assert!(
        candles.iter().any(|candle| candle.volume.is_some()),
        "no daily forex candle carried a volume"
    );
    assert!(
        candles
            .iter()
            .any(|candle| candle.trade_count.is_some_and(|count| count > 0)),
        "no daily forex candle carried a trade count"
    );
    println!("CANARY_OK: {} daily {FOREX} candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn hourly_forex_candles_sit_on_the_hour_grid() {
    let Some(client) = client() else { return };
    let (start, end) = days_back(3);

    let candles = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::Forex,
            FOREX,
            CandleInterval::Hour1,
            start,
            end,
        ))
        .await
        .expect("hourly forex candles");

    assert_ascending(&candles, "hourly forex");
    assert!(
        candles.len() >= 3,
        "hourly forex: {} candles in three days",
        candles.len()
    );
    let gaps = gaps(&candles);
    // Longer gaps are legitimate (a weekend, a quiet hour); a gap off the hour grid is not.
    assert!(
        gaps.iter()
            .all(|gap| *gap >= Duration::hours(1) && gap.num_seconds() % 3600 == 0),
        "hourly forex: a gap is not a whole number of hours ({gaps:?})"
    );
    assert!(
        gaps.contains(&Duration::hours(1)),
        "hourly forex: no gap is exactly one hour - a coarser resolution was served"
    );
    println!("CANARY_OK: {} hourly {FOREX} candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn daily_equity_candles_carry_volume_and_trades() {
    let Some(client) = client() else { return };
    let (start, end) = days_back(30);

    let candles = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            CandleInterval::Day1,
            start,
            end,
        ))
        .await
        .expect("daily AAPL candles");

    assert_ascending(&candles, "daily AAPL");
    assert_daily(&candles, 30, "daily AAPL");
    assert!(
        candles.iter().any(|candle| candle.volume.is_some()),
        "no AAPL candle carried a volume"
    );
    assert!(
        candles
            .iter()
            .any(|candle| candle.trade_count.is_some_and(|count| count > 0)),
        "no AAPL candle carried a trade count"
    );
    println!("CANARY_OK: {} daily AAPL candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn hourly_candles_do_not_depend_on_the_chunk_size() {
    let Some(client) = client() else { return };
    let day = settled_midweek_day();
    let request = LseCandleRequest::new(
        LseMarketCode::Forex,
        FOREX,
        CandleInterval::Hour1,
        at(day, 10),
        at(day, 16),
    );

    let whole = client
        .collect_candles(request.clone())
        .await
        .expect("hourly candles in one chunk");
    let chunked = client
        .clone()
        .with_chunk_bars(NonZeroU32::MIN)
        .collect_candles(request)
        .await
        .expect("hourly candles in one-bar chunks");

    assert!(whole.len() >= 3, "only {} hourly candles", whole.len());
    assert_eq!(
        chunked, whole,
        "one-bar chunks disagree with one chunk: the server aggregates a window differently from \
         how the fetch aligns it"
    );
    println!(
        "CANARY_OK: {} hourly candles agree across chunk sizes",
        whole.len()
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn daily_candles_do_not_depend_on_the_chunk_size() {
    let Some(client) = client() else { return };
    let end = at(settled_midweek_day(), 0);
    let request = LseCandleRequest::new(
        LseMarketCode::UsStocks,
        "AAPL",
        CandleInterval::Day1,
        end - Duration::days(7),
        end,
    );

    let whole = client
        .collect_candles(request.clone())
        .await
        .expect("daily candles in one chunk");
    let chunked = client
        .clone()
        .with_chunk_bars(NonZeroU32::MIN)
        .collect_candles(request)
        .await
        .expect("daily candles in one-bar chunks");

    assert!(whole.len() >= 3, "only {} daily candles", whole.len());
    assert_eq!(
        chunked, whole,
        "one-session chunks disagree with one chunk: the server aggregates a date window \
         differently from how the fetch aligns it"
    );
    println!(
        "CANARY_OK: {} daily candles agree across chunk sizes",
        whole.len()
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn usage_allows_a_default_chunk_in_one_export() {
    let Some(client) = client() else { return };

    let usage = client.usage().await.expect("usage");

    assert!(usage.calls_per_minute > 0, "calls_per_minute missing");
    // `None` is the provider's "unlimited".
    assert!(
        usage
            .rows
            .per_export
            .is_none_or(|rows| rows >= DEFAULT_CHUNK_BARS),
        "the plan caps an export at {:?} rows, below the {DEFAULT_CHUNK_BARS}-bar default chunk",
        usage.rows.per_export
    );
    println!("CANARY_OK: usage {usage:?}");
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn the_markets_list_decodes_and_names_only_known_markets() {
    let Some(client) = client() else { return };

    let markets = client.fetch_markets().await.expect("markets");

    // A market this integration has no code for is drift to act on: add it to `LseMarketCode`.
    for market in &markets {
        market
            .market()
            .unwrap_or_else(|error| panic!("market {:?}: {error}", market.code));
    }
    let us_stocks = markets
        .iter()
        .find(|market| market.market().ok() == Some(LseMarketCode::UsStocks))
        .expect("us_stocks is listed");
    assert!(us_stocks.allowed, "us_stocks is not allowed on this key");
    assert!(
        us_stocks.capabilities.candles,
        "us_stocks no longer offers candles"
    );
    println!("CANARY_OK: {} markets", markets.len());
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn an_exact_pair_search_finds_aapl() {
    let Some(client) = client() else { return };

    let page = client
        .fetch_pairs(
            LseMarketCode::UsStocks,
            Some(LsePairSearch::Symbol("AAPL")),
            10,
            0,
        )
        .await
        .expect("pairs");

    let aapl = page
        .pairs
        .iter()
        .find(|pair| pair.symbol == "AAPL")
        .unwrap_or_else(|| panic!("AAPL not in {page:?}"));
    assert!(aapl.pair_id > 0, "AAPL has no pair id");
    assert!(page.total >= 1, "total {} below the page", page.total);
    println!("CANARY_OK: AAPL is pair {}", aapl.pair_id);
}
