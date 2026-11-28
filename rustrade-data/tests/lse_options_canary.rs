//! London Strategic Edge options shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for the option print tape, chain, candles and Greeks runs against synthetic
//! `wiremock` fixtures, because the provider's licence forbids passing its data on
//! (<https://londonstrategicedge.com/licence>, <https://londonstrategicedge.com/terms>), so no
//! recorded response may be committed. Fixtures check the decoder against *our own assumptions*
//! about the wire format. This canary checks those assumptions against the **real API**.
//!
//! # ⚠️ It must not be green on an empty answer
//!
//! The print tape answers `200` with no rows for a closed market and for a window the provider has
//! not ingested, so a shape assertion over whatever came back passes on nothing at all. Every
//! assertion here therefore first requires rows to exist:
//!
//! - **The tape is live**: one minute inside the US session, on the most recent weekday that has
//!   one, must hold prints. The search reaches back a week and no further, so a feed that has
//!   **frozen** fails here rather than passing on old data forever.
//! - **Paging by time agrees with one page**: the same window read in small pages, each starting
//!   at the last page's latest instant with that instant's prints held back, must return exactly
//!   the prints one page returns, and so must the fetch. The provider returns prints sharing an
//!   instant in a different order from one request to the next, so the comparison ignores order;
//!   what it checks is that `from` is inclusive and `to` exclusive to the nanosecond, which the
//!   fetch's paging rests on.
//! - **Greeks arrive with prints**: at least one print carries one, and the Greeks selection holds
//!   rows for a contract that printed. A tape that silently stopped carrying them would otherwise
//!   decode cleanly to all-`None`.
//! - **Candles exist for a contract that printed**, carrying a trade count.
//! - **The chain decodes** and lists contracts of the underlying asked for.
//!
//! The window is read days after it closed, well outside the ingestion lag the fetch's settle
//! margin guards against, so an incomplete read is not a plausible cause of failure here.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**, so a mistyped key cannot report green forever.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! Every test is `#[serial]`: the provider allows one download per key at a time, and clients
//! built separately do not share a request gate.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_options_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the key's allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Datelike, Duration, NaiveDateTime, NaiveTime, Utc, Weekday};
use futures::TryStreamExt;
use rust_decimal::Decimal;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::options::LseOptionPrint;
use rustrade_data::subscription::candle::CandleInterval;
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;

const KEY_ENV: &str = "LSE_API_KEY";

/// The print tape, for the raw reads the paging test compares the fetch against.
const FLOW_URL: &str = "https://download.londonstrategicedge.com/v3/options/flow";

/// The most rows one page of the tape may hold.
const MAX_PAGE_ROWS: usize = 5000;

/// The busiest underlying the provider carries, so one minute of it is never legitimately empty
/// on a trading day.
const UNDERLYING: &str = "SPY";

/// 15:00 UTC is inside the US options session on both sides of the DST change (13:30–20:00 UTC in
/// summer, 14:30–21:00 in winter), so the probe window never lands on a closed market by season.
const PROBE_TIME: NaiveTime = NaiveTime::from_hms_opt(15, 0, 0).unwrap();

/// How far back to look for a session. A week always spans at least five weekdays, which absorbs a
/// holiday; a feed with nothing in it has stopped.
const LOOKBACK_DAYS: i64 = 7;

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

/// The one-minute probe window on the most recent weekday that holds prints, and those prints.
async fn recent_session(client: &LseClient) -> (DateTime<Utc>, Vec<LseOptionPrint>) {
    let today = Utc::now().date_naive();

    for days_back in 1..=LOOKBACK_DAYS {
        let day = today - Duration::days(days_back);
        if matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
            continue;
        }

        let start = day.and_time(PROBE_TIME).and_utc();
        let prints = client
            .collect_option_flow(Some(UNDERLYING), start, start + Duration::minutes(1))
            .await
            .expect("option flow fetch");

        if !prints.is_empty() {
            println!("probe window {start}: {} prints", prints.len());
            return (start, prints);
        }
        println!("probe window {start}: empty (holiday?), looking further back");
    }

    panic!(
        "no {UNDERLYING} option prints at {PROBE_TIME} UTC on any weekday in the last \
         {LOOKBACK_DAYS} days: the print tape appears to have stopped"
    );
}

/// The contract that printed most in the window.
fn busiest_ticker(prints: &[LseOptionPrint]) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for print in prints {
        *counts.entry(print.contract.ticker.as_str()).or_default() += 1;
    }
    let (ticker, _) = counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .expect("at least one print");
    ticker.to_owned()
}

/// One raw page of the tape in `[start, end)`, oldest first, as JSON rows.
async fn raw_page(start: DateTime<Utc>, end: DateTime<Utc>, limit: usize) -> Vec<Value> {
    let key = std::env::var(KEY_ENV).expect("key checked above");
    let format = "%Y-%m-%d %H:%M:%S%.f";
    let page: Value = reqwest::Client::new()
        .get(FLOW_URL)
        .header("x-api-key", key)
        .header("user-agent", "rustrade-data-canary")
        .query(&[
            ("underlying", UNDERLYING.to_owned()),
            ("from", start.format(format).to_string()),
            ("to", end.format(format).to_string()),
            ("order", "asc".to_owned()),
            ("limit", limit.to_string()),
        ])
        .send()
        .await
        .expect("raw flow request")
        .error_for_status()
        .expect("raw flow status")
        .json()
        .await
        .expect("raw flow body");
    page["rows"].as_array().expect("a rows array").clone()
}

/// A raw row's `ts`, to the nanosecond.
fn row_time(row: &Value) -> DateTime<Utc> {
    let ts = row["ts"].as_str().expect("a string ts");
    NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap_or_else(|error| panic!("ts {ts:?}: {error}"))
        .and_utc()
}

/// Rows in a canonical order, so two reads compare whatever order ties came back in.
fn canonical(rows: &[Value]) -> Vec<String> {
    let mut rows: Vec<String> = rows.iter().map(Value::to_string).collect();
    rows.sort_unstable();
    rows
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn the_print_tape_is_live_and_decodes() {
    let Some(client) = client() else { return };
    let (start, prints) = recent_session(&client).await;
    let end = start + Duration::minutes(1);

    assert!(
        prints.windows(2).all(|pair| pair[0].time <= pair[1].time),
        "prints are not in time order"
    );
    for print in &prints {
        let ticker = &print.contract.ticker;
        assert!(
            print.time >= start && print.time < end,
            "print of {ticker} at {} is outside the window",
            print.time
        );
        assert!(
            print.contract.underlying.eq_ignore_ascii_case(UNDERLYING),
            "print of {ticker} is for {}",
            print.contract.underlying
        );
        assert!(
            ticker.starts_with(UNDERLYING),
            "ticker {ticker} does not name its underlying"
        );
        assert!(
            print.price > Decimal::ZERO,
            "print of {ticker} has no price"
        );
        assert!(print.size > 0, "print of {ticker} has no size");
        assert!(print.pair_id > 0, "print of {ticker} has no pair id");
        assert!(!print.venue.is_empty(), "print of {ticker} has no venue");
        assert!(
            print.contract.strike > Decimal::ZERO,
            "print of {ticker} has no strike"
        );
        assert!(
            print.contract.expiry >= print.time.date_naive(),
            "print of {ticker} is for a contract that had already expired"
        );
    }

    let with_greeks = prints
        .iter()
        .filter(|print| print.greeks.has_any_greek())
        .count();
    println!("{with_greeks}/{} prints carry greeks", prints.len());
    assert!(
        with_greeks > 0,
        "no print carries any greek: the tape has stopped publishing them, or they were renamed"
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn paging_by_time_returns_exactly_what_one_page_returns() {
    let Some(client) = client() else { return };
    let (start, _) = recent_session(&client).await;
    // SPY prints some 350 times a second mid-session (21,095 in one minute, measured 2026-11-25),
    // so two seconds fit one page with room to spare.
    let end = start + Duration::seconds(2);

    let whole = raw_page(start, end, MAX_PAGE_ROWS).await;
    assert!(!whole.is_empty(), "the probe window emptied between reads");
    assert!(
        whole.len() < MAX_PAGE_ROWS,
        "{} rows fill a page, so one page cannot be the reference; shorten the window",
        whole.len()
    );

    // Pages far smaller than the window, walked as the fetch walks: hold back the latest instant's
    // prints and start the next page at it.
    let limit = (whole.len() / 4).max(2);
    let mut walked = Vec::new();
    let mut cursor = start;
    let mut requests = 0;
    loop {
        let page = raw_page(cursor, end, limit).await;
        requests += 1;
        let Some(latest) = page.last().map(row_time) else {
            break;
        };
        if latest == cursor {
            assert!(
                page.len() < limit,
                "a full page at one instant; raise the limit"
            );
            walked.extend(page);
            cursor = latest + Duration::nanoseconds(1);
        } else {
            walked.extend(page.into_iter().filter(|row| row_time(row) < latest));
            cursor = latest;
        }
    }
    println!(
        "{} rows in one page, {} walked in {requests} pages of {limit}",
        whole.len(),
        walked.len()
    );
    assert_eq!(
        canonical(&walked),
        canonical(&whole),
        "paging by time did not reproduce the single page"
    );

    let fetched = client
        .collect_option_flow(Some(UNDERLYING), start, end)
        .await
        .expect("fetched read");
    let mut fetched_keys: Vec<_> = fetched
        .iter()
        .map(|print| (print.time, print.pair_id, print.size))
        .collect();
    fetched_keys.sort_unstable();
    let mut whole_keys: Vec<_> = whole
        .iter()
        .map(|row| {
            (
                row_time(row),
                row["pair_id"].as_u64().expect("a numeric pair_id"),
                row["size"].as_u64().expect("a numeric size"),
            )
        })
        .collect();
    whole_keys.sort_unstable();
    assert_eq!(
        fetched_keys, whole_keys,
        "the fetch returned different prints from the single page"
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn candles_and_greeks_exist_for_a_contract_that_printed() {
    let Some(client) = client() else { return };
    let (start, prints) = recent_session(&client).await;
    let ticker = busiest_ticker(&prints);
    let (from, to) = (start - Duration::hours(1), start + Duration::hours(1));

    let candles: Vec<_> = client
        .fetch_option_candles(&ticker, CandleInterval::Min1, from, to)
        .try_collect()
        .await
        .expect("option candle fetch");
    println!("{ticker}: {} candles", candles.len());
    assert!(
        !candles.is_empty(),
        "no candles for {ticker}, which printed in the window"
    );
    for candle in &candles {
        assert!(
            candle.trade_count.is_some_and(|count| count > 0),
            "a candle for {ticker} reports no trades"
        );
        assert!(candle.low <= candle.high, "inverted OHLC for {ticker}");
    }

    let greeks: Vec<_> = client
        .fetch_option_greeks(&ticker, from, to)
        .try_collect()
        .await
        .expect("option greeks fetch");
    println!("{ticker}: {} minutes of greeks", greeks.len());
    assert!(
        !greeks.is_empty(),
        "no greeks for {ticker}, which printed in the window"
    );
    assert!(
        greeks.iter().any(|minute| minute.greeks.has_any_greek()),
        "no minute for {ticker} carries any greek"
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn the_chain_decodes() {
    let Some(client) = client() else { return };

    let chain = client
        .fetch_option_chain(UNDERLYING, 100, 0)
        .await
        .expect("option chain");

    assert!(
        chain.underlying.eq_ignore_ascii_case(UNDERLYING),
        "the chain echoes {}",
        chain.underlying
    );
    assert!(!chain.entries.is_empty(), "the {UNDERLYING} chain is empty");
    assert!(
        chain.contracts >= chain.entries.len() as u64,
        "{} contracts reported, fewer than the page's {}",
        chain.contracts,
        chain.entries.len()
    );
    for entry in &chain.entries {
        assert!(
            entry.contract.underlying.eq_ignore_ascii_case(UNDERLYING),
            "{} is not a {UNDERLYING} contract",
            entry.contract.ticker
        );
        assert!(
            entry.pair_id > 0,
            "{} has no pair id",
            entry.contract.ticker
        );
    }
    println!(
        "CANARY_OK: {} of {} {UNDERLYING} contracts",
        chain.entries.len(),
        chain.contracts
    );
}
