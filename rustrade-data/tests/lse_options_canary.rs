//! London Strategic Edge options shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for the option print tape and option candles runs against synthetic
//! `wiremock` fixtures, because the provider prohibits redistributing its data
//! (<https://londonstrategicedge.com/terms>) so no recorded response may be committed. Fixtures
//! validate the decoder against *our own assumptions* about the wire format. This canary validates
//! those assumptions against the **real API**.
//!
//! # ⚠️ It must not be green on an empty answer
//!
//! Both option endpoints answer `200` with an empty array for an unknown symbol, a closed market,
//! and a window read before the provider has ingested it — so a shape assertion over whatever came
//! back passes on nothing at all. Every assertion here therefore first requires rows to exist:
//!
//! - **The tape is live**: one minute inside the US session, on the most recent weekday that has
//!   one, must hold prints. The search reaches back a week and no further, so a feed that has
//!   **frozen** — as the provider's economic calendar did — fails here rather than passing on old
//!   data forever.
//! - **The adaptive walk agrees with a single read**: the same window fetched at a tiny page limit,
//!   which forces the walk to halve repeatedly, must return exactly the prints one page returns.
//!   That is the property the walk's correctness rests on — that a full page means a truncated one,
//!   and that narrowing the window recovers what was cut.
//! - **Greeks arrive with prints** — at least one print carries one. A tape that silently stopped
//!   carrying them would otherwise decode cleanly to all-`None`.
//! - **Candles exist for a contract that printed**, carrying a print count.
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
//! Every test is `#[serial]`: clients built separately do not share a request gate, and the
//! provider rations concurrent requests per key.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_options_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the shared allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Datelike, Duration, NaiveTime, Utc, Weekday};
use rust_decimal::Decimal;
use rustrade_data::exchange::lse::options::LseOptionPrint;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use serial_test::serial;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

const KEY_ENV: &str = "LSE_API_KEY";

/// The busiest underlying the provider carries, so one minute of it is never legitimately empty
/// on a trading day.
const UNDERLYING: &str = "SPY";

/// 15:00 UTC is inside the US options session on both sides of the DST change (13:30–20:00 UTC in
/// summer, 14:30–21:00 in winter), so the probe window never lands on a closed market by season.
const PROBE_TIME: NaiveTime = NaiveTime::from_hms_opt(15, 0, 0).unwrap();

/// How far back to look for a session. A week always spans at least five weekdays, which absorbs a
/// holiday; a feed with nothing in it has stopped.
const LOOKBACK_DAYS: i64 = 7;

fn client() -> Option<LseVaultClient> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseVaultClient::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

/// The one-minute probe window on the most recent weekday that holds prints, and those prints.
async fn recent_session(client: &LseVaultClient) -> (DateTime<Utc>, Vec<LseOptionPrint>) {
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

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_print_tape_is_live_and_decodes() {
    let Some(client) = client() else { return };
    let (start, prints) = recent_session(&client).await;
    let end = start + Duration::minutes(1);

    let mut ids = HashSet::new();
    for pair in prints.windows(2) {
        assert!(
            (pair[0].time, pair[0].id) <= (pair[1].time, pair[1].id),
            "prints out of order: {} then {}",
            pair[0].id,
            pair[1].id
        );
    }
    for print in &prints {
        assert!(ids.insert(print.id), "print id {} repeated", print.id);
        assert!(
            print.time >= start && print.time < end,
            "print {} outside window",
            print.id
        );
        assert!(
            print.contract.underlying.eq_ignore_ascii_case(UNDERLYING),
            "print {} is for {}",
            print.id,
            print.contract.underlying
        );
        assert!(
            print.contract.ticker.starts_with(UNDERLYING),
            "ticker {} does not name its underlying",
            print.contract.ticker
        );
        assert!(
            print.price > Decimal::ZERO,
            "print {} has no price",
            print.id
        );
        assert!(print.volume > 0, "print {} has no size", print.id);
        assert!(
            print.contract.strike > Decimal::ZERO,
            "print {} has no strike",
            print.id
        );
        assert!(
            print.contract.expiry >= print.time.date_naive(),
            "print {} is for a contract that had already expired",
            print.id
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
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_adaptive_walk_recovers_exactly_what_one_page_returns() {
    let Some(client) = client() else { return };
    let (start, _) = recent_session(&client).await;
    let end = start + Duration::seconds(20);

    let whole = client
        .collect_option_flow(Some(UNDERLYING), start, end)
        .await
        .expect("single-page read");
    assert!(!whole.is_empty(), "the probe window emptied between reads");

    // A limit far below the window's size forces the walk to treat nearly every read as truncated,
    // halve, and re-read — the path a busy underlying takes against the real cap.
    let page_limit = u32::try_from(whole.len() / 4).unwrap().max(2);
    let walked = client
        .clone()
        .with_page_limit(NonZeroU32::new(page_limit).unwrap())
        .collect_option_flow(Some(UNDERLYING), start, end)
        .await
        .expect("walked read");

    let whole_ids: Vec<u64> = whole.iter().map(|print| print.id).collect();
    let walked_ids: Vec<u64> = walked.iter().map(|print| print.id).collect();
    println!(
        "{} prints read whole, {} walked at a page limit of {page_limit}",
        whole_ids.len(),
        walked_ids.len()
    );
    assert_eq!(
        walked_ids, whole_ids,
        "the adaptive walk did not reproduce the single-page read"
    );
}

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn option_candles_exist_for_a_contract_that_printed() {
    let Some(client) = client() else { return };
    let (start, prints) = recent_session(&client).await;

    let mut counts: HashMap<&str, usize> = HashMap::new();
    for print in &prints {
        *counts.entry(print.contract.ticker.as_str()).or_default() += 1;
    }
    let (ticker, _) = counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .expect("at least one print");

    let candles = client
        .collect_option_candles(
            ticker,
            start - Duration::hours(1),
            start + Duration::hours(1),
        )
        .await
        .expect("option candle fetch");
    println!("{ticker}: {} candles", candles.len());

    assert!(
        !candles.is_empty(),
        "no candles for {ticker}, which printed in the window"
    );
    for candle in &candles {
        assert_eq!(candle.contract.ticker.as_str(), ticker);
        assert!(
            candle.candle.trade_count.is_some_and(|count| count > 0),
            "a candle for {ticker} reports no prints"
        );
        assert!(
            candle.candle.low <= candle.candle.high,
            "inverted OHLC for {ticker}"
        );
    }
    assert!(
        candles.iter().any(|candle| candle.greeks.has_any_greek()),
        "no candle for {ticker} carries any averaged greek"
    );
}
