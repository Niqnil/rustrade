//! London Strategic Edge vault shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for this integration runs against synthetic `wiremock` fixtures, because the
//! provider prohibits redistributing its data (<https://londonstrategicedge.com/terms>) so no
//! recorded response may be committed. Fixtures validate the decoder against *our own assumptions*
//! about the wire format. This canary is what validates those assumptions against the **real API**.
//!
//! # ⚠️ It asserts SPACING, not merely shape
//!
//! A shape-only canary would pass on wrong data here. The vault **silently ignores unknown query
//! parameters** and defaults to 1-minute bars: requesting `resolution=1d` (the wrong parameter
//! name) returns 1-minute bars with a `200` and a **byte-identical response shape**. Field presence
//! and types are therefore blind to the single most consequential regression this integration can
//! suffer. Timestamp spacing is the only detectable signal, so it is asserted:
//!
//! - **no gap shorter than the requested interval** — catches a finer-than-requested response, the
//!   exact failure a silently-ignored parameter produces;
//! - **at least one gap equal to it** — catches a coarser-than-requested response.
//!
//! Gaps *longer* than the interval are legitimate and expected: the vault omits periods with no
//! activity, so weekends and holidays appear as gaps rather than as filled bars.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever.
//! - Key present but the assertion fails → **FAIL** (the real signal).
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_vault_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the shared allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Duration, Utc};
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_data::subscription::candle::{Candle, CandleInterval};

const KEY_ENV: &str = "LSE_API_KEY";

/// Build a client, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable* — a stray newline from a `.env`
/// edit, a mis-encoded paste — is a misconfiguration, and reporting it as a skip would let this
/// canary pass green while never once reaching the provider, which is exactly the state it exists
/// to detect. The error is safe to print: [`LseError`] redacts the key from every message.
///
/// [`LseError`]: rustrade_data::exchange::lse::error::LseError
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

/// Assert the bars actually arrived at the resolution that was requested.
///
/// See the module header: this is the assertion a shape-only canary cannot make.
fn assert_spacing(candles: &[Candle], interval: CandleInterval, expected: Duration) {
    assert!(
        candles.len() >= 3,
        "{interval} returned {} candles; too few to check spacing",
        candles.len()
    );

    let gaps = candles
        .windows(2)
        .map(|pair| pair[1].close_time - pair[0].close_time)
        .collect::<Vec<_>>();

    let shortest = gaps.iter().min().expect("at least one gap");
    assert!(
        *shortest >= expected,
        "{interval}: shortest gap is {shortest}, shorter than the requested {expected} - the \
         request appears to have been served at a finer resolution than asked for"
    );

    assert!(
        gaps.contains(&expected),
        "{interval}: no gap equals the requested {expected} (gaps: {gaps:?}) - the request appears \
         to have been served at a coarser resolution than asked for"
    );
}

/// Candles must arrive strictly ascending by `close_time` — the ordering every downstream
/// consumer, and the backtest replay in particular, relies on.
fn assert_ascending(candles: &[Candle], interval: CandleInterval) {
    assert!(
        candles
            .windows(2)
            .all(|pair| pair[0].close_time < pair[1].close_time),
        "{interval}: candles are not strictly ascending by close_time"
    );
}

fn range(days: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let end = Utc::now();
    (end - Duration::days(days), end)
}

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
async fn daily_candles_arrive_at_daily_spacing() {
    let Some(client) = client() else { return };
    let (start, end) = range(30);

    let candles = client
        .collect_candles("EUR/USD", CandleInterval::Day1, start, end)
        .await
        .expect("daily EUR/USD candles");

    assert_ascending(&candles, CandleInterval::Day1);
    assert_spacing(&candles, CandleInterval::Day1, Duration::days(1));

    // FX carries no volume on this host: the field is omitted, which must surface as the explicit
    // unknown rather than as a zero.
    assert!(
        candles.iter().all(|candle| candle.volume.is_none()),
        "FX candles reported a volume; the vault is expected to omit it entirely"
    );
    println!("CANARY_OK: {} daily EUR/USD candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
async fn hourly_candles_arrive_at_hourly_spacing() {
    let Some(client) = client() else { return };
    let (start, end) = range(3);

    let candles = client
        .collect_candles("EUR/USD", CandleInterval::Hour1, start, end)
        .await
        .expect("hourly EUR/USD candles");

    assert_ascending(&candles, CandleInterval::Hour1);
    assert_spacing(&candles, CandleInterval::Hour1, Duration::hours(1));
    println!("CANARY_OK: {} hourly EUR/USD candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
async fn equity_candles_carry_a_volume() {
    let Some(client) = client() else { return };
    let (start, end) = range(30);

    let candles = client
        .collect_candles("AAPL", CandleInterval::Day1, start, end)
        .await
        .expect("daily AAPL candles");

    assert_ascending(&candles, CandleInterval::Day1);
    assert_spacing(&candles, CandleInterval::Day1, Duration::days(1));

    // The asymmetry with FX above is the point: equities do report volume, so a blanket `None`
    // would mean the field had stopped being decoded.
    assert!(
        candles.iter().any(|candle| candle.volume.is_some()),
        "no AAPL candle carried a volume; the field is expected on equity datasets"
    );
    println!("CANARY_OK: {} daily AAPL candles", candles.len());
}

#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
async fn usage_reports_every_allowance_dimension() {
    let Some(client) = client() else { return };

    let status = client.usage().await.expect("usage");

    // Sanity bounds on the values themselves. Note these cannot detect a *renamed* field:
    // `QuotaStatus` has no `#[serde(default)]`, so a rename is a missing key, which fails
    // deserialisation at the `expect` above before any assertion here runs. That is the desired
    // behaviour -- a rename should be loud -- but it means the real content check is the one below.
    assert!(status.calls_per_minute > 0, "calls_per_minute missing");
    assert!(
        status.max_rows_per_request > 0,
        "max_rows_per_request missing"
    );
    assert!(status.vault_concurrency > 0, "vault_concurrency missing");
    assert!(status.exports_cap_hour > 0, "exports_cap_hour missing");
    assert!(status.bytes_cap_month > 0, "bytes_cap_month missing");
    assert!(status.bytes_cap_week > 0, "bytes_cap_week missing");

    // The page size this integration requests must not exceed what the provider will serve, or
    // every page would be silently capped short.
    assert!(
        status.max_rows_per_request >= 5000,
        "the provider now caps rows at {}, below the page size this integration requests",
        status.max_rows_per_request
    );

    println!("CANARY_OK: usage {status:?}");
}
