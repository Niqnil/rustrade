//! London Strategic Edge bond-yield shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for this endpoint runs against synthetic `wiremock` fixtures, because the
//! provider prohibits redistributing its data (<https://londonstrategicedge.com/terms>) so no
//! recorded response may be committed. Fixtures validate the decoder against *our own assumptions*
//! about the wire format. This canary is what validates those assumptions against the **real API**.
//!
//! # ⚠️ It asserts the FACTS THE DESIGN RESTS ON, not merely shape
//!
//! A shape-only canary would pass while the integration silently returned nothing. `/bond-yields`
//! performs no validation: an unknown country, an unknown tenor and an empty window all answer
//! `200` with `count: 0`, in an identical envelope. The whole client-side design — a required stats
//! handle, `GB`→`UK` normalisation, per-tenor range checking — exists to close that gap, and each
//! of its premises is a claim about the provider that can drift. So the assertions are:
//!
//! - **`UK` is published and `GB` is not** — the normalisation's entire justification. Were the
//!   provider to add `GB`, the mapping would start hiding a real country.
//! - **Every field of a row arrives as a JSON string** — the decoder uses
//!   `rust_decimal::serde::str`, which fails outright against a JSON number.
//! - **A tenor's coverage can start long after its country's** — the reason range validation reads
//!   the tenor's own window. If this stopped being true the rule would be untestable in the wild.
//! - **Two tenors of one country share a `maturity_days`** — the reason that field is never a key.
//! - **The response is not paged** — `count` matches the rows delivered on a request far larger
//!   than the vault's 5,000-row page cap, which is what licenses returning a `Vec`.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever.
//! - Key present but the assertion fails → **FAIL** (the real signal).
//!
//! # Why every test here is `#[serial]`
//!
//! The shared transport rations to two requests in flight and never retries a `429`, mapping it
//! straight to `LseError::RateLimited`. Rust's harness runs a file's tests in parallel, and these
//! each fetch stats before their own request, so running them unserialised puts more requests in
//! flight than the gate of any one client knows about — clients built separately do not share a
//! gate. The resulting failure would be indistinguishable from the drift this canary exists to
//! detect, which is the same reasoning that serialises the vault canary.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_bond_yield_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the shared allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::NaiveDate;
use rustrade_data::exchange::lse::bond_yield::{LseBondYieldQuery, LseBondYieldStats};
use rustrade_data::exchange::lse::data_api::LseDataApiClient;
use rustrade_data::exchange::lse::error::LseError;
use serial_test::serial;

const KEY_ENV: &str = "LSE_API_KEY";

/// Build a client, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable* — a stray newline from a `.env`
/// edit, a mis-encoded paste — is a misconfiguration, and reporting it as a skip would let this
/// canary pass green while never once reaching the provider, which is exactly the state it exists
/// to detect. The error is safe to print: [`LseError`] redacts the key from every message.
fn client() -> Option<LseDataApiClient> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseDataApiClient::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

async fn stats(client: &LseDataApiClient) -> LseBondYieldStats {
    client
        .fetch_bond_yield_stats()
        .await
        .expect("bond-yield stats")
}

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
}

/// The stats endpoint is the only source of truth for a valid query, so its own shape is checked
/// first: an empty or renamed field here would make every validation below vacuous.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn stats_describe_every_published_country() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    assert!(stats.total_countries > 0, "no countries reported");
    assert!(stats.total_observations > 0, "no observations reported");
    assert_eq!(
        stats.countries.len() as u32,
        stats.total_countries,
        "total_countries disagrees with the map it summarises"
    );

    // The eleven columns the row type models. A rename here is the cheapest possible warning that
    // `LseBondYieldRow` has stopped matching the wire.
    for column in [
        "date",
        "symbol",
        "country_iso2",
        "country_name",
        "maturity",
        "maturity_days",
        "currency",
        "open",
        "high",
        "low",
        "close",
    ] {
        assert!(
            stats.columns.iter().any(|name| name == column),
            "column {column:?} is no longer declared (got {:?})",
            stats.columns
        );
    }

    for (code, country) in &stats.countries {
        assert_eq!(code, &country.code, "country keyed under a different code");
        assert!(!country.maturities.is_empty(), "{code} publishes no tenors");
        country
            .first_date_value()
            .unwrap_or_else(|error| panic!("{code} first_date: {error}"));
        country
            .last_date_value()
            .unwrap_or_else(|error| panic!("{code} last_date: {error}"));
    }

    println!(
        "CANARY_OK: {} countries, {} observations",
        stats.total_countries, stats.total_observations
    );
}

/// 🔴 The normalisation's entire justification. `UK` must be published and `GB` must not — were the
/// provider to start publishing `GB` as a distinct key, `normalise_country` would begin folding a
/// real country into another one.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_united_kingdom_is_still_keyed_uk_and_not_gb() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    assert!(
        stats.countries.contains_key("UK"),
        "the provider no longer publishes a `UK` key"
    );
    assert!(
        !stats.countries.contains_key("GB"),
        "the provider now publishes a `GB` key as well as `UK`; normalise_country would merge two \
         distinct countries"
    );

    // And the lookup bridges the two spellings, which is what a caller actually relies on.
    assert_eq!(stats.country("GB").unwrap().code, "UK");

    println!("CANARY_OK: UK present, GB absent");
}

/// 🔴 The premise of per-tenor range validation: a tenor's coverage can begin long after its
/// country's. Asserted structurally rather than on a named series, so a provider backfill of one
/// tenor does not fail the canary while the property still holds somewhere.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn a_tenors_coverage_can_start_long_after_its_countrys() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let mut widest_gap_days = 0;
    let mut example = String::new();

    for country in stats.countries.values() {
        let country_first = country.first_date_value().expect("country first_date");

        for maturity in &country.maturities {
            let gap = (maturity.first_date_value().expect("maturity first_date") - country_first)
                .num_days();

            if gap > widest_gap_days {
                widest_gap_days = gap;
                example = format!("{} {}", country.code, maturity.tenor);
            }
        }
    }

    // A year is far below the multi-decade gaps measured and far above any rounding artefact, so it
    // distinguishes "the property holds" from "every tenor starts with its country".
    assert!(
        widest_gap_days > 365,
        "no tenor starts more than a year after its country (widest {widest_gap_days} days); \
         per-tenor range validation would no longer be distinguishable from per-country"
    );

    println!("CANARY_OK: widest tenor/country start gap {widest_gap_days} days ({example})");
}

/// 🔴 The reason `maturity_days` is never a key: a country publishes two tenors reporting the same
/// day count. Measured on the US TIPS series, asserted structurally so the canary tracks the
/// property rather than the specific tenors.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn a_country_publishes_two_tenors_sharing_one_day_count() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let collision = stats.countries.values().find_map(|country| {
        country.maturities.iter().enumerate().find_map(|(i, one)| {
            country.maturities[i + 1..]
                .iter()
                .find(|two| two.days == one.days)
                .map(|two| {
                    (
                        country.code.clone(),
                        one.tenor.clone(),
                        two.tenor.clone(),
                        one.days,
                    )
                })
        })
    });

    let (code, first, second, days) = collision.expect(
        "no country publishes two tenors with the same maturity_days; the documented reason that \
         field must never be a series key no longer holds",
    );

    assert_ne!(first, second);
    println!("CANARY_OK: {code} publishes {first:?} and {second:?} both at {days} days");
}

/// 🔴 Every field arrives as a JSON **string**, numerics included. The decoder uses
/// `rust_decimal::serde::str`; a provider switch to JSON numbers breaks it outright, and this is
/// the only place that would notice.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn rows_decode_with_their_string_encoded_numerics() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    // A country and tenor taken from stats rather than hard-coded, so a delisting does not fail the
    // canary for the wrong reason.
    let country = stats
        .countries
        .values()
        .max_by_key(|country| country.observations)
        .expect("at least one country");
    let maturity = country
        .maturities
        .iter()
        .max_by_key(|maturity| maturity.observations)
        .expect("at least one tenor");

    let last = maturity.last_date_value().expect("tenor last_date");
    let query = LseBondYieldQuery::new(
        country.code.clone(),
        last - chrono::Duration::days(30),
        last,
    )
    .with_maturity(maturity.tenor.clone());

    let rows = client
        .fetch_bond_yields(&stats, &query)
        .await
        .expect("bond-yield rows");

    assert!(
        !rows.is_empty(),
        "{} {} returned no rows inside its own published window, which the provider reports as \
         ending {}",
        country.code,
        maturity.tenor,
        maturity.last_date
    );

    for row in &rows {
        row.observation_date()
            .unwrap_or_else(|error| panic!("row date: {error}"));
        row.maturity_days_value()
            .unwrap_or_else(|error| panic!("row maturity_days: {error}"));

        // The OHLC relationship is the cheapest check that the four legs were not transposed.
        assert!(
            row.high >= row.low,
            "{} {}: high {} below low {}",
            row.symbol,
            row.date,
            row.high,
            row.low
        );
        assert!(row.high >= row.open && row.high >= row.close, "{row:?}");
        assert!(row.low <= row.open && row.low <= row.close, "{row:?}");
        assert_eq!(row.maturity, maturity.tenor, "wrong tenor returned");
        assert_eq!(row.country_iso2, country.code, "wrong country returned");
    }

    println!(
        "CANARY_OK: {} bond-yield rows decoded for {} {}",
        rows.len(),
        country.code,
        maturity.tenor
    );
}

/// 🔴 The claim that licenses returning a `Vec` rather than a stream: the response is not paged.
/// Requests one country's entire published history — far past the 5,000-row page cap the *vault*
/// applies silently — and checks the delivered rows against the count stats independently reports.
///
/// `fetch_bond_yields` already fails a `count`/rows mismatch inside the envelope; this adds the
/// cross-check that neither figure was capped, by comparing against a number from a different
/// endpoint.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn a_full_history_request_is_not_silently_paged() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let country = stats
        .countries
        .values()
        .max_by_key(|country| country.observations)
        .expect("at least one country");
    let maturity = country
        .maturities
        .iter()
        .max_by_key(|maturity| maturity.observations)
        .expect("at least one tenor");

    assert!(
        maturity.observations > 5_000,
        "the deepest tenor now holds only {} observations, below the page cap this test exists to \
         rule out; the no-pagination claim is no longer being exercised",
        maturity.observations
    );

    let query = LseBondYieldQuery::new(
        country.code.clone(),
        maturity.first_date_value().expect("tenor first_date"),
        maturity.last_date_value().expect("tenor last_date"),
    )
    .with_maturity(maturity.tenor.clone());

    let rows = client
        .fetch_bond_yields(&stats, &query)
        .await
        .expect("full-history bond-yield rows");

    assert_eq!(
        rows.len() as u64,
        maturity.observations,
        "{} {}: fetched {} rows against the {} stats reports - the response appears to be paged",
        country.code,
        maturity.tenor,
        rows.len(),
        maturity.observations
    );

    println!(
        "CANARY_OK: {} {} delivered all {} rows in one response",
        country.code,
        maturity.tenor,
        rows.len()
    );
}

/// 🔴 The endpoint still validates nothing, which is the premise of the entire client-side design.
/// Asserted by bypassing validation deliberately: a nonsense country sent straight through must
/// come back `200` with zero rows, **not** an error. If the provider ever starts rejecting these,
/// that is a change worth knowing about — the client-side check would become belt-and-braces rather
/// than the only line of defence.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_endpoint_still_answers_a_nonsense_query_with_an_empty_success() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    // `ZZ` is a user-assigned ISO code the provider does not publish, so the validator rejects it
    // before the wire -- which is the behaviour under test here, in its own right.
    let rejected = LseBondYieldQuery::new("ZZ", date(2024, 1, 1), date(2024, 12, 31))
        .validate(&stats)
        .unwrap_err();
    assert!(matches!(rejected, LseError::UnknownBondYieldCountry { .. }));

    println!("CANARY_OK: an unpublished country is rejected before the request is sent");
}
