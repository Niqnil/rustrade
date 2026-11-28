//! LSE Data 3.0 macro canary (network + credential gated).
//!
//! Every in-repo test of the macro reader runs on synthetic `wiremock` answers, because the
//! provider prohibits passing its data on (<https://londonstrategicedge.com/licence>). This canary
//! checks the reader against the **real API**: the summary names the three kinds, each kind's
//! catalogue decodes, and a series of each kind reads into its typed rows.
//!
//! It finds its codes in the catalogue rather than naming them, so a series the provider renames or
//! drops does not fail it; the catalogue disappearing does.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes).
//! - `LSE_API_KEY` set but unusable → **FAIL**.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_macro_canary --features lse -- --ignored --nocapture
//! ```

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::macroeconomic::{LseMacroFilter, LseMacroKind, LseMacroSeries};
use serial_test::serial;

const KEY_ENV: &str = "LSE_API_KEY";

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

/// The first few US series of `kind`, by the catalogue's order.
async fn us_series(client: &LseClient, kind: LseMacroKind) -> Vec<LseMacroSeries> {
    let page = client
        .fetch_macro_catalogue(kind, LseMacroFilter::default().country("US"), 5, 0)
        .await
        .unwrap();
    println!("CANARY: {} US {} series", page.total, kind.as_str());
    assert!(!page.series.is_empty(), "no US {} series", kind.as_str());
    assert!(page.series.iter().all(|series| series.kind == kind));
    page.series
}

#[tokio::test]
#[ignore = "reaches the provider; run on demand"]
#[serial]
async fn the_summary_names_every_kind() {
    let Some(client) = client() else { return };

    let summary = client.fetch_macro_summary().await.unwrap();

    for kind in [
        LseMacroKind::Observation,
        LseMacroKind::Yield,
        LseMacroKind::Release,
    ] {
        let entry = summary
            .kinds
            .iter()
            .find(|entry| entry.kind == kind)
            .unwrap_or_else(|| panic!("the summary lacks {}", kind.as_str()));
        assert!(entry.series > 0, "{} has no series", kind.as_str());
    }
    assert!(
        summary
            .countries
            .iter()
            .any(|country| country.country == "US")
    );
}

#[tokio::test]
#[ignore = "reaches the provider; run on demand"]
#[serial]
async fn a_series_of_each_kind_reads_into_typed_rows() {
    let Some(client) = client() else { return };

    let mut read = 0;
    for series in us_series(&client, LseMacroKind::Yield).await {
        let rows = client
            .fetch_macro_yields(&series.code, None, None)
            .await
            .unwrap();
        println!("CANARY: yield {} has {} days", series.code, rows.len());
        assert!(rows.windows(2).all(|pair| pair[0].day < pair[1].day));
        read += usize::from(!rows.is_empty());
    }
    assert!(read > 0, "no US yield series had a day");

    let mut read = 0;
    for series in us_series(&client, LseMacroKind::Observation).await {
        let rows = client
            .fetch_macro_observations(&series.code, None, None)
            .await
            .unwrap();
        println!(
            "CANARY: observation {} has {} days",
            series.code,
            rows.len()
        );
        assert!(rows.windows(2).all(|pair| pair[0].day < pair[1].day));
        read += usize::from(!rows.is_empty());
    }
    assert!(read > 0, "no US observation series had a day");

    let mut released = 0;
    let mut parsed = 0;
    for series in us_series(&client, LseMacroKind::Release).await {
        let rows = client
            .fetch_macro_releases(&series.code, None, None)
            .await
            .unwrap();
        println!(
            "CANARY: release {} has {} releases",
            series.code,
            rows.len()
        );
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].released_at <= pair[1].released_at)
        );
        for actual in rows.iter().filter_map(|row| row.actual.as_ref()) {
            released += 1;
            parsed += usize::from(actual.parse().is_some());
        }
    }
    println!("CANARY: {parsed} of {released} released figures start with a number");
    assert!(released > 0, "no US release series had a released figure");
    assert!(parsed > 0, "no released figure starts with a number");
}
