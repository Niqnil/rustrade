//! London Strategic Edge economic-calendar shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for this endpoint runs against synthetic `wiremock` fixtures, because the
//! provider prohibits redistributing its data (<https://londonstrategicedge.com/terms>) so no
//! recorded response may be committed. Fixtures validate the decoder against *our own assumptions*
//! about the wire format. This canary is what validates those assumptions against the **real API**.
//!
//! # 🔴 It must stay meaningful on a FROZEN feed
//!
//! The calendar stopped publishing on 2026-03-24 and has not moved since — measured identical to
//! the unit two months apart. That makes the usual "is fresh data arriving?" assertion useless
//! here, and worse than useless as a gate: it would fail every week for a reason nobody can fix,
//! and a canary that always fails is a canary nobody reads.
//!
//! So the assertions below are **structural**, and hold whether or not the feed ever revives:
//!
//! - The stats envelope still describes a non-empty vocabulary and a parseable range.
//! - `UK` is published and `GB` is not — the normalisation's entire justification.
//! - The impact vocabulary is still exactly the four ratings, `"None"` among them as a literal
//!   string rather than a null.
//! - Every field of an event decodes, numerics arriving as JSON strings.
//! - `event_date` still carries a real time of day, which is why it is a `DateTime<Utc>`.
//! - A comma-separated country list is still a genuine OR, and still not a repeated key.
//! - The response is still not paged.
//!
//! # The freeze is RECORDED, not asserted
//!
//! [`total_events`] and [`latest`] are printed on every run rather than compared against 124,896
//! and 2026-03-24. A hard assertion either way would be wrong: asserting they are unchanged makes
//! a *revival* — the outcome we want — fail the build, and asserting they have changed fails
//! forever. Printing them puts the two numbers in the weekly log, where a human reading a drift
//! report sees the day they move.
//!
//! The one thing that *is* asserted is that `latest` never goes **backwards** past the earliest
//! bound, which would mean the archive itself had been truncated.
//!
//! [`total_events`]: rustrade_data::exchange::lse::calendar::LseCalendarStats::total_events
//! [`latest`]: rustrade_data::exchange::lse::calendar::LseCalendarStats::latest
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
//! detect, which is the same reasoning that serialises the vault and bond-yield canaries.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_economic_calendar_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the shared allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use rustrade_data::exchange::lse::calendar::{
    LseCalendarImpact, LseCalendarQuery, LseCalendarStats,
};
use rustrade_data::exchange::lse::data_api::LseDataApiClient;
use rustrade_data::exchange::lse::error::LseError;
use serial_test::serial;

const KEY_ENV: &str = "LSE_API_KEY";

/// The date the feed stopped, as measured. Printed for comparison, never asserted — see the module
/// documentation for why a revival must not fail this canary.
const MEASURED_LATEST: &str = "2026-03-24";

/// The event count at both measurements, two months apart. Printed, never asserted.
const MEASURED_TOTAL_EVENTS: u64 = 124_896;

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

async fn stats(client: &LseDataApiClient) -> LseCalendarStats {
    client
        .fetch_economic_calendar_stats()
        .await
        .expect("economic-calendar stats")
}

/// The stats endpoint is the only source of truth for a valid query, so its own shape is checked
/// first: an empty or renamed field here would make every validation below vacuous.
///
/// 🔴 This is also where the freeze is **recorded**. The two figures are printed, not asserted.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn stats_describe_a_non_empty_vocabulary_and_a_parseable_range() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    assert!(!stats.countries.is_empty(), "no countries reported");
    assert!(!stats.impacts.is_empty(), "no impact ratings reported");
    assert!(stats.total_events > 0, "no events reported");

    let earliest = stats.earliest_date().expect("earliest");
    let latest = stats.latest_date().expect("latest");

    // The archive must not shrink. This is the only direction worth failing on: `latest` moving
    // FORWARD is the revival we are watching for and must never fail the build.
    assert!(
        earliest <= latest,
        "the published range is inverted: {earliest}..={latest}"
    );

    // Every code is two characters -- the property `normalise_country`'s upper-casing relies on.
    for code in &stats.countries {
        assert_eq!(
            code.len(),
            2,
            "country code {code:?} is not the two characters every published code has been"
        );
        assert_eq!(
            code.as_str(),
            code.to_ascii_uppercase(),
            "country code {code:?} is not upper-case; the client-side membership check would miss it"
        );
    }

    // 🔴 RECORDED, NOT ASSERTED. A human reading the weekly log sees the day these move.
    println!(
        "CANARY_OK: {} countries, {} events, range {earliest}..={latest}",
        stats.countries.len(),
        stats.total_events
    );
    if stats.latest != MEASURED_LATEST || stats.total_events != MEASURED_TOTAL_EVENTS {
        println!(
            "CANARY_NOTE: the feed has MOVED. Measured {MEASURED_LATEST} / \
             {MEASURED_TOTAL_EVENTS} events; now {} / {} events. If `latest` has advanced, the \
             economic calendar has revived and the module documentation's freeze warning needs \
             revisiting.",
            stats.latest, stats.total_events
        );
    } else {
        println!(
            "CANARY_NOTE: still frozen at {MEASURED_LATEST} with {MEASURED_TOTAL_EVENTS} events, \
             unchanged since the first measurement."
        );
    }
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
        stats.countries.iter().any(|code| code == "UK"),
        "the provider no longer publishes a `UK` code"
    );
    assert!(
        !stats.countries.iter().any(|code| code == "GB"),
        "the provider now publishes a `GB` code as well as `UK`; normalise_country would merge \
         two distinct countries"
    );

    // And the lookup bridges the two spellings, which is what a caller actually relies on.
    assert!(stats.publishes_country("GB"));

    println!("CANARY_OK: UK present, GB absent");
}

/// 🔴 The impact vocabulary is closed at four, and `"None"` is a literal rating rather than a JSON
/// null. `LseCalendarImpact` is a closed enum, so a fifth rating is a decode failure — this is the
/// only place that would notice before it reached a caller.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_impact_vocabulary_is_still_exactly_four_ratings() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let mut published: Vec<&str> = stats.impacts.iter().map(|rating| rating.as_str()).collect();
    published.sort_unstable();

    assert_eq!(
        published,
        ["High", "Low", "Medium", "None"],
        "the impact vocabulary has changed; LseCalendarImpact is a closed enum and would fail to \
         decode a rating outside this set"
    );

    // Each one must resolve through the client's own lookup, and `"None"` must be a rating.
    for rating in [
        LseCalendarImpact::High,
        LseCalendarImpact::Medium,
        LseCalendarImpact::Low,
        LseCalendarImpact::None,
    ] {
        assert_eq!(
            stats
                .resolve_impact(rating.as_str())
                .map(|resolved| resolved.as_str()),
            Some(rating.as_str()),
            "{rating} no longer resolves against the published vocabulary"
        );
    }

    println!("CANARY_OK: impacts are exactly {published:?}, \"None\" among them as a rating");
}

/// 🔴 Every field arrives as a JSON **string**, numerics included, and `event_date` carries a real
/// time of day. The decoder uses `rust_decimal::serde::str_option`, which a JSON number breaks
/// outright, and `DateTime<Utc>` rather than a date because 348 distinct times occur.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn events_decode_with_their_string_encoded_numerics_and_intraday_timestamps() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    // The window is taken from stats rather than hard-coded, so the frozen end does not have to be
    // known here -- and so this keeps working unchanged if the feed revives.
    let latest = stats.latest_date().expect("latest");
    let query = LseCalendarQuery::new(latest - chrono::Duration::days(60), latest);

    let events = client
        .fetch_economic_calendar(&stats, &query)
        .await
        .expect("economic-calendar events");

    assert!(
        !events.is_empty(),
        "the 60 days ending at the provider's own `latest` ({latest}) returned no events across \
         every country, which should be impossible while the archive exists"
    );

    let mut times_of_day = std::collections::BTreeSet::new();

    for event in &events {
        let time = event
            .event_time()
            .unwrap_or_else(|error| panic!("event_date: {error}"));
        times_of_day.insert(time.format("%H:%M:%S").to_string());

        assert!(
            !event.country.is_empty(),
            "event with no country: {event:?}"
        );
        assert!(!event.event.is_empty(), "event with no name: {event:?}");

        // 🔴 Absence is spelled `null`, never an empty string. An empty string here would decode
        // into `Some("")` and quietly defeat every `is_none()` check a caller writes.
        assert_ne!(
            event.currency.as_deref(),
            Some(""),
            "currency arrived as an empty string rather than null: {event:?}"
        );
        assert_ne!(
            event.unit.as_deref(),
            Some(""),
            "unit arrived as an empty string rather than null: {event:?}"
        );
    }

    // 🔴 The claim that licenses `DateTime<Utc>` over `NaiveDate`. If every event in a 60-day
    // window shared one time of day, a date would carry the same information and the richer type
    // would be unjustified.
    assert!(
        times_of_day.len() > 1,
        "every event in the window shares the time of day {:?}; `event_date` would no longer be \
         carrying intraday information",
        times_of_day
    );

    println!(
        "CANARY_OK: {} events decoded across {} distinct times of day",
        events.len(),
        times_of_day.len()
    );
}

/// 🔴 A comma-separated country list is a genuine OR, and the client must never repeat the key.
/// Asserted arithmetically: the pair must return exactly the sum of the two singles. A client that
/// repeated the key would return only the second country's events, which this catches as a
/// mismatch rather than as a plausible-looking number.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn a_comma_separated_country_list_is_still_a_real_or() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let earliest = stats.earliest_date().expect("earliest");
    let latest = stats.latest_date().expect("latest");

    // Two codes taken from the published vocabulary rather than hard-coded, so a delisting does not
    // fail the canary for the wrong reason. `UK` is pinned because the GB->UK bridge runs through
    // it; the partner is simply the first other code published.
    let partner = stats
        .countries
        .iter()
        .find(|code| *code != "UK")
        .expect("a second published country")
        .clone();

    let count = |country: Option<&str>| {
        let mut query = LseCalendarQuery::new(earliest, latest);
        if let Some(country) = country {
            query = query.with_country(country);
        }
        query
    };

    let uk_only = client
        .fetch_economic_calendar(&stats, &count(Some("UK")))
        .await
        .expect("UK events")
        .len();
    let partner_only = client
        .fetch_economic_calendar(&stats, &count(Some(partner.as_str())))
        .await
        .expect("partner events")
        .len();

    let both = client
        .fetch_economic_calendar(
            &stats,
            &LseCalendarQuery::new(earliest, latest).with_countries(["UK", partner.as_str()]),
        )
        .await
        .expect("combined events")
        .len();

    assert_eq!(
        both,
        uk_only + partner_only,
        "UK ({uk_only}) + {partner} ({partner_only}) should equal the combined query ({both}); a \
         repeated `country` key would return only the last country's events"
    );

    println!("CANARY_OK: UK ({uk_only}) + {partner} ({partner_only}) == {both} combined");
}

/// 🔴 The claim that licenses returning a `Vec` rather than a stream: the response is not paged.
/// Requests the entire corpus and checks the delivered events against the count stats independently
/// reports.
///
/// `fetch_economic_calendar` already fails a `count`/events mismatch inside the envelope; this adds
/// the cross-check that neither figure was capped, by comparing against a number from a different
/// endpoint.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn the_whole_corpus_arrives_in_one_unpaged_response() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let query = LseCalendarQuery::new(
        stats.earliest_date().expect("earliest"),
        stats.latest_date().expect("latest"),
    );

    let events = client
        .fetch_economic_calendar(&stats, &query)
        .await
        .expect("the whole economic-calendar corpus");

    assert_eq!(
        events.len() as u64,
        stats.total_events,
        "fetched {} events against the {} stats reports - the response appears to be paged",
        events.len(),
        stats.total_events
    );

    println!(
        "CANARY_OK: all {} events delivered in one response",
        events.len()
    );
}

/// 🔴 The endpoint still validates nothing, which is the premise of the entire client-side design,
/// and the frozen end is still enforced before the wire.
#[tokio::test]
#[ignore = "spends the shared provider allowance; run on demand"]
#[serial]
async fn an_invalid_query_is_still_rejected_before_the_request_is_sent() {
    let Some(client) = client() else { return };
    let stats = stats(&client).await;

    let latest = stats.latest_date().expect("latest");

    // `ZZ` is a user-assigned ISO code the provider does not publish.
    let unknown_country = LseCalendarQuery::new(stats.earliest_date().expect("earliest"), latest)
        .with_country("ZZ")
        .validate(&stats)
        .unwrap_err();
    assert!(matches!(
        unknown_country,
        LseError::UnknownCalendarCountry { .. }
    ));

    let unknown_impact = LseCalendarQuery::new(stats.earliest_date().expect("earliest"), latest)
        .with_impact("Critical")
        .validate(&stats)
        .unwrap_err();
    assert!(matches!(
        unknown_impact,
        LseError::UnknownCalendarImpact { .. }
    ));

    // Everything after the published end, which on a frozen feed is every future window.
    let after_the_end = LseCalendarQuery::new(
        latest + chrono::Duration::days(1),
        latest + chrono::Duration::days(365),
    )
    .validate(&stats)
    .unwrap_err();
    assert!(matches!(
        after_the_end,
        LseError::CalendarRangeOutsideCoverage { .. }
    ));

    println!("CANARY_OK: unknown country, unknown impact and a post-{latest} window all rejected");
}
