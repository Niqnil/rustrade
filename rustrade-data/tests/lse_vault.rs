//! Transport, pagination and range-contract tests for the London Strategic Edge vault.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider
//! data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by the shape canary, which asserts against the real service without
//! storing anything.
//!
//! Run with: `cargo test --test lse_vault --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use rust_decimal_macros::dec;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_data::subscription::candle::CandleInterval;
use std::num::NonZeroU32;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build a client pointed at `server` with pacing disabled so tests do not sleep between pages.
fn client(server: &MockServer) -> LseVaultClient {
    LseVaultClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/vault", server.uri()))
        .with_pace(Duration::ZERO)
}

fn utc(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// One equity-shaped candle row, as the vault serves it: `ts` is the bar's OPEN time.
fn equity_row(ts: &str, close: &str) -> String {
    format!(
        r#"{{"ts":"{ts}","symbol":"AAPL","open":1.0,"high":2.0,"low":0.5,"close":{close},"volume":1000}}"#
    )
}

/// Mount a candle page keyed on the `start` cursor the client is expected to send.
async fn mount_page(server: &MockServer, start: &str, rows: &[String]) {
    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .and(query_param("start", start))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!("[{}]", rows.join(","))))
        .mount(server)
        .await;
}

#[tokio::test]
async fn paginates_by_advancing_the_cursor_one_second_past_the_last_bars_open() {
    let server = MockServer::start().await;

    // Page 1 opens at the widened lower bound and returns two daily bars.
    mount_page(
        &server,
        "2024-01-01 00:00:00",
        &[
            equity_row("2024-01-01 00:00:00.000000", "10.0"),
            equity_row("2024-01-02 00:00:00.000000", "11.0"),
        ],
    )
    .await;
    // The cursor resumes one second past the last bar's OPEN time -- not its close. The parameter
    // is inclusive and rejects sub-second values, so this is the smallest lossless step.
    mount_page(
        &server,
        "2024-01-02 00:00:01",
        &[equity_row("2024-01-03 00:00:00.000000", "12.0")],
    )
    .await;
    // No third page: the cursor then lands exactly on the exclusive upper bound, which selects an
    // empty window, so the fetch stops without asking.

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap();

    // `close_time == open + interval`: the bar labelled 01-01 closes on 01-02.
    let closes = candles
        .iter()
        .map(|candle| candle.close_time)
        .collect::<Vec<_>>();
    assert_eq!(
        closes,
        vec![
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        ]
    );
    assert_eq!(candles[2].close, dec!(12.0));
}

#[tokio::test]
async fn a_short_page_does_not_end_pagination() {
    // The row cap is applied silently, so a short page is indistinguishable from the end of the
    // data. Treating it as terminal would truncate silently if the provider lowered its cap.
    let server = MockServer::start().await;

    mount_page(
        &server,
        "2024-01-01 00:00:00",
        &[equity_row("2024-01-01 00:00:00.000000", "10.0")],
    )
    .await;
    mount_page(
        &server,
        "2024-01-01 00:00:01",
        &[equity_row("2024-01-05 00:00:00.000000", "14.0")],
    )
    .await;
    // The range extends past the data, so the fetch ends on an empty page rather than on the bound.
    mount_page(&server, "2024-01-05 00:00:01", &[]).await;

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-10T00:00:00Z"),
        )
        .await
        .unwrap();

    // The second bar is only reachable if a one-row page did not end the fetch.
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[1].close_time, utc("2024-01-06T00:00:00Z"));
}

#[tokio::test]
async fn pins_the_resolution_parameter_to_timeframe() {
    // The vault ignores unknown parameters and defaults to 1-minute bars, returning a
    // byte-identical shape. Sending `resolution` would silently yield the wrong resolution.
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .and(query_param("timeframe", "1d"))
        .and(query_param("limit", "5000"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap();

    // `expect(1)` is verified on drop.
}

#[tokio::test]
async fn with_page_limit_overrides_the_rows_requested_per_page() {
    // A key on a plan allowing more (or a caller bounding per-page memory) must be able to move
    // this; the default above is the provider's measured cap, not a protocol constant.
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .and(query_param("limit", "250"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .with_page_limit(NonZeroU32::new(250).unwrap())
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap();

    // `expect(1)` is verified on drop.
}

#[tokio::test]
async fn sends_the_provider_spelling_of_one_month() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        // Not the shared enum's `1M`.
        .and(query_param("timeframe", "1mo"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Month1,
            utc("2024-02-01T00:00:00Z"),
            utc("2024-03-01T00:00:00Z"),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn trims_bars_outside_the_close_time_contract() {
    let server = MockServer::start().await;

    // The lower bound is widened to capture the bar whose close equals `start`, so the page can
    // legitimately contain a bar that closes before it. That one must not be yielded.
    //
    // Only the lower side is exercised here. A bar closing past `end` is not a trim but a range
    // violation, and is covered by `a_candle_closing_past_the_requested_end_is_an_error` below.
    mount_page(
        &server,
        "2024-01-03 00:00:00",
        &[
            equity_row("2024-01-02 00:00:00.000000", "11.0"), // closes 01-03, before `start`
            equity_row("2024-01-03 00:00:00.000000", "12.0"), // closes 01-04, in range
        ],
    )
    .await;

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-04T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap();

    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].close_time, utc("2024-01-04T00:00:00Z"));
    assert_eq!(candles[0].close, dec!(12.0));
}

#[tokio::test]
async fn a_candle_closing_past_the_requested_end_is_an_error() {
    let server = MockServer::start().await;

    // The client asks for opens in `[01-03 00:00:00, 01-03 00:00:01)` — the upper bound is
    // `end - interval + 1s` against an exclusive parameter, so the newest bar a compliant page can
    // carry is the one closing exactly on `end`. The 01-04 open closes 01-05 and could only arrive
    // from a vault that ignored the bound it was given.
    mount_page(
        &server,
        "2024-01-03 00:00:00",
        &[
            equity_row("2024-01-03 00:00:00.000000", "12.0"), // closes 01-04, in range
            equity_row("2024-01-04 00:00:00.000000", "13.0"), // closes 01-05, past `end`
        ],
    )
    .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-04T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap_err();

    // Pinned on the fields, not just the variant: trimming the bar away and ending `Ok` was the
    // previous behaviour, and it is unobservable in the returned candles by construction — a test
    // that asserted only on the output could not tell the guard from its absence.
    let LseError::UnexpectedCandleRange {
        symbol, page, end, ..
    } = &error
    else {
        panic!("expected LseError::UnexpectedCandleRange, got {error:?}");
    };
    assert_eq!(symbol, "AAPL");
    assert_eq!(*page, 1);
    assert_eq!(*end, utc("2024-01-04T00:00:00Z"));
}

#[tokio::test]
async fn fx_candles_report_absent_volume_as_none() {
    let server = MockServer::start().await;

    // The measured FX shape: no `volume` key at all.
    let row = r#"{"ts":"2024-01-01 00:00:00.000000","symbol":"EUR/USD","open":1.05,"high":1.06,"low":1.04,"close":1.055}"#;
    mount_page(&server, "2024-01-01 00:00:00", &[row.to_owned()]).await;

    let candles = client(&server)
        .collect_candles(
            "EUR/USD",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-02T00:00:00Z"),
        )
        .await
        .unwrap();

    // `None`, never `Some(0)` -- a synthetic zero would aggregate into a legitimate-looking total.
    assert_eq!(candles[0].volume, None);
    assert_eq!(candles[0].trade_count, None);
}

#[tokio::test]
async fn an_inverted_range_fails_before_any_request_is_sent() {
    let server = MockServer::start().await;

    // No mock is mounted: reaching the network at all would fail the test with a 404 body.
    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-05T00:00:00Z"),
            utc("2024-01-01T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn an_unserved_resolution_is_rejected_before_any_request_is_sent() {
    let server = MockServer::start().await;

    // The provider publishes no 6-hour bars; the caller gets a typed answer, not a relayed 400.
    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Hour6,
            utc("2024-01-01T00:00:00Z"),
            utc("2024-01-02T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::UnsupportedInterval {
            interval: CandleInterval::Hour6
        }
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn a_rate_limit_is_terminal_and_carries_retry_after() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "42")
                .set_body_string(r#"{"detail":"slow down"}"#),
        )
        // Terminal: the client must not sleep and retry on the caller's behalf.
        .expect(1)
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::RateLimited {
            retry_after: Some(delay),
        } if delay == Duration::from_secs(42)
    ));
}

#[tokio::test]
async fn a_double_encoded_error_body_surfaces_the_inner_message() {
    let server = MockServer::start().await;

    // The measured 400 shape: `detail` is itself a JSON document encoded as a string.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            r#"{"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s\"}"}"#,
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap_err();

    match error {
        LseError::Api { status, message } => {
            assert_eq!(status, 400);
            // Not the raw `{"detail": ...}` envelope.
            assert_eq!(message, "invalid timeframe '7q'; valid: 1s, 5s");
        }
        other => panic!("expected Api, got {other:?}"),
    }
}

#[tokio::test]
async fn a_single_encoded_error_body_surfaces_the_message() {
    let server = MockServer::start().await;

    // The measured 401 shape, which nests one level less than the 400.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"detail":"invalid api key"}"#))
        .mount(&server)
        .await;

    let error = client(&server).usage().await.unwrap_err();

    match error {
        LseError::Api { status, message } => {
            assert_eq!(status, 401);
            assert_eq!(message, "invalid api key");
        }
        other => panic!("expected Api, got {other:?}"),
    }
}

#[tokio::test]
async fn usage_reports_the_shared_allowance() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/vault/usage"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"bytes_used_month":5962,"bytes_cap_month":53687091200,
                "bytes_used_week":5962,"bytes_cap_week":16106127360,
                "exports_this_hour":0,"exports_cap_hour":5,
                "historical_data_months":-1,"calls_per_minute":200,
                "max_rows_per_request":5000,"vault_concurrency":2}"#,
        ))
        .mount(&server)
        .await;

    let status = client(&server).usage().await.unwrap();

    assert_eq!(status.exports_cap_hour, 5);
    assert_eq!(status.max_rows_per_request, 5000);
    assert_eq!(status.historical_limit_months(), None);
    assert!(!status.is_exhausted());
}

#[tokio::test]
async fn a_page_that_does_not_advance_the_cursor_is_an_error_not_an_infinite_loop() {
    let server = MockServer::start().await;

    // A provider that ignored `start` would serve page one forever. Surfaced rather than trusted:
    // silently-ignored parameters are a measured behaviour of this API.
    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "[{}]",
            equity_row("2023-01-01 00:00:00.000000", "9.0")
        )))
        .mount(&server)
        .await;

    // Bounded, because the failure this guards against is a *hang*, not a wrong error: delete the
    // non-advance check and this mock answers the same page forever. Without the timeout the test
    // would not fail, it would run until the CI job's own limit killed the whole run with nothing
    // pointing back here.
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        client(&server).collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-10T00:00:00Z"),
        ),
    )
    .await
    .expect("pagination did not terminate: the cursor non-advance guard is gone")
    .unwrap_err();

    // Pinned on the message, not just the variant: every transport and provider failure in this
    // path is also `Api`, so the variant alone would be satisfied by an unrelated error.
    let LseError::Api { message, .. } = &error else {
        panic!("expected LseError::Api, got {error:?}");
    };
    assert!(
        message.contains("did not advance"),
        "expected the cursor non-advance diagnostic, got: {message}"
    );
}

#[tokio::test]
async fn a_malformed_timestamp_surfaces_rather_than_being_skipped() {
    let server = MockServer::start().await;

    mount_page(
        &server,
        "2024-01-01 00:00:00",
        &[r#"{"ts":"not-a-timestamp","symbol":"AAPL","open":1.0,"high":2.0,"low":0.5,"close":1.5,"volume":1}"#.to_owned()],
    )
    .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::Deserialize { .. }));
}

/// A descending page is rejected as a whole, **before** any of its bars is yielded.
///
/// This supersedes an earlier contract on the same input, which yielded the in-range bar and only
/// then reported the range violation. That tolerance was written for a page whose rows arrive in an
/// order the module does not require — but such a page also breaks the cursor arithmetic, which
/// advances past the newest open a page carried, so tolerating it means a newest-first *and*
/// row-capped page silently truncates a multi-million bar fetch to its final page. Order is a
/// property of the response as a whole: a page that violates it is not partially usable, so nothing
/// from it is emitted.
///
/// Driven as a stream rather than through `collect_candles`, which discards everything it collected
/// the moment an item is `Err` — "nothing was yielded first" is invisible through it.
#[tokio::test]
async fn a_descending_page_fails_without_yielding_any_of_its_bars() {
    let server = MockServer::start().await;

    mount_page(
        &server,
        "2024-01-03 00:00:00",
        &[
            equity_row("2024-01-04 00:00:00.000000", "13.0"), // closes 01-05, past `end`
            equity_row("2024-01-03 00:00:00.000000", "12.0"), // closes 01-04, in range
        ],
    )
    .await;

    let client = client(&server);
    let stream = client.fetch_candles(
        "AAPL",
        CandleInterval::Day1,
        utc("2024-01-04T00:00:00Z"),
        utc("2024-01-04T00:00:00Z"),
    );
    futures::pin_mut!(stream);

    let error = stream
        .next()
        .await
        .expect("the ordering violation must terminate the stream")
        .unwrap_err();
    assert!(
        matches!(error, LseError::NonMonotonicCandlePage { .. }),
        "expected the ordering violation, got {error:?}"
    );

    assert!(
        stream.next().await.is_none(),
        "the stream must end at the ordering violation"
    );
}

/// The per-row range trim keeps its own behaviour on a **well-formed** page: the bar past the upper
/// bound is not emitted, every in-range bar before it is, and the violation is terminal. This is the
/// half of the old out-of-order test that still describes a page the vault can legitimately serve.
#[tokio::test]
async fn an_ascending_page_yields_its_in_range_bars_before_the_range_violation() {
    let server = MockServer::start().await;

    mount_page(
        &server,
        "2024-01-03 00:00:00",
        &[
            equity_row("2024-01-03 00:00:00.000000", "12.0"), // closes 01-04, in range
            equity_row("2024-01-04 00:00:00.000000", "13.0"), // closes 01-05, past `end`
        ],
    )
    .await;

    let client = client(&server);
    let stream = client.fetch_candles(
        "AAPL",
        CandleInterval::Day1,
        utc("2024-01-04T00:00:00Z"),
        utc("2024-01-04T00:00:00Z"),
    );
    futures::pin_mut!(stream);

    let candle = stream
        .next()
        .await
        .expect("the in-range bar must be yielded before the range violation")
        .expect("the in-range bar must arrive as Ok, not as the error");
    assert_eq!(candle.close_time, utc("2024-01-04T00:00:00Z"));
    assert_eq!(candle.close, dec!(12.0));

    let error = stream
        .next()
        .await
        .expect("the range violation must terminate the stream")
        .unwrap_err();
    assert!(
        matches!(error, LseError::UnexpectedCandleRange { .. }),
        "expected the range violation, got {error:?}"
    );

    assert!(
        stream.next().await.is_none(),
        "the stream must end at the range violation"
    );
}

#[tokio::test]
async fn a_full_page_at_the_row_cap_seams_onto_the_next_without_gap_or_duplicate() {
    let server = MockServer::start().await;

    // The row cap is applied silently, so the page boundary it creates is the one seam pagination
    // never sees coming. Every other test here pages at one or two rows; this one pages at the real
    // 5,000-row cap, where an off-by-one in the cursor step drops or repeats exactly one bar --
    // invisible at small page sizes, and a whole minute of data at this one.
    const CAP: i64 = 5000;
    let first_open = utc("2024-01-01T00:00:00Z");
    let minute_row =
        |open: DateTime<Utc>| equity_row(&open.format("%Y-%m-%d %H:%M:%S%.6f").to_string(), "10.0");

    let full_page: Vec<String> = (0..CAP)
        .map(|index| minute_row(first_open + TimeDelta::minutes(index)))
        .collect();
    mount_page(&server, "2024-01-01 00:00:00", &full_page).await;

    // The cursor resumes one second past the last bar's OPEN, so page two must begin at the very
    // next minute. Anything else shows up in the sequence check below as a gap or a repeat.
    let last_open_page_one = first_open + TimeDelta::minutes(CAP - 1);
    let next_open = last_open_page_one + TimeDelta::minutes(1);
    mount_page(
        &server,
        &(last_open_page_one + TimeDelta::seconds(1))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
        &[minute_row(next_open)],
    )
    .await;

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Min1,
            first_open + TimeDelta::minutes(1), // close of the first bar
            next_open + TimeDelta::minutes(1),  // close of the last bar
        )
        .await
        .unwrap();

    // No third request: the cursor lands exactly on the exclusive upper bound after page two.
    assert_eq!(candles.len(), CAP as usize + 1);
    for (index, candle) in candles.iter().enumerate() {
        assert_eq!(
            candle.close_time,
            first_open + TimeDelta::minutes(index as i64 + 1),
            "bar {index} breaks the one-minute sequence across the cap boundary"
        );
    }
}

// -----------------------------------------------------------------------------------------------
// Credential containment
// -----------------------------------------------------------------------------------------------

/// The client attaches `x-api-key` as a client-wide default header, and reqwest's cross-host
/// stripping covers `Authorization`, `Cookie`, `Cookie2`, `Proxy-Authorization` and
/// `WWW-Authenticate` — a custom header is **not** on that list. Under reqwest's default `Policy::limited(10)` a single
/// vault-issued 302 would therefore re-issue the request, live key attached, against whatever host
/// the vault named. `Policy::none()` makes the "only ever requests URLs it builds itself" invariant
/// structural: the 3xx is surfaced unfollowed, and — the load-bearing assertion — the redirect
/// target receives no request at all, so the key cannot leak even if reqwest's stripping changes.
#[tokio::test]
async fn a_candle_fetch_does_not_follow_a_redirect_off_host() {
    // Catch-all: it would answer, and receive the key, if the client ever followed the redirect.
    let attacker = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .mount(&attacker)
        .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "location",
            format!("{}/vault/candles", attacker.uri()).as_str(),
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-03T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::Api { status, .. } if (300..400).contains(&status)),
        "expected an unfollowed 3xx as LseError::Api, got {error:?}"
    );
    assert!(
        attacker.received_requests().await.unwrap().is_empty(),
        "the client must not follow a redirect off-host: the x-api-key would ride along"
    );
}

// -----------------------------------------------------------------------------------------------
// Range and ordering contracts the vault supplies but does not guarantee
// -----------------------------------------------------------------------------------------------

/// The vault's `end` is **exclusive on open time**, so the client extends it one second past the
/// final bar's open to readmit that bar. Nothing else in this suite matched on `end`, which meant
/// deleting that compensation left every pagination test green while every live fetch silently
/// returned one bar fewer at every resolution — and the shape canary could not see it either, since
/// a missing *trailing* bar changes neither spacing nor ordering.
///
/// This mock matches on `end` explicitly, so the compensation is now pinned by a failing test
/// rather than by the comment next to it.
#[tokio::test]
async fn the_exclusive_upper_bound_is_extended_past_the_final_bars_open() {
    let server = MockServer::start().await;

    // Requesting bars closing in [01-02, 01-04] means opens in [01-01, 01-03]. `end` is exclusive
    // on open, so it must be sent as 01-03 00:00:01 — one second past the last bar's open — or the
    // vault drops the bar that closes exactly on the requested end.
    Mock::given(method("GET"))
        .and(path("/vault/candles"))
        .and(query_param("start", "2024-01-01 00:00:00"))
        .and(query_param("end", "2024-01-03 00:00:01"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "[{}]",
            [
                equity_row("2024-01-01 00:00:00.000000", "10.0"),
                equity_row("2024-01-02 00:00:00.000000", "11.0"),
                equity_row("2024-01-03 00:00:00.000000", "12.0"),
            ]
            .join(",")
        )))
        .expect(1)
        .mount(&server)
        .await;
    // The cursor lands on the exclusive bound after page one, so the fetch stops without a second
    // request. Any request that does not carry the expected `end` matches nothing and 404s.

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap();

    assert_eq!(
        candles.len(),
        3,
        "the bar closing on `end` must be included"
    );
    assert_eq!(candles[2].close_time, utc("2024-01-04T00:00:00Z"));
}

/// Ascending order is how the vault answers today, not something it promises, and the cursor
/// arithmetic depends on it: the cursor advances past the newest open a page carried, so a page
/// served newest-first would jump it to the end of the range, make page two empty — the documented
/// end-of-data signal — and return `Ok` holding the tail of the series with nothing to say the rest
/// was skipped. A typed error is the only outcome that a caller can act on.
#[tokio::test]
async fn a_page_that_steps_backwards_in_open_time_is_a_terminal_error() {
    let server = MockServer::start().await;
    mount_page(
        &server,
        "2024-01-01 00:00:00",
        &[
            equity_row("2024-01-03 00:00:00.000000", "12.0"),
            equity_row("2024-01-02 00:00:00.000000", "11.0"),
            equity_row("2024-01-01 00:00:00.000000", "10.0"),
        ],
    )
    .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::NonMonotonicCandlePage { .. }),
        "expected NonMonotonicCandlePage, got {error:?}"
    );
}

/// The failure the whole module is arranged around: the vault answers `200` to a misspelled
/// resolution parameter and silently serves 1-minute bars in a byte-identical shape. Until now
/// every mitigation was documentation plus an `#[ignore]`d weekly canary — a `Day1` request
/// receiving minute bars passes the range check (the bars are in range), passes the order check
/// (they ascend), and is stamped `close_time = open + 24h` by this module's own arithmetic.
///
/// Spacing is the one property that gives it away: consecutive bars of a compliant fixed-interval
/// series are exactly one interval apart, and a gap only ever widens that.
#[tokio::test]
async fn candles_spaced_closer_than_the_requested_interval_are_rejected() {
    let server = MockServer::start().await;
    // A `Day1` request answered with one-minute bars.
    mount_page(
        &server,
        "2024-01-01 00:00:00",
        &[
            equity_row("2024-01-01 00:00:00.000000", "10.0"),
            equity_row("2024-01-01 00:01:00.000000", "10.1"),
            equity_row("2024-01-01 00:02:00.000000", "10.2"),
        ],
    )
    .await;

    let error = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-02T00:00:00Z"),
            utc("2024-01-04T00:00:00Z"),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::UnexpectedCandleResolution { .. }),
        "expected UnexpectedCandleResolution, got {error:?}"
    );
}

/// The mirror of the case above, and the reason the check is `<` rather than `!=`: a **gap** is
/// ordinary. Weekends, holidays and quiet symbols all produce spacing wider than the interval, and
/// rejecting those would make the detector useless on every real equity series.
#[tokio::test]
async fn a_gap_wider_than_the_interval_is_accepted() {
    let server = MockServer::start().await;
    // Friday, then Monday: a three-day step in a daily series.
    mount_page(
        &server,
        "2024-01-05 00:00:00",
        &[
            equity_row("2024-01-05 00:00:00.000000", "10.0"),
            equity_row("2024-01-08 00:00:00.000000", "11.0"),
        ],
    )
    .await;
    mount_page(&server, "2024-01-08 00:00:01", &[]).await;

    let candles = client(&server)
        .collect_candles(
            "AAPL",
            CandleInterval::Day1,
            utc("2024-01-06T00:00:00Z"),
            utc("2024-01-10T00:00:00Z"),
        )
        .await
        .unwrap();

    assert_eq!(
        candles.len(),
        2,
        "a weekend gap is not a resolution failure"
    );
}
