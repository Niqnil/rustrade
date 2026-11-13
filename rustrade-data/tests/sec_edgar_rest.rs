//! SEC EDGAR client against a mocked SEC (`wiremock`, no network). Every filing here is synthetic.
//!
//! The tests pin what the client requests (paths, the declared User-Agent, which pages and
//! headers it reads) and how it turns a filing index into earnings releases.

#![cfg(feature = "sec-edgar")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{NaiveDate, TimeZone, Utc};
use rustrade_data::{
    earnings::{EarningsProvider, ReleaseStatus, ReleaseTiming},
    exchange::sec_edgar::{Cik, CompanyTicker, SecEdgarClient, SecEdgarError},
};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

const USER_AGENT: &str = "Test Suite tests@example.com";
const CIK: Cik = Cik::new(1234);

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

async fn client(server: &MockServer) -> SecEdgarClient {
    SecEdgarClient::new(USER_AGENT)
        .unwrap()
        .with_base_urls(&server.uri(), &server.uri())
        .unwrap()
}

/// One filing of a synthetic index: (accession, form, filing date, report date, items).
type Row<'a> = (&'a str, &'a str, &'a str, &'a str, &'a str);

fn columns(rows: &[Row<'_>]) -> Value {
    json!({
        "accessionNumber": rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        "form": rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        "filingDate": rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        "reportDate": rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        "items": rows.iter().map(|r| r.4).collect::<Vec<_>>(),
        "acceptanceDateTime": rows.iter().map(|r| format!("{}T12:00:00.000Z", r.2)).collect::<Vec<_>>(),
        "primaryDocument": rows.iter().map(|_| "doc.htm").collect::<Vec<_>>(),
    })
}

fn submissions(recent: &[Row<'_>], pages: Value) -> Value {
    json!({
        "cik": CIK.get().to_string(),
        "name": "Synthetic Corp",
        "tickers": ["XYZ"],
        "fiscalYearEnd": "1231",
        "filings": { "recent": columns(recent), "files": pages },
    })
}

async fn mount_submissions(server: &MockServer, body: Value) {
    Mock::given(method("GET"))
        .and(path(format!("/submissions/CIK{CIK}.json")))
        .and(header("user-agent", USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(server)
        .await;
}

fn header_path(accession: &str) -> String {
    format!(
        "/Archives/edgar/data/{}/{}/{accession}-index-headers.html",
        CIK.get(),
        accession.replace('-', "")
    )
}

/// Serve `accession`'s filing header with acceptance time `acceptance` (New York,
/// `YYYYMMDDHHMMSS`), expecting `times` requests for it.
async fn mount_header(server: &MockServer, accession: &str, acceptance: &str, times: u64) {
    let body = format!(
        "<html><body><pre>&lt;SEC-HEADER&gt;\n&lt;ACCEPTANCE-DATETIME&gt;{acceptance}\n\
         ACCESSION NUMBER:\t\t{accession}\nCONFORMED SUBMISSION TYPE:\t8-K\n</pre></body></html>"
    );
    Mock::given(method("GET"))
        .and(path(header_path(accession)))
        .and(header("user-agent", USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(times)
        .mount(server)
        .await;
}

/// A year of a synthetic issuer: quarterly figures furnished under Item 2.02 early in each
/// quarter, then the results release, then the report.
const YEAR: [Row<'static>; 9] = [
    (
        "0000001234-26-000009",
        "10-Q",
        "2026-07-24",
        "2026-06-30",
        "",
    ),
    (
        "0000001234-26-000008",
        "8-K",
        "2026-07-14",
        "2026-07-14",
        "2.02,9.01",
    ),
    (
        "0000001234-26-000007",
        "8-K",
        "2026-07-02",
        "2026-07-02",
        "2.02,9.01",
    ),
    (
        "0000001234-26-000006",
        "8-K",
        "2026-05-20",
        "2026-05-20",
        "5.07",
    ),
    (
        "0000001234-26-000005",
        "10-Q",
        "2026-04-24",
        "2026-03-31",
        "",
    ),
    (
        "0000001234-26-000004",
        "8-K",
        "2026-04-22",
        "2026-04-22",
        "2.02,9.01",
    ),
    (
        "0000001234-26-000003",
        "10-K",
        "2026-02-10",
        "2025-12-31",
        "",
    ),
    (
        "0000001234-26-000002",
        "8-K",
        "2026-01-28",
        "2026-01-28",
        "2.02,9.01",
    ),
    (
        "0000001234-26-000001",
        "8-K",
        "2026-01-05",
        "2026-01-02",
        "2.02,9.01",
    ),
];

#[tokio::test]
async fn each_period_gets_its_latest_results_8k_dated_by_its_header() {
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&YEAR, json!([]))).await;
    // After the close in winter (UTC-5), before the open in summer (UTC-4), during the session.
    mount_header(&server, "0000001234-26-000002", "20260128163005", 1).await;
    mount_header(&server, "0000001234-26-000004", "20260422120000", 1).await;
    mount_header(&server, "0000001234-26-000008", "20260714063038", 1).await;
    // The quarterly-figures filings are never paired, so their headers are never read.
    mount_header(&server, "0000001234-26-000001", "20260105090000", 0).await;
    mount_header(&server, "0000001234-26-000007", "20260702090000", 0).await;

    let before = Utc::now();
    let releases = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 1, 1), date(2026, 12, 31))
        .await
        .unwrap();
    let after = Utc::now();

    let summary: Vec<_> = releases
        .iter()
        .map(|r| (r.fiscal_period_end, r.date, r.timing, r.known_at))
        .collect();
    assert_eq!(
        summary,
        vec![
            (
                date(2025, 12, 31),
                date(2026, 1, 28),
                ReleaseTiming::AfterClose,
                Utc.with_ymd_and_hms(2026, 1, 28, 21, 30, 5).unwrap(),
            ),
            (
                date(2026, 3, 31),
                date(2026, 4, 22),
                ReleaseTiming::DuringSession,
                Utc.with_ymd_and_hms(2026, 4, 22, 16, 0, 0).unwrap(),
            ),
            (
                date(2026, 6, 30),
                date(2026, 7, 14),
                ReleaseTiming::BeforeOpen,
                Utc.with_ymd_and_hms(2026, 7, 14, 10, 30, 38).unwrap(),
            ),
        ]
    );
    for release in &releases {
        assert_eq!(release.symbol, "XYZ");
        assert_eq!(release.status, ReleaseStatus::Reported);
        assert_eq!(release.source.provider, EarningsProvider::SecEdgar);
        assert_eq!(release.source.updated_at, None);
        assert!(release.source.observed_at >= before && release.source.observed_at <= after);
    }
    assert_eq!(
        releases[2].source.record_id.as_deref(),
        Some("0000001234-26-000008")
    );
}

#[tokio::test]
async fn releases_outside_the_range_are_not_looked_up_or_are_dropped_by_their_date() {
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&YEAR, json!([]))).await;
    // Range: 2026-04-01 to 2026-07-13.
    // - January's release was filed before the range: never looked up.
    mount_header(&server, "0000001234-26-000002", "20260128163005", 0).await;
    // - April's is in the range.
    mount_header(&server, "0000001234-26-000004", "20260422120000", 1).await;
    // - July's 8-K was filed within the filing lag after the range ends, so its header is read,
    //   and its date, the day after the range, drops it.
    mount_header(&server, "0000001234-26-000008", "20260714063038", 1).await;

    let releases = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 4, 1), date(2026, 7, 13))
        .await
        .unwrap();
    let dates: Vec<_> = releases.iter().map(|r| r.date).collect();
    assert_eq!(dates, vec![date(2026, 4, 22)]);
}

#[tokio::test]
async fn an_evening_filing_dated_the_next_business_day_keeps_its_release_date() {
    // Accepted at 18:05 New York on a Friday, so EDGAR dates the filing Monday.
    let rows: [Row<'_>; 2] = [
        (
            "0000001234-26-000020",
            "10-Q",
            "2026-08-10",
            "2026-06-30",
            "",
        ),
        (
            "0000001234-26-000019",
            "8-K",
            "2026-07-20",
            "2026-07-17",
            "2.02,9.01",
        ),
    ];
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&rows, json!([]))).await;
    mount_header(&server, "0000001234-26-000019", "20260717180500", 1).await;

    let releases = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 7, 17), date(2026, 7, 17))
        .await
        .unwrap();
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].date, date(2026, 7, 17));
    assert_eq!(releases[0].timing, ReleaseTiming::AfterClose);
}

#[tokio::test]
async fn older_pages_are_read_only_when_the_range_reaches_them() {
    let page_name = format!("CIK{CIK}-submissions-001.json");
    let page_rows: [Row<'_>; 2] = [
        (
            "0000001234-20-000002",
            "10-Q",
            "2020-08-05",
            "2020-06-30",
            "",
        ),
        (
            "0000001234-20-000001",
            "8-K",
            "2020-07-21",
            "2020-07-21",
            "2.02,9.01",
        ),
    ];
    let pages = json!([{
        "name": page_name,
        "filingCount": 2,
        "filingFrom": "2020-07-21",
        "filingTo": "2020-08-05",
    }]);

    // A range after the page's last filing date: the page is not requested.
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&YEAR, pages.clone())).await;
    Mock::given(method("GET"))
        .and(path(format!("/submissions/{page_name}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(columns(&page_rows)))
        .expect(0)
        .mount(&server)
        .await;
    mount_header(&server, "0000001234-26-000008", "20260714063038", 1).await;
    let releases = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 7, 1), date(2026, 7, 31))
        .await
        .unwrap();
    assert_eq!(releases.len(), 1);
    drop(server);

    // A range reaching back into the page: it is read, and its release paired.
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&YEAR, pages)).await;
    Mock::given(method("GET"))
        .and(path(format!("/submissions/{page_name}")))
        .and(header("user-agent", USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_json(columns(&page_rows)))
        .expect(1)
        .mount(&server)
        .await;
    mount_header(&server, "0000001234-20-000001", "20200721161000", 1).await;
    let releases = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2020, 7, 1), date(2020, 7, 31))
        .await
        .unwrap();
    let pairs: Vec<_> = releases
        .iter()
        .map(|r| (r.fiscal_period_end, r.date))
        .collect();
    assert_eq!(pairs, vec![(date(2020, 6, 30), date(2020, 7, 21))]);
}

#[tokio::test]
async fn a_foreign_private_issuer_is_refused() {
    let rows: [Row<'_>; 2] = [
        ("0000001234-26-000002", "6-K", "2026-05-01", "", ""),
        (
            "0000001234-26-000001",
            "20-F",
            "2026-04-01",
            "2025-12-31",
            "",
        ),
    ];
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&rows, json!([]))).await;
    let error = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 1, 1), date(2026, 12, 31))
        .await
        .unwrap_err();
    assert_eq!(error, SecEdgarError::ForeignPrivateIssuer { cik: CIK });
}

#[tokio::test]
async fn an_inverted_range_is_refused_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let error = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 2, 1), date(2026, 1, 31))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SecEdgarError::InvalidInput { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn error_statuses_and_bad_bodies_keep_their_meaning() {
    let refusal_page = "<html><title>SEC.gov | Request Rate Threshold Exceeded</title></html>";
    type IsExpected = fn(&SecEdgarError) -> bool;
    let cases: [(ResponseTemplate, IsExpected); 5] = [
        (
            ResponseTemplate::new(403).set_body_string(refusal_page),
            |e| matches!(e, SecEdgarError::Refused { message } if message.contains("Threshold")),
        ),
        (ResponseTemplate::new(429), |e| {
            matches!(e, SecEdgarError::Throttled { .. }) && e.is_transient()
        }),
        (
            ResponseTemplate::new(404),
            |e| matches!(e, SecEdgarError::NotFound { url } if url.ends_with("CIK0000001234.json")),
        ),
        (ResponseTemplate::new(503), |e| {
            matches!(e, SecEdgarError::Api { status: 503, .. }) && e.is_transient()
        }),
        (
            ResponseTemplate::new(200).set_body_string("{\"filings\": 1}"),
            |e| matches!(e, SecEdgarError::Decode { .. }),
        ),
    ];
    for (response, expected) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/submissions/CIK{CIK}.json")))
            .respond_with(response)
            .mount(&server)
            .await;
        let error = client(&server)
            .await
            .earnings_releases(CIK, "XYZ", date(2026, 1, 1), date(2026, 12, 31))
            .await
            .unwrap_err();
        assert!(expected(&error), "{error:?}");
    }
}

#[tokio::test]
async fn a_header_without_an_acceptance_time_is_a_decode_error() {
    let server = MockServer::start().await;
    mount_submissions(&server, submissions(&YEAR, json!([]))).await;
    Mock::given(method("GET"))
        .and(path(header_path("0000001234-26-000008")))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>no header</html>"))
        .mount(&server)
        .await;
    let error = client(&server)
        .await
        .earnings_releases(CIK, "XYZ", date(2026, 7, 1), date(2026, 7, 31))
        .await
        .unwrap_err();
    assert!(matches!(error, SecEdgarError::Decode { .. }), "{error:?}");
}

#[tokio::test]
async fn company_tickers_come_back_in_secs_order() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/company_tickers.json"))
        .and(header("user-agent", USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "10": {"cik_str": 3, "ticker": "CCC", "title": "Third Co"},
            "0": {"cik_str": 1, "ticker": "AAA", "title": "First Co"},
            "2": {"cik_str": 1, "ticker": "AAB", "title": "First Co"},
        })))
        .expect(1)
        .mount(&server)
        .await;
    let tickers = client(&server).await.company_tickers().await.unwrap();
    let ticker = |cik, ticker: &str, title: &str| CompanyTicker {
        cik: Cik::new(cik),
        ticker: ticker.into(),
        title: title.into(),
    };
    assert_eq!(
        tickers,
        vec![
            ticker(1, "AAA", "First Co"),
            ticker(1, "AAB", "First Co"),
            ticker(3, "CCC", "Third Co"),
        ]
    );
}
