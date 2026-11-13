//! Alpha Vantage client against a mocked Alpha Vantage (`wiremock`, no network). Every response
//! here is synthetic.
//!
//! The tests pin what the client requests (path, function, symbol, horizon, the API key) and how
//! it reads data, notices and error statuses.

#![cfg(feature = "alpha-vantage")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::NaiveDate;
use rustrade_data::{
    earnings::{EarningsProvider, ReleaseStatus, ReleaseTiming},
    exchange::alpha_vantage::{AlphaVantageClient, AlphaVantageError, CalendarHorizon},
};
use std::num::NonZeroU32;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param, query_param_is_missing},
};

const KEY: &str = "TESTKEY123456789";

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

/// A client for `server`, paced fast enough that tests do not wait.
fn client(server: &MockServer) -> AlphaVantageClient {
    AlphaVantageClient::new(KEY)
        .unwrap()
        .with_base_url(&server.uri())
        .unwrap()
        .with_requests_per_minute(NonZeroU32::new(60_000).unwrap())
}

const HISTORY: &str = r#"{
    "symbol": "XYZ",
    "annualEarnings": [{"fiscalDateEnding": "2025-12-31", "reportedEPS": "4.00"}],
    "quarterlyEarnings": [
        {"fiscalDateEnding": "2025-12-31", "reportedDate": "2026-01-28", "reportedEPS": "1.10",
         "estimatedEPS": "1.00", "surprise": "0.10", "surprisePercentage": "10",
         "reportTime": "post-market"},
        {"fiscalDateEnding": "2025-09-30", "reportedDate": "2025-10-22", "reportedEPS": "1.00",
         "estimatedEPS": "1.00", "surprise": "0", "surprisePercentage": "0",
         "reportTime": "pre-market"}
    ]
}"#;

const CALENDAR: &str = "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\r\n\
XYZ,\"Xyz, Inc.\",2026-04-22,2026-03-31,1.20,USD,pre-market\r\n";

#[tokio::test]
async fn history_sends_the_function_symbol_and_key_and_reads_each_quarter() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "EARNINGS"))
        .and(query_param("symbol", "XYZ"))
        .and(query_param("apikey", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(HISTORY))
        .expect(1)
        .mount(&server)
        .await;

    let releases = client(&server).earnings_history(" XYZ ").await.unwrap();
    let rows: Vec<_> = releases
        .iter()
        .map(|r| (r.fiscal_period_end, r.date, r.timing, r.status))
        .collect();
    assert_eq!(
        rows,
        vec![
            (
                date(2025, 9, 30),
                date(2025, 10, 22),
                ReleaseTiming::BeforeOpen,
                ReleaseStatus::Reported
            ),
            (
                date(2025, 12, 31),
                date(2026, 1, 28),
                ReleaseTiming::AfterClose,
                ReleaseStatus::Reported
            ),
        ]
    );
    assert!(releases.iter().all(|r| r.symbol == "XYZ"
        && r.source.provider == EarningsProvider::AlphaVantage
        && r.known_at < r.source.observed_at));
}

#[tokio::test]
async fn the_calendar_sends_the_horizon_and_an_optional_symbol() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "EARNINGS_CALENDAR"))
        .and(query_param("horizon", "12month"))
        .and(query_param("symbol", "XYZ"))
        .and(query_param("apikey", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(CALENDAR))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "EARNINGS_CALENDAR"))
        .and(query_param("horizon", "3month"))
        .and(query_param_is_missing("symbol"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CALENDAR))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    let one = client
        .earnings_calendar(Some("XYZ"), CalendarHorizon::TwelveMonths)
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(
        (
            one[0].date,
            one[0].fiscal_period_end,
            one[0].timing,
            one[0].status
        ),
        (
            date(2026, 4, 22),
            date(2026, 3, 31),
            ReleaseTiming::BeforeOpen,
            ReleaseStatus::Unknown
        )
    );
    assert_eq!(one[0].known_at, one[0].source.observed_at);
    let all = client
        .earnings_calendar(None, CalendarHorizon::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!((&all[0].symbol, all[0].date), (&one[0].symbol, one[0].date));
}

#[tokio::test]
async fn notices_and_error_statuses_keep_their_meaning_without_the_key() {
    let server = MockServer::start().await;
    let respond = |symbol: &str, response: ResponseTemplate| {
        Mock::given(method("GET"))
            .and(path("/query"))
            .and(query_param("function", "EARNINGS"))
            .and(query_param("symbol", symbol))
            .respond_with(response)
    };
    respond(
        "LIMIT",
        ResponseTemplate::new(200).set_body_string(
            r#"{"Information": "Thank you for using Alpha Vantage! 25 requests per day."}"#,
        ),
    )
    .mount(&server)
    .await;
    respond(
        "BADKEY",
        ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"Error Message": "the parameter apikey={KEY} is invalid"}}"#
        )),
    )
    .mount(&server)
    .await;
    respond("NOPE", ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    respond(
        "DOWN",
        ResponseTemplate::new(503).set_body_string("unavailable"),
    )
    .mount(&server)
    .await;

    let client = client(&server);
    let limit = client.earnings_history("LIMIT").await.unwrap_err();
    assert!(
        matches!(&limit, AlphaVantageError::Refused { message } if message.contains("25 requests per day")),
        "{limit:?}"
    );
    let bad_key = client.earnings_history("BADKEY").await.unwrap_err();
    assert!(
        matches!(&bad_key, AlphaVantageError::Api { status: 200, message }
            if message.contains("apikey=<api key> is invalid")),
        "{bad_key:?}"
    );
    assert!(!bad_key.to_string().contains(KEY));
    assert_eq!(
        client.earnings_history("NOPE").await.unwrap_err(),
        AlphaVantageError::NotFound {
            symbol: "NOPE".into()
        }
    );
    let down = client.earnings_history("DOWN").await.unwrap_err();
    assert_eq!(
        down,
        AlphaVantageError::Api {
            status: 503,
            message: "unavailable".into()
        }
    );
    assert!(down.is_transient());
}

#[tokio::test]
async fn a_calendar_refusal_cut_into_the_csv_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "EARNINGS_CALENDAR"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\r\nI,n,f,o,r,m,a\r\n",
        ))
        .mount(&server)
        .await;
    let error = client(&server)
        .earnings_calendar(None, CalendarHorizon::ThreeMonths)
        .await
        .unwrap_err();
    assert!(
        matches!(error, AlphaVantageError::Refused { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_transport_error_does_not_carry_the_key() {
    // Nothing listens on port 1.
    let client = AlphaVantageClient::new(KEY)
        .unwrap()
        .with_base_url("http://127.0.0.1:1")
        .unwrap();
    let error = client.earnings_history("XYZ").await.unwrap_err();
    assert!(matches!(error, AlphaVantageError::Http { .. }), "{error:?}");
    assert!(error.is_transient());
    let text = format!("{error} {error:?}");
    assert!(!text.contains(KEY), "{text}");
}

#[tokio::test]
async fn an_empty_symbol_is_refused_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let client = client(&server);
    assert!(matches!(
        client.earnings_history(" ").await,
        Err(AlphaVantageError::InvalidInput { .. })
    ));
    assert!(matches!(
        client
            .earnings_calendar(Some(""), CalendarHorizon::ThreeMonths)
            .await,
        Err(AlphaVantageError::InvalidInput { .. })
    ));
}
