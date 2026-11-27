//! Alpha Vantage client against a mocked Alpha Vantage (`wiremock`, no network). Every response
//! here is synthetic.
//!
//! The tests pin what the client requests (path, function, symbol, horizon, the API key) and how
//! it reads data, notices and error statuses, for earnings, dividends and splits.

#![cfg(feature = "alpha-vantage")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::NaiveDate;
use futures::StreamExt;
use rust_decimal_macros::dec;
use rustrade_data::{
    dividends::DividendProvider,
    earnings::{EarningsProvider, ReleaseStatus, ReleaseTiming},
    exchange::alpha_vantage::{AlphaVantageClient, AlphaVantageError, CalendarHorizon},
};
use rustrade_instrument::corporate_action::CorporateActionKind;
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};
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

/// A redirect is not followed: a followed one would carry a `Referer` holding the request's URL,
/// API key included, to the host it names. The 3xx is reported as `Api` and is not transient.
#[tokio::test]
async fn a_redirect_is_reported_and_not_followed() {
    let target = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&target)
        .await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("location", format!("{}/query", target.uri())),
        )
        .mount(&server)
        .await;

    let error = client(&server).earnings_history("XYZ").await.unwrap_err();

    assert!(
        matches!(&error, AlphaVantageError::Api { status: 302, .. }),
        "{error:?}"
    );
    assert!(!error.is_transient());
    assert!(target.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_key_cut_by_the_message_cap_leaves_no_part_behind() {
    // The key starts 505 bytes in, so a cap at 512 bytes would cut it.
    let text = format!("{}{KEY} tail", "x".repeat(505));
    let server = MockServer::start().await;
    let respond = |symbol: &str, response: ResponseTemplate| {
        Mock::given(method("GET"))
            .and(path("/query"))
            .and(query_param("symbol", symbol))
            .respond_with(response)
    };
    respond(
        "NOTICE",
        ResponseTemplate::new(200).set_body_string(format!(r#"{{"Information": "{text}"}}"#)),
    )
    .mount(&server)
    .await;
    respond("STATUS", ResponseTemplate::new(500).set_body_string(text))
        .mount(&server)
        .await;
    let client = client(&server);
    for symbol in ["NOTICE", "STATUS"] {
        let error = client.earnings_history(symbol).await.unwrap_err();
        let shown = format!("{error} {error:?}");
        assert!(!shown.contains(&KEY[..7]), "{symbol}: {shown}");
        assert!(shown.contains("<api ke"), "{symbol}: {shown}");
    }
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
    assert!(matches!(
        client.dividends("").await,
        Err(AlphaVantageError::InvalidInput { .. })
    ));
}

const DIVIDENDS: &str = r#"{"symbol": "XYZ", "data": [
    {"ex_dividend_date": "2026-02-05", "declaration_date": "2026-01-28",
     "record_date": "2026-02-05", "payment_date": "2026-02-12", "amount": "0.26"},
    {"ex_dividend_date": "2025-12-23", "declaration_date": "2025-12-19",
     "record_date": "2025-12-23", "payment_date": "2025-12-30", "amount": "0.0"},
    {"ex_dividend_date": "2015-02-05", "declaration_date": "None",
     "record_date": "None", "payment_date": "None", "amount": "0.47"}
]}"#;

#[tokio::test]
async fn dividends_send_the_function_and_symbol_and_read_each_row() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "DIVIDENDS"))
        .and(query_param("symbol", "XYZ"))
        .and(query_param("apikey", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(DIVIDENDS))
        .expect(1)
        .mount(&server)
        .await;

    let dividends = client(&server).dividends(" XYZ ").await.unwrap();
    let rows: Vec<_> = dividends
        .iter()
        .map(|d| (d.ex_date, d.amount, d.declaration_date, d.payment_date))
        .collect();
    // Oldest first, the zero row skipped.
    assert_eq!(
        rows,
        vec![
            (date(2015, 2, 5), dec!(0.47), None, None),
            (
                date(2026, 2, 5),
                dec!(0.26),
                Some(date(2026, 1, 28)),
                Some(date(2026, 2, 12))
            ),
        ]
    );
    assert!(dividends.iter().all(|d| d.symbol == "XYZ"
        && d.source.provider == DividendProvider::AlphaVantage
        && d.known_at <= d.source.observed_at));
}

#[tokio::test]
async fn dividend_and_split_reads_turn_a_notice_into_its_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("symbol", "LIMIT"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"Information": "25 requests per day."}"#),
        )
        .expect(2)
        .mount(&server)
        .await;
    let client = client(&server);
    let error = client.dividends("LIMIT").await.unwrap_err();
    assert!(
        matches!(&error, AlphaVantageError::Refused { message } if message.contains("25 requests")),
        "{error:?}"
    );
    let filter = CorporateActionFilter::new(vec!["LIMIT".into()], None, None);
    let items: Vec<_> = client.fetch_splits(&filter).collect().await;
    assert!(
        matches!(items.as_slice(), [Err(AlphaVantageError::Refused { .. })]),
        "{items:?}"
    );
}

#[tokio::test]
async fn splits_fan_out_per_symbol_and_keep_the_filters_dates() {
    let server = MockServer::start().await;
    let respond = |symbol: &str, body: &str| {
        Mock::given(method("GET"))
            .and(path("/query"))
            .and(query_param("function", "SPLITS"))
            .and(query_param("symbol", symbol))
            .and(query_param("apikey", KEY))
            .respond_with(ResponseTemplate::new(200).set_body_string(body.to_owned()))
            .expect(1)
    };
    respond(
        "AAA",
        r#"{"symbol": "AAA", "data": [
            {"effective_date": "2024-06-10", "split_factor": "10.0000"},
            {"effective_date": "2021-07-20", "split_factor": "4.0000"},
            {"effective_date": "2000-06-27", "split_factor": "2.0000"}
        ]}"#,
    )
    .mount(&server)
    .await;
    respond(
        "BBB",
        r#"{"symbol": "BBB", "data": [{"effective_date": "2022-01-03", "split_factor": "0.3333"}]}"#,
    )
    .mount(&server)
    .await;

    let filter = CorporateActionFilter::new(
        vec!["AAA".into(), "BBB".into()],
        Some(date(2021, 7, 20)),
        Some(date(2024, 6, 9)),
    );
    let actions: Vec<_> = client(&server)
        .fetch_splits(&filter)
        .map(Result::unwrap)
        .collect()
        .await;
    let rows: Vec<_> = actions
        .iter()
        .map(|a| (a.instrument.as_str(), a.effective_date, a.kind.clone()))
        .collect();
    // The bounds are inclusive: 2021-07-20 is kept, 2024-06-10 is after the end.
    assert_eq!(
        rows,
        vec![
            (
                "AAA",
                Some(date(2021, 7, 20)),
                CorporateActionKind::stock_split(dec!(4), dec!(1)).unwrap()
            ),
            (
                "BBB",
                Some(date(2022, 1, 3)),
                CorporateActionKind::stock_split(dec!(1), dec!(3)).unwrap()
            ),
        ]
    );
}

#[tokio::test]
async fn splits_with_no_bounds_return_every_split_and_an_empty_filter_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/query"))
        .and(query_param("function", "SPLITS"))
        .and(query_param("symbol", "AAA"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol": "AAA", "data": [
                {"effective_date": "2024-06-10", "split_factor": "10.0000"},
                {"effective_date": "2000-06-27", "split_factor": "2.0000"}
            ]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server);
    let all: Vec<_> = client
        // Named twice (once with spaces): one request, each split once.
        .fetch_splits(&CorporateActionFilter::new(
            vec!["AAA".into(), " AAA ".into()],
            None,
            None,
        ))
        .map(|item| item.unwrap().effective_date)
        .collect()
        .await;
    assert_eq!(all, [Some(date(2000, 6, 27)), Some(date(2024, 6, 10))]);

    for filter in [
        CorporateActionFilter::default(),
        CorporateActionFilter::new(vec![" ".into(), "AAA".into()], None, None),
    ] {
        let items: Vec<_> = client.fetch_splits(&filter).collect().await;
        assert!(
            matches!(
                items.as_slice(),
                [Err(AlphaVantageError::InvalidInput { .. })]
            ),
            "{filter:?}: {items:?}"
        );
    }
}
