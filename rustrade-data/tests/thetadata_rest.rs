//! ThetaData REST client against a mocked Theta Terminal (`wiremock`, no Terminal, no account).
//!
//! Every row here is synthetic: ThetaData's data may not be redistributed, so none of it is
//! committed. The tests pin what each method sends (path and query) and how it decodes the rows,
//! splits ranges, maps errors and limits concurrency.

#![cfg(feature = "thetadata")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use futures_util::{Stream, StreamExt};
use rust_decimal_macros::dec;
use rustrade_data::{
    books::Level,
    exchange::thetadata::{
        Expiration, InterestRateSeries, Interval, Listing, MarketDayKind, OptionContract,
        OptionSelection, Strike, ThetaDataClient, ThetaDataError, TimeWindow,
    },
    subscription::book::OrderBookL1,
};
use rustrade_instrument::instrument::kind::option::OptionKind;
use std::{num::NonZeroUsize, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param, query_param_is_missing},
};

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn ndjson(lines: &[&str]) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-ndjson")
        .set_body_string(lines.join("\n"))
}

async fn client(server: &MockServer) -> ThetaDataClient {
    ThetaDataClient::new()
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .unwrap()
}

async fn collect<T>(stream: impl Stream<Item = Result<T, ThetaDataError>>) -> Vec<T> {
    stream.map(|row| row.unwrap()).collect().await
}

async fn first_error<T: std::fmt::Debug>(
    stream: impl Stream<Item = Result<T, ThetaDataError>>,
) -> ThetaDataError {
    let results: Vec<_> = stream.collect().await;
    results
        .into_iter()
        .find_map(Result::err)
        .expect("the stream should end in an error")
}

const CONTRACT: &str =
    r#""symbol":"XYZ","expiration":"2024-03-15","strike":101.500,"right":"CALL""#;
const NBBO: &str = r#""bid":1.10,"bid_size":12,"bid_exchange":4,"bid_condition":50,"ask":1.25,"ask_size":7,"ask_exchange":9,"ask_condition":50"#;

fn xyz_call() -> OptionContract {
    OptionContract {
        root: "XYZ".into(),
        expiration: date(2024, 3, 15),
        strike: dec!(101.5),
        kind: OptionKind::Call,
    }
}

#[tokio::test]
async fn option_quote_history_sends_the_contract_and_decodes_rows() {
    let server = MockServer::start().await;
    let first = format!(r#"{{{CONTRACT},{NBBO},"timestamp":"2024-03-01T09:31:00"}}"#);
    let second = format!(r#"{{{CONTRACT},{NBBO},"timestamp":"2024-03-01T09:32:00.5"}}"#);
    Mock::given(method("GET"))
        .and(path("/v3/option/history/quote"))
        .and(query_param("symbol", "XYZ"))
        .and(query_param("expiration", "20240315"))
        .and(query_param("strike", "101.5"))
        .and(query_param("right", "call"))
        .and(query_param("interval", "1m"))
        .and(query_param("start_time", "09:30:00.000"))
        .and(query_param("end_time", "10:00:00.000"))
        .and(query_param("start_date", "20240301"))
        .and(query_param("end_date", "20240301"))
        .and(query_param("format", "ndjson"))
        // A blank line, a CRLF ending and no final newline are all tolerated.
        .respond_with(ndjson(&[&format!("{first}\r"), "", &second]))
        .expect(1)
        .mount(&server)
        .await;

    let window = TimeWindow {
        start: NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
        end: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
    };
    let quotes = collect(client(&server).await.option_quote_history(
        &OptionSelection::contract(&xyz_call()),
        date(2024, 3, 1),
        date(2024, 3, 1),
        Interval::M1,
        Some(window),
    ))
    .await;

    assert_eq!(quotes.len(), 2);
    assert_eq!(quotes[0].contract, xyz_call());
    // 09:31 New York on 2024-03-01 is EST, UTC-5.
    assert_eq!(
        quotes[0].time,
        Utc.with_ymd_and_hms(2024, 3, 1, 14, 31, 0).unwrap()
    );
    assert_eq!(quotes[1].time.timestamp_subsec_millis(), 500);
    assert_eq!(quotes[0].nbbo.bid, dec!(1.10));
    assert_eq!(quotes[0].nbbo.ask_size, 7);
    let book = OrderBookL1::from(&quotes[0]);
    assert_eq!(book.best_bid, Some(Level::new(dec!(1.10), dec!(12))));
    assert_eq!(book.best_ask, Some(Level::new(dec!(1.25), dec!(7))));
}

#[tokio::test]
async fn a_long_range_is_split_and_streamed_in_order() {
    let server = MockServer::start().await;
    let spans = [
        ("20240101", "20240128", "2024-01-02T09:30:00"),
        ("20240129", "20240225", "2024-01-29T09:30:00"),
        ("20240226", "20240301", "2024-02-26T09:30:00"),
    ];
    for (start, end, timestamp) in spans {
        Mock::given(path("/v3/stock/history/quote"))
            .and(query_param("start_date", start))
            .and(query_param("end_date", end))
            .and(query_param("interval", "5m"))
            .and(query_param_is_missing("start_time"))
            .respond_with(ndjson(&[&format!(
                r#"{{{NBBO},"timestamp":"{timestamp}"}}"#
            )]))
            .expect(1)
            .mount(&server)
            .await;
    }

    let quotes = collect(client(&server).await.stock_quote_history(
        "XYZ",
        date(2024, 1, 1),
        date(2024, 3, 1),
        Interval::M5,
        None,
    ))
    .await;

    let days: Vec<_> = quotes.iter().map(|q| q.time.date_naive()).collect();
    assert_eq!(
        days,
        vec![date(2024, 1, 2), date(2024, 1, 29), date(2024, 2, 26)]
    );
}

#[tokio::test]
async fn sub_minute_intervals_and_whole_chains_go_one_day_per_request() {
    let server = MockServer::start().await;
    for day in ["20240304", "20240305"] {
        Mock::given(path("/v3/option/history/quote"))
            .and(query_param("expiration", "*"))
            .and(query_param("start_date", day))
            .and(query_param("end_date", day))
            .respond_with(ndjson(&[]))
            .expect(1)
            .mount(&server)
            .await;
    }
    let client = client(&server).await;
    // A whole chain at one minute: one day per request.
    let quotes = collect(client.option_quote_history(
        &OptionSelection::new("XYZ", Expiration::All),
        date(2024, 3, 4),
        date(2024, 3, 5),
        Interval::M1,
        None,
    ))
    .await;
    assert!(quotes.is_empty());

    for day in ["20240304", "20240305"] {
        Mock::given(path("/v3/index/history/price"))
            .and(query_param("interval", "1s"))
            .and(query_param("start_date", day))
            .and(query_param("end_date", day))
            .respond_with(ndjson(&[
                r#"{"timestamp":"2024-03-04T09:30:01","price":5100.25}"#,
            ]))
            .expect(1)
            .mount(&server)
            .await;
    }
    let prices = collect(client.index_price_history(
        "XYZ",
        date(2024, 3, 4),
        date(2024, 3, 5),
        Interval::S1,
        None,
    ))
    .await;
    assert_eq!(prices.len(), 2);
    assert_eq!(prices[0].price, dec!(5100.25));
}

#[tokio::test]
async fn no_data_is_an_empty_result() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/stock/history/eod"))
        .respond_with(ResponseTemplate::new(472).set_body_string("No data for the request"))
        .mount(&server)
        .await;
    let rows = collect(client(&server).await.stock_end_of_day_history(
        "XYZ",
        date(2024, 1, 1),
        date(2024, 1, 5),
    ))
    .await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn error_statuses_keep_their_meaning_and_body() {
    let server = MockServer::start().await;
    type IsExpected = fn(&ThetaDataError) -> bool;
    let cases: [(u16, IsExpected); 4] = [
        (403, |e| {
            matches!(e, ThetaDataError::PermissionDenied { status: 403, .. })
        }),
        (471, |e| {
            matches!(e, ThetaDataError::PermissionDenied { status: 471, .. })
        }),
        (478, |e| matches!(e, ThetaDataError::SessionInvalid { .. })),
        (429, |e| matches!(e, ThetaDataError::Throttled { .. })),
    ];
    let client = client(&server).await;
    for (status, is_expected) in cases {
        server.reset().await;
        Mock::given(path("/v3/option/list/symbols"))
            .respond_with(ResponseTemplate::new(status).set_body_string("terminal says no"))
            .mount(&server)
            .await;
        let error = first_error(client.option_roots()).await;
        assert!(is_expected(&error), "{status}: {error:?}");
        assert!(error.to_string().contains("terminal says no"), "{error}");
    }
}

#[tokio::test]
async fn an_error_midway_through_a_range_ends_the_stream_after_the_rows_before_it() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/interest_rate/history/eod"))
        .and(query_param("symbol", "treasury_y10"))
        .and(query_param("start_date", "20230101"))
        .respond_with(ndjson(&[r#"{"created":"2023-01-03","rate":3.79}"#]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/interest_rate/history/eod"))
        .and(query_param("start_date", "20240101"))
        .respond_with(ResponseTemplate::new(474).set_body_string("lost MDDS"))
        .mount(&server)
        .await;
    let results: Vec<_> = client(&server)
        .await
        .interest_rate_history(
            InterestRateSeries::TreasuryY10,
            date(2023, 1, 1),
            date(2024, 6, 1),
        )
        .collect()
        .await;
    assert_eq!(results.len(), 2, "{results:?}");
    let rate = results[0].as_ref().unwrap();
    assert_eq!(
        (rate.date, rate.rate_percent),
        (date(2023, 1, 3), dec!(3.79))
    );
    let error = results[1].as_ref().unwrap_err();
    assert!(
        matches!(error, ThetaDataError::Disconnected { .. }),
        "{error:?}"
    );
    assert!(error.is_transient());
}

#[tokio::test]
async fn an_unreachable_terminal_is_reported_as_such() {
    // Nothing listens on port 1.
    let client = ThetaDataClient::new()
        .unwrap()
        .with_base_url("http://127.0.0.1:1/v3")
        .unwrap();
    let error = first_error(client.stock_symbols()).await;
    assert!(
        matches!(error, ThetaDataError::TerminalUnreachable { .. }),
        "{error:?}"
    );
    assert!(error.is_transient());
}

#[tokio::test]
async fn a_malformed_row_is_a_decode_error() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/option/list/expirations"))
        .respond_with(ndjson(&[
            r#"{"symbol":"XYZ","expiration":"2024-03-15"}"#,
            r#"{"symbol":"XYZ","expiration":"soon"}"#,
        ]))
        .mount(&server)
        .await;
    let results: Vec<_> = client(&server)
        .await
        .option_expirations("XYZ")
        .collect()
        .await;
    assert_eq!(results[0].as_ref().unwrap(), &date(2024, 3, 15));
    assert!(
        matches!(results[1], Err(ThetaDataError::Decode { .. })),
        "{results:?}"
    );
}

#[tokio::test]
async fn requests_beyond_the_limit_wait_for_a_free_slot() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/stock/list/symbols"))
        .respond_with(ndjson(&[r#"{"symbol":"XYZ"}"#]).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;

    let one_at_a_time = client(&server).await;
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(
        collect(one_at_a_time.stock_symbols()),
        collect(one_at_a_time.stock_symbols())
    );
    assert_eq!((a.len(), b.len()), (1, 1));
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "{:?}",
        started.elapsed()
    );

    let two_at_a_time = one_at_a_time.with_max_concurrent_requests(NonZeroUsize::new(2).unwrap());
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(
        collect(two_at_a_time.stock_symbols()),
        collect(two_at_a_time.stock_symbols())
    );
    assert_eq!((a.len(), b.len()), (1, 1));
    assert!(
        started.elapsed() < Duration::from_millis(790),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_stream_holds_its_slot_until_it_ends_or_is_dropped() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/stock/list/symbols"))
        .respond_with(ndjson(&[r#"{"symbol":"XYZ"}"#, r#"{"symbol":"ABC"}"#]))
        .mount(&server)
        .await;
    let client = client(&server).await;

    // The first stream is between rows: it still holds the only slot.
    let mut first = Box::pin(client.stock_symbols());
    assert_eq!(first.next().await.unwrap().unwrap(), "XYZ");
    let mut second = Box::pin(client.stock_symbols());
    assert!(
        tokio::time::timeout(Duration::from_millis(200), second.next())
            .await
            .is_err(),
        "a second request started while the first stream was unfinished"
    );

    // Dropping the unfinished stream frees the slot.
    drop(first);
    let rows = tokio::time::timeout(Duration::from_secs(5), async {
        let mut rows = Vec::new();
        while let Some(row) = second.next().await {
            rows.push(row.unwrap());
        }
        rows
    })
    .await
    .expect("the dropped stream released its slot");
    assert_eq!(rows, ["XYZ", "ABC"]);
}

#[tokio::test]
async fn a_body_without_newlines_is_refused_not_buffered_without_bound() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/stock/list/symbols"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(2 << 20)))
        .mount(&server)
        .await;
    let client = client(&server).await;
    let error = first_error(client.stock_symbols()).await;
    assert!(
        matches!(&error, ThetaDataError::Decode { message, .. } if message.contains("longer than")),
        "{error:?}"
    );
}

#[tokio::test]
async fn option_bars_carry_their_close_time_and_vwap() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/option/history/ohlc"))
        .and(query_param("interval", "5m"))
        .and(query_param("strike_range", "3"))
        .respond_with(ndjson(&[&format!(
            r#"{{{CONTRACT},"timestamp":"2024-07-01T09:30:00","open":2.00,"high":2.40,"low":1.95,"close":2.30,"volume":41,"count":9,"vwap":2.21}}"#
        )]))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server).await;
    let selection =
        OptionSelection::new("XYZ", Expiration::On(date(2024, 3, 15))).with_strike_range(3);
    let bars = collect(client.option_ohlc_history(
        &selection,
        date(2024, 7, 1),
        date(2024, 7, 1),
        Interval::M5,
        None,
    ))
    .await;
    let bar = &bars[0].bar;
    assert_eq!(bars[0].contract, xyz_call());
    // 09:30 New York in July is EDT, UTC-4.
    assert_eq!(
        bar.open_time,
        Utc.with_ymd_and_hms(2024, 7, 1, 13, 30, 0).unwrap()
    );
    assert_eq!(
        bar.candle.close_time,
        Utc.with_ymd_and_hms(2024, 7, 1, 13, 35, 0).unwrap()
    );
    assert_eq!(
        (
            bar.candle.open,
            bar.candle.high,
            bar.candle.low,
            bar.candle.close
        ),
        (dec!(2.00), dec!(2.40), dec!(1.95), dec!(2.30))
    );
    assert_eq!(bar.candle.volume, Some(dec!(41)));
    assert_eq!(bar.candle.trade_count, Some(9));
    assert_eq!(bar.vwap, Some(dec!(2.21)));

    let error = first_error(client.option_ohlc_history(
        &selection,
        date(2024, 7, 1),
        date(2024, 7, 1),
        Interval::Tick,
        None,
    ))
    .await;
    assert!(
        matches!(error, ThetaDataError::InvalidInput { .. }),
        "{error:?}"
    );
    let error = first_error(client.option_ohlc_history(
        &selection.with_max_dte(5),
        date(2024, 7, 1),
        date(2024, 7, 1),
        Interval::M5,
        None,
    ))
    .await;
    assert!(
        matches!(error, ThetaDataError::InvalidInput { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn option_daily_history_decodes_open_interest_and_end_of_day() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/option/history/open_interest"))
        .and(query_param("expiration", "*"))
        .and(query_param("max_dte", "30"))
        // A whole chain's daily history goes in one request over several days; only quote history
        // needs an expiration for that.
        .and(query_param("start_date", "20240304"))
        .and(query_param("end_date", "20240315"))
        .respond_with(ndjson(&[&format!(
            r#"{{{CONTRACT},"timestamp":"2024-03-04T06:30:02","open_interest":1234}}"#
        )]))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/history/eod"))
        .and(query_param("expiration", "*"))
        .and(query_param("start_date", "20240304"))
        .and(query_param("end_date", "20240315"))
        .respond_with(ndjson(&[&format!(
            r#"{{{CONTRACT},"created":"2024-03-04T17:15:01.5","last_trade":"2024-03-04T15:59:58","open":1.0,"high":1.5,"low":0.9,"close":1.2,"volume":300,"count":25,{NBBO}}}"#
        )]))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server).await;
    let selection = OptionSelection::new("XYZ", Expiration::All).with_max_dte(30);

    let open_interest = collect(client.option_open_interest_history(
        &selection,
        date(2024, 3, 4),
        date(2024, 3, 15),
    ))
    .await;
    assert_eq!(open_interest[0].contract, xyz_call());
    assert_eq!(open_interest[0].open_interest, 1234);
    assert_eq!(
        open_interest[0].time,
        Utc.with_ymd_and_hms(2024, 3, 4, 11, 30, 2).unwrap()
    );

    let end_of_day =
        collect(client.option_end_of_day_history(&selection, date(2024, 3, 4), date(2024, 3, 15)))
            .await;
    let eod = &end_of_day[0].end_of_day;
    assert_eq!(end_of_day[0].contract, xyz_call());
    assert_eq!(
        (eod.close, eod.volume, eod.trade_count),
        (dec!(1.2), 300, 25)
    );
    assert_eq!(eod.closing_nbbo.bid_size, 12);
    assert_eq!(
        eod.last_trade,
        Utc.with_ymd_and_hms(2024, 3, 4, 20, 59, 58).unwrap()
    );
}

#[tokio::test]
async fn option_lists_send_their_filters() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/option/list/contracts/quote"))
        .and(query_param("date", "20240304"))
        .and(query_param("symbol", "XYZ,ABC"))
        .respond_with(ndjson(&[&format!("{{{CONTRACT}}}")]))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/list/contracts/trade"))
        .and(query_param_is_missing("symbol"))
        .respond_with(ndjson(&[]))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/list/strikes"))
        .and(query_param("symbol", "XYZ"))
        .and(query_param("expiration", "20240315"))
        .respond_with(ndjson(&[
            r#"{"symbol":"XYZ","strike":100.000}"#,
            r#"{"symbol":"XYZ","strike":101.500}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/list/dates/trade"))
        .and(query_param("expiration", "20240315"))
        .and(query_param("strike", "101.5"))
        .and(query_param_is_missing("max_dte"))
        .respond_with(ndjson(&[r#"{"date":"20240304"}"#]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/list/symbols"))
        .respond_with(ndjson(&[r#"{"symbol":"SPXW"}"#]))
        .mount(&server)
        .await;
    let client = client(&server).await;

    let contracts =
        collect(client.option_contracts(Listing::Quote, date(2024, 3, 4), &["XYZ", "ABC"])).await;
    assert_eq!(contracts, vec![xyz_call()]);
    assert!(
        collect(client.option_contracts(Listing::Trade, date(2024, 3, 4), &[]))
            .await
            .is_empty()
    );
    assert_eq!(
        collect(client.option_strikes("XYZ", date(2024, 3, 15))).await,
        vec![dec!(100), dec!(101.5)]
    );
    let selection = OptionSelection::contract(&xyz_call()).with_max_dte(3);
    assert_eq!(
        collect(client.option_dates(Listing::Trade, &selection)).await,
        vec![date(2024, 3, 4)]
    );
    assert_eq!(collect(client.option_roots()).await, vec!["SPXW"]);

    let error = first_error(client.option_dates(
        Listing::Quote,
        &OptionSelection::new("XYZ", Expiration::All),
    ))
    .await;
    assert!(
        matches!(error, ThetaDataError::InvalidInput { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_chain_snapshot_is_one_request() {
    let server = MockServer::start().await;
    let put = r#""symbol":"XYZ","expiration":"2024-03-15","strike":95,"right":"PUT""#;
    Mock::given(path("/v3/option/snapshot/quote"))
        .and(query_param("expiration", "*"))
        .and(query_param("strike", "*"))
        .and(query_param("right", "both"))
        .and(query_param("current_day_only", "true"))
        .respond_with(ndjson(&[
            &format!(r#"{{"timestamp":"2024-03-04T15:59:59.9",{CONTRACT},{NBBO}}}"#),
            &format!(r#"{{"timestamp":"2024-03-04T15:59:58",{put},"bid":0,"bid_size":0,"bid_exchange":0,"bid_condition":0,"ask":0.05,"ask_size":100,"ask_exchange":1,"ask_condition":50}}"#),
        ]))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/snapshot/open_interest"))
        .and(query_param_is_missing("current_day_only"))
        .respond_with(ndjson(&[&format!(
            r#"{{"timestamp":"2024-03-04T06:30:00",{CONTRACT},"open_interest":7}}"#
        )]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/option/snapshot/ohlc"))
        .and(query_param("strike", "101.5"))
        .respond_with(ndjson(&[&format!(
            r#"{{"timestamp":"2024-03-04T12:00:00",{CONTRACT},"open":1,"high":2,"low":0.5,"close":1.5,"volume":10,"count":3}}"#
        )]))
        .mount(&server)
        .await;
    let client = client(&server).await;
    let chain = OptionSelection::new("XYZ", Expiration::All);

    let quotes = collect(client.option_quote_snapshot(&chain, true)).await;
    assert_eq!(quotes.len(), 2);
    assert_eq!(quotes[1].contract.kind, OptionKind::Put);
    let book = OrderBookL1::from(&quotes[1]);
    assert_eq!(book.best_bid, None, "a zero bid is no bid");
    assert_eq!(book.best_ask, Some(Level::new(dec!(0.05), dec!(100))));

    let open_interest = collect(client.option_open_interest_snapshot(&chain, false)).await;
    assert_eq!(open_interest[0].open_interest, 7);
    let ohlc =
        collect(client.option_ohlc_snapshot(&chain.with_strike(Strike::At(dec!(101.5))), false))
            .await;
    assert_eq!((ohlc[0].close, ohlc[0].trade_count), (dec!(1.5), 3));
}

#[tokio::test]
async fn stock_and_index_endpoints_decode_their_rows() {
    let server = MockServer::start().await;
    let eod = format!(
        r#"{{"created":"2024-03-04T17:15:00","last_trade":"2024-03-04T17:10:00","open":10,"high":11,"low":9.5,"close":10.5,"volume":1000,"count":100,{NBBO}}}"#
    );
    Mock::given(path("/v3/index/history/eod"))
        .and(query_param("symbol", "IDX"))
        .respond_with(ndjson(&[&eod]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/stock/history/ohlc"))
        .and(query_param("interval", "1h"))
        .respond_with(ndjson(&[
            r#"{"timestamp":"2024-03-04T10:00:00","open":10,"high":11,"low":9,"close":10.5,"volume":500,"count":40,"vwap":10.2}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/index/history/ohlc"))
        .respond_with(ndjson(&[
            r#"{"timestamp":"2024-03-04T10:00:00","open":10,"high":11,"low":9,"close":10.5,"volume":0,"count":0,"vwap":null}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/stock/snapshot/quote"))
        .and(query_param("symbol", "XYZ,ABC"))
        .respond_with(ndjson(&[&format!(
            r#"{{"timestamp":"2024-03-04T12:00:00","symbol":"ABC",{NBBO}}}"#
        )]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/stock/snapshot/ohlc"))
        .respond_with(ndjson(&[
            r#"{"timestamp":"2024-03-04T12:00:00","symbol":"XYZ","open":1,"high":2,"low":1,"close":2,"volume":5,"count":2}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/index/snapshot/price"))
        .respond_with(ndjson(&[
            r#"{"timestamp":"2024-03-04T12:00:00","symbol":"IDX","price":4321.5}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/stock/list/dates/quote"))
        .and(query_param("symbol", "XYZ"))
        .respond_with(ndjson(&[r#"{"date":"2024-03-04"}"#]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/index/list/dates"))
        .respond_with(ndjson(&[r#"{"date":"2024-03-05"}"#]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/index/list/symbols"))
        .respond_with(ndjson(&[r#"{"symbol":"IDX"}"#]))
        .mount(&server)
        .await;
    let client = client(&server).await;
    let day = date(2024, 3, 4);

    let eod = collect(client.index_end_of_day_history("IDX", day, day)).await;
    assert_eq!((eod[0].close, eod[0].volume), (dec!(10.5), 1000));

    let bars = collect(client.stock_ohlc_history("XYZ", day, day, Interval::H1, None)).await;
    assert_eq!(
        bars[0].candle.close_time,
        Utc.with_ymd_and_hms(2024, 3, 4, 16, 0, 0).unwrap()
    );
    assert_eq!(bars[0].vwap, Some(dec!(10.2)));
    let bars = collect(client.index_ohlc_history("IDX", day, day, Interval::H1, None)).await;
    assert_eq!((bars[0].vwap, bars[0].candle.volume), (None, Some(dec!(0))));
    let error = first_error(client.stock_ohlc_history("XYZ", day, day, Interval::Tick, None)).await;
    assert!(
        matches!(error, ThetaDataError::InvalidInput { .. }),
        "{error:?}"
    );

    let quotes = collect(client.stock_quote_snapshot(&["XYZ", "ABC"], false)).await;
    assert_eq!(quotes[0].symbol, "ABC");
    assert_eq!(
        OrderBookL1::from(&quotes[0]).best_bid,
        Some(Level::new(dec!(1.10), dec!(12)))
    );
    let ohlc = collect(client.stock_ohlc_snapshot(&["XYZ"], true)).await;
    assert_eq!((ohlc[0].symbol.as_str(), ohlc[0].volume), ("XYZ", 5));
    let prices = collect(client.index_price_snapshot(&["IDX"], false)).await;
    assert_eq!(prices[0].price, dec!(4321.5));
    assert_eq!(
        collect(client.stock_dates(Listing::Quote, "XYZ")).await,
        vec![day]
    );
    assert_eq!(
        collect(client.index_dates("IDX")).await,
        vec![date(2024, 3, 5)]
    );
    assert_eq!(collect(client.index_symbols()).await, vec!["IDX"]);
}

#[tokio::test]
async fn the_calendar_decodes_days_and_holidays() {
    let server = MockServer::start().await;
    Mock::given(path("/v3/calendar/today"))
        .respond_with(ndjson(&[
            r#"{"type":"open","open":"09:30:00","close":"16:00:00"}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/calendar/on_date"))
        .and(query_param("date", "20241225"))
        .respond_with(ndjson(&[
            r#"{"type":"full_close","open":null,"close":null}"#,
        ]))
        .mount(&server)
        .await;
    Mock::given(path("/v3/calendar/year_holidays"))
        .and(query_param("year", "2024"))
        .respond_with(ndjson(&[
            r#"{"date":"2024-07-03","type":"early_close","open":"09:30:00","close":"13:00:00"}"#,
            r#"{"date":"2024-07-04","type":"full_close","open":null,"close":null}"#,
        ]))
        .mount(&server)
        .await;
    let client = client(&server).await;

    let today = client.market_day_today().await.unwrap().unwrap();
    assert_eq!(today.kind, MarketDayKind::Open);
    assert_eq!(today.close, NaiveTime::from_hms_opt(16, 0, 0));
    let christmas = client
        .market_day(date(2024, 12, 25))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (christmas.kind, christmas.open),
        (MarketDayKind::FullClose, None)
    );
    let holidays = collect(client.market_holidays(2024)).await;
    assert_eq!(holidays.len(), 2);
    assert_eq!(holidays[0].date, date(2024, 7, 3));
    assert_eq!(holidays[0].day.kind, MarketDayKind::EarlyClose);
    assert_eq!(holidays[0].day.close, NaiveTime::from_hms_opt(13, 0, 0));

    server.reset().await;
    Mock::given(path("/v3/calendar/on_date"))
        .respond_with(ResponseTemplate::new(472))
        .mount(&server)
        .await;
    assert_eq!(client.market_day(date(2030, 1, 2)).await, Ok(None));
}
