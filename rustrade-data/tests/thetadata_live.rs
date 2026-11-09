//! Live checks against a running Theta Terminal, on endpoints ThetaData's free plan serves.
//!
//! `#[ignore]`d: they need a Terminal logged in to a ThetaData account, at `THETADATA_BASE_URL`
//! (default `http://127.0.0.1:25503/v3`). Run with
//! `cargo test -p rustrade-data --features thetadata --test thetadata_live -- --ignored`.
//!
//! They assert shapes and invariants only, and print nothing of the data: ThetaData's data may not
//! be redistributed.

#![cfg(feature = "thetadata")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Datelike, NaiveDate, NaiveTime, Utc};
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_data::exchange::thetadata::{
    DEFAULT_BASE_URL, Expiration, InterestRateSeries, OptionSelection, Strike, ThetaDataClient,
    ThetaDataError,
};
use rustrade_instrument::instrument::kind::option::OptionKind;

fn client() -> ThetaDataClient {
    let base_url =
        std::env::var("THETADATA_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());
    ThetaDataClient::new()
        .unwrap()
        .with_base_url(base_url)
        .unwrap()
}

async fn collect<T>(stream: impl Stream<Item = Result<T, ThetaDataError>>) -> Vec<T> {
    stream.map(|row| row.unwrap()).collect().await
}

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn lists_name_known_symbols() {
    let client = client();
    let roots = collect(client.option_roots()).await;
    assert!(
        roots.iter().any(|root| root == "SPY"),
        "{} roots",
        roots.len()
    );
    let stocks = collect(client.stock_symbols()).await;
    assert!(
        stocks.iter().any(|symbol| symbol == "AAPL"),
        "{} symbols",
        stocks.len()
    );

    let expirations = collect(client.option_expirations("SPY")).await;
    assert!(!expirations.is_empty());
    let next = *expirations
        .iter()
        .find(|expiration| **expiration >= Utc::now().date_naive())
        .expect("SPY has a future expiration");
    let strikes = collect(client.option_strikes("SPY", next)).await;
    assert!(!strikes.is_empty());
    assert!(strikes.iter().all(|strike| strike.is_sign_positive()));
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn end_of_day_history_covers_each_trading_day() {
    let client = client();
    // Two weeks of October 2025, all trading days (no US holiday).
    let (start, end) = (date(2025, 10, 6), date(2025, 10, 17));
    let stock = collect(client.stock_end_of_day_history("SPY", start, end)).await;
    assert_eq!(stock.len(), 10, "one report per trading day");
    for eod in &stock {
        let candle = &eod.candle;
        assert!(
            candle.low <= candle.open && candle.open <= candle.high,
            "{eod:?}"
        );
        assert!(
            candle.low <= candle.close && candle.close <= candle.high,
            "{eod:?}"
        );
        assert!(candle.volume > Some(Decimal::ZERO) && candle.trade_count > Some(0));
        assert!(eod.last_trade <= eod.created);
        // The trading day is the report's New York date, and the daily candle closes at the end
        // of that UTC day, after the report was generated.
        assert!((start..=end).contains(&eod.date), "{eod:?}");
        assert_eq!(
            candle.close_time,
            eod.date
                .succ_opt()
                .unwrap()
                .and_time(NaiveTime::MIN)
                .and_utc()
        );
        assert!(eod.created < candle.close_time, "{eod:?}");
    }
    assert!(
        stock.windows(2).all(|w| w[0].date < w[1].date),
        "one report per trading day, in order"
    );
    let index = collect(client.index_end_of_day_history("SPX", start, end)).await;
    assert!(!index.is_empty());

    // One expiration's end-of-day reports on one day: every row names that expiration.
    let expiration = date(2025, 10, 17);
    let option = collect(client.option_end_of_day_history(
        &OptionSelection::new("SPY", Expiration::On(expiration)),
        date(2025, 10, 15),
        date(2025, 10, 15),
    ))
    .await;
    assert!(!option.is_empty());
    assert!(
        option
            .iter()
            .all(|row| row.contract.expiration == expiration)
    );
    assert!(option.iter().all(|row| row.contract.root == "SPY"));
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn end_of_day_history_longer_than_one_request_is_split() {
    let client = client();
    // Stock: more than a year, so more than one span of daily data.
    let stock =
        collect(client.stock_end_of_day_history("SPY", date(2024, 6, 3), date(2025, 10, 17))).await;
    assert!(stock.len() > 340, "{} reports", stock.len());
    assert!(
        stock
            .windows(2)
            .all(|w| w[0].created.date_naive() < w[1].created.date_naive()),
        "one report per day, in order"
    );

    // Option: one contract over more than 28 days, so more than one span of option daily data.
    let contract = OptionSelection::new("SPY", Expiration::On(date(2025, 12, 19)))
        .with_strike(Strike::At(dec!(600)))
        .with_kind(OptionKind::Call);
    let option =
        collect(client.option_end_of_day_history(&contract, date(2025, 10, 1), date(2025, 11, 14)))
            .await;
    assert!(option.len() > 28, "{} reports", option.len());
    assert!(
        option.windows(2).all(|w| {
            w[0].end_of_day.created.date_naive() < w[1].end_of_day.created.date_naive()
        }),
        "one report per day, in order"
    );
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn whole_chain_end_of_day_history_takes_several_days_per_request() {
    // Every expiration within a week, over a trading week: ThetaData serves several days of a
    // whole chain's end-of-day reports in one request.
    let chain = OptionSelection::new("SPY", Expiration::All).with_max_dte(7);
    let rows =
        collect(client().option_end_of_day_history(&chain, date(2025, 10, 13), date(2025, 10, 17)))
            .await;
    let mut days: Vec<_> = rows
        .iter()
        .map(|row| row.end_of_day.created.date_naive())
        .collect();
    days.sort_unstable();
    days.dedup();
    assert_eq!(days.len(), 5, "one report day per trading day");
    let mut expirations: Vec<_> = rows.iter().map(|row| row.contract.expiration).collect();
    expirations.sort_unstable();
    expirations.dedup();
    assert!(expirations.len() > 1, "{} expirations", expirations.len());
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn interest_rates_span_more_than_one_request() {
    // Two years: more than one span of daily data, so the split is exercised live. The free plan's
    // rate history starts in 2024.
    let rates = collect(client().interest_rate_history(
        InterestRateSeries::Sofr,
        date(2024, 1, 1),
        date(2025, 12, 31),
    ))
    .await;
    assert!(rates.len() > 400, "{} rates", rates.len());
    assert!(
        rates.windows(2).all(|w| w[0].date < w[1].date),
        "dates ascend without repeats"
    );
    assert!(
        rates
            .iter()
            .all(|rate| rate.rate_percent.is_sign_positive() && rate.rate_percent < 20.into())
    );
    assert_eq!(rates.first().unwrap().date.year(), 2024);
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn history_before_the_plan_starts_is_a_rejected_request() {
    // The free plan's rate history starts in 2024; earlier dates are refused with HTTP 400, not
    // with a permission status. On a plan that reaches back further they arrive.
    let result: Vec<_> = client()
        .interest_rate_history(
            InterestRateSeries::Sofr,
            date(2023, 12, 1),
            date(2023, 12, 29),
        )
        .collect()
        .await;
    match result.first() {
        Some(Err(ThetaDataError::InvalidRequest { status: 400, .. })) => {}
        Some(Ok(_)) => assert!(result.iter().all(Result::is_ok)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn the_calendar_answers_today() {
    let client = client();
    let today = client
        .market_day_today()
        .await
        .unwrap()
        .expect("today has a schedule");
    if today.open.is_some() {
        assert!(today.open < today.close, "{today:?}");
    }
    // Year holidays need the value plan: either answer is acceptable here.
    let holidays: Vec<_> = client.market_holidays(2025).collect().await;
    match holidays.first() {
        Some(Err(ThetaDataError::PermissionDenied { .. })) | None => {}
        Some(Err(other)) => panic!("{other:?}"),
        Some(Ok(_)) => assert!(holidays.iter().all(Result::is_ok)),
    }
}

#[tokio::test]
#[ignore = "needs a running Theta Terminal"]
async fn a_request_beyond_the_plan_is_a_permission_error_not_empty_data() {
    // One-minute option quotes need the value plan; on a free account they must be refused, not
    // silently empty. On a paid account they arrive.
    let result: Vec<_> = client()
        .option_quote_history(
            &OptionSelection::new("SPY", Expiration::On(date(2025, 10, 17))).with_strike_range(1),
            date(2025, 10, 15),
            date(2025, 10, 15),
            rustrade_data::exchange::thetadata::Interval::M1,
            None,
        )
        .collect()
        .await;
    match result.first() {
        Some(Err(ThetaDataError::PermissionDenied { .. })) => {}
        Some(Ok(_)) => assert!(result.iter().all(Result::is_ok)),
        other => panic!("{other:?}"),
    }
}
