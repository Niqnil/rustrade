//! Alpaca options and time-in-force checks against a paper account.
//!
//! Kept apart from `alpaca_integration.rs`, which CI runs with `--ignored` on every push: these
//! place option orders that fill, so they need US regular trading hours (9:30 AM - 4:00 PM ET),
//! and a paper account approved for options trading, with crypto trading enabled for the crypto
//! check. Each test that needs the market open checks Alpaca's clock first, and fails with a
//! message saying so when the market is closed or closes within ten minutes.
//!
//! The tests that trade an option refuse to start while the account holds a position in the
//! contract. A check that fails after an order is placed first cancels the open orders on the
//! contract and closes the position in it, through Alpaca's paper API directly. If that cleanup
//! reports an error, check the paper account by hand.
//!
//! # What they check
//! - A market `day` buy to open and a market `day` sell to close of one option contract fill, and
//!   the fills are read back.
//! - A `day` limit option order rests and cancels.
//! - Alpaca itself rejects the time in force `AlpacaClient` refuses unsent: `ioc` on an option
//!   and `day` on crypto. These send the request directly rather than through the client, since
//!   the client never sends them.
//!
//! The contract is chosen at run time: a tradable SPY call with the nearest expiry from tomorrow
//! on, a last close between $0.50 and $3.00, and the most open interest. Set `ALPACA_TEST_OPTION`
//! to an OCC symbol to use another one.
//!
//! # Running
//! CI runs them in the weekly Alpaca workflow (`alpaca-weekly.yml`), whose schedule falls in
//! regular trading hours. To run them by hand:
//! ```bash
//! (set -a; . ./.env; set +a; cargo test -p rustrade-execution --features alpaca \
//!     --test alpaca_options_paper -- --ignored --test-threads=1)
//! ```

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{DateTime, TimeDelta, Utc};
use futures::FutureExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        alpaca::{AlpacaClient, AlpacaConfig},
    },
    error::OrderError,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{ReduceOnly, RequestCancel, RequestOpen},
        state::{ActiveOrderState, InactiveOrderState, OrderState},
    },
    trade::Trade,
};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use serde_json::Value;
use serial_test::serial;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

const PAPER_REST: &str = "https://paper-api.alpaca.markets";

fn credentials() -> (String, String) {
    (
        std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required"),
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required"),
    )
}

fn client() -> AlpacaClient {
    let (key, secret) = credentials();
    AlpacaClient::new(AlpacaConfig::paper(key, secret))
}

/// A request to Alpaca's paper trading API, authenticated, without going through the client.
fn paper_request(method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
    let (key, secret) = credentials();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("HTTP client")
        .request(method, format!("{PAPER_REST}{path}"))
        .header("APCA-API-KEY-ID", key)
        .header("APCA-API-SECRET-KEY", secret)
}

async fn get_json(path: &str) -> Value {
    let response = paper_request(reqwest::Method::GET, path)
        .send()
        .await
        .expect("send");
    let status = response.status();
    let body = response.text().await.expect("body");
    assert!(status.is_success(), "GET {path}: {status} {body}");
    serde_json::from_str(&body).expect("JSON")
}

/// Panics unless the US market is open and stays open for ten more minutes, since an option order
/// fills only then, and a position bought just before the close could not be sold until the next
/// day. Returns Alpaca's own time, so that the fills are read back free of any local clock skew.
async fn require_market_open() -> DateTime<Utc> {
    let clock = get_json("/v2/clock").await;
    assert_eq!(
        clock["is_open"],
        Value::Bool(true),
        "the US market is closed (next open {}): run these during regular trading hours",
        clock["next_open"]
    );
    let time = |field: &str| {
        let text = clock[field]
            .as_str()
            .unwrap_or_else(|| panic!("clock {field}: {clock}"));
        DateTime::parse_from_rfc3339(text)
            .unwrap_or_else(|error| panic!("clock {field} {text}: {error}"))
            .with_timezone(&Utc)
    };
    let now = time("timestamp");
    assert!(
        time("next_close") - now >= TimeDelta::minutes(10),
        "the US market closes at {}: too soon to open and close a position before it",
        clock["next_close"]
    );
    now
}

/// Panics if the account holds a position in `instrument`: a test that trades it would otherwise
/// read another position's fills, and its cleanup would close a position it did not open.
async fn require_no_position(instrument: &InstrumentNameExchange) {
    assert_eq!(
        holds_position(instrument).await,
        Ok(false),
        "the account already holds {instrument}, so close it first"
    );
}

/// The ids of the open orders on `instrument`, read from Alpaca's paper API directly.
async fn open_order_ids(instrument: &InstrumentNameExchange) -> Result<Vec<String>, String> {
    let symbol = instrument.name().as_str();
    let response = paper_request(
        reqwest::Method::GET,
        &format!("/v2/orders?status=open&symbols={symbol}"),
    )
    .send()
    .await
    .map_err(|error| error.to_string())?;
    let orders: Value = response.json().await.map_err(|error| error.to_string())?;
    let orders = orders
        .as_array()
        .ok_or_else(|| format!("not a list: {orders}"))?;
    Ok(orders
        .iter()
        .filter_map(|order| order["id"].as_str().map(str::to_owned))
        .collect())
}

/// Whether the account holds a position in `instrument`, read from Alpaca's paper API directly.
async fn holds_position(instrument: &InstrumentNameExchange) -> Result<bool, String> {
    let path = format!("/v2/positions/{}", instrument.name());
    let response = paper_request(reqwest::Method::GET, &path)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    match response.status() {
        reqwest::StatusCode::NOT_FOUND => Ok(false),
        status if status.is_success() => Ok(true),
        status => Err(format!("GET {path}: {status}")),
    }
}

/// Polls `done` every half second for up to ten seconds, returning whether it held.
async fn within_ten_seconds<F: Future<Output = bool>>(mut done: impl FnMut() -> F) -> bool {
    let started = tokio::time::Instant::now();
    loop {
        if done().await {
            return true;
        }
        if started.elapsed() >= Duration::from_secs(10) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Panics unless the account's position in `instrument` is gone within ten seconds, since Alpaca
/// may update the position a moment after the order that closed it reports filled.
async fn await_no_position(instrument: &InstrumentNameExchange) {
    let gone =
        within_ten_seconds(|| async { matches!(holds_position(instrument).await, Ok(false)) })
            .await;
    assert!(gone, "the account still holds {instrument} after the test");
}

/// Cancels every open order on `instrument` and closes any position in it, through Alpaca's paper
/// API directly: the cleanup a failed check runs. It waits for the cancels to land before closing
/// the position, since an open sell holds the position's quantity. Reports what it could not do
/// rather than panicking, since it runs while a panic is already on its way.
async fn flatten(instrument: &InstrumentNameExchange) {
    let symbol = instrument.name().as_str();
    match open_order_ids(instrument).await {
        Ok(ids) => {
            for id in ids {
                let cancelled = paper_request(reqwest::Method::DELETE, &format!("/v2/orders/{id}"))
                    .send()
                    .await;
                if !cancelled
                    .as_ref()
                    .is_ok_and(|response| response.status().is_success())
                {
                    eprintln!("CLEANUP: could not cancel order {id} on {symbol}: {cancelled:?}");
                }
            }
        }
        Err(error) => eprintln!("CLEANUP: could not list the open orders on {symbol}: {error}"),
    }
    if !within_ten_seconds(|| async {
        open_order_ids(instrument)
            .await
            .is_ok_and(|ids| ids.is_empty())
    })
    .await
    {
        eprintln!("CLEANUP: {symbol} still lists open orders after cancelling them");
    }

    let closed = within_ten_seconds(|| async {
        if matches!(holds_position(instrument).await, Ok(false)) {
            return true;
        }
        let response = paper_request(reqwest::Method::DELETE, &format!("/v2/positions/{symbol}"))
            .send()
            .await;
        if let Ok(response) = &response
            && response.status().is_success()
        {
            // The close is a market order: the next poll sees the position gone once it fills.
            return false;
        }
        eprintln!("CLEANUP: closing the position in {symbol}: {response:?}");
        false
    })
    .await;
    if !closed {
        eprintln!("CLEANUP: the account may still hold {symbol}: check the paper account by hand");
    }
}

/// Runs `checks`, and if any of them panics, [`flatten`]s `instrument` before passing the panic on.
async fn flattened_on_failure(
    instrument: &InstrumentNameExchange,
    checks: impl Future<Output = ()>,
) {
    if let Err(panic) = AssertUnwindSafe(checks).catch_unwind().await {
        flatten(instrument).await;
        std::panic::resume_unwind(panic);
    }
}

/// The option contract to trade: `ALPACA_TEST_OPTION` if set, else one chosen as the module
/// documentation says. Also returns its last close, if Alpaca reports one.
async fn option_contract() -> (InstrumentNameExchange, Option<Decimal>) {
    if let Ok(symbol) = std::env::var("ALPACA_TEST_OPTION") {
        return (symbol.as_str().into(), None);
    }

    let today = Utc::now().date_naive();
    let from = today + TimeDelta::days(1);
    let to = today + TimeDelta::days(10);
    let listed = get_json(&format!(
        "/v2/options/contracts?underlying_symbols=SPY&status=active&type=call\
         &expiration_date_gte={from}&expiration_date_lte={to}&limit=10000"
    ))
    .await;

    let decimal = |value: &Value| value.as_str().and_then(|text| text.parse::<Decimal>().ok());
    let candidates: Vec<(&str, &str, Decimal, Decimal)> = listed["option_contracts"]
        .as_array()
        .expect("option_contracts")
        .iter()
        .filter(|contract| contract["tradable"] == Value::Bool(true))
        .filter_map(|contract| {
            let close = decimal(&contract["close_price"])?;
            (dec!(0.50)..=dec!(3.00)).contains(&close).then_some((
                contract["symbol"].as_str()?,
                contract["expiration_date"].as_str()?,
                close,
                decimal(&contract["open_interest"]).unwrap_or_default(),
            ))
        })
        .collect();

    let nearest = candidates
        .iter()
        .map(|(_, expiry, _, _)| *expiry)
        .min()
        .expect(
            "no SPY call between $0.50 and $3.00 expiring in the next 10 days: set \
             ALPACA_TEST_OPTION to an OCC symbol to trade",
        );
    let (symbol, _, close, _) = candidates
        .iter()
        .filter(|(_, expiry, _, _)| *expiry == nearest)
        .max_by_key(|(_, _, _, open_interest)| *open_interest)
        .unwrap();

    println!("Option contract: {symbol} (expires {nearest}, last close {close})");
    ((*symbol).into(), Some(*close))
}

fn cid(prefix: &str) -> ClientOrderId {
    ClientOrderId::new(format!("{prefix}-{}", Utc::now().timestamp_millis()))
}

async fn open(
    client: &AlpacaClient,
    instrument: &InstrumentNameExchange,
    cid: ClientOrderId,
    request: RequestOpen,
) -> rustrade_execution::order::Order<
    ExchangeId,
    InstrumentNameExchange,
    rustrade_execution::order::state::UnindexedOrderState,
> {
    client
        .open_order(OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument,
                strategy: StrategyId::new("options-paper"),
                cid,
            },
            state: request,
        })
        .await
}

/// Wait until `instrument` lists no open orders, as a cancel Alpaca has finished leaves it,
/// panicking after 30 seconds.
async fn await_no_open_orders(client: &AlpacaClient, instrument: &InstrumentNameExchange) {
    let instruments = [instrument.clone()];
    let started = tokio::time::Instant::now();
    loop {
        let listed = client.fetch_open_orders(&instruments).await;
        if matches!(&listed, Ok(orders) if orders.is_empty()) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{instrument} still lists open orders: {listed:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The id Alpaca gave the order `placed` reports, which a market order reports either resting or
/// already filled.
fn placed_id(placed: &rustrade_execution::order::state::UnindexedOrderState) -> OrderId {
    match placed {
        OrderState::Active(ActiveOrderState::Open(open)) => open
            .id
            .assigned()
            .cloned()
            .unwrap_or_else(|| panic!("Alpaca assigned no order id: {placed:?}")),
        OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => filled.id.clone(),
        other => panic!("not placed: {other:?}"),
    }
}

/// Waits until Alpaca reports order `id` filled. Panics at once if it ends any other way, or after
/// 60 seconds.
async fn await_filled(id: &OrderId) {
    let started = tokio::time::Instant::now();
    loop {
        let order = get_json(&format!("/v2/orders/{id}")).await;
        match order["status"].as_str() {
            Some("filled") => return,
            Some(ended @ ("canceled" | "expired" | "rejected" | "done_for_day")) => {
                panic!("order {id} ended {ended} without filling: {order}")
            }
            _ => {}
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "order {id} not filled after 60 seconds: {order}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The fills of order `id` on `instrument` since `since`, read through the client, waiting up to
/// 60 seconds for at least one.
async fn await_fills(
    client: &AlpacaClient,
    instrument: &InstrumentNameExchange,
    id: &OrderId,
    since: DateTime<Utc>,
) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
    let started = tokio::time::Instant::now();
    loop {
        let read = client
            // Alpaca stamps the fills by its own clock, so the end is generous to absorb skew.
            .fetch_trades(
                since,
                Utc::now() + TimeDelta::minutes(5),
                std::slice::from_ref(instrument),
            )
            .await
            .expect("fetch_trades");
        let fills: Vec<_> = read
            .trades
            .into_iter()
            .filter(|trade| trade.order_id == *id)
            .collect();
        if !fills.is_empty() {
            return fills;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "no fill of order {id} on {instrument} read back after 60 seconds"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn option_market_day_buy_to_open_then_sell_to_close() {
    let now = require_market_open().await;
    let client = client();
    let (contract, _) = option_contract().await;
    require_no_position(&contract).await;
    let since = now - TimeDelta::minutes(1);

    flattened_on_failure(&contract, async {
        let bought = open(
            &client,
            &contract,
            cid("opt-bto"),
            RequestOpen::new(
                Side::Buy,
                None,
                dec!(1),
                OrderKind::Market,
                TimeInForce::GoodUntilEndOfDay,
            ),
        )
        .await;
        let bought_id = placed_id(&bought.state);
        await_filled(&bought_id).await;
        let buys = await_fills(&client, &contract, &bought_id, since).await;
        println!(
            "Buy to open filled: {:?}",
            buys.iter()
                .map(|fill| (fill.quantity, fill.price, &fill.fees))
                .collect::<Vec<_>>()
        );
        assert!(buys.iter().all(|fill| fill.side == Side::Buy));
        assert_eq!(
            buys.iter().map(|fill| fill.quantity).sum::<Decimal>(),
            dec!(1)
        );

        let sold = open(
            &client,
            &contract,
            cid("opt-stc"),
            RequestOpen::new(
                Side::Sell,
                None,
                dec!(1),
                OrderKind::Market,
                TimeInForce::GoodUntilEndOfDay,
            )
            .with_reduce_only(ReduceOnly::Position),
        )
        .await;
        let sold_id = placed_id(&sold.state);
        await_filled(&sold_id).await;
        let sells = await_fills(&client, &contract, &sold_id, since).await;
        println!(
            "Sell to close filled: {:?}",
            sells
                .iter()
                .map(|fill| (fill.quantity, fill.price, &fill.fees))
                .collect::<Vec<_>>()
        );
        assert!(sells.iter().all(|fill| fill.side == Side::Sell));
        assert_eq!(
            sells.iter().map(|fill| fill.quantity).sum::<Decimal>(),
            dec!(1)
        );
        assert!(
            buys.iter()
                .chain(&sells)
                .all(|fill| fill.price > Decimal::ZERO)
        );
        await_no_position(&contract).await;
    })
    .await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn option_day_limit_rests_then_cancels() {
    require_market_open().await;
    let client = client();
    let (contract, close) = option_contract().await;
    require_no_position(&contract).await;
    // Far below the market, so it rests: a fifth of the last close, never under a cent, and a cent
    // when the close is not known.
    let price = close
        .map(|close| (close / dec!(5)).round_dp(2))
        .unwrap_or(dec!(0.01))
        .max(dec!(0.01));

    flattened_on_failure(&contract, async {
        let placed = open(
            &client,
            &contract,
            cid("opt-day-limit"),
            RequestOpen::new(
                Side::Buy,
                Some(price),
                dec!(1),
                OrderKind::Limit,
                TimeInForce::GoodUntilEndOfDay,
            ),
        )
        .await;
        let OrderState::Active(ActiveOrderState::Open(open_state)) = &placed.state else {
            panic!("day limit at {price}: {:?}", placed.state);
        };
        assert_eq!(open_state.filled_quantity, Decimal::ZERO);
        println!("Day limit resting at {price}: {:?}", open_state.id);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let cancelled = client
            .cancel_order(OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::AlpacaBroker,
                    instrument: &contract,
                    strategy: placed.key.strategy.clone(),
                    cid: placed.key.cid.clone(),
                },
                state: RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            })
            .await;
        assert!(cancelled.state.is_ok(), "cancel: {:?}", cancelled.state);
        await_no_open_orders(&client, &contract).await;
        await_no_position(&contract).await;
    })
    .await;
}

/// POSTs `order` to Alpaca's paper `/v2/orders` and returns the HTTP status and body. An order
/// Alpaca unexpectedly takes is cancelled before this returns, so a failing check leaves nothing
/// working.
async fn post_order_directly(order: Value) -> (reqwest::StatusCode, Value) {
    let response = paper_request(reqwest::Method::POST, "/v2/orders")
        .header("Content-Type", "application/json")
        .body(order.to_string())
        .send()
        .await
        .expect("send");
    let status = response.status();
    let text = response.text().await.expect("body");
    // A body that is not JSON, such as a proxy's error page, is kept as text for the message.
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    if status.is_success()
        && let Some(id) = body["id"].as_str()
    {
        // 404 or 422: the order already ended, an IOC most likely, so nothing is left working.
        let cancelled = paper_request(reqwest::Method::DELETE, &format!("/v2/orders/{id}"))
            .send()
            .await;
        assert!(
            cancelled.as_ref().is_ok_and(|response| {
                response.status().is_success()
                    || matches!(
                        response.status(),
                        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::UNPROCESSABLE_ENTITY
                    )
            }),
            "Alpaca took order {id} ({status} {body}), and cancelling it failed: {cancelled:?}. \
             Cancel it by hand"
        );
    }
    (status, body)
}

/// Asserts Alpaca's 422 `body` refused the order for its time in force, not some other field.
/// The code `42210000` covers every validation failure, so the message is what names the field.
fn assert_time_in_force_refusal(body: &Value) {
    assert_eq!(body["code"], serde_json::json!(42210000), "{body}");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("time_in_force")),
        "{body}"
    );
}

/// Alpaca refuses an option order with `ioc`, which is why `AlpacaClient` refuses it unsent.
#[tokio::test]
#[ignore]
#[serial]
async fn alpaca_rejects_an_option_ioc_that_the_client_refuses_unsent() {
    let (contract, _) = option_contract().await;

    let refused = open(
        &client(),
        &contract,
        cid("opt-ioc"),
        RequestOpen::new(
            Side::Buy,
            Some(dec!(0.01)),
            dec!(1),
            OrderKind::Limit,
            TimeInForce::ImmediateOrCancel,
        ),
    )
    .await;
    assert!(
        matches!(
            refused.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(
                OrderError::UnsupportedTimeInForce { .. }
            ))
        ),
        "{:?}",
        refused.state
    );

    let (status, body) = post_order_directly(serde_json::json!({
        "symbol": contract.name().as_str(),
        "qty": "1",
        "side": "buy",
        "type": "limit",
        "limit_price": "0.01",
        "time_in_force": "ioc",
    }))
    .await;
    println!("Alpaca on an option ioc: {status} {body}");
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_time_in_force_refusal(&body);
}

/// Alpaca refuses a crypto order with `day`, which is why `AlpacaClient` refuses it unsent.
#[tokio::test]
#[ignore]
#[serial]
async fn alpaca_rejects_a_crypto_day_that_the_client_refuses_unsent() {
    let instrument: InstrumentNameExchange = "BTC/USD".into();

    let refused = open(
        &client(),
        &instrument,
        cid("btc-day"),
        RequestOpen::new(
            Side::Buy,
            Some(dec!(1000)),
            dec!(0.001),
            OrderKind::Limit,
            TimeInForce::GoodUntilEndOfDay,
        ),
    )
    .await;
    assert!(
        matches!(
            refused.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(
                OrderError::UnsupportedTimeInForce { .. }
            ))
        ),
        "{:?}",
        refused.state
    );

    let (status, body) = post_order_directly(serde_json::json!({
        "symbol": "BTC/USD",
        "qty": "0.001",
        "side": "buy",
        "type": "limit",
        "limit_price": "1000",
        "time_in_force": "day",
    }))
    .await;
    println!("Alpaca on a crypto day: {status} {body}");
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_time_in_force_refusal(&body);
}
