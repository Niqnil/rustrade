//! Alpaca options and time-in-force checks against a paper account.
//!
//! Kept apart from `alpaca_integration.rs`, which CI runs with `--ignored` on every push: these
//! place option orders that fill, so they need US regular trading hours (9:30 AM - 4:00 PM ET),
//! and a paper account approved for options trading. Each test that needs the market open checks
//! Alpaca's clock first and fails with a message saying so.
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
//! ```bash
//! (set -a; . ./.env; set +a; cargo test -p rustrade-execution --features alpaca \
//!     --test alpaca_options_paper -- --ignored --test-threads=1)
//! ```

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{TimeDelta, Utc};
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
        id::{ClientOrderId, StrategyId},
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
use std::time::Duration;

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
    reqwest::Client::new()
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

/// Panics unless the US market is open, since an option order fills only then.
async fn require_market_open() {
    let clock = get_json("/v2/clock").await;
    assert_eq!(
        clock["is_open"],
        Value::Bool(true),
        "the US market is closed (next open {}): run these during regular trading hours",
        clock["next_open"]
    );
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
        .expect("no SPY call between $0.50 and $3.00 expiring in the next 10 days");
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

/// Wait until `instrument` lists no open orders, panicking after 30 seconds. A market order that
/// filled, or a cancel Alpaca has finished, both leave it so.
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

/// The fills on `instrument` on `side` since `since`, waiting up to 60 seconds for at least one.
async fn await_fills(
    client: &AlpacaClient,
    instrument: &InstrumentNameExchange,
    side: Side,
    since: chrono::DateTime<Utc>,
) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
    let started = tokio::time::Instant::now();
    loop {
        let read = client
            .fetch_trades(since, Utc::now(), std::slice::from_ref(instrument))
            .await
            .expect("fetch_trades");
        let fills: Vec<_> = read
            .trades
            .into_iter()
            .filter(|trade| trade.instrument == *instrument && trade.side == side)
            .collect();
        if !fills.is_empty() {
            return fills;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "no {side} fill on {instrument} read back after 60 seconds"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn option_market_day_buy_to_open_then_sell_to_close() {
    require_market_open().await;
    let client = client();
    let (contract, _) = option_contract().await;
    let started = Utc::now() - TimeDelta::seconds(5);

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
    assert!(
        matches!(bought.state, OrderState::Active(ActiveOrderState::Open(_))),
        "buy to open: {:?}",
        bought.state
    );
    await_no_open_orders(&client, &contract).await;
    let buys = await_fills(&client, &contract, Side::Buy, started).await;
    let bought_quantity: Decimal = buys.iter().map(|fill| fill.quantity).sum();
    println!(
        "Buy to open filled: {:?}",
        buys.iter()
            .map(|fill| (fill.quantity, fill.price, &fill.fees))
            .collect::<Vec<_>>()
    );
    assert_eq!(bought_quantity, dec!(1));

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
    assert!(
        matches!(sold.state, OrderState::Active(ActiveOrderState::Open(_))),
        "sell to close: {:?}",
        sold.state
    );
    await_no_open_orders(&client, &contract).await;
    let sells = await_fills(&client, &contract, Side::Sell, started).await;
    let sold_quantity: Decimal = sells.iter().map(|fill| fill.quantity).sum();
    println!(
        "Sell to close filled: {:?}",
        sells
            .iter()
            .map(|fill| (fill.quantity, fill.price, &fill.fees))
            .collect::<Vec<_>>()
    );
    assert_eq!(sold_quantity, dec!(1));
    assert!(
        buys.iter()
            .chain(&sells)
            .all(|fill| fill.price > Decimal::ZERO)
    );
}

#[tokio::test]
#[ignore]
#[serial]
async fn option_day_limit_rests_then_cancels() {
    require_market_open().await;
    let client = client();
    let (contract, close) = option_contract().await;
    // Far below the market, so it rests: a fifth of the last close, never under a cent.
    let price = close
        .map(|close| (close / dec!(5)).round_dp(2))
        .unwrap_or(dec!(0.05))
        .max(dec!(0.01));

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
    let body: Value = serde_json::from_str(&response.text().await.expect("body")).expect("JSON");
    if status.is_success()
        && let Some(id) = body["id"].as_str()
    {
        let _ = paper_request(reqwest::Method::DELETE, &format!("/v2/orders/{id}"))
            .send()
            .await;
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
