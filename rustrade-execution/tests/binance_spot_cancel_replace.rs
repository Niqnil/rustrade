//! Binance Spot cancel-replace against testnet: a `CancelThenOpen` pair sent as one
//! `POST /api/v3/order/cancelReplace`, with each leg answered from the venue's per-leg result.
//!
//! # Prerequisites
//!
//! A Binance Spot testnet account (https://testnet.binance.vision/) with some USDT, and
//! `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` set (see `.env.template`).
//!
//! # Running
//!
//! ```bash
//! (set -a; . ./.env; set +a; cargo test -p rustrade-execution --features binance \
//!     --test binance_spot_cancel_replace -- --ignored --test-threads=1)
//! ```
//!
//! Every test is hardcoded to testnet, and cancels what it placed. They are `#[ignore]`d, so CI,
//! which has no testnet credentials, does not run them.

#![cfg(feature = "binance")]
// Integration tests: unwrap/expect produce clear panic messages identifying the
// failed assertion, which is the desired behavior in test code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    client::{
        ExecutionClient,
        binance::{BinanceSpot, BinanceSpotConfig},
    },
    error::{ApiError, OrderError},
    order::{
        Order, OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId, VenueOrderId},
        request::{CancelReplaceAnswer, OnCancelFailure, RequestCancel, RequestOpen},
        state::{ActiveOrderState, InactiveOrderState, OrderState, UnindexedOrderState},
    },
};
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use std::time::Duration;

fn client() -> BinanceSpot {
    let api_key = std::env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY env var required");
    let secret_key =
        std::env::var("BINANCE_SECRET_KEY").expect("BINANCE_SECRET_KEY env var required");
    // These tests place orders, so they are hardcoded to testnet whatever BINANCE_TESTNET says.
    let config = BinanceSpotConfig::testnet(api_key, secret_key);
    assert!(config.testnet, "these tests must run on testnet");
    BinanceSpot::new(config)
}

fn btcusdt() -> InstrumentNameExchange {
    "BTCUSDT".into()
}

/// A client order id unique to this run.
fn cid(prefix: &str) -> ClientOrderId {
    ClientOrderId::new(format!(
        "{prefix}-{}",
        chrono::Utc::now().timestamp_millis()
    ))
}

fn key<'a>(
    instrument: &'a InstrumentNameExchange,
    cid: &ClientOrderId,
) -> OrderKey<ExchangeId, &'a InstrumentNameExchange> {
    OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument,
        strategy: StrategyId::new("test-cancel-replace"),
        cid: cid.clone(),
    }
}

/// Testnet's average price of `instrument`, which its `PERCENT_PRICE_BY_SIDE` filter measures
/// every order price against, so prices are derived from it rather than hard-coded.
async fn average_price(instrument: &InstrumentNameExchange) -> Decimal {
    let body = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client")
        .get(format!(
            "https://testnet.binance.vision/api/v3/avgPrice?symbol={instrument}"
        ))
        .send()
        .await
        .expect("avgPrice request failed")
        .error_for_status()
        .expect("avgPrice returned an error status")
        .json::<serde_json::Value>()
        .await
        .expect("avgPrice body is not JSON");
    body["price"]
        .as_str()
        .expect("avgPrice body has no price")
        .parse()
        .expect("avgPrice price is not a decimal")
}

/// A buy limit order of `quantity` at `factor` times the average price, on BTCUSDT's 0.01 tick:
/// below the market, so it rests.
fn buy_limit(average: Decimal, factor: Decimal, quantity: Decimal) -> RequestOpen {
    RequestOpen::new(
        Side::Buy,
        Some((average * factor).round_dp(2)),
        quantity,
        OrderKind::Limit,
        TimeInForce::GoodUntilCancelled { post_only: false },
    )
}

/// Place `request` under `cid`, and return the venue id it rests under.
async fn place(
    client: &BinanceSpot,
    instrument: &InstrumentNameExchange,
    cid: &ClientOrderId,
    request: RequestOpen,
) -> VenueOrderId {
    let placed = client
        .open_order(OrderEvent {
            key: key(instrument, cid),
            state: request,
        })
        .await;
    match placed.state {
        OrderState::Active(ActiveOrderState::Open(open)) => open.id,
        other => panic!("the order should rest: {other:?}"),
    }
}

/// Cancel the order `cid`, ignoring the answer: cleanup.
async fn cancel_quietly(
    client: &BinanceSpot,
    instrument: &InstrumentNameExchange,
    cid: &ClientOrderId,
) {
    let _ = client
        .cancel_order(OrderEvent {
            key: key(instrument, cid),
            state: RequestCancel::new(None),
        })
        .await;
}

/// The client order ids of the orders resting on `instrument`.
async fn resting(client: &BinanceSpot, instrument: &InstrumentNameExchange) -> Vec<ClientOrderId> {
    client
        .fetch_open_orders(std::slice::from_ref(instrument))
        .await
        .expect("fetch_open_orders")
        .into_iter()
        .map(|order| order.key.cid)
        .collect()
}

/// Send the cancel of `cancel` (by `venue_id` when given) and the open `open` under `new` as one
/// cancel-replace.
async fn cancel_replace(
    client: &BinanceSpot,
    instrument: &InstrumentNameExchange,
    cancel: &ClientOrderId,
    venue_id: Option<VenueOrderId>,
    new: &ClientOrderId,
    open: RequestOpen,
    on_cancel_failure: OnCancelFailure,
) -> CancelReplaceAnswer {
    client
        .cancel_replace(
            OrderEvent {
                key: key(instrument, cancel),
                state: RequestCancel::new(venue_id),
            },
            OrderEvent {
                key: key(instrument, new),
                state: open,
            },
            on_cancel_failure,
        )
        .expect("a same-symbol limit order goes as one request")
        .await
}

fn open_state(
    open: Option<&Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>,
) -> Option<&UnindexedOrderState> {
    open.map(|order| &order.state)
}

/// A resting order is moved in one request: the cancel answers with the old order's id, and the
/// new order rests under its own id.
#[tokio::test]
#[ignore]
async fn a_resting_order_is_moved_in_one_request() {
    let client = client();
    let instrument = btcusdt();
    let average = average_price(&instrument).await;
    let (old, new) = (cid("cr-old"), cid("cr-new"));
    let old_id = place(
        &client,
        &instrument,
        &old,
        buy_limit(average, dec!(0.85), dec!(0.001)),
    )
    .await;

    let answer = cancel_replace(
        &client,
        &instrument,
        &old,
        Some(old_id.clone()),
        &new,
        buy_limit(average, dec!(0.86), dec!(0.001)),
        OnCancelFailure::DropOpens,
    )
    .await;
    let resting_after = resting(&client, &instrument).await;
    cancel_quietly(&client, &instrument, &new).await;
    cancel_quietly(&client, &instrument, &old).await;

    let cancelled = answer.cancel.state.expect("the cancel succeeded");
    assert_eq!(VenueOrderId::Assigned(cancelled.id), old_id);
    match open_state(answer.open.as_ref()) {
        Some(OrderState::Active(ActiveOrderState::Open(open))) => assert_ne!(open.id, old_id),
        other => panic!("the new order should rest: {other:?}"),
    }
    assert!(resting_after.contains(&new), "{resting_after:?}");
    assert!(!resting_after.contains(&old), "{resting_after:?}");
}

/// A cancel that fails places nothing under `DropOpens`: the cancel carries its own reason, and
/// the new order is not attempted.
#[tokio::test]
#[ignore]
async fn a_failed_cancel_places_nothing() {
    let client = client();
    let instrument = btcusdt();
    let average = average_price(&instrument).await;
    let (missing, new) = (cid("cr-missing"), cid("cr-new"));

    let answer = cancel_replace(
        &client,
        &instrument,
        &missing,
        None,
        &new,
        buy_limit(average, dec!(0.86), dec!(0.001)),
        OnCancelFailure::DropOpens,
    )
    .await;
    let resting_after = resting(&client, &instrument).await;
    cancel_quietly(&client, &instrument, &new).await;

    assert!(
        matches!(answer.cancel.state, Err(OrderError::Rejected(_))),
        "{:?}",
        answer.cancel.state
    );
    assert_eq!(answer.open, None);
    assert!(!resting_after.contains(&new), "{resting_after:?}");
}

/// A new order refused after its cancel succeeded is answered with its own reason, and leaves
/// nothing resting.
#[tokio::test]
#[ignore]
async fn a_refused_new_order_carries_its_own_reason() {
    let client = client();
    let instrument = btcusdt();
    let average = average_price(&instrument).await;
    let (old, new) = (cid("cr-old"), cid("cr-new"));
    place(
        &client,
        &instrument,
        &old,
        buy_limit(average, dec!(0.85), dec!(0.001)),
    )
    .await;

    // More than a testnet account holds, so the venue refuses the new order for its balance,
    // yet within the symbol's notional cap, so the client's own rules let it through.
    let answer = cancel_replace(
        &client,
        &instrument,
        &old,
        None,
        &new,
        buy_limit(average, dec!(0.86), dec!(50)),
        OnCancelFailure::DropOpens,
    )
    .await;
    let resting_after = resting(&client, &instrument).await;
    cancel_quietly(&client, &instrument, &new).await;
    cancel_quietly(&client, &instrument, &old).await;

    assert!(answer.cancel.state.is_ok(), "{:?}", answer.cancel.state);
    match open_state(answer.open.as_ref()) {
        Some(OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::BalanceInsufficient(..),
        )))) => {}
        other => panic!("the new order should be refused for its balance: {other:?}"),
    }
    assert!(!resting_after.contains(&old), "{resting_after:?}");
    assert!(!resting_after.contains(&new), "{resting_after:?}");
}
