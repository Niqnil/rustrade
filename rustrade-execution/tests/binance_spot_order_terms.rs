//! Binance Spot order terms against testnet: what the account stream and an account snapshot
//! state about an order changed outside this client, by a keep-priority amend, and how the stream
//! reports an amend that ends the order.
//!
//! # Prerequisites
//!
//! A Binance Spot testnet account (https://testnet.binance.vision/) with some USDT, and
//! `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` set (see `.env.template`). The amend that ends an
//! order also needs some WBTC: it fills an order of its own against one of its own.
//!
//! # Running
//!
//! ```bash
//! (set -a; . ./.env; set +a; cargo test -p rustrade-execution --features binance \
//!     --test binance_spot_order_terms -- --ignored --test-threads=1)
//! ```
//!
//! Every test is hardcoded to testnet, and cancels what it placed, even when it fails. They are
//! `#[ignore]`d, so CI, which has no testnet credentials, does not run them.

#![cfg(feature = "binance")]
// Integration tests: unwrap/expect produce clear panic messages identifying the
// failed assertion, which is the desired behavior in test code.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use binance_sdk::{
    common::config::ConfigurationRestApi,
    spot::{
        SpotRestApi,
        rest_api::{
            NewOrderParams, NewOrderSelfTradePreventionModeEnum, NewOrderSideEnum,
            NewOrderTimeInForceEnum, NewOrderTypeEnum, OrderAmendKeepPriorityParams,
        },
    },
};
use futures::{FutureExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountEventKind,
    client::{
        ExecutionClient,
        binance::{BinanceSpot, BinanceSpotConfig},
    },
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        report::{ReportedTerms, UnindexedOrderReport},
        request::{RequestCancel, RequestOpen},
        state::{ActiveOrderState, OrderState},
    },
};
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use std::time::Duration;

fn credentials() -> (String, String) {
    (
        std::env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY env var required"),
        std::env::var("BINANCE_SECRET_KEY").expect("BINANCE_SECRET_KEY env var required"),
    )
}

fn client() -> BinanceSpot {
    let (api_key, secret_key) = credentials();
    // These tests place orders, so they are hardcoded to testnet whatever BINANCE_TESTNET says.
    let config = BinanceSpotConfig::testnet(api_key, secret_key);
    assert!(config.testnet, "these tests must run on testnet");
    BinanceSpot::new(config)
}

/// A testnet REST client of the SDK's own, standing in for a change made outside this client.
fn outside_client() -> binance_sdk::spot::rest_api::RestApi {
    let (api_key, secret_key) = credentials();
    let config = ConfigurationRestApi::builder()
        .api_key(api_key)
        .api_secret(secret_key)
        .build()
        .expect("REST configuration");
    SpotRestApi::testnet(config)
}

fn key<'a>(
    instrument: &'a InstrumentNameExchange,
    cid: &ClientOrderId,
) -> OrderKey<ExchangeId, &'a InstrumentNameExchange> {
    OrderKey {
        exchange: ExchangeId::BinanceSpot,
        instrument,
        strategy: StrategyId::new("test-order-terms"),
        cid: cid.clone(),
    }
}

/// The JSON body of a public testnet endpoint, `path_and_query` under `/api/v3/`.
async fn public_get(path_and_query: &str) -> serde_json::Value {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client")
        .get(format!(
            "https://testnet.binance.vision/api/v3/{path_and_query}"
        ))
        .send()
        .await
        .unwrap_or_else(|e| panic!("{path_and_query} request failed: {e}"))
        .error_for_status()
        .unwrap_or_else(|e| panic!("{path_and_query} returned an error status: {e}"))
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|e| panic!("{path_and_query} body is not JSON: {e}"))
}

/// The decimal a JSON string field holds.
fn decimal(value: &serde_json::Value, field: &str) -> Decimal {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("no {field} in {value}"))
        .parse()
        .unwrap_or_else(|e| panic!("{field} is not a decimal: {e}"))
}

/// Testnet's average price of `instrument`, which its `PERCENT_PRICE_BY_SIDE` filter measures
/// every order price against, so prices are derived from it rather than hard-coded.
async fn average_price(instrument: &InstrumentNameExchange) -> Decimal {
    decimal(
        &public_get(&format!("avgPrice?symbol={instrument}")).await,
        "price",
    )
}

/// The first report on `stream` of the order `cid` at `quantity`, within ten seconds.
async fn next_report(
    stream: &mut (impl futures::Stream<Item = rustrade_execution::UnindexedAccountEvent> + Unpin),
    cid: &ClientOrderId,
    quantity: Decimal,
) -> Option<UnindexedOrderReport> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, stream.next()).await {
        if let AccountEventKind::OrderSnapshot(snapshot) = event.kind {
            let report = snapshot.0;
            if &report.order.key.cid == cid && report.order.quantity == quantity {
                return Some(report);
            }
        }
    }
    None
}

/// A keep-priority amend that keeps the client order id is a change made at the venue: the stream
/// reports the order on its new quantity, and an account snapshot lists it so, each stating every
/// term.
#[tokio::test]
#[ignore]
async fn an_amend_that_keeps_the_client_order_id_is_reported_on_its_new_terms() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let client = client();
    let instrument = InstrumentNameExchange::new("BTCUSDT");
    let cid = ClientOrderId::new(format!("terms-{}", chrono::Utc::now().timestamp_millis()));
    // Below the market, inside PERCENT_PRICE_BY_SIDE's bid floor, so it rests.
    let price = (average_price(&instrument).await * dec!(0.7)).round_dp(2);
    let mut stream = client
        .account_stream(&[], std::slice::from_ref(&instrument))
        .await
        .expect("account_stream");

    let placed = client
        .open_order(OrderEvent {
            key: key(&instrument, &cid),
            state: RequestOpen::new(
                Side::Buy,
                Some(price),
                dec!(0.003),
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: false },
            ),
        })
        .await;
    let body = async {
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "the order should rest: {:?}",
            placed.state
        );
        let acknowledged = next_report(&mut stream, &cid, dec!(0.003))
            .await
            .expect("the stream reports the order as placed");
        assert_eq!(acknowledged.reported, ReportedTerms::ALL);

        outside_client()
            .order_amend_keep_priority(
                OrderAmendKeepPriorityParams::builder(instrument.to_string(), dec!(0.002))
                    .orig_client_order_id(Some(cid.0.to_string()))
                    .new_client_order_id(Some(cid.0.to_string()))
                    .build()
                    .expect("amend parameters"),
            )
            .await
            .expect("the amend is accepted");

        let amended = next_report(&mut stream, &cid, dec!(0.002))
            .await
            .expect("the stream reports the order on its new quantity");
        assert_eq!(amended.reported, ReportedTerms::ALL);
        assert_eq!(amended.order.price, Some(price));
        assert_eq!(amended.order.kind, OrderKind::Limit);
        assert_eq!(
            amended.order.time_in_force,
            TimeInForce::GoodUntilCancelled { post_only: false }
        );
        assert!(
            matches!(
                amended.order.state,
                OrderState::Active(ActiveOrderState::Open(_))
            ),
            "{:?}",
            amended.order.state
        );

        let snapshot = client
            .account_snapshot(&[], std::slice::from_ref(&instrument))
            .await
            .expect("account_snapshot");
        let listed = snapshot
            .instruments
            .iter()
            .flat_map(|instrument| &instrument.orders)
            .find(|report| report.order.key.cid == cid)
            .expect("the snapshot lists the order");
        assert_eq!(listed.order.quantity, dec!(0.002));
        assert_eq!(listed.reported, ReportedTerms::ALL);
    };
    // Cancel what was placed whatever the outcome, a failed assertion included, then report it.
    let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(60), body))
        .catch_unwind()
        .await;
    let _ = client
        .cancel_order(OrderEvent {
            key: key(&instrument, &cid),
            state: RequestCancel::new(None),
        })
        .await;
    match outcome {
        Ok(finished) => finished.expect("the test finishes within a minute"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// A limit order placed by [`outside_client`] with self-trade prevention off, so it can trade
/// against another order of this account.
async fn place_self_trading(
    instrument: &InstrumentNameExchange,
    cid: &ClientOrderId,
    side: NewOrderSideEnum,
    quantity: Decimal,
    price: Decimal,
) {
    outside_client()
        .new_order(
            NewOrderParams::builder(instrument.to_string(), side, NewOrderTypeEnum::Limit)
                .time_in_force(Some(NewOrderTimeInForceEnum::Gtc))
                .quantity(Some(quantity))
                .price(Some(price))
                .new_client_order_id(Some(cid.0.to_string()))
                .self_trade_prevention_mode(Some(NewOrderSelfTradePreventionModeEnum::None))
                .build()
                .expect("order parameters"),
        )
        .await
        .unwrap_or_else(|e| panic!("placing {cid} failed: {e}"));
}

/// An amend down to the filled quantity ends the order, in the amend's own report, so the stream
/// reports the order cancelled with what filled. The order is filled in part by a sell of this
/// account's own, on a pair whose testnet spread leaves room for both inside it.
#[tokio::test]
#[ignore]
async fn an_amend_down_to_the_filled_quantity_ends_the_order() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let client = client();
    let instrument = InstrumentNameExchange::new("WBTCUSDT");
    let info = public_get(&format!("exchangeInfo?symbol={instrument}")).await;
    let filter = |kind: &str| {
        info["symbols"][0]["filters"]
            .as_array()
            .expect("exchangeInfo filters")
            .iter()
            .find(|filter| filter["filterType"] == kind)
            .unwrap_or_else(|| panic!("no {kind} filter"))
            .clone()
    };
    let tick = decimal(&filter("PRICE_FILTER"), "tickSize").normalize();
    let unit = decimal(&filter("LOT_SIZE"), "stepSize")
        .normalize()
        .max(dec!(0.001));
    let book = public_get(&format!("ticker/bookTicker?symbol={instrument}")).await;
    let (bid, ask) = (decimal(&book, "bidPrice"), decimal(&book, "askPrice"));
    let price = ((bid + ask) / dec!(2) / tick).floor() * tick;
    assert!(
        bid < price && price < ask,
        "no room inside the spread ({bid}, {ask}) for this test's own orders"
    );
    let stamp = chrono::Utc::now().timestamp_millis();
    let sell = ClientOrderId::new(format!("exhaust-s-{stamp}"));
    let buy = ClientOrderId::new(format!("exhaust-b-{stamp}"));
    let mut stream = client
        .account_stream(&[], std::slice::from_ref(&instrument))
        .await
        .expect("account_stream");

    let body = async {
        place_self_trading(&instrument, &sell, NewOrderSideEnum::Sell, unit, price).await;
        // Fills one unit against the sell above and rests the other two.
        place_self_trading(
            &instrument,
            &buy,
            NewOrderSideEnum::Buy,
            unit * dec!(3),
            price,
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut partly_filled = false;
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, stream.next()).await {
            if let AccountEventKind::OrderSnapshot(snapshot) = &event.kind
                && snapshot.0.order.key.cid == buy
                && let OrderState::Active(ActiveOrderState::Open(open)) = &snapshot.0.order.state
                && open.filled_quantity == unit
            {
                partly_filled = true;
                break;
            }
        }
        assert!(partly_filled, "the stream reports the buy filled in part");

        outside_client()
            .order_amend_keep_priority(
                OrderAmendKeepPriorityParams::builder(instrument.to_string(), unit)
                    .orig_client_order_id(Some(buy.0.to_string()))
                    .new_client_order_id(Some(buy.0.to_string()))
                    .build()
                    .expect("amend parameters"),
            )
            .await
            .expect("the amend is accepted");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut ended = None;
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, stream.next()).await {
            match event.kind {
                AccountEventKind::OrderCancelled(response) if response.key.cid == buy => {
                    ended = Some(response.state);
                    break;
                }
                AccountEventKind::OrderSnapshot(snapshot) if snapshot.0.order.key.cid == buy => {
                    panic!("the amend is reported as a live order: {snapshot:?}")
                }
                _ => {}
            }
        }
        let cancelled = ended
            .expect("the stream reports the order ended")
            .expect("the order ended, not refused");
        assert_eq!(cancelled.filled_quantity, Some(unit));
    };
    // Cancel what was placed whatever the outcome, a failed assertion included, then report it.
    let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(60), body))
        .catch_unwind()
        .await;
    for cid in [&buy, &sell] {
        let _ = client
            .cancel_order(OrderEvent {
                key: key(&instrument, cid),
                state: RequestCancel::new(None),
            })
            .await;
    }
    match outcome {
        Ok(finished) => finished.expect("the test finishes within a minute"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
