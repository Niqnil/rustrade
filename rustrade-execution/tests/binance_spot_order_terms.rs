//! Binance Spot order terms against testnet: what the account stream and an account snapshot
//! state about an order changed outside this client, by a keep-priority amend.
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
    spot::{SpotRestApi, rest_api::OrderAmendKeepPriorityParams},
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
