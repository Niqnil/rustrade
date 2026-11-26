//! Alpaca Execution Client Integration Tests
//!
//! These tests require Alpaca paper trading API credentials.
//!
//! # Status
//!
//! **Tested locally, CI planned (paper trading allowed by Alpaca):**
//! - All order types: market, limit, stop, stop-limit, trailing stop
//! - Bracket orders with TP/SL legs
//! - Account snapshots, balances, positions
//! - Account event streaming via WebSocket
//!
//! Tests are marked `#[ignore]` to avoid CI failures without credentials.
//!
//! # Prerequisites
//!
//! 1. Alpaca paper trading account (https://app.alpaca.markets)
//! 2. Environment variables set (see .env.template):
//!    - ALPACA_API_KEY: Paper trading API key
//!    - ALPACA_SECRET_KEY: Paper trading secret key
//!    - ALPACA_PAPER=true (optional, defaults to true in tests)
//!
//! # Running
//!
//! ```bash
//! # Load env vars from .env (optional, or export manually)
//! source .env
//!
//! # Run all Alpaca integration tests
//! cargo test --test alpaca_integration --features alpaca -- --ignored
//!
//! # Run specific test
//! cargo test --test alpaca_integration --features alpaca test_account_snapshot -- --ignored
//! ```
//!
//! # Market Hours Note
//!
//! US equity market hours are 9:30 AM - 4:00 PM ET (9:30 PM - 4:00 AM SGT next day).
//! Order placement tests work on paper accounts regardless of market hours, but orders
//! for equities placed outside regular/extended hours may sit unfilled until markets open.
//! Crypto orders (e.g., BTC/USD) can be placed 24/7.
//!
//! Tests whose name contains `market_hours` need the US regular session open, and fail when it is
//! closed. Skip them outside it with `-- --ignored --skip market_hours`.

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountEventKind,
    client::{
        ExecutionClient,
        alpaca::{AlpacaClient, AlpacaConfig},
    },
    order::{
        OrderKey, OrderKind, TimeInForce, TrailingOffsetType,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        report::{Replaces, ReportedTerms, UnindexedOrderReport},
        request::RequestOpen,
        state::{ActiveOrderState, InactiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use serial_test::serial;
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing_subscriber::{EnvFilter, fmt};

fn init_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init();
}

fn test_config() -> AlpacaConfig {
    let api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required");
    let secret_key =
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required");

    AlpacaConfig::paper(api_key, secret_key)
}

#[allow(dead_code)] // Reserved for future AAPL equity tests; SPY currently used as the equity fixture
fn aapl_instrument() -> InstrumentNameExchange {
    "AAPL".into()
}

fn spy_instrument() -> InstrumentNameExchange {
    "SPY".into()
}

fn btc_instrument() -> InstrumentNameExchange {
    "BTC/USD".into()
}

/// Wait until [`ExecutionClient::fetch_open_orders`] lists nothing on `instrument`, panicking once
/// 15 seconds have passed.
///
/// Alpaca answers a cancel once it has accepted the request, while the order may still be
/// `pending_cancel` and live. The order tests run one after another on the same symbols, so an
/// order still live when its test ends can get the next test's order refused: a sell against a
/// buy still open is "potential wash trade detected" or "cannot open a short sell while a long
/// buy order is open". Every test that cancels waits here, so it leaves the instrument as it
/// found it. Waiting on the instrument rather than on the one order also covers a bracket's
/// legs, whose ids the placement does not return.
///
/// The list is Alpaca's `status=open`, which its documentation does not define status by status.
/// This relies on `pending_cancel`, which is not a final status, being included. If it were not,
/// the wait would end early and the refusals above would recur, not some new failure. The list
/// also leaves out what `fetch_open_orders` cannot convert, notional orders among them, and
/// these tests place none.
async fn await_no_open_orders(client: &AlpacaClient, instrument: &InstrumentNameExchange) {
    const POLL: Duration = Duration::from_millis(250);
    const DEADLINE: Duration = Duration::from_secs(15);

    let instruments = [instrument.clone()];
    let started = tokio::time::Instant::now();
    loop {
        let listed = client.fetch_open_orders(&instruments).await;
        if matches!(&listed, Ok(orders) if orders.is_empty()) {
            println!("{instrument}: no open orders after {:?}", started.elapsed());
            return;
        }
        // A rate-limited fetch can wait out its own backoff, so report the time actually taken.
        let waited = started.elapsed();
        if waited >= DEADLINE {
            panic!("{instrument} still lists open orders {waited:?} after the cancel: {listed:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Cancel every open order on `instrument`, as best it can, then wait until none is listed: the
/// cleanup of a test that failed before its own cancel, so the shared paper account is left flat.
async fn cancel_open_orders(client: &AlpacaClient, instrument: &InstrumentNameExchange) {
    for order in client
        .fetch_open_orders(std::slice::from_ref(instrument))
        .await
        .unwrap_or_default()
    {
        let cancelled = client
            .cancel_order(rustrade_execution::order::OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::AlpacaBroker,
                    instrument,
                    strategy: order.key.strategy.clone(),
                    cid: order.key.cid.clone(),
                },
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(order.state.id.clone()),
                },
            })
            .await;
        println!("cleanup cancel of {}: {:?}", order.key.cid, cancelled.state);
    }
    await_no_open_orders(client, instrument).await;
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_connection() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    assert_eq!(AlpacaClient::EXCHANGE, ExchangeId::AlpacaBroker);
    println!("AlpacaClient created successfully (paper trading mode)");

    let balances = client.fetch_balances(&[]).await;
    assert!(
        balances.is_ok(),
        "Basic auth check via fetch_balances failed: {:?}",
        balances.err()
    );
    println!("Authentication verified via fetch_balances");
}

// ============================================================================
// Account Snapshot Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_account_snapshot() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let result = client.account_snapshot(&assets, &instruments).await;

    assert!(
        result.is_ok(),
        "account_snapshot failed: {:?}",
        result.err()
    );

    let snapshot = result.unwrap();
    println!("Exchange: {:?}", snapshot.exchange);
    println!("Balances: {}", snapshot.balances.len());

    for balance in &snapshot.balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }

    println!("Instruments: {}", snapshot.instruments.len());
    for snap in &snapshot.instruments {
        println!(
            "  {}: {} open orders, position {:?}",
            snap.instrument,
            snap.orders.len(),
            snap.position
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_fetch_balances() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let result = client.fetch_balances(&assets).await;

    assert!(result.is_ok(), "fetch_balances failed: {:?}", result.err());

    let balances = result.unwrap();
    println!("Fetched {} balance(s)", balances.len());

    let has_usd = balances
        .iter()
        .any(|b| b.asset.name().as_str().eq_ignore_ascii_case("usd"));
    assert!(has_usd, "Expected USD balance in paper account");

    for balance in &balances {
        println!(
            "  {}: total={}, free={}",
            balance.asset, balance.balance.total, balance.balance.free
        );
    }
}

// ============================================================================
// Shortability Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_shortability() {
    use rust_decimal::Decimal;
    use rustrade_execution::{client::ShortabilityClient, error::ApiError, error::ClientError};

    init_logging();
    let client = AlpacaClient::new(test_config());

    let spy = client
        .fetch_shortability(&spy_instrument())
        .await
        .expect("SPY shortability");
    println!("SPY: {spy:?}");
    assert_eq!(spy.shortable, Some(true), "SPY is shortable");
    assert_eq!(spy.easy_to_borrow, Some(true), "SPY is easy to borrow");
    assert_eq!(
        spy.fee_rate,
        Some(Decimal::ZERO),
        "Alpaca charges no ETB fee"
    );
    assert_eq!(spy.available, None);

    // The pair's slash must reach Alpaca escaped, as one path segment.
    let btc = client
        .fetch_shortability(&btc_instrument())
        .await
        .expect("BTC/USD shortability");
    println!("BTC/USD: {btc:?}");
    // Alpaca lends no crypto. Its `borrow_status` is for US equities only, but a crypto asset
    // still carries the `easy_to_borrow` flag, false, which is reported as given.
    assert_eq!(btc.shortable, Some(false), "Alpaca does not short crypto");
    assert_eq!(btc.fee_rate, None);

    let unknown = client
        .fetch_shortability(&InstrumentNameExchange::new("NOSUCHSYMBOLXYZ"))
        .await;
    println!("unknown: {unknown:?}");
    assert!(
        matches!(
            unknown,
            Err(ClientError::Api(ApiError::InstrumentInvalid(..)))
        ),
        "{unknown:?}"
    );
}

#[tokio::test]
#[ignore]
async fn test_fetch_options_late_close_underlyings() {
    use rustrade_instrument::asset::name::AssetNameExchange;

    init_logging();
    let client = AlpacaClient::new(test_config());

    let symbols = client
        .fetch_options_late_close_underlyings()
        .await
        .expect("late-close underlyings");
    println!("{} late-close underlyings: {symbols:?}", symbols.len());

    // Alpaca support names SPY, QQQ and IWM as closing at 16:15 ET.
    for symbol in ["SPY", "QQQ", "IWM"] {
        assert!(
            symbols.contains(&AssetNameExchange::new(symbol)),
            "{symbol} closes late: {symbols:?}"
        );
    }
    assert!(
        !symbols.contains(&AssetNameExchange::new("AAPL")),
        "a single stock's options close at 16:00 ET: {symbols:?}"
    );
    assert!(symbols.is_sorted(), "{symbols:?}");
}

// ============================================================================
// Open Orders Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_open_orders() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instruments: Vec<InstrumentNameExchange> = vec![];
    let result = client.fetch_open_orders(&instruments).await;

    assert!(
        result.is_ok(),
        "fetch_open_orders failed: {:?}",
        result.err()
    );

    let orders = result.unwrap();
    println!("Open orders: {}", orders.len());
    for order in &orders {
        println!(
            "  {:?} {} {} @ {:?}",
            order.side, order.quantity, order.key.instrument, order.price
        );
    }
}

/// The activities endpoint answers both reads built on it: fills, and the non-trade activities
/// read as cash flows. A paper account may hold neither, so this checks that each read succeeds
/// and its records are in the span, not that there are any.
#[tokio::test]
#[ignore]
async fn test_fetch_trades_and_cash_flows() {
    init_logging();

    let client = AlpacaClient::new(test_config());
    let end = chrono::Utc::now();
    let start = end - chrono::TimeDelta::days(30);

    let trades = client
        .fetch_trades(start, end, &[])
        .await
        .expect("fetch_trades");
    println!(
        "Fills, 30 days: {} (resume: {:?})",
        trades.trades.len(),
        trades.resume
    );
    assert!(
        trades
            .trades
            .iter()
            .all(|trade| (start..=end).contains(&trade.time_exchange))
    );

    let flows = client
        .fetch_cash_flows(start, end)
        .await
        .expect("fetch_cash_flows");
    println!(
        "Cash flows, 30 days: {} (resume: {:?})",
        flows.flows.len(),
        flows.resume
    );
    for flow in &flows.flows {
        println!("  {:?} {} {}", flow.kind, flow.amount, flow.asset);
        assert!((start..=end).contains(&flow.time_exchange));
        assert!(flow.id.is_some());
    }
}

/// The activities endpoint accepts the option event types. A paper account may hold no option
/// events, so this checks that the read succeeds and its trades are in the span, not that there
/// are any.
#[tokio::test]
#[ignore]
async fn test_fetch_option_events() {
    init_logging();

    let client = AlpacaClient::new(test_config());
    let end = chrono::Utc::now();
    let start = end - chrono::TimeDelta::days(30);

    let read = client
        .fetch_option_events(start, end)
        .await
        .expect("fetch_option_events");
    println!(
        "Option events, 30 days: {} (resume: {:?})",
        read.trades.len(),
        read.resume
    );
    for trade in &read.trades {
        println!(
            "  {} {} {} {} @ {}",
            trade.origin, trade.instrument, trade.side, trade.quantity, trade.price
        );
        assert!((start..=end).contains(&trade.time_exchange));
    }
}

/// A GET to Alpaca's paper trading API, authenticated, without going through the client: its
/// status and its body as JSON (`Null` when it is not JSON).
async fn paper_get(path: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required");
    let secret_key =
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required");
    let response = reqwest::Client::new()
        .get(format!("https://paper-api.alpaca.markets{path}"))
        .header("APCA-API-KEY-ID", api_key)
        .header("APCA-API-SECRET-KEY", secret_key)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("send");
    let status = response.status();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// The symbol of a listed SPY call expiring on `expiry` (any day from `expiry` on when `from_on`),
/// which the account holds no position in; `None` when Alpaca lists none.
async fn unheld_spy_call(
    expiry: chrono::NaiveDate,
    from_on: bool,
) -> Option<InstrumentNameExchange> {
    let dates = if from_on {
        format!("expiration_date_gte={expiry}")
    } else {
        format!("expiration_date={expiry}")
    };
    let (status, listed) = paper_get(&format!(
        "/v2/options/contracts?underlying_symbols=SPY&status=active&type=call&{dates}&limit=50"
    ))
    .await;
    assert!(status.is_success(), "contract listing: {status} {listed}");
    for symbol in listed["option_contracts"]
        .as_array()
        .expect("option_contracts")
        .iter()
        .filter_map(|listed| listed["symbol"].as_str())
    {
        match paper_get(&format!("/v2/positions/{symbol}")).await {
            (reqwest::StatusCode::NOT_FOUND, _) => {
                return Some(InstrumentNameExchange::new(symbol));
            }
            (status, _) if status.is_success() => continue,
            (status, body) => panic!("position read for {symbol}: {status} {body}"),
        }
    }
    None
}

/// What Alpaca refuses a do-not-exercise with when the account holds no position, so this changes
/// nothing and runs at any hour. Alpaca takes one only on the contract's expiration day, until a
/// deadline that day, and judges that before the position:
/// - a contract expiring after today: not its expiry day, dated from the contract;
/// - a contract Alpaca does not list: an invalid instrument;
/// - on an expiration day, a contract expiring today: no position before the deadline, and past
///   the deadline after it.
#[tokio::test]
#[ignore]
async fn test_instruct_exercise_without_a_position() {
    use chrono_tz::America::New_York;
    use rustrade_execution::{
        client::OptionExerciseClient,
        error::{ApiError, ClientError, ExerciseRefusal},
        exercise::{ExerciseInstruction, ExerciseQuantity},
    };

    init_logging();
    let client = AlpacaClient::new(test_config());
    let lapse = ExerciseInstruction::Lapse {
        contracts: ExerciseQuantity::WholePosition,
    };
    let now = chrono::Utc::now();
    let today = now.with_timezone(&New_York).date_naive();

    let later = unheld_spy_call(today + chrono::TimeDelta::days(1), true)
        .await
        .expect("a listed SPY call expiring after today, not held");
    let not_today = client.instruct_exercise(&later, lapse).await;
    println!("{later}, expiring later: {not_today:?}");
    let ClientError::Api(ApiError::ExerciseRefused(ExerciseRefusal::NotExpiryDay { expiry_day })) =
        not_today.expect_err("refused")
    else {
        panic!("expected NotExpiryDay for {later}");
    };
    assert!(
        later
            .name()
            .contains(&expiry_day.format("%y%m%d").to_string()),
        "{expiry_day} is {later}'s expiry"
    );

    // Well formed, but no SPY call has a $9,999 strike.
    let unlisted = InstrumentNameExchange::new("SPY301220C09999000");
    let not_listed = client.instruct_exercise(&unlisted, lapse).await;
    println!("{unlisted}, not listed: {not_listed:?}");
    assert!(
        matches!(
            not_listed,
            Err(ClientError::Api(ApiError::InstrumentInvalid(..)))
        ),
        "{not_listed:?}"
    );

    let Some(expiring) = unheld_spy_call(today, false).await else {
        println!("No SPY call expires today ({today}): the expiry-day answers are not checked");
        return;
    };
    let on_the_day = client.instruct_exercise(&expiring, lapse).await;
    println!("{expiring}, expiring today, not held: {on_the_day:?}");
    match on_the_day.expect_err("refused") {
        ClientError::Api(ApiError::ExerciseRefused(ExerciseRefusal::NoPosition)) => {}
        ClientError::Api(ApiError::ExerciseRefused(ExerciseRefusal::PastDeadline { deadline })) => {
            println!("Deadline: {}", deadline.with_timezone(&New_York));
            assert!(deadline <= chrono::Utc::now(), "the deadline has passed");
            assert_eq!(deadline.with_timezone(&New_York).date_naive(), today);
        }
        other => panic!("expected NoPosition or PastDeadline for {expiring}, got {other:?}"),
    }
}

// ============================================================================
// Order Lifecycle Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_limit_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-order-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen::new(
        Side::Buy,
        Some(dec!(1.00)),
        dec!(1),
        OrderKind::Limit,
        TimeInForce::GoodUntilEndOfDay,
    );

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing limit order: BUY 1 SPY @ $1.00 (won't fill - price too low)");

    let response = client.open_order(open_request).await;

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Side: {:?}", response.side);
            println!("  Quantity: {}", response.quantity);
            println!("  Price: {:?}", response.price);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(InactiveOrderState::FullyFilled(_)) => {
            panic!("Unexpected full fill at $1.00 - market moved unexpectedly");
        }
        OrderState::Inactive(e) => {
            panic!("Order rejected or expired: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

/// Place `request` on `instrument`, then check the account snapshot lists it under the client id
/// it was placed with and declares the instrument's list complete; once the order is cancelled,
/// the complete list must no longer show it. Together these are what let the engine retire an
/// order that ended while the account stream was down. The instrument is requested by the name
/// the order is placed under, so this also checks Alpaca reports that name back unchanged.
async fn assert_snapshot_lists_the_order_then_drops_it(
    instrument: InstrumentNameExchange,
    request: RequestOpen,
) {
    let client = AlpacaClient::new(test_config());
    let instruments = [instrument.clone()];
    let cid = ClientOrderId::new(format!(
        "test-listed-{}",
        chrono::Utc::now().timestamp_millis()
    ));
    let key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: StrategyId::new("test-strategy"),
        cid: cid.clone(),
    };

    let response = client
        .open_order(rustrade_execution::order::OrderEvent {
            key: key.clone(),
            state: request,
        })
        .await;
    let OrderState::Active(ActiveOrderState::Open(open)) = &response.state else {
        panic!(
            "{instrument} limit order did not rest: {:?}",
            response.state
        );
    };

    let listed = client
        .account_snapshot(&[], &instruments)
        .await
        .expect("account_snapshot failed");
    let snapshot = listed
        .instruments
        .iter()
        .find(|snapshot| snapshot.instrument == instrument)
        .expect("a requested instrument always has an entry");
    assert!(
        snapshot.orders_complete,
        "{instrument}'s order list is not declared complete"
    );
    assert!(
        snapshot
            .orders
            .iter()
            .any(|order| order.order.key.cid == cid),
        "the resting {instrument} order is not listed under its client id {cid}: {:?}",
        snapshot.orders
    );

    let cancelled = client
        .cancel_order(rustrade_execution::order::OrderEvent {
            key,
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(open.id.clone()),
            },
        })
        .await;
    assert!(
        cancelled.state.is_ok(),
        "Cancel rejected: {:?}",
        cancelled.state
    );
    await_no_open_orders(&client, &instrument).await;

    // Alpaca's order listing is eventually consistent: one empty read does not stop the next
    // from still returning the cancelled order, so poll the snapshot itself. Like
    // `await_no_open_orders`, a failed read is retried until the deadline.
    const POLL: Duration = Duration::from_millis(250);
    const DEADLINE: Duration = Duration::from_secs(15);
    let started = tokio::time::Instant::now();
    loop {
        let last = match client.account_snapshot(&[], &instruments).await {
            Ok(listed) => {
                let snapshot = listed
                    .instruments
                    .iter()
                    .find(|snapshot| snapshot.instrument == instrument)
                    .expect("a requested instrument always has an entry");
                assert!(
                    snapshot.orders_complete,
                    "{instrument}'s order list is not declared complete"
                );
                if snapshot
                    .orders
                    .iter()
                    .all(|order| order.order.key.cid != cid)
                {
                    return;
                }
                format!("still listed: {:?}", snapshot.orders)
            }
            Err(error) => format!("account_snapshot failed: {error:?}"),
        };
        let waited = started.elapsed();
        assert!(
            waited < DEADLINE,
            "the cancelled {instrument} order did not drop out {waited:?} after the cancel; {last}"
        );
        tokio::time::sleep(POLL).await;
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_snapshot_lists_an_equity_order_under_its_client_id_and_complete() {
    init_logging();
    assert_snapshot_lists_the_order_then_drops_it(
        spy_instrument(),
        RequestOpen::new(
            Side::Buy,
            Some(dec!(1.00)),
            dec!(1),
            OrderKind::Limit,
            TimeInForce::GoodUntilEndOfDay,
        ),
    )
    .await;
}

/// Alpaca names a crypto pair with a slash (`BTC/USD`), unlike an equity ticker.
#[tokio::test]
#[ignore]
#[serial]
async fn test_snapshot_lists_a_crypto_order_under_its_client_id_and_complete() {
    init_logging();
    assert_snapshot_lists_the_order_then_drops_it(
        btc_instrument(),
        RequestOpen::new(
            Side::Buy,
            Some(dec!(1000.00)),
            dec!(0.01),
            OrderKind::Limit,
            TimeInForce::GoodUntilCancelled { post_only: false },
        ),
    )
    .await;
}

/// The terms an order rests on, as `(quantity, price, kind, time in force)`.
fn terms(
    report: &UnindexedOrderReport,
) -> (
    rust_decimal::Decimal,
    Option<rust_decimal::Decimal>,
    OrderKind,
    TimeInForce,
) {
    let order = &report.order;
    (order.quantity, order.price, order.kind, order.time_in_force)
}

/// What the account stream and an account snapshot report about a resting order placed with
/// `request`: the stream's first snapshot of it within ten seconds, if any, and the snapshot's
/// listing of it. The order is cancelled before either is returned, so a failed assertion on them
/// leaves nothing resting.
async fn reports_of_a_resting_order(
    instrument: &InstrumentNameExchange,
    request: RequestOpen,
) -> (Option<UnindexedOrderReport>, Option<UnindexedOrderReport>) {
    let client = AlpacaClient::new(test_config());
    let instruments = [instrument.clone()];
    let mut stream = client
        .account_stream(&[], &instruments)
        .await
        .expect("account_stream failed");
    let cid = ClientOrderId::new(format!(
        "test-terms-{}",
        chrono::Utc::now().timestamp_millis()
    ));
    let key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument,
        strategy: StrategyId::new("test-strategy"),
        cid: cid.clone(),
    };

    let response = client
        .open_order(rustrade_execution::order::OrderEvent {
            key: key.clone(),
            state: request,
        })
        .await;
    let OrderState::Active(ActiveOrderState::Open(open)) = &response.state else {
        panic!(
            "{instrument} limit order did not rest: {:?}",
            response.state
        );
    };

    let streamed = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = stream.next().await {
            if let AccountEventKind::OrderSnapshot(snapshot) = event.kind
                && snapshot.0.order.key.cid == cid
            {
                return Some(snapshot.0);
            }
        }
        None
    })
    .await
    .ok()
    .flatten();
    let listed = client
        .account_snapshot(&[], &instruments)
        .await
        .expect("account_snapshot failed")
        .instruments
        .into_iter()
        .flat_map(|snapshot| snapshot.orders)
        .find(|order| order.order.key.cid == cid);

    let cancelled = client
        .cancel_order(rustrade_execution::order::OrderEvent {
            key,
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(open.id.clone()),
            },
        })
        .await;
    assert!(
        cancelled.state.is_ok(),
        "Cancel rejected: {:?}",
        cancelled.state
    );
    await_no_open_orders(&client, instrument).await;
    (streamed, listed)
}

/// A resting crypto order is reported on the terms it was placed with, every one stated, by the
/// account stream and by an account snapshot alike. Crypto trades around the clock, so the
/// stream acknowledges it at any hour.
#[tokio::test]
#[ignore]
#[serial]
async fn test_a_resting_crypto_order_states_its_terms() {
    init_logging();
    let request = RequestOpen::new(
        Side::Buy,
        Some(dec!(1000.00)),
        dec!(0.01),
        OrderKind::Limit,
        TimeInForce::GoodUntilCancelled { post_only: false },
    );
    let expected = (
        request.quantity,
        request.price,
        request.kind,
        request.time_in_force,
    );
    let (streamed, listed) = reports_of_a_resting_order(&btc_instrument(), request).await;

    let streamed = streamed.expect("the stream reports the order");
    assert_eq!(streamed.reported, ReportedTerms::ALL, "{streamed:?}");
    assert_eq!(terms(&streamed), expected);
    let listed = listed.expect("the account snapshot lists the order");
    assert_eq!(listed.reported, ReportedTerms::ALL, "{listed:?}");
    assert_eq!(terms(&listed), expected);
}

/// A resting equity order is listed on the terms it was placed with, every one stated. Outside
/// market hours it waits for the session, so the stream's report of it, if any, is checked only
/// when it comes.
#[tokio::test]
#[ignore]
#[serial]
async fn test_a_resting_equity_order_states_its_terms() {
    init_logging();
    let request = RequestOpen::new(
        Side::Buy,
        Some(dec!(1.00)),
        dec!(1),
        OrderKind::Limit,
        TimeInForce::GoodUntilCancelled { post_only: false },
    );
    let expected = (
        request.quantity,
        request.price,
        request.kind,
        request.time_in_force,
    );
    let (streamed, listed) = reports_of_a_resting_order(&spy_instrument(), request).await;

    match streamed {
        Some(streamed) => {
            assert_eq!(streamed.reported, ReportedTerms::ALL, "{streamed:?}");
            assert_eq!(terms(&streamed), expected);
        }
        None => println!("the stream did not report the equity order within ten seconds"),
    }
    let listed = listed.expect("the account snapshot lists the order");
    assert_eq!(listed.reported, ReportedTerms::ALL, "{listed:?}");
    assert_eq!(terms(&listed), expected);
}

/// Replace a resting buy limit order on `instrument` at Alpaca, by a `PATCH` to `new_price` this
/// client does not send itself, and check the order goes on as its replacement: the account
/// stream reports the replacement linked to the order it replaces, by its order id and client
/// order id, then the old order ended as cancelled, and an account snapshot lists the
/// replacement linked by order id. `price` must be far enough from the market that the order
/// rests. Cancels what it placed, also when a step fails.
async fn assert_a_replaced_order_is_followed(
    instrument: InstrumentNameExchange,
    quantity: Decimal,
    price: Decimal,
    new_price: &str,
    time_in_force: TimeInForce,
) {
    let config = test_config();
    let api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required");
    let secret_key =
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required");
    let client = AlpacaClient::new(config.clone());
    let instruments = [instrument.clone()];
    let mut stream = client
        .account_stream(&[], &instruments)
        .await
        .expect("account_stream failed");
    let old_cid = ClientOrderId::new(format!(
        "test-replace-{}",
        chrono::Utc::now().timestamp_millis()
    ));
    let key = |cid: &ClientOrderId| OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: StrategyId::new("test-strategy"),
        cid: cid.clone(),
    };

    let response = client
        .open_order(rustrade_execution::order::OrderEvent {
            key: key(&old_cid),
            state: RequestOpen::new(
                Side::Buy,
                Some(price),
                quantity,
                OrderKind::Limit,
                time_in_force,
            ),
        })
        .await;
    // From here until the order is cancelled, a failure cancels what rests before panicking:
    // even an answer that is not `Open` may leave an order resting at Alpaca.
    let OrderState::Active(ActiveOrderState::Open(open)) = &response.state else {
        eprintln!(
            "the {instrument} limit order did not rest: {:?}",
            response.state
        );
        cancel_open_orders(&client, &instrument).await;
        panic!(
            "the {instrument} limit order did not rest: {:?}",
            response.state
        );
    };
    let VenueOrderId::Assigned(old_id) = &open.id else {
        eprintln!("Alpaca names the order it placed: {open:?}");
        cancel_open_orders(&client, &instrument).await;
        panic!("Alpaca names the order it placed: {open:?}");
    };

    let http = reqwest::Client::new();
    let order_url = format!("{}/v2/orders/{old_id}", config.rest_base_url());
    let replace = async {
        // Alpaca refuses to replace an order it has not yet acknowledged `new`; an equity order
        // can stay `accepted` or `pending_new` for a moment after it is placed.
        let mut acknowledged = false;
        for _ in 0..40 {
            let order: serde_json::Value = http
                .get(&order_url)
                .header("APCA-API-KEY-ID", &api_key)
                .header("APCA-API-SECRET-KEY", &secret_key)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| format!("the order could not be read: {error}"))?
                .json()
                .await
                .map_err(|error| format!("the order read is not JSON: {error}"))?;
            if !matches!(order["status"].as_str(), Some("accepted" | "pending_new")) {
                acknowledged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if !acknowledged {
            return Err("Alpaca did not acknowledge the order `new` within ten seconds".to_owned());
        }
        let replaced: serde_json::Value = http
            .patch(&order_url)
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &secret_key)
            .json(&serde_json::json!({ "limit_price": new_price }))
            .send()
            .await
            .map_err(|error| format!("the replace request was not sent: {error}"))?
            .error_for_status()
            .map_err(|error| format!("Alpaca refused the replace: {error}"))?
            .json()
            .await
            .map_err(|error| format!("the replace answer is not JSON: {error}"))?;
        let field = |name: &str| {
            replaced[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("the replace answer has no `{name}`: {replaced}"))
        };
        Ok::<_, String>((
            OrderId::new(field("id")?),
            ClientOrderId::new(field("client_order_id")?),
        ))
    };
    let (new_id, new_cid) = match replace.await {
        Ok(ids) => ids,
        Err(error) => {
            // Printed first, so a cleanup that panics too does not hide it.
            eprintln!("{error}");
            cancel_open_orders(&client, &instrument).await;
            panic!("{error}");
        }
    };

    // The stream's reports of either order until the old one has ended, within ten seconds.
    let streamed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut reports = Vec::new();
        while let Some(event) = stream.next().await {
            let AccountEventKind::OrderSnapshot(snapshot) = event.kind else {
                continue;
            };
            let report = snapshot.0;
            if report.order.key.cid != old_cid && report.order.key.cid != new_cid {
                continue;
            }
            let ended = report.order.key.cid == old_cid
                && matches!(report.order.state, OrderState::Inactive(_));
            reports.push(report);
            if ended {
                break;
            }
        }
        reports
    })
    .await
    .unwrap_or_default();
    let listed = client
        .account_snapshot(&[], &instruments)
        .await
        .map(|snapshot| {
            snapshot
                .instruments
                .into_iter()
                .flat_map(|snapshot| snapshot.orders)
                .find(|order| order.order.key.cid == new_cid)
        });

    let cancelled = client
        .cancel_order(rustrade_execution::order::OrderEvent {
            key: key(&new_cid),
            state: rustrade_execution::order::request::RequestCancel {
                id: Some(VenueOrderId::Assigned(new_id.clone())),
            },
        })
        .await;
    if cancelled.state.is_err() {
        eprintln!("Cancel rejected: {:?}", cancelled.state);
        cancel_open_orders(&client, &instrument).await;
        panic!("Cancel rejected: {:?}", cancelled.state);
    }
    await_no_open_orders(&client, &instrument).await;

    let linked = streamed
        .iter()
        .position(|report| {
            report.order.key.cid == new_cid
                && report.replaces == Some(Replaces::new(old_id.clone()).with_cid(old_cid.clone()))
        })
        .unwrap_or_else(|| panic!("the replacement is reported linked: {streamed:#?}"));
    let ended = streamed
        .iter()
        .position(|report| {
            report.order.key.cid == old_cid
                && matches!(
                    report.order.state,
                    OrderState::Inactive(InactiveOrderState::Cancelled(_))
                )
        })
        .unwrap_or_else(|| panic!("the old order is reported ended: {streamed:#?}"));
    assert!(linked < ended, "the replacement comes first: {streamed:#?}");
    let listed = listed
        .expect("account_snapshot failed")
        .expect("the account snapshot lists the replacement");
    assert_eq!(listed.replaces, Some(Replaces::new(old_id.clone())));
}

/// A replaced crypto order is followed (see [`assert_a_replaced_order_is_followed`]). Alpaca
/// reports the replacement on the stream itself. Crypto, so it runs at any hour.
#[tokio::test]
#[ignore]
#[serial]
async fn test_a_replaced_crypto_order_is_followed() {
    init_logging();
    assert_a_replaced_order_is_followed(
        btc_instrument(),
        dec!(0.01),
        dec!(1000.00),
        "1001",
        TimeInForce::GoodUntilCancelled { post_only: false },
    )
    .await;
}

/// A replaced equity order is followed (see [`assert_a_replaced_order_is_followed`]). In regular
/// hours Alpaca streams only `replaced` for the old order and nothing for the replacement, so
/// the client reads the replacement over REST before it reports the old order ended.
///
/// Needs the US regular session open, and fails at once when it is closed: an equity order placed
/// outside it waits `accepted`, which Alpaca refuses to replace. The `market_hours` suffix lets a
/// run at any hour skip it (`--skip market_hours`).
#[tokio::test]
#[ignore]
#[serial]
async fn test_a_replaced_equity_order_is_followed_market_hours() {
    init_logging();
    require_regular_session().await;
    assert_a_replaced_order_is_followed(
        spy_instrument(),
        dec!(1),
        dec!(1.00),
        "1.01",
        TimeInForce::GoodUntilEndOfDay,
    )
    .await;
}

/// Panic unless Alpaca's clock says the US regular session is open.
async fn require_regular_session() {
    let api_key = std::env::var("ALPACA_API_KEY").expect("ALPACA_API_KEY env var required");
    let secret_key =
        std::env::var("ALPACA_SECRET_KEY").expect("ALPACA_SECRET_KEY env var required");
    let clock: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v2/clock", test_config().rest_base_url()))
        .header("APCA-API-KEY-ID", api_key)
        .header("APCA-API-SECRET-KEY", secret_key)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .expect("Alpaca's clock is readable")
        .json()
        .await
        .expect("Alpaca's clock is JSON");
    assert_eq!(
        clock["is_open"],
        serde_json::Value::Bool(true),
        "this test needs the US regular session open; Alpaca's clock: {clock}"
    );
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_crypto_limit_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = btc_instrument();
    let strategy = StrategyId::new("test-crypto");
    let order_cid = ClientOrderId::new(format!(
        "test-btc-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen::new(
        Side::Buy,
        Some(dec!(1000.00)),
        dec!(0.01),
        OrderKind::Limit,
        TimeInForce::GoodUntilCancelled { post_only: false },
    );

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing crypto limit order: BUY 0.01 BTC/USD @ $1000 (won't fill, $10 value)");

    let response = client.open_order(open_request).await;

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Crypto order placed successfully!");
            println!("  Exchange Order ID: {:?}", open_state.id);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling crypto order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(_) => println!("Crypto order canceled successfully!"),
                Err(e) => panic!("Cancel rejected: {:?}", e),
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(e) => {
            panic!("Crypto order rejected: {:?}", e);
        }
        _ => {
            panic!("Unexpected order state: {:?}", response.state);
        }
    }
}

// ============================================================================
// Account Stream Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_account_stream() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    let stream_result = client.account_stream(&assets, &instruments).await;

    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );

    let mut stream = stream_result.unwrap();

    println!("Account stream connected. Waiting for events (10 second timeout)...");
    println!("(Alpaca sends heartbeats every ~35s, so we may not see events in 10s)");

    let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut count = 0;
        while let Some(event) = stream.next().await {
            println!("Event received: {:?}", event.kind);
            count += 1;
            if count >= 3 {
                break;
            }
        }
        count
    })
    .await;

    match timeout_result {
        Ok(count) => {
            println!(
                "Received {} event(s) before stream ended or limit reached",
                count
            );
        }
        Err(_) => {
            println!("Timeout reached (expected - Alpaca heartbeat is 35s)");
            println!("Stream connection verified successfully!");
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_account_stream_with_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let assets: Vec<AssetNameExchange> = vec![];
    let instruments: Vec<InstrumentNameExchange> = vec![];

    println!("Starting account stream...");
    let stream_result = client.account_stream(&assets, &instruments).await;
    assert!(
        stream_result.is_ok(),
        "account_stream failed: {:?}",
        stream_result.err()
    );
    let mut stream = stream_result.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-stream");
    let order_cid = ClientOrderId::new(format!(
        "stream-test-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    let request_open = RequestOpen::new(
        Side::Buy,
        Some(dec!(1.00)),
        dec!(1),
        OrderKind::Limit,
        TimeInForce::GoodUntilEndOfDay,
    );

    println!("Placing order to trigger stream events...");
    let response = client
        .open_order(rustrade_execution::order::OrderEvent {
            key: order_key,
            state: request_open,
        })
        .await;

    let order_id = match &response.state {
        OrderState::Active(ActiveOrderState::Open(o)) => Some(o.id.clone()),
        _ => None,
    };

    println!("Waiting for order events on stream (5s timeout)...");
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            println!("Stream event: {:?}", event.kind);
            events.push(event);
            if events.len() >= 2 {
                break;
            }
        }
        events
    })
    .await;

    match events {
        Ok(evts) => {
            println!("Received {} event(s) from stream", evts.len());
            for evt in &evts {
                match &evt.kind {
                    AccountEventKind::OrderSnapshot(snapshot) => {
                        let order = snapshot.value();
                        println!(
                            "  OrderSnapshot: {:?} {} {}",
                            order.order.side, order.order.quantity, order.order.key.instrument
                        );
                    }
                    AccountEventKind::OrderCancelled(resp) => {
                        println!("  OrderCancelled: {:?}", resp.state);
                    }
                    AccountEventKind::Trade(trade) => {
                        println!("  Trade: {} @ {}", trade.quantity, trade.price);
                    }
                    _ => {
                        println!("  Other event type");
                    }
                }
            }
        }
        Err(_) => {
            println!(
                "Timeout waiting for stream events (may be normal if order wasn't acknowledged yet)"
            );
        }
    }

    if let Some(oid) = order_id {
        println!("Cleaning up: canceling test order...");
        let cancel_key = OrderKey {
            exchange: ExchangeId::AlpacaBroker,
            instrument: &instrument,
            strategy,
            cid: order_cid,
        };
        let _ = client
            .cancel_order(rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel { id: Some(oid) },
            })
            .await;
        await_no_open_orders(&client, &instrument).await;
        println!("Cleanup complete");
    }
}

// ============================================================================
// Stop Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_stop_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-stop-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Stop order: triggers a market order when SPY drops to $1.00
    // This price is unrealistically low so the order will never trigger
    let request_open = RequestOpen::new(
        Side::Sell,
        None,
        dec!(1),
        OrderKind::Stop {
            trigger_price: dec!(1.00),
        },
        TimeInForce::GoodUntilEndOfDay,
    );

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing stop order: SELL 1 SPY @ stop $1.00 (won't trigger - price too low)");

    let response = client.open_order(open_request).await;

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Kind: {:?}", response.kind);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling stop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Stop order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(e) => {
            panic!("Stop order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_trailing_stop_order() {
    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-strategy");
    let order_cid = ClientOrderId::new(format!(
        "test-trail-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    let order_key = OrderKey {
        exchange: ExchangeId::AlpacaBroker,
        instrument: &instrument,
        strategy: strategy.clone(),
        cid: order_cid.clone(),
    };

    // Trailing stop order with 5% trail distance
    let request_open = RequestOpen::new(
        Side::Sell,
        None,
        dec!(1),
        OrderKind::TrailingStop {
            offset: dec!(5.0),
            offset_type: TrailingOffsetType::Percentage,
            activation_price: None,
        },
        TimeInForce::GoodUntilEndOfDay,
    );

    let open_request = rustrade_execution::order::OrderEvent {
        key: order_key.clone(),
        state: request_open,
    };

    println!("Placing trailing stop order: SELL 1 SPY with 5% trail");

    let response = client.open_order(open_request).await;

    match &response.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Trailing stop order placed successfully!");
            println!("  Client Order ID: {}", response.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Kind: {:?}", response.kind);

            tokio::time::sleep(Duration::from_millis(500)).await;

            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: response.key.strategy.clone(),
                cid: response.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling trailing stop order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Trailing stop order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(e) => {
            panic!("Trailing stop order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

// ============================================================================
// Bracket Order Tests
// ============================================================================

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_bracket_order_with_stop() {
    use rustrade_execution::client::alpaca::{AlpacaBracketOrderRequest, AlpacaBracketOrderResult};

    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-bracket");
    let order_cid = ClientOrderId::new(format!(
        "test-bracket-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    // Bracket order with stop-loss as a simple stop order
    // Entry at $100 (way below market ~$580+, won't fill), TP at $120, SL at $90
    let request = AlpacaBracketOrderRequest::new(
        instrument.clone(),
        strategy.clone(),
        order_cid.clone(),
        Side::Buy,
        dec!(1),
        dec!(100.00),
        dec!(120.00),
        dec!(90.00),
        TimeInForce::GoodUntilCancelled { post_only: false },
    );

    println!("Placing bracket order: BUY 1 SPY @ $100 entry, $120 TP, $90 SL (stop)");

    let result: AlpacaBracketOrderResult = client.open_bracket_order(request).await;

    match &result.parent.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Bracket order placed successfully!");
            println!("  Client Order ID: {}", result.parent.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Entry Price: {:?}", result.parent.price);

            // Verify the order is on the book
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Fetch open orders to see all legs
            let instruments: Vec<InstrumentNameExchange> = vec![instrument.clone()];
            let open_orders = client.fetch_open_orders(&instruments).await;
            match open_orders {
                Ok(orders) => {
                    println!("Open orders after bracket placement: {}", orders.len());
                    for order in &orders {
                        println!(
                            "  {:?} {} {} @ {:?} (kind: {:?})",
                            order.side,
                            order.quantity,
                            order.key.instrument,
                            order.price,
                            order.kind
                        );
                    }
                    // Bracket order should create 3 legs (entry, TP, SL)
                    // but we only assert >= 1 since entry might be the only one visible before fill
                    assert!(
                        !orders.is_empty(),
                        "Expected at least the entry order to be visible"
                    );
                }
                Err(e) => {
                    println!("Warning: fetch_open_orders failed: {:?}", e);
                }
            }

            // Cancel the bracket order (canceling parent cancels all legs)
            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: result.parent.key.strategy.clone(),
                cid: result.parent.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling bracket order...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Bracket order canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(e) => {
            panic!("Bracket order rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_place_and_cancel_bracket_order_with_stop_limit() {
    use rustrade_execution::client::alpaca::{AlpacaBracketOrderRequest, AlpacaBracketOrderResult};

    init_logging();

    let config = test_config();
    let client = AlpacaClient::new(config);

    let instrument = spy_instrument();
    let strategy = StrategyId::new("test-bracket-sl");
    let order_cid = ClientOrderId::new(format!(
        "test-bracket-sl-{}",
        chrono::Utc::now().timestamp_millis()
    ));

    // Bracket order with stop-loss as a stop-limit order
    // Entry at $100 (way below market, won't fill), TP at $120, SL triggers at $90, limit at $88
    let request = AlpacaBracketOrderRequest::new(
        instrument.clone(),
        strategy.clone(),
        order_cid.clone(),
        Side::Buy,
        dec!(1),
        dec!(100.00),
        dec!(120.00),
        dec!(90.00),
        TimeInForce::GoodUntilEndOfDay,
    )
    .with_stop_loss_limit_price(dec!(88.00));

    println!("Placing bracket order: BUY 1 SPY @ $100 entry, $120 TP, $90/$88 SL (stop-limit)");

    let result: AlpacaBracketOrderResult = client.open_bracket_order(request).await;

    match &result.parent.state {
        OrderState::Active(ActiveOrderState::Open(open_state)) => {
            println!("Bracket order (stop-limit SL) placed successfully!");
            println!("  Client Order ID: {}", result.parent.key.cid);
            println!("  Exchange Order ID: {:?}", open_state.id);
            println!("  Entry Price: {:?}", result.parent.price);

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Cancel the bracket order
            let cancel_key = OrderKey {
                exchange: ExchangeId::AlpacaBroker,
                instrument: &instrument,
                strategy: result.parent.key.strategy.clone(),
                cid: result.parent.key.cid.clone(),
            };

            let cancel_request = rustrade_execution::order::OrderEvent {
                key: cancel_key,
                state: rustrade_execution::order::request::RequestCancel {
                    id: Some(open_state.id.clone()),
                },
            };

            println!("Canceling bracket order (stop-limit)...");
            let cancel_response = client.cancel_order(cancel_request).await;

            match &cancel_response.state {
                Ok(cancelled) => {
                    println!("Bracket order (stop-limit) canceled successfully!");
                    println!("  Exchange Order ID: {:?}", cancelled.id);
                }
                Err(e) => {
                    panic!("Cancel rejected: {:?}", e);
                }
            }

            await_no_open_orders(&client, &instrument).await;
        }
        OrderState::Inactive(e) => {
            panic!("Bracket order (stop-limit) rejected: {:?}", e);
        }
        OrderState::Active(other) => {
            panic!("Unexpected active state: {:?}", other);
        }
    }
}
