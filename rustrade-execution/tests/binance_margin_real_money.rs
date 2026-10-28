//! Live check of `BinanceMargin` that places real orders, with real money, on production.
//!
//! Binance margin has no testnet, so the only way to see how its account stream reports orders
//! and borrowing is to place some. Each test places the smallest orders it can, far below the
//! market so that they rest, and cancels them:
//!
//! 1. a resting `LIMIT` buy with no borrowing, then its cancel;
//! 2. a resting `LIMIT` buy larger than the account's free quote asset, placed with
//!    `MarginEffect::Borrow` so that Binance lends the shortfall (about 6 of the quote asset),
//!    then its cancel, which repays the loan (`autoRepayAtCancel`).
//!
//! It checks the account stream delivers each order, its cancel and the borrow. Cancelled
//! orders pay no fee. The loan costs one hour of interest, a fraction of a cent, which the
//! cleanup repays.
//!
//! Two more pairs place a trailing `STOP_LOSS_LIMIT` buy of about 6 of the quote asset, with no
//! borrowing, that cannot fill, and cancel it:
//!
//! 3. with an activation price 15 % above the market, so that it does not start trailing. It
//!    reads the order raw from `GET /sapi/v1/margin/openOrders`, `/order` and `/allOrders`, and
//!    prints whether each row carries `trailingDelta` and `trailingTime`, which Binance's
//!    documentation of those responses does not list;
//! 4. with no activation price, so with no `stopPrice`, which Binance takes only with a
//!    `trailingDelta`, and a limit price below the market, so that once triggered it would buy no
//!    higher than that. It reads the order raw from `GET /sapi/v1/margin/order`.
//!
//! Each checks the account stream reports the order as the trailing stop placed. On an isolated
//! account Binance's `executionReport` sends an empty `d` even for a trailing order, so only the
//! kind the client recorded when it placed the order can say so. Each prints only field names and
//! the trailing fields' values. The raw rows, which hold account data, go to
//! `BINANCE_REAL_MONEY_RAW_LOG` when that is set.
//!
//! # Cleanup
//! Whether the steps pass or fail, each test then cancels every open order on the pair and
//! repays all debt, interest included, in the cross account and the isolated pair, and fails
//! unless the subaccount ends flat. A run that was killed can leave it otherwise:
//! `real_money_flatten_subaccount` does the same cleanup alone. Every test refuses to start
//! unless the subaccount is flat already.
//!
//! # Requirements
//! - A dedicated subaccount that nothing else trades, holding only what these tests may spend:
//!   about 10 USDT in cross margin, and 10 USDT in the isolated pair (`BTCUSDT` unless set).
//! - `BINANCE_SUBACCOUNT_TRADING_API_KEY`, `BINANCE_SUBACCOUNT_TRADING_API_SECRET`: its key, with
//!   reading, spot and margin trading, and margin loan and repay enabled (the cleanup repays
//!   leftover interest), never withdrawal.
//! - `BINANCE_REAL_MONEY_TESTS=yes`, the opt-in. Without it each test returns without a request.
//! - `BINANCE_REAL_MONEY_SYMBOL`: the pair, `BTCUSDT` if unset. Its quote asset is what is lent.
//! - `BINANCE_REAL_MONEY_FRAMES_LOG` (optional): a file path. When set, the test also reads the
//!   stream itself and writes every raw frame there, which holds account data: keep the file
//!   private. It prints only each frame's event type and field names.
//! - `BINANCE_REAL_MONEY_RAW_LOG` (optional): a file path for the trailing read-back's raw REST
//!   rows, which hold account data: keep the file private.
//!
//! Run the tests one at a time (they share the subaccount):
//!
//! ```bash
//! cargo test -p rustrade-execution --features binance --test binance_margin_real_money -- --ignored --nocapture --test-threads=1
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)] // Live check: a panic is the report.

use binance_sdk::{
    common::{config::ConfigurationRestApi, constants::MARGIN_TRADING_REST_API_PROD_URL},
    margin_trading::{
        MarginTradingRestApi,
        rest_api::{
            MarginAccountBorrowRepayIsIsolatedEnum, MarginAccountBorrowRepayParams,
            MarginAccountBorrowRepayTypeEnum,
            MarginAccountCancelAllOpenOrdersOnASymbolIsIsolatedEnum,
            MarginAccountCancelAllOpenOrdersOnASymbolParams, QueryCrossMarginAccountDetailsParams,
            QueryIsolatedMarginAccountInfoParams, QueryMarginAccountsOpenOrdersIsIsolatedEnum,
            QueryMarginAccountsOpenOrdersParams, RestApi,
        },
    },
};
use futures::{FutureExt, SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_execution::{
    AccountEventKind, MarginDebtKind, UnindexedAccountEvent,
    client::{
        ExecutionClient,
        binance::{BinanceMargin, BinanceMarginConfig},
    },
    margin::{MarginEffect, MarginPlacement},
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce, TrailingOffsetType,
        id::{ClientOrderId, StrategyId, VenueOrderId},
        request::{RequestCancel, RequestOpen},
        state::{ActiveOrderState, OrderState},
    },
};
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use rustrade_integration::protocol::websocket::{WsMessage, connect};
use serde_json::Value;
use serial_test::serial;
use std::{
    collections::BTreeMap,
    io::Write as _,
    panic::AssertUnwindSafe,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

/// How much each borrow step asks Binance to lend, in the quote asset. Above the 5 USDT minimum
/// borrow, so the loan is the shortfall itself.
const BORROW: Decimal = dec!(6);
/// The notional of the step that borrows nothing, in the quote asset.
const RESTING_NOTIONAL: Decimal = dec!(6);
/// How far below the last price the orders rest.
const BELOW_MARKET: Decimal = dec!(0.8);
/// How long the stream has to deliver an expected event.
const EVENT_WAIT: Duration = Duration::from_secs(15);
/// How far above the last price the trailing buy's activation price, and its limit price, are:
/// far enough that it does not start trailing during the run, and under the 1.2 times the
/// average price that Binance's `PERCENT_PRICE_BY_SIDE` filter allows a buy on BTCUSDT.
const ABOVE_MARKET: Decimal = dec!(1.15);
/// The trailing buy's trailing delta, in basis points (Binance takes 10 to 2000).
const TRAILING_BIPS: Decimal = dec!(1000);

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} env var required"))
}

/// Whether the opt-in is set. A test that returns early here sends nothing.
fn opted_in() -> bool {
    let yes = std::env::var("BINANCE_REAL_MONEY_TESTS").is_ok_and(|value| value == "yes");
    if !yes {
        eprintln!("skipped: set BINANCE_REAL_MONEY_TESTS=yes to place real orders");
    }
    yes
}

fn symbol() -> String {
    std::env::var("BINANCE_REAL_MONEY_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_owned())
}

fn keys() -> (String, String) {
    (
        env("BINANCE_SUBACCOUNT_TRADING_API_KEY"),
        env("BINANCE_SUBACCOUNT_TRADING_API_SECRET"),
    )
}

fn rest_config() -> ConfigurationRestApi {
    let (api_key, secret_key) = keys();
    ConfigurationRestApi::builder()
        .api_key(api_key)
        .api_secret(secret_key)
        .base_path(MARGIN_TRADING_REST_API_PROD_URL)
        .build()
        .expect("REST configuration")
}

fn rest() -> RestApi {
    MarginTradingRestApi::production(rest_config())
}

fn decimal(text: Option<&String>) -> Decimal {
    text.map_or(Decimal::ZERO, |text| {
        Decimal::from_str(text).expect("a decimal string")
    })
}

// ---------------------------------------------------------------------------
// The subaccount's state, read and flattened over REST
// ---------------------------------------------------------------------------

/// One account to check and flatten: the cross account, or the isolated pair.
#[derive(Clone, Copy, Debug)]
enum Account {
    Cross,
    Isolated,
}

/// What keeps an account from being flat.
#[derive(Debug, Default)]
struct Leftovers {
    open_orders: usize,
    /// `(asset, borrowed + interest)` for each asset still owed.
    debt: Vec<(String, Decimal)>,
}

impl Leftovers {
    fn is_flat(&self) -> bool {
        self.open_orders == 0 && self.debt.is_empty()
    }
}

async fn leftovers(rest: &RestApi, account: Account, symbol: &str) -> Leftovers {
    let open_orders = {
        let params = match account {
            Account::Cross => QueryMarginAccountsOpenOrdersParams::builder()
                .is_isolated(QueryMarginAccountsOpenOrdersIsIsolatedEnum::False),
            Account::Isolated => QueryMarginAccountsOpenOrdersParams::builder()
                .symbol(symbol.to_owned())
                .is_isolated(QueryMarginAccountsOpenOrdersIsIsolatedEnum::True),
        };
        let orders = rest
            .query_margin_accounts_open_orders(params.build().unwrap())
            .await
            .expect("open orders")
            .data()
            .await
            .expect("open orders decode");
        orders.len()
    };

    let mut debt = Vec::new();
    let mut owed =
        |asset: Option<&String>, borrowed: Option<&String>, interest: Option<&String>| {
            let total = decimal(borrowed) + decimal(interest);
            if !total.is_zero() {
                debt.push((asset.cloned().unwrap_or_default(), total));
            }
        };
    match account {
        Account::Cross => {
            let details = rest
                .query_cross_margin_account_details(
                    QueryCrossMarginAccountDetailsParams::builder()
                        .build()
                        .unwrap(),
                )
                .await
                .expect("cross margin account")
                .data()
                .await
                .expect("cross margin account decode");
            for asset in details.user_assets.unwrap_or_default() {
                owed(
                    asset.asset.as_ref(),
                    asset.borrowed.as_ref(),
                    asset.interest.as_ref(),
                );
            }
        }
        Account::Isolated => {
            let info = rest
                .query_isolated_margin_account_info(
                    QueryIsolatedMarginAccountInfoParams::builder()
                        .symbols(symbol.to_owned())
                        .build()
                        .unwrap(),
                )
                .await
                .expect("isolated margin account")
                .data()
                .await
                .expect("isolated margin account decode");
            let pair = info
                .assets
                .unwrap_or_default()
                .into_iter()
                .find(|pair| pair.symbol.as_deref() == Some(symbol))
                .unwrap_or_else(|| panic!("the isolated {symbol} account is not activated"));
            if let Some(base) = pair.base_asset {
                owed(
                    base.asset.as_ref(),
                    base.borrowed.as_ref(),
                    base.interest.as_ref(),
                );
            }
            if let Some(quote) = pair.quote_asset {
                owed(
                    quote.asset.as_ref(),
                    quote.borrowed.as_ref(),
                    quote.interest.as_ref(),
                );
            }
        }
    }
    Leftovers { open_orders, debt }
}

/// The account's free quote asset, which bounds what the steps can place without borrowing.
async fn free_quote(rest: &RestApi, account: Account, symbol: &str, quote: &str) -> Decimal {
    match account {
        Account::Cross => {
            let details = rest
                .query_cross_margin_account_details(
                    QueryCrossMarginAccountDetailsParams::builder()
                        .build()
                        .unwrap(),
                )
                .await
                .expect("cross margin account")
                .data()
                .await
                .expect("cross margin account decode");
            details
                .user_assets
                .unwrap_or_default()
                .into_iter()
                .find(|asset| asset.asset.as_deref() == Some(quote))
                .map_or(Decimal::ZERO, |asset| decimal(asset.free.as_ref()))
        }
        Account::Isolated => {
            let info = rest
                .query_isolated_margin_account_info(
                    QueryIsolatedMarginAccountInfoParams::builder()
                        .symbols(symbol.to_owned())
                        .build()
                        .unwrap(),
                )
                .await
                .expect("isolated margin account")
                .data()
                .await
                .expect("isolated margin account decode");
            info.assets
                .unwrap_or_default()
                .into_iter()
                .find(|pair| pair.symbol.as_deref() == Some(symbol))
                .and_then(|pair| pair.quote_asset)
                .map_or(Decimal::ZERO, |asset| decimal(asset.free.as_ref()))
        }
    }
}

/// Cancels the account's open orders on the pair and repays what it owes, then reads it again,
/// up to six times three seconds apart, since account reads can trail a cancel or repay.
async fn flatten(rest: &RestApi, account: Account, symbol: &str) -> Leftovers {
    let mut left = Leftovers::default();
    for _ in 0..6 {
        left = leftovers(rest, account, symbol).await;
        if left.is_flat() {
            return left;
        }
        println!("flattening {account:?}: {left:?}");
        if left.open_orders > 0 {
            let is_isolated = match account {
                Account::Cross => MarginAccountCancelAllOpenOrdersOnASymbolIsIsolatedEnum::False,
                Account::Isolated => MarginAccountCancelAllOpenOrdersOnASymbolIsIsolatedEnum::True,
            };
            let params =
                MarginAccountCancelAllOpenOrdersOnASymbolParams::builder(symbol.to_owned())
                    .is_isolated(is_isolated)
                    .build()
                    .unwrap();
            if let Err(error) = rest
                .margin_account_cancel_all_open_orders_on_a_symbol(params)
                .await
            {
                println!("cancel all on {symbol} failed: {error}");
            }
        }
        for (asset, amount) in &left.debt {
            let (is_isolated, pair) = match account {
                Account::Cross => (MarginAccountBorrowRepayIsIsolatedEnum::False, None),
                Account::Isolated => (
                    MarginAccountBorrowRepayIsIsolatedEnum::True,
                    Some(symbol.to_owned()),
                ),
            };
            let params = MarginAccountBorrowRepayParams::builder(
                asset.clone(),
                is_isolated,
                amount.to_string(),
                MarginAccountBorrowRepayTypeEnum::Repay,
            )
            .symbol(pair)
            .build()
            .unwrap();
            match rest.margin_account_borrow_repay(params).await {
                Ok(_) => println!("repaid {amount} {asset} ({account:?})"),
                Err(error) => println!("repay of {amount} {asset} ({account:?}) failed: {error}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    left
}

/// Panics, before anything is placed, unless the whole subaccount is flat.
async fn refuse_unless_flat(rest: &RestApi, symbol: &str) {
    for account in [Account::Cross, Account::Isolated] {
        let left = leftovers(rest, account, symbol).await;
        assert!(
            left.is_flat(),
            "refusing to start: the {account:?} account is not flat ({left:?}). Run \
             real_money_flatten_subaccount, or flatten it by hand"
        );
    }
}

/// Flattens both accounts and panics unless both end flat.
async fn flatten_all(rest: &RestApi, symbol: &str) {
    let mut not_flat = Vec::new();
    for account in [Account::Cross, Account::Isolated] {
        let left = flatten(rest, account, symbol).await;
        if !left.is_flat() {
            not_flat.push((account, left));
        }
    }
    assert!(
        not_flat.is_empty(),
        "the subaccount is NOT flat after cleanup: {not_flat:?}. Flatten it by hand. The cleanup \
         cancels orders on the tested pair only: an open cross order on another pair must be \
         cancelled by hand"
    );
    println!("the subaccount is flat");
}

// ---------------------------------------------------------------------------
// Public market data: the price to rest below, and the pair's rules
// ---------------------------------------------------------------------------

struct Market {
    last: Decimal,
    tick: Decimal,
    step: Decimal,
    min_notional: Decimal,
    quote: String,
}

async fn market(symbol: &str) -> Market {
    let http = reqwest::Client::new();
    let get = |url: String| {
        let http = http.clone();
        async move {
            http.get(url)
                .send()
                .await
                .expect("public request")
                .json::<serde_json::Value>()
                .await
                .expect("public JSON")
        }
    };
    let ticker = get(format!(
        "https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    ))
    .await;
    let info = get(format!(
        "https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}"
    ))
    .await;
    let number = |value: &serde_json::Value| {
        Decimal::from_str(value.as_str().expect("a decimal string")).expect("a decimal")
    };
    let pair = &info["symbols"][0];
    let filter = |kind: &str| {
        pair["filters"]
            .as_array()
            .expect("filters")
            .iter()
            .find(|filter| filter["filterType"] == kind)
            .unwrap_or_else(|| panic!("{symbol} has no {kind} filter"))
            .clone()
    };
    Market {
        last: number(&ticker["price"]),
        tick: number(&filter("PRICE_FILTER")["tickSize"]),
        step: number(&filter("LOT_SIZE")["stepSize"]),
        min_notional: number(&filter("NOTIONAL")["minNotional"]),
        quote: pair["quoteAsset"].as_str().expect("quoteAsset").to_owned(),
    }
}

impl Market {
    /// A price `BELOW_MARKET` of the last one, on the tick grid.
    fn resting_price(&self) -> Decimal {
        (self.last * BELOW_MARKET / self.tick).floor() * self.tick
    }

    /// A price `ABOVE_MARKET` of the last one, on the tick grid.
    fn activation_price(&self) -> Decimal {
        (self.last * ABOVE_MARKET / self.tick).ceil() * self.tick
    }

    /// The smallest quantity on the lot grid whose notional at `price` reaches `notional`.
    fn quantity(&self, notional: Decimal, price: Decimal) -> Decimal {
        let notional = notional.max(self.min_notional);
        ((notional / price / self.step).ceil() * self.step).normalize()
    }
}

// ---------------------------------------------------------------------------
// The stream, as the client delivers it and (optionally) raw
// ---------------------------------------------------------------------------

type Events = Arc<Mutex<Vec<UnindexedAccountEvent>>>;

/// Waits up to `EVENT_WAIT` for an event matching `wanted`, and says whether one arrived.
async fn wait_for(events: &Events, wanted: impl Fn(&UnindexedAccountEvent) -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + EVENT_WAIT;
    while tokio::time::Instant::now() < deadline {
        if events.lock().unwrap().iter().any(&wanted) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

fn describe(event: &UnindexedAccountEvent) -> String {
    match &event.kind {
        AccountEventKind::OrderSnapshot(order) => format!("OrderSnapshot {}", order.0.key.cid),
        AccountEventKind::OrderCancelled(cancel) => format!("OrderCancelled {}", cancel.key.cid),
        AccountEventKind::Trade(_) => "Trade".to_owned(),
        AccountEventKind::BalanceStreamUpdate(_) => "BalanceStreamUpdate".to_owned(),
        AccountEventKind::InstrumentBalanceUpdate(_) => "InstrumentBalanceUpdate".to_owned(),
        AccountEventKind::MarginDebt(debt) => format!(
            "MarginDebt {} {} ({}), names an instrument: {}",
            debt.asset,
            debt.kind,
            debt.venue_kind,
            debt.instrument.is_some()
        ),
        AccountEventKind::Notice(notice) => format!(
            "Notice {:?} ({}), names an instrument: {}",
            notice.kind,
            notice.status,
            notice.instrument.is_some()
        ),
        AccountEventKind::StreamTerminated(reason) => format!("StreamTerminated {reason:?}"),
        _ => "Other".to_owned(),
    }
}

/// Reads the stream with a listen key of its own (Binance hands out the same key while one is
/// live), writing each raw frame to `path`.
async fn raw_tap(api_key: String, path: String) {
    let answer = reqwest::Client::new()
        .post("https://api.binance.com/sapi/v2/margin/listen-key")
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .expect("listen key request")
        .json::<serde_json::Value>()
        .await
        .expect("listen key JSON");
    let listen_key = answer["listenKey"].as_str().expect("listenKey").to_owned();
    let mut ws = connect("wss://fstream.binance.com/private/ws")
        .await
        .expect("raw tap connect");
    let subscribe = serde_json::json!({"method": "SUBSCRIBE", "params": [listen_key], "id": 1});
    ws.send(WsMessage::Text(subscribe.to_string().into()))
        .await
        .expect("raw tap subscribe");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("frames log");
    while let Some(Ok(message)) = ws.next().await {
        if let WsMessage::Text(text) = message {
            writeln!(file, "{}", text.as_str()).expect("frames log write");
        }
    }
}

/// Prints each raw frame's event type and field names, and for a margin-level notice its status:
/// what the frames' shapes are, without their account data.
fn print_frame_shapes(path: &str) {
    let Ok(text) = std::fs::read_to_string(path) else {
        println!("no raw frames log at the given path");
        return;
    };
    for line in text.lines() {
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(object) = frame.as_object() else {
            continue;
        };
        let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
        fields.sort_unstable();
        let status = if frame["e"] == "marginLevelChange" {
            format!(" s={}", frame["s"])
        } else {
            String::new()
        };
        println!(
            "raw frame: e={} is={}{status} fields={fields:?}",
            frame["e"], frame["is"]
        );
    }
}

// ---------------------------------------------------------------------------
// The steps
// ---------------------------------------------------------------------------

/// Places a buy and returns its client order id and the client's answer, whatever it is.
async fn submit(
    client: &BinanceMargin,
    instrument: &InstrumentNameExchange,
    tag: &str,
    kind: OrderKind,
    price: Decimal,
    quantity: Decimal,
    effect: MarginEffect,
) -> (ClientOrderId, MarginPlacement) {
    let cid = ClientOrderId::new(format!(
        "rt-live-{tag}-{}",
        chrono::Utc::now().timestamp_millis()
    ));
    let placement = client
        .open_margin_order(OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::BinanceMargin,
                instrument,
                strategy: StrategyId::new("live-check"),
                cid: cid.clone(),
            },
            state: RequestOpen::new(
                Side::Buy,
                Some(price),
                quantity,
                kind,
                TimeInForce::GoodUntilCancelled { post_only: false },
            )
            .with_margin_effect(effect),
        })
        .await;
    (cid, placement)
}

/// Places a buy that must rest, and returns its ids and what it borrowed.
async fn place(
    client: &BinanceMargin,
    instrument: &InstrumentNameExchange,
    tag: &str,
    kind: OrderKind,
    price: Decimal,
    quantity: Decimal,
    effect: MarginEffect,
) -> (
    ClientOrderId,
    VenueOrderId,
    Option<rustrade_execution::margin::MarginBorrow>,
) {
    let (cid, placement) = submit(client, instrument, tag, kind, price, quantity, effect).await;
    let OrderState::Active(ActiveOrderState::Open(open)) = &placement.order.state else {
        panic!(
            "{tag}: the order is not resting: {:?}",
            placement.order.state
        );
    };
    println!("{tag}: placed {cid}, borrowed {:?}", placement.borrowed);
    (cid, open.id.clone(), placement.borrowed)
}

async fn cancel(
    client: &BinanceMargin,
    instrument: &InstrumentNameExchange,
    cid: &ClientOrderId,
    id: VenueOrderId,
) {
    let response = client
        .cancel_order(OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::BinanceMargin,
                instrument,
                strategy: StrategyId::new("live-check"),
                cid: cid.clone(),
            },
            state: RequestCancel { id: Some(id) },
        })
        .await;
    assert!(
        response.state.is_ok(),
        "cancel of {cid} failed: {:?}",
        response.state
    );
}

async fn steps(client: &BinanceMargin, rest: &RestApi, account: Account, events: &Events) {
    let symbol = symbol();
    let instrument = InstrumentNameExchange::new(symbol.clone());
    let market = market(&symbol).await;
    let price = market.resting_price();
    let free = free_quote(rest, account, &symbol, &market.quote).await;
    assert!(
        free >= RESTING_NOTIONAL,
        "the {account:?} account holds {free} {}, less than the {RESTING_NOTIONAL} the first \
         step places",
        market.quote
    );

    // 1. A resting order that borrows nothing.
    let quantity = market.quantity(RESTING_NOTIONAL, price);
    let (cid, id, borrowed) = place(
        client,
        &instrument,
        "plain",
        OrderKind::Limit,
        price,
        quantity,
        MarginEffect::NoSideEffect,
    )
    .await;
    assert!(
        borrowed.is_none(),
        "a NoSideEffect order borrowed {borrowed:?}"
    );
    let snapshot_of = |cid: ClientOrderId| move |event: &UnindexedAccountEvent| matches!(&event.kind, AccountEventKind::OrderSnapshot(order) if order.0.key.cid == cid);
    let cancel_of = |cid: ClientOrderId| move |event: &UnindexedAccountEvent| matches!(&event.kind, AccountEventKind::OrderCancelled(cancel) if cancel.key.cid == cid);
    assert!(
        wait_for(events, snapshot_of(cid.clone())).await,
        "the stream did not deliver {cid} resting"
    );
    cancel(client, &instrument, &cid, id).await;
    assert!(
        wait_for(events, cancel_of(cid.clone())).await,
        "the stream did not deliver {cid}'s cancel"
    );

    // 2. A resting order larger than the free quote asset, so that Binance lends the shortfall.
    let free = free_quote(rest, account, &symbol, &market.quote).await;
    let quantity = market.quantity(free + BORROW, price);
    let (cid, id, borrowed) = place(
        client,
        &instrument,
        "borrow",
        OrderKind::Limit,
        price,
        quantity,
        MarginEffect::Borrow,
    )
    .await;
    let loan = borrowed.expect("Binance reports the loan in its answer");
    assert_eq!(loan.asset.as_ref(), market.quote, "the quote asset is lent");
    assert!(loan.amount > Decimal::ZERO, "a positive loan: {loan:?}");
    let debt_of = |kind: MarginDebtKind| {
        let quote = market.quote.clone();
        move |event: &UnindexedAccountEvent| {
            matches!(&event.kind, AccountEventKind::MarginDebt(debt)
                if debt.kind == kind && debt.asset.as_ref() == quote)
        }
    };
    assert!(
        wait_for(events, snapshot_of(cid.clone())).await,
        "the stream did not deliver {cid} resting"
    );
    let borrow_streamed = wait_for(events, debt_of(MarginDebtKind::Borrow)).await;
    // Cancelled before anything else is judged, so that its auto-repay settles the loan.
    cancel(client, &instrument, &cid, id).await;
    assert!(
        wait_for(events, cancel_of(cid.clone())).await,
        "the stream did not deliver {cid}'s cancel"
    );
    let repay_streamed = wait_for(events, debt_of(MarginDebtKind::Repay)).await;
    println!("{account:?}: borrow streamed {borrow_streamed}, repay streamed {repay_streamed}");

    // The cancel repaid the loan: what is left owed is at most the hour's interest.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let left = leftovers(rest, account, &symbol).await;
    let owed: Decimal = left.debt.iter().map(|(_, amount)| *amount).sum();
    assert!(
        owed < dec!(0.001),
        "the cancel did not repay the loan: {left:?}"
    );

    // Binance sends `liabilityChange` for the cross account. For an isolated pair it has not
    // been seen to, so there its absence is reported rather than failed.
    match account {
        Account::Cross => assert!(
            borrow_streamed,
            "the stream did not deliver the borrow as MarginDebt"
        ),
        Account::Isolated if !borrow_streamed => {
            println!("NOTE: no MarginDebt for the isolated borrow");
        }
        Account::Isolated => {}
    }
}

/// Reads `path` raw, signed, with `query` and the account's `isIsolated`, so that fields the SDK's
/// response types leave out are kept.
async fn raw_get(
    config: &ConfigurationRestApi,
    path: &str,
    account: Account,
    mut query: BTreeMap<String, Value>,
) -> Value {
    let is_isolated = match account {
        Account::Cross => "FALSE",
        Account::Isolated => "TRUE",
    };
    query.insert("isIsolated".to_owned(), Value::from(is_isolated));
    binance_sdk::common::utils::send_request::<Value>(
        config,
        path,
        reqwest::Method::GET,
        query,
        BTreeMap::new(),
        None,
        true,
    )
    .await
    .unwrap_or_else(|error| panic!("{path}: {error}"))
    .data()
    .await
    .unwrap_or_else(|error| panic!("{path} decode: {error}"))
}

/// Prints `row`'s field names and its trailing fields, and returns whether it has
/// `trailingDelta`. Appends the row to `raw_log` when set.
fn report_row(endpoint: &str, row: &Value, raw_log: Option<&str>) -> bool {
    if let Some(path) = raw_log {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("raw log");
        writeln!(file, "{endpoint}: {row}").expect("raw log write");
    }
    let object = row.as_object().expect("an order row is an object");
    let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
    fields.sort_unstable();
    println!(
        "{endpoint}: type={} trailingDelta={} trailingTime={} fields={fields:?}",
        row["type"], row["trailingDelta"], row["trailingTime"]
    );
    object.contains_key("trailingDelta")
}

/// The row of `rows`, an array, whose `orderId` (a JSON number) is `order_id`.
fn row_of<'a>(endpoint: &str, rows: &'a Value, order_id: i64) -> &'a Value {
    rows.as_array()
        .unwrap_or_else(|| panic!("{endpoint} answers an array"))
        .iter()
        .find(|row| row["orderId"].as_i64() == Some(order_id))
        .unwrap_or_else(|| panic!("{endpoint} does not list order {order_id}"))
}

async fn trailing_read_back(
    client: &BinanceMargin,
    rest: &RestApi,
    account: Account,
    events: &Events,
) {
    let symbol = symbol();
    let instrument = InstrumentNameExchange::new(symbol.clone());
    let market = market(&symbol).await;
    let free = free_quote(rest, account, &symbol, &market.quote).await;
    assert!(
        free >= RESTING_NOTIONAL,
        "the {account:?} account holds {free} {}, less than the {RESTING_NOTIONAL} the order \
         locks",
        market.quote
    );

    // A trailing buy that starts trailing only once the market reaches `activation`, far above
    // it, and then buys at no more than `activation`.
    let activation = market.activation_price();
    let quantity = market.quantity(RESTING_NOTIONAL, activation);
    let kind = OrderKind::TrailingStopFixedLimit {
        offset: TRAILING_BIPS,
        offset_type: TrailingOffsetType::BasisPoints,
        activation_price: Some(activation),
    };
    let (cid, id, borrowed) = place(
        client,
        &instrument,
        "trailing",
        kind,
        activation,
        quantity,
        MarginEffect::NoSideEffect,
    )
    .await;
    assert!(
        borrowed.is_none(),
        "a NoSideEffect order borrowed {borrowed:?}"
    );
    let order_id: i64 = id
        .assigned()
        .expect("Binance assigns an orderId")
        .0
        .parse()
        .expect("Binance's orderId is an integer");

    // The stream carries `trailingDelta`: if it reports the order as trailing, a REST row without
    // the field lacks it, rather than describing a fixed stop.
    let snapshot_arrived = wait_for(events, |event| matches!(&event.kind, AccountEventKind::OrderSnapshot(order) if order.0.key.cid == cid)).await;

    let raw_log = std::env::var("BINANCE_REAL_MONEY_RAW_LOG").ok();
    let config = rest_config();
    let symbol_query = || BTreeMap::from([("symbol".to_owned(), Value::from(symbol.clone()))]);
    let open = raw_get(
        &config,
        "/sapi/v1/margin/openOrders",
        account,
        symbol_query(),
    )
    .await;
    let in_open = report_row(
        "openOrders",
        row_of("openOrders", &open, order_id),
        raw_log.as_deref(),
    );
    let mut query = symbol_query();
    query.insert("orderId".to_owned(), Value::from(order_id));
    let single = raw_get(&config, "/sapi/v1/margin/order", account, query.clone()).await;
    let in_single = report_row("order", &single, raw_log.as_deref());
    query.insert("limit".to_owned(), Value::from(1));
    let all = raw_get(&config, "/sapi/v1/margin/allOrders", account, query).await;
    let in_all = report_row(
        "allOrders",
        row_of("allOrders", &all, order_id),
        raw_log.as_deref(),
    );

    cancel(client, &instrument, &cid, id).await;
    println!(
        "RESULT {account:?}: trailingDelta in openOrders {in_open}, order {in_single}, allOrders \
         {in_all}"
    );
    assert!(snapshot_arrived, "the stream did not deliver {cid} resting");
    let streamed = events
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order) if order.0.key.cid == cid => Some(order.0.kind),
            _ => None,
        });
    println!("{account:?}: the stream reported the order as {streamed:?}");
    assert_eq!(
        streamed,
        Some(kind),
        "the stream did not report the order as the trailing stop placed"
    );
}

async fn trailing_without_activation(
    client: &BinanceMargin,
    rest: &RestApi,
    account: Account,
    events: &Events,
) {
    let symbol = symbol();
    let instrument = InstrumentNameExchange::new(symbol.clone());
    let market = market(&symbol).await;
    let free = free_quote(rest, account, &symbol, &market.quote).await;
    assert!(
        free >= RESTING_NOTIONAL,
        "the {account:?} account holds {free} {}, less than the {RESTING_NOTIONAL} the order \
         locks",
        market.quote
    );

    // No activation price, so no `stopPrice`: trailing only by `trailingDelta`. Once triggered it
    // would buy at no more than `limit`, below the market.
    let limit = market.resting_price();
    let quantity = market.quantity(RESTING_NOTIONAL, limit);
    let kind = OrderKind::TrailingStopFixedLimit {
        offset: TRAILING_BIPS,
        offset_type: TrailingOffsetType::BasisPoints,
        activation_price: None,
    };
    let (cid, placement) = submit(
        client,
        &instrument,
        "trail-noact",
        kind,
        limit,
        quantity,
        MarginEffect::NoSideEffect,
    )
    .await;
    let id = match &placement.order.state {
        OrderState::Active(ActiveOrderState::Open(open)) => open.id.clone(),
        other => {
            println!("RESULT {account:?}: not accepted as a resting order: {other:?}");
            return;
        }
    };
    println!("{account:?}: accepted {cid}");
    let order_id: i64 = id
        .assigned()
        .expect("Binance assigns an orderId")
        .0
        .parse()
        .expect("Binance's orderId is an integer");
    let snapshot_arrived = wait_for(events, |event| matches!(&event.kind, AccountEventKind::OrderSnapshot(order) if order.0.key.cid == cid)).await;

    let raw_log = std::env::var("BINANCE_REAL_MONEY_RAW_LOG").ok();
    let mut query = BTreeMap::from([("symbol".to_owned(), Value::from(symbol.clone()))]);
    query.insert("orderId".to_owned(), Value::from(order_id));
    let single = raw_get(&rest_config(), "/sapi/v1/margin/order", account, query).await;
    report_row("order", &single, raw_log.as_deref());
    println!(
        "order: status={} stopPrice={}",
        single["status"], single["stopPrice"]
    );

    cancel(client, &instrument, &cid, id).await;
    let streamed = events
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order) if order.0.key.cid == cid => Some(order.0.kind),
            _ => None,
        });
    println!("RESULT {account:?}: accepted; the stream reported the order as {streamed:?}");
    assert!(snapshot_arrived, "the stream did not deliver {cid} resting");
    assert_eq!(
        streamed,
        Some(kind),
        "the stream did not report the order as the trailing stop placed"
    );
}

/// What a run places and checks.
#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// [`steps`]: resting orders, one of them borrowing, and their cancels.
    OrdersAndBorrow,
    /// [`trailing_read_back`]: a trailing stop read back over REST.
    TrailingReadBack,
    /// [`trailing_without_activation`]: whether a trailing stop with no activation price is
    /// accepted.
    TrailingWithoutActivation,
}

/// Runs `scenario` on one account, then flattens the subaccount whatever happened, and reports.
async fn real_money_run(account: Account, scenario: Scenario) {
    if !opted_in() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let symbol = symbol();
    let rest = rest();
    refuse_unless_flat(&rest, &symbol).await;

    let (api_key, secret_key) = keys();
    let frames_log = std::env::var("BINANCE_REAL_MONEY_FRAMES_LOG").ok();
    let tap = frames_log
        .clone()
        .map(|path| tokio::spawn(raw_tap(api_key.clone(), path)));
    let config = match account {
        Account::Cross => BinanceMarginConfig::cross_margin(api_key, secret_key),
        Account::Isolated => BinanceMarginConfig::isolated(
            api_key,
            secret_key,
            vec![InstrumentNameExchange::new(symbol.clone())],
        ),
    };
    let client = BinanceMargin::new(config);
    let mut stream = client
        .account_stream(&[], &[InstrumentNameExchange::new(symbol.clone())])
        .await
        .expect("account_stream opens");
    let events = Events::default();
    let collector = {
        let events = Arc::clone(&events);
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                println!("stream event: {}", describe(&event));
                events.lock().unwrap().push(event);
            }
        })
    };
    // Let the raw tap subscribe before anything is placed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let outcome = match scenario {
        Scenario::OrdersAndBorrow => {
            AssertUnwindSafe(steps(&client, &rest, account, &events))
                .catch_unwind()
                .await
        }
        Scenario::TrailingReadBack => {
            AssertUnwindSafe(trailing_read_back(&client, &rest, account, &events))
                .catch_unwind()
                .await
        }
        Scenario::TrailingWithoutActivation => {
            AssertUnwindSafe(trailing_without_activation(
                &client, &rest, account, &events,
            ))
            .catch_unwind()
            .await
        }
    };
    // Let late frames (a repay, a notice) arrive before the summary.
    tokio::time::sleep(Duration::from_secs(5)).await;
    flatten_all(&rest, &symbol).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    collector.abort();
    if let Some(tap) = tap {
        // The tap reads until it is aborted, so one already finished failed.
        if tap.is_finished() {
            match tap.await {
                Err(error) if error.is_panic() => println!("NOTE: the raw tap failed: {error}"),
                _ => println!("NOTE: the raw tap stopped early"),
            }
        } else {
            tap.abort();
        }
    }
    let summary: Vec<String> = events.lock().unwrap().iter().map(describe).collect();
    println!("SUMMARY {account:?} stream events: {summary:#?}");
    if let Some(path) = frames_log {
        print_frame_shapes(&path);
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_cross_orders_and_borrow_reach_the_stream() {
    real_money_run(Account::Cross, Scenario::OrdersAndBorrow).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_isolated_orders_and_borrow_reach_the_stream() {
    real_money_run(Account::Isolated, Scenario::OrdersAndBorrow).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_cross_trailing_stop_read_back() {
    real_money_run(Account::Cross, Scenario::TrailingReadBack).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_isolated_trailing_stop_read_back() {
    real_money_run(Account::Isolated, Scenario::TrailingReadBack).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_cross_trailing_stop_without_activation() {
    real_money_run(Account::Cross, Scenario::TrailingWithoutActivation).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn real_money_isolated_trailing_stop_without_activation() {
    real_money_run(Account::Isolated, Scenario::TrailingWithoutActivation).await;
}

/// The cleanup alone, for a subaccount a killed run left with open orders or debt.
#[tokio::test]
#[ignore]
#[serial]
async fn real_money_flatten_subaccount() {
    if !opted_in() {
        return;
    }
    flatten_all(&rest(), &symbol()).await;
}
