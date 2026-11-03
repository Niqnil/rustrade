#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! A paper venue books what a backtest of the same venue books.
//!
//! One scripted session runs twice. The paper run goes through everything a live setup uses: a
//! market stream wrapped by `PaperMarketFeed::tap`, a venue added with
//! `ExecutionBuilder::add_paper`, its `ExecutionManager`, and requests sent on the execution
//! channel, all on tokio's paused clock with a `TokioClock`. The reference run steps the
//! market-driven `SimulatedVenue` a backtest uses through the same script by hand, booking each
//! request half the latency after it was sent, as `SimRunner` does.
//!
//! The script covers a limit order resting and then filled by the market, a market order whose
//! price moves while it is on its way to the venue, and an order expiring at its deadline with no
//! market event or request to move the clock. Both runs must book the same fills and the same
//! expiry, at the same instants.

use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    engine::{
        clock::{EngineClock, TokioClock},
        execution_tx::ExecutionTxMap,
    },
    execution::{
        Execution,
        builder::{ExecutionBuild, ExecutionBuilder},
        paper::{PaperMarketFeed, PaperOptions, PaperVenue},
        request::ExecutionRequest,
        sim::VenueMarketUpdate,
    },
    shutdown::AsyncShutdown,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::reconnect,
    subscription::trade::PublicTrade,
};
use rustrade_execution::{
    AccountEventKind, AccountSnapshot,
    balance::{AssetBalance, Balance},
    client::mock::MockExecutionConfig,
    error::{ApiError, ClientError, ConnectivityError, UnindexedClientError},
    exchange::mock::SimulatedVenue,
    fee::{FeeModelConfig, PercentageFeeModel},
    margin::{MarginEffect, SpotMarginConfig},
    market::MarketObservation,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::RequestOpen,
        state::{InactiveOrderState, OrderState},
    },
};
use rustrade_instrument::{
    Side, Underlying,
    asset::name::AssetNameExchange,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex, kind::InstrumentKind, name::InstrumentNameExchange,
        quote::InstrumentQuoteAsset,
    },
    test_utils::instrument,
};
use rustrade_integration::channel::Tx;
use std::time::Duration;

const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
const LATENCY_MS: u64 = 100;

/// The session's start. Everything in the script is an offset from it, in milliseconds, late
/// enough that the manager has finished opening the account before the first event.
fn start() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap()
}

fn at(offset_ms: i64) -> DateTime<Utc> {
    start() + TimeDelta::milliseconds(offset_ms)
}

/// One scripted input, at its offset from the start.
enum Step {
    /// A trade printed at this price.
    Trade(Decimal),
    /// The client sends this order.
    Order(RequestOpen, &'static str),
}

fn script() -> Vec<(i64, Step)> {
    let expiry = at(6_000);
    vec![
        (1_000, Step::Trade(dec!(100))),
        // Rests below the market, then fills when the market trades through it.
        (
            1_500,
            Step::Order(limit(Side::Buy, dec!(1), dec!(97), gtc()), "rest"),
        ),
        (2_000, Step::Trade(dec!(98))),
        (3_000, Step::Trade(dec!(95))),
        // Sent at 95, reaches the venue at 3_550, after the market has moved to 96.
        (3_500, Step::Order(market(Side::Buy, dec!(1)), "market")),
        (3_520, Step::Trade(dec!(96))),
        (4_000, Step::Trade(dec!(101))),
        // Rests above the market until its deadline, with nothing arriving after it.
        (
            4_500,
            Step::Order(
                limit(
                    Side::Sell,
                    dec!(1),
                    dec!(110),
                    TimeInForce::GoodTillDate { expiry },
                ),
                "expires",
            ),
        ),
    ]
}

fn gtc() -> TimeInForce {
    TimeInForce::GoodUntilCancelled { post_only: false }
}

fn limit(side: Side, quantity: Decimal, price: Decimal, time_in_force: TimeInForce) -> RequestOpen {
    RequestOpen::new(side, Some(price), quantity, OrderKind::Limit, time_in_force)
}

fn market(side: Side, quantity: Decimal) -> RequestOpen {
    RequestOpen::new(
        side,
        None,
        quantity,
        OrderKind::Market,
        TimeInForce::ImmediateOrCancel,
    )
}

fn trade_event(offset_ms: i64, price: Decimal) -> MarketEvent<InstrumentIndex, DataKind> {
    MarketEvent {
        time_exchange: at(offset_ms),
        time_received: at(offset_ms),
        exchange: EXCHANGE,
        instrument: InstrumentIndex(0),
        kind: DataKind::Trade(PublicTrade {
            id: offset_ms.to_string().into(),
            price,
            amount: dec!(1),
            side: None,
        }),
    }
}

fn instruments() -> IndexedInstruments {
    IndexedInstruments::new([instrument(EXCHANGE, "btc", "usdt")])
}

fn funded(asset: &str, amount: Decimal) -> AssetBalance<AssetNameExchange> {
    AssetBalance {
        asset: AssetNameExchange::new(asset),
        balance: Balance::new(amount, amount),
        time_exchange: start(),
    }
}

fn config() -> MockExecutionConfig {
    MockExecutionConfig {
        mocked_exchange: EXCHANGE,
        initial_state: AccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![funded("btc", dec!(10)), funded("usdt", dec!(100_000))],
            instruments: vec![],
        },
        latency_ms: LATENCY_MS,
        fee_model: FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.001))),
        fill_model: Default::default(),
        spot_fee_asset: Default::default(),
        slippage: Default::default(),
        market_order_timing: Default::default(),
        stale_market: Default::default(),
        spot_margin: None,
        fee_asset: None,
    }
}

/// What both runs must agree on: each fill, and each expiry, with its instant.
#[derive(Debug, PartialEq)]
enum Booked {
    Fill {
        order: OrderId,
        time: DateTime<Utc>,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        fee: Decimal,
    },
    Expired {
        cid: ClientOrderId,
        time: DateTime<Utc>,
    },
}

fn booked<Exchange, Asset, Instrument>(
    kind: AccountEventKind<Exchange, Asset, Instrument>,
) -> Option<Booked> {
    match kind {
        AccountEventKind::Trade(trade) => Some(Booked::Fill {
            order: trade.order_id,
            time: trade.time_exchange,
            side: trade.side,
            price: trade.price,
            quantity: trade.quantity,
            fee: trade.fees.fees,
        }),
        AccountEventKind::OrderSnapshot(order) => match order.0.state {
            OrderState::Inactive(InactiveOrderState::Expired(expired)) => Some(Booked::Expired {
                cid: order.0.key.cid,
                time: expired.time_exchange,
            }),
            _ => None,
        },
        _ => None,
    }
}

/// The paper run: tap, `add_paper`, manager, and the execution channel, on tokio's paused clock.
async fn paper_run() -> Vec<Booked> {
    let instruments = instruments();
    let anchor = tokio::time::Instant::now();
    let clock = TokioClock::starting_at(start());

    let (market_tx, market_rx) = futures::channel::mpsc::unbounded();
    let (tap, feed) = PaperMarketFeed::tap(market_rx);
    // Stands in for the `Engine`'s market feed, which is what polls the tap in a live system.
    tokio::spawn(tap.for_each(|_| std::future::ready(())));

    let execution = ExecutionBuilder::new(&instruments)
        .add_paper(config(), PaperOptions::default(), clock, &feed)
        .unwrap()
        .build()
        .init()
        .await
        .unwrap();
    let execution_tx = execution
        .execution_txs
        .find(&ExchangeIndex(0))
        .unwrap()
        .clone();

    for (offset_ms, step) in script() {
        tokio::time::sleep_until(anchor + Duration::from_millis(offset_ms as u64)).await;
        match step {
            Step::Trade(price) => market_tx
                .unbounded_send(reconnect::Event::Item(trade_event(offset_ms, price)))
                .unwrap(),
            Step::Order(request, cid) => execution_tx
                .send(ExecutionRequest::Open(OrderEvent {
                    key: OrderKey {
                        exchange: ExchangeIndex(0),
                        instrument: InstrumentIndex(0),
                        strategy: StrategyId::new("paper"),
                        cid: ClientOrderId::new(cid),
                    },
                    state: request,
                }))
                .unwrap(),
        }
    }
    tokio::time::sleep_until(anchor + Duration::from_millis(8_000)).await;
    assert_eq!(clock.time(), at(8_000), "the clock moves with tokio's");

    let mut account = execution.account_channel.rx;
    std::iter::from_fn(|| account.rx.try_recv().ok())
        .filter_map(|event| match event {
            reconnect::Event::Item(event) => booked(event.kind),
            reconnect::Event::Reconnecting(_) => None,
        })
        .collect()
}

/// The reference run: the backtest's market-driven venue, stepped through the same script by
/// hand, each request booked half the latency after it was sent.
fn backtest_run() -> Vec<Booked> {
    let (name, mut venue) = backtest_venue(&config());

    // What the venue owes, in the order it falls due.
    enum Input {
        Market(MarketObservation),
        Book(RequestOpen, &'static str),
    }
    let mut state = <DataKind as VenueMarketUpdate>::State::default();
    let mut inputs = script()
        .into_iter()
        .map(|(offset_ms, step)| match step {
            Step::Trade(price) => (
                offset_ms,
                Input::Market(observe(&mut state, &trade_event(offset_ms, price))),
            ),
            Step::Order(request, cid) => (
                offset_ms + (LATENCY_MS / 2) as i64,
                Input::Book(request, cid),
            ),
        })
        .collect::<Vec<_>>();
    inputs.sort_by_key(|(offset_ms, _)| *offset_ms);

    let mut events = venue.advance_time(start());
    for (offset_ms, input) in inputs {
        // Each deadline before this input falls at its own instant, as the driver wakes for it.
        while let Some(deadline) = venue
            .next_deadline()
            .filter(|deadline| *deadline < at(offset_ms))
        {
            events.extend(venue.advance_time(deadline));
        }
        match input {
            Input::Market(observation) => {
                events.extend(venue.apply_market(&name, observation, at(offset_ms)));
            }
            Input::Book(request, cid) => {
                events.extend(venue.advance_time(at(offset_ms)));
                let outcome = venue.open_order(OrderEvent {
                    key: OrderKey {
                        exchange: EXCHANGE,
                        instrument: name.clone(),
                        strategy: StrategyId::new("paper"),
                        cid: ClientOrderId::new(cid),
                    },
                    state: request,
                });
                events.extend(outcome.events);
            }
        }
    }
    while let Some(deadline) = venue
        .next_deadline()
        .filter(|deadline| *deadline <= at(8_000))
    {
        events.extend(venue.advance_time(deadline));
    }

    events
        .into_iter()
        .filter_map(|event| booked(event.kind))
        .collect()
}

/// The backtest's market-driven venue of `config`, trading the one spot instrument, and its name.
fn backtest_venue(config: &MockExecutionConfig) -> (InstrumentNameExchange, SimulatedVenue) {
    let spot = Instrument {
        exchange: EXCHANGE,
        name_internal: instrument(EXCHANGE, "btc", "usdt").name_internal,
        name_exchange: InstrumentNameExchange::new("btc_usdt"),
        underlying: Underlying::new(
            AssetNameExchange::new("btc"),
            AssetNameExchange::new("usdt"),
        ),
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Spot,
        spec: None,
        data_venue: None,
    };
    let name = spot.name_exchange.clone();
    let venue =
        SimulatedVenue::new_market_driven(config, [(name.clone(), spot)].into_iter().collect());
    (name, venue)
}

/// What a venue observes of `event`, folded into `state` as a paper venue's feed folds it.
fn observe(
    state: &mut <DataKind as VenueMarketUpdate>::State,
    event: &MarketEvent<InstrumentIndex, DataKind>,
) -> MarketObservation {
    let bar = DataKind::bar(state, event);
    let fresh = DataKind::fresh(state, event);
    DataKind::apply(state, event);
    MarketObservation::new(DataKind::snapshot(state))
        .with_depth(DataKind::depth(state))
        .with_bar(bar)
        .with_fresh(fresh)
}

/// `config()` on a margin account, lending up to three times its value in usdt.
fn margin_config() -> MockExecutionConfig {
    MockExecutionConfig {
        spot_margin: Some(
            SpotMarginConfig::new(
                AssetNameExchange::new("usdt"),
                dec!(3),
                MarginEffect::Borrow,
            )
            .unwrap(),
        ),
        ..config()
    }
}

/// A paper venue of `config`, built on tokio's paused clock and fed by the returned sender, with
/// its handle, before it is started.
fn paper_build(
    instruments: &IndexedInstruments,
    config: MockExecutionConfig,
) -> (
    ExecutionBuild,
    PaperVenue,
    futures::channel::mpsc::UnboundedSender<
        reconnect::Event<ExchangeId, MarketEvent<InstrumentIndex, DataKind>>,
    >,
) {
    let (market_tx, market_rx) = futures::channel::mpsc::unbounded();
    let (tap, feed) = PaperMarketFeed::tap(market_rx);
    // Stands in for the `Engine`'s market feed, which is what polls the tap in a live system.
    tokio::spawn(tap.for_each(|_| std::future::ready(())));
    let build = ExecutionBuilder::new(instruments)
        .add_paper(
            config,
            PaperOptions::default(),
            TokioClock::starting_at(start()),
            &feed,
        )
        .unwrap()
        .build();
    let venue = build.paper_venues.get(&EXCHANGE).unwrap().clone();
    (build, venue, market_tx)
}

/// A paper venue's handle answers how much the account can borrow as the backtest's venue does
/// on the same market, on either side.
#[tokio::test(start_paused = true)]
async fn a_paper_venue_s_handle_answers_borrow_capacity_as_the_backtest_venue_does() {
    let instruments = instruments();
    let anchor = tokio::time::Instant::now();
    let (build, venue, market_tx) = paper_build(&instruments, margin_config());
    assert_eq!(venue.exchange(), EXCHANGE);
    let execution = build.init().await.unwrap();
    assert_eq!(
        execution.paper_venues.keys().collect::<Vec<_>>(),
        [&EXCHANGE],
        "init carries the handles on"
    );

    tokio::time::sleep_until(anchor + Duration::from_millis(1_000)).await;
    let trade = trade_event(1_000, dec!(100));
    market_tx
        .unbounded_send(reconnect::Event::Item(trade.clone()))
        .unwrap();
    tokio::time::sleep_until(anchor + Duration::from_millis(1_500)).await;

    let (name, mut reference) = backtest_venue(&margin_config());
    // What the reference venue reports is not compared: only what it then answers.
    let _restated = reference.advance_time(start());
    let _observed =
        reference.apply_market(&name, observe(&mut Default::default(), &trade), at(1_000));
    for side in [Side::Sell, Side::Buy] {
        let paper = venue.fetch_borrow_capacity(&name, side).await.unwrap();
        assert_eq!(
            paper,
            reference.borrow_capacity(&name, side).unwrap(),
            "{side:?}"
        );
        assert!(
            paper
                .borrowable_now
                .is_some_and(|amount| amount > Decimal::ZERO),
            "the account can borrow on {side:?}: {paper:?}"
        );
    }
}

/// A paper venue without spot margin lends against nothing, so its handle answers that the
/// instrument cannot be borrowed against, as a live venue's client does.
#[tokio::test(start_paused = true)]
async fn a_cash_paper_venue_s_handle_answers_that_it_lends_against_no_instrument() {
    let instruments = instruments();
    let (build, venue, _market_tx) = paper_build(&instruments, config());
    let _execution = build.init().await.unwrap();

    let answer = venue
        .fetch_borrow_capacity(&InstrumentNameExchange::new("btc_usdt"), Side::Sell)
        .await;

    assert!(
        matches!(
            answer,
            Err(ClientError::Api(ApiError::InstrumentInvalid(..)))
        ),
        "{answer:?}"
    );
}

/// A query sent through the handle before the venue starts waits for it, and is answered once it
/// runs.
#[tokio::test(start_paused = true)]
async fn a_paper_venue_s_handle_queried_before_the_venue_starts_is_answered_once_it_runs() {
    let instruments = instruments();
    let (build, venue, _market_tx) = paper_build(&instruments, config());
    let query = tokio::spawn(async move {
        venue
            .fetch_borrow_capacity(&InstrumentNameExchange::new("btc_usdt"), Side::Sell)
            .await
    });
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!query.is_finished(), "the venue has not started");

    let _execution = build.init().await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(60), query)
        .await
        .expect("answered once the venue runs")
        .unwrap();

    assert!(
        matches!(
            answer,
            Err(ClientError::Api(ApiError::InstrumentInvalid(..)))
        ),
        "{answer:?}"
    );
}

/// The handle keeps no venue running: a system shuts down while one is held, and a query then
/// finds the venue offline, as it does for a build dropped without being started.
#[tokio::test(start_paused = true)]
async fn a_paper_venue_s_handle_keeps_no_venue_running() {
    let instruments = instruments();
    let name = InstrumentNameExchange::new("btc_usdt");
    let offline = |answer: Result<_, UnindexedClientError>| {
        matches!(
            answer,
            Err(ClientError::Connectivity(
                ConnectivityError::ExchangeOffline(EXCHANGE)
            ))
        )
    };

    let (build, venue, _market_tx) = paper_build(&instruments, margin_config());
    drop(build);
    assert!(offline(
        venue.fetch_borrow_capacity(&name, Side::Sell).await
    ));

    let (build, venue, _market_tx) = paper_build(&instruments, margin_config());
    let Execution {
        execution_txs,
        mut handles,
        ..
    } = build.init().await.unwrap();
    drop(execution_txs);
    tokio::time::timeout(Duration::from_secs(60), handles.shutdown())
        .await
        .expect("the venue stops with its manager, though a handle to it is held")
        .unwrap();
    assert!(offline(
        venue.fetch_borrow_capacity(&name, Side::Sell).await
    ));
}

#[tokio::test(start_paused = true)]
async fn a_paper_venue_books_what_a_backtest_of_the_same_venue_books() {
    let backtest = backtest_run();
    assert_eq!(
        backtest
            .iter()
            .filter(|booked| matches!(booked, Booked::Fill { .. }))
            .count(),
        2,
        "the script fills the resting buy and the market buy: {backtest:#?}"
    );
    assert!(
        backtest.contains(&Booked::Expired {
            cid: ClientOrderId::new("expires"),
            time: at(6_000),
        }),
        "the script expires the sell at its deadline: {backtest:#?}"
    );
    assert!(
        backtest.iter().any(|booked| matches!(
            booked,
            Booked::Fill { time, price, .. } if *time == at(3_550) && *price == dec!(96)
        )),
        "the market buy fills at the market as it arrived, not as it was sent: {backtest:#?}"
    );

    assert_eq!(paper_run().await, backtest);
}
