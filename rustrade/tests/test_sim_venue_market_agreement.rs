#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! The venue's market and the engine's must be the same market.
//!
//! A simulated venue and the `Engine` price the same fill. The `Engine` samples
//! [`InstrumentDataState::market_snapshot`] when it emits an order request and carries the result
//! on `RequestOpen::market`; the venue folds the same stream through [`VenueMarketUpdate`] into its
//! own view. Two derivations of one number is one derivation too many — if they drift, a backtest
//! matches at a price the engine never saw, and nothing says so.
//!
//! The shipped [`DataKind`] implementation is not a second derivation: it delegates to the same
//! `Processor` and the same `market_snapshot` the engine calls, so agreement holds by construction.
//! This pins that, because the cheapest way to break it is to "optimise" the impl into a
//! hand-rolled match on `DataKind` that looks equivalent and is not.
//!
//! # Why it matters more than it looks
//!
//! `DefaultInstrumentMarketData::price` is **not** the last trade price. It prefers the L1
//! volume-weighted mid — the microprice — over the last trade, and ranks a candle close against
//! whichever of those is fresher. Replayed over the committed fixture, a venue deriving
//! `last_price` as "the most recent trade" instead diverges from the microprice by a median of
//! 1.5–1.7x the **half-spread**, exceeding it around 60% of the time. That is small in basis points
//! and large in the only unit a fill is measured in.
//!
//! # Fixture
//!
//! The Binance spot L1-and-trades capture that ships with the examples: 50,000 events across three
//! instruments. It is the only market-data fixture in this repo that may be redistributed.

use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use rust_decimal::Decimal;
use rustrade::{
    engine::{
        Processor,
        state::instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
    },
    execution::sim::VenueMarketUpdate,
};
use rustrade_data::{event::DataKind, streams::consumer::MarketStreamEvent};
use rustrade_instrument::instrument::InstrumentIndex;

const FILE_PATH_MARKET_DATA: &str =
    "examples/data/binance_spot_trades_l1_btcusdt_ethusdt_solusdt.json";

/// Every event in the fixture, in recorded order.
///
/// Deliberately **not** sorted, unlike the result-stability harness. Both state machines apply
/// their own recency guards, and feeding them out-of-order events is what exercises those guards
/// against each other — a divergence in `DefaultInstrumentMarketData`'s "ignore anything older than
/// what is held" rule would otherwise be invisible.
fn market_data() -> Vec<MarketStreamEvent<InstrumentIndex, DataKind>> {
    let file = File::open(FILE_PATH_MARKET_DATA).expect("market data fixture must be readable");
    BufReader::new(file)
        .lines()
        .map(|line| {
            serde_json::from_str(&line.expect("market data fixture must be readable"))
                .expect("every market data fixture line must parse")
        })
        .collect()
}

/// Replaying one stream through both derivations must agree after every single event.
///
/// Compared after each event rather than only at the end: a divergence that heals before the last
/// event is still a divergence, and it is exactly the kind a fill lands in the middle of.
#[test]
fn venue_market_update_agrees_with_the_engines_instrument_state() {
    let events = market_data();
    assert!(
        events.len() > 1_000,
        "the fixture must be substantial enough to exercise both state machines, got {}",
        events.len()
    );

    // Keyed by instrument: the fixture interleaves three, and folding them into one state would
    // compare two wrong answers to each other.
    let mut engine_states: Vec<(InstrumentIndex, DefaultInstrumentMarketData)> = Vec::new();
    let mut venue_states: Vec<(InstrumentIndex, <DataKind as VenueMarketUpdate>::State)> =
        Vec::new();

    let mut compared = 0_usize;

    for (index, event) in events.iter().enumerate() {
        let MarketStreamEvent::Item(event) = event else {
            continue;
        };

        let engine = match engine_states
            .iter_mut()
            .find(|(key, _)| *key == event.instrument)
        {
            Some((_, state)) => state,
            None => {
                engine_states.push((event.instrument, DefaultInstrumentMarketData::default()));
                &mut engine_states.last_mut().expect("just pushed").1
            }
        };
        Processor::process(engine, event);

        let venue = match venue_states
            .iter_mut()
            .find(|(key, _)| *key == event.instrument)
        {
            Some((_, state)) => state,
            None => {
                venue_states.push((event.instrument, Default::default()));
                &mut venue_states.last_mut().expect("just pushed").1
            }
        };
        <DataKind as VenueMarketUpdate>::apply(venue, event);

        assert_eq!(
            <DataKind as VenueMarketUpdate>::snapshot(venue),
            InstrumentDataState::market_snapshot(engine),
            "the venue's market diverged from the engine's at event {index} \
             (instrument {:?}, time_exchange {})",
            event.instrument,
            event.time_exchange
        );
        compared += 1;
    }

    assert_eq!(
        compared,
        events.len(),
        "the fixture is Item-only, so every event must have been compared"
    );
    assert_eq!(
        engine_states.len(),
        3,
        "the fixture covers three instruments"
    );
}

/// The size a venue caps a taker by is the size on the very book the snapshot priced from.
///
/// [`VenueMarketUpdate::depth`] and [`VenueMarketUpdate::snapshot`] must read one observation, not
/// two: a fill struck at a price from one book and capped by a size from another is a fill that
/// never existed. They agree by construction today — both project the same held [`OrderBookL1`] —
/// and this pins that, because the cheapest way to break it is to start tracking sizes separately.
///
/// # Why this needs the real fixture
///
/// `sim_venue_stability_summary.txt` cannot catch a regression here. Its 40 orders buy `0.001`,
/// and at the instants they arrive the book is always deeper than that, so the cap never binds and
/// the golden artifact is identical whether depth reaches the venue correctly, arrives as garbage,
/// or never arrives at all. Depth that silently stopped flowing would look exactly like depth that
/// flowed and never bit.
///
/// So this asserts the two things that artifact cannot: that the sizes are really there, and that
/// a taker large enough would really be capped by them.
#[test]
fn venue_depth_is_the_size_on_the_book_the_venue_priced_from() {
    let events = market_data();

    let mut states: Vec<(InstrumentIndex, <DataKind as VenueMarketUpdate>::State)> = Vec::new();
    let mut sized = 0_usize;
    let mut thinner_than_a_tenth = 0_usize;

    for (index, event) in events.iter().enumerate() {
        let MarketStreamEvent::Item(event) = event else {
            continue;
        };

        let state = match states.iter_mut().find(|(key, _)| *key == event.instrument) {
            Some((_, state)) => state,
            None => {
                states.push((event.instrument, Default::default()));
                &mut states.last_mut().expect("just pushed").1
            }
        };
        <DataKind as VenueMarketUpdate>::apply(state, event);

        let depth = <DataKind as VenueMarketUpdate>::depth(state);
        let snapshot = <DataKind as VenueMarketUpdate>::snapshot(state);

        // One observation, read twice: the price and the size must come off the same level.
        assert_eq!(
            depth.best_ask,
            state
                .l1
                .best_ask
                .map(|level| level.amount)
                .filter(|amount| !amount.is_zero()),
            "the venue's ask size diverged from the book it prices from at event {index}"
        );
        assert_eq!(
            depth.best_bid,
            state
                .l1
                .best_bid
                .map(|level| level.amount)
                .filter(|amount| !amount.is_zero()),
            "the venue's bid size diverged from the book it prices from at event {index}"
        );
        assert_eq!(
            depth.best_ask.is_some(),
            snapshot.best_ask.is_some(),
            "a priced level and a sized level are the same level at event {index}"
        );

        if let Some(ask) = depth.best_ask {
            sized += 1;
            // A tenth of a Bitcoin is a size a real taker sends, and this capture is thin enough
            // to refuse it outright often enough to matter.
            if ask < Decimal::new(1, 1) && event.instrument == InstrumentIndex(0) {
                thinner_than_a_tenth += 1;
            }
        }
    }

    assert!(
        sized > 30_000,
        "this fixture is mostly L1 and every row of it carries sizes, got {sized} sized \
         observations"
    );
    assert!(
        thinner_than_a_tenth > 0,
        "the cap must be reachable on committed data, or nothing in this repo exercises it \
         end to end"
    );
}

/// `last_price` is the engine's considered mark, not the most recent trade print.
///
/// The single most consequential fact about this agreement, and the one a reimplementation is most
/// likely to get wrong, because "last price" reads like "last trade". With both sides of an L1 book
/// present it is the volume-weighted mid, and the trade that just printed does not appear in the
/// answer at all.
#[test]
fn last_price_is_the_microprice_not_the_last_trade() {
    let events = market_data();

    let mut state = DefaultInstrumentMarketData::default();
    let mut divergences = 0_usize;
    let mut last_trade = None;

    for event in events.iter().take(20_000) {
        let MarketStreamEvent::Item(event) = event else {
            continue;
        };
        // One instrument only: mixing three would compare a trade on one against a book on another.
        if event.instrument != InstrumentIndex(0) {
            continue;
        }

        if let DataKind::Trade(trade) = &event.kind {
            last_trade = Some(trade.price);
        }
        Processor::process(&mut state, event);

        let snapshot = <DataKind as VenueMarketUpdate>::snapshot(&state);
        if let (Some(last_price), Some(trade)) = (snapshot.last_price, last_trade)
            && last_price != trade
        {
            divergences += 1;
        }
    }

    assert!(
        divergences > 0,
        "`last_price` must not be the last trade price — if this passes vacuously the fixture \
         stopped carrying an L1 book, and the agreement test above is no longer testing the \
         derivation it was written for"
    );
}
