//! How a market event kind becomes a simulated venue's view of an instrument.

use crate::engine::{
    Processor,
    state::instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
};
use rust_decimal::Decimal;
use rustrade_data::event::{DataKind, MarketEvent};
use rustrade_execution::market::{MarketDepth, MarketSnapshot};
use rustrade_instrument::instrument::InstrumentIndex;
use std::fmt::Debug;

/// Folds a stream of market events into the [`MarketSnapshot`] a simulated venue holds.
///
/// [`SimRunner`] routes each source market event to the venues trading that instrument before the
/// `Engine` sees it. It cannot do that generically over its `MarketKind` parameter without knowing
/// how to read a price out of one, which is what this trait supplies.
///
/// # Why it is stateful
///
/// A [`MarketSnapshot`] is a complete view — both sides of the book and a price — while a single
/// market event is almost never that. A trade carries no book; an L1 update carries no trade price;
/// a candle carries neither. Deriving a snapshot therefore needs everything seen so far, not just
/// the event in hand, which is why the state is an associated type rather than the conversion being
/// a plain function.
///
/// # Implementing it
///
/// The venue's answer must match the `Engine`'s, because the two price the same fill. The
/// [`DataKind`] implementation gets that by construction: its [`State`](Self::State) **is**
/// [`DefaultInstrumentMarketData`], and its two methods delegate to the same
/// [`Processor`] and
/// [`InstrumentDataState::market_snapshot`] the engine calls. A custom implementation that
/// re-derives prices instead takes on the burden of agreeing with whatever
/// [`InstrumentDataState`] the engine was built with — see
/// `venue_market_update_agrees_with_the_engines_instrument_state`, which pins the shipped one.
///
/// [`SimRunner`]: super::SimRunner
pub trait VenueMarketUpdate: Sized {
    /// Everything needed to answer "what is this instrument's market right now".
    type State: Default + Debug;

    /// Folds one market event into `state`.
    fn apply(state: &mut Self::State, event: &MarketEvent<InstrumentIndex, Self>);

    /// The venue-visible market implied by everything folded into `state` so far.
    fn snapshot(state: &Self::State) -> MarketSnapshot;

    /// How much is on offer at each side of [`snapshot`](Self::snapshot), where the feed says.
    ///
    /// This is what bounds a taker fill at a simulated venue. Returning a size for a side makes
    /// an arriving order fill at most that much of itself there; returning `None` leaves it
    /// uncapped.
    ///
    /// # Default body
    ///
    /// [`MarketDepth::UNKNOWN`] — no size information, which caps nothing. That is the honest
    /// answer for a state tracking prices alone, and it keeps a price-only feed filling exactly
    /// as it did before sizes existed. See [`MarketDepth`] for why absent size means unlimited
    /// rather than unfillable.
    fn depth(_state: &Self::State) -> MarketDepth {
        MarketDepth::UNKNOWN
    }
}

impl VenueMarketUpdate for DataKind {
    type State = DefaultInstrumentMarketData;

    /// Delegates to the same `Processor` impl the engine's instrument state uses, so the venue and
    /// the engine cannot fall out of step — including the recency and lookahead guards, which a
    /// second implementation would have to reproduce exactly to agree.
    fn apply(state: &mut Self::State, event: &MarketEvent<InstrumentIndex, Self>) {
        Processor::process(state, event);
    }

    fn snapshot(state: &Self::State) -> MarketSnapshot {
        InstrumentDataState::market_snapshot(state)
    }

    /// The best levels' own sizes, read off the same [`OrderBookL1`] whose prices
    /// [`snapshot`](Self::snapshot) reports — so the size and the price a fill is capped and
    /// struck at come from one observation rather than two.
    ///
    /// # A zero amount is no size *information*, not an empty book
    ///
    /// A level carrying a price and a zero amount is what a feed that publishes no sizes looks
    /// like — a bulk export, an FX quote tape — and it is reported on every row rather than on a
    /// degenerate one. `DefaultInstrumentMarketData` already reads it that way when it prices the
    /// book, falling back from the volume-weighted mid to the plain mid. Reading it as "nothing is
    /// on offer" here would stop such a feed filling anything at all, so it maps to `None` and
    /// caps nothing.
    ///
    /// An instrument with no book — a trades-only or candle feed — holds the default
    /// [`OrderBookL1`], whose levels are absent, and so reports [`MarketDepth::UNKNOWN`] by the
    /// same route.
    ///
    /// [`OrderBookL1`]: rustrade_data::subscription::book::OrderBookL1
    fn depth(state: &Self::State) -> MarketDepth {
        fn sized(level: Option<rustrade_data::books::Level>) -> Option<Decimal> {
            level
                .map(|level| level.amount)
                .filter(|amount| !amount.is_zero())
        }

        MarketDepth::new(sized(state.l1.best_bid), sized(state.l1.best_ask))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_data::{books::Level, subscription::book::OrderBookL1};

    fn book(bid: Option<Level>, ask: Option<Level>) -> DefaultInstrumentMarketData {
        DefaultInstrumentMarketData {
            l1: OrderBookL1 {
                last_update_time: DateTime::<Utc>::MIN_UTC,
                best_bid: bid,
                best_ask: ask,
            },
            ..Default::default()
        }
    }

    #[test]
    fn depth_reads_the_sizes_off_the_same_book_the_snapshot_prices_from() {
        let state = book(
            Some(Level::new(dec!(99), dec!(4))),
            Some(Level::new(dec!(101), dec!(7))),
        );

        assert_eq!(
            <DataKind as VenueMarketUpdate>::depth(&state),
            MarketDepth::new(Some(dec!(4)), Some(dec!(7)))
        );
        assert_eq!(
            <DataKind as VenueMarketUpdate>::snapshot(&state).best_ask,
            Some(dec!(101))
        );
    }

    #[test]
    fn a_feed_publishing_prices_without_sizes_reports_no_size_rather_than_no_liquidity() {
        // A bulk export or an FX quote tape: every row priced, every amount zero. Reading that as
        // an empty book would stop such a feed filling anything at all.
        let state = book(
            Some(Level::new(dec!(99), Decimal::ZERO)),
            Some(Level::new(dec!(101), Decimal::ZERO)),
        );

        assert_eq!(
            <DataKind as VenueMarketUpdate>::depth(&state),
            MarketDepth::UNKNOWN
        );
        assert!(
            <DataKind as VenueMarketUpdate>::snapshot(&state)
                .best_bid
                .is_some()
        );
    }

    #[test]
    fn one_sided_size_information_caps_only_the_side_that_has_it() {
        let state = book(
            Some(Level::new(dec!(99), Decimal::ZERO)),
            Some(Level::new(dec!(101), dec!(7))),
        );

        assert_eq!(
            <DataKind as VenueMarketUpdate>::depth(&state),
            MarketDepth::new(None, Some(dec!(7)))
        );
    }

    #[test]
    fn an_instrument_with_no_book_reports_no_size() {
        // What a trades-only or candle feed holds: the default book, whose levels are absent.
        assert_eq!(
            <DataKind as VenueMarketUpdate>::depth(&DefaultInstrumentMarketData::default()),
            MarketDepth::UNKNOWN
        );
    }
}
