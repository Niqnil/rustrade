//! How a market event kind becomes a simulated venue's view of an instrument.

use crate::engine::{
    Processor,
    state::instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
};
use rustrade_data::event::{DataKind, MarketEvent};
use rustrade_execution::market::MarketSnapshot;
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
}
