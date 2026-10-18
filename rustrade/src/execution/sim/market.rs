//! How a market event kind becomes a simulated venue's view of an instrument.

use crate::engine::{
    Processor,
    state::instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
};
use rust_decimal::Decimal;
use rustrade_data::event::{DataKind, MarketEvent};
use rustrade_execution::market::{MarketBar, MarketDepth, MarketSnapshot};
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

    /// The completed bar `event` carries, if folding it into `state` takes it as a new bar, or
    /// `None` for any other event.
    ///
    /// Asked **before** [`apply`](Self::apply) folds `event` in, because only the state as it was
    /// can say whether the bar is new. What releases a market order a venue holds for the
    /// instrument's next bar under [`MarketOrderTiming::NextBar`], so a bar the fold will refuse
    /// or has already seen must answer `None`: a candle closing after its own event's instant, or
    /// one sent a second time, is not a next bar.
    ///
    /// # Default body
    ///
    /// `None`: a kind with no bars. A venue configured for `NextBar` then rejects every market
    /// order, naming the setting, rather than holding orders that nothing would ever release.
    ///
    /// [`MarketOrderTiming::NextBar`]: rustrade_execution::fill::MarketOrderTiming::NextBar
    fn bar(_state: &Self::State, _event: &MarketEvent<InstrumentIndex, Self>) -> Option<MarketBar> {
        None
    }

    /// Whether `event` shows the market trading or quoting: a trade, a quote, or a bar something
    /// traded in — see [`MarketObservation::fresh`].
    ///
    /// Asked **before** [`apply`](Self::apply) folds `event` in, as [`bar`](Self::bar) is. What a
    /// venue measures a market's age from under [`StaleMarketPolicy::Wait`], so an event the
    /// fold refuses or ignores, or one that only carries a price forward, must answer `false`.
    ///
    /// # Default body
    ///
    /// `true`: every event of a kind that says nothing more is taken as a live observation, so
    /// such a market is never stale while events keep arriving.
    ///
    /// [`MarketObservation::fresh`]: rustrade_execution::market::MarketObservation::fresh
    /// [`StaleMarketPolicy::Wait`]: rustrade_execution::fill::StaleMarketPolicy::Wait
    fn fresh(_state: &Self::State, _event: &MarketEvent<InstrumentIndex, Self>) -> bool {
        true
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

    /// A [`Candle`](DataKind::Candle) the state will keep, as the bar it describes.
    ///
    /// Decided by the same rule the `Processor` keeps a candle by, so a candle it drops — one
    /// closing after its event's instant, or no newer than the one held — releases nothing.
    fn bar(state: &Self::State, event: &MarketEvent<InstrumentIndex, Self>) -> Option<MarketBar> {
        let DataKind::Candle(candle) = &event.kind else {
            return None;
        };
        state
            .takes_candle(candle, event.time_exchange)
            .then(|| MarketBar::new(candle.open, candle.high, candle.low, candle.close))
    }

    /// A trade or an L1 book the state will keep, or a candle it will keep unless the candle
    /// reports that nothing traded in it.
    ///
    /// A candle reporting zero volume or zero trades is what a feed that fills gaps produces: the
    /// previous close carried forward. One reporting neither is fresh, since some feeds (spot FX)
    /// report no volume at all, and an unknown volume is not a zero one.
    ///
    /// L2 depth, liquidations and greeks are never fresh: the state prices from none of them.
    fn fresh(state: &Self::State, event: &MarketEvent<InstrumentIndex, Self>) -> bool {
        match &event.kind {
            DataKind::Trade(_) => state.takes_trade(event.time_exchange),
            DataKind::OrderBookL1(l1) => state.takes_l1(l1),
            DataKind::Candle(candle) => {
                state.takes_candle(candle, event.time_exchange)
                    && candle.volume != Some(Decimal::ZERO)
                    && candle.trade_count != Some(0)
            }
            DataKind::OrderBook(_) | DataKind::Liquidation(_) | DataKind::OptionGreeks(_) => false,
        }
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

    fn event(at: i64, kind: DataKind) -> MarketEvent<InstrumentIndex, DataKind> {
        MarketEvent {
            time_exchange: instant(at),
            time_received: instant(at),
            exchange: rustrade_instrument::exchange::ExchangeId::Simulated,
            instrument: InstrumentIndex::new(0),
            kind,
        }
    }

    fn instant(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000 + seconds, 0).unwrap()
    }

    fn trade(price: Decimal) -> DataKind {
        DataKind::Trade(rustrade_data::subscription::trade::PublicTrade {
            id: "1".into(),
            price,
            amount: dec!(1),
            side: None,
        })
    }

    fn l1(at: i64) -> DataKind {
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time: instant(at),
            best_bid: Some(Level::new(dec!(99), dec!(1))),
            best_ask: Some(Level::new(dec!(101), dec!(1))),
        })
    }

    fn candle(close_at: i64, volume: Option<Decimal>, trade_count: Option<u64>) -> DataKind {
        DataKind::Candle(rustrade_data::subscription::candle::Candle {
            close_time: instant(close_at),
            open: dec!(100),
            high: dec!(100),
            low: dec!(100),
            close: dec!(100),
            volume,
            trade_count,
        })
    }

    /// Whether `event` is fresh to `state`, then `state` with it folded in.
    fn fresh(
        state: &mut DefaultInstrumentMarketData,
        event: MarketEvent<InstrumentIndex, DataKind>,
    ) -> bool {
        let fresh = <DataKind as VenueMarketUpdate>::fresh(state, &event);
        <DataKind as VenueMarketUpdate>::apply(state, &event);
        fresh
    }

    #[test]
    fn a_trade_or_a_quote_is_fresh_unless_the_state_already_holds_a_newer_one() {
        let mut state = DefaultInstrumentMarketData::default();
        assert!(fresh(&mut state, event(2, trade(dec!(100)))));
        assert!(!fresh(&mut state, event(2, trade(dec!(101)))), "no newer");
        assert!(!fresh(&mut state, event(1, trade(dec!(101)))), "older");

        assert!(fresh(&mut state, event(2, l1(2))));
        assert!(!fresh(&mut state, event(3, l1(2))), "the same book again");
        assert!(fresh(&mut state, event(3, l1(3))));
    }

    #[test]
    fn a_candle_is_fresh_unless_it_reports_that_nothing_traded_in_it() {
        for (volume, trade_count, expected) in [
            (Some(dec!(5)), Some(3), true),
            (None, None, true),
            (Some(dec!(5)), None, true),
            (None, Some(3), true),
            (Some(Decimal::ZERO), None, false),
            (None, Some(0), false),
            (Some(Decimal::ZERO), Some(0), false),
        ] {
            let mut state = DefaultInstrumentMarketData::default();
            assert_eq!(
                fresh(&mut state, event(1, candle(1, volume, trade_count))),
                expected,
                "volume {volume:?}, trade count {trade_count:?}"
            );
        }
    }

    #[test]
    fn a_candle_the_state_refuses_is_not_fresh() {
        let mut state = DefaultInstrumentMarketData::default();
        assert!(
            !fresh(&mut state, event(1, candle(2, Some(dec!(5)), None))),
            "closes after its event"
        );
        assert!(fresh(&mut state, event(2, candle(2, Some(dec!(5)), None))));
        assert!(
            !fresh(&mut state, event(3, candle(2, Some(dec!(5)), None))),
            "seen already"
        );
    }

    #[test]
    fn depth_liquidations_and_greeks_are_never_fresh() {
        let mut state = DefaultInstrumentMarketData::default();
        let liquidation =
            DataKind::Liquidation(rustrade_data::subscription::liquidation::Liquidation {
                side: rustrade_instrument::Side::Sell,
                price: dec!(90),
                quantity: dec!(1),
                time: instant(1),
            });
        assert!(!fresh(&mut state, event(1, liquidation)));
        assert!(!fresh(
            &mut state,
            event(
                1,
                DataKind::OrderBook(rustrade_data::subscription::book::OrderBookEvent::Snapshot(
                    Default::default()
                ))
            )
        ));
        assert!(!fresh(
            &mut state,
            event(1, DataKind::OptionGreeks(Default::default()))
        ));
    }

    /// A kind that carries a last price and no bars, implementing only what it must.
    #[derive(Debug)]
    struct LastPrice(Decimal);

    impl VenueMarketUpdate for LastPrice {
        type State = Option<Decimal>;

        fn apply(state: &mut Self::State, event: &MarketEvent<InstrumentIndex, Self>) {
            *state = Some(event.kind.0);
        }

        fn snapshot(state: &Self::State) -> MarketSnapshot {
            MarketSnapshot::from_last_price(*state)
        }
    }

    /// A kind without bars reports none, so a venue holding market orders for the next bar
    /// refuses them rather than holding them for a bar this kind can never deliver.
    #[test]
    fn a_kind_without_bars_reports_no_bar() {
        let event = MarketEvent {
            time_exchange: DateTime::<Utc>::MIN_UTC,
            time_received: DateTime::<Utc>::MIN_UTC,
            exchange: rustrade_instrument::exchange::ExchangeId::Simulated,
            instrument: InstrumentIndex::new(0),
            kind: LastPrice(dec!(100)),
        };

        assert_eq!(<LastPrice as VenueMarketUpdate>::bar(&None, &event), None);
        assert_eq!(
            <LastPrice as VenueMarketUpdate>::bar(&Some(dec!(99)), &event),
            None
        );
    }

    /// A kind that says nothing about freshness is taken as live on every event, so a venue never
    /// finds its market stale while its events keep arriving.
    #[test]
    fn a_kind_that_says_nothing_about_freshness_is_always_fresh() {
        let event = MarketEvent {
            time_exchange: DateTime::<Utc>::MIN_UTC,
            time_received: DateTime::<Utc>::MIN_UTC,
            exchange: rustrade_instrument::exchange::ExchangeId::Simulated,
            instrument: InstrumentIndex::new(0),
            kind: LastPrice(dec!(100)),
        };

        assert!(<LastPrice as VenueMarketUpdate>::fresh(&None, &event));
    }
}
