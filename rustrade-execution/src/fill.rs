use crate::market::{MarketBar, MarketSnapshot};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Everything a [`FillModel`] may price from.
///
/// # Fields are added, never removed
///
/// An implementation only ever reads this, so every increase in the simulated venue's fidelity —
/// sizes at the touch, depth, a queue position — arrives as a new field rather than as a new
/// parameter on [`fill_price`](FillModel::fill_price). That is what `#[non_exhaustive]` is for
/// here: constructing one outside this crate would turn each such addition back into a breaking
/// change, which is the churn this type exists to end.
///
/// The order's limit price is deliberately absent — see [`FillModel`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FillContext<'a> {
    /// Which way the order trades.
    pub side: Side,
    /// How much the order asks to trade, which a size-aware impact model prices from.
    ///
    /// This is the order's whole quantity. Where the book shows less size than that, the venue
    /// fills only what the book has and prices that part at what the model returns, so a model
    /// reading this prices the order as if all of it traded.
    pub quantity: Decimal,
    /// The venue's view of the instrument. Every field is optional and absence is ordinary; see
    /// [`MarketSnapshot`].
    pub market: &'a MarketSnapshot,
}

impl<'a> FillContext<'a> {
    pub fn new(side: Side, quantity: Decimal, market: &'a MarketSnapshot) -> Self {
        Self {
            side,
            quantity,
            market,
        }
    }
}

/// Prices a fill the venue has already decided will happen.
///
/// # Backtest-only
///
/// Only simulated execution consults a `FillModel`. Live execution clients receive real fill
/// prices from the venue.
///
/// # Called only for a taker
///
/// A market order, or a limit order that is marketable when it arrives. A resting order matched
/// later fills at *its own limit price* and never reaches a fill model: a maker is paid the price
/// it quoted, and price improvement accrues to the aggressor that crossed it.
///
/// # This model does not see the limit price, and must not
///
/// A limit constrains the *result*, not the pricing. The venue clamps whatever is returned to the
/// order's limit — never above a buy's, never below a sell's — so an implementation models
/// slippage and spread and cannot produce a fill outside the order's terms.
///
/// Reading the limit here is how two of the three models this crate ships came to return the limit
/// *itself* for a marketable order: a buy limit of 51,000 arriving against an ask of 50,000 filled
/// at 51,000 — a worse price than a market order got under the same configuration. The third
/// returned the midpoint even when it sat above a buy's limit. Removing the parameter is what
/// makes that class of error unwritable rather than merely documented.
///
/// # Extensibility
///
/// Implement this to model market impact or other execution dynamics, and hand it to the venue
/// with [`SimulatedVenue::with_fill_model`]. The built-in models ([`LastPriceFillModel`],
/// [`BidAskFillModel`], [`MidpointFillModel`]) are baselines, and [`SimFillConfig`] selects one of
/// them from a serialisable configuration.
///
/// Adverse slippage, fixed or drawn at random, needs no model of its own: the venue applies its
/// [`SlippageConfig`] to whatever price the model returns, whichever model that is.
///
/// # It must be a pure function of its context
///
/// The venue may call [`fill_price`](Self::fill_price) more than once for one order, and for
/// orders that never fill: it asks, for instance, whether a short sale is priceable at all before
/// deciding to refuse it. So the same context must always give the same price, which is why this
/// takes `&self`. Randomness belongs in the venue's [`SlippageConfig`], whose seeded stream
/// advances exactly once per fill.
///
/// The price returned must be a real market price: the venue's arithmetic on it, slippage and
/// settlement included, panics on `Decimal` overflow.
///
/// # It fills optimistically by default
///
/// The venue prices a taker at the market as it stands when the order arrives, which in a backtest
/// on bars is the close of the bar the strategy has just seen. That is not look-ahead, since the
/// close is known when deciding, but it is optimistic: it assumes trading at an observed price with
/// no latency and, unless a model or slippage says otherwise, no cost of crossing.
///
/// # Returns
///
/// `None` when the context carries nothing this model can price from — no prices at all on the
/// first ticks of a backtest, say. The venue turns that into a rejection naming the instrument
/// rather than a panic.
///
/// [`SimulatedVenue::with_fill_model`]: crate::exchange::mock::SimulatedVenue::with_fill_model
/// [`SlippageConfig`]: crate::slippage::SlippageConfig
pub trait FillModel: Debug + Send + Sync {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal>;
}

/// Fills at the last traded price, falling back to the price a taker would have to cross to.
///
/// Fallback chain: `last_price` → `best_ask` (Buy) / `best_bid` (Sell).
///
/// The simplest fill model — it ignores the spread entirely. Useful when spread modelling is
/// handled elsewhere, or when deterministic fills are preferred.
///
/// Note that with both sides of an L1 book present, `last_price` is the *microprice* rather than
/// the most recent print; see [`MarketSnapshot`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct LastPriceFillModel;

impl FillModel for LastPriceFillModel {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal> {
        fill.market.last_price.or(match fill.side {
            Side::Buy => fill.market.best_ask,
            Side::Sell => fill.market.best_bid,
        })
    }
}

/// Fills at the current best ask (buys) or best bid (sells), crossing the spread as a taker does.
///
/// Falls back to `last_price` when the relevant side of the book is absent. More realistic than
/// [`LastPriceFillModel`] for strategies that frequently cross the spread.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct BidAskFillModel;

impl FillModel for BidAskFillModel {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal> {
        match fill.side {
            Side::Buy => fill.market.best_ask.or(fill.market.last_price),
            Side::Sell => fill.market.best_bid.or(fill.market.last_price),
        }
    }
}

/// Fills at the midpoint of best bid and best ask, falling back to `last_price`.
///
/// Useful when modelling execution quality between taker (crossing the spread) and maker (resting
/// at the quote), or when both sides of the book are always present in the backtest feed.
///
/// # It is optimistic for a taker
///
/// A midpoint is not a price anyone is offering, so a crossing order modelled this way saves half
/// the spread it would really have paid. The venue bounds the result by the order's own limit;
/// nothing bounds it by the touch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct MidpointFillModel;

impl FillModel for MidpointFillModel {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal> {
        fill.market.mid_price().or(fill.market.last_price)
    }
}

/// Selects one of the built-in fill models from a serialisable configuration, as
/// [`MockExecutionConfig::fill_model`] does. Defaults to [`LastPriceFillModel`].
///
/// A model of your own cannot be named here; hand it to the venue with
/// [`SimulatedVenue::with_fill_model`] instead.
///
/// [`MockExecutionConfig::fill_model`]: crate::client::mock::MockExecutionConfig::fill_model
/// [`SimulatedVenue::with_fill_model`]: crate::exchange::mock::SimulatedVenue::with_fill_model
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum SimFillConfig {
    LastPrice(LastPriceFillModel),
    BidAsk(BidAskFillModel),
    Midpoint(MidpointFillModel),
}

impl Default for SimFillConfig {
    fn default() -> Self {
        Self::LastPrice(LastPriceFillModel)
    }
}

impl FillModel for SimFillConfig {
    fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal> {
        match self {
            SimFillConfig::LastPrice(model) => model.fill_price(fill),
            SimFillConfig::BidAsk(model) => model.fill_price(fill),
            SimFillConfig::Midpoint(model) => model.fill_price(fill),
        }
    }
}

/// When a simulated venue fills a market order, as [`MockExecutionConfig::market_order_timing`]
/// selects. Defaults to [`OnArrival`](Self::OnArrival).
///
/// [`MockExecutionConfig::market_order_timing`]: crate::client::mock::MockExecutionConfig::market_order_timing
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum MarketOrderTiming {
    /// Fill the moment the order reaches the venue, against the market it holds then.
    ///
    /// On a bar feed with no latency, that market is the close of the bar the sender has just
    /// seen: the order trades at exactly the price it decided on, with no time to react, no
    /// spread crossed and no impact. A common convention, and an optimistic one — a strategy can
    /// come to depend on it. [`NextBar`](Self::NextBar) is the pessimistic comparison.
    #[default]
    OnArrival,

    /// Hold the order until its instrument's next bar, and fill it at that bar's price.
    ///
    /// For bar data only: the venue must be fed bars, through the `bar` argument of
    /// [`SimulatedVenue::apply_market`](crate::exchange::mock::SimulatedVenue::apply_market).
    /// Anything else fails visibly rather than falling back:
    /// - a market order on an instrument the venue has never been fed a bar for is rejected, naming
    ///   this setting. That includes every market order on a
    ///   [`RequestPriced`](crate::exchange::mock::venue::VenueRegime::RequestPriced) venue, which
    ///   has no feed at all;
    /// - a tick or quote between two bars moves the market but does not release a held order.
    ///
    /// # While it is held
    /// The order is accepted as `Open`, so a cancel takes it back as it would a resting order. The
    /// venue holds what the order would cost at its current price, as it holds a limit order's
    /// cost at its limit, so two held orders cannot both spend one balance. An order the account
    /// cannot cover at that price is refused on arrival, as it is under
    /// [`OnArrival`](Self::OnArrival).
    ///
    /// # When the next bar arrives
    /// The hold is released and the order filled as a taker, at the reference price
    /// [`NextBarPrice`] picks, priced by the venue's [`FillModel`] and moved by its slippage like
    /// any other taker fill. The fill is costed afresh, and an order the account can no longer
    /// afford is cancelled and reported. Its time in force decides the rest as on arrival:
    /// immediate-or-cancel keeps what the bar's size allows and cancels the remainder,
    /// fill-or-kill trades all of it or nothing. The fill is stamped at the bar's instant.
    ///
    /// # When the data ends first
    /// An order still held when its driver runs out of data is retired as expired, filled
    /// nothing, so every accepted order is answered.
    ///
    /// Only market orders are held. A limit order rests and matches as it always does.
    NextBar(NextBarPrice),
}

/// Which price of the next bar fills a market order held for it. See
/// [`MarketOrderTiming::NextBar`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum NextBarPrice {
    /// The next bar's open: the first price the market traded at after the order was sent. The
    /// realistic choice.
    Open,

    /// The next bar's worst price for the order: its high for a buy, its low for a sell.
    ///
    /// A deliberately **pessimistic bound**, not a model. A market order fills near the start of
    /// the next bar, not at its extreme, so this overstates the cost of trading, and the more so
    /// the longer or more volatile the bar. Its use is as a stress test: a strategy that still
    /// makes money under it does not owe its edge to fill quality. It already charges more than
    /// any slippage would, so combining it with slippage counts the same cost twice.
    AdverseExtreme,
}

impl NextBarPrice {
    /// The reference price `bar` gives an order on `side`.
    pub fn reference(self, side: Side, bar: &MarketBar) -> Decimal {
        match (self, side) {
            (Self::Open, _) => bar.open,
            (Self::AdverseExtreme, Side::Buy) => bar.high,
            (Self::AdverseExtreme, Side::Sell) => bar.low,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn the_next_bar_price_is_its_open_or_its_worst_for_the_side() {
        let bar = MarketBar::new(d("100"), d("110"), d("90"), d("105"));

        assert_eq!(NextBarPrice::Open.reference(Side::Buy, &bar), d("100"));
        assert_eq!(NextBarPrice::Open.reference(Side::Sell, &bar), d("100"));
        assert_eq!(
            NextBarPrice::AdverseExtreme.reference(Side::Buy, &bar),
            d("110")
        );
        assert_eq!(
            NextBarPrice::AdverseExtreme.reference(Side::Sell, &bar),
            d("90")
        );
    }

    #[test]
    fn market_order_timing_defaults_to_on_arrival_and_round_trips() {
        assert_eq!(MarketOrderTiming::default(), MarketOrderTiming::OnArrival);

        for timing in [
            MarketOrderTiming::OnArrival,
            MarketOrderTiming::NextBar(NextBarPrice::Open),
            MarketOrderTiming::NextBar(NextBarPrice::AdverseExtreme),
        ] {
            let json = serde_json::to_string(&timing).unwrap();
            assert_eq!(
                serde_json::from_str::<MarketOrderTiming>(&json).unwrap(),
                timing
            );
        }
    }

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// Bid 99.5 / ask 100.5 / last 100.0 — a complete snapshot, mid also 100.0.
    fn prices() -> MarketSnapshot {
        MarketSnapshot::new(Some(d("99.5")), Some(d("100.5")), Some(d("100.0")))
    }

    /// A snapshot from three literal prices, for the partial-book cases.
    fn snapshot(
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        last: Option<Decimal>,
    ) -> MarketSnapshot {
        MarketSnapshot::new(bid, ask, last)
    }

    #[test]
    fn last_price_market_buy_uses_last() {
        let market = prices();
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            Some(d("100.0"))
        );
    }

    #[test]
    fn bid_ask_market_buy_uses_ask() {
        let market = prices();
        assert_eq!(
            BidAskFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            Some(d("100.5"))
        );
    }

    #[test]
    fn bid_ask_market_sell_uses_bid() {
        let market = prices();
        assert_eq!(
            BidAskFillModel.fill_price(&FillContext::new(Side::Sell, Decimal::ONE, &market)),
            Some(d("99.5"))
        );
    }

    #[test]
    fn midpoint_uses_mid() {
        let market = prices();
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_falls_back_to_last_when_no_bid_ask() {
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(
                Side::Buy,
                Decimal::ONE,
                &snapshot(None, None, Some(d("100.0")))
            )),
            Some(d("100.0"))
        );
    }

    // --- SimFillConfig enum dispatch ---

    #[test]
    fn fill_model_config_last_price_dispatches() {
        let market = prices();
        let cfg = SimFillConfig::LastPrice(LastPriceFillModel);
        assert_eq!(
            cfg.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market))
        );
    }

    #[test]
    fn fill_model_config_bid_ask_dispatches() {
        let market = prices();
        let cfg = SimFillConfig::BidAsk(BidAskFillModel);
        assert_eq!(
            cfg.fill_price(&FillContext::new(Side::Sell, Decimal::ONE, &market)),
            BidAskFillModel.fill_price(&FillContext::new(Side::Sell, Decimal::ONE, &market))
        );
    }

    #[test]
    fn fill_model_config_midpoint_dispatches() {
        let market = prices();
        let cfg = SimFillConfig::Midpoint(MidpointFillModel);
        assert_eq!(
            cfg.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market))
        );
    }

    #[test]
    fn fill_model_config_default_is_last_price() {
        assert_eq!(
            SimFillConfig::default(),
            SimFillConfig::LastPrice(LastPriceFillModel)
        );
    }

    // --- Edge cases ---

    #[test]
    fn last_price_all_none_returns_none() {
        // No market data at all — e.g. first tick of a backtest before any prices arrive.
        // The mock exchange falls back to request.state.price when fill_price returns None.
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(
                Side::Buy,
                Decimal::ONE,
                &snapshot(None, None, None)
            )),
            None
        );
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(
                Side::Sell,
                Decimal::ONE,
                &snapshot(None, None, None)
            )),
            None
        );
    }

    #[test]
    fn last_price_falls_back_to_bid_ask_when_no_last_price() {
        // When last_price=None but bid/ask are present, the model falls back to
        // bid/ask (as documented in the fallback chain). This exercises the tertiary
        // fallback that was previously untested.
        let market = snapshot(Some(d("99.5")), Some(d("100.5")), None);
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            Some(d("100.5")),
            "Buy with no last_price should fall back to best_ask"
        );
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(Side::Sell, Decimal::ONE, &market)),
            Some(d("99.5")),
            "Sell with no last_price should fall back to best_bid"
        );
    }

    /// A partial book falls back to the last price, whichever side is missing.
    ///
    /// These replace a pair that asserted the *limit* beat a stale `last_price` on a partial book.
    /// That special case existed only to stop a limit buy filling above its own limit, which is now
    /// the venue's job — and doing it here was what let the complete-book arm keep filling above a
    /// limit unnoticed.
    #[test]
    fn midpoint_with_only_bid_falls_back_to_last() {
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(
                Side::Buy,
                Decimal::ONE,
                &snapshot(Some(d("99.5")), None, Some(d("100.0")))
            )),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_with_only_ask_falls_back_to_last() {
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(
                Side::Sell,
                Decimal::ONE,
                &snapshot(None, Some(d("100.5")), Some(d("100.0")))
            )),
            Some(d("100.0"))
        );
    }

    /// A complete book prices at the midpoint even where that sits far from the last print.
    ///
    /// This case had no coverage at all, and it is where the model used to fill a limit buy above
    /// its own limit: `mid_price()` won outright and the limit was never consulted. There is no
    /// limit to consult now — bounding the result is the venue's job — so what this pins is that
    /// the midpoint wins over `last_price`, which is the whole content of the model.
    #[test]
    fn midpoint_complete_book_prices_at_mid_not_last() {
        let market = snapshot(Some(d("99.5")), Some(d("100.5")), Some(d("110.0")));
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, Decimal::ONE, &market)),
            Some(d("100.0")),
            "the midpoint of 99.5/100.5, not the stale 110.0 print"
        );
    }

    /// No model reads the side it is not given, and none invents a price from nothing.
    #[test]
    fn every_model_returns_none_on_an_empty_snapshot() {
        let empty = MarketSnapshot::default();
        for side in [Side::Buy, Side::Sell] {
            let fill = FillContext::new(side, Decimal::ONE, &empty);
            assert_eq!(LastPriceFillModel.fill_price(&fill), None);
            assert_eq!(BidAskFillModel.fill_price(&fill), None);
            assert_eq!(MidpointFillModel.fill_price(&fill), None);
        }
    }
}
