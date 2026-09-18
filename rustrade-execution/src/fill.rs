use crate::market::MarketSnapshot;
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Serialize};

/// Everything a [`FillModel`] may price from.
///
/// # Fields are added, never removed
///
/// An implementation only ever reads this, so every increase in the simulated venue's fidelity —
/// the order's quantity for a size-aware impact model, sizes at the touch, depth, a queue
/// position — arrives as a new field rather than as a new parameter on
/// [`fill_price`](FillModel::fill_price). That is what `#[non_exhaustive]` is for here:
/// constructing one outside this crate would turn each such addition back into a breaking change,
/// which is the churn this type exists to end.
///
/// The order's limit price is deliberately absent — see [`FillModel`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FillContext<'a> {
    /// Which way the order trades.
    pub side: Side,
    /// The venue's view of the instrument. Every field is optional and absence is ordinary; see
    /// [`MarketSnapshot`].
    pub market: &'a MarketSnapshot,
}

impl<'a> FillContext<'a> {
    pub fn new(side: Side, market: &'a MarketSnapshot) -> Self {
        Self { side, market }
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
/// Implement this to model slippage, market impact, or other execution dynamics. The built-in
/// models ([`LastPriceFillModel`], [`BidAskFillModel`], [`MidpointFillModel`]) are baselines.
///
/// # Returns
///
/// `None` when the context carries nothing this model can price from — no prices at all on the
/// first ticks of a backtest, say. The venue turns that into a rejection naming the instrument
/// rather than a panic.
pub trait FillModel {
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

/// Enum-dispatched fill model for use in types that require `Clone`,
/// `Serialize`, and `Deserialize` (e.g. `MockExchangeConfig`).
///
/// Prefer this over `Box<dyn FillModel>` when the field must be part of
/// a derived `serde` struct. Defaults to [`LastPriceFillModel`].
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

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

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
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, &market)),
            Some(d("100.0"))
        );
    }

    #[test]
    fn bid_ask_market_buy_uses_ask() {
        let market = prices();
        assert_eq!(
            BidAskFillModel.fill_price(&FillContext::new(Side::Buy, &market)),
            Some(d("100.5"))
        );
    }

    #[test]
    fn bid_ask_market_sell_uses_bid() {
        let market = prices();
        assert_eq!(
            BidAskFillModel.fill_price(&FillContext::new(Side::Sell, &market)),
            Some(d("99.5"))
        );
    }

    #[test]
    fn midpoint_uses_mid() {
        let market = prices();
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, &market)),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_falls_back_to_last_when_no_bid_ask() {
        assert_eq!(
            MidpointFillModel.fill_price(&FillContext::new(
                Side::Buy,
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
            cfg.fill_price(&FillContext::new(Side::Buy, &market)),
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, &market))
        );
    }

    #[test]
    fn fill_model_config_bid_ask_dispatches() {
        let market = prices();
        let cfg = SimFillConfig::BidAsk(BidAskFillModel);
        assert_eq!(
            cfg.fill_price(&FillContext::new(Side::Sell, &market)),
            BidAskFillModel.fill_price(&FillContext::new(Side::Sell, &market))
        );
    }

    #[test]
    fn fill_model_config_midpoint_dispatches() {
        let market = prices();
        let cfg = SimFillConfig::Midpoint(MidpointFillModel);
        assert_eq!(
            cfg.fill_price(&FillContext::new(Side::Buy, &market)),
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, &market))
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
            LastPriceFillModel
                .fill_price(&FillContext::new(Side::Buy, &snapshot(None, None, None))),
            None
        );
        assert_eq!(
            LastPriceFillModel
                .fill_price(&FillContext::new(Side::Sell, &snapshot(None, None, None))),
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
            LastPriceFillModel.fill_price(&FillContext::new(Side::Buy, &market)),
            Some(d("100.5")),
            "Buy with no last_price should fall back to best_ask"
        );
        assert_eq!(
            LastPriceFillModel.fill_price(&FillContext::new(Side::Sell, &market)),
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
            MidpointFillModel.fill_price(&FillContext::new(Side::Buy, &market)),
            Some(d("100.0")),
            "the midpoint of 99.5/100.5, not the stale 110.0 print"
        );
    }

    /// No model reads the side it is not given, and none invents a price from nothing.
    #[test]
    fn every_model_returns_none_on_an_empty_snapshot() {
        let empty = MarketSnapshot::default();
        for side in [Side::Buy, Side::Sell] {
            let fill = FillContext::new(side, &empty);
            assert_eq!(LastPriceFillModel.fill_price(&fill), None);
            assert_eq!(BidAskFillModel.fill_price(&fill), None);
            assert_eq!(MidpointFillModel.fill_price(&fill), None);
        }
    }
}
