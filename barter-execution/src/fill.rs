use barter_instrument::Side;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Simulates the execution price for a backtest fill.
///
/// Called by the mock exchange engine when deciding at what price a pending
/// order should be filled against incoming market data.
///
/// # Backtest-only
///
/// This trait is only relevant for simulated execution via `MockExchange`.
/// Live execution clients receive real fill prices from the venue — they do
/// not use `FillModel`.
///
/// # Arguments
///
/// * `side` — order side (Buy or Sell).
/// * `order_price` — limit price if limit order; `None` for market orders.
/// * `best_bid` — current best bid in the order book, if available.
/// * `best_ask` — current best ask in the order book, if available.
/// * `last_price` — most recent trade price, if available.
///
/// Returns `None` if insufficient market data is available to determine a
/// fill price (e.g. no prices at all on the first tick of a backtest).
pub trait FillModel {
    fn fill_price(
        &self,
        side: Side,
        order_price: Option<Decimal>,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Option<Decimal>;
}

/// Fills at the last trade price for market orders, or the order's limit
/// price for limit orders.
///
/// Fallback chain: `order_price` → `last_price` → `best_ask` (Buy) / `best_bid` (Sell).
///
/// This is the simplest fill model and is well-suited for RL training where
/// speed matters more than realism: it eliminates spread noise and keeps
/// episode reward signals clean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize)]
pub struct LastPriceFillModel;

impl FillModel for LastPriceFillModel {
    fn fill_price(
        &self,
        side: Side,
        order_price: Option<Decimal>,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Option<Decimal> {
        order_price
            .or(last_price)
            .or(match side {
                Side::Buy => best_ask,
                Side::Sell => best_bid,
            })
    }
}

/// Fills market orders at the current best ask (buys) or best bid (sells),
/// crossing the spread as a market order taker would.
///
/// Limit orders fill at the limit price (the price is already favorable
/// relative to the market when fill is triggered).
///
/// Falls back to `last_price` if bid/ask are not available. This model is
/// more realistic than [`LastPriceFillModel`] for strategies that frequently
/// cross the spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize)]
pub struct BidAskFillModel;

impl FillModel for BidAskFillModel {
    fn fill_price(
        &self,
        side: Side,
        order_price: Option<Decimal>,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Option<Decimal> {
        if let Some(limit) = order_price {
            // Limit order: fill at the limit price (caller is responsible for
            // only calling fill_price when the limit is marketable).
            return Some(limit);
        }
        // Market order: taker crosses the spread.
        match side {
            Side::Buy => best_ask.or(last_price),
            Side::Sell => best_bid.or(last_price),
        }
    }
}

/// Fills at the midpoint of best bid and best ask.
///
/// Falls back to `order_price`, then `last_price` when the book is incomplete.
///
/// Useful when modelling execution quality between taker (crossing spread)
/// and maker (resting at the limit), or when bid/ask data is always
/// available in the backtest feed.
///
/// # Note on `order_price` (limit orders)
///
/// Unlike [`BidAskFillModel`], this model does **not** honour `order_price`
/// when both bid and ask are present — it always fills at the midpoint
/// regardless of the limit price. When the book is incomplete (only one
/// side present or neither), `order_price` is preferred over `last_price`
/// to avoid a fill at a worse price than the limit due to a stale last-trade
/// price (above the limit for buys, below the limit for sells).
///
/// The caller is responsible for invoking `fill_price` only when a limit
/// order is marketable (i.e., the limit has already been crossed). Using
/// `MidpointFillModel` for strategies that require limit-price guarantees
/// may result in fills at the midpoint rather than the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize)]
pub struct MidpointFillModel;

impl FillModel for MidpointFillModel {
    fn fill_price(
        &self,
        _side: Side,
        order_price: Option<Decimal>,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Option<Decimal> {
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => order_price.or(last_price),
        }
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
    fn fill_price(
        &self,
        side: Side,
        order_price: Option<Decimal>,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Option<Decimal> {
        match self {
            SimFillConfig::LastPrice(m) => {
                m.fill_price(side, order_price, best_bid, best_ask, last_price)
            }
            SimFillConfig::BidAsk(m) => {
                m.fill_price(side, order_price, best_bid, best_ask, last_price)
            }
            SimFillConfig::Midpoint(m) => {
                m.fill_price(side, order_price, best_bid, best_ask, last_price)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn prices() -> (Option<Decimal>, Option<Decimal>, Option<Decimal>) {
        (Some(d("99.5")), Some(d("100.5")), Some(d("100.0")))
    }

    #[test]
    fn last_price_market_buy_uses_last() {
        let (bid, ask, last) = prices();
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Buy, None, bid, ask, last),
            Some(d("100.0"))
        );
    }

    #[test]
    fn last_price_limit_uses_order_price() {
        let (bid, ask, last) = prices();
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Buy, Some(d("99.0")), bid, ask, last),
            Some(d("99.0"))
        );
    }

    #[test]
    fn bid_ask_market_buy_uses_ask() {
        let (bid, ask, last) = prices();
        assert_eq!(
            BidAskFillModel.fill_price(Side::Buy, None, bid, ask, last),
            Some(d("100.5"))
        );
    }

    #[test]
    fn bid_ask_market_sell_uses_bid() {
        let (bid, ask, last) = prices();
        assert_eq!(
            BidAskFillModel.fill_price(Side::Sell, None, bid, ask, last),
            Some(d("99.5"))
        );
    }

    #[test]
    fn midpoint_uses_mid() {
        let (bid, ask, last) = prices();
        assert_eq!(
            MidpointFillModel.fill_price(Side::Buy, None, bid, ask, last),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_falls_back_to_last_when_no_bid_ask() {
        assert_eq!(
            MidpointFillModel.fill_price(Side::Buy, None, None, None, Some(d("100.0"))),
            Some(d("100.0"))
        );
    }

    // --- SimFillConfig enum dispatch ---

    #[test]
    fn fill_model_config_last_price_dispatches() {
        let (bid, ask, last) = prices();
        let cfg = SimFillConfig::LastPrice(LastPriceFillModel);
        assert_eq!(
            cfg.fill_price(Side::Buy, None, bid, ask, last),
            LastPriceFillModel.fill_price(Side::Buy, None, bid, ask, last),
        );
    }

    #[test]
    fn fill_model_config_bid_ask_dispatches() {
        let (bid, ask, last) = prices();
        let cfg = SimFillConfig::BidAsk(BidAskFillModel);
        assert_eq!(
            cfg.fill_price(Side::Sell, None, bid, ask, last),
            BidAskFillModel.fill_price(Side::Sell, None, bid, ask, last),
        );
    }

    #[test]
    fn fill_model_config_midpoint_dispatches() {
        let (bid, ask, last) = prices();
        let cfg = SimFillConfig::Midpoint(MidpointFillModel);
        assert_eq!(
            cfg.fill_price(Side::Buy, None, bid, ask, last),
            MidpointFillModel.fill_price(Side::Buy, None, bid, ask, last),
        );
    }

    #[test]
    fn fill_model_config_default_is_last_price() {
        assert_eq!(SimFillConfig::default(), SimFillConfig::LastPrice(LastPriceFillModel));
    }

    // --- Edge cases ---

    #[test]
    fn last_price_all_none_returns_none() {
        // No market data at all — e.g. first tick of a backtest before any prices arrive.
        // The mock exchange falls back to request.state.price when fill_price returns None.
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Buy, None, None, None, None),
            None
        );
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Sell, None, None, None, None),
            None
        );
    }

    #[test]
    fn last_price_falls_back_to_bid_ask_when_no_last_price() {
        // When last_price=None but bid/ask are present, the model falls back to
        // bid/ask (as documented in the fallback chain). This exercises the tertiary
        // fallback that was previously untested.
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Buy, None, Some(d("99.5")), Some(d("100.5")), None),
            Some(d("100.5")),
            "Buy with no last_price should fall back to best_ask"
        );
        assert_eq!(
            LastPriceFillModel.fill_price(Side::Sell, None, Some(d("99.5")), Some(d("100.5")), None),
            Some(d("99.5")),
            "Sell with no last_price should fall back to best_bid"
        );
    }

    #[test]
    fn bid_ask_limit_order_wins_over_bid_ask() {
        // Limit price must take priority over bid/ask even when both are present.
        let (bid, ask, last) = prices();
        let limit = Some(d("98.0"));
        assert_eq!(
            BidAskFillModel.fill_price(Side::Buy, limit, bid, ask, last),
            limit,
            "limit price should beat best_ask for buy"
        );
        assert_eq!(
            BidAskFillModel.fill_price(Side::Sell, limit, bid, ask, last),
            limit,
            "limit price should beat best_bid for sell"
        );
    }

    #[test]
    fn midpoint_with_only_bid_falls_back_to_last() {
        // Partial book: only bid present, no ask. Should fall back to last_price.
        assert_eq!(
            MidpointFillModel.fill_price(Side::Buy, None, Some(d("99.5")), None, Some(d("100.0"))),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_with_only_ask_falls_back_to_last() {
        // Partial book: only ask present, no bid. Should fall back to last_price
        // (order_price is None, so order_price.or(last_price) = last_price).
        assert_eq!(
            MidpointFillModel.fill_price(Side::Sell, None, None, Some(d("100.5")), Some(d("100.0"))),
            Some(d("100.0"))
        );
    }

    #[test]
    fn midpoint_partial_book_prefers_order_price_over_last() {
        // With a partial book (one side missing), limit price takes priority over
        // a potentially stale last_price. Previously last_price would win, which
        // could fill a limit buy above its own limit.
        assert_eq!(
            MidpointFillModel.fill_price(
                Side::Buy, Some(d("100.0")), Some(d("99.5")), None, Some(d("110.0"))
            ),
            Some(d("100.0")),
            "partial book: limit price should beat stale last_price"
        );
    }
}
