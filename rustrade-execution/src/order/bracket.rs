//! Bracket order types for the [`BracketOrderClient`](crate::client::BracketOrderClient) trait.
//!
//! A bracket order consists of three linked orders:
//! 1. **Entry**: Limit order to enter the position
//! 2. **Take Profit**: Limit order to exit at profit target
//! 3. **Stop Loss**: Stop (or stop-limit) order to exit at loss limit
//!
//! When either exit leg fills, the exchange automatically cancels the other.

use crate::order::{Order, OrderEvent, OrderKey, TimeInForce};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use serde::{Deserialize, Serialize};

use super::{id::StrategyId, state::UnindexedOrderState};

/// Request parameters for opening a bracket order.
///
/// Contains the common fields needed by all exchanges that support bracket orders.
/// Exchange-specific behavior is documented per field.
///
/// # Price Ordering
///
/// For a **Buy** bracket: `stop_loss_price < entry_price < take_profit_price`
/// For a **Sell** bracket: `take_profit_price < entry_price < stop_loss_price`
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Constructor)]
pub struct RequestOpenBracket {
    /// Buy or Sell for the entry order. Exit legs use the opposite side.
    pub side: Side,
    /// Number of shares/contracts for all three legs.
    pub quantity: Decimal,
    /// Entry limit price.
    pub entry_price: Decimal,
    /// Take-profit limit price.
    pub take_profit_price: Decimal,
    /// Stop-loss trigger price.
    pub stop_loss_price: Decimal,
    /// Optional stop-loss limit price. When `Some`, the stop-loss becomes a stop-limit order.
    ///
    /// | Exchange | Behavior |
    /// |----------|----------|
    /// | Alpaca   | Used — creates stop-limit SL leg |
    /// | IBKR     | Ignored — SL is always a stop (market) order |
    pub stop_loss_limit_price: Option<Decimal>,
    /// Time-in-force for all three legs.
    ///
    /// **Note:** Most exchanges restrict bracket orders to `Day` or `GoodUntilCancelled`.
    pub time_in_force: TimeInForce,
}

/// Bracket order request: entry + take-profit + stop-loss.
///
/// This is an [`OrderEvent`] with [`RequestOpenBracket`] as the state, providing
/// the standard `key` (exchange, instrument, strategy, client order ID) plus
/// bracket-specific parameters.
///
/// # Example
///
/// ```ignore
/// use rustrade_execution::order::bracket::{BracketOrderRequest, RequestOpenBracket};
/// use rustrade_execution::order::{OrderKey, TimeInForce};
/// use rustrade_execution::order::id::{ClientOrderId, StrategyId};
/// use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
/// use rust_decimal_macros::dec;
///
/// let request = BracketOrderRequest {
///     key: OrderKey::new(
///         ExchangeId::AlpacaBroker,
///         InstrumentNameExchange::from("AAPL"),
///         StrategyId::new("momentum"),
///         ClientOrderId::new("bracket-001"),
///     ),
///     state: RequestOpenBracket::new(
///         Side::Buy,
///         dec!(10),
///         dec!(150.00),  // entry
///         dec!(160.00),  // take profit
///         dec!(145.00),  // stop loss
///         None,          // no stop-limit for SL
///         TimeInForce::GoodUntilCancelled { post_only: false },
///     ),
/// };
/// ```
pub type BracketOrderRequest<ExchangeKey = ExchangeId, InstrumentKey = InstrumentNameExchange> =
    OrderEvent<RequestOpenBracket, ExchangeKey, InstrumentKey>;

/// Result of bracket order placement.
///
/// Contains the parent order and optionally the child legs. The `Option` types
/// document API divergence between exchanges:
///
/// | Exchange | `take_profit` | `stop_loss` | Reason |
/// |----------|---------------|-------------|--------|
/// | IBKR     | `Some(...)` | `Some(...)` | Returns all three orders immediately |
/// | Alpaca   | `None` | `None` | Child legs created server-side; use `fetch_open_orders` |
///
/// # Invariants
///
/// - Either all orders are `Active(Open)` or all are `Inactive` (placement failed).
///   Partial success is prevented by all-or-nothing error handling in implementations.
/// - Child legs are either both `Some` (exchange returns legs immediately, e.g. IBKR)
///   or both `None` (exchange creates legs server-side, e.g. Alpaca). Asymmetric leg
///   presence is not supported — no current exchange returns one leg but not the other,
///   so [`with_all_legs`](Self::with_all_legs) and [`parent_only`](Self::parent_only)
///   are the only public constructors.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct BracketOrderResult {
    /// Parent (entry) order.
    pub parent: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    /// Take-profit order (opposite side, limit).
    ///
    /// `None` when the exchange creates legs server-side (Alpaca).
    /// `Some` when the exchange returns legs immediately (IBKR).
    pub take_profit: Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>,
    /// Stop-loss order (opposite side, stop or stop-limit).
    ///
    /// `None` when the exchange creates legs server-side (Alpaca).
    /// `Some` when the exchange returns legs immediately (IBKR).
    pub stop_loss: Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>,
}

impl BracketOrderResult {
    /// Create a result with all three legs present.
    ///
    /// Use for exchanges that return all orders immediately (e.g., IBKR).
    pub fn with_all_legs(
        parent: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        take_profit: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        stop_loss: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    ) -> Self {
        Self {
            parent,
            take_profit: Some(take_profit),
            stop_loss: Some(stop_loss),
        }
    }

    /// Create a result with only the parent order.
    ///
    /// Use for exchanges that create child legs server-side (e.g., Alpaca).
    pub fn parent_only(
        parent: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    ) -> Self {
        Self {
            parent,
            take_profit: None,
            stop_loss: None,
        }
    }

    /// Returns `true` if all child legs are present.
    pub fn has_all_legs(&self) -> bool {
        self.take_profit.is_some() && self.stop_loss.is_some()
    }

    /// Returns `true` if the parent order placement failed.
    ///
    /// Checking only the parent is sufficient because of the struct invariant:
    /// either all orders are active or all are inactive. A failed parent implies
    /// failed legs (or no legs returned, in the case of Alpaca).
    pub fn is_failed(&self) -> bool {
        self.parent.state.is_failed()
    }
}

/// Builder for creating [`BracketOrderRequest`] with a fluent API.
///
/// # Example
///
/// ```ignore
/// use rustrade_execution::order::bracket::BracketOrderRequestBuilder;
/// use rustrade_execution::order::id::{ClientOrderId, StrategyId};
/// use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
/// use rust_decimal_macros::dec;
///
/// let instrument = InstrumentNameExchange::from("AAPL");
/// let request = BracketOrderRequestBuilder::new(
///     ExchangeId::AlpacaBroker,
///     &instrument,
///     StrategyId::new("momentum"),
///     ClientOrderId::new("bracket-001"),
/// )
/// .side(Side::Buy)
/// .quantity(dec!(10))
/// .entry_price(dec!(150.00))
/// .take_profit_price(dec!(160.00))
/// .stop_loss_price(dec!(145.00))
/// .build();
/// ```
#[derive(Debug, Clone)]
#[must_use = "builder does nothing unless .build() or .try_build() is called"]
pub struct BracketOrderRequestBuilder<
    ExchangeKey = ExchangeId,
    InstrumentKey = InstrumentNameExchange,
> {
    key: OrderKey<ExchangeKey, InstrumentKey>,
    side: Option<Side>,
    quantity: Option<Decimal>,
    entry_price: Option<Decimal>,
    take_profit_price: Option<Decimal>,
    stop_loss_price: Option<Decimal>,
    stop_loss_limit_price: Option<Decimal>,
    time_in_force: TimeInForce,
}

impl<ExchangeKey, InstrumentKey> BracketOrderRequestBuilder<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Clone,
    InstrumentKey: Clone,
{
    /// Create a new builder with the given order key components.
    pub fn new(
        exchange: ExchangeKey,
        instrument: InstrumentKey,
        strategy: StrategyId,
        cid: super::id::ClientOrderId,
    ) -> Self {
        Self {
            key: OrderKey::new(exchange, instrument, strategy, cid),
            side: None,
            quantity: None,
            entry_price: None,
            take_profit_price: None,
            stop_loss_price: None,
            stop_loss_limit_price: None,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        }
    }

    /// Set the order side (Buy or Sell).
    pub fn side(mut self, side: Side) -> Self {
        self.side = Some(side);
        self
    }

    /// Set the quantity for all legs.
    pub fn quantity(mut self, quantity: Decimal) -> Self {
        self.quantity = Some(quantity);
        self
    }

    /// Set the entry limit price.
    pub fn entry_price(mut self, price: Decimal) -> Self {
        self.entry_price = Some(price);
        self
    }

    /// Set the take-profit limit price.
    pub fn take_profit_price(mut self, price: Decimal) -> Self {
        self.take_profit_price = Some(price);
        self
    }

    /// Set the stop-loss trigger price.
    pub fn stop_loss_price(mut self, price: Decimal) -> Self {
        self.stop_loss_price = Some(price);
        self
    }

    /// Set the stop-loss limit price (creates stop-limit SL on supporting exchanges).
    pub fn stop_loss_limit_price(mut self, price: Decimal) -> Self {
        self.stop_loss_limit_price = Some(price);
        self
    }

    /// Set the time-in-force for all legs.
    pub fn time_in_force(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }

    /// Build the bracket order request.
    ///
    /// # Panics
    ///
    /// Panics if any required field is missing: `side`, `quantity`, `entry_price`,
    /// `take_profit_price`, or `stop_loss_price`.
    #[track_caller]
    #[allow(clippy::expect_used)] // Panic is intentional per doc contract
    pub fn build(self) -> BracketOrderRequest<ExchangeKey, InstrumentKey> {
        BracketOrderRequest {
            key: self.key,
            state: RequestOpenBracket {
                side: self.side.expect("side is required"),
                quantity: self.quantity.expect("quantity is required"),
                entry_price: self.entry_price.expect("entry_price is required"),
                take_profit_price: self
                    .take_profit_price
                    .expect("take_profit_price is required"),
                stop_loss_price: self.stop_loss_price.expect("stop_loss_price is required"),
                stop_loss_limit_price: self.stop_loss_limit_price,
                time_in_force: self.time_in_force,
            },
        }
    }

    /// Try to build the bracket order request, returning `None` if any required field is missing.
    pub fn try_build(self) -> Option<BracketOrderRequest<ExchangeKey, InstrumentKey>> {
        Some(BracketOrderRequest {
            key: self.key,
            state: RequestOpenBracket {
                side: self.side?,
                quantity: self.quantity?,
                entry_price: self.entry_price?,
                take_profit_price: self.take_profit_price?,
                stop_loss_price: self.stop_loss_price?,
                stop_loss_limit_price: self.stop_loss_limit_price,
                time_in_force: self.time_in_force,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::id::ClientOrderId;
    use rust_decimal_macros::dec;

    #[test]
    fn test_request_open_bracket_new() {
        let req = RequestOpenBracket::new(
            Side::Buy,
            dec!(100),
            dec!(150.00),
            dec!(160.00),
            dec!(145.00),
            None,
            TimeInForce::GoodUntilCancelled { post_only: false },
        );

        assert_eq!(req.side, Side::Buy);
        assert_eq!(req.quantity, dec!(100));
        assert_eq!(req.entry_price, dec!(150.00));
        assert_eq!(req.take_profit_price, dec!(160.00));
        assert_eq!(req.stop_loss_price, dec!(145.00));
        assert!(req.stop_loss_limit_price.is_none());
    }

    #[test]
    fn test_request_open_bracket_with_stop_limit() {
        let req = RequestOpenBracket::new(
            Side::Buy,
            dec!(100),
            dec!(150.00),
            dec!(160.00),
            dec!(145.00),
            Some(dec!(144.00)),
            TimeInForce::GoodUntilEndOfDay,
        );

        assert_eq!(req.stop_loss_limit_price, Some(dec!(144.00)));
    }

    #[test]
    fn test_bracket_order_request_builder() {
        let instrument = InstrumentNameExchange::from("AAPL");
        let request = BracketOrderRequestBuilder::new(
            ExchangeId::AlpacaBroker,
            instrument.clone(),
            StrategyId::new("test"),
            ClientOrderId::new("bracket-001"),
        )
        .side(Side::Buy)
        .quantity(dec!(10))
        .entry_price(dec!(150.00))
        .take_profit_price(dec!(160.00))
        .stop_loss_price(dec!(145.00))
        .build();

        assert_eq!(request.key.exchange, ExchangeId::AlpacaBroker);
        assert_eq!(request.key.instrument, instrument);
        assert_eq!(request.state.side, Side::Buy);
        assert_eq!(request.state.quantity, dec!(10));
    }

    #[test]
    fn test_bracket_order_request_builder_try_build_missing_field() {
        let instrument = InstrumentNameExchange::from("AAPL");
        let result = BracketOrderRequestBuilder::new(
            ExchangeId::AlpacaBroker,
            instrument,
            StrategyId::new("test"),
            ClientOrderId::new("bracket-001"),
        )
        .side(Side::Buy)
        .quantity(dec!(10))
        // missing entry_price
        .take_profit_price(dec!(160.00))
        .stop_loss_price(dec!(145.00))
        .try_build();

        assert!(result.is_none());
    }
}
