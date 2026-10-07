use crate::order::{
    Order as RustradeOrder, OrderKind, TimeInForce, TrailingOffsetType,
    id::{ClientOrderId, StrategyId},
    state::UnindexedOrderState,
};
use fnv::FnvHashMap;
use ibapi::orders::{
    Action, OcaType, Order, TimeInForce as IbTimeInForce, builder::BracketPrices, order_builder,
};
use parking_lot::{Mutex, RwLock};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use std::{sync::Arc, time::Instant};
use thiserror::Error;

// ============================================================================
// Bracket Order Types
// ============================================================================

/// Request to place a bracket order (entry + take-profit + stop-loss).
///
/// A bracket order consists of three linked orders:
/// 1. **Entry**: Limit order to enter the position
/// 2. **Take Profit**: Limit order to exit at profit target
/// 3. **Stop Loss**: Stop order to exit at loss limit
///
/// The entry order is submitted with `transmit=false`, holding all orders until
/// the stop-loss (with `transmit=true`) triggers atomic submission of all three.
///
/// Take-profit and stop-loss are linked via OCA (One-Cancels-All) group, so when
/// one fills, IB automatically cancels the other.
#[derive(Debug, Clone)]
pub struct BracketOrderRequest {
    /// Instrument to trade.
    pub instrument: InstrumentNameExchange,
    /// Strategy identifier for order correlation.
    pub strategy: StrategyId,
    /// Client order ID for the parent (entry) order.
    /// Child orders will have IDs derived from this (parent_cid + "_tp", parent_cid + "_sl").
    pub parent_cid: ClientOrderId,
    /// Buy or Sell for the entry order (exits use opposite side).
    pub side: Side,
    /// Number of shares/contracts.
    pub quantity: Decimal,
    /// Entry limit price.
    pub entry_price: Decimal,
    /// Take profit limit price.
    pub take_profit_price: Decimal,
    /// Stop loss trigger price.
    pub stop_loss_price: Decimal,
    /// Time-in-force for all three orders.
    pub time_in_force: TimeInForce,
}

/// Result of placing a bracket order.
///
/// Contains the three orders with their states. Use the `ClientOrderId` from each
/// order's key to cancel individual legs or correlate stream events.
///
/// # Invariant
///
/// Either all three orders are `Active(Open)` or all three are `Inactive`, except
/// that a leg of a failed placement whose fate at TWS is unknown is `Active(Open)`.
/// See [`IbkrClient::open_bracket_order`](super::IbkrClient::open_bracket_order).
#[derive(Debug, Clone)]
pub struct BracketOrderResult {
    /// Parent (entry) order.
    pub parent: RustradeOrder<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    /// Take profit order (opposite side, limit).
    pub take_profit: RustradeOrder<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    /// Stop loss order (opposite side, stop).
    pub stop_loss: RustradeOrder<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
}

// ============================================================================
// Order Context
// ============================================================================

/// Order context stored when placing orders.
///
/// IB's `OrderStatus` callback doesn't include instrument/side/price/kind,
/// so we store this context at order placement time to reconstruct full
/// `Order` structs from status updates.
///
/// # Invariant
///
/// This data is immutable after order placement. IB doesn't support in-flight
/// order modification — amendments are cancel+replace (new order ID, new entry).
#[derive(Debug, Clone)]
pub struct OrderContext {
    pub instrument: InstrumentNameExchange,
    pub side: Side,
    /// Limit price. `None` for Market/Stop/TrailingStop orders.
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
}

/// Bidirectional mapping between rustrade ClientOrderId and IB order IDs.
///
/// IB uses `i32` order IDs from a sequence. Barter uses `ClientOrderId` (SmolStr).
/// This map maintains the bidirectional relationship and stores order context
/// for reconstructing full `Order` structs from `OrderStatus` callbacks.
///
/// # A client order id names one live order at a time
///
/// TWS never sees a [`ClientOrderId`], so this map is the only place its uniqueness can be kept.
/// An id is held from [`register`](Self::register) until its order ends, and registering an id
/// already held is refused with [`ClientOrderIdInUse`]. Once its order has ended the id may name a
/// new order.
///
/// An order ends here when the account stream reports it `Filled`, `Cancelled` or `Inactive`, or
/// when [`clear_stale`](Self::clear_stale) reaps it. A `Filled` order releases its id
/// ([`release_client_id`](Self::release_client_id)) but keeps its IB-id entry, so the executions
/// and commissions IB may still send for it resolve to the id it was placed under. That entry is
/// reaped by `clear_stale`.
///
/// Every removal is conditional on the id still naming the order removed, so an earlier order
/// under a reused id cannot take the mapping of the order now under it. Its late executions and
/// commissions still resolve to the id, carrying its own IB order ID, while a status IB re-sends
/// for it is dropped by the account stream ([`names_other_order`](Self::names_other_order)).
#[derive(Debug, Clone)]
pub struct OrderIdMap {
    inner: Arc<RwLock<OrderIdMapInner>>,
}

/// [`OrderIdMap::register`] refused an id because a live order already holds it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("client order id {0} already names a live IBKR order")]
pub struct ClientOrderIdInUse(pub ClientOrderId);

#[derive(Debug, Default)]
struct OrderIdMapInner {
    /// The live orders: each id held by an order that has not ended.
    cid_to_ib: FnvHashMap<ClientOrderId, i32>,
    /// Merged map: IB order ID → (ClientOrderId, OrderContext, registration time) for single-lookup on hot path.
    /// The Instant tracks when the order was registered, enabling age-based cleanup.
    ///
    /// Holds the live orders and the filled ones not yet reaped, which no longer hold their id.
    ib_to_entry: FnvHashMap<i32, (ClientOrderId, OrderContext, Instant)>,
}

impl OrderIdMapInner {
    /// Removes `ib_id`'s entry, and its id's mapping if that still names `ib_id`.
    fn remove(&mut self, ib_id: i32) -> Option<(ClientOrderId, OrderContext)> {
        let (client_id, ctx, _) = self.ib_to_entry.remove(&ib_id)?;
        self.release(&client_id, ib_id);
        Some((client_id, ctx))
    }

    /// Frees `client_id` if it still names `ib_id`, rather than a later order under the same id.
    fn release(&mut self, client_id: &ClientOrderId, ib_id: i32) {
        if self.cid_to_ib.get(client_id) == Some(&ib_id) {
            self.cid_to_ib.remove(client_id);
        }
    }
}

impl OrderIdMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(OrderIdMapInner::default())),
        }
    }

    /// Register a mapping between ClientOrderId and IB order ID with order context.
    ///
    /// # Errors
    /// [`ClientOrderIdInUse`] if a live order already holds `client_id`. Nothing is registered.
    pub fn register(
        &self,
        client_id: ClientOrderId,
        ib_id: i32,
        context: OrderContext,
    ) -> Result<(), ClientOrderIdInUse> {
        self.register_all([(client_id, ib_id, context)])
    }

    /// Register several orders at once, such as the legs of a bracket: all of them or none.
    ///
    /// # Errors
    /// [`ClientOrderIdInUse`], naming the first id at fault, if a live order already holds one of
    /// the ids or two of `orders` share one. Nothing is registered.
    pub fn register_all<const N: usize>(
        &self,
        orders: [(ClientOrderId, i32, OrderContext); N],
    ) -> Result<(), ClientOrderIdInUse> {
        let mut inner = self.inner.write();
        for (index, (client_id, _, _)) in orders.iter().enumerate() {
            let repeated = orders[..index]
                .iter()
                .any(|(earlier, _, _)| earlier == client_id);
            if repeated || inner.cid_to_ib.contains_key(client_id) {
                return Err(ClientOrderIdInUse(client_id.clone()));
            }
        }
        let now = Instant::now();
        for (client_id, ib_id, context) in orders {
            inner.cid_to_ib.insert(client_id.clone(), ib_id);
            inner.ib_to_entry.insert(ib_id, (client_id, context, now));
        }
        Ok(())
    }

    /// Look up the IB order ID of the live order under `client_id`.
    pub fn get_ib_id(&self, client_id: &ClientOrderId) -> Option<i32> {
        self.inner.read().cid_to_ib.get(client_id).copied()
    }

    /// Look up ClientOrderId by IB order ID.
    pub fn get_client_id(&self, ib_id: i32) -> Option<ClientOrderId> {
        self.inner
            .read()
            .ib_to_entry
            .get(&ib_id)
            .map(|(cid, _, _)| cid.clone())
    }

    /// Look up ClientOrderId and OrderContext together by IB order ID (single lookup).
    pub fn get_client_id_and_context(&self, ib_id: i32) -> Option<(ClientOrderId, OrderContext)> {
        self.inner
            .read()
            .ib_to_entry
            .get(&ib_id)
            .map(|(cid, ctx, _)| (cid.clone(), ctx.clone()))
    }

    /// Whether `client_id` now names a live order other than `ib_id`: the id was freed when `ib_id`
    /// ended and has since been registered again.
    pub fn names_other_order(&self, client_id: &ClientOrderId, ib_id: i32) -> bool {
        self.inner
            .read()
            .cid_to_ib
            .get(client_id)
            .is_some_and(|live| *live != ib_id)
    }

    /// Free the id of the filled order `ib_id`, keeping its entry, and return both, in a single
    /// write lock acquisition.
    ///
    /// Use this for a `Filled` status: the id may then name a new order, while the executions and
    /// commissions IB may still send for this one resolve to it by IB id.
    pub fn release_client_id(&self, ib_id: i32) -> Option<(ClientOrderId, OrderContext)> {
        let mut inner = self.inner.write();
        let (client_id, ctx, _) = inner.ib_to_entry.get(&ib_id)?;
        let (client_id, ctx) = (client_id.clone(), ctx.clone());
        inner.release(&client_id, ib_id);
        Some((client_id, ctx))
    }

    /// Remove mapping and return context in a single write lock acquisition.
    ///
    /// Use this for terminal status events (Cancelled/Inactive) to avoid the
    /// read-then-write pattern of `get_client_id_and_context` + `remove_by_ib_id`.
    /// The id's mapping is removed only if it still names `ib_id`.
    pub fn remove_and_get_context(&self, ib_id: i32) -> Option<(ClientOrderId, OrderContext)> {
        self.inner.write().remove(ib_id)
    }

    /// Remove a mapping by IB order ID (used when an order failed or was rolled back).
    ///
    /// The id's mapping is removed only if it still names `ib_id`.
    pub fn remove_by_ib_id(&self, ib_id: i32) -> Option<ClientOrderId> {
        self.inner
            .write()
            .remove(ib_id)
            .map(|(client_id, _)| client_id)
    }

    /// Clear order ID mappings older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// # Why This Is Needed
    ///
    /// IB does not guarantee event ordering between `OrderStatus("Filled")` and
    /// `ExecutionData`/`CommissionReport`. For fast-filling orders (especially
    /// market orders), execution data may arrive AFTER the filled status — or
    /// the filled status may not arrive at all. So a `Filled` status frees the
    /// order's client id but keeps its IB-id entry, and an order whose terminal
    /// status never arrives keeps both.
    ///
    /// Call this method periodically to clean up old mappings. A
    /// reasonable interval is 5-10 minutes with a max_age of 1 hour. An order
    /// still working past `max_age` is cleared too, freeing its id.
    pub fn clear_stale(&self, max_age: std::time::Duration) -> usize {
        let mut inner = self.inner.write();
        let before = inner.ib_to_entry.len();

        // Collect IB IDs to remove (can't mutate while iterating)
        let stale_ids: Vec<i32> = inner
            .ib_to_entry
            .iter()
            .filter(|(_, (_, _, registered_at))| registered_at.elapsed() >= max_age)
            .map(|(ib_id, _)| *ib_id)
            .collect();

        for ib_id in stale_ids {
            inner.remove(ib_id);
        }

        before - inner.ib_to_entry.len()
    }

    /// Number of live orders: those holding a client id.
    pub fn len(&self) -> usize {
        self.inner.read().cid_to_ib.len()
    }

    /// Check if no order is live.
    pub fn is_empty(&self) -> bool {
        self.inner.read().cid_to_ib.is_empty()
    }
}

impl Default for OrderIdMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks IB order IDs with pending cancel requests.
///
/// Used to differentiate user-initiated cancellation from time-based expiration.
/// IBKR sends `"Cancelled"` status for both cases; this map tracks which cancels
/// were user-initiated via `cancel_order()`.
#[derive(Debug, Clone)]
pub struct PendingCancels {
    inner: Arc<Mutex<FnvHashMap<i32, Instant>>>,
}

impl PendingCancels {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FnvHashMap::with_capacity_and_hasher(
                8,
                Default::default(),
            ))),
        }
    }

    /// Record a pending cancel request for the given IB order ID.
    pub fn insert(&self, ib_id: i32) {
        self.inner.lock().insert(ib_id, Instant::now());
    }

    /// Check if a cancel was user-initiated and remove from tracking.
    ///
    /// Returns `true` if the order ID was in the pending set (user-initiated cancel).
    #[must_use]
    pub fn remove(&self, ib_id: i32) -> bool {
        self.inner.lock().remove(&ib_id).is_some()
    }

    /// Clear entries older than the given duration.
    ///
    /// Returns the number of cleared entries. Call periodically to prevent
    /// memory leaks from orphaned cancel requests (e.g., network issues).
    #[must_use]
    pub fn clear_stale(&self, max_age: std::time::Duration) -> usize {
        let mut map = self.inner.lock();
        let before = map.len();
        map.retain(|_, registered_at| registered_at.elapsed() < max_age);
        before - map.len()
    }

    /// Number of pending cancel requests being tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Check if there are no pending cancel requests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

impl Default for PendingCancels {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert rustrade Side to IB Action.
pub(crate) fn side_to_action(side: rustrade_instrument::Side) -> Action {
    match side {
        rustrade_instrument::Side::Buy => Action::Buy,
        rustrade_instrument::Side::Sell => Action::Sell,
    }
}

/// Error when mapping rustrade order types to IB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderMappingError {
    PostOnlyNotSupported,
    /// Price conversion to f64 failed (overflow or invalid decimal).
    InvalidPrice(String),
    /// Limit price required for this order type but was None.
    MissingLimitPrice(OrderKind),
    /// Trailing offset type not supported by IBKR.
    UnsupportedOffsetType(TrailingOffsetType),
    /// Order kind not supported by IBKR (e.g., TakeProfit).
    UnsupportedOrderKind(OrderKind),
    /// AtClose TIF only valid with Market or Limit orders (becomes MOC/LOC).
    /// Stop orders cannot be combined with at-close execution.
    UnsupportedOrderKindForAtClose(OrderKind),
    /// AtClose TIF reached `time_in_force_to_ib` directly. AtClose changes the
    /// order TYPE (MOC/LOC), not just the TIF; callers must route through
    /// `build_ib_order` which handles the type promotion.
    AtCloseRequiresOrderTypeChange,
    /// `ibapi` rejected a bracket's prices: one is not finite, or the take profit or stop loss is
    /// on the wrong side of the entry (for a buy, the take profit must be above the entry and the
    /// stop loss below; a sell the reverse).
    InvalidBracketPrices(String),
}

impl std::fmt::Display for OrderMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PostOnlyNotSupported => write!(f, "post_only not supported by IB"),
            Self::InvalidPrice(p) => write!(f, "invalid price for f64 conversion: {p}"),
            Self::MissingLimitPrice(k) => {
                write!(f, "limit price required for {k} but was None")
            }
            Self::UnsupportedOffsetType(t) => {
                write!(f, "trailing offset type {t:?} not supported by IBKR")
            }
            Self::UnsupportedOrderKind(k) => {
                write!(f, "order kind {k} not supported by IBKR")
            }
            Self::UnsupportedOrderKindForAtClose(k) => {
                write!(
                    f,
                    "AtClose TIF only valid with Market or Limit orders, got {k}"
                )
            }
            Self::AtCloseRequiresOrderTypeChange => write!(
                f,
                "AtClose TIF must be routed through build_ib_order, which promotes \
                 the order to MOC/LOC; time_in_force_to_ib cannot map it directly"
            ),
            Self::InvalidBracketPrices(e) => write!(f, "invalid bracket prices: {e}"),
        }
    }
}

impl std::error::Error for OrderMappingError {}

/// Convert rustrade TimeInForce to IB TimeInForce.
///
/// Returns [`OrderMappingError::AtCloseRequiresOrderTypeChange`] for
/// [`TimeInForce::AtClose`]: AtClose changes the order TYPE (MOC/LOC), not
/// just the TIF, and is handled by [`build_ib_order`] which promotes the
/// order type. Callers needing AtClose support should route through
/// [`build_ib_order`] rather than this helper.
pub fn time_in_force_to_ib(tif: &TimeInForce) -> Result<IbTimeInForce, OrderMappingError> {
    match tif {
        TimeInForce::GoodUntilCancelled { post_only } => {
            if *post_only {
                Err(OrderMappingError::PostOnlyNotSupported)
            } else {
                Ok(IbTimeInForce::GoodTillCanceled)
            }
        }
        TimeInForce::GoodUntilEndOfDay => Ok(IbTimeInForce::Day),
        TimeInForce::FillOrKill => Ok(IbTimeInForce::FillOrKill),
        TimeInForce::ImmediateOrCancel => Ok(IbTimeInForce::ImmediateOrCancel),
        TimeInForce::GoodTillDate { .. } => Ok(IbTimeInForce::GoodTillDate),
        TimeInForce::AtOpen => Ok(IbTimeInForce::OnOpen),
        // AtClose changes the order TYPE to MOC/LOC, not just the TIF.
        // `build_ib_order` intercepts AtClose; surfacing Err here lets other
        // callers (e.g. the bracket-order path) reject gracefully.
        TimeInForce::AtClose => Err(OrderMappingError::AtCloseRequiresOrderTypeChange),
    }
}

/// Convert a Decimal to f64, returning an error if conversion fails.
fn decimal_to_f64(value: rust_decimal::Decimal) -> Result<f64, OrderMappingError> {
    value.try_into().or_else(|_| {
        value
            .to_string()
            .parse()
            .map_err(|_| OrderMappingError::InvalidPrice(value.to_string()))
    })
}

/// Extract required limit price from Option, returning error if None.
fn require_limit_price(
    price: Option<rust_decimal::Decimal>,
    kind: OrderKind,
) -> Result<f64, OrderMappingError> {
    match price {
        Some(p) => decimal_to_f64(p),
        None => Err(OrderMappingError::MissingLimitPrice(kind)),
    }
}

/// Build an IB Order from rustrade order parameters.
///
/// # Order Type Mapping
///
/// - `Market` → IB "MKT"
/// - `Limit` → IB "LMT" with `limit_price`
/// - `Stop` → IB "STP" with `aux_price` as trigger
/// - `StopLimit` → IB "STP LMT" with `aux_price` as trigger, `limit_price` from Order.price
/// - `TrailingStop` (Percentage) → IB "TRAIL" with `trailing_percent`
/// - `TrailingStop` (Absolute) → IB "TRAIL" with `aux_price`
/// - `TrailingStopLimit` (Absolute) → IB "TRAIL LIMIT" with `aux_price`, `limit_price_offset`
/// - `TrailingStopLimit` (Percentage) → IB "TRAIL LIMIT" with `trailing_percent`, `limit_price_offset`
/// - `TakeProfit` → `Err(UnsupportedOrderKind)` (IBKR has no native TP; use bracket orders)
/// - `TakeProfitLimit` → `Err(UnsupportedOrderKind)` (IBKR has no native TP; use bracket orders)
/// - `BasisPoints` offset type → Error (not supported by IBKR)
///
/// # Price Requirements
///
/// - `Market`, `Stop`, `TrailingStop`: `price` should be `None` (ignored if provided)
/// - `Limit`, `StopLimit`, `TrailingStopLimit`: `price` must be `Some(limit_price)`
///
/// # Time-in-Force Special Cases
///
/// - `AtClose` + `Market` → IB "MOC" (Market-on-Close)
/// - `AtClose` + `Limit` → IB "LOC" (Limit-on-Close)
/// - `AtClose` + Stop/Trailing → Error (not supported by IBKR)
/// - `GoodTillDate` → Sets both TIF and `good_till_date` field
pub fn build_ib_order(
    side: rustrade_instrument::Side,
    quantity: f64,
    kind: &OrderKind,
    price: Option<rust_decimal::Decimal>,
    tif: &TimeInForce,
) -> Result<Order, OrderMappingError> {
    let action = side_to_action(side);

    // Handle AtClose specially - it changes the order TYPE, not just TIF
    if matches!(tif, TimeInForce::AtClose) {
        return build_at_close_order(action, quantity, kind, price);
    }

    let tif_ib = time_in_force_to_ib(tif)?;

    let mut order = match kind {
        OrderKind::Market => order_builder::market_order(action, quantity),

        OrderKind::Limit => {
            let price_f64 = require_limit_price(price, *kind)?;
            order_builder::limit_order(action, quantity, price_f64)
        }

        OrderKind::Stop { trigger_price } => {
            let trigger_f64 = decimal_to_f64(*trigger_price)?;
            order_builder::stop(action, quantity, trigger_f64)
        }

        OrderKind::StopLimit { trigger_price } => {
            let limit_f64 = require_limit_price(price, *kind)?;
            let trigger_f64 = decimal_to_f64(*trigger_price)?;
            // Built by hand: `ibapi` 5 removed `order_builder::stop_limit`, and its fluent
            // replacement adds nothing our checks above have not done. Same fields as before.
            Order {
                action,
                order_type: "STP LMT".to_owned(),
                total_quantity: quantity,
                limit_price: Some(limit_f64),
                aux_price: Some(trigger_f64),
                ..Order::default()
            }
        }

        OrderKind::TrailingStop {
            offset,
            offset_type,
        } => {
            let offset_f64 = decimal_to_f64(*offset)?;
            match offset_type {
                TrailingOffsetType::Percentage => {
                    // Use the builder for percentage-based trailing stop.
                    // trail_stop_price is None, letting IB derive it from market price.
                    Order {
                        action,
                        order_type: "TRAIL".to_owned(),
                        total_quantity: quantity,
                        trailing_percent: Some(offset_f64),
                        trail_stop_price: None,
                        ..Order::default()
                    }
                }
                TrailingOffsetType::Absolute => {
                    // Manual construction for absolute trailing stop.
                    // aux_price holds the trailing amount.
                    Order {
                        action,
                        order_type: "TRAIL".to_owned(),
                        total_quantity: quantity,
                        aux_price: Some(offset_f64),
                        trail_stop_price: None,
                        ..Order::default()
                    }
                }
                TrailingOffsetType::BasisPoints => {
                    return Err(OrderMappingError::UnsupportedOffsetType(
                        TrailingOffsetType::BasisPoints,
                    ));
                }
            }
        }

        OrderKind::TrailingStopLimit {
            offset,
            offset_type,
            limit_offset,
        } => {
            let offset_f64 = decimal_to_f64(*offset)?;
            let limit_offset_f64 = decimal_to_f64(*limit_offset)?;
            match offset_type {
                TrailingOffsetType::Absolute => {
                    // aux_price = trailing_amount, limit_price_offset = limit offset from stop.
                    // The fields `ibapi` 4's removed `order_builder::trailing_stop_limit` set,
                    // including its `trail_stop_price` of 0.0, so the order IB receives is
                    // unchanged.
                    Order {
                        action,
                        order_type: "TRAIL LIMIT".to_owned(),
                        total_quantity: quantity,
                        trail_stop_price: Some(0.0),
                        limit_price_offset: Some(limit_offset_f64),
                        aux_price: Some(offset_f64),
                        ..Order::default()
                    }
                }
                TrailingOffsetType::Percentage => {
                    // Manual construction for percentage-based trailing stop-limit.
                    Order {
                        action,
                        order_type: "TRAIL LIMIT".to_owned(),
                        total_quantity: quantity,
                        trailing_percent: Some(offset_f64),
                        limit_price_offset: Some(limit_offset_f64),
                        trail_stop_price: None,
                        ..Order::default()
                    }
                }
                TrailingOffsetType::BasisPoints => {
                    return Err(OrderMappingError::UnsupportedOffsetType(
                        TrailingOffsetType::BasisPoints,
                    ));
                }
            }
        }

        // IBKR does not have native take-profit order types. Callers should use
        // bracket orders or manage TP logic at the strategy level.
        OrderKind::TakeProfit { .. } | OrderKind::TakeProfitLimit { .. } => {
            return Err(OrderMappingError::UnsupportedOrderKind(*kind));
        }
    };

    order.tif = tif_ib;

    // Handle GoodTillDate: set the good_till_date string field
    if let TimeInForce::GoodTillDate { expiry } = tif {
        order.good_till_date = format_gtd_datetime(expiry);
    }

    Ok(order)
}

/// Format a `DateTime<Utc>` for IB's good_till_date field.
///
/// IB accepts format: "yyyyMMdd HH:mm:ss" with optional timezone suffix.
/// We use UTC format "yyyyMMdd-HH:mm:ss" which IB interprets as UTC.
fn format_gtd_datetime(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y%m%d-%H:%M:%S").to_string()
}

/// Build an at-close order (MOC or LOC).
///
/// Only Market and Limit orders can be combined with at-close execution.
/// Stop orders cannot execute at close due to IBKR's order model constraints.
fn build_at_close_order(
    action: Action,
    quantity: f64,
    kind: &OrderKind,
    price: Option<rust_decimal::Decimal>,
) -> Result<Order, OrderMappingError> {
    match kind {
        OrderKind::Market => Ok(order_builder::market_on_close(action, quantity)),
        OrderKind::Limit => {
            let price_f64 = require_limit_price(price, *kind)?;
            Ok(order_builder::limit_on_close(action, quantity, price_f64))
        }
        // Stop orders cannot be combined with at-close execution
        _ => Err(OrderMappingError::UnsupportedOrderKindForAtClose(*kind)),
    }
}

/// Build a bracket order with proper OCA linking.
///
/// ibapi's `order_builder::bracket_order()` sets `parent_id` on child orders (TP/SL
/// wait for entry fill) but does NOT set `oca_group` or `oca_type`. Without OCA
/// linking, if take-profit fills, stop-loss stays open — dangerous position exposure.
///
/// This function wraps `bracket_order()` and adds OCA group linking so that when
/// either child order fills, the other is automatically cancelled by IB.
///
/// # Arguments
///
/// * `parent_order_id` - The IB order ID for the parent (entry) order
/// * `action` - Buy or Sell for the entry order (children use opposite action)
/// * `quantity` - Number of shares/contracts
/// * `limit_price` - Entry limit price
/// * `take_profit_price` - Take profit limit price (above entry for Buy, below for Sell)
/// * `stop_loss_price` - Stop loss trigger price (below entry for Buy, above for Sell)
///
/// # Returns
///
/// [`OrderMappingError::InvalidBracketPrices`] if `ibapi` rejects the prices: one is not finite,
/// or the take profit or stop loss is on the wrong side of the entry. Otherwise a
/// Vec of 3 orders: `[parent, take_profit, stop_loss]` with:
/// - `parent_id` set on children (IB's bracket linkage)
/// - `oca_group` set on children (OCA linkage between TP and SL)
/// - `oca_type = CancelWithBlock` (safest: one at a time, cancel remaining)
/// - `transmit` flags: parent=false, TP=false, SL=true (atomic submission)
///
/// # Order ID Convention
///
/// ibapi's `bracket_order()` assigns consecutive IDs: parent=N, TP=N+1, SL=N+2.
/// Caller must ensure these IDs are reserved atomically via `allocate_order_id_range(3)`.
pub fn build_ib_bracket_with_oca(
    parent_order_id: i32,
    action: Action,
    quantity: f64,
    limit_price: f64,
    take_profit_price: f64,
    stop_loss_price: f64,
    tif: IbTimeInForce,
) -> Result<Vec<Order>, OrderMappingError> {
    let prices = BracketPrices {
        entry: limit_price,
        take_profit: take_profit_price,
        stop_loss: stop_loss_price,
    };
    let mut orders = order_builder::bracket_order(parent_order_id, action, quantity, prices)
        .map_err(|e| OrderMappingError::InvalidBracketPrices(e.to_string()))?;
    assert_eq!(
        orders.len(),
        3,
        "ibapi bracket_order must return exactly 3 orders (parent, TP, SL)"
    );

    // ibapi's bracket_order() defaults all legs to TimeInForce::Day; override
    // with the caller-specified TIF so it actually reaches the wire.
    orders[0].tif = tif.clone();
    orders[1].tif = tif.clone();
    orders[2].tif = tif;

    // Link TP and SL via OCA group so one cancels the other.
    // CancelWithBlock provides overfill protection by routing only one child at a time.
    let oca_group = format!("bracket_{}", parent_order_id);
    orders[1].oca_group = oca_group.clone();
    orders[1].oca_type = OcaType::CancelWithBlock;
    orders[2].oca_group = oca_group;
    orders[2].oca_type = OcaType::CancelWithBlock;

    Ok(orders)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn test_context() -> OrderContext {
        OrderContext {
            instrument: rustrade_instrument::instrument::name::InstrumentNameExchange::from("AAPL"),
            side: Side::Buy,
            price: Some(Decimal::from(150)),
            quantity: Decimal::from(100),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        }
    }

    #[test]
    fn test_order_id_map_basic() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("order-123");
        let ctx = test_context();

        map.register(cid.clone(), 42, ctx.clone()).unwrap();

        assert_eq!(map.get_ib_id(&cid), Some(42));
        assert_eq!(map.get_client_id(42), Some(cid.clone()));
        assert_eq!(map.len(), 1);

        let (retrieved_cid, retrieved_ctx) = map.get_client_id_and_context(42).unwrap();
        assert_eq!(retrieved_cid, cid);
        assert_eq!(retrieved_ctx.side, Side::Buy);
        assert_eq!(retrieved_ctx.price, Some(Decimal::from(150)));
    }

    #[test]
    fn test_order_id_map_remove() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("order-456");

        map.register(cid.clone(), 100, test_context()).unwrap();
        assert_eq!(map.len(), 1);

        let removed = map.remove_by_ib_id(100);
        assert_eq!(removed, Some(cid.clone()));
        assert!(map.is_empty());
        assert!(map.get_ib_id(&cid).is_none());
        assert!(map.get_client_id(100).is_none());
        assert!(map.get_client_id_and_context(100).is_none());
    }

    #[test]
    fn test_side_conversion() {
        assert!(matches!(
            side_to_action(rustrade_instrument::Side::Buy),
            Action::Buy
        ));
        assert!(matches!(
            side_to_action(rustrade_instrument::Side::Sell),
            Action::Sell
        ));
    }

    #[test]
    fn test_time_in_force_conversion() {
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::GoodUntilCancelled { post_only: false }),
            Ok(IbTimeInForce::GoodTillCanceled)
        );

        assert!(matches!(
            time_in_force_to_ib(&TimeInForce::GoodUntilCancelled { post_only: true }),
            Err(OrderMappingError::PostOnlyNotSupported)
        ));

        assert_eq!(
            time_in_force_to_ib(&TimeInForce::GoodUntilEndOfDay),
            Ok(IbTimeInForce::Day)
        );
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::FillOrKill),
            Ok(IbTimeInForce::FillOrKill)
        );
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::ImmediateOrCancel),
            Ok(IbTimeInForce::ImmediateOrCancel)
        );
    }

    #[test]
    fn test_time_in_force_conversion_good_till_date() {
        use chrono::{TimeZone, Utc};

        let expiry = Utc.with_ymd_and_hms(2025, 6, 30, 23, 59, 59).unwrap();
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::GoodTillDate { expiry }),
            Ok(IbTimeInForce::GoodTillDate)
        );
    }

    #[test]
    fn test_time_in_force_conversion_at_open() {
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::AtOpen),
            Ok(IbTimeInForce::OnOpen)
        );
    }

    #[test]
    fn test_time_in_force_conversion_at_close_returns_err() {
        // AtClose changes the order TYPE (MOC/LOC), so callers must route
        // through build_ib_order. Other callers (e.g. bracket orders) get a
        // graceful Err that they can surface as an order rejection.
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::AtClose),
            Err(OrderMappingError::AtCloseRequiresOrderTypeChange)
        );
    }

    #[test]
    fn test_build_market_order() {
        let order = build_ib_order(
            rustrade_instrument::Side::Buy,
            100.0,
            &OrderKind::Market,
            None, // Market orders have no limit price
            &TimeInForce::GoodUntilEndOfDay,
        )
        .unwrap();

        assert_eq!(order.action, Action::Buy);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "MKT");
    }

    #[test]
    fn test_build_limit_order() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            50.0,
            &OrderKind::Limit,
            Some(Decimal::try_from(150.5).unwrap()),
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 50.0);
        assert_eq!(order.order_type, "LMT");
    }

    #[test]
    fn test_order_id_map_remove_and_get_context() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("order-789");
        let ctx = test_context();

        map.register(cid.clone(), 50, ctx).unwrap();
        assert_eq!(map.len(), 1);

        // Remove and get context in single operation
        let result = map.remove_and_get_context(50);
        assert!(result.is_some());
        let (retrieved_cid, retrieved_ctx) = result.unwrap();
        assert_eq!(retrieved_cid, cid);
        assert_eq!(retrieved_ctx.side, Side::Buy);

        // Map should be empty now
        assert!(map.is_empty());
        assert!(map.get_client_id(50).is_none());
        assert!(map.get_ib_id(&cid).is_none());

        // Second removal returns None
        assert!(map.remove_and_get_context(50).is_none());
    }

    #[test]
    fn test_order_id_map_clear_stale() {
        use std::time::Duration;

        let map = OrderIdMap::new();

        // Register orders
        map.register(ClientOrderId::new("old-1"), 1, test_context())
            .unwrap();
        map.register(ClientOrderId::new("old-2"), 2, test_context())
            .unwrap();

        // With zero max_age, all entries are stale
        let cleared = map.clear_stale(Duration::ZERO);
        assert_eq!(cleared, 2);
        assert!(map.is_empty());

        // Register new orders
        map.register(ClientOrderId::new("new-1"), 10, test_context())
            .unwrap();
        map.register(ClientOrderId::new("new-2"), 20, test_context())
            .unwrap();

        // With large max_age, nothing is stale
        let cleared = map.clear_stale(Duration::from_secs(3600));
        assert_eq!(cleared, 0);
        assert_eq!(map.len(), 2);
    }

    /// A live order's id is refused to a second order, and the first keeps its mapping.
    #[test]
    fn a_live_orders_id_is_refused_to_a_second_order() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("taken");
        map.register(cid.clone(), 1, test_context()).unwrap();

        assert_eq!(
            map.register(cid.clone(), 2, test_context()),
            Err(ClientOrderIdInUse(cid.clone()))
        );
        assert_eq!(
            map.get_ib_id(&cid),
            Some(1),
            "a cancel by id still reaches the first"
        );
        assert!(
            map.get_client_id(2).is_none(),
            "nothing of the second was registered"
        );
    }

    /// A filled order frees its id at once, while its late executions still resolve to it.
    #[test]
    fn a_filled_order_frees_its_id_and_keeps_resolving_by_ib_id() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("reused");
        map.register(cid.clone(), 1, test_context()).unwrap();

        assert_eq!(
            map.release_client_id(1).map(|(id, _)| id),
            Some(cid.clone())
        );
        assert!(map.is_empty(), "no order is live");
        assert_eq!(
            map.get_client_id(1),
            Some(cid.clone()),
            "a late execution resolves"
        );

        // The id is free again.
        map.register(cid.clone(), 2, test_context()).unwrap();
        assert_eq!(map.get_ib_id(&cid), Some(2));
        assert_eq!(map.get_client_id(1), Some(cid.clone()), "and still does");
    }

    /// An earlier order under a reused id ending, or being reaped, leaves the order now under the
    /// id mapped.
    #[test]
    fn an_earlier_order_ending_leaves_a_reused_ids_mapping() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("reused");
        map.register(cid.clone(), 1, test_context()).unwrap();
        let _ = map.release_client_id(1);
        map.register(cid.clone(), 2, test_context()).unwrap();

        assert!(
            map.remove_and_get_context(1).is_some(),
            "the first order's entry goes"
        );
        assert_eq!(map.get_ib_id(&cid), Some(2), "the second keeps the id");
        assert!(map.remove_by_ib_id(1).is_none());
        assert_eq!(map.get_ib_id(&cid), Some(2));
    }

    /// Reaping a filled order's entry leaves a reused id's mapping too.
    #[test]
    fn reaping_a_filled_order_leaves_a_reused_ids_mapping() {
        use std::time::Duration;

        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("reused");
        map.register(cid.clone(), 1, test_context()).unwrap();
        let _ = map.release_client_id(1);
        std::thread::sleep(Duration::from_millis(5));
        map.register(cid.clone(), 2, test_context()).unwrap();

        assert_eq!(
            map.clear_stale(Duration::from_millis(5)),
            1,
            "only the filled one"
        );
        assert_eq!(map.get_ib_id(&cid), Some(2));
    }

    /// Several orders register all or none: an id already live, or one repeated among them,
    /// registers none of them.
    #[test]
    fn register_all_registers_every_order_or_none() {
        let map = OrderIdMap::new();
        map.register(ClientOrderId::new("live"), 1, test_context())
            .unwrap();

        let clash = map.register_all([
            (ClientOrderId::new("a"), 10, test_context()),
            (ClientOrderId::new("live"), 11, test_context()),
        ]);
        assert_eq!(clash, Err(ClientOrderIdInUse(ClientOrderId::new("live"))));

        let repeated = map.register_all([
            (ClientOrderId::new("b"), 20, test_context()),
            (ClientOrderId::new("b"), 21, test_context()),
        ]);
        assert_eq!(repeated, Err(ClientOrderIdInUse(ClientOrderId::new("b"))));

        assert_eq!(map.len(), 1, "neither registered anything");
        assert!(
            [10, 11, 20, 21]
                .iter()
                .all(|ib_id| map.get_client_id(*ib_id).is_none())
        );

        map.register_all([
            (ClientOrderId::new("a"), 10, test_context()),
            (ClientOrderId::new("b"), 20, test_context()),
        ])
        .unwrap();
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_pending_cancels_insert_remove() {
        let cancels = PendingCancels::new();
        assert!(cancels.is_empty());

        // Insert a cancel request
        cancels.insert(42);
        assert_eq!(cancels.len(), 1);
        assert!(!cancels.is_empty());

        // Remove returns true for tracked ID
        assert!(cancels.remove(42));
        assert!(cancels.is_empty());

        // Remove returns false for untracked ID
        assert!(!cancels.remove(42));
        assert!(!cancels.remove(999));
    }

    #[test]
    fn test_pending_cancels_multiple() {
        let cancels = PendingCancels::new();

        cancels.insert(1);
        cancels.insert(2);
        cancels.insert(3);
        assert_eq!(cancels.len(), 3);

        // Remove middle one
        assert!(cancels.remove(2));
        assert_eq!(cancels.len(), 2);

        // Other IDs still tracked
        assert!(cancels.remove(1));
        assert!(cancels.remove(3));
        assert!(cancels.is_empty());
    }

    #[test]
    fn test_pending_cancels_clear_stale() {
        use std::time::Duration;

        let cancels = PendingCancels::new();

        cancels.insert(1);
        cancels.insert(2);

        // With zero max_age, all entries are stale
        let cleared = cancels.clear_stale(Duration::ZERO);
        assert_eq!(cleared, 2);
        assert!(cancels.is_empty());

        // Insert new ones
        cancels.insert(10);
        cancels.insert(20);

        // With large max_age, nothing is stale
        let cleared = cancels.clear_stale(Duration::from_secs(3600));
        assert_eq!(cleared, 0);
        assert_eq!(cancels.len(), 2);
    }

    #[test]
    fn test_pending_cancels_duplicate_insert() {
        let cancels = PendingCancels::new();

        cancels.insert(42);
        cancels.insert(42);

        // HashMap deduplicates by ID; second insert updates timestamp
        assert_eq!(cancels.len(), 1);
        assert!(cancels.remove(42));
        assert!(cancels.is_empty());
    }

    #[test]
    fn test_build_stop_order() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::Stop {
                trigger_price: Decimal::from(45),
            },
            None, // Stop orders have no limit price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "STP");
        assert_eq!(order.aux_price, Some(45.0));
        assert_eq!(order.limit_price, None);
    }

    #[test]
    fn test_build_stop_limit_order() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::StopLimit {
                trigger_price: Decimal::from(44),
            },
            Some(Decimal::from(45)), // limit price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "STP LMT");
        assert_eq!(order.aux_price, Some(44.0)); // trigger/stop price
        assert_eq!(order.limit_price, Some(45.0)); // limit price
    }

    #[test]
    fn test_build_trailing_stop_percentage() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStop {
                offset: Decimal::from(5),
                offset_type: TrailingOffsetType::Percentage,
            },
            None, // Trailing stop orders have no limit price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "TRAIL");
        assert_eq!(order.trailing_percent, Some(5.0));
        assert_eq!(order.aux_price, None); // percentage uses trailing_percent, not aux_price
        assert_eq!(order.trail_stop_price, None); // let IB derive from market
    }

    #[test]
    fn test_build_trailing_stop_absolute() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStop {
                offset: Decimal::from(2),
                offset_type: TrailingOffsetType::Absolute,
            },
            None, // Trailing stop orders have no limit price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "TRAIL");
        assert_eq!(order.aux_price, Some(2.0)); // absolute uses aux_price
        assert_eq!(order.trailing_percent, None);
        assert_eq!(order.trail_stop_price, None);
    }

    #[test]
    fn test_build_trailing_stop_limit_absolute() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStopLimit {
                offset: Decimal::from(2),
                offset_type: TrailingOffsetType::Absolute,
                limit_offset: Decimal::try_from(0.5).unwrap(),
            },
            None, // Trailing stop limit uses limit_offset, not a fixed price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "TRAIL LIMIT");
        assert_eq!(order.aux_price, Some(2.0)); // trailing amount
        assert_eq!(order.limit_price_offset, Some(0.5)); // limit offset from stop
        assert_eq!(order.trailing_percent, None);
        assert_eq!(order.trail_stop_price, Some(0.0)); // as ibapi 4's builder sent it
    }

    #[test]
    fn test_build_trailing_stop_limit_percentage() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStopLimit {
                offset: Decimal::from(5),
                offset_type: TrailingOffsetType::Percentage,
                limit_offset: Decimal::try_from(0.5).unwrap(),
            },
            None, // Trailing stop limit uses limit_offset, not a fixed price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "TRAIL LIMIT");
        assert_eq!(order.trailing_percent, Some(5.0));
        assert_eq!(order.limit_price_offset, Some(0.5));
        assert_eq!(order.aux_price, None); // percentage doesn't use aux_price
    }

    #[test]
    fn test_build_trailing_stop_basis_points_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStop {
                offset: Decimal::from(50), // 50 basis points
                offset_type: TrailingOffsetType::BasisPoints,
            },
            None, // Trailing stop orders have no limit price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOffsetType(
                TrailingOffsetType::BasisPoints
            ))
        ));
    }

    #[test]
    fn test_build_trailing_stop_limit_basis_points_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStopLimit {
                offset: Decimal::from(50),
                offset_type: TrailingOffsetType::BasisPoints,
                limit_offset: Decimal::try_from(0.5).unwrap(),
            },
            None, // Trailing stop limit uses limit_offset, not a fixed price
            &TimeInForce::GoodUntilCancelled { post_only: false },
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOffsetType(
                TrailingOffsetType::BasisPoints
            ))
        ));
    }

    #[test]
    fn test_build_take_profit_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TakeProfit {
                trigger_price: Decimal::from(150),
            },
            None,
            &TimeInForce::GoodUntilCancelled { post_only: false },
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKind(
                OrderKind::TakeProfit { .. }
            ))
        ));
    }

    #[test]
    fn test_build_take_profit_limit_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TakeProfitLimit {
                trigger_price: Decimal::from(150),
            },
            None,
            &TimeInForce::GoodUntilCancelled { post_only: false },
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKind(
                OrderKind::TakeProfitLimit { .. }
            ))
        ));
    }

    // =========================================================================
    // Extended Time-in-Force Tests
    // =========================================================================

    #[test]
    fn test_build_market_order_at_open() {
        let order = build_ib_order(
            rustrade_instrument::Side::Buy,
            100.0,
            &OrderKind::Market,
            None, // Market orders have no limit price
            &TimeInForce::AtOpen,
        )
        .unwrap();

        assert_eq!(order.action, Action::Buy);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "MKT");
        assert_eq!(order.tif, IbTimeInForce::OnOpen);
    }

    #[test]
    fn test_build_limit_order_at_open() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            50.0,
            &OrderKind::Limit,
            Some(Decimal::from(150)),
            &TimeInForce::AtOpen,
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 50.0);
        assert_eq!(order.order_type, "LMT");
        assert_eq!(order.limit_price, Some(150.0));
        assert_eq!(order.tif, IbTimeInForce::OnOpen);
    }

    #[test]
    fn test_build_stop_order_at_open() {
        // Stop orders can use AtOpen TIF
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::Stop {
                trigger_price: Decimal::from(45),
            },
            None, // Stop orders have no limit price
            &TimeInForce::AtOpen,
        )
        .unwrap();

        assert_eq!(order.order_type, "STP");
        assert_eq!(order.aux_price, Some(45.0));
        assert_eq!(order.tif, IbTimeInForce::OnOpen);
    }

    #[test]
    fn test_build_market_on_close() {
        let order = build_ib_order(
            rustrade_instrument::Side::Buy,
            100.0,
            &OrderKind::Market,
            None, // Market orders have no limit price
            &TimeInForce::AtClose,
        )
        .unwrap();

        assert_eq!(order.action, Action::Buy);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "MOC");
    }

    #[test]
    fn test_build_limit_on_close() {
        let order = build_ib_order(
            rustrade_instrument::Side::Sell,
            50.0,
            &OrderKind::Limit,
            Some(Decimal::from(150)),
            &TimeInForce::AtClose,
        )
        .unwrap();

        assert_eq!(order.action, Action::Sell);
        assert_eq!(order.total_quantity, 50.0);
        assert_eq!(order.order_type, "LOC");
        assert_eq!(order.limit_price, Some(150.0));
    }

    #[test]
    fn test_build_stop_at_close_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::Stop {
                trigger_price: Decimal::from(45),
            },
            None, // Stop orders have no limit price
            &TimeInForce::AtClose,
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKindForAtClose(
                OrderKind::Stop { .. }
            ))
        ));
    }

    #[test]
    fn test_build_stop_limit_at_close_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::StopLimit {
                trigger_price: Decimal::from(44),
            },
            Some(Decimal::from(45)),
            &TimeInForce::AtClose,
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKindForAtClose(
                OrderKind::StopLimit { .. }
            ))
        ));
    }

    #[test]
    fn test_build_trailing_stop_at_close_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStop {
                offset: Decimal::from(5),
                offset_type: TrailingOffsetType::Percentage,
            },
            None, // Trailing stop orders have no limit price
            &TimeInForce::AtClose,
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKindForAtClose(
                OrderKind::TrailingStop { .. }
            ))
        ));
    }

    #[test]
    fn test_build_trailing_stop_limit_at_close_unsupported() {
        let result = build_ib_order(
            rustrade_instrument::Side::Sell,
            100.0,
            &OrderKind::TrailingStopLimit {
                offset: Decimal::from(5),
                offset_type: TrailingOffsetType::Absolute,
                limit_offset: Decimal::from(1),
            },
            None, // Trailing stop limit uses limit_offset, not a fixed price
            &TimeInForce::AtClose,
        );

        assert!(matches!(
            result,
            Err(OrderMappingError::UnsupportedOrderKindForAtClose(
                OrderKind::TrailingStopLimit { .. }
            ))
        ));
    }

    #[test]
    fn test_build_good_till_date_order() {
        use chrono::{TimeZone, Utc};

        let expiry = Utc.with_ymd_and_hms(2025, 6, 30, 23, 59, 59).unwrap();
        let order = build_ib_order(
            rustrade_instrument::Side::Buy,
            100.0,
            &OrderKind::Limit,
            Some(Decimal::from(150)),
            &TimeInForce::GoodTillDate { expiry },
        )
        .unwrap();

        assert_eq!(order.action, Action::Buy);
        assert_eq!(order.total_quantity, 100.0);
        assert_eq!(order.order_type, "LMT");
        assert_eq!(order.tif, IbTimeInForce::GoodTillDate);
        assert_eq!(order.good_till_date, "20250630-23:59:59");
    }

    #[test]
    fn test_format_gtd_datetime() {
        use chrono::{TimeZone, Utc};

        let dt = Utc.with_ymd_and_hms(2024, 12, 25, 14, 30, 0).unwrap();
        assert_eq!(format_gtd_datetime(&dt), "20241225-14:30:00");

        let dt2 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(format_gtd_datetime(&dt2), "20250101-00:00:00");
    }

    // =========================================================================
    // Bracket Order Tests
    // =========================================================================

    #[test]
    fn test_build_ib_bracket_with_oca_sets_oca_fields() {
        let orders = build_ib_bracket_with_oca(
            1000,
            Action::Buy,
            10.0,
            150.0,
            160.0,
            140.0,
            IbTimeInForce::Day,
        )
        .unwrap();

        assert_eq!(orders.len(), 3);

        // OCA group must be non-empty on both child orders
        assert!(!orders[1].oca_group.is_empty());
        assert_eq!(orders[1].oca_group, orders[2].oca_group);

        // Both children must use CancelWithBlock
        assert_eq!(orders[1].oca_type, OcaType::CancelWithBlock);
        assert_eq!(orders[2].oca_type, OcaType::CancelWithBlock);

        // Parent must NOT be in OCA group (only children are linked)
        assert!(orders[0].oca_group.is_empty());
        assert_eq!(orders[0].oca_type, OcaType::None);
    }

    #[test]
    fn test_build_ib_bracket_with_oca_parent_id_linkage() {
        let orders = build_ib_bracket_with_oca(
            1000,
            Action::Buy,
            10.0,
            150.0,
            160.0,
            140.0,
            IbTimeInForce::Day,
        )
        .unwrap();

        // Children must reference parent via parent_id
        assert_eq!(orders[1].parent_id, 1000); // TP waits for parent fill
        assert_eq!(orders[2].parent_id, 1000); // SL waits for parent fill

        // Parent has no parent
        assert_eq!(orders[0].parent_id, 0);
    }

    #[test]
    fn test_build_ib_bracket_with_oca_transmit_flags() {
        let orders = build_ib_bracket_with_oca(
            1000,
            Action::Buy,
            10.0,
            150.0,
            160.0,
            140.0,
            IbTimeInForce::Day,
        )
        .unwrap();

        // Only the last order triggers transmission of all three
        assert!(!orders[0].transmit); // Parent: don't transmit yet
        assert!(!orders[1].transmit); // TP: don't transmit yet
        assert!(orders[2].transmit); // SL: transmit all
    }

    #[test]
    fn test_build_ib_bracket_with_oca_order_ids_are_consecutive() {
        let orders = build_ib_bracket_with_oca(
            500,
            Action::Sell,
            5.0,
            100.0,
            90.0,
            110.0,
            IbTimeInForce::Day,
        )
        .unwrap();

        assert_eq!(orders[0].order_id, 500); // Parent
        assert_eq!(orders[1].order_id, 501); // Take profit
        assert_eq!(orders[2].order_id, 502); // Stop loss
    }

    #[test]
    fn test_build_ib_bracket_with_oca_rejects_prices_on_the_wrong_side_of_the_entry() {
        // A buy's take profit below its entry.
        let result =
            build_ib_bracket_with_oca(7, Action::Buy, 1.0, 10.0, 9.0, 8.0, IbTimeInForce::Day);
        assert!(matches!(
            result,
            Err(OrderMappingError::InvalidBracketPrices(_))
        ));

        // A sell's stop loss below its entry.
        let result =
            build_ib_bracket_with_oca(7, Action::Sell, 1.0, 10.0, 8.0, 9.0, IbTimeInForce::Day);
        assert!(matches!(
            result,
            Err(OrderMappingError::InvalidBracketPrices(_))
        ));
    }

    #[test]
    fn test_build_ib_bracket_with_oca_group_name_contains_parent_id() {
        let orders =
            build_ib_bracket_with_oca(42, Action::Buy, 1.0, 10.0, 12.0, 8.0, IbTimeInForce::Day)
                .unwrap();

        assert!(orders[1].oca_group.contains("42"));
        assert!(orders[2].oca_group.contains("42"));
    }

    #[test]
    fn test_build_ib_bracket_with_oca_order_types() {
        let orders = build_ib_bracket_with_oca(
            1000,
            Action::Buy,
            100.0,
            150.0,
            160.0,
            140.0,
            IbTimeInForce::Day,
        )
        .unwrap();

        // Parent is limit order
        assert_eq!(orders[0].order_type, "LMT");
        assert_eq!(orders[0].limit_price, Some(150.0));

        // Take profit is limit order
        assert_eq!(orders[1].order_type, "LMT");
        assert_eq!(orders[1].limit_price, Some(160.0));

        // Stop loss is stop order
        assert_eq!(orders[2].order_type, "STP");
        assert_eq!(orders[2].aux_price, Some(140.0));
    }

    #[test]
    fn test_build_ib_bracket_with_oca_actions_reversed_for_children() {
        // Buy bracket: entry=Buy, exits=Sell
        let buy_orders =
            build_ib_bracket_with_oca(100, Action::Buy, 10.0, 50.0, 55.0, 45.0, IbTimeInForce::Day)
                .unwrap();
        assert_eq!(buy_orders[0].action, Action::Buy);
        assert_eq!(buy_orders[1].action, Action::Sell);
        assert_eq!(buy_orders[2].action, Action::Sell);

        // Sell bracket: entry=Sell, exits=Buy
        let sell_orders = build_ib_bracket_with_oca(
            200,
            Action::Sell,
            10.0,
            50.0,
            45.0,
            55.0,
            IbTimeInForce::Day,
        )
        .unwrap();
        assert_eq!(sell_orders[0].action, Action::Sell);
        assert_eq!(sell_orders[1].action, Action::Buy);
        assert_eq!(sell_orders[2].action, Action::Buy);
    }

    #[test]
    fn test_build_ib_bracket_with_oca_applies_tif_to_all_legs() {
        let orders = build_ib_bracket_with_oca(
            1000,
            Action::Buy,
            10.0,
            150.0,
            160.0,
            140.0,
            IbTimeInForce::GoodTillCanceled,
        )
        .unwrap();

        assert_eq!(orders[0].tif, IbTimeInForce::GoodTillCanceled);
        assert_eq!(orders[1].tif, IbTimeInForce::GoodTillCanceled);
        assert_eq!(orders[2].tif, IbTimeInForce::GoodTillCanceled);
    }
}
