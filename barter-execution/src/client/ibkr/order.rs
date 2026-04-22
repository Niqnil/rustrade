use crate::order::{OrderKind, TimeInForce, id::ClientOrderId};
use barter_instrument::{Side, instrument::name::InstrumentNameExchange};
use fnv::FnvHashMap;
use ibapi::orders::{Action, Order, TimeInForce as IbTimeInForce, order_builder};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::{sync::Arc, time::Instant};

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
    pub price: Decimal,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
}

/// Bidirectional mapping between barter ClientOrderId and IB order IDs.
///
/// IB uses `i32` order IDs from a sequence. Barter uses `ClientOrderId` (SmolStr).
/// This map maintains the bidirectional relationship and stores order context
/// for reconstructing full `Order` structs from `OrderStatus` callbacks.
#[derive(Debug, Clone)]
pub struct OrderIdMap {
    inner: Arc<RwLock<OrderIdMapInner>>,
}

#[derive(Debug, Default)]
struct OrderIdMapInner {
    cid_to_ib: FnvHashMap<ClientOrderId, i32>,
    /// Merged map: IB order ID → (ClientOrderId, OrderContext, registration time) for single-lookup on hot path.
    /// The Instant tracks when the order was registered, enabling age-based cleanup.
    ib_to_entry: FnvHashMap<i32, (ClientOrderId, OrderContext, Instant)>,
}

impl OrderIdMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(OrderIdMapInner::default())),
        }
    }

    /// Register a mapping between ClientOrderId and IB order ID with order context.
    pub fn register(&self, client_id: ClientOrderId, ib_id: i32, context: OrderContext) {
        let mut inner = self.inner.write();
        inner.cid_to_ib.insert(client_id.clone(), ib_id);
        inner
            .ib_to_entry
            .insert(ib_id, (client_id, context, Instant::now()));
    }

    /// Look up IB order ID by ClientOrderId.
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

    /// Remove mapping and return context in a single write lock acquisition.
    ///
    /// Use this for terminal status events (Cancelled/Inactive) to avoid the
    /// read-then-write pattern of `get_client_id_and_context` + `remove_by_ib_id`.
    pub fn remove_and_get_context(&self, ib_id: i32) -> Option<(ClientOrderId, OrderContext)> {
        let mut inner = self.inner.write();
        if let Some((client_id, ctx, _)) = inner.ib_to_entry.remove(&ib_id) {
            inner.cid_to_ib.remove(&client_id);
            Some((client_id, ctx))
        } else {
            None
        }
    }

    /// Remove a mapping by IB order ID (used when order is fully filled/cancelled).
    pub fn remove_by_ib_id(&self, ib_id: i32) -> Option<ClientOrderId> {
        let mut inner = self.inner.write();
        if let Some((client_id, _, _)) = inner.ib_to_entry.remove(&ib_id) {
            inner.cid_to_ib.remove(&client_id);
            Some(client_id)
        } else {
            None
        }
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
    /// the filled status may not arrive at all. Removing mappings on terminal
    /// status would cause data loss.
    ///
    /// Instead, call this method periodically to clean up old mappings. A
    /// reasonable interval is 5-10 minutes with a max_age of 1 hour.
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
            if let Some((client_id, _, _)) = inner.ib_to_entry.remove(&ib_id) {
                inner.cid_to_ib.remove(&client_id);
            }
        }

        before - inner.ib_to_entry.len()
    }

    /// Number of active mappings.
    pub fn len(&self) -> usize {
        self.inner.read().cid_to_ib.len()
    }

    /// Check if map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().cid_to_ib.is_empty()
    }
}

impl Default for OrderIdMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert barter Side to IB Action.
pub fn side_to_action(side: barter_instrument::Side) -> Action {
    match side {
        barter_instrument::Side::Buy => Action::Buy,
        barter_instrument::Side::Sell => Action::Sell,
    }
}

/// Error when mapping barter order types to IB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderMappingError {
    PostOnlyNotSupported,
    /// Price conversion to f64 failed (overflow or invalid decimal).
    InvalidPrice(String),
}

impl std::fmt::Display for OrderMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PostOnlyNotSupported => write!(f, "post_only not supported by IB"),
            Self::InvalidPrice(p) => write!(f, "invalid price for f64 conversion: {p}"),
        }
    }
}

impl std::error::Error for OrderMappingError {}

/// Convert barter TimeInForce to IB TimeInForce.
pub fn time_in_force_to_ib(tif: &TimeInForce) -> Result<IbTimeInForce, OrderMappingError> {
    match tif {
        TimeInForce::GoodUntilCancelled { post_only } => {
            if *post_only {
                Err(OrderMappingError::PostOnlyNotSupported)
            } else {
                Ok(IbTimeInForce::GoodTilCanceled)
            }
        }
        TimeInForce::GoodUntilEndOfDay => Ok(IbTimeInForce::Day),
        TimeInForce::FillOrKill => Ok(IbTimeInForce::FillOrKill),
        TimeInForce::ImmediateOrCancel => Ok(IbTimeInForce::ImmediateOrCancel),
    }
}

/// Build an IB Order from barter order parameters.
pub fn build_ib_order(
    side: barter_instrument::Side,
    quantity: f64,
    kind: &OrderKind,
    price: rust_decimal::Decimal,
    tif: &TimeInForce,
) -> Result<Order, OrderMappingError> {
    let action = side_to_action(side);
    let tif_ib = time_in_force_to_ib(tif)?;

    let mut order = match kind {
        OrderKind::Market => order_builder::market_order(action, quantity),
        OrderKind::Limit => {
            let price_f64: f64 = price.try_into().or_else(|_| {
                price
                    .to_string()
                    .parse()
                    .map_err(|_| OrderMappingError::InvalidPrice(price.to_string()))
            })?;
            order_builder::limit_order(action, quantity, price_f64)
        }
    };

    order.tif = tif_ib;

    Ok(order)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn test_context() -> OrderContext {
        OrderContext {
            instrument: barter_instrument::instrument::name::InstrumentNameExchange::from("AAPL"),
            side: Side::Buy,
            price: Decimal::from(150),
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

        map.register(cid.clone(), 42, ctx.clone());

        assert_eq!(map.get_ib_id(&cid), Some(42));
        assert_eq!(map.get_client_id(42), Some(cid.clone()));
        assert_eq!(map.len(), 1);

        let (retrieved_cid, retrieved_ctx) = map.get_client_id_and_context(42).unwrap();
        assert_eq!(retrieved_cid, cid);
        assert_eq!(retrieved_ctx.side, Side::Buy);
        assert_eq!(retrieved_ctx.price, Decimal::from(150));
    }

    #[test]
    fn test_order_id_map_remove() {
        let map = OrderIdMap::new();
        let cid = ClientOrderId::new("order-456");

        map.register(cid.clone(), 100, test_context());
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
            side_to_action(barter_instrument::Side::Buy),
            Action::Buy
        ));
        assert!(matches!(
            side_to_action(barter_instrument::Side::Sell),
            Action::Sell
        ));
    }

    #[test]
    fn test_time_in_force_conversion() {
        assert_eq!(
            time_in_force_to_ib(&TimeInForce::GoodUntilCancelled { post_only: false }),
            Ok(IbTimeInForce::GoodTilCanceled)
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
    fn test_build_market_order() {
        let order = build_ib_order(
            barter_instrument::Side::Buy,
            100.0,
            &OrderKind::Market,
            rust_decimal::Decimal::ZERO,
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
            barter_instrument::Side::Sell,
            50.0,
            &OrderKind::Limit,
            Decimal::try_from(150.5).unwrap(),
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

        map.register(cid.clone(), 50, ctx);
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
        map.register(ClientOrderId::new("old-1"), 1, test_context());
        map.register(ClientOrderId::new("old-2"), 2, test_context());

        // With zero max_age, all entries are stale
        let cleared = map.clear_stale(Duration::ZERO);
        assert_eq!(cleared, 2);
        assert!(map.is_empty());

        // Register new orders
        map.register(ClientOrderId::new("new-1"), 10, test_context());
        map.register(ClientOrderId::new("new-2"), 20, test_context());

        // With large max_age, nothing is stale
        let cleared = map.clear_stale(Duration::from_secs(3600));
        assert_eq!(cleared, 0);
        assert_eq!(map.len(), 2);
    }
}
