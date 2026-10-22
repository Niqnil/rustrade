//! Which of an instrument's orders were sent reduce-only, kept so a fill can be matched to the
//! flag of its order.
//!
//! Neither an [`Order`](crate::order::Order) nor a venue's report of one carries the flag set by
//! [`RequestOpen::reduce_only`](crate::order::request::RequestOpen::reduce_only). Whoever sends
//! the request is the one place that learns it, so whoever needs it for a fill later, such as a
//! tracker that cuts a reduce-only fill short of flipping a position (see
//! [`Trade::holding_move`](crate::trade::Trade::holding_move)), has to keep it. A
//! [`ReduceOnlyBook`] is that record, and finds the order of a fill by its client order id or, for
//! a fill that carries only the venue's id (a recovered fill), by the venue order id.

use crate::{
    order::id::{ClientOrderId, OrderId},
    trade::Trade,
};
use rustrade_integration::collection::FnvIndexMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// An order the book was told of, still in force.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
struct LiveOrder {
    reduce_only: bool,
    venue_id: Option<OrderId>,
}

/// An order found in a [`ReduceOnlyBook`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BookedOrder<'a> {
    /// The order's client order id, as the book holds it.
    pub cid: &'a ClientOrderId,
    /// Whether the order was sent reduce-only.
    pub reduce_only: bool,
}

/// One instrument's record of which orders were sent reduce-only. Plain data with no locking and
/// no I/O, so the owner keeps it behind whatever synchronisation it already has, one book for each
/// instrument.
///
/// The owner calls, for each order:
/// 1. [`register`](Self::register) when the open request is sent,
/// 2. [`acknowledge`](Self::acknowledge) when the venue reports the order's id, and
/// 3. [`retire`](Self::retire) when the order ends. **Every** order must be retired, or the book
///    keeps it for good.
///
/// Then [`lookup`](Self::lookup) finds the flag of a fill's order.
///
/// A retired order stays findable only if it was reduce-only, and only the last
/// [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY) of those, oldest dropped first, so that a late
/// fill of one is still recognised and ordinary orders do not push them out. A late fill of any
/// other retired order is not found: read that as not reduce-only.
///
/// # Serialised form
/// `{"live": {<cid>: {"reduce_only": bool, "venue_id": <id>|null}}, "retired": {<cid>: <id>|null}}`,
/// each map in order, either part optional. It is stored as part of its owner's state, so a
/// change to it is a breaking change. A book loaded with more retired orders than
/// [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY) keeps the latest.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "BookParts")]
pub struct ReduceOnlyBook {
    /// In the order registered.
    live: FnvIndexMap<ClientOrderId, LiveOrder>,
    /// The reduce-only orders that ended, each with its venue order id once known, in the order
    /// they ended.
    retired: FnvIndexMap<ClientOrderId, Option<OrderId>>,
    /// How many of `live` are reduce-only, so that [`has_reduce_only`](Self::has_reduce_only) is
    /// O(1). Derived, so it is not serialised.
    #[serde(skip)]
    live_reduce_only: usize,
}

/// What is serialised of a [`ReduceOnlyBook`]; loading one rebuilds the rest.
#[derive(Deserialize)]
struct BookParts {
    #[serde(default)]
    live: FnvIndexMap<ClientOrderId, LiveOrder>,
    #[serde(default)]
    retired: FnvIndexMap<ClientOrderId, Option<OrderId>>,
}

impl From<BookParts> for ReduceOnlyBook {
    fn from(parts: BookParts) -> Self {
        let BookParts { live, mut retired } = parts;
        // An order is live or retired, never both; the live one wins.
        retired.retain(|cid, _| !live.contains_key(cid));
        let excess = retired.len().saturating_sub(Self::RETIRED_CAPACITY);
        retired.drain(..excess);
        let live_reduce_only = live.values().filter(|order| order.reduce_only).count();
        Self {
            live,
            retired,
            live_reduce_only,
        }
    }
}

impl ReduceOnlyBook {
    /// How many retired reduce-only orders the book keeps.
    pub const RETIRED_CAPACITY: usize = 64;

    /// An empty book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the book holds no order, live or retired.
    pub fn is_empty(&self) -> bool {
        self.live.is_empty() && self.retired.is_empty()
    }

    /// How many orders are live: registered and not yet retired. An owner that retires every
    /// order sees this fall back to the number it has open.
    pub fn live_len(&self) -> usize {
        self.live.len()
    }

    /// Whether any order the book holds is reduce-only, live or retired. When it is not, every
    /// [`lookup`](Self::lookup) finds no reduce-only order, so a caller can skip it. O(1).
    pub fn has_reduce_only(&self) -> bool {
        self.live_reduce_only > 0 || !self.retired.is_empty()
    }

    /// Record that the open request `cid` is being sent, reduce-only or not.
    ///
    /// A client order id reused for a new order replaces everything an earlier order under it
    /// left, live or retired, so its flag does not outlive the order. The new order counts as the
    /// latest registered. Registering a client order id that is still live replaces that order,
    /// dropping the venue order id it was acknowledged with, so register once for each order sent.
    pub fn register(&mut self, cid: &ClientOrderId, reduce_only: bool) {
        self.retired.shift_remove(cid);
        if let Some(earlier) = self.live.shift_remove(cid) {
            self.live_reduce_only -= usize::from(earlier.reduce_only);
        }
        self.live_reduce_only += usize::from(reduce_only);
        self.live.insert(
            cid.clone(),
            LiveOrder {
                reduce_only,
                venue_id: None,
            },
        );
    }

    /// Record the venue's order id for `cid`, live or retired, so a fill that names only that id
    /// finds the order. Replaces an id given before. Does nothing for an order the book does not
    /// hold.
    ///
    /// An order can end before its id is known, such as one filled on its acknowledgement, so a
    /// retired order takes it too.
    pub fn acknowledge(&mut self, cid: &ClientOrderId, venue_id: &OrderId) {
        if let Some(live) = self.live.get_mut(cid) {
            live.venue_id = Some(venue_id.clone());
        } else if let Some(retired) = self.retired.get_mut(cid) {
            *retired = Some(venue_id.clone());
        }
    }

    /// Record that the order `cid` ended. Does nothing for an order the book does not hold.
    ///
    /// A reduce-only order moves to the retired orders, dropping the oldest of them when there
    /// are [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY). Any other order is forgotten.
    pub fn retire(&mut self, cid: &ClientOrderId) {
        if let Some(live) = self.live.shift_remove(cid)
            && live.reduce_only
        {
            self.live_reduce_only -= 1;
            self.insert_retired(cid.clone(), live.venue_id);
        }
    }

    /// [`retire`](Self::retire) every live order for which `is_tracked` is `false`, in the order
    /// they were registered, so which retirement is dropped first never depends on hashing.
    ///
    /// One pass over the live orders, so O(live orders), and it allocates only when a reduce-only
    /// order ended.
    pub fn retire_untracked(&mut self, mut is_tracked: impl FnMut(&ClientOrderId) -> bool) {
        let mut ended: Vec<(ClientOrderId, Option<OrderId>)> = Vec::new();
        self.live.retain(|cid, order| {
            let tracked = is_tracked(cid);
            if !tracked && order.reduce_only {
                ended.push((cid.clone(), order.venue_id.take()));
            }
            tracked
        });
        self.live_reduce_only -= ended.len();
        for (cid, venue_id) in ended {
            self.insert_retired(cid, venue_id);
        }
    }

    /// Find the order a fill belongs to.
    ///
    /// By the fill's client order id if it has one, and else by its venue order id: a live order
    /// first, then a retired one. A client order id the book does not hold finds nothing, and does
    /// not fall back to the venue's id.
    pub fn lookup<AssetKey, InstrumentKey>(
        &self,
        trade: &Trade<AssetKey, InstrumentKey>,
    ) -> Option<BookedOrder<'_>> {
        self.lookup_ids(trade.client_order_id.as_ref(), &trade.order_id)
    }

    /// [`lookup`](Self::lookup) for the ids of a fill, for a caller that resolved the client order
    /// id itself. A `cid` the book does not hold finds nothing and does not fall back to
    /// `order_id`; pass `None` to look up by the venue's id alone.
    pub fn lookup_ids(
        &self,
        cid: Option<&ClientOrderId>,
        order_id: &OrderId,
    ) -> Option<BookedOrder<'_>> {
        if let Some(cid) = cid {
            return self
                .live
                .get_key_value(cid)
                .map(|(cid, live)| BookedOrder {
                    cid,
                    reduce_only: live.reduce_only,
                })
                .or_else(|| {
                    self.retired.get_key_value(cid).map(|(cid, _)| BookedOrder {
                        cid,
                        reduce_only: true,
                    })
                });
        }
        // A live order outranks a retired one under the same venue order id. The scan is of the
        // open orders, and at most `RETIRED_CAPACITY` retired ones.
        self.live
            .iter()
            .find(|(_, live)| live.venue_id.as_ref() == Some(order_id))
            .map(|(cid, live)| BookedOrder {
                cid,
                reduce_only: live.reduce_only,
            })
            .or_else(|| {
                self.retired
                    .iter()
                    .find(|(_, venue_id)| venue_id.as_ref() == Some(order_id))
                    .map(|(cid, _)| BookedOrder {
                        cid,
                        reduce_only: true,
                    })
            })
    }

    /// Whether `trade` is a fill of an order sent reduce-only: [`lookup`](Self::lookup) with an
    /// order the book does not hold counted as not.
    pub fn is_reduce_only<AssetKey, InstrumentKey>(
        &self,
        trade: &Trade<AssetKey, InstrumentKey>,
    ) -> bool {
        self.lookup(trade).is_some_and(|order| order.reduce_only)
    }

    /// Forget every order, for when the orders the book records are dropped wholesale.
    pub fn clear(&mut self) {
        self.live.clear();
        self.retired.clear();
        self.live_reduce_only = 0;
    }

    fn insert_retired(&mut self, cid: ClientOrderId, venue_id: Option<OrderId>) {
        if self.retired.len() >= Self::RETIRED_CAPACITY
            && !self.retired.contains_key(&cid)
            && let Some((evicted, _)) = self.retired.shift_remove_index(0)
        {
            debug!(
                evicted_cid = %evicted,
                capacity = Self::RETIRED_CAPACITY,
                "Retired reduce-only orders at capacity: evicting the oldest. A late fill of the \
                 evicted order is now not recognised as reduce-only."
            );
        }
        self.retired.insert(cid, venue_id);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{order::id::StrategyId, trade::AssetFees};
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use rustrade_instrument::Side;
    use rustrade_instrument::asset::QuoteAsset;

    fn cid(id: &str) -> ClientOrderId {
        ClientOrderId::new(id)
    }

    fn venue(id: &str) -> OrderId {
        OrderId::new(id)
    }

    fn fill(cid: Option<&str>, venue_id: &str) -> Trade<QuoteAsset, ()> {
        let trade = Trade::new(
            crate::trade::TradeId::new("t"),
            venue(venue_id),
            (),
            StrategyId::new("s"),
            DateTime::UNIX_EPOCH,
            Side::Sell,
            dec!(1),
            dec!(1),
            None,
            AssetFees::quote_fees(dec!(0)),
        );
        match cid {
            Some(id) => trade.with_client_order_id(self::cid(id)),
            None => trade,
        }
    }

    fn flag(book: &ReduceOnlyBook, trade: &Trade<QuoteAsset, ()>) -> Option<bool> {
        book.lookup(trade).map(|order| order.reduce_only)
    }

    #[test]
    fn a_fill_finds_its_order_by_client_order_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        book.register(&cid("plain"), false);
        assert_eq!(flag(&book, &fill(Some("ro"), "v")), Some(true));
        assert_eq!(flag(&book, &fill(Some("plain"), "v")), Some(false));
        assert_eq!(flag(&book, &fill(Some("other"), "v")), None);
        assert!(book.is_reduce_only(&fill(Some("ro"), "v")));
        assert!(!book.is_reduce_only(&fill(Some("plain"), "v")));
        assert!(!book.is_reduce_only(&fill(Some("other"), "v")));
    }

    #[test]
    fn a_fill_naming_only_the_venue_id_finds_its_order_once_acknowledged() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        assert_eq!(flag(&book, &fill(None, "v1")), None, "not yet acknowledged");
        book.acknowledge(&cid("ro"), &venue("v1"));
        let trade = fill(None, "v1");
        let found = book.lookup(&trade).unwrap();
        assert_eq!((found.cid, found.reduce_only), (&cid("ro"), true));
    }

    #[test]
    fn a_client_order_id_the_book_lacks_does_not_fall_back_to_the_venue_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        book.acknowledge(&cid("ro"), &venue("v1"));
        assert_eq!(flag(&book, &fill(Some("other"), "v1")), None);
    }

    #[test]
    fn a_retired_reduce_only_order_is_still_found_and_an_ordinary_one_is_forgotten() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        book.register(&cid("plain"), false);
        book.acknowledge(&cid("ro"), &venue("v1"));
        book.acknowledge(&cid("plain"), &venue("v2"));
        book.retire(&cid("ro"));
        book.retire(&cid("plain"));
        assert_eq!(flag(&book, &fill(Some("ro"), "x")), Some(true));
        assert_eq!(flag(&book, &fill(None, "v1")), Some(true));
        assert_eq!(flag(&book, &fill(Some("plain"), "x")), None);
        assert_eq!(flag(&book, &fill(None, "v2")), None);
    }

    #[test]
    fn an_order_retired_before_its_acknowledgement_takes_the_venue_id_after() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        book.retire(&cid("ro"));
        assert_eq!(flag(&book, &fill(None, "v1")), None);
        book.acknowledge(&cid("ro"), &venue("v1"));
        assert_eq!(flag(&book, &fill(None, "v1")), Some(true));
    }

    #[test]
    fn a_live_order_outranks_a_retired_one_under_the_same_venue_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("old"), true);
        book.acknowledge(&cid("old"), &venue("v1"));
        book.retire(&cid("old"));
        book.register(&cid("new"), false);
        book.acknowledge(&cid("new"), &venue("v1"));
        let trade = fill(None, "v1");
        let found = book.lookup(&trade).unwrap();
        assert_eq!((found.cid, found.reduce_only), (&cid("new"), false));
    }

    #[test]
    fn registering_a_reused_client_order_id_drops_what_the_earlier_order_left() {
        for retire_first in [false, true] {
            let mut book = ReduceOnlyBook::new();
            book.register(&cid("c"), true);
            book.acknowledge(&cid("c"), &venue("v1"));
            if retire_first {
                book.retire(&cid("c"));
            }
            book.register(&cid("c"), false);
            assert_eq!(flag(&book, &fill(Some("c"), "x")), Some(false));
            assert_eq!(flag(&book, &fill(None, "v1")), None, "{retire_first}");
        }
    }

    #[test]
    fn the_oldest_retirement_is_dropped_at_capacity() {
        let mut book = ReduceOnlyBook::new();
        let cids: Vec<_> = (0..=ReduceOnlyBook::RETIRED_CAPACITY)
            .map(|i| cid(&format!("c{i}")))
            .collect();
        for c in &cids {
            book.register(c, true);
            book.retire(c);
        }
        assert_eq!(book.retired.len(), ReduceOnlyBook::RETIRED_CAPACITY);
        assert_eq!(flag(&book, &fill(Some("c0"), "x")), None);
        assert_eq!(flag(&book, &fill(Some("c1"), "x")), Some(true));
        assert_eq!(flag(&book, &fill(Some("c64"), "x")), Some(true));
    }

    #[test]
    fn ordinary_orders_do_not_push_out_retired_reduce_only_ones() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), true);
        book.retire(&cid("ro"));
        for i in 0..ReduceOnlyBook::RETIRED_CAPACITY * 2 {
            let c = cid(&format!("p{i}"));
            book.register(&c, false);
            book.retire(&c);
        }
        assert_eq!(flag(&book, &fill(Some("ro"), "x")), Some(true));
    }

    #[test]
    fn retire_untracked_retires_in_the_order_registered() {
        let mut book = ReduceOnlyBook::new();
        let cids: Vec<_> = (0..5).map(|i| cid(&format!("c{i}"))).collect();
        for c in &cids {
            book.register(c, true);
        }
        let kept = cid("c2");
        book.retire_untracked(|c| *c == kept);
        assert!(
            book.retired
                .keys()
                .eq([&cids[0], &cids[1], &cids[3], &cids[4]])
        );
        assert!(book.live.keys().eq([&kept]));
    }

    #[test]
    fn retire_untracked_keeps_the_reduce_only_count_and_forgets_ordinary_orders() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro1"), true);
        book.register(&cid("gone"), false);
        book.register(&cid("ro2"), true);
        book.register(&cid("kept"), false);
        book.retire_untracked(|c| *c == cid("kept"));
        assert_eq!(book.live_reduce_only, 0);
        assert!(book.live.keys().eq([&cid("kept")]));
        assert!(book.retired.keys().eq([&cid("ro1"), &cid("ro2")]));
        assert!(book.has_reduce_only(), "retired ones are still held");
        book.retired.clear();
        assert!(!book.has_reduce_only());
    }

    #[test]
    fn retiring_or_acknowledging_an_unknown_order_does_nothing() {
        let mut book = ReduceOnlyBook::new();
        book.retire(&cid("none"));
        book.acknowledge(&cid("none"), &venue("v"));
        assert!(book.is_empty());
    }

    #[test]
    fn clear_forgets_everything() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("a"), true);
        book.register(&cid("b"), true);
        book.retire(&cid("b"));
        book.clear();
        assert!(book.is_empty());
    }

    #[test]
    fn has_reduce_only_follows_the_orders_held() {
        let mut book = ReduceOnlyBook::new();
        assert!(!book.has_reduce_only());
        book.register(&cid("plain"), false);
        assert!(!book.has_reduce_only());
        book.register(&cid("ro"), true);
        assert!(book.has_reduce_only());
        book.register(&cid("ro"), false);
        assert!(!book.has_reduce_only(), "replaced by an ordinary order");
        book.register(&cid("ro"), true);
        book.retire(&cid("ro"));
        assert!(book.has_reduce_only(), "retired but still held");
        book.clear();
        assert!(!book.has_reduce_only());
        book.register(&cid("a"), true);
        book.register(&cid("b"), true);
        book.retire_untracked(|c| *c == cid("a"));
        assert_eq!((book.live_len(), book.has_reduce_only()), (1, true));
    }

    #[test]
    fn the_serialised_form_is_pinned_and_keeps_the_retirement_order() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("b"), true);
        book.acknowledge(&cid("b"), &venue("v"));
        book.retire(&cid("b"));
        book.register(&cid("a"), true);
        book.retire(&cid("a"));
        book.register(&cid("c"), false);
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(
            json,
            r#"{"live":{"c":{"reduce_only":false,"venue_id":null}},"retired":{"b":"v","a":null}}"#
        );
        let loaded: ReduceOnlyBook = serde_json::from_str(&json).unwrap();
        assert!(loaded.retired.keys().eq(book.retired.keys()));
        assert_eq!(loaded.has_reduce_only(), book.has_reduce_only());
    }

    #[test]
    fn a_loaded_book_keeps_its_bound_and_counts_its_live_reduce_only_orders() {
        // Built as text: `serde_json::Value` would sort the keys.
        let retired: Vec<String> = (0..100).map(|i| format!(r#""c{i}":null"#)).collect();
        let json = format!(
            r#"{{"live":{{"l":{{"reduce_only":true,"venue_id":null}},"c99":{{"reduce_only":false,"venue_id":null}}}},"retired":{{{}}}}}"#,
            retired.join(",")
        );
        let mut book: ReduceOnlyBook = serde_json::from_str(&json).unwrap();
        assert_eq!(book.retired.len(), ReduceOnlyBook::RETIRED_CAPACITY);
        assert!(book.retired.keys().next().is_some_and(|c| *c == cid("c35")));
        assert!(!book.retired.contains_key(&cid("c99")), "live wins");
        book.retire(&cid("l"));
        assert_eq!(book.retired.len(), ReduceOnlyBook::RETIRED_CAPACITY);
        book.retire(&cid("c99"));
        assert!(book.live.is_empty() && book.live_reduce_only == 0);
    }

    #[test]
    fn a_book_round_trips_through_json_and_loads_from_nothing() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("live"), true);
        book.acknowledge(&cid("live"), &venue("v1"));
        book.register(&cid("gone"), true);
        book.retire(&cid("gone"));
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(serde_json::from_str::<ReduceOnlyBook>(&json).unwrap(), book);
        assert_eq!(
            serde_json::from_str::<ReduceOnlyBook>("{}").unwrap(),
            ReduceOnlyBook::new()
        );
    }
}
