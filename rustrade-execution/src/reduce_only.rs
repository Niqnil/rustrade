//! Which of an instrument's orders were sent reduce-only, kept so a fill can be matched to the
//! [`ReduceOnly`] of its order.
//!
//! Neither an [`Order`](crate::order::Order) nor a venue's report of one carries what
//! [`RequestOpen::reduce_only`](crate::order::request::RequestOpen::reduce_only) set. Whoever
//! sends the request is the one place that learns it, so whoever needs it for a fill later, such
//! as a tracker that cuts a reduce-only fill short of flipping a position (see
//! [`Trade::holding_move`](crate::trade::Trade::holding_move)), has to keep it. A
//! [`ReduceOnlyBook`] is that record, and finds the order of a fill by its client order id or, for
//! a fill that carries only the venue's id (a recovered fill), by the venue order id.

use crate::{
    order::{
        id::{ClientOrderId, OrderId},
        request::ReduceOnly,
    },
    trade::Trade,
};
use rustrade_integration::collection::FnvIndexMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// An order the book was told of: live, or retired if it was reduce-only.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
struct SentOrder {
    reduce_only: ReduceOnly,
    venue_id: Option<OrderId>,
}

/// What is serialised of a retired order. Before the book kept a retired order's
/// [`ReduceOnly`], it kept its venue order id alone, and every retired order was
/// [`ReduceOnly::Position`].
#[derive(Deserialize)]
#[serde(untagged)]
enum RetiredParts {
    Sent(SentOrder),
    VenueId(Option<OrderId>),
}

impl From<RetiredParts> for SentOrder {
    fn from(parts: RetiredParts) -> Self {
        match parts {
            RetiredParts::Sent(order) => order,
            RetiredParts::VenueId(venue_id) => Self {
                reduce_only: ReduceOnly::Position,
                venue_id,
            },
        }
    }
}

/// An order found in a [`ReduceOnlyBook`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BookedOrder<'a> {
    /// The order's client order id, as the book holds it.
    pub cid: &'a ClientOrderId,
    /// What the order was sent as: never [`ReduceOnly::No`] for a retired order.
    pub reduce_only: ReduceOnly,
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
/// Then [`lookup`](Self::lookup) finds what a fill's order was sent as.
///
/// A retired order stays findable only if it was reduce-only, and only the last
/// [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY) of those, oldest dropped first, so that a late
/// fill of one is still recognised and ordinary orders do not push them out. A late fill of any
/// other retired order is not found: read that as not reduce-only.
///
/// # Serialised form
/// `{"live": {<cid>: <order>}, "retired": {<cid>: <order>}}`, where an `<order>` is
/// `{"reduce_only": <ReduceOnly>, "venue_id": <id>|null}`, each map in order, either part
/// optional. It is stored as part of its owner's state, so a change to it is a breaking change.
/// A book loaded with more retired orders than [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY) keeps
/// the latest. A retired order serialised as its venue order id alone (`<id>|null`), the form
/// before a retired order kept its [`ReduceOnly`], loads as [`ReduceOnly::Position`], as does a
/// `reduce_only` of `true`.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "BookParts")]
pub struct ReduceOnlyBook {
    /// In the order registered.
    live: FnvIndexMap<ClientOrderId, SentOrder>,
    /// The reduce-only orders that ended, in the order they ended.
    retired: FnvIndexMap<ClientOrderId, SentOrder>,
    /// How many of `live` are reduce-only, so that [`has_reduce_only`](Self::has_reduce_only) is
    /// O(1). Derived, so it is not serialised.
    #[serde(skip)]
    live_reduce_only: usize,
}

/// What is serialised of a [`ReduceOnlyBook`]; loading one rebuilds the rest.
#[derive(Deserialize)]
struct BookParts {
    #[serde(default)]
    live: FnvIndexMap<ClientOrderId, SentOrder>,
    #[serde(default)]
    retired: FnvIndexMap<ClientOrderId, RetiredParts>,
}

impl From<BookParts> for ReduceOnlyBook {
    fn from(parts: BookParts) -> Self {
        let BookParts { live, retired } = parts;
        // A retired order that was not reduce-only is not one the book would have kept.
        let mut retired: FnvIndexMap<ClientOrderId, SentOrder> = retired
            .into_iter()
            .map(|(cid, order)| (cid, SentOrder::from(order)))
            .filter(|(_, order)| order.reduce_only.is_reduce_only())
            .collect();
        // An order is live or retired, never both; the live one wins.
        retired.retain(|cid, _| !live.contains_key(cid));
        let excess = retired.len().saturating_sub(Self::RETIRED_CAPACITY);
        retired.drain(..excess);
        let live_reduce_only = live
            .values()
            .filter(|order| order.reduce_only.is_reduce_only())
            .count();
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

    /// Record that the open request `cid` is being sent, and what as.
    ///
    /// A client order id reused for a new order replaces everything an earlier order under it
    /// left, live or retired, so its flag does not outlive the order. The new order counts as the
    /// latest registered. Registering a client order id that is still live replaces that order,
    /// dropping the venue order id it was acknowledged with, so register once for each order sent.
    pub fn register(&mut self, cid: &ClientOrderId, reduce_only: ReduceOnly) {
        self.retired.shift_remove(cid);
        if let Some(earlier) = self.live.shift_remove(cid) {
            self.live_reduce_only -= usize::from(earlier.reduce_only.is_reduce_only());
        }
        self.live_reduce_only += usize::from(reduce_only.is_reduce_only());
        self.live.insert(
            cid.clone(),
            SentOrder {
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
        if let Some(order) = self.live.get_mut(cid).or_else(|| self.retired.get_mut(cid)) {
            order.venue_id = Some(venue_id.clone());
        }
    }

    /// Record that the order `cid` ended. Does nothing for an order the book does not hold.
    ///
    /// A reduce-only order moves to the retired orders, dropping the oldest of them when there
    /// are [`RETIRED_CAPACITY`](Self::RETIRED_CAPACITY). Any other order is forgotten.
    pub fn retire(&mut self, cid: &ClientOrderId) {
        if let Some(order) = self.live.shift_remove(cid)
            && order.reduce_only.is_reduce_only()
        {
            self.live_reduce_only -= 1;
            self.insert_retired(cid.clone(), order);
        }
    }

    /// [`retire`](Self::retire) every live order for which `is_tracked` is `false`, in the order
    /// they were registered, so which retirement is dropped first never depends on hashing.
    ///
    /// One pass over the live orders, so O(live orders), and it allocates only when a reduce-only
    /// order ended.
    pub fn retire_untracked(&mut self, mut is_tracked: impl FnMut(&ClientOrderId) -> bool) {
        let mut ended: Vec<(ClientOrderId, SentOrder)> = Vec::new();
        self.live.retain(|cid, order| {
            let tracked = is_tracked(cid);
            if !tracked && order.reduce_only.is_reduce_only() {
                ended.push((
                    cid.clone(),
                    SentOrder {
                        reduce_only: order.reduce_only,
                        venue_id: order.venue_id.take(),
                    },
                ));
            }
            tracked
        });
        self.live_reduce_only -= ended.len();
        for (cid, order) in ended {
            self.insert_retired(cid, order);
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
        fn booked<'a>((cid, order): (&'a ClientOrderId, &'a SentOrder)) -> BookedOrder<'a> {
            BookedOrder {
                cid,
                reduce_only: order.reduce_only,
            }
        }
        if let Some(cid) = cid {
            return self
                .live
                .get_key_value(cid)
                .or_else(|| self.retired.get_key_value(cid))
                .map(booked);
        }
        // A live order outranks a retired one under the same venue order id. The scan is of the
        // open orders, and at most `RETIRED_CAPACITY` retired ones.
        let by_venue_id =
            |(_, order): &(&ClientOrderId, &SentOrder)| order.venue_id.as_ref() == Some(order_id);
        self.live
            .iter()
            .find(by_venue_id)
            .or_else(|| self.retired.iter().find(by_venue_id))
            .map(booked)
    }

    /// Whether `trade` is a fill of an order sent reduce-only, either way: [`lookup`](Self::lookup)
    /// with an order the book does not hold counted as not.
    pub fn is_reduce_only<AssetKey, InstrumentKey>(
        &self,
        trade: &Trade<AssetKey, InstrumentKey>,
    ) -> bool {
        self.lookup(trade)
            .is_some_and(|order| order.reduce_only.is_reduce_only())
    }

    /// Forget every order, for when the orders the book records are dropped wholesale.
    pub fn clear(&mut self) {
        self.live.clear();
        self.retired.clear();
        self.live_reduce_only = 0;
    }

    fn insert_retired(&mut self, cid: ClientOrderId, order: SentOrder) {
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
        self.retired.insert(cid, order);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{order::id::StrategyId, trade::AssetFees};
    use ReduceOnly::{No, Position, PositionAndFreeBase};
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

    fn flag(book: &ReduceOnlyBook, trade: &Trade<QuoteAsset, ()>) -> Option<ReduceOnly> {
        book.lookup(trade).map(|order| order.reduce_only)
    }

    #[test]
    fn a_fill_finds_its_order_by_client_order_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        book.register(&cid("plain"), No);
        assert_eq!(flag(&book, &fill(Some("ro"), "v")), Some(Position));
        assert_eq!(flag(&book, &fill(Some("plain"), "v")), Some(No));
        assert_eq!(flag(&book, &fill(Some("other"), "v")), None);
        assert!(book.is_reduce_only(&fill(Some("ro"), "v")));
        assert!(!book.is_reduce_only(&fill(Some("plain"), "v")));
        assert!(!book.is_reduce_only(&fill(Some("other"), "v")));
    }

    #[test]
    fn a_fill_naming_only_the_venue_id_finds_its_order_once_acknowledged() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        assert_eq!(flag(&book, &fill(None, "v1")), None, "not yet acknowledged");
        book.acknowledge(&cid("ro"), &venue("v1"));
        let trade = fill(None, "v1");
        let found = book.lookup(&trade).unwrap();
        assert_eq!((found.cid, found.reduce_only), (&cid("ro"), Position));
    }

    #[test]
    fn a_client_order_id_the_book_lacks_does_not_fall_back_to_the_venue_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        book.acknowledge(&cid("ro"), &venue("v1"));
        assert_eq!(flag(&book, &fill(Some("other"), "v1")), None);
    }

    #[test]
    fn a_retired_reduce_only_order_is_still_found_and_an_ordinary_one_is_forgotten() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        book.register(&cid("plain"), No);
        book.acknowledge(&cid("ro"), &venue("v1"));
        book.acknowledge(&cid("plain"), &venue("v2"));
        book.retire(&cid("ro"));
        book.retire(&cid("plain"));
        assert_eq!(flag(&book, &fill(Some("ro"), "x")), Some(Position));
        assert_eq!(flag(&book, &fill(None, "v1")), Some(Position));
        assert_eq!(flag(&book, &fill(Some("plain"), "x")), None);
        assert_eq!(flag(&book, &fill(None, "v2")), None);
    }

    #[test]
    fn an_order_retired_before_its_acknowledgement_takes_the_venue_id_after() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        book.retire(&cid("ro"));
        assert_eq!(flag(&book, &fill(None, "v1")), None);
        book.acknowledge(&cid("ro"), &venue("v1"));
        assert_eq!(flag(&book, &fill(None, "v1")), Some(Position));
    }

    #[test]
    fn a_live_order_outranks_a_retired_one_under_the_same_venue_id() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("old"), Position);
        book.acknowledge(&cid("old"), &venue("v1"));
        book.retire(&cid("old"));
        book.register(&cid("new"), No);
        book.acknowledge(&cid("new"), &venue("v1"));
        let trade = fill(None, "v1");
        let found = book.lookup(&trade).unwrap();
        assert_eq!((found.cid, found.reduce_only), (&cid("new"), No));
    }

    #[test]
    fn registering_a_reused_client_order_id_drops_what_the_earlier_order_left() {
        for retire_first in [false, true] {
            let mut book = ReduceOnlyBook::new();
            book.register(&cid("c"), Position);
            book.acknowledge(&cid("c"), &venue("v1"));
            if retire_first {
                book.retire(&cid("c"));
            }
            book.register(&cid("c"), No);
            assert_eq!(flag(&book, &fill(Some("c"), "x")), Some(No));
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
            book.register(c, Position);
            book.retire(c);
        }
        assert_eq!(book.retired.len(), ReduceOnlyBook::RETIRED_CAPACITY);
        assert_eq!(flag(&book, &fill(Some("c0"), "x")), None);
        assert_eq!(flag(&book, &fill(Some("c1"), "x")), Some(Position));
        assert_eq!(flag(&book, &fill(Some("c64"), "x")), Some(Position));
    }

    #[test]
    fn ordinary_orders_do_not_push_out_retired_reduce_only_ones() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("ro"), Position);
        book.retire(&cid("ro"));
        for i in 0..ReduceOnlyBook::RETIRED_CAPACITY * 2 {
            let c = cid(&format!("p{i}"));
            book.register(&c, No);
            book.retire(&c);
        }
        assert_eq!(flag(&book, &fill(Some("ro"), "x")), Some(Position));
    }

    #[test]
    fn retire_untracked_retires_in_the_order_registered() {
        let mut book = ReduceOnlyBook::new();
        let cids: Vec<_> = (0..5).map(|i| cid(&format!("c{i}"))).collect();
        for c in &cids {
            book.register(c, Position);
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
        book.register(&cid("ro1"), Position);
        book.register(&cid("gone"), No);
        book.register(&cid("ro2"), Position);
        book.register(&cid("kept"), No);
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
        book.register(&cid("a"), Position);
        book.register(&cid("b"), Position);
        book.retire(&cid("b"));
        book.clear();
        assert!(book.is_empty());
    }

    #[test]
    fn is_empty_and_live_len_count_live_and_retired_orders() {
        let mut book = ReduceOnlyBook::new();
        assert_eq!((book.is_empty(), book.live_len()), (true, 0));
        book.register(&cid("plain"), No);
        book.register(&cid("ro"), Position);
        assert_eq!((book.is_empty(), book.live_len()), (false, 2), "live only");
        book.retire(&cid("ro"));
        assert_eq!((book.is_empty(), book.live_len()), (false, 1));
        book.retire(&cid("plain"));
        assert_eq!(
            (book.is_empty(), book.live_len()),
            (false, 0),
            "retired only"
        );
    }

    #[test]
    fn has_reduce_only_follows_the_orders_held() {
        let mut book = ReduceOnlyBook::new();
        assert!(!book.has_reduce_only());
        book.register(&cid("plain"), No);
        assert!(!book.has_reduce_only());
        book.register(&cid("ro"), Position);
        assert!(book.has_reduce_only());
        book.register(&cid("ro"), No);
        assert!(!book.has_reduce_only(), "replaced by an ordinary order");
        book.register(&cid("ro"), Position);
        book.retire(&cid("ro"));
        assert!(book.has_reduce_only(), "retired but still held");
        book.clear();
        assert!(!book.has_reduce_only());
        book.register(&cid("a"), Position);
        book.register(&cid("b"), Position);
        book.retire_untracked(|c| *c == cid("a"));
        assert_eq!((book.live_len(), book.has_reduce_only()), (1, true));
    }

    #[test]
    fn the_serialised_form_is_pinned_and_keeps_the_retirement_order() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("b"), Position);
        book.acknowledge(&cid("b"), &venue("v"));
        book.retire(&cid("b"));
        book.register(&cid("a"), Position);
        book.retire(&cid("a"));
        book.register(&cid("c"), No);
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(
            json,
            r#"{"live":{"c":{"reduce_only":"no","venue_id":null}},"retired":{"b":{"reduce_only":"position","venue_id":"v"},"a":{"reduce_only":"position","venue_id":null}}}"#
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
    fn a_book_serialised_with_a_reduce_only_flag_and_bare_retirements_loads() {
        // The form before the book kept a `ReduceOnly`: a flag for a live order, and the venue
        // order id alone for a retired one, which was always reduce-only.
        let json = r#"{"live":{"l":{"reduce_only":true,"venue_id":"v0"},"p":{"reduce_only":false,"venue_id":null}},"retired":{"b":"v1","a":null}}"#;
        let book: ReduceOnlyBook = serde_json::from_str(json).unwrap();
        assert_eq!(flag(&book, &fill(Some("l"), "x")), Some(Position));
        assert_eq!(flag(&book, &fill(Some("p"), "x")), Some(No));
        assert_eq!(flag(&book, &fill(Some("a"), "x")), Some(Position));
        assert_eq!(flag(&book, &fill(None, "v1")), Some(Position));
        assert_eq!(book.live_reduce_only, 1);
    }

    #[test]
    fn a_loaded_retirement_that_was_not_reduce_only_is_dropped() {
        let json = r#"{"retired":{"p":{"reduce_only":"no","venue_id":"v"}}}"#;
        let book: ReduceOnlyBook = serde_json::from_str(json).unwrap();
        assert!(book.is_empty() && !book.has_reduce_only());
    }

    #[test]
    fn an_order_keeps_what_it_was_sent_as_once_retired() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("sweep"), PositionAndFreeBase);
        book.register(&cid("close"), Position);
        book.acknowledge(&cid("sweep"), &venue("v1"));
        book.retire(&cid("sweep"));
        book.retire_untracked(|_| false);
        assert_eq!(
            flag(&book, &fill(Some("sweep"), "x")),
            Some(PositionAndFreeBase)
        );
        assert_eq!(flag(&book, &fill(None, "v1")), Some(PositionAndFreeBase));
        assert_eq!(flag(&book, &fill(Some("close"), "x")), Some(Position));
        assert!(book.is_reduce_only(&fill(Some("sweep"), "x")));
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(serde_json::from_str::<ReduceOnlyBook>(&json).unwrap(), book);
    }

    #[test]
    fn a_book_round_trips_through_json_and_loads_from_nothing() {
        let mut book = ReduceOnlyBook::new();
        book.register(&cid("live"), Position);
        book.acknowledge(&cid("live"), &venue("v1"));
        book.register(&cid("gone"), Position);
        book.retire(&cid("gone"));
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(serde_json::from_str::<ReduceOnlyBook>(&json).unwrap(), book);
        assert_eq!(
            serde_json::from_str::<ReduceOnlyBook>("{}").unwrap(),
            ReduceOnlyBook::new()
        );
    }
}
