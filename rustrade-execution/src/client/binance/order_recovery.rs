//! Learning how orders ended while a Binance account stream was disconnected.
//!
//! A reconnect recovers the fills it missed by time, but an order cancelled, expired or rejected in
//! the meantime cannot be found that way: Binance filters its order history on when an order was
//! created, so a window opening at the disconnect misses an order placed before it. So the client
//! keeps the orders it knows to be live ([`KnownLiveOrders`]), and after a reconnect asks about
//! those the venue no longer lists, one by one, by client order id ([`fetch_ended_by_key`]).
//! [`UncheckedOrders`] holds which instruments still need that check, and when it is retried.

use super::shared::{GAP_RETRY_BASE_SECS, GapFailure, MAX_GAP_RETRIES, UnrecoveredFills};
use crate::{
    AccountEventKind, UnindexedAccountEvent,
    error::UnindexedClientError,
    order::{
        UnindexedInactiveOrder, UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId},
        state::{ActiveOrderState, Open, OrderState},
    },
};
use fnv::{FnvHashMap, FnvHashSet};
use lru::LruCache;
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use rustrade_integration::collection::snapshot::Snapshot;
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tracing::warn;

/// How many live orders [`KnownLiveOrders`] holds before it forgets the oldest.
///
/// Binance caps an account at 1,000 open orders on spot (`EXCHANGE_MAX_NUM_ORDERS`), and every
/// order leaves the set when it ends, so a full set means orders whose end was never seen, such as
/// a terminal report that could not be converted. The oldest is the likeliest to be one of those.
pub(crate) const MAX_KNOWN_LIVE_ORDERS: usize = 4_096;

/// How many recently ended client order ids [`KnownLiveOrders`] remembers, so that a late report
/// of an order as live cannot bring it back.
const RECENTLY_ENDED: NonZeroUsize = match NonZeroUsize::new(1_024) {
    Some(capacity) => capacity,
    None => panic!("a non-zero literal"),
};

/// How many order lookups run at once.
pub(crate) const ORDER_LOOKUPS_IN_FLIGHT: usize = 8;

/// The time budget for one check of how known orders ended.
pub(crate) const ORDER_CHECK_TIMEOUT_SECS: u64 = 30;

/// The known-live orders of one client, shared by its calls and its account streams.
pub(crate) type SharedKnownLiveOrders = Arc<parking_lot::Mutex<KnownLiveOrders>>;

/// What [`KnownLiveOrders`] keeps about one order.
#[derive(Debug)]
struct KnownLive {
    instrument: InstrumentNameExchange,
    /// The venue's id, when known, which is how a fill names its order.
    order_id: Option<OrderId>,
    quantity: Decimal,
    /// The order in which entries were added, to forget the oldest first.
    seq: u64,
}

/// The orders a client has seen live and not yet seen end, by client order id.
///
/// It learns of an order from the response to placing it, from a listing of open orders, and from
/// its live reports on the account stream, and drops it when it sees the order end there: a
/// terminal report, a successful cancel, or a fill that brings it to its full quantity. Whatever is
/// left after a disconnect is what a reconnect asks about.
///
/// A client order id is taken to name one order for good, as the engine takes it. One seen to end
/// is remembered for a while, so a report of it as live that arrives late, such as the response to
/// placing an order that has already filled, does not add it back.
#[derive(Debug)]
pub(crate) struct KnownLiveOrders {
    orders: FnvHashMap<ClientOrderId, KnownLive>,
    by_order_id: FnvHashMap<(InstrumentNameExchange, OrderId), ClientOrderId>,
    recently_ended: LruCache<ClientOrderId, ()>,
    next_seq: u64,
}

impl Default for KnownLiveOrders {
    fn default() -> Self {
        Self {
            orders: FnvHashMap::default(),
            by_order_id: FnvHashMap::default(),
            recently_ended: LruCache::new(RECENTLY_ENDED),
            next_seq: 0,
        }
    }
}

impl KnownLiveOrders {
    /// A new, empty set to share.
    pub(crate) fn shared() -> SharedKnownLiveOrders {
        Arc::new(parking_lot::Mutex::new(Self::default()))
    }

    /// Record that the order under `key`, of `quantity`, is live as `open` says. An `open` with
    /// nothing left to fill has ended instead.
    pub(crate) fn live(&mut self, key: &UnindexedOrderKey, quantity: Decimal, open: &Open) {
        let cid = &key.cid;
        if open.filled_quantity >= quantity {
            self.ended(cid);
            return;
        }
        if self.recently_ended.contains(cid) {
            return;
        }
        let order_id = open.id.assigned().cloned();
        // Re-indexed from scratch, so an entry under an id or instrument it no longer has goes.
        let indexed = if let Some(known) = self.orders.get_mut(cid) {
            let previous = known.order_id.take();
            if let Some(stale) = &previous {
                self.by_order_id
                    .remove(&(known.instrument.clone(), stale.clone()));
            }
            known.instrument = key.instrument.clone();
            known.quantity = quantity;
            known.order_id = order_id.or(previous);
            known.order_id.clone()
        } else {
            if self.orders.len() >= MAX_KNOWN_LIVE_ORDERS {
                self.forget_oldest();
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            self.orders.insert(
                cid.clone(),
                KnownLive {
                    instrument: key.instrument.clone(),
                    order_id: order_id.clone(),
                    quantity,
                    seq,
                },
            );
            order_id
        };
        if let Some(order_id) = indexed {
            self.by_order_id
                .insert((key.instrument.clone(), order_id), cid.clone());
        }
    }

    /// Record that the order `cid` has ended. Returns whether it was held as live.
    pub(crate) fn ended(&mut self, cid: &ClientOrderId) -> bool {
        self.recently_ended.put(cid.clone(), ());
        let Some(known) = self.orders.remove(cid) else {
            return false;
        };
        if let Some(order_id) = known.order_id {
            self.by_order_id.remove(&(known.instrument, order_id));
        }
        true
    }

    /// Learn from an event the account stream delivers.
    ///
    /// On the stream, an [`OrderCancelled`](AccountEventKind::OrderCancelled) always reports an
    /// order leaving the book, as an error when the venue rejected it after accepting it. A cancel
    /// request that fails is answered only to its caller, and never reaches this.
    pub(crate) fn observe(&mut self, event: &UnindexedAccountEvent) {
        match &event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => match &order.state {
                OrderState::Active(ActiveOrderState::Open(open)) => {
                    self.live(&order.key, order.quantity, open);
                }
                OrderState::Active(_) => {}
                OrderState::Inactive(_) => {
                    self.ended(&order.key.cid);
                }
            },
            AccountEventKind::OrderCancelled(response) => {
                self.ended(&response.key.cid);
            }
            AccountEventKind::Trade(trade) => {
                let Some(filled) = trade.order_filled_quantity else {
                    return;
                };
                let Some(cid) = self
                    .by_order_id
                    .get(&(trade.instrument.clone(), trade.order_id.clone()))
                else {
                    return;
                };
                if self
                    .orders
                    .get(cid)
                    .is_some_and(|known| filled >= known.quantity)
                {
                    let cid = cid.clone();
                    self.ended(&cid);
                }
            }
            _ => {}
        }
    }

    /// Whether the order `cid` is held as live.
    #[cfg(test)]
    pub(crate) fn contains(&self, cid: &ClientOrderId) -> bool {
        self.orders.contains_key(cid)
    }

    /// The instruments with an order held as live.
    pub(crate) fn instruments(&self) -> FnvHashSet<InstrumentNameExchange> {
        self.orders
            .values()
            .map(|known| known.instrument.clone())
            .collect()
    }

    /// The keys of the orders held as live on `instrument`, under `exchange`. Binance records no
    /// strategy, so each carries [`StrategyId::unknown`].
    pub(crate) fn keys_on(
        &self,
        exchange: ExchangeId,
        instrument: &InstrumentNameExchange,
    ) -> Vec<UnindexedOrderKey> {
        self.orders
            .iter()
            .filter(|(_, known)| known.instrument == *instrument)
            .map(|(cid, known)| UnindexedOrderKey {
                exchange,
                instrument: known.instrument.clone(),
                strategy: StrategyId::unknown(),
                cid: cid.clone(),
            })
            .collect()
    }

    fn forget_oldest(&mut self) {
        let Some(cid) = self
            .orders
            .iter()
            .min_by_key(|(_, known)| known.seq)
            .map(|(cid, _)| cid.clone())
        else {
            return;
        };
        warn!(
            %cid,
            held = MAX_KNOWN_LIVE_ORDERS,
            "Binance forgets the oldest order it holds as live: if it ended while the stream was \
             disconnected, a reconnect will not report it"
        );
        if let Some(known) = self.orders.remove(&cid)
            && let Some(order_id) = known.order_id
        {
            self.by_order_id.remove(&(known.instrument, order_id));
        }
    }
}

/// The instruments whose known-live orders a reconnect has yet to check, and when a failed check
/// is retried.
///
/// A reconnect adds every instrument with an order held as live ([`open`](Self::open)). An
/// instrument is checked only once fill recovery has no gap left on it, so the fills of an order
/// reach the stream before how it ended ([`ready`](Self::ready)). A check that fails is retried
/// after the same backoff as a fill gap, until it has failed [`MAX_GAP_RETRIES`] retries
/// ([`failed`](Self::failed)); a reconnect does not bring a retry forward.
#[derive(Debug, Default)]
pub(crate) struct UncheckedOrders {
    instruments: FnvHashSet<InstrumentNameExchange>,
    failures: u32,
    retry_at: Option<tokio::time::Instant>,
}

impl UncheckedOrders {
    /// Add `instruments` to those to check.
    pub(crate) fn open(&mut self, instruments: impl IntoIterator<Item = InstrumentNameExchange>) {
        self.instruments.extend(instruments);
    }

    /// When a failed check is next retried, or `None` when none is waiting for one.
    pub(crate) fn retry_at(&self) -> Option<tokio::time::Instant> {
        self.retry_at.filter(|_| !self.instruments.is_empty())
    }

    /// The instruments due for a check at `now` that fill recovery has no gap left on.
    pub(crate) fn ready(
        &self,
        unrecovered: &UnrecoveredFills,
        now: tokio::time::Instant,
    ) -> Vec<InstrumentNameExchange> {
        if self.retry_at.is_some_and(|retry_at| now < retry_at) {
            return Vec::new();
        }
        self.instruments
            .iter()
            .filter(|instrument| !unrecovered.covers(instrument))
            .cloned()
            .collect()
    }

    /// Record that `instruments` were checked.
    pub(crate) fn checked(&mut self, instruments: &[InstrumentNameExchange]) {
        for instrument in instruments {
            self.instruments.remove(instrument);
        }
        self.failures = 0;
        self.retry_at = None;
    }

    /// Record that a check failed or did not finish at `now`: it is retried after a backoff, or,
    /// once it has failed [`MAX_GAP_RETRIES`] retries, every instrument waiting is dropped. Returns
    /// which.
    pub(crate) fn failed(&mut self, now: tokio::time::Instant) -> GapFailure {
        self.failures += 1;
        if self.failures > MAX_GAP_RETRIES {
            self.instruments.clear();
            self.failures = 0;
            self.retry_at = None;
            return GapFailure::GivenUp;
        }
        let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << (self.failures - 1));
        self.retry_at = Some(now + delay);
        GapFailure::Retry(delay)
    }

    /// Whether no instrument is left to check.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

/// Answer [`OrderStatusClient::fetch_ended_orders`](crate::client::OrderStatusClient) by looking
/// each order up with `lookup`, which returns how the order under a key ended, or `None` when it
/// is live or unknown.
///
/// Keeps the trait's contract: each distinct instrument and client order id is looked up once,
/// under the first key that names it; a client order id is reported at most once, under the first
/// key that finds it; and any failed lookup fails the whole call. Up to
/// [`ORDER_LOOKUPS_IN_FLIGHT`] lookups run at once.
pub(crate) async fn fetch_ended_by_key<F, Fut>(
    orders: &[UnindexedOrderKey],
    lookup: F,
) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError>
where
    F: FnMut(UnindexedOrderKey) -> Fut,
    Fut: Future<Output = Result<Option<UnindexedInactiveOrder>, UnindexedClientError>>,
{
    use futures::{StreamExt as _, TryStreamExt as _};

    let mut asked = FnvHashSet::default();
    let lookups: Vec<UnindexedOrderKey> = orders
        .iter()
        .filter(|key| asked.insert((&key.instrument, &key.cid)))
        .cloned()
        .collect();
    // `buffered`, not `buffer_unordered`: the results come back in the order asked, which is what
    // makes the first key that finds an order the one it is reported under.
    let found: Vec<_> = futures::stream::iter(lookups)
        .map(lookup)
        .buffered(ORDER_LOOKUPS_IN_FLIGHT)
        .try_collect()
        .await?;
    let mut reported = FnvHashSet::default();
    Ok(found
        .into_iter()
        .flatten()
        .filter(|order| reported.insert(order.key.cid.clone()))
        .collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::{ApiError, OrderError},
        order::{
            Order, OrderKey, OrderKind, TimeInForce,
            id::VenueOrderId,
            request::UnindexedOrderResponseCancel,
            state::{Cancelled, InactiveOrderState},
        },
        trade::{AssetFees, Trade, TradeId},
    };
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use rustrade_instrument::{Side, asset::name::AssetNameExchange};

    fn key(instrument: &str, cid: &str) -> UnindexedOrderKey {
        OrderKey::new(
            ExchangeId::BinanceSpot,
            InstrumentNameExchange::new(instrument),
            StrategyId::unknown(),
            ClientOrderId::new(cid),
        )
    }

    fn open(order_id: &str, filled: Decimal) -> Open {
        Open::new(
            VenueOrderId::Assigned(OrderId::new(order_id)),
            Utc::now(),
            filled,
        )
    }

    fn event(
        kind: AccountEventKind<ExchangeId, AssetNameExchange, InstrumentNameExchange>,
    ) -> UnindexedAccountEvent {
        UnindexedAccountEvent::new(ExchangeId::BinanceSpot, kind)
    }

    fn snapshot(
        key: UnindexedOrderKey,
        state: OrderState<AssetNameExchange, InstrumentNameExchange>,
    ) -> UnindexedAccountEvent {
        event(AccountEventKind::OrderSnapshot(Snapshot(Order {
            key,
            side: Side::Buy,
            price: Some(dec!(100)),
            quantity: dec!(2),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state,
        })))
    }

    fn fill(order_id: &str, order_filled: Option<Decimal>) -> UnindexedAccountEvent {
        event(AccountEventKind::Trade(Trade::new(
            TradeId::new("t"),
            OrderId::new(order_id),
            InstrumentNameExchange::new("BTCUSDT"),
            StrategyId::unknown(),
            Utc::now(),
            Side::Buy,
            dec!(100),
            dec!(1),
            order_filled,
            AssetFees::new(AssetNameExchange::new("usdt"), Decimal::ZERO, None),
        )))
    }

    #[test]
    fn the_stream_adds_a_live_order_and_drops_it_at_each_kind_of_end() {
        let mut known = KnownLiveOrders::default();
        let cancelled = key("BTCUSDT", "cancelled");
        let rejected = key("BTCUSDT", "rejected");
        let inactive = key("BTCUSDT", "inactive");
        for (key, id) in [(&cancelled, "1"), (&rejected, "2"), (&inactive, "3")] {
            known.observe(&snapshot(
                key.clone(),
                OrderState::active(open(id, Decimal::ZERO)),
            ));
            assert!(known.contains(&key.cid));
        }

        known.observe(&event(AccountEventKind::OrderCancelled(
            UnindexedOrderResponseCancel {
                key: cancelled.clone(),
                state: Ok(Cancelled::new(OrderId::new("1"), Utc::now(), Decimal::ZERO)),
            },
        )));
        known.observe(&event(AccountEventKind::OrderCancelled(
            UnindexedOrderResponseCancel {
                key: rejected.clone(),
                state: Err(OrderError::Rejected(ApiError::OrderRejected("no".into()))),
            },
        )));
        known.observe(&snapshot(
            inactive.clone(),
            OrderState::Inactive(InactiveOrderState::Cancelled(Cancelled::new(
                OrderId::new("3"),
                Utc::now(),
                Decimal::ZERO,
            ))),
        ));

        assert!(known.instruments().is_empty(), "every order ended");
    }

    #[test]
    fn a_fill_drops_its_order_only_once_it_reports_the_whole_quantity() {
        let mut known = KnownLiveOrders::default();
        let order = key("BTCUSDT", "a");
        known.live(&order, dec!(2), &open("7", Decimal::ZERO));

        known.observe(&fill("7", None));
        assert!(known.contains(&order.cid), "no cumulative says nothing");
        known.observe(&fill("7", Some(dec!(1))));
        assert!(known.contains(&order.cid), "half filled is still live");
        known.observe(&fill("8", Some(dec!(2))));
        assert!(known.contains(&order.cid), "another order's fill");
        known.observe(&fill("7", Some(dec!(2))));
        assert!(!known.contains(&order.cid), "filled");
    }

    #[test]
    fn a_report_without_the_venue_id_keeps_the_one_known() {
        let mut known = KnownLiveOrders::default();
        let order = key("BTCUSDT", "a");
        known.live(&order, dec!(2), &open("7", Decimal::ZERO));
        known.live(
            &order,
            dec!(2),
            &Open::new(VenueOrderId::ClientAssigned, Utc::now(), dec!(1)),
        );

        known.observe(&fill("7", Some(dec!(2))));
        assert!(!known.contains(&order.cid), "still found by its venue id");
    }

    #[test]
    fn an_order_with_nothing_left_or_already_ended_is_not_added() {
        let mut known = KnownLiveOrders::default();
        let filled = key("BTCUSDT", "filled");
        known.live(&filled, dec!(2), &open("1", dec!(2)));
        assert!(!known.contains(&filled.cid));

        let late = key("BTCUSDT", "late");
        known.live(&late, dec!(2), &open("2", Decimal::ZERO));
        assert!(known.ended(&late.cid));
        known.live(&late, dec!(2), &open("2", Decimal::ZERO));
        assert!(
            !known.contains(&late.cid),
            "a late live report does not bring it back"
        );
        assert!(!known.ended(&late.cid), "and it is no longer held");
    }

    #[test]
    fn a_full_set_forgets_its_oldest_order() {
        let mut known = KnownLiveOrders::default();
        for n in 0..=MAX_KNOWN_LIVE_ORDERS {
            known.live(
                &key("BTCUSDT", &n.to_string()),
                dec!(1),
                &open(&n.to_string(), Decimal::ZERO),
            );
        }
        assert!(!known.contains(&ClientOrderId::new("0")));
        assert!(known.contains(&ClientOrderId::new("1")));
        assert!(known.contains(&ClientOrderId::new(MAX_KNOWN_LIVE_ORDERS.to_string())));
        assert_eq!(
            known
                .keys_on(
                    ExchangeId::BinanceSpot,
                    &InstrumentNameExchange::new("BTCUSDT")
                )
                .len(),
            MAX_KNOWN_LIVE_ORDERS
        );
    }

    #[test]
    fn keys_on_names_only_that_instrument() {
        let mut known = KnownLiveOrders::default();
        known.live(&key("BTCUSDT", "a"), dec!(1), &open("1", Decimal::ZERO));
        known.live(&key("ETHUSDT", "b"), dec!(1), &open("2", Decimal::ZERO));

        assert_eq!(
            known.keys_on(
                ExchangeId::BinanceSpot,
                &InstrumentNameExchange::new("ETHUSDT")
            ),
            [key("ETHUSDT", "b")]
        );
        assert_eq!(known.instruments().len(), 2);
    }

    #[test]
    fn an_instrument_with_a_fill_gap_waits_for_it() {
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let eth = InstrumentNameExchange::new("ETHUSDT");
        let mut unrecovered = UnrecoveredFills::default();
        let now = Utc::now();
        unrecovered.open(
            std::slice::from_ref(&btc),
            now - chrono::Duration::minutes(1),
            now,
        );
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([btc.clone(), eth.clone()]);

        assert_eq!(
            unchecked.ready(&unrecovered, tokio::time::Instant::now()),
            std::slice::from_ref(&eth)
        );
        unchecked.checked(&[eth]);
        assert!(!unchecked.is_empty(), "BTCUSDT still waits");
        assert_eq!(unchecked.retry_at(), None, "on its gap, not on a timer");

        let unrecovered = UnrecoveredFills::default();
        assert_eq!(
            unchecked.ready(&unrecovered, tokio::time::Instant::now()),
            std::slice::from_ref(&btc)
        );
        unchecked.checked(&[btc]);
        assert!(unchecked.is_empty());
    }

    #[test]
    fn a_failed_check_backs_off_and_is_given_up_after_its_retries() {
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
        let unrecovered = UnrecoveredFills::default();
        let now = tokio::time::Instant::now();

        for retry in 0..MAX_GAP_RETRIES {
            let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << retry);
            assert_eq!(unchecked.failed(now), GapFailure::Retry(delay));
            assert_eq!(unchecked.retry_at(), Some(now + delay));
            assert!(unchecked.ready(&unrecovered, now).is_empty(), "waits");
            assert_eq!(unchecked.ready(&unrecovered, now + delay).len(), 1);
        }
        assert_eq!(unchecked.failed(now), GapFailure::GivenUp);
        assert!(unchecked.is_empty());
        assert_eq!(unchecked.retry_at(), None);
    }

    fn cancelled_order(key: &UnindexedOrderKey) -> UnindexedInactiveOrder {
        Order {
            key: key.clone(),
            side: Side::Buy,
            price: Some(dec!(100)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: InactiveOrderState::Cancelled(Cancelled::new(
                OrderId::new("1"),
                Utc::now(),
                Decimal::ZERO,
            )),
        }
    }

    #[tokio::test]
    async fn each_order_is_looked_up_once_and_reported_under_the_first_key_that_finds_it() {
        let wrong = key("ETHUSDT", "a");
        let right = key("BTCUSDT", "a");
        let mut repeated = key("BTCUSDT", "a");
        repeated.strategy = StrategyId::new("other");
        let live = key("BTCUSDT", "live");
        let asked = std::sync::Mutex::new(Vec::new());

        let ended = fetch_ended_by_key(
            &[wrong.clone(), right.clone(), repeated, live.clone(), live],
            |key| {
                asked.lock().unwrap().push(key.clone());
                async move {
                    Ok((key.instrument.name() == "BTCUSDT" && key.cid.0 == "a")
                        .then(|| cancelled_order(&key)))
                }
            },
        )
        .await
        .unwrap();

        let reported: Vec<_> = ended.iter().map(|order| &order.key).collect();
        assert_eq!(reported, [&right], "under the key that found it, once");
        assert_eq!(
            *asked.lock().unwrap(),
            [wrong, right, key("BTCUSDT", "live")],
            "a repeated instrument and cid is not asked again"
        );
    }

    #[tokio::test]
    async fn one_failed_lookup_fails_the_call() {
        let result = fetch_ended_by_key(
            &[key("BTCUSDT", "a"), key("BTCUSDT", "b")],
            |key| async move {
                if key.cid.0 == "b" {
                    Err(UnindexedClientError::Api(ApiError::RateLimit))
                } else {
                    Ok(Some(cancelled_order(&key)))
                }
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(UnindexedClientError::Api(ApiError::RateLimit))
        ));
    }

    #[tokio::test]
    async fn nothing_asked_is_nothing_looked_up() {
        let ended = fetch_ended_by_key(&[], |_| async {
            panic!("nothing to look up");
        })
        .await
        .unwrap();
        assert!(ended.is_empty());
    }
}
