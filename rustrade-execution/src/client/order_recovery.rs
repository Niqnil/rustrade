//! Learning how orders ended while an account stream was disconnected.
//!
//! A reconnect recovers the fills it missed by time, but an order cancelled, expired or rejected in
//! the meantime cannot be found that way: Binance and Alpaca both filter their order history on
//! when an order was created, so a window opening at the disconnect misses an order placed before
//! it. So the client keeps the orders it knows to be live ([`KnownLiveOrders`]), and after a
//! reconnect lists the open orders of each instrument it holds one on and asks about each held
//! order no longer listed, by client order id ([`recover_ended_orders`]). [`UncheckedOrders`] holds
//! which instruments still need that check, and when a failed one is retried. [`fetch_ended_by_key`]
//! answers [`OrderStatusClient`](crate::client::OrderStatusClient) with the same lookup.
//!
//! Nothing here names a venue's endpoint: each client passes in its own listing and lookup, and
//! says through [`PendingFills`] which instruments still wait for fills it has to recover.

use crate::{
    AccountEventKind, UnindexedAccountEvent,
    error::UnindexedClientError,
    order::{
        UnindexedInactiveOrder, UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId},
        state::{ActiveOrderState, Open, OrderState, UnindexedOrderState},
    },
};
use fnv::{FnvHashMap, FnvHashSet};
use lru::LruCache;
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use rustrade_integration::collection::snapshot::Snapshot;
use std::{
    collections::BTreeMap,
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// How many times a failed read is retried after its first failure before it is given up: a
/// reconnect's check of how orders ended here, and a Binance fill gap.
pub(crate) const MAX_GAP_RETRIES: u32 = 5;
/// The wait before a failed read's first retry. It doubles after each failure, so the retries come
/// 1, 2, 4, 8 and 16 minutes apart, about half an hour in all.
pub(crate) const GAP_RETRY_BASE_SECS: u64 = 60;

/// What became of a read that failed or did not finish: a reconnect's check of how orders ended,
/// or a Binance fill gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GapFailure {
    /// It is read again after this delay.
    Retry(Duration),
    /// It has failed [`MAX_GAP_RETRIES`] retries and is dropped.
    GivenUp,
}

/// Which instruments still have fills a reconnect has to recover, so that how an order ended is
/// not reported before its fills.
pub(crate) trait PendingFills {
    /// Whether fills on `instrument` are still to be recovered.
    fn pending(&self, instrument: &InstrumentNameExchange) -> bool;
}

/// No fills left to recover, as for a client whose fill recovery is done, or given up, before its
/// order check starts.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(not(feature = "alpaca"), allow(dead_code))] // Only Alpaca recovers fills before its check.
pub(crate) struct NoPendingFills;

impl PendingFills for NoPendingFills {
    fn pending(&self, _: &InstrumentNameExchange) -> bool {
        false
    }
}

/// How many live orders [`KnownLiveOrders`] holds before it forgets the oldest.
///
/// Venues cap an account's open orders well below this (Binance at 1,000 on spot,
/// `EXCHANGE_MAX_NUM_ORDERS`), and every order leaves the set when it ends, so a full set means
/// orders whose end was never seen, such as a terminal report that could not be converted. The
/// oldest is the likeliest to be one of those.
pub(crate) const MAX_KNOWN_LIVE_ORDERS: usize = 4_096;

/// How many recently ended client order ids [`KnownLiveOrders`] remembers, so that a late report
/// of an order as live cannot bring it back.
const RECENTLY_ENDED: NonZeroUsize = match NonZeroUsize::new(1_024) {
    Some(capacity) => capacity,
    None => panic!("a non-zero literal"),
};

/// How many order lookups run at once: in [`fetch_ended_by_key`], and per listing in
/// [`recover_ended_orders`].
pub(crate) const ORDER_LOOKUPS_IN_FLIGHT: usize = 8;

/// How many listings [`recover_ended_orders`] checks at once. With [`ORDER_LOOKUPS_IN_FLIGHT`]
/// lookups each, at most 16 requests are in flight.
const LISTINGS_IN_FLIGHT: usize = 2;

/// The time budget for one pass of [`recover_ended_orders`].
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
    /// When the entry was added, as its key in [`KnownLiveOrders::by_age`].
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
    /// The venue, named in what the set logs.
    exchange: ExchangeId,
    orders: FnvHashMap<ClientOrderId, KnownLive>,
    by_order_id: FnvHashMap<(InstrumentNameExchange, OrderId), ClientOrderId>,
    by_instrument: OrdersByInstrument,
    /// Every held order by when it was added, oldest first.
    by_age: BTreeMap<u64, ClientOrderId>,
    recently_ended: LruCache<ClientOrderId, ()>,
    next_seq: u64,
    /// Whether the set is forgetting orders, so that it warns once per episode, not per order.
    forgetting: bool,
}

impl KnownLiveOrders {
    /// A new, empty set of `exchange`'s orders.
    pub(crate) fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            orders: FnvHashMap::default(),
            by_order_id: FnvHashMap::default(),
            by_instrument: OrdersByInstrument::default(),
            by_age: BTreeMap::new(),
            recently_ended: LruCache::new(RECENTLY_ENDED),
            next_seq: 0,
            forgetting: false,
        }
    }

    /// A new, empty set of `exchange`'s orders, to share.
    pub(crate) fn shared(exchange: ExchangeId) -> SharedKnownLiveOrders {
        Arc::new(parking_lot::Mutex::new(Self::new(exchange)))
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
            if known.instrument != key.instrument {
                self.by_instrument.remove(&known.instrument, cid);
                self.by_instrument.insert(&key.instrument, cid);
                known.instrument = key.instrument.clone();
            }
            known.quantity = quantity;
            known.order_id = order_id.or(previous);
            known.order_id.clone()
        } else {
            if self.orders.len() >= MAX_KNOWN_LIVE_ORDERS {
                self.forget_oldest();
            } else if self.orders.len() <= MAX_KNOWN_LIVE_ORDERS * 3 / 4 {
                self.forgetting = false;
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            self.by_age.insert(seq, cid.clone());
            self.by_instrument.insert(&key.instrument, cid);
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

    /// Record what the response to placing the order under `key`, of `quantity`, said: live, or
    /// already ended, which a later report of it as live then cannot undo.
    pub(crate) fn placed(
        &mut self,
        key: &UnindexedOrderKey,
        quantity: Decimal,
        state: &UnindexedOrderState,
    ) {
        match state {
            OrderState::Active(ActiveOrderState::Open(open)) => self.live(key, quantity, open),
            OrderState::Active(_) => {}
            OrderState::Inactive(_) => {
                self.ended(&key.cid);
            }
        }
    }

    /// Record that the order `cid` has ended. Returns whether it was held as live.
    pub(crate) fn ended(&mut self, cid: &ClientOrderId) -> bool {
        self.recently_ended.put(cid.clone(), ());
        self.remove(cid)
    }

    /// Whether [`observe`](Self::observe) learns anything from an event of this kind, so a caller
    /// can skip the lock for the rest.
    pub(crate) fn observes<ExchangeKey, AssetKey, InstrumentKey>(
        kind: &AccountEventKind<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> bool {
        matches!(
            kind,
            AccountEventKind::OrderSnapshot(_)
                | AccountEventKind::OrderCancelled(_)
                | AccountEventKind::Trade(_)
        )
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

    /// Panic unless the indexes agree with the orders held.
    #[cfg(test)]
    fn assert_consistent(&self) {
        assert_eq!(self.by_age.len(), self.orders.len(), "by_age");
        for (seq, cid) in &self.by_age {
            assert_eq!(self.orders.get(cid).map(|known| known.seq), Some(*seq));
        }
        for ((instrument, order_id), cid) in &self.by_order_id {
            let known = &self.orders[cid];
            assert_eq!(
                (&known.instrument, known.order_id.as_ref()),
                (instrument, Some(order_id))
            );
        }
        let with_ids = self
            .orders
            .values()
            .filter(|known| known.order_id.is_some());
        assert_eq!(with_ids.count(), self.by_order_id.len(), "by_order_id");
        for (instrument, cids) in &self.by_instrument.0 {
            assert!(!cids.is_empty(), "by_instrument keeps no empty entry");
            for cid in cids {
                assert_eq!(&self.orders[cid].instrument, instrument);
            }
        }
        let indexed: usize = self.by_instrument.0.values().map(FnvHashSet::len).sum();
        assert_eq!(indexed, self.orders.len(), "by_instrument");
    }

    /// Every instrument with an order held as live.
    #[cfg_attr(not(feature = "alpaca"), allow(dead_code))] // Only Alpaca streams every instrument.
    pub(crate) fn instruments(&self) -> Vec<InstrumentNameExchange> {
        self.by_instrument.0.keys().cloned().collect()
    }

    /// Those of `instruments` with an order held as live.
    pub(crate) fn instruments_among(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Vec<InstrumentNameExchange> {
        instruments
            .iter()
            .filter(|instrument| self.by_instrument.0.contains_key(*instrument))
            .cloned()
            .collect()
    }

    /// The keys of the orders held as live on `instrument`, under `exchange`. Venues record no
    /// strategy, so each carries [`StrategyId::unknown`].
    pub(crate) fn keys_on(
        &self,
        exchange: ExchangeId,
        instrument: &InstrumentNameExchange,
    ) -> Vec<UnindexedOrderKey> {
        self.by_instrument
            .0
            .get(instrument)
            .into_iter()
            .flatten()
            .map(|cid| UnindexedOrderKey {
                exchange,
                instrument: instrument.clone(),
                strategy: StrategyId::unknown(),
                cid: cid.clone(),
            })
            .collect()
    }

    fn remove(&mut self, cid: &ClientOrderId) -> bool {
        let Some(known) = self.orders.remove(cid) else {
            return false;
        };
        self.by_age.remove(&known.seq);
        self.by_instrument.remove(&known.instrument, cid);
        if let Some(order_id) = known.order_id {
            self.by_order_id.remove(&(known.instrument, order_id));
        }
        true
    }

    fn forget_oldest(&mut self) {
        let Some((_, cid)) = self.by_age.pop_first() else {
            return;
        };
        let exchange = self.exchange;
        if self.forgetting {
            debug!(%exchange, %cid, "Forgetting the oldest order held as live");
        } else {
            self.forgetting = true;
            warn!(
                %exchange,
                %cid,
                held = MAX_KNOWN_LIVE_ORDERS,
                "Holding the most orders as live the client can, and forgetting the oldest as \
                 more arrive: if one ended while the stream was disconnected, a reconnect will \
                 not report it. Further orders forgotten are logged at debug"
            );
        }
        self.remove(&cid);
    }
}

/// The client order ids of the orders [`KnownLiveOrders`] holds, by instrument. An instrument is
/// present only while it has an order held, so its keys are the instruments with one.
#[derive(Debug, Default)]
struct OrdersByInstrument(FnvHashMap<InstrumentNameExchange, FnvHashSet<ClientOrderId>>);

impl OrdersByInstrument {
    fn insert(&mut self, instrument: &InstrumentNameExchange, cid: &ClientOrderId) {
        self.0
            .entry(instrument.clone())
            .or_default()
            .insert(cid.clone());
    }

    fn remove(&mut self, instrument: &InstrumentNameExchange, cid: &ClientOrderId) {
        if let Some(cids) = self.0.get_mut(instrument) {
            cids.remove(cid);
            if cids.is_empty() {
                self.0.remove(instrument);
            }
        }
    }
}

/// When a failed check of one instrument is retried.
#[derive(Debug, Default)]
struct InstrumentCheck {
    failures: u32,
    retry_at: Option<tokio::time::Instant>,
}

/// The instruments whose known-live orders a reconnect has yet to check, each with when a failed
/// check of it is retried.
///
/// A reconnect adds every instrument it recovers fills for that has an order held as live
/// ([`open`](Self::open)). An instrument is checked only once it has no fills pending
/// ([`PendingFills`]), so the fills of an order reach the stream before how it ended
/// ([`ready`](Self::ready)). A check that fails is retried after the same backoff as a Binance
/// fill gap, until it has failed
/// [`MAX_GAP_RETRIES`] retries ([`failed`](Self::failed)); a reconnect does not bring a retry
/// forward.
#[derive(Debug, Default)]
pub(crate) struct UncheckedOrders(FnvHashMap<InstrumentNameExchange, InstrumentCheck>);

impl UncheckedOrders {
    /// Add `instruments` to those to check. One already waiting keeps its retry schedule.
    pub(crate) fn open(&mut self, instruments: impl IntoIterator<Item = InstrumentNameExchange>) {
        for instrument in instruments {
            self.0.entry(instrument).or_default();
        }
    }

    /// The instruments with no fills pending whose check is due at `now`, in name order, so they
    /// are checked in a stable order.
    pub(crate) fn ready(
        &self,
        pending: &impl PendingFills,
        now: tokio::time::Instant,
    ) -> Vec<InstrumentNameExchange> {
        let mut ready: Vec<_> = self
            .0
            .iter()
            .filter(|(instrument, check)| {
                !pending.pending(instrument) && check.retry_at.is_none_or(|at| at <= now)
            })
            .map(|(instrument, _)| instrument.clone())
            .collect();
        ready.sort_unstable();
        ready
    }

    /// When an instrument with no fills pending is next due, or `None` when there is none. One
    /// with fills pending is due only once they are settled, which fill recovery's own schedule
    /// wakes for, so it never makes this a time already past that [`ready`](Self::ready) would
    /// answer with nothing.
    pub(crate) fn next_due(&self, pending: &impl PendingFills) -> Option<tokio::time::Instant> {
        self.0
            .iter()
            .filter(|(instrument, _)| !pending.pending(instrument))
            .map(|(_, check)| check.retry_at.unwrap_or_else(tokio::time::Instant::now))
            .min()
    }

    /// Record that `instrument` was checked.
    pub(crate) fn checked(&mut self, instrument: &InstrumentNameExchange) {
        self.0.remove(instrument);
    }

    /// Record that a check of `instrument` failed or did not finish at `now`: it is retried after a
    /// backoff, or dropped once it has failed [`MAX_GAP_RETRIES`] retries. Returns which, or `None`
    /// if the instrument is not waiting.
    pub(crate) fn failed(
        &mut self,
        instrument: &InstrumentNameExchange,
        now: tokio::time::Instant,
    ) -> Option<GapFailure> {
        let check = self.0.get_mut(instrument)?;
        check.failures += 1;
        if check.failures > MAX_GAP_RETRIES {
            self.0.remove(instrument);
            return Some(GapFailure::GivenUp);
        }
        let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << (check.failures - 1));
        check.retry_at = Some(now + delay);
        Some(GapFailure::Retry(delay))
    }

    /// Whether `instrument` is waiting for a check.
    #[cfg(test)]
    pub(crate) fn contains(&self, instrument: &InstrumentNameExchange) -> bool {
        self.0.contains_key(instrument)
    }

    /// Whether no instrument is left to check.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// What looking up one order by its key found.
#[derive(Debug)]
pub(crate) enum OrderLookup {
    /// The order has ended, as this says. Boxed, as the other variants carry nothing.
    Ended(Box<UnindexedInactiveOrder>),
    /// The order is live, or in a state this version cannot read as ended.
    NotEnded,
    /// The venue does not know the order under the key's instrument.
    Unknown,
}

impl OrderLookup {
    /// How the order ended, if it has.
    pub(crate) fn ended(self) -> Option<UnindexedInactiveOrder> {
        match self {
            Self::Ended(order) => Some(*order),
            Self::NotEnded | Self::Unknown => None,
        }
    }
}

/// Answer [`OrderStatusClient::fetch_ended_orders`](crate::client::OrderStatusClient) by looking
/// each order up with `lookup`.
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
    Fut: Future<Output = Result<OrderLookup, UnindexedClientError>>,
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
    let found: Vec<OrderLookup> = futures::stream::iter(lookups)
        .map(lookup)
        .buffered(ORDER_LOOKUPS_IN_FLIGHT)
        .try_collect()
        .await?;
    let mut reported = FnvHashSet::default();
    Ok(found
        .into_iter()
        .filter_map(OrderLookup::ended)
        .filter(|order| reported.insert(order.key.cid.clone()))
        .collect())
}

/// How [`recover_ended_orders`] lists the open orders it compares the held ones with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenListing {
    /// One listing per instrument, as on a venue that lists the open orders of one symbol at a
    /// time. A listing that fails charges only its own instrument.
    // Only Binance lists by one symbol.
    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    PerInstrument,
    /// One listing of every instrument due, as on a venue that lists several symbols in one
    /// request. A listing that fails charges every instrument in it. Either way, a lookup that
    /// fails charges only the instrument of the order it asked about.
    // Only Alpaca lists several symbols.
    #[cfg_attr(not(feature = "alpaca"), allow(dead_code))]
    Batched,
}

/// Report how each order held as live ended, where it has, on every instrument due for a check:
/// the order lifecycle events missed while the stream was disconnected.
///
/// Checks only an instrument with no fills pending (see [`UncheckedOrders`]), so an order's
/// recovered fills reach the stream before how it ended. It lists the open orders with
/// `list_open`, which returns the client order ids listed on the instruments it is given, one
/// instrument or all of them at a time as `listing` says, and looks up with `lookup` each order
/// held as live that the listing no longer shows: one request per listing plus one per order that
/// ended, rather than one per order held. The orders held are read before the listings, so one
/// placed after a listing cannot be taken for one that ended.
///
/// Each order that has ended is sent as an [`AccountEventKind::OrderSnapshot`] of its inactive
/// state, under [`StrategyId::unknown`], and leaves the set. One the stream reported ending in the
/// meantime has left it already, and is not sent twice. One the venue does not know leaves the set
/// with a warning, since asking again cannot change that. One still live, or in a state this
/// version cannot read, stays.
///
/// Each lookup is settled as soon as it ends, and the instruments of each listing as soon as its
/// check ends, so dropping this part-way, as a disconnect during a retry does, loses nothing, and a
/// pass that outlasts [`ORDER_CHECK_TIMEOUT_SECS`] keeps what it finished. An instrument whose
/// listing or any lookup fails, or whose check does not finish, is retried later, asking only about
/// the orders still held; one never started stays due as it was.
pub(crate) async fn recover_ended_orders<L, LFut, Q, QFut>(
    exchange: ExchangeId,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    pending: &impl PendingFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    listing: OpenListing,
    list_open: L,
    lookup: Q,
) where
    L: Fn(Vec<InstrumentNameExchange>) -> LFut,
    LFut: Future<Output = Result<FnvHashSet<ClientOrderId>, UnindexedClientError>>,
    Q: Fn(UnindexedOrderKey) -> QFut,
    QFut: Future<Output = Result<OrderLookup, UnindexedClientError>>,
{
    use futures::StreamExt as _;

    let ready = unchecked.ready(pending, tokio::time::Instant::now());
    if ready.is_empty() {
        return;
    }
    let mut held: Vec<(InstrumentNameExchange, Vec<UnindexedOrderKey>)> = {
        let known = known.lock();
        ready
            .into_iter()
            .map(|instrument| {
                let keys = known.keys_on(exchange, &instrument);
                (instrument, keys)
            })
            .collect()
    };
    held.retain(|(instrument, keys)| {
        if keys.is_empty() {
            unchecked.checked(instrument);
        }
        !keys.is_empty()
    });
    if held.is_empty() {
        return;
    }
    info!(
        %exchange,
        instruments = held.len(),
        orders = held.iter().map(|(_, keys)| keys.len()).sum::<usize>(),
        "Checking how the orders held as live ended while disconnected"
    );

    // One check per listing.
    let checks: Vec<Vec<(InstrumentNameExchange, Vec<UnindexedOrderKey>)>> = match listing {
        OpenListing::PerInstrument => held.into_iter().map(|one| vec![one]).collect(),
        OpenListing::Batched => vec![held],
    };
    // Which checks have been settled, and how many have started: the stream starts them in order,
    // so those from `started` on were never read. The stream owns what it reads, so no future it
    // builds borrows from a closure argument, which `tokio::spawn` cannot prove `Send`.
    let instruments: Vec<Vec<InstrumentNameExchange>> = checks
        .iter()
        .map(|check| {
            check
                .iter()
                .map(|(instrument, _)| instrument.clone())
                .collect()
        })
        .collect();
    let mut settled = vec![false; checks.len()];
    let started = AtomicUsize::new(0);
    let (list_open, lookup) = (&list_open, &lookup);
    let recovery = async {
        let mut reported = 0u32;
        let mut runs =
            futures::stream::iter(checks.into_iter().enumerate().map(|(index, check)| {
                started.store(index + 1, Ordering::Relaxed);
                async move {
                    let (listed_on, keys): (Vec<_>, Vec<_>) = check.into_iter().unzip();
                    let listed = match list_open(listed_on).await {
                        Ok(listed) => listed,
                        Err(e) => return (index, Err(e)),
                    };
                    let unlisted: Vec<UnindexedOrderKey> = keys
                        .into_iter()
                        .flatten()
                        .filter(|key| !listed.contains(&key.cid))
                        .collect();
                    // Each lookup is settled as it ends, so one that fails, or a pass that times
                    // out, loses none of those already answered.
                    let mut lookups = futures::stream::iter(unlisted)
                        .map(|key| async move {
                            let found = lookup(key.clone()).await;
                            (key, found)
                        })
                        .buffered(ORDER_LOOKUPS_IN_FLIGHT);
                    let mut checked = CheckedListing::default();
                    while let Some((key, found)) = lookups.next().await {
                        match found {
                            Ok(found) => match settle(exchange, known, key, found, tx) {
                                Some(sent) => checked.sent += sent,
                                None => {
                                    checked.consumer_gone = true;
                                    break;
                                }
                            },
                            Err(e) => {
                                checked
                                    .failed
                                    .entry(key.instrument)
                                    .or_insert_with(|| e.to_string());
                            }
                        }
                    }
                    (index, Ok(checked))
                }
            }))
            .buffer_unordered(LISTINGS_IN_FLIGHT);
        while let Some((index, checked)) = runs.next().await {
            settled[index] = true;
            match checked {
                Ok(checked) => {
                    if checked.consumer_gone {
                        debug!(%exchange, "Order check: consumer dropped during recovery");
                        return;
                    }
                    reported += checked.sent;
                    for instrument in &instruments[index] {
                        match checked.failed.get(instrument) {
                            Some(reason) => {
                                order_check_failed(exchange, unchecked, instrument, reason);
                            }
                            None => unchecked.checked(instrument),
                        }
                    }
                }
                Err(e) => {
                    let reason = e.to_string();
                    for instrument in &instruments[index] {
                        order_check_failed(exchange, unchecked, instrument, &reason);
                    }
                }
            }
        }
        info!(%exchange, reported, "Order check complete");
    };
    if tokio::time::timeout(Duration::from_secs(ORDER_CHECK_TIMEOUT_SECS), recovery)
        .await
        .is_err()
    {
        let started = started.load(Ordering::Relaxed);
        for (check, _) in instruments[..started]
            .iter()
            .zip(&settled)
            .filter(|(_, settled)| !**settled)
        {
            for instrument in check {
                order_check_failed(exchange, unchecked, instrument, "order check timed out");
            }
        }
    }
}

/// What one listing's lookups came to, once each has been settled.
#[derive(Debug, Default)]
struct CheckedListing {
    /// How many orders that ended were sent.
    sent: u32,
    /// The instruments a lookup failed on, each with the first failure's reason.
    failed: FnvHashMap<InstrumentNameExchange, String>,
    /// Whether the consumer has gone, so the check stopped.
    consumer_gone: bool,
}

/// Apply one lookup to the set and send the order if it ended. Returns how many were sent (none or
/// one), or `None` if the consumer has gone.
///
/// The set's lock is held across the send, as the stream holds it across its own, so an order the
/// stream reports ending and this check reach the consumer in the order they were decided, and an
/// order is reported once.
fn settle(
    exchange: ExchangeId,
    known: &SharedKnownLiveOrders,
    key: UnindexedOrderKey,
    found: OrderLookup,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) -> Option<u32> {
    let mut known = known.lock();
    match found {
        OrderLookup::Ended(order) => {
            if !known.ended(&order.key.cid) {
                return Some(0);
            }
            let event = UnindexedAccountEvent::new(
                exchange,
                AccountEventKind::OrderSnapshot(Snapshot::new(
                    (*order).map_state(OrderState::Inactive),
                )),
            );
            tx.send(event).ok()?;
            Some(1)
        }
        OrderLookup::Unknown => {
            if known.ended(&key.cid) {
                warn!(
                    %exchange,
                    instrument = %key.instrument,
                    cid = %key.cid,
                    "The venue does not know an order held as live; a reconnect no longer asks \
                     about it"
                );
            }
            Some(0)
        }
        OrderLookup::NotEnded => Some(0),
    }
}

/// Record that the check of how the orders held as live on `instrument` ended did not finish,
/// because of `reason`, and log what follows: a retry, or giving the check up.
fn order_check_failed(
    exchange: ExchangeId,
    unchecked: &mut UncheckedOrders,
    instrument: &InstrumentNameExchange,
    reason: &str,
) {
    match unchecked.failed(instrument, tokio::time::Instant::now()) {
        Some(GapFailure::Retry(delay)) => warn!(
            %exchange,
            %instrument,
            retry_in_secs = delay.as_secs(),
            reason,
            "Could not check how the orders held as live ended, retrying later"
        ),
        Some(GapFailure::GivenUp) => error!(
            %exchange,
            %instrument,
            retries = MAX_GAP_RETRIES,
            reason,
            "Gave up checking how the orders held as live ended: an order that ended \
             while disconnected stays live in engine state until the next reconnect checks it; \
             reconcile with fetch_open_orders"
        ),
        None => {}
    }
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
            state::{Cancelled, Expired, Filled, InactiveOrderState, OpenInFlight},
        },
        trade::{AssetFees, Trade, TradeId},
    };
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use rustrade_instrument::{Side, asset::name::AssetNameExchange};

    /// Fills pending on `instrument` alone.
    fn pending_on(instrument: &InstrumentNameExchange) -> FnvHashSet<InstrumentNameExchange> {
        FnvHashSet::from_iter([instrument.clone()])
    }

    impl PendingFills for FnvHashSet<InstrumentNameExchange> {
        fn pending(&self, instrument: &InstrumentNameExchange) -> bool {
            self.contains(instrument)
        }
    }

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
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
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

        assert!(
            known
                .instruments_among(&[InstrumentNameExchange::new("BTCUSDT")])
                .is_empty(),
            "every order ended"
        );
    }

    #[test]
    fn a_fill_drops_its_order_only_once_it_reports_the_whole_quantity() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        let order = key("BTCUSDT", "a");
        known.live(&order, dec!(2), &open("7", Decimal::ZERO));

        known.observe(&fill("7", None));
        assert!(known.contains(&order.cid), "no cumulative says nothing");
        known.observe(&fill("7", Some(dec!(1))));
        assert!(known.contains(&order.cid), "half filled is still live");
        known.observe(&fill("8", Some(dec!(2))));
        assert!(known.contains(&order.cid), "another order's fill");
        known.assert_consistent();
        known.observe(&fill("7", Some(dec!(2))));
        assert!(!known.contains(&order.cid), "filled");
        known.assert_consistent();
    }

    #[test]
    fn a_report_without_the_venue_id_keeps_the_one_known() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        let order = key("BTCUSDT", "a");
        known.live(&order, dec!(2), &open("7", Decimal::ZERO));
        known.live(
            &order,
            dec!(2),
            &Open::new(VenueOrderId::ClientAssigned, Utc::now(), dec!(1)),
        );

        known.assert_consistent();
        known.observe(&fill("7", Some(dec!(2))));
        assert!(!known.contains(&order.cid), "still found by its venue id");
        known.assert_consistent();
    }

    #[test]
    fn an_order_with_nothing_left_or_already_ended_is_not_added() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
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
    fn a_placement_holds_an_open_order_and_settles_an_ended_one() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        let open_order = key("BTCUSDT", "open");
        known.placed(
            &open_order,
            dec!(2),
            &OrderState::active(open("1", Decimal::ZERO)),
        );
        assert!(known.contains(&open_order.cid));

        let in_flight = key("BTCUSDT", "in-flight");
        known.placed(&in_flight, dec!(2), &OrderState::active(OpenInFlight));
        assert!(
            !known.contains(&in_flight.cid),
            "not yet known to the venue"
        );

        let ended = [
            (
                "filled",
                OrderState::fully_filled(Filled::new(OrderId::new("2"), Utc::now(), dec!(2), None)),
            ),
            (
                "expired",
                OrderState::inactive(Expired::new(OrderId::new("3"), Utc::now(), dec!(1))),
            ),
            (
                "failed",
                OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected("no".into()))),
            ),
        ];
        for (cid, state) in ended {
            let order = key("BTCUSDT", cid);
            known.placed(&order, dec!(2), &state);
            known.live(&order, dec!(2), &open("9", Decimal::ZERO));
            assert!(
                !known.contains(&order.cid),
                "{cid}: a late live report does not bring it back"
            );
        }
        known.assert_consistent();
    }

    #[test]
    fn a_full_set_forgets_its_oldest_order() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        // The oldest is alone on its instrument, so forgetting it stops holding that instrument.
        for n in 0..=MAX_KNOWN_LIVE_ORDERS {
            let instrument = if n == 0 { "ETHUSDT" } else { "BTCUSDT" };
            known.live(
                &key(instrument, &n.to_string()),
                dec!(1),
                &open(&n.to_string(), Decimal::ZERO),
            );
        }
        assert!(!known.contains(&ClientOrderId::new("0")));
        assert!(known.contains(&ClientOrderId::new("1")));
        assert_eq!(
            known.instruments(),
            [InstrumentNameExchange::new("BTCUSDT")]
        );
        // An order that ends frees its place, so the next one forgets nothing.
        assert!(known.ended(&ClientOrderId::new("2")));
        known.live(&key("BTCUSDT", "new"), dec!(1), &open("new", Decimal::ZERO));
        assert!(known.contains(&ClientOrderId::new("1")));
        known.live(
            &key("BTCUSDT", "newer"),
            dec!(1),
            &open("newer", Decimal::ZERO),
        );
        assert!(
            !known.contains(&ClientOrderId::new("1")),
            "then the oldest again"
        );
        known.assert_consistent();
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
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        known.live(&key("BTCUSDT", "a"), dec!(1), &open("1", Decimal::ZERO));
        known.live(&key("ETHUSDT", "b"), dec!(1), &open("2", Decimal::ZERO));

        assert_eq!(
            known.keys_on(
                ExchangeId::BinanceSpot,
                &InstrumentNameExchange::new("ETHUSDT")
            ),
            [key("ETHUSDT", "b")]
        );
        let (btc, eth, sol) = (
            InstrumentNameExchange::new("BTCUSDT"),
            InstrumentNameExchange::new("ETHUSDT"),
            InstrumentNameExchange::new("SOLUSDT"),
        );
        assert_eq!(
            known.instruments_among(&[sol, eth.clone()]),
            [eth],
            "only those asked about that hold an order"
        );
        assert_eq!(known.instruments_among(std::slice::from_ref(&btc)), [btc]);
        known.assert_consistent();
    }

    #[test]
    fn an_instrument_stops_being_held_with_its_last_order() {
        let mut known = KnownLiveOrders::new(ExchangeId::BinanceSpot);
        let (btc, eth) = (
            InstrumentNameExchange::new("BTCUSDT"),
            InstrumentNameExchange::new("ETHUSDT"),
        );
        known.live(&key("BTCUSDT", "a"), dec!(1), &open("1", Decimal::ZERO));
        known.live(&key("BTCUSDT", "b"), dec!(1), &open("2", Decimal::ZERO));

        known.ended(&ClientOrderId::new("a"));
        assert_eq!(
            known.instruments(),
            std::slice::from_ref(&btc),
            "b still holds it"
        );
        known.assert_consistent();

        known.live(&key("ETHUSDT", "b"), dec!(1), &open("2", Decimal::ZERO));
        assert_eq!(
            known.instruments(),
            std::slice::from_ref(&eth),
            "b moved instrument"
        );
        assert!(known.keys_on(ExchangeId::BinanceSpot, &btc).is_empty());
        assert_eq!(
            known.keys_on(ExchangeId::BinanceSpot, &eth),
            [key("ETHUSDT", "b")]
        );
        known.assert_consistent();

        known.ended(&ClientOrderId::new("b"));
        assert!(known.instruments().is_empty());
        assert!(known.instruments_among(&[btc, eth]).is_empty());
        known.assert_consistent();
    }

    #[test]
    fn an_instrument_with_a_fill_gap_waits_for_it() {
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let eth = InstrumentNameExchange::new("ETHUSDT");
        let pending = pending_on(&btc);
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([btc.clone(), eth.clone()]);

        assert_eq!(
            unchecked.ready(&pending, tokio::time::Instant::now()),
            std::slice::from_ref(&eth)
        );
        unchecked.checked(&eth);
        assert!(unchecked.contains(&btc), "BTCUSDT still waits");
        assert_eq!(
            unchecked.next_due(&pending),
            None,
            "on its gap, not on a timer"
        );

        let pending = NoPendingFills;
        assert_eq!(
            unchecked.ready(&pending, tokio::time::Instant::now()),
            std::slice::from_ref(&btc)
        );
        assert!(unchecked.next_due(&pending).is_some(), "due now");
        unchecked.checked(&btc);
        assert!(unchecked.is_empty());
    }

    /// A retry that falls due while the instrument is back on a fill gap must not wake the loop for
    /// a check `ready` would refuse, or it spins until the gap settles.
    #[test]
    fn a_due_retry_on_an_instrument_back_on_a_gap_does_not_wake_the_loop() {
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([btc.clone()]);
        let start = tokio::time::Instant::now();
        unchecked.failed(&btc, start);
        let pending = pending_on(&btc);
        let later = start + Duration::from_secs(GAP_RETRY_BASE_SECS * 2);

        assert!(unchecked.ready(&pending, later).is_empty());
        assert_eq!(unchecked.next_due(&pending), None);
        assert_eq!(
            unchecked.ready(&NoPendingFills, later),
            std::slice::from_ref(&btc),
            "due again once the gap is settled"
        );
    }

    #[test]
    fn instruments_are_retried_on_their_own_schedules() {
        let (btc, eth) = (
            InstrumentNameExchange::new("BTCUSDT"),
            InstrumentNameExchange::new("ETHUSDT"),
        );
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([btc.clone(), eth.clone()]);
        let now = tokio::time::Instant::now();
        unchecked.failed(&btc, now);
        unchecked.failed(&btc, now);
        unchecked.failed(&eth, now);
        let pending = NoPendingFills;

        let base = Duration::from_secs(GAP_RETRY_BASE_SECS);
        assert_eq!(
            unchecked.ready(&pending, now + base),
            std::slice::from_ref(&eth)
        );
        assert_eq!(unchecked.next_due(&pending), Some(now + base));
        unchecked.open([btc.clone()]);
        assert!(
            unchecked.ready(&pending, now + base).len() == 1,
            "a reconnect does not bring BTCUSDT's retry forward"
        );
        assert_eq!(unchecked.ready(&pending, now + base * 2).len(), 2);
    }

    #[test]
    fn a_failed_check_backs_off_and_is_given_up_after_its_retries() {
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let pending = NoPendingFills;
        let now = tokio::time::Instant::now();

        for retry in 0..MAX_GAP_RETRIES {
            let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << retry);
            assert_eq!(unchecked.failed(&btc, now), Some(GapFailure::Retry(delay)));
            assert_eq!(unchecked.next_due(&pending), Some(now + delay));
            assert!(unchecked.ready(&pending, now).is_empty(), "waits");
            assert_eq!(unchecked.ready(&pending, now + delay).len(), 1);
        }
        assert_eq!(unchecked.failed(&btc, now), Some(GapFailure::GivenUp));
        assert!(unchecked.is_empty());
        assert_eq!(unchecked.next_due(&pending), None);
        assert_eq!(unchecked.failed(&btc, now), None, "no longer waiting");
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
                    Ok(if key.instrument.name() == "BTCUSDT" && key.cid.0 == "a" {
                        OrderLookup::Ended(Box::new(cancelled_order(&key)))
                    } else if key.cid.0 == "a" {
                        OrderLookup::Unknown
                    } else {
                        OrderLookup::NotEnded
                    })
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
                    Ok(OrderLookup::Ended(Box::new(cancelled_order(&key))))
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

    /// One order held on each of `instruments`, all due for a check.
    fn held_on(instruments: &[&str]) -> (SharedKnownLiveOrders, UncheckedOrders) {
        let known = KnownLiveOrders::shared(ExchangeId::BinanceSpot);
        let mut unchecked = UncheckedOrders::default();
        for (n, instrument) in instruments.iter().enumerate() {
            known.lock().live(
                &key(instrument, instrument),
                dec!(1),
                &open(&n.to_string(), Decimal::ZERO),
            );
            unchecked.open([InstrumentNameExchange::new(*instrument)]);
        }
        (known, unchecked)
    }

    /// Run a check whose listing never answers on an instrument named `SLOW…`, and lists nothing
    /// open on the others, whose orders have all ended.
    async fn check_with_slow_instruments(
        known: &SharedKnownLiveOrders,
        unchecked: &mut UncheckedOrders,
    ) -> Vec<UnindexedAccountEvent> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        recover_ended_orders(
            ExchangeId::BinanceSpot,
            known,
            unchecked,
            &NoPendingFills,
            &tx,
            OpenListing::PerInstrument,
            |instruments: Vec<InstrumentNameExchange>| async move {
                if instruments
                    .iter()
                    .any(|instrument| instrument.name().starts_with("SLOW"))
                {
                    std::future::pending::<()>().await;
                }
                Ok(FnvHashSet::default())
            },
            |key: UnindexedOrderKey| async move {
                Ok(OrderLookup::Ended(Box::new(cancelled_order(&key))))
            },
        )
        .await;
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    /// Two instruments are checked at a time, in name order. When the pass times out, the two
    /// started and unfinished have failed once, and the one never started stays due as it was.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_pass_charges_only_the_instruments_it_started() {
        let (known, mut unchecked) = held_on(&["SLOWA", "SLOWB", "XFAST"]);
        let start = tokio::time::Instant::now();

        let sent = check_with_slow_instruments(&known, &mut unchecked).await;

        assert!(sent.is_empty(), "XFAST never started: {sent:?}");
        let pending = NoPendingFills;
        let after = start + Duration::from_secs(ORDER_CHECK_TIMEOUT_SECS);
        assert_eq!(
            unchecked.ready(&pending, after),
            [InstrumentNameExchange::new("XFAST")],
            "only the one never started is due at once"
        );
        let retry = after + Duration::from_secs(GAP_RETRY_BASE_SECS);
        assert_eq!(unchecked.ready(&pending, retry).len(), 3);
        for instrument in ["SLOWA", "SLOWB"] {
            assert_eq!(
                unchecked.failed(&InstrumentNameExchange::new(instrument), retry),
                Some(GapFailure::Retry(Duration::from_secs(
                    GAP_RETRY_BASE_SECS * 2
                ))),
                "{instrument} has failed once already"
            );
        }
        assert!(known.lock().contains(&ClientOrderId::new("XFAST")));
    }

    /// An instrument settled before the pass times out keeps its result.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_pass_keeps_what_it_finished() {
        let (known, mut unchecked) = held_on(&["AFAST", "SLOWB", "SLOWC"]);

        let sent = check_with_slow_instruments(&known, &mut unchecked).await;

        let [event] = sent.as_slice() else {
            panic!("AFAST's order: {sent:?}");
        };
        let AccountEventKind::OrderSnapshot(Snapshot(order)) = &event.kind else {
            panic!("an order snapshot: {event:?}");
        };
        assert_eq!(order.key.cid, ClientOrderId::new("AFAST"));
        assert!(!unchecked.contains(&InstrumentNameExchange::new("AFAST")));
        assert!(!known.lock().contains(&ClientOrderId::new("AFAST")));
        assert!(
            unchecked
                .ready(&NoPendingFills, tokio::time::Instant::now())
                .is_empty(),
            "SLOWB and SLOWC both started, so both wait for a retry"
        );
        known.lock().assert_consistent();
    }

    /// A batch's lookup that fails charges only its own instrument, and every lookup that was
    /// answered, on that instrument too, is kept.
    #[tokio::test]
    async fn a_failed_lookup_in_a_batch_charges_only_its_instrument() {
        let known = KnownLiveOrders::shared(ExchangeId::BinanceSpot);
        for (instrument, cid, id) in [
            ("BTCUSDT", "btc-ended", "1"),
            ("BTCUSDT", "btc-failing", "2"),
            ("ETHUSDT", "eth-ended", "3"),
        ] {
            known
                .lock()
                .live(&key(instrument, cid), dec!(1), &open(id, Decimal::ZERO));
        }
        let mut unchecked = UncheckedOrders::default();
        unchecked.open(known.lock().instruments());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let listings = AtomicUsize::new(0);

        recover_ended_orders(
            ExchangeId::BinanceSpot,
            &known,
            &mut unchecked,
            &NoPendingFills,
            &tx,
            OpenListing::Batched,
            |instruments: Vec<InstrumentNameExchange>| {
                listings.fetch_add(1, Ordering::Relaxed);
                assert_eq!(instruments.len(), 2, "one listing of both");
                async { Ok(FnvHashSet::default()) }
            },
            |key: UnindexedOrderKey| async move {
                if key.cid.0 == "btc-failing" {
                    Err(UnindexedClientError::Api(ApiError::RateLimit))
                } else {
                    Ok(OrderLookup::Ended(Box::new(cancelled_order(&key))))
                }
            },
        )
        .await;

        assert_eq!(listings.load(Ordering::Relaxed), 1);
        let mut sent: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|event| match event.kind {
                AccountEventKind::OrderSnapshot(Snapshot(order)) => order.key.cid,
                other => panic!("an order snapshot: {other:?}"),
            })
            .collect();
        sent.sort();
        assert_eq!(
            sent,
            [
                ClientOrderId::new("btc-ended"),
                ClientOrderId::new("eth-ended")
            ]
        );
        let btc = InstrumentNameExchange::new("BTCUSDT");
        assert!(!unchecked.contains(&InstrumentNameExchange::new("ETHUSDT")));
        assert!(unchecked.contains(&btc), "BTCUSDT waits for a retry");
        assert_eq!(
            unchecked.failed(&btc, tokio::time::Instant::now()),
            Some(GapFailure::Retry(Duration::from_secs(
                GAP_RETRY_BASE_SECS * 2
            ))),
            "and has failed once already"
        );
        let known = known.lock();
        assert!(known.contains(&ClientOrderId::new("btc-failing")));
        assert!(!known.contains(&ClientOrderId::new("btc-ended")));
        known.assert_consistent();
    }

    /// A batch whose listing never answers charges every instrument in it once when the pass
    /// times out.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_batch_charges_every_instrument_in_it() {
        let (known, mut unchecked) = held_on(&["AAA", "BBB"]);
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_ended_orders(
            ExchangeId::BinanceSpot,
            &known,
            &mut unchecked,
            &NoPendingFills,
            &tx,
            OpenListing::Batched,
            |_: Vec<InstrumentNameExchange>| async {
                std::future::pending::<()>().await;
                Ok(FnvHashSet::default())
            },
            |_: UnindexedOrderKey| async { Ok(OrderLookup::NotEnded) },
        )
        .await;

        assert!(rx.try_recv().is_err(), "nothing reported");
        let now = tokio::time::Instant::now();
        assert!(
            unchecked.ready(&NoPendingFills, now).is_empty(),
            "both wait"
        );
        for instrument in ["AAA", "BBB"] {
            assert_eq!(
                unchecked.failed(&InstrumentNameExchange::new(instrument), now),
                Some(GapFailure::Retry(Duration::from_secs(
                    GAP_RETRY_BASE_SECS * 2
                ))),
                "{instrument} has failed once"
            );
        }
    }
}
