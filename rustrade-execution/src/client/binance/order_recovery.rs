//! Learning how orders ended while a Binance account stream was disconnected.
//!
//! A reconnect recovers the fills it missed by time, but an order cancelled, expired or rejected in
//! the meantime cannot be found that way: Binance filters its order history on when an order was
//! created, so a window opening at the disconnect misses an order placed before it. So the client
//! keeps the orders it knows to be live ([`KnownLiveOrders`]), and after a reconnect lists each
//! instrument's open orders and asks about each held order no longer listed, by client order id
//! ([`recover_ended_orders`]). [`UncheckedOrders`] holds which instruments still need that check,
//! and when a failed one is retried. [`fetch_ended_by_key`] answers
//! [`OrderStatusClient`](crate::client::OrderStatusClient) with the same lookup.
//!
//! Nothing here names a Binance endpoint: each client passes in its own listing and lookup.

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

/// How many order lookups run at once: in [`fetch_ended_by_key`], and per instrument in
/// [`recover_ended_orders`].
pub(crate) const ORDER_LOOKUPS_IN_FLIGHT: usize = 8;

/// How many instruments [`recover_ended_orders`] checks at once. With
/// [`ORDER_LOOKUPS_IN_FLIGHT`] lookups each, at most 16 requests are in flight.
const INSTRUMENT_CHECKS_IN_FLIGHT: usize = 2;

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
    orders: FnvHashMap<ClientOrderId, KnownLive>,
    by_order_id: FnvHashMap<(InstrumentNameExchange, OrderId), ClientOrderId>,
    /// Every held order by when it was added, oldest first.
    by_age: BTreeMap<u64, ClientOrderId>,
    recently_ended: LruCache<ClientOrderId, ()>,
    next_seq: u64,
    /// Whether the set is forgetting orders, so that it warns once per episode, not per order.
    forgetting: bool,
}

impl Default for KnownLiveOrders {
    fn default() -> Self {
        Self {
            orders: FnvHashMap::default(),
            by_order_id: FnvHashMap::default(),
            by_age: BTreeMap::new(),
            recently_ended: LruCache::new(RECENTLY_ENDED),
            next_seq: 0,
            forgetting: false,
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
            } else if self.orders.len() <= MAX_KNOWN_LIVE_ORDERS * 3 / 4 {
                self.forgetting = false;
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            self.by_age.insert(seq, cid.clone());
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

    /// Those of `instruments` with an order held as live.
    pub(crate) fn instruments_among(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Vec<InstrumentNameExchange> {
        let held: FnvHashSet<&InstrumentNameExchange> = self
            .orders
            .values()
            .map(|known| &known.instrument)
            .collect();
        instruments
            .iter()
            .filter(|instrument| held.contains(instrument))
            .cloned()
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

    fn remove(&mut self, cid: &ClientOrderId) -> bool {
        let Some(known) = self.orders.remove(cid) else {
            return false;
        };
        self.by_age.remove(&known.seq);
        if let Some(order_id) = known.order_id {
            self.by_order_id.remove(&(known.instrument, order_id));
        }
        true
    }

    fn forget_oldest(&mut self) {
        let Some((_, cid)) = self.by_age.pop_first() else {
            return;
        };
        if self.forgetting {
            debug!(%cid, "Binance forgets the oldest order it holds as live");
        } else {
            self.forgetting = true;
            warn!(
                %cid,
                held = MAX_KNOWN_LIVE_ORDERS,
                "Binance holds the most orders as live it can, and forgets the oldest as more \
                 arrive: if one ended while the stream was disconnected, a reconnect will not \
                 report it. Further orders forgotten are logged at debug"
            );
        }
        self.remove(&cid);
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
/// ([`open`](Self::open)). An instrument is checked only once fill recovery has no gap left on it,
/// so the fills of an order reach the stream before how it ended ([`ready`](Self::ready)). A check
/// that fails is retried after the same backoff as a fill gap, until it has failed
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

    /// The instruments fill recovery has no gap left on whose check is due at `now`.
    pub(crate) fn ready(
        &self,
        unrecovered: &UnrecoveredFills,
        now: tokio::time::Instant,
    ) -> Vec<InstrumentNameExchange> {
        self.0
            .iter()
            .filter(|(instrument, check)| {
                !unrecovered.covers(instrument) && check.retry_at.is_none_or(|at| at <= now)
            })
            .map(|(instrument, _)| instrument.clone())
            .collect()
    }

    /// When an instrument fill recovery has no gap left on is next due, or `None` when there is
    /// none. One still on a gap is due only once the gap is settled, which fill recovery's own
    /// schedule wakes for, so it never makes this a time already past that [`ready`](Self::ready)
    /// would answer with nothing.
    pub(crate) fn next_due(&self, unrecovered: &UnrecoveredFills) -> Option<tokio::time::Instant> {
        self.0
            .iter()
            .filter(|(instrument, _)| !unrecovered.covers(instrument))
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

/// Report how each order held as live ended, where it has, on every instrument due for a check:
/// the order lifecycle events missed while the stream was disconnected.
///
/// Checks only an instrument fill recovery has no gap left on (see [`UncheckedOrders`]), so an
/// order's recovered fills reach the stream before how it ended. For each, it lists the open
/// orders with `list_open`, which returns the client order ids listed, and looks up with `lookup`
/// each order held as live that the listing no longer shows: one request per instrument plus one
/// per order that ended, rather than one per order held. The orders held are read before the
/// listings, so one placed after a listing cannot be taken for one that ended.
///
/// Each order that has ended is sent as an [`AccountEventKind::OrderSnapshot`] of its inactive
/// state, under [`StrategyId::unknown`], and leaves the set. One the stream reported ending in the
/// meantime has left it already, and is not sent twice. One the venue does not know leaves the set
/// with a warning, since asking again cannot change that. One still live, or in a state this
/// version cannot read, stays.
///
/// Each instrument is settled as soon as its check ends, so dropping this part-way, as a
/// disconnect during a retry does, loses nothing, and a pass that outlasts
/// [`ORDER_CHECK_TIMEOUT_SECS`] keeps what it finished. An instrument whose check fails or does not
/// finish is retried later; one never started stays due as it was.
pub(crate) async fn recover_ended_orders<L, LFut, Q, QFut>(
    exchange: ExchangeId,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    unrecovered: &UnrecoveredFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    list_open: L,
    lookup: Q,
) where
    L: Fn(InstrumentNameExchange) -> LFut,
    LFut: Future<Output = Result<FnvHashSet<ClientOrderId>, UnindexedClientError>>,
    Q: Fn(UnindexedOrderKey) -> QFut,
    QFut: Future<Output = Result<OrderLookup, UnindexedClientError>>,
{
    use futures::{StreamExt as _, TryStreamExt as _};

    let ready = unchecked.ready(unrecovered, tokio::time::Instant::now());
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
        "Binance checking how the orders held as live ended while disconnected"
    );

    // Which instruments have been settled, and how many have started: the stream starts them in
    // order, so those from `started` on were never read. The stream owns what it reads, so no
    // future it builds borrows from a closure argument, which `tokio::spawn` cannot prove `Send`.
    let instruments: Vec<InstrumentNameExchange> = held
        .iter()
        .map(|(instrument, _)| instrument.clone())
        .collect();
    let mut settled = vec![false; held.len()];
    let started = AtomicUsize::new(0);
    let (list_open, lookup) = (&list_open, &lookup);
    let recovery = async {
        let mut reported = 0u32;
        let mut checks = futures::stream::iter(held.into_iter().enumerate().map(
            |(index, (instrument, keys))| {
                started.store(index + 1, Ordering::Relaxed);
                async move {
                    let found = async {
                        let listed = list_open(instrument).await?;
                        let unlisted: Vec<UnindexedOrderKey> = keys
                            .into_iter()
                            .filter(|key| !listed.contains(&key.cid))
                            .collect();
                        futures::stream::iter(unlisted)
                            .map(|key| async move {
                                let found = lookup(key.clone()).await?;
                                Ok::<_, UnindexedClientError>((key, found))
                            })
                            .buffered(ORDER_LOOKUPS_IN_FLIGHT)
                            .try_collect::<Vec<_>>()
                            .await
                    };
                    (index, found.await)
                }
            },
        ))
        .buffer_unordered(INSTRUMENT_CHECKS_IN_FLIGHT);
        while let Some((index, found)) = checks.next().await {
            settled[index] = true;
            let instrument = &instruments[index];
            match found {
                Ok(found) => {
                    let Some(sent) = settle(exchange, known, found, tx) else {
                        debug!(%exchange, "Binance order check: consumer dropped during recovery");
                        return;
                    };
                    reported += sent;
                    unchecked.checked(instrument);
                }
                Err(e) => order_check_failed(exchange, unchecked, instrument, &e.to_string()),
            }
        }
        info!(%exchange, reported, "Binance order check complete");
    };
    if tokio::time::timeout(Duration::from_secs(ORDER_CHECK_TIMEOUT_SECS), recovery)
        .await
        .is_err()
    {
        let started = started.load(Ordering::Relaxed);
        for (instrument, _) in instruments[..started]
            .iter()
            .zip(&settled)
            .filter(|(_, settled)| !**settled)
        {
            order_check_failed(exchange, unchecked, instrument, "order check timed out");
        }
    }
}

/// Apply one instrument's lookups to the set and send each order that ended. Returns how many were
/// sent, or `None` if the consumer has gone.
///
/// The set's lock is held across the sends, as the stream holds it across its own, so an order the
/// stream reports ending and this check reach the consumer in the order they were decided, and an
/// order is reported once.
fn settle(
    exchange: ExchangeId,
    known: &SharedKnownLiveOrders,
    found: Vec<(UnindexedOrderKey, OrderLookup)>,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) -> Option<u32> {
    let mut known = known.lock();
    let mut sent = 0;
    for (key, found) in found {
        match found {
            OrderLookup::Ended(order) => {
                if !known.ended(&order.key.cid) {
                    continue;
                }
                let event = UnindexedAccountEvent::new(
                    exchange,
                    AccountEventKind::OrderSnapshot(Snapshot::new(
                        (*order).map_state(OrderState::Inactive),
                    )),
                );
                tx.send(event).ok()?;
                sent += 1;
            }
            OrderLookup::Unknown => {
                if known.ended(&key.cid) {
                    warn!(
                        %exchange,
                        instrument = %key.instrument,
                        cid = %key.cid,
                        "Binance does not know an order held as live; a reconnect no longer asks \
                         about it"
                    );
                }
            }
            OrderLookup::NotEnded => {}
        }
    }
    Some(sent)
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
            "Binance could not check how the orders held as live ended, retrying later"
        ),
        Some(GapFailure::GivenUp) => error!(
            %exchange,
            %instrument,
            retries = MAX_GAP_RETRIES,
            reason,
            "Binance gave up checking how the orders held as live ended: an order that ended \
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

        assert!(
            known
                .instruments_among(&[InstrumentNameExchange::new("BTCUSDT")])
                .is_empty(),
            "every order ended"
        );
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
        unchecked.checked(&eth);
        assert!(unchecked.contains(&btc), "BTCUSDT still waits");
        assert_eq!(
            unchecked.next_due(&unrecovered),
            None,
            "on its gap, not on a timer"
        );

        let unrecovered = UnrecoveredFills::default();
        assert_eq!(
            unchecked.ready(&unrecovered, tokio::time::Instant::now()),
            std::slice::from_ref(&btc)
        );
        assert!(unchecked.next_due(&unrecovered).is_some(), "due now");
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
        let mut unrecovered = UnrecoveredFills::default();
        let now = Utc::now();
        unrecovered.open(
            std::slice::from_ref(&btc),
            now - chrono::Duration::minutes(1),
            now,
        );
        let later = start + Duration::from_secs(GAP_RETRY_BASE_SECS * 2);

        assert!(unchecked.ready(&unrecovered, later).is_empty());
        assert_eq!(unchecked.next_due(&unrecovered), None);
        assert_eq!(
            unchecked.ready(&UnrecoveredFills::default(), later),
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
        let unrecovered = UnrecoveredFills::default();

        let base = Duration::from_secs(GAP_RETRY_BASE_SECS);
        assert_eq!(
            unchecked.ready(&unrecovered, now + base),
            std::slice::from_ref(&eth)
        );
        assert_eq!(unchecked.next_due(&unrecovered), Some(now + base));
        unchecked.open([btc.clone()]);
        assert!(
            unchecked.ready(&unrecovered, now + base).len() == 1,
            "a reconnect does not bring BTCUSDT's retry forward"
        );
        assert_eq!(unchecked.ready(&unrecovered, now + base * 2).len(), 2);
    }

    #[test]
    fn a_failed_check_backs_off_and_is_given_up_after_its_retries() {
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let unrecovered = UnrecoveredFills::default();
        let now = tokio::time::Instant::now();

        for retry in 0..MAX_GAP_RETRIES {
            let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << retry);
            assert_eq!(unchecked.failed(&btc, now), Some(GapFailure::Retry(delay)));
            assert_eq!(unchecked.next_due(&unrecovered), Some(now + delay));
            assert!(unchecked.ready(&unrecovered, now).is_empty(), "waits");
            assert_eq!(unchecked.ready(&unrecovered, now + delay).len(), 1);
        }
        assert_eq!(unchecked.failed(&btc, now), Some(GapFailure::GivenUp));
        assert!(unchecked.is_empty());
        assert_eq!(unchecked.next_due(&unrecovered), None);
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
}
