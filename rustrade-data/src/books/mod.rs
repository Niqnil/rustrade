use crate::subscription::book::OrderBookEvent;
use chrono::{DateTime, Utc};
use derive_more::Display;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use tracing::debug;

/// Provides a [`OrderBookL2Manager`](manager::OrderBookL2Manager) for maintaining a set of local
/// L2 [`OrderBook`]s.
pub mod manager;

/// Provides an abstract collection of cheaply cloneable shared-state [`OrderBook`].
pub mod map;

/// Venue + local timestamps for an [`OrderBook`] revision.
///
/// Serves double duty as both the constructor argument
/// ([`OrderBook::new`]/[`OrderBook::from_sides`]) and the stored field, so
/// `update`/`snapshot` are one-line copies. The named fields prevent
/// transposition of the two same-typed `Option<DateTime<Utc>>` times.
///
/// Each timestamp carries exactly one honest meaning; staleness *policy* is left
/// to the consumer (compose over the accessors).
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default, Deserialize, Serialize)]
pub struct OrderBookTimes {
    /// Matching-engine time (`"T"` on Binance futures). `None` when the venue
    /// doesn't supply it (Binance spot, IBKR, ...). Latency/engine audit — not a
    /// liveness signal.
    pub time_engine: Option<DateTime<Utc>>,
    /// Venue event/broadcast time (`"E"` on Binance, `ts` on Bybit). `None` when
    /// the venue supplies none (IBKR; Binance spot REST seed). Feed-lag-aware
    /// liveness where present.
    pub time_exchange: Option<DateTime<Utc>>,
    /// Local ingestion wall-clock — ALWAYS present once a revision is applied.
    /// Universal liveness floor; use when `time_exchange` is `None` (e.g. IBKR).
    ///
    /// NOTE: a default/pre-population book ([`OrderBook::default`], used as the
    /// manager placeholder before the first event) carries the epoch (1970) here
    /// — desirable for a liveness floor (reads as stale → consumer fail-closes
    /// until the first revision), but it is NOT a real ingestion time.
    pub time_received: DateTime<Utc>,
}

/// Normalised Barter [`OrderBook`] snapshot.
///
/// ### Equality
/// `PartialEq`/`Eq` is derived over **all** fields, including the [`OrderBookTimes`]
/// timestamps. Because `time_received` is stamped per construction, two
/// content-identical books observed at different instants compare **unequal**. For
/// content comparison use the accessors (`sequence()`/`bids()`/`asks()`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Deserialize, Serialize)]
pub struct OrderBook {
    sequence: u64,
    times: OrderBookTimes,
    bids: OrderBookSide<Bids>,
    asks: OrderBookSide<Asks>,
}

impl OrderBook {
    /// Construct a new sorted [`OrderBook`].
    ///
    /// Note that the passed bid and asks levels do not need to be pre-sorted.
    pub fn new<IterBids, IterAsks, L>(
        sequence: u64,
        times: OrderBookTimes,
        bids: IterBids,
        asks: IterAsks,
    ) -> Self
    where
        IterBids: IntoIterator<Item = L>,
        IterAsks: IntoIterator<Item = L>,
        L: Into<Level>,
    {
        Self {
            sequence,
            times,
            bids: OrderBookSide::bids(bids),
            asks: OrderBookSide::asks(asks),
        }
    }

    /// Construct an [`OrderBook`] from pre-sorted [`OrderBookSide`]s.
    ///
    /// Use this when you already have sorted sides to avoid re-sorting overhead.
    /// Caller must ensure sides are correctly sorted (bids descending, asks ascending).
    pub fn from_sides(
        sequence: u64,
        times: OrderBookTimes,
        bids: OrderBookSide<Bids>,
        asks: OrderBookSide<Asks>,
    ) -> Self {
        debug_assert!(
            bids.levels().windows(2).all(|w| w[0].price >= w[1].price),
            "bids must be sorted descending by price"
        );
        debug_assert!(
            asks.levels().windows(2).all(|w| w[0].price <= w[1].price),
            "asks must be sorted ascending by price"
        );
        Self {
            sequence,
            times,
            bids,
            asks,
        }
    }

    /// Current `u64` sequence number associated with the [`OrderBook`].
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Matching-engine time associated with the [`OrderBook`] (`"T"` on Binance
    /// futures).
    ///
    /// `None` when the venue doesn't supply matching-engine time (Binance spot,
    /// IBKR, ...). This is for latency / engine audit — **not** a liveness signal;
    /// use [`OrderBook::time_exchange`]/[`OrderBook::time_received`] for staleness.
    pub fn time_engine(&self) -> Option<DateTime<Utc>> {
        self.times.time_engine
    }

    /// Venue event/broadcast time associated with the [`OrderBook`] (`"E"` on
    /// Binance, `ts` on Bybit).
    ///
    /// Feed-lag-aware liveness where present: `now - time_exchange` catches data
    /// that is old despite being just received. `None` when the venue supplies no
    /// broadcast timestamp (IBKR; Binance spot REST seed before the first diff).
    /// `None` is a capability signal, not a defect — fall back to
    /// [`OrderBook::time_received`].
    ///
    /// Note the asymmetry with `MarketEvent::time_exchange` (non-`Option`, with a
    /// local fallback): here `None` means "the venue gave nothing".
    pub fn time_exchange(&self) -> Option<DateTime<Utc>> {
        self.times.time_exchange
    }

    /// Local ingestion wall-clock associated with the [`OrderBook`].
    ///
    /// The **universal liveness floor** — always present once a revision is
    /// applied, on every venue (including IBKR, where it is the only liveness
    /// signal). Skew-immune: `now - time_received` is a same-clock comparison.
    /// Prefer it as the fallback when [`OrderBook::time_exchange`] is `None`.
    ///
    /// A default/pre-population book ([`OrderBook::default`]) reports the epoch
    /// (1970) here, so it reads as stale until the first revision — the intended
    /// liveness-floor behaviour.
    pub fn time_received(&self) -> DateTime<Utc> {
        self.times.time_received
    }

    /// All revision timestamps for this [`OrderBook`] as a single
    /// [`OrderBookTimes`].
    ///
    /// Convenience over the individual `time_engine`/`time_exchange`/
    /// `time_received` accessors for forwarding the whole set in one (`Copy`) move
    /// — e.g. reconstructing a book that shares this revision's times.
    pub fn times(&self) -> OrderBookTimes {
        self.times
    }

    /// Generate a sorted [`OrderBook`] snapshot with a maximum depth.
    ///
    /// The returned snapshot carries the same [`OrderBookTimes`] as the source
    /// book (timestamps are not reset).
    pub fn snapshot(&self, depth: usize) -> Self {
        Self {
            sequence: self.sequence,
            times: self.times,
            bids: OrderBookSide::bids(self.bids.levels.iter().take(depth).copied()),
            asks: OrderBookSide::asks(self.asks.levels.iter().take(depth).copied()),
        }
    }

    /// Update the local [`OrderBook`] from a new [`OrderBookEvent`].
    ///
    /// `Update` advances the book's [`OrderBookTimes`] wholesale (alongside
    /// upserting the changed levels); `Snapshot` replaces the book in its
    /// entirety, including its times.
    pub fn update(&mut self, event: &OrderBookEvent) {
        match event {
            OrderBookEvent::Snapshot(snapshot) => {
                *self = snapshot.clone();
            }
            OrderBookEvent::Update(update) => {
                self.sequence = update.sequence;
                self.times = update.times;
                self.upsert_bids(&update.bids);
                self.upsert_asks(&update.asks);
            }
        }
    }

    /// Update the local [`OrderBook`] by upserting the levels in an [`OrderBookSide`].
    fn upsert_bids(&mut self, update: &OrderBookSide<Bids>) {
        self.bids.upsert(&update.levels)
    }

    /// Update the local [`OrderBook`] by upserting the levels in an [`OrderBookSide`].
    fn upsert_asks(&mut self, update: &OrderBookSide<Asks>) {
        self.asks.upsert(&update.levels)
    }

    /// Return a reference to this [`OrderBook`]s bids.
    pub fn bids(&self) -> &OrderBookSide<Bids> {
        &self.bids
    }

    /// Return a reference to this [`OrderBook`]s asks.
    pub fn asks(&self) -> &OrderBookSide<Asks> {
        &self.asks
    }

    /// Calculate the mid-price by taking the average of the best bid and ask prices.
    ///
    /// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.bids.best(), self.asks.best()) {
            (Some(best_bid), Some(best_ask)) => Some(mid_price(best_bid.price, best_ask.price)),
            (Some(best_bid), None) => Some(best_bid.price),
            (None, Some(best_ask)) => Some(best_ask.price),
            (None, None) => None,
        }
    }

    /// Calculate the volume weighted mid-price (micro-price), weighing the best bid and ask prices
    /// with their associated amount.
    ///
    /// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
    pub fn volume_weighed_mid_price(&self) -> Option<Decimal> {
        match (self.bids.best(), self.asks.best()) {
            (Some(best_bid), Some(best_ask)) => {
                Some(volume_weighted_mid_price(*best_bid, *best_ask))
            }
            (Some(best_bid), None) => Some(best_bid.price),
            (None, Some(best_ask)) => Some(best_ask.price),
            (None, None) => None,
        }
    }
}

/// Normalised Barter [`Level`]s for one `Side` of the [`OrderBook`].
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct OrderBookSide<Side> {
    #[serde(skip_serializing)]
    pub side: Side,
    levels: Vec<Level>,
}

/// Unit type to tag an [`OrderBookSide`] as the bid Side (ie/ buyers) of an [`OrderBook`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Display)]
pub struct Bids;

/// Unit type to tag an [`OrderBookSide`] as the ask Side (ie/ sellers) of an [`OrderBook`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Display)]
pub struct Asks;

impl OrderBookSide<Bids> {
    /// Construct a new [`OrderBookSide<Bids>`] from the provided [`Level`]s.
    pub fn bids<Iter, L>(levels: Iter) -> Self
    where
        Iter: IntoIterator<Item = L>,
        L: Into<Level>,
    {
        let mut levels = levels.into_iter().map(L::into).collect::<Vec<_>>();
        levels.sort_unstable_by(|a, b| a.price.cmp(&b.price).reverse());

        Self { side: Bids, levels }
    }

    /// Upsert bid [`Level`]s into this [`OrderBookSide<Bids>`].
    pub fn upsert<L>(&mut self, levels: &[L])
    where
        L: Into<Level> + Copy,
    {
        levels.iter().for_each(|upsert| {
            let upsert: Level = (*upsert).into();
            self.upsert_single(upsert, |existing| {
                existing.price.cmp(&upsert.price).reverse()
            })
        })
    }
}

impl OrderBookSide<Asks> {
    /// Construct a new [`OrderBookSide<Asks>`] from the provided [`Level`]s.
    pub fn asks<Iter, L>(levels: Iter) -> Self
    where
        Iter: IntoIterator<Item = L>,
        L: Into<Level>,
    {
        let mut levels = levels.into_iter().map(L::into).collect::<Vec<_>>();
        levels.sort_unstable_by_key(|a| a.price);

        Self { side: Asks, levels }
    }

    /// Upsert ask [`Level`]s into this [`OrderBookSide<Asks>`].
    pub fn upsert<L>(&mut self, levels: &[L])
    where
        L: Into<Level> + Copy,
    {
        levels.iter().for_each(|upsert| {
            let upsert = (*upsert).into();
            self.upsert_single(upsert, |existing| existing.price.cmp(&upsert.price))
        })
    }
}

impl<Side> OrderBookSide<Side>
where
    Side: std::fmt::Display + std::fmt::Debug,
{
    /// Get best [`Level`] on the [`OrderBookSide`].
    pub fn best(&self) -> Option<&Level> {
        self.levels.first()
    }

    /// Return a reference to the [`OrderBookSide`] levels.
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }

    /// Upsert a single [`Level`] into this [`OrderBookSide`].
    ///
    /// ### Upsert Scenarios
    /// #### 1 Level Already Exists
    /// 1a) New value is 0, remove the level
    /// 1b) New value is > 0, replace the level
    ///
    /// #### 2 Level Does Not Exist
    /// 2a) New value is 0, log warn and continue
    /// 2b) New value is > 0, insert new level
    pub fn upsert_single<FnOrd>(&mut self, new_level: Level, fn_ord: FnOrd)
    where
        FnOrd: Fn(&Level) -> Ordering,
    {
        match (self.levels.binary_search_by(fn_ord), new_level.amount) {
            (Ok(index), new_amount) => {
                if new_amount.is_zero() {
                    // Scenario 1a: Level exists & new value is 0 => remove level
                    let _removed = self.levels.remove(index);
                } else {
                    // Scenario 1b: Level exists & new value is > 0 => replace level
                    self.levels[index].amount = new_amount;
                }
            }
            (Err(index), new_amount) => {
                if new_amount.is_zero() {
                    // Scenario 2a: Level does not exist & new value is 0 => log & continue
                    debug!(
                        ?new_level,
                        side = %self.side,
                        "received upsert Level with zero amount (to remove) that was not found"
                    );
                } else {
                    // Scenario 2b: Level does not exist & new value > 0 => insert new level
                    self.levels.insert(index, new_level);
                }
            }
        }
    }
}

impl Default for OrderBookSide<Bids> {
    fn default() -> Self {
        Self {
            side: Bids,
            levels: vec![],
        }
    }
}

impl Default for OrderBookSide<Asks> {
    fn default() -> Self {
        Self {
            side: Asks,
            levels: vec![],
        }
    }
}

/// Normalised Barter OrderBook [`Level`].
#[derive(Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize)]
pub struct Level {
    pub price: Decimal,
    pub amount: Decimal,
}

impl<T> From<(T, T)> for Level
where
    T: Into<Decimal>,
{
    fn from((price, amount): (T, T)) -> Self {
        Self::new(price, amount)
    }
}

impl Eq for Level {}

impl Level {
    pub fn new<T>(price: T, amount: T) -> Self
    where
        T: Into<Decimal>,
    {
        Self {
            price: price.into(),
            amount: amount.into(),
        }
    }
}

/// Calculate the mid-price by taking the average of the best bid and ask prices.
///
/// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
pub fn mid_price(best_bid_price: Decimal, best_ask_price: Decimal) -> Decimal {
    (best_bid_price + best_ask_price) / Decimal::TWO
}

/// Calculate the volume weighted mid-price (micro-price), weighing the best bid and ask prices
/// with their associated amount.
///
/// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
pub fn volume_weighted_mid_price(best_bid: Level, best_ask: Level) -> Decimal {
    ((best_bid.price * best_ask.amount) + (best_ask.price * best_bid.amount))
        / (best_bid.amount + best_ask.amount)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod order_book_l1 {
        use super::*;
        use crate::subscription::book::OrderBookL1;
        use rust_decimal_macros::dec;

        #[test]
        fn test_mid_price() {
            struct TestCase {
                input: OrderBookL1,
                expected: Option<Decimal>,
            }

            let tests = vec![
                TestCase {
                    // TC0
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(100, 999999)),
                        best_ask: Some(Level::new(200, 1)),
                    },
                    expected: Some(dec!(150.0)),
                },
                TestCase {
                    // TC1
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(50, 1)),
                        best_ask: Some(Level::new(250, 999999)),
                    },
                    expected: Some(dec!(150.0)),
                },
                TestCase {
                    // TC2
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(10, 999999)),
                        best_ask: Some(Level::new(250, 999999)),
                    },
                    expected: Some(dec!(130.0)),
                },
                TestCase {
                    // TC3
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(10, 999999)),
                        best_ask: None,
                    },
                    expected: None,
                },
                TestCase {
                    // TC4
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: None,
                        best_ask: Some(Level::new(250, 999999)),
                    },
                    expected: None,
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(test.input.mid_price(), test.expected, "TC{index} failed")
            }
        }

        #[test]
        fn test_volume_weighted_mid_price() {
            struct TestCase {
                input: OrderBookL1,
                expected: Option<Decimal>,
            }

            let tests = vec![
                TestCase {
                    // TC0: volume the same so should be equal to non-weighted mid price
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(100, 100)),
                        best_ask: Some(Level::new(200, 100)),
                    },
                    expected: Some(dec!(150.0)),
                },
                TestCase {
                    // TC1: volume affects mid-price
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(100, 600)),
                        best_ask: Some(Level::new(200, 1000)),
                    },
                    expected: Some(dec!(137.5)),
                },
                TestCase {
                    // TC2: volume the same and price the same
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(1000, 999999)),
                        best_ask: Some(Level::new(1000, 999999)),
                    },
                    expected: Some(dec!(1000.0)),
                },
                TestCase {
                    // TC3: best ask is None
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: Some(Level::new(1000, 999999)),
                        best_ask: None,
                    },
                    expected: None,
                },
                TestCase {
                    // TC4: best bid is None
                    input: OrderBookL1 {
                        last_update_time: Default::default(),
                        best_bid: None,
                        best_ask: Some(Level::new(1000, 999999)),
                    },
                    expected: None,
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(
                    test.input.volume_weighed_mid_price(),
                    test.expected,
                    "TC{index} failed"
                )
            }
        }
    }

    mod order_book {
        use super::*;
        use rust_decimal_macros::dec;

        fn times(received_ms: i64) -> OrderBookTimes {
            OrderBookTimes {
                time_engine: Some(DateTime::from_timestamp_millis(1).unwrap()),
                time_exchange: Some(DateTime::from_timestamp_millis(2).unwrap()),
                time_received: DateTime::from_timestamp_millis(received_ms).unwrap(),
            }
        }

        #[test]
        fn test_update_replaces_times_wholesale() {
            let mut book = OrderBook::new(1, times(100), vec![Level::new(50, 1)], vec![]);

            let newer = OrderBookTimes {
                time_engine: None,
                time_exchange: Some(DateTime::from_timestamp_millis(999).unwrap()),
                time_received: DateTime::from_timestamp_millis(1000).unwrap(),
            };
            let update = OrderBook::new(2, newer, vec![Level::new(50, 1)], vec![]);
            book.update(&OrderBookEvent::Update(update));

            assert_eq!(book.time_engine(), newer.time_engine);
            assert_eq!(book.time_exchange(), newer.time_exchange);
            assert_eq!(book.time_received(), newer.time_received);
        }

        #[test]
        fn test_update_snapshot_propagates_times_wholesale() {
            // The `Snapshot` arm replaces the live book via `*self = snapshot.clone()`,
            // so it must carry the snapshot's `times` (not retain the old ones).
            let mut book = OrderBook::new(1, times(100), vec![Level::new(50, 1)], vec![]);

            let newer = OrderBookTimes {
                time_engine: None,
                time_exchange: Some(DateTime::from_timestamp_millis(999).unwrap()),
                time_received: DateTime::from_timestamp_millis(1000).unwrap(),
            };
            let snapshot = OrderBook::new(2, newer, vec![Level::new(60, 1)], vec![]);
            book.update(&OrderBookEvent::Snapshot(snapshot));

            assert_eq!(book.time_engine(), newer.time_engine);
            assert_eq!(book.time_exchange(), newer.time_exchange);
            assert_eq!(book.time_received(), newer.time_received);
        }

        #[test]
        fn test_snapshot_copies_times() {
            let book = OrderBook::new(1, times(100), vec![Level::new(50, 1)], vec![]);
            let snap = book.snapshot(10);
            assert_eq!(snap.time_engine(), book.time_engine());
            assert_eq!(snap.time_exchange(), book.time_exchange());
            assert_eq!(snap.time_received(), book.time_received());
        }

        #[test]
        fn test_equality_reflects_timestamps() {
            // Two content-identical books with different `time_received` are unequal.
            let a = OrderBook::new(1, times(100), vec![Level::new(50, 1)], vec![]);
            let b = OrderBook::new(1, times(200), vec![Level::new(50, 1)], vec![]);
            assert_ne!(a, b);
        }

        #[test]
        fn test_mid_price() {
            struct TestCase {
                input: OrderBook,
                expected: Option<Decimal>,
            }

            let tests = vec![
                TestCase {
                    // TC0: no levels so 0.0 mid-price
                    input: OrderBook::new::<Vec<_>, Vec<_>, Level>(
                        0,
                        Default::default(),
                        vec![],
                        vec![],
                    ),
                    expected: None,
                },
                TestCase {
                    // TC1: no asks in the books so take best bid price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![
                            Level::new(dec!(100.0), dec!(100.0)),
                            Level::new(dec!(50.0), dec!(100.0)),
                        ],
                        vec![],
                    ),
                    expected: Some(dec!(100.0)),
                },
                TestCase {
                    // TC2: no bids in the books so take ask price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![],
                        vec![
                            Level::new(dec!(50.0), dec!(100.0)),
                            Level::new(dec!(100.0), dec!(100.0)),
                        ],
                    ),
                    expected: Some(dec!(50.0)),
                },
                TestCase {
                    // TC3: best bid and ask amount is the same, so regular mid-price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![
                            Level::new(dec!(100.0), dec!(100.0)),
                            Level::new(dec!(50.0), dec!(100.0)),
                        ],
                        vec![
                            Level::new(dec!(200.0), dec!(100.0)),
                            Level::new(dec!(300.0), dec!(100.0)),
                        ],
                    ),
                    expected: Some(dec!(150.0)),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(test.input.mid_price(), test.expected, "TC{index} failed")
            }
        }

        #[test]
        fn test_volume_weighted_mid_price() {
            struct TestCase {
                input: OrderBook,
                expected: Option<Decimal>,
            }

            let tests = vec![
                TestCase {
                    // TC0: no levels so 0.0 mid-price
                    input: OrderBook::new::<Vec<_>, Vec<_>, Level>(
                        0,
                        Default::default(),
                        vec![],
                        vec![],
                    ),
                    expected: None,
                },
                TestCase {
                    // TC1: no asks in the books so take best bid price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![
                            Level::new(dec!(100.0), dec!(100.0)),
                            Level::new(dec!(50.0), dec!(100.0)),
                        ],
                        vec![],
                    ),
                    expected: Some(dec!(100.0)),
                },
                TestCase {
                    // TC2: no bids in the books so take ask price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![],
                        vec![
                            Level::new(dec!(50.0), dec!(100.0)),
                            Level::new(dec!(100.0), dec!(100.0)),
                        ],
                    ),
                    expected: Some(dec!(50.0)),
                },
                TestCase {
                    // TC3: best bid and ask amount is the same, so regular mid-price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![
                            Level::new(dec!(100.0), dec!(100.0)),
                            Level::new(dec!(50.0), dec!(100.0)),
                        ],
                        vec![
                            Level::new(dec!(200.0), dec!(100.0)),
                            Level::new(dec!(300.0), dec!(100.0)),
                        ],
                    ),
                    expected: Some(dec!(150.0)),
                },
                TestCase {
                    // TC4: valid volume weighted mid-price
                    input: OrderBook::new(
                        0,
                        Default::default(),
                        vec![
                            Level::new(dec!(100.0), dec!(3000.0)),
                            Level::new(dec!(50.0), dec!(100.0)),
                        ],
                        vec![
                            Level::new(dec!(200.0), dec!(1000.0)),
                            Level::new(dec!(300.0), dec!(100.0)),
                        ],
                    ),
                    expected: Some(dec!(175.0)),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(
                    test.input.volume_weighed_mid_price(),
                    test.expected,
                    "TC{index} failed"
                )
            }
        }
    }

    mod order_book_side {
        use super::*;
        use rust_decimal_macros::dec;

        #[test]
        fn test_upsert_single() {
            struct TestCase {
                book_side: OrderBookSide<Bids>,
                new_level: Level,
                expected: OrderBookSide<Bids>,
            }

            let tests = vec![
                TestCase {
                    // TC0: Level exists & new value is 0 => remove Level
                    book_side: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                    ]),
                    new_level: Level::new(dec!(100), dec!(0)),
                    expected: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                    ]),
                },
                TestCase {
                    // TC1: Level exists & new value is > 0 => replace Level
                    book_side: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                    ]),
                    new_level: Level::new(dec!(100), dec!(10)),
                    expected: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(10)),
                    ]),
                },
                TestCase {
                    // TC2: Level does not exist & new value > 0 => insert new Level
                    book_side: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                    ]),
                    new_level: Level::new(dec!(110), dec!(1)),
                    expected: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                        Level::new(dec!(110), dec!(1)),
                    ]),
                },
                TestCase {
                    // TC3: Level does not exist & new value is 0 => no change
                    book_side: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                    ]),
                    new_level: Level::new(dec!(110), dec!(0)),
                    expected: OrderBookSide::bids(vec![
                        Level::new(dec!(80), dec!(1)),
                        Level::new(dec!(90), dec!(1)),
                        Level::new(dec!(100), dec!(1)),
                    ]),
                },
            ];

            for (index, mut test) in tests.into_iter().enumerate() {
                test.book_side.upsert_single(test.new_level, |existing| {
                    existing.price.cmp(&test.new_level.price).reverse()
                });
                assert_eq!(test.book_side, test.expected, "TC{} failed", index);
            }
        }
    }
}
