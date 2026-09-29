use crate::{
    error::OrderError,
    order::id::{OrderId, VenueOrderId},
};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};

/// Convenient type alias for an [`OrderState`] keyed with [`AssetNameExchange`]
/// and [`InstrumentNameExchange`].
pub type UnindexedOrderState = OrderState<AssetNameExchange, InstrumentNameExchange>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum OrderState<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    Active(ActiveOrderState),
    Inactive(InactiveOrderState<AssetKey, InstrumentKey>),
}

impl<AssetKey, InstrumentKey> OrderState<AssetKey, InstrumentKey> {
    pub fn active<S>(state: S) -> Self
    where
        S: Into<ActiveOrderState>,
    {
        OrderState::Active(state.into())
    }

    pub fn inactive<S>(state: S) -> Self
    where
        S: Into<InactiveOrderState<AssetKey, InstrumentKey>>,
    {
        OrderState::Inactive(state.into())
    }

    pub fn fully_filled(filled: Filled) -> Self {
        Self::Inactive(InactiveOrderState::FullyFilled(filled))
    }

    pub fn expired(expired: Expired) -> Self {
        Self::Inactive(InactiveOrderState::Expired(expired))
    }

    pub fn time_exchange(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Active(active) => match active {
                ActiveOrderState::OpenInFlight(_) => None,
                ActiveOrderState::Open(state) => Some(state.time_exchange),
                ActiveOrderState::CancelInFlight(state) => {
                    state.order.as_ref().map(|order| order.time_exchange)
                }
            },
            Self::Inactive(inactive) => match inactive {
                InactiveOrderState::Cancelled(state) => Some(state.time_exchange),
                InactiveOrderState::FullyFilled(state) => Some(state.time_exchange),
                InactiveOrderState::Expired(state) => Some(state.time_exchange),
                InactiveOrderState::OpenFailed(_) => None,
            },
        }
    }

    /// Returns `true` if the order was not rejected at placement.
    ///
    /// Returns `true` for all states except `Inactive(OpenFailed(_))`:
    /// - `Active(_)` — order is working on the exchange
    /// - `Inactive(FullyFilled(_))` — order completed successfully
    /// - `Inactive(Cancelled(_))` — order was accepted then cancelled
    /// - `Inactive(Expired(_))` — order was accepted then expired
    ///
    /// This is the opposite of [`is_failed()`](Self::is_failed).
    pub fn is_accepted(&self) -> bool {
        !self.is_failed()
    }

    /// Returns `true` if the order failed to open.
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Inactive(InactiveOrderState::OpenFailed(_)))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum ActiveOrderState {
    OpenInFlight(OpenInFlight),
    Open(Open),
    CancelInFlight(CancelInFlight),
}

impl ActiveOrderState {
    pub fn open_meta(&self) -> Option<&Open> {
        match self {
            Self::OpenInFlight(_) => None,
            Self::Open(open) => Some(open),
            Self::CancelInFlight(cancel) => cancel.order.as_ref(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OpenInFlight;

/// An order the exchange reports as working, in the state the exchange last reported it.
///
/// Successive `Open` values for one order describe that order over time -- an acknowledgement,
/// then each partial fill -- so consumers keep the later of two and discard the other. Use
/// [`Open::is_superseded_by`] to decide which is which; it does not rest on
/// [`Open::time_exchange`] alone, because not every venue supplies a usable one. Producers must
/// still stamp that field correctly where they can; see it for the obligation.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Open {
    /// How the venue addresses this order.
    ///
    /// [`VenueOrderId::ClientAssigned`] when the venue acknowledged the order without assigning an
    /// identifier of its own. A consumer deciding whether two `Open` values describe the same
    /// venue order must go through [`VenueOrderId::assigned`] rather than comparing this field
    /// directly, because two `ClientAssigned` values are equal without being evidence of anything.
    pub id: VenueOrderId,
    /// When the exchange last reported this state -- **not** when the order was created.
    ///
    /// ## Producer obligation
    ///
    /// A client building this from a venue response must use the venue's last-update field
    /// (Binance `updateTime`, Alpaca `updated_at`) rather than its creation field. Creation time
    /// is identical across every snapshot of one order, so a snapshot stamped with it cannot be
    /// ordered against the states that followed it and is discarded as stale by any consumer
    /// applying the rule above -- silently, and precisely for the partially-filled orders a
    /// reconciliation fetch exists to repair.
    pub time_exchange: DateTime<Utc>,
    /// Cumulative quantity filled across every execution against this order, as the exchange
    /// reports it -- not the size of the most recent execution.
    pub filled_quantity: Decimal,
}

impl Open {
    pub fn quantity_remaining(&self, initial_quantity: Decimal) -> Decimal {
        initial_quantity - self.filled_quantity
    }

    /// Whether `update` describes a later state of this order than `self` does, and so should
    /// replace it.
    ///
    /// Ordering rests on two properties of a single venue order:
    ///
    /// 1. **[`Open::time_exchange`]** is non-decreasing, where the venue supplies a usable one.
    /// 2. **[`Open::filled_quantity`]** is non-decreasing *always* -- cumulative fill is
    ///    append-only, because an execution against an order cannot be un-executed.
    ///
    /// The second is the stronger property, and it is used in both directions. An update
    /// reporting strictly **less** filled is out of sequence whatever its stamp says, so it is
    /// refused outright. Otherwise an update is taken as later when it is stamped no earlier
    /// **or** when it reports strictly more filled.
    ///
    /// Each direction carries a different class of venue:
    ///
    /// - Refusing a lower cumulative is what orders a venue whose stamps are *locally* applied.
    ///   IBKR's `orderStatus` callback carries no timestamp field at all, so its client stamps
    ///   `Utc::now()` on receipt; those rise in arrival order, which makes the stamp admit every
    ///   snapshot and degrades ordering to last-writer-wins. The fill invariant still rejects a
    ///   snapshot that overtook a newer one in flight.
    /// - Admitting a higher cumulative is what rescues a venue whose stamps are *too old* -- a
    ///   reconciliation fetch pinned to the order's creation time, whose snapshot would otherwise
    ///   be discarded along with the only accurate cumulative it carried.
    ///
    /// # Caller obligation
    ///
    /// Both values must already be known to describe the *same* venue order. This orders states,
    /// it does not establish identity -- check that first with [`VenueOrderId::contradicts`], or
    /// a snapshot belonging to a different order will be ordered against this one as though it
    /// were a later state of it.
    ///
    /// # Known limitations
    ///
    /// Two snapshots reporting the same cumulative fill are ordered on the stamp alone, so a
    /// venue that supplies no usable one is unordered across them. That covers an order's whole
    /// life before its first execution, which is why the producer obligation on
    /// [`Open::time_exchange`] still stands.
    ///
    /// A venue that *reduces* a reported cumulative -- busting or correcting an execution -- has
    /// its correction refused, since a decrease is indistinguishable from out-of-sequence
    /// delivery. The order is left on the higher cumulative until a later snapshot moves it.
    ///
    /// A caller replacing `self` with `update` wholesale adopts `update`'s `time_exchange` too,
    /// which may be the earlier of the two. That is deliberate: an `Open` is one state the venue
    /// reported, and carrying the newer stamp onto the other's quantities would synthesise a
    /// state the venue never reported.
    pub fn is_superseded_by(&self, update: &Self) -> bool {
        if update.filled_quantity < self.filled_quantity {
            return false;
        }

        self.time_exchange <= update.time_exchange || update.filled_quantity > self.filled_quantity
    }
}

/// Metadata for a fully filled order.
///
/// Unlike [`Open`], this represents an order that has completed execution
/// and is no longer active on the exchange.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Filled {
    pub id: OrderId,
    pub time_exchange: DateTime<Utc>,
    pub filled_quantity: Decimal,
    /// Volume-weighted average execution price across all fills.
    ///
    /// `Some` when the exchange provides it in the response, `None` otherwise.
    /// When `None`, downstream consumers should compute from individual fill events.
    pub avg_price: Option<Decimal>,
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct CancelInFlight {
    pub order: Option<Open>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum InactiveOrderState<AssetKey, InstrumentKey> {
    Cancelled(Cancelled),
    FullyFilled(Filled),
    OpenFailed(OrderError<AssetKey, InstrumentKey>),
    Expired(Expired),
}

/// Metadata for a cancelled order.
///
/// Includes `filled_quantity` to handle IOC (Immediate-Or-Cancel) orders
/// that partially fill before the remainder is cancelled.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Cancelled {
    pub id: OrderId,
    /// Cancellation timestamp.
    ///
    /// Normally the venue-reported cancel time. Some venues omit a timestamp on their cancel
    /// response (e.g. Binance margin cancels carry no `transactTime`); for those the client falls
    /// back to the local receive time, which can differ from the true venue cancel time by network
    /// latency. Consumers building fill ledgers or P&L should not assume sub-second venue accuracy.
    pub time_exchange: DateTime<Utc>,
    /// Quantity filled before the order was cancelled.
    ///
    /// Zero for orders cancelled with no fills (e.g., GTC limit order cancelled by user).
    /// Non-zero for IOC orders that partially filled before cancellation.
    pub filled_quantity: Decimal,
}

/// Metadata for an expired order.
///
/// Includes `filled_quantity` to handle GTD (Good-Till-Date) orders
/// that partially fill before expiration.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Expired {
    pub id: OrderId,
    pub time_exchange: DateTime<Utc>,
    /// Quantity filled before the order expired.
    pub filled_quantity: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use rust_decimal_macros::dec;

    /// An `Open` for one venue order, `secs` after an arbitrary epoch, reporting `filled`.
    fn open(secs: i64, filled: Decimal) -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId::new("1")),
            time_exchange: DateTime::<Utc>::MIN_UTC + TimeDelta::seconds(secs),
            filled_quantity: filled,
        }
    }

    #[test]
    fn a_later_stamp_supersedes() {
        assert!(open(0, dec!(0)).is_superseded_by(&open(1, dec!(0))));
    }

    /// Deliberately `<=`. Venues report at a coarser resolution than they act, so two states of
    /// one order routinely share a stamp; the later-arriving one is taken as the later state.
    #[test]
    fn an_equal_stamp_supersedes() {
        assert!(open(1, dec!(0)).is_superseded_by(&open(1, dec!(0))));
    }

    #[test]
    fn an_earlier_stamp_alone_does_not_supersede() {
        assert!(!open(1, dec!(0)).is_superseded_by(&open(0, dec!(0))));
    }

    /// The creation-stamped reconciliation snapshot. Its stamp cannot place it after the states
    /// that followed the order's acknowledgement, but the cumulative it carries is evidence
    /// enough on its own.
    #[test]
    fn more_filled_supersedes_an_earlier_stamp() {
        assert!(open(1, dec!(0.3)).is_superseded_by(&open(0, dec!(0.6))));
    }

    /// The case the stamp cannot catch at a venue that stamps locally on receipt: a snapshot that
    /// overtook a newer one in flight carries the later stamp and the earlier state. Cumulative
    /// fill is append-only, so reporting less of it is proof the snapshot is out of sequence.
    #[test]
    fn less_filled_is_refused_despite_a_later_stamp() {
        assert!(!open(0, dec!(0.6)).is_superseded_by(&open(1, dec!(0.3))));
    }

    /// Both keys together are still weaker than a correct stamp: where the cumulative does not
    /// move, ordering falls back to the stamp alone. This is the whole of an order's life before
    /// its first execution.
    #[test]
    fn an_unmoved_cumulative_leaves_ordering_to_the_stamp() {
        assert!(!open(1, dec!(0.3)).is_superseded_by(&open(0, dec!(0.3))));
        assert!(open(0, dec!(0.3)).is_superseded_by(&open(1, dec!(0.3))));
    }
}
