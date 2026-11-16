//! What applying order reports did to the orders the engine tracks: orders it followed across a
//! replace at the venue, orders it began tracking from a venue's report, reports of untracked
//! orders it refused, and terms it adopted or left diverged.

use crate::engine::state::order::terms::{OrderTermsChanged, OrderTermsDiverged};
use rustrade_execution::order::{
    Order, OrderKey,
    id::VenueOrderId,
    report::{Replaces, ReportedTerms},
    state::ActiveOrderState,
};
use serde::{Deserialize, Serialize};

/// The engine followed an order it tracks across a replace at the venue: a report linked it
/// ([`OrderReport::replaces`](rustrade_execution::order::report::OrderReport::replaces)) to the
/// order the engine tracked, under a new venue order id, a new client order id, or both.
///
/// Emitted as [`EngineOutput::OrderReplaced`](crate::engine::EngineOutput::OrderReplaced). The
/// engine tracks the order on as one order: its strategy, its fills and the terms it held,
/// until the report's own terms are applied, as for any report of it. Not emitted for the
/// engine's own amends, whose answer names the venue order id it replaced
/// ([`Amended::previous_id`](rustrade_execution::order::amend::Amended::previous_id)).
///
/// Moved to a new client order id, the order is no longer tracked under the old one, and any
/// cancel or amend in flight under it is dropped: the venue answers it under the old id, which
/// the engine then ignores. The order keeps working at the venue under its new ids, so a
/// strategy that still wants it cancelled or amended sends that request again, under
/// [`key`](Self::key).
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderReplaced<ExchangeKey, InstrumentKey> {
    /// The order's key as the engine now tracks it: the replacement's client order id, with the
    /// strategy the order was tracked under.
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    /// The venue order id the report names, which the order is addressed by now.
    pub id: VenueOrderId,
    /// What the order was addressed by before: its venue order id, and its client order id when
    /// that changed.
    pub replaced: Replaces,
}

/// The engine began tracking an order from a venue's report of it, not from a request it sent.
///
/// Emitted as [`EngineOutput::OrderAdopted`](crate::engine::EngineOutput::OrderAdopted). The
/// order may be one placed outside the engine (by hand in the venue's interface, by another API
/// client on the account), or one this engine's own earlier run placed, listed again after a
/// restart or a reconnect. The report alone cannot tell the two apart. Whether to cancel the
/// order, hand it to a strategy or leave it is the consumer's decision.
///
/// The engine tracks the order on the reported terms as they stand, stated or not. A term the
/// report did not state ([`reported`](Self::reported)) may be a stand-in, such as a placement
/// value or an unknown kind; a later report stating it replaces it.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderAdopted<ExchangeKey, InstrumentKey> {
    /// The order as the engine now tracks it.
    pub order: Order<ExchangeKey, InstrumentKey, ActiveOrderState>,
    /// The terms the report stated as the venue holds them now.
    pub reported: ReportedTerms,
}

/// The engine did not track an order a venue reported working, for the reason given.
///
/// Emitted as [`EngineOutput::OrderNotAdopted`](crate::engine::EngineOutput::OrderNotAdopted).
/// The order may still be working at the venue, untracked: the engine neither counts it nor
/// routes requests for it. The engine does not remember the refusal, so a later report of the
/// same order that can be tracked is tracked, and one that cannot is reported again.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderNotAdopted<ExchangeKey, InstrumentKey> {
    /// The order as reported.
    pub order: Order<ExchangeKey, InstrumentKey, ActiveOrderState>,
    /// Why the engine did not track it.
    pub reason: OrderNotAdoptedReason,
}

/// Why the engine did not track an order a venue reported working. See [`OrderNotAdopted`].
///
/// `#[non_exhaustive]`, so a reason can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum OrderNotAdoptedReason {
    /// The reported quantity is zero or negative, which no working order has: a placeholder, or a
    /// value the producer could not read. Tracked, the order would have nothing to fill, and the
    /// engine would take it for finished while it may still be working.
    QuantityNotPositive,
}

/// What applying one or more order reports did to the orders they describe: the orders followed
/// across a replace, the orders adopted and those refused, and the terms changed and those stated
/// that the engine did not adopt.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Each field defaults
/// when deserialised, through a path, so that the key types need no `Default`.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderReportsApplied<ExchangeKey, InstrumentKey> {
    /// The orders followed across a replace at the venue, one per replace.
    #[serde(default = "Vec::new")]
    pub replaced: Vec<OrderReplaced<ExchangeKey, InstrumentKey>>,
    /// The terms adopted, one per order changed.
    #[serde(default = "Vec::new")]
    pub changed: Vec<OrderTermsChanged<ExchangeKey, InstrumentKey>>,
    /// The terms not adopted, one per order whose report stated them.
    #[serde(default = "Vec::new")]
    pub diverged: Vec<OrderTermsDiverged<ExchangeKey, InstrumentKey>>,
    /// The orders the engine began tracking from a report, one per order.
    #[serde(default = "Vec::new")]
    pub adopted: Vec<OrderAdopted<ExchangeKey, InstrumentKey>>,
    /// The reports of untracked working orders the engine did not track, one per report.
    #[serde(default = "Vec::new")]
    pub not_adopted: Vec<OrderNotAdopted<ExchangeKey, InstrumentKey>>,
}

impl<ExchangeKey, InstrumentKey> Default for OrderReportsApplied<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            replaced: Vec::new(),
            changed: Vec::new(),
            diverged: Vec::new(),
            adopted: Vec::new(),
            not_adopted: Vec::new(),
        }
    }
}

impl<ExchangeKey, InstrumentKey> OrderReportsApplied<ExchangeKey, InstrumentKey> {
    /// Whether no order was followed across a replace, adopted or refused, and no order's terms
    /// were changed or left diverged.
    pub fn is_empty(&self) -> bool {
        self.replaced.is_empty()
            && self.changed.is_empty()
            && self.diverged.is_empty()
            && self.adopted.is_empty()
            && self.not_adopted.is_empty()
    }

    /// Append what `other` applied, after what this holds.
    pub fn extend(&mut self, other: Self) {
        self.replaced.extend(other.replaced);
        self.changed.extend(other.changed);
        self.diverged.extend(other.diverged);
        self.adopted.extend(other.adopted);
        self.not_adopted.extend(other.not_adopted);
    }
}
