use crate::order::OrderKey;
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};

/// The [`cancel_after`](crate::order::request::RequestOpen::cancel_after) of an order ran out: what
/// is left of it is to be cancelled.
///
/// Sent on the account stream as
/// [`AccountEventKind::OrderDeadline`](crate::AccountEventKind::OrderDeadline) by the driver that
/// sent the order, not by the venue, so it says nothing about the account link. It may arrive
/// after the order has ended (a fill or cancel the driver had not seen), in which case there is
/// nothing to cancel.
#[non_exhaustive]
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct OrderDeadline<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// The order whose time ran out.
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    /// When it ran out: the instant the open was sent plus its `cancel_after`, on the driver's
    /// clock (simulated time in a simulation).
    pub deadline: DateTime<Utc>,
}
