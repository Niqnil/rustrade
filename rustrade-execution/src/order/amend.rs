//! Amending a resting order in place: what an amend changes, what a client can amend, and the
//! answer to one.
//!
//! An amend changes a working order's price, total quantity or trigger price, and nothing else.
//! A change of side, kind or time in force is a different order: send a
//! [`CancelThenOpen`](super::request::CancelThenOpen) instead.
//!
//! A client declares what it can amend in [`AmendSupport`]. An amend it cannot carry is refused
//! with [`OrderError::AmendRefused`](crate::error::OrderError::AmendRefused), never served some
//! other way, such as by cancelling and placing a new order.

use crate::{
    error::{AmendField, AmendRefusal},
    order::{OrderKind, TimeInForce, id::VenueOrderId, request::ReduceOnly, state::Open},
};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use serde::{Deserialize, Serialize};

/// What an amend changes: each field is `Some` with its new value, or `None` to leave it.
///
/// - `price`: the new limit price. Only an order that has one (a limit, stop-limit,
///   take-profit-limit or trailing stop with a fixed limit) can change it.
/// - `quantity`: the new **total** quantity of the order, filled part included, not the quantity
///   left to fill. It must be above the quantity already filled.
/// - `trigger_price`: the new trigger of a [`Stop`](OrderKind::Stop),
///   [`StopLimit`](OrderKind::StopLimit), [`TakeProfit`](OrderKind::TakeProfit) or
///   [`TakeProfitLimit`](OrderKind::TakeProfitLimit). A trailing stop's offset and activation
///   cannot be amended.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// build one from [`AmendPatch::default`] and the `with_*` methods.
#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
pub struct AmendPatch {
    pub price: Option<Decimal>,
    pub quantity: Option<Decimal>,
    pub trigger_price: Option<Decimal>,
}

impl AmendPatch {
    /// This patch, setting the new limit price.
    pub fn with_price(self, price: Decimal) -> Self {
        Self {
            price: Some(price),
            ..self
        }
    }

    /// This patch, setting the new total quantity.
    pub fn with_quantity(self, quantity: Decimal) -> Self {
        Self {
            quantity: Some(quantity),
            ..self
        }
    }

    /// This patch, setting the new trigger price.
    pub fn with_trigger_price(self, trigger_price: Decimal) -> Self {
        Self {
            trigger_price: Some(trigger_price),
            ..self
        }
    }

    /// The fields this patch sets, in the order price, quantity, trigger price.
    pub fn fields(&self) -> impl Iterator<Item = AmendField> {
        [
            self.price.map(|_| AmendField::Price),
            self.quantity.map(|_| AmendField::Quantity),
            self.trigger_price.map(|_| AmendField::TriggerPrice),
        ]
        .into_iter()
        .flatten()
    }

    /// Checks this patch against the order it amends, `current`, and returns the order as the
    /// patch would leave it.
    ///
    /// `filled` is the quantity already filled. Refused when the patch sets nothing, or changes
    /// nothing; when it sets a price on an order without one, or a trigger on a kind without
    /// one; or when its quantity is not above `filled`.
    pub fn apply(
        &self,
        current: &AmendTarget,
        filled: Decimal,
    ) -> Result<AmendTarget, AmendRefusal> {
        let mut amended = current.clone();

        if let Some(price) = self.price {
            if current.price.is_none() || current.kind.is_conditional_unresolved() {
                return Err(AmendRefusal::NotOnKind {
                    field: AmendField::Price,
                });
            }
            amended.price = Some(price);
        }

        if let Some(quantity) = self.quantity {
            if quantity <= filled {
                return Err(AmendRefusal::QuantityNotAboveFilled { filled });
            }
            amended.quantity = quantity;
        }

        if let Some(trigger_price) = self.trigger_price {
            amended.kind = match current.kind {
                OrderKind::Stop { .. } => OrderKind::Stop { trigger_price },
                OrderKind::StopLimit { .. } => OrderKind::StopLimit { trigger_price },
                OrderKind::TakeProfit { .. } => OrderKind::TakeProfit { trigger_price },
                OrderKind::TakeProfitLimit { .. } => OrderKind::TakeProfitLimit { trigger_price },
                _ => {
                    return Err(AmendRefusal::NotOnKind {
                        field: AmendField::TriggerPrice,
                    });
                }
            };
        }

        if amended == *current {
            return Err(AmendRefusal::NoChange);
        }

        Ok(amended)
    }
}

/// The order an amend applies to, as tracked when the amend was sent.
///
/// A venue that takes an amend only as the whole order sent again reads the fields an amend
/// leaves from here.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct AmendTarget {
    pub side: Side,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
    /// As the order was opened with: see [`RequestOpen::reduce_only`](super::request::RequestOpen::reduce_only).
    pub reduce_only: ReduceOnly,
}

/// An amend of a working order, as sent to a client.
///
/// The engine builds it from the order it tracks: `id` and `current` describe that order, and
/// `patch` is the change.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// build one with [`RequestAmend::new`].
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct RequestAmend {
    /// How the venue addresses the order: an amend applies only to an order the venue has
    /// acknowledged. [`VenueOrderId::ClientAssigned`] means by the `ClientOrderId` in the
    /// request's key.
    pub id: VenueOrderId,
    /// What to change.
    pub patch: AmendPatch,
    /// The order before the change.
    pub current: AmendTarget,
    /// The quantity already filled when the amend was sent.
    pub filled: Decimal,
}

impl RequestAmend {
    pub fn new(id: VenueOrderId, patch: AmendPatch, current: AmendTarget, filled: Decimal) -> Self {
        Self {
            id,
            patch,
            current,
            filled,
        }
    }

    /// The order as this amend would leave it.
    ///
    /// # Errors
    /// As [`AmendPatch::apply`].
    pub fn amended(&self) -> Result<AmendTarget, AmendRefusal> {
        self.patch.apply(&self.current, self.filled)
    }
}

/// The answer to an amend the venue made: the order as it now stands.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// build one with [`Amended::new`].
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct Amended {
    /// The order's state after the amend, with the identifier the venue now addresses it by.
    pub open: Open,
    /// The order's limit price after the amend.
    pub price: Option<Decimal>,
    /// The order's total quantity after the amend, filled part included.
    pub quantity: Decimal,
    /// The order's kind after the amend, carrying its trigger price.
    pub kind: OrderKind,
    /// The identifier the venue addressed the order by before the amend, when the amend changed
    /// it. `None` when the order kept its identifier.
    pub previous_id: Option<VenueOrderId>,
}

impl Amended {
    pub fn new(
        open: Open,
        price: Option<Decimal>,
        quantity: Decimal,
        kind: OrderKind,
        previous_id: Option<VenueOrderId>,
    ) -> Self {
        Self {
            open,
            price,
            quantity,
            kind,
            previous_id,
        }
    }
}

/// Whether a client can amend one field of an order.
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
pub enum AmendAllowed {
    /// The field cannot be amended.
    #[default]
    Unsupported,
    /// The field can be amended to any value.
    Any,
    /// The field can only be lowered.
    DecreaseOnly,
}

/// What an amend does to an order's place in the venue's queue.
#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
pub enum AmendPriority {
    /// The venue does not say.
    #[default]
    Unknown,
    /// The order keeps its place when only its quantity is lowered; any other amend sends it to
    /// the back of the queue at its price.
    KeptOnQuantityDecrease,
}

/// What a client can amend.
///
/// [`ExecutionClient::amend_support`](crate::client::ExecutionClient::amend_support) declares it,
/// and the engine's `ExecutionManager` refuses an amend it does not
/// [`permit`](Self::permits) without sending it. A client may still refuse an amend this permits,
/// for what only the venue knows, such as an instrument or order type it cannot amend.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// start from [`AmendSupport::NONE`] and the `with_*` methods.
#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
pub struct AmendSupport {
    pub price: AmendAllowed,
    pub quantity: AmendAllowed,
    pub trigger_price: AmendAllowed,
    /// Whether one amend may change more than one field. When `false`, an amend setting two or
    /// more is refused, rather than applied in part.
    pub multi_field: bool,
    /// Whether an amended order keeps the identifier the venue addresses it by.
    pub keeps_id: bool,
    /// What an amend does to the order's queue position.
    pub priority: AmendPriority,
}

impl AmendSupport {
    /// Nothing can be amended: every amend is refused.
    pub const NONE: Self = Self {
        price: AmendAllowed::Unsupported,
        quantity: AmendAllowed::Unsupported,
        trigger_price: AmendAllowed::Unsupported,
        multi_field: false,
        keeps_id: true,
        priority: AmendPriority::Unknown,
    };

    /// Every field can be amended to any value, several at once, and the order keeps its
    /// identifier.
    pub const ALL: Self = Self {
        price: AmendAllowed::Any,
        quantity: AmendAllowed::Any,
        trigger_price: AmendAllowed::Any,
        multi_field: true,
        keeps_id: true,
        priority: AmendPriority::Unknown,
    };

    pub fn with_price(self, price: AmendAllowed) -> Self {
        Self { price, ..self }
    }

    pub fn with_quantity(self, quantity: AmendAllowed) -> Self {
        Self { quantity, ..self }
    }

    pub fn with_trigger_price(self, trigger_price: AmendAllowed) -> Self {
        Self {
            trigger_price,
            ..self
        }
    }

    pub fn with_multi_field(self, multi_field: bool) -> Self {
        Self {
            multi_field,
            ..self
        }
    }

    pub fn with_keeps_id(self, keeps_id: bool) -> Self {
        Self { keeps_id, ..self }
    }

    pub fn with_priority(self, priority: AmendPriority) -> Self {
        Self { priority, ..self }
    }

    /// Whether nothing can be amended.
    pub fn is_none(&self) -> bool {
        [self.price, self.quantity, self.trigger_price]
            .iter()
            .all(|allowed| *allowed == AmendAllowed::Unsupported)
    }

    /// Checks `request` against what this declares.
    ///
    /// # Errors
    /// - [`AmendRefusal::Unsupported`] for the first field the request changes that cannot be
    ///   amended;
    /// - [`AmendRefusal::Increase`] for a field that can only be lowered and is raised;
    /// - [`AmendRefusal::MultipleFields`] for a request changing more than one field where
    ///   [`multi_field`](Self::multi_field) is `false`.
    pub fn permits(&self, request: &RequestAmend) -> Result<(), AmendRefusal> {
        let patch = &request.patch;
        let current = &request.current;
        let current_trigger = trigger_price(&current.kind);

        for (field, allowed, new, old) in [
            (AmendField::Price, self.price, patch.price, current.price),
            (
                AmendField::Quantity,
                self.quantity,
                patch.quantity,
                Some(current.quantity),
            ),
            (
                AmendField::TriggerPrice,
                self.trigger_price,
                patch.trigger_price,
                current_trigger,
            ),
        ] {
            let Some(new) = new else { continue };
            match allowed {
                AmendAllowed::Unsupported => return Err(AmendRefusal::Unsupported { field }),
                AmendAllowed::Any => {}
                AmendAllowed::DecreaseOnly => {
                    if old.is_none_or(|old| new > old) {
                        return Err(AmendRefusal::Increase { field });
                    }
                }
            }
        }

        if !self.multi_field && patch.fields().count() > 1 {
            return Err(AmendRefusal::MultipleFields);
        }

        Ok(())
    }
}

/// The trigger price of a kind whose trigger can be amended.
fn trigger_price(kind: &OrderKind) -> Option<Decimal> {
    match kind {
        OrderKind::Stop { trigger_price }
        | OrderKind::StopLimit { trigger_price }
        | OrderKind::TakeProfit { trigger_price }
        | OrderKind::TakeProfitLimit { trigger_price } => Some(*trigger_price),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::order::id::OrderId;
    use rust_decimal_macros::dec;

    fn target(kind: OrderKind, price: Option<Decimal>) -> AmendTarget {
        AmendTarget {
            side: Side::Buy,
            price,
            quantity: dec!(10),
            kind,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            reduce_only: ReduceOnly::No,
        }
    }

    fn limit() -> AmendTarget {
        target(OrderKind::Limit, Some(dec!(100)))
    }

    fn request(patch: AmendPatch, current: AmendTarget) -> RequestAmend {
        RequestAmend::new(
            VenueOrderId::Assigned(OrderId::new("1")),
            patch,
            current,
            dec!(0),
        )
    }

    #[test]
    fn a_patch_sets_only_the_fields_it_names() {
        let amended = AmendPatch::default()
            .with_price(dec!(99))
            .apply(&limit(), dec!(0))
            .unwrap();
        assert_eq!(amended.price, Some(dec!(99)));
        assert_eq!(amended.quantity, dec!(10));
        assert_eq!(amended.kind, OrderKind::Limit);
    }

    #[test]
    fn a_patch_that_changes_nothing_is_refused() {
        assert_eq!(
            AmendPatch::default().apply(&limit(), dec!(0)),
            Err(AmendRefusal::NoChange)
        );
        assert_eq!(
            AmendPatch::default()
                .with_price(dec!(100))
                .apply(&limit(), dec!(0)),
            Err(AmendRefusal::NoChange)
        );
    }

    #[test]
    fn a_quantity_must_stay_above_what_filled() {
        assert_eq!(
            AmendPatch::default()
                .with_quantity(dec!(4))
                .apply(&limit(), dec!(4)),
            Err(AmendRefusal::QuantityNotAboveFilled { filled: dec!(4) })
        );
        assert_eq!(
            AmendPatch::default()
                .with_quantity(dec!(5))
                .apply(&limit(), dec!(4))
                .unwrap()
                .quantity,
            dec!(5)
        );
    }

    #[test]
    fn a_price_needs_an_order_with_one() {
        let stop = target(
            OrderKind::Stop {
                trigger_price: dec!(90),
            },
            None,
        );
        assert_eq!(
            AmendPatch::default()
                .with_price(dec!(95))
                .apply(&stop, dec!(0)),
            Err(AmendRefusal::NotOnKind {
                field: AmendField::Price
            })
        );
    }

    #[test]
    fn a_trigger_moves_the_trigger_of_its_kind() {
        let stop_limit = target(
            OrderKind::StopLimit {
                trigger_price: dec!(90),
            },
            Some(dec!(89)),
        );
        let amended = AmendPatch::default()
            .with_trigger_price(dec!(92))
            .apply(&stop_limit, dec!(0))
            .unwrap();
        assert_eq!(
            amended.kind,
            OrderKind::StopLimit {
                trigger_price: dec!(92)
            }
        );
        assert_eq!(amended.price, Some(dec!(89)));

        assert_eq!(
            AmendPatch::default()
                .with_trigger_price(dec!(92))
                .apply(&limit(), dec!(0)),
            Err(AmendRefusal::NotOnKind {
                field: AmendField::TriggerPrice
            })
        );
    }

    #[test]
    fn support_refuses_a_field_it_cannot_amend() {
        let request = request(AmendPatch::default().with_price(dec!(99)), limit());
        assert_eq!(
            AmendSupport::NONE.permits(&request),
            Err(AmendRefusal::Unsupported {
                field: AmendField::Price
            })
        );
        assert_eq!(AmendSupport::ALL.permits(&request), Ok(()));
    }

    #[test]
    fn support_for_decreases_only_refuses_an_increase() {
        let support = AmendSupport::NONE.with_quantity(AmendAllowed::DecreaseOnly);
        assert_eq!(
            support.permits(&request(
                AmendPatch::default().with_quantity(dec!(11)),
                limit()
            )),
            Err(AmendRefusal::Increase {
                field: AmendField::Quantity
            })
        );
        assert_eq!(
            support.permits(&request(
                AmendPatch::default().with_quantity(dec!(10)),
                limit()
            )),
            Ok(())
        );
        assert_eq!(
            support.permits(&request(
                AmendPatch::default().with_quantity(dec!(9)),
                limit()
            )),
            Ok(())
        );
    }

    #[test]
    fn support_for_one_field_refuses_two() {
        let request = request(
            AmendPatch::default()
                .with_price(dec!(99))
                .with_quantity(dec!(9)),
            limit(),
        );
        assert_eq!(
            AmendSupport::ALL.with_multi_field(false).permits(&request),
            Err(AmendRefusal::MultipleFields)
        );
        assert_eq!(AmendSupport::ALL.permits(&request), Ok(()));
    }

    #[test]
    fn none_is_none_and_all_is_not() {
        assert!(AmendSupport::NONE.is_none());
        assert!(!AmendSupport::ALL.is_none());
        assert!(
            !AmendSupport::NONE
                .with_trigger_price(AmendAllowed::Any)
                .is_none()
        );
    }
}
