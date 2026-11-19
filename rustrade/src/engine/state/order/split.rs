//! Rescaling the orders an instrument tracks when its shares split.
//!
//! A split changes the unit an order is counted in, not the order: a resting order for 10 shares
//! at 100 is, after a 2-for-1 split, one for 20 shares at 50. A venue that keeps such orders
//! working restates them that way, filled part included. So the engine rescales its view of each
//! tracked order exactly, with no rounding: quantities × the ratio, prices ÷ the ratio. Rounding to
//! a lot or a tick is the venue's, and its next report of the order says what it chose (see "After
//! a split" on [`TrackedOrder`]).
//!
//! What is a price is rescaled: the limit price, every trigger and activation price, a trailing
//! stop-limit's stop price and limit offset, and an [`Absolute`](TrailingOffsetType::Absolute)
//! trailing offset. A [`Percentage`](TrailingOffsetType::Percentage) or
//! [`BasisPoints`](TrailingOffsetType::BasisPoints) offset is a proportion, which a split leaves
//! as it is. The time in force is not a quantity or a price.

use crate::engine::{
    OpenOrderAtSplit,
    state::{
        order::{Orders, TrackedOrder, terms::OrderTerms},
        position::SplitError,
    },
};
use rust_decimal::Decimal;
use rustrade_execution::order::{
    OrderKind, TrailingOffsetType,
    amend::AmendPatch,
    state::{ActiveOrderState, AmendInFlight, CancelInFlight, Open},
};
use rustrade_instrument::corporate_action::SplitRatio;

/// The tracked orders of one instrument, rescaled for a split but not yet written back: built by
/// [`Orders::prepare_split`], written by [`Orders::commit_split`].
#[derive(Debug)]
pub(crate) struct PreparedOrdersSplit<ExchangeKey, InstrumentKey>(
    Vec<TrackedOrder<ExchangeKey, InstrumentKey>>,
);

impl<ExchangeKey, InstrumentKey> Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Clone,
    InstrumentKey: Clone,
{
    /// Every tracked order rescaled for a split of `ratio`, mutating nothing.
    ///
    /// # Errors
    /// [`SplitError::Overflow`] when a rescaled value does not fit a [`Decimal`].
    pub(crate) fn prepare_split(
        &self,
        ratio: SplitRatio,
    ) -> Result<PreparedOrdersSplit<ExchangeKey, InstrumentKey>, SplitError> {
        self.0
            .values()
            .map(|tracked| tracked.split(ratio.get()).ok_or(SplitError::Overflow))
            .collect::<Result<_, _>>()
            .map(PreparedOrdersSplit)
    }

    /// Write the orders `prepared` rescaled, returning each one's terms before and after.
    ///
    /// # Panics
    /// When `prepared` holds an order no longer tracked: it was prepared from these orders, so
    /// that is plan and state out of step.
    pub(crate) fn commit_split(
        &mut self,
        prepared: PreparedOrdersSplit<ExchangeKey, InstrumentKey>,
    ) -> Vec<OpenOrderAtSplit> {
        prepared
            .0
            .into_iter()
            .map(|rescaled| {
                let cid = rescaled.order.key.cid.clone();
                let post_split = OrderTerms::of(&rescaled.order);
                match self.0.insert(cid.clone(), rescaled) {
                    Some(held) => OpenOrderAtSplit {
                        cid,
                        pre_split: OrderTerms::of(&held.order),
                        post_split,
                    },
                    None => unreachable!(
                        "a split prepared order {cid} that its instrument no longer tracks"
                    ),
                }
            })
            .collect()
    }
}

impl<ExchangeKey, InstrumentKey> TrackedOrder<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Clone,
    InstrumentKey: Clone,
{
    /// This order rescaled for a split of `ratio`, marked to take the venue's next report of it,
    /// with no term stated yet. `None` when a value overflows.
    fn split(&self, ratio: Decimal) -> Option<Self> {
        let mut order = self.order.clone();
        split_terms(OrderTerms::of(&order), ratio)?.apply_to(&mut order);
        order.state = split_state(&order.state, ratio)?;
        Some(Self {
            order,
            // The venue stated the terms held before the split, on the old scale: none of them
            // speaks for the rescaled ones.
            terms_times: Default::default(),
            superseded_ids: self.superseded_ids.clone(),
            split_unreported: true,
        })
    }
}

/// `terms` rescaled for a split of `ratio`, or `None` when a value overflows.
fn split_terms(terms: OrderTerms, ratio: Decimal) -> Option<OrderTerms> {
    Some(OrderTerms {
        price: split_price(terms.price, ratio)?,
        quantity: terms.quantity.checked_mul(ratio)?,
        kind: split_kind(terms.kind, ratio)?,
        time_in_force: terms.time_in_force,
    })
}

/// `price` ÷ `ratio`, `Some(None)` for no price, `None` on overflow.
fn split_price(price: Option<Decimal>, ratio: Decimal) -> Option<Option<Decimal>> {
    match price {
        Some(price) => price.checked_div(ratio).map(Some),
        None => Some(None),
    }
}

/// A trailing offset rescaled: an absolute one is a price; a proportion is unchanged.
fn split_offset(
    offset: Decimal,
    offset_type: TrailingOffsetType,
    ratio: Decimal,
) -> Option<Decimal> {
    match offset_type {
        TrailingOffsetType::Absolute => offset.checked_div(ratio),
        TrailingOffsetType::Percentage | TrailingOffsetType::BasisPoints => Some(offset),
    }
}

fn split_kind(kind: OrderKind, ratio: Decimal) -> Option<OrderKind> {
    Some(match kind {
        OrderKind::Market | OrderKind::Limit => kind,
        OrderKind::Stop { trigger_price } => OrderKind::Stop {
            trigger_price: trigger_price.checked_div(ratio)?,
        },
        OrderKind::StopLimit { trigger_price } => OrderKind::StopLimit {
            trigger_price: trigger_price.checked_div(ratio)?,
        },
        OrderKind::TakeProfit { trigger_price } => OrderKind::TakeProfit {
            trigger_price: trigger_price.checked_div(ratio)?,
        },
        OrderKind::TakeProfitLimit { trigger_price } => OrderKind::TakeProfitLimit {
            trigger_price: trigger_price.checked_div(ratio)?,
        },
        OrderKind::TrailingStop {
            offset,
            offset_type,
            activation_price,
        } => OrderKind::TrailingStop {
            offset: split_offset(offset, offset_type, ratio)?,
            offset_type,
            activation_price: split_price(activation_price, ratio)?,
        },
        OrderKind::TrailingStopLimit {
            offset,
            offset_type,
            stop_price,
            limit_offset,
        } => OrderKind::TrailingStopLimit {
            offset: split_offset(offset, offset_type, ratio)?,
            offset_type,
            stop_price: stop_price.checked_div(ratio)?,
            limit_offset: limit_offset.checked_div(ratio)?,
        },
        OrderKind::TrailingStopFixedLimit {
            offset,
            offset_type,
            activation_price,
        } => OrderKind::TrailingStopFixedLimit {
            offset: split_offset(offset, offset_type, ratio)?,
            offset_type,
            activation_price: split_price(activation_price, ratio)?,
        },
        OrderKind::ConditionalUnresolved {
            family,
            trigger_price,
        } => OrderKind::ConditionalUnresolved {
            family,
            trigger_price: split_price(trigger_price, ratio)?,
        },
        // `OrderKind` is `#[non_exhaustive]` in another crate. A kind added there carrying a
        // price must gain an arm here; until then it keeps its terms, and the venue's next report
        // of the order restates them. Warned of, so the gap shows. The audit replica runs this
        // pass too, so it warns a second time.
        other => {
            tracing::warn!(
                kind = ?other,
                "A split rescaled an order of a kind it does not know - its prices keep the old scale"
            );
            other
        }
    })
}

/// `state` with every quantity it carries rescaled for a split of `ratio`, `None` on overflow.
fn split_state(state: &ActiveOrderState, ratio: Decimal) -> Option<ActiveOrderState> {
    Some(match state {
        ActiveOrderState::OpenInFlight(in_flight) => ActiveOrderState::OpenInFlight(*in_flight),
        ActiveOrderState::Open(open) => ActiveOrderState::Open(split_open(open, ratio)?),
        ActiveOrderState::CancelInFlight(cancel) => {
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: match &cancel.order {
                    Some(open) => Some(split_open(open, ratio)?),
                    None => None,
                },
                time_sent: cancel.time_sent,
            })
        }
        ActiveOrderState::AmendInFlight(amend) => ActiveOrderState::AmendInFlight(AmendInFlight {
            order: split_open(&amend.order, ratio)?,
            patch: split_patch(amend.patch, ratio)?,
            time_sent: amend.time_sent,
        }),
    })
}

fn split_open(open: &Open, ratio: Decimal) -> Option<Open> {
    Some(Open {
        filled_quantity: open.filled_quantity.checked_mul(ratio)?,
        ..open.clone()
    })
}

/// An amend's new values rescaled, so the order it was sent for and its answer compare on one
/// scale.
///
/// `AmendPatch` is `#[non_exhaustive]` in another crate, so its fields are rescaled in place. A
/// field added there carrying a quantity or a price must be rescaled here too.
fn split_patch(mut patch: AmendPatch, ratio: Decimal) -> Option<AmendPatch> {
    patch.price = split_price(patch.price, ratio)?;
    patch.quantity = match patch.quantity {
        Some(quantity) => Some(quantity.checked_mul(ratio)?),
        None => None,
    };
    patch.trigger_price = split_price(patch.trigger_price, ratio)?;
    Some(patch)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::order::{
        ConditionalFamily, Order, OrderKey, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        state::OpenInFlight,
    };
    use rustrade_instrument::{Side, exchange::ExchangeId};

    const TIME: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;

    fn open(filled: Decimal) -> Open {
        Open::new(VenueOrderId::Assigned(OrderId::new("1")), TIME, filled)
    }

    fn terms(price: Option<Decimal>, quantity: Decimal, kind: OrderKind) -> OrderTerms {
        OrderTerms {
            price,
            quantity,
            kind,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
        }
    }

    fn orders(price: Option<Decimal>, quantity: Decimal) -> Orders<ExchangeId, u64> {
        let order = Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid: ClientOrderId::new("split"),
            },
            side: Side::Buy,
            price,
            quantity,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: ActiveOrderState::Open(open(Decimal::ZERO)),
        };
        Orders::new([(order.key.cid.clone(), order)].into_iter().collect())
    }

    /// Every price a kind carries is divided by the ratio, a proportion is not, and the quantity
    /// is multiplied by it.
    #[test]
    fn a_split_divides_every_price_and_multiplies_the_quantity() {
        let ratio = dec!(4);
        let trailing = |offset_type| OrderKind::TrailingStop {
            offset: dec!(8),
            offset_type,
            activation_price: Some(dec!(40)),
        };
        let trailing_limit = |offset_type| OrderKind::TrailingStopLimit {
            offset: dec!(8),
            offset_type,
            stop_price: dec!(40),
            limit_offset: dec!(2),
        };
        let cases = [
            (OrderKind::Market, OrderKind::Market),
            (OrderKind::Limit, OrderKind::Limit),
            (
                OrderKind::Stop {
                    trigger_price: dec!(40),
                },
                OrderKind::Stop {
                    trigger_price: dec!(10),
                },
            ),
            (
                OrderKind::StopLimit {
                    trigger_price: dec!(40),
                },
                OrderKind::StopLimit {
                    trigger_price: dec!(10),
                },
            ),
            (
                OrderKind::TakeProfit {
                    trigger_price: dec!(40),
                },
                OrderKind::TakeProfit {
                    trigger_price: dec!(10),
                },
            ),
            (
                OrderKind::TakeProfitLimit {
                    trigger_price: dec!(40),
                },
                OrderKind::TakeProfitLimit {
                    trigger_price: dec!(10),
                },
            ),
            (
                trailing(TrailingOffsetType::Absolute),
                OrderKind::TrailingStop {
                    offset: dec!(2),
                    offset_type: TrailingOffsetType::Absolute,
                    activation_price: Some(dec!(10)),
                },
            ),
            (
                trailing(TrailingOffsetType::Percentage),
                OrderKind::TrailingStop {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::Percentage,
                    activation_price: Some(dec!(10)),
                },
            ),
            (
                OrderKind::TrailingStop {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::BasisPoints,
                    activation_price: None,
                },
                OrderKind::TrailingStop {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::BasisPoints,
                    activation_price: None,
                },
            ),
            (
                trailing_limit(TrailingOffsetType::Absolute),
                OrderKind::TrailingStopLimit {
                    offset: dec!(2),
                    offset_type: TrailingOffsetType::Absolute,
                    stop_price: dec!(10),
                    limit_offset: dec!(0.5),
                },
            ),
            (
                trailing_limit(TrailingOffsetType::BasisPoints),
                OrderKind::TrailingStopLimit {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::BasisPoints,
                    stop_price: dec!(10),
                    limit_offset: dec!(0.5),
                },
            ),
            (
                OrderKind::TrailingStopFixedLimit {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::Absolute,
                    activation_price: Some(dec!(40)),
                },
                OrderKind::TrailingStopFixedLimit {
                    offset: dec!(2),
                    offset_type: TrailingOffsetType::Absolute,
                    activation_price: Some(dec!(10)),
                },
            ),
            (
                OrderKind::TrailingStopFixedLimit {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::Percentage,
                    activation_price: None,
                },
                OrderKind::TrailingStopFixedLimit {
                    offset: dec!(8),
                    offset_type: TrailingOffsetType::Percentage,
                    activation_price: None,
                },
            ),
            (
                OrderKind::ConditionalUnresolved {
                    family: ConditionalFamily::Stop,
                    trigger_price: Some(dec!(40)),
                },
                OrderKind::ConditionalUnresolved {
                    family: ConditionalFamily::Stop,
                    trigger_price: Some(dec!(10)),
                },
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                split_terms(terms(Some(dec!(40)), dec!(3), kind), ratio),
                Some(terms(Some(dec!(10)), dec!(12), expected)),
                "{kind:?}"
            );
        }
        assert_eq!(
            split_terms(terms(None, dec!(3), OrderKind::Market), ratio),
            Some(terms(None, dec!(12), OrderKind::Market))
        );
    }

    /// Every filled quantity a state carries is multiplied by the ratio, and an amend's values are
    /// rescaled as the order's terms are.
    #[test]
    fn a_split_rescales_the_filled_quantity_in_every_state() {
        let ratio = dec!(2);
        let cases = [
            (
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME)),
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME)),
            ),
            (
                ActiveOrderState::Open(open(dec!(3))),
                ActiveOrderState::Open(open(dec!(6))),
            ),
            (
                ActiveOrderState::CancelInFlight(CancelInFlight::new(Some(open(dec!(3))), TIME)),
                ActiveOrderState::CancelInFlight(CancelInFlight::new(Some(open(dec!(6))), TIME)),
            ),
            (
                ActiveOrderState::CancelInFlight(CancelInFlight::new(None, TIME)),
                ActiveOrderState::CancelInFlight(CancelInFlight::new(None, TIME)),
            ),
            (
                ActiveOrderState::AmendInFlight(AmendInFlight::new(
                    open(dec!(3)),
                    AmendPatch::default()
                        .with_price(dec!(10))
                        .with_quantity(dec!(7))
                        .with_trigger_price(dec!(8)),
                    TIME,
                )),
                ActiveOrderState::AmendInFlight(AmendInFlight::new(
                    open(dec!(6)),
                    AmendPatch::default()
                        .with_price(dec!(5))
                        .with_quantity(dec!(14))
                        .with_trigger_price(dec!(4)),
                    TIME,
                )),
            ),
            (
                ActiveOrderState::AmendInFlight(AmendInFlight::new(
                    open(dec!(3)),
                    AmendPatch::default(),
                    TIME,
                )),
                ActiveOrderState::AmendInFlight(AmendInFlight::new(
                    open(dec!(6)),
                    AmendPatch::default(),
                    TIME,
                )),
            ),
        ];
        for (state, expected) in cases {
            assert_eq!(split_state(&state, ratio), Some(expected), "{state:?}");
        }
    }

    /// A rescale that overflows refuses the split and leaves the orders as they were.
    #[test]
    fn a_split_that_overflows_is_refused_without_changing_any_order() {
        let tiny = SplitRatio::new(dec!(0.0000000000000000000000000001)).expect("positive");
        let huge = SplitRatio::new(Decimal::MAX).expect("positive");
        for (orders, ratio) in [
            // The quantity × the ratio.
            (orders(None, dec!(2)), huge),
            // The price ÷ the ratio.
            (orders(Some(Decimal::MAX), dec!(1)), tiny),
        ] {
            let before = orders.clone();
            assert!(matches!(
                orders.prepare_split(ratio),
                Err(SplitError::Overflow)
            ));
            assert_eq!(orders, before);
        }
    }
}
