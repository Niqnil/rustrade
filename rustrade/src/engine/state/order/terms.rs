//! Adopting the terms a venue reports for an order the engine tracks.
//!
//! An order can change at the venue without the engine's hand: in the venue's own interface,
//! through another API client, or by the venue itself. A venue report that states the order's
//! terms ([`ReportedTerms`]) then differs from what the engine tracks. The rules below decide,
//! term by term, what the engine takes from such a report.
//!
//! # What is adopted
//!
//! Only the changes an amend could make:
//! - the total quantity;
//! - the limit price of a kind that carries a fixed one;
//! - the trigger of a [`Stop`](OrderKind::Stop), [`StopLimit`](OrderKind::StopLimit),
//!   [`TakeProfit`](OrderKind::TakeProfit) or [`TakeProfitLimit`](OrderKind::TakeProfitLimit),
//!   within the same kind.
//!
//! A stand-in the engine holds is replaced by the venue's value: a kind tracked as
//! [`ConditionalUnresolved`](OrderKind::ConditionalUnresolved) takes a reported kind of its
//! family, and a time in force tracked as [`Unreported`](TimeInForce::Unreported) takes the
//! reported one.
//!
//! # What is not
//!
//! A change of kind, of time in force, of a trailing order's offsets, or a limit price the venue
//! reports gone, is a different order to the engine. It keeps its terms, and reports the
//! difference as diverged. A trailing order's moving stop, or the activation price a venue may
//! restate once the stop trails, is no change at all and is ignored.
//!
//! A term the report does not state is never compared, and neither is a stand-in the report
//! carries for one.

use rust_decimal::Decimal;
use rustrade_execution::order::{
    ConditionalFamily, Order, OrderKey, OrderKind, TimeInForce, amend::AmendPatch,
    report::ReportedTerms,
};
use serde::{Deserialize, Serialize};

/// An order's terms: the parts of an [`Order`] a venue can change while it works.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderTerms {
    /// The limit price, `None` for an order without one.
    pub price: Option<Decimal>,
    /// The total quantity, filled part included.
    pub quantity: Decimal,
    /// The kind, with its trigger or trailing terms.
    pub kind: OrderKind,
    /// The time in force.
    pub time_in_force: TimeInForce,
}

impl OrderTerms {
    /// The terms `order` carries.
    pub fn of<ExchangeKey, InstrumentKey, State>(
        order: &Order<ExchangeKey, InstrumentKey, State>,
    ) -> Self {
        Self {
            price: order.price,
            quantity: order.quantity,
            kind: order.kind,
            time_in_force: order.time_in_force,
        }
    }

    /// Write these terms onto `order`.
    pub(crate) fn apply_to<ExchangeKey, InstrumentKey, State>(
        self,
        order: &mut Order<ExchangeKey, InstrumentKey, State>,
    ) {
        order.price = self.price;
        order.quantity = self.quantity;
        order.kind = self.kind;
        order.time_in_force = self.time_in_force;
    }
}

/// The engine adopted terms a venue reported for an order it tracks: the order changed at the
/// venue, other than by an amend the engine sent.
///
/// Emitted as [`EngineOutput::OrderTermsChanged`](crate::engine::EngineOutput::OrderTermsChanged).
/// `to` may leave the order nothing to fill, in which case the engine stopped tracking it.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderTermsChanged<ExchangeKey, InstrumentKey> {
    /// The order.
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    /// The terms the engine tracked before the report.
    pub from: OrderTerms,
    /// The terms the engine tracks now.
    pub to: OrderTerms,
}

/// A venue reported terms for an order the engine tracks that the engine did not adopt.
///
/// Emitted as
/// [`EngineOutput::OrderTermsDiverged`](crate::engine::EngineOutput::OrderTermsDiverged). The
/// engine goes on tracking the order on its own terms, which may no longer be the order the venue
/// is working. Whether to look the order up, cancel it or carry on is the consumer's decision.
///
/// Two causes, told apart by [`amend_in_flight`](Self::amend_in_flight):
/// - a change the engine never takes from a report: of kind, time in force, a trailing order's
///   offsets, or a limit price reported gone;
/// - while an amend is in flight, a quantity, price or trigger that is neither the order's before
///   the amend nor after it. The engine adopts nothing from a report while an amend is in flight:
///   the amend's answer settles the terms.
///
/// The engine keeps no record of a divergence it has reported, so a divergence that persists is
/// reported again for every report that states the term, an account snapshot listing the order
/// included. A consumer acting on it once deduplicates on [`key`](Self::key) and
/// [`reported`](Self::reported).
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderTermsDiverged<ExchangeKey, InstrumentKey> {
    /// The order.
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    /// The terms the engine tracks, after adopting whatever else the report changed.
    pub tracked: OrderTerms,
    /// The terms as reported. Only those [`terms`](Self::terms) names were compared.
    pub reported: OrderTerms,
    /// The terms the report stated that differ from those tracked and were not adopted.
    pub terms: ReportedTerms,
    /// Whether an amend was in flight when the report arrived.
    pub amend_in_flight: bool,
}

/// The terms order reports changed, and those they stated that the engine did not adopt: what
/// applying one or more order reports did to the orders' terms.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Each field defaults
/// when deserialised, through a path, so that the key types need no `Default`.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OrderTermsApplied<ExchangeKey, InstrumentKey> {
    /// The terms adopted, one per order changed.
    #[serde(default = "Vec::new")]
    pub changed: Vec<OrderTermsChanged<ExchangeKey, InstrumentKey>>,
    /// The terms not adopted, one per order whose report stated them.
    #[serde(default = "Vec::new")]
    pub diverged: Vec<OrderTermsDiverged<ExchangeKey, InstrumentKey>>,
}

impl<ExchangeKey, InstrumentKey> Default for OrderTermsApplied<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            changed: Vec::new(),
            diverged: Vec::new(),
        }
    }
}

impl<ExchangeKey, InstrumentKey> OrderTermsApplied<ExchangeKey, InstrumentKey> {
    /// Whether no order's terms were changed or left diverged.
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.diverged.is_empty()
    }

    /// Append what `other` applied, after what this holds.
    pub fn extend(&mut self, other: Self) {
        self.changed.extend(other.changed);
        self.diverged.extend(other.diverged);
    }
}

/// What the engine takes from a report: the terms to track, and those left diverged.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct Reconciled {
    /// The terms the engine tracks after the report.
    pub terms: OrderTerms,
    /// The terms the report stated, differing from those tracked, that were not adopted.
    pub diverged: ReportedTerms,
}

/// What the engine takes from a report stating `stated` of `reported`, for an order tracked with
/// `tracked`.
///
/// `amend` is the patch of an amend in flight, if any. Then nothing is adopted, and a quantity,
/// price or trigger matching neither `tracked` nor `tracked` with the patch applied is diverged.
/// A stand-in the engine tracks is not replaced while the amend is in flight either, nor counted
/// as diverged: a later report replaces it.
pub(crate) fn reconcile(
    tracked: &OrderTerms,
    reported: &OrderTerms,
    stated: ReportedTerms,
    amend: Option<&AmendPatch>,
) -> Reconciled {
    let adopted = adopt(tracked, reported, stated);
    let Some(patch) = amend else {
        return adopted;
    };

    let after = amended(tracked, patch);
    let unexplained = |tracked: bool, after: bool| !tracked && !after;
    let in_flight = ReportedTerms::NONE
        .with_quantity(unexplained(
            adopted.terms.quantity == tracked.quantity,
            adopted.terms.quantity == after.quantity,
        ))
        .with_price(unexplained(
            adopted.terms.price == tracked.price,
            adopted.terms.price == after.price,
        ))
        .with_kind(
            !tracked.kind.is_conditional_unresolved()
                && unexplained(
                    adopted.terms.kind == tracked.kind,
                    adopted.terms.kind == after.kind,
                ),
        );

    Reconciled {
        terms: *tracked,
        diverged: adopted.diverged.union(in_flight),
    }
}

/// [`reconcile`] with no amend in flight.
fn adopt(tracked: &OrderTerms, reported: &OrderTerms, stated: ReportedTerms) -> Reconciled {
    let mut terms = *tracked;
    let mut diverged = ReportedTerms::NONE;

    if stated.kind && !reported.kind.is_conditional_unresolved() {
        match reconcile_kind(&tracked.kind, &reported.kind) {
            KindOutcome::Same => {}
            KindOutcome::Adopt => terms.kind = reported.kind,
            KindOutcome::Diverged => diverged = diverged.with_kind(true),
        }
    }

    if stated.time_in_force
        && reported.time_in_force != TimeInForce::Unreported
        && reported.time_in_force != tracked.time_in_force
    {
        if tracked.time_in_force == TimeInForce::Unreported {
            terms.time_in_force = reported.time_in_force;
        } else {
            diverged = diverged.with_time_in_force(true);
        }
    }

    // Read against the kind the order now has, so a kind adopted from a stand-in brings its limit
    // with it. A kind without a fixed limit leaves the price unread: the engine may track a price
    // there that the venue does not hold, such as a market order's reference price.
    if stated.price && has_fixed_limit(&terms.kind) && reported.price != tracked.price {
        match reported.price {
            Some(_) => terms.price = reported.price,
            None => diverged = diverged.with_price(true),
        }
    }

    if stated.quantity {
        terms.quantity = reported.quantity;
    }

    Reconciled { terms, diverged }
}

/// What a reported kind does to a tracked one.
enum KindOutcome {
    /// No change the engine reads: equal, or differing only in a trailing order's moving prices.
    Same,
    /// Adopt the reported kind: a new fixed trigger, or the venue's kind for a stand-in.
    Adopt,
    /// A different order to the engine.
    Diverged,
}

fn reconcile_kind(tracked: &OrderKind, reported: &OrderKind) -> KindOutcome {
    use OrderKind::*;
    if tracked == reported {
        return KindOutcome::Same;
    }
    match (tracked, reported) {
        (ConditionalUnresolved { family, .. }, reported)
            if family_of(reported) == Some(*family) =>
        {
            KindOutcome::Adopt
        }
        (Stop { .. }, Stop { .. })
        | (StopLimit { .. }, StopLimit { .. })
        | (TakeProfit { .. }, TakeProfit { .. })
        | (TakeProfitLimit { .. }, TakeProfitLimit { .. }) => KindOutcome::Adopt,
        // The activation price may be restated once the stop trails.
        (
            TrailingStop {
                offset,
                offset_type,
                ..
            },
            TrailingStop {
                offset: reported_offset,
                offset_type: reported_type,
                ..
            },
        )
        | (
            TrailingStopFixedLimit {
                offset,
                offset_type,
                ..
            },
            TrailingStopFixedLimit {
                offset: reported_offset,
                offset_type: reported_type,
                ..
            },
        ) if offset == reported_offset && offset_type == reported_type => KindOutcome::Same,
        // The stop trails the market.
        (
            TrailingStopLimit {
                offset,
                offset_type,
                limit_offset,
                ..
            },
            TrailingStopLimit {
                offset: reported_offset,
                offset_type: reported_type,
                limit_offset: reported_limit_offset,
                ..
            },
        ) if offset == reported_offset
            && offset_type == reported_type
            && limit_offset == reported_limit_offset =>
        {
            KindOutcome::Same
        }
        _ => KindOutcome::Diverged,
    }
}

/// The [`ConditionalFamily`] a concrete kind belongs to, as
/// [`ConditionalUnresolved`](OrderKind::ConditionalUnresolved) names them.
fn family_of(kind: &OrderKind) -> Option<ConditionalFamily> {
    match kind {
        OrderKind::Stop { .. } | OrderKind::TrailingStop { .. } => Some(ConditionalFamily::Stop),
        OrderKind::StopLimit { .. }
        | OrderKind::TrailingStopFixedLimit { .. }
        | OrderKind::TrailingStopLimit { .. } => Some(ConditionalFamily::StopLimit),
        OrderKind::TakeProfit { .. } => Some(ConditionalFamily::TakeProfit),
        OrderKind::TakeProfitLimit { .. } => Some(ConditionalFamily::TakeProfitLimit),
        _ => None,
    }
}

/// Whether `kind` carries a limit price fixed when the order is placed, which a venue can change.
///
/// A [`TrailingStopLimit`](OrderKind::TrailingStopLimit)'s limit follows its stop, so it has none.
/// Nor, for this purpose, does a stand-in
/// [`ConditionalUnresolved`](OrderKind::ConditionalUnresolved): a family with a limit includes a
/// trailing stop-limit, so its price is read only once a report resolves the kind.
fn has_fixed_limit(kind: &OrderKind) -> bool {
    matches!(
        kind,
        OrderKind::Limit
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfitLimit { .. }
            | OrderKind::TrailingStopFixedLimit { .. }
    )
}

/// `terms` with `patch` applied, field by field, without [`AmendPatch::apply`]'s checks: what the
/// order would be if the venue made the amend.
fn amended(terms: &OrderTerms, patch: &AmendPatch) -> OrderTerms {
    let kind = match (patch.trigger_price, terms.kind) {
        (Some(trigger_price), OrderKind::Stop { .. }) => OrderKind::Stop { trigger_price },
        (Some(trigger_price), OrderKind::StopLimit { .. }) => {
            OrderKind::StopLimit { trigger_price }
        }
        (Some(trigger_price), OrderKind::TakeProfit { .. }) => {
            OrderKind::TakeProfit { trigger_price }
        }
        (Some(trigger_price), OrderKind::TakeProfitLimit { .. }) => {
            OrderKind::TakeProfitLimit { trigger_price }
        }
        (_, kind) => kind,
    };
    OrderTerms {
        price: patch.price.or(terms.price),
        quantity: patch.quantity.unwrap_or(terms.quantity),
        kind,
        time_in_force: terms.time_in_force,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_execution::order::TrailingOffsetType;

    const GTC: TimeInForce = TimeInForce::GoodUntilCancelled { post_only: false };
    const POST_ONLY: TimeInForce = TimeInForce::GoodUntilCancelled { post_only: true };

    fn terms(price: Option<Decimal>, quantity: Decimal, kind: OrderKind) -> OrderTerms {
        OrderTerms {
            price,
            quantity,
            kind,
            time_in_force: GTC,
        }
    }

    fn limit(price: Decimal, quantity: Decimal) -> OrderTerms {
        terms(Some(price), quantity, OrderKind::Limit)
    }

    fn stop(trigger_price: Decimal) -> OrderKind {
        OrderKind::Stop { trigger_price }
    }

    fn trailing_stop_limit(offset: Decimal, stop_price: Decimal) -> OrderKind {
        OrderKind::TrailingStopLimit {
            offset,
            offset_type: TrailingOffsetType::Absolute,
            stop_price,
            limit_offset: dec!(1),
        }
    }

    fn unresolved(family: ConditionalFamily) -> OrderKind {
        OrderKind::ConditionalUnresolved {
            family,
            trigger_price: Some(dec!(90)),
        }
    }

    /// The terms `terms` tracked, with nothing diverged.
    fn adopted(terms: OrderTerms) -> Reconciled {
        Reconciled {
            terms,
            diverged: ReportedTerms::NONE,
        }
    }

    #[test]
    fn a_stated_quantity_is_adopted_and_an_unstated_one_is_not_read() {
        let tracked = limit(dec!(100), dec!(2));
        let reported = limit(dec!(100), dec!(3));

        assert_eq!(
            reconcile(
                &tracked,
                &reported,
                ReportedTerms::NONE.with_quantity(true),
                None
            ),
            adopted(reported)
        );
        assert_eq!(
            reconcile(
                &tracked,
                &reported,
                ReportedTerms::ALL.with_quantity(false),
                None
            ),
            adopted(tracked)
        );
    }

    #[test]
    fn a_limit_price_is_adopted_only_for_a_kind_with_a_fixed_one() {
        let stated = ReportedTerms::NONE.with_price(true);

        let tracked = limit(dec!(100), dec!(2));
        let reported = limit(dec!(101), dec!(2));
        assert_eq!(
            reconcile(&tracked, &reported, stated, None),
            adopted(reported)
        );

        let fixed = OrderKind::TrailingStopFixedLimit {
            offset: dec!(5),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: None,
        };
        let tracked = terms(Some(dec!(100)), dec!(2), fixed);
        let reported = terms(Some(dec!(99)), dec!(2), fixed);
        assert_eq!(
            reconcile(&tracked, &reported, stated, None),
            adopted(reported)
        );

        // A market order's tracked reference price, or a trailing stop-limit's limit, which
        // follows its stop, is no venue term to compare.
        for kind in [OrderKind::Market, trailing_stop_limit(dec!(2), dec!(95))] {
            let tracked = terms(Some(dec!(100)), dec!(2), kind);
            let reported = terms(None, dec!(2), kind);
            assert_eq!(
                reconcile(&tracked, &reported, stated, None),
                adopted(tracked),
                "{kind}"
            );
        }
    }

    #[test]
    fn a_limit_price_reported_gone_is_diverged() {
        let tracked = limit(dec!(100), dec!(2));
        let reported = terms(None, dec!(2), OrderKind::Limit);

        assert_eq!(
            reconcile(
                &tracked,
                &reported,
                ReportedTerms::NONE.with_price(true),
                None
            ),
            Reconciled {
                terms: tracked,
                diverged: ReportedTerms::NONE.with_price(true),
            }
        );
    }

    #[test]
    fn a_trigger_is_adopted_within_its_kind_and_a_new_kind_is_diverged() {
        let stated = ReportedTerms::NONE.with_kind(true);
        let tracked = terms(None, dec!(2), stop(dec!(90)));

        let moved = terms(None, dec!(2), stop(dec!(91)));
        assert_eq!(reconcile(&tracked, &moved, stated, None), adopted(moved));

        for kind in [
            OrderKind::StopLimit {
                trigger_price: dec!(90),
            },
            OrderKind::Market,
            OrderKind::TakeProfit {
                trigger_price: dec!(90),
            },
        ] {
            assert_eq!(
                reconcile(&tracked, &terms(None, dec!(2), kind), stated, None),
                Reconciled {
                    terms: tracked,
                    diverged: stated,
                },
                "{kind}"
            );
        }
    }

    #[test]
    fn a_trailing_orders_moving_prices_are_ignored_and_its_offsets_are_not_adopted() {
        let stated = ReportedTerms::NONE.with_kind(true);

        let tracked = terms(None, dec!(2), trailing_stop_limit(dec!(2), dec!(95)));
        let trailed = terms(None, dec!(2), trailing_stop_limit(dec!(2), dec!(97)));
        assert_eq!(
            reconcile(&tracked, &trailed, stated, None),
            adopted(tracked)
        );
        let reoffset = terms(None, dec!(2), trailing_stop_limit(dec!(3), dec!(95)));
        assert_eq!(
            reconcile(&tracked, &reoffset, stated, None),
            Reconciled {
                terms: tracked,
                diverged: stated,
            }
        );

        let trailing_stop = |offset, activation_price| OrderKind::TrailingStop {
            offset,
            offset_type: TrailingOffsetType::Percentage,
            activation_price,
        };
        let tracked = terms(None, dec!(2), trailing_stop(dec!(1), Some(dec!(90))));
        let restated = terms(None, dec!(2), trailing_stop(dec!(1), Some(dec!(92))));
        assert_eq!(
            reconcile(&tracked, &restated, stated, None),
            adopted(tracked)
        );
        let reoffset = terms(None, dec!(2), trailing_stop(dec!(2), Some(dec!(90))));
        assert_eq!(
            reconcile(&tracked, &reoffset, stated, None),
            Reconciled {
                terms: tracked,
                diverged: stated,
            }
        );
    }

    #[test]
    fn a_stand_in_kind_takes_a_reported_kind_of_its_family_with_its_limit() {
        let stated = ReportedTerms::NONE.with_kind(true).with_price(true);
        let tracked = terms(
            Some(dec!(80)),
            dec!(2),
            unresolved(ConditionalFamily::StopLimit),
        );

        let resolved = terms(
            Some(dec!(81)),
            dec!(2),
            OrderKind::StopLimit {
                trigger_price: dec!(90),
            },
        );
        assert_eq!(
            reconcile(&tracked, &resolved, stated, None),
            adopted(resolved)
        );

        let other_family = terms(Some(dec!(80)), dec!(2), stop(dec!(90)));
        assert_eq!(
            reconcile(&tracked, &other_family, stated, None),
            Reconciled {
                terms: tracked,
                diverged: ReportedTerms::NONE.with_kind(true),
            }
        );
    }

    #[test]
    fn every_family_takes_its_own_kinds_only() {
        let stated = ReportedTerms::NONE.with_kind(true);
        let trailing_stop = OrderKind::TrailingStop {
            offset: dec!(1),
            offset_type: TrailingOffsetType::Percentage,
            activation_price: None,
        };
        let families = [
            (ConditionalFamily::Stop, vec![stop(dec!(90)), trailing_stop]),
            (
                ConditionalFamily::StopLimit,
                vec![
                    OrderKind::StopLimit {
                        trigger_price: dec!(90),
                    },
                    trailing_stop_limit(dec!(2), dec!(95)),
                ],
            ),
            (
                ConditionalFamily::TakeProfit,
                vec![OrderKind::TakeProfit {
                    trigger_price: dec!(90),
                }],
            ),
            (
                ConditionalFamily::TakeProfitLimit,
                vec![OrderKind::TakeProfitLimit {
                    trigger_price: dec!(90),
                }],
            ),
        ];

        for (family, kinds) in &families {
            let tracked = terms(None, dec!(2), unresolved(*family));
            for (other, other_kinds) in &families {
                for kind in other_kinds {
                    let reported = terms(None, dec!(2), *kind);
                    let expected = if family == other {
                        adopted(reported)
                    } else {
                        Reconciled {
                            terms: tracked,
                            diverged: stated,
                        }
                    };
                    assert_eq!(
                        reconcile(&tracked, &reported, stated, None),
                        expected,
                        "{family} given {kind}"
                    );
                }
            }
            assert!(!kinds.is_empty());
        }
    }

    /// A stand-in kind's family with a limit includes a trailing stop-limit, whose limit moves,
    /// so a price stated without resolving the kind is not read.
    #[test]
    fn a_stand_in_kinds_price_is_read_only_once_its_kind_is_resolved() {
        let tracked = terms(
            Some(dec!(80)),
            dec!(2),
            unresolved(ConditionalFamily::StopLimit),
        );
        let reported = terms(
            Some(dec!(81)),
            dec!(2),
            unresolved(ConditionalFamily::StopLimit),
        );

        for stated in [ReportedTerms::NONE.with_price(true), ReportedTerms::ALL] {
            assert_eq!(
                reconcile(&tracked, &reported, stated, None),
                adopted(tracked),
                "{stated:?}"
            );
        }
    }

    #[test]
    fn a_reported_stand_in_is_not_read_even_when_stated() {
        let tracked = terms(None, dec!(2), stop(dec!(90)));
        let reported = OrderTerms {
            time_in_force: TimeInForce::Unreported,
            ..terms(None, dec!(2), unresolved(ConditionalFamily::Stop))
        };

        assert_eq!(
            reconcile(&tracked, &reported, ReportedTerms::ALL, None),
            adopted(tracked)
        );
    }

    #[test]
    fn a_time_in_force_fills_a_stand_in_and_is_otherwise_diverged() {
        let stated = ReportedTerms::NONE.with_time_in_force(true);
        let reported = OrderTerms {
            time_in_force: POST_ONLY,
            ..limit(dec!(100), dec!(2))
        };

        let unreported = OrderTerms {
            time_in_force: TimeInForce::Unreported,
            ..limit(dec!(100), dec!(2))
        };
        assert_eq!(
            reconcile(&unreported, &reported, stated, None),
            adopted(reported)
        );

        let tracked = limit(dec!(100), dec!(2));
        assert_eq!(
            reconcile(&tracked, &reported, stated, None),
            Reconciled {
                terms: tracked,
                diverged: stated,
            }
        );
    }

    #[test]
    fn a_report_adopts_some_terms_and_leaves_others_diverged() {
        let tracked = limit(dec!(100), dec!(2));
        let reported = OrderTerms {
            time_in_force: TimeInForce::ImmediateOrCancel,
            ..limit(dec!(101), dec!(3))
        };

        assert_eq!(
            reconcile(&tracked, &reported, ReportedTerms::ALL, None),
            Reconciled {
                terms: limit(dec!(101), dec!(3)),
                diverged: ReportedTerms::NONE.with_time_in_force(true),
            }
        );
    }

    #[test]
    fn nothing_is_adopted_while_an_amend_is_in_flight_and_only_the_unexplained_is_diverged() {
        let tracked = terms(
            Some(dec!(100)),
            dec!(2),
            OrderKind::StopLimit {
                trigger_price: dec!(90),
            },
        );
        let patch = AmendPatch::default()
            .with_quantity(dec!(3))
            .with_price(dec!(101))
            .with_trigger_price(dec!(91));
        let after = amended(&tracked, &patch);
        let both = |quantity, price, trigger_price| {
            terms(
                Some(price),
                quantity,
                OrderKind::StopLimit { trigger_price },
            )
        };

        // Before the amend, after it, or a mix of the two: each term is explained by one side.
        for reported in [tracked, after, both(dec!(3), dec!(100), dec!(90))] {
            assert_eq!(
                reconcile(&tracked, &reported, ReportedTerms::ALL, Some(&patch)),
                adopted(tracked),
                "{reported:?}"
            );
        }

        let unexplained = both(dec!(4), dec!(102), dec!(92));
        assert_eq!(
            reconcile(&tracked, &unexplained, ReportedTerms::ALL, Some(&patch)),
            Reconciled {
                terms: tracked,
                diverged: ReportedTerms::NONE
                    .with_quantity(true)
                    .with_price(true)
                    .with_kind(true),
            }
        );
        // Unstated, nothing is compared.
        assert_eq!(
            reconcile(&tracked, &unexplained, ReportedTerms::NONE, Some(&patch)),
            adopted(tracked)
        );

        // An amend changes no time in force, so a new one is diverged as it is with no amend.
        let retimed = OrderTerms {
            time_in_force: POST_ONLY,
            ..after
        };
        assert_eq!(
            reconcile(&tracked, &retimed, ReportedTerms::ALL, Some(&patch)),
            Reconciled {
                terms: tracked,
                diverged: ReportedTerms::NONE.with_time_in_force(true),
            }
        );
    }

    #[test]
    fn a_stand_in_is_neither_replaced_nor_diverged_while_an_amend_is_in_flight() {
        let tracked = OrderTerms {
            time_in_force: TimeInForce::Unreported,
            ..terms(
                Some(dec!(80)),
                dec!(2),
                unresolved(ConditionalFamily::StopLimit),
            )
        };
        let reported = terms(
            Some(dec!(80)),
            dec!(2),
            OrderKind::StopLimit {
                trigger_price: dec!(90),
            },
        );
        let patch = AmendPatch::default().with_quantity(dec!(3));

        assert_eq!(
            reconcile(&tracked, &reported, ReportedTerms::ALL, Some(&patch)),
            adopted(tracked)
        );
    }
}
