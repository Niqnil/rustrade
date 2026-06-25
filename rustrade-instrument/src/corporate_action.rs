use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A corporate-action market fact — *what* happened to an instrument, independent of any
/// account, broker, or rounding policy.
///
/// This type carries **market facts only**. It deliberately contains no rounding policy, no
/// account context, and no resolved timestamp: a data source emitting a split knows the ratio,
/// not how a particular broker will round fractional shares or when a particular engine should
/// stamp the adjustment. Those concerns live with the consumer (see the engine-side
/// `SplitRoundingPolicy` and the `effective_time` resolved via [`split_effective_instant`]).
///
/// `#[non_exhaustive]`: further corporate actions (dividends, spin-offs, symbol changes, …) can
/// be added without breaking downstream exhaustive matches.
// `Ord`/`PartialOrd`/`Hash` are derived (over and above `Eq`) so this type can be embedded in
// engine outputs such as `EngineOutput::UnsupportedCorporateAction`, which derive those traits.
// `rust_decimal::Decimal` implements all three, so the derives are sound for `StockSplit`.
// MAINTAINER NOTE: every future variant must use field types whose `Ord`/`Hash` stay consistent
// with `Eq` (e.g. no `f32`/`f64`, which lack `Ord`); otherwise these derives must be dropped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum CorporateActionKind {
    /// A forward or reverse stock split, expressed as a single multiplicative `ratio`.
    ///
    /// `ratio = split_to / split_from`:
    /// - **Forward** split → `ratio > 1` (e.g. a 2-for-1 split is `2.0`; share count scales up,
    ///   per-share price scales down).
    /// - **Reverse** split → `ratio < 1` (e.g. a 1-for-10 reverse split is `0.1`; share count
    ///   scales down, per-share price scales up).
    StockSplit {
        /// `split_to / split_from`. See the variant docs for the forward/reverse convention.
        ratio: Decimal,
    },
}

impl CorporateActionKind {
    /// Construct a [`StockSplit`](CorporateActionKind::StockSplit) from a provider's raw
    /// `split_to` / `split_from` pair, computing `ratio = split_to / split_from` **once, in one
    /// place**, so every data source derives the ratio identically (no per-provider arithmetic).
    ///
    /// Providers report a split as two share counts — shares *after* the split (`split_to`) over
    /// shares *before* (`split_from`): a 2-for-1 forward split is `split_to = 2, split_from = 1`
    /// (`ratio = 2`); a 1-for-10 reverse split is `split_to = 1, split_from = 10` (`ratio = 0.1`).
    ///
    /// Returns `None` for **degenerate** inputs — a `split_from` of zero (division by zero) or any
    /// non-positive `ratio` (including a `split_to` of zero) — rather than panicking or fabricating
    /// a nonsensical action. Callers should treat `None` as bad source data and log + skip it, per
    /// the library's "observable failures over silent ones" principle.
    ///
    /// Note: `ratio` is computed at `rust_decimal`'s 28-significant-digit precision, so an inexact
    /// quotient (e.g. 1-for-3) is rounded there — identical to how the engine's `apply_split`
    /// consumes it.
    #[must_use]
    pub fn stock_split(split_to: Decimal, split_from: Decimal) -> Option<Self> {
        split_to
            .checked_div(split_from)
            .filter(|ratio| ratio.is_sign_positive() && !ratio.is_zero())
            .map(|ratio| Self::StockSplit { ratio })
    }

    /// Classify how this action affects listed **options** on the underlying, per the OCC
    /// option-adjustment rules (OCC By-Laws Article VI, §11):
    /// - `Some(`[`Standard`](SplitAdjustmentKind::Standard)`)` — a whole-number forward split, which
    ///   the engine adjusts in place;
    /// - `Some(`[`NonStandard`](SplitAdjustmentKind::NonStandard)`)` — every reverse split, every
    ///   *fractional* forward split, and the `ratio == 1` no-op, none of which the engine can adjust
    ///   in place;
    /// - `None` — this action is **not a split** and so has no split classification (a future
    ///   dividend, spin-off, …).
    ///
    /// A `StockSplit`'s `ratio` is **standard** iff it is a whole number strictly greater than one
    /// (`ratio > 1 && ratio.fract() == 0`). A non-positive `ratio` cannot occur:
    /// [`CorporateActionKind::stock_split`] rejects it at construction and `Position::apply_split`
    /// asserts it, so this is only ever reached with a positive `ratio`.
    #[must_use]
    pub fn split_kind(&self) -> Option<SplitAdjustmentKind> {
        // No `_` catch-all: `CorporateActionKind` is `#[non_exhaustive]`, so adding a future
        // non-split variant (dividend, spin-off, …) makes this match fail to compile, forcing an
        // explicit `None` mapping here rather than silently classifying it as `NonStandard`. The
        // `Option` return already expresses "not a split ⇒ no split kind".
        match self {
            Self::StockSplit { ratio } => {
                Some(if *ratio > Decimal::ONE && ratio.fract().is_zero() {
                    SplitAdjustmentKind::Standard
                } else {
                    SplitAdjustmentKind::NonStandard
                })
            }
        }
    }
}

/// How the OCC adjusts listed **options** on an underlying that undergoes a stock split (OCC
/// By-Laws Article VI, §11) — the classification returned by [`CorporateActionKind::split_kind`].
///
/// The two cases demand opposite handling, so the engine branches on them when a split targets an
/// underlying that has open option positions. A downstream wrapper that PULL-sources corporate
/// actions can use the same classification to decide how to react *before* injecting the event.
///
/// `#[non_exhaustive]`: were the OCC rule ever to gain a further category, it can be added without
/// breaking downstream exhaustive matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum SplitAdjustmentKind {
    /// A *whole-number forward* split (2-for-1, 3-for-1, …). The option survives with the **same**
    /// contract identity: strike is divided by `ratio`, contract count is multiplied by `ratio`,
    /// and the deliverable/multiplier stays 100. A purely mechanical adjustment that keeps the
    /// existing position valid, so the engine applies it in place.
    Standard,
    /// Every *reverse* split (always `ratio < 1`), every *fractional* forward split (e.g. 3-for-2 →
    /// `1.5`), and the `ratio == 1` no-op. The OCC changes the deliverable and assigns a **new**
    /// option symbol (e.g. `MSFT` → `MSFT1`), destroying the instrument identity. No mechanical
    /// in-place adjustment keeps the position valid, so the response is a downstream policy decision.
    NonStandard,
}

/// Resolve a **stock split**'s *effective date* (a calendar `NaiveDate` market fact) to the exact
/// `DateTime<Utc>` instant at which the adjustment takes effect: **midnight (00:00) UTC** on that
/// date.
///
/// # Why midnight UTC
///
/// This is a convenience default tuned for **US equities**, where corporate actions take effect
/// at the start of the effective session. Midnight UTC falls in the overnight gap after the prior
/// session's close and before the effective session's open, so when the resulting instant is used
/// as a merge-sort key against intraday market events, the adjustment lands exactly where a broker
/// applies it — after the previous day's trading, before the effective day's first print. This
/// avoids look-ahead (the split is not applied to prior-session events) without skipping into the
/// effective session's data.
///
/// # When to override
///
/// Exchanges whose sessions are not naturally bracketed by midnight UTC (non-UTC venues) may need
/// a different resolution. Callers owning that context should construct the `DateTime<Utc>`
/// themselves rather than relying on this default.
///
/// This instant doubles as the **sort key** when interleaving a corporate-action event into a
/// time-ordered backtest replay stream.
pub fn split_effective_instant(date: NaiveDate) -> DateTime<Utc> {
    date.and_time(NaiveTime::MIN).and_utc()
}

/// A corporate-action market fact **bound to a specific instrument**, as yielded by a PULL
/// reference-data source (see `StockSplitSource` in `rustrade-integration`).
///
/// Generic over the instrument key `K` so the *same* descriptor serves both ends of the sourcing
/// pipeline: a **source** yields `CorporateAction<Symbol>` carrying an unresolved provider ticker
/// (e.g. `CorporateAction<SmolStr>`), and a **wrapper** resolves that symbol to the engine's
/// instrument key, producing `CorporateAction<InstrumentKey>` before constructing the engine event.
///
/// This is a *fact*, not an engine event: it carries **no rounding policy** (the source does not
/// know the account/broker) and **no resolved `effective_time` instant**. `effective_date` is the
/// calendar date the action takes effect (the market fact; `None` if the source omits it);
/// resolving it to the `DateTime<Utc>` the engine stamps is the caller's job via
/// [`split_effective_instant`].
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Constructor, Deserialize, Serialize,
)]
pub struct CorporateAction<K> {
    /// The instrument the action applies to — a provider symbol at the source boundary, an engine
    /// instrument key after the wrapper's resolution.
    pub instrument: K,
    /// The corporate-action market fact (e.g. a stock-split ratio).
    pub kind: CorporateActionKind,
    /// The calendar date the action takes effect, if the source supplies one. Resolve it to the
    /// engine's stamping instant with [`split_effective_instant`].
    pub effective_date: Option<NaiveDate>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn split_effective_instant_is_midnight_utc_on_the_date() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 22).unwrap();
        let instant = split_effective_instant(date);

        assert_eq!(instant.date_naive(), date);
        assert_eq!(instant.time(), NaiveTime::MIN);
        assert_eq!(instant.naive_utc().year(), 2026);
        assert_eq!(instant.to_rfc3339(), "2026-06-22T00:00:00+00:00");
    }

    #[test]
    fn stock_split_computes_ratio_from_components() {
        // Forward 2-for-1.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(2), Decimal::from(1)),
            Some(CorporateActionKind::StockSplit {
                ratio: Decimal::from(2)
            })
        );
        // Reverse 1-for-10 → 0.1.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(1), Decimal::from(10)),
            Some(CorporateActionKind::StockSplit {
                ratio: Decimal::new(1, 1)
            })
        );
        // Inexact 3-for-2 → 1.5.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(3), Decimal::from(2)),
            Some(CorporateActionKind::StockSplit {
                ratio: Decimal::new(15, 1)
            })
        );
    }

    #[test]
    fn stock_split_rejects_degenerate_components() {
        // Division by zero (split_from == 0).
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(1), Decimal::ZERO),
            None
        );
        // Zero ratio (split_to == 0).
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::ZERO, Decimal::from(5)),
            None
        );
        // Non-positive ratio (negative component).
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(-2), Decimal::from(1)),
            None
        );
    }

    #[test]
    fn split_kind_classifies_per_occ_rules() {
        use SplitAdjustmentKind::{NonStandard, Standard};
        let split = |ratio| CorporateActionKind::StockSplit { ratio };

        // Standard: whole-number forward splits.
        assert_eq!(split(Decimal::from(2)).split_kind(), Some(Standard)); // 2-for-1
        assert_eq!(split(Decimal::from(3)).split_kind(), Some(Standard)); // 3-for-1
        assert_eq!(split(Decimal::from(10)).split_kind(), Some(Standard)); // 10-for-1

        // Standard regardless of internal scale: a whole number carried at non-zero scale (2.0,
        // 3.00) is still standard. Guards the `fract().is_zero()` classifier against a future
        // `scale() == 0` regression that would misread these as non-standard.
        assert_eq!(split(Decimal::new(20, 1)).split_kind(), Some(Standard)); // 2.0
        assert_eq!(split(Decimal::new(300, 2)).split_kind(), Some(Standard)); // 3.00

        // Non-standard: fractional forward splits.
        assert_eq!(split(Decimal::new(15, 1)).split_kind(), Some(NonStandard)); // 3-for-2 = 1.5
        assert_eq!(split(Decimal::new(43, 10)).split_kind(), Some(NonStandard)); // odd fractional

        // Non-standard: every reverse split (ratio < 1).
        assert_eq!(split(Decimal::new(5, 1)).split_kind(), Some(NonStandard)); // 1-for-2 = 0.5
        assert_eq!(split(Decimal::new(1, 1)).split_kind(), Some(NonStandard)); // 1-for-10 = 0.1

        // Boundary: a 1-for-1 no-op is not a standard adjustment to apply.
        assert_eq!(split(Decimal::ONE).split_kind(), Some(NonStandard));
    }

    #[test]
    fn corporate_action_descriptor_is_generic_over_the_key() {
        let kind = CorporateActionKind::StockSplit {
            ratio: Decimal::from(4),
        };
        let date = NaiveDate::from_ymd_opt(2020, 8, 31);

        // Source boundary: keyed by an unresolved provider symbol.
        let sourced = CorporateAction::new("AAPL", kind.clone(), date);
        assert_eq!(sourced.instrument, "AAPL");
        assert_eq!(sourced.effective_date, date);

        // Wrapper re-keys the same fact to an engine-style index.
        let resolved = CorporateAction::new(0_usize, sourced.kind.clone(), sourced.effective_date);
        assert_eq!(resolved.instrument, 0_usize);
        assert_eq!(resolved.kind, kind);
    }
}
