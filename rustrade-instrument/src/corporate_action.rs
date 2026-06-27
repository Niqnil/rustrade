use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A validated stock-split ratio: strictly positive (`> 0`).
///
/// `SplitRatio` makes a degenerate ratio **unconstructible**. Every value is `> 0`, so the engine's
/// split arithmetic (`Position::apply_split` in the `rustrade` crate and the
/// `EngineEvent::CorporateAction` handler) can never receive a zero or negative ratio *through the
/// type system* — the failure mode is moved from a runtime panic to a compile-time guarantee.
///
/// Construct with [`SplitRatio::new`] (returns `Option`), [`SplitRatio::try_from`] (returns a typed
/// [`InvalidSplitRatio`] error, for `?`-propagation), or via [`CorporateActionKind::stock_split`]
/// which builds one from a provider's `split_to` / `split_from` pair. Read the inner value with
/// [`SplitRatio::get`].
///
/// # Serde
/// Serializes **transparently** as its inner [`Decimal`] (the wire format is a JSON string — the
/// same format a raw [`Decimal`] field produces — so persisted [`CorporateAction`] / `EngineEvent`
/// payloads are unchanged). Deserialization is
/// **validated**: a non-positive ratio on the wire is a hard deserialization error, so a
/// persisted/replayed event can never reintroduce a degenerate ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SplitRatio(Decimal);

impl SplitRatio {
    /// Construct a `SplitRatio`, returning `Some` iff `ratio` is strictly positive (`> 0`).
    ///
    /// Returns `None` for zero or negative input — the same degeneracy
    /// [`CorporateActionKind::stock_split`] rejects, enforced here at the type boundary.
    #[must_use]
    pub fn new(ratio: Decimal) -> Option<Self> {
        (ratio > Decimal::ZERO).then_some(Self(ratio))
    }

    /// The inner ratio value, always `> 0`.
    #[must_use]
    pub const fn get(self) -> Decimal {
        self.0
    }
}

impl From<SplitRatio> for Decimal {
    /// Infallible downcast: every [`SplitRatio`] is a valid [`Decimal`]. The inverse of the
    /// validated [`TryFrom<Decimal>`] upcast — prefer `.into()` / `Decimal::from(ratio)` over
    /// [`SplitRatio::get`] in generic (`impl Into<Decimal>`) contexts.
    fn from(ratio: SplitRatio) -> Self {
        ratio.0
    }
}

impl std::fmt::Display for SplitRatio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Error from constructing a [`SplitRatio`] from a non-positive [`Decimal`].
///
/// Carries the rejected value so callers (and serde's deserializer) can report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSplitRatio(Decimal);

impl InvalidSplitRatio {
    /// The rejected (non-positive) [`Decimal`] that failed the `> 0` invariant.
    #[must_use]
    pub const fn rejected(self) -> Decimal {
        self.0
    }
}

impl std::fmt::Display for InvalidSplitRatio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "split ratio must be strictly positive, got {}", self.0)
    }
}

impl std::error::Error for InvalidSplitRatio {}

impl TryFrom<Decimal> for SplitRatio {
    type Error = InvalidSplitRatio;

    /// Fallible construction for `?`-propagation. Mirrors [`SplitRatio::new`]'s `> 0` rule but
    /// returns the rejected value in [`InvalidSplitRatio`] instead of discarding it (`None`).
    fn try_from(ratio: Decimal) -> Result<Self, Self::Error> {
        SplitRatio::new(ratio).ok_or(InvalidSplitRatio(ratio))
    }
}

// Validated deserialization: read the inner `Decimal`, then enforce the `> 0` invariant via
// `TryFrom` (the single source of validation logic). A hand-written impl is still required because
// `#[serde(transparent)]` `Serialize` and `#[serde(try_from)]` `Deserialize` cannot coexist on one
// derive — this keeps the transparent wire format while reusing `TryFrom`'s check.
impl<'de> Deserialize<'de> for SplitRatio {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ratio = <Decimal as Deserialize>::deserialize(deserializer)?;
        SplitRatio::try_from(ratio).map_err(serde::de::Error::custom)
    }
}

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
        /// `split_to / split_from`, as a validated [`SplitRatio`] (always `> 0`). See the variant
        /// docs for the forward/reverse convention.
        ratio: SplitRatio,
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
    /// Returns `None` for **degenerate** inputs rather than panicking or fabricating a nonsensical
    /// action. Both share counts must be strictly positive: a non-positive `split_to` or
    /// `split_from` (zero or negative) is rejected up front. This also closes the both-negative
    /// normalization hole, where two negative components would divide to a positive quotient (e.g.
    /// `-2 / -1 = 2`) and otherwise slip through. The resulting quotient is then re-validated as a
    /// [`SplitRatio`] (`> 0`). Callers should treat `None` as bad source data and log + skip it, per
    /// the library's "observable failures over silent ones" principle.
    ///
    /// Note: `ratio` is computed at `rust_decimal`'s 28-significant-digit precision, so an inexact
    /// quotient (e.g. 1-for-3) is rounded there — identical to how the engine's `apply_split`
    /// consumes it.
    #[must_use]
    pub fn stock_split(split_to: Decimal, split_from: Decimal) -> Option<Self> {
        let positive = |d: Decimal| d > Decimal::ZERO;
        if !positive(split_to) || !positive(split_from) {
            return None;
        }
        split_to
            .checked_div(split_from)
            .and_then(SplitRatio::new)
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
                let ratio = ratio.get();
                Some(if ratio > Decimal::ONE && ratio.fract().is_zero() {
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

    /// Build a `StockSplit` from a raw ratio for assertions (panics on a non-positive ratio, which
    /// is fine in test code).
    fn split_kind_with(ratio: Decimal) -> CorporateActionKind {
        CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(ratio).unwrap(),
        }
    }

    #[test]
    fn stock_split_computes_ratio_from_components() {
        // Forward 2-for-1.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(2), Decimal::from(1)),
            Some(split_kind_with(Decimal::from(2)))
        );
        // Reverse 1-for-10 → 0.1.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(1), Decimal::from(10)),
            Some(split_kind_with(Decimal::new(1, 1)))
        );
        // Inexact 3-for-2 → 1.5.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(3), Decimal::from(2)),
            Some(split_kind_with(Decimal::new(15, 1)))
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
        // Single negative component.
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(-2), Decimal::from(1)),
            None
        );
        // Both-negative components: -2 / -1 = 2 is positive and would slip past a quotient-only
        // check, but the component guard rejects it (no real provider reports negative share counts).
        assert_eq!(
            CorporateActionKind::stock_split(Decimal::from(-2), Decimal::from(-1)),
            None
        );
    }

    #[test]
    fn split_ratio_rejects_non_positive() {
        assert!(SplitRatio::new(Decimal::from(2)).is_some());
        assert!(SplitRatio::new(Decimal::new(1, 1)).is_some()); // 0.1
        assert!(SplitRatio::new(Decimal::ZERO).is_none());
        assert!(SplitRatio::new(Decimal::from(-1)).is_none());
        assert_eq!(
            SplitRatio::new(Decimal::from(2)).unwrap().get(),
            Decimal::from(2)
        );
        // `From<SplitRatio> for Decimal` is the infallible inverse of `get()`.
        assert_eq!(
            Decimal::from(SplitRatio::new(Decimal::from(2)).unwrap()),
            Decimal::from(2)
        );
    }

    #[test]
    fn split_ratio_try_from_matches_new_and_carries_rejected_value() {
        // Same `> 0` rule as `new`, but `?`-friendly and the error carries the rejected value.
        assert_eq!(
            SplitRatio::try_from(Decimal::from(2)).unwrap().get(),
            Decimal::from(2)
        );
        assert_eq!(
            SplitRatio::try_from(Decimal::from(-3)),
            Err(InvalidSplitRatio(Decimal::from(-3)))
        );
        assert_eq!(
            SplitRatio::try_from(Decimal::ZERO),
            Err(InvalidSplitRatio(Decimal::ZERO))
        );
    }

    #[test]
    fn split_ratio_serde_roundtrips_and_validates() {
        // Serializes transparently as a JSON string — the same wire format a raw `Decimal`
        // produces (wire format unchanged).
        let ratio = SplitRatio::new(Decimal::from(2)).unwrap();
        let json = serde_json::to_string(&ratio).unwrap();
        assert_eq!(json, "\"2\"");
        assert_eq!(serde_json::from_str::<SplitRatio>(&json).unwrap(), ratio);

        // A fractional positive ratio (e.g. a 1-for-2 reverse split = 0.5) round-trips through the
        // validating `Deserialize` impl — the `> 0` re-check accepts sub-integer ratios.
        let frac = SplitRatio::new(Decimal::new(5, 1)).unwrap(); // 0.5
        let frac_json = serde_json::to_string(&frac).unwrap();
        assert_eq!(
            serde_json::from_str::<SplitRatio>(&frac_json).unwrap(),
            frac
        );

        // A non-positive ratio on the wire is rejected at deserialization — a persisted/replayed
        // event can never reintroduce a degenerate ratio.
        assert!(serde_json::from_str::<SplitRatio>("\"0\"").is_err());
        assert!(serde_json::from_str::<SplitRatio>("\"-1\"").is_err());
    }

    #[test]
    fn split_kind_classifies_per_occ_rules() {
        use SplitAdjustmentKind::{NonStandard, Standard};
        let split = split_kind_with;

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
        let kind = split_kind_with(Decimal::from(4));
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
