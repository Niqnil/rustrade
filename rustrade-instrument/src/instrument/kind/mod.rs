use crate::instrument::{
    kind::{
        cfd::CfdContract, future::FutureContract, option::OptionContract,
        perpetual::PerpetualContract,
    },
    market_data::kind::{
        MarketDataFutureContract, MarketDataInstrumentKind, MarketDataOptionContract,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Defines an [`PerpetualContract`].
pub mod perpetual;

/// Defines an [`FutureContract`].
pub mod future;

/// Defines an [`OptionContract`].
pub mod option;

/// Defines a [`CfdContract`].
pub mod cfd;

/// [`Instrument`](super::Instrument) kind, one of `Spot`, `Perpetual`, `Future`, `Option` and
/// `Cfd`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind<AssetKey> {
    Spot,
    Perpetual(PerpetualContract<AssetKey>),
    Future(FutureContract<AssetKey>),
    Option(OptionContract<AssetKey>),
    /// Contract-for-difference — an OTC contract on an underlying's price movement.
    ///
    /// Distinct from `Spot` because a CFD is **not deliverable**: it confers no claim on the
    /// underlying, carries no venue attribution, and may be priced off the broker's own series
    /// rather than any exchange's book. Mapping CFDs to `Spot` would pollute every downstream
    /// `Spot` filter — including the corporate-action and option-settlement scans, which mean
    /// "the deliverable equity" — with instruments that cannot satisfy those meanings.
    Cfd(CfdContract<AssetKey>),
}

/// Names an [`InstrumentKind`] variant without its contract payload.
///
/// [`InstrumentKind`] carries per-contract data (`contract_size`, `settlement_asset`, expiry,
/// strike) and is generic over `AssetKey`, so it can be neither named in a `const` nor compared
/// without first constructing a contract. This is the payload-free spelling of *which* kind, which
/// is what a venue needs in order to declare the kinds it can trade as a `const` capability set —
/// see `ExecutionClient::SUPPORTED_KINDS` in `rustrade-execution`.
///
/// Deliberately distinct from [`MarketDataInstrumentKind`], which is not payload-free: its `Future`
/// and `Option` variants carry the expiry and strike that bind a subscription. That type answers
/// "which contract is this stream for"; this one answers "which family is this instrument".
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKindDiscriminant {
    Spot,
    Perpetual,
    Future,
    Option,
    Cfd,
}

impl Display for InstrumentKindDiscriminant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            InstrumentKindDiscriminant::Spot => "spot",
            InstrumentKindDiscriminant::Perpetual => "perpetual",
            InstrumentKindDiscriminant::Future => "future",
            InstrumentKindDiscriminant::Option => "option",
            InstrumentKindDiscriminant::Cfd => "cfd",
        })
    }
}

impl<AssetKey> InstrumentKind<AssetKey> {
    /// Returns the [`InstrumentKindDiscriminant`] naming this variant, discarding contract detail.
    pub fn discriminant(&self) -> InstrumentKindDiscriminant {
        match self {
            InstrumentKind::Spot => InstrumentKindDiscriminant::Spot,
            InstrumentKind::Perpetual(_) => InstrumentKindDiscriminant::Perpetual,
            InstrumentKind::Future(_) => InstrumentKindDiscriminant::Future,
            InstrumentKind::Option(_) => InstrumentKindDiscriminant::Option,
            InstrumentKind::Cfd(_) => InstrumentKindDiscriminant::Cfd,
        }
    }

    /// Returns the `contract_size` value for the `InstrumentKind`.
    ///
    /// Note that `Spot` is always `Decimal::ONE`.
    pub fn contract_size(&self) -> Decimal {
        match self {
            InstrumentKind::Spot => Decimal::ONE,
            InstrumentKind::Perpetual(kind) => kind.contract_size,
            InstrumentKind::Future(kind) => kind.contract_size,
            InstrumentKind::Option(kind) => kind.contract_size,
            InstrumentKind::Cfd(kind) => kind.contract_size,
        }
    }

    /// Returns the contract expiry instant for expiring kinds (`Future` & `Option`), and `None`
    /// for the non-expiring `Spot`, `Perpetual` & `Cfd` kinds.
    pub fn expiry(&self) -> Option<DateTime<Utc>> {
        match self {
            InstrumentKind::Spot | InstrumentKind::Perpetual(_) | InstrumentKind::Cfd(_) => None,
            InstrumentKind::Future(kind) => Some(kind.expiry),
            InstrumentKind::Option(kind) => Some(kind.expiry),
        }
    }

    /// For `Perpetual`, `Future`, `Option` & `Cfd` variants of [`Self`], returns the settlement
    /// `AssetKey`, and `None` for Spot.
    pub fn settlement_asset(&self) -> Option<&AssetKey> {
        match self {
            InstrumentKind::Spot => None,
            InstrumentKind::Perpetual(kind) => Some(&kind.settlement_asset),
            InstrumentKind::Future(kind) => Some(&kind.settlement_asset),
            InstrumentKind::Option(kind) => Some(&kind.settlement_asset),
            InstrumentKind::Cfd(kind) => Some(&kind.settlement_asset),
        }
    }

    /// Determines whether an equity corporate action (eg/ a stock split) may target an
    /// [`Instrument`](super::Instrument) of this kind.
    ///
    /// # The rule
    /// **Only the deliverable equity itself.** `Spot` is today's only spelling of that, but the
    /// rule is the deliverable equity — not the `Spot` variant — so judge a new variant against
    /// the rule rather than copying the current arm.
    ///
    /// Derivative kinds are excluded because equity-split arithmetic is invalid for them: an
    /// option on the splitting underlying is adjusted as part of processing the *equity's* action,
    /// never as a target in its own right.
    ///
    /// `Cfd` is excluded, and this one is load-bearing rather than conservative. Split arithmetic
    /// reads neither `kind` nor `contract_size`, so a split "applied" to a CFD would be committed
    /// and recorded, not rejected: positions rescaled and basis divided with no warning. And
    /// [`CfdContract`] carries no underlying-asset-class discriminant, so this predicate could not
    /// separate an equity CFD (where a split is meaningful) from an index, commodity, rates or
    /// volatility CFD (where it is nonsense) even if it wanted to. Admitting `Cfd` would convert
    /// an obviously invalid event into a silently committed mutation.
    ///
    /// Rejection is also the reversible choice: the caller's rejection path deliberately does not
    /// record the action id, so a rejected action replays cleanly if support is ever widened.
    pub fn is_split_eligible(&self) -> bool {
        match self {
            InstrumentKind::Spot => true,
            InstrumentKind::Perpetual(_)
            | InstrumentKind::Future(_)
            | InstrumentKind::Option(_)
            | InstrumentKind::Cfd(_) => false,
        }
    }

    /// Determines if the provided [`MarketDataInstrumentKind`] is equivalent to [`Self`] (ignores
    /// settlement asset).
    ///
    /// # Adding an `InstrumentKind` variant
    /// This matches exhaustively on `self`, with the wildcard confined to each arm's inner
    /// comparison, **deliberately**: a new variant must produce a compile error here rather than
    /// fall through to `false`. This function is the binding key between an `Instrument` and its
    /// market-data subscription, so an arm that answered `false` where it should answer `true`
    /// would leave the subscription unmatchable against any registered instrument.
    ///
    /// That failure is currently *loud* — the sole caller (`rustrade-data`'s indexed dynamic
    /// stream builder) turns an unmatched subscription into `IndexError::InstrumentIndex`, which
    /// propagates as a `DataError` rather than dropping the subscription silently. The exhaustive
    /// match is kept regardless: the caller's loudness is that caller's property, not this
    /// function's, and a future caller that resolves subscriptions by filtering would have no way
    /// to tell "no such instrument" from "this predicate forgot a variant".
    pub fn eq_market_data_instrument_kind(&self, other: &MarketDataInstrumentKind) -> bool {
        match self {
            Self::Spot => matches!(other, MarketDataInstrumentKind::Spot),
            Self::Perpetual(_) => matches!(other, MarketDataInstrumentKind::Perpetual),
            Self::Cfd(_) => matches!(other, MarketDataInstrumentKind::Cfd),
            Self::Future(contract) => matches!(
                other,
                MarketDataInstrumentKind::Future(other_contract)
                    if contract.expiry == other_contract.expiry
            ),
            Self::Option(contract) => matches!(
                other,
                MarketDataInstrumentKind::Option(other_contract)
                    if contract.kind == other_contract.kind
                        && contract.exercise == other_contract.exercise
                        && contract.expiry == other_contract.expiry
                        && contract.strike == other_contract.strike
            ),
        }
    }
}

impl<AssetKey> From<InstrumentKind<AssetKey>> for MarketDataInstrumentKind {
    fn from(value: InstrumentKind<AssetKey>) -> Self {
        match value {
            InstrumentKind::Spot => MarketDataInstrumentKind::Spot,
            InstrumentKind::Perpetual(_) => MarketDataInstrumentKind::Perpetual,
            InstrumentKind::Cfd(_) => MarketDataInstrumentKind::Cfd,
            InstrumentKind::Future(contract) => {
                MarketDataInstrumentKind::Future(MarketDataFutureContract {
                    expiry: contract.expiry,
                })
            }
            InstrumentKind::Option(contract) => {
                MarketDataInstrumentKind::Option(MarketDataOptionContract {
                    kind: contract.kind,
                    exercise: contract.exercise,
                    expiry: contract.expiry,
                    strike: contract.strike,
                })
            }
        }
    }
}

impl<AssetKey> From<&InstrumentKind<AssetKey>> for MarketDataInstrumentKind {
    fn from(value: &InstrumentKind<AssetKey>) -> Self {
        match value {
            InstrumentKind::Spot => MarketDataInstrumentKind::Spot,
            InstrumentKind::Perpetual(_) => MarketDataInstrumentKind::Perpetual,
            InstrumentKind::Cfd(_) => MarketDataInstrumentKind::Cfd,
            InstrumentKind::Future(contract) => {
                MarketDataInstrumentKind::Future(MarketDataFutureContract {
                    expiry: contract.expiry,
                })
            }
            InstrumentKind::Option(contract) => {
                MarketDataInstrumentKind::Option(MarketDataOptionContract {
                    kind: contract.kind,
                    exercise: contract.exercise,
                    expiry: contract.expiry,
                    strike: contract.strike,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::kind::option::{OptionExercise, OptionKind};
    use chrono::TimeZone;

    fn expiry(day: u32) -> DateTime<Utc> {
        #[allow(clippy::unwrap_used)] // Test code: a hardcoded valid date cannot fail.
        Utc.with_ymd_and_hms(2026, 7, day, 0, 0, 0).unwrap()
    }

    fn spot() -> InstrumentKind<&'static str> {
        InstrumentKind::Spot
    }

    fn cfd() -> InstrumentKind<&'static str> {
        InstrumentKind::Cfd(CfdContract {
            contract_size: Decimal::ONE,
            settlement_asset: "usd",
        })
    }

    fn perpetual() -> InstrumentKind<&'static str> {
        InstrumentKind::Perpetual(PerpetualContract {
            contract_size: Decimal::ONE,
            settlement_asset: "usdt",
        })
    }

    fn future(day: u32) -> InstrumentKind<&'static str> {
        InstrumentKind::Future(FutureContract {
            contract_size: Decimal::ONE,
            settlement_asset: "usdt",
            expiry: expiry(day),
        })
    }

    fn option(day: u32, strike: Decimal) -> InstrumentKind<&'static str> {
        InstrumentKind::Option(OptionContract {
            contract_size: Decimal::ONE,
            settlement_asset: "usdt",
            kind: OptionKind::Call,
            exercise: OptionExercise::European,
            expiry: expiry(day),
            strike,
        })
    }

    #[test]
    fn test_eq_market_data_instrument_kind_matches_own_twin() {
        for kind in [
            spot(),
            cfd(),
            perpetual(),
            future(1),
            option(1, Decimal::ONE),
        ] {
            let twin = MarketDataInstrumentKind::from(&kind);
            assert!(
                kind.eq_market_data_instrument_kind(&twin),
                "{kind:?} does not match the twin it converts into"
            );
        }
    }

    #[test]
    fn test_eq_market_data_instrument_kind_rejects_other_variants() {
        // Every kind must reject every *other* kind's twin. The `Spot`/`Cfd` pair is the one that
        // matters most: a CFD binding to a spot subscription (or the reverse) is exactly the
        // pollution a distinct `Cfd` variant exists to prevent, and one connector can serve both
        // on the same (exchange, base, quote).
        let kinds = [
            spot(),
            cfd(),
            perpetual(),
            future(1),
            option(1, Decimal::ONE),
        ];

        for kind in &kinds {
            for other in &kinds {
                if std::mem::discriminant(kind) == std::mem::discriminant(other) {
                    continue;
                }
                let twin = MarketDataInstrumentKind::from(other);
                assert!(
                    !kind.eq_market_data_instrument_kind(&twin),
                    "{kind:?} incorrectly matched {twin:?}"
                );
            }
        }
    }

    #[test]
    fn test_eq_market_data_instrument_kind_compares_contract_fields() {
        // Same variant, different discriminating field -> not equivalent.
        assert!(
            !future(1).eq_market_data_instrument_kind(&MarketDataInstrumentKind::from(&future(2)))
        );
        assert!(!option(1, Decimal::ONE).eq_market_data_instrument_kind(
            &MarketDataInstrumentKind::from(&option(2, Decimal::ONE))
        ));
        assert!(!option(1, Decimal::ONE).eq_market_data_instrument_kind(
            &MarketDataInstrumentKind::from(&option(1, Decimal::TWO))
        ));
    }

    #[test]
    fn test_is_split_eligible_admits_only_the_deliverable_equity() {
        assert!(spot().is_split_eligible());

        // Every derivative kind is rejected. `Cfd` is the one that must not be relaxed casually:
        // split arithmetic reads neither `kind` nor `contract_size`, so admitting it would commit
        // and record a rescaling of an index/commodity/rates position rather than reject it.
        for kind in [cfd(), perpetual(), future(1), option(1, Decimal::ONE)] {
            assert!(
                !kind.is_split_eligible(),
                "{kind:?} must not be a corporate-action split target"
            );
        }
    }

    #[test]
    fn test_cfd_accessors() {
        let contract_size = Decimal::from(25);
        let kind = InstrumentKind::Cfd(CfdContract {
            contract_size,
            settlement_asset: "gbp",
        });

        // `contract_size` reaches fee, PnL and risk-notional arithmetic, so a CFD must report its
        // own multiplier rather than an implicit one.
        assert_eq!(kind.contract_size(), contract_size);
        // A CFD is cash-settled in the account currency, which the builder must register as an
        // indexed asset -- returning `None` here would make that balance unreachable.
        assert_eq!(kind.settlement_asset(), Some(&"gbp"));
        // No contract chain, no expiry: a CFD must never reach contract-expiry settlement.
        assert_eq!(kind.expiry(), None);
    }

    #[test]
    fn test_cfd_serde_roundtrip() {
        let kind = InstrumentKind::Cfd(CfdContract {
            contract_size: Decimal::ONE,
            settlement_asset: "usd",
        });

        #[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable.
        let json = serde_json::to_string(&kind).unwrap();
        assert!(json.contains("\"cfd\""), "{json}");

        #[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable.
        let actual = serde_json::from_str::<InstrumentKind<&str>>(&json).unwrap();
        assert_eq!(actual, kind);
    }

    #[test]
    fn test_discriminant_names_each_variant() {
        // The match in `discriminant` is exhaustive, so a *missing* arm cannot compile -- but a
        // transposed one (`Future(_) => Discriminant::Option`) compiles, passes clippy, and silently
        // inverts every `ExecutionClient::SUPPORTED_KINDS` guard built on it: an instrument of a kind
        // a venue cannot encode would be admitted, and one it can would be rejected. Nothing but this
        // test stands between a typo here and the wrong-symbol order the capability set exists to
        // prevent, so each pairing is asserted individually rather than by round-tripping a list.
        assert_eq!(spot().discriminant(), InstrumentKindDiscriminant::Spot);
        assert_eq!(
            perpetual().discriminant(),
            InstrumentKindDiscriminant::Perpetual
        );
        assert_eq!(future(1).discriminant(), InstrumentKindDiscriminant::Future);
        assert_eq!(
            option(1, Decimal::ONE).discriminant(),
            InstrumentKindDiscriminant::Option
        );
        assert_eq!(cfd().discriminant(), InstrumentKindDiscriminant::Cfd);
    }
}
