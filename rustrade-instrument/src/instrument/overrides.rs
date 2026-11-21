use crate::{
    Underlying,
    asset::name::AssetNameExchange,
    instrument::{Instrument, kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// A value for some instruments, each in place of a default: by the instrument's exact name, by
/// its kind and underlying, or by its kind alone.
///
/// A venue holds one default (a fee model, a session calendar) and these overrides for the
/// instruments that differ from it. [`resolve`](Self::resolve) picks the value of the most
/// specific entry that matches an instrument, in this order, and `None` if none does, so the
/// caller falls back to its default:
/// 1. its name, exactly (see [`with_name`](Self::with_name));
/// 2. its kind and underlying, base and quote (see [`with_underlying`](Self::with_underlying));
/// 3. its kind (see [`with_kind`](Self::with_kind)).
///
/// An entry that matches no instrument is never used, and an instrument no entry matches gets
/// the default. [`unmatched`](Self::unmatched) finds the entries by name or by kind and underlying
/// that match none of a set of instruments, which are likely typos.
///
/// # Choosing a key
/// A key by kind separates a venue's options from its shares. A key by kind and underlying
/// separates the options on one underlying from the options on others, without catching the
/// underlying itself, which is of another kind. It cannot separate two families of options on one
/// underlying, such as an index's standard and weekly options, which an [`OptionContract`] does
/// not tell apart: key those by name.
///
/// [`OptionContract`]: crate::instrument::kind::option::OptionContract
///
/// # Serialised form
/// Each part is left out when empty, and deserialising refuses an unknown key and an underlying
/// listed twice for one kind ([`InstrumentOverridesError::DuplicateUnderlying`]):
///
/// ```json
/// {
///   "by_name": {"SPXW261218C06000000": <value>},
///   "by_underlying": [
///     {"kind": "option", "underlying": {"base": "spx", "quote": "usd"}, "value": <value>}
///   ],
///   "by_kind": {"option": <value>}
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(
    try_from = "InstrumentOverridesRepr<T>",
    into = "InstrumentOverridesRepr<T>",
    bound(
        serialize = "T: Serialize + Clone",
        deserialize = "T: Deserialize<'de>"
    )
)]
pub struct InstrumentOverrides<T> {
    by_name: BTreeMap<InstrumentNameExchange, T>,
    by_underlying: BTreeMap<InstrumentKindDiscriminant, BTreeMap<Underlying<AssetNameExchange>, T>>,
    by_kind: BTreeMap<InstrumentKindDiscriminant, T>,
}

// Not derived: a derive would require `T: Default`, which an empty set of overrides does not need.
impl<T> Default for InstrumentOverrides<T> {
    fn default() -> Self {
        Self {
            by_name: BTreeMap::new(),
            by_underlying: BTreeMap::new(),
            by_kind: BTreeMap::new(),
        }
    }
}

impl<T> InstrumentOverrides<T> {
    /// No overrides: every instrument gets the default.
    pub fn new() -> Self {
        Self::default()
    }

    /// These overrides, with the instrument named `name` on the venue given `value`, replacing
    /// any value it had.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<InstrumentNameExchange>, value: T) -> Self {
        self.by_name.insert(name.into(), value);
        self
    }

    /// These overrides, with instruments of `kind` on `underlying` given `value`, replacing any
    /// value they had.
    #[must_use]
    pub fn with_underlying(
        mut self,
        kind: InstrumentKindDiscriminant,
        underlying: Underlying<AssetNameExchange>,
        value: T,
    ) -> Self {
        self.by_underlying
            .entry(kind)
            .or_default()
            .insert(underlying, value);
        self
    }

    /// These overrides, with instruments of `kind` given `value`, replacing any value they had.
    #[must_use]
    pub fn with_kind(mut self, kind: InstrumentKindDiscriminant, value: T) -> Self {
        self.by_kind.insert(kind, value);
        self
    }

    /// The values by instrument name.
    pub fn by_name(&self) -> &BTreeMap<InstrumentNameExchange, T> {
        &self.by_name
    }

    /// The values by kind and underlying, flattened into one entry each, in order of kind and
    /// then underlying.
    pub fn by_underlying(
        &self,
    ) -> impl Iterator<
        Item = (
            InstrumentKindDiscriminant,
            &Underlying<AssetNameExchange>,
            &T,
        ),
    > {
        self.by_underlying.iter().flat_map(|(kind, values)| {
            values
                .iter()
                .map(|(underlying, value)| (*kind, underlying, value))
        })
    }

    /// The values by kind.
    pub fn by_kind(&self) -> &BTreeMap<InstrumentKindDiscriminant, T> {
        &self.by_kind
    }

    /// Whether there are no overrides at all.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty() && self.by_underlying.is_empty() && self.by_kind.is_empty()
    }

    /// The value of the most specific entry matching the instrument named `name`, of `kind`, on
    /// `underlying`, or `None` if no entry matches. See the type's documentation for the order.
    pub fn resolve(
        &self,
        name: &InstrumentNameExchange,
        kind: InstrumentKindDiscriminant,
        underlying: &Underlying<AssetNameExchange>,
    ) -> Option<&T> {
        self.by_name
            .get(name)
            .or_else(|| {
                self.by_underlying
                    .get(&kind)
                    .and_then(|values| values.get(underlying))
            })
            .or_else(|| self.by_kind.get(&kind))
    }

    /// The entries by name, or by kind and underlying, that match none of `instruments`, in that
    /// order and then in the order of [`by_name`](Self::by_name) and
    /// [`by_underlying`](Self::by_underlying). An instrument matches by its
    /// [`name_exchange`](Instrument::name_exchange).
    ///
    /// Entries by kind are not checked: one configuration often serves venues of different
    /// instruments, and a kind a venue lacks is no sign of a mistake.
    pub fn unmatched<'i, ExchangeKey: 'i>(
        &self,
        instruments: impl IntoIterator<Item = &'i Instrument<ExchangeKey, AssetNameExchange>>,
    ) -> Vec<UnmatchedOverride<'_>> {
        let mut names = BTreeSet::new();
        let mut underlyings = BTreeSet::new();
        for instrument in instruments {
            names.insert(&instrument.name_exchange);
            underlyings.insert((instrument.kind.discriminant(), &instrument.underlying));
        }
        let by_name = self
            .by_name
            .keys()
            .filter(|name| !names.contains(name))
            .map(UnmatchedOverride::Name);
        let by_underlying = self
            .by_underlying()
            .filter(|(kind, underlying, _)| !underlyings.contains(&(*kind, *underlying)))
            .map(|(kind, underlying, _)| UnmatchedOverride::Underlying(kind, underlying));
        by_name.chain(by_underlying).collect()
    }
}

/// An entry of an [`InstrumentOverrides`] that matches none of a set of instruments. See
/// [`InstrumentOverrides::unmatched`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnmatchedOverride<'a> {
    /// An entry by name.
    Name(&'a InstrumentNameExchange),
    /// An entry by kind and underlying.
    Underlying(
        InstrumentKindDiscriminant,
        &'a Underlying<AssetNameExchange>,
    ),
}

/// [`InstrumentOverrides`]' serialised form, validated on the way in.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstrumentOverridesRepr<T> {
    #[serde(default = "BTreeMap::new", skip_serializing_if = "BTreeMap::is_empty")]
    by_name: BTreeMap<InstrumentNameExchange, T>,
    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    by_underlying: Vec<UnderlyingOverrideRepr<T>>,
    #[serde(default = "BTreeMap::new", skip_serializing_if = "BTreeMap::is_empty")]
    by_kind: BTreeMap<InstrumentKindDiscriminant, T>,
}

/// One entry of [`InstrumentOverridesRepr::by_underlying`]: a list rather than a map, since a JSON
/// map's keys are strings and this one is a kind and an underlying.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UnderlyingOverrideRepr<T> {
    kind: InstrumentKindDiscriminant,
    underlying: Underlying<AssetNameExchange>,
    value: T,
}

impl<T> TryFrom<InstrumentOverridesRepr<T>> for InstrumentOverrides<T> {
    type Error = InstrumentOverridesError;

    fn try_from(repr: InstrumentOverridesRepr<T>) -> Result<Self, Self::Error> {
        let InstrumentOverridesRepr {
            by_name,
            by_underlying: entries,
            by_kind,
        } = repr;
        let mut by_underlying: BTreeMap<_, BTreeMap<_, _>> = BTreeMap::new();
        for UnderlyingOverrideRepr {
            kind,
            underlying,
            value,
        } in entries
        {
            let values = by_underlying.entry(kind).or_default();
            if values.contains_key(&underlying) {
                return Err(InstrumentOverridesError::DuplicateUnderlying { kind, underlying });
            }
            values.insert(underlying, value);
        }
        Ok(Self {
            by_name,
            by_underlying,
            by_kind,
        })
    }
}

impl<T> From<InstrumentOverrides<T>> for InstrumentOverridesRepr<T> {
    fn from(overrides: InstrumentOverrides<T>) -> Self {
        let InstrumentOverrides {
            by_name,
            by_underlying,
            by_kind,
        } = overrides;
        Self {
            by_name,
            by_underlying: by_underlying
                .into_iter()
                .flat_map(|(kind, values)| {
                    values
                        .into_iter()
                        .map(move |(underlying, value)| UnderlyingOverrideRepr {
                            kind,
                            underlying,
                            value,
                        })
                })
                .collect(),
            by_kind,
        }
    }
}

/// Why an [`InstrumentOverrides`] was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum InstrumentOverridesError {
    /// `underlying` is listed more than once for instruments of `kind`.
    #[error(
        "instrument overrides list {kind:?} on {}/{} more than once",
        .underlying.base,
        .underlying.quote
    )]
    DuplicateUnderlying {
        /// The instruments' kind.
        kind: InstrumentKindDiscriminant,
        /// Their underlying.
        underlying: Underlying<AssetNameExchange>,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{exchange::ExchangeId, instrument::kind::InstrumentKind};

    fn option_kind() -> InstrumentKindDiscriminant {
        InstrumentKindDiscriminant::Option
    }

    fn spx() -> Underlying<AssetNameExchange> {
        Underlying::new("spx", "usd")
    }

    fn name(name: &str) -> InstrumentNameExchange {
        InstrumentNameExchange::new(name)
    }

    /// One override of each kind, each a different value.
    fn overrides() -> InstrumentOverrides<u32> {
        InstrumentOverrides::new()
            .with_name("SPXW-C", 3)
            .with_underlying(option_kind(), spx(), 2)
            .with_kind(option_kind(), 1)
    }

    #[test]
    fn an_override_by_name_beats_one_by_underlying_which_beats_one_by_kind() {
        let overrides = overrides();

        assert_eq!(
            overrides.resolve(&name("SPXW-C"), option_kind(), &spx()),
            Some(&3)
        );
        assert_eq!(
            overrides.resolve(&name("SPX-C"), option_kind(), &spx()),
            Some(&2)
        );
        assert_eq!(
            overrides.resolve(
                &name("NDX-C"),
                option_kind(),
                &Underlying::new("ndx", "usd")
            ),
            Some(&1)
        );
        // The same base in another quote is another underlying.
        assert_eq!(
            overrides.resolve(
                &name("SPX-C"),
                option_kind(),
                &Underlying::new("spx", "eur")
            ),
            Some(&1)
        );
    }

    #[test]
    fn an_override_by_underlying_does_not_catch_another_kind_on_it() {
        let overrides = InstrumentOverrides::new().with_underlying(option_kind(), spx(), 2);

        assert_eq!(
            overrides.resolve(&name("SPX"), InstrumentKindDiscriminant::Spot, &spx()),
            None
        );
        assert_eq!(
            InstrumentOverrides::<u32>::new().resolve(&name("SPX"), option_kind(), &spx()),
            None
        );
    }

    #[test]
    fn a_later_override_replaces_an_earlier_one_with_its_key() {
        let overrides = overrides()
            .with_name("SPXW-C", 13)
            .with_underlying(option_kind(), spx(), 12)
            .with_kind(option_kind(), 11);

        assert_eq!(overrides.by_name(), &BTreeMap::from([(name("SPXW-C"), 13)]));
        assert_eq!(
            overrides.by_underlying().collect::<Vec<_>>(),
            vec![(option_kind(), &spx(), &12)]
        );
        assert_eq!(overrides.by_kind(), &BTreeMap::from([(option_kind(), 11)]));
    }

    #[test]
    fn overrides_are_empty_only_with_no_entry_of_any_kind() {
        assert!(InstrumentOverrides::<u32>::new().is_empty());
        assert!(!InstrumentOverrides::new().with_name("x", 1).is_empty());
        assert!(
            !InstrumentOverrides::new()
                .with_underlying(option_kind(), spx(), 1)
                .is_empty()
        );
        assert!(
            !InstrumentOverrides::new()
                .with_kind(option_kind(), 1)
                .is_empty()
        );
    }

    #[test]
    fn overrides_round_trip_with_underlyings_as_a_list() {
        let overrides = overrides();
        let json = serde_json::to_string(&overrides).unwrap();

        assert_eq!(
            json,
            r#"{"by_name":{"SPXW-C":3},"by_underlying":[{"kind":"option","underlying":{"base":"spx","quote":"usd"},"value":2}],"by_kind":{"option":1}}"#
        );
        assert_eq!(
            serde_json::from_str::<InstrumentOverrides<u32>>(&json).unwrap(),
            overrides
        );
        assert_eq!(
            serde_json::to_string(&InstrumentOverrides::<u32>::new()).unwrap(),
            "{}"
        );
        assert_eq!(
            serde_json::from_str::<InstrumentOverrides<u32>>("{}").unwrap(),
            InstrumentOverrides::new()
        );
    }

    #[test]
    fn deserialising_overrides_refuses_a_repeated_underlying_and_an_unknown_key() {
        let entry = r#"{"kind":"option","underlying":{"base":"spx","quote":"usd"},"value":0}"#;
        let error = serde_json::from_str::<InstrumentOverrides<u32>>(&format!(
            r#"{{"by_underlying":[{entry},{entry}]}}"#
        ))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("instrument overrides list Option on spx/usd more than once"),
            "{error}"
        );

        let error =
            serde_json::from_str::<InstrumentOverrides<u32>>(r#"{"by_names":{}}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    fn instrument(
        name: &str,
        underlying: Underlying<AssetNameExchange>,
        kind: InstrumentKind<AssetNameExchange>,
    ) -> Instrument<ExchangeId, AssetNameExchange> {
        Instrument::new(
            ExchangeId::Simulated,
            name,
            name,
            underlying,
            crate::instrument::quote::InstrumentQuoteAsset::UnderlyingQuote,
            kind,
            None,
        )
    }

    #[test]
    fn an_override_no_instrument_matches_is_found_except_by_kind() {
        let spy_usd = Underlying::new("spy", "usd");
        let spy_eur = Underlying::new("spy", "eur");
        let instruments = [
            instrument("SPY", spy_usd.clone(), InstrumentKind::Spot),
            instrument("SPX", spx(), InstrumentKind::Spot),
        ];
        let overrides = InstrumentOverrides::new()
            .with_name("SPY", 1)
            .with_name("SPX_C100", 1)
            .with_underlying(InstrumentKindDiscriminant::Spot, spy_usd.clone(), 1)
            .with_underlying(InstrumentKindDiscriminant::Spot, spy_eur.clone(), 1)
            .with_underlying(InstrumentKindDiscriminant::Spot, spx(), 1)
            // SPY trades here, but only as spot: a key matches on kind and underlying together.
            .with_underlying(InstrumentKindDiscriminant::Option, spy_usd.clone(), 1)
            .with_kind(InstrumentKindDiscriminant::Future, 1);

        assert_eq!(
            overrides.unmatched(&instruments),
            vec![
                UnmatchedOverride::Name(&name("SPX_C100")),
                UnmatchedOverride::Underlying(InstrumentKindDiscriminant::Spot, &spy_eur),
                UnmatchedOverride::Underlying(InstrumentKindDiscriminant::Option, &spy_usd),
            ]
        );
        assert!(
            InstrumentOverrides::<u32>::new()
                .unmatched(&instruments)
                .is_empty()
        );
    }
}
