use crate::{asset::name::AssetNameExchange, exchange::ExchangeId};
use derive_more::Display;
use serde::Serialize;
use smol_str::{SmolStr, StrExt, format_smolstr};
use std::borrow::Borrow;

/// Barter lowercase `SmolStr` representation for an [`Instrument`](super::Instrument) - unique
/// across all exchanges.
///
/// Note: Binance btc_usdt spot is not considered the same instrument as Bitfinex btc_usdt spot.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Display)]
pub struct InstrumentNameInternal(pub SmolStr);

impl InstrumentNameInternal {
    /// Construct a new lowercase [`Self`] from the provided `Into<SmolStr>`.
    ///
    /// Should be unique across exchanges.
    pub fn new<S>(name: S) -> Self
    where
        S: Into<SmolStr>,
    {
        let name = name.into();
        if name.chars().all(char::is_lowercase) {
            Self(name)
        } else {
            Self(name.to_lowercase_smolstr())
        }
    }

    /// Construct a new lowercase [`Self`], combining the [`ExchangeId`] and
    /// base and quote [`AssetNameExchange`]s.
    ///
    /// Generates an internal instrument identifier unique across exchanges.
    ///
    /// The exchange segment is [`ExchangeId::as_str`] — the same canonical `snake_case`
    /// spelling [`Self::new_from_exchange`] uses — so both constructors resolve the same
    /// instrument to the same name. See that method for why the two must agree.
    ///
    /// # No in-tree caller
    /// Every constructor in this workspace now builds names from an `InstrumentNameExchange`, so
    /// this is public API for downstream users whose instrument source gives them a base/quote
    /// pair and no exchange symbol — not a path this library exercises. It is kept rather than
    /// deleted because the must-agree invariant above is only meaningful while both spellings
    /// exist, and a downstream caller reaching for it would otherwise hand-roll the format string
    /// and reintroduce exactly the divergence that invariant exists to prevent. The agreement is
    /// pinned by `test_both_constructors_agree_on_the_same_instrument` rather than by a caller.
    pub fn new_from_exchange_underlying<Ass>(exchange: ExchangeId, base: &Ass, quote: &Ass) -> Self
    where
        for<'a> &'a Ass: Into<&'a AssetNameExchange>,
    {
        // Named explicitly rather than interpolating the `ExchangeId`. The two now agree --
        // `ExchangeId`'s `Display` delegates to `as_str` -- but this is an identity key, and it
        // costs nothing to depend on the one spelling that is defined to be canonical rather than
        // on a `Display` impl remaining a delegation.
        let exchange = exchange.as_str();
        Self::new(format_smolstr!(
            "{exchange}-{}_{}",
            base.into(),
            quote.into()
        ))
    }

    /// Construct a new lowercase [`Self`], combining the [`ExchangeId`] and
    /// [`InstrumentNameExchange`].
    ///
    /// Generates an internal instrument identifier unique across exchanges.
    ///
    /// # Why the two constructors must agree
    /// `InstrumentNameInternal` is an identity key, not a label: it keys the engine's instrument
    /// state map and is the lookup argument of both `InstrumentStates::instrument` (which
    /// **panics** when absent) and `IndexedInstruments::find_instrument_index`. Two spellings of
    /// the same instrument are therefore two *different* instruments — one declared through a
    /// JSON config and one built in-library would miss each other across that boundary. Both
    /// constructors consequently spell the exchange segment with [`ExchangeId::as_str`].
    pub fn new_from_exchange<S>(exchange: ExchangeId, name_exchange: S) -> Self
    where
        S: Into<InstrumentNameExchange>,
    {
        let name_exchange = name_exchange.into();
        let exchange = exchange.as_str();
        Self::new(format_smolstr!("{exchange}-{name_exchange}"))
    }

    /// Return the internal instrument `SmolStr` name of [`Self`].
    pub fn name(&self) -> &SmolStr {
        &self.0
    }
}

impl From<&str> for InstrumentNameInternal {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<SmolStr> for InstrumentNameInternal {
    fn from(value: SmolStr) -> Self {
        Self::new(value)
    }
}

impl From<String> for InstrumentNameInternal {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Borrow<str> for InstrumentNameInternal {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl AsRef<str> for InstrumentNameInternal {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'de> serde::de::Deserialize<'de> for InstrumentNameInternal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let name = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        Ok(InstrumentNameInternal::new(name))
    }
}

/// Exchange `SmolStr` representation for an [`Instrument`](super::Instrument) - most likely not
/// unique across all exchanges.
///
/// For example: `InstrumentNameExchange("XBT-USDT")`, which is distinct from the internal
/// representation of the instrument, such as `InstrumentIndex(1)` or
/// `InstrumentNameInternal("btc_usdt"`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Display)]
pub struct InstrumentNameExchange(SmolStr);

impl InstrumentNameExchange {
    /// Construct a new [`Self`] from the provided `Into<SmolStr>`.
    pub fn new<S>(name: S) -> Self
    where
        S: Into<SmolStr>,
    {
        Self(name.into())
    }

    /// Return the execution instrument `SmolStr` name of [`Self`].
    pub fn name(&self) -> &SmolStr {
        &self.0
    }
}

impl From<&str> for InstrumentNameExchange {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<SmolStr> for InstrumentNameExchange {
    fn from(value: SmolStr) -> Self {
        Self::new(value)
    }
}

impl From<String> for InstrumentNameExchange {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Borrow<str> for InstrumentNameExchange {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl AsRef<str> for InstrumentNameExchange {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'de> serde::de::Deserialize<'de> for InstrumentNameExchange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let name = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        Ok(InstrumentNameExchange::new(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every [`ExchangeId`] whose canonical `as_str` is multi-word, ie/ the variants where the
    /// `Display` impl (which renders the bare variant name) and `as_str` diverge. Single-word
    /// variants like `Kraken` cannot distinguish the two spellings, so they prove nothing here.
    const MULTI_WORD_EXCHANGES: [ExchangeId; 4] = [
        ExchangeId::BinanceSpot,
        ExchangeId::BinanceFuturesUsd,
        ExchangeId::AlpacaIex,
        ExchangeId::HyperliquidPerp,
    ];

    #[test]
    fn test_new_from_exchange_underlying_uses_canonical_exchange_str() {
        // `ExchangeId` derives `derive_more::Display` with no format attribute, so interpolating
        // it renders the *variant* name -- `BinanceSpot` -> lowercased "binancespot". The
        // canonical spelling is `as_str`'s "binance_spot".
        let actual = InstrumentNameInternal::new_from_exchange_underlying(
            ExchangeId::BinanceSpot,
            &AssetNameExchange::new("BTC"),
            &AssetNameExchange::new("USDT"),
        );

        assert_eq!(actual, InstrumentNameInternal::new("binance_spot-btc_usdt"));
    }

    #[test]
    fn test_both_constructors_agree_on_the_same_instrument() {
        // Two public constructors for the same identity key. `new_from_exchange` is what the
        // library builds names with today; `new_from_exchange_underlying` is offered to callers who
        // hold the underlying pair rather than an assembled name, and has no in-tree caller. That
        // is precisely why it needs pinning: `InstrumentNameInternal` is an identity key, so a
        // downstream user reaching for the unused constructor while the library uses the other
        // would silently split one instrument into two that never resolve to each other.
        for exchange in MULTI_WORD_EXCHANGES {
            let from_underlying = InstrumentNameInternal::new_from_exchange_underlying(
                exchange,
                &AssetNameExchange::new("btc"),
                &AssetNameExchange::new("usdt"),
            );
            let from_name_exchange =
                InstrumentNameInternal::new_from_exchange(exchange, "btc_usdt");

            assert_eq!(
                from_underlying, from_name_exchange,
                "constructors disagree for {exchange:?}"
            );
            assert!(
                from_underlying
                    .name()
                    .starts_with(&format!("{}-", exchange.as_str())),
                "{exchange:?} name is not prefixed by its canonical as_str"
            );
        }
    }
}
