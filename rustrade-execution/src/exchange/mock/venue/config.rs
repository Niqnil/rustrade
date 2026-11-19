//! Why [`SimulatedVenue::try_new`](super::SimulatedVenue::try_new) refuses a configuration, and
//! the check that a venue's configuration spells each asset one way.

use crate::{client::mock::MockExecutionConfig, exchange::mock::account::DuplicateSeededOrder};
use fnv::FnvHashMap;
use itertools::Itertools;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::{SameAsset, name::AssetNameExchange},
    calendar::CalendarError,
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use std::fmt;
use thiserror::Error;

/// Why [`SimulatedVenue::try_new`](super::SimulatedVenue::try_new) or
/// [`SimulatedVenue::try_new_market_driven`](super::SimulatedVenue::try_new_market_driven) refused
/// its configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum VenueConfigError {
    /// The configuration spells one asset two ways.
    #[error(transparent)]
    AssetSpelledTwice(#[from] AssetSpelledTwice),

    /// `initial_state` lists one client order id more than once among its open and cancelled
    /// orders.
    #[error(transparent)]
    DuplicateOrder(#[from] DuplicateSeededOrder),

    /// `initial_state` reports an open CFD or option position with no entry price. Closing it
    /// credits margin and realised PnL computed from the entry: see the caller obligations on
    /// [`SimulatedVenue`](super::SimulatedVenue).
    #[error(
        "initial_state reports an open position in {0} with no entry price, so the margin and \
         PnL its close would credit cannot be computed"
    )]
    PositionWithoutEntry(InstrumentNameExchange),

    /// `initial_state` reports a short position in a cash-settled call, which nothing in a cash
    /// account secures. See `# How an option is accounted` on
    /// [`SimulatedVenue`](super::SimulatedVenue).
    #[error(
        "initial_state reports a short position in the cash-settled call {0}, which nothing in a \
         cash account secures"
    )]
    OptionShortUnsecurable(InstrumentNameExchange),

    /// `initial_state` reports a short option position whose collateral asset it holds no
    /// balance of.
    #[error(
        "initial_state reports a short position of {quantity} in the option {instrument}, whose \
         collateral of {amount} {asset} it holds no balance of"
    )]
    OptionCollateralNotHeld {
        instrument: InstrumentNameExchange,
        quantity: Decimal,
        asset: AssetNameExchange,
        amount: Decimal,
    },

    /// `initial_state` reports a short option position whose collateral its seeded free balance
    /// does not cover.
    #[error(
        "initial_state reports a short position of {quantity} in the option {instrument}, whose \
         collateral of {amount} {asset} the seeded free balance of {free} does not cover"
    )]
    OptionCollateralNotCovered {
        instrument: InstrumentNameExchange,
        quantity: Decimal,
        asset: AssetNameExchange,
        amount: Decimal,
        free: Decimal,
    },

    /// [`MockExecutionConfig::session_close`] cannot place the close of a day limit order seeded
    /// in `initial_state`.
    #[error("session_close cannot place the close of a day order seeded in initial_state: {0}")]
    SessionCalendar(#[source] CalendarError),
}

/// Two names in one [`SimulatedVenue`](super::SimulatedVenue)'s configuration that name one asset
/// but are spelled differently, such as `BTC` and `btc`.
///
/// Two names name one asset when [`SameAsset`] says so: asset names on one exchange do when they
/// differ only in ASCII case. A live venue spells each asset one way, and building an instrument
/// index refuses two such names, so the venue refuses them too, rather than keep a balance under
/// each spelling, or look a price or the fee asset up under one spelling and miss it under the
/// other.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "the configuration spells one asset two ways: {first} in {first_source}, {second} in \
     {second_source}"
)]
pub struct AssetSpelledTwice {
    /// The spelling the check read first. It reads the instruments in order of name, then the
    /// account's balances, the fee asset, the spot margin configuration and the fee model
    /// overrides.
    pub first: AssetNameExchange,
    /// Where [`first`](Self::first) was read.
    pub first_source: AssetSource,
    /// The other spelling.
    pub second: AssetNameExchange,
    /// Where [`second`](Self::second) was read.
    pub second_source: AssetSource,
}

/// Where a [`SimulatedVenue`](super::SimulatedVenue)'s configuration names an asset, as
/// [`AssetSpelledTwice`] reports it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum AssetSource {
    /// The base, quote or settlement asset of the instrument the venue lists under this name.
    Instrument(InstrumentNameExchange),
    /// A balance of the account the venue starts with: `initial_state`'s, or a restored
    /// snapshot's, including the debt it reports.
    Balance,
    /// [`MockExecutionConfig::fee_asset`].
    FeeAsset,
    /// [`MockExecutionConfig::spot_margin`]: its valuation asset, or an asset it sets a borrow
    /// limit or a collateral ratio for.
    SpotMargin,
    /// An underlying in [`MockExecutionConfig::fee_model_overrides`].
    FeeModelOverride,
}

impl fmt::Display for AssetSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instrument(name) => write!(f, "instrument {name}"),
            Self::Balance => f.write_str("a balance"),
            Self::FeeAsset => f.write_str("the fee asset"),
            Self::SpotMargin => f.write_str("the spot margin configuration"),
            Self::FeeModelOverride => f.write_str("a fee model override"),
        }
    }
}

/// Refuses the first name in `names` that names an asset an earlier name spelled differently.
pub(super) fn check_spellings<'a>(
    names: impl IntoIterator<Item = (&'a AssetNameExchange, AssetSource)>,
) -> Result<(), AssetSpelledTwice> {
    // One entry per asset, compared with `SameAsset` so the rule has one definition. A venue's
    // configuration names few assets, and this runs once, when the venue is built.
    let mut seen: Vec<(&AssetNameExchange, AssetSource)> = Vec::new();
    for (name, source) in names {
        match seen
            .iter()
            .find(|(spelling, _)| spelling.is_same_asset(name))
        {
            Some((spelling, _)) if *spelling == name => {}
            Some((spelling, first_source)) => {
                return Err(AssetSpelledTwice {
                    first: (*spelling).clone(),
                    first_source: first_source.clone(),
                    second: name.clone(),
                    second_source: source,
                });
            }
            None => seen.push((name, source)),
        }
    }
    Ok(())
}

/// Every asset name a venue built from `config`, `instruments` and an account holding `balances`
/// is given, with where it was read, in the order [`AssetSpelledTwice::first`] states.
pub(super) fn configured_asset_names<'a>(
    config: &'a MockExecutionConfig,
    instruments: &'a FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    balances: impl Iterator<Item = &'a AssetNameExchange> + 'a,
) -> impl Iterator<Item = (&'a AssetNameExchange, AssetSource)> + 'a {
    let listed = instruments
        .iter()
        .sorted_unstable_by_key(|(name, _)| *name)
        .flat_map(|(name, instrument)| {
            [&instrument.underlying.base, &instrument.underlying.quote]
                .into_iter()
                .chain(instrument.kind.settlement_asset())
                .map(|asset| (asset, AssetSource::Instrument(name.clone())))
        });
    let fee_asset = config
        .fee_asset
        .as_ref()
        .map(|fee_asset| (fee_asset.asset(), AssetSource::FeeAsset));
    let spot_margin = config
        .spot_margin
        .iter()
        .flat_map(|margin| margin.assets())
        .map(|asset| (asset, AssetSource::SpotMargin));
    let overrides = config
        .fee_model_overrides
        .by_underlying()
        .flat_map(|(_, underlying, _)| [&underlying.base, &underlying.quote])
        .map(|asset| (asset, AssetSource::FeeModelOverride));
    listed
        .chain(balances.map(|asset| (asset, AssetSource::Balance)))
        .chain(fee_asset)
        .chain(spot_margin)
        .chain(overrides)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(spelling: &str) -> AssetNameExchange {
        AssetNameExchange::new(spelling)
    }

    #[test]
    fn one_spelling_in_many_places_passes() {
        let btc = name("BTC");
        let usdt = name("USDT");

        let checked = check_spellings([
            (&btc, AssetSource::Balance),
            (&usdt, AssetSource::Balance),
            (&btc, AssetSource::FeeAsset),
            (&usdt, AssetSource::SpotMargin),
        ]);

        assert_eq!(checked, Ok(()));
    }

    #[test]
    fn two_spellings_of_one_asset_are_refused_naming_both_in_order() {
        let upper = name("BNB");
        let usdt = name("USDT");
        let lower = name("bnb");

        let checked = check_spellings([
            (&upper, AssetSource::Balance),
            (&usdt, AssetSource::Balance),
            (&lower, AssetSource::FeeAsset),
        ]);

        assert_eq!(
            checked,
            Err(AssetSpelledTwice {
                first: upper,
                first_source: AssetSource::Balance,
                second: lower,
                second_source: AssetSource::FeeAsset,
            })
        );
    }

    #[test]
    fn different_assets_pass() {
        let btc = name("BTC");
        let wbtc = name("WBTC");

        assert_eq!(
            check_spellings([(&btc, AssetSource::Balance), (&wbtc, AssetSource::Balance)]),
            Ok(())
        );
    }

    #[test]
    fn the_message_names_both_spellings_and_where_each_was_read() {
        let error = AssetSpelledTwice {
            first: name("BTC"),
            first_source: AssetSource::Instrument(InstrumentNameExchange::new("BTCUSDT")),
            second: name("btc"),
            second_source: AssetSource::Balance,
        };

        assert_eq!(
            error.to_string(),
            "the configuration spells one asset two ways: BTC in instrument BTCUSDT, btc in a \
             balance"
        );
    }
}
