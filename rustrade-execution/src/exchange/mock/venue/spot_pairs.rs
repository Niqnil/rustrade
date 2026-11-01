//! The simulated venue's spot instruments, indexed by the two assets each one pairs.

use fnv::FnvHashMap;
use rustrade_instrument::{
    Underlying,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{Instrument, kind::InstrumentKind, name::InstrumentNameExchange},
};

/// Every spot instrument a venue trades, looked up by the two assets it pairs, either way round.
///
/// Valuing an account and choosing a liquidation's leg both ask "which spot instruments pair these
/// two assets?" once per balance, on every market input while something is owed. Answering from
/// this index costs two hash lookups, where a scan of the instrument map costs its whole length.
///
/// Built once, from the instrument map the venue is constructed with, which the venue never
/// changes afterwards.
#[derive(Debug, Clone, Default)]
pub(super) struct SpotPairs(
    FnvHashMap<AssetNameExchange, FnvHashMap<AssetNameExchange, Vec<SpotPair>>>,
);

/// One spot instrument, seen from one of the two assets it pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SpotPair {
    /// The instrument's name.
    pub instrument: InstrumentNameExchange,
    /// `false` when the asset it was looked up from is the instrument's base, `true` when it is
    /// the quote.
    pub inverse: bool,
}

impl SpotPairs {
    /// Indexes every [`InstrumentKind::Spot`] instrument in `instruments`.
    ///
    /// An instrument whose base and quote are the same asset pairs nothing, so it is left out.
    pub fn new(
        instruments: &FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        let mut index = Self::default();
        for (name, instrument) in instruments {
            let Underlying { base, quote } = &instrument.underlying;
            if !matches!(instrument.kind, InstrumentKind::Spot) || base == quote {
                continue;
            }
            index.insert(base, quote, name, false);
            index.insert(quote, base, name, true);
        }
        for pairs in index.0.values_mut().flat_map(FnvHashMap::values_mut) {
            pairs.sort_unstable_by(|a, b| a.instrument.cmp(&b.instrument));
        }
        index
    }

    fn insert(
        &mut self,
        from: &AssetNameExchange,
        to: &AssetNameExchange,
        instrument: &InstrumentNameExchange,
        inverse: bool,
    ) {
        self.0
            .entry(from.clone())
            .or_default()
            .entry(to.clone())
            .or_default()
            .push(SpotPair {
                instrument: instrument.clone(),
                inverse,
            });
    }

    /// Every spot instrument pairing `from` with `to`, in either direction, ordered by name.
    ///
    /// [`SpotPair::inverse`] is `false` for an instrument whose base is `from` and quote is `to`.
    pub fn between(&self, from: &AssetNameExchange, to: &AssetNameExchange) -> &[SpotPair] {
        self.0
            .get(from)
            .and_then(|to_pairs| to_pairs.get(to))
            .map_or(&[], Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use rustrade_instrument::instrument::{
        kind::perpetual::PerpetualContract, name::InstrumentNameInternal,
        quote::InstrumentQuoteAsset,
    };

    fn instrument(
        base: &str,
        quote: &str,
        kind: InstrumentKind<AssetNameExchange>,
    ) -> Instrument<ExchangeId, AssetNameExchange> {
        Instrument {
            exchange: ExchangeId::Simulated,
            name_internal: InstrumentNameInternal::new(format!("{base}_{quote}")),
            name_exchange: InstrumentNameExchange::new(format!("{base}{quote}")),
            underlying: Underlying::new(asset(base), asset(quote)),
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind,
            spec: None,
            data_venue: None,
        }
    }

    fn index(
        instruments: impl IntoIterator<Item = (&'static str, Instrument<ExchangeId, AssetNameExchange>)>,
    ) -> SpotPairs {
        SpotPairs::new(
            &instruments
                .into_iter()
                .map(|(name, instrument)| (InstrumentNameExchange::new(name), instrument))
                .collect(),
        )
    }

    fn pair(instrument: &str, inverse: bool) -> SpotPair {
        SpotPair {
            instrument: InstrumentNameExchange::new(instrument),
            inverse,
        }
    }

    fn asset(name: &str) -> AssetNameExchange {
        AssetNameExchange::new(name)
    }

    #[test]
    fn a_spot_instrument_is_found_from_either_asset_with_its_direction() {
        let index = index([("btc_usdt", instrument("btc", "usdt", InstrumentKind::Spot))]);

        assert_eq!(
            index.between(&asset("btc"), &asset("usdt")),
            [pair("btc_usdt", false)]
        );
        assert_eq!(
            index.between(&asset("usdt"), &asset("btc")),
            [pair("btc_usdt", true)]
        );
        assert!(index.between(&asset("btc"), &asset("eth")).is_empty());
        assert!(index.between(&asset("eth"), &asset("usdt")).is_empty());
    }

    #[test]
    fn instruments_pairing_the_same_assets_are_ordered_by_name_whichever_way_round() {
        // Inserted out of order, and from a hash map, so only the sort can put them in order.
        let index = index([
            ("c", instrument("usdt", "btc", InstrumentKind::Spot)),
            ("a", instrument("btc", "usdt", InstrumentKind::Spot)),
            ("b", instrument("btc", "usdt", InstrumentKind::Spot)),
        ]);

        assert_eq!(
            index.between(&asset("btc"), &asset("usdt")),
            [pair("a", false), pair("b", false), pair("c", true)]
        );
        assert_eq!(
            index.between(&asset("usdt"), &asset("btc")),
            [pair("a", true), pair("b", true), pair("c", false)]
        );
    }

    #[test]
    fn only_spot_instruments_pairing_two_different_assets_are_indexed() {
        let index = index([
            ("btc_btc", instrument("btc", "btc", InstrumentKind::Spot)),
            (
                "btc_usdt_perp",
                instrument(
                    "btc",
                    "usdt",
                    InstrumentKind::Perpetual(PerpetualContract {
                        contract_size: Decimal::ONE,
                        settlement_asset: asset("usdt"),
                    }),
                ),
            ),
        ]);

        assert!(index.between(&asset("btc"), &asset("btc")).is_empty());
        assert!(index.between(&asset("btc"), &asset("usdt")).is_empty());
        assert!(index.between(&asset("usdt"), &asset("btc")).is_empty());
    }
}
