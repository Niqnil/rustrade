use crate::error::KeyError;
use fnv::FnvHashMap;
use rustrade_instrument::{
    Keyed,
    asset::{Asset, AssetIndex, ExchangeAsset, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::{IndexedInstruments, error::IndexError},
    instrument::{Instrument, InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::collection::FnvIndexSet;
use smol_str::{SmolStr, StrExt};
use tracing::{debug, warn};

/// Indexed instrument map used to associate the internal Barter representation of instruments and
/// assets with the [`ExecutionClient`](super::client::ExecutionClient) representation.
///
/// Similarly, when the execution manager received an [`AccountEvent`](super::AccountEvent)
/// from the execution API, it needs to determine the internal representation of the associated
/// assets and instruments.
///
/// eg/ `InstrumentNameExchange("XBT-USDT")` <--> `InstrumentIndex(1)` <br>
/// eg/ `AssetNameExchange("XBT")` <--> `AssetIndex(1)`
///
/// # Names are matched ignoring ASCII case
/// [`Self::find_asset_index`] and [`Self::find_instrument_index`] resolve a name the way it was
/// registered first, and failing that ignoring ASCII case. Venues and clients do not always spell a
/// name the way it was registered (`usd` against `USD`), and a name that fails to resolve loses the
/// event carrying it. Ignoring case cannot merge two registered names, because
/// [`IndexedInstruments`] rejects two on one exchange that differ only in case. The lookups in the
/// other direction, from an index to a name, return the registered spelling.
///
/// The case-insensitive index is built by [`Self::new`] from [`Self::asset_names`] and
/// [`Self::instrument_names`], so a change made to those fields afterwards is not reflected in it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionInstrumentMap {
    /// The exchange associated with this execution map.
    pub exchange: Keyed<ExchangeIndex, ExchangeId>,
    /// Collection of assets available by the engine with their
    /// exchange-specific representations. This holds all indexed assets.
    pub assets: FnvIndexSet<ExchangeAsset<Asset>>,
    /// Collection of instruments available by the engine. This holds all
    /// indexed instruments.
    pub instruments: FnvIndexSet<Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
    /// Map from exchange-specific asset names to internal asset indices for
    /// fast lookups.
    pub asset_names: FnvHashMap<AssetNameExchange, AssetIndex>,
    /// Map from exchange-specific instrument names to internal instrument
    /// indices for fast lookups.
    pub instrument_names: FnvHashMap<InstrumentNameExchange, InstrumentIndex>,
    /// [`Self::asset_names`] keyed by the ASCII-lowercased name, for the case-insensitive lookup.
    asset_names_folded: FnvHashMap<SmolStr, AssetIndex>,
    /// [`Self::instrument_names`] keyed by the ASCII-lowercased name, for the case-insensitive
    /// lookup.
    instrument_names_folded: FnvHashMap<SmolStr, InstrumentIndex>,
}

impl ExecutionInstrumentMap {
    /// Construct a new [`Self`] using the provided indexed assets and instruments.
    pub fn new(
        exchange: Keyed<ExchangeIndex, ExchangeId>,
        instruments: &IndexedInstruments,
    ) -> Self {
        let asset_names: FnvHashMap<AssetNameExchange, AssetIndex> = instruments
            .assets()
            .iter()
            .filter_map(|Keyed { key, value }| {
                (value.exchange == exchange.value)
                    .then_some((value.asset.name_exchange.clone(), *key))
            })
            .collect();

        let assets = instruments
            .assets()
            .iter()
            .map(|Keyed { value, .. }| value.clone())
            .collect();

        let instrument_names: FnvHashMap<InstrumentNameExchange, InstrumentIndex> = instruments
            .instruments()
            .iter()
            .filter_map(|Keyed { key, value }| {
                (value.exchange.value == exchange.value)
                    .then_some((value.name_exchange.clone(), *key))
            })
            .collect();

        let asset_names_folded = fold_names(
            asset_names
                .iter()
                .map(|(name, index)| (name.name().as_str(), *index)),
        );
        let instrument_names_folded = fold_names(
            instrument_names
                .iter()
                .map(|(name, index)| (name.name().as_str(), *index)),
        );

        let instruments = instruments
            .instruments()
            .iter()
            .map(|Keyed { value, .. }| value.clone())
            .collect();

        Self {
            exchange,
            asset_names,
            instrument_names,
            assets,
            instruments,
            asset_names_folded,
            instrument_names_folded,
        }
    }

    pub fn exchange_assets(&self) -> impl Iterator<Item = &AssetNameExchange> {
        self.asset_names.keys()
    }

    pub fn exchange_instruments(&self) -> impl Iterator<Item = &InstrumentNameExchange> {
        self.instrument_names.keys()
    }

    pub fn find_exchange_id(&self, exchange: ExchangeIndex) -> Result<ExchangeId, KeyError> {
        if self.exchange.key == exchange {
            Ok(self.exchange.value)
        } else {
            Err(KeyError::ExchangeId(format!(
                "ExecutionInstrumentMap does not contain {exchange}"
            )))
        }
    }

    pub fn find_exchange_index(&self, exchange: ExchangeId) -> Result<ExchangeIndex, IndexError> {
        if self.exchange.value == exchange {
            Ok(self.exchange.key)
        } else {
            Err(IndexError::ExchangeIndex(format!(
                "ExecutionInstrumentMap does not contain {exchange}"
            )))
        }
    }

    pub fn find_asset_name_exchange(
        &self,
        asset: AssetIndex,
    ) -> Result<&AssetNameExchange, KeyError> {
        self.assets
            .get_index(asset.index())
            .ok_or_else(|| {
                KeyError::AssetKey(format!("ExecutionInstrumentMap does not contain: {asset}"))
            })
            .map(|asset| &asset.asset.name_exchange)
    }

    /// Find the [`AssetIndex`] of the asset this exchange reports as `asset`.
    ///
    /// Matches the registered spelling first, and failing that ignores ASCII case, logging the
    /// case-insensitive match at `debug!` with both spellings. See the
    /// [type-level note](Self#names-are-matched-ignoring-ascii-case).
    pub fn find_asset_index(&self, asset: &AssetNameExchange) -> Result<AssetIndex, IndexError> {
        if let Some(index) = self.asset_names.get(asset) {
            return Ok(*index);
        }

        let index = find_folded(&self.asset_names_folded, asset.name()).ok_or_else(|| {
            IndexError::AssetIndex(format!("ExecutionInstrumentMap does not contain: {asset}"))
        })?;

        debug!(
            exchange = %self.exchange.value,
            reported = %asset,
            registered = ?self.find_asset_name_exchange(index).ok(),
            "ExecutionInstrumentMap matched an asset name ignoring case"
        );
        Ok(index)
    }

    pub fn find_instrument_name_exchange(
        &self,
        instrument: InstrumentIndex,
    ) -> Result<&InstrumentNameExchange, KeyError> {
        self.instruments
            .get_index(instrument.index())
            .ok_or_else(|| {
                KeyError::InstrumentKey(format!(
                    "ExecutionInstrumentMap does not contain: {instrument}"
                ))
            })
            .map(|instrument| &instrument.name_exchange)
    }

    /// Find the [`InstrumentIndex`] of the instrument this exchange reports as `instrument`.
    ///
    /// Matches the registered spelling first, and failing that ignores ASCII case, logging the
    /// case-insensitive match at `debug!` with both spellings. See the
    /// [type-level note](Self#names-are-matched-ignoring-ascii-case).
    pub fn find_instrument_index(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<InstrumentIndex, IndexError> {
        if let Some(index) = self.instrument_names.get(instrument) {
            return Ok(*index);
        }

        let index =
            find_folded(&self.instrument_names_folded, instrument.name()).ok_or_else(|| {
                IndexError::InstrumentIndex(format!(
                    "ExecutionInstrumentMap does not contain: {instrument}"
                ))
            })?;

        debug!(
            exchange = %self.exchange.value,
            reported = %instrument,
            registered = ?self.find_instrument_name_exchange(index).ok(),
            "ExecutionInstrumentMap matched an instrument name ignoring case"
        );
        Ok(index)
    }
}

/// Key each name by its ASCII-lowercased form.
///
/// A key two names share is left out, with a `warn!`, so a lookup by it misses rather than picks
/// one of them. [`IndexedInstruments`] rejects such a pair on one exchange, so this guards only an
/// invariant.
fn fold_names<'a, Index>(
    names: impl Iterator<Item = (&'a str, Index)>,
) -> FnvHashMap<SmolStr, Index> {
    let mut folded = FnvHashMap::default();
    let mut shared = Vec::new();
    for (name, index) in names {
        let key = name.to_ascii_lowercase_smolstr();
        if folded.insert(key.clone(), index).is_some() {
            shared.push(key);
        }
    }
    shared.sort_unstable();
    shared.dedup();
    for key in shared {
        warn!(
            name = %key,
            "ExecutionInstrumentMap holds names that differ only in case - none resolves \
             ignoring case"
        );
        folded.remove(&key);
    }
    folded
}

/// Look `name` up in a map built by [`fold_names`].
fn find_folded<Index: Copy>(folded: &FnvHashMap<SmolStr, Index>, name: &str) -> Option<Index> {
    folded.get(&name.to_ascii_lowercase_smolstr()).copied()
}

pub fn generate_execution_instrument_map(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
) -> Result<ExecutionInstrumentMap, IndexError> {
    let exchange_index = instruments
        .exchanges()
        .iter()
        .find_map(|keyed_exchange| (keyed_exchange.value == exchange).then_some(keyed_exchange.key))
        .ok_or_else(|| {
            IndexError::ExchangeIndex(format!(
                "IndexedInstrument does not contain index for: {exchange}"
            ))
        })?;

    Ok(ExecutionInstrumentMap::new(
        Keyed::new(exchange_index, exchange),
        instruments,
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::{exchange::ExchangeId, test_utils};

    fn indexed_instruments() -> IndexedInstruments {
        let instruments = vec![
            test_utils::instrument(ExchangeId::BinanceSpot, "BTC", "ETH"),
            test_utils::instrument(ExchangeId::Coinbase, "BTC", "ETH"),
            test_utils::instrument(ExchangeId::Kraken, "USDC", "USDT"),
        ];

        IndexedInstruments::new(instruments)
    }

    #[test]
    fn test_find_exchange_id() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let exchange_id = kraken.find_exchange_id(kraken.exchange.key).unwrap();
        assert_eq!(exchange_id, ExchangeId::Kraken);
    }

    #[test]
    fn test_find_exchange_index() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let exchange_index = kraken.find_exchange_index(ExchangeId::Kraken).unwrap();
        assert_eq!(exchange_index, kraken.exchange.key);
    }

    #[test]
    fn test_find_exchange_id_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        // Create a different exchange index that doesn't match
        let binance_index = instruments
            .exchanges()
            .iter()
            .find(|ex| ex.value == ExchangeId::BinanceSpot)
            .map(|ex| ex.key)
            .unwrap();

        let result = kraken.find_exchange_id(binance_index);
        assert!(result.is_err());
        assert!(matches!(result, Err(KeyError::ExchangeId(_))));
    }

    #[test]
    fn test_find_exchange_index_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let result = kraken.find_exchange_index(ExchangeId::BinanceSpot);
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::ExchangeIndex(_))));
    }

    #[test]
    fn test_find_asset_name_exchange() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let usdt = test_utils::asset("USDT");
        let usdt_index = instruments
            .find_asset_index(ExchangeId::Kraken, &usdt.name_internal)
            .unwrap();

        let usdt_exchange_name = kraken.find_asset_name_exchange(usdt_index).unwrap();
        assert_eq!(usdt_exchange_name, &usdt.name_exchange);
    }

    #[test]
    fn test_find_asset_index() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let usdc = test_utils::asset("USDC");
        let asset_index = kraken.find_asset_index(&usdc.name_exchange).unwrap();

        let expected_index = instruments
            .find_asset_index(ExchangeId::Kraken, &usdc.name_internal)
            .unwrap();
        assert_eq!(asset_index, expected_index);
    }

    #[test]
    fn test_find_asset_index_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let btc = test_utils::asset("BTC");
        let result = kraken.find_asset_index(&btc.name_exchange);
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::AssetIndex(_))));
    }

    #[test]
    fn test_find_asset_name_exchange_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        // Try to find asset with invalid index
        let invalid_index = AssetIndex::new(999);
        let result = kraken.find_asset_name_exchange(invalid_index);
        assert!(result.is_err());
        assert!(matches!(result, Err(KeyError::AssetKey(_))));
    }

    #[test]
    fn test_find_instrument_name_exchange() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();
        let usdc_usdt = test_utils::instrument(ExchangeId::Kraken, "USDC", "USDT");

        let usdc_usdt_index = instruments
            .find_instrument_index(ExchangeId::Kraken, &usdc_usdt.name_internal)
            .unwrap();

        let usdc_usdt_exchange_name = kraken
            .find_instrument_name_exchange(usdc_usdt_index)
            .unwrap();

        assert_eq!(usdc_usdt_exchange_name, &usdc_usdt.name_exchange);
    }

    #[test]
    fn test_find_instrument_index() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let usdc_usdt = test_utils::instrument(ExchangeId::Kraken, "USDC", "USDT");
        let instrument_index = kraken
            .find_instrument_index(&usdc_usdt.name_exchange)
            .unwrap();

        let expected_index = instruments
            .find_instrument_index(ExchangeId::Kraken, &usdc_usdt.name_internal)
            .unwrap();
        assert_eq!(instrument_index, expected_index);
    }

    #[test]
    fn test_find_instrument_index_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let btc_eth = test_utils::instrument(ExchangeId::Kraken, "BTC", "ETH");
        let result = kraken.find_instrument_index(&btc_eth.name_exchange);
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::InstrumentIndex(_))));
    }

    #[test]
    fn test_find_instrument_name_exchange_error() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        // Try to find instrument with invalid index
        let invalid_index = InstrumentIndex::new(999);
        let result = kraken.find_instrument_name_exchange(invalid_index);
        assert!(result.is_err());
        assert!(matches!(result, Err(KeyError::InstrumentKey(_))));
    }

    #[test]
    fn test_exchange_assets_iterator() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let exchange_assets: Vec<&AssetNameExchange> = kraken.exchange_assets().collect();

        // Verify that the iterator returns the expected assets for Kraken
        let expected_assets = vec!["USDC", "USDT"];
        for expected in &expected_assets {
            assert!(
                exchange_assets
                    .iter()
                    .any(|asset| asset.as_ref() == *expected)
            );
        }
    }

    #[test]
    fn test_exchange_instruments_iterator() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let exchange_instruments: Vec<&InstrumentNameExchange> =
            kraken.exchange_instruments().collect();

        // Should have exactly one instrument for Kraken
        assert_eq!(exchange_instruments.len(), 1);

        // Verify it contains the USDC-USDT instrument
        let usdc_usdt = test_utils::instrument(ExchangeId::Kraken, "USDC", "USDT");
        assert!(exchange_instruments.contains(&&usdc_usdt.name_exchange));
    }

    #[test]
    fn test_generate_execution_instrument_map_error() {
        let instruments = indexed_instruments();

        // Try to generate map for exchange not in indexed instruments
        let result = generate_execution_instrument_map(&instruments, ExchangeId::Bitstamp);
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::ExchangeIndex(_))));
    }

    #[test]
    fn an_asset_name_in_another_case_resolves_to_the_registered_asset() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let registered = kraken
            .find_asset_index(&AssetNameExchange::new("USDT"))
            .unwrap();

        for reported in ["usdt", "Usdt", "uSdT"] {
            assert_eq!(
                kraken.find_asset_index(&AssetNameExchange::new(reported)),
                Ok(registered),
                "{reported}"
            );
        }
        // The other direction keeps the registered spelling.
        assert_eq!(
            kraken.find_asset_name_exchange(registered).unwrap().name(),
            "USDT"
        );
    }

    #[test]
    fn an_instrument_name_in_another_case_resolves_to_the_registered_instrument() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        let registered = kraken
            .find_instrument_index(&InstrumentNameExchange::new("USDC_USDT"))
            .unwrap();

        assert_eq!(
            kraken.find_instrument_index(&InstrumentNameExchange::new("usdc_usdt")),
            Ok(registered)
        );
        assert_eq!(
            kraken
                .find_instrument_name_exchange(registered)
                .unwrap()
                .name(),
            "USDC_USDT"
        );
    }

    #[test]
    fn ignoring_case_does_not_reach_another_exchanges_names() {
        let instruments = indexed_instruments();
        let kraken = generate_execution_instrument_map(&instruments, ExchangeId::Kraken).unwrap();

        // `BTC` and `BTC_ETH` are registered, but on Binance and Coinbase only.
        assert!(matches!(
            kraken.find_asset_index(&AssetNameExchange::new("btc")),
            Err(IndexError::AssetIndex(_))
        ));
        assert!(matches!(
            kraken.find_instrument_index(&InstrumentNameExchange::new("btc_eth")),
            Err(IndexError::InstrumentIndex(_))
        ));
    }

    #[test]
    fn a_folded_name_two_names_share_resolves_to_neither() {
        let folded = fold_names([("USD", 0), ("usd", 1), ("BTC", 2)].into_iter());

        assert_eq!(find_folded(&folded, "Usd"), None);
        assert_eq!(find_folded(&folded, "btc"), Some(2));
    }
}
