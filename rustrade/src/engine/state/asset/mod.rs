use crate::{
    Timed, engine::state::asset::filter::AssetFilter,
    statistic::summary::asset::TearSheetAssetGenerator,
};
use chrono::Utc;
use derive_more::Constructor;
use itertools::Either;
use rustrade_execution::balance::{AssetBalance, AssetBalanceUpdate, Balance};
use rustrade_instrument::{
    asset::{
        Asset, AssetIndex, ExchangeAsset,
        name::{AssetNameExchange, AssetNameInternal},
    },
    index::IndexedInstruments,
};
use rustrade_integration::collection::{FnvIndexMap, snapshot::Snapshot};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::fmt::{self, Debug};

/// Defines an `AssetFilter`, used to filter asset-centric data structures.
pub mod filter;

/// Collection of exchange [`AssetState`]s indexed by [`AssetIndex`].
///
/// Note that the same named assets on different exchanges will have their own [`AssetState`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetStates(pub FnvIndexMap<ExchangeAsset<AssetNameInternal>, AssetState>);

impl Serialize for AssetStates {
    fn serialize<S: Serializer>(&self, serialiser: S) -> Result<S::Ok, S::Error> {
        // serde_json cannot use struct keys in JSON objects, so serialise as a sequence of pairs.
        // Stream directly from the map iterator — no intermediate Vec allocation.
        let mut seq = serialiser.serialize_seq(Some(self.0.len()))?;
        for pair in &self.0 {
            seq.serialize_element(&pair)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for AssetStates {
    fn deserialize<D: Deserializer<'de>>(deserialiser: D) -> Result<Self, D::Error> {
        struct AssetStatesVisitor;

        impl<'de> Visitor<'de> for AssetStatesVisitor {
            type Value = AssetStates;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a sequence of (ExchangeAsset, AssetState) pairs")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                // Pre-allocate with the size hint to avoid rehashing, then populate in one pass.
                let mut map = FnvIndexMap::default();
                map.reserve(seq.size_hint().unwrap_or(0));
                while let Some((k, v)) = seq.next_element()? {
                    map.insert(k, v);
                }
                Ok(AssetStates(map))
            }
        }

        deserialiser.deserialize_seq(AssetStatesVisitor)
    }
}

impl AssetStates {
    /// Return a reference to the `AssetState` associated with an `AssetIndex`.
    ///
    /// Panics if the `AssetState` associated with the `AssetIndex` does not exist.
    pub fn asset_index(&self, key: &AssetIndex) -> &AssetState {
        self.0
            .get_index(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("AssetStates does not contain: {key}"))
    }

    /// Return a mutable reference to the `AssetState` associated with an `AssetIndex`.
    ///
    /// Panics if the `AssetState` associated with the `AssetIndex` does not exist.
    pub fn asset_index_mut(&mut self, key: &AssetIndex) -> &mut AssetState {
        self.0
            .get_index_mut(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("AssetStates does not contain: {key}"))
    }

    /// Return a reference to the `AssetState` associated with an `ExchangeAsset<AssetNameInternal>`.
    ///
    /// Panics if the `AssetState` associated with the `ExchangeAsset<AssetNameInternal>`
    /// does not exist.
    pub fn asset(&self, key: &ExchangeAsset<AssetNameInternal>) -> &AssetState {
        self.0
            .get(key)
            .unwrap_or_else(|| panic!("AssetStates does not contain: {key:?}"))
    }

    /// Return a mutable reference to the `AssetState` associated with an
    /// `ExchangeAsset<AssetNameInternal>`.
    ///
    /// Panics if the `AssetState` associated with the `ExchangeAsset<AssetNameInternal>`
    /// does not exist.
    pub fn asset_mut(&mut self, key: &ExchangeAsset<AssetNameInternal>) -> &mut AssetState {
        self.0
            .get_mut(key)
            .unwrap_or_else(|| panic!("AssetStates does not contain: {key:?}"))
    }

    /// Return an `Iterator` of filtered `AssetState`s based on the provided [`AssetFilter`].
    pub fn filtered<'a>(&'a self, filter: &'a AssetFilter) -> impl Iterator<Item = &'a AssetState> {
        use filter::AssetFilter::*;
        match filter {
            None => Either::Left(self.assets()),
            Exchanges(exchanges) => Either::Right(self.0.iter().filter_map(|(asset, state)| {
                if exchanges.contains(&asset.exchange) {
                    Some(state)
                } else {
                    Option::<&AssetState>::None
                }
            })),
        }
    }

    /// Returns an `Iterator` of all `AssetState`s being tracked.
    pub fn assets(&self) -> impl Iterator<Item = &AssetState> {
        self.0.values()
    }
}

/// Represents the current state of an asset, including its [`Balance`] and last update
/// `time_exchange`.
///
/// When used in the context of [`AssetStates`], this state is for an exchange asset, but it could
/// be used for a "global" asset that aggregates data for similar named assets on multiple
/// exchanges.
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize, Constructor)]
pub struct AssetState {
    /// `Asset` name data that details the internal and exchange names.
    pub asset: Asset,

    /// TearSheet generator for summarising trading session changes the asset.
    pub statistics: TearSheetAssetGenerator,

    /// Current balance of the asset and associated exchange timestamp.
    pub balance: Option<Timed<Balance>>,
}

impl AssetState {
    /// Updates the `AssetState` from an [`AssetBalance`] snapshot, if the snapshot is more recent.
    ///
    /// This method ensures temporal consistency by only applying updates from snapshots that
    /// are at least as recent as the current state.
    pub fn update_from_balance<AssetKey>(&mut self, snapshot: Snapshot<&AssetBalance<AssetKey>>) {
        let Some(balance) = &mut self.balance else {
            self.balance = Some(Timed::new(snapshot.0.balance, snapshot.0.time_exchange));
            self.statistics.update_from_balance(snapshot);
            return;
        };

        if balance.time <= snapshot.value().time_exchange {
            balance.time = snapshot.value().time_exchange;
            balance.value = snapshot.value().balance;
            self.statistics.update_from_balance(snapshot);
        }
    }

    /// Applies a WS partial [`AssetBalanceUpdate`] (`free`/`locked` only), if more recent.
    ///
    /// Updates `free` and recomputes `total = free + locked`, while **preserving** any existing
    /// [`MarginDetails`](rustrade_execution::balance::MarginDetails) — a stream update carries no
    /// debt, so this path structurally cannot clobber known `borrowed`/`interest`. On a cold start
    /// (no prior balance) the merged balance has `margin: None`, consistent with the debt-freshness
    /// contract (debt becomes known only via a [`BalanceSnapshot`](rustrade_execution::AccountEventKind::BalanceSnapshot)).
    ///
    /// Like [`Self::update_from_balance`], stale updates (older than the current state) are ignored.
    pub fn apply_balance_update<AssetKey>(
        &mut self,
        snapshot: Snapshot<&AssetBalanceUpdate<AssetKey>>,
    ) {
        let update = snapshot.value();

        // Timestamp-gate: ignore updates older than the current state.
        if let Some(balance) = &self.balance
            && balance.time > update.time_exchange
        {
            return;
        }

        // Preserve existing margin debt — a WS partial never carries it.
        let prior_margin = self.balance.as_ref().and_then(|b| b.value.margin);

        let merged = Balance {
            total: update.update.total(),
            free: update.update.free,
            margin: prior_margin,
        };
        self.balance = Some(Timed::new(merged, update.time_exchange));

        // Drawdown statistics track `total`, which the update changes — feed it through the same
        // path as a full snapshot. The generator needs only `balance`/`time_exchange`, so the
        // asset key (and a throwaway `AssetBalance`) is unnecessary here.
        self.statistics
            .update_from_balance_parts(merged, update.time_exchange);
    }
}

impl From<&AssetState> for AssetBalance<AssetNameExchange> {
    fn from(value: &AssetState) -> Self {
        let AssetState {
            asset,
            statistics: _,
            balance,
        } = value;

        let (balance, time_exchange) = match balance {
            None => (Balance::default(), Utc::now()),
            Some(balance) => (balance.value, balance.time),
        };

        Self {
            asset: asset.name_exchange.clone(),
            balance,
            time_exchange,
        }
    }
}

/// Generates an indexed [`AssetStates`] containing default asset balance data.
///
/// Note that the `time_exchange` is set to `DateTime::<Utc>::MIN_UTC`
pub fn generate_empty_indexed_asset_states(instruments: &IndexedInstruments) -> AssetStates {
    AssetStates(
        instruments
            .assets()
            .iter()
            .map(|asset| {
                (
                    ExchangeAsset::new(
                        asset.value.exchange,
                        asset.value.asset.name_internal.clone(),
                    ),
                    AssetState::new(
                        asset.value.asset.clone(),
                        TearSheetAssetGenerator::default(),
                        None,
                    ),
                )
            })
            .collect(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::test_utils::asset_state;
    use chrono::{DateTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use rustrade_execution::balance::{BalanceUpdate, MarginDetails};
    use rustrade_instrument::{asset::name::AssetNameExchange, exchange::ExchangeId};

    #[test]
    fn test_update_from_balance_with_first_ever_snapshot() {
        let mut state = AssetState {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("btc"),
            },
            statistics: Default::default(),
            balance: None,
        };

        let snapshot = Snapshot(AssetBalance {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("btc"),
            },
            balance: Balance::new(dec!(1100.0), dec!(1100.0)),
            time_exchange: DateTime::<Utc>::MIN_UTC,
        });

        state.update_from_balance(snapshot.as_ref());

        let expected = asset_state("btc", 1100.0, 1100.0, DateTime::<Utc>::MIN_UTC);

        assert_eq!(state, expected)
    }

    #[test]
    fn test_update_from_balance_with_more_recent_snapshot() {
        let mut state = asset_state("btc", 1000.0, 1000.0, DateTime::<Utc>::MIN_UTC);

        let snapshot = Snapshot(AssetBalance {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("xbt"),
            },
            balance: Balance::new(dec!(1100.0), dec!(1100.0)),
            time_exchange: DateTime::<Utc>::MAX_UTC,
        });

        state.update_from_balance(snapshot.as_ref());

        let expected = asset_state("btc", 1100.0, 1100.0, DateTime::<Utc>::MAX_UTC);

        assert_eq!(state, expected)
    }

    #[test]
    fn test_update_from_balance_with_equal_timestamp() {
        // Test case: Verify state updates when snapshot has equal timestamp
        let time = Utc.timestamp_opt(1000, 0).unwrap();

        let mut state = asset_state("btc", 1000.0, 900.0, time);

        let snapshot = Snapshot(AssetBalance {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("xbt"),
            },
            balance: Balance::new(dec!(1000.0), dec!(800.0)),
            time_exchange: time,
        });

        state.update_from_balance(snapshot.as_ref());

        assert_eq!(state.balance.unwrap().value.total, dec!(1000.0));
        assert_eq!(state.balance.unwrap().value.free, dec!(800.0));
        assert_eq!(state.balance.unwrap().time, time);
    }

    #[test]
    fn test_asset_states_serde_round_trip_preserves_index_and_key_lookup() {
        // Build AssetStates with two assets inserted in a known order so that insertion-order
        // index access (AssetIndex) is deterministic.
        let btc_key = ExchangeAsset::new(ExchangeId::BinanceSpot, AssetNameInternal::new("btc"));
        let usdt_key = ExchangeAsset::new(ExchangeId::BinanceSpot, AssetNameInternal::new("usdt"));

        let btc_state = asset_state("btc", 1.0, 0.5, DateTime::<Utc>::MIN_UTC);
        let usdt_state = asset_state("usdt", 1000.0, 1000.0, DateTime::<Utc>::MIN_UTC);

        let original = AssetStates(
            [
                (btc_key.clone(), btc_state.clone()),
                (usdt_key.clone(), usdt_state.clone()),
            ]
            .into_iter()
            .collect(),
        );

        // Serialise → deserialise round-trip.
        let json = serde_json::to_string(&original).unwrap();
        let restored: AssetStates = serde_json::from_str(&json).unwrap();

        // Full equality check — sequence is preserved.
        assert_eq!(original, restored);

        // Index lookup: BTC was inserted first → AssetIndex(0), USDT second → AssetIndex(1).
        assert_eq!(restored.asset_index(&AssetIndex(0)), &btc_state);
        assert_eq!(restored.asset_index(&AssetIndex(1)), &usdt_state);

        // Key lookup.
        assert_eq!(restored.asset(&btc_key), &btc_state);
        assert_eq!(restored.asset(&usdt_key), &usdt_state);
    }

    #[test]
    fn test_update_from_balance_with_stale_snapshot() {
        let mut state = asset_state("btc", 1000.0, 900.0, DateTime::<Utc>::MAX_UTC);

        let snapshot = Snapshot(AssetBalance {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("xbt"),
            },
            balance: Balance::new(dec!(1000.0), dec!(800.0)),
            time_exchange: DateTime::<Utc>::MIN_UTC,
        });

        state.update_from_balance(snapshot.as_ref());

        let expected = asset_state("btc", 1000.0, 900.0, DateTime::<Utc>::MAX_UTC);

        assert_eq!(state, expected)
    }

    fn btc_balance_update(
        free: f64,
        locked: f64,
        time: DateTime<Utc>,
    ) -> AssetBalanceUpdate<AssetNameExchange> {
        AssetBalanceUpdate {
            asset: AssetNameExchange::new("btc"),
            update: BalanceUpdate::new(
                Decimal::try_from(free).unwrap(),
                Decimal::try_from(locked).unwrap(),
            ),
            time_exchange: time,
        }
    }

    #[test]
    fn test_apply_balance_update_preserves_margin_debt() {
        // Seed a margin balance carrying debt via a full snapshot...
        let time = Utc.timestamp_opt(1000, 0).unwrap();
        let mut state = asset_state("btc", 0.0, 0.0, time);
        let seed = Snapshot(AssetBalance {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("btc"),
            },
            balance: Balance::new_margin(dec!(2.0), dec!(2.0), dec!(1.5), dec!(0.01)),
            time_exchange: time,
        });
        state.update_from_balance(seed.as_ref());

        // ...then apply a WS partial (free/locked only) that must NOT clobber the debt.
        let later = Utc.timestamp_opt(2000, 0).unwrap();
        let update = btc_balance_update(1.0, 0.5, later);
        state.apply_balance_update(Snapshot(&update));

        let balance = state.balance.unwrap().value;
        assert_eq!(balance.free, dec!(1.0));
        assert_eq!(balance.total, dec!(1.5)); // free + locked
        // Debt preserved from the snapshot — net_asset still deducts it.
        assert_eq!(
            balance.margin,
            Some(MarginDetails::new(dec!(1.5), dec!(0.01)))
        );
        assert_eq!(balance.net_asset(), dec!(0.0)); // 1.5 total - 1.5 borrowed
    }

    #[test]
    fn test_apply_balance_update_cold_start_has_no_margin() {
        // First-ever event is a WS partial → margin is None (debt unknown until a snapshot).
        let time = Utc.timestamp_opt(1000, 0).unwrap();
        let mut state = AssetState {
            asset: Asset {
                name_internal: AssetNameInternal::new("btc"),
                name_exchange: AssetNameExchange::new("btc"),
            },
            statistics: Default::default(),
            balance: None,
        };
        let update = btc_balance_update(3.0, 1.0, time);
        state.apply_balance_update(Snapshot(&update));

        let balance = state.balance.unwrap().value;
        assert_eq!(balance.total, dec!(4.0));
        assert_eq!(balance.free, dec!(3.0));
        assert_eq!(balance.margin, None);
        assert_eq!(balance.net_asset(), dec!(4.0));
    }

    #[test]
    fn test_apply_balance_update_equal_timestamp_is_applied() {
        // Mirrors update_from_balance: an update with a timestamp equal to the current state is
        // applied (the gate only rejects strictly-older updates).
        let time = Utc.timestamp_opt(1000, 0).unwrap();
        let mut state = asset_state("btc", 1000.0, 900.0, time);

        let update = btc_balance_update(1.5, 0.5, time);
        state.apply_balance_update(Snapshot(&update));

        assert_eq!(state.balance.unwrap().value.free, dec!(1.5));
        assert_eq!(state.balance.unwrap().value.total, dec!(2.0)); // free + locked
        assert_eq!(state.balance.unwrap().time, time);
    }

    #[test]
    fn test_apply_balance_update_ignores_stale() {
        let time = Utc.timestamp_opt(2000, 0).unwrap();
        let mut state = asset_state("btc", 1000.0, 900.0, time);

        let stale = btc_balance_update(1.0, 0.0, Utc.timestamp_opt(1000, 0).unwrap());
        state.apply_balance_update(Snapshot(&stale));

        // Unchanged: stale update older than current state is ignored.
        assert_eq!(state.balance.unwrap().value.total, dec!(1000.0));
        assert_eq!(state.balance.unwrap().value.free, dec!(900.0));
    }
}
