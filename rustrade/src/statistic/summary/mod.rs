use crate::{
    engine::state::{asset::AssetStates, instrument::InstrumentStates, position::PositionExited},
    statistic::{
        summary::{
            asset::{BalanceBasis, TearSheetAsset, TearSheetAssetGenerator},
            instrument::{TearSheet, TearSheetGenerator},
        },
        time::TimeInterval,
    },
};
use chrono::{DateTime, TimeDelta, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_execution::balance::AssetBalance;
use rustrade_instrument::{
    asset::{AssetIndex, ExchangeAsset, name::AssetNameInternal},
    instrument::{InstrumentIndex, name::InstrumentNameInternal},
};
use rustrade_integration::collection::{FnvIndexMap, snapshot::Snapshot};
use serde::{Deserialize, Serialize};

pub mod asset;
pub mod dataset;
pub mod display;
pub mod instrument;
pub mod pnl;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Constructor)]
pub struct TradingSummary<Interval> {
    /// Trading session start time defined by the [`Engine`](crate::engine::Engine) clock.
    pub time_engine_start: DateTime<Utc>,

    /// Trading session end time defined by the [`Engine`](crate::engine::Engine) clock.
    pub time_engine_end: DateTime<Utc>,

    /// Instrument [`TearSheet`]s.
    ///
    /// Note that an Instrument is unique to an exchange, so, for example, Binance btc_usdt_spot
    /// and Okx btc_usdt_spot will be summarised by distinct [`TearSheet`]s.
    pub instruments: FnvIndexMap<InstrumentNameInternal, TearSheet<Interval>>,

    /// [`ExchangeAsset`] [`TearSheet`]s.
    pub assets: FnvIndexMap<ExchangeAsset<AssetNameInternal>, TearSheetAsset>,

    /// [`BalanceBasis`] the asset drawdown and end-of-session balance figures were computed from.
    ///
    /// Reported once at the summary level (a session uses one basis for all assets). `#[serde(default)]`
    /// so summaries serialised before this field existed load as [`BalanceBasis::Gross`].
    #[serde(default)]
    pub basis: BalanceBasis,

    /// Open requests sent this session, summed over every instrument.
    ///
    /// `#[serde(default)]` so summaries serialised before this field existed still load.
    #[serde(default)]
    pub orders_opened: usize,

    /// Of those, how many the exchange rejected.
    ///
    /// See [`Self::rejected_every_order`] for why this is worth checking before reading any
    /// other figure in the summary.
    #[serde(default)]
    pub orders_rejected: usize,

    /// Fills this session that opened a position under their raw exchange `OrderId` despite
    /// belonging to a known order, summed over every instrument — `OmsMode::Hedging` only.
    ///
    /// Non-zero means at least one order's PnL is **split across two position slots**, so the
    /// per-position figures in this summary partition that order's activity in a way the strategy
    /// never chose. See [`Self::has_split_positions`], and
    /// [`TearSheet::fallback_positions`](instrument::TearSheet::fallback_positions) for which
    /// positions to inspect.
    #[serde(default)]
    pub fills_routed_by_fallback: usize,

    /// Fills this session for which no order was tracked at all, summed over every instrument —
    /// `OmsMode::Hedging` only.
    ///
    /// Reported apart from [`Self::fills_routed_by_fallback`] because it is not by itself a fault:
    /// an account also traded by hand or by another system produces these legitimately, and one
    /// position per external order is a reasonable reading of them.
    #[serde(default)]
    pub fills_unmatched: usize,
}

impl<Interval> TradingSummary<Interval> {
    /// Whether every open request this session was rejected.
    ///
    /// When true, the session filled nothing and every ratio in this summary was computed over
    /// zero trades — a tear sheet of zeros that looks like a flat strategy rather than a broken
    /// run. A strategy that simply chose not to trade sends no requests at all and so is not
    /// reported here.
    ///
    /// Per-instrument counts and the first rejection reason are on each
    /// [`TearSheet`].
    pub fn rejected_every_order(&self) -> bool {
        self.orders_opened > 0 && self.orders_rejected == self.orders_opened
    }

    /// Whether any fill was booked against a position other than the one its order opened.
    ///
    /// When true, at least one order's PnL is divided between two position slots and nothing
    /// rejoins them, so every per-position figure in this summary describes a partition of the
    /// session that the strategy did not choose. Worth checking before reading them.
    ///
    /// Deliberately does **not** consider [`Self::fills_unmatched`]: a fill with no order behind it
    /// is expected whenever the account is traded from elsewhere, and folding it in here would
    /// raise this flag on correct operation.
    ///
    /// The positions to inspect are on each
    /// [`TearSheet::fallback_positions`](instrument::TearSheet::fallback_positions).
    pub fn has_split_positions(&self) -> bool {
        self.fills_routed_by_fallback > 0
    }

    /// Duration of trading that the `TradingSummary` covers.
    pub fn trading_duration(&self) -> TimeDelta {
        self.time_engine_end
            .signed_duration_since(self.time_engine_start)
    }
}

/// Generator for a [`TradingSummary`].
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Constructor)]
pub struct TradingSummaryGenerator {
    /// Theoretical rate of return of an investment with zero risk.
    ///
    /// See docs: <https://www.investopedia.com/terms/r/risk-freerate.asp>
    pub risk_free_return: Decimal,

    /// Trading session summary start time defined by the [`Engine`](crate::engine::Engine) clock.
    pub time_engine_start: DateTime<Utc>,

    /// Trading session summary most recent update time defined by the
    /// [`Engine`](crate::engine::Engine) clock.
    pub time_engine_now: DateTime<Utc>,

    /// Instrument [`TearSheetGenerator`]s.
    ///
    /// Note that an Instrument is unique to an exchange, so, for example, Binance btc_usdt_spot
    /// and Okx btc_usdt_spot will be summarised by distinct [`TearSheet`]s.
    pub instruments: FnvIndexMap<InstrumentNameInternal, TearSheetGenerator>,

    /// [`ExchangeAsset`] [`TearSheetAssetGenerator`]s.
    pub assets: FnvIndexMap<ExchangeAsset<AssetNameInternal>, TearSheetAssetGenerator>,
}

impl TradingSummaryGenerator {
    /// Initialise a [`TradingSummaryGenerator`] from a `risk_free_return` value, and initial
    /// indexed state.
    pub fn init<InstrumentData>(
        risk_free_return: Decimal,
        time_engine_start: DateTime<Utc>,
        time_engine_now: DateTime<Utc>,
        instruments: &InstrumentStates<InstrumentData>,
        assets: &AssetStates,
    ) -> Self {
        Self {
            risk_free_return,
            time_engine_start,
            time_engine_now,
            instruments: instruments
                .0
                .values()
                .map(|state| {
                    (
                        state.instrument.name_internal.clone(),
                        state.tear_sheet.clone(),
                    )
                })
                .collect(),
            assets: assets
                .0
                .iter()
                .map(|(asset, state)| (asset.clone(), state.statistics.clone()))
                .collect(),
        }
    }

    /// Update the [`TradingSummaryGenerator`] `time_now`.
    pub fn update_time_now(&mut self, time_now: DateTime<Utc>) {
        self.time_engine_now = time_now;
    }

    /// Update the [`TradingSummaryGenerator`] from the next [`PositionExited`].
    pub fn update_from_position<AssetKey, InstrumentKey>(
        &mut self,
        position: &PositionExited<AssetKey, InstrumentKey>,
    ) where
        Self: InstrumentTearSheetManager<InstrumentKey>,
    {
        if self.time_engine_now < position.time_exit {
            self.time_engine_now = position.time_exit;
        }

        self.instrument_mut(&position.instrument)
            .update_from_position(position)
    }

    /// Update the [`TradingSummaryGenerator`] from the next [`Snapshot`] [`AssetBalance`].
    pub fn update_from_balance<AssetKey>(&mut self, balance: Snapshot<&AssetBalance<AssetKey>>)
    where
        Self: AssetTearSheetManager<AssetKey>,
    {
        if self.time_engine_now < balance.0.time_exchange {
            self.time_engine_now = balance.0.time_exchange;
        }

        self.asset_mut(&balance.0.asset)
            .update_from_balance(balance)
    }

    /// Generate the latest [`TradingSummary`] at the specific [`TimeInterval`].
    ///
    /// For example, pass [`Annual365`](super::time::Annual365) to generate a crypto-centric
    /// (24/7 trading) annualised [`TradingSummary`].
    pub fn generate<Interval>(&mut self, interval: Interval) -> TradingSummary<Interval>
    where
        Interval: TimeInterval,
    {
        let instruments = self
            .instruments
            .iter_mut()
            .map(|(instrument, tear_sheet)| {
                (
                    instrument.clone(),
                    tear_sheet.generate(self.risk_free_return, interval),
                )
            })
            .collect();

        let assets = self
            .assets
            .iter_mut()
            .map(|(asset, tear_sheet)| (asset.clone(), tear_sheet.generate()))
            .collect();

        // Single source of truth: the per-asset generators all carry the session basis (set once at
        // engine-state construction; the field is crate-private and never mutated after, so this
        // uniformity is an enforced invariant), so derive the report-level basis from them rather
        // than storing a redundant field. Empty (no assets) defaults to Gross — no rows to label.
        let basis = self
            .assets
            .values()
            .next()
            .map(|generator| generator.basis)
            .unwrap_or_default();

        let (orders_opened, orders_rejected, fills_routed_by_fallback, fills_unmatched) =
            self.instruments.values().fold(
                (0usize, 0usize, 0usize, 0usize),
                |(opened, rejected, fallback, unmatched), generator| {
                    (
                        opened.saturating_add(generator.orders_opened),
                        rejected.saturating_add(generator.orders_rejected),
                        fallback.saturating_add(generator.fills_routed_by_fallback),
                        unmatched.saturating_add(generator.fills_unmatched),
                    )
                },
            );

        TradingSummary {
            time_engine_start: self.time_engine_start,
            time_engine_end: self.time_engine_now,
            instruments,
            assets,
            basis,
            orders_opened,
            orders_rejected,
            fills_routed_by_fallback,
            fills_unmatched,
        }
    }
}

pub trait InstrumentTearSheetManager<InstrumentKey> {
    fn instrument(&self, key: &InstrumentKey) -> &TearSheetGenerator;
    fn instrument_mut(&mut self, key: &InstrumentKey) -> &mut TearSheetGenerator;
}

impl InstrumentTearSheetManager<InstrumentNameInternal> for TradingSummaryGenerator {
    fn instrument(&self, key: &InstrumentNameInternal) -> &TearSheetGenerator {
        self.instruments
            .get(key)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }

    fn instrument_mut(&mut self, key: &InstrumentNameInternal) -> &mut TearSheetGenerator {
        self.instruments
            .get_mut(key)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }
}

impl InstrumentTearSheetManager<InstrumentIndex> for TradingSummaryGenerator {
    fn instrument(&self, key: &InstrumentIndex) -> &TearSheetGenerator {
        self.instruments
            .get_index(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }

    fn instrument_mut(&mut self, key: &InstrumentIndex) -> &mut TearSheetGenerator {
        self.instruments
            .get_index_mut(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }
}

pub trait AssetTearSheetManager<AssetKey> {
    fn asset(&self, key: &AssetKey) -> &TearSheetAssetGenerator;
    fn asset_mut(&mut self, key: &AssetKey) -> &mut TearSheetAssetGenerator;
}

impl AssetTearSheetManager<AssetIndex> for TradingSummaryGenerator {
    fn asset(&self, key: &AssetIndex) -> &TearSheetAssetGenerator {
        self.assets
            .get_index(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }

    fn asset_mut(&mut self, key: &AssetIndex) -> &mut TearSheetAssetGenerator {
        self.assets
            .get_index_mut(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key}"))
    }
}

impl AssetTearSheetManager<ExchangeAsset<AssetNameInternal>> for TradingSummaryGenerator {
    fn asset(&self, key: &ExchangeAsset<AssetNameInternal>) -> &TearSheetAssetGenerator {
        self.assets
            .get(key)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key:?}"))
    }

    fn asset_mut(
        &mut self,
        key: &ExchangeAsset<AssetNameInternal>,
    ) -> &mut TearSheetAssetGenerator {
        self.assets
            .get_mut(key)
            .unwrap_or_else(|| panic!("TradingSummaryGenerator does not contain: {key:?}"))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::statistic::{summary::asset::BalanceBasis, time::Annual365};
    use rust_decimal_macros::dec;
    use rustrade_instrument::exchange::ExchangeId;

    fn generator_with_assets(
        assets: FnvIndexMap<ExchangeAsset<AssetNameInternal>, TearSheetAssetGenerator>,
    ) -> TradingSummaryGenerator {
        TradingSummaryGenerator::new(
            dec!(0),
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC,
            FnvIndexMap::default(),
            assets,
        )
    }

    /// The report-level basis on the generated `TradingSummary` is derived from the per-asset
    /// generators (which all carry the session basis), so a net-asset session is reported as such.
    #[test]
    fn generate_stamps_basis_from_asset_generators() {
        let mut assets = FnvIndexMap::default();
        assets.insert(
            ExchangeAsset::new(ExchangeId::BinanceSpot, AssetNameInternal::new("btc")),
            TearSheetAssetGenerator {
                basis: BalanceBasis::NetAsset,
                ..Default::default()
            },
        );

        let mut generator = generator_with_assets(assets);
        assert_eq!(generator.generate(Annual365).basis, BalanceBasis::NetAsset);
    }

    /// With no assets there is nothing to derive a basis from (and no rows to label), so the report
    /// defaults to Gross.
    #[test]
    fn generate_with_no_assets_defaults_basis_to_gross() {
        let mut generator = generator_with_assets(FnvIndexMap::default());
        assert_eq!(generator.generate(Annual365).basis, BalanceBasis::Gross);
    }

    /// The session totals are the sum over instruments, not a copy of any one of them: a split can
    /// happen on one instrument while another trades cleanly, and a summary that reported either in
    /// isolation would mislead in opposite directions.
    #[test]
    fn generate_sums_fallback_counters_over_every_instrument() {
        let mut instruments = FnvIndexMap::default();
        for (name, routed, unmatched) in [("btc_usdt", 2usize, 1usize), ("eth_usdt", 3, 4)] {
            let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
            generator.fills_routed_by_fallback = routed;
            generator.fills_unmatched = unmatched;
            instruments.insert(InstrumentNameInternal::new(name), generator);
        }

        let summary = TradingSummaryGenerator::new(
            dec!(0),
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC,
            instruments,
            FnvIndexMap::default(),
        )
        .generate(Annual365);

        assert_eq!(summary.fills_routed_by_fallback, 5);
        assert_eq!(summary.fills_unmatched, 5);
        assert!(
            summary.has_split_positions(),
            "a split on any instrument is a split for the session"
        );
    }

    /// `has_split_positions` keys off the split counter alone. An account traded from elsewhere
    /// produces unmatched fills during correct operation, and flagging those would raise the alarm
    /// on a healthy session.
    #[test]
    fn unmatched_fills_alone_do_not_flag_the_session_as_split() {
        let mut instruments = FnvIndexMap::default();
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.fills_unmatched = 9;
        instruments.insert(InstrumentNameInternal::new("btc_usdt"), generator);

        let summary = TradingSummaryGenerator::new(
            dec!(0),
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC,
            instruments,
            FnvIndexMap::default(),
        )
        .generate(Annual365);

        assert_eq!(summary.fills_unmatched, 9);
        assert_eq!(summary.fills_routed_by_fallback, 0);
        assert!(!summary.has_split_positions());
    }
}
