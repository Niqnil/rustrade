use crate::{
    Timed,
    statistic::metric::drawdown::{
        Drawdown, DrawdownGenerator,
        max::{MaxDrawdown, MaxDrawdownGenerator},
        mean::{MeanDrawdown, MeanDrawdownGenerator},
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_execution::balance::{AssetBalance, Balance};
use rustrade_integration::collection::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Which figure of a [`Balance`] the asset statistics (drawdown, end-of-session balance) are
/// computed from.
///
/// The library deliberately does **not** hard-code one basis: gross is always safe, but net-asset
/// is only meaningful for margin accounts that stay solvent (see [`Self::NetAsset`]). Picking the
/// basis is therefore caller policy, selected once via
/// [`EngineStateBuilder::balance_basis`](crate::engine::state::builder::EngineStateBuilder::balance_basis)
/// and defaulting to [`Self::Gross`] so existing/cash users see no change.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BalanceBasis {
    /// Gross holdings ([`Balance::total`]) — always non-negative, the safe default.
    #[default]
    Gross,

    /// Net asset value ([`Balance::net_asset`], `total - borrowed`).
    ///
    /// # Caller precondition — net must stay strictly positive
    /// Drawdown is only well-defined while net asset stays **strictly above zero**. The underlying
    /// [`DrawdownGenerator`] computes `(peak - point) / peak`, so under this basis:
    /// - a **zero** peak (fully borrowed, `net == 0`) makes the division return `None` and the
    ///   sample is silently dropped;
    /// - a **negative** peak (short held from session open) sign-flips the ratio, so a real loss
    ///   yields a negative "drawdown" that never exceeds the running max and is silently never
    ///   recorded.
    ///
    /// Both failures are silent. A consumer selecting `NetAsset` is responsible for keeping net
    /// asset strictly positive (or interpreting the drawdown metrics accordingly).
    ///
    /// # Freshness
    /// [`Balance::net_asset`] excludes accrued interest and reflects debt only as fresh as the last
    /// [`BalanceSnapshot`](rustrade_execution::AccountEventKind::BalanceSnapshot): WS partial
    /// updates carry no debt, so between snapshots `net_asset == total` and the value steps at
    /// snapshot boundaries.
    NetAsset,
}

impl BalanceBasis {
    /// Extract the [`Balance`] figure selected by this basis.
    pub fn value(self, balance: Balance) -> Decimal {
        match self {
            Self::Gross => balance.total,
            Self::NetAsset => balance.net_asset(),
        }
    }
}

impl fmt::Display for BalanceBasis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Gross => "gross",
            Self::NetAsset => "net asset",
        })
    }
}

/// TearSheet summarising the trading session changes for an Asset.
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TearSheetAsset {
    pub balance_end: Option<Balance>,
    pub drawdown: Option<Drawdown>,
    pub drawdown_mean: Option<MeanDrawdown>,
    pub drawdown_max: Option<MaxDrawdown>,
}

/// Generator for an [`TearSheetAsset`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Deserialize, Serialize)]
pub struct TearSheetAssetGenerator {
    /// Which [`Balance`] figure drawdown and end-of-session balance are computed from.
    ///
    /// `pub(crate)`: set once at construction (via `init`/`init_with_basis`, ultimately the
    /// [`EngineStateBuilder`](crate::engine::state::builder::EngineStateBuilder)) and never mutated
    /// afterwards. Keeping it crate-private makes the session-wide uniformity that
    /// [`TradingSummaryGenerator::generate`](crate::statistic::summary::TradingSummaryGenerator::generate)
    /// relies on to report a single basis an actual invariant, not a convention an external caller
    /// could break.
    ///
    /// See [`BalanceBasis::NetAsset`] for the net-asset precondition. `#[serde(default)]` so state
    /// serialised before this field existed loads as [`BalanceBasis::Gross`].
    #[serde(default)]
    pub(crate) basis: BalanceBasis,
    pub balance_now: Option<Balance>,
    pub drawdown: DrawdownGenerator,
    pub drawdown_mean: MeanDrawdownGenerator,
    pub drawdown_max: MaxDrawdownGenerator,
}

impl TearSheetAssetGenerator {
    /// Initialise a [`TearSheetAssetGenerator`] from an initial `AssetState`, using the default
    /// [`BalanceBasis::Gross`].
    pub fn init(initial: &Timed<Balance>) -> Self {
        Self::init_with_basis(initial, BalanceBasis::default())
    }

    /// Initialise a [`TearSheetAssetGenerator`] from an initial `AssetState` and an explicit
    /// [`BalanceBasis`].
    ///
    /// The drawdown seed and every subsequent update are taken through the same `basis`, so the
    /// seed-vs-update basis can never disagree. See [`BalanceBasis::NetAsset`] for the net-asset
    /// precondition.
    pub fn init_with_basis(initial: &Timed<Balance>, basis: BalanceBasis) -> Self {
        Self {
            basis,
            balance_now: Some(initial.value),
            drawdown: DrawdownGenerator::init(Timed::new(basis.value(initial.value), initial.time)),
            drawdown_mean: MeanDrawdownGenerator::default(),
            drawdown_max: MaxDrawdownGenerator::default(),
        }
    }

    /// Update the [`TearSheetAssetGenerator`] from the next [`Snapshot`] [`AssetBalance`].
    pub fn update_from_balance<AssetKey>(&mut self, balance: Snapshot<&AssetBalance<AssetKey>>) {
        self.update_from_balance_parts(balance.value().balance, balance.value().time_exchange);
    }

    /// Update the [`TearSheetAssetGenerator`] from a [`Balance`] and its exchange timestamp.
    ///
    /// The asset-key-free core of [`Self::update_from_balance`]; the generator tracks only `total`
    /// drawdown and so never needs the asset identity. Lets callers that hold a balance without an
    /// owned asset key (e.g. WS partial updates) avoid constructing a throwaway [`AssetBalance`].
    ///
    /// `time_exchange` is expected to be non-decreasing across calls; the underlying
    /// [`DrawdownGenerator`] tracks peak state and does not itself guard against out-of-order
    /// timestamps (the engine applies a staleness gate before calling this). `pub(crate)`: an
    /// internal helper for [`Self::update_from_balance`] and the engine's balance-apply path.
    pub(crate) fn update_from_balance_parts(
        &mut self,
        balance: Balance,
        time_exchange: DateTime<Utc>,
    ) {
        self.balance_now = Some(balance);

        if let Some(next_drawdown) = self
            .drawdown
            .update(Timed::new(self.basis.value(balance), time_exchange))
        {
            self.drawdown_mean.update(&next_drawdown);
            self.drawdown_max.update(&next_drawdown);
        }
    }

    /// Generate the latest [`TearSheetAsset`].
    pub fn generate(&mut self) -> TearSheetAsset {
        let current_drawdown = self.drawdown.generate();
        if let Some(drawdown) = &current_drawdown {
            self.drawdown_mean.update(drawdown);
            self.drawdown_max.update(drawdown);
        }

        TearSheetAsset {
            balance_end: self.balance_now,
            drawdown: current_drawdown,
            drawdown_mean: self.drawdown_mean.generate(),
            drawdown_max: self.drawdown_max.generate(),
        }
    }

    /// Reset the internal state, using a new starting `Timed<Balance>` as seed.
    ///
    /// Preserves the configured [`BalanceBasis`] — a reset re-seeds the session, not the policy.
    pub fn reset(&mut self, balance_start: &Timed<Balance>) {
        *self = Self::init_with_basis(balance_start, self.basis);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::test_utils::time_plus_days;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_instrument::asset::AssetIndex;

    fn balance(balance: Balance, time: DateTime<Utc>) -> AssetBalance<AssetIndex> {
        AssetBalance {
            asset: AssetIndex(0),
            balance,
            time_exchange: time,
        }
    }

    fn duration_ms(start: DateTime<Utc>, end: DateTime<Utc>) -> i64 {
        end.signed_duration_since(start).num_milliseconds()
    }

    #[test]
    fn test_tear_sheet_asset_generator() {
        struct TestCase {
            input: AssetBalance<AssetIndex>,
            expected: TearSheetAssetGenerator,
        }

        let base_time = DateTime::<Utc>::MIN_UTC;

        let mut generator = TearSheetAssetGenerator::init(&Timed::new(
            Balance::new(dec!(1.0), dec!(1.0)),
            base_time,
        ));

        let cases = vec![
            // TC0: Balance increased from 1.0 peak, so no expected drawdowns
            TestCase {
                input: balance(
                    Balance::new(dec!(2.0), dec!(2.0)),
                    time_plus_days(base_time, 1),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(2.0), dec!(2.0))),
                    drawdown: DrawdownGenerator::init(Timed::new(
                        dec!(2.0),
                        time_plus_days(base_time, 1),
                    )),
                    drawdown_mean: MeanDrawdownGenerator::default(),
                    drawdown_max: MaxDrawdownGenerator::default(),
                },
            },
            // TC1: Balance decreased, so expect a current drawdown only
            TestCase {
                input: balance(
                    Balance::new(dec!(1.5), dec!(1.5)),
                    time_plus_days(base_time, 2),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(1.5), dec!(1.5))),
                    drawdown: DrawdownGenerator {
                        peak: Some(dec!(2.0)),
                        drawdown_max: dec!(0.25), // (2.0 - 1.5) / 2.0,
                        time_peak: Some(time_plus_days(base_time, 1)),
                        time_now: time_plus_days(base_time, 2),
                    },
                    drawdown_mean: MeanDrawdownGenerator::default(),
                    drawdown_max: MaxDrawdownGenerator::default(),
                },
            },
            // TC2: Further decrease - larger drawdown
            TestCase {
                input: balance(
                    Balance::new(dec!(1.0), dec!(1.0)),
                    time_plus_days(base_time, 3),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(1.0), dec!(1.0))),
                    drawdown: DrawdownGenerator {
                        peak: Some(dec!(2.0)),
                        drawdown_max: dec!(0.5), // (2.0 - 1.0) / 2.0
                        time_peak: Some(time_plus_days(base_time, 1)),
                        time_now: time_plus_days(base_time, 3),
                    },
                    drawdown_mean: MeanDrawdownGenerator::default(),
                    drawdown_max: MaxDrawdownGenerator::default(),
                },
            },
            // TC3: Recovery above previous peak - should complete drawdown period
            TestCase {
                input: balance(
                    Balance::new(dec!(2.5), dec!(2.5)),
                    time_plus_days(base_time, 4),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(2.5), dec!(2.5))),
                    drawdown: DrawdownGenerator::init(Timed::new(
                        dec!(2.5),
                        time_plus_days(base_time, 4),
                    )),
                    drawdown_mean: MeanDrawdownGenerator {
                        count: 1,
                        mean_drawdown: Some(MeanDrawdown {
                            mean_drawdown: dec!(0.5), // Only one drawdown period completed
                            mean_drawdown_ms: duration_ms(
                                time_plus_days(base_time, 1),
                                time_plus_days(base_time, 4),
                            ),
                        }),
                    },
                    drawdown_max: MaxDrawdownGenerator {
                        max: Some(MaxDrawdown(Drawdown {
                            value: dec!(0.5),
                            time_start: time_plus_days(base_time, 1),
                            time_end: time_plus_days(base_time, 4),
                        })),
                    },
                },
            },
            // TC4: Small drawdown after new peak (2.5 -> 2.4)
            TestCase {
                input: balance(
                    Balance::new(dec!(2.4), dec!(2.4)),
                    time_plus_days(base_time, 5),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(2.4), dec!(2.4))),
                    drawdown: DrawdownGenerator {
                        peak: Some(dec!(2.5)),
                        drawdown_max: dec!(0.04), // (2.5 - 2.4) / 2.5
                        time_peak: Some(time_plus_days(base_time, 4)),
                        time_now: time_plus_days(base_time, 5),
                    },
                    drawdown_mean: MeanDrawdownGenerator {
                        count: 1,
                        mean_drawdown: Some(MeanDrawdown {
                            mean_drawdown: dec!(0.5), // Only one drawdown period completed
                            mean_drawdown_ms: duration_ms(
                                time_plus_days(base_time, 1),
                                time_plus_days(base_time, 4),
                            ),
                        }),
                    },
                    drawdown_max: MaxDrawdownGenerator {
                        max: Some(MaxDrawdown(Drawdown {
                            value: dec!(0.5),
                            time_start: time_plus_days(base_time, 1),
                            time_end: time_plus_days(base_time, 4),
                        })),
                    },
                },
            },
            // TC5: Equal to previous value - drawdown continues
            TestCase {
                input: balance(
                    Balance::new(dec!(2.4), dec!(2.4)),
                    time_plus_days(base_time, 6),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(2.4), dec!(2.4))),
                    drawdown: DrawdownGenerator {
                        peak: Some(dec!(2.5)),
                        drawdown_max: dec!(0.04), // (2.5 - 2.4) / 2.5
                        time_peak: Some(time_plus_days(base_time, 4)),
                        time_now: time_plus_days(base_time, 6),
                    },
                    drawdown_mean: MeanDrawdownGenerator {
                        count: 1,
                        mean_drawdown: Some(MeanDrawdown {
                            mean_drawdown: dec!(0.5), // Only one drawdown period completed
                            mean_drawdown_ms: duration_ms(
                                time_plus_days(base_time, 1),
                                time_plus_days(base_time, 4),
                            ),
                        }),
                    },
                    drawdown_max: MaxDrawdownGenerator {
                        max: Some(MaxDrawdown(Drawdown {
                            value: dec!(0.5),
                            time_start: time_plus_days(base_time, 1),
                            time_end: time_plus_days(base_time, 4),
                        })),
                    },
                },
            },
            // TC6: Tiny change, but still in drawdown - retain max drawdown from current period
            TestCase {
                input: balance(
                    Balance::new(dec!(2.41), dec!(2.41)),
                    time_plus_days(base_time, 7),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(2.41), dec!(2.41))),
                    drawdown: DrawdownGenerator {
                        peak: Some(dec!(2.5)),
                        drawdown_max: dec!(0.04), // (2.5 - 2.4) / 2.5
                        time_peak: Some(time_plus_days(base_time, 4)),
                        time_now: time_plus_days(base_time, 7),
                    },
                    drawdown_mean: MeanDrawdownGenerator {
                        count: 1,
                        mean_drawdown: Some(MeanDrawdown {
                            mean_drawdown: dec!(0.5), // Only one drawdown period completed
                            mean_drawdown_ms: duration_ms(
                                time_plus_days(base_time, 1),
                                time_plus_days(base_time, 4),
                            ),
                        }),
                    },
                    drawdown_max: MaxDrawdownGenerator {
                        max: Some(MaxDrawdown(Drawdown {
                            value: dec!(0.5),
                            time_start: time_plus_days(base_time, 1),
                            time_end: time_plus_days(base_time, 4),
                        })),
                    },
                },
            },
            // TC7: recovery above previous peak - should complete drawdown period
            TestCase {
                input: balance(
                    Balance::new(dec!(3.0), dec!(3.0)),
                    time_plus_days(base_time, 8),
                ),
                expected: TearSheetAssetGenerator {
                    basis: BalanceBasis::Gross,
                    balance_now: Some(Balance::new(dec!(3.0), dec!(3.0))),
                    drawdown: DrawdownGenerator::init(Timed::new(
                        dec!(3.0),
                        time_plus_days(base_time, 8),
                    )),
                    drawdown_mean: MeanDrawdownGenerator {
                        count: 2,
                        mean_drawdown: Some(MeanDrawdown {
                            mean_drawdown: dec!(0.27), // (0.5 + 0.04) / 2
                            mean_drawdown_ms: (duration_ms(
                                time_plus_days(base_time, 1),
                                time_plus_days(base_time, 4),
                            ) + duration_ms(
                                time_plus_days(base_time, 4),
                                time_plus_days(base_time, 8),
                            )) / 2,
                        }),
                    },
                    drawdown_max: MaxDrawdownGenerator {
                        max: Some(MaxDrawdown(Drawdown {
                            value: dec!(0.5),
                            time_start: time_plus_days(base_time, 1),
                            time_end: time_plus_days(base_time, 4),
                        })),
                    },
                },
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            generator.update_from_balance(Snapshot(&test.input));
            assert_eq!(generator, test.expected, "TC{index} failed");
        }
    }

    #[test]
    fn balance_basis_value_selects_total_or_net_asset() {
        let cash = Balance::new(dec!(100.0), dec!(100.0));
        // Gross == net for cash (no margin).
        assert_eq!(BalanceBasis::Gross.value(cash), dec!(100.0));
        assert_eq!(BalanceBasis::NetAsset.value(cash), dec!(100.0));

        // Margin: net deducts borrowed principal (interest excluded).
        let margin = Balance::new_margin(dec!(200.0), dec!(50.0), dec!(150.0), dec!(5.0));
        assert_eq!(BalanceBasis::Gross.value(margin), dec!(200.0));
        assert_eq!(BalanceBasis::NetAsset.value(margin), dec!(50.0));

        // Short: net asset is negative (borrowed base asset exceeds holdings).
        let short = Balance::new_margin(dec!(0.0), dec!(0.0), dec!(10.0), dec!(0.0));
        assert_eq!(BalanceBasis::NetAsset.value(short), dec!(-10.0));
    }

    #[test]
    fn balance_basis_display() {
        assert_eq!(BalanceBasis::Gross.to_string(), "gross");
        assert_eq!(BalanceBasis::NetAsset.to_string(), "net asset");
    }

    #[test]
    fn balance_basis_serde_snake_case_and_default() {
        // Serialises to snake_case.
        assert_eq!(
            serde_json::to_string(&BalanceBasis::NetAsset).unwrap(),
            "\"net_asset\""
        );
        assert_eq!(
            serde_json::from_str::<BalanceBasis>("\"gross\"").unwrap(),
            BalanceBasis::Gross
        );

        // A generator serialised before `basis` existed (field absent) loads as Gross.
        let legacy = r#"{"balance_now":null,"drawdown":{"peak":null,"drawdown_max":"0","time_peak":null,"time_now":"-262143-01-01T00:00:00Z"},"drawdown_mean":{"count":0,"mean_drawdown":null},"drawdown_max":{"max":null}}"#;
        let generator: TearSheetAssetGenerator = serde_json::from_str(legacy).unwrap();
        assert_eq!(generator.basis, BalanceBasis::Gross);
    }

    /// Cash balances are basis-invariant: a Gross generator reports the same drawdown a NetAsset one
    /// would, because `net_asset == total` with no margin. (Default-Gross ⇒ no change for cash users.)
    #[test]
    fn cash_balance_gross_equals_net_asset() {
        let base_time = DateTime::<Utc>::MIN_UTC;
        let seed = Timed::new(Balance::new(dec!(100.0), dec!(100.0)), base_time);

        let mut gross = TearSheetAssetGenerator::init(&seed);
        let mut net = TearSheetAssetGenerator::init_with_basis(&seed, BalanceBasis::NetAsset);

        // total 100 -> 80 (a 20% drawdown either way for a cash balance).
        let next = Balance::new(dec!(80.0), dec!(80.0));
        gross.update_from_balance_parts(next, time_plus_days(base_time, 1));
        net.update_from_balance_parts(next, time_plus_days(base_time, 1));

        // `generate` is `&mut self` (it folds the in-progress drawdown into the mean/max sub-
        // generators), so bind each result once rather than calling it repeatedly.
        let (gross_sheet, net_sheet) = (gross.generate(), net.generate());
        assert_eq!(gross_sheet.drawdown, net_sheet.drawdown);
        assert_eq!(gross_sheet.drawdown.unwrap().value, dec!(0.2));
    }

    /// Margin balance: Gross tracks `total` (200 -> 180 = 10% drawdown), while `balance_now.total`
    /// stays the raw gross figure regardless of basis.
    #[test]
    fn margin_gross_tracks_total() {
        let base_time = DateTime::<Utc>::MIN_UTC;
        let seed = Timed::new(
            Balance::new_margin(dec!(200.0), dec!(50.0), dec!(150.0), dec!(0.0)),
            base_time,
        );
        let mut generator = TearSheetAssetGenerator::init_with_basis(&seed, BalanceBasis::Gross);

        let next = Balance::new_margin(dec!(180.0), dec!(30.0), dec!(150.0), dec!(0.0));
        generator.update_from_balance_parts(next, time_plus_days(base_time, 1));

        let tear_sheet = generator.generate();
        assert_eq!(tear_sheet.drawdown.unwrap().value, dec!(0.1)); // (200 - 180) / 200
        assert_eq!(tear_sheet.balance_end.unwrap().total, dec!(180.0));
    }

    /// Margin balance under NetAsset: drawdown tracks net (50 -> 30 = 40% drawdown), but the reported
    /// `balance_now.total` is still the raw gross 180 — only the drawdown basis changes.
    #[test]
    fn margin_net_asset_tracks_net() {
        let base_time = DateTime::<Utc>::MIN_UTC;
        let seed = Timed::new(
            Balance::new_margin(dec!(200.0), dec!(50.0), dec!(150.0), dec!(0.0)),
            base_time,
        );
        let mut generator = TearSheetAssetGenerator::init_with_basis(&seed, BalanceBasis::NetAsset);

        // net: 200-150=50 -> 180-150=30.
        let next = Balance::new_margin(dec!(180.0), dec!(30.0), dec!(150.0), dec!(0.0));
        generator.update_from_balance_parts(next, time_plus_days(base_time, 1));

        let tear_sheet = generator.generate();
        assert_eq!(tear_sheet.drawdown.unwrap().value, dec!(0.4)); // (50 - 30) / 50
        assert_eq!(tear_sheet.balance_end.unwrap().total, dec!(180.0));
    }

    /// Known limitation (documented on [`BalanceBasis::NetAsset`]): when the net-asset peak is zero,
    /// `(peak - point) / peak` divides by zero, `checked_div` returns `None`, and the drawdown sample
    /// is silently dropped — no drawdown is ever recorded even though net moved.
    #[test]
    fn net_asset_zero_peak_silently_drops_drawdown() {
        let base_time = DateTime::<Utc>::MIN_UTC;
        // Seed peak at net == 0 (fully borrowed): total 100, borrowed 100.
        let seed = Timed::new(
            Balance::new_margin(dec!(100.0), dec!(0.0), dec!(100.0), dec!(0.0)),
            base_time,
        );
        let mut generator = TearSheetAssetGenerator::init_with_basis(&seed, BalanceBasis::NetAsset);

        // net moves 0 -> -20 (a real loss), but the zero peak makes the ratio undefined.
        let next = Balance::new_margin(dec!(100.0), dec!(0.0), dec!(120.0), dec!(0.0));
        generator.update_from_balance_parts(next, time_plus_days(base_time, 1));

        // Silent: the public tear sheet records neither a drawdown nor a max-drawdown.
        let sheet = generator.generate();
        assert_eq!(sheet.drawdown, None);
        assert_eq!(sheet.drawdown_max, None);
    }

    /// `reset` preserves the configured basis rather than reverting to Gross.
    #[test]
    fn reset_preserves_basis() {
        let base_time = DateTime::<Utc>::MIN_UTC;
        let seed = Timed::new(Balance::new(dec!(100.0), dec!(100.0)), base_time);
        let mut generator = TearSheetAssetGenerator::init_with_basis(&seed, BalanceBasis::NetAsset);

        let new_seed = Timed::new(Balance::new(dec!(200.0), dec!(200.0)), base_time);
        generator.reset(&new_seed);

        assert_eq!(generator.basis, BalanceBasis::NetAsset);
    }
}
