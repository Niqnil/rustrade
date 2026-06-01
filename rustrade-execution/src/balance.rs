use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct AssetBalance<AssetKey> {
    pub asset: AssetKey,
    pub balance: Balance,
    pub time_exchange: DateTime<Utc>,
}

/// Per-asset margin debt detail for venues that report borrowing on a per-asset basis
/// (the CEX per-asset-margin class — Binance / Kraken / Bitfinex / dYdX).
///
/// `borrowed` and `interest` are always co-reported by such venues' REST snapshots, so they
/// are modelled as a single `Option`-of-struct on [`Balance`] rather than two loose `Option`s —
/// this keeps them co-present and preserves `Balance`'s `Copy`/`Eq`/`Ord`/`Hash`/`Default`.
///
/// Account-level-margin venues (e.g. IBKR, which reports only aggregate `EquityWithLoanValue` /
/// `MaintMarginReq`, never per-asset `borrowed`/`interest`) legitimately leave [`Balance::margin`]
/// as `None`.
///
/// Futures/perps *position* margin (maintenance margin, unrealised PnL) is a different model and
/// is intentionally **not** represented here.
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Constructor,
)]
pub struct MarginDetails {
    /// Outstanding borrowed principal for the asset.
    pub borrowed: Decimal,
    /// Accrued, unpaid interest on the borrowed principal.
    pub interest: Decimal,
}

/// Per-asset balance: gross holdings, the freely-tradable portion, and optional margin debt.
///
/// Construct via [`Balance::new`] (cash) or [`Balance::new_margin`] (per-asset debt). `total` is
/// gross holdings and `free` the freely-tradable portion; the remainder (`total - free`, exposed
/// by [`Balance::used`]) is reserved against resting orders. Callers building the struct literally
/// must keep `free <= total`.
///
/// `margin: None` denotes a cash / no-debt context, **not** "unknown debt" — see [`Balance::margin`]
/// and [`Balance::net_asset`].
///
/// `Ord`/`PartialOrd` are derived and compare lexicographically over `(total, free, margin)`
/// (`None` sorts before `Some`); they exist for use as collection keys and generic bounds, not as a
/// financial ranking. Use [`Balance::net_asset`] for value comparisons that account for debt.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct Balance {
    /// Gross holdings of the asset (`free + locked`). Unaffected by any borrowing — debt is
    /// carried separately in [`Balance::margin`].
    pub total: Decimal,
    /// Portion of `total` available to trade (not reserved against resting orders).
    pub free: Decimal,
    /// Per-asset margin debt, present only for venues that report it (see [`MarginDetails`]).
    ///
    /// `None` means a cash / no-debt context — **not** "unknown debt". The REST account snapshot
    /// always populates this for a margin account, and the WS partial update ([`BalanceUpdate`])
    /// structurally cannot clear it, so `None` reliably denotes "no margin facility for this
    /// asset". See [`Balance::net_asset`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin: Option<MarginDetails>,
}

impl Balance {
    /// Construct a cash [`Balance`] with no margin debt (`margin: None`).
    pub const fn new(total: Decimal, free: Decimal) -> Self {
        Self {
            total,
            free,
            margin: None,
        }
    }

    /// Construct a margin [`Balance`] carrying per-asset `borrowed`/`interest` debt.
    pub const fn new_margin(
        total: Decimal,
        free: Decimal,
        borrowed: Decimal,
        interest: Decimal,
    ) -> Self {
        Self {
            total,
            free,
            margin: Some(MarginDetails { borrowed, interest }),
        }
    }

    /// Portion of `total` reserved against resting orders (`total - free`).
    pub fn used(&self) -> Decimal {
        self.total - self.free
    }

    /// Net asset value after deducting borrowed principal.
    ///
    /// Returns `total` when [`Balance::margin`] is `None` (a cash / no-debt context, where net
    /// asset is simply the gross holding) and `total - borrowed` when `Some`. The result can be
    /// **negative** — a short position is a negative net holding in the borrowed (base) asset.
    ///
    /// # Freshness
    /// The returned value reflects debt only as fresh as the last
    /// [`BalanceSnapshot`](crate::AccountEventKind::BalanceSnapshot) for this asset. WS partial
    /// updates keep `free`/`locked` live but never carry debt, so a position **borrowed since the
    /// last snapshot** reads as zero-debt (`net_asset == total`) until the next snapshot. Consumers
    /// needing exact live debt should react to the venue's borrow/repay event by refreshing the
    /// account snapshot.
    pub fn net_asset(&self) -> Decimal {
        match self.margin {
            Some(margin) => self.total - margin.borrowed,
            None => self.total,
        }
    }
}

/// Partial balance update carrying only `free`/`locked` — the shape delivered by exchange WS
/// user-data streams (e.g. Binance `outboundAccountPosition`, which reports `a`/`f`/`l` only).
///
/// Deliberately has **no** `margin` field: applying a `BalanceUpdate` therefore *structurally
/// cannot* clobber known per-asset debt. Authoritative `borrowed`/`interest` totals arrive only via
/// the REST [`BalanceSnapshot`](crate::AccountEventKind::BalanceSnapshot).
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Constructor,
)]
pub struct BalanceUpdate {
    /// Portion of holdings available to trade.
    pub free: Decimal,
    /// Portion of holdings reserved against resting orders.
    pub locked: Decimal,
}

impl BalanceUpdate {
    /// Gross holdings of the asset (`free + locked`).
    pub fn total(&self) -> Decimal {
        self.free + self.locked
    }
}

/// Per-asset WS partial balance update (the [`BalanceUpdate`] counterpart of [`AssetBalance`]).
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct AssetBalanceUpdate<AssetKey> {
    /// Asset the update applies to (exchange-native name or resolved index).
    pub asset: AssetKey,
    /// Partial `free`/`locked` payload delivered by the WS stream.
    pub update: BalanceUpdate,
    /// Exchange-reported timestamp of the update.
    pub time_exchange: DateTime<Utc>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn net_asset_cash_returns_total() {
        // No margin facility → net asset is simply the gross holding.
        let balance = Balance::new(dec!(100), dec!(80));
        assert_eq!(balance.net_asset(), dec!(100));
        assert_eq!(balance.used(), dec!(20));
    }

    #[test]
    fn net_asset_margin_deducts_borrowed() {
        // total - borrowed; interest does not affect net asset.
        let balance = Balance::new_margin(dec!(100), dec!(100), dec!(30), dec!(2));
        assert_eq!(balance.net_asset(), dec!(70));
    }

    #[test]
    fn net_asset_short_is_negative() {
        // A short borrows the base asset and sells it: holdings are ~0 but debt remains, so net
        // asset is negative.
        let balance = Balance::new_margin(dec!(0), dec!(0), dec!(1.5), dec!(0.001));
        assert_eq!(balance.net_asset(), dec!(-1.5));
    }

    #[test]
    fn balance_update_total() {
        let update = BalanceUpdate::new(dec!(1.5), dec!(0.5));
        assert_eq!(update.total(), dec!(2.0));
        assert_eq!(update.locked, dec!(0.5));
    }

    #[test]
    fn balance_default_has_no_margin() {
        assert_eq!(Balance::default().margin, None);
    }

    #[test]
    fn balance_serde_omits_margin_when_none() {
        let json = serde_json::to_string(&Balance::new(dec!(1), dec!(1))).unwrap();
        assert!(
            !json.contains("margin"),
            "cash balance must not serialise a margin field"
        );

        let margin = Balance::new_margin(dec!(1), dec!(1), dec!(0.5), dec!(0));
        let round_trip: Balance =
            serde_json::from_str(&serde_json::to_string(&margin).unwrap()).unwrap();
        assert_eq!(round_trip, margin);
    }
}
