//! Margin borrowing: the borrow and repay policy a margin account applies to its orders, and the
//! simulated venue's spot margin configuration.
//!
//! [`MarginSideEffect`] is the policy a live margin client sends with each order, and the one the
//! simulated venue applies when [`SpotMarginConfig`] turns its spot margin on.

use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Whether a margin account borrows what an order lacks and repays its loans from what an order
/// receives, fixed once per client or simulated venue.
///
/// Only the two *intent-agnostic* modes are modelled. Whether an order opens or closes a position
/// decides whether it should borrow or repay, and that is position state the library deliberately
/// does not track, so the venue is left to infer it from the account's loans. The per-order modes
/// (Binance's `MARGIN_BUY` / `AUTO_REPAY`) are intentionally **not** modelled here; they belong to
/// a future generic per-order `RequestOpen::margin_effect` field that all margin adapters would
/// map, added only when a venue with genuine per-order borrow intent appears. Mixed borrow
/// appetite on a single venue today means running two clients (one per mode).
///
/// The `Serialize`/`Deserialize` wire form is the config-file value (`"auto_borrow_repay"` /
/// `"no_borrow"`), which is not any venue's API value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MarginSideEffect {
    /// Borrow what an order lacks when it is placed, and repay loans from what an order receives,
    /// as Binance's `AUTO_BORROW_REPAY` does.
    ///
    /// # Warning
    /// This is the default because it makes shorting work out of the box, but it means a
    /// **mis-sized order silently takes on debt**: an order larger than the free balance borrows
    /// the shortfall without any further opt-in (same footgun class as `RequestOpen::reduce_only`).
    /// Use [`MarginSideEffect::NoBorrow`] for a client that must never silently borrow.
    #[default]
    AutoBorrowRepay,
    /// Never borrow or repay; an order that would need to borrow is rejected, as Binance's
    /// `NO_SIDE_EFFECT` is. The conservative opt-out from the
    /// [`AutoBorrowRepay`](Self::AutoBorrowRepay) silent-borrow behaviour.
    NoBorrow,
}

/// Spot margin on the simulated venue: what it lends a spot order, how much, and what it repays.
///
/// Off by default: `MockExecutionConfig::spot_margin` is `None`, and the venue is a cash account
/// whose spot orders must be paid for with what it holds. With a configuration, it is a cross
/// margin account, with one pool of balances shared by every instrument, which lends a spot order
/// what it lacks and reports each balance's loan as Binance's margin account does.
///
/// # What it lends, and when
/// Under [`MarginSideEffect::AutoBorrowRepay`], an order the account cannot pay for with its free
/// balance borrows the shortfall, and only the shortfall, of the asset it pays with: base for a
/// sell, quote for a buy. It borrows when the venue accepts the order, as Binance does, so a
/// resting limit order or a held market order is lent its shortfall at once, and the venue holds
/// it against the order as it holds any other balance. An order that is cancelled or expires
/// before it fills repays what it borrowed, interest first. One that fills in part and rests the
/// remainder pays for the part that filled with the account's own balance first, so the loan is
/// held against the remainder and repaid if the remainder is cancelled.
///
/// Under [`MarginSideEffect::NoBorrow`], the venue lends nothing and repays nothing: an order it
/// cannot pay for is rejected as on a cash account, and a loan already in the venue's initial
/// state is left as it is. Balances still report their loans.
///
/// Only spot orders borrow. A CFD's margin is posted from the quote asset's free balance, as
/// without this configuration.
///
/// # How much it lends
/// A borrow is allowed while the account's debt, after it, is at most `max_leverage − 1` times
/// the account's equity. Both are valued in [`valuation_asset`](Self::valuation_asset):
/// - Each asset is valued at the mid of an instrument on the venue that pairs it with the
///   valuation asset, else that market's last price, in either direction (`BTC/USDT`, or the
///   inverse of `USDT/BTC`). The valuation asset is worth one. A market priced at zero values
///   nothing.
/// - **Equity** is the sum over assets of `total − borrowed − interest`, valued that way. An asset
///   the venue cannot value counts as no equity, which understates what the account can borrow
///   rather than overstating it.
/// - **Debt** is the sum over assets of `borrowed + interest`, valued the same way. The venue
///   lends nothing while it cannot value a debt, and never lends an asset it cannot value.
///
/// An order that needs more than that is rejected as unaffordable, with a reason that says how
/// much the venue would lend. A `max_leverage` of one lends nothing, but still reports each
/// balance's loan.
///
/// # What it repays
/// Under [`MarginSideEffect::AutoBorrowRepay`], whatever a fill delivers into an asset the account
/// owes repays that asset's loan, interest first, then principal, and only the rest becomes free
/// balance. What a buy delivers is its quantity less any fee charged in base, so a buy of exactly
/// what is owed, with a base fee, leaves a fee-sized loan. Gross the buy up by the fee to clear
/// it.
///
/// # What it does not model
/// - **Liquidation.** An account whose debt grows past its equity keeps trading; nothing closes
///   its positions.
/// - **Isolated margin.** One pool of balances backs every instrument.
/// - **Per-asset borrow limits and the venue's lending pool.** Only the leverage limit applies.
/// - **Interest.** No interest is charged on a loan by this configuration alone.
///
/// The constructors refuse a `max_leverage` below one, and so does deserialising one, so a
/// configuration that exists is one the venue can apply.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "SpotMarginRepr", into = "SpotMarginRepr")]
pub struct SpotMarginConfig(SpotMarginRepr);

/// The serialised form of a [`SpotMarginConfig`], before validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct SpotMarginRepr {
    valuation_asset: AssetNameExchange,
    #[serde(with = "rust_decimal::serde::str")]
    max_leverage: Decimal,
    #[serde(default)]
    side_effect: MarginSideEffect,
}

impl SpotMarginConfig {
    /// Spot margin that borrows and repays automatically
    /// ([`MarginSideEffect::AutoBorrowRepay`]), up to `max_leverage` over the account's equity,
    /// valued in `valuation_asset`.
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError`] if `max_leverage` is below one.
    pub fn new(
        valuation_asset: AssetNameExchange,
        max_leverage: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        Self::try_from(SpotMarginRepr {
            valuation_asset,
            max_leverage,
            side_effect: MarginSideEffect::AutoBorrowRepay,
        })
    }

    /// This configuration, applying `side_effect` instead.
    #[must_use]
    pub fn with_side_effect(mut self, side_effect: MarginSideEffect) -> Self {
        self.0.side_effect = side_effect;
        self
    }

    /// The asset equity and debt are valued in.
    pub fn valuation_asset(&self) -> &AssetNameExchange {
        &self.0.valuation_asset
    }

    /// The most the account's debt may be, as a multiple of its equity, plus one.
    pub fn max_leverage(&self) -> Decimal {
        self.0.max_leverage
    }

    /// Whether the venue borrows and repays automatically.
    pub fn side_effect(&self) -> MarginSideEffect {
        self.0.side_effect
    }
}

impl TryFrom<SpotMarginRepr> for SpotMarginConfig {
    type Error = SpotMarginConfigError;

    fn try_from(repr: SpotMarginRepr) -> Result<Self, Self::Error> {
        if repr.max_leverage < Decimal::ONE {
            return Err(SpotMarginConfigError::LeverageBelowOne(repr.max_leverage));
        }
        Ok(Self(repr))
    }
}

impl From<SpotMarginConfig> for SpotMarginRepr {
    fn from(config: SpotMarginConfig) -> Self {
        config.0
    }
}

/// Why a [`SpotMarginConfig`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SpotMarginConfigError {
    /// A `max_leverage` below one, which would allow less than the account's own equity.
    #[error("spot margin max_leverage {0} is below 1; a leverage of 1 lends nothing")]
    LeverageBelowOne(Decimal),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn a_leverage_below_one_is_refused_by_the_constructor_and_by_deserialising() {
        assert_eq!(
            SpotMarginConfig::new(AssetNameExchange::new("usdt"), dec!(0.5)),
            Err(SpotMarginConfigError::LeverageBelowOne(dec!(0.5)))
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"0.5"}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn the_side_effect_defaults_to_auto_borrow_repay_when_omitted() {
        let json = r#"{"valuation_asset":"usdt","max_leverage":"3"}"#;
        let config: SpotMarginConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.side_effect(), MarginSideEffect::AutoBorrowRepay);
        assert_eq!(config.max_leverage(), dec!(3));
        assert_eq!(config.valuation_asset(), &AssetNameExchange::new("usdt"));
    }

    #[test]
    fn a_configuration_round_trips_through_its_serialised_form() {
        let config = SpotMarginConfig::new(AssetNameExchange::new("usdt"), dec!(3))
            .unwrap()
            .with_side_effect(MarginSideEffect::NoBorrow);

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","side_effect":"no_borrow"}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
    }
}
