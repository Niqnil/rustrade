use crate::{
    engine::state::position::{PositionExited, calculate_pnl_return},
    statistic::summary::dataset::DataSetSummary,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Records Profit and Loss (PnL) data.
///
/// Includes tracking of:
/// - Raw PnL.
/// - Statistical summaries of returns for all closed positions (wins and losses combined)
/// - Statistical summaries of returns for all losing closed positions (useful for downside risk analysis).
///
/// # Asset Denomination
/// The raw PnL values can be denominated in different assets depending on the context:
/// - For Instrument PnL:
///   - Usually denominated in the quote asset (e.g., USDT for BTC-USDT spot)
///   - For derivatives, may be in the settlement asset if different from quote
/// - For Portfolio/Strategy PnL:
///   - Can be denominated in any chosen asset for cross-instrument aggregation
#[derive(Debug, Clone, PartialEq, PartialOrd, Default, Deserialize, Serialize)]
pub struct PnLReturns {
    /// Raw PnL.
    ///
    /// For Instrument PnL, this is most likely denominated in "quote asset" units. For example,
    /// btc_usdt  spot PnL would be in usdt. However, in some derivative cases the
    /// "settlement asset" could be different from the "quote asset.
    ///
    /// For Portfolio and Strategy PnL, this could be denominated in any asset chosen to aggregate
    /// PnL across different instruments.
    pub pnl_raw: Decimal,

    /// PnL returns statistical summary for wins and losses.
    pub total: DataSetSummary,

    /// PnL returns statistical summary for losses only.
    pub losses: DataSetSummary,
}

impl PnLReturns {
    /// Update the `PnLReturns` from the next [`PositionExited`].
    ///
    /// Uses **checked** `Decimal` arithmetic on both accumulations so a corrupt or extreme
    /// `pnl_realised` cannot panic this reporting path:
    /// - The raw-PnL total holds its last-good value on overflow (consistent with
    ///   `Position::update_pnl_realised`) and emits a `warn!`.
    /// - If the per-position return is not representable (see [`calculate_pnl_return`]), that single
    ///   data point is **skipped** — `total` and `losses` are left untouched and a `warn!` is emitted
    ///   — rather than dropping the whole update.
    pub fn update<AssetKey, InstrumentKey>(
        &mut self,
        position: &PositionExited<AssetKey, InstrumentKey>,
    ) {
        // Checked, hold-last-good on overflow (consistent with `Position::update_pnl_realised`):
        // a raw-PnL accumulation that can't be represented holds the prior total rather than
        // panicking this reporting path.
        match self.pnl_raw.checked_add(position.pnl_realised) {
            Some(new_total) => self.pnl_raw = new_total,
            None => warn!(
                pnl_raw_held = %self.pnl_raw,
                position_pnl_realised = %position.pnl_realised,
                "pnl_raw accumulation overflowed Decimal; holding last-good value"
            ),
        }

        let Some(pnl_return) = calculate_pnl_return(
            position.pnl_realised,
            position.price_entry_average,
            position.quantity_abs_max,
        ) else {
            warn!(
                position_pnl_realised = %position.pnl_realised,
                "pnl_return computation overflowed Decimal; skipping this return data point"
            );
            return;
        };

        self.total.update(pnl_return);

        if pnl_return.is_sign_negative() {
            self.losses.update(pnl_return)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::id::PositionId,
        trade::{AssetFees, TradeId},
    };
    use rustrade_instrument::{Side, asset::QuoteAsset, instrument::name::InstrumentNameInternal};

    /// Build a closed-position record carrying the three fields `PnLReturns::update` reads
    /// (`pnl_realised`, `price_entry_average`, `quantity_abs_max`); the rest are inert filler.
    fn exited(
        pnl_realised: Decimal,
        price_entry_average: Decimal,
        quantity_abs_max: Decimal,
    ) -> PositionExited<QuoteAsset, InstrumentNameInternal> {
        PositionExited {
            position_id: PositionId::NETTING,
            instrument: InstrumentNameInternal::new("btc_usdt"),
            side: Side::Buy,
            price_entry_average,
            quantity_abs_max,
            pnl_realised,
            fees_enter: AssetFees {
                asset: QuoteAsset,
                fees: dec!(0.0),
                fees_quote: Some(dec!(0.0)),
            },
            fees_exit: AssetFees {
                asset: QuoteAsset,
                fees: dec!(0.0),
                fees_quote: Some(dec!(0.0)),
            },
            time_enter: DateTime::<Utc>::MIN_UTC,
            time_exit: DateTime::<Utc>::MIN_UTC,
            trades: vec![TradeId::new("t")],
        }
    }

    #[test]
    fn test_update_holds_pnl_raw_on_overflow() {
        // Mirrors the position-layer overflow-safety tests for the statistics layer: a raw-PnL
        // accumulation that overflows `Decimal` must NOT panic and must hold the last-good `pnl_raw`
        // rather than corrupting the running total. Needs a `~Decimal::MAX` prior total —
        // unreachable with real data.
        let mut returns = PnLReturns {
            pnl_raw: Decimal::MAX,
            ..Default::default()
        };

        // `Decimal::MAX + 1` overflows the checked accumulation ⇒ no panic, prior total held.
        returns.update(&exited(dec!(1.0), dec!(100.0), dec!(1.0)));

        assert_eq!(returns.pnl_raw, Decimal::MAX);
    }

    #[test]
    fn test_update_skips_data_point_when_return_not_representable() {
        // When `calculate_pnl_return` cannot represent the return (here the cost-of-investment basis
        // `price_entry_average × quantity_abs_max` overflows `Decimal`), that single sample is
        // skipped: `total`/`losses` stay untouched and the update does not panic.
        let mut returns = PnLReturns::default();

        // basis = `Decimal::MAX × 2` overflows ⇒ `calculate_pnl_return` returns `None` ⇒ skip.
        returns.update(&exited(dec!(100.0), Decimal::MAX, dec!(2.0)));

        // `pnl_raw` still accumulates (that step did not overflow), but no return sample was recorded.
        assert_eq!(returns.pnl_raw, dec!(100.0));
        assert_eq!(returns.total, DataSetSummary::default());
        assert_eq!(returns.losses, DataSetSummary::default());
    }
}
