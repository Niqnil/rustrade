use crate::{
    engine::state::position::{NetPnlError, PositionExited, calculate_pnl_return},
    statistic::summary::dataset::DataSetSummary,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Records Profit and Loss (PnL) data.
///
/// Every figure is net of carry: a closed position counts as its
/// [`pnl_realised_net`](PositionExited::pnl_realised_net), realised PnL plus carry.
///
/// Raw PnL also counts the fees of fills that opened no position (see
/// [`Self::fees_unattributed`]). They are a cost of trading the instrument, but belong to no round
/// trip, so the per-position return statistics leave them out.
///
/// Includes tracking of:
/// - Raw PnL, and the carry and unattributed fees included in it.
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
    /// Raw PnL, net of carry and of [`Self::fees_unattributed`].
    ///
    /// For Instrument PnL, this is most likely denominated in "quote asset" units. For example,
    /// btc_usdt  spot PnL would be in usdt. However, in some derivative cases the
    /// "settlement asset" could be different from the "quote asset.
    ///
    /// For Portfolio and Strategy PnL, this could be denominated in any asset chosen to aggregate
    /// PnL across different instruments.
    pub pnl_raw: Decimal,

    /// Carry included in `pnl_raw`: funding, interest and borrow fees, signed (positive =
    /// received), in the same asset. Base-asset carry is valued at each position's exit price;
    /// carry in any other asset is in neither.
    ///
    /// `#[serde(default)]` so a summary serialised before this field existed still loads.
    #[serde(default)]
    pub carry: Decimal,

    /// How many positions had base-asset carry and no exit price, so counted in `carry` and
    /// `pnl_raw` with their quote carry alone: ones removed without a closing trade, such as by a
    /// reverse split that rounds them to zero. Zero when every closed position was priced.
    ///
    /// `#[serde(default)]` so a summary serialised before this field existed still loads.
    #[serde(default)]
    pub positions_carry_unpriced: u64,

    /// Fees of fills that opened no position, already subtracted from `pnl_raw`: positive =
    /// paid, negative = a rebate received. Each is the fee's quote equivalent, as a position's
    /// PnL counts one.
    ///
    /// A spot buy from flat whose fee the venue took from the base asset it bought, all of it,
    /// moves no holding, so it has no position to charge the fee to. See
    /// [`InstrumentState::update_from_trade`](crate::engine::state::instrument::InstrumentState::update_from_trade).
    ///
    /// `#[serde(default)]` so a summary serialised before this field existed still loads.
    #[serde(default)]
    pub fees_unattributed: Decimal,

    /// PnL returns statistical summary for wins and losses.
    pub total: DataSetSummary,

    /// PnL returns statistical summary for losses only.
    pub losses: DataSetSummary,
}

impl PnLReturns {
    /// Update the `PnLReturns` from the next [`PositionExited`].
    ///
    /// Uses **checked** `Decimal` arithmetic throughout so a corrupt or extreme `pnl_realised` or
    /// `carry` cannot panic this reporting path:
    /// - If the position's net PnL is not representable, the whole update is **skipped** with a
    ///   `warn!`.
    /// - The raw-PnL and carry totals each hold their last-good value on overflow (consistent with
    ///   `Position::update_pnl_realised`) and emit a `warn!`.
    /// - If the per-position return is not representable (see [`calculate_pnl_return`]), that single
    ///   data point is **skipped** — `total` and `losses` are left untouched and a `warn!` is emitted
    ///   — rather than dropping the whole update.
    pub fn update<AssetKey, InstrumentKey>(
        &mut self,
        position: &PositionExited<AssetKey, InstrumentKey>,
    ) {
        // Base-asset carry is valued at the exit price. A position removed without a closing trade
        // has none, so it counts with its quote carry alone, and is counted and warned about:
        // skipping it would drop its realised PnL from the summary.
        let carry = match position.carry_valued() {
            Ok(carry) => carry,
            Err(NetPnlError::BaseCarryUnpriced) => {
                self.positions_carry_unpriced = self.positions_carry_unpriced.saturating_add(1);
                warn!(
                    base_carry = %position.carry_assets.base,
                    "position has base-asset carry but no exit price; counting its quote carry only"
                );
                position.carry
            }
            Err(NetPnlError::Overflow) => {
                warn!(
                    position_pnl_realised = %position.pnl_realised,
                    position_carry = %position.carry,
                    "position net PnL overflowed Decimal; skipping this position"
                );
                return;
            }
        };
        let Some(pnl_net) = position.pnl_realised.checked_add(carry) else {
            warn!(
                position_pnl_realised = %position.pnl_realised,
                position_carry = %carry,
                "position net PnL overflowed Decimal; skipping this position"
            );
            return;
        };

        // Checked, hold-last-good on overflow (consistent with `Position::update_pnl_realised`):
        // an accumulation that can't be represented holds the prior total rather than panicking
        // this reporting path.
        match self.pnl_raw.checked_add(pnl_net) {
            Some(new_total) => self.pnl_raw = new_total,
            None => warn!(
                pnl_raw_held = %self.pnl_raw,
                position_pnl_net = %pnl_net,
                "pnl_raw accumulation overflowed Decimal; holding last-good value"
            ),
        }
        match self.carry.checked_add(carry) {
            Some(new_total) => self.carry = new_total,
            None => warn!(
                carry_held = %self.carry,
                position_carry = %carry,
                "carry accumulation overflowed Decimal; holding last-good value"
            ),
        }

        let Some(pnl_return) = calculate_pnl_return(
            pnl_net,
            position.price_entry_average,
            position.quantity_abs_max,
        ) else {
            warn!(
                position_pnl_net = %pnl_net,
                "pnl_return computation overflowed Decimal; skipping this return data point"
            );
            return;
        };

        self.total.update(pnl_return);

        if pnl_return.is_sign_negative() {
            self.losses.update(pnl_return)
        }
    }

    /// Charge a fee that no position carries to raw PnL, and add it to
    /// [`Self::fees_unattributed`]. `fee` is the fee's quote equivalent, positive = paid.
    ///
    /// The return statistics are left alone: the fee belongs to no round trip. Checked as
    /// [`Self::update`] is: each total holds its last-good value on overflow and emits a `warn!`.
    pub fn update_from_fee_unattributed(&mut self, fee: Decimal) {
        match self.pnl_raw.checked_sub(fee) {
            Some(new_total) => self.pnl_raw = new_total,
            None => warn!(
                pnl_raw_held = %self.pnl_raw,
                fee = %fee,
                "pnl_raw accumulation overflowed Decimal; holding last-good value"
            ),
        }
        match self.fees_unattributed.checked_add(fee) {
            Some(new_total) => self.fees_unattributed = new_total,
            None => warn!(
                fees_unattributed_held = %self.fees_unattributed,
                fee = %fee,
                "fees_unattributed accumulation overflowed Decimal; holding last-good value"
            ),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use crate::engine::state::position::AssetCarry;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::id::PositionId,
        trade::{AssetFees, TradeId},
    };
    use rustrade_instrument::{Side, asset::QuoteAsset, instrument::name::InstrumentNameInternal};

    /// Build a closed-position record carrying three of the fields `PnLReturns::update` reads
    /// (`pnl_realised`, `price_entry_average`, `quantity_abs_max`), with zero `carry`; the rest are
    /// inert filler.
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
            carry: Decimal::ZERO,
            carry_assets: Default::default(),
            price_exit: None,
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
    fn test_update_is_net_of_carry() {
        // A position that made 10 on price and paid 4 of funding counts as a 6 win; one that lost
        // 1 on price and received 3 counts as a 2 win. Returns are on a 100 basis.
        let mut returns = PnLReturns::default();
        returns.update(&PositionExited {
            carry: dec!(-4),
            ..exited(dec!(10), dec!(100), dec!(1))
        });
        returns.update(&PositionExited {
            carry: dec!(3),
            ..exited(dec!(-1), dec!(100), dec!(1))
        });

        assert_eq!(returns.pnl_raw, dec!(8));
        assert_eq!(returns.carry, dec!(-1));
        assert_eq!(returns.total.count, dec!(2));
        assert_eq!(returns.total.sum, dec!(0.08));
        assert_eq!(returns.losses.count, Decimal::ZERO);
    }

    #[test]
    fn test_base_carry_is_valued_at_the_exit_price() {
        // 10 won on price; 0.02 of base interest paid, bought back at 150: 3 of cost.
        let mut returns = PnLReturns::default();
        returns.update(&PositionExited {
            carry: dec!(-1),
            carry_assets: AssetCarry {
                base: dec!(-0.02),
                other: vec![(QuoteAsset, dec!(-9))],
            },
            price_exit: Some(dec!(150)),
            ..exited(dec!(10), dec!(100), dec!(1))
        });

        assert_eq!(
            returns.pnl_raw,
            dec!(6),
            "10 - 1 - 3; the unpriced asset is left out"
        );
        assert_eq!(returns.carry, dec!(-4));
        assert_eq!(returns.positions_carry_unpriced, 0);
    }

    #[test]
    fn test_unpriced_base_carry_counts_the_position_with_its_quote_carry() {
        let mut returns = PnLReturns::default();
        returns.update(&PositionExited {
            carry: dec!(-1),
            carry_assets: AssetCarry {
                base: dec!(-0.02),
                other: Vec::new(),
            },
            price_exit: None,
            ..exited(dec!(10), dec!(100), dec!(1))
        });

        assert_eq!(returns.pnl_raw, dec!(9), "not dropped from the summary");
        assert_eq!(returns.carry, dec!(-1));
        assert_eq!(returns.total.count, Decimal::ONE);
        assert_eq!(returns.positions_carry_unpriced, 1);
    }

    #[test]
    fn test_unattributed_fee_reaches_pnl_raw_but_no_return() {
        // A fee paid with no position is a cost in raw PnL, and a rebate a gain, but neither is a
        // round trip, so no return sample is recorded for it.
        let mut returns = PnLReturns::default();
        returns.update(&exited(dec!(10), dec!(100), dec!(1)));
        returns.update_from_fee_unattributed(dec!(3));
        returns.update_from_fee_unattributed(dec!(-1));

        assert_eq!(returns.pnl_raw, dec!(8));
        assert_eq!(returns.fees_unattributed, dec!(2));
        assert_eq!(returns.carry, Decimal::ZERO);
        assert_eq!(returns.total.count, Decimal::ONE);
        assert_eq!(returns.total.sum, dec!(0.1));
    }

    #[test]
    fn test_unattributed_fee_holds_each_total_on_overflow() {
        // Each total holds its last-good value on its own overflow, and the other still moves.
        let mut returns = PnLReturns {
            pnl_raw: Decimal::MIN,
            ..Default::default()
        };
        returns.update_from_fee_unattributed(dec!(1));
        assert_eq!(returns.pnl_raw, Decimal::MIN);
        assert_eq!(returns.fees_unattributed, dec!(1));

        let mut returns = PnLReturns {
            fees_unattributed: Decimal::MAX,
            ..Default::default()
        };
        returns.update_from_fee_unattributed(dec!(1));
        assert_eq!(returns.pnl_raw, dec!(-1));
        assert_eq!(returns.fees_unattributed, Decimal::MAX);
    }

    #[test]
    fn test_a_summary_serialised_before_fees_unattributed_loads_with_zero() {
        let mut value = serde_json::to_value(PnLReturns {
            pnl_raw: dec!(5),
            fees_unattributed: dec!(2),
            ..Default::default()
        })
        .unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("fees_unattributed")
            .unwrap();

        let loaded: PnLReturns = serde_json::from_value(value).unwrap();
        assert_eq!(loaded.pnl_raw, dec!(5));
        assert_eq!(loaded.fees_unattributed, Decimal::ZERO);
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
