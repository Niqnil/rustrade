//! Drift between the positions a venue reports and the positions the engine holds.

use super::PositionManager;
use rust_decimal::Decimal;
use rustrade_instrument::{Side, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};

/// The engine's net position in an instrument differs from the position its venue reported in an
/// account snapshot.
///
/// Found by [`EngineState::position_drift`](crate::engine::state::EngineState::position_drift),
/// and emitted by the engine as
/// [`EngineOutput::PositionDrift`](crate::engine::EngineOutput::PositionDrift). The engine only
/// reports it: what to do about it (alert, halt, re-seed on restart) is the caller's decision.
///
/// Only the signed quantities are compared. The entry prices are carried for information: venues
/// legitimately differ from the engine there, for example IBKR folds commissions into its entry
/// price.
///
/// `#[non_exhaustive]`: built by the engine.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub struct PositionDrift<InstrumentKey = InstrumentIndex> {
    /// The instrument whose positions differ.
    pub instrument: InstrumentKey,
    /// The engine's signed net quantity, positive when long: the sum over its position slots,
    /// see [`PositionManager::quantity_net`].
    pub quantity_engine: Decimal,
    /// The venue's signed quantity, positive when long, and zero when it reported the instrument
    /// flat.
    pub quantity_venue: Decimal,
    /// The engine's average entry price, when it holds exactly one position in the instrument.
    pub price_entry_engine: Option<Decimal>,
    /// The venue's average entry price, when it reported an open position with one.
    pub price_entry_venue: Option<Decimal>,
}

impl<InstrumentKey> PositionDrift<InstrumentKey> {
    pub(crate) fn new(
        instrument: InstrumentKey,
        quantity_engine: Decimal,
        quantity_venue: Decimal,
        price_entry_engine: Option<Decimal>,
        price_entry_venue: Option<Decimal>,
    ) -> Self {
        Self {
            instrument,
            quantity_engine,
            quantity_venue,
            price_entry_engine,
            price_entry_venue,
        }
    }
}

impl<AssetKey, InstrumentKey> PositionManager<AssetKey, InstrumentKey> {
    /// The signed net quantity of every open position, positive when long.
    ///
    /// Under [`OmsMode::Netting`](super::OmsMode::Netting) this is the one position's signed
    /// quantity. Under [`OmsMode::Hedging`](super::OmsMode::Hedging) longs and shorts offset, as a
    /// venue that nets them reports them.
    pub fn quantity_net(&self) -> Decimal {
        self.positions
            .values()
            .map(|position| match position.side {
                Side::Buy => position.quantity_abs,
                Side::Sell => -position.quantity_abs,
            })
            .sum()
    }

    /// The average entry price of the one open position, or `None` when there are none or
    /// several.
    pub(crate) fn price_entry_single(&self) -> Option<Decimal> {
        match self.positions.len() {
            1 => self
                .positions
                .values()
                .next()
                .map(|position| position.price_entry_average),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::engine::state::position::{OmsMode, PositionSeed};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::order::id::PositionId;
    use rustrade_instrument::asset::AssetIndex;

    fn seed(side: Side, quantity: Decimal, price: Decimal, id: Option<&str>) -> PositionSeed {
        let seed = PositionSeed::new("x", side, quantity, price, DateTime::<Utc>::MIN_UTC);
        match id {
            Some(id) => seed.with_position_id(PositionId::new(id)),
            None => seed,
        }
    }

    fn manager(mode: OmsMode, seeds: Vec<PositionSeed>) -> PositionManager {
        let mut manager = PositionManager::new(mode);
        for seed in seeds {
            seed.seed_into(
                &mut manager,
                InstrumentIndex(0),
                AssetIndex(0),
                Decimal::ONE,
            )
            .unwrap();
        }
        manager
    }

    #[test]
    fn quantity_net_and_single_entry_price() {
        let empty = manager(OmsMode::Netting, vec![]);
        assert_eq!(empty.quantity_net(), Decimal::ZERO);
        assert_eq!(empty.price_entry_single(), None);

        let short = manager(
            OmsMode::Netting,
            vec![seed(Side::Sell, dec!(2), dec!(50), None)],
        );
        assert_eq!(short.quantity_net(), dec!(-2));
        assert_eq!(short.price_entry_single(), Some(dec!(50)));

        // Hedging longs and shorts offset; with two slots there is no single entry price.
        let hedged = manager(
            OmsMode::Hedging,
            vec![
                seed(Side::Buy, dec!(3), dec!(10), Some("a")),
                seed(Side::Sell, dec!(1), dec!(12), Some("b")),
            ],
        );
        assert_eq!(hedged.quantity_net(), dec!(2));
        assert_eq!(hedged.price_entry_single(), None);
    }
}
