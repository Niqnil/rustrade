use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Represents an open position in a derivative instrument (perpetuals, futures, margin).
///
/// For spot instruments, positions are implicit in asset balances — this struct is only
/// used for instruments that track position state separately from cash balances.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Position {
    /// Signed quantity: positive = long, negative = short, zero = flat.
    ///
    /// Using signed quantity is the industry standard for derivatives and avoids
    /// a separate `side` field.
    pub quantity: Decimal,

    /// Average entry price. `None` if position is flat or entry price unavailable.
    pub entry_price: Option<Decimal>,

    /// Unrealized PnL in quote currency. `None` if not provided by exchange.
    pub unrealized_pnl: Option<Decimal>,

    /// Margin/collateral allocated to this position. `None` if not applicable.
    pub margin_used: Option<Decimal>,

    /// Liquidation price. `None` for cross-margin or if not provided.
    pub liquidation_price: Option<Decimal>,

    /// Leverage setting. `None` if not applicable (e.g., spot-margin).
    pub leverage: Option<Decimal>,

    /// Exchange timestamp when this position state was reported.
    pub time_exchange: DateTime<Utc>,
}

impl Position {
    /// Returns true if this position is flat (zero quantity).
    pub fn is_flat(&self) -> bool {
        self.quantity.is_zero()
    }

    /// Returns true if this is a long position (positive quantity).
    pub fn is_long(&self) -> bool {
        self.quantity.is_sign_positive() && !self.quantity.is_zero()
    }

    /// Returns true if this is a short position (negative quantity).
    pub fn is_short(&self) -> bool {
        self.quantity.is_sign_negative()
    }

    /// Returns the absolute position size.
    pub fn abs_quantity(&self) -> Decimal {
        self.quantity.abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_position_side_detection() {
        let now = Utc::now();

        let long = Position::new(dec!(1.5), None, None, None, None, None, now);
        assert!(long.is_long());
        assert!(!long.is_short());
        assert!(!long.is_flat());

        let short = Position::new(dec!(-1.5), None, None, None, None, None, now);
        assert!(!short.is_long());
        assert!(short.is_short());
        assert!(!short.is_flat());

        let flat = Position::new(dec!(0), None, None, None, None, None, now);
        assert!(!flat.is_long());
        assert!(!flat.is_short());
        assert!(flat.is_flat());
    }

    #[test]
    fn test_abs_quantity() {
        let now = Utc::now();
        let short = Position::new(dec!(-2.5), None, None, None, None, None, now);
        assert_eq!(short.abs_quantity(), dec!(2.5));
    }
}
