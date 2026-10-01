use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// An open position in an instrument whose holding is tracked separately from cash balances:
/// perpetuals, futures, options, and equities.
///
/// A holding a venue reports as an asset balance instead, such as spot crypto, has no
/// `Position`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Position {
    /// Signed quantity: positive = long, negative = short, zero = flat.
    ///
    /// Using signed quantity is the industry standard for positions and avoids
    /// a separate `side` field.
    pub quantity: Decimal,

    /// Average entry price, quoted as the instrument's orders are priced: for an option or a
    /// future, per unit of the underlying, not multiplied by the contract size. `None` if position
    /// is flat or entry price unavailable.
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
        self.quantity > Decimal::ZERO
    }

    /// Returns true if this is a short position (negative quantity). A negative zero is flat, not
    /// short.
    pub fn is_short(&self) -> bool {
        self.quantity < Decimal::ZERO
    }

    /// Returns the absolute position size.
    pub fn abs_quantity(&self) -> Decimal {
        self.quantity.abs()
    }
}

/// What a venue reported about its position in one instrument, in an
/// [`InstrumentAccountSnapshot`](crate::InstrumentAccountSnapshot).
///
/// Only [`Flat`](Self::Flat) and [`Open`](Self::Open) are claims about the position. A consumer
/// comparing the venue's position with its own (the engine does, see
/// `EngineOutput::PositionDrift` in `rustrade`) must compare only those, and read nothing into
/// [`Unreported`](Self::Unreported) or into an instrument the snapshot does not list.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum PositionReport {
    /// The venue does not report a position for this instrument: it does not report positions
    /// at all, it reports this holding as an asset balance instead (such as spot crypto), or
    /// this read could not establish it.
    #[default]
    Unreported,
    /// The venue reports positions for this instrument and holds none.
    Flat,
    /// The venue's open position. A client must not report a zero quantity as open: build it
    /// with [`Self::from_position`], which reports one as [`Flat`](Self::Flat).
    Open(Position),
}

impl PositionReport {
    /// Report a position the venue returned: [`Flat`](Self::Flat) when its quantity is zero,
    /// otherwise [`Open`](Self::Open).
    pub fn from_position(position: Position) -> Self {
        if position.is_flat() {
            Self::Flat
        } else {
            Self::Open(position)
        }
    }

    /// The open position, if any.
    pub fn open(&self) -> Option<&Position> {
        match self {
            Self::Open(position) => Some(position),
            Self::Unreported | Self::Flat => None,
        }
    }

    /// The venue's signed quantity: zero when [`Flat`](Self::Flat), `None` when
    /// [`Unreported`](Self::Unreported).
    pub fn quantity(&self) -> Option<Decimal> {
        match self {
            Self::Unreported => None,
            Self::Flat => Some(Decimal::ZERO),
            Self::Open(position) => Some(position.quantity),
        }
    }

    /// Whether this is [`Unreported`](Self::Unreported).
    pub fn is_unreported(&self) -> bool {
        matches!(self, Self::Unreported)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// The snapshot field is absent when unreported, `"Flat"` when flat and `{"Open": ...}` when
    /// open, and an absent field reads back as unreported.
    #[test]
    fn instrument_snapshot_position_serde() {
        use crate::InstrumentAccountSnapshot;
        use rustrade_instrument::{
            asset::name::AssetNameExchange, exchange::ExchangeId,
            instrument::name::InstrumentNameExchange,
        };
        type Snapshot =
            InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

        let open = Position::new(dec!(-2), Some(dec!(10)), None, None, None, None, Utc::now());
        for (position, expected) in [
            (PositionReport::Unreported, None),
            (PositionReport::Flat, Some(serde_json::json!("Flat"))),
            (
                PositionReport::Open(open.clone()),
                Some(serde_json::json!({ "Open": serde_json::to_value(&open).unwrap() })),
            ),
        ] {
            let snapshot = Snapshot::new("x".into(), vec![], false, position, None);
            let json = serde_json::to_value(&snapshot).unwrap();
            assert_eq!(json.get("position").cloned(), expected);
            assert_eq!(serde_json::from_value::<Snapshot>(json).unwrap(), snapshot);
        }
    }

    #[test]
    fn position_report_from_position_reports_zero_as_flat() {
        let now = Utc::now();
        let open = Position::new(dec!(-2), None, None, None, None, None, now);
        assert_eq!(
            PositionReport::from_position(open.clone()),
            PositionReport::Open(open.clone())
        );
        for zero in [dec!(0), -dec!(0)] {
            let flat = Position::new(zero, None, None, None, None, None, now);
            assert_eq!(PositionReport::from_position(flat), PositionReport::Flat);
        }
        assert_eq!(PositionReport::Open(open).quantity(), Some(dec!(-2)));
        assert_eq!(PositionReport::Flat.quantity(), Some(Decimal::ZERO));
        assert_eq!(PositionReport::Unreported.quantity(), None);
    }

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

        // Negative zero, as `Decimal` can produce from arithmetic or parsing "-0", is flat too.
        for zero in [dec!(0), -dec!(0)] {
            let flat = Position::new(zero, None, None, None, None, None, now);
            assert!(!flat.is_long(), "{zero:?}");
            assert!(!flat.is_short(), "{zero:?}");
            assert!(flat.is_flat(), "{zero:?}");
        }
    }

    #[test]
    fn test_abs_quantity() {
        let now = Utc::now();
        let short = Position::new(dec!(-2.5), None, None, None, None, None, now);
        assert_eq!(short.abs_quantity(), dec!(2.5));
    }
}
