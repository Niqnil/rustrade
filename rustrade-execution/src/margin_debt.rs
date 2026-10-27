//! A change in what a margin account owes in one asset, as the venue streams it: a borrow, a repay
//! or an interest charge.
//!
//! An account stream reports each one as
//! [`AccountEventKind::MarginDebt`](crate::AccountEventKind::MarginDebt), carrying a
//! [`MarginDebtUpdate`]. The engine applies one that names no instrument to the asset's
//! [`Balance::margin`](crate::balance::Balance::margin), so debt stays current between REST
//! account snapshots.
use crate::balance::MarginDetails;
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt::{Display, Formatter};

/// What a margin account owes in one asset after a change, as the venue reported it.
///
/// Sent as [`AccountEventKind::MarginDebt`](crate::AccountEventKind::MarginDebt).
///
/// # Totals, not deltas
///
/// [`debt`](Self::debt) is everything owed in [`asset`](Self::asset) once the change is applied:
/// the whole principal and the whole unpaid interest, not the amount the change borrowed, repaid or
/// charged. Applying an update therefore replaces the debt the consumer held, and applying the same
/// update twice changes nothing. A debt repaid in full is reported as zero, not left out.
///
/// # Ordering
///
/// A venue may send these on a channel of their own, unordered against the account's fills and
/// balance updates. Order them by [`time_exchange`](Self::time_exchange), not by arrival, and
/// against debt alone: a newer free/locked balance update says nothing about debt. An update can
/// be missed while a stream is disconnected; a REST account snapshot restates the debt.
///
/// # Producers
///
/// Known producers, as of writing:
/// - Binance margin (`BinanceMargin`): `USER_LIABILITY_CHANGE`. See the client's rustdoc for which
///   streams it is read from, and how an isolated account names the pair.
/// - The simulated venue (`SimulatedVenue`) on a spot-margin account, after each borrow, repay and
///   interest charge.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
pub struct MarginDebtUpdate<AssetKey, InstrumentKey> {
    /// The asset owed.
    pub asset: AssetKey,
    /// The isolated margin pair the debt belongs to, or `None` for a debt of the account as a whole,
    /// as on a cross margin account.
    ///
    /// The engine applies only an update with no instrument to its asset balances: an isolated
    /// pair's balances are per pair, and the engine stores none of them (see
    /// [`InstrumentBalanceUpdate`](crate::InstrumentBalanceUpdate)).
    pub instrument: Option<InstrumentKey>,
    /// The principal and interest owed in [`asset`](Self::asset) after the change, both totals.
    pub debt: MarginDetails,
    /// What changed the debt, independent of venue.
    pub kind: MarginDebtKind,
    /// The venue's own name for the change, verbatim, such as Binance's `BORROW`.
    pub venue_kind: SmolStr,
    /// When the venue made the change.
    pub time_exchange: DateTime<Utc>,
}

/// What changed a [`MarginDebtUpdate`]'s debt, independent of venue.
///
/// Each producer documents how its venue's names map onto these. A name that maps to none is sent
/// as [`Other`](Self::Other), with the venue's name in [`MarginDebtUpdate::venue_kind`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum MarginDebtKind {
    /// A loan was taken, or added to.
    Borrow,
    /// Principal or interest was repaid.
    Repay,
    /// Interest was charged on the principal owed.
    Interest,
    /// A change no other variant names. [`MarginDebtUpdate::venue_kind`] carries the venue's name
    /// for it.
    Other,
}

impl Display for MarginDebtKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Borrow => "borrow",
            Self::Repay => "repay",
            Self::Interest => "interest",
            Self::Other => "other",
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn an_update_round_trips_through_serde() {
        let update = MarginDebtUpdate::new(
            SmolStr::new("BTC"),
            Some(SmolStr::new("BTCUSDT")),
            MarginDetails {
                borrowed: dec!(0.00006),
                interest: dec!(0.00000002),
            },
            MarginDebtKind::Interest,
            SmolStr::new("INTEREST"),
            Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        );
        let json = serde_json::to_string(&update).unwrap();
        assert_eq!(
            serde_json::from_str::<MarginDebtUpdate<SmolStr, SmolStr>>(&json).unwrap(),
            update
        );
    }

    #[test]
    fn a_kind_displays_as_its_lowercase_name() {
        let names = [
            MarginDebtKind::Borrow,
            MarginDebtKind::Repay,
            MarginDebtKind::Interest,
            MarginDebtKind::Other,
        ]
        .map(|kind| kind.to_string());
        assert_eq!(names, ["borrow", "repay", "interest", "other"]);
    }
}
