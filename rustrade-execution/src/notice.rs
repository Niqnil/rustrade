//! Notices a venue sends about an account's state that are not trades, orders or balances: margin
//! calls, liquidation warnings, a liquidation under way, and the all-clear after them.
//!
//! An account stream reports each one as
//! [`AccountEventKind::Notice`](crate::AccountEventKind::Notice), carrying an [`AccountNotice`].
//! The library delivers the notice; what to do about it (stop opening positions, reduce exposure,
//! alert someone) stays the consumer's.

use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt::{Display, Formatter};

/// A notice from the venue about the account's margin or liquidation state.
///
/// Sent as [`AccountEventKind::Notice`](crate::AccountEventKind::Notice). A notice reports a
/// change of state, not a delta: it changes no balance or position, and the engine only logs it.
///
/// A notice can be missed: one sent while the stream is disconnected is not recovered when it
/// reconnects. A consumer that acts on a [`NoticeKind::MarginCall`] and waits for a
/// [`NoticeKind::MarginRestored`] should re-check the venue's state after a reconnect, rather than
/// wait for a notice that may already have gone by.
///
/// # Producers
///
/// Known producers, as of writing:
/// - Binance margin (`BinanceMargin`): `MARGIN_LEVEL_STATUS_CHANGE`. See the client's rustdoc for
///   which streams it is read from and how a notice sent on more than one is delivered once.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
pub struct AccountNotice<InstrumentKey> {
    /// What the notice says, independent of venue.
    pub kind: NoticeKind,
    /// The instrument the notice is about, when the venue scopes it to one, as it does an isolated
    /// margin pair. `None` for a notice about the account as a whole, and for one the producer
    /// could not attribute; each producer says which it sends.
    pub instrument: Option<InstrumentKey>,
    /// When the venue sent the notice.
    pub time_exchange: DateTime<Utc>,
    /// The venue's margin level when it sent the notice, when it reports one.
    ///
    /// It is the venue's own figure under the venue's own definition, so it is not comparable
    /// across venues. Each producer says what it is. `None` when the venue sent none, or sent one
    /// that could not be read.
    pub margin_level: Option<Decimal>,
    /// The venue's own name for the status, verbatim, such as Binance's `PRE_LIQUIDATION`.
    pub status: SmolStr,
}

/// What an [`AccountNotice`] says, independent of venue.
///
/// Each producer documents how its venue's statuses map onto these. A status that maps to none
/// is sent as [`Other`](Self::Other), with the venue's name for it in
/// [`AccountNotice::status`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum NoticeKind {
    /// The account's margin has fallen to the venue's margin-call level.
    MarginCall,
    /// The account's margin is close to the level at which the venue liquidates.
    LiquidationWarning,
    /// The venue has started liquidating the account's positions.
    Liquidation,
    /// A margin call or liquidation condition the venue reported earlier no longer holds.
    MarginRestored,
    /// A status that no other variant names. [`AccountNotice::status`] carries the venue's name
    /// for it.
    Other,
}

impl NoticeKind {
    /// Whether the notice reports rising risk: a margin call, a liquidation warning or a
    /// liquidation.
    pub fn is_escalation(self) -> bool {
        matches!(
            self,
            Self::MarginCall | Self::LiquidationWarning | Self::Liquidation
        )
    }
}

impl Display for NoticeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MarginCall => "margin call",
            Self::LiquidationWarning => "liquidation warning",
            Self::Liquidation => "liquidation",
            Self::MarginRestored => "margin restored",
            Self::Other => "other",
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn only_rising_risk_is_an_escalation() {
        for (kind, escalation) in [
            (NoticeKind::MarginCall, true),
            (NoticeKind::LiquidationWarning, true),
            (NoticeKind::Liquidation, true),
            (NoticeKind::MarginRestored, false),
            (NoticeKind::Other, false),
        ] {
            assert_eq!(kind.is_escalation(), escalation, "{kind}");
        }
    }

    #[test]
    fn a_notice_round_trips_through_serde() {
        let notice = AccountNotice::new(
            NoticeKind::LiquidationWarning,
            Some(SmolStr::new("BTCUSDT")),
            DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            Some(Decimal::new(115, 2)),
            SmolStr::new("PRE_LIQUIDATION"),
        );
        let json = serde_json::to_string(&notice).unwrap();
        assert_eq!(
            serde_json::from_str::<AccountNotice<SmolStr>>(&json).unwrap(),
            notice
        );
    }
}
