//! Trading status data model: whether an instrument can trade at its venue.
//!
//! A venue halts or pauses an instrument for regulatory reasons, pending news, an order imbalance
//! or extreme volatility. While it does, orders sent to it rest unfilled or are refused, and its
//! quotes stop being a price anyone can trade at. A [`TradingStatus`] reports each change the
//! venue publishes.
//!
//! [`TradingStatus`]: crate::subscription::trading_status::TradingStatus

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// A change in whether an instrument can trade at its venue, as the venue reported it.
///
/// The latest status received is the instrument's state until the next one. Silence is not
/// [`TradingState::Trading`]: a venue may report only changes, and may send nothing at all for an
/// instrument that trades normally, so a consumer that has received no status knows nothing.
/// After a stream reconnects, the state from before is stale until the venue reports again.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingStatus {
    /// What the instrument may do now.
    pub state: TradingState,
    /// Why, when the venue says.
    pub reason: Option<HaltReason>,
    /// The venue's own code for the state or reason, as sent, when it has one. Its meaning is the
    /// venue's: see the venue integration's documentation.
    pub venue_code: Option<SmolStr>,
}

impl TradingStatus {
    /// A status with no reason and no venue code.
    pub fn new(state: TradingState) -> Self {
        Self {
            state,
            reason: None,
            venue_code: None,
        }
    }

    /// With the reason the venue gave.
    #[must_use]
    pub fn with_reason(mut self, reason: HaltReason) -> Self {
        self.reason = Some(reason);
        self
    }

    /// With the venue's own code for the state or reason.
    #[must_use]
    pub fn with_venue_code(mut self, code: impl Into<SmolStr>) -> Self {
        self.venue_code = Some(code.into());
        self
    }
}

/// What an instrument may do at its venue.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingState {
    /// Trading normally.
    Trading,
    /// Halted: no trading and no quoting, until the venue resumes it. A halt has no set length.
    Halted,
    /// Paused for a set time, such as a Limit Up-Limit Down trading pause or another pause to
    /// guard against extreme volatility. Trading resumes on its own, usually through an auction.
    Paused,
    /// Quoting has resumed but trading has not yet: orders may be entered, nothing executes.
    QuoteOnly,
    /// The venue says it cannot report the state, for example with frozen data.
    Unknown,
}

/// Why a venue halted or paused an instrument.
///
/// Venues use many more codes than these; one that maps to none of them is [`Self::Other`], with
/// the venue's code in [`TradingStatus::venue_code`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HaltReason {
    /// A regulator's halt or suspension, such as a regulatory concern or a trading suspension.
    Regulatory,
    /// News is pending, being disseminated, or awaited.
    News,
    /// An order imbalance.
    OrderImbalance,
    /// Extreme volatility, such as a Limit Up-Limit Down trading pause.
    Volatility,
    /// A market-wide circuit breaker.
    MarketWideCircuitBreaker,
    /// A corporate action.
    CorporateAction,
    /// An operational problem at the venue.
    Operational,
    /// A reason none of the above covers.
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_a_status_with_its_reason_and_venue_code() {
        let status = TradingStatus::new(TradingState::Paused)
            .with_reason(HaltReason::Volatility)
            .with_venue_code("2");

        assert_eq!(status.state, TradingState::Paused);
        assert_eq!(status.reason, Some(HaltReason::Volatility));
        assert_eq!(status.venue_code.as_deref(), Some("2"));
    }

    #[test]
    fn a_new_status_has_no_reason_or_venue_code() {
        let status = TradingStatus::new(TradingState::Trading);

        assert_eq!(status.reason, None);
        assert_eq!(status.venue_code, None);
    }
}
