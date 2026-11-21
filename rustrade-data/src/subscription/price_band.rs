//! Price band data model: the prices an instrument may trade between at its venue.
//!
//! US equity venues bound each stock's price with a band around a reference price (Limit Up-Limit
//! Down). A trade outside it is refused; a quote resting at a limit for too long triggers a trading
//! pause. A [`PriceBand`] reports each band the venue publishes.

use super::SubscriptionKind;
use rust_decimal::Decimal;
use rustrade_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`PriceBand`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct PriceBands;

impl SubscriptionKind for PriceBands {
    type Event = PriceBand;

    fn as_str(&self) -> &'static str {
        "price_bands"
    }
}

impl std::fmt::Display for PriceBands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The prices an instrument may trade between at its venue, as the venue published them.
///
/// The latest band received is in effect until the next one. A side with no price has no limit
/// from this band; when both are `None` the venue has published no band in effect. After a stream
/// reconnects, the band from before is stale until the venue publishes again.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceBand {
    /// The highest price the instrument may trade at, if the band sets one.
    pub upper: Option<Decimal>,
    /// The lowest price the instrument may trade at, if the band sets one.
    pub lower: Option<Decimal>,
    /// Why the venue published this band.
    pub update: PriceBandUpdate,
    /// The venue's own code for the band, as sent, when it has one. Its meaning is the venue's:
    /// see the venue integration's documentation.
    pub venue_code: Option<SmolStr>,
}

impl PriceBand {
    /// A band with these limits, no venue code, and [`PriceBandUpdate::Other`].
    pub fn new(upper: Option<Decimal>, lower: Option<Decimal>) -> Self {
        Self {
            upper,
            lower,
            update: PriceBandUpdate::Other,
            venue_code: None,
        }
    }

    /// With the reason the venue published it.
    #[must_use]
    pub fn with_update(mut self, update: PriceBandUpdate) -> Self {
        self.update = update;
        self
    }

    /// With the venue's own code for the band.
    #[must_use]
    pub fn with_venue_code(mut self, code: impl Into<SmolStr>) -> Self {
        self.venue_code = Some(code.into());
        self
    }
}

/// Why a venue published a [`PriceBand`].
///
/// Venues may say more than these; a reason that maps to none of them, or one the venue does not
/// state, is [`Self::Other`], with the venue's code in [`PriceBand::venue_code`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PriceBandUpdate {
    /// The first band of the trading day, set at the open.
    Opening,
    /// A band moved during the day as the reference price moved.
    Intraday,
    /// A band published again unchanged, such as after a feed recovery.
    Restated,
    /// Bands are suspended because the instrument is halted or paused.
    SuspendedDuringHalt,
    /// The band the instrument reopens with after a halt or pause.
    Reopening,
    /// Outside the hours bands apply in.
    OutsideRegularHours,
    /// A reason none of the above covers, or none was given.
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn builds_a_band_with_its_update_and_venue_code() {
        let band = PriceBand::new(Some(dec!(3.24)), Some(dec!(2.65)))
            .with_update(PriceBandUpdate::Intraday)
            .with_venue_code("C:B");

        assert_eq!(band.upper, Some(dec!(3.24)));
        assert_eq!(band.lower, Some(dec!(2.65)));
        assert_eq!(band.update, PriceBandUpdate::Intraday);
        assert_eq!(band.venue_code.as_deref(), Some("C:B"));
    }

    #[test]
    fn a_new_band_has_no_reason_or_venue_code() {
        let band = PriceBand::new(None, None);

        assert_eq!(band.update, PriceBandUpdate::Other);
        assert_eq!(band.venue_code, None);
    }

    #[test]
    fn the_kind_is_named_price_bands() {
        assert_eq!(PriceBands.as_str(), "price_bands");
        assert_eq!(PriceBands.to_string(), "price_bands");
    }
}
