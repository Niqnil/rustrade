//! How an option position settles at expiry, what a simulated venue sells before it, and when it
//! stops taking orders that open a position in it.

use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::calendar::{CalendarError, SessionCalendar};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How a long option position settles at expiry, whoever settles it: an engine that settles
/// expiry itself, or the [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue). Short options
/// and futures are not affected.
#[non_exhaustive]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum LongOptionExpiry {
    /// An option in the money is exercised, against the underlying's price as whoever settles it
    /// takes it (see their documentation). One out of the money settles at zero. The default, matching an exchange that exercises in-the-money
    /// options automatically. What exercise does to the account is up to whoever settles it: an
    /// engine settles in cash at the intrinsic value, and the simulated venue settles as the
    /// contract's [`OptionSettlement`](rustrade_instrument::instrument::kind::option::OptionSettlement)
    /// says.
    #[default]
    Exercise,
    /// Every long option lapses: the position settles at zero, in the money or not, and no
    /// underlying price is needed for it. This matches a holder who instructs the broker not to
    /// exercise any option still held at expiry.
    Lapse,
}

/// When a [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) sells a long option the
/// account cannot afford to exercise, before it expires, as a broker does for an account short of
/// the cash or shares an exercise needs.
///
/// Set on [`MockExecutionConfig::expiry_sell_out`]. For example, Alpaca exercises a long option in
/// the money at expiry, but sells it in the last hour before the market closes on the expiry day
/// when the account lacks the buying power (a call) or the shares (a put):
///
/// ```
/// use rust_decimal_macros::dec;
/// use rustrade_execution::expiry::{ExpiryAnchor, ExpirySellOut, SellOutAffordability};
/// use std::time::Duration;
///
/// // Checked an hour before the expiry day's close, the whole position sold if it cannot all be
/// // exercised, at 2% under the price a market sell would fill at.
/// let sell_out = ExpirySellOut::new(ExpiryAnchor::BeforeClose(Duration::from_secs(60 * 60)))
///     .with_affordability(SellOutAffordability::PerPosition)
///     .with_haircut(dec!(0.02))
///     .unwrap();
/// ```
///
/// # What the venue does
/// At the instant the [`anchor`](Self::anchor) places for each physically settled option, the
/// venue looks at every long it holds in that option:
/// - **In the money?** Against the underlying's price as expiry settlement takes it, the mid of
///   its spot market, else its last price. Only an option in the money, by any amount, is sold.
///   Without a price for the underlying the venue cannot tell, warns, and sells nothing.
/// - **Affordable?** Exercising a call pays `strike × quantity × contract_size` of quote: the
///   account's free quote, plus on a venue with spot margin what it would lend for a buy (its
///   leverage limit and the asset's borrow cap; nothing while a liquidation is under way).
///   Exercising a put delivers `quantity × contract_size` of the underlying, which the account
///   must hold free: shares securing a short call are not free, and the venue never lends shares
///   to exercise a put. Cash and shares reserved for orders still working, the option's own
///   included, are not free. Several longs checked at one instant share what the account has, by
///   instrument name: each spends on what goes on to expiry, the contracts kept and any its sale
///   left unsold, and what one sale brings in does not count towards another's exercise.
/// - **What is sold:** under [`SellOutAffordability::PerPosition`], the whole position, unless
///   all of it is affordable; under [`SellOutAffordability::PerContract`], the contracts beyond
///   what is affordable.
/// - **How:** the orders still working on the option are cancelled first, then the venue sells as
///   a market sell would fill: priced by its fill model, moved by its slippage, up to the depth
///   on offer, paying a taker's sell-side fee. The price is then cut by the
///   [`haircut`](Self::haircut). The trade is reported as a
///   [`TradeOrigin::Liquidation`](crate::trade::TradeOrigin::Liquidation), with no client order
///   id.
///
/// Whatever is not sold, because it was affordable or out of the money, or because the option or
/// its underlying had no price, the depth ran out or the account could not pay the fee, goes on to
/// expiry and settles under [`MockExecutionConfig::long_option_expiry`]. A cash-settled option is
/// never sold: exercising it needs no money.
///
/// # Known limitations
/// - **One anchor for every option.** A venue whose classes of option are sold out at different
///   offsets before their closes cannot be modelled; each option's own close is still used.
/// - **One check.** A broker sells at some point in a window before expiry. The venue checks once,
///   at the instant this sets, against the market it holds then, however old. A price that
///   arrives later in the window does not sell what the check could not.
/// - **No order cut-off of its own.** Orders that open or add to an expiring position are still
///   taken between the check and expiry, unless an [`ExpiryDayCutoff`] refuses them.
/// - **Only the account's own money.** What exercising one option receives (a put's cash) does
///   not fund another (a call), and an option a little out of the money, which a broker may also
///   sell, is left to expire.
/// - **The haircut is fixed.** Where a broker's forced sale fills is not documented; the haircut
///   is a modelling choice, and zero sells at the price a market sell would get.
///
/// [`MockExecutionConfig::expiry_sell_out`]:
///     crate::client::mock::MockExecutionConfig::expiry_sell_out
/// [`MockExecutionConfig::long_option_expiry`]:
///     crate::client::mock::MockExecutionConfig::long_option_expiry
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "ExpirySellOutRepr", into = "ExpirySellOutRepr")]
pub struct ExpirySellOut {
    anchor: ExpiryAnchor,
    affordability: SellOutAffordability,
    haircut: Decimal,
}

/// How much of a long option in the money the venue sells when the account cannot afford to
/// exercise all of it. See [`ExpirySellOut`].
#[non_exhaustive]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum SellOutAffordability {
    /// The whole position is sold unless the account can exercise all of it, as Alpaca documents
    /// its sell-out. The default.
    #[default]
    PerPosition,
    /// As many whole contracts as the account can exercise are kept for expiry, and the rest are
    /// sold, so a position can be partly exercised and partly sold.
    PerContract,
}

/// The serialised form of an [`ExpirySellOut`], before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct ExpirySellOutRepr {
    anchor: ExpiryAnchor,
    #[serde(default)]
    affordability: SellOutAffordability,
    #[serde(default, with = "rust_decimal::serde::str")]
    haircut: Decimal,
}

/// Why an [`ExpirySellOut`] was refused.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpirySellOutError {
    /// The haircut is below zero, or not below one: a sale at or below nothing.
    #[error("expiry sell-out haircut {haircut} is outside [0, 1)")]
    Haircut {
        /// The haircut refused.
        haircut: Decimal,
    },
}

impl ExpirySellOut {
    /// Sells at the instant `anchor` places for each option, the whole position unless all of it
    /// is affordable ([`SellOutAffordability::PerPosition`]), at the price a market sell would get
    /// (no haircut).
    pub const fn new(anchor: ExpiryAnchor) -> Self {
        Self {
            anchor,
            affordability: SellOutAffordability::PerPosition,
            haircut: Decimal::ZERO,
        }
    }

    /// With `affordability` deciding how much of a position is sold.
    #[must_use]
    pub const fn with_affordability(mut self, affordability: SellOutAffordability) -> Self {
        self.affordability = affordability;
        self
    }

    /// With the sale's price cut by `haircut`, a fraction of it: `0.02` sells at 98% of the price
    /// a market sell would get.
    ///
    /// # Errors
    /// [`ExpirySellOutError::Haircut`] unless `0 <= haircut < 1`.
    pub fn with_haircut(mut self, haircut: Decimal) -> Result<Self, ExpirySellOutError> {
        if !(Decimal::ZERO..Decimal::ONE).contains(&haircut) {
            return Err(ExpirySellOutError::Haircut { haircut });
        }
        self.haircut = haircut;
        Ok(self)
    }

    /// When the venue checks each option.
    pub const fn anchor(&self) -> ExpiryAnchor {
        self.anchor
    }

    /// How much of a position the venue sells.
    pub const fn affordability(&self) -> SellOutAffordability {
        self.affordability
    }

    /// The fraction the sale's price is cut by.
    pub const fn haircut(&self) -> Decimal {
        self.haircut
    }
}

impl TryFrom<ExpirySellOutRepr> for ExpirySellOut {
    type Error = ExpirySellOutError;

    fn try_from(repr: ExpirySellOutRepr) -> Result<Self, Self::Error> {
        Self::new(repr.anchor)
            .with_affordability(repr.affordability)
            .with_haircut(repr.haircut)
    }
}

impl From<ExpirySellOut> for ExpirySellOutRepr {
    fn from(sell_out: ExpirySellOut) -> Self {
        Self {
            anchor: sell_out.anchor,
            affordability: sell_out.affordability,
            haircut: sell_out.haircut,
        }
    }
}

/// When a [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) stops taking orders that open
/// or increase a position in an option about to expire, as a broker does on an option's expiry
/// day, so that no new exposure is taken on that it could not settle or sell out in time.
///
/// Set on [`MockExecutionConfig::expiry_day_cutoff`]. For example, a broker that refuses such
/// orders in the last 45 minutes before the expiry day's close:
///
/// ```
/// use rustrade_execution::expiry::{ExpiryAnchor, ExpiryDayCutoff};
/// use std::time::Duration;
///
/// let cutoff = ExpiryDayCutoff::new(ExpiryAnchor::BeforeClose(Duration::from_secs(45 * 60)));
/// ```
///
/// # What the venue does
/// From the instant the [`anchor`](Self::anchor) places for an option until its expiry, the venue
/// refuses an order on it of which any part would open or increase a position, with
/// [`ApiError::OpenRefused`] carrying [`OpenRefusal::ExpiryDayCutoff`]. Every option is covered,
/// cash-settled or physically settled.
///
/// An order is measured against the position as it would stand once every order already working
/// on the same side of the option filled, in the order the venue accepted them: an order closes
/// only what the position has left after those orders close their part. A sell of 3 against a
/// long of 5 with a sell of 4 already working closes 1 and opens 2, so it is refused. An order
/// that would close the position and open one on the other side, such as a sell of 7 against a
/// long of 5, is refused whole. An order that only closes is taken as before the cutoff.
///
/// The venue amends no option order, so a working order cannot be amended past the cutoff
/// either.
///
/// # Known limitations
/// - **Working orders are left working.** An order that opens a position, accepted before the
///   cutoff, keeps working after it and may fill until the option expires, or until an
///   [`ExpirySellOut`] check cancels it. Whether a broker cancels such orders at its cutoff is
///   not documented.
/// - **One anchor for every option.** A venue whose classes of option are cut off at different
///   offsets before their closes, or at one fixed time of day whatever each class's close, cannot
///   be modelled; each option's own close is still used.
/// - **Only the simulated venue refuses this way.** A live client reports its venue's refusal of
///   such an order as [`ApiError::OrderRejected`] until that venue's wording is confirmed.
///
/// [`MockExecutionConfig::expiry_day_cutoff`]:
///     crate::client::mock::MockExecutionConfig::expiry_day_cutoff
/// [`ApiError::OpenRefused`]: crate::error::ApiError::OpenRefused
/// [`ApiError::OrderRejected`]: crate::error::ApiError::OrderRejected
/// [`OpenRefusal::ExpiryDayCutoff`]: crate::error::OpenRefusal::ExpiryDayCutoff
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct ExpiryDayCutoff {
    anchor: ExpiryAnchor,
}

impl ExpiryDayCutoff {
    /// Refuses opening orders from the instant `anchor` places for each option.
    pub const fn new(anchor: ExpiryAnchor) -> Self {
        Self { anchor }
    }

    /// When the venue starts refusing opening orders on each option.
    pub const fn anchor(&self) -> ExpiryAnchor {
        self.anchor
    }
}

/// When an expiry-day rule of a [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) applies
/// to an option, as an offset before its expiry day's session close or before its expiry itself.
///
/// Serialised externally tagged in snake case, the offset as a [`Duration`]:
/// `{"before_close": {"secs": 3600, "nanos": 0}}`.
///
/// # The expiry instant
/// An option's [`expiry`](rustrade_instrument::instrument::kind::option::OptionContract::expiry)
/// is the instant the venue settles it. [`BeforeClose`](Self::BeforeClose) reads the expiry day
/// as the date that instant falls on in the session calendar's own time zone, so stamp the expiry
/// at or after that day's close. A contract stamped at midnight UTC of its expiration date, as
/// the market data connectors stamp theirs to carry the date, falls on the previous date in a
/// time zone west of UTC, such as New York's: the venue settles it that evening, and this anchors
/// to that previous day's close, consistently with the settlement but a day early against the
/// market.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryAnchor {
    /// This long before the close of the session on the expiry's date, from the option's session
    /// calendar, as brokers set their expiry-day deadlines. Needs a calendar: a venue with none
    /// for the option panics when its clock first moves (see
    /// [`SimulatedVenue::next_deadline`](crate::exchange::mock::SimulatedVenue::next_deadline)).
    BeforeClose(Duration),
    /// This long before the expiry instant itself, for a venue that trades around the clock and
    /// has no session close.
    BeforeExpiry(Duration),
}

/// Why an [`ExpiryAnchor`] could not place its instant for an option.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpiryAnchorError {
    /// The anchor is [`ExpiryAnchor::BeforeClose`], and the option has no session calendar.
    #[error("the anchor is before the session close, but the option has no session calendar")]
    NoCalendar,
    /// The expiry's date has no session (a weekend or a holiday) in the option's calendar.
    #[error("the expiry {expiry} falls on a date with no session")]
    NoSession {
        /// The option's expiry.
        expiry: DateTime<Utc>,
    },
    /// The session close on the expiry's date is after the expiry, so a rule anchored before the
    /// close could apply after the option has settled. The expiry is likely stamped before the
    /// close of its class, such as 16:00 for options that close at 16:15.
    #[error("the session close {close} on the expiry's date is after the expiry {expiry}")]
    CloseAfterExpiry {
        /// The close of the session on the expiry's date.
        close: DateTime<Utc>,
        /// The option's expiry.
        expiry: DateTime<Utc>,
    },
    /// The option's session calendar could not answer.
    #[error(transparent)]
    Calendar(#[from] CalendarError),
}

impl ExpiryAnchor {
    /// The offset before the anchor's reference instant. Private: a later variant set at a clock
    /// time would have none.
    const fn offset(&self) -> Duration {
        match *self {
            Self::BeforeClose(offset) | Self::BeforeExpiry(offset) => offset,
        }
    }

    /// The instant this anchor places for an option expiring at `expiry`, whose session calendar,
    /// if it has one, is `calendar`. An instant earlier than a [`DateTime`] holds is clamped to
    /// the earliest it does.
    ///
    /// # Errors
    /// For [`BeforeClose`](Self::BeforeClose) only:
    /// - [`ExpiryAnchorError::NoCalendar`] if `calendar` is `None`.
    /// - [`ExpiryAnchorError::NoSession`] if the expiry's date has no session.
    /// - [`ExpiryAnchorError::CloseAfterExpiry`] if that date's close is after `expiry`. A close
    ///   equal to it is accepted: a rule at the settlement instant applies before settlement.
    /// - [`ExpiryAnchorError::Calendar`] if `calendar` cannot answer.
    pub fn instant(
        &self,
        expiry: DateTime<Utc>,
        calendar: Option<&dyn SessionCalendar>,
    ) -> Result<DateTime<Utc>, ExpiryAnchorError> {
        let reference = match self {
            Self::BeforeExpiry(_) => expiry,
            Self::BeforeClose(_) => {
                let calendar = calendar.ok_or(ExpiryAnchorError::NoCalendar)?;
                let close = calendar
                    .session_close_on(expiry)?
                    .ok_or(ExpiryAnchorError::NoSession { expiry })?;
                if close > expiry {
                    return Err(ExpiryAnchorError::CloseAfterExpiry { close, expiry });
                }
                close
            }
        };
        let offset = TimeDelta::from_std(self.offset()).unwrap_or(TimeDelta::MAX);
        Ok(reference
            .checked_sub_signed(offset)
            .unwrap_or(DateTime::<Utc>::MIN_UTC))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: a failed unwrap is a failed test.
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, TimeZone};
    use rustrade_instrument::calendar::{DailyClose, SessionDates};

    const HOUR: Duration = Duration::from_secs(60 * 60);

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap()
    }

    /// 16:00 New York every weekday, with Thanksgiving 2026 a holiday.
    fn new_york() -> DailyClose {
        let thanksgiving = NaiveDate::from_ymd_opt(2026, 11, 26).unwrap();
        DailyClose::new(
            NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            chrono_tz::America::New_York,
        )
        .with_dates(SessionDates::new([thanksgiving], []).unwrap(), None)
        .unwrap()
    }

    fn before_close(expiry: DateTime<Utc>) -> Result<DateTime<Utc>, ExpiryAnchorError> {
        ExpiryAnchor::BeforeClose(HOUR).instant(expiry, Some(&new_york()))
    }

    #[test]
    fn before_expiry_is_the_offset_before_the_expiry_and_needs_no_calendar() {
        let expiry = utc(2026, 11, 20, 21, 0, 0);
        assert_eq!(
            ExpiryAnchor::BeforeExpiry(HOUR).instant(expiry, None),
            Ok(utc(2026, 11, 20, 20, 0, 0))
        );
        assert_eq!(ExpiryAnchor::BeforeExpiry(HOUR).offset(), HOUR);
        assert_eq!(ExpiryAnchor::BeforeClose(HOUR).offset(), HOUR);
    }

    /// An expiry stamped at the close itself is accepted, as is one stamped after it on the same
    /// New York date, in winter (EST) and in summer (EDT).
    #[test]
    fn before_close_is_the_offset_before_the_close_of_the_expiry_date() {
        // Friday 2026-11-20, 16:00 EST is 21:00Z.
        for expiry in [utc(2026, 11, 20, 21, 0, 0), utc(2026, 11, 20, 23, 30, 0)] {
            assert_eq!(before_close(expiry), Ok(utc(2026, 11, 20, 20, 0, 0)));
        }
        // Friday 2026-07-17, 16:00 EDT is 20:00Z.
        assert_eq!(
            before_close(utc(2026, 7, 17, 20, 0, 0)),
            Ok(utc(2026, 7, 17, 19, 0, 0))
        );
    }

    #[test]
    fn a_close_after_the_expiry_is_refused() {
        let expiry = utc(2026, 11, 20, 20, 59, 59);
        assert_eq!(
            before_close(expiry),
            Err(ExpiryAnchorError::CloseAfterExpiry {
                close: utc(2026, 11, 20, 21, 0, 0),
                expiry,
            })
        );
    }

    #[test]
    fn an_expiry_on_a_date_with_no_session_is_refused() {
        // Thanksgiving, then a Saturday.
        for expiry in [utc(2026, 11, 26, 21, 0, 0), utc(2026, 11, 21, 21, 0, 0)] {
            assert_eq!(
                before_close(expiry),
                Err(ExpiryAnchorError::NoSession { expiry })
            );
        }
    }

    #[test]
    fn before_close_without_a_calendar_is_refused() {
        assert_eq!(
            ExpiryAnchor::BeforeClose(HOUR).instant(utc(2026, 11, 20, 21, 0, 0), None),
            Err(ExpiryAnchorError::NoCalendar)
        );
    }

    #[derive(Debug)]
    struct Unavailable;

    impl SessionCalendar for Unavailable {
        fn next_close_after(&self, _: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError> {
            Err(CalendarError::unavailable("down"))
        }

        fn session_close_on(
            &self,
            _: DateTime<Utc>,
        ) -> Result<Option<DateTime<Utc>>, CalendarError> {
            Err(CalendarError::unavailable("down"))
        }
    }

    #[test]
    fn a_calendar_that_cannot_answer_is_reported() {
        assert_eq!(
            ExpiryAnchor::BeforeClose(HOUR)
                .instant(utc(2026, 11, 20, 21, 0, 0), Some(&Unavailable)),
            Err(ExpiryAnchorError::Calendar(CalendarError::unavailable(
                "down"
            )))
        );
    }

    /// Midnight UTC of 2026-11-20 is the evening of 2026-11-19 in New York, so the anchor is that
    /// day's close: consistent with a venue settling at that instant, a day early against the
    /// market.
    #[test]
    fn an_expiry_stamped_at_midnight_utc_anchors_to_the_previous_new_york_close() {
        assert_eq!(
            before_close(utc(2026, 11, 20, 0, 0, 0)),
            Ok(utc(2026, 11, 19, 20, 0, 0))
        );
    }

    #[test]
    fn an_offset_before_the_earliest_instant_is_clamped_to_it() {
        let expiry = utc(2026, 11, 20, 21, 0, 0);
        for offset in [Duration::MAX, Duration::from_secs(i64::MAX as u64 / 1_000)] {
            assert_eq!(
                ExpiryAnchor::BeforeExpiry(offset).instant(expiry, None),
                Ok(DateTime::<Utc>::MIN_UTC)
            );
        }
    }
}
