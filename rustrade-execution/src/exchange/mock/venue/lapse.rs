//! Lapse instructions: the holder of a long option telling the venue not to exercise it at expiry.
//! See [`SimulatedVenue::instruct_exercise`].

use super::{ExpiryRule, LongOptionExpiry, SimulatedVenue};
use crate::{
    error::{ApiError, ExerciseRefusal, UnindexedApiError},
    exercise::{ExerciseAck, ExerciseInstruction, ExerciseQuantity},
};
use chrono::{DateTime, NaiveDate, Utc};
use rustrade_instrument::{calendar::Tz, instrument::name::InstrumentNameExchange};

/// Where the venue's [`LapseDeadline`](crate::expiry::LapseDeadline) places for one option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LapseWindow {
    /// The instant from which the option takes no lapse instruction.
    pub(super) deadline: DateTime<Utc>,
    /// The only day the option takes one on, under
    /// [`expiry_day_only`](crate::expiry::LapseDeadline::expiry_day_only).
    pub(super) expiry_day: Option<ExpiryDay>,
}

/// An option's expiry day: the date its expiry instant falls on in its session calendar's zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExpiryDay {
    date: NaiveDate,
    zone: Tz,
}

impl ExpiryDay {
    /// The day `expiry` falls on in `zone`.
    pub(super) fn of(expiry: DateTime<Utc>, zone: Tz) -> Self {
        Self {
            date: expiry.with_timezone(&zone).date_naive(),
            zone,
        }
    }

    /// Whether `instant` falls on this day, in its zone.
    fn contains(&self, instant: DateTime<Utc>) -> bool {
        instant.with_timezone(&self.zone).date_naive() == self.date
    }
}

impl SimulatedVenue {
    /// Takes `instruction` for the long position held in the option `instrument`: what
    /// [`OptionExerciseClient::instruct_exercise`] asks a simulated venue.
    ///
    /// A [`Lapse`](ExerciseInstruction::Lapse) for the whole position marks it to lapse at expiry,
    /// in the money or not: it settles at zero, as [`LongOptionExpiry::Lapse`] settles a long,
    /// needing no price for the underlying. Unlike that venue-wide policy, which governs only what
    /// reaches expiry, the instruction also spares the long from an [`expiry_sell_out`], since
    /// lapsing it needs no money. Nothing is reported when the instruction is taken; the
    /// position's settlement is, at expiry, as a [`TradeOrigin::Expiry`] trade at zero. Taking the
    /// same instruction again changes nothing and is accepted again.
    ///
    /// # What a later trade does to it
    /// The instruction holds for whatever long the position is at expiry: contracts sold after it
    /// leave the rest instructed, and contracts bought after it are instructed too. A position
    /// that goes flat, or short, loses the instruction, so a long opened later is exercised as
    /// [`long_option_expiry`] says unless instructed again. A split that restates the position
    /// keeps it. Venues do not document what a later trade does to a do-not-exercise instruction,
    /// so this is a modelling choice.
    ///
    /// # Errors
    /// - [`ApiError::InstrumentInvalid`] for an instrument this venue does not list as an option.
    /// - [`ApiError::ExerciseRefused`] with:
    ///   - [`ExerciseRefusal::QuantityUnsupported`] for
    ///     [`ExerciseQuantity::Contracts`]: this venue instructs only whole positions.
    ///   - [`ExerciseRefusal::Expired`] at or after the option's expiry, even while a position
    ///     waits for a price to settle against.
    ///   - [`ExerciseRefusal::NotExpiryDay`] on a date other than the option's expiry day, where
    ///     the [`LapseDeadline`](crate::expiry::LapseDeadline) is
    ///     [`expiry_day_only`](crate::expiry::LapseDeadline::expiry_day_only).
    ///   - [`ExerciseRefusal::PastDeadline`] at or after the option's
    ///     [`LapseDeadline`](crate::expiry::LapseDeadline), where one is configured.
    ///   - [`ExerciseRefusal::NoPosition`] when the account holds none in the option.
    ///   - [`ExerciseRefusal::ShortPosition`] when it is short the option.
    ///
    /// The instant judged is [`time_exchange`](Self::time_exchange).
    ///
    /// # Panics
    /// If the [`LapseDeadline`](crate::expiry::LapseDeadline) needs a session calendar, being
    /// anchored before the session close or
    /// [`expiry_day_only`](crate::expiry::LapseDeadline::expiry_day_only), and the option has none
    /// to place it: see
    /// [`next_deadline`](Self::next_deadline)'s `# Panics`.
    ///
    /// [`OptionExerciseClient::instruct_exercise`]: crate::client::OptionExerciseClient::instruct_exercise
    /// [`TradeOrigin::Expiry`]: crate::trade::TradeOrigin::Expiry
    /// [`expiry_sell_out`]: crate::client::mock::MockExecutionConfig::expiry_sell_out
    /// [`long_option_expiry`]: crate::client::mock::MockExecutionConfig::long_option_expiry
    pub fn instruct_exercise(
        &mut self,
        instrument: &InstrumentNameExchange,
        instruction: ExerciseInstruction,
    ) -> Result<ExerciseAck, UnindexedApiError> {
        let Some((_, contract)) = self.option_terms(instrument) else {
            return Err(ApiError::InstrumentInvalid(
                instrument.clone(),
                format!("SimulatedVenue lists no option {instrument} to instruct"),
            ));
        };
        let refuse = |refusal| Err(ApiError::ExerciseRefused(refusal));
        let ExerciseInstruction::Lapse { contracts } = instruction;
        if let ExerciseQuantity::Contracts(_) = contracts {
            return refuse(ExerciseRefusal::QuantityUnsupported);
        }

        let now = self.time_exchange();
        if now >= contract.expiry {
            return refuse(ExerciseRefusal::Expired {
                expiry: contract.expiry,
            });
        }
        self.assert_rule_placed(ExpiryRule::LapseDeadline, instrument);
        if let Some(&LapseWindow {
            deadline,
            expiry_day,
        }) = self.lapse_deadlines.get(instrument)
        {
            if let Some(day) = expiry_day
                && !day.contains(now)
            {
                return refuse(ExerciseRefusal::NotExpiryDay {
                    expiry_day: day.date,
                });
            }
            if now >= deadline {
                return refuse(ExerciseRefusal::PastDeadline { deadline });
            }
        }

        match self.positions.get(instrument) {
            None => refuse(ExerciseRefusal::NoPosition),
            Some(position) if !position.is_long() => refuse(ExerciseRefusal::ShortPosition),
            Some(_) => {
                self.lapse_instructed.insert(instrument.clone());
                Ok(ExerciseAck::new(instruction))
            }
        }
    }

    /// Whether a long position in the option `instrument` lapses at expiry: under
    /// [`LongOptionExpiry::Lapse`] every long does, and otherwise one the holder instructed to.
    pub(super) fn long_lapses(&self, instrument: &InstrumentNameExchange) -> bool {
        self.long_option_expiry == LongOptionExpiry::Lapse
            || self.lapse_instructed.contains(instrument)
    }

    /// Drops the lapse instruction of the option `instrument` unless the account is still long it:
    /// called after each change to its position.
    pub(super) fn forget_lapse_unless_long(&mut self, instrument: &InstrumentNameExchange) {
        if !self.lapse_instructed.is_empty()
            && !self
                .positions
                .get(instrument)
                .is_some_and(|position| position.is_long())
        {
            self.lapse_instructed.remove(instrument);
        }
    }
}
