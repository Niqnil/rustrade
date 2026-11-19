//! A corporate action applied to the venue. See [`SimulatedVenue::apply_corporate_action`].

use super::{SimulatedVenue, VenueOutcome};
use chrono::{DateTime, Utc};
use rustrade_instrument::{
    corporate_action::CorporateActionKind, instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
use tracing::warn;

/// Response type of [`SimulatedVenue::apply_corporate_action`].
pub type CorporateActionOutcome = VenueOutcome<CorporateActionResponse>;

/// What [`SimulatedVenue::apply_corporate_action`] did with an action.
///
/// `#[non_exhaustive]`, so an outcome can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorporateActionResponse {
    /// The venue applied the action and recorded its id.
    Applied,
    /// The venue had already applied an action under this id, so it changed nothing.
    AlreadyApplied,
    /// The venue did not apply the action. It changed nothing and did not record the id, so the
    /// same action can be applied once the reason is fixed.
    Refused(CorporateActionRefusal),
}

/// Why [`SimulatedVenue::apply_corporate_action`] refused an action.
///
/// `#[non_exhaustive]`, so a reason can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorporateActionRefusal {
    /// The venue does not trade the instrument.
    UnknownInstrument,
    /// The instrument is not the deliverable equity: see
    /// [`InstrumentKind::is_split_eligible`](rustrade_instrument::instrument::kind::InstrumentKind::is_split_eligible).
    InstrumentKindNotSupported,
    /// The venue does not model this kind of action.
    KindNotSupported,
}

impl SimulatedVenue {
    /// Applies the corporate action `id` of `kind` to `instrument`, effective at `time_exchange`,
    /// as the venue that lists it would.
    ///
    /// # What a stock split does
    /// - **Orders:** the venue cancels every order on `instrument`, resting or held, releasing what
    ///   each held. It cancels rather than adjusts, as many venues do. Each is reported as for a
    ///   cancel: `[balance, cash flow.., order]`, the order an ended
    ///   [`Cancelled`](crate::order::state::Cancelled) snapshot stamped at the split, or `[order]`
    ///   alone for one that held nothing.
    /// - **Market:** the venue keeps the market it holds for `instrument`, which still values what
    ///   the account holds of it, but trades nothing against it until the next
    ///   [`apply_market`](Self::apply_market) on `instrument`: see
    ///   [`VenueInstrumentMarket::split_at`](super::VenueInstrumentMarket::split_at). Until then a
    ///   market order on `instrument` is refused as unpriced and a limit order rests uncrossed, as
    ///   on an instrument the venue was never fed.
    ///
    /// # What it does not do yet
    /// - **Holdings stay pre-split.** The balance of the instrument's base asset, a loan of it, and
    ///   the price the venue values them at keep their pre-split units. An engine that rescales its
    ///   positions on the split therefore holds more or fewer shares than the venue's balance
    ///   until the venue reports one it has rescaled.
    /// - **Options on `instrument` are untouched:** their strikes, positions and orders keep their
    ///   pre-split terms.
    /// - **No configurable rule:** the venue always cancels.
    ///
    /// # Idempotent by `id`
    /// The venue records the id of each action it applies, and changes nothing for an id it has
    /// already applied, answering [`AlreadyApplied`](CorporateActionResponse::AlreadyApplied). The
    /// record is part of the venue's [`snapshot`](Self::snapshot). Give each action, including a
    /// correction of an earlier one, an id of its own.
    ///
    /// # Refused
    /// An instrument this venue does not trade, an instrument that is not the deliverable equity,
    /// and a kind of action it does not model are refused with a warning. Nothing changes and the
    /// id is not recorded: see [`CorporateActionRefusal`].
    ///
    /// # Returns what the action caused, which a driver must deliver
    /// The clock is first moved to `time_exchange`, as by [`advance_time`](Self::advance_time),
    /// whatever the answer, and the events that move causes come first. An order whose deadline
    /// falls at the split therefore expires rather than being cancelled by it. The cancels follow.
    /// `time_exchange` is clamped as `advance_time` clamps it, and the cancels are stamped at the
    /// clamped instant.
    ///
    /// # Panics
    /// As [`advance_time`](Self::advance_time).
    #[must_use]
    pub fn apply_corporate_action(
        &mut self,
        id: &str,
        instrument: &InstrumentNameExchange,
        kind: &CorporateActionKind,
        time_exchange: DateTime<Utc>,
    ) -> CorporateActionOutcome {
        let mut events = self.advance_time(time_exchange);
        let time_exchange = self.time_exchange();

        let response = match self.corporate_action_refusal(id, instrument, kind) {
            Some(response) => response,
            None => {
                let mut cancelled = Vec::new();
                self.cancel_orders(
                    |working| working == instrument,
                    time_exchange,
                    &mut cancelled,
                );
                // Releasing a hold can give back a loan the order took when it was placed.
                self.refresh_next_interest_if_loans_changed();
                self.report_debt_changes(&mut cancelled);
                events.append(&mut cancelled);

                if let Some(market) = self.market.get_mut(instrument) {
                    market.split_at = Some(time_exchange);
                }
                self.corporate_actions_applied.insert(SmolStr::new(id));
                CorporateActionResponse::Applied
            }
        };

        VenueOutcome { events, response }
    }

    /// The answer to an action this venue does not apply, with the warning it gives, or `None` for
    /// one it applies. Checked in the engine's order: the instrument, then the id, then the
    /// instrument's kind, then the action's.
    fn corporate_action_refusal(
        &self,
        id: &str,
        instrument: &InstrumentNameExchange,
        kind: &CorporateActionKind,
    ) -> Option<CorporateActionResponse> {
        let refuse = |reason: CorporateActionRefusal, why: &str| {
            warn!(
                exchange = %self.exchange,
                %id,
                %instrument,
                ?kind,
                "SimulatedVenue refused a corporate action: {why}. Nothing changed and the id is \
                 not recorded"
            );
            Some(CorporateActionResponse::Refused(reason))
        };

        let Some(traded) = self.instruments.get(instrument) else {
            return refuse(
                CorporateActionRefusal::UnknownInstrument,
                "the venue does not trade the instrument",
            );
        };
        if self.corporate_actions_applied.contains(id) {
            warn!(
                exchange = %self.exchange,
                %id,
                %instrument,
                "SimulatedVenue already applied a corporate action under this id, so it changed \
                 nothing. Give each action, including a correction, an id of its own"
            );
            return Some(CorporateActionResponse::AlreadyApplied);
        }
        if !traded.kind.is_split_eligible() {
            return refuse(
                CorporateActionRefusal::InstrumentKindNotSupported,
                "the instrument is not the deliverable equity",
            );
        }
        let CorporateActionKind::StockSplit { .. } = kind else {
            return refuse(
                CorporateActionRefusal::KindNotSupported,
                "the venue does not model this kind of action",
            );
        };
        None
    }
}
