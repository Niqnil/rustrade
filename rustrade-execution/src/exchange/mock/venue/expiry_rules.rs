//! The expiry-day rules of a [`SimulatedVenue`]: where each rule's anchor places it for each
//! option, the [`ExpiryDayCutoff`] that refuses opening orders from there, and the
//! [`LapseDeadline`] that refuses lapse instructions. The
//! [`ExpirySellOut`] check itself runs in `sell_out`.
//!
//! [`ExpiryDayCutoff`]: crate::expiry::ExpiryDayCutoff
//! [`LapseDeadline`]: crate::expiry::LapseDeadline
//! [`ExpirySellOut`]: crate::expiry::ExpirySellOut

use super::{
    ExpiryRule, ExpiryRuleUnplaceable, SimulatedVenue,
    lapse::{ExpiryDay, LapseWindow},
};
use crate::{
    error::{ApiError, OpenRefusal, UnindexedApiError},
    expiry::{ExpiryAnchor, ExpiryAnchorError},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use itertools::Itertools;
use rustrade_instrument::instrument::{
    kind::{InstrumentKind, option::OptionSettlement},
    name::InstrumentNameExchange,
};

/// Where one rule's anchor places it: the instant for each option it could place, and the options
/// it could not for want of a session calendar, each by name.
#[derive(Default)]
struct Placement {
    placed: Vec<(DateTime<Utc>, InstrumentNameExchange)>,
    unplaced: Vec<InstrumentNameExchange>,
}

impl SimulatedVenue {
    /// Places each configured expiry-day rule for the options it covers, under each option's
    /// session calendar: the [`ExpirySellOut`](crate::expiry::ExpirySellOut) checks for every
    /// physically settled option (a cash-settled one is never sold out, since exercising it needs
    /// no money), and the [`ExpiryDayCutoff`](crate::expiry::ExpiryDayCutoff) and
    /// [`LapseDeadline`](crate::expiry::LapseDeadline) for every option.
    /// Then finds the next sell-out after the instant the clock has reached.
    ///
    /// An option with no calendar to place a rule is left in
    /// [`expiry_rules_unplaced`](Self::expiry_rules_unplaced), for a calendar given later.
    ///
    /// # Errors
    /// [`ExpiryRuleUnplaceable`] for the first option, by rule then name, an anchor cannot place
    /// for any other reason. Every rule is then left as it was.
    pub(super) fn resolve_expiry_rules(&mut self) -> Result<(), ExpiryRuleUnplaceable> {
        let sell_outs = self.place(
            ExpiryRule::SellOut,
            self.expiry_sell_out.map(|sell_out| sell_out.anchor()),
            |settlement| settlement == OptionSettlement::Physical,
        )?;
        let cutoffs = self.place(
            ExpiryRule::DayCutoff,
            self.expiry_day_cutoff.map(|cutoff| cutoff.anchor()),
            |_| true,
        )?;
        let lapse_deadlines = self.place(
            ExpiryRule::LapseDeadline,
            self.lapse_deadline.map(|deadline| deadline.anchor()),
            |_| true,
        )?;
        let (lapse_windows, lapse_unplaced) = self.lapse_windows(lapse_deadlines);
        self.expiry_rules_unplaced = sell_outs
            .unplaced
            .into_iter()
            .map(|name| (ExpiryRule::SellOut, name))
            .chain(
                cutoffs
                    .unplaced
                    .into_iter()
                    .map(|name| (ExpiryRule::DayCutoff, name)),
            )
            .chain(
                lapse_unplaced
                    .into_iter()
                    .map(|name| (ExpiryRule::LapseDeadline, name)),
            )
            .collect();
        self.sell_outs = sell_outs.placed;
        self.sell_outs.sort_unstable();
        self.cutoffs = cutoffs
            .placed
            .into_iter()
            .map(|(cutoff, name)| (name, cutoff))
            .collect();
        self.lapse_deadlines = lapse_windows;
        self.refresh_next_sell_out();
        Ok(())
    }

    /// Each option's [`LapseWindow`] from where its deadline was placed, with its expiry day if
    /// the [`LapseDeadline`](crate::expiry::LapseDeadline) is
    /// [`expiry_day_only`](crate::expiry::LapseDeadline::expiry_day_only); and the options it
    /// cannot place, by name: those the anchor could not, and under `expiry_day_only` those with
    /// no session calendar to date the expiry day in.
    fn lapse_windows(
        &self,
        deadlines: Placement,
    ) -> (
        FnvHashMap<InstrumentNameExchange, LapseWindow>,
        Vec<InstrumentNameExchange>,
    ) {
        let expiry_day_only = self
            .lapse_deadline
            .is_some_and(|deadline| deadline.is_expiry_day_only());
        let mut unplaced = deadlines.unplaced;
        let mut windows = FnvHashMap::default();
        for (deadline, name) in deadlines.placed {
            let expiry_day = if expiry_day_only {
                let expiry =
                    self.instruments
                        .get(&name)
                        .and_then(|instrument| match &instrument.kind {
                            InstrumentKind::Option(contract) => Some(contract.expiry),
                            _ => None,
                        });
                match (expiry, self.session_calendar_of(&name)) {
                    (Some(expiry), Some(calendar)) => Some(ExpiryDay::of(expiry, calendar.zone())),
                    _ => {
                        unplaced.push(name);
                        continue;
                    }
                }
            } else {
                None
            };
            windows.insert(
                name,
                LapseWindow {
                    deadline,
                    expiry_day,
                },
            );
        }
        unplaced.sort_unstable();
        (windows, unplaced)
    }

    /// Where `anchor`, `rule`'s if it is configured, places the rule for each option whose
    /// settlement `covers`. Nothing without one.
    fn place(
        &self,
        rule: ExpiryRule,
        anchor: Option<ExpiryAnchor>,
        covers: impl Fn(OptionSettlement) -> bool,
    ) -> Result<Placement, ExpiryRuleUnplaceable> {
        let Some(anchor) = anchor else {
            return Ok(Placement::default());
        };
        let mut placement = Placement::default();
        let options = self
            .instruments
            .iter()
            .filter_map(|(name, instrument)| match &instrument.kind {
                InstrumentKind::Option(contract) if covers(contract.settlement) => {
                    Some((name, contract.expiry))
                }
                _ => None,
            })
            .sorted_unstable_by(|(left, _), (right, _)| left.cmp(right));
        for (name, expiry) in options {
            let calendar = self.session_calendar_of(name).map(AsRef::as_ref);
            match anchor.instant(expiry, calendar) {
                Ok(instant) => placement.placed.push((instant, name.clone())),
                Err(ExpiryAnchorError::NoCalendar) => placement.unplaced.push(name.clone()),
                Err(source) => {
                    return Err(ExpiryRuleUnplaceable {
                        rule,
                        instrument: name.clone(),
                        source,
                    });
                }
            }
        }
        Ok(placement)
    }

    /// Panics if [`expiry_rules_unplaced`](Self::expiry_rules_unplaced) holds an option: see
    /// [`next_deadline`](Self::next_deadline)'s `# Panics`.
    pub(super) fn assert_expiry_rules_placed(&self) {
        assert!(
            self.expiry_rules_unplaced.is_empty(),
            "SimulatedVenue has expiry-day rules anchored to the session calendar or held to the \
             expiry day, but these options have no session calendar to place them, by rule: {}. Configure \
             MockExecutionConfig::session_close, a session_close_overrides entry, or \
             SimulatedVenue::with_session_calendar",
            self.expiry_rules_unplaced
                .iter()
                .map(|(rule, name)| format!("{rule} {name}"))
                .join(", ")
        );
    }

    /// Panics if the [`ExpiryDayCutoff`](crate::expiry::ExpiryDayCutoff) is anchored to the
    /// session calendar and the option `instrument` has no session calendar to place it: see
    /// [`next_deadline`](Self::next_deadline)'s `# Panics`. Checked on every order on the option
    /// before its expiry, since a venue restored from a snapshot has its clock moved already, so
    /// an order it took before a calendar was given could pass a cutoff not yet placed.
    pub(super) fn assert_cutoff_placed(&self, instrument: &InstrumentNameExchange) {
        self.assert_rule_placed(ExpiryRule::DayCutoff, instrument);
    }

    /// Panics if `rule` needs a session calendar, being anchored to it (before the close or at a
    /// local time) or a lapse deadline held to the expiry day, and the option `instrument` has
    /// none to place it.
    pub(super) fn assert_rule_placed(&self, rule: ExpiryRule, instrument: &InstrumentNameExchange) {
        let why = match rule {
            ExpiryRule::LapseDeadline => {
                "anchored to the session calendar or held to the expiry day"
            }
            ExpiryRule::SellOut | ExpiryRule::DayCutoff => "anchored to the session calendar",
        };
        assert!(
            !self
                .expiry_rules_unplaced
                .iter()
                .any(|(unplaced, name)| *unplaced == rule && name == instrument),
            "SimulatedVenue's {rule} is {why}, but the option {instrument} has no session \
             calendar to place it. Configure \
             MockExecutionConfig::session_close, a session_close_overrides entry, or \
             SimulatedVenue::with_session_calendar"
        );
    }

    /// Refuses an order on the option `instrument` that would open or increase a position, if the
    /// venue's clock, at `now`, has reached the option's
    /// [`ExpiryDayCutoff`](crate::expiry::ExpiryDayCutoff). An option with no cutoff, or one not
    /// yet placed, is not refused: [`assert_cutoff_placed`](Self::assert_cutoff_placed) runs
    /// first.
    pub(super) fn check_expiry_day_cutoff(
        &self,
        instrument: &InstrumentNameExchange,
        now: DateTime<Utc>,
    ) -> Result<(), UnindexedApiError> {
        match self.cutoffs.get(instrument) {
            Some(&cutoff) if now >= cutoff => {
                Err(ApiError::OpenRefused(OpenRefusal::ExpiryDayCutoff {
                    cutoff,
                }))
            }
            _ => Ok(()),
        }
    }
}
