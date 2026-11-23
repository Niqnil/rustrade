//! Completed bars from IB's keepUpToDate bar stream, which sends a history batch and then the
//! forming bar again and again, with no mark that a bar is final.
//!
//! [`BarAssembler`] does no I/O and reads no clock: the caller passes IB's time in, asks when the
//! next timer is due ([`BarAssembler::next_deadline`]), and calls [`BarAssembler::on_tick`] then.
//! The rules are in [`candles`](crate::exchange::ibkr::candles) § "When a bar is complete".

use chrono::{DateTime, TimeDelta, Utc};

/// Which bars a subscription emits: those after the last one it delivered, or from its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResumePoint {
    /// Bars opening at or after this time: the subscription's start, before any bar is delivered.
    From(DateTime<Utc>),
    /// Bars opening after this time: the open of the last bar delivered.
    After(DateTime<Utc>),
}

impl ResumePoint {
    /// Whether the bar opening at `open` is still to be emitted.
    fn admits(self, open: DateTime<Utc>) -> bool {
        match self {
            Self::From(start) => open >= start,
            Self::After(last) => open > last,
        }
    }

    /// The open of the first bar a request must reach back to, for bars `step` long.
    pub(super) fn first_open(self, step: TimeDelta) -> DateTime<Utc> {
        match self {
            Self::From(start) => start,
            Self::After(last) => add_or_max(last, step),
        }
    }
}

/// What the assembler hands back.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Output<B> {
    /// A completed bar, to emit.
    Final { open: DateTime<Utc>, bar: B },
    /// An update for a bar already emitted, or for one older than the bar forming, which is
    /// dropped: the bar went out without what it adds.
    Late { open: DateTime<Utc> },
}

/// Turns one keepUpToDate subscription's history batch and updates into completed bars, each
/// emitted once.
///
/// Invariants:
/// - Emitted opens strictly increase, and each is admitted by the [`ResumePoint`] it started with.
/// - An emitted bar is the last version of it the assembler was given.
/// - After [`on_tick`](Self::on_tick) at or past [`next_deadline`](Self::next_deadline), the
///   deadline is gone: a caller waiting until it never spins.
#[derive(Debug)]
pub(super) struct BarAssembler<B> {
    /// The length of a bar.
    step: TimeDelta,
    /// How long after a bar's end the timer emits it.
    settle: TimeDelta,
    resume: ResumePoint,
    /// The bar forming: its open and its latest version.
    pending: Option<(DateTime<Utc>, B)>,
}

impl<B> BarAssembler<B> {
    /// An assembler for bars `step` long, emitting those `resume` admits, with the timer `settle`
    /// after a bar's end.
    pub(super) fn new(step: TimeDelta, settle: TimeDelta, resume: ResumePoint) -> Self {
        Self {
            step,
            settle,
            resume,
            pending: None,
        }
    }

    /// Take the history batch IB sends first, in ascending order of open, at IB time `now`.
    ///
    /// Every bar but the last is complete. The last is kept as the bar forming, and emitted at
    /// once if its timer is already past (a bar from a session that has closed). Bars the
    /// [`ResumePoint`] does not admit are skipped without a word: the request reaches back further
    /// than needed on purpose.
    pub(super) fn on_history(
        &mut self,
        bars: impl IntoIterator<Item = (DateTime<Utc>, B)>,
        now: DateTime<Utc>,
        out: &mut Vec<Output<B>>,
    ) {
        for (open, bar) in bars {
            if self.resume.admits(open) {
                self.take(open, bar, out);
            }
        }
        self.on_tick(now, out);
    }

    /// Take an update of the bar opening at `open`, arriving at IB time `now`.
    pub(super) fn on_update(
        &mut self,
        open: DateTime<Utc>,
        bar: B,
        now: DateTime<Utc>,
        out: &mut Vec<Output<B>>,
    ) {
        if self.resume.admits(open) {
            self.take(open, bar, out);
            // An update arriving after its bar's end is IB's closing update for it.
            if self
                .pending
                .as_ref()
                .is_some_and(|(open, _)| now >= add_or_max(*open, self.step))
            {
                self.emit_pending(out);
            }
        } else if matches!(self.resume, ResumePoint::After(_)) {
            out.push(Output::Late { open });
        }
        // Otherwise an update for a bar before the start, such as the closing update of the bar
        // before the one forming when the stream connected, was never wanted.
        self.on_tick(now, out);
    }

    /// Emit the bar forming if its timer is due at IB time `now`.
    pub(super) fn on_tick(&mut self, now: DateTime<Utc>, out: &mut Vec<Output<B>>) {
        if self.next_deadline().is_some_and(|deadline| now >= deadline) {
            self.emit_pending(out);
        }
    }

    /// When the timer emits the bar forming, in IB time, if a bar is forming.
    pub(super) fn next_deadline(&self) -> Option<DateTime<Utc>> {
        self.pending
            .as_ref()
            .map(|(open, _)| add_or_max(add_or_max(*open, self.step), self.settle))
    }

    /// The version of the bar opening at `open` the resume point admits, against the bar forming:
    /// a later bar completes it, the same bar replaces it, an earlier one is late.
    fn take(&mut self, open: DateTime<Utc>, bar: B, out: &mut Vec<Output<B>>) {
        match self.pending.as_ref().map(|(pending, _)| *pending) {
            Some(pending) if open < pending => out.push(Output::Late { open }),
            Some(pending) if open > pending => {
                self.emit_pending(out);
                self.pending = Some((open, bar));
            }
            _ => self.pending = Some((open, bar)),
        }
    }

    fn emit_pending(&mut self, out: &mut Vec<Output<B>>) {
        if let Some((open, bar)) = self.pending.take() {
            self.resume = ResumePoint::After(open);
            out.push(Output::Final { open, bar });
        }
    }
}

/// `time + delta`, or the latest representable time past it.
fn add_or_max(time: DateTime<Utc>, delta: TimeDelta) -> DateTime<Utc> {
    time.checked_add_signed(delta)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
// Test code: a panic is the failure report.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const STEP: TimeDelta = TimeDelta::seconds(60);
    const SETTLE: TimeDelta = TimeDelta::seconds(2);

    /// 14:30 UTC plus `seconds`.
    fn t(seconds: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-23T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + TimeDelta::seconds(seconds)
    }

    /// 14:30 UTC plus `millis` milliseconds.
    fn t_ms(millis: i64) -> DateTime<Utc> {
        t(0) + TimeDelta::milliseconds(millis)
    }

    fn assembler(resume: ResumePoint) -> BarAssembler<&'static str> {
        BarAssembler::new(STEP, SETTLE, resume)
    }

    fn finals(out: &[Output<&'static str>]) -> Vec<(DateTime<Utc>, &'static str)> {
        out.iter()
            .filter_map(|o| match o {
                Output::Final { open, bar } => Some((*open, *bar)),
                Output::Late { .. } => None,
            })
            .collect()
    }

    #[test]
    fn history_emits_all_but_the_last_bar_and_keeps_that_one_forming() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_history([(t(0), "a"), (t(60), "b"), (t(120), "c")], t(130), &mut out);
        assert_eq!(finals(&out), [(t(0), "a"), (t(60), "b")]);
        assert_eq!(a.next_deadline(), Some(t(182)));
    }

    #[test]
    fn history_skips_bars_before_the_start_and_already_delivered() {
        let mut a = assembler(ResumePoint::From(t(60)));
        let mut out = Vec::new();
        a.on_history([(t(0), "a"), (t(60), "b"), (t(120), "c")], t(130), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(60),
                bar: "b"
            }]
        );

        let mut a = assembler(ResumePoint::After(t(60)));
        let mut out = Vec::new();
        a.on_history([(t(0), "a"), (t(60), "b"), (t(120), "c")], t(130), &mut out);
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(a.next_deadline(), Some(t(182)));
    }

    #[test]
    fn historys_last_bar_is_emitted_at_once_when_its_timer_has_passed() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_history([(t(0), "a"), (t(60), "b")], t(122), &mut out);
        assert_eq!(finals(&out), [(t(0), "a"), (t(60), "b")]);
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn historys_last_bar_waits_until_its_timer() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_history([(t(60), "b")], t_ms(121_999), &mut out);
        assert!(out.is_empty(), "{out:?}");
        a.on_tick(t_ms(121_999), &mut out);
        assert!(out.is_empty(), "{out:?}");
        a.on_tick(t(122), &mut out);
        assert_eq!(finals(&out), [(t(60), "b")]);
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn an_empty_history_leaves_nothing_forming() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_history([], t(10), &mut out);
        assert!(out.is_empty());
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn updates_replace_the_forming_bar_until_the_next_bar_completes_it() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a1", t(5), &mut out);
        a.on_update(t(0), "a2", t(10), &mut out);
        a.on_update(t(0), "a3", t(55), &mut out);
        assert!(out.is_empty(), "{out:?}");
        a.on_update(t(60), "b1", t(60), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(0),
                bar: "a3"
            }]
        );
        assert_eq!(a.next_deadline(), Some(t(122)));
    }

    #[test]
    fn an_update_after_its_bars_end_is_the_closing_update() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a1", t(55), &mut out);
        a.on_update(t(0), "a2", t_ms(59_999), &mut out);
        assert!(out.is_empty(), "{out:?}");
        a.on_update(t(0), "a3", t(60), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(0),
                bar: "a3"
            }]
        );
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn the_first_update_of_a_bar_after_its_end_completes_it_at_once() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a", t_ms(60_500), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(0),
                bar: "a"
            }]
        );
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn the_timer_emits_a_bar_with_no_closing_update() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a", t(50), &mut out);
        assert_eq!(a.next_deadline(), Some(t(62)));
        a.on_tick(t_ms(61_999), &mut out);
        assert!(out.is_empty(), "{out:?}");
        a.on_tick(t(62), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(0),
                bar: "a"
            }]
        );
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn an_update_for_an_emitted_bar_is_late_and_changes_nothing() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a", t(50), &mut out);
        a.on_tick(t(62), &mut out);
        out.clear();
        a.on_update(t(0), "a-late", t(63), &mut out);
        assert_eq!(out, [Output::Late { open: t(0) }]);
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn an_update_older_than_the_forming_bar_is_late() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(60), "b", t(61), &mut out);
        a.on_update(t(0), "a", t(62), &mut out);
        assert_eq!(out, [Output::Late { open: t(0) }]);
        // The forming bar is untouched.
        a.on_tick(t(122), &mut out);
        assert_eq!(finals(&out), [(t(60), "b")]);
    }

    #[test]
    fn an_update_before_the_start_is_dropped_without_a_word() {
        let mut a = assembler(ResumePoint::From(t(60)));
        let mut out = Vec::new();
        a.on_update(t(0), "a-closing", t_ms(60_400), &mut out);
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(a.next_deadline(), None);
    }

    #[test]
    fn an_unwanted_update_still_lets_a_due_timer_fire() {
        let mut a = assembler(ResumePoint::From(t(60)));
        let mut out = Vec::new();
        a.on_update(t(60), "b", t(70), &mut out);
        a.on_update(t(0), "a", t(122), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(60),
                bar: "b"
            }]
        );

        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a", t(50), &mut out);
        a.on_tick(t(62), &mut out);
        a.on_update(t(60), "b", t(70), &mut out);
        out.clear();
        a.on_update(t(0), "a-late", t(122), &mut out);
        assert_eq!(
            out,
            [
                Output::Late { open: t(0) },
                Output::Final {
                    open: t(60),
                    bar: "b"
                }
            ]
        );
    }

    #[test]
    fn a_resumed_assembler_reports_an_update_before_its_resume_point_as_late() {
        let mut a = assembler(ResumePoint::After(t(0)));
        let mut out = Vec::new();
        a.on_update(t(0), "a", t(61), &mut out);
        assert_eq!(out, [Output::Late { open: t(0) }]);
    }

    #[test]
    fn history_then_updates_continue_from_the_batchs_forming_bar() {
        let mut a = assembler(ResumePoint::From(t(0)));
        let mut out = Vec::new();
        a.on_history([(t(0), "a"), (t(60), "b-hist")], t(70), &mut out);
        assert_eq!(finals(&out), [(t(0), "a")]);
        out.clear();
        a.on_update(t(60), "b1", t(75), &mut out);
        a.on_update(t(120), "c1", t(120), &mut out);
        assert_eq!(
            out,
            [Output::Final {
                open: t(60),
                bar: "b1"
            }]
        );
    }

    #[test]
    fn first_open_is_the_start_or_the_bar_after_the_last_delivered() {
        assert_eq!(ResumePoint::From(t(60)).first_open(STEP), t(60));
        assert_eq!(ResumePoint::After(t(60)).first_open(STEP), t(120));
    }

    #[test]
    fn a_deadline_past_the_last_time_saturates() {
        let mut a = assembler(ResumePoint::From(DateTime::<Utc>::MIN_UTC));
        let mut out = Vec::new();
        a.on_update(DateTime::<Utc>::MAX_UTC, "z", t(0), &mut out);
        assert_eq!(a.next_deadline(), Some(DateTime::<Utc>::MAX_UTC));
        assert!(out.is_empty(), "{out:?}");
    }

    /// A small deterministic generator (64-bit LCG), so the randomized run below needs no
    /// dependency and replays the same sequences every time.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: u64) -> i64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((self.0 >> 33) % n) as i64
        }
    }

    /// Whatever IB sends in whatever order: opens strictly increase, every emitted bar was
    /// admitted, each is the last version given before it went out, and the deadline is always
    /// in the future after a call.
    #[test]
    fn emitted_bars_are_ordered_unique_and_the_latest_version() {
        for seed in 0..3_000u64 {
            let mut rng = Lcg(seed);
            let start = rng.below(3);
            let mut a: BarAssembler<(i64, i64)> =
                BarAssembler::new(STEP, SETTLE, ResumePoint::From(t(start * 60)));
            let mut out = Vec::new();
            let mut history: Vec<i64> = (0..rng.below(4)).map(|_| rng.below(4)).collect();
            history.sort_unstable();
            history.dedup();
            let mut now = t(0);
            // The last version of each bar given to the assembler and accepted.
            let mut latest = std::collections::HashMap::new();
            for &b in &history {
                if b >= start {
                    latest.insert(b, 0);
                }
            }
            a.on_history(history.iter().map(|&b| (t(b * 60), (b, 0))), now, &mut out);
            let mut checked = 0;
            let mut last_open: Option<DateTime<Utc>> = None;
            for step in 0..rng.below(40) {
                if rng.below(2) == 0 {
                    let bar = rng.below(6);
                    now += TimeDelta::seconds(rng.below(40));
                    if a.resume.admits(t(bar * 60))
                        && a.pending.as_ref().is_none_or(|(p, _)| t(bar * 60) >= *p)
                    {
                        latest.insert(bar, step + 1);
                    }
                    a.on_update(t(bar * 60), (bar, step + 1), now, &mut out);
                } else {
                    now += TimeDelta::seconds(rng.below(90));
                    a.on_tick(now, &mut out);
                }
                for o in &out[checked..] {
                    if let Output::Final {
                        open,
                        bar: (b, version),
                    } = o
                    {
                        assert!(*open >= t(start * 60), "seed {seed}");
                        assert!(last_open.is_none_or(|last| *open > last), "seed {seed}");
                        assert_eq!(*open, t(b * 60), "seed {seed}");
                        assert_eq!(latest.get(b), Some(version), "seed {seed}");
                        last_open = Some(*open);
                    }
                }
                checked = out.len();
                if let Some(deadline) = a.next_deadline() {
                    assert!(deadline > now, "seed {seed}");
                }
            }
        }
    }
}
