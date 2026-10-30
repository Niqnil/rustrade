//! An estimate of a venue's clock, read from the times the venue stamps on its events.
//!
//! A venue's timestamps and this host's wall clock differ by an unknown amount that changes: the
//! host clock can be skewed, drift, or step, as on an NTP step or a manual change. A span read
//! from the venue by time, such as the fills missed while a stream was down, is a span on the
//! venue's clock, so a bound taken from the host's wall clock can miss part of it.
//!
//! [`VenueClock`] does not read the host's wall clock. It keeps one time the venue stamped,
//! paired with the moment the event arrived on tokio's monotonic clock, and counts on from there
//! on that clock, which a step of the wall clock does not move.
//!
//! A read of a span the venue stamped, such as the fills missed while a stream was down, is
//! placed with [`VenueClock::span_start`] and [`VenueClock::span_end`], from the [`HostMoment`]s
//! that bound it on this host.

use chrono::{DateTime, TimeDelta, Utc};
use std::time::Duration;
use tokio::time::Instant;

/// The most the monotonic clock is assumed to run fast or slow against the venue's: 1 part in
/// this many, 100 ppm. An undisciplined quartz oscillator stays within about 50 ppm, and a clock
/// NTP disciplines much closer, so this is a wide margin. It costs a read that starts 0.36 s
/// earlier for each hour since the sample.
const DRIFT_DIVISOR: u32 = 10_000;

/// The earliest a venue's clock can read at a given moment, from the times it stamped on events
/// this host received.
///
/// Each [`observe`](Self::observe) gives a stamp and the [`Instant`] its event arrived. The event
/// was stamped before it arrived, so at that instant the venue's clock read the stamp or later:
/// the delay in between only makes the stamp earlier. The clock keeps the one sample that bounds
/// the venue's time the tightest, allowing for drift since it was taken, and counts on from it on
/// tokio's clock. An old event delivered late, such as a fill a venue replays when a stream
/// resubscribes, gives a loose bound and so never displaces a better one.
///
/// # Caller obligations
///
/// - Observe only times the venue itself stamped. A time filled in from this host's clock, for
///   an event that arrived without one, is not the venue's: if this host's clock is ahead, it
///   reads as a venue clock that is ahead too, and every bound taken from it is late. A
///   [`Trade`](crate::trade::Trade) says which it is in its
///   [`time_source`](crate::trade::Trade::time_source).
/// - Pass the instant the event arrived, read from [`Instant::now`]. That is tokio's clock, so a
///   test runtime with a paused clock controls it.
///
/// # Known limitations
///
/// - A stamp the venue sends from ahead of its own clock, such as one read in the wrong time
///   zone, is kept: it bounds the venue's time later than it is until a later sample bounds it
///   later still. Compare the result with this host's clock (see
///   [`host_lead`](Self::host_lead)) to see such a stamp.
/// - A venue clock that steps back is followed only as fast as the drift allowance lets the old
///   sample go, about 0.36 s an hour.
/// - Before the first sample there is no bound: [`earliest_at`](Self::earliest_at) is `None`.
/// - The monotonic clock is not free of the host's time keeping. On some platforms, such as
///   Linux, it does not advance while the host is suspended, which makes the bound earlier than
///   it could be: safe for a read's start, but no help for its end. NTP can also slew it, up to
///   500 ppm or more while it corrects an offset, beyond the drift allowance. A slew that speeds it up corrects a wall clock that is
///   behind, so a caller that also bounds a read's start by the wall clock, taking the earlier of
///   the two, stays covered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VenueClock {
    sample: Option<Sample>,
}

/// A moment on this host: the time its wall clock read then, and the same moment on tokio's
/// monotonic clock.
///
/// The instant lets a span bounded by the moment be counted on from it on the monotonic clock, so
/// a step of the wall clock after it does not move the span (see [`VenueClock::span_end`]).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostMoment {
    /// This host's wall-clock time at the moment.
    pub time: DateTime<Utc>,
    /// The same moment on tokio's clock.
    pub instant: Instant,
}

impl HostMoment {
    /// The moment this host's wall clock read `time` and tokio's clock read `instant`.
    pub const fn new(time: DateTime<Utc>, instant: Instant) -> Self {
        Self { time, instant }
    }

    /// Now, by [`Utc::now`] and [`Instant::now`].
    pub fn now() -> Self {
        Self::new(Utc::now(), Instant::now())
    }
}

/// A time the venue stamped and the moment its event arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sample {
    stamp: DateTime<Utc>,
    received: Instant,
}

impl VenueClock {
    /// A clock with no sample yet.
    pub const fn new() -> Self {
        Self { sample: None }
    }

    /// Records `stamp`, a time the venue stamped on an event that arrived at `received`.
    ///
    /// Kept when it bounds the venue's time at `received` later than the sample held, and
    /// otherwise dropped.
    pub fn observe(&mut self, stamp: DateTime<Utc>, received: Instant) {
        if self
            .earliest_at(received)
            .is_none_or(|earliest| stamp > earliest)
        {
            self.sample = Some(Sample { stamp, received });
        }
    }

    /// Records the sample `other` holds, as [`observe`](Self::observe) would, so that this clock
    /// learns from another source of the same venue's times, such as a read of its server time.
    pub(crate) fn merge(&mut self, other: &VenueClock) {
        if let Some(Sample { stamp, received }) = other.sample {
            self.observe(stamp, received);
        }
    }

    /// The earliest the venue's clock can read at `at`, or `None` before any sample.
    ///
    /// Counts from the sample on tokio's clock, less the drift allowance, so the venue's clock
    /// reads this or later at `at`. `at` may be before the sample.
    pub fn earliest_at(&self, at: Instant) -> Option<DateTime<Utc>> {
        let Sample { stamp, received } = self.sample?;
        Some(if at >= received {
            let elapsed = at - received;
            to_delta(elapsed - elapsed / DRIFT_DIVISOR)
                .and_then(|delta| stamp.checked_add_signed(delta))
                // Beyond chrono's range: the sample's own stamp is still a bound.
                .unwrap_or(stamp)
        } else {
            let before = received - at;
            to_delta(before.saturating_add(before / DRIFT_DIVISOR))
                .and_then(|delta| stamp.checked_sub_signed(delta))
                .unwrap_or(DateTime::<Utc>::MIN_UTC)
        })
    }

    /// The most this host's clock can lead the venue's, given it read `host_time` at `at`, or
    /// `None` before any sample. Negative when this host's clock is behind the venue's.
    ///
    /// An upper bound: it is the true lead plus the delay of the sample held and the drift
    /// allowed for since. A large negative value says the venue's clock is ahead, or that a stamp
    /// was (see the type's known limitations).
    pub fn host_lead(&self, host_time: DateTime<Utc>, at: Instant) -> Option<TimeDelta> {
        self.earliest_at(at)
            .map(|earliest| host_time.signed_duration_since(earliest))
    }

    /// Where a read of what the venue stamped from `moment` on starts: the earliest the venue's
    /// clock can read at `moment`, or this host's time then if that is earlier, as on a host clock
    /// behind the venue's. `None` before any sample, when the caller has to allow for the
    /// difference between the clocks itself.
    ///
    /// A host clock ahead of the venue's does not make the start late, and one behind it costs a
    /// read that starts that much earlier. Less a caller's own margin, for the delay between an
    /// event and its arrival, this is where a read of what was missed after `moment` begins.
    pub fn span_start(&self, moment: HostMoment) -> Option<DateTime<Utc>> {
        self.earliest_at(moment.instant)
            .map(|venue| venue.min(moment.time))
    }

    /// Where a read that has to reach `now` ends, for a span that began at `since`: the latest of
    /// this host's time at `now`, the earliest the venue's clock can read then, and `since`'s host
    /// time counted on to `now` on tokio's clock. Before any sample the venue's time is left out.
    ///
    /// So neither a host clock behind the venue's nor one stepped back since `since` ends the read
    /// early. The venue's clock can still read later than this, as an estimate is a lower bound:
    /// add a margin for that.
    pub fn span_end(&self, since: HostMoment, now: HostMoment) -> DateTime<Utc> {
        let counted_on = to_delta(now.instant.saturating_duration_since(since.instant))
            .and_then(|delta| since.time.checked_add_signed(delta))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let venue = self.earliest_at(now.instant).unwrap_or(now.time);
        venue.max(now.time).max(counted_on)
    }
}

/// `duration` as a [`TimeDelta`], or `None` beyond its range.
fn to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn a_clock_with_no_sample_gives_no_bound() {
        let clock = VenueClock::new();

        assert_eq!(clock.earliest_at(Instant::now()), None);
        assert_eq!(clock.host_lead(at(0), Instant::now()), None);
    }

    #[test]
    fn the_bound_counts_on_from_the_sample_less_the_drift_allowance() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);

        assert_eq!(clock.earliest_at(base), Some(at(100)));
        // 10 000 s on, less 1 s of drift at 100 ppm.
        assert_eq!(clock.earliest_at(base + secs(10_000)), Some(at(10_099)));
    }

    #[test]
    fn the_bound_before_the_sample_counts_back_with_the_drift_allowance_added() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(20_000), base + secs(10_000));

        // 10 000 s back, plus 1 s of drift at 100 ppm.
        assert_eq!(clock.earliest_at(base), Some(at(9_999)));
    }

    #[test]
    fn a_sample_that_bounds_the_venue_later_replaces_the_one_held() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);
        // Stamped 10 s later and arrived 1 ms sooner: later than the held sample allows for.
        let received = base + secs(10) - Duration::from_millis(1);
        clock.observe(at(110), received);

        assert_eq!(clock.earliest_at(received), Some(at(110)));
    }

    #[test]
    fn a_late_old_event_does_not_displace_a_tighter_sample() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);
        // A replayed fill from an hour ago, arriving now.
        clock.observe(at(100 - 3_600), base + secs(1));

        assert_eq!(clock.earliest_at(base), Some(at(100)));
    }

    #[test]
    fn a_sample_equal_to_the_bound_held_keeps_the_older_one() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);
        // Exactly the held sample's bound 10 000 s on: 10 000 s less 1 s of drift.
        clock.observe(at(10_099), base + secs(10_000));

        // The held sample still bounds the time it arrived at; the new one would bound it 2 s
        // earlier, counting back with the drift allowance added.
        assert_eq!(clock.earliest_at(base), Some(at(100)));
    }

    #[test]
    fn a_merged_clock_keeps_whichever_sample_bounds_the_venue_later() {
        let base = Instant::now();
        let mut tighter = VenueClock::new();
        tighter.observe(at(110), base);
        let mut looser = VenueClock::new();
        looser.observe(at(100), base);

        let mut clock = looser;
        clock.merge(&tighter);
        assert_eq!(clock.earliest_at(base), Some(at(110)));

        let mut clock = tighter;
        clock.merge(&looser);
        clock.merge(&VenueClock::new());
        assert_eq!(clock.earliest_at(base), Some(at(110)));
    }

    #[test]
    fn a_span_with_no_sample_has_no_start_and_ends_at_the_later_host_time() {
        let base = Instant::now();
        let clock = VenueClock::new();

        assert_eq!(clock.span_start(HostMoment::new(at(100), base)), None);
        // 10 s on by tokio's clock, but the wall clock stepped back an hour.
        let since = HostMoment::new(at(100), base);
        let now = HostMoment::new(at(110 - 3_600), base + secs(10));
        assert_eq!(clock.span_end(since, now), at(110));
    }

    #[test]
    fn a_span_from_a_host_clock_ahead_of_the_venue_starts_at_the_venues_time() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);

        // The host reads 60 s ahead of the venue.
        assert_eq!(
            clock.span_start(HostMoment::new(at(160), base)),
            Some(at(100))
        );
    }

    #[test]
    fn a_span_from_a_host_clock_behind_the_venue_starts_at_the_hosts_time() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);

        assert_eq!(
            clock.span_start(HostMoment::new(at(40), base)),
            Some(at(40))
        );
    }

    #[test]
    fn a_span_ends_at_the_latest_of_the_host_the_venue_and_the_start_counted_on() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);
        let since = HostMoment::new(at(100), base);

        // A host clock behind the venue's: the venue's time, 10 s on.
        let behind = HostMoment::new(at(50), base + secs(10));
        assert_eq!(clock.span_end(since, behind), at(110));
        // A host clock ahead of both: its own time.
        let ahead = HostMoment::new(at(200), base + secs(10));
        assert_eq!(clock.span_end(since, ahead), at(200));
    }

    #[test]
    fn a_span_end_counts_on_from_its_start_when_the_venue_estimate_is_earlier() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        // A venue 50 s behind the host's start, and a host clock stepped back since.
        clock.observe(at(50), base);
        let since = HostMoment::new(at(100), base);
        let now = HostMoment::new(at(0), base + secs(10));

        assert_eq!(clock.span_end(since, now), at(110));
    }

    #[test]
    fn the_host_lead_is_the_host_time_less_the_bound() {
        let base = Instant::now();
        let mut clock = VenueClock::new();
        clock.observe(at(100), base);

        assert_eq!(clock.host_lead(at(160), base), Some(TimeDelta::seconds(60)));
        assert_eq!(clock.host_lead(at(40), base), Some(TimeDelta::seconds(-60)));
    }
}
