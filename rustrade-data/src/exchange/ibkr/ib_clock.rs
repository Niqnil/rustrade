//! IB's time, as one connection estimates it: the host's monotonic clock plus IB's server time,
//! sampled at connect and again every [`RESYNC`].
//!
//! Candle subscriptions time a bar's end against it, so that a host clock running ahead of IB's
//! does not end bars early, and a step of the host's wall clock (NTP, a VM resume) moves no
//! deadline: between samples, time advances on [`Instant`].

use chrono::{DateTime, TimeDelta, Utc};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// How often IB's server time is sampled again. Even a clock drifting 200 parts per million
/// moves only 120 ms from IB's in this time.
pub(super) const RESYNC: Duration = Duration::from_secs(600);

/// IB's time at a monotonic instant: the basis for reading IB's time at any later instant.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Base {
    at: Instant,
    time: DateTime<Utc>,
}

impl Base {
    /// IB's time at `at`, counted on the monotonic clock from this base.
    fn time_at(self, at: Instant) -> DateTime<Utc> {
        let elapsed =
            TimeDelta::from_std(at.saturating_duration_since(self.at)).unwrap_or(TimeDelta::MAX);
        self.time
            .checked_add_signed(elapsed)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    /// The base from one sample of IB's server time: `server`, read between the instants `sent`
    /// and `received`, with the host's wall clock reading `wall_sent` at `sent`.
    ///
    /// The server time is taken as read at the round trip's midpoint, and the result's second
    /// value is IB's time minus the host's at that point.
    fn from_sample(
        sent: Instant,
        received: Instant,
        wall_sent: DateTime<Utc>,
        server: DateTime<Utc>,
    ) -> (Self, TimeDelta) {
        let half = received.saturating_duration_since(sent) / 2;
        let at = sent + half;
        let wall_mid = wall_sent
            .checked_add_signed(TimeDelta::from_std(half).unwrap_or(TimeDelta::zero()))
            .unwrap_or(wall_sent);
        (
            Self { at, time: server },
            server.signed_duration_since(wall_mid),
        )
    }
}

#[derive(Debug, Default)]
struct State {
    /// `None` until a sample succeeds.
    base: Option<Base>,
    /// When the next sample is due; `None` for at once.
    next_sample: Option<Instant>,
}

impl State {
    /// Whether a sample is due at `now`, claiming it if so: the caller must sample, and every other
    /// caller until [`RESYNC`] later reads the current base.
    fn claim_sample(&mut self, now: Instant) -> bool {
        if self.next_sample.is_some_and(|due| now < due) {
            return false;
        }
        self.next_sample = Some(now + RESYNC);
        true
    }
}

/// IB's time for one connection; see the module docs.
#[derive(Debug)]
pub(super) struct IbClock {
    state: parking_lot::Mutex<State>,
    /// An offset from the host's clock larger than this is logged as a warning.
    warn_offset: TimeDelta,
}

impl IbClock {
    /// A clock with no sample yet, for bars emitted `settle` after their end. It warns of an
    /// offset from the host's clock over half of `settle`, since an error that large would eat
    /// most of the margin the settle leaves, were the correction wrong.
    pub(super) fn new(settle: TimeDelta) -> Self {
        Self {
            state: parking_lot::Mutex::new(State::default()),
            warn_offset: settle / 2,
        }
    }

    /// Whether an offset of IB's time from the host's warrants a warning.
    fn warns(&self, offset: TimeDelta) -> bool {
        offset.abs() > self.warn_offset
    }

    /// IB's time now. Samples it with `server_time` first when a sample is due, without holding
    /// the lock, so that other subscriptions read the last base meanwhile.
    ///
    /// Before the first sample succeeds, the host's wall clock stands in: a failed sample is
    /// logged and tried again [`RESYNC`] later.
    pub(super) fn now<E: std::fmt::Display>(
        &self,
        server_time: impl FnOnce() -> Result<DateTime<Utc>, E>,
    ) -> DateTime<Utc> {
        let claimed = self.state.lock().claim_sample(Instant::now());
        if claimed {
            self.sample(server_time);
        }
        let base = self.state.lock().base;
        match base {
            Some(base) => base.time_at(Instant::now()),
            None => Utc::now(),
        }
    }

    fn sample<E: std::fmt::Display>(&self, server_time: impl FnOnce() -> Result<DateTime<Utc>, E>) {
        let (sent, wall_sent) = (Instant::now(), Utc::now());
        let server = server_time();
        let received = Instant::now();
        match server {
            Ok(server) => {
                let (base, offset) = Base::from_sample(sent, received, wall_sent, server);
                self.record(base, offset, received.saturating_duration_since(sent));
            }
            Err(error) => warn!(
                %error,
                "Could not read IB's server time; candle bar ends are timed on the last \
                 estimate, or on this host's clock before the first"
            ),
        }
    }

    fn record(&self, base: Base, offset: TimeDelta, round_trip: Duration) {
        self.state.lock().base = Some(base);
        if self.warns(offset) {
            warn!(
                offset_ms = offset.num_milliseconds(),
                round_trip_ms = round_trip.as_millis(),
                "IB's server time differs from this host's clock; check the host's time \
                 synchronisation (candle bar ends are timed on IB's time)"
            );
        } else {
            debug!(
                offset_ms = offset.num_milliseconds(),
                round_trip_ms = round_trip.as_millis(),
                "Sampled IB's server time"
            );
        }
    }
}

#[cfg(test)]
// Test code: a panic is the failure report.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn a_base_counts_ibs_time_forward_on_the_monotonic_clock() {
        let start = Instant::now();
        let base = Base {
            at: start,
            time: at("2026-11-23T14:30:00Z"),
        };
        assert_eq!(base.time_at(start), at("2026-11-23T14:30:00Z"));
        assert_eq!(
            base.time_at(start + Duration::from_millis(61_500)),
            at("2026-11-23T14:31:01.500Z")
        );
    }

    #[test]
    fn a_base_never_reads_time_before_itself() {
        let start = Instant::now() + Duration::from_secs(5);
        let base = Base {
            at: start,
            time: at("2026-11-23T14:30:00Z"),
        };
        assert_eq!(
            base.time_at(start - Duration::from_secs(5)),
            at("2026-11-23T14:30:00Z")
        );
    }

    #[test]
    fn a_sample_is_placed_at_the_round_trips_midpoint() {
        let sent = Instant::now();
        let received = sent + Duration::from_millis(200);
        let (base, offset) = Base::from_sample(
            sent,
            received,
            at("2026-11-23T14:30:00Z"),
            at("2026-11-23T14:30:01.600Z"),
        );
        assert_eq!(base.at, sent + Duration::from_millis(100));
        assert_eq!(base.time, at("2026-11-23T14:30:01.600Z"));
        // IB read 14:30:01.600 when the host read 14:30:00.100.
        assert_eq!(offset, TimeDelta::milliseconds(1_500));
        assert_eq!(base.time_at(received), at("2026-11-23T14:30:01.700Z"));
    }

    #[test]
    fn a_host_clock_ahead_gives_a_negative_offset() {
        let sent = Instant::now();
        let (_, offset) = Base::from_sample(
            sent,
            sent,
            at("2026-11-23T14:30:03Z"),
            at("2026-11-23T14:30:00Z"),
        );
        assert_eq!(offset, TimeDelta::seconds(-3));
    }

    #[test]
    fn a_sample_is_claimed_once_per_resync() {
        let mut state = State::default();
        let start = Instant::now();
        assert!(state.claim_sample(start));
        assert!(!state.claim_sample(start));
        assert!(!state.claim_sample(start + RESYNC - Duration::from_millis(1)));
        assert!(state.claim_sample(start + RESYNC));
        assert!(!state.claim_sample(start + RESYNC));
    }

    #[test]
    fn an_offset_over_half_the_settle_warns_either_way() {
        let clock = IbClock::new(TimeDelta::seconds(2));
        assert!(!clock.warns(TimeDelta::zero()));
        assert!(!clock.warns(TimeDelta::seconds(1)));
        assert!(!clock.warns(TimeDelta::seconds(-1)));
        assert!(clock.warns(TimeDelta::milliseconds(1_001)));
        assert!(clock.warns(TimeDelta::milliseconds(-1_001)));
    }

    #[test]
    fn now_reads_ibs_time_from_the_sample() {
        let clock = IbClock::new(TimeDelta::seconds(2));
        let ahead = TimeDelta::seconds(3_600);
        let read = clock.now(|| Ok::<_, String>(Utc::now() + ahead));
        let host = Utc::now();
        assert!(
            read > host + ahead - TimeDelta::seconds(1),
            "{read} vs {host}"
        );
        // The sample is placed at the round trip's midpoint, so it may read a little ahead.
        assert!(
            read < host + ahead + TimeDelta::seconds(1),
            "{read} vs {host}"
        );
        // The next read uses the base without sampling again.
        let again = clock
            .now(|| -> Result<DateTime<Utc>, String> { panic!("sampled again before the resync") });
        assert!(again >= read);
    }

    #[test]
    fn now_counts_forward_from_the_base_between_samples() {
        let clock = IbClock::new(TimeDelta::seconds(2));
        let base = Base {
            at: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
            time: at("2026-11-23T14:30:00Z"),
        };
        {
            let mut state = clock.state.lock();
            state.base = Some(base);
            state.next_sample = Some(Instant::now() + RESYNC);
        }
        let read =
            clock.now(|| -> Result<DateTime<Utc>, String> { panic!("sampled before the resync") });
        assert!(read >= at("2026-11-23T14:30:05Z"), "{read}");
        assert!(read < at("2026-11-23T14:30:06Z"), "{read}");
    }

    #[test]
    fn now_falls_back_to_the_hosts_clock_until_a_sample_succeeds() {
        let clock = IbClock::new(TimeDelta::seconds(2));
        let before = Utc::now();
        let read = clock.now(|| Err::<DateTime<Utc>, _>("no answer"));
        assert!(read >= before && read <= Utc::now());
        assert_eq!(clock.state.lock().base, None);
    }

    #[test]
    fn a_failed_resample_keeps_the_last_base() {
        let clock = IbClock::new(TimeDelta::seconds(2));
        let ahead = TimeDelta::seconds(3_600);
        clock.now(|| Ok::<_, String>(Utc::now() + ahead));
        let base = clock.state.lock().base;
        clock.state.lock().next_sample = None;
        let read = clock.now(|| Err::<DateTime<Utc>, _>("no answer"));
        assert_eq!(clock.state.lock().base, base);
        assert!(read > Utc::now() + ahead - TimeDelta::seconds(1));
    }
}
