//! The time an IBKR client stamps its order reports with.

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::sync::Arc;

/// This host's clock, held so that no stamp it gives is earlier than one it gave before.
///
/// IB gives an order's status and terms no time of their own, so every order report the client
/// builds from them (a status, a placement or cancel answer, an open-order listing, an ended order
/// IB lists without a completion time) is stamped with this host's time when it is made, and a
/// reader orders the reports of an order by that stamp. The wall clock can step back (an NTP correction, say), which would stamp
/// a later report earlier than the one before it. So each stamp is the later of the clock and the
/// latest stamp given, by any thread of the client: a stamp taken after another is never earlier.
///
/// Shared by clones, like the rest of the client's state.
#[derive(Debug, Clone, Default)]
pub(crate) struct OrderClock {
    last: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl OrderClock {
    /// The time to stamp a report made now with.
    pub(crate) fn now(&self) -> DateTime<Utc> {
        self.stamp(Utc::now())
    }

    /// A clock that has already given `stamp`, so that none it gives is earlier.
    #[cfg(test)]
    pub(crate) fn stamped_at(stamp: DateTime<Utc>) -> Self {
        Self {
            last: Arc::new(Mutex::new(Some(stamp))),
        }
    }

    /// The stamp for a report made when the wall clock reads `wall`: `wall`, or the latest stamp
    /// given if that is later.
    fn stamp(&self, wall: DateTime<Utc>) -> DateTime<Utc> {
        let mut last = self.last.lock();
        let stamp = last.map_or(wall, |last| last.max(wall));
        *last = Some(stamp);
        stamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    /// A stamp follows the wall clock forward, and holds at the latest stamp while the clock
    /// reads earlier, across clones.
    #[test]
    fn a_stamp_never_goes_back() {
        let clock = OrderClock::default();
        let t0 = DateTime::<Utc>::UNIX_EPOCH + TimeDelta::days(20_000);
        let second = |n| t0 + TimeDelta::seconds(n);

        assert_eq!(clock.stamp(second(10)), second(10));
        assert_eq!(clock.stamp(second(12)), second(12));
        assert_eq!(clock.clone().stamp(second(11)), second(12), "stepped back");
        assert_eq!(clock.stamp(second(12)), second(12));
        assert_eq!(clock.stamp(second(13)), second(13));
    }

    #[test]
    fn now_reads_the_wall_clock() {
        let clock = OrderClock::default();
        let before = Utc::now();
        let stamp = clock.now();
        assert!((before..=Utc::now()).contains(&stamp));
    }
}
