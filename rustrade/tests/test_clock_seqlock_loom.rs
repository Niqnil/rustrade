#![allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable

//! Model-checks the `HistoricalClock`'s seqlock with `loom`, which runs each test under every
//! interleaving of its threads, and every value the memory model lets each atomic load return.
//!
//! The seqlock's source is compiled here against `loom`'s atomics: it takes them from its parent's
//! `sync` module, which is this file's below.

use chrono::{DateTime, TimeDelta, Utc};
use loom::{sync::Arc, thread};

#[path = "../src/engine/clock/seqlock.rs"]
mod seqlock;

mod sync {
    pub(super) use loom::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering, fence};

    /// `loom` explores a spin loop only if it yields.
    pub(super) fn spin() {
        loom::thread::yield_now();
    }
}

use seqlock::SeqLockInstant;

/// Instants that differ in both seconds and nanoseconds, so a read mixing two of them is none of
/// them.
fn instant(n: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap()
        + TimeDelta::seconds(n)
        + TimeDelta::nanoseconds(n)
}

/// A read overlapping an advance sees the instant before it or after it, never a mix, and a later
/// read on the same thread never sees an earlier instant.
#[test]
fn a_read_overlapping_an_advance_sees_one_instant() {
    loom::model(|| {
        let clock = Arc::new(SeqLockInstant::new(instant(0)));

        let writer = {
            let clock = Arc::clone(&clock);
            thread::spawn(move || clock.advance(instant(1)))
        };

        let first = clock.load();
        let second = clock.load();
        assert!(first == instant(0) || first == instant(1), "{first:?}");
        assert!(second == instant(0) || second == instant(1), "{second:?}");
        assert!(second >= first, "{second:?} read after {first:?}");

        assert_eq!(writer.join().unwrap(), instant(0));
        assert_eq!(clock.load(), instant(1));
    });
}

/// Two advances racing end at the later instant whichever stores first, each reports an instant
/// actually held before it, and a reader alongside never sees the clock move backwards.
#[test]
fn racing_advances_end_at_the_later_instant() {
    loom::model(|| {
        let clock = Arc::new(SeqLockInstant::new(instant(0)));

        let writers: Vec<_> = [instant(2), instant(1)]
            .into_iter()
            .map(|time| {
                let clock = Arc::clone(&clock);
                thread::spawn(move || clock.advance(time))
            })
            .collect();

        let first = clock.load();
        let second = clock.load();
        assert!(
            [instant(0), instant(1), instant(2)].contains(&first),
            "{first:?}"
        );
        assert!(second >= first, "{second:?} read after {first:?}");

        let held_before: Vec<_> = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect();
        // Advancing to 2 saw 0 or 1 held; advancing to 1 saw 0 or 2.
        assert!(
            [instant(0), instant(1)].contains(&held_before[0]),
            "{held_before:?}"
        );
        assert!(
            [instant(0), instant(2)].contains(&held_before[1]),
            "{held_before:?}"
        );
        assert_eq!(clock.load(), instant(2));
    });
}
