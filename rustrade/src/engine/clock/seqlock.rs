//! An instant shared between threads without a lock: a sequence lock over its seconds and
//! nanoseconds.
//!
//! Every atomic, the fence and the spin hint come from the parent's `sync` module, never from
//! `std::sync::atomic` directly, so a model checker can compile this file against its own atomics
//! and check all of them.

use super::sync::{AtomicI64, AtomicU32, AtomicU64, Ordering, fence, spin};
use chrono::{DateTime, Utc};

/// An instant as [`SeqLockInstant`] stores it: `DateTime::timestamp` and
/// `DateTime::timestamp_subsec_nanos`.
///
/// Ordered as the `DateTime<Utc>` it came from: by seconds, then by nanoseconds, which exceed one
/// second only during a leap second, and so come after every other nanosecond of that second.
/// Comparing these needs no date arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RawInstant {
    secs: i64,
    nanos: u32,
}

impl From<DateTime<Utc>> for RawInstant {
    fn from(time: DateTime<Utc>) -> Self {
        Self {
            secs: time.timestamp(),
            nanos: time.timestamp_subsec_nanos(),
        }
    }
}

impl RawInstant {
    // Every `RawInstant` comes from a valid `DateTime<Utc>`: the fields are private, built only
    // by `From`, and `SeqLockInstant` stores and reads them as one pair.
    #[allow(clippy::expect_used)] // Unreachable for the reason above
    pub(super) fn to_datetime(self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.secs, self.nanos).expect("the fields of one DateTime<Utc>")
    }
}

/// A `DateTime<Utc>` that threads read without a lock and advance monotonically.
///
/// - **Reads** take no lock: a read loads the version, both fields, then the version again, and
///   retries if a write overlapped it. A read never blocks a write. A read that finds a write in
///   progress spins until it ends, which takes a few stores, unless the writing thread is
///   descheduled in between.
/// - **Writes** serialise among themselves: a write takes an odd version by compare-and-swap,
///   stores, then publishes the next even version. A second writer spins until the first is done.
/// - **Monotonic:** a write stores its instant only if that is later than the one held, decided
///   while it holds the version, so the instant never moves backwards whatever order concurrent
///   writers arrive in.
///
/// Every `DateTime<Utc>` is representable, leap-second nanoseconds included.
///
/// Aligned to 128 bytes so instants allocated next to each other and used by different threads
/// do not share a cache line, or a pair of lines that the hardware prefetches together.
#[repr(align(128))]
pub(super) struct SeqLockInstant {
    /// Even while no write is in progress; odd while one is.
    version: AtomicU64,
    /// [`RawInstant::secs`].
    secs: AtomicI64,
    /// [`RawInstant::nanos`].
    nanos: AtomicU32,
}

impl SeqLockInstant {
    pub(super) fn new(time: DateTime<Utc>) -> Self {
        let RawInstant { secs, nanos } = time.into();
        Self {
            version: AtomicU64::new(0),
            secs: AtomicI64::new(secs),
            nanos: AtomicU32::new(nanos),
        }
    }

    /// The instant held.
    pub(super) fn load(&self) -> DateTime<Utc> {
        self.load_raw().to_datetime()
    }

    fn load_raw(&self) -> RawInstant {
        loop {
            // Acquire: once this reads a writer's closing version, the fields it published are
            // visible.
            let before = self.version.load(Ordering::Acquire);
            if before.is_multiple_of(2) {
                let secs = self.secs.load(Ordering::Relaxed);
                let nanos = self.nanos.load(Ordering::Relaxed);
                // If either load read a store of a write after `before`, that store follows the
                // write's release fence, so this fence makes the write's odd version visible to the
                // check below, which then fails.
                fence(Ordering::Acquire);
                if self.version.load(Ordering::Relaxed) == before {
                    return RawInstant { secs, nanos };
                }
            }
            spin();
        }
    }

    /// Moves the instant to `time` if `time` is later than the instant held, and returns the
    /// instant held before.
    pub(super) fn advance(&self, time: RawInstant) -> RawInstant {
        // The instant only moves forwards, so a `time` no later than one read is no later than
        // the instant held now: no write needed.
        let current = self.load_raw();
        if time <= current {
            return current;
        }

        let version = self.lock();
        // The previous writer's stores are visible: `lock` acquired the version it published.
        let held = RawInstant {
            secs: self.secs.load(Ordering::Relaxed),
            nanos: self.nanos.load(Ordering::Relaxed),
        };
        if time > held {
            self.secs.store(time.secs, Ordering::Relaxed);
            self.nanos.store(time.nanos, Ordering::Relaxed);
        }
        // Release: a reader or writer that acquires this version sees the stores above.
        self.version
            .store(version.wrapping_add(2), Ordering::Release);
        held
    }

    /// Takes the version from even to odd, excluding other writers, and returns the even version
    /// it took.
    fn lock(&self) -> u64 {
        loop {
            let version = self.version.load(Ordering::Relaxed);
            if version.is_multiple_of(2)
                && self
                    .version
                    .compare_exchange_weak(
                        version,
                        version.wrapping_add(1),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                // Orders the odd version before the field stores that follow, for `load`'s fence.
                fence(Ordering::Release);
                return version;
            }
            spin();
        }
    }
}
