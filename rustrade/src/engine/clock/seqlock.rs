//! An instant shared between threads without a lock: a sequence lock over its seconds and
//! nanoseconds.
//!
//! The atomics, the fence and the spin hint come from the parent's `sync` module, so a model
//! checker can compile this file against its own atomics.

use super::sync::{AtomicI64, AtomicU32, AtomicU64, Ordering, fence, spin};
use chrono::{DateTime, Utc};

/// A `DateTime<Utc>` that threads read without a lock and advance monotonically.
///
/// - **Reads** are lock-free: a read loads the version, both fields, then the version again, and
///   retries if a write overlapped it. A read never blocks a write.
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
    /// `DateTime::timestamp`.
    secs: AtomicI64,
    /// `DateTime::timestamp_subsec_nanos`, which exceeds one second during a leap second.
    nanos: AtomicU32,
}

impl SeqLockInstant {
    pub(super) fn new(time: DateTime<Utc>) -> Self {
        Self {
            version: AtomicU64::new(0),
            secs: AtomicI64::new(time.timestamp()),
            nanos: AtomicU32::new(time.timestamp_subsec_nanos()),
        }
    }

    /// The instant held.
    pub(super) fn load(&self) -> DateTime<Utc> {
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
                    return decode(secs, nanos);
                }
            }
            spin();
        }
    }

    /// Moves the instant to `time` if `time` is later than the instant held, and returns the
    /// instant held before.
    pub(super) fn advance(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        // The instant only moves forwards, so a `time` no later than one read is no later than
        // the instant held now: no write needed.
        let current = self.load();
        if time <= current {
            return current;
        }

        let version = self.lock();
        // The previous writer's stores are visible: `lock` acquired the version it published.
        let held = decode(
            self.secs.load(Ordering::Relaxed),
            self.nanos.load(Ordering::Relaxed),
        );
        if time > held {
            self.secs.store(time.timestamp(), Ordering::Relaxed);
            self.nanos
                .store(time.timestamp_subsec_nanos(), Ordering::Relaxed);
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

/// Rebuilds an instant from the fields of one write, or of `new`.
// Both fields always come from one valid `DateTime<Utc>`: `new` and `advance` store the pair
// together, and the version check discards a read that overlapped a write.
#[allow(clippy::expect_used)] // Unreachable for the reason above
fn decode(secs: i64, nanos: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, nanos).expect("the fields of one stored DateTime<Utc>")
}
