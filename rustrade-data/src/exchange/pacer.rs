//! Even request spacing, shared by the REST providers whose limits are a request rate.

use parking_lot::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Spaces requests evenly: each waits for the slot one interval after the previous one's. Shared
/// through an `Arc` by a client's clones, so they keep one pace between them.
#[derive(Debug)]
pub(crate) struct Pacer {
    interval: Duration,
    next: Mutex<Option<Instant>>,
}

impl Pacer {
    /// A pace of one request every `interval`.
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: Mutex::new(None),
        }
    }

    /// The time between two requests' slots.
    #[cfg(test)]
    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    /// Wait for this request's slot. A caller dropped while waiting leaves its slot unused: later
    /// requests still wait for it, one interval each, but none is blocked indefinitely.
    pub(crate) async fn wait(&self) {
        tokio::time::sleep_until(self.claim()).await;
    }

    /// Claim the next free slot: now, or one interval after the last claimed slot if that is
    /// later.
    fn claim(&self) -> Instant {
        let mut next = self.next.lock();
        let now = Instant::now();
        let slot = next.map_or(now, |next| next.max(now));
        *next = Some(slot + self.interval);
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn the_pacer_spaces_requests_by_its_interval_and_shares_across_clones() {
        let pacer = Arc::new(Pacer::new(Duration::from_millis(250)));
        assert_eq!(pacer.interval(), Duration::from_millis(250));
        let start = Instant::now();
        pacer.wait().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        let other = Arc::clone(&pacer);
        other.wait().await;
        assert_eq!(start.elapsed(), Duration::from_millis(250));
        pacer.wait().await;
        assert_eq!(start.elapsed(), Duration::from_millis(500));

        // After an idle spell the next request goes at once, and the one after waits again.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let idle_end = Instant::now();
        pacer.wait().await;
        assert_eq!(idle_end.elapsed(), Duration::ZERO);
        pacer.wait().await;
        assert_eq!(idle_end.elapsed(), Duration::from_millis(250));
    }
}
