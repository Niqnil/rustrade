//! Bounded, cycle-safe pagination for Massive REST streams.
//!
//! Every Massive REST endpoint follows the same `next_url` cursor protocol: a
//! page carries an optional `next_url` pointing at the following page, and the
//! stream terminates when a page omits it. Two failure modes make that loop
//! unsafe if followed unconditionally:
//!
//! - a server (or proxy) that never stops returning a `next_url` would page
//!   forever, and
//! - a `next_url` that points back to an already-fetched page would loop over
//!   the same responses indefinitely.
//!
//! [`PaginationGuard`] makes both observable: it caps the page count at
//! [`MAX_PAGES`] and records visited URLs to detect cycles, returning a terminal
//! [`MassiveError`] instead of looping. It intentionally does **not** truncate
//! silently — for a market-data client a short `Vec` is indistinguishable from a
//! genuinely small result set, so incomplete pagination must fail loudly.

use super::error::MassiveError;
use crate::exchange::http::truncate_str;
use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

/// Maximum number of pages a single paginated fetch will follow.
///
/// This is a runaway backstop, not a business limit: it is set well above the
/// largest legitimate paginated response (e.g. the full reference-ticker
/// universe), so reaching it signals a pathological query or a misbehaving
/// server rather than normal operation.
pub(super) const MAX_PAGES: usize = 10_000;

/// Maximum byte length of a URL this paginator will follow.
///
/// Like [`MAX_PAGES`], a runaway backstop rather than a business limit: a real
/// Massive cursor URL is a few hundred bytes, and no mainstream HTTP stack
/// accepts a request line beyond a few KiB, so this only rejects pathological
/// input. It matters because a page's `next_url` is server-controlled and
/// success bodies are read unbounded — without a cap, a misbehaving origin
/// could hand back arbitrarily long URLs, and the guard would retain one per
/// page for the life of the stream.
pub(super) const MAX_URL_BYTES: usize = 8 * 1024;

/// Bytes of a rejected URL retained for diagnosis (see
/// [`MassiveError::PaginationUrlTooLong`]).
const URL_PREFIX_BYTES: usize = 200;

/// Tracks pagination progress across a single Massive REST stream, enforcing a
/// page-count cap and detecting `next_url` cycles.
///
/// Construct once per stream (via [`Default`]) and call [`observe`] once per page
/// with the URL about to be fetched. The guard borrows nothing and is reset by
/// simply creating a new instance.
///
/// [`observe`]: PaginationGuard::observe
#[derive(Debug, Default)]
pub(super) struct PaginationGuard {
    pages: usize,
    /// Fixed-size fingerprints of the URLs already fetched, for cycle detection.
    /// Bounded by [`MAX_PAGES`] entries: [`observe`] checks the page cap before
    /// inserting, so growth cannot exceed the cap before the stream terminates.
    ///
    /// Fingerprints rather than the URLs themselves so each entry costs a
    /// constant 8 bytes regardless of URL length — the URLs are server-supplied
    /// and would otherwise be retained in full, up to [`MAX_PAGES`] of them.
    /// Storing a *truncated* URL would not work: two distinct URLs sharing a
    /// long prefix would compare equal and be reported as a false cycle, whereas
    /// a hash is computed over the whole string.
    ///
    /// [`observe`]: PaginationGuard::observe
    visited: HashSet<u64>,
    /// Per-guard (and therefore per-stream) hash key.
    ///
    /// Deliberately [`RandomState`] rather than [`DefaultHasher::new`], whose
    /// SipHash key is a fixed `(0, 0)` published in std's source. With a known
    /// key, a misbehaving origin could precompute two distinct, well-formed
    /// `next_url`s that collide and so force a spurious
    /// [`MassiveError::CyclicPagination`] on a legitimate stream. A key the
    /// server cannot know removes that possibility — which is precisely the
    /// hash-flooding threat model SipHash was designed for.
    ///
    /// [`DefaultHasher::new`]: std::hash::DefaultHasher::new
    hasher: RandomState,
}

impl PaginationGuard {
    /// Record that `url` is about to be fetched as the next page.
    ///
    /// Returns:
    /// - [`MassiveError::PaginationLimitExceeded`] once more than [`MAX_PAGES`]
    ///   pages have been observed,
    /// - [`MassiveError::PaginationUrlTooLong`] if `url` exceeds
    ///   [`MAX_URL_BYTES`], and
    /// - [`MassiveError::CyclicPagination`] if `url` was already observed.
    ///
    /// All three are terminal: propagate them to end the stream rather than
    /// continuing to page.
    ///
    /// The length check runs before the URL is hashed or requested, so a
    /// pathological value is rejected without being retained. It applies to
    /// every URL the paginator follows — in practice always a server-supplied
    /// `next_url`, but the first page's URL (built by this client from the
    /// caller's query) passes through the same check, which is why the error is
    /// named for the paginator rather than for `next_url`.
    pub(super) fn observe(&mut self, url: &str) -> Result<(), MassiveError> {
        self.pages += 1;
        if self.pages > MAX_PAGES {
            return Err(MassiveError::PaginationLimitExceeded {
                pages: self.pages,
                limit: MAX_PAGES,
            });
        }
        if url.len() > MAX_URL_BYTES {
            return Err(MassiveError::PaginationUrlTooLong {
                len: url.len(),
                limit: MAX_URL_BYTES,
                prefix: truncate_str(url, URL_PREFIX_BYTES),
            });
        }
        if !self.visited.insert(self.hasher.hash_one(url)) {
            return Err(MassiveError::CyclicPagination {
                url: url.to_owned(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // Test code: panics on unexpected values are acceptable
mod tests {
    use super::*;

    #[test]
    fn distinct_urls_under_the_cap_are_accepted() {
        let mut guard = PaginationGuard::default();
        for i in 0..MAX_PAGES {
            assert!(
                guard
                    .observe(&format!("https://api.massive.com/page/{i}"))
                    .is_ok(),
                "page {i} within the cap should be accepted"
            );
        }
    }

    #[test]
    fn exceeding_the_page_cap_is_a_terminal_error() {
        let mut guard = PaginationGuard::default();
        // Exactly MAX_PAGES distinct pages are allowed.
        for i in 0..MAX_PAGES {
            guard
                .observe(&format!("https://api.massive.com/page/{i}"))
                .expect("within cap");
        }
        // The next distinct page trips the backstop.
        let err = guard
            .observe("https://api.massive.com/page/over")
            .expect_err("page MAX_PAGES + 1 must be rejected");
        assert_eq!(
            err,
            MassiveError::PaginationLimitExceeded {
                pages: MAX_PAGES + 1,
                limit: MAX_PAGES,
            }
        );
    }

    #[test]
    fn revisiting_a_url_is_detected_as_a_cycle() {
        let mut guard = PaginationGuard::default();
        let url = "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/minute/0/1?cursor=abc";
        guard.observe(url).expect("first visit is fine");
        let err = guard.observe(url).expect_err("second visit is a cycle");
        assert_eq!(
            err,
            MassiveError::CyclicPagination {
                url: url.to_owned()
            }
        );
    }

    #[test]
    fn cycle_detection_is_url_exact() {
        // URLs differing by cursor are distinct pages, not a cycle.
        let mut guard = PaginationGuard::default();
        guard
            .observe("https://api.massive.com/x?cursor=a")
            .expect("page 1");
        guard
            .observe("https://api.massive.com/x?cursor=b")
            .expect("page 2 is a different URL");
    }

    #[test]
    fn long_urls_sharing_a_prefix_are_not_a_false_cycle() {
        // The property that rules out storing a *truncated* URL as the dedup key: these two differ
        // only in their final byte, far beyond any sane truncation point, yet are distinct pages.
        // Hashing the whole string keeps them distinct; a truncating key would report a cycle and
        // kill a legitimate stream.
        let mut guard = PaginationGuard::default();
        let base = format!("https://api.massive.com/x?cursor={}", "a".repeat(2048));
        guard.observe(&format!("{base}1")).expect("page 1");
        guard
            .observe(&format!("{base}2"))
            .expect("page 2 differs only in its last byte but is a different URL");
    }

    #[test]
    fn a_url_at_the_byte_cap_is_accepted() {
        let mut guard = PaginationGuard::default();
        let url = "a".repeat(MAX_URL_BYTES);
        guard.observe(&url).expect("exactly at the cap is allowed");
    }

    #[test]
    fn a_url_over_the_byte_cap_is_rejected_with_a_bounded_prefix() {
        let mut guard = PaginationGuard::default();
        let url = format!("https://api.massive.com/{}", "a".repeat(MAX_URL_BYTES));
        let err = guard
            .observe(&url)
            .expect_err("past the cap must be rejected");

        match err {
            MassiveError::PaginationUrlTooLong { len, limit, prefix } => {
                assert_eq!(len, url.len());
                assert_eq!(limit, MAX_URL_BYTES);
                assert_eq!(prefix.len(), URL_PREFIX_BYTES);
                assert!(prefix.starts_with("https://api.massive.com/"));
            }
            other => panic!("expected PaginationUrlTooLong, got {other:?}"),
        }
    }

    #[test]
    fn an_oversized_url_is_rejected_before_being_recorded() {
        // The point of checking length *before* insertion: the guard must not retain the very value
        // it just refused, or the bound would be pointless.
        let mut guard = PaginationGuard::default();
        let url = "a".repeat(MAX_URL_BYTES + 1);
        guard.observe(&url).expect_err("rejected");
        assert!(
            guard.visited.is_empty(),
            "a rejected URL must leave no trace in the visited set"
        );
    }
}
