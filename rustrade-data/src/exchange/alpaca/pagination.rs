//! Bounded, cycle-safe pagination for Alpaca REST fetches.
//!
//! Every paginated Alpaca REST endpoint follows the same `page_token` cursor
//! protocol: a page carries an optional `next_page_token` to be echoed back as
//! the `page_token` query parameter of the following request, and pagination
//! terminates when a page omits it. Two failure modes make that loop unsafe if
//! followed unconditionally:
//!
//! - a server (or proxy) that never stops returning a `next_page_token` would
//!   page forever, and
//! - a token that repeats an already-used cursor would fetch the same pages
//!   indefinitely.
//!
//! [`PaginationGuard`] makes both observable: it caps the page count and records
//! used tokens to detect cycles, returning a terminal [`AlpacaRestError`] instead
//! of looping. It intentionally does **not** truncate silently — for a
//! market-data client a short result is indistinguishable from a genuinely small
//! result set, so incomplete pagination must fail loudly. Mirrors the
//! `PaginationGuard` of the Massive REST client, adapted from that client's
//! server-supplied `next_url`s to Alpaca's opaque cursor tokens.

use super::rest::AlpacaRestError;
use crate::exchange::http::truncate_str;
use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

/// Bytes of a cycling `page_token` retained for diagnosis (see
/// [`AlpacaRestError::CyclicPagination`]).
///
/// Real Alpaca tokens are well under this, so in practice the whole token is
/// retained; the bound only trims a pathological server-supplied value.
const TOKEN_PREFIX_BYTES: usize = 200;

/// Tracks pagination progress across a single Alpaca REST fetch, enforcing a
/// page-count cap and detecting `page_token` cycles.
///
/// Construct once per fetch with the call site's page limit and call [`observe`]
/// once per page — before the request — with the `page_token` about to be sent
/// (`None` for the first page, which has no cursor). The guard borrows nothing
/// and is reset by simply creating a new instance.
///
/// [`observe`]: PaginationGuard::observe
#[derive(Debug)]
pub(super) struct PaginationGuard {
    /// Maximum number of pages this fetch may follow. A runaway backstop rather
    /// than a business limit — reaching it signals a pathological query or a
    /// misbehaving server, not normal operation. Per call site because the
    /// Alpaca endpoints carry different documented bounds.
    limit: usize,
    pages: usize,
    /// Fixed-size fingerprints of the tokens already used, for cycle detection.
    /// Bounded by `limit` entries: [`observe`] checks the page cap before
    /// inserting, so growth cannot exceed the cap before the fetch terminates.
    ///
    /// Fingerprints rather than the tokens themselves so each entry costs a
    /// constant 8 bytes regardless of token length — the tokens are
    /// server-supplied and would otherwise be retained in full. Storing a
    /// *truncated* token would not work: two distinct tokens sharing a long
    /// prefix would compare equal and be reported as a false cycle, whereas a
    /// hash is computed over the whole string.
    ///
    /// [`observe`]: PaginationGuard::observe
    visited: HashSet<u64>,
    /// Per-guard (and therefore per-fetch) hash key.
    ///
    /// Deliberately [`RandomState`] rather than [`DefaultHasher::new`], whose
    /// SipHash key is a fixed `(0, 0)` published in std's source. With a known
    /// key, a misbehaving origin could precompute two distinct tokens that
    /// collide and so force a spurious [`AlpacaRestError::CyclicPagination`] on
    /// a legitimate fetch. A key the server cannot know removes that
    /// possibility — which is precisely the hash-flooding threat model SipHash
    /// was designed for.
    ///
    /// [`DefaultHasher::new`]: std::hash::DefaultHasher::new
    hasher: RandomState,
}

impl PaginationGuard {
    /// Create a guard that allows at most `limit` pages.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            pages: 0,
            // Sized up front: `observe` inserts at most one fingerprint per page,
            // so `limit` covers the fetch without rehashing.
            visited: HashSet::with_capacity(limit),
            hasher: RandomState::new(),
        }
    }

    /// Record that a page is about to be fetched with `page_token` as its cursor
    /// (`None` for the first page, which carries no cursor and therefore cannot
    /// cycle).
    ///
    /// Returns:
    /// - [`AlpacaRestError::PaginationLimitExceeded`] once more than `limit`
    ///   pages have been observed, and
    /// - [`AlpacaRestError::CyclicPagination`] if `page_token` was already used
    ///   by an earlier page of this fetch.
    ///
    /// Both are terminal: propagate them to end the fetch rather than
    /// continuing to page.
    pub(super) fn observe(&mut self, page_token: Option<&str>) -> Result<(), AlpacaRestError> {
        self.pages += 1;
        if self.pages > self.limit {
            return Err(AlpacaRestError::PaginationLimitExceeded {
                pages: self.pages,
                limit: self.limit,
            });
        }
        if let Some(token) = page_token
            && !self.visited.insert(self.hasher.hash_one(token))
        {
            return Err(AlpacaRestError::CyclicPagination {
                page_token: truncate_str(token, TOKEN_PREFIX_BYTES),
            });
        }
        Ok(())
    }

    /// Number of pages observed so far (1-based once the first page has been
    /// observed) — for progress logging at the call sites.
    pub(super) fn pages(&self) -> usize {
        self.pages
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // Test code: panics on unexpected values are acceptable
mod tests {
    use super::*;

    #[test]
    fn distinct_tokens_under_the_cap_are_accepted() {
        let mut guard = PaginationGuard::new(5);
        guard.observe(None).expect("first page has no token");
        for i in 1..5 {
            guard
                .observe(Some(&format!("token-{i}")))
                .unwrap_or_else(|_| panic!("page {} within the cap should be accepted", i + 1));
        }
    }

    #[test]
    fn exceeding_the_page_cap_is_a_terminal_error() {
        let mut guard = PaginationGuard::new(3);
        // Exactly `limit` pages are allowed.
        guard.observe(None).expect("page 1 within cap");
        guard.observe(Some("a")).expect("page 2 within cap");
        guard.observe(Some("b")).expect("page 3 within cap");
        // The next page trips the backstop — even before any cycle question arises.
        let err = guard
            .observe(Some("c"))
            .expect_err("page limit + 1 must be rejected");
        assert!(
            matches!(
                err,
                AlpacaRestError::PaginationLimitExceeded { pages: 4, limit: 3 }
            ),
            "expected PaginationLimitExceeded {{ pages: 4, limit: 3 }}, got {err:?}"
        );
    }

    #[test]
    fn reusing_a_token_is_detected_as_a_cycle() {
        let mut guard = PaginationGuard::new(10);
        guard.observe(None).expect("first page");
        guard
            .observe(Some("cursor-abc"))
            .expect("first use is fine");
        let err = guard
            .observe(Some("cursor-abc"))
            .expect_err("second use is a cycle");
        assert!(
            matches!(
                &err,
                AlpacaRestError::CyclicPagination { page_token } if page_token == "cursor-abc"
            ),
            "expected CyclicPagination for cursor-abc, got {err:?}"
        );
    }

    #[test]
    fn cycle_detection_is_token_exact() {
        // Tokens differing anywhere are distinct cursors, not a cycle.
        let mut guard = PaginationGuard::new(10);
        guard.observe(Some("cursor=a")).expect("page 1");
        guard
            .observe(Some("cursor=b"))
            .expect("page 2 is a different token");
    }

    #[test]
    fn tokenless_pages_never_cycle() {
        // `None` marks a page with no cursor (the first page). It participates in
        // the page count but not in cycle detection — there is no token to repeat.
        let mut guard = PaginationGuard::new(10);
        guard.observe(None).expect("page 1");
        guard
            .observe(None)
            .expect("a second tokenless page is not a cycle");
    }

    #[test]
    fn long_tokens_sharing_a_prefix_are_not_a_false_cycle() {
        // The property that rules out storing a *truncated* token as the dedup key:
        // these two differ only in their final byte, beyond the diagnostic-prefix
        // bound, yet are distinct cursors. Hashing the whole string keeps them
        // distinct; a truncating key would report a cycle and kill a legitimate fetch.
        let mut guard = PaginationGuard::new(10);
        let base = "a".repeat(2 * TOKEN_PREFIX_BYTES);
        guard.observe(Some(&format!("{base}1"))).expect("page 1");
        guard
            .observe(Some(&format!("{base}2")))
            .expect("page 2 differs only in its last byte but is a different token");
    }

    #[test]
    fn a_cycling_oversized_token_is_reported_with_a_bounded_prefix() {
        // The error must not retain a pathological server-supplied token in full.
        let mut guard = PaginationGuard::new(10);
        let token = "a".repeat(2 * TOKEN_PREFIX_BYTES);
        guard.observe(Some(&token)).expect("first use is fine");
        let err = guard
            .observe(Some(&token))
            .expect_err("second use is a cycle");
        match err {
            AlpacaRestError::CyclicPagination { page_token } => {
                assert_eq!(page_token.len(), TOKEN_PREFIX_BYTES);
                assert!(token.starts_with(&page_token));
            }
            other => panic!("expected CyclicPagination, got {other:?}"),
        }
    }
}
