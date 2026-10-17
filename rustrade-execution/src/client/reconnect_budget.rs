//! The reconnect budget of the Alpaca and Binance account streams.
//!
//! A stream spends one attempt on each failed reconnect, waiting an exponentially growing delay
//! before it, and ends once the budget is spent. A connection that proves live resets it. When a
//! connection counts as live, and how the stream ends, is each client's own and is stated in its
//! rustdoc.
//!
//! This is not `rustrade_integration::socket::backoff::ReconnectBackoff`. That trait is a delay
//! curve for the market-data streams, which retry without limit; this budget is bounded, keeps its
//! own count, and stays crate-private.

use std::time::Duration;
use tracing::debug;

/// The waits and the number of attempts a [`ReconnectBudget`] allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackoffLimits {
    /// The wait before the first attempt. Each later wait doubles the one before.
    pub(crate) initial: Duration,
    /// The longest wait: the doubling stops here.
    pub(crate) max_delay: Duration,
    /// How many attempts in a row may fail before the budget is spent.
    pub(crate) max_attempts: u32,
}

impl BackoffLimits {
    /// The account streams' budget: waits of 1 s doubling up to 30 s, and 10 attempts. Binance's
    /// module docs and Alpaca's `account_stream` rustdoc state these numbers.
    pub(crate) const ACCOUNT_STREAM: Self = Self {
        initial: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        max_attempts: 10,
    };
}

/// How many reconnects in a row an account stream may still fail, and the wait before each.
pub(crate) struct ReconnectBudget {
    /// The venue named in the log line of each wait.
    venue: &'static str,
    limits: BackoffLimits,
    attempt: u32,
}

impl ReconnectBudget {
    pub(crate) fn new(venue: &'static str, limits: BackoffLimits) -> Self {
        Self {
            venue,
            limits,
            attempt: 0,
        }
    }

    /// Restore the whole budget, once a connection has proved live.
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Number of reconnect attempts consumed so far.
    ///
    /// After [`wait`](Self::wait) has returned `false` this equals `max_attempts`, the number of
    /// attempts made before the budget was exhausted. Used to populate
    /// [`crate::error::StreamTerminationReason::ReconnectBudgetExhausted`].
    pub(crate) fn attempts(&self) -> u32 {
        self.attempt
    }

    /// Spend one attempt and return the wait before it, or `None`, spending nothing, once every
    /// attempt is spent.
    fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.limits.max_attempts {
            return None;
        }
        let delay = self
            .limits
            .initial
            .saturating_mul(2u32.saturating_pow(self.attempt))
            .min(self.limits.max_delay);
        self.attempt += 1;
        Some(delay)
    }

    /// Wait for the current backoff duration. Returns `false` if max attempts exhausted.
    pub(crate) async fn wait(&mut self) -> bool {
        let Some(delay) = self.next_delay() else {
            return false;
        };
        debug!(
            attempt = self.attempt,
            max = self.limits.max_attempts,
            // A u64 field, as before: tracing records a u128 through `Debug`.
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "{} reconnect backoff",
            self.venue
        );
        tokio::time::sleep(delay).await;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    /// The delays left in `budget`, capped so that a budget which never runs out fails the
    /// test's assertion instead of exhausting memory.
    fn delays(budget: &mut ReconnectBudget) -> Vec<Duration> {
        std::iter::from_fn(|| budget.next_delay())
            .take(100)
            .collect()
    }

    /// Binance's module docs and Alpaca's `account_stream` rustdoc state the budget as numbers.
    /// Change these only together with those docs.
    #[test]
    fn the_account_stream_budget_is_as_documented() {
        assert_eq!(
            BackoffLimits::ACCOUNT_STREAM,
            BackoffLimits {
                initial: Duration::from_secs(1),
                max_delay: Duration::from_secs(30),
                max_attempts: 10,
            }
        );
    }

    /// The account streams wait 1, 2, 4, 8 and 16 s, then 30 s for each attempt left.
    #[test]
    fn the_account_stream_waits_double_up_to_the_cap() {
        let mut budget = ReconnectBudget::new("Test", BackoffLimits::ACCOUNT_STREAM);
        let secs = [1, 2, 4, 8, 16, 30, 30, 30, 30, 30].map(Duration::from_secs);

        assert_eq!(delays(&mut budget), secs);
        assert_eq!(budget.attempts(), 10);
    }

    /// The waits follow the limits passed in, not the account streams' numbers.
    #[test]
    fn the_waits_follow_the_limits_given() {
        let limits = BackoffLimits {
            initial: ms(3),
            max_delay: ms(10),
            max_attempts: 4,
        };
        let mut budget = ReconnectBudget::new("Test", limits);

        assert_eq!(delays(&mut budget), [ms(3), ms(6), ms(10), ms(10)]);
    }

    /// A spent budget spends nothing more, and a reset restores all of it from the first wait.
    #[test]
    fn a_spent_budget_stays_spent_until_reset() {
        let limits = BackoffLimits {
            initial: ms(1),
            max_delay: ms(4),
            max_attempts: 2,
        };
        let mut budget = ReconnectBudget::new("Test", limits);
        assert_eq!(delays(&mut budget), [ms(1), ms(2)]);

        assert_eq!(budget.next_delay(), None);
        assert_eq!(budget.attempts(), 2);

        budget.reset();
        assert_eq!(budget.attempts(), 0);
        assert_eq!(delays(&mut budget), [ms(1), ms(2)]);
    }

    /// `wait` sleeps each delay in turn and, once the budget is spent, returns `false` at once.
    #[tokio::test(start_paused = true)]
    async fn wait_sleeps_each_delay_and_ends_when_spent() {
        let limits = BackoffLimits {
            initial: ms(1),
            max_delay: ms(4),
            max_attempts: 2,
        };
        let mut budget = ReconnectBudget::new("Test", limits);
        let started = tokio::time::Instant::now();

        assert!(budget.wait().await);
        assert_eq!(started.elapsed(), ms(1));
        assert!(budget.wait().await);
        assert_eq!(started.elapsed(), ms(3));
        assert!(!budget.wait().await);
        assert_eq!(started.elapsed(), ms(3));
        assert_eq!(budget.attempts(), 2);
    }
}
