use rustrade_execution::{
    client::{OrderStatusClient, VenueOrderStatus},
    error::{ConnectivityError, UnindexedClientError},
    order::{Order, UnindexedInactiveOrder, UnindexedOrderKey, state::Open},
};
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, warn};

/// How an [`ExecutionManager`](super::manager::ExecutionManager) settles an open request whose
/// outcome is unknown, by looking the order up at the venue. [`Default`] is the one in
/// [`AccountStreamOptions::default`](super::manager::AccountStreamOptions), which
/// [`ExecutionBuilder::add_live`](super::builder::ExecutionBuilder::add_live) uses.
///
/// # Why a lookup
/// An open request that times out, or fails with a connectivity error, may still have reached
/// the venue (see
/// [`OrderError::is_outcome_unknown`](rustrade_execution::error::OrderError::is_outcome_unknown)).
/// The engine keeps such an order in flight rather than guess. The manager then asks the venue
/// about it by its client order id, with
/// [`OrderStatusClient::fetch_order_status`], waiting [`first_delay`](Self::first_delay) before the
/// first lookup and twice as long before each later one. The first answer that finds the order
/// settles it, live or ended.
///
/// # Concluding the order was never placed
/// A venue may take a moment to list an order it has just accepted, so one
/// [`VenueOrderStatus::Unknown`] proves nothing. The manager reports
/// [`NotPlacedReason::UnknownToVenue`](rustrade_execution::error::NotPlacedReason::UnknownToVenue)
/// only after [`unknowns_to_conclude`](Self::unknowns_to_conclude) `Unknown` answers in a row,
/// each received at least [`grace`](Self::grace) after the open request was handed to the client.
/// A failed lookup is not an answer: it never counts, and it breaks the run. An `Unknown` within
/// the grace period neither counts nor breaks the run.
///
/// So the conclusion is reachable only when the lookups that end after the grace period number at
/// least `unknowns_to_conclude`. A policy with fewer, as with `unknowns_to_conclude` above
/// `attempts`, never concludes it: every order it does not find stays in flight.
///
/// # When the lookups run out
/// After [`attempts`](Self::attempts) lookups with no conclusion, the manager reports nothing more
/// and logs a warning. The order stays in flight until the venue reports it, and the engine's
/// in-flight deadline flags it with
/// [`InFlightOverdue`](crate::engine::in_flight::InFlightOverdue). What to do then is the
/// caller's decision. `attempts` of zero turns the lookups off.
///
/// # What the lookups cost
/// Each lookup is one [`fetch_order_status`](OrderStatusClient::fetch_order_status), which by
/// default is two reads at the venue: its ended orders, then its open orders on the instrument.
/// They count against the venue's rate limit like any other request, and each order whose outcome
/// is unknown has its own lookups, so an outage that leaves many opens unanswered multiplies them.
/// A lookup the rate limit holds back past its timeout counts as failed, never as `Unknown`.
/// Where that cost matters, lower `attempts`, or have the client override `fetch_order_status`
/// with a cheaper read.
///
/// Built from [`Default`] and the `with_*` methods, so fields can be added without a breaking
/// change. Venues differ in how late an order they accepted can show up, so the policy is set per
/// venue. Each [`Duration`] is serialised as `secs` and `nanos`; a missing field takes its default.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(default)]
pub struct UnknownOutcomeLookup {
    /// How many lookups to make at most.
    pub attempts: u32,
    /// The wait before the first lookup. Each later wait doubles the one before it.
    pub first_delay: Duration,
    /// How many `Unknown` answers in a row conclude that the order was never placed. Zero is
    /// treated as one. Above [`attempts`](Self::attempts), the conclusion is never reached.
    pub unknowns_to_conclude: u32,
    /// How long after the open request was handed to the client an `Unknown` answer starts to
    /// count. One longer than the lookups last leaves the conclusion out of reach.
    pub grace: Duration,
}

impl Default for UnknownOutcomeLookup {
    /// Five lookups, 1, 2, 4, 8 and 16 seconds apart, the first one second after the outcome
    /// came back unknown; three `Unknown` answers in a row, from five seconds after the open
    /// request was handed to the client, conclude the order was never placed.
    fn default() -> Self {
        Self {
            attempts: 5,
            first_delay: Duration::from_secs(1),
            unknowns_to_conclude: 3,
            grace: Duration::from_secs(5),
        }
    }
}

impl UnknownOutcomeLookup {
    /// Sets [`attempts`](Self::attempts).
    #[must_use]
    pub const fn with_attempts(self, attempts: u32) -> Self {
        Self { attempts, ..self }
    }

    /// Sets [`first_delay`](Self::first_delay).
    #[must_use]
    pub const fn with_first_delay(self, first_delay: Duration) -> Self {
        Self {
            first_delay,
            ..self
        }
    }

    /// Sets [`unknowns_to_conclude`](Self::unknowns_to_conclude).
    #[must_use]
    pub const fn with_unknowns_to_conclude(self, unknowns_to_conclude: u32) -> Self {
        Self {
            unknowns_to_conclude,
            ..self
        }
    }

    /// Sets [`grace`](Self::grace).
    #[must_use]
    pub const fn with_grace(self, grace: Duration) -> Self {
        Self { grace, ..self }
    }

    /// The longest the lookups can take in all, from the moment the outcome came back unknown,
    /// when each lookup is given up after `lookup_timeout`: the waits before each lookup, plus
    /// `lookup_timeout` for each.
    ///
    /// An [`ExecutionManager`](super::manager::ExecutionManager) gives each lookup its
    /// `request_timeout`.
    pub fn longest(&self, lookup_timeout: Duration) -> Duration {
        // From lookup `u32::BITS` on, the wait stops doubling, so those lookups are summed at once
        // and a large `attempts` costs no more than a small one.
        let doubling = self.attempts.min(u32::BITS);
        let doubled = (0..doubling)
            .map(|attempt| self.delay(attempt).saturating_add(lookup_timeout))
            .fold(Duration::ZERO, Duration::saturating_add);
        let level = self
            .delay(u32::BITS)
            .saturating_add(lookup_timeout)
            .saturating_mul(self.attempts - doubling);
        doubled.saturating_add(level)
    }

    /// The wait before lookup number `attempt`, counting from zero: `first_delay` doubled
    /// `attempt` times, the factor capped at `u32::MAX`.
    fn delay(&self, attempt: u32) -> Duration {
        let factor = 2_u32.checked_pow(attempt).unwrap_or(u32::MAX);
        self.first_delay.saturating_mul(factor)
    }
}

/// What the lookups of an order found.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum LookupOutcome {
    /// The order is live at the venue, in this state.
    Live(Box<Order<ExchangeId, InstrumentNameExchange, Open>>),
    /// The order has ended, as this says.
    Ended(Box<UnindexedInactiveOrder>),
    /// The venue answered `Unknown` as many times in a row as the policy asks.
    NeverPlaced,
    /// The lookups ran out without a conclusion.
    Exhausted,
}

/// Look the order under `key`, handed to the client at `sent`, up at the venue as `policy` says,
/// until it is found
/// or concluded never placed, or the lookups run out. A lookup still unanswered after
/// `lookup_timeout` counts as failed.
pub(crate) async fn look_up_unknown_outcome<Client>(
    client: &Client,
    key: &UnindexedOrderKey,
    sent: Instant,
    policy: UnknownOutcomeLookup,
    lookup_timeout: Duration,
) -> LookupOutcome
where
    Client: OrderStatusClient + Sync,
{
    let unknowns_to_conclude = policy.unknowns_to_conclude.max(1);
    let mut unknowns = 0;
    for attempt in 0..policy.attempts {
        tokio::time::sleep(policy.delay(attempt)).await;
        let status = tokio::time::timeout(lookup_timeout, client.fetch_order_status(key))
            .await
            .unwrap_or(Err(UnindexedClientError::Connectivity(
                ConnectivityError::Timeout,
            )));
        match status {
            Ok(VenueOrderStatus::Unknown) if sent.elapsed() >= policy.grace => {
                unknowns += 1;
                debug!(
                    instrument = %key.instrument,
                    cid = %key.cid,
                    unknowns,
                    "ExecutionManager looked up an order whose open outcome is unknown: the venue does not know it"
                );
                if unknowns >= unknowns_to_conclude {
                    return LookupOutcome::NeverPlaced;
                }
            }
            // Too soon after the send to count: the venue may not list an order it just accepted.
            Ok(VenueOrderStatus::Unknown) => {}
            Ok(VenueOrderStatus::Live(order)) => return LookupOutcome::Live(order),
            Ok(VenueOrderStatus::Ended(order)) => return LookupOutcome::Ended(order),
            Err(error) => {
                warn!(
                    instrument = %key.instrument,
                    cid = %key.cid,
                    ?error,
                    "ExecutionManager could not look up an order whose open outcome is unknown"
                );
                unknowns = 0;
            }
        }
    }
    LookupOutcome::Exhausted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_setter_sets_its_field_alone() {
        let set = UnknownOutcomeLookup::default()
            .with_attempts(7)
            .with_first_delay(Duration::from_millis(250))
            .with_unknowns_to_conclude(4)
            .with_grace(Duration::from_secs(9));
        assert_eq!(
            set,
            UnknownOutcomeLookup {
                attempts: 7,
                first_delay: Duration::from_millis(250),
                unknowns_to_conclude: 4,
                grace: Duration::from_secs(9),
            }
        );
    }

    #[test]
    fn longest_sums_the_doubling_waits_and_a_timeout_per_lookup() {
        let policy = UnknownOutcomeLookup::default()
            .with_attempts(3)
            .with_first_delay(Duration::from_millis(500));
        // Waits of 0.5, 1 and 2 s, each lookup given 3 s.
        assert_eq!(
            policy.longest(Duration::from_secs(3)),
            Duration::from_millis(500 + 1_000 + 2_000 + 3 * 3_000)
        );
        assert_eq!(
            policy.with_attempts(0).longest(Duration::from_secs(3)),
            Duration::ZERO
        );
    }

    /// Past the doubling, `longest` still sums every lookup, and does so without counting them
    /// one by one.
    #[test]
    fn longest_sums_the_lookups_past_the_doubling() {
        let policy = UnknownOutcomeLookup::default().with_first_delay(Duration::from_nanos(1));
        let timeout = Duration::from_nanos(7);
        for attempts in [31, 32, 33, 40] {
            let one_by_one = (0..attempts)
                .map(|attempt| policy.delay(attempt) + timeout)
                .sum::<Duration>();
            assert_eq!(
                policy.with_attempts(attempts).longest(timeout),
                one_by_one,
                "{attempts} attempts"
            );
        }
        assert_eq!(
            UnknownOutcomeLookup::default()
                .with_attempts(u32::MAX)
                .with_first_delay(Duration::from_secs(2))
                .longest(Duration::from_secs(1)),
            Duration::MAX,
            "saturates rather than overflowing"
        );
    }
}
