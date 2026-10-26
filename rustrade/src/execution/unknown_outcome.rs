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
/// each received at least [`grace`](Self::grace) after the open was sent. A failed lookup is not an
/// answer: it never counts, and it breaks the run.
///
/// # When the lookups run out
/// After [`attempts`](Self::attempts) lookups with no conclusion, the manager reports nothing more
/// and logs a warning. The order stays in flight until the venue reports it, and the engine's
/// in-flight deadline flags it with
/// [`InFlightOverdue`](crate::engine::in_flight::InFlightOverdue). What to do then is the
/// caller's decision. `attempts` of zero turns the lookups off.
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
    /// treated as one.
    pub unknowns_to_conclude: u32,
    /// How long after the open was sent an `Unknown` answer starts to count.
    pub grace: Duration,
}

impl Default for UnknownOutcomeLookup {
    /// Five lookups, 1, 2, 4, 8 and 16 seconds apart, the first one second after the outcome
    /// came back unknown; three `Unknown` answers in a row, from five seconds after the send,
    /// conclude the order was never placed.
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
        (0..self.attempts)
            .map(|attempt| self.delay(attempt).saturating_add(lookup_timeout))
            .fold(Duration::ZERO, Duration::saturating_add)
    }

    /// The wait before lookup number `attempt`, counting from zero.
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

/// Look the order under `key`, sent at `sent`, up at the venue as `policy` says, until it is found
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
