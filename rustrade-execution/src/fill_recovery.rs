//! What an account stream reports when its fill recovery gives up on a span of fills.
//!
//! Venues that reconnect internally (Binance Spot and Margin, Alpaca) read the fills they missed
//! while disconnected by REST after each reconnect. When that read fails for good, the fills in
//! its span never reach the consumer, and the stream says so in-band with
//! [`AccountEventKind::FillRecoveryGaveUp`](crate::AccountEventKind::FillRecoveryGaveUp), carrying
//! a [`FillRecoveryGap`].

use chrono::{DateTime, Utc};
use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A span of fills an account stream's recovery gave up on: fills in it may not have been
/// delivered.
///
/// Sent as [`AccountEventKind::FillRecoveryGaveUp`](crate::AccountEventKind::FillRecoveryGaveUp),
/// once per failed read. The library does not act on it beyond reporting it. What to do is the
/// consumer's policy: reconcile, alert, or halt.
///
/// # Reconciling
///
/// Read the span with
/// [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades), passing
/// `start` and [`FillRecoveryScope::instrument_filter`]:
/// - The span can overlap fills that were delivered, before the read failed or live around the
///   reconnect, so match what it returns against the fills already seen by
///   [`TradeId`](crate::trade::TradeId): a fill has one `TradeId` whether the stream or
///   `fetch_trades` delivers it.
/// - Fills after `end` arrived live, so a read that runs past `end`, as `fetch_trades` does,
///   returns them again.
/// - A span can hold more fills than `fetch_trades` can return. Alpaca's `fetch_trades` reads
///   every fill from `start` to now, with no end bound. When there are more than 5,000, it returns
///   [`Truncated`](crate::error::ClientError::Truncated) with none of them, so a span with more
///   fills than that after its `start` cannot be read with it. This is a known limitation:
///   reconcile such a span another way, for example from the venue's own account activity.
///
/// # Delivery
///
/// The event is sent on the account stream like any other, so it is lost if the consumer has
/// dropped the stream. On a [`Truncated`](FillRecoveryFailure::Truncated) read, the fills that
/// were read are sent first and the event after them.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
pub struct FillRecoveryGap<InstrumentKey> {
    /// Which instruments' fills the span covers.
    pub scope: FillRecoveryScope<InstrumentKey>,
    /// The first moment of the span, inclusive.
    pub start: DateTime<Utc>,
    /// The last moment of the span, inclusive.
    pub end: DateTime<Utc>,
    /// How many times the span was read before it was given up: 1 when the venue does not retry
    /// it. Retries within one read, such as after a rate limit, are not counted.
    pub attempts: u32,
    /// Why the last read failed.
    pub reason: FillRecoveryFailure,
}

/// Which instruments a [`FillRecoveryGap`] covers.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum FillRecoveryScope<InstrumentKey> {
    /// These instruments. A venue never sends an empty list; note that
    /// [`instrument_filter`](Self::instrument_filter) would pass one on as every instrument.
    Instruments(Vec<InstrumentKey>),
    /// Every instrument on the account: the read covered all of them, as Alpaca's does for a
    /// stream opened without an instrument list.
    AllInstruments,
}

impl<InstrumentKey> FillRecoveryScope<InstrumentKey> {
    /// The instruments to pass to
    /// [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades), in its
    /// convention: an empty slice means every instrument.
    pub fn instrument_filter(&self) -> &[InstrumentKey] {
        match self {
            Self::Instruments(instruments) => instruments,
            Self::AllInstruments => &[],
        }
    }
}

/// Why fill recovery's last read of a [`FillRecoveryGap`] failed.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Error)]
pub enum FillRecoveryFailure {
    /// The venue request failed.
    #[error("request failed: {0}")]
    Request(String),

    /// The read did not finish within the recovery's time budget.
    #[error("timed out after {timeout_secs}s")]
    TimedOut {
        /// The time budget, in seconds.
        timeout_secs: u64,
    },

    /// The read stopped at the venue integration's cap on how many fills one read returns. The
    /// fills it read were delivered, and the span starts just before the last of them, so fills
    /// sharing its time that the read cut off are inside it.
    #[error("truncated after {fills_read} fills")]
    Truncated {
        /// How many fills the read returned before it stopped, counted across the account,
        /// before any filter by instrument.
        fills_read: usize,
    },
}
