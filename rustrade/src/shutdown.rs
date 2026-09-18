use serde::{Deserialize, Serialize};

pub trait SyncShutdown {
    type Result;
    fn shutdown(&mut self) -> Self::Result;
}

pub trait AsyncShutdown {
    type Result;
    fn shutdown(&mut self) -> impl Future<Output = Self::Result>;
}

/// Instruction for the `Engine` to stop.
///
/// The two variants differ only in what happens to execution requests that are still in flight
/// when the instruction is processed — an order the `Engine` has sent to the exchange but has not
/// yet had a response for.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub enum Shutdown {
    /// Terminate as soon as this is processed, abandoning any execution request still in flight.
    ///
    /// Whatever those requests would have reported — a fill, a rejection, a balance update — never
    /// reaches the `Engine`. That is usually what live trading wants: stopping should not wait on a
    /// venue that may be slow or unreachable.
    #[default]
    Immediate,

    /// Stop generating new orders, then terminate once the execution side has finished.
    ///
    /// On receipt the `Engine` stops calling the strategy for new orders and sends every
    /// `ExecutionManager` an [`ExecutionRequest::Drain`]. It keeps processing account events until
    /// its feed ends, which happens when the last manager has finished and closed its channel.
    ///
    /// This is what a backtest needs, and [`System::shutdown_after_backtest`] sends it. A market
    /// stream ending says only that there is no more *input*; the responses to the orders that
    /// input provoked are still on their way, and terminating on the stream's end alone discards
    /// every one of them.
    ///
    /// # Why the `Engine` does not decide this itself
    /// Its only local signal is request quiescence — no order left in
    /// [`ActiveOrderState::OpenInFlight`] or [`ActiveOrderState::CancelInFlight`]. That signal
    /// fires too early. A fill is delivered as three separate things: the balance it debits, the
    /// `Trade` it consists of, and the response reporting it filled. Only the response clears the
    /// request from flight, so stopping there cut the run off with the other two still unread, and
    /// truncated the balance and position ledgers by *different*, run-dependent amounts. The
    /// managers are the only component that can see both, so they own the decision.
    ///
    /// # Termination
    /// Bounded by the `request_timeout` each `ExecutionManager` applies: every request that was
    /// successfully sent resolves into either a response or a timeout, so the drain always ends.
    /// Orders that are merely *resting* at the venue ([`ActiveOrderState::Open`]) are not in
    /// flight and do not hold it up. [`System::shutdown_after_backtest`] applies its own deadline
    /// on top, so misusing it against a live system fails rather than hangs.
    ///
    /// [`ActiveOrderState::OpenInFlight`]: rustrade_execution::order::state::ActiveOrderState::OpenInFlight
    /// [`ActiveOrderState::CancelInFlight`]: rustrade_execution::order::state::ActiveOrderState::CancelInFlight
    /// [`ActiveOrderState::Open`]: rustrade_execution::order::state::ActiveOrderState::Open
    /// [`ExecutionRequest::Drain`]: crate::execution::request::ExecutionRequest::Drain
    /// [`System::shutdown_after_backtest`]: crate::system::System::shutdown_after_backtest
    AfterDrain,
}
