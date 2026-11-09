use crate::engine::state::instrument::filter::InstrumentFilter;
use rustrade_execution::order::request::{
    CancelThenOpen, OrderAmend, OrderRequestCancel, OrderRequestOpen,
};
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};
use rustrade_integration::collection::one_or_many::OneOrMany;
use serde::{Deserialize, Serialize};

/// Trading related commands for the [`Engine`](super::Engine) to action, sent from an
/// external process.
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum Command<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    SendCancelRequests(OneOrMany<OrderRequestCancel<ExchangeKey, InstrumentKey>>),
    SendOpenRequests(OneOrMany<OrderRequestOpen<ExchangeKey, InstrumentKey>>),
    /// Send [`CancelThenOpen`] groups: each group's opens only once its cancels have been
    /// answered.
    ///
    /// Like the other commands, these do not pass through the
    /// [`RiskManager`](crate::risk::RiskManager). The `Engine` refuses a group whole when it is
    /// not valid, when a leg names an instrument it does not track, or when an open names a
    /// client order id already in use, and reports it in
    /// [`ActionOutput::CancelThenOpens`](super::action::ActionOutput::CancelThenOpens).
    SendCancelThenOpenRequests(OneOrMany<CancelThenOpen<ExchangeKey, InstrumentKey>>),
    /// Amend working orders in place: each names an order by its key and says what to change.
    ///
    /// Like the other commands, these do not pass through the
    /// [`RiskManager`](crate::risk::RiskManager). The `Engine` builds each from the order it
    /// tracks, refusing one it cannot send (see
    /// [`PreparesAmend`](crate::engine::state::PreparesAmend)), and reports them in
    /// [`ActionOutput::Amends`](super::action::ActionOutput::Amends).
    SendAmendRequests(OneOrMany<OrderAmend<ExchangeKey, InstrumentKey>>),
    ClosePositions(InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>),
    CancelOrders(InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>),
}
