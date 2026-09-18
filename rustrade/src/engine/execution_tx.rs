use crate::{engine::error::UnrecoverableEngineError, execution::request::ExecutionRequest};
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    index::error::IndexError,
    instrument::InstrumentIndex,
};
use rustrade_integration::{
    channel::{Tx, UnboundedTx},
    collection::FnvIndexMap,
};
use std::fmt::Debug;

/// Collection of [`ExecutionRequest`] [`Tx`]s for each
/// exchange [`ExecutionManager`](crate::execution::manager::ExecutionManager).
///
/// Facilitates the routing of execution requests in a multi or single exchange trading system.
pub trait ExecutionTxMap<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    type ExecutionTx: Tx<Item = ExecutionRequest<ExchangeKey, InstrumentKey>>;

    /// Attempt to find the [`ExecutionRequest`] [`Tx`] for the provided `ExchangeKey`.
    fn find(&self, exchange: &ExchangeKey) -> Result<&Self::ExecutionTx, UnrecoverableEngineError>;

    /// Returns an `Iterator` of all active [`ExecutionRequest`] [`Tx`]s.
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Self::ExecutionTx>
    where
        Self::ExecutionTx: 'a;
}

/// A map of exchange transmitters that efficiently routes execution requests to exchange-specific
/// transmitter channels.
///
/// `FnvIndexMap` of [`ExecutionRequest`] [`Tx`]s for each exchange.
///
/// Facilitates the routing of execution requests in a multi exchange trading system.
///
/// Note that a transmitter for an exchange is optional. This handles the case where instruments
/// for an exchange may be tracked by the trading system, but not trading on.
///
/// **Without this optional transmitter the [`ExchangeIndex`]s would not be valid.**.
#[derive(Debug)]
pub struct MultiExchangeTxMap<Tx = UnboundedTx<ExecutionRequest>>(
    FnvIndexMap<ExchangeId, Option<Tx>>,
);

impl<Tx> MultiExchangeTxMap<Tx> {
    /// Venues that actually have a registered execution client.
    ///
    /// A venue is present in this map but holds `None` when the system tracks instruments for it
    /// without trading on them — see the type-level note above on why the slot must still exist.
    /// Having a transmitter is therefore what decides whether the engine will ever receive an
    /// `AccountStream` event from that venue, which is precisely the account dimension
    /// [`VenueRole`](crate::engine::state::connectivity::VenueRole) is derived from.
    ///
    /// Borrows `self`, so collect the result before moving the map into an
    /// [`Engine`](crate::engine::Engine).
    pub(crate) fn execution_venues(&self) -> impl Iterator<Item = ExchangeId> + '_ {
        self.0
            .iter()
            .filter_map(|(exchange, tx)| tx.as_ref().map(|_| *exchange))
    }
}

impl<Tx> FromIterator<(ExchangeId, Option<Tx>)> for MultiExchangeTxMap<Tx> {
    fn from_iter<Iter>(iter: Iter) -> Self
    where
        Iter: IntoIterator<Item = (ExchangeId, Option<Tx>)>,
    {
        MultiExchangeTxMap(FnvIndexMap::from_iter(iter))
    }
}

impl<'a, Tx> IntoIterator for &'a MultiExchangeTxMap<Tx> {
    type Item = (&'a ExchangeId, &'a Option<Tx>);
    type IntoIter = indexmap::map::Iter<'a, ExchangeId, Option<Tx>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, Tx> IntoIterator for &'a mut MultiExchangeTxMap<Tx> {
    type Item = (&'a ExchangeId, &'a mut Option<Tx>);
    type IntoIter = indexmap::map::IterMut<'a, ExchangeId, Option<Tx>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<Transmitter> ExecutionTxMap<ExchangeIndex, InstrumentIndex> for MultiExchangeTxMap<Transmitter>
where
    Transmitter: Tx<Item = ExecutionRequest> + Debug,
{
    type ExecutionTx = Transmitter;

    fn find(
        &self,
        exchange: &ExchangeIndex,
    ) -> Result<&Self::ExecutionTx, UnrecoverableEngineError> {
        self.0
            .get_index(exchange.index())
            .and_then(|(_exchange, tx)| tx.as_ref())
            .ok_or_else(|| {
                UnrecoverableEngineError::IndexError(IndexError::ExchangeIndex(format!(
                    "failed to find ExecutionTx for ExchangeIndex: {exchange}. Available: {self:?}"
                )))
            })
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Self::ExecutionTx>
    where
        Self::ExecutionTx: 'a,
    {
        self.0.values().filter_map(|tx| tx.as_ref())
    }
}
