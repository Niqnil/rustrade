//! An open order read from a venue's order listing, with the terms the listing states.

use crate::order::{
    Order,
    report::{OrderReport, ReportedTerms, UnindexedOrderReport},
    state::{Open, OrderState},
};
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};

/// An open order from a venue's order listing, with the terms the listing states for it (see
/// [`ReportedTerms::from_venue_read`]).
#[derive(Debug)]
pub(crate) struct ListedOrder {
    pub(crate) order: Order<ExchangeId, InstrumentNameExchange, Open>,
    pub(crate) reported: ReportedTerms,
}

impl ListedOrder {
    /// The order as an account snapshot reports it.
    pub(crate) fn into_report(self) -> UnindexedOrderReport {
        OrderReport::new(self.order.map_state(OrderState::active), self.reported)
    }
}
