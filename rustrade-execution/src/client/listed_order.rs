//! An open order read from a venue's order listing, with the terms the listing states.

use crate::order::{
    Order,
    report::{OrderReport, Replaces, ReportedTerms, UnindexedOrderReport},
    state::{Open, OrderState},
};
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};

/// An open order from a venue's order listing, with the terms the listing states for it (see
/// [`ReportedTerms::from_venue_read`]), and the order it replaces, when the listing names one.
#[derive(Debug)]
pub(crate) struct ListedOrder {
    pub(crate) order: Order<ExchangeId, InstrumentNameExchange, Open>,
    pub(crate) reported: ReportedTerms,
    /// The order this one replaces, on the venue's word in the listing (see
    /// [`OrderReport::replaces`]).
    pub(crate) replaces: Option<Replaces>,
}

impl ListedOrder {
    /// The order as an account snapshot reports it.
    pub(crate) fn into_report(self) -> UnindexedOrderReport {
        let report = OrderReport::new(self.order.map_state(OrderState::active), self.reported);
        match self.replaces {
            Some(replaces) => report.with_replaces(replaces),
            None => report,
        }
    }
}
