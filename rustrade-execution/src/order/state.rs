use crate::{error::OrderError, order::id::OrderId};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};

/// Convenient type alias for an [`OrderState`] keyed with [`AssetNameExchange`]
/// and [`InstrumentNameExchange`].
pub type UnindexedOrderState = OrderState<AssetNameExchange, InstrumentNameExchange>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum OrderState<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    Active(ActiveOrderState),
    Inactive(InactiveOrderState<AssetKey, InstrumentKey>),
}

impl<AssetKey, InstrumentKey> OrderState<AssetKey, InstrumentKey> {
    pub fn active<S>(state: S) -> Self
    where
        S: Into<ActiveOrderState>,
    {
        OrderState::Active(state.into())
    }

    pub fn inactive<S>(state: S) -> Self
    where
        S: Into<InactiveOrderState<AssetKey, InstrumentKey>>,
    {
        OrderState::Inactive(state.into())
    }

    pub fn fully_filled(filled: Filled) -> Self {
        Self::Inactive(InactiveOrderState::FullyFilled(filled))
    }

    pub fn expired(expired: Expired) -> Self {
        Self::Inactive(InactiveOrderState::Expired(expired))
    }

    pub fn time_exchange(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Active(active) => match active {
                ActiveOrderState::OpenInFlight(_) => None,
                ActiveOrderState::Open(state) => Some(state.time_exchange),
                ActiveOrderState::CancelInFlight(state) => {
                    state.order.as_ref().map(|order| order.time_exchange)
                }
            },
            Self::Inactive(inactive) => match inactive {
                InactiveOrderState::Cancelled(state) => Some(state.time_exchange),
                InactiveOrderState::FullyFilled(state) => Some(state.time_exchange),
                InactiveOrderState::Expired(state) => Some(state.time_exchange),
                InactiveOrderState::OpenFailed(_) => None,
            },
        }
    }

    /// Returns `true` if the order was not rejected at placement.
    ///
    /// Returns `true` for all states except `Inactive(OpenFailed(_))`:
    /// - `Active(_)` — order is working on the exchange
    /// - `Inactive(FullyFilled(_))` — order completed successfully
    /// - `Inactive(Cancelled(_))` — order was accepted then cancelled
    /// - `Inactive(Expired(_))` — order was accepted then expired
    ///
    /// This is the opposite of [`is_failed()`](Self::is_failed).
    pub fn is_accepted(&self) -> bool {
        !self.is_failed()
    }

    /// Returns `true` if the order failed to open.
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Inactive(InactiveOrderState::OpenFailed(_)))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum ActiveOrderState {
    OpenInFlight(OpenInFlight),
    Open(Open),
    CancelInFlight(CancelInFlight),
}

impl ActiveOrderState {
    pub fn open_meta(&self) -> Option<&Open> {
        match self {
            Self::OpenInFlight(_) => None,
            Self::Open(open) => Some(open),
            Self::CancelInFlight(cancel) => cancel.order.as_ref(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct OpenInFlight;

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Open {
    pub id: OrderId,
    pub time_exchange: DateTime<Utc>,
    pub filled_quantity: Decimal,
}

impl Open {
    pub fn quantity_remaining(&self, initial_quantity: Decimal) -> Decimal {
        initial_quantity - self.filled_quantity
    }
}

/// Metadata for a fully filled order.
///
/// Unlike [`Open`], this represents an order that has completed execution
/// and is no longer active on the exchange.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Filled {
    pub id: OrderId,
    pub time_exchange: DateTime<Utc>,
    pub filled_quantity: Decimal,
    /// Volume-weighted average execution price across all fills.
    ///
    /// `Some` when the exchange provides it in the response, `None` otherwise.
    /// When `None`, downstream consumers should compute from individual fill events.
    pub avg_price: Option<Decimal>,
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct CancelInFlight {
    pub order: Option<Open>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub enum InactiveOrderState<AssetKey, InstrumentKey> {
    Cancelled(Cancelled),
    FullyFilled(Filled),
    OpenFailed(OrderError<AssetKey, InstrumentKey>),
    Expired(Expired),
}

/// Metadata for a cancelled order.
///
/// Includes `filled_quantity` to handle IOC (Immediate-Or-Cancel) orders
/// that partially fill before the remainder is cancelled.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Cancelled {
    pub id: OrderId,
    /// Cancellation timestamp.
    ///
    /// Normally the venue-reported cancel time. Some venues omit a timestamp on their cancel
    /// response (e.g. Binance margin cancels carry no `transactTime`); for those the client falls
    /// back to the local receive time, which can differ from the true venue cancel time by network
    /// latency. Consumers building fill ledgers or P&L should not assume sub-second venue accuracy.
    pub time_exchange: DateTime<Utc>,
    /// Quantity filled before the order was cancelled.
    ///
    /// Zero for orders cancelled with no fills (e.g., GTC limit order cancelled by user).
    /// Non-zero for IOC orders that partially filled before cancellation.
    pub filled_quantity: Decimal,
}

/// Metadata for an expired order.
///
/// Includes `filled_quantity` to handle GTD (Good-Till-Date) orders
/// that partially fill before expiration.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Expired {
    pub id: OrderId,
    pub time_exchange: DateTime<Utc>,
    /// Quantity filled before the order expired.
    pub filled_quantity: Decimal,
}
