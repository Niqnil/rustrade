//! Cash that moves into or out of an account other than by a trade: perpetual funding, margin
//! interest, stock borrow fees and rebates.
//!
//! An account stream reports each such movement the venue posts as
//! [`AccountEventKind::CashFlow`](crate::AccountEventKind::CashFlow), carrying a [`CashFlow`]. The
//! library delivers the venue's own figures; rates, day counts and what to do with the flows stay
//! the consumer's.

use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt::{Display, Formatter};

/// One movement of cash into or out of an account that is not a trade, as the venue posted it.
///
/// Sent as [`AccountEventKind::CashFlow`](crate::AccountEventKind::CashFlow). A flow is a delta:
/// apply each one once. The engine adds a flow attributed to an instrument to the `carry` of that
/// instrument's open position (see `Position::carry` in `rustrade`), and applies none to
/// balances. A venue's balances already include the flows it has posted, so adding a flow to a
/// balance read after it counts the flow twice.
///
/// # Delivery
///
/// Each producer says how it avoids sending one flow twice, and what identifies a flow when the
/// venue gives it no [`id`](Self::id). Known producers, as of writing:
/// - Hyperliquid perpetuals: funding, as [`CashFlowKind::Funding`]. See the client's rustdoc.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
pub struct CashFlow<AssetKey, InstrumentKey> {
    /// What the flow is for.
    pub kind: CashFlowKind,
    /// The asset the cash moved in.
    pub asset: AssetKey,
    /// How much moved, signed: positive when the account received it, negative when it paid.
    pub amount: Decimal,
    /// The instrument whose position caused the flow, when the venue attributes it to one, as it
    /// does funding. `None` for a flow on the account as a whole.
    pub instrument: Option<InstrumentKey>,
    /// When the venue posted the flow.
    pub time_exchange: DateTime<Utc>,
    /// The venue's identifier for the flow, when it gives one. See [`CashFlowId`].
    pub id: Option<CashFlowId>,
}

/// What a [`CashFlow`] is for.
///
/// Each variant carries the venue's own figures where it reports them, and `None` where it does
/// not.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum CashFlowKind {
    /// A perpetual's funding payment, paid or received on a position held at a funding time.
    Funding {
        /// The funding rate the payment was charged at, per funding interval, in the venue's
        /// sign convention.
        rate: Option<Decimal>,
        /// The signed position the payment was charged on: positive long, negative short.
        position_quantity: Option<Decimal>,
    },
    /// A fee for borrowing a security to sell it short.
    BorrowFee,
    /// Interest on borrowed cash or assets.
    MarginInterest {
        /// The interest rate charged, per the venue's own period.
        rate: Option<Decimal>,
        /// The amount borrowed that the interest was charged on.
        principal: Option<Decimal>,
    },
    /// A rebate paid to the account, such as on cash collateral for a short sale.
    Rebate,
    /// A flow of a kind no other variant names, with the venue's own name for it.
    Other(String),
}

/// A venue's own identifier for one [`CashFlow`], carried through opaquely.
///
/// Like [`TradeId`](crate::trade::TradeId), its uniqueness is the venue's to define and may be
/// scoped to an asset or instrument rather than global. A consumer that needs a key should
/// qualify it by exchange and asset or instrument. `Ord` is byte order, not time order.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub struct CashFlowId(pub SmolStr);

impl Display for CashFlowId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl CashFlowId {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        Self(SmolStr::new(id))
    }
}
