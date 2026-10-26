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
/// balance read after it counts the flow twice. Carry a venue has accrued but not yet posted is no
/// flow: a venue that reports it puts it in
/// [`Balance::accrued`](crate::balance::Balance::accrued).
///
/// # Delivery
///
/// Each producer says how it avoids sending one flow twice, and what identifies a flow when the
/// venue gives it no [`id`](Self::id). Known producers, as of writing:
/// - Hyperliquid perpetuals: funding, as [`CashFlowKind::Funding`]. See the client's rustdoc.
/// - Alpaca: margin interest, fees and pass-through charges and rebates, read on request by
///   `AlpacaClient::fetch_cash_flows` rather than streamed, with Alpaca's activity id. The caller
///   drops what it has already read.
/// - Binance margin: interest, as [`CashFlowKind::MarginInterest`], read on request by
///   `BinanceMargin::fetch_margin_interest` rather than streamed, with Binance's transaction id.
///   The caller drops what it has already read.
/// - The simulated venue: whatever its configured
///   [`HoldingCostModel`](crate::holding_cost::HoldingCostModel)s and
///   [`MarginInterestModel`](crate::margin::MarginInterestModel) charge, each flow once, with an
///   id from its own sequence.
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
    /// does funding. `None` for a flow on the account as a whole, such as cross-margin interest.
    ///
    /// The `rustrade` engine applies a `None` flow to no position, and does not guess one. A
    /// consumer that knows which position a flow belongs to sets this before the engine sees the
    /// event. See `EngineState::update_from_account` in `rustrade` for why and how.
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
    BorrowFee {
        /// The fee rate charged, as the venue quotes it: usually annualised.
        rate: Option<Decimal>,
        /// The quantity borrowed that the fee was charged on.
        quantity: Option<Decimal>,
    },
    /// Interest on borrowed cash or assets.
    MarginInterest {
        /// The interest rate charged, as its source quotes it: per the venue's own period from a
        /// live venue (daily from Binance), annualised from the simulated venue's
        /// [`PeriodicMarginInterest`](crate::margin::PeriodicMarginInterest).
        rate: Option<Decimal>,
        /// The amount borrowed that the interest was charged on.
        principal: Option<Decimal>,
        /// When in the loan's life the charge falls, where the venue says.
        timing: Option<InterestTiming>,
    },
    /// The financing of a leveraged position held past a venue's daily cutoff, such as a CFD's
    /// overnight financing.
    ///
    /// Charged on the position rather than on a loan: a long and a short in one instrument are
    /// charged at different rates, and a short may receive rather than pay. Interest on a loan of
    /// cash or assets is [`MarginInterest`](Self::MarginInterest).
    Financing {
        /// The financing rate charged, as the venue quotes it: usually annualised.
        rate: Option<Decimal>,
        /// The notional value of the position the financing was charged on.
        notional: Option<Decimal>,
    },
    /// A rebate paid to the account, such as on cash collateral for a short sale.
    Rebate,
    /// A flow of a kind no other variant names, with the venue's own name for it.
    Other(String),
}

/// When in a loan's life a [`CashFlowKind::MarginInterest`] charge falls.
///
/// A venue charges the interest on a new loan when it is taken and again at each of its charging
/// boundaries. A charge on a new loan can carry a time before the position it belongs to opened,
/// since a venue may stamp it at the start of the period the loan was taken in.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum InterestTiming {
    /// The charge for holding the loan over a charging period, at one of the venue's boundaries.
    Periodic,
    /// The first charge, made when the loan is taken.
    OnBorrow,
    /// A timing neither variant names, with the venue's own name for it. Like an absent timing
    /// (`None`), it is unknown: do not treat it as either named variant.
    Other(SmolStr),
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

/// What one read of a venue's cash flows over a span returned.
///
/// `resume` says whether the read stopped before the span's end:
/// - `None`: every flow in the span the venue still holds, and the client reports, is in `flows`.
///   Each client documents the records it leaves out.
/// - `Some(t)`: the read stopped at its bound, and flows from `t` on may be missing from `flows`.
///   Read on from `start = t` with the same end. That read can return again flows this one
///   returned at or after `t`, so match them by [`CashFlowId`].
///
/// A venue also keeps its records for a limited time. `held_since` says when that limit cuts
/// into the span, so a caller can tell flows the venue no longer has from flows that never
/// happened.
#[non_exhaustive]
#[must_use = "`resume` and `held_since` say whether the span was read in full"]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct CashFlowsRead<AssetKey, InstrumentKey> {
    /// The flows read, in the span.
    pub flows: Vec<CashFlow<AssetKey, InstrumentKey>>,
    /// Where to read on from, or `None` when the read reached the span's end.
    pub resume: Option<DateTime<Utc>>,
    /// The earliest moment the read covers, when the span starts before it; flows before it may
    /// exist but cannot be read. It is after the span's end when none of the span can be read.
    /// `None` when the venue's limit is not known to cut into the span, which is no claim that
    /// the venue holds flows without limit.
    ///
    /// A caller that needs every flow in the span, such as one adding up a position's whole
    /// interest, treats the part before `held_since` as a gap: `resume: None` covers what the
    /// venue holds, not what it held. The limit cuts into the span from its start, so only the
    /// first read of a span can report it; a caller reading on from `resume` has nothing to learn
    /// from a later read's.
    pub held_since: Option<DateTime<Utc>>,
}

impl<AssetKey, InstrumentKey> CashFlowsRead<AssetKey, InstrumentKey> {
    /// A read that stopped at `resume` (`None`: it reached the span's end), with no venue limit
    /// cutting into the span.
    pub fn new(
        flows: Vec<CashFlow<AssetKey, InstrumentKey>>,
        resume: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            flows,
            resume,
            held_since: None,
        }
    }

    /// A read that reached the span's end.
    pub fn complete(flows: Vec<CashFlow<AssetKey, InstrumentKey>>) -> Self {
        Self::new(flows, None)
    }

    /// Sets [`held_since`](Self::held_since).
    pub fn with_held_since(mut self, held_since: Option<DateTime<Utc>>) -> Self {
        self.held_since = held_since;
        self
    }
}
