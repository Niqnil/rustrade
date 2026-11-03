//! Cash that moves into or out of an account other than by a trade: perpetual funding, margin
//! interest, stock borrow fees and rebates.
//!
//! An account stream reports each such movement the venue posts as
//! [`AccountEventKind::CashFlow`](crate::AccountEventKind::CashFlow), carrying a [`CashFlow`]. The
//! library delivers the venue's own figures; rates, day counts and what to do with the flows stay
//! the consumer's.

use crate::order::id::{ClientOrderId, OrderId};
use chrono::{DateTime, Utc};
use derive_more::From;
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
///   [`MarginInterestModel`](crate::margin::MarginInterestModel) charge, and the
///   [`LiquidationFee`](CashFlowKind::LiquidationFee) of a configured
///   [`SpotMarginLiquidation`](crate::margin::SpotMarginLiquidation), each flow once, with an id
///   from its own sequence.
///
/// `Ord` and `Hash` exist so a value holding one can sit in an engine output, which requires them,
/// and are not a meaningful ranking: they compare field by field.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
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
    /// The `rustrade` engine does not guess an instrument for a `None` flow: it asks the
    /// strategy's `CashFlowAttributionStrategy`, and applies the flow to no position when that
    /// names none. See `EngineState::update_from_account` in `rustrade` for why.
    pub instrument: Option<InstrumentKey>,
    /// The time the venue stamps the flow with: when it posted the flow, unless its producer
    /// documents otherwise. A venue may stamp a charge at the start of the period it is for, as
    /// Binance stamps its margin interest; such a producer also sets [`period`](Self::period).
    pub time_exchange: DateTime<Utc>,
    /// The venue's identifier for the flow, when it gives one. See [`CashFlowId`].
    pub id: Option<CashFlowId>,
    /// The period the flow is for, when its producer knows it, such as the charging period a
    /// margin interest charge was made in. `None` when the producer does not say. Set with
    /// [`with_period`](Self::with_period).
    ///
    /// [`time_exchange`](Self::time_exchange) stays the venue's own stamp, which a producer
    /// setting a period puts in the period or at its start. Nothing checks that. The `rustrade`
    /// engine reads the period only of an [`InterestTiming::OnBorrow`] charge that names no
    /// [`loan`](Self::loan): a venue that stamps the charge on a new loan at the start of its
    /// period stamps it before the position the loan funds opened, and the period tells the engine
    /// the position may still own it. See `InstrumentState::update_from_cash_flow` in
    /// `rustrade`.
    ///
    /// `#[serde(default)]` so a flow serialised before this field existed still loads.
    #[serde(default)]
    pub period: Option<CashFlowPeriod>,
    /// The loan the flow is the charge on, when its producer knows which loan, as for an
    /// [`InterestTiming::OnBorrow`] charge on a loan one of its own orders took. `None` when the
    /// producer does not know, which is the case for every other flow. Set with
    /// [`with_loan`](Self::with_loan).
    ///
    /// A producer that sets it also sets [`instrument`](Self::instrument) to the order's
    /// instrument. The `rustrade` engine applies a flow with a loan to the open position only if
    /// the loan's order opened or added to that position, whenever the flow is stamped. See
    /// `InstrumentState::update_from_cash_flow` in `rustrade`.
    ///
    /// `#[serde(default)]` so a flow serialised before this field existed still loads.
    #[serde(default)]
    pub loan: Option<CashFlowLoan>,
}

impl<AssetKey, InstrumentKey> CashFlow<AssetKey, InstrumentKey> {
    /// A flow with no [`period`](Self::period). Add one with [`with_period`](Self::with_period).
    pub fn new(
        kind: CashFlowKind,
        asset: AssetKey,
        amount: Decimal,
        instrument: Option<InstrumentKey>,
        time_exchange: DateTime<Utc>,
        id: Option<CashFlowId>,
    ) -> Self {
        Self {
            kind,
            asset,
            amount,
            instrument,
            time_exchange,
            id,
            period: None,
            loan: None,
        }
    }

    /// Sets [`period`](Self::period).
    pub fn with_period(mut self, period: Option<CashFlowPeriod>) -> Self {
        self.period = period;
        self
    }

    /// Sets [`loan`](Self::loan).
    pub fn with_loan(mut self, loan: Option<CashFlowLoan>) -> Self {
        self.loan = loan;
        self
    }
}

/// The loan a [`CashFlow`] is the charge on: when it was taken, and the order that took it.
///
/// A margin venue may lend when it accepts an order, as Binance does for an order that borrows,
/// and charge interest on the new loan at once. The order identifies the position the loan funds:
/// the one the order's trades open or add to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub struct CashFlowLoan {
    /// When the venue lent: its time for accepting the order that borrowed.
    pub time: DateTime<Utc>,
    /// The venue's id for the order that borrowed, as its [`Trade`](crate::trade::Trade)s carry
    /// it in [`order_id`](crate::trade::Trade::order_id).
    pub order_id: OrderId,
    /// The client order id the order was placed with.
    pub cid: ClientOrderId,
}

impl CashFlowLoan {
    /// The loan taken at `time` by the order with venue id `order_id` and client id `cid`.
    pub fn new(time: DateTime<Utc>, order_id: OrderId, cid: ClientOrderId) -> Self {
        Self {
            time,
            order_id,
            cid,
        }
    }
}

/// The period a [`CashFlow`] is for: from [`start`](Self::start), inclusive, to
/// [`end`](Self::end), exclusive. `end` is after `start`.
///
/// Deserialising checks the order too, so a period read back is as valid as one built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "CashFlowPeriodFields")]
pub struct CashFlowPeriod {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl CashFlowPeriod {
    /// The period from `start` to `end`, or `None` when `end` is not after `start`.
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Option<Self> {
        (start < end).then_some(Self { start, end })
    }

    /// When the period starts: the first instant in it.
    pub fn start(&self) -> DateTime<Utc> {
        self.start
    }

    /// When the period ends: the first instant after it.
    pub fn end(&self) -> DateTime<Utc> {
        self.end
    }

    /// Whether `time` is in the period.
    pub fn contains(&self, time: DateTime<Utc>) -> bool {
        (self.start..self.end).contains(&time)
    }
}

/// The serialised form of a [`CashFlowPeriod`], before its order is checked.
#[derive(Deserialize)]
struct CashFlowPeriodFields {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl TryFrom<CashFlowPeriodFields> for CashFlowPeriod {
    type Error = String;

    fn try_from(fields: CashFlowPeriodFields) -> Result<Self, Self::Error> {
        Self::new(fields.start, fields.end).ok_or_else(|| {
            format!(
                "CashFlowPeriod end {} is not after its start {}",
                fields.end, fields.start
            )
        })
    }
}

/// What a [`CashFlow`] is for.
///
/// Each variant carries the venue's own figures where it reports them, and `None` where it does
/// not.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
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
    /// The fee a venue charged for liquidating the account, on the debt the liquidation repaid.
    ///
    /// Charged on the debt a liquidation repaid rather than on each forced fill, so it is not
    /// part of any trade's fees. The forced fills themselves carry
    /// [`TradeOrigin::Liquidation`](crate::trade::TradeOrigin::Liquidation).
    LiquidationFee {
        /// The fee rate, as a fraction of `repaid`.
        rate: Option<Decimal>,
        /// The value of the debt the liquidation repaid, in the flow's asset, which the fee was
        /// charged on.
        repaid: Option<Decimal>,
    },
    /// A flow of a kind no other variant names, with the venue's own name for it.
    Other(String),
}

/// When in a loan's life a [`CashFlowKind::MarginInterest`] charge falls.
///
/// A venue charges the interest on a new loan when it is taken and again at each of its charging
/// boundaries. A charge on a new loan can carry a time before the position it belongs to opened,
/// since a venue may stamp it at the start of the period the loan was taken in. Such a producer
/// gives the flow that period as its [`CashFlow::period`], which lets the `rustrade` engine still
/// give the charge to that position.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 2, hour, 0, 0).unwrap()
    }

    #[test]
    fn a_period_must_end_after_it_starts() {
        assert!(CashFlowPeriod::new(at(10), at(10)).is_none());
        assert!(CashFlowPeriod::new(at(11), at(10)).is_none());
        let period = CashFlowPeriod::new(at(10), at(11)).unwrap();
        assert_eq!((period.start(), period.end()), (at(10), at(11)));
    }

    #[test]
    fn a_period_holds_its_start_and_not_its_end() {
        let period = CashFlowPeriod::new(at(10), at(11)).unwrap();
        assert!(period.contains(at(10)));
        assert!(period.contains(at(10) + chrono::Duration::minutes(59)));
        assert!(!period.contains(at(11)));
        assert!(!period.contains(at(9)));
    }

    #[test]
    fn a_period_round_trips_and_a_reversed_one_does_not_deserialise() {
        let period = CashFlowPeriod::new(at(10), at(11)).unwrap();
        let json = serde_json::to_string(&period).unwrap();
        assert_eq!(
            serde_json::from_str::<CashFlowPeriod>(&json).unwrap(),
            period
        );

        let reversed = json.replace("10:00:00Z", "12:00:00Z");
        assert!(serde_json::from_str::<CashFlowPeriod>(&reversed).is_err());
    }

    #[test]
    fn a_flow_serialised_without_a_period_loads_with_none() {
        let flow: CashFlow<String, String> = CashFlow::new(
            CashFlowKind::Rebate,
            "USD".to_owned(),
            Decimal::ONE,
            None,
            at(10),
            None,
        );
        let mut value = serde_json::to_value(&flow).unwrap();
        value.as_object_mut().unwrap().remove("period");
        let loaded: CashFlow<String, String> = serde_json::from_value(value).unwrap();
        assert_eq!(loaded, flow);
    }

    #[test]
    fn a_flow_serialised_without_a_loan_loads_with_none_and_one_with_a_loan_round_trips() {
        let flow: CashFlow<String, String> = CashFlow::new(
            CashFlowKind::Rebate,
            "USD".to_owned(),
            Decimal::ONE,
            None,
            at(10),
            None,
        );
        let mut value = serde_json::to_value(&flow).unwrap();
        value.as_object_mut().unwrap().remove("loan");
        let loaded: CashFlow<String, String> = serde_json::from_value(value).unwrap();
        assert_eq!(loaded, flow);

        let lent = flow.with_loan(Some(CashFlowLoan::new(
            at(9),
            OrderId::new("7"),
            ClientOrderId::new("borrow"),
        )));
        let json = serde_json::to_string(&lent).unwrap();
        assert_eq!(
            serde_json::from_str::<CashFlow<String, String>>(&json).unwrap(),
            lent
        );
    }
}
