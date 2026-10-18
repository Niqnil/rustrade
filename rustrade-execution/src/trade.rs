use crate::order::id::{ClientOrderId, OrderId, StrategyId};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::QuoteAsset,
    instrument::{Instrument, kind::InstrumentKind, round_toward_zero_to_step},
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt::{Display, Formatter};

/// A venue's own identifier for one trade, carried through opaquely.
///
/// # Uniqueness is the venue's to define, and is often narrower than global
///
/// This library does not mint these — it passes through whatever the venue said, so what an id
/// distinguishes is whatever that venue distinguishes. Binance's are **per symbol**, which is why
/// this crate's own deduplication keys on the instrument alongside the id rather than on the id
/// alone; Hyperliquid reports a transaction hash that can cover several matches at once. A
/// consumer that needs a key should use `(exchange, instrument, TradeId)` and not assume any part
/// of it is redundant.
///
/// [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) mints ids unique within one venue
/// instance for its lifetime — and, because each venue counts from zero, **not** across the
/// several a multi-exchange backtest holds. That is the same scoping a real venue gives, for the
/// same reason.
///
/// # `Ord` is byte order, not time order
///
/// The derived comparison is lexicographic over the underlying string, so `"10" < "9"`. It exists
/// so this type can key a map or a set; it carries no chronological meaning and is not a stand-in
/// for one. Order trades by [`Trade::time_exchange`].
///
/// # Finding the order behind a trade
///
/// Use [`Trade::order_id`], which is typed. Do **not** parse this id: one order can print more
/// than once, several venues embed structure of their own here (IBKR's `execution_id` is
/// dot-separated, Alpaca's can be a UUID), and no format is common to them.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
pub struct TradeId<T = SmolStr>(pub T);

impl<T> Display for TradeId<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TradeId {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        Self(SmolStr::new(id))
    }
}

/// One execution: a fill the venue reported, or one the engine settled itself.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// build one with [`Trade::new`] and the `with_*` methods, and change a copy by assigning its
/// public fields.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct Trade<AssetKey, InstrumentKey> {
    pub id: TradeId,
    pub order_id: OrderId,
    pub instrument: InstrumentKey,
    pub strategy: StrategyId,
    pub time_exchange: DateTime<Utc>,
    pub side: Side,
    pub price: Decimal,
    /// The size of *this* execution.
    pub quantity: Decimal,
    /// The order's cumulative filled quantity as of this execution, as the venue reported it --
    /// **not** the size of this execution, which is [`Trade::quantity`].
    ///
    /// A fill carries two facts: that an execution happened, and what it left the order at. A
    /// consumer that tracks order state needs both, and they arrive together or not at all. Where
    /// the venue supplies the second, carrying it here lets the order be advanced from the fill
    /// itself rather than from a separate message that may never come.
    ///
    /// Advancing from a cumulative is idempotent: re-applying the same fill, or applying two out
    /// of order, cannot double-count, because the receiver takes the greater of what it holds and
    /// what is reported rather than adding to a running total.
    ///
    /// `None` means the venue's fill payload does not carry it, and the order's state must be
    /// learned some other way -- an order snapshot, or a reconciliation fetch. It is not a claim
    /// that nothing is filled. Known producers, as of writing:
    ///
    /// | Source | Reports it |
    /// |---|---|
    /// | Binance Spot / Margin WebSocket `executionReport` | yes (`z`) |
    /// | Binance Spot / Margin fill recovery after a disconnect | yes, rebuilt from the order's executions; `None` where that lookup failed or ran out of time |
    /// | Binance Spot / Margin `fetch_trades` (`myTrades` REST) | no |
    /// | Alpaca WebSocket `trade_updates` | yes (`order.filled_qty`) |
    /// | Alpaca account-activities REST | yes (`cum_qty`) |
    /// | Interactive Brokers `ExecutionData` | yes (`cumulative_quantity`) |
    /// | Hyperliquid `userFills` | no |
    /// | `SimulatedVenue` | yes |
    ///
    /// `#[serde(default)]` so trades serialised before this field existed still deserialise, as
    /// `None`.
    #[serde(default)]
    pub order_filled_quantity: Option<Decimal>,
    pub fees: AssetFees<AssetKey>,
    /// Why the fill happened, as far as its producer can tell. See [`TradeOrigin`] for what each
    /// venue reports.
    ///
    /// `#[serde(default)]` so trades serialised before this field existed still deserialise, as
    /// [`TradeOrigin::Order`].
    #[serde(default)]
    pub origin: TradeOrigin,
    /// The side of the position the fill applies to, when the venue names one: [`Side::Buy`] for
    /// a long, [`Side::Sell`] for a short.
    ///
    /// A venue in hedge mode, holding a long and a short in one instrument at once, names the
    /// position each fill belongs to (Binance USD-M futures' `LONG` and `SHORT`). The engine in
    /// `OmsMode::Hedging` checks a forced fill, which matches none of its orders, against it: a
    /// forced fill must reduce the position the venue names. It only ever confirms the side a
    /// reducing fill already implies, the side opposite its own, and does not choose among
    /// several of the engine's positions on that side.
    ///
    /// It takes part in the derived `Eq`, `Hash` and `Ord`, as every field does: deduplicate fills
    /// by instrument and [`id`](Self::id), not by the whole `Trade`.
    /// `None` when the venue names none, as every client in this crate does as of writing: none
    /// trades a venue in hedge mode.
    ///
    /// `#[serde(default)]` so trades serialised before this field existed still deserialise, as
    /// `None`.
    #[serde(default)]
    pub position_side: Option<Side>,
    /// The client order id of the order this fill belongs to, as the venue reported it with the
    /// fill.
    ///
    /// The id is chosen before the order is sent, so a consumer can attribute a fill by it at
    /// once. That includes a fill of an order placed outside the engine on the same account,
    /// which no order the engine knows matches: [`order_id`](Self::order_id) is the venue's, and
    /// the fill can arrive before the venue's response to the order names it, or long after, in a
    /// recovery read. A consumer that trades an account outside the engine can mark those orders'
    /// client order ids, by a prefix say, and keep their fills from the engine.
    ///
    /// `None` means the fill's payload did not carry one. It is not a claim that the order has
    /// none. Known producers, as of writing:
    ///
    /// | Source | Reports it |
    /// |---|---|
    /// | Binance Spot / Margin WebSocket `executionReport` | yes (`c`) |
    /// | Binance Spot / Margin fill recovery after a disconnect | yes, read from each recovered fill's order; `None` where that lookup failed or ran out of time |
    /// | Binance Spot / Margin `fetch_trades` (`myTrades` REST) | no |
    /// | Alpaca WebSocket `trade_updates` | yes (`order.client_order_id`) |
    /// | Alpaca account-activities REST (fill recovery, `fetch_trades`), `fetch_option_events` | no |
    /// | Interactive Brokers executions | yes, the order reference, which this crate's client sets to the client order id; `None` for an order without one |
    /// | Hyperliquid `userFills` | yes, the client order id the order's cloid maps to; `None` for an order placed without a cloid |
    /// | `SimulatedVenue` | yes |
    /// | The `rustrade` engine's expiry settlement | no |
    ///
    /// `#[serde(default)]` so trades serialised before this field existed still deserialise, as
    /// `None`.
    #[serde(default)]
    pub client_order_id: Option<ClientOrderId>,
}

impl<AssetKey, InstrumentKey> Trade<AssetKey, InstrumentKey> {
    /// A trade of [`TradeOrigin::Order`]. Set another origin with
    /// [`with_origin`](Self::with_origin).
    #[allow(clippy::too_many_arguments)] // One argument per field a fill always has.
    pub fn new(
        id: TradeId,
        order_id: OrderId,
        instrument: InstrumentKey,
        strategy: StrategyId,
        time_exchange: DateTime<Utc>,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        order_filled_quantity: Option<Decimal>,
        fees: AssetFees<AssetKey>,
    ) -> Self {
        Self {
            id,
            order_id,
            instrument,
            strategy,
            time_exchange,
            side,
            price,
            quantity,
            order_filled_quantity,
            fees,
            origin: TradeOrigin::Order,
            position_side: None,
            client_order_id: None,
        }
    }

    /// This trade with its [`origin`](Self::origin) set.
    #[must_use]
    pub fn with_origin(self, origin: TradeOrigin) -> Self {
        Self { origin, ..self }
    }

    /// This trade with the [`position_side`](Self::position_side) the venue named.
    #[must_use]
    pub fn with_position_side(self, position_side: Side) -> Self {
        Self {
            position_side: Some(position_side),
            ..self
        }
    }

    /// This trade with the [`client_order_id`](Self::client_order_id) the venue reported.
    #[must_use]
    pub fn with_client_order_id(self, client_order_id: ClientOrderId) -> Self {
        Self {
            client_order_id: Some(client_order_id),
            ..self
        }
    }

    pub fn value_quote(&self) -> Decimal {
        self.price * self.quantity.abs()
    }

    /// How this fill moves the position it applies to, when it is a spot fill whose fee the
    /// venue charged in `instrument`'s base asset. `None` otherwise: the fill then moves the
    /// position by its own [`side`](Self::side) and [`quantity`](Self::quantity).
    ///
    /// The `rustrade` engine applies such a fill with this function. A component that mirrors
    /// positions outside the engine, such as a ledger of the fills an execution client forwarded,
    /// agrees with the engine by calling it too.
    ///
    /// `held` is the position the fill applies to, as it stands before the fill, signed: positive
    /// for a long, negative for a short, zero for none. In an account holding a long and a short
    /// at once, it is the one position the fill is routed to.
    ///
    /// # The rule
    ///
    /// - **The fee is netted.** It leaves the account in base on either side, so the holding moves
    ///   by the fill's signed quantity less the fee: a buy of `q` with a base fee `f` holds
    ///   `q − f`, and a sell delivers `q + f`. A negative fee, a rebate, adds to it.
    /// - **The position is rounded toward zero to the step.** When
    ///   [`Instrument::quantity_step`] gives a step, the position the fill leaves, `held` plus the
    ///   netted move, is rounded toward zero to a whole multiple of it, and the move is what lands
    ///   the position there. A venue sells only whole steps, so a position off the step could
    ///   never be closed to flat. It is the position that is rounded, not the move: a sell of 1
    ///   with a fee of 0.001 from a long of 2, in steps of 0.01, leaves 0.999, so the position is
    ///   0.99 and the move a sell of 1.01. What is rounded off is [`HoldingMove::dust`]. A `held`
    ///   already off the step is rounded too, so the dust can include a remainder this fill did
    ///   not create. Without a step, the dust is zero.
    /// - **The move's side is its sign.** A fee larger than a buy's quantity sells the difference.
    ///   A move of zero keeps the netted move's side, which is the fill's own unless the fee
    ///   reversed it, with a quantity of zero.
    /// - **Spot only**, spot margin included. A derivative's position is counted in contracts, and
    ///   an inverse contract pays its fees in the settlement coin, so a fee there never changes the
    ///   quantity. A fee in quote or a third asset, such as BNB, and a fee of zero give `None`.
    ///
    /// # What it leaves to the caller
    ///
    /// - **The price and the fee.** The netted quantity keeps the fill's price, so PnL is charged
    ///   the fee's quote equivalent, its `fees_quote`: buying `q − f` at `p` and paying `f × p`
    ///   costs `q × p`, what the fill paid. The engine also adds the value of a long's dust, at
    ///   the fill's price, to the fill's fee, since the account holds that base but cannot sell
    ///   it. A short's dust is base still owed on the loan, which the position books as bought
    ///   back at the fill's price, so nothing is added.
    /// - **Only the fee the venue reported** is netted. A fee the caller computes itself, as the
    ///   engine's fee model does, is in quote, and reaches PnL alone.
    /// - **A settlement fill larger than its position, in a hedged account.** Where an account
    ///   can hold a long and a short at once (the engine's `OmsMode::Hedging`), the engine does not
    ///   call this for a settlement fill, such as a liquidation, that matches none of its orders
    ///   and is larger than the position it reduces: it cuts that fill to the position, unnetted,
    ///   so the position lands flat, and reports the excess. A mirror that matches the engine does
    ///   the same rather than calling this. With one position per instrument, every fill is
    ///   netted.
    pub fn holding_move<ExchangeKey>(
        &self,
        instrument: &Instrument<ExchangeKey, AssetKey>,
        held: Decimal,
    ) -> Option<HoldingMove>
    where
        AssetKey: PartialEq,
    {
        if !matches!(instrument.kind, InstrumentKind::Spot)
            || self.fees.asset != instrument.underlying.base
            || self.fees.fees.is_zero()
        {
            return None;
        }

        let netted = signed(self.side, self.quantity.abs()) - self.fees.fees;
        let holds = held + netted;
        let rounded = instrument
            .quantity_step()
            .map_or(holds, |step| round_toward_zero_to_step(holds, step));
        let (netted_side, _) = unsigned(netted, self.side);
        let (side, quantity) = unsigned(rounded - held, netted_side);
        Some(HoldingMove {
            side,
            quantity,
            dust: holds - rounded,
        })
    }
}

/// How a spot fill with a fee in the base asset moves the position it applies to: see
/// [`Trade::holding_move`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct HoldingMove {
    /// [`Side::Buy`] if the move adds to the position, [`Side::Sell`] if it takes from it.
    pub side: Side,
    /// The size of the move, never negative.
    pub quantity: Decimal,
    /// The part of the netted holding rounded off the position to land it on the quantity step,
    /// signed as a position is, and less than one step in size. Positive is base the account holds
    /// that a long does not count; negative is base still owed on a short's loan that the short
    /// does not count. Zero without a step, or when the position lands on it.
    ///
    /// It stays in the account's balance. Each fill's is rounded off on its own, so the balance can
    /// gather more than one step of it over many fills, which no position holds.
    pub dust: Decimal,
}

/// `quantity` with the sign of `side`: positive for a buy, or for a long position.
fn signed(side: Side, quantity: Decimal) -> Decimal {
    match side {
        Side::Buy => quantity,
        Side::Sell => -quantity,
    }
}

/// The side and size of a signed move: a buy if positive, a sell if negative, and `at_zero`
/// with a size of zero if it moves nothing.
fn unsigned(moved: Decimal, at_zero: Side) -> (Side, Decimal) {
    let side = match moved.cmp(&Decimal::ZERO) {
        std::cmp::Ordering::Greater => Side::Buy,
        std::cmp::Ordering::Less => Side::Sell,
        std::cmp::Ordering::Equal => at_zero,
    };
    (side, moved.abs())
}

/// What one [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades) call
/// read of the span it was asked for.
///
/// A venue reads a span in bounded calls, so one call may stop before the span's end. `resume`
/// says whether this one did:
/// - `None`: every fill in the span the venue still holds is in `trades`.
/// - `Some(t)`: the call stopped at its bound, and fills from `t` on may be missing from
///   `trades`. Read on from `start = t` with the same end. That read can return again fills this
///   one returned at or after `t`, so match them by instrument and [`TradeId`].
///
/// `trades` can be empty while `resume` is `Some`: a venue that reads every instrument's fills
/// and filters them afterwards can spend a whole call on other instruments'. A caller that takes
/// `trades` without reading on from `resume` can miss fills.
#[non_exhaustive]
#[must_use = "`resume` says whether the span was read to its end"]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Constructor)]
pub struct TradesRead<AssetKey, InstrumentKey> {
    /// The fills read, in the span and for the requested instruments.
    pub trades: Vec<Trade<AssetKey, InstrumentKey>>,
    /// Where to read on from, or `None` when the read reached the span's end.
    pub resume: Option<DateTime<Utc>>,
}

impl<AssetKey, InstrumentKey> TradesRead<AssetKey, InstrumentKey> {
    /// A read that reached the span's end.
    pub fn complete(trades: Vec<Trade<AssetKey, InstrumentKey>>) -> Self {
        Self {
            trades,
            resume: None,
        }
    }
}

impl<AssetKey, InstrumentKey> Display for Trade<AssetKey, InstrumentKey>
where
    AssetKey: Display,
    InstrumentKey: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ instrument: {}, side: {}, price: {}, quantity: {}, time: {}",
            self.instrument, self.side, self.price, self.quantity, self.time_exchange
        )?;
        // Shown only when it is not the default, so a forced fill stands out in a log line.
        if self.origin != TradeOrigin::Order {
            write!(f, ", origin: {}", self.origin)?;
        }
        write!(f, " }}")
    }
}

/// Why a [`Trade`] happened, as far as its producer can tell.
///
/// # `Order` is the default, not a guarantee
///
/// [`Order`](Self::Order) means the producer reported no other origin. Most venues mark some
/// origins and not others, so a fill can be `Order` and still be forced, or still be for an order
/// this client did not place. What each producer reports, as of writing:
///
/// | Source | Reports |
/// |---|---|
/// | Interactive Brokers | [`Liquidation`](Self::Liquidation), [`External`](Self::External) (opt-in), `Order` |
/// | Binance Spot / Margin | `Order` only |
/// | Hyperliquid | `Order` only |
/// | Alpaca | `Order` on fills; [`Expiry`](Self::Expiry), [`Exercise`](Self::Exercise), [`Assignment`](Self::Assignment) and [`External`](Self::External) from `AlpacaClient::fetch_option_events` |
/// | [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) | `Order` only |
/// | The `rustrade` engine's expiry settlement | [`Expiry`](Self::Expiry) |
///
/// - **Interactive Brokers** flags a fill of an IB-initiated liquidation. Another API client's
///   fill is reported, as `External`, only when `IbkrConfig::other_clients_fills` opts in.
/// - **Binance** marks no forced liquidation in its Spot or Margin order stream: its liquidation
///   markers are documented for futures only. Every fill on the account arrives, whoever placed
///   its order. `BinanceMargin::fetch_forced_liquidations` reads the margin orders Binance placed
///   to liquidate the account, for a caller to match against its fills.
/// - **Hyperliquid** fills carry a liquidation marker, but the SDK this crate uses drops it.
/// - **Alpaca** marks no liquidation. Option assignment, exercise and expiry are account
///   activities, which this crate does not read.
/// - The **simulated venue** models no liquidation.
///
/// A forced variant takes precedence over [`External`](Self::External): a liquidation is
/// reported as a liquidation whoever placed the order it closed.
///
/// # Serialised form and ordering
///
/// Serialised as the variant's name (`"Liquidation"`), as [`Side`] is, while `Display` writes it
/// in lower case (`liquidation`). The names are a stored format: they are not renamed, and new
/// variants are added after the existing ones. A trade with an origin a build does not know does
/// not deserialise in that build.
///
/// `Ord` follows declaration order. It exists so [`Trade`] can derive `Ord`, and carries no
/// meaning beyond that.
#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
pub enum TradeOrigin {
    /// A fill of an order, with no other origin reported. See the type's docs for why this is not
    /// a guarantee that this client placed the order.
    #[default]
    Order,
    /// The venue or broker closed the position because the account fell short of margin.
    Liquidation,
    /// The venue reduced the position against a counterparty it was liquidating
    /// (auto-deleveraging).
    Adl,
    /// An option this account was short was assigned.
    Assignment,
    /// An option this account held was exercised, by the holder or automatically.
    Exercise,
    /// The instrument expired and the position was settled.
    Expiry,
    /// A fill of an order that another client, such as another API connection or the venue's own
    /// trading screen, placed on the same account.
    External,
}

impl TradeOrigin {
    /// Whether the position changed without the account placing an order for it: a
    /// [`Liquidation`](Self::Liquidation), an [`Adl`](Self::Adl) or an
    /// [`Assignment`](Self::Assignment).
    ///
    /// An [`Exercise`](Self::Exercise) is the holder's right, and an [`Expiry`](Self::Expiry) is
    /// the instrument's end, so neither counts.
    pub fn is_forced(self) -> bool {
        matches!(self, Self::Liquidation | Self::Adl | Self::Assignment)
    }
}

impl Display for TradeOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Order => "order",
            Self::Liquidation => "liquidation",
            Self::Adl => "adl",
            Self::Assignment => "assignment",
            Self::Exercise => "exercise",
            Self::Expiry => "expiry",
            Self::External => "external",
        })
    }
}

/// A venue's notice that a trade it reported earlier was busted or corrected.
///
/// Sent as [`AccountEventKind::TradeAmended`](crate::AccountEventKind::TradeAmended). The
/// [`Trade`] it amends was delivered as reported and is not withdrawn: positions, PnL and fees
/// built on it stay wrong until the consumer applies this. Find it by instrument and
/// [`original`](Self::original), reverse it, and apply the [`kind`](Self::kind)'s replacement, if
/// any. The library does not act on it beyond reporting it, and the engine only logs it.
///
/// A replacement can itself be amended later. That amendment names the replacement's id as its
/// `original`, so follow amendments by id, one at a time.
///
/// # Known limitations
///
/// - An order's cumulative filled quantity is not lowered by a bust:
///   [`Open::is_superseded_by`](crate::order::state::Open::is_superseded_by) refuses a lower
///   cumulative, so the order stays on what it reported before.
/// - Alpaca's fill recovery after a reconnect reads fills only, so an Alpaca amendment sent while
///   the stream was disconnected is not reported. IBKR's recovery reads corrections too.
///
/// # Delivery
///
/// Apply an amendment idempotently: reverse a given `original` once, and apply a replacement
/// once by its [`TradeId`]. Whether a venue can send one amendment twice, such as around a
/// reconnect, is not known for every producer. The library delivers each IBKR correction once
/// per account stream, but does not deduplicate Alpaca's amendments.
///
/// Known producers, as of writing:
/// - Alpaca's `trade_updates` `trade_bust` and `trade_correct`;
/// - IBKR's corrected executions, as [`Corrected`](TradeAmendmentKind::Corrected) only. IB
///   documents no busts.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Constructor)]
pub struct TradeAmendment<AssetKey, InstrumentKey> {
    /// The instrument of the trade amended.
    pub instrument: InstrumentKey,
    /// The order whose trade was amended.
    pub order_id: OrderId,
    /// When the venue amended the trade, or when it was received where the venue does not say.
    pub time_exchange: DateTime<Utc>,
    /// The [`TradeId`] of the trade amended, as the venue named it. `None` when the venue's notice
    /// did not name it: match it to a trade of [`order_id`](Self::order_id) yourself, or
    /// reconcile with
    /// [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades).
    pub original: Option<TradeId>,
    /// What the venue did to the trade.
    pub kind: TradeAmendmentKind<AssetKey, InstrumentKey>,
}

/// What a [`TradeAmendment`] did to the trade.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum TradeAmendmentKind<AssetKey, InstrumentKey> {
    /// The trade was cancelled: it did not happen.
    Busted {
        /// The quantity busted, as a magnitude, when the venue said. A venue that reports it as
        /// a negative reversal is read as its absolute value.
        quantity: Option<Decimal>,
    },
    /// The trade was replaced, in full.
    Corrected {
        /// The trade as corrected: its price and quantity are the corrected values, not
        /// differences from the original's. It has its own [`TradeId`], which a later amendment
        /// of it names.
        replacement: Trade<AssetKey, InstrumentKey>,
    },
    /// The trade was corrected, but the venue's notice lacked what a replacement [`Trade`] needs.
    /// What it did carry is here. Reconcile the trade with
    /// [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades).
    CorrectedUnresolved {
        /// The id of the replacement, which a later amendment of it names.
        id: Option<TradeId>,
        /// The corrected price, if the notice had one.
        price: Option<Decimal>,
        /// The corrected quantity, if the notice had one.
        quantity: Option<Decimal>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct AssetFees<AssetKey> {
    pub asset: AssetKey,
    pub fees: Decimal,
    /// Fee value in quote currency when computable.
    /// - `Some(fees)` if fee asset == quote asset
    /// - `Some(fees * price)` if fee asset == base asset (computed by indexer)
    /// - `None` if fee asset is third-party (e.g., BNB), which needs external price data. Also
    ///   `None` when the indexer zeroed a fee whose asset was not in its map (see
    ///   [`AccountEventIndexer::trade`](crate::indexer::AccountEventIndexer::trade)).
    ///
    /// The engine's position PnL counts only this quote equivalent. A fee with `None` is left
    /// out of PnL, and its raw [`fees`](Self::fees) stays recorded on the position. A consumer that
    /// wants it counted prices it. On a position's running fee totals, this is the sum of the
    /// quote equivalents its fees had, and `None` only while none had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fees_quote: Option<Decimal>,
}

impl<AssetKey> AssetFees<AssetKey> {
    pub fn new(asset: AssetKey, fees: Decimal, fees_quote: Option<Decimal>) -> Self {
        Self {
            asset,
            fees,
            fees_quote,
        }
    }
}

impl AssetFees<QuoteAsset> {
    /// Construct fees already denominated in quote asset.
    /// Sets `fees_quote = Some(fees)` since no conversion needed.
    pub fn quote_fees(fees: Decimal) -> Self {
        Self {
            asset: QuoteAsset,
            fees,
            fees_quote: Some(fees),
        }
    }
}

impl Default for AssetFees<QuoteAsset> {
    fn default() -> Self {
        Self {
            asset: QuoteAsset,
            fees: Decimal::ZERO,
            fees_quote: Some(Decimal::ZERO),
        }
    }
}

impl<AssetKey> Default for AssetFees<Option<AssetKey>> {
    fn default() -> Self {
        Self {
            asset: None,
            fees: Decimal::ZERO,
            fees_quote: None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        asset::{Asset, name::AssetNameExchange},
        exchange::ExchangeId,
        instrument::{
            kind::perpetual::PerpetualContract,
            name::InstrumentNameExchange,
            spec::{
                InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice,
                InstrumentSpecQuantity, OrderQuantityUnits,
            },
        },
        test_utils,
    };

    fn trade() -> Trade<AssetNameExchange, InstrumentNameExchange> {
        Trade::new(
            TradeId::new("t-1"),
            OrderId::new("o-1"),
            InstrumentNameExchange::new("btc_usdt"),
            StrategyId::unknown(),
            DateTime::<Utc>::MIN_UTC,
            Side::Sell,
            Decimal::ONE,
            Decimal::TWO,
            None,
            AssetFees::new(AssetNameExchange::from("usdt"), Decimal::ZERO, None),
        )
    }

    #[test]
    fn a_trade_is_of_an_order_unless_given_another_origin() {
        assert_eq!(trade().origin, TradeOrigin::Order);
        assert_eq!(
            trade().with_origin(TradeOrigin::Adl).origin,
            TradeOrigin::Adl
        );
    }

    /// A trade serialised before `origin` existed still loads, as an order's.
    #[test]
    fn a_trade_without_an_origin_deserialises_as_an_orders() {
        let mut json = serde_json::to_value(trade().with_origin(TradeOrigin::External)).unwrap();
        assert_eq!(json["origin"], "External");
        json.as_object_mut().unwrap().remove("origin");

        let read: Trade<AssetNameExchange, InstrumentNameExchange> =
            serde_json::from_value(json).unwrap();
        assert_eq!(read, trade());
    }

    #[test]
    fn display_names_the_origin_only_when_it_is_not_an_orders() {
        assert!(!trade().to_string().contains("origin"));
        assert!(
            trade()
                .with_origin(TradeOrigin::Liquidation)
                .to_string()
                .ends_with(", origin: liquidation }")
        );
    }

    // --- holding_move ------------------------------------------------------------------------

    /// A spot BTC/USDT instrument, with a quantity step in BTC when `step` is given.
    fn btc_usdt(step: Option<Decimal>) -> Instrument<ExchangeId, Asset> {
        let mut instrument = test_utils::instrument(ExchangeId::BinanceSpot, "btc", "usdt");
        instrument.spec = step.map(|step| {
            InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
                InstrumentSpecQuantity::new(
                    OrderQuantityUnits::Asset(test_utils::asset("btc")),
                    step,
                    step,
                ),
                InstrumentSpecNotional::new(Decimal::ZERO),
            )
        });
        instrument
    }

    /// A fill of `quantity` on `side`, charged `fee` in `fee_asset`.
    fn fill(side: Side, quantity: Decimal, fee_asset: &str, fee: Decimal) -> Trade<Asset, ()> {
        Trade::new(
            TradeId::new("t-1"),
            OrderId::new("o-1"),
            (),
            StrategyId::unknown(),
            DateTime::<Utc>::MIN_UTC,
            side,
            dec!(100),
            quantity,
            None,
            AssetFees::new(test_utils::asset(fee_asset), fee, None),
        )
    }

    fn moved(side: Side, quantity: Decimal, dust: Decimal) -> Option<HoldingMove> {
        Some(HoldingMove {
            side,
            quantity,
            dust,
        })
    }

    #[test]
    fn holding_move_is_none_unless_a_spot_fill_pays_a_base_fee() {
        let mut perpetual = btc_usdt(None);
        perpetual.kind = InstrumentKind::Perpetual(PerpetualContract {
            contract_size: Decimal::ONE,
            settlement_asset: test_utils::asset("btc"),
        });
        let cases = [
            (
                "quote fee",
                btc_usdt(None),
                fill(Side::Buy, dec!(1), "usdt", dec!(0.1)),
            ),
            (
                "third-asset fee",
                btc_usdt(None),
                fill(Side::Buy, dec!(1), "bnb", dec!(0.001)),
            ),
            (
                "zero base fee",
                btc_usdt(None),
                fill(Side::Buy, dec!(1), "btc", Decimal::ZERO),
            ),
            (
                "derivative",
                perpetual,
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
            ),
        ];

        for (case, instrument, fill) in cases {
            assert_eq!(
                fill.holding_move(&instrument, Decimal::ZERO),
                None,
                "{case}"
            );
        }
    }

    #[test]
    fn holding_move_nets_a_base_fee_on_either_side() {
        let spot = btc_usdt(None);
        let cases = [
            (
                "buy holds less",
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
                moved(Side::Buy, dec!(0.999), Decimal::ZERO),
            ),
            (
                "sell delivers more",
                fill(Side::Sell, dec!(1), "btc", dec!(0.001)),
                moved(Side::Sell, dec!(1.001), Decimal::ZERO),
            ),
            (
                "rebate adds",
                fill(Side::Buy, dec!(1), "btc", dec!(-0.001)),
                moved(Side::Buy, dec!(1.001), Decimal::ZERO),
            ),
            (
                "fee above a buy sells",
                fill(Side::Buy, dec!(0.001), "btc", dec!(0.003)),
                moved(Side::Sell, dec!(0.002), Decimal::ZERO),
            ),
            (
                "fee equal to a buy moves nothing",
                fill(Side::Buy, dec!(0.001), "btc", dec!(0.001)),
                moved(Side::Buy, Decimal::ZERO, Decimal::ZERO),
            ),
        ];

        for (case, fill, expected) in cases {
            assert_eq!(fill.holding_move(&spot, dec!(2)), expected, "{case}");
        }
    }

    /// The position `held` plus the netted move is what is rounded, toward zero, and the move is
    /// what lands it there.
    #[test]
    fn holding_move_rounds_the_position_toward_zero_to_the_step() {
        let spot = btc_usdt(Some(dec!(0.01)));
        let cases = [
            (
                "buy from flat",
                Decimal::ZERO,
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
                moved(Side::Buy, dec!(0.99), dec!(0.009)),
            ),
            // Rounding the move of 1.001 instead would leave a long of 1, more than the 0.999 held.
            (
                "sell from a long",
                dec!(2),
                fill(Side::Sell, dec!(1), "btc", dec!(0.001)),
                moved(Side::Sell, dec!(1.01), dec!(0.009)),
            ),
            // The fee leaves a short of 0.001, rounded away: base still owed, which no short counts.
            (
                "buy back a short",
                dec!(-1),
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
                moved(Side::Buy, dec!(1), dec!(-0.001)),
            ),
            (
                "sell opening a short",
                Decimal::ZERO,
                fill(Side::Sell, dec!(1), "btc", dec!(0.001)),
                moved(Side::Sell, dec!(1), dec!(-0.001)),
            ),
            (
                "held off the step",
                dec!(0.005),
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
                moved(Side::Buy, dec!(0.995), dec!(0.004)),
            ),
            (
                "lands on the step",
                dec!(0.001),
                fill(Side::Buy, dec!(1), "btc", dec!(0.001)),
                moved(Side::Buy, dec!(0.999), Decimal::ZERO),
            ),
        ];

        for (case, held, fill, expected) in cases {
            assert_eq!(fill.holding_move(&spot, held), expected, "{case}");
        }
    }

    /// A rounded move of zero keeps the netted move's side: the fill's own, unless the fee
    /// reversed it.
    #[test]
    fn holding_move_rounded_to_nothing_keeps_the_netted_side() {
        let spot = btc_usdt(Some(dec!(1)));
        let cases = [
            (
                "buy",
                dec!(1),
                fill(Side::Buy, dec!(0.5), "btc", dec!(0.001)),
                moved(Side::Buy, Decimal::ZERO, dec!(0.499)),
            ),
            (
                "fee reverses the buy",
                dec!(-1),
                fill(Side::Buy, dec!(0.001), "btc", dec!(0.003)),
                moved(Side::Sell, Decimal::ZERO, dec!(-0.002)),
            ),
        ];

        for (case, held, fill, expected) in cases {
            assert_eq!(fill.holding_move(&spot, held), expected, "{case}");
        }
    }

    #[test]
    fn only_liquidation_adl_and_assignment_are_forced() {
        let forced: Vec<_> = [
            TradeOrigin::Order,
            TradeOrigin::Liquidation,
            TradeOrigin::Adl,
            TradeOrigin::Assignment,
            TradeOrigin::Exercise,
            TradeOrigin::Expiry,
            TradeOrigin::External,
        ]
        .into_iter()
        .filter(|origin| origin.is_forced())
        .collect();
        assert_eq!(
            forced,
            [
                TradeOrigin::Liquidation,
                TradeOrigin::Adl,
                TradeOrigin::Assignment
            ]
        );
    }
}
