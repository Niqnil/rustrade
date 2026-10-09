use crate::order::id::{OrderId, StrategyId};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, asset::QuoteAsset};
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
        }
    }

    /// This trade with its [`origin`](Self::origin) set.
    #[must_use]
    pub fn with_origin(self, origin: TradeOrigin) -> Self {
        Self { origin, ..self }
    }

    pub fn value_quote(&self) -> Decimal {
        self.price * self.quantity.abs()
    }
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
/// | Alpaca | `Order` only |
/// | [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) | `Order` only |
/// | The `rustrade` engine's expiry settlement | [`Expiry`](Self::Expiry) |
///
/// - **Interactive Brokers** flags a fill of an IB-initiated liquidation. Another API client's
///   fill is reported, as `External`, only when `IbkrConfig::other_clients_fills` opts in.
/// - **Binance** marks no forced liquidation in its Spot or Margin order stream: its liquidation
///   markers are documented for futures only. Every fill on the account arrives, whoever placed
///   its order.
/// - **Hyperliquid** fills carry a liquidation marker, but the SDK this crate uses drops it.
/// - **Alpaca** marks no liquidation. Option assignment, exercise and expiry are account
///   activities, which this crate does not read.
/// - The **simulated venue** models no liquidation.
///
/// A forced variant takes precedence over [`External`](Self::External): a liquidation is
/// reported as a liquidation whoever placed the order it closed.
///
/// # Ordering
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
    /// - `None` if fee asset is third-party (e.g., BNB) — requires external price data
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
    use rustrade_instrument::{
        asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
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
