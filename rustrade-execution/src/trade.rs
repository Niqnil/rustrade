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

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Trade<AssetKey, InstrumentKey> {
    pub id: TradeId,
    pub order_id: OrderId,
    pub instrument: InstrumentKey,
    pub strategy: StrategyId,
    pub time_exchange: DateTime<Utc>,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fees: AssetFees<AssetKey>,
}

impl<AssetKey, InstrumentKey> Trade<AssetKey, InstrumentKey> {
    pub fn value_quote(&self) -> Decimal {
        self.price * self.quantity.abs()
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
            "{{ instrument: {}, side: {}, price: {}, quantity: {}, time: {} }}",
            self.instrument, self.side, self.price, self.quantity, self.time_exchange
        )
    }
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
