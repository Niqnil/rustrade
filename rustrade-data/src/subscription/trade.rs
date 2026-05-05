use super::SubscriptionKind;
use rust_decimal::Decimal;
use rustrade_instrument::Side;
use rustrade_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`PublicTrade`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct PublicTrades;

impl SubscriptionKind for PublicTrades {
    type Event = PublicTrade;

    fn as_str(&self) -> &'static str {
        "public_trades"
    }
}

impl std::fmt::Display for PublicTrades {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`PublicTrade`] model.
///
/// Uses [`SmolStr`] for `id` to avoid heap allocation for typical trade IDs
/// (up to 23 bytes on 64-bit systems are stored inline). Exceptions that
/// heap-allocate: Bitmex UUIDs (36 bytes), Kraken composite IDs (~34 bytes).
///
/// # Side Field
///
/// The `side` field indicates the taker/aggressor side of the trade:
/// - `Some(Side::Buy)`: Taker was buying (lifted the ask)
/// - `Some(Side::Sell)`: Taker was selling (hit the bid)
/// - `None`: Side information not available from the data source
///
/// Crypto exchanges typically provide side info. Equities feeds (e.g., Alpaca IEX/SIP)
/// often do not, as consolidated tape data doesn't include aggressor information.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct PublicTrade {
    pub id: SmolStr,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: Option<Side>,
}
