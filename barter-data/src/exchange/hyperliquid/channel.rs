use super::Hyperliquid;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL2, trade::PublicTrades},
};
use serde::Serialize;
use serde_json::{Value, json};

/// Hyperliquid WebSocket channel types.
///
/// Maps to the `"type"` field in the subscription payload:
/// ```json
/// {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub enum HyperliquidChannel {
    /// Real-time trades stream.
    Trades,
    /// L2 order book snapshots.
    L2Book,
}

impl HyperliquidChannel {
    /// Build the subscription payload for this channel.
    pub fn subscription_payload(&self, coin: &str) -> Value {
        match self {
            Self::Trades => json!({
                "type": "trades",
                "coin": coin
            }),
            Self::L2Book => json!({
                "type": "l2Book",
                "coin": coin
            }),
        }
    }
}

impl<Instrument> Identifier<HyperliquidChannel>
    for Subscription<Hyperliquid, Instrument, PublicTrades>
{
    fn id(&self) -> HyperliquidChannel {
        HyperliquidChannel::Trades
    }
}

impl<Instrument> Identifier<HyperliquidChannel>
    for Subscription<Hyperliquid, Instrument, OrderBooksL2>
{
    fn id(&self) -> HyperliquidChannel {
        HyperliquidChannel::L2Book
    }
}

impl AsRef<str> for HyperliquidChannel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Trades => "trades",
            Self::L2Book => "l2Book",
        }
    }
}
