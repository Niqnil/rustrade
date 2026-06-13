use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::borrow::Borrow;
use std::fmt::{Display, Formatter};

/// New type representing a unique `String` identifier for a stream that has been subscribed to.
/// This is used to identify data structures received over the socket.
///
/// For example, `Barter-Data` uses this identifier to associate received data structures from the
/// execution with the original `Barter-Data` `Subscription` that was actioned over the socket.
///
/// Note: Each execution will require the use of different `String` identifiers depending on the
/// data structures they send.
///
/// eg/ [`SubscriptionId`] of an `FtxTrade` is "{BASE}/{QUOTE}" (ie/ market).
/// eg/ [`SubscriptionId`] of a `BinanceTrade` is `"@trade|BTCUSDT"` (ie/ channel|MARKET).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct SubscriptionId(pub SmolStr);

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for SubscriptionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Borrow a [`SubscriptionId`] as a `&str` so an instrument map keyed on [`SubscriptionId`]
/// can be queried with a borrowed key (e.g. `Map::find("@kline_1m|BTCUSDT")`) without
/// allocating an owned [`SubscriptionId`] per lookup.
///
/// Soundness: the `Hash`, `Eq`, and `Ord` impls of [`SubscriptionId`] must agree with those of
/// `str` for `Borrow` to be valid. [`SubscriptionId`] derives all three over its single inner
/// [`SmolStr`] field, and `SmolStr`'s `Hash`/`Eq`/`Ord` delegate to its string contents — so a
/// `SubscriptionId` hashes, compares, and orders identically to the `str` it borrows.
impl Borrow<str> for SubscriptionId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl<S> From<S> for SubscriptionId
where
    S: Into<SmolStr>,
{
    fn from(input: S) -> Self {
        Self(input.into())
    }
}
