use derive_more::{Display, From};
use rand::prelude::IndexedRandom;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display, From,
)]
pub struct ClientOrderId<T = SmolStr>(pub T);

impl ClientOrderId<SmolStr> {
    /// Construct a `ClientOrderId` from the specified string.
    ///
    /// Use [`Self::random`] to generate a random stack-allocated `ClientOrderId`.
    pub fn new<S: Into<SmolStr>>(id: S) -> Self {
        Self(id.into())
    }

    /// Construct a `ClientOrderId` containing a UUID v4 string.
    ///
    /// Produces lowercase hyphenated format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, 36 chars).
    ///
    /// Required for every Hyperliquid order, of any kind. The venue stores the id as a 16-byte
    /// cloid and reports each order back under it, and only this spelling of a UUID converts
    /// back to the same id. The Hyperliquid clients refuse an order under any other id,
    /// including [`Self::random`], which suits other venues (stack-allocated, no heap).
    #[cfg(feature = "hyperliquid")]
    pub fn uuid() -> Self {
        Self(SmolStr::new(uuid::Uuid::new_v4().to_string()))
    }

    /// Construct a stack-allocated `ClientOrderId` backed by a 23 byte [`SmolStr`].
    pub fn random() -> Self {
        const LEN_URL_SAFE_SYMBOLS: usize = 64;
        const URL_SAFE_SYMBOLS: [char; LEN_URL_SAFE_SYMBOLS] = [
            '_', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
            'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        ];
        // SmolStr can be up to 23 bytes long without allocating
        const LEN_NON_ALLOCATING_CID: usize = 23;

        let mut thread_rng = rand::rng();

        #[allow(clippy::expect_used)] // Invariant: URL_SAFE_SYMBOLS is a const 64-element array
        let random_utf8: [u8; LEN_NON_ALLOCATING_CID] = std::array::from_fn(|_| {
            let symbol = URL_SAFE_SYMBOLS
                .choose(&mut thread_rng)
                .expect("URL_SAFE_SYMBOLS slice is not empty");

            *symbol as u8
        });

        #[allow(clippy::expect_used)] // Invariant: all URL_SAFE_SYMBOLS chars are ASCII
        let random_utf8_str =
            std::str::from_utf8(&random_utf8).expect("URL_SAFE_SYMBOLS are valid utf8");

        Self(SmolStr::new_inline(random_utf8_str))
    }
}

impl Default for ClientOrderId<SmolStr> {
    fn default() -> Self {
        Self::random()
    }
}

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display, From,
)]
pub struct OrderId<T = SmolStr>(pub T);

impl OrderId {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        Self(SmolStr::new(id))
    }
}

/// How an order accepted by a venue can be addressed.
///
/// A venue normally answers an accepted order with an identifier of its own, and that identifier
/// is what every later message about the order carries. Some venues do not: they acknowledge the
/// order while assigning nothing, and it remains addressable only by the [`ClientOrderId`] the
/// caller sent. Hyperliquid does this for an order that is resting but not yet triggered, where
/// the cancel endpoint takes the client id.
///
/// Naming both cases keeps them apart. The alternative -- storing the client id in the same field
/// as a venue id -- makes the field a union of two identifier kinds that can only be told apart by
/// inspecting the string, and leaves a consumer unable to answer the one question that matters:
/// whether two states describe the same venue order. See [`Self::assigned`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum VenueOrderId {
    /// The venue assigned its own identifier, and this is it.
    Assigned(OrderId),
    /// The venue accepted the order without assigning an identifier. It is addressable only by
    /// the [`ClientOrderId`] that was sent with it.
    ClientAssigned,
}

impl VenueOrderId {
    /// The venue's own identifier, or `None` if it never assigned one.
    ///
    /// Use this rather than matching on the variant when the question is identity: two states
    /// describe the same venue order only when both are `Some` and equal. Two `None`s prove
    /// nothing -- neither carries an identifier to compare -- so `None == None` must not be read
    /// as a match.
    pub fn assigned(&self) -> Option<&OrderId> {
        match self {
            Self::Assigned(id) => Some(id),
            Self::ClientAssigned => None,
        }
    }

    /// Whether this is the same venue order as `other`, as far as either can tell.
    ///
    /// `false` unless both carry a venue identifier and the two agree. A state with no venue
    /// identifier cannot be shown to be the same order as anything, including another such state.
    pub fn is_same_order_as(&self, other: &Self) -> bool {
        match (self.assigned(), other.assigned()) {
            (Some(lhs), Some(rhs)) => lhs == rhs,
            _ => false,
        }
    }

    /// Whether the venue has assigned an identifier that differs from `other`'s.
    ///
    /// This is the safe negation of [`Self::is_same_order_as`]: it is `true` only on positive
    /// evidence of a *different* order, never on the mere absence of an identifier. A caller
    /// rejecting an update must test this rather than `!is_same_order_as(..)`, which is also
    /// `true` when nothing is known.
    pub fn contradicts(&self, other: &Self) -> bool {
        match (self.assigned(), other.assigned()) {
            (Some(lhs), Some(rhs)) => lhs != rhs,
            _ => false,
        }
    }

    /// The identifier a *terminal* state records for this order.
    ///
    /// `Cancelled`, `Filled` and `Expired` carry a plain [`OrderId`]: they are records of an order
    /// that has ended, not handles for addressing one, and nothing compares them for identity. An
    /// order the venue never named has no identifier of its own to record, so it records `cid` --
    /// the client id it was addressed by throughout its life.
    pub fn or_client_id(&self, cid: &ClientOrderId) -> OrderId {
        match self {
            Self::Assigned(id) => id.clone(),
            Self::ClientAssigned => OrderId(cid.0.clone()),
        }
    }
}

impl From<OrderId> for VenueOrderId {
    fn from(id: OrderId) -> Self {
        Self::Assigned(id)
    }
}

impl std::fmt::Display for VenueOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Assigned(id) => id.fmt(f),
            // Deliberately not an empty string: this appears in diagnostics, where "no venue id"
            // and "a venue id that happens to be empty" must not look alike.
            Self::ClientAssigned => f.write_str("<client-assigned>"),
        }
    }
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display, From,
)]
pub struct StrategyId(pub SmolStr);

impl StrategyId {
    pub fn new<S: AsRef<str>>(id: S) -> Self {
        Self(SmolStr::new(id))
    }

    pub fn unknown() -> Self {
        // "unknown" is 7 bytes — always inline; new_inline avoids the heap-allocation
        // branch in SmolStr::new and is safe because the literal fits.
        Self(SmolStr::new_inline("unknown"))
    }

    /// The fixed `StrategyId` used for synthetic settlement trades generated by the engine
    /// on `ContractExpiry`. Using a `pub const` prevents bypassing any future validation.
    ///
    /// Note: conceptually this constant belongs to `rustrade::engine` (it is used exclusively
    /// by the engine's contract lifecycle logic). It lives here because `StrategyId` is
    /// defined in `rustrade-execution`; moving it to `rustrade::engine` would add a `smol_str`
    /// import to that crate for a single constant.
    pub const ENGINE_EXPIRY: StrategyId = StrategyId(SmolStr::new_static("__engine_expiry__"));
}

/// Opaque identifier for a tracked position.
///
/// In `OmsMode::Netting` this is always `"netting"` (at most one position per instrument).
/// In `OmsMode::Hedging` this is derived from the `ClientOrderId` of the opening order,
/// specified via [`crate::order::request::RequestOpen::position_id`] at order submission.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PositionId(pub SmolStr);

impl PositionId {
    pub fn new(id: impl Into<SmolStr>) -> Self {
        Self(id.into())
    }

    /// The fixed `PositionId` used for all netting-mode positions.
    pub const NETTING: PositionId = PositionId(SmolStr::new_static("netting"));
}

impl Default for PositionId {
    fn default() -> Self {
        Self::NETTING
    }
}

impl std::fmt::Display for PositionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn client_order_id_random_is_23_bytes() {
        let cid = ClientOrderId::random();
        assert_eq!(cid.0.len(), 23);
    }

    #[test]
    fn assigned_exposes_only_an_id_the_venue_gave() {
        assert_eq!(
            VenueOrderId::Assigned(OrderId::new("12345")).assigned(),
            Some(&OrderId::new("12345"))
        );
        assert_eq!(VenueOrderId::ClientAssigned.assigned(), None);
    }

    /// The distinction the whole type exists for: absence of an identifier is not an identifier
    /// that two orders can share. Deriving `PartialEq` makes two `ClientAssigned` values equal, so
    /// anything asking "same order?" must go through these methods rather than `==`.
    #[test]
    fn two_orders_the_venue_has_not_named_are_not_shown_to_be_the_same_order() {
        let unnamed = VenueOrderId::ClientAssigned;
        let also_unnamed = VenueOrderId::ClientAssigned;

        assert_eq!(unnamed, also_unnamed, "the derived equality still holds");
        assert!(!unnamed.is_same_order_as(&also_unnamed));
        // Nor are they evidence of being *different* orders, so neither test fires.
        assert!(!unnamed.contradicts(&also_unnamed));
    }

    #[test]
    fn identity_needs_two_agreeing_venue_ids_and_contradiction_needs_two_disagreeing_ones() {
        let a = VenueOrderId::Assigned(OrderId::new("A"));
        let a_again = VenueOrderId::Assigned(OrderId::new("A"));
        let b = VenueOrderId::Assigned(OrderId::new("B"));
        let unnamed = VenueOrderId::ClientAssigned;

        assert!(a.is_same_order_as(&a_again));
        assert!(!a.contradicts(&a_again));

        assert!(!a.is_same_order_as(&b));
        assert!(a.contradicts(&b));

        // An unnamed order proves nothing either way, in either direction.
        assert!(!a.is_same_order_as(&unnamed));
        assert!(!a.contradicts(&unnamed));
        assert!(!unnamed.is_same_order_as(&a));
        assert!(!unnamed.contradicts(&a));
    }

    #[test]
    fn a_terminal_record_falls_back_to_the_client_id() {
        let cid = ClientOrderId::new("cid-1");

        assert_eq!(
            VenueOrderId::Assigned(OrderId::new("12345")).or_client_id(&cid),
            OrderId::new("12345")
        );
        assert_eq!(
            VenueOrderId::ClientAssigned.or_client_id(&cid),
            OrderId::new("cid-1")
        );
    }

    /// `ClientAssigned` renders as a marker rather than an empty string, so a diagnostic cannot
    /// confuse "no venue id" with "a venue id that happens to be empty".
    #[test]
    fn display_names_an_unnamed_order_rather_than_rendering_nothing() {
        assert_eq!(
            VenueOrderId::Assigned(OrderId::new("12345")).to_string(),
            "12345"
        );
        assert_eq!(
            VenueOrderId::ClientAssigned.to_string(),
            "<client-assigned>"
        );
    }

    #[test]
    #[cfg(feature = "hyperliquid")]
    fn client_order_id_uuid_is_valid_uuid() {
        let cid = ClientOrderId::uuid();
        // UUID v4 string is 36 chars (8-4-4-4-12 with hyphens)
        assert_eq!(cid.0.len(), 36);
        // Verify it parses as a valid UUID
        uuid::Uuid::parse_str(&cid.0).expect("should be valid UUID");
    }
}
