//! How much more this account can borrow now: what a venue says about the account's own capacity
//! to borrow, as opposed to [`Shortability`](crate::shortability::Shortability), which says what
//! the venue has to lend.
//!
//! The two are kept apart because they are different kinds of fact. A [`Shortability`] describes
//! the instrument and can be recorded and replayed into a simulated venue, which subtracts the
//! account's own short from it. A [`BorrowCapacity`] is already net of this account's collateral and
//! existing borrows, so it changes with every fill, price move and borrow, and replaying it as the
//! lender's total would count the account's own short twice.
//!
//! [`Shortability`]: crate::shortability::Shortability

use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use serde::{Deserialize, Serialize};

/// How much of an asset this account can borrow now, as the venue reported it.
///
/// Returned by [`BorrowCapacityClient::fetch_borrow_capacity`]. Both amounts are in units of
/// [`asset`](Self::asset), and `None` means the venue did not say, never zero.
///
/// # Advisory
/// The values were true when the venue answered and are not reserved: a fill, a price move or
/// another borrow can change them before an order arrives, so an order sized to them must still
/// handle [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected).
///
/// [`BorrowCapacityClient::fetch_borrow_capacity`]: crate::client::BorrowCapacityClient::fetch_borrow_capacity
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BorrowCapacity {
    /// The asset that would be borrowed: an instrument's base asset for a sell, its quote asset for
    /// a buy.
    pub asset: AssetNameExchange,
    /// How much more of [`asset`](Self::asset) this account can borrow now: what its collateral
    /// and its limit allow, less what it has already borrowed, and no more than the venue has to
    /// lend.
    pub borrowable_now: Option<Decimal>,
    /// The most of [`asset`](Self::asset) the venue lets this account borrow in total, whatever its
    /// collateral. When [`borrowable_now`](Self::borrowable_now) is small, this says whether the
    /// account's limit is why.
    pub account_limit: Option<Decimal>,
}

impl BorrowCapacity {
    /// The capacity to borrow `asset`, with nothing known.
    pub fn new(asset: AssetNameExchange) -> Self {
        Self {
            asset,
            borrowable_now: None,
            account_limit: None,
        }
    }

    /// With how much more of the asset this account can borrow now.
    #[must_use]
    pub fn with_borrowable_now(mut self, borrowable_now: Decimal) -> Self {
        self.borrowable_now = Some(borrowable_now);
        self
    }

    /// With the most of the asset the venue lets this account borrow in total.
    #[must_use]
    pub fn with_account_limit(mut self, account_limit: Decimal) -> Self {
        self.account_limit = Some(account_limit);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn a_new_capacity_knows_only_its_asset() {
        let capacity = BorrowCapacity::new(AssetNameExchange::new("BTC"));

        assert_eq!(capacity.asset, AssetNameExchange::new("BTC"));
        assert_eq!(capacity.borrowable_now, None);
        assert_eq!(capacity.account_limit, None);
    }

    #[test]
    fn the_setters_fill_their_own_field() {
        let capacity = BorrowCapacity::new(AssetNameExchange::new("BTC"))
            .with_borrowable_now(dec!(1.5))
            .with_account_limit(dec!(10));

        assert_eq!(capacity.borrowable_now, Some(dec!(1.5)));
        assert_eq!(capacity.account_limit, Some(dec!(10)));
    }
}
