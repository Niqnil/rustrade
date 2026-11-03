//! The loans this client's orders took, held to link Binance's charge on a new loan to the order
//! that took it.
//!
//! Binance's interest history names no order or loan: an `ON_BORROW` record gives the asset, the
//! principal and the hour the loan was taken in. Its principal is the amount the order's answer
//! reports borrowing (`marginBuyBorrowAmount`), so a record is linked to the one loan of that
//! asset and exact principal taken in its hour, and to no loan when several fit.

use crate::cash_flow::CashFlowPeriod;
use crate::margin::OrderLoan;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use std::sync::Arc;

/// How many loans a client holds. Past it, the oldest by loan time is dropped.
///
/// An order that borrows costs 1500 of the account's order weight, so a client places at most a
/// few a minute: this holds about a day of them at that rate, and Binance posts a charge on a new
/// loan within hours.
pub(super) const MAX_LOANS: usize = 4096;

/// The loans held, shared by every clone of the client.
pub(super) type SharedLoans = Arc<parking_lot::Mutex<Loans>>;

/// The loans this client's orders took, oldest first by insertion, at most [`MAX_LOANS`].
#[derive(Debug, Default)]
pub(super) struct Loans {
    loans: Vec<OrderLoan>,
}

/// What an `ON_BORROW` record links to among the held loans.
#[derive(Debug, PartialEq)]
pub(super) enum LoanLink<'a> {
    /// No held loan fits.
    None,
    /// Exactly one does.
    One(&'a OrderLoan),
    /// This many do, so the record is linked to none.
    Ambiguous(usize),
}

impl Loans {
    pub(super) fn shared() -> SharedLoans {
        Arc::new(parking_lot::Mutex::new(Self::default()))
    }

    /// Holds `loan`, in place of a held loan of the same order on the same instrument. Returns
    /// whether it is held: past [`MAX_LOANS`] the oldest loan by time is dropped, which is `loan`
    /// itself when it is the oldest.
    pub(super) fn insert(&mut self, loan: OrderLoan) -> bool {
        if let Some(held) = self.loans.iter_mut().find(|held| {
            held.loan.order_id == loan.loan.order_id && held.instrument == loan.instrument
        }) {
            *held = loan;
            return true;
        }
        if self.loans.len() < MAX_LOANS {
            self.loans.push(loan);
            return true;
        }
        let Some((oldest, _)) = self
            .loans
            .iter()
            .enumerate()
            .min_by_key(|(_, held)| held.loan.time)
        else {
            return false;
        };
        if self.loans[oldest].loan.time > loan.loan.time {
            return false;
        }
        self.loans.remove(oldest);
        self.loans.push(loan);
        true
    }

    /// The held loan an `ON_BORROW` record of `asset` and `principal`, for the hour `period`, is
    /// the charge on: one of `asset` whose principal is exactly `principal`, taken in `period`,
    /// and, for a record read from an isolated pair, on `pair`.
    ///
    /// A pure function of the record and the held loans, so reading a record again links it
    /// again.
    pub(super) fn link(
        &self,
        asset: &AssetNameExchange,
        principal: Decimal,
        period: CashFlowPeriod,
        pair: Option<&InstrumentNameExchange>,
    ) -> LoanLink<'_> {
        let mut fits = self.loans.iter().filter(|held| {
            held.borrowed.asset == *asset
                && held.borrowed.amount == principal
                && period.contains(held.loan.time)
                && pair.is_none_or(|pair| held.instrument == *pair)
        });
        match (fits.next(), fits.next()) {
            (None, _) => LoanLink::None,
            (Some(one), None) => LoanLink::One(one),
            (Some(_), Some(_)) => LoanLink::Ambiguous(2 + fits.count()),
        }
    }

    /// How many loans are held.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.loans.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use crate::cash_flow::CashFlowLoan;
    use crate::margin::MarginBorrow;
    use crate::order::id::{ClientOrderId, OrderId};
    use chrono::{DateTime, TimeDelta, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use smol_str::format_smolstr;

    fn at(minute: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 2, 10, 0, 0).unwrap() + TimeDelta::minutes(minute)
    }

    fn hour_from(minute: i64) -> CashFlowPeriod {
        CashFlowPeriod::new(at(minute), at(minute + 60)).unwrap()
    }

    fn loan(
        order: u32,
        pair: &str,
        asset: &str,
        amount: Decimal,
        time: DateTime<Utc>,
    ) -> OrderLoan {
        OrderLoan::new(
            InstrumentNameExchange::new(pair),
            MarginBorrow::new(AssetNameExchange::new(asset), amount),
            CashFlowLoan::new(
                time,
                OrderId(format_smolstr!("{order}")),
                ClientOrderId(format_smolstr!("cid-{order}")),
            ),
        )
    }

    fn usdt() -> AssetNameExchange {
        AssetNameExchange::new("USDT")
    }

    #[test]
    fn a_record_links_to_the_one_loan_of_its_asset_and_exact_principal_in_its_hour() {
        let mut loans = Loans::default();
        let wanted = loan(1, "BTCUSDT", "USDT", dec!(6.23292076), at(5));
        loans.insert(wanted.clone());
        // Each differs from the record in one respect only.
        loans.insert(loan(2, "BTCUSDT", "BTC", dec!(6.23292076), at(5)));
        loans.insert(loan(3, "BTCUSDT", "USDT", dec!(6.23292077), at(5)));
        loans.insert(loan(4, "BTCUSDT", "USDT", dec!(6.23292076), at(60)));
        loans.insert(loan(5, "BTCUSDT", "USDT", dec!(6.23292076), at(-1)));

        assert_eq!(
            loans.link(&usdt(), dec!(6.23292076), hour_from(0), None),
            LoanLink::One(&wanted)
        );
    }

    #[test]
    fn a_principal_is_compared_by_value_not_by_its_written_scale() {
        let mut loans = Loans::default();
        let wanted = loan(1, "BTCUSDT", "USDT", dec!(6.2329), at(0));
        loans.insert(wanted.clone());

        assert_eq!(
            loans.link(&usdt(), dec!(6.23290000), hour_from(0), None),
            LoanLink::One(&wanted)
        );
    }

    #[test]
    fn two_fitting_loans_link_the_record_to_neither() {
        let mut loans = Loans::default();
        for order in 1..=3 {
            loans.insert(loan(order, "BTCUSDT", "USDT", dec!(6), at(order.into())));
        }

        assert_eq!(
            loans.link(&usdt(), dec!(6), hour_from(0), None),
            LoanLink::Ambiguous(3)
        );
    }

    #[test]
    fn a_record_read_from_an_isolated_pair_links_only_to_a_loan_on_that_pair() {
        let mut loans = Loans::default();
        let wanted = loan(1, "BTCUSDT", "USDT", dec!(6), at(5));
        loans.insert(wanted.clone());
        loans.insert(loan(2, "ETHUSDT", "USDT", dec!(6), at(5)));
        let pair = InstrumentNameExchange::new("BTCUSDT");

        assert_eq!(
            loans.link(&usdt(), dec!(6), hour_from(0), Some(&pair)),
            LoanLink::One(&wanted)
        );
        assert_eq!(
            loans.link(&usdt(), dec!(6), hour_from(0), None),
            LoanLink::Ambiguous(2)
        );
    }

    #[test]
    fn linking_is_repeatable_and_consumes_nothing() {
        let mut loans = Loans::default();
        let wanted = loan(1, "BTCUSDT", "USDT", dec!(6), at(5));
        loans.insert(wanted.clone());

        for _ in 0..2 {
            assert_eq!(
                loans.link(&usdt(), dec!(6), hour_from(0), None),
                LoanLink::One(&wanted)
            );
        }
        assert_eq!(loans.len(), 1);
    }

    #[test]
    fn a_loan_of_an_order_already_held_replaces_it() {
        let mut loans = Loans::default();
        loans.insert(loan(1, "BTCUSDT", "USDT", dec!(6), at(5)));
        let replacement = loan(1, "BTCUSDT", "USDT", dec!(6), at(6));
        assert!(loans.insert(replacement.clone()));
        // The same venue order id on another pair is another order.
        assert!(loans.insert(loan(1, "ETHUSDT", "USDT", dec!(6), at(5))));

        assert_eq!(loans.len(), 2);
        assert_eq!(
            loans.link(
                &usdt(),
                dec!(6),
                hour_from(0),
                Some(&InstrumentNameExchange::new("BTCUSDT"))
            ),
            LoanLink::One(&replacement)
        );
    }

    #[test]
    fn past_the_limit_the_oldest_loan_by_time_is_dropped() {
        let mut loans = Loans::default();
        // Inserted newest first, so the oldest by time is the last inserted.
        for order in 0..MAX_LOANS {
            let minute = i64::try_from(MAX_LOANS - order).unwrap();
            assert!(loans.insert(loan(
                u32::try_from(order).unwrap(),
                "BTCUSDT",
                "USDT",
                dec!(6),
                at(minute)
            )));
        }
        let oldest = at(1);
        assert_eq!(
            loans.link(
                &usdt(),
                dec!(6),
                CashFlowPeriod::new(oldest, at(2)).unwrap(),
                None
            ),
            LoanLink::One(&loan(
                u32::try_from(MAX_LOANS - 1).unwrap(),
                "BTCUSDT",
                "USDT",
                dec!(6),
                oldest
            ))
        );

        // Older than every held loan: not held.
        assert!(!loans.insert(loan(9_000, "BTCUSDT", "USDT", dec!(7), at(0))));
        assert_eq!(
            loans.link(&usdt(), dec!(7), hour_from(-30), None),
            LoanLink::None
        );

        // Newer: held, and the oldest is dropped.
        let newest = loan(9_001, "BTCUSDT", "USDT", dec!(8), at(10_000));
        assert!(loans.insert(newest.clone()));
        assert_eq!(loans.len(), MAX_LOANS);
        assert_eq!(
            loans.link(
                &usdt(),
                dec!(6),
                CashFlowPeriod::new(oldest, at(2)).unwrap(),
                None
            ),
            LoanLink::None
        );
        assert_eq!(
            loans.link(&usdt(), dec!(8), hour_from(10_000), None),
            LoanLink::One(&newest)
        );
    }
}
