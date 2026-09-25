//! OSI option contract symbols, in the unpadded spelling the options channel publishes:
//! `SPY260930C00700000`.
//!
//! The symbol is the underlying's root, then the expiry as `YYMMDD`, then `C` or `P`, then the
//! strike in thousandths as eight digits. The standard pads the root to six characters with spaces;
//! this provider does not, and neither does anything here.

use rust_decimal::{Decimal, prelude::ToPrimitive};
use rustrade_instrument::instrument::{
    kind::option::OptionKind, market_data::kind::MarketDataOptionContract,
};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Characters after the root: `YYMMDD`, `C`/`P`, and an eight-digit strike.
const SUFFIX_LEN: usize = 15;

/// The largest strike an eight-digit field of thousandths can carry.
const MAX_STRIKE_THOUSANDTHS: u64 = 99_999_999;

/// Spell `contract` on `root` as an OSI symbol.
///
/// The root is upper-cased, because the symbol is also the key a tick is resolved through and the
/// provider publishes it upper-case.
///
/// The expiry is the **UTC calendar date** of `contract.expiry`, the convention the other option
/// connectors in this crate follow. An instant set at the US close (20:00 or 21:00 UTC) or at
/// midnight UTC names the right date; one set late on the New York evening falls on the next UTC
/// day and names the wrong contract.
///
/// `None` when the strike cannot be spelled: OSI carries it in thousandths across eight digits, so
/// a strike with more than three decimal places, a non-positive one, or one of 100,000 or more has
/// no symbol.
pub(super) fn symbol(root: &str, contract: &MarketDataOptionContract) -> Option<SmolStr> {
    let thousandths = contract.strike.checked_mul(Decimal::ONE_THOUSAND)?;
    if !thousandths.fract().is_zero() || thousandths <= Decimal::ZERO {
        return None;
    }
    let thousandths = thousandths
        .to_u64()
        .filter(|t| *t <= MAX_STRIKE_THOUSANDTHS)?;

    let right = match contract.kind {
        OptionKind::Call => 'C',
        OptionKind::Put => 'P',
    };

    Some(format_smolstr!(
        "{}{}{right}{thousandths:08}",
        root.to_uppercase_smolstr(),
        contract.expiry.date_naive().format("%y%m%d"),
    ))
}

/// The underlying's root of an OSI symbol, or `None` if `symbol` is not one.
///
/// Only the fixed-width suffix is checked for shape. The root is whatever precedes it, provided it
/// is non-empty and holds no whitespace, so a root carrying a class suffix (`BRK.B`) or a digit
/// survives.
pub(super) fn root(symbol: &str) -> Option<&str> {
    let split = symbol.len().checked_sub(SUFFIX_LEN)?;
    let (root, suffix) = (symbol.get(..split)?, symbol.get(split..)?.as_bytes());

    let well_formed = !root.is_empty()
        && !root.contains(char::is_whitespace)
        && suffix[..6].iter().all(u8::is_ascii_digit)
        && matches!(suffix[6], b'C' | b'P')
        && suffix[7..].iter().all(u8::is_ascii_digit);

    well_formed.then_some(root)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_instrument::instrument::kind::option::OptionExercise;

    fn contract(kind: OptionKind, expiry: &str, strike: Decimal) -> MarketDataOptionContract {
        MarketDataOptionContract {
            kind,
            exercise: OptionExercise::American,
            expiry: expiry.parse::<DateTime<Utc>>().unwrap(),
            strike,
        }
    }

    #[test]
    fn a_contract_is_spelled_unpadded_in_thousandths() {
        let call = contract(OptionKind::Call, "2026-09-30T20:00:00Z", dec!(700));
        assert_eq!(symbol("spy", &call).unwrap(), "SPY260930C00700000");

        let put = contract(OptionKind::Put, "2026-09-25T00:00:00Z", dec!(327.5));
        assert_eq!(symbol("AAPL", &put).unwrap(), "AAPL260925P00327500");
    }

    /// Adjusted contracts carry strikes like 2.67, which OSI spells to the thousandth.
    #[test]
    fn a_fractional_strike_is_spelled_to_the_thousandth() {
        let call = contract(OptionKind::Call, "2026-10-16T20:00:00Z", dec!(2.67));
        assert_eq!(symbol("F", &call).unwrap(), "F261016C00002670");
    }

    #[test]
    fn a_strike_osi_cannot_carry_has_no_symbol() {
        for strike in [dec!(1.2345), dec!(0), dec!(-5), dec!(100000)] {
            let call = contract(OptionKind::Call, "2026-09-30T20:00:00Z", strike);
            assert_eq!(symbol("SPY", &call), None, "strike {strike}");
        }
    }

    /// The date is the expiry's UTC date: the close and midnight UTC both name it, while an instant
    /// on the New York evening crosses into the next UTC day.
    #[test]
    fn the_expiry_date_is_read_in_utc() {
        for expiry in ["2026-09-30T00:00:00Z", "2026-09-30T21:00:00Z"] {
            let call = contract(OptionKind::Call, expiry, dec!(700));
            assert_eq!(symbol("SPY", &call).unwrap(), "SPY260930C00700000");
        }

        let late = contract(OptionKind::Call, "2026-10-01T03:59:00Z", dec!(700));
        assert_eq!(symbol("SPY", &late).unwrap(), "SPY261001C00700000");
    }

    #[test]
    fn a_root_is_read_back_from_a_symbol() {
        assert_eq!(root("SPY260930C00700000"), Some("SPY"));
        assert_eq!(root("F261016P00002670"), Some("F"));
        assert_eq!(root("BRK.B261016C00450000"), Some("BRK.B"));
    }

    #[test]
    fn a_symbol_that_is_not_osi_has_no_root() {
        for symbol in [
            "SPY",
            "EUR/USD",
            "260930C00700000",
            "SPY260930X00700000",
            "SPY26093AC00700000",
            "SPY260930C0070000",
            "SPY 260930C00700000",
            "SPY (not an option contract: spot)",
        ] {
            assert_eq!(root(symbol), None, "{symbol}");
        }
    }

    #[test]
    fn a_spelled_symbol_reads_back_to_its_root() {
        let call = contract(OptionKind::Call, "2026-09-30T20:00:00Z", dec!(700));
        let spelled = symbol("qqq", &call).unwrap();
        assert_eq!(root(&spelled), Some("QQQ"));
    }
}
