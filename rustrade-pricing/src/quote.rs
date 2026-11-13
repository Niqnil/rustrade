//! Turning an option's bid and ask into the one price an implied-volatility solve takes.

/// Why a quote gives no price to solve from.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum QuoteError {
    /// Neither a bid nor an ask.
    #[error("quote has neither a bid nor an ask")]
    NoQuote,
    /// Only one side is quoted.
    #[error("quote is one-sided: bid {bid:?}, ask {ask:?}")]
    OneSided {
        /// The bid, if any.
        bid: Option<f64>,
        /// The ask, if any.
        ask: Option<f64>,
    },
    /// A side is negative or not finite.
    #[error("quote side must be finite and not negative: bid {bid}, ask {ask}")]
    InvalidSide {
        /// The bid.
        bid: f64,
        /// The ask.
        ask: f64,
    },
    /// The bid is zero: nobody will pay for the option, so the ask alone bounds its value.
    #[error("bid is zero (ask {ask})")]
    ZeroBid {
        /// The ask.
        ask: f64,
    },
    /// The bid is above the ask.
    #[error("quote is crossed: bid {bid} above ask {ask}")]
    Crossed {
        /// The bid.
        bid: f64,
        /// The ask.
        ask: f64,
    },
}

/// The midpoint of an option quote, or why the quote gives no price.
///
/// A quote with both sides, a positive bid and the bid at or below the ask (a locked quote is
/// accepted) gives `(bid + ask) / 2`. Anything else is refused with the reason, so a missing or
/// broken quote never reaches the solver as a made-up price.
///
/// This is one policy for choosing a price; a caller that wants another (the last trade, a
/// spread-weighted mid, a bid for a sale) passes that price to
/// [`implied_volatility`](crate::implied_volatility()) directly.
pub fn mid_price(bid: Option<f64>, ask: Option<f64>) -> Result<f64, QuoteError> {
    let (bid, ask) = match (bid, ask) {
        (Some(bid), Some(ask)) => (bid, ask),
        (None, None) => return Err(QuoteError::NoQuote),
        (bid, ask) => return Err(QuoteError::OneSided { bid, ask }),
    };
    let valid = |side: f64| side.is_finite() && side >= 0.0;
    if !valid(bid) || !valid(ask) {
        return Err(QuoteError::InvalidSide { bid, ask });
    }
    if bid > ask {
        return Err(QuoteError::Crossed { bid, ask });
    }
    if bid == 0.0 {
        return Err(QuoteError::ZeroBid { ask });
    }
    Ok(0.5 * (bid + ask))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn mid_of_a_two_sided_quote() {
        assert_eq!(mid_price(Some(1.10), Some(1.20)), Ok(0.5 * (1.10 + 1.20)));
        assert_eq!(mid_price(Some(2.0), Some(2.0)), Ok(2.0));
    }

    #[test]
    fn refuses_each_broken_quote() {
        assert_eq!(mid_price(None, None), Err(QuoteError::NoQuote));
        assert_eq!(
            mid_price(Some(1.0), None),
            Err(QuoteError::OneSided {
                bid: Some(1.0),
                ask: None
            })
        );
        assert_eq!(
            mid_price(None, Some(1.0)),
            Err(QuoteError::OneSided {
                bid: None,
                ask: Some(1.0)
            })
        );
        assert_eq!(
            mid_price(Some(0.0), Some(0.05)),
            Err(QuoteError::ZeroBid { ask: 0.05 })
        );
        assert_eq!(
            mid_price(Some(1.3), Some(1.2)),
            Err(QuoteError::Crossed { bid: 1.3, ask: 1.2 })
        );
        assert!(matches!(
            mid_price(Some(-0.1), Some(1.0)),
            Err(QuoteError::InvalidSide { .. })
        ));
        assert!(matches!(
            mid_price(Some(1.0), Some(f64::NAN)),
            Err(QuoteError::InvalidSide { .. })
        ));
        assert!(matches!(
            mid_price(Some(f64::INFINITY), Some(f64::INFINITY)),
            Err(QuoteError::InvalidSide { .. })
        ));
    }

    #[test]
    fn zero_bid_and_zero_ask_is_a_zero_bid() {
        assert_eq!(
            mid_price(Some(0.0), Some(0.0)),
            Err(QuoteError::ZeroBid { ask: 0.0 })
        );
    }
}
