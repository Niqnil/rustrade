//! Closed-form European prices (Black-Scholes-Merton with a continuous yield).
//!
//! Internal: it gives the implied-volatility solver a cheap bracket, and the tests an oracle for
//! the finite-difference model. Every transcendental goes through `libm`, so results do not
//! depend on the platform's maths library.

use rustrade_instrument::instrument::kind::option::OptionKind;

/// The standard normal cumulative distribution function.
pub(crate) fn norm_cdf(x: f64) -> f64 {
    0.5 * libm::erfc(-x / std::f64::consts::SQRT_2)
}

/// The standard normal density.
#[cfg(test)]
pub(crate) fn norm_pdf(x: f64) -> f64 {
    libm::exp(-0.5 * x * x) / (2.0 * std::f64::consts::PI).sqrt()
}

/// The present value of the stock delivered `tau` years from now: the stock net of its continuous
/// `dividend_yield` and of the cash `dividends`, given as `(years from now, amount)`, each paid
/// before `tau`.
///
/// A cash dividend paid at `t` leaves the stock with that amount less, which would have grown with
/// the stock at `rate - dividend_yield` from `t` to `tau`; its present value is therefore
/// `amount·exp(-rate·t - dividend_yield·(tau - t))`, not just `amount·exp(-rate·t)`.
pub(crate) fn discounted_forward(
    stock: f64,
    tau: f64,
    rate: f64,
    dividend_yield: f64,
    dividends: impl IntoIterator<Item = (f64, f64)>,
) -> f64 {
    let paid: f64 = dividends
        .into_iter()
        .map(|(time, amount)| amount * libm::exp(-rate * time - dividend_yield * (tau - time)))
        .sum();
    stock * libm::exp(-dividend_yield * tau) - paid
}

/// The Black-Scholes-Merton price of a European option.
///
/// `spot`, `strike` and `time` must be positive and `volatility` positive. The caller
/// validates; this does not.
pub(crate) fn european_price(
    kind: OptionKind,
    spot: f64,
    strike: f64,
    time: f64,
    rate: f64,
    dividend_yield: f64,
    volatility: f64,
) -> f64 {
    let std_dev = volatility * time.sqrt();
    let discounted_spot = spot * libm::exp(-dividend_yield * time);
    let discounted_strike = strike * libm::exp(-rate * time);
    let d1 = (libm::log(spot / strike) + (rate - dividend_yield) * time) / std_dev + 0.5 * std_dev;
    let d2 = d1 - std_dev;
    let price = match kind {
        OptionKind::Call => discounted_spot * norm_cdf(d1) - discounted_strike * norm_cdf(d2),
        OptionKind::Put => discounted_strike * norm_cdf(-d2) - discounted_spot * norm_cdf(-d1),
    };
    price.max(0.0)
}

/// The volatility in `[low, high]` at which `price_at` gives `price`, by bisection, or `None` when
/// none does. `price_at` must increase with volatility.
///
/// Used only to narrow the bracket of the American solve, so a coarse, always-terminating method
/// is enough.
pub(crate) fn bisect_volatility(
    price_at: impl Fn(f64) -> f64,
    price: f64,
    low: f64,
    high: f64,
) -> Option<f64> {
    let gap = |volatility| price_at(volatility) - price;
    let (mut low, mut high) = (low, high);
    if gap(low) > 0.0 || gap(high) < 0.0 {
        return None;
    }
    // 60 halvings of a bracket at most a few units wide reach far below any tolerance used here.
    for _ in 0..60 {
        let mid = 0.5 * (low + high);
        if gap(mid) < 0.0 {
            low = mid;
        } else {
            high = mid;
        }
    }
    Some(high)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn norm_cdf_known_values() {
        assert_eq!(norm_cdf(0.0), 0.5);
        assert!((norm_cdf(1.0) - 0.841_344_746_068_542_9).abs() < 1e-15);
        assert!((norm_cdf(-1.959_963_984_540_054) - 0.025).abs() < 1e-15);
        assert!((norm_pdf(0.0) - 0.398_942_280_401_432_7).abs() < 1e-15);
    }

    #[test]
    fn european_price_matches_textbook_value() {
        // S = 42, K = 40, r = 10%, sigma = 20%, T = 0.5: call 4.7594, put 0.8086.
        let call = european_price(OptionKind::Call, 42.0, 40.0, 0.5, 0.1, 0.0, 0.2);
        let put = european_price(OptionKind::Put, 42.0, 40.0, 0.5, 0.1, 0.0, 0.2);
        assert!((call - 4.7594).abs() < 1e-4, "{call}");
        assert!((put - 0.8086).abs() < 1e-4, "{put}");
    }

    #[test]
    fn european_price_satisfies_put_call_parity_with_yield() {
        let (s, k, t, r, q, v) = (100.0, 110.0, 0.75, 0.04, 0.02, 0.3);
        let call = european_price(OptionKind::Call, s, k, t, r, q, v);
        let put = european_price(OptionKind::Put, s, k, t, r, q, v);
        let parity = s * libm::exp(-q * t) - k * libm::exp(-r * t);
        assert!((call - put - parity).abs() < 1e-12);
    }

    #[test]
    fn bisect_volatility_round_trips_and_refuses_out_of_range() {
        let price_at =
            |volatility| european_price(OptionKind::Put, 100.0, 95.0, 0.5, 0.03, 0.01, volatility);
        let solved = bisect_volatility(price_at, price_at(0.27), 1e-7, 4.0).unwrap();
        assert!((solved - 0.27).abs() < 1e-12, "{solved}");

        let call =
            |volatility| european_price(OptionKind::Call, 100.0, 95.0, 0.5, 0.03, 0.0, volatility);
        assert_eq!(bisect_volatility(call, 101.0, 1e-7, 4.0), None);
        assert_eq!(bisect_volatility(call, 1.0, 1e-7, 4.0), None);
    }

    #[test]
    fn bisect_volatility_takes_prices_at_the_ends_and_exact_hits() {
        let identity = |volatility| volatility;
        // A price at either end of the range is inside it.
        let low = bisect_volatility(identity, 0.25, 0.25, 1.0).unwrap();
        assert!((low - 0.25).abs() < 1e-15, "{low}");
        assert_eq!(bisect_volatility(identity, 1.0, 0.25, 1.0), Some(1.0));
        // The first midpoint, 0.625, is the answer: it is kept as the top and returned exactly.
        assert_eq!(bisect_volatility(identity, 0.625, 0.25, 1.0), Some(0.625));
    }

    #[test]
    fn discounted_forward_takes_out_the_yield_and_each_dividend() {
        let (stock, tau, r, q) = (100.0, 0.75, 0.05, 0.03);
        let plain = discounted_forward(stock, tau, r, q, []);
        assert!((plain - stock * (-q * tau).exp()).abs() < 1e-12);
        // A dividend at t costs its amount discounted at r to t and at q from t to tau.
        let with = discounted_forward(stock, tau, r, q, [(0.25, 2.0), (0.5, 1.0)]);
        let expected =
            plain - 2.0 * (-r * 0.25 - q * 0.5).exp() - 1.0 * (-r * 0.5 - q * 0.25).exp();
        assert!((with - expected).abs() < 1e-12, "{with} vs {expected}");
    }
}
