# rustrade-pricing

Option pricing models for Rust: prices, implied volatility and Greeks for American and European
options on a stock, with discrete cash dividends and a continuous yield.

The crate is pure computation: no I/O, no clock, no market data access and no async runtime. The
caller supplies every input and gets the same answer for the same inputs, so a backtest and a live
system that feed it the same quotes get the same implied volatilities. It is part of the
[rustrade](https://github.com/Niqnil/rustrade) workspace but does not depend on its engine, and
can be used on its own.

## Features

- **American and European calls and puts.** `FiniteDifferenceModel` solves the Black-Scholes
  equation on a Crank-Nicolson grid, with early exercise solved at every time step.
- **Dividends.** Discrete cash dividends (`CashDividend`) drop the stock by their amount at each
  ex-dividend instant. A continuous yield can be given as well.
- **Greeks.** Delta and gamma come from the grid, and theta from the Black-Scholes equation at the
  spot. Vega and rho re-solve with the input bumped on the same grid.
- **Implied volatility for any model.** `implied_volatility` finds the volatility at which a model
  reproduces a price. When none exists it returns an `ImpliedVolatilityError` that says why: the
  price is below the option's no-arbitrage floor, at or below the model's price at the lowest
  volatility searched (no volatility is determined), or above the upper bound, the search did not converge, the price or a setting is invalid, or the model
  cannot price the option.
- **Quotes.** `mid_price` turns a bid and ask into a midpoint, or a `QuoteError` for a quote that
  has none: no quote, one-sided, zero bid, crossed, or a negative or non-finite side.
- **Bring your own model.** Implement the `OptionModel` trait, and the implied-volatility solver
  and the default vega and rho work with it.
- **Deterministic.** Results are bit-identical on every platform for a given version of the
  pure-Rust `libm` crate, which supplies every transcendental function.

## Quick start

```toml
[dependencies]
rustrade-pricing = "0.22"
rustrade-instrument = "0.22" # OptionKind and OptionExercise
chrono = "0.4"               # only for year_fraction's timestamps
```

```rust
use chrono::{TimeZone, Utc};
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};
use rustrade_pricing::{
    implied_volatility, year_fraction, CashDividend, FiniteDifferenceModel,
    ImpliedVolatilityConfig, OptionInputs, OptionModel,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc.with_ymd_and_hms(2026, 11, 16, 15, 0, 0).unwrap();
    let ex_dividend = Utc.with_ymd_and_hms(2026, 12, 10, 14, 30, 0).unwrap();
    let expiry = Utc.with_ymd_and_hms(2027, 1, 15, 21, 0, 0).unwrap();

    // An American call struck at 100 on a stock at 102, with a 4% rate and one $0.50 dividend
    // before expiry.
    let inputs = OptionInputs::new(
        OptionKind::Call,
        OptionExercise::American,
        102.0,
        100.0,
        year_fraction(now, expiry),
        0.04,
    )
    .with_dividends(vec![CashDividend::new(year_fraction(now, ex_dividend), 0.50)]);
    let model = FiniteDifferenceModel::default();

    // Price and Greeks at 25% volatility.
    let greeks = model.greeks(&inputs, 0.25)?;
    println!(
        "price {:.4}, delta {:.4}, vega per vol point {:.4}",
        greeks.price,
        greeks.delta,
        greeks.vega * 0.01
    );

    // The volatility at which the model reproduces a market price.
    let volatility = implied_volatility(&model, &inputs, 6.10, &ImpliedVolatilityConfig::default())?;
    println!("implied volatility {volatility:.4}");

    Ok(())
}
```

## Conventions

- Times are years of 365 days, counted to the second (`year_fraction`).
- Rates and yields are continuously compounded. Volatility is a decimal: 0.25 is 25%.
- Greeks are per unit of each input: vega per 1.0 of volatility, rho per 1.0 of rate, theta per
  year. Multiply vega and rho by 0.01 for per-point figures, and divide theta by 365 for a
  per-day figure.

## Accuracy and cost

The default grid has 400 steps in the stock price and 200 in time. On it, listed equity options
from a few hours to a year out at 20–40% volatility price within about 1e-5 of the spot of a
4000 × 4000 grid's value: a tenth of a cent on a $100 stock. Long-dated, high-volatility options
are less accurate (about 5e-5 of the spot for two years at 60%).

Where an option's price barely moves with volatility (deep in the money, or very close to expiry),
the grid's error can outweigh vega. There a price can fall slightly as volatility rises, and an
implied volatility means little. A price at or below the model's price at the lowest volatility searched is
reported as `AtIntrinsic`.

The cost of one price grows with the grid's size. An implied-volatility solve prices about five to
fifteen times, and `greeks` five times. `FiniteDifferenceModel::new` sets the grid: finer is
slower and more accurate. Different grids give slightly different prices and implied
volatilities, so use one configuration everywhere a result must match, for example in a backtest
and live.

A different `libm` version, or a new release of this crate, may change the last digits. Build
every system whose results must match from one lock file.

## Scope

The crate prices; choosing the inputs is up to you. It does not fetch market data, forecast
dividends, build rate curves or fit volatility surfaces.

## Documentation

API documentation is on [docs.rs](https://docs.rs/rustrade-pricing). See the
[workspace README](https://github.com/Niqnil/rustrade#readme) for the rest of rustrade.

## License

MIT License. See [LICENSE](LICENSE).
