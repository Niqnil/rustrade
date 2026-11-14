#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#![warn(
    unused,
    clippy::cognitive_complexity,
    unused_crate_dependencies,
    unused_extern_crates,
    clippy::unused_self,
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms
)]

//! # Rustrade-Pricing
//! Option pricing models: prices, implied volatility and Greeks for American and European options,
//! with discrete cash dividends and a continuous yield.
//!
//! The crate is pure computation: no I/O, no clock, no market-data access. The caller supplies
//! every input (spot, rate, dividends, the option's price) and gets the same answer for the same
//! inputs, so a backtest and a live system that feed it the same quotes get the same implied
//! volatilities.
//!
//! - [`OptionInputs`]: the contract and market inputs, everything but the volatility.
//! - [`OptionModel`]: the model trait. [`FiniteDifferenceModel`] implements it for American and
//!   European options; a caller can implement it for their own model.
//! - [`implied_volatility()`]: the volatility at which a model reproduces a price, or a typed
//!   [`ImpliedVolatilityError`] saying why none exists.
//! - [`mid_price`]: a quote's midpoint, or a typed [`QuoteError`] for a quote that has none.
//!
//! # Conventions
//! - Times are years of 365 days, counted to the second ([`year_fraction`]).
//! - Rates and yields are continuously compounded.
//! - Greeks are per unit of each input: vega per 1.0 of volatility, rho per 1.0 of rate, theta per
//!   year. Multiply vega and rho by 0.01 for per-point figures, and divide theta by 365 for a
//!   per-day figure.
//!
//! # Example
//! ```
//! use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};
//! use rustrade_pricing::{
//!     implied_volatility, mid_price, CashDividend, FiniteDifferenceModel, ImpliedVolatilityConfig,
//!     OptionInputs, OptionModel,
//! };
//!
//! // An American put struck at 100, three months out, with one $0.75 dividend in six weeks.
//! let inputs = OptionInputs::new(OptionKind::Put, OptionExercise::American, 101.0, 100.0, 0.25, 0.04)
//!     .with_dividends(vec![CashDividend::new(6.0 / 52.0, 0.75)]);
//! let model = FiniteDifferenceModel::default();
//!
//! let price = mid_price(Some(3.40), Some(3.50)).unwrap();
//! let volatility = implied_volatility(&model, &inputs, price, &ImpliedVolatilityConfig::default()).unwrap();
//! let valuation = model.value(&inputs, volatility).unwrap();
//!
//! assert!((valuation.price - price).abs() < 1e-6);
//! assert!(valuation.delta < 0.0 && valuation.delta > -1.0);
//! ```

mod black_scholes;

pub mod finite_difference;

pub mod implied_volatility;

pub mod inputs;

pub mod model;

pub mod quote;

pub use finite_difference::{FiniteDifferenceConfigError, FiniteDifferenceModel};
pub use implied_volatility::{ImpliedVolatilityConfig, ImpliedVolatilityError, implied_volatility};
pub use inputs::{CashDividend, OptionInputs, PricingError, year_fraction};
pub use model::{Greeks, OptionModel, Valuation};
pub use quote::{QuoteError, mid_price};
