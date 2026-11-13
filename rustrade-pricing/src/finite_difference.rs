//! A Crank-Nicolson finite-difference model for American and European options on a stock with
//! discrete cash dividends and a continuous yield.
//!
//! # Method
//! The model solves the Black-Scholes equation backward from expiry on a uniform grid in
//! `y = ln(S) + c·τ`, where `τ` is the time left to expiry and `c` a shift chosen per grid. Each
//! node's stock price is `exp(y - c·τ)`. The shift trades two errors:
//! - With `c = -q` the stock price is a steady solution on the grid, so its value carries no time
//!   stepping error however large `σ²τ`. This is the choice whenever the grid allows it.
//! - At low volatility the drift left in `y`, `r - σ²/2` there, would dominate the diffusion and
//!   make the scheme oscillate. The shift then absorbs as much of the drift as keeps the scheme
//!   monotone (an M-matrix), down to removing all of it as the volatility goes to zero.
//!
//! Both the diffusion and drift terms use coefficients fitted so the scheme is exact for the stock
//! price and for constants. The deep in- and out-of-the-money values, which combine the two, then
//! carry no grid error.
//!
//! - **Grid.** Centred on the spot, `std_devs` standard deviations of `ln(S)` over the option's life
//!   each side (at least a quarter of the dividends' total drop, so a low volatility does not
//!   leave the spot at an edge), widened by the drift left in `y` over the option's life and
//!   downward by the dividends' drops, with the strike placed on a node when it falls inside. Values beyond the
//!   edges come from the option's deep in- or out-of-the-money limit.
//! - **Time stepping.** Crank-Nicolson, with the first `damping_steps` steps each replaced by two
//!   implicit half steps, which damps the oscillation the payoff's kink at the strike would
//!   otherwise cause. Ex-dividend instants are step boundaries.
//! - **Dividends.** At each ex-dividend instant the stock drops by the cash amount: the value just
//!   before is the value just after at the lower price, interpolated on the grid. A stock worth
//!   less than the dividend pays what it is worth and falls to zero.
//! - **Early exercise.** An American option's value is floored at its exercise value at every step
//!   and just before every ex-dividend instant. With a non-negative yield the exercise region is
//!   one interval at the in-the-money end, and the floor is applied inside each step's linear
//!   solve (the Brennan-Schwartz method), which solves the early-exercise problem exactly at each
//!   step. With a negative yield it is applied after each step.
//! - **Sensitivities.** Price, delta and gamma come from the grid at the spot. Theta comes from the
//!   Black-Scholes equation at that point (zero where immediate exercise is optimal). Vega and rho
//!   bump the volatility and rate on the same grid, so the bump measures the input's effect and
//!   not a change of grid.

use crate::black_scholes::discounted_forward;
use crate::inputs::{OptionInputs, PricingError, payoff, validate_volatility};
use crate::model::{OptionModel, RATE_BUMP, Valuation, volatility_bump};
use rustrade_instrument::instrument::kind::option::{OptionExercise, OptionKind};

/// Fewest space steps a model accepts: enough for the three nodes the spot's value is read from
/// to lie inside the grid.
pub const MIN_SPACE_STEPS: usize = 4;

/// Default space steps.
pub const DEFAULT_SPACE_STEPS: usize = 400;

/// Default time steps.
pub const DEFAULT_TIME_STEPS: usize = 200;

/// Default number of Crank-Nicolson steps replaced by implicit half steps at expiry.
pub const DEFAULT_DAMPING_STEPS: usize = 2;

/// Default grid half-width, in standard deviations of `ln(S)` over the option's life.
pub const DEFAULT_STD_DEVS: f64 = 5.0;

/// A finite-difference grid could not be configured as asked.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum FiniteDifferenceConfigError {
    /// Fewer than [`MIN_SPACE_STEPS`] space steps.
    #[error("space_steps must be at least {MIN_SPACE_STEPS}, got {0}")]
    TooFewSpaceSteps(usize),
    /// Zero time steps.
    #[error("time_steps must be at least 1")]
    NoTimeSteps,
    /// A grid half-width that is not finite and positive.
    #[error("std_devs must be finite and positive, got {0}")]
    InvalidStdDevs(f64),
}

/// A Crank-Nicolson finite-difference model for American and European options, with discrete cash
/// dividends and a continuous yield. See the [module docs](self) for the method.
///
/// # Accuracy and cost
/// With the defaults ([`DEFAULT_SPACE_STEPS`] × [`DEFAULT_TIME_STEPS`]), listed equity options
/// from a few hours to a year out at 20–40% volatility price within about 1e-5 of the spot of a
/// 4000 × 4000 grid's value: a tenth of a cent on a $100 stock. Long-dated, high-volatility options
/// are less accurate (about 5e-5 of the spot for two years at 60%). Delta is within about 5e-5.
/// The cost of one price grows with `space_steps × time_steps`; an implied-volatility solve prices
/// around five to fifteen times. Finer grids
/// cost more and converge toward the exact value; coarser ones are faster and less accurate.
///
/// Results depend on the grid: two models with different settings give slightly different prices
/// and implied volatilities for the same inputs. Use one configuration everywhere a result must
/// match, for example in a backtest and live.
///
/// # Determinism
/// The same inputs, volatility and settings give bit-identical results on every platform: the
/// arithmetic is IEEE 754 double precision in a fixed order, and every transcendental function
/// comes from `libm` rather than the platform's maths library.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FiniteDifferenceModel {
    space_steps: usize,
    time_steps: usize,
    damping_steps: usize,
    std_devs: f64,
}

impl Default for FiniteDifferenceModel {
    fn default() -> Self {
        Self {
            space_steps: DEFAULT_SPACE_STEPS,
            time_steps: DEFAULT_TIME_STEPS,
            damping_steps: DEFAULT_DAMPING_STEPS,
            std_devs: DEFAULT_STD_DEVS,
        }
    }
}

impl FiniteDifferenceModel {
    /// A model with `space_steps` intervals in the stock-price dimension and about `time_steps` in
    /// time (each ex-dividend instant can add one), with the default damping and grid width.
    pub fn new(space_steps: usize, time_steps: usize) -> Result<Self, FiniteDifferenceConfigError> {
        if space_steps < MIN_SPACE_STEPS {
            return Err(FiniteDifferenceConfigError::TooFewSpaceSteps(space_steps));
        }
        if time_steps == 0 {
            return Err(FiniteDifferenceConfigError::NoTimeSteps);
        }
        Ok(Self {
            space_steps,
            time_steps,
            ..Self::default()
        })
    }

    /// Sets how many of the first time steps from expiry are each replaced by two implicit half
    /// steps. Zero gives plain Crank-Nicolson, which can oscillate near the strike close to
    /// expiry.
    pub fn with_damping_steps(mut self, damping_steps: usize) -> Self {
        self.damping_steps = damping_steps;
        self
    }

    /// Sets the grid's half-width, in standard deviations of `ln(S)` over the option's life.
    pub fn with_std_devs(mut self, std_devs: f64) -> Result<Self, FiniteDifferenceConfigError> {
        if !std_devs.is_finite() || std_devs <= 0.0 {
            return Err(FiniteDifferenceConfigError::InvalidStdDevs(std_devs));
        }
        self.std_devs = std_devs;
        Ok(self)
    }

    /// Intervals in the stock-price dimension.
    pub fn space_steps(&self) -> usize {
        self.space_steps
    }

    /// Target intervals in time.
    pub fn time_steps(&self) -> usize {
        self.time_steps
    }

    /// Time steps at expiry replaced by implicit half steps.
    pub fn damping_steps(&self) -> usize {
        self.damping_steps
    }

    /// Grid half-width, in standard deviations of `ln(S)` over the option's life.
    pub fn std_devs(&self) -> f64 {
        self.std_devs
    }

    /// Lays out the grid for `volatility` and solves at `volatility` and `rate` on it.
    fn solve_at(&self, problem: &Problem, volatility: f64, rate: f64, grid: &Grid) -> Valuation {
        Solver {
            problem,
            grid,
            volatility,
            rate,
            model: self,
        }
        .solve()
    }
}

impl OptionModel for FiniteDifferenceModel {
    fn value(&self, inputs: &OptionInputs, volatility: f64) -> Result<Valuation, PricingError> {
        let problem = Problem::new(inputs, volatility)?;
        let grid = Grid::new(&problem, volatility, self);
        Ok(self.solve_at(&problem, volatility, problem.rate, &grid))
    }

    /// Central difference over volatility ± 1e-4 (or half the volatility, when smaller), both
    /// solved on the grid laid out for `volatility`.
    fn vega(&self, inputs: &OptionInputs, volatility: f64) -> Result<f64, PricingError> {
        let problem = Problem::new(inputs, volatility)?;
        let grid = Grid::new(&problem, volatility, self);
        let bump = volatility_bump(volatility);
        let up = self.solve_at(&problem, volatility + bump, problem.rate, &grid);
        let down = self.solve_at(&problem, volatility - bump, problem.rate, &grid);
        Ok((up.price - down.price) / (2.0 * bump))
    }

    /// Central difference over rate ± 1e-4, holding the dividend yield fixed, both solved on the
    /// grid laid out for `volatility`.
    fn rho(&self, inputs: &OptionInputs, volatility: f64) -> Result<f64, PricingError> {
        let problem = Problem::new(inputs, volatility)?;
        let grid = Grid::new(&problem, volatility, self);
        let up = self.solve_at(&problem, volatility, problem.rate + RATE_BUMP, &grid);
        let down = self.solve_at(&problem, volatility, problem.rate - RATE_BUMP, &grid);
        Ok((up.price - down.price) / (2.0 * RATE_BUMP))
    }
}

/// Validated inputs, with the dividends reduced to the drops the solver applies.
#[derive(Debug)]
struct Problem {
    kind: OptionKind,
    american: bool,
    spot: f64,
    strike: f64,
    expiry: f64,
    rate: f64,
    dividend_yield: f64,
    /// Ex-dividend drops as `(time from valuation, amount)`, by time, one per distinct instant.
    dividends: Vec<(f64, f64)>,
}

impl Problem {
    fn new(inputs: &OptionInputs, volatility: f64) -> Result<Self, PricingError> {
        inputs.validate()?;
        validate_volatility(volatility)?;
        let mut dividends: Vec<(f64, f64)> = Vec::new();
        for dividend in inputs.dividends_before_expiry() {
            match dividends.last_mut() {
                Some((time, amount)) if *time == dividend.time => *amount += dividend.amount,
                _ => dividends.push((dividend.time, dividend.amount)),
            }
        }
        Ok(Self {
            kind: inputs.kind,
            american: inputs.exercise == OptionExercise::American,
            spot: inputs.spot,
            strike: inputs.strike,
            expiry: inputs.time_to_expiry,
            rate: inputs.rate,
            dividend_yield: inputs.dividend_yield,
            dividends,
        })
    }
}

/// A uniform grid in `y = ln(S) + shift·τ`, with each node's `exp(y)` precomputed.
#[derive(Debug)]
struct Grid {
    y_min: f64,
    dy: f64,
    /// The `c` in `y = ln(S) + c·τ`; see the module docs.
    shift: f64,
    exp_y: Vec<f64>,
}

impl Grid {
    fn new(problem: &Problem, volatility: f64, model: &FiniteDifferenceModel) -> Self {
        let expiry = problem.expiry;
        let variance = volatility * volatility;
        let drift = problem.rate - problem.dividend_yield - 0.5 * variance;
        let half_width = model.std_devs * volatility * expiry.sqrt();

        // The dividends move the stock down by these log amounts along its expected path; widen
        // the grid below by the same, so values just after an ex-dividend instant are read from
        // the grid rather than from its edge.
        let mut dividend_drop = 0.0;
        let mut stock = problem.spot;
        let mut previous = 0.0;
        for &(time, amount) in &problem.dividends {
            let before =
                stock * libm::exp((problem.rate - problem.dividend_yield) * (time - previous));
            // A dividend that would take the stock to (nearly) zero widens the grid by a capped
            // amount: below that, the edge's limit value is used.
            let after = (before - amount).max(before * MIN_DIVIDEND_SURVIVAL);
            dividend_drop += libm::log(before / after);
            stock = after;
            previous = time;
        }

        // The drift left in y. The ideal, which makes the stock price a steady solution, is
        // r - σ²/2 (shift -q). It is capped so the scheme stays monotone: the fitted coefficients
        // form an M-matrix while |drift| <= σ²/(2·tanh(dy/2)), checked at the widest grid this
        // can produce. It is also capped at one half-width over the option's life, so following it
        // at most doubles one side of the grid.
        // Keep the spot well inside the grid even when the volatility is too low for the
        // standard deviations to give it room: the edges carry only limit values, and the
        // dividends' drops otherwise set the grid's spacing.
        let margin = half_width.max(MIN_MARGIN_SHARE * dividend_drop);
        let steps = model.space_steps as f64;
        let widest_dy = (2.0 * margin + half_width + dividend_drop) / steps;
        let monotone = variance / (2.0 * libm::tanh(0.5 * widest_dy));
        let cap = monotone.min(half_width / expiry);
        let residual = (problem.rate - 0.5 * variance).clamp(-cap, cap);
        let shift = drift - residual;

        // Paths from the spot drift by `residual · expiry` in y over the option's life.
        let y_spot = libm::log(problem.spot) + shift * expiry;
        let travel = residual * expiry;
        let mut y_min = y_spot + travel.min(0.0) - margin - dividend_drop;
        let y_max = y_spot + travel.max(0.0) + margin;
        let dy = (y_max - y_min) / steps;

        // Put the strike on a node when it is inside the grid: the payoff's kink then sits on the
        // grid rather than between two nodes.
        let y_strike = libm::log(problem.strike);
        if y_strike > y_min && y_strike < y_max {
            let node = ((y_strike - y_min) / dy).round();
            y_min += y_strike - (y_min + node * dy);
        }

        let exp_y = (0..=model.space_steps)
            .map(|node| libm::exp(y_min + node as f64 * dy))
            .collect();
        Self {
            y_min,
            dy,
            shift,
            exp_y,
        }
    }

    fn len(&self) -> usize {
        self.exp_y.len()
    }
}

/// The share of its pre-dividend price below which the grid stops widening for a dividend.
const MIN_DIVIDEND_SURVIVAL: f64 = 1e-6;

/// The least room each side of the spot, as a share of the dividends' total log drop.
const MIN_MARGIN_SHARE: f64 = 0.25;

/// One backward solve at a given volatility and rate on a given grid.
struct Solver<'a> {
    problem: &'a Problem,
    grid: &'a Grid,
    volatility: f64,
    rate: f64,
    model: &'a FiniteDifferenceModel,
}

impl Solver<'_> {
    /// The drift of `y` at this solve's volatility and rate: what the grid's shift leaves of
    /// `r - q - σ²/2`.
    fn residual_drift(&self) -> f64 {
        self.rate
            - self.problem.dividend_yield
            - 0.5 * self.volatility * self.volatility
            - self.grid.shift
    }

    /// The factor turning a node's `exp(y)` into its stock price at `tau` years before expiry.
    fn stock_factor(&self, tau: f64) -> f64 {
        libm::exp(-self.grid.shift * tau)
    }

    fn exercise_value(&self, stock: f64) -> f64 {
        payoff(self.problem.kind, stock, self.problem.strike)
    }

    /// The option's limit value deep in or out of the money at `tau` years before expiry, used
    /// beyond the grid's edges: its discounted forward intrinsic value, and for an American option
    /// at least its exercise value.
    fn edge_value(&self, tau: f64, stock: f64) -> f64 {
        let problem = self.problem;
        // Dividends still to come before expiry are those whose instant is less than `tau` before
        // expiry, each `tau - dividend_tau` years from this instant. A stock cannot pay out more
        // than it is worth, so the forward is floored at zero.
        let pending = problem
            .dividends
            .iter()
            .map(|&(time, amount)| (problem.expiry - time, amount))
            .filter(|&(dividend_tau, _)| dividend_tau < tau)
            .map(|(dividend_tau, amount)| (tau - dividend_tau, amount));
        let forward =
            discounted_forward(stock, tau, self.rate, problem.dividend_yield, pending).max(0.0);
        let strike = problem.strike * libm::exp(-self.rate * tau);
        let european = match problem.kind {
            OptionKind::Call => (forward - strike).max(0.0),
            OptionKind::Put => (strike - forward).max(0.0),
        };
        if problem.american {
            european.max(self.exercise_value(stock))
        } else {
            european
        }
    }

    /// The option's value when the stock is worth nothing at `tau` years before expiry.
    fn value_at_zero(&self, tau: f64) -> f64 {
        match self.problem.kind {
            OptionKind::Call => 0.0,
            OptionKind::Put if self.problem.american => self.problem.strike,
            OptionKind::Put => self.problem.strike * libm::exp(-self.rate * tau),
        }
    }

    fn solve(&self) -> Valuation {
        let problem = self.problem;
        let grid = self.grid;
        let n = grid.len() - 1;
        // On the grid, exp(y)'s second difference is exp(y)·4·sinh²(dy/2) and its central
        // difference exp(y)·2·sinh(dy), not exp(y)·dy² and exp(y)·2·dy. Dividing by those makes
        // the scheme exact for the stock price itself (and for constants).
        let half_sinh = libm::sinh(0.5 * grid.dy);
        let operator = Operator {
            diffusion: 0.5 * self.volatility * self.volatility / (4.0 * half_sinh * half_sinh),
            convection: self.residual_drift() / (2.0 * libm::sinh(grid.dy)),
            rate: self.rate,
        };

        // At expiry the value is the payoff, and the stock price at each node is exp(y).
        let mut values: Vec<f64> = grid
            .exp_y
            .iter()
            .map(|&stock| self.exercise_value(stock))
            .collect();
        let mut scratch = Scratch::new(n);

        // Segment ends in tau: each ex-dividend instant (latest first, as tau runs backward), then
        // the valuation instant.
        let mut stops: Vec<(f64, Option<f64>)> = problem
            .dividends
            .iter()
            .rev()
            .map(|&(time, amount)| (problem.expiry - time, Some(amount)))
            .collect();
        stops.push((problem.expiry, None));

        let mut tau = 0.0;
        let mut steps_taken = 0;
        for (stop, dividend) in stops {
            let length = stop - tau;
            let steps = segment_steps(length, problem.expiry, self.model.time_steps);
            let dt = length / steps as f64;
            for step in 1..=steps {
                // The last step lands exactly on the stop, so rounding never skips the dividend
                // or the valuation instant.
                let next = if step == steps {
                    stop
                } else {
                    tau + dt * step as f64
                };
                let start = if step == 1 {
                    tau
                } else {
                    tau + dt * (step - 1) as f64
                };
                if steps_taken < self.model.damping_steps {
                    let middle = 0.5 * (start + next);
                    self.advance(
                        &mut values,
                        &mut scratch,
                        &operator,
                        1.0,
                        middle - start,
                        middle,
                    );
                    self.advance(
                        &mut values,
                        &mut scratch,
                        &operator,
                        1.0,
                        next - middle,
                        next,
                    );
                } else {
                    self.advance(
                        &mut values,
                        &mut scratch,
                        &operator,
                        0.5,
                        next - start,
                        next,
                    );
                }
                steps_taken += 1;
            }
            tau = stop;
            if let Some(amount) = dividend {
                self.pay_dividend(&mut values, &mut scratch.jumped, tau, amount);
            }
        }

        self.read_spot(&values)
    }

    /// One theta-scheme step to `tau` of length `dt` (`theta` 1 is implicit, 0.5 Crank-Nicolson),
    /// with the early-exercise floor.
    fn advance(
        &self,
        values: &mut [f64],
        scratch: &mut Scratch,
        operator: &Operator,
        theta: f64,
        dt: f64,
        tau: f64,
    ) {
        let n = values.len() - 1;
        let explicit = (1.0 - theta) * dt;
        let implicit = theta * dt;
        let factor = self.stock_factor(tau);
        let low = self.edge_value(tau, self.grid.exp_y[0] * factor);
        let high = self.edge_value(tau, self.grid.exp_y[n] * factor);
        // The operator's weights on a node's lower and upper neighbours.
        let to_lower = operator.diffusion - operator.convection;
        let to_upper = operator.diffusion + operator.convection;
        let to_self = 2.0 * operator.diffusion + operator.rate;

        // Right-hand side over the interior nodes 1..n-1.
        let rhs = &mut scratch.rhs;
        for node in 1..n {
            let applied =
                to_lower * values[node - 1] + to_upper * values[node + 1] - to_self * values[node];
            rhs[node - 1] = values[node] + explicit * applied;
        }
        rhs[0] += implicit * to_lower * low;
        rhs[n - 2] += implicit * to_upper * high;

        let american = self.problem.american;
        if american {
            for (exercise, &exp_y) in scratch.exercise.iter_mut().zip(&self.grid.exp_y) {
                *exercise = self.exercise_value(exp_y * factor);
            }
        }
        // With a non-negative yield, the region where exercise is optimal is one interval at the
        // grid's in-the-money end, and the floor can be applied exactly inside the solve. A
        // negative yield can split a put's exercise region in two, so it is floored after.
        let floor_in_solve = american && self.problem.dividend_yield >= 0.0;
        solve_tridiagonal(
            rhs,
            &mut scratch.upper,
            Tridiagonal {
                lower: -implicit * to_lower,
                diagonal: 1.0 + implicit * to_self,
                upper: -implicit * to_upper,
            },
            self.problem.kind,
            floor_in_solve.then_some(&scratch.exercise[1..n]),
        );

        values[0] = low;
        values[n] = high;
        values[1..n].copy_from_slice(&rhs[..n - 1]);

        if american && !floor_in_solve {
            for (value, &exercise) in values.iter_mut().zip(&scratch.exercise) {
                *value = value.max(exercise);
            }
        }
    }

    /// Steps back across an ex-dividend instant at `tau`: the value just before is the value just
    /// after at the stock price less the dividend, then the early-exercise floor.
    fn pay_dividend(&self, values: &mut [f64], jumped: &mut [f64], tau: f64, amount: f64) {
        let grid = self.grid;
        let n = values.len() - 1;
        let factor = self.stock_factor(tau);
        for (node, slot) in jumped.iter_mut().enumerate() {
            let stock = grid.exp_y[node] * factor;
            let after = stock - amount;
            let mut value = if after <= 0.0 {
                self.value_at_zero(tau)
            } else {
                let position = (libm::log(after) + grid.shift * tau - grid.y_min) / grid.dy;
                if position < 0.0 {
                    self.edge_value(tau, after)
                } else {
                    // `after < stock`, so the position never passes this node; the clamp only
                    // guards the last interval against rounding.
                    let lower = index_below(position).min(n - 1);
                    let weight = position - lower as f64;
                    (1.0 - weight) * values[lower] + weight * values[lower + 1]
                }
            };
            if self.problem.american {
                value = value.max(self.exercise_value(stock));
            }
            *slot = value;
        }
        values.copy_from_slice(jumped);
    }

    /// Reads price, delta, gamma and theta at the spot from the solution at the valuation instant.
    fn read_spot(&self, values: &[f64]) -> Valuation {
        let problem = self.problem;
        let grid = self.grid;
        let n = values.len() - 1;
        let y_spot = libm::log(problem.spot) + grid.shift * problem.expiry;

        // A cubic through the two nodes either side of the spot. A parabola through three would
        // give a second derivative constant across the interval, wrong to first order in the
        // spot's distance from the middle node.
        let position = (y_spot - grid.y_min) / grid.dy;
        let node = index_below(position).clamp(1, n - 2);
        let u = position - node as f64;
        let (before, at, after, beyond) = (
            values[node - 1],
            values[node],
            values[node + 1],
            values[node + 2],
        );
        let c = 0.5 * (after + before) - at;
        let d = (beyond - 3.0 * after + 3.0 * at - before) / 6.0;
        let b = after - at - c - d;
        let value = at + u * (b + u * (c + u * d));
        let d_dy = (b + u * (2.0 * c + 3.0 * u * d)) / grid.dy;
        let d2_dy2 = (2.0 * c + 6.0 * u * d) / (grid.dy * grid.dy);

        let spot = problem.spot;
        let delta = d_dy / spot;
        let gamma = (d2_dy2 - d_dy) / (spot * spot);

        // Where immediate exercise is optimal the value is the exercise value, which does not
        // change with time; the interpolation's last-digit error around it is dropped. Elsewhere
        // the price is floored at zero (and an American option's at its exercise value) and the
        // Black-Scholes equation gives theta from the others.
        let intrinsic = self.exercise_value(spot);
        let exercising =
            problem.american && value - intrinsic <= EXERCISE_TOLERANCE * problem.strike;
        let (price, theta) = if exercising {
            (intrinsic, 0.0)
        } else {
            let price = value.max(0.0);
            let theta = self.rate * price
                - (self.rate - problem.dividend_yield) * spot * delta
                - 0.5 * self.volatility * self.volatility * spot * spot * gamma;
            (price, theta)
        };

        Valuation {
            price,
            delta,
            gamma,
            theta,
        }
    }
}

/// How close to its exercise value, relative to the strike, an American option's price must be
/// for it to count as being exercised (theta zero).
const EXERCISE_TOLERANCE: f64 = 1e-12;

/// Reusable buffers for one solve.
struct Scratch {
    rhs: Vec<f64>,
    upper: Vec<f64>,
    jumped: Vec<f64>,
    exercise: Vec<f64>,
}

impl Scratch {
    fn new(n: usize) -> Self {
        Self {
            rhs: vec![0.0; n - 1],
            upper: vec![0.0; n - 1],
            jumped: vec![0.0; n + 1],
            exercise: vec![0.0; n + 1],
        }
    }
}

/// The space operator `L V_i = diffusion·(V_{i+1} - 2V_i + V_{i-1}) + convection·(V_{i+1} -
/// V_{i-1}) - rate·V_i`, with fitted coefficients.
#[derive(Debug, Clone, Copy)]
struct Operator {
    diffusion: f64,
    convection: f64,
    rate: f64,
}

/// A tridiagonal matrix with constant diagonals.
#[derive(Debug, Clone, Copy)]
struct Tridiagonal {
    lower: f64,
    diagonal: f64,
    upper: f64,
}

/// Solves `matrix · x = rhs` in place in `rhs` (Thomas algorithm), `scratch` being of the same
/// length.
///
/// With a `floor`, each unknown is floored at it as the back substitution reaches it, which
/// solves the early-exercise problem exactly when the exercise region is one interval at the
/// in-the-money end (Brennan-Schwartz). The back substitution must start from that end: the high
/// end for a call, the low end for a put, so a put eliminates from the top down.
fn solve_tridiagonal(
    rhs: &mut [f64],
    scratch: &mut [f64],
    matrix: Tridiagonal,
    kind: OptionKind,
    floor: Option<&[f64]>,
) {
    let len = rhs.len();
    // Elimination runs away from the exercise end; `to_previous` couples a row to the row
    // eliminated before it, `to_next` to the one after.
    let (index, to_previous, to_next): (&dyn Fn(usize) -> usize, f64, f64) = match kind {
        OptionKind::Call => (&|step| step, matrix.lower, matrix.upper),
        OptionKind::Put => (&|step| len - 1 - step, matrix.upper, matrix.lower),
    };
    let diagonal = matrix.diagonal;

    let first = index(0);
    scratch[0] = to_next / diagonal;
    rhs[first] /= diagonal;
    for step in 1..len {
        let (row, previous) = (index(step), index(step - 1));
        let pivot = diagonal - to_previous * scratch[step - 1];
        scratch[step] = to_next / pivot;
        rhs[row] = (rhs[row] - to_previous * rhs[previous]) / pivot;
    }

    let floored = |row: usize, value: f64| match floor {
        Some(floor) => value.max(floor[row]),
        None => value,
    };
    let last = index(len - 1);
    rhs[last] = floored(last, rhs[last]);
    for step in (0..len - 1).rev() {
        let (row, next) = (index(step), index(step + 1));
        rhs[row] = floored(row, rhs[row] - scratch[step] * rhs[next]);
    }
}

/// Steps for a segment of `length` out of an option life of `expiry`, so the whole life gets about
/// `time_steps`; at least one.
fn segment_steps(length: f64, expiry: f64, time_steps: usize) -> usize {
    index_below((time_steps as f64 * length / expiry).round()).max(1)
}

/// The grid index at or below a position in grid units; zero for a negative position.
fn index_below(position: f64) -> usize {
    // `as` saturates: a negative or NaN position gives 0, and the callers clamp the top.
    position.floor() as usize
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::black_scholes::{european_price, norm_cdf, norm_pdf};

    #[test]
    fn index_below_floors_and_saturates() {
        assert_eq!(index_below(2.7), 2);
        assert_eq!(index_below(3.0), 3);
        assert_eq!(index_below(-0.5), 0);
        assert_eq!(index_below(f64::NAN), 0);
    }
    use crate::inputs::CashDividend;
    use OptionExercise::{American, European};
    use OptionKind::{Call, Put};

    fn model() -> FiniteDifferenceModel {
        FiniteDifferenceModel::default()
    }

    /// Closed-form European price and Greeks, the oracle for the grid.
    fn closed_form(inputs: &OptionInputs, volatility: f64) -> (Valuation, f64, f64) {
        let (s, k, t, r, q) = (
            inputs.spot,
            inputs.strike,
            inputs.time_to_expiry,
            inputs.rate,
            inputs.dividend_yield,
        );
        let std_dev = volatility * t.sqrt();
        let d1 = ((s / k).ln() + (r - q) * t) / std_dev + 0.5 * std_dev;
        let d2 = d1 - std_dev;
        let (dq, dr) = ((-q * t).exp(), (-r * t).exp());
        let price = european_price(inputs.kind, s, k, t, r, q, volatility);
        let gamma = dq * norm_pdf(d1) / (s * std_dev);
        let vega = s * dq * norm_pdf(d1) * t.sqrt();
        let decay = -s * dq * norm_pdf(d1) * volatility / (2.0 * t.sqrt());
        let (delta, theta, rho) = match inputs.kind {
            Call => (
                dq * norm_cdf(d1),
                decay - r * k * dr * norm_cdf(d2) + q * s * dq * norm_cdf(d1),
                k * t * dr * norm_cdf(d2),
            ),
            Put => (
                -dq * norm_cdf(-d1),
                decay + r * k * dr * norm_cdf(-d2) - q * s * dq * norm_cdf(-d1),
                -k * t * dr * norm_cdf(-d2),
            ),
        };
        (
            Valuation {
                price,
                delta,
                gamma,
                theta,
            },
            vega,
            rho,
        )
    }

    fn assert_close(actual: f64, expected: f64, tolerance: f64, what: &str) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{what}: {actual} vs {expected} (tolerance {tolerance})"
        );
    }

    #[test]
    fn european_matches_closed_form() {
        let cases = [
            (Call, 100.0, 100.0, 0.5, 0.05, 0.0, 0.2),
            (Put, 100.0, 100.0, 0.5, 0.05, 0.0, 0.2),
            (Call, 100.0, 120.0, 1.0, 0.03, 0.02, 0.35),
            (Put, 100.0, 80.0, 0.25, 0.01, 0.015, 0.4),
            (Put, 50.0, 60.0, 2.0, -0.005, 0.0, 0.25),
            (Call, 100.0, 101.0, 2.0 / 365.0, 0.04, 0.0, 0.15),
        ];
        for (kind, s, k, t, r, q, v) in cases {
            let inputs = OptionInputs::new(kind, European, s, k, t, r).with_dividend_yield(q);
            let (expected, vega, rho) = closed_form(&inputs, v);
            let actual = model().value(&inputs, v).unwrap();
            let label = format!("{kind} S{s} K{k} T{t}");
            assert_close(
                actual.price,
                expected.price,
                2e-5 * s,
                &format!("{label} price"),
            );
            assert_close(
                actual.delta,
                expected.delta,
                2e-4,
                &format!("{label} delta"),
            );
            assert_close(
                actual.gamma,
                expected.gamma,
                2e-3 * expected.gamma.abs() + 1e-6,
                &format!("{label} gamma"),
            );
            assert_close(
                actual.theta,
                expected.theta,
                1e-2 * expected.theta.abs() + 1e-3,
                &format!("{label} theta"),
            );
            assert_close(
                model().vega(&inputs, v).unwrap(),
                vega,
                1e-3 * vega + 1e-4,
                &format!("{label} vega"),
            );
            assert_close(
                model().rho(&inputs, v).unwrap(),
                rho,
                1e-3 * rho.abs() + 1e-4,
                &format!("{label} rho"),
            );
        }
    }

    #[test]
    fn american_call_without_dividends_is_european() {
        // With no dividends and a non-negative rate a call is never exercised early.
        let inputs = OptionInputs::new(Call, American, 100.0, 95.0, 0.75, 0.05);
        let (expected, _, _) = closed_form(&inputs, 0.3);
        let actual = model().value(&inputs, 0.3).unwrap();
        assert_close(actual.price, expected.price, 2e-3, "price");
        assert_close(actual.delta, expected.delta, 2e-4, "delta");
    }

    #[test]
    fn american_put_carries_an_early_exercise_premium() {
        let american = OptionInputs::new(Put, American, 90.0, 100.0, 1.0, 0.08);
        let european = OptionInputs {
            exercise: European,
            ..american.clone()
        };
        let american_price = model().price(&american, 0.25).unwrap();
        let european_price = model().price(&european, 0.25).unwrap();
        assert!(
            american_price > european_price + 0.5,
            "{american_price} vs {european_price}"
        );
        assert!(american_price >= american.intrinsic_value());
    }

    #[test]
    fn american_put_reference_value() {
        // S = K = 50, r = 10%, sigma = 40%, 5 months: the converged value is 4.2842.
        let inputs = OptionInputs::new(Put, American, 50.0, 50.0, 5.0 / 12.0, 0.10);
        let price = model().price(&inputs, 0.4).unwrap();
        assert_close(price, 4.2842, 1e-3, "price");
    }

    #[test]
    fn american_put_call_symmetry() {
        // An American call (S, K, r, q) is worth the American put (K, S, q, r).
        let call =
            OptionInputs::new(Call, American, 100.0, 90.0, 1.0, 0.03).with_dividend_yield(0.07);
        let put =
            OptionInputs::new(Put, American, 90.0, 100.0, 1.0, 0.07).with_dividend_yield(0.03);
        let call_price = model().price(&call, 0.3).unwrap();
        let put_price = model().price(&put, 0.3).unwrap();
        assert_close(call_price, put_price, 2e-3, "symmetry");
        // The call is worth more than its European value: the high yield makes early exercise pay.
        let (european, _, _) = closed_form(
            &OptionInputs {
                exercise: European,
                ..call.clone()
            },
            0.3,
        );
        assert!(call_price > european.price + 0.1);
    }

    /// The value of a call with no yield and one cash dividend `amount` at `ex`, by quadrature
    /// over the stock price just before the ex-dividend instant. After it the call is European,
    /// so the value there is the closed form; an American call may also be exercised just before.
    fn one_dividend_call(inputs: &OptionInputs, ex: f64, amount: f64, volatility: f64) -> f64 {
        let (s, k, t, r) = (
            inputs.spot,
            inputs.strike,
            inputs.time_to_expiry,
            inputs.rate,
        );
        let american = inputs.exercise == American;
        let points = 8000;
        let (low, high) = (-10.0, 10.0);
        let h = (high - low) / points as f64;
        let mut sum = 0.0;
        for point in 0..=points {
            let z = low + point as f64 * h;
            let before =
                s * ((r - 0.5 * volatility * volatility) * ex + volatility * ex.sqrt() * z).exp();
            let after = before - amount;
            let held = if after > 0.0 {
                european_price(Call, after, k, t - ex, r, 0.0, volatility)
            } else {
                0.0
            };
            let value = if american { held.max(before - k) } else { held };
            let weight = if point == 0 || point == points {
                1.0
            } else if point % 2 == 1 {
                4.0
            } else {
                2.0
            };
            sum += weight * value * norm_pdf(z);
        }
        (-r * ex).exp() * sum * h / 3.0
    }

    #[test]
    fn one_cash_dividend_matches_quadrature() {
        let cases = [
            (American, 100.0, 100.0, 0.5, 0.25, 2.0),
            (European, 100.0, 100.0, 0.5, 0.25, 2.0),
            (American, 100.0, 90.0, 0.5, 0.45, 5.0),
            (European, 100.0, 90.0, 0.5, 0.45, 5.0),
            (American, 100.0, 110.0, 1.0, 0.1, 3.0),
        ];
        for (exercise, s, k, t, ex, amount) in cases {
            let inputs = OptionInputs::new(Call, exercise, s, k, t, 0.05)
                .with_dividends(vec![CashDividend::new(ex, amount)]);
            let expected = one_dividend_call(&inputs, ex, amount, 0.25);
            let actual = model().price(&inputs, 0.25).unwrap();
            assert_close(
                actual,
                expected,
                2e-3,
                &format!("{exercise} K{k} ex{ex} D{amount}"),
            );
        }
    }

    #[test]
    fn a_late_large_dividend_makes_early_exercise_pay() {
        let inputs = OptionInputs::new(Call, American, 100.0, 90.0, 0.5, 0.05)
            .with_dividends(vec![CashDividend::new(0.45, 5.0)]);
        let american = model().price(&inputs, 0.25).unwrap();
        let european = model()
            .price(
                &OptionInputs {
                    exercise: European,
                    ..inputs.clone()
                },
                0.25,
            )
            .unwrap();
        assert!(american > european + 0.5, "{american} vs {european}");
    }

    #[test]
    fn dividends_lower_calls_and_raise_puts() {
        let plain = OptionInputs::new(Call, American, 100.0, 100.0, 0.5, 0.03);
        let with_dividend = plain
            .clone()
            .with_dividends(vec![CashDividend::new(0.2, 1.5)]);
        assert!(model().price(&with_dividend, 0.3).unwrap() < model().price(&plain, 0.3).unwrap());
        let put = OptionInputs { kind: Put, ..plain };
        let put_with_dividend = OptionInputs {
            kind: Put,
            ..with_dividend
        };
        assert!(
            model().price(&put_with_dividend, 0.3).unwrap() > model().price(&put, 0.3).unwrap()
        );
    }

    #[test]
    fn dividends_outside_the_options_life_change_nothing() {
        let plain = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.03);
        let outside = plain.clone().with_dividends(vec![
            CashDividend::new(0.0, 1.0),
            CashDividend::new(-0.1, 1.0),
            CashDividend::new(0.5, 1.0),
            CashDividend::new(0.8, 1.0),
            CashDividend::new(0.2, 0.0),
        ]);
        assert_eq!(model().value(&outside, 0.3), model().value(&plain, 0.3));
    }

    #[test]
    fn dividends_at_one_instant_add_up() {
        let split =
            OptionInputs::new(Call, American, 100.0, 100.0, 0.5, 0.03).with_dividends(vec![
                CashDividend::new(0.2, 0.5),
                CashDividend::new(0.2, 1.0),
            ]);
        let summed = OptionInputs {
            dividends: vec![CashDividend::new(0.2, 1.5)],
            ..split.clone()
        };
        assert_eq!(model().value(&split, 0.3), model().value(&summed, 0.3));
    }

    #[test]
    fn a_dividend_larger_than_the_stock_leaves_it_worthless() {
        // The stock pays what it is worth and falls to zero: the call is worth only its
        // exercise before the ex-dividend instant, and the put is worth about the strike.
        let call = OptionInputs::new(Call, American, 100.0, 50.0, 0.5, 0.0)
            .with_dividends(vec![CashDividend::new(0.25, 500.0)]);
        let call_value = model().value(&call, 0.3).unwrap();
        assert!(call_value.price >= 50.0 && call_value.price.is_finite());
        let put = OptionInputs { kind: Put, ..call };
        let put_price = model().price(&put, 0.3).unwrap();
        assert_close(put_price, 50.0, 1e-6, "put");
    }

    #[test]
    fn deep_in_the_money_put_is_exercised_now() {
        let inputs = OptionInputs::new(Put, American, 50.0, 100.0, 0.05, 0.05);
        let valuation = model().value(&inputs, 0.2).unwrap();
        assert_eq!(valuation.price, 50.0);
        assert_eq!(valuation.theta, 0.0);
        assert_close(valuation.delta, -1.0, 1e-6, "delta");
        assert_close(valuation.gamma, 0.0, 1e-6, "gamma");
    }

    #[test]
    fn delta_and_theta_match_bumped_prices() {
        let inputs = OptionInputs::new(Put, American, 100.0, 105.0, 0.4, 0.04)
            .with_dividends(vec![CashDividend::new(0.2, 1.0)]);
        let volatility = 0.3;
        let valuation = model().value(&inputs, volatility).unwrap();

        let at_spot = |spot: f64| {
            model()
                .price(
                    &OptionInputs {
                        spot,
                        ..inputs.clone()
                    },
                    volatility,
                )
                .unwrap()
        };
        let bump = 0.5;
        let delta = (at_spot(100.0 + bump) - at_spot(100.0 - bump)) / (2.0 * bump);
        let gamma =
            (at_spot(100.0 + bump) - 2.0 * valuation.price + at_spot(100.0 - bump)) / (bump * bump);
        assert_close(valuation.delta, delta, 2e-3, "delta");
        assert_close(valuation.gamma, gamma, 2e-3, "gamma");

        // A day passes: the dividend and expiry are a day nearer.
        let day = 1.0 / 365.0;
        let later = OptionInputs {
            time_to_expiry: inputs.time_to_expiry - day,
            dividends: vec![CashDividend::new(0.2 - day, 1.0)],
            ..inputs.clone()
        };
        let theta = (model().price(&later, volatility).unwrap() - valuation.price) / day;
        assert_close(valuation.theta, theta, 0.03 * theta.abs(), "theta");
    }

    #[test]
    fn negative_yield_floors_after_each_step() {
        // q < r < 0: the put's exercise region can split, so it is floored after each step.
        let american =
            OptionInputs::new(Put, American, 100.0, 105.0, 1.0, -0.01).with_dividend_yield(-0.03);
        let european = OptionInputs {
            exercise: European,
            ..american.clone()
        };
        let american_price = model().price(&american, 0.2).unwrap();
        let european_price = model().price(&european, 0.2).unwrap();
        assert!(american_price.is_finite());
        // Early exercise is worth about 0.17 here: the floor is applied, not skipped.
        assert!(
            american_price > european_price + 0.1,
            "{american_price} vs {european_price}"
        );
        assert!(american_price >= american.intrinsic_value());
    }

    #[test]
    fn extreme_volatilities_stay_finite_and_bounded() {
        for volatility in [1e-7, 1e-3, 4.0] {
            for kind in [Call, Put] {
                for exercise in [American, European] {
                    let inputs = OptionInputs::new(kind, exercise, 100.0, 100.0, 2.0, 0.03)
                        .with_dividends(vec![CashDividend::new(1.0, 1.0)]);
                    let valuation = model().value(&inputs, volatility).unwrap();
                    let label = format!("{kind} {exercise} vol {volatility}");
                    assert!(
                        valuation.price.is_finite() && valuation.delta.is_finite(),
                        "{label}"
                    );
                    assert!(
                        valuation.price >= 0.0 && valuation.price <= 100.0,
                        "{label}"
                    );
                }
            }
        }
        // At almost no volatility the European call is its discounted forward intrinsic value.
        let call = OptionInputs::new(Call, European, 100.0, 90.0, 1.0, 0.05);
        let expected = 100.0 - 90.0 * (-0.05f64).exp();
        assert_close(
            model().price(&call, 1e-7).unwrap(),
            expected,
            1e-6,
            "zero-vol call",
        );
    }

    #[test]
    fn near_zero_volatility_follows_the_deterministic_path() {
        // With no volatility the stock grows at r and drops by each dividend; the American put is
        // worth its best discounted exercise along that path. Here that is just after the first
        // dividend: the drop is worth more than the interest on the strike.
        let (s, k, r) = (100.0, 115.0, 0.04);
        let dividends = [(0.1, 0.8), (0.35, 0.8)];
        let inputs = OptionInputs::new(Put, American, s, k, 0.5, r).with_dividends(
            dividends
                .iter()
                .map(|&(time, amount)| CashDividend::new(time, amount))
                .collect(),
        );
        let mut best = k - s;
        let mut stock = s;
        let mut previous = 0.0;
        for (time, amount) in dividends {
            stock = stock * (r * (time - previous)).exp() - amount;
            previous = time;
            best = best.max((-r * time).exp() * (k - stock));
        }
        assert!(best > k - s + 0.3, "{best}");
        for volatility in [1e-7, 1e-4] {
            let price = model().price(&inputs, volatility).unwrap();
            assert_close(price, best, 1e-5, &format!("vol {volatility}"));
        }
    }

    #[test]
    fn invalid_inputs_are_refused() {
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.03);
        for volatility in [0.0, -0.2, f64::NAN] {
            assert!(matches!(
                model().value(&inputs, volatility),
                Err(PricingError::InvalidInput {
                    name: "volatility",
                    ..
                })
            ));
            assert!(model().vega(&inputs, volatility).is_err());
            assert!(model().rho(&inputs, volatility).is_err());
        }
        let expired = OptionInputs {
            time_to_expiry: 0.0,
            ..inputs
        };
        assert!(matches!(
            model().value(&expired, 0.2),
            Err(PricingError::Expired { .. })
        ));
    }

    #[test]
    fn greeks_combine_the_valuation_vega_and_rho() {
        let inputs = OptionInputs::new(Call, American, 100.0, 100.0, 0.5, 0.03)
            .with_dividends(vec![CashDividend::new(0.3, 1.0)]);
        let greeks = model().greeks(&inputs, 0.25).unwrap();
        let valuation = model().value(&inputs, 0.25).unwrap();
        assert_eq!(greeks.price, valuation.price);
        assert_eq!(greeks.delta, valuation.delta);
        assert_eq!(greeks.gamma, valuation.gamma);
        assert_eq!(greeks.theta, valuation.theta);
        assert_eq!(greeks.vega, model().vega(&inputs, 0.25).unwrap());
        assert_eq!(greeks.rho, model().rho(&inputs, 0.25).unwrap());
        assert!(greeks.vega > 0.0);
    }

    #[test]
    fn results_are_deterministic() {
        let inputs = OptionInputs::new(Put, American, 97.3, 100.0, 0.3, 0.04)
            .with_dividends(vec![CashDividend::new(0.1, 0.8)]);
        let first = model().greeks(&inputs, 0.27).unwrap();
        let second = model().greeks(&inputs, 0.27).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn configuration_is_validated_and_kept() {
        assert_eq!(
            FiniteDifferenceModel::new(3, 10),
            Err(FiniteDifferenceConfigError::TooFewSpaceSteps(3))
        );
        assert_eq!(
            FiniteDifferenceModel::new(10, 0),
            Err(FiniteDifferenceConfigError::NoTimeSteps)
        );
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                model().with_std_devs(bad),
                Err(FiniteDifferenceConfigError::InvalidStdDevs(_))
            ));
        }
        let custom = FiniteDifferenceModel::new(MIN_SPACE_STEPS, 7)
            .unwrap()
            .with_damping_steps(0)
            .with_std_devs(6.0)
            .unwrap();
        assert_eq!(custom.space_steps(), MIN_SPACE_STEPS);
        assert_eq!(custom.time_steps(), 7);
        assert_eq!(custom.damping_steps(), 0);
        assert_eq!(custom.std_devs(), 6.0);

        let default = model();
        assert_eq!(default.space_steps(), DEFAULT_SPACE_STEPS);
        assert_eq!(default.time_steps(), DEFAULT_TIME_STEPS);
        assert_eq!(default.damping_steps(), DEFAULT_DAMPING_STEPS);
        assert_eq!(default.std_devs(), DEFAULT_STD_DEVS);

        // The smallest grid still prices.
        let inputs = OptionInputs::new(Put, American, 100.0, 100.0, 0.5, 0.03);
        assert!(custom.price(&inputs, 0.2).unwrap().is_finite());
    }

    #[test]
    fn damping_removes_oscillation_near_expiry() {
        // Plain Crank-Nicolson leaves the payoff's kink oscillating in gamma near expiry; the
        // damped start does not.
        let inputs = OptionInputs::new(Call, European, 100.0, 100.0, 1.0 / 365.0, 0.0);
        let (expected, _, _) = closed_form(&inputs, 0.2);
        let coarse_time = FiniteDifferenceModel::new(400, 4).unwrap();
        let damped = coarse_time.value(&inputs, 0.2).unwrap();
        let plain = coarse_time
            .with_damping_steps(0)
            .value(&inputs, 0.2)
            .unwrap();
        let damped_error = (damped.gamma - expected.gamma).abs();
        let plain_error = (plain.gamma - expected.gamma).abs();
        assert!(
            damped_error < 0.5 * plain_error,
            "{damped_error} vs {plain_error}"
        );
    }

    #[test]
    fn no_damping_starts_with_plain_crank_nicolson() {
        // With one time step, one damped step replaces Crank-Nicolson with two implicit half
        // steps; with none the step stays Crank-Nicolson, so the two prices differ.
        let inputs = OptionInputs::new(Call, European, 100.0, 100.0, 0.1, 0.04);
        let one_step = FiniteDifferenceModel::new(400, 1).unwrap();
        let plain = one_step.with_damping_steps(0).price(&inputs, 0.2).unwrap();
        let damped = one_step.with_damping_steps(1).price(&inputs, 0.2).unwrap();
        assert!((plain - damped).abs() > 1e-6, "{plain} vs {damped}");
    }

    #[test]
    fn linear_payoffs_are_priced_exactly() {
        // Every node of a deep in-the-money European option is in the money, so its value is
        // linear in the stock, and the fitted scheme is exact for the stock and for constants.
        // The grid's edges carry the same linear value, so any error there would show here. A
        // zero rate keeps the strike's leg constant: time steps only approximate its discounting.
        let (s, t, q) = (100.0, 0.75, 0.03);
        let call = OptionInputs::new(Call, European, s, 1.0, t, 0.0).with_dividend_yield(q);
        let put = OptionInputs::new(Put, European, s, 400.0, t, 0.0).with_dividend_yield(q);
        let forward = s * (-q * t).exp();
        let call_price = model().price(&call, 0.2).unwrap();
        assert_close(call_price, forward - 1.0, 1e-9, "call");
        let put_price = model().price(&put, 0.2).unwrap();
        assert_close(put_price, 400.0 - forward, 1e-9, "put");
    }

    /// Runs `check` on a solver for `inputs` at `volatility` on the default grid.
    fn with_solver(inputs: &OptionInputs, volatility: f64, check: impl FnOnce(&Solver<'_>)) {
        let model = model();
        let problem = Problem::new(inputs, volatility).unwrap();
        let grid = Grid::new(&problem, volatility, &model);
        check(&Solver {
            problem: &problem,
            grid: &grid,
            volatility,
            rate: problem.rate,
            model: &model,
        });
    }

    #[test]
    fn edge_value_is_the_discounted_forward_intrinsic_value() {
        // Dividends at 0.25 and 0.75 of a one-year life are 0.75 and 0.25 before expiry.
        let (r, q) = (0.05, 0.03);
        let dividends = vec![CashDividend::new(0.25, 1.5), CashDividend::new(0.75, 2.0)];
        let call = OptionInputs::new(Call, European, 100.0, 90.0, 1.0, r)
            .with_dividend_yield(q)
            .with_dividends(dividends);
        // Each dividend still to come costs its amount discounted at r to its instant and at q
        // from there to expiry.
        let pending = |tau: f64| {
            let mut total = 0.0;
            if tau > 0.75 {
                total += 1.5 * (-r * (tau - 0.75) - q * 0.75).exp();
            }
            if tau > 0.25 {
                total += 2.0 * (-r * (tau - 0.25) - q * 0.25).exp();
            }
            total
        };
        let forward = |tau: f64, stock: f64| stock * (-q * tau).exp() - pending(tau);
        with_solver(&call, 0.2, |solver| {
            // At 0.25 before expiry the later dividend has just been paid: none is still to come.
            for tau in [0.25, 0.5, 1.0] {
                let expected = forward(tau, 100.0) - 90.0 * (-r * tau).exp();
                assert_close(
                    solver.edge_value(tau, 100.0),
                    expected,
                    1e-12,
                    &format!("{tau}"),
                );
            }
            // A stock worth less than its dividends delivers nothing.
            assert_eq!(solver.edge_value(1.0, 3.0), 0.0);
        });
        let put = OptionInputs {
            kind: Put,
            strike: 110.0,
            ..call
        };
        with_solver(&put, 0.2, |solver| {
            let expected = 110.0 * (-r * 0.5).exp() - forward(0.5, 50.0);
            assert_close(solver.edge_value(0.5, 50.0), expected, 1e-12, "put");
        });
        // An American put deep in the money is worth at least its exercise value.
        let american = OptionInputs {
            exercise: American,
            ..put
        };
        with_solver(&american, 0.2, |solver| {
            assert!(110.0 * (-r * 0.5).exp() - forward(0.5, 50.0) < 60.0);
            assert_eq!(solver.edge_value(0.5, 50.0), 60.0);
        });
    }

    #[test]
    fn value_at_zero_by_kind_and_exercise() {
        let r = 0.05;
        let put = OptionInputs::new(Put, European, 100.0, 110.0, 1.0, r);
        with_solver(&put, 0.2, |solver| {
            assert_close(
                solver.value_at_zero(0.5),
                110.0 * (-r * 0.5).exp(),
                1e-12,
                "european",
            )
        });
        let american = OptionInputs {
            exercise: American,
            ..put.clone()
        };
        with_solver(&american, 0.2, |solver| {
            assert_eq!(solver.value_at_zero(0.5), 110.0)
        });
        let call = OptionInputs { kind: Call, ..put };
        with_solver(&call, 0.2, |solver| {
            assert_eq!(solver.value_at_zero(0.5), 0.0)
        });
    }

    #[test]
    fn a_dividend_reads_each_value_at_the_stock_less_the_dividend() {
        // A deep in-the-money European call is linear in the stock: just after the dividend it is
        // worth its edge value, and just before, the same at the stock less the dividend.
        let (amount, tau) = (2.0, 0.5);
        let inputs = OptionInputs::new(Call, European, 100.0, 1.0, 1.0, 0.05)
            .with_dividend_yield(0.03)
            .with_dividends(vec![CashDividend::new(0.5, amount)]);
        with_solver(&inputs, 0.2, |solver| {
            let factor = solver.stock_factor(tau);
            let stocks: Vec<f64> = solver.grid.exp_y.iter().map(|&y| y * factor).collect();
            let mut values: Vec<f64> = stocks
                .iter()
                .map(|&stock| solver.edge_value(tau, stock))
                .collect();
            let mut jumped = vec![0.0; values.len()];
            solver.pay_dividend(&mut values, &mut jumped, tau, amount);
            let dy = solver.grid.dy;
            let mut below = 0;
            for (node, (&stock, &value)) in stocks.iter().zip(&values).enumerate() {
                let after = stock - amount;
                let expected = solver.edge_value(tau, after);
                if after < stocks[0] {
                    // Below the grid: the edge's limit value, exactly.
                    below += 1;
                    assert_eq!(value, expected, "node {node}");
                } else {
                    // Linear interpolation in ln(S) of a value linear in S errs by at most
                    // S·dy²/8.
                    let tolerance = stock * dy * dy / 8.0 + 1e-12;
                    assert_close(value, expected, tolerance, &format!("node {node}"));
                }
            }
            assert!(below > 0);
        });
    }

    #[test]
    fn the_spot_is_read_exactly_from_a_cubic() {
        // The read fits a cubic through the four nodes around the spot, so values on a cubic in y
        // give back its value and derivatives at the spot. A strike off the spot and a yield keep
        // the spot between nodes.
        let inputs =
            OptionInputs::new(Call, European, 100.0, 94.7, 0.5, 0.04).with_dividend_yield(0.01);
        with_solver(&inputs, 0.2, |solver| {
            let grid = solver.grid;
            let y_spot = 100f64.ln() + grid.shift * 0.5;
            let position = (y_spot - grid.y_min) / grid.dy;
            assert!((position - position.round()).abs() > 0.1, "{position}");
            let cubic = |y: f64| {
                let x = y - y_spot;
                10.0 + x * (50.0 + x * (30.0 + x * 40.0))
            };
            let values: Vec<f64> = (0..grid.len())
                .map(|node| cubic(grid.y_min + node as f64 * grid.dy))
                .collect();
            let valuation = solver.read_spot(&values);
            // At the spot the cubic is 10, its first derivative in y 50 and its second 60.
            assert_close(valuation.price, 10.0, 1e-9, "price");
            assert_close(valuation.delta, 50.0 / 100.0, 1e-9, "delta");
            assert_close(
                valuation.gamma,
                (60.0 - 50.0) / (100.0 * 100.0),
                1e-9,
                "gamma",
            );
        });
    }

    #[test]
    fn segment_steps_share_the_life_and_never_drop_to_zero() {
        assert_eq!(segment_steps(0.5, 1.0, 200), 100);
        assert_eq!(segment_steps(1e-9, 1.0, 200), 1);
        assert_eq!(segment_steps(1.0, 1.0, 200), 200);
    }
}
