use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, asset::name::AssetNameExchange};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Which side of the trade a fill was on: the one that supplied liquidity, or the one that took it.
///
/// Venues price these differently, and usually by a wide margin — a maker rebate against a taker
/// fee is the whole economics of quoting. A fee model that cannot tell them apart charges the taker
/// rate for everything, which systematically overstates the cost of exactly the strategies that
/// rest orders in order to earn the maker side.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub enum Liquidity {
    /// The fill took liquidity already resting on the book: a market order, or a limit order
    /// marketable on arrival. The default, because it is the conservative assumption — a model that
    /// guesses wrong this way overstates costs rather than inventing profit.
    #[default]
    Taker,
    /// The fill supplied liquidity: an order that rested on the book and was matched by someone
    /// else's incoming order.
    Maker,
}

/// What a [`FeeModel`] prices: one fill, when it happened, and how much of its order had filled
/// before it.
///
/// Build one with [`new`](Self::new), which assumes a taker fill that is its order's first, and
/// set the rest with [`with_liquidity`](Self::with_liquidity) and
/// [`with_prior_filled`](Self::with_prior_filled).
///
/// `#[non_exhaustive]`: further facts about the fill may be added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct FeeInput {
    /// Whether the fill bought or sold.
    pub side: Side,
    /// Execution price per unit of the underlying.
    pub price: Decimal,
    /// Number of contracts (or shares/units) filled. Its sign is ignored.
    pub quantity: Decimal,
    /// Multiplier converting contracts to underlying units (e.g. 100 for standard equity
    /// options). `Decimal::ONE` for spot.
    pub contract_size: Decimal,
    /// Whether the fill made or took liquidity; see [`Liquidity`].
    pub liquidity: Liquidity,
    /// How much of the fill's order had already filled before this fill, in contracts. Its sign
    /// is ignored. Zero for an order's first fill.
    ///
    /// A fee charged once per order, or bounded per order, is charged on the order's first fill
    /// and by difference on later ones, so this is what tells the two apart.
    pub prior_filled: Decimal,
    /// When the fill happened, on the venue's clock. A [`DatedFeeModel`] charges the schedule in
    /// effect at this time.
    pub time: DateTime<Utc>,
}

impl FeeInput {
    /// A taker fill of `quantity` at `price` at `time`, the first of its order.
    pub fn new(
        side: Side,
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        time: DateTime<Utc>,
    ) -> Self {
        Self {
            side,
            price,
            quantity,
            contract_size,
            liquidity: Liquidity::Taker,
            prior_filled: Decimal::ZERO,
            time,
        }
    }

    /// The same fill, on the `liquidity` side of the book.
    #[must_use]
    pub fn with_liquidity(self, liquidity: Liquidity) -> Self {
        Self { liquidity, ..self }
    }

    /// The same fill, after `prior_filled` of its order had filled.
    #[must_use]
    pub fn with_prior_filled(self, prior_filled: Decimal) -> Self {
        Self {
            prior_filled,
            ..self
        }
    }
}

/// Computes the trading fee for a single fill, in the instrument's quote asset.
///
/// # Implementer obligations
/// A fill's fee must not depend on how its order's quantity was split into fills at one price and
/// one time: the fee of `q1` after `p` filled plus the fee of `q2` after `p + q1` filled must equal
/// the fee of `q1 + q2` after `p` filled. The simulated venue relies on this. It reserves an
/// order's remaining fee in one amount when the order rests, and charges it fill by fill. Every
/// model in this module meets it at one price and one time. A bounded
/// [`Notional`](FeeBasis::Notional) component, with an order that fills at several prices, only
/// approximates it (see [`FeeComponent`]). A [`DatedFeeModel`] whose schedule changes while an
/// order rests charges the order's later fills by the new schedule; the simulated venue then costs
/// such a fill afresh rather than settling what it reserved.
pub trait FeeModel {
    /// The fee of the fill `input` describes. Negative is a rebate.
    fn compute_fee(&self, input: &FeeInput) -> Decimal;
}

/// Zero-fee model. Useful for backtests where fees are excluded.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct ZeroFeeModel;

impl FeeModel for ZeroFeeModel {
    fn compute_fee(&self, _input: &FeeInput) -> Decimal {
        Decimal::ZERO
    }
}

/// Flat commission charged per contract filled.
///
/// `total_fee = commission_per_contract * quantity.abs()`
///
/// This matches the typical Alpaca/IBKR per-contract options pricing.
/// The input's `contract_size` is not used; the fee is per contract unit,
/// not per underlying share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PerContractFeeModel {
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_per_contract: Decimal,
}

impl FeeModel for PerContractFeeModel {
    /// The input's `liquidity` is ignored: a per-contract commission is a flat brokerage charge,
    /// and the brokers this models (Alpaca, IBKR options) do not vary it by who supplied
    /// liquidity.
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        self.commission_per_contract * input.quantity.abs()
    }
}

/// Percentage-of-notional fee model for spot, futures and CFD exchanges.
///
/// `total_fee = rate * price * quantity.abs() * contract_size`
///
/// Common for crypto spot/futures exchanges (e.g. Binance 0.1% taker fee).
///
/// # Why `contract_size` is applied
///
/// The notional a percentage is charged on is the *underlying* exposure, not the contract count.
/// For `Spot` the multiplier is `Decimal::ONE` and the term is inert, but a €25-per-point index
/// CFD at 5000 has a notional of €125,000, not €5,000 — dropping the multiplier would understate
/// the fee by exactly that factor, silently, on every fill. The same applies to any `Future` or
/// `Perpetual` whose `contract_size` is not one.
///
/// Contrast [`PerContractFeeModel`], which ignores the multiplier because its fee genuinely is
/// per contract rather than per underlying unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PercentageFeeModel {
    /// Fee rate charged when the fill **took** liquidity, and when no [`maker_rate`](Self::maker_rate)
    /// is configured. Typical range is `[0, 1]`:
    /// - `0.001` = 0.1% (common taker fee)
    /// - `0.0005` = 0.05% (common maker fee)
    ///
    /// No validation is performed; values outside `[0, 1]` are accepted
    /// but produce unusual fee amounts.
    #[serde(with = "rust_decimal::serde::str")]
    pub rate: Decimal,

    /// Fee rate charged when the fill **made** liquidity. `None` charges [`rate`](Self::rate) on
    /// both sides.
    ///
    /// Absent by default so a configuration written before this field existed deserialises and
    /// prices exactly as it did. Set it to model a venue's maker schedule — commonly a fraction of
    /// the taker rate, and on some venues a rebate, which this represents as a negative rate.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::str_option"
    )]
    pub maker_rate: Option<Decimal>,
}

impl PercentageFeeModel {
    /// One rate, charged on both sides of the book.
    pub fn new(rate: Decimal) -> Self {
        Self {
            rate,
            maker_rate: None,
        }
    }

    /// Distinct maker and taker rates, as a venue's fee schedule quotes them.
    ///
    /// A maker *rebate* is a negative `maker_rate`.
    pub fn maker_taker(maker_rate: Decimal, taker_rate: Decimal) -> Self {
        Self {
            rate: taker_rate,
            maker_rate: Some(maker_rate),
        }
    }

    /// The rate charged for `liquidity`: [`maker_rate`](Self::maker_rate) when it is configured and
    /// the fill made liquidity, otherwise [`rate`](Self::rate).
    pub fn rate_for(&self, liquidity: Liquidity) -> Decimal {
        match liquidity {
            Liquidity::Maker => self.maker_rate.unwrap_or(self.rate),
            Liquidity::Taker => self.rate,
        }
    }
}

impl FeeModel for PercentageFeeModel {
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        self.rate_for(input.liquidity) * input.price * input.quantity.abs() * input.contract_size
    }
}

/// What a [`FeeComponent`] charges for: its rate and the quantity the rate applies to.
///
/// Each variant states the charge for an order that has filled `n` contracts in total at `price`.
/// A fill is charged the difference that it makes to that total (see [`FeeComponent`]).
///
/// Serialised, the amount is a decimal string: `{"PerContract":"0.025"}`.
///
/// `#[non_exhaustive]`: further bases may be added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum FeeBasis {
    /// `amount × n`: a charge per contract, whatever the contract's size. An options clearing
    /// fee, or a per-contract commission.
    PerContract(#[serde(with = "rust_decimal::serde::str")] Decimal),
    /// `amount × n × contract_size`: a charge per unit of the underlying. A fee quoted per share
    /// charges a 100-share option contract 100 times.
    PerUnderlyingUnit(#[serde(with = "rust_decimal::serde::str")] Decimal),
    /// `amount × price × n × contract_size`: a share of the traded value, `amount` being the rate
    /// (`0.0000206` for 0.00206 %).
    Notional(#[serde(with = "rust_decimal::serde::str")] Decimal),
    /// `amount` once the order has filled anything: a ticket charge, paid on the order's first
    /// fill only.
    PerOrder(#[serde(with = "rust_decimal::serde::str")] Decimal),
}

impl FeeBasis {
    /// The charge for an order that has filled `filled` contracts in total, at `input`'s price.
    ///
    /// For a [`PerOrder`](Self::PerOrder) basis, `filled` is above zero: such a component is
    /// charged through [`FeeComponent::bounded`], which charges nothing for an order that has
    /// filled nothing, without asking the basis.
    fn charge(&self, input: &FeeInput, filled: Decimal) -> Decimal {
        match *self {
            Self::PerContract(amount) => amount * filled,
            Self::PerUnderlyingUnit(amount) => amount * filled * input.contract_size,
            Self::Notional(rate) => rate * input.price * filled * input.contract_size,
            Self::PerOrder(amount) => amount,
        }
    }
}

/// One line of a venue's fee schedule: a [`FeeBasis`], the fills it applies to, and optional
/// bounds per order.
///
/// # Which fills
/// [`on_side`](Self::on_side) limits the component to buys or to sells (a fee charged on sales
/// only), and [`on_liquidity`](Self::on_liquidity) to maker or taker fills. Unset, it applies to
/// every fill.
///
/// # Bounds per order
/// [`with_min_per_order`](Self::with_min_per_order) and
/// [`with_max_per_order`](Self::with_max_per_order) bound what the component charges one order in
/// total. A fill pays the bounded charge for everything its order has filled, less the bounded
/// charge for what had filled before it. So the order's first fill pays at least the minimum, and
/// its fills together never pay more than the maximum, however it is split. No fill, no charge:
/// the minimum is not charged for an order that never fills.
///
/// Two approximations follow from pricing each fill on its own:
/// - What had filled before is valued at this fill's price, which matters only for a
///   [`Notional`](FeeBasis::Notional) basis with bounds, when an order fills at several prices.
/// - The bounds count the order's whole filled quantity, including fills a side or liquidity
///   filter excludes. So a [`PerOrder`](FeeBasis::PerOrder) charge, or a minimum, limited to one
///   side or liquidity is charged only if the order's first fill is on it: a ticket on maker fills
///   is never charged to an order whose first fill took liquidity. The engine prices every fill
///   as taker.
///
/// # Validation
/// A component charges only as part of a [`CompositeFeeModel`], which refuses one whose minimum
/// exceeds its maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct FeeComponent {
    basis: FeeBasis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    side: Option<Side>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    liquidity: Option<Liquidity>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::str_option"
    )]
    min_per_order: Option<Decimal>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::str_option"
    )]
    max_per_order: Option<Decimal>,
}

impl FeeComponent {
    /// A component charging `basis` on every fill, unbounded.
    pub fn new(basis: FeeBasis) -> Self {
        Self {
            basis,
            side: None,
            liquidity: None,
            min_per_order: None,
            max_per_order: None,
        }
    }

    /// The same component, charged only on fills on `side`.
    #[must_use]
    pub fn on_side(self, side: Side) -> Self {
        Self {
            side: Some(side),
            ..self
        }
    }

    /// The same component, charged only on fills on the `liquidity` side of the book.
    #[must_use]
    pub fn on_liquidity(self, liquidity: Liquidity) -> Self {
        Self {
            liquidity: Some(liquidity),
            ..self
        }
    }

    /// The same component, charging an order at least `min` in total once it fills.
    #[must_use]
    pub fn with_min_per_order(self, min: Decimal) -> Self {
        Self {
            min_per_order: Some(min),
            ..self
        }
    }

    /// The same component, charging an order at most `max` in total.
    #[must_use]
    pub fn with_max_per_order(self, max: Decimal) -> Self {
        Self {
            max_per_order: Some(max),
            ..self
        }
    }

    /// What the component charges for.
    pub fn basis(&self) -> FeeBasis {
        self.basis
    }

    /// The side it is charged on, or `None` for both.
    pub fn side(&self) -> Option<Side> {
        self.side
    }

    /// The liquidity it is charged on, or `None` for both.
    pub fn liquidity(&self) -> Option<Liquidity> {
        self.liquidity
    }

    /// The least it charges an order that fills, if bounded.
    pub fn min_per_order(&self) -> Option<Decimal> {
        self.min_per_order
    }

    /// The most it charges an order, if bounded.
    pub fn max_per_order(&self) -> Option<Decimal> {
        self.max_per_order
    }

    /// Whether the component charges a fill its quantity times one contract's charge, whatever
    /// its order filled before: neither per order nor bounded per order.
    fn is_linear(&self) -> bool {
        !matches!(self.basis, FeeBasis::PerOrder(_))
            && self.min_per_order.is_none()
            && self.max_per_order.is_none()
    }

    /// The bounded charge for an order that has filled `filled` contracts in total.
    fn bounded(&self, input: &FeeInput, filled: Decimal) -> Decimal {
        if filled.is_zero() {
            return Decimal::ZERO;
        }
        let charge = self.basis.charge(input, filled);
        let charge = self.min_per_order.map_or(charge, |min| charge.max(min));
        self.max_per_order.map_or(charge, |max| charge.min(max))
    }

    /// The component's charge for the fill `input` describes. Not a [`FeeModel`] of its own, so
    /// that it charges only inside a [`CompositeFeeModel`], which validates its bounds.
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        if self.side.is_some_and(|side| side != input.side)
            || self
                .liquidity
                .is_some_and(|liquidity| liquidity != input.liquidity)
        {
            return Decimal::ZERO;
        }
        if self.is_linear() {
            return self.basis.charge(input, input.quantity.abs());
        }
        let prior = input.prior_filled.abs();
        let filled = prior + input.quantity.abs();
        self.bounded(input, filled) - self.bounded(input, prior)
    }
}

/// A fee schedule of several lines, each a [`FeeComponent`], whose charges add.
///
/// It expresses a schedule no single model can: a $0 commission plus pass-through regulatory fees
/// charged per contract, per share or on the value of sales; a per-contract commission with a
/// minimum per order; or a ticket charge per order on top of a rate.
///
/// [`new`](Self::new) refuses a component whose minimum exceeds its maximum, and so does
/// deserialising one. Serialised, it is the list of its components.
///
/// # Rounding
/// The fee is not rounded. A venue that rounds each fee, or each day's total of each fee type, up
/// to the cent charges up to a cent more per fee type per rounding.
///
/// # Example: Alpaca's US option fees
/// Alpaca charges no commission on retail option trades, and passes regulatory and clearing fees
/// through. This schedule follows its fee schedule of 2026-10-12, for equity options. The rates
/// change, so check the venue's current schedule before relying on them. Alpaca rounds each fee
/// type's daily total up to the cent, which this does not model (see `# Rounding`). Index options
/// also pay a commission per contract and exchange fees that vary by symbol and premium, which
/// this model cannot yet express.
///
/// ```
/// use rust_decimal_macros::dec;
/// use rustrade_execution::fee::{CompositeFeeModel, FeeBasis, FeeComponent};
/// use rustrade_instrument::Side;
///
/// let alpaca_options = CompositeFeeModel::new(vec![
///     // SEC Section 31 fee: a share of the value of a sale.
///     FeeComponent::new(FeeBasis::Notional(dec!(0.0000206))).on_side(Side::Sell),
///     // FINRA trading activity fee: per contract sold (currently zero).
///     FeeComponent::new(FeeBasis::PerContract(dec!(0))).on_side(Side::Sell),
///     // Consolidated audit trail fee: per executed equivalent share.
///     FeeComponent::new(FeeBasis::PerUnderlyingUnit(dec!(0.000003))),
///     // Options regulatory fee and OCC clearing fee: per contract.
///     FeeComponent::new(FeeBasis::PerContract(dec!(0.015))),
///     FeeComponent::new(FeeBasis::PerContract(dec!(0.025))),
/// ])
/// .expect("no component has a minimum above its maximum");
/// # let _ = alpaca_options;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "Vec<FeeComponent>", into = "Vec<FeeComponent>")]
pub struct CompositeFeeModel(Vec<FeeComponent>);

impl CompositeFeeModel {
    /// A schedule charging the sum of `components`. An empty list charges nothing.
    ///
    /// # Errors
    /// Returns [`FeeModelError::MinAboveMax`] if a component's minimum per order exceeds its
    /// maximum.
    pub fn new(components: Vec<FeeComponent>) -> Result<Self, FeeModelError> {
        Self::try_from(components)
    }

    /// The schedule's components, in the order given.
    pub fn components(&self) -> &[FeeComponent] {
        &self.0
    }
}

impl TryFrom<Vec<FeeComponent>> for CompositeFeeModel {
    type Error = FeeModelError;

    fn try_from(components: Vec<FeeComponent>) -> Result<Self, Self::Error> {
        for (index, component) in components.iter().enumerate() {
            if let (Some(min), Some(max)) = (component.min_per_order, component.max_per_order)
                && min > max
            {
                return Err(FeeModelError::MinAboveMax { index, min, max });
            }
        }
        Ok(Self(components))
    }
}

impl From<CompositeFeeModel> for Vec<FeeComponent> {
    fn from(model: CompositeFeeModel) -> Self {
        model.0
    }
}

impl FeeModel for CompositeFeeModel {
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        self.0
            .iter()
            .map(|component| component.compute_fee(input))
            .sum()
    }
}

/// A fee schedule that changes over time: each fill is charged by the model in effect at its
/// [`time`](FeeInput::time).
///
/// Venues revise their fees. Regulatory rates are reset by the bodies that charge them, and
/// brokers publish revised schedules, each from a date. A backtest that spans a revision with one
/// fixed model charges every past fill today's fees. This model holds the whole schedule at each
/// date instead: an [`initial`](Self::initial) model, then a list of [`FeeChange`]s, each a model
/// that takes effect at its [`from`](FeeChange::from) time.
///
/// The model in effect at a time is the last change whose `from` is at or before it, or `initial`
/// before the first change, so `initial` covers all time before the first change. A change takes
/// effect at its `from` instant: a fill at exactly that time pays the new model.
///
/// [`new`](Self::new) refuses, and so does deserialising:
/// - changes whose times are not strictly increasing ([`FeeModelError::ChangesNotIncreasing`]);
/// - a dated model as `initial` or as a change's model ([`FeeModelError::NestedDated`]): any such
///   schedule can be written as one flat list.
///
/// An empty list of changes is accepted, and charges as `initial` does.
///
/// Serialised, it is `{"initial": <model>, "changes": [{"from": <RFC 3339 time>, "model":
/// <model>}, ...]}`.
///
/// # Orders resting across a change
/// The simulated venue holds a resting order's fee by the model in effect when the order rested,
/// and charges each fill by the model in effect when it fills. When the two differ, the fill is
/// costed afresh, and an order the account can no longer afford is cancelled, as a venue
/// re-checking the order's cost at fill time would cancel it.
///
/// # Example: a commission cut
/// The rates here are illustrative.
///
/// ```
/// use chrono::{TimeZone, Utc};
/// use rust_decimal_macros::dec;
/// use rustrade_execution::fee::{
///     DatedFeeModel, FeeChange, FeeModelConfig, PerContractFeeModel,
/// };
///
/// let per_contract = |commission_per_contract| {
///     FeeModelConfig::PerContract(PerContractFeeModel { commission_per_contract })
/// };
/// let schedule = DatedFeeModel::new(
///     per_contract(dec!(0.70)),
///     vec![FeeChange::new(
///         Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
///         per_contract(dec!(0.65)),
///     )],
/// )
/// .expect("one change, and no dated model inside");
///
/// assert_eq!(
///     schedule.model_at(Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap()),
///     &per_contract(dec!(0.70)),
/// );
/// assert_eq!(
///     schedule.model_at(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
///     &per_contract(dec!(0.65)),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "DatedFeeModelRepr", into = "DatedFeeModelRepr")]
pub struct DatedFeeModel {
    initial: Box<FeeModelConfig>,
    changes: Vec<FeeChange>,
}

/// One revision of a [`DatedFeeModel`]: the [`model`](Self::model) charged from
/// [`from`](Self::from) until the next change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct FeeChange {
    /// The instant the model takes effect, inclusive.
    pub from: DateTime<Utc>,
    /// The whole schedule in effect from then.
    pub model: FeeModelConfig,
}

impl FeeChange {
    /// `model`, in effect from `from`.
    pub fn new(from: DateTime<Utc>, model: FeeModelConfig) -> Self {
        Self { from, model }
    }
}

/// [`DatedFeeModel`]'s serialised form, validated on the way in.
#[derive(Deserialize, Serialize)]
struct DatedFeeModelRepr {
    initial: FeeModelConfig,
    changes: Vec<FeeChange>,
}

impl DatedFeeModel {
    /// A schedule charging `initial` until the first of `changes`, and each change's model from
    /// its time until the next.
    ///
    /// # Errors
    /// - [`FeeModelError::ChangesNotIncreasing`] if a change's time is not after the one before.
    /// - [`FeeModelError::NestedDated`] if `initial` or a change's model is itself dated.
    pub fn new(initial: FeeModelConfig, changes: Vec<FeeChange>) -> Result<Self, FeeModelError> {
        Self::try_from(DatedFeeModelRepr { initial, changes })
    }

    /// The model in effect before the first change.
    pub fn initial(&self) -> &FeeModelConfig {
        &self.initial
    }

    /// The changes, in time order.
    pub fn changes(&self) -> &[FeeChange] {
        &self.changes
    }

    /// The model in effect at `time`: the last change whose time is at or before it, else
    /// [`initial`](Self::initial).
    pub fn model_at(&self, time: DateTime<Utc>) -> &FeeModelConfig {
        let in_effect = self.changes.partition_point(|change| change.from <= time);
        match in_effect
            .checked_sub(1)
            .and_then(|index| self.changes.get(index))
        {
            Some(change) => &change.model,
            None => &self.initial,
        }
    }

    /// Whether every model in the schedule is linear; see [`FeeModelConfig::is_linear`].
    fn is_linear(&self) -> bool {
        self.initial.is_linear() && self.changes.iter().all(|change| change.model.is_linear())
    }
}

impl TryFrom<DatedFeeModelRepr> for DatedFeeModel {
    type Error = FeeModelError;

    fn try_from(repr: DatedFeeModelRepr) -> Result<Self, Self::Error> {
        let DatedFeeModelRepr { initial, changes } = repr;
        if matches!(initial, FeeModelConfig::Dated(_)) {
            return Err(FeeModelError::NestedDated { index: None });
        }
        for (index, change) in changes.iter().enumerate() {
            if matches!(change.model, FeeModelConfig::Dated(_)) {
                return Err(FeeModelError::NestedDated { index: Some(index) });
            }
        }
        for (index, pair) in changes.windows(2).enumerate() {
            if let [previous, change] = pair
                && change.from <= previous.from
            {
                return Err(FeeModelError::ChangesNotIncreasing {
                    index: index + 1,
                    previous: previous.from,
                    from: change.from,
                });
            }
        }
        Ok(Self {
            initial: Box::new(initial),
            changes,
        })
    }
}

impl From<DatedFeeModel> for DatedFeeModelRepr {
    fn from(model: DatedFeeModel) -> Self {
        Self {
            initial: *model.initial,
            changes: model.changes,
        }
    }
}

impl FeeModel for DatedFeeModel {
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        self.model_at(input.time).compute_fee(input)
    }
}

/// Why a fee model was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum FeeModelError {
    /// The component at `index` charges an order at least `min` and at most `max`, with `min`
    /// above `max`.
    #[error("fee component {index}: minimum per order {min} is above its maximum {max}")]
    MinAboveMax {
        /// The component's position in the list.
        index: usize,
        /// Its minimum per order.
        min: Decimal,
        /// Its maximum per order.
        max: Decimal,
    },
    /// The change at `index` of a [`DatedFeeModel`] takes effect at `from`, which is not after
    /// `previous`, the time of the change before it.
    #[error(
        "fee change {index}: effective from {}, which is not after {}",
        .from.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        .previous.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    )]
    ChangesNotIncreasing {
        /// The change's position in the list.
        index: usize,
        /// The time of the change before it.
        previous: DateTime<Utc>,
        /// Its own time.
        from: DateTime<Utc>,
    },
    /// A [`DatedFeeModel`] holds another dated model: as its initial model when `index` is
    /// `None`, else as the model of the change at `index`.
    #[error("a dated fee model holds another dated model ({})", match .index {
        Some(index) => format!("change {index}"),
        None => "its initial model".to_owned(),
    })]
    NestedDated {
        /// The change's position in the list, or `None` for the initial model.
        index: Option<usize>,
    },
}

/// Enum-dispatched fee model for use in types that require `Clone`, `PartialEq`,
/// `Serialize`, and `Deserialize` (e.g. `InstrumentState`).
///
/// Prefer this over `Box<dyn FeeModel>` when the field must be part of a derived
/// `serde` struct. Defaults to [`ZeroFeeModel`].
///
/// # Double-counting warning
///
/// Only enable a non-[`Zero`](FeeModelConfig::Zero) fee model when the `ExecutionClient`
/// reports `Trade.fees.fees = 0` for fills (i.e., commission is not already embedded in
/// fill reports). If the client already includes fees in `fees.fees` and a fee model
/// is also active, fees will be counted twice.
///
/// # Variants
///
/// - [`Zero`](FeeModelConfig::Zero): No fees (backtests where fees are excluded).
/// - [`PerContract`](FeeModelConfig::PerContract): Flat per-contract commission (options).
/// - [`Percentage`](FeeModelConfig::Percentage): Percentage of notional (spot/futures).
/// - [`Composite`](FeeModelConfig::Composite): Several components that add, each limited to a
///   side or a liquidity and optionally bounded per order (a $0 commission with regulatory fees,
///   a commission with a minimum per order).
/// - [`Dated`](FeeModelConfig::Dated): A schedule that changes over time, charging each fill by
///   the model in effect at its time.
///
/// `#[non_exhaustive]`: further models may be added.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum FeeModelConfig {
    Zero(ZeroFeeModel),
    PerContract(PerContractFeeModel),
    Percentage(PercentageFeeModel),
    Composite(CompositeFeeModel),
    Dated(DatedFeeModel),
}

impl Default for FeeModelConfig {
    fn default() -> Self {
        Self::Zero(ZeroFeeModel)
    }
}

impl FeeModelConfig {
    /// Whether a fill's fee is its quantity times the fee of one unit, whatever its order filled
    /// before: true unless a [`Composite`](Self::Composite) charges per order or bounds a
    /// component per order, or a [`Dated`](Self::Dated) schedule holds such a model at any time.
    pub(crate) fn is_linear(&self) -> bool {
        match self {
            Self::Zero(_) | Self::PerContract(_) | Self::Percentage(_) => true,
            Self::Composite(model) => model.0.iter().all(FeeComponent::is_linear),
            Self::Dated(model) => model.is_linear(),
        }
    }

    /// Whether the fee can depend on the fill's time: true for a [`Dated`](Self::Dated) schedule.
    pub(crate) fn is_dated(&self) -> bool {
        matches!(self, Self::Dated(_))
    }
}

/// Which asset the simulated venue charges a spot fill's fee in.
///
/// The amount always comes from the venue's [`FeeModelConfig`], which prices a fee in quote. This
/// rule says only which balance it is taken from, so it changes no cost, only which asset pays it:
///
/// | Rule | Buy | Sell |
/// |---|---|---|
/// | [`Quote`](Self::Quote) | quote, on top of the notional | quote, out of the proceeds |
/// | [`Received`](Self::Received) | base, out of the quantity received | quote, out of the proceeds |
///
/// A fee in base is the quote fee divided by the fill price, to `Decimal`'s 28 digits. The fill
/// reports it in base, with that quote fee as its
/// [`AssetFees::fees_quote`](crate::trade::AssetFees::fees_quote).
///
/// Venues differ, and so do accounts on one venue. `Quote` matches Coinbase spot's default.
/// `Received` matches Binance spot's default, with BNB fee payment turned off. Pick the rule of
/// the account a backtest stands in for. A CFD fill is cash-settled, so its fee is always in
/// quote, whatever this says.
///
/// # A buy's fee in base
/// Under `Received`, a buy receives its quantity less the fee, and the engine nets a base fee from
/// a spot position (see `rustrade`'s `InstrumentState::update_from_trade`), so the two stay equal.
/// The holding is then not a round number: a buy of 1 with a 0.1 % fee holds 0.999. A live venue
/// sells only whole multiples of its lot step, so when the instrument's spec gives a quantity
/// step, the engine rounds the position toward zero to it and leaves the remainder in the balance,
/// where only the venue's own dust conversion can clear it. This venue has no lot step: it fills
/// whatever quantity it is sent, so a close of the rounded position leaves the same remainder
/// here as live.
///
/// A flat fee on a tiny buy can exceed the quantity it receives. The excess is then taken from the
/// base balance, and the order is refused if that cannot cover it, as a sell whose fee exceeds its
/// proceeds takes the excess from quote. A fill at a price that is not positive cannot be
/// converted, so it is charged in quote under either rule.
///
/// A fee charged in a third asset, such as BNB, is set apart from this rule, by
/// [`FeeAssetConfig`]. Where it does not apply, this rule does.
///
/// `#[non_exhaustive]`: further rules may be added.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[non_exhaustive]
pub enum SpotFeeAsset {
    /// The fee is in quote on both sides: on top of what a buy pays, and out of what a sell
    /// receives. Venue-neutral, and the base a fill moves is exactly its quantity.
    #[default]
    Quote,
    /// The fee is in the asset the fill receives: base out of a buy's quantity, quote out of a
    /// sell's proceeds.
    Received,
}

/// A third asset the simulated venue charges spot fees in, at a discount, while the account
/// holds enough of it: Binance's "pay fees with BNB".
///
/// # What a fill pays
/// The fee is the [`FeeModelConfig`]'s fee in quote, less [`discount`](Self::discount), converted
/// to the fee asset at the venue's price for it: the mid, else the last price, of a spot
/// instrument on the venue pairing the fee asset with the fill's quote asset, in either direction
/// (`BNB/USDT`, or the reciprocal of `USDT/BNB`). Of several, the one first by name. It is taken
/// from the fee asset's free balance when the fill settles, apart from what the fill exchanges,
/// and the fill reports it in the fee asset, as a live venue does. Nothing is reserved for it
/// while an order rests, since a venue does not hold the fee asset for open orders.
///
/// The fee is not rounded to the fee asset's precision, which this venue does not know, so it
/// can differ from a live venue's in its last decimals.
///
/// # When it falls back
/// The fill is charged as without this setting, the undiscounted fee under the venue's
/// [`SpotFeeAsset`] rule, when any of these holds:
/// - the fee asset's free balance is short of the fee: the whole fee falls back, none of it is
///   split. A margin account does not lend the fee asset for a fee.
/// - the venue holds no positive price for the fee asset in the fill's quote. A venue with no
///   market feed holds none.
/// - the instrument is the fee asset's own, its base or its quote (`BNB/USDT`, `ETH/BNB`). A live
///   venue discounts those too; this one does not.
/// - the fill is cash-settled (a CFD), or its fee is zero or negative.
///
/// A fill's fee asset says which happened. With an engine that prices the fee asset, PnL counts a
/// fee charged in it; otherwise a backtest leaves it out, as a live run does.
///
/// [`new`](Self::new) refuses a discount outside zero to one, and so does deserialising one.
/// Serialised, the discount is a decimal string (`"0.25"`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "FeeAssetRepr", into = "FeeAssetRepr")]
pub struct FeeAssetConfig(FeeAssetRepr);

/// The serialised form of a [`FeeAssetConfig`], before validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct FeeAssetRepr {
    asset: AssetNameExchange,
    #[serde(with = "rust_decimal::serde::str")]
    discount: Decimal,
}

impl FeeAssetConfig {
    /// Fees charged in `asset`, at `discount` off the fee model's fee: `0.25` for a quarter off.
    ///
    /// # Errors
    /// Returns [`FeeAssetConfigError`] if `discount` is below zero or above one.
    pub fn new(asset: AssetNameExchange, discount: Decimal) -> Result<Self, FeeAssetConfigError> {
        Self::try_from(FeeAssetRepr { asset, discount })
    }

    /// The asset fees are charged in.
    pub fn asset(&self) -> &AssetNameExchange {
        &self.0.asset
    }

    /// The fraction taken off the fee model's fee: zero for none, one for no fee at all.
    pub fn discount(&self) -> Decimal {
        self.0.discount
    }
}

impl TryFrom<FeeAssetRepr> for FeeAssetConfig {
    type Error = FeeAssetConfigError;

    fn try_from(repr: FeeAssetRepr) -> Result<Self, Self::Error> {
        if repr.discount < Decimal::ZERO || repr.discount > Decimal::ONE {
            return Err(FeeAssetConfigError::DiscountOutOfRange(repr.discount));
        }
        Ok(Self(repr))
    }
}

impl From<FeeAssetConfig> for FeeAssetRepr {
    fn from(config: FeeAssetConfig) -> Self {
        config.0
    }
}

/// Why a [`FeeAssetConfig`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum FeeAssetConfigError {
    /// A discount below zero, which would charge more than the fee model, or above one, which
    /// would pay the account for trading.
    #[error("fee asset discount {0} is outside 0 to 1")]
    DiscountOutOfRange(Decimal),
}

impl FeeModel for FeeModelConfig {
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        match self {
            FeeModelConfig::Zero(model) => model.compute_fee(input),
            FeeModelConfig::PerContract(model) => model.compute_fee(input),
            FeeModelConfig::Percentage(model) => model.compute_fee(input),
            FeeModelConfig::Composite(model) => model.compute_fee(input),
            FeeModelConfig::Dated(model) => model.compute_fee(input),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// A fixed fill time, for models that do not depend on it.
    fn t0() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    /// A buy that is its order's first fill.
    fn input(
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        liquidity: Liquidity,
    ) -> FeeInput {
        FeeInput::new(Side::Buy, price, quantity, contract_size, t0()).with_liquidity(liquidity)
    }

    #[test]
    fn zero_fee_model_always_returns_zero() {
        assert_eq!(
            ZeroFeeModel.compute_fee(&input(d("100"), d("5"), d("100"), Liquidity::Taker)),
            Decimal::ZERO
        );
        assert_eq!(
            ZeroFeeModel.compute_fee(&input(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ONE,
                Liquidity::Taker
            )),
            Decimal::ZERO
        );
    }

    #[test]
    fn per_contract_fee_charges_by_quantity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Taker)),
            d("6.5")
        );
    }

    #[test]
    fn per_contract_fee_uses_abs_quantity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        // Negative quantity (sell side) should produce the same fee as positive.
        assert_eq!(
            model.compute_fee(&input(d("100"), d("-10"), d("100"), Liquidity::Taker)),
            model.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Taker)),
        );
    }

    // --- FeeModelConfig enum dispatch ---

    #[test]
    fn fee_model_config_zero_dispatches() {
        let cfg = FeeModelConfig::Zero(ZeroFeeModel);
        assert_eq!(
            cfg.compute_fee(&input(d("100"), d("5"), d("100"), Liquidity::Taker)),
            Decimal::ZERO
        );
    }

    #[test]
    fn fee_model_config_per_contract_dispatches() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        let cfg = FeeModelConfig::PerContract(model);
        assert_eq!(
            cfg.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Taker)),
            model.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Taker)),
        );
    }

    #[test]
    fn fee_model_config_default_is_zero() {
        assert_eq!(
            FeeModelConfig::default(),
            FeeModelConfig::Zero(ZeroFeeModel)
        );
    }

    // --- PercentageFeeModel ---

    #[test]
    fn percentage_fee_computes_rate_times_notional() {
        // 0.1% fee rate
        let model = PercentageFeeModel::new(d("0.001"));
        // 10 units at price 100 = notional 1000, fee = 1000 * 0.001 = 1
        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), d("1"), Liquidity::Taker)),
            d("1")
        );
    }

    #[test]
    fn percentage_fee_uses_abs_quantity() {
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(&input(d("100"), d("-10"), d("1"), Liquidity::Taker)),
            model.compute_fee(&input(d("100"), d("10"), d("1"), Liquidity::Taker)),
        );
    }

    #[test]
    fn percentage_fee_scales_by_contract_size() {
        // A EUR25-per-point index CFD at 5000: the notional is 125_000, not 5_000, so 0.1% is 125.
        // Charging 5 here would understate every CFD fill by the multiplier.
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(&input(d("5000"), d("1"), d("25"), Liquidity::Taker)),
            d("125")
        );
    }

    #[test]
    fn percentage_fee_is_unchanged_for_a_unit_contract_size() {
        // Spot is the overwhelmingly common case and must be untouched by the multiplier.
        let model = PercentageFeeModel::new(d("0.001"));
        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Taker)),
            d("1")
        );
    }

    #[test]
    fn per_contract_fee_ignores_contract_size() {
        // The counterpart to `percentage_fee_scales_by_contract_size`: this fee genuinely is per
        // contract, so the multiplier must NOT apply. Pinned so the two models cannot drift into
        // sharing one rule.
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };
        assert_eq!(
            model.compute_fee(&input(d("5000"), d("10"), d("25"), Liquidity::Taker)),
            model.compute_fee(&input(d("5000"), d("10"), Decimal::ONE, Liquidity::Taker)),
        );
    }

    #[test]
    fn fee_model_config_percentage_dispatches() {
        let model = PercentageFeeModel::new(d("0.001"));
        let cfg = FeeModelConfig::Percentage(model);
        assert_eq!(
            cfg.compute_fee(&input(d("100"), d("10"), d("1"), Liquidity::Taker)),
            model.compute_fee(&input(d("100"), d("10"), d("1"), Liquidity::Taker)),
        );
    }

    // --- Serde round-trip tests ---

    #[test]
    fn zero_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::Zero(ZeroFeeModel);
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"Zero":null}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn fee_model_config_default_when_field_omitted() {
        // Simulates deserializing a struct where fee_model field is absent
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            fee_model: FeeModelConfig,
        }
        let parsed: Wrapper = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(parsed.fee_model, FeeModelConfig::Zero(ZeroFeeModel));
    }

    #[test]
    fn percentage_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001")));
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"Percentage":{"rate":"0.001"}}"#);
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn per_contract_fee_model_serde_roundtrip() {
        let cfg = FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d("0.65"),
        });
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(
            json,
            r#"{"PerContract":{"commission_per_contract":"0.65"}}"#
        );
        let parsed: FeeModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }

    // --- Maker / taker ---

    #[test]
    fn a_percentage_model_without_a_maker_rate_charges_one_rate_on_both_sides() {
        let model = PercentageFeeModel::new(d("0.001"));

        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Maker)),
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Taker)),
            "an unconfigured maker rate must price exactly as the model did before the field \
             existed"
        );
    }

    #[test]
    fn a_configured_maker_rate_is_charged_only_to_the_maker() {
        let model = PercentageFeeModel::maker_taker(d("0.0002"), d("0.001"));

        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Taker)),
            d("1"),
            "0.1% of 1000 notional"
        );
        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Maker)),
            d("0.2"),
            "0.02% of 1000 notional"
        );
    }

    /// Some venues pay for liquidity. A rebate is a negative fee, not a missing one.
    #[test]
    fn a_maker_rebate_is_a_negative_fee() {
        let model = PercentageFeeModel::maker_taker(d("-0.0001"), d("0.001"));

        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), Decimal::ONE, Liquidity::Maker)),
            d("-0.1")
        );
    }

    #[test]
    fn a_per_contract_commission_does_not_vary_with_liquidity() {
        let model = PerContractFeeModel {
            commission_per_contract: d("0.65"),
        };

        assert_eq!(
            model.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Maker)),
            model.compute_fee(&input(d("100"), d("10"), d("100"), Liquidity::Taker))
        );
    }

    /// A config written before `maker_rate` existed must still deserialise, and price as it did.
    #[test]
    fn a_percentage_model_deserialises_without_a_maker_rate() {
        let model: PercentageFeeModel =
            serde_json::from_str(r#"{"rate":"0.001"}"#).expect("legacy shape must parse");

        assert_eq!(model, PercentageFeeModel::new(d("0.001")));
        assert_eq!(model.maker_rate, None);
    }

    #[test]
    fn a_percentage_model_round_trips_a_maker_rate() {
        let model = PercentageFeeModel::maker_taker(d("0.0002"), d("0.001"));
        let json = serde_json::to_string(&model).expect("must serialise");

        assert_eq!(
            serde_json::from_str::<PercentageFeeModel>(&json).expect("must round-trip"),
            model
        );
    }

    #[test]
    fn a_fee_asset_discount_must_lie_between_zero_and_one() {
        let bnb = || AssetNameExchange::new("BNB");
        for discount in ["0", "0.25", "1"] {
            let config = FeeAssetConfig::new(bnb(), d(discount)).unwrap();
            assert_eq!((config.asset(), config.discount()), (&bnb(), d(discount)));
        }
        for discount in ["-0.01", "1.01"] {
            assert_eq!(
                FeeAssetConfig::new(bnb(), d(discount)),
                Err(FeeAssetConfigError::DiscountOutOfRange(d(discount)))
            );
        }
    }

    /// The discount serialises as a decimal string, and deserialising refuses one out of range.
    #[test]
    fn a_fee_asset_config_round_trips_and_refuses_an_invalid_discount() {
        let config = FeeAssetConfig::new(AssetNameExchange::new("BNB"), d("0.25")).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"asset":"BNB","discount":"0.25"}"#);
        assert_eq!(
            serde_json::from_str::<FeeAssetConfig>(&json).unwrap(),
            config
        );

        let error = serde_json::from_str::<FeeAssetConfig>(r#"{"asset":"BNB","discount":"2"}"#)
            .unwrap_err();
        assert!(error.to_string().contains("outside 0 to 1"), "{error}");
    }

    // --- Composite ---

    fn sell(price: &str, quantity: &str, contract_size: &str) -> FeeInput {
        FeeInput::new(Side::Sell, d(price), d(quantity), d(contract_size), t0())
    }

    fn buy(price: &str, quantity: &str, contract_size: &str) -> FeeInput {
        FeeInput::new(Side::Buy, d(price), d(quantity), d(contract_size), t0())
    }

    /// Each basis prices what its rustdoc says, for an order's first fill.
    #[test]
    fn each_basis_charges_its_own_quantity() {
        // 3 contracts of 100 at 2.5: traded value 750.
        let fill = buy("2.5", "3", "100");
        let fee = |basis| FeeComponent::new(basis).compute_fee(&fill);

        assert_eq!(fee(FeeBasis::PerContract(d("0.025"))), d("0.075"));
        assert_eq!(fee(FeeBasis::PerUnderlyingUnit(d("0.000003"))), d("0.0009"));
        assert_eq!(fee(FeeBasis::Notional(d("0.0000206"))), d("0.01545"));
        assert_eq!(fee(FeeBasis::PerOrder(d("1"))), d("1"));
    }

    #[test]
    fn a_per_order_charge_is_paid_on_the_first_fill_only() {
        let ticket = FeeComponent::new(FeeBasis::PerOrder(d("1")));

        assert_eq!(ticket.compute_fee(&buy("10", "2", "1")), d("1"));
        assert_eq!(
            ticket.compute_fee(&buy("10", "2", "1").with_prior_filled(d("2"))),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_side_filter_charges_only_that_side() {
        let sec_fee = FeeComponent::new(FeeBasis::Notional(d("0.0000206"))).on_side(Side::Sell);

        assert_eq!(sec_fee.compute_fee(&buy("2.5", "3", "100")), Decimal::ZERO);
        assert_eq!(sec_fee.compute_fee(&sell("2.5", "3", "100")), d("0.01545"));
    }

    #[test]
    fn a_liquidity_filter_charges_only_that_liquidity() {
        let maker =
            FeeComponent::new(FeeBasis::PerContract(d("0.2"))).on_liquidity(Liquidity::Maker);
        let fill = buy("1", "5", "1");

        assert_eq!(maker.compute_fee(&fill), Decimal::ZERO, "a taker fill");
        assert_eq!(
            maker.compute_fee(&fill.with_liquidity(Liquidity::Maker)),
            d("1")
        );
    }

    /// A ticket limited to maker fills is charged on the order's first fill only, so an order
    /// whose first fill took liquidity never pays it.
    #[test]
    fn a_ticket_on_one_liquidity_is_charged_only_if_the_first_fill_is_on_it() {
        let ticket = FeeComponent::new(FeeBasis::PerOrder(d("5"))).on_liquidity(Liquidity::Maker);
        let maker = buy("1", "2", "1").with_liquidity(Liquidity::Maker);

        assert_eq!(ticket.compute_fee(&maker), d("5"), "a maker first fill");
        assert_eq!(
            ticket.compute_fee(&buy("1", "2", "1")),
            Decimal::ZERO,
            "a taker first fill"
        );
        assert_eq!(
            ticket.compute_fee(&maker.with_prior_filled(d("2"))),
            Decimal::ZERO,
            "a maker fill after a taker first fill"
        );
    }

    /// The order's first fill pays the minimum; later fills pay only what takes the order's total
    /// above it.
    #[test]
    fn a_minimum_per_order_is_charged_by_difference() {
        // $0.65 a contract, at least $1 an order.
        let commission =
            FeeComponent::new(FeeBasis::PerContract(d("0.65"))).with_min_per_order(d("1"));

        assert_eq!(commission.compute_fee(&buy("1", "1", "100")), d("1"));
        // Two contracts in total cost 1.30, of which 1 was paid.
        assert_eq!(
            commission.compute_fee(&buy("1", "1", "100").with_prior_filled(d("1"))),
            d("0.30")
        );
        // A fill that leaves the total under the minimum pays nothing more.
        let cheap = FeeComponent::new(FeeBasis::PerContract(d("0.1"))).with_min_per_order(d("1"));
        assert_eq!(
            cheap.compute_fee(&buy("1", "3", "100").with_prior_filled(d("2"))),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_maximum_per_order_caps_the_order_total() {
        let capped = FeeComponent::new(FeeBasis::PerContract(d("1"))).with_max_per_order(d("5"));

        assert_eq!(capped.compute_fee(&buy("1", "3", "1")), d("3"));
        assert_eq!(
            capped.compute_fee(&buy("1", "3", "1").with_prior_filled(d("3"))),
            d("2"),
            "the cap leaves 2 of the order's 5"
        );
        assert_eq!(
            capped.compute_fee(&buy("1", "3", "1").with_prior_filled(d("6"))),
            Decimal::ZERO
        );
    }

    /// The trait's obligation: splitting an order's quantity into fills at one price does not
    /// change what it pays, which is what lets the venue reserve a remainder's fee in one amount.
    #[test]
    fn a_composite_charges_an_order_the_same_however_it_is_split() {
        let model = CompositeFeeModel::new(vec![
            FeeComponent::new(FeeBasis::PerContract(d("0.65")))
                .with_min_per_order(d("1"))
                .with_max_per_order(d("4")),
            FeeComponent::new(FeeBasis::PerOrder(d("0.5"))),
            FeeComponent::new(FeeBasis::Notional(d("0.001"))).on_side(Side::Sell),
        ])
        .unwrap();
        let whole = model.compute_fee(&sell("2", "10", "100").with_prior_filled(d("1")));

        for split in ["0.5", "1", "3", "9"] {
            let first = model.compute_fee(&sell("2", split, "100").with_prior_filled(d("1")));
            let rest = d("10") - d(split);
            let second = model.compute_fee(
                &sell("2", &rest.to_string(), "100").with_prior_filled(d("1") + d(split)),
            );
            assert_eq!(first + second, whole, "split after {split}");
        }
    }

    #[test]
    fn a_composite_adds_its_components() {
        let model = CompositeFeeModel::new(vec![
            FeeComponent::new(FeeBasis::PerContract(d("0.015"))),
            FeeComponent::new(FeeBasis::PerContract(d("0.025"))),
            FeeComponent::new(FeeBasis::Notional(d("0.0000206"))).on_side(Side::Sell),
        ])
        .unwrap();

        assert_eq!(model.compute_fee(&buy("2.5", "3", "100")), d("0.12"));
        assert_eq!(model.compute_fee(&sell("2.5", "3", "100")), d("0.13545"));
        assert_eq!(
            FeeModelConfig::Composite(model.clone()).compute_fee(&sell("2.5", "3", "100")),
            model.compute_fee(&sell("2.5", "3", "100"))
        );
        assert_eq!(
            CompositeFeeModel::default().compute_fee(&sell("2.5", "3", "100")),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_minimum_above_the_maximum_is_refused() {
        let inverted = FeeComponent::new(FeeBasis::PerContract(d("1")))
            .with_min_per_order(d("5"))
            .with_max_per_order(d("4"));
        let equal = inverted.with_min_per_order(d("4"));

        assert_eq!(
            CompositeFeeModel::new(vec![equal, inverted]),
            Err(FeeModelError::MinAboveMax {
                index: 1,
                min: d("5"),
                max: d("4"),
            })
        );
        assert!(CompositeFeeModel::new(vec![equal]).is_ok());

        let error = serde_json::from_str::<CompositeFeeModel>(
            r#"[{"basis":{"PerContract":"1"},"min_per_order":"5","max_per_order":"4"}]"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("above its maximum"), "{error}");
    }

    #[test]
    fn a_composite_round_trips_as_its_list_of_components() {
        let cfg = FeeModelConfig::Composite(
            CompositeFeeModel::new(vec![
                FeeComponent::new(FeeBasis::Notional(d("0.0000206"))).on_side(Side::Sell),
                FeeComponent::new(FeeBasis::PerContract(d("0.65")))
                    .on_liquidity(Liquidity::Taker)
                    .with_min_per_order(d("1"))
                    .with_max_per_order(d("10")),
            ])
            .unwrap(),
        );
        let json = serde_json::to_string(&cfg).unwrap();

        assert_eq!(
            json,
            r#"{"Composite":[{"basis":{"Notional":"0.0000206"},"side":"Sell"},{"basis":{"PerContract":"0.65"},"liquidity":"Taker","min_per_order":"1","max_per_order":"10"}]}"#
        );
        assert_eq!(serde_json::from_str::<FeeModelConfig>(&json).unwrap(), cfg);
    }

    #[test]
    fn only_a_composite_with_a_per_order_charge_or_bound_is_not_linear() {
        let composite =
            |component| FeeModelConfig::Composite(CompositeFeeModel::new(vec![component]).unwrap());
        let per_contract = FeeComponent::new(FeeBasis::PerContract(d("1")));

        assert!(FeeModelConfig::default().is_linear());
        assert!(FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001"))).is_linear());
        assert!(
            FeeModelConfig::PerContract(PerContractFeeModel {
                commission_per_contract: d("1")
            })
            .is_linear()
        );
        assert!(composite(per_contract.on_side(Side::Sell)).is_linear());
        assert!(!composite(FeeComponent::new(FeeBasis::PerOrder(d("1")))).is_linear());
        assert!(!composite(per_contract.with_min_per_order(d("1"))).is_linear());
        assert!(!composite(per_contract.with_max_per_order(d("1"))).is_linear());
    }

    #[test]
    fn a_component_reports_what_it_was_built_with() {
        let component = FeeComponent::new(FeeBasis::PerOrder(d("1")))
            .on_side(Side::Buy)
            .on_liquidity(Liquidity::Maker)
            .with_min_per_order(d("0.5"))
            .with_max_per_order(d("2"));

        assert_eq!(component.basis(), FeeBasis::PerOrder(d("1")));
        assert_eq!(component.side(), Some(Side::Buy));
        assert_eq!(component.liquidity(), Some(Liquidity::Maker));
        assert_eq!(component.min_per_order(), Some(d("0.5")));
        assert_eq!(component.max_per_order(), Some(d("2")));
        assert_eq!(
            CompositeFeeModel::new(vec![component])
                .unwrap()
                .components(),
            &[component]
        );
    }

    // --- DatedFeeModel ------------------------------------------------------------------------

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap()
    }

    fn per_contract(commission: &str) -> FeeModelConfig {
        FeeModelConfig::PerContract(PerContractFeeModel {
            commission_per_contract: d(commission),
        })
    }

    /// 1 per contract, then 2 from `at(10)`, then 3 from `at(20)`.
    fn two_changes() -> DatedFeeModel {
        DatedFeeModel::new(
            per_contract("1"),
            vec![
                FeeChange::new(at(10), per_contract("2")),
                FeeChange::new(at(20), per_contract("3")),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_dated_model_charges_the_model_in_effect_at_the_fill_time() {
        let model = two_changes();
        let fee_at = |seconds| {
            model.compute_fee(&FeeInput::new(
                Side::Buy,
                d("5"),
                d("2"),
                d("100"),
                at(seconds),
            ))
        };

        assert_eq!(fee_at(-1_000_000), d("2"), "initial covers all time before");
        assert_eq!(fee_at(9), d("2"));
        assert_eq!(
            fee_at(10),
            d("4"),
            "a change takes effect at its own instant"
        );
        assert_eq!(fee_at(19), d("4"));
        assert_eq!(fee_at(20), d("6"));
        assert_eq!(fee_at(1_000_000), d("6"), "the last change stays in effect");
    }

    #[test]
    fn a_dated_model_reports_what_it_was_built_with() {
        let model = two_changes();

        assert_eq!(model.initial(), &per_contract("1"));
        assert_eq!(
            model.changes(),
            &[
                FeeChange::new(at(10), per_contract("2")),
                FeeChange::new(at(20), per_contract("3")),
            ]
        );
    }

    #[test]
    fn a_dated_model_with_no_changes_charges_its_initial_model() {
        let model = DatedFeeModel::new(per_contract("1"), vec![]).unwrap();

        assert_eq!(model.model_at(at(0)), &per_contract("1"));
        assert_eq!(
            model.compute_fee(&FeeInput::new(Side::Sell, d("5"), d("3"), d("1"), at(0))),
            d("3")
        );
    }

    #[test]
    fn a_dated_model_refuses_changes_not_strictly_increasing() {
        for second in [10, 5] {
            let refused = DatedFeeModel::new(
                per_contract("1"),
                vec![
                    FeeChange::new(at(0), per_contract("2")),
                    FeeChange::new(at(10), per_contract("3")),
                    FeeChange::new(at(second), per_contract("4")),
                ],
            );

            assert_eq!(
                refused,
                Err(FeeModelError::ChangesNotIncreasing {
                    index: 2,
                    previous: at(10),
                    from: at(second),
                })
            );
        }
    }

    #[test]
    fn a_dated_model_refuses_a_dated_model_inside_it() {
        let dated = || FeeModelConfig::Dated(two_changes());

        assert_eq!(
            DatedFeeModel::new(dated(), vec![]),
            Err(FeeModelError::NestedDated { index: None })
        );
        assert_eq!(
            DatedFeeModel::new(
                per_contract("1"),
                vec![
                    FeeChange::new(at(0), per_contract("2")),
                    FeeChange::new(at(10), dated()),
                ],
            ),
            Err(FeeModelError::NestedDated { index: Some(1) })
        );
    }

    #[test]
    fn a_refused_dated_model_says_why() {
        assert_eq!(
            FeeModelError::NestedDated { index: None }.to_string(),
            "a dated fee model holds another dated model (its initial model)"
        );
        assert_eq!(
            FeeModelError::NestedDated { index: Some(3) }.to_string(),
            "a dated fee model holds another dated model (change 3)"
        );
        assert_eq!(
            FeeModelError::ChangesNotIncreasing {
                index: 1,
                previous: at(10),
                from: at(10),
            }
            .to_string(),
            "fee change 1: effective from 2023-11-14T22:13:30Z, which is not after \
             2023-11-14T22:13:30Z"
        );
    }

    #[test]
    fn a_dated_model_serialises_as_its_initial_model_and_changes() {
        let cfg = FeeModelConfig::Dated(
            DatedFeeModel::new(
                FeeModelConfig::default(),
                vec![FeeChange::new(at(0), per_contract("0.65"))],
            )
            .unwrap(),
        );
        let json = serde_json::to_string(&cfg).unwrap();

        assert_eq!(
            json,
            r#"{"Dated":{"initial":{"Zero":null},"changes":[{"from":"2023-11-14T22:13:20Z","model":{"PerContract":{"commission_per_contract":"0.65"}}}]}}"#
        );
        assert_eq!(serde_json::from_str::<FeeModelConfig>(&json).unwrap(), cfg);
    }

    #[test]
    fn deserialising_a_dated_model_validates_it() {
        let out_of_order = r#"{"Dated":{"initial":{"Zero":null},"changes":[
            {"from":"2024-01-02T00:00:00Z","model":{"Zero":null}},
            {"from":"2024-01-01T00:00:00Z","model":{"Zero":null}}]}}"#;
        let nested = r#"{"Dated":{"initial":{"Dated":{"initial":{"Zero":null},"changes":[]}},
            "changes":[]}}"#;
        let nested_in_a_change = r#"{"Dated":{"initial":{"Zero":null},"changes":[
            {"from":"2024-01-01T00:00:00Z","model":{"Zero":null}},
            {"from":"2024-01-02T00:00:00Z",
             "model":{"Dated":{"initial":{"Zero":null},"changes":[]}}}]}}"#;

        let error = serde_json::from_str::<FeeModelConfig>(out_of_order)
            .unwrap_err()
            .to_string();
        assert!(error.contains("fee change 1"), "{error}");
        let error = serde_json::from_str::<FeeModelConfig>(nested)
            .unwrap_err()
            .to_string();
        assert!(error.contains("its initial model"), "{error}");
        let error = serde_json::from_str::<FeeModelConfig>(nested_in_a_change)
            .unwrap_err()
            .to_string();
        assert!(error.contains("(change 1)"), "{error}");
    }

    #[test]
    fn a_dated_model_is_linear_only_if_every_model_in_it_is() {
        let per_order = || {
            FeeModelConfig::Composite(
                CompositeFeeModel::new(vec![FeeComponent::new(FeeBasis::PerOrder(d("1")))])
                    .unwrap(),
            )
        };
        let dated = |initial, change| {
            FeeModelConfig::Dated(
                DatedFeeModel::new(initial, vec![FeeChange::new(at(0), change)]).unwrap(),
            )
        };

        assert!(dated(per_contract("1"), per_contract("2")).is_linear());
        assert!(!dated(per_order(), per_contract("2")).is_linear());
        assert!(!dated(per_contract("1"), per_order()).is_linear());
        assert!(dated(per_contract("1"), per_contract("2")).is_dated());
        assert!(!per_contract("1").is_dated());
    }
}
