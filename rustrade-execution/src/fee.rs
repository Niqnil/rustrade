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

/// What a [`FeeModel`] prices: one fill, and how much of its order had filled before it.
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
}

impl FeeInput {
    /// A taker fill of `quantity` at `price`, the first of its order.
    pub fn new(side: Side, price: Decimal, quantity: Decimal, contract_size: Decimal) -> Self {
        Self {
            side,
            price,
            quantity,
            contract_size,
            liquidity: Liquidity::Taker,
            prior_filled: Decimal::ZERO,
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
/// A fill's fee must not depend on how its order's quantity was split into fills at one price:
/// the fee of `q1` after `p` filled plus the fee of `q2` after `p + q1` filled must equal the fee
/// of `q1 + q2` after `p` filled. The simulated venue relies on this. It reserves an order's
/// remaining fee in one amount when the order rests, and charges it fill by fill. Every model in
/// this module meets it.
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
/// `contract_size` is accepted but not used; the fee is per contract unit,
/// not per underlying share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PerContractFeeModel {
    #[serde(with = "rust_decimal::serde::str")]
    pub commission_per_contract: Decimal,
}

impl FeeModel for PerContractFeeModel {
    /// `liquidity` is ignored: a per-contract commission is a flat brokerage charge, and the
    /// brokers this models (Alpaca, IBKR options) do not vary it by who supplied liquidity.
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
    /// `filled` is above zero: [`FeeComponent::bounded`] charges an order that has filled nothing
    /// nothing, without asking the basis.
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
///   filter excludes.
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

    /// The bounded charge for an order that has filled `filled` contracts in total.
    fn bounded(&self, input: &FeeInput, filled: Decimal) -> Decimal {
        if filled.is_zero() {
            return Decimal::ZERO;
        }
        let charge = self.basis.charge(input, filled);
        let charge = self.min_per_order.map_or(charge, |min| charge.max(min));
        self.max_per_order.map_or(charge, |max| charge.min(max))
    }
}

impl FeeModel for FeeComponent {
    fn compute_fee(&self, input: &FeeInput) -> Decimal {
        if self.side.is_some_and(|side| side != input.side)
            || self
                .liquidity
                .is_some_and(|liquidity| liquidity != input.liquidity)
        {
            return Decimal::ZERO;
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
///
/// `#[non_exhaustive]`: further models may be added.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum FeeModelConfig {
    Zero(ZeroFeeModel),
    PerContract(PerContractFeeModel),
    Percentage(PercentageFeeModel),
    Composite(CompositeFeeModel),
}

impl Default for FeeModelConfig {
    fn default() -> Self {
        Self::Zero(ZeroFeeModel)
    }
}

impl FeeModelConfig {
    /// Whether a fill's fee is its quantity times the fee of one unit, whatever its order filled
    /// before: true unless a [`Composite`](Self::Composite) charges per order or bounds a
    /// component per order.
    pub(crate) fn is_linear(&self) -> bool {
        match self {
            Self::Zero(_) | Self::PerContract(_) | Self::Percentage(_) => true,
            Self::Composite(model) => model.0.iter().all(|component| {
                !matches!(component.basis, FeeBasis::PerOrder(_))
                    && component.min_per_order.is_none()
                    && component.max_per_order.is_none()
            }),
        }
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

    /// A buy that is its order's first fill.
    fn input(
        price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        liquidity: Liquidity,
    ) -> FeeInput {
        FeeInput::new(Side::Buy, price, quantity, contract_size).with_liquidity(liquidity)
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
        FeeInput::new(Side::Sell, d(price), d(quantity), d(contract_size))
    }

    fn buy(price: &str, quantity: &str, contract_size: &str) -> FeeInput {
        FeeInput::new(Side::Buy, d(price), d(quantity), d(contract_size))
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
}
