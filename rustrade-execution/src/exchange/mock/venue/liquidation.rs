//! Liquidating the simulated venue's spot margin account, and the margin-call notices before it.
//! See [`SpotMarginLiquidation`] for what the venue does, and when.

use super::{
    FeePricing, InstrumentTerms, MarginCheck, NetPosition, Prices, Settlement, SimulatedVenue,
    Valued, mid_or_last, owed,
};
use crate::{
    AccountEventKind, UnindexedAccountEvent,
    cash_flow::{CashFlow, CashFlowId, CashFlowKind},
    exchange::mock::account::{Credit, Delivery},
    fee::Liquidity,
    fill::FillContext,
    margin::{MarginEffect, SpotMarginLiquidation},
    notice::{AccountNotice, NoticeKind},
    order::id::StrategyId,
    trade::{TimeSource, Trade, TradeOrigin},
};
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, RoundingStrategy};
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use rustrade_integration::collection::snapshot::Snapshot;
use smol_str::ToSmolStr;
use tracing::warn;

/// Where a spot margin account stands against its [`SpotMarginLiquidation`].
#[derive(Debug, Default)]
pub(super) struct LiquidationState {
    /// A liquidation has started and left debt owed, so the next input continues it.
    under_way: bool,
    /// A margin call has been sent and not yet restored.
    margin_called: bool,
    /// The venue has warned that it cannot value a debt, which it does once.
    warned_unvalued: bool,
    /// The liquidation under way has warned that it left debt owed, which it does once.
    warned_shortfall: bool,
}

impl LiquidationState {
    /// Whether a liquidation has started and still leaves debt owed, during which the venue lends
    /// nothing.
    pub(super) fn under_way(&self) -> bool {
        self.under_way
    }

    /// Whether a margin call has been sent and not yet restored.
    pub(super) fn margin_called(&self) -> bool {
        self.margin_called
    }

    /// The state a venue restored from a snapshot starts in: a liquidation `under_way` and a
    /// `margin_called` as they were, and no warning yet given, so the restored process warns again.
    pub(super) fn restored(under_way: bool, margin_called: bool) -> Self {
        Self {
            under_way,
            margin_called,
            ..Self::default()
        }
    }
}

/// One forced trade: paying [`pay`](Self::pay) for the other asset of
/// [`instrument`](Self::instrument), at [`price`](Self::price).
struct Leg {
    instrument: InstrumentNameExchange,
    terms: InstrumentTerms,
    side: Side,
    pay: AssetNameExchange,
    price: Decimal,
    /// What one unit of base costs in [`pay`](Self::pay), fee included where it is charged in it.
    /// Under a fee model that is not linear in quantity, the average over the quantity `leg`
    /// probed, which only seeds the search.
    paid_per_unit: Decimal,
    /// What one unit of base delivers of the asset received, net of any fee charged in it,
    /// averaged as [`paid_per_unit`](Self::paid_per_unit) is.
    delivered_per_unit: Decimal,
}

/// How far below `Decimal::MAX` the largest quantity a search tries stays, as a factor on its
/// cost, so that pricing it cannot overflow.
const SEARCH_HEADROOM: Decimal = Decimal::from_parts(1_000_000, 0, 0, false, 0);

impl Leg {
    /// The most of this leg a search for a quantity tries; see [`search_limit`].
    fn search_limit(&self) -> Decimal {
        search_limit(self.paid_per_unit, self.price)
    }
}

/// The most of a leg priced at `price` and paying `paid_per_unit` that a search tries: one costing
/// about 7.9e22 of what it pays with or of quote, more than any account holds, and a millionth of
/// `Decimal::MAX`, so that its fee and settlement cannot overflow.
fn search_limit(paid_per_unit: Decimal, price: Decimal) -> Decimal {
    (Decimal::MAX / SEARCH_HEADROOM)
        .checked_div(paid_per_unit.max(price))
        .unwrap_or(Decimal::MAX)
}

impl SimulatedVenue {
    /// Checks a spot margin account against its configured [`SpotMarginLiquidation`] at the end of
    /// an input, sending the margin-call notices it has crossed into and liquidating the account
    /// when it is due, onto `events`. Nothing without a configured liquidation.
    pub(super) fn check_margin(
        &mut self,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let Some(config) = self.spot_margin.as_ref() else {
            return;
        };
        let Some(liquidation) = config.liquidation() else {
            return;
        };
        let valuation = config.valuation_asset().clone();

        if !self
            .account
            .balances()
            .any(|balance| owed(balance) > Decimal::ZERO)
        {
            self.liquidation.under_way = false;
            self.liquidation.warned_shortfall = false;
            if self.liquidation.margin_called {
                self.liquidation.margin_called = false;
                events.push(self.notice(NoticeKind::MarginRestored, None, time_exchange));
            }
            return;
        }

        let Some(Valued { equity, debt }) =
            self.valued(config, MarginCheck::Liquidation, None, Prices::Current)
        else {
            if !self.liquidation.warned_unvalued {
                self.liquidation.warned_unvalued = true;
                warn!(
                    exchange = %self.exchange,
                    %valuation,
                    "SimulatedVenue cannot value a debt of its spot margin account, so does not \
                     check it for liquidation until it can; warned once per venue"
                );
            }
            return;
        };
        // Each asset owed is valued at a positive price, but a dust debt at a tiny price can
        // still value at zero, which leaves nothing to liquidate. The state is left as it was
        // until the debt values above zero.
        if debt <= Decimal::ZERO {
            return;
        }
        let level = (equity + debt) / debt;

        if self.liquidation.under_way || level <= liquidation.level() {
            self.liquidate(&liquidation, &valuation, level, time_exchange, events);
            return;
        }

        let Some(margin_call) = liquidation.margin_call_level() else {
            return;
        };
        let called = level <= margin_call;
        if called != self.liquidation.margin_called {
            self.liquidation.margin_called = called;
            let kind = match called {
                true => NoticeKind::MarginCall,
                false => NoticeKind::MarginRestored,
            };
            events.push(self.notice(kind, Some(level), time_exchange));
        }
    }

    /// One pass of a liquidation: the notice if it starts here, then the steps
    /// [`SpotMarginLiquidation`]'s `# How` lists, onto `events`.
    fn liquidate(
        &mut self,
        liquidation: &SpotMarginLiquidation,
        valuation: &AssetNameExchange,
        level: Decimal,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        if !self.liquidation.under_way {
            self.liquidation.under_way = true;
            self.liquidation.margin_called = false;
            events.push(self.notice(NoticeKind::Liquidation, Some(level), time_exchange));
        }

        self.cancel_orders(|_| true, time_exchange, events);
        // Valued after the cancels, so the fee is charged on what the liquidation repaid, not on
        // the loans the cancelled orders gave back.
        let debt_before = self.debt_in();

        for asset in self.owed_assets() {
            if self.account.free(&asset) > Decimal::ZERO {
                let owed = self.owed_of(&asset);
                let balance = self.account.repay_loan(&asset, owed, time_exchange);
                events.push(self.build_account_event(Snapshot(balance)));
            }
        }

        let owed = self.owed_of(valuation);
        self.raise(valuation, owed, time_exchange, events);

        for asset in self.owed_assets() {
            if asset != *valuation {
                self.buy_back(&asset, valuation, time_exchange, events);
            }
        }

        // The prices have not moved since `debt_before`, and nothing here borrows, so every debt
        // is still valued and the difference is what this pass repaid.
        let repaid = (debt_before - self.debt_in()).max(Decimal::ZERO);
        let fee = liquidation.fee_rate() * repaid;
        if fee > Decimal::ZERO {
            self.charge_liquidation_fee(
                valuation,
                fee,
                liquidation.fee_rate(),
                repaid,
                time_exchange,
                events,
            );
        }

        let left = self.owed_assets();
        if left.is_empty() {
            self.liquidation.under_way = false;
            self.liquidation.warned_shortfall = false;
        } else if !self.liquidation.warned_shortfall {
            self.liquidation.warned_shortfall = true;
            warn!(
                exchange = %self.exchange,
                assets = ?left,
                "SimulatedVenue could not repay every debt of the spot margin account it is \
                 liquidating, and leaves the rest owed: it continues on later inputs; warned once \
                 per liquidation"
            );
        }
    }

    /// Sells the account's other assets for `need` of `valuation`, in name order and as much of
    /// each as is needed, stopping when it has raised `need` or run out of assets to sell. Nothing
    /// when `need` is not positive.
    fn raise(
        &mut self,
        valuation: &AssetNameExchange,
        mut need: Decimal,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let mut held = self
            .account
            .balances()
            .filter(|balance| balance.asset != *valuation && balance.balance.free > Decimal::ZERO)
            .map(|balance| balance.asset.clone())
            .collect::<Vec<_>>();
        held.sort_unstable();

        for asset in held {
            if need <= Decimal::ZERO {
                return;
            }
            if let Some(leg) = self.leg(&asset, valuation, need) {
                need -= self.fill_leg(&leg, need, time_exchange, events);
            }
        }
    }

    /// Buys back what the account still owes of `asset`, paying `valuation`, after raising what
    /// the buy costs beyond the account's free `valuation`.
    fn buy_back(
        &mut self,
        asset: &AssetNameExchange,
        valuation: &AssetNameExchange,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let owed = self.owed_of(asset);
        if owed <= Decimal::ZERO {
            return;
        }
        let Some(leg) = self.leg(valuation, asset, owed) else {
            return;
        };
        let short = self.leg_cost(&leg, owed) - self.account.free(valuation);
        self.raise(valuation, short, time_exchange, events);
        self.fill_leg(&leg, owed, time_exchange, events);
    }

    /// Charges `fee`, `rate` times the `repaid` value of debt, in `valuation`, as
    /// `[balance, cash flow]`, raising what the account's free `valuation` lacks first. A fee it
    /// still cannot pay takes the free balance below zero, with a WARN.
    fn charge_liquidation_fee(
        &mut self,
        valuation: &AssetNameExchange,
        fee: Decimal,
        rate: Decimal,
        repaid: Decimal,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let short = fee - self.account.free(valuation);
        self.raise(valuation, short, time_exchange, events);

        // A delivery that takes, opening the balance in an account that never held the
        // valuation asset, and taking `free` below zero rather than being refused, as a charge
        // does.
        let delivery = Delivery {
            asset: valuation.clone(),
            amount: -fee,
        };
        let balance = self
            .account
            .deliver(&delivery, MarginEffect::NoSideEffect, time_exchange);
        if balance.balance.free.is_sign_negative() {
            warn!(
                exchange = %self.exchange,
                asset = %valuation,
                %fee,
                free = %balance.balance.free,
                "SimulatedVenue charged a liquidation fee the account could not cover: free \
                 balance is negative"
            );
        }

        let id = CashFlowId::new(self.cash_flow_sequence.to_smolstr());
        self.cash_flow_sequence += 1;
        events.push(self.build_account_event(Snapshot(balance)));
        events.push(
            self.build_account_event(AccountEventKind::CashFlow(CashFlow::new(
                CashFlowKind::LiquidationFee {
                    rate: Some(rate),
                    repaid: Some(repaid),
                },
                valuation.clone(),
                -fee,
                None,
                time_exchange,
                Some(id),
            ))),
        );
    }

    /// A forced trade paying `pay` for `receive`, priced for about `want` of `receive`, or `None`
    /// if no spot instrument pairing the two has a market here that prices it, or what it
    /// delivers is not worth more than its fee: at one unit under a fee model linear in quantity,
    /// at any quantity up to the [search limit](search_limit) under one that is not.
    ///
    /// The instrument is the first by name, either way round. Priced by the fill model and
    /// slipped, as a market order on arrival is, which draws from the slippage stream.
    fn leg(
        &mut self,
        pay: &AssetNameExchange,
        receive: &AssetNameExchange,
        want: Decimal,
    ) -> Option<Leg> {
        // Ordered by name. Seen from `pay`, a direct pair sells it as base, an inverse one buys
        // `receive` with it as quote.
        let (instrument, side) = self
            .spot_pairs
            .between(pay, receive)
            .iter()
            .find(|pair| self.market.contains_key(&pair.instrument))
            .map(|pair| {
                let side = match pair.inverse {
                    true => Side::Buy,
                    false => Side::Sell,
                };
                (pair.instrument.clone(), side)
            })?;
        let snapshot = self.market(&instrument)?.snapshot;
        // `instrument_terms` refuses only an instrument it cannot settle, and this one is spot.
        let terms = self.instrument_terms(&instrument).ok()?;

        // The model sees roughly the quantity the leg trades, in base: `want` when base is what
        // it receives, else `want` at the market's mid.
        let estimate = match (side, mid_or_last(&snapshot)) {
            (Side::Buy, _) | (Side::Sell, None) => want,
            (Side::Sell, Some(mid)) => want / mid,
        };
        let reference = self
            .fill_model
            .fill_price(&FillContext::new(side, estimate, &snapshot))?;
        let price = self.slippage.apply(side, reference);
        if price <= Decimal::ZERO {
            return None;
        }

        // One unit's settlement prices any quantity when the fee model is linear in quantity, and
        // gives a quantity to start from when it is not; see `fill_leg`. A charge per order can
        // outweigh what one unit delivers, so under such a model the probe doubles until what it
        // delivers is positive, and its average per unit seeds the search.
        let probe = |quantity| {
            self.fill_settlement(
                &terms,
                NetPosition::default(),
                side,
                quantity,
                price,
                FeePricing::first(Liquidity::Taker, quantity, self.time_exchange()),
            )
        };
        let mut quantity = Decimal::ONE;
        let mut sample = probe(quantity);
        if !self.fee_model_for(&terms).is_linear() {
            let limit = search_limit(sample.amount, price);
            while delivered(&sample.credit) <= Decimal::ZERO {
                quantity = quantity.checked_mul(Decimal::TWO).filter(|q| *q <= limit)?;
                sample = probe(quantity);
            }
        }
        let delivered_per_unit = match sample.credit {
            Credit::Delivery(delivery) => delivery.amount / quantity,
            Credit::PaidBack(_) => return None,
        };
        let paid_per_unit = sample.amount / quantity;
        if delivered_per_unit <= Decimal::ZERO || paid_per_unit <= Decimal::ZERO {
            return None;
        }

        Some(Leg {
            instrument,
            terms,
            side,
            pay: pay.clone(),
            price,
            paid_per_unit,
            delivered_per_unit,
        })
    }

    /// Fills `leg` for `want` of the asset it receives, or for as much as the account's free
    /// balance of the asset it pays, and the depth on offer, allow, as `[balance.., trade]`.
    /// Returns what it delivered, all of which repays the debt of that asset first.
    fn fill_leg(
        &mut self,
        leg: &Leg,
        want: Decimal,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) -> Decimal {
        let budget = self.account.free(&leg.pay);
        let depth = self.depth_available(&leg.instrument, leg.side);
        let affordable = if self.fee_model_for(&leg.terms).is_linear() {
            self.linear_leg_quantity(leg, want, budget, depth)
        } else {
            self.searched_leg_quantity(leg, want, budget, depth)
        };
        let Some((quantity, settlement)) = affordable else {
            return Decimal::ZERO;
        };

        // `Repay`, so what the trade delivers into an asset the account owes repays that loan.
        // Nothing borrows, and the account covers the debit, so this is not refused.
        let Ok(mut commitment) = self.commit(
            &settlement.settled(),
            Some(&settlement.credit),
            MarginEffect::Repay,
            &leg.terms,
            None,
            // `Repay` borrows nothing, so there is no loan to link to the order.
            None,
            time_exchange,
        ) else {
            return Decimal::ZERO;
        };
        self.take_fee_apart(&settlement, &mut commitment, time_exchange);
        self.consume_depth(&leg.instrument, leg.side, quantity);

        let order_id = self.order_id_sequence_fetch_add();
        let trade = Trade {
            id: self.trade_id_sequence_fetch_add(),
            order_id,
            instrument: leg.instrument.clone(),
            strategy: StrategyId::unknown(),
            time_exchange,
            side: leg.side,
            price: leg.price,
            quantity,
            order_filled_quantity: Some(quantity),
            fees: settlement.fees.clone(),
            origin: TradeOrigin::Liquidation,
            position_side: None,
            client_order_id: None,
            time_source: TimeSource::Venue,
        };
        self.account.ack_trade(trade.clone());

        self.push_committed(events, commitment, Some(trade));
        delivered(&settlement.credit)
    }

    /// What filling `quantity` of `leg` costs and delivers, as its own order's only fill.
    ///
    /// Priced at the venue's clock, as every leg is: [`check_margin`](Self::check_margin) moves the
    /// clock to its `time_exchange` before it liquidates, so that is the fill's time.
    fn price_leg(&self, leg: &Leg, quantity: Decimal) -> Settlement {
        self.fill_settlement(
            &leg.terms,
            NetPosition::default(),
            leg.side,
            quantity,
            leg.price,
            FeePricing::first(Liquidity::Taker, quantity, self.time_exchange()),
        )
    }

    /// What `leg` costs in what it pays with to deliver `want` of what it receives. Zero when no
    /// quantity up to the search limit delivers `want`, so that nothing is raised for a buy that
    /// cannot clear the debt: the buy then takes what the free balance pays for.
    fn leg_cost(&self, leg: &Leg, want: Decimal) -> Decimal {
        if self.fee_model_for(&leg.terms).is_linear() {
            return want / leg.delivered_per_unit * leg.paid_per_unit;
        }
        self.needed_leg_quantity(leg, want)
            .map_or(Decimal::ZERO, |quantity| {
                self.price_leg(leg, quantity).amount
            })
    }

    /// The least quantity of `leg` that delivers `want`, bisected on the settlement, or `None` if
    /// none up to its [search limit](Leg::search_limit) does. Under a fee charged only from an
    /// order size it delivers `want` but may not be the least; see
    /// [`searched_leg_quantity`](Self::searched_leg_quantity).
    fn needed_leg_quantity(&self, leg: &Leg, want: Decimal) -> Option<Decimal> {
        let delivers = |quantity| delivered(&self.price_leg(leg, quantity).credit) >= want;
        bracket(want / leg.delivered_per_unit, leg.search_limit(), delivers)
            .map(|high| least(high, delivers))
    }

    /// The quantity of `leg` that delivers `want`, or the most that `budget` and the `depth` on
    /// offer allow, under a fee model linear in quantity, with what it settles. `None` if nothing
    /// is affordable.
    ///
    /// One unit's settlement prices every quantity, so the quantity is a division, then nudged by
    /// its last digit to absorb the division's rounding.
    fn linear_leg_quantity(
        &self,
        leg: &Leg,
        want: Decimal,
        budget: Decimal,
        depth: Option<Decimal>,
    ) -> Option<(Decimal, Settlement)> {
        let mut cap = budget / leg.paid_per_unit;
        if let Some(available) = depth {
            cap = cap.min(available);
        }
        let mut quantity = (want / leg.delivered_per_unit).min(cap);

        // The divisions round, so the quantity is nudged by its last digit until it delivers all
        // of `want` without costing more than the account holds. A quantity that costs too much
        // lowers the cap, so the nudge never climbs back to it. The largest affordable quantity
        // tried is traded, which may deliver a digit short of `want`.
        let mut affordable = None;
        for _ in 0..4 {
            if quantity <= Decimal::ZERO {
                break;
            }
            let priced = self.price_leg(leg, quantity);
            let step = Decimal::new(1, quantity.scale());
            if priced.amount > budget {
                cap = quantity - step;
                quantity = cap;
                continue;
            }
            let short = delivered(&priced.credit) < want && quantity + step <= cap;
            affordable = Some((quantity, priced));
            if !short {
                break;
            }
            quantity += step;
        }
        affordable
    }

    /// As [`linear_leg_quantity`](Self::linear_leg_quantity), under a fee model that is not linear
    /// in quantity: one charging per order, bounded per order, or charging only from an order
    /// size.
    ///
    /// What a fill costs grows with its quantity, and so, but for an order size threshold, does
    /// what it delivers, so each is found by bisecting on the settlement itself: the least
    /// quantity that delivers `want`, else the most that `budget` pays for, within the `depth` on
    /// offer. Bisection lands a digit from the bound, so the result is then rounded up to the
    /// fewest decimal places that still meet it: a quantity that delivers exactly `want`, or one
    /// `budget` still pays for. `None` also when what `budget` pays for delivers nothing, its fee
    /// outweighing it.
    ///
    /// A fee charged only from an order size, taken from what a sale receives, makes what the
    /// sale delivers drop where its quantity reaches the threshold. The bisection keeps a quantity
    /// that delivers `want` throughout, so the leg still delivers it, but the quantity found may
    /// lie past the threshold when a smaller one below it would have done: the sale is then
    /// larger than it needed to be, and pays the fee.
    fn searched_leg_quantity(
        &self,
        leg: &Leg,
        want: Decimal,
        budget: Decimal,
        depth: Option<Decimal>,
    ) -> Option<(Decimal, Settlement)> {
        let pays = |quantity| self.price_leg(leg, quantity).amount <= budget;
        let within = |quantity| depth.is_none_or(|depth| quantity <= depth);

        let needed = self.needed_leg_quantity(leg, want);
        let quantity = match needed {
            Some(needed) if within(needed) && pays(needed) => shortest_round_up(needed, |round| {
                within(round) && {
                    let priced = self.price_leg(leg, round);
                    priced.amount <= budget && delivered(&priced.credit) == want
                }
            }),
            needed => {
                // What the account can pay for, short of what it needs: at most `needed` when that
                // is known, and the depth, else up to the first doubling it cannot pay for.
                let high = match (needed, depth) {
                    (Some(needed), Some(depth)) => needed.min(depth),
                    (Some(needed), None) => needed,
                    // The depth is only observed, so it is held to the search limit.
                    (None, Some(depth)) => depth.min(leg.search_limit()),
                    (None, None) => {
                        bracket(budget / leg.paid_per_unit, leg.search_limit(), |quantity| {
                            !pays(quantity)
                        })?
                    }
                };
                if pays(high) {
                    high
                } else {
                    let low = greatest(high, pays);
                    shortest_round_up(low, |round| round <= high && pays(round))
                }
            }
        };
        // A quantity short of `want` can deliver nothing, or less, when a fee taken in what it
        // receives outweighs it. Trading it would spend the budget for nothing, and a sale would
        // take the rest of its fee from the very balance it was raising.
        (quantity > Decimal::ZERO)
            .then(|| (quantity, self.price_leg(leg, quantity)))
            .filter(|(_, settlement)| delivered(&settlement.credit) > Decimal::ZERO)
    }

    /// Every asset the account owes, by name.
    fn owed_assets(&self) -> Vec<AssetNameExchange> {
        let mut assets = self
            .account
            .balances()
            .filter(|balance| owed(balance) > Decimal::ZERO)
            .map(|balance| balance.asset.clone())
            .collect::<Vec<_>>();
        assets.sort_unstable();
        assets
    }

    /// What the account owes of `asset`, loan plus interest.
    fn owed_of(&self, asset: &AssetNameExchange) -> Decimal {
        self.account
            .balances()
            .find(|balance| balance.asset == *asset)
            .map_or(Decimal::ZERO, owed)
    }

    /// The account's debt valued in its spot margin configuration's valuation asset, zero if a
    /// debt cannot be valued or the venue has no spot margin.
    fn debt_in(&self) -> Decimal {
        self.spot_margin
            .as_ref()
            // The ratios weight only equity, so either check values the debt alike.
            .and_then(|config| self.valued(config, MarginCheck::Liquidation, None, Prices::Current))
            .map_or(Decimal::ZERO, |valued| valued.debt)
    }

    /// An account notice of `kind`, for the account as a whole, named as its kind is.
    fn notice(
        &self,
        kind: NoticeKind,
        margin_level: Option<Decimal>,
        time_exchange: DateTime<Utc>,
    ) -> UnindexedAccountEvent {
        self.build_account_event(AccountEventKind::Notice(AccountNotice::new(
            kind,
            None,
            time_exchange,
            margin_level,
            kind.to_smolstr(),
        )))
    }
}

/// What a spot settlement's credit delivers, in the asset it receives.
fn delivered(credit: &Credit) -> Decimal {
    match credit {
        Credit::Delivery(delivery) => delivery.amount,
        Credit::PaidBack(_) => Decimal::ZERO,
    }
}

/// `start`, doubled until `reached` holds of it, or `None` if it does of none up to `limit`.
///
/// A `start` that is not above zero, such as a dust amount that its division rounded to zero,
/// starts from the least positive `Decimal` instead.
fn bracket(start: Decimal, limit: Decimal, reached: impl Fn(Decimal) -> bool) -> Option<Decimal> {
    let mut quantity = start.max(Decimal::new(1, Decimal::MAX_SCALE));
    while quantity <= limit {
        if reached(quantity) {
            return Some(quantity);
        }
        quantity = quantity.checked_mul(Decimal::TWO)?;
    }
    None
}

/// The least quantity in `(0, high]` of which `holds` holds, to `Decimal`'s precision, given that
/// it holds of `high` and of every quantity above the least.
fn least(high: Decimal, holds: impl Fn(Decimal) -> bool) -> Decimal {
    let (mut low, mut high) = (Decimal::ZERO, high);
    for _ in 0..256 {
        let middle = low + (high - low) / Decimal::TWO;
        if middle <= low || middle >= high {
            break;
        }
        if holds(middle) {
            high = middle;
        } else {
            low = middle;
        }
    }
    high
}

/// The greatest quantity in `[0, high]` of which `holds` holds, to `Decimal`'s precision, given
/// that it holds of every quantity below the greatest. Zero if it holds of none above zero.
fn greatest(high: Decimal, holds: impl Fn(Decimal) -> bool) -> Decimal {
    let (mut low, mut high) = (Decimal::ZERO, high);
    for _ in 0..256 {
        let middle = low + (high - low) / Decimal::TWO;
        if middle <= low || middle >= high {
            break;
        }
        if holds(middle) {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

/// `quantity` rounded up to the fewest decimal places for which `accept` holds of it, or
/// `quantity` itself if it holds at none.
fn shortest_round_up(quantity: Decimal, accept: impl Fn(Decimal) -> bool) -> Decimal {
    (0..=Decimal::MAX_SCALE)
        .map(|scale| quantity.round_dp_with_strategy(scale, RoundingStrategy::ToPositiveInfinity))
        .find(|&round| accept(round))
        .unwrap_or(quantity)
}
