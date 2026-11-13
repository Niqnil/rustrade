//! Selling a long option the account cannot afford to exercise, before it expires. See
//! [`ExpirySellOut`] for what the venue does, and when.

use super::{
    ExpirySellOut, FeePricing, Fill, InstrumentTerms, SellOutAffordability, SimulatedVenue,
    intrinsic,
};
use crate::{
    UnindexedAccountEvent,
    fee::Liquidity,
    fill::FillContext,
    order::id::StrategyId,
    trade::{TimeSource, Trade, TradeOrigin},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    instrument::{
        kind::option::{OptionKind, OptionSettlement},
        name::InstrumentNameExchange,
    },
};
use tracing::warn;

impl SimulatedVenue {
    /// Runs the [`ExpirySellOut`] check of each option in `instruments`, all due at `at`, in
    /// order, pushing what each sale moves onto `events`. Nothing without a configured sell-out,
    /// and nothing for an option the account holds no long in.
    ///
    /// The options share what the account has to exercise with: a budget per asset, read when it
    /// is first needed. Each option spends on what goes on to expiry, the contracts kept and any
    /// its sale left unsold, so a later one sees only what an earlier one left. A sale's proceeds
    /// are not added to it.
    pub(super) fn sell_out(
        &mut self,
        at: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let Some(config) = self.expiry_sell_out else {
            return;
        };
        let mut budgets = FnvHashMap::<AssetNameExchange, ExerciseBudget>::default();
        for instrument in instruments {
            let Some(contracts) = self
                .positions
                .get(instrument)
                .filter(|position| position.is_long())
                .map(|position| position.quantity)
            else {
                continue;
            };
            let Some((terms, contract)) = self.option_terms(instrument) else {
                continue;
            };
            debug_assert!(
                contract.settlement == OptionSettlement::Physical,
                "only a physically settled option is checked for a sell-out"
            );
            let Some((_, price)) = self.underlying_price(instrument) else {
                warn!(
                    instrument = %instrument,
                    %at,
                    "SimulatedVenue cannot check a long option for an expiry sell-out: its \
                     underlying has no price, so it cannot tell whether the option is in the \
                     money. Nothing is sold; the position goes on to expiry"
                );
                continue;
            };
            if intrinsic(&contract, price) <= Decimal::ZERO {
                continue;
            }

            // What exercising one contract takes: a call's strike in quote, a put's shares.
            let (asset, per_contract) = match contract.kind {
                OptionKind::Call => (
                    terms.underlying.quote.clone(),
                    contract.strike * contract.contract_size,
                ),
                OptionKind::Put => (terms.underlying.base.clone(), contract.contract_size),
            };
            let budget = match budgets.get_mut(&asset) {
                Some(budget) => budget,
                None => {
                    let budget = self.exercise_budget(&asset);
                    budgets.entry(asset).or_insert(budget)
                }
            };
            let kept = affordable(
                config.affordability(),
                contracts,
                per_contract,
                budget.available(contract.kind),
            );

            let to_sell = contracts - kept;
            let unsold = match to_sell > Decimal::ZERO {
                true => {
                    to_sell - self.sell_out_option(instrument, &terms, to_sell, config, at, events)
                }
                false => Decimal::ZERO,
            };
            budget.spend(contract.kind, (kept + unsold) * per_contract);
        }
    }

    /// What the account has to exercise with in `asset`: its free balance, and, on a venue with
    /// spot margin, what the venue would lend for a buy (its leverage limit, and the asset's
    /// borrow cap; nothing while a liquidation is under way).
    fn exercise_budget(&self, asset: &AssetNameExchange) -> ExerciseBudget {
        let free = self.account.free(asset).max(Decimal::ZERO);
        let lendable = match self.spot_margin.as_ref() {
            Some(config) if !self.liquidation.under_way() => {
                // A debt this venue cannot value could be any size, so it lends nothing more.
                let room = self
                    .leverage_room(config, asset, None)
                    .unwrap_or(Decimal::ZERO);
                config
                    .max_borrow(asset)
                    .map_or(room, |cap| room.min(self.account.cap_room(asset, cap)))
            }
            _ => Decimal::ZERO,
        };
        ExerciseBudget {
            free,
            lendable: lendable.max(Decimal::ZERO),
        }
    }

    /// Sells `quantity` of the long option `instrument`, or as much as the depth on offer allows,
    /// as a market sell fills, its price cut by `config`'s haircut, after cancelling the orders
    /// still working on it, as `[order.., balance.., trade]`, returning the quantity sold. A sale
    /// that cannot be priced or finds no depth sells nothing and cancels nothing, with a WARN. One
    /// whose fee the account cannot pay, with those orders cancelled, leaves them cancelled and
    /// sells nothing, with a WARN.
    fn sell_out_option(
        &mut self,
        instrument: &InstrumentNameExchange,
        terms: &InstrumentTerms,
        quantity: Decimal,
        config: ExpirySellOut,
        at: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) -> Decimal {
        let not_sold = |reason: &str| {
            warn!(
                instrument = %instrument,
                %at,
                %quantity,
                "SimulatedVenue cannot sell out a long option it would sell before expiry: \
                 {reason}. It goes on to expiry"
            );
            Decimal::ZERO
        };
        let Some(snapshot) = self.market.get(instrument).map(|market| market.snapshot) else {
            return not_sold("the option has no market");
        };
        let Some(reference) =
            self.fill_model
                .fill_price(&FillContext::new(Side::Sell, quantity, &snapshot))
        else {
            return not_sold("the fill model gives no price to sell at");
        };
        let price = self.slippage.apply(Side::Sell, reference) * (Decimal::ONE - config.haircut());
        // As no order fills an option at nothing.
        if price <= Decimal::ZERO {
            return not_sold("its price is not positive");
        }
        let quantity = self
            .depth_available(instrument, Side::Sell)
            .map_or(quantity, |available| quantity.min(available));
        if quantity <= Decimal::ZERO {
            return not_sold("the market has no depth left to sell into");
        }

        // The orders cancelled first, so the position sold is the whole of what is held, and an
        // order resting to sell it cannot later open a short.
        let position = self.positions.get(instrument).copied().unwrap_or_default();
        let settlement = self.fill_settlement(
            terms,
            position,
            Side::Sell,
            quantity,
            price,
            FeePricing::first(Liquidity::Taker, at),
        );
        let mut cancelled = Vec::new();
        self.cancel_orders(|working| working == instrument, at, &mut cancelled);
        let Ok(mut commitment) = self.commit(
            &settlement.settled(),
            Some(&settlement.credit),
            self.margin_effect(None),
            terms,
            Some(&snapshot),
            // A sale of a long borrows nothing, so there is no loan to link to an order.
            None,
            at,
        ) else {
            events.append(&mut cancelled);
            return not_sold("the account cannot pay its fee");
        };
        events.append(&mut cancelled);
        self.take_fee_apart(&settlement, &mut commitment, at);
        self.record_fill(terms, instrument, Side::Sell, Fill { quantity, price });
        self.consume_depth(instrument, Side::Sell, quantity);

        let order_id = self.order_id_sequence_fetch_add();
        let trade = Trade {
            id: self.trade_id_sequence_fetch_add(),
            order_id,
            instrument: instrument.clone(),
            strategy: StrategyId::unknown(),
            time_exchange: at,
            side: Side::Sell,
            price,
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
        quantity
    }
}

/// What the account has to exercise with in one asset, as the options checked at one instant
/// spend it.
#[derive(Debug, Clone, Copy)]
pub(super) struct ExerciseBudget {
    /// The free balance not yet spent.
    pub(super) free: Decimal,
    /// What the venue would still lend for a buy, which only a call's exercise draws on.
    pub(super) lendable: Decimal,
}

impl ExerciseBudget {
    /// What exercising an option of `kind` may take: a call's strike may also be lent, a put's
    /// shares never are.
    pub(super) fn available(&self, kind: OptionKind) -> Decimal {
        match kind {
            OptionKind::Call => self.free + self.lendable,
            OptionKind::Put => self.free,
        }
    }

    /// Spends `amount` on exercising an option of `kind`, out of the free balance first, then,
    /// for a call, what would be lent. More than is available leaves nothing.
    pub(super) fn spend(&mut self, kind: OptionKind, amount: Decimal) {
        let from_free = amount.min(self.free);
        self.free -= from_free;
        if kind == OptionKind::Call {
            self.lendable = (self.lendable - (amount - from_free)).max(Decimal::ZERO);
        }
    }
}

/// How many of `contracts` the account keeps for exercise with `budget`, each taking
/// `per_contract` of it, under `affordability`.
pub(super) fn affordable(
    affordability: SellOutAffordability,
    contracts: Decimal,
    per_contract: Decimal,
    budget: Decimal,
) -> Decimal {
    if contracts * per_contract <= budget {
        return contracts;
    }
    match affordability {
        SellOutAffordability::PerPosition => Decimal::ZERO,
        // Fewer than all, so the whole contracts the budget pays for. A contract costing nothing
        // was kept whole above.
        SellOutAffordability::PerContract => (budget / per_contract).floor(),
    }
}
