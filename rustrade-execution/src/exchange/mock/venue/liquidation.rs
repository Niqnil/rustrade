//! Liquidating the simulated venue's spot margin account, and the margin-call notices before it.
//! See [`SpotMarginLiquidation`] for what the venue does, and when.

use super::{InstrumentTerms, NetPosition, SimulatedVenue, Valued, mid_or_last, owed};
use crate::{
    AccountEventKind, UnindexedAccountEvent,
    cash_flow::{CashFlow, CashFlowId, CashFlowKind},
    exchange::mock::account::{Credit, Delivery},
    fee::Liquidity,
    fill::FillContext,
    margin::{MarginEffect, SpotMarginLiquidation},
    notice::{AccountNotice, NoticeKind},
    order::id::{ClientOrderId, StrategyId},
    trade::{TimeSource, Trade, TradeOrigin},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    instrument::{kind::InstrumentKind, name::InstrumentNameExchange},
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

/// One forced trade: paying [`pay`](Self::pay) for the other asset of
/// [`instrument`](Self::instrument), at [`price`](Self::price).
struct Leg {
    instrument: InstrumentNameExchange,
    terms: InstrumentTerms,
    side: Side,
    pay: AssetNameExchange,
    price: Decimal,
    /// What one unit of base costs in [`pay`](Self::pay), fee included where it is charged in it.
    paid_per_unit: Decimal,
    /// What one unit of base delivers of the asset received, net of any fee charged in it.
    delivered_per_unit: Decimal,
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
        let Some(liquidation) = config.liquidation().cloned() else {
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

        let Some(Valued { equity, debt }) = self.valued(&valuation, None) else {
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
        // Each asset owed is valued at a positive price, so an account that owes has a debt.
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

        self.cancel_every_order(time_exchange, events);
        // Valued after the cancels, so the fee is charged on what the liquidation repaid, not on
        // the loans the cancelled orders gave back.
        let debt_before = self.debt_in(valuation);

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
        let repaid = (debt_before - self.debt_in(valuation)).max(Decimal::ZERO);
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

    /// Cancels every open order, resting or held, releasing what each held, as `[balance, order]`
    /// per order, or `[order]` for one that held nothing. By client order id, so every run cancels
    /// in the same order.
    fn cancel_every_order(
        &mut self,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let mut open = self
            .account
            .orders()
            .iter()
            .map(|order| order.key.cid.clone())
            .collect::<Vec<ClientOrderId>>();
        open.sort_unstable();

        for cid in open {
            // Collected from the book with nothing in between, so this always finds it.
            if let Some(resting) = self.account.remove_order(&cid) {
                let released = resting
                    .reservation
                    .map(|reservation| self.release_reservation(&reservation, time_exchange));
                events.extend(self.cancel_unfilled(resting.order, released, time_exchange));
            }
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
        let cost = owed / leg.delivered_per_unit * leg.paid_per_unit;
        let short = cost - self.account.free(valuation);
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
    /// delivers is not worth more than its fee.
    ///
    /// The instrument is the first by name, either way round. Priced by the fill model and
    /// slipped, as a market order on arrival is, which draws from the slippage stream.
    fn leg(
        &mut self,
        pay: &AssetNameExchange,
        receive: &AssetNameExchange,
        want: Decimal,
    ) -> Option<Leg> {
        let (instrument, side) = self
            .instruments
            .iter()
            .filter(|(name, instrument)| {
                matches!(instrument.kind, InstrumentKind::Spot) && self.market.contains_key(*name)
            })
            .filter_map(|(name, instrument)| {
                let underlying = &instrument.underlying;
                let side = match (&underlying.base, &underlying.quote) {
                    (base, quote) if base == receive && quote == pay => Side::Buy,
                    (base, quote) if base == pay && quote == receive => Side::Sell,
                    _ => return None,
                };
                Some((name, side))
            })
            .min_by(|(a, _), (b, _)| a.cmp(b))
            .map(|(name, side)| (name.clone(), side))?;
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

        // Every fee model is linear in quantity, so one unit's settlement prices any quantity.
        let one = self.fill_settlement(
            &terms,
            NetPosition::default(),
            side,
            Decimal::ONE,
            price,
            Liquidity::Taker,
        );
        let delivered_per_unit = match one.credit {
            Credit::Delivery(delivery) => delivery.amount,
            Credit::PaidBack(_) => return None,
        };
        if delivered_per_unit <= Decimal::ZERO || one.amount <= Decimal::ZERO {
            return None;
        }

        Some(Leg {
            instrument,
            terms,
            side,
            pay: pay.clone(),
            price,
            paid_per_unit: one.amount,
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
        let mut cap = budget / leg.paid_per_unit;
        if let Some(available) = self.depth_available(&leg.instrument, leg.side) {
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
            let priced = self.fill_settlement(
                &leg.terms,
                NetPosition::default(),
                leg.side,
                quantity,
                leg.price,
                Liquidity::Taker,
            );
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

        self.push_committed(events, commitment);
        events.push(self.build_account_event(AccountEventKind::Trade(trade)));
        delivered(&settlement.credit)
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

    /// The account's debt valued in `valuation`, zero if a debt cannot be valued.
    fn debt_in(&self, valuation: &AssetNameExchange) -> Decimal {
        self.valued(valuation, None)
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
