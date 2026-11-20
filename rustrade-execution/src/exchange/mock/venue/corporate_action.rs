//! A corporate action applied to the venue. See [`SimulatedVenue::apply_corporate_action`].

use super::{
    NetPosition, Reentry, Release, SimulatedVenue, VenueInstrumentMarket, VenueOutcome,
    booked_report, mid_or_last,
};
use crate::{
    UnindexedAccountEvent,
    balance::{Balance, MarginDetails},
    client::mock::SplitAction,
    exchange::mock::{
        account::Delivery,
        orders::{NotQueued, OpenOrder, Rank, RestingOrder},
    },
    margin::MarginEffect,
    market::{MarketBar, MarketDepth, MarketSnapshot},
    order::{
        Order, OrderKind,
        id::ClientOrderId,
        state::{Open, OrderState},
    },
};
use chrono::{DateTime, Utc};
use fnv::FnvHashSet;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    corporate_action::{CorporateActionKind, SplitAdjustmentKind, SplitRatio, SplitRoundingPolicy},
    instrument::{kind::InstrumentKind, name::InstrumentNameExchange, round_toward_zero_to_step},
};
use rustrade_integration::collection::snapshot::Snapshot;
use smol_str::SmolStr;
use tracing::warn;

/// Response type of [`SimulatedVenue::apply_corporate_action`].
pub type CorporateActionOutcome = VenueOutcome<CorporateActionResponse>;

/// What [`SimulatedVenue::apply_corporate_action`] did with an action.
///
/// `#[non_exhaustive]`, so an outcome can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorporateActionResponse {
    /// The venue applied the action and recorded its id.
    Applied,
    /// The venue had already applied an action under this id, so it changed nothing.
    AlreadyApplied,
    /// The venue did not apply the action. It changed nothing and did not record the id, so the
    /// same action can be applied once the reason is fixed.
    Refused(CorporateActionRefusal),
}

/// Why [`SimulatedVenue::apply_corporate_action`] refused an action.
///
/// `#[non_exhaustive]`, so a reason can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorporateActionRefusal {
    /// The venue does not trade the instrument.
    UnknownInstrument,
    /// The instrument is not the deliverable equity: see
    /// [`InstrumentKind::is_split_eligible`](rustrade_instrument::instrument::kind::InstrumentKind::is_split_eligible).
    InstrumentKindNotSupported,
    /// The venue does not model this kind of action.
    KindNotSupported,
    /// An order on another instrument holds the equity, as a sell of it against another quote
    /// asset does. The venue would have to restate that order's hold with the split, which it
    /// does not model. Cancel the order, then apply the action again.
    HeldByAnotherInstrument,
    /// Under [`SplitRoundingPolicy::Floor`], the split leaves fractional what the account owes of
    /// the equity, or a holding below zero. How a whole-share broker settles a fractional short is
    /// not modelled.
    FractionalShortUnderFloor,
    /// Under [`SplitRoundingPolicy::Floor`], the split leaves a fractional share to pay out as
    /// cash in lieu, and the venue holds no market for the equity to price it at. Feed the venue
    /// a market for it with [`apply_market`](SimulatedVenue::apply_market), then apply the action
    /// again. A [`RequestPriced`](super::VenueRegime::RequestPriced) venue is fed
    /// no market, so it applies such a split only under
    /// [`Fractional`](SplitRoundingPolicy::Fractional), or for a holding the split leaves whole.
    NoPriceForCashInLieu,
    /// A holding or a price the split restates does not fit a [`Decimal`].
    Overflow,
    /// A [non-standard](SplitAdjustmentKind::NonStandard) split (a reverse split, or a forward
    /// split by a ratio that is not a whole number), while the account holds a position in, or
    /// has an order on, an option on the equity. Such a split changes what each option contract
    /// delivers rather than its strike and contract count, and listed options then trade under a
    /// new symbol, closing only. The venue does not model that. Close the positions and cancel
    /// the orders, then apply the action again.
    OptionExposureAtNonStandardSplit,
}

impl SimulatedVenue {
    /// Applies the corporate action `id` of `kind` to `instrument`, effective at `time_exchange`,
    /// as the venue that lists it would, rounding a share count it leaves fractional as `policy`
    /// says.
    ///
    /// # What a stock split does
    /// A split of `ratio` multiplies every quantity of the equity by `ratio` and divides every
    /// price of it by `ratio`. In order:
    /// - **Orders:** under the venue's [`SplitAction`] for the split (see
    ///   [`MockExecutionConfig::split`](crate::client::mock::MockExecutionConfig::split)), each
    ///   order on `instrument` is cancelled, releasing what it held, or, under
    ///   [`Adjust`](SplitAction::Adjust), a resting buy limit order is adjusted. A cancel is
    ///   reported as for a client's cancel: `[balance, cash flow.., order]`, the order an ended
    ///   [`Cancelled`](crate::order::state::Cancelled) snapshot stamped at the split, or `[order]`
    ///   alone for one that held nothing. Every order on an option on `instrument` (see below) is
    ///   cancelled the same way, whatever the [`SplitAction`]: options exchanges cancel the resting
    ///   orders in options a split adjusts. These cancels come first, by client order id.
    /// - **Holdings:** the balance of the equity, with what the account owes of it, is restated:
    ///   `total`, `free`, the loan's principal and its interest are each multiplied by `ratio`.
    ///   Under [`SplitRoundingPolicy::Floor`], `total` is then rounded down to a whole share, and
    ///   the fraction taken off `total` and `free` is paid out as cash in lieu, into the quote
    ///   asset's free balance, at the mid (else the last price) of the market the venue holds for
    ///   `instrument`, divided by `ratio`. That price is an approximation: a broker pays at a price
    ///   of its own, often the next day's. Reported as a balance snapshot of the equity, then one
    ///   of the quote asset for cash in lieu. A loan of the equity that changed is reported as
    ///   [`MarginDebt`](crate::AccountEventKind::MarginDebt), after everything else.
    /// - **Market:** the market the venue holds for `instrument` is restated, prices divided by
    ///   `ratio` and sizes multiplied by it, as is its borrow price window, so it values the
    ///   restated holdings. It trades nothing until the next [`apply_market`](Self::apply_market)
    ///   on `instrument`: see [`VenueInstrumentMarket::split_at`]. Until then a market order on
    ///   `instrument` is refused as unpriced and a limit order rests uncrossed, as on an instrument
    ///   the venue was never fed.
    /// - **Adjusted orders:** each buy limit order the action adjusts takes the terms
    ///   [`SplitAction::Adjust`] describes, and the venue recommits its hold for them, releasing
    ///   what the new hold no longer needs, as an amend does. It is reported as an amend is:
    ///   the restated balances, then the order's [`Open`] snapshot stamped at the split, with its
    ///   venue order id kept. The adjusted orders keep their order among themselves, best price
    ///   first, but go behind any order that later joins their price, as if they had arrived at
    ///   the split. An order seeded through
    ///   [`MockExecutionConfig::initial_state`](crate::client::mock::MockExecutionConfig::initial_state)
    ///   holds nothing, so only its terms change. One whose hold the account can no longer afford
    ///   (a fee schedule raised since it rested, a free balance charged below zero, or on a
    ///   reverse split the venue adjusts, a hold that grew) is cancelled instead, with a warning,
    ///   reported in its turn among the adjusted orders. That cancel releases only the quote
    ///   asset the buy order held, so it leaves the restated holding as it is.
    ///
    /// # What a split does to the options on the equity
    /// The options on `instrument` are the ones the venue lists whose underlying is
    /// `instrument`'s pair. What happens to them depends on the split's
    /// [`SplitAdjustmentKind`]:
    /// - **[`Standard`](SplitAdjustmentKind::Standard)** (a whole-number forward split): each
    ///   option is adjusted as listed options are. Its strike is divided by `ratio`, with its
    ///   contract size unchanged. The account's position in it is multiplied by `ratio`, and the
    ///   position's entry price divided by it. The market the venue holds for it is restated and
    ///   trades nothing until observed again, as the equity's is. The collateral a short holds
    ///   follows from the restated position and strike: a put holds the same cash, and a call the
    ///   restated shares. The strike is divided exactly. A clearing house rounds an adjusted
    ///   strike to an increment of its own, which the venue does not model. The venue reports no
    ///   event for these changes, as it reports none for a position: an engine applying the same
    ///   action restates its own positions in the options, and a consumer keeping positions
    ///   without one applies the same restatement. The restated strikes are part of the venue's
    ///   [`snapshot`](Self::snapshot), and [`instruments`](Self::instruments) reports them.
    /// - **[`NonStandard`](SplitAdjustmentKind::NonStandard)** (a reverse split, or a forward
    ///   split by a ratio that is not a whole number), or a kind this venue does not know:
    ///   refused with [`OptionExposureAtNonStandardSplit`] while the account holds a position in,
    ///   or has an order on, an option on `instrument`. Such a split changes what each contract
    ///   delivers, and the options then trade under a new symbol, closing only. With no such
    ///   position or order the split is applied and the options are left as they are, so an
    ///   order on one later trades it on its old terms. Stop trading them at the split.
    ///
    /// [`OptionExposureAtNonStandardSplit`]: CorporateActionRefusal::OptionExposureAtNonStandardSplit
    ///
    /// # What it does not do yet
    /// - **No minimum price or size:** an adjusted order is rounded to the tick and the quantity
    ///   step, but not checked against the instrument's minimums.
    ///
    /// # Idempotent by `id`
    /// The venue records the id of each action it applies, and changes nothing for an id it has
    /// already applied, answering [`AlreadyApplied`](CorporateActionResponse::AlreadyApplied). The
    /// record is part of the venue's [`snapshot`](Self::snapshot). Give each action, including a
    /// correction of an earlier one, an id of its own.
    ///
    /// # Refused
    /// Each [`CorporateActionRefusal`] is refused with a warning, before anything changes: nothing
    /// changes and the id is not recorded, so the same action can be applied once its reason is
    /// fixed.
    ///
    /// # Returns what the action caused, which a driver must deliver
    /// The clock is first moved to `time_exchange`, as by [`advance_time`](Self::advance_time),
    /// whatever the answer, and the events that move causes come first. An order whose deadline
    /// falls at the split therefore expires rather than being cancelled by it. What the split
    /// causes follows. `time_exchange` is clamped as `advance_time` clamps it, and everything the
    /// split reports is stamped at the clamped instant.
    ///
    /// # Panics
    /// As [`advance_time`](Self::advance_time).
    #[must_use]
    pub fn apply_corporate_action(
        &mut self,
        id: &str,
        instrument: &InstrumentNameExchange,
        kind: &CorporateActionKind,
        policy: SplitRoundingPolicy,
        time_exchange: DateTime<Utc>,
    ) -> CorporateActionOutcome {
        let mut events = self.advance_time(time_exchange);
        let time_exchange = self.time_exchange();

        let response = match self.corporate_action_refusal(id, instrument, kind) {
            Some(response) => response,
            None => {
                let CorporateActionKind::StockSplit { ratio } = kind else {
                    unreachable!("corporate_action_refusal refuses every kind but a split");
                };
                // A kind this venue does not know yet is taken as non-standard, as the engine
                // takes it: it touches no option.
                let options = match kind.split_kind() {
                    Some(SplitAdjustmentKind::Standard) => OptionSplit::Restate,
                    _ => OptionSplit::RefuseIfHeld,
                };
                match self.plan_split(instrument, *ratio, policy, options) {
                    Err(reason) => self.refuse_corporate_action(id, instrument, kind, reason),
                    Ok(plan) => {
                        self.commit_split(instrument, plan, time_exchange, &mut events);
                        self.corporate_actions_applied.insert(SmolStr::new(id));
                        CorporateActionResponse::Applied
                    }
                }
            }
        };

        VenueOutcome { events, response }
    }

    /// The answer to an action this venue does not apply, with the warning it gives, or `None` for
    /// one it applies. Checked in the engine's order: the instrument, then the id, then the
    /// instrument's kind, then the action's.
    fn corporate_action_refusal(
        &self,
        id: &str,
        instrument: &InstrumentNameExchange,
        kind: &CorporateActionKind,
    ) -> Option<CorporateActionResponse> {
        let refuse = |reason| Some(self.refuse_corporate_action(id, instrument, kind, reason));

        let Some(traded) = self.instruments.get(instrument) else {
            return refuse(CorporateActionRefusal::UnknownInstrument);
        };
        if self.corporate_actions_applied.contains(id) {
            warn!(
                exchange = %self.exchange,
                %id,
                %instrument,
                "SimulatedVenue already applied a corporate action under this id, so it changed \
                 nothing. Give each action, including a correction, an id of its own"
            );
            return Some(CorporateActionResponse::AlreadyApplied);
        }
        if !traded.kind.is_split_eligible() {
            return refuse(CorporateActionRefusal::InstrumentKindNotSupported);
        }
        let CorporateActionKind::StockSplit { .. } = kind else {
            return refuse(CorporateActionRefusal::KindNotSupported);
        };
        None
    }

    /// Refuses an action for `reason`, with a warning saying why.
    fn refuse_corporate_action(
        &self,
        id: &str,
        instrument: &InstrumentNameExchange,
        kind: &CorporateActionKind,
        reason: CorporateActionRefusal,
    ) -> CorporateActionResponse {
        let why = match reason {
            CorporateActionRefusal::UnknownInstrument => "the venue does not trade the instrument",
            CorporateActionRefusal::InstrumentKindNotSupported => {
                "the instrument is not the deliverable equity"
            }
            CorporateActionRefusal::KindNotSupported => {
                "the venue does not model this kind of action"
            }
            CorporateActionRefusal::HeldByAnotherInstrument => {
                "an order on another instrument holds the equity"
            }
            CorporateActionRefusal::FractionalShortUnderFloor => {
                "under Floor, the split leaves a short or a loan of the equity fractional"
            }
            CorporateActionRefusal::NoPriceForCashInLieu => {
                "under Floor, the split leaves cash in lieu to pay and the venue holds no market \
                 to price it"
            }
            CorporateActionRefusal::Overflow => "a restated holding or price overflows",
            CorporateActionRefusal::OptionExposureAtNonStandardSplit => {
                "a non-standard split would change what the account's options on the equity \
                 deliver"
            }
        };
        warn!(
            exchange = %self.exchange,
            %id,
            %instrument,
            ?kind,
            ?reason,
            "SimulatedVenue refused a corporate action: {why}. Nothing changed and the id is not \
             recorded"
        );
        CorporateActionResponse::Refused(reason)
    }

    /// Everything a split of `ratio` on `instrument` changes, worked out against the venue as it
    /// stands, changing nothing: the first of the split's two phases, which is the only one that
    /// can refuse. [`commit_split`](Self::commit_split) is the second.
    ///
    /// `instrument` is one the venue trades, as checked before.
    fn plan_split(
        &self,
        instrument: &InstrumentNameExchange,
        ratio: SplitRatio,
        policy: SplitRoundingPolicy,
        option_split: OptionSplit,
    ) -> Result<SplitPlan, CorporateActionRefusal> {
        let ratio = ratio.get();
        let Some(traded) = self.instruments.get(instrument) else {
            unreachable!("corporate_action_refusal checked the venue trades {instrument}");
        };
        let base = &traded.underlying.base;
        let orders = self.account.orders();

        let options = self.options_on(instrument);
        if option_split == OptionSplit::RefuseIfHeld
            && options
                .iter()
                .any(|option| self.has_option_exposure(option))
        {
            return Err(CorporateActionRefusal::OptionExposureAtNonStandardSplit);
        }
        // Past that check, an order on an option of the equity remains only on a standard split,
        // which cancels it with the equity's own: neither counts as held elsewhere.
        let split_by_this = |order_instrument: &InstrumentNameExchange| {
            order_instrument == instrument || options.contains(&order_instrument)
        };

        let held_elsewhere = orders
            .iter()
            .filter(|order| !split_by_this(&order.key.instrument))
            .filter_map(|order| orders.resting_order(&order.key.cid)?.reservation.as_ref())
            .any(|reservation| reservation.asset == *base);
        if held_elsewhere {
            return Err(CorporateActionRefusal::HeldByAnotherInstrument);
        }

        let adjust = match self.split.action(ratio) {
            SplitAction::Cancel => Vec::new(),
            SplitAction::Adjust => {
                let tick = traded
                    .spec
                    .as_ref()
                    .map_or(Decimal::ZERO, |spec| spec.price.tick_size);
                let step = traded.quantity_step().unwrap_or(Decimal::ZERO);
                // Best first, so committing them in turn keeps their order among themselves.
                orders
                    .resting(instrument, Side::Buy)
                    .filter_map(|order| split_adjustment(order, ratio, policy, tick, step))
                    .collect()
            }
        };
        let adjusted = adjust
            .iter()
            .map(|adjustment| &adjustment.cid)
            .collect::<FnvHashSet<_>>();
        let mut cancel = orders
            .iter()
            .filter(|order| split_by_this(&order.key.instrument))
            .map(|order| &order.key.cid)
            .filter(|cid| !adjusted.contains(cid))
            .cloned()
            .collect::<Vec<_>>();
        cancel.sort_unstable();

        let holding = match self.account.balance_after_releases(
            base,
            cancel
                .iter()
                .filter_map(|cid| orders.resting_order(cid)?.reservation.as_ref()),
            self.repays_at_cancel(),
        ) {
            None => None,
            Some(after_cancels) => {
                let (restated, remainder) = restate_holding(after_cancels, ratio, policy)?;
                Some(SplitHolding {
                    after_cancels,
                    restated,
                    remainder,
                })
            }
        };

        let market = self.market(instrument);
        let cash_in_lieu = match holding.as_ref().map(|holding| holding.remainder) {
            Some(remainder) if remainder > Decimal::ZERO => {
                let price = market
                    .and_then(|market| mid_or_last(&market.snapshot))
                    .ok_or(CorporateActionRefusal::NoPriceForCashInLieu)?;
                let amount = price
                    .checked_div(ratio)
                    .and_then(|price| price.checked_mul(remainder))
                    .ok_or(CorporateActionRefusal::Overflow)?;
                Some(Delivery {
                    asset: traded.underlying.quote.clone(),
                    amount,
                })
            }
            _ => None,
        };

        let market = market
            .map(|market| restate_market(market, ratio).ok_or(CorporateActionRefusal::Overflow))
            .transpose()?;
        let borrow_prices = self
            .borrow_prices
            .get(instrument)
            .map(|window| {
                window
                    .rescaled(ratio)
                    .ok_or(CorporateActionRefusal::Overflow)
            })
            .transpose()?;

        let options = match option_split {
            OptionSplit::Restate => options
                .into_iter()
                .map(|option| self.restate_option(option, ratio))
                .collect::<Result<_, _>>()?,
            // Nothing on them is held, as checked above.
            OptionSplit::RefuseIfHeld => Vec::new(),
        };

        Ok(SplitPlan {
            cancel,
            adjust,
            holding,
            cash_in_lieu,
            market,
            borrow_prices,
            options,
        })
    }

    /// The options this venue lists on the equity `instrument`: those whose underlying is its
    /// pair. Sorted by name, so the plan built from them does not depend on the map's order.
    fn options_on(&self, instrument: &InstrumentNameExchange) -> Vec<&InstrumentNameExchange> {
        let underlying = &self.instruments[instrument].underlying;
        let mut options = self
            .instruments
            .iter()
            .filter(|(_, listed)| {
                matches!(listed.kind, InstrumentKind::Option(_)) && listed.underlying == *underlying
            })
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        options.sort_unstable();
        options
    }

    /// The option `option`'s terms, position and market after a standard split of `ratio`: the
    /// strike divided by it, the contracts multiplied by it and their entry price divided by it,
    /// and the market restated as the equity's is.
    fn restate_option(
        &self,
        option: &InstrumentNameExchange,
        ratio: Decimal,
    ) -> Result<OptionRestatement, CorporateActionRefusal> {
        let InstrumentKind::Option(contract) = &self.instruments[option].kind else {
            unreachable!("options_on lists only options, and {option} is listed");
        };
        let strike = contract
            .strike
            .checked_div(ratio)
            .ok_or(CorporateActionRefusal::Overflow)?;
        let position = self
            .positions
            .get(option)
            .map(|position| {
                Ok(NetPosition {
                    quantity: position
                        .quantity
                        .checked_mul(ratio)
                        .ok_or(CorporateActionRefusal::Overflow)?,
                    entry: position
                        .entry
                        .checked_div(ratio)
                        .ok_or(CorporateActionRefusal::Overflow)?,
                })
            })
            .transpose()?;
        let market = self
            .market(option)
            .map(|market| restate_market(market, ratio).ok_or(CorporateActionRefusal::Overflow))
            .transpose()?;
        Ok(OptionRestatement {
            option: option.clone(),
            strike,
            position,
            market,
        })
    }

    /// Makes every change `plan` holds to `instrument`, pushing what each reports onto `events`:
    /// the second of the split's two phases, which cannot refuse the action. Its one outcome the
    /// plan does not hold is an adjusted order cancelled because its hold cannot be recommitted:
    /// see [`adjust_for_split`](Self::adjust_for_split). See
    /// [`apply_corporate_action`](Self::apply_corporate_action) for the order it reports in.
    fn commit_split(
        &mut self,
        instrument: &InstrumentNameExchange,
        plan: SplitPlan,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let SplitPlan {
            cancel,
            adjust,
            holding,
            cash_in_lieu,
            market,
            borrow_prices,
            options,
        } = plan;

        self.cancel_each(cancel, time_exchange, events);

        if let Some(SplitHolding {
            after_cancels,
            restated,
            ..
        }) = holding
        {
            let base = &self.instruments[instrument].underlying.base;
            let Some(balance) = self.account.balance_mut(base) else {
                unreachable!("plan_split found a balance of {base}, and nothing removes one");
            };
            debug_assert_eq!(
                balance.balance, after_cancels,
                "SimulatedVenue's cancels left {base} other than plan_split worked out"
            );
            balance.balance = restated;
            balance.time_exchange = time_exchange;
            let restated = balance.clone();
            events.push(self.build_account_event(Snapshot(restated)));
        }

        if let Some(cash_in_lieu) = cash_in_lieu {
            // Paid into the free balance, repaying no loan: the venue pays it, no order receives
            // it.
            let paid =
                self.account
                    .deliver(&cash_in_lieu, MarginEffect::NoSideEffect, time_exchange);
            events.push(self.build_account_event(Snapshot(paid)));
        }

        if let Some(market) = market {
            self.market.insert(
                instrument.clone(),
                VenueInstrumentMarket {
                    split_at: Some(time_exchange),
                    ..market
                },
            );
        }
        if let Some(window) = borrow_prices {
            self.borrow_prices.insert(instrument.clone(), window);
        }

        // A restated position is still open, so no holding-cost boundary moves.
        for option in options {
            self.commit_option_restatement(option, time_exchange);
        }

        for adjustment in adjust {
            self.adjust_for_split(adjustment, time_exchange, events);
        }

        // Cancelling an order can repay the loan it took, and the split restates a loan of the
        // equity.
        self.refresh_next_interest_if_loans_changed();
        self.report_debt_changes(events);
    }

    /// Gives an option on a split equity the strike, position and market `restatement` holds. It
    /// reports nothing: see [`apply_corporate_action`](Self::apply_corporate_action).
    fn commit_option_restatement(
        &mut self,
        restatement: OptionRestatement,
        time_exchange: DateTime<Utc>,
    ) {
        let OptionRestatement {
            option,
            strike,
            position,
            market,
        } = restatement;
        let Some(InstrumentKind::Option(contract)) = self
            .instruments
            .get_mut(&option)
            .map(|listed| &mut listed.kind)
        else {
            unreachable!("plan_split found {option} listed as an option, and the list is fixed");
        };
        contract.strike = strike;
        self.restated_strikes.insert(option.clone(), strike);
        if let Some(position) = position {
            self.positions.insert(option.clone(), position);
        }
        if let Some(market) = market {
            self.market.insert(
                option,
                VenueInstrumentMarket {
                    split_at: Some(time_exchange),
                    ..market
                },
            );
        }
    }

    /// Adjusts the buy limit order `adjustment` names to its terms after the split, recommitting
    /// its hold as an amend does, or cancels it if the account cannot afford that hold. Pushes
    /// what it reports onto `events`.
    fn adjust_for_split(
        &mut self,
        adjustment: SplitAdjustment,
        time_exchange: DateTime<Utc>,
        events: &mut Vec<UnindexedAccountEvent>,
    ) {
        let SplitAdjustment {
            cid,
            price,
            quantity,
            filled,
        } = adjustment;
        let Some(RestingOrder {
            order, reservation, ..
        }) = self.account.orders().resting_order(&cid).cloned()
        else {
            unreachable!("plan_split found {cid} resting, and only the split has moved since");
        };
        let adjusted = Order {
            price: Some(price),
            quantity,
            state: Open::new(order.state.id.clone(), time_exchange, filled),
            ..order
        };

        let reservation = match reservation {
            // Seeded through `initial_state`: the venue holds nothing against it to recommit.
            None => None,
            Some(reservation) => {
                let reentry = match self.instrument_terms(&adjusted.key.instrument) {
                    // No market crosses an order on an instrument just split: see `split_at`.
                    Ok(terms) => self.reenter(
                        &adjusted,
                        Some(reservation),
                        price,
                        Release::Market(&MarketSnapshot::default()),
                        &terms,
                        time_exchange,
                    ),
                    Err(error) => {
                        unreachable!("SimulatedVenue trades the instrument of {cid}: {error:?}")
                    }
                };
                match reentry {
                    Reentry::Entered {
                        committed,
                        trade,
                        remainder,
                    } => {
                        debug_assert!(trade.is_none(), "an empty market crossed {cid}");
                        self.push_committed(events, committed, trade);
                        let Some(remainder) = remainder else {
                            unreachable!("{cid} traded nothing, so it has something left to rest");
                        };
                        Some(remainder)
                    }
                    Reentry::Refused { cause, .. } => {
                        warn!(
                            exchange = %self.exchange,
                            %cid,
                            ?cause,
                            "SimulatedVenue cancelled an order a split would have adjusted: it \
                             cannot recommit the order's hold at the adjusted terms"
                        );
                        // The refusal left the order holding what it held, so it is cancelled as
                        // the plan's cancels were.
                        self.cancel_each(vec![cid], time_exchange, events);
                        return;
                    }
                }
            }
        };

        // Re-queued at the split, behind those adjusted before it: see `apply_corporate_action`.
        if let Err(NotQueued(_)) =
            self.account
                .amend_order(adjusted.clone(), reservation, Rank::Lost)
        {
            unreachable!("plan_split found {cid} queued, and only the split has moved since");
        }
        events.push(self.build_account_event(Snapshot(booked_report(
            adjusted.map_state(OrderState::active),
        ))));
    }
}

/// Everything a split changes, worked out by [`SimulatedVenue::plan_split`] and made by
/// [`SimulatedVenue::commit_split`].
#[derive(Debug)]
struct SplitPlan {
    /// The orders cancelled, in that order: by client order id.
    cancel: Vec<ClientOrderId>,
    /// The buy limit orders adjusted, best first.
    adjust: Vec<SplitAdjustment>,
    /// The balance of the equity, or `None` if the account has never held it.
    holding: Option<SplitHolding>,
    /// The cash in lieu paid for a fraction of a share, if any.
    cash_in_lieu: Option<Delivery>,
    /// The market held for the equity, restated, if the venue holds one.
    market: Option<VenueInstrumentMarket>,
    /// The equity's borrow price window, restated, if the venue keeps one.
    borrow_prices: Option<super::PriceWindow>,
    /// The options on the equity, restated: every one the venue lists on a standard split, by
    /// name, and none on any other.
    options: Vec<OptionRestatement>,
}

/// What a split does to the options on its equity, by its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OptionSplit {
    /// A standard split: each option's strike, position and market are restated.
    Restate,
    /// Any other split: refused while the account holds a position in, or has an order on, an
    /// option on the equity, and otherwise touching no option.
    RefuseIfHeld,
}

/// An option on a split equity, after a standard split.
#[derive(Debug)]
struct OptionRestatement {
    option: InstrumentNameExchange,
    strike: Decimal,
    /// The account's position in it, or `None` if it holds none.
    position: Option<NetPosition>,
    /// The market held for it, or `None` if the venue holds none.
    market: Option<VenueInstrumentMarket>,
}

/// A buy limit order's terms after a split.
#[derive(Debug)]
struct SplitAdjustment {
    cid: ClientOrderId,
    price: Decimal,
    quantity: Decimal,
    filled: Decimal,
}

/// The balance of a split equity.
#[derive(Debug)]
struct SplitHolding {
    /// What the split's cancels leave it, before the split restates it.
    after_cancels: Balance,
    /// What the split restates it to.
    restated: Balance,
    /// The fraction of a share taken off it under `Floor`, in post-split shares, paid as cash in
    /// lieu.
    remainder: Decimal,
}

/// The terms `order`, a resting buy limit order, takes on a split of `ratio`, as
/// [`SplitAction::Adjust`] describes, or `None` if it is cancelled instead: a price or an unfilled
/// quantity that rounds to zero, or a value that overflows. A step or tick of zero rounds nothing.
fn split_adjustment(
    order: &OpenOrder,
    ratio: Decimal,
    policy: SplitRoundingPolicy,
    tick: Decimal,
    step: Decimal,
) -> Option<SplitAdjustment> {
    if order.kind != OrderKind::Limit {
        return None;
    }
    let price = round_toward_zero_to_step(order.price?.checked_div(ratio)?, tick);
    let filled = order.state.filled_quantity.checked_mul(ratio)?;
    let unfilled = order
        .state
        .quantity_remaining(order.quantity)
        .checked_mul(ratio)?;
    let unfilled = round_toward_zero_to_step(policy.round_shares(unfilled), step);
    if price <= Decimal::ZERO || unfilled <= Decimal::ZERO {
        return None;
    }
    Some(SplitAdjustment {
        cid: order.key.cid.clone(),
        price,
        quantity: filled.checked_add(unfilled)?,
        filled,
    })
}

/// `balance`, of a split equity, restated for a split of `ratio` under `policy`, with the fraction
/// of a share `Floor` takes off it.
fn restate_holding(
    balance: Balance,
    ratio: Decimal,
    policy: SplitRoundingPolicy,
) -> Result<(Balance, Decimal), CorporateActionRefusal> {
    let scale = |value: Decimal| {
        value
            .checked_mul(ratio)
            .ok_or(CorporateActionRefusal::Overflow)
    };
    let margin = balance
        .margin
        .map(|margin| {
            Ok::<_, CorporateActionRefusal>(MarginDetails {
                borrowed: scale(margin.borrowed)?,
                interest: scale(margin.interest)?,
            })
        })
        .transpose()?;
    let total = scale(balance.total)?;
    let free = scale(balance.free)?;
    // A carry accrued in the equity is counted in shares too.
    let accrued = balance.accrued.map(scale).transpose()?;
    // Rounding a loan or a holding below zero would need a rule for who pays or is paid for the
    // fraction, which the venue does not model yet.
    let borrowed = margin.map_or(Decimal::ZERO, |margin| margin.borrowed);
    let rounds = |shares: Decimal| policy.round_shares(shares) != shares;
    if rounds(borrowed) || (total < Decimal::ZERO && rounds(total)) {
        return Err(CorporateActionRefusal::FractionalShortUnderFloor);
    }
    let remainder = total - policy.round_shares(total);
    Ok((
        Balance {
            total: total - remainder,
            free: free - remainder,
            margin,
            accrued,
        },
        remainder,
    ))
}

/// `market` restated for a split of `ratio`: prices divided by it, sizes multiplied by it. `None`
/// if a value overflows.
fn restate_market(market: &VenueInstrumentMarket, ratio: Decimal) -> Option<VenueInstrumentMarket> {
    // `Some(None)` for a value the market does not have, `None` on overflow.
    let price = |price: Option<Decimal>| match price {
        Some(price) => price.checked_div(ratio).map(Some),
        None => Some(None),
    };
    let size = |size: Option<Decimal>| match size {
        Some(size) => size.checked_mul(ratio).map(Some),
        None => Some(None),
    };
    let bar = match market.bar {
        Some(bar) => Some(MarketBar {
            open: bar.open.checked_div(ratio)?,
            high: bar.high.checked_div(ratio)?,
            low: bar.low.checked_div(ratio)?,
            close: bar.close.checked_div(ratio)?,
        }),
        None => None,
    };
    Some(VenueInstrumentMarket {
        snapshot: MarketSnapshot {
            best_bid: price(market.snapshot.best_bid)?,
            best_ask: price(market.snapshot.best_ask)?,
            last_price: price(market.snapshot.last_price)?,
        },
        depth: MarketDepth {
            best_bid: size(market.depth.best_bid)?,
            best_ask: size(market.depth.best_ask)?,
        },
        bar,
        ..*market
    })
}
