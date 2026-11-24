use crate::{
    AccountEventKind, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    borrow_capacity::BorrowCapacity,
    client::{BorrowCapacityClient, ExecutionClient, OrderStatusClient},
    error::{
        ClientError, ConnectivityError, NotPlacedReason, OrderError, StreamTerminationReason,
        UnindexedClientError, UnindexedOrderError,
    },
    exchange::mock::{
        CorporateActionResponse, request::MockExchangeRequest, venue::is_requested_instrument,
    },
    expiry::{ExpiryDayCutoff, ExpirySellOut, LongOptionExpiry},
    fee::{FeeAssetConfig, FeeModelConfig, FeeModelOverrides, SpotFeeAsset},
    fill::{MarketOrderTiming, SimFillConfig, StaleMarketPolicy},
    margin::SpotMarginConfig,
    order::{
        Order, OrderKey, TimeInForce, TimeInForceKind, UnindexedInactiveOrder, UnindexedOrderKey,
        amend::{AmendAllowed, AmendPriority, AmendSupport},
        request::{
            OrderRequestAmend, OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseAmend,
            UnindexedOrderResponseCancel,
        },
        state::{Open, OrderState, UnindexedOrderState},
    },
    rules::OrderRules,
    short_option::ShortPutCollateral,
    slippage::SlippageConfig,
    trade::TradesRead,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use futures::{StreamExt, stream::BoxStream};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    calendar::{DailyClose, SessionCalendar},
    corporate_action::{CorporateActionKind, SplitRoundingPolicy},
    exchange::ExchangeId,
    instrument::{
        kind::InstrumentKindDiscriminant, name::InstrumentNameExchange,
        overrides::InstrumentOverrides,
    },
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

/// How a simulated venue behaves: which exchange it stands in for, the account it starts with, how
/// long a request takes to reach it, and the rules it fills, charges and refuses by.
///
/// Build one with [`new`](Self::new), which takes the three settings that have no neutral value,
/// then change any other setting with its `with_*` method. Every other setting defaults to the
/// simplest venue: no fees, fills at the last price, no slippage, a cash account, and every time in
/// force it can model accepted. Each field's documentation names its default. Deserialising
/// applies the same defaults to the fields a stored config leaves out, and requires the three that
/// [`new`](Self::new) takes.
///
/// `#[non_exhaustive]`, so a setting can be added without breaking a caller. Outside this crate,
/// construct it with [`new`](Self::new) rather than a struct literal. The fields stay public, so a
/// built config's settings can still be read and assigned in place.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct MockExecutionConfig {
    /// The exchange the simulated venue stands in for: the [`ExchangeId`] on every event and
    /// snapshot it reports.
    pub mocked_exchange: ExchangeId,
    /// The account the venue starts with: its balances, positions and resting orders.
    pub initial_state: UnindexedAccountSnapshot,
    /// The round trip, in milliseconds, of a request to the venue and its response back.
    ///
    /// It is applied by the driver that carries requests to the venue, not by the venue itself;
    /// each driver documents how it splits the round trip (see
    /// [`MockExchange`](crate::exchange::mock::MockExchange)). At `0` the venue books a request at
    /// the instant it is sent, which no live venue does: a strategy can learn to trade on prices it
    /// could not have reached in time. Set it to the latency measured against the live venue. A
    /// driver that splits it into two equal halves rounds each down to a whole millisecond, so an
    /// odd value shortens that driver's round trip by one millisecond.
    pub latency_ms: u64,
    /// Fee model used by the mock exchange to compute trading fees, for every instrument no entry
    /// of [`fee_model_overrides`](Self::fee_model_overrides) matches.
    ///
    /// Defaults to [`FeeModelConfig::Zero`]. Use [`FeeModelConfig::Percentage`]
    /// for spot/futures simulation (e.g. 0.1% taker fee), and [`FeeModelConfig::Composite`] for a
    /// schedule of several fees, such as a broker's regulatory pass-through fees. Wrap any of them
    /// in [`FeeModelConfig::Dated`] for fees that change over a backtest's span.
    #[serde(default)]
    pub fee_model: FeeModelConfig,
    /// Fill model used by the mock exchange to compute execution prices.
    ///
    /// Defaults to [`SimFillConfig::LastPrice`], which fills at the snapshot's `last_price`,
    /// falling back to the side of the book a taker would cross to. Switch to
    /// [`SimFillConfig::BidAsk`] or [`SimFillConfig::Midpoint`] for more realistic spread-cost
    /// simulation when market prices are injected alongside orders.
    #[serde(default)]
    pub fill_model: SimFillConfig,
    /// Which asset the mock exchange charges a spot fill's fee in.
    ///
    /// Defaults to [`SpotFeeAsset::Quote`], on both sides. [`SpotFeeAsset::Received`] takes a
    /// buy's fee out of the base it receives, as Binance spot does by default.
    #[serde(default)]
    pub spot_fee_asset: SpotFeeAsset,
    /// How far the mock exchange moves each taker fill against the order, after the
    /// [`fill_model`](Self::fill_model) has priced it.
    ///
    /// Defaults to [`SlippageConfig::none`]. A seeded [`SlippageConfig::uniform`] draws a fresh
    /// rate for each fill, reproducibly from its seed. See [`SlippageConfig`] for what it applies
    /// to and how.
    #[serde(default)]
    pub slippage: SlippageConfig,
    /// When the mock exchange fills a market order.
    ///
    /// Defaults to [`MarketOrderTiming::OnArrival`]: against the market the venue holds when the
    /// order arrives, which on bar data is the close of the bar the sender has just seen.
    /// [`MarketOrderTiming::NextBar`] holds it for the instrument's next bar instead, and needs a
    /// driver that feeds the venue bars, as `SimRunner` does on a candle feed.
    #[serde(default)]
    pub market_order_timing: MarketOrderTiming,
    /// What the mock exchange does with an order when the instrument's market is stale.
    ///
    /// Defaults to [`StaleMarketPolicy::Off`]: it fills against the market it holds, however old.
    /// [`StaleMarketPolicy::Wait`] makes an order on an instrument with no fresh observation within
    /// an age wait for the next one, and needs a driver that feeds the venue a market, as
    /// `SimRunner` does.
    #[serde(default)]
    pub stale_market: StaleMarketPolicy,
    /// Whether the mock exchange lends a spot order what the account lacks, as a margin account
    /// does.
    ///
    /// Defaults to `None`: a cash account, whose spot orders are paid for with what it holds, and
    /// whose balances report no loans. A [`SpotMarginConfig`] makes it a cross margin account that
    /// borrows under each order's [`MarginEffect`](crate::margin::MarginEffect), up to a
    /// leverage limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spot_margin: Option<SpotMarginConfig>,
    /// A third asset the mock exchange charges spot fees in, at a discount, while the account
    /// holds enough of it, as Binance's "pay fees with BNB" does.
    ///
    /// Defaults to `None`: every fee is charged under [`spot_fee_asset`](Self::spot_fee_asset).
    /// See [`FeeAssetConfig`] for what a fill pays and when it falls back to that rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_asset: Option<FeeAssetConfig>,
    /// How the venue settles a long option it holds at expiry: exercised when in the money, the
    /// default, or lapsed. See [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue)'s
    /// `# How an option is accounted`.
    ///
    /// A request-priced venue ([`SimulatedVenue::new`](crate::exchange::mock::SimulatedVenue::new),
    /// and so [`MockExchange::new`](crate::exchange::mock::MockExchange::new)) has no market feed
    /// to price an underlying at expiry, so it takes an option buy only under
    /// [`LongOptionExpiry::Lapse`].
    ///
    /// A long the account cannot afford to exercise can be sold before expiry instead: see
    /// [`expiry_sell_out`](Self::expiry_sell_out).
    #[serde(default)]
    pub long_option_expiry: LongOptionExpiry,
    /// How much quote a short put holds on the venue: the full strike, the default, or the
    /// strike less the premium the sale receives. See [`ShortPutCollateral`].
    #[serde(default)]
    pub short_put_collateral: ShortPutCollateral,
    /// What the mock exchange amends in place, and whether an amended order keeps its venue
    /// order id. See [`AmendConfig`].
    ///
    /// Defaults to [`AmendConfig::FULL`]: a resting order's price and quantity can be amended,
    /// together or alone, and the order keeps its id.
    #[serde(default)]
    pub amend: AmendConfig,
    /// Which [`TimeInForce`] the mock exchange takes for each kind of instrument, on top of the
    /// ones it can model at all. See [`TimeInForceRules`].
    ///
    /// Defaults to no rules: every time in force the venue can model is taken on every kind.
    /// Orders seeded through `initial_state` are not checked: they stand for orders the live
    /// venue has already taken.
    #[serde(default, skip_serializing_if = "TimeInForceRules::is_empty")]
    pub time_in_force: TimeInForceRules,
    /// When the mock exchange's trading sessions close, which is when a
    /// [`TimeInForce::GoodUntilEndOfDay`] limit order expires. See [`SessionCalendarConfig`].
    ///
    /// Defaults to `None`: the venue has no session calendar and refuses a day limit order with
    /// [`OrderError::UnsupportedTimeInForce`]. A calendar of your own can be attached instead with
    /// [`SimulatedVenue::with_session_calendar`].
    ///
    /// [`SimulatedVenue::with_session_calendar`]:
    ///     crate::exchange::mock::SimulatedVenue::with_session_calendar
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_close: Option<SessionCalendarConfig>,
    /// Session closes for some instruments in place of [`session_close`](Self::session_close),
    /// by name, by kind and underlying, or by kind: a venue whose index options close at 16:15
    /// while everything else closes at 16:00. See [`SessionCloseOverrides`] for which entry an
    /// instrument takes.
    ///
    /// An instrument an entry matches takes a day limit order even when `session_close` is
    /// `None`, and keeps its entry's close when a calendar is attached with
    /// [`SimulatedVenue::with_session_calendar`], which replaces only `session_close`. When it
    /// builds, the venue logs a warning for each name, and each kind and underlying, that none of
    /// its instruments has, as it does for [`fee_model_overrides`](Self::fee_model_overrides).
    ///
    /// Defaults to empty: every instrument closes under `session_close`.
    ///
    /// [`SimulatedVenue::with_session_calendar`]:
    ///     crate::exchange::mock::SimulatedVenue::with_session_calendar
    #[serde(default, skip_serializing_if = "SessionCloseOverrides::is_empty")]
    pub session_close_overrides: SessionCloseOverrides,
    /// Fee models for some instruments in place of [`fee_model`](Self::fee_model), by name, by
    /// kind and underlying, or by kind: a venue whose options and shares pay different fees, or
    /// whose index options pay more than its equity options. See [`FeeModelOverrides`] for which
    /// entry an instrument pays.
    ///
    /// When it builds, the venue logs a warning for each name, and each kind and underlying, that
    /// none of its instruments has: likely a typo, which would otherwise charge that instrument the
    /// default. An entry by kind is not checked, since one config often serves venues of different
    /// instruments.
    ///
    /// Defaults to empty: every instrument pays [`fee_model`](Self::fee_model).
    #[serde(default, skip_serializing_if = "FeeModelOverrides::is_empty")]
    pub fee_model_overrides: FeeModelOverrides,
    /// Whether the mock exchange sells a long option the account cannot afford to exercise, at a
    /// set time before its expiry day's close or before its expiry, as a broker sells out an
    /// account short of the cash or shares. See [`ExpirySellOut`]. One anchored
    /// [before the close](crate::expiry::ExpiryAnchor::BeforeClose) needs a session calendar for
    /// each option: [`session_close`](Self::session_close), an entry of
    /// [`session_close_overrides`](Self::session_close_overrides), or one the venue is given
    /// later.
    ///
    /// Defaults to `None`: every long option is held to expiry and settles under
    /// [`long_option_expiry`](Self::long_option_expiry), an exercise the account cannot cover
    /// taking its free balance below zero, or borrowing on a venue with
    /// [`spot_margin`](Self::spot_margin).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_sell_out: Option<ExpirySellOut>,
    /// Whether the mock exchange refuses an order that would open or increase a position in an
    /// option from a set time before its expiry day's close, or before its expiry, until it
    /// expires, as a broker stops taking new exposure in an expiring option. See
    /// [`ExpiryDayCutoff`]. One anchored
    /// [before the close](crate::expiry::ExpiryAnchor::BeforeClose) needs a session calendar for
    /// each option, as [`expiry_sell_out`](Self::expiry_sell_out) does.
    ///
    /// Defaults to `None`: an option takes opening orders until its expiry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_day_cutoff: Option<ExpiryDayCutoff>,
    /// What a stock split does to the mock exchange's open orders on the split equity: cancelled,
    /// or adjusted. See [`SplitConfig`].
    ///
    /// Defaults to [`SplitConfig::CANCEL`]: every order on the equity is cancelled.
    #[serde(default)]
    pub split: SplitConfig,
    /// The order rules of some instruments, by name: the tick a price must sit on, the step and
    /// range of a quantity, and the range of a notional, as the live venue publishes them. See
    /// [`OrderRules`].
    ///
    /// The venue refuses an order on an instrument with rules that breaks one, unsent to its
    /// book, with the error a live client that loads the same rules refuses it with:
    /// [`OrderError::InvalidPrecision`] for a value off its grid, [`OrderError::OutOfBounds`] for
    /// one outside its range. It never rounds. Each order is checked by [`OrderRules::check`] when
    /// it arrives, and a market order also by [`OrderRules::check_market_notional`], at the price
    /// the venue's fill model gives it before slippage, times the quantity asked for. An amend is
    /// checked in the same way, on the whole order as amended.
    ///
    /// Defaults to empty: an instrument with no entry is not checked. Rules are never derived
    /// from an instrument's [`InstrumentSpec`](rustrade_instrument::instrument::spec::InstrumentSpec),
    /// which holds less than they do.
    ///
    /// Building the venue refuses an entry for an instrument it does not trade, which would
    /// otherwise check nothing unnoticed, and a rule whose minimum is above its maximum. See
    /// [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue)'s `# Order rules` for what the
    /// venue cannot match.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub order_rules: BTreeMap<InstrumentNameExchange, OrderRules>,
}

impl MockExecutionConfig {
    /// A venue standing in for `mocked_exchange`, starting with the account `initial_state`, with
    /// a round trip of `latency_ms` milliseconds (see [`latency_ms`](Self::latency_ms)).
    ///
    /// Every other setting takes its default, named in its field's documentation. Change one with
    /// its `with_*` method.
    pub fn new(
        mocked_exchange: ExchangeId,
        initial_state: UnindexedAccountSnapshot,
        latency_ms: u64,
    ) -> Self {
        Self {
            mocked_exchange,
            initial_state,
            latency_ms,
            fee_model: FeeModelConfig::default(),
            fill_model: SimFillConfig::default(),
            spot_fee_asset: SpotFeeAsset::default(),
            slippage: SlippageConfig::default(),
            market_order_timing: MarketOrderTiming::default(),
            stale_market: StaleMarketPolicy::default(),
            spot_margin: None,
            fee_asset: None,
            long_option_expiry: LongOptionExpiry::default(),
            short_put_collateral: ShortPutCollateral::default(),
            amend: AmendConfig::default(),
            time_in_force: TimeInForceRules::default(),
            session_close: None,
            session_close_overrides: SessionCloseOverrides::default(),
            fee_model_overrides: FeeModelOverrides::default(),
            expiry_sell_out: None,
            expiry_day_cutoff: None,
            split: SplitConfig::default(),
            order_rules: BTreeMap::new(),
        }
    }

    /// This config with its [`fee_model`](Self::fee_model) set to `fee_model`.
    #[must_use]
    pub fn with_fee_model(self, fee_model: FeeModelConfig) -> Self {
        Self { fee_model, ..self }
    }

    /// This config with its [`fill_model`](Self::fill_model) set to `fill_model`.
    #[must_use]
    pub fn with_fill_model(self, fill_model: SimFillConfig) -> Self {
        Self { fill_model, ..self }
    }

    /// This config with its [`spot_fee_asset`](Self::spot_fee_asset) set to `spot_fee_asset`.
    #[must_use]
    pub fn with_spot_fee_asset(self, spot_fee_asset: SpotFeeAsset) -> Self {
        Self {
            spot_fee_asset,
            ..self
        }
    }

    /// This config with its [`slippage`](Self::slippage) set to `slippage`.
    #[must_use]
    pub fn with_slippage(self, slippage: SlippageConfig) -> Self {
        Self { slippage, ..self }
    }

    /// This config with its [`market_order_timing`](Self::market_order_timing) set to
    /// `market_order_timing`.
    #[must_use]
    pub fn with_market_order_timing(self, market_order_timing: MarketOrderTiming) -> Self {
        Self {
            market_order_timing,
            ..self
        }
    }

    /// This config with its [`stale_market`](Self::stale_market) policy set to `stale_market`.
    #[must_use]
    pub fn with_stale_market(self, stale_market: StaleMarketPolicy) -> Self {
        Self {
            stale_market,
            ..self
        }
    }

    /// This config as a cross margin account that lends spot orders what the account lacks, under
    /// `spot_margin`: see [`spot_margin`](Self::spot_margin).
    #[must_use]
    pub fn with_spot_margin(self, spot_margin: SpotMarginConfig) -> Self {
        Self {
            spot_margin: Some(spot_margin),
            ..self
        }
    }

    /// This config charging spot fees in a third asset, under `fee_asset`: see
    /// [`fee_asset`](Self::fee_asset).
    #[must_use]
    pub fn with_fee_asset(self, fee_asset: FeeAssetConfig) -> Self {
        Self {
            fee_asset: Some(fee_asset),
            ..self
        }
    }

    /// This config with its [`long_option_expiry`](Self::long_option_expiry) set to
    /// `long_option_expiry`.
    #[must_use]
    pub fn with_long_option_expiry(self, long_option_expiry: LongOptionExpiry) -> Self {
        Self {
            long_option_expiry,
            ..self
        }
    }

    /// This config with its [`short_put_collateral`](Self::short_put_collateral) set to
    /// `short_put_collateral`.
    #[must_use]
    pub fn with_short_put_collateral(self, short_put_collateral: ShortPutCollateral) -> Self {
        Self {
            short_put_collateral,
            ..self
        }
    }

    /// This config with its [`amend`](Self::amend) support set to `amend`.
    #[must_use]
    pub fn with_amend(self, amend: AmendConfig) -> Self {
        Self { amend, ..self }
    }

    /// This config with its [`time_in_force`](Self::time_in_force) rules set to `time_in_force`.
    #[must_use]
    pub fn with_time_in_force(self, time_in_force: TimeInForceRules) -> Self {
        Self {
            time_in_force,
            ..self
        }
    }

    /// This config with trading sessions that close under `session_close`: see
    /// [`session_close`](Self::session_close).
    #[must_use]
    pub fn with_session_close(self, session_close: SessionCalendarConfig) -> Self {
        Self {
            session_close: Some(session_close),
            ..self
        }
    }

    /// This config with its [`session_close_overrides`](Self::session_close_overrides) set to
    /// `session_close_overrides`.
    #[must_use]
    pub fn with_session_close_overrides(
        self,
        session_close_overrides: SessionCloseOverrides,
    ) -> Self {
        Self {
            session_close_overrides,
            ..self
        }
    }

    /// This config with its [`fee_model_overrides`](Self::fee_model_overrides) set to
    /// `fee_model_overrides`.
    #[must_use]
    pub fn with_fee_model_overrides(self, fee_model_overrides: FeeModelOverrides) -> Self {
        Self {
            fee_model_overrides,
            ..self
        }
    }

    /// This config selling out a long option the account cannot afford to exercise, under
    /// `expiry_sell_out`: see [`expiry_sell_out`](Self::expiry_sell_out).
    #[must_use]
    pub fn with_expiry_sell_out(self, expiry_sell_out: ExpirySellOut) -> Self {
        Self {
            expiry_sell_out: Some(expiry_sell_out),
            ..self
        }
    }

    /// This config refusing orders that open a position in an option about to expire, under
    /// `expiry_day_cutoff`: see [`expiry_day_cutoff`](Self::expiry_day_cutoff).
    #[must_use]
    pub fn with_expiry_day_cutoff(self, expiry_day_cutoff: ExpiryDayCutoff) -> Self {
        Self {
            expiry_day_cutoff: Some(expiry_day_cutoff),
            ..self
        }
    }

    /// This config with its [`split`](Self::split) rule set to `split`.
    #[must_use]
    pub fn with_split(self, split: SplitConfig) -> Self {
        Self { split, ..self }
    }

    /// This config checking orders on `instrument` against `rules`, replacing any rules it had:
    /// see [`order_rules`](Self::order_rules).
    #[must_use]
    pub fn with_order_rules(
        mut self,
        instrument: InstrumentNameExchange,
        rules: OrderRules,
    ) -> Self {
        self.order_rules.insert(instrument, rules);
        self
    }

    /// This config checking orders on each instrument in `rules` against its rules, replacing any
    /// rules it had: see [`order_rules`](Self::order_rules). Takes a map as a live client's
    /// `load_rules` returns it, or one built by hand.
    #[must_use]
    pub fn with_order_rules_from(
        mut self,
        rules: impl IntoIterator<Item = (InstrumentNameExchange, OrderRules)>,
    ) -> Self {
        self.order_rules.extend(rules);
        self
    }
}

/// When a simulated venue's trading sessions close: what a
/// [`TimeInForce::GoodUntilEndOfDay`] limit order lives until.
///
/// A day limit order the venue accepts expires at the first instant after the next close (see
/// [`SessionCalendar::next_close_after`]), as a [`TimeInForce::GoodTillDate`] order expires at
/// its `expiry`. Something trading at the close itself can still fill it. An order sent after
/// the close, on a weekend or before the open is accepted and expires at the next close, so one
/// sent after Friday's close expires at Monday's.
///
/// # Known limitations
/// - **Holidays and early closes only as listed** in [`Daily`](Self::Daily): a day order lives
///   through a market holiday, and past an early close, that its [`SessionDates`] do not list.
///
/// [`SessionDates`]: rustrade_instrument::calendar::SessionDates
/// - **No open time**: a day order sent after the close works at once, so it can fill on
///   after-hours data that the live venue would hold until the next open.
/// - **One close per instrument, fixed when the venue is built**: a venue whose instruments close
///   at different times (US equity options at 16:00 ET, index options at 16:15 ET) gives each
///   class its own with [`MockExecutionConfig::session_close_overrides`]. Refuse a day order
///   where the live venue refuses one with [`MockExecutionConfig::time_in_force`].
///
/// `#[non_exhaustive]`, so other sources of a close can be added without breaking a caller. Not
/// `Copy`, since a [`DailyClose`] holds its holiday and early-close dates; `Ord` and `Hash` are
/// what [`MockExecutionConfig`] requires.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCalendarConfig {
    /// One close time in a time zone, every weekday: see [`DailyClose`].
    ///
    /// ```
    /// use chrono::NaiveTime;
    /// use rustrade_execution::client::mock::SessionCalendarConfig;
    /// use rustrade_instrument::calendar::{DailyClose, Tz};
    ///
    /// // US equity options close at 16:00 New York time.
    /// let close = SessionCalendarConfig::Daily(DailyClose::new(
    ///     NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
    ///     "America/New_York".parse::<Tz>().unwrap(),
    /// ));
    /// ```
    Daily(DailyClose),
}

/// Session closes for some instruments, each in place of a venue's
/// [`session_close`](MockExecutionConfig::session_close): by the instrument's exact name, by its
/// kind and underlying, or by its kind alone.
///
/// The keys, the order an instrument's close is picked in and the serialised form are
/// [`InstrumentOverrides`]'. An instrument no entry matches closes under the venue's
/// `session_close`.
pub type SessionCloseOverrides = InstrumentOverrides<SessionCalendarConfig>;

impl SessionCalendarConfig {
    /// The calendar this configuration names.
    pub fn calendar(&self) -> Arc<dyn SessionCalendar> {
        match self {
            Self::Daily(close) => Arc::new(close.clone()),
        }
    }
}

/// Which [`TimeInForce`] a simulated venue takes for each kind of instrument, so it refuses what
/// the live venue it stands in for refuses.
///
/// A kind with no rule takes every time in force the venue can model (see
/// [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue)'s "Which `TimeInForce` this venue
/// honours"). A kind with a rule takes only the [`TimeInForceKind`]s listed, and an order with
/// any other is refused with [`OrderError::UnsupportedTimeInForce`], as the live client refuses
/// it. A rule can only narrow what the venue models: listing a time in force it cannot model does
/// not make it take one.
///
/// For example, Alpaca takes a US options order only as `day` or `gtc`. A venue standing in for
/// it takes the same set for options:
///
/// ```
/// use rustrade_execution::{client::mock::TimeInForceRules, order::TimeInForceKind};
/// use rustrade_instrument::instrument::kind::InstrumentKindDiscriminant;
///
/// let rules = TimeInForceRules::new().with_accepted(
///     InstrumentKindDiscriminant::Option,
///     &[TimeInForceKind::GoodUntilEndOfDay, TimeInForceKind::GoodUntilCancelled],
/// );
/// ```
///
/// With the `alpaca` feature, `AlpacaClient::OPTION_TIME_IN_FORCE` is that set, so
/// `with_accepted(InstrumentKindDiscriminant::Option, AlpacaClient::OPTION_TIME_IN_FORCE)` keeps
/// the venue and the client in step.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct TimeInForceRules(BTreeMap<InstrumentKindDiscriminant, BTreeSet<TimeInForceKind>>);

impl TimeInForceRules {
    /// No rules: every kind takes every time in force the venue can model.
    pub fn new() -> Self {
        Self::default()
    }

    /// With `kind` taking only the time in force in `accepted`, replacing any rule it had.
    #[must_use]
    pub fn with_accepted(
        mut self,
        kind: InstrumentKindDiscriminant,
        accepted: &[TimeInForceKind],
    ) -> Self {
        self.0.insert(kind, accepted.iter().copied().collect());
        self
    }

    /// The time in force `kind` is limited to, or `None` if it has no rule.
    pub fn accepted(&self, kind: InstrumentKindDiscriminant) -> Option<&BTreeSet<TimeInForceKind>> {
        self.0.get(&kind)
    }

    /// Whether an order on an instrument of `kind` may carry `time_in_force`.
    pub fn accepts(&self, kind: InstrumentKindDiscriminant, time_in_force: TimeInForce) -> bool {
        self.accepted(kind)
            .is_none_or(|accepted| accepted.contains(&time_in_force.kind()))
    }

    /// Whether there are no rules.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// What the mock exchange amends in place: see
/// [`ExecutionClient::amend_order`].
///
/// The simulated venue amends a resting limit order's price and total quantity. It has no
/// conditional orders, so it never amends a trigger price.
///
/// - **Priority.** An order keeps its place in the queue only when its quantity is lowered and
///   its price is left alone. Any other amend sends it to the back of the queue at its price, as
///   if it had arrived when the amend did.
/// - **Crossing the market.** An amend whose new price crosses the market trades as a taker, as an
///   arriving limit order does, and what is left rests at the new price. A post-only order's
///   amend that would cross is refused instead.
/// - **Reported.** The account stream reports the amended order's snapshot, stating every term,
///   with its [`Open::time_exchange`](crate::order::state::Open::time_exchange) stamped at the
///   amend whichever way it ranks.
/// - **Refused** with [`AmendRefusal::Venue`](crate::error::AmendRefusal::Venue): an order the
///   venue is holding for a later bar or a fresh market, an order seeded through
///   [`MockExecutionConfig::initial_state`], and an option order.
/// - **Changed outside the client.**
///   [`SimulatedVenue::modify_externally`](crate::exchange::mock::SimulatedVenue::modify_externally)
///   is not limited by this, which says what a client may ask to amend. The other rules above
///   are the book's and apply to it too, except that it can change a seeded order, can rename
///   the order to a new client order id, and ends an order amended down to its filled quantity
///   rather than refusing it.
/// - **Client order ids.** A client's amend keeps the order's client order id. A venue whose
///   amends give the order a new one is simulated only for a change outside the client.
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// start from [`AmendConfig::FULL`] or [`AmendConfig::NONE`] and the `with_*` methods. A field
/// missing from a serialized config takes its value from `FULL`.
#[non_exhaustive]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(default)]
pub struct AmendConfig {
    /// Whether an amend may change the limit price.
    pub price: AmendAllowed,
    /// Whether an amend may change the total quantity.
    pub quantity: AmendAllowed,
    /// Whether one amend may change both.
    pub multi_field: bool,
    /// Whether an amended order is given a new venue order id, as a venue that amends by
    /// replacing the order does. The answer's
    /// [`Amended::previous_id`](crate::order::amend::Amended::previous_id) then names the old
    /// one. A fill the amend itself makes is reported under the old id, and an amend that fills
    /// the order outright reassigns none. Priority still follows the rule above: a quantity
    /// decrease at the same price keeps the order's place under its new id.
    ///
    /// The account stream's snapshot of the amended order names the new id, links the old one
    /// in [`OrderReport::replaces`](crate::order::report::OrderReport::replaces), and arrives
    /// before the answer that says which id it replaced, as a live venue's stream can.
    ///
    /// The order keeps its client order id. A venue that replaces an order under a new client
    /// order id too, as Alpaca does, can be simulated only for a change made from outside any
    /// client: [`SimulatedVenue::modify_externally`] with
    /// [`ExternalChange::with_cid`] renames the order. A client's own amend cannot rename it yet.
    ///
    /// [`SimulatedVenue::modify_externally`]: crate::exchange::mock::SimulatedVenue::modify_externally
    /// [`ExternalChange::with_cid`]: crate::exchange::mock::ExternalChange::with_cid
    pub reassign_id: bool,
    /// With [`reassign_id`](Self::reassign_id): whether the order under its new id starts
    /// afresh, as a venue that carries out an amend by cancelling the order and placing a new
    /// one does, rather than going on as the same order under another id.
    ///
    /// The order under the old id then ends: after the snapshot of the amended order, the
    /// account stream reports it cancelled under the old id, with the quantity filled under that
    /// id. The amended order is still reported in totals, its filled quantity counting what
    /// filled under the old id, as a client reports it after adding the venue's figures back up
    /// (see [`OrderReport`](crate::order::report::OrderReport)'s producer obligations). So
    /// what a reader sees differ is that report of the old id ending.
    ///
    /// Does nothing without [`reassign_id`](Self::reassign_id), or for an amend that fills the
    /// order outright.
    pub filled_restarts: bool,
}

impl Default for AmendConfig {
    /// [`AmendConfig::FULL`].
    fn default() -> Self {
        Self::FULL
    }
}

impl AmendConfig {
    /// Price and quantity can each be amended to any value, together or alone, and the order
    /// keeps its id.
    pub const FULL: Self = Self {
        price: AmendAllowed::Any,
        quantity: AmendAllowed::Any,
        multi_field: true,
        reassign_id: false,
        filled_restarts: false,
    };

    /// Nothing can be amended: every amend is refused, as a venue without amends refuses it.
    pub const NONE: Self = Self {
        price: AmendAllowed::Unsupported,
        quantity: AmendAllowed::Unsupported,
        multi_field: false,
        reassign_id: false,
        filled_restarts: false,
    };

    /// With [`price`](Self::price): whether the limit price may be amended.
    #[must_use]
    pub fn with_price(self, price: AmendAllowed) -> Self {
        Self { price, ..self }
    }

    /// With [`quantity`](Self::quantity): whether the total quantity may be amended.
    #[must_use]
    pub fn with_quantity(self, quantity: AmendAllowed) -> Self {
        Self { quantity, ..self }
    }

    /// With [`multi_field`](Self::multi_field): whether one amend may change both.
    #[must_use]
    pub fn with_multi_field(self, multi_field: bool) -> Self {
        Self {
            multi_field,
            ..self
        }
    }

    /// With [`reassign_id`](Self::reassign_id): whether an amended order gets a new venue order
    /// id.
    #[must_use]
    pub fn with_reassign_id(self, reassign_id: bool) -> Self {
        Self {
            reassign_id,
            ..self
        }
    }

    /// With [`filled_restarts`](Self::filled_restarts): whether an order given a new venue order
    /// id starts afresh under it, the order under the old id reported cancelled.
    #[must_use]
    pub fn with_filled_restarts(self, filled_restarts: bool) -> Self {
        Self {
            filled_restarts,
            ..self
        }
    }

    /// What a client of a venue configured this way declares: no trigger price, since the venue
    /// has no conditional orders, and priority kept only on a quantity decrease.
    pub fn support(&self) -> AmendSupport {
        AmendSupport::NONE
            .with_price(self.price)
            .with_quantity(self.quantity)
            .with_multi_field(self.multi_field)
            .with_keeps_id(!self.reassign_id)
            .with_priority(AmendPriority::KeptOnQuantityDecrease)
    }
}

/// What a simulated venue does with its open orders on an equity that splits, and so what
/// [`MockExecutionConfig::split`] sets. One [`SplitAction`] for a forward split (a ratio of one or
/// more) and one for a reverse split (a ratio below one).
///
/// The default, [`SplitConfig::CANCEL`], cancels every order on a split of either kind.
/// [`SplitConfig::ADJUST_FORWARD`] adjusts on a forward split and cancels on a reverse one, as
/// US equity venues and brokers do under FINRA Rule 5330. It does not apply to orders on options
/// of the equity, which a split always cancels. Whatever the action, the venue rescales the
/// account's holdings of the equity: see
/// [`SimulatedVenue::apply_corporate_action`](crate::exchange::mock::SimulatedVenue::apply_corporate_action).
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// start from a constant and the `with_*` methods. A field missing from a serialized config takes
/// its value from `CANCEL`.
#[non_exhaustive]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(default)]
pub struct SplitConfig {
    /// What a split with a ratio of one or more does to the open orders on its equity.
    pub forward: SplitAction,
    /// What a split with a ratio below one does to the open orders on its equity.
    pub reverse: SplitAction,
}

impl Default for SplitConfig {
    /// [`SplitConfig::CANCEL`].
    fn default() -> Self {
        Self::CANCEL
    }
}

impl SplitConfig {
    /// Every order on the equity is cancelled, on a split of either kind.
    pub const CANCEL: Self = Self {
        forward: SplitAction::Cancel,
        reverse: SplitAction::Cancel,
    };

    /// Buy limit orders are adjusted on a forward split, and every order is cancelled on a reverse
    /// split, as FINRA Rule 5330 has US brokers do.
    pub const ADJUST_FORWARD: Self = Self {
        forward: SplitAction::Adjust,
        reverse: SplitAction::Cancel,
    };

    /// With [`forward`](Self::forward): what a forward split does to the orders.
    #[must_use]
    pub fn with_forward(self, forward: SplitAction) -> Self {
        Self { forward, ..self }
    }

    /// With [`reverse`](Self::reverse): what a reverse split does to the orders.
    #[must_use]
    pub fn with_reverse(self, reverse: SplitAction) -> Self {
        Self { reverse, ..self }
    }

    /// The action for a split of `ratio`: [`reverse`](Self::reverse) below one, else
    /// [`forward`](Self::forward).
    pub fn action(self, ratio: Decimal) -> SplitAction {
        if ratio < Decimal::ONE {
            self.reverse
        } else {
            self.forward
        }
    }
}

/// What a simulated venue does with the open orders on an equity that splits: see
/// [`SplitConfig`].
///
/// `#[non_exhaustive]`, so an action can be added without breaking a caller.
#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SplitAction {
    /// Every order on the equity, resting or held, is cancelled, releasing what it held.
    #[default]
    Cancel,
    /// Each resting buy limit order is adjusted to the split, as FINRA Rule 5330 has US brokers
    /// adjust one, and every other order is cancelled: a sell limit order, and a market order
    /// held for a later bar or a fresh market.
    ///
    /// The limit price is divided by the ratio and rounded down to the instrument's tick size. The
    /// unfilled quantity is multiplied by the ratio and rounded down as the action's
    /// [`SplitRoundingPolicy`] says, to a whole share under `Floor`, and then to the instrument's
    /// quantity step. What has filled is multiplied by the ratio exactly. With no tick size or
    /// step, the price or quantity is exact. An order whose price or unfilled quantity rounds to
    /// zero is cancelled instead.
    Adjust,
}

#[derive(Debug, Constructor)]
pub struct MockExecutionClientConfig<FnTime> {
    pub mocked_exchange: ExchangeId,
    pub clock: FnTime,
    pub request_tx: mpsc::UnboundedSender<MockExchangeRequest>,
    pub event_rx: broadcast::Receiver<UnindexedAccountEvent>,
    /// What the client declares it can amend: the venue's [`AmendConfig::support`]. The
    /// `rustrade` execution builders read it from the same [`MockExecutionConfig::amend`] the
    /// venue was built with. A client built by hand must do the same, or it declares amends its
    /// venue refuses, or refuses amends its venue would make.
    pub amend_support: AmendSupport,
}

impl<FnTime> Clone for MockExecutionClientConfig<FnTime>
where
    FnTime: Clone,
{
    fn clone(&self) -> Self {
        Self {
            mocked_exchange: self.mocked_exchange,
            clock: self.clock.clone(),
            request_tx: self.request_tx.clone(),
            event_rx: self.event_rx.resubscribe(),
            amend_support: self.amend_support,
        }
    }
}

#[derive(Debug, Constructor)]
pub struct MockExecution<FnTime> {
    pub mocked_exchange: ExchangeId,
    pub clock: FnTime,
    pub request_tx: mpsc::UnboundedSender<MockExchangeRequest>,
    pub event_rx: broadcast::Receiver<UnindexedAccountEvent>,
    /// What [`amend_support`](ExecutionClient::amend_support) declares: see
    /// [`MockExecutionClientConfig::amend_support`].
    pub amend_support: AmendSupport,
}

impl<FnTime> Clone for MockExecution<FnTime>
where
    FnTime: Clone,
{
    fn clone(&self) -> Self {
        Self {
            mocked_exchange: self.mocked_exchange,
            clock: self.clock.clone(),
            request_tx: self.request_tx.clone(),
            event_rx: self.event_rx.resubscribe(),
            amend_support: self.amend_support,
        }
    }
}

impl<FnTime> MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc>,
{
    pub fn time_request(&self) -> DateTime<Utc> {
        (self.clock)()
    }
}

impl<FnTime> ExecutionClient for MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc> + Clone + Send + Sync,
{
    const EXCHANGE: ExchangeId = ExchangeId::Mock;

    // `MockExchange` models no funding schedule or margin for a future, so `Perpetual` and `Future`
    // have no faithful projection onto it. `Spot` and `Cfd` are positions the mock can open, close
    // and mark without simulating a lifecycle, and an `Option` is one it settles itself at expiry.
    // Kept in step with the projection in `generate_mock_exchange_instruments`.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[
        InstrumentKindDiscriminant::Spot,
        InstrumentKindDiscriminant::Cfd,
        InstrumentKindDiscriminant::Option,
    ];
    type Config = MockExecutionClientConfig<FnTime>;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        Self {
            mocked_exchange: config.mocked_exchange,
            clock: config.clock,
            request_tx: config.request_tx,
            event_rx: config.event_rx,
            amend_support: config.amend_support,
        }
    }

    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_account_snapshot(
                self.time_request(),
                assets.to_vec(),
                instruments.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    async fn account_stream(
        &self,
        _: &[AssetNameExchange],
        _: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // A lag is delivered in-band as one StreamTerminated, where `map_while` would end the
        // stream with a silent EOF. Whatever its source, a StreamTerminated is terminal by
        // contract, so the stream ends on the poll after it: a lag, or one the venue sends itself,
        // as a market-driven venue does when its market feed lags. It ends there rather than on the
        // next broadcast item, which a quiet venue may never send.
        let exchange = self.mocked_exchange;
        let events = BroadcastStream::new(self.event_rx.resubscribe()).map(move |result| {
            result.unwrap_or_else(|error| {
                error!(
                    ?error,
                    "MockExchange Broadcast AccountStream lagged - terminating"
                );
                UnindexedAccountEvent::stream_terminated(
                    exchange,
                    StreamTerminationReason::Error("mock broadcast stream lagged".to_string()),
                )
            })
        });
        Ok(
            futures::stream::unfold((events, false), |(mut events, ended)| async move {
                if ended {
                    return None;
                }
                let event = events.next().await?;
                let ended = matches!(event.kind, AccountEventKind::StreamTerminated(_));
                Some((event, (events, ended)))
            })
            // `unfold` panics when polled again after it ends, which a combinator downstream may do.
            .fuse()
            .boxed(),
        )
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> UnindexedOrderResponseCancel {
        let (response_tx, response_rx) = oneshot::channel();

        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        if self
            .request_tx
            .send(MockExchangeRequest::cancel_order(
                self.time_request(),
                response_tx,
                request.into_owned_instrument(),
            ))
            .is_err()
        {
            return UnindexedOrderResponseCancel {
                key,
                state: Err(UnindexedOrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            };
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => UnindexedOrderResponseCancel {
                key,
                state: Err(UnindexedOrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            },
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = request.into_owned_instrument();

        if self
            .request_tx
            .send(MockExchangeRequest::open_order(
                self.time_request(),
                response_tx,
                // Carries the snapshot the engine sampled when it decided to send this order on
                // `RequestOpen::market`. It is the venue's only price source for a Market order,
                // which carries no limit price of its own. `None` there means nothing sampled
                // one, and the venue rejects rather than guessing.
                request.clone(),
            ))
            .is_err()
        {
            // The venue's channel is closed, so the request never reached it.
            return Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(OrderError::NotPlaced(NotPlacedReason::NotSent(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                ))),
            };
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(OrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            },
        }
    }

    fn amend_support(&self) -> AmendSupport {
        self.amend_support
    }

    async fn amend_order(
        &self,
        request: OrderRequestAmend<ExchangeId, &InstrumentNameExchange>,
    ) -> UnindexedOrderResponseAmend {
        let (response_tx, response_rx) = oneshot::channel();

        let request = request.into_owned_instrument();
        let key = request.key.clone();
        let offline =
            || OrderError::Connectivity(ConnectivityError::ExchangeOffline(self.mocked_exchange));

        if self
            .request_tx
            .send(MockExchangeRequest::amend_order(
                self.time_request(),
                response_tx,
                request,
            ))
            .is_err()
        {
            // The venue's channel is closed. Answered as a cancel's is, as a lost connection,
            // which the manager settles by looking the order up.
            return UnindexedOrderResponseAmend {
                key,
                state: Err(offline()),
            };
        }

        response_rx
            .await
            .unwrap_or_else(|_| UnindexedOrderResponseAmend {
                key,
                state: Err(offline()),
            })
    }

    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_balances(
                self.time_request(),
                assets.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_orders_open(
                self.time_request(),
                instruments.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    /// The mock venue holds every trade and answers a span whole, so the read is always
    /// complete.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_trades(
                self.time_request(),
                response_tx,
                start,
                end,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        let mut trades = response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })?;
        trades.retain(|trade| is_requested_instrument(instruments, &trade.instrument));
        Ok(TradesRead::complete(trades))
    }
}

/// Answered from the venue's own ledger: an order it filled, cancelled or retired at its deadline
/// is reported, and one still open or never held is omitted. The venue rejects an order only on
/// arrival, in the response to its open request, so it never reports `OpenFailed`.
impl<FnTime> OrderStatusClient for MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc> + Clone + Send + Sync,
{
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_orders_ended(
                self.time_request(),
                orders.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }
}

/// Answered by the simulated venue from its own ledger and the markets it holds: see
/// [`SimulatedVenue::borrow_capacity`](crate::exchange::mock::venue::SimulatedVenue::borrow_capacity)
/// for how it values the account, and when it answers `None` or an error. A venue without a
/// [`SpotMarginConfig`] lends against no instrument, so answers
/// [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for every one.
impl<FnTime> BorrowCapacityClient for MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc> + Clone + Send + Sync,
{
    async fn fetch_borrow_capacity(
        &self,
        instrument: &InstrumentNameExchange,
        side: Side,
    ) -> Result<BorrowCapacity, UnindexedClientError> {
        request_borrow_capacity(
            &self.request_tx,
            self.mocked_exchange,
            self.time_request(),
            instrument,
            side,
        )
        .await
    }
}

/// Asks the [`MockExchange`](crate::exchange::mock::MockExchange) that reads `request_tx` how much
/// the account can borrow now for an order on `instrument` on `side`, with the request stamped
/// `time_request`.
///
/// What [`MockExecution`]'s [`BorrowCapacityClient`] implementation sends, for a caller that holds
/// the exchange's request channel without a client, such as a handle to a paper venue. The answer
/// and its errors are the same.
///
/// # Errors
/// - [`ConnectivityError::ExchangeOffline`] for `mocked_exchange` when the exchange is not running:
///   it never started, or it has stopped.
/// - [`ClientError::Api`] with the venue's own error, as
///   [`SimulatedVenue::borrow_capacity`](crate::exchange::mock::venue::SimulatedVenue::borrow_capacity)
///   says.
pub async fn request_borrow_capacity(
    request_tx: &mpsc::UnboundedSender<MockExchangeRequest>,
    mocked_exchange: ExchangeId,
    time_request: DateTime<Utc>,
    instrument: &InstrumentNameExchange,
    side: Side,
) -> Result<BorrowCapacity, UnindexedClientError> {
    let offline =
        || UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(mocked_exchange));
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(MockExchangeRequest::fetch_borrow_capacity(
            time_request,
            instrument.clone(),
            side,
            response_tx,
        ))
        .map_err(|_| offline())?;

    response_rx
        .await
        .map_err(|_| offline())?
        .map_err(ClientError::Api)
}

/// Asks the [`MockExchange`](crate::exchange::mock::MockExchange) that reads `request_tx` to send a
/// [`VenueCheckpoint`](crate::exchange::mock::venue::VenueCheckpoint) on its account stream, with
/// the request stamped `time_request`.
///
/// Answers with the checkpoint's id once the exchange has sent it, after every account event it
/// produced before it. Sent is not received: a consumer whose account stream ended or lagged
/// before the checkpoint reached it never sees it, and should ask again.
///
/// The exchange takes the checkpoint once the request reaches it, whether or not this call is
/// still awaited, so a dropped call can still put a checkpoint on the account stream.
///
/// # Errors
/// [`ConnectivityError::ExchangeOffline`] for `mocked_exchange` when the exchange is not running:
/// it never started, or it has stopped.
pub async fn request_checkpoint(
    request_tx: &mpsc::UnboundedSender<MockExchangeRequest>,
    mocked_exchange: ExchangeId,
    time_request: DateTime<Utc>,
) -> Result<u64, UnindexedClientError> {
    let offline =
        || UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(mocked_exchange));
    let (response_tx, response_rx) = oneshot::channel();
    request_tx
        .send(MockExchangeRequest::checkpoint(time_request, response_tx))
        .map_err(|_| offline())?;
    response_rx.await.map_err(|_| offline())
}

/// Asks the [`MockExchange`](crate::exchange::mock::MockExchange) that reads `request_tx` to apply
/// the corporate action `id` of `kind` to `instrument`, rounding a share count it leaves
/// fractional as `policy` says, with the request stamped `time_request`.
///
/// The exchange applies it when the request reaches it, as
/// [`SimulatedVenue::apply_corporate_action`](crate::exchange::mock::venue::SimulatedVenue::apply_corporate_action)
/// says, sends what it caused on its account stream, and answers with what it did. It applies it
/// whether or not this call is still awaited.
///
/// # Errors
/// [`ConnectivityError::ExchangeOffline`] for `mocked_exchange` when the exchange is not running:
/// it never started, or it has stopped.
pub async fn request_corporate_action(
    request_tx: &mpsc::UnboundedSender<MockExchangeRequest>,
    mocked_exchange: ExchangeId,
    time_request: DateTime<Utc>,
    id: SmolStr,
    instrument: InstrumentNameExchange,
    kind: CorporateActionKind,
    policy: SplitRoundingPolicy,
) -> Result<CorporateActionResponse, UnindexedClientError> {
    let offline =
        || UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(mocked_exchange));
    let (response_tx, response_rx) = oneshot::channel();
    request_tx
        .send(MockExchangeRequest::corporate_action(
            time_request,
            response_tx,
            id,
            instrument,
            kind,
            policy,
        ))
        .map_err(|_| offline())?;
    response_rx.await.map_err(|_| offline())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{AccountEventKind, balance::Balance, expiry::ExpiryAnchor};
    use rust_decimal::Decimal;
    use rustrade_integration::collection::snapshot::Snapshot;

    #[test]
    fn a_split_config_takes_its_reverse_action_below_a_ratio_of_one_and_its_forward_one_otherwise()
    {
        let config = SplitConfig::CANCEL.with_forward(SplitAction::Adjust);
        assert_eq!(config.action(Decimal::new(5, 1)), SplitAction::Cancel);
        assert_eq!(config.action(Decimal::ONE), SplitAction::Adjust);
        assert_eq!(config.action(Decimal::TWO), SplitAction::Adjust);
        assert_eq!(
            SplitConfig::ADJUST_FORWARD.with_reverse(SplitAction::Adjust),
            SplitConfig {
                forward: SplitAction::Adjust,
                reverse: SplitAction::Adjust,
            }
        );
        assert_eq!(SplitConfig::default(), SplitConfig::CANCEL);
    }

    #[test]
    fn a_stored_config_without_a_split_rule_cancels_and_a_partial_rule_takes_cancel_for_the_rest() {
        let config = MockExecutionConfig::new(
            ExchangeId::Simulated,
            UnindexedAccountSnapshot {
                exchange: ExchangeId::Simulated,
                balances: vec![],
                instruments: vec![],
            },
            0,
        );
        let mut json = serde_json::to_value(&config).unwrap();
        json.as_object_mut().unwrap().remove("split");
        let read: MockExecutionConfig = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(read.split, SplitConfig::CANCEL);

        json["split"] = serde_json::json!({ "forward": "adjust" });
        let read: MockExecutionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(read.split, SplitConfig::ADJUST_FORWARD);
    }

    #[test]
    fn an_amend_config_declares_what_it_allows_and_never_a_trigger() {
        let full = AmendConfig::FULL.support();
        assert_eq!(
            full,
            AmendSupport::NONE
                .with_price(AmendAllowed::Any)
                .with_quantity(AmendAllowed::Any)
                .with_multi_field(true)
                .with_keeps_id(true)
                .with_priority(AmendPriority::KeptOnQuantityDecrease)
        );

        let narrow = AmendConfig::NONE
            .with_quantity(AmendAllowed::DecreaseOnly)
            .with_reassign_id(true)
            .support();
        assert_eq!(narrow.price, AmendAllowed::Unsupported);
        assert_eq!(narrow.quantity, AmendAllowed::DecreaseOnly);
        assert_eq!(narrow.trigger_price, AmendAllowed::Unsupported);
        assert!(!narrow.multi_field);
        assert!(!narrow.keeps_id);
        assert!(AmendConfig::NONE.support().is_none());
        assert_eq!(AmendConfig::default(), AmendConfig::FULL);
    }

    #[test]
    fn time_in_force_rules_narrow_only_the_kinds_they_name() {
        let rules = TimeInForceRules::new().with_accepted(
            InstrumentKindDiscriminant::Option,
            &[
                TimeInForceKind::GoodUntilEndOfDay,
                TimeInForceKind::GoodUntilCancelled,
            ],
        );
        let option = InstrumentKindDiscriminant::Option;
        assert!(rules.accepts(option, TimeInForce::GoodUntilEndOfDay));
        assert!(rules.accepts(option, TimeInForce::GoodUntilCancelled { post_only: false }));
        assert!(!rules.accepts(option, TimeInForce::GoodUntilCancelled { post_only: true }));
        assert!(!rules.accepts(option, TimeInForce::ImmediateOrCancel));
        assert!(rules.accepts(
            InstrumentKindDiscriminant::Spot,
            TimeInForce::ImmediateOrCancel
        ));
        assert!(TimeInForceRules::new().accepts(option, TimeInForce::FillOrKill));
        assert!(!rules.is_empty());

        let json = serde_json::to_value(&rules).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"option": ["GoodUntilCancelled", "GoodUntilEndOfDay"]})
        );
        assert_eq!(
            serde_json::from_value::<TimeInForceRules>(json).unwrap(),
            rules
        );
    }

    fn empty_snapshot() -> UnindexedAccountSnapshot {
        UnindexedAccountSnapshot {
            exchange: ExchangeId::Mock,
            balances: Vec::new(),
            instruments: Vec::new(),
        }
    }

    #[test]
    fn a_new_config_holds_the_defaults_a_stored_config_reads_with() {
        let built = MockExecutionConfig::new(ExchangeId::Mock, empty_snapshot(), 40);
        let read: MockExecutionConfig = serde_json::from_value(serde_json::json!({
            "mocked_exchange": serde_json::to_value(ExchangeId::Mock).unwrap(),
            "initial_state": serde_json::to_value(empty_snapshot()).unwrap(),
            "latency_ms": 40,
        }))
        .unwrap();
        assert_eq!(built, read);

        let missing_latency = serde_json::json!({
            "mocked_exchange": serde_json::to_value(ExchangeId::Mock).unwrap(),
            "initial_state": serde_json::to_value(empty_snapshot()).unwrap(),
        });
        assert!(
            serde_json::from_value::<MockExecutionConfig>(missing_latency).is_err(),
            "a stored config must state its latency"
        );
    }

    #[test]
    fn each_setter_sets_its_own_setting() {
        use crate::{
            fee::PercentageFeeModel,
            fill::{BidAskFillModel, NextBarPrice},
            margin::MarginEffect,
        };
        use chrono::NaiveTime;
        use rust_decimal_macros::dec;
        use std::time::Duration;

        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.001)));
        let option_fee = FeeModelConfig::Percentage(PercentageFeeModel::new(dec!(0.002)));
        let fee_model_overrides =
            FeeModelOverrides::new().with_kind(InstrumentKindDiscriminant::Option, option_fee);
        let slippage = SlippageConfig::uniform(dec!(0.0001), dec!(0.0002), 7).unwrap();
        let stale_market = StaleMarketPolicy::Wait {
            max_age: Duration::from_secs(5),
        };
        let spot_margin = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap();
        let fee_asset = FeeAssetConfig::new(AssetNameExchange::new("bnb"), dec!(0.25)).unwrap();
        let time_in_force = TimeInForceRules::new().with_accepted(
            InstrumentKindDiscriminant::Option,
            &[TimeInForceKind::GoodUntilEndOfDay],
        );
        let session_close = SessionCalendarConfig::Daily(DailyClose::new(
            NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            "America/New_York".parse().unwrap(),
        ));
        let session_close_overrides = SessionCloseOverrides::new().with_kind(
            InstrumentKindDiscriminant::Option,
            SessionCalendarConfig::Daily(DailyClose::new(
                NaiveTime::from_hms_opt(16, 15, 0).unwrap(),
                "America/New_York".parse().unwrap(),
            )),
        );
        let expiry_sell_out =
            ExpirySellOut::new(ExpiryAnchor::BeforeClose(Duration::from_secs(3600)));
        let expiry_day_cutoff =
            ExpiryDayCutoff::new(ExpiryAnchor::BeforeClose(Duration::from_secs(2700)));

        let config = MockExecutionConfig::new(ExchangeId::Mock, empty_snapshot(), 40)
            .with_fee_model(fee_model.clone())
            .with_fill_model(SimFillConfig::BidAsk(BidAskFillModel))
            .with_spot_fee_asset(SpotFeeAsset::Received)
            .with_slippage(slippage)
            .with_market_order_timing(MarketOrderTiming::NextBar(NextBarPrice::Open))
            .with_stale_market(stale_market)
            .with_spot_margin(spot_margin.clone())
            .with_fee_asset(fee_asset.clone())
            .with_long_option_expiry(LongOptionExpiry::Lapse)
            .with_short_put_collateral(ShortPutCollateral::StrikeLessPremium)
            .with_amend(AmendConfig::NONE)
            .with_time_in_force(time_in_force.clone())
            .with_session_close(session_close.clone())
            .with_session_close_overrides(session_close_overrides.clone())
            .with_fee_model_overrides(fee_model_overrides.clone())
            .with_expiry_sell_out(expiry_sell_out)
            .with_expiry_day_cutoff(expiry_day_cutoff);

        assert_eq!(config.mocked_exchange, ExchangeId::Mock);
        assert_eq!(config.initial_state, empty_snapshot());
        assert_eq!(config.latency_ms, 40);
        assert_eq!(config.fee_model, fee_model);
        assert_eq!(config.fill_model, SimFillConfig::BidAsk(BidAskFillModel));
        assert_eq!(config.spot_fee_asset, SpotFeeAsset::Received);
        assert_eq!(config.slippage, slippage);
        assert_eq!(
            config.market_order_timing,
            MarketOrderTiming::NextBar(NextBarPrice::Open)
        );
        assert_eq!(config.stale_market, stale_market);
        assert_eq!(config.spot_margin, Some(spot_margin));
        assert_eq!(config.fee_asset, Some(fee_asset));
        assert_eq!(config.long_option_expiry, LongOptionExpiry::Lapse);
        assert_eq!(
            config.short_put_collateral,
            ShortPutCollateral::StrikeLessPremium
        );
        assert_eq!(config.amend, AmendConfig::NONE);
        assert_eq!(config.time_in_force, time_in_force);
        assert_eq!(config.session_close, Some(session_close));
        assert_eq!(config.session_close_overrides, session_close_overrides);
        assert_eq!(config.expiry_sell_out, Some(expiry_sell_out));
        assert_eq!(config.expiry_day_cutoff, Some(expiry_day_cutoff));
        assert_eq!(config.fee_model_overrides, fee_model_overrides);
    }

    #[test]
    fn an_amend_config_missing_from_a_serialized_config_is_full() {
        let config = MockExecutionConfig::new(
            ExchangeId::Mock,
            UnindexedAccountSnapshot {
                exchange: ExchangeId::Mock,
                balances: Vec::new(),
                instruments: Vec::new(),
            },
            0,
        )
        .with_amend(AmendConfig::NONE);
        let mut json = serde_json::to_value(&config).unwrap();
        assert!(
            json.get("time_in_force").is_none(),
            "no rules are left out of a written config"
        );
        json.as_object_mut().unwrap().remove("amend");
        let read: MockExecutionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(read.amend, AmendConfig::FULL);
        assert!(read.time_in_force.is_empty());

        let partial: AmendConfig = serde_json::from_str(r#"{"reassign_id": true}"#).unwrap();
        assert_eq!(partial, AmendConfig::FULL.with_reassign_id(true));
    }

    fn filler_event() -> UnindexedAccountEvent {
        // Content is irrelevant — these events are overwritten by the lag before they can be
        // delivered; they exist only to overflow the broadcast buffer.
        UnindexedAccountEvent::new(
            ExchangeId::Mock,
            AccountEventKind::BalanceSnapshot(Snapshot::new(AssetBalance {
                asset: AssetNameExchange::new("btc"),
                balance: Balance::new(Decimal::ONE, Decimal::ONE),
                time_exchange: Utc::now(),
            })),
        )
    }

    /// `fetch_trades` asks the mock exchange for its span and keeps the requested instruments'
    /// trades, reading the span whole.
    #[tokio::test]
    async fn fetch_trades_reads_the_span_for_the_requested_instruments() {
        use crate::{
            order::id::{OrderId, StrategyId},
            trade::{AssetFees, Trade, TradeId},
        };
        use rustrade_instrument::Side;

        let (_event_tx, event_rx) = broadcast::channel::<UnindexedAccountEvent>(1);
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let client = MockExecution::new(
            ExchangeId::Mock,
            Utc::now,
            request_tx,
            event_rx,
            AmendSupport::NONE,
        );
        let start = Utc::now() - chrono::TimeDelta::hours(1);
        let end = Utc::now();

        let trade = move |instrument: &str| {
            Trade::new(
                TradeId::new(instrument),
                OrderId::new("order"),
                InstrumentNameExchange::new(instrument),
                StrategyId::unknown(),
                start,
                Side::Buy,
                Decimal::ONE,
                Decimal::ONE,
                None,
                AssetFees::new(AssetNameExchange::new("usdt"), Decimal::ZERO, None),
            )
        };
        let exchange = tokio::spawn(async move {
            let request = request_rx.recv().await.expect("a request");
            let crate::exchange::mock::request::MockExchangeRequestKind::FetchTrades {
                response_tx,
                start: asked_start,
                end: asked_end,
            } = request.kind
            else {
                panic!("expected FetchTrades, got {:?}", request.kind);
            };
            assert_eq!((asked_start, asked_end), (start, end));
            response_tx
                .send(vec![trade("btc_usdt"), trade("eth_usdt")])
                .expect("the client awaits the answer");
        });

        let read = client
            .fetch_trades(start, end, &[InstrumentNameExchange::new("eth_usdt")])
            .await
            .unwrap();
        exchange.await.unwrap();
        assert_eq!(read, TradesRead::complete(vec![trade("eth_usdt")]));
    }

    /// The client sends the assets and instruments it was asked for in its snapshot request, for
    /// the venue to filter the balances and the instruments by.
    #[tokio::test]
    async fn account_snapshot_asks_the_venue_for_the_requested_assets_and_instruments() {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel::<MockExchangeRequest>();
        let (_event_tx, event_rx) = broadcast::channel::<UnindexedAccountEvent>(1);
        let client = MockExecution::new(
            ExchangeId::Mock,
            Utc::now,
            request_tx,
            event_rx,
            AmendSupport::NONE,
        );
        let exchange = tokio::spawn(async move {
            let request = request_rx.recv().await.expect("a request");
            let crate::exchange::mock::request::MockExchangeRequestKind::FetchAccountSnapshot {
                assets,
                instruments,
                response_tx,
            } = request.kind
            else {
                panic!("expected FetchAccountSnapshot, got {:?}", request.kind);
            };
            response_tx
                .send(empty_snapshot())
                .expect("the caller awaits the answer");
            (assets, instruments)
        });

        let assets = [AssetNameExchange::new("usdt")];
        let instruments = [InstrumentNameExchange::new("BTCUSDT")];
        client
            .account_snapshot(&assets, &instruments)
            .await
            .unwrap();

        assert_eq!(
            exchange.await.unwrap(),
            (assets.to_vec(), instruments.to_vec())
        );
    }

    /// `request_checkpoint` asks the mock exchange for a checkpoint and answers with the id the
    /// exchange gave it, or that the exchange is offline once it has stopped.
    #[tokio::test]
    async fn request_checkpoint_answers_with_the_exchange_s_id() {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel::<MockExchangeRequest>();
        let exchange = tokio::spawn(async move {
            let request = request_rx.recv().await.expect("a request");
            let crate::exchange::mock::request::MockExchangeRequestKind::Checkpoint { response_tx } =
                request.kind
            else {
                panic!("expected Checkpoint, got {:?}", request.kind);
            };
            response_tx.send(7).expect("the caller awaits the answer");
        });

        let id = request_checkpoint(&request_tx, ExchangeId::Mock, Utc::now()).await;
        // Checked before joining the exchange's task, which waits for a request until one comes.
        assert_eq!(id.unwrap(), 7);
        exchange.await.unwrap();

        // The exchange's task has ended and dropped its receiver.
        assert!(matches!(
            request_checkpoint(&request_tx, ExchangeId::Mock, Utc::now()).await,
            Err(UnindexedClientError::Connectivity(
                ConnectivityError::ExchangeOffline(ExchangeId::Mock)
            ))
        ));
    }

    /// Broadcast lag is a terminal stream death: the account stream must deliver an in-band
    /// `StreamTerminated(Error)` (not a silent EOF) and then end.
    #[tokio::test]
    async fn account_stream_emits_stream_terminated_on_broadcast_lag() {
        // Capacity 1 so a burst of sends with no reader immediately overflows → lag.
        let (event_tx, event_rx) = broadcast::channel::<UnindexedAccountEvent>(1);
        let (request_tx, _request_rx) = mpsc::unbounded_channel();
        // `MockExecution` derives `Constructor`, so its inherent `new` takes the four fields
        // directly (the `ExecutionClient::new(config)` trait method is shadowed here).
        let client = MockExecution::new(
            ExchangeId::Mock,
            Utc::now,
            request_tx,
            event_rx,
            AmendSupport::NONE,
        );

        let mut stream = client.account_stream(&[], &[]).await.unwrap();

        // Overflow the buffer before the stream's receiver reads anything → force a lag.
        for _ in 0..3 {
            event_tx.send(filler_event()).unwrap();
        }

        // The lag is surfaced in-band as the terminal event...
        let event = stream.next().await.expect("expected a terminal event");
        assert!(
            matches!(
                event.kind,
                AccountEventKind::StreamTerminated(StreamTerminationReason::Error(_))
            ),
            "expected StreamTerminated(Error) on broadcast lag, got {:?}",
            event.kind
        );
        assert_eq!(event.exchange, ExchangeId::Mock);

        // ...after which the stream ends.
        assert!(
            stream.next().await.is_none(),
            "stream must end after the terminal event"
        );
    }

    /// A terminal event the venue sends itself, as a market-driven one does when its market feed
    /// lags, ends the stream on the next poll, without waiting for an event a quiet venue may
    /// never send.
    #[tokio::test]
    async fn account_stream_ends_right_after_a_terminal_event_from_the_venue() {
        let (event_tx, event_rx) = broadcast::channel::<UnindexedAccountEvent>(16);
        let (request_tx, _request_rx) = mpsc::unbounded_channel();
        let client = MockExecution::new(
            ExchangeId::Mock,
            Utc::now,
            request_tx,
            event_rx,
            AmendSupport::NONE,
        );
        let mut stream = client.account_stream(&[], &[]).await.unwrap();

        event_tx.send(filler_event()).unwrap();
        event_tx
            .send(UnindexedAccountEvent::stream_terminated(
                ExchangeId::Mock,
                StreamTerminationReason::Error("market feed lagged".to_string()),
            ))
            .unwrap();

        assert!(matches!(
            stream.next().await.map(|event| event.kind),
            Some(AccountEventKind::BalanceSnapshot(_))
        ));
        assert!(matches!(
            stream.next().await.map(|event| event.kind),
            Some(AccountEventKind::StreamTerminated(_))
        ));
        assert_eq!(
            futures::FutureExt::now_or_never(stream.next()),
            Some(None),
            "the stream must end on the poll after its terminal event, with the sender still open"
        );
        assert_eq!(
            futures::FutureExt::now_or_never(stream.next()),
            Some(None),
            "and stay ended however often it is polled"
        );
        drop(event_tx);
    }
}
