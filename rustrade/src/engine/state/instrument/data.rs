use crate::{
    Timed,
    engine::{Processor, state::order::in_flight_recorder::InFlightRequestRecorder},
};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_data::{
    event::{DataKind, MarketEvent},
    subscription::{book::OrderBookL1, candle::Candle},
};
use rustrade_execution::{
    AccountEvent,
    market::MarketSnapshot,
    order::request::{OrderRequestCancel, OrderRequestOpen},
};
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

/// Defines a state object for tracking and managing custom instrument level data.
///
/// Implementations must handle market event & account event processing, as well as logic for
/// determining the latest instrument market price.
///
/// This trait enables users to define their own instrument level data, and specify the type of
/// [`MarketEvent`] that is required to update it. The custom instrument data could include
/// market data, strategy-specific data, risk-specific data, or any other instrument level data.
///
/// For an example, see the [`DefaultInstrumentMarketData`] implementation.
pub trait InstrumentDataState<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> where
    Self: Debug
        + Clone
        + for<'a> Processor<&'a MarketEvent<InstrumentKey, Self::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
        + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
{
    /// [`MarketEvent<_, EventKind>`](MarketEvent) expected by this instrument data state.
    type MarketEventKind: Debug + Clone + Send;

    /// Latest price for an instrument, if available.
    ///
    /// Return the latest market price for an instrument, if available.
    ///
    /// An instrument price could be derived in many ways, but some common examples include:
    /// - Most recent `PublicTrade` price.
    /// - Volume-weighted mid-price from an `OrderBookL1`.
    /// - Close of the most recent `Candle`.
    /// - Volume-weighted mid-price from an `OrderBookL2`.
    fn price(&self) -> Option<Decimal>;

    /// Market state for this instrument right now, for a simulated venue to price a fill against.
    ///
    /// The `Engine` samples this when it emits an order request and carries the result on
    /// [`RequestOpen::market`](rustrade_execution::order::request::RequestOpen::market). A
    /// simulated venue has no book of its own, so for a market order — which carries no limit price
    /// — this snapshot is the only thing standing between a fill and a rejection.
    ///
    /// # Default body
    ///
    /// Reports [`price`](Self::price) as the last traded price and no book. That is what a state
    /// tracking a single price can honestly say, and it is enough for the default
    /// [`LastPriceFillModel`](rustrade_execution::fill::LastPriceFillModel) to fill. Override it to
    /// supply bid and ask, which the
    /// [`BidAskFillModel`](rustrade_execution::fill::BidAskFillModel) and
    /// [`MidpointFillModel`](rustrade_execution::fill::MidpointFillModel) need in order to model
    /// the spread.
    ///
    /// # Look-ahead
    ///
    /// Return only what this state has already processed. The point of sampling here rather than at
    /// the venue is that this instant has a well-defined position on the simulated timeline;
    /// reaching past it for a price that has not been consumed yet would reintroduce exactly the
    /// look-ahead that sampling here avoids.
    fn market_snapshot(&self) -> MarketSnapshot {
        MarketSnapshot::from_last_price(self.price())
    }
}

/// Basic [`InstrumentDataState`] implementation that tracks the [`OrderBookL1`], last traded price,
/// and last [`Candle`] for an instrument.
///
/// This is a simple example of instrument level data. Trading strategies typically maintain more
/// comprehensive data, such as rolling candle windows, technical indicators, market depth (L2 book),
/// volatility metrics, or strategy-specific state data.
///
/// The whole [`Candle`] is retained rather than just its close, since strategies consuming a
/// candle feed generally want OHLCV.
///
/// # Which [`DataKind`]s update this state
/// [`Trade`](DataKind::Trade), [`OrderBookL1`](DataKind::OrderBookL1) and
/// [`Candle`](DataKind::Candle) are price inputs. [`OrderBook`](DataKind::OrderBook) (L2),
/// [`Liquidation`](DataKind::Liquidation) and [`OptionGreeks`](DataKind::OptionGreeks) are
/// deliberately ignored — see the [`Processor`] impl, where each exclusion is stated with its
/// reason.
///
/// Does not implement `Ord`/`PartialOrd`: `last_traded_price` holds a [`Timed`] value, whose
/// whole-struct ordering is intentionally not provided (see [`Timed`] docs).
///
/// # Deserialising state written by an older build
/// Fields added after this type shipped carry `#[serde(default)]` individually, so state persisted
/// before such a field existed still loads with that field taking its `Default`. Without it serde
/// treats a missing key as a hard error even for an `Option` — deriving `Default` does not change
/// that — and an engine-state snapshot, audit replica or replay stream from an earlier version
/// would fail to load outright.
///
/// The attribute is deliberately **per field rather than on the container**. A container-level
/// `#[serde(default)]` also makes `l1` and `last_traded_price` optional, and their `Default` is not
/// a neutral value: an absent `l1` would load as an epoch-stamped empty book, which
/// [`price`](InstrumentDataState::price) reads as "no price yet" rather than as the truncated
/// snapshot it is. Per field, a new field's author has to decide whether its `Default` is a sound
/// stand-in for "written before this existed" — which for `candle` it is, and for a book it is not.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default, Deserialize, Serialize, Constructor)]
pub struct DefaultInstrumentMarketData {
    pub l1: OrderBookL1,
    pub last_traded_price: Option<Timed<Decimal>>,
    /// Most recent [`Candle`], ordered on its
    /// [`close_time`](rustrade_data::subscription::candle::Candle::close_time).
    ///
    /// Only candles that have actually closed by the event's own `time_exchange` are retained; a
    /// candle closing after it is dropped as lookahead (see the [`Processor`] impl).
    ///
    /// Defaults on deserialisation: this field postdates the type, and `None` is exactly what a
    /// build that never tracked candles meant.
    #[serde(default)]
    pub candle: Option<Candle>,
}

impl InstrumentDataState for DefaultInstrumentMarketData {
    type MarketEventKind = DataKind;

    /// Latest price: the **tick-regime** input (the L1 book where it has a mid, else the last
    /// trade), unless a candle has closed more recently than that input's own stamp.
    ///
    /// `None` if the instrument has received no price input at all.
    ///
    /// # Why the two regimes are ranked differently
    /// Recency is the right rule *across* regimes and the wrong rule *within* one, because the three
    /// stamps are not drawn from a single clock.
    ///
    /// **Across regimes — recency.** On a mixed feed, which one provider alone can produce (a
    /// session of quote ticks followed by a year of daily bars), a fixed precedence means whichever
    /// kind sits at the top wins forever: an L1 book that stopped updating a year ago would mark
    /// every position after it, `pnl_unrealised` would stop moving, and the tear sheet would look
    /// entirely normal. A candle therefore wins when it closed strictly later than the held tick.
    /// The gap this decides is usually a *regime change* — days to years — which survives clock skew
    /// of any realistic size. It is not always: subscribing to L1 **and** candles on one connector
    /// concurrently is supported, and on a venue whose book instant is host-stamped that narrows the
    /// gap to the bar interval. What remains there is a skew-sized window just after each bar close
    /// in which the candle can win by the host's error rather than the market's — bounded by the
    /// skew, self-correcting once a tick is stamped past the bar close, and far smaller than ranking
    /// two tick stamps against each other, but not nothing.
    ///
    /// **Within the tick regime — fixed precedence, L1 over trade.** These two stamps routinely come
    /// from different clocks on the same connector, and the gap between them is milliseconds, so a
    /// recency comparison would be decided by the skew rather than by the market.
    /// [`OrderBookL1::last_update_time`] is a *payload* field and several venues publish no book
    /// instant at all — Binance Spot's `bookTicker` and IBKR's tick stream both carry none, so their
    /// connectors stamp it from the host clock — while `time_exchange` on a trade is the venue's own
    /// instant. Ranking those against each other means a host running 200 ms behind the venue (well
    /// inside the skew Binance's own `recvWindow` tolerates, and routine on a VM after
    /// suspend/resume) flips every mark from the book mid to the last trade, with no log and no
    /// error. An NTP step mid-session flips it back. L1 is preferred outright instead: it is the
    /// finer view of the current market, and the choice no longer depends on a clock comparison that
    /// cannot be made soundly.
    ///
    /// # Known limitation
    /// The flip side of that fixed precedence: an L1 book that **freezes while trades keep
    /// arriving** shadows those trades for as long as it stays frozen. That is narrower than the
    /// cross-regime hole above — both inputs come from one connector's live feed, so a book that
    /// stops while the trade tape runs is a connector defect rather than a feed shape — but it is
    /// real, and it is the price of not comparing two clocks. A strategy that needs trade-led marks
    /// on such a feed should implement its own [`InstrumentDataState`].
    ///
    /// # The L1 contribution
    /// The volume-weighted mid-price where the book publishes sizes, else the plain mid.
    /// A **one-sided** book contributes nothing: half a book has no mid, and
    /// marking a position at whichever side happens to be quoted is a judgement this state does not
    /// make on the caller's behalf. A book contributing nothing falls through to the last trade, so
    /// the fixed precedence never *costs* a price.
    ///
    /// # Caller obligation
    /// A candle producer must set `close_time` to the true end of the bar's period, and stamp
    /// `MarketEvent::time_exchange` with it (see the [`Processor`] impl, which rejects a candle
    /// closing after its own `time_exchange` as lookahead). The tick-regime stamps are no longer
    /// ranked against each other, so a producer that stamps [`OrderBookL1::last_update_time`] from a
    /// local clock no longer loses the mark to a *trade*. It still costs the freshness guard in
    /// `process`, and — on a feed carrying candles too — the skew-sized window after each bar close
    /// described above.
    fn price(&self) -> Option<Decimal> {
        // Fixed precedence within the tick regime: a book that has a mid wins outright, and a book
        // with nothing to say (one-sided, or never received) falls through to the last trade.
        let tick = self
            .l1_price()
            .map(|price| (self.l1.last_update_time, price))
            .or_else(|| self.last_traded_price.map(|last| (last.time, last.value)));

        match (tick, self.candle) {
            // Strictly later, so an exact tie goes to the tick: `close_time` is the EXCLUSIVE period
            // end, and an observation stamped at that instant belongs to the next period.
            (Some((tick_time, price)), Some(candle)) => Some(if candle.close_time > tick_time {
                candle.close
            } else {
                price
            }),
            (Some((_, price)), None) => Some(price),
            (None, Some(candle)) => Some(candle.close),
            (None, None) => None,
        }
    }

    /// Both sides of the L1 book, plus [`price`](Self::price) as the last traded price.
    ///
    /// # Why `last_price` is `price`, not `last_traded_price`
    ///
    /// `price` is this state's considered answer to "what is this instrument worth right now",
    /// across all three inputs it tracks — including a candle close on a bar feed, where
    /// `last_traded_price` is `None` and a fill model reading it would find nothing to price
    /// against. Feeding the fill model the same mark the engine values positions at also keeps a
    /// simulated fill consistent with the PnL computed from it.
    ///
    /// The book's own sides are passed through unchanged rather than folded into that answer, so a
    /// fill model that wants to model the spread sees the real one.
    fn market_snapshot(&self) -> MarketSnapshot {
        MarketSnapshot {
            best_bid: self.l1.best_bid.map(|level| level.price),
            best_ask: self.l1.best_ask.map(|level| level.price),
            last_price: self.price(),
        }
    }
}

/// Lookahead candles dropped by [`DefaultInstrumentMarketData`] in this process.
///
/// Rate-limits the warning that accompanies the drop. A mis-stamped producer is not a one-off — it
/// stamps every bar the same way — so an unconditional `warn!` is one line per candle, millions of
/// them on a large backtest. Logging on the 1st, 2nd, 4th, 8th … occurrence keeps the condition
/// observable, which is the entire reason it is logged (`Processor::Audit` is `()` here, so there
/// is no error channel to use instead), at a logarithmic number of lines. The running total rides
/// along on each line, so a reader can see the true scale from any one of them.
///
/// Process-global rather than per-instrument: `InstrumentKey` carries no bound this impl could key
/// a map on, which is the same reason the warning cannot name the instrument it came from.
static LOOKAHEAD_CANDLES_DROPPED: AtomicU64 = AtomicU64::new(0);

impl DefaultInstrumentMarketData {
    /// The [`OrderBookL1`]'s price contribution, or `None` if it has none to make.
    ///
    /// The volume-weighted mid-price, falling back to the plain mid when **both** best levels carry
    /// a zero amount. That is not a degenerate book: a feed publishing prices without sizes — a
    /// bulk export, an FX quote tape — produces it on every row, and the weighting is undefined
    /// there while the mid is perfectly well defined. Without the fallback such a feed would mark no
    /// position at all.
    fn l1_price(&self) -> Option<Decimal> {
        self.l1
            .volume_weighed_mid_price()
            .or_else(|| self.l1.mid_price())
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>>
    for DefaultInstrumentMarketData
{
    type Audit = ();

    /// Apply a market event, ignoring any that is older than what is already held.
    ///
    /// A candle whose `close_time` is **ahead of its own `time_exchange`** is dropped with a
    /// warning rather than applied: it would mark positions with the outcome of a period the
    /// engine's clock says is still open. Every in-tree candle producer sets the two fields to the
    /// same value, so this can only fire on a third-party producer — see the `DataKind::Candle`
    /// arm, which also records what it does *not* catch. Every such candle is dropped, but the
    /// warning is rate-limited by a
    /// counter shared by **every instance of this type in the process**: on a multi-instrument run
    /// the power-of-two cadence and the `dropped` total on the line count drops across all of them
    /// combined, so neither is scoped to the exchange the line does name.
    ///
    /// The match is **exhaustive on purpose**: a catch-all is how `DataKind::Candle` went unhandled
    /// here for as long as it did — silently, with no compile error to prompt the edit. Each
    /// excluded variant states why it is not a price input, so a new variant forces a deliberate
    /// decision rather than inheriting a default of "ignore".
    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::Trade(trade) => {
                if self
                    .last_traded_price
                    .as_ref()
                    .is_none_or(|price| price.time < event.time_exchange)
                {
                    self.last_traded_price
                        .replace(Timed::new(trade.price, event.time_exchange));
                }
            }
            // Ordered on the payload's `last_update_time`, not `event.time_exchange`, for the same
            // reason as the candle below: that is the field `price()` reads, so keying the guard on
            // anything else would let the two disagree.
            //
            // NOT every in-tree producer stamps this from the venue: Binance Spot's `bookTicker` and
            // IBKR's tick stream publish no book instant, so those connectors fill it from the host
            // clock (see `OrderBookL1::last_update_time`). That is why `price()` no longer ranks
            // this field against a trade's venue stamp. It still orders books against each other
            // here, which is sound — one connector's books all carry the same clock.
            DataKind::OrderBookL1(l1) => {
                if self.l1.last_update_time < l1.last_update_time {
                    self.l1 = l1.clone()
                }
            }
            // Ordered on `close_time`, not `event.time_exchange`, so the staleness guard and
            // `price()`'s precedence rule key on the same instant. A well-formed producer sets
            // `time_exchange` to `close_time` anyway (see `Candle`'s docs); keying on the payload
            // means a producer that does not cannot desynchronise the two.
            //
            // But that same freedom is a lookahead hole, and the reason for the first guard below.
            // The merge and the engine clock key on `time_exchange`, while everything here keys on
            // `close_time`, so a producer stamping `time_exchange` with the bar's OPEN — an easy
            // and plausible mistake, since that is the timestamp most venues put in the bar record
            // — hands the engine a close price for a period that, by the engine's own clock, has
            // not finished. A strategy would then trade the whole bar on its own outcome. Unlike
            // the L1 arm above, this is not physically impossible input: `close_time` is a computed
            // boundary, not an observed instant, so nothing about the wire format prevents it.
            //
            // Dropped rather than clamped or accepted: there is no correct price to salvage, and a
            // clamp would silently paper over a mis-stamped feed. `process` has no error channel
            // (`Audit = ()`), so the warning is what makes it observable.
            //
            // Reachable only from OUTSIDE this workspace, and worth knowing before trusting it.
            // Every in-tree candle producer sets `time_exchange = close_time` by construction —
            // `lse::parquet`, `databento::transformer` and `massive::transformer` all do — so the
            // two fields are the same value and this comparison cannot fire on any feed this repo
            // ships. It guards a third-party `Processor` input, which is exactly what a public
            // library should guard, but it is not a check on our own transformers.
            //
            // Nor is it a general lookahead check. It compares two fields of one event, so it
            // catches only a producer that disagrees with itself. A producer that derives
            // `close_time` from a wrong bar period and stamps `time_exchange` with that same wrong
            // value — declaring a daily interval over a file of one-minute bars, say — passes here
            // and still injects lookahead. That class has to be caught where the period is
            // declared, not here.
            DataKind::Candle(candle) => {
                if candle.close_time > event.time_exchange {
                    // Rate-limited: see `LOOKAHEAD_CANDLES_DROPPED`. The drop itself is
                    // unconditional; only the line about it is thinned out.
                    let dropped = LOOKAHEAD_CANDLES_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped.is_power_of_two() {
                        // No instrument field: `InstrumentKey` is unbounded here, and narrowing
                        // this impl to `Debug` keys to name it in a log would be a breaking change
                        // for a downstream key type. The exchange and the two instants identify the
                        // mis-stamped producer, which is what the reader needs to act on.
                        tracing::warn!(
                            exchange = %event.exchange,
                            close_time = %candle.close_time,
                            time_exchange = %event.time_exchange,
                            dropped,
                            "dropping candle closing after its own time_exchange: the producer is \
                             stamping the event before the period it describes has ended, which \
                             would inject lookahead. This warning is rate-limited; `dropped` is \
                             the running total for the process"
                        );
                    }
                } else if self
                    .candle
                    .is_none_or(|current| current.close_time < candle.close_time)
                {
                    self.candle = Some(*candle);
                }
            }
            // L2 depth is excluded, not overlooked. Deriving a mid-price from a full book is a real
            // gap (named in `InstrumentDataState::price`'s docs), but satisfying this struct's
            // `Hash` derive would mean threading `Hash` through the entire order book type chain —
            // invasive, and hashing a depth book is close to meaningless. Tracked separately.
            DataKind::OrderBook(_) => {}
            // A liquidation is a FORCED fill at a potentially dislocated price, not a market
            // consensus. Feeding it to `price()` would corrupt `pnl_unrealised`, which is
            // recomputed from that price on every market event. "Handle every variant" is actively
            // wrong here.
            DataKind::Liquidation(_) => {}
            // Greeks are risk sensitivities, not a price, and belong in a dedicated option state
            // rather than a struct cloned per-instrument for every non-option user.
            DataKind::OptionGreeks(_) => {}
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey>
    Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>> for DefaultInstrumentMarketData
{
    type Audit = ();

    fn process(&mut self, _: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self::Audit {}
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey>
    for DefaultInstrumentMarketData
{
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_data::{
        books::Level,
        subscription::{liquidation::Liquidation, trade::PublicTrade},
    };
    use rustrade_instrument::{Side, instrument::InstrumentIndex};

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn event(time: DateTime<Utc>, kind: DataKind) -> MarketEvent<InstrumentIndex, DataKind> {
        MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: rustrade_instrument::exchange::ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(0),
            kind,
        }
    }

    fn trade(price: Decimal) -> DataKind {
        DataKind::Trade(PublicTrade {
            id: "1".into(),
            price,
            amount: dec!(1),
            side: Some(Side::Buy),
        })
    }

    /// A candle closing at `close_time` with `close` as its close price.
    fn candle(close_time: DateTime<Utc>, close: Decimal) -> DataKind {
        DataKind::Candle(Candle {
            close_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(1)),
            trade_count: Some(1),
        })
    }

    /// An L1 with a symmetric book, so the volume-weighted mid-price is exactly `price`.
    ///
    /// `last_update_time` is passed explicitly because it — not the wrapping event's
    /// `time_exchange` — is what both the staleness guard and `price()` order on.
    fn l1(last_update_time: DateTime<Utc>, price: Decimal) -> DataKind {
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time,
            best_bid: Some(Level::new(price, dec!(1))),
            best_ask: Some(Level::new(price, dec!(1))),
        })
    }

    /// A two-sided book carrying prices but **no sizes** — what a bulk export or an FX quote tape
    /// publishes. The volume-weighted mid is undefined; the plain mid is `(bid + ask) / 2`.
    fn l1_without_sizes(last_update_time: DateTime<Utc>, bid: Decimal, ask: Decimal) -> DataKind {
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time,
            best_bid: Some(Level::new(bid, Decimal::ZERO)),
            best_ask: Some(Level::new(ask, Decimal::ZERO)),
        })
    }

    #[test]
    fn price_is_none_without_any_input() {
        assert_eq!(DefaultInstrumentMarketData::default().price(), None);
    }

    /// The compatibility claim `#[serde(default)]` on `candle` is here for: engine state written by
    /// a build predating that field still loads, with the absent field taking its `Default`.
    ///
    /// Pinned because the attribute is easy to drop in a refactor and nothing else would notice.
    /// Serde treats a missing key as a hard error even when the field is an `Option`, and deriving
    /// `Default` does not change that — without the attribute this is `missing field 'candle'`,
    /// and a persisted snapshot, audit replica or replay stream from an earlier version fails to
    /// load outright.
    #[test]
    fn state_written_before_the_candle_field_existed_still_deserialises() {
        // Exactly the pre-field shape: `candle` absent, everything else as it was written.
        let pre_candle = r#"{
            "l1": {
                "last_update_time": "2024-01-01T00:00:00Z",
                "best_bid": null,
                "best_ask": null
            },
            "last_traded_price": null
        }"#;

        let data: DefaultInstrumentMarketData = serde_json::from_str(pre_candle).unwrap();

        assert_eq!(data.candle, None);
        // The fields that *were* present still round-trip rather than being defaulted wholesale.
        assert_eq!(data.l1.last_update_time, at(1_704_067_200));
    }

    /// The other half of that choice: the default is per field, so a snapshot missing a field that
    /// predates `candle` is an error rather than a silent substitution.
    ///
    /// A container-level `#[serde(default)]` would load this as an epoch-stamped empty book, which
    /// `price()` reports as "no price yet" — indistinguishable from an instrument that simply has
    /// not ticked, and it would mark nothing for the rest of the run.
    #[test]
    fn a_snapshot_missing_the_book_is_rejected_rather_than_defaulted_to_an_empty_one() {
        let error =
            serde_json::from_str::<DefaultInstrumentMarketData>(r#"{ "last_traded_price": null }"#)
                .unwrap_err();

        assert!(error.to_string().contains("l1"), "{error}");
    }

    #[test]
    fn newest_l1_beats_older_candle_and_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(30), l1(at(30), dec!(100))));

        assert_eq!(data.price(), Some(dec!(100)));
    }

    /// The failure this rule exists to prevent, applied to L1: on a mixed feed — a session of quote
    /// ticks, then a long run of bars, which one provider alone can produce — a book that stopped
    /// updating must not mark every position that follows it.
    #[test]
    fn stale_l1_does_not_shadow_fresh_candle() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(100), l1(at(100), dec!(100))));
        data.process(&event(at(86_500), candle(at(86_500), dec!(300))));

        assert_eq!(data.price(), Some(dec!(300)));
    }

    /// The deliberate counterpart to the candle case above, and the reason it is *not* symmetric.
    ///
    /// An L1 book and a trade are both tick-regime inputs, and their stamps routinely come from
    /// different clocks on one connector — Binance Spot's `bookTicker` publishes no venue instant,
    /// so its book carries a host stamp while its trades carry Binance's `T`. Ranking them by
    /// recency would decide the mark by that skew: a host 200 ms behind the venue flips essentially
    /// every mark from the book mid to the last trade, silently. L1 therefore wins outright,
    /// however old it is.
    ///
    /// The cost is stated in `price`'s "Known limitation": a frozen book shadows a live trade tape.
    /// That is a connector defect rather than a feed shape, which is the trade this rule makes.
    #[test]
    fn stale_l1_still_outranks_a_fresh_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(100), l1(at(100), dec!(100))));
        data.process(&event(at(86_500), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(100)));
    }

    /// The property the rule above exists for, stated directly: no timestamp a *trade* carries can
    /// change which source `price()` reads while a two-sided book is held. Sweeping the trade across
    /// both sides of the book's stamp — and onto it exactly — pins that the comparison is gone
    /// rather than merely currently favouring L1.
    #[test]
    fn no_trade_timestamp_can_displace_a_two_sided_book() {
        for trade_seconds in [1, 99, 100, 101, 86_400] {
            let mut data = DefaultInstrumentMarketData::default();
            data.process(&event(at(100), l1(at(100), dec!(100))));
            data.process(&event(at(trade_seconds), trade(dec!(200))));

            assert_eq!(
                data.price(),
                Some(dec!(100)),
                "a trade stamped at t={trade_seconds} must not displace the book mid"
            );
        }
    }

    /// At an exact tie the tick regime wins, which keeps an L1-only feed behaving exactly as it did
    /// before the other two inputs were considered. `close_time` is the exclusive period end, so a
    /// book stamped at that instant belongs to the next period and is the fresher observation.
    #[test]
    fn l1_wins_exact_tie_with_candle_and_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(20), trade(dec!(200))));
        data.process(&event(at(20), l1(at(20), dec!(100))));

        assert_eq!(data.price(), Some(dec!(100)));
    }

    /// A prices-only book must produce a price rather than a panic: `Decimal` division by the zero
    /// total size panics, and this book is what an export-driven backtest feeds in on every row.
    #[test]
    fn two_sided_l1_without_sizes_falls_back_to_plain_mid() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(
            at(10),
            l1_without_sizes(at(10), dec!(100), dec!(200)),
        ));

        assert_eq!(data.price(), Some(dec!(150)));
    }

    /// Half a book has no mid, so it contributes nothing and the other inputs stand.
    #[test]
    fn one_sided_l1_contributes_no_price() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));
        data.process(&event(
            at(20),
            DataKind::OrderBookL1(OrderBookL1 {
                last_update_time: at(20),
                best_bid: Some(Level::new(dec!(100), Decimal::ZERO)),
                best_ask: None,
            }),
        ));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    #[test]
    fn stale_l1_does_not_replace_newer_stored_l1() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), l1(at(30), dec!(300))));
        data.process(&event(at(20), l1(at(20), dec!(999))));

        assert_eq!(data.l1.last_update_time, at(30));
        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn newer_candle_beats_older_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));
        data.process(&event(at(20), candle(at(20), dec!(300))));

        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn newer_trade_beats_older_candle() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(30), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    /// A coarse bar must not shadow a fresher tick — the reason precedence is recency-based rather
    /// than a fixed "candle beats trade".
    #[test]
    fn stale_coarse_candle_does_not_shadow_fresh_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        // A daily bar that closed at t=100, then a tick a long while later.
        data.process(&event(at(100), candle(at(100), dec!(300))));
        data.process(&event(at(86_500), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    /// `close_time` is the exclusive period end, so a trade stamped on it belongs to the next
    /// period and is the fresher observation.
    #[test]
    fn trade_wins_exact_tie_with_candle_close_time() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(20), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    /// A producer stamping `time_exchange` with the bar's **open** — the timestamp most venues put
    /// in the bar record — offers the close of a period the engine's clock says is still running.
    /// Applying it would let a strategy trade the whole bar on its own outcome.
    #[test]
    fn candle_closing_after_its_own_time_exchange_is_dropped_as_lookahead() {
        let mut data = DefaultInstrumentMarketData::default();
        // Stamped at the bar open (t=20) but describing the period ending at t=80.
        data.process(&event(at(20), candle(at(80), dec!(300))));

        assert_eq!(data.candle, None, "the lookahead candle must not be stored");
        assert_eq!(data.price(), None);
    }

    /// The guard rejects only what is genuinely ahead. A live feed stamps `time_exchange` at
    /// receipt, which is strictly *after* the close it delivers, and that candle must still apply.
    #[test]
    fn candle_closing_before_its_time_exchange_is_retained() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(85), candle(at(80), dec!(300))));

        assert_eq!(data.candle.unwrap().close_time, at(80));
        assert_eq!(data.price(), Some(dec!(300)));
    }

    /// A dropped lookahead candle must not shadow the state that was already there — the drop is a
    /// no-op on everything else, not a reset.
    #[test]
    fn a_dropped_lookahead_candle_leaves_the_stored_candle_intact() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), candle(at(30), dec!(300))));
        data.process(&event(at(40), candle(at(90), dec!(999))));

        assert_eq!(data.candle.unwrap().close_time, at(30));
        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn stale_candle_does_not_replace_newer_stored_candle() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), candle(at(30), dec!(300))));
        data.process(&event(at(20), candle(at(20), dec!(999))));

        assert_eq!(data.candle.unwrap().close_time, at(30));
        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn stale_trade_does_not_replace_newer_stored_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), trade(dec!(300))));
        data.process(&event(at(20), trade(dec!(999))));

        assert_eq!(data.price(), Some(dec!(300)));
    }

    /// A liquidation is a forced fill at a potentially dislocated price; admitting it would corrupt
    /// `pnl_unrealised`.
    #[test]
    fn excluded_variants_do_not_move_price() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));

        data.process(&event(
            at(20),
            DataKind::Liquidation(Liquidation {
                side: Side::Buy,
                price: dec!(999),
                quantity: dec!(1),
                time: at(20),
            }),
        ));

        assert_eq!(data.price(), Some(dec!(200)));
        assert_eq!(data.candle, None);
    }
}
