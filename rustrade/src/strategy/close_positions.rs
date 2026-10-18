use crate::engine::state::{
    EngineState,
    instrument::{InstrumentState, data::InstrumentDataState, filter::InstrumentFilter},
    position::Position,
};
use rust_decimal::Decimal;
use rustrade_execution::order::{
    OrderKey, OrderKind, TimeInForce,
    id::{ClientOrderId, PositionId, StrategyId},
    request::{OrderRequestCancel, OrderRequestOpen, RequestOpen},
};
use rustrade_instrument::{
    Side, asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};
use tracing::warn;

/// Strategy interface for generating open and cancel order requests that close open positions.
///
/// This allows full customisation of how a strategy will close a position.
///
/// Different strategies may:
/// - Use different order types (Market, Limit, etc.).
/// - Prioritise certain exchanges.
/// - Increase the position of an inversely correlated instrument in order to neutralise exposure.
/// - etc.
///
/// # Type Parameters
/// * `ExchangeKey` - Type used to identify an exchange (defaults to [`ExchangeIndex`]).
/// * `AssetKey` - Type used to identify an asset (defaults to [`AssetIndex`]).
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait ClosePositionsStrategy<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
>
{
    /// State used by the `ClosePositionsStrategy` to determine what open and cancel requests
    /// to generate.
    ///
    /// For Barter ecosystem strategies, this is the full `EngineState` of the trading system.
    ///
    /// eg/ `EngineState<DefaultGlobalData, DefaultInstrumentMarketData>`
    type State;

    /// Generate orders based on current system `State`.
    fn close_positions_requests<'a>(
        &'a self,
        state: &'a Self::State,
        filter: &'a InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeKey, InstrumentKey>> + 'a,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeKey, InstrumentKey>> + 'a,
    )
    where
        ExchangeKey: 'a,
        AssetKey: 'a,
        InstrumentKey: 'a;
}

/// Naive `ClosePositionsStrategy` logic for closing open positions with market orders only.
///
/// This function finds all open positions and generates equal but opposite `Side` market orders
/// that will neutralise the position, each rounded toward zero to its instrument's
/// [`quantity_step`](rustrade_instrument::instrument::Instrument::quantity_step) (see
/// [`build_ioc_market_order_to_close_position`]). A position smaller than one step gets no order:
/// it is logged at WARN and stays open, as the venue would not fill it.
pub fn close_open_positions_with_market_orders<'a, GlobalData, InstrumentData>(
    strategy_id: &'a StrategyId,
    state: &'a EngineState<GlobalData, InstrumentData>,
    filter: &'a InstrumentFilter,
    gen_cid: impl Fn(&InstrumentState<InstrumentData>, &PositionId) -> ClientOrderId + Copy + 'a,
) -> (
    impl IntoIterator<Item = OrderRequestCancel<ExchangeIndex, InstrumentIndex>> + 'a,
    impl IntoIterator<Item = OrderRequestOpen<ExchangeIndex, InstrumentIndex>> + 'a,
)
where
    InstrumentData: InstrumentDataState,
    ExchangeIndex: 'a,
    AssetIndex: 'a,
    InstrumentIndex: 'a,
{
    // In Hedging mode, gen_cid receives both the InstrumentState and the PositionId so
    // callers can derive a unique ClientOrderId per position (e.g., by incorporating
    // pos_id.0 into the CID). Using the same CID for multiple positions in Hedging mode
    // causes routing collisions and undefined exchange behavior.
    let open_requests = state
        .instruments
        .instruments(filter)
        // Require a live price feed before generating a closing market order: an instrument
        // with no recent price is treated as stale, and the price itself is intentionally not
        // forwarded — market orders fill at exchange best price, not a caller-supplied limit.
        .filter(|state| state.data.price().is_some())
        .flat_map(move |state| {
            // Generate one closing order per open position.
            // In Netting mode there is at most one position; in Hedging mode there may be N.
            // Each order carries the PositionId so hedging-mode fills route to the right slot.
            state
                .position
                .positions
                .iter()
                .filter_map(move |(pos_id, position)| {
                    let step = state.instrument.quantity_step();
                    let Some(mut req) = build_ioc_market_order_to_close_position(
                        state.instrument.exchange,
                        position,
                        step,
                        strategy_id.clone(),
                        || gen_cid(state, pos_id),
                    ) else {
                        warn!(
                            instrument = %state.instrument.name_internal,
                            position_id = %pos_id,
                            quantity = %position.quantity_abs,
                            step = ?step,
                            "position is smaller than one quantity step, so no order can close \
                             it; it stays open"
                        );
                        return None;
                    };
                    req.state.position_id = Some(pos_id.clone());
                    Some(req)
                })
        });

    (std::iter::empty(), open_requests)
}

/// Build an equal but opposite `Side` `ImmediateOrCancel` `Market` order that neutralises the
/// provided [`Position`], its quantity rounded toward zero to `quantity_step`.
///
/// For example, if [`Position`] is LONG by 100, build a market order request to sell 100.
///
/// A venue fills only whole multiples of its quantity step, so pass the instrument's
/// [`quantity_step`](rustrade_instrument::instrument::Instrument::quantity_step). `None` sends the
/// position's exact quantity, which a venue may reject or round when it is off the step.
///
/// Returns `None`, without calling `gen_cid`, when the rounded quantity is zero: a position
/// smaller than one step, which no order can close. The caller decides how to report it.
pub fn build_ioc_market_order_to_close_position<ExchangeKey, AssetKey, InstrumentKey>(
    exchange: ExchangeKey,
    position: &Position<AssetKey, InstrumentKey>,
    quantity_step: Option<Decimal>,
    strategy_id: StrategyId,
    gen_cid: impl Fn() -> ClientOrderId,
) -> Option<OrderRequestOpen<ExchangeKey, InstrumentKey>>
where
    ExchangeKey: Clone,
    InstrumentKey: Clone,
{
    let quantity = match quantity_step {
        Some(step) => position.quantity_abs - position.quantity_abs % step,
        None => position.quantity_abs,
    };
    if quantity.is_zero() {
        return None;
    }

    Some(OrderRequestOpen {
        key: OrderKey {
            exchange: exchange.clone(),
            instrument: position.instrument.clone(),
            strategy: strategy_id,
            cid: gen_cid(),
        },
        state: RequestOpen {
            side: match position.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            },
            price: None,
            quantity,
            kind: OrderKind::Market,
            time_in_force: TimeInForce::ImmediateOrCancel,
            position_id: None, // caller sets this when hedging-mode routing is required
            reduce_only: true, // closing existing position
            market: None,
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        order::id::OrderId,
        trade::{AssetFees, Trade, TradeId},
    };

    /// A long of `quantity`, as opened by one buy.
    fn long(quantity: Decimal) -> Position<AssetIndex, InstrumentIndex> {
        Position::from(&Trade::new(
            TradeId::new("trade"),
            OrderId::new("order"),
            InstrumentIndex(0),
            StrategyId::new("strategy"),
            DateTime::<Utc>::MIN_UTC,
            Side::Buy,
            dec!(100),
            quantity,
            None,
            AssetFees::new(AssetIndex(0), Decimal::ZERO, Some(Decimal::ZERO)),
        ))
    }

    fn close(
        position: &Position<AssetIndex, InstrumentIndex>,
        quantity_step: Option<Decimal>,
    ) -> Option<OrderRequestOpen<ExchangeIndex, InstrumentIndex>> {
        build_ioc_market_order_to_close_position(
            ExchangeIndex(0),
            position,
            quantity_step,
            StrategyId::new("strategy"),
            || ClientOrderId::new("cid"),
        )
    }

    #[test]
    fn a_close_is_rounded_toward_zero_to_the_step() {
        let request = close(&long(dec!(0.999)), Some(dec!(0.01))).unwrap();

        assert_eq!(request.state.side, Side::Sell);
        assert_eq!(request.state.quantity, dec!(0.99));
        assert!(request.state.reduce_only);
    }

    #[test]
    fn a_close_without_a_step_sends_the_exact_quantity() {
        let request = close(&long(dec!(0.999)), None).unwrap();

        assert_eq!(request.state.quantity, dec!(0.999));
    }

    /// The naive strategy rounds each close to its instrument's step, and sends none for a
    /// position smaller than one step.
    #[test]
    fn closing_all_positions_rounds_each_to_the_step_and_skips_one_below_it() {
        use crate::{
            Timed,
            engine::state::{
                instrument::data::DefaultInstrumentMarketData, position::PositionManager,
            },
        };
        use rustrade_instrument::{
            index::IndexedInstruments,
            instrument::spec::{
                InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice,
                InstrumentSpecQuantity, OrderQuantityUnits,
            },
            test_utils::instrument,
        };

        let instruments = IndexedInstruments::new([
            instrument(
                rustrade_instrument::exchange::ExchangeId::BinanceSpot,
                "btc",
                "usdt",
            ),
            instrument(
                rustrade_instrument::exchange::ExchangeId::BinanceSpot,
                "eth",
                "usdt",
            ),
        ]);
        let mut state: EngineState<(), DefaultInstrumentMarketData> =
            EngineState::builder(&instruments, (), |_| DefaultInstrumentMarketData {
                last_traded_price: Some(Timed::new(dec!(100), DateTime::<Utc>::MIN_UTC)),
                ..Default::default()
            })
            .build();
        for (held, instrument_state) in [dec!(0.999), dec!(0.009)]
            .into_iter()
            .zip(state.instruments.0.values_mut())
        {
            let base = instrument_state.instrument.underlying.base;
            instrument_state.instrument.spec = Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
                InstrumentSpecQuantity::new(
                    OrderQuantityUnits::Asset(base),
                    dec!(0.01),
                    dec!(0.01),
                ),
                InstrumentSpecNotional::new(Decimal::ZERO),
            ));
            let mut position = long(held);
            position.instrument = instrument_state.key;
            let mut manager = PositionManager::default();
            manager.positions.insert(PositionId::NETTING, position);
            instrument_state.position = manager;
        }

        let strategy = StrategyId::new("strategy");
        let (_, opens) = close_open_positions_with_market_orders(
            &strategy,
            &state,
            &InstrumentFilter::None,
            |_, _| ClientOrderId::new("cid"),
        );
        let opens: Vec<_> = opens.into_iter().collect();

        assert_eq!(opens.len(), 1);
        assert_eq!(opens[0].state.quantity, dec!(0.99));
        assert_eq!(opens[0].state.position_id, Some(PositionId::NETTING));
    }

    #[test]
    fn a_position_smaller_than_one_step_gets_no_close_and_no_client_order_id() {
        let request = build_ioc_market_order_to_close_position(
            ExchangeIndex(0),
            &long(dec!(0.009)),
            Some(dec!(0.01)),
            StrategyId::new("strategy"),
            || panic!("no order is built, so no id is drawn"),
        );

        assert!(request.is_none());
    }
}
