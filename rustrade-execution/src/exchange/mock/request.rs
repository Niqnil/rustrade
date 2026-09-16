use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Open, UnindexedOrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct MockExchangeRequest {
    pub time_request: DateTime<Utc>,
    pub kind: MockExchangeRequestKind,
}

impl MockExchangeRequest {
    pub fn new(time_request: DateTime<Utc>, kind: MockExchangeRequestKind) -> Self {
        Self { time_request, kind }
    }

    pub fn fetch_account_snapshot(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<UnindexedAccountSnapshot>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchAccountSnapshot { response_tx },
        )
    }

    pub fn fetch_balances(
        time_request: DateTime<Utc>,
        assets: Vec<AssetNameExchange>,
        response_tx: oneshot::Sender<Vec<AssetBalance<AssetNameExchange>>>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchBalances {
                response_tx,
                assets,
            },
        )
    }

    pub fn fetch_orders_open(
        time_request: DateTime<Utc>,
        instruments: Vec<InstrumentNameExchange>,
        response_tx: oneshot::Sender<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchOrdersOpen {
                response_tx,
                instruments,
            },
        )
    }

    pub fn fetch_trades(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>>,
        time_since: DateTime<Utc>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchTrades {
                response_tx,
                time_since,
            },
        )
    }

    pub fn cancel_order(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<UnindexedOrderResponseCancel>,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::CancelOrder {
                response_tx,
                request,
            },
        )
    }

    pub fn open_order(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<
            Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        >,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::OpenOrder {
                response_tx,
                request,
            },
        )
    }
}

#[derive(Debug)]
pub enum MockExchangeRequestKind {
    FetchAccountSnapshot {
        response_tx: oneshot::Sender<UnindexedAccountSnapshot>,
    },
    FetchBalances {
        assets: Vec<AssetNameExchange>,
        response_tx: oneshot::Sender<Vec<AssetBalance<AssetNameExchange>>>,
    },
    FetchOrdersOpen {
        instruments: Vec<InstrumentNameExchange>,
        response_tx: oneshot::Sender<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>>,
    },
    FetchTrades {
        response_tx: oneshot::Sender<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>>,
        time_since: DateTime<Utc>,
    },
    CancelOrder {
        response_tx: oneshot::Sender<UnindexedOrderResponseCancel>,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    },
    OpenOrder {
        response_tx:
            oneshot::Sender<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>,
        /// The order to open, carrying on
        /// [`RequestOpen::market`](crate::order::request::RequestOpen::market) the market its
        /// sender observed when it decided to open it.
        ///
        /// That snapshot is the venue's only price source for a Market order, which carries no
        /// limit price of its own. It is not repeated as a field of its own here: two copies of
        /// the same snapshot on one message could disagree, and nothing could arbitrate.
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    },
}
