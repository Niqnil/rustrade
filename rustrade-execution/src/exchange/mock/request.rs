use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    borrow_capacity::BorrowCapacity,
    error::{UnindexedApiError, UnindexedOrderError},
    exchange::mock::{CorporateActionResponse, ExternalChange},
    order::{
        Order, UnindexedInactiveOrder, UnindexedOrderKey,
        id::ClientOrderId,
        request::{
            OrderRequestAmend, OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseAmend,
            UnindexedOrderResponseCancel,
        },
        state::{Open, UnindexedOrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    corporate_action::{CorporateActionKind, SplitRoundingPolicy},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
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
        assets: Vec<AssetNameExchange>,
        response_tx: oneshot::Sender<UnindexedAccountSnapshot>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchAccountSnapshot {
                assets,
                response_tx,
            },
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

    pub fn fetch_borrow_capacity(
        time_request: DateTime<Utc>,
        instrument: InstrumentNameExchange,
        side: Side,
        response_tx: oneshot::Sender<Result<BorrowCapacity, UnindexedApiError>>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchBorrowCapacity {
                response_tx,
                instrument,
                side,
            },
        )
    }

    pub fn fetch_orders_ended(
        time_request: DateTime<Utc>,
        orders: Vec<UnindexedOrderKey>,
        response_tx: oneshot::Sender<Vec<UnindexedInactiveOrder>>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchOrdersEnded {
                orders,
                response_tx,
            },
        )
    }

    pub fn fetch_trades(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::FetchTrades {
                response_tx,
                start,
                end,
            },
        )
    }

    pub fn checkpoint(time_request: DateTime<Utc>, response_tx: oneshot::Sender<u64>) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::Checkpoint { response_tx },
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

    pub fn amend_order(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<UnindexedOrderResponseAmend>,
        request: OrderRequestAmend<ExchangeId, InstrumentNameExchange>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::AmendOrder {
                response_tx,
                request,
            },
        )
    }

    /// A request to change the order resting under `cid` as if from outside any client: see
    /// [`MockExchangeRequestKind::ModifyExternally`]. What the change reports goes to the account
    /// stream; `response_tx` hears only whether it was made.
    pub fn modify_externally(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<Result<(), UnindexedOrderError>>,
        cid: ClientOrderId,
        change: impl Into<ExternalChange>,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::ModifyExternally {
                response_tx,
                cid,
                change: change.into(),
            },
        )
    }

    /// A request to apply the corporate action `id` of `kind` to `instrument`, rounding as
    /// `policy` says: see [`MockExchangeRequestKind::CorporateAction`]. What it causes goes to the
    /// account stream; `response_tx` hears what the venue did with it.
    pub fn corporate_action(
        time_request: DateTime<Utc>,
        response_tx: oneshot::Sender<CorporateActionResponse>,
        id: SmolStr,
        instrument: InstrumentNameExchange,
        kind: CorporateActionKind,
        policy: SplitRoundingPolicy,
    ) -> Self {
        Self::new(
            time_request,
            MockExchangeRequestKind::CorporateAction {
                response_tx,
                id,
                instrument,
                kind,
                policy,
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

/// What a [`MockExchangeRequest`] asks the venue for.
///
/// Non-exhaustive: requests are built through [`MockExchangeRequest`]'s constructors and matched only
/// by the venue's own driver, so a new request kind is not a breaking change.
#[derive(Debug)]
#[non_exhaustive]
pub enum MockExchangeRequestKind {
    FetchAccountSnapshot {
        /// The assets whose balances to report; empty for every asset held.
        assets: Vec<AssetNameExchange>,
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
    FetchOrdersEnded {
        orders: Vec<UnindexedOrderKey>,
        response_tx: oneshot::Sender<Vec<UnindexedInactiveOrder>>,
    },
    /// How much more the account can borrow for an order on `instrument` on `side`.
    FetchBorrowCapacity {
        instrument: InstrumentNameExchange,
        side: Side,
        response_tx: oneshot::Sender<Result<BorrowCapacity, UnindexedApiError>>,
    },
    /// Every trade from `start` to `end`, both inclusive.
    FetchTrades {
        response_tx: oneshot::Sender<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    /// Send a [`VenueCheckpoint`](crate::exchange::mock::venue::VenueCheckpoint) on the account
    /// stream, after everything the venue produced before it, answering with its id once sent.
    Checkpoint { response_tx: oneshot::Sender<u64> },
    CancelOrder {
        response_tx: oneshot::Sender<UnindexedOrderResponseCancel>,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    },
    /// Amend a resting order in place: see
    /// [`SimulatedVenue::amend_order`](crate::exchange::mock::SimulatedVenue::amend_order).
    AmendOrder {
        response_tx: oneshot::Sender<UnindexedOrderResponseAmend>,
        request: OrderRequestAmend<ExchangeId, InstrumentNameExchange>,
    },
    /// A test hook: change the order resting under `cid` as `change` says, as if from outside
    /// any client, reporting only on the account stream, and answer whether it was made. See
    /// [`SimulatedVenue::modify_externally`](crate::exchange::mock::SimulatedVenue::modify_externally).
    ModifyExternally {
        response_tx: oneshot::Sender<Result<(), UnindexedOrderError>>,
        cid: ClientOrderId,
        change: ExternalChange,
    },
    /// Apply the corporate action `id` of `kind` to `instrument`, as the venue that lists it would,
    /// at the instant the driver books the request: see
    /// [`SimulatedVenue::apply_corporate_action`](crate::exchange::mock::SimulatedVenue::apply_corporate_action).
    /// So send it when the action takes effect. What it causes goes to the account stream, ahead
    /// of the answer.
    CorporateAction {
        response_tx: oneshot::Sender<CorporateActionResponse>,
        id: SmolStr,
        instrument: InstrumentNameExchange,
        kind: CorporateActionKind,
        /// How the broker rounds a share count the action leaves fractional.
        policy: SplitRoundingPolicy,
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
