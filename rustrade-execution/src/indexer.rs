use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    error::{
        ApiError, ClientError, KeyError, OrderError, UnindexedApiError, UnindexedClientError,
        UnindexedOrderError,
    },
    map::ExecutionInstrumentMap,
    order::{
        Order, OrderEvent, OrderKey, OrderSnapshot, UnindexedOrderKey, UnindexedOrderSnapshot,
        request::OrderResponseCancel,
        state::{InactiveOrderState, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade},
};
use derive_more::Constructor;
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::error::IndexError,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::{
    collection::snapshot::Snapshot,
    stream::ext::indexed::{IndexedStream, Indexer},
};
use std::sync::Arc;

pub type IndexedAccountStream<St> = IndexedStream<St, AccountEventIndexer>;

#[derive(Debug, Clone, Constructor)]
pub struct AccountEventIndexer {
    pub map: Arc<ExecutionInstrumentMap>,
}

impl Indexer for AccountEventIndexer {
    type Unindexed = UnindexedAccountEvent;
    type Indexed = AccountEvent;

    fn index(&self, item: Self::Unindexed) -> Result<Self::Indexed, IndexError> {
        self.account_event(item)
    }
}

impl AccountEventIndexer {
    pub fn account_event(&self, event: UnindexedAccountEvent) -> Result<AccountEvent, IndexError> {
        let UnindexedAccountEvent { exchange, kind } = event;

        let exchange = self.map.find_exchange_index(exchange)?;

        let kind = match kind {
            AccountEventKind::Snapshot(snapshot) => {
                AccountEventKind::Snapshot(self.snapshot(snapshot)?)
            }
            AccountEventKind::BalanceSnapshot(snapshot) => {
                AccountEventKind::BalanceSnapshot(self.asset_balance(snapshot.0).map(Snapshot)?)
            }
            AccountEventKind::OrderSnapshot(snapshot) => {
                AccountEventKind::OrderSnapshot(self.order_snapshot(snapshot.0).map(Snapshot)?)
            }
            AccountEventKind::OrderCancelled(response) => {
                AccountEventKind::OrderCancelled(self.order_response_cancel(response)?)
            }
            AccountEventKind::Trade(trade) => AccountEventKind::Trade(self.trade(trade)?),
            AccountEventKind::StreamError(msg) => AccountEventKind::StreamError(msg),
        };

        Ok(AccountEvent { exchange, kind })
    }

    pub fn snapshot(
        &self,
        snapshot: UnindexedAccountSnapshot,
    ) -> Result<AccountSnapshot, IndexError> {
        let UnindexedAccountSnapshot {
            exchange,
            balances,
            instruments,
        } = snapshot;

        let exchange = self.map.find_exchange_index(exchange)?;

        let balances = balances
            .into_iter()
            .map(|balance| self.asset_balance(balance))
            .collect::<Result<Vec<_>, _>>()?;

        let instruments = instruments
            .into_iter()
            .map(|snapshot| {
                let InstrumentAccountSnapshot {
                    instrument,
                    orders,
                    position,
                } = snapshot;

                let instrument = self.map.find_instrument_index(&instrument)?;

                let orders = orders
                    .into_iter()
                    .map(|order| self.order_snapshot(order))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(InstrumentAccountSnapshot {
                    instrument,
                    orders,
                    position,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(AccountSnapshot {
            exchange,
            balances,
            instruments,
        })
    }

    pub fn asset_balance(
        &self,
        balance: AssetBalance<AssetNameExchange>,
    ) -> Result<AssetBalance<AssetIndex>, IndexError> {
        let AssetBalance {
            asset,
            balance,
            time_exchange,
        } = balance;
        let asset = self.map.find_asset_index(&asset)?;

        Ok(AssetBalance {
            asset,
            balance,
            time_exchange,
        })
    }

    pub fn order_snapshot(
        &self,
        order: UnindexedOrderSnapshot,
    ) -> Result<OrderSnapshot, IndexError> {
        let Order {
            key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        } = order;

        let key = self.order_key(key)?;
        let state = self.order_state(state)?;

        Ok(Order {
            key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        })
    }

    pub fn order_response_cancel(
        &self,
        response: OrderResponseCancel<ExchangeId, AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<OrderResponseCancel, IndexError> {
        let OrderResponseCancel { key, state } = response;

        Ok(OrderResponseCancel {
            key: self.order_key(key)?,
            state: match state {
                Ok(cancelled) => Ok(cancelled),
                Err(error) => Err(self.order_error(error)?),
            },
        })
    }

    pub fn order_key(&self, key: UnindexedOrderKey) -> Result<OrderKey, IndexError> {
        let UnindexedOrderKey {
            exchange,
            instrument,
            strategy,
            cid,
        } = key;

        Ok(OrderKey {
            exchange: self.map.find_exchange_index(exchange)?,
            instrument: self.map.find_instrument_index(&instrument)?,
            strategy,
            cid,
        })
    }

    /// Index an [`UnindexedOrderState`] to an [`OrderState`].
    ///
    /// Used by `ExecutionManager` to index `open_order` responses.
    pub fn order_state(&self, state: UnindexedOrderState) -> Result<OrderState, IndexError> {
        Ok(match state {
            UnindexedOrderState::Active(active) => OrderState::Active(active),
            UnindexedOrderState::Inactive(inactive) => match inactive {
                InactiveOrderState::OpenFailed(failed) => match failed {
                    OrderError::Rejected(rejected) => {
                        OrderState::inactive(OrderError::Rejected(self.api_error(rejected)?))
                    }
                    OrderError::Connectivity(error) => {
                        OrderState::inactive(OrderError::Connectivity(error))
                    }
                },
                InactiveOrderState::Cancelled(cancelled) => OrderState::inactive(cancelled),
                InactiveOrderState::FullyFilled(filled) => OrderState::fully_filled(filled),
                InactiveOrderState::Expired(expired) => OrderState::expired(expired),
            },
        })
    }

    pub fn api_error(&self, error: UnindexedApiError) -> Result<ApiError, IndexError> {
        Ok(match error {
            UnindexedApiError::RateLimit => ApiError::RateLimit,
            UnindexedApiError::Unauthenticated(msg) => ApiError::Unauthenticated(msg),
            UnindexedApiError::AssetInvalid(asset, value) => {
                ApiError::AssetInvalid(self.map.find_asset_index(&asset)?, value)
            }
            UnindexedApiError::InstrumentInvalid(instrument, value) => {
                ApiError::InstrumentInvalid(self.map.find_instrument_index(&instrument)?, value)
            }
            UnindexedApiError::BalanceInsufficient(asset, value) => {
                ApiError::BalanceInsufficient(self.map.find_asset_index(&asset)?, value)
            }
            UnindexedApiError::OrderRejected(reason) => ApiError::OrderRejected(reason),
            UnindexedApiError::OrderAlreadyCancelled => ApiError::OrderAlreadyCancelled,
            UnindexedApiError::OrderAlreadyFullyFilled => ApiError::OrderAlreadyFullyFilled,
        })
    }

    pub fn order_request<Kind>(
        &self,
        order: &OrderEvent<Kind, ExchangeIndex, InstrumentIndex>,
    ) -> Result<OrderEvent<Kind, ExchangeId, &InstrumentNameExchange>, KeyError>
    where
        Kind: Clone,
    {
        let OrderEvent {
            key:
                OrderKey {
                    exchange,
                    instrument,
                    strategy,
                    cid,
                },
            state,
        } = order;

        let exchange = self.map.find_exchange_id(*exchange)?;
        let instrument = self.map.find_instrument_name_exchange(*instrument)?;

        Ok(OrderEvent {
            key: OrderKey {
                exchange,
                instrument,
                strategy: strategy.clone(),
                cid: cid.clone(),
            },
            state: state.clone(),
        })
    }

    pub fn order_error(&self, error: UnindexedOrderError) -> Result<OrderError, IndexError> {
        Ok(match error {
            UnindexedOrderError::Connectivity(error) => OrderError::Connectivity(error),
            UnindexedOrderError::Rejected(error) => OrderError::Rejected(self.api_error(error)?),
        })
    }

    pub fn client_error(&self, error: UnindexedClientError) -> Result<ClientError, IndexError> {
        Ok(match error {
            UnindexedClientError::Connectivity(error) => ClientError::Connectivity(error),
            UnindexedClientError::Api(error) => ClientError::Api(self.api_error(error)?),
            UnindexedClientError::TaskFailed(value) => ClientError::TaskFailed(value),
            UnindexedClientError::Internal(value) => ClientError::Internal(value),
            UnindexedClientError::Truncated { limit } => ClientError::Truncated { limit },
            UnindexedClientError::TruncatedSnapshot { limit } => {
                ClientError::TruncatedSnapshot { limit }
            }
        })
    }

    /// Index a trade, converting fee asset and computing `fees_quote`.
    ///
    /// Computes `fees_quote` based on fee asset relationship to instrument:
    /// - Fee in quote asset: `fees_quote = Some(fees)`
    /// - Fee in base asset: `fees_quote = Some(fees * price)`
    /// - Fee in third-party asset (e.g., BNB): `fees_quote = None`
    ///
    /// # Errors
    /// Returns `IndexError` if fee asset is not in the map. Some integrations use
    /// "UNKNOWN" as a placeholder when fee data is unavailable (e.g., IBKR `fetch_trades`,
    /// Binance when API omits `commission_asset`). These trades will fail indexing.
    pub fn trade(
        &self,
        trade: Trade<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<Trade<AssetIndex, InstrumentIndex>, IndexError> {
        let Trade {
            id,
            order_id,
            instrument,
            strategy,
            time_exchange,
            side,
            price: trade_price,
            quantity,
            fees,
        } = trade;

        let instrument_index = self.map.find_instrument_index(&instrument)?;
        let fee_asset_index = self.map.find_asset_index(&fees.asset)?;

        // Compute fees_quote based on fee asset relationship to instrument
        let fees_quote = self
            .map
            .instruments
            .get_index(instrument_index.index())
            .and_then(|instr| {
                if fee_asset_index == instr.underlying.quote {
                    // Fee is in quote asset — no conversion needed
                    Some(fees.fees)
                } else if fee_asset_index == instr.underlying.base {
                    // Fee is in base asset — convert using trade price
                    Some(fees.fees * trade_price)
                } else {
                    // Fee is in third-party asset (e.g., BNB) — needs external price
                    None
                }
            });

        Ok(Trade {
            id,
            order_id,
            instrument: instrument_index,
            strategy,
            time_exchange,
            side,
            price: trade_price,
            quantity,
            fees: AssetFees {
                asset: fee_asset_index,
                fees: fees.fees,
                fees_quote,
            },
        })
    }
}
