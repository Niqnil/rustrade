use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    InstrumentBalanceUpdate, IsolatedInstrumentState, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, AssetBalanceUpdate},
    error::{
        AccountReinitFailure, AccountStreamInitError, ApiError, ClientError, KeyError, OrderError,
        UnindexedAccountStreamInitError, UnindexedApiError, UnindexedClientError,
        UnindexedOrderError,
    },
    fill_recovery::{FillRecoveryGap, FillRecoveryScope},
    map::ExecutionInstrumentMap,
    order::{
        Order, OrderEvent, OrderKey, OrderSnapshot, UnindexedOrderKey, UnindexedOrderSnapshot,
        request::OrderResponseCancel,
        state::{InactiveOrderState, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeAmendment, TradeAmendmentKind},
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
use tracing::warn;

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
            AccountEventKind::BalanceStreamUpdate(snapshot) => {
                AccountEventKind::BalanceStreamUpdate(
                    self.asset_balance_update(snapshot.0).map(Snapshot)?,
                )
            }
            AccountEventKind::InstrumentBalanceUpdate(update) => {
                AccountEventKind::InstrumentBalanceUpdate(self.instrument_balance_update(update)?)
            }
            AccountEventKind::OrderSnapshot(snapshot) => {
                AccountEventKind::OrderSnapshot(self.order_snapshot(snapshot.0).map(Snapshot)?)
            }
            AccountEventKind::OrderCancelled(response) => {
                AccountEventKind::OrderCancelled(self.order_response_cancel(response)?)
            }
            AccountEventKind::Trade(trade) => AccountEventKind::Trade(self.trade(trade)?),
            // Termination reason carries no exchange/asset/instrument keys — pass through verbatim.
            AccountEventKind::StreamTerminated(reason) => {
                AccountEventKind::StreamTerminated(reason)
            }
            AccountEventKind::FillRecoveryGaveUp(gap) => {
                AccountEventKind::FillRecoveryGaveUp(self.fill_recovery_gap(gap)?)
            }
            AccountEventKind::TradeAmended(amendment) => {
                AccountEventKind::TradeAmended(self.trade_amendment(amendment)?)
            }
            AccountEventKind::ReinitFailed(failure) => {
                AccountEventKind::ReinitFailed(self.reinit_failure(failure))
            }
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
                    orders_complete,
                    position,
                    isolated,
                } = snapshot;

                let instrument = self.map.find_instrument_index(&instrument)?;

                let orders = orders
                    .into_iter()
                    .map(|order| self.order_snapshot(order))
                    .collect::<Result<Vec<_>, _>>()?;

                // Per-pair isolated balances are generic over `AssetKey`, so (unlike `position`)
                // their base/quote asset names must be mapped to indices. A pair whose asset is
                // unregistered fails the snapshot index, matching top-level-balance behaviour.
                let isolated = isolated
                    .map(|state| self.isolated_instrument_state(state))
                    .transpose()?;

                Ok(InstrumentAccountSnapshot {
                    instrument,
                    orders,
                    orders_complete,
                    position,
                    isolated,
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

    pub fn asset_balance_update(
        &self,
        update: AssetBalanceUpdate<AssetNameExchange>,
    ) -> Result<AssetBalanceUpdate<AssetIndex>, IndexError> {
        let AssetBalanceUpdate {
            asset,
            update,
            time_exchange,
        } = update;
        let asset = self.map.find_asset_index(&asset)?;

        Ok(AssetBalanceUpdate {
            asset,
            update,
            time_exchange,
        })
    }

    /// Index the per-pair isolated state's `base`/`quote` asset names to indices.
    ///
    /// # Errors
    /// Returns `IndexError` if either the base or quote asset is not registered in the map —
    /// matching the fail-fast behaviour of top-level [`Self::asset_balance`].
    pub fn isolated_instrument_state(
        &self,
        state: IsolatedInstrumentState<AssetNameExchange>,
    ) -> Result<IsolatedInstrumentState<AssetIndex>, IndexError> {
        let IsolatedInstrumentState { base, quote, risk } = state;

        Ok(IsolatedInstrumentState {
            base: self.asset_balance(base)?,
            quote: self.asset_balance(quote)?,
            risk,
        })
    }

    /// Index a [`FillRecoveryGap`]'s instruments.
    ///
    /// # Errors
    /// Returns `IndexError` if any instrument is not registered in the map. The whole gap fails
    /// rather than dropping that instrument, which would understate what is missing. A venue names
    /// only the instruments its stream was opened with, so this fails only for a stream opened
    /// with an instrument the map does not hold.
    pub fn fill_recovery_gap(
        &self,
        gap: FillRecoveryGap<InstrumentNameExchange>,
    ) -> Result<FillRecoveryGap<InstrumentIndex>, IndexError> {
        let FillRecoveryGap {
            scope,
            start,
            end,
            attempts,
            reason,
        } = gap;

        let scope = match scope {
            FillRecoveryScope::Instruments(instruments) => FillRecoveryScope::Instruments(
                instruments
                    .iter()
                    .map(|instrument| self.map.find_instrument_index(instrument))
                    .collect::<Result<_, _>>()?,
            ),
            FillRecoveryScope::AllInstruments => FillRecoveryScope::AllInstruments,
        };

        Ok(FillRecoveryGap {
            scope,
            start,
            end,
            attempts,
            reason,
        })
    }

    /// Index an [`InstrumentBalanceUpdate`]'s instrument and `base`/`quote` asset keys.
    ///
    /// # Errors
    /// Returns `IndexError` if the instrument or either asset is not registered in the map.
    pub fn instrument_balance_update(
        &self,
        update: InstrumentBalanceUpdate<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<InstrumentBalanceUpdate, IndexError> {
        let InstrumentBalanceUpdate {
            instrument,
            base,
            quote,
        } = update;

        Ok(InstrumentBalanceUpdate {
            instrument: self.map.find_instrument_index(&instrument)?,
            base: self.asset_balance_update(base)?,
            quote: self.asset_balance_update(quote)?,
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
        let state = self.order_state(state);

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
                Err(error) => Err(self.order_error(error)),
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
    /// Used by `ExecutionManager` to index `open_order` responses. Never fails: an order state
    /// carries no key, and the error of a failed open is indexed by [`Self::order_error`].
    pub fn order_state(&self, state: UnindexedOrderState) -> OrderState {
        match state {
            UnindexedOrderState::Active(active) => OrderState::Active(active),
            UnindexedOrderState::Inactive(inactive) => match inactive {
                InactiveOrderState::OpenFailed(failed) => {
                    OrderState::inactive(self.order_error(failed))
                }
                InactiveOrderState::Cancelled(cancelled) => OrderState::inactive(cancelled),
                InactiveOrderState::FullyFilled(filled) => OrderState::fully_filled(filled),
                InactiveOrderState::Expired(expired) => OrderState::expired(expired),
            },
        }
    }

    /// Index an [`UnindexedApiError`] returned for an order request (open or cancel).
    ///
    /// Never fails. An error carries the venue's answer to a request, and the caller must
    /// receive that answer, so a name the [`ExecutionInstrumentMap`] does not hold degrades the
    /// error instead of discarding it (and the order response or client error carrying it). Each
    /// degrade logs a `warn!` naming what was dropped:
    /// - [`ApiError::BalanceInsufficient`] keeps its variant, with the asset `None`.
    /// - [`ApiError::AssetInvalid`] and [`ApiError::InstrumentInvalid`] have no index to carry,
    ///   so they become [`ApiError::OrderRejected`] holding their own message, which keeps the
    ///   name and the venue's text.
    ///
    /// For an error from any other request, use [`Self::client_error`], which degrades to
    /// [`ApiError::RequestRejected`] instead.
    pub fn api_error(&self, error: UnindexedApiError) -> ApiError {
        self.index_api_error(error, ApiError::OrderRejected)
    }

    /// Index an [`UnindexedApiError`], degrading a name the map does not hold as
    /// [`Self::api_error`] describes. `rejected` builds the rejection that an unresolvable
    /// [`ApiError::AssetInvalid`] or [`ApiError::InstrumentInvalid`] becomes, which depends on
    /// whether the request was an order.
    fn index_api_error(
        &self,
        error: UnindexedApiError,
        rejected: fn(String) -> ApiError,
    ) -> ApiError {
        match error {
            UnindexedApiError::RateLimit => ApiError::RateLimit,
            UnindexedApiError::Unauthenticated(msg) => ApiError::Unauthenticated(msg),
            UnindexedApiError::AssetInvalid(asset, value) => {
                match self.map.find_asset_index(&asset) {
                    Ok(asset) => ApiError::AssetInvalid(asset, value),
                    Err(_) => self.degrade_unresolvable(
                        UnindexedApiError::AssetInvalid(asset, value),
                        rejected,
                    ),
                }
            }
            UnindexedApiError::InstrumentInvalid(instrument, value) => {
                match self.map.find_instrument_index(&instrument) {
                    Ok(instrument) => ApiError::InstrumentInvalid(instrument, value),
                    Err(_) => self.degrade_unresolvable(
                        UnindexedApiError::InstrumentInvalid(instrument, value),
                        rejected,
                    ),
                }
            }
            UnindexedApiError::BalanceInsufficient(asset, value) => {
                let asset = asset.and_then(|asset| {
                    self.map
                        .find_asset_index(&asset)
                        .inspect_err(|_| {
                            warn!(
                                exchange = %self.map.exchange.value,
                                %asset,
                                message = %value,
                                "AccountEventIndexer dropping the asset of a BalanceInsufficient \
                                 error: the instrument map does not hold it"
                            )
                        })
                        .ok()
                });
                ApiError::BalanceInsufficient(asset, value)
            }
            UnindexedApiError::OrderRejected(reason) => ApiError::OrderRejected(reason),
            UnindexedApiError::OrderAlreadyCancelled => ApiError::OrderAlreadyCancelled,
            UnindexedApiError::OrderAlreadyFullyFilled => ApiError::OrderAlreadyFullyFilled,
            UnindexedApiError::OrderAlreadyExpired => ApiError::OrderAlreadyExpired,
            UnindexedApiError::DuplicateClientOrderId(message) => {
                ApiError::DuplicateClientOrderId(message)
            }
            UnindexedApiError::RequestRejected(reason) => ApiError::RequestRejected(reason),
        }
    }

    /// Degrade an error naming an asset or instrument the map does not hold to the rejection
    /// `rejected` builds, carrying the error's own message so neither the name nor the venue's
    /// text is lost.
    fn degrade_unresolvable(
        &self,
        error: UnindexedApiError,
        rejected: fn(String) -> ApiError,
    ) -> ApiError {
        let error = error.to_string();
        warn!(
            exchange = %self.map.exchange.value,
            %error,
            "AccountEventIndexer degrading an API error to a rejection: the instrument map does \
             not hold the asset or instrument it names"
        );
        rejected(error)
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

    /// Index an [`UnindexedOrderError`]. Never fails: see [`Self::api_error`].
    pub fn order_error(&self, error: UnindexedOrderError) -> OrderError {
        match error {
            UnindexedOrderError::Connectivity(error) => OrderError::Connectivity(error),
            UnindexedOrderError::Rejected(error) => OrderError::Rejected(self.api_error(error)),
            UnindexedOrderError::UnsupportedOrderType(msg) => OrderError::UnsupportedOrderType(msg),
            UnindexedOrderError::InvalidPrecision(violation) => {
                OrderError::InvalidPrecision(violation)
            }
        }
    }

    /// Index an [`UnindexedClientError`]. Never fails: like [`Self::api_error`], but an
    /// unresolvable [`ApiError::AssetInvalid`] or [`ApiError::InstrumentInvalid`] becomes
    /// [`ApiError::RequestRejected`], since the request need not have been an order.
    pub fn client_error(&self, error: UnindexedClientError) -> ClientError {
        match error {
            UnindexedClientError::Connectivity(error) => ClientError::Connectivity(error),
            UnindexedClientError::Api(error) => {
                ClientError::Api(self.index_api_error(error, ApiError::RequestRejected))
            }
            UnindexedClientError::TaskFailed(value) => ClientError::TaskFailed(value),
            UnindexedClientError::Internal(value) => ClientError::Internal(value),
            UnindexedClientError::Truncated { fills_read } => ClientError::Truncated { fills_read },
            UnindexedClientError::TruncatedSnapshot { limit } => {
                ClientError::TruncatedSnapshot { limit }
            }
        }
    }

    /// Index an [`UnindexedAccountStreamInitError`]. Never fails, as [`Self::client_error`].
    pub fn account_stream_init_error(
        &self,
        error: UnindexedAccountStreamInitError,
    ) -> AccountStreamInitError {
        match error {
            AccountStreamInitError::Client(error) => {
                AccountStreamInitError::Client(self.client_error(error))
            }
            AccountStreamInitError::Index(error) => AccountStreamInitError::Index(error),
        }
    }

    /// Index an [`AccountReinitFailure`]. Never fails, as [`Self::client_error`].
    pub fn reinit_failure(
        &self,
        failure: AccountReinitFailure<AssetNameExchange, InstrumentNameExchange>,
    ) -> AccountReinitFailure {
        let AccountReinitFailure { attempt, error } = failure;
        AccountReinitFailure {
            attempt,
            error: self.account_stream_init_error(error),
        }
    }

    /// Index a [`TradeAmendment`]. A replacement trade is indexed as [`trade`](Self::trade) indexes
    /// one, so an amendment fails where its trade would.
    pub fn trade_amendment(
        &self,
        amendment: TradeAmendment<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<TradeAmendment<AssetIndex, InstrumentIndex>, IndexError> {
        let TradeAmendment {
            instrument,
            order_id,
            time_exchange,
            original,
            kind,
        } = amendment;

        let instrument = self.map.find_instrument_index(&instrument)?;
        let kind = match kind {
            TradeAmendmentKind::Busted { quantity } => TradeAmendmentKind::Busted { quantity },
            TradeAmendmentKind::Corrected { replacement } => TradeAmendmentKind::Corrected {
                replacement: self.trade(replacement)?,
            },
            TradeAmendmentKind::CorrectedUnresolved {
                id,
                price,
                quantity,
            } => TradeAmendmentKind::CorrectedUnresolved {
                id,
                price,
                quantity,
            },
        };

        Ok(TradeAmendment {
            instrument,
            order_id,
            time_exchange,
            original,
            kind,
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
    /// "UNKNOWN" as a placeholder when fee data is unavailable (e.g., IBKR `fetch_trades` for an
    /// execution IB sent no commission report for, Binance when API omits `commission_asset`).
    /// These trades will fail indexing.
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
            order_filled_quantity,
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
            order_filled_quantity,
            fees: AssetFees {
                asset: fee_asset_index,
                fees: fees.fees,
                fees_quote,
            },
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::StreamTerminationReason, fill_recovery::FillRecoveryFailure,
        map::generate_execution_instrument_map,
    };
    use chrono::{DateTime, Utc};
    use rustrade_instrument::{index::IndexedInstruments, test_utils};

    fn binance_indexer() -> AccountEventIndexer {
        let instruments = IndexedInstruments::new(vec![test_utils::instrument(
            ExchangeId::BinanceSpot,
            "BTC",
            "USDT",
        )]);
        let map = generate_execution_instrument_map(&instruments, ExchangeId::BinanceSpot).unwrap();
        AccountEventIndexer::new(Arc::new(map))
    }

    /// `StreamTerminated` carries no asset/instrument keys, so it must index by mapping only the
    /// exchange and passing the reason through verbatim.
    #[test]
    fn account_event_passes_stream_terminated_through_verbatim() {
        let indexer = binance_indexer();
        let reason = StreamTerminationReason::ReconnectBudgetExhausted {
            attempts: 3,
            last_error: "socket reset".to_string(),
        };

        let indexed = indexer
            .account_event(UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::StreamTerminated(reason.clone()),
            ))
            .unwrap();

        assert_eq!(indexed.exchange, indexer.map.exchange.key);
        assert!(
            matches!(indexed.kind, AccountEventKind::StreamTerminated(r) if r == reason),
            "expected StreamTerminated to pass through unchanged",
        );
    }

    /// A failed re-init never fails to index: its client error is indexed as
    /// [`AccountEventIndexer::client_error`] indexes one, and an index error passes through.
    #[test]
    fn account_event_indexes_a_reinit_failure() {
        let indexer = binance_indexer();
        let btc = indexer
            .map
            .find_asset_index(&AssetNameExchange::new("BTC"))
            .unwrap();
        let unindexed_client = AccountStreamInitError::Client(ClientError::Api(
            ApiError::BalanceInsufficient(Some(AssetNameExchange::new("BTC")), "low".to_owned()),
        ));
        let indexed_client = AccountStreamInitError::Client(ClientError::Api(
            ApiError::BalanceInsufficient(Some(btc), "low".to_owned()),
        ));
        let index_error = IndexError::AssetIndex("ETH".to_owned());

        for (error, expected) in [
            (unindexed_client, indexed_client),
            (
                AccountStreamInitError::Index(index_error.clone()),
                AccountStreamInitError::Index(index_error),
            ),
        ] {
            let indexed = indexer
                .account_event(UnindexedAccountEvent::new(
                    ExchangeId::BinanceSpot,
                    AccountEventKind::ReinitFailed(AccountReinitFailure::new(2, error)),
                ))
                .unwrap();

            assert_eq!(indexed.exchange, indexer.map.exchange.key);
            assert_eq!(
                indexed.kind,
                AccountEventKind::ReinitFailed(AccountReinitFailure::new(2, expected))
            );
        }
    }

    /// A fill recovery give-up indexes each instrument it names, and fails as a whole when one
    /// is not in the map rather than understating what is missing. `AllInstruments` names none.
    #[test]
    fn account_event_indexes_a_fill_recovery_give_up_by_its_instruments() {
        let indexer = binance_indexer();
        let start = DateTime::<Utc>::MIN_UTC;
        let end = DateTime::<Utc>::MAX_UTC;
        let give_up = |scope| {
            UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::FillRecoveryGaveUp(FillRecoveryGap::new(
                    scope,
                    start,
                    end,
                    6,
                    FillRecoveryFailure::TimedOut { timeout_secs: 30 },
                )),
            )
        };
        let index_of = |scope| match indexer.account_event(give_up(scope)) {
            Ok(AccountEvent {
                kind: AccountEventKind::FillRecoveryGaveUp(gap),
                ..
            }) => Ok(gap),
            Ok(other) => panic!("expected FillRecoveryGaveUp, got {other:?}"),
            Err(error) => Err(error),
        };
        let Ok(btc) = indexer
            .map
            .find_instrument_index(&InstrumentNameExchange::new("BTC_USDT"))
        else {
            panic!("BTC_USDT is mapped");
        };

        assert_eq!(
            index_of(FillRecoveryScope::Instruments(vec![
                InstrumentNameExchange::new("BTC_USDT")
            ])),
            Ok(FillRecoveryGap::new(
                FillRecoveryScope::Instruments(vec![btc]),
                start,
                end,
                6,
                FillRecoveryFailure::TimedOut { timeout_secs: 30 },
            ))
        );
        assert_eq!(
            index_of(FillRecoveryScope::AllInstruments).map(|gap| gap.scope),
            Ok(FillRecoveryScope::AllInstruments)
        );
        assert!(
            index_of(FillRecoveryScope::Instruments(vec![
                InstrumentNameExchange::new("BTC_USDT"),
                InstrumentNameExchange::new("ETHUSDT"),
            ]))
            .is_err(),
            "an unmapped instrument fails the whole give-up"
        );
    }

    /// A trade amendment indexes its instrument, and a corrected one its replacement as a trade
    /// is indexed. An unmapped instrument fails it.
    #[test]
    fn account_event_indexes_a_trade_amendment() {
        use crate::{
            order::id::{OrderId, StrategyId},
            trade::TradeId,
        };
        use rust_decimal::Decimal;
        use rustrade_instrument::Side;

        let indexer = binance_indexer();
        let time = DateTime::<Utc>::MIN_UTC;
        let amended = |instrument: &str, kind| {
            UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::TradeAmended(TradeAmendment::new(
                    InstrumentNameExchange::new(instrument),
                    OrderId::new("ord-1"),
                    time,
                    Some(TradeId::new("t-1")),
                    kind,
                )),
            )
        };
        let index_of = |event| match indexer.account_event(event) {
            Ok(AccountEvent {
                kind: AccountEventKind::TradeAmended(amendment),
                ..
            }) => Ok(amendment),
            Ok(other) => panic!("expected TradeAmended, got {other:?}"),
            Err(error) => Err(error),
        };
        let Ok(btc) = indexer
            .map
            .find_instrument_index(&InstrumentNameExchange::new("BTC_USDT"))
        else {
            panic!("BTC_USDT is mapped");
        };

        assert_eq!(
            index_of(amended(
                "BTC_USDT",
                TradeAmendmentKind::Busted {
                    quantity: Some(Decimal::ONE)
                }
            )),
            Ok(TradeAmendment::new(
                btc,
                OrderId::new("ord-1"),
                time,
                Some(TradeId::new("t-1")),
                TradeAmendmentKind::Busted {
                    quantity: Some(Decimal::ONE)
                },
            ))
        );

        let replacement = Trade::new(
            TradeId::new("t-2"),
            OrderId::new("ord-1"),
            InstrumentNameExchange::new("BTC_USDT"),
            StrategyId::unknown(),
            time,
            Side::Buy,
            Decimal::TEN,
            Decimal::ONE,
            None,
            AssetFees::new(AssetNameExchange::new("USDT"), Decimal::ONE, None),
        );
        let Ok(TradeAmendment {
            kind: TradeAmendmentKind::Corrected { replacement },
            ..
        }) = index_of(amended(
            "BTC_USDT",
            TradeAmendmentKind::Corrected { replacement },
        ))
        else {
            panic!("a correction indexes to a correction");
        };
        assert_eq!(replacement.instrument, btc);
        assert_eq!(
            replacement.fees.fees_quote,
            Some(Decimal::ONE),
            "indexed as a trade is, fees and all"
        );

        assert!(
            index_of(amended(
                "ETHUSDT",
                TradeAmendmentKind::Busted { quantity: None }
            ))
            .is_err(),
            "an unmapped instrument fails the amendment"
        );
    }

    /// Run `index`, returning its value and the number of `WARN` events it emitted on this
    /// thread. A degrade is reported only by its log line, so asserting on the value alone could
    /// not tell a logged degrade from a silent one.
    fn count_warnings<T>(index: impl FnOnce() -> T) -> (T, usize) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default)]
        struct CountWarnings(Arc<AtomicUsize>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountWarnings {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() == tracing::Level::WARN {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let layer = CountWarnings::default();
        let count = Arc::clone(&layer.0);
        // Thread-local, so other tests running in parallel are not counted.
        let value =
            tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), index);
        (value, count.load(Ordering::Relaxed))
    }

    fn btc_usdt() -> InstrumentNameExchange {
        binance_indexer()
            .map
            .exchange_instruments()
            .next()
            .cloned()
            .unwrap_or_else(|| panic!("the map holds one instrument"))
    }

    #[test]
    fn balance_insufficient_keeps_a_held_asset_and_drops_an_unheld_one() {
        let indexer = binance_indexer();
        let btc = indexer
            .map
            .find_asset_index(&AssetNameExchange::new("BTC"))
            .unwrap();

        for (asset, expected, warnings) in [
            (Some("BTC"), Some(btc), 0),
            // An instrument name, as Binance's error mapping once put there.
            (Some("ETH_USDT"), None, 1),
            (None, None, 0),
        ] {
            let (indexed, warned) = count_warnings(|| {
                indexer.api_error(ApiError::BalanceInsufficient(
                    asset.map(AssetNameExchange::new),
                    "low".to_string(),
                ))
            });
            assert_eq!(
                indexed,
                ApiError::BalanceInsufficient(expected, "low".to_string()),
                "{asset:?}"
            );
            assert_eq!(warned, warnings, "{asset:?}: a dropped asset is logged");
        }
    }

    /// An invalid asset or instrument the map does not hold has no index to carry, so it becomes
    /// a rejection keeping its own message: `OrderRejected` for an order, `RequestRejected` for
    /// any other request.
    #[test]
    fn an_unresolvable_invalid_name_degrades_to_a_rejection_keeping_its_text() {
        let indexer = binance_indexer();
        for (error, text) in [
            (
                ApiError::AssetInvalid(AssetNameExchange::new("XYZ"), "bad".to_string()),
                "asset XYZ invalid: bad",
            ),
            (
                ApiError::InstrumentInvalid(
                    InstrumentNameExchange::new("XYZUSDT"),
                    "bad".to_string(),
                ),
                "instrument XYZUSDT invalid: bad",
            ),
        ] {
            let (indexed, warned) = count_warnings(|| indexer.api_error(error.clone()));
            assert_eq!(indexed, ApiError::OrderRejected(text.to_string()));
            assert_eq!(warned, 1, "{text}: a degrade is logged");

            let (indexed, warned) =
                count_warnings(|| indexer.client_error(ClientError::Api(error)));
            assert_eq!(
                indexed,
                ClientError::Api(ApiError::RequestRejected(text.to_string()))
            );
            assert_eq!(warned, 1, "{text}: a degrade is logged");
        }

        // A name the map holds still indexes, and nothing is logged.
        let (indexed, warned) = count_warnings(|| {
            indexer.api_error(ApiError::InstrumentInvalid(btc_usdt(), "bad".to_string()))
        });
        assert!(matches!(indexed, ApiError::InstrumentInvalid(_, _)));
        assert_eq!(warned, 0);
    }

    /// A response whose rejection names something the map does not hold still indexes, so the
    /// order is settled rather than left in flight.
    #[test]
    fn a_response_whose_rejection_names_an_unresolvable_asset_still_indexes() {
        let indexer = binance_indexer();
        let rejection = || {
            OrderError::Rejected(ApiError::BalanceInsufficient(
                Some(AssetNameExchange::new("ETH_USDT")),
                "low".to_string(),
            ))
        };
        let indexed = OrderError::Rejected(ApiError::BalanceInsufficient(None, "low".to_string()));

        assert_eq!(
            indexer.order_state(OrderState::inactive(rejection())),
            OrderState::inactive(indexed.clone())
        );

        let key = OrderKey {
            exchange: ExchangeId::BinanceSpot,
            instrument: btc_usdt(),
            strategy: crate::order::id::StrategyId::new("test"),
            cid: crate::order::id::ClientOrderId::random(),
        };
        let cancel = indexer
            .order_response_cancel(OrderResponseCancel {
                key,
                state: Err(rejection()),
            })
            .unwrap();
        assert_eq!(cancel.state, Err(indexed));
    }
}
