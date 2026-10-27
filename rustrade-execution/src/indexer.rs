use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    InstrumentBalanceUpdate, IsolatedInstrumentState, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, AssetBalanceUpdate},
    cash_flow::CashFlow,
    error::{
        AccountReinitFailure, AccountStreamInitError, ApiError, ClientError, KeyError, OrderError,
        UnindexedAccountStreamInitError, UnindexedApiError, UnindexedClientError,
        UnindexedOrderError,
    },
    fill_recovery::{FillRecoveryGap, FillRecoveryScope},
    map::ExecutionInstrumentMap,
    margin_debt::MarginDebtUpdate,
    notice::AccountNotice,
    order::{
        Order, OrderEvent, OrderKey, OrderSnapshot, UnindexedOrderKey, UnindexedOrderSnapshot,
        request::OrderResponseCancel,
        state::{InactiveOrderState, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeAmendment, TradeAmendmentKind},
};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::error::IndexError,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::collection::snapshot::Snapshot;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone, Constructor)]
pub struct AccountEventIndexer {
    pub map: Arc<ExecutionInstrumentMap>,
}

/// Why [`AccountEventIndexer::index_account_event`] did not index an event.
///
/// [`AccountEventIndexer::account_event`] folds both causes into one [`IndexError`]; this tells
/// them apart for a caller that treats them differently.
#[derive(Debug, Clone, PartialEq)]
pub enum AccountIndexError {
    /// A balance snapshot or update named an asset the [`ExecutionInstrumentMap`] does not
    /// register.
    ///
    /// Expected rather than a fault: a venue such as Binance pushes balances for every asset on
    /// the account, including ones the caller never asked to track (BNB held for fees, say). The
    /// balance is not tracked. An unregistered asset on any other event is still an
    /// [`Self::Rejected`] error, because that drops ledger data.
    UntrackedBalance(AssetNameExchange),
    /// The event named an exchange, asset or instrument the map does not register, and is lost.
    Rejected(IndexError),
}

/// An indexed [`AccountSnapshot`] and the assets whose balances it left out. See
/// [`AccountEventIndexer::snapshot_partitioned`], whose caller must report `untracked`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PartitionedSnapshot {
    /// The snapshot, without the balances of the untracked assets.
    pub snapshot: AccountSnapshot,
    /// The assets of the skipped balances, in the order the venue reported them.
    pub untracked: Vec<AssetNameExchange>,
}

impl AccountEventIndexer {
    /// Index `event`, telling a balance for an unregistered asset apart from a lost event.
    ///
    /// See [`AccountIndexError`]. [`Self::account_event`] is the same indexing with one error.
    pub fn index_account_event(
        &self,
        event: UnindexedAccountEvent,
    ) -> Result<AccountEvent, AccountIndexError> {
        let balance_asset = match &event.kind {
            AccountEventKind::BalanceSnapshot(snapshot) => Some(snapshot.0.asset.clone()),
            AccountEventKind::BalanceStreamUpdate(update) => Some(update.0.asset.clone()),
            _ => None,
        };

        self.account_event(event)
            .map_err(|error| match (error, balance_asset) {
                // A balance event reaches no other lookup that can fail on an asset.
                (IndexError::AssetIndex(_), Some(asset)) => {
                    AccountIndexError::UntrackedBalance(asset)
                }
                (error, _) => AccountIndexError::Rejected(error),
            })
    }

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
            AccountEventKind::CashFlow(flow) => AccountEventKind::CashFlow(self.cash_flow(flow)?),
            AccountEventKind::Notice(notice) => {
                AccountEventKind::Notice(self.notice(exchange, notice))
            }
            AccountEventKind::MarginDebt(update) => {
                AccountEventKind::MarginDebt(self.margin_debt(update)?)
            }
        };

        Ok(AccountEvent { exchange, kind })
    }

    /// Index `snapshot`, failing on any asset the [`ExecutionInstrumentMap`] does not register.
    ///
    /// Use [`Self::snapshot_partitioned`] to skip an unregistered balance instead. This is also
    /// the path of an [`AccountEventKind::Snapshot`] event on an account stream, which stays
    /// strict: it replaces the engine's whole view of the account, so losing part of it is not
    /// tolerated.
    ///
    /// # Errors
    ///
    /// [`IndexError`] for an exchange, asset, instrument or order the map does not register.
    pub fn snapshot(
        &self,
        snapshot: UnindexedAccountSnapshot,
    ) -> Result<AccountSnapshot, IndexError> {
        let PartitionedSnapshot {
            snapshot,
            untracked,
        } = self.snapshot_partitioned(snapshot)?;
        match untracked.first() {
            None => Ok(snapshot),
            Some(asset) => Err(self
                .map
                .find_asset_index(asset)
                .err()
                .unwrap_or_else(|| IndexError::AssetIndex(format!("not registered: {asset}")))),
        }
    }

    /// Index `snapshot`, skipping the top-level balance of an asset the
    /// [`ExecutionInstrumentMap`] does not register, and returning those assets beside it.
    ///
    /// A venue may report more assets than the caller registered (see
    /// [`AccountIndexError::UntrackedBalance`]), so such a balance is not a fault. Only top-level
    /// balances are skipped: an unregistered exchange, instrument, order, or isolated-margin
    /// base or quote asset still fails the snapshot, because each registered instrument brings
    /// its own assets and the loss would be a mapping defect.
    ///
    /// The caller must report [`PartitionedSnapshot::untracked`], or the drop is silent. An empty
    /// [`PartitionedSnapshot::snapshot`] balance list beside a non-empty `untracked` usually means
    /// the registered asset names do not match the venue's; it can also be an account that holds
    /// only unregistered assets.
    ///
    /// # Errors
    ///
    /// [`IndexError`] for an exchange, instrument, order or isolated-margin asset the map does
    /// not register.
    pub fn snapshot_partitioned(
        &self,
        snapshot: UnindexedAccountSnapshot,
    ) -> Result<PartitionedSnapshot, IndexError> {
        let UnindexedAccountSnapshot {
            exchange,
            balances,
            instruments,
        } = snapshot;

        let exchange = self.map.find_exchange_index(exchange)?;

        let mut untracked = Vec::new();
        let mut indexed_balances = Vec::with_capacity(balances.len());
        for balance in balances {
            match self.map.find_asset_index(&balance.asset) {
                Ok(asset) => indexed_balances.push(AssetBalance {
                    asset,
                    balance: balance.balance,
                    time_exchange: balance.time_exchange,
                }),
                Err(IndexError::AssetIndex(_)) => untracked.push(balance.asset),
                Err(error) => return Err(error),
            }
        }

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

        Ok(PartitionedSnapshot {
            snapshot: AccountSnapshot {
                exchange,
                balances: indexed_balances,
                instruments,
            },
            untracked,
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

    /// Index a [`CashFlow`]'s asset and, when it names one, instrument.
    ///
    /// # Errors
    /// Returns `IndexError` if the asset, or a named instrument, is not registered in the map.
    pub fn cash_flow(
        &self,
        flow: CashFlow<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<CashFlow<AssetIndex, InstrumentIndex>, IndexError> {
        let CashFlow {
            kind,
            asset,
            amount,
            instrument,
            time_exchange,
            id,
        } = flow;

        Ok(CashFlow {
            kind,
            asset: self.map.find_asset_index(&asset)?,
            amount,
            instrument: instrument
                .map(|instrument| self.map.find_instrument_index(&instrument))
                .transpose()?,
            time_exchange,
            id,
        })
    }

    /// Index a [`MarginDebtUpdate`]'s asset and, when it names one, instrument.
    ///
    /// Unlike a notice's, an unknown instrument is an error rather than dropped: an update without
    /// one is a debt of the account as a whole, which the engine applies to its asset balances, so
    /// an isolated pair's debt delivered unattributed would overwrite the account's.
    ///
    /// # Errors
    /// Returns `IndexError` if the asset, or a named instrument, is not registered in the map.
    pub fn margin_debt(
        &self,
        update: MarginDebtUpdate<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<MarginDebtUpdate<AssetIndex, InstrumentIndex>, IndexError> {
        let MarginDebtUpdate {
            asset,
            instrument,
            debt,
            kind,
            venue_kind,
            time_exchange,
        } = update;

        Ok(MarginDebtUpdate {
            asset: self.map.find_asset_index(&asset)?,
            instrument: instrument
                .map(|instrument| self.map.find_instrument_index(&instrument))
                .transpose()?,
            debt,
            kind,
            venue_kind,
            time_exchange,
        })
    }

    /// Index an [`AccountNotice`]'s instrument, when it names one.
    ///
    /// Never fails. A notice naming an instrument that is not in the map is kept, with its
    /// instrument set to `None` and a `warn`: a notice changes no state, so dropping one about a
    /// margin call or liquidation would cost more than losing its attribution.
    pub fn notice(
        &self,
        exchange: ExchangeIndex,
        notice: AccountNotice<InstrumentNameExchange>,
    ) -> AccountNotice<InstrumentIndex> {
        let AccountNotice {
            kind,
            instrument,
            time_exchange,
            margin_level,
            status,
        } = notice;

        let instrument = instrument.and_then(|instrument| {
            self.map
                .find_instrument_index(&instrument)
                .inspect_err(|error| {
                    warn!(
                        ?exchange,
                        %instrument,
                        %kind,
                        %status,
                        %error,
                        "account notice names an instrument that is not in the map, \
                         delivering it unattributed",
                    );
                })
                .ok()
        });

        AccountNotice {
            kind,
            instrument,
            time_exchange,
            margin_level,
            status,
        }
    }

    /// Index an [`InstrumentBalanceUpdate`]'s instrument and asset keys.
    ///
    /// # Errors
    /// Returns `IndexError` if the instrument or the asset is not registered in the map.
    pub fn instrument_balance_update(
        &self,
        update: InstrumentBalanceUpdate<AssetNameExchange, InstrumentNameExchange>,
    ) -> Result<InstrumentBalanceUpdate, IndexError> {
        let InstrumentBalanceUpdate {
            instrument,
            balance,
        } = update;

        Ok(InstrumentBalanceUpdate {
            instrument: self.map.find_instrument_index(&instrument)?,
            balance: self.asset_balance_update(balance)?,
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
            UnindexedApiError::RateLimit { retry_after } => ApiError::rate_limited(retry_after),
            UnindexedApiError::CapacityExceeded(msg) => ApiError::CapacityExceeded(msg),
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
            UnindexedApiError::BorrowRejected(reject) => ApiError::BorrowRejected(reject),
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

    /// Index an order request for this exchange, resolving its instrument to the name this
    /// exchange knows it by.
    ///
    /// # Errors
    /// Returns [`KeyError`] when the order's exchange or instrument is not in the map, and when
    /// the instrument is indexed but listed on a different exchange: a request keyed to another
    /// exchange's instrument is not this exchange's to send.
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
        let instrument = self
            .map
            .find_own_instrument_name_exchange(*instrument)
            .ok_or_else(|| {
                KeyError::InstrumentKey(format!(
                    "ExecutionInstrumentMap for {} does not contain: {instrument}",
                    self.map.exchange.value
                ))
            })?;

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
            UnindexedOrderError::NotPlaced(reason) => OrderError::NotPlaced(reason),
            UnindexedOrderError::Rejected(error) => OrderError::Rejected(self.api_error(error)),
            UnindexedOrderError::UnsupportedOrderType(msg) => OrderError::UnsupportedOrderType(msg),
            UnindexedOrderError::InvalidPrecision(violation) => {
                OrderError::InvalidPrecision(violation)
            }
            UnindexedOrderError::OutOfBounds(violation) => OrderError::OutOfBounds(violation),
            UnindexedOrderError::UnsupportedMarginEffect(effect) => {
                OrderError::UnsupportedMarginEffect(effect)
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
    /// - Fee in third-party asset (e.g., BNB): `fees_quote = None`, since the indexer holds no
    ///   prices. The `rustrade` engine values it when it prices that asset.
    ///
    /// # A fee asset that is not in the map
    /// The trade is kept, with its fee zeroed: `fees` is `0` in the instrument's quote asset and
    /// `fees_quote` is `None`. A `warn` names the fee's asset and amount. Dropping the trade would
    /// leave the engine's position different from the venue's, which costs far more than one
    /// unknown fee. The `warn` is per trade, so an account that pays every fee in an unregistered
    /// asset logs one per fill until the asset is registered. Two cases reach this:
    /// - An asset the account pays fees in but no instrument implies, such as BNB on Binance spot
    ///   with "pay fees with BNB" turned on. Register it with
    ///   [`IndexedInstrumentsBuilder::add_asset`](rustrade_instrument::index::builder::IndexedInstrumentsBuilder::add_asset)
    ///   to keep its fee.
    /// - The `"UNKNOWN"` placeholder some integrations use when the venue gave no fee asset: IBKR
    ///   `fetch_trades` for an execution IB sent no commission report for, and Binance when a
    ///   report omits `commissionAsset`.
    ///
    /// # Errors
    /// Returns `IndexError` if the trade's instrument is not in the map.
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
            origin,
            position_side,
            client_order_id,
        } = trade;

        let instrument_index = self.map.find_instrument_index(&instrument)?;
        // Invariant check: `find_instrument_index` resolves only names built from `instruments`,
        // so the index is always present. Failing loudly beats indexing a fee against nothing.
        let underlying = &self
            .map
            .instruments
            .get_index(instrument_index.index())
            .ok_or_else(|| {
                IndexError::InstrumentIndex(format!(
                    "ExecutionInstrumentMap has no instrument at {instrument_index} for {instrument}"
                ))
            })?
            .underlying;

        let fees = match self.map.find_asset_index(&fees.asset) {
            Ok(fee_asset) => {
                let fees_quote = if fee_asset == underlying.quote {
                    // Fee is in quote asset — no conversion needed
                    Some(fees.fees)
                } else if fee_asset == underlying.base {
                    // Fee is in base asset — convert using trade price
                    Some(fees.fees * trade_price)
                } else {
                    // Fee is in third-party asset (e.g., BNB) — needs external price
                    None
                };
                AssetFees::new(fee_asset, fees.fees, fees_quote)
            }
            Err(error) => {
                warn!(
                    exchange = %self.map.exchange.value,
                    %instrument,
                    trade_id = %id,
                    fee_asset = %fees.asset,
                    fee = %fees.fees,
                    %error,
                    "trade's fee asset is not in the map, keeping the trade with its fee zeroed; \
                     register the asset with IndexedInstrumentsBuilder::add_asset to keep its fee",
                );
                AssetFees::new(underlying.quote, Decimal::ZERO, None)
            }
        };

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
            fees,
            origin,
            position_side,
            client_order_id,
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

    #[test]
    fn a_partitioned_snapshot_skips_an_unregistered_balance_and_the_strict_one_fails() {
        let indexer = binance_indexer();
        let snapshot = || {
            UnindexedAccountSnapshot::new(
                ExchangeId::BinanceSpot,
                ["usdt", "bnb", "btc", "sol"]
                    .map(|asset| {
                        AssetBalance::new(
                            AssetNameExchange::new(asset),
                            crate::balance::Balance::new(Decimal::ONE, Decimal::ONE),
                            DateTime::<Utc>::MIN_UTC,
                        )
                    })
                    .to_vec(),
                vec![],
            )
        };

        let partitioned = indexer.snapshot_partitioned(snapshot()).unwrap();
        assert_eq!(partitioned.snapshot.balances.len(), 2);
        assert_eq!(
            partitioned.untracked,
            vec![AssetNameExchange::new("bnb"), AssetNameExchange::new("sol")]
        );
        assert!(matches!(
            indexer.snapshot(snapshot()),
            Err(IndexError::AssetIndex(_))
        ));
    }

    #[test]
    fn a_partitioned_snapshot_still_fails_on_an_unregistered_instrument_or_exchange() {
        let indexer = binance_indexer();
        let instrument = |name: &str| {
            InstrumentAccountSnapshot::new(
                InstrumentNameExchange::new(name),
                vec![],
                false,
                crate::position::PositionReport::Unreported,
                None,
            )
        };
        let unknown_instrument = UnindexedAccountSnapshot::new(
            ExchangeId::BinanceSpot,
            vec![],
            vec![instrument("ETHUSDT")],
        );
        assert!(matches!(
            indexer.snapshot_partitioned(unknown_instrument),
            Err(IndexError::InstrumentIndex(_))
        ));
        // A skipped balance does not hide a failure elsewhere in the same snapshot.
        let skipped_and_unknown_instrument = UnindexedAccountSnapshot::new(
            ExchangeId::BinanceSpot,
            vec![AssetBalance::new(
                AssetNameExchange::new("bnb"),
                crate::balance::Balance::new(Decimal::ONE, Decimal::ONE),
                DateTime::<Utc>::MIN_UTC,
            )],
            vec![instrument("ETHUSDT")],
        );
        assert!(matches!(
            indexer.snapshot_partitioned(skipped_and_unknown_instrument),
            Err(IndexError::InstrumentIndex(_))
        ));
        let other_exchange = UnindexedAccountSnapshot::new(ExchangeId::Ibkr, vec![], vec![]);
        assert!(matches!(
            indexer.snapshot_partitioned(other_exchange),
            Err(IndexError::ExchangeIndex(_))
        ));
    }

    /// An order naming an instrument listed on another exchange does not resolve, though the plain
    /// instrument lookup would find it.
    #[test]
    fn an_order_request_for_another_exchanges_instrument_fails() {
        use crate::order::id::{ClientOrderId, StrategyId};
        let instruments = IndexedInstruments::new(vec![
            test_utils::instrument(ExchangeId::Kraken, "BTC", "USDT"),
            test_utils::instrument(ExchangeId::BinanceSpot, "BTC", "USDT"),
        ]);
        let binance = generate_execution_instrument_map(&instruments, ExchangeId::BinanceSpot)
            .map(Arc::new)
            .map(AccountEventIndexer::new)
            .unwrap();
        let instrument_on = |exchange| {
            instruments
                .find_instrument_index(
                    exchange,
                    &test_utils::instrument(exchange, "BTC", "USDT").name_internal,
                )
                .unwrap()
        };
        let order = |exchange, instrument| OrderEvent {
            key: OrderKey {
                exchange,
                instrument,
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::random(),
            },
            state: (),
        };
        let binance_index = instruments
            .find_exchange_index(ExchangeId::BinanceSpot)
            .unwrap();

        let own = binance
            .order_request(&order(
                binance_index,
                instrument_on(ExchangeId::BinanceSpot),
            ))
            .unwrap();
        assert_eq!(own.key.exchange, ExchangeId::BinanceSpot);
        assert_eq!(own.key.instrument.as_ref(), "BTC_USDT");

        let foreign =
            binance.order_request(&order(binance_index, instrument_on(ExchangeId::Kraken)));
        assert!(
            matches!(foreign, Err(KeyError::InstrumentKey(_))),
            "{foreign:?}"
        );
    }

    fn binance_indexer() -> AccountEventIndexer {
        let instruments = IndexedInstruments::new(vec![test_utils::instrument(
            ExchangeId::BinanceSpot,
            "BTC",
            "USDT",
        )]);
        let map = generate_execution_instrument_map(&instruments, ExchangeId::BinanceSpot).unwrap();
        AccountEventIndexer::new(Arc::new(map))
    }

    fn balance_update(asset: &str) -> UnindexedAccountEvent {
        UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::BalanceStreamUpdate(Snapshot(AssetBalanceUpdate::new(
                AssetNameExchange::new(asset),
                crate::balance::BalanceUpdate::new(rust_decimal::Decimal::ONE, Default::default()),
                DateTime::<Utc>::MIN_UTC,
            ))),
        )
    }

    #[test]
    fn a_balance_for_an_unregistered_asset_is_an_untracked_balance_not_a_rejection() {
        let indexer = binance_indexer();

        assert!(indexer.index_account_event(balance_update("usdt")).is_ok());
        assert_eq!(
            indexer.index_account_event(balance_update("bnb")),
            Err(AccountIndexError::UntrackedBalance(AssetNameExchange::new(
                "bnb"
            )))
        );

        let snapshot = UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance::new(
                AssetNameExchange::new("bnb"),
                crate::balance::Balance::new(Default::default(), Default::default()),
                DateTime::<Utc>::MIN_UTC,
            ))),
        );
        assert!(matches!(
            indexer.index_account_event(snapshot),
            Err(AccountIndexError::UntrackedBalance(_))
        ));
    }

    #[test]
    fn an_unregistered_asset_on_any_other_event_is_still_a_rejection() {
        let indexer = binance_indexer();
        let cash_flow = UnindexedAccountEvent::new(
            ExchangeId::BinanceSpot,
            AccountEventKind::CashFlow(CashFlow {
                kind: crate::cash_flow::CashFlowKind::Rebate,
                asset: AssetNameExchange::new("bnb"),
                amount: rust_decimal::Decimal::ONE,
                instrument: None,
                time_exchange: DateTime::<Utc>::MIN_UTC,
                id: None,
            }),
        );
        assert!(matches!(
            indexer.index_account_event(cash_flow),
            Err(AccountIndexError::Rejected(IndexError::AssetIndex(_)))
        ));

        // A balance on an exchange the map does not hold fails on the exchange, not the asset.
        let mut wrong_exchange = balance_update("bnb");
        wrong_exchange.exchange = ExchangeId::Kraken;
        assert!(matches!(
            indexer.index_account_event(wrong_exchange),
            Err(AccountIndexError::Rejected(IndexError::ExchangeIndex(_)))
        ));
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

    /// A fill is never dropped for its fee: an unmapped fee asset, or the `"UNKNOWN"` placeholder,
    /// keeps the trade with its fee zeroed in quote and `fees_quote: None`, with one `warn`. A fee
    /// asset registered with `add_asset` indexes as a third-party fee.
    #[test]
    fn trade_keeps_a_fill_whose_fee_asset_is_unmapped() {
        use crate::{
            order::id::{OrderId, StrategyId},
            trade::TradeId,
        };
        use rust_decimal_macros::dec;
        use rustrade_instrument::{Side, index::builder::IndexedInstrumentsBuilder};

        let fill = |fee_asset: &str, fee| {
            Trade::new(
                TradeId::new("t-1"),
                OrderId::new("ord-1"),
                InstrumentNameExchange::new("BTC_USDT"),
                StrategyId::unknown(),
                DateTime::<Utc>::MIN_UTC,
                Side::Buy,
                dec!(50000),
                dec!(0.01),
                None,
                AssetFees::new(AssetNameExchange::new(fee_asset), fee, None),
            )
        };
        let asset = |indexer: &AccountEventIndexer, name: &str| {
            indexer
                .map
                .find_asset_index(&AssetNameExchange::new(name))
                .unwrap()
        };

        let indexer = binance_indexer();
        let usdt = asset(&indexer, "USDT");
        let btc = asset(&indexer, "BTC");

        let (quote, warnings) = count_warnings(|| indexer.trade(fill("USDT", dec!(0.5))));
        assert_eq!(
            quote.unwrap().fees,
            AssetFees::new(usdt, dec!(0.5), Some(dec!(0.5)))
        );
        assert_eq!(warnings, 0);

        let (base, warnings) = count_warnings(|| indexer.trade(fill("BTC", dec!(0.00001))));
        assert_eq!(
            base.unwrap().fees,
            AssetFees::new(btc, dec!(0.00001), Some(dec!(0.5)))
        );
        assert_eq!(warnings, 0);

        for unmapped in ["BNB", "UNKNOWN"] {
            let (kept, warnings) = count_warnings(|| indexer.trade(fill(unmapped, dec!(0.001))));
            let kept = kept.unwrap_or_else(|error| panic!("{unmapped}: dropped: {error}"));
            assert_eq!(
                kept.quantity,
                dec!(0.01),
                "{unmapped}: the fill itself is intact"
            );
            assert_eq!(
                kept.fees,
                AssetFees::new(usdt, Decimal::ZERO, None),
                "{unmapped}: fee zeroed in quote, with no quote equivalent"
            );
            assert_eq!(warnings, 1, "{unmapped}: the zeroed fee is reported");
        }

        let instruments = IndexedInstrumentsBuilder::new()
            .add_instrument(test_utils::instrument(
                ExchangeId::BinanceSpot,
                "BTC",
                "USDT",
            ))
            .add_asset(test_utils::exchange_asset(ExchangeId::BinanceSpot, "BNB"))
            .build();
        let with_bnb = AccountEventIndexer::new(Arc::new(
            generate_execution_instrument_map(&instruments, ExchangeId::BinanceSpot).unwrap(),
        ));
        let bnb = asset(&with_bnb, "BNB");
        let (third_party, warnings) = count_warnings(|| with_bnb.trade(fill("BNB", dec!(0.001))));
        assert_eq!(
            third_party.unwrap().fees,
            AssetFees::new(bnb, dec!(0.001), None),
            "a registered fee asset keeps its fee, with no quote equivalent"
        );
        assert_eq!(warnings, 0);

        let mut unmapped_instrument = fill("USDT", dec!(0.5));
        unmapped_instrument.instrument = InstrumentNameExchange::new("ETH_USDT");
        assert!(
            indexer.trade(unmapped_instrument).is_err(),
            "an unmapped instrument still fails"
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

    /// A cash flow indexes its asset and, when it names one, its instrument, and fails when
    /// either is not in the map.
    #[test]
    fn account_event_indexes_a_cash_flow() {
        use crate::cash_flow::{CashFlowId, CashFlowKind};
        use rust_decimal_macros::dec;

        let indexer = binance_indexer();
        let time = DateTime::<Utc>::MIN_UTC;
        let kind = CashFlowKind::Funding {
            rate: Some(dec!(0.0001)),
            position_quantity: Some(dec!(-2)),
        };
        let flow = |asset: &str, instrument: Option<&str>| {
            UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::CashFlow(CashFlow::new(
                    kind.clone(),
                    AssetNameExchange::new(asset),
                    dec!(1.5),
                    instrument.map(InstrumentNameExchange::new),
                    time,
                    Some(CashFlowId::new("cf-1")),
                )),
            )
        };
        let index_of = |event| match indexer.account_event(event) {
            Ok(AccountEvent {
                kind: AccountEventKind::CashFlow(flow),
                ..
            }) => Ok(flow),
            Ok(other) => panic!("expected CashFlow, got {other:?}"),
            Err(error) => Err(error),
        };
        let (Ok(usdt), Ok(btc_usdt)) = (
            indexer
                .map
                .find_asset_index(&AssetNameExchange::new("USDT")),
            indexer
                .map
                .find_instrument_index(&InstrumentNameExchange::new("BTC_USDT")),
        ) else {
            panic!("USDT and BTC_USDT are mapped");
        };

        assert_eq!(
            index_of(flow("USDT", Some("BTC_USDT"))),
            Ok(CashFlow::new(
                kind.clone(),
                usdt,
                dec!(1.5),
                Some(btc_usdt),
                time,
                Some(CashFlowId::new("cf-1")),
            ))
        );
        assert_eq!(
            index_of(flow("USDT", None)).map(|flow| flow.instrument),
            Ok(None)
        );
        assert!(
            index_of(flow("ETH", None)).is_err(),
            "an unmapped asset fails"
        );
        assert!(
            index_of(flow("USDT", Some("ETHUSDT"))).is_err(),
            "an unmapped instrument fails"
        );
    }

    /// A notice indexes its instrument when it names a mapped one, and is kept unattributed,
    /// rather than dropped, when it names one that is not mapped.
    #[test]
    fn account_event_indexes_a_notice() {
        use crate::notice::{AccountNotice, NoticeKind};
        use rust_decimal_macros::dec;
        use smol_str::SmolStr;

        let indexer = binance_indexer();
        let time = DateTime::<Utc>::MIN_UTC;
        let notice = |instrument: Option<&str>| {
            UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::Notice(AccountNotice::new(
                    NoticeKind::MarginCall,
                    instrument.map(InstrumentNameExchange::new),
                    time,
                    Some(dec!(1.1)),
                    SmolStr::new("MARGIN_CALL"),
                )),
            )
        };
        let index_of = |event| match indexer.account_event(event) {
            Ok(AccountEvent {
                kind: AccountEventKind::Notice(notice),
                ..
            }) => notice,
            other => panic!("expected Notice, got {other:?}"),
        };
        let Ok(btc_usdt) = indexer
            .map
            .find_instrument_index(&InstrumentNameExchange::new("BTC_USDT"))
        else {
            panic!("BTC_USDT is mapped");
        };

        assert_eq!(
            index_of(notice(Some("BTC_USDT"))),
            AccountNotice::new(
                NoticeKind::MarginCall,
                Some(btc_usdt),
                time,
                Some(dec!(1.1)),
                SmolStr::new("MARGIN_CALL"),
            )
        );
        assert_eq!(index_of(notice(None)).instrument, None);
        assert_eq!(
            index_of(notice(Some("ETHUSDT"))).instrument,
            None,
            "an unmapped instrument is dropped from the notice, not the notice itself"
        );
    }

    /// A margin debt update indexes its asset and instrument, and one naming an unmapped
    /// instrument fails rather than becoming the account's debt.
    #[test]
    fn account_event_indexes_a_margin_debt_update() {
        use crate::{
            balance::MarginDetails,
            margin_debt::{MarginDebtKind, MarginDebtUpdate},
        };
        use rust_decimal_macros::dec;
        use smol_str::SmolStr;

        let indexer = binance_indexer();
        let time = DateTime::<Utc>::MIN_UTC;
        let debt = MarginDetails::new(dec!(0.5), dec!(0.01));
        let update = |asset: &str, instrument: Option<&str>| {
            UnindexedAccountEvent::new(
                ExchangeId::BinanceSpot,
                AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                    AssetNameExchange::new(asset),
                    instrument.map(InstrumentNameExchange::new),
                    debt,
                    MarginDebtKind::Borrow,
                    SmolStr::new("BORROW"),
                    time,
                )),
            )
        };
        let index_of = |event| match indexer.account_event(event) {
            Ok(AccountEvent {
                kind: AccountEventKind::MarginDebt(update),
                ..
            }) => Ok(update),
            Ok(other) => panic!("expected MarginDebt, got {other:?}"),
            Err(error) => Err(error),
        };
        let (Ok(btc), Ok(btc_usdt)) = (
            indexer.map.find_asset_index(&AssetNameExchange::new("BTC")),
            indexer
                .map
                .find_instrument_index(&InstrumentNameExchange::new("BTC_USDT")),
        ) else {
            panic!("BTC and BTC_USDT are mapped");
        };

        assert_eq!(
            index_of(update("BTC", Some("BTC_USDT"))),
            Ok(MarginDebtUpdate::new(
                btc,
                Some(btc_usdt),
                debt,
                MarginDebtKind::Borrow,
                SmolStr::new("BORROW"),
                time,
            ))
        );
        assert_eq!(
            index_of(update("BTC", None)).map(|update| update.instrument),
            Ok(None)
        );
        assert!(
            index_of(update("ETH", None)).is_err(),
            "an unmapped asset fails"
        );
        assert!(
            index_of(update("BTC", Some("ETHUSDT"))).is_err(),
            "an unmapped instrument fails"
        );
    }
}
