//! Interactive Brokers ExecutionClient implementation.
//!
//! Uses the `ibapi` crate for IB TWS/Gateway connectivity. Supports equities,
//! futures, options, and forex.
//!
//! # Testing Status
//!
//! **NOT TESTED in CI.** IBKR has not confirmed permission to use credentials
//! for CI, and requires IB Gateway/TWS running locally.
//!
//! **Tested locally:** All execution tests (connection, orders, account streaming)
//! run on paper trading accounts — no market data subscriptions required.
//!
//! # Connection
//!
//! Requires TWS or IB Gateway running locally with API enabled:
//!
//! | Application | Live Port | Paper Port |
//! |-------------|-----------|------------|
//! | TWS         | 7496      | 7497       |
//! | IB Gateway  | 4001      | 4002       |
//!
//! Enable API in TWS/Gateway: Configure → API → Settings → Enable ActiveX and Socket Clients.
//! For order placement, uncheck "Read-Only API".
//!
//! # Architecture
//!
//! - Connection: TCP socket to TWS/Gateway
//! - Orders: Subscription-based events (OrderStatus, ExecutionData, CommissionReport)
//! - Account: Subscription-based position and balance updates
//!
//! # Balances and carrying costs
//!
//! Balances are read from IB's account summary, one per currency: `total` is `TotalCashValue` and
//! `free` is `AvailableFunds`. No other tag is read, and [`ExecutionClient::account_stream`]
//! delivers no balances.
//!
//! As IB documents it, margin interest and stock borrow fees accrue through the month, reported
//! under the `AccruedCash` tag, and are posted to cash monthly. Until they are posted,
//! `TotalCashValue`, and so `total`, leaves them out: up to a month of charges. This client does
//! not read `AccruedCash`. Fills carry none of these charges, so PnL computed from them excludes
//! them too.
//!
//! # Limitations
//!
//! - **Order types**: Market, Limit, Stop, StopLimit, TrailingStop, TrailingStopLimit,
//!   and Bracket (entry + take-profit + stop-loss) supported. No Algo orders.
//! - **TimeInForce**: No `post_only` (IB has no maker-only orders)
//! - **Client order ids** travel as each order's IB order reference, so they are at most 128
//!   ASCII characters
//!
//! # Reconnection & Recovery
//!
//! `ibapi` reconnects its socket to TWS/Gateway by itself, with the same client ID.
//! In-flight requests fail with `ConnectionReset` at the drop, and new sends are refused
//! until the reconnect completes. Order sends classify that as a transient
//! [`OrderError::Connectivity`]. If the reconnect fails, `ibapi` shuts the client down.
//!
//! [`ExecutionClient::account_stream`] stays open across a successful reconnect. It
//! detects the gap and, once delivery is restored, recovers the fills TWS sent while
//! the socket was down, emitting each exactly once: as a `Trade`, or as a
//! `TradeAmended` when it corrects an earlier execution. It then reports how each order
//! it held as live ended during the gap, if it did. The same applies when TWS loses its
//! own link to IB's servers (notice 1100) and later restores it (1101/1102). See that
//! method for the details.
//!
//! Each order carries its client order id as its IB order reference, which IB lists with
//! the order, its executions and its completion. So a client finds its orders by the id
//! they were placed with even across a restart: [`ExecutionClient::account_snapshot`] and
//! [`ExecutionClient::fetch_open_orders`] list them under it and track them again, and
//! [`OrderStatusClient`] finds how they ended.
//!
//! # Caller Responsibilities
//!
//! 1. **Fill reconciliation**: when fill recovery fails repeatedly, the stream reports the
//!    gap as `AccountEventKind::FillRecoveryGaveUp` and stays open. Read the gap's fills
//!    with [`ExecutionClient::fetch_trades`].
//! 2. **Permanent disconnect**: when `ibapi` gives up reconnecting,
//!    [`IbkrClient::disconnect`] is called, or TWS/Gateway ends the API session,
//!    `account_stream` ends with `StreamTerminated`, and `account_stream` on the
//!    shut-down client fails, so replace the client, by reconnecting with
//!    [`IbkrClient::connect_sync`] and choosing the client ID: the caller's decision. The old
//!    client's ID stays in use until every clone of it is dropped (see
//!    [`IbkrClient::disconnect`]). A new
//!    `IbkrClient` tracks the orders the old one placed once an account snapshot or
//!    `fetch_open_orders` lists them; until then their events are dropped.
//! 3. **Stale state cleanup**: Periodically call [`IbkrClient::clear_stale_executions`],
//!    [`IbkrClient::clear_stale_order_ids`], and [`IbkrClient::clear_stale_pending_cancels`]
//!
//! **Rationale**: IBKR uses TCP to local TWS/Gateway, not cloud WebSocket. `ibapi`
//! owns the transient reconnect. Replacing a client that is gone for good requires IB
//! Gateway availability and client ID coordination, decisions that belong in the
//! caller's wrapper, not the library.
//!
//! # See Also
//!
//! - [IB API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/trader-workstation-api/)
//! - `rustrade_data::exchange::ibkr` for market data

pub mod account;
mod connect;
pub mod contract;
mod ended_orders;
pub mod execution;
pub mod order;
mod recovery;

use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, Snapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::{
        BracketOrderClient, ClientInstrument, ExecutionClient, OrderStatusClient,
        dedup::new_dedup_cache,
        order_recovery::{KnownLiveOrders, SharedKnownLiveOrders},
    },
    error::{
        ApiError, ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce, UnindexedOrderSnapshot,
        bracket::{
            BracketOrderRequest as UnifiedBracketOrderRequest,
            BracketOrderResult as UnifiedBracketOrderResult,
        },
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{
            OrderRequestCancel, OrderRequestOpen, OrderResponseCancel, UnindexedOrderResponseCancel,
        },
        state::{Cancelled, Expired, Filled, Open, OrderState, UnindexedOrderState},
    },
    position::PositionReport,
    trade::{Trade, TradesRead},
};
use account::{BalanceAggregator, PositionAggregator};
use chrono::{DateTime, Utc};
pub use connect::{ConnectOutcome, ContractSkipReason, IbkrConnectError, SkippedContract};
use contract::ResolveContractError;
use execution::{ExecutionBuffer, ExecutionRevision, parse_decimal_or_warn, try_decimal_or_warn};
use futures::stream::BoxStream;
use ibapi::{
    accounts::{
        AccountSummaryResult,
        types::{AccountGroup, AccountId},
    },
    client::blocking::Client,
};
pub use order::{BracketOrderRequest, BracketOrderResult};
use order::{
    OrderContext, OrderIdMap, PendingCancels, build_ib_bracket_with_oca, build_ib_order,
    side_to_action, time_in_force_to_ib,
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    ibkr::{ContractRegistry, ContractRegistryError},
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    ops::ControlFlow,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// Configuration for the IBKR execution client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrConfig {
    /// TWS/Gateway host (e.g., "127.0.0.1")
    pub host: String,
    /// TWS/Gateway port (7496=TWS live, 7497=TWS paper, 4001=GW live, 4002=GW paper)
    pub port: u16,
    /// Client ID (must be unique per connection)
    pub client_id: i32,
    /// Account ID (e.g., "DU123456" for paper).
    ///
    /// Currently unused — balance/position queries use "All" group.
    /// Reserved for future multi-account routing (advisor accounts).
    pub account: String,
    /// Contracts to resolve and register on connect. [`IbkrClient::connect_sync`] fails if any
    /// of them cannot be registered; [`IbkrClient::connect_sync_lenient`] connects without it
    /// and reports it.
    #[serde(default)]
    pub contracts: Vec<ContractConfig>,
}

/// Pre-configured contract for startup registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractConfig {
    pub name: String,
    pub symbol: String,
    pub security_type: String,
    pub exchange: String,
    pub currency: String,
    #[serde(default)]
    pub last_trade_date: Option<String>,
    /// Strike price for options. Uses f64 because `ibapi::Contract.strike` is f64.
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default)]
    pub right: Option<String>,
}

impl ContractConfig {
    /// Convert this config into an [`ibapi::contracts::Contract`].
    ///
    /// # Errors
    ///
    /// Returns a [`ContractConfigError`](contract::ContractConfigError) instead of
    /// silently fabricating a wrong contract when the config is incomplete or
    /// unsupported:
    /// - [`UnrecognizedSecurityType`](contract::ContractConfigError::UnrecognizedSecurityType)
    ///   — `security_type` is not one of `STK`/`FUT`/`OPT`/`CASH`.
    /// - [`MissingLastTradeDate`](contract::ContractConfigError::MissingLastTradeDate)
    ///   — `last_trade_date` is absent on a `FUT` or `OPT` contract.
    /// - [`MissingStrike`](contract::ContractConfigError::MissingStrike) — `strike`
    ///   is absent on an `OPT` contract.
    /// - [`MissingOptionRight`](contract::ContractConfigError::MissingOptionRight) —
    ///   `right` is absent on an `OPT` contract.
    /// - [`UnrecognizedOptionRight`](contract::ContractConfigError::UnrecognizedOptionRight)
    ///   — `right` is present but not one of `C`/`CALL`/`P`/`PUT` (case-insensitive).
    ///
    /// A security type added here must also be added to `security_types_of`, or every entry naming
    /// it fails `validate_config`. Nothing enforces that: add the type to
    /// `security_types_cover_every_type_to_contract_builds` too.
    fn to_contract(&self) -> Result<ibapi::contracts::Contract, contract::ContractConfigError> {
        use contract::ContractConfigError as E;
        Ok(match self.security_type.as_str() {
            "STK" => contract::stock_contract(&self.symbol, &self.exchange, &self.currency),
            "FUT" => contract::futures_contract(
                &self.symbol,
                self.last_trade_date
                    .as_deref()
                    .ok_or(E::MissingLastTradeDate)?,
                &self.exchange,
                &self.currency,
            ),
            "OPT" => contract::option_contract(
                &self.symbol,
                self.last_trade_date
                    .as_deref()
                    .ok_or(E::MissingLastTradeDate)?,
                self.strike.ok_or(E::MissingStrike)?,
                self.right.as_deref().ok_or(E::MissingOptionRight)?,
                &self.exchange,
                &self.currency,
            )?,
            "CASH" => contract::forex_contract(&self.symbol, &self.currency),
            other => {
                return Err(E::UnrecognizedSecurityType {
                    security_type: other.to_string(),
                });
            }
        })
    }
}

/// The `ContractConfig::security_type`s that describe an instrument of `kind`: the inverse of the
/// mapping `SUPPORTED_KINDS` states. Empty for a kind this client cannot trade, which
/// `SUPPORTED_KINDS` rejects before any config is checked.
fn security_types_of(kind: InstrumentKindDiscriminant) -> &'static [&'static str] {
    match kind {
        InstrumentKindDiscriminant::Spot => &["STK", "CASH"],
        InstrumentKindDiscriminant::Future => &["FUT"],
        InstrumentKindDiscriminant::Option => &["OPT"],
        InstrumentKindDiscriminant::Perpetual | InstrumentKindDiscriminant::Cfd => &[],
    }
}

/// Account group for "All" accounts (used by reqAccountSummary).
static ACCOUNT_GROUP_ALL: std::sync::LazyLock<AccountGroup> =
    std::sync::LazyLock::new(|| AccountGroup("All".to_string()));

/// How long `account_snapshot` waits for IB's next report of an account's positions before
/// giving up on that account's listing.
///
/// Each account's read ends at IB's end-of-listing marker. This only bounds a stall before it.
const POSITION_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum time to await an initial `OrderStatus` on an order-placement
/// subscription before giving up. Bounds the placement loops so a silent TWS
/// (e.g. a half-open socket) cannot hang them indefinitely. A timeout yields
/// [`PlacementOutcome::NoStatus`]; the order may still have been submitted, so
/// callers resolve the final state via the order-update/account stream.
const PLACEMENT_STATUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// IBKR "Order Message" codes that are informational rather than rejections.
///
/// ibapi classifies the `200..=399` range as order rejections
/// (`ORDER_REJECTION_CODE_RANGE`) — bar 202 and, since 4.1.0, 317, both of
/// which are resolved ahead of the range — but IBKR uses code 399 as a generic order
/// message — e.g. *"Your order will not be placed at the exchange until
/// 09:30:00 US/Eastern"* — for an order that is in fact **accepted and held**
/// (it proceeds to `PreSubmitted`). We therefore report the order as
/// live-but-pending ([`PlacementOutcome::HeldPending`]) rather than rejected
/// when one of these codes arrives.
///
/// # ibapi 4.0 splits code 399 by message text
///
/// `is_warning_message` now classifies a 399 whose text carries a `Warning:`
/// line as a warning, and `classify_error` routes a warning owned by a
/// request-bound subscription as a non-terminal `RoutedItem::Notice` rather
/// than a stream-ending `RoutedItem::Error`. `timeout_iter_data` drops notices,
/// so the two forms now reach [`await_order_placement`] differently:
///
/// - **399 carrying a `Warning:` line** — dropped, and the subscription stays
///   open. The eventual `OrderStatus(PreSubmitted)` therefore arrives on the
///   placement subscription itself and yields
///   [`PlacementOutcome::Accepted`] without this list being consulted. Under
///   ibapi 3.x that status was reachable only via the order-update/account
///   stream, because the notice had already closed the subscription.
/// - **399 without one** — still `Err(Error::Notice)`, still closes the
///   subscription, and is still matched against this list.
///
/// Re-verified against ibapi 4.1.0, which reworked this classification: the
/// warning band widened to `2100..=2199`, `classify_error`'s predicate became
/// `is_informational_code`, and 317 joined the data advisories. The change is
/// strictly additive and 399 is in neither the advisory nor the system list, so
/// both forms above route exactly as described. Re-verified against ibapi 5.0.0, which
/// routes an error frame by whether its id is an order's or a request's, and moves 316, 354
/// and 366 out of the order-rejection category: 399 routes to its order as before, and the
/// warning predicate is unchanged.
///
/// The list stays load-bearing for the second form. It documents the known gap
/// between ibapi's range heuristic and IBKR's actual protocol semantics; if
/// IBKR adds further informational codes in the 200-399 range, an out-of-RTH
/// placement test will surface them and they can be added here.
const INFORMATIONAL_ORDER_CODES: &[i32] = &[399];

/// Outcome of awaiting the initial status on an order-placement subscription.
///
/// The authoritative acceptance/rejection signal in the IBKR protocol is the
/// `OrderStatus` event, not an error notice — see [`await_order_placement`].
///
/// Every variant carries a distinct order-placement outcome that callers must
/// dispatch on; dropping a value would silently discard the order's status.
#[must_use]
#[derive(Debug)]
enum PlacementOutcome {
    /// The order was acknowledged by IB and its order-id mapping must be
    /// retained so the account stream can resolve its terminal/fill state. This
    /// covers the working statuses (`Submitted`/`PreSubmitted`/`PendingSubmit`),
    /// an immediate `Filled`, and the transitional cancel states
    /// (`ApiCancelled`/`PendingCancel`) that precede a confirmed terminal status.
    /// Carries the filled quantity reported with the status.
    Accepted { filled: f64 },
    /// A terminal rejection: a `Cancelled`/`Inactive` `OrderStatus`, a genuine
    /// non-informational TWS notice, or an ibapi error other than transport
    /// loss (see [`is_transport_loss`]). Carries the human-readable reason.
    Rejected(String),
    /// The order exists, but the placement subscription could not classify its
    /// state: either an informational notice (see
    /// [`INFORMATIONAL_ORDER_CODES`]) closed the subscription, or TWS reported
    /// a status ibapi does not model (`OrderStatusKind::Unknown`). Either way
    /// the order is live/held and its authoritative status will arrive via the
    /// order-update/account stream. Carries the notice or status text.
    HeldPending(String),
    /// The subscription ended — or [`PLACEMENT_STATUS_TIMEOUT`] elapsed, or the
    /// transport was lost — without any terminal status or informational
    /// notice. The order may or may not have been accepted; resolve via the
    /// order-update/account stream.
    NoStatus,
}

/// Whether `e` is a transport loss `ibapi` recovers from by reconnecting by itself, so that the
/// same request may succeed once it has: a [`ConnectivityError`].
///
/// Narrower than [`is_transport_loss`]: `ibapi` returns `ConnectionFailed` only once it has given
/// up reconnecting, and `Shutdown` once the client is shut down, so retrying cannot help.
fn is_transient_transport_loss(e: &ibapi::Error) -> bool {
    matches!(e, ibapi::Error::ConnectionReset | ibapi::Error::Io(_))
}

/// Whether `e` means the TWS transport was lost, as opposed to TWS or ibapi
/// answering the request.
///
/// Since ibapi 4.2 the sync transport fails in-flight requests with one of these
/// the moment the socket drops, and refuses new sends until the reconnect has
/// completed. None of them says what TWS did with an order already written.
fn is_transport_loss(e: &ibapi::Error) -> bool {
    matches!(
        e,
        ibapi::Error::ConnectionReset
            | ibapi::Error::ConnectionFailed
            | ibapi::Error::Shutdown
            | ibapi::Error::Io(_)
    )
}

/// The instrument `execution` is in, or `None` when another API client placed its order, or this
/// client does not track its order or its contract.
///
/// IB numbers orders per API client, so an order id names this client's order only together with
/// this client's id.
fn resolve_execution(
    execution: &ibapi::orders::ExecutionData,
    api_client_id: i32,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
) -> Option<InstrumentNameExchange> {
    let order_id = execution.execution.order_id;
    let con_id = execution.contract.contract_id;

    if execution.execution.client_id != api_client_id {
        trace!(
            ib_order_id = order_id,
            api_client_id = execution.execution.client_id,
            "ExecutionData for another API client's order, dropping"
        );
        return None;
    }
    // Fail-fast: skip second lookup if first fails
    if !order_ids.contains(order_id) {
        debug!(
            ib_order_id = order_id,
            con_id, "ExecutionData for unknown order ID, dropping"
        );
        return None;
    }
    let Some(instrument) = contracts.get_name_by_con_id(con_id) else {
        debug!(
            ib_order_id = order_id,
            con_id, "ExecutionData for unknown contract ID, dropping"
        );
        return None;
    };
    Some(instrument)
}

/// How long a listing read waits for IB's next message before giving up on the read.
///
/// IB sends a listing in one burst and then marks its end, so a pause this long means TWS has
/// stalled, not that the listing is long.
const LISTING_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Read one IB listing, passing each item to `on_item`, until IB marks the end of the listing or
/// `on_item` breaks.
///
/// `next` is the subscription's `next_timeout`. That answers `None` at the end marker, when its
/// wait runs out, and when `ibapi` drops the subscription's channel. Only the first and last
/// return early (see [`recovery::returned_early`]), so an early `None` is the end of the listing
/// only while `connected` still says the client is connected.
///
/// # Errors
/// [`UnindexedClientError::Internal`], naming `what`, when IB answers with an error, sends
/// nothing for `stall_timeout` ([`LISTING_STALL_TIMEOUT`] outside tests) before the end of the
/// listing, or the connection is gone when the listing stops. Either way what was read is not the
/// whole listing, and must not be mistaken for it.
///
/// # Known limitation
/// A connection that drops and is restored between the last item and that check reads as the end
/// of the listing. `ibapi` fails a request in flight when its socket drops, which surfaces here as
/// an error first, so this needs the drop to lose that error too.
fn read_listing<T>(
    what: &str,
    stall_timeout: std::time::Duration,
    connected: impl Fn() -> bool,
    next: impl FnMut(
        std::time::Duration,
    ) -> Option<Result<ibapi::subscriptions::SubscriptionItem<T>, ibapi::Error>>,
    on_item: impl FnMut(T) -> ControlFlow<()>,
) -> Result<(), UnindexedClientError> {
    read_listing_items(what, stall_timeout, connected, next, on_item)
        .map_err(|e| UnindexedClientError::Internal(format!("{what}: {e}")))
}

/// Why a listing read ended short of IB's end-of-listing marker: see [`read_listing`].
#[derive(Debug, Error)]
enum ListingError {
    /// IB answered the request with an error.
    #[error(transparent)]
    Ibapi(ibapi::Error),
    /// The connection was gone when the listing stopped.
    #[error("the connection dropped before the end of the listing")]
    Dropped,
    /// IB sent nothing for this long before the end of the listing.
    #[error("IB sent nothing for {0:?} before the end of the listing")]
    Stalled(std::time::Duration),
}

/// [`read_listing`], keeping why the read failed for a caller that tells IB's errors apart.
/// `what` only names the listing in the notices logged during the read.
fn read_listing_items<T>(
    what: &str,
    stall_timeout: std::time::Duration,
    connected: impl Fn() -> bool,
    mut next: impl FnMut(
        std::time::Duration,
    ) -> Option<Result<ibapi::subscriptions::SubscriptionItem<T>, ibapi::Error>>,
    mut on_item: impl FnMut(T) -> ControlFlow<()>,
) -> Result<(), ListingError> {
    use ibapi::subscriptions::SubscriptionItem;

    loop {
        let waited = std::time::Instant::now();
        match next(stall_timeout) {
            Some(Ok(SubscriptionItem::Data(item))) => {
                if on_item(item).is_break() {
                    return Ok(());
                }
            }
            Some(Ok(SubscriptionItem::Notice(notice))) => {
                debug!(%notice, what, "Notice during an IBKR listing read");
            }
            Some(Err(e)) => return Err(ListingError::Ibapi(e)),
            None if recovery::returned_early(waited.elapsed(), stall_timeout) => {
                if !connected() {
                    return Err(ListingError::Dropped);
                }
                return Ok(());
            }
            None => return Err(ListingError::Stalled(stall_timeout)),
        }
    }
}

/// IB's error code for a contract description that matches no contract ("No security definition
/// has been found for the request").
const NO_SECURITY_DEFINITION_CODE: i32 = 200;

/// Resolve `contract` through IB's contract details: see [`IbkrClient::resolve_contract`].
fn resolve_contract_blocking(
    client: &Client,
    contract: &ibapi::contracts::Contract,
) -> Result<ibapi::contracts::Contract, ResolveContractError> {
    let subscription = client
        .contract_details_stream(contract)
        .subscribe()
        .map_err(contract_details_error)?;
    let mut matches = Vec::new();
    let read = read_listing_items(
        "contract_details",
        LISTING_STALL_TIMEOUT,
        || client.is_connected(),
        |timeout| subscription.next_timeout(timeout),
        |details: ibapi::contracts::ContractDetails| {
            matches.push(details.contract);
            ControlFlow::Continue(())
        },
    );
    classify_contract_details(read, matches)
}

/// The resolved contract from a contract-details read: its one match, or why there is not one.
fn classify_contract_details(
    read: Result<(), ListingError>,
    mut matches: Vec<ibapi::contracts::Contract>,
) -> Result<ibapi::contracts::Contract, ResolveContractError> {
    match read {
        Ok(()) => {}
        Err(ListingError::Ibapi(e)) => return Err(contract_details_error(e)),
        Err(e @ ListingError::Dropped) => {
            return Err(ConnectivityError::Socket(e.to_string()).into());
        }
        Err(ListingError::Stalled(_)) => return Err(ConnectivityError::Timeout.into()),
    }
    if matches.len() > 1 {
        return Err(ResolveContractError::Ambiguous { matches });
    }
    matches.pop().ok_or(ResolveContractError::NoMatch)
}

/// What an `ibapi` error answering a contract-details request means for the resolution.
fn contract_details_error(e: ibapi::Error) -> ResolveContractError {
    match e {
        // IB answers a description that matches nothing with this error, not an empty listing.
        ibapi::Error::Notice(notice) if notice.code == NO_SECURITY_DEFINITION_CODE => {
            ResolveContractError::NoMatch
        }
        ibapi::Error::Notice(notice) => ResolveContractError::Refused {
            code: notice.code,
            message: notice.message,
        },
        e if is_transient_transport_loss(&e) => ConnectivityError::Socket(e.to_string()).into(),
        e => ResolveContractError::Failed(e.to_string()),
    }
}

/// Serializes the reads of IB's order listings.
///
/// IB answers an open-orders or completed-orders request without naming the request, so `ibapi`
/// copies each answer to every such request of the kind in flight. Two reads at once would each
/// see the other's orders, and one could stop at the other's end marker, short of its own
/// listing. Each read holds this for its whole length.
#[derive(Debug, Clone, Default)]
struct ListingLock(Arc<Mutex<()>>);

impl ListingLock {
    /// Read IB's listing of every API client's open orders, each order followed by its status.
    ///
    /// # Errors
    /// As [`read_listing`].
    fn open_orders(
        &self,
        client: &Client,
    ) -> Result<Vec<ibapi::orders::Orders>, UnindexedClientError> {
        let _serial = self.0.lock();
        // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
        let subscription = client
            .all_open_orders()
            .map_err(|e| UnindexedClientError::Internal(format!("open_orders: {e}")))?;
        let mut listing = Vec::new();
        read_listing(
            "open_orders",
            LISTING_STALL_TIMEOUT,
            || client.is_connected(),
            |timeout| subscription.next_timeout(timeout),
            |item| {
                listing.push(item);
                ControlFlow::Continue(())
            },
        )?;
        Ok(listing)
    }

    /// Read IB's listing of the account's completed orders: every API client's, and those
    /// entered in TWS. Each item is handed to `on_item` as it arrives, since the listing can be
    /// long and only a few of its orders are wanted.
    ///
    /// # Errors
    /// As [`read_listing`].
    fn completed_orders(
        &self,
        client: &Client,
        mut on_item: impl FnMut(ibapi::orders::Orders),
    ) -> Result<(), UnindexedClientError> {
        let _serial = self.0.lock();
        let subscription = client
            .completed_orders(false)
            .map_err(|e| UnindexedClientError::Internal(format!("completed_orders: {e}")))?;
        read_listing(
            "completed_orders",
            LISTING_STALL_TIMEOUT,
            || client.is_connected(),
            |timeout| subscription.next_timeout(timeout),
            |item| {
                on_item(item);
                ControlFlow::Continue(())
            },
        )
    }
}

/// Read one account-summary listing into balances.
///
/// Errors and stalls are surfaced rather than a partial balance set returned (see
/// [`read_listing`]). The subscription stays live after the listing, so its end is the `End` item,
/// and a subscription that stops before it fails the read.
fn read_account_summary(
    stall_timeout: std::time::Duration,
    connected: impl Fn() -> bool,
    next: impl FnMut(
        std::time::Duration,
    ) -> Option<
        Result<ibapi::subscriptions::SubscriptionItem<AccountSummaryResult>, ibapi::Error>,
    >,
) -> Result<BalanceAggregator, UnindexedClientError> {
    let mut aggregator = BalanceAggregator::new();
    let mut ended = false;
    read_listing(
        "account_summary",
        stall_timeout,
        connected,
        next,
        |summary| match summary {
            AccountSummaryResult::Summary(s) => {
                aggregator.process(&s);
                ControlFlow::Continue(())
            }
            AccountSummaryResult::End => {
                ended = true;
                ControlFlow::Break(())
            }
        },
    )?;
    if !ended {
        return Err(UnindexedClientError::Internal(
            "account_summary: the subscription ended before the end of the listing".to_string(),
        ));
    }
    Ok(aggregator)
}

/// Read one account's positions into `positions`, returning whether IB marked the end of the
/// listing before [`POSITION_STALL_TIMEOUT`] passed without a report.
///
/// Positions-multi rather than `positions()`: its replies carry this request's ID, while
/// `positions()` replies carry none and reach every open positions subscription, so another read's
/// listing, end marker and all, could end this one early. IB documents the account parameter as
/// optional only for a login with a single account, so it is always given.
///
/// Only reports for a registered instrument, and in `instruments` when that is given, are kept.
fn read_account_positions(
    client: &Client,
    account: AccountId,
    contracts: &ContractRegistry,
    instruments: Option<&HashSet<InstrumentNameExchange>>,
    positions: &mut PositionAggregator,
) -> Result<bool, UnindexedClientError> {
    use ibapi::accounts::{Position, PositionUpdateMulti};

    let subscription = client
        .positions_multi(Some(&account), None)
        .map_err(|e| UnindexedClientError::Internal(format!("positions of {account}: {e}")))?;

    for update in subscription.timeout_iter_data(POSITION_STALL_TIMEOUT) {
        // Surface subscription errors rather than returning partial positions
        // (a truncated snapshot could be misread as positions having closed).
        let update = update.map_err(|e| {
            UnindexedClientError::Internal(format!("positions subscription of {account}: {e}"))
        })?;
        let position = match update {
            PositionUpdateMulti::Position(position) => Position {
                account: position.account,
                contract: position.contract,
                position: position.position,
                average_cost: position.average_cost,
            },
            // The subscription stays live after the listing, streaming changes; this read wants
            // the listing alone. Dropping the subscription cancels it.
            PositionUpdateMulti::PositionEnd => return Ok(true),
        };
        let Some(instrument) = contracts.get_name_by_con_id(position.contract.contract_id) else {
            continue;
        };
        if instruments.is_some_and(|filter| !filter.contains(&instrument)) {
            continue;
        }
        positions.process(instrument, position);
    }
    Ok(false)
}

/// Forward `updates`, from `ibapi`'s order-update subscription, to `sink` as account events,
/// until the subscription ends or the account stream does.
///
/// A closed stream is noticed on the next update, whether or not that update would be
/// forwarded, so the thread running this releases `ibapi`'s single order-update slot as soon as
/// TWS sends anything.
///
/// `ibapi` ends the subscription with [`ibapi::Error::Shutdown`] when the client shuts down for
/// good, and the stream is terminated with [`recovery::CLIENT_SHUT_DOWN`].
fn forward_order_updates(
    updates: impl IntoIterator<Item = Result<ibapi::orders::OrderUpdate, ibapi::Error>>,
    sink: &recovery::EventSink,
    api_client_id: i32,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
    pending_cancels: &PendingCancels,
    exec_buffer: &ExecutionBuffer,
) {
    use ibapi::orders::{OrderStatusKind, OrderUpdate};

    for update in updates {
        // Recovery ended the stream, or the consumer dropped it. Stop on this update rather than
        // the next one that would be forwarded, which may never come.
        if !sink.is_open() {
            return;
        }
        let update = match update {
            Ok(u) => u,
            Err(ibapi::Error::Shutdown) => {
                warn!("IBKR client shut down; terminating the account stream");
                sink.terminate(StreamTerminationReason::Error(
                    recovery::CLIENT_SHUT_DOWN.to_string(),
                ));
                return;
            }
            Err(e) => {
                // ibapi's own reconnect does not surface here (see the
                // recovery module), so an error on the subscription is terminal.
                // Surface it in-band as StreamTerminated(Error) so the caller
                // gets a programmatic signal rather than inferring EOF.
                // (best-effort — a no-op if the consumer already dropped rx.)
                error!(error = %e, "Order stream subscription error");
                sink.terminate(StreamTerminationReason::Error(e.to_string()));
                return;
            }
        };
        let event = match update {
            // IB numbers orders per API client, and `ibapi` copies here every order status an
            // open-orders listing carries, other clients' included.
            OrderUpdate::OrderStatus(status) if status.client_id != api_client_id => {
                trace!(
                    ib_order_id = status.order_id,
                    api_client_id = status.client_id,
                    "OrderStatus for another API client's order, dropping"
                );
                None
            }
            OrderUpdate::OrderStatus(status) => {
                let ib_id = status.order_id;
                // Single-lock methods for the terminal statuses, to avoid read+write. Each ends
                // the order, freeing its client id. Only `Cancelled`/`Inactive` remove its entry:
                // a `Filled` order's is retained so late-arriving ExecutionData/CommissionReport
                // events still resolve it (reaped later by `OrderIdMap::clear_stale`).
                let lookup_result = match status.status {
                    OrderStatusKind::Cancelled | OrderStatusKind::Inactive => {
                        order_ids.remove_and_get_context(ib_id)
                    }
                    OrderStatusKind::Filled => order_ids.release_client_id(ib_id),
                    _ => order_ids.get_client_id_and_context(ib_id),
                };

                match lookup_result {
                    // An order that already ended, whose id now names a later order: IB re-sent
                    // a status for it. Forwarded under the id, it would be read as the later
                    // order's, and a re-sent `Filled` would end that order.
                    Some((client_id, _)) if order_ids.names_other_order(&client_id, ib_id) => {
                        debug!(
                            ib_order_id = ib_id,
                            cid = %client_id,
                            "OrderStatus for an ended order whose client order id names a later order, dropping"
                        );
                        None
                    }
                    Some((client_id, ctx)) => {
                        let order =
                            make_order_from_status(&status, client_id, &ctx, pending_cancels);
                        Some(UnindexedAccountEvent {
                            exchange: ExchangeId::Ibkr,
                            kind: AccountEventKind::OrderSnapshot(Snapshot::new(order)),
                        })
                    }
                    None => {
                        debug!(ib_order_id = ib_id, "OrderStatus for unknown order ID");
                        None
                    }
                }
            }
            OrderUpdate::ExecutionData(exec) => {
                // ibapi copies an executions request's answers here too; they
                // are not fills happening now, and recovery emits its own.
                if recovery::is_replayed_execution(&exec) {
                    trace!(
                        exec_id = %exec.execution.execution_id,
                        request_id = exec.request_id,
                        "ExecutionData answering an executions request, skipping"
                    );
                    continue;
                }
                let Some(instrument) =
                    resolve_execution(&exec, api_client_id, contracts, order_ids)
                else {
                    continue;
                };
                // Logged on arrival, so the correction is seen even if no commission report
                // ever completes it.
                if ExecutionRevision::parse(&exec.execution.execution_id)
                    .is_some_and(|revision| revision.is_correction())
                {
                    warn!(
                        exec_id = %exec.execution.execution_id,
                        %instrument,
                        price = exec.execution.price,
                        shares = exec.execution.shares,
                        "IBKR corrected an execution; it will be reported as TradeAmended if its \
                         commission report arrives"
                    );
                }

                exec_buffer.add_execution(exec, instrument);
                None
            }
            OrderUpdate::CommissionReport(report) => {
                if let Some(trade) = exec_buffer.complete_with_commission(&report)
                    && !sink.send_execution(trade)
                {
                    return;
                }
                None
            }
            _ => None,
        };

        if let Some(e) = event
            && !sink.send(e)
        {
            // The consumer dropped rx, or recovery ended the stream: either
            // way nothing more may be sent.
            return;
        }
    }

    // The subscription iterator ended without an error (e.g. clean
    // disconnect/unsubscribe, or ibapi giving up reconnecting). Still a
    // terminal stream death — surface it in-band so the consumer doesn't have
    // to infer it from channel EOF.
    sink.terminate(StreamTerminationReason::Error(
        "IBKR order-update stream ended".to_string(),
    ));
}

/// The message a worker thread panicked with.
fn panic_message(panic_info: &(dyn std::any::Any + Send)) -> String {
    panic_info
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic_info.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Classify the error from a send (`place_order` / `cancel_order`) that failed
/// before TWS acknowledged anything, carrying `message` as the reason.
///
/// A dropped or reconnecting transport (`ConnectionReset`, `Io`) is a transient
/// [`OrderError::Connectivity`]: the request did not reach TWS and may be retried
/// once the connection is back. `Shutdown` and `ConnectionFailed` mean ibapi has
/// given up reconnecting, so they are non-transient, like any other refusal
/// ([`ApiError::OrderRejected`]).
///
/// # Known limitation
///
/// ibapi refuses a send with `ConnectionReset` whenever the session is not
/// connected, including after it has shut down for good, so a send refused on a
/// dead client still reads as transient here. The client's streams ending is
/// the signal that it will not come back.
fn send_error<AssetKey, InstrumentKey>(
    e: &ibapi::Error,
    message: String,
) -> OrderError<AssetKey, InstrumentKey> {
    if is_transient_transport_loss(e) {
        OrderError::Connectivity(ConnectivityError::Socket(message))
    } else {
        OrderError::Rejected(ApiError::OrderRejected(message))
    }
}

/// Drive an order-placement subscription to its initial [`PlacementOutcome`].
///
/// Shared by the single-order and bracket-leg placement paths. Consumes events
/// from a `place_order` subscription (typically `sub.timeout_iter_data(..)`)
/// until a decisive outcome is reached.
///
/// # Why notices are not blanket rejections
///
/// ibapi delivers a TWS error frame that it does not classify as a warning as
/// `Err(Error::Notice)`, and then closes the subscription. IB emits
/// informational order messages (e.g. code 399, order held until RTH) the same
/// way, so treating every `Err` as a rejection would falsely reject orders that
/// are actually live. We instead key off the notice code: known informational
/// codes yield [`PlacementOutcome::HeldPending`]; transport loss yields
/// [`PlacementOutcome::NoStatus`], because the order may already be live; all
/// other notices and errors yield [`PlacementOutcome::Rejected`]. The `OrderStatus`
/// event — when one is delivered before the closing notice — remains
/// authoritative.
///
/// ibapi 4.0 widened that warning classification beyond the `2100..=2169` range
/// to include code 0 and the `Warning:` form of code 399, and now routes a
/// warning owned by a request-bound subscription as a non-terminal notice that
/// `timeout_iter_data` drops rather than an `Err` that ends the stream. See
/// [`INFORMATIONAL_ORDER_CODES`] for what that changes here.
fn await_order_placement<I>(events: I) -> PlacementOutcome
where
    I: IntoIterator<Item = Result<ibapi::orders::PlaceOrder, ibapi::Error>>,
{
    use ibapi::orders::PlaceOrder;

    for event in events {
        let event = match event {
            Ok(event) => event,
            // Informational order message (e.g. 399 held-until-RTH): the order
            // is accepted; ibapi has closed the subscription, so stop here and
            // let the update stream carry the real status.
            Err(ibapi::Error::Notice(n)) if INFORMATIONAL_ORDER_CODES.contains(&n.code) => {
                return PlacementOutcome::HeldPending(format!("[{}] {}", n.code, n.message));
            }
            // The socket dropped before TWS reported a status. ibapi 4.2 fails
            // in-flight requests at the drop rather than after the reconnect, so
            // this can arrive after TWS received the order: its fate is unknown,
            // not rejected.
            Err(e) if is_transport_loss(&e) => {
                warn!(error = %e, "transport lost while awaiting order placement; status unknown");
                return PlacementOutcome::NoStatus;
            }
            // Genuine notice (e.g. 201 reject) or another ibapi error.
            Err(e) => return PlacementOutcome::Rejected(e.to_string()),
        };

        if let PlaceOrder::OrderStatus(status) = event {
            use ibapi::orders::OrderStatusKind;
            match status.status {
                OrderStatusKind::Submitted
                | OrderStatusKind::PreSubmitted
                | OrderStatusKind::PendingSubmit
                // A marketable order can fill before any working status is
                // delivered on the placement subscription — ibapi sends
                // `OrderStatus(Filled)` directly in that case. The order is
                // live, not rejected; its authoritative terminal/fill state
                // arrives via the account stream. Report it accepted so the
                // order-id mapping is retained and the stream's
                // executions/commissions are captured (treating it as a
                // rejection would remove the mapping and drop those events).
                | OrderStatusKind::Filled => {
                    return PlacementOutcome::Accepted {
                        filled: status.filled,
                    };
                }
                OrderStatusKind::Cancelled | OrderStatusKind::Inactive => {
                    return PlacementOutcome::Rejected(status.status.to_string());
                }
                // Transitional cancel states: a cancellation was already requested
                // (e.g. a fast cancel racing the placement ack), but the order was
                // submitted — we have a placement subscription — and is still live.
                // `ApiCancelled` always precedes a confirmed `Cancelled`; both it
                // and `PendingCancel` resolve to a terminal status on the account
                // stream, which is authoritative. Report accepted so the order-id
                // mapping is retained and the stream's terminal/execution/commission
                // events are captured (treating these as a rejection would remove
                // the mapping and drop those events).
                OrderStatusKind::ApiCancelled | OrderStatusKind::PendingCancel => {
                    return PlacementOutcome::Accepted {
                        filled: status.filled,
                    };
                }
                // Pre-transmission state: ibapi has not yet sent the order to IB
                // (e.g. awaiting a security-definition lookup). This is not a
                // placement decision — keep waiting for a definitive status. The
                // `PLACEMENT_STATUS_TIMEOUT` backstop yields `NoStatus` if none
                // arrives.
                OrderStatusKind::ApiPending => {}
                // TWS reported a status ibapi does not model. The order exists
                // — TWS is reporting on it — but neither we nor ibapi can
                // classify it: `is_active()` and `is_terminal()` are both false
                // for this variant. Report it live-but-indeterminate, which
                // retains the order-id mapping so the account stream can
                // resolve the real state.
                //
                // Deliberately NOT falling through to the `PLACEMENT_STATUS_TIMEOUT`
                // backstop the way `ApiPending` does: that yields `NoStatus`,
                // and on the bracket path `NoStatus` cancels all three legs —
                // a destructive response to a status we merely failed to
                // recognise.
                OrderStatusKind::Unknown(ref raw) => {
                    warn!(
                        ib_order_id = status.order_id,
                        status = raw.as_str(),
                        "unmodelled IBKR order status during placement; \
                         treating as live, account stream is authoritative"
                    );
                    return PlacementOutcome::HeldPending(format!(
                        "unmodelled order status: {raw}"
                    ));
                }
            }
        }
        // Non-status events (OpenOrder, ExecutionData, CommissionReport): keep
        // waiting for the OrderStatus.
    }

    PlacementOutcome::NoStatus
}

/// Interactive Brokers execution client.
///
/// # Clone Behavior
///
/// Cloning creates a shallow copy with shared `Arc` references to the underlying
/// IB connection, contract registry, order ID map, and execution buffer. All clones
/// share the same TWS/Gateway connection and state.
#[derive(Clone)]
pub struct IbkrClient {
    config: Arc<IbkrConfig>,
    client: Arc<Client>,
    contracts: ContractRegistry,
    order_ids: OrderIdMap,
    pending_cancels: PendingCancels,
    execution_buffer: ExecutionBuffer,
    next_order_id: Arc<Mutex<i32>>,
    /// The orders this client has seen live and not yet seen end, for the account stream's check
    /// of how they ended while it was disconnected.
    known_live: SharedKnownLiveOrders,
    /// Held across each read of IB's open-order and completed-order listings (see
    /// [`ListingLock`]).
    listings: ListingLock,
    /// The completions taken as an earlier order's under a reused client order id, each warned
    /// about once.
    earlier_completions: ended_orders::EarlierCompletions,
}

impl std::fmt::Debug for IbkrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbkrClient")
            .field("config", &self.config)
            .field("contracts_count", &self.contracts.len())
            .field("pending_orders", &self.order_ids.len())
            .finish()
    }
}

impl IbkrClient {
    /// Connect to TWS/Gateway and register every contract in [`IbkrConfig::contracts`]
    /// (sync, blocking).
    ///
    /// Each contract is built from its config, resolved as
    /// [`resolve_contract`](Self::resolve_contract) does and registered as
    /// [`register_contract`](Self::register_contract) does. If any of them fails, the
    /// connection is dropped and [`IbkrConnectError::Contracts`] lists every contract that
    /// failed, not just the first, so that a client never starts without a contract its caller
    /// configured. To connect anyway and get the failures back, use
    /// [`connect_sync_lenient`](Self::connect_sync_lenient).
    ///
    /// The library does not retry. If every contract in [`IbkrConnectError::Contracts`] has a
    /// [`reason`](SkippedContract::reason) that [`is_transient`](ContractSkipReason::is_transient),
    /// connecting again may succeed. Otherwise fix the config, or connect with
    /// [`connect_sync_lenient`](Self::connect_sync_lenient).
    ///
    /// # Errors
    ///
    /// - [`IbkrConnectError::Connect`] if TWS/Gateway cannot be reached or refuses the
    ///   connection.
    /// - [`IbkrConnectError::Contracts`] if any configured contract cannot be registered.
    pub fn connect_sync(config: IbkrConfig) -> Result<Self, IbkrConnectError> {
        let ConnectOutcome { client, skipped } = Self::connect_inner(config)?;
        if skipped.is_empty() {
            Ok(client)
        } else {
            // Dropping the only `ibapi` client closes the connection, freeing the client ID.
            drop(client);
            Err(IbkrConnectError::Contracts(skipped))
        }
    }

    /// Connect to TWS/Gateway as [`connect_sync`](Self::connect_sync) does, but without each
    /// configured contract that cannot be registered, returning those in
    /// [`ConnectOutcome::skipped`].
    ///
    /// Orders for a skipped contract's instrument are refused as for any unregistered
    /// instrument, so check `skipped`. To retry one, call
    /// [`resolve_contract`](Self::resolve_contract) and then
    /// [`register_contract`](Self::register_contract) on the connected client.
    ///
    /// Blocking, like [`connect_sync`](Self::connect_sync): it waits for the connection and for
    /// one contract details request per configured contract, each of which can take up to 10
    /// seconds if IB stops answering. From async code, call it inside
    /// `tokio::task::spawn_blocking`.
    ///
    /// # Errors
    ///
    /// Returns an error if TWS/Gateway cannot be reached or refuses the connection.
    pub fn connect_sync_lenient(
        config: IbkrConfig,
    ) -> Result<ConnectOutcome, UnindexedClientError> {
        Self::connect_inner(config)
    }

    /// Connect, then register each configured contract, collecting the ones that fail.
    fn connect_inner(config: IbkrConfig) -> Result<ConnectOutcome, UnindexedClientError> {
        let url = format!("{}:{}", config.host, config.port);
        info!(url = %url, client_id = config.client_id, "Connecting to IB");

        let client = Client::connect(&url, config.client_id).map_err(|e| {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
        })?;

        let next_id = client.next_order_id();

        let contracts = ContractRegistry::new();
        let skipped = connect::register_configured(&config.contracts, &contracts, |contract| {
            resolve_contract_blocking(&client, contract)
        });

        info!(
            contracts = contracts.len(),
            skipped = skipped.len(),
            next_order_id = next_id,
            "Connected to IB"
        );

        let client = Self {
            config: Arc::new(config),
            client: Arc::new(client),
            contracts,
            order_ids: OrderIdMap::new(),
            pending_cancels: PendingCancels::new(),
            execution_buffer: ExecutionBuffer::new(),
            next_order_id: Arc::new(Mutex::new(next_id)),
            known_live: KnownLiveOrders::shared(ExchangeId::Ibkr),
            listings: ListingLock::default(),
            earlier_completions: ended_orders::EarlierCompletions::new(),
        };
        Ok(ConnectOutcome { client, skipped })
    }

    /// Get the next order ID and increment the counter.
    fn allocate_order_id(&self) -> i32 {
        self.allocate_order_id_range(1)
    }

    /// Allocate a contiguous range of order IDs atomically.
    ///
    /// Returns the first ID in the range. Caller uses `base`, `base+1`, ..., `base+(count-1)`.
    ///
    /// This is essential for bracket orders which require consecutive IDs (parent=N,
    /// take_profit=N+1, stop_loss=N+2). A single lock acquisition ensures no other
    /// concurrent `open_order` call can grab an ID in the middle of the range.
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds `i32::MAX`, or if the resulting range would overflow `i32::MAX`.
    #[allow(clippy::expect_used)] // Panic is correct: i32::MAX orders means system is broken
    fn allocate_order_id_range(&self, count: u32) -> i32 {
        let count_i32: i32 = count.try_into().expect("count exceeds i32::MAX");
        let mut id = self.next_order_id.lock();
        let base = *id;
        *id = id
            .checked_add(count_i32)
            .expect("order ID overflow: i32::MAX exceeded");
        base
    }

    /// Resolve a contract description through IB's contract details, on this client's
    /// connection, into the one IB contract it matches.
    ///
    /// A contract built locally, such as `stock_contract("AAPL", "SMART", "USD")`, has no IB
    /// contract id until it is resolved, and [`register_contract`](Self::register_contract)
    /// refuses it. Resolve it here first:
    ///
    /// ```no_run
    /// # async fn example(client: rustrade_execution::client::ibkr::IbkrClient)
    /// # -> Result<(), Box<dyn std::error::Error>> {
    /// use rustrade_execution::client::ibkr::contract::stock_contract;
    ///
    /// let aapl = client
    ///     .resolve_contract(&stock_contract("AAPL", "SMART", "USD"))
    ///     .await?;
    /// client.register_contract("AAPL".into(), aapl)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`ResolveContractError::is_transient`] tells the failures worth retrying from the rest.
    ///
    /// - [`ResolveContractError::NoMatch`] when no IB contract matches the description.
    /// - [`ResolveContractError::Ambiguous`] when several do. Narrow the description, or pick
    ///   one of the matches it carries.
    /// - [`ResolveContractError::Refused`] when IB answers the request with another error, such
    ///   as a description it cannot validate. The same request is usually refused again; tell the
    ///   errors worth retrying apart by IB's `code`.
    /// - [`ResolveContractError::Connectivity`] when the connection drops
    ///   ([`ConnectivityError::Socket`]) or IB sends nothing for 10 s before finishing its answer
    ///   ([`ConnectivityError::Timeout`]). Transient: retry once `ibapi` has reconnected.
    /// - [`ResolveContractError::Failed`] when the request cannot be made or read for any other
    ///   reason, such as a client that has shut down or given up reconnecting.
    pub async fn resolve_contract(
        &self,
        contract: &ibapi::contracts::Contract,
    ) -> Result<ibapi::contracts::Contract, ResolveContractError> {
        let client = Arc::clone(&self.client);
        let contract = contract.clone();
        tokio::task::spawn_blocking(move || resolve_contract_blocking(&client, &contract))
            .await
            .map_err(|e| ResolveContractError::Failed(format!("task join: {e}")))?
    }

    /// Register a contract IB has resolved for an instrument, so that orders can be placed on
    /// it and everything IB reports for it (fills, order listings, positions) is attributed to
    /// `name`.
    ///
    /// Resolve the contract first with [`resolve_contract`](Self::resolve_contract), or list it
    /// in [`IbkrConfig::contracts`] for [`connect_sync`](Self::connect_sync) to resolve.
    ///
    /// # Errors
    ///
    /// Leaves the registry unchanged and returns:
    /// - [`ContractRegistryError::Unresolved`] when the contract has no IB contract id. IB
    ///   reports everything about a contract by that id, so without it the orders would fill
    ///   with no fill ever reported.
    /// - [`ContractRegistryError::ContractIdTaken`] when the contract is already registered
    ///   under another name.
    pub fn register_contract(
        &self,
        name: InstrumentNameExchange,
        contract: ibapi::contracts::Contract,
    ) -> Result<(), ContractRegistryError> {
        self.contracts.register(name, contract)
    }

    /// Get the contract registry.
    pub fn contract_registry(&self) -> &ContractRegistry {
        &self.contracts
    }

    /// Get the number of pending executions awaiting commission reports.
    ///
    /// Useful for monitoring whether commission reports are being received.
    /// A growing count may indicate IB connection issues or delayed reports.
    pub fn pending_execution_count(&self) -> usize {
        self.execution_buffer.pending_count()
    }

    /// Clear stale executions older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// Call this periodically to prevent unbounded memory growth if commission
    /// reports are delayed or lost. A reasonable interval is 5-10 minutes with
    /// a max_age of 1 hour.
    pub fn clear_stale_executions(&self, max_age: std::time::Duration) -> usize {
        self.execution_buffer.clear_stale(max_age)
    }

    /// Clear order ID mappings older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// # Why This Is Needed
    ///
    /// IB does not guarantee event ordering between `OrderStatus("Filled")` and
    /// `ExecutionData`/`CommissionReport`. For fast-filling orders (especially
    /// market orders), execution data may arrive AFTER the filled status — or
    /// the filled status may not arrive at all. So a `Filled` status frees the
    /// order's client order id but keeps its IB order ID's entry, and an order
    /// whose terminal status never arrives keeps both: its id cannot name a new
    /// order (see `open_order`'s "Client order ids") until this clears it.
    ///
    /// Call this periodically alongside `clear_stale_executions()`. A reasonable
    /// interval is 5-10 minutes with a max_age of 1 hour. An order still working
    /// past `max_age` is cleared too, and its id freed.
    pub fn clear_stale_order_ids(&self, max_age: std::time::Duration) -> usize {
        self.order_ids.clear_stale(max_age)
    }

    /// Clear pending cancel entries older than the given duration.
    ///
    /// Returns the number of cleared entries.
    ///
    /// Pending cancels are tracked to differentiate user-initiated cancellation
    /// from time-based expiration. If a cancel request is submitted but the order
    /// never receives terminal status (e.g., network disconnect), the entry would
    /// remain indefinitely. Call this alongside other stale cleanup methods.
    pub fn clear_stale_pending_cancels(&self, max_age: std::time::Duration) -> usize {
        self.pending_cancels.clear_stale(max_age)
    }

    /// Disconnect from IB Gateway.
    ///
    /// Shuts the `ibapi` client down for every clone sharing it: requests in flight fail, any
    /// active `account_stream()` ends with `StreamTerminated`, and later requests are refused.
    ///
    /// # Client ID release
    ///
    /// This does not release the API client ID. `ibapi` keeps the TCP connection open until the
    /// last clone of this client is dropped ([`IbkrClient`] implements [`Clone`] and shares one
    /// connection), and TWS/Gateway treats the ID as in use while the connection is open. To
    /// reconnect under the same ID, drop every clone first; otherwise
    /// [`connect_sync`](Self::connect_sync) is refused with IB error 326 ("client id is already in
    /// use").
    ///
    /// This is idempotent — calling it multiple times is safe.
    pub fn disconnect(&self) {
        debug!("Disconnecting IbkrClient");
        self.client.disconnect();
    }

    /// Place a bracket order (entry + take-profit + stop-loss) with OCA linking.
    ///
    /// A bracket order consists of three linked orders:
    /// 1. **Entry**: Limit order to enter the position
    /// 2. **Take Profit**: Limit order to exit at profit target (OCA-linked to SL)
    /// 3. **Stop Loss**: Stop order to exit at loss limit (OCA-linked to TP)
    ///
    /// # OCA (One-Cancels-All) Behavior
    ///
    /// When either exit order fills, IB automatically cancels the other. This
    /// prevents the dangerous scenario where take-profit fills but stop-loss
    /// remains open, potentially opening an unintended opposing position.
    ///
    /// # Failure Semantics
    ///
    /// If any leg is rejected by IB (e.g., insufficient margin, invalid price),
    /// or cannot be sent, this method cancels every leg already sent and
    /// returns the legs as `Inactive`, except for legs of unknown fate (below).
    ///
    /// The same all-legs cancellation applies if any leg fails to report a
    /// status within the placement timeout, or the transport drops while
    /// waiting (an unknown/no-status outcome): leaving part of a bracket
    /// working while the rest is in an unknown state is unsafe, so every leg is
    /// cancelled and an error is returned.
    ///
    /// If any leg's client order id (the parent's, or the `_tp`/`_sl` id
    /// derived from it) is held by a live order, nothing is sent and every leg
    /// comes back `Inactive` with [`ApiError::DuplicateClientOrderId`]; the live
    /// order is unaffected. See `ExecutionClient::open_order`'s "Client order
    /// ids" for when an id is held.
    ///
    /// # Legs of Unknown Fate
    ///
    /// Cancelling does not settle a leg whose fate is unknown: a leg that
    /// reported no status, or whose rollback cancel could not be sent (ibapi
    /// refuses sends while its transport is down), may still be live or held
    /// at TWS. Such a leg comes back `Open` with zero fill and keeps its
    /// order-id mapping, as a no-status single order does, so the account
    /// stream can report how it ends. The other legs come back `Inactive` with
    /// the error. A leg IB rejected is settled even if its cancel was refused.
    /// So a failed bracket can return a mix of `Open` and `Inactive` legs, and
    /// every leg can be `Open` when none reported a status: an all-`Open`
    /// result is then indistinguishable from success, and the failure shows
    /// only in the log and on the account stream. Treat an `Open` leg as
    /// working until the account stream or
    /// [`ExecutionClient::fetch_open_orders`] says otherwise.
    ///
    /// # Cancellation Safety
    ///
    /// This future registers order ID mappings before submitting to IB. If
    /// cancelled mid-flight, orders may still be submitted and mappings will
    /// leak. Avoid cancelling this future; use IB's native order timeout via
    /// `TimeInForce` if timeout behavior is needed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = BracketOrderRequest {
    ///     instrument: "AAPL".into(),
    ///     strategy: StrategyId::new("my-strategy"),
    ///     parent_cid: ClientOrderId::new("bracket-001"),
    ///     side: Side::Buy,
    ///     quantity: dec!(100),
    ///     entry_price: dec!(150.00),
    ///     take_profit_price: dec!(160.00),  // +$10 profit target
    ///     stop_loss_price: dec!(145.00),    // -$5 stop loss
    ///     time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
    /// };
    /// let result = client.open_bracket_order(request).await;
    /// ```
    pub async fn open_bracket_order(&self, request: BracketOrderRequest) -> BracketOrderResult {
        let instrument = request.instrument.clone();

        // Look up contract
        let contract = match self.contracts.get_contract(&instrument) {
            Some(c) => c,
            None => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::InstrumentInvalid(
                        instrument,
                        "contract not registered".to_string(),
                    )),
                );
            }
        };

        // Convert quantity to f64
        let quantity: f64 = match request.quantity.try_into() {
            Ok(q) => q,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "quantity {} exceeds f64 range",
                        request.quantity
                    ))),
                );
            }
        };

        // Convert prices to f64
        let entry_price: f64 = match request.entry_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "entry_price {} exceeds f64 range",
                        request.entry_price
                    ))),
                );
            }
        };
        let tp_price: f64 = match request.take_profit_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "take_profit_price {} exceeds f64 range",
                        request.take_profit_price
                    ))),
                );
            }
        };
        let sl_price: f64 = match request.stop_loss_price.try_into() {
            Ok(p) => p,
            Err(_) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "stop_loss_price {} exceeds f64 range",
                        request.stop_loss_price
                    ))),
                );
            }
        };

        // Validate time_in_force and convert to IB wire format
        let ib_tif = match time_in_force_to_ib(&request.time_in_force) {
            Ok(tif) => tif,
            Err(e) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "TIF not supported by IB: {e}"
                    ))),
                );
            }
        };

        // Allocate 3 consecutive order IDs atomically
        let parent_ib_id = self.allocate_order_id_range(3);
        let tp_ib_id = parent_ib_id + 1;
        let sl_ib_id = parent_ib_id + 2;

        // Build bracket orders with OCA linking
        let action = side_to_action(request.side);
        // Nothing has been sent yet, so on an error the three allocated IDs are simply skipped.
        let mut ib_orders = match build_ib_bracket_with_oca(
            parent_ib_id,
            action,
            quantity,
            entry_price,
            tp_price,
            sl_price,
            ib_tif,
        ) {
            Ok(orders) => orders,
            Err(e) => {
                return make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(e.to_string())),
                );
            }
        };

        // Generate client order IDs for children
        let parent_cid = request.parent_cid.clone();
        let (tp_cid, sl_cid) = derive_child_cids(&parent_cid);
        // Each leg carries its own client order id as its order reference.
        for (ib_order, cid) in ib_orders.iter_mut().zip([&parent_cid, &tp_cid, &sl_cid]) {
            match order::order_ref(cid) {
                Ok(order_ref) => ib_order.order_ref = order_ref,
                Err(e) => {
                    return make_all_inactive_bracket(
                        &request,
                        OrderError::Rejected(ApiError::OrderRejected(e.to_string())),
                    );
                }
            }
        }

        // Determine opposite side for exit orders
        let exit_side = match request.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        // Register order contexts for all three legs
        let parent_ctx = OrderContext {
            instrument: instrument.clone(),
            side: request.side,
            price: Some(request.entry_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
        };
        let tp_ctx = OrderContext {
            instrument: instrument.clone(),
            side: exit_side,
            price: Some(request.take_profit_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
        };
        let sl_ctx = OrderContext {
            instrument: instrument.clone(),
            side: exit_side,
            price: None, // Stop orders don't have a limit price
            quantity: request.quantity,
            kind: OrderKind::Stop {
                trigger_price: request.stop_loss_price,
            },
            time_in_force: request.time_in_force,
        };

        // All three or none, before anything is sent: a leg under an id a live order holds would
        // take that order's mapping. On a refusal the three allocated IDs are simply skipped.
        if let Err(in_use) = self.order_ids.register_all([
            (parent_cid.clone(), parent_ib_id, parent_ctx),
            (tp_cid.clone(), tp_ib_id, tp_ctx),
            (sl_cid.clone(), sl_ib_id, sl_ctx),
        ]) {
            return make_all_inactive_bracket(
                &request,
                OrderError::Rejected(ApiError::DuplicateClientOrderId(in_use.to_string())),
            );
        }
        {
            let mut known = self.known_live.lock();
            for cid in [&parent_cid, &tp_cid, &sl_cid] {
                known.placing(cid);
            }
        }

        // Place all three orders in spawn_blocking
        let client = self.client.clone();
        let result = tokio::task::spawn_blocking(move || {
            // Roll back legs already sent. Returns the ids whose cancel could not
            // be sent, and a suffix for the error message naming them. A refused
            // cancel must not vanish: ibapi refuses sends while the transport is
            // down, and the leg it names may then be live at TWS.
            let rollback = |ids: &[i32]| -> (Vec<i32>, String) {
                let failed: Vec<(i32, ibapi::Error)> = ids
                    .iter()
                    .filter_map(|&id| {
                        let e = client.cancel_order(id, "").err()?;
                        error!(order_id = id, error = %e, "bracket rollback cancel failed; leg may be live");
                        Some((id, e))
                    })
                    .collect();
                if failed.is_empty() {
                    return (Vec::new(), String::new());
                }
                let named: Vec<String> = failed.iter().map(|(id, e)| format!("{id} ({e})")).collect();
                let suffix = format!(
                    "; rollback cancel failed for order ids {}, which may be live",
                    named.join(", ")
                );
                (failed.into_iter().map(|(id, _)| id).collect(), suffix)
            };

            // A later leg's send failed after the legs in `sent` were written:
            // roll them back. A leg whose cancel was refused may be held at TWS,
            // so it is unresolved. The failing leg itself is treated as never
            // having reached TWS, as in `send_error`.
            let leg_send_failure = |e: &ibapi::Error, message: String, sent: &[i32]| {
                let (cancel_failed, rollback) = rollback(sent);
                BracketFailure {
                    error: leg_send_error(e, message, &rollback),
                    unresolved: cancel_failed,
                }
            };

            // Place parent (transmit=false, held until SL is sent)
            let parent_sub = match client.place_order(parent_ib_id, &contract, &ib_orders[0]) {
                Ok(s) => s,
                Err(e) => {
                    return Err(BracketFailure {
                        error: send_error(&e, format!("parent order failed: {e}")),
                        unresolved: Vec::new(),
                    });
                }
            };

            // Place take-profit (transmit=false)
            let tp_sub = match client.place_order(tp_ib_id, &contract, &ib_orders[1]) {
                Ok(s) => s,
                Err(e) => {
                    return Err(leg_send_failure(
                        &e,
                        format!("take_profit order failed: {e}"),
                        &[parent_ib_id],
                    ));
                }
            };

            // Place stop-loss (transmit=true, triggers all)
            let sl_sub = match client.place_order(sl_ib_id, &contract, &ib_orders[2]) {
                Ok(s) => s,
                Err(e) => {
                    return Err(leg_send_failure(
                        &e,
                        format!("stop_loss order failed: {e}"),
                        &[parent_ib_id, tp_ib_id],
                    ));
                }
            };

            // Await the first status from each subscription. A leg held until
            // RTH (informational notice, e.g. 399) is treated as live with an
            // unknown fill (0.0) — the order is working, not rejected. `None`
            // means the subscription closed/timed out without a terminal status.
            let leg_status = |outcome: PlacementOutcome, leg: &str| match outcome {
                PlacementOutcome::Accepted { filled } => Some(Ok(filled)),
                PlacementOutcome::HeldPending(notice) => {
                    warn!(leg, %notice, "bracket leg held/pending; tracking via stream");
                    Some(Ok(0.0))
                }
                PlacementOutcome::Rejected(reason) => Some(Err(reason)),
                PlacementOutcome::NoStatus => None,
            };

            let parent_status = leg_status(
                await_order_placement(parent_sub.timeout_iter_data(PLACEMENT_STATUS_TIMEOUT)),
                "parent",
            );
            let tp_status = leg_status(
                await_order_placement(tp_sub.timeout_iter_data(PLACEMENT_STATUS_TIMEOUT)),
                "take_profit",
            );
            let sl_status = leg_status(
                await_order_placement(sl_sub.timeout_iter_data(PLACEMENT_STATUS_TIMEOUT)),
                "stop_loss",
            );

            // Verify all three legs reported a successful status. A `None` here
            // means the subscription closed without producing a recognised event
            // — surface that as an error rather than silently treating it as
            // success. Any error or missing status cancels all legs.
            match (parent_status, tp_status, sl_status) {
                (Some(Ok(parent)), Some(Ok(tp)), Some(Ok(sl))) => Ok((parent, tp, sl)),
                (parent, tp, sl) => {
                    let (cancel_failed, rollback) = rollback(&[parent_ib_id, tp_ib_id, sl_ib_id]);
                    let unresolved = unresolved_legs(
                        [(parent_ib_id, &parent), (tp_ib_id, &tp), (sl_ib_id, &sl)],
                        &cancel_failed,
                    );
                    let show = |status: Option<Result<f64, String>>| match status {
                        Some(status) => format!("{status:?}"),
                        None => "no status (timed out or transport lost)".to_string(),
                    };
                    Err(BracketFailure {
                        error: OrderError::Rejected(ApiError::OrderRejected(format!(
                            "bracket order failed: parent={}, tp={}, sl={}{rollback}",
                            show(parent),
                            show(tp),
                            show(sl)
                        ))),
                        unresolved,
                    })
                }
            }
        })
        .await;

        // Recorded only once sent, as for a single order.
        let result = match result {
            Ok(Ok((parent_filled, tp_filled, sl_filled))) => {
                let now = Utc::now();
                let parent_filled_dec = parse_decimal_or_warn(parent_filled, "parent.filled");
                let tp_filled_dec = parse_decimal_or_warn(tp_filled, "tp.filled");
                let sl_filled_dec = parse_decimal_or_warn(sl_filled, "sl.filled");

                BracketOrderResult {
                    parent: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: parent_cid,
                        },
                        side: request.side,
                        price: Some(request.entry_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            VenueOrderId::Assigned(OrderId::new(format_smolstr!(
                                "{}",
                                parent_ib_id
                            ))),
                            now,
                            parent_filled_dec,
                        )),
                    },
                    take_profit: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: tp_cid,
                        },
                        side: exit_side,
                        price: Some(request.take_profit_price),
                        quantity: request.quantity,
                        kind: OrderKind::Limit,
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            VenueOrderId::Assigned(OrderId::new(format_smolstr!("{}", tp_ib_id))),
                            now,
                            tp_filled_dec,
                        )),
                    },
                    stop_loss: Order {
                        key: OrderKey {
                            exchange: ExchangeId::Ibkr,
                            instrument: instrument.clone(),
                            strategy: request.strategy.clone(),
                            cid: sl_cid,
                        },
                        side: exit_side,
                        price: None, // Stop orders don't have a limit price
                        quantity: request.quantity,
                        kind: OrderKind::Stop {
                            trigger_price: request.stop_loss_price,
                        },
                        time_in_force: request.time_in_force,
                        state: OrderState::active(Open::new(
                            VenueOrderId::Assigned(OrderId::new(format_smolstr!("{}", sl_ib_id))),
                            now,
                            sl_filled_dec,
                        )),
                    },
                }
            }
            Ok(Err(failure)) => failed_bracket(
                &request,
                [parent_ib_id, tp_ib_id, sl_ib_id],
                failure,
                &self.order_ids,
            ),
            Err(join_err) => {
                // The placement task panicked, and which legs it sent was lost
                // with it. Unlike `failed_bracket`, all three are released: a
                // leg it did send is then untracked, but keeping unsent legs
                // Open would report orders that do not exist.
                self.order_ids.remove_by_ib_id(parent_ib_id);
                self.order_ids.remove_by_ib_id(tp_ib_id);
                self.order_ids.remove_by_ib_id(sl_ib_id);

                make_all_inactive_bracket(
                    &request,
                    OrderError::Rejected(ApiError::OrderRejected(format!(
                        "task join error: {join_err}"
                    ))),
                )
            }
        };
        let mut known = self.known_live.lock();
        for leg in [&result.parent, &result.take_profit, &result.stop_loss] {
            known.placed(&leg.key, leg.quantity, leg.kind, &leg.state);
        }
        drop(known);
        result
    }
}

/// Derive bracket child CIDs from the parent CID.
///
/// Convention: `{parent}_tp` for take-profit, `{parent}_sl` for stop-loss.
/// All bracket call sites must use this helper to keep the naming consistent.
fn derive_child_cids(parent_cid: &ClientOrderId) -> (ClientOrderId, ClientOrderId) {
    (
        ClientOrderId::new(format_smolstr!("{}_tp", parent_cid.0)),
        ClientOrderId::new(format_smolstr!("{}_sl", parent_cid.0)),
    )
}

/// A failed bracket placement: the error each settled leg carries, and the legs
/// whose fate at TWS is unknown.
struct BracketFailure {
    error: OrderError<AssetNameExchange, InstrumentNameExchange>,
    /// IB order ids of legs that may be live at TWS: no placement status
    /// arrived for them, or their rollback cancel could not be sent.
    unresolved: Vec<i32>,
}

/// The legs of a bracket that failed after all three were sent whose fate at
/// TWS is unknown: those without a placement status (`None`), and those that
/// were accepted but whose rollback cancel could not be sent.
///
/// A rejected leg (`Some(Err)`) is settled whether or not its cancel went out:
/// its terminal status was consumed by the placement subscription and will not
/// be replayed, so keeping it `Open` would leave a phantom order. An accepted
/// leg whose cancel went out is settled as being cancelled.
fn unresolved_legs<T, E>(
    legs: [(i32, &Option<Result<T, E>>); 3],
    cancel_failed: &[i32],
) -> Vec<i32> {
    legs.into_iter()
        .filter(|(id, status)| match status {
            None => true,
            Some(Ok(_)) => cancel_failed.contains(id),
            Some(Err(_)) => false,
        })
        .map(|(id, _)| id)
        .collect()
}

/// Classify a later bracket leg's send failure, after the legs sent before it
/// were rolled back. `rollback` is the suffix naming the cancels that could
/// not be sent, empty when every cancel went out.
///
/// Retrying is only safe when every rollback cancel went out, so only then does
/// the failure keep [`send_error`]'s classification. Otherwise a leg may still
/// be held at TWS, and the failure is a non-transient rejection naming it.
fn leg_send_error(
    e: &ibapi::Error,
    message: String,
    rollback: &str,
) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
    if rollback.is_empty() {
        send_error(e, message)
    } else {
        OrderError::Rejected(ApiError::OrderRejected(format!("{message}{rollback}")))
    }
}

/// Build the result of a failed bracket placement, releasing the order-id
/// mappings of its settled legs.
///
/// A leg in `failure.unresolved` comes back `Open` with zero fill and keeps its
/// mapping, as single-order placement does for an unknown outcome, so the
/// account stream can still resolve it. Every other leg was never sent, was
/// rejected, or had its cancel sent: it comes back `Inactive` with
/// `failure.error`, and its mapping is removed.
fn failed_bracket(
    request: &BracketOrderRequest,
    [parent_ib_id, tp_ib_id, sl_ib_id]: [i32; 3],
    failure: BracketFailure,
    order_ids: &OrderIdMap,
) -> BracketOrderResult {
    let BracketFailure { error, unresolved } = failure;
    if !unresolved.is_empty() {
        warn!(
            %error,
            ?unresolved,
            "bracket placement failed with legs of unknown fate; returning them Open, tracking via stream"
        );
    }

    let mut result = make_all_inactive_bracket(request, error);
    let now = Utc::now();
    for (leg, ib_id) in [
        (&mut result.parent, parent_ib_id),
        (&mut result.take_profit, tp_ib_id),
        (&mut result.stop_loss, sl_ib_id),
    ] {
        if unresolved.contains(&ib_id) {
            leg.state = OrderState::active(Open::new(
                VenueOrderId::Assigned(OrderId::new(format_smolstr!("{ib_id}"))),
                now,
                Decimal::ZERO,
            ));
        } else {
            order_ids.remove_by_ib_id(ib_id);
        }
    }
    result
}

/// Helper to create a BracketOrderResult with all legs inactive (same error).
fn make_all_inactive_bracket(
    request: &BracketOrderRequest,
    error: OrderError<AssetNameExchange, InstrumentNameExchange>,
) -> BracketOrderResult {
    let exit_side = match request.side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    };

    let parent_cid = request.parent_cid.clone();
    let (tp_cid, sl_cid) = derive_child_cids(&parent_cid);

    BracketOrderResult {
        parent: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: parent_cid,
            },
            side: request.side,
            price: Some(request.entry_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error.clone()),
        },
        take_profit: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: tp_cid,
            },
            side: exit_side,
            price: Some(request.take_profit_price),
            quantity: request.quantity,
            kind: OrderKind::Limit,
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error.clone()),
        },
        stop_loss: Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: request.instrument.clone(),
                strategy: request.strategy.clone(),
                cid: sl_cid,
            },
            side: exit_side,
            price: None, // Stop orders don't have a limit price
            quantity: request.quantity,
            kind: OrderKind::Stop {
                trigger_price: request.stop_loss_price,
            },
            time_in_force: request.time_in_force,
            state: OrderState::inactive(error),
        },
    }
}

impl ExecutionClient for IbkrClient {
    const EXCHANGE: ExchangeId = ExchangeId::Ibkr;

    // Mirrors the security types `ContractConfig::to_contract` can build: `STK` and `CASH` (forex)
    // are both outright holdings and so map to `Spot`, `FUT` to `Future`, `OPT` to `Option`.
    //
    // `Cfd` is absent deliberately. IBKR does offer CFDs, but this client builds no `SecurityType`
    // for them, so a CFD instrument would fail `to_contract` with `UnrecognizedSecurityType` at
    // registration rather than trade.
    //
    // Scope: this declares which `InstrumentKind`s the client can represent AT ALL. Whether each
    // `ContractConfig` describes the instrument it is keyed to is `validate_config`'s check.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[
        InstrumentKindDiscriminant::Spot,
        InstrumentKindDiscriminant::Future,
        InstrumentKindDiscriminant::Option,
    ];

    type Config = IbkrConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Rejects a `ContractConfig` that [`connect_sync`](Self::connect_sync) would refuse as invalid,
    /// or whose `security_type` contradicts the kind of the instrument it is keyed to by `name`:
    /// `STK` or `CASH` for a `Spot`, `FUT` for a `Future`, `OPT` for an `Option`. Every problem
    /// found is reported, not just the first.
    ///
    /// `Spot` covers both a stock and a currency pair, so `STK` and `CASH` cannot be told apart
    /// here: either is accepted for any `Spot` instrument.
    ///
    /// An entry keyed to no instrument on this exchange, and an instrument with no entry, are both
    /// accepted: a contract can also be registered later with
    /// [`register_contract`](Self::register_contract), which this cannot see.
    fn validate_config(
        config: &Self::Config,
        instruments: &[ClientInstrument<'_>],
    ) -> Result<(), String> {
        let problems = config
            .contracts
            .iter()
            .filter_map(|contract| {
                if let Err(error) = contract.to_contract() {
                    return Some(format!("contract {:?}: {error}", contract.name));
                }
                let instrument = instruments
                    .iter()
                    .find(|instrument| instrument.name_exchange.as_ref() == contract.name)?;
                let expected = security_types_of(instrument.kind);
                if expected.is_empty() {
                    // Unreachable through `ExecutionBuilder`, which checks `SUPPORTED_KINDS` first,
                    // but a direct caller can pass any kind.
                    return Some(format!(
                        "contract {:?} is for an instrument of kind {}, which this client cannot \
                         trade",
                        contract.name, instrument.kind,
                    ));
                }
                (!expected.contains(&contract.security_type.as_str())).then(|| {
                    format!(
                        "contract {:?} names security type {:?}, but its instrument's kind is \
                         {}, which takes {}",
                        contract.name,
                        contract.security_type,
                        instrument.kind,
                        expected.join(" or "),
                    )
                })
            })
            .collect::<Vec<_>>();

        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("; "))
        }
    }

    /// Create a new IBKR client by connecting to TWS/Gateway.
    ///
    /// # Blocking
    ///
    /// This method performs blocking TCP I/O and IB API handshake. If called
    /// from an async context, wrap in `tokio::task::spawn_blocking` or call
    /// from a dedicated thread.
    ///
    /// # Panics
    ///
    /// Panics if [`IbkrClient::connect_sync`] fails: when TWS/Gateway cannot be reached or
    /// refuses the connection, or when any contract in [`IbkrConfig::contracts`] cannot be
    /// registered. The `ExecutionClient` trait doesn't allow `new()` to return `Result`. Call
    /// [`IbkrClient::connect_sync`] or [`IbkrClient::connect_sync_lenient`] directly for
    /// fallible construction.
    #[track_caller]
    fn new(config: Self::Config) -> Self {
        #[allow(clippy::expect_used)] // Trait signature doesn't allow Result
        Self::connect_sync(config)
            .expect("failed to connect to IB or to register its configured contracts")
    }

    /// Fetch account snapshot: balances, and the position and open orders of each registered
    /// instrument IB reports either in.
    ///
    /// # Positions
    ///
    /// Each instrument IB reports a position or an open order of this API client's in, and that
    /// is registered (and in `instruments`, when that is not empty), gets an
    /// `InstrumentAccountSnapshot`. Its `position` is
    /// [`PositionReport::Open`], carrying:
    /// - `quantity`: IB's signed position, negative when short, in shares for a stock and in
    ///   contracts for a future or an option. `ibapi` hands it over as an `f64`, converted
    ///   with `Decimal::try_from`, which rounds to the float's precision of about 15 significant
    ///   digits (0.1 stays 0.1) rather than keeping its exact binary expansion.
    /// - `entry_price`: IB's average cost divided by the contract multiplier, so a future or an
    ///   option is quoted as its orders are priced. It includes commissions. `None` when IB sends
    ///   no average cost, or no multiplier for a contract other than a stock or a forex pair.
    /// - `time_exchange`: the time of the call, since IB does not timestamp positions.
    ///
    /// `unrealized_pnl` and the margin fields are `None`: IB's positions subscription does not
    /// carry them.
    ///
    /// A zero quantity, which IB reports for a position closed today, is
    /// [`PositionReport::Flat`]. So is each instrument in
    /// `instruments` that is registered with its contract ID but that IB did not list, provided IB
    /// marked the end of its listing (`PositionEnd`) during the read. Without that marker such
    /// instruments are left out, with a warning, since the listing may be incomplete.
    /// A requested instrument registered without a contract ID, or whose ID was registered again
    /// under another name, cannot be matched to IB's reports and is left out too.
    ///
    /// **Several accounts.** This client reads the positions of every account the login manages,
    /// one account at a time. When more than one account holds the same instrument, the first account to report a
    /// non-zero quantity is kept and the others are dropped with a warning; they are never summed.
    /// A consumer comparing the reported quantity with its own, as the `rustrade` engine's position
    /// drift check does, therefore compares against that one account.
    /// Which account comes first depends on the order IB lists the login's accounts in, so a
    /// caller holding the same instrument in several accounts should not rely on it.
    ///
    /// An instrument with an open order whose position the read could not establish, because IB
    /// did not mark the end of its listing, reports it
    /// [`PositionReport::Unreported`].
    ///
    /// # Orders
    ///
    /// Each instrument's `orders` are this API client's open orders in it, read as
    /// [`fetch_open_orders`](ExecutionClient::fetch_open_orders) reads them, from one listing of
    /// the account's open orders. An order this client does not track but placed under a client
    /// order id, such as one placed before a restart, is listed under that id, and tracked from
    /// then on.
    ///
    /// `orders_complete` is `true` when IB listed every open order to the end of the listing and
    /// each of the instrument's was read back under the client order id it was placed with and
    /// with its status. So it is `false` on an instrument with an order of this API client's
    /// that carries no client order id, which is listed under its IB order id: one placed by an
    /// earlier version of this client, or, for a client connected as 0, one entered in TWS. So it
    /// is too with an order whose shape this client never sends, one IB listed without its
    /// status, or one whose client order id names another order this client tracks. When the
    /// listing cannot be read, no instrument's orders are complete, with a warning, and the
    /// snapshot is still returned.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if a position quantity does not convert to a
    /// `Decimal`, if the request for the login's accounts fails or yields none (as it does when the
    /// session ends during it), if a positions subscription fails, or as
    /// [`fetch_balances`](ExecutionClient::fetch_balances).
    ///
    /// # Known Issue: ibapi Decode Errors
    ///
    /// You may see log errors like `"error decoding message: error occurred:
    /// unexpected message: Error"`. This is an upstream ibapi limitation where
    /// IB Error messages (type 4) on subscription channels aren't properly
    /// routed — they're logged but don't affect functionality.
    ///
    /// # Timeout
    ///
    /// Each account's positions read ends at IB's end-of-listing marker. If IB sends
    /// nothing for 5 seconds before it, that account's read gives up and the next
    /// one starts, so a login with N accounts can wait up to N × 5 seconds. The
    /// positions received are returned rather than blocking indefinitely, without
    /// the flat reports that need every account's listing to be complete.
    ///
    /// Each read uses IB's positions-multi request for that account, whose replies
    /// carry a request ID, so concurrent calls on clones of this client never read
    /// each other's listing.
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // H-2 fix: Run balances and positions concurrently (independent IB requests)
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        let balances_future = self.fetch_balances(assets);
        let positions_filter = instruments_filter.clone();
        let positions_future = tokio::task::spawn_blocking(move || {
            let instruments_filter = positions_filter;
            // ibapi::Error is unstructured — we cannot distinguish connection failures
            // (transient, should retry) from API errors (e.g., invalid request).
            // Mapped to Internal (non-transient) conservatively; a caller needing reconnect
            // logic should drive it from connection state, not from the error type.
            let accounts = client
                .managed_accounts()
                .map_err(|e| UnindexedClientError::Internal(format!("managed accounts: {e}")))?;
            if accounts.is_empty() {
                // `ibapi` also answers with no accounts when the session ends mid-request.
                return Err(UnindexedClientError::Internal(
                    "no managed accounts: IB listed none, or the session ended".to_string(),
                ));
            }

            let mut positions = PositionAggregator::default();
            let mut listed_all = true;
            for account in accounts {
                listed_all &= read_account_positions(
                    &client,
                    AccountId(account),
                    &contracts,
                    instruments_filter.as_ref(),
                    &mut positions,
                )?;
            }
            if listed_all {
                positions.listing_ended();
            }

            // IB's reports are attributed by contract ID, so a requested instrument can be
            // reported flat only if its ID is registered and resolves back to it: with no ID, or
            // one another name took over, IB's position in it would be missed.
            let requested: Vec<_> = instruments_filter
                .iter()
                .flatten()
                .filter(|instrument| {
                    contracts.get_contract(instrument).is_some_and(|contract| {
                        contract.contract_id != 0
                            && contracts.get_name_by_con_id(contract.contract_id).as_ref()
                                == Some(*instrument)
                    })
                })
                .cloned()
                .collect();
            Ok::<_, UnindexedClientError>((positions, requested))
        });

        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        let listings = self.listings.clone();
        let orders_future = tokio::task::spawn_blocking(move || {
            let listing = listings.open_orders(&client)?;
            Ok::<_, UnindexedClientError>(open_orders_from_listing(
                listing,
                client.client_id(),
                &contracts,
                &order_ids,
            ))
        });

        // ibapi routes responses by request_id via thread-safe channels (RwLock<HashMap>),
        // so concurrent requests on the same client are safe. The open-order listing is a shared
        // request, serialized by `ListingLock`.
        let (balances_result, positions_result, orders_result) =
            tokio::join!(balances_future, positions_future, orders_future);
        let balances = balances_result?;
        let (positions, requested) = positions_result
            .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))??;
        let listed = match orders_result
            .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
        {
            Ok(mut listed) => {
                if let Some(filter) = &instruments_filter {
                    listed.retain(|listed| filter.contains(&listed.instrument));
                }
                Some(listed)
            }
            Err(error) => {
                warn!(
                    %error,
                    "Could not read IBKR's open orders for the account snapshot; no instrument's \
                     orders are reported complete"
                );
                None
            }
        };
        if let Some(listed) = &listed {
            remember_listed(&self.known_live, listed);
        }
        let mut orders = SnapshotOrders::new(listed);

        // An instrument with open orders is attributed by contract ID too, so its position is
        // reported along with them.
        let reported = positions.into_reports(
            Utc::now(),
            requested
                .iter()
                .chain(orders.instruments())
                .collect::<HashSet<_>>(),
        )?;
        let mut instrument_snapshots = Vec::with_capacity(reported.len());
        for (instrument, position) in reported {
            let (orders, orders_complete) = orders.take(&instrument);
            instrument_snapshots.push(InstrumentAccountSnapshot {
                instrument,
                orders,
                orders_complete,
                position,
                isolated: None,
            });
        }
        // Instruments with open orders whose positions the read did not finish listing.
        instrument_snapshots.extend(orders.into_rest().map(|(instrument, (orders, complete))| {
            InstrumentAccountSnapshot {
                instrument,
                orders,
                orders_complete: complete,
                position: PositionReport::Unreported,
                isolated: None,
            }
        }));

        Ok(AccountSnapshot {
            exchange: ExchangeId::Ibkr,
            balances,
            instruments: instrument_snapshots,
        })
    }

    /// Stream account events (order updates, fills, commissions).
    ///
    /// # Thread Lifecycle
    ///
    /// Spawns two background threads: `ibkr-order-stream` reads the blocking IB
    /// subscription, and `ibkr-recovery` watches for gaps in event delivery
    /// (see below). The watcher exits within about a second of the returned
    /// `BoxStream` being dropped or the stream ending, or once the recovery or
    /// order check in progress then ends. The reader exits when:
    /// - The returned `BoxStream` is dropped (channel closes), or the stream has
    ///   ended, and the next IB event arrives
    /// - The IB subscription ends
    ///
    /// **Important:** If IB is stalled (no events flowing), the reader thread blocks
    /// on the iterator. Dropping the stream signals termination, but the thread won't
    /// observe it until the next IB event arrives. A shutdown does end the
    /// subscription, so the thread exits then.
    ///
    /// # Reconnects and Recovery
    ///
    /// `ibapi` reconnects its socket to TWS/Gateway by itself, and this stream
    /// stays open across that: a transient drop produces neither an error nor
    /// `StreamTerminated`. TWS does not resend what it sent while the socket was
    /// down, and TWS losing its own link to IB's servers (notice 1100) opens the
    /// same kind of gap. Once delivery is restored, this stream asks TWS for the
    /// day's executions and emits the ones from the gap as `Trade` events. The
    /// gap is taken to start up to a poll interval before the drop was noticed,
    /// minus a lookback margin. Trades the stream already delivered are not
    /// emitted twice. The log line `Recovered IBKR fills after a gap in event
    /// delivery` marks one.
    ///
    /// When that read fails three times for a reason other than the transport
    /// dropping again, the gap is given up: the stream sends one
    /// `AccountEventKind::FillRecoveryGaveUp` covering every instrument, from the
    /// start of the gap to when it gave up, and stays open. Read that span with
    /// [`ExecutionClient::fetch_trades`].
    ///
    /// **Orders that ended during the gap.** After the fills, recovered or given
    /// up, the stream checks how each order the client holds as live ended:
    /// those placed through it, listed by `fetch_open_orders` or
    /// `account_snapshot`, or reported live on this stream, and not yet seen to
    /// end. It lists the open orders once, and looks up each held order the
    /// listing no longer shows in IB's completed orders, as
    /// [`OrderStatusClient::fetch_ended_orders`] does. Each that ended is sent
    /// as an `OrderSnapshot` of its inactive state, under `StrategyId::unknown`,
    /// and its client order id is freed, as when the stream reports an order
    /// ending itself. An order IB lists as neither is dropped from the set with a
    /// warning. A check that fails is retried after 1, 2, 4, 8 and 16 minutes
    /// while connected, then given up with an error log: an order that ended
    /// then stays live in engine state until the next gap's check, so reconcile
    /// with `fetch_open_orders`. The client holds at most 4,096 orders as live,
    /// forgetting the oldest beyond that. The check reads IB's listings on the
    /// recovery thread, each bounded by a 10-second stall timeout, and the thread
    /// reads notices again once it ends.
    ///
    /// Call this within a Tokio runtime with its time driver enabled, as
    /// `#[tokio::main]` and `Runtime::new` build: the recovery thread runs the
    /// order check on the runtime this was called on.
    ///
    /// While TWS reports its link to IB's servers lost (1100), nothing marks the
    /// gap on this stream until the link is restored and the recovered fills
    /// arrive.
    ///
    /// The stream ends with `StreamTerminated` when the client shuts down for
    /// good, because `ibapi` gave up reconnecting, [`IbkrClient::disconnect`] was
    /// called, or TWS/Gateway ended the API session. Called on a client that has
    /// already shut down, this method fails, so replace the client.
    ///
    /// # Duplicate Events
    ///
    /// Orders submitted via `open_order()` will emit `OrderSnapshot` events on
    /// this stream in addition to the response from `open_order()` itself. This
    /// is inherent to IB's API — both the per-order subscription and the global
    /// order update stream receive the same `OrderStatus` events. Callers should
    /// deduplicate if needed.
    ///
    /// `Trade` events are delivered once each. Executions that answer an
    /// executions request, such as [`ExecutionClient::fetch_trades`], are not
    /// reported here as fills.
    ///
    /// # Corrections
    ///
    /// IB reports a corrected execution as a further execution whose id differs
    /// only in the digits after the final period (`….01.02` corrects `….01.01`).
    /// It is sent as `AccountEventKind::TradeAmended`, `Corrected`, with the
    /// correction as the replacement `Trade`, once its commission report arrives.
    /// Its `original` is the revision this stream delivered before, or, for an
    /// execution from before the stream, the revision the correction's id says
    /// it corrects. A revision older than one already delivered is dropped.
    /// Fills recovered after a gap are delivered originals first, so a
    /// correction recovered with them comes after every original in the gap,
    /// not in time order.
    /// Each correction is logged at `warn!` when it arrives, so one that never
    /// gets a commission report is still seen. IB documents no busts.
    ///
    /// The revisions delivered are remembered in a bounded cache, like the dedup
    /// cache. A correction of an execution the cache has forgotten names the
    /// previous revision by its id, which is the original unless an
    /// intermediate revision was delivered and forgotten too.
    ///
    /// One case loses a fill: a correction that arrives live after a reconnect,
    /// before recovery has delivered its original from the gap. The correction
    /// is sent naming the original, and recovery then drops the original as
    /// older than a revision already delivered, so the trade reaches the stream
    /// only as the amendment.
    ///
    /// # Filter Parameters
    ///
    /// The `assets` and `instruments` parameters are currently ignored. IB's
    /// `order_update_stream()` returns events for all orders on the account.
    /// Callers subscribing for a specific instrument will receive events for
    /// all instruments.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let client = self.client.clone();

        // M-8 fix: Wrap blocking subscription call in spawn_blocking
        // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
        let (notices, order_sub) = tokio::task::spawn_blocking(move || {
            // Notices first, so a reconnect that lands while the order stream is being set up is
            // not missed.
            let notices = client.notice_stream()?;
            client.order_update_stream().map(|sub| (notices, sub))
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
        .map_err(|e| UnindexedClientError::Internal(format!("order updates: {e}")))?;

        let api_client_id = self.client.client_id();
        let contracts_clone = self.contracts.clone();
        let order_ids_clone = self.order_ids.clone();
        let pending_cancels_clone = self.pending_cancels.clone();
        let exec_buffer_clone = self.execution_buffer.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(tx, new_dedup_cache(), self.known_live.clone());

        // Spawned before the reader: if the reader fails to spawn, `rx` drops with the error and
        // the watcher sees its consumer gone.
        let watcher = recovery::RecoveryWatcher {
            client: self.client.clone(),
            notices,
            contracts: self.contracts.clone(),
            order_ids: self.order_ids.clone(),
            pending_cancels: self.pending_cancels.clone(),
            pending: self.execution_buffer.clone(),
            known: self.known_live.clone(),
            listings: self.listings.clone(),
            earlier_completions: self.earlier_completions.clone(),
            sink: sink.clone(),
            runtime: tokio::runtime::Handle::current(),
        };
        let watcher_sink = sink.clone();
        std::thread::Builder::new()
            .name("ibkr-recovery".to_string())
            .spawn(move || {
                // Same panic policy as the reader below: a panic ends recovery, and a stream that
                // can no longer recover must say so rather than stay open.
                if let Err(panic_info) = catch_unwind(AssertUnwindSafe(|| watcher.run())) {
                    let msg = panic_message(panic_info.as_ref());
                    error!("IBKR recovery worker panicked: {msg}");
                    watcher_sink.terminate(StreamTerminationReason::Error(format!(
                        "IBKR recovery worker panicked: {msg}"
                    )));
                }
            })
            .map_err(|e| UnindexedClientError::TaskFailed(format!("thread spawn: {e}")))?;

        std::thread::Builder::new()
            .name("ibkr-order-stream".to_string())
            .spawn(move || {
                // Panic safety: parking_lot mutexes do not poison on panic, so shared state
                // (ContractRegistry, OrderIdMap, etc.) remains usable. catch_unwind unwinds only
                // the inner closure — `sink` lives in this outer thread closure and is still open
                // afterward, so the panic handler below emits a terminal StreamTerminated
                // (a panic is a terminal stream death like any other).
                let result = catch_unwind(AssertUnwindSafe(|| {
                    forward_order_updates(
                        order_sub.iter_data(),
                        &sink,
                        api_client_id,
                        &contracts_clone,
                        &order_ids_clone,
                        &pending_cancels_clone,
                        &exec_buffer_clone,
                    );
                }));

                if let Err(panic_info) = result {
                    let msg = panic_message(panic_info.as_ref());
                    error!("Order stream worker panicked: {msg}");
                    // A panic is a terminal stream death — surface it in-band so the consumer
                    // gets a programmatic signal rather than inferring EOF.
                    // Best-effort: a no-op if the consumer already dropped rx.
                    sink.terminate(StreamTerminationReason::Error(format!(
                        "IBKR order-update worker panicked: {msg}"
                    )));
                }
            })
            .map_err(|e| UnindexedClientError::TaskFailed(format!("thread spawn: {e}")))?;

        Ok(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
    }

    /// Cancel an order.
    ///
    /// # Async Cancel Semantics
    ///
    /// Returns `Ok(Cancelled)` when the cancel request is **submitted**, not when
    /// the order is confirmed cancelled. The actual cancellation confirmation comes
    /// via `account_stream` as an `OrderStatus::Cancelled` event.
    ///
    /// The order ID mapping is retained until terminal status is received via
    /// `account_stream`, ensuring fill events arriving between cancel request and
    /// confirmation are not lost.
    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> UnindexedOrderResponseCancel {
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let ib_order_id = match self.order_ids.get_ib_id(&request.key.cid) {
            Some(id) => id,
            None => {
                return OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        "order ID not found in map".to_string(),
                    ))),
                };
            }
        };

        let client = self.client.clone();

        let result =
            tokio::task::spawn_blocking(move || client.cancel_order(ib_order_id, "")).await;

        match result {
            Ok(Ok(_sub)) => {
                // Track user-initiated cancel for Cancelled vs Expired differentiation.
                // Insert only after cancel request succeeded — avoids stale entries if
                // the future is dropped before reaching this branch.
                self.pending_cancels.insert(ib_order_id);

                // H-3 fix: Do NOT remove order_ids here. The mapping is needed to
                // correlate any fill events that arrive between now and when IB
                // confirms the cancel. Removal happens in account_stream when
                // OrderStatus::Cancelled is received.
                // IBKR cancel_order returns no filled qty, so it is unknown here; the
                // subsequent OrderStatus events report it.
                OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(
                        OrderId::new(format_smolstr!("{}", ib_order_id)),
                        Utc::now(),
                        None,
                    )),
                }
            }
            Ok(Err(e)) => {
                error!(order_id = ib_order_id, error = %e, "Failed to cancel order");
                OrderResponseCancel {
                    key,
                    state: Err(send_error(&e, e.to_string())),
                }
            }
            Err(e) => {
                error!(order_id = ib_order_id, error = %e, "Task join error");
                OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                }
            }
        }
    }

    /// Submit an order to IB.
    ///
    /// # Client order ids
    ///
    /// The client order id is sent as the order's IB order reference, so it must be at most 128
    /// ASCII characters; any other is refused before anything is sent. TWS does not check the
    /// reference's uniqueness, so this client keeps each id unique itself: an open
    /// under an id that a live order holds is refused before anything is sent, as
    /// [`ApiError::DuplicateClientOrderId`], and the live order is unaffected. An order stops
    /// holding its id when the account stream reports it `Filled`, `Cancelled` or `Inactive`, or
    /// when [`clear_stale_order_ids`](IbkrClient::clear_stale_order_ids) reaps it, or when the
    /// account stream's check after a gap finds it ended. Without a running
    /// [`account_stream`](Self::account_stream), no order is reported ended, and an id is held
    /// until reaped.
    ///
    /// A cancel names the live order under its id, so cancelling a filled order's id once it is
    /// freed is refused locally as not found, rather than at TWS.
    ///
    /// # Cancellation Safety
    ///
    /// This future registers the order ID mapping before submitting to IB.
    /// If the future is cancelled (e.g., via `tokio::select!` timeout) after
    /// registration but before completion:
    /// - The order may still be submitted to IB
    /// - The order ID mapping will leak (not cleaned up)
    /// - Subsequent fills will be processed via `account_stream`
    /// - The client order id stays held, so a retry under it is refused as
    ///   [`ApiError::DuplicateClientOrderId`] until the account stream reports
    ///   the order ended or `clear_stale_order_ids` reaps it. Retry under a
    ///   fresh id. The same holds when this returns `Open` without a status
    ///   from TWS.
    ///
    /// Callers should avoid cancelling this future mid-flight. If timeout
    /// behavior is needed, prefer setting IB's native order timeout via
    /// `TimeInForce` instead.
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
        let key = OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let contract = match self.contracts.get_contract(request.key.instrument) {
            Some(c) => c,
            None => {
                return Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::InstrumentInvalid(
                        request.key.instrument.clone(),
                        "contract not registered".to_string(),
                    ))),
                };
            }
        };

        let quantity: f64 = match request.state.quantity.try_into() {
            Ok(q) => q,
            Err(_) => {
                return Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        format!("quantity {} exceeds f64 range", request.state.quantity),
                    ))),
                };
            }
        };

        let ib_order = match order::order_ref(&request.key.cid).and_then(|order_ref| {
            build_ib_order(
                request.state.side,
                quantity,
                &request.state.kind,
                request.state.price,
                &request.state.time_in_force,
            )
            .map(|order| ibapi::orders::Order { order_ref, ..order })
        }) {
            Ok(o) => o,
            Err(e) => {
                return Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                };
            }
        };

        let ib_order_id = self.allocate_order_id();

        // Store order context for reconstructing Order from OrderStatus callbacks
        let context = OrderContext {
            instrument: request.key.instrument.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
        };
        // Before anything is sent: TWS does not check the uniqueness of the order reference that
        // carries the client order id, so this is the only place a second live order under it can
        // be refused. A refused id simply skips the allocated ID.
        if let Err(in_use) = self
            .order_ids
            .register(request.key.cid.clone(), ib_order_id, context)
        {
            return Order {
                key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(OrderError::Rejected(
                    ApiError::DuplicateClientOrderId(in_use.to_string()),
                )),
            };
        }
        // The id names no live order, so this is a new order under it, though an earlier one
        // under it ended.
        self.known_live.lock().placing(&request.key.cid);

        let client = self.client.clone();
        let side = request.state.side;
        let price = request.state.price;
        let req_quantity = request.state.quantity;
        let kind = request.state.kind;
        let tif = request.state.time_in_force;

        // Move both place_order AND subscription iteration into spawn_blocking
        // to avoid blocking Tokio worker threads on IB's synchronous iterator.
        let result = tokio::task::spawn_blocking(move || {
            let sub = match client.place_order(ib_order_id, &contract, &ib_order) {
                Ok(s) => s,
                Err(e) => return Err(send_error(&e, e.to_string())),
            };

            match await_order_placement(sub.timeout_iter_data(PLACEMENT_STATUS_TIMEOUT)) {
                PlacementOutcome::Accepted { filled } => {
                    let filled = parse_decimal_or_warn(filled, "status.filled");
                    Ok(Some((ib_order_id, filled)))
                }
                // Cancelled/Inactive/unexpected status or a genuine notice/error.
                // Don't remove the mapping here — the outer match centralizes all
                // error-path removals.
                PlacementOutcome::Rejected(reason) => {
                    Err(OrderError::Rejected(ApiError::OrderRejected(reason)))
                }
                // Held until RTH (informational notice): the order is live; its
                // authoritative status arrives via account_stream. Surface as
                // "no terminal status yet" (Open with zero fill), same as below.
                PlacementOutcome::HeldPending(notice) => {
                    warn!(ib_order_id, %notice, "order held/pending; tracking via stream");
                    Ok(None)
                }
                // Subscription exhausted/timed out without a terminal status.
                // Do NOT remove the mapping — ExecutionData/CommissionReport may
                // still arrive via account_stream; clear_stale_order_ids() handles
                // stale mappings.
                PlacementOutcome::NoStatus => Ok(None),
            }
        })
        .await;

        // Recorded only once sent: an open refused before sending, as under a client order id a
        // live order holds, must not end that order in the set.
        let order = match result {
            Ok(Ok(Some((order_id, filled)))) => {
                // IB always returns order status via subscription - never immediate fills.
                // The filled quantity here is from the OrderStatus event, not a complete fill.
                Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::active(Open::new(
                        VenueOrderId::Assigned(OrderId::new(format_smolstr!("{}", order_id))),
                        Utc::now(),
                        filled,
                    )),
                }
            }
            Ok(Ok(None)) => {
                // Subscription exhausted without terminal status. The order WAS submitted
                // (we have an IB order ID), but we lost tracking. Return Open with zero
                // filled — caller can query via fetch_open_orders or wait for account_stream.
                warn!(
                    ib_order_id,
                    "Order subscription ended without terminal status, returning Open"
                );
                Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::active(Open::new(
                        VenueOrderId::Assigned(OrderId::new(format_smolstr!("{}", ib_order_id))),
                        Utc::now(),
                        Decimal::ZERO,
                    )),
                }
            }
            Ok(Err(error)) => {
                // Cleanup order_ids: the order was never sent (place_order error)
                // or TWS rejected it (Cancelled/Inactive or a genuine notice).
                self.order_ids.remove_by_ib_id(ib_order_id);
                Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::inactive(error),
                }
            }
            Err(e) => {
                self.order_ids.remove_by_ib_id(ib_order_id);
                Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                }
            }
        };
        self.known_live
            .lock()
            .placed(&order.key, order.quantity, order.kind, &order.state);
        order
    }

    /// Fetch account balances.
    ///
    /// # Limitations
    ///
    /// The `time_exchange` field in returned balances uses `Utc::now()`, not
    /// the actual IB server timestamp. IB's account summary endpoint does not
    /// provide timestamps per balance update.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if the request fails, IB answers with an
    /// error, IB sends nothing for 10 seconds before the end of the listing, or the
    /// connection drops during it.
    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let client = self.client.clone();
        let assets_filter: Option<HashSet<AssetNameExchange>> = if assets.is_empty() {
            None
        } else {
            Some(assets.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            // IB's reqAccountSummary expects a group name ("All" for all linked accounts),
            // not an account ID. Using account ID causes error 321 "Unified group name is invalid".
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .account_summary(&ACCOUNT_GROUP_ALL, &["TotalCashValue", "AvailableFunds"])
                .map_err(|e| UnindexedClientError::Internal(format!("account_summary: {e}")))?;

            let mut balances = read_account_summary(
                LISTING_STALL_TIMEOUT,
                || client.is_connected(),
                |timeout| sub.next_timeout(timeout),
            )?
            .to_balances();

            if let Some(ref filter) = assets_filter {
                balances.retain(|b| filter.contains(&b.asset));
            }

            Ok(balances)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }

    /// Fetch the open orders this API client placed.
    ///
    /// IB numbers orders per API client, so orders other clients placed, and orders entered in
    /// TWS, are left out: their ids could name an order of this client's. IB reports orders
    /// entered in TWS under API client id 0, so a client connected as 0 keeps them.
    ///
    /// Each order is returned under the client order id it was placed with: the one this client
    /// tracks it under, with the kind, price and time in force it was placed with, or else the
    /// one its IB order reference carries, with those read back from IB's listing. An order
    /// this client does not track but placed under a client order id, such as one placed before
    /// a restart, is tracked from then on, so its fills and status reach the account stream and
    /// it can be cancelled. Every listed order of this client's is adopted so, whatever
    /// `instruments` asks for. An order without a reference, such as one placed by an earlier
    /// version of this client, or, for a client connected as 0, one entered in TWS, is returned
    /// under its IB order id; so is one whose reference names another order this client tracks.
    /// One whose order type or time in force this client never sends is left out, with a
    /// warning. Every order's filled quantity is the one IB reports with it.
    ///
    /// Each returned order named by the id it was placed with is held as live, for the account
    /// stream's check of how it ended after a gap.
    ///
    /// IB lists every client's open orders to every request for them, so this client and its
    /// clones read the listing one at a time.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if the request fails, IB answers with an
    /// error, IB sends nothing for 10 seconds before the end of the listing, or the
    /// connection drops during it.
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        let listings = self.listings.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        let listed = tokio::task::spawn_blocking(move || {
            let listing = listings.open_orders(&client)?;
            let mut listed =
                open_orders_from_listing(listing, client.client_id(), &contracts, &order_ids);
            if let Some(filter) = &instruments_filter {
                listed.retain(|listed| filter.contains(&listed.instrument));
            }
            Ok::<_, UnindexedClientError>(listed)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))??;

        remember_listed(&self.known_live, &listed);
        Ok(listed
            .into_iter()
            .filter_map(|listed| listed.order)
            .collect())
    }

    /// Fetch historical trades (executions).
    ///
    /// # Limitations
    ///
    /// - IB only returns executions from the current trading day. The span is applied
    ///   client-side to filter within that day, and the read is always complete
    ///   (`resume: None`) for what IB returned: a span reaching before today is not read
    ///   before today. For historical executions beyond today, use IB's Flex Query or
    ///   Activity Statements.
    /// - Only the executions of orders this API client placed are read. IB numbers
    ///   orders per API client, so another client's execution could name an order id
    ///   of this client's.
    /// - Each trade's fees come from the commission report IB sends with its
    ///   execution. An execution whose report IB did not send is returned with a zero
    ///   fee in [`UNKNOWN_FEE_ASSET`](execution::UNKNOWN_FEE_ASSET), with a warning.
    /// - Trades are returned in `time_exchange` order, then by id. A corrected execution keeps the
    ///   position of its first revision.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if the request fails, IB answers with an
    /// error, IB sends nothing for 10 seconds before the end of the listing, or the
    /// connection drops during it.
    ///
    /// # Corrections
    ///
    /// IB reports a corrected execution as a further execution whose id differs
    /// only in the digits after the final period. Each execution is returned
    /// once, at its latest revision, under that revision's id: the replacement a
    /// `TradeAmended` on [`ExecutionClient::account_stream`] names.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        if start > end {
            return Ok(TradesRead::complete(Vec::new()));
        }
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            let api_client_id = client.client_id();
            let exec_filter = ibapi::orders::ExecutionFilter {
                client_id: Some(api_client_id),
                ..ibapi::orders::ExecutionFilter::default()
            };
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .executions(exec_filter)
                .map_err(|e| UnindexedClientError::Internal(format!("executions: {e}")))?;

            let mut listing = Vec::new();
            read_listing(
                "executions",
                LISTING_STALL_TIMEOUT,
                || client.is_connected(),
                |timeout| sub.next_timeout(timeout),
                |item| {
                    listing.push(item);
                    ControlFlow::Continue(())
                },
            )?;

            let trades = trades_from_executions(
                listing,
                api_client_id,
                &contracts,
                start..=end,
                instruments_filter.as_ref(),
            );
            Ok(TradesRead::complete(execution::keep_latest_revisions(
                trades,
            )))
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }
}

/// The trades [`ExecutionClient::fetch_trades`] returns from what one executions request
/// listed: the executions of this API client's orders, in registered contracts, at a time in
/// `span`, in `instruments` when that is given. Each is paired with the commission report IB lists
/// after it.
///
/// An execution IB listed no commission report for is a trade with an unknown fee, warned about.
/// The trades are in time order, then by id.
fn trades_from_executions(
    listing: impl IntoIterator<Item = ibapi::orders::Executions>,
    api_client_id: i32,
    contracts: &ContractRegistry,
    span: std::ops::RangeInclusive<DateTime<Utc>>,
    instruments: Option<&HashSet<InstrumentNameExchange>>,
) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
    use ibapi::orders::Executions;

    let awaiting_commission = ExecutionBuffer::new();
    let mut trades = Vec::new();
    for item in listing {
        match item {
            Executions::ExecutionData(data) => {
                let exec = &data.execution;
                if exec.client_id != api_client_id {
                    // The request asked IB for this client's alone.
                    debug!(
                        exec_id = %exec.execution_id,
                        api_client_id = exec.client_id,
                        "Execution of another API client's order, skipping"
                    );
                    continue;
                }
                let Some(instrument) = contracts.get_name_by_con_id(data.contract.contract_id)
                else {
                    continue;
                };
                if instruments.is_some_and(|filter| !filter.contains(&instrument)) {
                    continue;
                }
                match execution::parse_ib_timestamp(&exec.time) {
                    Some(time) if span.contains(&time) => {
                        awaiting_commission.add_execution(data, instrument);
                    }
                    Some(_) => {}
                    None => warn!(
                        exec_id = %exec.execution_id,
                        time = %exec.time,
                        "Unparseable timestamp in execution, skipping"
                    ),
                }
            }
            Executions::CommissionReport(report) => {
                if let Some(trade) = awaiting_commission.complete_with_commission(&report) {
                    trades.push(trade);
                }
            }
        }
    }

    let without_commission = awaiting_commission.take_without_commission();
    if !without_commission.is_empty() {
        warn!(
            count = without_commission.len(),
            "IBKR executions arrived without a commission report; returned with an unknown fee"
        );
        trades.extend(without_commission);
    }
    // In time order. IB lists commission reports after their executions, and a map holds those
    // without one, so neither gives that order.
    trades.sort_by(|a, b| {
        a.time_exchange
            .cmp(&b.time_exchange)
            .then_with(|| a.id.cmp(&b.id))
    });
    trades
}

/// One of this API client's open orders, read back from an open-orders listing.
#[derive(Debug)]
struct ListedOpenOrder {
    instrument: InstrumentNameExchange,
    /// The order, or `None` when it cannot be read back: IB lists a shape this client never
    /// sends.
    order: Option<Order<ExchangeId, InstrumentNameExchange, Open>>,
    /// Whether the order is named by the client order id it was placed with, rather than by its
    /// IB order id.
    named_as_placed: bool,
    /// Whether IB listed the order's status, which carries its fill.
    paired_with_status: bool,
}

impl ListedOpenOrder {
    /// Whether the order is reported exactly: read back, under the client order id it was placed
    /// with, and with the fill its status reports.
    fn is_exact(&self) -> bool {
        self.order.is_some() && self.named_as_placed && self.paired_with_status
    }
}

/// This API client's open orders from what one open-orders request listed, by IB order id, each
/// read back as [`open_order_from_listing`] does. Orders in contracts not registered, and orders
/// that have ended, are left out.
fn open_orders_from_listing(
    listing: impl IntoIterator<Item = ibapi::orders::Orders>,
    api_client_id: i32,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
) -> Vec<ListedOpenOrder> {
    use ibapi::orders::Orders;

    // IB lists each order, then its status, which carries the fill.
    let mut listed = std::collections::BTreeMap::new();
    let mut statuses = fnv::FnvHashMap::default();
    for item in listing {
        match item {
            Orders::OrderData(data) if data.order.client_id == api_client_id => {
                listed.insert(data.order_id, data);
            }
            Orders::OrderStatus(status) if status.client_id == api_client_id => {
                statuses.insert(status.order_id, status);
            }
            _ => {}
        }
    }
    listed
        .into_values()
        .filter_map(|data| {
            open_order_from_listing(&data, statuses.get(&data.order_id), contracts, order_ids)
        })
        .collect()
}

/// The open order IB lists as `data`, with `status`, the status IB lists after it, or `None` when
/// it is not in a registered contract or has ended.
///
/// An order is named by the client order id it was placed with: the one this client tracks it
/// under, or else its order reference, which carries the id ([`order::order_ref`]). Either way an
/// order this client tracks keeps the shape it was placed with, and any other is read back by
/// [`order::order_shape_from_ib`]. An order named by its reference that this client does not track,
/// such as one placed before a restart, is adopted: tracked from then on under that id, so its
/// fills and status reach the account stream and it can be cancelled.
///
/// An order without a reference, such as one placed by an earlier version of this client, is named
/// by its IB order id, which is not an id it was placed with, so it is not
/// [exact](ListedOpenOrder::is_exact). So is one whose reference names another order this client
/// tracks, under that id or its IB order id, which cannot be adopted. Nor is one IB listed without
/// its status, whose fill is the order's own figure, which IB leaves zero.
fn open_order_from_listing(
    data: &ibapi::orders::OrderData,
    status: Option<&ibapi::orders::OrderStatus>,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
) -> Option<ListedOpenOrder> {
    let ib_id = data.order_id;
    let instrument = contracts.get_name_by_con_id(data.contract.contract_id)?;

    // IB can still list an order that has just ended.
    let state = status.map_or(&data.order_state.status, |status| &status.status);
    if state.is_terminal() {
        debug!(
            ib_order_id = ib_id,
            ?state,
            "Listed open order has ended, skipping"
        );
        return None;
    }
    // The status carries the fill. Without one, the order's own figure, which IB may leave zero.
    let filled = status.map_or(data.order.filled_quantity, |status| status.filled);
    let filled = try_decimal_or_warn(filled, format_args!("filled quantity of order {ib_id}"))
        .unwrap_or(Decimal::ZERO);
    let order_ref = (!data.order.order_ref.is_empty())
        .then(|| ClientOrderId::new(data.order.order_ref.as_str()));

    // The order this client tracks under IB order id `ib_id`, unless that id now names another:
    // IB numbers orders per API client and reuses the numbers across sessions.
    let tracked = order_ids
        .get_client_id_and_context(ib_id)
        .filter(|(cid, ctx)| {
            ctx.instrument == instrument && order_ref.as_ref().is_none_or(|r| r == cid)
        });
    let (cid, ctx, named_as_placed) = match tracked {
        Some((cid, ctx)) => (cid, ctx, true),
        None => {
            let shape = match order::order_shape_from_ib(&data.order) {
                Ok(shape) => shape,
                Err(reason) => {
                    warn!(
                        ib_order_id = ib_id,
                        %instrument,
                        %reason,
                        "Open IBKR order cannot be read back, leaving it out"
                    );
                    return Some(ListedOpenOrder {
                        instrument,
                        order: None,
                        named_as_placed: false,
                        paired_with_status: status.is_some(),
                    });
                }
            };
            let ctx = OrderContext {
                instrument: instrument.clone(),
                side: order::action_to_side(&data.order.action),
                price: shape.price,
                quantity: parse_decimal_or_warn(data.order.total_quantity, "total_quantity"),
                kind: shape.kind,
                time_in_force: shape.time_in_force,
            };
            match order_ref {
                Some(cid) if adopt_listed_order(order_ids, &cid, ib_id, &ctx) => (cid, ctx, true),
                // Not trackable under its id, which names another order here, so not reported
                // under it either: two orders would share it.
                Some(_) | None => (ClientOrderId::new(format_smolstr!("{ib_id}")), ctx, false),
            }
        }
    };

    Some(ListedOpenOrder {
        instrument,
        order: Some(Order {
            key: OrderKey {
                exchange: ExchangeId::Ibkr,
                instrument: ctx.instrument,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: ctx.side,
            price: ctx.price,
            quantity: ctx.quantity,
            kind: ctx.kind,
            time_in_force: ctx.time_in_force,
            state: Open::new(
                VenueOrderId::Assigned(execution::ib_order_id(ib_id)),
                Utc::now(),
                filled,
            ),
        }),
        named_as_placed,
        paired_with_status: status.is_some(),
    })
}

/// The open orders of an account snapshot, by instrument, each instrument's with whether they are
/// all of its open orders.
#[derive(Debug)]
struct SnapshotOrders {
    by_instrument: fnv::FnvHashMap<InstrumentNameExchange, (Vec<UnindexedOrderSnapshot>, bool)>,
    /// Whether the listing was read to its end, so an instrument it shows no order on has none.
    read: bool,
}

impl SnapshotOrders {
    /// The orders of `listed`, the listing read, or `None` when it could not be read.
    ///
    /// An instrument's orders are complete when each of them was read back
    /// [exactly](ListedOpenOrder::is_exact): IB lists every open order of the account, unpaged,
    /// so only one this client cannot name as placed, or read back, can be missing from them.
    fn new(listed: Option<Vec<ListedOpenOrder>>) -> Self {
        let read = listed.is_some();
        let mut by_instrument = fnv::FnvHashMap::default();
        for listed in listed.into_iter().flatten() {
            let exact = listed.is_exact();
            let (orders, complete): &mut (Vec<_>, bool) = by_instrument
                .entry(listed.instrument)
                .or_insert_with(|| (Vec::new(), true));
            *complete &= exact;
            if let Some(order) = listed.order {
                orders.push(order.into());
            }
        }
        Self {
            by_instrument,
            read,
        }
    }

    /// The instruments with an open order.
    fn instruments(&self) -> impl Iterator<Item = &InstrumentNameExchange> {
        self.by_instrument.keys()
    }

    /// Take `instrument`'s orders, and whether they are complete.
    fn take(&mut self, instrument: &InstrumentNameExchange) -> (Vec<UnindexedOrderSnapshot>, bool) {
        self.by_instrument
            .remove(instrument)
            .unwrap_or_else(|| (Vec::new(), self.read))
    }

    /// The instruments not yet taken, with their orders.
    fn into_rest(
        self,
    ) -> impl Iterator<Item = (InstrumentNameExchange, (Vec<UnindexedOrderSnapshot>, bool))> {
        self.by_instrument.into_iter()
    }
}

/// The client order ids of this API client's orders in an open-orders listing, for the account
/// stream's check of how the orders it holds as live ended: each order's reference, or else the
/// id this client tracks it under.
///
/// Unlike [`open_orders_from_listing`], nothing is read back or adopted, and an order IB lists
/// with a status that has ended is kept. IB's completed orders may not list it yet, and its end
/// reaches the account stream as that status, so the check need not look it up.
fn listed_cids(
    listing: impl IntoIterator<Item = ibapi::orders::Orders>,
    api_client_id: i32,
    order_ids: &OrderIdMap,
) -> fnv::FnvHashSet<ClientOrderId> {
    listing
        .into_iter()
        .filter_map(|item| match item {
            ibapi::orders::Orders::OrderData(data) if data.order.client_id == api_client_id => {
                if data.order.order_ref.is_empty() {
                    order_ids.get_client_id(data.order_id)
                } else {
                    Some(ClientOrderId::new(data.order.order_ref.as_str()))
                }
            }
            _ => None,
        })
        .collect()
}

/// Hold each order of `listed` named by the client order id it was placed with as live, for the
/// account stream's check of how it ended if the stream misses its end. One named by its IB order
/// id could not be looked up by it once ended.
fn remember_listed(known: &SharedKnownLiveOrders, listed: &[ListedOpenOrder]) {
    let mut known = known.lock();
    for listed in listed.iter().filter(|listed| listed.named_as_placed) {
        if let Some(order) = &listed.order {
            known.live(&order.key, order.quantity, order.kind, &order.state);
        }
    }
}

/// Track the listed open order `ib_id` under `cid`, the id its order reference carries, unless
/// either already names an order here. Returns whether it is tracked.
fn adopt_listed_order(
    order_ids: &OrderIdMap,
    cid: &ClientOrderId,
    ib_id: i32,
    ctx: &OrderContext,
) -> bool {
    match order_ids.adopt(cid.clone(), ib_id, ctx.clone()) {
        Ok(()) => {
            info!(
                ib_order_id = ib_id,
                %cid,
                instrument = %ctx.instrument,
                "Adopted an open IBKR order this client did not track"
            );
            true
        }
        Err(refused) => {
            warn!(
                ib_order_id = ib_id,
                %cid,
                reason = %refused,
                "Listed IBKR order cannot be tracked under its client order id; listing it under \
                 its IB order id"
            );
            false
        }
    }
}

/// How long after a good-till-date order's expiry a cancellation is still read as the expiry,
/// rather than as a cancellation before it: IB's clock and this host's differ, and the status
/// takes time to arrive.
const GTD_EXPIRY_TOLERANCE: chrono::Duration = chrono::Duration::seconds(5);

/// Whether an order IB reports `Cancelled` at `at` expired rather than was cancelled: a cancel of
/// it was not requested through this client (`user_cancel`), and it is a day order, or a
/// good-till-date order at or past its expiry, give or take [`GTD_EXPIRY_TOLERANCE`]. An order of
/// any other time in force was cancelled by the broker or the exchange.
fn cancelled_at_expiry(user_cancel: bool, time_in_force: &TimeInForce, at: DateTime<Utc>) -> bool {
    !user_cancel
        && match time_in_force {
            TimeInForce::GoodUntilEndOfDay => true,
            TimeInForce::GoodTillDate { expiry } => at + GTD_EXPIRY_TOLERANCE >= *expiry,
            _ => false,
        }
}

/// Build an Order from IB OrderStatus using stored OrderContext.
///
/// # IBKR Status Mapping
///
/// | `OrderStatusKind`                           | → OrderState         | Rationale                                   |
/// |---------------------------------------------|----------------------|---------------------------------------------|
/// | `Inactive`                                  | OpenFailed(Rejected) | Order blocked by validation/margin/exchange |
/// | `Cancelled`                                 | Cancelled or Expired | See differentiation logic below             |
/// | `Filled`                                    | FullyFilled          | Order fully executed                        |
/// | `Submitted`/`PreSubmitted`/`PendingSubmit`  | Active(Open)         | Order working on exchange                   |
/// | `ApiPending`/`PendingCancel`/`ApiCancelled` | Active(Open)         | Transitional; await a confirmed terminal    |
/// | `Unknown(raw)`                              | Active(Open) + warn  | Unmodelled status; stream is authoritative  |
///
/// The match is exhaustive over `OrderStatusKind` (no wildcard), so a new ibapi
/// *variant* becomes a compile error rather than a silent `Active(Open)`.
///
/// Since ibapi 4.0 a new TWS *status string* no longer reaches that guard: it
/// decodes to `OrderStatusKind::Unknown(raw)` instead of failing the whole
/// subscription with `Error::Parse`. This function handles that variant
/// explicitly and logs it at `warn!`, so an unrecognised status is observable
/// rather than silent — but it is no longer a compile error.
///
/// # Cancelled vs Expired Differentiation
///
/// IBKR sends `"Cancelled"` status for both user-initiated cancellation and
/// time-based expiration. We differentiate using:
///
/// 1. If order ID is in `pending_cancels` (set by `cancel_order`) → `Cancelled`
/// 2. Else if `time_in_force == GoodUntilEndOfDay` → `Expired` (DAY order expired)
/// 3. Else if `time_in_force` is `GoodTillDate` and its expiry has passed, give or take
///    [`GTD_EXPIRY_TOLERANCE`] → `Expired`
/// 4. Else → `Cancelled` (broker-initiated or external cancellation)
///
/// # Known Limitation
///
/// If the broker cancels a DAY order before market close (e.g., insufficient margin),
/// or a GTD order is cancelled outside this client, as in TWS, within
/// [`GTD_EXPIRY_TOLERANCE`] of its expiry, it will be misclassified as `Expired`. This
/// is rare and acceptable given the alternative (forking ibapi to preserve order_id in
/// error callbacks).
fn make_order_from_status(
    status: &ibapi::orders::OrderStatus,
    client_id: ClientOrderId,
    ctx: &OrderContext,
    pending_cancels: &PendingCancels,
) -> Order<ExchangeId, InstrumentNameExchange, OrderState<AssetNameExchange, InstrumentNameExchange>>
{
    use ibapi::orders::OrderStatusKind;

    let ib_id = status.order_id;
    let order_id = execution::ib_order_id(ib_id);

    // `None` when IB reports a fill that is not a number, so that an ended order does not report
    // an unknown fill as zero.
    let reported_fill = try_decimal_or_warn(
        status.filled,
        format_args!("status.filled of order {ib_id}"),
    );
    // A live order's fill only grows, so an unknown one reads as nothing filled until IB reports
    // more.
    let filled_qty = reported_fill.unwrap_or(Decimal::ZERO);
    let state = match status.status {
        OrderStatusKind::Inactive => {
            // "Inactive" means the order was accepted by IB but is not working:
            // validation failure, margin issue, exchange closed, share location hold.
            // This is a placement failure, not a cancellation.
            OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                "IB status: Inactive (order blocked by validation/margin/exchange)".into(),
            )))
        }
        OrderStatusKind::Cancelled => {
            let now = Utc::now();
            if cancelled_at_expiry(pending_cancels.remove(ib_id), &ctx.time_in_force, now) {
                OrderState::inactive(Expired::new(order_id, now, reported_fill))
            } else {
                OrderState::inactive(Cancelled::new(order_id, now, reported_fill))
            }
        }
        OrderStatusKind::Filled => {
            // A filled order filled its whole quantity, whether or not IB's figure parsed.
            let filled_qty = reported_fill.unwrap_or(ctx.quantity);
            OrderState::fully_filled(Filled::new(order_id, Utc::now(), filled_qty, None))
        }
        // Working states (Submitted/PreSubmitted/PendingSubmit) and the
        // transitional ones (ApiPending/PendingCancel/ApiCancelled) are reported
        // as active/Open until a confirmed Cancelled/Inactive/Filled arrives.
        // ApiCancelled is deliberately NOT treated as terminal here: it precedes
        // a confirmed Cancelled, whose event reaps the order-id mapping (or
        // clear_stale does), mirroring the account_stream `is_terminal` guard.
        // Enumerated explicitly so a future ibapi variant fails to compile rather
        // than silently defaulting to Open.
        OrderStatusKind::Submitted
        | OrderStatusKind::PreSubmitted
        | OrderStatusKind::PendingSubmit
        | OrderStatusKind::ApiPending
        | OrderStatusKind::PendingCancel
        | OrderStatusKind::ApiCancelled => OrderState::active(Open::new(
            VenueOrderId::Assigned(order_id),
            Utc::now(),
            filled_qty,
        )),
        // A status string ibapi does not model. Upstream declines to classify
        // it — `is_active()` and `is_terminal()` are both false — and so do we:
        // report it active/Open so the order-id mapping is retained and the
        // account stream can still resolve the order's real state. The
        // alternative, treating it as a rejection, would drop the mapping for
        // an order that may well be live in the market and discard its
        // executions and commissions. An order that is in fact dead and left
        // Open here is reaped by `OrderIdMap::clear_stale`.
        OrderStatusKind::Unknown(ref raw) => {
            warn!(
                ib_id,
                status = raw.as_str(),
                "unmodelled IBKR order status; treating as live, account \
                 stream is authoritative"
            );
            OrderState::active(Open::new(
                VenueOrderId::Assigned(order_id),
                Utc::now(),
                filled_qty,
            ))
        }
    };

    Order {
        key: OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: ctx.instrument.clone(),
            strategy: StrategyId::unknown(),
            cid: client_id,
        },
        side: ctx.side,
        price: ctx.price,
        quantity: ctx.quantity,
        kind: ctx.kind,
        time_in_force: ctx.time_in_force,
        state,
    }
}

pub use execution::parse_ib_timestamp;

impl BracketOrderClient for IbkrClient {
    async fn open_bracket_order(
        &self,
        request: UnifiedBracketOrderRequest<ExchangeId, &InstrumentNameExchange>,
    ) -> UnifiedBracketOrderResult {
        let ibkr_request = BracketOrderRequest {
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            parent_cid: request.key.cid.clone(),
            side: request.state.side,
            quantity: request.state.quantity,
            entry_price: request.state.entry_price,
            take_profit_price: request.state.take_profit_price,
            stop_loss_price: request.state.stop_loss_price,
            time_in_force: request.state.time_in_force,
        };

        let result = self.open_bracket_order(ibkr_request).await;

        UnifiedBracketOrderResult::with_all_legs(
            result.parent,
            result.take_profit,
            result.stop_loss,
        )
    }
}

/// Looks each order up in IB's listing of the account's completed orders, by the order reference
/// that carries the client order id it was placed with, so it finds an order this client placed
/// before a restart too. One listing is read per call, and, when an order ended cancelled or
/// expired, one read of the last seven days' executions for its fill.
///
/// A filled order is reported fully filled, without an average price; a cancelled or expired one
/// with the fill its executions show, which is nothing when they show none of it and the order
/// was placed through this client under six days ago, since the read then covers its whole life,
/// and otherwise unknown (`None`); a placement IB accepted and then rejected, or left `Inactive`,
/// as [`OpenFailed`](crate::order::state::InactiveOrderState::OpenFailed). A cancelled order is
/// reported expired by the rule [`ExecutionClient::account_stream`] applies. An order IB lists
/// under no client order id, such as one entered in TWS, cannot be found.
///
/// IB lists the orders completed before a Gateway restart under API client id 0, so a completed
/// order is matched by its client order id whichever of this client's id and 0 it is listed
/// under. Client order ids must therefore be unique across the API clients on the account.
///
/// When IB lists several completed orders under one id, the one that completed last is reported.
/// One that completed more than 5 seconds before this client began tracking the order now under
/// that id is an earlier order's, under a reused id, and the order is not reported ended, with a
/// warning: should IB's clock lag this host's by more, an order that did end is held as live.
///
/// The account stream runs the same lookup itself after a gap in event delivery; see
/// [`ExecutionClient::account_stream`].
///
/// # Errors
///
/// [`UnindexedClientError::Internal`] when a listing fails, IB sends nothing for 10 seconds
/// before the end of one, or the connection drops during it.
impl OrderStatusClient for IbkrClient {
    async fn fetch_ended_orders(
        &self,
        orders: &[crate::order::UnindexedOrderKey],
    ) -> Result<Vec<crate::order::UnindexedInactiveOrder>, UnindexedClientError> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }
        let orders = orders.to_vec();
        let client = self.client.clone();
        let listings = self.listings.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        let pending_cancels = self.pending_cancels.clone();
        let earlier_completions = self.earlier_completions.clone();
        tokio::task::spawn_blocking(move || {
            let reader = ended_orders::EndedOrderReader::new(
                &client,
                &listings,
                &contracts,
                &order_ids,
                &pending_cancels,
                &earlier_completions,
            );
            // Each lookup is answered from listings read on this thread, so the futures are
            // ready at once and nothing awaits the runtime.
            futures::executor::block_on(crate::client::order_recovery::fetch_ended_by_key(
                &orders,
                |key| std::future::ready(reader.lookup(&key)),
            ))
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod contract_config_tests {
    use super::*;
    use contract::ContractConfigError as E;

    /// A `ContractConfig` with every optional field present, so individual tests
    /// can null out exactly the field under test.
    fn full_config(security_type: &str) -> ContractConfig {
        ContractConfig {
            name: "TEST".to_string(),
            symbol: "AAPL".to_string(),
            security_type: security_type.to_string(),
            exchange: "SMART".to_string(),
            currency: "USD".to_string(),
            last_trade_date: Some("20260116".to_string()),
            strike: Some(150.0),
            right: Some("C".to_string()),
        }
    }

    #[test]
    fn stk_and_cash_ignore_option_fields() {
        // STK/CASH never read last_trade_date/strike/right, so absence is fine.
        let mut stk = full_config("STK");
        stk.last_trade_date = None;
        stk.strike = None;
        stk.right = None;
        assert!(stk.to_contract().is_ok());

        let mut cash = full_config("CASH");
        cash.last_trade_date = None;
        cash.strike = None;
        cash.right = None;
        assert!(cash.to_contract().is_ok());
    }

    #[test]
    fn valid_fut_and_opt_build() {
        assert!(full_config("FUT").to_contract().is_ok());
        assert!(full_config("OPT").to_contract().is_ok());
    }

    #[test]
    fn opt_missing_right_is_error_not_silent_call() {
        let mut cfg = full_config("OPT");
        cfg.right = None;
        assert_eq!(cfg.to_contract().unwrap_err(), E::MissingOptionRight);
    }

    #[test]
    fn opt_unrecognized_right_is_error() {
        let mut cfg = full_config("OPT");
        cfg.right = Some("X".to_string());
        assert_eq!(
            cfg.to_contract().unwrap_err(),
            E::UnrecognizedOptionRight {
                right: "X".to_string()
            }
        );
    }

    #[test]
    fn opt_missing_strike_is_error() {
        let mut cfg = full_config("OPT");
        cfg.strike = None;
        assert_eq!(cfg.to_contract().unwrap_err(), E::MissingStrike);
    }

    #[test]
    fn fut_and_opt_missing_last_trade_date_is_error() {
        let mut fut = full_config("FUT");
        fut.last_trade_date = None;
        assert_eq!(fut.to_contract().unwrap_err(), E::MissingLastTradeDate);

        let mut opt = full_config("OPT");
        opt.last_trade_date = None;
        assert_eq!(opt.to_contract().unwrap_err(), E::MissingLastTradeDate);
    }

    #[test]
    fn unknown_security_type_is_error_not_silent_stk() {
        let cfg = full_config("BOND");
        assert_eq!(
            cfg.to_contract().unwrap_err(),
            E::UnrecognizedSecurityType {
                security_type: "BOND".to_string()
            }
        );
    }

    fn ibkr_config(contracts: Vec<ContractConfig>) -> IbkrConfig {
        IbkrConfig {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 1,
            account: String::new(),
            contracts,
        }
    }

    fn named(name: &str, security_type: &str) -> ContractConfig {
        ContractConfig {
            name: name.to_string(),
            ..full_config(security_type)
        }
    }

    #[test]
    fn validate_config_accepts_each_security_type_its_kind_takes() {
        let names = ["STOCK", "FX", "FUTURE", "OPTION"].map(InstrumentNameExchange::new);
        let instruments = [
            ClientInstrument::new(&names[0], InstrumentKindDiscriminant::Spot),
            ClientInstrument::new(&names[1], InstrumentKindDiscriminant::Spot),
            ClientInstrument::new(&names[2], InstrumentKindDiscriminant::Future),
            ClientInstrument::new(&names[3], InstrumentKindDiscriminant::Option),
        ];
        let config = ibkr_config(vec![
            named("STOCK", "STK"),
            named("FX", "CASH"),
            named("FUTURE", "FUT"),
            named("OPTION", "OPT"),
        ]);

        assert_eq!(IbkrClient::validate_config(&config, &instruments), Ok(()));
    }

    /// The disagreement the kind gate cannot see: a stock contract registered for an instrument
    /// the engine models as an option would route option orders as stock orders.
    #[test]
    fn validate_config_rejects_a_security_type_its_instrument_kind_contradicts() {
        let name = InstrumentNameExchange::new("AAPL-C150");
        let instruments = [ClientInstrument::new(
            &name,
            InstrumentKindDiscriminant::Option,
        )];
        let config = ibkr_config(vec![named("AAPL-C150", "STK")]);

        let error = IbkrClient::validate_config(&config, &instruments).unwrap_err();
        assert!(error.contains("AAPL-C150"), "{error}");
        assert!(error.contains("STK"), "{error}");
        assert!(error.contains("OPT"), "{error}");
    }

    /// `connect_sync` refuses an invalid entry only once connected; built through
    /// `ExecutionBuilder`, it fails the build before connecting. Every problem is reported, not just the first.
    #[test]
    fn validate_config_reports_every_invalid_or_contradicting_entry() {
        let name = InstrumentNameExchange::new("ES");
        let instruments = [ClientInstrument::new(
            &name,
            InstrumentKindDiscriminant::Future,
        )];
        let mut missing_date = named("NQ", "FUT");
        missing_date.last_trade_date = None;
        let config = ibkr_config(vec![named("ES", "STK"), missing_date]);

        let error = IbkrClient::validate_config(&config, &instruments).unwrap_err();
        assert!(error.contains("\"ES\""), "{error}");
        assert!(error.contains("\"NQ\""), "{error}");
    }

    /// Every (kind, security type) pair: accepted exactly where `security_types_of` lists it, and
    /// a kind this client cannot trade rejected whatever the type.
    #[test]
    fn validate_config_checks_every_kind_against_every_security_type() {
        use InstrumentKindDiscriminant as K;
        let accepted = [
            (K::Spot, "STK"),
            (K::Spot, "CASH"),
            (K::Future, "FUT"),
            (K::Option, "OPT"),
        ];
        let name = InstrumentNameExchange::new("X");
        for kind in [K::Spot, K::Perpetual, K::Future, K::Option, K::Cfd] {
            for security_type in ["STK", "CASH", "FUT", "OPT"] {
                let instruments = [ClientInstrument::new(&name, kind)];
                let config = ibkr_config(vec![named("X", security_type)]);
                assert_eq!(
                    IbkrClient::validate_config(&config, &instruments).is_ok(),
                    accepted.contains(&(kind, security_type)),
                    "{kind} with {security_type}"
                );
            }
        }
    }

    /// `security_types_of` is written separately from `to_contract`'s match, so a type added
    /// there must be added here too: an entry naming a type no kind lists fails every check.
    #[test]
    fn security_types_cover_every_type_to_contract_builds() {
        use InstrumentKindDiscriminant as K;
        let listed = [K::Spot, K::Perpetual, K::Future, K::Option, K::Cfd]
            .into_iter()
            .flat_map(security_types_of)
            .copied()
            .collect::<Vec<_>>();

        // Every type listed must be one `to_contract` can build.
        for security_type in &listed {
            assert!(
                full_config(security_type).to_contract().is_ok(),
                "{security_type}"
            );
        }

        // Every type `to_contract` builds must be listed. A `match` cannot be enumerated, so this
        // names them by hand, and a type added to `to_contract` must be added here as well.
        // `BOND` stands for a type it does not build.
        for security_type in ["STK", "CASH", "FUT", "OPT", "BOND"] {
            assert_eq!(
                full_config(security_type).to_contract().is_ok(),
                listed.contains(&security_type),
                "{security_type}"
            );
        }
    }

    /// A kind this client cannot trade is named as such, rather than as a mismatch with no
    /// security type to expect.
    #[test]
    fn validate_config_names_a_kind_this_client_cannot_trade() {
        let name = InstrumentNameExchange::new("X");
        let instruments = [ClientInstrument::new(
            &name,
            InstrumentKindDiscriminant::Perpetual,
        )];
        let config = ibkr_config(vec![named("X", "STK")]);

        let error = IbkrClient::validate_config(&config, &instruments).unwrap_err();
        assert!(error.contains("perpetual"), "{error}");
        assert!(error.contains("cannot trade"), "{error}");
    }

    /// A contract can also be registered later with `register_contract`, so neither side of an
    /// unmatched pair is an error here.
    #[test]
    fn validate_config_accepts_entries_and_instruments_without_a_counterpart() {
        let name = InstrumentNameExchange::new("MSFT");
        let instruments = [ClientInstrument::new(
            &name,
            InstrumentKindDiscriminant::Spot,
        )];
        let config = ibkr_config(vec![named("SOMETHING-ELSE", "OPT")]);

        assert_eq!(IbkrClient::validate_config(&config, &instruments), Ok(()));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod order_status_tests {
    use super::*;
    use crate::order::state::ActiveOrderState;
    use ibapi::orders::{OrderStatus, OrderStatusKind, PlaceOrder};

    fn status(kind: OrderStatusKind, filled: f64) -> Result<PlaceOrder, ibapi::Error> {
        Ok(PlaceOrder::OrderStatus(OrderStatus {
            order_id: 42,
            status: kind,
            filled,
            ..OrderStatus::default()
        }))
    }

    fn ctx() -> OrderContext {
        OrderContext {
            instrument: InstrumentNameExchange::new("AAPL"),
            side: Side::Buy,
            price: Some(Decimal::from(150)),
            quantity: Decimal::from(10),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        }
    }

    /// ibapi 4.0 preserves an unrecognised TWS status as
    /// `OrderStatusKind::Unknown(raw)` instead of failing the subscription with
    /// `Error::Parse`. Placement must treat it as live-but-indeterminate: the
    /// order exists, so the order-id mapping has to survive for the account
    /// stream to resolve it.
    #[test]
    fn unknown_status_is_held_pending_during_placement() {
        let events = vec![status(OrderStatusKind::Unknown("Reclassified".into()), 0.0)];

        match await_order_placement(events) {
            PlacementOutcome::HeldPending(reason) => {
                assert!(
                    reason.contains("Reclassified"),
                    "the raw status must reach the caller, got {reason:?}"
                );
            }
            other => panic!("expected HeldPending, got {other:?}"),
        }
    }

    /// Specifically *not* the `ApiPending` treatment. Falling through to the
    /// `PLACEMENT_STATUS_TIMEOUT` backstop yields `NoStatus`, which on the
    /// bracket path cancels all three legs — so an unrecognised status must
    /// produce a decision here rather than deferring to the timeout.
    #[test]
    fn unknown_status_does_not_fall_through_to_no_status() {
        let events = vec![status(OrderStatusKind::Unknown("Reclassified".into()), 0.0)];
        assert!(!matches!(
            await_order_placement(events),
            PlacementOutcome::NoStatus
        ));

        // ApiPending, by contrast, keeps waiting and exhausts the iterator.
        let events = vec![status(OrderStatusKind::ApiPending, 0.0)];
        assert!(matches!(
            await_order_placement(events),
            PlacementOutcome::NoStatus
        ));
    }

    /// ibapi 4.2 fails an in-flight placement with a transport error the moment
    /// the socket drops. TWS may already hold the order, so it must stay
    /// trackable (`NoStatus` keeps the order-id mapping), not read as rejected.
    #[test]
    fn transport_loss_while_awaiting_placement_is_no_status() {
        for e in [
            ibapi::Error::ConnectionReset,
            ibapi::Error::ConnectionFailed,
            ibapi::Error::Shutdown,
            ibapi::Error::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        ] {
            let label = e.to_string();
            assert!(
                matches!(
                    await_order_placement(vec![Err(e)]),
                    PlacementOutcome::NoStatus
                ),
                "{label} must leave the placement unresolved, not rejected"
            );
        }

        // A status that arrived before the drop still decides.
        let events = vec![
            status(OrderStatusKind::Submitted, 0.0),
            Err(ibapi::Error::ConnectionReset),
        ];
        assert!(matches!(
            await_order_placement(events),
            PlacementOutcome::Accepted { .. }
        ));
    }

    #[test]
    fn non_transport_error_while_awaiting_placement_is_rejected() {
        let events = vec![Err(ibapi::Error::Simple("boom".into()))];
        assert!(matches!(
            await_order_placement(events),
            PlacementOutcome::Rejected(reason) if reason.contains("boom")
        ));
    }

    /// A send refused because the transport is down or reconnecting never
    /// reached TWS: it is a transient connectivity error, not a venue rejection.
    #[test]
    fn send_error_separates_transport_loss_from_refusal() {
        let refused: OrderError<AssetNameExchange, InstrumentNameExchange> =
            send_error(&ibapi::Error::ConnectionReset, "down".into());
        assert!(
            matches!(&refused, OrderError::Connectivity(ConnectivityError::Socket(m)) if m == "down")
        );
        assert!(refused.is_transient());

        let io: OrderError<AssetNameExchange, InstrumentNameExchange> = send_error(
            &ibapi::Error::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            "io".into(),
        );
        assert!(io.is_transient());

        // ibapi has given up reconnecting: retrying cannot succeed.
        for e in [
            ibapi::Error::Shutdown,
            ibapi::Error::ConnectionFailed,
            ibapi::Error::Simple("no".into()),
        ] {
            let label = e.to_string();
            let rejected: OrderError<AssetNameExchange, InstrumentNameExchange> =
                send_error(&e, "no".into());
            assert!(
                matches!(&rejected, OrderError::Rejected(ApiError::OrderRejected(m)) if m == "no"),
                "{label} must be a non-transient refusal"
            );
            assert!(!rejected.is_transient(), "{label}");
        }
    }

    /// Regression anchors: the statuses that were already decisive must keep
    /// their outcomes across the 4.0 migration.
    #[test]
    fn known_statuses_keep_their_placement_outcomes() {
        assert!(matches!(
            await_order_placement(vec![status(OrderStatusKind::Submitted, 0.0)]),
            PlacementOutcome::Accepted { .. }
        ));
        assert!(matches!(
            await_order_placement(vec![status(OrderStatusKind::Filled, 10.0)]),
            PlacementOutcome::Accepted { filled } if filled == 10.0
        ));
        assert!(matches!(
            await_order_placement(vec![status(OrderStatusKind::Inactive, 0.0)]),
            PlacementOutcome::Rejected(_)
        ));
        assert!(matches!(
            await_order_placement(vec![status(OrderStatusKind::Cancelled, 0.0)]),
            PlacementOutcome::Rejected(_)
        ));
    }

    /// An unmodelled status must not be reported as terminal. Treating it as a
    /// rejection would drop the order-id mapping for an order that may well be
    /// live in the market, discarding its executions and commissions.
    #[test]
    fn unknown_status_maps_to_active_open() {
        let raw = OrderStatus {
            order_id: 42,
            status: OrderStatusKind::Unknown("Reclassified".into()),
            filled: 3.0,
            ..OrderStatus::default()
        };

        let order = make_order_from_status(
            &raw,
            ClientOrderId::new("cid-1"),
            &ctx(),
            &PendingCancels::new(),
        );

        assert!(
            matches!(order.state, OrderState::Active(_)),
            "unmodelled status must stay active, got {:?}",
            order.state
        );
    }

    /// A cancelled order carries what IB reports filled, and a fill that is not a number stays
    /// unknown rather than reading as zero; a filled order filled its whole quantity regardless.
    #[test]
    fn an_ended_status_carries_its_fill_or_none() {
        let ended = |status: OrderStatusKind, filled: f64| {
            let raw = OrderStatus {
                order_id: 42,
                status,
                filled,
                ..OrderStatus::default()
            };
            make_order_from_status(
                &raw,
                ClientOrderId::new("cid-1"),
                &ctx(),
                &PendingCancels::new(),
            )
            .state
        };
        let cancelled_fill = |state: OrderState<_, _>| match state {
            OrderState::Inactive(crate::order::state::InactiveOrderState::Cancelled(cancelled)) => {
                cancelled.filled_quantity
            }
            other => panic!("expected Cancelled, got {other:?}"),
        };

        assert_eq!(
            cancelled_fill(ended(OrderStatusKind::Cancelled, 3.0)),
            Some(Decimal::from(3))
        );
        assert_eq!(
            cancelled_fill(ended(OrderStatusKind::Cancelled, f64::NAN)),
            None
        );
        assert!(matches!(
            ended(OrderStatusKind::Filled, f64::NAN),
            OrderState::Inactive(crate::order::state::InactiveOrderState::FullyFilled(Filled { filled_quantity, .. }))
                if filled_quantity == ctx().quantity
        ));
    }

    /// A good-till-date order cancelled at or past its expiry, with no cancel requested, expired.
    /// One cancelled before its expiry was cancelled.
    #[test]
    fn a_good_till_date_order_cancelled_at_its_expiry_expired() {
        let cancelled_at = |expiry: DateTime<Utc>| {
            let ctx = OrderContext {
                time_in_force: TimeInForce::GoodTillDate { expiry },
                ..ctx()
            };
            let raw = OrderStatus {
                order_id: 42,
                status: OrderStatusKind::Cancelled,
                ..OrderStatus::default()
            };
            make_order_from_status(
                &raw,
                ClientOrderId::new("cid-1"),
                &ctx,
                &PendingCancels::new(),
            )
            .state
        };
        use crate::order::state::InactiveOrderState;

        assert!(matches!(
            cancelled_at(Utc::now() - chrono::Duration::minutes(1)),
            OrderState::Inactive(InactiveOrderState::Expired(_))
        ));
        // IB's clock may run ahead of this host's.
        assert!(matches!(
            cancelled_at(Utc::now() + chrono::Duration::seconds(2)),
            OrderState::Inactive(InactiveOrderState::Expired(_))
        ));
        assert!(matches!(
            cancelled_at(Utc::now() + chrono::Duration::hours(1)),
            OrderState::Inactive(InactiveOrderState::Cancelled(_))
        ));
    }

    /// An unmodelled status must not consume a pending cancel: the entry has to
    /// survive for the confirmed `Cancelled` that may still follow.
    #[test]
    fn unknown_status_leaves_pending_cancel_intact() {
        let pending = PendingCancels::new();
        pending.insert(42);

        let raw = OrderStatus {
            order_id: 42,
            status: OrderStatusKind::Unknown("Reclassified".into()),
            ..OrderStatus::default()
        };
        let _ = make_order_from_status(&raw, ClientOrderId::new("cid-1"), &ctx(), &pending);

        assert!(
            pending.remove(42),
            "the pending cancel must still be there for the confirmed Cancelled"
        );
    }

    /// A rejected leg is settled even when its rollback cancel was refused: its
    /// terminal status will not be replayed, so keeping it Open would leave a
    /// phantom order.
    #[test]
    fn rejected_leg_with_failed_cancel_is_settled() {
        let accepted: Option<Result<f64, String>> = Some(Ok(0.0));
        let rejected: Option<Result<f64, String>> = Some(Err("Cancelled".into()));

        assert_eq!(
            unresolved_legs([(1, &rejected), (2, &accepted), (3, &accepted)], &[1, 2, 3]),
            vec![2, 3]
        );
    }

    /// A later leg's send failure keeps `send_error`'s classification only when
    /// every rollback cancel went out; otherwise it is a non-transient
    /// rejection naming the cancels that failed.
    #[test]
    fn leg_send_error_is_transient_only_after_a_clean_rollback() {
        let clean = leg_send_error(&ibapi::Error::ConnectionReset, "sl failed".into(), "");
        assert!(matches!(clean, OrderError::Connectivity(_)));

        let dirty = leg_send_error(
            &ibapi::Error::ConnectionReset,
            "sl failed".into(),
            "; rollback cancel failed for order ids 10 (connection reset), which may be live",
        );
        match dirty {
            OrderError::Rejected(ApiError::OrderRejected(message)) => {
                assert!(message.starts_with("sl failed; rollback cancel failed"));
            }
            other => panic!("expected a non-transient rejection, got {other:?}"),
        }
    }

    /// A leg is unresolved when it reported no status, or was accepted and its
    /// rollback cancel was refused; a leg whose cancel went out is settled.
    #[test]
    fn unresolved_legs_are_no_status_or_failed_cancel() {
        let accepted: Option<Result<f64, String>> = Some(Ok(0.0));
        let rejected: Option<Result<f64, String>> = Some(Err("201".into()));
        let no_status: Option<Result<f64, String>> = None;

        assert_eq!(
            unresolved_legs([(1, &accepted), (2, &rejected), (3, &no_status)], &[]),
            vec![3]
        );
        assert_eq!(
            unresolved_legs([(1, &accepted), (2, &rejected), (3, &accepted)], &[1]),
            vec![1]
        );
        assert_eq!(
            unresolved_legs([(1, &no_status), (2, &no_status), (3, &accepted)], &[2]),
            vec![1, 2]
        );
        assert!(unresolved_legs([(1, &accepted), (2, &rejected), (3, &accepted)], &[]).is_empty());
    }

    fn bracket_request() -> BracketOrderRequest {
        BracketOrderRequest {
            instrument: InstrumentNameExchange::new("AAPL"),
            strategy: StrategyId::new("test"),
            parent_cid: ClientOrderId::new("br"),
            side: Side::Buy,
            quantity: Decimal::from(10),
            entry_price: Decimal::from(150),
            take_profit_price: Decimal::from(160),
            stop_loss_price: Decimal::from(145),
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        }
    }

    /// An unresolved leg comes back Open under its IB id and keeps its mapping,
    /// so the account stream can still resolve it; a settled leg comes back
    /// Inactive with the error and its mapping is released.
    #[test]
    fn failed_bracket_keeps_unresolved_legs_open_and_tracked() {
        let request = bracket_request();
        let (tp_cid, sl_cid) = derive_child_cids(&request.parent_cid);
        let order_ids = OrderIdMap::new();
        order_ids
            .register(request.parent_cid.clone(), 10, ctx())
            .unwrap();
        order_ids.register(tp_cid, 11, ctx()).unwrap();
        order_ids.register(sl_cid, 12, ctx()).unwrap();

        let result = failed_bracket(
            &request,
            [10, 11, 12],
            BracketFailure {
                error: OrderError::Rejected(ApiError::OrderRejected("boom".into())),
                unresolved: vec![11],
            },
            &order_ids,
        );

        assert!(matches!(result.parent.state, OrderState::Inactive(_)));
        assert!(matches!(result.stop_loss.state, OrderState::Inactive(_)));
        match &result.take_profit.state {
            OrderState::Active(ActiveOrderState::Open(open)) => {
                assert_eq!(open.id.assigned(), Some(&OrderId::new("11")));
                assert_eq!(open.filled_quantity, Decimal::ZERO);
            }
            other => panic!("unresolved leg must be Open, got {other:?}"),
        }

        assert!(order_ids.get_client_id(10).is_none());
        assert!(order_ids.get_client_id(11).is_some());
        assert!(order_ids.get_client_id(12).is_none());
    }

    /// With no unresolved leg the failure keeps its all-inactive shape and
    /// releases every mapping.
    #[test]
    fn failed_bracket_without_unresolved_legs_is_all_inactive() {
        let request = bracket_request();
        let (tp_cid, sl_cid) = derive_child_cids(&request.parent_cid);
        let order_ids = OrderIdMap::new();
        order_ids
            .register(request.parent_cid.clone(), 10, ctx())
            .unwrap();
        order_ids.register(tp_cid, 11, ctx()).unwrap();
        order_ids.register(sl_cid, 12, ctx()).unwrap();

        let result = failed_bracket(
            &request,
            [10, 11, 12],
            BracketFailure {
                error: OrderError::Rejected(ApiError::OrderRejected("boom".into())),
                unresolved: Vec::new(),
            },
            &order_ids,
        );

        for leg in [&result.parent, &result.take_profit, &result.stop_loss] {
            assert!(matches!(leg.state, OrderState::Inactive(_)));
        }
        for ib_id in [10, 11, 12] {
            assert!(order_ids.get_client_id(ib_id).is_none());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod order_reader_tests {
    use super::*;
    use crate::client::dedup::new_dedup_cache;
    use crate::trade::TradeId;
    use ibapi::orders::{OrderStatus, OrderUpdate};
    use std::cell::Cell;

    /// Status updates for an order this client did not place, so none is forwarded.
    fn unforwarded(n: usize) -> Vec<Result<OrderUpdate, ibapi::Error>> {
        (0..n)
            .map(|_| {
                Ok(OrderUpdate::OrderStatus(OrderStatus {
                    order_id: 999,
                    ..OrderStatus::default()
                }))
            })
            .collect()
    }

    /// Run the reader over `updates` and return how many of them it pulled.
    fn run(sink: &recovery::EventSink, updates: Vec<Result<OrderUpdate, ibapi::Error>>) -> usize {
        let pulled = Cell::new(0);
        forward_order_updates(
            updates
                .into_iter()
                .inspect(|_| pulled.set(pulled.get() + 1)),
            sink,
            0,
            &ContractRegistry::new(),
            &OrderIdMap::new(),
            &PendingCancels::new(),
            &ExecutionBuffer::new(),
        );
        pulled.get()
    }

    #[test]
    fn stops_on_the_first_update_after_the_stream_ends() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );
        sink.terminate(StreamTerminationReason::Error("recovery failed".into()));

        assert_eq!(run(&sink, unforwarded(3)), 1);
    }

    #[test]
    fn stops_on_the_first_update_after_the_consumer_goes() {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );
        drop(rx);

        assert_eq!(run(&sink, unforwarded(3)), 1);
    }

    /// How each terminal status leaves an order's client id and IB-id entry: `Filled` frees the
    /// id and keeps the entry for late executions, `Cancelled`/`Inactive` remove both, and a
    /// working status keeps both.
    #[test]
    fn a_terminal_status_frees_the_orders_client_id() {
        use ibapi::orders::OrderStatusKind;

        for (kind, entry_kept, id_held) in [
            (OrderStatusKind::Filled, true, false),
            (OrderStatusKind::Cancelled, false, false),
            (OrderStatusKind::Inactive, false, false),
            (OrderStatusKind::Submitted, true, true),
        ] {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let sink = recovery::EventSink::new(
                tx,
                new_dedup_cache(),
                KnownLiveOrders::shared(ExchangeId::Ibkr),
            );
            let order_ids = OrderIdMap::new();
            let cid = ClientOrderId::new("cid-7");
            order_ids
                .register(
                    cid.clone(),
                    7,
                    OrderContext {
                        instrument: InstrumentNameExchange::new("AAPL"),
                        side: Side::Buy,
                        price: Some(Decimal::from(100)),
                        quantity: Decimal::ONE,
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    },
                )
                .unwrap();

            forward_order_updates(
                [Ok(OrderUpdate::OrderStatus(OrderStatus {
                    order_id: 7,
                    status: kind.clone(),
                    ..OrderStatus::default()
                }))],
                &sink,
                0,
                &ContractRegistry::new(),
                &order_ids,
                &PendingCancels::new(),
                &ExecutionBuffer::new(),
            );

            assert!(rx.try_recv().is_ok(), "{kind:?}: the status is forwarded");
            assert_eq!(
                order_ids.get_client_id(7).is_some(),
                entry_kept,
                "{kind:?}: entry"
            );
            assert_eq!(order_ids.get_ib_id(&cid).is_some(), id_held, "{kind:?}: id");
        }
    }

    /// Another API client's status under a tracked order's IB id is not this client's order's:
    /// IB numbers orders per client. It is dropped, and the tracked order keeps its entry.
    #[test]
    fn another_api_clients_status_does_not_reach_a_tracked_order() {
        use ibapi::orders::OrderStatusKind;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );
        let order_ids = OrderIdMap::new();
        let cid = ClientOrderId::new("cid-7");
        order_ids
            .register(
                cid.clone(),
                7,
                OrderContext {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    side: Side::Buy,
                    price: Some(Decimal::from(100)),
                    quantity: Decimal::ONE,
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                },
            )
            .unwrap();

        forward_order_updates(
            [Ok(OrderUpdate::OrderStatus(OrderStatus {
                order_id: 7,
                client_id: 905,
                status: OrderStatusKind::Cancelled,
                ..OrderStatus::default()
            }))],
            &sink,
            903,
            &ContractRegistry::new(),
            &order_ids,
            &PendingCancels::new(),
            &ExecutionBuffer::new(),
        );

        assert!(
            matches!(
                rx.try_recv().map(|event| event.kind),
                Ok(AccountEventKind::StreamTerminated(_))
            ),
            "nothing forwarded before the stream ended"
        );
        assert_eq!(order_ids.get_ib_id(&cid), Some(7));
        assert!(order_ids.contains(7));
    }

    /// A status IB re-sends for a filled order, after its id names a later order, is dropped
    /// rather than read as the later order's, which it would end. The later order's own status is
    /// forwarded.
    #[test]
    fn a_resent_status_for_an_ended_order_does_not_reach_a_reused_id() {
        use ibapi::orders::OrderStatusKind;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );
        let order_ids = OrderIdMap::new();
        let cid = ClientOrderId::new("reused");
        let ctx = || OrderContext {
            instrument: InstrumentNameExchange::new("AAPL"),
            side: Side::Buy,
            price: Some(Decimal::from(100)),
            quantity: Decimal::ONE,
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        };
        let status = |order_id, status| {
            Ok(OrderUpdate::OrderStatus(OrderStatus {
                order_id,
                status,
                ..OrderStatus::default()
            }))
        };
        order_ids.register(cid.clone(), 1, ctx()).unwrap();
        // One stream, which ends with the updates: the first order fills, its id is registered
        // again for a second order, then IB re-sends the first order's fill.
        let updates = [status(1, OrderStatusKind::Filled)]
            .into_iter()
            .chain(std::iter::once_with(|| {
                order_ids.register(cid.clone(), 2, ctx()).unwrap();
                status(1, OrderStatusKind::Filled)
            }))
            .chain([status(2, OrderStatusKind::Submitted)]);
        forward_order_updates(
            updates,
            &sink,
            0,
            &ContractRegistry::new(),
            &order_ids,
            &PendingCancels::new(),
            &ExecutionBuffer::new(),
        );

        let Ok(first) = rx.try_recv() else {
            panic!("the first order's fill is forwarded");
        };
        assert!(
            matches!(first.kind, AccountEventKind::OrderSnapshot(Snapshot(ref order)) if !matches!(order.state, OrderState::Active(_))),
            "{first:?}"
        );
        let Ok(event) = rx.try_recv() else {
            panic!("the later order's status is forwarded");
        };
        let AccountEventKind::OrderSnapshot(Snapshot(order)) = event.kind else {
            panic!("an order snapshot, got {event:?}");
        };
        assert!(
            matches!(order.state, OrderState::Active(_)),
            "it is the later, working order's: {order:?}"
        );
        assert!(
            matches!(
                rx.try_recv().map(|event| event.kind),
                Ok(AccountEventKind::StreamTerminated(_))
            ),
            "and the re-sent fill was dropped: nothing else before the stream ended"
        );
        assert_eq!(order_ids.get_ib_id(&cid), Some(2));
    }

    /// An execution and its correction, each completed by its commission report, reach the
    /// stream as a trade and an amendment of it, never as two trades.
    #[test]
    fn a_corrected_execution_is_reported_as_an_amendment() {
        use crate::trade::TradeAmendmentKind;
        use ibapi::{
            contracts::Contract,
            orders::{CommissionReport, Execution, ExecutionData},
        };

        let contracts = ContractRegistry::new();
        contracts
            .register(
                InstrumentNameExchange::new("AAPL"),
                Contract {
                    contract_id: 265598,
                    ..Contract::default()
                },
            )
            .unwrap();
        let order_ids = OrderIdMap::new();
        order_ids
            .register(
                ClientOrderId::new("cid-7"),
                7,
                OrderContext {
                    instrument: InstrumentNameExchange::new("AAPL"),
                    side: Side::Buy,
                    price: Some(Decimal::from(100)),
                    quantity: Decimal::ONE,
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                },
            )
            .unwrap();
        let execution = |exec_id: &str, price: f64| {
            Ok(OrderUpdate::ExecutionData(ExecutionData {
                // TWS tags an execution happening now with -1.
                request_id: -1,
                contract: Contract {
                    contract_id: 265598,
                    ..Contract::default()
                },
                execution: Execution {
                    order_id: 7,
                    execution_id: exec_id.to_string(),
                    time: "20261005 14:00:00 UTC".to_string(),
                    shares: 1.0,
                    price,
                    cumulative_quantity: 1.0,
                    ..Execution::default()
                },
            }))
        };
        let commission = |exec_id: &str| {
            Ok(OrderUpdate::CommissionReport(CommissionReport {
                execution_id: exec_id.to_string(),
                commission: 1.0,
                currency: "USD".to_string(),
                ..CommissionReport::default()
            }))
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );
        forward_order_updates(
            [
                execution("0000e0d5.5f8b1c2a.01.01", 100.0),
                commission("0000e0d5.5f8b1c2a.01.01"),
                execution("0000e0d5.5f8b1c2a.01.02", 100.5),
                commission("0000e0d5.5f8b1c2a.01.02"),
            ],
            &sink,
            0,
            &contracts,
            &order_ids,
            &PendingCancels::new(),
            &ExecutionBuffer::new(),
        );

        let AccountEventKind::Trade(original) = rx.try_recv().unwrap().kind else {
            panic!("expected the original as a trade");
        };
        assert_eq!(original.id, TradeId::new("0000e0d5.5f8b1c2a.01.01"));
        let AccountEventKind::TradeAmended(amendment) = rx.try_recv().unwrap().kind else {
            panic!("expected the correction as an amendment");
        };
        assert_eq!(amendment.original, Some(original.id));
        // The IB order id, which the order's `Open` state carries, not the client order id.
        assert_eq!(amendment.order_id, OrderId::new("7"));
        let TradeAmendmentKind::Corrected { replacement } = amendment.kind else {
            panic!("expected a resolved correction, got {:?}", amendment.kind);
        };
        assert_eq!(replacement.id, TradeId::new("0000e0d5.5f8b1c2a.01.02"));
        assert_eq!(replacement.price, Decimal::new(1005, 1));
        assert_eq!(replacement.fees.fees, Decimal::ONE);
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            AccountEventKind::StreamTerminated(_)
        ));
    }

    #[test]
    fn a_shutdown_terminates_the_stream_as_one() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );

        let mut updates = unforwarded(1);
        updates.push(Err(ibapi::Error::Shutdown));
        updates.extend(unforwarded(1));

        assert_eq!(run(&sink, updates), 2, "nothing is read past the shutdown");
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            AccountEventKind::StreamTerminated(StreamTerminationReason::Error(ref reason))
                if reason == recovery::CLIENT_SHUT_DOWN
        ));
        assert!(
            rx.try_recv().is_err(),
            "the stream ends with its termination"
        );
    }

    #[test]
    fn open_stream_reads_past_unforwarded_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(
            tx,
            new_dedup_cache(),
            KnownLiveOrders::shared(ExchangeId::Ibkr),
        );

        assert_eq!(run(&sink, unforwarded(3)), 3);
        // The subscription ended, so the stream says so.
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            AccountEventKind::StreamTerminated(StreamTerminationReason::Error(ref reason))
                if reason == "IBKR order-update stream ended"
        ));
    }

    mod listings {
        use super::*;
        use ibapi::{
            contracts::Contract,
            orders::{
                CommissionReport, Execution, ExecutionData, Executions, OrderData,
                OrderState as IbOrderState, OrderStatusKind, Orders,
            },
            subscriptions::SubscriptionItem,
        };
        use rust_decimal_macros::dec;
        use std::time::Duration;

        const API_CLIENT: i32 = 903;
        const CON_ID: i32 = 265598;

        fn contracts() -> ContractRegistry {
            let contracts = ContractRegistry::new();
            contracts
                .register(
                    InstrumentNameExchange::new("AAPL"),
                    Contract {
                        contract_id: CON_ID,
                        ..Contract::default()
                    },
                )
                .unwrap();
            contracts
        }

        /// A `next_timeout` that hands out `items`, then answers as IB does at the end of a
        /// listing, at once; or, with `stalls`, only once the wait has run out.
        fn subscription<T>(
            items: Vec<Result<SubscriptionItem<T>, ibapi::Error>>,
            stalls: bool,
        ) -> impl FnMut(Duration) -> Option<Result<SubscriptionItem<T>, ibapi::Error>> {
            let mut items = items.into_iter();
            move |timeout| {
                items.next().or_else(|| {
                    if stalls {
                        std::thread::sleep(timeout);
                    }
                    None
                })
            }
        }

        fn read(
            items: Vec<Result<SubscriptionItem<u32>, ibapi::Error>>,
            stalls: bool,
            stop_at: Option<u32>,
        ) -> Result<Vec<u32>, UnindexedClientError> {
            read_while(items, stalls, stop_at, true)
        }

        fn read_while(
            items: Vec<Result<SubscriptionItem<u32>, ibapi::Error>>,
            stalls: bool,
            stop_at: Option<u32>,
            connected: bool,
        ) -> Result<Vec<u32>, UnindexedClientError> {
            let mut read = Vec::new();
            read_listing(
                "test",
                Duration::from_millis(40),
                || connected,
                subscription(items, stalls),
                |item| {
                    read.push(item);
                    if Some(item) == stop_at {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            )?;
            Ok(read)
        }

        #[test]
        fn a_listing_read_ends_at_the_end_of_the_listing() {
            let items = || vec![Ok(SubscriptionItem::Data(1)), Ok(SubscriptionItem::Data(2))];
            assert_eq!(read(items(), false, None).unwrap(), [1, 2]);
            assert_eq!(
                read(items(), false, Some(1)).unwrap(),
                [1],
                "or where it breaks"
            );
        }

        /// What a stalled or failed read holds is not the whole listing, so it is not returned.
        #[test]
        fn a_stalled_or_failed_listing_read_is_an_error() {
            let stalled = read(vec![Ok(SubscriptionItem::Data(1))], true, None);
            assert!(
                matches!(&stalled, Err(UnindexedClientError::Internal(m)) if m.contains("sent nothing")),
                "{stalled:?}"
            );
            let failed = read(
                vec![
                    Ok(SubscriptionItem::Data(1)),
                    Err(ibapi::Error::ConnectionReset),
                ],
                false,
                None,
            );
            assert!(
                matches!(failed, Err(UnindexedClientError::Internal(_))),
                "{failed:?}"
            );
        }

        /// `ibapi` answers early when it drops a subscription's channel as well as at the end
        /// marker, so a listing that stops while the client is disconnected is not its end.
        #[test]
        fn a_listing_that_stops_while_disconnected_is_an_error() {
            let read = read_while(vec![Ok(SubscriptionItem::Data(1))], false, None, false);
            assert!(
                matches!(&read, Err(UnindexedClientError::Internal(m)) if m.contains("connection dropped")),
                "{read:?}"
            );
        }

        fn contract(contract_id: i32) -> Contract {
            Contract {
                contract_id,
                ..Contract::default()
            }
        }

        fn ib_notice(code: i32) -> ListingError {
            ListingError::Ibapi(ibapi::Error::Notice(ibapi::Notice {
                request_id: Some(9),
                code,
                message: "notice".to_string(),
                error_time: None,
                advanced_order_reject_json: String::new(),
            }))
        }

        /// A description resolves only when IB lists exactly one contract for it.
        #[test]
        fn a_contract_description_resolves_only_to_a_single_match() {
            assert_eq!(
                classify_contract_details(Ok(()), vec![contract(1)]),
                Ok(contract(1))
            );
            assert_eq!(
                classify_contract_details(Ok(()), Vec::new()),
                Err(ResolveContractError::NoMatch)
            );
            assert_eq!(
                classify_contract_details(Ok(()), vec![contract(1), contract(2)]),
                Err(ResolveContractError::Ambiguous {
                    matches: vec![contract(1), contract(2)]
                })
            );
        }

        /// IB answers a description that matches nothing with error 200, and any other error
        /// is a refusal. A failure, even after some matches arrived, fails the resolution: the
        /// matches read are not all there are.
        #[test]
        fn a_failed_contract_details_read_says_whether_a_retry_can_help() {
            let classify = |failure| classify_contract_details(Err(failure), vec![contract(1)]);

            assert_eq!(
                classify(ib_notice(NO_SECURITY_DEFINITION_CODE)),
                Err(ResolveContractError::NoMatch)
            );
            assert_eq!(
                classify(ib_notice(321)),
                Err(ResolveContractError::Refused {
                    code: 321,
                    message: "notice".to_string()
                })
            );

            let transient = [
                (
                    ListingError::Dropped,
                    ConnectivityError::Socket(ListingError::Dropped.to_string()),
                ),
                (
                    ListingError::Stalled(Duration::from_secs(10)),
                    ConnectivityError::Timeout,
                ),
                (
                    ListingError::Ibapi(ibapi::Error::ConnectionReset),
                    ConnectivityError::Socket(ibapi::Error::ConnectionReset.to_string()),
                ),
            ];
            for (failure, connectivity) in transient {
                let classified = classify(failure);
                assert_eq!(
                    classified,
                    Err(ResolveContractError::Connectivity(connectivity))
                );
                assert!(classified.unwrap_err().is_transient());
            }

            // ibapi has given up reconnecting, or the client is shut down: retrying cannot help.
            for permanent in [ibapi::Error::ConnectionFailed, ibapi::Error::Shutdown] {
                let classified = classify(ListingError::Ibapi(permanent));
                assert!(
                    matches!(&classified, Err(ResolveContractError::Failed(_))),
                    "{classified:?}"
                );
                assert!(!classified.unwrap_err().is_transient());
            }
            assert!(
                !ResolveContractError::Refused {
                    code: 321,
                    message: String::new()
                }
                .is_transient()
            );
            assert!(!ResolveContractError::NoMatch.is_transient());
        }

        /// The account summary stays live after its listing, so only its `End` item ends the
        /// read; one that stops before it is not a whole balance set.
        #[test]
        fn an_account_summary_read_needs_its_end_item() {
            use ibapi::accounts::{AccountSummary, AccountSummaryResult};

            let summary = || {
                Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(
                    AccountSummary {
                        account: "DU1".to_string(),
                        tag: "TotalCashValue".to_string(),
                        value: "100".to_string(),
                        currency: "USD".to_string(),
                    },
                )))
            };
            let read = |items| {
                read_account_summary(
                    Duration::from_millis(40),
                    || true,
                    subscription(items, false),
                )
            };

            let balances = read(vec![
                summary(),
                Ok(SubscriptionItem::Data(AccountSummaryResult::End)),
            ])
            .unwrap()
            .to_balances();
            assert_eq!(balances.len(), 1, "{balances:?}");
            let cut_short = read(vec![summary()]);
            assert!(
                matches!(cut_short, Err(UnindexedClientError::Internal(_))),
                "{cut_short:?}"
            );
        }

        fn listed(api_client_id: i32, order_id: i32, order: ibapi::orders::Order) -> Orders {
            Orders::OrderData(OrderData {
                order_id,
                contract: Contract {
                    contract_id: CON_ID,
                    ..Contract::default()
                },
                order: ibapi::orders::Order {
                    client_id: api_client_id,
                    order_id,
                    ..order
                },
                order_state: IbOrderState {
                    status: OrderStatusKind::Submitted,
                    ..IbOrderState::default()
                },
            })
        }

        fn status(
            api_client_id: i32,
            order_id: i32,
            status: OrderStatusKind,
            filled: f64,
        ) -> Orders {
            Orders::OrderStatus(OrderStatus {
                order_id,
                client_id: api_client_id,
                status,
                filled,
                ..OrderStatus::default()
            })
        }

        fn stop_order() -> ibapi::orders::Order {
            ibapi::orders::Order {
                action: ibapi::orders::Action::Sell,
                order_type: "STP".to_owned(),
                total_quantity: 3.0,
                aux_price: Some(140.5),
                tif: ibapi::orders::TimeInForce::Day,
                ..ibapi::orders::Order::default()
            }
        }

        /// This client's open orders, by IB order id: a tracked one as it was placed, under its
        /// client order id; another read back from IB's listing, under its IB order id. Each with
        /// the fill its status reports.
        #[test]
        fn open_orders_are_this_api_clients_with_their_fills() {
            let order_ids = OrderIdMap::new();
            order_ids
                .register(
                    ClientOrderId::new("tracked"),
                    2,
                    OrderContext {
                        instrument: InstrumentNameExchange::new("AAPL"),
                        side: Side::Buy,
                        price: Some(dec!(150)),
                        quantity: dec!(10),
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    },
                )
                .unwrap();

            let listed_orders = open_orders_from_listing(
                [
                    listed(API_CLIENT, 5, stop_order()),
                    status(API_CLIENT, 5, OrderStatusKind::PreSubmitted, 0.0),
                    // IB lists the tracked order with whatever it holds; what it was placed with
                    // stands.
                    listed(API_CLIENT, 2, ibapi::orders::Order::default()),
                    status(API_CLIENT, 2, OrderStatusKind::Submitted, 4.0),
                    // Another API client's order under the tracked order's id.
                    listed(904, 2, stop_order()),
                    status(904, 2, OrderStatusKind::Submitted, 9.0),
                ],
                API_CLIENT,
                &contracts(),
                &order_ids,
            );

            assert_eq!(listed_orders.len(), 2, "{listed_orders:?}");
            // Only the tracked order is named as placed: the other has no order reference.
            let exact: Vec<_> = listed_orders
                .iter()
                .map(ListedOpenOrder::is_exact)
                .collect();
            assert_eq!(exact, [true, false]);
            let orders: Vec<_> = listed_orders
                .into_iter()
                .map(|listed| listed.order.unwrap())
                .collect();
            let (tracked, untracked) = (&orders[0], &orders[1]);
            assert_eq!(tracked.key.cid, ClientOrderId::new("tracked"));
            assert_eq!(tracked.kind, OrderKind::Limit);
            assert_eq!(tracked.price, Some(dec!(150)));
            assert_eq!(tracked.quantity, dec!(10));
            assert_eq!(tracked.state.id, VenueOrderId::Assigned(OrderId::new("2")));
            assert_eq!(tracked.state.filled_quantity, dec!(4));

            assert_eq!(untracked.key.cid, ClientOrderId::new("5"));
            assert_eq!(
                untracked.key.instrument,
                InstrumentNameExchange::new("AAPL")
            );
            assert_eq!(untracked.side, Side::Sell);
            assert_eq!(untracked.quantity, dec!(3));
            assert_eq!(
                untracked.kind,
                OrderKind::Stop {
                    trigger_price: dec!(140.5)
                }
            );
            assert_eq!(untracked.time_in_force, TimeInForce::GoodUntilEndOfDay);
            assert_eq!(
                untracked.state.id,
                VenueOrderId::Assigned(OrderId::new("5"))
            );
            assert_eq!(untracked.state.filled_quantity, Decimal::ZERO);
        }

        /// An order that has just ended, and one in a contract not registered, are not listed;
        /// one whose shape this client never sends is listed without an order, and not exactly.
        #[test]
        fn open_orders_leave_out_what_cannot_be_reported_open() {
            let unregistered = Orders::OrderData(OrderData {
                order_id: 3,
                contract: Contract {
                    contract_id: 1,
                    ..Contract::default()
                },
                order: ibapi::orders::Order {
                    client_id: API_CLIENT,
                    ..stop_order()
                },
                order_state: IbOrderState::default(),
            });
            let unreadable = ibapi::orders::Order {
                order_type: "REL".to_owned(),
                ..stop_order()
            };

            let orders = open_orders_from_listing(
                [
                    listed(API_CLIENT, 1, stop_order()),
                    status(API_CLIENT, 1, OrderStatusKind::Cancelled, 0.0),
                    unregistered,
                    listed(API_CLIENT, 4, unreadable),
                ],
                API_CLIENT,
                &contracts(),
                &OrderIdMap::new(),
            );
            assert_eq!(orders.len(), 1, "{orders:?}");
            assert!(orders[0].order.is_none());
            assert!(!orders[0].is_exact());
        }

        fn referenced(order_ref: &str) -> ibapi::orders::Order {
            ibapi::orders::Order {
                order_ref: order_ref.to_owned(),
                ..stop_order()
            }
        }

        /// An order this client does not track, such as one placed before a restart, is named by
        /// the client order id its order reference carries, and tracked from then on.
        #[test]
        fn an_untracked_order_is_named_by_its_reference_and_adopted() {
            let order_ids = OrderIdMap::new();
            let listed = open_orders_from_listing(
                [
                    listed(API_CLIENT, 7, referenced("before-restart")),
                    status(API_CLIENT, 7, OrderStatusKind::Submitted, 1.0),
                ],
                API_CLIENT,
                &contracts(),
                &order_ids,
            );

            assert_eq!(listed.len(), 1, "{listed:?}");
            assert!(listed[0].is_exact());
            let order = listed[0].order.as_ref().unwrap();
            assert_eq!(order.key.cid, ClientOrderId::new("before-restart"));
            assert_eq!(order.state.filled_quantity, dec!(1));
            assert_eq!(
                order_ids.get_ib_id(&ClientOrderId::new("before-restart")),
                Some(7)
            );
            let (_, ctx) = order_ids.get_client_id_and_context(7).unwrap();
            assert_eq!(ctx.quantity, dec!(3));
        }

        /// A tracked order under the listed order's IB order id but another client order id is an
        /// earlier order whose id IB reused: the tracked one keeps its mapping, and the listed
        /// order, which cannot be tracked under its reference, is listed under its IB order id and
        /// not exactly.
        #[test]
        fn a_reused_ib_order_id_does_not_name_the_listed_order() {
            let order_ids = OrderIdMap::new();
            order_ids
                .register(
                    ClientOrderId::new("earlier"),
                    7,
                    OrderContext {
                        instrument: InstrumentNameExchange::new("AAPL"),
                        side: Side::Buy,
                        price: Some(dec!(150)),
                        quantity: dec!(10),
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    },
                )
                .unwrap();

            let listed = open_orders_from_listing(
                [
                    listed(API_CLIENT, 7, referenced("later")),
                    status(API_CLIENT, 7, OrderStatusKind::Submitted, 0.0),
                ],
                API_CLIENT,
                &contracts(),
                &order_ids,
            );

            let order = listed[0].order.as_ref().unwrap();
            assert_eq!(order.key.cid, ClientOrderId::new("7"));
            assert!(!listed[0].is_exact());
            assert_eq!(
                order.kind,
                OrderKind::Stop {
                    trigger_price: dec!(140.5)
                }
            );
            assert_eq!(order_ids.get_ib_id(&ClientOrderId::new("earlier")), Some(7));
            assert_eq!(order_ids.get_ib_id(&ClientOrderId::new("later")), None);
        }

        /// The order check's listing names each of this client's orders by its reference, or else
        /// its tracked id, keeps one that has just ended, and adopts nothing.
        #[test]
        fn listed_cids_name_this_clients_orders_without_adopting_them() {
            let order_ids = OrderIdMap::new();
            order_ids
                .register(
                    ClientOrderId::new("tracked"),
                    2,
                    OrderContext {
                        instrument: InstrumentNameExchange::new("AAPL"),
                        side: Side::Buy,
                        price: Some(dec!(150)),
                        quantity: dec!(10),
                        kind: OrderKind::Limit,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    },
                )
                .unwrap();

            let cids = listed_cids(
                [
                    listed(API_CLIENT, 1, referenced("ended")),
                    status(API_CLIENT, 1, OrderStatusKind::Cancelled, 0.0),
                    listed(API_CLIENT, 2, stop_order()),
                    listed(API_CLIENT, 3, stop_order()),
                    listed(904, 4, referenced("other-client")),
                ],
                API_CLIENT,
                &order_ids,
            );

            assert_eq!(
                cids,
                fnv::FnvHashSet::from_iter([
                    ClientOrderId::new("ended"),
                    ClientOrderId::new("tracked")
                ])
            );
            assert_eq!(order_ids.get_ib_id(&ClientOrderId::new("ended")), None);
        }

        /// A snapshot's instrument has complete orders only when each was read back exactly, and
        /// an instrument without a listed order has none open only if the listing was read.
        #[test]
        fn snapshot_orders_are_complete_only_when_each_is_exact() {
            let msft = InstrumentNameExchange::new("MSFT");
            let contracts = contracts();
            contracts
                .register(
                    msft.clone(),
                    Contract {
                        contract_id: 272093,
                        ..Contract::default()
                    },
                )
                .unwrap();
            let in_msft = |order_id, order| {
                let Orders::OrderData(mut data) = listed(API_CLIENT, order_id, order) else {
                    unreachable!()
                };
                data.contract.contract_id = 272093;
                Orders::OrderData(data)
            };
            let listed_orders = open_orders_from_listing(
                [
                    listed(API_CLIENT, 1, referenced("a")),
                    status(API_CLIENT, 1, OrderStatusKind::Submitted, 0.0),
                    // Entered in TWS: no reference, so named by its IB order id.
                    in_msft(2, stop_order()),
                    status(API_CLIENT, 2, OrderStatusKind::Submitted, 0.0),
                    in_msft(3, referenced("b")),
                    status(API_CLIENT, 3, OrderStatusKind::Submitted, 0.0),
                ],
                API_CLIENT,
                &contracts,
                &OrderIdMap::new(),
            );

            let mut orders = SnapshotOrders::new(Some(listed_orders));
            let (aapl_orders, aapl_complete) = orders.take(&InstrumentNameExchange::new("AAPL"));
            assert_eq!(aapl_orders.len(), 1);
            assert!(aapl_complete);
            let (msft_orders, msft_complete) = orders.take(&msft);
            assert_eq!(msft_orders.len(), 2);
            assert!(!msft_complete);
            assert_eq!(
                orders.take(&InstrumentNameExchange::new("TSLA")),
                (vec![], true)
            );

            let mut unread = SnapshotOrders::new(None);
            assert_eq!(unread.take(&msft), (vec![], false));
        }

        fn executed(api_client_id: i32, order_id: i32, exec_id: &str, time: &str) -> Executions {
            Executions::ExecutionData(ExecutionData {
                request_id: 9001,
                contract: Contract {
                    contract_id: CON_ID,
                    ..Contract::default()
                },
                execution: Execution {
                    order_id,
                    client_id: api_client_id,
                    execution_id: exec_id.to_string(),
                    time: time.to_string(),
                    shares: 1.0,
                    price: 100.0,
                    cumulative_quantity: 1.0,
                    ..Execution::default()
                },
            })
        }

        fn commission(exec_id: &str) -> Executions {
            Executions::CommissionReport(CommissionReport {
                execution_id: exec_id.to_string(),
                commission: 1.25,
                currency: "USD".to_string(),
                ..CommissionReport::default()
            })
        }

        /// The trades of a read: this API client's executions in the span, each under its IB
        /// order id, with the fee of its commission report, or an unknown fee without one.
        #[test]
        fn trades_carry_their_ib_order_id_and_commission() {
            let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
            let trades = trades_from_executions(
                [
                    executed(API_CLIENT, 7, "paired", "20261007 10:00:02 UTC"),
                    commission("paired"),
                    // Earlier than the paired one, and returned first: in time order.
                    executed(API_CLIENT, 7, "unpaired", "20261007 10:00:01 UTC"),
                    executed(API_CLIENT, 7, "too-early", "20261007 09:59:59 UTC"),
                    commission("too-early"),
                    executed(905, 7, "other-client", "20261007 10:00:03 UTC"),
                    commission("other-client"),
                ],
                API_CLIENT,
                &contracts(),
                at("2026-10-07T10:00:00Z")..=at("2026-10-07T11:00:00Z"),
                None,
            );

            let read: Vec<_> = trades
                .iter()
                .map(|trade| {
                    (
                        trade.id.0.as_str(),
                        trade.order_id.0.as_str(),
                        trade.fees.asset.as_ref(),
                        trade.fees.fees,
                    )
                })
                .collect();
            assert_eq!(
                read,
                [
                    ("unpaired", "7", execution::UNKNOWN_FEE_ASSET, Decimal::ZERO),
                    ("paired", "7", "USD", dec!(1.25)),
                ]
            );

            let filtered = trades_from_executions(
                [
                    executed(API_CLIENT, 7, "paired", "20261007 10:00:00 UTC"),
                    commission("paired"),
                ],
                API_CLIENT,
                &contracts(),
                at("2026-10-07T10:00:00Z")..=at("2026-10-07T11:00:00Z"),
                Some(&HashSet::from([InstrumentNameExchange::new("MSFT")])),
            );
            assert!(filtered.is_empty(), "{filtered:?}");
        }
    }
}
