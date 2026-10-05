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
//! # Limitations
//!
//! - **Order types**: Market, Limit, Stop, StopLimit, TrailingStop, TrailingStopLimit,
//!   and Bracket (entry + take-profit + stop-loss) supported. No Algo orders.
//! - **TimeInForce**: No `post_only` (IB has no maker-only orders)
//! - **Order lifecycle events missed during a gap are not recovered** (see below)
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
//! the socket was down, emitting each as a `Trade` exactly once. The same applies when
//! TWS loses its own link to IB's servers (notice 1100) and later restores it (1101/1102).
//! See that method for the details.
//!
//! # Caller Responsibilities
//!
//! 1. **Order reconciliation**: order lifecycle events sent during a gap are lost. An
//!    order cancelled, expired or rejected in that window is not reported. After a
//!    reconnect, call [`ExecutionClient::fetch_open_orders`] to reconcile open-order
//!    state. The IBKR account snapshot carries no open orders (#371). On `ibapi` 4.2.0
//!    that call is unreliable, and fails for a while after a reconnect: see
//!    [Known Issues](#known-issues-ibapi-420-shared-queues).
//! 2. **Permanent disconnect**: when `ibapi` gives up reconnecting, or fill recovery
//!    fails repeatedly, `account_stream` ends with `StreamTerminated` within about a
//!    second. Its reader thread then stays blocked until TWS sends another event, and
//!    another `account_stream` call on the client fails until the thread exits. After
//!    recovery fails, the client is still connected, so the reader exits on the next
//!    TWS event and `account_stream` works again. After a shutdown, no event follows,
//!    so replace the client. Shutdown is detected only while a stream is open, so
//!    `account_stream` on a client that has already shut down returns a stream that
//!    never ends; see [`ExecutionClient::account_stream`]. Replacing the client, by
//!    reconnecting with [`IbkrClient::connect_sync`] and choosing the client ID, is the
//!    caller's decision. A new `IbkrClient` does not know the orders the old one
//!    placed, so their later events are dropped.
//! 3. **Stale state cleanup**: Periodically call [`IbkrClient::clear_stale_executions`],
//!    [`IbkrClient::clear_stale_order_ids`], and [`IbkrClient::clear_stale_pending_cancels`]
//!
//! **Rationale**: IBKR uses TCP to local TWS/Gateway, not cloud WebSocket. `ibapi`
//! owns the transient reconnect. Replacing a client that is gone for good requires IB
//! Gateway availability and client ID coordination, decisions that belong in the
//! caller's wrapper, not the library.
//!
//! # Known Issues: `ibapi` 4.2.0 Shared Queues
//!
//! `ibapi` 4.2.0 answers the open-orders and positions requests from one queue per
//! request type. Every call on the client reads from the same queue, and nothing clears
//! it between calls. Besides each call's own reply, two things land in it:
//!
//! - A connection drop queues one `ConnectionReset` per response type: three on the
//!   open-orders queue, two on the positions queue.
//! - Every `OpenOrder` and `OrderStatus` message for an order with no live placement
//!   subscription is copied into `ibapi`'s three open-orders queues. That covers every
//!   update for an order this client placed, once its placement call has returned, and
//!   every row of an open-orders reply.
//!
//! As a result:
//!
//! - [`ExecutionClient::fetch_open_orders`] can report orders that have since filled or
//!   been cancelled as open. Each connection drop makes three calls fail and adds three
//!   calls of lag, which never clears. See that method.
//! - [`ExecutionClient::account_snapshot`] fails twice after each connection drop, then
//!   recovers.
//! - This client never reads two of the three open-orders queues, and reads the third
//!   only when `fetch_open_orders` is called. They keep a copy of every order update
//!   until read, so the two unread ones grow for the life of the client: `ibapi`'s own
//!   reconnect does not clear them. `ibapi` logs a warning each time one of them passes a
//!   multiple of 10,000 queued messages.
//!
//! [Account order events](ExecutionClient::account_stream) come through a separate
//! channel and are unaffected.
//!
//! Fixed upstream after 4.2.0 by
//! [rust-ibapi#836](https://github.com/wboayue/rust-ibapi/pull/836), which gives each
//! call its own queue and drops messages no call asked for.
//!
//! # See Also
//!
//! - [IB API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/trader-workstation-api/)
//! - `rustrade_data::exchange::ibkr` for market data

pub mod account;
pub mod contract;
pub mod execution;
pub mod order;
mod recovery;

use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, Snapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::{BracketOrderClient, ClientInstrument, ExecutionClient, dedup::new_dedup_cache},
    error::{
        ApiError, ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
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
    trade::{AssetFees, Trade, TradeId, TradesRead},
};
use account::{BalanceAggregator, PositionAggregator};
use chrono::{DateTime, Utc};
use execution::{ExecutionBuffer, parse_decimal_or_warn, try_decimal_or_warn};
use futures::stream::BoxStream;
use ibapi::{
    accounts::{AccountSummaryResult, types::AccountGroup},
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
    ibkr::ContractRegistry,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
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
    /// Pre-configured contracts to register on startup
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

/// Quiet period that ends the positions read in `account_snapshot`.
///
/// IB's positions request is a live subscription that keeps streaming after its
/// `PositionEnd` marker, so the read stops once this long passes without an
/// update. It does not stop at `PositionEnd`: on `ibapi` 4.2.0 the positions
/// queue is shared across calls (see the module's Known Issues), so after a
/// connection drop it may hold the replies to the calls that failed. Stopping at
/// the first `PositionEnd` would return one of those replies and leave the rest
/// for the next call, which would lag behind for good. Reading until quiet
/// drains them.
const POSITION_STREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
/// both forms above route exactly as described.
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

/// The instrument and client order id `execution` belongs to, or `None` when this
/// client does not track its order or its contract.
fn resolve_execution(
    execution: &ibapi::orders::ExecutionData,
    contracts: &ContractRegistry,
    order_ids: &OrderIdMap,
) -> Option<(InstrumentNameExchange, ClientOrderId)> {
    let order_id = execution.execution.order_id;
    let con_id = execution.contract.contract_id;

    // Fail-fast: skip second lookup if first fails
    let Some(client_id) = order_ids.get_client_id(order_id) else {
        debug!(
            ib_order_id = order_id,
            con_id, "ExecutionData for unknown order ID, dropping"
        );
        return None;
    };
    let Some(instrument) = contracts.get_name_by_con_id(con_id) else {
        debug!(
            ib_order_id = order_id,
            con_id, "ExecutionData for unknown contract ID, dropping"
        );
        return None;
    };
    Some((instrument, client_id))
}

/// Forward `updates`, from `ibapi`'s order-update subscription, to `sink` as account events,
/// until the subscription ends or the account stream does.
///
/// A closed stream is noticed on the next update, whether or not that update would be
/// forwarded, so the thread running this releases `ibapi`'s single order-update slot as soon as
/// TWS sends anything.
fn forward_order_updates(
    updates: impl IntoIterator<Item = Result<ibapi::orders::OrderUpdate, ibapi::Error>>,
    sink: &recovery::EventSink,
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
            OrderUpdate::OrderStatus(status) => {
                let ib_id = status.order_id;
                // Use single-lock method for terminal status to avoid read+write.
                // Only `Cancelled`/`Inactive` remove the mapping here: a `Filled`
                // order's mapping is intentionally retained so late-arriving
                // ExecutionData/CommissionReport events still resolve it (reaped
                // later by `OrderIdMap::clear_stale`), so this is deliberately
                // narrower than `OrderStatusKind::is_terminal()`.
                let is_terminal = matches!(
                    status.status,
                    OrderStatusKind::Cancelled | OrderStatusKind::Inactive
                );

                let lookup_result = if is_terminal {
                    order_ids.remove_and_get_context(ib_id)
                } else {
                    order_ids.get_client_id_and_context(ib_id)
                };

                if let Some((client_id, ctx)) = lookup_result {
                    let order = make_order_from_status(&status, client_id, &ctx, pending_cancels);
                    Some(UnindexedAccountEvent {
                        exchange: ExchangeId::Ibkr,
                        kind: AccountEventKind::OrderSnapshot(Snapshot::new(order)),
                    })
                } else {
                    debug!(ib_order_id = ib_id, "OrderStatus for unknown order ID");
                    None
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
                let Some((instrument, client_id)) = resolve_execution(&exec, contracts, order_ids)
                else {
                    continue;
                };

                exec_buffer.add_execution(exec, instrument, client_id);
                None
            }
            OrderUpdate::CommissionReport(report) => {
                if let Some(trade) = exec_buffer.complete_with_commission(&report)
                    && !sink.send_trade(trade)
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
    match e {
        ibapi::Error::ConnectionReset | ibapi::Error::Io(_) => {
            OrderError::Connectivity(ConnectivityError::Socket(message))
        }
        _ => OrderError::Rejected(ApiError::OrderRejected(message)),
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
    /// Connect to TWS/Gateway and initialize the client (sync, blocking).
    ///
    /// # Errors
    ///
    /// Returns error if connection fails or contract resolution fails.
    pub fn connect_sync(config: IbkrConfig) -> Result<Self, UnindexedClientError> {
        let url = format!("{}:{}", config.host, config.port);
        info!(url = %url, client_id = config.client_id, "Connecting to IB");

        let client = Client::connect(&url, config.client_id).map_err(|e| {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
        })?;

        let next_id = client.next_order_id();

        let contracts = ContractRegistry::new();

        for contract_config in &config.contracts {
            let contract = match contract_config.to_contract() {
                Ok(contract) => contract,
                Err(e) => {
                    warn!(name = %contract_config.name, error = %e, "Invalid contract config, skipping");
                    continue;
                }
            };
            let name = InstrumentNameExchange::from(contract_config.name.as_str());

            match client.contract_details(&contract) {
                Ok(details) => {
                    if let Some(detail) = details.into_iter().next() {
                        contracts.register(name.clone(), detail.contract.clone());
                        debug!(name = %name, con_id = detail.contract.contract_id, "Registered contract");
                    }
                }
                Err(e) => {
                    warn!(name = %name, error = %e, "Failed to resolve contract");
                }
            }
        }

        info!(
            contracts = contracts.len(),
            next_order_id = next_id,
            "Connected to IB"
        );

        Ok(Self {
            config: Arc::new(config),
            client: Arc::new(client),
            contracts,
            order_ids: OrderIdMap::new(),
            pending_cancels: PendingCancels::new(),
            execution_buffer: ExecutionBuffer::new(),
            next_order_id: Arc::new(Mutex::new(next_id)),
        })
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

    /// Register a contract for an instrument.
    pub fn register_contract(
        &self,
        name: InstrumentNameExchange,
        contract: ibapi::contracts::Contract,
    ) {
        self.contracts.register(name, contract);
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
    /// the filled status may not arrive at all. Removing mappings on terminal
    /// status would cause data loss.
    ///
    /// Call this periodically alongside `clear_stale_executions()`. A reasonable
    /// interval is 5-10 minutes with a max_age of 1 hour.
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
    /// Signals the ibapi client to shut down and releases the client ID for reuse.
    ///
    /// [`IbkrClient`] implements [`Clone`] and has no `Drop` impl: the underlying
    /// connection is released automatically when the last `Arc<Client>` reference
    /// is dropped. Calling `disconnect()` explicitly terminates the connection
    /// **immediately for all clones** sharing this client.
    ///
    /// Any active `account_stream()` ends with `StreamTerminated` within about a
    /// second.
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
        let ib_orders = build_ib_bracket_with_oca(
            parent_ib_id,
            action,
            quantity,
            entry_price,
            tp_price,
            sl_price,
            ib_tif,
        );

        // Generate client order IDs for children
        let parent_cid = request.parent_cid.clone();
        let (tp_cid, sl_cid) = derive_child_cids(&parent_cid);

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

        self.order_ids
            .register(parent_cid.clone(), parent_ib_id, parent_ctx);
        self.order_ids.register(tp_cid.clone(), tp_ib_id, tp_ctx);
        self.order_ids.register(sl_cid.clone(), sl_ib_id, sl_ctx);

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

        match result {
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
        }
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

    /// Rejects a `ContractConfig` that [`connect_sync`](Self::connect_sync) would skip as invalid,
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
    /// Panics if connection fails. The `ExecutionClient` trait doesn't allow
    /// `new()` to return `Result`. Use `IbkrClient::connect_sync()` directly
    /// for fallible construction.
    #[track_caller]
    fn new(config: Self::Config) -> Self {
        #[allow(clippy::expect_used)] // Trait signature doesn't allow Result
        Self::connect_sync(config).expect("failed to connect to IB")
    }

    /// Fetch account snapshot: balances, and a position per registered instrument IB reports.
    ///
    /// # Positions
    ///
    /// Each instrument IB reports a position in, and that is registered (and in `instruments`,
    /// when that is not empty), gets an `InstrumentAccountSnapshot`. Its `position` is
    /// [`PositionReport::Open`](crate::position::PositionReport::Open), carrying:
    /// - `quantity`: IB's signed position, negative when short, in shares for a stock and in
    ///   contracts for a future or an option. `ibapi` 4.2.0 hands it over as an `f64`, converted
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
    /// [`PositionReport::Flat`](crate::position::PositionReport::Flat). So is each instrument in
    /// `instruments` that is registered with its contract ID but that IB did not list, provided IB
    /// marked the end of its listing (`PositionEnd`) during the read. Without that marker such
    /// instruments are left out, with a warning, since the listing may be incomplete: IB can
    /// start the read with a stale listing, and only an end marker after the last report counts.
    /// IB's positions request carries no request ID, so one case cannot be caught: a stale end
    /// marker on its own, followed by a current listing that does not start within the read's
    /// 5-second quiet period. It reads as the listing of an account holding nothing, and the
    /// requested instruments are reported flat.
    /// A requested instrument registered without a contract ID, or whose ID was registered again
    /// under another name, cannot be matched to IB's reports and is left out too.
    ///
    /// IB can report the same position more than once during the read, as it changes. The latest
    /// report for each account is used.
    ///
    /// **Several accounts.** IB reports positions per account, and this client does not select
    /// one. When more than one account holds the same instrument, the first account to report a
    /// non-zero quantity is kept and the others are dropped with a warning; they are never summed.
    /// A consumer comparing the reported quantity with its own, as the `rustrade` engine's position
    /// drift check does, therefore compares against that one account.
    /// Which account comes first depends on the order IB reports them in, so a caller holding the
    /// same instrument in several accounts should not rely on it.
    ///
    /// # Limitations
    ///
    /// - The `orders` field in each `InstrumentAccountSnapshot` is always empty.
    ///   IB's positions endpoint returns position data only, not open orders.
    ///   Use `fetch_open_orders()` or `account_stream()` for order state.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if a position quantity does not convert to a
    /// `Decimal`, or if the positions subscription fails.
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
    /// IB's positions request is a live subscription that keeps streaming after
    /// the initial set, so the read ends once 5 seconds pass without a position
    /// update. **Every call therefore takes at least 5 seconds**, including for
    /// an account with no positions. If IB stalls mid-stream, the positions
    /// received so far are returned rather than blocking indefinitely.
    ///
    /// # Known Issue: Errors After a Connection Drop on `ibapi` 4.2.0
    ///
    /// After each connection drop, the next two calls fail with
    /// [`UnindexedClientError::Internal`]. The third succeeds. See the
    /// [module's Known Issues](self#known-issues-ibapi-420-shared-queues).
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
        let positions_future = tokio::task::spawn_blocking(move || {
            use ibapi::accounts::PositionUpdate;

            // ibapi::Error is unstructured — we cannot distinguish connection failures
            // (transient, should retry) from API errors (e.g., invalid request).
            // Mapped to Internal (non-transient) conservatively; a caller needing reconnect
            // logic should drive it from connection state, not from the error type.
            let positions_sub = client
                .positions()
                .map_err(|e| UnindexedClientError::Internal(format!("positions: {e}")))?;

            let mut positions = PositionAggregator::default();

            // Read until `POSITION_STREAM_TIMEOUT` passes with no update, not until
            // `PositionEnd`: see that constant for why.
            for pos_update in positions_sub.timeout_iter_data(POSITION_STREAM_TIMEOUT) {
                // Surface subscription errors rather than returning partial positions
                // (a truncated snapshot could be misread as positions having closed).
                let pos_update = match pos_update {
                    Ok(p) => p,
                    Err(e) => {
                        return Err(UnindexedClientError::Internal(format!(
                            "positions subscription: {e}"
                        )));
                    }
                };
                let pos = match pos_update {
                    PositionUpdate::Position(pos) => {
                        positions.report_seen();
                        pos
                    }
                    PositionUpdate::PositionEnd => {
                        positions.listing_ended();
                        continue;
                    }
                };
                let Some(instrument) = contracts.get_name_by_con_id(pos.contract.contract_id)
                else {
                    continue;
                };
                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
                    continue;
                }
                positions.process(instrument, pos);
            }
            // IB's reports are attributed by contract ID, so a requested instrument can be
            // reported flat only if its ID is registered and resolves back to it: with no ID, or
            // one another name took over, IB's position in it would be missed.
            let requested = instruments_filter.iter().flatten().filter(|instrument| {
                contracts.get_contract(instrument).is_some_and(|contract| {
                    contract.contract_id != 0
                        && contracts.get_name_by_con_id(contract.contract_id).as_ref()
                            == Some(*instrument)
                })
            });
            let reported = positions.into_reports(Utc::now(), requested)?;
            let snapshots = reported
                .into_iter()
                .map(|(instrument, position)| InstrumentAccountSnapshot {
                    instrument,
                    orders: Vec::new(),
                    // Nothing is read, so absence says nothing: see the limitation above and #371.
                    orders_complete: false,
                    position,
                    isolated: None,
                })
                .collect::<Vec<_>>();
            Ok::<_, UnindexedClientError>(snapshots)
        });

        // ibapi routes responses by request_id via thread-safe channels (RwLock<HashMap>),
        // so concurrent requests on the same client are safe.
        let (balances_result, positions_result) = tokio::join!(balances_future, positions_future);
        let balances = balances_result?;
        let instrument_snapshots = positions_result
            .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))??;

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
    /// subscription, and `ibkr-fill-recovery` watches for gaps in event delivery
    /// (see below). The watcher exits within about a second of the returned
    /// `BoxStream` being dropped or the stream ending. The reader exits when:
    /// - The returned `BoxStream` is dropped (channel closes), or the stream has
    ///   ended, and the next IB event arrives
    /// - The IB subscription ends
    ///
    /// **Important:** If IB is stalled (no events flowing), the reader thread blocks
    /// on the iterator. Dropping the stream signals termination, but the thread won't
    /// observe it until the next IB event arrives. Disconnecting does not release
    /// it either: `ibapi` does not end the order-update subscription when the
    /// client shuts down (wboayue/rust-ibapi#871), so after a shutdown the thread
    /// stays blocked until the process exits.
    ///
    /// # Reconnects and Fill Recovery
    ///
    /// `ibapi` reconnects its socket to TWS/Gateway by itself, and this stream
    /// stays open across that: a transient drop produces neither an error nor
    /// `StreamTerminated`. TWS does not resend what it sent while the socket was
    /// down, and TWS losing its own link to IB's servers (notice 1100) opens the
    /// same kind of gap. Once delivery is restored, this stream asks TWS for the
    /// day's executions and emits the ones from the gap as `Trade` events. The
    /// gap is taken to start up to a poll interval before the drop was noticed,
    /// minus a lookback margin. Trades the stream already delivered are not
    /// emitted twice.
    ///
    /// **Order lifecycle events are not recovered.** An order that was cancelled,
    /// expired or rejected during the gap is not reported here. Reconcile with
    /// [`ExecutionClient::fetch_open_orders`] after a reconnect, minding its
    /// known issue on `ibapi` 4.2.0. The log line
    /// `Recovered IBKR fills after a gap in event delivery` marks one.
    ///
    /// While TWS reports its link to IB's servers lost (1100), nothing marks the
    /// gap on this stream until the link is restored and the recovered fills
    /// arrive.
    ///
    /// The stream ends with `StreamTerminated` when:
    /// - recovery fails three times for a reason other than the transport
    ///   dropping again, rather than stay open with a gap;
    /// - the client shuts down for good, because `ibapi` gave up reconnecting or
    ///   [`IbkrClient::disconnect`] was called. `ibapi` never ends the order
    ///   update subscription itself, so this is detected from its notice stream,
    ///   which it does close.
    ///
    /// After either, the reader thread stays blocked on the subscription, and
    /// holds `ibapi`'s single order-update slot, until TWS sends another event.
    /// Calling `account_stream` again on the same client fails meanwhile. After a
    /// shutdown, TWS sends nothing more, so replace the client.
    ///
    /// Shutdown is detected only while this stream is open. Called on a client
    /// that has already shut down, this method still returns a stream, which
    /// never ends. After a stream ends with a shutdown, replace the client rather
    /// than resubscribe on it.
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

        let contracts_clone = self.contracts.clone();
        let order_ids_clone = self.order_ids.clone();
        let pending_cancels_clone = self.pending_cancels.clone();
        let exec_buffer_clone = self.execution_buffer.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(tx, new_dedup_cache());

        // Spawned before the reader: if the reader fails to spawn, `rx` drops with the error and
        // the watcher sees its consumer gone.
        let watcher = recovery::RecoveryWatcher {
            client: self.client.clone(),
            notices,
            contracts: self.contracts.clone(),
            order_ids: self.order_ids.clone(),
            pending: self.execution_buffer.clone(),
            sink: sink.clone(),
        };
        let watcher_sink = sink.clone();
        std::thread::Builder::new()
            .name("ibkr-fill-recovery".to_string())
            .spawn(move || {
                // Same panic policy as the reader below: a panic ends recovery, and a stream that
                // can no longer recover must say so rather than stay open.
                if let Err(panic_info) = catch_unwind(AssertUnwindSafe(|| watcher.run())) {
                    let msg = panic_message(panic_info.as_ref());
                    error!("Fill recovery worker panicked: {msg}");
                    watcher_sink.terminate(StreamTerminationReason::Error(format!(
                        "IBKR fill-recovery worker panicked: {msg}"
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
    ) -> Option<UnindexedOrderResponseCancel> {
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let ib_order_id = match self.order_ids.get_ib_id(&request.key.cid) {
            Some(id) => id,
            None => {
                return Some(OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        "order ID not found in map".to_string(),
                    ))),
                });
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
                Some(OrderResponseCancel {
                    key,
                    state: Ok(Cancelled::new(
                        OrderId::new(format_smolstr!("{}", ib_order_id)),
                        Utc::now(),
                        None,
                    )),
                })
            }
            Ok(Err(e)) => {
                error!(order_id = ib_order_id, error = %e, "Failed to cancel order");
                Some(OrderResponseCancel {
                    key,
                    state: Err(send_error(&e, e.to_string())),
                })
            }
            Err(e) => {
                error!(order_id = ib_order_id, error = %e, "Task join error");
                Some(OrderResponseCancel {
                    key,
                    state: Err(crate::error::OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                })
            }
        }
    }

    /// Submit an order to IB.
    ///
    /// # Cancellation Safety
    ///
    /// This future registers the order ID mapping before submitting to IB.
    /// If the future is cancelled (e.g., via `tokio::select!` timeout) after
    /// registration but before completion:
    /// - The order may still be submitted to IB
    /// - The order ID mapping will leak (not cleaned up)
    /// - Subsequent fills will be processed via `account_stream`
    ///
    /// Callers should avoid cancelling this future mid-flight. If timeout
    /// behavior is needed, prefer setting IB's native order timeout via
    /// `TimeInForce` instead.
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
        let key = OrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let contract = match self.contracts.get_contract(request.key.instrument) {
            Some(c) => c,
            None => {
                return Some(Order {
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
                });
            }
        };

        let quantity: f64 = match request.state.quantity.try_into() {
            Ok(q) => q,
            Err(_) => {
                return Some(Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        format!("quantity {} exceeds f64 range", request.state.quantity),
                    ))),
                });
            }
        };

        let ib_order = match build_ib_order(
            request.state.side,
            quantity,
            &request.state.kind,
            request.state.price,
            &request.state.time_in_force,
        ) {
            Ok(o) => o,
            Err(e) => {
                return Some(Order {
                    key,
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                });
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
        self.order_ids
            .register(request.key.cid.clone(), ib_order_id, context);

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

        match result {
            Ok(Ok(Some((order_id, filled)))) => {
                // IB always returns order status via subscription - never immediate fills.
                // The filled quantity here is from the OrderStatus event, not a complete fill.
                Some(Order {
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
                })
            }
            Ok(Ok(None)) => {
                // Subscription exhausted without terminal status. The order WAS submitted
                // (we have an IB order ID), but we lost tracking. Return Open with zero
                // filled — caller can query via fetch_open_orders or wait for account_stream.
                warn!(
                    ib_order_id,
                    "Order subscription ended without terminal status, returning Open"
                );
                Some(Order {
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
                })
            }
            Ok(Err(error)) => {
                // Cleanup order_ids: the order was never sent (place_order error)
                // or TWS rejected it (Cancelled/Inactive or a genuine notice).
                self.order_ids.remove_by_ib_id(ib_order_id);
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::inactive(error),
                })
            }
            Err(e) => {
                self.order_ids.remove_by_ib_id(ib_order_id);
                Some(Order {
                    key,
                    side,
                    price,
                    quantity: req_quantity,
                    kind,
                    time_in_force: tif,
                    state: OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                        e.to_string(),
                    ))),
                })
            }
        }
    }

    /// Fetch account balances.
    ///
    /// # Limitations
    ///
    /// The `time_exchange` field in returned balances uses `Utc::now()`, not
    /// the actual IB server timestamp. IB's account summary endpoint does not
    /// provide timestamps per balance update.
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

            let mut aggregator = BalanceAggregator::new();
            for summary in sub.iter_data() {
                // Surface errors rather than returning a partial balance set.
                let summary = summary
                    .map_err(|e| UnindexedClientError::Internal(format!("account_summary: {e}")))?;
                match summary {
                    AccountSummaryResult::Summary(s) => aggregator.process(&s),
                    AccountSummaryResult::End => break,
                }
            }

            let mut balances = aggregator.to_balances();

            if let Some(ref filter) = assets_filter {
                balances.retain(|b| filter.contains(&b.asset));
            }

            Ok(balances)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }

    /// Fetch open orders.
    ///
    /// # Limitations
    ///
    /// - The `filled_qty` in returned orders is set to zero. IB's open orders endpoint
    ///   returns order definitions, not fill status. For accurate filled quantities,
    ///   use `account_stream` which provides `OrderStatus` events with fill progress.
    /// - The `time_in_force` defaults to `GoodUntilCancelled`. IB's open orders endpoint
    ///   does not return the original TIF setting.
    /// - This method blocks on IB's subscription until IB sends an end-of-data marker.
    ///   If IB is stalled, this will block indefinitely.
    ///
    /// # Known Issue: Stale Results on `ibapi` 4.2.0
    ///
    /// The result may not be this call's answer. See the
    /// [module's Known Issues](self#known-issues-ibapi-420-shared-queues).
    ///
    /// - Order updates received since the previous call are read as part of the
    ///   result. It can hold the same order more than once, and orders that have
    ///   since filled or been cancelled, reported as open.
    /// - After the first connection drop, the next three calls fail with
    ///   [`UnindexedClientError::Internal`], and every call after them returns
    ///   the reply to the call three before it. Each later drop adds three more
    ///   calls of lag, and three more failures, which come once the replies
    ///   already queued have been read. Retrying does not catch up.
    ///
    /// Do not treat the result as authoritative open-order state on 4.2.0. The
    /// order events from [`ExecutionClient::account_stream`] are unaffected.
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let client = self.client.clone();
        let contracts = self.contracts.clone();
        let order_ids = self.order_ids.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            use ibapi::orders::Orders;

            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .all_open_orders()
                .map_err(|e| UnindexedClientError::Internal(format!("open_orders: {e}")))?;

            let mut orders = Vec::new();
            for order_item in sub.iter_data() {
                // Surface errors rather than returning a partial open-order set,
                // which the caller could misread during order reconciliation.
                let order_item = order_item
                    .map_err(|e| UnindexedClientError::Internal(format!("open_orders: {e}")))?;
                let order_data = match order_item {
                    Orders::OrderData(data) => data,
                    _ => continue,
                };

                let instrument = match contracts.get_name_by_con_id(order_data.contract.contract_id)
                {
                    Some(i) => i,
                    None => continue,
                };

                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
                    continue;
                }

                let client_id = order_ids
                    .get_client_id(order_data.order_id)
                    .unwrap_or_else(|| {
                        ClientOrderId::new(format_smolstr!("{}", order_data.order_id))
                    });

                let side = match order_data.order.action {
                    ibapi::orders::Action::Buy => Side::Buy,
                    ibapi::orders::Action::Sell
                    | ibapi::orders::Action::SellShort
                    | ibapi::orders::Action::SellLong => Side::Sell,
                };

                let price = order_data
                    .order
                    .limit_price
                    .map(|p| parse_decimal_or_warn(p, "limit_price"));
                let kind = if order_data.order.order_type == "LMT" {
                    OrderKind::Limit
                } else {
                    OrderKind::Market
                };

                orders.push(Order {
                    key: OrderKey {
                        exchange: ExchangeId::Ibkr,
                        instrument,
                        strategy: StrategyId::unknown(),
                        cid: client_id,
                    },
                    side,
                    price,
                    quantity: parse_decimal_or_warn(
                        order_data.order.total_quantity,
                        "total_quantity",
                    ),
                    kind,
                    // IB's open orders endpoint doesn't return TIF; default to GTC
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                    state: Open::new(
                        VenueOrderId::Assigned(OrderId::new(format_smolstr!(
                            "{}",
                            order_data.order_id
                        ))),
                        Utc::now(),
                        Decimal::ZERO, // M-5: filled_qty unavailable from open orders endpoint
                    ),
                });
            }

            Ok(orders)
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
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
    /// - **Fees are always zero.** IB's executions endpoint doesn't include commission
    ///   data. For trades with accurate fees, use `account_stream()` which pairs
    ///   `ExecutionData` with `CommissionReport` events.
    /// - This method blocks on IB's executions subscription until IB sends an
    ///   end-of-data marker. If IB is stalled, this will block indefinitely.
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
        let order_ids = self.order_ids.clone();
        let instruments_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            Some(instruments.iter().cloned().collect())
        };

        tokio::task::spawn_blocking(move || {
            use ibapi::orders::{ExecutionSide, Executions};

            let exec_filter = ibapi::orders::ExecutionFilter::default();
            // Note: ibapi errors are unstructured — see comment in account_snapshot() re: Internal
            let sub = client
                .executions(exec_filter)
                .map_err(|e| UnindexedClientError::Internal(format!("executions: {e}")))?;

            let mut trades = Vec::new();
            for exec_item in sub.iter_data() {
                // Surface errors rather than returning a partial fill set, which the
                // caller could misread during fill recovery.
                let exec_item = exec_item
                    .map_err(|e| UnindexedClientError::Internal(format!("executions: {e}")))?;
                let exec_data = match exec_item {
                    Executions::ExecutionData(data) => data,
                    _ => continue,
                };

                let instrument = match contracts.get_name_by_con_id(exec_data.contract.contract_id)
                {
                    Some(i) => i,
                    None => continue,
                };

                if instruments_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
                    continue;
                }

                let exec = &exec_data.execution;
                let exec_time = match execution::parse_ib_timestamp(&exec.time) {
                    Some(t) => t,
                    None => {
                        warn!(
                            exec_id = %exec.execution_id,
                            time = %exec.time,
                            "Unparseable timestamp in execution, skipping"
                        );
                        continue;
                    }
                };

                if exec_time < start || exec_time > end {
                    continue;
                }

                // `ExecutionSide` is a closed two-variant enum in ibapi 3.x
                // (the decoder rejects unknown wire values), so this is total.
                let side = match exec.side {
                    ExecutionSide::Bought => Side::Buy,
                    ExecutionSide::Sold => Side::Sell,
                };

                let client_id = order_ids
                    .get_client_id(exec.order_id)
                    .unwrap_or_else(|| ClientOrderId::new(format_smolstr!("{}", exec.order_id)));

                trades.push(Trade {
                    id: TradeId::new(&exec.execution_id),
                    order_id: OrderId::new(&client_id.0),
                    instrument,
                    strategy: StrategyId::unknown(),
                    time_exchange: exec_time,
                    side,
                    price: parse_decimal_or_warn(exec.price, "exec.price"),
                    quantity: parse_decimal_or_warn(exec.shares, "exec.shares"),
                    order_filled_quantity: Some(parse_decimal_or_warn(
                        exec.cumulative_quantity,
                        "exec.cumulative_quantity",
                    )),
                    // IBKR executions API lacks commission data (available via CommissionReport callback).
                    // "UNKNOWN" placeholder will fail indexing - use unindexed or correlate with WS.
                    fees: AssetFees::new(AssetNameExchange::from("UNKNOWN"), Decimal::ZERO, None),
                });
            }

            Ok(TradesRead::complete(trades))
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
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
/// 3. Else → `Cancelled` (broker-initiated or external cancellation)
///
/// # Known Limitation
///
/// If the broker cancels a DAY order before market close (e.g., insufficient margin),
/// it will be misclassified as `Expired`. This is rare and acceptable given the
/// alternative (forking ibapi to preserve order_id in error callbacks).
fn make_order_from_status(
    status: &ibapi::orders::OrderStatus,
    client_id: ClientOrderId,
    ctx: &OrderContext,
    pending_cancels: &PendingCancels,
) -> Order<ExchangeId, InstrumentNameExchange, OrderState<AssetNameExchange, InstrumentNameExchange>>
{
    use ibapi::orders::OrderStatusKind;

    let ib_id = status.order_id;
    let order_id = OrderId::new(format_smolstr!("{}", ib_id));

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
            // Differentiate user-cancel from time-expiration
            let was_user_cancel = pending_cancels.remove(ib_id);

            if was_user_cancel {
                // User called cancel_order() — definitely a cancellation
                OrderState::inactive(Cancelled::new(order_id, Utc::now(), reported_fill))
            } else if matches!(ctx.time_in_force, TimeInForce::GoodUntilEndOfDay) {
                // DAY order without pending cancel — expired at market close
                OrderState::inactive(Expired::new(order_id, Utc::now(), reported_fill))
            } else {
                // GTC/IOC/FOK without pending cancel — broker or exchange cancelled
                OrderState::inactive(Cancelled::new(order_id, Utc::now(), reported_fill))
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

    /// `connect_sync` only logs and skips an invalid entry; built through `ExecutionBuilder`, it
    /// fails the build instead. Every problem is reported, not just the first.
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
        order_ids.register(request.parent_cid.clone(), 10, ctx());
        order_ids.register(tp_cid, 11, ctx());
        order_ids.register(sl_cid, 12, ctx());

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
        order_ids.register(request.parent_cid.clone(), 10, ctx());
        order_ids.register(tp_cid, 11, ctx());
        order_ids.register(sl_cid, 12, ctx());

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
        let sink = recovery::EventSink::new(tx, new_dedup_cache());
        sink.terminate(StreamTerminationReason::Error("recovery failed".into()));

        assert_eq!(run(&sink, unforwarded(3)), 1);
    }

    #[test]
    fn stops_on_the_first_update_after_the_consumer_goes() {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(tx, new_dedup_cache());
        drop(rx);

        assert_eq!(run(&sink, unforwarded(3)), 1);
    }

    #[test]
    fn open_stream_reads_past_unforwarded_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = recovery::EventSink::new(tx, new_dedup_cache());

        assert_eq!(run(&sink, unforwarded(3)), 3);
        // The subscription ended, so the stream says so.
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            AccountEventKind::StreamTerminated(StreamTerminationReason::Error(ref reason))
                if reason == "IBKR order-update stream ended"
        ));
    }
}
