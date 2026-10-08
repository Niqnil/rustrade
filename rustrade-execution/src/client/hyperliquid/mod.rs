//! Hyperliquid ExecutionClient implementations for perpetual futures and spot trading.
//!
//! Uses the official `hyperliquid_rust_sdk` crate for REST and WebSocket API access.
//! Gated behind the "hyperliquid" feature flag.
//!
//! # Modules
//!
//! - [`HyperliquidClient`]: Perpetual futures client
//! - [`spot::HyperliquidSpotClient`]: Spot trading client
//!
//! # Authentication
//!
//! Hyperliquid uses EVM-based authentication (Ethereum private key + EIP-712 signatures)
//! instead of traditional API key/secret. The SDK handles all signing internally.
//!
//! # Architecture
//!
//! - REST (`InfoClient`): account_snapshot, fetch_balances, fetch_open_orders, fetch_trades.
//!   `fetch_trades` posts `userFillsByTime` to `/info` through the SDK's HTTP client but parses
//!   the response into [`common::UserFill`] rather than the SDK's type, which drops `tid` and
//!   `feeToken`.
//! - REST (`ExchangeClient`): open_order, cancel_order
//! - WebSocket (`InfoClient` with `with_reconnect`): account_stream via UserFills + OrderUpdates subscriptions
//! - REST (`InfoClient`), after each reconnect and through [`OrderStatusClient`]: how the orders
//!   held as live ended, by `openOrders` and `orderStatus`
//!
//! # SDK Delegation Model
//!
//! This client delegates connection management to the official `hyperliquid_rust_sdk`:
//!
//! | Responsibility | Implementation |
//! |----------------|----------------|
//! | **Reconnection** | `InfoClient::with_reconnect()` handles WebSocket reconnection automatically |
//! | **Heartbeat** | SDK manages WebSocket ping/pong internally |
//! | **Deduplication** | This client's own, on the fills stream. See below. |
//! | **Fill recovery** | Only the recent fills the venue's `userFills` snapshot redelivers on each resubscription — use [`ExecutionClient::fetch_trades`] for older ones |
//! | **Ended-order recovery** | This client's own, after each reconnect: see [`HyperliquidClient`]'s `account_stream` and [`OrderStatusClient`] |
//!
//! **Caller responsibilities**:
//! - If fill recovery is critical, monitor for reconnection events and call `fetch_trades()`
//! - REST clients (`InfoClient::new()`, `ExchangeClient::new()`) do NOT auto-reconnect;
//!   only WebSocket streams via `with_reconnect()` do
//!
//! # Trade identity and duplicate fills
//!
//! A [`Trade`]'s id is the venue's `tid`, which identifies the fill. It is deliberately **not**
//! `hash`, which identifies the transaction: one aggressive order sweeping several resting orders
//! produces several fills under a single hash, and keying on the hash makes them indistinguishable.
//! Hyperliquid documents `tid` as unique per fill but qualified by coin rather than globally, so
//! callers reconciling across instruments should qualify it the same way.
//!
//! The fills subscription opens with a snapshot of recent fills and the SDK resubscribes on every
//! reconnect, so the same fill is redelivered each time the socket comes back. A trade is a delta
//! the consumer accumulates, so this client deduplicates the stream itself — the cache is scoped to
//! one [`ExecutionClient::account_stream`] call and survives reconnects within it.
//!
//! Order updates are deliberately **not** deduplicated. They assert absolute state rather than a
//! delta, so a replayed one is idempotent while a dropped one could strand a consumer on stale
//! state.
//!
//! [`ExecutionClient::fetch_trades`] does not deduplicate: it answers the window it was asked for,
//! and its results reach the caller rather than the stream. A caller feeding both into one consumer
//! should expect overlap and reconcile on the trade id.
//!
//! # Perpetual and spot coins
//!
//! Hyperliquid reports every market a wallet trades through the same account streams and
//! open-order and fill endpoints, each order and fill under its *coin*. Each client keeps only its
//! own, judged by [`CoinKind::of`](rustrade_instrument::hyperliquid::CoinKind::of):
//! - [`HyperliquidClient`] keeps perpetuals (`BTC`, `kPEPE`, and HIP-3 perpetuals such as
//!   `xyz:TSLA` on the DEXs it is configured with), named `{coin}-{collateral}-PERP`; see [HIP-3
//!   perpetuals](#hip-3-perpetuals);
//! - [`spot::HyperliquidSpotClient`] keeps spot pairs (`@107`, `PURR/USDC`), named from their
//!   tokens; see its [Spot coins](spot#spot-coins).
//!
//! A coin of a kind neither recognises, such as an outcome coin (`#12`), is left out by both, and
//! logged with `warn!` by the perpetuals client.
//!
//! # HIP-3 perpetuals
//!
//! Besides its default perpetuals DEX, Hyperliquid hosts builder-deployed (HIP-3) DEXs, each
//! with its own perpetuals, named `{dex}:{asset}` (`xyz:TSLA`), its own margin, and its own
//! collateral token, which need not be USDC. [`HyperliquidClient`] trades the default DEX and the
//! HIP-3 DEXs named in [`HyperliquidConfig::dexes`], which it reads when it connects.
//!
//! - **Names.** Every perpetual is named after the token its DEX settles in:
//!   `{coin}-{collateral}-PERP`, such as `BTC-USDC-PERP`, `xyz:TSLA-USDC-PERP` and
//!   `flx:TSLA-USDH-PERP`. An order, cancel or filtered read on any other name, a perpetual on an
//!   unconfigured DEX included, is refused with
//!   [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid).
//! - **Unconfigured DEXs.** Their fills and orders still arrive on the account stream and in the
//!   venue's listings, but without the DEX's metadata they cannot be named, so they are left out,
//!   logged at `warn` once per DEX.
//! - **New listings.** The SDK addresses an order by the asset ids read at connect, so a
//!   perpetual listed after that, on any DEX, cannot be ordered until the client connects again.
//! - **Balances.** Where collateral is held depends on the account's abstraction mode; see
//!   [`account_snapshot`](HyperliquidClient#method.account_snapshot).
//! - **Cost.** Connecting reads `perpDexs`, `spotMeta` and each configured DEX's `meta` (weight
//!   20 each) when any DEX is configured. Each read of open orders or positions makes one
//!   request per DEX, the default one included: Hyperliquid lists a HIP-3 DEX's only when asked
//!   for it. Fills need no such request.
//!
//! # Client order ids
//!
//! **Every order must be placed under a client id in
//! [`ClientOrderId::uuid()`](crate::order::id::ClientOrderId::uuid) form**: a UUID, lowercase and
//! hyphenated. Any other id is refused at placement with [`OrderError::Rejected`], before anything
//! is sent.
//!
//! Hyperliquid tracks an order by its venue `oid` and by an optional 16-byte `cloid`, and reports
//! the `cloid` back on every order record, REST and WebSocket alike. This client sends the client
//! id as the `cloid` and turns the one reported back into the same id, so every account snapshot,
//! open-order listing and order update names an order by the id it was placed with. Only that
//! form of UUID comes back unchanged, which is why nothing else is accepted.
//!
//! An order placed some other way, such as the web app, has no `cloid` and is reported under its
//! `oid`. It cannot be one of this client's orders, which is what lets an account snapshot declare
//! its order list complete (see [`InstrumentAccountSnapshot::orders_complete`]).
//!
//! # Conditional Orders (Stop, TakeProfit)
//!
//! Hyperliquid supports conditional (trigger) orders via the `ClientOrder::Trigger` variant.
//!
//! **Supported order kinds**:
//! - `Stop` → market order triggered at stop price (`tpsl: "sl"`, `is_market: true`)
//! - `StopLimit` → limit order triggered at stop price (`tpsl: "sl"`, `is_market: false`)
//! - `TakeProfit` → market order triggered at take-profit price (`tpsl: "tp"`, `is_market: true`)
//! - `TakeProfitLimit` → limit order triggered at take-profit price (`tpsl: "tp"`, `is_market: false`)
//!
//! **Unsupported**: `TrailingStop`, `TrailingStopLimit` (Hyperliquid does not support trailing stops)
//!
//! **SDK limitations** (hyperliquid_rust_sdk 0.6.x): neither the open orders this client reads
//! nor `OrderUpdate` carry an order's trigger fields or time in force, so orders from both are
//! reported as `OrderKind::Limit`, `GoodUntilCancelled`. Track `OrderKind` from the placement
//! request.
//!
//! # Limitations
//!
//! - **Order precision**: Hyperliquid limits a size to the asset's `szDecimals` decimal places,
//!   and a price to 5 significant figures and `6 - szDecimals` decimal places (integers always
//!   allowed). An order whose quantity, price or trigger price breaks these is refused with
//!   [`OrderError::InvalidPrecision`] before anything is sent; the client never rounds a value,
//!   since that would place an order other than the one requested. Read the rules with
//!   [`HyperliquidClient::order_precision`] and round first. See [`precision`].

pub mod common;
pub mod config;
pub mod error;
mod order_recovery;
mod perp_account;
mod perp_dexes;
pub mod precision;
pub mod spot;
mod spot_coins;

use crate::client::dedup::new_dedup_cache;
use crate::client::order_recovery::{
    KnownLiveOrders, OrderLookup, SharedKnownLiveOrders, fetch_ended_by_key,
};
use crate::{
    AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::{ExecutionClient, OrderStatusClient},
    emit_stream_terminated,
    error::{
        ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    order::{
        Order, OrderKey, OrderKind, TimeInForce, UnindexedInactiveOrder, UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Filled, Open, OrderState, UnindexedOrderState},
    },
    position::{Position, PositionReport},
    trade::{AssetFees, Trade, TradeId, TradesRead},
};
use chrono::{DateTime, Utc};
use common::{
    CLOID_REQUIRED, CancelOnDropStream, OpenOrderListing, UnknownCoins, cancel_outcome,
    cid_to_cloid, map_tif, millis_to_datetime, open_order_to_order, parse_decimal, parse_side,
    span_millis, user_fills_by_time, warn_unknown_coins,
};
pub use config::{HyperliquidConfig, HyperliquidConfigError, Network};
pub use error::HyperliquidConnectError;
use error::map_order_error;
use ethers::signers::Signer;
use fnv::FnvHashSet;
use futures::{StreamExt, stream::BoxStream};
use hyperliquid_rust_sdk::{ExchangeClient, InfoClient, Message, Subscription};
use order_recovery::{
    ReconnectWatch, fetch_order_record, listed_cids, lookup_from_record, remember_open,
    remember_snapshot, send_fills, send_observed, spawn_order_checks,
};
use perp_account::{account_mode, dex_states};
use perp_dexes::{PerpDexes, UnconfiguredDexes, warn_unconfigured_dexes};
pub use precision::OrderPrecision;
use precision::{WireOrderError, wire_order};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use smol_str::format_smolstr;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Hyperliquid perpetual futures execution client.
///
/// Wraps the official `hyperliquid_rust_sdk` to implement the `ExecutionClient` trait.
/// Supports perpetual futures trading on Hyperliquid DEX.
#[derive(Debug, Clone)]
pub struct HyperliquidClient {
    config: HyperliquidConfig,
    info_client: Arc<InfoClient>,
    exchange_client: Arc<ExchangeClient>,
    /// The DEXs traded, and the names of their perpetuals.
    dexes: Arc<PerpDexes>,
    /// The orders seen live and not yet seen end, which a reconnect asks about. Shared by every
    /// clone and every account stream, since an order placed through one ends on any of them.
    known_live: SharedKnownLiveOrders,
}

impl HyperliquidClient {
    /// Create a new client asynchronously.
    ///
    /// Use this when calling from an async context (e.g., tokio tests).
    /// For sync contexts, use `ExecutionClient::new()`.
    ///
    /// Reads the markets of each DEX in [`HyperliquidConfig::dexes`]; see [HIP-3
    /// perpetuals](self#hip-3-perpetuals).
    ///
    /// # Errors
    ///
    /// - [`HyperliquidConnectError::Connectivity`] when the SDK clients cannot be created, or a
    ///   request for the DEXs' markets fails in transit.
    /// - [`HyperliquidConnectError::UnknownDex`] when Hyperliquid lists no DEX by a configured
    ///   name on the configured network.
    /// - [`HyperliquidConnectError::Metadata`] when a DEX's markets cannot be read.
    ///
    /// # Cost
    ///
    /// With any DEX configured: `perpDexs`, `spotMeta` and each DEX's `meta`, weight 20 each, so
    /// `40 + 20 × dexes`, besides what the SDK reads to build its own clients.
    pub async fn connect(config: HyperliquidConfig) -> Result<Self, HyperliquidConnectError> {
        let base_url = config.base_url();

        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(format!("InfoClient: {e}")))?;

        let mut dexes = PerpDexes::fetch(&info_client, &config.dexes).await?;

        let wallet = config.wallet.clone();
        let mut exchange_client = ExchangeClient::new(None, wallet, Some(base_url), None, None)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("ExchangeClient: {e}")))?;
        // The SDK addresses an order by the coin's asset id, and reads only the default DEX's.
        exchange_client.coin_to_asset.extend(dexes.asset_ids());
        dexes.add_default_perps(&exchange_client.meta.universe);

        info!(
            network = ?config.network,
            dexes = ?config.dexes,
            wallet = %config.wallet_address_hex(),
            "Created HyperliquidClient"
        );

        Ok(Self {
            config,
            info_client: Arc::new(info_client),
            exchange_client: Arc::new(exchange_client),
            dexes: Arc::new(dexes),
            known_live: KnownLiveOrders::shared(ExchangeId::HyperliquidPerp),
        })
    }

    /// Returns the wallet address as a hex string (for logging/debugging).
    pub fn wallet_address(&self) -> String {
        self.config.wallet_address_hex()
    }

    /// The precision rules for orders on the perpetual `instrument`, from its `szDecimals`.
    ///
    /// [`open_order`](ExecutionClient::open_order) refuses an order that breaks them with
    /// [`OrderError::InvalidPrecision`] and sends nothing. Round with these rules first. See
    /// [`OrderPrecision`].
    ///
    /// `None` when `instrument` names no perpetual this client trades, or one listed after the
    /// client connected: the client reads each DEX's perpetuals once, at
    /// [`connect`](Self::connect), so a perpetual listed since needs a new client.
    pub fn order_precision(&self, instrument: &InstrumentNameExchange) -> Option<OrderPrecision> {
        let coin = self.dexes.coin(instrument).ok()?;
        self.dexes.sz_decimals(coin).map(OrderPrecision::perp)
    }

    /// Returns the wallet address as ethers H160.
    fn wallet_h160(&self) -> ethers::types::H160 {
        self.config.wallet.address()
    }

    /// Fail with [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) on
    /// the first of `instruments` that names no perpetual this client trades. A read filtered to
    /// it would otherwise report it empty without having looked.
    fn check_instruments(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<(), UnindexedClientError> {
        for instrument in instruments {
            self.dexes.coin(instrument).map_err(|reason| {
                UnindexedClientError::Api(crate::error::ApiError::InstrumentInvalid(
                    instrument.clone(),
                    reason,
                ))
            })?;
        }
        Ok(())
    }
}

impl ExecutionClient for HyperliquidClient {
    const EXCHANGE: ExchangeId = ExchangeId::HyperliquidPerp;

    // Perpetuals only — spot is served by `HyperliquidSpotClient`, which uses different coin
    // naming, asset indices and balance endpoints.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] =
        &[InstrumentKindDiscriminant::Perpetual];

    type Config = HyperliquidConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Creates a new Hyperliquid client synchronously.
    ///
    /// # Panics
    ///
    /// - If no Tokio runtime is available on the current thread
    /// - If called from within an async context (e.g., inside `async fn`, `spawn`, or `block_on`)
    /// - Whenever [`HyperliquidClient::connect`] would fail: SDK initialization fails (network
    ///   error, invalid credentials), or a configured DEX is unknown or unreadable
    ///
    /// # Recommended Usage
    ///
    /// Use [`HyperliquidClient::connect`] instead — it's async-safe and returns `Result`.
    /// This method exists only for trait compliance; prefer `connect()` in all new code.
    fn new(config: Self::Config) -> Self {
        // SDK initialization is async; block on it since ExecutionClient::new is sync.
        // WARNING: This will panic if called from within an async context.
        tokio::runtime::Handle::current()
            .block_on(Self::connect(config))
            .unwrap_or_else(|e| panic!("Failed to create HyperliquidClient: {e}"))
    }

    /// The collateral balances, and each perpetual's open orders and position, across the default
    /// DEX and every configured HIP-3 DEX.
    ///
    /// # Balances
    ///
    /// Read where the account's abstraction mode holds them (see [HIP-3
    /// perpetuals](self#hip-3-perpetuals)):
    /// - **Unified account or portfolio margin**: one balance per collateral token of the DEXs
    ///   traded (`USDC`, `USDH`), from the spot clearinghouse, free of what is on hold. A token not
    ///   held is reported at zero. Other tokens, which portfolio margin may also count as
    ///   collateral, are left to [`spot::HyperliquidSpotClient`].
    /// - **Standard** (the default for an account that never chose a mode): one balance per DEX,
    ///   from its margin summary, as each margins separately: the default DEX's as `USDC`, each
    ///   HIP-3 DEX's as `{dex}:{collateral}` (`xyz:USDC`, `flx:USDH`). The free balance is what
    ///   margin does not use, never below zero.
    ///
    /// A mode this version does not know fails the snapshot rather than report balances read from
    /// the wrong place.
    ///
    /// Positions on a HIP-3 DEX the client is not configured with are not read, and so not
    /// reported: name the DEX in [`HyperliquidConfig::dexes`] to see them.
    ///
    /// # Positions
    ///
    /// Every requested perpetual is listed. Its position is [`PositionReport::Open`] when
    /// Hyperliquid reports a non-zero size for it, and [`PositionReport::Flat`] otherwise, since
    /// each DEX's state holds every open position on it. A size that does not parse is
    /// [`PositionReport::Unreported`], with a warning.
    ///
    /// # Errors
    ///
    /// [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for a requested
    /// instrument that names no perpetual this client trades, such as one on a HIP-3 DEX it is not
    /// configured with: it was not read, so nothing can be said about it.
    ///
    /// # Cost
    ///
    /// At once, `userAbstraction` (weight 20) and, for each DEX traded, the default one included,
    /// a `clearinghouseState` (weight 2) and an `openOrders` (weight 20); then, under a unified
    /// account or portfolio margin, `spotClearinghouseState` (weight 2). With `n` HIP-3 DEXs
    /// that is `42 + 22n`, or `44 + 22n`, against Hyperliquid's limit of 1,200 a minute per IP
    /// address.
    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        self.check_instruments(instruments)?;
        let address = self.wallet_h160();
        let info_client = &*self.info_client;
        let dexes = &*self.dexes;

        let (mode, states, open_orders) = tokio::try_join!(
            account_mode(info_client, address),
            dex_states(info_client, address, dexes),
            perp_account::open_orders(info_client, address, dexes),
        )?;
        let now = Utc::now();
        let balances =
            perp_account::snapshot_balances(info_client, address, dexes, mode, &states, now)
                .await?;

        // Build instrument filter if provided
        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        warn_unknown_coins(open_orders.iter().map(|order| order.coin.as_str()));
        let mut listing = OpenOrderListing::new(
            &open_orders,
            ExchangeId::HyperliquidPerp,
            instruments,
            |coin| dexes.instrument(coin),
        );

        // Build positions from each DEX's asset_positions
        let mut instrument_snapshots = Vec::new();
        for asset_pos in states.iter().flat_map(|(_, state)| &state.asset_positions) {
            let pos = &asset_pos.position;
            let Some(instrument) = dexes.instrument(&pos.coin) else {
                continue;
            };

            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let position = perp_position_report(pos, now);

            let (orders, orders_complete) = match listing.remove(&instrument) {
                Some(listed) => (listed.orders, listed.orders_complete),
                // The listing has an entry for every requested instrument, so this is an
                // unfiltered read with no open order on the instrument.
                None => (Vec::new(), true),
            };

            instrument_snapshots.push(InstrumentAccountSnapshot {
                instrument,
                orders,
                orders_complete,
                position,
                isolated: None,
            });
        }

        // Every other instrument listed: open orders but no position, or requested with neither.
        // Each DEX's `asset_positions` holds every open position on it, so these are flat.
        instrument_snapshots.extend(listing.into_snapshots().map(|mut snapshot| {
            snapshot.position = PositionReport::Flat;
            snapshot
        }));

        let snapshot = AccountSnapshot {
            exchange: ExchangeId::HyperliquidPerp,
            balances,
            instruments: instrument_snapshots,
        };
        remember_snapshot(&self.known_live, &snapshot);
        Ok(snapshot)
    }

    /// Returns a live stream of account events (fills, order updates).
    ///
    /// # Instrument filtering
    ///
    /// The `instruments` parameter is **ignored** — Hyperliquid's WebSocket API does not
    /// support per-instrument subscriptions for user events. All fills and order updates
    /// across all instruments are delivered. Consumers requiring instrument filtering
    /// must filter client-side.
    ///
    /// # Task lifecycle
    ///
    /// Spawns three background tasks (fills, orders, and the reconnect's order check below) that
    /// are automatically cancelled when the returned stream is dropped. The `ws_client` is held by
    /// the orders task; when cancelled, the tasks exit and the WebSocket connection closes.
    ///
    /// # Orders that ended while disconnected
    ///
    /// The SDK reconnects the socket underneath this stream and resubscribes, but `orderUpdates`
    /// opens with no snapshot, so an order that ended while the socket was down is never reported
    /// on it. After each reconnect the stream therefore reports how each order the client holds as
    /// live ended, where it did, as an [`AccountEventKind::OrderSnapshot`] of its inactive state,
    /// read as [`OrderStatusClient::fetch_ended_orders`] reads it: filled (without an average
    /// price), cancelled (with what filled before) or rejected. Each carries
    /// [`StrategyId::unknown`], since Hyperliquid records no strategy; the engine matches it to the
    /// order it tracks by client order id.
    ///
    /// - **When.** The SDK tells each subscription that the socket dropped, and the venue opens
    ///   the resubscribed `userFills` with a snapshot of recent fills. The check starts once that
    ///   snapshot's fills have been sent on, so the fills of an order that it carries arrive before
    ///   how the order ended. The snapshot is all the fill recovery there is (see the [module
    ///   docs](self#sdk-delegation-model)), and Hyperliquid does not document how far back it
    ///   reaches, so after a long outage an order can be reported filled before, or without, fills
    ///   older than that. The SDK resubscribes in no fixed order and only logs a resubscription
    ///   that fails: an order that ends after the check lists the open orders but before
    ///   `orderUpdates` is subscribed again is missed, and if `userFills` is not subscribed again
    ///   no check runs until a later reconnect.
    /// - **Which orders.** The client holds an order as live from the response to placing it,
    ///   from a listing of open orders ([`account_snapshot`](ExecutionClient::account_snapshot),
    ///   [`fetch_open_orders`](ExecutionClient::fetch_open_orders)), and from its live reports on
    ///   any of its account streams, until it sees the order end, which a successful cancel does.
    ///   It holds up to 4,096 orders and forgets the oldest past that, logged at `warn`. An order
    ///   placed outside this client and never listed or reported to it is not covered. Every
    ///   instrument is checked, as the stream ignores `instruments`.
    /// - **Cost.** One `openOrders` request (weight 20) per DEX traded, together listing every
    ///   open order, then one `orderStatus` request (weight 2) for each held order the listing no
    ///   longer shows, 8 at a time. Both count against Hyperliquid's REST limit of 1,200 a minute
    ///   per IP address. The cost is per stream: a perpetuals and a spot stream on one wallet each
    ///   make their own.
    /// - **Failures.** Each order's lookup is settled as it ends. An instrument whose listing or
    ///   lookup fails, or whose check is still running after 30 s, is retried while the stream is
    ///   open 1, 2, 4, 8 and 16 minutes later, asking only about the orders still held, then given
    ///   up, logged at `error`; its orders are asked about again at the next reconnect. A listing
    ///   that fails charges every instrument in it. An order Hyperliquid does not know stops being
    ///   held, logged at `warn`; one still live, or in a state this version cannot read, stays
    ///   held.
    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let user = self.wallet_h160();
        let base_url = self.config.base_url();

        // Create a dedicated InfoClient for WebSocket streaming.
        // Using with_reconnect() enables SDK-managed reconnection.
        let mut ws_client = InfoClient::with_reconnect(None, Some(base_url))
            .await
            .map_err(|e| ConnectivityError::Socket(e.to_string()))?;

        // Create channels for subscriptions
        let (fills_tx, mut fills_rx) = mpsc::unbounded_channel::<Message>();
        let (orders_tx, mut orders_rx) = mpsc::unbounded_channel::<Message>();

        // Subscribe to user fills
        ws_client
            .subscribe(Subscription::UserFills { user }, fills_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("UserFills subscribe: {e}")))?;

        // Subscribe to order updates
        ws_client
            .subscribe(Subscription::OrderUpdates { user }, orders_tx)
            .await
            .map_err(|e| ConnectivityError::Socket(format!("OrderUpdates subscribe: {e}")))?;

        info!(%user, "Subscribed to Hyperliquid account stream");

        // Create output channel for merged events
        let (event_tx, event_rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();

        // CancellationToken ensures tasks exit when stream is dropped
        let cancel_token = CancellationToken::new();

        // Both tasks share `event_tx` and both observe `recv() -> None` when the SDK gives up on the
        // socket, so guard the terminal emit with a shared flag — only the first non-cancellation
        // terminal sends `StreamTerminated`, avoiding a double-emit. Relaxed ordering suffices: the
        // flag has no associated payload to synchronise; the channel send is the synchronisation point.
        let terminated = Arc::new(AtomicBool::new(false));

        // Scoped to this stream rather than to the client: a fresh `account_stream` opens a fresh
        // subscription, whose snapshot the new consumer has not seen. It must outlive a reconnect,
        // which it does -- the SDK reconnects underneath this task, not around it.
        let fills_dedup = new_dedup_cache();

        // How the orders held as live ended while the socket was down: checked after each
        // reconnect, once the fills snapshot that opens the resubscription has been sent on. Its
        // own token also stops it promptly when the stream terminates.
        let reconnected = Arc::new(Notify::new());
        let checks_cancel = cancel_token.child_token();
        let (list_client, lookup_client) = (self.info_client.clone(), self.info_client.clone());
        let (list_dexes, lookup_dexes) = (self.dexes.clone(), self.dexes.clone());
        spawn_order_checks(
            ExchangeId::HyperliquidPerp,
            self.known_live.clone(),
            reconnected.clone(),
            checks_cancel.clone(),
            event_tx.clone(),
            move |instruments| {
                perp_listed_cids(list_client.clone(), list_dexes.clone(), user, instruments)
            },
            move |key| perp_order_lookup(lookup_client.clone(), lookup_dexes.clone(), user, key),
        );

        // Spawn task to process fills
        let fills_event_tx = event_tx.clone();
        let fills_cancel = cancel_token.clone();
        let fills_terminated = terminated.clone();
        let fills_checks_cancel = checks_cancel.clone();
        let fills_dexes = self.dexes.clone();
        tokio::spawn(async move {
            let mut unknown_coins = UnknownCoins::default();
            let mut unconfigured = UnconfiguredDexes::default();
            let mut reconnects = ReconnectWatch::new(reconnected);
            loop {
                tokio::select! {
                    biased;
                    () = fills_cancel.cancelled() => {
                        debug!("Fills task cancelled");
                        return;
                    }
                    msg = fills_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Fills receiver closed");
                            fills_checks_cancel.cancel();
                            // SDK gave up on the stream (channel closed). Emit a single terminal
                            // StreamTerminated across both tasks (guarded by the shared flag).
                            if fills_terminated
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                            {
                                emit_stream_terminated(
                                    &fills_event_tx,
                                    ExchangeId::HyperliquidPerp,
                                    StreamTerminationReason::Error(
                                        "hyperliquid account stream closed".to_string(),
                                    ),
                                );
                            }
                            return;
                        };
                        match msg {
                            Message::UserFills(fills) => {
                                let convert = |fill: &hyperliquid_rust_sdk::TradeInfo| {
                                    unknown_coins.warn_once(&fill.coin);
                                    unconfigured.warn_once(&fills_dexes, &fill.coin);
                                    fill_to_account_event(fill, &fills_dexes)
                                };
                                if !send_fills(
                                    &fills.data,
                                    convert,
                                    &fills_dedup,
                                    &fills_event_tx,
                                    &mut reconnects,
                                ) {
                                    debug!("Fills event channel closed");
                                    return;
                                }
                            }
                            Message::NoData => {
                                warn!("UserFills WebSocket disconnected");
                                reconnects.dropped();
                            }
                            Message::HyperliquidError(e) => {
                                // Transient, non-terminal: the loop continues. Log only — no
                                // in-band event (consumers took no action on the old StreamError).
                                error!(%e, "UserFills WebSocket error");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Spawn task to process order updates
        // NOTE: ws_client is moved here to keep the WebSocket alive. When this task
        // exits (via cancellation or channel close), the WebSocket connection closes,
        // which causes fills_rx to also close.
        let orders_event_tx = event_tx;
        let orders_cancel = cancel_token.clone();
        let orders_terminated = terminated;
        let orders_known = self.known_live.clone();
        let orders_dexes = self.dexes.clone();
        tokio::spawn(async move {
            let _ws_client = ws_client;
            let mut unknown_coins = UnknownCoins::default();
            let mut unconfigured = UnconfiguredDexes::default();

            loop {
                tokio::select! {
                    biased;
                    () = orders_cancel.cancelled() => {
                        debug!("Orders task cancelled");
                        return;
                    }
                    msg = orders_rx.recv() => {
                        let Some(msg) = msg else {
                            debug!("Orders receiver closed");
                            checks_cancel.cancel();
                            // SDK gave up on the stream (channel closed). Emit a single terminal
                            // StreamTerminated across both tasks (guarded by the shared flag).
                            if orders_terminated
                                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                            {
                                emit_stream_terminated(
                                    &orders_event_tx,
                                    ExchangeId::HyperliquidPerp,
                                    StreamTerminationReason::Error(
                                        "hyperliquid account stream closed".to_string(),
                                    ),
                                );
                            }
                            return;
                        };
                        match msg {
                            Message::OrderUpdates(updates) => {
                                for update in updates.data {
                                    unknown_coins.warn_once(&update.order.coin);
                                    unconfigured.warn_once(&orders_dexes, &update.order.coin);
                                    if let Some(event) =
                                        order_update_to_account_event(&update, &orders_dexes)
                                        && !send_observed(&orders_known, &orders_event_tx, event)
                                    {
                                        debug!("Orders event channel closed");
                                        return;
                                    }
                                }
                            }
                            Message::NoData => {
                                warn!("OrderUpdates WebSocket disconnected");
                            }
                            Message::HyperliquidError(e) => {
                                // Transient, non-terminal: the loop continues. Log only — no
                                // in-band event (consumers took no action on the old StreamError).
                                error!(%e, "OrderUpdates WebSocket error");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Wrap stream with drop guard that cancels tasks
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(event_rx);
        let guarded_stream = CancelOnDropStream::new(stream, cancel_token);
        Ok(guarded_stream.boxed())
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> UnindexedOrderResponseCancel {
        use crate::order::{request::OrderResponseCancel, state::Cancelled};
        use hyperliquid_rust_sdk::{ClientCancelRequest, ClientCancelRequestCloid};
        use uuid::Uuid;

        let coin = match self.dexes.coin(request.key.instrument) {
            Ok(coin) => coin.to_owned(),
            Err(reason) => {
                warn!(%reason, cid = %request.key.cid, "Cannot cancel order on this instrument");
                return OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::InstrumentInvalid(
                            request.key.instrument.clone(),
                            reason,
                        ),
                    )),
                };
            }
        };

        enum CancelMethod {
            ByOid(u64),
            ByCloid(Uuid),
        }

        // How the venue addresses the order decides which endpoint cancels it. This reads the
        // identifier's kind rather than inferring it from the string's shape: an oid and a cloid
        // are different identifiers that happen to both be text, and telling them apart by parsing
        // means a cloid which parses as a number gets cancelled as somebody else's oid.
        let cancel_method = match &request.state.id {
            Some(VenueOrderId::Assigned(id)) => {
                id.0.parse::<u64>()
                    .map(CancelMethod::ByOid)
                    .map_err(|_| "venue order id is not a numeric Hyperliquid oid")
            }
            // The venue accepted the order without assigning an oid, so the only handle on it is
            // the cloid it was placed with -- which placement derives from the client id the same
            // way, and refuses the order if it cannot.
            Some(VenueOrderId::ClientAssigned) => cid_to_cloid(&request.key.cid)
                .map(CancelMethod::ByCloid)
                .ok_or("order is addressable only by its client id, which is not a canonical UUID"),
            None => Err("order is still in flight; the venue has acknowledged nothing to cancel"),
        };

        let cancel_method = match cancel_method {
            Ok(method) => method,
            Err(reason) => {
                warn!(%reason, cid = %request.key.cid, "Cannot determine how to cancel order");
                return OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(reason.to_string()),
                    )),
                };
            }
        };

        // Terminal states keep a plain `OrderId`: nothing compares them for identity, so an order
        // the venue never named records the client id it was addressed by instead.
        let cancelled_id = match &request.state.id {
            Some(venue_id) => venue_id.or_client_id(&request.key.cid),
            None => OrderId(request.key.cid.0.clone()),
        };

        let response = match cancel_method {
            CancelMethod::ByOid(oid) => {
                debug!(oid, "Cancelling by OID");
                let cancel_request = ClientCancelRequest { asset: coin, oid };
                self.exchange_client.cancel(cancel_request, None).await
            }
            CancelMethod::ByCloid(cloid) => {
                debug!(%cloid, "Cancelling by cloid (no oid assigned)");
                let cancel_request = ClientCancelRequestCloid { asset: coin, cloid };
                self.exchange_client
                    .cancel_by_cloid(cancel_request, None)
                    .await
            }
        };

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Cancel order failed (transport)");
                return OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(map_order_error(e, request.key.instrument)),
                };
            }
        };

        match cancel_outcome(response) {
            Ok(()) => {
                debug!("Cancel order accepted");
                // Hyperliquid answers a cancel once the order has left the book.
                self.known_live.lock().ended(&request.key.cid);
                // Hyperliquid cancel response doesn't include an exchange timestamp
                OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Ok(Cancelled::new(
                        cancelled_id.clone(),
                        Utc::now(),
                        None, // Cancel response doesn't include filled quantity
                    )),
                }
            }
            // Not reported ended: if the order did end, the account stream or a lookup says how.
            Err(reason) => {
                warn!(%reason, cid = %request.key.cid, "Cancel rejected by exchange");
                OrderResponseCancel {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    state: Err(UnindexedOrderError::Rejected(
                        crate::error::ApiError::OrderRejected(reason),
                    )),
                }
            }
        }
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
        use hyperliquid_rust_sdk::{
            ClientOrderRequest, ExchangeDataStatus, ExchangeResponseStatus,
        };

        let make_inactive =
            |error: UnindexedOrderError| -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
                Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(error),
                }
            };
        let make_rejected = |msg: String| {
            make_inactive(OrderError::Rejected(crate::error::ApiError::OrderRejected(
                msg,
            )))
        };
        let make_unsupported = |msg: String| make_inactive(OrderError::UnsupportedOrderType(msg));
        let make_invalid_instrument = |reason: String| {
            make_inactive(OrderError::Rejected(
                crate::error::ApiError::InstrumentInvalid(request.key.instrument.clone(), reason),
            ))
        };

        let coin = match self.dexes.coin(request.key.instrument) {
            Ok(coin) => coin.to_owned(),
            Err(reason) => return make_invalid_instrument(reason),
        };
        let is_buy = request.state.side == Side::Buy;

        match request.state.kind {
            OrderKind::Market => {
                return make_unsupported(
                    "Hyperliquid does not support market orders; use Limit with IOC time-in-force"
                        .to_string(),
                );
            }
            OrderKind::TrailingStop { .. } | OrderKind::TrailingStopLimit { .. } => {
                return make_unsupported(
                    "Hyperliquid does not support trailing stop orders".to_string(),
                );
            }
            OrderKind::Limit
            | OrderKind::Stop { .. }
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TakeProfitLimit { .. } => {}
        }

        // The venue reports every order under its cloid, so an order placed without one could
        // not be matched to the id the caller tracks it by. See the module docs.
        let Some(cloid) = cid_to_cloid(&request.key.cid) else {
            return make_rejected(CLOID_REQUIRED.to_string());
        };

        // Map time-in-force (warn if FOK is substituted with IOC)
        if matches!(request.state.time_in_force, TimeInForce::FillOrKill) {
            warn!(
                instrument = %request.key.instrument,
                "FillOrKill not supported by Hyperliquid, using ImmediateOrCancel (may result in partial fills)"
            );
        }
        let tif = map_tif(&request.state.time_in_force).to_string();

        let Some(precision) = self.order_precision(request.key.instrument) else {
            return make_invalid_instrument(format!(
                "{coin} was not listed when the client connected; reconnect to trade a perpetual \
                 listed since"
            ));
        };
        let wire = match wire_order(
            &precision,
            request.state.kind,
            tif,
            request.state.price,
            request.state.quantity,
        ) {
            Ok(wire) => wire,
            Err(WireOrderError::Precision(violation)) => {
                return make_inactive(OrderError::InvalidPrecision(violation));
            }
            Err(WireOrderError::MissingPrice) => {
                return make_rejected(
                    "Hyperliquid requires limit price for Limit/StopLimit/TakeProfitLimit orders"
                        .to_string(),
                );
            }
            Err(WireOrderError::UnsupportedKind) => {
                return make_unsupported(format!(
                    "Hyperliquid does not support {} orders",
                    request.state.kind
                ));
            }
        };

        let order_request = ClientOrderRequest {
            asset: coin,
            is_buy,
            reduce_only: request.state.reduce_only,
            limit_px: wire.limit_px,
            sz: wire.sz,
            cloid: Some(cloid),
            order_type: wire.order_type,
        };

        let response = match self.exchange_client.order(order_request, None).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "Open order failed");
                return Order {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument: request.key.instrument.clone(),
                        strategy: request.key.strategy.clone(),
                        cid: request.key.cid.clone(),
                    },
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: OrderState::inactive(map_order_error(e, request.key.instrument)),
                };
            }
        };

        // Parse response
        let state = match response {
            ExchangeResponseStatus::Ok(exchange_resp) => {
                // Check status from response data
                let status = exchange_resp
                    .data
                    .and_then(|d| d.statuses.into_iter().next());

                match status {
                    Some(ExchangeDataStatus::Resting(resting)) => {
                        debug!(oid = resting.oid, "Order resting");
                        OrderState::active(Open {
                            id: VenueOrderId::Assigned(OrderId(format_smolstr!("{}", resting.oid))),
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Filled(filled)) => {
                        debug!(oid = filled.oid, avg_px = %filled.avg_px, "Order filled");
                        // Hyperliquid provides avg_px for filled orders
                        let avg_price = parse_decimal(&filled.avg_px, "avg_px");
                        OrderState::fully_filled(Filled::new(
                            OrderId(format_smolstr!("{}", filled.oid)),
                            Utc::now(),
                            parse_decimal(&filled.total_sz, "total_sz")
                                .unwrap_or(request.state.quantity),
                            avg_price,
                        ))
                    }
                    Some(ExchangeDataStatus::Error(msg)) => {
                        warn!(%msg, "Order rejected by exchange");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(msg),
                        ))
                    }
                    Some(
                        ExchangeDataStatus::WaitingForFill | ExchangeDataStatus::WaitingForTrigger,
                    ) => {
                        // The venue answered without an oid, so the order's only handle is the
                        // cloid it was placed with. A standalone trigger order is answered
                        // `resting` with an oid; these statuses belong to grouped orders.
                        debug!(cid = %request.key.cid, "Order waiting under its cloid");
                        OrderState::active(Open {
                            id: VenueOrderId::ClientAssigned,
                            time_exchange: Utc::now(),
                            filled_quantity: Decimal::ZERO,
                        })
                    }
                    Some(ExchangeDataStatus::Success) | None => {
                        // Generic success without order ID — SDK didn't return structured data.
                        // This shouldn't happen for limit orders; reject to avoid silent failures.
                        warn!("Order accepted but no order ID returned");
                        OrderState::inactive(OrderError::Rejected(
                            crate::error::ApiError::OrderRejected(
                                "no order ID in response".to_string(),
                            ),
                        ))
                    }
                }
            }
            ExchangeResponseStatus::Err(msg) => {
                warn!(%msg, "Order rejected");
                OrderState::inactive(OrderError::Rejected(crate::error::ApiError::OrderRejected(
                    msg,
                )))
            }
        };
        let order = Order {
            key: OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
                instrument: request.key.instrument.clone(),
                strategy: request.key.strategy.clone(),
                cid: request.key.cid.clone(),
            },
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state,
        };
        self.known_live
            .lock()
            .placed(&order.key, order.quantity, &order.state);
        order
    }

    /// The collateral balances, read as [`account_snapshot`](ExecutionClient::account_snapshot)
    /// reads them: `userAbstraction` (weight 20), then either `spotClearinghouseState` or each
    /// DEX's `clearinghouseState` (weight 2 each).
    async fn fetch_balances(
        &self,
        _assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        perp_account::fetch_balances(&self.info_client, self.wallet_h160(), &self.dexes).await
    }

    /// Every open order on each DEX traded, with one `openOrders` (weight 20) per DEX.
    ///
    /// # Errors
    ///
    /// [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for an instrument
    /// in `instruments` that names no perpetual this client trades.
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        self.check_instruments(instruments)?;
        let address = self.wallet_h160();

        let open_orders =
            perp_account::open_orders(&self.info_client, address, &self.dexes).await?;
        warn_unknown_coins(open_orders.iter().map(|order| order.coin.as_str()));

        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        let orders: Vec<_> = open_orders
            .iter()
            .filter_map(|order| {
                let instrument = self.dexes.instrument(&order.coin)?;
                if instrument_filter
                    .as_ref()
                    .is_some_and(|f| !f.contains(&instrument))
                {
                    return None;
                }
                open_order_to_order(order, ExchangeId::HyperliquidPerp, instrument)
            })
            .collect();
        remember_open(&self.known_live, &orders);
        Ok(orders)
    }

    /// Reads the span with `userFillsByTime`, to its end within the call, so the read is always
    /// complete (`resume: None`). Hyperliquid keeps only each wallet's 10,000 most recent fills,
    /// so a span reaching further back is read only as far as those go. One read covers every
    /// DEX; fills on a HIP-3 DEX this client is not configured with are left out.
    ///
    /// # Errors
    ///
    /// [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for an instrument
    /// in `instruments` that names no perpetual this client trades.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        self.check_instruments(instruments)?;
        let Some((start_ms, end_ms)) = span_millis(start, end) else {
            return Ok(TradesRead::complete(Vec::new()));
        };
        let address = self.wallet_h160();

        let fills = user_fills_by_time(&self.info_client, address, start_ms, end_ms).await?;
        warn_unknown_coins(fills.iter().map(|fill| fill.coin.as_str()));
        warn_unconfigured_dexes(&self.dexes, fills.iter().map(|fill| fill.coin.as_str()));

        let instrument_filter: Option<HashSet<_>> = if instruments.is_empty() {
            None
        } else {
            let mut set = HashSet::with_capacity(instruments.len());
            set.extend(instruments.iter().cloned());
            Some(set)
        };

        let mut result = Vec::new();
        for fill in fills {
            let Some((instrument, collateral)) = self.dexes.perp(&fill.coin) else {
                continue;
            };

            if instrument_filter
                .as_ref()
                .is_some_and(|f| !f.contains(&instrument))
            {
                continue;
            }

            let Some(side) = parse_side(&fill.side) else {
                continue;
            };
            let Some(price) = parse_decimal(&fill.px, "px") else {
                continue;
            };
            let Some(quantity) = parse_decimal(&fill.sz, "sz") else {
                continue;
            };
            let fee = parse_decimal(&fill.fee, "fee").unwrap_or(Decimal::ZERO);

            let Some(time_exchange) = millis_to_datetime(fill.time) else {
                warn!(time = fill.time, "Invalid fill timestamp, skipping");
                continue;
            };
            if !(start..=end).contains(&time_exchange) {
                continue;
            }

            result.push(Trade {
                id: TradeId(format_smolstr!("{}", fill.tid)),
                order_id: OrderId(format_smolstr!("{}", fill.oid)),
                instrument,
                strategy: StrategyId::unknown(),
                time_exchange,
                side,
                price,
                quantity,
                // `TradeInfo` carries no cumulative filled quantity; Hyperliquid reports order
                // state as its own `OrderUpdate` message.
                order_filled_quantity: None,
                fees: perp_fill_fees(fill.fee_token.as_deref(), fee, collateral),
            });
        }

        Ok(TradesRead::complete(result))
    }
}

/// Looks each order up with `orderStatus` (weight 2), by the cloid it was placed with, or by its
/// `oid` for an order placed without one, which this client reports under its `oid`. Up to 8 run
/// at once.
///
/// `filled` is reported as fully filled, without an average price, which the record does not
/// carry; every status ending in `Canceled`, and `scheduledCancel`, as cancelled with what filled
/// before it; and every status ending in `Rejected` as
/// [`OpenFailed`](crate::order::state::InactiveOrderState::OpenFailed). Hyperliquid has no expiry.
/// An order the venue does not know, one on another instrument than the key's, and an id that is
/// neither a canonical UUID nor an `oid` are unknown. An `oid` whose order carries a cloid is
/// unknown too, since this client reports that order under its cloid.
///
/// The account stream runs the same lookup itself after a reconnect; see
/// [`account_stream`](ExecutionClient::account_stream).
impl OrderStatusClient for HyperliquidClient {
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        let address = self.wallet_h160();
        fetch_ended_by_key(orders, |key| {
            perp_order_lookup(self.info_client.clone(), self.dexes.clone(), address, key)
        })
        .await
    }
}

/// Look the order under `key` up with `orderStatus`, on the perpetual its coin names.
async fn perp_order_lookup(
    info_client: Arc<InfoClient>,
    dexes: Arc<PerpDexes>,
    address: ethers::types::H160,
    key: UnindexedOrderKey,
) -> Result<OrderLookup, UnindexedClientError> {
    let Some(record) = fetch_order_record(&info_client, address, &key.cid).await? else {
        return Ok(OrderLookup::Unknown);
    };
    let instrument = dexes.instrument(&record.order.coin);
    Ok(lookup_from_record(key, &record, instrument))
}

/// The client order ids `openOrders` (weight 20 per DEX traded) lists on the perpetuals
/// `instruments`, for a reconnect's check of the orders held as live.
async fn perp_listed_cids(
    info_client: Arc<InfoClient>,
    dexes: Arc<PerpDexes>,
    address: ethers::types::H160,
    instruments: Vec<InstrumentNameExchange>,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let rows = perp_account::open_orders(&info_client, address, &dexes).await?;
    Ok(listed_cids(&rows, &instruments, |coin| {
        dexes.instrument(coin)
    }))
}

/// Report one perpetual position from Hyperliquid's user state.
///
/// [`PositionReport::Open`] for a non-zero size, [`PositionReport::Flat`] for zero, and
/// [`PositionReport::Unreported`] when the size does not parse (with a warning): an unreadable
/// size establishes nothing, so it must not read as flat.
fn perp_position_report(
    position: &hyperliquid_rust_sdk::PositionData,
    now: DateTime<Utc>,
) -> PositionReport {
    let Some(quantity) = parse_decimal(&position.szi, "szi") else {
        return PositionReport::Unreported;
    };
    PositionReport::from_position(Position::new(
        quantity,
        position
            .entry_px
            .as_ref()
            .and_then(|p| parse_decimal(p, "entry_px")),
        parse_decimal(&position.unrealized_pnl, "unrealized_pnl"),
        parse_decimal(&position.margin_used, "margin_used"),
        position
            .liquidation_px
            .as_ref()
            .and_then(|p| parse_decimal(p, "liquidation_px")),
        Some(Decimal::from(position.leverage.value)),
        now,
    ))
}

/// The fee of a fill on a perpetual whose DEX settles in `collateral`, in the asset `fee_token`
/// names.
///
/// The venue's `feeToken` is read rather than assumed; only its absence, which has not been
/// observed, falls back to `collateral`.
///
/// The quote-equivalent is set when the fee is in `collateral`, which is the instrument's quote,
/// and is `None` otherwise. It serves unindexed consumers: the indexer recomputes it either way,
/// from the instrument's own quote and base.
fn perp_fill_fees(
    fee_token: Option<&str>,
    fee: Decimal,
    collateral: &str,
) -> AssetFees<AssetNameExchange> {
    let asset = fee_token.unwrap_or(collateral);
    let fees_quote = (asset == collateral).then_some(fee);
    AssetFees::new(AssetNameExchange::from(asset), fee, fees_quote)
}

/// Convert SDK TradeInfo (fill) to AccountEvent::Trade, `None` if its coin names no perpetual
/// this client trades.
fn fill_to_account_event(
    fill: &hyperliquid_rust_sdk::TradeInfo,
    dexes: &PerpDexes,
) -> Option<UnindexedAccountEvent> {
    let (instrument, collateral) = dexes.perp(&fill.coin)?;
    let side = parse_side(&fill.side)?;
    let price = parse_decimal(&fill.px, "fill.px")?;
    let quantity = parse_decimal(&fill.sz, "fill.sz")?;
    let fee = parse_decimal(&fill.fee, "fill.fee").unwrap_or(Decimal::ZERO);
    let time_exchange = millis_to_datetime(fill.time)?;
    let order_id = OrderId(format_smolstr!("{}", fill.oid));

    let trade = Trade {
        id: TradeId(format_smolstr!("{}", fill.tid)),
        order_id,
        instrument,
        strategy: StrategyId::unknown(),
        time_exchange,
        side,
        price,
        quantity,
        // `TradeInfo` carries no cumulative filled quantity, so the order's state must be learned
        // from an `OrderUpdate` -- which Hyperliquid sends as its own message.
        order_filled_quantity: None,
        fees: perp_fill_fees(Some(&fill.fee_token), fee, collateral),
    };

    Some(AccountEvent::new(
        ExchangeId::HyperliquidPerp,
        AccountEventKind::Trade(trade),
    ))
}

/// Convert SDK OrderUpdate to AccountEvent::OrderSnapshot, `None` if its coin names no
/// perpetual this client trades.
fn order_update_to_account_event(
    update: &hyperliquid_rust_sdk::OrderUpdate,
    dexes: &PerpDexes,
) -> Option<UnindexedAccountEvent> {
    common::order_update_to_account_event(
        update,
        ExchangeId::HyperliquidPerp,
        dexes.instrument(&update.order.coin)?,
    )
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::client::dedup::{dedup_key_from_event, is_duplicate};
    use rust_decimal_macros::dec;
    use rustrade_integration::collection::snapshot::Snapshot;
    use std::collections::HashMap;

    /// Only Hyperliquid's default DEX.
    fn default_dex() -> PerpDexes {
        PerpDexes::default()
    }

    mod order_recovery {
        use super::super::common::info_tests::info_client_against;
        use super::super::order_recovery::tests::{CID, CLOID, record_json, row};
        use super::*;
        use crate::order::state::InactiveOrderState;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        async fn serve(body: serde_json::Value) -> (MockServer, Arc<InfoClient>) {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/info"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Arc::new(info_client_against(server.uri()).await);
            (server, client)
        }

        fn key() -> UnindexedOrderKey {
            OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
                instrument: InstrumentNameExchange::new("BTC-USDC-PERP"),
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::new(CID),
            }
        }

        fn order_status(coin: &str) -> serde_json::Value {
            serde_json::json!({
                "status": "order",
                "order": record_json(coin, "canceled", "0.01", Some(CLOID)),
            })
        }

        #[test]
        fn a_reconnect_snapshot_wakes_the_check_once_its_fills_are_sent() {
            use futures::FutureExt as _;
            let notified = |reconnected: &Notify| reconnected.notified().now_or_never().is_some();
            let snapshot = |fills| hyperliquid_rust_sdk::UserFillsData {
                is_snapshot: Some(true),
                user: ethers::types::H160::zero(),
                fills,
            };
            let reconnected = Arc::new(Notify::new());
            let mut reconnects = ReconnectWatch::new(reconnected.clone());
            let dedup = new_dedup_cache();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let tids = |rx: &mut mpsc::UnboundedReceiver<UnindexedAccountEvent>| {
                std::iter::from_fn(|| rx.try_recv().ok())
                    .map(|event| match event.kind {
                        AccountEventKind::Trade(trade) => trade.id.0.to_string(),
                        other => panic!("expected a trade, got {other:?}"),
                    })
                    .collect::<Vec<_>>()
            };

            // The snapshot opening the first subscription.
            let first = snapshot(vec![sweep_fill(1, "60000")]);
            assert!(send_fills(
                &first,
                |fill| fill_to_account_event(fill, &default_dex()),
                &dedup,
                &tx,
                &mut reconnects
            ));
            assert_eq!(tids(&mut rx), ["1"]);
            assert!(!notified(&reconnected));

            // A drop, then the resubscription's snapshot: its new fill is sent, then the check
            // woken.
            reconnects.dropped();
            let resubscribed = snapshot(vec![sweep_fill(1, "60000"), sweep_fill(2, "60001")]);
            assert!(send_fills(
                &resubscribed,
                |fill| fill_to_account_event(fill, &default_dex()),
                &dedup,
                &tx,
                &mut reconnects
            ));
            assert_eq!(
                tids(&mut rx),
                ["2"],
                "the fill already sent is not sent again"
            );
            assert!(notified(&reconnected));

            // With the consumer gone the fills cannot be sent, and the check is not woken.
            drop(rx);
            reconnects.dropped();
            let unsent = snapshot(vec![sweep_fill(3, "60002")]);
            assert!(!send_fills(
                &unsent,
                |fill| fill_to_account_event(fill, &default_dex()),
                &dedup,
                &tx,
                &mut reconnects
            ));
            assert!(!notified(&reconnected));
        }

        #[tokio::test]
        async fn a_perp_lookup_reads_how_the_order_ended() {
            let (_server, client) = serve(order_status("BTC")).await;

            let lookup =
                perp_order_lookup(client, Arc::default(), ethers::types::H160::zero(), key())
                    .await
                    .unwrap();

            let OrderLookup::Ended(order) = lookup else {
                panic!("expected an ended order, got {lookup:?}");
            };
            assert_eq!(order.key, key());
            assert!(matches!(order.state, InactiveOrderState::Cancelled(_)));
        }

        #[tokio::test]
        async fn a_perp_lookup_of_a_spot_order_is_unknown() {
            let (_server, client) = serve(order_status("@107")).await;

            let lookup =
                perp_order_lookup(client, Arc::default(), ethers::types::H160::zero(), key())
                    .await
                    .unwrap();

            assert!(matches!(lookup, OrderLookup::Unknown));
        }

        #[tokio::test]
        async fn a_perp_listing_names_only_the_orders_on_the_perpetuals_asked_about() {
            let rows = serde_json::json!([
                row("BTC", 1, Some(CLOID)),
                row("ETH", 2, None),
                row("@107", 3, None),
            ]);
            let (_server, client) = serve(rows).await;

            let listed = perp_listed_cids(
                client,
                Arc::default(),
                ethers::types::H160::zero(),
                vec![InstrumentNameExchange::new("BTC-USDC-PERP")],
            )
            .await
            .unwrap();

            assert_eq!(
                listed.into_iter().collect::<Vec<_>>(),
                [ClientOrderId::new(CID)]
            );
        }
    }

    /// A client trading `xyz` and `flx` against `server`, built without the network, that places
    /// and cancels on the assets in `coin_to_asset`. The default DEX lists `BTC`, with
    /// `szDecimals` 5 as on mainnet.
    async fn client_against(
        server: &wiremock::MockServer,
        coin_to_asset: HashMap<String, u32>,
    ) -> HyperliquidClient {
        use super::common::info_tests::{
            exchange_client_against, info_client_against, test_wallet,
        };
        let mut dexes = super::perp_dexes::tests::xyz_and_flx().await;
        dexes.add_default_perps(&[hyperliquid_rust_sdk::AssetMeta {
            name: "BTC".to_owned(),
            sz_decimals: 5,
        }]);
        HyperliquidClient {
            config: HyperliquidConfig::new(test_wallet(), Network::Testnet),
            info_client: Arc::new(info_client_against(server.uri()).await),
            exchange_client: Arc::new(exchange_client_against(server.uri(), coin_to_asset).await),
            dexes: Arc::new(dexes),
            known_live: KnownLiveOrders::shared(ExchangeId::HyperliquidPerp),
        }
    }

    mod precision {
        use super::*;
        use crate::error::{OrderField, PrecisionLimit, PrecisionViolation};
        use crate::order::{OrderEvent, request::RequestOpen, state::InactiveOrderState};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// A server whose exchange endpoint rests every order it is sent.
        async fn resting_server() -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/exchange"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "ok",
                    "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 77}}]}},
                })))
                .mount(&server)
                .await;
            server
        }

        async fn place(
            client: &HyperliquidClient,
            instrument: &InstrumentNameExchange,
            kind: OrderKind,
            price: Option<Decimal>,
            quantity: Decimal,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            client
                .open_order(OrderEvent {
                    key: OrderKey {
                        exchange: ExchangeId::HyperliquidPerp,
                        instrument,
                        strategy: StrategyId::new("strategy"),
                        cid: ClientOrderId::uuid(),
                    },
                    state: RequestOpen {
                        side: Side::Buy,
                        price,
                        quantity,
                        kind,
                        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                        position_id: None,
                        reduce_only: false,
                        market: None,
                    },
                })
                .await
        }

        /// The orders the server was sent, as the SDK wrote them.
        async fn sent_orders(server: &MockServer) -> Vec<serde_json::Value> {
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|request| request.url.path() == "/exchange")
                .map(|request| {
                    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                    body["action"]["orders"][0].clone()
                })
                .collect()
        }

        #[tokio::test]
        async fn the_client_reports_each_perpetuals_precision() {
            let server = MockServer::start().await;
            let client = client_against(&server, HashMap::new()).await;

            assert_eq!(
                client.order_precision(&InstrumentNameExchange::from("BTC-USDC-PERP")),
                Some(OrderPrecision::perp(5))
            );
            assert_eq!(
                client.order_precision(&InstrumentNameExchange::from("xyz:TSLA-USDC-PERP")),
                Some(OrderPrecision::perp(2)),
                "a HIP-3 perpetual's szDecimals come from its DEX's meta"
            );
            assert_eq!(
                client.order_precision(&InstrumentNameExchange::from("ETH-USDC-PERP")),
                None,
                "not listed when the client connected"
            );
            assert_eq!(
                client.order_precision(&InstrumentNameExchange::from("HYPE-USDC-SPOT")),
                None
            );
        }

        #[tokio::test]
        async fn an_order_breaking_the_precision_rules_is_refused_unsent() {
            let server = resting_server().await;
            let client = client_against(&server, HashMap::from([("BTC".to_owned(), 0)])).await;
            let btc = InstrumentNameExchange::from("BTC-USDC-PERP");

            let cases = [
                (
                    OrderKind::Limit,
                    dec!(60000),
                    dec!(0.000001),
                    OrderField::Quantity,
                    PrecisionLimit::DecimalPlaces { max: 5 },
                ),
                (
                    OrderKind::Limit,
                    dec!(65432.1),
                    dec!(0.001),
                    OrderField::Price,
                    PrecisionLimit::SignificantFigures { max: 5 },
                ),
                (
                    OrderKind::Limit,
                    dec!(6543.21),
                    dec!(0.001),
                    OrderField::Price,
                    PrecisionLimit::DecimalPlaces { max: 1 },
                ),
                (
                    OrderKind::StopLimit {
                        trigger_price: dec!(6543.21),
                    },
                    dec!(6543),
                    dec!(0.001),
                    OrderField::TriggerPrice,
                    PrecisionLimit::DecimalPlaces { max: 1 },
                ),
            ];
            for (kind, price, quantity, field, limit) in cases {
                let order = place(&client, &btc, kind, Some(price), quantity).await;
                let expected = PrecisionViolation {
                    field,
                    value: match field {
                        OrderField::Quantity => quantity,
                        OrderField::TriggerPrice => dec!(6543.21),
                        _ => price,
                    },
                    limit,
                };
                assert_eq!(
                    order.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(
                        OrderError::InvalidPrecision(expected)
                    )),
                    "{kind} at {price} for {quantity}"
                );
            }
            assert_eq!(sent_orders(&server).await, Vec::<serde_json::Value>::new());
        }

        #[tokio::test]
        async fn a_valid_order_is_sent_unchanged() {
            let server = resting_server().await;
            let client = client_against(&server, HashMap::from([("BTC".to_owned(), 0)])).await;
            let btc = InstrumentNameExchange::from("BTC-USDC-PERP");

            // An integer price past 5 significant figures, which the client once rounded to 123460.
            let order = place(
                &client,
                &btc,
                OrderKind::Limit,
                Some(dec!(123456)),
                dec!(0.00123),
            )
            .await;
            assert!(
                matches!(order.state, OrderState::Active(_)),
                "{:?}",
                order.state
            );

            let sent = sent_orders(&server).await;
            assert_eq!(sent.len(), 1);
            assert_eq!(
                (&sent[0]["p"], &sent[0]["s"]),
                (&serde_json::json!("123456"), &serde_json::json!("0.00123"))
            );
        }

        #[tokio::test]
        async fn an_order_on_a_perpetual_listed_since_connecting_is_refused_unsent() {
            let server = resting_server().await;
            let client = client_against(&server, HashMap::from([("ETH".to_owned(), 1)])).await;

            let order = place(
                &client,
                &InstrumentNameExchange::from("ETH-USDC-PERP"),
                OrderKind::Limit,
                Some(dec!(3000)),
                dec!(1),
            )
            .await;
            let OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                crate::error::ApiError::InstrumentInvalid(_, reason),
            ))) = &order.state
            else {
                panic!("expected InstrumentInvalid, got {:?}", order.state);
            };
            assert!(reason.contains("reconnect"), "{reason}");
            assert_eq!(sent_orders(&server).await, Vec::<serde_json::Value>::new());
        }
    }

    mod cancel {
        use super::super::common::info_tests::{CANCEL_NOT_APPLIED, cancel_answer};
        use super::*;
        use crate::error::ApiError;
        use crate::order::{OrderEvent, request::RequestCancel};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// Cancel a live `BTC-USDC-PERP` order through a client whose exchange endpoint answers
        /// `answer`. Returns the response, and whether the order is still held live afterwards.
        async fn cancel_answered(
            answer: serde_json::Value,
        ) -> (UnindexedOrderResponseCancel, bool) {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/exchange"))
                .respond_with(ResponseTemplate::new(200).set_body_json(answer))
                .mount(&server)
                .await;
            let client = client_against(&server, HashMap::from([("BTC".to_owned(), 0)])).await;
            let instrument = InstrumentNameExchange::new("BTC-USDC-PERP");
            let key = OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
                instrument: instrument.clone(),
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::uuid(),
            };
            let id = VenueOrderId::Assigned(OrderId::new("7"));
            let open = Open {
                id: id.clone(),
                time_exchange: Utc::now(),
                filled_quantity: Decimal::ZERO,
            };
            client.known_live.lock().live(&key, dec!(1), &open);

            let response = client
                .cancel_order(OrderEvent {
                    key: OrderKey {
                        exchange: key.exchange,
                        instrument: &instrument,
                        strategy: key.strategy.clone(),
                        cid: key.cid.clone(),
                    },
                    state: RequestCancel { id: Some(id) },
                })
                .await;
            let live = client.known_live.lock().contains(&key.cid);
            (response, live)
        }

        #[tokio::test]
        async fn a_success_status_cancels_the_order() {
            let (response, live) =
                cancel_answered(cancel_answer(serde_json::json!(["success"]))).await;
            assert!(response.state.is_ok(), "{:?}", response.state);
            assert!(!live, "a cancelled order is no longer held live");
        }

        /// An order that filled before the cancel arrived is answered this way, so it must not be
        /// reported cancelled, and stays held live until the account stream or a lookup ends it.
        #[tokio::test]
        async fn an_error_status_is_a_rejected_cancel_and_the_order_stays_live() {
            let (response, live) = cancel_answered(cancel_answer(serde_json::json!([
                {"error": CANCEL_NOT_APPLIED}
            ])))
            .await;
            let Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(reason))) =
                &response.state
            else {
                panic!("expected a rejected cancel, got {:?}", response.state);
            };
            assert_eq!(reason, CANCEL_NOT_APPLIED);
            assert!(live, "the order is still held live");
        }

        #[tokio::test]
        async fn an_answer_without_a_status_is_a_rejected_cancel() {
            let (response, live) = cancel_answered(serde_json::json!({
                "status": "ok",
                "response": {"type": "cancel"},
            }))
            .await;
            assert!(
                matches!(
                    response.state,
                    Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(_)))
                ),
                "{:?}",
                response.state
            );
            assert!(live, "the order is still held live");
        }
    }

    mod instrument_names {
        use super::*;
        use crate::error::ApiError;
        use crate::order::{
            OrderEvent,
            request::{RequestCancel, RequestOpen},
        };
        use wiremock::MockServer;

        /// A client trading `xyz` and `flx`, built without the network, against a server with
        /// nothing mounted: any request it sent would fail with something other than
        /// `InstrumentInvalid`.
        async fn offline_client() -> (MockServer, HyperliquidClient) {
            let server = MockServer::start().await;
            let client = client_against(&server, HashMap::new()).await;
            (server, client)
        }

        /// Names no perpetual the client trades: the old `-USD-` name, a quote other than the
        /// DEX's collateral, a DEX not configured, and a spot pair.
        const REFUSED: [&str; 4] = [
            "BTC-USD-PERP",
            "flx:TSLA-USDC-PERP",
            "km:TSLA-USDH-PERP",
            "HYPE-USDC-SPOT",
        ];

        fn is_invalid(
            error: &ApiError<AssetNameExchange, InstrumentNameExchange>,
            name: &str,
        ) -> bool {
            matches!(error, ApiError::InstrumentInvalid(instrument, _) if instrument.as_ref() == name)
        }

        #[tokio::test]
        async fn an_order_on_a_name_the_client_does_not_trade_is_refused_unsent() {
            let (_server, client) = offline_client().await;
            for name in REFUSED {
                let instrument = InstrumentNameExchange::from(name);
                let order = client
                    .open_order(OrderEvent {
                        key: OrderKey {
                            exchange: ExchangeId::HyperliquidPerp,
                            instrument: &instrument,
                            strategy: StrategyId::new("strategy"),
                            cid: ClientOrderId::uuid(),
                        },
                        state: RequestOpen {
                            side: Side::Buy,
                            price: Some(dec!(1)),
                            quantity: dec!(1),
                            kind: OrderKind::Limit,
                            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                            position_id: None,
                            reduce_only: false,
                            market: None,
                        },
                    })
                    .await;
                let OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
                    OrderError::Rejected(error),
                )) = &order.state
                else {
                    panic!("{name}: expected a rejection, got {:?}", order.state);
                };
                assert!(is_invalid(error, name), "{name}: {error:?}");
            }
        }

        #[tokio::test]
        async fn a_cancel_on_a_name_the_client_does_not_trade_is_refused_unsent() {
            let (_server, client) = offline_client().await;
            for name in REFUSED {
                let instrument = InstrumentNameExchange::from(name);
                let response = client
                    .cancel_order(OrderEvent {
                        key: OrderKey {
                            exchange: ExchangeId::HyperliquidPerp,
                            instrument: &instrument,
                            strategy: StrategyId::new("strategy"),
                            cid: ClientOrderId::uuid(),
                        },
                        state: RequestCancel {
                            id: Some(VenueOrderId::Assigned(OrderId::new("1"))),
                        },
                    })
                    .await;
                let Err(UnindexedOrderError::Rejected(error)) = &response.state else {
                    panic!("{name}: expected a rejection, got {:?}", response.state);
                };
                assert!(is_invalid(error, name), "{name}: {error:?}");
            }
        }

        #[tokio::test]
        async fn a_read_filtered_to_a_name_the_client_does_not_trade_fails_unsent() {
            let (_server, client) = offline_client().await;
            for name in REFUSED {
                let instruments = [InstrumentNameExchange::from(name)];
                let snapshot = client.account_snapshot(&[], &instruments).await;
                let open = client.fetch_open_orders(&instruments).await;
                let trades = client
                    .fetch_trades(
                        Utc::now() - chrono::Duration::hours(1),
                        Utc::now(),
                        &instruments,
                    )
                    .await;
                for error in [snapshot.err(), open.err(), trades.err()] {
                    let Some(UnindexedClientError::Api(error)) = &error else {
                        panic!("{name}: expected an API error, got {error:?}");
                    };
                    assert!(is_invalid(error, name), "{name}: {error:?}");
                }
            }
        }
    }

    fn perp_position(szi: &str) -> hyperliquid_rust_sdk::PositionData {
        serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "entryPx": "50000.0",
            "leverage": {"type": "cross", "value": 5},
            "liquidationPx": null,
            "marginUsed": "100.0",
            "positionValue": "500.0",
            "returnOnEquity": "0.0",
            "szi": szi,
            "unrealizedPnl": "1.5",
            "maxLeverage": 50,
            "cumFunding": {"allTime": "0", "sinceOpen": "0", "sinceChange": "0"}
        }))
        .unwrap()
    }

    #[test]
    fn perp_position_report_is_open_flat_or_unreported() {
        let now = Utc::now();
        let open = perp_position_report(&perp_position("-0.01"), now);
        let position = open.open().unwrap();
        assert_eq!(
            (position.quantity, position.entry_price, position.leverage),
            (dec!(-0.01), Some(dec!(50000.0)), Some(dec!(5)))
        );
        assert_eq!(
            perp_position_report(&perp_position("0.0"), now),
            PositionReport::Flat
        );
        assert_eq!(
            perp_position_report(&perp_position("not a number"), now),
            PositionReport::Unreported
        );
    }

    #[test]
    fn test_fill_to_account_event() {
        let fill_json = r#"{
            "coin": "BTC",
            "side": "B",
            "px": "65000.5",
            "sz": "0.1",
            "time": 1714100000000,
            "hash": "0xabc123",
            "startPosition": "0",
            "dir": "Open Long",
            "closedPnl": "0",
            "oid": 12345,
            "cloid": null,
            "crossed": false,
            "fee": "0.65",
            "feeToken": "USDC",
            "tid": 99999
        }"#;

        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();
        let event = fill_to_account_event(&fill, &default_dex()).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidPerp);
        match event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.instrument.as_ref(), "BTC-USDC-PERP");
                assert_eq!(
                    trade.id.0, "99999",
                    "the id is the fill's tid, not its hash"
                );
                assert_eq!(trade.side, Side::Buy);
                assert_eq!(trade.price, dec!(65000.5));
                assert_eq!(trade.quantity, dec!(0.1));
                assert_eq!(trade.fees.fees, dec!(0.65));
                assert_eq!(trade.fees.asset.as_ref(), "USDC");
                assert_eq!(trade.fees.fees_quote, Some(dec!(0.65)));
            }
            _ => panic!("Expected Trade event"),
        }
    }

    #[tokio::test]
    async fn a_builder_deployed_fill_is_named_and_charged_in_its_dex_collateral() {
        // A HIP-3 perpetual settles in its deployer's collateral, which is not always USDC.
        let fill_json = r#"{
            "coin": "flx:TSLA", "side": "B", "px": "250", "sz": "2",
            "time": 1714100000000, "hash": "0xhip3", "startPosition": "0",
            "dir": "Open Long", "closedPnl": "0", "oid": 7, "cloid": null,
            "crossed": true, "fee": "0.2", "feeToken": "USDH", "tid": 77
        }"#;
        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();

        let dexes = perp_dexes::tests::xyz_and_flx().await;
        let AccountEventKind::Trade(trade) = fill_to_account_event(&fill, &dexes).unwrap().kind
        else {
            panic!("Expected Trade event");
        };
        assert_eq!(trade.instrument.as_ref(), "flx:TSLA-USDH-PERP");
        assert_eq!(trade.fees.asset.as_ref(), "USDH");
        assert_eq!(trade.fees.fees, dec!(0.2));
        assert_eq!(
            trade.fees.fees_quote,
            Some(dec!(0.2)),
            "the fee is in the instrument's quote"
        );

        // Without `flx` configured, its collateral is unknown, so the fill is left out.
        assert!(fill_to_account_event(&fill, &default_dex()).is_none());
    }

    #[test]
    fn perp_fill_fees_reads_the_fee_token_and_falls_back_to_the_collateral() {
        assert_eq!(
            perp_fill_fees(Some("USDT0"), dec!(1.5), "USDC"),
            AssetFees::new(AssetNameExchange::from("USDT0"), dec!(1.5), None)
        );
        assert_eq!(
            perp_fill_fees(Some("USDC"), dec!(1.5), "USDC"),
            AssetFees::new(AssetNameExchange::from("USDC"), dec!(1.5), Some(dec!(1.5)))
        );
        assert_eq!(
            perp_fill_fees(None, dec!(1.5), "USDH"),
            AssetFees::new(AssetNameExchange::from("USDH"), dec!(1.5), Some(dec!(1.5))),
            "an absent feeToken is the DEX's collateral"
        );
    }

    #[test]
    fn test_fill_to_account_event_sell() {
        let fill_json = r#"{
            "coin": "ETH",
            "side": "A",
            "px": "3200",
            "sz": "1.5",
            "time": 1714100000000,
            "hash": "0xdef456",
            "startPosition": "1.5",
            "dir": "Close Long",
            "closedPnl": "150.0",
            "oid": 12346,
            "cloid": null,
            "crossed": true,
            "fee": "4.8",
            "feeToken": "USDC",
            "tid": 100000
        }"#;

        let fill: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(fill_json).unwrap();
        let event = fill_to_account_event(&fill, &default_dex()).unwrap();

        match event.kind {
            AccountEventKind::Trade(trade) => {
                assert_eq!(trade.instrument.as_ref(), "ETH-USDC-PERP");
                assert_eq!(trade.side, Side::Sell);
                assert_eq!(trade.price, dec!(3200));
                assert_eq!(trade.quantity, dec!(1.5));
            }
            _ => panic!("Expected Trade event"),
        }
    }

    /// Build a perp `TradeInfo` sharing one transaction hash with its siblings.
    fn sweep_fill(tid: u64, px: &str) -> hyperliquid_rust_sdk::TradeInfo {
        let json = format!(
            r#"{{
                "coin": "BTC", "side": "B", "px": "{px}", "sz": "0.1",
                "time": 1714100000000, "hash": "0xonesweep", "startPosition": "0",
                "dir": "Open Long", "closedPnl": "0", "oid": 4242, "cloid": null,
                "crossed": true, "fee": "0.65", "feeToken": "USDC", "tid": {tid}
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn a_trade_id_identifies_the_fill_not_the_transaction() {
        // One aggressive order sweeping two resting levels: same `hash`, same `oid`, two fills.
        // Deriving the id from `hash` gave both the same `TradeId`, so anything reconciling on it
        // treated them as one fill and dropped the second -- understating filled quantity and fees.
        let first = fill_to_account_event(&sweep_fill(111, "65000.5"), &default_dex()).unwrap();
        let second = fill_to_account_event(&sweep_fill(222, "65001.0"), &default_dex()).unwrap();

        let (AccountEventKind::Trade(first), AccountEventKind::Trade(second)) =
            (first.kind, second.kind)
        else {
            panic!("Expected Trade events");
        };

        assert_eq!(first.order_id, second.order_id, "one order produced both");
        assert_ne!(first.id, second.id, "but they are distinct fills");
        assert_eq!(first.id.0, "111");
        assert_eq!(second.id.0, "222");
    }

    #[test]
    fn a_replayed_fill_is_delivered_once() {
        // The `userFills` subscription opens with a snapshot of recent fills, and the SDK
        // resubscribes on reconnect -- so the same fill arrives again on every reconnect.
        let cache = new_dedup_cache();
        let event = fill_to_account_event(&sweep_fill(111, "65000.5"), &default_dex()).unwrap();
        let replay = fill_to_account_event(&sweep_fill(111, "65000.5"), &default_dex()).unwrap();

        assert!(!is_duplicate(&cache, dedup_key_from_event(&event).unwrap()));
        assert!(is_duplicate(&cache, dedup_key_from_event(&replay).unwrap()));
    }

    #[test]
    fn both_fills_of_one_sweep_survive_dedup() {
        // The guard against the fix and the dedup fighting each other: two fills of one sweep are
        // not duplicates of one another, and dedup must not collapse what the id now separates.
        let cache = new_dedup_cache();
        let first = fill_to_account_event(&sweep_fill(111, "65000.5"), &default_dex()).unwrap();
        let second = fill_to_account_event(&sweep_fill(222, "65001.0"), &default_dex()).unwrap();

        assert!(!is_duplicate(&cache, dedup_key_from_event(&first).unwrap()));
        assert!(!is_duplicate(
            &cache,
            dedup_key_from_event(&second).unwrap()
        ));
    }

    #[test]
    fn one_tid_on_two_instruments_is_two_fills() {
        // Hyperliquid documents `tid` as qualified by coin rather than globally unique, which is
        // why the dedup key carries the instrument.
        let cache = new_dedup_cache();
        let btc = fill_to_account_event(&sweep_fill(111, "65000.5"), &default_dex()).unwrap();

        let eth_json = r#"{
            "coin": "ETH", "side": "B", "px": "3200", "sz": "1.5",
            "time": 1714100000000, "hash": "0xother", "startPosition": "0",
            "dir": "Open Long", "closedPnl": "0", "oid": 9, "cloid": null,
            "crossed": true, "fee": "4.8", "feeToken": "USDC", "tid": 111
        }"#;
        let eth: hyperliquid_rust_sdk::TradeInfo = serde_json::from_str(eth_json).unwrap();
        let eth = fill_to_account_event(&eth, &default_dex()).unwrap();

        assert!(!is_duplicate(&cache, dedup_key_from_event(&btc).unwrap()));
        assert!(!is_duplicate(&cache, dedup_key_from_event(&eth).unwrap()));
    }

    #[test]
    fn test_order_update_to_account_event_open() {
        let update_json = r#"{
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "64000",
                "sz": "0.5",
                "oid": 12345,
                "timestamp": 1714100000000,
                "origSz": "0.5",
                "cloid": null
            },
            "status": "open",
            "statusTimestamp": 1714100000000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update, &default_dex()).unwrap();

        assert_eq!(event.exchange, ExchangeId::HyperliquidPerp);
        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.key.instrument.as_ref(), "BTC-USDC-PERP");
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.price, Some(dec!(64000)));
                assert_eq!(order.quantity, dec!(0.5));
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Active(_)
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_to_account_event_filled() {
        let update_json = r#"{
            "order": {
                "coin": "ETH",
                "side": "A",
                "limitPx": "3250",
                "sz": "0",
                "oid": 12346,
                "timestamp": 1714100000000,
                "origSz": "2.0",
                "cloid": null
            },
            "status": "filled",
            "statusTimestamp": 1714100001000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update, &default_dex()).unwrap();

        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.side, Side::Sell);
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Inactive(
                        crate::order::state::InactiveOrderState::FullyFilled(_)
                    )
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_to_account_event_cancelled() {
        let update_json = r#"{
            "order": {
                "coin": "SOL",
                "side": "B",
                "limitPx": "150",
                "sz": "10",
                "oid": 12347,
                "timestamp": 1714100000000,
                "origSz": "10",
                "cloid": null
            },
            "status": "canceled",
            "statusTimestamp": 1714100002000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update, &default_dex()).unwrap();

        match event.kind {
            AccountEventKind::OrderSnapshot(Snapshot(order)) => {
                assert_eq!(order.key.instrument.as_ref(), "SOL-USDC-PERP");
                assert!(matches!(
                    order.state,
                    crate::order::state::OrderState::Inactive(
                        crate::order::state::InactiveOrderState::Cancelled(_)
                    )
                ));
            }
            _ => panic!("Expected OrderSnapshot event"),
        }
    }

    #[test]
    fn test_order_update_unknown_status_returns_none() {
        let update_json = r#"{
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "64000",
                "sz": "0.5",
                "oid": 12345,
                "timestamp": 1714100000000,
                "origSz": "0.5",
                "cloid": null
            },
            "status": "unknown_status",
            "statusTimestamp": 1714100000000
        }"#;

        let update: hyperliquid_rust_sdk::OrderUpdate = serde_json::from_str(update_json).unwrap();
        let event = order_update_to_account_event(&update, &default_dex());
        assert!(event.is_none());
    }
}
