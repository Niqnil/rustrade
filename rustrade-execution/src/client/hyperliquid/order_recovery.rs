//! How a Hyperliquid client learns how the orders it held as live ended while its account stream
//! was disconnected, on the venue-neutral machinery of [`crate::client::order_recovery`].
//!
//! The SDK reconnects the account stream's socket itself and resubscribes, and says so only in
//! passing: it sends [`Message::NoData`](hyperliquid_rust_sdk::Message::NoData) to every
//! subscription when the socket drops, and the venue opens each resubscribed `userFills` with a
//! snapshot of recent fills. `orderUpdates` opens with no snapshot, so an order that ended while the
//! socket was down is never reported on it. [`ReconnectWatch`] reads a drop followed by a fills
//! snapshot as a reconnect, once the snapshot's fills have been sent on, and wakes the task
//! [`spawn_order_checks`] starts. That task lists the open orders once, with `openOrders`, and asks
//! `orderStatus` about each order held as live that the listing no longer shows.

use super::common::{OpenOrder, cid_to_cloid, info, order_update_to_order, record_cid};
use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    client::order_recovery::{
        KnownLiveOrders, NoPendingFills, OpenListing, OrderLookup, SharedKnownLiveOrders,
        UncheckedOrders, recover_ended_orders,
    },
    error::UnindexedClientError,
    order::{
        Order, UnindexedOrderKey,
        id::ClientOrderId,
        state::{ActiveOrderState, Open, OrderState},
    },
};
use ethers::types::H160;
use fnv::FnvHashSet;
use hyperliquid_rust_sdk::{InfoClient, OrderUpdate};
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use smol_str::format_smolstr;
use std::{future::Future, sync::Arc};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

/// What `orderStatus` answers.
#[derive(Debug, serde::Deserialize)]
struct OrderStatusResponse {
    /// `order` when the venue knows the order asked about, `unknownOid` when it does not.
    status: String,
    /// The order's record, present with `order`.
    ///
    /// Parsed as the SDK's [`OrderUpdate`], the shape `orderUpdates` sends, rather than as its
    /// `OrderInfo`, which requires a `tif` the venue sends as `null` for a trigger order.
    #[serde(default)]
    order: Option<OrderUpdate>,
}

/// How `orderStatus` is asked about the order under a client order id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusQuery {
    /// By the cloid of an order placed through this client.
    Cloid(Uuid),
    /// By the venue id of an order placed without a cloid, which is reported under it
    /// ([`record_cid`]).
    Oid(u64),
}

impl StatusQuery {
    /// How to ask about the order under `cid`, or `None` for an id no Hyperliquid order is reported
    /// under.
    fn of(cid: &ClientOrderId) -> Option<Self> {
        if let Some(cloid) = cid_to_cloid(cid) {
            return Some(Self::Cloid(cloid));
        }
        // Only an oid's own spelling: `parse` also takes `+7` and `007`, which no order is
        // reported under.
        let oid = cid.0.parse::<u64>().ok()?;
        (format_smolstr!("{oid}") == cid.0).then_some(Self::Oid(oid))
    }

    /// The `orderStatus` request body for the order of the wallet at `address`.
    fn body(self, address: H160) -> String {
        // `{:?}` on H160 renders the full 0x-prefixed form the endpoint expects; see `user_info`.
        match self {
            Self::Cloid(cloid) => format!(
                r#"{{"type":"orderStatus","user":"{address:?}","oid":"0x{}"}}"#,
                cloid.simple()
            ),
            Self::Oid(oid) => {
                format!(r#"{{"type":"orderStatus","user":"{address:?}","oid":{oid}}}"#)
            }
        }
    }
}

/// The record `orderStatus` (weight 2) holds of the order under `cid` in the wallet at `address`,
/// or `None` when it knows no order under that id.
///
/// An id that is neither a cloid nor an oid is not asked about. A record found by an oid that
/// carries a cloid is `None` too: this client reports that order under its cloid, so the oid does
/// not name it.
///
/// # Errors
///
/// When the request fails, or the answer does not parse or is neither an order nor `unknownOid`.
pub(super) async fn fetch_order_record(
    info_client: &InfoClient,
    address: H160,
    cid: &ClientOrderId,
) -> Result<Option<OrderUpdate>, UnindexedClientError> {
    let Some(query) = StatusQuery::of(cid) else {
        debug!(%cid, "Not a Hyperliquid cloid or oid, so no order is reported under it");
        return Ok(None);
    };
    let response: OrderStatusResponse =
        info(info_client, "orderStatus", query.body(address)).await?;
    match (response.status.as_str(), response.order) {
        ("order", Some(record)) => {
            let names = record_cid(record.order.cloid.as_deref(), record.order.oid)
                .is_some_and(|reported| reported == *cid);
            if !names {
                debug!(%cid, oid = record.order.oid, cloid = ?record.order.cloid, "Hyperliquid reports this order under another id");
            }
            Ok(names.then_some(record))
        }
        ("unknownOid", _) => Ok(None),
        (status, _) => Err(UnindexedClientError::Internal(format!(
            "Hyperliquid orderStatus answered status {status:?} with no order"
        ))),
    }
}

/// What the order `record` says of the order under `key`, the key it was asked about.
///
/// `instrument` is the instrument the record's coin names, `None` for a market this client does not
/// trade. An order on another instrument than the key's is [`OrderLookup::Unknown`]. One live, or
/// whose record does not convert (logged at `warn`), is [`OrderLookup::NotEnded`], so it is asked
/// about again rather than retired on a guess. One that ended carries `key`, so a strategy the
/// venue does not record survives.
pub(super) fn lookup_from_record(
    key: UnindexedOrderKey,
    record: &OrderUpdate,
    instrument: Option<InstrumentNameExchange>,
) -> OrderLookup {
    if instrument.as_ref() != Some(&key.instrument) {
        debug!(cid = %key.cid, asked = %key.instrument, coin = %record.order.coin, "Hyperliquid order is on another instrument than asked");
        return OrderLookup::Unknown;
    }
    let Some(Order {
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state,
        ..
    }) = order_update_to_order(record, key.exchange, key.instrument.clone())
    else {
        return OrderLookup::NotEnded;
    };
    match state {
        OrderState::Active(_) => OrderLookup::NotEnded,
        OrderState::Inactive(state) => OrderLookup::Ended(Box::new(Order {
            key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        })),
    }
}

/// The client order ids of the open-order `rows` on `instruments`, given by `to_instrument` the
/// instrument each row's coin names (`None` for a market the client does not trade).
///
/// # Errors
///
/// The first error `to_instrument` returns.
pub(super) fn listed_cids(
    rows: &[OpenOrder],
    instruments: &[InstrumentNameExchange],
    mut to_instrument: impl FnMut(&str) -> Result<Option<InstrumentNameExchange>, UnindexedClientError>,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let wanted: FnvHashSet<&InstrumentNameExchange> = instruments.iter().collect();
    let mut listed = FnvHashSet::default();
    for row in rows {
        let Some(instrument) = to_instrument(&row.coin)? else {
            continue;
        };
        if !wanted.contains(&instrument) {
            continue;
        }
        // A row whose cloid is malformed names no order of this client's (`record_cid` warns).
        listed.extend(record_cid(row.cloid.as_deref(), row.oid));
    }
    Ok(listed)
}

/// Hold each open order in `snapshot` as live, for a reconnect to ask about if the stream misses
/// its end.
pub(super) fn remember_snapshot(
    known: &SharedKnownLiveOrders,
    snapshot: &UnindexedAccountSnapshot,
) {
    let mut known = known.lock();
    for order in snapshot
        .instruments
        .iter()
        .flat_map(|instrument| &instrument.orders)
    {
        if let OrderState::Active(ActiveOrderState::Open(open)) = &order.state {
            known.live(&order.key, order.quantity, open);
        }
    }
}

/// Hold each of `orders` as live, for a reconnect to ask about if the stream misses its end.
pub(super) fn remember_open<'a>(
    known: &SharedKnownLiveOrders,
    orders: impl IntoIterator<Item = &'a Order<ExchangeId, InstrumentNameExchange, Open>>,
) {
    let mut known = known.lock();
    for order in orders {
        known.live(&order.key, order.quantity, &order.state);
    }
}

/// Send `event` on the account stream, having learnt from it what [`KnownLiveOrders`] learns.
///
/// The set's lock is held across the send, as the order check holds it across its own, so an order
/// this stream reports ending and the check reach the consumer in the order they were decided, and
/// the order is reported once.
///
/// Returns whether it was sent, which it is unless the consumer has gone.
pub(super) fn send_observed(
    known: &SharedKnownLiveOrders,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    event: UnindexedAccountEvent,
) -> bool {
    let known = KnownLiveOrders::observes(&event.kind).then(|| {
        let mut known = known.lock();
        known.observe(&event);
        known
    });
    let sent = tx.send(event).is_ok();
    drop(known);
    sent
}

/// Reads the SDK's reconnects off the account stream's `userFills` subscription, and wakes the
/// order check after each (see the [module docs](self)).
#[derive(Debug)]
pub(super) struct ReconnectWatch {
    /// Whether the socket has dropped since the last snapshot.
    dropped: bool,
    reconnected: Arc<Notify>,
}

impl ReconnectWatch {
    /// A watch that wakes `reconnected` after each reconnect.
    pub(super) fn new(reconnected: Arc<Notify>) -> Self {
        Self {
            dropped: false,
            reconnected,
        }
    }

    /// The socket dropped: the SDK sent `NoData`.
    pub(super) fn dropped(&mut self) {
        self.dropped = true;
    }

    /// A `userFills` message, a snapshot when `is_snapshot` says so, has been sent on. A snapshot
    /// after a drop opens the resubscription, so the socket is back and the fills the snapshot
    /// carries have reached the stream: the order check may run.
    pub(super) fn fills_sent(&mut self, is_snapshot: Option<bool>) {
        if self.dropped && is_snapshot == Some(true) {
            self.dropped = false;
            self.reconnected.notify_one();
        }
    }
}

/// Start the task that checks how the orders held as live ended while the account stream was
/// disconnected, each time `reconnected` is woken, until `cancel` is cancelled.
///
/// Each check is [`recover_ended_orders`] over every instrument with an order held: one listing of
/// them all by `list_open`, and `lookup` for each order held that the listing no longer shows. The
/// fills the venue's snapshot redelivered have been sent before the task is woken, so it has no
/// fills to wait for. A failed check is retried while connected, on the backoff that function
/// documents, and a reconnect does not bring a retry forward.
pub(super) fn spawn_order_checks<L, LFut, Q, QFut>(
    exchange: ExchangeId,
    known: SharedKnownLiveOrders,
    reconnected: Arc<Notify>,
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
    list_open: L,
    lookup: Q,
) where
    L: Fn(Vec<InstrumentNameExchange>) -> LFut + Send + Sync + 'static,
    LFut: Future<Output = Result<FnvHashSet<ClientOrderId>, UnindexedClientError>> + Send,
    Q: Fn(UnindexedOrderKey) -> QFut + Send + Sync + 'static,
    QFut: Future<Output = Result<OrderLookup, UnindexedClientError>> + Send,
{
    tokio::spawn(async move {
        let mut unchecked = UncheckedOrders::default();
        loop {
            let due = unchecked.next_due(&NoPendingFills);
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = reconnected.notified() => {
                    let held = known.lock().instruments();
                    unchecked.open(held);
                }
                // `select!` builds every branch's future, disabled or not, so the fallback is only
                // never polled.
                () = tokio::time::sleep_until(due.unwrap_or_else(tokio::time::Instant::now)),
                    if due.is_some() => {}
            }
            // Dropping a check part-way loses nothing: each lookup and each listing's instruments
            // are settled as soon as they end.
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = recover_ended_orders(
                    exchange,
                    &known,
                    &mut unchecked,
                    &NoPendingFills,
                    &tx,
                    OpenListing::Batched,
                    &list_open,
                    &lookup,
                ) => {}
            }
        }
    });
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
pub(super) mod tests {
    use super::super::common::info_tests::info_client_against;
    use super::*;
    use crate::{
        AccountEventKind,
        order::{
            OrderKey,
            id::{OrderId, StrategyId, VenueOrderId},
            state::InactiveOrderState,
        },
    };
    use futures::FutureExt as _;
    use rust_decimal_macros::dec;
    use rustrade_integration::collection::snapshot::Snapshot;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    pub(in crate::client::hyperliquid) const CID: &str = "873ce360-db32-4c79-9534-57fc5b02fbe1";
    pub(in crate::client::hyperliquid) const CLOID: &str = "0x873ce360db324c79953457fc5b02fbe1";

    fn btc() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTC-USD-PERP")
    }

    fn key(cid: &str) -> UnindexedOrderKey {
        OrderKey {
            exchange: ExchangeId::HyperliquidPerp,
            instrument: btc(),
            strategy: StrategyId::new("strategy"),
            cid: ClientOrderId::new(cid),
        }
    }

    /// An order record as `orderStatus` returns it, with the fields the venue sends beyond those
    /// the SDK's type models, and the `null` time in force of a trigger order.
    pub(in crate::client::hyperliquid) fn record_json(
        coin: &str,
        status: &str,
        sz: &str,
        cloid: Option<&str>,
    ) -> serde_json::Value {
        serde_json::json!({
            "order": {
                "coin": coin, "side": "B", "limitPx": "60000.0", "sz": sz, "oid": 42,
                "timestamp": 1_790_000_000_000_u64, "triggerCondition": "N/A", "isTrigger": false,
                "triggerPx": "0.0", "children": [], "isPositionTpsl": false, "reduceOnly": false,
                "orderType": "Limit", "origSz": "0.01", "tif": null, "cloid": cloid
            },
            "status": status,
            "statusTimestamp": 1_790_000_001_000_u64
        })
    }

    fn record(coin: &str, status: &str, sz: &str) -> OrderUpdate {
        serde_json::from_value(record_json(coin, status, sz, Some(CLOID))).unwrap()
    }

    /// A server answering every info request with `body`.
    async fn serve(body: serde_json::Value) -> (MockServer, InfoClient) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        let client = info_client_against(server.uri()).await;
        (server, client)
    }

    #[test]
    fn an_order_is_asked_about_by_its_cloid_or_its_oid() {
        let cloid = StatusQuery::of(&ClientOrderId::new(CID)).unwrap();
        assert_eq!(cloid, StatusQuery::Cloid(Uuid::try_parse(CID).unwrap()));
        assert!(
            cloid
                .body(H160::zero())
                .contains(&format!(r#""oid":"{CLOID}""#))
        );

        let oid = StatusQuery::of(&ClientOrderId::new("60908759016")).unwrap();
        assert_eq!(oid, StatusQuery::Oid(60_908_759_016));
        assert!(oid.body(H160::zero()).contains(r#""oid":60908759016}"#));
    }

    #[test]
    fn an_id_no_order_is_reported_under_is_not_asked_about() {
        for cid in [
            "+7",
            "007",
            "",
            "not-an-id",
            "873CE360-DB32-4C79-9534-57FC5B02FBE1",
            "18446744073709551616",
        ] {
            assert_eq!(StatusQuery::of(&ClientOrderId::new(cid)), None, "{cid:?}");
        }
    }

    #[tokio::test]
    async fn a_known_order_is_found_by_its_cloid() {
        let (server, client) = serve(serde_json::json!({
            "status": "order",
            "order": record_json("BTC", "canceled", "0.01", Some(CLOID)),
        }))
        .await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new(CID))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(found.status, "canceled");
        let requests = server.received_requests().await.unwrap();
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        assert!(body.contains(r#""type":"orderStatus""#), "{body}");
        assert!(body.contains(CLOID), "{body}");
    }

    #[tokio::test]
    async fn an_order_the_venue_does_not_know_is_none() {
        let (_server, client) = serve(serde_json::json!({"status": "unknownOid"})).await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new(CID))
            .await
            .unwrap();

        assert!(found.is_none());
    }

    #[tokio::test]
    async fn an_oid_does_not_name_an_order_that_carries_a_cloid() {
        let (_server, client) = serve(serde_json::json!({
            "status": "order",
            "order": record_json("BTC", "canceled", "0.01", Some(CLOID)),
        }))
        .await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new("42"))
            .await
            .unwrap();

        assert!(found.is_none(), "reported under its cloid, not its oid");
    }

    #[tokio::test]
    async fn an_oid_names_an_order_placed_without_a_cloid() {
        let (server, client) = serve(serde_json::json!({
            "status": "order",
            "order": record_json("BTC", "canceled", "0.01", None),
        }))
        .await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new("42"))
            .await
            .unwrap();

        assert_eq!(found.unwrap().order.oid, 42);
        let requests = server.received_requests().await.unwrap();
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        assert!(body.contains(r#""oid":42}"#), "{body}");
    }

    #[tokio::test]
    async fn an_id_no_order_is_reported_under_sends_no_request() {
        let (server, client) = serve(serde_json::json!({"status": "unknownOid"})).await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new("not-an-id"))
            .await
            .unwrap();

        assert!(found.is_none());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_answer_that_is_neither_an_order_nor_unknown_is_an_error() {
        let (_server, client) = serve(serde_json::json!({"status": "order"})).await;

        let found = fetch_order_record(&client, H160::zero(), &ClientOrderId::new(CID)).await;

        assert!(matches!(found, Err(UnindexedClientError::Internal(_))));
    }

    fn ended(lookup: OrderLookup) -> crate::order::UnindexedInactiveOrder {
        match lookup {
            OrderLookup::Ended(order) => *order,
            other => panic!("expected an ended order, got {other:?}"),
        }
    }

    #[test]
    fn a_cancelled_order_carries_the_key_asked_and_what_filled() {
        let order = ended(lookup_from_record(
            key(CID),
            &record("BTC", "canceled", "0.004"),
            Some(btc()),
        ));

        assert_eq!(order.key, key(CID), "the strategy asked with survives");
        assert_eq!(order.quantity, dec!(0.01));
        let InactiveOrderState::Cancelled(cancelled) = order.state else {
            panic!("expected cancelled, got {:?}", order.state);
        };
        assert_eq!(cancelled.id, OrderId::new("42"));
        assert_eq!(cancelled.filled_quantity, Some(dec!(0.006)));
    }

    #[test]
    fn each_way_an_order_ends_is_read() {
        let state = |status: &str| {
            ended(lookup_from_record(
                key(CID),
                &record("BTC", status, "0"),
                Some(btc()),
            ))
            .state
        };

        assert!(
            matches!(state("filled"), InactiveOrderState::FullyFilled(filled) if filled.filled_quantity == dec!(0.01))
        );
        assert!(matches!(
            state("marginCanceled"),
            InactiveOrderState::Cancelled(_)
        ));
        assert!(matches!(
            state("scheduledCancel"),
            InactiveOrderState::Cancelled(_)
        ));
        assert!(matches!(
            state("tickRejected"),
            InactiveOrderState::OpenFailed(_)
        ));
    }

    #[test]
    fn a_live_or_unreadable_order_has_not_ended() {
        for status in ["open", "triggered", "aStatusAddedLater"] {
            let lookup = lookup_from_record(key(CID), &record("BTC", status, "0.01"), Some(btc()));
            assert!(matches!(lookup, OrderLookup::NotEnded), "{status}");
        }
    }

    #[test]
    fn an_order_on_another_instrument_is_unknown() {
        let eth = InstrumentNameExchange::new("ETH-USD-PERP");
        let elsewhere = lookup_from_record(key(CID), &record("ETH", "canceled", "0"), Some(eth));
        let untraded = lookup_from_record(key(CID), &record("@107", "canceled", "0"), None);

        assert!(matches!(elsewhere, OrderLookup::Unknown));
        assert!(matches!(untraded, OrderLookup::Unknown));
    }

    /// An `openOrders` row.
    pub(in crate::client::hyperliquid) fn row(
        coin: &str,
        oid: u64,
        cloid: Option<&str>,
    ) -> serde_json::Value {
        serde_json::json!({
            "coin": coin, "side": "B", "limitPx": "1.0", "sz": "1.0", "oid": oid,
            "timestamp": 1_790_000_000_000_u64, "origSz": "1.0", "cloid": cloid
        })
    }

    fn open_order_row(coin: &str, oid: u64, cloid: Option<&str>) -> OpenOrder {
        serde_json::from_value(row(coin, oid, cloid)).unwrap()
    }

    #[test]
    fn a_listing_names_the_orders_on_the_instruments_asked_about() {
        let rows = [
            open_order_row("BTC", 1, Some(CLOID)),
            open_order_row("BTC", 2, None),
            open_order_row("BTC", 3, Some("0xnot-a-cloid")),
            open_order_row("ETH", 4, Some("0x00000000000000000000000000000004")),
            open_order_row("@107", 5, None),
        ];
        let to_instrument = |coin: &str| {
            Ok((!coin.starts_with('@'))
                .then(|| InstrumentNameExchange::new(format!("{coin}-USD-PERP"))))
        };

        let listed = listed_cids(&rows, &[btc()], to_instrument).unwrap();

        let expected: FnvHashSet<_> = [ClientOrderId::new(CID), ClientOrderId::new("2")]
            .into_iter()
            .collect();
        assert_eq!(listed, expected);
    }

    #[test]
    fn a_listing_fails_when_a_coin_cannot_be_named() {
        let rows = [open_order_row("@999", 1, None)];

        let listed = listed_cids(&rows, &[btc()], |_| {
            Err(UnindexedClientError::Internal(
                "missing from spotMeta".into(),
            ))
        });

        assert!(listed.is_err());
    }

    fn notified(reconnected: &Notify) -> bool {
        reconnected.notified().now_or_never().is_some()
    }

    #[test]
    fn only_a_fills_snapshot_after_a_drop_is_a_reconnect() {
        let reconnected = Arc::new(Notify::new());
        let mut watch = ReconnectWatch::new(reconnected.clone());

        watch.fills_sent(Some(true));
        assert!(
            !notified(&reconnected),
            "the snapshot opening the first subscription"
        );

        watch.dropped();
        watch.dropped();
        watch.fills_sent(None);
        watch.fills_sent(Some(false));
        assert!(
            !notified(&reconnected),
            "live fills are not a resubscription"
        );

        watch.fills_sent(Some(true));
        assert!(notified(&reconnected));

        watch.fills_sent(Some(true));
        assert!(
            !notified(&reconnected),
            "one reconnect wakes the check once"
        );
    }

    fn open() -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId::new("42")),
            time_exchange: chrono::Utc::now(),
            filled_quantity: dec!(0),
        }
    }

    #[test]
    fn an_order_the_stream_reports_ending_is_no_longer_held() {
        let known = KnownLiveOrders::shared(ExchangeId::HyperliquidPerp);
        known.lock().live(&key(CID), dec!(0.01), &open());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let ended = record("BTC", "canceled", "0.01");
        let event = super::super::common::order_update_to_account_event(
            &ended,
            ExchangeId::HyperliquidPerp,
            btc(),
        )
        .unwrap();

        assert!(send_observed(&known, &tx, event));

        assert!(!known.lock().contains(&ClientOrderId::new(CID)));
        assert!(rx.try_recv().is_ok());
    }

    /// Run the check task with a listing that lists nothing and a lookup that finds each order
    /// cancelled, holding one order on BTC.
    fn spawn_checks() -> (
        SharedKnownLiveOrders,
        Arc<Notify>,
        CancellationToken,
        mpsc::UnboundedReceiver<UnindexedAccountEvent>,
    ) {
        let known = KnownLiveOrders::shared(ExchangeId::HyperliquidPerp);
        known.lock().live(&key(CID), dec!(0.01), &open());
        let reconnected = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        spawn_order_checks(
            ExchangeId::HyperliquidPerp,
            known.clone(),
            reconnected.clone(),
            cancel.clone(),
            tx,
            |_| async { Ok(FnvHashSet::default()) },
            |key: UnindexedOrderKey| async move {
                Ok(lookup_from_record(
                    key,
                    &record("BTC", "canceled", "0.01"),
                    Some(btc()),
                ))
            },
        );
        (known, reconnected, cancel, rx)
    }

    #[tokio::test]
    async fn a_reconnect_reports_how_a_held_order_ended() {
        let (known, reconnected, _cancel, mut rx) = spawn_checks();

        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "nothing is checked before a reconnect"
        );

        reconnected.notify_one();
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();

        let AccountEventKind::OrderSnapshot(Snapshot(order)) = event.kind else {
            panic!("expected an order snapshot, got {:?}", event.kind);
        };
        // A reconnect asks under the strategy venues do not record.
        let held = OrderKey {
            strategy: StrategyId::unknown(),
            ..key(CID)
        };
        assert_eq!(order.key, held);
        assert!(matches!(
            order.state,
            OrderState::Inactive(InactiveOrderState::Cancelled(_))
        ));
        assert!(!known.lock().contains(&ClientOrderId::new(CID)));
    }

    #[tokio::test]
    async fn a_cancelled_task_checks_nothing() {
        let (known, reconnected, cancel, mut rx) = spawn_checks();

        cancel.cancel();
        tokio::task::yield_now().await;
        reconnected.notify_one();
        tokio::task::yield_now().await;

        assert!(
            rx.recv().await.is_none(),
            "the task has ended and dropped its sender"
        );
        assert!(known.lock().contains(&ClientOrderId::new(CID)));
    }
}
