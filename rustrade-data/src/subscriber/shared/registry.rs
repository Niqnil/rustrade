//! Every stream attached to a shared connection, and the routing index over them.
//!
//! Holds no socket, so it is tested directly.

use super::{AttachId, AttachRequest, Batch, Protocol, REATTACH_GRACE};
use fnv::FnvHashMap;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::protocol::websocket::WsMessage;
use tokio::{sync::mpsc, time::Instant};
use tracing::debug;

/// One attached stream's batch, and where its frames go.
#[derive(Debug)]
pub(crate) struct Registration<P: Protocol> {
    pub(crate) exchange: ExchangeId,
    pub(crate) kind: &'static str,
    pub(crate) slots: Vec<P::Slot>,
    pub(crate) batch: P::Batch,
    sink: Sink,
}

#[derive(Debug)]
enum Sink {
    /// Attached: frames go straight to the stream.
    Live(mpsc::UnboundedSender<WsMessage>),
    /// Re-subscribed by a reconnect the stream has not re-attached to yet.
    Held {
        frames: Vec<WsMessage>,
        until: Instant,
    },
    /// Its socket was lost, and nothing has reconnected since.
    Lost { until: Instant },
}

impl<P: Protocol> Registration<P> {
    /// Whether `request` is this registration's stream re-attaching.
    ///
    /// The reconnect wrapper re-initialises a stream with the batch it was built with, so an equal
    /// batch is the same stream — or an exact duplicate of it, which is interchangeable with it.
    fn is_requested_by(&self, request: &AttachRequest<P>) -> bool {
        self.exchange == request.exchange
            && self.kind == request.kind
            && self.slots == request.slots
            && self.batch.is_same_stream(&request.batch)
    }

    fn is_routed(&self) -> bool {
        !matches!(self.sink, Sink::Lost { .. })
    }

    /// When a registration waiting for its stream to re-attach is given up on.
    fn until(&self) -> Option<Instant> {
        match self.sink {
            Sink::Live(_) => None,
            Sink::Held { until, .. } | Sink::Lost { until } => Some(until),
        }
    }

    fn deliver(&mut self, id: AttachId, frame: WsMessage) {
        match &mut self.sink {
            Sink::Live(tx) => {
                if tx.send(frame).is_err() {
                    // Normal for a moment: the stream is gone and its detach is on the way.
                    debug!(
                        exchange = %self.exchange,
                        id,
                        "{} frame for a stream that has already dropped its attachment",
                        P::NAME,
                    );
                }
            }
            Sink::Held { frames, .. } => frames.push(frame),
            Sink::Lost { .. } => {}
        }
    }
}

/// Which registrations hold each slot — equivalently, what the socket holds.
#[derive(Debug)]
struct Index<P: Protocol> {
    holders: FnvHashMap<P::Slot, Vec<AttachId>>,
}

impl<P: Protocol> Default for Index<P> {
    fn default() -> Self {
        Self {
            holders: FnvHashMap::default(),
        }
    }
}

impl<P: Protocol> Index<P> {
    fn link(&mut self, id: AttachId, slots: &[P::Slot]) {
        for slot in slots {
            self.holders.entry(slot.clone()).or_default().push(id);
        }
    }

    /// Remove `id` from `slots`, returning those nothing holds any longer.
    fn unlink(&mut self, id: AttachId, slots: &[P::Slot]) -> Vec<P::Slot> {
        let mut orphaned = Vec::new();

        for slot in slots {
            if let Some(holders) = self.holders.get_mut(slot) {
                holders.retain(|holder| *holder != id);
                if holders.is_empty() {
                    self.holders.remove(slot);
                    orphaned.push(slot.clone());
                }
            }
        }

        orphaned
    }

    fn clear(&mut self) {
        self.holders.clear();
    }
}

/// Every registration on one connection, and the routing index over them.
#[derive(Debug)]
pub(crate) struct Registry<P: Protocol> {
    next_id: AttachId,
    registrations: FnvHashMap<AttachId, Registration<P>>,
    index: Index<P>,
}

impl<P: Protocol> Default for Registry<P> {
    fn default() -> Self {
        Self {
            next_id: 0,
            registrations: FnvHashMap::default(),
            index: Index::default(),
        }
    }
}

impl<P: Protocol> Registry<P> {
    pub(crate) fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    /// How many streams are registered, attached or not.
    pub(crate) fn len(&self) -> usize {
        self.registrations.len()
    }

    pub(crate) fn get(&self, id: AttachId) -> Option<&Registration<P>> {
        self.registrations.get(&id)
    }

    pub(crate) fn get_mut(&mut self, id: AttachId) -> Option<&mut Registration<P>> {
        self.registrations.get_mut(&id)
    }

    /// The registrations holding `slot`, if any does.
    pub(crate) fn holders(&self, slot: &P::Slot) -> Option<&[AttachId]> {
        self.index.holders.get(slot).map(Vec::as_slice)
    }

    /// Whether any registration holds `slot`: whether the socket holds it.
    pub(crate) fn holds(&self, slot: &P::Slot) -> bool {
        self.index.holders.contains_key(slot)
    }

    /// How many slots the socket holds.
    pub(crate) fn held(&self) -> usize {
        self.index.holders.len()
    }

    /// Every slot the socket holds.
    pub(crate) fn slots(&self) -> impl Iterator<Item = &P::Slot> + '_ {
        self.index.holders.keys()
    }

    /// Deliver one frame to registration `id`, or hold it for its stream.
    pub(crate) fn deliver(&mut self, id: AttachId, frame: WsMessage) {
        if let Some(registration) = self.registrations.get_mut(&id) {
            registration.deliver(id, frame);
        }
    }

    /// Deliver one frame to every registration holding `slot` that `accepts` it.
    ///
    /// Returns whether any registration holds `slot`.
    pub(crate) fn route(
        &mut self,
        slot: &P::Slot,
        mut accepts: impl FnMut(&Registration<P>) -> bool,
        frame: &WsMessage,
    ) -> bool {
        let Some(holders) = self.index.holders.get(slot) else {
            return false;
        };

        for id in holders {
            if let Some(registration) = self.registrations.get_mut(id)
                && accepts(registration)
            {
                registration.deliver(*id, frame.clone());
            }
        }

        true
    }

    fn insert(&mut self, registration: Registration<P>) -> AttachId {
        let id = self.next_id;
        self.next_id += 1;

        if registration.is_routed() {
            self.index.link(id, &registration.slots);
        }
        self.registrations.insert(id, registration);

        id
    }

    pub(crate) fn insert_live(
        &mut self,
        request: AttachRequest<P>,
        frames: mpsc::UnboundedSender<WsMessage>,
    ) -> AttachId {
        self.insert(Registration {
            exchange: request.exchange,
            kind: request.kind,
            slots: request.slots,
            batch: request.batch,
            sink: Sink::Live(frames),
        })
    }

    /// Remove a registration in any state, returning the slots nothing holds any longer.
    pub(super) fn remove(&mut self, id: AttachId) -> Vec<P::Slot> {
        match self.registrations.remove(&id) {
            Some(registration) if registration.is_routed() => {
                self.index.unlink(id, &registration.slots)
            }
            _ => Vec::new(),
        }
    }

    /// Remove an attached registration, returning the slots nothing holds any longer.
    ///
    /// `None` for anything else. A registration whose socket was lost is waiting for its stream to
    /// re-attach: the stream's old attachment is dropped as it ends, and that must not remove it.
    pub(super) fn remove_live(&mut self, id: AttachId) -> Option<Vec<P::Slot>> {
        let live = matches!(
            self.registrations
                .get(&id)
                .map(|registration| &registration.sink),
            Some(Sink::Live(_))
        );

        live.then(|| self.remove(id))
    }

    /// Re-attach `request`'s stream to the registration a reconnect is holding frames for.
    ///
    /// The registration moves to a fresh identifier, so a late detach from the stream's previous
    /// attachment cannot remove it.
    pub(super) fn reattach(
        &mut self,
        request: &AttachRequest<P>,
        frames: &mpsc::UnboundedSender<WsMessage>,
    ) -> Option<AttachId> {
        let held = self
            .registrations
            .iter()
            .find(|(_, registration)| {
                matches!(registration.sink, Sink::Held { .. })
                    && registration.is_requested_by(request)
            })
            .map(|(id, _)| *id)?;

        let mut registration = self.registrations.remove(&held)?;
        self.index.unlink(held, &registration.slots);

        if let Sink::Held { frames: kept, .. } =
            std::mem::replace(&mut registration.sink, Sink::Live(frames.clone()))
        {
            for frame in kept {
                // Fails only if the stream has already dropped the receiver it is about to be
                // handed, and then its detach will follow.
                let _ = frames.send(frame);
            }
        }

        Some(self.insert(registration))
    }

    /// Drop the lost registration `request`'s stream left behind: the request supersedes it.
    pub(super) fn forget_lost(&mut self, request: &AttachRequest<P>) {
        let lost = self.registrations.iter().find_map(|(id, registration)| {
            (!registration.is_routed() && registration.is_requested_by(request)).then_some(*id)
        });

        if let Some(id) = lost {
            self.registrations.remove(&id);
        }
    }

    /// Every attached or held registration loses its socket at `now`.
    ///
    /// An attached registration is kept for [`REATTACH_GRACE`] from `now`; one that was already
    /// waiting keeps the deadline it had, so a reconnect that fails does not extend it.
    ///
    /// Returns each held registration whose frames go with it: its stream had not re-attached to
    /// the reconnect that re-subscribed it before that socket was lost as well.
    pub(super) fn lose_all(&mut self, now: Instant) -> Vec<Discarded> {
        let discarded = self
            .registrations
            .values_mut()
            .filter_map(|registration| {
                let until = registration.until().unwrap_or(now + REATTACH_GRACE);

                match std::mem::replace(&mut registration.sink, Sink::Lost { until }) {
                    Sink::Held { frames, .. } if !frames.is_empty() => {
                        Some(Discarded::new(registration, frames.len()))
                    }
                    _ => None,
                }
            })
            .collect();
        self.index.clear();

        discarded
    }

    /// A new socket re-subscribes what the lost one held, and holds frames for each registration
    /// until its stream re-attaches or its deadline passes.
    pub(super) fn hold_lost(&mut self) {
        for (id, registration) in &mut self.registrations {
            let Sink::Lost { until } = registration.sink else {
                continue;
            };

            registration.sink = Sink::Held {
                frames: Vec::new(),
                until,
            };
            registration.batch.reset();
            self.index.link(*id, &registration.slots);
        }
    }

    pub(super) fn next_expiry(&self) -> Option<Instant> {
        self.registrations
            .values()
            .filter_map(Registration::until)
            .min()
    }

    /// Give up on every registration whose stream has not re-attached by `now`.
    pub(super) fn expire(&mut self, now: Instant) -> Expired<P> {
        let expired = self
            .registrations
            .iter()
            .filter(|(_, registration)| registration.until().is_some_and(|until| until <= now))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();

        let mut outcome = Expired {
            discarded: Vec::new(),
            forgotten: Vec::new(),
            orphaned: Vec::new(),
        };

        for id in expired {
            let Some(registration) = self.registrations.remove(&id) else {
                continue;
            };

            match &registration.sink {
                Sink::Held { frames, .. } => {
                    outcome
                        .orphaned
                        .extend(self.index.unlink(id, &registration.slots));
                    outcome
                        .discarded
                        .push(Discarded::new(&registration, frames.len()));
                }
                Sink::Lost { .. } => outcome.forgotten.push(Discarded::new(&registration, 0)),
                Sink::Live(_) => {}
            }
        }

        outcome
    }
}

/// What an expiry gave up on.
#[derive(Debug)]
pub(super) struct Expired<P: Protocol> {
    /// Registrations a reconnect held frames for.
    pub(super) discarded: Vec<Discarded>,
    /// Registrations that lost their socket and saw no reconnect, so nothing was held for them.
    pub(super) forgotten: Vec<Discarded>,
    /// The slots nothing holds any longer.
    pub(super) orphaned: Vec<P::Slot>,
}

/// A registration given up on, for the log line that reports it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Discarded {
    pub(super) exchange: ExchangeId,
    pub(super) kind: &'static str,
    pub(super) subscriptions: usize,
    /// The frames held for it, which its stream never receives.
    pub(super) frames: usize,
}

impl Discarded {
    fn new<P: Protocol>(registration: &Registration<P>, frames: usize) -> Self {
        Self {
            exchange: registration.exchange,
            kind: registration.kind,
            subscriptions: registration.slots.len(),
            frames,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscriber::shared::{Frame, Handshake};
    use rustrade_integration::{error::SocketError, protocol::websocket::WebSocket};
    use std::time::Duration;
    use url::Url;

    const TRADES: &str = "public_trades";
    const QUOTES: &str = "quotes";

    /// A protocol with nothing but the registry's needs: slots are symbols, and a batch is a tag
    /// telling two otherwise equal streams apart.
    #[derive(Debug)]
    struct Test;

    #[derive(Debug, PartialEq)]
    struct Tag(u8);

    impl Batch for Tag {
        type Attached = ();

        fn is_same_stream(&self, other: &Self) -> bool {
            self == other
        }

        fn attached(&self) {}

        fn reset(&mut self) {}
    }

    struct Settled;

    impl Handshake<()> for Settled {
        fn is_settled(&self) -> bool {
            true
        }

        fn observe(&mut self, (): ()) {}

        fn timed_out(&self, _: Duration) -> String {
            String::new()
        }
    }

    impl Protocol for Test {
        const NAME: &'static str = "Test";
        const CONNECTION_PER_ENDPOINT: bool = false;
        const REFUSAL_IS_ATOMIC: bool = false;

        type Credentials = ();
        type Slot = &'static str;
        type Batch = Tag;
        type Session = ();
        type Answer = ();
        type Handshake = Settled;

        async fn connect(_: &(), _: &Url) -> Result<(WebSocket, ()), SocketError> {
            Err(SocketError::Subscribe(
                "the registry tests open no socket".to_owned(),
            ))
        }

        fn subscribe(
            _: &mut Registry<Self>,
            _: AttachId,
            _: &[&'static str],
        ) -> (Vec<WsMessage>, Settled) {
            (Vec::new(), Settled)
        }

        fn release(_: &[&'static str]) -> (Vec<WsMessage>, Option<Settled>) {
            (Vec::new(), None)
        }

        fn read(_: &mut Registry<Self>, _: &WsMessage) -> Frame<()> {
            Frame::Answers(Vec::new())
        }
    }

    fn request(
        exchange: ExchangeId,
        kind: &'static str,
        slots: &[&'static str],
        tag: u8,
    ) -> AttachRequest<Test> {
        AttachRequest {
            exchange,
            url: Url::parse("ws://127.0.0.1:1").unwrap(),
            kind,
            slots: slots.to_vec(),
            timeout: Duration::from_secs(1),
            batch: Tag(tag),
        }
    }

    fn trades(slots: &[&'static str]) -> AttachRequest<Test> {
        request(ExchangeId::AlpacaIex, TRADES, slots, 0)
    }

    fn live(
        registry: &mut Registry<Test>,
        request: AttachRequest<Test>,
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (registry.insert_live(request, tx), rx)
    }

    fn frame(n: u8) -> WsMessage {
        WsMessage::text(n.to_string())
    }

    fn route(registry: &mut Registry<Test>, slot: &'static str, frame: &WsMessage) -> bool {
        registry.route(&slot, |_| true, frame)
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<WsMessage>) -> Vec<WsMessage> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    #[test]
    fn a_slot_held_by_two_streams_is_held_once_and_reaches_every_holder_that_accepts() {
        let mut registry = Registry::default();
        let (first, mut first_rx) = live(&mut registry, trades(&["AAPL"]));
        let (_, mut second_rx) = live(&mut registry, trades(&["AAPL", "MSFT"]));

        assert_eq!(registry.held(), 2);
        assert!(registry.holds(&"AAPL"));

        assert!(registry.route(
            &"AAPL",
            |registration| registration.slots.len() == 1,
            &frame(1)
        ));
        assert!(route(&mut registry, "AAPL", &frame(2)));
        assert!(
            !route(&mut registry, "TSLA", &frame(3)),
            "no stream holds it"
        );

        assert_eq!(drain(&mut first_rx), [frame(1), frame(2)]);
        assert_eq!(drain(&mut second_rx), [frame(2)]);
        assert_eq!(registry.holders(&"AAPL").map(<[_]>::len), Some(2));
        assert!(registry.get(first).is_some());
    }

    #[test]
    fn the_last_detach_orphans_a_slot_and_an_earlier_one_does_not() {
        let mut registry = Registry::default();
        let (first, _first_rx) = live(&mut registry, trades(&["AAPL", "MSFT"]));
        let (second, _second_rx) = live(&mut registry, trades(&["AAPL"]));

        assert_eq!(
            registry.remove_live(first),
            Some(vec!["MSFT"]),
            "a slot another stream holds stays subscribed",
        );
        assert_eq!(registry.remove_live(second), Some(vec!["AAPL"]));
        assert!(registry.is_empty());
    }

    /// The stream's old attachment is dropped as the stream ends, which is before it re-attaches.
    /// That detach must not remove the registration the reconnect re-subscribes for it.
    #[test]
    fn a_detach_after_the_socket_is_lost_keeps_the_registration() {
        let mut registry = Registry::default();
        let (id, _rx) = live(&mut registry, trades(&["AAPL"]));

        registry.lose_all(Instant::now());

        assert_eq!(registry.remove_live(id), None);
        assert!(!registry.is_empty());
    }

    #[test]
    fn a_reconnect_holds_frames_until_the_stream_re_attaches_then_delivers_them_in_order() {
        let mut registry = Registry::default();
        let (old, _rx) = live(&mut registry, trades(&["AAPL"]));

        registry.lose_all(Instant::now());
        assert!(
            !route(&mut registry, "AAPL", &frame(0)),
            "a lost socket holds nothing"
        );

        registry.hold_lost();
        route(&mut registry, "AAPL", &frame(1));
        route(&mut registry, "AAPL", &frame(2));

        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = registry.reattach(&trades(&["AAPL"]), &tx).unwrap();

        assert_ne!(
            id, old,
            "a re-attach must not be removable by the old detach"
        );
        assert_eq!(drain(&mut rx), [frame(1), frame(2)]);
        assert_eq!(registry.remove_live(old), None);
        assert_eq!(
            registry.next_expiry(),
            None,
            "an attached stream has no deadline"
        );
    }

    #[test]
    fn a_different_batch_does_not_re_attach_to_a_held_registration() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL"]));
        registry.lose_all(Instant::now());
        registry.hold_lost();

        let (tx, _rx) = mpsc::unbounded_channel();
        for other in [
            trades(&["MSFT"]),
            request(ExchangeId::AlpacaIex, QUOTES, &["AAPL"], 0),
            request(ExchangeId::AlpacaCrypto, TRADES, &["AAPL"], 0),
            request(ExchangeId::AlpacaIex, TRADES, &["AAPL"], 1),
        ] {
            assert!(registry.reattach(&other, &tx).is_none(), "{other:?}");
        }
    }

    #[test]
    fn a_reconnect_supersedes_only_the_lost_registration_its_own_stream_left() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL"]));
        live(&mut registry, trades(&["MSFT"]));
        registry.lose_all(Instant::now());

        registry.forget_lost(&trades(&["AAPL"]));
        registry.hold_lost();

        assert_eq!(registry.len(), 1);
        assert!(registry.holds(&"MSFT"));
        assert!(!registry.holds(&"AAPL"));
    }

    #[test]
    fn a_held_registration_expires_and_releases_what_only_it_held() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL", "MSFT"]));

        let lost = Instant::now();
        let until = lost + REATTACH_GRACE;
        registry.lose_all(lost);
        registry.hold_lost();

        // Another stream, on a slot the held one also holds.
        let (_, _live) = live(
            &mut registry,
            request(ExchangeId::AlpacaIex, QUOTES, &["MSFT"], 0),
        );
        route(&mut registry, "AAPL", &frame(1));

        assert_eq!(registry.next_expiry(), Some(until));
        let early = registry.expire(until - Duration::from_millis(1));
        assert!(early.discarded.is_empty() && early.forgotten.is_empty());

        let expired = registry.expire(until);
        assert_eq!(
            expired.discarded,
            [Discarded {
                exchange: ExchangeId::AlpacaIex,
                kind: TRADES,
                subscriptions: 2,
                frames: 1,
            }]
        );
        assert!(expired.forgotten.is_empty());
        assert_eq!(expired.orphaned, ["AAPL"]);
        assert_eq!(registry.next_expiry(), None);
    }

    /// A second loss inside the grace window takes the frames held for a stream that has not
    /// re-attached yet. They must be reported, as an expiry reports them, and the registration kept
    /// for the next reconnect.
    #[test]
    fn a_loss_before_a_held_stream_re_attaches_reports_the_frames_it_discards() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL"]));
        live(
            &mut registry,
            request(ExchangeId::AlpacaIex, QUOTES, &["MSFT"], 0),
        );

        assert!(
            registry.lose_all(Instant::now()).is_empty(),
            "nothing was held yet"
        );

        registry.hold_lost();
        route(&mut registry, "AAPL", &frame(1));
        route(&mut registry, "AAPL", &frame(2));

        assert_eq!(
            registry.lose_all(Instant::now()),
            [Discarded {
                exchange: ExchangeId::AlpacaIex,
                kind: TRADES,
                subscriptions: 1,
                frames: 2,
            }],
            "a held registration with no frames discards nothing, and is not reported",
        );

        registry.hold_lost();
        let (tx, mut rx) = mpsc::unbounded_channel();
        assert!(registry.reattach(&trades(&["AAPL"]), &tx).is_some());
        assert!(drain(&mut rx).is_empty(), "the discarded frames were held");
    }

    /// A stream dropped while its socket is down never re-attaches, and its detach cannot remove a
    /// lost registration. Past the grace it is forgotten, so no reconnect re-subscribes it.
    #[test]
    fn a_lost_registration_whose_stream_never_re_attaches_is_forgotten_at_the_grace() {
        let mut registry = Registry::default();
        let (dropped, _rx) = live(&mut registry, trades(&["AAPL"]));

        let lost = Instant::now();
        registry.lose_all(lost);
        assert_eq!(registry.remove_live(dropped), None);
        assert_eq!(registry.next_expiry(), Some(lost + REATTACH_GRACE));

        let expired = registry.expire(lost + REATTACH_GRACE);
        assert!(expired.discarded.is_empty());
        assert_eq!(
            expired.forgotten,
            [Discarded {
                exchange: ExchangeId::AlpacaIex,
                kind: TRADES,
                subscriptions: 1,
                frames: 0,
            }]
        );
        assert!(
            expired.orphaned.is_empty(),
            "a lost socket holds nothing to release"
        );
        assert!(registry.is_empty());

        registry.hold_lost();
        assert_eq!(
            registry.held(),
            0,
            "the next reconnect re-subscribes nothing for it"
        );
    }

    /// A reconnect that fails puts every held registration back to waiting. If that renewed its
    /// deadline, a stream that is never coming back would be re-subscribed by every attempt, and a
    /// provider refusing the whole subscribe over its cap would refuse each of them.
    #[test]
    fn a_failed_reconnect_does_not_extend_the_grace() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL"]));

        let lost = Instant::now();
        registry.lose_all(lost);

        for attempt in 1..=3 {
            registry.hold_lost();
            assert_eq!(
                registry.next_expiry(),
                Some(lost + REATTACH_GRACE),
                "held, attempt {attempt}"
            );

            registry.lose_all(lost + Duration::from_secs(10 * attempt));
            assert_eq!(
                registry.next_expiry(),
                Some(lost + REATTACH_GRACE),
                "lost, attempt {attempt}"
            );
        }

        assert_eq!(registry.expire(lost + REATTACH_GRACE).forgotten.len(), 1);
        assert!(registry.is_empty());
    }

    /// A stream that re-attached and later loses its socket again starts a new grace.
    #[test]
    fn a_re_attached_stream_gets_a_new_grace_from_its_next_loss() {
        let mut registry = Registry::default();
        live(&mut registry, trades(&["AAPL"]));

        let first = Instant::now();
        registry.lose_all(first);
        registry.hold_lost();
        let (tx, _rx) = mpsc::unbounded_channel();
        registry.reattach(&trades(&["AAPL"]), &tx).unwrap();

        let second = first + Duration::from_secs(60);
        registry.lose_all(second);

        assert_eq!(registry.next_expiry(), Some(second + REATTACH_GRACE));
    }
}
