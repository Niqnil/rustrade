//! Live WebSocket subscription: authentication, the pre-subscribe guards, and the subscribe flow.

use super::{
    connection::{AttachRequest, LseAttachment, LseConnection},
    market::LseDataset,
    osi,
    resume::{LseResumeState, epoch_seconds},
};
use crate::exchange::lse::transport::api_key_from_env;
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscriber::{
        Subscribed, Subscriber,
        mapper::{SubscriptionMapper, WebSocketSubMapper},
    },
    subscription::{Subscription, SubscriptionKind, SubscriptionMeta},
};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{SinkExt, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smol_str::SmolStr;
use std::{fmt, sync::Arc, time::Duration};
use tracing::{debug, warn};

/// How long to wait for the `authenticated` frame.
///
/// Generous because that frame is not small — it enumerates every symbol the key may subscribe to,
/// which was over eight thousand entries when measured.
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);

/// The API key the WebSocket authenticates with.
///
/// `Debug` is implemented manually to redact the key: subscribers are routinely logged with `?`,
/// and a credential that reaches a log line has effectively been disclosed.
#[derive(Clone)]
pub struct LseCredentials {
    api_key: String,
}

impl fmt::Debug for LseCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LseCredentials")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl LseCredentials {
    /// Construct credentials from an explicit key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }

    /// Construct credentials from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] if the variable is unset or does not hold valid UTF-8.
    /// The message names the variable and never its value; the REST clients read the same variable
    /// through the same helper, so the redaction cannot drift between the two surfaces.
    pub fn from_env() -> Result<Self, SocketError> {
        let api_key = api_key_from_env().map_err(SocketError::Subscribe)?;

        Ok(Self::new(api_key))
    }
}

/// The London Strategic Edge WebSocket subscriber.
///
/// # Why this provider needs a subscriber of its own
/// Authentication is a **message**, not a header, and subscriptions are only accepted after the
/// server answers it — so [`Connector::requests`] alone cannot express the flow.
///
/// The handshake also produces something the guards below need: the `authenticated` frame
/// enumerates every symbol the key may subscribe to. That list is the only defence against this
/// surface's quietest failure — see [`Self::subscribe`].
///
/// # ⚠️ One connection per key: clone the subscriber, do not build a second one
/// The provider allows a key **one** WebSocket, so every stream this subscriber and its clones open
/// shares one socket — whatever the dataset, the kind or the option underlying, and across every
/// `subscribe` call. [`StreamBuilder::subscribe`](crate::streams::builder::StreamBuilder::subscribe)
/// clones the subscriber it is given, so passing one subscriber, or clones of it, to every call is
/// all sharing takes. See [`connection`](super::connection) for how the socket is shared, how a
/// reconnect is coordinated across the streams on it, and what the shared subscription cap means
/// for a batch.
///
/// A subscriber built separately — a second [`LseSubscriber::new`] or [`LseSubscriber::from_env`]
/// for the same key — opens a socket of its own, and the provider refuses it with
/// `TOO_MANY_CONNECTIONS`.
///
/// # Example
/// ```ignore
/// use rustrade_data::exchange::lse::{LseCrypto, live::LseSubscriber};
/// use rustrade_data::streams::Streams;
/// use rustrade_data::subscription::trade::PublicTrades;
/// use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
///
/// let subscriber = LseSubscriber::from_env()?;
///
/// let streams = Streams::<PublicTrades>::builder()
///     .subscribe(subscriber, [(
///         LseCrypto::default(), "btc", "usd", MarketDataInstrumentKind::Spot, PublicTrades,
///     )])
///     .init()
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct LseSubscriber {
    connection: Arc<LseConnection>,
    resume: Option<Arc<LseResumeState>>,
}

impl LseSubscriber {
    /// Construct a subscriber with the provided credentials, without resumption.
    ///
    /// Each call opens a connection of its own when first used; clone the result to share it.
    pub fn new(credentials: LseCredentials) -> Self {
        Self {
            connection: Arc::new(LseConnection::new(credentials)),
            resume: None,
        }
    }

    /// Construct a subscriber from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// See [`LseCredentials::from_env`].
    pub fn from_env() -> Result<Self, SocketError> {
        Ok(Self::new(LseCredentials::from_env()?))
    }

    /// Resume from the last delivered event when a reconnect re-subscribes.
    ///
    /// Every stream opened by this subscriber shares `state`, and the same subscriber instance is
    /// cloned into each reconnect attempt, so the watermark it accumulates survives the
    /// reconnect that consults it.
    ///
    /// # ⚠️ Replay is not free, which is why this is opt-in
    /// A resumed subscription is served its historical window before a single live tick. That
    /// window is large: one hour of a single busy crypto symbol replayed **107,395 ticks** before
    /// going live, and a 24-hour window had not drained after 30 seconds. A consumer that must
    /// stay current will prefer the gap; a consumer recording a continuous tape will prefer the
    /// replay. Choosing between those is the caller's business, so the default is off.
    ///
    /// # ⚠️ The provider retains only 24 hours, and clamps silently
    /// A resume point older than that is **moved forward with no error**. The stream compares what
    /// the provider says it will replay from against what was asked for and warns on a mismatch;
    /// see [`LseMessage::ReplayStarted`](super::tick::LseMessage::ReplayStarted).
    ///
    /// # ⚠️ One state per duplicated subscription
    /// Watermarks are keyed by dataset, subscription **and** subscription kind, so one state serves
    /// any set of streams that differ in any of those three. Two streams that duplicate all three
    /// share one watermark and advance it independently, which resumes the trailing one past events
    /// it never delivered — see [`LseResumeState`] for the obligation in full.
    ///
    /// # ⚠️ The replayed prefix is skipped by position, not by the `replay` flag
    /// The provider re-serves every tick sharing the resume instant, and the ones already delivered
    /// are dropped by counting rather than by trusting each tick's `replay` stamp — a provider that
    /// stopped stamping them would otherwise turn every reconnect into a duplicate flood. The cost
    /// is a narrow one: if the replay under-delivers at that instant, a genuinely *live* tick
    /// arriving there while the count is still open is dropped as though it were one of the missing
    /// duplicates. It requires the whole reconnect to land inside one timestamp quantum of the
    /// resume instant, so it is unlikely rather than impossible, and it is reported by a `warn!`
    /// naming the subscription and the instant rather than passing silently.
    ///
    /// # Streams sharing a symbol share its replay window
    /// Streams on one connection that hold one symbol — two kinds, or two datasets spelling it
    /// alike — hold one provider subscription to it, and the provider opens one replay window per
    /// subscription. A reconnect therefore opens each symbol's window at the **earliest** watermark
    /// among the streams holding it, and every stream drops, silently, the replayed ticks before its
    /// own watermark: it delivered them before the connection was lost. Replayed ticks reach only
    /// the streams that resume that symbol. See [`connection`](super::connection).
    ///
    /// A clone made *before* this call shares the connection but not the state. That is supported —
    /// each stream resumes from its own subscriber's state — but it is rarely what was meant.
    ///
    /// Covers reconnects, not process restarts; see [`LseResumeState`].
    #[must_use]
    pub fn with_resume(mut self, state: Arc<LseResumeState>) -> Self {
        self.resume = Some(state);
        self
    }

    /// The resume state this subscriber shares with the streams it opens, if any.
    pub(super) fn resume_state(&self) -> Option<Arc<LseResumeState>> {
        self.resume.clone()
    }
}

impl Subscriber for LseSubscriber {
    type SubMapper = WebSocketSubMapper;
    type Transport = LseAttachment;

    /// Attach the batch to this subscriber's shared connection — connecting and authenticating
    /// first if nothing is attached yet — check it, then subscribe whatever the connection does not
    /// already hold.
    ///
    /// # ⚠️ Every guard runs before the first subscribe leaves the client, deliberately
    /// This surface confirms a subscription to a symbol it has never heard of: it answers
    /// `subscribed`, never errors, never ticks, **and holds one of the connection's slots** until
    /// it is unsubscribed — which nothing would prompt, since a confirmation is all the client ever
    /// sees. So the requested symbols are checked against the list the `authenticated` frame
    /// supplies *first*, and a batch that fails costs nothing.
    ///
    /// Two checks run over that list:
    /// - **Membership is enforced.** A symbol the provider does not offer fails the batch.
    /// - **The category is only cross-checked when the provider supplies a label this integration
    ///   recognises.** Roughly half the published entries carry no `category` key at all — and some
    ///   carry neither `category` nor `name` — so absence is normal and can never be treated as a
    ///   mismatch. Neither can a label we do not know: only one we recognise as belonging to a
    ///   *different* dataset is evidence of anything.
    ///
    /// The subscription cap is enforced in the same pass, because an over-cap batch is rejected
    /// *anonymously*: the provider's rejection does not name the symbol it refused, leaving no
    /// partial recovery. Failing the whole batch before sending is the only outcome that does not
    /// leave the caller guessing which subscriptions survived.
    ///
    /// # ⚠️ The cap is the connection's, not the batch's
    /// Every stream sharing the connection draws on one subscription cap, so a batch is checked
    /// against what the connection would hold with it: the symbols and underlyings already held by
    /// other streams count, and a symbol one of them already holds costs nothing more. A batch that
    /// fits on its own can therefore be refused here.
    ///
    /// Should the provider reject a subscribe anyway, the batch fails and whatever it sent is
    /// unsubscribed again, so a failed batch still holds no slot.
    ///
    /// # Option contracts subscribe per underlying
    /// On [`LseOptions`](super::LseOptions) one subscribe covers an underlying's whole chain, so the
    /// batch is reduced to its distinct underlyings: that is what is sent, what the cap counts, and
    /// what the provider confirms. Every contract must spell an OSI symbol, checked before
    /// connecting. The offered-symbol check does not apply: the handshake's list does not hold every
    /// underlying that has options (index roots are absent from it), and an underlying with no
    /// options is rejected by the provider **by name** rather than confirmed.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] if the batch is empty, the key is rejected, the handshake
    /// times out, the batch would take the connection over its subscription cap, any requested
    /// symbol is not offered, or — on the options dataset — any contract has no OSI symbol or names
    /// an underlying with no options.
    async fn subscribe<Exchange, Instrument, Kind>(
        &self,
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key, Self::Transport>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to London Strategic Edge WebSocket");

        // Every subscription in a batch shares one `Kind`, and a watermark is filed under the
        // dataset and the kind it was delivered for -- see `LseResumeKey`.
        let Some(kind) = subscriptions
            .first()
            .map(|subscription| subscription.kind.as_str())
        else {
            return Err(SocketError::Subscribe(format!(
                "no subscriptions were given to subscribe to on {exchange}"
            )));
        };

        let markets = requested_markets::<Exchange, Instrument, Kind>(subscriptions);

        // Checked before attaching: a contract with no OSI symbol needs no socket to reject.
        let underlyings = subscribes_per_underlying(exchange)
            .then(|| option_underlyings(exchange, &markets))
            .transpose()?;

        // Only the instrument map is taken from the standard mapper. The subscribe payloads are
        // built by the connection instead, because it alone knows what the socket already holds
        // and which replay window each symbol needs -- `Connector::requests` is a static function
        // with access to neither. Both routes build their payloads with `subscribe_message`, so
        // they cannot drift apart.
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions: _,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        let transport = self
            .connection
            .attach(AttachRequest {
                exchange,
                url,
                kind,
                markets,
                underlyings,
                timeout: Exchange::subscription_timeout(),
                resume: self.resume.clone(),
            })
            .await?;

        debug!(%exchange, "attached to the London Strategic Edge connection");
        Ok(Subscribed {
            transport,
            map: instrument_map,
            // The connection routes every frame for the batch into `transport` from the moment it
            // is registered, so nothing is read ahead of it.
            buffered_websocket_events: Vec::new(),
        })
    }
}

/// Build the subscribe payload for one symbol, optionally resuming from `start`.
///
/// Shared with [`Connector::requests`] so the two routes into this protocol cannot disagree about
/// its shape. That route never resumes — it has no access to per-symbol state — and passes `None`.
///
/// `start` is sent as epoch seconds, the only spelling that can express a sub-second resume point;
/// see [`epoch_seconds`] for why.
pub(super) fn subscribe_message(symbol: &str, start: Option<DateTime<Utc>>) -> WsMessage {
    let payload = match start {
        Some(start) => json!({
            "action": "subscribe",
            "symbol": symbol,
            "start": epoch_seconds(start),
        }),
        None => json!({ "action": "subscribe", "symbol": symbol }),
    };

    WsMessage::text(payload.to_string())
}

/// Build the subscribe payload for every option contract on `underlying`.
///
/// Carries no replay window: whether the provider honours one on this channel is unestablished.
pub(super) fn subscribe_options_message(underlying: &str) -> WsMessage {
    WsMessage::text(json!({ "action": "subscribe_options", "underlying": underlying }).to_string())
}

/// Build the payload releasing one symbol's subscription and its slot.
///
/// Answered by an `unsubscribed` frame, and ticks stop at once.
pub(super) fn unsubscribe_message(symbol: &str) -> WsMessage {
    WsMessage::text(json!({ "action": "unsubscribe", "symbol": symbol }).to_string())
}

/// Build the payload releasing an underlying's option chain and its slot.
///
/// Answered by an `options_unsubscribed` frame.
pub(super) fn unsubscribe_options_message(underlying: &str) -> WsMessage {
    WsMessage::text(
        json!({ "action": "unsubscribe_options", "underlying": underlying }).to_string(),
    )
}

/// Whether `exchange` subscribes per option underlying rather than per symbol.
///
/// The one place that decision is made. The subscriber, the confirmation count, the transformer and
/// the stream all ask it, so they cannot disagree about which dataset fans out.
pub(super) fn subscribes_per_underlying(exchange: ExchangeId) -> bool {
    exchange == ExchangeId::LseOptions
}

/// The distinct underlyings a batch of option contracts subscribes to, in the order requested.
///
/// # Errors
/// Returns [`SocketError::Subscribe`] naming every requested symbol that is not an OSI contract
/// symbol — an instrument that is not an option, or whose strike OSI cannot carry.
pub(super) fn option_underlyings(
    exchange: ExchangeId,
    markets: &[SmolStr],
) -> Result<Vec<SmolStr>, SocketError> {
    let mut seen = FnvHashSet::default();
    let mut underlyings = Vec::<SmolStr>::new();
    let mut unspellable = Vec::new();

    for market in markets {
        match osi::root(market) {
            Some(root) => {
                if seen.insert(root) {
                    underlyings.push(SmolStr::new(root));
                }
            }
            None => unspellable.push(market.as_str()),
        }
    }

    if !unspellable.is_empty() {
        return Err(SocketError::Subscribe(format!(
            "{exchange} subscribes option contracts by OSI symbol (root, YYMMDD, C or P, strike in \
             thousandths as eight digits), and {unspellable:?} have none - each must be an option \
             instrument whose strike is positive, has at most three decimal places and is below \
             100,000",
        )));
    }

    Ok(underlyings)
}

/// The distinct symbols a batch will subscribe to, in the order they were requested.
///
/// Two subscriptions naming one symbol are one slot and one confirmation, and the instrument map
/// is keyed the same way — so sending a payload per *subscription* would over-count against the
/// cap and leave the handshake waiting for a confirmation that never comes.
///
/// The batch is not bounded by the subscription cap: on the options dataset each entry is a
/// *contract*, and one underlying can carry thousands of them into a single slot. Hence the
/// seen-set rather than a linear scan, which would make a large option batch quadratic.
fn requested_markets<Exchange, Instrument, Kind>(
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> Vec<SmolStr>
where
    Exchange: Connector,
    Subscription<Exchange, Instrument, Kind>: Identifier<Exchange::Market>,
{
    let mut seen =
        FnvHashSet::<SmolStr>::with_capacity_and_hasher(subscriptions.len(), Default::default());
    let mut markets = Vec::<SmolStr>::with_capacity(subscriptions.len());

    for subscription in subscriptions {
        let market = Identifier::<Exchange::Market>::id(subscription);
        let symbol = SmolStr::new(market.as_ref());

        if seen.insert(symbol.clone()) {
            markets.push(symbol);
        }
    }

    markets
}

/// Reject a batch that would take the connection past what it may hold.
///
/// `added` counts the slots the batch needs that the connection does not hold yet — distinct
/// symbols, or on the options dataset distinct underlyings — and `held` the slots other streams on
/// the connection already hold. The cap is the connection's, so both count against it.
///
/// # Errors
/// Returns [`SocketError::Subscribe`] if `held + added` exceeds `max_subscriptions`.
pub(super) fn check_subscription_cap(
    exchange: ExchangeId,
    added: usize,
    held: usize,
    max_subscriptions: Option<u32>,
) -> Result<(), SocketError> {
    let Some(max) = max_subscriptions else {
        // Refusing here would break any tier whose cap this integration cannot read, and the
        // provider still rejects an over-subscription observably. Proceed, but say so.
        warn!(
            %exchange,
            "London Strategic Edge reported no subscription cap; the batch cannot be checked \
             before it is sent",
        );
        return Ok(());
    };

    let cap = usize::try_from(max).unwrap_or(usize::MAX);
    if held.saturating_add(added) > cap {
        let unit = if subscribes_per_underlying(exchange) {
            "option underlyings"
        } else {
            "symbols"
        };

        return Err(SocketError::Subscribe(format!(
            "{added} {unit} requested on {exchange} beyond the {held} subscriptions this \
             connection already holds, but it holds at most {cap} - the cap is shared by every \
             stream opened by this subscriber and its clones, and the provider's rejection does \
             not name the subscriptions it refuses, so there is no partial subscription to recover \
             and the batch is rejected before it is sent",
        )));
    }

    Ok(())
}

/// The handshake's symbol list, indexed once per connection for the per-batch guard.
///
/// The list ran to over eight thousand entries when measured, and every batch attached to a
/// connection is checked against it.
#[derive(Debug, Default)]
pub(super) struct OfferedSymbols(FnvHashMap<SmolStr, Option<SmolStr>>);

impl OfferedSymbols {
    /// Index the `authenticated` frame's symbols by spelling, keeping each one's category.
    pub(super) fn new(symbols: Vec<LseSymbol>) -> Self {
        Self(
            symbols
                .into_iter()
                .map(|entry| (entry.symbol, entry.category))
                .collect(),
        )
    }
}

/// Check every requested symbol against the list the handshake published.
///
/// # Errors
/// Returns [`SocketError::Subscribe`] if a symbol is not offered, or if one is offered under a
/// category that contradicts the dataset being subscribed on.
pub(super) fn check_symbols_are_offered(
    exchange: ExchangeId,
    markets: &[SmolStr],
    offered: &OfferedSymbols,
) -> Result<(), SocketError> {
    if offered.0.is_empty() {
        warn!(
            %exchange,
            "London Strategic Edge published no symbol list; a typo'd symbol will be confirmed and \
             then never tick",
        );
        return Ok(());
    }

    let expected = expected_categories(exchange);

    let mut unknown = Vec::new();
    let mut miscategorised = Vec::new();

    for market in markets {
        match offered.0.get(market.as_str()) {
            None => unknown.push(market.as_str()),
            Some(category) => {
                if let Some(category) = category.as_deref()
                    && !expected.is_empty()
                    && !expected.contains(&category)
                    && is_published_category(category)
                {
                    miscategorised.push(format!("{market} is {category}"));
                }
            }
        }
    }

    if !unknown.is_empty() {
        return Err(SocketError::Subscribe(format!(
            "London Strategic Edge does not offer {unknown:?} on its WebSocket - a symbol may be \
             reachable through the catalog or the candle store and still be absent here, and \
             subscribing to one the provider does not offer is CONFIRMED rather than rejected, \
             then never ticks, while holding a subscription slot for the life of the connection",
        )));
    }

    if !miscategorised.is_empty() {
        return Err(SocketError::Subscribe(format!(
            "London Strategic Edge categorises {miscategorised:?}, which does not match {exchange} \
             (expected {expected:?}) - the symbol exists, but on a different dataset than the one \
             being subscribed",
        )));
    }

    Ok(())
}

/// The categories the handshake may report for symbols belonging to `exchange`.
///
/// Empty means this integration has no expectation to check against — the provider publishes no
/// category for the datasets behind that identifier, so any category it did report would be
/// unrecognised rather than wrong.
fn expected_categories(exchange: ExchangeId) -> Vec<&'static str> {
    LseDataset::ALL
        .into_iter()
        .filter(|dataset| dataset.exchange_id() == exchange)
        .filter_map(|dataset| dataset.ws_category())
        .collect()
}

/// Whether `category` is a label this integration knows some dataset by.
///
/// # Why an unrecognised label can never be a contradiction
/// [`expected_categories`] is only as complete as what the provider publishes *today*, and it is
/// complete for no better reason than that the provider has not labelled more. Five datasets stand
/// behind [`ExchangeId::LseCfd`] and only two of them are labelled, so the day the provider begins
/// publishing a category for one of the other three — its published list moved by hundreds of
/// entries in a week when measured — a correctly-requested symbol would read as belonging to a
/// different dataset and fail the whole batch, every other symbol in it included, with no change on
/// our side.
///
/// A label we do not recognise says nothing about which dataset a symbol belongs to. Only one we
/// recognise as belonging somewhere *else* is evidence, and that is the only case this admits.
fn is_published_category(category: &str) -> bool {
    LseDataset::ALL
        .into_iter()
        .any(|dataset| dataset.ws_category() == Some(category))
}

/// Authenticate, and return the frame the provider answers with.
pub(super) async fn authenticate(
    websocket: &mut WebSocket,
    credentials: &LseCredentials,
) -> Result<LseAuthenticated, SocketError> {
    let auth = json!({ "action": "auth", "api_key": credentials.api_key }).to_string();

    websocket
        .send(WsMessage::text(auth))
        .await
        .map_err(|error| SocketError::WebSocket(Box::new(error)))?;

    tokio::time::timeout(AUTH_TIMEOUT, async {
        loop {
            match websocket.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if let Some(result) = read_auth_frame(text.as_str()) {
                        return result;
                    }
                }
                Some(Ok(WsMessage::Binary(bytes))) => {
                    if let Ok(text) = std::str::from_utf8(&bytes)
                        && let Some(result) = read_auth_frame(text)
                    {
                        return result;
                    }
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    return Err(SocketError::Subscribe(format!(
                        "WebSocket closed during London Strategic Edge authentication: {frame:?}"
                    )));
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(SocketError::WebSocket(Box::new(error))),
                None => {
                    return Err(SocketError::Subscribe(
                        "WebSocket closed before London Strategic Edge authentication completed"
                            .to_owned(),
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        SocketError::Subscribe(format!(
            "London Strategic Edge authentication timed out after {AUTH_TIMEOUT:?}"
        ))
    })?
}

/// Classify one frame received while awaiting authentication.
///
/// `None` means "not an answer to the auth request" — the server opens with a `welcome` frame
/// before the key is ever sent, so waiting for a specific frame rather than any frame is what
/// separates being connected from being authenticated.
fn read_auth_frame(text: &str) -> Option<Result<LseAuthenticated, SocketError>> {
    match serde_json::from_str::<LseAuthFrame>(text).ok()? {
        LseAuthFrame::Authenticated(authenticated) => Some(Ok(authenticated)),
        LseAuthFrame::Error { code, message } => Some(Err(SocketError::Subscribe(format!(
            "London Strategic Edge rejected authentication ({}): {}",
            code.as_deref().unwrap_or("unknown"),
            message.as_deref().unwrap_or("no message"),
        )))),
        LseAuthFrame::Other => None,
    }
}

/// A frame that may arrive while awaiting authentication.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LseAuthFrame {
    Authenticated(LseAuthenticated),
    Error {
        #[serde(default)]
        code: Option<SmolStr>,
        #[serde(default)]
        message: Option<SmolStr>,
    },
    /// The `welcome` frame, and anything else this integration does not model.
    #[serde(other)]
    Other,
}

/// The provider's answer to a successful `auth`.
///
/// Its symbol list is what the pre-subscribe guards check against — see
/// [`LseSubscriber::subscribe`].
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LseAuthenticated {
    /// The key's plan, as the provider names it (`registered` on a free key).
    #[serde(default)]
    pub tier: Option<SmolStr>,

    /// Subscriptions this connection may hold, shared between symbols and option underlyings.
    ///
    /// `None` if the provider did not report one, in which case the batch cannot be checked before
    /// it is sent.
    #[serde(default)]
    pub max_subscriptions: Option<u32>,

    /// Every symbol the key may subscribe to.
    ///
    /// # ⚠️ This is not the same population as the catalog or the candle store
    /// A symbol reachable on one of the provider's surfaces need not be reachable on all of them —
    /// at least one series with tens of millions of historical ticks is absent from this list
    /// entirely. Its length also drifts week to week, so nothing should assert a count.
    #[serde(default)]
    pub symbols: Vec<LseSymbol>,
}

impl fmt::Debug for LseAuthenticated {
    /// Summarises the symbol list rather than printing it.
    ///
    /// The list ran to over eight thousand entries when measured, and this type is reachable from
    /// tracing fields — deriving `Debug` would put megabytes into a single log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LseAuthenticated")
            .field("tier", &self.tier)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("symbols", &format_args!("<{} offered>", self.symbols.len()))
            .finish()
    }
}

/// One entry of the handshake's symbol list.
///
/// # ⚠️ Every field but `symbol` may be absent
/// Entries are not uniform: roughly half carry no `category`, and at least one arrives as
/// `{"symbol": "ES.F"}` — missing keys rather than null values. This is why the category check can
/// only reject a *contradiction* and never an absence.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct LseSymbol {
    /// The display symbol, exactly as a subscribe must spell it.
    pub symbol: SmolStr,

    /// The instrument's descriptive name, where the provider publishes one.
    #[serde(default)]
    pub name: Option<SmolStr>,

    /// The dataset the provider files this symbol under, where it publishes one.
    #[serde(default)]
    pub category: Option<SmolStr>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::lse::LseFx;
    use crate::subscription::trade::PublicTrades;
    use rustrade_instrument::instrument::market_data::MarketDataInstrument;
    use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

    fn authenticated(symbols: &[(&str, Option<&str>)], max: Option<u32>) -> LseAuthenticated {
        LseAuthenticated {
            tier: Some("registered".into()),
            max_subscriptions: max,
            symbols: symbols
                .iter()
                .map(|(symbol, category)| LseSymbol {
                    symbol: SmolStr::new(symbol),
                    name: None,
                    category: category.map(SmolStr::new),
                })
                .collect(),
        }
    }

    fn markets(symbols: &[&str]) -> Vec<SmolStr> {
        symbols.iter().map(SmolStr::new).collect()
    }

    fn at(spelling: &str) -> DateTime<Utc> {
        spelling.parse::<DateTime<Utc>>().unwrap()
    }

    /// The microsecond count as a float, so the epoch assertions compare in the float domain and
    /// need no truncating cast of their own.
    fn micros_as_f64(time: DateTime<Utc>) -> f64 {
        time.timestamp_micros() as f64
    }

    #[test]
    fn credentials_do_not_print_the_key() {
        let printed = format!("{:?}", LseCredentials::new("super-secret-key"));

        assert!(!printed.contains("super-secret-key"), "{printed}");
        assert!(printed.contains("REDACTED"), "{printed}");
    }

    #[test]
    fn a_subscribe_payload_names_the_symbol_and_nothing_else() {
        let WsMessage::Text(payload) = subscribe_message("EUR/USD", None) else {
            panic!("expected a text payload");
        };
        let payload: serde_json::Value = serde_json::from_str(payload.as_str()).unwrap();

        assert_eq!(payload, json!({"action": "subscribe", "symbol": "EUR/USD"}));
    }

    /// The resume point has to survive the trip out as a number the provider can filter on at its
    /// own resolution: it parses `start` with a float conversion first, and its datetime fallback
    /// accepts neither a fractional second nor an offset.
    #[test]
    fn a_subscribe_payload_carrying_a_resume_point_sends_it_as_epoch_seconds() {
        let start = at("2026-08-14T10:16:55.161234Z");

        let WsMessage::Text(payload) = subscribe_message("BTC/USD", Some(start)) else {
            panic!("expected a text payload");
        };
        let payload: serde_json::Value = serde_json::from_str(payload.as_str()).unwrap();
        let payload = payload.as_object().unwrap();

        assert_eq!(payload.len(), 3, "{payload:?}");
        assert_eq!(payload["action"], json!("subscribe"));
        assert_eq!(payload["symbol"], json!("BTC/USD"));

        let seconds = payload["start"]
            .as_f64()
            .unwrap_or_else(|| panic!("start must be a JSON number, not {:?}", payload["start"]));
        let drift = seconds * 1_000_000.0 - micros_as_f64(start);
        assert!(drift.abs() < 0.5, "start drifted {drift} microseconds");
    }

    /// Two subscriptions naming one symbol are one slot and one confirmation. Sending two payloads
    /// would leave the validator waiting for a confirmation that never arrives.
    #[test]
    fn repeated_symbols_produce_one_subscribe_each() {
        let subscription = |base: &str| -> Subscription<LseFx, MarketDataInstrument, PublicTrades> {
            Subscription::from((
                LseFx::default(),
                base,
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ))
        };
        let subscriptions = [
            subscription("eur"),
            subscription("gbp"),
            subscription("eur"),
        ];

        assert_eq!(
            requested_markets(&subscriptions),
            markets(&["EUR/USD", "GBP/USD"])
        );
    }

    #[test]
    fn an_options_subscribe_payload_names_the_underlying_and_nothing_else() {
        let WsMessage::Text(payload) = subscribe_options_message("SPY") else {
            panic!("expected a text payload");
        };
        let payload: serde_json::Value = serde_json::from_str(payload.as_str()).unwrap();

        assert_eq!(
            payload,
            json!({"action": "subscribe_options", "underlying": "SPY"})
        );
    }

    #[test]
    fn only_the_options_dataset_subscribes_per_underlying() {
        assert!(subscribes_per_underlying(ExchangeId::LseOptions));

        for exchange in [
            ExchangeId::LseFx,
            ExchangeId::LseCrypto,
            ExchangeId::LseEquities,
            ExchangeId::LseFutures,
            ExchangeId::LseCfd,
        ] {
            assert!(!subscribes_per_underlying(exchange), "{exchange}");
        }
    }

    /// One subscribe covers a whole chain and is confirmed once, however often it is sent, so the
    /// contracts of one underlying must collapse into a single subscription.
    #[test]
    fn contracts_sharing_an_underlying_collapse_into_one_subscription() {
        let requested = markets(&[
            "SPY260930C00700000",
            "QQQ260930P00500000",
            "SPY260930P00650000",
            "SPY261016C00700000",
        ]);

        assert_eq!(
            option_underlyings(ExchangeId::LseOptions, &requested).unwrap(),
            markets(&["SPY", "QQQ"])
        );
    }

    /// Checked before connecting, and every offender is named — not only the first.
    #[test]
    fn a_contract_with_no_osi_symbol_fails_the_batch_naming_it() {
        let requested = markets(&["SPY260930C00700000", "SPY", "SPY (no OSI symbol for spot)"]);

        let error = option_underlyings(ExchangeId::LseOptions, &requested)
            .unwrap_err()
            .to_string();
        assert!(error.contains(r#""SPY""#), "{error}");
        assert!(error.contains("no OSI symbol for spot"), "{error}");
        assert!(!error.contains("SPY260930C00700000"), "{error}");
    }

    /// On the options dataset a slot holds an underlying, so that is what an over-cap rejection
    /// must count.
    #[test]
    fn an_over_cap_options_batch_is_reported_in_underlyings() {
        let requested = markets(&["SPY", "QQQ", "AAPL"]);
        let error = check_subscription_cap(ExchangeId::LseOptions, requested.len(), 0, Some(2))
            .unwrap_err()
            .to_string();

        assert!(error.contains("3 option underlyings"), "{error}");
        assert!(error.contains("at most 2"), "{error}");
    }

    /// The cap is the connection's: slots other streams on it already hold count, so a batch that
    /// fits on its own is refused when the connection it joins is nearly full.
    #[test]
    fn slots_held_by_other_streams_count_against_the_cap() {
        let error = check_subscription_cap(ExchangeId::LseFx, 2, 15, Some(16))
            .unwrap_err()
            .to_string();

        assert!(error.contains("2 symbols"), "{error}");
        assert!(error.contains("already holds"), "{error}");
        assert!(error.contains("shared by every stream"), "{error}");
        assert!(check_subscription_cap(ExchangeId::LseFx, 1, 15, Some(16)).is_ok());
    }

    #[test]
    fn a_batch_within_the_cap_is_accepted() {
        let requested = markets(&["EUR/USD", "GBP/USD"]);
        assert!(check_subscription_cap(ExchangeId::LseFx, requested.len(), 0, Some(2)).is_ok());
    }

    #[test]
    fn a_batch_over_the_cap_is_rejected_before_it_is_sent() {
        let requested = markets(&["EUR/USD", "GBP/USD", "XAU/USD"]);
        let error = check_subscription_cap(ExchangeId::LseFx, requested.len(), 0, Some(2))
            .unwrap_err()
            .to_string();

        assert!(error.contains("at most 2"), "{error}");
    }

    /// Refusing a batch because the cap could not be read would break any tier whose cap this
    /// integration does not recognise, and the provider still rejects an over-subscription itself.
    #[test]
    fn an_unreported_cap_does_not_reject_the_batch() {
        let requested = markets(&["EUR/USD"]);
        assert!(check_subscription_cap(ExchangeId::LseFx, requested.len(), 0, None).is_ok());
    }

    #[test]
    fn an_offered_symbol_passes_the_guard() {
        let frame = authenticated(&[("EUR/USD", Some("Forex"))], Some(16));
        let requested = markets(&["EUR/USD"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseFx,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// The failure this guard exists for: the provider confirms a symbol it does not offer, never
    /// ticks it, and holds the slot for the life of the connection.
    #[test]
    fn a_symbol_the_provider_does_not_offer_fails_the_batch() {
        let frame = authenticated(&[("EUR/USD", Some("Forex"))], Some(16));
        let requested = markets(&["EUR/USD", "NOPE_XYZ"]);

        let error = check_symbols_are_offered(
            ExchangeId::LseFx,
            &requested,
            &OfferedSymbols::new(frame.symbols),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("NOPE_XYZ"), "{error}");
    }

    /// The same symbol the test above rejects, and the list it was checked against removed. There
    /// is then nothing to check, so the guard degrades to a warning rather than failing a batch it
    /// cannot judge — refusing would break every caller the moment the provider stopped publishing
    /// its catalog in the handshake.
    ///
    /// This branch is why the live canary asserts a rejection instead of trusting the guard: pinned
    /// only by this test, the degradation is silent, and a provider that quietly dropped the list
    /// would leave the guard passing everything forever.
    #[test]
    fn an_absent_symbol_list_degrades_to_a_warning_rather_than_failing_the_batch() {
        let frame = authenticated(&[], Some(16));
        let requested = markets(&["NOPE_XYZ"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseFx,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    #[test]
    fn a_contradicting_category_fails_the_batch() {
        let frame = authenticated(&[("AAPL", Some("Stocks"))], Some(16));
        let requested = markets(&["AAPL"]);

        let error = check_symbols_are_offered(
            ExchangeId::LseFx,
            &requested,
            &OfferedSymbols::new(frame.symbols),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("AAPL is Stocks"), "{error}");
    }

    /// The same rule where the labels are *complete*, which is the case a symbol-shaped reading of
    /// "only cross-check what we recognise" would leave out.
    ///
    /// A label is evidence only if some dataset is known by it, whether or not this exchange's own
    /// expectation has gaps. Restricting the leniency to the partially-labelled exchanges would
    /// leave the same defect standing here in a slower form: the provider renaming `Forex` makes
    /// this expectation complete but *stale*, and every correctly-requested `LseFx` batch then
    /// fails on a label that is simply newer than the table.
    #[test]
    fn an_unrecognised_label_is_not_a_contradiction_even_where_the_labels_are_complete() {
        let expected = expected_categories(ExchangeId::LseFx);
        assert_eq!(
            expected,
            ["Forex"],
            "this exchange's expectation is complete, which is the point: {expected:?}"
        );

        let frame = authenticated(&[("EUR/USD", Some("FX Majors"))], Some(16));
        let requested = markets(&["EUR/USD"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseFx,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// Roughly half the published entries carry no category, so absence must never be a mismatch.
    #[test]
    fn a_missing_category_is_not_a_contradiction() {
        let frame = authenticated(&[("EUR/USD", None)], Some(16));
        let requested = markets(&["EUR/USD"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseFx,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// Datasets the provider publishes no category for must not reject the categories it does
    /// publish — there is nothing to compare against.
    #[test]
    fn a_dataset_with_no_published_category_accepts_whatever_is_reported() {
        assert!(expected_categories(ExchangeId::LseFutures).is_empty());

        let frame = authenticated(&[("ES.F", Some("Anything"))], Some(16));
        let requested = markets(&["ES.F"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseFutures,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// Five datasets stand behind `LseCfd` and the provider labels two of them, so its expectation
    /// is partial. A label appearing on one of the other three must not read as "this symbol
    /// belongs to a different dataset" and fail the batch — the published list moved by hundreds of
    /// entries in a week when measured, and this is what a correct subscription would meet.
    #[test]
    fn a_label_no_dataset_is_known_by_is_not_a_contradiction() {
        let expected = expected_categories(ExchangeId::LseCfd);
        assert!(expected.contains(&"Indices"), "{expected:?}");
        assert!(
            !expected.contains(&"Volatility"),
            "the expectation is partial by construction: {expected:?}"
        );

        let frame = authenticated(&[("VIX/USD", Some("Volatility"))], Some(16));
        let requested = markets(&["VIX/USD"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseCfd,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// The other half of the same rule: a label we recognise as belonging elsewhere still fails,
    /// because that is evidence the symbol is on a different dataset.
    #[test]
    fn a_label_belonging_to_another_dataset_still_fails_the_batch() {
        let frame = authenticated(&[("AAPL", Some("Stocks"))], Some(16));
        let requested = markets(&["AAPL"]);

        let error = check_symbols_are_offered(
            ExchangeId::LseCfd,
            &requested,
            &OfferedSymbols::new(frame.symbols),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("AAPL is Stocks"), "{error}");
    }

    /// Equities and ETFs share one identifier, so both categories must satisfy it.
    #[test]
    fn stocks_and_etfs_both_satisfy_the_equities_identifier() {
        let expected = expected_categories(ExchangeId::LseEquities);
        assert!(expected.contains(&"Stocks"), "{expected:?}");
        assert!(expected.contains(&"ETFs"), "{expected:?}");

        let frame = authenticated(&[("AAPL", Some("Stocks")), ("SPY", Some("ETFs"))], Some(16));
        let requested = markets(&["AAPL", "SPY"]);

        assert!(
            check_symbols_are_offered(
                ExchangeId::LseEquities,
                &requested,
                &OfferedSymbols::new(frame.symbols)
            )
            .is_ok()
        );
    }

    /// The server opens with a `welcome` frame before the key is ever sent, so treating any frame
    /// as the answer would report success without authenticating.
    #[test]
    fn the_welcome_frame_is_not_mistaken_for_an_auth_response() {
        let welcome = r#"{"type":"welcome","message":"hello","symbols_available":8516}"#;
        assert!(read_auth_frame(welcome).is_none());
    }

    #[test]
    fn an_auth_response_yields_the_symbol_list_and_the_cap() {
        let input = r#"{"type":"authenticated","tier":"registered","key_type":"main",
            "max_subscriptions":16,"symbols":[
                {"symbol":"BTC/USD","name":"Bitcoin","category":"Crypto"},
                {"symbol":"ES.F"}
            ]}"#;
        let frame = read_auth_frame(input).unwrap().unwrap();

        assert_eq!(frame.tier.as_deref(), Some("registered"));
        assert_eq!(frame.max_subscriptions, Some(16));
        assert_eq!(frame.symbols.len(), 2);
        assert_eq!(frame.symbols[1].symbol, "ES.F");
        assert!(frame.symbols[1].name.is_none());
        assert!(frame.symbols[1].category.is_none());
    }

    #[test]
    fn a_rejected_key_is_reported_rather_than_awaited() {
        let input = r#"{"type":"error","code":"INVALID_KEY","message":"invalid api key"}"#;
        let error = read_auth_frame(input).unwrap().unwrap_err().to_string();

        assert!(error.contains("INVALID_KEY"), "{error}");
    }

    /// The handshake frame is reachable from tracing fields, and the real list runs to thousands of
    /// entries.
    #[test]
    fn the_auth_frame_summarises_its_symbol_list_when_printed() {
        let frame = authenticated(&[("BTC/USD", Some("Crypto")), ("ETH/USD", None)], Some(16));
        let printed = format!("{frame:?}");

        assert!(printed.contains("<2 offered>"), "{printed}");
        assert!(!printed.contains("BTC/USD"), "{printed}");
    }

    #[test]
    fn every_dataset_identifier_expects_only_categories_the_provider_publishes() {
        // A category expectation that no dataset publishes would reject every symbol on that
        // identifier, so the two must be derived from one another rather than listed twice.
        for exchange in [
            ExchangeId::LseFx,
            ExchangeId::LseCrypto,
            ExchangeId::LseEquities,
            ExchangeId::LseFutures,
            ExchangeId::LseCfd,
        ] {
            for category in expected_categories(exchange) {
                assert!(
                    LseDataset::ALL
                        .into_iter()
                        .any(|dataset| dataset.ws_category() == Some(category)),
                    "{exchange} expects an unpublished category {category:?}",
                );
            }
        }
    }

    #[test]
    fn unrelated_datasets_do_not_share_category_expectations() {
        assert_eq!(expected_categories(ExchangeId::LseFx), vec!["Forex"]);
        assert_eq!(expected_categories(ExchangeId::LseCrypto), vec!["Crypto"]);
        assert!(!expected_categories(ExchangeId::LseCfd).contains(&"Forex"));
    }
}
