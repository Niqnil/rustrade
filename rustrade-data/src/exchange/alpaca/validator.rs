use super::{
    channel::AlpacaChannel,
    subscription::{AlpacaSubResponse, AlpacaSubResponseInner},
};
use crate::{
    Identifier,
    exchange::{Connector, ExchangeSub},
    subscriber::validator::SubscriptionValidator,
    subscription::{Map, SubscriptionKind},
};
use futures::StreamExt;
use rustrade_integration::{
    Validator,
    error::SocketError,
    protocol::{
        StreamParser,
        websocket::{WebSocket, WebSocketSerdeParser, WsMessage},
    },
    subscription::SubscriptionId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::debug;

/// [`Alpaca`](super::Alpaca) specific [`SubscriptionValidator`].
///
/// ### Why Alpaca cannot use the standard validator
///
/// [`WebSocketSubValidator`](crate::subscriber::validator::WebSocketSubValidator) counts
/// responses: it succeeds once it has seen
/// [`Connector::expected_responses`] payloads that did not report an error. Alpaca answers a
/// multi-symbol subscribe with a *single* confirmation frame listing the symbols it actually
/// registered, so counting can only ever establish that *a* confirmation arrived — never that it
/// covered what was asked for. A confirmation naming one of two requested symbols is
/// indistinguishable from one naming both, and the caller receives a stream that is silently
/// subscribed to less than it requested.
///
/// The same gap admits a second, subtler acceptance: `[{"T":"success", ...}]` carries no symbols
/// at all, yet satisfies a counting validator just as well as a real confirmation.
///
/// ### What this validator does instead
///
/// Success is defined by *coverage*, not by a count. Every requested
/// [`SubscriptionId`] must be named in a confirmation before validation
/// passes; anything still outstanding when the timeout expires is reported by name. A venue that
/// quietly drops a symbol therefore fails the subscribe rather than producing a half-subscribed
/// stream, and a `success` frame confirms nothing because it names nothing.
///
/// Symbols Alpaca confirms that were never requested are ignored: the confirmation reports the
/// connection's whole subscription state, which is not this validator's concern.
///
/// ### What it does not do
///
/// Coverage says the venue accepted the subscription, not that the symbol will produce data.
/// Alpaca's crypto feed publishes a quote when top-of-book changes, and the delay before a given
/// symbol first ticks is both large and highly variable -- measured at 1s, 15s, 96s and 132s for
/// four symbols confirmed on one connection in a single 300s window. Absence of data is not
/// evidence of a failed subscription, and callers must not treat it as such.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct AlpacaWebSocketSubValidator;

impl SubscriptionValidator for AlpacaWebSocketSubValidator {
    type Parser = WebSocketSerdeParser;

    async fn validate<Exchange, Instrument, Kind>(
        instrument_map: Map<Instrument>,
        websocket: &mut WebSocket,
    ) -> Result<(Map<Instrument>, Vec<WsMessage>), SocketError>
    where
        Exchange: Connector + Send,
        Instrument: Send,
        Kind: SubscriptionKind + Send,
    {
        let timeout = Exchange::subscription_timeout();

        // Every requested Subscription, by SubscriptionId. A confirmation removes the ids it
        // names; validation passes only once this is empty, so an unconfirmed symbol cannot be
        // mistaken for a confirmed one.
        let mut awaiting: HashSet<SubscriptionId> = instrument_map.0.keys().cloned().collect();

        // Buffer any active Subscription market events that are received during validation
        let mut buff_active_subscription_events = Vec::new();

        loop {
            if awaiting.is_empty() {
                debug!(exchange = %Exchange::ID, "validated exchange WebSocket subscriptions");
                break Ok((instrument_map, buff_active_subscription_events));
            }

            tokio::select! {
                _ = tokio::time::sleep(timeout) => {
                    break Err(SocketError::Subscribe(format!(
                        "subscription validation timeout reached: {timeout:?}; \
                         Alpaca never confirmed: {}",
                        display_awaiting(&awaiting)
                    )))
                },
                message = websocket.next() => {
                    let response = match message {
                        Some(response) => response,
                        None => break Err(SocketError::Subscribe(
                            "WebSocket stream terminated unexpectedly".to_string()
                        ))
                    };

                    match <WebSocketSerdeParser as StreamParser<AlpacaSubResponse>>::parse(response) {
                        Some(Ok(response)) => match response.validate() {
                            Ok(response) => {
                                for inner in &response.0 {
                                    confirm(&mut awaiting, inner);
                                }
                                debug!(
                                    exchange = %Exchange::ID,
                                    outstanding = %display_awaiting(&awaiting),
                                    payload = ?response,
                                    "received Alpaca subscription response",
                                );
                            }
                            Err(err) => break Err(err),
                        }
                        Some(Err(SocketError::Deserialise { error: _, payload })) => {
                            // Most likely already active subscription payload, so add to market
                            // event buffer for post validation processing
                            buff_active_subscription_events.push(WsMessage::text(payload));
                            continue
                        }
                        Some(Err(SocketError::Terminated(close_frame))) => {
                            break Err(SocketError::Subscribe(
                                format!("received WebSocket CloseFrame: {close_frame}")
                            ))
                        }
                        _ => {
                            // Pings, Pongs, Frames, etc.
                            continue
                        }
                    }
                }
            }
        }
    }
}

/// Remove from `awaiting` every [`SubscriptionId`] this message confirms.
///
/// Only [`AlpacaSubResponseInner::Subscription`] names symbols. `Success` and `Error` confirm
/// nothing -- `Error` never reaches here, having already failed
/// [`AlpacaSubResponse::validate`](rustrade_integration::Validator::validate).
fn confirm(awaiting: &mut HashSet<SubscriptionId>, inner: &AlpacaSubResponseInner) {
    let AlpacaSubResponseInner::Subscription {
        trades,
        quotes,
        bars: _,
    } = inner
    else {
        return;
    };

    // `bars` is deliberately ignored: AlpacaChannel models only trades and quotes, so no
    // Subscription is ever keyed against a bars channel and nothing could match it.
    for (channel, markets) in [
        (AlpacaChannel::Trades, trades),
        (AlpacaChannel::Quotes, quotes),
    ] {
        for market in markets {
            awaiting.remove(&ExchangeSub::from((channel, market.as_str())).id());
        }
    }
}

/// Render the outstanding [`SubscriptionId`]s in a stable order, so a failure names the same
/// symbols in the same sequence on every run.
fn display_awaiting(awaiting: &HashSet<SubscriptionId>) -> String {
    let mut ids = awaiting.iter().map(|id| id.0.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    ids.join(", ")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use smol_str::SmolStr;

    fn id(channel: AlpacaChannel, market: &str) -> SubscriptionId {
        ExchangeSub::from((channel, market)).id()
    }

    fn awaiting(ids: impl IntoIterator<Item = SubscriptionId>) -> HashSet<SubscriptionId> {
        ids.into_iter().collect()
    }

    fn subscription(trades: &[&str], quotes: &[&str]) -> AlpacaSubResponseInner {
        AlpacaSubResponseInner::Subscription {
            trades: trades.iter().map(|s| SmolStr::new(*s)).collect(),
            quotes: quotes.iter().map(|s| SmolStr::new(*s)).collect(),
            bars: vec![],
        }
    }

    #[test]
    fn a_confirmation_naming_every_requested_symbol_clears_them_all() {
        let mut outstanding = awaiting([
            id(AlpacaChannel::Quotes, "BTC/USD"),
            id(AlpacaChannel::Quotes, "ETH/USD"),
        ]);

        confirm(
            &mut outstanding,
            &subscription(&[], &["BTC/USD", "ETH/USD"]),
        );

        assert!(outstanding.is_empty());
    }

    /// The property this validator exists for. A counting validator accepts this response as
    /// readily as the one above, and the caller is handed a stream subscribed to one of the two
    /// symbols it asked for, with nothing reported.
    #[test]
    fn a_confirmation_that_omits_a_requested_symbol_leaves_it_outstanding() {
        let eth = id(AlpacaChannel::Quotes, "ETH/USD");
        let mut outstanding = awaiting([id(AlpacaChannel::Quotes, "BTC/USD"), eth.clone()]);

        confirm(&mut outstanding, &subscription(&[], &["BTC/USD"]));

        assert_eq!(outstanding, awaiting([eth]));
        assert_eq!(display_awaiting(&outstanding), "quotes|ETH/USD");
    }

    /// `[{"T":"success","msg":"authenticated"}]` names no symbols, so it must confirm none. It
    /// validates `Ok`, which is exactly why a validator that counts non-error responses can be
    /// satisfied by an auth acknowledgement that arrived late.
    #[test]
    fn a_success_message_confirms_nothing() {
        let requested = awaiting([id(AlpacaChannel::Quotes, "BTC/USD")]);
        let mut outstanding = requested.clone();

        confirm(
            &mut outstanding,
            &AlpacaSubResponseInner::Success {
                msg: SmolStr::new("authenticated"),
            },
        );

        assert_eq!(outstanding, requested);
    }

    /// The confirmation reports the connection's whole subscription state, which may name symbols
    /// this validation did not request. They are not ours to account for.
    #[test]
    fn symbols_confirmed_but_never_requested_are_ignored() {
        let btc = id(AlpacaChannel::Quotes, "BTC/USD");
        let mut outstanding = awaiting([btc.clone()]);

        confirm(&mut outstanding, &subscription(&[], &["SOL/USD"]));

        assert_eq!(outstanding, awaiting([btc]));
    }

    /// A symbol confirmed on one channel must not satisfy a request on another: subscribing to
    /// BTC/USD trades says nothing about BTC/USD quotes.
    #[test]
    fn a_confirmation_on_one_channel_does_not_satisfy_the_other() {
        let quotes = id(AlpacaChannel::Quotes, "BTC/USD");
        let mut outstanding = awaiting([quotes.clone()]);

        confirm(&mut outstanding, &subscription(&["BTC/USD"], &[]));

        assert_eq!(outstanding, awaiting([quotes]));
    }

    /// A `HashSet` iterates in an arbitrary order, so the failure message sorts before printing.
    #[test]
    fn outstanding_subscriptions_are_reported_in_a_stable_order() {
        let outstanding = awaiting([
            id(AlpacaChannel::Quotes, "SOL/USD"),
            id(AlpacaChannel::Quotes, "BTC/USD"),
            id(AlpacaChannel::Trades, "ETH/USD"),
        ]);

        assert_eq!(
            display_awaiting(&outstanding),
            "quotes|BTC/USD, quotes|SOL/USD, trades|ETH/USD"
        );
    }
}
