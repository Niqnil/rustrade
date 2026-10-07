use crate::{
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
};
use fnv::FnvHashSet;
use futures::{Stream, StreamExt};
use rustrade_integration::{
    Validator,
    error::SocketError,
    protocol::{
        StreamParser,
        websocket::{WebSocket, WebSocketSerdeParser, WsError, WsMessage},
    },
    subscription::SubscriptionId,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, time::Duration};
use tracing::debug;

/// Defines how to validate that actioned market data
/// [`Subscription`](crate::subscription::Subscription)s were accepted by the exchange.
pub trait SubscriptionValidator {
    type Parser;

    fn validate<Exchange, InstrumentKey, Kind>(
        instrument_map: Map<InstrumentKey>,
        websocket: &mut WebSocket,
    ) -> impl Future<Output = Result<(Map<InstrumentKey>, Vec<WsMessage>), SocketError>> + Send
    where
        Exchange: Connector + Send,
        InstrumentKey: Send,
        Kind: SubscriptionKind + Send;
}

/// Standard [`SubscriptionValidator`] for [`WebSocket`]s suitable for most exchanges.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketSubValidator;

impl SubscriptionValidator for WebSocketSubValidator {
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
        validate_responses::<Exchange, _, _>(
            instrument_map,
            websocket,
            Exchange::subscription_timeout(),
        )
        .await
    }
}

/// Reads `messages` until every expected acknowledgement has arrived, or fails naming the
/// subscriptions left unacknowledged.
///
/// Generic over the message source so the validation logic is testable without a socket.
async fn validate_responses<Exchange, Instrument, Messages>(
    instrument_map: Map<Instrument>,
    messages: &mut Messages,
    timeout: Duration,
) -> Result<(Map<Instrument>, Vec<WsMessage>), SocketError>
where
    Exchange: Connector,
    Messages: Stream<Item = Result<WsMessage, WsError>> + Unpin,
{
    let expected_responses = Exchange::expected_responses(&instrument_map);
    let mut acknowledged = Acknowledged::default();

    // Buffer any active Subscription market events that are received during validation
    let mut buff_active_subscription_events = Vec::new();

    // One deadline for the whole validation. A sleep re-armed per message would never expire
    // while an acknowledged subscription keeps streaming events.
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        // Break if all Subscriptions were a success
        if acknowledged.count == expected_responses {
            debug!(exchange = %Exchange::ID, "validated exchange WebSocket subscriptions");
            break Ok((instrument_map, buff_active_subscription_events));
        }

        let failure = tokio::select! {
            _ = &mut deadline => format!("subscription validation timeout reached: {timeout:?}"),
            message = messages.next() => {
                let Some(response) = message else {
                    break Err(acknowledged.into_error(
                        "WebSocket stream terminated unexpectedly".to_owned(),
                        &instrument_map,
                    ));
                };

                match <WebSocketSerdeParser as StreamParser<Exchange::SubResponse>>::parse(response) {
                    Some(Ok(response)) => match response.validate() {
                        // Subscription success
                        Ok(response) => {
                            acknowledged.record(
                                Exchange::acknowledged_subscription(&response),
                                &instrument_map,
                            );
                            debug!(
                                exchange = %Exchange::ID,
                                success_responses = %acknowledged.count,
                                %expected_responses,
                                payload = ?response,
                                "received valid Ok subscription response",
                            );
                            continue
                        }

                        // Subscription failure
                        Err(error) => error.to_string(),
                    }
                    Some(Err(SocketError::Deserialise { error: _, payload })) => {
                        // Most likely already active subscription payload, so add to market
                        // event buffer for post validation processing
                        buff_active_subscription_events.push(WsMessage::text(payload));
                        continue
                    }
                    Some(Err(SocketError::Terminated(close_frame))) => {
                        format!("received WebSocket CloseFrame: {close_frame}")
                    }
                    _ => {
                        // Pings, Pongs, Frames, etc.
                        continue
                    }
                }
            }
        };

        break Err(acknowledged.into_error(failure, &instrument_map));
    }
}

/// The acknowledgements a validation has received so far.
#[derive(Debug, Default)]
struct Acknowledged {
    /// Every successful response, matched to a subscription or not.
    count: usize,
    /// The subscriptions the venue's responses named.
    matched: FnvHashSet<SubscriptionId>,
}

impl Acknowledged {
    fn record<Instrument>(&mut self, id: Option<SubscriptionId>, map: &Map<Instrument>) {
        self.count += 1;
        if let Some(id) = id.filter(|id| map.0.contains_key(id)) {
            self.matched.insert(id);
        }
    }

    /// The error naming every subscription not shown to be acknowledged.
    ///
    /// Only when each acknowledgement named a distinct subscription in the batch are the matched
    /// ones left out. Otherwise — a venue that does not echo the subscription, one acknowledgement
    /// for the whole batch, or a response that named nothing recognisable — any subscription could
    /// be the unanswered one, so the whole batch is named.
    fn into_error<Instrument>(self, reason: String, map: &Map<Instrument>) -> SocketError {
        let narrowed = self.matched.len() == self.count;
        let mut unacknowledged = map
            .0
            .keys()
            .filter(|id| !(narrowed && self.matched.contains(*id)))
            .cloned()
            .collect::<Vec<_>>();
        unacknowledged.sort_unstable();

        SocketError::Unacknowledged {
            reason,
            unacknowledged,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::okx::Okx;
    use futures::stream;

    const TIMEOUT: Duration = Duration::from_secs(10);

    fn map(ids: &[&str]) -> Map<usize> {
        ids.iter()
            .enumerate()
            .map(|(key, id)| (SubscriptionId::from(*id), key))
            .collect()
    }

    fn unacknowledged(error: SocketError) -> (String, Vec<SubscriptionId>) {
        match error {
            SocketError::Unacknowledged {
                reason,
                unacknowledged,
            } => (reason, unacknowledged),
            other => panic!("expected Unacknowledged, got {other:?}"),
        }
    }

    fn ids(ids: &[&str]) -> Vec<SubscriptionId> {
        ids.iter().map(|id| SubscriptionId::from(*id)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_venue_that_does_not_echo_its_subscriptions_names_the_whole_batch() {
        let mut messages = stream::iter([Ok(WsMessage::text(r#"{"event":"subscribe"}"#))]);

        let error = validate_responses::<Okx, _, _>(
            map(&["trades|BTC-USDT", "trades|ETH-USDT"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap_err();

        let (_, unacknowledged) = unacknowledged(error);
        assert_eq!(unacknowledged, ids(&["trades|BTC-USDT", "trades|ETH-USDT"]));
    }

    #[cfg(feature = "hyperliquid")]
    mod hyperliquid {
        use super::*;
        use crate::exchange::hyperliquid::Hyperliquid;

        fn hyperliquid_ack(kind: &str, coin: &str) -> Result<WsMessage, WsError> {
            Ok(WsMessage::text(format!(
                r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{{"type":"{kind}","coin":"{coin}"}}}}}}"#
            )))
        }

        fn hyperliquid_trade() -> Result<WsMessage, WsError> {
            Ok(WsMessage::text(
                r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"hash":"0x","tid":1}]}"#,
            ))
        }

        #[tokio::test(start_paused = true)]
        async fn every_acknowledgement_validates_and_keeps_early_events() {
            let mut messages = stream::iter([
                hyperliquid_ack("trades", "BTC"),
                hyperliquid_trade(),
                hyperliquid_ack("l2Book", "ETH"),
            ]);

            let (map, buffered) = validate_responses::<Hyperliquid, _, _>(
                map(&["trades|BTC", "l2Book|ETH"]),
                &mut messages,
                TIMEOUT,
            )
            .await
            .unwrap();

            assert_eq!(map.0.len(), 2);
            assert_eq!(buffered.len(), 1);
        }

        #[tokio::test(start_paused = true)]
        async fn a_dropped_connection_names_the_subscriptions_left_unacknowledged() {
            // As Hyperliquid mainnet behaves: a bad coin is never acknowledged and the connection
            // drops, so the subscription sent after it goes unanswered too.
            let mut messages = stream::iter([
                hyperliquid_ack("trades", "BTC"),
                hyperliquid_ack("l2Book", "ETH"),
            ]);

            let error = validate_responses::<Hyperliquid, _, _>(
                map(&["trades|BTC", "l2Book|ETH", "l2Book|XYZ:TSLA", "trades|@107"]),
                &mut messages,
                TIMEOUT,
            )
            .await
            .unwrap_err();

            let (reason, unacknowledged) = unacknowledged(error);
            assert_eq!(reason, "WebSocket stream terminated unexpectedly");
            assert_eq!(unacknowledged, ids(&["l2Book|XYZ:TSLA", "trades|@107"]));
        }

        #[tokio::test(start_paused = true)]
        async fn the_timeout_fires_while_an_acknowledged_subscription_keeps_streaming() {
            // One event a second, forever: a sleep re-armed per message would never expire
            let events = stream::unfold((), |()| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Some((hyperliquid_trade(), ()))
            });
            let mut messages =
                Box::pin(stream::iter([hyperliquid_ack("trades", "BTC")]).chain(events));
            let started = tokio::time::Instant::now();

            let error = validate_responses::<Hyperliquid, _, _>(
                map(&["trades|BTC", "l2Book|ETH"]),
                &mut messages,
                TIMEOUT,
            )
            .await
            .unwrap_err();

            assert_eq!(started.elapsed(), TIMEOUT);
            let (reason, unacknowledged) = unacknowledged(error);
            assert!(reason.contains("timeout"), "{reason}");
            assert_eq!(unacknowledged, ids(&["l2Book|ETH"]));
        }

        #[tokio::test(start_paused = true)]
        async fn an_error_response_names_the_unacknowledged_subscriptions_with_its_reason() {
            let mut messages = stream::iter([
                hyperliquid_ack("trades", "BTC"),
                Ok(WsMessage::text(
                    r#"{"channel":"error","data":"invalid subscription"}"#,
                )),
            ]);

            let error = validate_responses::<Hyperliquid, _, _>(
                map(&["trades|BTC", "l2Book|ETH"]),
                &mut messages,
                TIMEOUT,
            )
            .await
            .unwrap_err();

            let (reason, unacknowledged) = unacknowledged(error);
            assert!(reason.contains("invalid subscription"), "{reason}");
            assert_eq!(unacknowledged, ids(&["l2Book|ETH"]));
        }

        #[tokio::test(start_paused = true)]
        async fn an_acknowledgement_naming_no_subscription_in_the_batch_names_the_whole_batch() {
            // Which subscription it answered is unknown, so any of them may be the unanswered one
            let mut messages = stream::iter([hyperliquid_ack("trades", "SOL")]);

            let error = validate_responses::<Hyperliquid, _, _>(
                map(&["trades|BTC", "l2Book|ETH"]),
                &mut messages,
                TIMEOUT,
            )
            .await
            .unwrap_err();

            let (_, unacknowledged) = unacknowledged(error);
            assert_eq!(unacknowledged, ids(&["l2Book|ETH", "trades|BTC"]));
        }
    }
}
