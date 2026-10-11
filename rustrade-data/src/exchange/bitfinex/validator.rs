use super::subscription::{BitfinexPlatformEvent, BitfinexSubResponse};
use crate::{
    Identifier,
    exchange::{Connector, ExchangeSub},
    subscriber::validator::SubscriptionValidator,
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
use smol_str::ToSmolStr;
use std::time::Duration;
use tracing::debug;

/// [`Bitfinex`](super::Bitfinex) specific [`SubscriptionValidator`].
///
/// ### Notes
/// - Required because Bitfinex has a non-self-describing data format after subscriptions have been
///   validated.
/// - The [`BitfinexChannelId`](super::subscription::BitfinexChannelId) is used to identify the
///   [`Subscription`](crate::subscription::Subscription) associated with incoming
///   events, rather than a `String` channel-market identifier.
/// - Therefore the [`SubscriptionId`] format must change during [`BitfinexWebSocketSubValidator::validate`]
///   to use the [`BitfinexChannelId`](super::subscription::BitfinexChannelId)
///   (see module level "SubscriptionId" documentation notes for more details).
///
/// A batch that fails names the subscriptions Bitfinex had not acknowledged, in
/// [`SocketError::Unacknowledged`]. Bitfinex echoes each subscription it accepts, so the rest
/// are left out.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct BitfinexWebSocketSubValidator;

impl SubscriptionValidator for BitfinexWebSocketSubValidator {
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

/// Reads `messages` until every subscription has been acknowledged and has sent its initial
/// snapshot, re-keying each acknowledged one by its channel id, or fails naming the
/// subscriptions left unacknowledged.
///
/// Generic over the message source so the validation logic is testable without a socket.
async fn validate_responses<Exchange, Instrument, Messages>(
    mut instrument_map: Map<Instrument>,
    messages: &mut Messages,
    timeout: Duration,
) -> Result<(Map<Instrument>, Vec<WsMessage>), SocketError>
where
    Exchange: Connector,
    Messages: Stream<Item = Result<WsMessage, WsError>> + Unpin,
{
    let expected_responses = Exchange::expected_responses(&instrument_map);
    let mut success_responses = 0usize;
    let mut init_snapshots_received = 0usize;

    // The subscriptions not yet acknowledged, by the id the mapper keyed them under
    let mut unacknowledged = instrument_map.0.keys().cloned().collect::<FnvHashSet<_>>();

    // Buffer any active Subscription market events that are received during validation
    let mut buff_active_subscription_events = Vec::new();

    // One deadline for the whole validation. A sleep re-armed per message would never expire
    // while an acknowledged subscription keeps streaming events.
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        // Break if all Subscriptions were a success
        if success_responses == expected_responses && init_snapshots_received == expected_responses
        {
            debug!(exchange = %Exchange::ID, "validated exchange WebSocket subscriptions");
            break Ok((instrument_map, buff_active_subscription_events));
        }

        let failure = tokio::select! {
            _ = &mut deadline => format!("subscription validation timeout reached: {timeout:?}"),
            message = messages.next() => {
                let Some(response) = message else {
                    break Err(failure_error(
                        "WebSocket stream terminated unexpectedly".to_owned(),
                        unacknowledged,
                        init_snapshots_received,
                        expected_responses,
                    ));
                };

                match <WebSocketSerdeParser as StreamParser<BitfinexPlatformEvent>>::parse(response) {
                    Some(Ok(response)) => match response.validate() {
                        // Bitfinex platform status is online
                        Ok(BitfinexPlatformEvent::PlatformStatus(status)) => {
                            debug!(
                                exchange = %Exchange::ID,
                                %success_responses,
                                %expected_responses,
                                payload = ?status,
                                "received Bitfinex platform status",
                            );
                            continue
                        }

                        // Subscription success
                        Ok(BitfinexPlatformEvent::Subscribed(response)) => {
                            // Determine SubscriptionId associated with the success response
                            let BitfinexSubResponse { channel, market, channel_id } = &response;
                            let subscription_id = ExchangeSub::from((channel, market)).id();

                            // Replace SubscriptionId with SubscriptionId(channel_id)
                            if let Some(subscription) = instrument_map.0.remove(&subscription_id) {
                                success_responses += 1;
                                unacknowledged.remove(&subscription_id);
                                instrument_map
                                    .0
                                    .insert(SubscriptionId(channel_id.0.to_smolstr()), subscription);

                                debug!(
                                    exchange = %Exchange::ID,
                                    %success_responses,
                                    %expected_responses,
                                    payload = ?response,
                                    "received valid Ok subscription response",
                                );
                            }
                            continue
                        }

                        // Subscription failure: the venue's own message, without the
                        // `Subscribe` wrapper `Unacknowledged` would repeat
                        Err(SocketError::Subscribe(message)) => message,
                        Err(error) => error.to_string(),

                        // `validate` turns every error event into an `Err`
                        Ok(BitfinexPlatformEvent::Error(error)) => format!("{error:?}"),
                    }
                    Some(Err(SocketError::Deserialise { error: _, payload }))
                        if success_responses >= 1 =>
                    {
                        // Already active Bitfinex subscriptions will send initial snapshots
                        init_snapshots_received += 1;
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

        break Err(failure_error(
            failure,
            unacknowledged,
            init_snapshots_received,
            expected_responses,
        ));
    }
}

/// The error a validation failing for `reason` ends with: [`SocketError::Unacknowledged`] naming
/// the subscriptions not acknowledged, or, once every one has been, a [`SocketError::Subscribe`]
/// saying how many initial snapshots arrived.
fn failure_error(
    reason: String,
    unacknowledged: FnvHashSet<SubscriptionId>,
    init_snapshots_received: usize,
    expected_responses: usize,
) -> SocketError {
    if unacknowledged.is_empty() {
        return SocketError::Subscribe(format!(
            "{reason}; every subscription was acknowledged, but only \
             {init_snapshots_received} of {expected_responses} initial snapshots arrived"
        ));
    }

    let mut unacknowledged = unacknowledged.into_iter().collect::<Vec<_>>();
    unacknowledged.sort_unstable();
    SocketError::Unacknowledged {
        reason,
        unacknowledged,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::bitfinex::Bitfinex;
    use futures::stream;

    const TIMEOUT: Duration = Duration::from_secs(10);

    fn map(ids: &[&str]) -> Map<usize> {
        ids.iter()
            .enumerate()
            .map(|(key, id)| (SubscriptionId::from(*id), key))
            .collect()
    }

    fn ids(ids: &[&str]) -> Vec<SubscriptionId> {
        ids.iter().map(|id| SubscriptionId::from(*id)).collect()
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

    fn text(message: &str) -> Result<WsMessage, WsError> {
        Ok(WsMessage::text(message.to_owned()))
    }

    fn subscribed(channel: &str, symbol: &str, channel_id: u32) -> Result<WsMessage, WsError> {
        Ok(WsMessage::text(format!(
            r#"{{"event":"subscribed","channel":"{channel}","chanId":{channel_id},"symbol":"{symbol}","pair":"X"}}"#
        )))
    }

    fn snapshot(channel_id: u32) -> Result<WsMessage, WsError> {
        Ok(WsMessage::text(format!(
            "[{channel_id},[[1,1791679984451,0.1,82995]]]"
        )))
    }

    const INFO: &str = r#"{"event":"info","version":2,"serverId":"x","platform":{"status":1}}"#;

    #[tokio::test(start_paused = true)]
    async fn every_acknowledgement_rekeys_its_subscription_by_channel_id() {
        let mut messages = stream::iter([
            text(INFO),
            subscribed("trades", "tBTCUSD", 53),
            snapshot(53),
            subscribed("trades", "tETHUSD", 54),
            snapshot(54),
        ]);

        let (map, buffered) = validate_responses::<Bitfinex, _, _>(
            map(&["trades|tBTCUSD", "trades|tETHUSD"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(map.find("53").unwrap(), &0);
        assert_eq!(map.find("54").unwrap(), &1);
        assert_eq!(buffered.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_names_the_subscriptions_left_unacknowledged() {
        // As Bitfinex answers an unknown symbol: an error naming the subscription it refuses
        let mut messages = stream::iter([
            text(INFO),
            subscribed("trades", "tBTCUSD", 53),
            text(
                r#"{"channel":"trades","symbol":"tNOSUCH","event":"error","msg":"symbol: invalid","code":10300,"pair":"NOSUCH"}"#,
            ),
        ]);

        let error = validate_responses::<Bitfinex, _, _>(
            map(&["trades|tBTCUSD", "trades|tNOSUCH", "trades|tETHUSD"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap_err();

        let (reason, unacknowledged) = unacknowledged(error);
        assert_eq!(
            reason,
            "Bitfinex refused trades tNOSUCH: symbol: invalid (code 10300)"
        );
        assert_eq!(unacknowledged, ids(&["trades|tETHUSD", "trades|tNOSUCH"]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_connection_names_the_subscriptions_left_unacknowledged() {
        let mut messages = stream::iter([subscribed("trades", "tBTCUSD", 53)]);

        let error = validate_responses::<Bitfinex, _, _>(
            map(&["trades|tBTCUSD", "book|tETHUSD"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap_err();

        let (reason, unacknowledged) = unacknowledged(error);
        assert_eq!(reason, "WebSocket stream terminated unexpectedly");
        assert_eq!(unacknowledged, ids(&["book|tETHUSD"]));
    }

    #[tokio::test(start_paused = true)]
    async fn the_timeout_fires_while_an_acknowledged_subscription_keeps_streaming() {
        // One heartbeat a second, forever: a sleep re-armed per message would never expire
        let heartbeats = stream::unfold((), |()| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Some((text(r#"[53,"hb"]"#), ()))
        });
        let mut messages =
            Box::pin(stream::iter([subscribed("trades", "tBTCUSD", 53)]).chain(heartbeats));
        let started = tokio::time::Instant::now();

        let error = validate_responses::<Bitfinex, _, _>(
            map(&["trades|tBTCUSD", "trades|tETHUSD"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap_err();

        assert_eq!(started.elapsed(), TIMEOUT);
        let (reason, unacknowledged) = unacknowledged(error);
        assert_eq!(
            reason,
            format!("subscription validation timeout reached: {TIMEOUT:?}")
        );
        assert_eq!(unacknowledged, ids(&["trades|tETHUSD"]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_snapshot_after_every_acknowledgement_says_so() {
        let mut messages = stream::iter([
            subscribed("trades", "tBTCUSD", 53),
            subscribed("trades", "tETHUSD", 54),
            snapshot(53),
        ]);

        let error = validate_responses::<Bitfinex, _, _>(
            map(&["trades|tBTCUSD", "trades|tETHUSD"]),
            &mut messages,
            TIMEOUT,
        )
        .await
        .unwrap_err();

        let SocketError::Subscribe(message) = error else {
            panic!("expected Subscribe, got {error:?}");
        };
        assert_eq!(
            message,
            "WebSocket stream terminated unexpectedly; every subscription was acknowledged, but \
             only 1 of 2 initial snapshots arrived"
        );
    }
}
