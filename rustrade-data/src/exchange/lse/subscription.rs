//! What the provider answers a `subscribe` with.
//!
//! The replay window's own boundary frames are decoded on the stream instead, by
//! [`LseMessage`](super::tick::LseMessage) — see
//! [`ReplayStarted`](super::tick::LseMessage::ReplayStarted) for why the handshake is the wrong
//! place to read them.

use rustrade_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// The provider's answer to a `subscribe` request.
///
/// One frame arrives per request — `{"action":"subscribe","symbol":"EUR/USD"}` is answered by
/// `{"type":"subscribed","symbol":"EUR/USD","count":1,"max":16}` — so the default
/// [`expected_responses`](crate::exchange::Connector::expected_responses) of one-per-subscription
/// is correct.
///
/// # ⚠️ A confirmation is NOT evidence the symbol exists
/// Subscribing to a symbol the provider has never heard of is **confirmed, not rejected**: it
/// answers `subscribed`, never errors, never ticks, and permanently consumes one of the
/// connection's subscription slots. Validation here therefore cannot catch a typo, and does not
/// try to. That guard runs in the subscriber, against the symbol list the `authenticated` frame
/// supplies, *before* any subscribe is sent — which is also what keeps a bad batch from spending
/// slots it can never get back.
///
/// # Why replay frames are deliberately not modelled here
/// The subscription validator counts every frame that deserialises into this type and validates
/// `Ok` as one more subscription confirmed. A `replay_started` frame modelled as a success would
/// inflate that count and end validation before the last real confirmation arrived. It is decoded
/// on the stream instead, and reaches the transformer through the buffered-events path — the same
/// route ticks that arrive mid-validation take. For the same reason this enum has no catch-all
/// variant: one would swallow ticks into the success count.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseSubResponse {
    /// A subscription was accepted, and the connection now holds `count` of its `max` slots.
    ///
    /// **No field is required to deserialise**, `symbol` included. All three are reported by the
    /// provider and modelled for the reader, but nothing acts on any of them: the validator counts
    /// confirmations rather than matching them to symbols, and the cap is enforced before a
    /// subscribe is sent, against the `authenticated` frame. Requiring a field nothing reads would
    /// let a rename or an omission upstream turn an arriving confirmation into a ten-second
    /// validation timeout blaming latency — and `symbol` is not privileged here, because a
    /// confirmation that cannot be matched to a request is worth exactly as much as one that can.
    /// The same reasoning the [`Error`](Self::Error) variant is built on.
    Subscribed {
        /// The symbol, case-normalised by the provider (`eur/usd` is confirmed as `EUR/USD`).
        #[serde(default)]
        symbol: Option<SmolStr>,
        /// Slots held on this connection after the subscription.
        #[serde(default)]
        count: Option<u32>,
        /// Slots this connection may hold in total.
        #[serde(default)]
        max: Option<u32>,
    },

    /// A subscription was rejected.
    ///
    /// Both fields are optional so that a rejection missing one cannot fail to deserialise and be
    /// silently reclassified as stream data — an error frame must never be the thing that gets
    /// swallowed.
    Error {
        /// The provider's error code. `LIMIT_REACHED` and `INVALID_START` are the two observed;
        /// the field is kept as text rather than an enum so an unrecognised code reaches the
        /// caller intact instead of collapsing into a catch-all.
        #[serde(default)]
        code: Option<SmolStr>,
        /// The provider's own diagnostic.
        #[serde(default)]
        message: Option<SmolStr>,
    },
}

impl Validator for LseSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError> {
        let Self::Error { code, message } = &self else {
            return Ok(self);
        };

        let code = code.as_deref().unwrap_or("unknown");
        let message = message.as_deref().unwrap_or("no message");

        // The rejection does not name the symbol it rejected -- measured: 20 subscriptions
        // requested against a cap of 16 produced 16 confirmations and 4 anonymous errors. There is
        // therefore no partial recovery available, and continuing would leave the caller holding a
        // subscription set the provider silently truncated.
        Err(SocketError::Subscribe(format!(
            "London Strategic Edge rejected a subscription ({code}): {message} - the rejection \
             does not name the symbol, so the whole batch fails"
        )))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn a_subscription_confirmation_deserialises_and_validates() {
        let input = r#"{"type":"subscribed","symbol":"EUR/USD","count":1,"max":16}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        assert_eq!(
            response,
            LseSubResponse::Subscribed {
                symbol: Some("EUR/USD".into()),
                count: Some(1),
                max: Some(16),
            }
        );
        assert!(response.validate().is_ok());
    }

    #[test]
    fn a_subscription_rejection_fails_validation() {
        let input = r#"{"type":"error","code":"LIMIT_REACHED",
            "message":"Max 16 symbols on registered plan"}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        let error = response.validate().unwrap_err().to_string();
        assert!(error.contains("LIMIT_REACHED"), "{error}");
        assert!(error.contains("Max 16 symbols"), "{error}");
    }

    #[test]
    fn a_rejected_replay_start_fails_validation() {
        let input = r#"{"type":"error","code":"INVALID_START",
            "message":"Invalid start: could not convert string to float"}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        assert!(response.validate().is_err());
    }

    /// An error frame that fails to deserialise would be reclassified as stream data and silently
    /// dropped, turning a loud rejection into no signal at all.
    #[test]
    fn a_rejection_missing_its_fields_still_fails_validation() {
        let response: LseSubResponse = serde_json::from_str(r#"{"type":"error"}"#).unwrap();
        assert!(response.validate().is_err());
    }

    /// The subscription validator counts everything that deserialises into [`LseSubResponse`] as a
    /// confirmation. Ticks and replay frames must therefore NOT deserialise into it — they take
    /// the buffered-events path instead.
    #[test]
    fn stream_frames_do_not_deserialise_as_subscription_responses() {
        for input in [
            r#"{"type":"tick","symbol":"EUR/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":1.1,"bid":1.1,"ask":1.1001,"volume":1.0}"#,
            r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:39:31+00:00"}"#,
            r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41,"buffered_drained":9}"#,
        ] {
            assert!(
                serde_json::from_str::<LseSubResponse>(input).is_err(),
                "deserialised as a subscription response: {input}"
            );
        }
    }

    /// A confirmation that failed to deserialise would fall through to the buffered-events path and
    /// leave the validator waiting out its ten-second timeout for a frame that had already arrived
    /// — a failure that reads as latency. Nothing consumes any of these fields, `symbol` included,
    /// so nothing is worth that.
    #[test]
    fn a_confirmation_missing_the_fields_nothing_reads_still_confirms() {
        let response: LseSubResponse =
            serde_json::from_str(r#"{"type":"subscribed","symbol":"EUR/USD"}"#).unwrap();

        assert_eq!(
            response,
            LseSubResponse::Subscribed {
                symbol: Some("EUR/USD".into()),
                count: None,
                max: None,
            }
        );
        assert!(response.validate().is_ok());

        // The tag alone is enough. The validator counts confirmations rather than matching them to
        // requests, so a confirmation naming nothing still confirms one.
        let bare: LseSubResponse = serde_json::from_str(r#"{"type":"subscribed"}"#).unwrap();

        assert_eq!(
            bare,
            LseSubResponse::Subscribed {
                symbol: None,
                count: None,
                max: None,
            }
        );
        assert!(bare.validate().is_ok());
    }
}
