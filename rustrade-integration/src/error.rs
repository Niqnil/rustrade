use crate::subscription::{SubscriptionId, display_subscription_ids};
use prost::DecodeError;
use reqwest::Error;
use thiserror::Error;

/// All socket IO related errors generated in `rustrade-integration`.
#[derive(Debug, Error)]
pub enum SocketError {
    #[error("Sink error")]
    Sink,

    #[error("Deserialising JSON error: {error} for payload: {payload}")]
    Deserialise {
        error: serde_json::Error,
        payload: String,
    },

    #[error("Deserialising JSON error: {error} for binary payload: {payload:?}")]
    DeserialiseBinary {
        error: serde_json::Error,
        payload: Vec<u8>,
    },

    #[error("Deserialising protobuf error: {error} for binary payload: {payload:?}")]
    DeserialiseProtobuf {
        error: DecodeError,
        payload: Vec<u8>,
    },

    #[error("Serialising JSON error: {0}")]
    Serialise(serde_json::Error),

    #[error("SerDe Query String serialisation error: {0}")]
    QueryParams(#[from] serde_qs::Error),

    #[error("SerDe url encoding serialisation error: {0}")]
    UrlEncoded(#[from] serde_urlencoded::ser::Error),

    #[error("error parsing Url: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("error subscribing to resources over the socket: {0}")]
    Subscribe(String),

    /// Subscription validation ended before every subscription in a batch was acknowledged.
    ///
    /// `unacknowledged` holds the subscriptions that received no acknowledgement, sorted. They are
    /// candidates, not proof of rejection: a venue that closes the connection on one bad
    /// subscription also leaves those sent after it unanswered. When the venue's acknowledgements
    /// cannot be matched to subscriptions, it holds the whole batch.
    #[error(
        "subscriptions not acknowledged ({reason}): {}",
        display_subscription_ids(unacknowledged)
    )]
    Unacknowledged {
        reason: String,
        unacknowledged: Vec<SubscriptionId>,
    },

    #[error("ExchangeStream terminated with closing frame: {0}")]
    Terminated(String),

    #[error("{entity} does not support: {item}")]
    Unsupported { entity: String, item: String },

    #[error("WebSocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("HTTP error: {0}")]
    Http(reqwest::Error),

    #[error("HTTP request timed out")]
    HttpTimeout(reqwest::Error),

    /// REST http response error
    #[error("HTTP response (status={0}) error: {1}")]
    HttpResponse(reqwest::StatusCode, String),

    #[error("consumed unidentifiable message: {0}")]
    Unidentifiable(SubscriptionId),

    #[error("consumed error message from execution: {0}")]
    Exchange(String),
}

impl From<reqwest::Error> for SocketError {
    fn from(error: Error) -> Self {
        match error {
            error if error.is_timeout() => SocketError::HttpTimeout(error),
            error => SocketError::Http(error),
        }
    }
}
