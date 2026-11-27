use crate::{Message, error::SocketError, protocol::StreamParser};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::Duration};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, connect_async,
    tungstenite::{
        Utf8Bytes,
        client::IntoClientRequest,
        error::ProtocolError,
        protocol::{CloseFrame, frame::Frame},
    },
};
use tracing::debug;

/// Convenient type alias for a tungstenite `WebSocketStream`.
pub type WebSocket = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Convenient type alias for the `Sink` half of a tungstenite [`WebSocket`].
pub type WsSink = futures::stream::SplitSink<WebSocket, WsMessage>;

/// Convenient type alias for the `Stream` half of a tungstenite [`WebSocket`].
pub type WsStream = futures::stream::SplitStream<WebSocket>;

/// Communicative type alias for a tungstenite [`WebSocket`] `Message`.
pub type WsMessage = tokio_tungstenite::tungstenite::Message;

/// Communicative type alias for a tungstenite [`WebSocket`] `Error`.
pub type WsError = tokio_tungstenite::tungstenite::Error;

/// [`WebSocket`] administration message variants.
#[derive(Debug)]
pub enum AdminWs {
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
    WsError(WsError),
}

/// [`WebSocket`] parser.
///
/// Translates a `Result<WsMessage, WsError>` to a `Message<AdminWs, Bytes>`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WsParser;

impl WsParser {
    pub fn parse(ws_result: Result<WsMessage, WsError>) -> Message<AdminWs, Bytes> {
        match ws_result {
            Ok(WsMessage::Text(utf8)) => Message::Payload(Bytes::from(utf8)),
            Ok(WsMessage::Binary(bytes)) => Message::Payload(bytes),
            Ok(WsMessage::Frame(frame)) => Message::Payload(frame.into_payload()),
            Ok(WsMessage::Ping(bytes)) => Message::Admin(AdminWs::Ping(bytes)),
            Ok(WsMessage::Pong(bytes)) => Message::Admin(AdminWs::Pong(bytes)),
            Ok(WsMessage::Close(close)) => Message::Admin(AdminWs::Close(close)),
            Err(error) => Message::Admin(AdminWs::WsError(error)),
        }
    }
}

/// Default [`StreamParser`] implementation for a [`WebSocket`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketSerdeParser;

impl<Output> StreamParser<Output> for WebSocketSerdeParser
where
    Output: for<'de> Deserialize<'de>,
{
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<Output, SocketError>> {
        match input {
            Ok(ws_message) => match ws_message {
                WsMessage::Text(text) => process_text(text),
                WsMessage::Binary(binary) => process_binary(binary),
                WsMessage::Ping(ping) => process_ping(ping),
                WsMessage::Pong(pong) => process_pong(pong),
                WsMessage::Close(close_frame) => process_close_frame(close_frame),
                WsMessage::Frame(frame) => process_frame(frame),
            },
            Err(ws_err) => Some(Err(SocketError::WebSocket(Box::new(ws_err)))),
        }
    }
}

/// [`StreamParser`] implementation for a [`WebSocket`] that decodes protobuf
/// binary payloads.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketProtobufParser;

impl<Output> StreamParser<Output> for WebSocketProtobufParser
where
    Output: prost::Message + Default,
{
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<Output, SocketError>> {
        match input {
            Ok(ws_message) => match ws_message {
                WsMessage::Text(payload) => {
                    debug!(?payload, "received Text WebSocket message");
                    None
                }
                WsMessage::Binary(binary) => {
                    Some(Output::decode(binary.as_ref()).map_err(|error| {
                        SocketError::DeserialiseProtobuf {
                            error,
                            payload: binary.to_vec(),
                        }
                    }))
                }
                WsMessage::Ping(ping) => process_ping::<Output>(ping),
                WsMessage::Pong(pong) => process_pong::<Output>(pong),
                WsMessage::Close(close_frame) => process_close_frame::<Output>(close_frame),
                WsMessage::Frame(frame) => process_frame::<Output>(frame),
            },
            Err(ws_err) => Some(Err(SocketError::WebSocket(Box::new(ws_err)))),
        }
    }
}

/// Process a payload of `String` by deserialising into an `ExchangeMessage`.
pub fn process_text<ExchangeMessage>(
    payload: Utf8Bytes,
) -> Option<Result<ExchangeMessage, SocketError>>
where
    ExchangeMessage: for<'de> Deserialize<'de>,
{
    Some(
        serde_json::from_str::<ExchangeMessage>(&payload).map_err(|error| {
            debug!(
                ?error,
                ?payload,
                action = "returning Some(Err(err))",
                "failed to deserialize WebSocket Message into domain specific Message"
            );
            SocketError::Deserialise {
                error,
                payload: payload.to_string(),
            }
        }),
    )
}

/// Process a payload of `Vec<u8>` bytes by deserialising into an `ExchangeMessage`.
pub fn process_binary<ExchangeMessage>(
    payload: Bytes,
) -> Option<Result<ExchangeMessage, SocketError>>
where
    ExchangeMessage: for<'de> Deserialize<'de>,
{
    Some(
        serde_json::from_slice::<ExchangeMessage>(&payload).map_err(|error| {
            debug!(
                ?error,
                ?payload,
                action = "returning Some(Err(err))",
                "failed to deserialize WebSocket Message into domain specific Message"
            );
            SocketError::Deserialise {
                error,
                payload: String::from_utf8(payload.into()).unwrap_or_else(|x| x.to_string()),
            }
        }),
    )
}

/// Basic process for a [`WebSocket`] ping message. Logs the payload at `trace` level.
pub fn process_ping<ExchangeMessage>(ping: Bytes) -> Option<Result<ExchangeMessage, SocketError>> {
    debug!(payload = ?ping, "received Ping WebSocket message");
    None
}

/// Basic process for a [`WebSocket`] pong message. Logs the payload at `trace` level.
pub fn process_pong<ExchangeMessage>(pong: Bytes) -> Option<Result<ExchangeMessage, SocketError>> {
    debug!(payload = ?pong, "received Pong WebSocket message");
    None
}

/// Basic process for a [`WebSocket`] CloseFrame message. Logs the payload at `trace` level.
pub fn process_close_frame<ExchangeMessage>(
    close_frame: Option<CloseFrame>,
) -> Option<Result<ExchangeMessage, SocketError>> {
    let close_frame = format!("{close_frame:?}");
    debug!(payload = %close_frame, "received CloseFrame WebSocket message");
    Some(Err(SocketError::Terminated(close_frame)))
}

/// Basic process for a [`WebSocket`] Frame message. Logs the payload at `trace` level.
pub fn process_frame<ExchangeMessage>(
    frame: Frame,
) -> Option<Result<ExchangeMessage, SocketError>> {
    let frame = format!("{frame:?}");
    debug!(payload = %frame, "received unexpected Frame WebSocket message");
    None
}

/// How long [`connect`] waits for a WebSocket connection to be established.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connect asynchronously to a [`WebSocket`] server, giving up after [`DEFAULT_CONNECT_TIMEOUT`].
///
/// See [`connect_with_timeout`].
///
/// # Errors
/// As [`connect_with_timeout`].
pub async fn connect<R>(request: R) -> Result<WebSocket, SocketError>
where
    R: IntoClientRequest + Unpin + Debug,
{
    connect_with_timeout(request, DEFAULT_CONNECT_TIMEOUT).await
}

/// Connect asynchronously to a [`WebSocket`] server, giving up after `timeout`.
///
/// `timeout` bounds establishing the connection as a whole: the TCP connect, the TLS handshake
/// and the HTTP upgrade. A peer that accepts the TCP connection but never completes the handshake
/// would otherwise hold the attempt open indefinitely. On expiry the attempt is dropped, which
/// closes whatever part of the connection was open.
///
/// # Errors
/// - [`SocketError::ConnectTimeout`] if the connection is not established within `timeout`.
/// - [`SocketError::WebSocket`] if establishing it fails.
pub async fn connect_with_timeout<R>(
    request: R,
    timeout: Duration,
) -> Result<WebSocket, SocketError>
where
    R: IntoClientRequest + Unpin + Debug,
{
    debug!(
        ?request,
        ?timeout,
        "attempting to establish WebSocket connection"
    );
    match tokio::time::timeout(timeout, connect_async(request)).await {
        Ok(Ok((websocket, _))) => Ok(websocket),
        Ok(Err(error)) => Err(SocketError::WebSocket(Box::new(error))),
        Err(_elapsed) => Err(SocketError::ConnectTimeout { timeout }),
    }
}

/// Determine whether a [`WsError`] indicates the [`WebSocket`] has disconnected.
pub fn is_websocket_disconnected(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Io(_)
            | WsError::Protocol(ProtocolError::SendAfterClosing)
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    #[test]
    fn test_ws_parser_text_message() {
        let msg = Ok(WsMessage::Text("hello".into()));
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Payload(bytes) if bytes == "hello"));
    }

    #[test]
    fn test_ws_parser_binary_message() {
        let msg = Ok(WsMessage::Binary(Bytes::from_static(b"\x01\x02")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Payload(bytes) if bytes == Bytes::from_static(b"\x01\x02"))
        );
    }

    #[test]
    fn test_ws_parser_ping() {
        let msg = Ok(WsMessage::Ping(Bytes::from_static(b"ping")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Admin(AdminWs::Ping(bytes)) if bytes == Bytes::from_static(b"ping"))
        );
    }

    #[test]
    fn test_ws_parser_pong() {
        let msg = Ok(WsMessage::Pong(Bytes::from_static(b"pong")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Admin(AdminWs::Pong(bytes)) if bytes == Bytes::from_static(b"pong"))
        );
    }

    #[test]
    fn test_ws_parser_close() {
        let close = CloseFrame {
            code: CloseCode::Normal,
            reason: "bye".into(),
        };
        let msg = Ok(WsMessage::Close(Some(close)));
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Admin(AdminWs::Close(Some(_)))));
    }

    #[test]
    fn test_ws_parser_error() {
        let msg = Err(WsError::ConnectionClosed);
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Admin(AdminWs::WsError(_))));
    }

    mod connect_with_timeout {
        use super::*;
        use tokio::net::TcpListener;

        /// A local endpoint whose server runs `serve` on the first accepted TCP connection.
        async fn endpoint<F, Fut>(serve: F) -> String
        where
            F: FnOnce(TcpStream) -> Fut + Send + 'static,
            Fut: Future<Output = ()> + Send,
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                serve(stream).await;
            });
            format!("ws://{address}")
        }

        #[tokio::test]
        async fn a_peer_that_never_completes_the_upgrade_times_out() {
            // Accepts the TCP connection, then holds it open without answering the upgrade.
            let url = endpoint(|stream| async move {
                let _held = stream;
                std::future::pending::<()>().await;
            })
            .await;
            let timeout = Duration::from_millis(200);

            let result = connect_with_timeout(url, timeout).await;

            assert!(
                matches!(result, Err(SocketError::ConnectTimeout { timeout: t }) if t == timeout),
                "{result:?}"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn connect_gives_up_after_the_default_connect_timeout() {
            let url = endpoint(|stream| async move {
                let _held = stream;
                std::future::pending::<()>().await;
            })
            .await;
            let started = tokio::time::Instant::now();

            let result = connect(url).await;

            assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
            assert!(
                matches!(
                    result,
                    Err(SocketError::ConnectTimeout { timeout }) if timeout == DEFAULT_CONNECT_TIMEOUT
                ),
                "{result:?}"
            );
            assert_eq!(started.elapsed(), DEFAULT_CONNECT_TIMEOUT);
        }

        #[tokio::test]
        async fn a_completed_upgrade_returns_the_connection() {
            let url = endpoint(|stream| async move {
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Keep the server side open until the client is done with it.
                let _ = futures::StreamExt::next(&mut websocket).await;
            })
            .await;

            let result = connect_with_timeout(url, Duration::from_secs(10)).await;

            assert!(result.is_ok(), "{result:?}");
        }

        #[tokio::test]
        async fn a_refused_upgrade_is_a_websocket_error_not_a_timeout() {
            // Answers the upgrade with a plain HTTP error, so the handshake fails at once.
            let url = endpoint(|mut stream| async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                    .await;
            })
            .await;

            let result = connect_with_timeout(url, Duration::from_secs(10)).await;

            assert!(
                matches!(result, Err(SocketError::WebSocket(_))),
                "{result:?}"
            );
        }
    }
}
