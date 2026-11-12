//! Errors made by `binance-sdk` itself, for tests.
//!
//! The SDK's error variants are `#[non_exhaustive]`, so a test cannot build one. Each helper here
//! serves a canned answer to the SDK, on a local server and a runtime of its own, and returns the
//! error the SDK reads from it. A test of a classifier so also pins how the SDK maps an answer to
//! an error, which a version bump can change.

use binance_sdk::common::{
    config::{ConfigurationRestApi, ConfigurationWebsocketApi},
    errors::{ConnectorError, WebsocketError},
    utils::send_request,
    websocket::{WebsocketApi, WebsocketMessageSendOptions},
};
use futures::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_tungstenite::tungstenite::Message;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

/// The error the SDK returns for a REST answer of `status` with `body` and `headers`.
///
/// # Panics
/// If the SDK reads the answer as a success, or fails with something other than a
/// [`ConnectorError`].
pub(crate) fn rest_error(
    status: u16,
    body: impl AsRef<[u8]>,
    headers: &[(&str, &str)],
) -> ConnectorError {
    let body = body.as_ref().to_vec();
    on_own_runtime(|| async {
        let server = MockServer::start().await;
        let mut answer = ResponseTemplate::new(status).set_body_bytes(body);
        for (name, value) in headers {
            answer = answer.insert_header(*name, *value);
        }
        Mock::given(any()).respond_with(answer).mount(&server).await;
        post(&server.uri()).await
    })
}

/// The error the SDK returns for a REST answer of `status` whose body is Binance's error object
/// with `code`, when there is one, and `msg`.
pub(crate) fn rest_refusal(status: u16, code: Option<i64>, msg: &str) -> ConnectorError {
    let body = match code {
        Some(code) => json!({ "code": code, "msg": msg }),
        None => json!({ "msg": msg }),
    };
    rest_error(status, body.to_string(), &[])
}

/// The error the SDK returns when a REST request does not complete: here, the connection is
/// refused.
pub(crate) fn rest_transport_error() -> ConnectorError {
    on_own_runtime(|| async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let url = format!("http://{}", listener.local_addr().expect("local address"));
        drop(listener);
        post(&url).await
    })
}

/// The error the SDK returns when a REST answer's body ends before the length its header
/// announced.
pub(crate) fn rest_truncated_error() -> ConnectorError {
    on_own_runtime(|| async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let url = format!("http://{}", listener.local_addr().expect("local address"));
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("a connection");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n{}")
                .await;
        });
        post(&url).await
    })
}

/// The error the SDK returns when a successful REST answer's body does not decode into the type
/// asked for.
pub(crate) fn rest_decode_error() -> ConnectorError {
    on_own_runtime(|| async {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        let answer = send_request::<Vec<u64>>(
            &rest_config(&server.uri()),
            "/test",
            reqwest::Method::POST,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            false,
        )
        .await
        .expect("a successful answer");
        answer
            .data()
            .await
            .expect_err("an object does not decode as a sequence")
    })
}

/// The error the SDK returns for a WS-API answer of `status` carrying `error`, Binance's error
/// object (`code`, `msg`, and optionally `data`).
///
/// # Panics
/// If the SDK reads the answer as a success.
pub(crate) fn ws_error(status: u16, error: Value) -> WebsocketError {
    on_own_runtime(|| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let url = format!("ws://{}", listener.local_addr().expect("local address"));
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("a connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("a WebSocket handshake");
            while let Some(Ok(message)) = ws.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let request: Value = serde_json::from_str(&text).expect("a JSON request");
                let answer = json!({ "id": request["id"], "status": status, "error": error });
                ws.send(Message::text(answer.to_string()))
                    .await
                    .expect("send the answer");
            }
        });

        let config = ConfigurationWebsocketApi::builder()
            .api_key("key")
            .api_secret("secret")
            .ws_url(url)
            .build()
            .expect("a WS-API config");
        let ws = WebsocketApi::new(config, vec![]);
        Arc::clone(&ws).connect().await.expect("connect");
        match ws
            .send_message::<Value>("test", BTreeMap::new(), WebsocketMessageSendOptions::new())
            .await
        {
            Ok(_) => panic!("the SDK read a {status} answer as a success"),
            Err(e) => e,
        }
    })
}

/// The error the SDK returns for a WS-API refusal: status 400, Binance's `code` and `msg`.
pub(crate) fn ws_refusal(code: i64, msg: &str) -> WebsocketError {
    ws_error(400, json!({ "code": code, "msg": msg }))
}

/// A REST config whose requests go to `base`, each sent once.
fn rest_config(base: &str) -> ConfigurationRestApi {
    ConfigurationRestApi::builder()
        .api_key("key")
        .api_secret("secret")
        .base_path(base)
        .retries(0)
        .build()
        .expect("a REST config")
}

/// POST to `base` through the SDK, and return the error it fails with.
async fn post(base: &str) -> ConnectorError {
    match send_request::<Value>(
        &rest_config(base),
        "/test",
        reqwest::Method::POST,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
        false,
    )
    .await
    {
        Ok(answer) => panic!("the SDK read status {} as a success", answer.status),
        Err(e) => e
            .downcast::<ConnectorError>()
            .expect("the SDK fails a REST request with a ConnectorError"),
    }
}

/// Run the future `make` builds to completion on a runtime of its own, on a thread of its own, so
/// that a helper serves sync tests and async ones alike.
fn on_own_runtime<F, T>(make: impl FnOnce() -> F + Send) -> T
where
    F: Future<Output = T>,
    T: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a test runtime")
                    .block_on(make())
            })
            .join()
            .expect("the helper's thread panicked")
    })
}
