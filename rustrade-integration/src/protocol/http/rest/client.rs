use crate::{
    error::SocketError,
    metric::{Field, Metric, Tag},
    protocol::http::{BuildStrategy, HttpParser, rest::RestRequest},
};
use bytes::Bytes;
use chrono::Utc;
use std::borrow::Cow;

/// Configurable REST client capable of executing signed [`RestRequest`]s. Use this when
/// integrating APIs that require Http in order to interact with resources. Each API will require
/// a specific combination of [`Signer`](super::super::private::Signer), [`Mac`](hmac::Mac),
/// signature [`Encoder`](super::super::private::encoder::Encoder), and
/// [`HttpParser`].
#[derive(Debug)]
pub struct RestClient<'a, Strategy, Parser> {
    /// HTTP [`reqwest::Client`] for executing signed [`reqwest::Request`]s.
    pub http_client: reqwest::Client,

    /// Base Url of the API being interacted with.
    pub base_url: Cow<'a, str>,

    /// [`RestRequest`] build strategy for the API being interacted with that implements
    /// [`BuildStrategy`].
    ///
    /// An authenticated [`RestClient`] will utilise API specific
    /// [`Signer`](super::super::private::Signer) logic, a hashable [`Mac`](hmac::Mac), and a
    /// signature [`Encoder`](super::super::private::encoder::Encoder). Where as a non authorised
    /// [`RestRequest`] may add any mandatory `reqwest` headers that are required.
    pub strategy: Strategy,

    /// [`HttpParser`] that deserialises [`RestRequest::Response`]s, and upon failure parses
    /// API errors returned from the server.
    pub parser: Parser,
}

impl<Strategy, Parser> RestClient<'_, Strategy, Parser>
where
    Strategy: BuildStrategy,
    Parser: HttpParser,
{
    /// Execute the provided [`RestRequest`].
    pub async fn execute<Request>(
        &self,
        request: Request,
    ) -> Result<(Request::Response, Metric), Parser::OutputError>
    where
        Request: RestRequest,
    {
        // Use provided Request to construct a signed reqwest::Request
        let request = self.build(request)?;

        // Measure request execution
        let (status, payload, latency) = self.measured_execution::<Request>(request).await?;

        // Attempt to parse API Success or Error response
        self.parser
            .parse::<Request::Response>(status, &payload)
            .map(|response| (response, latency))
    }

    /// Use the provided [`RestRequest`] to construct a signed Http [`reqwest::Request`].
    pub fn build<Request>(&self, request: Request) -> Result<reqwest::Request, SocketError>
    where
        Request: RestRequest,
    {
        // Construct url
        let url = format!("{}{}", self.base_url, request.path());

        // Construct RequestBuilder with method & url
        let mut builder = self
            .http_client
            .request(Request::method(), url)
            .timeout(request.timeout());

        // Add optional query parameters
        if let Some(query_params) = request.query_params() {
            builder = builder.query(query_params);
        }

        // Add optional Body
        if let Some(body) = request.body() {
            builder = builder.json(body);
        }

        // Use RequestBuilder (public or private strategy) to build reqwest::Request
        self.strategy.build(request, builder)
    }

    /// Execute the built [`reqwest::Request`] using the [`reqwest::Client`].
    ///
    /// Measures and returns the Http request round trip duration.
    pub async fn measured_execution<Request>(
        &self,
        request: reqwest::Request,
    ) -> Result<(reqwest::StatusCode, Bytes, Metric), SocketError>
    where
        Request: RestRequest,
    {
        // Construct Http request duration Metric
        // timestamps are post-1970 (positive)
        #[allow(clippy::cast_sign_loss)]
        let time = Utc::now().timestamp_millis() as u64;
        let mut latency = Metric {
            name: "http_request_duration",
            time,
            tags: vec![
                Tag::new("http_method", Request::method().as_str()),
                Tag::new("base_url", self.base_url.as_ref()),
                Tag::new("path", request.url().path()),
            ],
            fields: Vec::with_capacity(1),
        };

        // Measure the HTTP request round trip duration
        let start = std::time::Instant::now();
        let response = self.http_client.execute(request).await?;
        // as_millis() returns u128; truncation impossible (u64::MAX ms ≈ 584M years)
        #[allow(clippy::cast_possible_truncation)]
        let duration = start.elapsed().as_millis() as u64;

        // Update Metric with response status and request duration
        latency
            .tags
            .push(Tag::new("status_code", response.status().as_str()));
        latency.fields.push(Field::new("duration", duration));

        // Extract Status Code & reqwest::Response Bytes
        let status_code = response.status();
        let payload = response.bytes().await?;

        Ok((status_code, payload, latency))
    }
}

impl<'a, Strategy, Parser> RestClient<'a, Strategy, Parser> {
    /// Construct a new [`Self`] using the provided configuration.
    pub fn new<Url: Into<Cow<'a, str>>>(base_url: Url, strategy: Strategy, parser: Parser) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            base_url: base_url.into(),
            strategy,
            parser,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::protocol::http::{HttpParser, public::PublicNoHeaders};
    use reqwest::StatusCode;
    use serde::Deserialize;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// `{"value":"decoded"}`, gzip-compressed with a zeroed mtime so the bytes are reproducible:
    /// `python3 -c 'import gzip; print(gzip.compress(b"{\"value\":\"decoded\"}", mtime=0))'`.
    const GZIPPED_BODY: &[u8] = &[
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xab, 0x56, 0x2a, 0x4b, 0xcc,
        0x29, 0x4d, 0x55, 0xb2, 0x52, 0x4a, 0x49, 0x4d, 0xce, 0x4f, 0x49, 0x4d, 0x51, 0xaa, 0x05,
        0x00, 0xba, 0xf3, 0x5c, 0x77, 0x13, 0x00, 0x00, 0x00,
    ];

    #[derive(Debug, Deserialize, PartialEq)]
    struct Payload {
        value: String,
    }

    struct GetPayload;

    impl RestRequest for GetPayload {
        type Response = Payload;
        type QueryParams = ();
        type Body = ();

        fn path(&self) -> Cow<'static, str> {
            Cow::Borrowed("/payload")
        }

        fn method() -> reqwest::Method {
            reqwest::Method::GET
        }
    }

    struct JsonParser;

    impl HttpParser for JsonParser {
        type ApiError = serde_json::Value;
        type OutputError = SocketError;

        fn parse_api_error(&self, status: StatusCode, error: Self::ApiError) -> Self::OutputError {
            SocketError::HttpResponse(status, error.to_string())
        }
    }

    #[tokio::test]
    async fn the_default_client_negotiates_and_decodes_gzip() {
        // Pins the workspace's reqwest `gzip` feature: without it the request advertises no
        // encoding and the compressed body reaches the parser undecoded.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/payload"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-encoding", "gzip")
                    .insert_header("content-type", "application/json")
                    .set_body_bytes(GZIPPED_BODY),
            )
            .mount(&server)
            .await;

        let client = RestClient::new(server.uri(), PublicNoHeaders, JsonParser);
        let (response, _) = client.execute(GetPayload).await.unwrap();

        assert_eq!(
            response,
            Payload {
                value: "decoded".to_owned()
            }
        );
        let requests = server.received_requests().await.unwrap();
        let accept_encoding = requests[0]
            .headers
            .get("accept-encoding")
            .and_then(|value| value.to_str().ok());
        assert!(
            accept_encoding.is_some_and(|value| value.contains("gzip")),
            "expected gzip in Accept-Encoding, sent {accept_encoding:?}"
        );
    }
}
