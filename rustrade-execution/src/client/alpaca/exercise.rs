//! Do-not-exercise instructions: [`OptionExerciseClient`] for [`AlpacaClient`].

use super::{
    AlpacaApiError, AlpacaClient, SendFailure, connectivity_err, instrument_invalid, occ_expiry,
    parse_api_error, rest_url, send_rate_limited, transport_error, upper_symbol, with_alpaca_code,
};
use crate::{
    client::OptionExerciseClient,
    error::{ApiError, ExerciseRefusal, UnindexedClientError},
    exercise::{ExerciseAck, ExerciseInstruction, ExerciseQuantity},
};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone as _, Utc};
use chrono_tz::America::New_York;
use rustrade_instrument::instrument::name::InstrumentNameExchange;

/// Alpaca's code for a do-not-exercise refused because the account is short the contract.
const SHORT_POSITION: i64 = 40_310_001;

/// Alpaca's generic code for a request refused on a business rule. On the do-not-exercise
/// endpoint it is documented for "no available position for the specified contract", and sent
/// too when the instruction is outside its window (see [`EXPIRY_DAY_ONLY`] and
/// [`NOT_ACCEPTED_AFTER`]). Elsewhere Alpaca sends it for other refusals, such as an account
/// restricted to closing trades. Only the message tells them apart.
const REFUSED: i64 = 40_310_000;

/// Alpaca's code for a symbol it does not know.
const INVALID_SYMBOL: i64 = 42_210_000;

/// How Alpaca's message begins a refusal on a day other than the expiry day, lower-cased: "dne
/// requests are only accepted on the expiration day of the option contract".
const EXPIRY_DAY_ONLY: &str = "only accepted on the expiration day";

/// What precedes the time in Alpaca's refusal after the expiry day's deadline, lower-cased: "dne
/// requests are not accepted after 5:15PM".
const NOT_ACCEPTED_AFTER: &str = "not accepted after";

impl OptionExerciseClient for AlpacaClient {
    /// Sends `POST /v2/positions/{symbol}/do-not-exercise` for the option `instrument`: one
    /// request, with no body, naming the contract by its OCC symbol upper-cased, as this client
    /// matches names ignoring case. Alpaca answers an accepted instruction with an empty 200.
    ///
    /// # When Alpaca takes it
    /// - **Whole positions only.** The endpoint takes no quantity, so
    ///   [`ExerciseQuantity::Contracts`] is refused without sending anything.
    /// - **Only on the expiration day, until 5:15 PM New York time.** Alpaca's API reference
    ///   states no window, and its FAQ asks for the instruction before 3:00 PM ET. Its paper API,
    ///   observed in November 2026, refuses one sent on any other day, before or after, with
    ///   [`ExerciseRefusal::NotExpiryDay`], and one sent on the day after 5:15 PM with
    ///   [`ExerciseRefusal::PastDeadline`]. It judges the day before it looks at the position, so
    ///   [`ExerciseRefusal::NoPosition`] comes only on the expiration day. This client checks
    ///   neither rule itself: it reports what Alpaca answers.
    /// - **Not documented:** what a sale or a purchase of the contract after the instruction does
    ///   to it; whether it keeps Alpaca from selling an expiring long that the account could not
    ///   afford to exercise, as Alpaca otherwise may on the expiration day; and what Alpaca
    ///   answers a repeat of an accepted instruction with.
    ///
    /// # Errors
    /// - [`ApiError::InstrumentInvalid`], without sending anything, for a name that is not an
    ///   OCC option symbol, and for a 422 with Alpaca's code `42210000`, a symbol Alpaca does not
    ///   know.
    /// - [`ApiError::ExerciseRefused`] with:
    ///   - [`ExerciseRefusal::QuantityUnsupported`], without sending anything, for
    ///     [`ExerciseQuantity::Contracts`];
    ///   - [`ExerciseRefusal::ShortPosition`] for a 403 with code `40310001`;
    ///   - for a 403 with Alpaca's generic code `40310000`, by its message, as nothing else tells
    ///     these apart: [`ExerciseRefusal::NotExpiryDay`], dated from the OCC symbol, when it
    ///     says the instruction is accepted only on the expiration day;
    ///     [`ExerciseRefusal::PastDeadline`] when it says the instruction is not accepted after a
    ///     time, which is read as that time on the expiration day in New York;
    ///     [`ExerciseRefusal::NoPosition`] when it speaks of a position; and otherwise
    ///     [`ExerciseRefusal::Other`];
    ///   - [`ExerciseRefusal::Other`] for any other 4xx, with Alpaca's code and message.
    /// - [`ApiError::Unauthenticated`] for a 401, or a 403 without a code: a wrong key, secret or
    ///   host.
    /// - [`ApiError::RateLimit`] once the 429 retries are exhausted. A 429 means Alpaca did not
    ///   take the request, so it is retried, as every request of this client is.
    /// - [`ClientError::Connectivity`] for a request that failed in transport, ran past
    ///   [`AlpacaConfig::request_timeout`], met a 5xx, or whose error body could not be read.
    ///   Alpaca may have taken the instruction; it is not sent again.
    ///
    /// [`ClientError::Connectivity`]: crate::error::ClientError::Connectivity
    /// [`AlpacaConfig::request_timeout`]: super::AlpacaConfig::request_timeout
    async fn instruct_exercise(
        &self,
        instrument: &InstrumentNameExchange,
        instruction: ExerciseInstruction,
    ) -> Result<ExerciseAck, UnindexedClientError> {
        let symbol = upper_symbol(instrument.name().as_str());
        let Some(expiry_day) = occ_expiry(&symbol) else {
            return Err(instrument_invalid(
                instrument,
                "Alpaca takes a do-not-exercise instruction only for an option contract, named \
                 by its OCC symbol",
            ));
        };
        let ExerciseInstruction::Lapse { contracts } = instruction;
        if let ExerciseQuantity::Contracts(_) = contracts {
            return Err(UnindexedClientError::Api(ApiError::ExerciseRefused(
                ExerciseRefusal::QuantityUnsupported,
            )));
        }

        let url = rest_url(
            self.base_url(),
            &["v2", "positions", &symbol, "do-not-exercise"],
        )?;
        let response = send_rate_limited(
            &self.rate_limiter,
            // `Content-Length: 0` stated, as HTTP/1.1 asks of a POST without content: the
            // transport sends none for an empty body, and some servers refuse a POST without it.
            || {
                self.http
                    .post(url.clone())
                    .header(reqwest::header::CONTENT_LENGTH, 0)
            },
            "Alpaca do-not-exercise request",
        )
        .await
        .map_err(|failure| match failure {
            SendFailure::Transport(error) => UnindexedClientError::Connectivity(transport_error(
                "Alpaca do-not-exercise request failed",
                &error,
            )),
            SendFailure::RateLimited(wait) => {
                UnindexedClientError::Api(ApiError::rate_limited(wait))
            }
        })?;

        let status = response.status();
        if status.is_success() {
            return Ok(ExerciseAck::new(instruction));
        }
        let bytes = response.bytes().await.map_err(|error| {
            UnindexedClientError::Connectivity(transport_error(
                "Alpaca do-not-exercise read body failed",
                &error,
            ))
        })?;
        let body = AlpacaApiError::from_body(&bytes);
        Err(exercise_error(
            instrument,
            expiry_day,
            status,
            body.code,
            &body.message,
        ))
    }
}

/// The error for Alpaca's answer `status` to a do-not-exercise for `instrument`, which expires
/// on `expiry_day`, other than a success or a 429, with the body's `code` and `message`: see
/// [`AlpacaClient::instruct_exercise`](OptionExerciseClient::instruct_exercise)'s `# Errors`.
fn exercise_error(
    instrument: &InstrumentNameExchange,
    expiry_day: NaiveDate,
    status: reqwest::StatusCode,
    code: Option<i64>,
    message: &str,
) -> UnindexedClientError {
    let refused = |refusal| UnindexedClientError::Api(ApiError::ExerciseRefused(refusal));
    match (status.as_u16(), code) {
        (403, Some(SHORT_POSITION)) => refused(ExerciseRefusal::ShortPosition),
        (403, Some(REFUSED)) => refused(generic_refusal(expiry_day, code, message)),
        (422, Some(INVALID_SYMBOL)) => UnindexedClientError::Api(ApiError::InstrumentInvalid(
            instrument.clone(),
            with_alpaca_code(code, message),
        )),
        (401, _) | (403, None) => UnindexedClientError::Api(parse_api_error(status, code, message)),
        _ if status.is_client_error() => {
            refused(ExerciseRefusal::Other(with_alpaca_code(code, message)))
        }
        _ => connectivity_err(format!(
            "Alpaca do-not-exercise error {status}: {}",
            with_alpaca_code(code, message)
        )),
    }
}

/// What a 403 with Alpaca's generic code [`REFUSED`] refuses, read from its `message`, for an
/// option expiring on `expiry_day`.
fn generic_refusal(expiry_day: NaiveDate, code: Option<i64>, message: &str) -> ExerciseRefusal {
    let lower = message.to_ascii_lowercase();
    if lower.contains(EXPIRY_DAY_ONLY) {
        ExerciseRefusal::NotExpiryDay { expiry_day }
    } else if let Some(deadline) = stated_deadline(&lower, expiry_day) {
        ExerciseRefusal::PastDeadline { deadline }
    } else if lower.contains("position") {
        ExerciseRefusal::NoPosition
    } else {
        ExerciseRefusal::Other(with_alpaca_code(code, message))
    }
}

/// The deadline a lower-cased refusal such as "dne requests are not accepted after 5:15pm"
/// states: that time on `expiry_day` in New York, where Alpaca keeps its expiry-day times. The
/// time may be in 12-hour form, as Alpaca writes it, or 24-hour, with or without spaces. `None`
/// when the message states no time this reads.
fn stated_deadline(lower: &str, expiry_day: NaiveDate) -> Option<DateTime<Utc>> {
    let (_, after) = lower.split_once(NOT_ACCEPTED_AFTER)?;
    let stated: String = after.chars().filter(|c| !c.is_whitespace()).collect();
    let stated = stated.trim_end_matches('.');
    let time = NaiveTime::parse_from_str(stated, "%I:%M%p")
        .or_else(|_| NaiveTime::parse_from_str(stated, "%H:%M"))
        .ok()?;
    New_York
        .from_local_datetime(&expiry_day.and_time(time))
        .single()
        .map(|deadline| deadline.with_timezone(&Utc))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        client::{ExecutionClient, alpaca::AlpacaConfig},
        error::{ConnectivityError, UnindexedApiError},
    };
    use rust_decimal_macros::dec;
    use std::time::Duration;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    const CONTRACT: &str = "SPY261218C00600000";
    const LAPSE: ExerciseInstruction = ExerciseInstruction::Lapse {
        contracts: ExerciseQuantity::WholePosition,
    };

    fn client_for(server: &MockServer) -> AlpacaClient {
        AlpacaClient::new(AlpacaConfig::with_base_url(
            "test-key".into(),
            "test-secret".into(),
            server.uri(),
        ))
    }

    fn dne_path(symbol: &str) -> String {
        format!("/v2/positions/{symbol}/do-not-exercise")
    }

    async fn requests(server: &MockServer) -> Vec<wiremock::Request> {
        server.received_requests().await.unwrap_or_default()
    }

    fn api_error(result: Result<ExerciseAck, UnindexedClientError>) -> UnindexedApiError {
        match result {
            Err(UnindexedClientError::Api(error)) => error,
            other => panic!("expected an API error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_accepted_instruction_is_one_empty_post_naming_the_contract_upper_cased() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ack = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new("spy261218c00600000"), LAPSE)
            .await;

        assert_eq!(ack, Ok(ExerciseAck::new(LAPSE)));
        let requests = requests(&server).await;
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(requests[0].body.is_empty(), "{:?}", requests[0].body);
        assert_eq!(
            requests[0]
                .headers
                .get(reqwest::header::CONTENT_LENGTH)
                .map(|value| value.as_bytes()),
            Some(b"0".as_slice()),
            "an empty POST states its length, as HTTP/1.1 asks of a client"
        );
    }

    #[tokio::test]
    async fn a_success_with_a_body_is_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let ack = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), LAPSE)
            .await;

        assert_eq!(ack, Ok(ExerciseAck::new(LAPSE)));
    }

    #[tokio::test]
    async fn a_quantity_is_refused_unsent() {
        let server = MockServer::start().await;
        let instruction = ExerciseInstruction::Lapse {
            contracts: ExerciseQuantity::Contracts(dec!(1)),
        };

        let result = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), instruction)
            .await;

        assert_eq!(
            api_error(result),
            ApiError::ExerciseRefused(ExerciseRefusal::QuantityUnsupported)
        );
        assert!(requests(&server).await.is_empty());
    }

    #[tokio::test]
    async fn a_name_that_is_not_an_option_is_refused_unsent() {
        let server = MockServer::start().await;
        let client = client_for(&server);

        // The last is shaped as a contract, with a thirteenth month.
        for name in [
            "SPY",
            "BTC/USD",
            "",
            "..",
            "SPY261218X00600000",
            "SPY261318C00600000",
        ] {
            let result = client
                .instruct_exercise(&InstrumentNameExchange::new(name), LAPSE)
                .await;

            assert!(
                matches!(api_error(result), ApiError::InstrumentInvalid(ref instrument, _)
                    if instrument.name().as_str() == name),
                "{name:?}"
            );
        }
        assert!(requests(&server).await.is_empty());
    }

    /// A non-option name is refused before a quantity is looked at, so the error names what is
    /// wrong with the instrument.
    #[tokio::test]
    async fn a_name_that_is_not_an_option_is_refused_before_a_quantity() {
        let server = MockServer::start().await;
        let instruction = ExerciseInstruction::Lapse {
            contracts: ExerciseQuantity::Contracts(dec!(1)),
        };

        let result = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new("SPY"), instruction)
            .await;

        assert!(
            matches!(api_error(result), ApiError::InstrumentInvalid(..)),
            "the instrument is checked first"
        );
    }

    #[tokio::test]
    async fn a_refusal_reaches_the_caller_by_its_code() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "code": 40310001,
                "message": "cannot submit DNE for short position",
            })))
            .mount(&server)
            .await;

        let result = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), LAPSE)
            .await;

        assert_eq!(
            api_error(result),
            ApiError::ExerciseRefused(ExerciseRefusal::ShortPosition)
        );
    }

    #[tokio::test]
    async fn a_refusal_outside_the_expiry_day_is_dated_from_the_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "code": 40310000,
                "message": "dne requests are only accepted on the expiration day of the option \
                            contract",
            })))
            .mount(&server)
            .await;

        let result = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), LAPSE)
            .await;

        assert_eq!(
            api_error(result),
            ApiError::ExerciseRefused(ExerciseRefusal::NotExpiryDay {
                expiry_day: NaiveDate::from_ymd_opt(2026, 12, 18).unwrap()
            })
        );
    }

    #[test]
    fn a_stated_deadline_is_that_time_on_the_expiry_day_in_new_york() {
        let day = |m, d| NaiveDate::from_ymd_opt(2026, m, d).unwrap();
        let utc = |text: &str| text.parse::<DateTime<Utc>>().ok();

        assert_eq!(
            stated_deadline("dne requests are not accepted after 5:15pm", day(11, 27)),
            utc("2026-11-27T22:15:00Z"),
            "winter time, UTC-5"
        );
        assert_eq!(
            stated_deadline("dne requests are not accepted after 5:15pm", day(6, 19)),
            utc("2026-06-19T21:15:00Z"),
            "summer time, UTC-4"
        );
        assert_eq!(
            stated_deadline("not accepted after 1:30 pm.", day(11, 27)),
            utc("2026-11-27T18:30:00Z"),
            "spaces and a full stop"
        );
        assert_eq!(
            stated_deadline("not accepted after 17:15", day(11, 27)),
            utc("2026-11-27T22:15:00Z"),
            "24-hour form"
        );
        assert_eq!(
            stated_deadline("not accepted after 9:05am", day(11, 27)),
            utc("2026-11-27T14:05:00Z"),
            "a morning time"
        );
        for no_time in [
            "not accepted after the close",
            "not accepted after",
            "accepted after 5:15pm",
            "not accepted after 25:00",
        ] {
            assert_eq!(stated_deadline(no_time, day(11, 27)), None, "{no_time}");
        }
    }

    #[tokio::test]
    async fn a_429_is_retried_and_its_success_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ack = client_for(&server)
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), LAPSE)
            .await;

        assert_eq!(ack, Ok(ExerciseAck::new(LAPSE)));
        assert_eq!(requests(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn a_request_that_times_out_is_a_connectivity_error_sent_once() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(dne_path(CONTRACT)))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let client = AlpacaClient::new(
            AlpacaConfig::with_base_url("test-key".into(), "test-secret".into(), server.uri())
                .with_request_timeout(Duration::from_millis(200)),
        );

        let result = client
            .instruct_exercise(&InstrumentNameExchange::new(CONTRACT), LAPSE)
            .await;

        assert!(
            matches!(
                result,
                Err(UnindexedClientError::Connectivity(
                    ConnectivityError::Timeout
                ))
            ),
            "{result:?}"
        );
        assert_eq!(requests(&server).await.len(), 1, "not retried");
    }

    #[test]
    fn each_answer_maps_to_its_error() {
        use reqwest::StatusCode;
        let instrument = InstrumentNameExchange::new(CONTRACT);
        let map = |status: u16, code: Option<i64>, message: &str| {
            exercise_error(
                &instrument,
                NaiveDate::from_ymd_opt(2026, 12, 18).unwrap(),
                StatusCode::from_u16(status).unwrap(),
                code,
                message,
            )
        };
        let refused = |refusal| UnindexedClientError::Api(ApiError::ExerciseRefused(refusal));

        assert_eq!(
            map(
                403,
                Some(40310000),
                "no available position for the specified contract"
            ),
            refused(ExerciseRefusal::NoPosition)
        );
        assert_eq!(
            map(403, Some(40310000), "No Position Found"),
            refused(ExerciseRefusal::NoPosition),
            "the message is matched ignoring case"
        );
        assert_eq!(
            map(403, Some(40310001), "cannot submit DNE for short position"),
            refused(ExerciseRefusal::ShortPosition)
        );
        assert_eq!(
            map(
                403,
                Some(40310000),
                "dne requests are only accepted on the expiration day of the option contract"
            ),
            refused(ExerciseRefusal::NotExpiryDay {
                expiry_day: NaiveDate::from_ymd_opt(2026, 12, 18).unwrap()
            }),
            "dated from the contract"
        );
        assert_eq!(
            map(
                403,
                Some(40310000),
                "dne requests are not accepted after 5:15PM"
            ),
            refused(ExerciseRefusal::PastDeadline {
                deadline: "2026-12-18T22:15:00Z".parse().unwrap()
            }),
            "17:15 New York on the expiry day, in winter time"
        );
        assert_eq!(
            map(
                403,
                Some(40310000),
                "dne requests are not accepted after the close"
            ),
            refused(ExerciseRefusal::Other(
                "40310000 dne requests are not accepted after the close".into()
            )),
            "a deadline without a time this reads"
        );
        assert_eq!(
            map(
                403,
                Some(40310000),
                "account is restricted to liquidation only"
            ),
            refused(ExerciseRefusal::Other(
                "40310000 account is restricted to liquidation only".into()
            )),
            "Alpaca's generic code without a position in its message"
        );
        assert_eq!(
            map(403, Some(40310002), "no available position"),
            refused(ExerciseRefusal::Other(
                "40310002 no available position".into()
            )),
            "only the documented code reads as no position"
        );
        assert_eq!(
            map(422, Some(42210000), "invalid symbol"),
            UnindexedClientError::Api(ApiError::InstrumentInvalid(
                instrument.clone(),
                "42210000 invalid symbol".into()
            ))
        );
        assert_eq!(
            map(
                422,
                Some(42210001),
                "dne requests are not accepted after 15:30"
            ),
            refused(ExerciseRefusal::Other(
                "42210001 dne requests are not accepted after 15:30".into()
            ))
        );
        assert_eq!(
            map(404, None, "position does not exist"),
            refused(ExerciseRefusal::Other("position does not exist".into()))
        );
        assert!(
            matches!(
                map(403, None, "forbidden."),
                UnindexedClientError::Api(ApiError::Unauthenticated(_))
            ),
            "a 403 without a code is a wrong key, secret or host"
        );
        assert!(
            matches!(
                map(401, Some(40110000), "request is not authorized"),
                UnindexedClientError::Api(ApiError::Unauthenticated(_))
            ),
            "a 401 is a wrong key, secret or host, whatever its code"
        );
        assert!(
            matches!(
                map(500, Some(50010000), "internal server error"),
                UnindexedClientError::Connectivity(ConnectivityError::Socket(ref message))
                    if message.contains("50010000 internal server error")
            ),
            "a 5xx leaves the outcome unknown"
        );
    }
}
