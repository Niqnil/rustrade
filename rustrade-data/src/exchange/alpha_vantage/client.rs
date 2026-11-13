//! The HTTP client for Alpha Vantage's earnings endpoints.

use super::{error::AlphaVantageError, records};
use crate::{
    earnings::EarningsRelease,
    exchange::{
        http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, http_base_url, read_body_capped},
        pacer::Pacer,
    },
};
use chrono::Utc;
use std::{num::NonZeroU32, sync::Arc, time::Duration};

/// Where Alpha Vantage serves its API.
pub const DEFAULT_BASE_URL: &str = "https://www.alphavantage.co";

/// The client's default pace: 50 requests a minute, one every 1.2 seconds, under a free key's
/// limit of one a second.
const DEFAULT_REQUESTS_PER_MINUTE: NonZeroU32 = match NonZeroU32::new(50) {
    Some(rate) => rate,
    None => unreachable!(),
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// What the API key is replaced with wherever it would appear in an error.
const KEY_PLACEHOLDER: &str = "<api key>";

/// How far ahead [`AlphaVantageClient::earnings_calendar`] looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CalendarHorizon {
    /// The next three months (Alpha Vantage's default).
    #[default]
    ThreeMonths,
    /// The next six months.
    SixMonths,
    /// The next twelve months.
    TwelveMonths,
}

impl CalendarHorizon {
    fn as_param(self) -> &'static str {
        match self {
            Self::ThreeMonths => "3month",
            Self::SixMonths => "6month",
            Self::TwelveMonths => "12month",
        }
    }
}

/// A client for Alpha Vantage's earnings calendar and earnings history.
///
/// See the [module documentation](super) for what each returns and the terms the data comes
/// under.
///
/// # Request rate
/// A free key allows one request a second and 25 a day; premium plans allow more a minute. This
/// client spaces its requests evenly, 50 a minute by default
/// ([`with_requests_per_minute`](Self::with_requests_per_minute)). Clones share the pace;
/// separate clients, or other programs using the same key, do not. The daily allowance is not
/// tracked: a request over it is answered [`AlphaVantageError::Refused`].
///
/// # Errors and retries
/// The client does not retry; see [`AlphaVantageError::is_transient`].
#[derive(Clone)]
pub struct AlphaVantageClient {
    http: reqwest::Client,
    base_url: String,
    /// Sent as the `apikey` query parameter. `Debug` leaves it out, and every error has it
    /// removed.
    api_key: String,
    pacer: Arc<Pacer>,
}

impl AlphaVantageClient {
    /// A client sending `api_key` with every request, to Alpha Vantage's own server.
    ///
    /// The key is refused before any request is sent if it is empty or holds whitespace or
    /// control characters.
    pub fn new(api_key: &str) -> Result<Self, AlphaVantageError> {
        let api_key = api_key.trim();
        if api_key.is_empty() || api_key.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(AlphaVantageError::invalid_input(
                "the API key is empty or holds whitespace or control characters",
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| {
                AlphaVantageError::invalid_input(format!("building the HTTP client: {e}"))
            })?;
        Ok(Self {
            http,
            base_url: DEFAULT_BASE_URL.to_owned(),
            api_key: api_key.to_owned(),
            pacer: Arc::new(Pacer::new(interval(DEFAULT_REQUESTS_PER_MINUTE))),
        })
    }

    /// The same client sending its requests to `base_url` in place of [`DEFAULT_BASE_URL`],
    /// such as a test server.
    ///
    /// The URL must be `http` or `https`; a trailing `/` is ignored. Clones keep sharing the
    /// pace.
    pub fn with_base_url(self, base_url: &str) -> Result<Self, AlphaVantageError> {
        Ok(Self {
            base_url: http_base_url(base_url).map_err(AlphaVantageError::invalid_input)?,
            ..self
        })
    }

    /// The same client sending at most `rate` requests a minute, for a premium plan's limit. A
    /// fresh pace: clones made before this call keep sharing the old one.
    pub fn with_requests_per_minute(self, rate: NonZeroU32) -> Self {
        Self {
            pacer: Arc::new(Pacer::new(interval(rate))),
            ..self
        }
    }

    /// `symbol`'s past quarterly earnings releases, oldest first. One request.
    ///
    /// A record is [`ReleaseStatus::Reported`] and public by the session bound of its date:
    /// the regular session's open (09:30 New York) for a release before the open, else the end
    /// of its date (00:00 New York the next day), the latest the date allows. A row whose bound
    /// has not passed when fetched (dated later, or dated that day and not yet past its bound) is
    /// not known to have happened: [`ReleaseStatus::Unknown`], known when fetched. So no record is
    /// known later than it was fetched. The records keep `symbol` as given.
    ///
    /// # Errors
    /// - [`AlphaVantageError::InvalidInput`] if `symbol` is empty.
    /// - [`AlphaVantageError::NotFound`] if Alpha Vantage has no earnings for `symbol`.
    /// - [`AlphaVantageError::Refused`] if Alpha Vantage refused the request.
    /// - Any other request error.
    ///
    /// [`ReleaseStatus::Reported`]: crate::earnings::ReleaseStatus::Reported
    /// [`ReleaseStatus::Unknown`]: crate::earnings::ReleaseStatus::Unknown
    pub async fn earnings_history(
        &self,
        symbol: &str,
    ) -> Result<Vec<EarningsRelease>, AlphaVantageError> {
        let symbol = checked_symbol(symbol)?;
        let body = self
            .get(&[("function", "EARNINGS"), ("symbol", symbol)])
            .await?;
        let observed_at = Utc::now();
        if let Some(error) = records::notice(&body) {
            return Err(error);
        }
        records::history(&body, symbol, observed_at)
    }

    /// The releases Alpha Vantage expects within `horizon`: of `symbol` only, or of every company
    /// it covers when `symbol` is `None`. One request. By date, then symbol.
    ///
    /// Each record is [`ReleaseStatus::Unknown`], since the calendar does not say whether a date
    /// is announced or estimated, and is known when fetched. The records keep the symbols as
    /// Alpha Vantage spells them. The calendar lists only releases Alpha Vantage has a date for,
    /// so an empty result, or a missing company, means no date yet, not no release.
    ///
    /// # Errors
    /// - [`AlphaVantageError::InvalidInput`] if `symbol` is empty.
    /// - [`AlphaVantageError::Refused`] if Alpha Vantage refused the request.
    /// - Any other request error.
    ///
    /// [`ReleaseStatus::Unknown`]: crate::earnings::ReleaseStatus::Unknown
    pub async fn earnings_calendar(
        &self,
        symbol: Option<&str>,
        horizon: CalendarHorizon,
    ) -> Result<Vec<EarningsRelease>, AlphaVantageError> {
        let mut params = vec![
            ("function", "EARNINGS_CALENDAR"),
            ("horizon", horizon.as_param()),
        ];
        if let Some(symbol) = symbol {
            params.push(("symbol", checked_symbol(symbol)?));
        }
        let body = self.get(&params).await?;
        let observed_at = Utc::now();
        records::calendar(&body, observed_at)
    }

    /// The body of a successful `GET /query` with `params` and the API key, without the key.
    ///
    /// Every error message is built from this body, or from an error body or transport error
    /// scrubbed here, so none holds the key. Scrubbing comes before any message is capped, which
    /// could otherwise cut the key and leave a part of it that no longer matches.
    async fn get(&self, params: &[(&str, &str)]) -> Result<String, AlphaVantageError> {
        self.pacer.wait().await;
        // `without_url`: the URL carries the API key in its query.
        let transport = |e: reqwest::Error| AlphaVantageError::Http {
            message: self.scrubbed(e.without_url().to_string()),
        };
        let response = self
            .http
            .get(format!("{}/query", self.base_url))
            .query(params)
            .query(&[("apikey", self.api_key.as_str())])
            .send()
            .await
            .map_err(transport)?;
        let status = response.status();
        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES)
                .await
                .unwrap_or_default();
            return Err(AlphaVantageError::Api {
                status: status.as_u16(),
                message: super::error::cap(&self.scrubbed(body)),
            });
        }
        let body = response.text().await.map_err(transport)?;
        Ok(self.scrubbed(body))
    }

    /// `text` with the API key replaced wherever it appears.
    fn scrubbed(&self, text: String) -> String {
        if text.contains(&self.api_key) {
            text.replace(&self.api_key, KEY_PLACEHOLDER)
        } else {
            text
        }
    }
}

impl std::fmt::Debug for AlphaVantageClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlphaVantageClient")
            .field("base_url", &self.base_url)
            .field("pacer", &self.pacer)
            .finish_non_exhaustive()
    }
}

fn checked_symbol(symbol: &str) -> Result<&str, AlphaVantageError> {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return Err(AlphaVantageError::invalid_input("the symbol is empty"));
    }
    Ok(symbol)
}

/// The time between requests at `rate` a minute.
fn interval(rate: NonZeroU32) -> Duration {
    Duration::from_secs(60) / rate.get()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn a_key_must_be_present_and_plain() {
        assert!(AlphaVantageClient::new(" KEY123 ").is_ok());
        for bad in ["", "   ", "KEY 123", "KEY\u{7}"] {
            assert!(
                matches!(
                    AlphaVantageClient::new(bad),
                    Err(AlphaVantageError::InvalidInput { .. })
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn debug_leaves_the_key_out() {
        let client = AlphaVantageClient::new("SECRETKEY1234567").unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains("SECRETKEY1234567"), "{debug}");
        assert!(debug.starts_with("AlphaVantageClient"), "{debug}");
        assert!(debug.contains(DEFAULT_BASE_URL), "{debug}");
    }

    #[test]
    fn the_pace_defaults_to_fifty_a_minute_and_takes_a_premium_rate() {
        let client = AlphaVantageClient::new("KEY").unwrap();
        assert_eq!(client.pacer.interval(), Duration::from_millis(1200));
        let premium = client
            .clone()
            .with_requests_per_minute(NonZeroU32::new(75).unwrap());
        assert_eq!(premium.pacer.interval(), Duration::from_millis(800));
        assert!(!Arc::ptr_eq(&premium.pacer, &client.pacer));
    }

    #[test]
    fn a_base_url_must_be_http() {
        let client = AlphaVantageClient::new("KEY").unwrap();
        let local = client.clone().with_base_url("http://127.0.0.1:1/").unwrap();
        assert_eq!(local.base_url, "http://127.0.0.1:1");
        for bad in ["ftp://alphavantage.co", "not a url"] {
            assert!(matches!(
                client.clone().with_base_url(bad),
                Err(AlphaVantageError::InvalidInput { .. })
            ));
        }
    }

    #[test]
    fn scrubbing_replaces_every_copy_of_the_key() {
        let client = AlphaVantageClient::new("KEY123").unwrap();
        assert_eq!(
            client.scrubbed("KEY123 bad key KEY123 here".to_owned()),
            "<api key> bad key <api key> here"
        );
        assert_eq!(client.scrubbed("no key".to_owned()), "no key");
    }

    #[test]
    fn horizons_name_alpha_vantages_parameter() {
        assert_eq!(CalendarHorizon::default(), CalendarHorizon::ThreeMonths);
        assert_eq!(CalendarHorizon::ThreeMonths.as_param(), "3month");
        assert_eq!(CalendarHorizon::SixMonths.as_param(), "6month");
        assert_eq!(CalendarHorizon::TwelveMonths.as_param(), "12month");
    }

    #[test]
    fn an_empty_symbol_is_refused() {
        assert!(matches!(
            checked_symbol("  "),
            Err(AlphaVantageError::InvalidInput { .. })
        ));
        assert_eq!(checked_symbol(" XYZ ").unwrap(), "XYZ");
    }
}
