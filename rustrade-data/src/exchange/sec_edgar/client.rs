//! The HTTP client for EDGAR's submissions API and filing archive.

use super::{
    Cik,
    error::SecEdgarError,
    filings::{self, Columns, Releases, Submissions},
};
use crate::{
    earnings::{EarningsProvider, EarningsRelease, ReleaseSource, ReleaseStatus},
    exchange::{
        http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped},
        new_york,
    },
};
use chrono::{NaiveDate, TimeDelta, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, de::DeserializeOwned};
use smol_str::SmolStr;
use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc, time::Duration};
use tokio::time::Instant;

/// Where SEC serves its website and filing archive.
pub const DEFAULT_WWW_URL: &str = "https://www.sec.gov";

/// Where SEC serves its JSON APIs.
pub const DEFAULT_DATA_URL: &str = "https://data.sec.gov";

/// SEC's limit on automated requests per second, across all of a sender's clients.
pub const SEC_MAX_REQUESTS_PER_SECOND: u32 = 10;

/// The client's default pace: one request a second under SEC's limit.
const DEFAULT_REQUESTS_PER_SECOND: NonZeroU32 = match NonZeroU32::new(9) {
    Some(rate) => rate,
    None => unreachable!(),
};

/// How long a filing's date can trail the New York date of its acceptance, which dates its
/// release. EDGAR dates a filing accepted after 17:30 the next business day, so a weekend and a
/// holiday put up to four calendar days between them; the rest is margin for closures. Releases
/// whose 8-K is dated later than this after the requested range are not looked up.
const MAX_FILING_LAG: TimeDelta = TimeDelta::days(8);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// A client for SEC EDGAR: company tickers, and earnings releases from each company's filings.
///
/// # User-Agent
/// SEC requires every automated request to declare who sends it, in the User-Agent: a name (a
/// person's, a company's or an application's) and a contact email, such as
/// `"Sample Company Name admin@example.com"`. [`new`](Self::new) takes it from the caller and
/// sends it on every request. SEC refuses requests without a declaration it accepts
/// ([`SecEdgarError::Refused`]).
///
/// # Request rate
/// SEC allows at most [`SEC_MAX_REQUESTS_PER_SECOND`] requests a second from one sender, and
/// blocks a sender over it for a while ([`SecEdgarError::Refused`]). This client spaces its
/// requests evenly, at most 9 a second by default
/// ([`with_max_requests_per_second`](Self::with_max_requests_per_second)). Clones share the pace;
/// separate clients, or other programs sending under the same declaration, do not, and their
/// requests add up against SEC's limit.
///
/// # Errors and retries
/// The client does not retry; see [`SecEdgarError::is_transient`].
#[derive(Clone)]
pub struct SecEdgarClient {
    /// Holds the User-Agent as a default header. `Debug` leaves it out: `reqwest::Client` prints
    /// its default headers, which would put the contact email in logs.
    http: reqwest::Client,
    www_url: String,
    data_url: String,
    pacer: Arc<Pacer>,
}

impl SecEdgarClient {
    /// A client sending `user_agent` on every request, to SEC's own servers.
    ///
    /// `user_agent` must name the sender and give a contact email, separated by a space (see
    /// ["User-Agent"](Self#user-agent)); it is refused otherwise, before any request is sent.
    pub fn new(user_agent: &str) -> Result<Self, SecEdgarError> {
        let user_agent = user_agent.trim();
        let words: Vec<&str> = user_agent.split_whitespace().collect();
        let has_email = words.iter().any(|word| is_email(word));
        if words.len() < 2 || !has_email {
            return Err(SecEdgarError::invalid_input(
                "the User-Agent must name the sender and give a contact email, \
                 e.g. \"Sample Company Name admin@example.com\"",
            ));
        }
        let http = reqwest::Client::builder()
            .user_agent(user_agent)
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| {
                // A User-Agent with characters a header cannot carry fails here.
                SecEdgarError::invalid_input(format!("building the HTTP client: {e}"))
            })?;
        Ok(Self {
            http,
            www_url: DEFAULT_WWW_URL.to_owned(),
            data_url: DEFAULT_DATA_URL.to_owned(),
            pacer: Arc::new(Pacer::new(DEFAULT_REQUESTS_PER_SECOND)),
        })
    }

    /// The same client sending its requests to other servers in place of
    /// [`DEFAULT_WWW_URL`] and [`DEFAULT_DATA_URL`], such as a test server or a mirror.
    ///
    /// Each URL must be `http` or `https`; a trailing `/` is ignored. Clones keep sharing the
    /// pace.
    pub fn with_base_urls(self, www_url: &str, data_url: &str) -> Result<Self, SecEdgarError> {
        Ok(Self {
            www_url: base_url(www_url)?,
            data_url: base_url(data_url)?,
            ..self
        })
    }

    /// The same client sending at most `rate` requests a second. A fresh pace: clones made
    /// before this call keep sharing the old one.
    ///
    /// A rate above [`SEC_MAX_REQUESTS_PER_SECOND`] is refused.
    pub fn with_max_requests_per_second(self, rate: NonZeroU32) -> Result<Self, SecEdgarError> {
        if rate.get() > SEC_MAX_REQUESTS_PER_SECOND {
            return Err(SecEdgarError::invalid_input(format!(
                "{rate} requests a second is over SEC's limit of {SEC_MAX_REQUESTS_PER_SECOND}"
            )));
        }
        Ok(Self {
            pacer: Arc::new(Pacer::new(rate)),
            ..self
        })
    }

    /// Every ticker EDGAR maps to a company, in SEC's order. One request.
    ///
    /// A company can have several tickers (share classes) under one CIK. SEC updates the list as
    /// companies change; it holds current tickers only, so a delisted company's ticker is absent.
    pub async fn company_tickers(&self) -> Result<Vec<CompanyTicker>, SecEdgarError> {
        #[derive(Deserialize)]
        struct Entry {
            cik_str: u64,
            ticker: SmolStr,
            title: String,
        }
        let url = format!("{}/files/company_tickers.json", self.www_url);
        let entries: BTreeMap<String, Entry> = self.get_json(&url).await?;
        let mut indexed = entries
            .into_iter()
            .map(|(index, entry)| {
                let index: u64 = index.parse().map_err(|_| {
                    SecEdgarError::decode(format!("company ticker index {index:?}"))
                })?;
                Ok((
                    index,
                    CompanyTicker {
                        cik: Cik::new(entry.cik_str),
                        ticker: entry.ticker,
                        title: entry.title,
                    },
                ))
            })
            .collect::<Result<Vec<_>, SecEdgarError>>()?;
        indexed.sort_by_key(|(index, _)| *index);
        Ok(indexed.into_iter().map(|(_, ticker)| ticker).collect())
    }

    /// The earnings releases of the company `cik` dated from `from` to `to` (both inclusive, New
    /// York dates), oldest first, each as an [`EarningsRelease`] keyed by `symbol`.
    ///
    /// See the [module documentation](super#earnings-releases) for how a release is found and
    /// dated, and what is missed. Every record has status [`ReleaseStatus::Reported`], and
    /// [`known_at`](EarningsRelease::known_at) at its filing's acceptance.
    ///
    /// **The newest period is missing until its 10-Q or 10-K is filed**, which can be weeks after
    /// its release. A range ending today therefore usually ends at the last period whose report is
    /// filed.
    ///
    /// # Requests
    /// One for the company's filing index, one for each page of older filings the range reaches
    /// (a company that files often can have dozens), and one per release for its filing header.
    ///
    /// # Errors
    /// - [`SecEdgarError::InvalidInput`] if `from` is after `to`.
    /// - [`SecEdgarError::NotFound`] if SEC knows no company `cik`, or a filing its index lists
    ///   is missing.
    /// - [`SecEdgarError::ForeignPrivateIssuer`] if the company files 20-F, 40-F and 6-K reports.
    /// - Any request's error.
    pub async fn earnings_releases(
        &self,
        cik: Cik,
        symbol: impl Into<SmolStr>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<EarningsRelease>, SecEdgarError> {
        if from > to {
            return Err(SecEdgarError::invalid_input(format!(
                "range starts {from}, after it ends {to}"
            )));
        }
        let symbol = symbol.into();

        let url = format!("{}/submissions/CIK{cik}.json", self.data_url);
        let submissions: Submissions = self.get_json(&url).await?;
        let observed_at = Utc::now();
        let mut index = submissions.filings.recent.into_filings()?;
        // A page whose newest filing predates the range holds neither a release in it nor the
        // reports that pair one: both are filed on or after the release.
        for page in submissions
            .filings
            .files
            .iter()
            .filter(|page| page.filing_to >= from)
        {
            if page.name.contains('/') {
                return Err(SecEdgarError::decode(format!(
                    "filing index page name {:?}",
                    page.name
                )));
            }
            let url = format!("{}/submissions/{}", self.data_url, page.name);
            let columns: Columns = self.get_json(&url).await?;
            index.extend(columns.into_filings()?);
        }

        let pairs = match filings::releases(&index) {
            Releases::Paired(pairs) => pairs,
            Releases::ForeignPrivateIssuer => {
                return Err(SecEdgarError::ForeignPrivateIssuer { cik });
            }
        };

        let mut releases = Vec::new();
        // A release is filed on or after its date, and within `MAX_FILING_LAG` of it.
        let last_filed = to
            .checked_add_signed(MAX_FILING_LAG)
            .unwrap_or(NaiveDate::MAX);
        for (period_end, filing) in pairs
            .into_iter()
            .filter(|(_, filing)| filing.filed >= from && filing.filed <= last_filed)
        {
            let accession = &filing.accession_number;
            let url = format!(
                "{}/Archives/edgar/data/{}/{}/{accession}-index-headers.html",
                self.www_url,
                cik.get(),
                accession.replace('-', ""),
            );
            let header = self.get_text(&url).await?;
            let local = filings::acceptance_time(&header)?;
            let known_at = new_york::to_utc(local).ok_or_else(|| {
                SecEdgarError::decode(format!(
                    "acceptance time {local} of {accession} falls in the hour New York skips \
                     for daylight saving"
                ))
            })?;
            let date = local.date();
            if date < from || date > to {
                continue;
            }
            releases.push(EarningsRelease {
                symbol: symbol.clone(),
                fiscal_period_end: period_end,
                date,
                timing: filings::timing(local.time()),
                status: ReleaseStatus::Reported,
                known_at,
                source: ReleaseSource {
                    provider: EarningsProvider::SecEdgar,
                    record_id: Some(SmolStr::new(accession)),
                    updated_at: None,
                    observed_at,
                },
            });
        }
        releases.sort_by_key(|release| (release.date, release.known_at));
        Ok(releases)
    }

    async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, SecEdgarError> {
        let body = self.get_text(url).await?;
        serde_json::from_str(&body).map_err(|e| SecEdgarError::decode(format!("{url}: {e}")))
    }

    async fn get_text(&self, url: &str) -> Result<String, SecEdgarError> {
        self.pacer.wait().await;
        let transport = |e: reqwest::Error| SecEdgarError::Http {
            message: e.to_string(),
        };
        let response = self.http.get(url).send().await.map_err(transport)?;
        let status = response.status();
        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES)
                .await
                .unwrap_or_default();
            return Err(SecEdgarError::from_status(status.as_u16(), url, &body));
        }
        response.text().await.map_err(transport)
    }
}

impl std::fmt::Debug for SecEdgarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecEdgarClient")
            .field("www_url", &self.www_url)
            .field("data_url", &self.data_url)
            .field("pacer", &self.pacer)
            .finish_non_exhaustive()
    }
}

/// One ticker of a company, from SEC's ticker list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompanyTicker {
    /// The company's EDGAR identifier.
    pub cik: Cik,
    /// The ticker symbol.
    pub ticker: SmolStr,
    /// The company's name as EDGAR records it.
    pub title: String,
}

/// Whether `word` looks like an email address: something on each side of one `@`, and a dot in
/// the domain.
fn is_email(word: &str) -> bool {
    match word.split_once('@') {
        Some((local, domain)) => {
            !local.is_empty()
                && !domain.contains('@')
                && domain
                    .split_once('.')
                    .is_some_and(|(name, tld)| !name.is_empty() && !tld.is_empty())
        }
        None => false,
    }
}

fn base_url(text: &str) -> Result<String, SecEdgarError> {
    let parsed = url::Url::parse(text)
        .map_err(|e| SecEdgarError::invalid_input(format!("base URL {text:?}: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(SecEdgarError::invalid_input(format!(
            "base URL {text:?} is not http or https"
        )));
    }
    Ok(text.trim_end_matches('/').to_owned())
}

/// Spaces requests evenly: each waits for the slot one interval after the previous one's.
#[derive(Debug)]
struct Pacer {
    interval: Duration,
    next: Mutex<Option<Instant>>,
}

impl Pacer {
    fn new(rate: NonZeroU32) -> Self {
        Self {
            interval: Duration::from_secs(1) / rate.get(),
            next: Mutex::new(None),
        }
    }

    /// Wait for this request's slot. A caller dropped while waiting gives up its slot unused but
    /// blocks no one.
    async fn wait(&self) {
        tokio::time::sleep_until(self.claim()).await;
    }

    /// Claim the next free slot: now, or one interval after the last claimed slot if that is
    /// later.
    fn claim(&self) -> Instant {
        let mut next = self.next.lock();
        let now = Instant::now();
        let slot = next.map_or(now, |next| next.max(now));
        *next = Some(slot + self.interval);
        slot
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn a_user_agent_needs_a_name_and_an_email() {
        for good in [
            "Sample Company Name admin@example.com",
            "rustrade someone@example.org",
            "  someone@example.org  My App ",
        ] {
            assert!(SecEdgarClient::new(good).is_ok(), "{good:?}");
        }
        for bad in [
            "",
            "someone@example.com",
            "My App",
            "My App admin@",
            "My App @example.com",
            "My App admin@localhost",
            "My App admin@.com",
            "My App admin@example.",
            "My App a@b@example.com",
            "My App admin@example.com\u{7}",
        ] {
            assert!(
                matches!(
                    SecEdgarClient::new(bad),
                    Err(SecEdgarError::InvalidInput { .. })
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn debug_does_not_print_the_user_agent() {
        let client = SecEdgarClient::new("Sample Company admin@example.com").unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains("admin@example.com"), "{debug}");
        assert!(debug.starts_with("SecEdgarClient"), "{debug}");
        assert!(debug.contains(DEFAULT_DATA_URL), "{debug}");
    }

    #[test]
    fn base_urls_must_be_http_and_lose_a_trailing_slash() {
        let client = SecEdgarClient::new("App admin@example.com").unwrap();
        let client = client
            .with_base_urls("http://127.0.0.1:1/", "https://data.example/")
            .unwrap();
        assert_eq!(client.www_url, "http://127.0.0.1:1");
        assert_eq!(client.data_url, "https://data.example");
        for bad in ["ftp://sec.gov", "not a url"] {
            assert!(matches!(
                client.clone().with_base_urls(bad, "https://data.sec.gov"),
                Err(SecEdgarError::InvalidInput { .. })
            ));
            assert!(matches!(
                client.clone().with_base_urls("https://www.sec.gov", bad),
                Err(SecEdgarError::InvalidInput { .. })
            ));
        }
    }

    #[test]
    fn a_rate_over_secs_limit_is_refused() {
        let client = SecEdgarClient::new("App admin@example.com").unwrap();
        assert_eq!(client.pacer.interval, Duration::from_secs(1) / 9);
        let at_limit = NonZeroU32::new(SEC_MAX_REQUESTS_PER_SECOND).unwrap();
        let fast = client
            .clone()
            .with_max_requests_per_second(at_limit)
            .unwrap();
        assert_eq!(fast.pacer.interval, Duration::from_millis(100));
        assert!(!Arc::ptr_eq(&fast.pacer, &client.pacer));
        let over = NonZeroU32::new(SEC_MAX_REQUESTS_PER_SECOND + 1).unwrap();
        assert!(matches!(
            client.with_max_requests_per_second(over),
            Err(SecEdgarError::InvalidInput { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn the_pacer_spaces_requests_by_its_interval_and_shares_across_clones() {
        let pacer = Arc::new(Pacer::new(NonZeroU32::new(4).unwrap()));
        let start = Instant::now();
        pacer.wait().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        let other = Arc::clone(&pacer);
        other.wait().await;
        assert_eq!(start.elapsed(), Duration::from_millis(250));
        pacer.wait().await;
        assert_eq!(start.elapsed(), Duration::from_millis(500));

        // After an idle spell the next request goes at once, and the one after waits again.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let idle_end = Instant::now();
        pacer.wait().await;
        assert_eq!(idle_end.elapsed(), Duration::ZERO);
        pacer.wait().await;
        assert_eq!(idle_end.elapsed(), Duration::from_millis(250));
    }
}
