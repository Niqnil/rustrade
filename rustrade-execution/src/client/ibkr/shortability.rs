//! [`ShortabilityClient`] for IBKR: what IB says about lending a stock, read from market data.
//!
//! IB reports it as two ticks of a streaming market data request for generic tick 236: tick 46,
//! a level saying how readily the stock can be borrowed, and tick 89, how many shares IB has to
//! lend. A snapshot request cannot carry generic ticks, so the request streams until both have
//! arrived or the wait runs out, and is then cancelled.
//!
//! The request asks for IB's delayed market data, which IB serves only where the account has no
//! live subscription for the stock: an account with one still gets live data. An account with
//! neither, such as a paper account without market data sharing, would otherwise get no ticks at
//! all.

use super::{IbkrClient, is_transient_transport_loss};
use crate::{
    client::ShortabilityClient,
    error::{ApiError, ConnectivityError, UnindexedClientError},
    shortability::Shortability,
};
use ibapi::{
    Notice,
    contracts::{SecurityType, tick_types::TickType},
    market_data::{
        MarketDataType,
        realtime::{TickTypes, generic_tick},
    },
    subscriptions::SubscriptionItem,
};
use rust_decimal::Decimal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// How long [`IbkrClient::fetch_shortability`] waits for IB's shortability ticks, unless
/// [`IbkrConfig::shortability_timeout`](super::IbkrConfig::shortability_timeout) says otherwise.
pub const DEFAULT_SHORTABILITY_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`IbkrClient::fetch_shortability`] waits for tick 89 once tick 46 has arrived without
/// it.
///
/// For a stock IB has shares of, it sends both ticks together. For one it has few or none of, it
/// can send tick 46 and no tick 89 at all, and the read would otherwise wait out the whole
/// timeout for it. The grace still takes a tick 89 that IB sends just after tick 46.
///
/// Measured on a paper account in regular trading hours: a stock IB had shares of got both ticks
/// in the same message, and stocks at level 2.0 got no tick 89 within a minute. With no gap to
/// scale from, the grace is a small margin for a tick 89 that trails tick 46 rather than a
/// measured bound.
const SHORTABLE_SHARES_GRACE: Duration = Duration::from_secs(2);

/// IB's error code for a request that would exceed the account's market data lines ("Max number
/// of tickers has been reached"). The lines are a cap on concurrent subscriptions, shared by every
/// TWS session and API client on the account, and one frees only when a subscription is
/// cancelled: this is not IB's rate limit, which is code 100.
const MAX_TICKERS_CODE: i32 = 101;

/// IB's notices that the account may not have the stock's market data, after which no tick comes:
/// 354 (not subscribed), 10089 (the subscription does not extend to the API) and 10168 (not
/// subscribed, and delayed data is not enabled). They end the read at once rather than waiting
/// out the timeout. 10167, delayed data shown in place of live, is not one: the ticks follow it.
const NO_MARKET_DATA_CODES: [i32; 3] = [354, 10089, 10168];

impl ShortabilityClient for IbkrClient {
    /// Reads IB's shortability ticks with a streaming market data request for generic tick 236,
    /// which IB answers with:
    ///
    /// - tick 46, a level: above 2.5, at least 1000 shares are available to borrow, so the stock
    ///   is `shortable` and `easy_to_borrow`; above 1.5, it is `shortable` once shares are
    ///   located, so not easy to borrow; 1.5 or below, it is not `shortable`, and
    ///   `easy_to_borrow` is left unknown.
    /// - tick 89, the shares IB has to lend, as `available`.
    ///
    /// A stock that is `shortable` but not `easy_to_borrow` needs a locate. IB accepts a short
    /// sale of it and holds the order while it looks for shares (IB's code 404, which this client
    /// reports as a working order, not a rejection), and may never find them. So `shortable`
    /// says a short sale may be tried, not that shares are on hand.
    ///
    /// `fee_rate` is unknown: IB reports no borrow fee to the API. `delisting_at` is unknown too.
    ///
    /// The request is read until both ticks have arrived, or for
    /// [`IbkrConfig::shortability_timeout`](super::IbkrConfig::shortability_timeout) (10 s by
    /// default); one that arrives alone is returned on its own. Once tick 46 has arrived, tick 89
    /// is waited for at most 2 s more: IB sends both together for a stock it has shares of, and
    /// can send no tick 89 at all for one it has few or none of, which then comes back with
    /// `available` unknown. While it runs it holds one of the account's market data lines, and it
    /// is cancelled when the read ends. Calls made at once each hold their own line, so checking
    /// many stocks together can fill the account's lines. The read runs on a blocking thread, so dropping the
    /// returned future does not end it early: it runs to its end, holding the line until then.
    ///
    /// The request asks for IB's delayed market data, which IB serves only where the account has
    /// no live subscription for the stock, so an account without one still gets the ticks. IB
    /// does not say how far behind delayed shortability ticks are.
    ///
    /// # Errors
    ///
    /// - [`ApiError::InstrumentInvalid`] for an instrument whose contract is not registered, and
    ///   for one that is not a stock, as only a stock is sold short by borrowing it.
    /// - [`ConnectivityError::Timeout`] if neither tick arrives in time, or
    ///   [`ApiError::RequestRejected`] when IB sent notices instead. The message carries IB's
    ///   notices. A notice that the account may not have the stock's market data, live or
    ///   delayed, ends the read at once.
    /// - [`ApiError::CapacityExceeded`] if every one of the account's market data lines is in use,
    ///   by this client, another API client or TWS. Waiting does not free one; cancelling a
    ///   subscription does.
    /// - [`ApiError::RequestRejected`] if IB refuses the request, with IB's code and message.
    /// - [`ConnectivityError::Socket`] if the connection to TWS/Gateway drops.
    async fn fetch_shortability(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<Shortability, UnindexedClientError> {
        let Some(contract) = self.contracts.get_contract(instrument) else {
            return Err(instrument_invalid(instrument, "contract not registered"));
        };
        if contract.security_type != SecurityType::Stock {
            return Err(instrument_invalid(
                instrument,
                &format!(
                    "shortability does not apply to security type {}: only a stock is sold \
                     short by borrowing it",
                    contract.security_type
                ),
            ));
        }
        let client = self.client.clone();
        let timeout = self.config.shortability_timeout;
        let instrument = instrument.clone();

        tokio::task::spawn_blocking(move || {
            // Applies to this connection's later market data requests, and this client makes no
            // other kind, so setting it on every request costs one message and changes nothing
            // else.
            client
                .switch_market_data_type(MarketDataType::Delayed)
                .map_err(|e| request_error(&instrument, e))?;
            let subscription = client
                .market_data(&contract)
                .generic_ticks(&[generic_tick::SHORTABLE])
                .subscribe()
                .map_err(|e| request_error(&instrument, e))?;
            // Dropping the subscription at the end of this closure cancels the request.
            let ticks = read_shortability_ticks(timeout, SHORTABLE_SHARES_GRACE, |wait| {
                subscription.next_timeout(wait)
            })
            .map_err(|e| request_error(&instrument, e))?;
            ticks.into_shortability(&instrument, client.is_connected())
        })
        .await
        .map_err(|e| UnindexedClientError::TaskFailed(format!("task join: {e}")))?
    }
}

fn instrument_invalid(instrument: &InstrumentNameExchange, reason: &str) -> UnindexedClientError {
    UnindexedClientError::Api(ApiError::InstrumentInvalid(
        instrument.clone(),
        reason.to_owned(),
    ))
}

/// What an `ibapi` error answering the shortability request means for the caller.
fn request_error(instrument: &InstrumentNameExchange, e: ibapi::Error) -> UnindexedClientError {
    match e {
        ibapi::Error::Notice(notice) if notice.code == MAX_TICKERS_CODE => {
            UnindexedClientError::Api(ApiError::CapacityExceeded(format!(
                "{instrument}: {} {}",
                notice.code, notice.message
            )))
        }
        ibapi::Error::Notice(notice) => UnindexedClientError::Api(ApiError::RequestRejected(
            format!("{instrument}: {} {}", notice.code, notice.message),
        )),
        e if is_transient_transport_loss(&e) => {
            UnindexedClientError::Connectivity(ConnectivityError::Socket(e.to_string()))
        }
        e => UnindexedClientError::Internal(format!("{instrument} shortability: {e}")),
    }
}

/// The shortability ticks IB sent, and the notices it sent instead of or alongside them.
#[derive(Debug, Default, PartialEq)]
struct ShortabilityTicks {
    /// Tick 46.
    level: Option<f64>,
    /// Tick 89.
    shares: Option<f64>,
    notices: Vec<Notice>,
}

impl ShortabilityTicks {
    fn is_complete(&self) -> bool {
        self.level.is_some() && self.shares.is_some()
    }

    /// Keep `tick` if it is one of the two, and a finite number: anything else is no answer.
    fn record(&mut self, tick: &TickTypes) {
        let (slot, value) = match tick {
            TickTypes::Generic(generic) if generic.tick_type == TickType::Shortable => {
                (&mut self.level, generic.value)
            }
            // IB documents tick 89 as a generic tick, but `ibapi` decodes it as a size; read it
            // from either.
            TickTypes::Generic(generic) if generic.tick_type == TickType::ShortableShares => {
                (&mut self.shares, generic.value)
            }
            TickTypes::Size(size) if size.tick_type == TickType::ShortableShares => {
                (&mut self.shares, size.size)
            }
            _ => return,
        };
        if value.is_finite() {
            *slot = Some(value);
        } else {
            warn!(
                ?tick,
                "IBKR shortability tick is not a finite number, ignoring it"
            );
        }
    }

    /// What the ticks say, or why there is no answer when neither arrived.
    fn into_shortability(
        self,
        instrument: &InstrumentNameExchange,
        connected: bool,
    ) -> Result<Shortability, UnindexedClientError> {
        if self.level.is_none() && self.shares.is_none() {
            return Err(if !connected {
                UnindexedClientError::Connectivity(ConnectivityError::Socket(
                    "the connection to TWS/Gateway dropped before IB sent shortability".to_owned(),
                ))
            } else if self.notices.is_empty() {
                UnindexedClientError::Connectivity(ConnectivityError::Timeout)
            } else {
                let notices = self
                    .notices
                    .iter()
                    .map(|notice| format!("{} {}", notice.code, notice.message))
                    .collect::<Vec<_>>()
                    .join("; ");
                UnindexedClientError::Api(ApiError::RequestRejected(format!(
                    "{instrument}: IB sent no shortability, only: {notices}"
                )))
            });
        }
        Ok(shortability_from_ticks(self.level, self.shares))
    }
}

/// Read the shortability request's ticks until both have arrived or `timeout` has passed since
/// the read began. Once tick 46 has arrived, tick 89 is waited for at most `grace` more.
///
/// `next` is the subscription's `next_timeout`, which answers `None` when its wait runs out and
/// when the request ends. A streaming request ends only on an error, which comes first, or on the
/// connection dropping, which the caller checks.
///
/// A notice that the account may not have the stock's market data ends the read with the notices
/// gathered so far.
///
/// # Errors
/// The error IB or `ibapi` ended the request with, or IB's notice that the account has no market
/// data line free, after which no tick comes.
fn read_shortability_ticks(
    timeout: Duration,
    grace: Duration,
    mut next: impl FnMut(Duration) -> Option<Result<SubscriptionItem<TickTypes>, ibapi::Error>>,
) -> Result<ShortabilityTicks, ibapi::Error> {
    let mut deadline = Instant::now() + timeout;
    let mut ticks = ShortabilityTicks::default();
    while !ticks.is_complete() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match next(remaining) {
            Some(Ok(SubscriptionItem::Data(tick))) => {
                let had_level = ticks.level.is_some();
                ticks.record(&tick);
                if !had_level && ticks.level.is_some() && ticks.shares.is_none() {
                    deadline = deadline.min(Instant::now() + grace);
                }
            }
            Some(Ok(SubscriptionItem::Notice(notice))) if notice.code == MAX_TICKERS_CODE => {
                return Err(ibapi::Error::Notice(notice));
            }
            Some(Ok(SubscriptionItem::Notice(notice)))
                if NO_MARKET_DATA_CODES.contains(&notice.code) =>
            {
                debug!(%notice, "IBKR shortability request has no market data");
                ticks.notices.push(notice);
                break;
            }
            Some(Ok(SubscriptionItem::Notice(notice))) => {
                debug!(%notice, "Notice on an IBKR shortability request");
                ticks.notices.push(notice);
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    Ok(ticks)
}

/// The [`Shortability`] IB's tick 46 `level` and tick 89 `shares` describe; either may be
/// missing. See [`IbkrClient::fetch_shortability`] for the mapping.
fn shortability_from_ticks(level: Option<f64>, shares: Option<f64>) -> Shortability {
    let mut shortability = Shortability::new();
    if let Some(level) = level {
        shortability = if level > 2.5 {
            shortability.with_shortable(true).with_easy_to_borrow(true)
        } else if level > 1.5 {
            shortability.with_shortable(true).with_easy_to_borrow(false)
        } else {
            shortability.with_shortable(false)
        };
    }
    match shares.map(Decimal::try_from) {
        Some(Ok(shares)) if !shares.is_sign_negative() => shortability.with_available(shares),
        Some(_) => {
            warn!(?shares, "IBKR shortable shares is not a count, ignoring it");
            shortability
        }
        None => shortability,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use ibapi::market_data::realtime::{TickGeneric, TickSize};
    use rust_decimal_macros::dec;

    fn generic(
        tick_type: TickType,
        value: f64,
    ) -> Result<SubscriptionItem<TickTypes>, ibapi::Error> {
        Ok(SubscriptionItem::Data(TickTypes::Generic(TickGeneric {
            tick_type,
            value,
        })))
    }

    fn size(tick_type: TickType, size: f64) -> Result<SubscriptionItem<TickTypes>, ibapi::Error> {
        Ok(SubscriptionItem::Data(TickTypes::Size(TickSize {
            tick_type,
            size,
        })))
    }

    fn notice(code: i32, message: &str) -> Notice {
        Notice {
            request_id: Some(7),
            code,
            message: message.to_owned(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        }
    }

    /// A `next_timeout` that hands out `items`, then waits out every later call, as a streaming
    /// request that has nothing more to send does.
    fn subscription(
        items: Vec<Result<SubscriptionItem<TickTypes>, ibapi::Error>>,
    ) -> impl FnMut(Duration) -> Option<Result<SubscriptionItem<TickTypes>, ibapi::Error>> {
        let mut items = items.into_iter();
        move |wait| {
            items.next().or_else(|| {
                std::thread::sleep(wait);
                None
            })
        }
    }

    fn instrument() -> InstrumentNameExchange {
        InstrumentNameExchange::new("AAPL")
    }

    #[test]
    fn levels_map_to_shortable_and_easy_to_borrow() {
        let easy = shortability_from_ticks(Some(3.0), None);
        assert_eq!(easy.shortable, Some(true));
        assert_eq!(easy.easy_to_borrow, Some(true));

        let hard = shortability_from_ticks(Some(2.0), None);
        assert_eq!(hard.shortable, Some(true));
        assert_eq!(hard.easy_to_borrow, Some(false));

        let none = shortability_from_ticks(Some(1.0), None);
        assert_eq!(none.shortable, Some(false));
        assert_eq!(none.easy_to_borrow, None);
    }

    #[test]
    fn level_boundaries_belong_to_the_level_below() {
        // IB's thresholds are "greater than", so each boundary value is the lower level.
        let at_easy = shortability_from_ticks(Some(2.5), None);
        assert_eq!(
            (at_easy.shortable, at_easy.easy_to_borrow),
            (Some(true), Some(false))
        );
        let at_hard = shortability_from_ticks(Some(1.5), None);
        assert_eq!(at_hard.shortable, Some(false));
    }

    #[test]
    fn shares_are_available_and_the_fee_is_unknown() {
        let shortability = shortability_from_ticks(Some(3.0), Some(1_500_000.0));
        assert_eq!(shortability.available, Some(dec!(1500000)));
        assert_eq!(shortability.fee_rate, None);

        let only_shares = shortability_from_ticks(None, Some(0.0));
        assert_eq!(
            only_shares,
            Shortability::new().with_available(Decimal::ZERO)
        );
    }

    #[test]
    fn negative_shares_are_unknown() {
        assert_eq!(
            shortability_from_ticks(None, Some(-1.0)),
            Shortability::new()
        );
    }

    #[test]
    fn read_stops_once_both_ticks_have_arrived() {
        let timeout = Duration::from_secs(5);
        let started = Instant::now();
        let ticks = read_shortability_ticks(
            timeout,
            SHORTABLE_SHARES_GRACE,
            subscription(vec![
                generic(TickType::Shortable, 3.0),
                generic(TickType::Bid, 1.0),
                size(TickType::ShortableShares, 200.0),
            ]),
        )
        .unwrap();
        assert!(started.elapsed() < timeout, "waited for the timeout");
        assert_eq!(ticks.level, Some(3.0));
        assert_eq!(ticks.shares, Some(200.0));
    }

    #[test]
    fn shares_are_read_from_a_generic_tick_too() {
        let ticks = read_shortability_ticks(
            Duration::from_secs(5),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![
                generic(TickType::ShortableShares, 100.0),
                generic(TickType::Shortable, 2.0),
            ]),
        )
        .unwrap();
        assert!(ticks.is_complete());
        assert_eq!(ticks.shares, Some(100.0));
    }

    #[test]
    fn a_tick_that_arrives_alone_is_returned_after_the_timeout() {
        let ticks = read_shortability_ticks(
            Duration::from_millis(40),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![generic(TickType::Shortable, 2.0)]),
        )
        .unwrap();
        let shortability = ticks.into_shortability(&instrument(), true).unwrap();
        assert_eq!(
            shortability,
            Shortability::new()
                .with_shortable(true)
                .with_easy_to_borrow(false)
        );
    }

    /// Tick 46 alone ends the read once the grace for tick 89 has passed, well before the
    /// timeout, and is returned on its own. Tick 89 alone still waits for tick 46.
    #[test]
    fn tick_46_alone_waits_only_the_grace_for_tick_89() {
        let timeout = Duration::from_secs(5);
        let started = Instant::now();
        let ticks = read_shortability_ticks(
            timeout,
            Duration::from_millis(40),
            subscription(vec![
                generic(TickType::Shortable, 2.0),
                generic(TickType::Bid, 1.0),
            ]),
        )
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waited past the grace"
        );
        assert_eq!((ticks.level, ticks.shares), (Some(2.0), None));

        let started = Instant::now();
        let ticks = read_shortability_ticks(
            Duration::from_millis(300),
            Duration::from_millis(40),
            subscription(vec![size(TickType::ShortableShares, 100.0)]),
        )
        .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(300),
            "tick 89 alone starts no grace"
        );
        assert_eq!((ticks.level, ticks.shares), (None, Some(100.0)));
    }

    #[test]
    fn a_non_finite_tick_is_no_answer() {
        let ticks = read_shortability_ticks(
            Duration::from_millis(40),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![generic(TickType::Shortable, f64::NAN)]),
        )
        .unwrap();
        assert_eq!(ticks.level, None);
    }

    #[test]
    fn nothing_in_time_is_a_timeout() {
        let ticks = read_shortability_ticks(
            Duration::from_millis(40),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![]),
        )
        .unwrap();
        let error = ticks.into_shortability(&instrument(), true).unwrap_err();
        assert!(matches!(
            error,
            UnindexedClientError::Connectivity(ConnectivityError::Timeout)
        ));
    }

    #[test]
    fn nothing_but_notices_is_a_rejection_naming_them() {
        let started = Instant::now();
        let ticks = read_shortability_ticks(
            Duration::from_secs(5),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![Ok(SubscriptionItem::Notice(notice(
                10089,
                "Requested market data requires additional subscription for API.",
            )))]),
        )
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a no-data notice should end the read"
        );
        let error = ticks.into_shortability(&instrument(), true).unwrap_err();
        let UnindexedClientError::Api(ApiError::RequestRejected(message)) = error else {
            panic!("expected RequestRejected, got {error:?}");
        };
        assert!(message.starts_with("AAPL: "), "{message}");
        assert!(message.contains("10089 Requested market data"), "{message}");
    }

    #[test]
    fn a_delayed_data_notice_is_followed_by_the_ticks() {
        let ticks = read_shortability_ticks(
            Duration::from_secs(5),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![
                Ok(SubscriptionItem::Notice(notice(
                    10167,
                    "Requested market data is not subscribed. Displaying delayed market data.",
                ))),
                generic(TickType::Shortable, 3.0),
                size(TickType::ShortableShares, 188_524_117.0),
            ]),
        )
        .unwrap();
        assert_eq!(ticks.level, Some(3.0));
        assert_eq!(ticks.shares, Some(188_524_117.0));
        assert_eq!(ticks.notices.len(), 1);
    }

    #[test]
    fn a_notice_that_no_line_is_free_ends_the_read() {
        let started = Instant::now();
        let error = read_shortability_ticks(
            Duration::from_secs(5),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![Ok(SubscriptionItem::Notice(notice(
                101,
                "Max number of tickers has been reached",
            )))]),
        )
        .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5), "waited it out");
        assert!(matches!(
            request_error(&instrument(), error),
            UnindexedClientError::Api(ApiError::CapacityExceeded(_))
        ));
    }

    #[test]
    fn nothing_after_a_drop_is_a_socket_error() {
        let error = ShortabilityTicks::default()
            .into_shortability(&instrument(), false)
            .unwrap_err();
        assert!(matches!(
            error,
            UnindexedClientError::Connectivity(ConnectivityError::Socket(_))
        ));
    }

    #[test]
    fn an_error_ends_the_read() {
        let error = read_shortability_ticks(
            Duration::from_secs(5),
            SHORTABLE_SHARES_GRACE,
            subscription(vec![
                generic(TickType::Shortable, 3.0),
                Err(ibapi::Error::Notice(notice(354, "Not subscribed"))),
            ]),
        )
        .unwrap_err();
        let error = request_error(&instrument(), error);
        let UnindexedClientError::Api(ApiError::RequestRejected(message)) = error else {
            panic!("expected RequestRejected, got {error:?}");
        };
        assert_eq!(message, "AAPL: 354 Not subscribed");
    }

    #[test]
    fn no_free_market_data_line_is_capacity_exceeded_not_a_rate_limit() {
        let error = request_error(
            &instrument(),
            ibapi::Error::Notice(notice(101, "Max number of tickers has been reached")),
        );
        assert_eq!(
            error,
            UnindexedClientError::Api(ApiError::CapacityExceeded(
                "AAPL: 101 Max number of tickers has been reached".to_owned()
            ))
        );
        assert!(!error.is_transient());
    }

    #[test]
    fn config_timeout_is_milliseconds_with_a_default() {
        use super::super::IbkrConfig;

        let minimal = r#"{"host":"127.0.0.1","port":4002,"client_id":1,"account":""}"#;
        let config: IbkrConfig = serde_json::from_str(minimal).unwrap();
        assert_eq!(config.shortability_timeout, DEFAULT_SHORTABILITY_TIMEOUT);

        let config = config.with_shortability_timeout(Duration::from_millis(2500));
        let json = serde_json::to_value(&config).unwrap();
        assert_eq!(json["shortability_timeout_ms"], 2500);
        let back: IbkrConfig = serde_json::from_value(json).unwrap();
        assert_eq!(back.shortability_timeout, Duration::from_millis(2500));
    }

    #[test]
    fn a_dropped_socket_is_a_connectivity_error() {
        let error = request_error(&instrument(), ibapi::Error::ConnectionReset);
        assert!(matches!(
            error,
            UnindexedClientError::Connectivity(ConnectivityError::Socket(_))
        ));
    }
}
