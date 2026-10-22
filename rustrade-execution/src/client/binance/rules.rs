//! Binance's per-symbol order rules: read from `exchangeInfo`, held per symbol and refreshed.
//!
//! Both clients check an order against the symbol's [`OrderRules`] before sending it (see
//! [`PreSendRules`]). The rules come from the **spot** `exchangeInfo`, which is public. Binance
//! margin has no rules endpoint of its own, and a margin order obeys its pair's spot filters.
//!
//! # Loading
//!
//! Rules load lazily, one symbol at a time, on the first order for the symbol: a client needs no
//! symbol list up front. [`RulesCache`] reloads them once they are an hour old. Loading is
//! single-flight: orders that arrive while a load is under way wait for it instead of sending
//! their own request. A failed reload keeps the old rules until they are 24 hours old, so a
//! blip at the venue does not turn into a stream of false refusals. Rules older than that are
//! dropped, and a symbol with no rules, because the venue lists none or the load failed, is sent
//! unchecked with a warning, and the venue decides. After a failed load a symbol is left alone for
//! 30 seconds, so an outage costs one request per window, not one per order. One gate serves every
//! symbol, so a slow load delays the first order of another symbol that needs one.
//!
//! # What is read
//!
//! `PRICE_FILTER`, `LOT_SIZE`, `MARKET_LOT_SIZE` and `NOTIONAL` (or the older `MIN_NOTIONAL`).
//! Each part is optional and a zero means off, as the venue means it. The filters that need a
//! reference price the library does not have (`PERCENT_PRICE`, and the average price behind a
//! market order's notional) and the ones that depend on account state (`MAX_NUM_ORDERS`,
//! `MAX_POSITION`, ...) are not read, so the venue still enforces them.

use super::shared::{
    RateLimitTracker, RequestKind, classify_rest_query_error, rest_call_with_retry,
};
use crate::{
    error::{OrderError, UnindexedClientError},
    order::OrderKind,
    rules::{OrderRules, PreSendRules, ValueRule},
};
use binance_sdk::spot::rest_api::{
    ExchangeInfoParams, ExchangeInfoResponseSymbolsInner, MyFiltersResponseSymbolFiltersInner,
    RestApi,
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use smol_str::SmolStr;
use std::{collections::HashMap, future::Future, str::FromStr, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::warn;

/// How old a symbol's rules are before they are loaded again.
const RULES_TTL: Duration = Duration::from_secs(60 * 60);

/// How old a symbol's rules may be before a failed reload stops them being used.
const RULES_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// How long to leave a symbol alone after a failed reload of its stale rules, so that every
/// order during an outage does not send a request of its own.
const RELOAD_BACKOFF: Duration = Duration::from_secs(30);

/// One symbol's rules as last loaded.
#[derive(Debug, Clone, Copy)]
struct Loaded {
    /// `None` when the venue lists no rules for the symbol.
    rules: Option<OrderRules>,
    loaded_at: Instant,
    /// Set after a failed reload: no new attempt before it.
    retry_at: Option<Instant>,
}

/// The rules of the symbols a client has traded, and the policy for loading them.
#[derive(Debug)]
pub(crate) struct RulesCache {
    mode: PreSendRules,
    ttl: Duration,
    max_age: Duration,
    entries: Mutex<HashMap<SmolStr, Loaded>>,
    /// Held across a load, so concurrent orders share one request.
    load_gate: tokio::sync::Mutex<()>,
}

impl RulesCache {
    pub(crate) fn new(mode: PreSendRules) -> Self {
        Self {
            mode,
            ttl: RULES_TTL,
            max_age: RULES_MAX_AGE,
            entries: Mutex::new(HashMap::new()),
            load_gate: tokio::sync::Mutex::new(()),
        }
    }

    /// The rules to check an order for `symbol` against, or `None` to send it unchecked.
    ///
    /// Loads them with `load` when none are held or they are older than the TTL. `load` returns
    /// the symbol's rules (`None` if the venue lists none) or why it failed.
    pub(crate) async fn rules<F, Fut>(&self, symbol: &str, load: F) -> Option<OrderRules>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<OrderRules>, String>>,
    {
        if self.mode == PreSendRules::Off {
            return None;
        }
        if let Some(rules) = self.usable(symbol) {
            return rules;
        }
        let _single_flight = self.load_gate.lock().await;
        // Another order may have loaded it while this one waited for the gate.
        if let Some(rules) = self.usable(symbol) {
            return rules;
        }
        match load().await {
            Ok(rules) => {
                if rules.is_none() {
                    warn!(%symbol, "Binance lists no order rules for the symbol: orders are sent unchecked");
                }
                self.entries.lock().insert(
                    SmolStr::new(symbol),
                    Loaded {
                        rules,
                        loaded_at: Instant::now(),
                        retry_at: None,
                    },
                );
                rules
            }
            Err(error) => self.keep_stale(symbol, &error),
        }
    }

    /// Hold `rules` as just loaded, for symbols loaded together.
    pub(crate) fn store(&self, rules: &HashMap<SmolStr, OrderRules>) {
        let now = Instant::now();
        let mut entries = self.entries.lock();
        for (symbol, rules) in rules {
            entries.insert(
                symbol.clone(),
                Loaded {
                    rules: Some(*rules),
                    loaded_at: now,
                    retry_at: None,
                },
            );
        }
    }

    /// The held rules if they need no load: fresh, or inside the back-off of a failed reload (then
    /// only while within the max age). `Some(None)` means no rules are to be used.
    fn usable(&self, symbol: &str) -> Option<Option<OrderRules>> {
        let entries = self.entries.lock();
        let held = entries.get(symbol)?;
        let now = Instant::now();
        let age = now.saturating_duration_since(held.loaded_at);
        match held.retry_at {
            Some(retry_at) if now < retry_at => {
                Some(if age < self.max_age { held.rules } else { None })
            }
            Some(_) => None,
            None => (age < self.ttl).then_some(held.rules),
        }
    }

    /// After a failed load: the held rules while they are within the max age, else none. Either
    /// way the symbol is left alone for [`RELOAD_BACKOFF`], so a first load that fails, or rules
    /// gone over-age, do not send a request for every order.
    fn keep_stale(&self, symbol: &str, error: &str) -> Option<OrderRules> {
        let mut entries = self.entries.lock();
        let now = Instant::now();
        let held = entries.entry(SmolStr::new(symbol)).or_insert(Loaded {
            rules: None,
            loaded_at: now,
            retry_at: None,
        });
        if now.saturating_duration_since(held.loaded_at) >= self.max_age {
            held.rules = None;
        }
        held.retry_at = Some(now + RELOAD_BACKOFF);
        if held.rules.is_some() {
            warn!(%symbol, %error, "Binance order rules failed to reload: keeping the old ones");
        } else {
            warn!(%symbol, %error, "Binance order rules unavailable: orders are sent unchecked");
        }
        held.rules
    }
}

/// A client's rules: what it holds, and the spot REST connection it loads more with.
pub(crate) struct RulesLoader {
    cache: RulesCache,
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
}

impl RulesLoader {
    /// Rules loaded over `rest`, whose requests count against `rate_limiter`.
    pub(crate) fn new(
        mode: PreSendRules,
        rest: Arc<RestApi>,
        rate_limiter: Arc<RateLimitTracker>,
    ) -> Self {
        Self {
            cache: RulesCache::new(mode),
            rest,
            rate_limiter,
        }
    }

    /// Check an order on `symbol` against its rules, loading them if need be.
    ///
    /// Passes when the rules are off or unavailable: see the module docs.
    pub(crate) async fn check<AssetKey, InstrumentKey>(
        &self,
        symbol: &str,
        kind: OrderKind,
        price: Option<Decimal>,
        quantity: Decimal,
    ) -> Result<(), OrderError<AssetKey, InstrumentKey>> {
        let rules = self
            .cache
            .rules(symbol, || async {
                let wanted = [symbol.to_owned()];
                let mut loaded = fetch_rules(&self.rest, &self.rate_limiter, &wanted)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(loaded.remove(symbol))
            })
            .await;
        match rules {
            Some(rules) => rules.check(kind, price, quantity),
            None => Ok(()),
        }
    }

    /// Load the rules of `instruments` now and hold them, as `load_rules` on the clients.
    pub(crate) async fn load(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<HashMap<InstrumentNameExchange, OrderRules>, UnindexedClientError> {
        if instruments.is_empty() {
            return Ok(HashMap::new());
        }
        let symbols: Vec<String> = instruments
            .iter()
            .map(|instrument| instrument.name().to_string())
            .collect();
        let loaded = fetch_rules(&self.rest, &self.rate_limiter, &symbols)
            .await
            .map_err(|error| classify_rest_query_error(&error, None))?;
        self.cache.store(&loaded);
        Ok(loaded
            .into_iter()
            .map(|(symbol, rules)| (InstrumentNameExchange::new(symbol), rules))
            .collect())
    }
}

/// Read the rules of `symbols` from the spot `exchangeInfo`, with the rate-limit retry the
/// clients' other requests have. A symbol Binance lists no rules for is absent from the result.
///
/// Binance refuses the whole request when one symbol is not listed.
pub(crate) async fn fetch_rules(
    rest: &Arc<RestApi>,
    rate_limiter: &RateLimitTracker,
    symbols: &[String],
) -> anyhow::Result<HashMap<SmolStr, OrderRules>> {
    let response = rest_call_with_retry(rest, rate_limiter, RequestKind::Query, |rest| {
        let symbols = symbols.to_vec();
        Box::pin(async move {
            let params = ExchangeInfoParams::builder().symbols(symbols).build()?;
            rest.exchange_info(params).await
        })
    })
    .await?;
    let info = response.data().await?;
    Ok(info
        .symbols
        .unwrap_or_default()
        .iter()
        .filter_map(|symbol| {
            let name = symbol.symbol.as_deref()?;
            Some((SmolStr::new(name), parse_symbol_rules(symbol)))
        })
        .collect())
}

/// A symbol's [`OrderRules`] from its `exchangeInfo` filters.
///
/// A filter that is missing, or whose value does not parse, leaves its part off, so a rule that
/// cannot be read is never enforced wrongly: the venue still enforces it.
pub(crate) fn parse_symbol_rules(symbol: &ExchangeInfoResponseSymbolsInner) -> OrderRules {
    let parse = |value: &Option<String>| {
        value
            .as_deref()
            .and_then(|text| Decimal::from_str(text).ok())
            .unwrap_or(Decimal::ZERO)
    };
    let mut rules = OrderRules::default();
    // MIN_NOTIONAL is the older form of NOTIONAL, which wins when a symbol lists both.
    let mut min_notional = None;
    for filter in symbol.filters.iter().flatten() {
        match filter {
            MyFiltersResponseSymbolFiltersInner::PriceFilter(filter) => {
                rules.price = ValueRule::new(
                    parse(&filter.min_price),
                    parse(&filter.max_price),
                    parse(&filter.tick_size),
                );
            }
            MyFiltersResponseSymbolFiltersInner::LotSize(filter) => {
                rules.quantity = ValueRule::new(
                    parse(&filter.min_qty),
                    parse(&filter.max_qty),
                    parse(&filter.step_size),
                );
            }
            MyFiltersResponseSymbolFiltersInner::MarketLotSize(filter) => {
                rules.market_quantity = ValueRule::new(
                    parse(&filter.min_qty),
                    parse(&filter.max_qty),
                    parse(&filter.step_size),
                );
            }
            MyFiltersResponseSymbolFiltersInner::Notional(filter) => {
                rules.notional = ValueRule::new(
                    parse(&filter.min_notional),
                    parse(&filter.max_notional),
                    Decimal::ZERO,
                );
            }
            MyFiltersResponseSymbolFiltersInner::MinNotional(filter) => {
                min_notional = Some(parse(&filter.min_notional));
            }
            // Every other filter needs a reference price or account state.
            _ => {}
        }
    }
    if rules.notional.is_off()
        && let Some(min) = min_notional
    {
        rules.notional = ValueRule::new(min, Decimal::ZERO, Decimal::ZERO);
    }
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn symbol(filters: serde_json::Value) -> ExchangeInfoResponseSymbolsInner {
        match serde_json::from_value(serde_json::json!({
            "symbol": "BTCUSDT", "status": "TRADING", "filters": filters,
        })) {
            Ok(symbol) => symbol,
            Err(error) => panic!("symbol should parse: {error}"),
        }
    }

    fn some_rules() -> OrderRules {
        OrderRules::new(
            ValueRule::new(dec!(0.01), dec!(0), dec!(0.01)),
            ValueRule::default(),
            ValueRule::default(),
            ValueRule::default(),
        )
    }

    #[test]
    fn the_spot_filters_become_the_rules() {
        let rules = parse_symbol_rules(&symbol(serde_json::json!([
            {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
            {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000", "maxQty": "100.00000000", "stepSize": "0.00000000"},
            {"filterType": "NOTIONAL", "minNotional": "5.00000000", "applyMinToMarket": true, "maxNotional": "9000000.00000000", "applyMaxToMarket": false, "avgPriceMins": 5},
            {"filterType": "ICEBERG_PARTS", "limit": 10},
            {"filterType": "PERCENT_PRICE_BY_SIDE", "bidMultiplierUp": "5", "bidMultiplierDown": "0.2", "askMultiplierUp": "5", "askMultiplierDown": "0.2", "avgPriceMins": 5},
        ])));
        assert_eq!(
            rules,
            OrderRules::new(
                ValueRule::new(dec!(0.01), dec!(1000000), dec!(0.01)),
                ValueRule::new(dec!(0.00001), dec!(9000), dec!(0.00001)),
                ValueRule::new(dec!(0), dec!(100), dec!(0)),
                ValueRule::new(dec!(5), dec!(9000000), dec!(0)),
            )
        );
        assert_eq!(rules.market_quantity.min, None, "a zero minimum is off");
        assert_eq!(rules.market_quantity.step, None, "a zero step is off");
    }

    #[test]
    fn min_notional_stands_in_for_notional_and_notional_wins_when_both_are_listed() {
        let min_only = parse_symbol_rules(&symbol(serde_json::json!([
            {"filterType": "MIN_NOTIONAL", "minNotional": "10.0", "applyToMarket": true, "avgPriceMins": 5},
        ])));
        assert_eq!(
            min_only.notional,
            ValueRule::new(dec!(10), dec!(0), dec!(0))
        );
        for order in [[0, 1], [1, 0]] {
            let filters = [
                serde_json::json!({"filterType": "MIN_NOTIONAL", "minNotional": "10.0", "applyToMarket": true, "avgPriceMins": 5}),
                serde_json::json!({"filterType": "NOTIONAL", "minNotional": "5.0", "applyMinToMarket": true, "maxNotional": "0", "applyMaxToMarket": false, "avgPriceMins": 5}),
            ];
            let both = parse_symbol_rules(&symbol(serde_json::Value::Array(
                order.iter().map(|&i| filters[i].clone()).collect(),
            )));
            assert_eq!(
                both.notional,
                ValueRule::new(dec!(5), dec!(0), dec!(0)),
                "{order:?}"
            );
        }
    }

    #[test]
    fn a_filter_that_is_missing_or_unparseable_leaves_its_part_off() {
        let rules = parse_symbol_rules(&symbol(serde_json::json!([
            {"filterType": "LOT_SIZE", "minQty": "oops", "maxQty": "100", "stepSize": "0.1"},
        ])));
        assert_eq!(
            rules.quantity,
            ValueRule::new(dec!(0), dec!(100), dec!(0.1))
        );
        assert!(rules.price.is_off());
        assert!(parse_symbol_rules(&symbol(serde_json::json!([]))).is_off());
    }

    #[tokio::test(start_paused = true)]
    async fn rules_load_once_and_are_reused_until_the_ttl() {
        assert_eq!(RULES_TTL, Duration::from_secs(3_600));
        let cache = RulesCache::new(PreSendRules::Enforce);
        let loads = AtomicUsize::new(0);
        let load = || async {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(some_rules()))
        };
        assert_eq!(cache.rules("BTCUSDT", load).await, Some(some_rules()));
        assert_eq!(cache.rules("BTCUSDT", load).await, Some(some_rules()));
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        tokio::time::advance(RULES_TTL - Duration::from_secs(1)).await;
        cache.rules("BTCUSDT", load).await;
        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "still fresh a second before the TTL"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        cache.rules("BTCUSDT", load).await;
        assert_eq!(loads.load(Ordering::SeqCst), 2, "reloaded at the TTL");

        cache.rules("ETHUSDT", load).await;
        assert_eq!(
            loads.load(Ordering::SeqCst),
            3,
            "each symbol loads on its own"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn off_never_loads_and_checks_nothing() {
        let cache = RulesCache::new(PreSendRules::Off);
        let loaded = cache
            .rules("BTCUSDT", || async {
                panic!("Off must not load");
                #[allow(unreachable_code)] // gives the async block its output type
                Ok(None)
            })
            .await;
        assert_eq!(loaded, None);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_orders_for_a_symbol_share_one_load() {
        let cache = Arc::new(RulesCache::new(PreSendRules::Enforce));
        let loads = Arc::new(AtomicUsize::new(0));
        let orders = (0..5).map(|_| {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            tokio::spawn(async move {
                cache
                    .rules("BTCUSDT", || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(Some(some_rules()))
                    })
                    .await
            })
        });
        for order in orders.collect::<Vec<_>>() {
            assert_eq!(order.await.ok().flatten(), Some(some_rules()));
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_first_load_sends_unchecked_and_backs_off_before_trying_again() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        let attempts = AtomicUsize::new(0);
        let failing = || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("offline".to_owned())
        };
        assert_eq!(cache.rules("BTCUSDT", failing).await, None);
        assert_eq!(cache.rules("BTCUSDT", failing).await, None);
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "backing off");
        tokio::time::advance(RELOAD_BACKOFF).await;
        let loaded = cache
            .rules("BTCUSDT", || async { Ok(Some(some_rules())) })
            .await;
        assert_eq!(loaded, Some(some_rules()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_symbol_with_no_rules_is_remembered_so_it_is_not_loaded_per_order() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        let loads = AtomicUsize::new(0);
        let load = || async {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        };
        assert_eq!(cache.rules("XYZ", load).await, None);
        assert_eq!(cache.rules("XYZ", load).await, None);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_reload_keeps_the_old_rules_and_backs_off_until_they_are_a_day_old() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        cache
            .rules("BTCUSDT", || async { Ok(Some(some_rules())) })
            .await;
        let attempts = AtomicUsize::new(0);
        let failing = || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("offline".to_owned())
        };

        tokio::time::advance(RULES_TTL).await;
        assert_eq!(cache.rules("BTCUSDT", failing).await, Some(some_rules()));
        assert_eq!(cache.rules("BTCUSDT", failing).await, Some(some_rules()));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "backing off");

        tokio::time::advance(RELOAD_BACKOFF).await;
        assert_eq!(cache.rules("BTCUSDT", failing).await, Some(some_rules()));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "tries again after the back-off"
        );

        tokio::time::advance(RULES_MAX_AGE).await;
        assert_eq!(
            cache.rules("BTCUSDT", failing).await,
            None,
            "a day old: dropped"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_successful_reload_replaces_the_rules() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        cache
            .rules("BTCUSDT", || async { Ok(Some(some_rules())) })
            .await;
        tokio::time::advance(RULES_TTL).await;
        let changed = OrderRules::default();
        assert_eq!(
            cache.rules("BTCUSDT", || async { Ok(Some(changed)) }).await,
            Some(changed)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stored_rules_are_used_without_a_load() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        cache.store(&HashMap::from([(SmolStr::new("BTCUSDT"), some_rules())]));
        let rules = cache
            .rules("BTCUSDT", || async { Err("must not load".to_owned()) })
            .await;
        assert_eq!(rules, Some(some_rules()));
    }

    /// The max age is a day, and rules exactly that old are no longer used.
    #[tokio::test(start_paused = true)]
    async fn rules_are_dropped_at_exactly_the_max_age_inside_a_back_off() {
        assert_eq!(RULES_MAX_AGE, Duration::from_secs(86_400));
        let cache = RulesCache::new(PreSendRules::Enforce);
        cache
            .rules("BTCUSDT", || async { Ok(Some(some_rules())) })
            .await;
        let failing = || async { Err("offline".to_owned()) };
        tokio::time::advance(RULES_MAX_AGE - Duration::from_secs(10)).await;
        assert_eq!(cache.rules("BTCUSDT", failing).await, Some(some_rules()));
        // Still inside the back-off, now exactly one max age since the load.
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(cache.rules("BTCUSDT", failing).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_inside_the_back_off_past_the_max_age_drops_the_rules() {
        let cache = RulesCache::new(PreSendRules::Enforce);
        cache
            .rules("BTCUSDT", || async { Ok(Some(some_rules())) })
            .await;
        tokio::time::advance(RULES_MAX_AGE - Duration::from_secs(1)).await;
        let failing = || async { Err("offline".to_owned()) };
        assert_eq!(cache.rules("BTCUSDT", failing).await, Some(some_rules()));
        // Inside the 30 s back-off, but now past the max age.
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(cache.rules("BTCUSDT", failing).await, None);
    }
}
