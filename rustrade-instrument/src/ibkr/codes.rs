//! IB message codes that the market data stream and the execution client both act on.
//!
//! TWS/Gateway answers a request it refuses, and reports its own state, with a numbered message.
//! These are the codes this workspace gives a meaning to; any other code is passed on as IB's text.

/// TWS/Gateway has lost its connection to IB's servers. The API socket stays up, but no market
/// data or order updates arrive until the link is restored.
pub const LINK_LOST: i32 = 1100;

/// TWS/Gateway is reconnected to IB's servers, and its market data subscriptions were lost: every
/// subscription made before must be made again.
pub const LINK_RESTORED_DATA_LOST: i32 = 1101;

/// TWS/Gateway is reconnected to IB's servers, and its market data subscriptions were kept.
pub const LINK_RESTORED_DATA_KEPT: i32 = 1102;

/// A request would exceed the account's market data lines ("Max number of tickers has been
/// reached").
///
/// The lines cap concurrent subscriptions. The cap is per username and shared by TWS itself and
/// every API connection on it, and a line frees only when a subscription is cancelled. IB offers
/// no API to read the cap or the lines in use. This is not IB's message rate limit, which is
/// code 100.
pub const MAX_TICKERS: i32 = 101;

/// A market depth request would exceed the account's concurrent depth requests ("Max number of
/// market depth requests has been reached"): 3 at the minimum line allocation.
pub const MAX_DEPTH_REQUESTS: i32 = 309;

/// The requested market data is not subscribed.
pub const MARKET_DATA_NOT_SUBSCRIBED: i32 = 354;

/// The account's market data subscription does not extend to the API.
pub const API_MARKET_DATA_NOT_SUBSCRIBED: i32 = 10089;

/// The market data is not subscribed, and delayed market data is not enabled.
pub const DELAYED_MARKET_DATA_NOT_ENABLED: i32 = 10168;

/// No market data during a competing session: a paper account gets none while the same user is
/// logged in to a live session elsewhere, which takes precedence.
pub const COMPETING_LIVE_SESSION: i32 = 10197;

/// Codes refusing a subscription because a cap on concurrent subscriptions is reached:
/// [`MAX_TICKERS`] and [`MAX_DEPTH_REQUESTS`].
pub const SUBSCRIPTION_LIMIT_CODES: [i32; 2] = [MAX_TICKERS, MAX_DEPTH_REQUESTS];

/// Codes saying the account may not have the instrument's market data, after which no tick comes:
/// [`MARKET_DATA_NOT_SUBSCRIBED`], [`API_MARKET_DATA_NOT_SUBSCRIBED`] and
/// [`DELAYED_MARKET_DATA_NOT_ENABLED`].
///
/// 10167, delayed data shown in place of live, is not one: the ticks follow it.
pub const MARKET_DATA_NOT_SUBSCRIBED_CODES: [i32; 3] = [
    MARKET_DATA_NOT_SUBSCRIBED,
    API_MARKET_DATA_NOT_SUBSCRIBED,
    DELAYED_MARKET_DATA_NOT_ENABLED,
];

/// Codes saying IB will send this account no market data for the instrument, after which no tick
/// comes: the [`MARKET_DATA_NOT_SUBSCRIBED_CODES`], and [`COMPETING_LIVE_SESSION`].
pub const MARKET_DATA_NOT_AVAILABLE_CODES: [i32; 4] = [
    MARKET_DATA_NOT_SUBSCRIBED,
    API_MARKET_DATA_NOT_SUBSCRIBED,
    DELAYED_MARKET_DATA_NOT_ENABLED,
    COMPETING_LIVE_SESSION,
];
