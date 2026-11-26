//! Helpers shared by unit tests in more than one module.

use std::sync::OnceLock;

/// A local WebSocket endpoint that accepts every TCP connection and never answers the upgrade, so
/// a connect to it stalls until its timeout.
///
/// One endpoint serves the whole test binary, from a thread of its own rather than a task, so that
/// it outlives each test's runtime. [`ExchangeServer::websocket_url`] returns a `&'static str`,
/// which this provides without leaking one per test.
///
/// [`ExchangeServer::websocket_url`]: crate::exchange::ExchangeServer::websocket_url
pub(crate) fn stalling_websocket_url() -> &'static str {
    static URL: OnceLock<String> = OnceLock::new();
    URL.get_or_init(|| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let url = format!(
            "ws://{}",
            listener.local_addr().expect("a bound listener's address")
        );
        std::thread::spawn(move || {
            // Held, never read or written, so each connection stays open and unanswered.
            let mut held = Vec::new();
            for stream in listener.incoming() {
                held.push(stream);
            }
        });
        url
    })
}

/// An [`ExchangeServer`](crate::exchange::ExchangeServer) at [`stalling_websocket_url`], for a
/// connector generic over its server (such as [`Bybit`](crate::exchange::bybit::Bybit)) to stall
/// against.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StallingServer;

impl crate::exchange::ExchangeServer for StallingServer {
    const ID: rustrade_instrument::exchange::ExchangeId =
        rustrade_instrument::exchange::ExchangeId::BybitSpot;

    fn websocket_url() -> &'static str {
        stalling_websocket_url()
    }
}

/// A public trades subscription to BTC/USDT on [`Bybit`](crate::exchange::bybit::Bybit) at
/// [`StallingServer`].
pub(crate) fn stalling_bybit_trades() -> crate::subscription::Subscription<
    crate::exchange::bybit::Bybit<StallingServer>,
    rustrade_instrument::instrument::market_data::MarketDataInstrument,
    crate::subscription::trade::PublicTrades,
> {
    crate::subscription::Subscription::from((
        crate::exchange::bybit::Bybit::<StallingServer>::default(),
        "btc",
        "usdt",
        rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind::Spot,
        crate::subscription::trade::PublicTrades,
    ))
}
