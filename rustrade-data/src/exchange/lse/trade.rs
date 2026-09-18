//! Decode a London Strategic Edge tick as a [`PublicTrade`].

use super::tick::LseMessage;
use crate::{
    event::{MarketEvent, MarketIter},
    subscription::trade::PublicTrade,
};
use chrono::Utc;
use rustrade_instrument::exchange::ExchangeId;
use smol_str::SmolStr;

/// Decode a tick frame as a [`PublicTrade`].
///
/// # ⚠️ The tick is a QUOTE — a `PublicTrade` from this feed may not be a print
/// `price` equals `bid` exactly on every sample taken — 3,966 of 3,966 ticks spanning every dataset
/// family, plus every sample from the provider's separate price endpoint. So the price here is a
/// bid-side quote wearing a trade's shape, and its arrival is **not** evidence that a transaction
/// occurred, at that price or at all. The mapping ships regardless, because saying so is more useful
/// than withholding a feed whose sizes do reconcile; but a strategy that treats these as executions
/// is reading one side of a quote as a fill.
///
/// # ⚠️ `amount` is genuine on some venues and FABRICATED on others
/// This is per-dataset and there is no in-band signal separating the two:
///
/// - [`LseCrypto`](super::LseCrypto) and [`LseEquities`](super::LseEquities) carry a **real**
///   per-tick size. Summing it across three whole minutes of one crypto symbol reproduced the
///   provider's own one-minute candle volume to the last decimal, ratio `1.000`.
/// - [`LseFx`](super::LseFx) and [`LseCfd`](super::LseCfd) carry a **hard-coded `1.0`** — measured
///   on every tick of every FX and commodity symbol sampled, one distinct value across all of them.
///   It is a placeholder, not a measurement. Because it is a plausible-looking positive number it
///   will aggregate into a legitimate-looking total at any resolution, so volume-weighted prices,
///   participation rates and size filters computed on these two venues are meaningless rather than
///   merely imprecise. Note this also differs from the provider's REST vault, which *omits* FX
///   volume entirely — the true absence; the WebSocket invents a value instead.
///
/// # ⚠️ Identical consecutive ticks are REAL and are both emitted
/// This decoder performs no de-duplication, deliberately. See
/// [`LseTick`](super::tick::LseTick) for the reconciliation that proves the repeats are distinct
/// prints. The property is pinned by `identical_live_ticks_are_never_deduplicated` in
/// [`transformer`](super::transformer), which is the only place a filter could be added: this
/// impl is a stateless `From` and has nothing to remember a previous tick with.
impl<InstrumentKey> From<(ExchangeId, InstrumentKey, LseMessage)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, message): (ExchangeId, InstrumentKey, LseMessage)) -> Self {
        let LseMessage::Tick(tick) = message else {
            // Control frames share the data stream with ticks. The transformer resolves a frame to
            // its instrument before reaching here and drops anything unidentifiable, so this arm is
            // unreachable through it -- but the conversion is total, and a control frame yielding
            // nothing is the only answer that is correct if it ever is reached.
            return Self(vec![]);
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: tick.time_exchange,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: PublicTrade {
                // The feed publishes no trade identifier, and one cannot be synthesised: ticks are
                // genuinely non-unique in `(ts, price, size)` -- one signature recurred 49 times in
                // a sampled run -- so anything derived from those fields would collide across
                // distinct prints. An empty id says "unidentified" rather than asserting a false
                // one.
                id: SmolStr::default(),
                price: tick.price,
                amount: tick.volume,
                // No aggressor side is published, and it is not inferable: the price is the bid on
                // every sample taken, which would make every tick look like a sell.
                side: None,
            },
        })])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_decimal_macros::dec;

    fn tick_json(ts: &str, price: &str, volume: &str) -> String {
        format!(
            r#"{{"type":"tick","symbol":"BTC/USD","ts":"{ts}","price":{price},
               "bid":{price},"ask":42001.0,"volume":{volume}}}"#
        )
    }

    fn decode(json: &str) -> Vec<MarketEvent<u8, PublicTrade>> {
        let message: LseMessage = serde_json::from_str(json).unwrap();
        MarketIter::<u8, PublicTrade>::from((ExchangeId::LseCrypto, 1_u8, message))
            .0
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_tick_decodes_to_one_trade() {
        let events = decode(&tick_json(
            "2026-01-02T09:37:24.760146+00:00",
            "42000.5",
            "0.00155",
        ));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].exchange, ExchangeId::LseCrypto);
        assert_eq!(events[0].instrument, 1);
        assert_eq!(
            events[0].time_exchange,
            "2026-01-02T09:37:24.760146Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(events[0].kind.price, dec!(42000.5));
        assert_eq!(events[0].kind.amount, dec!(0.00155));
        assert_eq!(events[0].kind.side, None);
    }

    /// Two identical frames decode to two identical events, which is what makes the repeats
    /// indistinguishable downstream and therefore what a de-duplication filter would key on. The
    /// filter itself could only live in the transformer, which is stateful — see
    /// `identical_live_ticks_are_never_deduplicated` there for the property that pins its absence.
    /// This decoder is a stateless `From`, so what it pins is narrower: the decode carries no
    /// distinguishing mark that would let one of the two be dropped.
    #[test]
    fn identical_consecutive_ticks_decode_to_identical_events() {
        let json = tick_json("2026-01-02T09:37:24.760146+00:00", "42000.5", "0.00155");

        let first = decode(&json);
        let second = decode(&json);

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].time_exchange, second[0].time_exchange);
        assert_eq!(first[0].kind, second[0].kind);
    }

    /// The empty id is forced rather than tidy — see the field comment.
    #[test]
    fn the_trade_id_is_empty_because_no_identifier_could_be_synthesised() {
        let events = decode(&tick_json(
            "2026-01-02T09:37:24.760146+00:00",
            "42000.5",
            "0.00155",
        ));
        assert!(events[0].kind.id.is_empty());
    }

    /// The fabricated FX size is passed through unaltered: rewriting it would replace one invented
    /// number with another, and the warning belongs in the docs, not in the data.
    #[test]
    fn the_fabricated_fx_size_is_passed_through_rather_than_rewritten() {
        let events = decode(&tick_json("2026-01-02 09:37:21.690159+00:00", "1.1", "1.0"));
        assert_eq!(events[0].kind.amount, dec!(1.0));
    }

    #[test]
    fn a_control_frame_yields_no_trades() {
        let events = decode(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#);
        assert!(events.is_empty());
    }
}
