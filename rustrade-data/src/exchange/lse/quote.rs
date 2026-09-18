//! Decode a London Strategic Edge tick as an [`OrderBookL1`].

use super::tick::LseMessage;
use crate::{
    books::Level,
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookL1,
};
use chrono::Utc;
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;

/// Decode a tick frame as a top-of-book quote.
///
/// This is the mapping the frame fits most honestly: the tick carries a bid and an ask, and its
/// `price` equals the bid on every sample taken. The trade mapping in
/// [`trade`](super::trade) exists alongside it and is documented as the approximation it is.
///
/// # Both levels carry a ZERO size
/// The feed publishes no bid or ask size — only prices — so the sizes here are placeholders, not
/// measurements. This is the same choice the provider's bulk-export decoder makes, for the same
/// reason: a fabricated size would be indistinguishable downstream from a real one. It is safe
/// because `DefaultInstrumentMarketData::price` in the `rustrade` crate falls back from the
/// volume-weighted mid to the plain mid when the sizes are absent, so a zero-size book prices
/// correctly rather than not at all. Consumers that genuinely need depth must not read these as
/// available quantity.
///
/// The tick's `volume` is deliberately **not** used for either level: it is a traded size, not
/// resting bid or ask liquidity, and on two of the five venues it is a fabricated constant. See
/// [`trade`](super::trade) for that warning in full.
///
/// # ⚠️ A zero PRICE is absent, and it is not the same case as a zero size
/// The provider publishes `0.0` for a side it has no quote for — a venue outside its session, a
/// symbol quoted on one side only. Published as `Some(Level { price: 0, .. })` that becomes a real
/// level asserting someone will trade at zero, and the damage is worse than the obvious one: it is
/// precisely the volume-weighted-mid fallback that makes the zero *sizes* above harmless which
/// makes a zero price dangerous. `mid_price` requires only that both sides be `Some`, and it
/// averages whatever prices they carry — so a book quoting a real ask of `42001` against a zero bid
/// prices at `21000.5`, a number that is neither wrong-looking nor recoverable downstream. The
/// consumer cannot filter it either, because nothing distinguishes it from a genuine quote.
///
/// So a zero price maps to `None`, following the same rule as the Binance L1 decoder. `None` is the
/// spelling every consumer of this type already handles as "no quote on this side"; a one-sided
/// book prices as no book at all, which is the honest answer when there is no quote to average.
impl<InstrumentKey> From<(ExchangeId, InstrumentKey, LseMessage)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange, instrument, message): (ExchangeId, InstrumentKey, LseMessage)) -> Self {
        let LseMessage::Tick(tick) = message else {
            // See the matching arm in the trade decoder: unreachable through the transformer, and
            // an empty result is the only correct answer if it is ever reached anyway.
            return Self(vec![]);
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: tick.time_exchange,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookL1 {
                // Must equal the event's `time_exchange` -- downstream state orders L1 updates on
                // this field rather than on the event's, and a producer that lets the two diverge
                // gets no error and no log.
                last_update_time: tick.time_exchange,
                best_bid: quoted(tick.bid),
                best_ask: quoted(tick.ask),
            },
        })])
    }
}

/// One side of the book, or `None` where the provider published no quote for it.
///
/// The size is a placeholder rather than a measurement — see the type-level note — so only the
/// price can say whether the side exists at all.
fn quoted(price: Decimal) -> Option<Level> {
    (!price.is_zero()).then(|| Level::new(price, Decimal::ZERO))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const TICK: &str = r#"{"type":"tick","symbol":"BTC/USD",
        "ts":"2026-01-02T09:37:24.760146+00:00","price":42000.5,"bid":42000.5,
        "ask":42001.0,"volume":0.00155}"#;

    fn decode(json: &str) -> Vec<MarketEvent<u8, OrderBookL1>> {
        let message: LseMessage = serde_json::from_str(json).unwrap();
        MarketIter::<u8, OrderBookL1>::from((ExchangeId::LseCrypto, 1_u8, message))
            .0
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_tick_decodes_to_a_two_sided_book() {
        let events = decode(TICK);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.best_bid.unwrap().price, dec!(42000.5));
        assert_eq!(events[0].kind.best_ask.unwrap().price, dec!(42001.0));
    }

    /// The producer obligation stated on the field: downstream state ranks L1 updates on
    /// `last_update_time`, so a divergence from the event's own instant silently freezes the book.
    #[test]
    fn the_book_instant_equals_the_events_instant() {
        let events = decode(TICK);
        assert_eq!(events[0].kind.last_update_time, events[0].time_exchange);
    }

    /// The feed publishes no sizes. A fabricated one would be indistinguishable from a real one.
    #[test]
    fn both_levels_carry_a_zero_size_because_none_is_published() {
        let events = decode(TICK);

        assert_eq!(events[0].kind.best_bid.unwrap().amount, Decimal::ZERO);
        assert_eq!(events[0].kind.best_ask.unwrap().amount, Decimal::ZERO);
    }

    /// The traded size is not resting liquidity, and on two venues it is a fabricated constant.
    #[test]
    fn the_tick_volume_is_not_used_as_a_level_size() {
        let events = decode(TICK);
        assert_ne!(events[0].kind.best_bid.unwrap().amount, dec!(0.00155));
    }

    fn one_sided(bid: &str, ask: &str) -> String {
        format!(
            r#"{{"type":"tick","symbol":"BTC/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":{bid},"bid":{bid},"ask":{ask},"volume":0.0}}"#
        )
    }

    /// The provider publishes `0.0` for a side it holds no quote for. Published as a level it
    /// asserts someone will trade at zero, which no consumer can distinguish from a genuine quote.
    #[test]
    fn a_zero_priced_side_is_absent_rather_than_a_level_at_zero() {
        let bid_only = decode(&one_sided("42000.5", "0.0"));
        assert_eq!(bid_only[0].kind.best_bid.unwrap().price, dec!(42000.5));
        assert_eq!(bid_only[0].kind.best_ask, None);

        let ask_only = decode(&one_sided("0.0", "42001.0"));
        assert_eq!(ask_only[0].kind.best_bid, None);
        assert_eq!(ask_only[0].kind.best_ask.unwrap().price, dec!(42001.0));

        let neither = decode(&one_sided("0.0", "0.0"));
        assert_eq!(neither[0].kind.best_bid, None);
        assert_eq!(neither[0].kind.best_ask, None);
    }

    /// Why the zero *price* is the dangerous one while the zero *sizes* are safe: the fallback that
    /// rescues a size-less book is `mid_price`, which requires only that both sides be `Some` and
    /// then averages whatever prices they carry. A zero bid published as a level would halve the
    /// instrument's price, silently and unrecoverably.
    #[test]
    fn a_zero_priced_side_would_have_halved_the_mid_had_it_been_published() {
        let events = decode(&one_sided("0.0", "42001.0"));
        let book = &events[0].kind;

        assert_eq!(
            crate::books::mid_price(Decimal::ZERO, dec!(42001.0)),
            dec!(21000.5),
            "this is what a published zero bid would have priced the instrument at",
        );

        // Absent instead, so `mid_price` has nothing to average and no price reaches a consumer
        // that was never quoted. A missing price is recoverable downstream; a halved one is not.
        assert!(book.best_bid.is_none());
        assert_eq!(book.mid_price(), None);
        assert_eq!(book.volume_weighed_mid_price(), None);
    }

    #[test]
    fn a_control_frame_yields_no_books() {
        let events = decode(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#);
        assert!(events.is_empty());
    }
}
