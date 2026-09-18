//! The London Strategic Edge WebSocket tick frame, and the three timestamp spellings it arrives in.

use super::channel::LseChannel;
use crate::{Identifier, exchange::ExchangeSub};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_integration::subscription::SubscriptionId;
use serde::{
    Deserialize, Deserializer,
    de::{self, Unexpected, Visitor},
};
use smol_str::SmolStr;
use std::fmt;

/// A tick published on the London Strategic Edge WebSocket.
///
/// One frame shape serves every dataset — FX, crypto, equities, ETFs, indices, commodities,
/// futures, volatility, currency indices and interest rates all publish the same seven keys. This
/// is the opposite of the provider's bulk-export path, where the column set varies per dataset.
///
/// ```json
/// {"type":"tick","symbol":"BTC/USD","ts":"2026-01-02T09:37:24.760146+00:00",
///  "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}
/// ```
///
/// # ⚠️ The tick is a QUOTE, not a print
/// `price` equals `bid` exactly — measured on 3,966 of 3,966 sampled ticks spanning every dataset
/// family, plus every sample taken on the provider's other price endpoint. Anything decoded from
/// this frame as a trade is therefore a bid-side quote wearing a trade's shape, and is not evidence
/// that a transaction occurred at that price or at all. See the trade transformer for what ships
/// anyway and why.
///
/// # ⚠️ `volume` is real on some datasets and fabricated on others
/// Crypto and equity ticks carry a genuine per-tick size: summing it over three whole minutes of
/// one crypto symbol reproduced the provider's own one-minute candle volume to the last decimal,
/// ratio `1.000`. FX and commodity ticks carry a **hard-coded `1.0`** on every tick of every
/// symbol sampled — a fabricated size, not a measured one, and it will aggregate into a
/// legitimate-looking total at any resolution. Note this differs from the provider's REST vault,
/// which *omits* FX volume entirely; the WebSocket invents a value instead.
///
/// # ⚠️ Identical consecutive ticks are REAL and must not be filtered
/// Ticks routinely repeat a prior `(ts, price, bid, ask, volume)` signature — barely a third of a
/// sampled run was unique, and one signature recurred 49 times. They are nonetheless distinct
/// prints: de-duplicating them destroyed 3–10% of the volume that reconciles exactly against the
/// provider's candles. This is an aggregated cross-venue tape on which identical-size fills at the
/// same millisecond are ordinary. Do not add a de-duplication filter.
///
/// # The four [`Decimal`] fields carry ~15 significant digits, not arbitrary precision
/// This feed sends prices as JSON **numbers** rather than as strings — it is the only connector in
/// this crate that does — so they are parsed as `f64` before reaching [`Decimal`]. The ceiling is
/// therefore the `f64`'s: 15 significant digits survive exactly, and a 17-digit literal like
/// `10.123456789012345` lands as `10.123456789012344`. The provider's own tick shapes
/// (`42000.5`, `0.00155`, `16.9`) are nowhere near that, so no observed value loses a digit.
///
/// The same ceiling applies on the bulk-export path for the same reason, where it is pinned by
/// `an_equity_row_deserializes_float_prices_and_an_integer_volume` in
/// [`historical`](super::historical).
///
/// Reading the provider's digits instead of the `f64` is **not available here**, and that is
/// structural rather than an omission: [`LseMessage`] is internally tagged, so serde buffers the
/// frame body into its `Content` type before this struct is deserialised, and `Content` stores a
/// JSON number as an already-parsed `f64`. A raw-text deserialiser — `serde_json::value::RawValue`
/// — cannot be driven from that buffer at all, for the same reason a borrowed `&str` symbol cannot
/// (see `de_tick_subscription_id`). Recovering the digits would mean giving up the derived
/// tagging for a hand-written dispatch on the crate's central wire enum.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct LseTick {
    /// Subscription identifier built from `symbol` during deserialisation.
    ///
    /// Constructing it here keeps the per-tick `format!` out of the transformer's hot path, and
    /// mirrors how every other WebSocket integration in this crate resolves a frame to its
    /// instrument.
    #[serde(rename = "symbol", deserialize_with = "de_tick_subscription_id")]
    pub subscription_id: SubscriptionId,

    /// The instant the provider stamped the tick with.
    ///
    /// # The three spellings, all measured on one connection
    /// 1. **RFC 3339** — `2026-01-02T09:37:24.760146+00:00`.
    /// 2. **The same with a space** separating date and time —
    ///    `2026-01-02 09:37:21.690159+00:00`, which is what five of thirteen sampled dataset
    ///    families send. [`DateTime::parse_from_rfc3339`] **accepts** it: RFC 3339 §5.6 permits a
    ///    space in place of the `T` by prior agreement, and `chrono` implements that lenience —
    ///    verified against both 0.4.39, the oldest this workspace admits, and the pinned 0.4.45.
    ///    It is a lenience rather than a guarantee, so
    ///    `a_space_separated_timestamp_decodes_to_the_same_instant_as_the_t_form` pins it: a future
    ///    `chrono` that tightened the grammar would fail the build rather than silently drop five
    ///    dataset families.
    /// 3. **Epoch seconds as a JSON number** — `1786091921.387`. Replayed ticks use this while
    ///    live ticks on the *same connection* use a string, so the field changes JSON type
    ///    mid-stream. A decoder typed as a string fails the moment replay is enabled.
    ///
    /// Sub-second precision also varies per symbol within one spelling: microseconds,
    /// milliseconds, and — on at least one symbol — no fractional component at all.
    ///
    /// # Every spelling is quantised to microseconds
    /// An `f64` cannot carry nanosecond resolution at epoch scale: near the present its spacing is
    /// roughly a quarter of a microsecond, so nanoseconds recovered from spelling 3 are noise.
    /// Rounding to the nearest microsecond discards that noise and recovers the provider's own
    /// precision exactly, which matters for more than tidiness — **it is what makes the three
    /// spellings of one instant decode to the same value.** The replay path re-sends ticks the live
    /// path already delivered, in a different spelling, so anything comparing the two for equality
    /// (resume arithmetic above all) depends on them agreeing.
    ///
    /// The string spellings are quantised to that same resolution rather than passed through at
    /// whatever they carry, so the agreement is *enforced* rather than observed. Every string
    /// sampled carried at most six fractional digits, but a seventh would defeat the resume
    /// comparison silently: the watermark would hold a sub-microsecond instant, `start` would go
    /// out truncated to the microsecond below it, and every replayed tick at that instant would
    /// then sort *before* the watermark and be re-emitted as a duplicate.
    ///
    /// A string that is neither spelling, including one carrying no UTC offset, is rejected rather
    /// than guessed at — a mis-parsed timestamp is a silently misdated market event.
    #[serde(rename = "ts", deserialize_with = "de_timestamp")]
    pub time_exchange: DateTime<Utc>,

    /// The tick price. Equal to [`bid`](Self::bid) on every sample taken; see the type-level note,
    /// which also covers the ~15-significant-digit ceiling this and the three fields below decode
    /// under.
    pub price: Decimal,

    /// The bid.
    pub bid: Decimal,

    /// The ask.
    pub ask: Decimal,

    /// The size traded at this tick — genuine on crypto and equities, fabricated on FX and
    /// commodities. See the type-level note.
    pub volume: Decimal,

    /// Whether the tick was served from the historical replay buffer rather than live.
    ///
    /// The provider sets this only on replayed ticks; live ticks carry no `replay` key at all, so
    /// absence means live and the default is load-bearing rather than cosmetic.
    #[serde(default)]
    pub replay: bool,
}

/// A frame received on the London Strategic Edge WebSocket data stream.
///
/// The stream carries control frames (subscription confirmations, replay boundaries, errors)
/// interleaved with ticks, on the same connection and after the handshake has completed. Anything
/// this integration does not act on decodes to [`Other`](Self::Other) so a control frame cannot
/// fail the parse and take the stream down with it; the confirmations that carry meaning to the
/// *handshake* are decoded by the subscription types instead, where they can be acted on.
///
/// Two control frames are modelled rather than collapsed into [`Other`](Self::Other), because both
/// carry something no other frame does: [`ReplayStarted`](Self::ReplayStarted) is the only evidence
/// a replay window was clamped, and [`Error`](Self::Error) is the only evidence the provider
/// rejected something after the handshake stopped reading.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseMessage {
    /// A tick.
    Tick(LseTick),

    /// The boundary frame announcing the replay window the provider actually opened.
    ///
    /// # ⚠️ `from` is the only signal that a `start` was clamped
    /// The provider retains 24 hours. A `start` further back is **silently moved forward**, with no
    /// error: a window requested 48 hours back was answered with one beginning exactly 24 hours
    /// back, and streamed from there. Comparing `from` against what was asked for is the only way
    /// to detect it.
    ///
    /// # Why it is modelled on the stream rather than caught during the handshake
    /// The provider answers `subscribe` *before* it announces the window — measured on both a
    /// crypto and an FX symbol, the order is `subscribed`, then `replay_started`, then the replayed
    /// ticks. The subscription validator stops reading the socket the moment the last confirmation
    /// arrives, so a single-symbol resumed subscription never has a `replay_started` to inspect at
    /// handshake time, and a batch never has one for its last symbol. Reading it here instead makes
    /// the check independent of frame ordering and of batch size.
    ReplayStarted {
        /// The subscription the window belongs to, built from `symbol` exactly as a tick's is.
        #[serde(rename = "symbol", deserialize_with = "de_tick_subscription_id")]
        subscription_id: SubscriptionId,

        /// The instant the provider will actually replay from.
        #[serde(deserialize_with = "de_timestamp")]
        from: DateTime<Utc>,
    },

    /// A rejection raised *after* the handshake completed.
    ///
    /// # Why the handshake cannot catch these
    /// The subscription validator stops reading the socket the moment the last confirmation
    /// arrives, so every rejection the provider raises afterwards lands here instead — a later
    /// `LIMIT_REACHED`, an `INVALID_START` on a resumed symbol, a credential that expired
    /// mid-connection. Collapsed into [`Other`](Self::Other) it would be discarded in silence,
    /// which is the one thing a rejection must never be: the provider does **not** name the symbol
    /// it rejected, so the only outward sign is a subscription that stops ticking.
    ///
    /// Both fields are optional for the same reason they are on the handshake's own rejection: a
    /// frame missing one must still decode, because failing the parse on an error frame would take
    /// the whole stream down over the message that was trying to report a problem.
    Error {
        /// The provider's error code — `LIMIT_REACHED` and `INVALID_START` are the two observed.
        #[serde(default)]
        code: Option<SmolStr>,

        /// The provider's own diagnostic.
        #[serde(default)]
        message: Option<SmolStr>,
    },

    /// Any other frame — a control frame, or one this integration does not model.
    #[serde(other)]
    Other,
}

impl Identifier<Option<SubscriptionId>> for LseMessage {
    /// A tick resolves to the subscription its symbol was registered under; anything else resolves
    /// to nothing.
    ///
    /// The `None` is what keeps control frames out of the market stream: the transformer drops an
    /// unidentifiable frame rather than attempting to convert it, so the two decoders never have to
    /// invent an event for a frame that describes none.
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Tick(tick) => Some(tick.subscription_id.clone()),
            // A replay boundary names a subscription but describes the stream rather than a market
            // event, and a rejection names no subscription at all. The transformer consumes both
            // before this point; resolving either to an instrument would only offer it to a decoder
            // that has nothing to build from it.
            Self::ReplayStarted { .. } | Self::Error { .. } | Self::Other => None,
        }
    }
}

/// Deserialise the tick's `symbol` into the [`SubscriptionId`] it was registered under.
///
/// # Why this is not typed as `&str`
/// Every symbol this feed publishes contains a `/`, and a JSON producer is free to spell that as
/// the escape `\/` — RFC 8259 lists it among the permitted escapes, and escaping it is common
/// enough that several encoders do it by default. A borrowed `&str` cannot be produced from an
/// escaped string, because unescaping needs somewhere to write the result, so a decoder typed that
/// way fails at runtime on a frame that is entirely legal. `SmolStr` accepts either spelling and
/// keeps a symbol of this length inline, so nothing is allocated for the ordinary case.
fn de_tick_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: Deserializer<'de>,
{
    SmolStr::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((LseChannel::Tick, symbol.as_str())).id())
}

/// Deserialise a WebSocket timestamp from any of the three spellings the provider uses.
///
/// Every timestamp the WebSocket sends shares this problem, not just a tick's `ts` — a replay
/// window's `from` is decoded through it too. The spellings, and why the epoch form is quantised
/// to microseconds, are documented on [`LseTick::time_exchange`], which is where a reader of the
/// public API meets them.
fn de_timestamp<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(TimestampVisitor)
}

struct TimestampVisitor;

impl Visitor<'_> for TimestampVisitor {
    type Value = DateTime<Utc>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "an RFC 3339 timestamp, the same with a space separating date and time, or epoch \
             seconds as a number",
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        parse_timestamp(value).ok_or_else(|| E::invalid_value(Unexpected::Str(value), &self))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        time_from_epoch_seconds(value)
            .ok_or_else(|| E::invalid_value(Unexpected::Float(value), &self))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // An epoch that lands on a whole second has no fractional part to print, so JSON carries
        // it as an integer and never reaches `visit_f64`.
        i64::try_from(value)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(value), &self))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        DateTime::from_timestamp(value, 0)
            .ok_or_else(|| E::invalid_value(Unexpected::Signed(value), &self))
    }
}

/// Parse spellings 1 and 2 — see [`de_timestamp`].
///
/// Both spellings take one code path because `chrono` accepts the space separator itself, as the
/// RFC 3339 §5.6 lenience it is. There is deliberately no second, laxer grammar behind this: an
/// offset RFC 3339 rejects is rejected here too, so the spellings cannot drift apart in what they
/// admit.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
        .and_then(round_to_micros)
}

/// Round an instant to the nearest microsecond.
///
/// The provider is measured never to spell more than six fractional digits, so this is a no-op on
/// every string sampled. It is applied anyway because the resume arithmetic compares instants
/// decoded from different spellings for *equality* — see [`LseTick::time_exchange`], which is where
/// that dependency is documented.
fn round_to_micros(time: DateTime<Utc>) -> Option<DateTime<Utc>> {
    // `timestamp_subsec_nanos` measures from the second below, so this rounds half upward. The
    // epoch spelling's `f64::round` rounds half away from zero, and the two agree on every instant
    // at or after the epoch -- which is every instant this feed can carry.
    let round_up = i64::from(time.timestamp_subsec_nanos() % 1_000 >= 500);

    DateTime::from_timestamp_micros(time.timestamp_micros() + round_up)
}

/// Parse spelling 3 — see [`de_timestamp`] for why this rounds to microseconds.
fn time_from_epoch_seconds(epoch: f64) -> Option<DateTime<Utc>> {
    /// Microseconds in a second, as the multiplier for the epoch conversion.
    const MICROS_PER_SECOND: f64 = 1_000_000.0;

    if !epoch.is_finite() {
        return None;
    }

    // The whole conversion stays inside `f64`'s exactly-representable integer range: epoch
    // microseconds near the present are ~1.8e15, comfortably under 2^53. Scaling first and
    // rounding once is therefore exact, where splitting into seconds and a fraction would round
    // twice for no gain.
    let micros = (epoch * MICROS_PER_SECOND).round();
    if micros < i64::MIN as f64 || micros > i64::MAX as f64 {
        return None;
    }

    // `micros` is integral (just rounded) and inside `i64`'s range (just checked), so this
    // conversion is exact -- neither fact is visible to the lint, and `f64` carries no fallible
    // conversion to `i64` that would express them.
    #[allow(clippy::cast_possible_truncation)]
    let micros = micros as i64;

    DateTime::from_timestamp_micros(micros)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A live tick, in the RFC 3339 spelling with a `T` separator.
    const LIVE_T_SEPARATED: &str = r#"{"type":"tick","symbol":"BTC/USD",
        "ts":"2026-01-02T09:37:24.760146+00:00","price":42000.5,"bid":42000.5,
        "ask":42001.0,"volume":0.00155}"#;

    /// A live tick from one of the families that separates date and time with a space.
    const LIVE_SPACE_SEPARATED: &str = r#"{"type":"tick","symbol":"EUR/USD",
        "ts":"2026-01-02 09:37:21.690159+00:00","price":1.1,"bid":1.1,"ask":1.1001,
        "volume":1.0}"#;

    #[test]
    fn a_t_separated_timestamp_decodes() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T09:37:24.760146Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(tick.price, dec!(42000.5));
        assert_eq!(tick.volume, dec!(0.00155));
    }

    /// Five of thirteen sampled dataset families send this spelling. `chrono` accepts the space as
    /// an RFC 3339 §5.6 lenience rather than a guarantee, so this test is the canary: if a future
    /// version tightened the grammar, those five families would otherwise start failing to decode
    /// silently, in production, on a dependency bump.
    #[test]
    fn a_space_separated_timestamp_decodes_to_the_same_instant_as_the_t_form() {
        let tick: LseTick = serde_json::from_str(LIVE_SPACE_SEPARATED).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T09:37:21.690159Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    /// At least one symbol publishes no fractional component at all.
    #[test]
    fn a_timestamp_without_a_subsecond_component_decodes() {
        let input = r#"{"type":"tick","symbol":"VIX/USD","ts":"2026-01-02T19:59:46+00:00",
            "price":16.9,"bid":16.9,"ask":16.93,"volume":0.0}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T19:59:46Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    /// Replayed ticks send the timestamp as a JSON number while live ticks on the same connection
    /// send a string, so the field changes JSON type mid-stream.
    #[test]
    fn a_float_epoch_timestamp_decodes() {
        let input = r#"{"type":"tick","symbol":"BTC/USD","ts":1767346644.592,"name":"Bitcoin",
            "replay":true,"price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            DateTime::from_timestamp_micros(1_767_346_644_592_000).unwrap()
        );
        assert!(tick.replay);
    }

    /// The property the resume arithmetic depends on: a replayed tick and the live tick it repeats
    /// describe the same instant in different spellings, and must decode equal. Nanosecond
    /// conversion would fail this — `f64` spacing near the present is ~0.24µs.
    #[test]
    fn the_string_and_float_spellings_of_one_instant_decode_equal() {
        let as_string = parse_timestamp("2026-01-02T09:37:24.760146+00:00").unwrap();
        let as_float = time_from_epoch_seconds(1_767_346_644.760_146).unwrap();

        assert_eq!(as_string, as_float);
    }

    /// The enforcement half of the same property: a string carrying more precision than the epoch
    /// spelling can express is quantised to microseconds rather than passed through, so the two
    /// still decode equal. Without it the watermark would hold an instant `start` cannot name, and
    /// the replay would re-deliver it as a duplicate.
    #[test]
    fn a_sub_microsecond_string_is_quantised_so_it_still_matches_the_float_spelling() {
        let seven_digits = parse_timestamp("2026-01-02T09:37:24.7601461+00:00").unwrap();
        let as_float = time_from_epoch_seconds(1_767_346_644.760_146).unwrap();

        assert_eq!(seven_digits, as_float);
        assert_eq!(seven_digits.timestamp_subsec_nanos() % 1_000, 0);

        // Rounds to nearest rather than truncating, matching what the epoch spelling recovers.
        assert_eq!(
            parse_timestamp("2026-01-02T09:37:24.7601465+00:00").unwrap(),
            "2026-01-02T09:37:24.760147Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    /// A whole-second epoch has no fractional part to print, so JSON carries it as an integer.
    #[test]
    fn an_integer_epoch_timestamp_decodes() {
        let input = r#"{"type":"tick","symbol":"BTC/USD","ts":1767346644,"replay":true,
            "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            DateTime::from_timestamp(1_767_346_644, 0).unwrap()
        );
    }

    /// Live ticks carry no `replay` key at all, so the default decides whether every live tick is
    /// mistaken for a replayed one.
    #[test]
    fn replay_defaults_to_false_when_the_key_is_absent() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert!(!tick.replay);
    }

    #[test]
    fn the_subscription_id_is_built_from_the_symbol() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert_eq!(tick.subscription_id.as_ref(), "tick|BTC/USD");
    }

    /// A timestamp carrying no offset is ambiguous, and guessing at it would silently misdate the
    /// event rather than fail.
    #[test]
    fn a_timestamp_without_an_offset_is_rejected() {
        assert!(parse_timestamp("2026-01-02 09:37:21.690159").is_none());
        assert!(parse_timestamp("2026-01-02T09:37:21.690159").is_none());
    }

    #[test]
    fn a_non_timestamp_string_is_rejected() {
        assert!(parse_timestamp("").is_none());
        assert!(parse_timestamp("not a timestamp").is_none());
    }

    #[test]
    fn a_non_finite_epoch_is_rejected() {
        assert!(time_from_epoch_seconds(f64::NAN).is_none());
        assert!(time_from_epoch_seconds(f64::INFINITY).is_none());
        assert!(time_from_epoch_seconds(f64::MAX).is_none());
    }

    #[test]
    fn a_tick_frame_decodes_as_a_tick() {
        let message: LseMessage = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert!(matches!(message, LseMessage::Tick(_)));
    }

    #[test]
    fn a_tick_identifies_itself_by_its_subscription() {
        let message: LseMessage = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert_eq!(
            message.id(),
            Some(SubscriptionId::from("tick|BTC/USD")),
            "a tick must resolve to the subscription it was registered under"
        );
    }

    /// The transformer drops a frame that identifies no subscription, which is what keeps control
    /// frames from reaching the decoders at all.
    #[test]
    fn a_control_frame_identifies_no_subscription() {
        let message: LseMessage =
            serde_json::from_str(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#)
                .unwrap();
        assert_eq!(message.id(), None);
    }

    /// Control frames share the data stream with ticks. Failing the parse on one would take the
    /// stream down over a message the transformer has no interest in.
    #[test]
    fn control_frames_decode_as_other_rather_than_failing_the_parse() {
        for input in [
            r#"{"type":"subscribed","symbol":"BTC/USD","count":1,"max":16}"#,
            r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41,"buffered_drained":9}"#,
            r#"{"type":"welcome","message":"hello","symbols_available":1}"#,
        ] {
            let message: LseMessage = serde_json::from_str(input).unwrap();
            assert_eq!(message, LseMessage::Other, "failed on {input}");
        }
    }

    /// A rejection raised after the handshake stopped reading is the one control frame that must
    /// not be discarded in silence — the provider names no symbol, so a swallowed rejection leaves
    /// a subscription that simply stops ticking as the only sign anything went wrong.
    #[test]
    fn a_rejection_decodes_as_an_error_rather_than_being_collapsed_into_other() {
        let message: LseMessage = serde_json::from_str(
            r#"{"type":"error","code":"LIMIT_REACHED","message":"Max 16 symbols"}"#,
        )
        .unwrap();

        assert_eq!(
            message,
            LseMessage::Error {
                code: Some("LIMIT_REACHED".into()),
                message: Some("Max 16 symbols".into()),
            }
        );
    }

    /// Failing the parse on an error frame would take the stream down over the message that was
    /// trying to report a problem, so a rejection missing either field must still decode.
    #[test]
    fn a_rejection_missing_its_fields_still_decodes_as_an_error() {
        let message: LseMessage = serde_json::from_str(r#"{"type":"error"}"#).unwrap();

        assert_eq!(
            message,
            LseMessage::Error {
                code: None,
                message: None,
            }
        );
    }

    /// The rejection names no subscription, so it must resolve to no instrument — the transformer
    /// consumes it, and no decoder has a market event to build from it.
    #[test]
    fn a_rejection_identifies_no_subscription() {
        let message: LseMessage =
            serde_json::from_str(r#"{"type":"error","code":"LIMIT_REACHED"}"#).unwrap();

        assert_eq!(message.id(), None);
    }

    /// `/` is in every symbol this feed publishes and RFC 8259 permits spelling it `\/`, which
    /// several JSON encoders do by default. A symbol decoded as a borrowed `&str` cannot survive
    /// that — unescaping needs somewhere to write — so this pins the owned decode.
    #[test]
    fn a_symbol_carrying_an_escaped_solidus_decodes_to_the_same_subscription() {
        let escaped: LseMessage = serde_json::from_str(
            r#"{"type":"tick","symbol":"BTC\/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#,
        )
        .unwrap();

        assert_eq!(escaped.id(), Some(SubscriptionId::from("tick|BTC/USD")));

        // The replay boundary decodes its symbol through the same function, and a clamp that went
        // undetected because the boundary frame failed to parse would be silent.
        let boundary: LseMessage = serde_json::from_str(
            r#"{"type":"replay_started","symbol":"BTC\/USD","from":"2026-01-02T09:37:24+00:00"}"#,
        )
        .unwrap();

        assert_eq!(
            boundary,
            LseMessage::ReplayStarted {
                subscription_id: SubscriptionId::from("tick|BTC/USD"),
                from: "2026-01-02T09:37:24Z".parse::<DateTime<Utc>>().unwrap(),
            }
        );
    }

    /// The replay boundary is the one control frame that is modelled, because its `from` is the
    /// only evidence a requested window was clamped to the provider's retention.
    #[test]
    fn a_replay_boundary_decodes_with_its_window_in_every_timestamp_spelling() {
        for raw in [
            r#""2026-01-02 09:39:31.716622+00:00""#,
            r#""2026-01-02T09:39:31.716622+00:00""#,
            "1767346771.716622",
        ] {
            let input = format!(r#"{{"type":"replay_started","symbol":"BTC/USD","from":{raw}}}"#);
            let message: LseMessage = serde_json::from_str(&input).unwrap();

            assert_eq!(
                message,
                LseMessage::ReplayStarted {
                    subscription_id: SubscriptionId::from("tick|BTC/USD"),
                    from: "2026-01-02T09:39:31.716622Z"
                        .parse::<DateTime<Utc>>()
                        .unwrap(),
                },
                "failed on {raw}"
            );
        }
    }

    /// It names a subscription but describes the stream, so it must not resolve to an instrument —
    /// a decoder handed one has no market event to build from it.
    #[test]
    fn a_replay_boundary_identifies_no_subscription() {
        let message: LseMessage = serde_json::from_str(
            r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:37:24+00:00"}"#,
        )
        .unwrap();

        assert_eq!(message.id(), None);
    }
}
