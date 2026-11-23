//! Limit Up-Limit Down price bands from Alpaca's `lulds` channel.
//!
//! Each message carries the limit up price `u`, the limit down price `d`, an indicator `i` and the
//! tape `z`. A zero or missing limit is no limit, so it is `None`.
//!
//! # Indicator
//! Alpaca does not document the indicator's values. The two US consolidated tape plans spell them
//! differently, so it is read by its tape, as each plan defines it:
//!
//! | Tape | `i` | Plan's meaning | [`PriceBandUpdate`] |
//! |---|---|---|---|
//! | C, O | `A` | opening update | [`Opening`](PriceBandUpdate::Opening) |
//! | C, O | `B` | intraday update | [`Intraday`](PriceBandUpdate::Intraday) |
//! | C, O | `C` | restated value | [`Restated`](PriceBandUpdate::Restated) |
//! | C, O | `D` | suspended during a halt or pause | [`SuspendedDuringHalt`](PriceBandUpdate::SuspendedDuringHalt) |
//! | C, O | `E` | reopening update | [`Reopening`](PriceBandUpdate::Reopening) |
//! | C, O | `F` | outside the hours bands apply in | [`OutsideRegularHours`](PriceBandUpdate::OutsideRegularHours) |
//! | A, B | `A` | price band, opening or intraday alike | [`Other`](PriceBandUpdate::Other) |
//! | A, B | `B` | republished price band | [`Restated`](PriceBandUpdate::Restated) |
//!
//! Any other value, and any value with no tape or an unknown one, is [`PriceBandUpdate::Other`].
//! The band's prices are reported either way.
//!
//! On tapes A and B, `C` to `J` say that the national best bid or offer entered or left a limit
//! state. They are not bands, and their prices are zero, which would read as the bands being
//! removed, so no [`PriceBand`] is produced for them: each is logged at DEBUG.
//!
//! # Venue code
//! [`PriceBand::venue_code`] is `"<tape>:<i>"` as Alpaca sent them, trimmed of padding spaces, with
//! an empty field for a missing or blank one: `"C:B"`, `"A:B"`.

use super::{
    channel::AlpacaChannel,
    decode::{AlpacaDecoder, AlpacaMessage},
};
use crate::{
    Identifier,
    exchange::ExchangeSub,
    subscription::price_band::{PriceBand, PriceBandUpdate},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Deserializer};
use smol_str::{SmolStr, format_smolstr};
use tracing::debug;

/// Deserialize "S" (symbol) field as the associated [`SubscriptionId`] for LULD bands.
fn de_luld_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((AlpacaChannel::Lulds, symbol)).id())
}

/// Alpaca Limit Up-Limit Down message.
///
/// ```json
/// {"T":"l","S":"IONM","u":3.24,"d":2.65,"i":"B","t":"2023-04-06T13:34:45.565004401Z","z":"C"}
/// ```
#[derive(Debug, Deserialize)]
pub struct AlpacaLuld {
    /// Subscription ID constructed from symbol during deserialization.
    #[serde(rename = "S", deserialize_with = "de_luld_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(rename = "u", default)]
    pub limit_up: Option<Decimal>,
    #[serde(rename = "d", default)]
    pub limit_down: Option<Decimal>,
    #[serde(rename = "i", default)]
    pub indicator: Option<SmolStr>,
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "z", default)]
    pub tape: Option<SmolStr>,
}

impl AlpacaMessage for AlpacaLuld {
    fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    fn time_exchange(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

/// An Alpaca frame's Limit Up-Limit Down messages: `[{"T":"l",...},...]`.
#[derive(Debug)]
pub struct AlpacaLuldMessage(Vec<AlpacaLuld>);

/// Dispatches on the message type in one parse pass, skipping any other type.
#[derive(Deserialize)]
#[serde(tag = "T")]
enum AlpacaArrayLuldMsg {
    #[serde(rename = "l")]
    Luld(AlpacaLuld),
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for AlpacaLuldMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let messages = Vec::<AlpacaArrayLuldMsg>::deserialize(deserializer)?;
        Ok(Self(
            messages
                .into_iter()
                .filter_map(|message| match message {
                    AlpacaArrayLuldMsg::Luld(luld) => Some(luld),
                    AlpacaArrayLuldMsg::Other => None,
                })
                .collect(),
        ))
    }
}

impl IntoIterator for AlpacaLuldMessage {
    type Item = AlpacaLuld;
    type IntoIter = std::vec::IntoIter<AlpacaLuld>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Why the band was published, from indicator `i` on `tape`, or `None` when the message is not a
/// band: a limit state on tapes A and B.
fn update(tape: &str, indicator: &str) -> Option<PriceBandUpdate> {
    Some(match (tape, indicator) {
        ("C" | "O", "A") => PriceBandUpdate::Opening,
        ("C" | "O", "B") => PriceBandUpdate::Intraday,
        ("C" | "O", "C") => PriceBandUpdate::Restated,
        ("C" | "O", "D") => PriceBandUpdate::SuspendedDuringHalt,
        ("C" | "O", "E") => PriceBandUpdate::Reopening,
        ("C" | "O", "F") => PriceBandUpdate::OutsideRegularHours,
        ("A" | "B", "B") => PriceBandUpdate::Restated,
        ("A" | "B", "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J") => return None,
        _ => PriceBandUpdate::Other,
    })
}

/// A limit, or `None` where Alpaca published none.
fn limit(price: Option<Decimal>) -> Option<Decimal> {
    price.filter(|price| !price.is_zero())
}

/// Decodes the `lulds` channel, dropping a limit state on tapes A and B, which is not a band.
#[derive(Debug, Default)]
pub struct AlpacaLuldDecoder;

impl AlpacaDecoder for AlpacaLuldDecoder {
    type Frame = AlpacaLuldMessage;
    type Message = AlpacaLuld;
    type Event = PriceBand;

    fn decode(&mut self, luld: AlpacaLuld) -> Option<PriceBand> {
        let tape = luld.tape.as_deref().unwrap_or_default().trim();
        let indicator = luld.indicator.as_deref().unwrap_or_default().trim();

        let Some(update) = update(tape, indicator) else {
            debug!(
                subscription = %luld.subscription_id,
                tape,
                indicator,
                "Alpaca LULD message is a limit state, not a band; not reported"
            );
            return None;
        };
        Some(
            PriceBand::new(limit(luld.limit_up), limit(luld.limit_down))
                .with_update(update)
                .with_venue_code(format_smolstr!("{tape}:{indicator}")),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_instrument::test_utils::logs::capture;
    use tracing::Level;

    fn luld(input: &str) -> AlpacaLuld {
        let frame: AlpacaLuldMessage = serde_json::from_str(input).unwrap();
        let [luld] = <[AlpacaLuld; 1]>::try_from(frame.0).unwrap();
        luld
    }

    fn decode(tape: &str, indicator: &str) -> Option<PriceBand> {
        AlpacaLuldDecoder.decode(luld(&format!(
            r#"[{{"T":"l","S":"IONM","u":3.24,"d":2.65,"i":"{indicator}","t":"2023-04-06T13:34:45Z","z":"{tape}"}}]"#
        )))
    }

    fn band(tape: &str, indicator: &str) -> PriceBand {
        decode(tape, indicator).unwrap()
    }

    #[test]
    fn deserialises_and_decodes_alpacas_documented_example() {
        let input = r#"[{"T":"l","S":"IONM","u":3.24,"d":2.65,"i":"B","t":"2023-04-06T13:34:45.565004401Z","z":"C"},{"T":"s","S":"IONM","sc":"H","t":"2023-04-06T13:34:45Z","z":"C"}]"#;
        let luld = luld(input);
        assert_eq!(luld.subscription_id.as_ref(), "lulds|IONM");
        assert_eq!(
            luld.timestamp.to_rfc3339(),
            "2023-04-06T13:34:45.565004401+00:00"
        );

        let band = AlpacaLuldDecoder.decode(luld).unwrap();

        assert_eq!(
            band,
            PriceBand::new(Some(dec!(3.24)), Some(dec!(2.65)))
                .with_update(PriceBandUpdate::Intraday)
                .with_venue_code("C:B")
        );
    }

    #[test]
    fn reads_the_indicator_as_the_utp_plan_defines_it_on_tapes_c_and_o() {
        use PriceBandUpdate::*;
        let cases = [
            ("A", Opening),
            ("B", Intraday),
            ("C", Restated),
            ("D", SuspendedDuringHalt),
            ("E", Reopening),
            ("F", OutsideRegularHours),
            ("G", Other),
            ("", Other),
        ];
        for tape in ["C", "O"] {
            for (indicator, update) in cases {
                let band = band(tape, indicator);
                assert_eq!(band.update, update, "{tape}:{indicator}");
                assert_eq!(
                    band.venue_code.as_deref(),
                    Some(format!("{tape}:{indicator}").as_str())
                );
            }
        }
    }

    #[test]
    fn reads_the_indicator_as_the_cta_plan_defines_it_on_tapes_a_and_b() {
        use PriceBandUpdate::*;
        let cases = [("A", Other), ("B", Restated), ("K", Other), ("", Other)];
        for tape in ["A", "B"] {
            for (indicator, update) in cases {
                let band = band(tape, indicator);
                assert_eq!(band.update, update, "{tape}:{indicator}");
                assert_eq!(
                    band.venue_code.as_deref(),
                    Some(format!("{tape}:{indicator}").as_str())
                );
                assert_eq!(
                    (band.upper, band.lower),
                    (Some(dec!(3.24)), Some(dec!(2.65)))
                );
            }
        }
    }

    #[test]
    fn a_limit_state_on_tapes_a_and_b_is_dropped_at_debug() {
        for tape in ["A", "B"] {
            for indicator in ["C", "D", "E", "F", "G", "H", "I", "J"] {
                let (decoded, events) = capture(|| decode(tape, indicator));

                assert_eq!(decoded, None, "{tape}:{indicator}");
                assert_eq!(events.len(), 1, "{tape}:{indicator}: {events:?}");
                assert_eq!(events[0].level, Level::DEBUG, "{tape}:{indicator}");
            }
        }
    }

    #[test]
    fn an_indicator_with_no_tape_or_an_unknown_one_is_other() {
        for tape in ["", "X"] {
            for indicator in ["A", "B", "C", "F"] {
                let band = band(tape, indicator);
                assert_eq!(band.update, PriceBandUpdate::Other, "{tape}:{indicator}");
            }
        }
    }

    #[test]
    fn padding_spaces_are_trimmed_from_the_tape_and_indicator() {
        let padded = band(" B ", "B ");
        assert_eq!(padded.update, PriceBandUpdate::Restated);
        assert_eq!(padded.venue_code.as_deref(), Some("B:B"));

        let blank = band("C", " ");
        assert_eq!(blank.update, PriceBandUpdate::Other);
        assert_eq!(blank.venue_code.as_deref(), Some("C:"));
    }

    #[test]
    fn a_sub_penny_limit_is_kept_exactly_and_a_null_one_is_none() {
        let luld =
            luld(r#"[{"T":"l","S":"X","u":3.2451,"d":null,"i":"B","t":"2023-04-06T13:34:45Z"}]"#);

        let band = AlpacaLuldDecoder.decode(luld).unwrap();

        assert_eq!((band.upper, band.lower), (Some(dec!(3.2451)), None));
    }

    #[test]
    fn a_zero_or_missing_limit_is_none() {
        let zero = luld(r#"[{"T":"l","S":"X","u":0,"d":2.65,"t":"2023-04-06T13:34:45Z"}]"#);
        let missing = luld(r#"[{"T":"l","S":"X","u":3.24,"t":"2023-04-06T13:34:45Z"}]"#);

        let zero = AlpacaLuldDecoder.decode(zero).unwrap();
        let missing = AlpacaLuldDecoder.decode(missing).unwrap();

        assert_eq!((zero.upper, zero.lower), (None, Some(dec!(2.65))));
        assert_eq!((missing.upper, missing.lower), (Some(dec!(3.24)), None));
        // No tape and no indicator: both fields of the code are empty.
        assert_eq!(missing.venue_code.as_deref(), Some(":"));
        assert_eq!(missing.update, PriceBandUpdate::Other);
    }
}
