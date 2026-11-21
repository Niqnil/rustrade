//! Limit Up-Limit Down price bands from Alpaca's `lulds` channel.
//!
//! Each message carries the limit up price `u`, the limit down price `d`, an indicator `i` and the
//! tape `z`. A zero or missing limit is no limit, so it is `None`.
//!
//! # Indicator
//! Alpaca does not document the indicator's values. On tapes C and O it is read as the UTP plan
//! defines it:
//!
//! | `i` | [`PriceBandUpdate`] |
//! |---|---|
//! | `A` | [`Opening`](PriceBandUpdate::Opening) |
//! | `B` | [`Intraday`](PriceBandUpdate::Intraday) |
//! | `C` | [`Restated`](PriceBandUpdate::Restated) |
//! | `D` | [`SuspendedDuringHalt`](PriceBandUpdate::SuspendedDuringHalt) |
//! | `E` | [`Reopening`](PriceBandUpdate::Reopening) |
//! | `F` | [`OutsideRegularHours`](PriceBandUpdate::OutsideRegularHours) |
//!
//! Any other value, and every value on tapes A and B, whose spelling of it is not known, is
//! [`PriceBandUpdate::Other`]. The band's prices are reported either way.
//!
//! # Venue code
//! [`PriceBand::venue_code`] is `"<tape>:<i>"` as Alpaca sent them, with an empty field for a
//! missing one: `"C:B"`, `"A:B"`.

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

/// Why the band was published, from indicator `i` on `tape`.
fn update(tape: &str, indicator: &str) -> PriceBandUpdate {
    if !matches!(tape, "C" | "O") {
        return PriceBandUpdate::Other;
    }
    match indicator {
        "A" => PriceBandUpdate::Opening,
        "B" => PriceBandUpdate::Intraday,
        "C" => PriceBandUpdate::Restated,
        "D" => PriceBandUpdate::SuspendedDuringHalt,
        "E" => PriceBandUpdate::Reopening,
        "F" => PriceBandUpdate::OutsideRegularHours,
        _ => PriceBandUpdate::Other,
    }
}

/// A limit, or `None` where Alpaca published none.
fn limit(price: Option<Decimal>) -> Option<Decimal> {
    price.filter(|price| !price.is_zero())
}

/// Decodes the `lulds` channel. Every message is a band.
#[derive(Debug, Default)]
pub struct AlpacaLuldDecoder;

impl AlpacaDecoder for AlpacaLuldDecoder {
    type Frame = AlpacaLuldMessage;
    type Message = AlpacaLuld;
    type Event = PriceBand;

    fn decode(&mut self, luld: AlpacaLuld) -> Option<PriceBand> {
        let tape = luld.tape.as_deref().unwrap_or_default();
        let indicator = luld.indicator.as_deref().unwrap_or_default();

        Some(
            PriceBand::new(limit(luld.limit_up), limit(luld.limit_down))
                .with_update(update(tape, indicator))
                .with_venue_code(format_smolstr!("{tape}:{indicator}")),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn luld(input: &str) -> AlpacaLuld {
        let frame: AlpacaLuldMessage = serde_json::from_str(input).unwrap();
        let [luld] = <[AlpacaLuld; 1]>::try_from(frame.0).unwrap();
        luld
    }

    fn band(tape: &str, indicator: &str) -> PriceBand {
        AlpacaLuldDecoder
            .decode(luld(&format!(
                r#"[{{"T":"l","S":"IONM","u":3.24,"d":2.65,"i":"{indicator}","t":"2023-04-06T13:34:45Z","z":"{tape}"}}]"#
            )))
            .unwrap()
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
    fn every_indicator_on_tapes_a_and_b_is_other_with_its_code_and_prices_kept() {
        for tape in ["A", "B", ""] {
            for indicator in ["A", "B", "F"] {
                let band = band(tape, indicator);
                assert_eq!(band.update, PriceBandUpdate::Other, "{tape}:{indicator}");
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
