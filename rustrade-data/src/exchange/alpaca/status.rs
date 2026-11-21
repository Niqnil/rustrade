//! Trading status from Alpaca's `statuses` channel.
//!
//! Each message carries a status code `sc`, a reason code `rc` and the tape `z`. The two US
//! consolidated tape plans spell their codes differently, and their single-letter codes overlap, so
//! a code is read by its tape: tapes A and B (CTA) and tapes C and O (UTP).
//!
//! # Status codes
//!
//! | Tape | `sc` | Meaning | [`TradingState`] |
//! |---|---|---|---|
//! | A, B | `2` | trading halt | [`Halted`](TradingState::Halted); [`Paused`](TradingState::Paused) when `rc` is `M` (a Limit Up-Limit Down pause) |
//! | A, B | `3` | resume | [`Trading`](TradingState::Trading) |
//! | C, O | `H` | trading halt | [`Halted`](TradingState::Halted); [`Paused`](TradingState::Paused) when `rc` is `LUDP`, `LUDS`, `T5` or `T7` |
//! | C, O | `P` | volatility trading pause | [`Paused`](TradingState::Paused) |
//! | C, O | `Q` | quotation resumption | [`QuoteOnly`](TradingState::QuoteOnly) |
//! | C, O | `T` | trading resumption | [`Trading`](TradingState::Trading) |
//!
//! A pause, by either route, has [`HaltReason::Volatility`].
//!
//! # Codes that report no trading state
//! Tapes A and B also carry price and trading range indications (`5`, `6`), order imbalances
//! (`7`, `8`, `9`, `A`, `C`, `D`), a short sale restriction (`E`), and a Limit Up-Limit Down code
//! (`F`). None says whether the instrument can trade, and reporting one as a state would overwrite
//! the real one, so no [`TradingStatus`] is produced for them: each is logged at DEBUG. A status
//! code Alpaca does not document, or a message with no tape or a tape other than A, B, C or O, is
//! dropped too, with a WARN the first time each tape and code arrive on a connection.
//!
//! # Reasons
//! A halt's or pause's `rc` maps to a [`HaltReason`], and a code that maps to none of them is
//! [`HaltReason::Other`]. A resumption has no reason: its `rc` says why trading resumed, which is
//! not a [`HaltReason`].
//!
//! # Venue code
//! [`TradingStatus::venue_code`] is `"<tape>:<sc>:<rc>"` as Alpaca sent them, always with three
//! fields and an empty one for a missing code: `"C:H:T12"`, `"A:2:M"`, `"A:3:"`.

use super::{
    channel::AlpacaChannel,
    decode::{AlpacaDecoder, AlpacaMessage},
};
use crate::{
    Identifier,
    exchange::ExchangeSub,
    subscription::trading_status::{HaltReason, TradingState, TradingStatus},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashSet;
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Deserializer};
use smol_str::{SmolStr, format_smolstr};
use tracing::{debug, warn};

/// Deserialize "S" (symbol) field as the associated [`SubscriptionId`] for statuses.
fn de_status_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((AlpacaChannel::Statuses, symbol)).id())
}

/// Alpaca trading status message.
///
/// ```json
/// {"T":"s","S":"AAPL","sc":"H","sm":"Trading Halt","rc":"T12","rm":"Trading Halted; For information requested by NASDAQ","t":"2021-02-22T19:15:00Z","z":"C"}
/// ```
///
/// The status and reason messages (`sm`, `rm`) restate the codes in words and are not read.
#[derive(Debug, Deserialize)]
pub struct AlpacaStatus {
    /// Subscription ID constructed from symbol during deserialization.
    #[serde(rename = "S", deserialize_with = "de_status_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(rename = "sc", default)]
    pub status_code: Option<SmolStr>,
    #[serde(rename = "rc", default)]
    pub reason_code: Option<SmolStr>,
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "z", default)]
    pub tape: Option<SmolStr>,
}

impl AlpacaMessage for AlpacaStatus {
    fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    fn time_exchange(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

/// An Alpaca frame's trading status messages: `[{"T":"s",...},...]`.
#[derive(Debug)]
pub struct AlpacaStatusMessage(Vec<AlpacaStatus>);

/// Dispatches on the message type in one parse pass, skipping any other type.
#[derive(Deserialize)]
#[serde(tag = "T")]
enum AlpacaArrayStatusMsg {
    #[serde(rename = "s")]
    Status(AlpacaStatus),
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for AlpacaStatusMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let messages = Vec::<AlpacaArrayStatusMsg>::deserialize(deserializer)?;
        Ok(Self(
            messages
                .into_iter()
                .filter_map(|message| match message {
                    AlpacaArrayStatusMsg::Status(status) => Some(status),
                    AlpacaArrayStatusMsg::Other => None,
                })
                .collect(),
        ))
    }
}

impl IntoIterator for AlpacaStatusMessage {
    type Item = AlpacaStatus;
    type IntoIter = std::vec::IntoIter<AlpacaStatus>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// The consolidated tape plan a tape's codes belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Tapes A and B.
    Cta,
    /// Tapes C and O.
    Utp,
}

impl Plan {
    fn of(tape: &str) -> Option<Self> {
        match tape {
            "A" | "B" => Some(Self::Cta),
            "C" | "O" => Some(Self::Utp),
            _ => None,
        }
    }
}

/// What a status code reports.
#[derive(Debug, PartialEq)]
enum Reading {
    /// A trading state.
    State(TradingStatus),
    /// A documented code that is not a trading state.
    NotState,
    /// A code Alpaca does not document for the tape.
    Undocumented,
}

/// What status code `sc` with reason code `rc` reports on `plan`'s tapes.
fn read(plan: Plan, sc: &str, rc: &str) -> Reading {
    let status = match (plan, sc) {
        (Plan::Cta, "2") => halt(cta_reason(rc)),
        (Plan::Utp, "H") => halt(utp_reason(rc)),
        (Plan::Utp, "P") => {
            TradingStatus::new(TradingState::Paused).with_reason(HaltReason::Volatility)
        }
        (Plan::Utp, "Q") => TradingStatus::new(TradingState::QuoteOnly),
        (Plan::Cta, "3") | (Plan::Utp, "T") => TradingStatus::new(TradingState::Trading),
        (Plan::Cta, "5" | "6" | "7" | "8" | "9" | "A" | "C" | "D" | "E" | "F") => {
            return Reading::NotState;
        }
        _ => return Reading::Undocumented,
    };
    Reading::State(status)
}

/// A halt with `reason`: a pause when the reason is volatility, which a volatility halt is.
fn halt(reason: Option<HaltReason>) -> TradingStatus {
    match reason {
        Some(HaltReason::Volatility) => {
            TradingStatus::new(TradingState::Paused).with_reason(HaltReason::Volatility)
        }
        Some(reason) => TradingStatus::new(TradingState::Halted).with_reason(reason),
        None => TradingStatus::new(TradingState::Halted),
    }
}

/// The reason a CTA halt's reason code gives, or `None` when it gives none.
fn cta_reason(rc: &str) -> Option<HaltReason> {
    Some(match rc {
        "" => return None,
        "A" | "C" => HaltReason::Regulatory,
        "D" | "P" => HaltReason::News,
        "E" | "N" => HaltReason::CorporateAction,
        "I" => HaltReason::OrderImbalance,
        "M" => HaltReason::Volatility,
        "X" => HaltReason::Operational,
        "1" | "2" | "3" => HaltReason::MarketWideCircuitBreaker,
        _ => HaltReason::Other,
    })
}

/// The reason a UTP halt's reason code gives, or `None` when it gives none.
fn utp_reason(rc: &str) -> Option<HaltReason> {
    Some(match rc {
        "" => return None,
        "T1" | "T2" | "T3" => HaltReason::News,
        "T6" | "T12" | "H4" | "H9" | "H10" | "H11" => HaltReason::Regulatory,
        "T5" | "T7" | "LUDP" | "LUDS" => HaltReason::Volatility,
        "M1" => HaltReason::CorporateAction,
        // Alpaca's table spells the operations halt with a zero; the UTP plan, with the letter O.
        "O1" | "01" => HaltReason::Operational,
        "MWC0" | "MWC1" | "MWC2" | "MWC3" => HaltReason::MarketWideCircuitBreaker,
        _ => HaltReason::Other,
    })
}

/// Decodes the `statuses` channel, logging each undocumented tape and code once per connection.
#[derive(Debug, Default)]
pub struct AlpacaStatusDecoder {
    warned: FnvHashSet<(SmolStr, SmolStr)>,
}

impl AlpacaStatusDecoder {
    fn warn_once(&mut self, status: &AlpacaStatus, tape: &str, sc: &str, why: &str) {
        if self.warned.insert((SmolStr::new(tape), SmolStr::new(sc))) {
            warn!(
                subscription = %status.subscription_id,
                tape,
                status_code = sc,
                why,
                "dropping an Alpaca trading status message; further ones with this tape and code \
                 on this connection are dropped without a warning"
            );
        }
    }
}

impl AlpacaDecoder for AlpacaStatusDecoder {
    type Frame = AlpacaStatusMessage;
    type Message = AlpacaStatus;
    type Event = TradingStatus;

    fn decode(&mut self, status: AlpacaStatus) -> Option<TradingStatus> {
        let tape = status.tape.as_deref().unwrap_or_default();
        let sc = status.status_code.as_deref().unwrap_or_default();
        let rc = status.reason_code.as_deref().unwrap_or_default();

        let Some(plan) = Plan::of(tape) else {
            self.warn_once(
                &status,
                tape,
                sc,
                "no tape, or a tape Alpaca does not document",
            );
            return None;
        };

        match read(plan, sc, rc) {
            Reading::State(trading_status) => {
                Some(trading_status.with_venue_code(format_smolstr!("{tape}:{sc}:{rc}")))
            }
            Reading::NotState => {
                debug!(
                    subscription = %status.subscription_id,
                    tape,
                    status_code = sc,
                    "Alpaca status code reports no trading state; not reported"
                );
                None
            }
            Reading::Undocumented => {
                self.warn_once(&status, tape, sc, "a status code Alpaca does not document");
                None
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::test_utils::logs::capture;
    use tracing::Level;

    fn status(tape: Option<&str>, sc: Option<&str>, rc: Option<&str>) -> AlpacaStatus {
        AlpacaStatus {
            subscription_id: SubscriptionId::from("statuses|AAPL"),
            status_code: sc.map(SmolStr::new),
            reason_code: rc.map(SmolStr::new),
            timestamp: DateTime::<Utc>::MIN_UTC,
            tape: tape.map(SmolStr::new),
        }
    }

    fn decode(tape: &str, sc: &str, rc: &str) -> Option<TradingStatus> {
        AlpacaStatusDecoder::default().decode(status(Some(tape), Some(sc), Some(rc)))
    }

    fn expected(state: TradingState, reason: Option<HaltReason>, code: &str) -> TradingStatus {
        TradingStatus {
            state,
            reason,
            venue_code: Some(code.into()),
        }
    }

    #[test]
    fn deserialises_alpacas_documented_example() {
        let input = r#"[{"T":"s","S":"AAPL","sc":"H","sm":"Trading Halt","rc":"T12","rm":"Trading Halted; For information requested by NASDAQ","t":"2021-02-22T19:15:00Z","z":"C"},{"T":"l","S":"AAPL","u":1,"d":1,"i":"B","t":"2021-02-22T19:15:00Z","z":"C"}]"#;

        let frame: AlpacaStatusMessage = serde_json::from_str(input).unwrap();

        let [status] = <[AlpacaStatus; 1]>::try_from(frame.0).unwrap();
        assert_eq!(status.subscription_id.as_ref(), "statuses|AAPL");
        assert_eq!(status.status_code.as_deref(), Some("H"));
        assert_eq!(status.reason_code.as_deref(), Some("T12"));
        assert_eq!(status.tape.as_deref(), Some("C"));
        assert_eq!(status.timestamp.to_rfc3339(), "2021-02-22T19:15:00+00:00");
    }

    #[test]
    fn a_message_missing_its_codes_and_tape_still_deserialises() {
        let input = r#"[{"T":"s","S":"AAPL","t":"2021-02-22T19:15:00Z"}]"#;

        let frame: AlpacaStatusMessage = serde_json::from_str(input).unwrap();

        let status = &frame.0[0];
        assert_eq!(
            (&status.status_code, &status.reason_code, &status.tape),
            (&None, &None, &None)
        );
    }

    #[test]
    fn maps_each_state_code_on_its_own_tapes() {
        use HaltReason::*;
        use TradingState::*;
        let cases = [
            ("A", "2", "D", Halted, Some(News), "A:2:D"),
            ("B", "2", "M", Paused, Some(Volatility), "B:2:M"),
            ("A", "2", "", Halted, None, "A:2:"),
            ("A", "3", "D", Trading, None, "A:3:D"),
            ("C", "H", "T12", Halted, Some(Regulatory), "C:H:T12"),
            ("C", "H", "", Halted, None, "C:H:"),
            ("O", "H", "LUDP", Paused, Some(Volatility), "O:H:LUDP"),
            ("C", "H", "LUDS", Paused, Some(Volatility), "C:H:LUDS"),
            ("C", "H", "T5", Paused, Some(Volatility), "C:H:T5"),
            ("C", "H", "T7", Paused, Some(Volatility), "C:H:T7"),
            ("C", "P", "", Paused, Some(Volatility), "C:P:"),
            ("C", "Q", "IPOQ", QuoteOnly, None, "C:Q:IPOQ"),
            ("O", "T", "R4", Trading, None, "O:T:R4"),
        ];
        for (tape, sc, rc, state, reason, code) in cases {
            assert_eq!(
                decode(tape, sc, rc),
                Some(expected(state, reason, code)),
                "{code}"
            );
        }
    }

    #[test]
    fn a_code_is_read_by_its_tape_not_its_spelling() {
        // `T` resumes trading on a UTP tape and means nothing on a CTA one; `2` halts on a CTA
        // tape and means nothing on a UTP one.
        assert_eq!(decode("A", "T", ""), None);
        assert_eq!(decode("C", "2", ""), None);
        assert_eq!(decode("C", "A", ""), None);
    }

    #[test]
    fn maps_each_cta_halt_reason() {
        use HaltReason::*;
        let cases = [
            ("A", Regulatory),
            ("C", Regulatory),
            ("D", News),
            ("P", News),
            ("E", CorporateAction),
            ("N", CorporateAction),
            ("I", OrderImbalance),
            ("X", Operational),
            ("1", MarketWideCircuitBreaker),
            ("2", MarketWideCircuitBreaker),
            ("3", MarketWideCircuitBreaker),
            ("F", Other),
            ("O", Other),
            ("V", Other),
            ("Y", Other),
            ("ZZ", Other),
        ];
        for (rc, reason) in cases {
            let status = decode("A", "2", rc).unwrap();
            assert_eq!(
                (status.state, status.reason),
                (TradingState::Halted, Some(reason)),
                "{rc}"
            );
        }
    }

    #[test]
    fn maps_each_utp_halt_reason() {
        use HaltReason::*;
        let cases = [
            ("T1", News),
            ("T2", News),
            ("T3", News),
            ("T6", Regulatory),
            ("T12", Regulatory),
            ("H4", Regulatory),
            ("H9", Regulatory),
            ("H10", Regulatory),
            ("H11", Regulatory),
            ("M1", CorporateAction),
            ("O1", Operational),
            ("01", Operational),
            ("MWC0", MarketWideCircuitBreaker),
            ("MWC1", MarketWideCircuitBreaker),
            ("MWC2", MarketWideCircuitBreaker),
            ("MWC3", MarketWideCircuitBreaker),
            ("T8", Other),
            ("IPO1", Other),
            ("M2", Other),
            ("ZZ", Other),
        ];
        for (rc, reason) in cases {
            let status = decode("C", "H", rc).unwrap();
            assert_eq!(
                (status.state, status.reason),
                (TradingState::Halted, Some(reason)),
                "{rc}"
            );
        }
    }

    #[test]
    fn a_code_that_is_not_a_state_is_dropped_at_debug() {
        for sc in ["5", "6", "7", "8", "9", "A", "C", "D", "E", "F"] {
            let (decoded, events) = capture(|| decode("B", sc, ""));

            assert_eq!(decoded, None, "{sc}");
            assert_eq!(events.len(), 1, "{sc}: {events:?}");
            assert_eq!(events[0].level, Level::DEBUG, "{sc}");
        }
    }

    #[test]
    fn an_undocumented_code_or_tape_is_dropped_with_one_warning_per_tape_and_code() {
        let mut decoder = AlpacaStatusDecoder::default();
        let messages = [
            status(Some("C"), Some("Z"), None),
            status(Some("C"), Some("Z"), Some("T1")),
            status(Some("A"), Some("Z"), None),
            status(Some("X"), Some("H"), None),
            status(None, Some("H"), None),
            status(None, Some("H"), None),
            status(Some("C"), None, None),
        ];

        let (decoded, events) = capture(|| {
            messages
                .into_iter()
                .map(|message| decoder.decode(message))
                .collect::<Vec<_>>()
        });

        assert!(decoded.iter().all(Option::is_none), "{decoded:?}");
        // (C, Z), (A, Z), (X, H), (no tape, H), (C, no code): the repeats warn nothing.
        assert_eq!(events.len(), 5, "{events:?}");
        assert!(events.iter().all(|event| event.level == Level::WARN));
    }
}
