//! Trading status from IB's Halted tick.
//!
//! IB reports whether a contract is halted on its market data subscription, as tick type 49
//! (`Halted`), or 90 (`DelayedHalted`) under delayed data, with no generic tick to ask for. Its
//! values:
//!
//! | Value | IB's meaning | [`TradingStatus`] |
//! |---|---|---|
//! | -1 | status not available, usually with frozen data | [`TradingState::Unknown`] |
//! | 0 | not halted | [`TradingState::Trading`] |
//! | 1 | general halt, for regulatory reasons | [`TradingState::Halted`], [`HaltReason::Regulatory`] |
//! | 2 | volatility halt, imposed by the exchange | [`TradingState::Paused`], [`HaltReason::Volatility`] |
//!
//! The value is kept as the status's venue code. IB documents 0 as sent only for a contract in a
//! TWS watchlist, so a contract that trades normally may never report a status at all.

use crate::subscription::trading_status::{HaltReason, TradingState, TradingStatus};
use ibapi::{contracts::tick_types::TickType, market_data::realtime::TickGeneric};
use smol_str::format_smolstr;
use tracing::warn;

/// The trading status a generic tick reports, if it is a Halted tick.
///
/// A value IB does not document is reported as [`TradingState::Unknown`], with the value as the
/// venue code, and logged.
pub(crate) fn trading_status_from_tick(tick: &TickGeneric) -> Option<TradingStatus> {
    if !matches!(tick.tick_type, TickType::Halted | TickType::DelayedHalted) {
        return None;
    }
    let value = tick.value;
    let status = if value == -1.0 {
        TradingStatus::new(TradingState::Unknown)
    } else if value == 0.0 {
        TradingStatus::new(TradingState::Trading)
    } else if value == 1.0 {
        TradingStatus::new(TradingState::Halted).with_reason(HaltReason::Regulatory)
    } else if value == 2.0 {
        TradingStatus::new(TradingState::Paused).with_reason(HaltReason::Volatility)
    } else {
        warn!(
            value,
            tick_type = ?tick.tick_type,
            "IB Halted tick has a value IB does not document; reporting the state as unknown"
        );
        TradingStatus::new(TradingState::Unknown)
    };
    Some(status.with_venue_code(format_smolstr!("{value}")))
}

/// The changes in one subscription's trading status: IB may repeat a Halted value, and only a
/// change is an event.
#[derive(Debug, Default)]
pub(crate) struct StatusChanges(Option<TradingStatus>);

impl StatusChanges {
    /// The status `tick` reports, if it is a Halted tick whose status differs from the last one.
    pub(crate) fn change(&mut self, tick: &TickGeneric) -> Option<TradingStatus> {
        let status = trading_status_from_tick(tick)?;
        if self.0.as_ref() == Some(&status) {
            return None;
        }
        self.0 = Some(status.clone());
        Some(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(tick_type: TickType, value: f64) -> TickGeneric {
        TickGeneric { tick_type, value }
    }

    #[test]
    fn maps_each_documented_halted_value() {
        let cases = [
            (-1.0, TradingState::Unknown, None, "-1"),
            (0.0, TradingState::Trading, None, "0"),
            (1.0, TradingState::Halted, Some(HaltReason::Regulatory), "1"),
            (2.0, TradingState::Paused, Some(HaltReason::Volatility), "2"),
        ];
        for (value, state, reason, code) in cases {
            let status = trading_status_from_tick(&tick(TickType::Halted, value));

            let expected = TradingStatus {
                state,
                reason,
                venue_code: Some(code.into()),
            };
            assert_eq!(status, Some(expected), "value {value}");
        }
    }

    #[test]
    fn the_delayed_halted_tick_maps_the_same() {
        let status = trading_status_from_tick(&tick(TickType::DelayedHalted, 1.0));

        assert_eq!(
            status,
            Some(
                TradingStatus::new(TradingState::Halted)
                    .with_reason(HaltReason::Regulatory)
                    .with_venue_code("1")
            )
        );
    }

    #[test]
    fn an_undocumented_value_is_unknown_with_its_code() {
        let status = trading_status_from_tick(&tick(TickType::Halted, 3.0));

        assert_eq!(
            status,
            Some(TradingStatus::new(TradingState::Unknown).with_venue_code("3"))
        );
    }

    #[test]
    fn other_generic_ticks_are_not_a_status() {
        assert_eq!(
            trading_status_from_tick(&tick(TickType::Shortable, 3.0)),
            None
        );
    }

    #[test]
    fn only_a_change_in_status_is_reported() {
        let mut changes = StatusChanges::default();

        let reported: Vec<_> = [0.0, 0.0, 1.0, 1.0, 0.0]
            .into_iter()
            .map(|value| {
                changes
                    .change(&tick(TickType::Halted, value))
                    .map(|status| status.state)
            })
            .collect();

        assert_eq!(
            reported,
            [
                Some(TradingState::Trading),
                None,
                Some(TradingState::Halted),
                None,
                Some(TradingState::Trading),
            ]
        );
    }

    #[test]
    fn other_generic_ticks_leave_the_last_status_alone() {
        let mut changes = StatusChanges::default();
        changes.change(&tick(TickType::Halted, 1.0));

        assert_eq!(changes.change(&tick(TickType::Shortable, 3.0)), None);
        assert_eq!(changes.change(&tick(TickType::Halted, 1.0)), None);
    }
}
