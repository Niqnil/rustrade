//! Lapse instructions: a long option the holder instructs not to exercise lapses at expiry, in
//! the money or not, and the instructions the venue refuses.

use super::{
    options::{
        balance, buy, driven_instruments, driven_venue, expiry, feed, funds, option_name,
        option_position, sell, spy, spy_name, trades, usd,
    },
    *,
};
use crate::{
    client::mock::SessionCalendarConfig,
    error::ExerciseRefusal,
    exchange::mock::venue::VenueRestoreError,
    exercise::{ExerciseAck, ExerciseInstruction, ExerciseQuantity},
    expiry::{ExpiryAnchor, ExpirySellOut, LapseDeadline},
};
use rustrade_instrument::{
    calendar::DailyClose,
    instrument::kind::option::{OptionKind, OptionSettlement},
};
use std::time::Duration;

/// A lapse of the whole position.
fn lapse() -> ExerciseInstruction {
    ExerciseInstruction::Lapse {
        contracts: ExerciseQuantity::WholePosition,
    }
}

/// A venue holding `contracts` of a physically settled call bought at 1.5 with SPY at 100, then
/// with SPY at 120 and the call at 20: in the money by 20.
fn holding_calls(
    contracts: &str,
    configure: impl FnOnce(&mut MockExecutionConfig),
) -> SimulatedVenue {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, configure);
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, contracts);
    feed(&mut venue, "120", "20", time(50));
    venue
}

/// Instructs `venue` to lapse the option's whole position, asserting it is accepted.
fn instruct_lapse(venue: &mut SimulatedVenue) {
    assert_eq!(
        venue.instruct_exercise(&option_name(), lapse()),
        Ok(ExerciseAck::new(lapse()))
    );
}

/// The refusal `venue` answers a lapse of the whole option position with.
fn refusal(venue: &mut SimulatedVenue, instruction: ExerciseInstruction) -> ExerciseRefusal {
    match venue.instruct_exercise(&option_name(), instruction) {
        Err(ApiError::ExerciseRefused(refusal)) => refusal,
        other => panic!("expected an exercise refusal, got {other:?}"),
    }
}

/// Asserts `events` settle the option by lapsing `contracts` of it at zero, at expiry, with
/// nothing delivered.
fn assert_lapsed(events: &[UnindexedAccountEvent], contracts: &str) {
    let trades = trades(events);
    assert_eq!(trades.len(), 1, "only the option leaves: {events:?}");
    let trade = trades[0];
    assert_eq!(trade.instrument, option_name());
    assert_eq!(
        (trade.origin, trade.side, trade.price, trade.quantity),
        (TradeOrigin::Expiry, Side::Sell, d("0"), d(contracts))
    );
    assert_eq!(trade.time_exchange, expiry());
}

/// Asserts `events` exercise the option, buying the underlying at the strike.
fn assert_exercised(events: &[UnindexedAccountEvent]) {
    let origins = trades(events)
        .iter()
        .map(|trade| (trade.instrument.clone(), trade.origin))
        .collect::<Vec<_>>();
    assert_eq!(
        origins,
        [
            (option_name(), TradeOrigin::Exercise),
            (spy_name(), TradeOrigin::Exercise)
        ],
        "{events:?}"
    );
}

/// On a venue that exercises in the money, an instructed long lapses at zero instead, and no
/// shares are delivered or paid for.
#[test]
fn an_instructed_long_lapses_at_zero_in_the_money() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);

    let events = venue.advance_time(expiry());

    assert_lapsed(&events, "1");
    assert_eq!(option_position(&venue), None);
    assert_eq!(balance(&venue, spy()).total, Decimal::ZERO);
    assert_eq!(balance(&venue, usd()).total, d("10000") - d("150"));
}

/// The same position uninstructed is exercised: the instruction is what changes the outcome.
#[test]
fn an_uninstructed_long_is_exercised() {
    let mut venue = holding_calls("1", |_| {});

    assert_exercised(&venue.advance_time(expiry()));
}

/// A lapse needs no price for the underlying, so an instructed long settles at its expiry even
/// with none, where an exercised one would wait for a price.
#[test]
fn an_instructed_long_lapses_without_an_underlying_price() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");
    instruct_lapse(&mut venue);

    assert_lapsed(&venue.advance_time(expiry()), "1");
    assert_eq!(option_position(&venue), None);
}

/// The instruction holds for the whole long at expiry: contracts bought after it lapse too.
#[test]
fn contracts_bought_after_the_instruction_lapse_too() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);
    buy(&mut venue, "1");

    assert_lapsed(&venue.advance_time(expiry()), "2");
}

/// A sale that leaves part of the long leaves that part instructed.
#[test]
fn the_rest_of_a_partly_sold_long_lapses() {
    let mut venue = holding_calls("2", |_| {});
    instruct_lapse(&mut venue);
    let _ = sell(&mut venue, "1");

    assert_lapsed(&venue.advance_time(expiry()), "1");
}

/// A long closed after the instruction loses it, so one opened again is exercised.
#[test]
fn a_long_closed_and_opened_again_is_exercised() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);
    let _ = sell(&mut venue, "1");
    buy(&mut venue, "1");

    assert_exercised(&venue.advance_time(expiry()));
}

/// Instructing the same long again is accepted again, and changes nothing.
#[test]
fn a_repeated_instruction_is_accepted() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);
    instruct_lapse(&mut venue);

    assert_lapsed(&venue.advance_time(expiry()), "1");
}

#[test]
fn an_instruction_without_a_position_is_refused() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));

    assert_eq!(refusal(&mut venue, lapse()), ExerciseRefusal::NoPosition);
}

/// Only the holder instructs: a short put, its strike held as collateral, is refused, and is
/// still assigned in the money.
#[test]
fn an_instruction_for_a_short_is_refused() {
    let mut venue = driven_venue(OptionKind::Put, OptionSettlement::Physical, |_| {});
    feed(&mut venue, "100", "1.5", time(2));
    let _ = sell(&mut venue, "1");
    feed(&mut venue, "80", "20", time(50));

    assert_eq!(refusal(&mut venue, lapse()), ExerciseRefusal::ShortPosition);
    let events = venue.advance_time(expiry());
    assert_eq!(trades(&events)[0].origin, TradeOrigin::Assignment);
}

/// The venue instructs whole positions only: a quantity of contracts is refused, even one equal
/// to the long, and the long is still exercised.
#[test]
fn an_instruction_for_a_quantity_of_contracts_is_refused() {
    let mut venue = holding_calls("1", |_| {});

    assert_eq!(
        refusal(
            &mut venue,
            ExerciseInstruction::Lapse {
                contracts: ExerciseQuantity::Contracts(d("1")),
            }
        ),
        ExerciseRefusal::QuantityUnsupported
    );
    assert_exercised(&venue.advance_time(expiry()));
}

/// After expiry the instruction comes too late, even for a long still waiting for a price to
/// settle against.
#[test]
fn an_instruction_after_expiry_is_refused() {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Cash, |_| {});
    let _ = venue.apply_market(
        &option_name(),
        MarketObservation::new(traded("1.5")),
        time(2),
    );
    buy(&mut venue, "1");
    let _ = venue.advance_time(expiry());
    assert_eq!(option_position(&venue), Some(d("1")), "waiting for a price");

    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::Expired { expiry: expiry() }
    );
}

#[test]
fn an_instruction_for_an_instrument_that_is_not_a_listed_option_is_refused() {
    let mut venue = holding_calls("1", |_| {});

    for instrument in [spy_name(), InstrumentNameExchange::new("SPY_P100")] {
        assert!(
            matches!(
                venue.instruct_exercise(&instrument, lapse()),
                Err(ApiError::InstrumentInvalid(name, _)) if name == instrument
            ),
            "{instrument}"
        );
    }
}

/// Lapse instructions refused from ten seconds before expiry.
fn deadline() -> LapseDeadline {
    LapseDeadline::new(ExpiryAnchor::BeforeExpiry(Duration::from_secs(10)))
}

/// Taken until the deadline, refused from it on, and one taken before it still lapses the long.
#[test]
fn an_instruction_at_or_after_the_deadline_is_refused() {
    let mut venue = holding_calls("1", |config| config.lapse_deadline = Some(deadline()));
    feed(&mut venue, "120", "20", time(89));
    instruct_lapse(&mut venue);

    feed(&mut venue, "120", "20", time(90));
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::PastDeadline { deadline: time(90) }
    );
    assert_lapsed(&venue.advance_time(expiry()), "1");
}

/// A deadline anchored before the close needs the option's session calendar, which the venue
/// places it under.
#[test]
fn a_deadline_before_the_close_is_placed_under_the_session_calendar() {
    let mut venue = holding_calls("1", |config| {
        // The session closes at the expiry itself, 22:15 UTC on a Tuesday, so the deadline ten
        // seconds before it is at `time(90)`.
        config.session_close = Some(SessionCalendarConfig::Daily(DailyClose::new(
            chrono::NaiveTime::from_hms_opt(22, 15, 0).unwrap(),
            chrono_tz::UTC,
        )));
        config.lapse_deadline = Some(LapseDeadline::new(ExpiryAnchor::BeforeClose(
            Duration::from_secs(10),
        )));
    });
    feed(&mut venue, "120", "20", time(90));

    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::PastDeadline { deadline: time(90) }
    );
}

/// An instructed long the account cannot afford to exercise is not sold at the sell-out check,
/// since a lapse needs no money: it lapses at expiry instead.
#[test]
fn an_instructed_long_is_not_sold_out() {
    let mut venue = holding_calls("1", |config| {
        funds("10000", "0")(config);
        config.expiry_sell_out = Some(ExpirySellOut::new(ExpiryAnchor::BeforeExpiry(
            Duration::from_secs(10),
        )));
    });
    instruct_lapse(&mut venue);

    let checked = venue.advance_time(time(90));
    assert!(trades(&checked).is_empty(), "nothing sold: {checked:?}");
    assert_lapsed(&venue.advance_time(expiry()), "1");
}

/// The same long uninstructed is sold at the check, so the instruction is what spares it.
#[test]
fn an_uninstructed_long_the_account_cannot_exercise_is_sold_out() {
    let mut venue = holding_calls("1", |config| {
        funds("10000", "0")(config);
        config.expiry_sell_out = Some(ExpirySellOut::new(ExpiryAnchor::BeforeExpiry(
            Duration::from_secs(10),
        )));
    });

    let checked = venue.advance_time(time(90));
    assert_eq!(
        trades(&checked)
            .iter()
            .map(|trade| trade.origin)
            .collect::<Vec<_>>(),
        [TradeOrigin::Liquidation]
    );
}

/// `venue`'s snapshot as JSON.
fn snapshot_json(venue: &SimulatedVenue) -> serde_json::Value {
    serde_json::to_value(venue.snapshot()).unwrap()
}

/// A venue restored from `snapshot`, with [`driven_venue`]'s default config and a call.
fn restore(snapshot: serde_json::Value) -> Result<SimulatedVenue, VenueRestoreError> {
    let mut config = super::options::config("10000", FeeModelConfig::default());
    config.initial_state.balances = vec![funded(usd(), d("10000")), funded(spy(), d("0"))];
    SimulatedVenue::from_snapshot(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Physical),
        serde_json::from_value(snapshot).unwrap(),
    )
}

#[test]
fn a_restored_venue_keeps_the_instruction() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);

    let mut restored = restore(snapshot_json(&venue)).expect("its own snapshot restores");

    assert_eq!(restored.snapshot(), venue.snapshot());
    assert_lapsed(&restored.advance_time(expiry()), "1");
}

/// A venue holding no instruction writes no field for it, so its snapshot reads as one written
/// before the field existed; and such a snapshot restores with no instruction.
#[test]
fn a_snapshot_without_instructions_has_no_field_for_them() {
    let mut venue = holding_calls("1", |_| {});
    assert!(snapshot_json(&venue).get("lapse_instructed").is_none());

    instruct_lapse(&mut venue);
    let mut snapshot = snapshot_json(&venue);
    assert_eq!(
        snapshot["lapse_instructed"],
        serde_json::json!([option_name()])
    );
    snapshot.as_object_mut().unwrap().remove("lapse_instructed");

    let mut restored = restore(snapshot).expect("restores without the field");
    // A snapshot carries no market, so the exercise needs the underlying priced again.
    feed(&mut restored, "120", "20", time(60));
    assert_exercised(&restored.advance_time(expiry()));
}

/// An instruction for an option the snapshot holds no long in is refused, as one naming an
/// instrument that is not an option is.
#[test]
fn a_snapshot_instructing_no_long_option_is_refused() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);
    let mut snapshot = snapshot_json(&venue);
    snapshot["positions"] = serde_json::json!([]);

    assert_eq!(
        restore(snapshot).err(),
        Some(VenueRestoreError::LapseWithoutLong(option_name()))
    );

    let mut snapshot = snapshot_json(&venue);
    snapshot["lapse_instructed"] = serde_json::json!([option_name(), spy_name()]);
    assert_eq!(
        restore(snapshot).err(),
        Some(VenueRestoreError::LapseWithoutLong(spy_name()))
    );
}

/// An instruction for an option the snapshot holds short is refused: the instruction needs a
/// long in that very option.
#[test]
fn a_snapshot_instructing_a_short_option_is_refused() {
    let mut venue = holding_calls("1", |_| {});
    instruct_lapse(&mut venue);
    let mut snapshot = snapshot_json(&venue);
    let positions = snapshot["positions"].as_array_mut().unwrap();
    let held = positions
        .iter_mut()
        .find(|entry| entry[0] == serde_json::json!(option_name()))
        .expect("the snapshot holds the option");
    held[1]["quantity"] = serde_json::json!("-1");

    assert_eq!(
        restore(snapshot).err(),
        Some(VenueRestoreError::LapseWithoutLong(option_name()))
    );
}
