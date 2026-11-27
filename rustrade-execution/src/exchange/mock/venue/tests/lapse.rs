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
use chrono::TimeZone as _;
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

/// A sale through the long to a short loses the instruction, so a long bought back is exercised.
#[test]
fn a_long_sold_through_to_a_short_loses_the_instruction() {
    // A put, so the short is secured by cash, with shares held to deliver on exercise.
    let mut venue = driven_venue(
        OptionKind::Put,
        OptionSettlement::Physical,
        funds("20000", "100"),
    );
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");
    instruct_lapse(&mut venue);

    let _ = sell(&mut venue, "2");
    assert_eq!(option_position(&venue), Some(d("-1")));
    assert!(
        snapshot_json(&venue).get("lapse_instructed").is_none(),
        "a short holds no instruction"
    );

    buy(&mut venue, "2");
    feed(&mut venue, "80", "20", time(50));
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

/// Where several refusals apply, the first in the documented order wins: the quantity, then the
/// expiry, then the deadline, and only then the position.
#[test]
fn the_first_refusal_in_the_documented_order_wins() {
    let contracts = ExerciseInstruction::Lapse {
        contracts: ExerciseQuantity::Contracts(d("1")),
    };
    let flat = || {
        let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
            config.lapse_deadline = Some(deadline());
        });
        feed(&mut venue, "100", "1.5", time(2));
        venue
    };

    let mut venue = flat();
    assert_eq!(
        refusal(&mut venue, contracts),
        ExerciseRefusal::QuantityUnsupported,
        "a quantity of contracts, held flat"
    );

    feed(&mut venue, "100", "1.5", time(90));
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::PastDeadline { deadline: time(90) },
        "past the deadline, held flat"
    );

    let _ = venue.advance_time(expiry());
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::Expired { expiry: expiry() },
        "past the deadline and the expiry, held flat"
    );
    assert_eq!(
        refusal(&mut venue, contracts),
        ExerciseRefusal::QuantityUnsupported,
        "a quantity of contracts past the expiry"
    );
}

/// A deadline anchored before the close, for an option given no session calendar, cannot be
/// placed: an instruction panics rather than judge it against no deadline.
#[test]
#[should_panic(
    expected = "lapse deadline is anchored to the session calendar or held to the \
                           expiry day, but the option SPY_C100 has no session calendar"
)]
fn an_instruction_against_an_unplaced_deadline_panics() {
    let mut config = super::options::config("10000", FeeModelConfig::default());
    config.lapse_deadline = Some(LapseDeadline::new(ExpiryAnchor::BeforeClose(
        Duration::from_secs(10),
    )));
    let mut venue = SimulatedVenue::try_new_market_driven(
        &config,
        driven_instruments(OptionKind::Call, OptionSettlement::Physical),
    )
    .expect("built without a calendar, for one given later");

    let _ = venue.instruct_exercise(&option_name(), lapse());
}

/// Thursday 2023-11-16 22:00 New York, 03:00 UTC on the Friday: an expiry whose UTC date is the
/// day after its New York date.
fn evening_expiry() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2023, 11, 17, 3, 0, 0).unwrap()
}

/// Lapse instructions taken only on the expiry day, until an hour before the expiry.
fn expiry_day_window() -> LapseDeadline {
    LapseDeadline::new(ExpiryAnchor::BeforeExpiry(Duration::from_secs(60 * 60))).expiry_day_only()
}

/// New York's 16:00 close: the expiry day is dated in its zone.
fn new_york_close() -> SessionCalendarConfig {
    SessionCalendarConfig::Daily(DailyClose::new(
        chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        chrono_tz::America::New_York,
    ))
}

/// [`driven_instruments`], with the call expiring at [`evening_expiry`].
fn evening_instruments()
-> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    let mut instruments = driven_instruments(OptionKind::Call, OptionSettlement::Physical);
    match instruments
        .get_mut(&option_name())
        .map(|option| &mut option.kind)
    {
        Some(InstrumentKind::Option(contract)) => contract.expiry = evening_expiry(),
        other => panic!("the call is listed: {other:?}"),
    }
    instruments
}

/// A market-driven venue listing SPY and a call expiring at [`evening_expiry`], taking lapse
/// instructions in [`expiry_day_window`] under [`new_york_close`], with `configure` applied.
fn evening_venue(configure: impl FnOnce(&mut MockExecutionConfig)) -> SimulatedVenue {
    let mut config = super::options::config("10000", FeeModelConfig::default());
    config.session_close = Some(new_york_close());
    config.lapse_deadline = Some(expiry_day_window());
    configure(&mut config);
    let mut venue = SimulatedVenue::new_market_driven(&config, evening_instruments());
    advance(&mut venue, time(1));
    venue
}

/// The expiry day in New York.
fn thursday() -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(2023, 11, 16).unwrap()
}

/// Refused on any day before the expiry day, dated in the calendar's zone, not UTC's; taken
/// from the expiry day's first instant until the deadline; and the long instructed lapses.
#[test]
fn a_window_held_to_the_expiry_day_takes_instructions_only_on_it() {
    let mut venue = evening_venue(|_| {});
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    feed(&mut venue, "120", "20", time(50));
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::NotExpiryDay {
            expiry_day: thursday()
        },
        "the Tuesday"
    );

    // 23:59:59 on the Wednesday in New York, already the Thursday in UTC.
    let wednesday_night = Utc.with_ymd_and_hms(2023, 11, 16, 4, 59, 59).unwrap();
    feed(&mut venue, "120", "20", wednesday_night);
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::NotExpiryDay {
            expiry_day: thursday()
        },
        "the Wednesday in New York"
    );

    feed(
        &mut venue,
        "120",
        "20",
        wednesday_night + chrono::TimeDelta::seconds(1),
    );
    instruct_lapse(&mut venue);

    let deadline = evening_expiry() - chrono::TimeDelta::hours(1);
    feed(&mut venue, "120", "20", deadline);
    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::PastDeadline { deadline },
        "the expiry day, at the deadline"
    );

    let events = venue.advance_time(evening_expiry());
    let trades = trades(&events);
    assert_eq!(trades.len(), 1, "only the option leaves: {events:?}");
    assert_eq!(
        (trades[0].origin, trades[0].price, trades[0].quantity),
        (TradeOrigin::Expiry, d("0"), d("1"))
    );
}

/// A deadline at a local time, as Alpaca's do-not-exercise deadline: 17:15 New York on a regular
/// day, 14:30 on one closing early at 13:00. Taken until the deadline, refused from it, on each.
#[test]
fn a_deadline_at_a_local_time_moves_to_its_early_close_time_on_an_early_close_day() {
    let at = |h, m| chrono::NaiveTime::from_hms_opt(h, m, 0).unwrap();
    let deadline_anchor = ExpiryAnchor::AtLocalTime {
        time: at(17, 15),
        early_close: Some(at(14, 30)),
    };
    let regular = new_york_close();
    let SessionCalendarConfig::Daily(close) = new_york_close() else {
        unreachable!("a daily close")
    };
    let early = SessionCalendarConfig::Daily(
        close
            .with_dates(
                rustrade_instrument::calendar::SessionDates::new([], [thursday()]).unwrap(),
                Some(at(13, 0)),
            )
            .unwrap(),
    );
    // Thursday 2023-11-16 in EST: 17:15 is 22:15 UTC, 14:30 is 19:30 UTC.
    for (calendar, deadline, label) in [
        (
            regular,
            Utc.with_ymd_and_hms(2023, 11, 16, 22, 15, 0).unwrap(),
            "regular",
        ),
        (
            early,
            Utc.with_ymd_and_hms(2023, 11, 16, 19, 30, 0).unwrap(),
            "early close",
        ),
    ] {
        let mut venue = evening_venue(|config| {
            config.session_close = Some(calendar);
            config.lapse_deadline = Some(LapseDeadline::new(deadline_anchor).expiry_day_only());
        });
        feed(&mut venue, "100", "1.5", time(2));
        buy(&mut venue, "1");

        feed(
            &mut venue,
            "120",
            "20",
            deadline - chrono::TimeDelta::seconds(1),
        );
        instruct_lapse(&mut venue);
        feed(&mut venue, "120", "20", deadline);
        assert_eq!(
            refusal(&mut venue, lapse()),
            ExerciseRefusal::PastDeadline { deadline },
            "{label}"
        );
    }
}

/// The day is judged before the position, as the deadline is: an account holding none is
/// refused for the day, and only on the expiry day for want of a position.
#[test]
fn the_day_is_judged_before_the_position() {
    let mut venue = evening_venue(|_| {});
    feed(&mut venue, "100", "1.5", time(2));

    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::NotExpiryDay {
            expiry_day: thursday()
        }
    );

    feed(
        &mut venue,
        "100",
        "1.5",
        Utc.with_ymd_and_hms(2023, 11, 16, 15, 0, 0).unwrap(),
    );
    assert_eq!(refusal(&mut venue, lapse()), ExerciseRefusal::NoPosition);
}

/// Without the window, the same venue takes the instruction days before the expiry.
#[test]
fn a_deadline_not_held_to_the_expiry_day_takes_instructions_before_it() {
    let mut venue = evening_venue(|config| {
        config.lapse_deadline = Some(LapseDeadline::new(ExpiryAnchor::BeforeExpiry(
            Duration::from_secs(60 * 60),
        )));
    });
    feed(&mut venue, "100", "1.5", time(2));
    buy(&mut venue, "1");

    instruct_lapse(&mut venue);
}

/// A window held to the expiry day needs the option's session calendar to date the day, even
/// with a deadline that needs none: an instruction panics rather than judge it on no day.
#[test]
#[should_panic(
    expected = "lapse deadline is anchored to the session calendar or held to the \
                           expiry day, but the option SPY_C100 has no session calendar"
)]
fn an_instruction_against_an_undated_window_panics() {
    let mut config = super::options::config("10000", FeeModelConfig::default());
    config.lapse_deadline = Some(expiry_day_window());
    let mut venue = SimulatedVenue::try_new_market_driven(&config, evening_instruments())
        .expect("built without a calendar, for one given later");

    let _ = venue.instruct_exercise(&option_name(), lapse());
}

/// A calendar given after construction dates the window.
#[test]
fn a_calendar_given_later_dates_the_window() {
    let mut config = super::options::config("10000", FeeModelConfig::default());
    config.lapse_deadline = Some(expiry_day_window());
    let mut venue = SimulatedVenue::try_new_market_driven(&config, evening_instruments())
        .expect("built without a calendar, for one given later")
        .with_session_calendar(Arc::new(DailyClose::new(
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            chrono_tz::America::New_York,
        )))
        .expect("the calendar places the window");
    advance(&mut venue, time(1));
    feed(&mut venue, "100", "1.5", time(2));

    assert_eq!(
        refusal(&mut venue, lapse()),
        ExerciseRefusal::NotExpiryDay {
            expiry_day: thursday()
        }
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
