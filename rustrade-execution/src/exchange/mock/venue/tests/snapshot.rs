//! Snapshot and restore: a venue restored from its snapshot carries on exactly as the original
//! would, and a snapshot that does not fit the venue being restored is refused.

use super::*;
use crate::{
    exchange::mock::account::DuplicateSeededOrder,
    slippage::SlippageConfig,
    stored::{Stored, StoredFormatError},
};

/// A margin account lending up to 3x in USDT, with an hour's borrow price window and seeded
/// slippage, holding 1 BTC and 100,000 USDT.
fn margin_window_config() -> MockExecutionConfig {
    let spot_margin = SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow)
        .unwrap()
        .with_borrow_price_window(TimeDelta::hours(1))
        .unwrap();
    MockExecutionConfig {
        spot_margin: Some(spot_margin),
        slippage: SlippageConfig::uniform(d("0"), d("0.001"), 42).unwrap(),
        ..spot_config("1", "100000", FeeModelConfig::default())
    }
}

/// `snapshot` written to JSON and read back, as a caller keeping it would.
fn through_json(snapshot: &SimulatedVenueSnapshot) -> SimulatedVenueSnapshot {
    serde_json::from_str(&serde_json::to_string(snapshot).unwrap()).unwrap()
}

fn observe(venue: &mut SimulatedVenue, price: &str, seconds: i64) -> Vec<UnindexedAccountEvent> {
    venue.apply_market(
        &instrument_name(),
        MarketObservation::new(traded(price)),
        time(seconds),
    )
}

/// A venue that has borrowed BTC and is charged interest on it, with two bids resting at one price
/// and an offer resting until two hours in.
fn venue_mid_run() -> SimulatedVenue {
    let mut venue = SimulatedVenue::new_market_driven(&margin_window_config(), spot_instruments())
        .with_margin_interest(Arc::new(hourly_interest(base())));
    advance(&mut venue, time(0));
    assert!(observe(&mut venue, "50000", 1).is_empty());

    // Selling 2 BTC with 1 held borrows the other.
    let sold = venue.open_order(sell_request("2", None));
    assert!(matches!(
        sold.response.state,
        OrderState::Inactive(InactiveOrderState::FullyFilled(_))
    ));
    assert_eq!(
        venue.account.owed(&base()),
        d("1.0001"),
        "1 BTC and its interest on borrow"
    );

    for cid in ["bid-1", "bid-2"] {
        venue.open_order(limit_request(cid, Side::Buy, "0.5", "49000", gtc()));
    }
    let gtd = TimeInForce::GoodTillDate { expiry: time(7200) };
    venue.open_order(limit_request("ask-gtd", Side::Sell, "0.1", "52000", gtd));
    assert_eq!(venue.account.orders().len(), 3);

    assert!(observe(&mut venue, "50500", 1800).is_empty());
    venue
}

/// What follows the snapshot: three hours of interest and the offer's deadline passed while the
/// venue was not running, a price that crosses both bids, and a market order that draws slippage.
fn carry_on(venue: &mut SimulatedVenue) -> Vec<UnindexedAccountEvent> {
    let mut events = venue.advance_time(time(3 * 3600 + 5));
    events.extend(observe(venue, "48900", 3 * 3600 + 10));
    events.extend(venue.open_order(buy_request("0.2", None)).events);
    events
}

#[test]
fn a_restored_venue_carries_on_exactly_as_the_original_would() {
    let mut original = venue_mid_run();
    let snapshot = original.snapshot();

    let mut restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        through_json(&snapshot),
    )
    .unwrap()
    .with_margin_interest(Arc::new(hourly_interest(base())));
    assert_eq!(restored.snapshot(), snapshot, "restored as it was taken");
    // Both sides of the equality above go through `snapshot`, so a field it lost would compare
    // equal: the fields a caller can see are checked against the original directly.
    assert_eq!(
        snapshot.accrued_through(),
        Some(time(1800)),
        "interest is charged up to the venue's clock"
    );
    let sold = ClientOrderId::new("test-cid");
    assert!(original.account.order_ended(&sold).is_some());
    assert_eq!(
        restored.account.order_ended(&sold),
        original.account.order_ended(&sold),
        "the filled sell is still known as filled"
    );
    assert_eq!(restored.regime(), VenueRegime::MarketDriven);
    assert_eq!(restored.next_deadline(), original.next_deadline());

    let expected = carry_on(&mut original);
    assert_eq!(carry_on(&mut restored), expected);
    assert_eq!(restored.snapshot(), original.snapshot());

    // The script did what it says, so the equality above covers it.
    let interest = expected
        .iter()
        .filter(|event| matches!(&event.kind, AccountEventKind::CashFlow(_)))
        .count();
    assert!(
        interest >= 3,
        "an interest charge per hour passed: {expected:#?}"
    );
    let filled = expected
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::Trade(trade) => Some(trade.order_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        filled.len(),
        3,
        "both bids and the market buy: {expected:#?}"
    );
    assert!(
        expected.iter().any(|event| matches!(
            &event.kind,
            AccountEventKind::OrderSnapshot(order)
                if order.value().key.cid == ClientOrderId::new("ask-gtd")
        )),
        "the offer expired: {expected:#?}"
    );
}

#[test]
fn bids_at_one_price_fill_in_the_order_they_arrived_after_a_restore() {
    let original = venue_mid_run();
    let mut restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        through_json(&original.snapshot()),
    )
    .unwrap();

    let crossed = observe(&mut restored, "48900", 1900);
    let cids = crossed
        .iter()
        .filter_map(|event| match &event.kind {
            AccountEventKind::OrderSnapshot(order) => Some(order.value().key.cid.0.to_string()),
            _ => None,
        })
        .dedup()
        .collect::<Vec<_>>();
    assert_eq!(cids, ["bid-1", "bid-2"]);
}

#[test]
fn a_held_market_order_is_released_by_the_next_bar_after_a_restore() {
    let config = next_bar_config("1000", NextBarPrice::Open);
    let mut original = venue_holding(NextBarPrice::Open, "1000");
    let held = original.open_order(market_order("held", Side::Buy, "1", gtc()));
    assert!(
        held.events
            .iter()
            .all(|event| !matches!(&event.kind, AccountEventKind::Trade(_)))
    );

    let mut restored = SimulatedVenue::from_snapshot(
        &config,
        spot_instruments(),
        through_json(&original.snapshot()),
    )
    .unwrap();

    let expected = feed_bar(&mut original, 2, ohlc("101", "106", "99", "104"));
    assert!(
        expected
            .iter()
            .any(|event| matches!(&event.kind, AccountEventKind::Trade(_))),
        "{expected:#?}"
    );
    assert_eq!(
        feed_bar(&mut restored, 2, ohlc("101", "106", "99", "104")),
        expected
    );
}

#[test]
fn a_cfd_position_closes_against_its_restored_entry() {
    let config = config_from_balances(vec![funded(usd(), d("100000"))], FeeModelConfig::default());
    let mut original = make_cfd_market_venue("100000");
    assert!(cfd_market(&mut original, "99", "101", "100").is_empty());
    cfd_market_fill(&mut original, Side::Buy, "2");
    let snapshot_open = original.snapshot();

    let mut restored = SimulatedVenue::from_snapshot(
        &config,
        instruments_of(cfd_instrument()),
        through_json(&snapshot_open),
    )
    .unwrap();
    assert!(cfd_position(&restored).is_some());
    assert_eq!(cfd_position(&restored), cfd_position(&original));
    assert_eq!(
        original.snapshot().positions().collect::<Vec<_>>(),
        [(&cfd_instrument_name(), d("2"))]
    );

    let close = |venue: &mut SimulatedVenue| {
        let mut events = cfd_market(venue, "109", "111", "110");
        events.extend(venue.open_order(cfd_request(Side::Sell, "2", None)).events);
        events
    };
    assert_eq!(close(&mut restored), close(&mut original));
    assert_eq!(cfd_position(&restored), None);

    let mut json = serde_json::to_value(snapshot_open).unwrap();
    let positions = json["positions"].as_array_mut().unwrap();
    positions.push(positions[0].clone());
    let refused = SimulatedVenue::from_snapshot(
        &config,
        instruments_of(cfd_instrument()),
        serde_json::from_value(json).unwrap(),
    );
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::DuplicateInstrument(cfd_instrument_name())
    );
}

#[test]
fn a_restored_venue_mints_no_id_the_original_minted() {
    let mut original = venue_mid_run();
    let mut restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        through_json(&original.snapshot()),
    )
    .unwrap();

    let next = |venue: &mut SimulatedVenue| {
        assert!(observe(venue, "50500", 1900).is_empty());
        let outcome = venue.open_order(buy_request("0.01", None));
        (outcome.response.state.clone(), outcome.events)
    };
    assert_eq!(next(&mut restored), next(&mut original));
    assert_eq!(restored.order_sequence(), original.order_sequence());
}

/// The market is not restored: until its driver feeds one, a restored venue cannot price a
/// market order, and refuses it as a fresh venue would.
#[test]
fn a_restored_venue_holds_no_market_until_it_observes_one() {
    let original = venue_mid_run();
    assert!(original.market(&instrument_name()).is_some());
    let mut restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        original.snapshot(),
    )
    .unwrap();
    assert!(restored.market(&instrument_name()).is_none());

    let refused = restored.open_order(buy_request("0.01", None));
    assert!(matches!(
        refused.response.state,
        OrderState::Inactive(InactiveOrderState::OpenFailed(_))
    ));
}

#[test]
fn a_snapshot_of_another_version_is_refused() {
    let mut json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    json["version"] = serde_json::json!(SimulatedVenueSnapshot::VERSION + 1);
    let snapshot: SimulatedVenueSnapshot = serde_json::from_value(json).unwrap();
    assert_eq!(snapshot.version(), SimulatedVenueSnapshot::VERSION + 1);

    let refused =
        SimulatedVenue::from_snapshot(&margin_window_config(), spot_instruments(), snapshot);
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::UnsupportedVersion {
            found: SimulatedVenueSnapshot::VERSION + 1,
            supported: SimulatedVenueSnapshot::VERSION,
        }
    );
}

/// `json` read as a stored snapshot.
fn stored(json: serde_json::Value) -> Result<SimulatedVenueSnapshot, StoredFormatError> {
    serde_json::from_value::<Stored<SimulatedVenueSnapshot>>(json)
        .unwrap()
        .into_current()
}

#[test]
fn a_stored_snapshot_reads_back_and_one_of_another_version_is_refused_whatever_its_shape() {
    let snapshot = venue_mid_run().snapshot();
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(stored(json.clone()), Ok(snapshot));

    // The widest order id a venue can start from survives the stored read.
    let mut wide = json.clone();
    wide["order_id_start"] = serde_json::json!(u128::MAX.to_string());
    assert_eq!(
        stored(wide.clone()),
        Ok(serde_json::from_value::<SimulatedVenueSnapshot>(wide).unwrap())
    );

    let unsupported = Err(StoredFormatError::UnsupportedVersion {
        found: SimulatedVenueSnapshot::VERSION + 1,
        supported: SimulatedVenueSnapshot::VERSION,
    });
    let mut later = json.clone();
    later["version"] = serde_json::json!(SimulatedVenueSnapshot::VERSION + 1);
    assert_eq!(stored(later.clone()), unsupported, "same shape");
    later["account"] = serde_json::json!(["a later shape"]);
    assert_eq!(stored(later), unsupported, "another shape");

    let mut edited = json;
    edited["account"] = serde_json::json!(["not this version's shape"]);
    assert_eq!(
        stored(edited),
        Err(StoredFormatError::Malformed {
            version: SimulatedVenueSnapshot::VERSION
        })
    );
}

/// A field a later release added reads with its default from a snapshot written before it: here
/// a trade's `order_filled_quantity`, which older trades do not carry.
#[test]
fn an_older_snapshot_without_a_later_field_restores_with_its_default() {
    let mut json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    let trades = json["account"]["trades"].as_array_mut().unwrap();
    assert!(!trades.is_empty(), "the sell traded");
    for trade in trades.iter_mut() {
        assert!(
            trade
                .as_object_mut()
                .unwrap()
                .remove("order_filled_quantity")
                .is_some()
        );
    }

    let restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        stored(json).unwrap(),
    )
    .unwrap();
    let trades = restored
        .account
        .trades(DateTime::<Utc>::MIN_UTC)
        .collect::<Vec<_>>();
    assert!(!trades.is_empty());
    assert!(
        trades
            .iter()
            .all(|trade| trade.order_filled_quantity.is_none())
    );
}

#[test]
fn a_snapshot_of_another_exchange_is_refused() {
    let config = MockExecutionConfig {
        mocked_exchange: ExchangeId::Kraken,
        ..margin_window_config()
    };
    let refused =
        SimulatedVenue::from_snapshot(&config, spot_instruments(), venue_mid_run().snapshot());
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::ExchangeMismatch {
            snapshot: EXCHANGE,
            config: ExchangeId::Kraken,
        }
    );
}

#[test]
fn a_snapshot_of_other_instruments_is_refused() {
    let refused = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        btc_and_eth_instruments(),
        venue_mid_run().snapshot(),
    );
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::InstrumentsMismatch {
            only_in_snapshot: vec![],
            only_in_venue: vec![eth_instrument_name()],
        }
    );
}

#[test]
fn a_snapshot_naming_an_instrument_it_does_not_list_is_refused() {
    let mut json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    let positions = json["positions"].as_array_mut().unwrap();
    positions.push(serde_json::json!(["ETHUSDT", { "quantity": "1", "entry": "2000" }]));
    let snapshot = serde_json::from_value(json).unwrap();

    let refused =
        SimulatedVenue::from_snapshot(&margin_window_config(), spot_instruments(), snapshot);
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::UnknownInstrument(eth_instrument_name())
    );
}

#[test]
fn a_snapshot_whose_account_names_an_instrument_it_does_not_list_is_refused() {
    let mut json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    let trades = json["account"]["trades"].as_array_mut().unwrap();
    assert!(!trades.is_empty(), "the sell traded");
    trades[0]["instrument"] = serde_json::json!("ETHUSDT");
    let snapshot = serde_json::from_value(json).unwrap();

    let refused =
        SimulatedVenue::from_snapshot(&margin_window_config(), spot_instruments(), snapshot);
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::UnknownInstrument(eth_instrument_name())
    );
}

/// A checkpoint's id counts from one, and a venue restored from one carries on counting.
#[test]
fn checkpoints_are_numbered_from_one_across_a_restore() {
    let mut original = venue_mid_run();
    assert_eq!(original.checkpoint().id, 1);
    let second = original.checkpoint();
    assert_eq!(second.id, 2);
    assert_eq!(second.snapshot, original.snapshot(), "the venue as it is");

    let mut restored = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        through_json(&second.snapshot),
    )
    .unwrap();
    assert_eq!(restored.checkpoint().id, 3);
}

#[test]
fn a_margin_snapshot_restored_without_spot_margin_is_refused_and_the_other_way_round() {
    let cash = MockExecutionConfig {
        spot_margin: None,
        ..margin_window_config()
    };
    let refused =
        SimulatedVenue::from_snapshot(&cash, spot_instruments(), venue_mid_run().snapshot());
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::MarginMismatch {
            snapshot_margin: true
        }
    );

    let cash_venue = SimulatedVenue::new_market_driven(&cash, spot_instruments());
    let refused = SimulatedVenue::from_snapshot(
        &margin_window_config(),
        spot_instruments(),
        cash_venue.snapshot(),
    );
    let error = refused.unwrap_err();
    assert_eq!(
        error,
        VenueRestoreError::MarginMismatch {
            snapshot_margin: false
        }
    );
    assert!(error.to_string().contains("cash account"), "{error}");
}

#[test]
fn a_snapshot_listing_an_open_order_twice_is_refused() {
    let mut json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    let open = json["account"]["orders_open"].as_array_mut().unwrap();
    let first = open[0].clone();
    open.push(first);
    let snapshot = serde_json::from_value(json).unwrap();

    let refused =
        SimulatedVenue::from_snapshot(&margin_window_config(), spot_instruments(), snapshot);
    assert_eq!(
        refused.unwrap_err(),
        VenueRestoreError::DuplicateOrder(DuplicateSeededOrder(ClientOrderId::new("bid-1")))
    );
}

#[test]
fn a_window_the_configuration_no_longer_sets_is_dropped() {
    let original = venue_mid_run();
    let without_window = MockExecutionConfig {
        spot_margin: Some(SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow).unwrap()),
        ..margin_window_config()
    };
    let (restored, warned) = warnings(|| {
        SimulatedVenue::from_snapshot(&without_window, spot_instruments(), original.snapshot())
            .unwrap()
    });

    assert!(!original.borrow_prices.is_empty());
    assert!(restored.borrow_prices.is_empty());
    assert_eq!(
        warned
            .iter()
            .filter(|message| message.contains("drops the borrow price windows"))
            .count(),
        1,
        "{warned:?}"
    );
}

/// A list the venue holds one entry per key in, listing a key twice, is refused rather than
/// restored with one of the two entries.
#[test]
fn a_snapshot_listing_an_instrument_or_asset_twice_is_refused() {
    let json = serde_json::to_value(venue_mid_run().snapshot()).unwrap();
    let restore = |json: serde_json::Value| {
        SimulatedVenue::from_snapshot(
            &margin_window_config(),
            spot_instruments(),
            serde_json::from_value(json).unwrap(),
        )
    };
    let doubled = |mut json: serde_json::Value, pointer: &str| {
        let list = json.pointer_mut(pointer).unwrap().as_array_mut().unwrap();
        assert!(!list.is_empty(), "{pointer} has an entry to repeat");
        list.push(list[0].clone());
        json
    };

    assert_eq!(
        restore(doubled(json.clone(), "/borrow_prices")).unwrap_err(),
        VenueRestoreError::DuplicateInstrument(instrument_name())
    );
    assert_eq!(
        restore(doubled(json.clone(), "/account/balances")).unwrap_err(),
        VenueRestoreError::DuplicateAsset(base())
    );
    assert_eq!(
        restore(doubled(json.clone(), "/reported_debt")).unwrap_err(),
        VenueRestoreError::DuplicateAsset(base())
    );
    assert!(restore(json).is_ok());
}
