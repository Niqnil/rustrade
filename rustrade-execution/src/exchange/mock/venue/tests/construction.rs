//! A configuration the venue refuses when it is built or restored: one that spells an asset two
//! ways, naming both spellings and where each was read, and one seeding an order twice. The
//! refusals of seeded positions are tested with their instruments.

use super::*;
use crate::exchange::mock::account::DuplicateSeededOrder;

fn asset(spelling: &str) -> AssetNameExchange {
    AssetNameExchange::new(spelling)
}

/// A config for BTCUSDT holding the two named balances.
fn holding(base_spelling: &str, quote_spelling: &str) -> MockExecutionConfig {
    config_from_balances(
        vec![
            funded(asset(base_spelling), d("1")),
            funded(asset(quote_spelling), d("1000")),
        ],
        FeeModelConfig::default(),
    )
}

fn spelled_twice(
    first: &str,
    first_source: AssetSource,
    second: &str,
    second_source: AssetSource,
) -> AssetSpelledTwice {
    AssetSpelledTwice {
        first: asset(first),
        first_source,
        second: asset(second),
        second_source,
    }
}

fn btcusdt() -> AssetSource {
    AssetSource::Instrument(instrument_name())
}

#[test]
fn one_spelling_everywhere_is_accepted() {
    let config = holding("BTC", "USDT")
        .with_fee_asset(FeeAssetConfig::new(quote(), d("0.25")).unwrap())
        .with_spot_margin(
            SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow)
                .unwrap()
                .with_min_borrow(base(), d("0.001"))
                .unwrap(),
        )
        .with_fee_model_overrides(FeeModelOverrides::new().with_underlying(
            InstrumentKindDiscriminant::Spot,
            Underlying::new(base(), quote()),
            FeeModelConfig::default(),
        ));

    assert!(SimulatedVenue::try_new(&config, spot_instruments()).is_ok());
}

#[test]
fn a_balance_spelling_an_instruments_asset_differently_is_refused() {
    let refused = SimulatedVenue::try_new(&holding("btc", "USDT"), spot_instruments());

    assert_eq!(
        refused.err(),
        Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
            "BTC",
            btcusdt(),
            "btc",
            AssetSource::Balance,
        )))
    );
}

#[test]
fn a_market_driven_venue_refuses_it_too() {
    let refused =
        SimulatedVenue::try_new_market_driven(&holding("BTC", "usdt"), spot_instruments());

    assert_eq!(
        refused.err(),
        Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
            "USDT",
            btcusdt(),
            "usdt",
            AssetSource::Balance,
        )))
    );
}

#[test]
fn a_settlement_asset_is_read_with_its_instrument() {
    let config = config_from_balances(
        vec![
            funded(asset("usd"), d("1000")),
            funded(asset("GBP"), d("1000")),
        ],
        FeeModelConfig::default(),
    );

    let refused = SimulatedVenue::try_new(&config, instruments_of(cfd_instrument()));

    assert_eq!(
        refused.err(),
        Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
            "gbp",
            AssetSource::Instrument(cfd_instrument_name()),
            "GBP",
            AssetSource::Balance,
        )))
    );
}

#[test]
fn a_fee_asset_spelled_unlike_its_balance_is_refused() {
    let config = config_from_balances(
        vec![
            funded(base(), d("1")),
            funded(quote(), d("1000")),
            funded(asset("BNB"), d("10")),
        ],
        FeeModelConfig::default(),
    )
    .with_fee_asset(FeeAssetConfig::new(asset("bnb"), d("0.25")).unwrap());

    let refused = SimulatedVenue::try_new(&config, spot_instruments());

    assert_eq!(
        refused.err(),
        Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
            "BNB",
            AssetSource::Balance,
            "bnb",
            AssetSource::FeeAsset,
        )))
    );
}

#[test]
fn every_asset_the_spot_margin_configuration_names_is_read() {
    let margin = || SpotMarginConfig::new(quote(), d("3"), MarginEffect::Borrow).unwrap();
    let configured = [
        (
            "valuation asset",
            SpotMarginConfig::new(asset("usdt"), d("3"), MarginEffect::Borrow).unwrap(),
            "usdt",
        ),
        (
            "min borrow",
            margin().with_min_borrow(asset("btc"), d("0.001")).unwrap(),
            "btc",
        ),
        (
            "max borrow",
            margin().with_max_borrow(asset("btc"), d("5")).unwrap(),
            "btc",
        ),
        (
            "borrow collateral ratio",
            margin()
                .with_borrow_collateral_ratio(asset("btc"), d("0.9"))
                .unwrap(),
            "btc",
        ),
        (
            "both collateral ratios",
            margin()
                .with_collateral_ratios(asset("btc"), d("0.8"), d("0.9"))
                .unwrap(),
            "btc",
        ),
    ];

    for (case, spot_margin, spelling) in configured {
        let config = holding("BTC", "USDT").with_spot_margin(spot_margin);

        let refused = SimulatedVenue::try_new(&config, spot_instruments());

        assert_eq!(
            refused.err(),
            Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
                &spelling.to_uppercase(),
                btcusdt(),
                spelling,
                AssetSource::SpotMargin,
            ))),
            "{case}"
        );
    }
}

#[test]
fn a_fee_model_override_spelling_an_underlying_differently_is_refused() {
    let config =
        holding("BTC", "USDT").with_fee_model_overrides(FeeModelOverrides::new().with_underlying(
            InstrumentKindDiscriminant::Spot,
            Underlying::new(base(), asset("usdt")),
            FeeModelConfig::default(),
        ));

    let refused = SimulatedVenue::try_new(&config, spot_instruments());

    assert_eq!(
        refused.err(),
        Some(VenueConfigError::AssetSpelledTwice(spelled_twice(
            "USDT",
            btcusdt(),
            "usdt",
            AssetSource::FeeModelOverride,
        )))
    );
}

#[test]
#[should_panic(
    expected = "invalid SimulatedVenue configuration: the configuration spells one asset two ways: \
                BTC in instrument BTCUSDT, btc in a balance"
)]
fn new_panics_with_the_refusal() {
    let _ = SimulatedVenue::new(&holding("btc", "USDT"), spot_instruments());
}

#[test]
fn a_restore_refuses_a_configuration_spelling_the_snapshots_assets_differently() {
    let config = holding("BTC", "USDT");
    let snapshot = SimulatedVenue::new_market_driven(&config, spot_instruments()).snapshot();
    let respelled = config.with_fee_asset(FeeAssetConfig::new(asset("btc"), d("0.25")).unwrap());

    let refused = SimulatedVenue::from_snapshot(&respelled, spot_instruments(), snapshot);

    assert_eq!(
        refused.err(),
        Some(VenueRestoreError::AssetSpelledTwice(spelled_twice(
            "BTC",
            btcusdt(),
            "btc",
            AssetSource::FeeAsset,
        )))
    );
}

#[test]
fn an_initial_state_seeding_one_order_twice_is_refused() {
    let seeded = Order {
        key: key_of("seeded"),
        side: Side::Buy,
        price: Some(d("48000")),
        quantity: d("1"),
        kind: OrderKind::Limit,
        time_in_force: gtc(),
        state: OrderState::inactive(Cancelled {
            id: OrderId::new("seeded"),
            time_exchange: time(0),
            filled_quantity: None,
        }),
    };
    let mut config = spot_config_holding("10", "1000000", seeded.clone());
    config.initial_state.instruments[0]
        .orders
        .push(OrderReport::state_only(seeded));

    assert_eq!(
        SimulatedVenue::try_new(&config, spot_instruments()).err(),
        Some(VenueConfigError::DuplicateOrder(DuplicateSeededOrder(
            ClientOrderId::new("seeded")
        )))
    );
}

#[test]
fn a_fee_model_override_no_instrument_matches_is_logged_except_by_kind() {
    let config = holding("BTC", "USDT").with_fee_model_overrides(
        FeeModelOverrides::new()
            .with_name("ETHUSDT", FeeModelConfig::default())
            .with_underlying(
                InstrumentKindDiscriminant::Spot,
                Underlying::new(asset("ETH"), quote()),
                FeeModelConfig::default(),
            )
            .with_underlying(
                InstrumentKindDiscriminant::Spot,
                Underlying::new(base(), quote()),
                FeeModelConfig::default(),
            )
            .with_kind(
                InstrumentKindDiscriminant::Future,
                FeeModelConfig::default(),
            ),
    );

    let (venue, events) = rustrade_instrument::test_utils::logs::capture(|| {
        SimulatedVenue::try_new(&config, spot_instruments())
    });

    assert!(venue.is_ok());
    let warnings = events
        .iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .map(|event| event.message().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        warnings,
        [
            "fee model override names an instrument this venue does not have; it is never used",
            "fee model override names a kind and underlying no instrument on this venue has; it \
             is never used",
        ],
        "{events:?}"
    );
}
