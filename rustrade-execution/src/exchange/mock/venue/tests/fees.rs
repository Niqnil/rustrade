//! Fees: which model prices an instrument's fills, and fees tiered by premium and by order size.

use super::{
    options::{driven_instruments, driven_venue, feed, option_name, spy, spy_name, usd},
    *,
};
use crate::fee::{FeeModelOverrides, PriceBand, PriceBands};
use rustrade_instrument::instrument::kind::{
    InstrumentKindDiscriminant,
    option::{OptionKind, OptionSettlement},
};

fn per_contract(commission: &str) -> FeeModelConfig {
    FeeModelConfig::PerContract(PerContractFeeModel {
        commission_per_contract: d(commission),
    })
}

fn composite(components: Vec<FeeComponent>) -> FeeModelConfig {
    FeeModelConfig::Composite(CompositeFeeModel::new(components).unwrap())
}

fn spy_usd() -> Underlying<AssetNameExchange> {
    Underlying::new(spy(), usd())
}

/// A venue listing SPY and a call on it, both at 1.5 for the call and 100 for SPY, charging `fee`
/// by default and `overrides` in its place.
fn venue_charging(fee: FeeModelConfig, overrides: FeeModelOverrides) -> SimulatedVenue {
    let mut venue = driven_venue(OptionKind::Call, OptionSettlement::Physical, |config| {
        config.fee_model = fee;
        config.fee_model_overrides = overrides;
    });
    feed(&mut venue, "100", "1.5", time(2));
    venue
}

/// The fee a market buy of `quantity` of `instrument` pays.
fn fee_of_buy(
    venue: &mut SimulatedVenue,
    instrument: InstrumentNameExchange,
    quantity: &str,
) -> Decimal {
    let outcome = venue.open_order(request(instrument, Side::Buy, quantity, None));
    filled_trade(&outcome).fees.fees
}

/// The fees one call and one share pay on a venue charging 1 a contract by default and
/// `overrides` in its place.
fn fees_under(overrides: FeeModelOverrides) -> (Decimal, Decimal) {
    let mut venue = venue_charging(per_contract("1"), overrides);
    let option = fee_of_buy(&mut venue, option_name(), "1");
    let share = fee_of_buy(&mut venue, spy_name(), "1");
    (option, share)
}

#[test]
fn an_instrument_pays_its_name_then_its_underlying_then_its_kind_then_the_default() {
    let option = InstrumentKindDiscriminant::Option;
    let spot = InstrumentKindDiscriminant::Spot;
    let by_kind = FeeModelOverrides::new().with_kind(option, per_contract("4"));
    let by_underlying = by_kind
        .clone()
        .with_underlying(option, spy_usd(), per_contract("2"));
    let by_name = by_underlying
        .clone()
        .with_name(option_name(), per_contract("3"));

    assert_eq!(fees_under(FeeModelOverrides::new()), (d("1"), d("1")));
    assert_eq!(fees_under(by_kind), (d("4"), d("1")));
    // The call's underlying is SPY's, but the override is for options on it: SPY pays the default.
    assert_eq!(fees_under(by_underlying), (d("2"), d("1")));
    assert_eq!(fees_under(by_name), (d("3"), d("1")));
    assert_eq!(
        fees_under(FeeModelOverrides::new().with_underlying(spot, spy_usd(), per_contract("6"))),
        (d("1"), d("6"))
    );
    assert_eq!(
        fees_under(FeeModelOverrides::new().with_kind(spot, per_contract("5"))),
        (d("1"), d("5"))
    );
}

#[test]
fn a_banded_fee_charges_the_band_of_the_fill_premium() {
    let banded = composite(vec![FeeComponent::new(FeeBasis::PerContractByPrice(
        PriceBands::new(vec![
            PriceBand::new(d("0"), d("0.50")),
            PriceBand::new(d("1"), d("0.65")),
        ])
        .unwrap(),
    ))]);
    let mut venue = venue_charging(banded, FeeModelOverrides::new());

    assert_eq!(
        fee_of_buy(&mut venue, option_name(), "2"),
        d("1.30"),
        "2 at 1.5"
    );

    feed(&mut venue, "100", "0.95", time(3));
    assert_eq!(
        fee_of_buy(&mut venue, option_name(), "2"),
        d("1.00"),
        "2 at 0.95"
    );
}

#[test]
fn a_threshold_charges_an_order_from_its_size_and_not_below() {
    let commission = composite(vec![
        FeeComponent::new(FeeBasis::PerContract(d("0.65"))).from_order_quantity(d("10")),
    ]);
    let mut venue = venue_charging(commission, FeeModelOverrides::new());

    assert_eq!(fee_of_buy(&mut venue, option_name(), "9"), Decimal::ZERO);
    assert_eq!(fee_of_buy(&mut venue, option_name(), "10"), d("6.50"));
}

#[test]
fn an_override_no_instrument_matches_is_found_except_by_kind() {
    let instruments = driven_instruments(OptionKind::Call, OptionSettlement::Physical);
    let spx_usd = Underlying::new("spx", "usd");
    let spy_eur = Underlying::new("spy", "eur");
    let overrides = FeeModelOverrides::new()
        .with_name(option_name(), per_contract("1"))
        .with_name("SPX_C100", per_contract("1"))
        .with_underlying(
            InstrumentKindDiscriminant::Option,
            spy_usd(),
            per_contract("1"),
        )
        .with_underlying(
            InstrumentKindDiscriminant::Option,
            spy_eur.clone(),
            per_contract("1"),
        )
        .with_underlying(
            InstrumentKindDiscriminant::Spot,
            spx_usd.clone(),
            per_contract("1"),
        )
        // SPY trades here, but only as spot: a key matches on kind and underlying together.
        .with_underlying(
            InstrumentKindDiscriminant::Future,
            spy_usd(),
            per_contract("1"),
        )
        .with_kind(InstrumentKindDiscriminant::Future, per_contract("1"));

    assert_eq!(
        unmatched_fee_overrides(&overrides, &instruments),
        vec![
            UnmatchedFeeOverride::Name(&InstrumentNameExchange::new("SPX_C100")),
            UnmatchedFeeOverride::Underlying(InstrumentKindDiscriminant::Spot, &spx_usd),
            UnmatchedFeeOverride::Underlying(InstrumentKindDiscriminant::Future, &spy_usd()),
            UnmatchedFeeOverride::Underlying(InstrumentKindDiscriminant::Option, &spy_eur),
        ]
    );
    assert!(unmatched_fee_overrides(&FeeModelOverrides::new(), &instruments).is_empty());
}
