//! Funding payments on the perpetuals account stream, from the `userFundings` subscription.
//!
//! See the [client docs](super#funding) for what is sent and how repeats are suppressed.

use super::{
    common::{millis_to_datetime, parse_decimal},
    perp_dexes::PerpDexes,
};
use crate::{
    AccountEvent, AccountEventKind, UnindexedAccountEvent,
    cash_flow::{CashFlow, CashFlowKind},
    client::dedup::{DedupEventKind, DedupKey, SharedDedupCache, is_duplicate},
};
use hyperliquid_rust_sdk::{UserFunding, UserFundingsData};
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use smol_str::{SmolStr, format_smolstr};
use tokio::sync::mpsc;
use tracing::trace;

/// Convert an SDK funding payment to a [`CashFlow`], `None` if its coin names no perpetual this
/// client trades or its amount or time does not parse. A rate or position that does not parse is
/// left `None` and the payment is still sent; [`parse_decimal`] logs each failure.
///
/// `usdc` is the amount, signed as [`CashFlow::amount`] is: Hyperliquid's documented examples show
/// a long paying at a positive rate as negative and a short receiving as positive. It is taken to
/// be in the collateral of the perpetual's DEX, which is USDC except on a HIP-3 DEX that settles in
/// another token. The rate and the signed position `szi` are kept as the venue gives them.
pub(super) fn funding_cash_flow(
    funding: &UserFunding,
    dexes: &PerpDexes,
) -> Option<CashFlow<AssetNameExchange, InstrumentNameExchange>> {
    let (instrument, collateral) = dexes.perp(&funding.coin)?;
    let amount = parse_decimal(&funding.usdc, "funding.usdc")?;
    let time_exchange = millis_to_datetime(funding.time)?;

    Some(CashFlow::new(
        CashFlowKind::Funding {
            rate: parse_decimal(&funding.funding_rate, "funding.funding_rate"),
            position_quantity: parse_decimal(&funding.szi, "funding.szi"),
        },
        AssetNameExchange::from(collateral),
        amount,
        Some(instrument),
        time_exchange,
        // Hyperliquid gives a funding payment no id; its coin and time identify it.
        None,
    ))
}

/// Send each of `fundings` on `tx` as an [`AccountEventKind::CashFlow`], skipping those `convert`
/// leaves out and those already sent through `dedup`.
///
/// The subscription opens with a snapshot of recent payments and the SDK resubscribes on every
/// reconnect, so the same payment is redelivered each time the socket comes back. Hyperliquid
/// pays funding at most once per coin at a time, so a payment is keyed on its coin and time.
///
/// Returns whether every event was sent, which it is unless the consumer has gone.
pub(super) fn send_fundings(
    fundings: &UserFundingsData,
    mut convert: impl FnMut(&UserFunding) -> Option<CashFlow<AssetNameExchange, InstrumentNameExchange>>,
    dedup: &SharedDedupCache,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
) -> bool {
    for funding in &fundings.fundings {
        let Some(flow) = convert(funding) else {
            continue;
        };
        let key = DedupKey {
            instrument: SmolStr::new(&funding.coin),
            id: format_smolstr!("{}", funding.time),
            kind: DedupEventKind::Funding,
        };
        if is_duplicate(dedup, key) {
            trace!(
                coin = %funding.coin,
                time = funding.time,
                "Hyperliquid dedup: skipping funding payment already delivered"
            );
            continue;
        }
        let event = AccountEvent::new(
            ExchangeId::HyperliquidPerp,
            AccountEventKind::CashFlow(flow),
        );
        if tx.send(event).is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::client::dedup::new_dedup_cache;
    use chrono::{TimeZone, Utc};
    use ethers::types::H160;
    use rust_decimal_macros::dec;

    fn funding(coin: &str, time: u64, usdc: &str, szi: &str, rate: &str) -> UserFunding {
        serde_json::from_value(serde_json::json!({
            "time": time,
            "coin": coin,
            "usdc": usdc,
            "szi": szi,
            "fundingRate": rate,
        }))
        .unwrap()
    }

    fn data(is_snapshot: Option<bool>, fundings: Vec<UserFunding>) -> UserFundingsData {
        UserFundingsData {
            is_snapshot,
            user: H160::zero(),
            fundings,
        }
    }

    fn received(rx: &mut mpsc::UnboundedReceiver<UnindexedAccountEvent>) -> Vec<CashFlowKey> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|event| match event.kind {
                AccountEventKind::CashFlow(flow) => (
                    flow.instrument.unwrap().to_string(),
                    flow.time_exchange.timestamp_millis(),
                ),
                other => panic!("expected CashFlow, got {other:?}"),
            })
            .collect()
    }

    type CashFlowKey = (String, i64);

    /// Hyperliquid's documented example: a long of 49.1477 ETH paying at a positive rate.
    #[test]
    fn a_funding_payment_becomes_a_signed_cash_flow_in_the_collateral() {
        let flow = funding_cash_flow(
            &funding(
                "ETH",
                1_681_222_254_710,
                "-3.625312",
                "49.1477",
                "0.0000417",
            ),
            &PerpDexes::default(),
        )
        .unwrap();

        assert_eq!(
            flow,
            CashFlow::new(
                CashFlowKind::Funding {
                    rate: Some(dec!(0.0000417)),
                    position_quantity: Some(dec!(49.1477)),
                },
                AssetNameExchange::new("USDC"),
                dec!(-3.625312),
                Some(InstrumentNameExchange::new("ETH-USDC-PERP")),
                Utc.timestamp_millis_opt(1_681_222_254_710).unwrap(),
                None,
            )
        );
    }

    /// A HIP-3 perpetual's payment is in its DEX's collateral; a coin this client does not trade
    /// as a perpetual, such as a spot pair or one on a DEX not configured, is left out.
    #[tokio::test]
    async fn a_funding_payment_is_in_its_dex_collateral_and_others_are_left_out() {
        let dexes = super::super::perp_dexes::tests::xyz_and_flx().await;
        let flow =
            |coin| funding_cash_flow(&funding(coin, 1, "2.378343", "-15.0", "0.00000625"), &dexes);

        let flx = flow("flx:TSLA").unwrap();
        assert_eq!(flx.asset, AssetNameExchange::new("USDH"));
        assert_eq!(
            flx.instrument,
            Some(InstrumentNameExchange::new("flx:TSLA-USDH-PERP"))
        );
        assert_eq!(flx.amount, dec!(2.378343));
        assert_eq!(
            flow("xyz:TSLA").unwrap().asset,
            AssetNameExchange::new("USDC")
        );
        assert!(flow("@107").is_none(), "a spot pair is left out");
        assert!(
            flow("abc:TSLA").is_none(),
            "an unconfigured DEX is left out"
        );
    }

    /// An amount that does not parse leaves the payment out; a rate or position that does not
    /// parse is left `None`, and the payment still goes out.
    #[test]
    fn only_an_unparsed_amount_leaves_a_funding_payment_out() {
        let dexes = PerpDexes::default();
        let kind = |szi, rate| {
            funding_cash_flow(&funding("BTC", 1, "-1", szi, rate), &dexes).map(|flow| flow.kind)
        };

        assert!(funding_cash_flow(&funding("BTC", 1, "x", "1", "0.0001"), &dexes).is_none());
        assert_eq!(
            kind("x", "0.0001"),
            Some(CashFlowKind::Funding {
                rate: Some(dec!(0.0001)),
                position_quantity: None,
            })
        );
        assert_eq!(
            kind("1", "x"),
            Some(CashFlowKind::Funding {
                rate: None,
                position_quantity: Some(dec!(1)),
            })
        );
    }

    /// The snapshot that opens each resubscription repeats payments already sent; only the new
    /// ones go out. Payments of two coins at one time are distinct.
    #[test]
    fn payments_a_resubscription_repeats_are_sent_once() {
        let dexes = PerpDexes::default();
        let dedup = new_dedup_cache();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let convert = |funding: &UserFunding| funding_cash_flow(funding, &dexes);
        let btc_1 = || funding("BTC", 1_000, "-1", "1", "0.0001");
        let eth_1 = || funding("ETH", 1_000, "2", "-3", "0.0001");
        let btc_2 = || funding("BTC", 2_000, "-1", "1", "0.0001");

        assert!(send_fundings(
            &data(Some(true), vec![btc_1(), eth_1()]),
            convert,
            &dedup,
            &tx
        ));
        assert!(send_fundings(
            &data(None, vec![btc_2()]),
            convert,
            &dedup,
            &tx
        ));
        assert!(send_fundings(
            &data(Some(true), vec![btc_1(), eth_1(), btc_2()]),
            convert,
            &dedup,
            &tx
        ));

        assert_eq!(
            received(&mut rx),
            [
                ("BTC-USDC-PERP".to_owned(), 1_000),
                ("ETH-USDC-PERP".to_owned(), 1_000),
                ("BTC-USDC-PERP".to_owned(), 2_000),
            ]
        );
    }

    #[test]
    fn sending_stops_once_the_consumer_has_gone() {
        let dexes = PerpDexes::default();
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        assert!(!send_fundings(
            &data(None, vec![funding("BTC", 1, "-1", "1", "0.0001")]),
            |funding| funding_cash_flow(funding, &dexes),
            &new_dedup_cache(),
            &tx
        ));
    }
}
