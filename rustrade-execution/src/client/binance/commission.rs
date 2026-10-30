//! An account's commission rates on a Binance symbol, which both clients read from the spot
//! `GET /api/v3/account/commission`.

use super::shared::{
    RateLimitTracker, RequestKind, classify_rest_query_error, optional_decimal,
    response_decode_error, rest_call_with_retry,
};
use crate::{error::UnindexedClientError, fee::Liquidity};
use binance_sdk::spot::rest_api::{AccountCommissionParams, AccountCommissionResponse, RestApi};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// One of the three parts of a Binance commission: the rates it charges a trade, as fractions of
/// the amount the trade gives the account.
///
/// A trade pays [`maker`](Self::maker) or [`taker`](Self::taker) by its liquidity, plus
/// [`buyer`](Self::buyer) or [`seller`](Self::seller) by its side: see [`rate`](Self::rate).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionPart {
    /// The rate on a trade that supplied liquidity.
    pub maker: Decimal,
    /// The rate on a trade that took liquidity.
    pub taker: Decimal,
    /// The rate added on a buy.
    pub buyer: Decimal,
    /// The rate added on a sell.
    pub seller: Decimal,
}

impl CommissionPart {
    /// A part with these rates.
    pub fn new(maker: Decimal, taker: Decimal, buyer: Decimal, seller: Decimal) -> Self {
        Self {
            maker,
            taker,
            buyer,
            seller,
        }
    }

    /// This part's rate on a trade on `side` with `liquidity`: [`maker`](Self::maker) or
    /// [`taker`](Self::taker), plus [`buyer`](Self::buyer) or [`seller`](Self::seller).
    ///
    /// `None` only when a `Decimal` cannot hold the sum.
    #[must_use]
    pub fn rate(&self, side: Side, liquidity: Liquidity) -> Option<Decimal> {
        let by_liquidity = match liquidity {
            Liquidity::Maker => self.maker,
            Liquidity::Taker => self.taker,
        };
        let by_side = match side {
            Side::Buy => self.buyer,
            Side::Sell => self.seller,
        };
        by_liquidity.checked_add(by_side)
    }
}

/// The discount Binance gives a commission paid in its discount asset (BNB).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionDiscount {
    /// Whether the account pays its commissions in [`asset`](Self::asset), a setting the account
    /// holder chooses.
    pub enabled_for_account: bool,
    /// Whether the symbol allows paying in [`asset`](Self::asset).
    pub enabled_for_symbol: bool,
    /// The asset a discounted commission is paid in. `None` when Binance names none, as Spot
    /// Testnet does.
    pub asset: Option<AssetNameExchange>,
    /// What the standard part is multiplied by when paid in [`asset`](Self::asset): 0.75 charges
    /// 75% of it. Binance's `discount`, which is this multiplier, not the share taken off.
    pub multiplier: Decimal,
}

impl CommissionDiscount {
    /// A discount with these terms.
    pub fn new(
        enabled_for_account: bool,
        enabled_for_symbol: bool,
        asset: Option<AssetNameExchange>,
        multiplier: Decimal,
    ) -> Self {
        Self {
            enabled_for_account,
            enabled_for_symbol,
            asset,
            multiplier,
        }
    }
}

/// An account's commission rates on one symbol, as Binance's `GET /api/v3/account/commission`
/// reports them, its three parts kept apart.
///
/// # How Binance charges them
///
/// - **Each part is charged on every trade:** the amount the trade gives the account (the base
///   quantity on a buy, the quote notional on a sell) times the part's
///   [`rate`](CommissionPart::rate) for the trade's side and liquidity.
/// - **Not paid in the discount asset,** the three are charged in the asset the trade gives, and
///   [`rate`](Self::rate) is their sum.
/// - **Paid in the discount asset** (both of [`discount`](Self::discount)'s flags set and enough of
///   it held), only [`standard`](Self::standard) is multiplied by
///   [`multiplier`](CommissionDiscount::multiplier). [`special`](Self::special) and
///   [`tax`](Self::tax) are not discounted. All three are converted into the discount asset at a
///   price Binance does not report, so no rate here gives that commission. With too little of the
///   discount asset, the whole commission is charged in the asset the trade gives, undiscounted.
///
/// These are the account's rates when read, at its VIP level and any promotion, and they can
/// change. A fill's own fee is what it was charged.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionRates {
    /// The symbol the rates are for.
    pub instrument: InstrumentNameExchange,
    /// The standard commission, the only part the discount applies to.
    pub standard: CommissionPart,
    /// The extra commission Binance adds in specific circumstances.
    pub special: CommissionPart,
    /// The tax commission.
    pub tax: CommissionPart,
    /// The discount for paying in the discount asset. `None` when Binance leaves it out of its
    /// answer, which says nothing about whether one applies.
    pub discount: Option<CommissionDiscount>,
}

impl CommissionRates {
    /// The rate Binance charges a trade on `side` with `liquidity` when the commission is not
    /// paid in the discount asset: the sum of [`standard`](Self::standard)'s,
    /// [`special`](Self::special)'s and [`tax`](Self::tax)'s [`rate`](CommissionPart::rate), a
    /// fraction of the amount the trade gives the account.
    ///
    /// `None` only when a `Decimal` cannot hold the sum.
    #[must_use]
    pub fn rate(&self, side: Side, liquidity: Liquidity) -> Option<Decimal> {
        self.standard
            .rate(side, liquidity)?
            .checked_add(self.special.rate(side, liquidity)?)?
            .checked_add(self.tax.rate(side, liquidity)?)
    }
}

/// Read the account's commission rates on `instrument` over the spot `rest`, with the rate-limit
/// retry the clients' other requests have. It costs 20 of Binance's per-IP spot request weight.
pub(crate) async fn fetch_commission_rates(
    rest: &Arc<RestApi>,
    rate_limiter: &RateLimitTracker,
    instrument: &InstrumentNameExchange,
) -> Result<CommissionRates, UnindexedClientError> {
    let symbol = instrument.name().to_string();
    let response = rest_call_with_retry(rest, rate_limiter, RequestKind::Query, |rest| {
        let symbol = symbol.clone();
        Box::pin(async move {
            let params = AccountCommissionParams::builder(symbol).build()?;
            rest.account_commission(params).await
        })
    })
    .await
    .map_err(|error| classify_rest_query_error(&error, Some(instrument)))?;
    let answer = response.data().await.map_err(response_decode_error)?;
    commission_rates(instrument, answer)
}

/// `answer`'s rates on `instrument`. Every part and rate must be present and read as a decimal;
/// only the discount block, and the discount asset in it, may be left out.
fn commission_rates(
    instrument: &InstrumentNameExchange,
    answer: AccountCommissionResponse,
) -> Result<CommissionRates, UnindexedClientError> {
    if let Some(symbol) = answer.symbol.as_deref()
        && symbol != instrument.name().as_str()
    {
        return Err(decode_error(format!(
            "account commission asked for {instrument} answers for {symbol}"
        )));
    }
    // The SDK has a type of its own for each part, with the same four fields.
    macro_rules! rates_of {
        ($part:expr) => {
            $part.map(|part| [part.maker, part.taker, part.buyer, part.seller])
        };
    }
    let standard = rates_of!(answer.standard_commission);
    let special = rates_of!(answer.special_commission);
    let tax = rates_of!(answer.tax_commission);
    let discount = answer
        .discount
        .map(|discount| {
            let discount = *discount;
            let flag = |name: &str, value: Option<bool>| {
                value.ok_or_else(|| decode_error(format!("discount.{name} missing")))
            };
            Ok::<_, UnindexedClientError>(CommissionDiscount {
                enabled_for_account: flag("enabledForAccount", discount.enabled_for_account)?,
                enabled_for_symbol: flag("enabledForSymbol", discount.enabled_for_symbol)?,
                asset: discount.discount_asset.map(AssetNameExchange::new),
                multiplier: required_decimal("discount.discount", discount.discount.as_deref())?,
            })
        })
        .transpose()?;
    Ok(CommissionRates {
        instrument: instrument.clone(),
        standard: commission_part("standardCommission", standard)?,
        special: commission_part("specialCommission", special)?,
        tax: commission_part("taxCommission", tax)?,
        discount,
    })
}

/// The part named `name` from its maker, taker, buyer and seller rates, each required.
fn commission_part(
    name: &str,
    rates: Option<[Option<String>; 4]>,
) -> Result<CommissionPart, UnindexedClientError> {
    let [maker, taker, buyer, seller] =
        rates.ok_or_else(|| decode_error(format!("{name} missing")))?;
    let rate = |field: &str, value: Option<String>| {
        required_decimal(&format!("{name}.{field}"), value.as_deref())
    };
    Ok(CommissionPart {
        maker: rate("maker", maker)?,
        taker: rate("taker", taker)?,
        buyer: rate("buyer", buyer)?,
        seller: rate("seller", seller)?,
    })
}

/// A decimal Binance must report: absent or unreadable is an error.
fn required_decimal(field: &str, value: Option<&str>) -> Result<Decimal, UnindexedClientError> {
    optional_decimal(field, value)?.ok_or_else(|| decode_error(format!("{field} missing")))
}

fn decode_error(detail: String) -> UnindexedClientError {
    UnindexedClientError::Internal(format!("decoding Binance response: {detail}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn answer(body: serde_json::Value) -> AccountCommissionResponse {
        serde_json::from_value(body).unwrap()
    }

    fn part(maker: &str, taker: &str, buyer: &str, seller: &str) -> serde_json::Value {
        serde_json::json!({"maker": maker, "taker": taker, "buyer": buyer, "seller": seller})
    }

    /// Binance's documented example answer, with distinct rates in every slot.
    fn documented() -> serde_json::Value {
        serde_json::json!({
            "symbol": "BTCUSDT",
            "standardCommission": part("0.00000010", "0.00000020", "0.00000030", "0.00000040"),
            "specialCommission": part("0.01000000", "0.02000000", "0.03000000", "0.04000000"),
            "taxCommission": part("0.00000112", "0.00000114", "0.00000118", "0.00000116"),
            "discount": {
                "enabledForAccount": true,
                "enabledForSymbol": false,
                "discountAsset": "BNB",
                "discount": "0.75000000"
            }
        })
    }

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    #[test]
    fn commission_rates_keep_every_part_and_the_discount() {
        let rates = commission_rates(&btcusdt(), answer(documented())).unwrap();
        assert_eq!(rates.instrument, btcusdt());
        assert_eq!(
            rates.standard,
            CommissionPart::new(
                dec!(0.0000001),
                dec!(0.0000002),
                dec!(0.0000003),
                dec!(0.0000004)
            )
        );
        assert_eq!(
            rates.special,
            CommissionPart::new(dec!(0.01), dec!(0.02), dec!(0.03), dec!(0.04))
        );
        assert_eq!(
            rates.tax,
            CommissionPart::new(
                dec!(0.00000112),
                dec!(0.00000114),
                dec!(0.00000118),
                dec!(0.00000116)
            )
        );
        assert_eq!(
            rates.discount,
            Some(CommissionDiscount::new(
                true,
                false,
                Some(AssetNameExchange::new("BNB")),
                dec!(0.75)
            ))
        );
    }

    #[test]
    fn commission_rate_adds_the_liquidity_and_side_rates_of_every_part() {
        let rates = commission_rates(&btcusdt(), answer(documented())).unwrap();
        // Binance's worked example: a taker sell pays taker + seller of each part.
        let cases = [
            (
                Side::Sell,
                Liquidity::Taker,
                dec!(0.0000006) + dec!(0.06) + dec!(0.0000023),
            ),
            (
                Side::Sell,
                Liquidity::Maker,
                dec!(0.0000005) + dec!(0.05) + dec!(0.00000228),
            ),
            (
                Side::Buy,
                Liquidity::Taker,
                dec!(0.0000005) + dec!(0.05) + dec!(0.00000232),
            ),
            (
                Side::Buy,
                Liquidity::Maker,
                dec!(0.0000004) + dec!(0.04) + dec!(0.0000023),
            ),
        ];
        for (side, liquidity, expected) in cases {
            assert_eq!(
                rates.rate(side, liquidity),
                Some(expected),
                "{side:?} {liquidity:?}"
            );
        }
        assert_eq!(
            rates.standard.rate(Side::Buy, Liquidity::Maker),
            Some(dec!(0.0000004))
        );
    }

    #[test]
    fn commission_rate_is_none_past_decimal_range() {
        let max = Decimal::MAX;
        let part = CommissionPart::new(max, max, Decimal::ONE, Decimal::ZERO);
        assert_eq!(part.rate(Side::Buy, Liquidity::Maker), None);
        assert_eq!(part.rate(Side::Sell, Liquidity::Maker), Some(max));
        let zero = CommissionPart::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
        let one = CommissionPart::new(Decimal::ONE, Decimal::ONE, Decimal::ZERO, Decimal::ZERO);
        let near_max = CommissionPart::new(max, max, Decimal::ZERO, Decimal::ZERO);
        for (standard, special, tax) in [
            (near_max, one, zero),
            (near_max, zero, one),
            (part, zero, zero),
        ] {
            let rates = CommissionRates {
                instrument: btcusdt(),
                standard,
                special,
                tax,
                discount: None,
            };
            assert_eq!(rates.rate(Side::Buy, Liquidity::Maker), None);
        }
    }

    #[test]
    fn commission_rates_without_a_discount_block_or_asset_leave_them_unknown() {
        let mut body = documented();
        body.as_object_mut().unwrap().remove("discount");
        let rates = commission_rates(&btcusdt(), answer(body)).unwrap();
        assert_eq!(rates.discount, None);

        // Spot Testnet's answer: every rate zero and no discount asset.
        let mut body = documented();
        body["discount"]["discountAsset"] = serde_json::Value::Null;
        let rates = commission_rates(&btcusdt(), answer(body)).unwrap();
        assert_eq!(rates.discount.unwrap().asset, None);
    }

    #[test]
    fn commission_rates_refuse_a_missing_or_unreadable_value() {
        let mut cases = Vec::new();
        for part in ["standardCommission", "specialCommission", "taxCommission"] {
            let mut body = documented();
            body.as_object_mut().unwrap().remove(part);
            cases.push((format!("{part} missing"), body));
            for field in ["maker", "taker", "buyer", "seller"] {
                let mut body = documented();
                body[part].as_object_mut().unwrap().remove(field);
                cases.push((format!("{part}.{field} missing"), body));
                let mut body = documented();
                body[part][field] = serde_json::json!("not a rate");
                cases.push((format!("{part}.{field} \"not a rate\""), body));
            }
        }
        for field in ["enabledForAccount", "enabledForSymbol", "discount"] {
            let mut body = documented();
            body["discount"].as_object_mut().unwrap().remove(field);
            cases.push((format!("discount.{field} missing"), body));
        }
        let mut body = documented();
        body["discount"]["discount"] = serde_json::json!("0.7.5");
        cases.push(("discount.discount \"0.7.5\"".to_string(), body));
        let mut body = documented();
        body["symbol"] = serde_json::json!("ETHUSDT");
        cases.push(("answers for ETHUSDT".to_string(), body));

        for (expected, body) in cases {
            match commission_rates(&btcusdt(), answer(body)) {
                Err(UnindexedClientError::Internal(message)) => {
                    assert!(message.contains(&expected), "{expected}: {message}");
                }
                other => panic!("{expected}: {other:?}"),
            }
        }
    }

    #[test]
    fn commission_rates_without_a_symbol_are_the_requested_ones() {
        let mut body = documented();
        body.as_object_mut().unwrap().remove("symbol");
        let rates = commission_rates(&btcusdt(), answer(body)).unwrap();
        assert_eq!(rates.instrument, btcusdt());
    }
}
