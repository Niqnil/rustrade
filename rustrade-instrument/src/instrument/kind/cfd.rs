use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// `CfdContract` specification containing all the information needed to fully identify a
/// contract-for-difference instrument.
///
/// A CFD is an OTC contract that pays the difference in an underlying's price between open and
/// close. It has no venue, no contract chain and no expiry — it is not a claim on the underlying,
/// so it is never *deliverable*, and its price series may be the broker's own rather than any
/// exchange's book.
///
/// # Type Parameters
/// * `AssetKey` - Type used to identify the settlement asset for the contract.
///
/// # Fields
/// * `contract_size` - Multiplier that determines the actual exposure per contract. Real CFDs are
///   commonly quoted per point (eg/ €25 per index point), so this is genuinely load-bearing: it
///   feeds unrealised PnL, risk notional, and any notional-based fee model (`PercentageFeeModel`
///   scales by it; a per-contract model deliberately does not). Providers that quote CFDs 1:1 with
///   the underlying set it to `Decimal::ONE`.
/// * `settlement_asset` - Asset used for settlement. A CFD is cash-settled in the *account*
///   currency, which is routinely **not** the quote asset (eg/ a GBP-denominated account trading
///   `SPX500/USD`), so it must be carried explicitly rather than inferred from the underlying.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct CfdContract<AssetKey> {
    pub contract_size: Decimal,
    pub settlement_asset: AssetKey,
}
