use crate::instrument::name::InstrumentNameExchange;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};

/// The venue an [`Instrument`](super::Instrument)s **market data** is sourced from, when that
/// differs from the venue it is **executed** on.
///
/// # Why this exists
/// Position and `pnl_unrealised` state is strictly `InstrumentIndex`-scoped: a position opened by a
/// fill only receives live PnL from market events tagged with *that* index. Modelling "priced by
/// venue A, traded on venue B" as two separate `Instrument`s would therefore leave the traded
/// instrument's PnL permanently stale, because the price updates would land on the other index.
/// A `DataVenue` keeps both roles on **one** instrument, and so on one ledger.
///
/// # Fallback
/// Both fields fall back to the execution venue's equivalents, so a `DataVenue` only has to state
/// what actually differs:
/// - no `DataVenue` at all ⇒ market data is sourced from `Instrument::exchange` under
///   `Instrument::name_exchange`, which is the behaviour of every single-venue instrument;
/// - a `DataVenue` with `name_exchange: None` ⇒ the data venue spells the symbol exactly as the
///   execution venue does.
///
/// Read them through [`Instrument::data_exchange`](super::Instrument::data_exchange) and
/// [`Instrument::data_name_exchange`](super::Instrument::data_name_exchange) rather than reaching
/// into the fields, so the fallback is applied in one place.
///
/// # `name_exchange` is not cosmetic
/// Two venues routinely spell one economic instrument differently — London Strategic Edge quotes BP
/// as `BP.L` while a US broker lists it as `BP` — so a data venue that could only carry an exchange
/// identifier would subscribe under the *execution* venue's symbol and silently receive nothing, or
/// worse, receive a different instrument's prices.
///
/// # Suitability (caller obligation)
/// Sourcing prices from one venue and filling on another means **the decision price is not the fill
/// price**: the two venues have separate books, and a data venue that publishes an aggregated or
/// synthetic series (a CFD or index proxy) has no book at all. That basis mismatch is sound for
/// research and paper trading, but for real capital it is a property the caller must opt into
/// knowingly — the library cannot detect it.
///
/// # When a `DataVenue` is the wrong tool
/// A `DataVenue` models **one** economic instrument that two venues both quote — the same shares,
/// the same contract, the same pair — where all that differs is who publishes the prices and how
/// they spell the symbol. It does not model *related but distinct* instruments: a spot-metal or
/// index CFD against the corresponding future, a perpetual against its underlying spot, one
/// maturity against another. Those have their own tick sizes, expiries, funding and basis;
/// collapsing them onto one `Instrument` would mark a position against a series that is not that
/// position's own market, and the resulting `pnl_unrealised` would be wrong rather than stale.
///
/// Trading one off the other is the **two-instrument pattern**, and it needs no library support:
///
/// 1. Register **both** as ordinary `Instrument`s — the one you price, and the one you trade. Each
///    carries its own kind, its own underlying and its own venue. Only the traded instrument needs
///    an execution client registered against its exchange.
/// 2. Subscribe to market data for both, or for the priced one alone.
/// 3. Read the priced instrument's state in the strategy, and emit orders keyed to the **traded**
///    instrument. Nothing couples an order's instrument to the instrument whose prices motivated
///    it — the order interface takes whatever instrument key you give it.
/// 4. Own the conversion. Hedge ratio, contract multiplier, quantity and basis are a trading
///    decision the library has no way to derive, and getting them wrong is silent.
///
/// The consequence to accept is the mirror image of the case this type exists for: the two
/// instruments keep **separate** ledgers. Position and `pnl_unrealised` live on the traded
/// instrument, which is correct, and the priced instrument holds no position at all. Reconciling
/// them — treating an exposure opened against one as a hedge for a signal from the other — is the
/// caller's, because only the caller knows the relationship.
///
/// This is also what makes a non-executable kind usable for live decisions. An instrument kind no
/// execution client accepts can still price a strategy; it simply cannot be the instrument an order
/// names.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct DataVenue<ExchangeKey> {
    /// Venue the market data is sourced from.
    pub exchange: ExchangeKey,

    /// Symbol the data venue uses for this instrument.
    ///
    /// `None` ⇒ the data venue spells it exactly as the execution venue does.
    #[serde(default)]
    pub name_exchange: Option<InstrumentNameExchange>,
}

impl<ExchangeKey> DataVenue<ExchangeKey> {
    /// Construct a `DataVenue` that shares the execution venue's symbol.
    pub fn new_same_name(exchange: ExchangeKey) -> Self {
        Self {
            exchange,
            name_exchange: None,
        }
    }

    /// Map this `DataVenue`s `ExchangeKey` to a new key, using the provided lookup closure.
    pub fn map_exchange_key<FnFindExchange, NewExchangeKey>(
        self,
        find_exchange: FnFindExchange,
    ) -> DataVenue<NewExchangeKey>
    where
        FnFindExchange: FnOnce(&ExchangeKey) -> NewExchangeKey,
    {
        let exchange = find_exchange(&self.exchange);

        DataVenue {
            exchange,
            name_exchange: self.name_exchange,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::ExchangeId;

    #[test]
    fn new_same_name_defers_the_symbol_to_the_execution_venue() {
        let data_venue = DataVenue::new_same_name(ExchangeId::LseEquities);

        assert_eq!(data_venue.exchange, ExchangeId::LseEquities);
        assert_eq!(
            data_venue.name_exchange, None,
            "None is what makes `Instrument::data_name_exchange` fall back, so it must not be \
             filled in with the execution venue's symbol here"
        );
    }

    #[test]
    fn map_exchange_key_rewrites_the_venue_and_carries_the_symbol_through() {
        let data_venue = DataVenue::new(
            ExchangeId::LseEquities,
            Some(InstrumentNameExchange::from("BP.L")),
        );

        let mapped = data_venue.map_exchange_key(|exchange| exchange.as_str().to_string());

        assert_eq!(mapped.exchange, "lse_equities");
        assert_eq!(
            mapped.name_exchange,
            Some(InstrumentNameExchange::from("BP.L")),
            "the data venue's own symbol must survive indexing - re-resolving it against the \
             execution venue is what silently subscribes to the wrong instrument"
        );
    }

    #[test]
    fn a_data_venue_without_a_symbol_deserialises_from_a_payload_that_omits_the_field() {
        // `name_exchange` is `#[serde(default)]`, so a config naming only the venue is valid.
        let data_venue: DataVenue<ExchangeId> =
            serde_json::from_str(r#"{"exchange":"lse_equities"}"#).unwrap();

        assert_eq!(
            data_venue,
            DataVenue::new_same_name(ExchangeId::LseEquities)
        );
    }
}
