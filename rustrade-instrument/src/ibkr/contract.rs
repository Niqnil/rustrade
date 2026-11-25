//! Mappings from rustrade instrument names to IB contracts.
//!
//! - [`ContractRegistry`] maps a name to a contract IB has resolved, and that contract's id
//!   back to the name. An execution client needs both directions: IB reports fills, order
//!   listings and positions by contract id.
//! - [`ContractsByName`] maps a name to any contract, resolved or not, for uses that only look
//!   contracts up by name, such as market data subscriptions, where IB resolves the contract
//!   on each request.
//!
//! Both implement [`ContractLookup`], the read a market data stream needs, so a stream can read
//! an execution client's registry without being able to change it.

use crate::instrument::name::InstrumentNameExchange;
use fnv::FnvHashMap;
use ibapi::contracts::Contract;
use parking_lot::RwLock;
use std::sync::Arc;
use thiserror::Error;

/// Looking up an IB contract by rustrade instrument name, read-only.
///
/// What a market data stream needs from its contracts. Implemented by [`ContractRegistry`]
/// and [`ContractsByName`]; implement it to combine them, for example a [`ContractsByName`]
/// that falls back to an execution client's registry.
pub trait ContractLookup {
    /// The contract registered under `name`, compared exactly, case included.
    fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract>;
}

/// Reasons [`ContractRegistry::register`] refuses a contract.
///
/// Both would leave IB's reports for the contract unattributable: IB keys every fill,
/// order listing and position by contract id, and the registry finds the instrument
/// through that id.
///
/// `#[non_exhaustive]`: new reasons may be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ContractRegistryError {
    /// The contract has no IB contract id (`contract_id` is not positive), so IB has
    /// not resolved it. A contract built locally, such as `Contract::stock("AAPL").build()`,
    /// carries id 0 until it is resolved through IB's contract details.
    #[error(
        "contract for {name} has no IB contract id; resolve it through IB's contract details first"
    )]
    Unresolved {
        /// The instrument the contract was to be registered under.
        name: InstrumentNameExchange,
    },

    /// The contract id is already registered under another instrument name. One IB contract
    /// can map back to only one name, so registering it again would move its reports to the
    /// new name.
    #[error("IB contract id {contract_id} is already registered as {registered}, not {name}")]
    ContractIdTaken {
        /// The contract's IB contract id.
        contract_id: i32,
        /// The instrument the contract was to be registered under.
        name: InstrumentNameExchange,
        /// The instrument the contract id is already registered under.
        registered: InstrumentNameExchange,
    },
}

/// Bidirectional registry mapping rustrade instrument names to IB contracts IB has resolved.
///
/// IB's `Contract` is a composite key (symbol, secType, exchange, currency, etc.).
/// Rustrade uses a single string `InstrumentNameExchange`. This registry maintains
/// the mapping in both directions: by name, to build requests, and by IB contract id,
/// to attribute what IB reports back (fills, order listings, positions).
///
/// Every registered name has a contract id that maps back to it:
/// [`register`](Self::register) refuses a contract IB has not resolved, and one whose id is
/// another name's. For lookups by name only, with contracts that need not be resolved, use
/// [`ContractsByName`].
///
/// # Thread Safety
///
/// This type is `Clone` and thread-safe. Cloning creates a shallow copy with
/// shared `Arc` reference to the underlying data.
#[derive(Debug, Clone)]
pub struct ContractRegistry {
    inner: Arc<RwLock<ContractRegistryInner>>,
}

/// Invariant: `by_con_id` holds exactly one entry per `by_name` entry, mapping that entry's
/// contract id back to its name.
#[derive(Debug, Default)]
struct ContractRegistryInner {
    by_name: FnvHashMap<InstrumentNameExchange, Contract>,
    by_con_id: FnvHashMap<i32, InstrumentNameExchange>,
}

impl ContractRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ContractRegistryInner::default())),
        }
    }

    /// Register a contract IB has resolved under its rustrade instrument name, in both
    /// directions: [`get_contract`](Self::get_contract) finds it by name, and
    /// [`get_name_by_con_id`](Self::get_name_by_con_id) finds the name by its contract id.
    ///
    /// Registering a name again replaces its contract. If the new contract has another id,
    /// the earlier id no longer maps back to the name, so anything IB still reports under it,
    /// such as a late fill of an order placed on the earlier contract, can no longer be
    /// attributed to the name.
    ///
    /// # Errors
    ///
    /// Leaves the registry unchanged and returns:
    /// - [`ContractRegistryError::Unresolved`] when the contract has no IB contract id.
    ///   Resolve it through IB's contract details first, or use [`ContractsByName`] where
    ///   nothing maps IB's reports back to the name.
    /// - [`ContractRegistryError::ContractIdTaken`] when the contract id is already
    ///   registered under a different name.
    pub fn register(
        &self,
        name: InstrumentNameExchange,
        contract: Contract,
    ) -> Result<(), ContractRegistryError> {
        let con_id = contract.contract_id;
        if con_id <= 0 {
            return Err(ContractRegistryError::Unresolved { name });
        }

        let mut inner = self.inner.write();
        if let Some(registered) = inner.by_con_id.get(&con_id)
            && *registered != name
        {
            return Err(ContractRegistryError::ContractIdTaken {
                contract_id: con_id,
                name,
                registered: registered.clone(),
            });
        }

        if let Some(old) = inner.by_name.insert(name.clone(), contract) {
            inner.by_con_id.remove(&old.contract_id);
        }
        inner.by_con_id.insert(con_id, name);
        Ok(())
    }

    /// Look up an IB contract by rustrade instrument name.
    pub fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        self.inner.read().by_name.get(name).cloned()
    }

    /// Look up a rustrade instrument name by IB contract ID.
    pub fn get_name_by_con_id(&self, con_id: i32) -> Option<InstrumentNameExchange> {
        self.inner.read().by_con_id.get(&con_id).cloned()
    }

    /// Check if an instrument is registered under this exact name, case included. A
    /// registered name's contract id maps back to it, so what IB reports for the contract
    /// is attributed to it.
    pub fn contains(&self, name: &InstrumentNameExchange) -> bool {
        self.inner.read().by_name.contains_key(name)
    }

    /// Number of registered contracts.
    pub fn len(&self) -> usize {
        self.inner.read().by_name.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().by_name.is_empty()
    }
}

impl Default for ContractRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ContractLookup for ContractRegistry {
    fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        ContractRegistry::get_contract(self, name)
    }
}

/// Map from rustrade instrument names to IB contracts, for lookups by name only.
///
/// For market data subscriptions, where IB resolves the contract description on each
/// request, so a contract need not be resolved first: one built locally, such as
/// `Contract::stock("AAPL").build()`, will do. Nothing maps a contract id back to a name, so
/// an execution client, which attributes IB's reports by contract id, needs a
/// [`ContractRegistry`] instead.
///
/// # Thread Safety
///
/// This type is `Clone` and thread-safe. Cloning creates a shallow copy with
/// shared `Arc` reference to the underlying data, so a contract inserted through one clone is
/// found through every other, such as the one a market data stream reads when it reconnects.
#[derive(Debug, Clone, Default)]
pub struct ContractsByName {
    inner: Arc<RwLock<FnvHashMap<InstrumentNameExchange, Contract>>>,
}

impl ContractsByName {
    /// Create a new empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert `contract` under `name`, returning the contract it replaces, if any.
    pub fn insert(&self, name: InstrumentNameExchange, contract: Contract) -> Option<Contract> {
        self.inner.write().insert(name, contract)
    }

    /// Look up an IB contract by rustrade instrument name, compared exactly, case included.
    pub fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        self.inner.read().get(name).cloned()
    }

    /// Check if a contract is held under this exact name, case included.
    pub fn contains(&self, name: &InstrumentNameExchange) -> bool {
        self.inner.read().contains_key(name)
    }

    /// Number of contracts held.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl ContractLookup for ContractsByName {
    fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        ContractsByName::get_contract(self, name)
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use ibapi::contracts::Contract;

    fn stock_contract(symbol: &str, exchange: &str, currency: &str) -> Contract {
        Contract::stock(symbol)
            .on_exchange(exchange)
            .in_currency(currency)
            .build()
    }

    #[test]
    fn test_contract_registry_basic() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("AAPL");

        let mut contract = stock_contract("AAPL", "SMART", "USD");
        contract.contract_id = 265598;

        registry.register(name.clone(), contract.clone()).unwrap();

        assert!(registry.contains(&name));
        assert_eq!(registry.len(), 1);

        let retrieved = registry.get_contract(&name).unwrap();
        assert_eq!(retrieved.symbol.as_str(), "AAPL");
        assert_eq!(retrieved.contract_id, 265598);

        let name_by_id = registry.get_name_by_con_id(265598).unwrap();
        assert_eq!(name_by_id, name);
    }

    #[test]
    fn test_contract_registry_missing() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("MISSING");

        assert!(!registry.contains(&name));
        assert!(registry.get_contract(&name).is_none());
        assert!(registry.get_name_by_con_id(999999).is_none());
    }

    #[test]
    fn test_contract_registry_reregistration() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("AAPL");

        // Register with first contract_id
        let mut contract1 = stock_contract("AAPL", "SMART", "USD");
        contract1.contract_id = 111111;
        registry.register(name.clone(), contract1).unwrap();

        assert_eq!(registry.get_name_by_con_id(111111), Some(name.clone()));

        // Re-register with different contract_id (e.g., after contract roll)
        let mut contract2 = stock_contract("AAPL", "SMART", "USD");
        contract2.contract_id = 222222;
        registry.register(name.clone(), contract2).unwrap();

        // New mapping works
        assert_eq!(registry.get_name_by_con_id(222222), Some(name.clone()));
        // Old mapping is cleared
        assert!(registry.get_name_by_con_id(111111).is_none());
        // Still only one entry
        assert_eq!(registry.len(), 1);
    }

    fn resolved(symbol: &str, contract_id: i32) -> Contract {
        Contract {
            contract_id,
            ..stock_contract(symbol, "SMART", "USD")
        }
    }

    #[test]
    fn an_unresolved_contract_is_refused_and_nothing_is_registered() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("AAPL");

        for contract_id in [0, -1] {
            assert_eq!(
                registry.register(name.clone(), resolved("AAPL", contract_id)),
                Err(ContractRegistryError::Unresolved { name: name.clone() })
            );
        }
        assert!(registry.is_empty());
        assert!(registry.get_name_by_con_id(0).is_none());
    }

    #[test]
    fn a_contract_id_registered_under_another_name_is_refused() {
        let registry = ContractRegistry::new();
        let aapl = InstrumentNameExchange::from("AAPL");
        let other = InstrumentNameExchange::from("AAPL-2");
        registry
            .register(aapl.clone(), resolved("AAPL", 265598))
            .unwrap();

        assert_eq!(
            registry.register(other.clone(), resolved("AAPL", 265598)),
            Err(ContractRegistryError::ContractIdTaken {
                contract_id: 265598,
                name: other.clone(),
                registered: aapl.clone(),
            })
        );
        assert_eq!(registry.get_name_by_con_id(265598), Some(aapl));
        assert!(!registry.contains(&other));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registering_the_same_name_and_contract_id_again_is_accepted() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("AAPL");
        registry
            .register(name.clone(), resolved("AAPL", 265598))
            .unwrap();

        registry
            .register(name.clone(), resolved("AAPL", 265598))
            .unwrap();

        assert_eq!(registry.get_name_by_con_id(265598), Some(name));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn a_contract_id_freed_by_reregistration_can_be_registered_under_another_name() {
        let registry = ContractRegistry::new();
        let front = InstrumentNameExchange::from("ES-FRONT");
        let next = InstrumentNameExchange::from("ES-NEXT");
        registry
            .register(front.clone(), resolved("ES", 111))
            .unwrap();

        // The front month rolls to a new contract; its old id becomes free.
        registry
            .register(front.clone(), resolved("ES", 222))
            .unwrap();
        registry
            .register(next.clone(), resolved("ES", 111))
            .unwrap();

        assert_eq!(registry.get_name_by_con_id(111), Some(next));
        assert_eq!(registry.get_name_by_con_id(222), Some(front));
    }

    #[test]
    fn contracts_by_name_hold_unresolved_contracts_and_report_a_replacement() {
        let contracts = ContractsByName::new();
        let name = InstrumentNameExchange::from("AAPL");
        assert!(contracts.is_empty());

        let unresolved = stock_contract("AAPL", "SMART", "USD");
        assert!(contracts.insert(name.clone(), unresolved).is_none());
        let replaced = contracts.insert(name.clone(), resolved("AAPL", 265598));

        assert_eq!(replaced.map(|contract| contract.contract_id), Some(0));
        assert_eq!(contracts.get_contract(&name).unwrap().contract_id, 265598);
        assert!(contracts.contains(&name));
        assert!(!contracts.contains(&InstrumentNameExchange::from("aapl")));
        assert_eq!(contracts.len(), 1);
    }

    #[test]
    fn a_contract_inserted_through_one_clone_is_found_through_another() {
        let contracts = ContractsByName::new();
        let reader = contracts.clone();
        let name = InstrumentNameExchange::from("MSFT");

        contracts.insert(name.clone(), stock_contract("MSFT", "SMART", "USD"));

        assert_eq!(reader.get_contract(&name).unwrap().symbol.as_str(), "MSFT");
    }

    #[test]
    fn both_types_answer_a_lookup_by_name() {
        let registry = ContractRegistry::new();
        let by_name = ContractsByName::new();
        let traded = InstrumentNameExchange::from("AAPL");
        let quoted = InstrumentNameExchange::from("MSFT");
        registry
            .register(traded.clone(), resolved("AAPL", 265598))
            .unwrap();
        by_name.insert(quoted.clone(), stock_contract("MSFT", "SMART", "USD"));

        let lookups: [&dyn ContractLookup; 2] = [&registry, &by_name];
        let found = |lookup: &dyn ContractLookup, name: &InstrumentNameExchange| {
            lookup
                .get_contract(name)
                .map(|contract| contract.symbol.to_string())
        };

        assert_eq!(found(lookups[0], &traded).as_deref(), Some("AAPL"));
        assert_eq!(found(lookups[0], &quoted), None);
        assert_eq!(found(lookups[1], &quoted).as_deref(), Some("MSFT"));
        assert_eq!(found(lookups[1], &traded), None);
    }
}
