//! Bidirectional registry mapping rustrade instrument names to IB contracts.

use crate::instrument::name::InstrumentNameExchange;
use fnv::FnvHashMap;
use ibapi::contracts::Contract;
use parking_lot::RwLock;
use std::sync::Arc;
use thiserror::Error;

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

/// Bidirectional registry mapping rustrade instrument names to IB contracts.
///
/// IB's `Contract` is a composite key (symbol, secType, exchange, currency, etc.).
/// Rustrade uses a single string `InstrumentNameExchange`. This registry maintains
/// the mapping in both directions: by name, to build requests, and by IB contract id,
/// to attribute what IB reports back (fills, order listings, positions).
///
/// [`register`](Self::register) fills both directions and requires a contract IB has
/// resolved. [`register_by_name_only`](Self::register_by_name_only) fills only the
/// name direction, for uses that never map IB's reports back, such as market data
/// subscriptions, where IB resolves the contract on each request.
///
/// # Thread Safety
///
/// This type is `Clone` and thread-safe. Cloning creates a shallow copy with
/// shared `Arc` reference to the underlying data.
#[derive(Debug, Clone)]
pub struct ContractRegistry {
    inner: Arc<RwLock<ContractRegistryInner>>,
}

#[derive(Debug, Default)]
struct ContractRegistryInner {
    by_name: FnvHashMap<InstrumentNameExchange, Contract>,
    by_con_id: FnvHashMap<i32, InstrumentNameExchange>,
}

impl ContractRegistryInner {
    /// Remove the contract id that maps back to `name`, if one does.
    fn remove_reverse_mapping(&mut self, name: &InstrumentNameExchange) {
        let Some(old_con_id) = self.by_name.get(name).map(|contract| contract.contract_id) else {
            return;
        };
        // A name registered by name only keeps no reverse mapping, and its contract id
        // may be another name's.
        if self.by_con_id.get(&old_con_id) == Some(name) {
            self.by_con_id.remove(&old_con_id);
        }
    }
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
    /// Registering a name again replaces its contract, and its earlier contract id no
    /// longer maps back to it.
    ///
    /// # Errors
    ///
    /// Leaves the registry unchanged and returns:
    /// - [`ContractRegistryError::Unresolved`] when the contract has no IB contract id.
    ///   Resolve it through IB's contract details first, or use
    ///   [`register_by_name_only`](Self::register_by_name_only) where nothing maps IB's
    ///   reports back to the name.
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

        inner.remove_reverse_mapping(&name);
        inner.by_name.insert(name.clone(), contract);
        inner.by_con_id.insert(con_id, name);
        Ok(())
    }

    /// Register a contract under its rustrade instrument name for lookups by name only.
    ///
    /// The contract may be unresolved. [`get_name_by_con_id`](Self::get_name_by_con_id)
    /// never finds the name through it, even if the contract carries an id, so anything
    /// IB reports for it by contract id cannot be attributed. Use this only where
    /// nothing maps IB's reports back to the name, such as market data subscriptions.
    /// An execution client needs [`register`](Self::register).
    ///
    /// Registering a name again replaces its contract, and an earlier contract id
    /// registered with [`register`](Self::register) no longer maps back to it.
    pub fn register_by_name_only(&self, name: InstrumentNameExchange, contract: Contract) {
        let mut inner = self.inner.write();
        inner.remove_reverse_mapping(&name);
        inner.by_name.insert(name, contract);
    }

    /// Look up an IB contract by rustrade instrument name.
    pub fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        self.inner.read().by_name.get(name).cloned()
    }

    /// Look up a rustrade instrument name by IB contract ID.
    pub fn get_name_by_con_id(&self, con_id: i32) -> Option<InstrumentNameExchange> {
        self.inner.read().by_con_id.get(&con_id).cloned()
    }

    /// Check if an instrument is registered.
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
    fn a_name_only_contract_is_found_by_name_and_never_by_contract_id() {
        let registry = ContractRegistry::new();
        let unresolved = InstrumentNameExchange::from("MSFT");
        let with_id = InstrumentNameExchange::from("AAPL");

        registry.register_by_name_only(unresolved.clone(), stock_contract("MSFT", "SMART", "USD"));
        registry.register_by_name_only(with_id.clone(), resolved("AAPL", 265598));

        assert!(registry.get_contract(&unresolved).is_some());
        assert!(registry.get_contract(&with_id).is_some());
        assert!(registry.get_name_by_con_id(0).is_none());
        assert!(registry.get_name_by_con_id(265598).is_none());
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn registering_a_name_by_name_only_clears_its_contract_id() {
        let registry = ContractRegistry::new();
        let name = InstrumentNameExchange::from("AAPL");
        registry
            .register(name.clone(), resolved("AAPL", 265598))
            .unwrap();

        registry.register_by_name_only(name.clone(), stock_contract("AAPL", "SMART", "USD"));

        assert!(registry.get_name_by_con_id(265598).is_none());
        assert_eq!(registry.get_contract(&name).unwrap().contract_id, 0);
    }

    #[test]
    fn a_name_only_entry_does_not_clear_another_names_contract_id() {
        let registry = ContractRegistry::new();
        let aapl = InstrumentNameExchange::from("AAPL");
        let quotes = InstrumentNameExchange::from("AAPL-QUOTES");
        registry
            .register(aapl.clone(), resolved("AAPL", 265598))
            .unwrap();
        registry.register_by_name_only(quotes.clone(), resolved("AAPL", 265598));

        // Re-registering the name-only entry must leave AAPL's reverse mapping alone.
        registry.register_by_name_only(quotes, stock_contract("AAPL", "SMART", "USD"));

        assert_eq!(registry.get_name_by_con_id(265598), Some(aapl));
    }
}
