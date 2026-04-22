//! Bidirectional registry mapping barter instrument names to IB contracts.

use crate::instrument::name::InstrumentNameExchange;
use fnv::FnvHashMap;
use ibapi::contracts::Contract;
use parking_lot::RwLock;
use std::sync::Arc;

/// Bidirectional registry mapping barter instrument names to IB contracts.
///
/// IB's `Contract` is a composite key (symbol, secType, exchange, currency, etc.).
/// Barter uses a single string `InstrumentNameExchange`. This registry maintains
/// the mapping in both directions.
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

impl ContractRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ContractRegistryInner::default())),
        }
    }

    /// Register a contract with its barter instrument name.
    ///
    /// If the name was previously registered with a different contract_id,
    /// the old reverse mapping is removed to prevent stale lookups.
    pub fn register(&self, name: InstrumentNameExchange, contract: Contract) {
        let mut inner = self.inner.write();
        let new_con_id = contract.contract_id;

        // Clear stale reverse mapping if re-registering with different contract
        if let Some(old_contract) = inner.by_name.get(&name) {
            let old_con_id = old_contract.contract_id;
            if old_con_id != new_con_id && old_con_id != 0 {
                inner.by_con_id.remove(&old_con_id);
            }
        }

        inner.by_name.insert(name.clone(), contract);
        if new_con_id != 0 {
            inner.by_con_id.insert(new_con_id, name);
        }
    }

    /// Look up an IB contract by barter instrument name.
    pub fn get_contract(&self, name: &InstrumentNameExchange) -> Option<Contract> {
        self.inner.read().by_name.get(name).cloned()
    }

    /// Look up a barter instrument name by IB contract ID.
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

        registry.register(name.clone(), contract.clone());

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
        registry.register(name.clone(), contract1);

        assert_eq!(registry.get_name_by_con_id(111111), Some(name.clone()));

        // Re-register with different contract_id (e.g., after contract roll)
        let mut contract2 = stock_contract("AAPL", "SMART", "USD");
        contract2.contract_id = 222222;
        registry.register(name.clone(), contract2);

        // New mapping works
        assert_eq!(registry.get_name_by_con_id(222222), Some(name.clone()));
        // Old mapping is cleared
        assert!(registry.get_name_by_con_id(111111).is_none());
        // Still only one entry
        assert_eq!(registry.len(), 1);
    }
}
