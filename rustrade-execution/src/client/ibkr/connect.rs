//! What [`IbkrClient::connect_sync`](super::IbkrClient::connect_sync) and
//! [`IbkrClient::connect_sync_lenient`](super::IbkrClient::connect_sync_lenient) report about
//! the connection, and about the contracts listed in
//! [`IbkrConfig::contracts`](super::IbkrConfig::contracts).

use super::{
    ContractConfig, IbkrClient,
    contract::{ContractConfigError, ResolveContractError},
};
use crate::error::{ConnectivityError, UnindexedClientError};
use itertools::Itertools;
use rustrade_instrument::{
    ibkr::{ContractRegistry, ContractRegistryError},
    instrument::name::InstrumentNameExchange,
};
use thiserror::Error;
use tracing::{debug, warn};

/// Why [`IbkrClient::connect_sync`](super::IbkrClient::connect_sync) or
/// [`IbkrClient::connect_sync_lenient`](super::IbkrClient::connect_sync_lenient) failed.
#[derive(Debug, Clone, PartialEq, Error)]
#[non_exhaustive]
pub enum IbkrConnectError {
    /// TWS/Gateway could not be reached, or refused the connection for a reason other than
    /// [`ClientIdInUse`](Self::ClientIdInUse).
    #[error(transparent)]
    Connect(#[from] UnindexedClientError),

    /// TWS/Gateway refused the connection because another connection holds `client_id` (IB
    /// code 326).
    ///
    /// TWS/Gateway can go on holding a client ID for a moment after the connection that held it
    /// has closed, so connecting again after a backoff can succeed once it lets the ID go. A
    /// refusal that persists means another connection, in this process or another, is using the
    /// same client ID: give each IB connection its own. The library does not retry.
    #[error("IB refused the connection: client ID {client_id} is already in use")]
    ClientIdInUse {
        /// The refused client ID, [`IbkrConfig::client_id`](super::IbkrConfig::client_id).
        client_id: i32,
    },

    /// The connection succeeded, but at least one contract in
    /// [`IbkrConfig::contracts`](super::IbkrConfig::contracts) could not be registered. Every
    /// such contract is listed, not just the first. The connection was dropped, so its client
    /// ID is free again.
    #[error(
        "{} configured contract(s) not registered: {}",
        .0.len(),
        .0.iter().format("; ")
    )]
    Contracts(Vec<SkippedContract>),
}

/// The connect error for an `ibapi` error that failed the connection:
/// [`IbkrConnectError::ClientIdInUse`] for TWS/Gateway's client ID refusal, else
/// [`IbkrConnectError::Connect`] with [`ConnectivityError::Socket`], `"IB connect: {error}"`.
pub(super) fn connect_error(error: ibapi::Error) -> IbkrConnectError {
    match error {
        ibapi::Error::ClientIdInUse { client_id } => IbkrConnectError::ClientIdInUse { client_id },
        other => UnindexedClientError::Connectivity(ConnectivityError::Socket(format!(
            "IB connect: {other}"
        )))
        .into(),
    }
}

/// The result of [`IbkrClient::connect_sync_lenient`](super::IbkrClient::connect_sync_lenient):
/// the connected client, and the configured contracts it connected without.
#[derive(Debug)]
#[must_use = "a configured contract in `skipped` is not registered, and its orders are refused"]
#[non_exhaustive]
pub struct ConnectOutcome {
    /// The connected client. Every configured contract not in `skipped` is registered.
    pub client: IbkrClient,
    /// Each configured contract that could not be registered, in config order. Empty when all
    /// of them were.
    pub skipped: Vec<SkippedContract>,
}

/// A contract in [`IbkrConfig::contracts`](super::IbkrConfig::contracts) that could not be
/// registered when the client connected.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SkippedContract {
    /// The instrument the contract was to be registered under, from
    /// [`ContractConfig::name`](super::ContractConfig::name).
    pub name: InstrumentNameExchange,
    /// Why it could not be registered.
    pub reason: ContractSkipReason,
}

impl SkippedContract {
    /// A skipped contract, for code that handles [`ConnectOutcome::skipped`] or
    /// [`IbkrConnectError::Contracts`] to be tested without IB.
    pub fn new(name: InstrumentNameExchange, reason: ContractSkipReason) -> Self {
        Self { name, reason }
    }
}

impl std::fmt::Display for SkippedContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.reason)
    }
}

/// Why a configured contract could not be registered. Each variant carries the error of the
/// step that failed: building, resolving or registering it.
#[derive(Debug, Clone, PartialEq, Error)]
#[non_exhaustive]
pub enum ContractSkipReason {
    /// The config does not describe a contract: it is incomplete or names an unsupported
    /// security type. Fix the config.
    #[error("invalid contract config: {0}")]
    Config(ContractConfigError),

    /// IB could not resolve the contract to exactly one of its own, as
    /// [`IbkrClient::resolve_contract`](super::IbkrClient::resolve_contract) reports.
    #[error("contract did not resolve: {0}")]
    Resolve(ResolveContractError),

    /// The resolved contract could not be registered, as
    /// [`IbkrClient::register_contract`](super::IbkrClient::register_contract) reports, such as
    /// for a contract already registered under another configured name.
    #[error("contract did not register: {0}")]
    Register(ContractRegistryError),
}

impl ContractSkipReason {
    /// Whether the same contract may register if tried again: only a
    /// [`Resolve`](Self::Resolve) failure that
    /// [`ResolveContractError::is_transient`] calls transient, such as a dropped connection.
    ///
    /// The library does not retry. To retry one contract on a connected client, call
    /// [`resolve_contract`](super::IbkrClient::resolve_contract) and then
    /// [`register_contract`](super::IbkrClient::register_contract).
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Resolve(error) => error.is_transient(),
            Self::Config(_) | Self::Register(_) => false,
        }
    }
}

/// Build, resolve and register each configured contract into `registry`, in config order, and
/// return the ones that failed. `resolve` asks IB for the one contract a description matches.
///
/// Every failure is logged at `warn!` as it happens and collected; one failure does not stop the
/// rest from being tried.
pub(super) fn register_configured(
    configs: &[ContractConfig],
    registry: &ContractRegistry,
    mut resolve: impl FnMut(
        &ibapi::contracts::Contract,
    ) -> Result<ibapi::contracts::Contract, ResolveContractError>,
) -> Vec<SkippedContract> {
    configs
        .iter()
        .filter_map(|config| {
            let name = InstrumentNameExchange::from(config.name.as_str());
            let registered = config
                .to_contract()
                .map_err(ContractSkipReason::Config)
                .and_then(|contract| resolve(&contract).map_err(ContractSkipReason::Resolve))
                .and_then(|resolved| {
                    let con_id = resolved.contract_id;
                    registry
                        .register(name.clone(), resolved)
                        .map(|()| con_id)
                        .map_err(ContractSkipReason::Register)
                });
            match registered {
                Ok(con_id) => {
                    debug!(name = %name, con_id, "Registered contract");
                    None
                }
                Err(reason) => {
                    warn!(name = %name, error = %reason, "Configured contract not registered");
                    Some(SkippedContract { name, reason })
                }
            }
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use ibapi::contracts::Contract;

    /// TWS/Gateway's client ID refusal is its own error, so a caller can tell it from an
    /// unreachable host without reading the text.
    #[test]
    fn a_client_id_refusal_is_its_own_error() {
        let refused = connect_error(ibapi::Error::ClientIdInUse { client_id: 7 });
        assert_eq!(refused, IbkrConnectError::ClientIdInUse { client_id: 7 });
        assert_eq!(
            refused.to_string(),
            "IB refused the connection: client ID 7 is already in use"
        );
    }

    /// Any other connect failure stays a socket error, naming the connect and `ibapi`'s error.
    #[test]
    fn another_connect_failure_is_a_socket_error() {
        let failed = connect_error(ibapi::Error::ConnectionFailed);
        let IbkrConnectError::Connect(UnindexedClientError::Connectivity(
            ConnectivityError::Socket(message),
        )) = &failed
        else {
            panic!("expected a socket error, got {failed:?}");
        };
        assert_eq!(
            message,
            &format!("IB connect: {}", ibapi::Error::ConnectionFailed)
        );
    }

    fn config(name: &str, security_type: &str) -> ContractConfig {
        ContractConfig {
            name: name.to_string(),
            symbol: name.to_string(),
            security_type: security_type.to_string(),
            exchange: "SMART".to_string(),
            currency: "USD".to_string(),
            last_trade_date: None,
            strike: None,
            right: None,
        }
    }

    /// Resolves a description to a copy carrying `contract_id`, looked up by symbol.
    fn resolve_by_symbol<'a>(
        ids: &'a [(&'a str, Result<i32, ResolveContractError>)],
    ) -> impl FnMut(&Contract) -> Result<Contract, ResolveContractError> + 'a {
        move |contract| {
            let (_, outcome) = ids
                .iter()
                .find(|(symbol, _)| *symbol == contract.symbol.as_str())
                .expect("every resolved symbol has an outcome");
            outcome.clone().map(|contract_id| Contract {
                contract_id,
                ..contract.clone()
            })
        }
    }

    #[test]
    fn register_configured_registers_every_contract_that_resolves() {
        let registry = ContractRegistry::new();
        let ids = [("AAPL", Ok(265_598)), ("MSFT", Ok(272_093))];

        let skipped = register_configured(
            &[config("AAPL", "STK"), config("MSFT", "STK")],
            &registry,
            resolve_by_symbol(&ids),
        );

        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn register_configured_collects_every_failure_in_config_order() {
        let registry = ContractRegistry::new();
        let ids = [
            ("AAPL", Ok(265_598)),
            ("GONE", Err(ResolveContractError::NoMatch)),
            // Resolves to AAPL's contract id, which is already registered under "AAPL".
            ("DUP", Ok(265_598)),
            ("MSFT", Ok(272_093)),
        ];
        let mut missing_date = config("ES", "FUT");
        missing_date.last_trade_date = None;

        let skipped = register_configured(
            &[
                config("AAPL", "STK"),
                missing_date,
                config("GONE", "STK"),
                config("DUP", "STK"),
                config("MSFT", "STK"),
            ],
            &registry,
            resolve_by_symbol(&ids),
        );

        let names: Vec<_> = skipped
            .iter()
            .map(|skip| skip.name.name().as_str())
            .collect();
        assert_eq!(names, ["ES", "GONE", "DUP"]);
        assert!(matches!(
            skipped[0].reason,
            ContractSkipReason::Config(ContractConfigError::MissingLastTradeDate)
        ));
        assert_eq!(
            skipped[1].reason,
            ContractSkipReason::Resolve(ResolveContractError::NoMatch)
        );
        assert!(matches!(
            skipped[2].reason,
            ContractSkipReason::Register(ContractRegistryError::ContractIdTaken { .. })
        ));
        // The failures did not stop the contracts after them from registering.
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn register_configured_does_not_resolve_an_invalid_config() {
        let registry = ContractRegistry::new();
        let mut resolved = 0;

        let skipped = register_configured(&[config("X", "BOND")], &registry, |_| {
            resolved += 1;
            Err(ResolveContractError::NoMatch)
        });

        assert_eq!(resolved, 0);
        assert!(matches!(
            skipped[0].reason,
            ContractSkipReason::Config(ContractConfigError::UnrecognizedSecurityType { .. })
        ));
    }

    #[test]
    fn only_a_transient_resolve_failure_is_transient() {
        let transient = ContractSkipReason::Resolve(ResolveContractError::Connectivity(
            ConnectivityError::Timeout,
        ));
        let no_match = ContractSkipReason::Resolve(ResolveContractError::NoMatch);
        let config = ContractSkipReason::Config(ContractConfigError::MissingStrike);
        let register = ContractSkipReason::Register(ContractRegistryError::Unresolved {
            name: InstrumentNameExchange::new("X"),
        });

        assert!(transient.is_transient());
        assert!(!no_match.is_transient());
        assert!(!config.is_transient());
        assert!(!register.is_transient());
    }

    #[test]
    fn contracts_error_names_every_skipped_contract() {
        let error = IbkrConnectError::Contracts(vec![
            SkippedContract {
                name: InstrumentNameExchange::new("ES"),
                reason: ContractSkipReason::Config(ContractConfigError::MissingLastTradeDate),
            },
            SkippedContract {
                name: InstrumentNameExchange::new("GONE"),
                reason: ContractSkipReason::Resolve(ResolveContractError::NoMatch),
            },
        ]);

        let message = error.to_string();
        assert!(message.starts_with("2 configured contract(s)"), "{message}");
        assert!(message.contains("ES: invalid contract config"), "{message}");
        assert!(
            message.contains("GONE: contract did not resolve"),
            "{message}"
        );
    }
}
