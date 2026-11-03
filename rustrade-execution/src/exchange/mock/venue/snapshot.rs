//! A [`SimulatedVenue`]'s state as plain data, and the venue it restores.

use super::price_window::PriceWindow;
use super::{NetPosition, SimulatedVenue, VenueRegime, liquidation::LiquidationState};
use crate::{
    balance::MarginDetails,
    client::mock::MockExecutionConfig,
    exchange::mock::account::{AccountState, AccountStateSnapshot, DuplicateSeededOrder},
    slippage::Slippage,
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use itertools::Itertools;
use rustrade_instrument::{
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Everything a [`SimulatedVenue`] has accumulated since it was built, as a value serde can write,
/// from which [`SimulatedVenue::from_snapshot`] builds a venue that carries on exactly as this one
/// would have.
///
/// Taken by [`SimulatedVenue::snapshot`]. When and where to keep one is the caller's choice.
///
/// # What it holds
/// - The account: every balance with its loan and unpaid interest, every open and held order with
///   what is held against it and its place in the queue, the orders that ended, and the trades.
/// - The net position of each cash-settled instrument, with its entry price.
/// - The order, trade and cash-flow id sequences, so no id the venue mints after a restore repeats
///   one minted before it.
/// - The venue's clock: the latest instant it reached and the instant up to which it has charged
///   interest and holding costs.
/// - The debt it last reported, whether a liquidation is under way or a margin call outstanding,
///   the prices over the borrow price window, and how far the slippage stream has drawn.
///
/// # What it does not hold
/// - **The configuration**, and the models set by builder methods (holding costs, margin interest,
///   a fill model, shortability): code, not data. Restore with the configuration and models the
///   venue ran with. A changed fee model or interest model applies from the restore on.
/// - **The market**: what the venue last observed is stale by the time it is restored. A restored
///   venue holds no market until its driver feeds one, so it values and matches nothing before
///   then, as a fresh venue does not.
/// - **Warnings it gave once**, which a restored venue gives again.
///
/// # The time between the snapshot and the restore
/// A restored venue resumes from the instant recorded:
/// - **Interest and holding costs** are charged for every boundary after it, once its clock first
///   moves, as for a host that slept through them.
/// - **A resting order** is matched against the first market observed after the restore, as on
///   any observation. A price that crossed it and turned back while the venue was not running was
///   never observed, so the order did not fill.
///
/// # Versions
/// Carries a format [`version`](Self::version). Fields a later release adds are read with defaults
/// from an older snapshot. A change an older snapshot cannot be read under raises the version, and
/// [`SimulatedVenue::from_snapshot`] refuses a version other than
/// [`VERSION`](Self::VERSION). A snapshot whose shape this release cannot parse fails in the
/// deserialiser instead, before it reaches the venue.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SimulatedVenueSnapshot {
    version: u32,
    exchange: ExchangeId,
    regime: VenueRegime,
    /// Sorted.
    instruments: Vec<InstrumentNameExchange>,
    account: AccountStateSnapshot,
    /// Sorted by instrument.
    positions: Vec<(InstrumentNameExchange, NetPosition)>,
    order_sequence: u64,
    order_id_start: u128,
    trade_sequence: u64,
    cash_flow_sequence: u64,
    time_exchange: DateTime<Utc>,
    accrued_through: Option<DateTime<Utc>>,
    /// Sorted by asset.
    reported_debt: Vec<(AssetNameExchange, MarginDetails)>,
    liquidation_under_way: bool,
    margin_called: bool,
    /// Sorted by instrument.
    borrow_prices: Vec<(InstrumentNameExchange, PriceWindow)>,
    slippage_draws: u64,
}

impl SimulatedVenueSnapshot {
    /// The format version this release writes, and the only one it restores.
    pub const VERSION: u32 = 1;

    /// The format version this snapshot was written in.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The exchange the venue simulated.
    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    /// The venue's price source, which the restored venue keeps.
    pub fn regime(&self) -> VenueRegime {
        self.regime
    }

    /// The latest instant the venue's clock had reached.
    pub fn time_exchange(&self) -> DateTime<Utc> {
        self.time_exchange
    }

    /// The instant up to which the venue had charged every interest and holding-cost boundary, or
    /// `None` if its clock had never moved.
    pub fn accrued_through(&self) -> Option<DateTime<Utc>> {
        self.accrued_through
    }
}

/// Why [`SimulatedVenue::from_snapshot`] refused a [`SimulatedVenueSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum VenueRestoreError {
    /// The snapshot was written in a format version this release does not read.
    #[error("venue snapshot is format version {found}; this release reads version {supported}")]
    UnsupportedVersion { found: u32, supported: u32 },

    /// The snapshot is of another exchange than the configuration.
    #[error("venue snapshot is of {snapshot}, but the configuration simulates {config}")]
    ExchangeMismatch {
        snapshot: ExchangeId,
        config: ExchangeId,
    },

    /// The snapshot's venue traded other instruments than the venue being restored.
    #[error(
        "venue snapshot traded other instruments than the venue being restored: only in the \
         snapshot {only_in_snapshot:?}, only in the venue {only_in_venue:?}"
    )]
    InstrumentsMismatch {
        only_in_snapshot: Vec<InstrumentNameExchange>,
        only_in_venue: Vec<InstrumentNameExchange>,
    },

    /// The snapshot names an instrument it does not list among those it traded.
    #[error("venue snapshot names {0}, which it does not list among its instruments")]
    UnknownInstrument(InstrumentNameExchange),

    /// The snapshot was of a margin account and the configuration lends nothing, or the other way
    /// round.
    #[error(
        "venue snapshot is of a {} account, but the configuration {}",
        if *snapshot_margin { "margin" } else { "cash" },
        if *snapshot_margin { "sets no spot_margin" } else { "sets spot_margin" }
    )]
    MarginMismatch { snapshot_margin: bool },

    /// The snapshot lists one order id twice.
    #[error(transparent)]
    DuplicateOrder(#[from] DuplicateSeededOrder),
}

impl SimulatedVenue {
    /// This venue's state as plain data: see [`SimulatedVenueSnapshot`].
    ///
    /// Taken between inputs, so it reflects every input applied so far and nothing of the next.
    pub fn snapshot(&self) -> SimulatedVenueSnapshot {
        SimulatedVenueSnapshot {
            version: SimulatedVenueSnapshot::VERSION,
            exchange: self.exchange,
            regime: self.regime,
            instruments: self.instruments.keys().cloned().sorted_unstable().collect(),
            account: self.account.snapshot(),
            positions: self
                .positions
                .iter()
                .map(|(instrument, position)| (instrument.clone(), *position))
                .sorted_unstable_by(|(a, _), (b, _)| a.cmp(b))
                .collect(),
            order_sequence: self.order_sequence,
            order_id_start: self.order_id_start,
            trade_sequence: self.trade_sequence,
            cash_flow_sequence: self.cash_flow_sequence,
            time_exchange: self.time_exchange_latest,
            accrued_through: self.accrued_through,
            reported_debt: self
                .reported_debt
                .iter()
                .map(|(asset, debt)| (asset.clone(), *debt))
                .sorted_unstable_by(|(a, _), (b, _)| a.cmp(b))
                .collect(),
            liquidation_under_way: self.liquidation.under_way(),
            margin_called: self.liquidation.margin_called(),
            borrow_prices: self
                .borrow_prices
                .iter()
                .map(|(instrument, window)| (instrument.clone(), window.clone()))
                .sorted_unstable_by(|(a, _), (b, _)| a.cmp(b))
                .collect(),
            slippage_draws: self.slippage.draws(),
        }
    }

    /// The venue `snapshot` was taken of, configured by `config` and trading `instruments`, which
    /// carries on exactly as that venue would have: see [`SimulatedVenueSnapshot`] for what it
    /// restores and what happens to the time in between.
    ///
    /// Takes the regime the snapshot's venue had; a driver checks [`regime`](Self::regime) is the
    /// one it drives. `config.initial_state` is not read: the snapshot's account replaces it. Add
    /// the models the venue ran with through the same builder methods as a fresh venue, such as
    /// [`with_margin_interest`](Self::with_margin_interest), which then charge from the recorded
    /// instant.
    ///
    /// # Errors
    /// - [`VenueRestoreError::UnsupportedVersion`] for a snapshot in another format version.
    /// - [`VenueRestoreError::ExchangeMismatch`] if `config` simulates another exchange.
    /// - [`VenueRestoreError::InstrumentsMismatch`] unless `instruments` are exactly the ones the
    ///   snapshot's venue traded.
    /// - [`VenueRestoreError::UnknownInstrument`] if the snapshot names an instrument it does not
    ///   list.
    /// - [`VenueRestoreError::MarginMismatch`] if the snapshot is of a margin account and `config`
    ///   sets no `spot_margin`, or the other way round.
    /// - [`VenueRestoreError::DuplicateOrder`] if the snapshot lists one order id twice among its
    ///   open orders, or among its cancelled, filled or expired ones.
    pub fn from_snapshot(
        config: &MockExecutionConfig,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        snapshot: SimulatedVenueSnapshot,
    ) -> Result<Self, VenueRestoreError> {
        if snapshot.version != SimulatedVenueSnapshot::VERSION {
            return Err(VenueRestoreError::UnsupportedVersion {
                found: snapshot.version,
                supported: SimulatedVenueSnapshot::VERSION,
            });
        }
        if snapshot.exchange != config.mocked_exchange {
            return Err(VenueRestoreError::ExchangeMismatch {
                snapshot: snapshot.exchange,
                config: config.mocked_exchange,
            });
        }

        let listed = snapshot.instruments.iter().collect::<BTreeSet<_>>();
        let trading = instruments.keys().collect::<BTreeSet<_>>();
        if listed != trading {
            return Err(VenueRestoreError::InstrumentsMismatch {
                only_in_snapshot: listed
                    .difference(&trading)
                    .map(|&name| name.clone())
                    .collect(),
                only_in_venue: trading
                    .difference(&listed)
                    .map(|&name| name.clone())
                    .collect(),
            });
        }
        if let Some(unknown) = snapshot
            .account
            .instruments()
            .chain(snapshot.positions.iter().map(|(instrument, _)| instrument))
            .chain(
                snapshot
                    .borrow_prices
                    .iter()
                    .map(|(instrument, _)| instrument),
            )
            .find(|instrument| !instruments.contains_key(*instrument))
        {
            return Err(VenueRestoreError::UnknownInstrument(unknown.clone()));
        }
        if snapshot.account.margin() != config.spot_margin.is_some() {
            return Err(VenueRestoreError::MarginMismatch {
                snapshot_margin: snapshot.account.margin(),
            });
        }

        let SimulatedVenueSnapshot {
            version: _,
            exchange: _,
            regime,
            instruments: _,
            account,
            positions,
            order_sequence,
            order_id_start,
            trade_sequence,
            cash_flow_sequence,
            time_exchange,
            accrued_through,
            reported_debt,
            liquidation_under_way,
            margin_called,
            borrow_prices,
            slippage_draws,
        } = snapshot;

        let mut venue = Self::assemble(
            config,
            instruments,
            regime,
            AccountState::from_snapshot(account)?,
            positions.into_iter().collect(),
            order_id_start,
            reported_debt.into_iter().collect(),
        );
        venue.order_sequence = order_sequence;
        venue.trade_sequence = trade_sequence;
        venue.cash_flow_sequence = cash_flow_sequence;
        venue.time_exchange_latest = time_exchange;
        venue.accrued_through = accrued_through;
        venue.liquidation = LiquidationState::restored(liquidation_under_way, margin_called);
        // A window the configuration no longer sets would never be read or trimmed.
        if config
            .spot_margin
            .as_ref()
            .is_some_and(|margin| margin.borrow_price_window().is_some())
        {
            venue.borrow_prices = borrow_prices.into_iter().collect();
        }
        venue.slippage = Slippage::resumed(config.slippage, slippage_draws);
        venue.refresh_next_charge();
        venue.refresh_next_interest();
        Ok(venue)
    }
}
