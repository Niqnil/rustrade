//! The [`SubscriptionId`] a London Strategic Edge tick is filed under, and the mapper that files
//! each subscription under it.

use crate::{
    Identifier,
    exchange::{Connector, subscription::ExchangeSub},
    instrument::InstrumentData,
    subscriber::mapper::SubscriptionMapper,
    subscription::{Map, Subscription, SubscriptionKind, SubscriptionMeta},
};
use fnv::FnvHashMap;
use rustrade_integration::subscription::SubscriptionId;
use serde::{Deserialize, Serialize};

/// The [`SubscriptionId`] the provider's ticks for `symbol` arrive under: the symbol itself.
///
/// The only place an LSE identifier is spelled. The instrument map, the tick decoder, the resume
/// bookkeeping and the shared connection all build it here, so none of them can disagree about
/// which subscription a tick belongs to.
///
/// # Why there is no channel prefix
/// The standard identifier is `channel|market`. This provider has one channel
/// ([`LseChannel::Tick`](super::channel::LseChannel::Tick)), so a prefix would carry no
/// information, and it would cost an allocation on every tick: a
/// [`SmolStr`](smol_str::SmolStr) holds 23 bytes inline, and an option contract's symbol is its
/// root plus fifteen characters. With a six-character root that is 21 bytes on its own, and 26
/// with `tick|`. Bare, every symbol this feed publishes fits inline.
///
/// Dropping the channel does not merge the two subscription kinds: each stream carries its own
/// instrument map for one kind, and a watermark is filed under the kind as well — see
/// [`LseResumeKey`](super::resume::LseResumeKey).
pub(super) fn subscription_id(symbol: &str) -> SubscriptionId {
    SubscriptionId::from(symbol)
}

/// The [`SubscriptionMapper`] for London Strategic Edge: [`WebSocketSubMapper`] with each
/// subscription filed under its bare symbol rather than `channel|market`.
///
/// # ⚠️ `ExchangeSub::id` is not this integration's identifier
/// The blanket `Identifier<SubscriptionId>` impl on [`ExchangeSub`] still spells `tick|<symbol>`
/// for this connector, because it serves every exchange and cannot be overridden for one. No tick
/// arrives under that spelling. Build an identifier through this mapper instead.
///
/// [`WebSocketSubMapper`]: crate::subscriber::mapper::WebSocketSubMapper
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct LseSubMapper;

impl SubscriptionMapper for LseSubMapper {
    fn map<Exchange, Instrument, Kind>(
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> SubscriptionMeta<Instrument::Key>
    where
        Exchange: Connector,
        Instrument: InstrumentData,
        Kind: SubscriptionKind,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let mut instrument_map = Map(FnvHashMap::with_capacity_and_hasher(
            subscriptions.len(),
            Default::default(),
        ));

        let exchange_subs = subscriptions
            .iter()
            .map(|subscription| {
                let exchange_sub =
                    ExchangeSub::<Exchange::Channel, Exchange::Market>::new(subscription);

                instrument_map.0.insert(
                    subscription_id(exchange_sub.market.as_ref()),
                    subscription.instrument.key().clone(),
                );

                exchange_sub
            })
            .collect::<Vec<_>>();

        SubscriptionMeta {
            instrument_map,
            ws_subscriptions: Exchange::requests(exchange_subs),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{exchange::lse::LseOptions, subscription::trade::PublicTrades};
    use rust_decimal_macros::dec;
    use rustrade_instrument::instrument::{
        kind::option::{OptionExercise, OptionKind},
        market_data::{
            MarketDataInstrument,
            kind::{MarketDataInstrumentKind, MarketDataOptionContract},
        },
    };

    /// The longest symbol the feed can publish: an OSI contract on a six-character root.
    const LONGEST_CONTRACT: &str = "GOOGLX261231C01234500";

    #[test]
    fn the_longest_option_contract_is_filed_inline() {
        assert_eq!(LONGEST_CONTRACT.len(), 21);

        let id = subscription_id(LONGEST_CONTRACT);

        assert_eq!(id.as_ref(), LONGEST_CONTRACT);
        assert!(!id.0.is_heap_allocated());
    }

    /// The instrument map is keyed exactly as the tick decoder keys a frame, so a contract on a
    /// root of more than three characters is filed inline and resolves.
    #[test]
    fn the_instrument_map_files_a_contract_under_its_bare_symbol() {
        let subscription: Subscription<LseOptions, MarketDataInstrument, PublicTrades> =
            Subscription::from((
                LseOptions::default(),
                "googl",
                "usd",
                MarketDataInstrumentKind::Option(MarketDataOptionContract {
                    kind: OptionKind::Call,
                    exercise: OptionExercise::American,
                    expiry: "2026-09-30T20:00:00Z".parse().unwrap(),
                    strike: dec!(700),
                }),
                PublicTrades,
            ));

        let SubscriptionMeta { instrument_map, .. } =
            LseSubMapper::map(std::slice::from_ref(&subscription));

        let (id, instrument) = instrument_map.0.iter().next().unwrap();
        assert_eq!(instrument_map.0.len(), 1);
        assert_eq!(id, &subscription_id("GOOGL260930C00700000"));
        assert!(!id.0.is_heap_allocated());
        assert_eq!(instrument, &subscription.instrument);
    }
}
