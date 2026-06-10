# PR Description
The Binance margin config landed a named convenience constructor
(BinanceMargin::cross_margin(api_key, secret_key)) plus the rule: new(...) takes only the
non-defaultable required args (credentials), and optional knobs are reached via named constructors
or #[serde(default)] — mirroring HyperliquidConfig::from_env / from_private_key.

For cross-exchange consistency, extend this pattern to the other credential-bearing execution
configs:

BinanceSpotConfig — currently a 3-arg new(...). Add named convenience constructor(s)
(e.g. production() / from_env()) and let optional knobs default.
AlpacaConfig — currently a 3-arg new(...). Same treatment.
This is opportunistic polish (both work today), not a forced migration. Apply the rule: required
args on new(...) = credentials + non-defaultable only; optional knobs via named ctors /
#[serde(default)].

IBKR is exempt — it has no credentials, so its public-field struct literal is the correct shape.

Reference: the named-constructor analysis that drove the margin config (mirrors
HyperliquidConfig's from_env / from_private_key).


Assuming the goal is full uniformity across credential-bearing execution configs, except IBKR, I’d frame the change as:

**Target Rule**
For credential-bearing execution configs:

```text
new(...) = credentials + only truly non-defaultable args
optional knobs = named constructors, builder/with methods, env parsing, or #[serde(default)]
```

IBKR is exempt because it has no API credentials; its config is connection parameters and public-field struct construction is appropriate.

**Binance Spot**
Current:

```rust
BinanceSpotConfig::new(api_key, secret_key, testnet)
```

Change toward:

```rust
BinanceSpotConfig::new(api_key, secret_key)          // default production
BinanceSpotConfig::production(api_key, secret_key)   // explicit production alias
BinanceSpotConfig::testnet(api_key, secret_key)      // explicit testnet
BinanceSpotConfig::from_env()?                       // optional, if desired
```

Config field:

```rust
#[serde(default)]
pub testnet: bool
```

Tests to add/update:

- `new_uses_production_defaults`
- `production_uses_production_defaults`
- `testnet_uses_testnet_endpoint_flag`
- deserialization omits `testnet` and defaults to `false`
- deserialization accepts `testnet: true`

**Binance Margin**
Current named constructor is already conceptually right:

```rust
BinanceMarginConfig::cross_margin(api_key, secret_key)
```

But `new(...)` still currently takes all knobs:

```rust
new(api_key, secret_key, testnet, is_isolated, side_effect)
```

For strict uniformity, change toward:

```rust
BinanceMarginConfig::new(api_key, secret_key)          // default cross margin
BinanceMarginConfig::cross_margin(api_key, secret_key) // named common case
```

Then expose optional variants explicitly, for example one of:

```rust
BinanceMarginConfig::with_side_effect(api_key, secret_key, side_effect)
BinanceMarginConfig::isolated_margin(api_key, secret_key)
BinanceMarginConfig::with_options(...)
```

or fluent methods:

```rust
BinanceMarginConfig::new(api_key, secret_key)
    .with_side_effect(MarginSideEffect::NoBorrow)
```

Fields already have the right serde idea for margin knobs:

```rust
#[serde(default)]
pub is_isolated: bool,

#[serde(default)]
pub side_effect: MarginSideEffect,
```

Consider whether `testnet` should also get `#[serde(default)]`, even though margin testnet is inert.

Tests to add/update:

- `new_uses_cross_margin_defaults`
- `cross_margin_uses_common_case_defaults`
- serde omits `is_isolated` and `side_effect`
- serde accepts explicit `side_effect`
- if `testnet` remains present, serde omits it and defaults to `false`
- any old tests calling 5-arg `new(...)` migrate to named/fluent constructor

**Alpaca**
Current:

```rust
AlpacaConfig::new(api_key, secret_key, paper)
```

Change toward:

```rust
AlpacaConfig::new(api_key, secret_key)
AlpacaConfig::paper(api_key, secret_key)
AlpacaConfig::production(api_key, secret_key)
AlpacaConfig::from_env()?
```

Important decision: default behavior.

Option A, consistency with serde bool default:

```rust
new(...) => paper: false // production
#[serde(default)]
pub paper: bool
```

Option B, safety-first trading API:

```rust
new(...) => paper: true
```

But if you use `#[serde(default)]` on a bool, omitted `paper` deserializes to `false`, so Option B needs a custom default function:

```rust
#[serde(default = "default_paper")]
pub paper: bool
```

Tests to add/update:

- `new_uses_default_paper_mode` or `new_uses_production_defaults`, depending on chosen semantics
- `paper_sets_paper_true`
- `production_sets_paper_false`
- serde omits `paper` and gets the intended default
- serde accepts `paper: true`
- serde accepts `paper: false`
- `Debug` still redacts credentials

**Hyperliquid**
Current:

```rust
HyperliquidConfig::new(wallet, testnet)
HyperliquidConfig::from_env()
HyperliquidConfig::from_private_key(private_key, testnet)
```

For strict uniformity, `testnet` should move out of the basic constructor:

```rust
HyperliquidConfig::new(wallet)                  // default mainnet
HyperliquidConfig::mainnet(wallet)
HyperliquidConfig::testnet(wallet)
```

For private keys:

```rust
HyperliquidConfig::from_private_key(private_key)          // default mainnet
HyperliquidConfig::from_private_key_mainnet(private_key)
HyperliquidConfig::from_private_key_testnet(private_key)
```

Or keep one explicit parser if you consider network selection non-defaultable for that path:

```rust
HyperliquidConfig::from_private_key(private_key, testnet)
```

But that is less uniform.

`from_env()` is already in the desired spirit: required credential from `HYPERLIQUID_PRIVATE_KEY`, optional `HYPERLIQUID_TESTNET` defaults.

Tests to add/update:

- `new_uses_mainnet_default`
- `mainnet_sets_testnet_false`
- `testnet_sets_testnet_true`
- `from_private_key_defaults_mainnet`
- `from_private_key_testnet_sets_testnet_true`
- existing prefixed/unprefixed private key parsing tests still pass
- `from_env` missing key errors
- `from_env` omitted `HYPERLIQUID_TESTNET` defaults to `false`
- config-file `HyperliquidConfigFile` omitted `testnet` defaults to `false`

**Call Site Updates**
Expect compile errors wherever old constructors are used:

```rust
BinanceSpotConfig::new(k, s, false)
AlpacaConfig::new(k, s, true)
BinanceMarginConfig::new(k, s, false, false, side_effect)
HyperliquidConfig::new(wallet, false)
HyperliquidConfig::from_private_key(key, true)
```

Migrate those to named constructors. That is also the best way to discover all impacted tests/examples.

**Recommended Implementation Order**
1. Decide defaults, especially Alpaca.
2. Update config constructors and serde defaults.
3. Run `cargo check` to find old call sites.
4. Update tests/examples to named constructors.
5. Add focused constructor/default/serde tests.
6. Run the relevant package tests, likely starting with `rustrade-execution`.
