# Contributing to rustrade

Thank you for your interest in contributing to rustrade!

## Prerequisites

A stable Rust toolchain (pinned by `rust-toolchain.toml`; MSRV is `1.95`).

**Linux contributors also need the [mold](https://github.com/rui314/mold) linker.**
`.cargo/config.toml` sets `-fuse-ld=mold` unconditionally for
`x86_64-unknown-linux-gnu`, so without it every link fails:

```bash
sudo apt install mold        # Debian/Ubuntu
```

mold is used because this workspace links large binaries against several exchange
SDKs at once, where the default linker is markedly slower. macOS and Windows are
unaffected — the flag is scoped to the Linux GNU target.

That same target also sets `-C split-debuginfo=unpacked`, which writes DWARF to `.dwo`
sidecar files next to the object files instead of into the linked binary. Debug builds
here are dominated by `.debug_str` — monomorphised type names — and the examples that
instantiate the stream machinery across every exchange come within a few percent of the
4 GiB ceiling on a 32-bit relocation, with one crossing it and failing to link. Splitting
the debug info leaves well under one percent of that behind. The visible cost is a
`target/` directory holding thousands of `.dwo` files; `cargo clean` clears them with
everything else.

## Branching Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Stable release branch. |
| `develop` | Integration and testing. All feature PRs target this branch. |
| `feature/*` | Your feature branches. Branch off `develop`. |
| `release/*` | Release-prep branches (version bump + changelog finalize). Branch off `develop`. |

### Workflow

1. **Branch off `develop`:**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/my-feature
   ```

2. **Make your changes** and commit with clear messages.

3. **Open a PR to `develop`** (not `main`).

4. **CI checks run automatically** (fmt, clippy, tests).

5. **After review and merge**, maintainers will periodically promote `develop` → `main` after validation.

## Pre-commit Hooks (Optional)

This repo provides optional git hooks. They're not required—CI enforces the same checks—but they catch issues before push. Set them up once after cloning:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook runs:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::correctness -D clippy::suspicious -D clippy::style -W clippy::complexity -D clippy::perf`

If your commit is blocked, fix the issues and try again:
```bash
cargo fmt --all      # Auto-fix formatting
cargo clippy ...     # Review and fix warnings
```

If your local toolchain is broken, you can bypass the hook: `git commit --no-verify` (CI still enforces quality).

## Code Quality

Before submitting a PR, ensure:

1. **Formatting:** `cargo fmt --all`
2. **Lints:** `cargo clippy --workspace --all-targets --all-features -- -D clippy::correctness -D clippy::suspicious -D clippy::style -W clippy::complexity -D clippy::perf`
   - Note: `complexity` is `-W` (warn) not `-D` (deny) because the codebase intentionally allows `type_complexity` and `too_many_arguments` in some areas.
3. **Tests pass** — the two commands CI runs:
   ```bash
   cargo test --workspace --all-features --lib --bins --tests --examples
   cargo test --workspace --all-features --doc
   ```
   See [Testing](#testing) for why the target list is spelled out rather than left
   to default, and for running a single test file while iterating.

## Testing

**Unit and doc tests** run in CI and require no API keys:
```bash
cargo test --workspace --all-features --lib --bins --tests --examples
cargo test --workspace --all-features --doc
```

`--all-features` is not optional here. Every exchange integration sits behind its own
Cargo feature and the crates declare `default = []`, so without it none of the
integration code compiles at all — the run reports success while that code goes
entirely unbuilt.

### Why the target list is spelled out

Naming the targets keeps doc-tests out of the first command so that the second owns
them outright. A rustdoc example that stops compiling then fails a step of its own,
rather than disappearing into a sweep of everything at once.

`--examples` is there because linking an example catches what type-checking it cannot:
errors that only surface once generic code is monomorphised and the binary is actually
produced. Under `--all-features` these are the largest binaries the workspace makes, so
they are also where the linker limits described under [Prerequisites](#prerequisites)
show up first.

Benches are not in either command. They are type-checked, along with everything else, by:

```bash
cargo check --workspace --all-targets --all-features
```

### Running a single test file

While iterating, narrow to one target instead of running the whole sweep:

```bash
cargo test --lib <filter>
cargo test --test <file>
```

### Integration tests

Integration tests talk to live exchange APIs. They need credentials, are marked
`#[ignore]` so they never run in the default suite, and are gated behind their
provider's feature:

```bash
cp .env.template .env
# Edit .env with your API keys

cargo test -p rustrade-execution --features alpaca --test alpaca_integration -- --ignored
cargo test -p rustrade-data --features alpaca --test alpaca_data -- --ignored
```

Both halves matter. Without `--features <provider>` the test file's contents are gated
out and the binary holds no tests at all; without `-- --ignored` the `#[ignore]` markers
mean none of them run. Either omission produces a green run that asserted nothing.

## Changelog & Versioning

We follow [Keep a Changelog](https://keepachangelog.com/) and [Semantic Versioning](https://semver.org/).

**For contributors:**
- Add notable changes under `## [Unreleased]` in CHANGELOG.md
- Use sections: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`
- Don't bump version numbers — maintainers handle this at release time

**Deprecation policy:**
This is a library crate — avoid breaking downstream users unnecessarily.
- Use `#[deprecated(since = "x.y.z", note = "Use X instead")]` before removing APIs
- Keep deprecated items for at least one minor version
- Document migration paths in CHANGELOG.md under `Deprecated`
- Only remove in the next major version (or minor version pre-1.0)

**Pre-release checklist (maintainers):**
Before cutting a release, verify documentation is current:
- [ ] **CHANGELOG.md** — `[Unreleased]` captures all notable changes since the last release
- [ ] **README.md files** — all are up to date with any API, feature, or behavior changes. Check every crate:
  - `README.md` (workspace root)
  - `rustrade/README.md`
  - `rustrade-data/README.md`
  - `rustrade-execution/README.md`
  - `rustrade-instrument/README.md`
  - `rustrade-integration/README.md`
- [ ] Version numbers are consistent across all `Cargo.toml` files

**Release process (maintainers):**

We use a **two-PR flow** so `develop` and `main` stay in sync — the version bump lands on `develop` first, so there's no post-release back-merge:

1. Complete the pre-release checklist above.
2. Create a release-prep branch off `develop` (e.g. `release/x.y.z`):
   - Bump versions in all `Cargo.toml` files and re-sync `Cargo.lock` (CI runs `--locked`, so the lock must stay consistent).
   - Rename `[Unreleased]` → `[x.y.z] - YYYY-MM-DD` and add a fresh empty `[Unreleased]`.
3. Open the release-prep PR targeting **`develop`**; merge after CI is green.
4. Open the release PR **`develop` → `main`**; merge after CI is green.
5. Tag the **merge commit on `main`** — not `develop`'s tip. Step 4 leaves you on `develop`, so a
   bare `git tag vx.y.z` would tag the wrong commit. Name the commit explicitly:

   ```bash
   git fetch origin
   git log -1 origin/main   # confirm this is the release merge commit
   git tag vx.y.z origin/main
   git push origin vx.y.z
   ```
6. The publish workflow runs automatically on the tag.

   **A failed publish does not need a version bump.** Every publish step is guarded by a
   `cargo search` check for the exact version, so crates already on crates.io are skipped and a
   re-run resumes where it stopped. `publish.yml` has no `workflow_dispatch` trigger, so a retry
   means deleting and re-pushing the same tag:

   ```bash
   git push origin :refs/tags/vx.y.z
   git tag -d vx.y.z
   # fix the cause, then re-tag and re-push as in step 5
   ```

## What NOT to Contribute

This is a generic trading engine library. The following belong in downstream consumers, not here:

- Trading strategy
- Exchange-specific business logic (margin routing, position tracking)
- Greeks computation
- Market hours logic

## Questions?

Open a Discussion.
