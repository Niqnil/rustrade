# Contributing to rustrade

Thank you for your interest in contributing to rustrade!

## Branching Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Stable release branch. |
| `develop` | Integration and testing. All feature PRs target this branch. |
| `feature/*` | Your feature branches. Branch off `develop`. |

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
3. **Tests pass:** `cargo test --workspace --all-features` (or specific test files)

## Testing

**Unit tests** run in CI and require no API keys:
```bash
cargo test --workspace --lib
```

**Integration tests** require exchange credentials and run locally only:
```bash
cp .env.template .env
# Edit .env with your API keys
cargo test --workspace --all-features
```

Integration tests are marked with `#[ignore]` by default to avoid running in CI.

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

**Release process (maintainers):**
1. Create release prep PR: bump versions in all Cargo.toml files
2. Rename `[Unreleased]` → `[x.y.z] - YYYY-MM-DD`, add new empty `[Unreleased]`
3. Merge to main
4. Tag: `git tag v0.1.0 && git push origin v0.1.0`
5. Publish workflow runs automatically

## What NOT to Contribute

This is a generic trading engine library. The following belong in downstream consumers, not here:

- Trading strategy
- Exchange-specific business logic (margin routing, position tracking)
- Greeks computation
- Market hours logic

## Questions?

Open a Discussion.
