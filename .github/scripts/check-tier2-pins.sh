#!/usr/bin/env bash
#
# Verify that .github/tier2-dependencies.txt agrees with the workspace manifests.
#
# That file's contract is that the hard `=` pin in a manifest is the *enforcement* and the file
# itself is the *record*, and that the two must move together — a bump editing only the manifest
# has skipped the re-verification the record mandates. Nothing enforced that sentence, so it drifted:
# the binance-sdk record read `=52.0.0` while the manifests had already moved to `=55.0.0` and then
# `=60.0.0` via plain Dependabot bumps, and the mandated re-verification went unrecorded for both.
#
# This script checks two things, both of which are "the record disagrees with reality":
#
#   1. Every crate pinned in the record (`crate=x.y.z`) is declared by every manifest that depends
#      on it as exactly `=x.y.z`.
#   2. Every crate named in the record is still a workspace dependency at all, so a removed
#      dependency leaves a stale entry behind rather than silently lingering.
#
# What it deliberately does NOT check: that the re-verification the record mandates was actually
# performed. Nothing in CI can know that. What it does is make the omission *visible* — a bump can
# no longer land without touching this file, which puts it in the diff where a reviewer sees either
# a real write-up or a version string bumped alone.
#
# Requirements are read via `cargo metadata` rather than by grepping TOML, so multi-line dependency
# declarations, commented-out blocks and `workspace = true` inheritance are all handled correctly.
# `--no-deps` resolves nothing and touches the network not at all; it only reads the manifests.

set -euo pipefail

TIER2_FILE="${TIER2_FILE:-.github/tier2-dependencies.txt}"

if [[ ! -f "$TIER2_FILE" ]]; then
    echo "error: tier-2 record not found at '$TIER2_FILE'" >&2
    exit 1
fi

# Record entries. Both forms may carry a trailing ` # comment`, which is stripped first:
#   crate=x.y.z   pinned  — the manifests must agree on the exact version
#   crate         listed  — tier-2 but unpinned; only its continued existence is checked
records="$(sed 's/[[:space:]]*#.*$//' "$TIER2_FILE" | grep -vE '^[[:space:]]*$' || true)"

if [[ -z "$records" ]]; then
    echo "error: '$TIER2_FILE' declares no tier-2 crates — expected at least one" >&2
    exit 1
fi

# Declared requirements, one "<package><TAB><dependency><TAB><req>" row per declaration.
manifest_reqs="$(cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[] | .name as $pkg | .dependencies[]
             | [$pkg, .name, .req] | @tsv')"

failures=0

fail() {
    echo "  ✗ $*" >&2
    failures=$((failures + 1))
}

while IFS= read -r entry; do
    crate="${entry%%=*}"
    # Every manifest declaring this crate, as "<package> <req>" rows.
    declarations="$(awk -F'\t' -v c="$crate" '$2 == c { print $1, $3 }' <<<"$manifest_reqs")"

    if [[ -z "$declarations" ]]; then
        fail "$crate: named in $TIER2_FILE but not declared by any workspace manifest." \
             "Remove the entry if the dependency is gone."
        continue
    fi

    # Unpinned entries have no version to compare — existence is the whole check.
    [[ "$entry" == *=* ]] || continue

    want="=${entry#*=}"
    while read -r pkg req; do
        if [[ "$req" != "$want" ]]; then
            fail "$crate: $TIER2_FILE records '$want' but $pkg declares '$req'." \
                 "The pin and the record must move together."
        fi
    done <<<"$declarations"
done <<<"$records"

if (( failures > 0 )); then
    cat >&2 <<EOF

$failures mismatch(es) between $TIER2_FILE and the workspace manifests.

A tier-2 pin and its record are a single unit: the pin enforces the version, the record carries the
review that version was granted. Bumping one without the other means a required re-verification was
skipped. Update $TIER2_FILE with the re-verification for the new version, in the same commit as the
manifest change.
EOF
    exit 1
fi

echo "$TIER2_FILE agrees with the workspace manifests."
