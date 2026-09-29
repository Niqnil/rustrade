#!/usr/bin/env bash
#
# Publish every publishable workspace crate to crates.io, each after the workspace crates it
# depends on, skipping any whose exact version is already there.
#
# crates.io publishes are irreversible, so a release that fails midway leaves some crates up and
# some not, and the only recovery short of bumping versions is re-running this script. That makes
# the "already published?" check load-bearing, and it reads the one surface that matters: the
# sparse index (https://index.crates.io), which is what dependency resolution and `cargo publish`
# itself read. An earlier guard grepped `cargo search`, which queries the separate search API. That
# API does not update in lockstep with the index, and a substring match on its fuzzy results could
# both fail a re-run on a crate that was up and skip one that never was.
#
# The index, not `GET /api/v1/crates/<name>/<version>`, because the API reads the database, which
# crates.io updates before its background job writes the index. The API can therefore report a
# version that a dependent cannot yet resolve against, whereas the index answers exactly "can the
# next crate be published against this one".
#
# Order: derived from `cargo metadata`, not written down, so adding a crate or a dependency edge
# cannot introduce a race. Versioned dev-dependencies count as edges, because `cargo publish` keeps
# them in the published manifest and crates.io requires every listed dependency to exist. Path-only
# dev-dependencies are stripped on publish, so they do not.
#
# Barrier: `cargo publish` (Cargo >= 1.66) waits for the new version to reach the index, but on
# timeout it only warns and exits 0. So after every publish this script polls the index itself
# until the exact version is listed, and fails if INDEX_TIMEOUT_SECS passes first. Every crate gets
# the same barrier, including siblings with no edge between them, so reordering cannot remove one.
#
# Environment:
#   DRY_RUN=1            print the order and what would be published; publish nothing
#   INDEX_TIMEOUT_SECS   how long to wait for a published version to appear (default 300)
#   INDEX_POLL_SECS      interval between index polls (default 10)

set -euo pipefail

INDEX_URL="https://index.crates.io"
INDEX_TIMEOUT_SECS="${INDEX_TIMEOUT_SECS:-300}"
INDEX_POLL_SECS="${INDEX_POLL_SECS:-10}"
DRY_RUN="${DRY_RUN:-0}"

body="$(mktemp)"
trap 'rm -f "$body"' EXIT

die() {
    # `::error::` surfaces as an annotation on the workflow run; elsewhere it is plain text
    echo "::error::$*" >&2
    exit 1
}

# Path of a crate's file in the sparse index:
# https://doc.rust-lang.org/cargo/reference/registry-index.html#index-files
index_path() {
    local name="${1,,}"
    case ${#name} in
        1) echo "1/$name" ;;
        2) echo "2/$name" ;;
        3) echo "3/${name:0:1}/$name" ;;
        *) echo "${name:0:2}/${name:2:2}/$name" ;;
    esac
}

# Succeeds if the index lists exactly `$2` for crate `$1`, fails if it does not, and exits the
# script if the index cannot be read. A lookup error must never read as "not published" (the
# publish would then fail confusingly) nor as "published" (a crate would be skipped silently).
index_has_version() {
    local name="$1" version="$2" url status listed
    url="$INDEX_URL/$(index_path "$name")"
    status="$(curl -sS --retry 3 --max-time 30 -o "$body" -w '%{http_code}' "$url")" \
        || die "could not reach $url"
    case "$status" in
        200) ;;
        404) return 1 ;; # the crate has never been published
        *) die "$url answered HTTP $status" ;;
    esac
    # One JSON object per published version. A yanked version is still published and cannot be
    # uploaded again, so it counts.
    listed="$(jq -rs --arg v "$version" 'any(.[]; .vers == $v)' "$body")" \
        || die "could not parse the index file at $url"
    [[ "$listed" == true ]]
}

metadata="$(cargo metadata --format-version=1 --no-deps)"

# One line per publishable crate: "<name> <version> <workspace deps...>". `publish` is null when
# unrestricted, [] for `publish = false`, or a list of allowed registries.
declare -A version deps
while read -r name ver dep_list; do
    version[$name]="$ver"
    deps[$name]="$dep_list"
done < <(jq -r '
    [.packages[].name] as $members
    | .packages[]
    | select(.publish == null or (.publish | index("crates-io")))
    | [.name, .version]
      + [.dependencies[]
         | select(.name as $n | $members | index($n))
         | select(.kind != "dev" or .req != "*")
         | .name]
    | join(" ")
' <<<"$metadata")

(( ${#version[@]} > 0 )) || die "cargo metadata listed no publishable crates"

# Topological order, ties broken by name so the order is stable across runs.
order=()
declare -A placed
mapfile -t remaining < <(printf '%s\n' "${!version[@]}" | sort)
while (( ${#remaining[@]} > 0 )); do
    unplaced=()
    for name in "${remaining[@]}"; do
        ready=1
        for dep in ${deps[$name]}; do
            [[ -n "${placed[$dep]:-}" ]] || ready=0
        done
        if (( ready )); then
            order+=("$name")
            placed[$name]=1
        else
            unplaced+=("$name")
        fi
    done
    # Covers a cycle, and a dependency on a workspace crate that is not itself publishable
    (( ${#unplaced[@]} < ${#remaining[@]} )) \
        || die "cannot order ${unplaced[*]}: each depends on a crate that is not placed before it"
    remaining=("${unplaced[@]}")
done

echo "Publish order: ${order[*]}"

for name in "${order[@]}"; do
    ver="${version[$name]}"
    echo "::group::$name@$ver"

    if index_has_version "$name" "$ver"; then
        echo "$name@$ver is already in the index, skipping"
    elif [[ "$DRY_RUN" == 1 ]]; then
        echo "DRY_RUN: would publish $name@$ver"
    else
        cargo publish -p "$name" --no-verify

        deadline=$(( SECONDS + INDEX_TIMEOUT_SECS ))
        until index_has_version "$name" "$ver"; do
            (( SECONDS < deadline )) \
                || die "$name@$ver was published but is not in the index after ${INDEX_TIMEOUT_SECS}s; re-run once it appears"
            echo "waiting for $name@$ver to reach the index"
            sleep "$INDEX_POLL_SECS"
        done
        echo "$name@$ver is in the index"
    fi

    echo "::endgroup::"
done
