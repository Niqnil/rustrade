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
# While polling, an index that cannot be read is retried until the deadline, because the upload has
# already succeeded; before publishing, it stops the script, because neither answer can be assumed.
#
# Cache: the index is served through a CDN with `max-age=600`, which crates.io purges on publish.
# Every lookup adds a throwaway query string, which the index ignores but which misses the CDN's
# cached copy, so neither the check nor the poll can be answered by a stale copy if a purge is late.
#
# Environment:
#   DRY_RUN=1            print the order and what would be published; publish nothing
#   INDEX_TIMEOUT_SECS   how long to wait for a published version to appear (default 300)
#   INDEX_POLL_SECS      interval between index polls (default 10)
#   RELEASE_TAG          the release's tag, `vX.Y.Z`; when set, every crate must be at X.Y.Z. The
#                        workflow passes the pushed tag. Without the check, a tag on a tree whose
#                        versions were not bumped would find every crate already in the index,
#                        publish nothing, and still get a GitHub Release. Before tagging, run
#                        `RELEASE_TAG=vX.Y.Z DRY_RUN=1` locally to catch that early

set -euo pipefail

INDEX_URL="https://index.crates.io"
INDEX_TIMEOUT_SECS="${INDEX_TIMEOUT_SECS:-300}"
INDEX_POLL_SECS="${INDEX_POLL_SECS:-10}"
DRY_RUN="${DRY_RUN:-0}"
RELEASE_TAG="${RELEASE_TAG:-}"

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

# Looks up exactly version `$2` of crate `$1` and sets INDEX_STATE to `listed`, `absent`, or
# `unreadable` with the reason in INDEX_ERROR. Unreadable is kept apart from absent so that a
# lookup error never reads as "not published" (the publish would then fail confusingly) nor as
# "published" (a crate would be skipped silently). Each caller decides what unreadable means.
index_lookup() {
    local name="$1" version="$2" url status listed
    url="$INDEX_URL/$(index_path "$name")"
    INDEX_STATE=unreadable
    if ! status="$(curl -sS --retry 3 --max-time 30 -o "$body" -w '%{http_code}' \
        "$url?nocache=$(date +%s)$RANDOM")"; then
        INDEX_ERROR="could not reach $url"
        return 0
    fi
    case "$status" in
        200) ;;
        404) INDEX_STATE=absent; return 0 ;; # the crate has never been published
        *) INDEX_ERROR="$url answered HTTP $status"; return 0 ;;
    esac
    # One JSON object per published version. A yanked version is still published and cannot be
    # uploaded again, so it counts.
    if ! listed="$(jq -rs --arg v "$version" 'any(.[]; .vers == $v)' "$body")"; then
        INDEX_ERROR="could not parse the index file at $url"
        return 0
    fi
    if [[ "$listed" == true ]]; then INDEX_STATE=listed; else INDEX_STATE=absent; fi
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

if [[ -n "$RELEASE_TAG" ]]; then
    [[ "$RELEASE_TAG" == v* ]] || die "RELEASE_TAG must look like vX.Y.Z, got '$RELEASE_TAG'"
    mismatched=()
    for name in "${!version[@]}"; do
        [[ "${version[$name]}" == "${RELEASE_TAG#v}" ]] || mismatched+=("$name@${version[$name]}")
    done
    (( ${#mismatched[@]} == 0 )) \
        || die "tag $RELEASE_TAG does not match the version of: $(printf '%s\n' "${mismatched[@]}" | sort | xargs)"
    echo "Every crate is at ${RELEASE_TAG#v}, matching tag $RELEASE_TAG"
fi

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

    index_lookup "$name" "$ver"
    [[ "$INDEX_STATE" != unreadable ]] || die "$INDEX_ERROR"

    if [[ "$INDEX_STATE" == listed ]]; then
        echo "$name@$ver is already in the index, skipping"
    elif [[ "$DRY_RUN" == 1 ]]; then
        echo "DRY_RUN: would publish $name@$ver"
    else
        cargo publish -p "$name" --no-verify

        deadline=$(( SECONDS + INDEX_TIMEOUT_SECS ))
        while index_lookup "$name" "$ver"; [[ "$INDEX_STATE" != listed ]]; do
            (( SECONDS < deadline )) \
                || die "$name@$ver was published but is not in the index after ${INDEX_TIMEOUT_SECS}s; re-run once it appears"
            if [[ "$INDEX_STATE" == unreadable ]]; then
                echo "::warning::$INDEX_ERROR; retrying"
            else
                echo "waiting for $name@$ver to reach the index"
            fi
            sleep "$INDEX_POLL_SECS"
        done
        echo "$name@$ver is in the index"
    fi

    echo "::endgroup::"
done
