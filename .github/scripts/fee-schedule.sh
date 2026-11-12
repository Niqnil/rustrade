#!/usr/bin/env bash
#
# Detect a change to a venue's published fee schedule (a PDF), for the weekly fee schedule workflow.
#
# The simulated venue's fee models and their rustdoc examples follow a venue's fee schedule as of a
# date. Venues revise those schedules without notice, so this compares the live document with a
# recorded baseline and reports the difference.
#
# The baseline is a small JSON file holding the document's URL, its "Revised on" date and a SHA-256
# of its words. The schedule's text itself is never committed: it is the venue's document, not
# ours. The workflow keeps each run's extracted text as a short-lived artifact instead, so a change
# can be shown as a diff against the text the previous run saw.
#
# Usage:
#   fee-schedule.sh check <baseline.json> <outdir>
#       Download the document, extract its text to <outdir>/fee-schedule.txt and compare it with the
#       baseline. Writes <outdir>/result.env (revised, words_sha256, changed=true|false) and appends
#       the same lines to $GITHUB_OUTPUT when set. Exits non-zero when the document cannot be
#       fetched or read, or carries no revision date: a check that silently compares nothing would
#       report "unchanged" forever.
#   fee-schedule.sh report <baseline.json> <outdir> [<previous text> <previous run description>]
#       After a check that found a change, write the issue title to <outdir>/title.txt and its body
#       to <outdir>/issue.md, with a diff against <previous text> when one is given.
#   fee-schedule.sh words-sha256 <text file>
#       Print the checksum the baseline records for an extracted text.
#
# The checksum covers the words in order, not the layout: the text is extracted with `pdftotext
# -layout`, whose spacing can differ between poppler versions while the words do not.

set -euo pipefail

die() {
    echo "::error::$*" >&2
    exit 1
}

# The SHA-256 of a text's words in order, separated by single newlines.
words_sha256() {
    tr -s '[:space:]' '\n' <"$1" | sed '/^$/d' | sha256sum | cut -d' ' -f1
}

# Extract a PDF's text, keeping its layout for a readable diff but dropping what varies without the
# document changing: zero-width characters, non-breaking spaces, form feeds, leading and trailing
# blanks, runs of spaces (squeezed to two, which keeps table columns apart) and blank lines.
extract_text() {
    local pdf=$1 out=$2
    pdftotext -layout -enc UTF-8 "$pdf" - |
        sed -E \
            -e 's/\xE2\x80\x8B|\xEF\xBB\xBF//g' \
            -e 's/\xC2\xA0/ /g' \
            -e 's/\f//g' \
            -e 's/^[[:space:]]+//; s/[[:space:]]+$//' \
            -e 's/[[:space:]]{2,}/  /g' |
        sed '/^$/d' >"$out"
}

baseline_field() {
    local value
    value=$(jq -er --arg f "$2" '.[$f] | strings' "$1") || die "$1 has no string field \"$2\""
    printf '%s' "$value"
}

cmd_check() {
    local baseline=$1 outdir=$2
    local url name base_revised base_sha
    url=$(baseline_field "$baseline" url)
    name=$(baseline_field "$baseline" name)
    base_revised=$(baseline_field "$baseline" revised)
    base_sha=$(baseline_field "$baseline" words_sha256)

    mkdir -p "$outdir"
    local pdf="$outdir/fee-schedule.pdf" text="$outdir/fee-schedule.txt"
    curl --fail --silent --show-error --location --retry 3 --max-time 60 -o "$pdf" "$url" ||
        die "could not download $name from $url"
    [ "$(head -c 5 "$pdf")" = "%PDF-" ] || die "$url did not return a PDF"

    extract_text "$pdf" "$text"
    [ -s "$text" ] || die "no text could be extracted from $url"

    local revised
    revised=$(grep -m 1 -oE 'Revised on [A-Z][a-z]+ [0-9]{1,2}, [0-9]{4}' "$text" |
        sed 's/^Revised on //') ||
        die "$name carries no \"Revised on <Month> <day>, <year>\" line: its format changed," \
            "so update this script and the baseline by hand"

    local sha changed=false
    sha=$(words_sha256 "$text")
    if [ "$sha" != "$base_sha" ] || [ "$revised" != "$base_revised" ]; then
        changed=true
    fi

    {
        echo "revised=$revised"
        echo "words_sha256=$sha"
        echo "changed=$changed"
    } | tee "$outdir/result.env"
    if [ -n "${GITHUB_OUTPUT:-}" ]; then
        cat "$outdir/result.env" >>"$GITHUB_OUTPUT"
    fi

    if [ "$changed" = true ]; then
        echo "$name changed: revised $base_revised -> $revised, words ${base_sha:0:12} -> ${sha:0:12}"
    else
        echo "$name unchanged (revised $revised)"
    fi
}

# GitHub refuses an issue body over 65536 characters. The diff gets most of that and is cut at a
# line boundary, with a note saying so.
DIFF_LIMIT=50000

cmd_report() {
    local baseline=$1 outdir=$2 prev=${3:-} prev_desc=${4:-}
    local url name base_revised base_sha
    url=$(baseline_field "$baseline" url)
    name=$(baseline_field "$baseline" name)
    base_revised=$(baseline_field "$baseline" revised)
    base_sha=$(baseline_field "$baseline" words_sha256)

    [ -f "$outdir/result.env" ] || die "run check before report"
    local revised words_sha256
    revised=$(sed -n 's/^revised=//p' "$outdir/result.env")
    words_sha256=$(sed -n 's/^words_sha256=//p' "$outdir/result.env")

    local run_url=""
    if [ -n "${GITHUB_RUN_ID:-}" ]; then
        run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
    fi

    # The title carries the new checksum, so the workflow opens one issue per version of the
    # document however many weekly runs see it before the baseline is updated.
    printf '%s changed: revised %s (words %s)\n' "$name" "$revised" "${words_sha256:0:12}" \
        >"$outdir/title.txt"

    {
        echo "The weekly fee schedule check found that the $name differs from the recorded baseline."
        echo
        echo "- Document: $url"
        echo "- Revised: $base_revised → $revised"
        echo "- Checksum of its words (SHA-256): \`${base_sha:0:12}\` → \`${words_sha256:0:12}\`"
        if [ -n "$run_url" ]; then
            echo "- Run: $run_url"
        fi
        echo
        echo "### What changed"
        echo
        if [ -n "$prev" ] && [ -f "$prev" ]; then
            local against
            if [ "$(words_sha256 "$prev")" = "$base_sha" ]; then
                against="the baseline"
            else
                against="an earlier change not yet in the baseline"
            fi
            echo "Diff of the extracted text against $prev_desc, which matched $against:"
            echo
            local diff_file="$outdir/fee-schedule.diff"
            diff -u --label previous --label current "$prev" "$outdir/fee-schedule.txt" \
                >"$diff_file" || true
            if [ ! -s "$diff_file" ]; then
                echo "_No difference in the extracted text: only the checksum or revision differs._"
            else
                # A tilde fence, which a line of backticks in the text cannot close.
                echo '~~~~diff'
                if [ "$(wc -c <"$diff_file")" -gt "$DIFF_LIMIT" ]; then
                    head -c "$DIFF_LIMIT" "$diff_file" | sed '$d'
                    echo '~~~~'
                    echo
                    echo "_The diff is cut short here. Read the document for the rest._"
                else
                    cat "$diff_file"
                    echo '~~~~'
                fi
            fi
        else
            echo "_No earlier extracted text was available (each run keeps it for 90 days), so" \
                "read the document itself._"
        fi
        echo
        echo "### To do"
        echo
        echo "- Check every fee model example and rate that follows this schedule, such as the" \
            "Alpaca example in \`CompositeFeeModel\`'s rustdoc (\`rustrade-execution/src/fee.rs\`)."
        echo "- Set \`revised\` to \`$revised\` and \`words_sha256\` to \`$words_sha256\` in the" \
            "baseline, \`$baseline\`, then close this issue. The scheduled check keeps using the" \
            "default branch's baseline until the update reaches it, and opens no second issue for" \
            "this version meanwhile."
    } >"$outdir/issue.md"
}

case "${1:-}" in
check)
    [ $# -eq 3 ] || die "usage: $0 check <baseline.json> <outdir>"
    cmd_check "$2" "$3"
    ;;
report)
    [ $# -eq 3 ] || [ $# -eq 5 ] ||
        die "usage: $0 report <baseline.json> <outdir> [<previous text> <description>]"
    cmd_report "$2" "$3" "${4:-}" "${5:-}"
    ;;
words-sha256)
    [ $# -eq 2 ] || die "usage: $0 words-sha256 <text file>"
    words_sha256 "$2"
    ;;
*)
    die "usage: $0 check|report|words-sha256 ..."
    ;;
esac
