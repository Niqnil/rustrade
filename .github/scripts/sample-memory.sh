#!/usr/bin/env bash
#
# Print one line of memory state every INTERVAL seconds until killed, along with any new
# out-of-memory lines in the kernel log or the systemd-oomd journal.
#
#   sample-memory.sh [INTERVAL] [RECORD]
#
# Written to attribute the `cargo test` job's intermittent exit-143 kills, which land during the
# final parallel compile-and-link and leave nothing but the exit code behind.
#
# Run it in the background of the step being diagnosed, not in a step of its own. That kill ends
# the whole job: later steps are skipped even under `if: always()`, and a background process
# started by an earlier step no longer writes to the log. Evidence therefore has to be in the
# step's own output before the kill lands. Each sample is also appended to RECORD, so a step that
# does run afterwards can summarise the peak.

set -uo pipefail

interval=${1:-5}
record=${2:-/dev/null}
cursors=$(mktemp -d)

echo "[mem] cpus=$(nproc) $(free -m | awk 'NR==2 {print "total=" $2 "M"} NR==3 {print "swap_total=" $2 "M"}' | paste -sd' ')"

# Print the journal entries added since the previous call. Polled once per sample rather than
# followed, because a `--follow` process runs as root and outlives any kill this script can send,
# holding the step's output open. `sudo -n` never prompts: hosted runners have passwordless sudo,
# and elsewhere this prints nothing. The first call also reports anything from earlier in the boot.
journal_since_last() {
  local name=$1
  shift
  sudo -n journalctl --quiet --no-pager --output=short-iso --cursor-file="$cursors/$name" "$@" 2>/dev/null
}

while true; do
  mem=$(free -m | awk 'NR==2 {printf "used=%sM avail=%sM", $3, $7} NR==3 {printf " swap=%sM", $3}')
  # Share of the last 10 s in which every task was stalled on memory: what systemd-oomd acts on.
  psi=$(awk '/^full/ {sub("avg10=", "", $2); print $2}' /proc/pressure/memory 2>/dev/null)
  # Every compiler process is `rustc`, so name each by the crate it is building, as `rustc:<crate>`.
  top=$(ps -eo rss=,args= --sort=-rss | head -5 | awk '{
    name = $2; sub(".*/", "", name)
    for (i = 3; i < NF; i++) if ($i == "--crate-name") { name = name ":" $(i + 1); break }
    printf "%s%s %dM", sep, name, $1 / 1024; sep = ", "
  }')
  echo "[mem $(date -u +%H:%M:%S)] $mem psi_full=${psi:-?}% | $top" | tee -a "$record"
  journal_since_last kernel --dmesg | grep -iE 'out of memory|oom|killed process' | sed 's/^/[kernel] /'
  journal_since_last oomd --unit=systemd-oomd | sed 's/^/[oomd] /'
  # Detached from the step's output, so a sleep left running by a kill cannot hold the log open.
  sleep "$interval" >/dev/null 2>&1
done
