#!/usr/bin/env bash
#
# Print one line of memory state every INTERVAL seconds until killed, and follow the kernel and
# systemd-oomd logs for out-of-memory kills as they happen.
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

# Stop the log followers with the sampler, or they outlive the step that started them.
trap 'kill $(jobs -p) 2>/dev/null' EXIT
trap 'exit 0' TERM INT

echo "[mem] cpus=$(nproc) $(free -m | awk 'NR==2 {print "total=" $2 "M"} NR==3 {print "swap_total=" $2 "M"}' | paste -sd' ')"

# `-n`: never prompt for a password. Hosted runners have passwordless sudo; elsewhere the followers
# print nothing rather than block.
sudo -n dmesg --follow 2>/dev/null \
  | grep --line-buffered -iE 'out of memory|oom|killed process' \
  | sed -u 's/^/[kernel] /' &
sudo -n journalctl --follow --lines=0 --unit=systemd-oomd --output=short-iso 2>/dev/null \
  | sed -u 's/^/[oomd] /' &

while true; do
  mem=$(free -m | awk 'NR==2 {printf "used=%sM avail=%sM", $3, $7} NR==3 {printf " swap=%sM", $3}')
  # Share of the last 10 s in which every task was stalled on memory: what systemd-oomd acts on.
  psi=$(awk '/^full/ {sub("avg10=", "", $2); print $2}' /proc/pressure/memory 2>/dev/null)
  top=$(ps -eo rss=,comm= --sort=-rss | head -5 | awk '{printf "%s%s %dM", sep, $2, $1 / 1024; sep = ", "}')
  echo "[mem $(date -u +%H:%M:%S)] $mem psi_full=${psi:-?}% | $top" | tee -a "$record"
  # Wait in the background so a TERM is handled at once rather than after the sleep.
  sleep "$interval" &
  wait $!
done
