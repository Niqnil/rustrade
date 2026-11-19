"""Wait for the US regular session to open, by Alpaca's paper clock.

Used by .github/workflows/alpaca-weekly.yml, whose tests need the session open: an equity order
placed outside it waits `accepted`, and the IEX feed carries no trades or quotes.

Usage: alpaca_wait_for_session.py <budget minutes> [--horizon <minutes>] [--final]

Returns once the session is open (exit 0). When this call saw the market closed first, it waits
two minutes more, so an order placed next rests `new` rather than queued for the open; a call that
finds the session already open returns at once. Each call knows only what it saw, so when the
open falls between one chained job's budget running out and the next job's first read, the next
job returns without the two minutes; the window is the gap between the jobs. A GitHub-hosted job
ends at 360 minutes, so a longer wait is split across chained jobs, each calling this with its own
budget:
- When the budget runs out first, it exits 0 so the next job carries on; with --final it exits 1.
- With --horizon, it exits 1 at once when the next open is further away than that many minutes
  (a market holiday, or a run that started after the close), instead of waiting out the budget.

Needs ALPACA_API_KEY and ALPACA_SECRET_KEY. Prints only clock times.
"""

import http.client
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime

# Consecutive failed clock reads before giving up.
MAX_FAILURES = 5
CLOCK_URL = "https://paper-api.alpaca.markets/v2/clock"
HEADERS = {
    "APCA-API-KEY-ID": os.environ["ALPACA_API_KEY"],
    "APCA-API-SECRET-KEY": os.environ["ALPACA_SECRET_KEY"],
}


def clock():
    request = urllib.request.Request(CLOCK_URL, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def parse(text):
    # Alpaca sends nanoseconds; fromisoformat takes at most microseconds.
    return datetime.fromisoformat(re.sub(r"\.\d+", "", text))


def option(name):
    if name not in sys.argv:
        return None
    return float(sys.argv[sys.argv.index(name) + 1])


def main():
    budget = float(sys.argv[1]) * 60
    horizon = option("--horizon")
    final = "--final" in sys.argv[2:]
    deadline = time.monotonic() + budget
    first = True
    failures = 0
    while True:
        try:
            c = clock()
        # OSError covers URLError, timeouts and a connection reset mid-read.
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as error:
            failures += 1
            print(f"clock request failed ({failures} of {MAX_FAILURES}): {error}", flush=True)
            if failures >= MAX_FAILURES:
                return 1
            time.sleep(60)
            continue
        failures = 0
        if c["is_open"]:
            print(f"open at {c['timestamp']}; next close {c['next_close']}", flush=True)
            if not first:
                time.sleep(120)
            return 0
        wait = (parse(c["next_open"]) - parse(c["timestamp"])).total_seconds()
        if first and horizon is not None and wait > horizon * 60:
            print(
                f"{c['timestamp']}: closed, next open {c['next_open']} is {wait / 60:.0f} min away, "
                f"beyond the {horizon:.0f} min this run can wait",
                flush=True,
            )
            return 1
        first = False
        left = deadline - time.monotonic()
        if left <= 0:
            print(f"budget spent; next open {c['next_open']}", flush=True)
            return 1 if final else 0
        print(f"{c['timestamp']}: closed, next open {c['next_open']} ({wait / 60:.0f} min)", flush=True)
        time.sleep(max(30, min(wait, 900, left)))


if __name__ == "__main__":
    sys.exit(main())
