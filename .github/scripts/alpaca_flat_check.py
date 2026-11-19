"""Alpaca PAPER flat check: lists the paper account's open orders and positions.

Used by .github/workflows/alpaca-weekly.yml after its tests, so an order or position a test left
behind fails the run instead of poisoning the next one.

Prints one line per open order and position (symbol, side, quantity, status; no account ids or
balances), then `FLAT` or `NOT FLAT`. Exit status: 0 when flat, 1 when anything is open, 2 when a
request fails. Needs ALPACA_API_KEY and ALPACA_SECRET_KEY.
"""

import json
import os
import sys
import urllib.error
import urllib.request

BASE = "https://paper-api.alpaca.markets"
if os.environ.get("ALPACA_PAPER", "true").lower() == "false":
    sys.exit("ALPACA_PAPER is false: this check runs on the paper account only")
HEADERS = {
    "APCA-API-KEY-ID": os.environ["ALPACA_API_KEY"],
    "APCA-API-SECRET-KEY": os.environ["ALPACA_SECRET_KEY"],
}


def get(path):
    request = urllib.request.Request(BASE + path, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def main():
    try:
        orders = get("/v2/orders?status=open&limit=500")
        positions = get("/v2/positions")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        print(f"request failed: {error}")
        return 2
    for o in orders:
        print(
            f"open order: {o.get('symbol')} {o.get('side')} {o.get('qty')} "
            f"{o.get('type')} @ {o.get('limit_price')} {o.get('status')} "
            f"cid={o.get('client_order_id')}"
        )
    for p in positions:
        print(f"position: {p.get('symbol')} {p.get('side')} {p.get('qty')}")
    print(f"{len(orders)} open orders, {len(positions)} positions")
    flat = not orders and not positions
    print("FLAT" if flat else "NOT FLAT")
    return 0 if flat else 1


if __name__ == "__main__":
    sys.exit(main())
