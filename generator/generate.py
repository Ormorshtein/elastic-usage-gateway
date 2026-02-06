"""
Traffic generator — sends weighted queries through the gateway to create
observable usage patterns.

Usage:
    python -m generator.generate                      # 60s, 5 rps
    python -m generator.generate --duration 120 --rps 10
    python -m generator.generate --count 500          # fixed count, no rate limit
"""

import argparse
import time
import sys
import requests

from config import GATEWAY_PORT
from generator.queries import random_query

GATEWAY_URL = f"http://localhost:{GATEWAY_PORT}"


def run_timed(duration_seconds: int, rps: float) -> dict:
    """Send queries at a target rate for a fixed duration."""
    interval = 1.0 / rps
    end_time = time.monotonic() + duration_seconds
    stats = {"sent": 0, "ok": 0, "errors": 0}

    print(f"Running for {duration_seconds}s at ~{rps} rps through {GATEWAY_URL}")

    while time.monotonic() < end_time:
        tick = time.monotonic()
        _send_one(stats)
        elapsed = time.monotonic() - tick
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    return stats


def run_count(count: int) -> dict:
    """Send exactly N queries as fast as possible."""
    stats = {"sent": 0, "ok": 0, "errors": 0}

    print(f"Sending {count} queries through {GATEWAY_URL}")

    for _ in range(count):
        _send_one(stats)

    return stats


def _send_one(stats: dict) -> None:
    """Send a single random query and update stats."""
    method, path, body = random_query()
    url = f"{GATEWAY_URL}{path}"

    headers = {
        "Content-Type": "application/json",
        "X-Client-Id": "query-generator",
    }

    try:
        if method == "GET" and body is None:
            resp = requests.get(url, headers=headers, timeout=10)
        elif method == "POST":
            resp = requests.post(url, data=body, headers=headers, timeout=10)
        elif method == "GET":
            resp = requests.get(url, data=body, headers=headers, timeout=10)
        else:
            resp = requests.request(method, url, data=body, headers=headers, timeout=10)

        stats["sent"] += 1
        if 200 <= resp.status_code < 300:
            stats["ok"] += 1
        else:
            stats["errors"] += 1

    except requests.RequestException as exc:
        stats["sent"] += 1
        stats["errors"] += 1
        print(f"  Request error: {exc}", file=sys.stderr)

    # Progress indicator every 100 requests
    if stats["sent"] % 100 == 0:
        print(f"  ...sent {stats['sent']} (ok={stats['ok']}, errors={stats['errors']})")


def main():
    parser = argparse.ArgumentParser(description="Generate traffic through the ES gateway")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (default: 60)")
    parser.add_argument("--rps", type=float, default=5.0, help="Requests per second (default: 5)")
    parser.add_argument("--count", type=int, default=None, help="Send exactly N queries (overrides duration/rps)")
    args = parser.parse_args()

    start = time.monotonic()

    if args.count:
        stats = run_count(args.count)
    else:
        stats = run_timed(args.duration, args.rps)

    elapsed = time.monotonic() - start
    actual_rps = stats["sent"] / max(elapsed, 0.01)

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Sent:     {stats['sent']}")
    print(f"  OK:       {stats['ok']}")
    print(f"  Errors:   {stats['errors']}")
    print(f"  Avg RPS:  {actual_rps:.1f}")
    print(f"\nCheck heat report: curl {GATEWAY_URL}/_gateway/heat")


if __name__ == "__main__":
    main()
