"""
Traffic generator — sends weighted queries through the gateway to create
observable usage patterns.

Usage:
    python -m generator.generate                           # all scenarios, 200 each
    python -m generator.generate --scenario products       # single scenario
    python -m generator.generate --count 500               # custom count
    python -m generator.generate --duration 120 --rps 10   # timed mode, products only
"""

import argparse
import time
import sys
import random
import requests

from config import GATEWAY_PORT
from generator.queries import SCENARIOS

GATEWAY_URL = f"http://localhost:{GATEWAY_PORT}"


def _send_one_from_scenario(scenario_key: str, stats: dict) -> None:
    """Send a single random query from a scenario and update stats."""
    scenario = SCENARIOS[scenario_key]
    funcs = list(scenario["queries"].values())
    weights = list(scenario["weights"].values())
    func = random.choices(funcs, weights=weights, k=1)[0]
    method, path, body = func()

    url = f"{GATEWAY_URL}{path}"
    headers = {
        "Content-Type": "application/json",
        "X-Client-Id": "query-generator",
    }

    try:
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


def run_scenario(scenario_key: str, count: int) -> dict:
    """Send N queries for a specific scenario."""
    stats = {"sent": 0, "ok": 0, "errors": 0}
    label = SCENARIOS[scenario_key]["label"]
    print(f"  {label} ({scenario_key}): sending {count} queries...")

    for _ in range(count):
        _send_one_from_scenario(scenario_key, stats)
        if stats["sent"] % 100 == 0:
            print(f"    ...sent {stats['sent']} (ok={stats['ok']}, errors={stats['errors']})")

    return stats


def run_timed(duration_seconds: int, rps: float) -> dict:
    """Send queries at a target rate for a fixed duration (products scenario)."""
    interval = 1.0 / rps
    end_time = time.monotonic() + duration_seconds
    stats = {"sent": 0, "ok": 0, "errors": 0}

    print(f"Running for {duration_seconds}s at ~{rps} rps through {GATEWAY_URL}")

    while time.monotonic() < end_time:
        tick = time.monotonic()
        _send_one_from_scenario("products", stats)
        elapsed = time.monotonic() - tick
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Generate traffic through the ES gateway")
    parser.add_argument("--scenario", type=str, default=None,
                        choices=list(SCENARIOS.keys()),
                        help="Run a specific scenario (default: all)")
    parser.add_argument("--count", type=int, default=200,
                        help="Queries per scenario (default: 200)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Timed mode: duration in seconds (single scenario only)")
    parser.add_argument("--rps", type=float, default=5.0,
                        help="Timed mode: requests per second (default: 5)")
    args = parser.parse_args()

    start = time.monotonic()

    # Timed mode (backwards compatible)
    if args.duration is not None:
        stats = run_timed(args.duration, args.rps)
        elapsed = time.monotonic() - start
        actual_rps = stats["sent"] / max(elapsed, 0.01)
        print(f"\nDone in {elapsed:.1f}s")
        print(f"  Sent: {stats['sent']}  OK: {stats['ok']}  Errors: {stats['errors']}  Avg RPS: {actual_rps:.1f}")
        return

    # Scenario mode
    targets = [args.scenario] if args.scenario else list(SCENARIOS.keys())
    total_stats = {"sent": 0, "ok": 0, "errors": 0}

    print(f"Sending {args.count} queries per scenario through {GATEWAY_URL}")
    for key in targets:
        stats = run_scenario(key, args.count)
        total_stats["sent"] += stats["sent"]
        total_stats["ok"] += stats["ok"]
        total_stats["errors"] += stats["errors"]

    elapsed = time.monotonic() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Total sent: {total_stats['sent']}  OK: {total_stats['ok']}  Errors: {total_stats['errors']}")
    print(f"\nCheck heat report: curl {GATEWAY_URL}/_gateway/heat")


if __name__ == "__main__":
    main()
