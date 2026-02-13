"""
Simple in-memory counters for gateway observability.

Counters reset on restart — acceptable for a dev/monitoring tool.
No external dependencies. Safe under asyncio's single-threaded model.
"""

from datetime import datetime, timezone

_startup_time = datetime.now(timezone.utc)

_counters = {
    "requests_proxied": 0,
    "requests_failed": 0,
    "events_emitted": 0,
    "events_failed": 0,
    "events_skipped": 0,
    "events_sampled_out": 0,
    "events_dropped": 0,
    "extraction_errors": 0,
    "metadata_refresh_ok": 0,
    "metadata_refresh_failed": 0,
    "mapping_diff_refresh_ok": 0,
    "mapping_diff_refresh_failed": 0,
}

# Timing trackers: each has its own count so avg is independent of counters.
_es_time = {"sum_ms": 0.0, "max_ms": 0.0, "count": 0}
_request_time = {"sum_ms": 0.0, "max_ms": 0.0, "count": 0}


def inc(name: str) -> None:
    """Increment a counter by 1."""
    _counters[name] = _counters.get(name, 0) + 1


def inc_by(name: str, n: int) -> None:
    """Increment a counter by n."""
    _counters[name] = _counters.get(name, 0) + n


def observe_es_time(ms: float) -> None:
    """Record an Elasticsearch round-trip time in milliseconds."""
    _es_time["sum_ms"] += ms
    _es_time["count"] += 1
    if ms > _es_time["max_ms"]:
        _es_time["max_ms"] = ms


def observe_request_time(ms: float) -> None:
    """Record total request processing time (ES round-trip + gateway overhead)."""
    _request_time["sum_ms"] += ms
    _request_time["count"] += 1
    if ms > _request_time["max_ms"]:
        _request_time["max_ms"] = ms


def _avg(tracker: dict, precision: int = 2) -> float:
    if tracker["count"] == 0:
        return 0.0
    return round(tracker["sum_ms"] / tracker["count"], precision)


def get_all() -> dict:
    """Return all counters plus uptime and timing info."""
    return {
        **_counters,
        "es_time_avg_ms": _avg(_es_time),
        "es_time_max_ms": round(_es_time["max_ms"], 2),
        "request_time_avg_ms": _avg(_request_time),
        "request_time_max_ms": round(_request_time["max_ms"], 2),
        "startup_time": _startup_time.isoformat(),
        "uptime_seconds": round(
            (datetime.now(timezone.utc) - _startup_time).total_seconds(), 1
        ),
    }


def reset() -> None:
    """Reset all counters and timings to zero. Used in tests."""
    for key in _counters:
        _counters[key] = 0
    for tracker in (_es_time, _request_time):
        tracker["sum_ms"] = 0.0
        tracker["max_ms"] = 0.0
        tracker["count"] = 0
