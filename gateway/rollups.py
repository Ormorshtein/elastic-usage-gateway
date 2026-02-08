"""
Periodic rollup of raw usage events into compact summary documents.

Follows the same background-loop pattern as metadata.py. The rollup cycle:
1. Read raw events since last rollup (via search_after pagination)
2. Aggregate in-memory by (index_group, index) with weighted field counts
3. Write rollup documents back to the usage index
4. Delete raw events older than retention window
5. Delete rollup docs older than retention days

All state is accessed from the single asyncio event loop — no locks needed.
"""

from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import httpx

from config import (
    ES_HOST, USAGE_INDEX, EVENT_TIMEOUT,
    ROLLUP_INTERVAL_SECONDS, ROLLUP_BATCH_SIZE,
    RAW_RETENTION_HOURS, ROLLUP_RETENTION_DAYS,
)
from gateway.events import USAGE_INDEX_MAPPING
from gateway import metrics

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

# Runtime-configurable rollup settings
_rollup_interval: int = ROLLUP_INTERVAL_SECONDS
_raw_retention_hours: float = RAW_RETENTION_HOURS
_rollup_retention_days: int = ROLLUP_RETENTION_DAYS

# Last rollup end timestamp — persisted as a document in the usage index
_last_rollup_end: str | None = None

# Field usage categories (must match events.py FIELD_CATEGORIES)
FIELD_CATEGORIES = ("queried", "filtered", "aggregated", "sorted", "sourced", "written")


def get_rollup_config() -> dict:
    """Return current rollup configuration."""
    return {
        "interval_seconds": _rollup_interval,
        "raw_retention_hours": _raw_retention_hours,
        "rollup_retention_days": _rollup_retention_days,
        "last_rollup_end": _last_rollup_end,
    }


def set_rollup_config(
    interval_seconds: int | None = None,
    raw_retention_hours: float | None = None,
    rollup_retention_days: int | None = None,
) -> dict:
    """Update rollup configuration at runtime."""
    global _rollup_interval, _raw_retention_hours, _rollup_retention_days
    if interval_seconds is not None:
        _rollup_interval = max(60, min(600, interval_seconds))
    if raw_retention_hours is not None:
        _raw_retention_hours = max(0.5, raw_retention_hours)
    if rollup_retention_days is not None:
        _rollup_retention_days = max(1, rollup_retention_days)
    return get_rollup_config()


def aggregate_events(events: list[dict]) -> list[dict]:
    """Aggregate raw events into rollup summaries grouped by (index_group, index).

    Each event is expected to have:
      - index_group, index, sample_weight (default 1.0)
      - fields.{queried, filtered, aggregated, sorted, sourced, written} (lists)
      - lookback_seconds (optional float)
      - response_time_ms (optional float)

    Returns a list of rollup-ready dicts (without window timestamps — caller adds those).
    """
    # Key: (index_group, index) → aggregated data
    groups: dict[tuple[str, str], dict] = {}

    for event in events:
        idx_group = event.get("index_group", event.get("index", "_unknown"))
        idx = event.get("index", "_unknown")
        weight = event.get("sample_weight", 1.0)
        key = (idx_group, idx)

        if key not in groups:
            groups[key] = {
                "total_operations": 0.0,
                "field_usage": defaultdict(lambda: {cat: 0.0 for cat in FIELD_CATEGORIES}),
                "lookback_sum_seconds": 0.0,
                "lookback_max_seconds": 0.0,
                "lookback_count": 0,
                "response_time_sum_ms": 0.0,
                "response_time_count": 0,
            }

        agg = groups[key]
        agg["total_operations"] += weight

        # Aggregate field usage
        fields = event.get("fields", {})
        for cat in FIELD_CATEGORIES:
            for field_name in fields.get(cat, []):
                agg["field_usage"][field_name][cat] += weight

        # Lookback stats
        lb = event.get("lookback_seconds")
        if lb is not None:
            agg["lookback_sum_seconds"] += lb * weight
            agg["lookback_max_seconds"] = max(agg["lookback_max_seconds"], lb)
            agg["lookback_count"] += 1

        # Response time
        rt = event.get("response_time_ms")
        if rt is not None:
            agg["response_time_sum_ms"] += rt * weight
            agg["response_time_count"] += 1

    # Build rollup documents
    result = []
    for (idx_group, idx), agg in groups.items():
        # Convert field_usage defaultdict to regular dict with rounded values
        field_usage = {}
        for field_name, cats in agg["field_usage"].items():
            field_usage[field_name] = {cat: round(count) for cat, count in cats.items()}

        avg_rt = (
            round(agg["response_time_sum_ms"] / agg["response_time_count"], 2)
            if agg["response_time_count"] > 0
            else 0.0
        )

        result.append({
            "type": "rollup",
            "index_group": idx_group,
            "index": idx,
            "total_operations": round(agg["total_operations"], 2),
            "field_usage": field_usage,
            "lookback_sum_seconds": round(agg["lookback_sum_seconds"], 1),
            "lookback_max_seconds": round(agg["lookback_max_seconds"], 1),
            "lookback_count": agg["lookback_count"],
            "avg_response_time_ms": avg_rt,
        })

    return result


async def _load_last_rollup_end() -> None:
    """Load the last rollup end time from the _rollup_state document."""
    global _last_rollup_end
    try:
        resp = await _client.get(f"/{USAGE_INDEX}/_doc/_rollup_state")
        if resp.status_code == 200:
            source = resp.json().get("_source", {})
            _last_rollup_end = source.get("last_rollup_end")
            logger.info("Loaded last rollup end: %s", _last_rollup_end)
        else:
            logger.info("No rollup state found — will start from beginning")
    except httpx.RequestError as exc:
        logger.warning("Could not load rollup state: %s", exc)


async def _save_rollup_state(last_end: str) -> None:
    """Persist the last rollup end time."""
    global _last_rollup_end
    try:
        resp = await _client.put(
            f"/{USAGE_INDEX}/_doc/_rollup_state",
            json={"type": "_meta", "last_rollup_end": last_end},
        )
        if resp.status_code in (200, 201):
            _last_rollup_end = last_end
        else:
            logger.warning("Failed to save rollup state: %s %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as exc:
        logger.warning("Could not save rollup state: %s", exc)


async def _fetch_raw_events(since: str, until: str) -> list[dict]:
    """Fetch raw events in the time window using search_after pagination."""
    all_events = []
    search_after = None

    # Match raw events and legacy events without a type field
    query = {
        "bool": {
            "must": [
                {"range": {"timestamp": {"gte": since, "lt": until}}},
            ],
            "should": [
                {"term": {"type": "raw"}},
                {"bool": {"must_not": {"exists": {"field": "type"}}}},
            ],
            "minimum_should_match": 1,
        }
    }

    while True:
        body: dict = {
            "size": ROLLUP_BATCH_SIZE,
            "query": query,
            "sort": [{"timestamp": "asc"}, {"_id": "asc"}],
            "_source": True,
        }
        if search_after:
            body["search_after"] = search_after

        try:
            resp = await _client.post(f"/{USAGE_INDEX}/_search", json=body)
            if resp.status_code != 200:
                logger.warning("Rollup fetch failed: %s %s", resp.status_code, resp.text[:200])
                break
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            for hit in hits:
                all_events.append(hit["_source"])
            search_after = hits[-1]["sort"]
            if len(hits) < ROLLUP_BATCH_SIZE:
                break
        except httpx.RequestError as exc:
            logger.warning("Rollup fetch error: %s", exc)
            break

    return all_events


async def _check_existing_rollups(window_start: str, window_end: str) -> bool:
    """Check if rollup docs already exist for this exact window."""
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "rollup"}},
                    {"term": {"window_start": window_start}},
                    {"term": {"window_end": window_end}},
                ],
            }
        },
    }
    try:
        resp = await _client.post(f"/{USAGE_INDEX}/_search", json=query)
        if resp.status_code == 200:
            total = resp.json().get("hits", {}).get("total", {}).get("value", 0)
            return total > 0
    except httpx.RequestError:
        pass
    return False


async def _write_rollup_docs(rollup_docs: list[dict]) -> int:
    """Bulk-write rollup documents. Returns count of successfully written docs."""
    if not rollup_docs:
        return 0

    # Build bulk body (NDJSON)
    lines = []
    for doc in rollup_docs:
        lines.append('{"index":{}}')
        import json
        lines.append(json.dumps(doc))
    bulk_body = "\n".join(lines) + "\n"

    try:
        resp = await _client.post(
            f"/{USAGE_INDEX}/_bulk",
            content=bulk_body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        if resp.status_code == 200:
            data = resp.json()
            errors = sum(1 for item in data.get("items", []) if item.get("index", {}).get("error"))
            written = len(data.get("items", [])) - errors
            if errors:
                logger.warning("Rollup bulk write had %d errors", errors)
            return written
        else:
            logger.warning("Rollup bulk write failed: %s %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as exc:
        logger.warning("Rollup bulk write error: %s", exc)
    return 0


async def _delete_old_raw_events() -> int:
    """Delete raw events older than retention window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=_raw_retention_hours)).isoformat()
    query = {
        "query": {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"lt": cutoff}}},
                ],
                "should": [
                    {"term": {"type": "raw"}},
                    {"bool": {"must_not": {"exists": {"field": "type"}}}},
                ],
                "minimum_should_match": 1,
            }
        }
    }
    try:
        resp = await _client.post(
            f"/{USAGE_INDEX}/_delete_by_query",
            json=query,
            params={"conflicts": "proceed"},
        )
        if resp.status_code == 200:
            deleted = resp.json().get("deleted", 0)
            if deleted:
                logger.info("Deleted %d old raw events (older than %s)", deleted, cutoff)
            return deleted
        else:
            logger.warning("Raw event cleanup failed: %s %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as exc:
        logger.warning("Raw event cleanup error: %s", exc)
    return 0


async def _delete_old_rollups() -> int:
    """Delete rollup documents older than retention period."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_rollup_retention_days)).isoformat()
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "rollup"}},
                    {"range": {"window_end": {"lt": cutoff}}},
                ],
            }
        }
    }
    try:
        resp = await _client.post(
            f"/{USAGE_INDEX}/_delete_by_query",
            json=query,
            params={"conflicts": "proceed"},
        )
        if resp.status_code == 200:
            deleted = resp.json().get("deleted", 0)
            if deleted:
                logger.info("Deleted %d old rollup docs (older than %s)", deleted, cutoff)
            return deleted
        else:
            logger.warning("Rollup cleanup failed: %s %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as exc:
        logger.warning("Rollup cleanup error: %s", exc)
    return 0


async def run_rollup_now() -> dict:
    """Execute a rollup cycle on demand. Returns a summary of what happened."""
    now = datetime.now(timezone.utc)
    window_end = (now - timedelta(minutes=1)).isoformat()

    # Determine window start
    if _last_rollup_end:
        window_start = _last_rollup_end
    else:
        # First rollup — go back 24 hours
        window_start = (now - timedelta(hours=24)).isoformat()

    # Check for existing rollups in this window (dedupe protection)
    if await _check_existing_rollups(window_start, window_end):
        logger.info("Rollup docs already exist for window %s → %s, skipping", window_start, window_end)
        return {"status": "skipped", "reason": "duplicate_window", "window_start": window_start, "window_end": window_end}

    # Fetch raw events
    events = await _fetch_raw_events(window_start, window_end)
    if not events:
        await _save_rollup_state(window_end)
        return {"status": "ok", "events_read": 0, "rollups_written": 0, "window_start": window_start, "window_end": window_end}

    # Aggregate
    rollup_docs = aggregate_events(events)

    # Add window timestamps
    for doc in rollup_docs:
        doc["window_start"] = window_start
        doc["window_end"] = window_end

    # Write rollup docs
    written = await _write_rollup_docs(rollup_docs)

    # Save state only after successful write
    await _save_rollup_state(window_end)

    # Cleanup
    raw_deleted = await _delete_old_raw_events()
    rollup_deleted = await _delete_old_rollups()

    metrics.inc("rollups_completed")

    return {
        "status": "ok",
        "events_read": len(events),
        "rollups_written": written,
        "raw_deleted": raw_deleted,
        "rollup_deleted": rollup_deleted,
        "window_start": window_start,
        "window_end": window_end,
    }


async def _rollup_loop() -> None:
    """Background loop that runs rollup cycles periodically."""
    await _load_last_rollup_end()
    while True:
        await asyncio.sleep(_rollup_interval)
        try:
            result = await run_rollup_now()
            logger.info("Rollup cycle: %s", result)
        except Exception:
            logger.exception("Rollup cycle failed")
            metrics.inc("rollups_failed")


def start_rollup_loop() -> None:
    """Start the background rollup loop. Call from within an async context."""
    loop = asyncio.get_running_loop()
    loop.create_task(_rollup_loop())
    logger.info("Rollup loop started (interval=%ds)", _rollup_interval)


async def close_rollup_client() -> None:
    """Close the rollup client. Called during gateway shutdown."""
    await _client.aclose()
