"""
Usage event model and asynchronous emission to Elasticsearch.

Events are emitted in the background via asyncio.create_task() after the
response has already been sent to the client. This "fire-and-forget" pattern
ensures that observation never delays or blocks the proxied request.

Thread safety: All state (module globals, httpx client) is accessed from the
single asyncio event loop thread — no locks needed. The _query_body_enabled
and _query_body_sample_rate globals are safe to read/write under asyncio's
cooperative scheduling model (no preemption mid-assignment).

Lifecycle: _event_client is created at module import and must be closed via
close_event_client() during shutdown (called from the lifespan hook in main.py).
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import random as _random
from datetime import datetime, timezone

import httpx

from config import (
    ES_HOST, USAGE_INDEX, CLUSTER_ID, QUERY_BODY_ENABLED, QUERY_BODY_SAMPLE_RATE,
    EVENT_TIMEOUT, SAMPLING_MAX_EVENTS_PER_SEC, SAMPLING_LOW_THRESHOLD,
)
from gateway.extractor import FieldRefs
from gateway import metrics

logger = logging.getLogger(__name__)

# Runtime-configurable query body storage
_query_body_enabled: bool = QUERY_BODY_ENABLED
_query_body_sample_rate: float = QUERY_BODY_SAMPLE_RATE

# Runtime-configurable adaptive sampling
_sampling_max_eps: float = SAMPLING_MAX_EVENTS_PER_SEC
_sampling_low_threshold: float = SAMPLING_LOW_THRESHOLD


def get_query_body_config() -> dict:
    """Return current query body storage configuration."""
    return {"enabled": _query_body_enabled, "sample_rate": _query_body_sample_rate}


def set_query_body_config(enabled: bool | None = None, sample_rate: float | None = None) -> dict:
    """Update query body storage configuration at runtime."""
    global _query_body_enabled, _query_body_sample_rate
    if enabled is not None:
        _query_body_enabled = enabled
    if sample_rate is not None:
        _query_body_sample_rate = max(0.0, min(1.0, sample_rate))
    return get_query_body_config()


def get_sampling_config() -> dict:
    """Return current adaptive sampling configuration."""
    return {"max_events_per_sec": _sampling_max_eps, "low_threshold": _sampling_low_threshold}


def set_sampling_config(max_events_per_sec: float | None = None, low_threshold: float | None = None) -> dict:
    """Update adaptive sampling configuration at runtime."""
    global _sampling_max_eps, _sampling_low_threshold
    if max_events_per_sec is not None:
        _sampling_max_eps = max(1.0, max_events_per_sec)
    if low_threshold is not None:
        _sampling_low_threshold = max(1.0, low_threshold)
    return get_sampling_config()


def compute_sample_rate() -> float:
    """Compute the current sample rate based on request throughput.

    Returns 1.0 (emit all) when rps is below the low threshold.
    Above the threshold, caps event rate at max_events_per_sec.
    """
    rps = metrics.get_requests_per_second()
    if rps <= _sampling_low_threshold:
        return 1.0
    return min(1.0, _sampling_max_eps / max(rps, 0.01))


# Dedicated client for writing usage events — separate from the proxy client
# to avoid contention.
_event_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

# Mapping for the .usage-events index
USAGE_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":       {"type": "date"},
            "cluster_id":      {"type": "keyword"},
            "index":           {"type": "keyword"},
            "operation":       {"type": "keyword"},
            "http_method":     {"type": "keyword"},
            "path":            {"type": "keyword"},
            "fields": {
                "properties": {
                    "queried":    {"type": "keyword"},
                    "filtered":   {"type": "keyword"},
                    "aggregated": {"type": "keyword"},
                    "sorted":     {"type": "keyword"},
                    "sourced":    {"type": "keyword"},
                    "written":    {"type": "keyword"},
                }
            },
            "language":           {"type": "keyword"},
            "query_fingerprint": {"type": "keyword"},
            "response_time_ms":  {"type": "float"},
            "response_status":   {"type": "integer"},
            "client_id":         {"type": "keyword"},
            "index_group":       {"type": "keyword"},
            "lookback_seconds":  {"type": "float"},
            "lookback_field":    {"type": "keyword"},
            "lookback_label":    {"type": "keyword"},
            "query_body":        {"type": "keyword", "index": False, "doc_values": False, "ignore_above": 4096},
            "type":              {"type": "keyword"},
            "sample_weight":     {"type": "float"},
            "window_start":      {"type": "date"},
            "window_end":        {"type": "date"},
            "total_operations":  {"type": "float"},
            "field_usage":       {"type": "object", "enabled": False},
            "lookback_sum_seconds":  {"type": "float"},
            "lookback_max_seconds":  {"type": "float"},
            "lookback_count":        {"type": "integer"},
            "avg_response_time_ms":  {"type": "float"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}


async def ensure_usage_index() -> None:
    """Create the usage-events index if it doesn't exist."""
    try:
        resp = await _event_client.head(f"/{USAGE_INDEX}")
        if resp.status_code == 200:
            return
        resp = await _event_client.put(
            f"/{USAGE_INDEX}",
            json=USAGE_INDEX_MAPPING,
        )
        if resp.status_code in (200, 201):
            logger.info("Created usage index: %s", USAGE_INDEX)
        else:
            logger.warning(
                "Failed to create usage index: %s %s",
                resp.status_code, resp.text
            )
    except httpx.RequestError as exc:
        logger.warning("Could not ensure usage index exists: %s", exc)


async def update_mapping() -> None:
    """Add new fields to an existing usage-events index mapping.

    Safe to call on every startup — ES allows adding new fields to an
    existing mapping without affecting existing documents.
    """
    new_fields = {
        "type":              {"type": "keyword"},
        "sample_weight":     {"type": "float"},
        "window_start":      {"type": "date"},
        "window_end":        {"type": "date"},
        "total_operations":  {"type": "float"},
        "field_usage":       {"type": "object", "enabled": False},
        "lookback_sum_seconds":  {"type": "float"},
        "lookback_max_seconds":  {"type": "float"},
        "lookback_count":        {"type": "integer"},
        "avg_response_time_ms":  {"type": "float"},
    }
    try:
        resp = await _event_client.put(
            f"/{USAGE_INDEX}/_mapping",
            json={"properties": new_fields},
        )
        if resp.status_code == 200:
            logger.info("Updated usage index mapping with new fields")
        else:
            logger.warning("Failed to update mapping: %s %s", resp.status_code, resp.text[:200])
    except httpx.RequestError as exc:
        logger.warning("Could not update usage index mapping: %s", exc)


def _compute_fingerprint(body: bytes) -> str | None:
    """SHA-256 of canonicalized JSON body, or None if not parseable."""
    if not body:
        return None
    try:
        parsed = json.loads(body)
        canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def build_event(
    index_name: str | None,
    operation: str,
    field_refs: FieldRefs,
    method: str,
    path: str,
    response_status: int,
    elapsed_ms: float,
    client_id: str | None = None,
    language: str = "dsl",
    body: bytes = b"",
    index_group: str | None = None,
) -> dict:
    """Build a usage event document."""
    idx = index_name or "_unknown"
    lookback = field_refs.lookback
    return {
        "type": "raw",
        "sample_weight": 1.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cluster_id": CLUSTER_ID,
        "index": idx,
        "index_group": index_group or idx,
        "operation": operation,
        "http_method": method,
        "path": path,
        "fields": field_refs.to_dict(),
        "language": language,
        "query_fingerprint": _compute_fingerprint(body),
        "response_time_ms": elapsed_ms,
        "response_status": response_status,
        "client_id": client_id,
        "lookback_seconds": lookback.seconds if lookback else None,
        "lookback_field": lookback.field if lookback else None,
        "lookback_label": lookback.label if lookback else None,
        "query_body": (
            body.decode("utf-8", errors="replace")[:4096]
            if body and _query_body_enabled and _random.random() < _query_body_sample_rate
            else None
        ),
    }


async def emit_event(event: dict) -> None:
    """
    Write a usage event to the usage index.

    This runs as a background task — failures are logged, never raised.
    """
    try:
        resp = await _event_client.post(
            f"/{USAGE_INDEX}/_doc",
            json=event,
        )
        if resp.status_code not in (200, 201):
            logger.warning(
                "Failed to emit usage event: %s %s",
                resp.status_code, resp.text[:200]
            )
            metrics.inc("events_failed")
        else:
            metrics.inc("events_emitted")
    except httpx.RequestError as exc:
        logger.warning("Failed to emit usage event: %s", exc)
        metrics.inc("events_failed")


def emit_event_background(event: dict) -> None:
    """Schedule event emission as a fire-and-forget background task.

    Applies adaptive sampling: when request rate exceeds the low threshold,
    only a fraction of events are emitted, with sample_weight adjusted to
    compensate for dropped events (so rollup aggregation stays unbiased).
    """
    rate = compute_sample_rate()
    if rate < 1.0:
        if _random.random() >= rate:
            metrics.inc("events_sampled_out")
            return
        event["sample_weight"] = round(1.0 / rate, 4)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(emit_event(event))
    except RuntimeError:
        logger.debug("No event loop available for background emission")


async def close_event_client() -> None:
    """Close the event client. Called during gateway shutdown."""
    await _event_client.aclose()
