"""
Usage event model and asynchronous emission to Elasticsearch.

Events are emitted in the background after responding to the client.
Emission failures are logged but never affect request handling.
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone

import httpx

from config import ES_HOST, USAGE_INDEX, CLUSTER_ID
from gateway.extractor import FieldRefs

logger = logging.getLogger(__name__)

# Dedicated client for writing usage events — separate from the proxy client
# to avoid contention.
_event_client = httpx.AsyncClient(base_url=ES_HOST, timeout=10.0)

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
) -> dict:
    """Build a usage event document."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cluster_id": CLUSTER_ID,
        "index": index_name or "_unknown",
        "operation": operation,
        "http_method": method,
        "path": path,
        "fields": field_refs.to_dict(),
        "language": language,
        "query_fingerprint": _compute_fingerprint(body),
        "response_time_ms": elapsed_ms,
        "response_status": response_status,
        "client_id": client_id,
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
    except httpx.RequestError as exc:
        logger.warning("Failed to emit usage event: %s", exc)


def emit_event_background(event: dict) -> None:
    """Schedule event emission as a fire-and-forget background task."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(emit_event(event))
    except RuntimeError:
        logger.debug("No event loop available for background emission")
