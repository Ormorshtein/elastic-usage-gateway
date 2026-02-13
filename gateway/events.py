"""
Usage event model and asynchronous bulk emission to Elasticsearch.

Events are placed into a bounded asyncio.Queue by the request handler.
A background consumer task drains the queue and flushes events to ES
via the _bulk API — either when the batch reaches BULK_FLUSH_SIZE or
every BULK_FLUSH_INTERVAL seconds, whichever comes first.

Backpressure: If the queue is full (BULK_QUEUE_SIZE), new events are
dropped and counted as events_dropped. This prevents unbounded memory
growth if ES is slow or unreachable.

Thread safety: All state (module globals, httpx client, queue) is accessed
from the single asyncio event loop thread — no locks needed. The
_query_body_enabled and _query_body_sample_rate globals are safe to
read/write under asyncio's cooperative scheduling model.

Lifecycle: The bulk writer must be started via start_bulk_writer() during
app startup and stopped via stop_bulk_writer() during shutdown (both
called from the lifespan hook in main.py).
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
    ES_HOST, USAGE_INDEX, CLUSTER_ID, EVENT_SAMPLE_RATE,
    QUERY_BODY_ENABLED, QUERY_BODY_SAMPLE_RATE, EVENT_TIMEOUT,
    BULK_FLUSH_SIZE, BULK_FLUSH_INTERVAL, BULK_QUEUE_SIZE,
)
from gateway.extractor import FieldRefs
from gateway import metrics

logger = logging.getLogger(__name__)

# Runtime-configurable event sampling
_event_sample_rate: float = EVENT_SAMPLE_RATE

# Runtime-configurable query body storage
_query_body_enabled: bool = QUERY_BODY_ENABLED
_query_body_sample_rate: float = QUERY_BODY_SAMPLE_RATE


def get_event_sample_config() -> dict:
    """Return current event sampling configuration."""
    return {"sample_rate": _event_sample_rate}


def set_event_sample_config(sample_rate: float | None = None) -> dict:
    """Update event sampling configuration at runtime."""
    global _event_sample_rate
    if sample_rate is not None:
        _event_sample_rate = max(0.0, min(1.0, sample_rate))
    return get_event_sample_config()


def should_sample_event() -> bool:
    """Return True if this event should be emitted based on the sample rate."""
    if _event_sample_rate >= 1.0:
        return True
    if _event_sample_rate <= 0.0:
        return False
    return _random.random() < _event_sample_rate


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

# Dedicated client for writing usage events — separate from the proxy client
# to avoid contention.
_event_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

# Bounded queue for backpressure — events are dropped (not blocked) when full.
_event_queue: asyncio.Queue | None = None

# Handle to the background bulk-writer task (set by start_bulk_writer).
_bulk_writer_task: asyncio.Task | None = None

# Sentinel object placed in the queue to signal the writer to stop.
_STOP_SENTINEL = object()

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
            "query_fingerprint":    {"type": "keyword"},
            "query_template_hash":  {"type": "keyword"},
            "query_template_text":  {"type": "keyword", "index": False, "doc_values": False, "ignore_above": 4096},
            "response_time_ms":  {"type": "float"},
            "response_status":   {"type": "integer"},
            "client_id":         {"type": "keyword"},
            "client_ip":         {"type": "keyword"},
            "client_user_agent": {"type": "keyword", "ignore_above": 512},
            "index_group":       {"type": "keyword"},
            "lookback_seconds":  {"type": "float"},
            "lookback_field":    {"type": "keyword"},
            "lookback_label":    {"type": "keyword"},
            "query_body":        {"type": "keyword", "index": False, "doc_values": False, "ignore_above": 4096},
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


def _templatize(obj):
    """Replace all leaf values with "?" to extract query structure.

    Dicts preserve keys, lists of dicts preserve structure,
    lists of all scalars collapse to ["?"] so the number of
    values doesn't affect the template.
    """
    if isinstance(obj, dict):
        return {k: _templatize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        if not obj:
            return []
        if all(not isinstance(item, (dict, list)) for item in obj):
            return ["?"]
        return [_templatize(item) for item in obj]
    return "?"


def _compute_template(body: bytes) -> tuple[str | None, str | None]:
    """Compute structural template hash and text from a query body.

    Returns (template_hash, template_text) or (None, None) if body
    is empty or not valid JSON.
    """
    if not body:
        return None, None
    try:
        parsed = json.loads(body)
        template = _templatize(parsed)
        canonical = json.dumps(template, sort_keys=True, separators=(",", ":"))
        template_hash = hashlib.sha256(canonical.encode()).hexdigest()
        # Readable version with spaces, capped at 4096 chars
        template_text = json.dumps(template, sort_keys=True, separators=(", ", ": "))[:4096]
        return template_hash, template_text
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, None


def build_event(
    index_name: str | None,
    operation: str,
    field_refs: FieldRefs,
    method: str,
    path: str,
    response_status: int,
    elapsed_ms: float,
    client_id: str | None = None,
    client_ip: str | None = None,
    client_user_agent: str | None = None,
    language: str = "dsl",
    body: bytes = b"",
    index_group: str | None = None,
) -> dict:
    """Build a usage event document."""
    idx = index_name or "_unknown"
    lookback = field_refs.lookback
    template_hash, template_text = _compute_template(body)
    return {
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
        "query_template_hash": template_hash,
        "query_template_text": template_text,
        "response_time_ms": elapsed_ms,
        "response_status": response_status,
        "client_id": client_id,
        "client_ip": client_ip,
        "client_user_agent": client_user_agent,
        "lookback_seconds": lookback.seconds if lookback else None,
        "lookback_field": lookback.field if lookback else None,
        "lookback_label": lookback.label if lookback else None,
        "query_body": (
            body.decode("utf-8", errors="replace")[:4096]
            if body and _query_body_enabled and _random.random() < _query_body_sample_rate
            else None
        ),
    }


# --- Bulk event writer ---

def _build_bulk_body(events: list[dict]) -> str:
    """Build an NDJSON bulk request body from a list of event dicts."""
    lines = []
    for event in events:
        lines.append(json.dumps({"index": {"_index": USAGE_INDEX}}))
        lines.append(json.dumps(event))
    return "\n".join(lines) + "\n"


async def _flush_events(events: list[dict]) -> None:
    """Flush a batch of events to ES via _bulk. Logs failures, never raises."""
    if not events:
        return
    try:
        body = _build_bulk_body(events)
        resp = await _event_client.post(
            "/_bulk",
            content=body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        if resp.status_code in (200, 201):
            result = resp.json()
            error_count = sum(1 for item in result.get("items", []) if item.get("index", {}).get("error"))
            ok_count = len(events) - error_count
            if ok_count > 0:
                metrics.inc_by("events_emitted", ok_count)
            if error_count > 0:
                metrics.inc_by("events_failed", error_count)
                logger.warning("Bulk write: %d/%d events had errors", error_count, len(events))
        else:
            logger.warning("Bulk write failed: %s %s", resp.status_code, resp.text[:200])
            metrics.inc_by("events_failed", len(events))
    except httpx.RequestError as exc:
        logger.warning("Bulk write failed: %s", exc)
        metrics.inc_by("events_failed", len(events))


async def _bulk_writer_loop() -> None:
    """Background loop: drain queue and flush in batches.

    Shutdown is signalled by placing _STOP_SENTINEL in the queue (not by
    task cancellation). This guarantees the final flush completes without
    being interrupted by CancelledError.
    """
    buffer: list[dict] = []
    stopping = False

    while not stopping:
        try:
            # Wait for the first event or flush interval timeout
            try:
                item = await asyncio.wait_for(
                    _event_queue.get(), timeout=BULK_FLUSH_INTERVAL
                )
                if item is _STOP_SENTINEL:
                    stopping = True
                else:
                    buffer.append(item)
            except asyncio.TimeoutError:
                pass

            # Drain remaining events from queue without blocking
            while not stopping and len(buffer) < BULK_FLUSH_SIZE:
                try:
                    item = _event_queue.get_nowait()
                    if item is _STOP_SENTINEL:
                        stopping = True
                    else:
                        buffer.append(item)
                except asyncio.QueueEmpty:
                    break

            # Flush if we have events
            if buffer:
                await _flush_events(buffer)
                buffer = []

        except Exception:
            logger.exception("Unexpected error in bulk writer loop")
            buffer = []

    # Shutdown: drain any remaining events and flush
    while not _event_queue.empty():
        try:
            item = _event_queue.get_nowait()
            if item is not _STOP_SENTINEL:
                buffer.append(item)
        except asyncio.QueueEmpty:
            break
    if buffer:
        await _flush_events(buffer)


def start_bulk_writer() -> None:
    """Start the background bulk writer. Call from within an async context."""
    global _event_queue, _bulk_writer_task
    _event_queue = asyncio.Queue(maxsize=BULK_QUEUE_SIZE)
    loop = asyncio.get_running_loop()
    _bulk_writer_task = loop.create_task(_bulk_writer_loop())
    logger.info(
        "Bulk writer started (flush_size=%d, flush_interval=%.1fs, queue_size=%d)",
        BULK_FLUSH_SIZE, BULK_FLUSH_INTERVAL, BULK_QUEUE_SIZE,
    )


async def stop_bulk_writer() -> None:
    """Stop the bulk writer gracefully, flushing remaining events."""
    global _bulk_writer_task
    if _bulk_writer_task is not None and _event_queue is not None:
        # Signal the writer to stop via sentinel (not cancellation)
        try:
            _event_queue.put_nowait(_STOP_SENTINEL)
        except asyncio.QueueFull:
            # Queue is full — cancel as fallback
            _bulk_writer_task.cancel()
        await _bulk_writer_task
        _bulk_writer_task = None
    logger.info("Bulk writer stopped")


def emit_event_background(event: dict) -> None:
    """Enqueue an event for bulk writing. Drops if queue is full."""
    if _event_queue is None:
        logger.debug("Bulk writer not started — dropping event")
        return
    try:
        _event_queue.put_nowait(event)
    except asyncio.QueueFull:
        metrics.inc("events_dropped")
        logger.debug("Event queue full — dropping event")


async def emit_event(event: dict) -> None:
    """Write a single event directly (bypasses bulk writer). Used for testing."""
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


async def close_event_client() -> None:
    """Close the event client. Called during gateway shutdown."""
    await stop_bulk_writer()
    await _event_client.aclose()
