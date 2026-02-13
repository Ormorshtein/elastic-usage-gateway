"""
Mapping diff engine — compares index mappings against actual field usage.

Periodically fetches ES index mappings, queries .usage-events for field
references, classifies each mapped field (active, unused, write_only,
sourced_only), and writes results to the .mapping-diff index.

Results are consumed exclusively via Kibana dashboards — no JSON API.

Lifecycle: Start via start_diff_loop() in the lifespan hook. The loop
runs every MAPPING_DIFF_REFRESH_INTERVAL seconds. Each tick processes
all known index groups from the metadata cache.
"""

from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from config import (
    ES_HOST, EVENT_TIMEOUT, USAGE_INDEX,
    MAPPING_DIFF_REFRESH_INTERVAL, MAPPING_DIFF_LOOKBACK_HOURS,
)
from gateway import metadata as metadata_mod
from gateway import metrics

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

MAPPING_DIFF_INDEX = ".mapping-diff"

FIELD_CATEGORIES = ("queried", "filtered", "aggregated", "sorted", "sourced", "written")

# Types where ES defaults doc_values=false
_NO_DOC_VALUES_BY_DEFAULT = {"text", "annotated_text"}

MAPPING_DIFF_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":            {"type": "date"},
            "index_group":          {"type": "keyword"},
            "field_name":           {"type": "keyword"},
            "mapped_type":          {"type": "keyword"},
            "is_indexed":           {"type": "boolean"},
            "has_doc_values":       {"type": "boolean"},
            "total_references":     {"type": "long"},
            "last_seen":            {"type": "date"},
            "last_seen_queried":    {"type": "date"},
            "last_seen_filtered":   {"type": "date"},
            "last_seen_aggregated": {"type": "date"},
            "last_seen_sorted":     {"type": "date"},
            "last_seen_sourced":    {"type": "date"},
            "last_seen_written":    {"type": "date"},
            "count_queried":        {"type": "long"},
            "count_filtered":       {"type": "long"},
            "count_aggregated":     {"type": "long"},
            "count_sorted":         {"type": "long"},
            "count_sourced":        {"type": "long"},
            "count_written":        {"type": "long"},
            "classification":       {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}


# ---------------------------------------------------------------------------
# Pure functions (no I/O, fully testable)
# ---------------------------------------------------------------------------

def flatten_mapping(mapping_properties: dict) -> list[dict]:
    """Flatten nested ES mapping properties into a list of field descriptors.

    Args:
        mapping_properties: The "properties" dict from an ES mapping response.

    Returns:
        List of dicts with keys: field_name, mapped_type, is_indexed, has_doc_values.
    """
    results: list[dict] = []
    _walk_properties(mapping_properties, prefix="", results=results)
    return results


def _walk_properties(properties: dict, prefix: str, results: list[dict]) -> None:
    """Recursively walk mapping properties, building dotted field paths."""
    for name, field_def in properties.items():
        if not isinstance(field_def, dict):
            continue

        full_name = f"{prefix}.{name}" if prefix else name
        field_type = field_def.get("type")

        # Concrete field with a type
        if field_type:
            results.append({
                "field_name": full_name,
                "mapped_type": field_type,
                "is_indexed": bool(field_def.get("index", True)),
                "has_doc_values": bool(field_def.get(
                    "doc_values",
                    field_type not in _NO_DOC_VALUES_BY_DEFAULT,
                )),
            })

        # Multi-fields: "fields": {"keyword": {"type": "keyword"}}
        multi_fields = field_def.get("fields")
        if isinstance(multi_fields, dict):
            for sub_name, sub_def in multi_fields.items():
                if isinstance(sub_def, dict) and "type" in sub_def:
                    sub_type = sub_def["type"]
                    results.append({
                        "field_name": f"{full_name}.{sub_name}",
                        "mapped_type": sub_type,
                        "is_indexed": bool(sub_def.get("index", True)),
                        "has_doc_values": bool(sub_def.get(
                            "doc_values",
                            sub_type not in _NO_DOC_VALUES_BY_DEFAULT,
                        )),
                    })

        # Nested objects: "properties": {"created_at": {"type": "date"}}
        sub_properties = field_def.get("properties")
        if isinstance(sub_properties, dict):
            _walk_properties(sub_properties, prefix=full_name, results=results)


def build_usage_aggregation_query(index_group: str, lookback_hours: int) -> dict:
    """Build the ES query to aggregate field usage from .usage-events.

    For each of the 6 field categories, creates a terms agg on
    fields.{category} with a max(timestamp) sub-agg for last_seen.
    """
    aggs = {}
    for category in FIELD_CATEGORIES:
        aggs[f"usage_{category}"] = {
            "terms": {
                "field": f"fields.{category}",
                "size": 1000,
            },
            "aggs": {
                "last_seen": {"max": {"field": "timestamp"}}
            },
        }

    return {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"index_group": index_group}},
                    {"range": {"timestamp": {"gte": f"now-{lookback_hours}h"}}},
                ]
            }
        },
        "aggs": aggs,
    }


def _parse_usage_response(response: dict) -> dict[str, dict]:
    """Parse ES aggregation response into per-field usage data.

    Returns dict mapping field_name -> {count_queried, last_seen_queried, ...}.
    """
    field_usage: dict[str, dict] = {}
    aggs = response.get("aggregations", {})

    for category in FIELD_CATEGORIES:
        buckets = aggs.get(f"usage_{category}", {}).get("buckets", [])
        for bucket in buckets:
            field_name = bucket["key"]
            count = bucket["doc_count"]
            last_seen = bucket.get("last_seen", {}).get("value_as_string")

            if field_name not in field_usage:
                field_usage[field_name] = {}
                for cat in FIELD_CATEGORIES:
                    field_usage[field_name][f"count_{cat}"] = 0
                    field_usage[field_name][f"last_seen_{cat}"] = None

            field_usage[field_name][f"count_{category}"] = count
            field_usage[field_name][f"last_seen_{category}"] = last_seen

    return field_usage


def classify_field(usage: dict | None) -> str:
    """Classify a field based on its usage pattern.

    Rules (checked in order):
    1. "unused"       — zero references across all 6 categories
    2. "active"       — queried/filtered/aggregated/sorted > 0
    3. "sourced_only" — only sourced (fetched in _source but never searched)
    4. "write_only"   — only written, never read in any way
    """
    if usage is None:
        return "unused"

    total = sum(usage.get(f"count_{cat}", 0) for cat in FIELD_CATEGORIES)
    if total == 0:
        return "unused"

    read_count = (
        usage.get("count_queried", 0)
        + usage.get("count_filtered", 0)
        + usage.get("count_aggregated", 0)
        + usage.get("count_sorted", 0)
    )
    if read_count > 0:
        return "active"

    if usage.get("count_sourced", 0) > 0:
        return "sourced_only"

    if usage.get("count_written", 0) > 0:
        return "write_only"

    return "unused"


def build_diff_docs(
    index_group: str,
    mapping_fields: list[dict],
    field_usage: dict[str, dict],
    timestamp: str,
) -> list[dict]:
    """Build .mapping-diff documents by merging mapping fields with usage data."""
    docs = []
    for field_info in mapping_fields:
        field_name = field_info["field_name"]
        usage = field_usage.get(field_name)
        classification = classify_field(usage)

        # Overall last_seen = max across all categories
        last_seen = None
        if usage:
            seen_dates = [
                usage[f"last_seen_{cat}"]
                for cat in FIELD_CATEGORIES
                if usage.get(f"last_seen_{cat}") is not None
            ]
            if seen_dates:
                last_seen = max(seen_dates)

        total_refs = 0
        if usage:
            total_refs = sum(usage.get(f"count_{cat}", 0) for cat in FIELD_CATEGORIES)

        doc: dict = {
            "timestamp": timestamp,
            "index_group": index_group,
            "field_name": field_name,
            "mapped_type": field_info["mapped_type"],
            "is_indexed": field_info["is_indexed"],
            "has_doc_values": field_info["has_doc_values"],
            "total_references": total_refs,
            "last_seen": last_seen,
            "classification": classification,
        }

        for cat in FIELD_CATEGORIES:
            doc[f"count_{cat}"] = usage.get(f"count_{cat}", 0) if usage else 0
            doc[f"last_seen_{cat}"] = usage.get(f"last_seen_{cat}") if usage else None

        docs.append(doc)

    return docs


# ---------------------------------------------------------------------------
# Async functions (ES I/O)
# ---------------------------------------------------------------------------

async def ensure_diff_index() -> None:
    """Create the .mapping-diff index if it doesn't exist."""
    try:
        resp = await _client.head(f"/{MAPPING_DIFF_INDEX}")
        if resp.status_code == 200:
            return
        resp = await _client.put(
            f"/{MAPPING_DIFF_INDEX}",
            json=MAPPING_DIFF_INDEX_MAPPING,
        )
        if resp.status_code in (200, 201):
            logger.info("Created mapping diff index: %s", MAPPING_DIFF_INDEX)
        else:
            logger.warning(
                "Failed to create mapping diff index: %s %s",
                resp.status_code, resp.text[:200],
            )
    except httpx.RequestError as exc:
        logger.warning("Could not ensure mapping diff index exists: %s", exc)


async def fetch_mapping_for_group(index_group: str) -> list[dict] | None:
    """Fetch and flatten the mapping for an index group.

    Uses the first concrete index from the metadata cache. If the group name
    itself is a standalone index, uses it directly.
    """
    groups = metadata_mod.get_groups()
    concrete_indices = groups.get(index_group, [])

    target = concrete_indices[0] if concrete_indices else index_group

    try:
        resp = await _client.get(f"/{target}/_mapping")
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch mapping for %s (via %s): %s",
                index_group, target, resp.status_code,
            )
            return None

        data = resp.json()
        # Response: {"index_name": {"mappings": {"properties": {...}}}}
        for _index_name, index_data in data.items():
            properties = index_data.get("mappings", {}).get("properties", {})
            return flatten_mapping(properties)

        return None
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch mapping for %s: %s", index_group, exc)
        return None


async def fetch_usage_for_group(
    index_group: str, lookback_hours: int,
) -> dict[str, dict] | None:
    """Query .usage-events and return per-field usage data."""
    query = build_usage_aggregation_query(index_group, lookback_hours)

    try:
        resp = await _client.post(f"/{USAGE_INDEX}/_search", json=query)
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch usage for %s: %s", index_group, resp.status_code,
            )
            return None

        return _parse_usage_response(resp.json())
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch usage for %s: %s", index_group, exc)
        return None


async def write_diff_docs(index_group: str, docs: list[dict]) -> None:
    """Write mapping diff documents to ES (delete-and-rewrite)."""
    if not docs:
        return

    # Delete existing docs for this group
    try:
        await _client.post(
            f"/{MAPPING_DIFF_INDEX}/_delete_by_query",
            json={"query": {"term": {"index_group": index_group}}},
            params={"refresh": "false"},
        )
    except httpx.RequestError as exc:
        logger.warning("Failed to delete old diff docs for %s: %s", index_group, exc)

    # Bulk-write new docs
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": MAPPING_DIFF_INDEX}}))
        lines.append(json.dumps(doc, default=str))
    bulk_body = "\n".join(lines) + "\n"

    try:
        resp = await _client.post(
            "/_bulk",
            content=bulk_body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
            params={"refresh": "true"},
        )
        if resp.status_code in (200, 201):
            result = resp.json()
            error_count = sum(
                1 for item in result.get("items", [])
                if item.get("index", {}).get("error")
            )
            if error_count:
                logger.warning(
                    "Mapping diff bulk write for %s: %d/%d errors",
                    index_group, error_count, len(docs),
                )
        else:
            logger.warning(
                "Mapping diff bulk write failed for %s: %s",
                index_group, resp.status_code,
            )
    except httpx.RequestError as exc:
        logger.warning("Mapping diff bulk write failed for %s: %s", index_group, exc)


async def refresh() -> None:
    """Recompute the mapping diff for all known index groups."""
    groups = metadata_mod.get_groups()
    if not groups:
        logger.debug("No index groups known — skipping mapping diff refresh")
        return

    timestamp = datetime.now(timezone.utc).isoformat()
    processed = 0

    for index_group in groups:
        if index_group.startswith("."):
            continue

        mapping_fields = await fetch_mapping_for_group(index_group)
        if mapping_fields is None:
            continue

        field_usage = await fetch_usage_for_group(
            index_group, MAPPING_DIFF_LOOKBACK_HOURS,
        )
        if field_usage is None:
            field_usage = {}

        docs = build_diff_docs(index_group, mapping_fields, field_usage, timestamp)
        await write_diff_docs(index_group, docs)
        processed += 1

    metrics.inc("mapping_diff_refresh_ok")
    logger.info("Mapping diff refreshed: %d groups processed", processed)


# ---------------------------------------------------------------------------
# Background lifecycle (same pattern as metadata.py)
# ---------------------------------------------------------------------------

async def _diff_loop() -> None:
    """Background loop that refreshes the mapping diff periodically."""
    while True:
        try:
            await refresh()
        except Exception:
            logger.exception("Mapping diff refresh failed")
            metrics.inc("mapping_diff_refresh_failed")
        await asyncio.sleep(MAPPING_DIFF_REFRESH_INTERVAL)


def start_diff_loop() -> None:
    """Start the background mapping diff refresh loop. Call from async context."""
    loop = asyncio.get_running_loop()
    loop.create_task(_diff_loop())
    logger.info(
        "Mapping diff loop started (interval=%ds, lookback=%dh)",
        MAPPING_DIFF_REFRESH_INTERVAL, MAPPING_DIFF_LOOKBACK_HOURS,
    )


async def close_diff_client() -> None:
    """Close the mapping diff client. Called during gateway shutdown."""
    await _client.aclose()
