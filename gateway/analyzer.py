"""
Heat analyzer — reads usage events and computes index-level and field-level heat.

Formulas:
  index_heat = total_operations / time_window_hours
  field_heat = field_references / total_field_references_in_index

Thresholds are configurable via config.py.
"""

from __future__ import annotations
import logging
from collections import defaultdict

import httpx

from config import (
    ES_HOST, USAGE_INDEX,
    INDEX_HEAT_HOT, INDEX_HEAT_WARM, INDEX_HEAT_COLD,
    FIELD_HEAT_HOT, FIELD_HEAT_WARM, FIELD_HEAT_COLD,
)

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=30.0)

# Field usage categories we track
FIELD_CATEGORIES = ("queried", "filtered", "aggregated", "sorted", "sourced", "written")


def _index_tier(ops_per_hour: float) -> str:
    if ops_per_hour > INDEX_HEAT_HOT:
        return "hot"
    if ops_per_hour > INDEX_HEAT_WARM:
        return "warm"
    if ops_per_hour > INDEX_HEAT_COLD:
        return "cold"
    return "frozen"


def _field_tier(proportion: float) -> str:
    if proportion >= FIELD_HEAT_HOT:
        return "hot"
    if proportion >= FIELD_HEAT_WARM:
        return "warm"
    if proportion >= FIELD_HEAT_COLD:
        return "cold"
    return "unused"


async def compute_heat(time_window_hours: float = 24.0) -> dict:
    """
    Compute heat report for all indices observed in the usage events.

    Returns a structured dict suitable for JSON response.
    """
    # Query all usage events within the time window
    query = {
        "size": 0,
        "query": {
            "range": {
                "timestamp": {
                    "gte": f"now-{int(time_window_hours)}h",
                }
            }
        },
        "aggs": {
            "by_index": {
                "terms": {
                    "field": "index",
                    "size": 1000,
                },
                "aggs": {
                    "field_queried":    {"terms": {"field": "fields.queried",    "size": 500}},
                    "field_filtered":   {"terms": {"field": "fields.filtered",   "size": 500}},
                    "field_aggregated": {"terms": {"field": "fields.aggregated", "size": 500}},
                    "field_sorted":     {"terms": {"field": "fields.sorted",     "size": 500}},
                    "field_sourced":    {"terms": {"field": "fields.sourced",    "size": 500}},
                    "field_written":    {"terms": {"field": "fields.written",    "size": 500}},
                }
            }
        }
    }

    try:
        resp = await _client.post(f"/{USAGE_INDEX}/_search", json=query)
        if resp.status_code != 200:
            logger.error("Heat query failed: %s %s", resp.status_code, resp.text[:300])
            return {"error": "Failed to query usage events", "status": resp.status_code}
        data = resp.json()
    except httpx.RequestError as exc:
        logger.error("Heat query failed: %s", exc)
        return {"error": str(exc)}

    # Parse aggregation results
    indices = {}
    for bucket in data.get("aggregations", {}).get("by_index", {}).get("buckets", []):
        index_name = bucket["key"]
        total_ops = bucket["doc_count"]
        ops_per_hour = total_ops / max(time_window_hours, 0.01)

        # Collect field counts across all categories
        field_counts: dict[str, dict[str, int]] = defaultdict(lambda: {
            cat: 0 for cat in FIELD_CATEGORIES
        })
        total_field_refs = 0

        for category in FIELD_CATEGORIES:
            agg_key = f"field_{category}"
            for fb in bucket.get(agg_key, {}).get("buckets", []):
                field_name = fb["key"]
                count = fb["doc_count"]
                field_counts[field_name][category] = count
                total_field_refs += count

        # Compute per-field heat
        fields_report = {}
        for field_name, cats in sorted(field_counts.items()):
            field_total = sum(cats.values())
            proportion = field_total / max(total_field_refs, 1)
            fields_report[field_name] = {
                "heat": round(proportion, 4),
                "tier": _field_tier(proportion),
                **cats,
            }

        indices[index_name] = {
            "heat_score": round(ops_per_hour, 2),
            "tier": _index_tier(ops_per_hour),
            "total_operations": total_ops,
            "fields": fields_report,
        }

    return {
        "time_window": f"last_{int(time_window_hours)}h",
        "indices": indices,
    }
