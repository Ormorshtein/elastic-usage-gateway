"""
Heat analyzer — reads usage events and computes index-level and field-level heat.

Formulas:
  index_heat = total_operations / time_window_hours
  field_heat = field_references / total_field_references_in_index

Report is grouped by `index_group` (alias/data stream), with nested
concrete indices and per-index field breakdowns.

Thresholds are configurable via config.py.
"""

from __future__ import annotations
import logging
from collections import defaultdict

import httpx

from config import (
    ES_HOST, USAGE_INDEX, ANALYZER_TIMEOUT,
    INDEX_HEAT_HOT, INDEX_HEAT_WARM, INDEX_HEAT_COLD,
    FIELD_HEAT_HOT, FIELD_HEAT_WARM, FIELD_HEAT_COLD,
)

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=ANALYZER_TIMEOUT)

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


def _recommend_index(tier: str, ops_per_hour: float) -> list[str]:
    """Generate actionable recommendations for an index based on its tier."""
    recs = []
    if tier == "frozen":
        recs.append("Very low usage — consider freezing this index or reducing replicas to save resources")
    elif tier == "cold":
        recs.append("Low usage — consider moving to a cold storage tier or reducing replicas")
    elif tier == "hot":
        recs.append("High usage — ensure adequate replicas and heap allocation for this index")
    return recs


def _recommend_field(field_name: str, tier: str, cats: dict[str, int]) -> str | None:
    """Generate a recommendation for a single field based on its usage pattern."""
    if tier == "unused":
        return f"Field '{field_name}' is never referenced — consider setting 'index: false' to save disk and indexing time"

    if tier == "cold":
        return f"Field '{field_name}' is rarely used — consider setting 'doc_values: false' if not needed for sorting/aggregation"

    # Check if a field is only sourced (returned in results) but never queried/filtered/aggregated
    active_uses = cats.get("queried", 0) + cats.get("filtered", 0) + cats.get("aggregated", 0) + cats.get("sorted", 0)
    if active_uses == 0 and cats.get("sourced", 0) > 0:
        return f"Field '{field_name}' is only fetched in _source, never queried — consider 'index: false' to save indexing cost"

    # Aggregated/sorted fields should have doc_values (keyword/numeric types do by default)
    if cats.get("aggregated", 0) > 0 or cats.get("sorted", 0) > 0:
        return f"Field '{field_name}' is used for aggregation/sorting — ensure doc_values is enabled and type is keyword or numeric"

    return None


def _compute_index_heat(bucket: dict, time_window_hours: float) -> dict:
    """Compute heat report for a single concrete index bucket.

    Expected bucket structure (from ES terms aggregation):
        {
            "key": "index-name",
            "doc_count": 42,
            "field_queried":    {"buckets": [{"key": "title", "doc_count": 10}, ...]},
            "field_filtered":   {"buckets": [...]},
            "field_aggregated": {"buckets": [...]},
            "field_sorted":     {"buckets": [...]},
            "field_sourced":    {"buckets": [...]},
            "field_written":    {"buckets": [...]},
        }

    Each field_* sub-aggregation is a terms agg on the corresponding
    fields.* keyword array from the usage event document.
    """
    total_ops = bucket["doc_count"]
    ops_per_hour = total_ops / max(time_window_hours, 0.01)
    tier = _index_tier(ops_per_hour)

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
    field_recommendations = []
    for field_name, cats in sorted(field_counts.items()):
        field_total = sum(cats.values())
        proportion = field_total / max(total_field_refs, 1)
        field_t = _field_tier(proportion)
        fields_report[field_name] = {
            "heat": round(proportion, 4),
            "tier": field_t,
            **cats,
        }
        rec = _recommend_field(field_name, field_t, cats)
        if rec:
            field_recommendations.append(rec)

    index_recommendations = _recommend_index(tier, ops_per_hour)
    index_recommendations.extend(field_recommendations)

    return {
        "heat_score": round(ops_per_hour, 2),
        "tier": tier,
        "total_operations": total_ops,
        "fields": fields_report,
        "recommendations": index_recommendations,
    }


def _compute_index_heat_weighted(bucket: dict, time_window_hours: float) -> dict:
    """Compute response-time-weighted field heat for a single concrete index bucket.

    Same structure as _compute_index_heat, but instead of counting how many
    events reference each field, it sums the response_time_ms of those events.
    A field involved in slow queries ranks higher than one involved in many
    fast queries.

    Expected bucket structure: same as _compute_index_heat, but each field
    bucket also contains:
        {"total_response_time": {"value": 12345.6}}
    """
    field_times: dict[str, dict[str, float]] = defaultdict(lambda: {
        cat: 0.0 for cat in FIELD_CATEGORIES
    })
    total_time_all_fields = 0.0

    for category in FIELD_CATEGORIES:
        agg_key = f"field_{category}"
        for fb in bucket.get(agg_key, {}).get("buckets", []):
            field_name = fb["key"]
            resp_time = fb.get("total_response_time", {}).get("value", 0.0)
            field_times[field_name][category] = resp_time
            total_time_all_fields += resp_time

    fields_report = {}
    field_recommendations = []
    for field_name, cats in sorted(field_times.items()):
        field_total_time = sum(cats.values())
        proportion = field_total_time / max(total_time_all_fields, 0.001)
        field_t = _field_tier(proportion)
        fields_report[field_name] = {
            "heat": round(proportion, 4),
            "tier": field_t,
            "total_time_ms": round(field_total_time, 1),
            **{k: round(v, 1) for k, v in cats.items()},
        }
        rec = _recommend_field(field_name, field_t, cats)
        if rec:
            field_recommendations.append(rec)

    return {
        "total_response_time_ms": round(total_time_all_fields, 1),
        "fields": fields_report,
        "recommendations": field_recommendations,
    }


# Field sub-aggregations shared by both query structures.
# Each field terms bucket includes a sum of response_time_ms so we can compute
# both count-based and time-weighted field heat from the same query.
_FIELD_SUB_AGGS = {
    f"field_{cat}": {
        "terms": {"field": f"fields.{cat}", "size": 500},
        "aggs": {"total_response_time": {"sum": {"field": "response_time_ms"}}},
    }
    for cat in FIELD_CATEGORIES
}


async def compute_heat(time_window_hours: float = 24.0,
                       index_group: str | None = None) -> dict:
    """
    Compute heat report grouped by index_group, with nested concrete indices.

    Returns a structured dict suitable for JSON response.
    """
    # Build query
    must_clauses = [
        {"range": {"timestamp": {"gte": f"now-{int(time_window_hours)}h"}}}
    ]
    if index_group:
        must_clauses.append({"term": {"index_group": index_group}})

    query = {
        "size": 0,
        "query": {"bool": {"must": must_clauses}},
        "aggs": {
            "by_group": {
                "terms": {
                    "field": "index_group",
                    "size": 100,
                },
                "aggs": {
                    "by_index": {
                        "terms": {
                            "field": "index",
                            "size": 1000,
                        },
                        "aggs": _FIELD_SUB_AGGS,
                    },
                    "lookback_avg": {"avg": {"field": "lookback_seconds"}},
                    "lookback_max": {"max": {"field": "lookback_seconds"}},
                    "lookback_percentiles": {
                        "percentiles": {
                            "field": "lookback_seconds",
                            "percents": [50],
                        },
                    },
                    "lookback_count": {"value_count": {"field": "lookback_seconds"}},
                },
            },
        },
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
    groups = {}
    summary_by_tier: dict[str, list[str]] = {"hot": [], "warm": [], "cold": [], "frozen": []}

    for group_bucket in data.get("aggregations", {}).get("by_group", {}).get("buckets", []):
        group_name = group_bucket["key"]
        group_total_ops = group_bucket["doc_count"]
        group_ops_per_hour = group_total_ops / max(time_window_hours, 0.01)
        group_tier = _index_tier(group_ops_per_hour)
        summary_by_tier[group_tier].append(group_name)

        # Process concrete indices within this group
        indices = {}
        group_recommendations = _recommend_index(group_tier, group_ops_per_hour)

        for index_bucket in group_bucket.get("by_index", {}).get("buckets", []):
            index_name = index_bucket["key"]
            index_report = _compute_index_heat(index_bucket, time_window_hours)
            weighted_report = _compute_index_heat_weighted(index_bucket, time_window_hours)
            index_report["fields_by_response_time"] = weighted_report["fields"]
            index_report["response_time_recommendations"] = weighted_report["recommendations"]
            index_report["total_response_time_ms"] = weighted_report["total_response_time_ms"]
            indices[index_name] = index_report
            # Bubble up field recommendations from concrete indices
            group_recommendations.extend(index_report.get("recommendations", []))

        # Parse lookback stats
        lb_avg = group_bucket.get("lookback_avg", {}).get("value")
        lb_max = group_bucket.get("lookback_max", {}).get("value")
        lb_p50_vals = group_bucket.get("lookback_percentiles", {}).get("values", {})
        lb_p50 = lb_p50_vals.get("50.0")
        lb_with = int(group_bucket.get("lookback_count", {}).get("value", 0))

        groups[group_name] = {
            "heat_score": round(group_ops_per_hour, 2),
            "tier": group_tier,
            "total_operations": group_total_ops,
            "indices": indices,
            "lookback": {
                "avg_seconds": round(lb_avg, 1) if lb_avg is not None else None,
                "max_seconds": round(lb_max, 1) if lb_max is not None else None,
                "p50_seconds": round(lb_p50, 1) if lb_p50 is not None else None,
                "queries_with_lookback": lb_with,
                "queries_total": group_total_ops,
            },
            "recommendations": group_recommendations,
        }

    # Remove empty tiers from summary
    summary_by_tier = {k: v for k, v in summary_by_tier.items() if v}

    return {
        "time_window": f"last_{int(time_window_hours)}h",
        "scoring": {
            "fields": {
                "method": "count",
                "description": (
                    "Field heat based on reference count. "
                    "heat = field references / total field references in the index. "
                    "Shows which fields are queried most often."
                ),
            },
            "fields_by_response_time": {
                "method": "response_time_weighted",
                "description": (
                    "Field heat based on total response time. "
                    "heat = sum(response_time_ms for events referencing field) / "
                    "total response_time_ms across all field references. "
                    "Fields involved in slow queries rank higher — "
                    "optimizing these saves the most cluster time."
                ),
            },
        },
        "summary": {
            "total_groups": len(groups),
            "by_tier": summary_by_tier,
        },
        "groups": groups,
    }


async def compute_query_patterns(
    time_window_hours: float = 24.0,
    index_group: str | None = None,
) -> dict:
    """Query pattern report grouped by structural template.

    Groups events by query_template_hash and returns execution count,
    response time stats, and a sample template text for each pattern.
    """
    must_clauses: list[dict] = [
        {"range": {"timestamp": {"gte": f"now-{int(time_window_hours)}h"}}},
        {"exists": {"field": "query_template_hash"}},
    ]
    if index_group:
        must_clauses.append({"term": {"index_group": index_group}})

    query = {
        "size": 0,
        "query": {"bool": {"must": must_clauses}},
        "aggs": {
            "by_template": {
                "terms": {
                    "field": "query_template_hash",
                    "size": 100,
                    "order": {"_count": "desc"},
                },
                "aggs": {
                    "total_response_time": {"sum": {"field": "response_time_ms"}},
                    "avg_response_time": {"avg": {"field": "response_time_ms"}},
                    "index_groups": {"terms": {"field": "index_group", "size": 20}},
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": ["query_template_text", "operation"],
                        }
                    },
                },
            },
        },
    }

    try:
        resp = await _client.post(f"/{USAGE_INDEX}/_search", json=query)
        if resp.status_code != 200:
            logger.error("Query patterns query failed: %s %s", resp.status_code, resp.text[:300])
            return {"error": "Failed to query usage events", "status": resp.status_code}
        data = resp.json()
    except httpx.RequestError as exc:
        logger.error("Query patterns request failed: %s", exc)
        return {"error": str(exc)}

    patterns = []
    total_executions = 0
    total_response_time = 0.0

    for bucket in data.get("aggregations", {}).get("by_template", {}).get("buckets", []):
        exec_count = bucket["doc_count"]
        total_time = bucket["total_response_time"]["value"]
        avg_time = bucket["avg_response_time"]["value"]

        groups = [g["key"] for g in bucket.get("index_groups", {}).get("buckets", [])]

        sample_hits = bucket.get("sample", {}).get("hits", {}).get("hits", [])
        template_text = None
        operation = None
        if sample_hits:
            src = sample_hits[0].get("_source", {})
            template_text = src.get("query_template_text")
            operation = src.get("operation")

        patterns.append({
            "template_hash": bucket["key"],
            "template_text": template_text,
            "execution_count": exec_count,
            "total_response_time_ms": round(total_time, 1),
            "avg_response_time_ms": round(avg_time, 1),
            "index_groups": groups,
            "operation": operation,
        })

        total_executions += exec_count
        total_response_time += total_time

    return {
        "time_window": f"last_{int(time_window_hours)}h",
        "summary": {
            "unique_templates": len(patterns),
            "total_executions": total_executions,
            "total_response_time_ms": round(total_response_time, 1),
        },
        "patterns": patterns,
    }


async def close_analyzer_client() -> None:
    """Close the analyzer client. Called during gateway shutdown."""
    await _client.aclose()
