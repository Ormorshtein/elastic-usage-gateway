"""
Heat analyzer — reads usage events and computes index-level and field-level heat.

Two-phase approach:
  Phase 1: Query raw events via ES aggregation (recent data, within retention window)
  Phase 2: Read rollup documents (older data, pre-aggregated summaries)
  Merge both sources into a unified heat report.

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


# Field sub-aggregations shared by both query structures
_FIELD_SUB_AGGS = {
    f"field_{cat}": {"terms": {"field": f"fields.{cat}", "size": 500}}
    for cat in FIELD_CATEGORIES
}


async def _query_raw_events(time_window_hours: float,
                            index_group: str | None = None) -> dict | None:
    """Phase 1: Query raw events via ES aggregation.

    Returns the parsed JSON response, or None on failure.
    Filters for type:"raw" or legacy events (no type field).
    """
    must_clauses = [
        {"range": {"timestamp": {"gte": f"now-{int(time_window_hours)}h"}}},
    ]
    if index_group:
        must_clauses.append({"term": {"index_group": index_group}})

    # Backward-compatible: match raw events and legacy events without type field
    type_filter = {
        "bool": {
            "should": [
                {"term": {"type": "raw"}},
                {"bool": {"must_not": {"exists": {"field": "type"}}}},
            ],
            "minimum_should_match": 1,
        }
    }
    must_clauses.append(type_filter)

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
            logger.error("Raw heat query failed: %s %s", resp.status_code, resp.text[:300])
            return None
        return resp.json()
    except httpx.RequestError as exc:
        logger.error("Raw heat query failed: %s", exc)
        return None


async def _query_rollup_docs(time_window_hours: float,
                             index_group: str | None = None) -> dict:
    """Phase 2: Read rollup documents and aggregate in Python.

    Returns a dict keyed by (group_name, index_name) with:
      - total_operations: float
      - field_counts: {field_name: {category: count}}
      - lookback_sum: float, lookback_max: float, lookback_count: int
    """
    must_clauses = [
        {"term": {"type": "rollup"}},
        {"range": {"window_start": {"gte": f"now-{int(time_window_hours)}h"}}},
    ]
    if index_group:
        must_clauses.append({"term": {"index_group": index_group}})

    rollup_data: dict[tuple[str, str], dict] = {}

    # Fetch rollup docs via scrolling (search_after)
    search_after = None
    while True:
        body: dict = {
            "size": 1000,
            "query": {"bool": {"must": must_clauses}},
            "sort": [{"window_start": "asc"}, {"_id": "asc"}],
            "_source": True,
        }
        if search_after:
            body["search_after"] = search_after

        try:
            resp = await _client.post(f"/{USAGE_INDEX}/_search", json=body)
            if resp.status_code != 200:
                logger.warning("Rollup heat query failed: %s", resp.status_code)
                break
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                src = hit["_source"]
                group_name = src.get("index_group", "_unknown")
                index_name = src.get("index", "_unknown")
                key = (group_name, index_name)

                if key not in rollup_data:
                    rollup_data[key] = {
                        "total_operations": 0.0,
                        "field_counts": defaultdict(lambda: {cat: 0 for cat in FIELD_CATEGORIES}),
                        "lookback_sum": 0.0,
                        "lookback_max": 0.0,
                        "lookback_count": 0,
                    }

                entry = rollup_data[key]
                entry["total_operations"] += src.get("total_operations", 0)

                # Merge field_usage
                for field_name, cats in src.get("field_usage", {}).items():
                    for cat in FIELD_CATEGORIES:
                        entry["field_counts"][field_name][cat] += cats.get(cat, 0)

                # Merge lookback
                entry["lookback_sum"] += src.get("lookback_sum_seconds", 0)
                entry["lookback_max"] = max(entry["lookback_max"], src.get("lookback_max_seconds", 0))
                entry["lookback_count"] += src.get("lookback_count", 0)

            search_after = hits[-1]["sort"]
            if len(hits) < 1000:
                break
        except httpx.RequestError as exc:
            logger.warning("Rollup heat query error: %s", exc)
            break

    return rollup_data


def merge_and_build_report(
    raw_data: dict | None,
    rollup_data: dict,
    time_window_hours: float,
) -> dict:
    """Merge raw aggregation results and rollup data into a unified heat report.

    This function constructs synthetic bucket dicts from merged data and feeds
    them into _compute_index_heat() — keeping that function unchanged.
    """
    # Step 1: Parse raw aggregation data into a merged structure
    # Key: (group_name, index_name) → merged counts
    merged: dict[tuple[str, str], dict] = {}
    group_lookback: dict[str, dict] = {}  # group → lookback stats from raw agg

    if raw_data:
        for group_bucket in raw_data.get("aggregations", {}).get("by_group", {}).get("buckets", []):
            group_name = group_bucket["key"]

            # Store raw lookback stats for this group
            lb_avg = group_bucket.get("lookback_avg", {}).get("value")
            lb_max = group_bucket.get("lookback_max", {}).get("value")
            lb_p50_vals = group_bucket.get("lookback_percentiles", {}).get("values", {})
            lb_p50 = lb_p50_vals.get("50.0")
            lb_count = int(group_bucket.get("lookback_count", {}).get("value", 0))
            raw_total = group_bucket["doc_count"]
            group_lookback[group_name] = {
                "lb_avg": lb_avg, "lb_max": lb_max, "lb_p50": lb_p50,
                "lb_count": lb_count, "raw_total": raw_total,
            }

            for index_bucket in group_bucket.get("by_index", {}).get("buckets", []):
                index_name = index_bucket["key"]
                key = (group_name, index_name)
                merged[key] = {
                    "total_ops": index_bucket["doc_count"],
                    "field_counts": {},
                }
                # Parse field aggregation buckets
                for cat in FIELD_CATEGORIES:
                    for fb in index_bucket.get(f"field_{cat}", {}).get("buckets", []):
                        fname = fb["key"]
                        if fname not in merged[key]["field_counts"]:
                            merged[key]["field_counts"][fname] = {c: 0 for c in FIELD_CATEGORIES}
                        merged[key]["field_counts"][fname][cat] = fb["doc_count"]

    # Step 2: Merge rollup data on top
    for (group_name, index_name), rdata in rollup_data.items():
        key = (group_name, index_name)
        if key not in merged:
            merged[key] = {"total_ops": 0, "field_counts": {}}

        merged[key]["total_ops"] += rdata["total_operations"]

        for fname, cats in rdata["field_counts"].items():
            if fname not in merged[key]["field_counts"]:
                merged[key]["field_counts"][fname] = {c: 0 for c in FIELD_CATEGORIES}
            for cat in FIELD_CATEGORIES:
                merged[key]["field_counts"][fname][cat] += cats.get(cat, 0)

        # Merge rollup lookback into group-level stats
        if group_name not in group_lookback:
            group_lookback[group_name] = {
                "lb_avg": None, "lb_max": None, "lb_p50": None,
                "lb_count": 0, "raw_total": 0,
            }
        gl = group_lookback[group_name]
        if rdata["lookback_max"] > 0:
            gl["lb_max"] = max(gl["lb_max"] or 0, rdata["lookback_max"])
        gl["lb_count"] += rdata["lookback_count"]
        gl["raw_total"] += rdata["total_operations"]

    # Step 3: Build synthetic buckets and compute heat via existing function
    # Group indices by their group name
    groups_indices: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for (group_name, index_name) in merged:
        groups_indices[group_name].append((group_name, index_name))

    groups = {}
    summary_by_tier: dict[str, list[str]] = {"hot": [], "warm": [], "cold": [], "frozen": []}

    for group_name, keys in groups_indices.items():
        group_total_ops = sum(merged[k]["total_ops"] for k in keys)
        group_ops_per_hour = group_total_ops / max(time_window_hours, 0.01)
        group_tier = _index_tier(group_ops_per_hour)
        summary_by_tier[group_tier].append(group_name)

        indices = {}
        group_recommendations = _recommend_index(group_tier, group_ops_per_hour)

        for key in keys:
            _, index_name = key
            mdata = merged[key]

            # Build a synthetic bucket dict matching what _compute_index_heat expects
            synthetic_bucket = {
                "key": index_name,
                "doc_count": mdata["total_ops"],
            }
            for cat in FIELD_CATEGORIES:
                buckets = []
                for fname, cats in mdata["field_counts"].items():
                    if cats.get(cat, 0) > 0:
                        buckets.append({"key": fname, "doc_count": cats[cat]})
                synthetic_bucket[f"field_{cat}"] = {"buckets": buckets}

            index_report = _compute_index_heat(synthetic_bucket, time_window_hours)
            indices[index_name] = index_report
            group_recommendations.extend(index_report.get("recommendations", []))

        # Lookback stats
        gl = group_lookback.get(group_name, {})
        lb_avg = gl.get("lb_avg")
        lb_max = gl.get("lb_max")
        lb_p50 = gl.get("lb_p50")
        lb_count = gl.get("lb_count", 0)

        groups[group_name] = {
            "heat_score": round(group_ops_per_hour, 2),
            "tier": group_tier,
            "total_operations": group_total_ops,
            "indices": indices,
            "lookback": {
                "avg_seconds": round(lb_avg, 1) if lb_avg is not None else None,
                "max_seconds": round(lb_max, 1) if lb_max is not None else None,
                "p50_seconds": round(lb_p50, 1) if lb_p50 is not None else None,
                "queries_with_lookback": lb_count,
                "queries_total": int(group_total_ops),
            },
            "recommendations": group_recommendations,
        }

    summary_by_tier = {k: v for k, v in summary_by_tier.items() if v}

    return {
        "time_window": f"last_{int(time_window_hours)}h",
        "summary": {
            "total_groups": len(groups),
            "by_tier": summary_by_tier,
        },
        "groups": groups,
    }


async def compute_heat(time_window_hours: float = 24.0,
                       index_group: str | None = None) -> dict:
    """
    Compute heat report grouped by index_group, with nested concrete indices.

    Two-phase: raw event aggregation + rollup document read, merged into a
    single report. Output format is identical to the pre-rollup version.
    """
    # Phase 1: Raw events
    raw_data = await _query_raw_events(time_window_hours, index_group)

    # Phase 2: Rollup documents
    rollup_data = await _query_rollup_docs(time_window_hours, index_group)

    # Check if both phases failed
    if raw_data is None and not rollup_data:
        return {"error": "Failed to query usage events"}

    return merge_and_build_report(raw_data, rollup_data, time_window_hours)


async def close_analyzer_client() -> None:
    """Close the analyzer client. Called during gateway shutdown."""
    await _client.aclose()
