"""
Index architecture recommendations engine — generates shard sizing,
settings audit, usage-based, and cluster health optimization advice.

Reads index metadata from ES APIs (_cat/indices, _cat/shards, _settings,
_mapping, _stats/segments) and query patterns from .usage-events, applies
15 decision rules (14 per-group + 1 cluster-level), and writes
recommendation documents to the .index-recommendations index.

Each recommendation includes:
- current_value: what we observed, with actual numbers
- why: explanation of the problem + Elastic best practice + impact
- how: concrete API calls and JSON snippets to fix it
- reference_url: direct link to Elastic documentation

Results are consumed via Kibana dashboards — no JSON API endpoint.

Lifecycle: Start via start_index_arch_loop() in the lifespan hook.
The loop runs every INDEX_ARCH_REFRESH_INTERVAL seconds.
"""

from __future__ import annotations
import asyncio
import json
import logging
import math
from datetime import datetime, timezone
from statistics import median

import httpx

from config import (
    ES_HOST, EVENT_TIMEOUT, USAGE_INDEX,
    INDEX_ARCH_REFRESH_INTERVAL, INDEX_ARCH_LOOKBACK_HOURS,
)
from gateway import metadata as metadata_mod
from gateway import metrics
from gateway.index_arch_text import TEMPLATES
from gateway.mapping_diff import flatten_mapping

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

INDEX_ARCH_INDEX = ".index-recommendations"

INDEX_ARCH_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":       {"type": "date"},
            "index_group":     {"type": "keyword"},
            "category":        {"type": "keyword"},
            "recommendation":  {"type": "keyword"},
            "severity":        {"type": "keyword"},
            "current_value":   {"type": "keyword", "index": False},
            "why":             {"type": "keyword", "index": False},
            "how":             {"type": "keyword", "index": False},
            "reference_url":   {"type": "keyword", "index": False},
            "breaking_change": {"type": "boolean"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(value, default: int = 0) -> int:
    """Convert a value to int, handling strings from _cat API and None."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    """Convert a value to float, handling strings, None, and NaN."""
    if value is None:
        return default
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


def _fmt_bytes(n: int) -> str:
    """Format bytes as a human-readable string (e.g., 1.5GB, 200MB)."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}GB"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.0f}MB"
    return f"{n / 1_000:.0f}KB"


def _parse_bytes_string(value: str | None) -> int | None:
    """Parse an ES byte-size string like '5gb', '500mb' to bytes.

    Returns None if the value is None or unparseable.
    """
    if not value:
        return None
    value = value.strip().lower()
    multipliers = {
        "tb": 1_000_000_000_000, "t": 1_000_000_000_000,
        "gb": 1_000_000_000, "g": 1_000_000_000,
        "mb": 1_000_000, "m": 1_000_000,
        "kb": 1_000, "k": 1_000,
        "b": 1,
    }
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            num_part = value[:-len(suffix)]
            try:
                return int(float(num_part) * mult)
            except (ValueError, TypeError):
                return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _build_rec(
    name: str, *, current_value: str, severity: str | None = None, **fmt,
) -> dict:
    """Assemble a recommendation dict from a template and dynamic values.

    Args:
        name: Key in TEMPLATES (e.g. "shard_too_small").
        current_value: Always computed by the rule function.
        severity: Override the template's default severity.
        **fmt: Values substituted into Template strings ($var).
    """
    t = TEMPLATES[name]
    why = t["why"]
    how = t["how"]
    return {
        "category": t["category"],
        "recommendation": name,
        "severity": severity or t["severity"],
        "current_value": current_value,
        "why": why.substitute(fmt) if hasattr(why, "substitute") else why,
        "how": how.substitute(fmt) if hasattr(how, "substitute") else how,
        "reference_url": t["reference_url"],
        "breaking_change": t["breaking_change"],
    }


# ---------------------------------------------------------------------------
# Pure functions: data normalization
# ---------------------------------------------------------------------------

def partition_by_group(
    rows: list[dict],
    index_to_group: dict[str, str],
) -> dict[str, list[dict]]:
    """Partition _cat/indices or _cat/shards rows by index group.

    Args:
        rows: List of dicts from _cat API (each has an "index" key).
        index_to_group: Mapping from concrete index name to group name.

    Returns:
        Dict mapping group name to list of rows.
    """
    result: dict[str, list[dict]] = {}
    for row in rows:
        idx_name = row.get("index", "")
        group = index_to_group.get(idx_name)
        if group:
            result.setdefault(group, []).append(row)
    return result


def pick_representative_index(concrete_indices: list[str]) -> str:
    """Pick the latest index from a group for settings/mapping checks.

    For rollover-based groups, the lexicographically last index has the
    current template's settings.
    """
    return sorted(concrete_indices)[-1]


def estimate_rollover_hours(creation_dates: list[str]) -> float | None:
    """Estimate rollover frequency from index creation date gaps.

    Args:
        creation_dates: ISO date strings from _cat/indices, sorted.

    Returns:
        Median gap in hours, or None if fewer than 2 indices.
    """
    if len(creation_dates) < 2:
        return None

    # Parse dates
    timestamps = []
    for ds in creation_dates:
        try:
            # Handle ES format: "2026-02-14T00:00:00.000Z"
            cleaned = ds.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            timestamps.append(dt)
        except (ValueError, TypeError):
            continue

    if len(timestamps) < 2:
        return None

    timestamps.sort()
    gaps_hours = []
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600
        if gap > 0:
            gaps_hours.append(gap)

    if not gaps_hours:
        return None

    return median(gaps_hours)


def build_usage_stats_query(index_group: str, lookback_hours: int) -> dict:
    """Build ES aggregation query for usage patterns from .usage-events."""
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
        "aggs": {
            "lookback_percentiles": {
                "percentiles": {
                    "field": "lookback_seconds",
                    "percents": [50, 95],
                }
            },
            "sorted_fields": {
                "terms": {
                    "field": "fields.sorted",
                    "size": 5,
                }
            },
            "total_sorted_queries": {
                "filter": {"exists": {"field": "fields.sorted"}},
            },
            "operations": {
                "terms": {
                    "field": "operation",
                    "size": 10,
                }
            },
        },
    }


def parse_usage_stats_response(response: dict) -> dict:
    """Parse the .usage-events aggregation response into a flat dict.

    Returns:
        Dict with keys: lookback_p50_seconds, lookback_p95_seconds,
        dominant_sort_field, dominant_sort_pct, search_count, write_count.
    """
    aggs = response.get("aggregations", {})

    # Lookback percentiles
    pctls = aggs.get("lookback_percentiles", {}).get("values", {})
    p50_raw = pctls.get("50.0")
    p95_raw = pctls.get("95.0")
    p50 = _safe_float(p50_raw) if p50_raw is not None else None
    p95 = _safe_float(p95_raw) if p95_raw is not None else None
    # ES returns 0.0 when there are no values; treat as None
    if p50 is not None and p50 == 0.0 and p95 is not None and p95 == 0.0:
        p50 = None
        p95 = None

    # Dominant sort field
    sorted_buckets = aggs.get("sorted_fields", {}).get("buckets", [])
    total_sorted = aggs.get("total_sorted_queries", {}).get("doc_count", 0)
    dominant_sort_field = None
    dominant_sort_pct = None
    if sorted_buckets and total_sorted > 0:
        top = sorted_buckets[0]
        dominant_sort_field = top["key"]
        dominant_sort_pct = top["doc_count"] / total_sorted

    # Operation counts
    op_buckets = aggs.get("operations", {}).get("buckets", [])
    search_count = 0
    write_count = 0
    for bucket in op_buckets:
        op = bucket["key"]
        count = bucket["doc_count"]
        if op in ("search", "async_search", "count"):
            search_count += count
        elif op in ("bulk", "index", "update", "delete"):
            write_count += count

    return {
        "lookback_p50_seconds": p50,
        "lookback_p95_seconds": p95,
        "dominant_sort_field": dominant_sort_field,
        "dominant_sort_pct": dominant_sort_pct,
        "search_count": search_count,
        "write_count": write_count,
    }


def build_group_profile(
    index_group: str,
    cat_indices_rows: list[dict],
    cat_shards_rows: list[dict],
    flat_settings: dict[str, str],
    mapping_field_count: int | None,
    source_enabled: bool,
    usage_stats: dict | None,
    segment_counts: dict[str, int] | None = None,
) -> dict:
    """Build a normalized profile dict for one index group.

    All rule functions operate on this profile — no raw ES data leaks
    into rule logic.

    Args:
        flat_settings: Dict of flattened settings from the representative
            index (keys like "index.number_of_replicas").
    """
    # Shard stats (primary shards only)
    primary_shards = [
        row for row in cat_shards_rows
        if row.get("prirep") == "p" and row.get("state") == "STARTED"
    ]
    primary_shard_count = len(primary_shards)
    total_primary_bytes = sum(_safe_int(s.get("store")) for s in primary_shards)
    avg_primary_bytes = (
        total_primary_bytes // primary_shard_count
        if primary_shard_count > 0 else 0
    )

    # Index-level stats
    indices = [row.get("index", "") for row in cat_indices_rows]
    creation_dates = sorted(
        ds for row in cat_indices_rows
        if (ds := row.get("creation.date.string"))
    )

    # Settings (flat keys like "index.number_of_replicas")
    replicas = _safe_int(flat_settings.get("index.number_of_replicas", "1"), 1)
    tier_pref = flat_settings.get("index.routing.allocation.include._tier_preference")
    codec = flat_settings.get("index.codec")
    blocks_write = flat_settings.get("index.blocks.write", "false") == "true"
    refresh_interval = flat_settings.get("index.refresh_interval")
    sort_field_raw = flat_settings.get("index.sort.field")
    total_fields_limit = _safe_int(
        flat_settings.get("index.mapping.total_fields.limit", "1000"), 1000,
    )

    # Parse sort field (can be a single string or JSON array)
    index_sort_field = None
    if sort_field_raw:
        if sort_field_raw.startswith("["):
            try:
                index_sort_field = json.loads(sort_field_raw)
            except (json.JSONDecodeError, TypeError):
                index_sort_field = [sort_field_raw]
        else:
            index_sort_field = [sort_field_raw]

    # Translog durability (Rule 11)
    translog_durability = flat_settings.get(
        "index.translog.durability", "request",
    )

    # Merge policy (Rule 14)
    max_merged_segment = flat_settings.get(
        "index.merge.policy.max_merged_segment",
    )

    # Segment counts (Rule 12) — avg segments per primary shard
    avg_segments_per_primary = 0
    if segment_counts and primary_shard_count > 0:
        group_segments = sum(
            segment_counts.get(idx, 0) for idx in indices
        )
        avg_segments_per_primary = group_segments // primary_shard_count

    # Max docs per primary shard (Rule 15)
    max_docs_per_primary_shard = 0
    for shard_row in primary_shards:
        doc_count = _safe_int(shard_row.get("docs"), 0)
        if doc_count > max_docs_per_primary_shard:
            max_docs_per_primary_shard = doc_count

    # Rollover inference
    rollover_hours = estimate_rollover_hours(creation_dates)

    # Usage stats
    us = usage_stats or {}

    return {
        "index_group": index_group,
        "indices": indices,
        "index_count": len(indices),
        # Shard stats
        "primary_shard_count": primary_shard_count,
        "avg_primary_shard_size_bytes": avg_primary_bytes,
        "total_primary_store_bytes": total_primary_bytes,
        # Settings
        "number_of_replicas": replicas,
        "tier_preference": tier_pref,
        "index_codec": codec,
        "blocks_write": blocks_write,
        "refresh_interval": refresh_interval,
        "index_sort_field": index_sort_field,
        "total_fields_limit": total_fields_limit,
        # Mapping
        "field_count": mapping_field_count,
        "source_enabled": source_enabled,
        # Rollover
        "creation_dates": creation_dates,
        "estimated_rollover_hours": rollover_hours,
        # Usage
        "lookback_p50_seconds": us.get("lookback_p50_seconds"),
        "lookback_p95_seconds": us.get("lookback_p95_seconds"),
        "dominant_sort_field": us.get("dominant_sort_field"),
        "dominant_sort_pct": us.get("dominant_sort_pct"),
        "search_count": us.get("search_count", 0),
        "write_count": us.get("write_count", 0),
        # Rules 11-15
        "translog_durability": translog_durability,
        "avg_segments_per_primary": avg_segments_per_primary,
        "max_merged_segment": max_merged_segment,
        "max_docs_per_primary_shard": max_docs_per_primary_shard,
    }


# ---------------------------------------------------------------------------
# Pure functions: 10 recommendation rules
#
# Each rule takes a group profile dict and returns a list of 0-1 dicts.
# Rules never raise exceptions — they return [] if data is missing.
# ---------------------------------------------------------------------------

def check_shard_too_small(profile: dict) -> list[dict]:
    """Rule 1: Primary shards avg < 1GB with multiple shards."""
    shard_count = profile["primary_shard_count"]
    avg_bytes = profile["avg_primary_shard_size_bytes"]
    if shard_count <= 1 or avg_bytes >= 1_000_000_000:
        return []

    avg_str = _fmt_bytes(avg_bytes)
    total_str = _fmt_bytes(profile["total_primary_store_bytes"])
    return [_build_rec("shard_too_small",
        current_value=f"{shard_count} primary shards x {avg_str} avg = {total_str} total",
        shard_count=shard_count, avg_str=avg_str,
    )]


def check_shard_too_large(profile: dict) -> list[dict]:
    """Rule 2: Primary shards avg > 50GB. Critical at >100GB."""
    avg_bytes = profile["avg_primary_shard_size_bytes"]
    shard_count = profile["primary_shard_count"]
    if shard_count == 0 or avg_bytes < 50_000_000_000:
        return []

    avg_str = _fmt_bytes(avg_bytes)
    severity = "critical" if avg_bytes > 100_000_000_000 else "warning"
    return [_build_rec("shard_too_large",
        current_value=f"{shard_count} primary shards averaging {avg_str} each",
        severity=severity, avg_str=avg_str,
    )]


def check_replica_risk(profile: dict) -> list[dict]:
    """Rule 3: 0 replicas on a non-snapshot-backed index."""
    if profile["number_of_replicas"] != 0:
        return []
    tier = profile.get("tier_preference") or ""
    if "frozen" in tier:
        return []  # snapshot-backed, no replicas needed

    return [_build_rec("replica_risk", current_value="0 replicas configured")]


def check_replica_waste(profile: dict) -> list[dict]:
    """Rule 4: Replicas on cold/frozen tier (searchable snapshot)."""
    if profile["number_of_replicas"] == 0:
        return []
    tier = profile.get("tier_preference") or ""
    if "cold" not in tier and "frozen" not in tier:
        return []

    replicas = profile["number_of_replicas"]
    tier_label = tier.split(",")[0].replace("data_", "")
    return [_build_rec("replica_waste",
        current_value=f"{replicas} replica(s) on {tier_label} tier",
        replicas=replicas, tier_label=tier_label,
    )]


def check_codec_opportunity(profile: dict) -> list[dict]:
    """Rule 5: Default codec (LZ4) on read-only or warm/cold index."""
    if profile["index_codec"] is not None:
        return []  # explicit codec already set
    tier = profile.get("tier_preference") or ""
    is_read_only = profile["blocks_write"]
    is_warm_cold = any(t in tier for t in ("warm", "cold", "frozen"))
    if not is_read_only and not is_warm_cold:
        return []

    total_str = _fmt_bytes(profile["total_primary_store_bytes"])
    reason = "read-only" if is_read_only else tier.split(",")[0].replace("data_", "") + " tier"
    return [_build_rec("codec_opportunity",
        current_value=f"Default codec (LZ4) on {reason} index ({total_str})",
        reason=reason,
    )]


def check_field_count_near_limit(profile: dict) -> list[dict]:
    """Rule 6: Field count > 80% of total_fields.limit."""
    field_count = profile.get("field_count")
    if field_count is None:
        return []
    limit = profile["total_fields_limit"]
    if limit <= 0:
        return []
    ratio = field_count / limit
    if ratio < 0.8:
        return []

    severity = "critical" if ratio > 0.95 else "warning"
    pct = int(ratio * 100)
    return [_build_rec("field_count_near_limit",
        current_value=f"{field_count} fields mapped out of {limit} limit ({pct}%)",
        severity=severity,
        field_count=field_count, pct=pct, limit=limit,
    )]


def check_source_disabled(profile: dict) -> list[dict]:
    """Rule 7: _source.enabled: false."""
    if profile["source_enabled"]:
        return []

    return [_build_rec("source_disabled", current_value="_source: false")]


def check_rollover_lookback_mismatch(profile: dict) -> list[dict]:
    """Rule 8: Rollover frequency doesn't match query lookback windows."""
    rollover_h = profile.get("estimated_rollover_hours")
    lookback_p95 = profile.get("lookback_p95_seconds")
    if rollover_h is None or lookback_p95 is None:
        return []
    if rollover_h <= 0:
        return []

    lookback_p95_hours = lookback_p95 / 3600
    # If p95 lookback > 2x rollover period, queries fan out too many indices
    if lookback_p95_hours <= rollover_h * 2:
        return []

    indices_hit = lookback_p95_hours / rollover_h
    lookback_label = (
        f"{lookback_p95_hours:.0f}h"
        if lookback_p95_hours < 48
        else f"{lookback_p95_hours / 24:.0f}d"
    )
    rollover_label = (
        f"{rollover_h:.0f}h"
        if rollover_h < 48
        else f"{rollover_h / 24:.0f}d"
    )
    return [_build_rec("rollover_lookback_mismatch",
        current_value=(
            f"Rollover ~{rollover_label}, "
            f"p95 query lookback {lookback_label} "
            f"(~{indices_hit:.0f} indices per query)"
        ),
        rollover_label=rollover_label,
        lookback_label=lookback_label,
        indices_hit=f"{indices_hit:.0f}",
    )]


def check_index_sorting_opportunity(profile: dict) -> list[dict]:
    """Rule 9: Dominant sort field detected, index unsorted."""
    dominant_field = profile.get("dominant_sort_field")
    dominant_pct = profile.get("dominant_sort_pct") or 0
    if dominant_field is None or dominant_pct < 0.7:
        return []
    if profile.get("index_sort_field"):
        return []  # already sorted

    pct = int(dominant_pct * 100)
    return [_build_rec("index_sorting_opportunity",
        current_value=f"{pct}% of sorted queries use '{dominant_field}', index is unsorted",
        pct=pct, dominant_field=dominant_field,
    )]


def check_refresh_interval_opportunity(profile: dict) -> list[dict]:
    """Rule 10: High write rate + low search rate but 1s refresh."""
    search_count = profile.get("search_count", 0)
    write_count = profile.get("write_count", 0)
    if write_count == 0:
        return []
    if write_count < search_count * 10:
        return []  # not write-dominant

    refresh = profile.get("refresh_interval")
    # Default is 1s (None means 1s)
    if refresh is not None and refresh not in ("1s", "1000ms"):
        return []  # already customized

    return [_build_rec("refresh_interval_opportunity",
        current_value=(
            f"Refresh: {refresh or '1s (default)'} | "
            f"Writes: {write_count:,}/period | "
            f"Searches: {search_count:,}/period"
        ),
        write_count=f"{write_count:,}",
        search_count=f"{search_count:,}",
    )]


def check_translog_async(profile: dict) -> list[dict]:
    """Rule 11: Translog durability set to async — risks data loss on crash."""
    durability = profile.get("translog_durability", "request")
    if durability != "async":
        return []

    return [_build_rec("translog_async",
        current_value="index.translog.durability = async",
    )]


def check_force_merge_opportunity(profile: dict) -> list[dict]:
    """Rule 12: Read-only index with many segments — force merge opportunity."""
    if not profile.get("blocks_write"):
        return []
    avg_seg = profile.get("avg_segments_per_primary", 0)
    if avg_seg <= 5:
        return []
    shard_count = profile["primary_shard_count"]
    if shard_count == 0:
        return []

    total_str = _fmt_bytes(profile["total_primary_store_bytes"])
    return [_build_rec("force_merge_opportunity",
        current_value=(
            f"Read-only index with ~{avg_seg} segments/shard "
            f"({shard_count} primary shards, {total_str} total)"
        ),
        avg_seg=avg_seg,
    )]


def check_node_shard_count(cat_shards_rows: list[dict]) -> list[dict]:
    """Rule 13: Node-level shard count exceeding safe limits.

    This is a cluster-level check (not per index group). Unlike other rules,
    it takes raw _cat/shards data instead of a profile dict.

    Returns recommendations for index_group="_cluster".
    """
    node_counts: dict[str, int] = {}
    for row in cat_shards_rows:
        node = row.get("node")
        if not node or row.get("state") != "STARTED":
            continue
        node_counts[node] = node_counts.get(node, 0) + 1

    results = []
    for node, count in sorted(node_counts.items()):
        if count <= 1000:
            continue

        severity = "critical" if count > 1500 else "warning"
        impact_note = (
            "CRITICAL: At >1,500 shards per node, cluster instability "
            "becomes likely. Master node elections, cluster state "
            "publications, and shard allocation decisions all degrade "
            "significantly."
            if count > 1500 else
            "At this level, the node is approaching the danger zone. "
            "Plan shard reduction before adding more indices."
        )
        results.append(_build_rec("node_shard_count",
            current_value=f"Node '{node}' has {count:,} shards",
            severity=severity,
            node=node, count=f"{count:,}", impact_note=impact_note,
        ))

    return results


def check_merge_policy_tuning(profile: dict) -> list[dict]:
    """Rule 14: Large shards with default max_merged_segment (5GB)."""
    avg_bytes = profile["avg_primary_shard_size_bytes"]
    if avg_bytes < 50_000_000_000:  # 50GB
        return []

    max_seg_str = profile.get("max_merged_segment")
    if max_seg_str is not None:
        max_seg_bytes = _parse_bytes_string(max_seg_str)
        if max_seg_bytes is not None and max_seg_bytes > 5_000_000_000:
            return []  # already tuned above 5GB

    avg_str = _fmt_bytes(avg_bytes)
    return [_build_rec("merge_policy_tuning",
        current_value=(
            f"Avg shard size {avg_str} with max_merged_segment "
            f"= {max_seg_str or '5gb (default)'}"
        ),
        avg_str=avg_str,
    )]


def check_shard_docs_limit(profile: dict) -> list[dict]:
    """Rule 15: Primary shard approaching 200M docs soft limit."""
    max_docs = profile.get("max_docs_per_primary_shard", 0)
    if max_docs <= 200_000_000:
        return []

    shard_count = profile["primary_shard_count"]
    severity = "critical" if max_docs > 500_000_000 else "warning"
    docs_label = f"{max_docs / 1_000_000:.0f}M"
    impact_note = (
        "CRITICAL: At >500M docs per shard, merge failures and "
        "out-of-memory errors become increasingly likely during "
        "segment merges."
        if max_docs > 500_000_000 else
        "At this document count, plan to reduce docs per shard "
        "before the index grows further."
    )
    shard_target = max(shard_count * 2, 2)
    return [_build_rec("shard_docs_limit",
        current_value=(
            f"Max docs per primary shard: {docs_label} "
            f"({shard_count} primary shards)"
        ),
        severity=severity,
        docs_label=docs_label, impact_note=impact_note,
        shard_target=shard_target,
    )]


# ---------------------------------------------------------------------------
# Rule orchestration
# ---------------------------------------------------------------------------

ALL_RULES = [
    check_shard_too_small,
    check_shard_too_large,
    check_replica_risk,
    check_replica_waste,
    check_codec_opportunity,
    check_field_count_near_limit,
    check_source_disabled,
    check_rollover_lookback_mismatch,
    check_index_sorting_opportunity,
    check_refresh_interval_opportunity,
    check_translog_async,
    check_force_merge_opportunity,
    check_merge_policy_tuning,
    check_shard_docs_limit,
]


def evaluate_all_rules(profile: dict) -> list[dict]:
    """Run all 14 per-group rules against a group profile.

    Rule 13 (check_node_shard_count) is a cluster-level check and runs
    separately in refresh() — it is not included in ALL_RULES.

    Returns a list of recommendation dicts (may be empty if the group
    is well-configured).
    """
    results = []
    for rule_fn in ALL_RULES:
        results.extend(rule_fn(profile))
    return results


# ---------------------------------------------------------------------------
# Async functions (ES I/O)
# ---------------------------------------------------------------------------

async def ensure_index_arch_index() -> None:
    """Create the .index-recommendations index if it doesn't exist."""
    try:
        resp = await _client.head(f"/{INDEX_ARCH_INDEX}")
        if resp.status_code == 200:
            return
        resp = await _client.put(
            f"/{INDEX_ARCH_INDEX}",
            json=INDEX_ARCH_INDEX_MAPPING,
        )
        if resp.status_code in (200, 201):
            logger.info("Created index arch index: %s", INDEX_ARCH_INDEX)
        else:
            logger.warning(
                "Failed to create index arch index: %s %s",
                resp.status_code, resp.text[:200],
            )
    except httpx.RequestError as exc:
        logger.warning("Could not ensure index arch index exists: %s", exc)


async def fetch_cat_indices() -> list[dict] | None:
    """Fetch index metadata for all non-system indices."""
    try:
        resp = await _client.get(
            "/_cat/indices/*,-.*",
            params={
                "format": "json",
                "bytes": "b",
                "h": "index,health,status,pri,rep,docs.count,"
                     "store.size,pri.store.size,creation.date.string",
            },
        )
        if resp.status_code != 200:
            logger.warning("Failed to fetch _cat/indices: %s", resp.status_code)
            return None
        return resp.json()
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch _cat/indices: %s", exc)
        return None


async def fetch_cat_shards() -> list[dict] | None:
    """Fetch shard metadata for all non-system indices."""
    try:
        resp = await _client.get(
            "/_cat/shards/*,-.*",
            params={
                "format": "json",
                "bytes": "b",
                "h": "index,shard,prirep,state,docs,store,node",
            },
        )
        if resp.status_code != 200:
            logger.warning("Failed to fetch _cat/shards: %s", resp.status_code)
            return None
        return resp.json()
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch _cat/shards: %s", exc)
        return None


async def fetch_all_settings() -> dict[str, dict] | None:
    """Fetch flattened settings for all non-system indices.

    Returns a dict mapping index name to its flat settings dict
    (keys like "index.number_of_replicas").
    """
    try:
        resp = await _client.get(
            "/*,-.*/_settings",
            params={"flat_settings": "true"},
        )
        if resp.status_code != 200:
            logger.warning("Failed to fetch _settings: %s", resp.status_code)
            return None
        raw = resp.json()
        # Flatten: {index_name: {"settings": {flat_keys...}}} -> {index_name: {flat_keys...}}
        result = {}
        for idx_name, idx_data in raw.items():
            result[idx_name] = idx_data.get("settings", {})
        return result
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch _settings: %s", exc)
        return None


async def fetch_index_segment_counts() -> dict[str, int] | None:
    """Fetch segment count per index for all non-system indices.

    Uses the _stats/segments API which returns pre-aggregated counts
    (much less data than _cat/segments which returns one row per segment).

    Returns a dict mapping index name to primary segment count,
    or None on failure.
    """
    try:
        resp = await _client.get("/*,-.*/_stats/segments")
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch _stats/segments: %s", resp.status_code,
            )
            return None
        data = resp.json()
        result = {}
        for idx_name, idx_data in data.get("indices", {}).items():
            count = _safe_int(
                idx_data.get("primaries", {})
                .get("segments", {})
                .get("count"),
                0,
            )
            result[idx_name] = count
        return result
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch _stats/segments: %s", exc)
        return None


async def fetch_mapping_info(index_name: str) -> tuple[int | None, bool]:
    """Fetch field count and _source.enabled for one index.

    Returns (field_count, source_enabled). field_count is None on failure.
    """
    try:
        resp = await _client.get(f"/{index_name}/_mapping")
        if resp.status_code != 200:
            return None, True

        data = resp.json()
        idx_mapping = data.get(index_name, {}).get("mappings", {})

        # Check _source.enabled
        source_cfg = idx_mapping.get("_source", {})
        source_enabled = source_cfg.get("enabled", True)

        # Count fields using flatten_mapping from mapping_diff
        properties = idx_mapping.get("properties", {})
        if not properties:
            return 0, source_enabled

        fields = flatten_mapping(properties)
        return len(fields), source_enabled
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch mapping for %s: %s", index_name, exc)
        return None, True


async def fetch_usage_stats_for_group(
    index_group: str, lookback_hours: int,
) -> dict | None:
    """Aggregate usage patterns from .usage-events for one index group."""
    query = build_usage_stats_query(index_group, lookback_hours)
    try:
        resp = await _client.post(f"/{USAGE_INDEX}/_search", json=query)
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch usage stats for %s: %s",
                index_group, resp.status_code,
            )
            return None
        return parse_usage_stats_response(resp.json())
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch usage stats for %s: %s", index_group, exc)
        return None


async def write_recommendation_docs(index_group: str, docs: list[dict]) -> None:
    """Write recommendation documents to ES (delete-and-rewrite per group)."""
    # Delete existing docs for this group
    try:
        await _client.post(
            f"/{INDEX_ARCH_INDEX}/_delete_by_query",
            json={"query": {"term": {"index_group": index_group}}},
            params={"refresh": "false"},
        )
    except httpx.RequestError as exc:
        logger.warning(
            "Failed to delete old index arch docs for %s: %s",
            index_group, exc,
        )

    if not docs:
        return

    # Bulk-write new docs
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": INDEX_ARCH_INDEX}}))
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
                    "Index arch bulk write for %s: %d/%d errors",
                    index_group, error_count, len(docs),
                )
        else:
            logger.warning(
                "Index arch bulk write failed for %s: %s",
                index_group, resp.status_code,
            )
    except httpx.RequestError as exc:
        logger.warning(
            "Index arch bulk write failed for %s: %s",
            index_group, exc,
        )


async def refresh() -> None:
    """Recompute index architecture recommendations for all groups."""
    groups = metadata_mod.get_groups()
    if not groups:
        logger.debug("No index groups known — skipping index arch refresh")
        return

    # Phase 1: Global data collection (4 calls, not per group)
    cat_indices = await fetch_cat_indices()
    cat_shards = await fetch_cat_shards()
    all_settings = await fetch_all_settings()

    if cat_indices is None or cat_shards is None or all_settings is None:
        logger.warning("Could not fetch cluster data — skipping index arch refresh")
        metrics.inc("index_arch_refresh_failed")
        return

    # Segment counts are optional — Rule 12 degrades gracefully if missing
    segment_counts = await fetch_index_segment_counts()

    # Phase 2: Build index-to-group lookup and partition
    index_to_group = metadata_mod.get_index_to_group()
    indices_by_group = partition_by_group(cat_indices, index_to_group)
    shards_by_group = partition_by_group(cat_shards, index_to_group)

    timestamp = datetime.now(timezone.utc).isoformat()
    processed = 0

    # Phase 3: Per-group processing
    for index_group in groups:
        if index_group.startswith("."):
            continue

        group_indices = indices_by_group.get(index_group, [])
        group_shards = shards_by_group.get(index_group, [])
        if not group_indices:
            continue

        # Pick representative index for settings/mapping
        concrete_names = sorted(groups[index_group])
        if not concrete_names:
            continue
        rep_index = pick_representative_index(concrete_names)
        rep_settings = all_settings.get(rep_index, {})

        # Fetch mapping info for representative index
        field_count, source_enabled = await fetch_mapping_info(rep_index)

        # Fetch usage stats from .usage-events
        usage_stats = await fetch_usage_stats_for_group(
            index_group, INDEX_ARCH_LOOKBACK_HOURS,
        )

        # Build profile
        profile = build_group_profile(
            index_group=index_group,
            cat_indices_rows=group_indices,
            cat_shards_rows=group_shards,
            flat_settings=rep_settings,
            mapping_field_count=field_count,
            source_enabled=source_enabled,
            usage_stats=usage_stats,
            segment_counts=segment_counts,
        )

        # Evaluate rules
        recs = evaluate_all_rules(profile)

        # Build final docs
        docs = []
        for rec in recs:
            docs.append({
                "timestamp": timestamp,
                "index_group": index_group,
                **rec,
            })

        await write_recommendation_docs(index_group, docs)
        processed += 1

    # Phase 4: Cluster-level checks (not per group)
    cluster_recs = check_node_shard_count(cat_shards)
    cluster_docs = []
    for rec in cluster_recs:
        cluster_docs.append({
            "timestamp": timestamp,
            "index_group": "_cluster",
            **rec,
        })
    await write_recommendation_docs("_cluster", cluster_docs)

    metrics.inc("index_arch_refresh_ok")
    logger.info(
        "Index architecture recommendations refreshed: %d groups processed",
        processed,
    )


# ---------------------------------------------------------------------------
# Background lifecycle
# ---------------------------------------------------------------------------

async def _index_arch_loop() -> None:
    """Background loop that refreshes index arch recommendations periodically."""
    while True:
        try:
            await refresh()
        except Exception:
            logger.exception("Index architecture refresh failed")
            metrics.inc("index_arch_refresh_failed")
        await asyncio.sleep(INDEX_ARCH_REFRESH_INTERVAL)


def start_index_arch_loop() -> None:
    """Start the background index arch refresh loop."""
    loop = asyncio.get_running_loop()
    loop.create_task(_index_arch_loop())
    logger.info(
        "Index arch loop started (interval=%ds, lookback=%dh)",
        INDEX_ARCH_REFRESH_INTERVAL, INDEX_ARCH_LOOKBACK_HOURS,
    )


async def close_index_arch_client() -> None:
    """Close the index arch client. Called during gateway shutdown."""
    await _client.aclose()
