"""
Kibana dashboard setup — creates data views, visualizations, and dashboards
via the Kibana saved objects _import API (NDJSON format).

Uses classic visualization saved objects (visState) instead of embedded Lens,
which has a stable, documented format.

Dashboards created:
  1. Products Explorer       — data table of seeded products
  2. Usage & Heat            — query traffic patterns + field heat (with index_group filter)
  3. Multi-Index Comparison  — cross-index-group heat and operations

Usage:
    python kibana_setup.py
    python kibana_setup.py --kibana http://localhost:5601
"""

import argparse
import json
import io
import time
import sys
import requests

KIBANA_URL = "http://localhost:5601"

HEADERS = {
    "kbn-xsrf": "true",
}


def wait_for_kibana(base_url: str, timeout: int = 120) -> None:
    print(f"Waiting for Kibana at {base_url} ...")
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            resp = requests.get(f"{base_url}/api/status", timeout=5)
            if resp.status_code == 200:
                status = resp.json().get("status", {}).get("overall", {}).get("level", "")
                if status == "available":
                    print("Kibana is ready.")
                    return
        except requests.RequestException:
            pass
        time.sleep(3)
    print("ERROR: Kibana did not become ready in time.", file=sys.stderr)
    sys.exit(1)


def create_data_view(base_url: str, title: str, name: str, dv_id: str, time_field: str = None) -> str:
    body = {
        "data_view": {
            "id": dv_id,
            "title": title,
            "name": name,
        }
    }
    if time_field:
        body["data_view"]["timeFieldName"] = time_field

    resp = requests.post(
        f"{base_url}/api/data_views/data_view",
        headers={**HEADERS, "Content-Type": "application/json"},
        json=body,
    )

    if resp.status_code in (200, 201):
        actual_id = resp.json()["data_view"]["id"]
        print(f"  Created data view '{name}' (id={actual_id})")
        return actual_id
    elif "Duplicate" in resp.text:
        print(f"  Data view '{name}' already exists (id={dv_id})")
        return dv_id
    else:
        print(f"  WARNING: {resp.status_code} {resp.text[:200]}")
        return dv_id


# --- Visualization helper ---

def _vis(vis_id, title, vis_type, vis_state_params, aggs, index_pattern_id,
         search_query=""):
    """Build a classic Kibana visualization saved object."""
    return {
        "id": vis_id,
        "type": "visualization",
        "attributes": {
            "title": title,
            "visState": json.dumps({
                "title": title,
                "type": vis_type,
                "params": vis_state_params,
                "aggs": aggs,
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": index_pattern_id,
                    "query": {"query": search_query, "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": [
            {"id": index_pattern_id, "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}
        ],
    }


def _markdown(md_id, title, markdown_text):
    """Build a Markdown visualization panel (used as section headers)."""
    return {
        "id": md_id,
        "type": "visualization",
        "attributes": {
            "title": title,
            "visState": json.dumps({
                "title": title,
                "type": "markdown",
                "params": {
                    "fontSize": 16,
                    "openLinksInNewTab": True,
                    "markdown": markdown_text,
                },
                "aggs": [],
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": [],
    }


# --- Shared params ---

TABLE_PARAMS = {
    "perPage": 10, "showPartialRows": False,
    "showMetricsAtAllLevels": False, "showTotal": True,
    "totalFunc": "sum", "percentageCol": "Count", "row": True,
}
  
AREA_AXES = {
    "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                      "show": True, "style": {}, "scale": {"type": "linear"},
                      "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
    "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                   "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                   "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                   "title": {"text": "Count"}}],
}

LINE_AXES = {
    "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                      "show": True, "style": {}, "scale": {"type": "linear"},
                      "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
    "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                   "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                   "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                   "title": {"text": "ms"}}],
}


# KQL filter to exclude rollup and meta docs from usage visualizations.
# Rollup docs (type:rollup) have a different schema (field_usage instead of
# fields.*) and would pollute counts.  Meta docs (type:_meta) are internal.
_RAW_EVENTS_ONLY = "NOT type:rollup AND NOT type:_meta"


def _control_group_input(data_view_id: str) -> tuple[dict, list[dict]]:
    """Build a Kibana 8 native controlGroupInput for index_group filtering.

    Returns (controlGroupInput dict, extra references list).
    """
    panels = {
        "0": {
            "type": "optionsListControl",
            "order": 0,
            "width": "medium",
            "grow": True,
            "explicitInput": {
                "id": "0",
                "fieldName": "index_group",
                "title": "Index Group",
                "selectedOptions": [],
                "enhancements": {},
                "singleSelect": False,
                "searchTechnique": "prefix",
            },
        },
    }
    control_input = {
        "chainingSystem": "HIERARCHICAL",
        "controlStyle": "oneLine",
        "showApplySelections": False,
        "ignoreParentSettingsJSON": json.dumps({
            "ignoreFilters": False,
            "ignoreQuery": False,
            "ignoreTimerange": False,
            "ignoreValidations": False,
        }),
        "panelsJSON": json.dumps(panels),
    }
    refs = [
        {"name": "controlGroup_0:optionsListDataView", "type": "index-pattern", "id": data_view_id},
    ]
    return control_input, refs


def build_saved_objects(products_dv_id: str, usage_dv_id: str, logs_dv_id: str, orders_dv_id: str) -> list[dict]:
    """Build all visualization + dashboard saved objects."""
    objects = []

    # =========================================================
    # PRODUCTS VISUALIZATIONS
    # =========================================================

    objects.append(_vis(
        "vis-products-table", "Products Table", "table",
        {"perPage": 25, "showPartialRows": False, "showMetricsAtAllLevels": False,
         "showTotal": True, "totalFunc": "count", "percentageCol": "", "row": True},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "title.keyword", "size": 100, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "category", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
            {"id": "4", "enabled": True, "type": "terms", "params": {"field": "brand", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
        ],
        products_dv_id,
    ))

    objects.append(_vis(
        "vis-products-by-category", "Products by Category", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "category", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        products_dv_id,
    ))

    # =========================================================
    # USAGE & HEAT VISUALIZATIONS
    # =========================================================

    # Index Groups table (all queries contribute to their group's count)
    objects.append(_vis(
        "vis-concrete-indices", "Index Groups", "table", TABLE_PARAMS,
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Operations over time (area chart)
    objects.append(_vis(
        "vis-ops-over-time", "Operations Over Time", "area",
        {"type": "area", "grid": {"categoryLines": False}, **AREA_AXES,
         "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram", "params": {"field": "timestamp", "useNormalizedEsInterval": True, "scaleMetricValues": False, "interval": "auto", "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Query type distribution (pie)
    objects.append(_vis(
        "vis-query-types", "Query Type Distribution", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "operation", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Field heat tables
    for field_cat, label in [
        ("fields.queried", "Top Queried Fields"),
        ("fields.filtered", "Top Filtered Fields"),
        ("fields.aggregated", "Top Aggregated Fields"),
        ("fields.sorted", "Top Sorted Fields"),
        ("fields.sourced", "Top Fetched Fields"),
    ]:
        vis_id = "vis-top-" + field_cat.split(".")[-1]
        objects.append(_vis(
            vis_id, label, "table", TABLE_PARAMS,
            [
                {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "terms", "params": {"field": field_cat, "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
            ],
            usage_dv_id,
            search_query=_RAW_EVENTS_ONLY,
        ))

    # =========================================================
    # FIELD HEAT BY RESPONSE TIME (time-weighted field importance)
    # =========================================================
    # Same field categories as above, but metric is sum(response_time_ms)
    # instead of count. Shows which fields cause the most total latency.

    for field_cat, label in [
        ("fields.queried", "Queried Fields by Response Time"),
        ("fields.filtered", "Filtered Fields by Response Time"),
        ("fields.aggregated", "Aggregated Fields by Response Time"),
        ("fields.sorted", "Sorted Fields by Response Time"),
        ("fields.sourced", "Fetched Fields by Response Time"),
    ]:
        vis_id = "vis-rt-" + field_cat.split(".")[-1]
        objects.append(_vis(
            vis_id, label, "table",
            {**TABLE_PARAMS, "totalFunc": "sum", "percentageCol": "Sum of response_time_ms"},
            [
                {"id": "1", "enabled": True, "type": "sum",
                 "params": {"field": "response_time_ms"},
                 "schema": "metric"},
                {"id": "2", "enabled": True, "type": "terms",
                 "params": {"field": field_cat, "size": 20,
                            "order": "desc", "orderBy": "1"},
                 "schema": "bucket"},
            ],
            usage_dv_id,
            search_query=_RAW_EVENTS_ONLY,
        ))

    # =========================================================
    # SECTION HEADERS (Markdown panels for dashboard organization)
    # =========================================================

    objects.append(_markdown(
        "md-header-overview", "Section: Overview",
        "## Overview\n"
        "Traffic volume, index groups, and query type breakdown.\n\n"
        "**Reading this section:**\n"
        "- **High-traffic index groups** (thousands of ops/hour) need adequate replicas and heap — check these first.\n"
        "- **Very low-traffic groups** (< 1 op/hour) are candidates for freezing or reducing replicas to save resources.\n"
        "- Use the *Index Group* filter at the top to drill into a specific group.",
    ))
    objects.append(_markdown(
        "md-header-field-heat-count", "Section: Field Heat by Count",
        "## Field Heat by Count\n"
        "Which fields are referenced most often, broken down by operation type (queried, filtered, aggregated, sorted, fetched).\n\n"
        "**How to act on this:**\n"
        "- **Fields that never appear** in any table are unused — set `index: false` in the mapping to save disk and indexing CPU.\n"
        "- **Fields only in \"Fetched\"** (returned in `_source` but never queried/filtered/aggregated) — also candidates for `index: false`.\n"
        "- **Fields in \"Aggregated\" or \"Sorted\"** — ensure `doc_values: true` and use `keyword` or numeric types for best performance.\n"
        "- **Fields in \"Filtered\" with high cardinality** — consider whether a `keyword` type with `eager_global_ordinals` would help.",
    ))
    objects.append(_markdown(
        "md-header-field-heat-time", "Section: Field Heat by Response Time",
        "## Field Heat by Response Time\n"
        "Same fields as above, but ranked by **total response time** instead of count. "
        "A field involved in 10 slow queries ranks higher than one in 1,000 fast queries.\n\n"
        "**How to act on this:**\n"
        "- Fields at the top of these tables are where optimization effort pays off most.\n"
        "- Compare with the count-based tables above: if a field is high here but low by count, it's involved in a few very expensive queries — investigate those query patterns.\n"
        "- If a field is high in both, it's both frequent and slow — highest priority for optimization (better mapping type, adding `doc_values`, or restructuring queries).",
    ))
    objects.append(_markdown(
        "md-header-query-patterns", "Section: Query Patterns",
        "## Query Patterns\n"
        "Structural query templates grouped by shape (leaf values replaced with `?`). "
        "Shows which query patterns dominate traffic and cluster time.\n\n"
        "**How to act on this:**\n"
        "- **Costliest templates** (high total response time) are your best optimization targets — rewrite the query, add caching, or adjust shard routing.\n"
        "- **High-count templates with low avg response time** are fine — volume alone isn't a problem if queries are fast.\n"
        "- **Many unique templates per index group** may indicate dynamic query generation — check if the application is building queries inefficiently.\n"
        "- Use the raw events table (bottom of dashboard) filtered by `query_template_hash` to see actual query bodies for a pattern.",
    ))
    objects.append(_markdown(
        "md-header-lookback", "Section: Lookback Analysis",
        "## Lookback Analysis\n"
        "How far back queries look in time (`now-Xh` ranges) — directly informs ILM tiering decisions.\n\n"
        "**How to act on this:**\n"
        "- If 95% of queries look back ≤ 24h, data older than 24h can move to warm/cold tiers.\n"
        "- If the max lookback is 7d but you're keeping 90d of hot data, you're over-provisioning.\n"
        "- **Avg lookback by group** shows which index groups need the deepest hot retention — set ILM policies per group accordingly.\n"
        "- **Lookback date fields** shows which timestamp fields are used for filtering — ensure these are optimized (`date` type, `doc_values: true`).",
    ))
    objects.append(_markdown(
        "md-header-raw-events", "Section: Raw Events",
        "## Raw Events\n"
        "Individual query-level event log for debugging and inspection.\n\n"
        "**Tips:** Filter by `query_template_hash` to see all executions of a specific query pattern. "
        "Filter by `response_time_ms > 1000` to find slow queries. "
        "Enable *Query Body Storage* in the gateway control panel to see full query text here.",
    ))

    # =========================================================
    # QUERY PATTERN VISUALIZATIONS
    # =========================================================

    # Top Query Templates — table showing most-executed structural patterns
    objects.append(_vis(
        "vis-top-templates", "Top Query Templates (by execution count)", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "avg",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "query_template_hash", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Query Templates Over Time — stacked area showing pattern drift
    objects.append(_vis(
        "vis-templates-over-time", "Query Templates Over Time", "area",
        {"type": "area", "grid": {"categoryLines": False}, **AREA_AXES,
         "seriesParams": [{"show": True, "type": "area", "mode": "stacked",
                           "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2,
                           "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False,
         "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "timestamp", "useNormalizedEsInterval": True,
                        "scaleMetricValues": False, "interval": "auto",
                        "drop_partials": False, "min_doc_count": 1,
                        "extended_bounds": {}},
             "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "query_template_hash", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "group"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Costliest Templates — horizontal bar ranked by total cluster time consumed
    TEMPLATE_COST_AXES = {
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                          "show": True, "style": {}, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 12}, "title": {}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value",
                       "position": "bottom", "show": True, "style": {},
                       "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": "Total Response Time (ms)"}}],
    }
    objects.append(_vis(
        "vis-costliest-templates", "Costliest Query Templates (by total cluster time)", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False}, **TEMPLATE_COST_AXES,
         "seriesParams": [{"show": True, "type": "histogram", "mode": "normal",
                           "valueAxis": "ValueAxis-1",
                           "data": {"label": "Total response_time_ms", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2,
                           "showCircles": True}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "sum",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "query_template_hash", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Template Count per Index Group — how many distinct patterns per group
    objects.append(_vis(
        "vis-templates-by-group", "Unique Templates per Index Group", "table",
        TABLE_PARAMS,
        [
            {"id": "1", "enabled": True, "type": "cardinality",
             "params": {"field": "query_template_hash"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "index_group", "size": 20,
                        "order": "desc", "orderBy": "2"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # =========================================================
    # LOOKBACK VISUALIZATIONS (query time-range window analysis)
    # =========================================================

    # Lookback Distribution — bar chart showing query count per lookback window (e.g. "1h", "24h", "7d")
    LOOKBACK_BAR_AXES = {
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                          "show": True, "style": {}, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100},
                          "title": {"text": "Lookback Window"}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                       "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": "Query Count"}}],
    }
    objects.append(_vis(
        "vis-lookback-distribution", "Lookback Window Distribution", "histogram",
        {"type": "histogram", "grid": {"categoryLines": False}, **LOOKBACK_BAR_AXES,
         "seriesParams": [{"show": True, "type": "histogram", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "lookback_label", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Avg Lookback by Group — horizontal bar showing avg lookback per index_group
    objects.append(_vis(
        "vis-lookback-by-group", "Avg Lookback by Group", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False},
         "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                           "show": True, "style": {}, "scale": {"type": "linear"},
                           "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
         "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value", "position": "bottom",
                        "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "Avg Lookback (seconds)"}}],
         "seriesParams": [{"show": True, "type": "histogram", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Avg lookback_seconds", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "avg", "params": {"field": "lookback_seconds"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Lookback Date Fields — pie chart showing which date fields are used for time filtering
    objects.append(_vis(
        "vis-lookback-fields", "Lookback Date Fields", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "lookback_field", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # =========================================================
    # RAW EVENTS TABLE (Saved Search — shows actual query bodies)
    # =========================================================
    objects.append({
        "id": "search-usage-events",
        "type": "search",
        "attributes": {
            "title": "Usage Events — Raw Queries",
            "columns": ["timestamp", "index_group", "operation", "lookback_label", "path", "query_body", "response_status", "response_time_ms"],
            "sort": [["timestamp", "desc"]],
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": usage_dv_id,
                    "query": {"query": _RAW_EVENTS_ONLY, "language": "kuery"},
                    "filter": [],
                    "highlightAll": True,
                    "version": True,
                })
            },
        },
        "references": [
            {"id": usage_dv_id, "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}
        ],
    })

    # Avg response time (line)
    objects.append(_vis(
        "vis-response-time", "Avg Response Time (ms)", "line",
        {"type": "line", "grid": {"categoryLines": False}, **LINE_AXES,
         "seriesParams": [{"show": True, "type": "line", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Avg response_time_ms", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"}},
        [
            {"id": "1", "enabled": True, "type": "avg", "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram", "params": {"field": "timestamp", "useNormalizedEsInterval": True, "scaleMetricValues": False, "interval": "auto", "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # =========================================================
    # MULTI-INDEX COMPARISON VISUALIZATIONS (grouped by index_group)
    # =========================================================

    # Operations by Index Group (horizontal bar)
    objects.append(_vis(
        "vis-ops-by-index", "Operations by Index Group", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False},
         "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                           "show": True, "style": {}, "scale": {"type": "linear"},
                           "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
         "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value", "position": "bottom",
                        "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "Count"}}],
         "seriesParams": [{"show": True, "type": "histogram", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Operations Over Time by Index Group (area, split by index_group)
    objects.append(_vis(
        "vis-ops-over-time-by-index", "Operations Over Time by Group", "area",
        {"type": "area", "grid": {"categoryLines": False}, **AREA_AXES,
         "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram", "params": {"field": "timestamp", "useNormalizedEsInterval": True, "scaleMetricValues": False, "interval": "auto", "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}}, "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "group"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Operations by Index Group + Operation (horizontal bar, split)
    objects.append(_vis(
        "vis-ops-by-index-operation", "Operations by Group & Type", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False},
         "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                           "show": True, "style": {}, "scale": {"type": "linear"},
                           "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
         "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value", "position": "bottom",
                        "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "Count"}}],
         "seriesParams": [{"show": True, "type": "histogram", "mode": "stacked", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "operation", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "group"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Avg Response Time by Index Group (horizontal bar)
    objects.append(_vis(
        "vis-response-time-by-index", "Avg Response Time by Group", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False},
         "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                           "show": True, "style": {}, "scale": {"type": "linear"},
                           "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
         "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value", "position": "bottom",
                        "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "ms"}}],
         "seriesParams": [{"show": True, "type": "histogram", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Avg response_time_ms", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "avg", "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Index Group breakdown for multi-index comparison
    objects.append(_vis(
        "vis-concrete-indices-comparison", "Index Group Breakdown", "table", TABLE_PARAMS,
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index_group", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "operation", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # =========================================================
    # DASHBOARDS
    # =========================================================

    def panel_ref(panel_idx, vis_id, x, y, w, h):
        return {
            "panelIndex": str(panel_idx),
            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(panel_idx)},
            "version": "8.12.2",
            "type": "visualization",
            "panelRefName": f"panel_{panel_idx}",
        }

    # Products Explorer dashboard
    products_panels = [
        panel_ref(0, "vis-products-table", 0, 0, 32, 18),
        panel_ref(1, "vis-products-by-category", 32, 0, 16, 18),
    ]
    products_refs = [
        {"name": "panel_0", "type": "visualization", "id": "vis-products-table"},
        {"name": "panel_1", "type": "visualization", "id": "vis-products-by-category"},
    ]
    objects.append({
        "id": "products-explorer",
        "type": "dashboard",
        "attributes": {
            "title": "Products Explorer",
            "panelsJSON": json.dumps(products_panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-10y",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": products_refs,
    })

    # Usage & Heat dashboard (with native index_group control at top)
    #
    # Layout rows (each section preceded by a 5-unit Markdown header):
    #   y=0:   [header] Overview
    #   y=5:   index groups, ops over time, query types
    #   y=17:  [header] Field Heat by Count
    #   y=22:  top queried/filtered/aggregated
    #   y=36:  top sorted/sourced + avg response time
    #   y=48:  [header] Field Heat by Response Time
    #   y=53:  queried/filtered/aggregated (time-weighted)
    #   y=67:  sorted/sourced (time-weighted)
    #   y=79:  [header] Query Patterns
    #   y=84:  top templates, templates over time
    #   y=98:  costliest templates, templates by group
    #   y=110: [header] Lookback Analysis
    #   y=115: lookback distribution, lookback fields
    #   y=129: [header] Raw Events
    #   y=134: raw events table
    control_input, control_refs = _control_group_input(usage_dv_id)
    usage_panels = [
        # --- Section: Overview ---
        panel_ref(0,  "md-header-overview",          0,  0, 48,  5),
        panel_ref(1,  "vis-concrete-indices",         0,  5, 18, 12),
        panel_ref(2,  "vis-ops-over-time",           18,  5, 18, 12),
        panel_ref(3,  "vis-query-types",             36,  5, 12, 12),
        # --- Section: Field Heat by Count ---
        panel_ref(4,  "md-header-field-heat-count",   0, 17, 48,  5),
        panel_ref(5,  "vis-top-queried",              0, 22, 16, 14),
        panel_ref(6,  "vis-top-filtered",            16, 22, 16, 14),
        panel_ref(7,  "vis-top-aggregated",          32, 22, 16, 14),
        panel_ref(8,  "vis-top-sorted",               0, 36, 16, 12),
        panel_ref(9,  "vis-top-sourced",             16, 36, 16, 12),
        panel_ref(10, "vis-response-time",           32, 36, 16, 12),
        # --- Section: Field Heat by Response Time ---
        panel_ref(11, "md-header-field-heat-time",    0, 48, 48,  5),
        panel_ref(12, "vis-rt-queried",               0, 53, 16, 14),
        panel_ref(13, "vis-rt-filtered",             16, 53, 16, 14),
        panel_ref(14, "vis-rt-aggregated",           32, 53, 16, 14),
        panel_ref(15, "vis-rt-sorted",                0, 67, 16, 12),
        panel_ref(16, "vis-rt-sourced",              16, 67, 16, 12),
        # --- Section: Query Patterns ---
        panel_ref(17, "md-header-query-patterns",     0, 79, 48,  5),
        panel_ref(18, "vis-top-templates",            0, 84, 24, 14),
        panel_ref(19, "vis-templates-over-time",     24, 84, 24, 14),
        panel_ref(20, "vis-costliest-templates",      0, 98, 24, 12),
        panel_ref(21, "vis-templates-by-group",      24, 98, 24, 12),
        # --- Section: Lookback Analysis ---
        panel_ref(22, "md-header-lookback",           0, 110, 48,  5),
        panel_ref(23, "vis-lookback-distribution",    0, 115, 24, 14),
        panel_ref(24, "vis-lookback-fields",         24, 115, 24, 14),
        # --- Section: Raw Events ---
        panel_ref(25, "md-header-raw-events",         0, 129, 48,  5),
        {
            "panelIndex": "26",
            "gridData": {"x": 0, "y": 134, "w": 48, "h": 18, "i": "26"},
            "version": "8.12.2",
            "type": "search",
            "panelRefName": "panel_26",
        },
    ]
    usage_refs = [
        {"name": "panel_0",  "type": "visualization", "id": "md-header-overview"},
        {"name": "panel_1",  "type": "visualization", "id": "vis-concrete-indices"},
        {"name": "panel_2",  "type": "visualization", "id": "vis-ops-over-time"},
        {"name": "panel_3",  "type": "visualization", "id": "vis-query-types"},
        {"name": "panel_4",  "type": "visualization", "id": "md-header-field-heat-count"},
        {"name": "panel_5",  "type": "visualization", "id": "vis-top-queried"},
        {"name": "panel_6",  "type": "visualization", "id": "vis-top-filtered"},
        {"name": "panel_7",  "type": "visualization", "id": "vis-top-aggregated"},
        {"name": "panel_8",  "type": "visualization", "id": "vis-top-sorted"},
        {"name": "panel_9",  "type": "visualization", "id": "vis-top-sourced"},
        {"name": "panel_10", "type": "visualization", "id": "vis-response-time"},
        {"name": "panel_11", "type": "visualization", "id": "md-header-field-heat-time"},
        {"name": "panel_12", "type": "visualization", "id": "vis-rt-queried"},
        {"name": "panel_13", "type": "visualization", "id": "vis-rt-filtered"},
        {"name": "panel_14", "type": "visualization", "id": "vis-rt-aggregated"},
        {"name": "panel_15", "type": "visualization", "id": "vis-rt-sorted"},
        {"name": "panel_16", "type": "visualization", "id": "vis-rt-sourced"},
        {"name": "panel_17", "type": "visualization", "id": "md-header-query-patterns"},
        {"name": "panel_18", "type": "visualization", "id": "vis-top-templates"},
        {"name": "panel_19", "type": "visualization", "id": "vis-templates-over-time"},
        {"name": "panel_20", "type": "visualization", "id": "vis-costliest-templates"},
        {"name": "panel_21", "type": "visualization", "id": "vis-templates-by-group"},
        {"name": "panel_22", "type": "visualization", "id": "md-header-lookback"},
        {"name": "panel_23", "type": "visualization", "id": "vis-lookback-distribution"},
        {"name": "panel_24", "type": "visualization", "id": "vis-lookback-fields"},
        {"name": "panel_25", "type": "visualization", "id": "md-header-raw-events"},
        {"name": "panel_26", "type": "search", "id": "search-usage-events"},
    ] + control_refs
    objects.append({
        "id": "usage-heat",
        "type": "dashboard",
        "attributes": {
            "title": "Usage & Heat Dashboard",
            "controlGroupInput": control_input,
            "panelsJSON": json.dumps(usage_panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": usage_refs,
    })

    # Multi-Index Comparison dashboard (using index_group native control)
    comp_control_input, comp_control_refs = _control_group_input(usage_dv_id)
    comparison_panels = [
        panel_ref(0, "vis-ops-by-index",               0,  0, 24, 14),
        panel_ref(1, "vis-ops-by-index-operation",    24,  0, 24, 14),
        panel_ref(2, "vis-ops-over-time-by-index",     0, 14, 32, 14),
        panel_ref(3, "vis-response-time-by-index",    32, 14, 16, 14),
        panel_ref(4, "vis-lookback-by-group",          0, 28, 24, 14),   # avg lookback by group
        panel_ref(5, "vis-concrete-indices-comparison", 24, 28, 24, 14),
    ]
    comparison_refs = [
        {"name": "panel_0", "type": "visualization", "id": "vis-ops-by-index"},
        {"name": "panel_1", "type": "visualization", "id": "vis-ops-by-index-operation"},
        {"name": "panel_2", "type": "visualization", "id": "vis-ops-over-time-by-index"},
        {"name": "panel_3", "type": "visualization", "id": "vis-response-time-by-index"},
        {"name": "panel_4", "type": "visualization", "id": "vis-lookback-by-group"},
        {"name": "panel_5", "type": "visualization", "id": "vis-concrete-indices-comparison"},
    ] + comp_control_refs
    objects.append({
        "id": "multi-index-comparison",
        "type": "dashboard",
        "attributes": {
            "title": "Multi-Index Heat Comparison",
            "controlGroupInput": comp_control_input,
            "panelsJSON": json.dumps(comparison_panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": comparison_refs,
    })

    return objects


def import_objects(base_url: str, objects: list[dict]) -> None:
    """Import saved objects via the _import API (NDJSON)."""
    ndjson = "\n".join(json.dumps(obj) for obj in objects) + "\n"

    resp = requests.post(
        f"{base_url}/api/saved_objects/_import",
        headers=HEADERS,
        params={"overwrite": "true"},
        files={"file": ("export.ndjson", io.BytesIO(ndjson.encode()), "application/ndjson")},
    )

    if resp.status_code == 200:
        result = resp.json()
        success = result.get("successCount", 0)
        errors = result.get("errors", [])
        print(f"  Imported {success} objects")
        if errors:
            for err in errors:
                print(f"  ERROR: {err.get('id')}: {json.dumps(err.get('error', {}), indent=2)}")
    else:
        print(f"  WARNING: Import failed: {resp.status_code} {resp.text[:300]}")


def main():
    parser = argparse.ArgumentParser(description="Setup Kibana dashboards")
    parser.add_argument("--kibana", default=KIBANA_URL, help="Kibana URL")
    parser.add_argument("--no-wait", action="store_true", help="Skip waiting for Kibana")
    args = parser.parse_args()

    base = args.kibana.rstrip("/")

    if not args.no_wait:
        wait_for_kibana(base)

    # --- Data Views (with fixed IDs so visualizations can reference them) ---
    print("\nCreating data views...")
    products_dv_id = create_data_view(base, "products", "Products", "dv-products")
    usage_dv_id = create_data_view(base, ".usage-events", "Usage Events", "dv-usage-events", time_field="timestamp")
    logs_dv_id = create_data_view(base, "logs*", "Logs", "dv-logs", time_field="timestamp")
    orders_dv_id = create_data_view(base, "orders*", "Orders", "dv-orders", time_field="order_date")

    # --- Build and import all visualizations + dashboards ---
    print("\nImporting visualizations and dashboards...")
    objects = build_saved_objects(products_dv_id, usage_dv_id, logs_dv_id, orders_dv_id)
    import_objects(base, objects)

    print(f"\nDone! Open Kibana at {base}")
    print(f"  Products:        {base}/app/dashboards#/view/products-explorer")
    print(f"  Usage/Heat:      {base}/app/dashboards#/view/usage-heat")
    print(f"  Multi-Index:     {base}/app/dashboards#/view/multi-index-comparison")
    print(f"  Discover:        {base}/app/discover")


if __name__ == "__main__":
    main()
