"""
Kibana dashboard setup — creates data views, visualizations, and dashboards
via the Kibana saved objects _import API (NDJSON format).

Uses classic visualization saved objects (visState) instead of embedded Lens,
which has a stable, documented format.

Dashboards created:
  1. Products Explorer       — data table of seeded products
  2. Usage & Heat            — query traffic patterns + field heat (with index_group filter)
  3. Multi-Index Comparison  — cross-index-group heat and operations
  4. Mapping Diff            — field usage vs. mapping comparison (from .mapping-diff index)
  5. Field Drill-Down        — per-field usage investigation (clients, templates, response time)

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


def build_saved_objects(products_dv_id: str, usage_dv_id: str, logs_dv_id: str, orders_dv_id: str, diff_dv_id: str = "dv-mapping-diff") -> list[dict]:
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
        "md-header-client-attribution", "Section: Client Attribution",
        "## Client Attribution\n"
        "Which clients (services, users, tools) are sending queries, broken down by IP, User-Agent, and optional `x-client-id` header.\n\n"
        "**How to act on this:**\n"
        "- **Before changing a field mapping**, check the *Fields by Client* table below — it tells you exactly which clients depend on that field and who to notify.\n"
        "- **Unknown IPs or User-Agents** appearing in traffic may indicate unauthorized access, runaway scripts, or a new service that hasn't registered an `x-client-id` header.\n"
        "- **Encourage teams to set the `x-client-id` header** — IP and User-Agent help, but an explicit ID (e.g. `order-service`) makes impact analysis trivial.\n"
        "- **High-traffic clients** (top of the count table) should be checked for query efficiency — a single service generating 80% of traffic is your best optimization target.",
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
    # CLIENT ATTRIBUTION VISUALIZATIONS
    # =========================================================

    # Top Clients — table showing most active clients by count
    objects.append(_vis(
        "vis-top-clients", "Top Clients (by request count)", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "avg",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "client_id", "size": 20,
                        "order": "desc", "orderBy": "1",
                        "missingBucket": True, "missingBucketLabel": "(no x-client-id)"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Client Traffic by IP — table showing distinct client IPs
    objects.append(_vis(
        "vis-clients-by-ip", "Client Traffic by IP", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "cardinality",
             "params": {"field": "client_user_agent"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "client_ip", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Client Usage by Index Group — which clients hit which indices
    objects.append(_vis(
        "vis-clients-by-group", "Client Usage by Index Group", "table",
        TABLE_PARAMS,
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "client_id", "size": 20,
                        "order": "desc", "orderBy": "1",
                        "missingBucket": True, "missingBucketLabel": "(no x-client-id)"},
             "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "index_group", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Fields by Client — which clients depend on which fields (queried + filtered)
    objects.append(_vis(
        "vis-fields-by-client", "Queried & Filtered Fields by Client", "table",
        TABLE_PARAMS,
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "client_id", "size": 20,
                        "order": "desc", "orderBy": "1",
                        "missingBucket": True, "missingBucketLabel": "(no x-client-id)"},
             "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "fields.queried", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Client User-Agents — pie chart to quickly identify Kibana vs app vs script traffic
    objects.append(_vis(
        "vis-client-user-agents", "Client User-Agents", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "client_user_agent", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
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
            "columns": ["timestamp", "index_group", "operation", "client_id", "client_ip", "client_user_agent", "lookback_label", "path", "query_body", "response_status", "response_time_ms"],
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
    # MAPPING DIFF VISUALIZATIONS
    # =========================================================

    objects.append(_markdown(
        "md-header-multi-index", "Section: Multi-Index Comparison",
        "## Multi-Index Heat Comparison\n"
        "Side-by-side comparison of traffic volume, operation mix, and response time across all index groups.\n\n"
        "**How to act on this:**\n"
        "- **Index groups with disproportionately high traffic** relative to their data size may need more replicas or dedicated nodes.\n"
        "- **Groups with high avg response time** are candidates for query optimization, shard rebalancing, or mapping changes.\n"
        "- **Compare operation types per group** — a group dominated by writes may benefit from different refresh/flush settings than a read-heavy one.\n"
        "- **Lookback differences** between groups inform per-group ILM policies — groups queried over short windows can move older data to cold tiers sooner.",
    ))

    objects.append(_markdown(
        "md-header-diff-overview", "Section: Mapping Diff Overview",
        "## Mapping vs. Usage Diff\n"
        "Every field in your index mappings compared against actual usage from traffic. "
        "Fields are classified as **active** (queried/filtered/aggregated/sorted), "
        "**sourced_only** (fetched in `_source` but never queried), "
        "**write_only** (indexed but never read), or **unused** (zero references). "
        "The **Last Seen** column shows when the field was last referenced — use it to prioritize which fields to investigate first.\n\n"
        "**How to act on this:**\n"
        "- **Unused keyword/numeric fields** with `is_indexed: true` → set `index: false` to save indexing CPU and disk.\n"
        "- **Unused text fields** → remove the field or its `.keyword` multi-field to save significant disk.\n"
        "- **Write-only fields** → data is being indexed but never searched — consider `index: false` + `doc_values: false`.\n"
        "- **Sourced-only fields** → fetched in responses but never used in queries — set `index: false`, keep `doc_values` if needed for sorts/aggs.\n\n"
        "**Drill-down per field:** Open the **Field Drill-Down** dashboard, select the field name "
        "in one of the category dropdowns (Queried, Filtered, or Aggregated), and see who uses it, "
        "when it was last accessed, which query templates reference it, and the response time impact.",
    ))

    objects.append(_vis(
        "vis-diff-classification", "Field Classification Breakdown", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True,
         "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "classification", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
        ],
        diff_dv_id,
    ))

    DIFF_BAR_AXES = {
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                          "show": True, "style": {}, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1", "type": "value",
                       "position": "bottom", "show": True, "style": {},
                       "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": "Field Count"}}],
    }
    objects.append(_vis(
        "vis-diff-by-group", "Field Classification by Index Group", "horizontal_bar",
        {"type": "horizontal_bar", "grid": {"categoryLines": False}, **DIFF_BAR_AXES,
         "seriesParams": [{"show": True, "type": "histogram", "mode": "stacked",
                           "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2,
                           "showCircles": True}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "index_group", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "classification", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "group"},
        ],
        diff_dv_id,
    ))

    objects.append(_vis(
        "vis-diff-fields-table", "All Fields — Mapping vs. Usage", "table",
        {**TABLE_PARAMS, "perPage": 25, "totalFunc": "count"},
        [
            {"id": "1", "enabled": True, "type": "max",
             "params": {"field": "total_references"}, "schema": "metric"},
            {"id": "6", "enabled": True, "type": "max",
             "params": {"field": "last_seen", "customLabel": "Last Seen"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "index_group", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "field_name", "size": 200,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "4", "enabled": True, "type": "terms",
             "params": {"field": "classification", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "5", "enabled": True, "type": "terms",
             "params": {"field": "mapped_type", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        diff_dv_id,
    ))

    objects.append(_vis(
        "vis-diff-unused", "Unused Fields (Optimization Candidates)", "table",
        {**TABLE_PARAMS, "perPage": 25},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "6", "enabled": True, "type": "max",
             "params": {"field": "last_seen", "customLabel": "Last Seen"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "index_group", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "field_name", "size": 200,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "4", "enabled": True, "type": "terms",
             "params": {"field": "mapped_type", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
            {"id": "5", "enabled": True, "type": "terms",
             "params": {"field": "is_indexed", "size": 5,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        diff_dv_id,
        search_query="classification: unused",
    ))

    objects.append(_vis(
        "vis-diff-types", "Field Type Distribution", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True,
         "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "mapped_type", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
        ],
        diff_dv_id,
    ))

    # =========================================================
    # FIELD DRILL-DOWN VISUALIZATIONS
    # =========================================================
    # Used on the "Field Drill-Down" dashboard. Users pick a field name
    # via controls; all panels filter to events referencing that field.

    objects.append(_markdown(
        "md-header-field-drilldown", "Section: Field Drill-Down",
        "## Field Drill-Down\n"
        "Investigate a specific field's usage: who queries it, when, with which query patterns, and how fast.\n\n"
        "**How to use this dashboard:**\n"
        "1. Select an **Index Group** to focus on one index.\n"
        "2. Pick a field name in **one** of the category dropdowns (Queried, Filtered, or Aggregated) "
        "to filter to events that reference that field.\n"
        "3. All panels below update to show only events matching your selection.\n\n"
        "**Cross-category search:** The dropdowns filter one category at a time. "
        "To find a field across ALL categories (queried + filtered + aggregated + sorted + sourced), "
        "use the KQL query bar at the top:\n\n"
        "`fields.queried: \"price\" OR fields.filtered: \"price\" OR fields.aggregated: \"price\" "
        "OR fields.sorted: \"price\" OR fields.sourced: \"price\"`\n\n"
        "**Tip:** Come here from the *Mapping Diff* dashboard — pick a field classified as "
        "*unused* or *sourced_only* and verify whether it's truly safe to remove by checking who depends on it.",
    ))

    # Usage Over Time — when was this field used
    objects.append(_vis(
        "vis-drilldown-usage-time", "Field Usage Over Time", "area",
        {"type": "area", "grid": {"categoryLines": False}, **AREA_AXES,
         "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2,
                           "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "timestamp", "useNormalizedEsInterval": True,
                        "scaleMetricValues": False, "interval": "auto",
                        "drop_partials": False, "min_doc_count": 1,
                        "extended_bounds": {}},
             "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Operations breakdown
    objects.append(_vis(
        "vis-drilldown-operations", "Operations Breakdown", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "operation", "size": 10, "order": "desc", "orderBy": "1"},
             "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Clients using this field
    objects.append(_vis(
        "vis-drilldown-clients", "Clients Using This Field", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "avg",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "client_id", "size": 20,
                        "order": "desc", "orderBy": "1",
                        "missingBucket": True, "missingBucketLabel": "(no x-client-id)"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Client IPs
    objects.append(_vis(
        "vis-drilldown-client-ips", "Client IPs Using This Field", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "cardinality",
             "params": {"field": "client_user_agent"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "terms",
             "params": {"field": "client_ip", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # User-Agent breakdown
    objects.append(_vis(
        "vis-drilldown-user-agents", "User-Agents Touching This Field", "pie",
        {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
         "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "terms",
             "params": {"field": "client_user_agent", "size": 10,
                        "order": "desc", "orderBy": "1"},
             "schema": "segment"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Query templates referencing this field
    objects.append(_vis(
        "vis-drilldown-templates", "Query Templates Referencing This Field", "table",
        {**TABLE_PARAMS, "percentageCol": "Count"},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "avg",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "3", "enabled": True, "type": "sum",
             "params": {"field": "response_time_ms", "customLabel": "Total Response Time"},
             "schema": "metric"},
            {"id": "4", "enabled": True, "type": "terms",
             "params": {"field": "query_template_hash", "size": 20,
                        "order": "desc", "orderBy": "1"},
             "schema": "bucket"},
        ],
        usage_dv_id,
        search_query=_RAW_EVENTS_ONLY,
    ))

    # Response time over time
    objects.append(_vis(
        "vis-drilldown-response-time", "Response Time Over Time", "line",
        {"type": "line", "grid": {"categoryLines": False}, **LINE_AXES,
         "seriesParams": [{"show": True, "type": "line", "mode": "normal", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Avg response_time_ms", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2,
                           "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": False,
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "avg",
             "params": {"field": "response_time_ms"}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram",
             "params": {"field": "timestamp", "useNormalizedEsInterval": True,
                        "scaleMetricValues": False, "interval": "auto",
                        "drop_partials": False, "min_doc_count": 1,
                        "extended_bounds": {}},
             "schema": "segment"},
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
    # Layout rows (each section preceded by a 10-unit Markdown header):
    #   y=0:   [header] Overview
    #   y=10:  index groups, ops over time, query types
    #   y=22:  [header] Field Heat by Count
    #   y=32:  top queried/filtered/aggregated
    #   y=46:  top sorted/sourced + avg response time
    #   y=58:  [header] Field Heat by Response Time
    #   y=68:  queried/filtered/aggregated (time-weighted)
    #   y=82:  sorted/sourced (time-weighted)
    #   y=94:  [header] Query Patterns
    #   y=104: top templates, templates over time
    #   y=118: costliest templates, templates by group
    #   y=130: [header] Client Attribution
    #   y=140: top clients, clients by IP, user-agents
    #   y=154: clients by group, fields by client
    #   y=168: [header] Lookback Analysis
    #   y=178: lookback distribution, lookback fields
    #   y=192: [header] Raw Events
    #   y=202: raw events table
    control_input, control_refs = _control_group_input(usage_dv_id)
    usage_panels = [
        # --- Section: Overview ---
        panel_ref(0,  "md-header-overview",          0,   0, 48, 10),
        panel_ref(1,  "vis-concrete-indices",         0,  10, 18, 12),
        panel_ref(2,  "vis-ops-over-time",           18,  10, 18, 12),
        panel_ref(3,  "vis-query-types",             36,  10, 12, 12),
        # --- Section: Field Heat by Count ---
        panel_ref(4,  "md-header-field-heat-count",   0,  22, 48, 10),
        panel_ref(5,  "vis-top-queried",              0,  32, 16, 14),
        panel_ref(6,  "vis-top-filtered",            16,  32, 16, 14),
        panel_ref(7,  "vis-top-aggregated",          32,  32, 16, 14),
        panel_ref(8,  "vis-top-sorted",               0,  46, 16, 12),
        panel_ref(9,  "vis-top-sourced",             16,  46, 16, 12),
        panel_ref(10, "vis-response-time",           32,  46, 16, 12),
        # --- Section: Field Heat by Response Time ---
        panel_ref(11, "md-header-field-heat-time",    0,  58, 48, 10),
        panel_ref(12, "vis-rt-queried",               0,  68, 16, 14),
        panel_ref(13, "vis-rt-filtered",             16,  68, 16, 14),
        panel_ref(14, "vis-rt-aggregated",           32,  68, 16, 14),
        panel_ref(15, "vis-rt-sorted",                0,  82, 16, 12),
        panel_ref(16, "vis-rt-sourced",              16,  82, 16, 12),
        # --- Section: Query Patterns ---
        panel_ref(17, "md-header-query-patterns",     0,  94, 48, 10),
        panel_ref(18, "vis-top-templates",            0, 104, 24, 14),
        panel_ref(19, "vis-templates-over-time",     24, 104, 24, 14),
        panel_ref(20, "vis-costliest-templates",      0, 118, 24, 12),
        panel_ref(21, "vis-templates-by-group",      24, 118, 24, 12),
        # --- Section: Client Attribution ---
        panel_ref(22, "md-header-client-attribution",  0, 130, 48, 10),
        panel_ref(23, "vis-top-clients",               0, 140, 16, 14),
        panel_ref(24, "vis-clients-by-ip",            16, 140, 16, 14),
        panel_ref(25, "vis-client-user-agents",       32, 140, 16, 14),
        panel_ref(26, "vis-clients-by-group",          0, 154, 24, 14),
        panel_ref(27, "vis-fields-by-client",         24, 154, 24, 14),
        # --- Section: Lookback Analysis ---
        panel_ref(28, "md-header-lookback",            0, 168, 48, 10),
        panel_ref(29, "vis-lookback-distribution",     0, 178, 24, 14),
        panel_ref(30, "vis-lookback-fields",          24, 178, 24, 14),
        # --- Section: Raw Events ---
        panel_ref(31, "md-header-raw-events",          0, 192, 48, 10),
        {
            "panelIndex": "32",
            "gridData": {"x": 0, "y": 202, "w": 48, "h": 18, "i": "32"},
            "version": "8.12.2",
            "type": "search",
            "panelRefName": "panel_32",
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
        {"name": "panel_22", "type": "visualization", "id": "md-header-client-attribution"},
        {"name": "panel_23", "type": "visualization", "id": "vis-top-clients"},
        {"name": "panel_24", "type": "visualization", "id": "vis-clients-by-ip"},
        {"name": "panel_25", "type": "visualization", "id": "vis-client-user-agents"},
        {"name": "panel_26", "type": "visualization", "id": "vis-clients-by-group"},
        {"name": "panel_27", "type": "visualization", "id": "vis-fields-by-client"},
        {"name": "panel_28", "type": "visualization", "id": "md-header-lookback"},
        {"name": "panel_29", "type": "visualization", "id": "vis-lookback-distribution"},
        {"name": "panel_30", "type": "visualization", "id": "vis-lookback-fields"},
        {"name": "panel_31", "type": "visualization", "id": "md-header-raw-events"},
        {"name": "panel_32", "type": "search", "id": "search-usage-events"},
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
        panel_ref(0, "md-header-multi-index",          0,  0, 48, 10),
        panel_ref(1, "vis-ops-by-index",               0, 10, 24, 14),
        panel_ref(2, "vis-ops-by-index-operation",    24, 10, 24, 14),
        panel_ref(3, "vis-ops-over-time-by-index",     0, 24, 32, 14),
        panel_ref(4, "vis-response-time-by-index",    32, 24, 16, 14),
        panel_ref(5, "vis-lookback-by-group",          0, 38, 24, 14),   # avg lookback by group
        panel_ref(6, "vis-concrete-indices-comparison", 24, 38, 24, 14),
    ]
    comparison_refs = [
        {"name": "panel_0", "type": "visualization", "id": "md-header-multi-index"},
        {"name": "panel_1", "type": "visualization", "id": "vis-ops-by-index"},
        {"name": "panel_2", "type": "visualization", "id": "vis-ops-by-index-operation"},
        {"name": "panel_3", "type": "visualization", "id": "vis-ops-over-time-by-index"},
        {"name": "panel_4", "type": "visualization", "id": "vis-response-time-by-index"},
        {"name": "panel_5", "type": "visualization", "id": "vis-lookback-by-group"},
        {"name": "panel_6", "type": "visualization", "id": "vis-concrete-indices-comparison"},
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

    # Mapping Diff dashboard
    diff_control_input, diff_control_refs = _control_group_input(diff_dv_id)
    diff_panels = [
        panel_ref(0, "md-header-diff-overview",  0,  0, 48, 10),
        panel_ref(1, "vis-diff-classification",   0, 10, 16, 14),
        panel_ref(2, "vis-diff-by-group",        16, 10, 32, 14),
        panel_ref(3, "vis-diff-fields-table",     0, 24, 48, 20),
        panel_ref(4, "vis-diff-unused",           0, 44, 36, 16),
        panel_ref(5, "vis-diff-types",           36, 44, 12, 16),
    ]
    diff_refs = [
        {"name": "panel_0", "type": "visualization", "id": "md-header-diff-overview"},
        {"name": "panel_1", "type": "visualization", "id": "vis-diff-classification"},
        {"name": "panel_2", "type": "visualization", "id": "vis-diff-by-group"},
        {"name": "panel_3", "type": "visualization", "id": "vis-diff-fields-table"},
        {"name": "panel_4", "type": "visualization", "id": "vis-diff-unused"},
        {"name": "panel_5", "type": "visualization", "id": "vis-diff-types"},
    ] + diff_control_refs
    objects.append({
        "id": "mapping-diff",
        "type": "dashboard",
        "attributes": {
            "title": "Mapping Diff — Field Usage vs. Mapping",
            "controlGroupInput": diff_control_input,
            "panelsJSON": json.dumps(diff_panels),
            "optionsJSON": json.dumps({
                "useMargins": True, "syncColors": True, "syncCursor": True,
                "syncTooltips": False, "hidePanelTitles": False,
            }),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"}, "filter": [],
                })
            },
        },
        "references": diff_refs,
    })

    # Field Drill-Down dashboard (per-field investigation from .usage-events)
    # Controls: index_group + field category dropdowns (queried, filtered, aggregated).
    # User picks a field in ONE category dropdown; all panels filter to matching events.
    # chainingSystem=NONE so controls don't cascade-filter each other's options.
    drilldown_control_panels = {
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
        "1": {
            "type": "optionsListControl",
            "order": 1,
            "width": "medium",
            "grow": True,
            "explicitInput": {
                "id": "1",
                "fieldName": "fields.queried",
                "title": "Queried Field",
                "selectedOptions": [],
                "enhancements": {},
                "singleSelect": True,
                "searchTechnique": "prefix",
            },
        },
        "2": {
            "type": "optionsListControl",
            "order": 2,
            "width": "medium",
            "grow": True,
            "explicitInput": {
                "id": "2",
                "fieldName": "fields.filtered",
                "title": "Filtered Field",
                "selectedOptions": [],
                "enhancements": {},
                "singleSelect": True,
                "searchTechnique": "prefix",
            },
        },
        "3": {
            "type": "optionsListControl",
            "order": 3,
            "width": "medium",
            "grow": True,
            "explicitInput": {
                "id": "3",
                "fieldName": "fields.aggregated",
                "title": "Aggregated Field",
                "selectedOptions": [],
                "enhancements": {},
                "singleSelect": True,
                "searchTechnique": "prefix",
            },
        },
    }
    drilldown_control_input = {
        "chainingSystem": "NONE",
        "controlStyle": "oneLine",
        "showApplySelections": False,
        "ignoreParentSettingsJSON": json.dumps({
            "ignoreFilters": False,
            "ignoreQuery": False,
            "ignoreTimerange": False,
            "ignoreValidations": False,
        }),
        "panelsJSON": json.dumps(drilldown_control_panels),
    }
    drilldown_control_refs = [
        {"name": "controlGroup_0:optionsListDataView", "type": "index-pattern", "id": usage_dv_id},
        {"name": "controlGroup_1:optionsListDataView", "type": "index-pattern", "id": usage_dv_id},
        {"name": "controlGroup_2:optionsListDataView", "type": "index-pattern", "id": usage_dv_id},
        {"name": "controlGroup_3:optionsListDataView", "type": "index-pattern", "id": usage_dv_id},
    ]

    drilldown_panels = [
        panel_ref(0,  "md-header-field-drilldown",  0,  0, 48,  8),
        panel_ref(1,  "vis-drilldown-usage-time",    0,  8, 32, 14),
        panel_ref(2,  "vis-drilldown-operations",   32,  8, 16, 14),
        panel_ref(3,  "vis-drilldown-clients",       0, 22, 16, 14),
        panel_ref(4,  "vis-drilldown-client-ips",   16, 22, 16, 14),
        panel_ref(5,  "vis-drilldown-user-agents",  32, 22, 16, 14),
        panel_ref(6,  "vis-drilldown-templates",     0, 36, 24, 14),
        panel_ref(7,  "vis-drilldown-response-time",24, 36, 24, 14),
        {
            "panelIndex": "8",
            "gridData": {"x": 0, "y": 50, "w": 48, "h": 18, "i": "8"},
            "version": "8.12.2",
            "type": "search",
            "panelRefName": "panel_8",
        },
    ]
    drilldown_refs = [
        {"name": "panel_0", "type": "visualization", "id": "md-header-field-drilldown"},
        {"name": "panel_1", "type": "visualization", "id": "vis-drilldown-usage-time"},
        {"name": "panel_2", "type": "visualization", "id": "vis-drilldown-operations"},
        {"name": "panel_3", "type": "visualization", "id": "vis-drilldown-clients"},
        {"name": "panel_4", "type": "visualization", "id": "vis-drilldown-client-ips"},
        {"name": "panel_5", "type": "visualization", "id": "vis-drilldown-user-agents"},
        {"name": "panel_6", "type": "visualization", "id": "vis-drilldown-templates"},
        {"name": "panel_7", "type": "visualization", "id": "vis-drilldown-response-time"},
        {"name": "panel_8", "type": "search", "id": "search-usage-events"},
    ] + drilldown_control_refs
    objects.append({
        "id": "field-drilldown",
        "type": "dashboard",
        "attributes": {
            "title": "Field Drill-Down — Who Uses This Field?",
            "controlGroupInput": drilldown_control_input,
            "panelsJSON": json.dumps(drilldown_panels),
            "optionsJSON": json.dumps({
                "useMargins": True, "syncColors": True, "syncCursor": True,
                "syncTooltips": False, "hidePanelTitles": False,
            }),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-7d",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"}, "filter": [],
                })
            },
        },
        "references": drilldown_refs,
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


def ensure_mapping_diff_index(es_url: str) -> None:
    """Create the .mapping-diff index in ES if it doesn't exist.

    Needed so the Kibana data view can discover the field schema
    even before the gateway's diff loop has run.
    """
    from gateway.mapping_diff import MAPPING_DIFF_INDEX, MAPPING_DIFF_INDEX_MAPPING

    try:
        resp = requests.head(f"{es_url}/{MAPPING_DIFF_INDEX}")
        if resp.status_code == 200:
            print(f"  .mapping-diff index already exists")
            return
        resp = requests.put(
            f"{es_url}/{MAPPING_DIFF_INDEX}",
            json=MAPPING_DIFF_INDEX_MAPPING,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code in (200, 201):
            print(f"  Created .mapping-diff index")
        else:
            print(f"  WARNING: Failed to create .mapping-diff index: {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as exc:
        print(f"  WARNING: Could not ensure .mapping-diff index: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Setup Kibana dashboards")
    parser.add_argument("--kibana", default=KIBANA_URL, help="Kibana URL")
    parser.add_argument("--no-wait", action="store_true", help="Skip waiting for Kibana")
    args = parser.parse_args()

    base = args.kibana.rstrip("/")

    if not args.no_wait:
        wait_for_kibana(base)

    # --- Ensure ES indices exist for data views ---
    from config import ES_HOST
    print("\nEnsuring ES indices...")
    ensure_mapping_diff_index(ES_HOST)

    # --- Data Views (with fixed IDs so visualizations can reference them) ---
    print("\nCreating data views...")
    products_dv_id = create_data_view(base, "products", "Products", "dv-products")
    usage_dv_id = create_data_view(base, ".usage-events", "Usage Events", "dv-usage-events", time_field="timestamp")
    logs_dv_id = create_data_view(base, "logs*", "Logs", "dv-logs", time_field="timestamp")
    orders_dv_id = create_data_view(base, "orders*", "Orders", "dv-orders", time_field="order_date")
    diff_dv_id = create_data_view(base, ".mapping-diff", "Mapping Diff", "dv-mapping-diff", time_field="timestamp")

    # --- Build and import all visualizations + dashboards ---
    print("\nImporting visualizations and dashboards...")
    objects = build_saved_objects(products_dv_id, usage_dv_id, logs_dv_id, orders_dv_id, diff_dv_id)
    import_objects(base, objects)

    print(f"\nDone! Open Kibana at {base}")
    print(f"  Products:        {base}/app/dashboards#/view/products-explorer")
    print(f"  Usage/Heat:      {base}/app/dashboards#/view/usage-heat")
    print(f"  Multi-Index:     {base}/app/dashboards#/view/multi-index-comparison")
    print(f"  Mapping Diff:    {base}/app/dashboards#/view/mapping-diff")
    print(f"  Field Drill-Down:{base}/app/dashboards#/view/field-drilldown")
    print(f"  Discover:        {base}/app/discover")


if __name__ == "__main__":
    main()
