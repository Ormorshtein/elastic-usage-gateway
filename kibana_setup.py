"""
Kibana dashboard setup — creates data views, visualizations, and dashboards
via the Kibana saved objects _import API (NDJSON format).

Uses classic visualization saved objects (visState) instead of embedded Lens,
which has a stable, documented format.

Dashboards created:
  1. Products Explorer       — data table of seeded products
  2. Usage & Heat            — query traffic patterns + field heat
  3. Multi-Index Comparison  — cross-index heat and operations

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

def _vis(vis_id, title, vis_type, vis_state_params, aggs, index_pattern_id):
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
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": [
            {"id": index_pattern_id, "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}
        ],
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
        ))

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
    ))

    # =========================================================
    # MULTI-INDEX COMPARISON VISUALIZATIONS
    # =========================================================

    # Operations by Index (horizontal bar)
    objects.append(_vis(
        "vis-ops-by-index", "Operations by Index", "horizontal_bar",
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
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
    ))

    # Operations Over Time by Index (area, split by index)
    objects.append(_vis(
        "vis-ops-over-time-by-index", "Operations Over Time by Index", "area",
        {"type": "area", "grid": {"categoryLines": False}, **AREA_AXES,
         "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "valueAxis": "ValueAxis-1",
                           "data": {"label": "Count", "id": "1"},
                           "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True, "interpolate": "linear"}],
         "addTooltip": True, "addLegend": True, "legendPosition": "right",
         "times": [], "addTimeMarker": False, "thresholdLine": {"show": False}},
        [
            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
            {"id": "2", "enabled": True, "type": "date_histogram", "params": {"field": "timestamp", "useNormalizedEsInterval": True, "scaleMetricValues": False, "interval": "auto", "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}}, "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "index", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "group"},
        ],
        usage_dv_id,
    ))

    # Operations by Index + Operation (horizontal bar, split)
    objects.append(_vis(
        "vis-ops-by-index-operation", "Operations by Index & Type", "horizontal_bar",
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
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
            {"id": "3", "enabled": True, "type": "terms", "params": {"field": "operation", "size": 10, "order": "desc", "orderBy": "1"}, "schema": "group"},
        ],
        usage_dv_id,
    ))

    # Avg Response Time by Index (horizontal bar)
    objects.append(_vis(
        "vis-response-time-by-index", "Avg Response Time by Index", "horizontal_bar",
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
            {"id": "2", "enabled": True, "type": "terms", "params": {"field": "index", "size": 20, "order": "desc", "orderBy": "1"}, "schema": "segment"},
        ],
        usage_dv_id,
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
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": products_refs,
    })

    # Usage & Heat dashboard
    usage_panels = [
        panel_ref(0, "vis-ops-over-time",   0,  0, 30, 12),
        panel_ref(1, "vis-query-types",     30,  0, 18, 12),
        panel_ref(2, "vis-top-queried",      0, 12, 16, 14),
        panel_ref(3, "vis-top-filtered",    16, 12, 16, 14),
        panel_ref(4, "vis-top-aggregated",  32, 12, 16, 14),
        panel_ref(5, "vis-top-sorted",       0, 26, 16, 12),
        panel_ref(6, "vis-top-sourced",     16, 26, 16, 12),
        panel_ref(7, "vis-response-time",   32, 26, 16, 12),
    ]
    usage_refs = [
        {"name": "panel_0", "type": "visualization", "id": "vis-ops-over-time"},
        {"name": "panel_1", "type": "visualization", "id": "vis-query-types"},
        {"name": "panel_2", "type": "visualization", "id": "vis-top-queried"},
        {"name": "panel_3", "type": "visualization", "id": "vis-top-filtered"},
        {"name": "panel_4", "type": "visualization", "id": "vis-top-aggregated"},
        {"name": "panel_5", "type": "visualization", "id": "vis-top-sorted"},
        {"name": "panel_6", "type": "visualization", "id": "vis-top-sourced"},
        {"name": "panel_7", "type": "visualization", "id": "vis-response-time"},
    ]
    objects.append({
        "id": "usage-heat",
        "type": "dashboard",
        "attributes": {
            "title": "Usage & Heat Dashboard",
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

    # Multi-Index Comparison dashboard
    comparison_panels = [
        panel_ref(0, "vis-ops-by-index",              0,  0, 24, 14),
        panel_ref(1, "vis-ops-by-index-operation",    24,  0, 24, 14),
        panel_ref(2, "vis-ops-over-time-by-index",     0, 14, 32, 14),
        panel_ref(3, "vis-response-time-by-index",    32, 14, 16, 14),
    ]
    comparison_refs = [
        {"name": "panel_0", "type": "visualization", "id": "vis-ops-by-index"},
        {"name": "panel_1", "type": "visualization", "id": "vis-ops-by-index-operation"},
        {"name": "panel_2", "type": "visualization", "id": "vis-ops-over-time-by-index"},
        {"name": "panel_3", "type": "visualization", "id": "vis-response-time-by-index"},
    ]
    objects.append({
        "id": "multi-index-comparison",
        "type": "dashboard",
        "attributes": {
            "title": "Multi-Index Heat Comparison",
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
    products_dv_id = create_data_view(base, "products", "Products", "dv-products", time_field="created_at")
    usage_dv_id = create_data_view(base, ".usage-events", "Usage Events", "dv-usage-events", time_field="timestamp")
    logs_dv_id = create_data_view(base, "logs", "Logs", "dv-logs", time_field="timestamp")
    orders_dv_id = create_data_view(base, "orders", "Orders", "dv-orders", time_field="order_date")

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
