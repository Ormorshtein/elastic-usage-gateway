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
  5. Mapping Recommendations — actionable optimization advice (from .mapping-recommendations index)
  6. Field Drill-Down        — per-field usage investigation (clients, templates, response time)
  7. Index Architecture      — index-level structural recommendations (from .index-recommendations index)

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



from kibana_objects import build_saved_objects


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


def ensure_recommendations_index(es_url: str) -> None:
    """Create the .mapping-recommendations index in ES if it doesn't exist.

    Needed so the Kibana data view can discover the field schema
    even before the gateway's recommendations loop has run.
    """
    from gateway.recommender import RECOMMENDATIONS_INDEX, RECOMMENDATIONS_INDEX_MAPPING

    try:
        resp = requests.head(f"{es_url}/{RECOMMENDATIONS_INDEX}")
        if resp.status_code == 200:
            print(f"  .mapping-recommendations index already exists")
            return
        resp = requests.put(
            f"{es_url}/{RECOMMENDATIONS_INDEX}",
            json=RECOMMENDATIONS_INDEX_MAPPING,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code in (200, 201):
            print(f"  Created .mapping-recommendations index")
        else:
            print(f"  WARNING: Failed to create .mapping-recommendations index: {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as exc:
        print(f"  WARNING: Could not ensure .mapping-recommendations index: {exc}")


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


def ensure_index_arch_index(es_url: str) -> None:
    """Create the .index-recommendations index in ES if it doesn't exist.

    Needed so the Kibana data view can discover the field schema
    even before the gateway's index arch loop has run.
    """
    from gateway.index_arch import INDEX_ARCH_INDEX, INDEX_ARCH_INDEX_MAPPING

    try:
        resp = requests.head(f"{es_url}/{INDEX_ARCH_INDEX}")
        if resp.status_code == 200:
            print(f"  .index-recommendations index already exists")
            return
        resp = requests.put(
            f"{es_url}/{INDEX_ARCH_INDEX}",
            json=INDEX_ARCH_INDEX_MAPPING,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code in (200, 201):
            print(f"  Created .index-recommendations index")
        else:
            print(f"  WARNING: Failed to create .index-recommendations index: {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as exc:
        print(f"  WARNING: Could not ensure .index-recommendations index: {exc}")


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
    ensure_recommendations_index(ES_HOST)
    ensure_index_arch_index(ES_HOST)

    # --- Data Views (with fixed IDs so visualizations can reference them) ---
    print("\nCreating data views...")
    products_dv_id = create_data_view(base, "products", "Products", "dv-products")
    usage_dv_id = create_data_view(base, ".usage-events", "Usage Events", "dv-usage-events", time_field="timestamp")
    logs_dv_id = create_data_view(base, "logs*", "Logs", "dv-logs", time_field="timestamp")
    orders_dv_id = create_data_view(base, "orders*", "Orders", "dv-orders", time_field="order_date")
    diff_dv_id = create_data_view(base, ".mapping-diff", "Mapping Diff", "dv-mapping-diff", time_field="timestamp")
    rec_dv_id = create_data_view(base, ".mapping-recommendations", "Recommendations", "dv-recommendations", time_field="timestamp")
    arch_dv_id = create_data_view(base, ".index-recommendations", "Index Architecture", "dv-index-arch", time_field="timestamp")

    # --- Build and import all visualizations + dashboards ---
    print("\nImporting visualizations and dashboards...")
    objects = build_saved_objects(products_dv_id, usage_dv_id, logs_dv_id, orders_dv_id, diff_dv_id, rec_dv_id, arch_dv_id)
    import_objects(base, objects)

    print(f"\nDone! Open Kibana at {base}")
    print(f"  Products:          {base}/app/dashboards#/view/products-explorer")
    print(f"  Usage/Heat:        {base}/app/dashboards#/view/usage-heat")
    print(f"  Multi-Index:       {base}/app/dashboards#/view/multi-index-comparison")
    print(f"  Mapping Diff:      {base}/app/dashboards#/view/mapping-diff")
    print(f"  Recommendations:   {base}/app/dashboards#/view/mapping-recommendations")
    print(f"  Field Drill-Down:  {base}/app/dashboards#/view/field-drilldown")
    print(f"  Index Arch:        {base}/app/dashboards#/view/index-architecture")
    print(f"  Discover:          {base}/app/discover")


if __name__ == "__main__":
    main()
