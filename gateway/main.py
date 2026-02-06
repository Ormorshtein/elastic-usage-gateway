"""
FastAPI application — the ES Usage Gateway entry point.

Routes:
  /_gateway/ui             — control panel UI (GET)
  /_gateway/scenarios      — available scenarios (GET)
  /_gateway/generate       — run query generator (POST)
  /_gateway/events         — clear usage events (DELETE)
  /_gateway/heat           — heat analysis endpoint (GET), ?index_group= filter
  /_gateway/groups         — index groups with concrete indices (GET)
  /_gateway/sample-events  — recent usage events (GET), ?index_group= filter
  /_gateway/config         — query body storage config (GET/PATCH)
  /_gateway/health         — gateway health check (GET)
  /{path:path}             — everything else proxied to Elasticsearch
"""

import asyncio
import json
import time
import logging
import uvicorn
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from config import GATEWAY_HOST, GATEWAY_PORT, ES_HOST, USAGE_INDEX
from gateway.proxy import proxy_request
from gateway.extractor import extract_from_request
from gateway.events import build_event, emit_event, emit_event_background, ensure_usage_index, get_query_body_config, set_query_body_config
from gateway.analyzer import compute_heat
from gateway.ui import HTML_PAGE
from gateway import metadata as metadata_mod
import random as _random
from generator.queries import SCENARIOS, DEFAULT_WEIGHTS, QUERY_FUNCTIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _resolve_concrete_indices(
    response_body: bytes,
    operation: str,
    path_indices: list[str] | None,
) -> list[str] | None:
    """Resolve concrete index names from ES response hits.

    For search operations with hits, extracts the actual _index from each hit
    so we know which concrete indices were touched. For aggregation-only
    queries (size=0, no hits), returns None — the caller uses the path index
    as-is and the index_group field handles grouping.
    """
    if operation == "search":
        try:
            data = json.loads(response_body)
            hits = data.get("hits", {}).get("hits", [])
            if hits:
                indices = list({h["_index"] for h in hits if "_index" in h})
                if indices:
                    return indices
        except Exception:
            pass

    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create usage-events index on startup and start metadata refresh."""
    logger.info("Gateway starting — ensuring usage index exists")
    await ensure_usage_index()
    metadata_mod.start_refresh_loop()
    logger.info("Gateway ready — proxying to Elasticsearch")
    yield
    logger.info("Gateway shutting down")


app = FastAPI(title="ES Usage Gateway", lifespan=lifespan)


# --- Gateway-specific endpoints (not proxied) ---

@app.get("/_gateway/health")
async def health():
    return {"status": "ok", "service": "es-usage-gateway"}


@app.get("/_gateway/config")
async def get_config():
    """Return current gateway runtime configuration."""
    return {"query_body": get_query_body_config()}


@app.patch("/_gateway/config")
async def update_config(request: Request):
    """Update gateway runtime configuration (e.g. query body sampling)."""
    body = await request.json()
    qb = body.get("query_body", {})
    result = set_query_body_config(
        enabled=qb.get("enabled"),
        sample_rate=qb.get("sample_rate"),
    )
    return {"query_body": result}


@app.get("/_gateway/heat")
async def heat(hours: float = 24.0, index_group: str | None = None):
    """Compute and return the heat report, optionally filtered by index group."""
    report = await compute_heat(time_window_hours=hours, index_group=index_group)
    return JSONResponse(content=report)


@app.get("/_gateway/groups")
async def groups():
    """Return known index groups with their concrete indices."""
    return metadata_mod.get_groups()


@app.get("/_gateway/sample-events")
async def sample_events(count: int = 20, index_group: str | None = None):
    """Return recent usage events for debugging, optionally filtered by group."""
    es_query: dict = {
        "size": min(count, 100),
        "sort": [{"timestamp": {"order": "desc"}}],
    }
    if index_group:
        es_query["query"] = {"term": {"index_group": index_group}}
    async with httpx.AsyncClient(base_url=ES_HOST, timeout=10.0) as client:
        resp = await client.post(
            f"/{USAGE_INDEX}/_search",
            json=es_query,
        )
        if resp.status_code == 200:
            hits = resp.json().get("hits", {}).get("hits", [])
            return {"count": len(hits), "events": [h["_source"] for h in hits]}
        return JSONResponse(
            content={"error": resp.text[:300]},
            status_code=resp.status_code,
        )


@app.get("/_gateway/ui")
async def ui():
    """Serve the control panel UI."""
    return HTMLResponse(content=HTML_PAGE)


@app.get("/_gateway/scenarios")
async def get_scenarios():
    """Return available scenarios with their weights and labels."""
    result = {}
    for key, scenario in SCENARIOS.items():
        result[key] = {
            "label": scenario["label"],
            "index": scenario["index"],
            "weights": scenario["weights"],
            "labels": scenario["labels"],
            "time_range_queries": sorted(scenario.get("time_range_queries", set())),
        }
    return result


class GenerateRequest(BaseModel):
    count: int = 200
    scenario: str | None = None
    weights: dict[str, int] | None = None
    lookback: str | None = None


@app.post("/_gateway/generate")
async def generate(req: GenerateRequest):
    """
    Run the query generator for a scenario with custom weights.

    Sends queries directly to ES (not through the gateway proxy) and
    emits usage events for each one — same observation pipeline as real traffic.
    """
    # Resolve scenario
    scenario_key = req.scenario or "products"
    if scenario_key not in SCENARIOS:
        return JSONResponse(
            content={"error": f"Unknown scenario: {scenario_key}", "available": list(SCENARIOS.keys())},
            status_code=400,
        )
    scenario = SCENARIOS[scenario_key]
    query_funcs = scenario["queries"]
    default_weights = scenario["weights"]

    # Build weighted selection pool
    funcs = []
    names = []
    w = []
    weights = req.weights or default_weights
    for name, weight in weights.items():
        if name in query_funcs and weight > 0:
            funcs.append(query_funcs[name])
            names.append(name)
            w.append(weight)
    if not funcs:
        funcs = list(query_funcs.values())
        names = list(query_funcs.keys())
        w = list(default_weights.values())

    # Pre-generate all queries (instant)
    tasks_list = []
    breakdown: dict[str, int] = {}
    for _ in range(req.count):
        idx = _random.choices(range(len(funcs)), weights=w, k=1)[0]
        query_name = names[idx]
        method, path, body = funcs[idx](lookback=req.lookback)
        breakdown[query_name] = breakdown.get(query_name, 0) + 1
        tasks_list.append((method, path, body))

    # Send concurrently with bounded parallelism
    sem = asyncio.Semaphore(20)
    results = []  # list of (status_code | None)

    async def _run_one(client, method, path, body):
        body_bytes = body.encode() if body else b""
        async with sem:
            try:
                resp = await client.request(
                    method=method, url=path, content=body_bytes,
                    headers={"Content-Type": "application/json"},
                )
            except httpx.RequestError:
                return None, method, path, body_bytes
            return resp, method, path, body_bytes

    async with httpx.AsyncClient(base_url=ES_HOST, timeout=30.0) as client:
        start = time.monotonic()
        raw = await asyncio.gather(*[
            _run_one(client, m, p, b) for m, p, b in tasks_list
        ])
        elapsed = round(time.monotonic() - start, 2)

    sent = 0
    ok = 0
    errors = 0
    for resp, method, path, body_bytes in raw:
        sent += 1
        if resp is None:
            errors += 1
            continue
        if 200 <= resp.status_code < 300:
            ok += 1
        else:
            errors += 1
        try:
            indices, operation, field_refs = extract_from_request(
                path=path, method=method, body=body_bytes,
            )
            concrete = _resolve_concrete_indices(resp.content, operation, indices)
            index_list = concrete or indices or [None]
            elapsed_ms = resp.elapsed.total_seconds() * 1000 if hasattr(resp, 'elapsed') else 0
            for idx_name in index_list:
                group = metadata_mod.resolve_group(idx_name) if idx_name else None
                event = build_event(
                    index_name=idx_name,
                    operation=operation,
                    field_refs=field_refs,
                    method=method,
                    path=path,
                    response_status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    client_id="control-panel",
                    body=body_bytes,
                    index_group=group,
                )
                emit_event_background(event)
        except Exception:
            logger.debug("Event emission failed for generated query", exc_info=True)

    return {"sent": sent, "ok": ok, "errors": errors, "elapsed_seconds": elapsed, "breakdown": breakdown, "lookback": req.lookback}


@app.delete("/_gateway/events")
async def clear_events():
    """Delete all documents in the usage-events index."""
    async with httpx.AsyncClient(base_url=ES_HOST, timeout=30.0) as client:
        resp = await client.post(
            f"/{USAGE_INDEX}/_delete_by_query",
            json={"query": {"match_all": {}}},
            params={"refresh": "true"},
        )
        if resp.status_code == 200:
            deleted = resp.json().get("deleted", 0)
            return {"deleted": deleted}
        return JSONResponse(
            content={"error": resp.text[:300]},
            status_code=resp.status_code,
        )


# --- Catch-all proxy route ---

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"])
async def proxy_catchall(request: Request):
    """
    Proxy all traffic to Elasticsearch.

    Flow:
    1. Forward request to ES and get response
    2. Extract metadata from request (index, operation, fields)
    3. Build usage event
    4. Emit event in background (fire-and-forget)
    5. Return ES response to client
    """
    # Step 1: proxy
    response, metadata = await proxy_request(request)

    if not metadata:
        # Proxy failed (502) — no metadata to extract
        return response

    # Step 2: extract (safe — never raises)
    try:
        indices, operation, field_refs = extract_from_request(
            path=metadata["path"],
            method=metadata["method"],
            body=metadata["body"],
        )
    except Exception:
        logger.exception("Extraction failed for %s — skipping event", metadata["path"])
        return response

    # Skip emitting events for our own usage index to avoid infinite recursion
    if indices and any(idx.startswith(".usage") for idx in indices):
        return response

    # Skip internal ES endpoints that aren't user queries
    if operation in ("cluster", "cat", "nodes", "tasks", "gateway"):
        return response

    # Skip browser artifact requests (favicon.ico, etc.)
    if indices and any(idx in ("favicon.ico",) for idx in indices):
        return response

    # Step 3+4: build and emit event(s) — one per index for multi-index queries
    # Prefer concrete indices from ES response (resolves alias → actual indices)
    client_id = request.headers.get("x-client-id")
    concrete = _resolve_concrete_indices(metadata.get("response_body", b""), operation, indices)
    index_list = concrete or indices or [None]
    for idx in index_list:
        group = metadata_mod.resolve_group(idx) if idx else None
        event = build_event(
            index_name=idx,
            operation=operation,
            field_refs=field_refs,
            method=metadata["method"],
            path=metadata["path"],
            response_status=metadata["response_status"],
            elapsed_ms=metadata["elapsed_ms"],
            client_id=client_id,
            body=metadata["body"],
            index_group=group,
        )
        emit_event_background(event)

    # Step 5: return response
    return response


if __name__ == "__main__":
    uvicorn.run(
        "gateway.main:app",
        host=GATEWAY_HOST,
        port=GATEWAY_PORT,
        log_level="info",
    )
