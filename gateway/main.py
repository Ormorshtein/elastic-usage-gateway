"""
FastAPI application — the ES Usage Gateway entry point.

Routes:
  /_gateway/ui             — control panel UI (GET)
  /_gateway/generate       — run query generator (POST)
  /_gateway/events         — clear usage events (DELETE)
  /_gateway/heat           — heat analysis endpoint (GET)
  /_gateway/health         — gateway health check (GET)
  /{path:path}             — everything else proxied to Elasticsearch
"""

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
from gateway.events import build_event, emit_event, emit_event_background, ensure_usage_index
from gateway.analyzer import compute_heat
from gateway.ui import HTML_PAGE
import random as _random
from generator.queries import DEFAULT_WEIGHTS, QUERY_FUNCTIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create usage-events index on startup."""
    logger.info("Gateway starting — ensuring usage index exists")
    await ensure_usage_index()
    logger.info("Gateway ready — proxying to Elasticsearch")
    yield
    logger.info("Gateway shutting down")


app = FastAPI(title="ES Usage Gateway", lifespan=lifespan)


# --- Gateway-specific endpoints (not proxied) ---

@app.get("/_gateway/health")
async def health():
    return {"status": "ok", "service": "es-usage-gateway"}


@app.get("/_gateway/heat")
async def heat(hours: float = 24.0):
    """Compute and return the heat report."""
    report = await compute_heat(time_window_hours=hours)
    return JSONResponse(content=report)


@app.get("/_gateway/sample-events")
async def sample_events(count: int = 20):
    """Return recent usage events for debugging."""
    async with httpx.AsyncClient(base_url=ES_HOST, timeout=10.0) as client:
        resp = await client.post(
            f"/{USAGE_INDEX}/_search",
            json={
                "size": min(count, 100),
                "sort": [{"timestamp": {"order": "desc"}}],
            },
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


class GenerateRequest(BaseModel):
    count: int = 200
    weights: dict[str, int] = DEFAULT_WEIGHTS


@app.post("/_gateway/generate")
async def generate(req: GenerateRequest):
    """
    Run the query generator with custom weights.

    Sends queries directly to ES (not through the gateway proxy) and
    emits usage events for each one — same observation pipeline as real traffic.
    """
    # Build weighted selection pool so we can track which type was picked
    funcs = []
    names = []
    w = []
    for name, weight in req.weights.items():
        if name in QUERY_FUNCTIONS and weight > 0:
            funcs.append(QUERY_FUNCTIONS[name])
            names.append(name)
            w.append(weight)
    if not funcs:
        funcs = list(QUERY_FUNCTIONS.values())
        names = list(QUERY_FUNCTIONS.keys())
        w = list(DEFAULT_WEIGHTS.values())

    async with httpx.AsyncClient(base_url=ES_HOST, timeout=30.0) as client:
        sent = 0
        ok = 0
        errors = 0
        breakdown: dict[str, int] = {}
        start = time.monotonic()

        for _ in range(req.count):
            idx = _random.choices(range(len(funcs)), weights=w, k=1)[0]
            query_name = names[idx]
            method, path, body = funcs[idx]()
            breakdown[query_name] = breakdown.get(query_name, 0) + 1
            body_bytes = body.encode() if body else b""

            try:
                resp = await client.request(
                    method=method,
                    url=path,
                    content=body_bytes,
                    headers={"Content-Type": "application/json"},
                )
                sent += 1
                if 200 <= resp.status_code < 300:
                    ok += 1
                else:
                    errors += 1

                # Extract fields and emit usage event — same as proxy flow
                try:
                    indices, operation, field_refs = extract_from_request(
                        path=path, method=method, body=body_bytes,
                    )
                    index_list = indices if indices else [None]
                    elapsed_ms = resp.elapsed.total_seconds() * 1000 if hasattr(resp, 'elapsed') else 0
                    for idx in index_list:
                        event = build_event(
                            index_name=idx,
                            operation=operation,
                            field_refs=field_refs,
                            method=method,
                            path=path,
                            response_status=resp.status_code,
                            elapsed_ms=elapsed_ms,
                            client_id="control-panel",
                            body=body_bytes,
                        )
                        await emit_event(event)
                except Exception:
                    logger.debug("Event emission failed for generated query", exc_info=True)

            except httpx.RequestError:
                sent += 1
                errors += 1

        elapsed = round(time.monotonic() - start, 2)

    return {"sent": sent, "ok": ok, "errors": errors, "elapsed_seconds": elapsed, "breakdown": breakdown}


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

    # Step 3+4: build and emit event(s) — one per index for multi-index queries
    client_id = request.headers.get("x-client-id")
    index_list = indices if indices else [None]
    for idx in index_list:
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
