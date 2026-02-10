"""
Reverse proxy that forwards all traffic to Elasticsearch transparently.

For request bodies below PROXY_BODY_LIMIT (default 1MB), the body is
buffered in memory so the extractor can parse it. For larger bodies
(e.g., big bulk imports), the request is streamed to ES and the response
streamed back — no field extraction, but no memory spike.

Safety invariant: the proxy NEVER modifies, delays, or blocks a request
due to observation failures. If extraction or event emission fails,
the request still goes through.
"""

import time
import logging
import httpx
from fastapi import Request, Response
from starlette.responses import StreamingResponse

from config import ES_HOST, PROXY_TIMEOUT, PROXY_BODY_LIMIT

logger = logging.getLogger(__name__)

# Long-lived async client — reused across all requests for connection pooling.
# Timeout is generous (default 120s) because bulk requests can be slow.
_client = httpx.AsyncClient(base_url=ES_HOST, timeout=PROXY_TIMEOUT)


def _build_forwarded_path(request: Request) -> str:
    """Build the full path with query string for forwarding."""
    path = request.url.path
    query_string = request.url.query
    if query_string:
        path = f"{path}?{query_string}"
    return path


def _clean_request_headers(request: Request) -> dict:
    """Copy and clean request headers for forwarding."""
    headers = dict(request.headers)
    for h in ("host", "accept-encoding", "connection"):
        headers.pop(h, None)
    return headers


_SKIP_RESPONSE_HEADERS = {
    "transfer-encoding", "connection", "keep-alive",
    "content-encoding", "content-length",
}


def _filter_response_headers(es_response) -> dict:
    """Strip framing/encoding headers from ES response for forwarding."""
    return {
        k: v for k, v in es_response.headers.items()
        if k.lower() not in _SKIP_RESPONSE_HEADERS
    }


async def proxy_request(request: Request) -> tuple[Response, dict]:
    """
    Forward a request to Elasticsearch and return the response.

    Bodies below PROXY_BODY_LIMIT are buffered (enabling field extraction).
    Bodies above the limit are streamed (no extraction, but no memory spike).

    Returns:
        Tuple of (Response to send to client, metadata dict for event emission).
        Metadata includes: path, method, body, response_status, elapsed_ms.
        For streamed requests, metadata is empty (no observation possible).
    """
    path = _build_forwarded_path(request)
    method = request.method
    headers = _clean_request_headers(request)

    # Check content-length to decide buffered vs streamed
    content_length = int(request.headers.get("content-length", "0") or "0")

    if content_length > PROXY_BODY_LIMIT:
        return await _proxy_streamed(request, path, method, headers)

    # --- Buffered path (normal — enables field extraction) ---
    body = await request.body()

    # Double-check actual body size (content-length may be missing or wrong)
    if len(body) > PROXY_BODY_LIMIT:
        return await _proxy_with_body_streamed(body, path, method, headers)

    start = time.monotonic()

    try:
        es_response = await _client.request(
            method=method,
            url=path,
            content=body,
            headers=headers,
        )
    except httpx.RequestError as exc:
        logger.error("Failed to reach Elasticsearch: %s", exc)
        return Response(
            content=f"Gateway error: could not reach Elasticsearch: {exc}",
            status_code=502,
        ), {}

    elapsed_ms = (time.monotonic() - start) * 1000

    metadata = {
        "path": request.url.path,
        "method": method,
        "body": body,
        "response_body": es_response.content,
        "response_status": es_response.status_code,
        "elapsed_ms": round(elapsed_ms, 2),
    }

    response = Response(
        content=es_response.content,
        status_code=es_response.status_code,
        headers=_filter_response_headers(es_response),
    )

    return response, metadata


async def _proxy_streamed(
    request: Request, path: str, method: str, headers: dict
) -> tuple[StreamingResponse, dict]:
    """Stream a large request body to ES without buffering."""
    start = time.monotonic()

    try:
        es_response = await _client.request(
            method=method,
            url=path,
            content=request.stream(),
            headers=headers,
        )
    except httpx.RequestError as exc:
        logger.error("Failed to reach Elasticsearch (streamed): %s", exc)
        return Response(
            content=f"Gateway error: could not reach Elasticsearch: {exc}",
            status_code=502,
        ), {}

    elapsed_ms = (time.monotonic() - start) * 1000
    logger.debug("Streamed request %s %s (%.1fms, no extraction)", method, path, elapsed_ms)

    response = Response(
        content=es_response.content,
        status_code=es_response.status_code,
        headers=_filter_response_headers(es_response),
    )

    # Return empty metadata — no extraction for streamed requests
    return response, {}


async def _proxy_with_body_streamed(
    body: bytes, path: str, method: str, headers: dict
) -> tuple[Response, dict]:
    """Forward an already-buffered large body without extraction metadata."""
    start = time.monotonic()

    try:
        es_response = await _client.request(
            method=method,
            url=path,
            content=body,
            headers=headers,
        )
    except httpx.RequestError as exc:
        logger.error("Failed to reach Elasticsearch (large body): %s", exc)
        return Response(
            content=f"Gateway error: could not reach Elasticsearch: {exc}",
            status_code=502,
        ), {}

    elapsed_ms = (time.monotonic() - start) * 1000
    logger.debug("Large body %s %s (%d bytes, %.1fms, no extraction)", method, path, len(body), elapsed_ms)

    response = Response(
        content=es_response.content,
        status_code=es_response.status_code,
        headers=_filter_response_headers(es_response),
    )

    return response, {}


async def close_proxy_client() -> None:
    """Close the proxy client. Called during gateway shutdown."""
    await _client.aclose()
