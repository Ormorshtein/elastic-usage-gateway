"""
Reverse proxy that forwards all traffic to Elasticsearch transparently.

Safety invariant: the proxy NEVER modifies, delays, or blocks a request
due to observation failures. If extraction or event emission fails,
the request still goes through.
"""

import time
import logging
import httpx
from fastapi import Request, Response

from config import ES_HOST, PROXY_TIMEOUT

logger = logging.getLogger(__name__)

# Long-lived async client — reused across all requests for connection pooling.
# Timeout is generous (default 120s) because bulk requests can be slow.
_client = httpx.AsyncClient(base_url=ES_HOST, timeout=PROXY_TIMEOUT)


async def proxy_request(request: Request) -> tuple[Response, dict]:
    """
    Forward a request to Elasticsearch and return the response.

    Returns:
        Tuple of (Response to send to client, metadata dict for event emission).
        Metadata includes: path, method, body_bytes, status_code, elapsed_ms.
    """
    path = request.url.path
    query_string = request.url.query
    if query_string:
        path = f"{path}?{query_string}"

    method = request.method
    headers = dict(request.headers)
    # Remove hop-by-hop and encoding headers — httpx manages its own
    # connection and we don't want compressed responses that we'd then
    # have to re-frame when forwarding.
    for h in ("host", "accept-encoding", "connection"):
        headers.pop(h, None)

    # Buffer the body once
    body = await request.body()

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

    # httpx automatically decodes the response body (un-chunks, decompresses
    # gzip/deflate). The original headers (content-length, content-encoding,
    # transfer-encoding) no longer match the decoded body we're forwarding.
    # Strip all framing/encoding headers and let FastAPI set correct ones.
    skip_headers = {
        "transfer-encoding", "connection", "keep-alive",
        "content-encoding", "content-length",
    }
    forwarded_headers = {
        k: v for k, v in es_response.headers.items()
        if k.lower() not in skip_headers
    }

    response = Response(
        content=es_response.content,
        status_code=es_response.status_code,
        headers=forwarded_headers,
    )

    return response, metadata
