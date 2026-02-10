# ES Usage Gateway — Architecture

## Project Purpose

The ES Usage Gateway is a transparent reverse proxy for Elasticsearch that observes query and indexing traffic to compute **index-level** and **field-level heat scores**. It answers:

- Which indices are hot, warm, cold, or frozen?
- Which fields are actively queried, filtered, aggregated, sorted, or written?
- Which fields are never used and can be safely de-indexed to save resources?
- How far back in time do queries typically look (lookback window)?

This enables data-driven decisions about ILM (Index Lifecycle Management), mapping optimization, and capacity planning.

## System Architecture

```
                          ┌─────────────────────────────────────┐
                          │          Gateway (port 9301)         │
                          │                                     │
  Client / Generator ───► │  FastAPI catch-all ──► Proxy ──────►│──► Elasticsearch (9200)
                          │       │                             │
                          │       ▼                             │
                          │  Extractor (parse DSL/bulk/msearch) │
                          │       │                             │
                          │       ▼                             │
                          │  Events (build + emit background)   │
                          │       │                             │
                          │       ▼                             │
                          │  .usage-events index ◄──────────────│
                          │       │                             │
                          │       ▼                             │
                          │  Analyzer (compute heat on demand)  │
                          │                                     │
                          │  Metadata (alias/data stream cache) │
                          │                                     │
                          │  UI (control panel at /_gateway/ui) │
                          └─────────────────────────────────────┘
```

## Data Flow

### Request Observation (proxy_catchall in main.py)

1. **Proxy**: Forward the request to ES, get response. Never block or modify.
2. **Extract**: Parse path (index, operation) and body (field references from DSL/bulk/msearch).
3. **Skip check**: Ignore internal operations (cluster, cat, nodes) and system indices (`.` prefix).
4. **Sample check**: Probabilistically skip event emission based on `EVENT_SAMPLE_RATE` (reduces ES load in production).
5. **Group**: Look up the logical group (alias or data stream) via metadata cache.
6. **Emit**: Build a usage event document and emit it as a fire-and-forget background task.
7. **Return**: Send the ES response to the client unchanged.

### Heat Analysis (GET /_gateway/heat)

1. Query `.usage-events` with time window filter and nested aggregations.
2. Group by `index_group`, then by concrete `index`, then by field category.
3. Compute `ops_per_hour` for index heat, `field_refs / total_refs` for field heat.
4. Classify into tiers (hot/warm/cold/frozen for indices, hot/warm/cold/unused for fields).
5. Generate actionable recommendations per index and field.

## Component Details

### gateway/proxy.py — Reverse Proxy

Forwards all HTTP traffic to Elasticsearch using a long-lived `httpx.AsyncClient` with connection pooling. Returns the raw response plus metadata (path, method, body, timing) for observation.

For request bodies below `PROXY_BODY_LIMIT` (default 1MB), the body is buffered in memory so the extractor can parse it. For larger bodies (e.g., large bulk imports), the request is streamed to ES without buffering — no field extraction, but no memory spike.

**Safety invariant**: The proxy NEVER modifies, delays, or blocks a request due to observation failures.

### gateway/extractor.py — DSL Field Extractor

Walks Elasticsearch Query DSL JSON to extract field references into categories:
- **queried**: Fields in `match`, `term`, `multi_match`, `exists`, etc.
- **filtered**: Fields inside `bool.filter` context (propagated through nested bools).
- **aggregated**: Fields in `aggs` (terms, avg, sum, date_histogram, etc.).
- **sorted**: Fields in `sort` clauses.
- **sourced**: Fields in `_source` includes/excludes.
- **written**: Fields in document bodies (index, create, update via bulk).

Also extracts **lookback** information from `range` queries with `now-*` syntax.

Handles three request formats:
- **Search/count**: Standard JSON query body
- **Bulk**: NDJSON alternating action/document lines (with update `doc`/`upsert` unwrapping)
- **Msearch**: NDJSON alternating header/query lines

The extractor never raises exceptions — it returns what it can find.

### gateway/events.py — Event Emission

Builds usage event documents and writes them to the `.usage-events` index via a **bulk writer** pipeline:

1. Request handler calls `emit_event_background(event)` which places the event into a bounded `asyncio.Queue`.
2. A background consumer task drains the queue and flushes events to ES via the `_bulk` API — either when the batch reaches `BULK_FLUSH_SIZE` (default 100) or every `BULK_FLUSH_INTERVAL` seconds (default 0.5s), whichever comes first.
3. On shutdown, the writer receives a stop sentinel, drains remaining events, and flushes before exiting.

**Backpressure**: If the queue reaches `BULK_QUEUE_SIZE` (default 5000), new events are dropped (not blocked) and counted as `events_dropped`. This prevents unbounded memory growth if ES is slow or unreachable.

**Shutdown**: Uses a sentinel-based stop signal (not `task.cancel()`) to guarantee the final flush completes without CancelledError interruption.

Features:
- **Bulk writes**: 10-50x more efficient than single-doc writes. One `_bulk` call per batch instead of one `POST /_doc` per event.
- **Event sampling**: Configurable `EVENT_SAMPLE_RATE` (0.0-1.0) to reduce event volume in production. Field heat (proportional) is unaffected; index heat (absolute ops/hour) scales proportionally but relative rankings are preserved. Runtime-adjustable via UI or config API.
- **Query fingerprinting**: SHA-256 of canonicalized JSON for deduplication.
- **Query body storage**: Optional, with configurable sampling rate (runtime-adjustable).
- **Dedicated httpx client**: Separate from the proxy client to avoid contention.

### gateway/analyzer.py — Heat Analysis

Reads usage events and computes heat reports:
- **Index heat**: `total_operations / time_window_hours` → hot/warm/cold/frozen tier.
- **Field heat**: `field_references / total_field_references` (proportion) → hot/warm/cold/unused tier.
- **Lookback stats**: avg, max, p50 lookback windows per index group.
- **Recommendations**: Actionable suggestions (freeze index, disable doc_values, set index: false).

Thresholds are configurable via environment variables.

### gateway/metadata.py — Index Metadata Cache

Periodically fetches alias and data stream mappings from ES and maintains an in-memory lookup. Maps concrete index names to their logical group (alias or data stream name).

- **Refresh loop**: Background task runs every `METADATA_REFRESH_INTERVAL` seconds.
- **Data stream priority**: If an index belongs to both an alias and a data stream, the data stream wins.
- **Atomic swap**: Lookup dicts are swapped via Python reference assignment (safe under asyncio's single-threaded model).

### gateway/ui.py — Control Panel

Single-page HTML/JS control panel served at `/_gateway/ui`. Provides:
- Traffic generator controls with per-query-type weight sliders.
- Lookback override for time-range queries.
- Event sampling rate slider.
- Query body sampling configuration.
- Live gateway stats (auto-refresh).

### gateway/metrics.py — In-Memory Counters

Simple module-level dict tracking request counts, event emission success/failure, extraction errors, and metadata refresh status. Counters reset on restart (acceptable for a dev tool). No external dependencies.

Exposed via `/_gateway/stats` and included in `/_gateway/health` responses.

### gateway/main.py — Application Entry Point

FastAPI application that wires everything together:
- Lifespan hook: creates usage index, starts metadata refresh, starts bulk writer, shuts down all httpx clients.
- Gateway endpoints (`/_gateway/*`): health, stats, heat, groups, sample-events, config, generate, UI.
- Catch-all proxy route: observation pipeline for all other traffic with metrics instrumentation.
- Shared httpx client (`_gw_client`) for gateway endpoints that query ES, avoiding per-request client creation.
- Supports `GATEWAY_WORKERS` for multi-process parallelism via Uvicorn's `--workers` flag.

### generator/ — Traffic Generator

- **queries.py**: Scenario-based query templates for products, logs, and orders indices. Each scenario defines weighted query functions with configurable lookback.
- **generate.py**: CLI tool that sends weighted queries through the gateway.
- **seed.py**: Seeds sample data into Elasticsearch for testing.

## Configuration

All settings are read from environment variables with defaults for local development.

| Variable | Default | Description |
|----------|---------|-------------|
| `ES_HOST` | `http://localhost:9200` | Elasticsearch URL |
| `GATEWAY_HOST` | `0.0.0.0` | Gateway bind address |
| `GATEWAY_PORT` | `9301` | Gateway bind port |
| `USAGE_INDEX` | `.usage-events` | Index name for usage events |
| `CLUSTER_ID` | `default` | Cluster identifier in events |
| `PROXY_TIMEOUT` | `120` | Proxy request timeout (seconds) |
| `PROXY_BODY_LIMIT` | `1048576` | Max body size (bytes) for buffered proxy; larger bodies are streamed without extraction |
| `EVENT_TIMEOUT` | `10` | Event emission timeout (seconds) |
| `ANALYZER_TIMEOUT` | `30` | Heat analysis query timeout (seconds) |
| `METADATA_REFRESH_INTERVAL` | `60` | Metadata cache refresh (seconds) |
| `GATEWAY_WORKERS` | `1` | Number of Uvicorn worker processes (set to CPU count for production) |
| `BULK_FLUSH_SIZE` | `100` | Max events per bulk write batch |
| `BULK_FLUSH_INTERVAL` | `0.5` | Max seconds between bulk flushes |
| `BULK_QUEUE_SIZE` | `5000` | Bounded event queue size (events dropped when full) |
| `INDEX_HEAT_HOT` | `100` | Hot tier threshold (ops/hour) |
| `INDEX_HEAT_WARM` | `10` | Warm tier threshold (ops/hour) |
| `INDEX_HEAT_COLD` | `1` | Cold tier threshold (ops/hour) |
| `FIELD_HEAT_HOT` | `0.15` | Hot field threshold (proportion) |
| `FIELD_HEAT_WARM` | `0.05` | Warm field threshold (proportion) |
| `FIELD_HEAT_COLD` | `0.01` | Cold field threshold (proportion) |
| `EVENT_SAMPLE_RATE` | `1.0` | Fraction of requests that emit events |
| `QUERY_BODY_ENABLED` | `true` | Store query bodies in events |
| `QUERY_BODY_SAMPLE_RATE` | `1.0` | Fraction of events to store bodies |

## Usage Event Schema

The `.usage-events` index stores one document per observed operation:

```json
{
  "timestamp": "2026-02-06T12:00:00Z",
  "cluster_id": "default",
  "index": "products-2026.02.06",
  "index_group": "products",
  "operation": "search",
  "http_method": "POST",
  "path": "/products/_search",
  "fields": {
    "queried": ["title"],
    "filtered": ["category", "price"],
    "aggregated": ["brand"],
    "sorted": ["price"],
    "sourced": ["title", "price", "category"],
    "written": []
  },
  "language": "dsl",
  "query_fingerprint": "a1b2c3...",
  "response_time_ms": 42.5,
  "response_status": 200,
  "client_id": "my-app",
  "lookback_seconds": 86400,
  "lookback_field": "timestamp",
  "lookback_label": "24h",
  "query_body": "{\"query\": ...}"
}
```

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| ES is down | Proxy returns 502. No events emitted. |
| Event emission fails | Logged as warning. Request already returned to client. |
| Metadata refresh fails | Logged. Stale cache used until next successful refresh. |
| Extractor can't parse body | Returns empty FieldRefs. Event still emitted with no field data. |
| Heat query fails | Returns error JSON with status code. |
| Usage index doesn't exist | Created automatically on gateway startup. |

## Safety Invariants

1. **Never block the request**: Observation (extraction, event emission) happens after the response is ready or in the background. Failures in observation never delay or prevent the proxied request.
2. **Never modify the request**: The proxy forwards requests byte-for-byte. No query rewriting, injection, or filtering.
3. **Graceful degradation**: If any observation component fails, the gateway continues to function as a transparent proxy.

## Project Structure

```
elastic_recommand/
  config.py              # Centralized env-var configuration
  docker-compose.yml     # ES + Kibana containers
  requirements.txt       # Python dependencies
  gateway/
    __init__.py
    main.py              # FastAPI app, routes, observation pipeline
    proxy.py             # Reverse proxy (httpx)
    extractor.py         # DSL field extraction
    events.py            # Usage event model and emission
    analyzer.py          # Heat computation
    metadata.py          # Index metadata cache (alias/data stream)
    metrics.py           # In-memory counters for monitoring
    ui.py                # Control panel HTML/JS
  generator/
    __init__.py
    generate.py          # CLI traffic generator
    queries.py           # Scenario-based query templates
    seed.py              # Sample data seeder
  tests/
    test_analyzer.py     # Heat tier and recommendation tests
    test_events.py       # Fingerprinting and event building tests
    test_extractor.py    # Path parsing, DSL extraction, bulk, msearch
    test_metadata.py     # Group resolution tests
    test_metrics.py      # In-memory counter tests
    test_queries.py      # Query template and lookback tests
```

## Production Scaling Decision Record

### Context

The gateway is designed for deployment on OpenShift, sitting on the hot path of all Elasticsearch traffic. The question: can the current Python/FastAPI stack handle production scale, or do we need to move to Nginx/Kong/Go?

### Current Tech Stack

| Layer | Technology | Role |
|---|---|---|
| HTTP server | Uvicorn (ASGI) | Async event loop, connection handling |
| Framework | FastAPI | Routing, middleware, request/response handling |
| Proxy client | httpx (AsyncClient) | Connection-pooled forwarding to ES |
| Event emission | httpx (separate client) | Writing usage events to `.usage-events` |
| Concurrency | asyncio (single-threaded) | Cooperative multitasking for I/O-bound work |
| State | Module-level globals | Metrics, metadata cache, sampling config (in-memory, per-process) |

### Option A: Kong/Nginx in front of Python gateway — REJECTED

**Arguments for:**
- Kong (OpenResty/Nginx) handles connection management at C speed — 100k+ concurrent connections without breaking a sweat. Python's asyncio tops out at ~10k before event loop scheduling overhead becomes measurable.
- Kong provides rate limiting, circuit breaking, auth, and TLS termination out of the box.
- If ES slows down and responses back up, Kong can shed load before it reaches Python.
- Kong can health-check multiple gateway pods and route around failures.

**Arguments against:**
- OpenShift already provides this. An OpenShift Route (HAProxy-based) handles TLS termination, load balancing across pods, and connection limits. Adding Kong is a second infrastructure layer to deploy, configure, monitor, and debug.
- Kong adds 1-3ms latency per request (extra network hop). If ES queries take 10-100ms, that's 1-10% overhead for infrastructure that duplicates OpenShift capabilities.
- Rate limiting ES traffic is unusual — ES itself has circuit breakers and thread pool queuing. Rate limiting the gateway protects the gateway, not ES.
- Kong requires its own backing store (Postgres or Cassandra for clustering), its own pods, its own monitoring — significant operational overhead for a single-service deployment.

**Decision:** Don't introduce Kong solely for this service. OpenShift Route + HPA covers TLS, load balancing, and connection limits with zero additional infrastructure. If the organization already has Kong deployed cluster-wide, place the gateway behind it opportunistically.

### Option B: Rewrite proxy layer in Go/Rust — REJECTED

**Arguments for:**
- Go is the standard language for proxies (Traefik, Caddy, CoreDNS). Goroutines are ~2KB stack vs ~8KB per Python coroutine, native concurrency without GIL, compiled speed for JSON parsing.
- CPU-bound work on the hot path (JSON parsing, SHA-256 fingerprinting) blocks the Python event loop. In Go this would be trivially parallel across goroutines.
- Python `json.loads` is ~3-5x slower than Go `encoding/json`. For a proxy that parses every body, this is measurable.
- On OpenShift with resource quotas, Go pods use 2-4x less CPU and memory for equivalent throughput. Fewer pods = lower cost.

**Arguments against:**
- The bottleneck is I/O, not CPU. The dominant cost per request is waiting for ES to respond (10-100ms) and writing the event to ES (5-20ms). JSON parse + DSL walk + SHA-256 is ~0.2-1ms for a typical search body — less than 1% of wall clock time.
- This is an observation tool, not a load balancer. A typical ES cluster serves 1k-5k rps. Python with horizontal scaling handles this comfortably.
- The DSL extractor is ~500 lines of recursive tree walking, lookback parsing, bulk/msearch NDJSON handling. This is exactly where Python excels — the same logic in Go would be 3x the code with 3x the bug surface area.
- A rewrite costs weeks-months and introduces new bugs in working, tested code. Adding 2 more OpenShift pods achieves the same throughput gain for zero engineering cost.
- FastAPI/Uvicorn benchmarks (TechEmpower) show ~15k-30k req/s for JSON workloads per process. With 4 workers per pod and 3 pods = 12 processes, theoretical capacity is 180k-360k simple req/s. Even at 1/10th (proxy + parsing overhead), that's 18k-36k rps across the deployment.

**Decision:** Stay with Python. The observation/extraction logic is the product's core value and Python is the right language for it. The proxy overhead is noise compared to ES response times. If the gateway ever needs to handle >50k rps, the escape hatch is a Go rewrite of the proxy layer with Python as an analysis sidecar — but that's a bridge to cross when measured, not speculated.

### Option C: Harden the existing Python stack — ACCEPTED

The current codebase has specific scaling bottlenecks that are implementation-level, not architectural. Fixing these keeps the tech stack while achieving production-grade throughput.

| Problem | Location | Impact | Fix |
|---|---|---|---|
| Single-doc event writes | `events.py` `emit_event()` | 1 ES index call per request. At 1k rps = 1k writes/sec | Buffer events in-memory, flush via `_bulk` every 500ms or 100 events |
| Unbounded background tasks | `events.py` `emit_event_background()` | No backpressure. Slow ES → unbounded task accumulation → OOM | `asyncio.Queue` with bounded size + fixed consumer pool |
| Single Uvicorn process | `main.py` `uvicorn.run()` | Zero CPU parallelism, single event loop | Multiple workers via `--workers` flag |
| Full body buffering | `proxy.py` `await request.body()` | Large bulk requests spike memory | httpx streaming for request/response bodies above a size threshold |
| New httpx client per health check | `main.py` `health()` | Connection churn under monitoring | Reuse existing shared client |
| New httpx client per sample-events call | `main.py` `sample_events()` | Same connection churn | Reuse existing shared client |

### Target Deployment Architecture (OpenShift)

```
            OpenShift Route (TLS termination + load balancing)
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         Pod (4 workers) Pod (4 workers) Pod (4 workers)
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                    Elasticsearch cluster
```

- **Deployment**: 3 replicas, 4 Uvicorn workers each = 12 processes
- **OpenShift Route**: TLS, load balancing, connection limits (no Kong needed)
- **HPA**: Horizontal Pod Autoscaler scales pods on CPU utilization (target 70%)
- **Resource requests**: ~500m CPU / 512Mi memory per pod
- **Estimated throughput**: ~5k-10k rps with room to scale horizontally via HPA
- **State model**: All state (metrics, metadata cache) is per-process and ephemeral. No shared state across pods, which is acceptable for a monitoring/observation tool.

## Known Limitations

- **No authentication**: The gateway does not add auth headers. Designed for same-trust-zone deployment.
- **No SQL query parsing**: Only DSL queries are analyzed. ES SQL queries pass through unobserved.
- **Partial msearch support**: msearch query bodies are parsed, but per-query index targeting from headers is not tracked.
- **No streaming**: Large request/response bodies above 1MB are streamed, but observation (field extraction) only applies to buffered bodies below the threshold.
- **Horizontal scaling**: Supports multiple Uvicorn workers per pod and multiple pods via OpenShift HPA. State is per-process (no shared state required).
- **No persistent queue**: Events are fire-and-forget. If the gateway crashes mid-flight, in-flight events are lost.

## Future Phases

- **Dockerfile + production deployment**: Containerize the gateway alongside ES.
- **Structured logging**: JSON-formatted logs for log aggregation systems.
- **Metrics endpoint**: Prometheus/OpenTelemetry metrics for gateway health monitoring.
- **Retention policy**: Auto-delete old usage events (ILM on `.usage-events`).
- **Field mapping integration**: Cross-reference heat data with actual index mappings to generate specific optimization commands.
- **Multi-cluster support**: Proxy and observe traffic across multiple ES clusters.
- **Authentication passthrough**: Forward auth headers and track per-user usage patterns.
- **Alerting**: Notify when indices transition between tiers or when unused fields are detected.
