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
                          │          Gateway (port 9201)         │
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
4. **Resolve**: Extract concrete index names from response hits (alias → concrete).
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

Builds usage event documents and writes them to the `.usage-events` index. Events are emitted as fire-and-forget background tasks via `asyncio.create_task()` — failures are logged but never affect request handling.

Features:
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
- Heat report visualization with tier-colored badges.
- Per-group drill-down with field-level breakdown.
- Traffic generator controls with per-query-type weight sliders.
- Lookback override for time-range queries.
- Query body sampling configuration.

### gateway/main.py — Application Entry Point

FastAPI application that wires everything together:
- Lifespan hook: creates usage index, starts metadata refresh, shuts down clients.
- Gateway endpoints (`/_gateway/*`): heat, groups, sample-events, config, generate, UI.
- Catch-all proxy route: observation pipeline for all other traffic.

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
| `GATEWAY_PORT` | `9201` | Gateway bind port |
| `USAGE_INDEX` | `.usage-events` | Index name for usage events |
| `CLUSTER_ID` | `default` | Cluster identifier in events |
| `PROXY_TIMEOUT` | `120` | Proxy request timeout (seconds) |
| `EVENT_TIMEOUT` | `10` | Event emission timeout (seconds) |
| `ANALYZER_TIMEOUT` | `30` | Heat analysis query timeout (seconds) |
| `METADATA_REFRESH_INTERVAL` | `60` | Metadata cache refresh (seconds) |
| `INDEX_HEAT_HOT` | `100` | Hot tier threshold (ops/hour) |
| `INDEX_HEAT_WARM` | `10` | Warm tier threshold (ops/hour) |
| `INDEX_HEAT_COLD` | `1` | Cold tier threshold (ops/hour) |
| `FIELD_HEAT_HOT` | `0.15` | Hot field threshold (proportion) |
| `FIELD_HEAT_WARM` | `0.05` | Warm field threshold (proportion) |
| `FIELD_HEAT_COLD` | `0.01` | Cold field threshold (proportion) |
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
    test_queries.py      # Query template and lookback tests
```

## Known Limitations

- **No authentication**: The gateway does not add auth headers. Designed for same-trust-zone deployment.
- **No SQL query parsing**: Only DSL queries are analyzed. ES SQL queries pass through unobserved.
- **Partial msearch support**: msearch query bodies are parsed, but per-query index targeting from headers is not tracked.
- **No streaming**: Large responses are fully buffered. Response body parsing is capped at 10MB.
- **Single-node**: No horizontal scaling. One gateway instance per ES cluster.
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
