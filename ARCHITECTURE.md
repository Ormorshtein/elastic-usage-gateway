# ES Usage Gateway — Architecture

## Project Purpose

The ES Usage Gateway is a transparent reverse proxy for Elasticsearch that observes query and indexing traffic to measure **field-level usage** and **query pattern cost**. It answers:

- Which fields are actively queried, filtered, aggregated, sorted, or written?
- Which fields are never used and can be safely de-indexed to save resources?
- Which query shapes consume the most cluster time?
- How far back in time do queries typically look (lookback window)?

This enables data-driven decisions about ILM (Index Lifecycle Management), mapping optimization, capacity planning, and query performance tuning. Insights are delivered through Kibana dashboards with built-in guidance text explaining how to act on each section.

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

### Heat & Recommendations (Kibana Dashboards)

Field-level heat and recommendations are delivered directly in Kibana dashboard panels via guidance text in Markdown headers (see `kibana_setup.py`). Each dashboard section includes actionable advice:
- **Overview**: Traffic tiers, index group prioritization
- **Field Heat by Count**: Unused fields, source-only fields, aggregation optimization
- **Field Heat by Response Time**: Slow-query fields, optimization priority
- **Query Patterns**: Costly templates, query inefficiency signals
- **Lookback Analysis**: ILM tiering based on actual query windows

### Field Drill-Down (Kibana Dashboard)

A dedicated dashboard for investigating a specific field's usage. Users come here from the Mapping Diff dashboard after spotting an unused or sourced-only field and want to verify whether it's safe to remove.

- **Controls**: Index Group + field category dropdowns (Queried, Filtered, Aggregated). Pick a field in one dropdown; all panels filter to matching events.
- **Panels**: Usage over time, operations breakdown, clients table, client IPs, user-agents, query templates, response time trend, raw events.
- **Cross-category search**: For fields that appear in sorted/sourced/written categories, use the KQL bar with `fields.queried: "field" OR fields.filtered: "field" OR ...`.

## Component Details

### gateway/proxy.py — Reverse Proxy

Forwards all HTTP traffic to Elasticsearch using a long-lived `httpx.AsyncClient` with connection pooling. Returns the raw response plus metadata (path, method, body, timing) for observation.

For request bodies below `PROXY_BODY_LIMIT` (default 1MB), the body is buffered in memory so the extractor can parse it. For larger bodies (e.g., large bulk imports), the request is streamed to ES without buffering — no field extraction, but no memory spike.

**Safety invariant**: The proxy NEVER modifies, delays, or blocks a request due to observation failures.

### gateway/extractor.py — DSL Field Extractor

Walks Elasticsearch Query DSL JSON to extract field references into categories:
- **queried**: Fields in `match`, `term`, `multi_match`, `exists`, `highlight`, suggesters, `function_score` (script_score, field_value_factor, decay functions), etc.
- **filtered**: Fields inside `bool.filter` context, `post_filter`, `collapse`, filter agg queries.
- **aggregated**: Fields in `aggs` (terms, avg, sum, date_histogram, composite sources, `scripted_metric`, `bucket_script`, `bucket_selector`, etc.).
- **sorted**: Fields in `sort` clauses, including scripted sorts (`_script`).
- **sourced**: Fields in `_source`, `docvalue_fields`, `stored_fields`, `script_fields`, `runtime_mappings`.
- **written**: Fields in document bodies (index, create, update via bulk).

**Painless script extraction**: Fields referenced inside Painless scripts (`doc['field']`, `doc["field"]`, `ctx._source.field`) are extracted via regex patterns. The extractor checks the script's `lang` field (defaults to `"painless"`) and skips Mustache templates and stored scripts. Coverage is ~90% — fields stored in variables or dynamically constructed are not tracked.

Also extracts **lookback** information from `range` queries with `now-*` syntax.

Handles five request formats:
- **Search/count/async_search**: Standard JSON query body
- **Update_by_query/delete_by_query**: Standard DSL query body
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
- **Query template hashing**: Structural template (leaf values replaced with `"?"`) and SHA-256 hash for pattern grouping. Two queries with same structure but different values get the same template hash.
- **Query body storage**: Optional, with configurable sampling rate (runtime-adjustable).
- **Dedicated httpx client**: Separate from the proxy client to avoid contention.

### gateway/metadata.py — Index Metadata Cache

Periodically fetches alias and data stream mappings from ES and maintains an in-memory lookup. Maps concrete index names to their logical group (alias or data stream name).

- **Refresh loop**: Background task runs every `METADATA_REFRESH_INTERVAL` seconds.
- **Data stream priority**: If an index belongs to both an alias and a data stream, the data stream wins.
- **Atomic swap**: Lookup dicts are swapped via Python reference assignment (safe under asyncio's single-threaded model).

### gateway/mapping_diff.py — Mapping Diff Engine

Compares index mappings against actual field usage observed through the gateway. Runs as a background loop (like metadata refresh) and writes results to the `.mapping-diff` index for Kibana visualization.

- **Mapping flattener**: Walks nested ES mapping properties via `GET /{index}/_mapping`, handles multi-fields (`title.keyword`), nested objects (`metadata.created_at`), and extracts `index`/`doc_values` metadata per field.
- **Usage aggregation**: Queries `.usage-events` with terms aggregations on all 6 field categories (`fields.queried`, `fields.filtered`, etc.) with `max(timestamp)` sub-aggs for last_seen tracking.
- **Classification**: Each field is classified as `active` (queried/filtered/aggregated/sorted), `sourced_only` (only fetched in _source), `write_only` (only written, never read), or `unused` (zero references).
- **Write strategy**: Delete-and-rewrite per index group. The `.mapping-diff` index stays small (one doc per field per group) so full rewrite is fast and avoids stale data.
- **Refresh loop**: Runs every `MAPPING_DIFF_REFRESH_INTERVAL` seconds (default 300). Processes all known index groups from the metadata cache.

### gateway/recommender.py — Mapping Recommendations Engine

Reads field classification and mapping metadata from `.mapping-diff`, applies 8 decision rules, and writes actionable recommendations to `.mapping-recommendations`. Each recommendation includes a `why` (explanation + tradeoffs) and `how` (concrete mapping changes with JSON snippets).

- **8 rules**: disable_index (write-only/sourced-only fields), disable_doc_values (queried but never aggregated/sorted), disable_norms (text field only filtered, never scored), change_to_keyword (text field only used with exact-match), add_keyword_subfield (text field needing both full-text and exact-match), remove_multifield (unused sub-field), remove_field (completely unused).
- **Sibling awareness**: Rules 6 and 7 check the full set of fields in an index group (e.g., whether `title.keyword` already exists).
- **Stacking**: Active fields can receive multiple recommendations (e.g., disable_norms + change_to_keyword).
- **Write strategy**: Same delete-and-rewrite pattern as mapping_diff.
- **Refresh loop**: Runs every `RECOMMENDATIONS_REFRESH_INTERVAL` seconds (default 300).

### gateway/index_arch.py — Index Architecture Recommendations Engine

Evaluates index-level structural design (shard sizing, settings, usage patterns) and writes recommendations to `.index-recommendations`. Follows the same three-tier architecture as `recommender.py`: pure functions (rule evaluation) → async I/O (ES data collection) → background lifecycle (refresh loop).

- **10 rules in 3 categories**:
  - **shard_sizing** (2 rules): `shard_too_small` (< 1GB with multiple shards), `shard_too_large` (> 50GB, critical at > 100GB).
  - **settings_audit** (5 rules): `replica_risk` (0 replicas), `replica_waste` (replicas on cold/frozen), `codec_opportunity` (default LZ4 on read-only data), `field_count_near_limit` (approaching `total_fields.limit`), `source_disabled` (`_source.enabled: false`).
  - **usage_based** (3 rules): `rollover_lookback_mismatch` (p95 lookback > 2x rollover period), `index_sorting_opportunity` (dominant sort field, index unsorted), `refresh_interval_opportunity` (write-heavy with default 1s refresh).
- **Data collection**: 3 global API calls (`_cat/indices`, `_cat/shards`, `_settings`) partitioned in Python by index group via the metadata cache. Per-group: mapping field count check + `.usage-events` aggregation.
- **Explainable output**: Every recommendation includes `current_value` (observed data), `why` (problem + best practice), `how` (concrete API calls), `reference_url` (Elastic docs link).
- **Write strategy**: Same delete-and-rewrite per index group pattern as recommender.py.
- **Refresh loop**: Runs every `INDEX_ARCH_REFRESH_INTERVAL` seconds (default 600).

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
- Lifespan hook: creates usage index, starts metadata refresh, starts bulk writer, starts mapping diff loop, starts recommendations loop, starts index architecture loop, shuts down all httpx clients.
- Gateway endpoints (`/_gateway/*`): health, stats, groups, sample-events, config, generate, UI.
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
| `METADATA_REFRESH_INTERVAL` | `60` | Metadata cache refresh (seconds) |
| `MAPPING_DIFF_REFRESH_INTERVAL` | `300` | Mapping diff refresh interval (seconds) |
| `MAPPING_DIFF_LOOKBACK_HOURS` | `168` | How far back to query usage events for mapping diff (hours, default 7 days) |
| `RECOMMENDATIONS_REFRESH_INTERVAL` | `300` | Recommendations refresh interval (seconds) |
| `INDEX_ARCH_REFRESH_INTERVAL` | `600` | Index architecture recs refresh interval (seconds) |
| `INDEX_ARCH_LOOKBACK_HOURS` | `168` | How far back to query usage events for index arch rules (hours, default 7 days) |
| `GATEWAY_WORKERS` | `1` | Number of Uvicorn worker processes (set to CPU count for production) |
| `BULK_FLUSH_SIZE` | `100` | Max events per bulk write batch |
| `BULK_FLUSH_INTERVAL` | `0.5` | Max seconds between bulk flushes |
| `BULK_QUEUE_SIZE` | `5000` | Bounded event queue size (events dropped when full) |
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
  "language": "dsl",            // "dsl", "dsl+painless", or "sql" (future)
  "query_fingerprint": "a1b2c3...",
  "query_template_hash": "d4e5f6...",
  "query_template_text": "{\"query\": {\"bool\": {\"filter\": [{\"range\": {\"timestamp\": {\"gte\": \"?\"}}}], \"must\": [{\"match\": {\"title\": \"?\"}}]}}}",
  "response_time_ms": 42.5,
  "response_status": 200,
  "client_id": "my-app",
  "lookback_seconds": 86400,
  "lookback_field": "timestamp",
  "lookback_label": "24h",
  "query_body": "{\"query\": ...}"
}
```

## Mapping Diff Schema

The `.mapping-diff` index stores one document per field per index group (latest snapshot only, refreshed every `MAPPING_DIFF_REFRESH_INTERVAL` seconds):

```json
{
  "timestamp": "2026-02-13T12:00:00Z",
  "index_group": "products",
  "field_name": "legacy_supplier_code",
  "mapped_type": "keyword",
  "is_indexed": true,
  "has_doc_values": true,
  "total_references": 0,
  "last_seen": null,
  "last_seen_queried": null,
  "last_seen_filtered": null,
  "last_seen_aggregated": null,
  "last_seen_sorted": null,
  "last_seen_sourced": null,
  "last_seen_written": null,
  "count_queried": 0,
  "count_filtered": 0,
  "count_aggregated": 0,
  "count_sorted": 0,
  "count_sourced": 0,
  "count_written": 0,
  "classification": "unused"
}
```

## Mapping Recommendations Schema

The `.mapping-recommendations` index stores one document per recommendation per field (refreshed every `RECOMMENDATIONS_REFRESH_INTERVAL` seconds):

```json
{
  "timestamp": "2026-02-14T12:00:00Z",
  "index_group": "products",
  "field_name": "description",
  "mapped_type": "text",
  "classification": "write_only",
  "recommendation": "disable_index",
  "why": "This field is stored in the index but never queried...",
  "how": "Update the index template mapping for this field:\n  \"description\": { \"type\": \"text\", \"index\": false, \"doc_values\": false }",
  "breaking_change": false
}
```

## Index Architecture Recommendations Schema

The `.index-recommendations` index stores one document per recommendation per index group (refreshed every `INDEX_ARCH_REFRESH_INTERVAL` seconds):

```json
{
  "timestamp": "2026-02-16T12:00:00Z",
  "index_group": "logs",
  "category": "shard_sizing",
  "recommendation": "shard_too_small",
  "severity": "warning",
  "current_value": "Avg primary shard size: 128.5 MB across 5 shards",
  "why": "Primary shards average 128.5 MB — well below the recommended 10-50 GB sweet spot. Each shard consumes heap, file descriptors, and cluster state regardless of size...",
  "how": "Reduce the number of primary shards in the index template:\nPUT /_index_template/logs\n{\"index_patterns\": [\"logs-*\"], \"template\": {\"settings\": {\"number_of_shards\": 1}}}",
  "reference_url": "https://www.elastic.co/guide/en/elasticsearch/reference/current/size-your-shards.html",
  "breaking_change": false
}
```

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| ES is down | Proxy returns 502. No events emitted. |
| Event emission fails | Logged as warning. Request already returned to client. |
| Metadata refresh fails | Logged. Stale cache used until next successful refresh. |
| Extractor can't parse body | Returns empty FieldRefs. Event still emitted with no field data. |
| Usage index doesn't exist | Created automatically on gateway startup. |
| Mapping diff refresh fails | Logged. Stale `.mapping-diff` data remains until next successful refresh. |
| Recommendations refresh fails | Logged. Stale `.mapping-recommendations` data remains. Depends on `.mapping-diff` being populated first. |
| Index arch refresh fails | Logged. Stale `.index-recommendations` data remains until next successful refresh. |

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
    metadata.py          # Index metadata cache (alias/data stream)
    mapping_diff.py      # Mapping vs. usage comparison engine
    recommender.py       # Mapping recommendations engine (8 rules)
    index_arch.py        # Index architecture recommendations (10 rules)
    metrics.py           # In-memory counters for monitoring
    ui.py                # Control panel HTML/JS
  generator/
    __init__.py
    generate.py          # CLI traffic generator
    queries.py           # Scenario-based query templates
    seed.py              # Sample data seeder
  tests/
    test_events.py       # Fingerprinting and event building tests
    test_extractor.py    # Path parsing, DSL extraction, bulk, msearch
    test_mapping_diff.py # Mapping diff flattening, classification, and diff tests
    test_recommender.py  # Recommendation rule tests
    test_index_arch.py   # Index architecture rule tests
    test_metadata.py     # Group resolution tests
    test_metrics.py      # In-memory counter tests
    test_queries.py      # Query template and lookback tests
```

## Deployment Architecture (OpenShift)

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

For the full tech stack evaluation (Kong, Go rewrite, and why we hardened Python instead), see [CHANGELOG.md — Production Scaling Decision Record](CHANGELOG.md#production-scaling-decision-record).

## Known Limitations

- **No authentication**: The gateway does not add auth headers. Designed for same-trust-zone deployment.
- **No SQL/ES|QL query parsing**: Only DSL queries are analyzed. ES SQL and ES|QL queries pass through unobserved. The `language` field supports future SQL tagging.
- **Partial Painless script coverage**: Fields accessed via `doc['field']` and `ctx._source.field` are extracted (~90% coverage). Fields stored in Painless variables or dynamically constructed names are not tracked.
- **Partial msearch support**: msearch query bodies are parsed, but per-query index targeting from headers is not tracked.
- **No streaming**: Large request/response bodies above 1MB are streamed, but observation (field extraction) only applies to buffered bodies below the threshold.
- **Horizontal scaling**: Supports multiple Uvicorn workers per pod and multiple pods via OpenShift HPA. State is per-process (no shared state required).
- **No persistent queue**: Events are fire-and-forget. If the gateway crashes mid-flight, in-flight events are lost.

## Future Phases

- **Dockerfile + production deployment**: Containerize the gateway alongside ES.
- **Structured logging**: JSON-formatted logs for log aggregation systems.
- **Metrics endpoint**: Prometheus/OpenTelemetry metrics for gateway health monitoring.
- **Retention policy**: Auto-delete old usage events (ILM on `.usage-events`).
- **Multi-cluster support**: Proxy and observe traffic across multiple ES clusters.
- **Authentication passthrough**: Forward auth headers and track per-user usage patterns.
- **Alerting**: Notify when indices transition between tiers or when unused fields are detected.
