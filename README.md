# ES Usage Gateway

A reverse-proxy gateway for Elasticsearch that observes query traffic and computes index-level and field-level heat scores. Helps identify hot/warm/cold/unused indices and fields for ILM and mapping optimization.

## Architecture

```
Query Generator --> Gateway (port 9301) --> Elasticsearch (port 9200)
                       |
                       +---> .usage-events index --> Heat Analyzer
```

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## How It Works

The gateway sits between your application and Elasticsearch as a transparent proxy. Every query that passes through is **observed but never modified** — the gateway parses the Elasticsearch Query DSL to extract which fields are being used and how.

### Extraction Example

When a search request like this passes through the gateway:

```
POST /products/_search
```
```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "wireless headphones" } }
      ],
      "filter": [
        { "term": { "category": "electronics" } },
        { "range": { "price": { "gte": 20, "lte": 100 } } }
      ]
    }
  },
  "aggs": {
    "by_brand": { "terms": { "field": "brand" } },
    "avg_rating": { "avg": { "field": "rating" } }
  },
  "sort": [{ "price": "desc" }],
  "_source": ["title", "price", "brand"]
}
```

The gateway extracts field usage into categories:

| Category | Fields | Meaning |
|----------|--------|---------|
| **queried** | `title` | Used in `match`, `term`, `multi_match`, etc. |
| **filtered** | `category`, `price` | Used inside `bool.filter` context |
| **aggregated** | `brand`, `rating` | Used in aggregations (`terms`, `avg`, etc.) |
| **sorted** | `price` | Used in `sort` clauses |
| **sourced** | `title`, `price`, `brand` | Returned in `_source` |

This produces a usage event stored in `.usage-events`:

```json
{
  "timestamp": "2026-02-08T12:00:00Z",
  "index": "products",
  "index_group": "products",
  "operation": "search",
  "fields": {
    "queried": ["title"],
    "filtered": ["category", "price"],
    "aggregated": ["brand", "rating"],
    "sorted": ["price"],
    "sourced": ["brand", "price", "title"],
    "written": []
  },
  "response_time_ms": 42.5,
  "response_status": 200
}
```

Over time, these events accumulate and the **heat analyzer** computes proportional field heat: if `title` appears in 30% of all field references for the `products` index, it's classified as **hot**. If `legacy_supplier_code` never appears, it's **unused** — a candidate for `"index": false` to save disk and indexing cost.

The extractor also handles:
- **Bulk requests** (`_bulk`): extracts written fields from index/create/update actions, including unwrapping `doc`/`upsert` wrappers
- **Multi-search** (`_msearch`): parses each query in the NDJSON body
- **Lookback windows**: detects `now-24h` style range filters and records the time window

### Event Sampling

In production, high-traffic clusters can generate a large volume of usage events. The gateway supports **configurable event sampling** to reduce load on the `.usage-events` index:

- Set `EVENT_SAMPLE_RATE` (0.0-1.0) to control what fraction of requests emit events
- Adjustable at runtime via the UI slider or `PATCH /_gateway/config`
- **Field heat is unaffected** by sampling — heat scores are proportions (ratios), so both numerator and denominator scale equally
- Index heat (ops/hour) will be proportionally lower, but relative rankings between indices are preserved

### Index and Index Group Resolution

Every usage event contains two index-related fields: `index` (raw) and `index_group` (logical).

**`index`** is extracted from the URL path by `parse_path()` in `extractor.py` — it's whatever the client targeted:

| Request path | `index` |
|---|---|
| `/orders/_search` | `orders` (the alias) |
| `/orders-us/_doc/1` | `orders-us` (concrete) |
| `/logs-2026.02.06/_bulk` | `logs-2026.02.06` (concrete) |
| `/products/_search` | `products` (alias = concrete) |
| `/_bulk` | `None` (system endpoint) |

**`index_group`** is resolved by `resolve_group()` in `metadata.py` using a cache of ES alias and data stream mappings (refreshed every 60s from `GET /_aliases` and `GET /_data_stream/*`):

1. **Concrete index lookup**: If the name is a known concrete index, return its alias/data stream group.
2. **Group name match**: If the name is itself a known group (alias queried directly), return it.
3. **Fallback**: Return the name as-is (unknown index, or metadata not yet loaded).

| `index` | Resolution path | `index_group` |
|---|---|---|
| `orders-us` | Concrete → alias `orders` | `orders` |
| `orders-eu` | Concrete → alias `orders` | `orders` |
| `orders` | Is a group name → itself | `orders` |
| `logs-2026.02.06` | Concrete → alias `logs` | `logs` |
| `products` | Concrete → alias `products` (self) | `products` |
| `mystery` | Not found → fallback | `mystery` |

**Why dashboards aggregate on `index_group`, not `index`**: Read queries target aliases (`/orders/_search`), while writes target concrete indices (`/orders-us/_doc/1`). If dashboards aggregated on `index`, reads and writes would appear as separate buckets. The `index_group` gives a consistent view where all operations on the same logical index contribute to the same bucket. The raw `index` field is preserved in events for debugging in Discover.

## Quick Start

### 1. Start Elasticsearch & Kibana

The project includes a `docker-compose.yml` that runs Elasticsearch 8.12.2 and Kibana 8.12.2 as containers (single-node, security disabled for local dev).

```bash
docker-compose up -d
# Wait for ES to be healthy
curl http://localhost:9200/_cluster/health
```

- **Elasticsearch**: [http://localhost:9200](http://localhost:9200)
- **Kibana**: [http://localhost:5601](http://localhost:5601)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the Gateway

```bash
python -m gateway.main
# Gateway listens on port 9301, proxies to ES on port 9200
```

### 4. Seed Sample Data

```bash
python -m generator.seed --gateway
# Creates 'products' index with 100 sample documents
```

### 5. Import Kibana Dashboards

```bash
python kibana_setup.py --no-wait
```

### 6. Generate Traffic & View Results

You can generate traffic in two ways:

**Option A — Via the UI** (recommended): Open the gateway control panel at [http://localhost:9301/_gateway/ui](http://localhost:9301/_gateway/ui). Use the Generator tab to select a scenario, adjust query weights, and click **Run Scenario** or **Run All Scenarios**.

**Option B — Via CLI**:
```bash
python -m generator.generate --duration 60 --rps 10
```

After generating traffic, view the results:
- **Kibana dashboards**: Open [http://localhost:5601](http://localhost:5601) and navigate to **Dashboards** to see the pre-built usage and heat visualizations.
- **Heat report API**: `curl http://localhost:9301/_gateway/heat | python -m json.tool`
- **Gateway monitor**: Switch to the **Monitor** tab in the UI to see live stats.

## Configuration

All settings via environment variables (see `config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `ES_HOST` | `http://localhost:9200` | Elasticsearch URL |
| `GATEWAY_HOST` | `0.0.0.0` | Gateway bind address |
| `GATEWAY_PORT` | `9301` | Gateway listen port |
| `USAGE_INDEX` | `.usage-events` | Index for storing usage events |
| `CLUSTER_ID` | `default` | Cluster identifier |
| `PROXY_TIMEOUT` | `120` | Proxy request timeout (seconds) |
| `EVENT_TIMEOUT` | `10` | Event emission timeout (seconds) |
| `ANALYZER_TIMEOUT` | `30` | Heat analysis query timeout (seconds) |
| `METADATA_REFRESH_INTERVAL` | `60` | Metadata cache refresh (seconds) |
| `EVENT_SAMPLE_RATE` | `1.0` | Fraction of requests that emit events (0.0-1.0) |
| `QUERY_BODY_ENABLED` | `true` | Store query bodies in events |
| `QUERY_BODY_SAMPLE_RATE` | `1.0` | Fraction of events to store bodies |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /_gateway/health` | Health check with ES connectivity probe |
| `GET /_gateway/stats` | Internal counters and metadata cache info |
| `GET /_gateway/heat?hours=24` | Heat report for the last N hours |
| `GET /_gateway/groups` | Index groups with concrete indices |
| `GET /_gateway/sample-events` | Recent usage events for debugging |
| `GET/PATCH /_gateway/config` | Event sampling and query body storage config |
| `GET /_gateway/ui` | Control panel UI |
| `POST /_gateway/generate` | Run query generator from UI |
| `DELETE /_gateway/events` | Clear all usage events |
| `* /{path}` | All other traffic proxied to Elasticsearch |

## Monitoring

### Health Check

```bash
curl http://localhost:9301/_gateway/health
```

Returns **200** when ES is reachable:
```json
{
  "service": "es-usage-gateway",
  "status": "healthy",
  "elasticsearch": "reachable",
  "uptime_seconds": 3600.1,
  "events_emitted": 1250,
  "events_failed": 3
}
```

Returns **503** when ES is unreachable:
```json
{
  "service": "es-usage-gateway",
  "status": "unhealthy",
  "elasticsearch": "connection refused",
  "uptime_seconds": 120.5,
  "events_emitted": 0,
  "events_failed": 15
}
```

Use this endpoint for load balancer health checks or uptime monitoring.

### Internal Stats

```bash
curl http://localhost:9301/_gateway/stats
```

Returns all internal counters:
```json
{
  "requests_proxied": 5000,
  "requests_failed": 2,
  "events_emitted": 4500,
  "events_failed": 8,
  "events_skipped": 490,
  "events_sampled_out": 120,
  "extraction_errors": 0,
  "metadata_refresh_ok": 60,
  "metadata_refresh_failed": 0,
  "startup_time": "2026-02-06T10:00:00+00:00",
  "uptime_seconds": 3600.1,
  "metadata_cache": {
    "groups": 5
  }
}
```

Key metrics to watch:
- **events_failed** — if this grows steadily, event writes to ES are failing
- **events_sampled_out** — events skipped due to sampling (expected when rate < 100%)
- **requests_failed** — proxy 502 errors (ES unreachable for proxied requests)
- **extraction_errors** — DSL parsing failures (should be rare/zero)
- **metadata_refresh_failed** — if this grows, the alias/data stream cache is stale

All counters reset to zero on restart.

## Crash Behavior

The gateway is designed with a "never block the request" principle. Observation (field extraction, event emission) is fire-and-forget. Here's what happens if the gateway crashes:

| State | On crash | After restart |
|-------|----------|---------------|
| Proxied traffic | Interrupted | Resumes immediately |
| In-flight events | Lost (fire-and-forget tasks) | Fresh start |
| Usage events in ES | Preserved (already written) | Still available for heat analysis |
| Metadata cache | Lost (in-memory) | Rebuilt automatically in <1 second |
| Runtime config changes | Lost (in-memory) | Reverts to environment variable defaults |
| Metrics counters | Lost (in-memory) | Reset to zero |

### Key design decisions

- **Events are best-effort**: A small number of lost events during a crash does not meaningfully affect heat analysis, which operates on aggregated data over hours/days.
- **No persistent queue**: Events go directly to ES via fire-and-forget. This keeps the gateway simple and avoids introducing queue management complexity.
- **Startup without ES**: The gateway starts even if Elasticsearch is unreachable. It will return 502 for proxied requests but the health endpoint will report `unhealthy`. Once ES becomes available, everything recovers automatically.
- **Clean shutdown**: On graceful shutdown (SIGTERM), all HTTP clients are properly closed and connections released.
