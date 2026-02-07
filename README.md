# ES Usage Gateway

A reverse-proxy gateway for Elasticsearch that observes query traffic and computes index-level and field-level heat scores. Helps identify hot/warm/cold/unused indices and fields for ILM and mapping optimization.

## Architecture

```
Query Generator → Gateway (port 9201) → Elasticsearch (port 9200)
                     │
                     └──▶ .usage-events index → Heat Analyzer
```

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick Start

### 1. Start Elasticsearch

```bash
docker-compose up -d
# Wait for ES to be healthy
curl http://localhost:9200/_cluster/health
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the Gateway

```bash
python -m gateway.main
# Gateway listens on port 9201, proxies to ES on port 9200
```

### 4. Seed Sample Data

```bash
python -m generator.seed --gateway
# Creates 'products' index with 100 sample documents
```

### 5. Generate Traffic

```bash
python -m generator.generate --duration 60 --rps 10
# Sends 600 queries with intentionally skewed field usage
```

### 6. View Heat Report

```bash
curl http://localhost:9201/_gateway/heat | python -m json.tool
```

## Expected Results

After running the generator, the heat report should show:

| Tier | Fields |
|------|--------|
| Hot | title, category, price |
| Warm | brand, rating, description |
| Unused | internal_sku, legacy_supplier_code, stock_count, created_at, subcategory, tags |

## Configuration

All settings via environment variables (see `config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `ES_HOST` | `http://localhost:9200` | Elasticsearch URL |
| `GATEWAY_HOST` | `0.0.0.0` | Gateway bind address |
| `GATEWAY_PORT` | `9201` | Gateway listen port |
| `USAGE_INDEX` | `.usage-events` | Index for storing usage events |
| `CLUSTER_ID` | `default` | Cluster identifier |
| `PROXY_TIMEOUT` | `120` | Proxy request timeout (seconds) |
| `EVENT_TIMEOUT` | `10` | Event emission timeout (seconds) |
| `ANALYZER_TIMEOUT` | `30` | Heat analysis query timeout (seconds) |
| `METADATA_REFRESH_INTERVAL` | `60` | Metadata cache refresh (seconds) |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /_gateway/health` | Health check with ES connectivity probe |
| `GET /_gateway/stats` | Internal counters and metadata cache info |
| `GET /_gateway/heat?hours=24` | Heat report for the last N hours |
| `GET /_gateway/groups` | Index groups with concrete indices |
| `GET /_gateway/sample-events` | Recent usage events for debugging |
| `GET/PATCH /_gateway/config` | Query body storage configuration |
| `GET /_gateway/ui` | Control panel UI |
| `POST /_gateway/generate` | Run query generator from UI |
| `DELETE /_gateway/events` | Clear all usage events |
| `* /{path}` | All other traffic proxied to Elasticsearch |

## Monitoring

### Health Check

```bash
curl http://localhost:9201/_gateway/health
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
curl http://localhost:9201/_gateway/stats
```

Returns all internal counters:
```json
{
  "requests_proxied": 5000,
  "requests_failed": 2,
  "events_emitted": 4500,
  "events_failed": 8,
  "events_skipped": 490,
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
