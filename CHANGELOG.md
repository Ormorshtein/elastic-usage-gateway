# ES Usage Gateway — Changelog

Reverse-chronological record of significant changes, decisions, and lessons learned.

---

## 2026-02-13 — Field Drill-Down Dashboard

Added a dedicated Kibana dashboard for per-field usage investigation. Instead of telling users to manually filter in Discover, the gateway now ships a ready-made "Field Drill-Down" dashboard with controls and pre-built panels.

**What changed:**
- New "Field Drill-Down — Who Uses This Field?" Kibana dashboard (`field-drilldown`)
- 4 controls at the top: Index Group, Queried Field, Filtered Field, Aggregated Field
- 8 panels: usage over time (area), operations breakdown (pie), clients table, client IPs table, user-agents (pie), query templates table, response time over time (line), raw events
- Updated Mapping Diff dashboard markdown to reference the new drill-down dashboard
- Default time range is 7 days (longer than other dashboards, since drill-down often needs more history)

**How it works:** User selects a field name in one of the category dropdowns (e.g., "price" in "Queried Field"). All panels filter to events that reference that field, showing who queries it, when, with which templates, and the response time impact. For cross-category search, the KQL bar accepts `fields.queried: "price" OR fields.filtered: "price" OR ...`.

**Files changed:** `kibana_setup.py`

---

## 2026-02-13 — Deliverable 5: Mapping Diff Engine

Added mapping-vs-usage comparison engine. The gateway now periodically fetches index mappings, cross-references them against actual field usage from `.usage-events`, classifies every mapped field, and writes results to the `.mapping-diff` index. A new Kibana dashboard visualizes field classifications, unused fields, and type distributions.

**What changed:**
- New `gateway/mapping_diff.py` — mapping flattener, usage aggregation, field classification, background refresh loop
- New `.mapping-diff` ES index (one doc per field per index group, latest snapshot)
- New Kibana "Mapping Diff" dashboard with 6 panels: classification pie chart, classification by group bar chart, full field detail table, unused fields table, type distribution pie chart, plus markdown guidance header
- New config vars: `MAPPING_DIFF_REFRESH_INTERVAL` (default 300s), `MAPPING_DIFF_LOOKBACK_HOURS` (default 168h / 7 days)

**Classification rules:**
- `active` — field is queried, filtered, aggregated, or sorted
- `sourced_only` — field is fetched in `_source` but never used in query/filter/agg/sort
- `write_only` — field is written but never read
- `unused` — zero references in any category within the lookback window

**Per-field tracking:** Each field records `last_seen` timestamps and reference counts for all 6 usage categories (queried, filtered, aggregated, sorted, sourced, written). The overall `last_seen` is the max across all categories. This enables drill-down from the Kibana dashboard to see exactly when and how a field was last used.

**Design decision:** No JSON API endpoint. Following the same pattern as the analyzer.py removal — code computes the diff and writes results to an ES index, Kibana visualizes it. Users interact with the data in the same tool they already use.

**Files changed:** `gateway/mapping_diff.py` (new), `tests/test_mapping_diff.py` (new), `config.py`, `gateway/main.py`, `gateway/metrics.py`, `kibana_setup.py`, `ARCHITECTURE.md`, `ROADMAP.md`
**Tests added:** 48 new (264 total)

---

## 2026-02-13 — Remove Analysis Endpoints, Deliver Insights via Kibana Dashboards

Removed both `GET /_gateway/heat` and `GET /_gateway/query-patterns` JSON endpoints. All analysis is now delivered through **Kibana dashboards** with inline guidance text in Markdown panels. Deleted `gateway/analyzer.py` entirely.

**What changed:**
- Removed `/_gateway/heat` endpoint, `compute_heat()`, and all supporting functions
- Removed `/_gateway/query-patterns` endpoint and `compute_query_patterns()`
- Deleted `gateway/analyzer.py` and `tests/test_analyzer.py`
- Removed `ANALYZER_TIMEOUT` and heat threshold config vars (`INDEX_HEAT_HOT/WARM/COLD`, `FIELD_HEAT_HOT/WARM/COLD`)
- Enriched all 6 Kibana dashboard Markdown section headers with actionable "How to act on this" guidance:
  - Overview: traffic tier interpretation, index group prioritization
  - Field Heat by Count: unused fields → `index: false`, source-only fields, aggregation/doc_values tips
  - Field Heat by Response Time: optimization priority based on count vs time ranking
  - Query Patterns: costly templates, unique-template-count signals, filtering by template hash
  - Lookback Analysis: ILM policy decisions from actual query windows
  - Raw Events: filtering tips for debugging
- Updated project goal (CLAUDE.md) to include hot query recommendations

**Why:** Both endpoints duplicated what Kibana already does natively (aggregations against `.usage-events`). The only unique value was recommendation text, which is more useful baked into the dashboard where users see it, not behind JSON endpoints nobody visits. Removing `analyzer.py` simplifies the codebase — one less module for the team to maintain.

**Files changed:** `gateway/analyzer.py` (deleted), `tests/test_analyzer.py` (deleted), `gateway/main.py`, `gateway/ui.html`, `kibana_setup.py`, `config.py`, `CLAUDE.md`, `ARCHITECTURE.md`, `README.md`

---

## 2026-02-13 — Deliverable 3: Query Template Clustering

Added structural query pattern analysis. Instead of treating every unique query body as distinct, the gateway now extracts a **template** by replacing all leaf values with `"?"` and hashing the skeleton. Structurally identical queries (e.g., same bool/range/term shape with different values) collapse into one template. This reveals which query *shapes* dominate traffic and cost.

**What changed:**
- New `_templatize()` function replaces leaf values with `"?"`, collapses scalar arrays to `["?"]`, preserves dict/list structure
- New event fields: `query_template_hash` (indexed keyword for grouping) and `query_template_text` (stored for display)
- New endpoint: `GET /_gateway/query-patterns?hours=24&index_group=...` — returns templates ranked by execution count with response time stats
- Template hash computation uses `sort_keys=True`, so key order is irrelevant (same as existing fingerprint)

**Kibana dashboard reorganization:**
- Added **Markdown section headers** throughout the Usage & Heat Dashboard — each section now has a title and one-line explanation (Overview, Field Heat by Count, Field Heat by Response Time, Query Patterns, Lookback Analysis, Raw Events)
- Added 4 new query pattern panels: Top Query Templates table, Query Templates Over Time (stacked area for drift detection), Costliest Query Templates (horizontal bar by total cluster time), Unique Templates per Index Group

**Files changed:** `gateway/events.py`, `gateway/analyzer.py`, `gateway/main.py`, `kibana_setup.py`, `tests/test_events.py`, `tests/test_analyzer.py`
**Tests added:** 28 new (244 total)

---

## 2026-02-13 — Deliverable 2: Response-Time-Weighted Field Heat Scoring

Added a second field scoring panel that weights field importance by total response time rather than reference count. A field involved in slow queries now ranks higher than one involved in many fast queries — matching how pganalyze, MongoDB Atlas, and AWS RDS Performance Insights prioritize optimization targets.

**What changed:**
- Heat report now includes `fields_by_response_time` alongside `fields` in each index
- `fields` (count-based) is unchanged — full backward compatibility
- `scoring` metadata block at top level explains both methods
- `response_time_recommendations` provides time-weighted field recommendations
- ES aggregation query adds `sum` sub-agg on `response_time_ms` inside field term buckets

**No new data collection needed** — `response_time_ms` already existed in every usage event.

**Kibana dashboards:**
- Added 5 new time-weighted visualizations to the Usage & Heat Dashboard (one per field category: queried, filtered, aggregated, sorted, fetched)
- Each uses `sum(response_time_ms)` as the metric instead of event count
- Dashboard layout: count-based field tables at top, time-weighted tables below, then lookback and raw events

**Files changed:** `gateway/analyzer.py`, `tests/test_analyzer.py`, `kibana_setup.py`
**Tests added:** 7 new (217 total)

---

## 2026-02-13 — Deliverable 1: Fix Parsing Blind Spots (Gaps 1-9)

Closed all 9 MUST-HAVE parsing gaps identified in research.md. The extractor now covers the DSL features that real production traffic (especially Kibana) uses heavily.

**New operation support:**
- `_async_search` — Kibana routes 20-50% of searches through this; was completely invisible
- `_update_by_query` / `_delete_by_query` — GDPR cleanup, log rotation, data migrations

**New field extraction in search bodies:**
- `docvalue_fields` (string and `{"field": ..., "format": ...}` formats) → `sourced`
- `stored_fields` → `sourced`
- `highlight.fields` → `queried`
- `suggest` (completion, term, phrase suggesters) → `queried`
- `collapse.field` → `filtered`

**Bug fixes:**
- Composite agg sources — fields nested inside `sources[].name.type.field` were silently missed
- Filter/filters agg queries — query clauses inside filter bucket aggs were not parsed for field references

**Tests:** 24 new tests (86 total in test_extractor.py, 210 total across all test files). Zero regressions.

**Files changed:** `gateway/extractor.py`, `tests/test_extractor.py`

---

## 2026-02-12 — Competitive Research & Feature Roadmap

Added [RESEARCH.md](RESEARCH.md) with comprehensive analysis of the ES ecosystem, competitive landscape, parsing gaps, and prioritized feature roadmap.

**Key findings:**
- No existing tool (commercial or open-source) does field-level Query DSL parsing for usage intelligence. Closest is ES `_field_usage_stats` (Lucene-level, still Technical Preview).
- Opster (acquired by Elastic Nov 2023) does cluster-ops monitoring — completely different layer than our field-level analysis.
- 9 MUST-HAVE parsing gaps identified (~81 lines of code total): `async_search`, `docvalue_fields`, `highlight`, `update_by_query`, `stored_fields`, `suggesters`, `collapse`, composite agg bug, filter agg bug.
- Key lesson from pganalyze: weight field heat by response time, not just count. `field_importance = total_response_time / sum(all_response_time)`.

---

## 2026-02-09 — Production Scaling Hardening

Hardened the Python/FastAPI stack for production deployment on OpenShift. Six implementation-level fixes, no architecture change.

**Changes:**
- Bulk event writer (`_bulk` API) replacing single-doc writes — 10-50x more efficient
- Bounded `asyncio.Queue` with backpressure (drops events when full instead of OOM)
- Multi-worker support via Uvicorn `--workers` flag
- Streaming proxy for large request bodies (>1MB streamed without buffering)
- Shared httpx client for gateway endpoints (eliminated per-request client creation)
- Dedicated httpx client for event emission (isolated from proxy traffic)

**Tested:** 2000 concurrent queries, 2000 events emitted, 0 failed, 0 dropped.

### Production Scaling Decision Record

#### Context

The gateway is designed for deployment on OpenShift, sitting on the hot path of all Elasticsearch traffic. The question: can the current Python/FastAPI stack handle production scale, or do we need to move to Nginx/Kong/Go?

#### Tech Stack

| Layer | Technology | Role |
|---|---|---|
| HTTP server | Uvicorn (ASGI) | Async event loop, connection handling |
| Framework | FastAPI | Routing, middleware, request/response handling |
| Proxy client | httpx (AsyncClient) | Connection-pooled forwarding to ES |
| Event emission | httpx (separate client) | Writing usage events to `.usage-events` |
| Concurrency | asyncio (single-threaded) | Cooperative multitasking for I/O-bound work |
| State | Module-level globals | Metrics, metadata cache, sampling config (in-memory, per-process) |

#### Option A: Kong/Nginx in front of Python gateway — REJECTED

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

#### Option B: Rewrite proxy layer in Go/Rust — REJECTED

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

#### Option C: Harden the existing Python stack — ACCEPTED

The current codebase had specific scaling bottlenecks that were implementation-level, not architectural. Fixing these kept the tech stack while achieving production-grade throughput.

| Problem | Location | Impact | Fix |
|---|---|---|---|
| Single-doc event writes | `events.py` `emit_event()` | 1 ES index call per request. At 1k rps = 1k writes/sec | Buffer events in-memory, flush via `_bulk` every 500ms or 100 events |
| Unbounded background tasks | `events.py` `emit_event_background()` | No backpressure. Slow ES → unbounded task accumulation → OOM | `asyncio.Queue` with bounded size + fixed consumer pool |
| Single Uvicorn process | `main.py` `uvicorn.run()` | Zero CPU parallelism, single event loop | Multiple workers via `--workers` flag |
| Full body buffering | `proxy.py` `await request.body()` | Large bulk requests spike memory | httpx streaming for request/response bodies above a size threshold |
| New httpx client per health check | `main.py` `health()` | Connection churn under monitoring | Reuse existing shared client |
| New httpx client per sample-events call | `main.py` `sample_events()` | Same connection churn | Reuse existing shared client |

---

## 2026-02-08 — UI Overhaul & Kibana Dashboards

- Extracted UI from inline Python string to standalone `gateway/ui.html` (edit + refresh, no restart)
- Grouped monitor metrics by category (proxy, events, performance, system)
- Added reset metrics button
- Added programmatic Kibana dashboard setup (`kibana_setup.py`)
- Switched dashboards to aggregate on `index_group` instead of raw `index`
- Added screenshots to README

---

## 2026-02-07 — Event Sampling & Write Operations

- Replaced adaptive sampling and rollup system with simple `EVENT_SAMPLE_RATE` (0.0-1.0)
- Added write operation support: extract fields from `_bulk` index/create/update actions
- Added `doc`/`upsert` unwrapping for update operations
- Fixed event fan-out bug: emit one event per query instead of one per concrete index

**Lesson learned:** Adaptive sampling added complexity without proportional value. A simple rate slider (adjustable at runtime) is easier to understand, debug, and explain to the team.

---

## 2026-02-06 — Multi-Index Support & Monitoring

- Added index group resolution (alias/data stream → logical group)
- Added metadata cache with periodic refresh from `GET /_aliases` and `GET /_data_stream/*`
- Added lookback window detection from `range` queries with `now-*` syntax
- Added query body storage with configurable sampling
- Added monitoring tab with auto-refresh stats
- Added request timing middleware
- Architect-level code review: fixed bugs, simplified abstractions, added ARCHITECTURE.md

---

## 2026-02-05 — Scenarios & Generator

- Added logs and orders indices to seeder (alongside products)
- Added scenario-based query templates with weighted distributions
- Added scenario-aware generator endpoint with dynamic UI controls
- Added tier recommendations to heat report (hot/warm/cold/frozen with actionable suggestions)

---

## 2026-02-04 — Initial Release

- ES Usage Gateway MVP: FastAPI reverse proxy, DSL field extraction, heat analysis
- Supports `match`, `term`, `range`, `bool`, `multi_match`, `nested`, and 10+ other query types
- Usage events stored in `.usage-events` index
- Heat report API with index-level and field-level tiers
- CLI traffic generator
- 14 tests covering extractor, events, analyzer, metadata, metrics, and query templates
