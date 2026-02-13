# ES Usage Gateway — Research & Feature Gap Analysis

## Context

This document captures the full research into related solutions, competitive landscape,
parsing gaps, missing features, and architecture patterns for the ES Usage Gateway.
The gateway is a transparent reverse proxy that intercepts Elasticsearch traffic, parses
DSL queries to extract field-level usage (queried, filtered, aggregated, sorted, sourced,
written), computes heat scores, and surfaces which fields are hot vs. cold. The goal is
to enable index mapping optimization.

---

## 1. Native ES APIs: Integrate, Don't Duplicate

### 1.1 `_field_usage_stats` API (ES 7.15+, Technical Preview)

The `_field_usage_stats` API is an index-level endpoint that reports cumulative counts of
how Lucene data structures have been accessed for each field in an index. Still marked as
**Technical Preview** — never promoted to GA.

**Endpoint:**
```
GET /<index>/_field_usage_stats
GET /<index>/_field_usage_stats?fields=title,price,category
```

**What it tracks per field, per shard:**

| Category | Meaning | Triggered By |
|---|---|---|
| `any` | Field was accessed at all | Any of the below |
| `inverted_index.terms` | Postings list was accessed | `term`, `terms`, `match`, `match_phrase`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `exists` queries; `terms` aggregation on keyword fields |
| `inverted_index.postings` | Postings with positions/offsets | `match_phrase` (needs positions), highlighting |
| `inverted_index.proximity` | Proximity-based access | `match_phrase` with slop, `span_near` |
| `inverted_index.payloads` | Payload data accessed | Custom similarity with payloads (rare) |
| `stored_fields` | Stored field values loaded | `stored_fields` parameter, `_source` reconstruction |
| `doc_values` | Columnar doc_values accessed | `sort`, `terms`/`histogram`/`date_histogram` aggregations on keyword/numeric/date fields, `script_fields` accessing doc values, field collapsing |
| `points` | BKD tree accessed | `range` queries on numeric/date/geo fields |
| `norms` | Field length norms read | Any query that uses BM25 relevance scoring on that field |
| `term_vectors` | Term vector data accessed | `_termvectors` API, MLT query |
| `knn_vectors` | Dense vector access | `knn` search |

**Critical limitations:**

- **Shard-level, not query-level.** No way to determine which specific query caused each access,
  when the access happened, or which client initiated it.
- **Cannot distinguish queried vs. filtered.** A `term` query in `bool.must` (scoring) and in
  `bool.filter` (non-scoring) both show up as `inverted_index.terms`. Only hint: scoring context
  also increments `norms`.
- **Cannot distinguish aggregated vs. sorted.** Both access `doc_values`.
- **No `_source` field selection tracking.** Reports `stored_fields` access on `_source` as a whole.
- **No write-side tracking.** Read-path only.
- **Counters reset on node restart or shard relocation.** Not persisted.
- **Monotonically increasing counters** with no time-series history.

**Example response:**
```json
{
  "products": {
    "shards": {
      "0": [{
        "stats": {
          "fields": {
            "title": {
              "any": 5000,
              "inverted_index": { "terms": 5000, "postings": 200, "proximity": 50 },
              "stored_fields": 0, "doc_values": 0, "points": 0, "norms": 4500
            },
            "price": {
              "any": 8000,
              "inverted_index": { "terms": 0 },
              "stored_fields": 0, "doc_values": 6000, "points": 3000, "norms": 0
            }
          }
        }
      }]
    }
  }
}
```

**Gap analysis vs. the gateway:**

| Capability | `_field_usage_stats` | Gateway |
|---|---|---|
| Distinguishes queried vs. filtered | Indirect only (norms hint) | **Yes** (bool clause walking) |
| Distinguishes aggregated vs. sorted | No (both are `doc_values`) | **Yes** (separate categories) |
| Tracks `_source` field selection | No | **Yes** (parses includes/excludes) |
| Tracks written fields | No (read-path only) | **Yes** (index/bulk/update parsing) |
| Per-request attribution | No | **Yes** (timestamp, client_id, fingerprint) |
| Time-series analysis | No (cumulative counter) | **Yes** (per-event timestamps in ES index) |
| Survives node restarts | No (counters reset) | **Yes** (events persisted) |
| Catches script-based field access | **Yes** (Lucene-level) | No |
| Catches direct-to-ES traffic | **Yes** (all traffic) | No (only proxied traffic) |
| Works without proxy deployment | **Yes** (built-in) | No |

**Integration action:** Poll periodically — cross-reference with our data to validate proxy
coverage and detect direct-to-ES traffic. If `_field_usage_stats` shows field X is accessed
but our events show zero references, some traffic is bypassing the proxy.

### 1.2 `_field_caps` API

Returns metadata about fields available across one or more indices. A **mapping introspection**
tool, not a usage tracking tool. Available since ES 5.4.

```
GET /<index>/_field_caps?fields=*
```

**What it returns per field:**
- `type` — field data type (keyword, text, long, date, etc.)
- `is_searchable` — whether the field can be used in queries
- `is_aggregatable` — whether the field supports aggregations
- `indices` — which indices contain this field
- `non_searchable_indices` / `non_aggregatable_indices` — indices with different capabilities
- Cross-index **type conflict detection** (e.g., `status` is `keyword` in index A but `long` in index B)

**Zero usage data.** Does not tell you whether a field is ever queried, how often, or by whom.

**How it complements the gateway:**

| Gateway Finding | `_field_caps` Enrichment | Combined Recommendation |
|---|---|---|
| Field `description` is never queried | `is_searchable: true`, type: `text` | "Field has inverted index but is never queried. Set `index: false`." |
| Field `price` is heavily aggregated | `is_aggregatable: true`, type: `float` | "Aggregation is healthy. Doc_values are enabled and being used." |
| Field `notes` is only written, never read | `is_searchable: true, is_aggregatable: true` | "Set `index: false, doc_values: false` and store in `_source` only." |
| Field `status` used across indices | Type conflict: keyword vs. text | "Type conflict. Standardize to keyword." |

**Integration action:** Poll `_field_caps?fields=*` on each tracked index periodically
(matching `METADATA_REFRESH_INTERVAL`). Annotate heat reports with field types, detect
unused indexed fields, flag type conflicts.

### 1.3 Index Stats API (`_stats`)

Returns operational and resource metrics at the index level.

```
GET /<index>/_stats
GET /<index>/_stats/fielddata,search,indexing?fields=*
```

**What it provides:**
- **Search metrics**: query_total, query_time_in_millis, fetch_total, scroll_total (per-index totals, no per-field breakdown)
- **Indexing metrics**: index_total, index_time_in_millis, delete_total
- **Fielddata metrics**: memory_size_in_bytes per field (only for `fielddata: true` fields — legacy)
- **Segments metrics**: stored_fields_memory, doc_values_memory, norms_memory, points_memory (per-index aggregates, not per-field)
- **Store metrics**: size_in_bytes, doc count

**Integration action:** Poll periodically — enable cost-impact ranking of recommendations.
"200GB index with zero query traffic — candidate for frozen tier."

### 1.4 Other Native APIs

| API | What It Provides | Relevance |
|---|---|---|
| `_nodes/usage` | Cumulative counts of REST endpoint invocations and aggregation types per node | **Fully subsumed** by our gateway. Low value. |
| Slow query logs | Query bodies for queries exceeding time threshold | Useful for cross-reference, but rebuilding our gateway from slow logs adds complexity for less accuracy |
| Audit logging (X-Pack) | Authenticated user identity per request | Unique complement — user/service identity. Requires Gold+ license and `emit_request_body: true`. |
| Watcher / Alerting | Scheduled queries with threshold-based actions | **Downstream consumer** of our `.usage-events` data — not a competitor |
| ILM | Automated index lifecycle (hot/warm/cold/frozen/delete) | Based on age/size, NOT usage. Our heat data could make ILM smarter. |
| Profile API (`?profile=true`) | Per-shard, per-query-clause Lucene execution timing | Expensive (2-10x overhead). Useful for sampled profiling, not continuous. |

### 1.5 Runtime Fields (Tracking Challenge)

Runtime fields (ES 7.11+) create a **tracking blind spot for both the gateway and native APIs**:
- Gateway sees the runtime field name but not the underlying field access (in the Painless script)
- `_field_usage_stats` sees the underlying field access but not the runtime field name
- Neither provides the complete picture alone

Gateway is better positioned to close this gap by parsing `runtime_mappings` definitions
and extracting `doc['field']` references from scripts.

### 1.6 Data Streams

Data streams are **well-handled by the gateway's existing architecture.** The `index_group`
concept and URL-path-based index extraction naturally operate at the data stream name level.
Minor enhancements: detect `.ds-` prefixed backing index names, track template relationships.

---

## 2. Competitive Landscape

### 2.1 Opster / AutoOps (Acquired by Elastic, Nov 2023)

**Timeline:**
- November 14, 2023 — Elastic announced intent to acquire Opster
- November 30, 2023 — Acquisition completed
- November 2024 — AutoOps integrated into Elastic Cloud (zero-setup, AWS US-East-1 first)
- October 2025 — AutoOps extended to self-managed Elasticsearch users

**Opster's tools:**

**AutoOps / Management Console:**
- Monitored hundreds of ES metrics in real-time: nodes (JVM, CPU, memory, disk), indices (health, shard balance), templates, query performance, ingestion
- Pre-configured alerts (PagerDuty, Slack, Teams, webhooks)
- Root-cause analysis with resolution paths including ready-to-run ES commands
- Cost optimization recommendations (hardware right-sizing, replica reduction)
- **Did NOT parse Query DSL bodies. Did NOT track individual field usage. Did NOT compute field-level heat scores.** Query analysis detected structural problems (nested aggs, overly broad time ranges, regex in queries) via Task Management API, not body parsing.

**Slow Log Analyzer (free community tool):**
- One-shot upload of slow log files — NOT continuous monitoring
- Showed search "took" time distributions, counted costly queries
- Identified WHY queries were slow (aggregation cardinality, large size param, nested aggs)
- Provided per-query optimization recommendations
- **Did NOT extract field names from query bodies. No field frequency counting, no heat scores, no mapping recommendations based on actual query patterns.**

**Template Optimizer (free community tool):**
- Accepted a JSON index template, analyzed for suboptimal settings
- Recommendations: appropriate data types, disable `index` for non-queried fields, disable `doc_values` for non-aggregated fields, shard/replica tuning
- **Static heuristic analysis — did NOT cross-reference with actual query traffic.** Advice was generic best-practice, not data-driven.

**OpsGPT (AI Assistant):**
- LLM wrapper around AutoOps metrics, conversational interface
- Did NOT do independent query analysis or field-level tracking

**What Opster did that we don't:**
1. Cluster health monitoring (node-level CPU, JVM, memory, disk)
2. Shard rebalancing analysis
3. Proactive alerting with PagerDuty/Slack
4. Cost optimization / hardware right-sizing
5. AI conversational interface

**What we do that Opster never did:**
1. Query DSL body parsing with field extraction
2. Field-level heat scores with 6-category breakdown
3. Data-driven mapping recommendations
4. Lookback window analysis for ILM policy guidance
5. Write-path field tracking

**They're in completely different layers — Opster = cluster ops, our gateway = field intelligence.**

### 2.2 Elastic APM

- APM agents instrument ES client calls, creating spans with: type "db", subtype "elasticsearch", duration, HTTP status
- Optionally captures query body (`ELASTIC_APM_CAPTURE_BODY`) as an **opaque blob** — no parsing, no field extraction
- Provides distributed tracing context (links ES calls to parent transactions)
- Provides service maps / dependency topology
- **Zero field-level analysis.** The APM UI shows raw JSON — you cannot ask "which fields are most commonly filtered on?"

**What APM has that we lack:** distributed tracing context, service maps, error correlation, user experience correlation (RUM).
**What we have that APM lacks:** everything field-level.

**Integration opportunity:** Propagate W3C `traceparent` headers through our proxy, store `trace_id` in usage events. Enables linking field usage to application transactions.

### 2.3 Elastic Rally

Benchmarking tool for ES. Runs predefined workloads ("tracks") and measures performance.
**Zero field-level analysis.** Treats queries as opaque operations, only measures latency/throughput.

**Integration opportunity:** Use Rally as a validation step downstream of our recommendations.
"Before: 50ms avg search. After removing unused field indexes: 42ms avg search."

### 2.4 Elasticsearch Curator

Index management tool (delete, close, forcemerge, snapshot, etc.) based on time/size/count
criteria. **Zero field-level awareness.** Largely superseded by ILM.

### 2.5 ReadonlyREST / SearchGuard

ES security plugins (auth, FLS, DLS, audit). Field-Level Security **restricts access** to
fields — it does NOT **track usage** of fields. ReadonlyREST's proxy mode is architecturally
similar to our gateway but with security intent, not observability.

### 2.6 ElastAlert / ElastAlert2

Alerting framework that queries data in ES. **Downstream consumer** of our data — can query
`.usage-events` for threshold alerts on field usage patterns. Not a competitor.

### 2.7 ES Management UIs

- **dejavu**: Spreadsheet-like data browser. Shows mapping but no usage tracking.
- **elasticsearch-head**: Cluster overview, query tab. No field analytics.
- **cerebro**: Cluster admin (shard visualization, index management). No field analytics.

None track field usage.

### 2.8 Commercial Solutions

| Capability | Elastic Cloud | AWS OpenSearch | Datadog | Dynatrace | New Relic | Sematext | **Our Gateway** |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Cluster health metrics | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Node-level metrics | Yes | Yes (CloudWatch) | Yes | Yes | Yes | Yes | No |
| Slow query capture | Yes | Yes | Yes (via logs) | No | No | Yes | N/A (captures all) |
| Distributed tracing | Yes (APM) | Yes (OTel) | Yes | Yes | Yes | No | No |
| Query body capture | Yes (APM) | Yes (Query Insights) | Yes (APM) | Partial | Partial | No | Yes |
| **Query DSL field parsing** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** |
| **Field usage categorization** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** (6 types) |
| **Field heat scoring** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** |
| **Schema optimization recs** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** |
| **Write-side field tracking** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** |
| **Lookback window analysis** | **No** | **No** | **No** | **No** | **No** | **No** | **Yes** |
| AI/ML anomaly detection | Yes | No | Yes | Yes (Davis) | Yes | Yes | No |
| Cost | $$$ | $$ | $$$ | $$$$ | $$ | $$ | Free |

**Sematext** specifically: Monitors 100+ ES metrics, has built-in anomaly detection and
threshold alerts, can find slow/broken/zero-hit queries. **Does NOT parse query bodies or
track field usage.** Competitive with AutoOps for cluster monitoring, but no field-level intelligence.

**AWS OpenSearch Query Insights** (2.12+): Captures top-N queries by latency/CPU/memory,
stores full query body, can group similar queries. Does NOT parse for field extraction,
does NOT compute field heat, does NOT generate mapping recommendations. Focuses on
performance, not schema optimization.

### 2.9 Database Observability Analogs

These are the most instructive comparisons — they solve the same problem for other databases.

**pganalyze (PostgreSQL):**
- Column usage tracking across 6 SQL clause types (WHERE, JOIN, ORDER, GROUP, SELECT, functions)
- **Index Advisor**: compares `pg_stat_statements` against indexes, recommends CREATE INDEX with estimated impact
- EXPLAIN plan analysis
- **Response-time-weighted importance**: ranks queries by `total_time = frequency * avg_latency`, not just count
- Time-series trend visualization (days/weeks)
- Unused index detection

**VividCortex / SolarWinds DPA (MySQL, PostgreSQL, MongoDB, Redis):**
- Top queries by total time consumed
- ML-based anomaly detection
- Regression detection ("this query used to be 10ms, now 200ms")
- Adaptive sampling (more detail during spikes)

**MongoDB Atlas Performance Advisor:**
- Query shape analysis (equivalent to fingerprinting)
- Recommends `createIndex()` with specific field order for equality + range + sort
- Ranks recommendations by impact (how many slow queries would benefit)
- Docs examined vs. docs returned ratio as efficiency metric
- Schema analysis (field types, frequency, value distributions)

**Feature comparison with analogs:**

| Feature | pganalyze | VividCortex/DPA | MongoDB Atlas | **Our Gateway** |
|---|:---:|:---:|:---:|:---:|
| Query fingerprinting | Yes (param replace) | Yes (param replace) | Yes (shape) | Yes (SHA-256 canonical) |
| Column/field usage tracking | Yes (6 SQL clause types) | Yes (table + column) | Yes (predicate + sort + project) | Yes (6 ES categories) |
| Usage-based index/mapping recs | Yes (CREATE INDEX) | No | Yes (createIndex) | Yes (index:false, doc_values) |
| Execution plan analysis | Yes (EXPLAIN) | Yes | Yes (explain) | No |
| **Time-weighted importance** | **Yes (total time)** | **Yes (total time)** | **Yes (duration * count)** | **No (count-based only)** |
| Time-series trends | Yes (weeks/months) | Yes (weeks/months) | Yes (days) | Partial (data exists) |
| Anomaly detection | No | Yes (ML) | No | No |
| Unused index detection | Yes | Partial | Yes ($indexStats) | Partial (unused fields) |
| Write-side tracking | No | No | No | **Yes** |
| Lookback analysis | No | No | No | **Yes** |
| Zero code changes | No (needs pg extension) | No (needs agent) | No (Atlas built-in) | **Yes** (proxy) |
| Adaptive sampling | No | Yes | No | No (static config) |

### 2.10 Other Notable Findings

- **GitHub issue elastic/elasticsearch#68759**: Community request for an "Index Mappings Prune API"
  to detect and remove unused fields from mappings. **Never implemented.** This is exactly what
  our gateway enables.
- **Sematext elasticsearch-field-stats**: Replacement for ES's deprecated Field Stats API (removed
  in ES 6.0). Focuses on field value statistics (min/max/cardinality), NOT field usage/access patterns.
- **ES Profile API**: Per-query Lucene operation timing (`?profile=true`). Single-query debugging tool
  with 2-10x overhead. Not for continuous monitoring but could be sampled.

### 2.11 Competitive Positioning Summary

**No tool in the Elasticsearch ecosystem — commercial or open source, including Opster/AutoOps,
Sematext, Datadog, Dynatrace, or any other — does what our ES Usage Gateway does.**

Nobody parses production Query DSL traffic for field-level intelligence. The `_field_usage_stats`
API provides complementary low-level data. Opster/AutoOps operates at cluster-ops level. All
commercial tools (Datadog, Sematext, etc.) monitor cluster/node/index metrics.

The closest analogs exist in other database ecosystems (pganalyze for Postgres, MongoDB Atlas
Performance Advisor) but nothing equivalent exists for Elasticsearch.

---

## 3. Parsing Gaps — Prioritized

### 3.1 Current State of the Extractor

The extractor (`gateway/extractor.py`) currently handles:

**Operations routed for field extraction:**
- `search` and `count` — parsed as DSL search body
- `msearch` — NDJSON multi-search
- `doc` (PUT/POST) — document write
- `update` (POST) — partial update, extracts from `doc`/`upsert` wrappers
- `bulk` — NDJSON bulk

**DSL elements parsed within search:**
- `query` — recursed via `_extract_query_fields`
- `post_filter` — recursed with `context="filtered"`
- `aggs` / `aggregations` — recursed via `_extract_agg_fields`
- `sort` — via `_extract_sort_fields`
- `_source` — via `_extract_source_fields`
- Time-range lookback parsing from `range` queries

**Recognized leaf query types:** `match`, `match_phrase`, `match_phrase_prefix`, `term`, `terms`,
`range`, `wildcard`, `prefix`, `fuzzy`, `regexp`, `exists`, `multi_match`

**Recognized compound queries:** `bool` (must/should/must_not/filter with correct context),
`nested`

**Recognized metric aggs:** `avg`, `sum`, `min`, `max`, `cardinality`, `value_count`, `stats`,
`extended_stats`, `percentiles`, `percentile_ranks`, `median_absolute_deviation`, `top_hits`

**Recognized bucket aggs:** `terms`, `date_histogram`, `histogram`, `range`, `date_range`,
`filter`, `filters`, `significant_terms`, `composite`, `auto_date_histogram`

### 3.2 MUST-HAVE Gaps (Implement First — ~81 lines total)

#### Gap 1: `_async_search` (1 line)

**The single highest-ROI fix.** Kibana routes 20-50% of searches through `POST /<index>/_async_search`.
The body is structurally identical to `_search`. Currently invisible because `"async_search"` doesn't
match the `operation in ("search", "count")` check.

**Fix:** Add `"async_search"` to the operation set.

#### Gap 2: `docvalue_fields` (10 lines)

**Kibana Discover uses `docvalue_fields` in nearly every query.** Each element is either a plain
string or `{"field": "name", "format": "..."}`. In Kibana-heavy deployments, 10-40% of search
requests include `docvalue_fields`.

```json
"docvalue_fields": [
  {"field": "timestamp", "format": "date_time"},
  "status_code",
  {"field": "level", "format": "use_field_mapping"}
]
```

Category: `sourced`. Parse both string and object formats.

#### Gap 3: Highlighting (10 lines)

Every search UI with result snippets. 15-40% of user-facing search traffic.

```json
"highlight": {
  "fields": {
    "title": {},
    "description": {"fragment_size": 200, "number_of_fragments": 2}
  }
}
```

Fields: keys of `highlight.fields` dict. Also parse `matched_fields` arrays and
`highlight_query` sub-queries within per-field configs.

Category: `queried`.

#### Gap 4: `_update_by_query` / `_delete_by_query` (10 lines)

Query body is standard DSL — free to parse. Operationally critical for log rotation,
GDPR cleanup, data migrations. Currently falls through with empty `FieldRefs()` because
`"update_by_query"` and `"delete_by_query"` don't match any dispatch case.

For `_update_by_query`, the `script.source` contains Painless that modifies fields
(track as `written` if Painless extraction is implemented).

#### Gap 5: `stored_fields` (5 lines)

Structurally identical to `_source` when it's a list. Category: `sourced`.

```json
"stored_fields": ["title", "price", "category"]
```

#### Gap 6: Suggesters (10 lines)

Autocomplete (`completion` suggester) is the highest-frequency query type in many search
applications — every keystroke sends a query. Also `term` and `phrase` suggesters.

```json
"suggest": {
  "title-suggest": {
    "text": "lapt",
    "completion": {"field": "title.suggest", "fuzzy": {"fuzziness": "auto"}}
  }
}
```

Field is at `suggest.NAME.<type>.field`. Category: `queried`.

#### Gap 7: Field Collapsing (5 lines)

Common in e-commerce dedup ("show one result per brand").

```json
"collapse": {"field": "brand"}
```

Also parse `collapse.inner_hits` for sort/source fields and multi-level collapse.
Category: `filtered` for the collapse field.

#### Gap 8: Composite Agg Sources — BUG (15 lines)

**Data-loss bug:** All fields in composite aggregation sources are missed today. The
current code looks for `agg_body.get("field")` but composite wraps its fields in a
`sources` array of named sub-aggregation specs:

```json
"composite": {
  "sources": [
    {"category_src": {"terms": {"field": "category"}}},
    {"date_bucket": {"date_histogram": {"field": "order_date", "calendar_interval": "month"}}}
  ]
}
```

Need special handling to iterate through `sources`, then into each source spec's inner agg body.

#### Gap 9: `filter`/`filters` Agg Queries — BUG (15 lines)

**Data-loss bug:** The `filter` bucket agg contains a query dict, not `{"field": "..."}`.
The `filters` bucket agg contains named filter queries. Neither is parsed for field references.

```json
"aggs": {
  "active_products": {
    "filter": {"term": {"status": "active"}},
    "aggs": { "avg_price": {"avg": {"field": "price"}} }
  }
}
```

Fix: detect `filter` agg and recurse into it as a query with `context="filtered"`.
Detect `filters` agg and recurse into each named filter.

### 3.3 SHOULD-HAVE Gaps (~375 lines)

#### Gap 10: Painless Script Field Extraction (20 lines)

Regex extraction of field references from Painless scripts. Enables script_fields,
scripted sorts, update scripts, runtime fields, function_score script_score, bucket_script.

Common patterns:
```python
_PAINLESS_DOC_RE = re.compile(r"doc\['\s*([^']+?)\s*'\]")        # doc['field']
_PAINLESS_DOC_DQ_RE = re.compile(r'doc\["\s*([^"]+?)\s*"\]')     # doc["field"]
_PAINLESS_CTX_SOURCE_RE = re.compile(r"ctx\._source\.(\w+)")      # ctx._source.field
```

~90% coverage. Misses: fields stored in variables (rare), dynamic field names (very rare).

#### Gap 11: `function_score` Query (20 lines)

Custom scoring is common in e-commerce (boost by popularity/recency). The inner `query` and
function-specific fields are not extracted:

```json
"function_score": {
  "query": {"match": {"title": "laptop"}},
  "functions": [
    {"field_value_factor": {"field": "popularity", "modifier": "log1p"}},
    {"gauss": {"date": {"origin": "now", "scale": "10d"}}},
    {"script_score": {"script": {"source": "doc['rating'].value * 2"}}}
  ]
}
```

Fields: `function_score.query` (recurse), `field_value_factor.field`, decay function keys
(the key under gauss/linear/exp is a field name), `script_score` (via Painless extraction).

#### Gap 12: Inner Hits + `has_child` / `has_parent` (20 lines)

`has_child` and `has_parent` queries are not recognized — their `query` sub-objects go
unextracted. `inner_hits` within nested/has_child/has_parent can contain sort, _source,
highlight, docvalue_fields — none parsed.

#### Gap 13: `script_fields` / `runtime_mappings` (25 lines)

`script_fields`: computed fields via Painless. Outer keys are aliases; actual field refs
are in `script.source`. Depends on Painless extraction (#10).

`runtime_mappings`: query-time field definitions. Same dependency on Painless extraction.

#### Gap 14: Missing Aggregation Types (30 lines)

Agg types with `"field"` key NOT in current type sets:
- `rare_terms`, `multi_terms`, `weighted_avg`, `top_metrics`, `boxplot`, `rate`,
  `string_stats`, `significant_text`, `geo_bounds`, `geo_centroid`, `geo_distance`,
  `geohash_grid`, `geotile_grid`, `ip_range`, `missing`, `diversified_sampler`,
  `variable_width_histogram`, `matrix_stats`, `t_test`

Special handling needed for:
- `multi_terms`: `terms[].field`
- `weighted_avg`: `value.field` and `weight.field`
- `top_metrics`: `metrics[].field`
- `matrix_stats`: `fields` (plural)
- `t_test`: `a.field` and `b.field`

#### Gap 15: ES|QL (`POST /_query`) (150-200 lines)

Elasticsearch's new pipe-based query language (ES 8.11+, GA in 8.14). **Strategic importance
— Elastic's direction.** Growing from 5-20% of traffic in modern deployments.

```json
POST /_query
{
  "query": "FROM products | WHERE price > 100 | STATS avg_price = AVG(price) BY brand | SORT avg_price DESC"
}
```

Regex-based parser gets ~80% coverage. Needs to:
- Split by `|` into stages
- Extract fields from WHERE (before operators), STATS (function args + BY fields),
  SORT, KEEP, EVAL, DISSECT/GROK (first arg)
- Strip quoted strings before extraction to avoid false positives

#### Gap 16: SQL API (`POST /_sql`) (60 lines)

For BI tool integration (Tableau, Grafana, DBeaver via JDBC driver). 3-10% in BI-heavy
environments.

```json
POST /_sql
{
  "query": "SELECT title, price FROM products WHERE category = 'electronics' ORDER BY price DESC"
}
```

Options:
- **`sqlglot` library** (recommended): Pure Python SQL parser, handles ES SQL syntax, produces AST
- **`_sql/translate` endpoint**: Forward to ES, get back DSL, parse with existing extractor.
  Perfect accuracy but adds network round-trip per SQL request.

### 3.4 NICE-TO-HAVE Gaps (~260 lines)

| Gap | Lines | Notes |
|-----|-------|-------|
| Search templates (inline) | 40 | Dict `source` is easy — pass to existing extractor. Stored templates need fetch + cache. |
| EQL (`_eql/search`) | 80 | Only relevant for Elastic Security deployments. Regex for field extraction from EQL strings. |
| `_reindex` source/dest | 30 | Low frequency but useful for data flow tracking. Two-index challenge (source vs. dest). |
| Scroll/PIT index attribution | 30 | PIT searches get `index: _unknown` because index is in opaque PIT ID, not URL path. |

### 3.5 Implementation Dependencies

```
Painless script extraction (#10)
  ├── script_fields (#13)
  ├── runtime_mappings (#13)
  ├── _update_by_query scripts (#4, script portion)
  ├── _reindex scripts (nice-to-have)
  ├── function_score script_score (#11)
  └── bucket_script/bucket_selector (pipeline agg scripts)

sqlglot library (pip install)
  └── SQL API (#16)

ES|QL tokenizer (new code)
  └── ES|QL (#15)

parse_path changes
  ├── _async_search detection (#1)
  ├── search/template detection (nice-to-have)
  ├── _eql/search detection (nice-to-have)
  └── _query detection (#15)
```

---

## 4. Features to Add — Ranked by Impact/Effort

### 4.0 Quick Win: Response-Time-Weighted Heat Scoring

**Key lesson from pganalyze, VividCortex, and MongoDB Atlas:** All three weight field importance
by `total_time = frequency * avg_latency`, not just by count. A field queried 100 times at 500ms
each (50s total) matters more than one queried 10,000 times at 1ms (10s total).

**Current heat scoring** (in `analyzer.py`):
`field_importance = reference_count / total_references`

**Better heat scoring:**
`field_importance = sum(response_time_ms for events referencing field) / sum(all response_time_ms)`

The data already exists in our events (`response_time_ms` per event). This is a change to the
aggregation query in `analyzer.py` — approximately ~20 lines. No new data collection needed.

**Should be implemented alongside the MUST-HAVE parsing fixes as a quick win.**

### 4.1 Tier 1: Build First (Highest Impact-to-Effort)

#### Feature 1: Query Template Clustering (~185 lines)

**Problem:** Current `query_fingerprint` is an exact SHA-256 of the canonicalized JSON body.
Two queries with identical structure but different parameter values produce different hashes:
```json
{"query": {"match": {"title": "laptop"}}}    // hash A
{"query": {"match": {"title": "wireless"}}}  // hash B
```

In a real system, 10,000 unique fingerprints may represent only 15 query templates.

**Solution:** Template extraction — recursively walk the DSL and replace leaf values with `"?"`:
```json
{"query": {"match": {"title": "?"}}, "size": "?"}
```

Hash the template for a stable structural fingerprint. Same approach as pganalyze (SQL
normalization), Datadog DBM ("query signatures"), MongoDB Atlas ("query shapes").

**Implementation:**
- New function `compute_query_template(body: dict) -> (template_hash, template_json)`
- Rules: dict keys preserved, primitive leaf values replaced with `"?"`, arrays of
  primitives collapsed to `["?"]`, `_source` field names preserved (different selections
  = different patterns)
- New event fields: `query_template` (keyword, hash), `query_template_text` (non-indexed, readable)
- New endpoint: `GET /_gateway/query-patterns?index_group=&hours=24`
- Pattern drift detection: compare template sets between consecutive time windows

**Enables:** "Top queries by total response time", pattern drift detection, "which query
template drives the most load?"

#### Feature 2: Client Attribution (~145 lines)

**Problem:** "Who would be affected if we remove field X?" blocks every schema change.

**Already built:** `client_id` captured from `X-Client-Id` header, stored as keyword in events.

**What's missing:** Aggregation endpoints.

**New endpoints:**
- `GET /_gateway/clients` — all known client IDs with event counts, index groups, last seen
- `GET /_gateway/client-usage?client_id=search-service` — detailed field usage for a client
- `GET /_gateway/field-clients?field=price&index_group=products` — which clients use a field
  (the inverse lookup — critical for impact analysis)

**Enhanced identification** (beyond X-Client-Id):
- Source IP from `request.client.host`
- User-Agent header (identifies client library)
- New event fields: `client_ip`, `client_user_agent`

#### Feature 3: Mapping Diff (~400 lines)

**The core optimization feature.** Compare actual index mapping vs. what usage data says is needed.

**New file:** `gateway/mapping_cache.py`
- Periodic fetch of `GET /<index>/_mapping`
- Flatten nested mappings into field inventory: `{field_path: {type, index, doc_values, norms, analyzer, multi_fields}}`
- Handle nested objects, multi-fields, dot-path flattening
- Cache with atomic swap (same pattern as `metadata.py`)

**New endpoint:** `GET /_gateway/mapping-diff?index_group=products&hours=168`

**Field classification matrix:**

| Usage Pattern | Mapping Implication |
|---|---|
| `queried` (match/match_phrase context) | Needs inverted index, norms, analyzer |
| `filtered` only (term/range/exists) | Needs inverted index, does NOT need norms |
| `aggregated` or `sorted` | Needs `doc_values: true` |
| `sourced` only | Does NOT need inverted index or doc_values |
| `written` only | Does NOT need inverted index or doc_values |
| Zero usage | Candidate for removal or `enabled: false` |

**Response structure:**
```json
{
  "index_group": "products",
  "observation_window": "168h",
  "total_events": 15420,
  "mapping_fields": 25,
  "used_fields": 12,
  "unused_fields": 13,
  "fields": {
    "description": {
      "in_mapping": true,
      "mapping": {"type": "text"},
      "usage": {"queried": 0, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 320},
      "classification": "write-only",
      "issues": ["Field is written but never read - index and doc_values are wasted"]
    }
  }
}
```

### 4.2 Tier 2: Build Next (High Impact, Moderate Effort)

#### Feature 4: Mapping Recommendations (~280 lines)

8 decision rules based on usage patterns + mapping context:

| Rule | Condition | Recommendation |
|------|-----------|----------------|
| 1 | Written but never read | `{"index": false, "doc_values": false}` |
| 2 | Sourced only (never queried/filtered/aggregated/sorted) | `{"index": false, "doc_values": false}` |
| 3 | Queried/filtered only, never aggregated/sorted | `{"doc_values": false}` |
| 4 | Filtered only (never scored), text type, norms enabled | `{"norms": false}` |
| 5 | Text type, never match/match_phrase, only term/terms | Change type to `keyword` (requires reindex) |
| 6 | Text type, used with match AND term/agg, no .keyword sub-field | Add `.keyword` multi-field |
| 7 | Multi-field (e.g., title.keyword) with zero usage | Remove multi-field |
| 8 | Zero usage across all categories | Remove field or `{"enabled": false}` |

Output includes: current mapping, proposed mapping, reason, confidence level, impact estimate
(storage savings), affected clients (via Feature 2), breaking change flag, caveats.

**Depends on:** Feature 3 (Mapping Diff).

#### Feature 5: CI/CD Validation API (~200 lines)

`POST /_gateway/validate-mapping-change` — "Will removing field X break any active query?"

**Request:**
```json
{
  "index_group": "products",
  "changes": [
    {"action": "remove_field", "field": "description"},
    {"action": "disable_doc_values", "field": "tags"},
    {"action": "add_field", "field": "sku", "mapping": {"type": "keyword"}}
  ],
  "observation_window_hours": 168
}
```

**Response:** Per-change safety assessment with severity, reason, affected clients,
affected query templates, weekly query count.

**Breaking change detection rules:**

| Proposed Change | Breaking If... |
|---|---|
| Remove field | Field has any non-zero usage |
| Change type | Incompatible (text→keyword breaks match; keyword→text breaks term) |
| Disable index | Field is queried or filtered |
| Disable doc_values | Field is aggregated or sorted |
| Disable norms | Field is queried in scoring context |
| Remove multi-field | Sub-field has any non-zero usage |
| Add field | Always safe |

**CI pipeline integration example:**
```yaml
- name: Validate mapping change
  run: |
    RESULT=$(curl -s -X POST http://gateway:9301/_gateway/validate-mapping-change \
      -H "Content-Type: application/json" -d @mapping-change.json)
    SAFE=$(echo "$RESULT" | jq -r '.safe')
    [ "$SAFE" = "true" ] || exit 1
```

**Depends on:** Features 2 (Client Attribution) and 3 (Mapping Diff).

#### Feature 6: Structural Alerting (~150 lines)

Deterministic, binary signals — no ML:

| Alert | Trigger | Detection |
|-------|---------|-----------|
| Unmapped field reference | Query references field not in mapping | Compare event fields vs. mapping (requires Feature 3) |
| New query template | Template seen now but not in prior window | Compare template sets (requires Feature 1) |
| Field usage dropped to zero | Field had N>threshold uses in prior window, zero now | Compare consecutive time windows |
| Dynamic mapping growth | New fields appeared in mapping snapshot | Compare consecutive mapping snapshots |

Storage: New `.usage-alerts` index. Endpoint: `GET /_gateway/alerts?index_group=&hours=24&severity=warning,critical`

### 4.3 Tier 3: Depth and Breadth

#### Feature 7: Cross-Index Field Lineage (~230 lines)

Detect type conflicts and naming inconsistencies across index groups.

New endpoint: `GET /_gateway/field-lineage?min_groups=2`

Consistency checks:
1. Type mismatch (same field name, different types)
2. Analyzer mismatch
3. Multi-field inconsistency (.keyword in one index but not another)
4. Naming inconsistency heuristic (e.g., `timestamp` vs. `created_at` both used as date filters)

**Depends on:** Feature 3 (Mapping Diff) for type comparison.

#### Feature 8: Cost Attribution — Proxy-Level (~100 lines)

Rank query templates by `total_time = avg_latency * count`. Data already exists in events.

```json
{
  "size": 0,
  "aggs": {
    "by_template": {
      "terms": {"field": "query_template", "size": 50, "order": {"total_time": "desc"}},
      "aggs": {
        "total_time": {"sum": {"field": "response_time_ms"}},
        "avg_time": {"avg": {"field": "response_time_ms"}},
        "p95_time": {"percentiles": {"field": "response_time_ms", "percents": [95]}},
        "fields_queried": {"terms": {"field": "fields.queried", "size": 20}}
      }
    }
  }
}
```

**No new data collection needed.** Purely an aggregation endpoint.
**Depends on:** Feature 1 (Query Templates) for meaningful grouping.

#### Feature 9: Schema Evolution Tracking (~310 lines)

Hourly mapping snapshots stored in `.mapping-history`. Detect field count growth, correlate
with usage changes.

New endpoint: `GET /_gateway/schema-history?index_group=products&days=30`

Tracks: fields added, fields removed (rare), type changes, field count trend over time.
Correlation: "Field `tags` was added on Feb 10. Within 48h, 340 queries filtered on it."

**Depends on:** Feature 3 (Mapping Diff) for mapping fetch/flatten logic.

### 4.4 Tier 4: Production Scale

| Feature | ~Lines | Notes |
|---------|--------|-------|
| ILM/Retention Awareness | 300 | Correlate lookback data with ILM phases. "p99 lookback is 48h but hot phase is 30d — oversized." |
| Field-level Capacity Planning | 300 | On-demand `_disk_usage` API call + join with usage data. "Field X costs 18MB but is never read." |
| Statistical Anomaly Detection | 300 | Z-score or EWMA on rolling baselines. Alert on usage deviations >3 sigma. |
| Cost Attribution — Cluster-Level | 200 | Background stats collection (fielddata, segments). Join cluster-level cost with field-level usage. |
| Threshold Alerting | 200 | User-configurable rules stored in `.alert-rules`. Evaluation loop with de-duplication. |

### 4.5 Feature Dependencies

```
Quick Win: Response-Time-Weighted Heat (standalone)

Feature 1 (Templates) ─────────────────┐
                                        ├──> Feature 5 (CI/CD Validation)
Feature 2 (Clients) ───────────────────┤
                                        ├──> Feature 6 (Alerting)
Feature 3 (Mapping Diff) ──┐           │
                            ├──> Feature 4 (Recommendations) ──┘
                            ├──> Feature 7 (Field Lineage)
                            ├──> Feature 9 (Schema Tracking)
                            └──> Tier 4 features

Feature 8 (Proxy Cost) ── needs Feature 1 (Templates) for grouping
```

Features 1, 2, and the quick win can all be built in parallel (no dependencies on each other).
Feature 3 should follow immediately because it unlocks the most downstream features.

---

## 5. Architecture Patterns

### 5.1 Our Proxy Approach is Architecturally Sound

pganalyze (Postgres), ProxySQL/MaxScale (MySQL), and MongoDB Atlas all use similar
query-interception patterns. The proxy/middleware approach for database observability
is a proven pattern across the industry.

### 5.2 Don't Switch to Envoy

ES Query DSL parsing is too complex for WASM filters (~500 lines of careful tree walking
in extractor.py, handling 20+ query types, nested bool logic, bulk NDJSON, msearch NDJSON).
A purpose-built Python proxy is the right tool. Envoy ext_proc (external processing) is a
theoretical option but adds latency and operational complexity for no clear benefit.

### 5.3 OTel Integration (Future Option)

Could emit usage data as OpenTelemetry metrics/spans instead of or in addition to custom ES
events. Would enable integration with OTel-native observability platforms. Not urgent — our
current ES-native storage works well and is self-contained.

### 5.4 Kafka/Redis Buffering is Overkill

Our new bulk writer with bounded asyncio.Queue handles throughput. Adding Kafka or Redis
would increase operational complexity without proportional benefit at current scale. Revisit
if queue drops become significant.

### 5.5 Deployment Patterns for OpenShift/K8s

**Sidecar pattern** (one gateway per ES pod): Lowest latency, scales with ES cluster, but
requires pod spec modification.

**Standalone proxy**: Simplest deployment, independent scaling, what we have now. Works
well with OpenShift Route + HPA.

**DaemonSet agent**: One gateway per node. Good for multi-tenant clusters where multiple
ES instances share nodes.

Current architecture (standalone proxy) is the right choice for initial deployment.

### 5.6 Data Pipeline Considerations

ES ingest pipelines could transform usage events before indexing (e.g., enrich with mapping
metadata, compute derived fields). However, this moves logic into ES-specific infrastructure
and makes the system harder to test and debug. Keep logic in the gateway Python code.

---

## 6. Implementation Roadmap

### Phase 1: Quick Wins (< 1 day)

1. Fix `_async_search` — one token, potentially 20-50% of invisible Kibana traffic
2. Add `docvalue_fields`, `stored_fields`, `highlight`, `suggest` parsing
3. Fix composite agg sources and filter agg query bugs
4. Response-time-weighted heat scoring in `analyzer.py`

### Phase 2: Core Features (1-3 days)

5. Query template clustering (structural fingerprinting)
6. Client attribution endpoints
7. Painless script field extraction (enables script_fields, runtime_mappings, function_score)

### Phase 3: Strategic Features (1-2 weeks)

8. Mapping diff + mapping cache
9. Mapping recommendations engine (8 rules)
10. CI/CD validation API
11. ES|QL parser (regex-based, 80% coverage)
12. Structural alerting

### Phase 4: Depth (ongoing)

13. Cross-index field lineage
14. Schema evolution tracking
15. SQL API parser (via sqlglot)
16. Missing agg types (long tail)
17. Cost attribution endpoints
18. ILM/retention awareness

---

## 7. Key Files That Would Change Per Feature

| Feature | New Files | Modified Files |
|---------|-----------|----------------|
| Parsing fixes (Phase 1) | (none) | `gateway/extractor.py` |
| Heat scoring fix | (none) | `gateway/analyzer.py` |
| Query Templates | (none) | `gateway/extractor.py`, `gateway/events.py`, `gateway/main.py` |
| Client Attribution | `gateway/clients.py` (optional) | `gateway/main.py`, `gateway/events.py` |
| Mapping Diff | `gateway/mapping_cache.py` | `gateway/metadata.py`, `gateway/main.py` |
| Recommendations | `gateway/recommender.py` | `gateway/analyzer.py`, `gateway/main.py` |
| CI/CD Validation | `gateway/validator.py` | `gateway/main.py` |
| Alerting | `gateway/alerting.py` | `gateway/main.py`, `config.py` |
| Field Lineage | (none) | `gateway/analyzer.py`, `gateway/main.py` |
| Cost Attribution | `gateway/cost.py` | `gateway/proxy.py`, `gateway/events.py`, `gateway/main.py` |
| Schema History | `gateway/schema_history.py` | `gateway/main.py`, `config.py` |
| ES|QL Parser | (none) | `gateway/extractor.py` |
| SQL Parser | (none) | `gateway/extractor.py`, `requirements.txt` |
