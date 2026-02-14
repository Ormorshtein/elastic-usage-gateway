# ES Usage Gateway — Implementation Roadmap

Testable deliverables derived from [RESEARCH.md](RESEARCH.md). Each deliverable is independently shippable with its own tests.

## Status Key

- [x] Done
- [ ] Not started

---

## Deliverable 1: Fix Parsing Blind Spots (Gaps 1-9)
**Status: [x] DONE (2026-02-13)**

Fix data quality at the foundation — the extractor was silently missing real traffic patterns.

| Gap | What | Value | Lines |
|-----|------|-------|-------|
| 1 | `_async_search` | 20-50% of Kibana searches were invisible | 1 |
| 2 | `docvalue_fields` | Kibana Discover field reads were invisible | 10 |
| 3 | `highlight` | Search UI snippet fields untracked | 10 |
| 4 | `_update_by_query` / `_delete_by_query` | GDPR cleanup, log rotation queries untracked | 1 |
| 5 | `stored_fields` | Explicit stored field retrieval untracked | 5 |
| 6 | Suggesters | Autocomplete (every keystroke) untracked | 10 |
| 7 | Field collapsing | E-commerce dedup field untracked | 5 |
| 8 | Composite agg sources (BUG) | All composite agg fields silently lost | 15 |
| 9 | Filter/filters agg queries (BUG) | Fields in agg filter clauses silently lost | 15 |

**Files changed:** `gateway/extractor.py`, `tests/test_extractor.py`
**Tests added:** 24 new (210 total)

---

## Deliverable 2: Response-Time-Weighted Heat Scoring
**Status: [x] DONE (2026-02-13)**

**Value:** Currently a field queried 100x at 500ms each looks less important than one queried 10,000x at 1ms. Every mature DB observability tool (pganalyze, MongoDB Atlas) weights by total time, not count. This makes heat rankings more accurate — fields that cause the most total latency get flagged first.

**Scope:**
- Added `fields_by_response_time` panel alongside existing `fields` in heat report
- Added `scoring` metadata explaining both methods
- ES aggregation query adds `sum` sub-agg on `response_time_ms` (~20 lines)
- No new data collection needed — `response_time_ms` already exists in events
- 5 new Kibana visualizations in Usage & Heat Dashboard (one per field category, using `sum(response_time_ms)`)

**Files changed:** `gateway/analyzer.py`, `tests/test_analyzer.py`, `kibana_setup.py`
**Tests added:** 7 new (217 total)

---

## Deliverable 3: Query Template Clustering (Feature 1)
**Status: [x] DONE (2026-02-13)**

**Value:** 10,000 unique fingerprints may represent only 15 query patterns. Without structural grouping, you can't answer "which query pattern drives the most load?" or detect when a new pattern appears. Prerequisite for cost attribution and pattern drift detection.

**Scope:**
- Template extraction: replace leaf values with `"?"`, hash the structure (~60 lines)
- New event fields: `query_template`, `query_template_text`
- New endpoint: `GET /_gateway/query-patterns`
- Pattern drift detection between time windows
- Kibana panels + dashboard section headers (see below)

**Files changed:** `gateway/events.py`, `gateway/analyzer.py`, `gateway/main.py`, `kibana_setup.py`, `tests/test_events.py`, `tests/test_analyzer.py`
**Tests added:** 28 new (244 total)

### Kibana Panels

Also reorganize the existing Usage & Heat dashboard with **Markdown section headers** so each group of panels is introduced with a title and a one-line explanation. Current layout has no visual separation — panels just flow together.

New section headers (Markdown visualization panels):
- **Overview** — Traffic volume, index groups, and query type breakdown.
- **Field Heat (by Count)** — Which fields are used most often, by operation type.
- **Field Heat (by Response Time)** — Which fields cause the most total latency.
- **Query Patterns** *(new, D3)* — Which structural query shapes drive traffic and cost.
- **Lookback Analysis** — How far back queries look in time.
- **Raw Events** — Individual query-level event log.

New panels in the "Query Patterns" section:

| Panel | Type | Metric | Insight |
|-------|------|--------|---------|
| Top Query Templates | table | count by `query_template_hash`, show `query_template_text` | "3 query shapes account for 73% of traffic" — tells you where to focus optimization |
| Query Templates Over Time | stacked area | count over time, split by `query_template_hash` | Pattern drift — a new color band means a new query shape entered production (deploy, new dashboard, runaway script) |
| Costliest Query Templates | horizontal bar | `sum(response_time_ms)` by template | Total cluster time consumed per pattern — 4,200 calls × 42ms = 176s matters more than 10 calls × 2s = 20s, even though the latter is "slower" |
| Template Count per Index Group | table | `cardinality(query_template_hash)` by `index_group` | Query complexity by index — if `products` jumps from 12 to 25 templates, someone is generating dynamic queries |

---

## Deliverable 4: Client Attribution (Feature 2)
**Status: [x] DONE (2026-02-13)**

**Value:** "Who would break if we remove field X?" blocks every schema change. Without client attribution, you can't do impact analysis. Unlocks CI/CD validation API later.

**Scope:**
- Capture `client_ip`, `client_user_agent`, `client_id` (via `x-client-id` header) in every usage event
- Client attribution surfaced via Kibana dashboards (not JSON endpoints — consistent with Kibana-first pivot):
  - 5-panel Client Attribution section in Usage & Heat Dashboard
  - Field Drill-Down dashboard includes clients table, client IPs table, user-agents pie chart
- Original plan listed 3 JSON endpoints (`/_gateway/clients`, etc.) — superseded by the Kibana-first approach adopted in the analysis endpoint removal

**Files changed:** `gateway/events.py`, `gateway/main.py`, `kibana_setup.py`, `tests/test_events.py`
**Tests added:** 4 new

---

## Deliverable 5: Mapping Diff (Feature 3)
**Status: [x] DONE (2026-02-13)**

**Value:** Core promise of the project — comparing what's in the mapping vs what's actually used. "Field X has an inverted index but is never queried." Unlocks recommendations, CI/CD validation, alerting, and lineage.

**Scope:**
- New `gateway/mapping_diff.py` — fetch + flatten index mappings, query usage, classify fields, background refresh loop
- Results written to `.mapping-diff` ES index (no JSON API endpoint — Kibana visualizes)
- Field classification: `active`, `sourced_only`, `write_only`, `unused`
- Per-field `last_seen` timestamps and reference counts across all 6 usage categories
- New Kibana "Mapping Diff" dashboard with 6 panels + index_group filter

**Files changed:** `gateway/mapping_diff.py` (new), `tests/test_mapping_diff.py` (new), `config.py`, `gateway/main.py`, `gateway/metrics.py`, `kibana_setup.py`
**Tests added:** 48 new (264 total)
**Depends on:** Deliverables 1-2 for accurate usage data

---

## Deliverable 6: Mapping Recommendations (Feature 4)
**Status: [x] DONE (2026-02-14)**

**Value:** 8 decision rules turn raw data into specific, actionable changes: "set `index: false`", "remove this multi-field", "change type to keyword". Each recommendation includes a detailed `why` (explanation + tradeoffs) and `how` (concrete JSON mapping snippets). This is the output teams actually act on.

**Scope:**
- New `gateway/recommender.py` with 8 rules — reads `.mapping-diff`, writes to `.mapping-recommendations`
- Background refresh loop (same pattern as mapping_diff.py)
- New Kibana "Mapping Recommendations" dashboard with 5 panels
- Manual refresh endpoint: `POST /_gateway/recommendations/refresh`
- Each recommendation includes `why` and `how` long-form text columns

**Files changed:** `gateway/recommender.py` (new), `tests/test_recommender.py` (new), `gateway/main.py`, `config.py`, `gateway/metrics.py`, `kibana_setup.py`
**Tests added:** 40 new (304 total)
**Depends on:** Deliverable 5 (Mapping Diff)

---

## Deliverable 7: Painless Script Extraction + Dependents
**Status: [x] DONE (2026-02-14)**

**Value:** Scripts were a blind spot — `script_fields`, scripted sorts, `function_score`, `runtime_mappings`, and pipeline aggs all access fields via Painless that the gateway was ignoring. Without this, fields used only in scripts appeared as "unused" and could receive false `remove_field` recommendations.

**Scope:**
- New `_extract_script_fields()` helper — regex extraction of `doc['field']`, `doc["field"]`, `ctx._source.field`
- Language-aware: checks `lang` field (default Painless), skips Mustache and stored scripts
- Wired into 5 DSL locations: `script_fields` → sourced, `runtime_mappings` → sourced, `function_score` (script_score + field_value_factor + decay) → queried, scripted sort → sorted, pipeline/scripted aggs → aggregated

**Files changed:** `gateway/extractor.py`, `tests/test_extractor.py`
**Tests added:** 27 new (331 total)

---

## Deliverable 8: Index Architecture Recommendations
**Status: [ ] NOT STARTED**

**Value:** D6 answers "how should this field be mapped?" but teams also need "how should this index be structured?" — rollover frequency, shard sizing, and index partitioning. Bad index architecture wastes more resources than bad field mappings. A 200GB daily index with 1 shard is as much of a problem as 50 unused indexed fields.

**Scope:**
- New `_stats` API polling: periodically fetch index size, doc count, shard count per index group
- Results written to `.index-stats` ES index (same pattern as `.mapping-diff`)
- Recommendation rules based on `_stats` data + existing lookback analysis from `.usage-events`:

| Rule | Input data | Recommendation |
|------|-----------|----------------|
| Rollover too frequent | p95 lookback is 48h, indices are daily → queries span 2-3 indices | Switch to weekly rollover — fewer shards searched per query |
| Rollover too infrequent | p95 lookback is 6h, indices are weekly → each query loads 7 days to read 6h | Switch to daily rollover — ES can skip irrelevant shards |
| Shards too large | Shard size > 50GB | Increase shard count or roll over more frequently |
| Shards too small | Shard size < 1GB and many shards | Reduce shard count or roll over less frequently — small shards waste cluster overhead |
| Too many shards per index | Shard count > 5 and shard size < 10GB | Reduce `number_of_shards` in template |

- New Kibana dashboard section or standalone dashboard with index-level sizing table + recommendations
- Each recommendation includes `why` (explanation + tradeoffs) and `how` (concrete template/ILM changes)

**Files:** `gateway/index_stats.py` (new), `kibana_setup.py`, `gateway/main.py`, `config.py`, tests
**Depends on:** Lookback data from `.usage-events` (D1-D3), `_stats` API polling (new)

---

## Future Deliverables (from RESEARCH.md Tiers 2-4)

These are not yet broken into testable deliverables. Scope when the above are done.

- **CI/CD Validation API** — "Will removing field X break any active query?" (depends on D4, D5)
- **Structural Alerting** — new template, field usage drop, unmapped references (depends on D3, D5)
- **Cross-Index Field Lineage** — type conflicts, naming inconsistencies (depends on D5)
- **Cost Attribution** — rank query templates by total response time (depends on D3)
- **Schema Evolution Tracking** — hourly mapping snapshots, field count growth (depends on D5)
- **ES|QL Parser** — regex-based, ~80% coverage (~200 lines)
- **SQL API Parser** — via sqlglot library (~60 lines)
- **Missing Agg Types** — long tail of less common agg types (~30 lines)

---

## Dependency Graph

```
D1 (Parsing Fixes) ✅
D2 (Heat Scoring) ✅
D3 (Templates) ✅
D4 (Clients) ✅

D5 (Mapping Diff) ✅
D6 (Recommendations) ✅
D7 (Painless) ✅
D8 (Index Arch Recs) ── needs _stats polling (new) + lookback data (D1-D3)

CI/CD Validation ───── after D3 ✅ + D4 ✅ + D5 ✅ → ready to start
Alerting ───────────── after D3 ✅ + D5 ✅ → ready to start
Lineage ────────────── after D5 ✅ → ready to start
Cost Attribution ───── after D3 ✅ → ready to start
```

Deliverables 1-7 are complete. D8 can start anytime — its only new dependency is `_stats` API polling, which is self-contained.
All future deliverables (CI/CD Validation, Alerting, Lineage, Cost Attribution) have their prerequisites met.
