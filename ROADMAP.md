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
**Status: [ ] NOT STARTED**

**Value:** "Who would break if we remove field X?" blocks every schema change. Without client attribution, you can't do impact analysis. Unlocks CI/CD validation API later.

**Scope:**
- Capture `client_ip`, `client_user_agent` in events
- 3 new endpoints: `/_gateway/clients`, `/_gateway/client-usage`, `/_gateway/field-clients`

**Files:** `gateway/events.py`, `gateway/main.py`, tests

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
**Status: [ ] NOT STARTED**

**Value:** 8 decision rules turn raw data into specific, actionable changes: "set `index: false`", "remove this multi-field", "change type to keyword". This is the output teams will actually act on.

**Scope:**
- New `gateway/recommender.py` with 8 rules (~280 lines)
- Endpoint: `GET /_gateway/recommendations`

**Files:** `gateway/recommender.py` (new), `gateway/analyzer.py`, `gateway/main.py`, tests
**Depends on:** Deliverable 5 (Mapping Diff)

---

## Deliverable 7: Painless Script Extraction + Dependents
**Status: [ ] NOT STARTED**

**Value:** Scripts are a blind spot — `script_fields`, scripted sorts, `function_score`, `runtime_mappings` all access fields via Painless that the gateway ignores. Rounds out extraction coverage.

**Scope:**
- Painless regex extraction (`doc['field']`, `ctx._source.field`) (~20 lines)
- Wire into `script_fields`, `runtime_mappings`, `function_score`

**Files:** `gateway/extractor.py`, tests

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
D4 (Clients) ────────── standalone, can start now

D5 (Mapping Diff) ✅
D6 (Recommendations) ─ after D5 ✅ — ready to start
D7 (Painless) ───────── standalone, can start now

CI/CD Validation ───── after D3 + D4 + D5
Alerting ───────────── after D3 + D5
Lineage ────────────── after D5
Cost Attribution ───── after D3
```

Deliverables 2, 3, 4, and 7 can all be built in parallel (no dependencies on each other).
Deliverable 5 should follow because it unlocks the most downstream features.
