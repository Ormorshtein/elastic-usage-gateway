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
**Status: [ ] NOT STARTED**

**Value:** Currently a field queried 100x at 500ms each looks less important than one queried 10,000x at 1ms. Every mature DB observability tool (pganalyze, MongoDB Atlas) weights by total time, not count. This makes heat rankings more accurate — fields that cause the most total latency get flagged first.

**Scope:**
- Change `analyzer.py` aggregation query to weight by `response_time_ms` (~20 lines)
- No new data collection needed — `response_time_ms` already exists in events

**Files:** `gateway/analyzer.py`, `tests/test_analyzer.py`

---

## Deliverable 3: Query Template Clustering (Feature 1)
**Status: [ ] NOT STARTED**

**Value:** 10,000 unique fingerprints may represent only 15 query patterns. Without structural grouping, you can't answer "which query pattern drives the most load?" or detect when a new pattern appears. Prerequisite for cost attribution and pattern drift detection.

**Scope:**
- Template extraction: replace leaf values with `"?"`, hash the structure (~60 lines)
- New event fields: `query_template`, `query_template_text`
- New endpoint: `GET /_gateway/query-patterns`
- Pattern drift detection between time windows

**Files:** `gateway/extractor.py`, `gateway/events.py`, `gateway/main.py`, tests

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
**Status: [ ] NOT STARTED**

**Value:** Core promise of the project — comparing what's in the mapping vs what's actually used. "Field X has an inverted index but is never queried." Unlocks recommendations, CI/CD validation, alerting, and lineage.

**Scope:**
- New `gateway/mapping_cache.py` — fetch + flatten index mappings
- New endpoint: `GET /_gateway/mapping-diff`
- Field classification (write-only, sourced-only, unused, etc.)

**Files:** `gateway/mapping_cache.py` (new), `gateway/metadata.py`, `gateway/main.py`, tests
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
D2 (Heat Scoring) ─── standalone, can start now
D3 (Templates) ─────── standalone, can start now
D4 (Clients) ────────── standalone, can start now

D5 (Mapping Diff) ──── after D1-D2 for accurate data
D6 (Recommendations) ─ after D5
D7 (Painless) ───────── standalone, can start now

CI/CD Validation ───── after D3 + D4 + D5
Alerting ───────────── after D3 + D5
Lineage ────────────── after D5
Cost Attribution ───── after D3
```

Deliverables 2, 3, 4, and 7 can all be built in parallel (no dependencies on each other).
Deliverable 5 should follow because it unlocks the most downstream features.
